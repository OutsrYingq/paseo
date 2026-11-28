import { statfs } from "node:fs/promises";
import { freemem, totalmem } from "node:os";
import type { Logger } from "pino";
import { DaemonConfigStore, toWireDaemonConfig } from "../daemon-config-store.js";
import { loadPersistedConfig, type PersistedConfig } from "../persisted-config.js";
import { getPidLockInfo } from "../pid-lock.js";
import {
  RemoteHostConfigSchema,
  RemoteHostProfilesPatchSchema,
  remoteHostProfileControlKeys,
  type RemoteHostSettingsOperation,
} from "./remote-host-settings-protocol.js";
import { WorkerOperationError } from "./execution-protocol.js";

const operatorControlledPaths = [
  "daemon.listen",
  "daemon.auth",
  "daemon.relay",
  "daemon.serviceProxy",
  "daemon.hostnames",
  "daemon.allowedHosts",
  "daemon.cors",
  "daemon.trustedProxies",
  "agents.providers",
];

function readProfiles(persisted: PersistedConfig) {
  return {
    agentProfiles: persisted.daemon?.agentProfiles,
    terminalProfiles: persisted.daemon?.terminalProfiles,
  };
}

export function createResidentHostSettings(options: {
  paseoHome: string;
  logger: Logger;
  daemonVersion?: string;
  listNativeProviders(): Promise<readonly string[]>;
  getStartedAt?(): string | null;
}) {
  const { paseoHome, logger } = options;
  const persisted = loadPersistedConfig(paseoHome, logger);
  const initial = {
    mcp: { injectIntoAgents: false as const },
    browserTools: { enabled: false as const },
    providers: {},
    metadataGeneration: { providers: [] },
    autoArchiveAfterMerge: false as const,
    enableTerminalAgentHooks: false as const,
    appendSystemPrompt: "" as const,
  };
  const configStore: DaemonConfigStore = new DaemonConfigStore(
    paseoHome,
    {
      ...initial,
      ...readProfiles(persisted),
      usage: { enabled: persisted.daemon?.usage?.enabled !== false },
      relay: { enabled: false },
      pluginsEnabled: persisted.pluginsEnabled,
      plugins: persisted.plugins,
    },
    logger,
    {
      startupPersisted: persisted,
      relayEnabledMutable: false,
      reloadSource: {
        resolve(next) {
          return {
            mutable: {
              ...configStore.get(),
              ...readProfiles(next),
              usage: { enabled: next.daemon?.usage?.enabled !== false },
            },
            overrideControlledPaths: operatorControlledPaths,
          };
        },
      },
    },
  );

  function readConfig() {
    const { agentProfiles, terminalProfiles, pluginsEnabled } = toWireDaemonConfig(
      configStore.get(),
    );
    return RemoteHostConfigSchema.parse({
      ...initial,
      agentProfiles: agentProfiles ?? [],
      terminalProfiles,
      pluginsEnabled,
    });
  }

  async function readStatus(includeProviders: boolean) {
    const [startedAt, providers] = await Promise.all([
      options.getStartedAt
        ? options.getStartedAt()
        : getPidLockInfo(paseoHome).then((lock) =>
            lock?.pid === process.pid ? lock.startedAt : null,
          ),
      includeProviders ? options.listNativeProviders() : [],
    ]);
    return {
      version: options.daemonVersion ?? null,
      pid: process.pid,
      nodePath: process.execPath,
      startedAt,
      providers: providers.map((provider) => ({ provider, available: true, error: null })),
    };
  }

  return {
    configStore,
    async execute(operation: RemoteHostSettingsOperation) {
      switch (operation.kind) {
        case "hostSettingsGet":
          return readConfig();
        case "hostSettingsPatch": {
          const patch = RemoteHostProfilesPatchSchema.safeParse(operation.config);
          if (!patch.success)
            throw new WorkerOperationError("worker_host_settings_patch_unsupported");
          if (Object.keys(patch.data).length) {
            await configStore.patch((current) => ({
              ...patch.data,
              agentProfiles: patch.data.agentProfiles?.map((profile) => {
                const previous = current.agentProfiles?.find(
                  ({ id, provider }) => profile.id === id && profile.provider === provider,
                );
                if (!previous?.options || profile.options === undefined) return profile;
                const residentOptions = Object.fromEntries(
                  Object.entries(previous.options).filter(
                    ([key]) => !remoteHostProfileControlKeys.includes(key),
                  ),
                );
                return { ...profile, options: { ...residentOptions, ...profile.options } };
              }),
            }));
          }
          return readConfig();
        }
        case "hostSettingsReload":
          return { ...(await configStore.reload()), config: readConfig() };
        case "hostStatus":
          return readStatus(operation.includeProviders !== false);
        case "hostDiagnostics": {
          const [status, disk] = await Promise.all([
            readStatus(true),
            statfs(paseoHome).catch(() => null),
          ]);
          const lines = [
            "Paseo diagnostics",
            `  Daemon version: ${status.version ?? "unknown"}`,
            "Resident process",
            `  PID: ${status.pid}`,
            `  Started at: ${status.startedAt ?? "unknown"}`,
            `  Node: ${process.version}`,
            `  Platform: ${process.platform} ${process.arch}`,
            `  RSS bytes: ${process.memoryUsage.rss()}`,
            `  Available memory bytes: ${freemem()} / ${totalmem()}`,
            `  Paseo volume available bytes: ${disk ? disk.bavail * disk.bsize : "unknown"}`,
            `  Available native providers: ${status.providers.map(({ provider }) => provider).join(", ") || "none"}`,
          ];
          return { diagnostic: lines.join("\n") };
        }
      }
    },
  };
}
