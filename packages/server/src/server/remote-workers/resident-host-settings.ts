import { statfs } from "node:fs/promises";
import { freemem, totalmem } from "node:os";
import type { Logger } from "pino";
import { DaemonConfigStore, toWireDaemonConfig } from "../daemon-config-store.js";
import { loadPersistedConfig, type PersistedConfig } from "../persisted-config.js";
import { getPidLockInfo } from "../pid-lock.js";
import {
  RemoteHostConfigSchema,
  RemoteHostConfigPatchSchema,
  remoteHostProfileControlKeys,
  type RemoteHostSettingsOperation,
} from "./remote-host-settings-protocol.js";
import { WorkerOperationError } from "./execution-protocol.js";
import type { SpeechSettingsOwner } from "../speech/speech-settings.js";

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
  speech?: SpeechSettingsOwner;
}) {
  const { paseoHome, logger } = options;
  const persisted = loadPersistedConfig(paseoHome, logger);
  const initial = {
    mcp: { injectIntoAgents: false as const },
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
      browserTools: { enabled: persisted.daemon?.browserTools?.enabled === true },
      metadataGeneration: {
        providers: persisted.agents?.metadataGeneration?.providers ?? [],
        instructions: persisted.agents?.metadataGeneration?.instructions,
      },
      relay: { enabled: false },
      pluginsEnabled: persisted.pluginsEnabled,
      plugins: persisted.plugins,
    },
    logger,
    {
      startupPersisted: persisted,
      relayEnabledMutable: false,
      speech: options.speech,
      reloadSource: {
        resolve(next) {
          return {
            mutable: {
              ...configStore.get(),
              ...readProfiles(next),
              usage: { enabled: next.daemon?.usage?.enabled !== false },
              browserTools: { enabled: next.daemon?.browserTools?.enabled === true },
              metadataGeneration: {
                providers: next.agents?.metadataGeneration?.providers ?? [],
                instructions: next.agents?.metadataGeneration?.instructions,
              },
            },
            overrideControlledPaths: operatorControlledPaths,
          };
        },
      },
    },
  );

  function readConfig(view: { browserSettings?: true; speechSettings?: true }) {
    const { agentProfiles, terminalProfiles, pluginsEnabled, usage, metadataGeneration, features } =
      toWireDaemonConfig(configStore.get());
    return RemoteHostConfigSchema.parse({
      ...initial,
      agentProfiles: agentProfiles ?? [],
      terminalProfiles,
      pluginsEnabled,
      usage,
      metadataGeneration,
      browserTools: { enabled: view.browserSettings === true && configStore.get().browserTools.enabled === true },
      features: view.speechSettings ? features : undefined,
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
          return readConfig(operation);
        case "hostSettingsPatch": {
          const patch = RemoteHostConfigPatchSchema.safeParse(operation.config);
          if (!patch.success || (patch.data.browserTools && !operation.browserSettings) ||
            (patch.data.features && (!operation.speechSettings || !options.speech)))
            throw new WorkerOperationError("worker_host_settings_patch_unsupported");
          const speech = patch.data.features;
          for (const site of [speech?.dictation?.stt, speech?.voiceMode?.stt, speech?.voiceMode?.tts]) {
            if (site && Object.hasOwn(site, "apiKey")) {
              throw new WorkerOperationError("worker_speech_credentials_managed_by_operator");
            }
          }
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
          return readConfig(operation);
        }
        case "hostSettingsReload":
          return { ...(await configStore.reload()), config: readConfig(operation) };
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
