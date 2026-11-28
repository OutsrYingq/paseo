import { z } from "zod";
import { isSameOrDescendantPath } from "../path-utils.js";
import { homedir } from "node:os";
import { randomUUID } from "node:crypto";
import { realpath, stat } from "node:fs/promises";
import path from "node:path";
import { type ChildProcessWithoutNullStreams } from "node:child_process";
import { spawnProcess } from "../../utils/spawn.js";
import { signalProcessTree, terminateWithTreeKill } from "../../utils/tree-kill.js";
import {
  buildCodexAppServerEnv,
  resolveCodexLaunchPrefix,
} from "../agent/providers/codex-app-server-agent.js";
import { readOmpHistoryTodoState, streamOmpHistory } from "../agent/providers/omp/history.js";
import { serializeAgentStreamEvent } from "../messages.js";
import type { AgentStreamEvent, AgentUsageSession } from "../agent/agent-sdk-types.js";
import { buildOmpLaunch } from "../agent/providers/omp/runtime.js";
import { resolveOmpSessionsDir } from "../agent/providers/omp/session-descriptor.js";
import { createResidentOmpImport } from "./resident-omp-import.js";
import { OmpAgentClient } from "../agent/providers/omp/agent.js";
import { createExternalCommandProcessEnv } from "../paseo-env.js";
import type { Logger } from "pino";
import type { ProviderRuntimeSettings } from "../agent/provider-launch-config.js";
import {
  WorkerOperationError,
  type WorkerEvent,
  type NativeLaunch,
  type WorkerOperation,
} from "./execution-protocol.js";
import type { ResidentRuntime } from "./resident-execution.js";
import { openResidentFiles } from "./resident-files.js";
import { openResidentFileTransfer } from "./resident-file-transfer.js";
import {
  changeProjectDirectory,
  inspectResidentProject,
  resolveProjectParent,
} from "./resident-projects.js";
import { openResidentCheckoutQueries } from "./resident-checkout.js";
import { openResidentState } from "./resident-state.js";
import { createResidentTerminals } from "./remote-terminals-resident.js";
import { RemoteTerminalOperationSchema } from "./remote-terminals-protocol.js";
import { PersistedProjectRecordSchema, PersistedWorkspaceRecordSchema } from "../workspace-registry.js";
import { WorkspaceSetupRuntime } from "../workspace-setup-runtime.js";
import {
  materializeResidentWorktree,
  approveResidentWorktreeSetup,
  startResidentWorktreeSetup,
} from "./resident-worktree-provisioning.js";
import { createResidentPluginHost, RemotePluginOperationError } from "./remote-plugins.js";
import type { WorkerBinding } from "./leases.js";
import { NativeTurnOwnership } from "./native-turn-ownership.js";
import { createResidentScripts } from "./resident-scripts.js";
import { createResidentWorkspaceArchive } from "./resident-workspace-archive.js";
import { createResidentHostSettings } from "./resident-host-settings.js";
import { executeResidentProjectConfig } from "./resident-project-config.js";
import { cloneResidentGithubProject, searchResidentGithubRepositories } from "./resident-project-github.js";
import { executeResidentProjectIcon } from "./resident-project-icons.js";

interface OwnedProcess {
  child: ChildProcessWithoutNullStreams;
  owner: symbol;
  launch: NativeLaunch;
  usageSession(): AgentUsageSession;
  turn: NativeTurnOwnership;
  detached: boolean;
  stopping?: Promise<void>;
}

function stopProcess(process: OwnedProcess): Promise<void> {
  return (process.stopping ??= terminateWithTreeKill(process.child, {
    gracefulTimeoutMs: 2_000,
    forceTimeoutMs: 2_000,
  }).then((outcome) => {
    if (outcome === "kill-timeout") throw new Error("worker_process_not_fenced");
    return undefined;
  }));
}

export interface NativeExecutionOptions {
  paseoHome: string;
  worktreesRoot?: string;
  workspaceRoots?: readonly string[];
  plugins?: {
    daemonVersion: string;
    builtinPlugins?: Parameters<typeof createResidentPluginHost>[0]["builtinPlugins"];
    providerSettings?: Parameters<typeof createResidentPluginHost>[0]["providerSettings"];
  };
  logger: Logger;
  providers: Partial<Record<"codex" | "omp", ProviderRuntimeSettings>>;
}

export function createNativeExecution(options: NativeExecutionOptions) {
  const { providers, logger } = options;
  const makeHostSettings = () => createResidentHostSettings({
    paseoHome: options.paseoHome,
    daemonVersion: options.plugins?.daemonVersion,
    logger,
    listNativeProviders,
  });
  let hostSettings = makeHostSettings();
  async function listNativeProviders() {
    const available: Array<"codex" | "omp"> = [];
    if (providers.codex && (await resolveCodexLaunchPrefix(providers.codex).catch(() => null)))
      available.push("codex");
    if (
      providers.omp &&
      (await new OmpAgentClient({ logger, runtimeSettings: providers.omp }).isAvailable())
    )
      available.push("omp");
    return available;
  }
  const terminals = createResidentTerminals({
    operationError: (code) => new WorkerOperationError(code),
  });
  const state = openResidentState(options.paseoHome);
  async function readRecords(path: "projects/projects.json" | "projects/workspaces.json") {
    const text = await (await state)({ kind: "readState", path });
    if (text === null || typeof text === "string") return text;
    throw new WorkerOperationError("worker_registry_read_invalid");
  }
  const workspaceRecords = z.array(PersistedWorkspaceRecordSchema);
  const projectRecords = z.array(PersistedProjectRecordSchema);
  async function listProjects() {
    const text = await readRecords("projects/projects.json");
    return projectRecords.parse(text === null ? [] : JSON.parse(text));
  }
  async function listWorkspaces() {
    const text = await readRecords("projects/workspaces.json");
    return workspaceRecords.parse(text === null ? [] : JSON.parse(text));
  }
  async function getWorkspace(id: string) {
    return (await listWorkspaces()).find((record) => record.workspaceId === id) ?? null;
  }
  const scripts = createResidentScripts({
    paseoHome: options.paseoHome,
    roots: options.workspaceRoots ?? [],
    logger,
    terminalManager: terminals.manager,
    readRecords,
  });
  const processes = new Map<string, OwnedProcess>();
  let stopped = false;
  let pluginHost: ReturnType<typeof createResidentPluginHost> | undefined;
  async function stopRuntime(): Promise<void> {
    await Promise.all([pluginHost?.close(), ...[...processes.values()].map(stopProcess)]);
    pluginHost = undefined;
  }
  function openRuntime(
    publish: (event: WorkerEvent) => void,
    signal: AbortSignal,
    binding: WorkerBinding,
  ): Promise<ResidentRuntime> {
    if (stopped) return Promise.reject(new Error("worker_runtime_stopped"));
    const ompImport = createResidentOmpImport(() =>
      resolveOmpSessionsDir({ runtimeSettings: providers.omp }),
    );
    const owner = Symbol("worker link");
    const setupRuntime = new WorkspaceSetupRuntime();
    const runningSetup = new Set<string>();
    const stopSetup = () => Promise.all([...runningSetup].map((id) => setupRuntime.stop(id)));
    const terminalChannel = terminals.open(publish, signal);
    const archives = createResidentWorkspaceArchive({
      roots: options.workspaceRoots ?? [],
      paseoHome: options.paseoHome,
      worktreesRoot: options.worktreesRoot,
      logger,
      terminalManager: terminals.manager,
      stopWorkspaceSetup: (id) => setupRuntime.stop(id),
      listWorkspaces,
      async getProject(id) {
        return (await listProjects()).find((record) => record.projectId === id) ?? null;
      },
      signal,
    });
    const scriptChannel = scripts.open(publish, signal);
    let plugins: ReturnType<ReturnType<typeof createResidentPluginHost>["openChannel"]> | undefined;
    const histories = new Map<string, AsyncGenerator<AgentStreamEvent>>();
    const files = openResidentFiles(options.workspaceRoots ?? [], publish);
    const transfers = openResidentFileTransfer({
      roots: options.workspaceRoots ?? [],
      paseoHome: options.paseoHome,
    });
    let checkout: ReturnType<typeof openResidentCheckoutQueries> | undefined;
    const checkoutRuntime = () =>
      (checkout ??= openResidentCheckoutQueries({
        roots: options.workspaceRoots ?? [],
        paseoHome: options.paseoHome,
        logger,
        signal,
        publish,
        readMetadataGeneration: () => hostSettings.configStore.get().metadataGeneration,
      }));
    const resolveProcess = (id: string) => {
      const entry = processes.get(id);
      return entry?.owner === owner ? entry : undefined;
    };
    async function executePlugin(operation: Extract<WorkerOperation, { kind: "plugin" }>) {
      if (!options.plugins) {
        if (
          operation.request.action === "providers" ||
          operation.request.action === "list" ||
          operation.request.action === "catalog"
        )
          return { action: operation.request.action, value: [] };
        throw new WorkerOperationError("worker_plugins_unavailable");
      }
      pluginHost ??= createResidentPluginHost({
        ...options.plugins,
        scope: binding,
        paseoHome: options.paseoHome,
        logger,
        configStore: hostSettings.configStore,
        nativeUsage: {
          usageSession(agentId) {
            for (const process of processes.values()) {
              if (!process.detached && process.launch.agentId === agentId) return process.usageSession();
            }
            return null;
          },
          async resolveLaunch(provider) {
            if (provider !== "codex" || !providers.codex) return;
            const prefix = await resolveCodexLaunchPrefix(providers.codex);
            return { ...prefix, env: buildCodexAppServerEnv(providers.codex) };
          },
          async isProviderActive(provider) {
            return (await listNativeProviders()).some((id) => id === provider);
          },
        },
      });
      plugins ??= pluginHost.openChannel({ scope: binding, signal, publish });
      try {
        return await plugins.execute(operation);
      } catch (error) {
        if (error instanceof RemotePluginOperationError)
          throw new WorkerOperationError(error.message);
        throw error;
      }
    }

    async function inspectWorkspacePath(
      operation: Extract<WorkerOperation, { kind: "workspacePath" }>,
    ) {
      if (!path.isAbsolute(operation.cwd) || !path.isAbsolute(operation.parent)) return null;
      try {
        const [cwd, parent] = await Promise.all([
          realpath(operation.cwd),
          realpath(operation.parent),
        ]);
        if (!isSameOrDescendantPath(parent, cwd) || !(await stat(cwd)).isDirectory()) return null;
        return cwd;
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code === "ENOENT") return null;
        throw error;
      }
    }

    async function inspectDirectory(
      operation: Extract<WorkerOperation, { kind: "inspectDirectory" }>,
    ) {
      if (!path.isAbsolute(operation.cwd))
        throw new WorkerOperationError("worker_directory_must_be_absolute");
      try {
        return (await stat(operation.cwd)).isDirectory() ? "directory" : "not_directory";
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code === "ENOENT") return "missing";
        throw error;
      }
    }


    function requireProcessCapacity(id: string) {
      if (processes.has(id) || processes.size >= 64)
        throw new WorkerOperationError("worker_process_limit");
    }

    async function startNativeProcess(operation: Extract<WorkerOperation, { kind: "spawn" }>) {
      const { launch, processId } = operation;
      const settings = providers[launch.provider];
      if (!settings) throw new WorkerOperationError("worker_provider_unavailable");
      requireProcessCapacity(processId);
      let command: string;
      let args: string[];
      let cwd: string | undefined;
      let env: Record<string, string> | undefined;
      if (launch.provider === "codex") {
        const prefix = await resolveCodexLaunchPrefix(settings);
        command = prefix.command;
        args = [...prefix.args, "app-server", ...(launch.goals ? ["--enable", "goals"] : [])];
        env = buildCodexAppServerEnv(settings);
      } else {
        const spec = buildOmpLaunch({
          command: [process.env.OMP_COMMAND ?? "omp"],
          runtimeSettings: settings,
          session: launch,
        });
        [command, ...args] = spec.argv as [string, ...string[]];
        cwd = spec.cwd;
        env = spec.env;
      }
      signal.throwIfAborted();
      requireProcessCapacity(processId);
      const previous =
        launch.agentId &&
        [...processes.values()].find(
          (entry) =>
            entry.launch.provider === launch.provider && entry.launch.agentId === launch.agentId,
        );
      if (previous) {
        if (!previous.detached || previous.turn.mayBeRunning) {
          throw new WorkerOperationError("worker_native_session_busy");
        }
        await stopProcess(previous);
        signal.throwIfAborted();
      }
      if (
        launch.agentId &&
        [...processes.values()].some(
          (entry) =>
            entry !== previous &&
            entry.launch.provider === launch.provider &&
            entry.launch.agentId === launch.agentId,
        )
      )
        throw new WorkerOperationError("worker_native_session_busy");
      const child = spawnProcess(command, args, {
        cwd,
        ...(launch.provider === "codex" ? { baseEnv: env } : { envOverlay: env }),
        detached: launch.provider === "codex" && process.platform !== "win32",
        stdio: ["pipe", "pipe", "pipe"],
      });
      if (!child.stdin || !child.stdout || !child.stderr)
        throw new Error("worker_spawn_without_pipes");
      const native = child as ChildProcessWithoutNullStreams;
      const entry = {
        child: native,
        owner,
        launch,
        usageSession() {
          const effectiveEnv = launch.provider === "codex" ? env ?? process.env : process.env;
          const usageEnv = createExternalCommandProcessEnv(command, effectiveEnv,
            ...(launch.provider === "omp" && env ? [env] : []),
          );
          const values = Object.fromEntries(Object.entries(usageEnv)
            .filter((entry): entry is [string, string] => entry[1] !== undefined));
          return {
            provider: launch.provider,
            model: launch.provider === "omp" ? launch.model : undefined,
            env: values,
            launch: { command, args, env: values },
            sessionKey: processId,
          };
        },
        turn: new NativeTurnOwnership(launch.provider),
        detached: false,
      };
      processes.set(processId, entry);
      for (const stream of ["stdout", "stderr"] as const) {
        native[stream].on("data", (chunk: Buffer) => {
          if (stream === "stdout") entry.turn.output.write(chunk);
          if (!signal.aborted)
            publish({
              type: "output",
              processId,
              stream,
              bytes: chunk.toString("base64"),
            });
        });
      }
      native.stdin.on("error", () => {});
      native.once("close", (code, exitSignal) => {
        processes.delete(processId);
        if (!signal.aborted) publish({ type: "exit", processId, code, signal: exitSignal });
      });
      await new Promise<void>((resolve, reject) => {
        native.once("spawn", resolve);
        native.once("error", reject);
      }).catch(() => {
        throw new WorkerOperationError("worker_spawn_failed");
      });
      return null;
    }

    async function writeNativeProcess(operation: Extract<WorkerOperation, { kind: "stdin" }>) {
      const entry = resolveProcess(operation.processId);
      if (!entry) throw new WorkerOperationError("worker_process_missing");
      const bytes = Buffer.from(operation.bytes, "base64");
      entry.turn.input.write(bytes);
      await new Promise<void>((resolve, reject) => {
        const abort = () => reject(new WorkerOperationError("worker_connection_lost"));
        signal.addEventListener("abort", abort, { once: true });
        entry.child.stdin.write(bytes, (error) => {
          signal.removeEventListener("abort", abort);
          if (error) reject(error);
          else resolve();
        });
      });
      return null;
    }

    async function openOmpHistory(operation: Extract<WorkerOperation, { kind: "ompHistoryOpen" }>) {
      if (
        !providers.omp ||
        histories.size >= 16 ||
        !path.isAbsolute(operation.sessionFile) ||
        path.extname(operation.sessionFile) !== ".jsonl"
      ) {
        throw new WorkerOperationError("worker_history_unavailable");
      }
      const { sessionFile, activeBranchEntryId } = operation;
      const id = randomUUID();
      async function* read() {
        yield* streamOmpHistory({ sessionFile, activeBranchEntryId, provider: "omp" });
        const todo = await readOmpHistoryTodoState(sessionFile);
        if (todo) yield { type: "timeline" as const, provider: "omp", item: todo };
      }
      histories.set(id, read());
      return id;
    }

    async function nextOmpHistory(operation: Extract<WorkerOperation, { kind: "ompHistoryNext" }>) {
      const history = histories.get(operation.id);
      if (!history) throw new WorkerOperationError("worker_history_unavailable");
      const events = [];
      let bytes = 0;
      for (let index = 0; index < 256 && bytes < 512 * 1024; index++) {
        const next = await history.next();
        if (next.done) return { events, done: true };
        const event = serializeAgentStreamEvent(next.value);
        if (!event) continue;
        events.push(event);
        bytes += Buffer.byteLength(JSON.stringify(event));
      }
      return { events, done: false };
    }

    async function executeProcess(operation: Extract<WorkerOperation, { processId: string }>) {
      switch (operation.kind) {
        case "spawn":
          return startNativeProcess(operation);
        case "stdin":
          return writeNativeProcess(operation);
        case "endStdin":
          resolveProcess(operation.processId)?.child.stdin.end();
          return null;
        case "signal": {
          const child = resolveProcess(operation.processId);
          if (child) await signalProcessTree(child.child, operation.signal);
          return null;
        }
      }
    }

    return Promise.resolve({
      workspaceOperations: true,
      async execute(operation) {
        signal.throwIfAborted();
        if ("processId" in operation) return executeProcess(operation);
        switch (operation.kind) {
          case "plugin":
            return executePlugin(operation);
          case "hostSettingsGet":
          case "hostSettingsPatch":
          case "hostSettingsReload":
          case "hostStatus":
          case "hostDiagnostics":
            return hostSettings.execute(operation);
          case "workspaceRefs": {
            return (await listWorkspaces())
              .filter((workspace) => !workspace.archivedAt)
              .map((workspace) => ({ workspaceId: workspace.workspaceId, cwd: workspace.cwd }));
          }
          case "workspacePath":
            return inspectWorkspacePath(operation);
          case "inspectDirectory":
            return inspectDirectory(operation);
          case "ompHistoryOpen":
            return openOmpHistory(operation);
          case "ompHistoryNext":
            return nextOmpHistory(operation);
          case "ompHistoryClose": {
            const history = histories.get(operation.id);
            histories.delete(operation.id);
            await history?.return(undefined);
            return null;
          }
          case "ompImportList":
          case "ompImportResolve":
            if (!providers.omp) throw new WorkerOperationError("worker_provider_unavailable");
            return ompImport(operation);
          case "providerHome":
            return homedir();
          case "inspectProject":
            return inspectResidentProject({
              roots: options.workspaceRoots ?? [],
              cwd: operation.cwd,
              paseoHome: options.paseoHome,
              serverId: binding.serverId,
            });
          case "projectDirectoryParent":
            return resolveProjectParent(options.workspaceRoots ?? [], operation.parentPath);
          case "projectDirectoryChange":
            return changeProjectDirectory(options.workspaceRoots ?? [], operation);
          case "projectGithubSearch":
            return searchResidentGithubRepositories(operation.request);
          case "projectGithubClone":
            return cloneResidentGithubProject({
              roots: options.workspaceRoots ?? [],
              paseoHome: options.paseoHome,
              serverId: binding.serverId,
              logger,
              signal,
            }, operation.request);
          case "projectIconGet":
          case "projectIconSet":
          case "projectIconRemove":
            return executeResidentProjectIcon({
              roots: options.workspaceRoots ?? [],
              paseoHome: options.paseoHome,
              listProjects,
              signal,
            }, operation);
          case "providers":
            return listNativeProviders();
          case "checkoutQuery":
            return checkoutRuntime().query(operation.request);
          case "checkoutAction":
            return checkoutRuntime().action(operation.request);
          case "checkoutMetadata":
            return checkoutRuntime().prepareMetadata(operation);
          case "worktreeMaterialize":
            return materializeResidentWorktree({
              roots: options.workspaceRoots ?? [],
              paseoHome: options.paseoHome,
              worktreesRoot: options.worktreesRoot,
              logger,
            }, operation, signal);
          case "projectConfigRead":
          case "projectConfigWrite":
            return executeResidentProjectConfig(
              { roots: options.workspaceRoots ?? [], listProjects, signal, logger },
              operation,
            );
          case "worktreeApproveSetup":
            return approveResidentWorktreeSetup({
              roots: options.workspaceRoots ?? [],
              getWorkspace,
              listWorkspaces,
              signal,
            }, operation.workspaceId);
          case "worktreeStartSetup":
            return startResidentWorktreeSetup({
              roots: options.workspaceRoots ?? [],
              logger,
              terminalManager: terminals.manager,
              getWorkspace,
              startWorkspaceSetup(id, run) {
                if (signal.aborted || runningSetup.has(id)) return;
                runningSetup.add(id);
                setupRuntime.start(id, async (setupSignal) => {
                  try {
                    await run(AbortSignal.any([setupSignal, signal]));
                  } finally {
                    runningSetup.delete(id);
                  }
                });
              },
              publish,
            }, operation.workspaceId);
          case "workspaceArchivePrepare":
          case "workspaceArchiveRemove":
          case "workspaceRecoveryInspect":
          case "workspaceRecoveryRestore":
            return archives.execute(operation);
          case "diffOpen":
          case "diffStart":
          case "legacyDiffOpen":
          case "diffAck":
          case "diffClose":
            return checkoutRuntime().diff(operation);
          case "explore":
          case "mutateWorkspaceFile":
          case "observeWorkspaceFile":
          case "unobserveWorkspaceFile":
          case "downloadInfo":
          case "downloadOpen":
          case "downloadRead":
          case "downloadClose":
            return files.execute(operation);
          case "uploadOpen":
          case "uploadBegin":
          case "uploadChunk":
          case "uploadEnd":
          case "uploadCancel":
          case "attachmentResolve":
          case "explorerStreamOpen":
          case "explorerStreamNext":
          case "explorerStreamClose":
            return transfers.execute(operation);
          case "scriptList":
          case "scriptStart":
          case "scriptStop":
            return scriptChannel.execute(operation);
          default:
            if ("path" in operation) return (await state)(operation);
            return terminalChannel.execute(RemoteTerminalOperationSchema.parse(operation));
        }
      },
      async restartRuntime(keepTerminals) {
        terminalChannel.dispose();
        scriptChannel.close();
        await transfers.close();
        await stopSetup();
        await Promise.all([stopRuntime(), keepTerminals ? undefined : terminals.killAll()]);
        hostSettings = makeHostSettings();
      },
      async close() {
        terminalChannel.dispose();
        scriptChannel.close();
        await plugins?.close();
        await Promise.allSettled(
          [...histories.values()].map((history) => history.return(undefined)),
        );
        histories.clear();
        await files.close();
        await transfers.close();
        await checkout?.close();
        await stopSetup();
        for (const entry of processes.values()) if (entry.owner === owner) entry.detached = true;
      },
    });
  }
  async function close() {
    stopped = true;
    await scripts.dispose();
    await Promise.all([terminals.dispose(), stopRuntime()]);
  }
  return { openRuntime, close };
}
