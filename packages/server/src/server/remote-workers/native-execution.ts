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
import type { AgentStreamEvent } from "../agent/agent-sdk-types.js";
import { buildOmpLaunch } from "../agent/providers/omp/runtime.js";
import { OmpAgentClient } from "../agent/providers/omp/agent.js";
import type { Logger } from "pino";
import type { ProviderRuntimeSettings } from "../agent/provider-launch-config.js";
import { WorkerOperationError, type WorkerEvent, type NativeLaunch } from "./execution-protocol.js";
import type { ResidentRuntime } from "./resident-execution.js";
import { openResidentFiles } from "./resident-files.js";
import { openResidentState } from "./resident-state.js";
import { createResidentTerminals } from "./remote-terminals-resident.js";
import { RemoteTerminalOperationSchema } from "./remote-terminals-protocol.js";
import { PersistedWorkspaceRecordSchema } from "../workspace-registry.js";
import { createResidentPluginHost, RemotePluginOperationError } from "./remote-plugins.js";
import type { WorkerBinding } from "./leases.js";
import { NativeTurnOwnership } from "./native-turn-ownership.js";

type OwnedProcess = {
  child: ChildProcessWithoutNullStreams;
  owner: symbol;
  launch: NativeLaunch;
  turn: NativeTurnOwnership;
  detached: boolean;
  stopping?: Promise<void>;
};

function stopProcess(process: OwnedProcess): Promise<void> {
  return (process.stopping ??= terminateWithTreeKill(process.child, {
    gracefulTimeoutMs: 2_000,
    forceTimeoutMs: 2_000,
  }).then((outcome) => {
    if (outcome === "kill-timeout") throw new Error("worker_process_not_fenced");
  }));
}

export interface NativeExecutionOptions {
  paseoHome: string;
  workspaceRoots?: readonly string[];
  plugins?: {
    daemonVersion: string;
    providerSettings?: Parameters<typeof createResidentPluginHost>[0]["providerSettings"];
  };
  logger: Logger;
  providers: Partial<Record<"codex" | "omp", ProviderRuntimeSettings>>;
}

export function createNativeExecution(options: NativeExecutionOptions) {
  const { providers, logger } = options;
  const terminals = createResidentTerminals({
    operationError: (code) => new WorkerOperationError(code),
  });
  const state = openResidentState(options.paseoHome);
  const processes = new Map<string, OwnedProcess>();
  let stopped = false;
  let pluginHost: ReturnType<typeof createResidentPluginHost> | undefined;
  function openRuntime(
    publish: (event: WorkerEvent) => void,
    signal: AbortSignal,
    binding: WorkerBinding,
  ): Promise<ResidentRuntime> {
    if (stopped) return Promise.reject(new Error("worker_runtime_stopped"));
    const owner = Symbol("worker link");
    const terminalChannel = terminals.open(publish, signal);
    let plugins: ReturnType<ReturnType<typeof createResidentPluginHost>["openChannel"]> | undefined;
    const histories = new Map<string, AsyncGenerator<AgentStreamEvent>>();
    const files = openResidentFiles(options.workspaceRoots ?? []);
    const resolveProcess = (id: string) => {
      const entry = processes.get(id);
      return entry?.owner === owner ? entry : undefined;
    };
    return Promise.resolve({
      async execute(operation) {
        signal.throwIfAborted();
        switch (operation.kind) {
          case "plugin": {
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
          case "workspaceRefs": {
            const text = await (
              await state
            )({ kind: "readState", path: "projects/workspaces.json" });
            return z
              .array(PersistedWorkspaceRecordSchema)
              .parse(typeof text === "string" ? JSON.parse(text) : [])
              .filter((workspace) => !workspace.archivedAt)
              .map((workspace) => ({ workspaceId: workspace.workspaceId, cwd: workspace.cwd }));
          }
          case "workspacePath": {
            if (!path.isAbsolute(operation.cwd) || !path.isAbsolute(operation.parent)) return null;
            try {
              const [cwd, parent] = await Promise.all([
                realpath(operation.cwd),
                realpath(operation.parent),
              ]);
              if (!isSameOrDescendantPath(parent, cwd) || !(await stat(cwd)).isDirectory())
                return null;
              return cwd;
            } catch (error) {
              if ((error as NodeJS.ErrnoException).code === "ENOENT") return null;
              throw error;
            }
          }
          case "inspectDirectory": {
            if (!path.isAbsolute(operation.cwd))
              throw new WorkerOperationError("worker_directory_must_be_absolute");
            try {
              return (await stat(operation.cwd)).isDirectory() ? "directory" : "not_directory";
            } catch (error) {
              if ((error as NodeJS.ErrnoException).code === "ENOENT") return "missing";
              throw error;
            }
          }
          case "ompHistoryOpen": {
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
          case "ompHistoryNext": {
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
          case "ompHistoryClose": {
            const history = histories.get(operation.id);
            histories.delete(operation.id);
            await history?.return(undefined);
            return null;
          }
          case "providerHome":
            return homedir();
          case "providers": {
            const available: Array<"codex" | "omp"> = [];
            if (
              providers.codex &&
              (await resolveCodexLaunchPrefix(providers.codex).catch(() => null))
            )
              available.push("codex");
            if (
              providers.omp &&
              (await new OmpAgentClient({
                logger,
                runtimeSettings: providers.omp,
              }).isAvailable())
            )
              available.push("omp");
            return available;
          }
          case "spawn": {
            const { launch, processId } = operation;
            const settings = providers[launch.provider];
            if (!settings) throw new WorkerOperationError("worker_provider_unavailable");
            if (processes.has(processId) || processes.size >= 64)
              throw new WorkerOperationError("worker_process_limit");
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
            if (processes.has(processId) || processes.size >= 64)
              throw new WorkerOperationError("worker_process_limit");
            const previous =
              launch.agentId &&
              [...processes.values()].find(
                (entry) =>
                  entry.launch.provider === launch.provider &&
                  entry.launch.agentId === launch.agentId,
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
          case "stdin": {
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
          case "endStdin":
            resolveProcess(operation.processId)?.child.stdin.end();
            return null;
          case "signal": {
            const child = resolveProcess(operation.processId);
            if (child) await signalProcessTree(child.child, operation.signal);
            return null;
          }
          case "explore":
          case "downloadInfo":
          case "downloadOpen":
          case "downloadRead":
          case "downloadClose":
            return files.execute(operation);
          default:
            if ("path" in operation) return (await state)(operation);
            return terminalChannel.execute(RemoteTerminalOperationSchema.parse(operation));
        }
      },
      async close() {
        terminalChannel.dispose();
        await plugins?.close();
        await Promise.allSettled(
          [...histories.values()].map((history) => history.return(undefined)),
        );
        histories.clear();
        await files.close();
        for (const entry of processes.values()) if (entry.owner === owner) entry.detached = true;
      },
    });
  }
  async function close() {
    stopped = true;
    await Promise.all([
      terminals.dispose(),
      pluginHost?.close(),
      ...[...processes.values()].map(stopProcess),
    ]);
  }
  return { openRuntime, close };
}
