import { stat } from "node:fs/promises";
import type { Logger } from "pino";
import { z } from "zod";
import { getCurrentBranch } from "../../utils/checkout-git.js";
import type { TerminalManager } from "../../terminal/terminal-manager.js";
import { loadPersistedConfigAsync } from "../persisted-config.js";
import { ScriptHealthMonitor } from "../script-health-monitor.js";
import { createServiceProxySubsystem } from "../service-proxy.js";
import { createWorkspaceScriptsService } from "../session/workspace-scripts/workspace-scripts-service.js";
import { assertWorkspaceAutomationAllowedForWorkspace } from "../workspace-automation-gate.js";
import { PersistedProjectRecordSchema, PersistedWorkspaceRecordSchema } from "../workspace-registry.js";
import { WorkspaceScriptRuntimeStore } from "../workspace-script-runtime-store.js";
import { spawnWorkspaceScript } from "../worktree-bootstrap.js";
import { WorkerOperationError } from "./execution-protocol.js";
import type { RemoteScriptEvent, RemoteScriptOperation } from "./remote-scripts-protocol.js";
import { resolveResidentWorkspacePath } from "./resident-projects.js";

const workspacesFile = "projects/workspaces.json";
const projectsFile = "projects/projects.json";
const workspacesSchema = z.array(PersistedWorkspaceRecordSchema);
const projectsSchema = z.array(PersistedProjectRecordSchema);

interface ResidentScriptsOptions {
  paseoHome: string;
  roots: readonly string[];
  logger: Logger;
  terminalManager: TerminalManager;
  readRecords(file: typeof workspacesFile | typeof projectsFile): Promise<string | null>;
}

export function createResidentScripts(options: ResidentScriptsOptions) {
  const { logger, paseoHome, roots, terminalManager, readRecords } = options;
  const serviceProxy = createServiceProxySubsystem({ logger });
  const scriptRuntimeStore = new WorkspaceScriptRuntimeStore();
  let publish: ((event: RemoteScriptEvent) => void) | undefined;
  let disposed = false;
  const pending = new Map<string, Promise<unknown>>();

  async function getWorkspaceRecord(workspaceId: string) {
    // Central writes these records atomically. Reload to observe registration, archive and trust
    // changes even when the worker's native runtime survives a Central or link restart.
    const records = await readRecords(workspacesFile);
    return workspacesSchema.parse(records === null ? [] : JSON.parse(records)).find(
      (workspace) => workspace.workspaceId === workspaceId && !workspace.archivedAt,
    ) ?? null;
  }

  const health = new ScriptHealthMonitor({
    serviceProxy,
    onChange(workspaceId) {
      if (publish) void scripts.emitStatusUpdate(workspaceId, "");
    },
  });
  const scripts = createWorkspaceScriptsService({
    serviceProxy,
    scriptRuntimeStore,
    terminalManager,
    workspaceRegistry: {
      async get(workspaceId) {
        const workspace = await getWorkspaceRecord(workspaceId);
        if (!workspace) return null;
        const cwd = await resolveResidentWorkspacePath(roots, workspace.cwd);
        if (!(await stat(cwd)).isDirectory()) {
          throw new WorkerOperationError("worker_directory_not_directory");
        }
        const branch = (await getCurrentBranch(cwd, { paseoHome, logger })) ?? workspace.branch;
        return { ...workspace, cwd, branch };
      },
    },
    projectRegistry: {
      async get(projectId) {
        const records = await readRecords(projectsFile);
        return projectsSchema.parse(records === null ? [] : JSON.parse(records)).find(
          (project) => project.projectId === projectId,
        ) ?? null;
      },
    },
    workspaceRuntimeStore: { get: () => null },
    getDaemonTcpPort: null,
    getDaemonTcpHost: null,
    serviceProxyPublicBaseUrl: null,
    resolveScriptHealth: (hostname) => health.getHealthForHostname(hostname),
    async getGlobalServicePorts() {
      return (await loadPersistedConfigAsync(paseoHome, logger)).worktrees?.servicePorts;
    },
    logger,
    emit: () => {},
    publishStatusUpdate: ({ payload }) => publish?.({ type: "scriptStatus", payload }),
    async spawnWorkspaceScript(input) {
      const result = await spawnWorkspaceScript(input);
      if (!disposed && result.port !== null) health.start();
      return result;
    },
    assertAutomationAllowed: (workspaceId) =>
      assertWorkspaceAutomationAllowedForWorkspace({ get: getWorkspaceRecord }, workspaceId),
  });

  return {
    open(deliver: (event: RemoteScriptEvent) => void, signal: AbortSignal) {
      signal.throwIfAborted();
      if (disposed || publish) throw new WorkerOperationError("worker_script_channel_unavailable");
      publish = deliver;
      let closed = false;
      function close() {
        if (closed) return;
        closed = true;
        signal.removeEventListener("abort", close);
        if (publish === deliver) publish = undefined;
      }
      signal.addEventListener("abort", close, { once: true });
      return {
        async execute(operation: RemoteScriptOperation) {
          signal.throwIfAborted();
          if (disposed || closed) {
            throw new WorkerOperationError("worker_script_channel_unavailable");
          }
          try {
            if (operation.kind === "scriptList") return await scripts.list(operation.workspaceId);
            const key = JSON.stringify([operation.workspaceId, operation.scriptName]);
            if (pending.has(key)) throw new WorkerOperationError("worker_script_busy");
            const action = operation.kind === "scriptStart" ? scripts.launch : scripts.stop;
            const task = action(operation);
            pending.set(key, task);
            try {
              return await task;
            } finally {
              pending.delete(key);
            }
          } catch (error) {
            if (error instanceof WorkerOperationError || !(error instanceof Error)) throw error;
            throw new WorkerOperationError(error.message, { cause: error });
          }
        },
        close,
      };
    },
    async dispose() {
      disposed = true;
      publish = undefined;
      health.stop();
      await Promise.allSettled(pending.values());
    },
  };
}
