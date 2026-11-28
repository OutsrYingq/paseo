import type { SessionInboundMessage, SessionOutboundMessage } from "@getpaseo/protocol/messages";
import { getErrorMessage } from "@getpaseo/protocol/error-utils";
import type { Logger } from "pino";
import { getRelativePathInsideRoot } from "../../utils/path.js";
import { toCheckoutError } from "../checkout-git-utils.js";
import { WorkspaceArchiveOperations } from "../workspace-archive-operations.js";
import {
  archiveWorkspaceContents,
  unarchiveWorkspaceAgents,
  WorkspaceArchiveCanceledError,
  type ArchiveResult,
  type ArchiveScope,
  type ArchiveWorkspaceContentsDependencies,
} from "../workspace-archive-service.js";
import { reconcileWorkspacePlacement } from "../workspace-registry-model.js";
import type {
  PersistedWorkspaceRecord,
  ProjectRegistry,
  WorkspaceRegistry,
} from "../workspace-registry.js";
import {
  remoteWorkspaceArchiveResults,
  type RemoteWorkspaceArchiveOperation,
} from "./remote-workspace-archive-protocol.js";

interface CentralWorkspaceArchiveOptions extends Pick<
  ArchiveWorkspaceContentsDependencies,
  "agentManager" | "agentStorage"
> {
  call(operation: RemoteWorkspaceArchiveOperation): Promise<unknown>;
  workspaceRegistry: Pick<
    WorkspaceRegistry,
    "get" | "listAll" | "archive" | "upsert" | "publishUpsert" | "waitForDeferredPublication"
  >;
  projectRegistry: Pick<ProjectRegistry, "get" | "upsert">;
  unarchiveAgent(agentId: string): Promise<unknown>;
  logger: Logger;
  emitWorkspaceUpdatesForWorkspaceIds?(workspaceIds: Iterable<string>): Promise<void>;
}

const MAX_CONCURRENT_ARCHIVES = 8;
const backing = (record: PersistedWorkspaceRecord) => record.worktreeRoot ?? record.cwd;
const samePath = (left: string, right: string) => getRelativePathInsideRoot(left, right) === "";

export function createCentralWorkspaceArchive(options: CentralWorkspaceArchiveOptions) {
  const { workspaceRegistry, projectRegistry, call, logger } = options;
  const operations = new WorkspaceArchiveOperations();
  let concurrentArchives = 0;

  function restoreAgents(workspaceId: string, archivedAt: string, allowLegacyFallback: boolean) {
    return unarchiveWorkspaceAgents({
      workspaceId,
      archivedAt,
      allowLegacyFallback,
      agentStorage: options.agentStorage,
      unarchiveAgent: options.unarchiveAgent,
      sessionLogger: logger,
    });
  }

  async function findByPath(targetPath: string) {
    const matches = (await workspaceRegistry.listAll()).filter(
      (item) => !item.archivedAt &&
        (samePath(item.cwd, targetPath) || samePath(backing(item), targetPath)),
    );
    return matches.find((item) => item.isPaseoOwnedWorktree) ?? matches[0];
  }

  async function archive(
    scope: ArchiveScope,
    requestId: string,
    { retainCheckout = false }: { retainCheckout?: boolean } = {},
  ): Promise<ArchiveResult & { canceled?: true }> {
    const workspaceId = scope.kind === "workspace"
      ? scope.workspaceId
      : (await findByPath(scope.targetPath))?.workspaceId;
    if (!workspaceId) throw new Error("Workspace not found");
    const outcome = await operations.archive(workspaceId, async (signal) => {
      const initial = await workspaceRegistry.get(workspaceId);
      if (!initial) throw new Error("Workspace not found");
      if (scope.kind === "worktree" && !initial.isPaseoOwnedWorktree)
        throw new Error("Worktree is not a Paseo-owned worktree");
      if (concurrentArchives >= MAX_CONCURRENT_ARCHIVES)
        throw new Error("worker_workspace_archive_busy");
      concurrentArchives++;
      try {
        return await operations.withCheckout(backing(initial), async () => {
          const latest = await workspaceRegistry.get(initial.workspaceId);
          if (!latest || !samePath(backing(latest), backing(initial)))
            throw new Error("Workspace checkout changed");
          const targets =
            scope.kind === "workspace"
              ? latest.archivedAt
                ? []
                : [latest]
              : (await workspaceRegistry.listAll()).filter(
                  (item) => !item.archivedAt && samePath(backing(item), backing(latest)),
                );
          const archivedAt = new Date().toISOString();
          const checkpoint = () => {
            if (signal.aborted) throw new WorkspaceArchiveCanceledError(archivedAt);
          };
          const workspaceIds = targets.map((item) => item.workspaceId);
          const archivedAgentIds = new Set<string>();
          const archivedWorkspaceIds: string[] = [];
          let diskOwner: PersistedWorkspaceRecord | undefined;
          operations.markArchiving(workspaceIds, archivedAt);
          try {
            await options.emitWorkspaceUpdatesForWorkspaceIds?.(workspaceIds);
            for (const target of targets) {
              const { workspaceId } = target;
              checkpoint();
              const { head } = remoteWorkspaceArchiveResults.workspaceArchivePrepare.parse(
                await call({ kind: "workspaceArchivePrepare", workspaceId }),
              );
              checkpoint();
              const contents = await archiveWorkspaceContents(
                {
                  agentManager: options.agentManager,
                  agentStorage: options.agentStorage,
                  sessionLogger: logger,
                  // Preparation already waits for the resident's scoped terminal teardown.
                  killTerminalsForWorkspace: async () => {},
                },
                workspaceId,
                archivedAt,
              );
              try {
                checkpoint();
                if (
                  !(await workspaceRegistry.archive(
                    workspaceId,
                    archivedAt,
                    head ? { head } : undefined,
                  ))
                ) {
                  throw new Error("Workspace archive state changed");
                }
              } catch (error) {
                const latest = await workspaceRegistry.get(workspaceId).catch(() => null);
                if (latest && !latest.archivedAt)
                  await restoreAgents(workspaceId, archivedAt, false);
                throw error;
              }
              archivedWorkspaceIds.push(workspaceId);
              for (const agentId of contents) archivedAgentIds.add(agentId);
              if (!retainCheckout && target.kind === "worktree" && target.isPaseoOwnedWorktree)
                diskOwner ??= target;
            }
            checkpoint();
            const result = { archivedWorkspaceIds, archivedAgentIds: [...archivedAgentIds] };
            if (!diskOwner) return { ...result, removedDirectory: false };
            try {
              return {
                ...result,
                ...remoteWorkspaceArchiveResults.workspaceArchiveRemove.parse(
                  await call({
                    kind: "workspaceArchiveRemove",
                    workspaceId: diskOwner.workspaceId,
                    archivedAt,
                  }),
                ),
              };
            } catch (error) {
              logger.warn(
                { err: error, requestId, workspaceId: diskOwner.workspaceId },
                "Worker did not confirm archive disk removal",
              );
              return {
                ...result,
                removedDirectory: false,
                directoryLeftBehind: {
                  path: backing(diskOwner),
                  reason: "The worker did not confirm checkout removal.",
                },
              };
            }
          } finally {
            operations.clearArchiving(workspaceIds);
            await options.emitWorkspaceUpdatesForWorkspaceIds?.(workspaceIds);
          }
        });
      } finally {
        concurrentArchives--;
      }
    });
    return outcome.kind === "archived"
      ? outcome.result
      : { archivedAgentIds: [], archivedWorkspaceIds: [], removedDirectory: false, canceled: true };
  }

  async function recover(
    workspaceId: string,
    mode: "restore" | "undo",
    canceledArchiveStamp?: string,
  ) {
    const initial = await workspaceRegistry.get(workspaceId);
    if (!initial) throw new Error("Workspace not found");
    await operations.withCheckout(backing(initial), async () => {
      const record = await workspaceRegistry.get(workspaceId);
      if (!record || !samePath(backing(initial), backing(record)))
        throw new Error("Workspace checkout changed");
      if (!record.archivedAt) {
        if (mode === "restore") throw new Error("Workspace is not archived");
        if (canceledArchiveStamp) await restoreAgents(workspaceId, canceledArchiveStamp, false);
        return;
      }
      const project = await projectRegistry.get(record.projectId);
      if (!project) throw new Error("Project not found");
      const restored = remoteWorkspaceArchiveResults.workspaceRecoveryRestore.parse(
        await call({
          kind: "workspaceRecoveryRestore",
          workspaceId,
          archivedAt: record.archivedAt,
        }),
      );
      const updatedAt = new Date().toISOString();
      const placement =
        reconcileWorkspacePlacement({ workspace: record, checkout: restored.checkout, updatedAt })
          ?.workspace ?? record;
      const { untrustedSource: _trust, worktreeManagerPluginId: _manager, ...fields } = placement;
      const next: PersistedWorkspaceRecord = {
        ...fields,
        archivedAt: null,
        updatedAt,
        ...(restored.untrustedSource ? { untrustedSource: restored.untrustedSource } : {}),
        ...(restored.worktreeManagerPluginId
          ? { worktreeManagerPluginId: restored.worktreeManagerPluginId }
          : {}),
      };
      if (project.archivedAt)
        await projectRegistry.upsert({ ...project, archivedAt: null, updatedAt });
      const deferToken = Symbol("remote workspace unarchive");
      const publish = workspaceRegistry.publishUpsert?.bind(workspaceRegistry);
      await workspaceRegistry.upsert(next, publish ? { publish: false, deferToken } : undefined);
      try {
        await restoreAgents(workspaceId, record.archivedAt, !canceledArchiveStamp);
      } finally {
        await publish?.(next, { deferToken });
      }
    });
  }

  async function inspect(workspaceId: string) {
    await workspaceRegistry.waitForDeferredPublication?.(workspaceId);
    return remoteWorkspaceArchiveResults.workspaceRecoveryInspect.parse(
      await call({ kind: "workspaceRecoveryInspect", workspaceId }),
    );
  }

  const restore = (workspaceId: string) =>
    operations.restore(workspaceId, () => recover(workspaceId, "restore"));
  const undo = (workspaceId: string) =>
    operations.undo(workspaceId, ({ archivedAt }) => recover(workspaceId, "undo", archivedAt));

  function dispatch(request: SessionInboundMessage): Promise<SessionOutboundMessage> | undefined {
    switch (request.type) {
      case "archive_workspace_request": {
        const { requestId, workspaceId } = request;
        return (async () => {
          let archivedAt: string | null = null;
          let error: string | null = null;
          let canceled: true | undefined;
          try {
            const result = await archive({ kind: "workspace", workspaceId }, requestId);
            canceled = result.canceled;
            if (canceled) error = "Workspace archive was undone";
            else archivedAt = (await workspaceRegistry.get(workspaceId))?.archivedAt ?? null;
          } catch (cause) {
            error = getErrorMessage(cause);
          }
          return {
            type: "archive_workspace_response",
            payload: { requestId, workspaceId, archivedAt, error, ...(canceled ? { canceled } : {}) },
          };
        })();
      }
      case "paseo_worktree_archive_request": {
        const { requestId, workspaceId, worktreePath, scope } = request;
        return (async () => {
          try {
            if (scope === "worktree" && !worktreePath)
              throw new Error("Worktree path is required");
            const id = workspaceId ??
              (worktreePath ? (await findByPath(worktreePath))?.workspaceId : undefined);
            const result = scope === "worktree" && worktreePath
              ? await archive({ kind: "worktree", targetPath: worktreePath }, requestId)
              : id ? await archive({ kind: "workspace", workspaceId: id }, requestId) : undefined;
            return {
              type: "paseo_worktree_archive_response",
              payload: { requestId, success: true, removedAgents: result?.archivedAgentIds ?? [], error: null },
            };
          } catch (cause) {
            return {
              type: "paseo_worktree_archive_response",
              payload: { requestId, success: false, removedAgents: [], error: toCheckoutError(cause) },
            };
          }
        })();
      }
      case "workspace.recovery.inspect.request":
        return inspect(request.workspaceId).then((state) => ({
          type: "workspace.recovery.inspect.response",
          payload: { requestId: request.requestId, state },
        }));
      case "workspace.recovery.restore.request":
      case "workspace.archive.undo.request": {
        const { requestId, workspaceId } = request;
        const isUndo = request.type === "workspace.archive.undo.request";
        return (async () => {
          let error: string | null = null;
          try {
            await (isUndo ? undo(workspaceId) : restore(workspaceId));
          } catch (cause) {
            error = getErrorMessage(cause);
          }
          return {
            type: isUndo ? "workspace.archive.undo.response" : "workspace.recovery.restore.response",
            payload: { requestId, workspaceId, accepted: error === null, error },
          };
        })();
      }
      default:
        return undefined;
    }
  }

  return {
    operations,
    archive,
    dispatch,
    inspect,
    restore,
    undo,
  };
}
