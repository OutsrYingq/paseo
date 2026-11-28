import { getErrorMessage } from "@getpaseo/protocol/error-utils";
import type { SessionInboundMessage, SessionOutboundMessage } from "@getpaseo/protocol/messages";
import type { Logger } from "pino";
import { getRelativePathInsideRoot } from "../../utils/path.js";
import type {
  PersistedProjectRecord,
  PersistedWorkspaceRecord,
  ProjectRegistry,
  WorkspaceRegistry,
} from "../workspace-registry.js";

type Request<T extends SessionInboundMessage["type"]> = Extract<SessionInboundMessage, { type: T }>;
type Response<T extends SessionOutboundMessage["type"]> = Extract<
  SessionOutboundMessage,
  { type: T }
>;

type ReadConfig = Request<"read_project_config_request">;
type WriteConfig = Request<"write_project_config_request">;

interface CentralProjectManagementOptions {
  projectRegistry: Pick<ProjectRegistry, "get" | "list" | "update" | "remove">;
  workspaceRegistry: Pick<WorkspaceRegistry, "get" | "listAll" | "listActive">;
  archiveWorkspaceForProjectRemoval(workspaceId: string, requestId: string): Promise<unknown>;
  restoreWorkspaceForOpen(workspaceId: string): Promise<void>;
  projectConfig?: {
    read(request: ReadConfig): Promise<Response<"read_project_config_response">>;
    write(request: WriteConfig): Promise<Response<"write_project_config_response">>;
    resolveRoot?(path: string): Promise<string | null>;
  };
  projectIcons?: {
    get(
      project: PersistedProjectRecord,
    ): Promise<Response<"project.icon.get.response">["payload"]["icon"]>;
    set(
      project: PersistedProjectRecord,
      source: Request<"project.icon.set.request">["source"],
    ): Promise<unknown>;
    remove?(project: PersistedProjectRecord): Promise<void>;
  };
  logger?: Pick<Logger, "warn">;
}

const MAX_REMOVAL_RECEIPTS = 128;
const samePath = (left: string, right: string) => getRelativePathInsideRoot(left, right) === "";

export function createCentralProjectManagement(options: CentralProjectManagementOptions) {
  const { projectRegistry, workspaceRegistry, projectConfig, projectIcons } = options;
  const projectMutations = new Map<string, Promise<unknown>>();
  // Empty projects have no workspace records to prove ownership after removal.
  const removalReceipts = new Map<string, string[]>();

  async function withProjectMutation<T>(projectId: string, mutation: () => Promise<T>): Promise<T> {
    const previous = projectMutations.get(projectId) ?? Promise.resolve();
    const current = previous.catch(() => undefined).then(mutation);
    projectMutations.set(projectId, current);
    try {
      return await current;
    } finally {
      if (projectMutations.get(projectId) === current) projectMutations.delete(projectId);
    }
  }

  function rememberRemoval(projectId: string, workspaceIds: string[]) {
    removalReceipts.delete(projectId);
    removalReceipts.set(projectId, workspaceIds);
    if (removalReceipts.size > MAX_REMOVAL_RECEIPTS) {
      const oldestId = removalReceipts.keys().next().value;
      if (oldestId !== undefined) removalReceipts.delete(oldestId);
    }
  }

  async function requireProject(projectId: string) {
    const project = await projectRegistry.get(projectId);
    if (!project) throw new Error("Project not found");
    return project;
  }

  async function rename(
    request: Request<"project.rename.request">,
  ): Promise<Response<"project.rename.response">> {
    const { projectId, requestId } = request;
    try {
      const customName = request.customName?.trim() || null;
      await withProjectMutation(projectId, async () => {
        const project = await requireProject(projectId);
        if (project.customName === customName) return;
        const updated = await projectRegistry.update(projectId, (record) => ({
          ...record,
          customName,
          updatedAt: new Date().toISOString(),
        }));
        if (!updated) throw new Error("Project not found");
      });
      return {
        type: "project.rename.response",
        payload: { projectId, requestId, accepted: true, customName, error: null },
      };
    } catch (error) {
      return {
        type: "project.rename.response",
        payload: {
          projectId,
          requestId,
          accepted: false,
          customName: null,
          error: getErrorMessage(error),
        },
      };
    }
  }

  async function remove(
    request: Request<"project.remove.request">,
  ): Promise<Response<"project.remove.response">> {
    const { projectId, requestId } = request;
    try {
      const removedWorkspaceIds = await withProjectMutation(projectId, async () => {
        const project = await projectRegistry.get(projectId);
        const projectWorkspaces = (await workspaceRegistry.listAll()).filter(
          (workspace) => workspace.projectId === projectId,
        );
        if (!project) {
          if (projectWorkspaces.some((workspace) => !workspace.archivedAt))
            throw new Error("Project not found");
          const receipt = removalReceipts.get(projectId);
          if (receipt) return receipt;
          if (projectWorkspaces.length) return [];
          throw new Error("Project not found");
        }
        const archived: string[] = [];
        for (const workspace of projectWorkspaces) {
          if (workspace.archivedAt) continue;
          await options.archiveWorkspaceForProjectRemoval(workspace.workspaceId, requestId);
          const current = await workspaceRegistry.get(workspace.workspaceId);
          if (!current?.archivedAt) throw new Error("Workspace archive did not complete");
          archived.push(workspace.workspaceId);
        }
        if (
          (await workspaceRegistry.listActive()).some(
            (workspace) => workspace.projectId === projectId,
          )
        ) {
          throw new Error("Project still has active workspaces");
        }
        // Keep the receipt if removal persists and its caller loses the acknowledgement.
        rememberRemoval(projectId, archived);
        await projectRegistry.remove(projectId);
        await projectIcons?.remove?.(project).catch((error: unknown) => {
          options.logger?.warn(
            { err: error, projectId },
            "Failed to clean up removed project icon",
          );
        });
        return archived;
      });
      return {
        type: "project.remove.response",
        payload: { projectId, requestId, accepted: true, removedWorkspaceIds, error: null },
      };
    } catch (error) {
      return {
        type: "project.remove.response",
        payload: {
          projectId,
          requestId,
          accepted: false,
          removedWorkspaceIds: [],
          error: getErrorMessage(error),
        },
      };
    }
  }

  async function config(
    request: ReadConfig | WriteConfig,
  ): Promise<Response<"read_project_config_response" | "write_project_config_response">> {
    const type =
      request.type === "read_project_config_request"
        ? "read_project_config_response"
        : "write_project_config_response";
    const { repoRoot, requestId } = request;
    const failure = (
      code: "project_not_found" | "write_failed",
    ): Response<"read_project_config_response" | "write_project_config_response"> => ({
      type,
      payload: { repoRoot, requestId, ok: false as const, error: { code } },
    });
    try {
      const activeProjects = (await projectRegistry.list()).filter(
        (project) => !project.archivedAt,
      );
      let known = activeProjects.find((project) => samePath(project.rootPath, repoRoot));
      if (!known && projectConfig?.resolveRoot) {
        const canonicalRoot = await projectConfig.resolveRoot(repoRoot).catch((error: unknown) => {
          if (
            [
              "worker_workspace_root_denied",
              "worker_directory_must_be_absolute",
              "ENOENT",
              "ENOTDIR",
            ].includes(getErrorMessage(error))
          )
            return null;
          throw error;
        });
        if (canonicalRoot !== null)
          known = activeProjects.find((project) => samePath(project.rootPath, canonicalRoot));
      }
      if (!known) return failure("project_not_found");
      if (!projectConfig) return failure("write_failed");
      const { projectId, rootPath } = known;
      return await withProjectMutation(projectId, async () => {
        const current = await projectRegistry.get(projectId);
        if (!current || current.archivedAt || !samePath(current.rootPath, rootPath))
          return failure("project_not_found");
        return request.type === "read_project_config_request"
          ? projectConfig.read({ ...request, repoRoot: current.rootPath })
          : projectConfig.write({ ...request, repoRoot: current.rootPath });
      });
    } catch (error) {
      options.logger?.warn({ err: error, requestId }, "Project config operation failed");
      return failure("write_failed");
    }
  }

  async function getIcon(
    request: Request<"project.icon.get.request">,
  ): Promise<Response<"project.icon.get.response">> {
    const { projectId, requestId } = request;
    try {
      const project = await requireProject(projectId);
      if (!projectIcons) throw new Error("Project icons are unavailable on this host");
      return {
        type: "project.icon.get.response",
        payload: { projectId, requestId, icon: await projectIcons.get(project), error: null },
      };
    } catch (error) {
      return {
        type: "project.icon.get.response",
        payload: { projectId, requestId, icon: null, error: getErrorMessage(error) },
      };
    }
  }

  async function setIcon(
    request: Request<"project.icon.set.request">,
  ): Promise<Response<"project.icon.set.response">> {
    const { projectId, requestId } = request;
    try {
      await withProjectMutation(projectId, async () => {
        const project = await requireProject(projectId);
        if (!projectIcons) throw new Error("Project icons are unavailable on this host");
        await projectIcons.set(project, request.source);
      });
      return {
        type: "project.icon.set.response",
        payload: { projectId, requestId, accepted: true, error: null },
      };
    } catch (error) {
      return {
        type: "project.icon.set.response",
        payload: { projectId, requestId, accepted: false, error: getErrorMessage(error) },
      };
    }
  }

  async function resolveOpenWorkspace(
    canonicalCwd: string,
  ): Promise<PersistedWorkspaceRecord | null> {
    const candidates = (await workspaceRegistry.listAll())
      .filter((workspace) => !workspace.background && samePath(workspace.cwd, canonicalCwd))
      .sort(
        (left, right) =>
          Date.parse(left.createdAt) - Date.parse(right.createdAt) ||
          left.workspaceId.localeCompare(right.workspaceId),
      );
    const candidate = candidates.find((workspace) => !workspace.archivedAt) ?? candidates[0];
    if (!candidate) return null;
    return withProjectMutation(candidate.projectId, async () => {
      const project = await projectRegistry.get(candidate.projectId);
      const workspace = await workspaceRegistry.get(candidate.workspaceId);
      if (
        !project ||
        project.archivedAt ||
        !workspace ||
        workspace.projectId !== project.projectId ||
        !samePath(workspace.cwd, canonicalCwd)
      )
        return null;
      if (!workspace.archivedAt) return workspace;
      await options.restoreWorkspaceForOpen(workspace.workspaceId);
      const restored = await workspaceRegistry.get(workspace.workspaceId);
      if (!restored || restored.archivedAt) throw new Error("Workspace restore did not complete");
      return restored;
    });
  }

  function dispatch(request: SessionInboundMessage): Promise<SessionOutboundMessage> | undefined {
    switch (request.type) {
      case "project.rename.request":
        return rename(request);
      case "project.remove.request":
        return remove(request);
      case "read_project_config_request":
      case "write_project_config_request":
        return config(request);
      case "project.icon.get.request":
        return getIcon(request);
      case "project.icon.set.request":
        return setIcon(request);
    }
  }

  return { dispatch, resolveOpenWorkspace, withProjectMutation };
}
