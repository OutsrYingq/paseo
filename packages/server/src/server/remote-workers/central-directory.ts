import type {
  WorkspaceCreateRequest,
  WorkspaceDescriptorPayload,
} from "@getpaseo/protocol/messages";
import { resolveFirstAgentPromptTitle } from "../agent/create-agent-title.js";
import type { WorkerConnection } from "./worker-connection.js";
import { createAgentDirectoryPager } from "../agent/directory-pager.js";
import { CursorError } from "../pagination/cursor.js";
import { isDeepStrictEqual } from "node:util";
import { randomUUID } from "node:crypto";
import { checkoutFromPersistedWorkspacePlacement } from "../workspace-registry-model.js";
import { z } from "zod";
import type { Logger } from "pino";
import type { SessionInboundMessage, SessionOutboundMessage } from "@getpaseo/protocol/messages";
import { DirectorySyncService } from "../directory-sync/index.js";
import { matchesAgentHistoryQuery } from "../agent-history-search.js";
import { WorkspaceDirectory } from "../workspace-directory.js";
import { describeProject } from "../project-descriptor.js";
import { registerProjectDirectory } from "../project-registration.js";
import { createProjectDirectory, ProjectDirectoryRequestError } from "../project-directory-service.js";
import { generateWorkspaceId, initialWorkspacePlacement } from "../workspace-registry-model.js";
import { areEquivalentPaths } from "../../utils/path.js";
import { createImportWorkspacePlacement } from "../session/agent-history/import-workspace-placement.js";
import pLimit from "p-limit";
import { getErrorMessage } from "@getpaseo/protocol/error-utils";
import { matchesAgentUpdatesFilter } from "../session/agent-updates/agent-updates-service.js";
import {
  PersistedProjectRecordSchema,
  PersistedWorkspaceRecordSchema,
  FileBackedProjectRegistry,
  FileBackedWorkspaceRegistry,
  createPersistedWorkspaceRecord,
  resolveProjectDisplayName,
  resolveWorkspaceDisplayName,
} from "../workspace-registry.js";
import type { openCentralAgents } from "./central-agents.js";
import type { createRemoteState } from "./remote-state.js";
import type { createRemoteScripts } from "./remote-scripts.js";
import { WorkerOperationError } from "./execution-protocol.js";
import { createCentralProjectManagement } from "./central-project-management.js";

type Request = Extract<
  SessionInboundMessage,
  {
    type:
      | "project.list.request"
      | "fetch_workspaces_request"
      | "fetch_agents_request"
      | "fetch_agent_history_request";
  }
>;

export function createCentralDirectory(
  logger: Logger,
  state: ReturnType<typeof createRemoteState>,
  agents: Awaited<ReturnType<typeof openCentralAgents>>,
  connection: WorkerConnection,
  scripts: Pick<ReturnType<typeof createRemoteScripts>, "list">,
) {
  const sync = new DirectorySyncService();
  const agentPager = createAgentDirectoryPager();
  const workspaceRegistry = new FileBackedWorkspaceRegistry("projects/workspaces.json", logger, {
    readRecords: state.read,
    writeRecords: (file, records) => state.write(file, JSON.stringify(records)),
  });
  const projectRegistry = new FileBackedProjectRegistry("projects/projects.json", logger, {
    readRecords: state.read,
    writeRecords: (file, records) => state.write(file, JSON.stringify(records)),
  });
  const archiveListeners = new Set<() => void>();
  const archive = agents.createArchiveSession({
    call: (operation) => connection.call(operation),
    workspaceRegistry,
    projectRegistry,
    async emitWorkspaceUpdatesForWorkspaceIds() {
      for (const notify of archiveListeners) notify();
    },
  });
  const projectManagement = createCentralProjectManagement({
    projectRegistry,
    workspaceRegistry,
    archiveWorkspaceForProjectRemoval: (workspaceId, requestId) =>
      archive.archive({ kind: "workspace", workspaceId }, requestId, { retainCheckout: true }),
    restoreWorkspaceForOpen: archive.restore,
    projectConfig: {
      read: (request) => connection.call({ kind: "projectConfigRead", request }),
      write: (request) => connection.call({ kind: "projectConfigWrite", request }),
      resolveRoot: (path) => connection.call({ kind: "projectDirectoryParent", parentPath: path }),
    },
    projectIcons: {
      get: (project) => connection.call({ kind: "projectIconGet", projectId: project.projectId }),
      async set(project, source) {
        const projectId = project.projectId;
        const customIconRevision = await connection.call({ kind: "projectIconSet", projectId, source });
        const updatedAt = new Date().toISOString();
        if (!(await projectRegistry.update(projectId, (current) => ({ ...current, customIconRevision, updatedAt }))))
          throw new Error("Project not found");
      },
      async remove(project) {
        await connection.call({ kind: "projectIconRemove", projectId: project.projectId });
      },
    },
    logger,
  });
  function dispatchProject(message: SessionInboundMessage): Promise<SessionOutboundMessage> | undefined {
    if (message.type === "workspace.github.search_repositories.request")
      return connection.call({ kind: "projectGithubSearch", request: message });
    if (message.type === "project.github.clone.request") return cloneGithubProject(message);
    return projectManagement.dispatch(message);
  }

  async function cloneGithubProject(
    request: Extract<SessionInboundMessage, { type: "project.github.clone.request" }>,
  ): Promise<SessionOutboundMessage> {
    const { requestId } = request;
    let repo = request.repo;
    let checkoutPath: string | null = null;
    try {
      const cloned = await connection.call({ kind: "projectGithubClone", request });
      ({ repo, checkoutPath } = cloned);
      const project = await registerProjectDirectory(projectRegistry, cloned.registration);
      return {
        type: "project.github.clone.response",
        payload: { requestId, repo, checkoutPath, project: describeProject(project), error: null },
      };
    } catch (error) {
      return {
        type: "project.github.clone.response",
        payload: { requestId, repo, checkoutPath, project: null, error: getErrorMessage(error) },
      };
    }
  }

  function dispatchWorkspaceArchive(message: SessionInboundMessage) {
    if (
      message.type !== "workspace.recovery.restore.request" &&
      message.type !== "workspace.archive.undo.request"
    ) {
      return archive.dispatch(message);
    }
    return workspaceRegistry.get(message.workspaceId).then((workspace) =>
      workspace
        ? projectManagement.withProjectMutation(workspace.projectId, () => archive.dispatch(message)!)
        : archive.dispatch(message)!,
    );
  }
  async function approveWorkspaceSetup(workspaceId: string): Promise<boolean> {
    const workspace = await workspaceRegistry.get(workspaceId);
    if (!workspace || workspace.archivedAt) throw new Error(`Workspace not found: ${workspaceId}`);
    const checkout = workspace.worktreeRoot ?? workspace.cwd;
    return archive.operations.withCheckout(checkout, async () => {
      const current = await workspaceRegistry.get(workspaceId);
      if (!current || current.archivedAt ||
        !areEquivalentPaths(checkout, current.worktreeRoot ?? current.cwd)) {
        throw new Error("Workspace checkout changed");
      }
      const { clearedMetadata, blockedWorkspaceIds } = await connection.call({
        kind: "worktreeApproveSetup",
        workspaceId,
      });
      if (!clearedMetadata && blockedWorkspaceIds.length === 0) return false;
      const updatedAt = new Date().toISOString();
      for (const id of blockedWorkspaceIds) {
        await workspaceRegistry.update(id, (record) => {
          if (!record.untrustedSource) return record;
          const { untrustedSource: _untrusted, ...trusted } = record;
          return { ...trusted, updatedAt };
        });
      }
      await connection.call({ kind: "worktreeStartSetup", workspaceId });
      return true;
    });
  }
  const listWorkspaces = () => workspaceRegistry.listAll();
  async function updateCheckoutBranch(cwd: string, branch: string | null) {
    for (const workspace of await listWorkspaces()) {
      if (workspace.archivedAt || workspace.branch === branch || !areEquivalentPaths(workspace.cwd, cwd)) {
        continue;
      }
      await workspaceRegistry.update(workspace.workspaceId, (record) => ({
        ...record,
        branch,
        updatedAt: new Date().toISOString(),
      }));
    }
  }
  const openProjectLane = pLimit(1);
  function placement(
    workspace: z.infer<typeof PersistedWorkspaceRecordSchema>,
    project: z.infer<typeof PersistedProjectRecordSchema>,
  ) {
    return {
      projectKey: project.projectId,
      projectName: resolveProjectDisplayName(project),
      workspaceName: resolveWorkspaceDisplayName(workspace),
      checkout: checkoutFromPersistedWorkspacePlacement({ workspace }),
    };
  }
  async function describeWorkspace({
    workspace,
    projectRecord,
  }: {
    workspace: z.infer<typeof PersistedWorkspaceRecordSchema>;
    projectRecord?: z.infer<typeof PersistedProjectRecordSchema> | null;
  }): Promise<WorkspaceDescriptorPayload> {
    return {
      id: workspace.workspaceId,
      projectId: workspace.projectId,
      projectDisplayName: projectRecord
        ? resolveProjectDisplayName(projectRecord)
        : workspace.projectId,
      projectCustomName: projectRecord?.customName ?? null,
      projectCustomIconRevision: projectRecord?.customIconRevision ?? null,
      projectRootPath: projectRecord?.rootPath ?? workspace.cwd,
      workspaceDirectory: workspace.cwd,
      projectKind: projectRecord?.kind ?? "non_git",
      workspaceKind: workspace.kind,
      name: resolveWorkspaceDisplayName(workspace),
      title: workspace.title,
      pinnedAt: workspace.pinnedAt,
      labels: workspace.labels,
      metadata: workspace.metadata,
      background: workspace.background,
      parentWorkspaceId: workspace.parentWorkspaceId,
      archivingAt: archive.operations.getArchivingAt(workspace.workspaceId),
      status: "done",
      statusEnteredAt: null,
      activityAt: null,
      createdAt: workspace.createdAt,
      diffStat: null,
      scripts: connection.supportsWorkspaceOperations && !workspace.archivedAt
        ? await scripts.list(workspace.workspaceId).catch((error: unknown) => {
            logger.debug({ err: error, workspaceId: workspace.workspaceId }, "Script snapshot unavailable");
            return [];
          })
        : [],
      ...(projectRecord ? { project: placement(workspace, projectRecord) } : {}),
    };
  }
  const directory = new WorkspaceDirectory({
    logger,
    projectRegistry,
    workspaceRegistry: {
      async listActive() {
        return (await listWorkspaces()).filter((record) => !record.archivedAt);
      },
      async get(id) {
        return (await listWorkspaces()).find((record) => record.workspaceId === id) ?? null;
      },
    },
    getArchivingAt: () => null,
    async listAgentPayloads(ids) {
      const allowed = ids && new Set(ids);
      return (await agents.listAgents()).filter(
        (agent) => !allowed || (agent.workspaceId && allowed.has(agent.workspaceId)),
      );
    },
    async workspaceLastUserMessageTimes() {
      return new Map();
    },
    async listProviderSubagentActivity() {
      return agents.manager.listProviderSubagentActivity();
    },
    async listTerminalActivityContributions() {
      return [];
    },
    isProviderVisibleToClient: () => true,
    buildWorkspaceDescriptor: describeWorkspace,
  });
  async function readWorkspaces(
    request: Extract<Request, { type: "fetch_workspaces_request" }>,
  ): Promise<DirectoryResponse> {
    if (request.sync && request.filter) throw new WorkerOperationError("invalid_request");
    const result = request.sync
      ? sync.synchronizeWorkspaces(
          await directory.listDescriptors(),
          request.sync,
          request.includeBackground,
        )
      : await directory.listFetchEntries({
          ...request,
          filter: {
            ...request.filter,
            ...(request.includeBackground ? { includeBackground: true } : {}),
          },
        });
    return {
      type: "fetch_workspaces_response",
      payload: { requestId: request.requestId, ...result },
    };
  }

  async function read(request: Request): Promise<DirectoryResponse> {
    if (request.type === "fetch_agents_request" || request.type === "fetch_agent_history_request") {
      const isHistory = request.type === "fetch_agent_history_request";
      const search = isHistory ? request.search?.trim() : undefined;
      const includeBackground = !isHistory && request.includeBackground;
      const [snapshots, workspaceRecords, projectRecords] = await Promise.all([
        agents.listAgents(),
        listWorkspaces(),
        projectRegistry.list(),
      ]);
      const byWorkspace = new Map(
        workspaceRecords.map((workspace) => [workspace.workspaceId, workspace]),
      );
      const byProject = new Map(projectRecords.map((project) => [project.projectId, project]));
      const filter = request.filter;
      const entries = snapshots.flatMap((agent) => {
        const workspace = agent.workspaceId ? byWorkspace.get(agent.workspaceId) : undefined;
        const project = workspace ? byProject.get(workspace.projectId) : undefined;
        const location =
          workspace && project
            ? placement(workspace, project)
            : {
                projectKey: agent.cwd,
                projectName: agent.cwd,
                checkout: {
                  cwd: agent.cwd,
                  isGit: false as const,
                  worktreeRoot: null,
                  currentBranch: null,
                  remoteUrl: null,
                  isPaseoOwnedWorktree: false as const,
                  mainRepoRoot: null,
                },
              };
        const entry = { agent, project: location };
        if (
          !(includeBackground || filter?.includeBackground) &&
          (agent.background || workspace?.background)
        ) {
          return [];
        }
        if (!matchesAgentUpdatesFilter({ ...entry, filter })) return [];
        return search && !matchesAgentHistoryQuery(search, entry) ? [] : [entry];
      });
      if (!isHistory && request.sync) {
        if (request.filter) throw new WorkerOperationError("invalid_request");
        const result = sync.synchronizeAgents(entries, request.sync, request.includeBackground);
        return {
          type: "fetch_agents_response",
          payload: { requestId: request.requestId, ...result },
        };
      }
      const sort = agentPager.normalizeSort(request.sort);
      entries.sort((left, right) => agentPager.compare(left.agent, right.agent, sort));
      const token = request.page?.cursor;
      let candidates = entries;
      if (token) {
        try {
          const cursor = agentPager.decode(token, sort);
          candidates = entries.filter(
            ({ agent }) => agentPager.compareWithCursor(agent, cursor, sort) > 0,
          );
        } catch (error) {
          if (error instanceof CursorError) throw new WorkerOperationError("invalid_cursor");
          throw error;
        }
      }
      const limit = request.page?.limit ?? 200;
      const page = candidates.slice(0, limit);
      const hasMore = candidates.length > limit;
      const nextCursor = hasMore ? agentPager.encode(page.at(-1)!.agent, sort) : null;
      return {
        type: isHistory ? "fetch_agent_history_response" : "fetch_agents_response",
        payload: {
          requestId: request.requestId,
          entries: page,
          pageInfo: { hasMore, nextCursor, prevCursor: token ?? null },
        },
      };
    }
    if (request.type === "project.list.request") {
      const visible = (await projectRegistry.list())
        .filter((record) => !record.archivedAt)
        .map((record) => describeProject(record));
      return {
        type: "project.list.response",
        payload: {
          requestId: request.requestId,
          ...(request.sync
            ? sync.synchronizeProjects(visible, request.sync)
            : { projects: visible }),
        },
      };
    }
    return readWorkspaces(request);
  }
  async function projectForWorkspace(workspaceId: string) {
    const [workspaces, projectRecords] = await Promise.all([
      listWorkspaces(),
      projectRegistry.list(),
    ]);
    const workspace = workspaces.find((entry) => entry.workspaceId === workspaceId);
    const project =
      workspace && projectRecords.find((entry) => entry.projectId === workspace.projectId);
    return workspace && project ? placement(workspace, project) : null;
  }
  async function addProject(
    request: Extract<SessionInboundMessage, { type: "project.add.request" }>,
  ): Promise<Extract<SessionOutboundMessage, { type: "project.add.response" }>> {
    const { cwd, requestId } = request;
    try {
      const registration = await connection.call({ kind: "inspectProject", cwd });
      if (!registration) {
        return {
          type: "project.add.response",
          payload: {
            requestId,
            project: null,
            error: `Directory not found: ${cwd}`,
            errorCode: "directory_not_found",
          },
        };
      }
      const project = await registerProjectDirectory(projectRegistry, registration);
      return {
        type: "project.add.response",
        payload: { requestId, project: describeProject(project), error: null },
      };
    } catch (error) {
      return {
        type: "project.add.response",
        payload: { requestId, project: null, error: getErrorMessage(error) },
      };
    }
  }
  async function createDirectory(
    request: Extract<SessionInboundMessage, { type: "project.create_directory.request" }>,
  ): Promise<Extract<SessionOutboundMessage, { type: "project.create_directory.response" }>> {
    const { requestId, name } = request;
    let parentPath = request.parentPath;
    async function change(action: "create" | "rollback") {
      try {
        await connection.call({ kind: "projectDirectoryChange", parentPath, name, action });
      } catch (error) {
        if (error instanceof WorkerOperationError) {
          throw Object.assign(error, { code: error.message });
        }
        throw error;
      }
    }
    try {
      const result = await createProjectDirectory(request, {
        async resolveParent(requestedPath) {
          try {
            parentPath = await connection.call({ kind: "projectDirectoryParent", parentPath: requestedPath });
            return parentPath;
          } catch (error) {
            const code = getErrorMessage(error);
            if (code === "ENOENT" || code === "ENOTDIR") {
              throw new ProjectDirectoryRequestError(
                "parent_directory_not_found", `Parent directory not found: ${requestedPath}`,
              );
            }
            if (code === "worker_workspace_root_denied" || code === "EACCES" || code === "EPERM") {
              throw new ProjectDirectoryRequestError(
                "permission_denied", `Permission denied reading parent directory: ${requestedPath}`,
              );
            }
            throw new ProjectDirectoryRequestError("filesystem_error", code);
          }
        },
        filesystem: {
          mkdir: () => change("create"),
          rmdir: () => change("rollback"),
        },
        async registerProject(cwd) {
          const registration = await connection.call({ kind: "inspectProject", cwd });
          if (!registration) throw new WorkerOperationError("directory_not_found");
          return registerProjectDirectory(projectRegistry, registration);
        },
      });
      return {
        type: "project.create_directory.response",
        payload: {
          requestId,
          directoryPath: result.directoryPath,
          project: describeProject(result.project),
          error: null,
          errorCode: null,
        },
      };
    } catch (error) {
      return {
        type: "project.create_directory.response",
        payload: {
          requestId,
          directoryPath: error instanceof ProjectDirectoryRequestError ? error.directoryPath : null,
          project: null,
          error: getErrorMessage(error),
          errorCode: error instanceof ProjectDirectoryRequestError ? error.code : "registration_failed",
        },
      };
    }
  }

  async function findOrCreateWorkspaceForDirectory(cwd: string) {
    const registration = await connection.call({ kind: "inspectProject", cwd });
    if (!registration) {
      throw Object.assign(new Error(`Directory not found: ${cwd}`), { code: "directory_not_found" });
    }
    return openProjectLane(async () => {
      const canonical = await connection.call({ kind: "projectDirectoryParent", parentPath: cwd });
      const candidate = await projectManagement.resolveOpenWorkspace(canonical);
      if (candidate) return candidate;
      const project = await registerProjectDirectory(projectRegistry, registration);
      const workspaceId = generateWorkspaceId();
      const prepared = await prepareWorkspace({
        type: "workspace.create.request",
        requestId: workspaceId,
        source: { kind: "directory", path: registration.rootPath, projectId: project.projectId },
      });
      return (await prepared.provision(workspaceId)).record;
    });
  }

  const runInImportWorkspace = createImportWorkspacePlacement({
    workspaceRegistry,
    projectRegistry,
    findOrCreateWorkspaceForDirectory,
    async matchesCwd(workspaceCwd, importCwd) {
      const target = await connection.call({ kind: "workspacePath", cwd: importCwd, parent: workspaceCwd });
      return target !== null && areEquivalentPaths(target, workspaceCwd);
    },
    logger,
  });

  async function openProject(
    request: Extract<SessionInboundMessage, { type: "open_project_request" }>,
  ): Promise<Extract<SessionOutboundMessage, { type: "open_project_response" }>> {
    const { requestId, cwd } = request;
    try {
      const record = await findOrCreateWorkspaceForDirectory(cwd);
      const workspace = await describeWorkspace({
        workspace: record,
        projectRecord: await projectRegistry.get(record.projectId),
      });
      return { type: "open_project_response", payload: { requestId, workspace, error: null } };
    } catch (error) {
      return {
        type: "open_project_response",
        payload: {
          requestId,
          workspace: null,
          error: getErrorMessage(error),
          errorCode: error instanceof Error && "code" in error && error.code === "directory_not_found"
            ? "directory_not_found"
            : null,
        },
      };
    }
  }

  async function prepareWorkspace(request: WorkspaceCreateRequest) {
    if (request.internal || request.callerAgentId) {
      throw new WorkerOperationError("worker_workspace_source_unsupported");
    }
    const source = request.source;
    const sourcePath = source.kind === "directory" ? source.path : source.cwd;
    const project = (await projectRegistry.list()).find(
      (record) =>
        !record.archivedAt &&
        (source.projectId
          ? record.projectId === source.projectId
          : record.rootPath === sourcePath),
    );
    if (!project) throw new WorkerOperationError("worker_workspace_project_required");
    if (source.kind === "worktree" && project.kind !== "git") {
      throw new WorkerOperationError("worker_checkout_not_git");
    }
    const cwd = await connection.call({
      kind: "workspacePath",
      cwd: sourcePath ?? project.rootPath,
      parent: project.rootPath,
    });
    if (!cwd) throw new WorkerOperationError("directory_not_found");
    const inWorktree = source.kind === "worktree";
    const agentCwd =
      !inWorktree && request.agent &&
      (await connection.call({
        kind: "workspacePath",
        cwd: request.agent.config.cwd,
        parent: cwd,
      }));
    if (!inWorktree && request.agent && !agentCwd)
      throw new WorkerOperationError("worker_agent_directory_outside_workspace");
    return {
      agentCwd: inWorktree || !agentCwd ? null : agentCwd,
      provision(workspaceId: string) {
        return projectManagement.withProjectMutation(project.projectId, async () => {
          const currentProject = await projectRegistry.get(project.projectId);
          if (!currentProject || currentProject.archivedAt ||
            !areEquivalentPaths(currentProject.rootPath, project.rootPath)) {
            throw new WorkerOperationError("worker_workspace_project_required");
          }
        const materialized = source.kind === "worktree"
          ? await connection.call({
              kind: "worktreeMaterialize",
              cwd,
              worktreeSlug: source.worktreeSlug,
              branchName: source.branchName,
              refName: source.refName ?? source.baseBranch,
              action: source.action,
              checkoutSource: source.checkoutSource,
              githubPrNumber: source.githubPrNumber,
            })
          : null;
        if (materialized && "error" in materialized) {
          throw Object.assign(new Error(materialized.error.message), { code: materialized.error.code });
        }
        const placement = materialized
          ? initialWorkspacePlacement({
              source: "created_worktree",
              cwd: materialized.cwd,
              worktreeRoot: materialized.worktreeRoot,
              branch: materialized.branch,
              baseBranch: materialized.baseBranch,
              mainRepoRoot: materialized.repoRoot,
            })
          : {
              cwd,
              kind: project.kind === "git" ? "local_checkout" as const : "directory" as const,
              displayName: cwd.replaceAll("\\", "/").split("/").findLast(Boolean) ?? cwd,
            };
        const now = new Date().toISOString();
        const workspace = createPersistedWorkspaceRecord({
          workspaceId,
          projectId: project.projectId,
          ...placement,
          untrustedSource: materialized?.untrustedSource,
          title:
            request.title?.trim() ||
            resolveFirstAgentPromptTitle(
              request.agent
                ? { prompt: request.agent.initialPrompt, attachments: request.agent.attachments }
                : request.firstAgentContext,
            ),
          metadata: request.metadata,
          background: request.background,
          createdAt: now,
          updatedAt: now,
        });
        await workspaceRegistry.upsert(workspace);
        if (materialized) {
          await connection.call({ kind: "worktreeStartSetup", workspaceId }).catch((error: unknown) => {
            logger.error({ err: error, workspaceId }, "Could not start workspace setup");
          });
        }
          return { workspace: await describeWorkspace({ workspace, projectRecord: currentProject }), record: workspace };
        });
      },
    };
  }
  return {
    read,
    addProject,
    createDirectory,
    openProject,
    projectForWorkspace,
    prepareWorkspace,
    updateCheckoutBranch,
    workspaceRegistry,
    archive: dispatchWorkspaceArchive,
    manageProject: dispatchProject,
    projectRegistry,
    approveWorkspaceSetup,
    subscribeToArchiveChanges(listener: () => void) {
      archiveListeners.add(listener);
      return () => archiveListeners.delete(listener);
    },
    runInImportWorkspace,
  };
}

type DirectoryResponse = Extract<
  SessionOutboundMessage,
  {
    type:
      | "project.list.response"
      | "fetch_workspaces_response"
      | "fetch_agents_response"
      | "fetch_agent_history_response";
  }
>;

export function createDirectorySubscriptions(options: {
  directory: ReturnType<typeof createCentralDirectory>;
  manager: Awaited<ReturnType<typeof openCentralAgents>>["manager"];
  send(message: SessionOutboundMessage): void;
  logger: Logger;
}) {
  const { directory, manager, send, logger } = options;
  const subscriptions = new Map<string, () => void>();
  return {
    async request(request: Request) {
      if (
        request.type === "project.list.request" ||
        request.type === "fetch_agent_history_request" ||
        !request.subscribe
      ) {
        send(await directory.read(request));
        return;
      }
      if (subscriptions.size >= 64) throw new WorkerOperationError("worker_subscription_limit");
      if (request.subscribe.subscriptionId) throw new WorkerOperationError("invalid_request");
      const refreshRequest = { ...request, sync: undefined };
      const subscriptionId = randomUUID();
      let stopped = false;
      let reading = true;
      let dirty = false;
      function entries(response: DirectoryResponse) {
        if (response.type === "fetch_agents_response") {
          return response.payload.entries.map((entry) => [entry.agent.id, entry] as const);
        }
        if (response.type === "fetch_workspaces_response") {
          return response.payload.entries.map(
            (workspace) => [workspace.id, { workspace }] as const,
          );
        }
        return [];
      }
      let previous = new Map<string, ReturnType<typeof entries>[number][1]>();
      function changed() {
        dirty = true;
        if (!reading) void refresh();
      }
      const stopWorkspace = directory.workspaceRegistry.subscribeToMutations(changed);
      const stopProject = directory.projectRegistry.subscribeToMutations(changed);
      const stopArchive = directory.subscribeToArchiveChanges(changed);
      const stopAgent = manager.subscribe((event) => {
        if (event.type === "agent_state" || event.type === "agent_removed") changed();
      });
      function stop() {
        stopped = true;
        stopWorkspace();
        stopProject();
        stopArchive();
        stopAgent();
      }
      subscriptions.set(subscriptionId, stop);
      function delta(value: ReturnType<typeof entries>[number][1]) {
        if ("agent" in value)
          send({ type: "agent_update", payload: { kind: "upsert", subscriptionId, ...value } });
        else
          send({ type: "workspace_update", payload: { kind: "upsert", subscriptionId, ...value } });
      }
      async function refresh() {
        reading = true;
        try {
          while (dirty) {
            if (stopped) return;
            dirty = false;
            const updated = await directory.read(refreshRequest);
            if (stopped) return;
            const current = new Map<string, ReturnType<typeof entries>[number][1]>(
              entries(updated),
            );
            for (const [id, value] of current)
              if (!isDeepStrictEqual(previous.get(id), value)) delta(value);
            for (const id of previous.keys()) {
              if (current.has(id)) continue;
              if (updated.type === "fetch_agents_response")
                send({
                  type: "agent_update",
                  payload: { kind: "remove", subscriptionId, agentId: id },
                });
              else
                send({ type: "workspace_update", payload: { kind: "remove", subscriptionId, id } });
            }
            previous = current;
          }
        } catch (error) {
          logger.warn({ err: error }, "Worker directory subscription refresh failed");
        } finally {
          reading = false;
        }
      }
      try {
        const initial = await directory.read(request);
        if (
          stopped ||
          (initial.type !== "fetch_agents_response" && initial.type !== "fetch_workspaces_response")
        )
          return;
        previous = new Map<string, ReturnType<typeof entries>[number][1]>(entries(initial));
        if (initial.type === "fetch_agents_response")
          send({ ...initial, payload: { ...initial.payload, subscriptionId } });
        else send({ ...initial, payload: { ...initial.payload, subscriptionId } });
        await refresh();
      } catch (error) {
        subscriptions.delete(subscriptionId);
        stop();
        throw error;
      }
    },
    release(subscriptionId: string) {
      subscriptions.get(subscriptionId)?.();
      subscriptions.delete(subscriptionId);
    },
    close() {
      for (const stop of subscriptions.values()) stop();
      subscriptions.clear();
    },
  };
}
