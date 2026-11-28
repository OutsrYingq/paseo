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
import { WorkspaceDirectory } from "../workspace-directory.js";
import { describeProject } from "../project-descriptor.js";
import {
  PersistedProjectRecordSchema,
  PersistedWorkspaceRecordSchema,
  FileBackedWorkspaceRegistry,
  createPersistedWorkspaceRecord,
  resolveProjectDisplayName,
  resolveWorkspaceDisplayName,
} from "../workspace-registry.js";
import type { openCentralAgents } from "./central-agents.js";
import type { createRemoteState } from "./remote-state.js";
import { WorkerOperationError } from "./execution-protocol.js";

type Request = Extract<
  SessionInboundMessage,
  { type: "project.list.request" | "fetch_workspaces_request" | "fetch_agents_request" }
>;

export function createCentralDirectory(
  logger: Logger,
  state: ReturnType<typeof createRemoteState>,
  agents: Awaited<ReturnType<typeof openCentralAgents>>,
  connection: WorkerConnection,
) {
  const sync = new DirectorySyncService();
  const agentPager = createAgentDirectoryPager();
  const projects = z.array(PersistedProjectRecordSchema);
  const workspaceRegistry = new FileBackedWorkspaceRegistry("projects/workspaces.json", logger, {
    readRecords: state.read,
    writeRecords: (file, records) => state.write(file, JSON.stringify(records)),
  });
  const projectRegistry = {
    list: async () =>
      projects.parse(JSON.parse((await state.read("projects/projects.json")) ?? "[]")),
  };
  const listWorkspaces = () => workspaceRegistry.listAll();
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
      archivingAt: null,
      status: "done",
      statusEnteredAt: null,
      activityAt: null,
      createdAt: workspace.createdAt,
      diffStat: null,
      scripts: [],
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
  async function read(request: Request): Promise<DirectoryResponse> {
    if (request.type === "fetch_agents_request") {
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
        if (
          (!filter?.includeArchived && agent.archivedAt) ||
          (!(request.includeBackground || filter?.includeBackground) &&
            (agent.background || workspace?.background)) ||
          (filter?.statuses && !filter.statuses.includes(agent.status)) ||
          (filter?.projectKeys && !filter.projectKeys.includes(location.projectKey)) ||
          (filter?.requiresAttention !== undefined &&
            filter.requiresAttention !== agent.requiresAttention) ||
          (filter?.thinkingOptionId !== undefined &&
            filter.thinkingOptionId !== agent.thinkingOptionId) ||
          (filter?.labels &&
            !Object.entries(filter.labels).every(([key, value]) => agent.labels[key] === value))
        )
          return [];
        return [{ agent, project: location }];
      });
      if (request.sync) {
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
        type: "fetch_agents_response",
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
  async function projectForWorkspace(workspaceId: string) {
    const [workspaces, projects] = await Promise.all([listWorkspaces(), projectRegistry.list()]);
    const workspace = workspaces.find((entry) => entry.workspaceId === workspaceId);
    const project = workspace && projects.find((entry) => entry.projectId === workspace.projectId);
    return workspace && project ? placement(workspace, project) : null;
  }
  async function prepareWorkspace(request: WorkspaceCreateRequest) {
    if (request.source.kind !== "directory" || request.internal || request.callerAgentId) {
      throw new WorkerOperationError("worker_workspace_source_unsupported");
    }
    const source = request.source;
    const project = (await projectRegistry.list()).find(
      (record) =>
        !record.archivedAt &&
        (source.projectId
          ? record.projectId === source.projectId
          : record.rootPath === source.path),
    );
    if (!project) throw new WorkerOperationError("worker_workspace_project_required");
    const cwd = await connection.call({
      kind: "workspacePath",
      cwd: source.path,
      parent: project.rootPath,
    });
    if (!cwd) throw new WorkerOperationError("directory_not_found");
    const agentCwd =
      request.agent &&
      (await connection.call({
        kind: "workspacePath",
        cwd: request.agent.config.cwd,
        parent: cwd,
      }));
    if (request.agent && !agentCwd)
      throw new WorkerOperationError("worker_agent_directory_outside_workspace");
    return {
      agentCwd,
      async provision(workspaceId: string) {
        const now = new Date().toISOString();
        const workspace = createPersistedWorkspaceRecord({
          workspaceId,
          projectId: project.projectId,
          cwd,
          kind: project.kind === "git" ? "local_checkout" : "directory",
          displayName: cwd.replaceAll("\\", "/").split("/").filter(Boolean).at(-1) ?? cwd,
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
        return { workspace: await describeWorkspace({ workspace, projectRecord: project }) };
      },
    };
  }
  return { read, projectForWorkspace, prepareWorkspace, workspaceRegistry };
}

type DirectoryResponse = Extract<
  SessionOutboundMessage,
  { type: "project.list.response" | "fetch_workspaces_response" | "fetch_agents_response" }
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
      if (request.type === "project.list.request" || !request.subscribe) {
        send(await directory.read(request));
        return;
      }
      if (subscriptions.size >= 64) throw new WorkerOperationError("worker_subscription_limit");
      if (request.subscribe.subscriptionId) throw new WorkerOperationError("invalid_request");
      const subscriptionId = randomUUID();
      let stopped = false;
      let reading = true;
      let dirty = false;
      const entries = (response: DirectoryResponse) =>
        response.type === "fetch_agents_response"
          ? response.payload.entries.map((entry) => [entry.agent.id, entry] as const)
          : response.type === "fetch_workspaces_response"
            ? response.payload.entries.map((workspace) => [workspace.id, { workspace }] as const)
            : [];
      let previous = new Map<string, ReturnType<typeof entries>[number][1]>();
      const stop = manager.subscribe((event) => {
        if (event.type !== "agent_state" && event.type !== "agent_removed") return;
        dirty = true;
        if (!reading) void refresh();
      });
      subscriptions.set(subscriptionId, () => {
        stopped = true;
        stop();
      });
      function delta(value: ReturnType<typeof entries>[number][1]) {
        if ("agent" in value)
          send({ type: "agent_update", payload: { kind: "upsert", subscriptionId, ...value } });
        else
          send({ type: "workspace_update", payload: { kind: "upsert", subscriptionId, ...value } });
      }
      async function refresh() {
        reading = true;
        try {
          while (dirty && !stopped) {
            dirty = false;
            const updated = await directory.read({ ...request, sync: undefined });
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
        if (stopped || initial.type === "project.list.response") return;
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
