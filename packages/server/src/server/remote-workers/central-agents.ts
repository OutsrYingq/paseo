import type {
  AgentSnapshotPayload,
  CreationSnapshot,
  WorkspaceCreateRequest,
  WorkspaceDescriptorPayload,
} from "@getpaseo/protocol/messages";
import type { WorkspaceRegistry } from "../workspace-registry.js";
import { randomUUID } from "node:crypto";
import type { Logger } from "pino";
import type { AgentCreateRequest, SendAgentMessageRequest } from "@getpaseo/protocol/messages";
import { AgentManager, type AgentManagerOptions } from "../agent/agent-manager.js";
import { buildStoredAgentPayload } from "../agent/agent-projections.js";
import {
  sendPreparedPromptToAgent,
  unarchiveAgentState,
  waitForAgentRunStartWithTimeout,
} from "../agent/agent-prompt.js";
import type { AgentClient } from "../agent/agent-sdk-types.js";
import { resolveCreateAgentTitles } from "../agent/create-agent-title.js";
import {
  archiveAgentCommand,
  cancelAgentRunCommand,
  setAgentModeCommand,
  updateAgentCommand,
} from "../agent/lifecycle-command.js";
import { AgentStorage } from "../agent/agent-storage.js";
import { respondToAgentPermission } from "../agent/permission-response.js";
import { buildAgentPrompt } from "../agent/prompt-attachments.js";
import { ProviderSnapshotManager } from "../agent/provider-snapshot-manager.js";
import { shutdownAgentClients } from "../agent/provider-registry.js";
import { CreationService } from "../creation/index.js";
import { MessageReceipts } from "../message-receipts/index.js";
import { serializeAgentSnapshot, serializeAgentStreamEvent } from "../messages.js";
import {
  attachAgentStoragePersistence,
  isStoredAgentProviderAvailable,
  toAgentPersistenceHandle,
} from "../persistence-hooks.js";
import { WorkerOperationError } from "./execution-protocol.js";
import type { createRemoteState } from "./remote-state.js";
import { ensureAgentLoaded, ensureUnarchivedAgentLoaded } from "../agent/agent-loading.js";
import { AgentConfigSession } from "../session/agent-config/agent-config-session.js";
import type { SessionInboundMessage, SessionOutboundMessage } from "@getpaseo/protocol/messages";

export type CentralTimelineUpdate = Extract<
  SessionOutboundMessage,
  { type: "agent_stream" | "agent.timeline.replacement" }
>;

export interface CentralAgentOptions {
  state: ReturnType<typeof createRemoteState>;
  logger: Logger;
  createClients(): Promise<Record<string, AgentClient>>;
  manager?: Omit<AgentManagerOptions, "logger" | "clients" | "registry">;
}

export async function openCentralAgents(
  options: CentralAgentOptions,
  publish: (event: CentralTimelineUpdate) => void,
  signal: AbortSignal,
) {
  const { logger, state } = options;
  const storage = new AgentStorage("agents", logger, state.agents);
  await storage.initialize();
  const clients = await options.createClients();
  const catalog = new ProviderSnapshotManager({
    logger,
    extraClients: clients,
    executionOnly: true,
    resolveCwd: (cwd) => cwd,
  });
  const manager = new AgentManager({
    ...options.manager,
    logger,
    clients,
    registry: storage,
  });
  const dependencies = { logger, agentManager: manager, agentStorage: storage };
  const detachStorage = attachAgentStoragePersistence(logger, manager, storage);
  const receiptsDirectory = "agent-requests";
  const receipts = new MessageReceipts(receiptsDirectory, state.receipts);
  const creations = new CreationService(
    "creations",
    logger,
    async (snapshot) => {
      if (snapshot.agentId && !(await storage.get(snapshot.agentId)))
        throw new Error("agent_no_longer_exists");
    },
    receiptsDirectory,
    state.creation,
  );
  const stopEvents = manager.subscribe(
    (event) => {
      if (signal.aborted) return;
      if (event.type === "timeline_replacement") {
        publish({
          type: "agent.timeline.replacement",
          payload: { agentId: event.agentId, epoch: event.epoch },
        });
        return;
      }
      if (event.type !== "agent_stream") return;
      const payload = serializeAgentStreamEvent(event.event);
      if (!payload) return;
      publish({
        type: "agent_stream",
        payload: {
          agentId: event.agentId,
          event: payload,
          timestamp: event.timestamp ?? new Date().toISOString(),
          seq: event.seq,
          epoch: event.epoch,
        },
      });
    },
    { replayState: false },
  );

  function requireActive(request?: AbortSignal): void {
    if (signal.aborted || request?.aborted)
      throw new WorkerOperationError("worker_connection_lost");
  }

  function snapshotAgent(
    agent: NonNullable<ReturnType<AgentManager["getAgent"]>>,
    stored: Awaited<ReturnType<AgentStorage["get"]>>,
  ) {
    let title = stored?.title;
    if (!title && (stored?.persistence || agent.lastUserMessageAt)) {
      const prompt = manager.getTimeline(agent.id).find((item) => item.type === "user_message");
      title = resolveCreateAgentTitles({ initialPrompt: prompt?.text }).provisionalTitle;
    }
    return serializeAgentSnapshot(agent, { title, archivedAt: stored?.archivedAt });
  }

  async function readAgent(id: string) {
    const live = manager.getAgent(id);
    const stored = await storage.get(id);
    if (live) return snapshotAgent(live, stored);
    if (!stored) return null;
    return buildStoredAgentPayload(stored, manager.getRegisteredProviderIds(), (provider, own) => {
      return clients[provider]?.effectiveOptions?.(own) ?? own ?? {};
    });
  }

  async function sendPrompt(
    agentId: string,
    input: {
      text: string;
      images?: SendAgentMessageRequest["images"];
      attachments?: SendAgentMessageRequest["attachments"];
      sessionMode?: string;
      activeTurnBehavior?: SendAgentMessageRequest["activeTurnBehavior"];
      messageId?: string;
    },
    request?: AbortSignal,
    beforeDispatch?: () => Promise<void>,
  ) {
    const dispatch = await sendPreparedPromptToAgent({
      ...dependencies,
      agentId,
      prompt: buildAgentPrompt(input.text, input.images, input.attachments),
      sessionMode: input.sessionMode,
      activeTurnBehavior:
        input.activeTurnBehavior === "queue" ? undefined : input.activeTurnBehavior,
      runOptions: { clientMessageId: input.messageId },
      admit: () => !signal.aborted && !request?.aborted,
      beforeDispatch: async () => {
        requireActive(request);
        await beforeDispatch?.();
        requireActive(request);
        const { provisionalTitle } = resolveCreateAgentTitles({ initialPrompt: input.text });
        if (provisionalTitle) {
          const stored = await storage.get(agentId);
          requireActive(request);
          if (stored && !stored.title && !stored.lastUserMessageAt) {
            await manager.setTitle(agentId, provisionalTitle);
          }
        }
        requireActive(request);
      },
    });
    if (dispatch.disposition === "turn_started") {
      await waitForAgentRunStartWithTimeout(manager, agentId, dispatch.started);
    }
  }

  function validateAgentCreation(input: AgentCreateRequest) {
    if (!manager.getRegisteredProviderIds().includes(input.config.provider)) {
      throw new WorkerOperationError("worker_provider_unavailable");
    }
    if (
      input.worktree ||
      input.worktreeName ||
      input.git ||
      input.autoArchive ||
      input.callerAgentId ||
      input.outputSchema ||
      input.internal
    ) {
      throw new WorkerOperationError("worker_creation_options_unsupported");
    }
  }

  async function launchAgent(
    input: AgentCreateRequest,
    agentId: string,
    onReady: (agent: AgentSnapshotPayload) => Promise<void>,
    requestSignal?: AbortSignal,
  ) {
    requireActive(requestSignal);
    const hasPrompt = Boolean(
      input.initialPrompt || input.images?.length || input.attachments?.length,
    );
    const agent = await manager.createAgent(input.config, agentId, {
      workspaceId: input.workspaceId,
      background: input.background,
      labels: input.labels,
      metadata: input.metadata,
      env: input.env,
    });
    const { provisionalTitle } = resolveCreateAgentTitles({
      configTitle: input.config.title,
      initialPrompt: input.initialPrompt,
    });
    await storage.applySnapshot(agent, { title: provisionalTitle });
    await onReady(serializeAgentSnapshot(agent, { title: provisionalTitle }));
    if (hasPrompt) {
      await sendPrompt(
        agent.id,
        {
          text: input.initialPrompt ?? "",
          images: input.images,
          attachments: input.attachments,
          messageId: input.clientMessageId,
        },
        requestSignal,
      );
    }
    return {
      agent: (await readAgent(agent.id)) ?? serializeAgentSnapshot(agent),
      initialPromptStarted: hasPrompt,
    };
  }

  async function createAgent(input: AgentCreateRequest, requestSignal?: AbortSignal) {
    requireActive(requestSignal);
    validateAgentCreation(input);
    const { idempotencyKey, requestId: _requestId, subscribe: _subscribe, ...request } = input;
    const hasPrompt = Boolean(
      input.initialPrompt || input.images?.length || input.attachments?.length,
    );
    return creations.create({
      kind: "agent",
      key: idempotencyKey ?? randomUUID(),
      request,
      hasAgent: true,
      hasPrompt,
      agentId: input.agentId,
      exists: async (kind, id) => kind === "agent" && Boolean(await storage.get(id)),
      readAgent,
      createAgent: (id, _workspace, onReady) => launchAgent(input, id, onReady, requestSignal),
    });
  }

  async function createWorkspace(
    directory: {
      workspaceRegistry: Pick<WorkspaceRegistry, "get">;
      prepareWorkspace(request: WorkspaceCreateRequest): Promise<{
        agentCwd?: string | null;
        provision(workspaceId: string): Promise<{ workspace: WorkspaceDescriptorPayload }>;
      }>;
    },
    input: WorkspaceCreateRequest,
    requestSignal: AbortSignal,
    observe?: (snapshot: CreationSnapshot) => void,
  ) {
    requireActive(requestSignal);
    const { type: _type, idempotencyKey, requestId, subscribe: _subscribe, ...intent } = input;
    const prepared = await directory.prepareWorkspace(input);
    const agent = input.agent && {
      ...input.agent,
      type: "agent.create.request" as const,
      requestId,
      config: { ...input.agent.config, cwd: prepared.agentCwd! },
    };
    if (agent) validateAgentCreation(agent);
    return creations.create(
      {
        kind: "workspace",
        key: idempotencyKey ?? requestId,
        request: intent,
        workspaceId: input.workspaceId,
        agentId: agent?.agentId,
        hasAgent: Boolean(agent),
        hasPrompt: Boolean(
          agent && (agent.initialPrompt || agent.images?.length || agent.attachments?.length),
        ),
        exists: async (kind, id) =>
          kind === "workspace"
            ? Boolean(await directory.workspaceRegistry.get(id))
            : Boolean(await storage.get(id)),
        provision: async (id) => {
          requireActive(requestSignal);
          return prepared.provision(id);
        },
        createAgent: agent
          ? (id, workspace, onReady) =>
              launchAgent({ ...agent, workspaceId: workspace?.id }, id, onReady, requestSignal)
          : undefined,
      },
      observe,
    );
  }

  async function sendMessage(
    input: SendAgentMessageRequest,
    requestSignal?: AbortSignal,
  ): Promise<null> {
    requireActive(requestSignal);
    if (!input.messageId) throw new WorkerOperationError("worker_message_id_required");
    if (input.sourceAgentServerId || input.sourceAgentId || input.activeTurnBehavior === "queue") {
      throw new WorkerOperationError("worker_message_source_unsupported");
    }
    const request = {
      prompt: buildAgentPrompt(input.text, input.images, input.attachments),
      sourceAgentId: input.sourceAgentId,
      sourceAgentServerId: input.sourceAgentServerId,
      sessionMode: input.sessionMode,
      activeTurnBehavior: input.activeTurnBehavior ?? "interrupt",
    };
    try {
      await receipts.send({
        agentId: input.agentId,
        messageId: input.messageId,
        request,
        send: (beforeDispatch) => sendPrompt(input.agentId, input, requestSignal, beforeDispatch),
      });
    } catch (error) {
      if (
        error instanceof Error &&
        /^agent_request_(outcome_unknown|key_conflict)$/.test(error.message)
      ) {
        throw new WorkerOperationError(error.message);
      }
      logger.warn({ err: error, agentId: input.agentId }, "Worker prompt dispatch failed");
      throw error;
    }
    return null;
  }

  let closing: Promise<void> | undefined;
  function close() {
    return (closing ??= (async () => {
      catalog.destroy();
      stopEvents();
      manager.prepareForShutdown();
      const closed = await Promise.allSettled(
        manager.listAgents({ includeInternal: true }).map((agent) => manager.closeAgent(agent.id)),
      );
      await manager.flushForShutdown();
      detachStorage();
      await storage.flush();
      await shutdownAgentClients(Object.values(clients), logger);
      const failed = closed.find((result) => result.status === "rejected");
      if (failed) throw failed.reason;
    })());
  }

  return {
    setPluginClients(pluginClients: Record<string, AgentClient>) {
      const previous = manager.getRegisteredProviderIds();
      const merged = { ...clients, ...pluginClients };
      manager.updateProviderRegistry({
        providerDefinitions: {},
        clients: merged,
        retiredProviders: previous.filter((id) => !(id in merged)),
      });
    },
    registeredProviders: () => manager.getRegisteredProviderIds(),
    timelineHead: (agentId: string) => manager.getLoadedTimelineHead(agentId),
    manager,
    catalog,
    createAgent,
    createWorkspace,
    sendMessage,
    cancelAgent: (id: string) => cancelAgentRunCommand(dependencies, id),
    archiveAgent: (id: string) => archiveAgentCommand(dependencies, id),
    async clearAgentAttention(ids: string[]) {
      requireActive();
      for (const id of ids) {
        if (!manager.getAgent(id) && !(await storage.get(id))) {
          throw new WorkerOperationError("agent_not_found");
        }
      }
      requireActive();
      await Promise.all(ids.map((id) => manager.clearAgentAttention(id)));
    },
    respondToPermission(
      input: Extract<SessionInboundMessage, { type: "agent_permission_response" }>,
    ) {
      requireActive();
      return respondToAgentPermission({ ...input, agentManager: manager, logger });
    },
    updateAgent: (input: Extract<SessionInboundMessage, { type: "update_agent_request" }>) =>
      updateAgentCommand(dependencies, input),
    createAgentConfigSession(emit: (message: SessionOutboundMessage) => void) {
      return new AgentConfigSession({
        host: { emit },
        logger,
        operations: {
          ensureLoaded: async (agentId) => {
            await ensureUnarchivedAgentLoaded(agentId, dependencies);
          },
          setMode: async (agentId, modeId) =>
            (await setAgentModeCommand(dependencies, { agentId, modeId })).notice,
          setModel: (agentId, modelId) => manager.setAgentModel(agentId, modelId),
          setOptions: (agentId, patch) => manager.setAgentOptions(agentId, patch),
          setThinking: (agentId, optionId) => manager.setAgentThinkingOption(agentId, optionId),
        },
      });
    },
    async refreshAgent(agentId: string, request: AbortSignal) {
      requireActive(request);
      await unarchiveAgentState(storage, manager, agentId);
      requireActive(request);
      if (manager.getAgent(agentId)) {
        // The manager cancels an in-flight run in its per-agent lifecycle lane before reopening.
        await manager.reloadAgentSession(agentId, undefined, { rehydrateFromDisk: true });
        requireActive(request);
        await manager.hydrateTimelineFromProvider(agentId, { broadcast: true });
      } else {
        const record = await storage.get(agentId);
        if (!record) throw new Error(`Agent not found: ${agentId}`);
        const registered = manager.getRegisteredProviderIds();
        if (!isStoredAgentProviderAvailable(record, registered)) {
          throw new Error(`Agent ${agentId} references unavailable provider '${record.provider}'`);
        }
        if (!toAgentPersistenceHandle(registered, record.persistence)) {
          throw new Error(`Agent ${agentId} cannot be refreshed because it lacks persistence`);
        }
        requireActive(request);
        await ensureAgentLoaded(agentId, { ...dependencies, broadcastTimeline: true });
      }
      requireActive(request);
      return manager.getTimeline(agentId).length;
    },
    async listAgents() {
      const records = await storage.listAll();
      const agents = await Promise.all(
        records.filter((record) => !record.internal).map((record) => readAgent(record.id)),
      );
      return agents.filter((agent) => agent !== null);
    },
    async readSnapshot(agentId: string) {
      return readAgent(agentId);
    },
    async fetchTimeline(
      input: Extract<SessionInboundMessage, { type: "fetch_agent_timeline_request" }>,
      projection: { reasoningMerge: boolean; notification: boolean; plugin: boolean },
    ): Promise<
      Extract<SessionOutboundMessage, { type: "fetch_agent_timeline_response" }>["payload"]
    > {
      const { requestId, agentId, cursor, limit, wholeTurns, mergeWindow } = input;
      const direction = input.direction ?? (cursor ? "after" : "tail");
      const reply = {
        requestId,
        agentId,
        direction,
        projection: "projected" as const,
        mergeWindow,
      };
      try {
        requireActive();
        const agent = await ensureAgentLoaded(agentId, { ...dependencies, purpose: "history" });
        const page = manager.fetchTimeline(agentId, { direction, cursor, limit, wholeTurns });
        const stored = await storage.get(agentId);
        return {
          ...reply,
          agent: snapshotAgent(agent, stored),
          epoch: page.epoch,
          window: page.window,
          reset: page.reset,
          staleCursor: page.staleCursor,
          gap: page.gap,
          startCursor: page.startSeq === null ? null : { epoch: page.epoch, seq: page.startSeq },
          endCursor: page.endSeq === null ? null : { epoch: page.epoch, seq: page.endSeq },
          hasOlder: page.hasOlder,
          hasNewer: page.hasNewer,
          entries: page.rows
            .filter(({ item }) => {
              if (item.type === "notification") return projection.notification;
              if (item.type === "plugin") return projection.plugin;
              return true;
            })
            .map((entry) => ({
              provider: agent.provider,
              item: entry.item,
              timestamp: entry.timestamp,
              seqStart: entry.seqStart,
              seqEnd: entry.seqEnd,
              turnId: entry.turnId,
              sourceSeqRanges: entry.sourceSeqRanges,
              turnMetrics: entry.turnMetrics,
              collapsed: projection.reasoningMerge
                ? entry.collapsed
                : entry.collapsed.filter((value) => value !== "reasoning_merge"),
            })),
          error: null,
        };
      } catch (error) {
        logger.warn({ err: error, agentId }, "Unable to read remote timeline");
        return {
          ...reply,
          agent: null,
          epoch: "",
          reset: false,
          staleCursor: false,
          gap: false,
          window: { minSeq: 0, maxSeq: 0, nextSeq: 0 },
          startCursor: null,
          endCursor: null,
          hasOlder: false,
          hasNewer: false,
          entries: [],
          error:
            error instanceof WorkerOperationError &&
            ["worker_plugin_session_busy", "worker_native_session_busy"].includes(error.message)
              ? error.message
              : "worker_timeline_unavailable",
        };
      }
    },
    async waitForFinish(agentId: string, timeoutMs: number | undefined, request: AbortSignal) {
      const linked = AbortSignal.any([signal, request]);
      const snapshot = manager.getAgent(agentId);
      if (!snapshot) {
        const final = await readAgent(agentId);
        let status: "timeout" | "idle" | "error" = "error";
        if (final) status = final.status === "running" ? "timeout" : "idle";
        return {
          status,
          final,
          lastMessage: null,
          error: final ? null : "agent_not_found",
        };
      }
      const timeout = AbortSignal.timeout(Math.min(timeoutMs ?? 60_000, 2_147_483_647));
      const result = await manager
        .waitForAgentEvent(agentId, { signal: AbortSignal.any([linked, timeout]) })
        .catch((error: unknown) => {
          if (timeout.aborted && !linked.aborted)
            return { status: "running" as const, permission: null, lastMessage: null };
          throw error;
        });
      const final = await readAgent(agentId);
      let status: "permission" | "error" | "timeout" | "idle" = "idle";
      if (result.permission) status = "permission";
      else if (result.status === "error") status = "error";
      else if (result.status === "running") status = "timeout";
      return {
        status,
        final,
        lastMessage: result.lastMessage,
        error: final?.lastError ?? null,
      };
    },
    observeCreation: (
      kind: CreationSnapshot["kind"],
      key: string,
      observe: (value: CreationSnapshot) => void,
    ) => creations.subscribe(kind, key, observe),
    close,
  };
}
