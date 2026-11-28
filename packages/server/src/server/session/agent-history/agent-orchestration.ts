import { CLIENT_CAPS, type ClientCapability } from "@getpaseo/protocol/client-capabilities";
import { getErrorMessage } from "@getpaseo/protocol/error-utils";
import { ensureUnarchivedAgentLoaded } from "../../agent/agent-loading.js";
import type { AgentManager, AgentManagerEvent, ManagedAgent } from "../../agent/agent-manager.js";
import type { AgentStorage, StoredAgentRecord } from "../../agent/agent-storage.js";
import {
  isDaemonBackgroundWorkItem,
  type AgentBackgroundWorkItem,
} from "../../agent/background-work/index.js";
import type { SessionAuthorization } from "../../authorization/index.js";
import type { SessionInboundMessage, SessionOutboundMessage } from "../../messages.js";

export type AgentOrchestrationRequest = Extract<
  SessionInboundMessage,
  {
    type:
      | "agent.provider_subagents.list.request"
      | "agent.provider_subagents.timeline.get.request"
      | "agent.background_work.list.request"
      | "agent.background_work.stop.request";
  }
>;

type SupportsCapability = (capability: ClientCapability) => boolean;
type ProviderSubagentEvent = Extract<AgentManagerEvent, { type: "provider_subagent" }>["event"];
type ProviderSubagentUpdate = Extract<
  SessionOutboundMessage,
  { type: "agent.provider_subagents.update" }
>;
type ProviderSubagentTimelineRequest = Extract<
  AgentOrchestrationRequest,
  { type: "agent.provider_subagents.timeline.get.request" }
>;

interface BackgroundWorkDependencies {
  agentManager: Pick<
    AgentManager,
    "getAgent" | "getAgentBackgroundWorkSummary" | "listAgentBackgroundWork"
  >;
  agentStorage: Pick<AgentStorage, "get">;
  authorization: Pick<SessionAuthorization, "allowsPermission" | "allowsInbound">;
}

type AgentOrchestrationDependencies = Parameters<typeof ensureUnarchivedAgentLoaded>[1] &
  BackgroundWorkDependencies & {
    agentManager: Pick<
      AgentManager,
      | "listProviderSubagents"
      | "getProviderSubagent"
      | "fetchProviderSubagentTimeline"
      | "stopAgentBackgroundWork"
    >;
    supports: SupportsCapability;
  };

function supportsTimelineItem(item: { type: string }, supports: SupportsCapability): boolean {
  if (item.type === "notification") return supports(CLIENT_CAPS.timelineNotifications);
  if (item.type === "plugin") return supports(CLIENT_CAPS.pluginTimelineItems);
  return true;
}

// The caller authorizes the RPC before dispatch, with the manager and storage from its worker.
export async function handleAgentOrchestrationRequest(
  deps: AgentOrchestrationDependencies,
  request: AgentOrchestrationRequest,
): Promise<SessionOutboundMessage> {
  const { requestId } = request;
  const { agentManager, authorization } = deps;
  switch (request.type) {
    case "agent.provider_subagents.list.request": {
      const { parentAgentId } = request;
      let subagents: ReturnType<AgentManager["listProviderSubagents"]> = [];
      let error: string | null = null;
      try {
        await ensureUnarchivedAgentLoaded(parentAgentId, deps);
        subagents = agentManager.listProviderSubagents(parentAgentId);
      } catch (cause) {
        error = getErrorMessage(cause);
      }
      return {
        type: "agent.provider_subagents.list.response",
        payload: { requestId, parentAgentId, subagents, error },
      };
    }
    case "agent.provider_subagents.timeline.get.request":
      return readProviderSubagentTimeline(deps, request);
    case "agent.background_work.list.request": {
      const { agentId } = request;
      let items: AgentBackgroundWorkItem[] = [];
      let error: string | null = null;
      try {
        const target = await resolveBackgroundWorkAgent(deps, agentId);
        if ("error" in target) throw new Error(target.error);
        if (!agentManager.getAgentBackgroundWorkSummary(agentId)) {
          throw new Error("Background work inventory is unavailable");
        }
        items = projectBackgroundWorkItems(
          authorization,
          agentManager.listAgentBackgroundWork(agentId),
        );
      } catch (cause) {
        error = getErrorMessage(cause);
      }
      return {
        type: "agent.background_work.list.response",
        payload: { requestId, agentId, items, error },
      };
    }
    case "agent.background_work.stop.request": {
      const { agentId, itemId, stopToken } = request;
      let error: string | null = null;
      try {
        const target = await resolveBackgroundWorkAgent(deps, agentId);
        if ("error" in target) throw new Error(target.error);
        await agentManager.stopAgentBackgroundWork({
          agentId,
          itemId,
          stopToken,
          canManageAutomation: authorization.allowsPermission("automation.manage"),
        });
      } catch (cause) {
        error = getErrorMessage(cause);
      }
      return {
        type: "agent.background_work.stop.response",
        payload: { requestId, agentId, itemId, error },
      };
    }
  }
}

async function readProviderSubagentTimeline(
  deps: AgentOrchestrationDependencies,
  request: ProviderSubagentTimelineRequest,
): Promise<
  Extract<SessionOutboundMessage, { type: "agent.provider_subagents.timeline.get.response" }>
> {
  const { requestId, parentAgentId, subagentId, cursor } = request;
  const { agentManager, supports } = deps;
  const direction = request.direction ?? (cursor ? "after" : "tail");
  const identity = { requestId, parentAgentId, subagentId, direction };
  try {
    await ensureUnarchivedAgentLoaded(parentAgentId, deps);
    const descriptor = agentManager.getProviderSubagent(parentAgentId, subagentId);
    if (!descriptor) throw new Error("Provider subagent not found");
    // COMPAT(projectedSubagentTimeline): added after v0.8.0, remove after 2027-03-14.
    const supportsProjection = supports(CLIENT_CAPS.projectedSubagentTimeline);
    const timeline = agentManager.fetchProviderSubagentTimeline(parentAgentId, subagentId, {
      direction,
      cursor,
      limit: request.limit ?? (direction === "after" ? 0 : 200),
    });
    return {
      type: "agent.provider_subagents.timeline.get.response",
      payload: {
        ...identity,
        provider: descriptor.provider,
        epoch: timeline.epoch,
        projection: "projected",
        startCursor:
          timeline.startSeq === null ? null : { epoch: timeline.epoch, seq: timeline.startSeq },
        endCursor:
          timeline.endSeq === null ? null : { epoch: timeline.epoch, seq: timeline.endSeq },
        reset: timeline.reset,
        staleCursor: timeline.staleCursor,
        gap: timeline.gap,
        window: timeline.window,
        hasOlder: supportsProjection && timeline.hasOlder,
        hasNewer: supportsProjection && timeline.hasNewer,
        rows: supportsProjection
          ? timeline.rows
              .filter((row) => supportsTimelineItem(row.item, supports))
              .map((row) => ({
                item: row.item,
                timestamp: row.timestamp,
                seq: row.seqEnd,
                seqStart: row.seqStart,
                seqEnd: row.seqEnd,
                sourceSeqRanges: row.sourceSeqRanges,
              }))
          : [
              {
                seq: timeline.window.maxSeq,
                timestamp: new Date().toISOString(),
                item: {
                  type: "assistant_message",
                  text: "Please upgrade the Paseo app to view this subagent conversation.",
                },
              },
            ],
        error: null,
      },
    };
  } catch (cause) {
    return {
      type: "agent.provider_subagents.timeline.get.response",
      payload: {
        ...identity,
        provider: null,
        epoch: "",
        reset: false,
        staleCursor: false,
        gap: false,
        window: { minSeq: 0, maxSeq: 0, nextSeq: 0 },
        hasOlder: false,
        hasNewer: false,
        rows: [],
        error: getErrorMessage(cause),
      },
    };
  }
}

export function projectProviderSubagentUpdate({
  agentManager,
  event,
  supports,
}: {
  agentManager: Pick<AgentManager, "getAgent">;
  event: ProviderSubagentEvent;
  supports: SupportsCapability;
}): ProviderSubagentUpdate | null {
  const parentAgentId =
    event.type === "upsert" ? event.subagent.parentAgentId : event.parentAgentId;
  if (agentManager.getAgent(parentAgentId)?.internal) return null;
  switch (event.type) {
    case "upsert":
      return {
        type: "agent.provider_subagents.update",
        payload: { kind: "upsert", subagent: event.subagent },
      };
    case "timeline":
      if (
        !supports(CLIENT_CAPS.projectedSubagentTimeline) ||
        !supportsTimelineItem(event.row.item, supports)
      )
        return null;
      return {
        type: "agent.provider_subagents.update",
        payload: {
          kind: "timeline",
          parentAgentId,
          subagentId: event.subagentId,
          provider: event.provider,
          item: event.row.item,
          timestamp: event.row.timestamp,
          seq: event.row.seq,
          epoch: event.epoch,
        },
      };
    case "remove":
      return {
        type: "agent.provider_subagents.update",
        payload: { kind: "remove", parentAgentId, subagentId: event.subagentId },
      };
  }
}

export async function readBackgroundWorkUpdate(deps: BackgroundWorkDependencies, agentId: string) {
  const target = await resolveBackgroundWorkAgent(deps, agentId);
  if ("error" in target) return null;
  // The inventory may have changed while resolving a dormant agent.
  const items = deps.agentManager.getAgentBackgroundWorkSummary(agentId)
    ? deps.agentManager.listAgentBackgroundWork(agentId)
    : [];
  const message: Extract<SessionOutboundMessage, { type: "agent.background_work.update" }> = {
    type: "agent.background_work.update",
    payload: { agentId, items: projectBackgroundWorkItems(deps.authorization, items) },
  };
  return { ...target, message };
}

function projectBackgroundWorkItems(
  authorization: BackgroundWorkDependencies["authorization"],
  items: AgentBackgroundWorkItem[],
): AgentBackgroundWorkItem[] {
  if (
    authorization.allowsPermission("automation.manage") &&
    authorization.allowsInbound({ type: "agent.background_work.stop.request" })
  ) {
    return items;
  }
  return items.map((item) => {
    if (!isDaemonBackgroundWorkItem(item)) return item;
    const { canStop: _canStop, stopToken: _stopToken, ...visible } = item;
    return visible;
  });
}

async function resolveBackgroundWorkAgent(
  deps: Pick<BackgroundWorkDependencies, "agentStorage" | "agentManager">,
  agentId: string,
): Promise<{ agent: ManagedAgent | null; record: StoredAgentRecord | null } | { error: string }> {
  const record = await deps.agentStorage.get(agentId);
  const agent = deps.agentManager.getAgent(agentId);
  if ((!agent && !record) || agent?.internal || record?.internal) {
    return { error: `Unknown agent '${agentId}'` };
  }
  if (record?.archivedAt) return { error: `Agent is archived: ${agentId}` };
  return { agent, record };
}
