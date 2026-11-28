import type { Logger } from "pino";
import { ensureAgentLoaded } from "../../agent/agent-loading.js";
import type { AgentManager } from "../../agent/agent-manager.js";
import { searchTimeline } from "../../agent/chat-search/index.js";
import { buildTimelinePromptIndex } from "../../agent/timeline-prompt-index.js";
import type { SessionInboundMessage, SessionOutboundMessage } from "../../messages.js";

type TimelineQuery = Extract<SessionInboundMessage, {
  type: "agent.timeline.search.request" | "agent.timeline.list_prompts.request";
}>;

type TimelineQueryDependencies = Parameters<typeof ensureAgentLoaded>[1] & {
  agentManager: Pick<AgentManager, "getTimelineRows" | "fetchTimeline">;
  logger: Logger;
};

export async function queryAgentTimeline(
  deps: TimelineQueryDependencies,
  request: TimelineQuery,
): Promise<SessionOutboundMessage> {
  const { agentId, requestId } = request;
  const { agentManager, logger } = deps;
  const fetchEpoch = () => agentManager.fetchTimeline(agentId, { direction: "tail", limit: 1 }).epoch;
  try {
    await ensureAgentLoaded(agentId, deps);
    const rows = await agentManager.getTimelineRows(agentId);
    const epoch = fetchEpoch();
    if (request.type === "agent.timeline.list_prompts.request") {
      return {
        type: "agent.timeline.list_prompts.response",
        payload: { agentId, requestId, ...buildTimelinePromptIndex(epoch, rows), error: null },
      };
    }
    const result = await searchTimeline({ rows, query: request.query, cursor: request.cursor });
    if (fetchEpoch() !== epoch) throw new Error("History changed; search again");
    return {
      type: "agent.timeline.search.response",
      payload: { agentId, requestId, epoch, ...result, error: null },
    };
  } catch (cause) {
    const error = cause instanceof Error ? cause.message : String(cause);
    if (request.type === "agent.timeline.list_prompts.request") {
      logger.error({ err: cause, agentId }, "Failed to handle agent.timeline.list_prompts.request");
      return {
        type: "agent.timeline.list_prompts.response",
        payload: { agentId, requestId, epoch: "", prompts: [], error },
      };
    }
    return {
      type: "agent.timeline.search.response",
      payload: { agentId, requestId, epoch: "", locations: [], nextCursor: null, error },
    };
  }
}
