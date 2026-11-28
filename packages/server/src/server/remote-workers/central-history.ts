import type { SessionInboundMessage, SessionOutboundMessage } from "@getpaseo/protocol/messages";
import type { Logger } from "pino";
import { buildAgentForkContextAttachment } from "../agent/activity-curator.js";
import { ensureAgentLoaded, ensureUnarchivedAgentLoaded } from "../agent/agent-loading.js";
import type { AgentManager, ManagedAgent } from "../agent/agent-manager.js";
import type { AgentStorage } from "../agent/agent-storage.js";
import {
  ImportSessionsRequestError,
  importProviderSession,
  listImportableProviderSessions,
  normalizeImportAgentRequest,
  type ImportProviderSessionInput,
  type ListImportableProviderSessionsInput,
} from "../agent/import-sessions.js";
import { serializeAgentSnapshot } from "../messages.js";
import { queryAgentTimeline } from "../session/agent-history/timeline-query.js";

type HistoryRequest = Extract<
  SessionInboundMessage,
  {
    type:
      | "agent.timeline.search.request"
      | "agent.timeline.list_prompts.request"
      | "agent.fork_context.request"
      | "agent.rewind.request"
      | "fetch_recent_provider_sessions_request"
      | "import_agent_request";
  }
>;

type HistoryDependencies = {
  agentManager: ImportProviderSessionInput["agentManager"] &
    Pick<AgentManager, "listAgents" | "listImportableSessions" | "fetchTimeline" | "getTimelineRows" | "rewind">;
  agentStorage: AgentStorage;
  logger: Logger;
  providerSnapshotManager: ListImportableProviderSessionsInput["providerSnapshotManager"];
  workspaceProvisioning: ImportProviderSessionInput["workspaceProvisioning"];
  agentPayload(snapshot: ManagedAgent): Promise<ReturnType<typeof serializeAgentSnapshot>>;
};

export function createCentralHistory(deps: HistoryDependencies) {
  const { agentManager, logger } = deps;

  return async (request: HistoryRequest): Promise<SessionOutboundMessage> => {
    const { requestId } = request;
    switch (request.type) {
      case "agent.timeline.search.request":
      case "agent.timeline.list_prompts.request":
        return queryAgentTimeline(deps, request);
      case "agent.fork_context.request": {
        const { agentId, boundaryCursor, boundaryMessageId } = request;
        try {
          const snapshot = await ensureAgentLoaded(agentId, { ...deps, purpose: "history" });
          const payload = await deps.agentPayload(snapshot);
          const timeline = agentManager.fetchTimeline(agentId, { direction: "tail", limit: 0 });
          return {
            type: "agent.fork_context.response",
            payload: {
              requestId,
              agentId,
              ...buildAgentForkContextAttachment({
                rows: timeline.rows,
                cursorBoundary: boundaryCursor
                  ? { timelineEpoch: timeline.epoch, cursor: boundaryCursor }
                  : null,
                boundaryMessageId,
                agentTitle: payload.title,
                cwd: snapshot.cwd,
              }),
              error: null,
            },
          };
        } catch (error) {
          logger.error({ err: error, agentId }, "Failed to load agent fork context");
          return {
            type: "agent.fork_context.response",
            payload: {
              requestId,
              agentId,
              attachment: null,
              itemCount: 0,
              boundaryCursor: boundaryCursor ?? null,
              boundaryMessageId: boundaryMessageId ?? null,
              error: error instanceof Error ? error.message : String(error),
            },
          };
        }
      }
      case "agent.rewind.request": {
        const { agentId, messageId, mode } = request;
        try {
          await ensureUnarchivedAgentLoaded(agentId, { ...deps, purpose: "history" });
          await agentManager.rewind(agentId, messageId, mode);
          return {
            type: "agent.rewind.response",
            payload: { requestId, agentId, ok: true, error: null },
          };
        } catch (error) {
          return {
            type: "agent.rewind.response",
            payload: {
              requestId,
              agentId,
              ok: false,
              error: error instanceof Error ? error.message : "Failed to rewind agent",
            },
          };
        }
      }
      case "fetch_recent_provider_sessions_request": {
        try {
          const { entries, filteredAlreadyImportedCount, providerErrors } =
            await listImportableProviderSessions({ ...deps, request });
          return {
            type: "fetch_recent_provider_sessions_response",
            payload: {
              requestId,
              entries,
              ...(filteredAlreadyImportedCount > 0 ? { filteredAlreadyImportedCount } : {}),
              ...(providerErrors.length > 0 ? { providerErrors } : {}),
            },
          };
        } catch (error) {
          logger.error({ err: error }, "Failed to fetch recent provider sessions");
          return {
            type: "rpc_error",
            payload: {
              requestId,
              requestType: request.type,
              error:
                error instanceof Error ? error.message : "Failed to fetch recent provider sessions",
              code:
                error instanceof ImportSessionsRequestError
                  ? error.code
                  : "fetch_recent_provider_sessions_failed",
            },
          };
        }
      }
      case "import_agent_request": {
        try {
          const normalized = normalizeImportAgentRequest(request);
          if ("error" in normalized) throw new Error(normalized.error);
          if (!agentManager.getRegisteredProviderIds().includes(normalized.provider)) {
            throw new Error("worker_provider_unavailable");
          }
          const { snapshot, timelineSize } = await importProviderSession({ ...deps, request: normalized });
          return {
            type: "status",
            payload: {
              status: "agent_resumed",
              agentId: snapshot.id,
              requestId,
              timelineSize,
              agent: await deps.agentPayload(snapshot),
            },
          };
        } catch (error) {
          logger.error({ err: error }, "Failed to import agent");
          return {
            type: "status",
            payload: {
              status: "agent_create_failed",
              requestId,
              error: error instanceof Error ? error.message : String(error),
            },
          };
        }
      }
    }
  };
}
