import type { Logger } from "pino";
import type { AgentManager } from "../agent-manager.js";
import type { AgentStorage } from "../agent-storage.js";
import { sendPreparedPromptToAgent, waitForAgentRunStartWithTimeout } from "../agent-prompt.js";
import { createAgentMessageDelivery } from "./delivery.js";

export function createManagerMessageDelivery({
  agentManager,
  agentStorage,
  logger,
  signal,
}: {
  agentManager: AgentManager;
  agentStorage: AgentStorage;
  logger: Logger;
  signal?: AbortSignal;
}) {
  return createAgentMessageDelivery({
    tryRunOutOfBand: (request, onAccepted) => {
      signal?.throwIfAborted();
      return agentManager.tryRunOutOfBand(
        request.recipientId,
        request.prompt,
        { clientMessageId: request.messageIds[0] },
        onAccepted,
      );
    },
    isRunning: (agentId) => agentManager.hasInFlightRun(agentId),
    canSteer: (agentId) => agentManager.canSteer(agentId),
    hasPendingPermission: (agentId) => agentManager.hasPendingPermissions(agentId),
    subscribe: (notify) =>
      agentManager.subscribe(
        (event) => {
          if (event.type === "agent_state") notify(event.agent.id);
        },
        { replayState: false },
      ),
    onStop: (notify) => agentManager.onStop(notify),
    async dispatch(batch) {
      signal?.throwIfAborted();
      const dispatch = await sendPreparedPromptToAgent({
        agentManager,
        agentStorage,
        agentId: batch.recipientId,
        prompt: batch.prompt,
        activeTurnBehavior: batch.activeTurnBehavior,
        idleOnly: batch.idleOnly,
        admit: signal ? () => !signal.aborted && batch.admit() : batch.admit,
        sessionMode: batch.sessionMode,
        clearPendingPermissions: batch.clearPendingPermissions,
        unarchive: batch.unarchive,
        runOptions: { clientMessageId: batch.messageIds[0] },
        logger,
      });
      if (dispatch.disposition === "turn_started") {
        await waitForAgentRunStartWithTimeout(
          agentManager,
          batch.recipientId,
          dispatch.started,
          signal,
        );
      }
      return dispatch.disposition;
    },
    awaitStart: (id, started) => waitForAgentRunStartWithTimeout(agentManager, id, started, signal),
    onError: (err, batch) =>
      logger.error(
        { err, agentId: batch.recipientId, messageIds: batch.messageIds },
        "Agent message batch dispatch failed",
      ),
  });
}
