import type { SessionInboundMessage, SessionOutboundMessage } from "@getpaseo/protocol/messages";
import type { Logger } from "pino";
import type { AgentManager } from "../agent/agent-manager.js";
import type { ProviderSnapshotManager } from "../agent/provider-snapshot-manager.js";
import type { CheckoutAction } from "../session/checkout/checkout-actions-protocol.js";
import {
  createAgentStructuredTextGeneration,
  createGitMetadataCompletion,
} from "../session/checkout/git-metadata-generator.js";
import { isCheckoutAction } from "../session/checkout/checkout-actions-protocol.js";
import { isCheckoutQuery } from "../session/checkout/checkout-queries-protocol.js";
import type { WorkerConnection } from "./worker-connection.js";

export function createCentralCheckout(
  connection: WorkerConnection,
  updateBranch: (cwd: string, branch: string | null) => Promise<void>,
  agents: {
    manager: AgentManager;
    catalog: ProviderSnapshotManager;
    logger: Logger;
  },
) {
  async function fillMetadata(request: CheckoutAction, focusedAgentId: string | null): Promise<CheckoutAction> {
    const commit = request.type === "checkout_commit_request" && !request.message?.trim();
    const pull = request.type === "checkout_pr_create_request" && (!request.title?.trim() || !request.body?.trim());
    if (!commit && !pull) return request;
    const input = await connection.call({
      kind: "checkoutMetadata",
      cwd: request.cwd,
      mode: commit ? "commit" : "pullRequest",
      baseRef: pull ? request.baseRef : undefined,
    });
    const focused = focusedAgentId ? agents.manager.getAgent(focusedAgentId) : null;
    const completion = createGitMetadataCompletion({
      logger: agents.logger,
      generation: createAgentStructuredTextGeneration({
        agentManager: agents.manager,
        providerSnapshotManager: agents.catalog,
        readDaemonConfig: () => ({ metadataGeneration: input.metadataGeneration }),
        getFocusedSelection: () => {
          if (!focused || focused.internal || (focused.cwd !== request.cwd && focused.cwd !== input.cwd)) return;
          return {
            provider: focused.provider,
            model: focused.runtimeInfo?.model ?? focused.config.model,
            thinkingOptionId: focused.runtimeInfo?.thinkingOptionId ?? focused.config.thinkingOptionId,
          };
        },
        logger: agents.logger,
      }),
    });
    if (request.type === "checkout_commit_request") {
      return { ...request, message: await completion.commit(input.cwd, input.prompt) };
    }
    const { title, body } = await completion.pullRequest(input.cwd, input.prompt);
    return { ...request, title: request.title?.trim() || title, body: request.body?.trim() || body };
  }

  return function dispatchCheckout(
    message: SessionInboundMessage,
    focusedAgentId: string | null,
    signal: AbortSignal,
  ): Promise<SessionOutboundMessage> | undefined {
    if (isCheckoutQuery(message)) {
      return connection.call({ kind: "checkoutQuery", request: message });
    }
    if (isCheckoutAction(message)) {
      return (async () => {
        const request = await fillMetadata(message, focusedAgentId);
        signal.throwIfAborted();
        const result = await connection.call({ kind: "checkoutAction", request });
        if (result.branchSnapshot && "cwd" in result.response.payload) {
          await updateBranch(result.response.payload.cwd, result.branchSnapshot.branchName);
        }
        return result.response;
      })();
    }
  };
}
