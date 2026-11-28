import type { SessionInboundMessage, SessionOutboundMessage } from "@getpaseo/protocol/messages";
import { isCheckoutAction } from "../session/checkout/checkout-actions-protocol.js";
import { isCheckoutQuery } from "../session/checkout/checkout-queries-protocol.js";
import type { WorkerConnection } from "./worker-connection.js";

export function createCentralCheckout(
  connection: WorkerConnection,
  updateBranch: (cwd: string, branch: string | null) => Promise<void>,
) {
  return function dispatchCheckout(message: SessionInboundMessage): Promise<SessionOutboundMessage> | undefined {
    if (isCheckoutQuery(message)) {
      return connection.call({ kind: "checkoutQuery", request: message });
    }
    if (isCheckoutAction(message)) {
      return connection.call({ kind: "checkoutAction", request: message }).then(async (result) => {
        if (result.branchSnapshot && "cwd" in result.response.payload) {
          await updateBranch(result.response.payload.cwd, result.branchSnapshot.branchName);
        }
        return result.response;
      });
    }
  };
}
