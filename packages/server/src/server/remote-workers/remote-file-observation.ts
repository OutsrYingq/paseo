import { randomUUID } from "node:crypto";
import type { FileVersion, SessionInboundMessage, SessionOutboundMessage } from "@getpaseo/protocol/messages";
import type { Logger } from "pino";
import { DiffSession } from "../diff/session.js";
import type { createRemoteDiffSubscriptions } from "./remote-diff.js";
import { CLIENT_CAPS } from "@getpaseo/protocol/client-capabilities";
import { WorkspaceFileObservation } from "../session/files/workspace-files-session.js";
import { SessionDelivery } from "../session/owned-subscriptions/index.js";
import { subscribeLegacyCheckoutDiff } from "../session/checkout/legacy-checkout-diff.js";
import type { WorkerConnection } from "./worker-connection.js";
import type { WorkerEvent } from "./execution-protocol.js";

export function createRemoteFileObserver(connection: WorkerConnection) {
  const listeners = new Map<string, (version: FileVersion) => void>();
  connection.signal.addEventListener("abort", () => listeners.clear(), { once: true });
  return {
    deliver(event: Extract<WorkerEvent, { type: "fileVersion" }>) {
      listeners.get(event.id)?.(event.version);
    },
    async subscribe(target: { cwd: string; path: string }, listener: (version: FileVersion) => void) {
      const id = randomUUID();
      listeners.set(id, listener);
      try {
        const initial = await connection.call({ kind: "observeWorkspaceFile", id, ...target });
        return {
          initial,
          unsubscribe: () => {
            if (listeners.delete(id)) {
              return connection.call({ kind: "unobserveWorkspaceFile", id }).then(() => undefined);
            }
          },
        };
      } catch (error) {
        listeners.delete(id);
        throw error;
      }
    },
  };
}

export function createRemoteWorkspaceObservations(options: {
  observer: ReturnType<typeof createRemoteFileObserver>;
  diffSubscriptions: ReturnType<typeof createRemoteDiffSubscriptions>;
  logger: Logger;
  sendBinary(bytes: Uint8Array): Promise<void>;
  socket: object;
  capabilities: Record<string, unknown> | undefined;
  send(message: SessionOutboundMessage): void;
}) {
  const { socket, send } = options;
  const delivery = new SessionDelivery(
    (_source, message) => send(message),
    (_source, bytes) => options.sendBinary(bytes),
  );
  delivery.attach(socket, options.capabilities?.[CLIENT_CAPS.ownedSubscriptions] === true);
  const observation = new WorkspaceFileObservation({ emit: send }, options.observer);
  const diff = new DiffSession(options.diffSubscriptions, options.logger);
  return {
    dispatch(message: SessionInboundMessage) {
      if (message.type === "fs.file.subscribe.request") {
        return delivery.request(socket, message, () => observation.handleFileSubscribeRequest(message, delivery));
      }
      if (message.type === "fs.file.unsubscribe.request") {
        return delivery.request(socket, message, () => observation.handleFileUnsubscribeRequest(message, delivery));
      }
      if (message.type === "diff.subscribe.request") {
        return delivery.request(socket, message, () => diff.subscribe(message, delivery, send));
      }
      if (message.type === "diff.unsubscribe.request") {
        return delivery.request(socket, message, () => delivery.release(message.subscriptionId));
      }
      if (message.type === "subscribe_checkout_diff_request") {
        return delivery.request(socket, message, () =>
          subscribeLegacyCheckoutDiff(message, delivery, options.diffSubscriptions.legacy, send),
        );
      }
      if (message.type === "unsubscribe_checkout_diff_request") {
        return delivery.request(socket, message, () => delivery.release(message.subscriptionId));
      }
    },
    release(id: string) {
      return delivery.release(id);
    },
    close() {
      return delivery.detach(socket);
    },
  };
}
