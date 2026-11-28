import { randomUUID } from "node:crypto";
import { decodeDiffFrame } from "@getpaseo/protocol/binary-frames/index";
import type { DiffSubscriptions } from "../diff/index.js";
import type { CheckoutDiffManager, CheckoutDiffSnapshotPayload } from "../checkout-diff-manager.js";
import type { RemoteDiffEvent } from "./remote-diff-protocol.js";
import type { WorkerConnection } from "./worker-connection.js";

export function createRemoteDiffSubscriptions(connection: WorkerConnection) {
  const deliveries = new Map<string, (event: Extract<RemoteDiffEvent, { type: "diffFrame" }>) => Promise<void>>();
  const legacyListeners = new Map<string, (snapshot: CheckoutDiffSnapshotPayload) => void>();
  connection.signal.addEventListener("abort", () => {
    deliveries.clear();
    legacyListeners.clear();
  }, { once: true });
  const subscribe: DiffSubscriptions["subscribe"] = async (compare, emit) => {
    const id = randomUUID();
    async function release() {
      if (deliveries.delete(id) && !connection.signal.aborted) {
        await connection.call({ kind: "diffClose", id });
      }
    }
    deliveries.set(id, async ({ bytes }) => {
      await emit(decodeDiffFrame(Buffer.from(bytes, "base64"))).catch(release);
      if (deliveries.has(id)) await connection.call({ kind: "diffAck", id });
    });
    try {
      await connection.call({
        kind: "diffOpen",
        id,
        request: { ...compare, type: "diff.subscribe.request", requestId: id },
      });
    } catch (error) {
      deliveries.delete(id);
      throw error;
    }
    return {
      async start() {
        await connection.call({ kind: "diffStart", id });
      },
      release,
    };
  };
  const subscribeLegacy: CheckoutDiffManager["subscribe"] = async ({ cwd, compare, signal }, notify) => {
    signal?.throwIfAborted();
    const id = randomUUID();
    function unsubscribe() {
      signal?.removeEventListener("abort", unsubscribe);
      if (legacyListeners.delete(id) && !connection.signal.aborted) {
        void connection.call({ kind: "diffClose", id }).catch(() => connection.close());
      }
    }
    legacyListeners.set(id, notify);
    signal?.addEventListener("abort", unsubscribe, { once: true });
    try {
      const initial = await connection.call({
        kind: "legacyDiffOpen",
        id,
        request: { type: "subscribe_checkout_diff_request", cwd, compare, requestId: id },
      });
      return { initial, unsubscribe };
    } catch (error) {
      unsubscribe();
      throw error;
    }
  };
  return {
    subscribe,
    legacy: { subscribe: subscribeLegacy },
    deliver(event: RemoteDiffEvent) {
      if (event.type === "legacyDiffSnapshot") {
        legacyListeners.get(event.id)?.(event.snapshot);
        return;
      }
      void deliveries.get(event.id)?.(event).catch(() => connection.close());
    },
  };
}
