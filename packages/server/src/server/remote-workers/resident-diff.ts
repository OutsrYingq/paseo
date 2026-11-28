import { encodeDiffFrame } from "@getpaseo/protocol/binary-frames/index";
import { expandTilde } from "../../utils/path.js";
import type { CheckoutDiffManager } from "../checkout-diff-manager.js";
import type { DiffSubscription, DiffSubscriptions } from "../diff/index.js";
import type { RemoteDiffEvent, RemoteDiffOperation } from "./remote-diff-protocol.js";
import { WorkerOperationError } from "./execution-protocol.js";
import { resolveResidentWorkspacePath } from "./resident-projects.js";

export function openResidentDiffChannel(options: {
  diffs: DiffSubscriptions;
  legacy: Pick<CheckoutDiffManager, "subscribe">;
  roots: readonly string[];
  signal: AbortSignal;
  publish(event: RemoteDiffEvent): void;
}) {
  const { diffs, roots, signal, publish } = options;
  const active = new Map<string, { source: DiffSubscription; acknowledge?: () => void }>();
  const legacy = new Map<string, AbortController>();
  async function release(id: string) {
    const entry = active.get(id);
    legacy.get(id)?.abort();
    legacy.delete(id);
    if (!entry) return;
    active.delete(id);
    entry.acknowledge?.();
    await entry.source.release();
  }
  return {
    async execute(operation: RemoteDiffOperation) {
      const { id } = operation;
      switch (operation.kind) {
        case "diffOpen": {
          if (active.has(id)) throw new WorkerOperationError("invalid_request");
          const cwd = await resolveResidentWorkspacePath(roots, expandTilde(operation.request.cwd));
          signal.throwIfAborted();
          const source = await diffs.subscribe({ ...operation.request, cwd }, async (frame) => {
            const entry = active.get(id);
            if (!entry || signal.aborted) return;
            const bytes = Buffer.from(encodeDiffFrame(frame)).toString("base64");
            await new Promise<void>((resolve) => {
              entry.acknowledge = resolve;
              publish({ type: "diffFrame", id, bytes });
            });
          });
          if (signal.aborted || active.has(id)) {
            await source.release();
            throw new WorkerOperationError("worker_diff_unavailable");
          }
          active.set(id, { source });
          break;
        }
        case "diffStart": {
          const entry = active.get(id);
          if (!entry) throw new WorkerOperationError("worker_diff_unavailable");
          await entry.source.start();
          break;
        }
        case "diffAck": {
          const entry = active.get(id);
          const acknowledge = entry?.acknowledge;
          if (entry) entry.acknowledge = undefined;
          acknowledge?.();
          break;
        }
        case "diffClose":
          await release(id);
          break;
        case "legacyDiffOpen": {
          if (legacy.has(id) || active.has(id)) throw new WorkerOperationError("invalid_request");
          const lifetime = new AbortController();
          legacy.set(id, lifetime);
          const subscriptionSignal = AbortSignal.any([signal, lifetime.signal]);
          try {
            const { cwd: requestedPath, compare } = operation.request;
            const cwd = await resolveResidentWorkspacePath(roots, expandTilde(requestedPath));
            subscriptionSignal.throwIfAborted();
            const subscription = await options.legacy.subscribe(
              { cwd, compare, signal: subscriptionSignal },
              (snapshot) => publish({ type: "legacyDiffSnapshot", id, snapshot }),
            );
            if (subscriptionSignal.aborted) {
              subscription.unsubscribe();
              subscriptionSignal.throwIfAborted();
            }
            subscriptionSignal.addEventListener("abort", subscription.unsubscribe, { once: true });
            return subscription.initial;
          } catch (error) {
            legacy.delete(id);
            throw error;
          }
        }
      }
      return null;
    },
    async close() {
      for (const lifetime of legacy.values()) lifetime.abort();
      legacy.clear();
      await Promise.all([...active.keys()].map(release));
    },
  };
}
