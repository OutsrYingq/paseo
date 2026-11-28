import type { SubscribeCheckoutDiffRequest } from "../../messages.js";
import type {
  CheckoutDiffSnapshotPayload,
  CheckoutDiffSubscription,
} from "../../checkout-diff-manager.js";
import type { SessionDelivery } from "../owned-subscriptions/index.js";
import { expandTilde } from "../../../utils/path.js";
import type { CheckoutDiffSubscriber, CheckoutSessionHost } from "./checkout-session.js";

export async function subscribeLegacyCheckoutDiff(
  msg: SubscribeCheckoutDiffRequest,
  ownership: SessionDelivery,
  diff: Pick<CheckoutDiffSubscriber, "subscribe">,
  emit: CheckoutSessionHost["emit"],
): Promise<void> {
  let bootstrap: Promise<CheckoutDiffSubscription> | undefined;
  const owner = ownership.begin(
    "diffs",
    msg.subscriptionId,
    async () => {
      await bootstrap?.then(
        (subscription) => subscription.unsubscribe(),
        () => undefined,
      );
    },
    `diff:${msg.subscriptionId}`,
  );
  let ready = false;
  let pending: CheckoutDiffSnapshotPayload | null = null;
  const emitSnapshot = (snapshot: CheckoutDiffSnapshotPayload) =>
    owner.emit({
      type: "checkout_diff_update",
      payload: { ...snapshot, subscriptionId: owner.responseId },
    });
  try {
    bootstrap = diff.subscribe(
      { cwd: expandTilde(msg.cwd), compare: msg.compare, signal: owner.signal },
      (snapshot) => {
        if (owner.signal.aborted) return;
        if (ready) emitSnapshot(snapshot);
        else pending = snapshot;
      },
    );
    const subscription = await bootstrap;
    if (owner.signal.aborted) {
      subscription.unsubscribe();
      return;
    }
    emit({
      type: "subscribe_checkout_diff_response",
      payload: {
        ...subscription.initial,
        subscriptionId: owner.responseId,
        requestId: msg.requestId,
      },
    });
    ready = true;
    if (pending) emitSnapshot(pending);
  } catch (error) {
    await owner.release();
    throw error;
  }
}
