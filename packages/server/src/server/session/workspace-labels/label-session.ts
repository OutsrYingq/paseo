import type { SessionInboundMessage, SessionOutboundMessage } from "../../messages.js";
import {
  WorkspaceLabelError,
  WorkspaceLabelStorageUncertainError,
  type WorkspaceLabelService,
} from "../../workspace-labels/index.js";
import type { SessionDelivery } from "../owned-subscriptions/index.js";

const workspaceLabelRequests = [
  "workspace.label.list.request",
  "workspace.label.assignment.set.request",
  "workspace.label.update.request",
  "workspace.label.delete.request",
  "workspace.label.delete.inspect.request",
] as const;

type WorkspaceLabelRequest = Extract<
  SessionInboundMessage,
  { type: (typeof workspaceLabelRequests)[number] }
>;
const requestTypes: ReadonlySet<string> = new Set(workspaceLabelRequests);

export function isWorkspaceLabelRequest(message: SessionInboundMessage): message is WorkspaceLabelRequest {
  return requestTypes.has(message.type);
}

export async function handleWorkspaceLabelRequest(
  service: WorkspaceLabelService | undefined,
  delivery: Pick<SessionDelivery, "begin">,
  emit: (message: SessionOutboundMessage) => void,
  request: WorkspaceLabelRequest,
): Promise<void> {
  const { requestId } = request;
  if (!service) {
    emit({
      type: "rpc_error",
      payload: {
        requestId,
        requestType: request.type,
        code: "workspace_labels_unavailable",
        error: "Workspace labels unavailable",
      },
    });
    return;
  }
  try {
    switch (request.type) {
      case "workspace.label.list.request":
        if (request.subscribe) {
          await subscribeToLabels(service, delivery, emit, request, request.subscribe);
        } else {
          emit({
            type: "workspace.label.list.response",
            payload: { requestId, ...(await service.list(request.sync)) },
          });
        }
        return;
      case "workspace.label.assignment.set.request":
        emit({
          type: "workspace.label.assignment.set.response",
          payload: { requestId, ...(await service.setAssignment(request)) },
        });
        return;
      case "workspace.label.update.request":
        emit({
          type: "workspace.label.update.response",
          payload: { requestId, ...(await service.update(request)) },
        });
        return;
      case "workspace.label.delete.request":
        emit({
          type: "workspace.label.delete.response",
          payload: { requestId, ...(await service.delete(request.name)) },
        });
        return;
      case "workspace.label.delete.inspect.request":
        emit({
          type: "workspace.label.delete.inspect.response",
          payload: { requestId, affectedWorkspaceCount: await service.countAffectedWorkspaces(request.name) },
        });
        return;
    }
  } catch (error) {
    emit({
      type: "rpc_error",
      payload: {
        requestId,
        requestType: request.type,
        code:
          error instanceof WorkspaceLabelError || error instanceof WorkspaceLabelStorageUncertainError
            ? error.code
            : "workspace_label_failed",
        error: error instanceof Error ? error.message : "Workspace label operation failed",
      },
    });
  }
}

type ListRequest = Extract<WorkspaceLabelRequest, { type: "workspace.label.list.request" }>;

async function subscribeToLabels(
  service: WorkspaceLabelService,
  delivery: Pick<SessionDelivery, "begin">,
  emit: (message: SessionOutboundMessage) => void,
  request: ListRequest,
  subscribe: NonNullable<ListRequest["subscribe"]>,
): Promise<void> {
  let bootstrap: ReturnType<typeof service.subscribe> | undefined;
  const owner = delivery.begin("labels", subscribe.subscriptionId, async () => {
    await bootstrap?.then((subscription) => subscription.unsubscribe(), () => undefined);
  });
  try {
    type Change = Parameters<Parameters<typeof service.subscribe>[0]["onChange"]>[0];
    const pending = new Map<string, Change>();
    let ready = false;
    const emitChange = (change: Change) => owner.emit({ type: "workspace.label.update", payload: change });
    bootstrap = service.subscribe({
      cursor: request.sync,
      onChange(change) {
        if (owner.signal.aborted) return;
        if (ready) emitChange(change);
        else pending.set(change.kind === "upsert" ? change.label.name : change.name, change);
      },
    });
    const { snapshot } = await bootstrap;
    if (owner.signal.aborted) return;
    emit({
      type: "workspace.label.list.response",
      payload: { requestId: request.requestId, subscriptionId: owner.responseId, ...snapshot },
    });
    ready = true;
    for (const change of pending.values()) {
      if (change.seq > snapshot.sync.headSeq) emitChange(change);
    }
    pending.clear();
  } catch (error) {
    await owner.release();
    throw error;
  }
}
