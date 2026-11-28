import { CLIENT_CAPS } from "@getpaseo/protocol/client-capabilities";
import type { SessionInboundMessage, SessionOutboundMessage, WorkspaceSetupSnapshot } from "@getpaseo/protocol/messages";
import { SessionDelivery } from "../session/owned-subscriptions/index.js";
import type { WorkspaceRegistry } from "../workspace-registry.js";
import type { WorkspaceSetupObservation } from "../workspace-setup-observation.js";
import { handleWorkspaceSetupStatusRequest } from "../worktree-session.js";

export function createRemoteWorkspaceSetupSession(options: {
  observation: WorkspaceSetupObservation;
  workspaceRegistry: Pick<WorkspaceRegistry, "get">;
  approveSetup(workspaceId: string): Promise<boolean>;
  socket: object;
  capabilities: Record<string, unknown> | undefined;
  send(message: SessionOutboundMessage): void;
}) {
  const { observation, workspaceRegistry, socket, capabilities, send } = options;
  const delivery = new SessionDelivery((_source, message) => send(message));
  delivery.attach(socket, capabilities?.[CLIENT_CAPS.ownedSubscriptions] === true);
  const dependencies = {
    workspaceSetupSnapshots: observation,
    getWorkspace: (id: string) => workspaceRegistry.get(id),
    emit: send,
  };
  return {
    dispatch(message: SessionInboundMessage) {
      if (message.type === "workspace.setup.run.request") {
        const { requestId, workspaceId } = message;
        return options.approveSetup(workspaceId).then(
          (started) => send({
            type: "workspace.setup.run.response",
            payload: { requestId, workspaceId, started, error: null },
          }),
          (cause: unknown) => send({
            type: "workspace.setup.run.response",
            payload: {
              requestId, workspaceId, started: false,
              error: cause instanceof Error ? cause.message : "Failed to run workspace setup",
            },
          }),
        );
      }
      if (message.type === "workspace_setup_status_request") {
        return handleWorkspaceSetupStatusRequest(dependencies, message);
      }
      if (message.type !== "workspace.setup.subscribe.request") return;
      return delivery.request(socket, message, async () => {
        let initial: WorkspaceSetupSnapshot | null = null;
        await handleWorkspaceSetupStatusRequest({
          ...dependencies,
          emit(outbound) {
            if (outbound.type === "workspace_setup_status_response") initial = outbound.payload.snapshot;
          },
        }, { ...message, type: "workspace_setup_status_request" });
        let stop: (() => void) | undefined;
        const owner = delivery.begin("workspace-setup", undefined, () => stop?.());
        stop = observation.subscribe((workspaceId, snapshot) => {
          if (workspaceId === message.workspaceId) {
            owner.emit({ type: "workspace_setup_progress", payload: { workspaceId, ...snapshot } });
          }
        });
        send({
          type: "workspace.setup.subscribe.response",
          payload: {
            requestId: message.requestId,
            subscriptionId: owner.responseId,
            workspaceId: message.workspaceId,
            snapshot: observation.get(message.workspaceId) ?? initial,
          },
        });
      });
    },
    release: (id: string) => delivery.release(id),
    close: () => delivery.detach(socket),
  };
}
