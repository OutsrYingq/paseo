import type { SessionInboundMessage, SessionOutboundMessage } from "@getpaseo/protocol/messages";
import { getErrorMessageOr } from "@getpaseo/protocol/error-utils";
import { applyRecordPatch } from "../../record-patch.js";
import type { WorkspaceRegistry } from "../../workspace-registry.js";

type WorkspaceSettingsRequest = Extract<SessionInboundMessage, {
  type: "workspace.title.set.request" | "workspace.pin.set.request" | "workspace.metadata.update.request";
}>;

export async function changeWorkspaceSettings(
  registry: Pick<WorkspaceRegistry, "update">,
  request: WorkspaceSettingsRequest,
): Promise<Extract<SessionOutboundMessage, {
  type: "workspace.title.set.response" | "workspace.pin.set.response" | "workspace.metadata.update.response";
}>> {
  const { workspaceId, requestId } = request;
  const updatedAt = new Date().toISOString();
  let updated = null;
  let error: string | null = null;
  try {
    updated = await registry.update(workspaceId, (record) => {
      switch (request.type) {
        case "workspace.title.set.request":
          return { ...record, updatedAt, title: request.title?.trim() || null };
        case "workspace.pin.set.request":
          return { ...record, updatedAt, pinnedAt: request.pinned ? updatedAt : null };
        case "workspace.metadata.update.request":
          return { ...record, updatedAt, metadata: applyRecordPatch(record.metadata, request.patch) };
      }
    });
    if (!updated) error = "Workspace not found";
  } catch (cause) {
    error = getErrorMessageOr(cause, "Failed to update workspace settings");
  }
  const base = { workspaceId, requestId, accepted: updated !== null, error };
  switch (request.type) {
    case "workspace.title.set.request":
      return { type: "workspace.title.set.response", payload: { ...base, title: updated?.title ?? null } };
    case "workspace.pin.set.request":
      return { type: "workspace.pin.set.response", payload: { ...base, pinnedAt: updated?.pinnedAt ?? null } };
    case "workspace.metadata.update.request":
      return { type: "workspace.metadata.update.response", payload: { ...base, metadata: updated?.metadata ?? null } };
  }
}

export function isWorkspaceSettingsRequest(message: SessionInboundMessage): message is WorkspaceSettingsRequest {
  return message.type === "workspace.title.set.request" ||
    message.type === "workspace.pin.set.request" ||
    message.type === "workspace.metadata.update.request";
}
