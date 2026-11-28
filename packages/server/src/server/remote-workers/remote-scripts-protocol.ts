import {
  ScriptStatusUpdateMessageSchema,
  WorkspaceScriptPayloadSchema,
} from "@getpaseo/protocol/messages";
import { z } from "zod";

const workspaceId = z.string().min(1);
const scriptName = z.string().min(1);

export const RemoteScriptOperationSchema = z.discriminatedUnion("kind", [
  z.strictObject({ kind: z.literal("scriptList"), workspaceId }),
  z.strictObject({ kind: z.literal("scriptStart"), workspaceId, scriptName }),
  z.strictObject({ kind: z.literal("scriptStop"), workspaceId, scriptName }),
]);

export const RemoteScriptEventSchema = z.strictObject({
  type: z.literal("scriptStatus"),
  payload: ScriptStatusUpdateMessageSchema.shape.payload.omit({ subscriptionId: true }),
});

export const remoteScriptResults = {
  scriptList: z.array(WorkspaceScriptPayloadSchema),
  scriptStart: WorkspaceScriptPayloadSchema,
  scriptStop: WorkspaceScriptPayloadSchema,
};

export type RemoteScriptOperation = z.infer<typeof RemoteScriptOperationSchema>;
export type RemoteScriptEvent = z.infer<typeof RemoteScriptEventSchema>;
