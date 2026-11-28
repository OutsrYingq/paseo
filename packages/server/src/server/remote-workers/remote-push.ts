import { join } from "node:path";
import type { Logger } from "pino";
import { NotificationPreferencesSchema } from "@getpaseo/protocol/notification-preferences";
import { TERMINAL_ACTIVITY_ATTENTION_REASONS } from "@getpaseo/protocol/terminal-activity";
import { z } from "zod";
import { createPushNotifications, type PushPayload } from "../push/index.js";

const PushPayloadSchema = z.strictObject({
  title: z.string(),
  body: z.string(),
  data: z.record(z.string(), z.json()).optional(),
});

const NotificationKindSchema = z.discriminatedUnion("kind", [
  z.strictObject({ kind: z.literal("agent"), reason: z.enum(["permission", "error", "finished"]) }),
  z.strictObject({
    kind: z.literal("terminal"),
    reason: z.enum(TERMINAL_ACTIVITY_ATTENTION_REASONS),
  }),
  z.strictObject({ kind: z.literal("plugin"), pluginId: z.string().min(1) }),
]);

export const RemotePushOperationSchema = z.discriminatedUnion("kind", [
  z.strictObject({
    kind: z.literal("pushTokenRenew"),
    token: z.string(),
    preferences: NotificationPreferencesSchema.optional(),
  }),
  z.strictObject({ kind: z.literal("pushTokenRevoke"), token: z.string() }),
  z.strictObject({
    kind: z.literal("pushNotificationSend"),
    payload: PushPayloadSchema,
    notification: NotificationKindSchema,
  }),
]);

export type RemotePushOperation = z.infer<typeof RemotePushOperationSchema>;

export const remotePushResults = {
  pushTokenRenew: z.null(),
  pushTokenRevoke: z.null(),
  pushNotificationSend: z.null(),
};

export function createResidentPush(options: {
  paseoHome: string;
  logger: Logger;
  deliver?: (tokens: string[], payload: PushPayload) => Promise<void>;
}) {
  const push = createPushNotifications({
    logger: options.logger,
    filePath: join(options.paseoHome, "push-tokens.json"),
    deliver: options.deliver,
  });

  return async function handlePushOperation(operation: RemotePushOperation): Promise<null> {
    switch (operation.kind) {
      case "pushTokenRenew":
        push.renew(operation.token, operation.preferences);
        break;
      case "pushTokenRevoke":
        push.revoke(operation.token);
        break;
      case "pushNotificationSend":
        await push.send(operation.payload, operation.notification);
        break;
    }
    return null;
  };
}
