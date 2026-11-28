import {
  buildAgentAttentionNotificationPayload,
  type AgentAttentionReason,
  type NotificationPermissionRequest,
} from "@getpaseo/protocol/agent-attention-notification";
import type {
  SessionEventSubscription,
  SessionInboundMessage,
  SessionOutboundMessage,
} from "@getpaseo/protocol/messages";
import type { TerminalActivity } from "@getpaseo/protocol/terminal-activity";
import type { Logger } from "pino";
import {
  computeNotificationPlan,
  isPushEligibleAttentionReason,
  type AttentionFocusTarget,
  type ClientPresenceState,
  type NotificationPlan,
} from "../agent-attention-policy.js";
import type { SessionAuthorization } from "../authorization/index.js";
import type { RemotePushOperation } from "./remote-push.js";

type PushSend = Extract<RemotePushOperation, { kind: "pushNotificationSend" }>;
type NotificationEvent = Extract<
  SessionOutboundMessage,
  { type: "agent_attention_required" | "terminal_attention_required" | "plugin.notification" }
>;
type NotificationEventType = Extract<SessionEventSubscription, NotificationEvent["type"]>;
type TerminalAttention = Extract<PushSend["notification"], { kind: "terminal" }>["reason"];

type PluginNotification = Omit<
  Extract<NotificationEvent, { type: "plugin.notification" }>["payload"],
  "serverId" | "shouldNotify" | "subscriptionId"
>;

interface NotificationSource {
  authorization: Pick<SessionAuthorization, "allowsInbound" | "allowsOutbound">;
  wantsEvent: (event: NotificationEventType) => boolean;
  wantsNotification: (event: NotificationEventType) => boolean;
  emit: (
    message:
      | NotificationEvent
      | Extract<SessionOutboundMessage, { type: "push.unregister.response" }>,
  ) => void;
  clearTerminalAttention?: (terminalId: string) => Promise<unknown>;
}

interface AgentAttention {
  agentId: string;
  workspaceId: string;
  reason: AgentAttentionReason;
  assistantMessage?: string | null;
  permissionRequest?: NotificationPermissionRequest | null;
  attentionTimestamp?: Date | null;
}

interface TerminalAttentionInput {
  terminalId: string;
  cwd: string;
  workspaceId?: string;
  terminalName: string;
  reason: TerminalAttention;
}

export function resolveCentralTerminalAttention(event: {
  activity: TerminalActivity | null;
  previous: TerminalActivity | null;
}): TerminalAttention | null {
  if (event.activity?.attentionReason) return event.activity.attentionReason;
  if (event.activity?.state === "attention") return "needs_input";
  if (event.previous?.state === "working" && event.activity?.state === "idle") return "finished";
  return null;
}

export function createCentralNotifications({
  serverId,
  logger,
  call,
  now = Date.now,
}: {
  serverId: string;
  logger: Logger;
  call: (operation: RemotePushOperation) => Promise<unknown>;
  now?: () => number;
}) {
  // The worker outlives its subscribed sockets. Its own resident retains the token store.
  const sources = new Map<NotificationSource, ClientPresenceState>();
  const notifiedUnreadFinishAt = new Map<string, number>();

  function notify(
    message: NotificationEvent,
    push: Pick<PushSend, "payload" | "notification">,
    focusTarget: AttentionFocusTarget | null,
    planForEvent: (plan: NotificationPlan) => NotificationPlan = (plan) => plan,
  ): void {
    const recipients = [...sources].filter(
      ([source]) => source.wantsEvent(message.type) && source.authorization.allowsOutbound(message),
    );
    const notificationEntries = recipients.filter(([source]) =>
      source.wantsNotification(message.type),
    );
    const plan = planForEvent(
      computeNotificationPlan({
        allStates: notificationEntries.map(([, state]) => state),
        focusTarget,
        pushEligible:
          push.notification.kind !== "agent" ||
          isPushEligibleAttentionReason(push.notification.reason),
        nowMs: now(),
      }),
    );
    if (plan.shouldPush) {
      void call({ kind: "pushNotificationSend", ...push }).catch((err) => {
        logger.warn({ err, event: message.type }, "Worker push notification failed");
      });
    }
    const inApp =
      plan.inAppRecipientIndex === null
        ? undefined
        : notificationEntries[plan.inAppRecipientIndex]?.[0];
    for (const [source] of recipients) {
      const shouldNotify = source === inApp;
      switch (message.type) {
        case "agent_attention_required":
          source.emit({ ...message, payload: { ...message.payload, shouldNotify } });
          break;
        case "terminal_attention_required":
          source.emit({ ...message, payload: { ...message.payload, shouldNotify } });
          break;
        case "plugin.notification":
          source.emit({ ...message, payload: { ...message.payload, shouldNotify } });
          break;
      }
    }
  }

  function attach(source: NotificationSource) {
    let pushToken: string | undefined;
    sources.set(source, {
      appVisible: false,
      focusedAgentId: null,
      focusedTerminalId: null,
      lastActivityAtMs: null,
    });

    function handle(request: SessionInboundMessage): Promise<void> | undefined {
      switch (request.type) {
        case "register_push_token": {
          if (!sources.has(source) || !source.authorization.allowsInbound(request)) return;
          pushToken = request.token;
          return call({
            kind: "pushTokenRenew",
            token: request.token,
            preferences: request.preferences,
          }).then(() => undefined);
        }
        case "push.unregister.request": {
          if (!sources.has(source) || !source.authorization.allowsInbound(request)) return;
          if (pushToken?.trim() === request.token.trim()) pushToken = undefined;
          return call({ kind: "pushTokenRevoke", token: request.token }).then(() => {
            const response = {
              type: "push.unregister.response",
              payload: { requestId: request.requestId },
            } as const;
            if (sources.has(source) && source.authorization.allowsOutbound(response))
              source.emit(response);
          });
        }
        case "client_heartbeat": {
          if (!sources.has(source) || !source.authorization.allowsInbound(request)) return;
          const focusedTerminalId = request.focusedTerminalId?.trim() || null;
          const lastActivityAtMs = Date.parse(request.lastActivityAt);
          sources.set(source, {
            appVisible: request.appVisible,
            focusedAgentId: request.focusedAgentId,
            focusedTerminalId,
            lastActivityAtMs: Number.isFinite(lastActivityAtMs) ? lastActivityAtMs : null,
          });
          if (request.appVisible && focusedTerminalId && source.clearTerminalAttention) {
            void source.clearTerminalAttention(focusedTerminalId).catch((err) => {
              logger.warn(
                { err, terminalId: focusedTerminalId },
                "Failed to clear terminal attention",
              );
            });
          }
          return pushToken === undefined
            ? Promise.resolve()
            : call({ kind: "pushTokenRenew", token: pushToken }).then(() => undefined);
        }
        default:
          return;
      }
    }

    return {
      handle,
      close: () => {
        sources.delete(source);
      },
    };
  }

  function agent(input: AgentAttention): void {
    const { agentId, workspaceId, reason, attentionTimestamp } = input;
    const { title, body } = buildAgentAttentionNotificationPayload({ ...input, serverId });
    const notification = { title, body, data: { serverId, workspaceId, agentId, reason } };
    notify(
      {
        type: "agent_attention_required",
        payload: {
          agentId,
          reason,
          timestamp: new Date(now()).toISOString(),
          notification,
          shouldNotify: false,
        },
      },
      { payload: notification, notification: { kind: "agent", reason } },
      { kind: "agent", id: agentId },
      (plan) => {
        if (reason !== "finished" || !attentionTimestamp) return plan;
        const unreadFinishAt = attentionTimestamp.getTime();
        if (notifiedUnreadFinishAt.get(agentId) === unreadFinishAt) {
          return { inAppRecipientIndex: null, shouldPush: false };
        }
        if (plan.shouldPush || plan.inAppRecipientIndex !== null) {
          notifiedUnreadFinishAt.set(agentId, unreadFinishAt);
        }
        return plan;
      },
    );
  }

  function terminal({
    terminalId,
    cwd,
    workspaceId,
    terminalName,
    reason,
  }: TerminalAttentionInput): void {
    const title = reason === "needs_input" ? "Terminal needs input" : "Terminal finished";
    const body = terminalName;
    const data = { serverId, terminalId, cwd, ...(workspaceId ? { workspaceId } : {}) };
    notify(
      {
        type: "terminal_attention_required",
        payload: { ...data, reason, title, body, shouldNotify: false },
      },
      { payload: { title, body, data }, notification: { kind: "terminal", reason } },
      { kind: "terminal", id: terminalId },
    );
  }

  function plugin({ pluginId, notificationId, title, body, open, data }: PluginNotification): void {
    const identity = {
      serverId,
      pluginId,
      notificationId,
      ...(open ? { open } : {}),
      ...(data ? { data } : {}),
    };
    notify(
      { type: "plugin.notification", payload: { ...identity, title, body, shouldNotify: false } },
      {
        payload: { title, body, data: { kind: "plugin", ...identity } },
        notification: { kind: "plugin", pluginId },
      },
      open?.type === "agent" ? { kind: "agent", id: open.agentId } : null,
    );
  }

  return { attach, agent, terminal, plugin };
}
