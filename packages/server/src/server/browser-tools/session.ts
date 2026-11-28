import { randomUUID } from "node:crypto";
import { BrowserAutomationHostCapabilitySchema } from "@getpaseo/protocol/browser-automation/capabilities";
import type { SessionInboundMessage } from "@getpaseo/protocol/messages";
import type { SessionDelivery } from "../session/owned-subscriptions/index.js";
import type { BrowserToolsBroker } from "./broker.js";

type RegisterBrowserHostRequest = Extract<SessionInboundMessage, { type: "browser.host.register.request" }>;
const FAMILY = "browser-host";

/** Only call after the session has authorized browser hosting on this source. */
export class BrowserToolsSession {
  constructor(
    private readonly broker: BrowserToolsBroker | null | undefined,
    private readonly delivery: SessionDelivery,
  ) {}
  dispatch(source: object | undefined, message: SessionInboundMessage): Promise<void> | undefined {
    if (message.type === "browser.host.register.request") {
      return this.delivery.request(source, message, () => this.register(message, true));
    }
    if (message.type === "browser.automation.execute.response") {
      if (source) {
        this.broker?.receiveResponse(message, this.delivery.subscriptionIds(source, FAMILY));
      }
      return Promise.resolve();
    }
    return undefined;
  }

  revoke(): Promise<void> {
    return this.delivery.releaseFamily(FAMILY);
  }

  registerLegacy(source: object, capability: unknown): Promise<void> | undefined {
    if (!this.broker || this.delivery.isModern(source)) return undefined;
    const host = BrowserAutomationHostCapabilitySchema.safeParse(capability);
    if (!host.success) return undefined;
    const request = {
      type: "browser.host.register.request" as const,
      requestId: randomUUID(),
      hostKind: host.data.hostKind,
      label: host.data.label,
      instanceId: host.data.instanceId,
      supportedCommands: host.data.supportedCommands,
    };
    return this.delivery.request(source, request, () => this.register(request, false));
  }

  private async register(request: RegisterBrowserHostRequest, respond: boolean): Promise<void> {
    if (!this.broker) throw new Error("Browser hosting is unavailable");
    let unregister: (() => void) | undefined;
    const owner = this.delivery.begin(FAMILY, undefined, () => unregister?.());
    try {
      unregister = this.broker.registerClient({
        id: owner.id,
        hostKind: request.hostKind,
        label: request.label,
        instanceId: request.instanceId,
        supportedCommands: request.supportedCommands,
        sendBrowserAutomationRequest: (message) => owner.emit(message),
      });
      if (respond) {
        this.delivery.reply({
          type: "browser.host.register.response",
          payload: { requestId: request.requestId, subscriptionId: owner.responseId },
        });
      }
    } catch (error) {
      await owner.release();
      throw error;
    }
  }
}
