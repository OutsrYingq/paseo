import { randomUUID } from "node:crypto";
import type {
  ProviderConnection,
  ProviderEvent,
  ProviderRegistration,
} from "@getpaseo/plugin/server/provider";
import {
  BufferedPluginSubscription,
  type PluginRuntimeSubscription,
} from "../plugins/runtime-subscription.js";
import { PluginSessionSocket } from "../plugins/session-socket.js";
import type { RemotePluginScope } from "./remote-plugins.js";
import {
  decodeRemotePluginFrame,
  decodeRemotePluginPayload,
  encodeRemotePluginFrame,
  encodeRemotePluginPayload,
  RemotePluginEventSchema,
  RemotePluginResultSchema,
  RemotePluginValueSchemas,
  type RemotePluginAction,
  type RemotePluginEvent,
  type RemotePluginOperation,
  type RemotePluginRef,
  type RemotePluginValue,
} from "./remote-plugins-protocol.js";

export interface RemotePluginClientOptions {
  readonly scope: RemotePluginScope;
  readonly signal: AbortSignal;
  call(operation: RemotePluginOperation): Promise<unknown>;
  // The host must build an already admitted Session for this scope and enforce its worker routing.
  attachPluginSocket(
    scope: RemotePluginScope,
    plugin: RemotePluginRef,
    socket: PluginSessionSocket,
  ): Promise<{ closed: Promise<void> }>;
}

type Notification = Extract<
  RemotePluginEvent["event"],
  { type: "catalogChanged" | "settingsChanged" | "providersChanged" }
>;

/** One proxy per authorized execution connection. Keep it alongside RemoteProviderProcesses. */
export class RemotePluginClient {
  private readonly scope: RemotePluginScope;
  private readonly sessions = new Map<
    string,
    { socket: PluginSessionSocket; ready: Promise<void> }
  >();
  private readonly subscriptions = new Map<string, BufferedPluginSubscription>();
  private readonly providers = new Map<string, ProviderRegistration>();
  private readonly providerListeners = new Map<string, Set<(event: ProviderEvent) => void>>();
  private readonly listeners = new Set<(event: Notification) => void>();

  constructor(private readonly options: RemotePluginClientOptions) {
    options.signal.throwIfAborted();
    this.scope = { ...options.scope };
    options.signal.addEventListener("abort", () => this.dispose(), { once: true });
  }

  async request<A extends RemotePluginAction>(request: A): Promise<RemotePluginValue<A["action"]>> {
    this.options.signal.throwIfAborted();
    const raw = await this.options.call({ kind: "plugin", request });
    this.options.signal.throwIfAborted();
    const result = RemotePluginResultSchema.parse(raw);
    if (result.action !== request.action) throw new Error("worker_plugin_reply_mismatch");
    return RemotePluginValueSchemas[request.action].parse(result.value) as RemotePluginValue<
      A["action"]
    >;
  }

  subscribe(listener: (event: Notification) => void): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  async invokePluginRpc(plugin: RemotePluginRef, method: string, input: unknown): Promise<unknown> {
    const payload = await this.request({
      action: "invoke",
      plugin,
      method,
      input: encodeRemotePluginPayload(input),
    });
    return decodeRemotePluginPayload(payload);
  }

  subscribePluginRpc(
    plugin: RemotePluginRef,
    method: string,
    input: unknown,
  ): PluginRuntimeSubscription {
    const encodedInput = encodeRemotePluginPayload(input);
    const subscriptionId = randomUUID();
    const subscription = new BufferedPluginSubscription();
    this.subscriptions.set(subscriptionId, subscription);
    const ready = this.request({
      action: "subscribe",
      plugin,
      method,
      input: encodedInput,
      subscriptionId,
    }).then(
      () => undefined,
      (error: unknown) => {
        this.subscriptions.delete(subscriptionId);
        subscription.released();
        throw error;
      },
    );
    return {
      ready,
      attach: (sink) => subscription.attach(sink),
      release: () => {
        if (!this.subscriptions.delete(subscriptionId)) return;
        subscription.released();
        void this.request({ action: "unsubscribe", subscriptionId }).catch(() => undefined);
      },
    };
  }

  async getProviderRegistrations(): Promise<readonly ProviderRegistration[]> {
    // Do not cache a listing across reloads. Reuse wrappers by generation to preserve live clients.
    const entries = await this.request({ action: "providers" });
    const tokens = new Set(entries.map(({ token }) => token));
    for (const token of this.providers.keys()) if (!tokens.has(token)) this.providers.delete(token);
    return entries.map(({ token, hasStatus, hasCatalogCacheKey, ...metadata }) => {
      let registration = this.providers.get(token);
      if (!registration) {
        registration = {
          ...metadata,
          status: hasStatus ? () => this.request({ action: "providerStatus", token }) : undefined,
          getCatalogCacheKey: hasCatalogCacheKey
            ? async ({ launch: _launch, ...options }) =>
                (await this.request({ action: "providerCatalogKey", token, options })) ?? undefined
            : undefined,
          connect: ({ versions, capabilities }) =>
            this.connectProvider(token, [...versions], [...capabilities]),
        };
        this.providers.set(token, registration);
      }
      return registration;
    });
  }

  private async connectProvider(
    token: string,
    versions: number[],
    capabilities: string[],
  ): Promise<ProviderConnection> {
    const connectionId = randomUUID();
    const listeners = new Set<(event: ProviderEvent) => void>();
    this.providerListeners.set(connectionId, listeners);
    try {
      const negotiation = await this.request({
        action: "providerConnect",
        token,
        connectionId,
        request: { versions, capabilities },
      });
      return {
        ...negotiation,
        send: async (input) => {
          await this.request({ action: "providerSend", connectionId, input });
        },
        onEvent: (listener) => {
          listeners.add(listener);
          return () => listeners.delete(listener);
        },
        close: async () => {
          if (!this.providerListeners.delete(connectionId)) return;
          listeners.clear();
          await this.request({ action: "providerClose", connectionId });
        },
      };
    } catch (error) {
      this.providerListeners.delete(connectionId);
      throw error;
    }
  }

  onEvent(message: RemotePluginEvent): void {
    if (this.options.signal.aborted) return;
    const { event } = RemotePluginEventSchema.parse(message);
    switch (event.type) {
      case "catalogChanged":
      case "settingsChanged":
      case "providersChanged":
        for (const listener of this.listeners) listener(event);
        break;
      case "subscriptionEvent":
        this.subscriptions.get(event.subscriptionId)?.event(decodeRemotePluginPayload(event.value));
        break;
      case "subscriptionClosed": {
        const subscription = this.subscriptions.get(event.subscriptionId);
        this.subscriptions.delete(event.subscriptionId);
        subscription?.closed(event.error);
        break;
      }
      case "providerEvent":
        for (const listener of this.providerListeners.get(event.connectionId) ?? [])
          listener(event.value);
        break;
      case "sessionOpened":
        this.openSession(event.sessionId, event.plugin);
        break;
      case "sessionFrame": {
        const session = this.sessions.get(event.sessionId);
        void session?.ready.then(() =>
          session.socket.receive(...decodeRemotePluginFrame(event.frame)),
        );
        break;
      }
      case "sessionClosed":
        this.sessions.get(event.sessionId)?.socket.peerClosed();
        break;
    }
  }

  private openSession(sessionId: string, plugin: RemotePluginRef): void {
    if (this.sessions.has(sessionId)) throw new Error("duplicate_worker_plugin_session");
    const socket = new PluginSessionSocket({
      send: (message, callback) => {
        if (message.type === "paseo_frame") {
          void this.request({
            action: "sessionFrame",
            sessionId,
            frame: encodeRemotePluginFrame(message.data),
          }).then(
            () => callback?.(null),
            () => {
              callback?.(new Error("worker_plugin_session_send_failed"));
              socket.close();
            },
          );
          return true;
        }
        if (message.type === "paseo_close") {
          void this.request({ action: "sessionClose", sessionId }).then(
            () => callback?.(null),
            () => callback?.(new Error("worker_plugin_session_close_failed")),
          );
          return true;
        }
        return false;
      },
    });
    socket.once("close", () => this.sessions.delete(sessionId));
    const ready = Promise.resolve()
      .then(async () => {
        if (socket.readyState !== 1) return;
        const attachment = await this.options.attachPluginSocket(this.scope, plugin, socket);
        void attachment.closed.then(
          () => socket.close(),
          () => socket.close(),
        );
      })
      .catch(() => socket.close());
    this.sessions.set(sessionId, { socket, ready });
  }

  private dispose(): void {
    for (const { socket } of this.sessions.values()) socket.peerClosed();
    for (const subscription of this.subscriptions.values())
      subscription.closed("worker_unavailable");
    this.subscriptions.clear();
    for (const listeners of this.providerListeners.values()) listeners.clear();
    this.providerListeners.clear();
    this.providers.clear();
    this.listeners.clear();
  }
}
