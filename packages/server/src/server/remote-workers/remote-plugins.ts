import { randomUUID } from "node:crypto";
import { realpath } from "node:fs/promises";
import { isDeepStrictEqual } from "node:util";
import path from "node:path";
import type pino from "pino";
import { MutableDaemonConfigSchema, type PluginListItem } from "@getpaseo/protocol/messages";
import { formatPluginSourceReference } from "@getpaseo/protocol/plugin-source-reference";
import type {
  ProviderConnection,
  ProviderEvent,
  ProviderLaunch,
  ProviderInput,
  ProviderPersistence,
  ProviderRegistration,
} from "@getpaseo/plugin/server/provider";
import {
  resolveProviderLaunch,
  checkProviderLaunchAvailable,
  createProviderEnv,
  type AgentProviderRuntimeSettingsMap,
} from "../agent/provider-launch-config.js";
import type { AgentUsageSession } from "../agent/agent-sdk-types.js";
import { DaemonConfigStore } from "../daemon-config-store.js";
import { loadPersistedConfig } from "../persisted-config.js";
import { PluginService } from "../plugins/index.js";
import type { ValidatedPluginNotification } from "../plugins/notifications.js";
import { ManagedPluginSources } from "../plugins/managed-source.js";
import { BuiltinPluginLoader, builtinPlugins } from "../plugins/builtin/index.js";
import type { AgentUsageLookup } from "../plugins/usage-sources/index.js";
import { PluginRuntime } from "../plugins/runtime.js";
import type { PluginRuntimeSubscription } from "../plugins/runtime-subscription.js";
import type { PluginSessionSocket } from "../plugins/session-socket.js";
import {
  decodeRemotePluginFrame,
  decodeRemotePluginPayload,
  encodeRemotePluginFrame,
  encodeRemotePluginPayload,
  RemotePluginEventSchema,
  RemotePluginOperationSchema,
  RemotePluginResultSchema,
  type RemotePluginAction,
  type RemotePluginEvent,
  type RemotePluginOperation,
  type RemotePluginRef,
  type RemotePluginResult,
} from "./remote-plugins-protocol.js";

export * from "./remote-plugins-protocol.js";
export { RemotePluginClient } from "./remote-plugins-client.js";

export interface RemotePluginScope {
  readonly workerId: string;
  readonly podUid: string;
  readonly serverId?: string;
}

interface ResidentPluginHostOptions {
  scope: RemotePluginScope;
  paseoHome: string;
  daemonVersion: string;
  logger: pino.Logger;
  providerSettings?: AgentProviderRuntimeSettingsMap;
  configStore?: DaemonConfigStore;
  builtinPlugins?: BuiltinPluginLoader;
  nativeUsage?: Pick<AgentUsageLookup, "usageSession" | "resolveLaunch" | "isProviderActive">;
}

interface ResidentPluginChannelOptions {
  scope: RemotePluginScope;
  signal: AbortSignal;
  publish(event: RemotePluginEvent): void;
}

export type ResidentPluginOptions = ResidentPluginHostOptions & ResidentPluginChannelOptions;

export class RemotePluginOperationError extends Error {}

export function createResidentPlugins(options: ResidentPluginOptions) {
  const host = createResidentPluginHost(options);
  const channel = host.openChannel(options);
  return { execute: channel.execute, close: host.close };
}

export function createResidentPluginHost(options: ResidentPluginHostOptions) {
  if (!path.isAbsolute(options.paseoHome)) throw new Error("invalid_worker_plugin_home");
  const scope = { ...options.scope };
  const logger = options.logger.child({ workerId: scope.workerId, podUid: scope.podUid });
  const persisted = loadPersistedConfig(options.paseoHome, logger);
  const config = options.configStore ?? new DaemonConfigStore(
    options.paseoHome,
    MutableDaemonConfigSchema.parse({
      mcp: { injectIntoAgents: false },
      pluginsEnabled: persisted.pluginsEnabled,
      usage: { enabled: persisted.daemon?.usage?.enabled !== false },
      plugins: persisted.plugins,
    }),
    logger,
    { startupPersisted: persisted },
  );
  const runtime = new PluginRuntime(logger, options.daemonVersion, {
    settingsDirectory: path.join(options.paseoHome, "plugin-settings"),
    onSettingsChanged: (id, settingsId) => channel?.notifySettings(id, settingsId),
  });
  const workers = new Set<ResidentProviderConnection>();
  function usageSession(id: string): AgentUsageSession | null {
    for (const worker of workers) {
      const session = worker.usageSession(id);
      if (session) return session;
    }
    return options.nativeUsage?.usageSession(id) ?? null;
  }
  const service = new PluginService(logger, config, options.daemonVersion, {
    runtime,
    builtinPlugins:
      options.builtinPlugins ??
      new BuiltinPluginLoader(undefined, builtinPlugins.filter((id) => id.endsWith("-usage-source"))),
    usageAgents: {
      hasAgent: (id) => usageSession(id) !== null,
      usageSession,
      async resolveLaunch(id) {
        const registration = service.getProviderRegistrations().find((entry) => entry.id === id);
        return registration ? providerLaunch(registration) : options.nativeUsage?.resolveLaunch?.(id);
      },
      async isProviderActive(id, refresh) {
        if (service.getProviderRegistrations().some((entry) => entry.id === id)) return true;
        return await options.nativeUsage?.isProviderActive?.(id, refresh) ?? false;
      },
    },
    managedSources: new ManagedPluginSources(options.paseoHome, {
      registries: persisted.pluginRegistries,
      defaultUrl: process.env.PASEO_PLUGIN_REGISTRY,
    }),
  });
  const reloading = new Set<string>();
  const sessions = new Map<
    string,
    {
      pluginId: string;
      socket: PluginSessionSocket;
      hello?: string;
    }
  >();
  let channel:
    | {
        attach(id: string, pluginId: string): void;
        forward(id: string, data: string | Uint8Array): void;
        notifySettings(id: string, settingsId: string): void;
        notifyPlugin(notification: ValidatedPluginNotification): void;
        close(): Promise<void>;
      }
    | undefined;
  let started: Promise<void> | undefined;
  let closing: Promise<void> | undefined;
  async function providerLaunch(registration: ProviderRegistration) {
    const command = registration.command;
    if (!command) return undefined;
    const settings = options.providerSettings?.[registration.id];
    const launch = await resolveProviderLaunch({
      commandConfig: settings?.command,
      defaultBinary: command[0],
    });
    if (launch.source !== "override") launch.args.unshift(...command.slice(1));
    launch.env = createProviderEnv({ runtimeSettings: settings });
    const available = await checkProviderLaunchAvailable(launch);
    if (!available.resolvedPath) throw new Error("worker_plugin_provider_command_unavailable");
    const env = Object.fromEntries(
      Object.entries(launch.env).filter(
        (entry): entry is [string, string] => entry[1] !== undefined,
      ),
    );
    return { command: available.resolvedPath, args: launch.args, env };
  }

  service.bindPaseoSessionHost({
    sendPluginNotification(notification) {
      channel?.notifyPlugin(notification);
    },
    async attachPluginSocket(pluginId, socket) {
      if (closing) throw new Error("worker_plugins_closed");
      const id = randomUUID();
      const session = { pluginId, socket, hello: undefined as string | undefined };
      sessions.set(id, session);
      channel?.attach(id, pluginId);
      const closed = new Promise<void>((resolve) =>
        socket.once("close", () => {
          sessions.delete(id);
          resolve();
        }),
      );
      socket.on("message", (data) => {
        if (typeof data === "string" || data instanceof Uint8Array) {
          const owner = channel;
          if (owner) {
            owner.forward(id, data);
          } else if (typeof data === "string" && isHelloFrame(data)) {
            session.hello = data;
          }
        }
      });
      return { closed };
    },
  });

  function openChannel(link: ResidentPluginChannelOptions) {
    link.signal.throwIfAborted();
    if (closing || channel) throw new Error("worker_plugin_channel_unavailable");
    if (
      scope.workerId !== link.scope.workerId ||
      scope.podUid !== link.scope.podUid ||
      scope.serverId !== link.scope.serverId
    ) {
      throw new Error("worker_plugin_binding_mismatch");
    }
    const owner = Symbol("resident plugin channel");
    const refs = new Map<string, string>();
    const providerTokens = new WeakMap<ProviderRegistration, string>();
    const subscriptions = new Map<string, PluginRuntimeSubscription>();
    const connections = new Map<string, ResidentProviderConnection>();
    const attached = new Set<string>();
    const pending = new Set<(error: Error) => void>();
    let activated = false;
    let detach: Promise<void> | undefined;

    function publish(event: RemotePluginEvent["event"]) {
      if (channel === current && !link.signal.aborted) {
        link.publish(RemotePluginEventSchema.parse({ type: "plugin", event }));
      }
    }

    function pluginRef(id: string): RemotePluginRef {
      let token = refs.get(id);
      if (!token) refs.set(id, (token = randomUUID()));
      return { id, token };
    }

    function requirePlugin(ref: RemotePluginRef): string {
      if (refs.get(ref.id) !== ref.token) throw new Error("unknown_worker_plugin");
      return ref.id;
    }

    function listItem(item: PluginListItem) {
      return { plugin: pluginRef(item.id), item };
    }

    function providerToken(registration: ProviderRegistration): string {
      let token = providerTokens.get(registration);
      if (!token) providerTokens.set(registration, (token = randomUUID()));
      return token;
    }

    function requireProvider(token: string): ProviderRegistration {
      const registration = service
        .getProviderRegistrations()
        .find((item) => providerTokens.get(item) === token);
      if (!registration) throw new Error("unknown_worker_plugin_provider");
      return registration;
    }

    const unsubscribe = [
      service.subscribe((id) => publish({ type: "catalogChanged", plugin: pluginRef(id) })),
      service.subscribeProviderRegistrations(() => publish({ type: "providersChanged" })),
    ];

    function requireActive() {
      link.signal.throwIfAborted();
      if (channel !== current || closing) throw new Error("worker_plugins_closed");
    }
    async function execute(operation: RemotePluginOperation): Promise<RemotePluginResult> {
      requireActive();
      return new Promise((resolve, reject) => {
        pending.add(reject);
        void executeRequest(operation)
          .then(resolve, reject)
          .finally(() => pending.delete(reject));
      });
    }

    async function executeRequest(operation: RemotePluginOperation): Promise<RemotePluginResult> {
      requireActive();
      const { request } = RemotePluginOperationSchema.parse(operation);
      activated = true;
      announceSessions();
      // The session frames can arrive while PluginService.start is still launching a child.
      if (request.action !== "sessionFrame" && request.action !== "sessionClose") {
        started ??= service.start();
        await started;
        requireActive();
      }
      let sourceRequest: Promise<unknown> | undefined;
      let sourceValue: unknown;
      try {
        sourceRequest = dispatchSource(request);
        if (sourceRequest) sourceValue = await sourceRequest;
      } catch (error) {
        return RemotePluginResultSchema.parse({
          action: request.action,
          error: error instanceof Error ? error.message : String(error),
        });
      }
      const value = sourceRequest ? sourceValue : await dispatch(request);
      return RemotePluginResultSchema.parse({ action: request.action, value });
    }

    function dispatchSource(request: RemotePluginAction): Promise<unknown> | undefined {
      switch (request.action) {
        case "usageReports":
          if (config.get().usage?.enabled === false) throw new Error("Usage is off on this host.");
          return service.listUsageReports({
            agentId: request.sessionId,
            forceRefresh: request.forceRefresh,
            reportIds: request.reportIds,
            onReport: (report) => publish({ type: "usageReport", queryId: request.queryId, report }),
          });
        case "usageLegacy":
          if (config.get().usage?.enabled === false) throw new Error("Usage is off on this host.");
          return service.listLegacyUsage();
        case "inspectDirectory":
          return service.inspectDirectory(request.path).then(({ id }) => ({ id }));
        case "installSource":
          return service
            .installSource({
              // COMPAT(plugin-source-path): accepted for v0.7 clients; remove after 2027-09-01.
              source: formatPluginSourceReference(request.source, request.pluginPath),
              id: request.id,
              ref: request.ref,
            })
            .then(listItem);
        case "statusSources":
          return service.statusSources(request.pluginId);
        case "updateSources":
          return service.updateSources(request.pluginId);
        case "previewUpdates":
          return service.previewUpdates(request);
        case "applyUpdates": {
          const previousWorkers = [...workers];
          return service.applyUpdates(request.proposals).then((results) => {
            for (const { id, outcome } of results) {
              if (outcome !== "updated") continue;
              for (const entry of previousWorkers) if (entry.pluginId === id) entry.retire();
            }
            return results;
          });
        }
        case "listRegistry":
          return service.listRegistry();
        case "getRegistryPlugin":
          return service.getRegistryPlugin(request.id);
        default:
          return undefined;
      }
    }

    async function dispatch(request: RemotePluginAction): Promise<unknown> {
      switch (request.action) {
        case "list":
          return (await service.listPlugins()).map(listItem);
        case "installDirectory":
          return listItem(await service.installDirectory(request));
        case "logs":
          return service.getLogs(requirePlugin(request.plugin));
        case "catalog": {
          const known = new Map(
            request.known?.map(({ id, clientBundleHash }) => [id, clientBundleHash]),
          );
          return service.catalog().map(({ id, clientBundle, ...item }) => ({
            ...item,
            plugin: pluginRef(id),
            ...(known.get(id) === item.clientBundleHash ? {} : { clientBundle }),
          }));
        }
        case "enable":
          return listItem(await service.enablePlugin(requirePlugin(request.plugin)));
        case "disable":
        case "remove":
        case "reload": {
          const pluginId = requirePlugin(request.plugin);
          if (reloading.has(pluginId))
            throw new RemotePluginOperationError("worker_plugin_reloading");
          reloading.add(pluginId);
          try {
            const item =
              request.action === "disable"
                ? await service.disablePlugin(pluginId)
                : request.action === "remove"
                  ? await service.removePlugin(pluginId)
                  : await service.reloadPlugin(pluginId);
            for (const entry of workers) if (entry.pluginId === pluginId) entry.retire();
            return item ? listItem(item) : null;
          } finally {
            reloading.delete(pluginId);
          }
        }
        case "invoke":
          return encodeRemotePluginPayload(
            await service.invokePluginRpc(
              requirePlugin(request.plugin),
              request.method,
              decodeRemotePluginPayload(request.input),
            ),
          );
        case "subscribe": {
          const { subscriptionId } = request;
          if (subscriptions.has(subscriptionId))
            throw new Error("duplicate_worker_plugin_subscription");
          const subscription = service.subscribePluginRpc(
            requirePlugin(request.plugin),
            request.method,
            decodeRemotePluginPayload(request.input),
          );
          subscriptions.set(subscriptionId, subscription);
          try {
            await subscription.ready;
            if (subscriptions.get(subscriptionId) === subscription) {
              subscription.attach({
                event(value) {
                  publish({
                    type: "subscriptionEvent",
                    subscriptionId,
                    value: encodeRemotePluginPayload(value),
                  });
                },
                closed(error) {
                  subscriptions.delete(subscriptionId);
                  publish({ type: "subscriptionClosed", subscriptionId, error });
                },
              });
            }
          } catch (error) {
            subscriptions.delete(subscriptionId);
            subscription.release();
            throw error;
          }
          return null;
        }
        case "unsubscribe": {
          subscriptions.get(request.subscriptionId)?.release();
          subscriptions.delete(request.subscriptionId);
          return null;
        }
        case "providers":
          return service.getProviderRegistrations().map((provider) => ({
            id: provider.id,
            token: providerToken(provider),
            label: provider.label,
            description: provider.description,
            docsUrl: provider.docsUrl,
            icon: provider.icon,
            requiresConfiguration: provider.requiresConfiguration,
            hasStatus: Boolean(provider.status || provider.command),
            hasCatalogCacheKey: Boolean(provider.getCatalogCacheKey),
          }));
        case "providerStatus": {
          const provider = requireProvider(request.token);
          let launch;
          try {
            launch = await providerLaunch(provider);
          } catch {
            return { available: false, diagnostic: "Provider command unavailable on worker" };
          }
          return provider.status ? provider.status({ launch }) : { available: true };
        }
        case "providerCatalogKey": {
          const provider = requireProvider(request.token);
          return (
            (await provider.getCatalogCacheKey?.({
              ...request.options,
              launch: await providerLaunch(provider),
            })) ?? null
          );
        }
        case "providerConnect": {
          const { connectionId } = request;
          if (connections.has(connectionId))
            throw new Error("duplicate_worker_provider_connection");
          const provider = requireProvider(request.token);
          const pluginId = service
            .catalog()
            .find(({ id }) =>
              runtime.getProviderRegistrations(id).some(({ id }) => id === provider.id),
            )?.id;
          if (!pluginId || reloading.has(pluginId))
            throw new RemotePluginOperationError("worker_plugin_reloading");
          let usageLaunch: ProviderLaunch | undefined;
          const connection = (async () => {
            const launch = usageLaunch = await providerLaunch(provider);
            requireActive();
            if (reloading.has(pluginId) || requireProvider(request.token) !== provider)
              throw new Error("unknown_worker_plugin_provider");
            return provider.connect({ ...request.request, launch });
          })();
          const entry = new ResidentProviderConnection({
            pluginId,
            providerId: provider.id,
            owner,
            connection,
            usageLaunch: () => usageLaunch,
            workers,
            publish: (value) => publish({ type: "providerEvent", connectionId, value }),
            onRetire: () => {
              if (connections.get(connectionId) === entry) connections.delete(connectionId);
            },
            onError: (error) =>
              logger.warn({ err: error, pluginId }, "Worker plugin provider close failed"),
          });
          connections.set(connectionId, entry);
          workers.add(entry);
          const connected = await entry.ready;
          requireActive();
          if (connections.get(connectionId) !== entry)
            throw new Error("worker_provider_connection_closed");
          return { version: connected.version, capabilities: connected.capabilities };
        }
        case "providerSend": {
          const entry = connections.get(request.connectionId);
          if (!entry) throw new Error("unknown_worker_provider_connection");
          await entry.send(request.input);
          return null;
        }
        case "providerClose": {
          const entry = connections.get(request.connectionId);
          connections.delete(request.connectionId);
          entry?.detach();
          return null;
        }
        case "sessionFrame": {
          const socket = attached.has(request.sessionId)
            ? sessions.get(request.sessionId)?.socket
            : undefined;
          if (!socket) return null;
          const [data] = decodeRemotePluginFrame(request.frame);
          await new Promise<void>((resolve, reject) => {
            socket.send(data, (error) => (error ? reject(error) : resolve()));
          });
          return null;
        }
        case "sessionClose": {
          if (attached.has(request.sessionId)) sessions.get(request.sessionId)?.socket.close();
          return null;
        }
      }
    }

    function announceSessions() {
      for (const [id, session] of sessions) {
        if (attached.has(id)) continue;
        current.attach(id, session.pluginId);
        if (session.hello !== undefined) {
          current.forward(id, session.hello);
          session.hello = undefined;
        }
      }
    }

    function closeChannel() {
      if (detach) return detach;
      if (channel === current) channel = undefined;
      const error = new RemotePluginOperationError("worker_plugin_channel_closed");
      for (const reject of pending) reject(error);
      pending.clear();
      link.signal.removeEventListener("abort", onAbort);
      for (const stop of unsubscribe) stop();
      for (const id of attached) sessions.get(id)?.socket.close();
      for (const subscription of subscriptions.values()) subscription.release();
      subscriptions.clear();
      for (const connection of connections.values()) connection.detach();
      connections.clear();
      return (detach = Promise.resolve());
    }
    const onAbort = () => {
      void closeChannel();
    };
    const current = {
      attach(id: string, pluginId: string) {
        if (!activated || attached.has(id)) return;
        attached.add(id);
        sessions.get(id)?.socket.once("close", () => {
          attached.delete(id);
          publish({ type: "sessionClosed", sessionId: id });
        });
        publish({ type: "sessionOpened", sessionId: id, plugin: pluginRef(pluginId) });
      },
      forward(id: string, data: string | Uint8Array) {
        if (!activated) {
          const session = sessions.get(id);
          if (session && typeof data === "string" && isHelloFrame(data)) session.hello = data;
          return;
        }
        if (!attached.has(id)) announceSessions();
        publish({ type: "sessionFrame", sessionId: id, frame: encodeRemotePluginFrame(data) });
      },
      notifySettings(id: string, settingsId: string) {
        publish({ type: "settingsChanged", plugin: pluginRef(id), settingsId });
      },
      notifyPlugin(notification: ValidatedPluginNotification) {
        publish({ type: "pluginNotification", notification });
      },
      close: closeChannel,
    };
    channel = current;
    link.signal.addEventListener("abort", onAbort, { once: true });
    return { execute, close: closeChannel };
  }

  function close() {
    closing ??= Promise.resolve().then(async () => {
      await channel?.close();
      for (const session of sessions.values()) session.socket.close();
      await service.stopAllPlugins();
      for (const entry of workers) entry.retire();
      await started;
    });
    return closing;
  }

  return { openChannel, close };
}

function isHelloFrame(data: string): boolean {
  try {
    return JSON.parse(data)?.type === "hello";
  } catch {
    return false;
  }
}

interface WorkerPluginSession {
  cwd: string;
  model?: string;
  opening?: string;
  closing?: string;
  persistence?: ProviderPersistence;
  prompts: Set<string>;
  turns: Set<string>;
  earlyEnds: Set<string>;
}

interface ResidentProviderConnectionOptions {
  pluginId: string;
  providerId: string;
  owner: symbol;
  connection: Promise<ProviderConnection>;
  usageLaunch(): ProviderLaunch | undefined;
  workers: Set<ResidentProviderConnection>;
  publish?(event: ProviderEvent): void;
  onRetire?(): void;
  onError(error: unknown): void;
}

class ResidentProviderConnection {
  readonly ready: Promise<ProviderConnection>;
  private readonly sessions = new Map<string, WorkerPluginSession>();
  private detached = false;
  private stopped = false;
  private stopping = false;
  private inflight = 0;
  private unsubscribe?: () => void;

  constructor(private readonly options: ResidentProviderConnectionOptions) {
    this.ready = options.connection.then(
      (connection) => {
        if (!this.stopped) this.unsubscribe = connection.onEvent((event) => this.onEvent(event));
        return connection;
      },
      (error: unknown) => {
        this.retire();
        throw error;
      },
    );
  }

  get pluginId() {
    return this.options.pluginId;
  }

  usageSession(id: string): AgentUsageSession | null {
    const launch = this.options.usageLaunch();
    if (!launch || this.detached || this.stopped) return null;
    for (const [sessionId, session] of this.sessions) {
      if (session.opening || session.closing) continue;
      if (sessionId === id || (session.persistence && `plugin:${JSON.stringify(session.persistence)}` === id)) {
        return {
          provider: this.options.providerId,
          model: session.model,
          env: launch.env,
          launch,
          sessionKey: sessionId,
        };
      }
    }
    return null;
  }

  async send(input: ProviderInput) {
    const driver = await this.ready;
    const cwd = input.type === "session.open" ? await normalizedCwd(input.config.cwd) : undefined;
    if (this.detached || this.stopped) throw new Error("worker_provider_connection_closed");
    if (input.type === "session.open") {
      this.reserve(input, cwd!);
    } else if ("sessionId" in input) {
      const session = this.sessions.get(input.sessionId);
      if (!session) throw new RemotePluginOperationError("unknown_worker_plugin_session");
      if (session.closing) throw new RemotePluginOperationError("worker_plugin_session_busy");
      if (input.type === "session.prompt") {
        if (session.prompts.has(input.prompt.clientMessageId)) {
          throw new RemotePluginOperationError("worker_plugin_session_busy");
        }
        session.prompts.add(input.prompt.clientMessageId);
      } else if (input.type === "session.close") {
        if (mayBeRunning(session))
          throw new RemotePluginOperationError("worker_plugin_session_busy");
        session.closing = input.requestId;
      }
    } else if (
      (input.type === "session.archive" || input.type === "session.unarchive") &&
      this.conflicts(undefined, undefined, input.persistence)
    ) {
      throw new RemotePluginOperationError("worker_plugin_session_busy");
    }
    // A rejected transport acknowledgement cannot establish whether the native provider ran it.
    this.inflight += 1;
    try {
      await driver.send(input);
    } finally {
      this.inflight -= 1;
      this.closeWhenIdle();
    }
  }

  private reserve(input: Extract<ProviderInput, { type: "session.open" }>, cwd: string) {
    if (this.conflicts(input.sessionId, cwd, input.persistence)) {
      throw new RemotePluginOperationError("worker_plugin_session_busy");
    }
    this.sessions.set(input.sessionId, {
      cwd,
      model: input.config.model,
      opening: input.requestId,
      persistence: input.persistence,
      prompts: new Set(),
      turns: new Set(),
      earlyEnds: new Set(),
    });
  }

  private conflicts(
    sessionId: string | undefined,
    cwd: string | undefined,
    persistence: ProviderPersistence | undefined,
  ): boolean {
    for (const other of this.options.workers) {
      if (other.options.providerId !== this.options.providerId) continue;
      for (const [id, session] of other.sessions) {
        if (
          id === sessionId ||
          samePersistence(this.options.providerId, persistence, session.persistence)
        )
          return true;
        // Providers need not expose their history identity before session.open. Across channels
        // (or while a connection is retiring), fence the worker's canonical workspace as well.
        if (session.cwd === cwd && (other.detached || other.options.owner !== this.options.owner))
          return true;
      }
    }
    return false;
  }

  private onEvent(event: ProviderEvent) {
    if (this.stopped) return;
    if (event.type === "request.failed") {
      for (const [id, session] of this.sessions) {
        if (session.opening === event.requestId) this.sessions.delete(id);
        if (session.closing === event.requestId) session.closing = undefined;
      }
    } else if ("sessionId" in event) {
      let session = this.sessions.get(event.sessionId);
      if (!session && event.type === "session.opened" && event.parentSessionId) {
        const parent = this.sessions.get(event.parentSessionId);
        if (parent) {
          session = { cwd: parent.cwd, prompts: new Set(), turns: new Set(), earlyEnds: new Set() };
          this.sessions.set(event.sessionId, session);
        }
      }
      if (session) {
        switch (event.type) {
          case "session.opened":
            if (event.requestId === undefined || event.requestId === session.opening) {
              session.opening = undefined;
              session.persistence = event.persistence ?? session.persistence;
            }
            break;
          case "session.persistence":
            session.persistence = event.persistence;
            break;
          case "session.runtime_failed":
          case "session.closed":
            // AP emits this exact diagnostic after Bun's close event. PluginRuntime also
            // synthesizes runtime_failed for IPC failures, which are not exit evidence.
            if (
              event.type === "session.closed" ||
              (this.options.providerId === "ap-omp" && event.error.message === "OMP runtime exited")
            ) {
              this.sessions.delete(event.sessionId);
            }
            break;
          case "session.prompt_result":
            if (!session.prompts.delete(event.clientMessageId)) break;
            if (event.result.type === "turn" || event.result.type === "steer") {
              if (!session.earlyEnds.has(event.result.turnId))
                session.turns.add(event.result.turnId);
            }
            if (session.prompts.size === 0) session.earlyEnds.clear();
            break;
          case "session.turn":
            if (event.state === "started") {
              session.turns.add(event.turnId);
            } else {
              session.turns.delete(event.turnId);
              if (session.prompts.size > 0) session.earlyEnds.add(event.turnId);
            }
            break;
        }
      }
    }
    this.options.publish?.(event);
    this.closeWhenIdle();
  }

  detach() {
    this.detached = true;
    this.options.publish = undefined;
    this.options.onRetire = undefined;
    this.closeWhenIdle();
  }

  retire() {
    if (this.stopped) return;
    this.stopped = true;
    this.unsubscribe?.();
    this.sessions.clear();
    this.options.workers.delete(this);
    this.options.onRetire?.();
  }

  private closeWhenIdle() {
    if (
      !this.detached ||
      this.stopped ||
      this.stopping ||
      this.inflight ||
      [...this.sessions.values()].some(mayBeRunning)
    )
      return;
    this.stopping = true;
    void this.ready
      .then((connection) => connection.close())
      .then(
        () => this.retire(),
        (error: unknown) => {
          if (!this.stopped) this.options.onError(error);
        },
      );
  }
}

function mayBeRunning(session: WorkerPluginSession): boolean {
  return session.opening !== undefined || session.prompts.size > 0 || session.turns.size > 0;
}

function samePersistence(
  providerId: string,
  a: ProviderPersistence | undefined,
  b: ProviderPersistence | undefined,
): boolean {
  if (!a || !b) return false;
  if (isDeepStrictEqual(a, b)) return true;
  if (providerId !== "ap-omp") return false;
  // AP versions 1 and 2 both store Bun session history under sessions/<key>.
  const key = apOmpHistoryKey(a);
  return key !== undefined && key === apOmpHistoryKey(b);
}

function apOmpHistoryKey({ version, data }: ProviderPersistence): string | undefined {
  if ((version !== 1 && version !== 2) || !data || typeof data !== "object" || Array.isArray(data))
    return undefined;
  return typeof data.key === "string" && data.key ? data.key : undefined;
}
async function normalizedCwd(cwd: string): Promise<string> {
  const canonical = await realpath(cwd).catch(() => path.resolve(cwd));
  return process.platform === "win32" ? canonical.toLowerCase() : canonical;
}
