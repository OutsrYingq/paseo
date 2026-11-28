import { ProviderCatalogSession } from "../session/provider/provider-catalog-session.js";
import { randomUUID } from "node:crypto";
import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import type { AddressInfo } from "node:net";
import { WebSocket, WebSocketServer } from "ws";
import type { Logger } from "pino";
import { decodePluginFrame, PluginFrameOpcode } from "@getpaseo/protocol/binary-frames/index";
import { PluginAgentClientRegistry } from "../agent/plugin-provider.js";
import { CLIENT_CAPS } from "@getpaseo/protocol/client-capabilities";
import {
  WSInboundMessageSchema,
  type SessionInboundMessage,
  type SessionOutboundMessage,
  type WSHelloMessage,
  type WSOutboundMessage,
} from "@getpaseo/protocol/messages";
import { extractHttpBearerToken, extractWsBearerProtocol, extractWsBearerToken } from "../auth.js";
import { OWNER_PERMISSIONS, SessionAuthorization } from "../authorization/index.js";
import { isDaemonOriginAllowed } from "../websocket-server.js";
import {
  dispatchMaintenance,
  readWorkerAgent,
  refreshWorkerAgent,
  parseMaintenance,
  type WorkerMaintenanceOperation,
  type WorkerMaintenanceResult,
} from "./central-maintenance.js";
import { workerHost } from "./authority.js";
import { openCentralAgents, type CentralTimelineUpdate } from "./central-agents.js";
import { decodeJson, WorkerOperationError } from "./execution-protocol.js";
import {
  WorkerLeases,
  type ActiveWorkerTarget,
  type WorkerAuthority,
  type WorkerGrant,
  type WorkerLease,
} from "./leases.js";
import { RemoteProviderProcesses } from "./remote-provider-processes.js";
import { createRemoteTerminalManager, type RemoteTerminalManager } from "./remote-terminals.js";
import { createRemoteTerminalSession } from "./remote-terminal-session.js";
import { RemotePluginClient } from "./remote-plugins-client.js";
import { createRemotePluginSession } from "./remote-plugin-session.js";
import { attachWorkerPluginSocket } from "./remote-plugin-socket.js";
import { RemoteDownloads } from "./remote-downloads.js";
import { createCentralDirectory, createDirectorySubscriptions } from "./central-directory.js";
import { createRemoteState } from "./remote-state.js";
import { WorkerConnection, type WorkerTlsClientOptions } from "./worker-connection.js";

export interface CentralOptions {
  authority: WorkerAuthority;
  workerTls: WorkerTlsClientOptions;
  listen: { host: string; port: number };
  allowedOrigins?: ReadonlySet<string>;
  version: string;
  logger: Logger;
}

type WorkerScope = Awaited<ReturnType<typeof openCentralAgents>> & {
  directory: ReturnType<typeof createCentralDirectory>;
  connection: WorkerConnection;
  terminals: RemoteTerminalManager;
  plugins: RemotePluginClient;
};
const connectionLostEvent = Symbol("connection lost");
type TimelineSubscriptions = Map<string, ReadonlySet<string>>;

type Delivery = (event: CentralTimelineUpdate | typeof connectionLostEvent) => void;

export async function startRemoteCentral(options: CentralOptions) {
  const verifiedConnections = new WeakMap<ActiveWorkerTarget, WorkerConnection>();
  const leases = new WorkerLeases(options.authority, async (candidate) => {
    // AP's per-worker Service has one live writer. Binding verifies that the signed candidate still serves it.
    const connection = new WorkerConnection(candidate, options.workerTls);
    await connection.ready();
    verifiedConnections.set(candidate, connection);
  });
  const downloads = new RemoteDownloads();
  const scopeByTarget = new Map<ActiveWorkerTarget, Promise<WorkerScope>>();
  const restarting = new Set<ActiveWorkerTarget>();
  const operations = new Map<ActiveWorkerTarget, Set<Promise<unknown>>>();
  const audiences = new Map<ActiveWorkerTarget, Set<Delivery>>();
  const upgrades = new WebSocketServer({
    noServer: true,
    maxPayload: 16 * 1024 * 1024,
    handleProtocols: (protocols) => {
      return [...protocols].find((value) => extractWsBearerProtocol(value) !== null) ?? false;
    },
  });
  const allowedOrigins = new Set(options.allowedOrigins);
  let closing = false;

  async function runOperation<T>(
    execution: ActiveWorkerTarget,
    action: () => Promise<T>,
  ): Promise<T> {
    if (restarting.has(execution)) throw new WorkerOperationError("worker_restarting");
    const pending = operations.get(execution) ?? new Set<Promise<unknown>>();
    const task = action();
    pending.add(task);
    operations.set(execution, pending);
    try {
      return await task;
    } finally {
      pending.delete(task);
      if (!pending.size) operations.delete(execution);
    }
  }

  function scopeFor(execution: ActiveWorkerTarget): Promise<WorkerScope> {
    if (restarting.has(execution)) throw new WorkerOperationError("worker_restarting");
    const current = scopeByTarget.get(execution);
    if (current) return current;
    let providers: RemoteProviderProcesses;
    let terminals: RemoteTerminalManager;
    let plugins: RemotePluginClient;
    const connection =
      verifiedConnections.get(execution) ?? new WorkerConnection(execution, options.workerTls);
    verifiedConnections.delete(execution);
    connection.setEventHandler((event) => {
      if ("processId" in event) providers.onEvent(event);
      else if (event.type === "plugin") plugins.onEvent(event);
      else terminals.handleEvent(event);
    });
    providers = new RemoteProviderProcesses(connection);
    const logger = options.logger.child({
      workerId: execution.target.workerId,
      podUid: execution.target.podUid,
    });
    terminals = createRemoteTerminalManager(connection, (error) =>
      logger.warn({ err: error }, "Remote terminal operation failed"),
    );
    const state = createRemoteState(connection);
    const opened = openCentralAgents(
      {
        logger,
        state,
        manager: {
          workspacePaths: {
            resolve: (cwd) => cwd,
            async inspectDirectory(cwd) {
              const result = await connection.call({ kind: "inspectDirectory", cwd });
              if (result === "missing")
                throw Object.assign(new WorkerOperationError("worker_directory_missing"), {
                  code: "ENOENT",
                });
              if (result !== "directory")
                throw new WorkerOperationError("worker_directory_not_directory");
            },
          },
        },
        createClients: () => providers.createClients(logger),
      },
      (event) => {
        for (const deliver of audiences.get(execution) ?? []) deliver(event);
      },
      connection.signal,
    );
    plugins = new RemotePluginClient({
      scope: execution.target,
      signal: connection.signal,
      call: (operation) => connection.call(operation),
      async attachPluginSocket(binding, _plugin, socket) {
        if (
          binding.workerId !== execution.target.workerId ||
          binding.podUid !== execution.target.podUid
        )
          throw new Error("worker_plugin_binding_mismatch");
        return attachWorkerPluginSocket({
          socket,
          signal: connection.signal,
          target: execution.target,
          version: options.version,
          scope: () => opened,
        });
      },
    });
    const pluginProviders = new PluginAgentClientRegistry(logger);
    let refreshTail: Promise<void> = Promise.resolve();
    function refreshPlugins() {
      const next = refreshTail.then(async () => {
        const value = await opened;
        const registrations = await plugins.getProviderRegistrations();
        if (connection.signal.aborted) return;
        value.catalog.replacePluginProviders(registrations);
        pluginProviders.replace(registrations);
        return value.setPluginClients(pluginProviders.clients());
      });
      refreshTail = next.catch((error) =>
        logger.warn({ err: error }, "Worker plugin providers unavailable"),
      );
      return refreshTail;
    }
    const stopPlugins = plugins.subscribe((event) => {
      if (event.type === "providersChanged") void refreshPlugins();
    });
    const scope = opened.then(async (value) => {
      await Promise.all([terminals.initialize(), refreshPlugins()]);
      return Object.assign(value, {
        connection,
        terminals,
        plugins,
        directory: createCentralDirectory(logger, state, value, connection),
      });
    });
    scopeByTarget.set(execution, scope);
    const invalidate = () => {
      stopPlugins();
      void pluginProviders.shutdown().catch(() => undefined);
      terminals.dispose();
      if (scopeByTarget.get(execution) === scope) scopeByTarget.delete(execution);
      if (!connection.signal.aborted) connection.close();
      for (const deliver of audiences.get(execution) ?? []) deliver(connectionLostEvent);
    };
    connection.signal.addEventListener("abort", invalidate, { once: true });
    void scope.catch(invalidate);
    return scope;
  }

  function json(res: ServerResponse, code: number, body: object): void {
    res.writeHead(code, {
      "content-type": "application/json",
      "cache-control": "no-store",
    });
    res.end(JSON.stringify(body));
  }

  async function download(request: IncomingMessage, response: ServerResponse, hostname: string) {
    const query = new URLSearchParams(request.url?.split("?")[1]);
    const tokens = query.getAll("token");
    if (tokens.length !== 1 || !tokens[0])
      return json(response, 400, { error: "Missing download token" });
    if (!(await downloads.consume(hostname, tokens[0], response)))
      return json(response, 403, { error: "Invalid or expired token" });
  }

  async function http(request: IncomingMessage, response: ServerResponse): Promise<void> {
    const hostname = workerHost(request);
    if (!hostname) return json(response, 400, { error: "Invalid Host" });
    if (closing) return json(response, 503, { error: "Worker unavailable" });
    const origin = request.headers.origin;
    if (origin && !isDaemonOriginAllowed(origin, request.headers.host ?? null, allowedOrigins))
      return json(response, 403, { error: "Origin denied" });
    if (origin) {
      response.setHeader("access-control-allow-origin", origin);
      response.setHeader("vary", "Origin");
    }
    const route = request.url?.split("?", 1)[0];
    if (request.method === "GET" && route === "/api/files/download")
      return download(request, response, hostname);
    const token = extractHttpBearerToken(request.headers.authorization);
    const admitted = await leases.admit(
      hostname,
      token === null ? { kind: "none" } : { kind: "password", password: token },
    );
    if (admitted.kind !== "allowed") {
      const code = { unknown_host: 421, unauthorized: 401, unavailable: 503 }[admitted.kind];
      return json(response, code, { error: admitted.kind });
    }
    if (admitted.lease.signal.aborted) return json(response, 503, { error: "Worker unavailable" });
    if (request.method === "GET" && route === "/api/status") {
      const scope = admitted.lease.grant.scope;
      if (scope !== "owner" && !scope.includes("daemon.read"))
        return json(response, 403, { error: "Permission denied" });
      return json(response, 200, {
        status: "server_info",
        serverId: admitted.lease.grant.target.serverId,
        hostname,
        version: options.version,
        listen: `${options.listen.host}:${options.listen.port}`,
      });
    }
    if (request.method === "GET" && route === "/api/health")
      return json(response, 200, { status: "ok" });
    return json(response, 404, { error: "Route unavailable" });
  }

  function accept(socket: WebSocket, request: IncomingMessage, hostname: string): void {
    let starts = false;
    let cleanup = () => {};
    const timer = setTimeout(() => socket.close(4008, "Hello timeout"), 10_000);
    socket.on("error", () => socket.terminate());
    socket.once("close", () => {
      clearTimeout(timer);
      cleanup();
    });
    const token =
      extractHttpBearerToken(request.headers.authorization) ??
      extractWsBearerToken(extractWsBearerProtocol(request.headers["sec-websocket-protocol"]));
    function send(message: WSOutboundMessage): void {
      if (socket.readyState !== WebSocket.OPEN) return;
      const wire = JSON.stringify(message);
      if (Buffer.byteLength(wire) + socket.bufferedAmount > 16 * 1024 * 1024)
        return socket.terminate();
      socket.send(wire, (error) => {
        if (error) socket.terminate();
      });
    }
    const sendSession = (message: SessionOutboundMessage) => send({ type: "session", message });
    let handle: ((message: SessionInboundMessage) => Promise<void>) | undefined;
    let receiveBinary: ((bytes: Uint8Array) => void) | undefined;
    socket.on("message", (frame, binary) => {
      if (binary) {
        if (receiveBinary && Buffer.isBuffer(frame)) receiveBinary(frame);
        return;
      }
      const parsed = WSInboundMessageSchema.safeParse(binary ? undefined : decodeJson(frame));
      if (!parsed.success) return socket.close(1008, "Invalid message");
      const message = parsed.data;
      if (message.type === "ping") {
        send({ type: "pong" });
      } else if (message.type === "hello" && !starts) {
        starts = true;
        void hello(message).catch(() => socket.close(1013, "Worker unavailable"));
      } else if (handle && message.type === "session") {
        const operation = message.message;
        void handle(operation).catch((error: unknown) => {
          if ("requestId" in operation && typeof operation.requestId === "string") {
            sendSession({
              type: "rpc_error",
              payload: {
                requestId: operation.requestId,
                requestType: operation.type,
                error:
                  error instanceof WorkerOperationError ? error.message : "worker_operation_failed",
              },
            });
          } else {
            socket.close(1011, "Worker operation failed");
          }
        });
      } else {
        socket.close(1008, "Unexpected message");
      }
    });

    async function admitHello(message: WSHelloMessage): Promise<WorkerLease | undefined> {
      if (message.protocolVersion !== 1) {
        send({
          type: "hello.rejected",
          reason: "incompatible_protocol",
          accepts: ["password"],
        });
        socket.close(4003, "Incompatible protocol version");
        return;
      }
      const password = token ?? (message.auth?.kind === "password" ? message.auth.password : null);
      const admitted = await leases.admit(
        hostname,
        password === null ? { kind: "none" } : { kind: "password", password },
      );
      if (admitted.kind !== "allowed") {
        if (admitted.kind !== "unauthorized") {
          socket.close(1013, "Worker unavailable");
          return;
        }
        const reason = password === null ? "password_required" : "incorrect_password";
        if (token === null && (message.auth || message.capabilities?.hello_rejection)) {
          send({ type: "hello.rejected", reason, accepts: ["password"] });
        }
        socket.close(4401, password === null ? "Password required" : "Incorrect password");
        return;
      }
      return admitted.lease;
    }

    async function hello(message: WSHelloMessage): Promise<void> {
      const lease = await admitHello(message);
      if (!lease) return;
      const granted = lease.grant.scope === "owner" ? OWNER_PERMISSIONS : lease.grant.scope;
      const requested = message.sessionPermissions;
      const authorization = new SessionAuthorization(
        requested === undefined
          ? granted
          : granted.filter((permission) => requested.includes(permission)),
      );
      const scope = await scopeFor(lease.execution);
      if (lease.signal.aborted || socket.readyState !== WebSocket.OPEN)
        return socket.close(1013, "Worker unavailable");
      clearTimeout(timer);
      const sendAuthorized = (outbound: SessionOutboundMessage) => {
        if (!lease.signal.aborted && authorization.allowsOutbound(outbound)) sendSession(outbound);
      };
      const terminalSession = createRemoteTerminalSession({
        socket,
        hello: message,
        signal: lease.signal,
        connection: scope.connection,
        manager: scope.terminals,
        logger: options.logger,
        send: sendAuthorized,
        sendBinary(bytes) {
          if (
            socket.readyState !== WebSocket.OPEN ||
            socket.bufferedAmount + bytes.byteLength > 16 * 1024 * 1024
          )
            return socket.terminate();
          socket.send(bytes, (error) => {
            if (error) socket.terminate();
          });
        },
      });
      const pluginSession = createRemotePluginSession({
        client: scope.plugins,
        signal: lease.signal,
        send: sendAuthorized,
        canReceive: (outbound) => !lease.signal.aborted && authorization.allowsOutbound(outbound),
        supportsBinary: message.capabilities?.[CLIENT_CAPS.pluginBinaryPayloads] === true,
        sendBinary(bytes) {
          if (
            socket.readyState !== WebSocket.OPEN ||
            socket.bufferedAmount + bytes.byteLength > 16 * 1024 * 1024
          )
            return socket.terminate();
          socket.send(bytes, (error) => {
            if (error) socket.terminate();
          });
        },
      });
      receiveBinary = (bytes) => {
        const frame = decodePluginFrame(bytes);
        if (frame?.opcode === PluginFrameOpcode.Input) {
          void handle?.({
            type: "plugin.rpc.invoke.request",
            requestId: frame.requestId,
            pluginId: frame.pluginId,
            method: frame.method,
            input: frame.payload,
          });
          return;
        }
        if (!restarting.has(lease.execution) && authorization.allowsPermission("workspace.write")) {
          terminalSession.handleBinary(bytes);
        }
      };
      const subscriptions: TimelineSubscriptions = new Map();
      const eventSubscriptions = new Map<string, Set<string>>();
      const creationSubscriptions = new Map<string, () => void>();
      let sessionClosed = false;
      const directory = createDirectorySubscriptions({
        directory: scope.directory,
        manager: scope.manager,
        send: sendAuthorized,
        logger: options.logger,
      });
      const catalogSession = new ProviderCatalogSession({
        logger: options.logger,
        providerSnapshotManager: scope.catalog,
        host: {
          emit: sendAuthorized,
          resolveCwd: (cwd) => cwd,
          isProviderVisibleToClient: () => true,
          supportsCustomModeIcons: () =>
            message.capabilities?.[CLIENT_CAPS.customModeIcons] === true,
          supportsCompactProviderSnapshots: () =>
            message.capabilities?.[CLIENT_CAPS.compactProviderSnapshots] === true,
          supportsProviderSnapshotReferences: () =>
            message.capabilities?.[CLIENT_CAPS.providerSnapshotReferences] === true,
          listProviderAvailability: () => scope.manager.listProviderAvailability(),
          listDraftControls: (config) => scope.manager.listDraftControls(config),
          publishSnapshot(project) {
            for (const [subscriptionId, events] of eventSubscriptions) {
              if (!events.has("providers_snapshot_update")) continue;
              const outbound = project();
              if (outbound?.type === "providers_snapshot_update") {
                sendAuthorized({ ...outbound, payload: { ...outbound.payload, subscriptionId } });
              }
            }
          },
        },
      });
      const agentConfigSession = scope.createAgentConfigSession(sendAuthorized);
      catalogSession.start();
      const detached = () => socket.close(1013, "Worker lease ended");
      function deliverPermissionEvent(event: CentralTimelineUpdate) {
        if (
          event.type === "agent_stream" &&
          (event.payload.event.type === "permission_requested" ||
            event.payload.event.type === "permission_resolved") &&
          scope.manager.getAgent(event.payload.agentId)?.internal !== true
        ) {
          const { agentId, event: stream } = event.payload;
          for (const [subscriptionId, events] of eventSubscriptions) {
            if (stream.type === "permission_requested" && events.has("agent_permission_request")) {
              sendAuthorized({
                type: "agent_permission_request",
                payload: { agentId, request: stream.request, subscriptionId },
              });
            }
            if (stream.type === "permission_resolved" && events.has("agent_permission_resolved")) {
              sendAuthorized({
                type: "agent_permission_resolved",
                payload: {
                  agentId,
                  requestId: stream.requestId,
                  resolution: stream.resolution,
                  subscriptionId,
                },
              });
            }
          }
        }
      }
      const deliver = (event: CentralTimelineUpdate | typeof connectionLostEvent) => {
        if (event === connectionLostEvent) return socket.close(1013, "Worker connection lost");
        if (lease.signal.aborted) return;
        deliverPermissionEvent(event);
        for (const [subscriptionId, ids] of subscriptions) {
          if (!ids.has(event.payload.agentId)) continue;
          if (event.type === "agent_stream") {
            const item = event.payload.event;
            if (
              item.type === "timeline" &&
              ((item.item.type === "notification" &&
                !message.capabilities?.[CLIENT_CAPS.timelineNotifications]) ||
                (item.item.type === "plugin" &&
                  !message.capabilities?.[CLIENT_CAPS.pluginTimelineItems]))
            )
              continue;
            sendAuthorized({ ...event, payload: { ...event.payload, subscriptionId } });
          } else {
            sendAuthorized({ ...event, payload: { ...event.payload, subscriptionId } });
          }
        }
      };
      const audience = audiences.get(lease.execution) ?? new Set<typeof deliver>();
      audience.add(deliver);
      audiences.set(lease.execution, audience);
      lease.signal.addEventListener("abort", detached, { once: true });
      cleanup = () => {
        sessionClosed = true;
        for (const stop of creationSubscriptions.values()) stop();
        creationSubscriptions.clear();
        directory.close();
        catalogSession.dispose();
        terminalSession.close();
        pluginSession.close();
        lease.signal.removeEventListener("abort", detached);
        audience.delete(deliver);
        if (audience.size === 0) audiences.delete(lease.execution);
      };
      async function subscribeCreation(
        operation: Extract<SessionInboundMessage, { type: "creation.subscribe.request" }>,
      ) {
        if (creationSubscriptions.size >= 64)
          throw new WorkerOperationError("worker_subscription_limit");
        const subscriptionId = operation.subscribe === false ? undefined : randomUUID();
        const updates: Array<
          Extract<
            SessionOutboundMessage,
            { type: "agent.create.update" | "workspace.create.update" }
          >
        > = [];
        let attached = false;
        const observed = await scope.observeCreation(
          operation.kind,
          operation.idempotencyKey,
          (snapshot) => {
            if (!subscriptionId || sessionClosed) return;
            const update = {
              type:
                snapshot.kind === "agent"
                  ? ("agent.create.update" as const)
                  : ("workspace.create.update" as const),
              payload: { ...snapshot, subscriptionId },
            };
            if (attached) sendAuthorized(update);
            else updates.push(update);
          },
        );
        if (sessionClosed || !subscriptionId) observed.unsubscribe();
        else creationSubscriptions.set(subscriptionId, observed.unsubscribe);
        sendAuthorized({
          type: "creation.subscribe.response",
          payload: {
            requestId: operation.requestId,
            subscriptionId,
            snapshot: observed.snapshot,
            error: null,
          },
        });
        attached = true;
        for (const update of updates) sendAuthorized(update);
      }

      async function updateAgent(
        operation: Extract<SessionInboundMessage, { type: "update_agent_request" }>,
      ) {
        const { requestId, agentId } = operation;
        const result = await scope.updateAgent(operation).catch((error: unknown) => ({
          accepted: false,
          error: error instanceof Error ? error.message : "Failed to update agent",
        }));
        sendAuthorized({
          type: "update_agent_response",
          payload: { requestId, agentId, ...result },
        });
      }

      async function clearAgentAttention(
        operation: Extract<SessionInboundMessage, { type: "clear_agent_attention" }>,
      ) {
        const { agentId, requestId } = operation;
        const ids = Array.isArray(agentId) ? agentId : [agentId];
        await scope.clearAgentAttention(ids);
        if (requestId) {
          const agents = await Promise.all(ids.map((id) => scope.readSnapshot(id)));
          sendAuthorized({
            type: "clear_agent_attention_response",
            payload: { requestId, agentId, agents: agents.filter((agent) => agent !== null) },
          });
        }
      }

      function dispatchAgentCommand(operation: SessionInboundMessage): Promise<void> | undefined {
        switch (operation.type) {
          case "list_commands_request":
            return scope
              .listCommands(operation, async (cwd) => {
                if (cwd !== "~" && !cwd.startsWith("~/") && !cwd.startsWith("~\\")) return cwd;
                const home = await scope.connection.call({ kind: "providerHome" });
                return home + cwd.slice(1);
              })
              .then(sendAuthorized);
          case "set_agent_model_request":
            return agentConfigSession.handleSetAgentModelRequest(operation);
          case "set_agent_mode_request":
            return agentConfigSession.handleSetAgentModeRequest(operation);
          case "set_agent_thinking_request":
            return agentConfigSession.handleSetAgentThinkingRequest(operation);
          case "set_agent_feature_request":
            return agentConfigSession.handleSetAgentFeatureRequest(operation);
          case "agent.config.apply.request":
            return agentConfigSession.handleAgentConfigApplyRequest(operation);
          case "update_agent_request":
            return updateAgent(operation);
          case "clear_agent_attention":
            return clearAgentAttention(operation);
          case "agent_permission_response":
            return scope.respondToPermission(operation);
        }
      }

      const timelineProjection = {
        reasoningMerge: message.capabilities?.[CLIENT_CAPS.reasoningMergeEnum] === true,
        notification: message.capabilities?.[CLIENT_CAPS.timelineNotifications] === true,
        plugin: message.capabilities?.[CLIENT_CAPS.pluginTimelineItems] === true,
      };
      const handleOperation = async (operation: SessionInboundMessage) => {
        if (!authorization.allowsInbound(operation)) {
          if ("requestId" in operation && typeof operation.requestId === "string")
            sendSession({
              type: "rpc_error",
              payload: {
                requestId: operation.requestId,
                error: "Permission denied",
                code: "permission_denied",
              },
            });
          return;
        }
        if (operation.type === "subscription.release.request") {
          subscriptions.delete(operation.subscriptionId);
          eventSubscriptions.delete(operation.subscriptionId);
          directory.release(operation.subscriptionId);
          creationSubscriptions.get(operation.subscriptionId)?.();
          creationSubscriptions.delete(operation.subscriptionId);
          pluginSession.release(operation.subscriptionId);
          await terminalSession.release(operation.subscriptionId);
          sendAuthorized({
            type: "subscription.release.response",
            payload: { requestId: operation.requestId, subscriptionId: operation.subscriptionId },
          });
          return;
        }
        if (operation.type === "session.events.set_subscription.request") {
          if (eventSubscriptions.size >= 64)
            throw new WorkerOperationError("worker_subscription_limit");
          const subscriptionId = randomUUID();
          eventSubscriptions.set(subscriptionId, new Set(operation.events));
          sendAuthorized({
            type: "session.events.set_subscription.response",
            payload: { requestId: operation.requestId, subscriptionId },
          });
          return;
        }
        if (
          operation.type === "project.list.request" ||
          operation.type === "fetch_workspaces_request" ||
          operation.type === "fetch_agents_request" ||
          operation.type === "fetch_agent_history_request"
        ) {
          return directory.request(operation);
        }
        if (
          operation.type === "workspace.create.request" &&
          operation.agent &&
          !authorization.allowsPermission("workspace.write")
        ) {
          sendAuthorized({
            type: "rpc_error",
            payload: {
              requestId: operation.requestId,
              requestType: operation.type,
              code: "permission_denied",
              error: "Permission denied",
            },
          });
          return;
        }
        if (operation.type === "creation.subscribe.request") return subscribeCreation(operation);
        const agentCommand = dispatchAgentCommand(operation);
        if (agentCommand) return agentCommand;
        const catalog = catalogSession.dispatch(operation);
        if (catalog) return catalog;
        if (terminalSession.handles(operation)) return terminalSession.dispatch(operation);
        if (await pluginSession.handle(operation)) return;
        await dispatch(
          scope,
          lease,
          operation,
          subscriptions,
          downloads,
          timelineProjection,
          sendAuthorized,
        );
      };
      handle = async (operation) => {
        if (
          operation.type !== "restart_server_request" ||
          !authorization.allowsInbound(operation)
        ) {
          // A finish observer can wait on an agent that this restart will close.
          if (operation.type === "wait_for_finish_request" && !restarting.has(lease.execution)) {
            return handleOperation(operation);
          }
          return runOperation(lease.execution, () => handleOperation(operation));
        }
        const { requestId, reason } = operation;
        const fail = (code: string) =>
          sendAuthorized({
            type: "rpc_error",
            payload: { requestId, requestType: operation.type, code, error: code },
          });
        if (!scope.connection.supportsRuntimeRestart) return fail("worker_restart_unsupported");
        if (restarting.has(lease.execution)) {
          return fail("worker_restart_busy");
        }
        if (
          lease.signal.aborted ||
          lease.grant.expiresAt <= Date.now() ||
          scope.connection.signal.aborted
        ) {
          return fail("worker_restart_stale_target");
        }
        restarting.add(lease.execution);
        const deadline = AbortSignal.timeout(15_000);
        const signal = AbortSignal.any([deadline, scope.connection.signal, lease.signal]);
        const abandon = () => {
          if (deadline.aborted && !scope.connection.signal.aborted)
            fail("worker_restart_unconfirmed");
          scope.connection.close();
        };
        signal.addEventListener("abort", abandon, { once: true });
        try {
          await Promise.allSettled(operations.get(lease.execution) ?? []);
          signal.throwIfAborted();
          await scope.close();
          signal.throwIfAborted();
          if (lease.grant.expiresAt <= Date.now()) return;
          await scope.connection.call({
            kind: "restartRuntime",
            keepTerminals: operation.killTerminals !== true,
          });
          signal.throwIfAborted();
          if (lease.grant.expiresAt <= Date.now()) return;
          sendAuthorized({
            type: "status",
            payload: {
              status: "restart_requested",
              clientId: message.clientId,
              requestId,
              ...(reason?.trim() ? { reason } : {}),
            },
          });
        } catch {
          if (!signal.aborted) fail("worker_restart_unconfirmed");
        } finally {
          signal.removeEventListener("abort", abandon);
          if (scope.connection.signal.aborted) restarting.delete(lease.execution);
          else {
            scope.connection.signal.addEventListener(
              "abort",
              () => restarting.delete(lease.execution),
              { once: true },
            );
            scope.connection.close();
          }
        }
      };
      sendSession({
        type: "status",
        payload: {
          status: "server_info",
          protocolVersion: 1,
          serverId: lease.grant.target.serverId,
          hostname,
          version: options.version,
          permissions: authorization.listPermissions(),
          features: {
            agentRequestReceipts: true,
            creationLifecycle: true,
            workspaceMultiplicity: true,
            workspaceTerminals: true,
            ...(scope.connection.supportsRuntimeRestart &&
            authorization.allowsPermission("daemon.manage")
              ? { terminalsSurviveRestart: true }
              : {}),
            providersSnapshot: true,
            plugins: true,
            pluginManagement: true,
            wholeTurnTimeline: true,
            terminalCollection: true,
            "terminal-restore-modes": true,
            ownedSubscriptions: true,
            timelineFreshnessCheck: true,
          },
        },
      });
    }
  }

  const server = createServer((request, response) => {
    void http(request, response).catch(() => {
      if (!response.headersSent) json(response, 503, { error: "Worker unavailable" });
      else response.destroy();
    });
  });
  server.on("upgrade", (request, socket, head) => {
    const hostname = workerHost(request);
    if (!hostname) return void socket.end("HTTP/1.1 400 Bad Request\r\nConnection: close\r\n\r\n");
    if (
      closing ||
      request.url?.split("?", 1)[0] !== "/ws" ||
      (request.headers.origin &&
        !isDaemonOriginAllowed(
          request.headers.origin,
          request.headers.host ?? null,
          allowedOrigins,
        ))
    ) {
      return void socket.end("HTTP/1.1 403 Forbidden\r\nConnection: close\r\n\r\n");
    }
    upgrades.handleUpgrade(request, socket, head, (connected) =>
      accept(connected, request, hostname),
    );
  });
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(options.listen, () => {
      server.removeListener("error", reject);
      resolve();
    });
  });
  server.on("error", (error) =>
    options.logger.error({ err: error }, "Remote Central listener failed"),
  );
  return {
    port: (server.address() as AddressInfo).port,
    /** The embedding adapter must authenticate and bind the grant before calling. */
    async maintenance<Operation extends WorkerMaintenanceOperation>(
      grant: WorkerGrant,
      operation: Operation,
      signal: AbortSignal,
    ): Promise<WorkerMaintenanceResult[Operation["type"]]> {
      const input = parseMaintenance(grant, operation);
      try {
        return await leases.runMaintenance(input.grant, signal, (target, deadline) =>
          runOperation(target, async () => {
            const scope = await scopeFor(target);
            const active = AbortSignal.any([scope.connection.signal, deadline]);
            active.throwIfAborted();
            const response = await dispatchMaintenance(
              scope,
              input.operation,
              active,
              options.logger,
            );
            active.throwIfAborted();
            return response.payload as WorkerMaintenanceResult[Operation["type"]];
          }),
        );
      } catch (error) {
        if (error instanceof WorkerOperationError && error.message !== "worker_connection_lost") {
          throw error;
        }
        throw new WorkerOperationError("worker_maintenance_unconfirmed", { cause: error });
      }
    },
    async close() {
      closing = true;
      const opened = [...scopeByTarget.values()];
      for (const client of upgrades.clients) client.terminate();
      leases.close();
      const stopped = new Promise<void>((resolve) => server.close(() => resolve()));
      server.closeAllConnections();
      await stopped;
      await Promise.allSettled(opened.map(async (scope) => (await scope).close()));
    },
  };
}

async function dispatch(
  scope: WorkerScope,
  lease: WorkerLease,
  message: SessionInboundMessage,
  subscriptions: TimelineSubscriptions,
  downloads: RemoteDownloads,
  timelineProjection: Parameters<WorkerScope["fetchTimeline"]>[1],
  send: (message: SessionOutboundMessage) => void,
) {
  if (lease.signal.aborted) return;
  const reply = (response: SessionOutboundMessage) => {
    if (!lease.signal.aborted) send(response);
  };
  switch (message.type) {
    case "agent.timeline.set_subscription.request":
      return subscribeTimelines(scope, message, subscriptions, reply);
    case "fetch_agent_request":
    case "refresh_agent_request":
    case "fetch_agent_timeline_request":
    case "cancel_agent_request":
    case "archive_agent_request":
    case "wait_for_finish_request":
      return dispatchAgent(scope, lease, message, timelineProjection, reply);
    case "file_explorer_request":
    case "file_download_token_request":
      return dispatchFiles(scope, lease, message, downloads, reply);
    case "agent.create.request":
    case "create_agent_request":
    case "send_agent_message_request":
    case "workspace.create.request":
      return dispatchCreation(scope, lease, message, reply);
    case "ping": {
      const now = Date.now();
      reply({
        type: "pong",
        payload: {
          requestId: message.requestId,
          clientSentAt: message.clientSentAt,
          serverReceivedAt: now,
          serverSentAt: now,
        },
      });
      break;
    }
    default:
      if ("requestId" in message && typeof message.requestId === "string") {
        reply({
          type: "rpc_error",
          payload: {
            requestId: message.requestId,
            requestType: message.type,
            error: "worker_operation_unsupported",
          },
        });
      }
  }
}

function errorCode(error: unknown) {
  return error instanceof WorkerOperationError ? error.message : "worker_operation_failed";
}

async function subscribeTimelines(
  scope: WorkerScope,
  message: Extract<SessionInboundMessage, { type: "agent.timeline.set_subscription.request" }>,
  subscriptions: TimelineSubscriptions,
  reply: (response: SessionOutboundMessage) => void,
) {
  if (subscriptions.size >= 64) throw new WorkerOperationError("worker_subscription_limit");
  const agentIds = [...new Set(message.agentIds)].sort();
  const subscriptionId = randomUUID();
  subscriptions.set(subscriptionId, new Set(agentIds));
  const timelines: Record<string, "current" | "behind"> = {};
  const agents: Record<string, NonNullable<Awaited<ReturnType<WorkerScope["readSnapshot"]>>>> = {};
  if (message.cursors) {
    for (const agentId of agentIds) {
      const expected = message.cursors[agentId];
      const actual = scope.timelineHead(agentId);
      timelines[agentId] = "behind";
      if (!expected || actual?.epoch !== expected.epoch || actual.seq !== expected.seq) continue;
      const agent = await scope.readSnapshot(agentId);
      if (agent) {
        agents[agentId] = agent;
        timelines[agentId] = "current";
      }
    }
  }
  reply({
    type: "agent.timeline.set_subscription.response",
    payload: {
      requestId: message.requestId,
      agentIds,
      subscriptionId,
      ...(message.cursors ? { timelines, agents } : {}),
    },
  });
}

async function dispatchAgent(
  scope: WorkerScope,
  lease: WorkerLease,
  message: Extract<
    SessionInboundMessage,
    {
      type:
        | "fetch_agent_request"
        | "refresh_agent_request"
        | "fetch_agent_timeline_request"
        | "cancel_agent_request"
        | "archive_agent_request"
        | "wait_for_finish_request";
    }
  >,
  timelineProjection: Parameters<WorkerScope["fetchTimeline"]>[1],
  reply: (response: SessionOutboundMessage) => void,
) {
  switch (message.type) {
    case "fetch_agent_request": {
      reply(await readWorkerAgent(scope, message));
      break;
    }
    case "refresh_agent_request": {
      try {
        reply(await refreshWorkerAgent(scope, message, lease.signal));
      } catch (error) {
        if (lease.signal.aborted || scope.connection.signal.aborted) return;
        reply({
          type: "rpc_error",
          payload: {
            requestId: message.requestId,
            requestType: message.type,
            code: "agent_refresh_failed",
            error: error instanceof Error ? error.message : "Agent refresh failed",
          },
        });
      }
      break;
    }
    case "fetch_agent_timeline_request": {
      reply({
        type: "fetch_agent_timeline_response",
        payload: await scope.fetchTimeline(message, timelineProjection),
      });
      break;
    }
    case "cancel_agent_request": {
      let error: string | null = null;
      try {
        await scope.cancelAgent(message.agentId);
      } catch (cause) {
        error = errorCode(cause);
      }
      if (message.requestId)
        reply({
          type: "cancel_agent_response",
          payload: {
            requestId: message.requestId,
            agentId: message.agentId,
            agent: await scope.readSnapshot(message.agentId),
            error,
          },
        });
      break;
    }
    case "archive_agent_request": {
      const { agentId, archivedAt } = await scope.archiveAgent(message.agentId);
      reply({
        type: "agent_archived",
        payload: { requestId: message.requestId, agentId, archivedAt },
      });
      break;
    }
    case "wait_for_finish_request": {
      let result;
      try {
        result = await scope.waitForFinish(message.agentId, message.timeoutMs, lease.signal);
      } catch (error) {
        result = {
          status: "error" as const,
          final: null,
          lastMessage: null,
          error: errorCode(error),
        };
      }
      reply({
        type: "wait_for_finish_response",
        payload: { requestId: message.requestId, ...result },
      });
      break;
    }
  }
}

async function dispatchFiles(
  scope: WorkerScope,
  lease: WorkerLease,
  message: Extract<
    SessionInboundMessage,
    { type: "file_explorer_request" | "file_download_token_request" }
  >,
  downloads: RemoteDownloads,
  reply: (response: SessionOutboundMessage) => void,
) {
  switch (message.type) {
    case "file_explorer_request": {
      const { cwd, requestId, mode } = message;
      const path = message.path || ".";
      let files = { directory: null, file: null } as Awaited<
        ReturnType<typeof scope.connection.call<"explore">>
      >;
      let error: string | null = null;
      try {
        files = await scope.connection.call({
          kind: "explore",
          cwd,
          path,
          mode,
        });
      } catch (cause) {
        error = errorCode(cause);
      }
      reply({
        type: "file_explorer_response",
        payload: { cwd, path, requestId, mode, ...files, error },
      });
      break;
    }
    case "file_download_token_request": {
      const { cwd, path, requestId } = message;
      try {
        const issued = await downloads.issue(scope.connection, lease, cwd, path);
        reply({
          type: "file_download_token_response",
          payload: { cwd, path, requestId, ...issued, error: null },
        });
      } catch (cause) {
        reply({
          type: "file_download_token_response",
          payload: {
            cwd,
            path,
            requestId,
            token: null,
            size: null,
            mimeType: null,
            fileName: null,
            error: errorCode(cause),
          },
        });
      }
      break;
    }
  }
}

async function dispatchCreation(
  scope: WorkerScope,
  lease: WorkerLease,
  message: Extract<
    SessionInboundMessage,
    {
      type:
        | "agent.create.request"
        | "create_agent_request"
        | "send_agent_message_request"
        | "workspace.create.request";
    }
  >,
  reply: (response: SessionOutboundMessage) => void,
) {
  switch (message.type) {
    case "agent.create.request": {
      try {
        const creation = await scope.createAgent(message, lease.signal);
        reply({
          type: "agent.create.response",
          payload: {
            requestId: message.requestId,
            agent: creation.agent ?? null,
            creation,
            error: creation.error,
          },
        });
      } catch (error) {
        reply({
          type: "agent.create.response",
          payload: {
            requestId: message.requestId,
            agent: null,
            error: errorCode(error),
          },
        });
      }
      break;
    }
    case "create_agent_request": {
      try {
        const creation = await scope.createAgent(
          { ...message, type: "agent.create.request" },
          lease.signal,
        );
        if (creation.agent && !creation.error) {
          reply({
            type: "status",
            payload: {
              status: "agent_created",
              agentId: creation.agent.id,
              requestId: message.requestId,
              agent: creation.agent,
            },
          });
        } else {
          reply({
            type: "status",
            payload: {
              status: "agent_create_failed",
              requestId: message.requestId,
              error: creation.error ?? "worker_operation_failed",
            },
          });
        }
      } catch (error) {
        reply({
          type: "status",
          payload: {
            status: "agent_create_failed",
            requestId: message.requestId,
            error: errorCode(error),
          },
        });
      }
      break;
    }
    case "send_agent_message_request": {
      let error: string | null = null;
      try {
        await scope.sendMessage(message, lease.signal);
      } catch (cause) {
        error = errorCode(cause);
      }
      reply({
        type: "send_agent_message_response",
        payload: {
          requestId: message.requestId,
          agentId: message.agentId,
          accepted: error === null,
          error,
        },
      });
      break;
    }
    case "workspace.create.request": {
      try {
        const creation = await scope.createWorkspace(
          scope.directory,
          message,
          lease.signal,
          message.subscribe
            ? (snapshot) => reply({ type: "workspace.create.update", payload: snapshot })
            : undefined,
        );
        reply({
          type: "workspace.create.response",
          payload: {
            requestId: message.requestId,
            workspace: creation.workspace ?? null,
            agent: creation.agent ?? undefined,
            creation,
            setupTerminalId: null,
            error: creation.error,
            errorCode: creation.errorCode,
          },
        });
      } catch (cause) {
        reply({
          type: "workspace.create.response",
          payload: {
            requestId: message.requestId,
            workspace: null,
            setupTerminalId: null,
            error: errorCode(cause),
          },
        });
      }
      break;
    }
  }
}
