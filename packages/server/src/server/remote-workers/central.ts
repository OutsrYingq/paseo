import { ProviderCatalogSession } from "../session/provider/provider-catalog-session.js";
import { randomUUID } from "node:crypto";
import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import type { AddressInfo } from "node:net";
import WebSocket, { WebSocketServer } from "ws";
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
import { workerHost } from "./authority.js";
import { openCentralAgents, type CentralTimelineUpdate } from "./central-agents.js";
import { decodeJson, WorkerOperationError } from "./execution-protocol.js";
import {
  WorkerLeases,
  type ActiveWorkerTarget,
  type WorkerAuthority,
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
  const leases = new WorkerLeases(options.authority);
  const downloads = new RemoteDownloads();
  const scopeByTarget = new Map<ActiveWorkerTarget, Promise<WorkerScope>>();
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

  function scopeFor(execution: ActiveWorkerTarget): Promise<WorkerScope> {
    const current = scopeByTarget.get(execution);
    if (current) return current;
    let providers: RemoteProviderProcesses;
    let terminals: RemoteTerminalManager;
    let plugins: RemotePluginClient;
    const connection = new WorkerConnection(execution, options.workerTls, (event) => {
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
        value.setPluginClients(pluginProviders.clients());
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
    if (request.method === "GET" && route === "/api/files/download") {
      const query = new URLSearchParams(request.url?.split("?")[1]);
      const tokens = query.getAll("token");
      if (tokens.length !== 1 || !tokens[0])
        return json(response, 400, { error: "Missing download token" });
      if (!(await downloads.consume(hostname, tokens[0], response)))
        return json(response, 403, { error: "Invalid or expired token" });
      return;
    }
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

    async function hello(message: WSHelloMessage): Promise<void> {
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
        if (admitted.kind !== "unauthorized") return socket.close(1013, "Worker unavailable");
        const reason = password === null ? "password_required" : "incorrect_password";
        if (token === null && (message.auth || message.capabilities?.hello_rejection)) {
          send({ type: "hello.rejected", reason, accepts: ["password"] });
        }
        return socket.close(4401, password === null ? "Password required" : "Incorrect password");
      }
      const lease = admitted.lease;
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
        canReceive: (message) => !lease.signal.aborted && authorization.allowsOutbound(message),
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
        if (authorization.allowsPermission("workspace.write")) terminalSession.handleBinary(bytes);
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
      catalogSession.start();
      const detached = () => socket.close(1013, "Worker lease ended");
      const deliver = (event: CentralTimelineUpdate | typeof connectionLostEvent) => {
        if (event === connectionLostEvent) return socket.close(1013, "Worker connection lost");
        if (lease.signal.aborted) return;
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
      handle = async (operation) => {
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
          operation.type === "fetch_agents_request"
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
        if (operation.type === "creation.subscribe.request") {
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
          return;
        }
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
          {
            reasoningMerge: message.capabilities?.[CLIENT_CAPS.reasoningMergeEnum] === true,
            notification: message.capabilities?.[CLIENT_CAPS.timelineNotifications] === true,
            plugin: message.capabilities?.[CLIENT_CAPS.pluginTimelineItems] === true,
          },
          sendAuthorized,
        );
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
  const errorCode = (error: unknown) =>
    error instanceof WorkerOperationError ? error.message : "worker_operation_failed";
  switch (message.type) {
    case "agent.timeline.set_subscription.request": {
      if (subscriptions.size >= 64) throw new WorkerOperationError("worker_subscription_limit");
      const agentIds = [...new Set(message.agentIds)].sort();
      const subscriptionId = randomUUID();
      subscriptions.set(subscriptionId, new Set(agentIds));
      const timelines: Record<string, "current" | "behind"> = {};
      const agents: Record<
        string,
        NonNullable<Awaited<ReturnType<WorkerScope["readSnapshot"]>>>
      > = {};
      if (message.cursors) {
        for (const agentId of agentIds) {
          const expected = message.cursors[agentId];
          const actual = scope.timelineHead(agentId);
          timelines[agentId] = "behind";
          if (expected && actual?.epoch === expected.epoch && actual.seq === expected.seq) {
            const agent = await scope.readSnapshot(agentId);
            if (agent) {
              agents[agentId] = agent;
              timelines[agentId] = "current";
            }
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
      break;
    }
    case "fetch_agent_request": {
      const agent = await scope.readSnapshot(message.agentId);
      const project = agent?.workspaceId
        ? await scope.directory.projectForWorkspace(agent.workspaceId)
        : null;
      reply({
        type: "fetch_agent_response",
        payload: { requestId: message.requestId, agent, project, error: null },
      });
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
            error: "worker_operation_unsupported",
          },
        });
      }
  }
}
