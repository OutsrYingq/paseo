import { createServer, type ServerOptions } from "node:https";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { TLSSocket } from "node:tls";
import type { AddressInfo } from "node:net";
import { WebSocket, WebSocketServer } from "ws";
import {
  EXECUTION_FEATURES_HEADER,
  EXECUTION_MAX_CALLS,
  EXECUTION_MAX_FRAME,
  EXECUTION_PATH,
  EXECUTION_PROTOCOL,
  RUNTIME_RESTART_FEATURE,
  ExecutionRequestSchema,
  WorkerOperationError,
  decodeJson,
  type ExecutionResponse,
  type WorkerEvent,
  type WorkerOperation,
} from "./execution-protocol.js";
import { WorkerBindingSchema, sameWorkerBinding, type WorkerBinding } from "./leases.js";

export interface ResidentRuntime {
  execute(operation: Exclude<WorkerOperation, { kind: "restartRuntime" }>): Promise<unknown>;
  restartRuntime?(keepTerminals: boolean): Promise<void>;
  close(): Promise<void>;
}

export interface ResidentExecutionOptions {
  binding: WorkerBinding;
  paseoHome: string;
  listen: { host: string; port: number };
  tls: ServerOptions;
  authorizeCentral(certificateFingerprint256: string, binding: WorkerBinding): boolean;
  openRuntime(
    publish: (event: WorkerEvent) => void,
    signal: AbortSignal,
    binding: WorkerBinding,
  ): Promise<ResidentRuntime>;
  onError(error: unknown): void;
  shutdownRuntime?(): Promise<void>;
}

export async function startResidentExecution(options: ResidentExecutionOptions) {
  const ownBinding = WorkerBindingSchema.parse(options.binding);
  const diskId = (await readFile(path.join(options.paseoHome, "server-id"), "utf8")).trim();
  if (ownBinding.serverId !== diskId) throw new Error("worker_server_id_mismatch");
  const server = createServer(
    { ...options.tls, requestCert: true, rejectUnauthorized: true },
    (_req, res) => {
      res.writeHead(404).end();
    },
  );
  const upgrades = new WebSocketServer({
    noServer: true,
    maxPayload: EXECUTION_MAX_FRAME,
    handleProtocols: (protocols) =>
      protocols.has(EXECUTION_PROTOCOL) ? EXECUTION_PROTOCOL : false,
  });
  const shutdown = new AbortController();
  let active: Promise<void> | undefined;
  let fenced = false;
  let socket: WebSocket | undefined;
  server.on("upgrade", (request, stream, head) => {
    const peer = request.socket instanceof TLSSocket ? request.socket : undefined;
    const fingerprint = peer?.getPeerCertificate().fingerprint256;
    if (!peer?.authorized || !fingerprint || !options.authorizeCentral(fingerprint, ownBinding)) {
      stream.end("HTTP/1.1 403 Forbidden\r\nConnection: close\r\n\r\n");
      return;
    }
    if (
      request.url !== EXECUTION_PATH ||
      request.headers["sec-websocket-protocol"] !== EXECUTION_PROTOCOL
    ) {
      stream.end("HTTP/1.1 400 Bad Request\r\nConnection: close\r\n\r\n");
      return;
    }
    if (shutdown.signal.aborted || active || fenced) {
      stream.end("HTTP/1.1 503 Service Unavailable\r\nConnection: close\r\n\r\n");
      return;
    }
    upgrades.handleUpgrade(request, stream, head, (connected) => {
      socket = connected;
      active = handleChannel(
        connected,
        ownBinding,
        options,
        request.headers[EXECUTION_FEATURES_HEADER] === RUNTIME_RESTART_FEATURE,
      )
        .catch((error: unknown) => {
          fenced = true;
          options.onError(error);
        })
        .finally(() => {
          active = undefined;
          socket = undefined;
        });
    });
  });
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(options.listen, () => {
      server.removeListener("error", reject);
      resolve();
    });
  });
  server.on("error", options.onError);
  return {
    port: (server.address() as AddressInfo).port,
    async close() {
      shutdown.abort();
      socket?.terminate();
      const closed = new Promise<void>((resolve) => server.close(() => resolve()));
      server.closeAllConnections();
      await active;
      await options.shutdownRuntime?.();
      await closed;
    },
  };
}

async function handleChannel(
  socket: WebSocket,
  binding: WorkerBinding,
  options: ResidentExecutionOptions,
  negotiateRestart: boolean,
): Promise<void> {
  const lifetime = new AbortController();
  const inflight = new Map<string, Promise<void>>();
  let runtime: Promise<ResidentRuntime> | undefined;
  let admitted: ResidentRuntime | undefined;
  let reset: Promise<void> | undefined;
  const timer = setTimeout(() => socket.terminate(), 10_000);
  let alive = true;
  socket.on("pong", () => {
    alive = true;
  });
  const heartbeat = setInterval(() => {
    if (!alive) return socket.terminate();
    alive = false;
    socket.ping();
  }, 5_000);
  const closed = new Promise<void>((resolve) =>
    socket.once("close", () => {
      clearInterval(heartbeat);
      clearTimeout(timer);
      lifetime.abort();
      resolve();
    }),
  );
  socket.on("error", () => socket.terminate());
  function send(message: ExecutionResponse): void {
    if (socket.readyState !== WebSocket.OPEN) return;
    const frame = JSON.stringify(message);
    if (Buffer.byteLength(frame) + socket.bufferedAmount > EXECUTION_MAX_FRAME) {
      socket.terminate();
      return;
    }
    socket.send(frame, (error) => {
      if (error) socket.terminate();
    });
  }
  socket.on("message", (raw, binary) => {
    const parsed = ExecutionRequestSchema.safeParse(binary ? undefined : decodeJson(raw));
    if (lifetime.signal.aborted || !parsed.success) return socket.terminate();
    const message = parsed.data;
    if (message.kind === "bind") {
      if (runtime || !sameWorkerBinding(binding, message.binding)) return socket.terminate();
      runtime = options.openRuntime(
        (event) => {
          if (admitted) send({ kind: "event", event });
        },
        lifetime.signal,
        binding,
      );
      void runtime.then(
        (opened) => {
          admitted = opened;
          clearTimeout(timer);
          return send({
            kind: "bound",
            binding,
            ...(negotiateRestart && opened.restartRuntime ? { restartRuntime: true } : {}),
          });
        },
        () => socket.terminate(),
      );
      return;
    }
    if (!admitted || inflight.has(message.id) || inflight.size >= EXECUTION_MAX_CALLS)
      return socket.terminate();
    const { operation, id } = message;
    if (reset) {
      send({ kind: "error", id, code: "worker_restarting" });
      return;
    }
    if (operation.kind === "restartRuntime") {
      if (!negotiateRestart || !admitted.restartRuntime) {
        send({ kind: "error", id, code: "worker_restart_unsupported" });
        return;
      }
      const restart = admitted.restartRuntime.bind(admitted);
      reset = Promise.allSettled(inflight.values()).then(() =>
        lifetime.signal.aborted ? undefined : restart(operation.keepTerminals),
      );
      void reset.then(
        () => send({ kind: "result", id, value: null }),
        () => send({ kind: "error", id, code: "worker_restart_unconfirmed" }),
      );
      return;
    }
    const task = admitted
      .execute(operation)
      .then(
        (value) => send({ kind: "result", id: message.id, value }),
        (error: unknown) => {
          if (!(error instanceof WorkerOperationError)) options.onError(error);
          send({
            kind: "error",
            id: message.id,
            code: error instanceof WorkerOperationError ? error.message : "worker_operation_failed",
          });
        },
      )
      .finally(() => inflight.delete(message.id));
    inflight.set(message.id, task);
  });
  await closed;
  try {
    try {
      await reset;
    } finally {
      if (runtime) await (await runtime).close();
    }
  } finally {
    await Promise.allSettled(inflight.values());
  }
}
