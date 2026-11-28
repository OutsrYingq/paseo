import { WSInboundMessageSchema, type WSOutboundMessage } from "@getpaseo/protocol/messages";
import type { PluginSessionSocket } from "../plugins/session-socket.js";
import type { WorkerBinding } from "./leases.js";
import type { openCentralAgents } from "./central-agents.js";
import type { createCentralDirectory } from "./central-directory.js";
import { decodeJson } from "./execution-protocol.js";

/** An internal plugin socket is admitted by the worker channel, with a scoped read/control surface. */
export function attachWorkerPluginSocket(options: {
  socket: PluginSessionSocket;
  signal: AbortSignal;
  target: WorkerBinding;
  version: string;
  scope(): Promise<Awaited<ReturnType<typeof openCentralAgents>>>;
  directory(): Promise<ReturnType<typeof createCentralDirectory>>;
}) {
  const { socket, target, signal } = options;
  let hello = false;
  const send = (wire: WSOutboundMessage) => {
    if (!signal.aborted && socket.readyState === 1) socket.send(JSON.stringify(wire));
  };
  function stop() {
    socket.close();
  }
  signal.addEventListener("abort", stop, { once: true });
  const closed = new Promise<void>((resolve) =>
    socket.once("close", () => {
      signal.removeEventListener("abort", stop);
      resolve();
    }),
  );
  socket.on("message", (raw, binary) => {
    if (signal.aborted || binary) return;
    const parsed = WSInboundMessageSchema.safeParse(
      typeof raw === "string" ? decodeJson(raw) : undefined,
    );
    if (!parsed.success) return socket.close();
    const msg = parsed.data;
    if (msg.type === "ping") {
      send({ type: "pong" });
      return;
    }
    if (msg.type === "hello" && !hello && msg.protocolVersion === 1) {
      hello = true;
      send({
        type: "session",
        message: {
          type: "status",
          payload: {
            status: "server_info",
            serverId: target.serverId,
            protocolVersion: 1,
            version: options.version,
            permissions: ["daemon.read", "workspace.read"],
            features: { workspaceMultiplicity: true },
          },
        },
      });
      return;
    }
    if (!hello || msg.type !== "session") return socket.close();
    const request = msg.message;
    if (!("requestId" in request) || typeof request.requestId !== "string") return;
    const { requestId } = request;
    if (request.type === "fetch_agent_timeline_request") {
      void options
        .scope()
        .then((scope) =>
          scope.fetchTimeline(request, { reasoningMerge: true, notification: true, plugin: true }),
        )
        .then(
          (payload) =>
            send({ type: "session", message: { type: "fetch_agent_timeline_response", payload } }),
          () => socket.close(),
        );
      return;
    }
    if (request.type === "fetch_workspaces_request" && !request.subscribe && !request.sync) {
      void options
        .directory()
        .then((directory) => directory.read(request))
        .then(
          (message) => send({ type: "session", message }),
          () => socket.close(),
        );
      return;
    }
    send({
      type: "session",
      message: {
        type: "rpc_error",
        payload: {
          requestId,
          requestType: request.type,
          error: "worker_plugin_session_operation_unavailable",
        },
      },
    });
  });
  return { closed };
}
