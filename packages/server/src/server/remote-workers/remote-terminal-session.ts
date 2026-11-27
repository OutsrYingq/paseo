import type { Logger } from "pino";
import type WebSocket from "ws";
import { CLIENT_CAPS } from "@getpaseo/protocol/client-capabilities";
import { decodeBinaryFrame } from "@getpaseo/protocol/binary-frames/index";
import type {
  SessionInboundMessage,
  SessionOutboundMessage,
  WSHelloMessage,
} from "@getpaseo/protocol/messages";
import { TerminalSessionController } from "../../terminal/terminal-session-controller.js";
import { SessionDelivery } from "../session/owned-subscriptions/index.js";
import { isSameOrDescendantPath } from "../path-utils.js";
import type { RemoteTerminalManager } from "./remote-terminals.js";
import type { WorkerConnection } from "./worker-connection.js";

const requests = new Set<SessionInboundMessage["type"]>([
  "subscribe_terminals_request",
  "unsubscribe_terminals_request",
  "list_terminals_request",
  "create_terminal_request",
  "subscribe_terminal_request",
  "unsubscribe_terminal_request",
  "terminal_input",
  "kill_terminal_request",
  "capture_terminal_request",
  "terminal.rename.request",
]);

export function createRemoteTerminalSession(options: {
  connection: WorkerConnection;
  manager: RemoteTerminalManager;
  logger: Logger;
  socket: WebSocket;
  hello: WSHelloMessage;
  send(message: SessionOutboundMessage): void;
  sendBinary(bytes: Uint8Array): void;
  signal: AbortSignal;
}) {
  const { socket, signal, hello } = options;
  const owned = new SessionDelivery(
    (_source, msg) => options.send(msg),
    (_source, bytes) => {
      if (!signal.aborted) options.sendBinary(bytes);
    },
  );
  owned.attach(socket, hello.capabilities?.[CLIENT_CAPS.ownedSubscriptions] === true);
  const controller = new TerminalSessionController({
    terminalManager: options.manager,
    emit: (msg) => {
      if (!signal.aborted) options.send(msg);
    },
    hasBinaryChannel: () => true,
    supportsChunkedReplay: () => hello.capabilities?.[CLIENT_CAPS.terminalChunkedReplay] === true,
    isPathWithinRoot: isSameOrDescendantPath,
    sessionLogger: options.logger,
    listTerminalWorkspaceRefs: () => options.connection.call({ kind: "workspaceRefs" }),
    getClientBufferedAmount: () => socket.bufferedAmount,
  });
  return {
    handles: (message: SessionInboundMessage) => requests.has(message.type),
    dispatch: (message: SessionInboundMessage) =>
      owned.request(socket, message, async () => {
        if (!signal.aborted) await controller.dispatch(message, owned);
      }),
    handleBinary(bytes: Uint8Array) {
      if (signal.aborted) return;
      const frame = decodeBinaryFrame(bytes);
      if (frame?.kind === "terminal") controller.handleBinaryFrame(frame.frame, socket);
    },
    release(subscriptionId: string) {
      return owned.release(subscriptionId);
    },
    close() {
      controller.dispose();
      void owned.detach(socket);
    },
  };
}
