import { Agent as HttpsAgent } from "node:https";
import { randomUUID } from "node:crypto";
import { checkServerIdentity, type ConnectionOptions, type PeerCertificate } from "node:tls";
import WebSocket from "ws";
import { sameWorkerBinding, type ActiveWorkerTarget } from "./leases.js";
import {
  EXECUTION_MAX_CALLS,
  EXECUTION_MAX_FRAME,
  EXECUTION_PROTOCOL,
  ExecutionResponseSchema,
  WorkerOperationError,
  decodeJson,
  operationResults,
  type WorkerEvent,
  type WorkerOperation,
  type WorkerResult,
} from "./execution-protocol.js";

export type WorkerTlsClientOptions = Pick<
  ConnectionOptions,
  "cert" | "key" | "ca" | "pfx" | "passphrase"
>;

export class WorkerConnection {
  private readonly socket: WebSocket;
  private readonly pending = new Map<
    string,
    { resolve(value: unknown): void; reject(error: Error): void }
  >();
  private readonly binding: Promise<void>;
  private readonly lifetime = new AbortController();
  readonly signal = this.lifetime.signal;

  constructor(
    lease: ActiveWorkerTarget,
    tls: WorkerTlsClientOptions,
    onEvent: (event: WorkerEvent) => void,
  ) {
    const { workerId, podUid, serverId } = lease.target;
    const binding = { workerId, podUid, serverId };
    const endpoint = new URL(lease.target.endpoint);
    if (endpoint.username || endpoint.password || endpoint.hash || endpoint.search)
      throw new Error("invalid_worker_endpoint");
    if (lease.signal.aborted) throw new Error("worker_unavailable");
    const fingerprint = lease.target.certFingerprint256;
    const agent = new HttpsAgent({
      ...tls,
      rejectUnauthorized: true,

      checkServerIdentity(hostname: string, cert: PeerCertificate) {
        const err = checkServerIdentity(hostname, cert);
        if (err) return err;
        if (cert.fingerprint256 !== fingerprint) return new Error("worker_certificate_mismatch");
      },
    });
    this.socket = new WebSocket(endpoint, EXECUTION_PROTOCOL, {
      agent,
      followRedirects: false,
      handshakeTimeout: 5_000,
      maxPayload: EXECUTION_MAX_FRAME,
      perMessageDeflate: false,
    });
    const socket = this.socket;
    let bound = false;
    this.binding = new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => this.close(), 10_000);
      const stop = () => this.close();
      lease.signal.addEventListener("abort", stop, { once: true });
      socket.on("error", stop);
      socket.once("close", () => {
        clearTimeout(timer);
        lease.signal.removeEventListener("abort", stop);
        agent.destroy();
        this.lifetime.abort();
        const error = new WorkerOperationError("worker_connection_lost");
        reject(error);
        for (const pending of this.pending.values()) pending.reject(error);
        this.pending.clear();
      });
      socket.once("open", () => socket.send(JSON.stringify({ kind: "bind", binding })));
      socket.on("message", (raw, binary) => {
        const decoded = binary ? undefined : decodeJson(raw);
        const parsed = ExecutionResponseSchema.safeParse(decoded);
        if (!parsed.success || lease.signal.aborted) return this.close();
        const message = parsed.data;
        if (!bound) {
          if (message.kind !== "bound" || !sameWorkerBinding(binding, message.binding))
            return this.close();
          bound = true;
          clearTimeout(timer);
          resolve();
          return;
        }
        if (message.kind === "bound") return this.close();
        if (message.kind === "event") {
          onEvent(message.event);
          return;
        }
        const pending = this.pending.get(message.id);
        if (!pending) return this.close();
        this.pending.delete(message.id);
        if (message.kind === "error") pending.reject(new WorkerOperationError(message.code));
        else pending.resolve(message.value);
      });
    });
    void this.binding.catch(() => undefined);
  }

  async ready(): Promise<void> {
    await this.binding;
    if (this.socket.readyState !== WebSocket.OPEN)
      throw new WorkerOperationError("worker_unavailable");
  }

  async call<K extends WorkerOperation["kind"]>(
    operation: WorkerOperation & { kind: K },
  ): Promise<WorkerResult<K>> {
    await this.ready();
    if (this.pending.size >= EXECUTION_MAX_CALLS) throw new WorkerOperationError("worker_busy");
    const id = randomUUID();
    const frame = JSON.stringify({ kind: "call", id, operation });
    if (
      Buffer.byteLength(frame) > EXECUTION_MAX_FRAME ||
      this.socket.bufferedAmount > EXECUTION_MAX_FRAME
    ) {
      throw new WorkerOperationError("worker_request_too_large");
    }
    const value = await new Promise<unknown>((resolve, reject) => {
      this.pending.set(id, { resolve, reject });
      this.socket.send(frame, (error) => {
        if (error) this.close();
      });
    });
    return operationResults[operation.kind].parse(value) as WorkerResult<K>;
  }

  close(): void {
    this.socket.terminate();
  }
}
