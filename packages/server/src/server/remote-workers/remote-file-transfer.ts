import { createHash, randomUUID } from "node:crypto";
import {
  encodeFileTransferFrame,
  FileTransferOpcode,
  type FileTransferFrame,
} from "@getpaseo/protocol/binary-frames/index";
import type { AgentAttachment } from "@getpaseo/protocol/messages";
import type { FileExplorerRequest, FileUploadRequest, SessionOutboundMessage } from "../messages.js";
import { WorkerOperationError } from "./execution-protocol.js";
import type { WorkerLease } from "./leases.js";
import {
  MAX_ACTIVE_FILE_TRANSFERS,
  MAX_UPLOAD_FRAME_BYTES,
  remoteUploadIdSchema,
} from "./remote-file-transfer-protocol.js";
import type { WorkerConnection } from "./worker-connection.js";

interface Upload {
  id: string;
  requestId: string;
  timer?: ReturnType<typeof setTimeout>;
  opening: Promise<unknown>;
  tail: Promise<void>;
  stage: "open" | "begun" | "ended";
  remaining: number;
}

const STALE_UPLOAD_MS = 10 * 60_000;
const PAUSE_BYTES = 2 * 1024 * 1024;
const RESUME_BYTES = PAUSE_BYTES / 2;
const MAX_PENDING_BYTES = MAX_UPLOAD_FRAME_BYTES + PAUSE_BYTES;
const MAX_PENDING_FRAMES = 128;

export function createRemoteFileTransfer(options: {
  connection: WorkerConnection;
  lease: WorkerLease;
  signal: AbortSignal;
  send(response: SessionOutboundMessage): void;
  sendBinary?(bytes: Uint8Array): Promise<void>;
  setUploadPaused?(paused: boolean): void;
}) {
  const { connection, lease, send, sendBinary, setUploadPaused } = options;
  const signal = AbortSignal.any([options.signal, lease.signal, connection.signal]);
  const { hostname, principalId, target } = lease.grant;
  const owner = createHash("sha256")
    .update(JSON.stringify([hostname, principalId, target.workerId, target.serverId]))
    .digest("hex");
  const uploads = new Map<string, Upload>();
  let queuedBytes = 0;
  let queuedFrames = 0;
  let paused = false;

  function requireActive(requestSignal?: AbortSignal) {
    if (signal.aborted || requestSignal?.aborted || lease.grant.expiresAt <= Date.now()) {
      throw new WorkerOperationError("worker_client_disconnected");
    }
  }

  function replyError(requestId: string, error: string) {
    send({ type: "file.upload.response", payload: { requestId, file: null, error } });
  }

  function remove(upload: Upload) {
    clearTimeout(upload.timer);
    uploads.delete(upload.requestId);
  }

  function cancel(upload: Upload, error?: string) {
    if (uploads.get(upload.requestId) !== upload) return;
    remove(upload);
    void upload.opening
      .then(() => connection.call({ kind: "uploadCancel", id: upload.id }))
      .catch(() => undefined);
    if (error && !signal.aborted) replyError(upload.requestId, error);
  }

  function newTimeout(upload: Upload) {
    const timer = setTimeout(() => cancel(upload, "worker_upload_expired"), STALE_UPLOAD_MS);
    timer.unref?.();
    return timer;
  }

  signal.addEventListener("abort", () => {
    for (const upload of uploads.values()) cancel(upload);
    if (paused) {
      paused = false;
      setUploadPaused?.(false);
    }
  }, { once: true });

  return {
    beginUpload(request: FileUploadRequest): void {
      requireActive();
      const existing = uploads.get(request.requestId);
      if (existing) {
        cancel(existing, "worker_upload_duplicate_request");
        return;
      }
      const requestIdBytes = Buffer.byteLength(request.requestId);
      if (requestIdBytes < 1 || requestIdBytes > 255 || uploads.size >= MAX_ACTIVE_FILE_TRANSFERS) {
        replyError(request.requestId, "worker_upload_unavailable");
        return;
      }
      const id = randomUUID();
      const opening = connection.call({ kind: "uploadOpen", id, owner, request });
      const upload: Upload = {
        id,
        requestId: request.requestId,
        stage: "open",
        remaining: request.size,
        opening,
        tail: opening.then(() => undefined),
      };
      upload.timer = newTimeout(upload);
      uploads.set(request.requestId, upload);
      upload.tail = upload.tail.catch(() => cancel(upload, "worker_upload_unavailable"));
    },

    handleFrame(frame: FileTransferFrame): void {
      const upload = uploads.get(frame.requestId);
      if (signal.aborted || !upload) return;
      const length = frame.payload.byteLength;
      if (
        queuedFrames >= MAX_PENDING_FRAMES ||
        length > MAX_UPLOAD_FRAME_BYTES || queuedBytes + length > MAX_PENDING_BYTES ||
        (frame.opcode === FileTransferOpcode.FileBegin ? upload.stage !== "open" : upload.stage !== "begun") ||
        (frame.opcode === FileTransferOpcode.FileEnd ? upload.remaining !== 0 : length > upload.remaining)
      ) {
        cancel(upload, "worker_upload_invalid_frame");
        return;
      }
      if (frame.opcode === FileTransferOpcode.FileBegin) upload.stage = "begun";
      else if (frame.opcode === FileTransferOpcode.FileEnd) upload.stage = "ended";
      else upload.remaining -= length;
      clearTimeout(upload.timer);
      upload.timer = newTimeout(upload);
      queuedBytes += length;
      queuedFrames++;
      if (!paused && (queuedBytes >= PAUSE_BYTES || queuedFrames >= MAX_PENDING_FRAMES / 2)) {
        paused = true;
        setUploadPaused?.(true);
      }
      upload.tail = upload.tail.then(async () => {
        if (uploads.get(frame.requestId) !== upload) return;
        requireActive();
        const { id, requestId } = upload;
        switch (frame.opcode) {
          case FileTransferOpcode.FileBegin:
            await connection.call({ kind: "uploadBegin", id, metadata: frame.metadata });
            break;
          case FileTransferOpcode.FileChunk:
            await connection.call({
              kind: "uploadChunk",
              id,
              bytes: Buffer.from(frame.payload.buffer, frame.payload.byteOffset, length).toString("base64"),
            });
            break;
          case FileTransferOpcode.FileEnd: {
            const file = await connection.call({ kind: "uploadEnd", id });
            requireActive();
            if (uploads.get(requestId) !== upload) return;
            remove(upload);
            send({
              type: "file.upload.response",
              payload: { requestId, file, error: null },
            });
            break;
          }
        }
      }).catch(() => cancel(upload, "worker_upload_unavailable")).finally(() => {
        queuedBytes -= length;
        queuedFrames--;
        if (paused && queuedBytes <= RESUME_BYTES && queuedFrames < MAX_PENDING_FRAMES / 2) {
          paused = false;
          setUploadPaused?.(false);
        }
      });
    },

    async explore(request: FileExplorerRequest): Promise<void> {
      const { requestId, mode, maxBytes } = request;
      const cwd = request.cwd.trim();
      const path = request.path ?? ".";
      try {
        requireActive();
        if (!cwd) throw new WorkerOperationError("cwd is required");
        if (mode === "file" && request.acceptBinary && sendBinary) {
          const id = randomUUID();
          try {
            const metadata = await connection.call({ kind: "explorerStreamOpen", id, cwd, path, maxBytes });
            requireActive();
            await sendBinary(encodeFileTransferFrame({ opcode: FileTransferOpcode.FileBegin, requestId, metadata }));
            for (;;) {
              requireActive();
              const bytes = await connection.call({ kind: "explorerStreamNext", id });
              requireActive();
              if (bytes === null) break;
              await sendBinary(encodeFileTransferFrame({ opcode: FileTransferOpcode.FileChunk, requestId, payload: Buffer.from(bytes, "base64") }));
            }
            await sendBinary(encodeFileTransferFrame({ opcode: FileTransferOpcode.FileEnd, requestId }));
          } finally {
            void connection.call({ kind: "explorerStreamClose", id }).catch(() => undefined);
          }
          return;
        }
        const files = await connection.call({ kind: "explore", mode, cwd, path, maxBytes });
        requireActive();
        send({ type: "file_explorer_response", payload: { ...files, cwd, path: files.directory?.path ?? files.file?.path ?? path, requestId, mode, error: null } });
      } catch (error) {
        if (!signal.aborted) send({
          type: "file_explorer_response",
          payload: { cwd: cwd || request.cwd, path, requestId, mode, directory: null, file: null, error: error instanceof WorkerOperationError ? error.message : "worker_operation_failed" },
        });
      }
    },

    async resolveAttachments(attachments: AgentAttachment[] | undefined, requestSignal?: AbortSignal): Promise<AgentAttachment[] | undefined> {
      if (!attachments?.some((attachment) => attachment.type === "uploaded_file")) return attachments;
      const resolved: AgentAttachment[] = [];
      for (const attachment of attachments) {
        requireActive(requestSignal);
        if (attachment.type !== "uploaded_file") {
          resolved.push(attachment);
          continue;
        }
        const { id, path } = attachment;
        const parsedId = remoteUploadIdSchema.safeParse(id);
        if (!parsedId.success) {
          throw new WorkerOperationError("worker_attachment_unavailable");
        }
        resolved.push(
          await connection.call({ kind: "attachmentResolve", owner, id: parsedId.data, expectedPath: path }),
        );
        requireActive(requestSignal);
      }
      return resolved;
    },
  };
}
