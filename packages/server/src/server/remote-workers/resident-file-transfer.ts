import { lstat, mkdir, readFile, realpath, rm, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { FileTransferOpcode, type FileBegin } from "@getpaseo/protocol/binary-frames/index";
import { UploadedFileAttachmentSchema, type UploadedFileAttachment } from "@getpaseo/protocol/messages";
import { FileUploadStore } from "../file-upload/index.js";
import { getDownloadableFileInfo, streamExplorerFile } from "../file-explorer/service.js";
import { WorkerOperationError } from "./execution-protocol.js";
import {
  MAX_ACTIVE_FILE_TRANSFERS,
  MAX_UPLOAD_FRAME_BYTES,
  type RemoteFileTransferOperation,
} from "./remote-file-transfer-protocol.js";
import { resolveResidentWorkspacePath } from "./resident-projects.js";

interface Upload {
  cancel?: () => Promise<void>;
  work?: Promise<unknown>;
  stage: "opening" | "open" | "begun" | "cancelled";
  ownerHome?: string;
  store?: FileUploadStore;
}

interface ExplorerStream {
  release(): void;
  task: Promise<void>;
  iterator?: AsyncIterator<Uint8Array>;
  read?: Promise<IteratorResult<Uint8Array>>;
}

export function openResidentFileTransfer(options: { roots: readonly string[]; paseoHome: string }) {
  const uploads = new Map<string, Upload>();
  const streams = new Map<string, ExplorerStream>();
  let closed = false;

  async function ownerHome(owner: string, create: boolean): Promise<string> {
    let directory = await realpath(options.paseoHome);
    for (const component of ["remote-attachments", owner, "uploads"]) {
      directory = join(directory, component);
      if (create) {
        try {
          await mkdir(directory, { mode: 0o700 });
        } catch (error) {
          if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
        }
      }
      if (!(await lstat(directory)).isDirectory()) throw new WorkerOperationError("worker_attachment_unavailable");
    }
    return dirname(directory);
  }

  async function stopUpload(id: string): Promise<void> {
    const upload = uploads.get(id);
    if (!upload) return;
    uploads.delete(id);
    upload.stage = "cancelled";
    await upload.cancel?.();
    await upload.work?.catch(() => undefined);
  }

  async function stopStream(id: string): Promise<void> {
    const stream = streams.get(id);
    if (!stream) return;
    streams.delete(id);
    // A pending read owns the handle until its revision check completes.
    try {
      await stream.read;
    } finally {
      stream.release();
      await stream.task;
    }
  }

  const uploadFailure = () => new WorkerOperationError("worker_upload_unavailable");
  const tooLarge = () => new WorkerOperationError("File is too large to display");

  async function writeUploadFrame(
    upload: Upload,
    operation: Extract<RemoteFileTransferOperation, { kind: "uploadBegin" | "uploadChunk" | "uploadEnd" }>,
  ) {
    const { id } = operation;
    const store = upload.store;
    const home = upload.ownerHome;
    if (!store || !home) throw uploadFailure();
    if (operation.kind === "uploadBegin") {
      if (upload.stage !== "open") throw uploadFailure();
      upload.stage = "begun";
      const response = await store.receiveFrame(
        { opcode: FileTransferOpcode.FileBegin, requestId: id, metadata: operation.metadata, payload: new Uint8Array() },
        upload,
      );
      if (response) throw uploadFailure();
      return null;
    }
    if (upload.stage !== "begun") throw uploadFailure();
    if (operation.kind === "uploadChunk") {
      const payload = Buffer.from(operation.bytes, "base64");
      if (payload.byteLength > MAX_UPLOAD_FRAME_BYTES) throw uploadFailure();
      const response = await store.receiveFrame({ opcode: FileTransferOpcode.FileChunk, requestId: id, payload }, upload);
      if (response) throw uploadFailure();
      return null;
    }
    const response = await store.receiveFrame({ opcode: FileTransferOpcode.FileEnd, requestId: id, payload: new Uint8Array() }, upload);
    const file = response?.payload.file;
    if (!file) throw uploadFailure();
    const { path, ...metadata } = file;
    const indexPath = join(home, `${file.id}.json`);
    try {
      if (uploads.get(id) !== upload || closed) throw uploadFailure();
      await writeFile(indexPath, JSON.stringify(metadata), { flag: "wx", mode: 0o600 });
      if (uploads.get(id) !== upload || closed) throw uploadFailure();
    } catch (error) {
      await Promise.all([rm(indexPath, { force: true }), rm(dirname(path), { recursive: true, force: true })]);
      throw error;
    }
    uploads.delete(id);
    return file;
  }

  return {
    async execute(operation: RemoteFileTransferOperation) {
      if (closed) throw new WorkerOperationError("worker_connection_lost");
      switch (operation.kind) {
        case "uploadOpen": {
          const { id, owner, request } = operation;
          if (uploads.has(id) || uploads.size >= MAX_ACTIVE_FILE_TRANSFERS) throw uploadFailure();
          const upload: Upload = { stage: "opening" };
          uploads.set(id, upload);
          const open = async () => {
            const home = await ownerHome(owner, true);
            if (closed || uploads.get(id) !== upload) throw uploadFailure();
            const store = new FileUploadStore({ paseoHome: home });
            upload.cancel = store.beginUpload({ ...request, requestId: id }, upload, (response) => {
              if (response === null && uploads.get(id) === upload) uploads.delete(id);
            });
            upload.store = store;
            upload.ownerHome = home;
            upload.stage = "open";
          };
          upload.work = open();
          try {
            await upload.work;
          } catch (error) {
            uploads.delete(id);
            throw error;
          } finally {
            upload.work = undefined;
          }
          return null;
        }
        case "uploadBegin":
        case "uploadChunk":
        case "uploadEnd": {
          const upload = uploads.get(operation.id);
          if (!upload || upload.work) throw uploadFailure();
          const work = writeUploadFrame(upload, operation);
          upload.work = work;
          try {
            return await work;
          } catch (error) {
            upload.work = undefined;
            if (uploads.get(operation.id) === upload) await stopUpload(operation.id);
            throw error;
          } finally {
            upload.work = undefined;
          }
        }
        case "uploadCancel":
          await stopUpload(operation.id);
          return null;
        case "attachmentResolve": {
          try {
            const home = await ownerHome(operation.owner, false);
            const index = join(home, `${operation.id}.json`);
            if (!(await lstat(index)).isFile()) throw uploadFailure();
            const file = UploadedFileAttachmentSchema.omit({ path: true }).parse(JSON.parse(await readFile(index, "utf8")));
            if (file.id !== operation.id || !file.fileName || /[\\/:]/u.test(file.fileName) || [".", ".."].includes(file.fileName)) throw uploadFailure();
            const directory = join(home, "uploads", file.id);
            const path = join(directory, file.fileName);
            if (operation.expectedPath !== path) throw uploadFailure();
            if (!(await lstat(directory)).isDirectory()) throw uploadFailure();
            const stats = await lstat(path);
            if (!stats.isFile() || stats.size !== file.size) throw uploadFailure();
            return { ...file, path } satisfies UploadedFileAttachment;
          } catch {
            throw new WorkerOperationError("worker_attachment_unavailable");
          }
        }
        case "explorerStreamOpen": {
          const { id, cwd, path, maxBytes } = operation;
          if (streams.has(id) || streams.size >= MAX_ACTIVE_FILE_TRANSFERS) throw new WorkerOperationError("worker_file_stream_limit");
          let resolveMetadata!: (value: FileBegin["metadata"]) => void;
          let rejectMetadata!: (error: unknown) => void;
          const metadata = new Promise<FileBegin["metadata"]>((resolve, reject) => {
            resolveMetadata = resolve;
            rejectMetadata = reject;
          });
          let release!: () => void;
          const done = new Promise<void>((resolve) => { release = resolve; });
          const stream: ExplorerStream = { release, task: Promise.resolve() };
          streams.set(id, stream);
          stream.task = (async () => {
            const root = await resolveResidentWorkspacePath(options.roots, cwd);
            const target = { root, relativePath: path };
            if (maxBytes !== undefined && (await getDownloadableFileInfo(target)).size > maxBytes) throw tooLarge();
            if (streams.get(id) !== stream) throw new WorkerOperationError("worker_file_stream_unavailable");
            await streamExplorerFile(target, async (file) => {
              if (maxBytes !== undefined && file.size > maxBytes) throw tooLarge();
              if (streams.get(id) !== stream) throw new WorkerOperationError("worker_file_stream_unavailable");
              stream.iterator = file.chunks[Symbol.asyncIterator]();
              resolveMetadata({ mime: file.mimeType, size: file.size, encoding: file.encoding, modifiedAt: file.modifiedAt, revision: file.revision });
              await done;
            });
          })().catch(rejectMetadata);
          try {
            return await metadata;
          } catch (error) {
            if (streams.get(id) === stream) await stopStream(id);
            throw error;
          }
        }
        case "explorerStreamNext": {
          const stream = streams.get(operation.id);
          if (!stream?.iterator || stream.read) throw new WorkerOperationError("worker_file_stream_unavailable");
          const read = stream.iterator.next();
          stream.read = read;
          try {
            const chunk = await read;
            if (chunk.done) {
              await stopStream(operation.id);
              return null;
            }
            return Buffer.from(chunk.value.buffer, chunk.value.byteOffset, chunk.value.byteLength).toString("base64");
          } finally {
            stream.read = undefined;
          }
        }
        case "explorerStreamClose":
          await stopStream(operation.id);
          return null;
      }
    },
    async close() {
      closed = true;
      await Promise.allSettled([
        ...[...uploads.keys()].map(stopUpload),
        ...[...streams.keys()].map(stopStream),
      ]);
    },
  };
}
