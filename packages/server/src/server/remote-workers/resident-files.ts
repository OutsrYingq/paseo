import { constants } from "node:fs";
import { open, realpath, type FileHandle } from "node:fs/promises";
import { randomUUID } from "node:crypto";
import path from "node:path";
import {
  getDownloadableFileInfo,
  listDirectoryEntries,
  readExplorerFile,
} from "../file-explorer/service.js";
import { WorkerOperationError, type WorkerOperation } from "./execution-protocol.js";

type FileOperation = Extract<
  WorkerOperation,
  {
    kind: "explore" | "downloadInfo" | "downloadOpen" | "downloadRead" | "downloadClose";
  }
>;
export const DOWNLOAD_CHUNK_BYTES = 256 * 1024;

export function openResidentFiles(roots: readonly string[]) {
  const handles = new Map<string, FileHandle>();
  let closed = false;
  async function scopedRoot(cwd: string): Promise<string> {
    const candidate = await realpath(cwd);
    for (const root of roots) {
      const relative = path.relative(await realpath(root), candidate);
      if (!path.isAbsolute(relative) && relative !== ".." && !relative.startsWith(`..${path.sep}`))
        return candidate;
    }
    throw new WorkerOperationError("worker_workspace_root_denied");
  }
  return {
    async execute(operation: FileOperation) {
      if (closed) throw new WorkerOperationError("worker_connection_lost");
      switch (operation.kind) {
        case "explore": {
          const root = await scopedRoot(operation.cwd);
          const relativePath = operation.path;
          return operation.mode === "list"
            ? {
                directory: await listDirectoryEntries({ root, relativePath }),
                file: null,
              }
            : {
                directory: null,
                file: await readExplorerFile({ root, relativePath }),
              };
        }
        case "downloadInfo": {
          const root = await scopedRoot(operation.cwd);
          const { fileName, mimeType, size } = await getDownloadableFileInfo({
            root,
            relativePath: operation.path,
          });
          return { fileName, mimeType, size };
        }
        case "downloadOpen": {
          const root = await scopedRoot(operation.cwd);
          const info = await getDownloadableFileInfo({
            root,
            relativePath: operation.path,
          });
          if (handles.size >= 16) throw new WorkerOperationError("worker_download_limit");
          const handle = await open(
            info.absolutePath,
            constants.O_RDONLY | (constants.O_NOFOLLOW ?? 0),
          );
          try {
            const stats = await handle.stat();
            if (closed || !stats.isFile() || handles.size >= 16)
              throw new WorkerOperationError("worker_download_unavailable");
            const id = randomUUID();
            handles.set(id, handle);
            return { id, size: stats.size };
          } catch (error) {
            await handle.close();
            throw error;
          }
        }
        case "downloadRead": {
          const handle = handles.get(operation.id);
          if (!handle) throw new WorkerOperationError("worker_download_unavailable");
          const buffer = Buffer.alloc(DOWNLOAD_CHUNK_BYTES);
          const { bytesRead } = await handle.read(buffer, 0, buffer.length, operation.offset);
          return buffer.subarray(0, bytesRead).toString("base64");
        }
        case "downloadClose": {
          const handle = handles.get(operation.id);
          handles.delete(operation.id);
          await handle?.close();
          return null;
        }
      }
    },
    async close() {
      closed = true;
      await Promise.allSettled([...handles.values()].map((handle) => handle.close()));
      handles.clear();
    },
  };
}
