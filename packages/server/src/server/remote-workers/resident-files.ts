import { constants } from "node:fs";
import { open, type FileHandle } from "node:fs/promises";
import { randomUUID } from "node:crypto";
import {
  getDownloadableFileInfo,
  listDirectoryEntries,
  readExplorerFile,
} from "../file-explorer/service.js";
import { WorkerOperationError, type WorkerOperation, type WorkerEvent } from "./execution-protocol.js";
import { mutateWorkspaceFile } from "../session/files/workspace-file-mutations.js";
import { resolveResidentWorkspacePath } from "./resident-projects.js";
import { workspaceFileObserver } from "../file-explorer/observer.js";

type FileOperation = Extract<
  WorkerOperation,
  {
    kind:
      | "explore"
      | "mutateWorkspaceFile"
      | "observeWorkspaceFile"
      | "unobserveWorkspaceFile"
      | "downloadInfo"
      | "downloadOpen"
      | "downloadRead"
      | "downloadClose";
  }
>;
export const DOWNLOAD_CHUNK_BYTES = 256 * 1024;

export function openResidentFiles(
  roots: readonly string[],
  publish: (event: Extract<WorkerEvent, { type: "fileVersion" }>) => void,
) {
  const handles = new Map<string, FileHandle>();
  const observations = new Map<string, () => void>();
  let closed = false;
  const scopedRoot = (cwd: string) => resolveResidentWorkspacePath(roots, cwd);
  return {
    async execute(operation: FileOperation) {
      if (closed) throw new WorkerOperationError("worker_connection_lost");
      switch (operation.kind) {
        case "observeWorkspaceFile": {
          const { id, cwd, path } = operation;
          if (observations.has(id)) throw new WorkerOperationError("invalid_request");
          const observation = await workspaceFileObserver.subscribe(
            { cwd: await scopedRoot(cwd), path },
            (version) => publish({ type: "fileVersion", id, version }),
          );
          if (closed || observations.has(id)) {
            observation.unsubscribe();
            throw new WorkerOperationError("worker_file_observation_unavailable");
          }
          observations.set(id, observation.unsubscribe);
          return observation.initial;
        }
        case "unobserveWorkspaceFile":
          observations.get(operation.id)?.();
          observations.delete(operation.id);
          return null;
        case "mutateWorkspaceFile":
          return mutateWorkspaceFile(operation.request, await scopedRoot(operation.request.cwd));
        case "explore": {
          const root = await scopedRoot(operation.cwd);
          const relativePath = operation.path;
          if (
            operation.mode === "file" &&
            operation.maxBytes !== undefined &&
            (await getDownloadableFileInfo({ root, relativePath })).size > operation.maxBytes
          ) {
            throw new WorkerOperationError("File is too large to display");
          }
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
      for (const stop of observations.values()) stop();
      observations.clear();
      await Promise.allSettled([...handles.values()].map((handle) => handle.close()));
      handles.clear();
    },
  };
}
