import { lstat, mkdir, open, readdir, readFile, realpath, rename, unlink } from "node:fs/promises";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { WorkerOperationError, type WorkerOperation } from "./execution-protocol.js";

type StateOperation = Extract<WorkerOperation, { path: string }>;
const directories = new Set(["agents", "creations", "agent-requests"]);
const registryFiles = new Set([
  "projects/projects.json",
  "projects/workspaces.json",
  "projects/workspace-labels.json",
  "projects/workspace-labels.transaction.json",
]);
function missing(error: unknown): boolean {
  return (error as NodeJS.ErrnoException).code === "ENOENT";
}

export async function openResidentState(paseoHome: string) {
  const root = await realpath(paseoHome);
  return async function accessState(operation: StateOperation): Promise<unknown> {
    const parts = operation.path.split("/");
    if (
      (!directories.has(parts[0] ?? "") &&
        !((operation.kind === "readState" || operation.kind === "writeState") &&
          registryFiles.has(operation.path)) &&
        !(operation.kind === "deleteState" && operation.path === "projects/workspace-labels.transaction.json")) ||
      parts.length > 3 ||
      parts.some((part) => !part || part === "." || part === ".." || /[\\:\0]/.test(part))
    ) {
      throw new WorkerOperationError("worker_state_path_denied");
    }
    if (
      operation.kind !== "listState" &&
      (parts.length < 2 || !/\.(?:json|claim)$/.test(parts.at(-1) ?? ""))
    ) {
      throw new WorkerOperationError("worker_state_path_denied");
    }
    let target = root;
    for (const part of parts) {
      target = path.join(target, part);
      const entry = await lstat(target).catch((error: unknown) => {
        if (missing(error)) return null;
        throw error;
      });
      if (entry?.isSymbolicLink()) throw new WorkerOperationError("worker_state_path_denied");
    }
    switch (operation.kind) {
      case "listState": {
        try {
          const entries = await readdir(target, { withFileTypes: true });
          return entries.flatMap((entry) =>
            entry.isFile()
              ? [{ name: entry.name, kind: "file" }]
              : entry.isDirectory()
                ? [{ name: entry.name, kind: "directory" }]
                : [],
          );
        } catch (error) {
          if (missing(error)) return [];
          throw error;
        }
      }
      case "readState":
        try {
          return await readFile(target, "utf8");
        } catch (error) {
          if (missing(error)) return null;
          throw error;
        }
      case "deleteState":
        await unlink(target).catch((error: unknown) => {
          if (!missing(error)) throw error;
        });
        return null;
      case "writeState": {
        const dir = path.dirname(target);
        await mkdir(dir, { recursive: true });
        const temporary = operation.exclusive ? target : path.join(dir, `.${randomUUID()}.tmp`);
        const handle = await open(temporary, "wx", 0o600);
        try {
          await handle.writeFile(operation.text);
          await handle.sync();
          await handle.close();
          if (!operation.exclusive) await rename(temporary, target);
          // A pending receipt must survive a process or machine restart before dispatch.
          if (process.platform !== "win32") {
            const directory = await open(dir, "r");
            try {
              await directory.sync();
            } finally {
              await directory.close();
            }
          }
        } finally {
          await handle.close();
          if (!operation.exclusive)
            await unlink(temporary).catch((error: unknown) => {
              if (!missing(error)) throw error;
            });
        }
        return null;
      }
    }
  };
}
