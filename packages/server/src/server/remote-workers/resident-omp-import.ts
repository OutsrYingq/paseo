import path from "node:path";
import { realpath, stat } from "node:fs/promises";
import { isPathInsideRoot } from "../../utils/path.js";
import {
  listOmpImportableSessions,
  readOmpImportSessionConfig,
  resolveOmpSessionFile,
} from "../agent/providers/omp/session-descriptor.js";
import { WorkerOperationError } from "./execution-protocol.js";
import type { RemoteOmpImportOperation } from "./remote-omp-import-protocol.js";

/** Use the resident's configured OMP session directory for both discovery and import. */
export function createResidentOmpImport(resolveSessionsDirectory: () => Promise<string>) {
  return async (operation: RemoteOmpImportOperation) => {
    const directory = await realpath(await resolveSessionsDirectory()).catch(() => null);
    if (operation.kind === "ompImportList") {
      return directory
        ? listOmpImportableSessions({
            sessionDir: directory,
            cwd: operation.cwd,
            limit: operation.limit,
            scanLimit: operation.scanLimit,
          })
        : [];
    }
    const unavailable = () => new WorkerOperationError("worker_omp_import_session_unavailable");
    if (!directory) throw unavailable();

    const handle = operation.providerHandleId;
    let file: string | null = null;
    if (path.isAbsolute(handle)) {
      if (path.normalize(handle) === handle) file = handle;
    } else if (/^[\w.-]+$/.test(handle) && handle !== "." && handle !== "..") {
      file = await resolveOmpSessionFile(handle, { sessionDir: directory });
    }

    if (
      !file ||
      !path.isAbsolute(file) ||
      !isPathInsideRoot(directory, file) ||
      path.extname(file) !== ".jsonl"
    )
      throw unavailable();
    // Discovery does not follow symlinks. Require the same concrete file for direct imports.
    if ((await realpath(file).catch(() => null)) !== file) throw unavailable();
    if (!(await stat(file).catch(() => null))?.isFile()) throw unavailable();
    return {
      sessionFile: file,
      config: await readOmpImportSessionConfig(file, { sessionDir: directory }),
    };
  };
}
