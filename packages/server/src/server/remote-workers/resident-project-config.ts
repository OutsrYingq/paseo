import { lstat } from "node:fs/promises";
import path from "node:path";
import type { Logger } from "pino";
import { resolveAbsoluteGitDir, resolveGitCommonDir } from "../../utils/checkout-git.js";
import { PASEO_CONFIG_FILE_NAME } from "../../utils/paseo-config-file.js";
import { ProjectConfigSession } from "../session/project-config/project-config-session.js";
import type { SessionOutboundMessage } from "../messages.js";
import type { PersistedProjectRecord } from "../workspace-registry.js";
import { WorkerOperationError } from "./execution-protocol.js";
import type { ProjectConfigOperation } from "./remote-project-config-protocol.js";
import { resolveResidentWorkspacePath } from "./resident-projects.js";

export async function executeResidentProjectConfig(
  options: {
    roots: readonly string[];
    listProjects(): Promise<PersistedProjectRecord[]>;
    signal: AbortSignal;
    logger: Logger;
  },
  { request }: ProjectConfigOperation,
) {
  const { roots, listProjects, signal, logger } = options;
  const repoRoot = await resolveResidentWorkspacePath(roots, request.repoRoot);
  const projects = await listProjects();
  const project = projects.find((record) => !record.archivedAt && path.relative(record.rootPath, repoRoot) === "");
  if (project) {
    const config = path.join(repoRoot, PASEO_CONFIG_FILE_NAME);
    const entry = await lstat(config).catch((error: NodeJS.ErrnoException) => {
      if (error.code === "ENOENT") return null;
      throw error;
    });
    if (entry?.isSymbolicLink()) await resolveResidentWorkspacePath([repoRoot], config);
    if (project.kind === "git") {
      const gitDirs = await Promise.all([resolveAbsoluteGitDir(repoRoot), resolveGitCommonDir(repoRoot)]);
      for (const directory of gitDirs) {
        if (directory) await resolveResidentWorkspacePath(roots, directory);
      }
    }
  }
  signal.throwIfAborted();
  let response: SessionOutboundMessage | undefined;
  const session = new ProjectConfigSession({
    host: { emit(message) { response = message; } },
    projectRegistry: { list: async () => project ? [project] : [] },
    logger,
  });
  if (request.type === "read_project_config_request") {
    await session.handleReadProjectConfigRequest({ ...request, repoRoot });
  } else {
    await session.handleWriteProjectConfigRequest({ ...request, repoRoot });
  }
  if (!response) throw new WorkerOperationError("worker_project_config_response_missing");
  return response;
}
