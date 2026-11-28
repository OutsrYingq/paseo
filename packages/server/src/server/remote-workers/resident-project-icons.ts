import {
  readProjectIconSnapshot,
  removeProjectCustomIcon,
  setProjectCustomIcon,
} from "../../utils/project-custom-icon.js";
import type { PersistedProjectRecord } from "../workspace-registry.js";
import { WorkerOperationError } from "./execution-protocol.js";
import type { RemoteProjectIconOperation } from "./remote-project-icon-protocol.js";
import { resolveResidentWorkspacePath } from "./resident-projects.js";

export async function executeResidentProjectIcon(
  options: {
    roots: readonly string[];
    paseoHome: string;
    listProjects(): Promise<PersistedProjectRecord[]>;
    signal: AbortSignal;
  },
  operation: RemoteProjectIconOperation,
) {
  const { roots, paseoHome, listProjects, signal } = options;
  const { projectId } = operation;
  if (operation.kind === "projectIconRemove") {
    signal.throwIfAborted();
    await removeProjectCustomIcon({ paseoHome, projectId });
    return null;
  }
  const project = (await listProjects()).find((record) => record.projectId === projectId && !record.archivedAt);
  if (!project) throw new WorkerOperationError("worker_project_not_found");
  const rootPath = await resolveResidentWorkspacePath(roots, project.rootPath);
  signal.throwIfAborted();
  if (operation.kind === "projectIconGet") {
    const { icon } = await readProjectIconSnapshot({
      paseoHome,
      project: { ...project, rootPath },
      confineToProject: true,
    });
    return icon;
  }
  const updated = await setProjectCustomIcon({
    paseoHome,
    projectId,
    source: operation.source,
    projects: {
      get: async (id) => id === projectId ? project : null,
      update: async (id, update) => id === projectId ? update(project) : null,
    },
  });
  return updated.customIconRevision ?? null;
}
