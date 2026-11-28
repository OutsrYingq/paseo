import type { WorkspaceProjectDescriptorPayload } from "@getpaseo/protocol/messages";
import { resolveProjectDisplayName, type PersistedProjectRecord } from "./workspace-registry.js";

export function describeProject(
  project: PersistedProjectRecord,
  iconRevision?: WorkspaceProjectDescriptorPayload["projectIconRevision"],
): WorkspaceProjectDescriptorPayload {
  return {
    projectId: project.projectId,
    ...(project.projectKey ? { projectKey: project.projectKey } : {}),
    projectDisplayName: resolveProjectDisplayName(project),
    projectCustomName: project.customName ?? null,
    projectCustomIconRevision: project.customIconRevision ?? null,
    ...(iconRevision !== undefined ? { projectIconRevision: iconRevision } : {}),
    projectRootPath: project.rootPath,
    projectKind: project.kind,
  };
}
