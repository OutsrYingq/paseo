import type { Logger } from "pino";
import type {
  PersistedWorkspaceRecord,
  ProjectRegistry,
  WorkspaceRegistry,
} from "../../workspace-registry.js";
import type { WorkspaceProvisioningService } from "../workspace-provisioning/workspace-provisioning-service.js";

export function createImportWorkspacePlacement({
  workspaceRegistry,
  projectRegistry,
  findOrCreateWorkspaceForDirectory,
  matchesCwd,
  logger,
}: {
  workspaceRegistry: Pick<WorkspaceRegistry, "get" | "listAll" | "listActive" | "remove">;
  projectRegistry: Pick<ProjectRegistry, "get" | "list" | "upsert" | "remove">;
  findOrCreateWorkspaceForDirectory: WorkspaceProvisioningService["findOrCreateWorkspaceForDirectory"];
  matchesCwd(workspaceCwd: string, importCwd: string): boolean | Promise<boolean>;
  logger: Logger;
}): WorkspaceProvisioningService["runInImportWorkspace"] {
  return async ({ cwd, requestedWorkspaceId }, operation) => {
    if (requestedWorkspaceId) {
      const workspace = await workspaceRegistry.get(requestedWorkspaceId);
      if (!workspace || workspace.archivedAt) {
        throw new Error(`Workspace not found: ${requestedWorkspaceId}`);
      }
      const project = await projectRegistry.get(workspace.projectId);
      if (!project || project.archivedAt) {
        throw new Error(`Project not found: ${workspace.projectId}`);
      }
      if (!(await matchesCwd(workspace.cwd, cwd))) {
        throw new Error(`Import cwd does not match workspace: ${workspace.workspaceId}`);
      }
      return { value: await operation(workspace), createdWorkspace: null };
    }

    const [projectsBeforeImport, workspacesBeforeImport] = await Promise.all([
      projectRegistry.list(),
      workspaceRegistry.listAll(),
    ]);
    const workspace = await findOrCreateWorkspaceForDirectory(cwd);
    const createdWorkspace = workspacesBeforeImport.some(
      (candidate) => candidate.workspaceId === workspace.workspaceId,
    )
      ? null
      : workspace;
    const previousProject = projectsBeforeImport.find(
      (project) => project.projectId === workspace.projectId,
    );

    try {
      return { value: await operation(workspace), createdWorkspace };
    } catch (error) {
      if (createdWorkspace) {
        await rollbackCreatedWorkspace(createdWorkspace);
      }
      throw error;
    }

    async function rollbackCreatedWorkspace(workspace: PersistedWorkspaceRecord): Promise<void> {
      try {
        await workspaceRegistry.remove(workspace.workspaceId);
        const projectHasActiveWorkspace = (await workspaceRegistry.listActive()).some(
          (candidate) => candidate.projectId === workspace.projectId && !candidate.archivedAt,
        );
        if (projectHasActiveWorkspace) return;
        if (previousProject?.archivedAt) {
          await projectRegistry.upsert(previousProject);
        } else if (!previousProject) {
          await projectRegistry.remove(workspace.projectId);
        }
      } catch (error) {
        logger.error(
          { err: error, workspaceId: workspace.workspaceId, projectId: workspace.projectId },
          "Failed to restore workspace state after provider import failure",
        );
      }
    }
  };
}
