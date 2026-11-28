import {
  ProjectCheckoutLitePayloadSchema,
  WorkspaceSetupSnapshotSchema,
} from "@getpaseo/protocol/messages";
import { z } from "zod";

const workspaceId = z.string().min(1);
const archivedAt = z.iso.datetime();

export const RemoteWorkspaceArchiveOperationSchema = z.discriminatedUnion("kind", [
  z.strictObject({ kind: z.literal("workspaceArchivePrepare"), workspaceId }),
  z.strictObject({ kind: z.literal("workspaceArchiveRemove"), workspaceId, archivedAt }),
  z.strictObject({ kind: z.literal("workspaceRecoveryInspect"), workspaceId }),
  z.strictObject({ kind: z.literal("workspaceRecoveryRestore"), workspaceId, archivedAt }),
]);

export const remoteWorkspaceArchiveResults = {
  workspaceArchivePrepare: z.strictObject({ head: z.string().nullable() }),
  workspaceArchiveRemove: z.strictObject({
    removedDirectory: z.boolean(),
    directoryLeftBehind: z.strictObject({ path: z.string(), reason: z.string() }).optional(),
  }),
  workspaceRecoveryInspect: z.discriminatedUnion("kind", [
    z.strictObject({
      kind: z.literal("recoverable"),
      workspaceId,
      workspaceName: z.string(),
      action: z.enum(["unarchive", "restore"]),
      branch: z.string().nullable(),
    }),
    z.strictObject({
      kind: z.literal("unavailable"),
      workspaceId,
      reason: z.enum([
        "workspace_not_found",
        "workspace_not_archived",
        "project_not_found",
        "project_directory_missing",
        "workspace_directory_missing",
        "worktree_branch_missing",
      ]),
      message: z.string(),
    }),
  ]),
  workspaceRecoveryRestore: z.strictObject({
    action: z.enum(["unarchive", "restore"]),
    checkout: ProjectCheckoutLitePayloadSchema,
    untrustedSource: WorkspaceSetupSnapshotSchema.shape.blockedSource,
    worktreeManagerPluginId: z.string().nullable(),
  }),
};

export type RemoteWorkspaceArchiveOperation = z.infer<typeof RemoteWorkspaceArchiveOperationSchema>;
export type RemoteWorkspaceArchiveResult<K extends RemoteWorkspaceArchiveOperation["kind"]> =
  z.infer<(typeof remoteWorkspaceArchiveResults)[K]>;
