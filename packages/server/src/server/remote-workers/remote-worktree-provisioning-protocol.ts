import {
  ChangeRequestCheckoutSourceSchema,
  WorkspaceSetupSnapshotSchema,
} from "@getpaseo/protocol/messages";
import { z } from "zod";

const worktreeMaterialize = z.strictObject({
  kind: z.literal("worktreeMaterialize"),
  cwd: z.string().min(1),
  worktreeSlug: z.string().optional(),
  branchName: z.string().optional(),
  refName: z.string().optional(),
  action: z.enum(["branch-off", "checkout"]).optional(),
  checkoutSource: ChangeRequestCheckoutSourceSchema.strict().optional(),
  githubPrNumber: z.number().int().positive().optional(),
});
const worktreeStartSetup = z.strictObject({
  kind: z.literal("worktreeStartSetup"),
  workspaceId: z.string().min(1),
});
const worktreeApproveSetup = z.strictObject({
  kind: z.literal("worktreeApproveSetup"),
  workspaceId: z.string().min(1),
});

export const RemoteWorktreeProvisioningOperationSchema = z.discriminatedUnion("kind", [
  worktreeMaterialize,
  worktreeStartSetup,
  worktreeApproveSetup,
]);

export const RemoteWorktreeMaterializationSchema = z.union([
  z.strictObject({
    sourceCwd: z.string(),
    repoRoot: z.string(),
    cwd: z.string(),
    worktreeRoot: z.string(),
    branch: z.string(),
    baseBranch: z.string().nullable(),
    untrustedSource: WorkspaceSetupSnapshotSchema.shape.blockedSource,
  }),
  z.strictObject({
    error: z.strictObject({
      code: z.enum([
        "branch_already_checked_out",
        "missing_checkout_target",
        "unknown_branch",
        "unknown",
      ]),
      message: z.string(),
    }),
  }),
]);

export const RemoteWorktreeSetupEventSchema = z.strictObject({
  type: z.literal("worktreeSetup"),
  payload: WorkspaceSetupSnapshotSchema.extend({ workspaceId: z.string().min(1) }),
});

export const remoteWorktreeProvisioningResults = {
  worktreeMaterialize: RemoteWorktreeMaterializationSchema,
  worktreeStartSetup: z.null(),
  worktreeApproveSetup: z.strictObject({
    clearedMetadata: z.boolean(),
    blockedWorkspaceIds: z.array(z.string()),
  }),
};

export type RemoteWorktreeProvisioningOperation = z.infer<
  typeof RemoteWorktreeProvisioningOperationSchema
>;
export type RemoteWorktreeMaterialization = z.infer<typeof RemoteWorktreeMaterializationSchema>;
export type RemoteWorktreeSetupEvent = z.infer<typeof RemoteWorktreeSetupEventSchema>;
