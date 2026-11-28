import * as messages from "@getpaseo/protocol/messages";
import { z } from "zod";

export const CheckoutActionSchema = z.discriminatedUnion("type", [
  messages.CheckoutSwitchBranchRequestSchema,
  messages.CheckoutRenameBranchRequestSchema,
  messages.CheckoutRefreshRequestSchema,
  messages.CheckoutDiscardChangesRequestSchema,
  messages.StashSaveRequestSchema,
  messages.StashPopRequestSchema,
  messages.StashListRequestSchema,
  messages.CheckoutCommitRequestSchema,
  messages.CheckoutMergeRequestSchema,
  messages.CheckoutMergeFromBaseRequestSchema,
  messages.CheckoutPullRequestSchema,
  messages.CheckoutPushRequestSchema,
  messages.CheckoutPrCreateRequestSchema,
  messages.CheckoutPrMergeRequestSchema,
  messages.CheckoutForgeSetAutoMergeRequestSchema,
  messages.CheckoutGithubSetAutoMergeRequestSchema,
  messages.CheckoutPrStatusRequestSchema,
  messages.PullRequestTimelineRequestSchema,
  messages.CheckoutForgeGetCheckDetailsRequestSchema,
  messages.CheckoutGithubGetCheckDetailsRequestSchema,
  messages.ForgeSearchRequestSchema,
  messages.GitHubSearchRequestSchema,
]);

export const CheckoutActionResponseSchema = z.discriminatedUnion("type", [
  messages.CheckoutSwitchBranchResponseSchema,
  messages.CheckoutRenameBranchResponseSchema,
  messages.CheckoutRefreshResponseSchema,
  messages.CheckoutDiscardChangesResponseSchema,
  messages.StashSaveResponseSchema,
  messages.StashPopResponseSchema,
  messages.StashListResponseSchema,
  messages.CheckoutCommitResponseSchema,
  messages.CheckoutMergeResponseSchema,
  messages.CheckoutMergeFromBaseResponseSchema,
  messages.CheckoutPullResponseSchema,
  messages.CheckoutPushResponseSchema,
  messages.CheckoutPrCreateResponseSchema,
  messages.CheckoutPrMergeResponseSchema,
  messages.CheckoutForgeSetAutoMergeResponseSchema,
  messages.CheckoutGithubSetAutoMergeResponseSchema,
  messages.CheckoutPrStatusResponseSchema,
  messages.PullRequestTimelineResponseSchema,
  messages.CheckoutForgeGetCheckDetailsResponseSchema,
  messages.CheckoutGithubGetCheckDetailsResponseSchema,
  messages.ForgeSearchResponseSchema,
  messages.GitHubSearchResponseSchema,
]);

export const CheckoutActionResultSchema = z.object({
  response: CheckoutActionResponseSchema,
  workspaceUpdate: z.boolean(),
  branchSnapshot: z.object({ branchName: z.string().nullable() }).optional(),
});

export type CheckoutAction = z.infer<typeof CheckoutActionSchema>;
export type CheckoutActionResult = z.infer<typeof CheckoutActionResultSchema>;

const actionTypes: ReadonlySet<string> = new Set(
  CheckoutActionSchema.options.map((schema) => schema.shape.type.value),
);

export function isCheckoutAction(message: messages.SessionInboundMessage): message is CheckoutAction {
  return actionTypes.has(message.type);
}
