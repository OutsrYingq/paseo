import {
  BranchSuggestionsRequestSchema,
  BranchSuggestionsResponseSchema,
  CheckoutCommitFileDiffRequestSchema,
  CheckoutCommitFileDiffResponseSchema,
  CheckoutCommitsListRequestSchema,
  CheckoutCommitsListResponseSchema,
  CheckoutDiffGetRequestSchema,
  CheckoutDiffGetResponseSchema,
  CheckoutStatusRequestSchema,
  CheckoutStatusResponseSchema,
  CheckoutWorktreesListRequestSchema,
  CheckoutWorktreesListResponseSchema,
  ValidateBranchRequestSchema,
  ValidateBranchResponseSchema,
  type SessionInboundMessage,
} from "@getpaseo/protocol/messages";
import { z } from "zod";

export const CheckoutQuerySchema = z.discriminatedUnion("type", [
  CheckoutStatusRequestSchema,
  CheckoutCommitsListRequestSchema,
  CheckoutWorktreesListRequestSchema,
  CheckoutCommitFileDiffRequestSchema,
  ValidateBranchRequestSchema,
  BranchSuggestionsRequestSchema,
  CheckoutDiffGetRequestSchema,
]);

export const CheckoutQueryResponseSchema = z.discriminatedUnion("type", [
  CheckoutStatusResponseSchema,
  CheckoutCommitsListResponseSchema,
  CheckoutWorktreesListResponseSchema,
  CheckoutCommitFileDiffResponseSchema,
  ValidateBranchResponseSchema,
  BranchSuggestionsResponseSchema,
  CheckoutDiffGetResponseSchema,
]);

export type CheckoutQuery = z.infer<typeof CheckoutQuerySchema>;

const queryTypes: ReadonlySet<string> = new Set(
  CheckoutQuerySchema.options.map((schema) => schema.shape.type.value),
);

export function isCheckoutQuery(message: SessionInboundMessage): message is CheckoutQuery {
  return queryTypes.has(message.type);
}
