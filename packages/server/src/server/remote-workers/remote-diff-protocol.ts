import {
  CheckoutDiffUpdateSchema,
  DiffSubscribeRequestSchema,
  SubscribeCheckoutDiffRequestSchema,
} from "@getpaseo/protocol/messages";
import { z } from "zod";

const id = z.uuid();
const LegacyDiffSnapshotSchema = CheckoutDiffUpdateSchema.shape.payload.omit({ subscriptionId: true });
export const RemoteDiffOperationSchema = z.discriminatedUnion("kind", [
  z.strictObject({ kind: z.literal("diffOpen"), id, request: DiffSubscribeRequestSchema }),
  z.strictObject({ kind: z.literal("diffStart"), id }),
  z.strictObject({ kind: z.literal("diffAck"), id }),
  z.strictObject({ kind: z.literal("diffClose"), id }),
  z.strictObject({ kind: z.literal("legacyDiffOpen"), id, request: SubscribeCheckoutDiffRequestSchema }),
]);
export const RemoteDiffEventSchema = z.discriminatedUnion("type", [
  z.strictObject({ type: z.literal("diffFrame"), id, bytes: z.base64() }),
  z.strictObject({ type: z.literal("legacyDiffSnapshot"), id, snapshot: LegacyDiffSnapshotSchema }),
]);
export const remoteDiffResults = {
  diffOpen: z.null(),
  diffStart: z.null(),
  diffAck: z.null(),
  diffClose: z.null(),
  legacyDiffOpen: LegacyDiffSnapshotSchema,
};
export type RemoteDiffOperation = z.infer<typeof RemoteDiffOperationSchema>;
export type RemoteDiffEvent = z.infer<typeof RemoteDiffEventSchema>;
