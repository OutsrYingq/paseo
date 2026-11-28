import {
  ReadProjectConfigRequestMessageSchema,
  ReadProjectConfigResponseMessageSchema,
  WriteProjectConfigRequestMessageSchema,
  WriteProjectConfigResponseMessageSchema,
} from "@getpaseo/protocol/messages";
import { z } from "zod";

export const ProjectConfigOperationSchema = z.discriminatedUnion("kind", [
  z.strictObject({ kind: z.literal("projectConfigRead"), request: ReadProjectConfigRequestMessageSchema }),
  z.strictObject({ kind: z.literal("projectConfigWrite"), request: WriteProjectConfigRequestMessageSchema }),
]);

export type ProjectConfigOperation = z.infer<typeof ProjectConfigOperationSchema>;
export const projectConfigResults = {
  projectConfigRead: ReadProjectConfigResponseMessageSchema,
  projectConfigWrite: WriteProjectConfigResponseMessageSchema,
};
