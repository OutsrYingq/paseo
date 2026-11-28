import { ProjectIconGetResponseSchema, ProjectIconSourceSchema } from "@getpaseo/protocol/messages";
import { z } from "zod";

const projectId = z.string().min(1);
export const RemoteProjectIconOperationSchema = z.discriminatedUnion("kind", [
  z.strictObject({ kind: z.literal("projectIconGet"), projectId }),
  z.strictObject({ kind: z.literal("projectIconSet"), projectId, source: ProjectIconSourceSchema }),
  z.strictObject({ kind: z.literal("projectIconRemove"), projectId }),
]);

export type RemoteProjectIconOperation = z.infer<typeof RemoteProjectIconOperationSchema>;
export const remoteProjectIconResults = {
  projectIconGet: ProjectIconGetResponseSchema.shape.payload.shape.icon,
  projectIconSet: z.string().nullable(),
  projectIconRemove: z.null(),
};
