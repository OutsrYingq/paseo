import { z } from "zod";

export const RemoteOmpImportOperationSchema = z.discriminatedUnion("kind", [
  z.strictObject({
    kind: z.literal("ompImportList"),
    cwd: z.string().optional(),
    limit: z.number().int().positive().max(500).optional(),
    scanLimit: z.number().int().positive().max(500).optional(),
  }),
  z.strictObject({
    kind: z.literal("ompImportResolve"),
    providerHandleId: z.string().min(1),
  }),
]);

export const remoteOmpImportResults = {
  ompImportList: z.array(
    z.strictObject({
      providerHandleId: z.string(),
      cwd: z.string(),
      title: z.string().nullable(),
      firstPromptPreview: z.string().nullable(),
      lastPromptPreview: z.string().nullable(),
      lastActivityAt: z.iso.datetime().transform((timestamp) => new Date(timestamp)),
    }),
  ),
  ompImportResolve: z.strictObject({
    sessionFile: z.string(),
    config: z.strictObject({
      model: z.string().optional(),
      thinkingOptionId: z.string().optional(),
    }),
  }),
};

export type RemoteOmpImportOperation = z.infer<typeof RemoteOmpImportOperationSchema>;
export type RemoteOmpImportPort = {
  [Kind in RemoteOmpImportOperation["kind"]]: (
    operation: Extract<RemoteOmpImportOperation, { kind: Kind }>,
  ) => Promise<z.output<(typeof remoteOmpImportResults)[Kind]>>;
};
