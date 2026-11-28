import { FileBeginMetadataSchema } from "@getpaseo/protocol/binary-frames/index";
import { FileUploadRequestSchema, UploadedFileAttachmentSchema } from "@getpaseo/protocol/messages";
import { z } from "zod";

export const MAX_UPLOAD_FRAME_BYTES = 16 * 1024 * 1024;
export const MAX_ACTIVE_FILE_TRANSFERS = 16;
export const remoteUploadOwnerSchema = z.string().regex(/^[a-f0-9]{64}$/);
export const remoteUploadIdSchema = z.templateLiteral(["upload_", z.uuid()]);
const id = z.uuid();
const limit = z.number().int().nonnegative().optional();
const size = z.number().int().nonnegative();

export const RemoteFileTransferOperationSchema = z.discriminatedUnion("kind", [
  z.strictObject({ kind: z.literal("uploadOpen"), id, owner: remoteUploadOwnerSchema, request: FileUploadRequestSchema }),
  z.strictObject({ kind: z.literal("uploadBegin"), id, metadata: FileBeginMetadataSchema }),
  z.strictObject({ kind: z.literal("uploadChunk"), id, bytes: z.base64().max(Math.ceil(MAX_UPLOAD_FRAME_BYTES / 3) * 4) }),
  z.strictObject({ kind: z.literal("uploadEnd"), id }),
  z.strictObject({ kind: z.literal("uploadCancel"), id }),
  z.strictObject({ kind: z.literal("attachmentResolve"), owner: remoteUploadOwnerSchema, id: remoteUploadIdSchema, expectedPath: z.string() }),
  z.strictObject({ kind: z.literal("explorerStreamOpen"), id, cwd: z.string().min(1), path: z.string().min(1), maxBytes: limit }),
  z.strictObject({ kind: z.literal("explorerStreamNext"), id }),
  z.strictObject({ kind: z.literal("explorerStreamClose"), id }),
]);

export const remoteFileTransferResults = {
  uploadOpen: z.null(),
  uploadBegin: z.null(),
  uploadChunk: z.null(),
  uploadEnd: UploadedFileAttachmentSchema.extend({ id: remoteUploadIdSchema }),
  uploadCancel: z.null(),
  attachmentResolve: UploadedFileAttachmentSchema,
  explorerStreamOpen: FileBeginMetadataSchema.extend({ size, revision: z.string() }),
  explorerStreamNext: z.base64().nullable(),
  explorerStreamClose: z.null(),
};

export type RemoteFileTransferOperation = z.infer<typeof RemoteFileTransferOperationSchema>;
