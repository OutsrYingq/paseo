import {
  FileEntryCreateRequestSchema,
  FileEntryCreateResponseSchema,
  FileEntryDeleteRequestSchema,
  FileEntryDeleteResponseSchema,
  FileEntryDuplicateRequestSchema,
  FileEntryDuplicateResponseSchema,
  FileEntryRenameRequestSchema,
  FileEntryRenameResponseSchema,
  FileWriteRequestSchema,
  FileWriteResponseSchema,
} from "@getpaseo/protocol/messages";
import { z } from "zod";
import {
  createExplorerEntry,
  deleteExplorerEntry,
  duplicateExplorerEntry,
  renameExplorerEntry,
  writeExplorerFile,
} from "../../file-explorer/service.js";

export const WorkspaceFileMutationSchema = z.discriminatedUnion("type", [
  FileWriteRequestSchema,
  FileEntryCreateRequestSchema,
  FileEntryRenameRequestSchema,
  FileEntryDuplicateRequestSchema,
  FileEntryDeleteRequestSchema,
]);

export const WorkspaceFileMutationResponseSchema = z.discriminatedUnion("type", [
  FileWriteResponseSchema,
  FileEntryCreateResponseSchema,
  FileEntryRenameResponseSchema,
  FileEntryDuplicateResponseSchema,
  FileEntryDeleteResponseSchema,
]);

export type WorkspaceFileMutation = z.infer<typeof WorkspaceFileMutationSchema>;
type Response = z.infer<typeof WorkspaceFileMutationResponseSchema>;

const mutationTypes: ReadonlySet<string> = new Set(
  WorkspaceFileMutationSchema.options.map((schema) => schema.shape.type.value),
);

export function isWorkspaceFileMutation<T extends { type: string }>(
  message: T,
): message is Extract<T, { type: WorkspaceFileMutation["type"] }> {
  return mutationTypes.has(message.type);
}

export async function mutateWorkspaceFile(
  request: WorkspaceFileMutation,
  root = request.cwd,
): Promise<Response> {
  const { cwd, requestId } = request;
  switch (request.type) {
    case "fs.file.write.request": {
      const result = await writeExplorerFile({
        root,
        relativePath: request.path,
        content: request.content,
        contentEncoding: request.contentEncoding,
        expectedModifiedAt: request.expectedModifiedAt,
        expectedRevision: request.expectedRevision,
      });
      return { type: "fs.file.write.response", payload: { result, requestId } };
    }
    case "fs.entry.create.request": {
      const { parentPath, name, kind } = request;
      const result = await createExplorerEntry({ root, parentPath, name, kind });
      return {
        type: "fs.entry.create.response",
        payload: {
          cwd,
          requestId,
          parentPath,
          path: result.status === "ok" ? result.path : null,
          success: result.status === "ok",
          error: result.status === "ok" ? null : result.error,
        },
      };
    }
    case "fs.entry.rename.request": {
      const { path, name } = request;
      const result = await renameExplorerEntry({ root, relativePath: path, name });
      return {
        type: "fs.entry.rename.response",
        payload: {
          cwd,
          requestId,
          path,
          renamedPath: result.status === "ok" ? result.path : null,
          success: result.status === "ok",
          error: result.status === "ok" ? null : result.error,
        },
      };
    }
    case "fs.entry.duplicate.request": {
      const { path } = request;
      const result = await duplicateExplorerEntry({ root, relativePath: path });
      return {
        type: "fs.entry.duplicate.response",
        payload: {
          cwd,
          requestId,
          path,
          duplicatedPath: result.status === "ok" ? result.path : null,
          success: result.status === "ok",
          error: result.status === "ok" ? null : result.error,
        },
      };
    }
    case "fs.entry.delete.request": {
      const { path } = request;
      const result = await deleteExplorerEntry({ root, relativePath: path });
      return {
        type: "fs.entry.delete.response",
        payload: {
          cwd,
          requestId,
          path,
          success: result.status === "ok",
          error: result.status === "ok" ? null : result.error,
        },
      };
    }
  }
}
