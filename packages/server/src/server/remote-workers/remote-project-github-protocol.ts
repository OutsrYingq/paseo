import {
  ProjectGithubCloneRequestSchema,
  WorkspaceGithubSearchRepositoriesRequestSchema,
  WorkspaceGithubSearchRepositoriesResponseSchema,
} from "@getpaseo/protocol/messages";
import { z } from "zod";

export const RemoteProjectGithubOperationSchema = z.discriminatedUnion("kind", [
  z.strictObject({ kind: z.literal("projectGithubSearch"), request: WorkspaceGithubSearchRepositoriesRequestSchema }),
  z.strictObject({ kind: z.literal("projectGithubClone"), request: ProjectGithubCloneRequestSchema }),
]);

export const remoteProjectGithubResults = {
  projectGithubSearch: WorkspaceGithubSearchRepositoriesResponseSchema,
  projectGithubClone: z.strictObject({
    repo: z.string(),
    checkoutPath: z.string(),
    registration: z.strictObject({ rootPath: z.string(), kind: z.enum(["git", "non_git"]), displayName: z.string(), projectKey: z.string() }),
  }),
};
