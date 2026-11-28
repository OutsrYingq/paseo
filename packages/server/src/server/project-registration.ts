import { basename } from "node:path";
import type { ProjectCheckoutLitePayload } from "@getpaseo/protocol/messages";
import { deriveProjectKey } from "./project-key.js";
import type { ProjectRegistry } from "./workspace-registry.js";

export function projectRegistrationForCheckout(checkout: ProjectCheckoutLitePayload, serverId?: string) {
  const { cwd: rootPath, remoteUrl, worktreeRoot, mainRepoRoot } = checkout;
  return {
    rootPath,
    kind: checkout.isGit ? ("git" as const) : ("non_git" as const),
    displayName: basename(rootPath) || rootPath,
    projectKey: deriveProjectKey({ rootPath, remoteUrl, worktreeRoot, mainRepoRoot, serverId }),
  };
}

export type ProjectRegistration = ReturnType<typeof projectRegistrationForCheckout>;

export function registerProjectDirectory(projectRegistry: ProjectRegistry, registration: ProjectRegistration) {
  return projectRegistry.getOrCreateActiveByRoot({
    ...registration,
    timestamp: new Date().toISOString(),
  });
}
