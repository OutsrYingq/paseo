import { mkdir, realpath, rmdir, stat } from "node:fs/promises";
import path from "node:path";
import { expandTilde } from "../../utils/path.js";
import { getCheckoutIdentity } from "../../utils/checkout-git.js";
import { isSameOrDescendantPath } from "../path-utils.js";
import { projectRegistrationForCheckout } from "../project-registration.js";
import { checkoutLiteFromGitSnapshot } from "../workspace-registry-model.js";
import { validateDirectoryName } from "../project-directory-service.js";
import { WorkerOperationError } from "./execution-protocol.js";

export async function resolveResidentWorkspacePath(
  roots: readonly string[],
  cwd: string,
): Promise<string> {
  if (!path.isAbsolute(cwd)) throw new WorkerOperationError("worker_directory_must_be_absolute");
  const candidate = await realpath(cwd);
  for (const root of roots) {
    if (isSameOrDescendantPath(await realpath(root), candidate)) return candidate;
  }
  throw new WorkerOperationError("worker_workspace_root_denied");
}

export async function resolveProjectParent(roots: readonly string[], parentPath: string) {
  try {
    const parent = await resolveResidentWorkspacePath(roots, expandTilde(parentPath));
    if (!(await stat(parent)).isDirectory()) throw new WorkerOperationError("ENOTDIR");
    return parent;
  } catch (error) {
    if (error instanceof WorkerOperationError) throw error;
    const code = (error as NodeJS.ErrnoException).code;
    if (code) throw new WorkerOperationError(code);
    throw error;
  }
}

export async function changeProjectDirectory(
  roots: readonly string[],
  input: { parentPath: string; name: string; action: "create" | "rollback" },
) {
  validateDirectoryName(input.name);
  const parent = await resolveProjectParent(roots, input.parentPath);
  const directoryPath = path.join(parent, input.name);
  try {
    if (input.action === "create") await mkdir(directoryPath);
    else await rmdir(directoryPath);
    return null;
  } catch (error) {
    const code = (error as NodeJS.ErrnoException).code;
    if (code) throw new WorkerOperationError(code);
    throw error;
  }
}

export async function inspectResidentProject(options: {
  roots: readonly string[];
  cwd: string;
  paseoHome: string;
  serverId: string;
}) {
  const cwd = await resolveResidentWorkspacePath(options.roots, expandTilde(options.cwd)).catch(
    (error: unknown) => {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") return null;
      throw error;
    },
  );
  if (!cwd || !(await stat(cwd)).isDirectory()) return null;
  const identity = await getCheckoutIdentity(cwd, { paseoHome: options.paseoHome });
  const checkout = checkoutLiteFromGitSnapshot(cwd, identity.isGit ? identity : {
    isGit: false,
    currentBranch: null,
    remoteUrl: null,
    repoRoot: null,
    mainRepoRoot: null,
    isPaseoOwnedWorktree: false,
  });
  return projectRegistrationForCheckout(checkout, options.serverId);
}
