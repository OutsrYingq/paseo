import { mkdir, realpath, rmdir, stat } from "node:fs/promises";
import { homedir } from "node:os";
import path from "node:path";
import type { DirectorySuggestionsRequest, DirectorySuggestionsResponse } from "@getpaseo/protocol/messages";
import {
  searchDirectoryEntries,
  WORKSPACE_SEARCH_HIDDEN_DIRECTORIES,
  type DirectorySuggestionEntry,
} from "../../utils/directory-suggestions.js";
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

export async function suggestResidentDirectories(
  roots: readonly string[],
  request: DirectorySuggestionsRequest,
): Promise<DirectorySuggestionsResponse> {
  const { cwd, requestId, includeFiles, includeDirectories, matchMode, limit = 20 } = request;
  try {
    const workspace = cwd?.trim();
    const query = workspace
      ? request.query
      : request.query.replace(/^~(?=$|[\\/])/u, () => homedir());
    const entries = new Map<string, DirectorySuggestionEntry>();
    const searchRoots = workspace ? [await resolveProjectParent(roots, workspace)] : roots;
    for (const root of searchRoots) {
      const matches = await searchDirectoryEntries({
        root,
        query,
        pathFormat: workspace ? "relative" : "absolute",
        pathQueryPolicy: workspace ? "slashes" : "rooted",
        blankQueryBehavior: workspace ? "children" : "none",
        traversableHiddenDirectoryNames: workspace ? WORKSPACE_SEARCH_HIDDEN_DIRECTORIES : [],
        confidentResultScanThreshold: workspace ? undefined : 5_000,
        respectGitIgnore: Boolean(workspace),
        includeFiles,
        includeDirectories,
        matchMode,
        limit,
      });
      for (const entry of matches) {
        entries.set(entry.path, entry);
        if (entries.size === limit) break;
      }
      if (entries.size === limit) break;
    }
    const candidates = [...entries.values()];
    const directories = candidates.filter((entry) => entry.kind === "directory").map((entry) => entry.path);
    return { type: "directory_suggestions_response", payload: { requestId, directories, entries: candidates, error: null } };
  } catch (error) {
    return {
      type: "directory_suggestions_response",
      payload: { requestId, directories: [], entries: [], error: error instanceof Error ? error.message : String(error) },
    };
  }
}

export async function resolveResidentProjectSuggestion(
  roots: readonly string[],
  cwd: string,
  paseoHome: string,
): Promise<string | null> {
  if (!path.isAbsolute(cwd)) return null;
  try {
    const directory = await resolveProjectParent(roots, cwd);
    const checkout = await getCheckoutIdentity(directory, { paseoHome });
    const candidate = checkout.isGit ? checkout.mainRepoRoot ?? checkout.repoRoot ?? directory : directory;
    const root = candidate === directory ? directory : await resolveProjectParent(roots, candidate);
    return root === homedir() || root === path.parse(root).root ? null : root;
  } catch (error) {
    if (
      error instanceof WorkerOperationError &&
      ["ENOENT", "ENOTDIR", "worker_workspace_root_denied"].includes(error.message)
    ) return null;
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
