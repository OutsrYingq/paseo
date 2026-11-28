import { lstat } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { getWorktreeRoot, resolveAbsoluteGitDir, resolveGitCommonDir } from "../../utils/checkout-git.js";
import { runGitCommand } from "../../utils/run-git-command.js";
import { assertSafeGitRef } from "../worktree-session.js";
import type { CheckoutAction } from "../session/checkout/checkout-actions-protocol.js";
import { WorkerOperationError } from "./execution-protocol.js";
import { resolveResidentWorkspacePath } from "./resident-projects.js";

async function validateGitStorage(roots: readonly string[], cwd: string): Promise<string> {
  const [gitDir, commonDir] = await Promise.all([
    resolveAbsoluteGitDir(cwd),
    resolveGitCommonDir(cwd),
  ]);
  if (!gitDir || !commonDir) throw new WorkerOperationError("worker_checkout_not_git");
  await resolveResidentWorkspacePath(roots, gitDir);
  return resolveResidentWorkspacePath(roots, commonDir);
}

async function validateWorktree(roots: readonly string[], cwd: string) {
  const worktree = await getWorktreeRoot(cwd);
  if (!worktree) throw new WorkerOperationError("worker_checkout_not_git");
  const root = await resolveResidentWorkspacePath(roots, worktree);
  const commonDir = await validateGitStorage(roots, cwd);
  return { root, commonDir };
}

async function validateDiscardPath(root: string, target: string): Promise<void> {
  const parts = target.split(/[\\/]/);
  if (!target || target.includes("\0") || path.isAbsolute(target) || /^[A-Za-z]:/.test(target) || parts.includes("..")) {
    throw new WorkerOperationError("worker_checkout_path_denied");
  }
  let candidate = root;
  // Git treats a final symlink as a file, but must never operate through one.
  for (const part of parts.slice(0, -1)) {
    candidate = path.join(candidate, part);
    try {
      if ((await lstat(candidate)).isSymbolicLink()) {
        throw new WorkerOperationError("worker_checkout_path_denied");
      }
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") return;
      throw error;
    }
  }
}

export async function prepareResidentCheckoutAction(
  roots: readonly string[],
  request: CheckoutAction,
) {
  const { root, commonDir } = await validateWorktree(roots, request.cwd);
  if ("baseRef" in request && request.baseRef !== undefined) {
    if (request.baseRef.startsWith("-")) throw new WorkerOperationError("worker_checkout_ref_denied");
    assertSafeGitRef(request.baseRef, "base branch");
  }
  if (request.type === "checkout.discard_changes.request") {
    await Promise.all(request.paths.map((target) => validateDiscardPath(root, target)));
  }

  return {
    async validateMergeTarget(cwd: string): Promise<void> {
      const candidate = await resolveResidentWorkspacePath(roots, cwd);
      if ((await validateWorktree(roots, candidate)).commonDir !== commonDir) {
        throw new WorkerOperationError("worker_checkout_merge_target_denied");
      }
    },
    async validatePushRemote(cwd: string, remote: string): Promise<void> {
      // Git performs the remote selection; ask Git to expand pushurl and insteadOf on that remote.
      const { stdout } = await runGitCommand(["remote", "get-url", "--push", "--all", "--", remote], { cwd });
      for (const url of stdout.trim().split(/\r?\n/)) {
        if (/^(https?|ssh|git):\/\//i.test(url)) continue;
        const windowsPath = process.platform === "win32" && /^[A-Za-z]:/.test(url);
        const fileUrl = /^file:\/\//i.test(url);
        if (!url || (!fileUrl && /^[\w+.-]+:(?::|\/\/)/.test(url) && !windowsPath)) {
          throw new WorkerOperationError("worker_checkout_remote_denied");
        }
        if (!windowsPath && !fileUrl && /^(?:[^/:\\\s]+|(?:[^@/\\\s]+@)?\[[^\]]+\]):\S+$/.test(url)) {
          continue; // scp-style SSH
        }
        const localPath = fileUrl ? fileURLToPath(url) : path.resolve(root, url);
        const local = await resolveResidentWorkspacePath(roots, localPath);
        await validateGitStorage(roots, local);
        const localWorktree = await getWorktreeRoot(local);
        if (localWorktree) await resolveResidentWorkspacePath(roots, localWorktree);
      }
    },
  };
}
