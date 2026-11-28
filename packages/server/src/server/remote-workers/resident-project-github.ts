import { lstat, mkdir, mkdtemp, realpath, rename, rm } from "node:fs/promises";
import { homedir } from "node:os";
import path from "node:path";
import type { Logger } from "pino";
import type { ProjectGithubCloneRequest, WorkspaceGithubSearchRepositoriesRequest, WorkspaceGithubSearchRepositoriesResponse } from "@getpaseo/protocol/messages";
import {
  createGitHubService,
  GitHubAuthenticationError,
  GitHubCliMissingError,
  type GitHubService,
} from "../../services/github-service.js";
import { expandTilde } from "../../utils/path.js";
import { runGitCommand, type RunGitCommand } from "../../utils/run-git-command.js";
import { normalizeCloneRepository } from "../project-github.js";
import { WorkerOperationError } from "./execution-protocol.js";
import { inspectResidentProject, resolveProjectParent, resolveResidentWorkspacePath } from "./resident-projects.js";

export async function searchResidentGithubRepositories(
  request: WorkspaceGithubSearchRepositoriesRequest,
  github: Pick<GitHubService, "searchRepositories"> = createGitHubService(),
): Promise<WorkspaceGithubSearchRepositoriesResponse> {
  const { requestId, query, limit } = request;
  try {
    const repositories = await github.searchRepositories({ cwd: homedir(), query, limit });
    return {
      type: "workspace.github.search_repositories.response",
      payload: { requestId, repositories, status: "success", available: true, error: null },
    };
  } catch (error) {
    const payload: WorkspaceGithubSearchRepositoriesResponse["payload"] = error instanceof GitHubCliMissingError
      ? { requestId, repositories: [], status: "unavailable", reason: "gh_missing", available: false, error: "GitHub CLI (gh) is not installed or not in PATH" }
      : error instanceof GitHubAuthenticationError
        ? { requestId, repositories: [], status: "unauthenticated", available: false, error: "GitHub CLI is not authenticated. Run gh auth login on the host." }
        : { requestId, repositories: [], status: "error", available: true, error: "GitHub search failed" };
    return { type: "workspace.github.search_repositories.response", payload };
  }
}

async function resolveCloneParent(roots: readonly string[], directory: string): Promise<string> {
  const missing: string[] = [];
  let current = directory;
  for (;;) {
    try {
      await lstat(current);
      const canonical = await resolveProjectParent(roots, current);
      return path.join(canonical, ...missing.reverse());
    } catch (error) {
      if (!(error instanceof Error) || !('code' in error) || error.code !== "ENOENT") throw error;
      const parent = path.dirname(current);
      if (parent === current) throw error;
      missing.push(path.basename(current));
      current = parent;
    }
  }
}

function requireCloneTransport(url: string) {
  if (/^[A-Za-z0-9._-]+@[A-Za-z0-9.-]+:[^\s]+$/.test(url)) return;
  let remote: URL;
  try {
    remote = new URL(url);
  } catch {
    throw new WorkerOperationError("worker_github_clone_remote_denied");
  }
  if (
    (remote.protocol !== "https:" && remote.protocol !== "ssh:") ||
    !remote.hostname || remote.password || remote.search || remote.hash ||
    (remote.protocol === "https:" && remote.username)
  ) {
    throw new WorkerOperationError("worker_github_clone_remote_denied");
  }
}

export async function cloneResidentGithubProject(
  options: {
    roots: readonly string[];
    paseoHome: string;
    serverId: string;
    signal: AbortSignal;
    logger: Logger;
    runGit?: RunGitCommand;
  },
  request: ProjectGithubCloneRequest,
) {
  const { roots, signal, logger, runGit = runGitCommand } = options;
  const repo = normalizeCloneRepository(request);
  requireCloneTransport(repo.cloneUrl);
  const requested = expandTilde(request.targetDirectory);
  if (!path.isAbsolute(requested) || repo.name === "." || repo.name === "..") {
    throw new WorkerOperationError("worker_github_clone_directory_denied");
  }
  const directory = await resolveCloneParent(roots, path.resolve(requested));
  signal.throwIfAborted();
  await mkdir(directory, { recursive: true });
  const parent = await resolveProjectParent(roots, directory);
  const checkoutPath = path.join(parent, repo.name);
  async function requireNewCheckout() {
    try {
      await lstat(checkoutPath);
      throw new WorkerOperationError("worker_github_clone_destination_exists");
    } catch (error) {
      if (!(error instanceof Error) || !("code" in error) || error.code !== "ENOENT") throw error;
    }
  }
  await requireNewCheckout();
  const staging = await mkdtemp(path.join(parent, ".paseo-clone-"));
  try {
    try {
      await runGit([
        "-c", "protocol.allow=never",
        "-c", "protocol.https.allow=always",
        "-c", "protocol.ssh.allow=always",
        "clone", "--", repo.cloneUrl, staging,
      ], { cwd: parent, timeout: 5 * 60 * 1000, maxOutputBytes: 1024 * 1024 });
    } catch {
      throw new WorkerOperationError("worker_github_clone_failed");
    }
    signal.throwIfAborted();
    await requireNewCheckout();
    if (await realpath(parent) !== parent || await resolveResidentWorkspacePath([parent], staging) !== staging) {
      throw new WorkerOperationError("worker_github_clone_directory_changed");
    }
    await rename(staging, checkoutPath);
  } catch (error) {
    await rm(staging, { recursive: true, force: true }).catch((cause: unknown) => {
      logger.warn({ err: cause }, "Failed to clean up partial worker GitHub clone");
    });
    throw error;
  }
  const registration = await inspectResidentProject({ ...options, cwd: checkoutPath });
  if (!registration) throw new WorkerOperationError("worker_github_clone_checkout_missing");
  return { repo: repo.displayName, checkoutPath, registration };
}
