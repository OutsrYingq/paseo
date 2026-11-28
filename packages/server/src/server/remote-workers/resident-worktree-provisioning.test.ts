import { mkdir, mkdtemp, readFile, realpath, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { pino } from "pino";
import { afterEach, expect, it, vi } from "vitest";
import { createForgeResolver } from "../../services/forge-resolver.js";
import { createGitHubService } from "../../services/github-service.js";
import { createTerminalManager } from "../../terminal/terminal-manager.js";
import { runGitCommand } from "../../utils/run-git-command.js";
import { getPaseoWorktreesRoot } from "../../utils/worktree.js";
import { readPaseoWorktreeMetadata } from "../../utils/worktree-metadata.js";
import { createPersistedWorkspaceRecord } from "../workspace-registry.js";
import type { WorkspaceSetupOperation } from "../workspace-setup-runtime.js";
import type {
  RemoteWorktreeMaterialization,
  RemoteWorktreeSetupEvent,
} from "./remote-worktree-provisioning-protocol.js";
import {
  approveResidentWorktreeSetup,
  materializeResidentWorktree,
  startResidentWorktreeSetup,
} from "./resident-worktree-provisioning.js";

const fixtures: string[] = [];
const logger = pino({ level: "silent" });

async function fixture() {
  const temp = await realpath(tmpdir());
  const home = await mkdtemp(path.join(temp, "paseo-resident-worktree-"));
  fixtures.push(home);
  const allowed = path.join(home, "allowed");
  const paseoHome = path.join(home, "paseo-home");
  const worktrees = path.join(paseoHome, "worktrees");
  const repo = path.join(allowed, "repo");
  const nested = path.join(repo, "nested");
  await mkdir(nested, { recursive: true });
  await mkdir(worktrees, { recursive: true });
  const git = async (...args: string[]) => (await runGitCommand(args, { cwd: repo })).stdout.trim();
  await git("init", "--initial-branch=main");
  await git("config", "user.name", "Fixture");
  await git("config", "user.email", "fixture@example.invalid");
  await writeFile(path.join(nested, "tracked.txt"), "tracked");
  await git("add", "nested/tracked.txt");
  const tree = await git("write-tree");
  const commit = await git("commit-tree", tree, "-m", "fixture");
  await git("update-ref", "refs/heads/main", commit);
  return {
    home,
    repo,
    nested,
    git,
    commit,
    options: { roots: [allowed, worktrees], paseoHome, logger },
  };
}

afterEach(async () => {
  const temp = await realpath(tmpdir());
  for (const home of fixtures.splice(0)) {
    if (
      path.dirname(home) !== temp ||
      !path.basename(home).startsWith("paseo-resident-worktree-")
    ) {
      throw new Error("Unexpected resident test fixture path");
    }
    await rm(home, { recursive: true, force: true });
  }
});

function materialized(result: RemoteWorktreeMaterialization) {
  if ("error" in result) throw new Error(result.error.message);
  return result;
}

function workspaceFor(result: ReturnType<typeof materialized>) {
  return createPersistedWorkspaceRecord({
    workspaceId: "worktree",
    projectId: "project",
    cwd: result.cwd,
    kind: "worktree",
    displayName: result.branch,
    branch: result.branch,
    baseBranch: result.baseBranch,
    worktreeRoot: result.worktreeRoot,
    mainRepoRoot: result.repoRoot,
    isPaseoOwnedWorktree: true,
    createdAt: "2026-10-01T00:00:00.000Z",
    updatedAt: "2026-10-01T00:00:00.000Z",
  });
}

it("materializes branch-off and branch checkout, and starts setup in the persisted nested workspace", async () => {
  const { options, repo, nested, git, commit } = await fixture();
  const command = `node -e "require('node:fs').writeFileSync('setup-ran', process.env.PASEO_BRANCH_NAME)"`;
  await writeFile(
    path.join(nested, "paseo.json"),
    JSON.stringify({ worktree: { setup: [command] } }),
  );
  const result = materialized(
    await materializeResidentWorktree(options, {
      kind: "worktreeMaterialize",
      cwd: nested,
      action: "branch-off",
      worktreeSlug: "new-worktree",
      branchName: "new-branch",
      refName: "main",
    }),
  );
  expect(result).toMatchObject({
    cwd: path.join(result.worktreeRoot, "nested"),
    sourceCwd: await realpath(nested),
    repoRoot: await realpath(repo),
    branch: "new-branch",
    baseBranch: "refs/heads/main",
  });
  expect(await git("rev-parse", "refs/heads/new-branch")).toBe(commit);

  const events: RemoteWorktreeSetupEvent[] = [];
  const workspace = workspaceFor(result);
  const terminalManager = createTerminalManager();
  let setup: Promise<void> | undefined;
  try {
    await startResidentWorktreeSetup(
      {
        ...options,
        terminalManager,
        getWorkspace: async () => workspace,
        startWorkspaceSetup(_workspaceId, operation) {
          setup = operation(new AbortController().signal);
        },
        publish: (event) => events.push(event),
      },
      workspace.workspaceId,
    );
    expect(setup).toBeDefined();
    await setup;
    expect(events.at(-1)?.payload).toMatchObject({ status: "completed" });
    expect(await readFile(path.join(result.cwd, "setup-ran"), "utf8")).toBe("new-branch");
  } finally {
    terminalManager.killAll();
  }

  await git("branch", "existing");
  const checkout = materialized(
    await materializeResidentWorktree(options, {
      kind: "worktreeMaterialize",
      cwd: repo,
      action: "checkout",
      refName: "existing",
    }),
  );
  expect(checkout).toMatchObject({ branch: "existing", cwd: checkout.worktreeRoot });
  await expect(
    materializeResidentWorktree(options, {
      kind: "worktreeMaterialize",
      cwd: repo,
      action: "checkout",
      refName: "missing",
    }),
  ).resolves.toEqual({ error: { code: "unknown_branch", message: "Unknown branch: missing" } });
});

it("uses the resolved forge's checkout refs and blocks fork setup from local trust metadata", async () => {
  const { options, repo, git, commit } = await fixture();
  await git("remote", "add", "origin", repo);
  await git("update-ref", "refs/merge-requests/7/head", commit);
  const getPullRequestCheckoutTarget = vi.fn(async () => ({
    number: 7,
    baseRefName: "main",
    headRefName: "changes",
    checkoutRefs: [{ remoteName: "origin", remoteRef: "refs/merge-requests/7/head" }],
    headOwnerLogin: "contributor",
    headRepositorySshUrl: null,
    headRepositoryUrl: null,
    isCrossRepository: true,
  }));
  const forgeResolver = createForgeResolver({
    resolveRemoteUrl: async () => "https://gitlab.com/fixture/repo",
    createService: () => ({ ...createGitHubService(), getPullRequestCheckoutTarget }),
  });
  const result = materialized(
    await materializeResidentWorktree(
      { ...options, forgeResolver },
      {
        kind: "worktreeMaterialize",
        cwd: repo,
        action: "checkout",
        checkoutSource: { kind: "change_request", forge: "gitlab", number: 7 },
      },
    ),
  );
  const untrustedSource = {
    kind: "change_request",
    forge: "gitlab",
    number: 7,
    headRepository: "contributor",
  };
  expect(getPullRequestCheckoutTarget).toHaveBeenCalledWith({ cwd: result.repoRoot, number: 7 });
  expect(result.untrustedSource).toEqual(untrustedSource);
  expect(await readPaseoWorktreeMetadata(result.worktreeRoot)).toMatchObject({ untrustedSource });

  const workspace = workspaceFor(result);
  const terminalManager = createTerminalManager();
  const startWorkspaceSetup = vi.fn<(_id: string, _op: WorkspaceSetupOperation) => void>();
  const publish = vi.fn<(event: RemoteWorktreeSetupEvent) => void>();
  try {
    await startResidentWorktreeSetup(
      {
        ...options,
        terminalManager,
        startWorkspaceSetup,
        publish,
        getWorkspace: async () => workspace,
      },
      workspace.workspaceId,
    );
    expect(startWorkspaceSetup).not.toHaveBeenCalled();
    expect(publish).toHaveBeenCalledWith({
      type: "worktreeSetup",
      payload: expect.objectContaining({ status: "blocked", blockedSource: untrustedSource }),
    });
    const blockedPeer = {
      ...workspace,
      workspaceId: "peer",
      untrustedSource: result.untrustedSource,
    };
    const otherCheckout = { ...blockedPeer, workspaceId: "other", worktreeRoot: repo, cwd: repo };
    const approval = await approveResidentWorktreeSetup(
      {
        ...options,
        getWorkspace: async () => workspace,
        listWorkspaces: async () => [workspace, blockedPeer, otherCheckout],
        signal: new AbortController().signal,
      },
      workspace.workspaceId,
    );
    expect(approval).toEqual({ clearedMetadata: true, blockedWorkspaceIds: ["peer"] });
    expect((await readPaseoWorktreeMetadata(result.worktreeRoot))?.untrustedSource).toBeUndefined();
  } finally {
    terminalManager.killAll();
  }
});

it("rejects a destination redirected into an unadmitted root before creating a branch", async () => {
  const { options, home, repo, git } = await fixture();
  const outside = path.join(home, "outside");
  await mkdir(outside);
  const redirect = await getPaseoWorktreesRoot(repo, options.paseoHome);
  await mkdir(path.dirname(redirect), { recursive: true });
  await symlink(outside, redirect, process.platform === "win32" ? "junction" : "dir");
  await expect(
    materializeResidentWorktree(options, {
      kind: "worktreeMaterialize",
      cwd: repo,
      branchName: "should-not-exist",
      refName: "main",
    }),
  ).rejects.toThrow("worker_workspace_root_denied");
  expect(await git("branch", "--list", "should-not-exist")).toBe("");
});

it("rolls back a checkout whose selected subdirectory is absent", async () => {
  const { options, repo, git } = await fixture();
  const cwd = path.join(repo, "untracked-directory");
  await mkdir(cwd);
  const existingWorktrees = await git("worktree", "list", "--porcelain");
  await expect(
    materializeResidentWorktree(options, {
      kind: "worktreeMaterialize",
      cwd,
      worktreeSlug: "missing-subdirectory",
      refName: "main",
    }),
  ).rejects.toThrow("worker_worktree_project_directory_missing");
  expect(await git("worktree", "list", "--porcelain")).toBe(existingWorktrees);
});
