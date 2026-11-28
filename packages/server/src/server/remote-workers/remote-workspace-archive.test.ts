import { mkdir, mkdtemp, realpath, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { pino } from "pino";
import { expect, it, vi } from "vitest";
import { createTerminalManager } from "../../terminal/terminal-manager.js";
import { runGitCommand } from "../../utils/run-git-command.js";
import { createWorktree } from "../../utils/worktree.js";
import { readPaseoWorktreeMetadata } from "../../utils/worktree-metadata.js";
import {
  createPersistedProjectRecord,
  createPersistedWorkspaceRecord,
  FileBackedProjectRegistry,
  FileBackedWorkspaceRegistry,
} from "../workspace-registry.js";
import { createCentralWorkspaceArchive } from "./central-workspace-archive.js";
import {
  remoteWorkspaceArchiveResults,
  type RemoteWorkspaceArchiveOperation,
} from "./remote-workspace-archive-protocol.js";
import { createResidentWorkspaceArchive } from "./resident-workspace-archive.js";

it("archives only the named workspace, recovers missing checkouts, and handles link loss after the archive write", async () => {
  const temp = await realpath(tmpdir());
  const root = await mkdtemp(path.join(temp, "paseo-remote-archive-"));
  const repo = path.join(root, "repo");
  const logger = pino({ level: "silent" });
  const terminalManager = createTerminalManager();
  try {
    await mkdir(repo);
    const git = async (...args: string[]) =>
      (await runGitCommand(args, { cwd: repo })).stdout.trim();
    await git("init", "--initial-branch=main");
    await git("config", "user.name", "Fixture");
    await git("config", "user.email", "fixture@example.invalid");
    await writeFile(path.join(repo, "fixture.txt"), "fixture");
    await git("add", "fixture.txt");
    const tree = await git("write-tree");
    const revision = await git("commit-tree", tree, "-m", "fixture");
    await git("update-ref", "refs/heads/main", revision);
    const { worktreePath, branchName } = await createWorktree({
      cwd: repo,
      paseoHome: root,
      worktreeSlug: "archive-test",
      source: { kind: "branch-off", branchName: "archive-test", baseBranch: "main" },
    });
    const forbiddenTeardown = `node -e "require('node:fs').writeFileSync(process.env.PASEO_SOURCE_CHECKOUT_PATH + '/teardown-ran', 'yes')"`;
    await writeFile(
      path.join(worktreePath, "paseo.json"),
      JSON.stringify({ worktree: { teardown: [forbiddenTeardown] } }),
    );
    const workspaceRegistry = new FileBackedWorkspaceRegistry(
      path.join(root, "workspaces.json"),
      logger,
    );
    const projectRegistry = new FileBackedProjectRegistry(path.join(root, "projects.json"), logger);
    const timestamps = {
      createdAt: "2026-10-01T00:00:00.000Z",
      updatedAt: "2026-10-01T00:00:00.000Z",
    };
    await projectRegistry.upsert(
      createPersistedProjectRecord({
        projectId: "project",
        rootPath: await realpath(repo),
        kind: "git",
        displayName: "Project",
        ...timestamps,
      }),
    );
    const untrustedSource = {
      kind: "change_request" as const,
      forge: "github",
      number: 7,
      headRepository: "contributor/repo",
    };
    const workspace = createPersistedWorkspaceRecord({
      workspaceId: "worktree",
      projectId: "project",
      cwd: worktreePath,
      kind: "worktree",
      displayName: "Worktree",
      branch: branchName,
      worktreeRoot: worktreePath,
      mainRepoRoot: await realpath(repo),
      baseBranch: "refs/heads/main",
      isPaseoOwnedWorktree: true,
      untrustedSource,
      ...timestamps,
    });
    await workspaceRegistry.upsert(workspace);
    await workspaceRegistry.upsert({ ...workspace, workspaceId: "sibling" });

    const stopWorkspaceSetup = vi.fn(async () => {});
    const resident = createResidentWorkspaceArchive({
      roots: [root],
      paseoHome: root,
      terminalManager,
      stopWorkspaceSetup,
      logger,
      listWorkspaces: () => workspaceRegistry.listAll(),
      getProject: (projectId) => projectRegistry.get(projectId),
      signal: new AbortController().signal,
    });
    let losePrepare = true;
    let loseRemoveAck = false;
    const call = async (request: RemoteWorkspaceArchiveOperation) => {
      if (request.kind === "workspaceArchivePrepare" && losePrepare)
        throw new Error("worker_connection_lost");
      const result = await resident.execute(request);
      if (request.kind === "workspaceArchiveRemove" && loseRemoveAck)
        throw new Error("worker_connection_lost");
      return remoteWorkspaceArchiveResults[request.kind].parse(result);
    };
    const central = createCentralWorkspaceArchive({
      call,
      workspaceRegistry,
      projectRegistry,
      logger,
      agentManager: {
        listAgents: () => [],
        getAgent: () => null,
        archiveAgent: vi.fn(),
        archiveSnapshot: vi.fn(),
      },
      agentStorage: { listByWorkspace: async () => [] },
      unarchiveAgent: vi.fn(),
    });
    await expect(
      central.archive({ kind: "workspace", workspaceId: "worktree" }, "lost-before-write"),
    ).rejects.toThrow("worker_connection_lost");
    expect((await workspaceRegistry.get("worktree"))?.archivedAt).toBeNull();
    expect(stopWorkspaceSetup).not.toHaveBeenCalled();
    losePrepare = false;
    expect(
      await central.archive({ kind: "workspace", workspaceId: "worktree" }, "first"),
    ).toMatchObject({
      removedDirectory: false,
      archivedWorkspaceIds: ["worktree"],
    });
    expect((await workspaceRegistry.get("sibling"))?.archivedAt).toBeNull();
    expect(stopWorkspaceSetup.mock.calls).toEqual([["worktree"]]);
    expect((await workspaceRegistry.get("worktree"))?.head).toBe("refs/heads/archive-test");
    expect(await readPaseoWorktreeMetadata(worktreePath)).toMatchObject({ untrustedSource });
    await expect(
      resident.execute({
        kind: "workspaceArchiveRemove",
        workspaceId: "worktree",
        archivedAt: timestamps.createdAt,
      }),
    ).rejects.toThrow("worker_workspace_archive_stamp_mismatch");
    expect(await central.inspect("worktree")).toMatchObject({ action: "unarchive" });

    loseRemoveAck = true;
    expect(
      await central.archive({ kind: "workspace", workspaceId: "sibling" }, "second"),
    ).toMatchObject({
      removedDirectory: false,
      archivedWorkspaceIds: ["sibling"],
      directoryLeftBehind: { path: worktreePath },
    });
    await expect(stat(worktreePath)).rejects.toMatchObject({ code: "ENOENT" });
    await expect(stat(path.join(repo, "teardown-ran"))).rejects.toMatchObject({ code: "ENOENT" });
    expect(await central.inspect("worktree")).toMatchObject({ action: "restore" });
    await central.restore("worktree");
    expect(await workspaceRegistry.get("worktree")).toMatchObject({
      archivedAt: null,
      branch: "archive-test",
      untrustedSource,
    });
    expect(await readPaseoWorktreeMetadata(worktreePath)).toMatchObject({ untrustedSource });
    expect((await workspaceRegistry.get("sibling"))?.archivedAt).toBeTruthy();
  } finally {
    terminalManager.killAll();
    if (path.dirname(root) !== temp || !path.basename(root).startsWith("paseo-remote-archive-")) {
      throw new Error("Unexpected resident archive fixture path");
    }
    await rm(root, { recursive: true, force: true });
  }
});
