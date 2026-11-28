import { mkdir, mkdtemp, readFile, realpath, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { pino } from "pino";
import { afterEach, expect, it, vi } from "vitest";
import {
  createPersistedProjectRecord,
  createPersistedWorkspaceRecord,
  FileBackedProjectRegistry,
  FileBackedWorkspaceRegistry,
} from "../workspace-registry.js";
import { createCentralProjectManagement } from "./central-project-management.js";
import { createCentralWorkspaceArchive } from "./central-workspace-archive.js";

const fixtures: string[] = [];
const logger = pino({ level: "silent" });
const createdAt = "2026-10-01T00:00:00.000Z";
const time = { createdAt, updatedAt: createdAt };

async function fixture() {
  const temporaryRoot = await realpath(tmpdir());
  const root = await mkdtemp(path.join(temporaryRoot, "paseo-remote-project-"));
  fixtures.push(root);
  const repoRoot = path.join(root, "repo");
  const worktreeRoot = path.join(root, "checkout");
  await mkdir(repoRoot);
  await mkdir(worktreeRoot);
  const projectRegistry = new FileBackedProjectRegistry(path.join(root, "projects.json"), logger);
  const workspaceRegistry = new FileBackedWorkspaceRegistry(
    path.join(root, "workspaces.json"),
    logger,
  );
  const project = createPersistedProjectRecord({
    projectId: "project",
    rootPath: repoRoot,
    displayName: "Repo",
    kind: "git",
    ...time,
  });
  const workspace = createPersistedWorkspaceRecord({
    workspaceId: "workspace",
    projectId: project.projectId,
    cwd: worktreeRoot,
    worktreeRoot,
    mainRepoRoot: repoRoot,
    kind: "worktree",
    branch: "topic",
    isPaseoOwnedWorktree: true,
    displayName: "Checkout",
    ...time,
  });
  await projectRegistry.upsert(project);
  await workspaceRegistry.upsert(workspace);
  return { projectRegistry, workspaceRegistry, project, workspace, repoRoot, worktreeRoot };
}

afterEach(async () => {
  const temp = await realpath(tmpdir());
  for (const root of fixtures.splice(0)) {
    if (path.dirname(root) !== temp || !path.basename(root).startsWith("paseo-remote-project-")) {
      throw new Error("Unexpected project fixture path");
    }
    await rm(root, { recursive: true, force: true });
  }
});

it("authorizes stock settings and reopens the original public workspace", async () => {
  const { projectRegistry, workspaceRegistry, project, workspace, repoRoot, worktreeRoot } =
    await fixture();
  const read = vi.fn(async (request: { requestId: string; repoRoot: string }) => ({
    type: "read_project_config_response" as const,
    payload: {
      requestId: request.requestId,
      repoRoot: request.repoRoot,
      ok: true as const,
      config: null,
      revision: null,
    },
  }));
  const write = vi.fn(async (request: { requestId: string; repoRoot: string }) => ({
    type: "write_project_config_response" as const,
    payload: {
      requestId: request.requestId,
      repoRoot: request.repoRoot,
      ok: false as const,
      error: { code: "write_failed" as const },
    },
  }));
  const restoreWorkspaceForOpen = vi.fn(async (workspaceId: string) => {
    const record = await workspaceRegistry.get(workspaceId);
    if (!record) throw new Error("Workspace not found");
    await workspaceRegistry.upsert({ ...record, archivedAt: null });
  });
  const archiveWorkspaceForProjectRemoval = vi.fn();
  const options = {
    projectRegistry,
    workspaceRegistry,
    restoreWorkspaceForOpen,
    archiveWorkspaceForProjectRemoval,
  };
  const management = createCentralProjectManagement({
    ...options,
    projectConfig: {
      read,
      write,
      resolveRoot: async (request) => {
        if (request === "/alias") return repoRoot;
        throw new Error("worker_workspace_root_denied");
      },
    },
  });
  expect(
    await management.dispatch({
      type: "project.rename.request",
      projectId: "foreign",
      requestId: "foreign",
      customName: "Incorrect",
    }),
  ).toMatchObject({ payload: { accepted: false, error: "Project not found" } });
  expect(
    await management.dispatch({
      type: "project.remove.request",
      projectId: "foreign",
      requestId: "remove-foreign",
    }),
  ).toMatchObject({ payload: { accepted: false } });
  expect(archiveWorkspaceForProjectRemoval).not.toHaveBeenCalled();
  const rename = {
    type: "project.rename.request" as const,
    projectId: project.projectId,
    requestId: "rename",
    customName: "  Label ",
  };
  expect(await management.dispatch(rename)).toMatchObject({
    payload: { accepted: true, customName: "Label" },
  });
  const renamed = await projectRegistry.get(project.projectId);
  expect(renamed?.customName).toBe("Label");
  expect(await management.dispatch(rename)).toMatchObject({ payload: { accepted: true } });
  expect(await projectRegistry.get(project.projectId)).toEqual(renamed);
  await management.dispatch({ ...rename, requestId: "clear", customName: "  " });
  expect((await projectRegistry.get(project.projectId))?.customName).toBeNull();

  expect(
    await management.dispatch({
      type: "read_project_config_request",
      repoRoot: "/foreign",
      requestId: "foreign-config",
    }),
  ).toMatchObject({ payload: { ok: false, error: { code: "project_not_found" } } });
  expect(read).not.toHaveBeenCalled();
  expect(
    await management.dispatch({
      type: "read_project_config_request",
      repoRoot: "/alias",
      requestId: "read",
    }),
  ).toMatchObject({ payload: { repoRoot, ok: true } });
  expect(read).toHaveBeenCalledWith({
    type: "read_project_config_request",
    requestId: "read",
    repoRoot,
  });
  expect(
    await createCentralProjectManagement(options).dispatch({
      type: "read_project_config_request",
      repoRoot,
      requestId: "unsupported",
    }),
  ).toMatchObject({ payload: { ok: false, error: { code: "write_failed" } } });
  expect(
    await management.dispatch({
      type: "project.icon.get.request",
      projectId: project.projectId,
      requestId: "icon",
    }),
  ).toMatchObject({ payload: { icon: null, error: "Project icons are unavailable on this host" } });

  await workspaceRegistry.archive(workspace.workspaceId, time.updatedAt);
  await workspaceRegistry.upsert({
    ...workspace,
    workspaceId: "newer",
    createdAt: "2026-10-02T00:00:00.000Z",
    archivedAt: time.updatedAt,
  });
  await workspaceRegistry.upsert({ ...workspace, workspaceId: "background", background: true });
  expect(await management.resolveOpenWorkspace(worktreeRoot)).toMatchObject({
    workspaceId: workspace.workspaceId,
    archivedAt: null,
  });
  await management.resolveOpenWorkspace(worktreeRoot);
  expect(restoreWorkspaceForOpen.mock.calls).toEqual([[workspace.workspaceId]]);
  await projectRegistry.remove(project.projectId);
  expect(await management.resolveOpenWorkspace(worktreeRoot)).toBeNull();
});

it("retries record-only workspace archives before project removal and preserves checkout directories", async () => {
  const { projectRegistry, workspaceRegistry, project, workspace, repoRoot, worktreeRoot } =
    await fixture();
  await writeFile(path.join(repoRoot, "source-marker"), "source");
  await writeFile(path.join(worktreeRoot, "worktree-marker"), "worktree");
  await workspaceRegistry.upsert({ ...workspace, workspaceId: "other" });
  let failOther = true;
  const call = vi.fn(async (operation: { kind: string; workspaceId: string }) => {
    if (operation.kind !== "workspaceArchivePrepare")
      throw new Error(`Unexpected operation: ${operation.kind}`);
    if (operation.workspaceId === "other" && failOther) throw new Error("worker_connection_lost");
    expect(await projectRegistry.get(project.projectId)).not.toBeNull();
    return { head: "refs/heads/topic" };
  });
  const archive = createCentralWorkspaceArchive({
    call,
    projectRegistry,
    workspaceRegistry,
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
  const management = createCentralProjectManagement({
    projectRegistry,
    workspaceRegistry,
    archiveWorkspaceForProjectRemoval: (workspaceId, requestId) =>
      archive.archive({ kind: "workspace", workspaceId }, requestId, { retainCheckout: true }),
    restoreWorkspaceForOpen: archive.restore,
  });
  const remove = {
    type: "project.remove.request" as const,
    projectId: project.projectId,
    requestId: "remove",
  };
  expect(await management.dispatch(remove)).toMatchObject({ payload: { accepted: false } });
  expect(await projectRegistry.get(project.projectId)).not.toBeNull();
  expect((await workspaceRegistry.get("workspace"))?.archivedAt).toBeTruthy();
  expect((await workspaceRegistry.get("other"))?.archivedAt).toBeNull();
  failOther = false;
  const [retry, duplicate] = await Promise.all([
    management.dispatch(remove),
    management.dispatch(remove),
  ]);
  expect(retry).toMatchObject({ payload: { accepted: true, removedWorkspaceIds: ["other"] } });
  expect(duplicate).toEqual(retry);
  expect(await projectRegistry.get(project.projectId)).toBeNull();
  expect(await workspaceRegistry.listActive()).toEqual([]);
  expect(call.mock.calls.map(([op]) => [op.kind, op.workspaceId])).toEqual([
    ["workspaceArchivePrepare", "workspace"],
    ["workspaceArchivePrepare", "other"],
    ["workspaceArchivePrepare", "other"],
  ]);
  expect(await readFile(path.join(repoRoot, "source-marker"), "utf8")).toBe("source");
  expect(await readFile(path.join(worktreeRoot, "worktree-marker"), "utf8")).toBe("worktree");
});
