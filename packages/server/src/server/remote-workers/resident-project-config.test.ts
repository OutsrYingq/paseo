import { mkdtemp, mkdir, readFile, realpath, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { pino } from "pino";
import { expect, it } from "vitest";
import { executeResidentProjectConfig } from "./resident-project-config.js";
import { projectConfigResults } from "./remote-project-config-protocol.js";

it("edits only a registered project and preserves its optimistic config revision", async () => {
  const directory = await mkdtemp(path.join(tmpdir(), "paseo-project-config-"));
  const home = await realpath(directory);
  try {
    const repoRoot = path.join(home, "project");
    const unregistered = path.join(home, "unregistered");
    await mkdir(repoRoot);
    await mkdir(unregistered);
    const project = {
      projectId: "project",
      rootPath: repoRoot,
      kind: "non_git" as const,
      displayName: "Project",
      archivedAt: null,
      createdAt: "2026-01-01T00:00:00.000Z",
      updatedAt: "2026-01-01T00:00:00.000Z",
    };
    const options = {
      roots: [home],
      listProjects: async () => [project],
      logger: pino({ level: "silent" }),
      signal: new AbortController().signal,
    };
    const read = { type: "read_project_config_request", requestId: "read", repoRoot } as const;
    const file = path.join(repoRoot, "paseo.json");
    const first = projectConfigResults.projectConfigRead.parse(
      await executeResidentProjectConfig(options, { kind: "projectConfigRead", request: read }),
    );
    if (!first.payload.ok) throw new Error(first.payload.error.code);
    expect(first.payload.revision).toBeNull();
    const denied = await executeResidentProjectConfig(options, {
      kind: "projectConfigRead", request: { ...read, repoRoot: unregistered },
    });
    expect(denied).toMatchObject({ payload: { ok: false, error: { code: "project_not_found" } } });
    const config = { worktree: { setup: "npm ci" } };
    const write = { type: "write_project_config_request", requestId: "write", repoRoot, config, expectedRevision: first.payload.revision } as const;
    expect(await executeResidentProjectConfig(options, { kind: "projectConfigWrite", request: write }))
      .toMatchObject({ payload: { ok: true, config } });
    expect(await executeResidentProjectConfig(options, {
      kind: "projectConfigWrite", request: { ...write, config: { worktree: { setup: "npm run test" } } },
    })).toMatchObject({ payload: { ok: false, error: { code: "stale_project_config" } } });
    expect(JSON.parse(await readFile(file, "utf8"))).toEqual(config);
    if (process.platform !== "win32") {
      const other = path.join(unregistered, "config.json");
      await writeFile(other, "{}");
      await rm(file);
      await symlink(other, file);
      await expect(executeResidentProjectConfig(options, { kind: "projectConfigRead", request: read }))
        .rejects.toThrow("worker_workspace_root_denied");
    }
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});
