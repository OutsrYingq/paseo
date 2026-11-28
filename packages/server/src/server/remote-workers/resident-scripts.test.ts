import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { pino } from "pino";
import { expect, it } from "vitest";
import { createTerminalManager } from "../../terminal/terminal-manager.js";
import { createResidentScripts } from "./resident-scripts.js";

it("looks up script workspaces and their automation gate from current worker records", async () => {
  const home = await mkdtemp(join(tmpdir(), "paseo-resident-scripts-"));
  const roots = join(home, "allowed");
  const cwd = join(roots, "workspace");
  const outside = join(home, "outside");
  const record = {
    workspaceId: "workspace",
    projectId: "project",
    cwd,
    kind: "directory",
    displayName: "Workspace",
    createdAt: "2026-10-01T00:00:00.000Z",
    updatedAt: "2026-10-01T00:00:00.000Z",
    archivedAt: null,
  };
  const recordsPath = join(home, "projects", "workspaces.json");
  const terminalManager = createTerminalManager();
  const scripts = createResidentScripts({
    paseoHome: home,
    roots: [roots],
    logger: pino({ level: "silent" }),
    terminalManager,
    readRecords: async (file) => readFile(join(home, file), "utf8"),
  });
  const lifetime = new AbortController();
  try {
    await mkdir(cwd, { recursive: true });
    await mkdir(outside);
    await mkdir(join(home, "projects"));
    await writeFile(join(home, "projects", "projects.json"), "[]");
    await writeFile(join(cwd, "paseo.json"), JSON.stringify({ scripts: { dev: { command: "dev" } } }));
    await writeFile(recordsPath, JSON.stringify([record]));
    const { execute } = scripts.open(() => {}, lifetime.signal);
    const list = { kind: "scriptList", workspaceId: record.workspaceId } as const;
    await expect(execute(list)).resolves.toMatchObject([
      { scriptName: "dev", lifecycle: "stopped", terminalId: null },
    ]);

    const untrustedSource = {
      kind: "change_request",
      forge: "github",
      number: 7,
      headRepository: "another/repo",
    };
    await writeFile(recordsPath, JSON.stringify([{ ...record, untrustedSource }]));
    await expect(execute({ ...list, kind: "scriptStart", scriptName: "dev" })).rejects.toThrow(
      "Scripts are blocked for PR #7 from another/repo",
    );

    await writeFile(recordsPath, JSON.stringify([{ ...record, cwd: outside }]));
    await expect(execute(list)).rejects.toThrow("worker_workspace_root_denied");
    await writeFile(recordsPath, JSON.stringify([{ ...record, archivedAt: record.updatedAt }]));
    await expect(execute(list)).rejects.toThrow("Workspace not found: workspace");
  } finally {
    lifetime.abort();
    await scripts.dispose();
    terminalManager.killAll();
    await rm(home, { recursive: true, force: true });
  }
});
