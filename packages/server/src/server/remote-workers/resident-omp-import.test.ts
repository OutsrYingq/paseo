import { mkdir, mkdtemp, realpath, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { afterEach, beforeEach, expect, test } from "vitest";
import { createResidentOmpImport } from "./resident-omp-import.js";
import { remoteOmpImportResults } from "./remote-omp-import-protocol.js";

let root: string;
let sessions: string;
let cwd: string;
let sessionFile: string;
const sessionId = "01800000-0000-7000-8000-000000000012";
const timestamp = "2026-06-09T00:00:00.000Z";

beforeEach(async () => {
  root = await realpath(await mkdtemp(path.join(tmpdir(), "paseo-remote-import-")));
  sessions = path.join(root, "sessions");
  cwd = path.join(root, "project");
  sessionFile = path.join(sessions, `test_${sessionId}.jsonl`);
  await mkdir(sessions);
  await writeFile(
    sessionFile,
    [
      { type: "session", version: 3, id: sessionId, timestamp, cwd },
      { type: "model_change", id: "model", timestamp, model: "openai-codex/gpt-5.1" },
      { type: "message", id: "user", timestamp, message: { role: "user", content: "hello" } },
    ]
      .map((entry) => JSON.stringify(entry))
      .join("\n"),
  );
});

afterEach(async () => {
  await rm(root, { recursive: true, force: true });
});

test("lists resident descriptors with wire dates restored, and resolves a legacy bare ID", async () => {
  const execute = createResidentOmpImport(async () => sessions);
  const result = await execute({ kind: "ompImportList", cwd, limit: 1 });
  expect(remoteOmpImportResults.ompImportList.parse(JSON.parse(JSON.stringify(result)))).toEqual([
    expect.objectContaining({
      providerHandleId: sessionFile,
      cwd,
      lastActivityAt: new Date(timestamp),
    }),
  ]);
  for (const providerHandleId of [sessionId, sessionFile]) {
    await expect(execute({ kind: "ompImportResolve", providerHandleId })).resolves.toEqual({
      sessionFile,
      config: { model: "openai-codex/gpt-5.1" },
    });
  }
});

test("cannot import a path from outside the worker's configured OMP directory", async () => {
  const outside = path.join(root, "outside");
  await mkdir(outside);
  const other = path.join(outside, "session.jsonl");
  await writeFile(other, "{}");
  const alias = path.join(sessions, "alias");
  await symlink(outside, alias, process.platform === "win32" ? "junction" : "dir");
  const execute = createResidentOmpImport(async () => sessions);
  for (const providerHandleId of [
    other,
    path.join(alias, "session.jsonl"),
    "../outside/session.jsonl",
  ]) {
    await expect(execute({ kind: "ompImportResolve", providerHandleId })).rejects.toThrow(
      "worker_omp_import_session_unavailable",
    );
  }
});
