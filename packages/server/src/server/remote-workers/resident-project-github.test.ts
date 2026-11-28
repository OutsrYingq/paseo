import { mkdtemp, mkdir, readdir, realpath, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { pino } from "pino";
import { expect, it } from "vitest";
import { runGitCommand } from "../../utils/run-git-command.js";
import { cloneResidentGithubProject } from "./resident-project-github.js";

it("stages a clone under an admitted root and refuses duplicate or escaping targets", async () => {
  const temp = await mkdtemp(path.join(tmpdir(), "paseo-github-project-"));
  const home = await realpath(temp);
  try {
    const root = path.join(home, "workspace");
    const origin = path.join(home, "origin");
    await Promise.all([mkdir(root), mkdir(origin)]);
    await runGitCommand(["init", "--bare", "--initial-branch=main"], { cwd: origin });
    const targetDirectory = path.join(root, "nested", "repos");
    const request = {
      type: "project.github.clone.request",
      repo: "example/project",
      cloneProtocol: "https",
      requestId: "clone",
      targetDirectory,
    } as const;
    const options: Parameters<typeof cloneResidentGithubProject>[0] = {
      roots: [root],
      paseoHome: home,
      serverId: "fixture-worker",
      signal: new AbortController().signal,
      logger: pino({ level: "silent" }),
      // Keep the transport deterministic while the checkout and registry probe use real Git.
      runGit: (args, invocation) => runGitCommand(["clone", "--", origin, args.at(-1)!], invocation),
    };
    const checkoutPath = path.join(targetDirectory, "project");
    await expect(cloneResidentGithubProject(options, request)).resolves.toMatchObject({
      repo: "example/project",
      checkoutPath,
      registration: { rootPath: checkoutPath, kind: "git" },
    });
    await expect(cloneResidentGithubProject(options, request)).rejects.toThrow("worker_github_clone_destination_exists");
    await expect(cloneResidentGithubProject(options, { ...request, targetDirectory: path.join(home, "outside", "new") }))
      .rejects.toThrow("worker_workspace_root_denied");
    await expect(cloneResidentGithubProject(options, { ...request, repo: "https://fixture@github.com/example/project.git" }))
      .rejects.toThrow("worker_github_clone_remote_denied");
    expect(await readdir(targetDirectory)).toEqual(["project"]);
    expect(await readdir(home)).toEqual(["origin", "workspace"]);
  } finally {
    await rm(temp, { recursive: true, force: true });
  }
});
