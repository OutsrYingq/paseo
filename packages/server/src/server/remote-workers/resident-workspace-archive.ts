import { lstat, realpath, stat } from "node:fs/promises";
import path from "node:path";
import type { Logger } from "pino";
import type { TerminalManager } from "../../terminal/terminal-manager.js";
import {
  getCheckoutIdentity,
  getWorktreeRoot,
  resolveAbsoluteGitDir,
  resolveGitCommonDir,
} from "../../utils/checkout-git.js";
import {
  deletePaseoWorktree,
  isPaseoOwnedWorktreeCwd,
  readWorktreeHeadReference,
  runWorktreeTeardownCommands,
} from "../../utils/worktree.js";
import {
  readPaseoWorktreeMetadata,
  setPaseoWorktreeUntrustedSource,
} from "../../utils/worktree-metadata.js";
import { validateWorktreePath } from "../../utils/worktree-path.js";
import { createWorkspaceRecoveryService } from "../session/workspace-recovery/workspace-recovery-service.js";
import { killTerminalsForWorkspace } from "../workspace-archive-service.js";
import { checkoutLiteFromGitSnapshot } from "../workspace-registry-model.js";
import type { PersistedProjectRecord, PersistedWorkspaceRecord } from "../workspace-registry.js";
import { WorkerOperationError } from "./execution-protocol.js";
import type {
  RemoteWorkspaceArchiveOperation,
  RemoteWorkspaceArchiveResult,
} from "./remote-workspace-archive-protocol.js";

interface ResidentWorkspaceArchiveOptions {
  roots: readonly string[];
  paseoHome: string;
  worktreesRoot?: string;
  logger: Logger;
  terminalManager: TerminalManager;
  stopWorkspaceSetup(workspaceId: string): Promise<void>;
  listWorkspaces(): Promise<PersistedWorkspaceRecord[]>;
  getProject(projectId: string): Promise<PersistedProjectRecord | null>;
  signal: AbortSignal;
}

function isInside(root: string, file: string) {
  const relative = path.relative(root, file);
  return relative !== ".." && !relative.startsWith(`..${path.sep}`) && !path.isAbsolute(relative);
}

function isMissing(error: unknown): boolean {
  return (
    error instanceof Error &&
    "code" in error &&
    (error.code === "ENOENT" || error.code === "ENOTDIR")
  );
}

// Resolve aliases through the nearest existing ancestor so missing paths cannot bypass confinement.
async function resolveCandidate(file: string): Promise<string> {
  if (!path.isAbsolute(file)) throw new WorkerOperationError("worker_directory_must_be_absolute");
  try {
    await lstat(file);
  } catch (error) {
    const parent = path.dirname(file);
    if (!isMissing(error) || parent === file) throw error;
    return path.join(await resolveCandidate(parent), path.basename(file));
  }
  return realpath(file);
}

export function createResidentWorkspaceArchive(options: ResidentWorkspaceArchiveOptions) {
  const { paseoHome, worktreesRoot, signal } = options;
  const ownership = { paseoHome, worktreesRoot };

  async function checkedPath(file: string, missing = false) {
    const roots = await Promise.all(options.roots.map((root) => realpath(root)));
    if (!path.isAbsolute(file)) throw new WorkerOperationError("worker_directory_must_be_absolute");
    const resolved = await (missing ? resolveCandidate(file) : realpath(file));
    if (!roots.some((root) => isInside(root, resolved))) {
      throw new WorkerOperationError("worker_workspace_root_denied");
    }
    return resolved;
  }

  async function isDirectory(file: string): Promise<boolean> {
    try {
      return (await stat(await checkedPath(file))).isDirectory();
    } catch (error) {
      if (!isMissing(error)) throw error;
      await checkedPath(file, true);
      return false;
    }
  }

  async function checkConfig(checkout: string, cwd: string): Promise<void> {
    const config = await checkedPath(path.join(cwd, "paseo.json"), true);
    if (!isInside(checkout, config)) throw new WorkerOperationError("worker_workspace_root_denied");
    try {
      if (!(await stat(config)).isFile())
        throw new WorkerOperationError("worker_worktree_config_not_file");
    } catch (error) {
      if (!isMissing(error)) throw error;
    }
  }

  async function checkGitDirectory(cwd: string) {
    const root = await getWorktreeRoot(cwd);
    const git = await resolveAbsoluteGitDir(cwd);
    const common = await resolveGitCommonDir(cwd);
    if (!root || !git || !common || path.relative(cwd, await checkedPath(root)) !== "") {
      throw new WorkerOperationError("worker_worktree_repository_mismatch");
    }
    await checkedPath(git);
    return checkedPath(common);
  }

  async function findWorkspace(workspaceId: string) {
    return (
      (await options.listWorkspaces()).find((record) => record.workspaceId === workspaceId) ?? null
    );
  }

  async function requireWorkspace(workspaceId: string, archivedAt?: string) {
    const record = await findWorkspace(workspaceId);
    if (!record || record.archivedAt !== (archivedAt ?? null)) {
      throw new WorkerOperationError("worker_workspace_archive_stamp_mismatch");
    }
    return record;
  }

  async function refersToCheckout(record: PersistedWorkspaceRecord, checkout: string) {
    for (const file of [record.worktreeRoot, record.cwd]) {
      if (!file) continue;
      const resolved = await resolveCandidate(file).catch((error: unknown) => {
        if (isMissing(error)) return file;
        throw error;
      });
      if (isInside(checkout, resolved)) return true;
    }
    return false;
  }

  async function activeReferences(checkout: string) {
    const refs: PersistedWorkspaceRecord[] = [];
    for (const workspace of await options.listWorkspaces()) {
      if (!workspace.archivedAt && (await refersToCheckout(workspace, checkout)))
        refs.push(workspace);
    }
    return refs;
  }

  async function prepare(
    workspaceId: string,
  ): Promise<RemoteWorkspaceArchiveResult<"workspaceArchivePrepare">> {
    const workspace = await requireWorkspace(workspaceId);
    signal.throwIfAborted();
    await options.stopWorkspaceSetup(workspaceId);
    await killTerminalsForWorkspace(
      { terminalManager: options.terminalManager, sessionLogger: options.logger },
      workspaceId,
    );
    if (workspace.kind !== "worktree") return { head: null };
    const worktreeRoot = workspace.worktreeRoot ?? workspace.cwd;
    if (!(await isDirectory(worktreeRoot))) return { head: null };
    const checkout = await checkedPath(worktreeRoot);
    await checkGitDirectory(checkout);
    return { head: await readWorktreeHeadReference(checkout) };
  }

  async function remove(
    workspaceId: string,
    archivedAt: string,
  ): Promise<RemoteWorkspaceArchiveResult<"workspaceArchiveRemove">> {
    const workspace = await requireWorkspace(workspaceId, archivedAt);
    const { mainRepoRoot, worktreeRoot } = workspace;
    if (workspace.kind !== "worktree" || !worktreeRoot || !(await isDirectory(worktreeRoot))) {
      return { removedDirectory: false };
    }
    const pathToCheckout = await checkedPath(worktreeRoot);
    const retained = (reason: string) => ({
      removedDirectory: false,
      directoryLeftBehind: { path: worktreeRoot, reason },
    });
    if (!workspace.isPaseoOwnedWorktree || !mainRepoRoot || workspace.worktreeManagerPluginId) {
      return retained("The checkout has no stock Paseo removal ownership proof.");
    }
    const repoRoot = await checkedPath(mainRepoRoot);
    if (
      path.relative(repoRoot, pathToCheckout) === "" ||
      (await checkGitDirectory(repoRoot)) !== (await checkGitDirectory(pathToCheckout))
    ) {
      return retained("The checkout does not match its source repository.");
    }
    const owned = await isPaseoOwnedWorktreeCwd(pathToCheckout, ownership);
    if (
      !owned.allowed ||
      !owned.worktreePath ||
      path.relative(pathToCheckout, owned.worktreePath) !== ""
    ) {
      return retained("The checkout has no stock Paseo removal ownership proof.");
    }
    await validateWorktreePath({
      candidate: pathToCheckout,
      repoPath: repoRoot,
      allowExistingPath: true,
    });
    const metadata = await readPaseoWorktreeMetadata(pathToCheckout);
    const references = await activeReferences(pathToCheckout);
    const archivedPeers: PersistedWorkspaceRecord[] = [];
    for (const record of await options.listWorkspaces()) {
      if (record.archivedAt === archivedAt && (await refersToCheckout(record, pathToCheckout))) {
        archivedPeers.push(record);
      }
    }
    const untrusted =
      metadata?.untrustedSource ??
      workspace.untrustedSource ??
      [...references, ...archivedPeers].find((item) => item.untrustedSource)?.untrustedSource;
    signal.throwIfAborted();
    if (untrusted && !metadata?.untrustedSource) {
      await setPaseoWorktreeUntrustedSource(pathToCheckout, untrusted);
    }
    if (references.length) return { removedDirectory: false };
    if (!untrusted) {
      const completedCwds = new Set<string>();
      for (const peer of archivedPeers) {
        if (!(await isDirectory(peer.cwd))) continue;
        const cwd = await checkedPath(peer.cwd);
        if (completedCwds.has(cwd)) continue;
        if (!isInside(pathToCheckout, cwd))
          return retained("The workspace directory is outside the checkout.");
        await checkConfig(pathToCheckout, cwd);
        signal.throwIfAborted();
        try {
          await runWorktreeTeardownCommands({
            worktreePath: pathToCheckout,
            teardownCwd: cwd,
            repoRootPath: repoRoot,
            async prepareCommand(command) {
              signal.throwIfAborted();
              return command;
            },
          });
        } catch {
          return retained("Worktree teardown did not complete.");
        }
        completedCwds.add(cwd);
      }
    }
    // Disk deletion follows Central's archive write and a fresh reference/stamp check.
    await requireWorkspace(workspaceId, archivedAt);
    if ((await activeReferences(pathToCheckout)).length) return { removedDirectory: false };
    signal.throwIfAborted();
    await deletePaseoWorktree({
      cwd: repoRoot,
      worktreePath: pathToCheckout,
      paseoHome,
      worktreesBaseRoot: worktreesRoot,
      teardownCwds: [],
      removeReason: "archive",
      untrusted: Boolean(untrusted),
    });
    return { removedDirectory: true };
  }

  function recovery(
    unarchiveWorkspace: (record: PersistedWorkspaceRecord) => Promise<void>,
    known?: PersistedWorkspaceRecord,
  ) {
    return createWorkspaceRecoveryService({
      ...ownership,
      getWorkspace: known ? async () => known : findWorkspace,
      getProject: options.getProject,
      isDirectory,
      unarchiveWorkspace,
      isCheckoutUntrusted: async (checkout) =>
        (await activeReferences(checkout)).some((record) => record.untrustedSource),
    });
  }

  async function restore(
    workspaceId: string,
    archivedAt: string,
  ): Promise<RemoteWorkspaceArchiveResult<"workspaceRecoveryRestore">> {
    const record = await requireWorkspace(workspaceId, archivedAt);
    if (record.kind === "worktree" && !(await isDirectory(record.cwd))) {
      const project = await options.getProject(record.projectId);
      const repo = record.mainRepoRoot ?? project?.rootPath;
      if (repo && (await isDirectory(repo))) {
        const repoRoot = await checkedPath(repo);
        await checkGitDirectory(repoRoot);
        if (
          !record.isPaseoOwnedWorktree ||
          !record.worktreeRoot ||
          record.worktreeManagerPluginId
        ) {
          throw new WorkerOperationError("worker_worktree_restore_ownership_denied");
        }
        const destination = await checkedPath(record.worktreeRoot, true);
        // The stock restore uses the recorded spelling. Insist it still names this destination.
        await validateWorktreePath({ candidate: record.worktreeRoot, repoPath: repoRoot });
        const cwd = await checkedPath(record.cwd, true);
        if (!isInside(destination, cwd))
          throw new WorkerOperationError("worker_workspace_root_denied");
        await checkConfig(repoRoot, repoRoot);
      }
    }
    signal.throwIfAborted();
    let recovered:
      | Omit<RemoteWorkspaceArchiveResult<"workspaceRecoveryRestore">, "action">
      | undefined;
    const { action } = await recovery(async (workspace) => {
      const cwd = await checkedPath(workspace.cwd);
      const observedRoot = await getWorktreeRoot(cwd);
      if (observedRoot) await checkGitDirectory(await checkedPath(observedRoot));
      const identity = await getCheckoutIdentity(cwd, ownership);
      let untrustedSource = workspace.untrustedSource;
      if (identity.isGit) {
        const worktree = await checkedPath(identity.repoRoot ?? cwd);
        await checkGitDirectory(worktree);
        if (identity.mainRepoRoot) await checkedPath(identity.mainRepoRoot);
        if (identity.isPaseoOwnedWorktree) {
          const metadata = await readPaseoWorktreeMetadata(worktree);
          const activeTrust = (await activeReferences(worktree)).find(
            (item) => item.untrustedSource,
          )?.untrustedSource;
          untrustedSource =
            metadata?.untrustedSource ?? activeTrust ?? (metadata ? undefined : untrustedSource);
        }
      }
      recovered = {
        checkout: checkoutLiteFromGitSnapshot(
          cwd,
          identity.isGit
            ? { ...identity, remoteUrl: null }
            : {
                isGit: false,
                currentBranch: null,
                remoteUrl: null,
                repoRoot: null,
                mainRepoRoot: null,
                isPaseoOwnedWorktree: false,
              },
        ),
        untrustedSource,
        worktreeManagerPluginId: workspace.worktreeManagerPluginId ?? null,
      };
    }, record).restore(workspaceId);
    if (!recovered) throw new WorkerOperationError("worker_workspace_recovery_missing_result");
    return { action, ...recovered };
  }

  return {
    async execute(operation: RemoteWorkspaceArchiveOperation) {
      try {
        switch (operation.kind) {
          case "workspaceArchivePrepare":
            return await prepare(operation.workspaceId);
          case "workspaceArchiveRemove":
            return await remove(operation.workspaceId, operation.archivedAt);
          case "workspaceRecoveryInspect":
            return await recovery(async () => {}).inspect(operation.workspaceId);
          case "workspaceRecoveryRestore":
            return await restore(operation.workspaceId, operation.archivedAt);
        }
      } catch (error) {
        if (error instanceof WorkerOperationError) throw error;
        if (signal.aborted) throw new WorkerOperationError("worker_connection_lost");
        throw new WorkerOperationError("worker_workspace_lifecycle_failed", { cause: error });
      }
    },
  };
}
