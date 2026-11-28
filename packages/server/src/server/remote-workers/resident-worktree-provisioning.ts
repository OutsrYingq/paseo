import { lstat, realpath, stat } from "node:fs/promises";
import path from "node:path";
import type { Logger } from "pino";
import { createForgeResolver } from "../../services/forge-resolver.js";
import type { ForgeService } from "../../services/forge-service.js";
import { createGitHubService } from "../../services/github-service.js";
import {
  getWorktreeRoot,
  resolveAbsoluteGitDir,
  resolveGitCommonDir,
} from "../../utils/checkout-git.js";
import { createRealpathAwarePathMatcher, expandTilde } from "../../utils/path.js";
import {
  InvalidGitBranchNameError,
  mapWorkspaceRelativeCwdToWorktree,
  resolvePaseoWorktreesBaseRoot,
  rollbackCreatedPaseoWorktree,
  seedPaseoConfigFile,
} from "../../utils/worktree.js";
import {
  readPaseoWorktreeMetadata,
  setPaseoWorktreeUntrustedSource,
} from "../../utils/worktree-metadata.js";
import { validateWorktreePath } from "../../utils/worktree-path.js";
import type { TerminalManager } from "../../terminal/terminal-manager.js";
import {
  CheckoutSourceForgeMismatchError,
  UnsupportedForgeCheckoutTargetError,
} from "../resolve-worktree-creation-intent.js";
import { WorkspaceGitServiceImpl } from "../workspace-git-service.js";
import type { PersistedWorkspaceRecord } from "../workspace-registry.js";
import type { WorkspaceSetupOperation } from "../workspace-setup-runtime.js";
import { buildWorktreeSetupDetail } from "../worktree-bootstrap.js";
import { createWorktreeCore } from "../worktree-core.js";
import { toWorktreeWireError } from "../worktree-errors.js";
import { runWorktreeSetupInBackground } from "../worktree-session.js";
import { WorkerOperationError } from "./execution-protocol.js";
import type {
  RemoteWorktreeMaterialization,
  RemoteWorktreeProvisioningOperation,
  RemoteWorktreeSetupEvent,
} from "./remote-worktree-provisioning-protocol.js";

type MaterializeRequest = Extract<
  RemoteWorktreeProvisioningOperation,
  { kind: "worktreeMaterialize" }
>;

interface MaterializeOptions {
  roots: readonly string[];
  paseoHome: string;
  worktreesRoot?: string;
  logger: Logger;
  github?: ForgeService;
  forgeResolver?: ReturnType<typeof createForgeResolver>;
}

function isInside(root: string, candidate: string): boolean {
  const relative = path.relative(root, candidate);
  return relative !== ".." && !relative.startsWith(`..${path.sep}`) && !path.isAbsolute(relative);
}

function hasCode(error: unknown, code: string): boolean {
  return error instanceof Error && "code" in error && error.code === code;
}

async function checkPath(roots: readonly string[], candidate: string): Promise<string> {
  if (!path.isAbsolute(candidate))
    throw new WorkerOperationError("worker_directory_must_be_absolute");
  const resolved = await realpath(candidate);
  if (!roots.some((root) => isInside(root, resolved))) {
    throw new WorkerOperationError("worker_workspace_root_denied");
  }
  return resolved;
}

async function checkDirectory(roots: readonly string[], candidate: string): Promise<string> {
  const resolved = await checkPath(roots, candidate);
  if (!(await stat(resolved)).isDirectory()) {
    throw new WorkerOperationError("worker_directory_not_directory");
  }
  return resolved;
}

async function checkDestination(roots: readonly string[], candidate: string): Promise<string> {
  try {
    await lstat(candidate);
  } catch (error) {
    const parent = path.dirname(candidate);
    if (!hasCode(error, "ENOENT") || parent === candidate) throw error;
    return path.join(await checkDestination(roots, parent), path.basename(candidate));
  }
  // In particular, a dangling link must fail rather than count as a missing component.
  return checkPath(roots, candidate);
}

async function checkGitStorage(roots: readonly string[], cwd: string): Promise<string> {
  const [gitDir, commonDir] = await Promise.all([
    resolveAbsoluteGitDir(cwd),
    resolveGitCommonDir(cwd),
  ]);
  if (!gitDir || !commonDir) throw new WorkerOperationError("worker_checkout_not_git");
  await checkDirectory(roots, gitDir);
  return checkDirectory(roots, commonDir);
}

async function checkPaseoConfig(roots: readonly string[], cwd: string): Promise<void> {
  const config = path.join(cwd, "paseo.json");
  try {
    await lstat(config);
  } catch (error) {
    if (hasCode(error, "ENOENT")) return;
    throw error;
  }
  const resolved = await checkPath(roots, config);
  if (!(await stat(resolved)).isFile()) {
    throw new WorkerOperationError("worker_worktree_config_not_file");
  }
}

export async function materializeResidentWorktree(
  options: MaterializeOptions,
  request: MaterializeRequest,
  signal?: AbortSignal,
): Promise<RemoteWorktreeMaterialization> {
  const { paseoHome, worktreesRoot, logger } = options;
  const github = options.github ?? createGitHubService();
  const git = new WorkspaceGitServiceImpl({
    paseoHome,
    worktreesRoot,
    logger,
    deps: { forgeResolver: options.forgeResolver ?? createForgeResolver() },
  });
  try {
    const roots = await Promise.all(options.roots.map((root) => realpath(root)));
    const sourceCwd = await checkDirectory(roots, expandTilde(request.cwd));
    const checkoutRoot = await getWorktreeRoot(sourceCwd);
    if (!checkoutRoot) throw new WorkerOperationError("worker_checkout_not_git");
    const sourceCheckout = await checkDirectory(roots, checkoutRoot);
    if (!isInside(sourceCheckout, sourceCwd)) {
      throw new WorkerOperationError("worker_workspace_root_denied");
    }
    const sourceCommonDir = await checkGitStorage(roots, sourceCheckout);
    const repoRoot = await checkDirectory(roots, await git.resolveRepoRoot(sourceCwd));
    if ((await checkGitStorage(roots, repoRoot)) !== sourceCommonDir) {
      throw new WorkerOperationError("worker_worktree_repository_mismatch");
    }
    await checkDestination(roots, resolvePaseoWorktreesBaseRoot({ paseoHome, worktreesRoot }));
    await checkPaseoConfig(roots, repoRoot);
    if (sourceCwd !== repoRoot) await checkPaseoConfig(roots, sourceCwd);
    const inheritedSource = (await readPaseoWorktreeMetadata(sourceCheckout))?.untrustedSource;
    for (const ref of [request.refName, request.branchName]) {
      if (ref?.trim().startsWith("-")) throw new WorkerOperationError("worker_worktree_ref_denied");
    }
    signal?.throwIfAborted();
    const created = await createWorktreeCore(
      { ...request, cwd: sourceCwd, paseoHome, worktreesRoot },
      {
        github,
        workspaceGitService: {
          resolveRepoRoot: async () => repoRoot,
          resolveDefaultBranch: (cwd) => git.resolveDefaultBranch(cwd),
          resolveForge: (cwd) => git.resolveForge(cwd),
        },
        lifecycle: {
          async before(_hook, input) {
            if (!("path" in input) || typeof input.path !== "string") return input;
            signal?.throwIfAborted();
            const destination = await checkDestination(roots, input.path);
            const validated = await validateWorktreePath({
              candidate: destination,
              repoPath: repoRoot,
            });
            return { ...input, path: validated };
          },
        },
      },
    );
    const { worktreePath, branchName, comparisonBaseRef } = created.worktree;
    const untrustedSource = created.untrustedSource ?? inheritedSource;
    try {
      const worktreeRoot = await checkDirectory(roots, worktreePath);
      const mappedCwd = mapWorkspaceRelativeCwdToWorktree({
        relativeWorkspaceCwd: path.relative(sourceCheckout, sourceCwd),
        targetWorktreePath: worktreeRoot,
      });
      const cwd = await checkDirectory([worktreeRoot], mappedCwd).catch((error: unknown) => {
        if (hasCode(error, "ENOENT")) {
          throw new WorkerOperationError("worker_worktree_project_directory_missing");
        }
        throw error;
      });
      if (inheritedSource && !created.untrustedSource) {
        await setPaseoWorktreeUntrustedSource(worktreeRoot, inheritedSource);
      }
      if (mappedCwd !== worktreeRoot) await seedPaseoConfigFile({ sourceCwd, targetCwd: cwd });
      github.invalidate({ cwd: worktreeRoot });
      return {
        sourceCwd,
        repoRoot,
        cwd,
        worktreeRoot,
        branch: branchName,
        baseBranch: comparisonBaseRef,
        untrustedSource,
      };
    } catch (error) {
      return await rollbackCreatedPaseoWorktree(
        {
          cwd: repoRoot,
          worktreePath,
          paseoHome,
          worktreesBaseRoot: worktreesRoot,
          teardownCwds: [],
          removeReason: "rollback",
          untrusted: Boolean(untrustedSource),
        },
        error,
      );
    }
  } catch (error) {
    if (error instanceof WorkerOperationError) throw error;
    if (signal?.aborted) throw new WorkerOperationError("worker_connection_lost");
    const wireError = toWorktreeWireError(error);
    if (
      wireError.code !== "unknown" ||
      error instanceof CheckoutSourceForgeMismatchError ||
      error instanceof UnsupportedForgeCheckoutTargetError ||
      error instanceof InvalidGitBranchNameError
    ) {
      return { error: wireError };
    }
    // Raw Git/forge errors can contain authenticated remote URLs or CLI output.
    throw new WorkerOperationError("worker_worktree_materialization_failed", { cause: error });
  } finally {
    await git.dispose();
  }
}

interface StartSetupOptions {
  roots: readonly string[];
  logger: Logger;
  terminalManager: TerminalManager;
  getWorkspace(workspaceId: string): Promise<PersistedWorkspaceRecord | null>;
  startWorkspaceSetup(workspaceId: string, operation: WorkspaceSetupOperation): void;
  publish(event: RemoteWorktreeSetupEvent): void;
}

async function resolveSetupCheckout(
  options: Pick<StartSetupOptions, "getWorkspace" | "roots">,
  workspaceId: string,
) {
  const workspace = await options.getWorkspace(workspaceId);
  if (
    !workspace ||
    workspace.archivedAt ||
    workspace.kind !== "worktree" ||
    !workspace.isPaseoOwnedWorktree ||
    !workspace.worktreeRoot ||
    !workspace.mainRepoRoot ||
    !workspace.branch
  ) {
    throw new WorkerOperationError("worker_worktree_setup_unavailable");
  }
  const roots = await Promise.all(options.roots.map((root) => realpath(root)));
  const worktreePath = await checkDirectory(roots, workspace.worktreeRoot);
  const cwd = await checkDirectory([worktreePath], workspace.cwd);
  const repoRoot = await checkDirectory(roots, workspace.mainRepoRoot);
  const checkoutRoot = await getWorktreeRoot(worktreePath);
  if (
    !checkoutRoot ||
    path.relative(worktreePath, await checkDirectory(roots, checkoutRoot)) !== "" ||
    (await checkGitStorage(roots, worktreePath)) !== (await checkGitStorage(roots, repoRoot))
  ) {
    throw new WorkerOperationError("worker_worktree_repository_mismatch");
  }
  const metadata = await readPaseoWorktreeMetadata(worktreePath);
  if (!metadata) throw new WorkerOperationError("worker_worktree_setup_unavailable");
  return { workspace, branchName: workspace.branch, worktreePath, cwd, repoRoot, metadata, roots };
}

export async function approveResidentWorktreeSetup(
  options: Pick<StartSetupOptions, "getWorkspace" | "roots"> & {
    listWorkspaces(): Promise<PersistedWorkspaceRecord[]>;
    signal: AbortSignal;
  },
  workspaceId: string,
) {
  const { worktreePath, cwd, roots } = await resolveSetupCheckout(options, workspaceId);
  await checkPaseoConfig(roots, cwd);
  const matchesCheckout = createRealpathAwarePathMatcher(worktreePath);
  const blockedWorkspaceIds = (await options.listWorkspaces())
    .filter((record) => record.untrustedSource && matchesCheckout(record.worktreeRoot ?? record.cwd))
    .map((record) => record.workspaceId);
  options.signal.throwIfAborted();
  const clearedMetadata = await setPaseoWorktreeUntrustedSource(worktreePath, null);
  return { clearedMetadata, blockedWorkspaceIds };
}

export async function startResidentWorktreeSetup(
  options: StartSetupOptions,
  workspaceId: string,
): Promise<null> {
  const { workspace, branchName, worktreePath, cwd, repoRoot, metadata, roots } =
    await resolveSetupCheckout(options, workspaceId);
  const worktree = { worktreePath, branchName };
  const blockedSource = workspace.untrustedSource ?? metadata.untrustedSource;
  if (blockedSource) {
    options.publish({
      type: "worktreeSetup",
      payload: {
        workspaceId,
        status: "blocked",
        detail: buildWorktreeSetupDetail({ worktree, results: [] }),
        error: null,
        blockedSource,
      },
    });
    return null;
  }
  await checkPaseoConfig(roots, cwd);
  options.startWorkspaceSetup(workspaceId, (signal) =>
    runWorktreeSetupInBackground(
      {
        emit(message) {
          if (message.type === "workspace_setup_progress") {
            options.publish({ type: "worktreeSetup", payload: message.payload });
          }
        },
        emitWorkspaceUpdateForWorkspaceId: async () => {},
        cacheWorkspaceSetupSnapshot() {},
        sessionLogger: options.logger,
        terminalManager: options.terminalManager,
        serviceProxy: null,
        scriptRuntimeStore: null,
        getDaemonTcpPort: null,
        getDaemonTcpHost: null,
        onScriptsChanged: null,
        lifecycle: null,
      },
      {
        requestCwd: cwd,
        repoRoot,
        workspaceId,
        worktree,
        shouldBootstrap: true,
        slug: path.basename(worktreePath),
        worktreePath,
        workspaceCwd: cwd,
      },
      signal,
    ),
  );
  return null;
}
