import type { Logger } from "pino";
import type { SessionOutboundMessage } from "@getpaseo/protocol/messages";
import { createForgeResolver } from "../../services/forge-resolver.js";
import { createGitHubService } from "../../services/github-service.js";
import { renameCurrentBranch } from "../../utils/checkout-git.js";
import { expandTilde } from "../../utils/path.js";
import { CheckoutDiffManager } from "../checkout-diff-manager.js";
import { CheckoutQuerySession, CheckoutSession } from "../session/checkout/checkout-session.js";
import {
  CheckoutActionResponseSchema,
  type CheckoutAction,
  type CheckoutActionResult,
} from "../session/checkout/checkout-actions-protocol.js";
import { createGitMetadataPrompts } from "../session/checkout/git-metadata-generator.js";
import type { StructuredGenerationDaemonConfig } from "../agent/structured-generation-providers.js";
import { createGitMutationService } from "../session/git-mutation/git-mutation-service.js";
import type { CheckoutQuery } from "../session/checkout/checkout-queries-protocol.js";
import { WorkspaceGitServiceImpl } from "../workspace-git-service.js";
import { createWorkspaceRuntimeStore } from "../workspace-runtime-store/index.js";
import { resolveResidentWorkspacePath } from "./resident-projects.js";
import { WorkerOperationError, type WorkerOperation } from "./execution-protocol.js";
import { openResidentDiffChannel } from "./resident-diff.js";
import type { RemoteDiffEvent } from "./remote-diff-protocol.js";
import {
  prepareResidentCheckoutAction,
  validateResidentCheckout,
} from "./resident-checkout-scope.js";

export function openResidentCheckoutQueries(options: {
  roots: readonly string[];
  paseoHome: string;
  logger: Logger;
  signal: AbortSignal;
  publish(event: RemoteDiffEvent): void;
  readMetadataGeneration(): StructuredGenerationDaemonConfig["metadataGeneration"];
}) {
  const { paseoHome, logger, signal, roots } = options;
  const git = new WorkspaceGitServiceImpl({
    paseoHome,
    logger,
    deps: { forgeResolver: createForgeResolver() },
  });
  const diffs = new CheckoutDiffManager({
    logger,
    paseoHome,
    workspaceGitService: git,
    workspaceRuntimeStore: createWorkspaceRuntimeStore(),
  });
  const channel = openResidentDiffChannel({ ...options, diffs: diffs.streams, legacy: diffs });
  const gitMutation = createGitMutationService({ workspaceGitService: git, logger });
  const github = createGitHubService();
  const prompts = createGitMetadataPrompts({
    workspaceGitService: git,
    readHostInstructions: () => options.readMetadataGeneration()?.instructions,
  });
  return {
    diff: channel.execute,
    async prepareMetadata(operation: Extract<WorkerOperation, { kind: "checkoutMetadata" }>) {
      const cwd = await resolveResidentWorkspacePath(roots, expandTilde(operation.cwd));
      await validateResidentCheckout(roots, cwd, operation.baseRef);
      signal.throwIfAborted();
      const prompt = operation.mode === "commit"
        ? await prompts.commit(cwd)
        : await prompts.pullRequest(cwd, operation.baseRef);
      const providers = options.readMetadataGeneration()?.providers;
      return { cwd, prompt, metadataGeneration: providers ? { providers } : undefined };
    },
    async query(request: CheckoutQuery) {
      const cwd = await resolveResidentWorkspacePath(roots, expandTilde(request.cwd));
      signal.throwIfAborted();
      let response: SessionOutboundMessage | undefined;
      const session = new CheckoutQuerySession({ emit: (message) => { response = message; } }, git, diffs, paseoHome, undefined);
      await session.dispatch({ ...request, cwd }, signal);
      if (!response) throw new WorkerOperationError("worker_checkout_query_unavailable");
      return response;
    },
    async action(request: CheckoutAction): Promise<CheckoutActionResult> {
      const cwd = await resolveResidentWorkspacePath(roots, expandTilde(request.cwd));
      const scope = await prepareResidentCheckoutAction(roots, { ...request, cwd });
      signal.throwIfAborted();
      let response: SessionOutboundMessage | undefined;
      let workspaceUpdate = false;
      let branchSnapshot: CheckoutActionResult["branchSnapshot"];
      const session = new CheckoutSession({
        host: {
          emit: (message) => { response = message; },
          emitWorkspaceUpdateForCwd: async () => { workspaceUpdate = true; },
          handleWorkspaceGitBranchSnapshot: (_cwd, branchName) => { branchSnapshot = { branchName }; },
          renameCurrentBranch,
        },
        gitMutation,
        workspaceGitService: git,
        github,
        checkoutDiffManager: diffs,
        ...scope,
        paseoHome,
        worktreesRoot: undefined,
        logger,
      });
      await session.dispatchAction({ ...request, cwd });
      if (workspaceUpdate && !branchSnapshot) {
        branchSnapshot = { branchName: (await git.getCheckout(cwd)).currentBranch };
      }
      return {
        response: CheckoutActionResponseSchema.parse(response),
        workspaceUpdate,
        branchSnapshot,
      };
    },
    async close() {
      await channel.close();
      try {
        await diffs.dispose();
      } finally {
        await git.dispose();
      }
    },
  };
}
