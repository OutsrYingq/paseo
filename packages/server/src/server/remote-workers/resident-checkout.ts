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
import {
  createGitMetadataGenerator,
  type StructuredTextGeneration,
} from "../session/checkout/git-metadata-generator.js";
import { createGitMutationService } from "../session/git-mutation/git-mutation-service.js";
import type { CheckoutQuery } from "../session/checkout/checkout-queries-protocol.js";
import { WorkspaceGitServiceImpl } from "../workspace-git-service.js";
import { createWorkspaceRuntimeStore } from "../workspace-runtime-store/index.js";
import { resolveResidentWorkspacePath } from "./resident-projects.js";
import { WorkerOperationError } from "./execution-protocol.js";
import { openResidentDiffChannel } from "./resident-diff.js";
import type { RemoteDiffEvent } from "./remote-diff-protocol.js";
import { prepareResidentCheckoutAction } from "./resident-checkout-scope.js";

export function openResidentCheckoutQueries(options: {
  roots: readonly string[];
  paseoHome: string;
  logger: Logger;
  signal: AbortSignal;
  publish(event: RemoteDiffEvent): void;
  generation?: StructuredTextGeneration;
  readHostInstructions?: () => string | undefined;
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
  const gitMetadataGenerator = options.generation
    ? createGitMetadataGenerator({
        workspaceGitService: git,
        generation: options.generation,
        readHostInstructions: options.readHostInstructions,
        logger,
      })
    : undefined;
  return {
    diff: channel.execute,
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
        gitMetadataGenerator,
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
