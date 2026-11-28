import type { SessionInboundMessage, SessionOutboundMessage } from "@getpaseo/protocol/messages";
import { getErrorMessage } from "@getpaseo/protocol/error-utils";
import type { Logger } from "pino";
import type { AgentManager } from "../../agent/agent-manager.js";
import { ensureAgentLoaded } from "../../agent/agent-loading.js";
import type { AgentStorage } from "../../agent/agent-storage.js";
import { foldFeatureValues } from "../../agent/option-controls.js";

export async function listAgentCommands(
  request: Extract<SessionInboundMessage, { type: "list_commands_request" }>,
  dependencies: {
    agentManager: AgentManager;
    agentStorage: AgentStorage;
    logger: Logger;
    resolveCwd(cwd: string): string | Promise<string>;
  },
): Promise<Extract<SessionOutboundMessage, { type: "list_commands_response" }>> {
  const { agentId, requestId, draftConfig } = request;
  const { agentManager, agentStorage, logger, resolveCwd } = dependencies;
  const response = (
    commands: Awaited<ReturnType<AgentManager["listDraftCommands"]>>,
    error: string | null,
  ) => {
    const seen = new Set<string>();
    const unique = commands.filter(({ name }) => {
      if (seen.has(name)) return false;
      seen.add(name);
      return true;
    });
    return {
      type: "list_commands_response" as const,
      payload: {
        agentId,
        requestId,
        commands: unique,
        error,
      },
    };
  };
  try {
    const existing = agentManager.getAgent(agentId);
    const stored = existing ? null : await agentStorage.get(agentId);
    const agent =
      existing || (stored && !stored.archivedAt)
        ? await ensureAgentLoaded(agentId, dependencies)
        : null;
    if (agent?.session?.listCommands) {
      return response(await agent.session.listCommands(), null);
    }
    if (!agent && draftConfig) {
      return response(
        await agentManager.listDraftCommands({
          ...foldFeatureValues(draftConfig),
          cwd: await resolveCwd(draftConfig.cwd),
        }),
        null,
      );
    }
    return response(
      [],
      agent ? "Agent does not support listing commands" : `Agent not found: ${agentId}`,
    );
  } catch (error) {
    logger.error({ err: error, agentId, provider: draftConfig?.provider }, "Failed to list commands");
    return response([], getErrorMessage(error));
  }
}
