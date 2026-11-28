import { isPaseoToolEnabled } from "../agent/paseo-tool-policy.js";
import { PaseoToolInputError, type PaseoToolCatalogFactory, type PaseoToolDefinition } from "../agent/tools/types.js";
import { visitRemoteBrowserTools } from "./remote-browser-tools.js";
import type { RemoteBrowserResult } from "./remote-browser-protocol.js";
import type { ProviderPaseoToolsPolicy } from "@getpaseo/protocol/provider-config";

interface BrowserPolicy {
  enabled: boolean;
  providerPolicy?: ProviderPaseoToolsPolicy;
}

export function createBrowserToolCatalogFactory(options: {
  readPolicy(): Promise<BrowserPolicy>;
  execute(agentId: string, tool: string, input: unknown): Promise<RemoteBrowserResult>;
}): PaseoToolCatalogFactory {
  return async ({ callerAgentId }) => {
    const tools = new Map<string, PaseoToolDefinition>();
    if (callerAgentId) {
      const policy = await options.readPolicy();
      if (policy.enabled) {
        visitRemoteBrowserTools((name, config) => {
          if (!isPaseoToolEnabled(policy.providerPolicy, name)) return;
          tools.set(name, {
            ...config,
            name,
            description: config.description ?? name,
            async handler(input) {
              const policy = await options.readPolicy();
              if (!policy.enabled || !isPaseoToolEnabled(policy.providerPolicy, name)) {
                throw new PaseoToolInputError("Browser tool disabled for this provider");
              }
              const result = await options.execute(callerAgentId, name, input);
              if (!result.ok) throw new PaseoToolInputError(result.error);
              return result.value;
            },
          });
        });
      }
    }
    return {
      tools,
      getTool: (name) => tools.get(name),
      async executeTool(name, input, context = {}) {
        const tool = tools.get(name);
        if (!tool) throw new PaseoToolInputError(`Unknown browser tool: ${name}`);
        return tool.handler(input, context);
      },
    };
  };
}
