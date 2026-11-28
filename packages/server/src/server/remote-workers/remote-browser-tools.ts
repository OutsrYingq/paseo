import { z } from "zod";
import type { BrowserToolsBroker } from "../browser-tools/broker.js";
import { visitBrowserTools } from "../browser-tools/tools.js";
import type { RemoteBrowserEvent, RemoteBrowserResult } from "./remote-browser-protocol.js";

export function visitRemoteBrowserTools(visit: Parameters<typeof visitBrowserTools>[0]): void {
  visitBrowserTools((name, config, run) => {
    // The stock Desktop resolves upload paths on its own disk, not on the worker.
    if (name !== "browser_upload") visit(name, config, run);
  });
}

type BrowserAgent = { id: string; cwd: string; workspaceId?: string };
type BrowserRunner = Parameters<Parameters<typeof visitBrowserTools>[0]>[2];
const tools = new Map<string, { schema: z.ZodType; run: BrowserRunner }>();
visitRemoteBrowserTools((name, { inputSchema }, run) => {
  const schema = inputSchema instanceof z.ZodType ? inputSchema : z.object(inputSchema ?? {});
  tools.set(name, { schema, run });
});

export function createCentralBrowserTools({
  broker,
  resolveAgent,
}: {
  broker: BrowserToolsBroker;
  resolveAgent(agentId: string): BrowserAgent | null | undefined;
}) {
  return {
    async execute(event: Pick<RemoteBrowserEvent, "agentId" | "tool"> & { input: unknown }): Promise<RemoteBrowserResult> {
      const tool = tools.get(event.tool);
      if (!tool) return { ok: false, error: "worker_browser_tool_unsupported" };
      const input = tool.schema.safeParse(event.input);
      if (!input.success) return { ok: false, error: input.error.message };
      const agent = resolveAgent(event.agentId);
      if (!agent || agent.id !== event.agentId) return { ok: false, error: "agent_not_found" };
      if (!agent.workspaceId && event.tool !== "browser_list_hosts") {
        return { ok: false, error: "Browser tools require an agent workspace." };
      }
      return {
        ok: true,
        value: await tool.run({ broker, callerAgentId: agent.id, resolveCallerAgent: () => agent }, input.data),
      };
    },
  };
}
