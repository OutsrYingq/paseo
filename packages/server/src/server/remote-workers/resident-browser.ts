import { randomUUID } from "node:crypto";
import type { CallToolResult, StandardSchemaWithJSON } from "@modelcontextprotocol/server";
import type { ProviderPaseoToolsPolicy } from "@getpaseo/protocol/provider-config";
import { z } from "zod";
import { isPaseoToolEnabled } from "../agent/paseo-tool-policy.js";
import { addModelVisibleStructuredContent } from "../agent/tools/paseo-tool-serialization.js";
import type { ResidentAgentMcp, ResidentMcpBinding } from "./resident-voice-mcp.js";
import {
  RemoteBrowserEvents,
  type RemoteBrowserEvent,
  type RemoteBrowserOperation,
  type RemoteBrowserResult,
} from "./remote-browser-protocol.js";
import { visitRemoteBrowserTools } from "./remote-browser-tools.js";

type BrowserBind = Extract<RemoteBrowserOperation, { kind: "browserBind" }>;
type BrowserBinding = {
  agentId: string;
  ready: Promise<ResidentMcpBinding["descriptor"]>;
  registration?: ResidentMcpBinding;
};
const toolInput = RemoteBrowserEvents[0].shape.input;
const tools = new Map<string, { title?: string; description?: string; inputSchema: StandardSchemaWithJSON }>();
visitRemoteBrowserTools((name, { title, description, inputSchema }) => {
  const schema = inputSchema instanceof z.ZodType ? inputSchema : z.object(inputSchema ?? {});
  // The SDK resolves its schema type through zod/v4; both entry points share a runtime.
  tools.set(name, { title, description, inputSchema: schema as StandardSchemaWithJSON });
});

export function openResidentBrowser({
  mcp,
  publish,
  resolvePolicy,
  signal,
}: {
  mcp: ResidentAgentMcp;
  publish(event: RemoteBrowserEvent): void;
  resolvePolicy(provider: BrowserBind["provider"]): {
    enabled: boolean;
    providerPolicy?: ProviderPaseoToolsPolicy;
  };
  signal: AbortSignal;
}) {
  const bindings = new Map<string, BrowserBinding>();
  const pending = new Map<string, { agentId: string; finish(result: RemoteBrowserResult): void }>();
  let closed = false;

  function unbind(agentId: string) {
    bindings.get(agentId)?.registration?.revoke();
    bindings.delete(agentId);
    for (const request of pending.values()) {
      if (request.agentId === agentId) request.finish({ ok: false, error: "worker_browser_agent_unbound" });
    }
  }

  function close() {
    if (closed) return;
    closed = true;
    signal.removeEventListener("abort", close);
    for (const agentId of bindings.keys()) unbind(agentId);
  }
  if (signal.aborted) close();
  else signal.addEventListener("abort", close, { once: true });

  function request(binding: BrowserBinding, tool: string, input: unknown, callSignal?: AbortSignal) {
    const { agentId } = binding;
    if (bindings.get(agentId) !== binding || callSignal?.aborted) {
      throw new Error("worker_browser_binding_closed");
    }
    const args = toolInput.parse(input);
    const requestId = randomUUID();
    return new Promise<CallToolResult>((resolve, reject) => {
      const abort = () => finish({ ok: false, error: "worker_browser_call_cancelled" });
      const finish = (result: RemoteBrowserResult) => {
        if (!pending.delete(requestId)) return;
        clearTimeout(timeout);
        callSignal?.removeEventListener("abort", abort);
        if (result.ok) {
          // The stock browser tool catalog constructs the MCP content blocks.
          resolve(addModelVisibleStructuredContent(result.value) as CallToolResult);
        } else {
          reject(new Error(result.error));
        }
      };
      // The stock longest browser_wait is 30s plus a 1s broker margin.
      const timeout = setTimeout(
        () => finish({ ok: false, error: "worker_browser_call_timeout" }),
        35_000,
      );
      pending.set(requestId, { agentId, finish });
      callSignal?.addEventListener("abort", abort, { once: true });
      try {
        publish({ type: "browserAction", agentId, requestId, tool, input: args });
      } catch (error) {
        finish({ ok: false, error: error instanceof Error ? error.message : String(error) });
      }
    });
  }

  function bind(agentId: string, provider: BrowserBind["provider"]) {
    if (closed) throw new Error("worker_browser_binding_closed");
    const existing = bindings.get(agentId);
    if (existing) return existing.ready;
    const binding: BrowserBinding = {
      agentId,
      ready: mcp.start().then(() => {
        if (bindings.get(agentId) !== binding) throw new Error("worker_browser_binding_closed");
        binding.registration = mcp.registerBrowser(agentId, (server) => {
          const { enabled, providerPolicy } = resolvePolicy(provider);
          if (!enabled) return;
          for (const [tool, config] of tools) {
            if (!isPaseoToolEnabled(providerPolicy, tool)) continue;
            server.registerTool(tool, config, async (input: unknown, context) => {
              const policy = resolvePolicy(provider);
              if (!policy.enabled || !isPaseoToolEnabled(policy.providerPolicy, tool))
                throw new Error("worker_browser_tool_disabled");
              return request(binding, tool, input, context?.mcpReq?.signal);
            });
          }
        });
        return binding.registration.descriptor;
      }),
    };
    bindings.set(agentId, binding);
    return binding.ready;
  }

  return {
    async execute(operation: RemoteBrowserOperation) {
      switch (operation.kind) {
        case "browserBind": {
          const { agentId, provider } = operation;
          const { enabled, providerPolicy } = resolvePolicy(provider);
          // Remote OMP has no HTTP MCP bridge for an endpoint on the worker.
          if (provider !== "codex" || !enabled || providerPolicy?.enabled === false) {
            unbind(agentId);
            return null;
          }
          return bind(agentId, provider);
        }
        case "browserUnbind":
          unbind(operation.agentId);
          return null;
        case "browserResult":
          pending.get(operation.requestId)?.finish(operation.result);
          return null;
      }
    },
    close,
  };
}
