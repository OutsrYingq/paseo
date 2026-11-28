import { expect, test, vi } from "vitest";
import type { RemoteBrowserEvent } from "./remote-browser-protocol.js";
import { openResidentBrowser } from "./resident-browser.js";
import { createResidentVoiceMcp, type ResidentMcpBinding } from "./resident-voice-mcp.js";

test("browser tools reuse the agent MCP token, respect provider policy, and revoke in-flight calls", async () => {
  const lifetime = new AbortController();
  const mcp = createResidentVoiceMcp();
  const publish = vi.fn<(event: RemoteBrowserEvent) => void>();
  const browser = openResidentBrowser({
    mcp,
    publish,
    signal: lifetime.signal,
    resolvePolicy: () => ({ enabled: true, providerPolicy: { disabledTools: ["browser_evaluate"] } }),
  });
  const rpc = (endpoint: ResidentMcpBinding["descriptor"], method: string, params?: object) =>
    fetch(endpoint.url, {
      method: "POST",
      headers: {
        authorization: `Bearer ${endpoint.token}`,
        "content-type": "application/json",
        accept: "application/json, text/event-stream",
        "mcp-protocol-version": "2025-06-18",
      },
      body: JSON.stringify({ jsonrpc: "2.0", id: 1, method, params }),
    });
  try {
    const endpoint = await browser.execute({ kind: "browserBind", agentId: "agent-1", provider: "codex" });
    if (!endpoint) throw new Error("Browser MCP unavailable");
    const speak = mcp.register("agent-1", async () => {});
    expect(speak.descriptor).toEqual(endpoint);
    const tools = await (await rpc(endpoint, "tools/list")).text();
    expect(tools).toContain('"name":"speak"');
    expect(tools).toContain('"name":"browser_list_tabs"');
    expect(tools).not.toContain('"name":"browser_evaluate"');
    expect(tools).not.toContain('"name":"browser_upload"');
    speak.revoke();

    const call = rpc(endpoint, "tools/call", { name: "browser_list_tabs", arguments: {} });
    await vi.waitFor(() => expect(publish).toHaveBeenCalledWith(expect.objectContaining({
      type: "browserAction",
      agentId: "agent-1",
      tool: "browser_list_tabs",
      input: {},
    })));
    await browser.execute({ kind: "browserUnbind", agentId: "agent-1" });
    expect(await (await call).text()).toContain("worker_browser_agent_unbound");
    expect((await rpc(endpoint, "tools/list")).status).toBe(401);
  } finally {
    lifetime.abort();
    await mcp.close();
  }
});
