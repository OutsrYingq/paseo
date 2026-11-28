import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { expect, test, vi } from "vitest";
import { AgentManager } from "../agent/agent-manager.js";
import { AgentStorage } from "../agent/agent-storage.js";
import { serializeAgentSnapshot } from "../messages.js";
import { createTestAgentClient } from "../test-utils/fake-agent-client.js";
import { createTestLogger } from "../../test-utils/test-logger.js";
import { createCentralHistory } from "./central-history.js";

test("scopes fork context and rewind to the worker's original agent timeline", async () => {
  const cwd = await mkdtemp(path.join(tmpdir(), "paseo-central-history-"));
  const logger = createTestLogger();
  const storage = new AgentStorage(path.join(cwd, "agents"), logger);
  const manager = new AgentManager({
    logger,
    clients: { codex: createTestAgentClient("codex") },
    registry: storage,
  });
  try {
    await storage.initialize();
    const history = createCentralHistory({
      logger,
      agentManager: manager,
      agentStorage: storage,
      providerSnapshotManager: { getProviderLabel: () => "Codex" },
      workspaceProvisioning: { runInImportWorkspace: vi.fn() },
      agentPayload: async (snapshot) => serializeAgentSnapshot(snapshot, { title: "Source" }),
    });
    const agent = await manager.createAgent({ provider: "codex", cwd }, undefined, {
      workspaceId: undefined,
    });
    const cursor = await manager.appendTimelineItem(agent.id, {
      type: "user_message",
      messageId: "boundary",
      text: "Include this",
    });
    await manager.appendTimelineItem(agent.id, {
      type: "assistant_message",
      text: "Leave this out",
    });
    const request = {
      type: "agent.fork_context.request" as const,
      requestId: "context",
      agentId: agent.id,
      boundaryCursor: cursor,
    };
    const response = await history(request);
    expect(response).toMatchObject({
      type: "agent.fork_context.response",
      payload: {
        requestId: "context",
        agentId: agent.id,
        itemCount: 1,
        boundaryCursor: cursor,
        attachment: { text: expect.stringContaining("Include this") },
        error: null,
      },
    });
    if (response.type !== "agent.fork_context.response") throw new Error("Unexpected response");
    expect(response.payload.attachment?.text).not.toContain("Leave this out");
    expect(
      await history({ ...request, boundaryCursor: { ...cursor, epoch: "stale" } }),
    ).toMatchObject({
      payload: { attachment: null, error: expect.any(String) },
    });

    const missingId = "01800000-0000-7000-8000-000000000013";
    const rewind = vi.spyOn(manager, "rewind").mockResolvedValue(undefined);
    const rewindRequest = {
      type: "agent.rewind.request" as const,
      requestId: "rewind",
      agentId: missingId,
      mode: "conversation" as const,
      messageId: "boundary",
    };
    expect(await history(rewindRequest)).toMatchObject({
      type: "agent.rewind.response",
      payload: { agentId: missingId, ok: false },
    });
    expect(rewind).not.toHaveBeenCalled();
    expect(await history({ ...rewindRequest, agentId: agent.id })).toMatchObject({
      payload: { agentId: agent.id, ok: true },
    });
    expect(rewind).toHaveBeenCalledExactlyOnceWith(agent.id, "boundary", "conversation");
  } finally {
    await Promise.all(manager.listAgents().map(({ id }) => manager.closeAgent(id)));
    await manager.flushForShutdown();
    await storage.flush();
    await rm(cwd, { recursive: true, force: true });
  }
});
