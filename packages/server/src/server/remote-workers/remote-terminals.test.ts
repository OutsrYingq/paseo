import { describe, expect, it, vi } from "vitest";
import type { TerminalReplayEvent } from "../../terminal/terminal-replay.js";
import type { ServerMessage, TerminalSession } from "../../terminal/terminal.js";
import {
  RemoteTerminalEventSchema,
  RemoteTerminalOperationSchema,
  createRemoteTerminalManager,
  createResidentTerminals,
  remoteTerminalOperationResults,
  type RemoteTerminalConnection,
  type RemoteTerminalEvent,
  type RemoteTerminalOperation,
  type RemoteTerminalResult,
} from "./remote-terminals.js";

// Real resident and PTYs. Serialize at the adapter boundary as the execution WS does.
async function connect(resident: ReturnType<typeof createResidentTerminals>) {
  const lifetime = new AbortController();
  const errors: unknown[] = [];
  function receive(event: RemoteTerminalEvent) {
    manager.handleEvent(RemoteTerminalEventSchema.parse(JSON.parse(JSON.stringify(event))));
  }
  const channel = resident.open(receive, lifetime.signal);
  const connection: RemoteTerminalConnection = {
    signal: lifetime.signal,
    async call<K extends RemoteTerminalOperation["kind"]>(
      operation: RemoteTerminalOperation & { kind: K },
    ) {
      const value = await channel.execute(
        RemoteTerminalOperationSchema.parse(JSON.parse(JSON.stringify(operation))),
      );
      return remoteTerminalOperationResults[operation.kind].parse(
        JSON.parse(JSON.stringify(value)),
      ) as RemoteTerminalResult<K>;
    },
  };
  const manager = createRemoteTerminalManager(connection, (error) => errors.push(error));
  await manager.initialize();
  return { manager, errors, disconnect: () => lifetime.abort() };
}

function watch(terminal: TerminalSession) {
  let text = "";
  let ready = false;
  const detach = terminal.subscribe((message: ServerMessage) => {
    if (message.type === "output") text += message.data;
    if (message.type === "snapshotReady") ready = true;
  });
  return {
    detach,
    get text() {
      return text;
    },
    get ready() {
      return ready;
    },
  };
}

async function replay(manager: ReturnType<typeof createRemoteTerminalManager>, id: string) {
  const chunks: Uint8Array[] = [];
  const events: TerminalReplayEvent["type"][] = [];
  const metadata = await manager.replayTerminal(id, undefined, (event) => {
    events.push(event.type);
    if (event.type === "data") chunks.push(event.data);
  });
  return { metadata, text: Buffer.concat(chunks).toString("utf8"), events };
}

const terminalProgram = [
  "const label = process.env.PASEO_REMOTE_PTY_TEST",
  "if (!process.stdin.isTTY || !process.stdout.isTTY) process.exit(91)",
  "process.stdin.setRawMode(true)",
  "process.stdout.write(label + '-boot\\n')",
  "process.stdin.on('data', bytes => process.stdout.write(label + '-' + bytes.toString('utf8') + '\\n'))",
].join("; ");

describe("remote terminal adapters", () => {
  it("isolates real PTYs with the same cwd and ID and restores viewers after a reconnect", async () => {
    const a = createResidentTerminals();
    const b = createResidentTerminals();
    const cwd = process.cwd();
    const id = "shared-terminal-id";
    const workspaceId = "shared-workspace-id";
    const create = {
      cwd,
      id,
      workspaceId,
      command: process.execPath,
      args: ["-e", terminalProgram],
    };
    a.registerCwdEnv({ cwd, env: { PASEO_REMOTE_PTY_TEST: "resident-a" } });
    b.registerCwdEnv({ cwd, env: { PASEO_REMOTE_PTY_TEST: "resident-b" } });
    try {
      const [clientA, clientB] = await Promise.all([connect(a), connect(b)]);
      const [terminalA, terminalB] = await Promise.all([
        clientA.manager.createTerminal(create),
        clientB.manager.createTerminal(create),
      ]);
      expect(clientA.manager.getTerminal(id)).toBe(terminalA);
      expect(clientB.manager.getTerminal(id)).toBe(terminalB);
      expect(await clientA.manager.getTerminals(cwd, { workspaceId })).toEqual([terminalA]);
      expect(await clientB.manager.getTerminals(cwd, { workspaceId })).toEqual([terminalB]);

      await vi.waitFor(
        async () => {
          expect((await replay(clientA.manager, id)).text).toContain("resident-a-boot");
          expect((await replay(clientB.manager, id)).text).toContain("resident-b-boot");
        },
        { timeout: 5_000 },
      );
      const viewerA = watch(terminalA);
      const viewerB = watch(terminalB);
      const secondViewerB = watch(terminalB);
      await vi.waitFor(() =>
        expect(viewerA.ready && viewerB.ready && secondViewerB.ready).toBe(true),
      );
      viewerB.detach();
      terminalA.send({ type: "input", data: "same-command" });
      terminalB.send({ type: "input", data: "same-command" });
      await vi.waitFor(() => {
        expect(viewerA.text).toContain("resident-a-same-command");
        expect(viewerA.text).not.toContain("resident-b-");
        expect(secondViewerB.text).toContain("resident-b-same-command");
        expect(secondViewerB.text).not.toContain("resident-a-");
        expect(viewerB.text).not.toContain("same-command");
      });

      terminalB.send({ type: "resize", rows: 31, cols: 97 });
      const snapshot = await replay(clientB.manager, id);
      expect(snapshot.metadata).toMatchObject({ rows: 31, cols: 97 });
      expect(snapshot.events).toEqual(["begin", "data", "end"]);
      expect(snapshot.text).toContain("resident-b-same-command");
      const exitA = new Promise((resolve) => terminalA.onExit(resolve));
      await clientA.manager.killTerminalAndWait(id);
      await expect(exitA).resolves.toHaveProperty("exitCode");
      expect(clientA.manager.getTerminal(id)).toBeUndefined();
      clientA.disconnect();
      await a.dispose();

      terminalB.send({ type: "input", data: "after-a-stopped" });
      await vi.waitFor(() => expect(secondViewerB.text).toContain("resident-b-after-a-stopped"));
      clientB.disconnect();
      const reconnectedB = await connect(b);
      const reattached = reconnectedB.manager.getTerminal(id);
      expect(reattached).toBeDefined();
      if (!reattached) throw new Error("Missing resident terminal after reconnect");
      expect(reattached.hasOutput()).toBe(true);
      expect((await replay(reconnectedB.manager, id)).text).toContain("resident-b-after-a-stopped");
      const viewer = watch(reattached);
      await vi.waitFor(() => expect(viewer.ready).toBe(true));
      reattached.send({ type: "input", data: "after-reconnect" });
      await vi.waitFor(() => expect(viewer.text).toContain("resident-b-after-reconnect"));
      expect([...clientA.errors, ...clientB.errors, ...reconnectedB.errors]).toEqual([]);
      reconnectedB.disconnect();
    } finally {
      await Promise.all([a.dispose(), b.dispose()]);
    }
  });
});
