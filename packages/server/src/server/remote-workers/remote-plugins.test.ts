import { randomUUID } from "node:crypto";
import type { ProviderInput, ProviderPersistence } from "@getpaseo/plugin/server/provider";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import pino from "pino";
import { expect, it, vi } from "vitest";
import { PluginAgentClientRegistry } from "../agent/plugin-provider.js";
import {
  createResidentPluginHost,
  RemotePluginClient,
  RemotePluginEventSchema,
  RemotePluginOperationSchema,
  RemotePluginResultSchema,
  type RemotePluginScope,
} from "./remote-plugins.js";

const pluginSource = String.raw`
import { defineRpc, defineSubscription } from "@getpaseo/plugin";
import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import type { ProviderInput, ProviderPersistence } from "@getpaseo/plugin/server/provider";
import { appendFile, readFile, writeFile } from "node:fs/promises";
import path from "node:path";
import { z } from "zod";

export default function contribute(server) {
  const launchId = randomUUID();
  const marker = process.env.PASEO_PLUGIN_TEST_MARKER;
  const home = process.env.PASEO_HOME;
  console.log("plugin marker " + marker);
  server.handle(defineRpc({ name: "marker", input: z.object({}), output: z.object({
    marker: z.string(), file: z.string(), launchId: z.string(), pid: z.number(),
  }) }), async () => {
    const file = await readFile(path.join(home, "marker"), "utf8");
    await writeFile(path.join(home, "rpc-output"), marker + ":" + file);
    return { marker, file, launchId, pid: process.pid };
  });
  server.handle(defineRpc({ name: "bytes", input: z.instanceof(Uint8Array), output: z.instanceof(Uint8Array) }), (bytes) => bytes.reverse());
  server.handle(defineSubscription({ name: "watch", input: z.object({}), event: z.instanceof(Uint8Array) }), async (_input, { push }) => {
    await push(new Uint8Array([marker.charCodeAt(0)]));
  });
  server.registerProvider({
    id: "ap-omp", label: "Synthetic AP OMP", async connect() {
      let receive = () => {};
      const timers = new Map();
      const keys = new Map();
      const acknowledgements = new Map();
      const runners = new Set();
      return {
        version: 1, capabilities: ["prompt.message", "session.persistence"],
        onEvent(listener) { receive = listener; return () => { receive = () => {}; }; },
        async send(input) {
          if (input.type === "catalog") {
            receive({ type: "catalog", requestId: input.requestId, catalog: {
              models: [{ id: marker, label: marker }], modes: [], thinkingOptions: [],
            } });
            return;
          }
          const sessionId = input.sessionId;
          if (input.type === "session.open") {
            const key = input.persistence?.data.key ?? randomUUID();
            keys.set(sessionId, key);
            receive({ type: "session.opened", requestId: input.requestId, sessionId,
              cwd: input.config.cwd, capabilities: ["prompt.message", "session.persistence"],
              restoration: "core", persistence: { version: 1, data: { key } },
            });
            if (input.history === "replay") {
              const history = await readFile(path.join(home, key + ".history"), "utf8");
              for (const line of history.trim().split("\n")) {
                receive({ type: "timeline.item", sessionId, item: { type: "assistant_message", id: line, text: line } });
              }
            }
            receive({ type: "session.ready", requestId: input.requestId, sessionId });
            return;
          }
          if (input.type === "session.prompt") {
            const { clientMessageId } = input.prompt;
            const turnId = clientMessageId + "-turn";
            const key = keys.get(sessionId);
            await appendFile(path.join(home, "accepted"), clientMessageId + "\n");
            receive({ type: "session.prompt_result", sessionId, clientMessageId, result: { type: "turn", turnId } });
            receive({ type: "session.turn", sessionId, turnId, state: "started" });
            receive({ type: "session.turn", sessionId, turnId: "another-turn", state: "completed" });
            receive({ type: "session.turn", sessionId: "another-session", turnId, state: "completed" });
            if (clientMessageId === "exit") {
              const gate = path.join(home, "exit.release");
              const runner = spawn(process.execPath, ["-e", "const watch = setInterval(() => { if (require('node:fs').existsSync(process.argv[1])) process.exit(8); }, 15);", gate], { stdio: "ignore" });
              const closed = new Promise((resolve) => runner.once("close", () => {
                runners.delete(runner);
                keys.delete(sessionId);
                receive({ type: "session.runtime_failed", sessionId, error: { message: "OMP runtime exited" } });
                resolve();
              }));
              runners.add(runner);
              receive({ type: "session.runtime_failed", sessionId, error: { message: "Plugin sent an invalid IPC message" } });
              await closed;
              return;
            }
            const checkGate = async () => {
              if (!timers.has(sessionId)) return;
              if (await readFile(path.join(home, clientMessageId + ".release"), "utf8").catch(() => null) === null) {
                if (timers.has(sessionId)) timers.set(sessionId, setTimeout(checkGate, 15));
                return;
              }
              if (!timers.delete(sessionId)) return;
              await appendFile(path.join(home, key + ".history"), clientMessageId + "-final\n");
              receive({ type: "session.turn", sessionId, turnId, state: "completed" });
              acknowledgements.get(sessionId)?.();
              acknowledgements.delete(sessionId);
            };
            timers.set(sessionId, setTimeout(checkGate, 15));
            if (clientMessageId === "blocked") await new Promise((resolve) => acknowledgements.set(sessionId, resolve));
            return;
          }
          if (input.type === "session.close") {
            clearTimeout(timers.get(sessionId));
            timers.delete(sessionId);
            receive({ type: "session.closed", sessionId });
          }
        },
        async close() {
          for (const timer of timers.values()) clearTimeout(timer);
          timers.clear();
          for (const runner of runners) runner.kill();
          for (const acknowledge of acknowledgements.values()) acknowledge();
          acknowledgements.clear();
          await appendFile(path.join(home, "connections-closed"), launchId + "\n");
        },      };
    },
  });
  return () => {};
}
`;

it("keeps native plugin turns across channel loss and scopes bundles, RPC and reload", async () => {
  const tmp = await mkdtemp(path.join(os.tmpdir(), "paseo-remote-plugin-test-"));
  const cleanups: Array<() => Promise<void>> = [];
  const attached: RemotePluginScope[] = [];
  try {
    async function worker(marker: string, hasPlugin = true, podUid = randomUUID()) {
      const root = path.join(tmp, marker + "-" + podUid);
      const directory = path.join(root, "plugin");
      await mkdir(directory, { recursive: true });
      await writeFile(path.join(root, "marker"), marker + "-file");
      await writeFile(
        path.join(directory, "paseo-plugin.json"),
        JSON.stringify({ id: "ap-omp", requirements: { paseo: ">=0.17.4" } }),
      );
      await writeFile(path.join(directory, "index.server.ts"), pluginSource);
      await writeFile(
        path.join(directory, "index.client.ts"),
        `export default () => { console.log(${JSON.stringify(marker)}); };`,
      );
      await writeFile(
        path.join(root, "config.json"),
        JSON.stringify({
          pluginsEnabled: true,
          plugins: hasPlugin ? { "ap-omp": { source: "directory", path: directory } } : {},
        }),
      );
      const scope = { workerId: marker, podUid };
      const host = createResidentPluginHost({
        scope,
        paseoHome: root,
        daemonVersion: "0.17.4",
        logger: pino({ level: "silent" }),
      });
      cleanups.push(host.close);
      const open = () => {
        const controller = new AbortController();
        const signal = controller.signal;
        let client: RemotePluginClient;
        const trace: unknown[] = [];
        const resident = host.openChannel({
          scope,
          signal,

          publish(event) {
            trace.push(event.event);
            client.onEvent(RemotePluginEventSchema.parse(JSON.parse(JSON.stringify(event))));
          },
        });
        client = new RemotePluginClient({
          scope,
          signal,
          async call(operation) {
            trace.push(operation.request);
            const request = RemotePluginOperationSchema.parse(
              JSON.parse(JSON.stringify(operation)),
            );
            const result = await resident.execute(request);
            return RemotePluginResultSchema.parse(JSON.parse(JSON.stringify(result)));
          },
          async attachPluginSocket(bound, _ref, socket) {
            attached.push(bound);
            const closed = new Promise<void>((resolve) => socket.once("close", resolve));
            socket.on("message", (data) => {
              if (typeof data !== "string" || JSON.parse(data).type !== "hello") return;
              socket.send(
                JSON.stringify({
                  type: "session",
                  message: {
                    type: "status",
                    payload: {
                      status: "server_info",
                      serverId: bound.podUid,
                      hostname: bound.workerId,
                      version: "0.17.4",
                      features: {},
                    },
                  },
                }),
              );
            });
            return { closed };
          },
        });
        return { client, trace, abort: () => controller.abort(), close: resident.close };
      };
      const { client, trace, abort, close } = open();
      const useEnvironment = () => {
        vi.stubEnv("PASEO_HOME", root);
        vi.stubEnv("PASEO_PLUGIN_TEST_MARKER", marker);
      };
      useEnvironment();
      const list = await client.request({ action: "list" });
      if (hasPlugin && list[0]?.item.status !== "running") {
        const logs = await client.request({ action: "logs", plugin: list[0]!.plugin });
        throw new Error(JSON.stringify({ list, trace, logs }));
      }
      return { client, directory, list, root, scope, host, open, abort, close, useEnvironment };
    }

    const a = await worker("A");
    const b = await worker("B");
    const codexOnly = await worker("codex", false);
    const replacement = await worker("A", false);
    expect(await codexOnly.client.getProviderRegistrations()).toEqual([]);
    expect(await codexOnly.client.request({ action: "catalog" })).toEqual([]);
    expect(a.list[0]?.item, JSON.stringify(a.list)).toMatchObject({
      id: "ap-omp",
      path: a.directory,
      status: "running",
    });
    expect(b.list[0]?.item, JSON.stringify(b.list)).toMatchObject({
      id: "ap-omp",
      path: b.directory,
      status: "running",
    });
    expect(await a.client.request({ action: "inspectDirectory", path: a.directory })).toEqual({
      id: "ap-omp",
    });
    const aRef = a.list[0]!.plugin;
    const bRef = b.list[0]!.plugin;
    expect(aRef.token).not.toBe(bRef.token);
    await expect(b.client.invokePluginRpc(aRef, "marker", {})).rejects.toThrow(
      "unknown_worker_plugin",
    );
    await expect(replacement.client.request({ action: "logs", plugin: aRef })).rejects.toThrow(
      "unknown_worker_plugin",
    );
    expect(
      RemotePluginOperationSchema.safeParse({
        kind: "plugin",
        request: { action: "list", workerId: "B" },
      }).success,
    ).toBe(false);
    expect(RemotePluginResultSchema.safeParse({ action: "logs", value: null }).success).toBe(false);

    const bundleA = (await a.client.request({ action: "catalog" }))[0]!;
    const bundleB = (await b.client.request({ action: "catalog" }))[0]!;
    expect(bundleA.clientBundle).toContain('"A"');
    expect(bundleB.clientBundle).toContain('"B"');
    expect(bundleA.clientBundleHash).not.toBe(bundleB.clientBundleHash);
    expect(
      (
        await a.client.request({
          action: "catalog",
          known: [{ id: aRef.id, clientBundleHash: bundleA.clientBundleHash }],
        })
      )[0]?.clientBundle,
    ).toBeUndefined();

    const aInitial = await a.client.invokePluginRpc(aRef, "marker", {});
    const bInitial = await b.client.invokePluginRpc(bRef, "marker", {});
    expect(aInitial).toMatchObject({ marker: "A", file: "A-file" });
    expect(bInitial).toMatchObject({ marker: "B", file: "B-file" });
    expect(aInitial).not.toMatchObject({ pid: process.pid });
    expect(await readFile(path.join(a.root, "rpc-output"), "utf8")).toBe("A:A-file");
    expect(await readFile(path.join(b.root, "rpc-output"), "utf8")).toBe("B:B-file");
    expect(
      (await a.client.request({ action: "logs", plugin: aRef })).map(({ message }) => message),
    ).toContain("plugin marker A");
    const bytes = await a.client.invokePluginRpc(aRef, "bytes", new Uint8Array([1, 2]));
    expect(bytes).toBeInstanceOf(Uint8Array);
    expect(Array.from(bytes as Uint8Array)).toEqual([2, 1]);
    const watch = b.client.subscribePluginRpc(bRef, "watch", {});
    await watch.ready;
    const events: unknown[] = [];
    const closed = new Promise<void>((resolve) =>
      watch.attach({ event: (value) => events.push(value), closed: () => resolve() }),
    );
    await closed;
    expect(events.map((value) => Array.from(value as Uint8Array))).toEqual([[66]]);

    const providerA = (await a.client.getProviderRegistrations())[0]!;
    const providerB = (await b.client.getProviderRegistrations())[0]!;
    expect(providerA.id).toBe("ap-omp");
    for (const [provider, marker] of [
      [providerA, "A"],
      [providerB, "B"],
    ] as const) {
      const adapters = new PluginAgentClientRegistry(pino({ level: "silent" }));
      try {
        adapters.replace([provider]);
        const catalog = await adapters.clients()["ap-omp"]!.fetchCatalog!({
          scope: "global",
          force: true,
        });
        expect(catalog.models).toMatchObject([{ id: marker, label: marker }]);
      } finally {
        await adapters.shutdown();
      }
    }

    replacement.useEnvironment();
    const installed = await replacement.client.request({
      action: "installDirectory",
      path: replacement.directory,
    });
    expect(installed.item.status).toBe("running");
    expect(installed.plugin.token).not.toBe(aRef.token);
    await expect(replacement.client.invokePluginRpc(aRef, "marker", {})).rejects.toThrow(
      "unknown_worker_plugin",
    );

    a.useEnvironment();
    expect((await a.client.request({ action: "reload", plugin: aRef })).item.status).toBe(
      "running",
    );
    const aReloaded = await a.client.invokePluginRpc(aRef, "marker", {});
    expect(aReloaded).toMatchObject({ marker: "A", file: "A-file" });
    expect(aReloaded).not.toEqual(aInitial);
    expect(await b.client.invokePluginRpc(bRef, "marker", {})).toEqual(bInitial);
    expect((await b.client.getProviderRegistrations())[0]).toBe(providerB);
    await expect(providerA.connect({ versions: [1], capabilities: [] })).rejects.toThrow(
      "unknown_worker_plugin_provider",
    );
    expect(attached).toEqual([a.scope, b.scope, replacement.scope, a.scope]);

    const openInput = (
      sessionId: string,
      persistence?: ProviderPersistence,
      cwd = a.root,
    ): ProviderInput => ({
      type: "session.open",
      requestId: randomUUID(),
      sessionId,
      config: { cwd, persist: true, env: {}, mcpServers: {}, settings: {} },
      history: persistence ? "replay" : "skip",
      persistence,
    });
    const capabilities = ["prompt.message", "session.persistence"];
    const connect = async (client: RemotePluginClient) =>
      (await client.getProviderRegistrations())[0]!.connect({ versions: [1], capabilities });
    const prompt = (sessionId: string, clientMessageId: string): ProviderInput => ({
      type: "session.prompt",
      sessionId,
      prompt: {
        clientMessageId,
        delivery: "auto",
        input: { type: "message", content: [{ type: "text", text: clientMessageId }] },
      },
    });
    const old = await connect(a.client);
    const observed = vi.fn();
    old.onEvent(observed);
    await old.send(openInput("old-session"));
    const persistence = observed.mock.calls.find(([event]) => event.type === "session.opened")?.[0]
      .persistence as ProviderPersistence;
    const key = (persistence.data as { key: string }).key;
    await old.send(prompt("old-session", "first"));
    await expect(
      old.send({ type: "session.close", sessionId: "old-session", requestId: "graceful" }),
    ).rejects.toThrow("worker_plugin_session_busy");
    await old.close();
    a.abort();
    await a.close();
    expect(() =>
      a.host.openChannel({
        scope: replacement.scope,
        signal: new AbortController().signal,
        publish: () => {},
      }),
    ).toThrow("worker_plugin_binding_mismatch");
    const reconnected = a.open();
    const ref = (await reconnected.client.request({ action: "list" }))[0]!.plugin;
    expect(ref.token).not.toBe(aRef.token);
    await expect(reconnected.client.invokePluginRpc(aRef, "marker", {})).rejects.toThrow(
      "unknown_worker_plugin",
    );
    expect(await reconnected.client.invokePluginRpc(ref, "marker", {})).toEqual(aReloaded);
    await expect
      .poll(() => attached.filter(({ podUid }) => podUid === a.scope.podUid).length, {
        timeout: 6000,
      })
      .toBe(3);
    const resumed = await connect(reconnected.client);
    await expect(
      resumed.send(openInput("new-session", { version: 2, data: { key, assets: {} } }, b.root)),
    ).rejects.toThrow("worker_plugin_session_busy");
    await expect(resumed.send(openInput("new-session"))).rejects.toThrow(
      "worker_plugin_session_busy",
    );
    const replay = vi.fn();
    resumed.onEvent(replay);
    await writeFile(path.join(a.root, "first.release"), "");
    await expect
      .poll(async () => readFile(path.join(a.root, key + ".history"), "utf8").catch(() => null), {
        timeout: 3000,
      })
      .toBe("first-final\n");
    await expect
      .poll(async () => {
        try {
          await resumed.send(openInput("new-session", persistence));
          return true;
        } catch (error) {
          if (!(error instanceof Error) || error.message !== "worker_plugin_session_busy")
            throw error;
          return false;
        }
      })
      .toBe(true);
    expect(replay).toHaveBeenCalledWith(
      expect.objectContaining({
        type: "timeline.item",
        sessionId: "new-session",
        item: { type: "assistant_message", id: "first-final", text: "first-final" },
      }),
    );
    expect(await readFile(path.join(a.root, key + ".history"), "utf8")).toBe("first-final\n");
    expect(await readFile(path.join(a.root, "accepted"), "utf8")).toBe("first\n");

    const exit = resumed.send(prompt("new-session", "exit")).catch((error: unknown) => error);
    await expect
      .poll(() => replay.mock.calls.map(([event]) => event))
      .toContainEqual(
        expect.objectContaining({
          type: "session.runtime_failed",
          sessionId: "new-session",
          error: { message: "Plugin sent an invalid IPC message" },
        }),
      );
    reconnected.abort();
    expect(await exit).toMatchObject({ message: "worker_plugin_channel_closed" });
    const afterExit = a.open();
    const afterExitProvider = await connect(afterExit.client);
    await expect(afterExitProvider.send(openInput("after-exit", persistence))).rejects.toThrow(
      "worker_plugin_session_busy",
    );
    await writeFile(path.join(a.root, "exit.release"), "");
    await expect
      .poll(
        async () => {
          try {
            await afterExitProvider.send(openInput("after-exit", persistence));
            return true;
          } catch (error) {
            if (!(error instanceof Error) || error.message !== "worker_plugin_session_busy")
              throw error;
            return false;
          }
        },
        { timeout: 6000 },
      )
      .toBe(true);
    expect(await readFile(path.join(a.root, key + ".history"), "utf8")).toBe("first-final\n");

    const blocked = afterExitProvider
      .send(prompt("after-exit", "blocked"))
      .catch((error: unknown) => error);
    await expect
      .poll(() => readFile(path.join(a.root, "accepted"), "utf8"))
      .toBe("first\nexit\nblocked\n");
    afterExit.abort();
    expect(await blocked).toMatchObject({ message: "worker_plugin_channel_closed" });
    const third = a.open();
    const thirdRef = (await third.client.request({ action: "list" }))[0]!.plugin;
    const waiting = await connect(third.client);
    await expect(waiting.send(openInput("blocked-session", persistence))).rejects.toThrow(
      "worker_plugin_session_busy",
    );
    expect(await third.client.invokePluginRpc(thirdRef, "marker", {})).toEqual(aReloaded);
    await third.client.request({ action: "reload", plugin: thirdRef });
    const afterReload = await third.client.invokePluginRpc(thirdRef, "marker", {});
    expect(afterReload).not.toEqual(aReloaded);
    const afterReloadProvider = await connect(third.client);
    await afterReloadProvider.send(openInput("after-reload", persistence));
    await afterReloadProvider.send(prompt("after-reload", "stop"));
    const pid = (afterReload as { pid: number }).pid;
    await a.host.close();
    expect(() => process.kill(pid, 0)).toThrow();
    expect(await readFile(path.join(a.root, key + ".history"), "utf8")).toBe("first-final\n");
  } finally {
    await Promise.all(cleanups.map((cleanup) => cleanup()));
    vi.unstubAllEnvs();
    await rm(tmp, { recursive: true, force: true });
  }
}, 60_000);
