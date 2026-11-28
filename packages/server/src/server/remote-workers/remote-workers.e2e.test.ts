import { createRequire } from "node:module";
import { X509Certificate, randomUUID } from "node:crypto";
import { mkdtemp, mkdir, readdir, readFile, rm, writeFile } from "node:fs/promises";
import { request } from "node:http";
import os from "node:os";
import path from "node:path";
import pino from "pino";
import { afterEach, expect, it } from "vitest";
import { WebSocket } from "ws";
import { DaemonClient, type WebSocketLike } from "@getpaseo/client/internal/daemon-client";
import {
  createNativeExecution,
  startRemoteCentral,
  startResidentExecution,
  type WorkerAuthority,
  type WorkerGrant,
  type WorkerMaintenanceOperation,
  type WorkerTarget,
} from "./index.js";

// Test-only certificate. The fixture creates a fresh private key for every run.
function testTls() {
  const forge = createRequire(import.meta.url)("node-forge");
  const keys = forge.pki.rsa.generateKeyPair(2048);
  const cert = forge.pki.createCertificate();
  cert.publicKey = keys.publicKey;
  cert.serialNumber = "01";
  cert.validity.notBefore = new Date(Date.now() - 60_000);
  cert.validity.notAfter = new Date(Date.now() + 60 * 60_000);
  cert.setSubject([{ name: "commonName", value: "localhost" }]);
  cert.setIssuer(cert.subject.attributes);
  cert.setExtensions([
    { name: "basicConstraints", cA: true },
    { name: "subjectAltName", altNames: [{ type: 7, ip: "127.0.0.1" }] },
  ]);
  cert.sign(keys.privateKey, forge.md.sha256.create());
  const pem = forge.pki.certificateToPem(cert);
  return {
    key: forge.pki.privateKeyToPem(keys.privateKey),
    cert: pem,
    ca: pem,
    fingerprint: new X509Certificate(pem).fingerprint256,
  };
}

const nativeFixture = String.raw`
const fs = require('node:fs');
const readline = require('node:readline');
const provider = process.argv[2];
const send = (message) => process.stdout.write(JSON.stringify(message) + '\n');
const state = { isStreaming: false, isCompacting: false, sessionId: 'shared-native-id', sessionFile: process.env.FIXTURE_STATE + '/native.jsonl', messageCount: 0, queuedMessageCount: 0 };
const count = () => fs.appendFileSync(process.env.FIXTURE_STATE + '/prompts', 'prompt\n');
const turnHeld = () => fs.existsSync(process.env.FIXTURE_STATE + '/hold-turn');
if (provider === 'omp') send({ type: 'ready', protocolVersion: 1, supportedProtocolVersions: [1] });
readline.createInterface({ input: process.stdin }).on('line', (line) => {
  const command = JSON.parse(line);
  const { id, method, type } = command;
  if (!id) return;
  if (provider === 'codex') {
    if (method === 'turn/start') count();
    if (method === 'thread/start' || method === 'thread/resume') fs.writeFileSync(process.env.FIXTURE_STATE + '/session-pid', String(process.pid));
    if (method === 'thread/resume') fs.appendFileSync(process.env.FIXTURE_STATE + '/resumes', 'resume\n');
    const result = ({ initialize: { userAgent: 'fixture' }, 'model/list': { data: [{ id: 'fixture', model: 'fixture', isDefault: true, displayName: 'Fixture', description: 'fixture', hidden: false, supportedReasoningEfforts: [], defaultReasoningEffort: 'medium' }] }, 'thread/start': { thread: { id: 'shared-native-id' } }, 'thread/resume': { thread: { id: 'shared-native-id' } }, 'thread/read': { thread: { id: 'shared-native-id', turns: [{ id: 't1', status: 'interrupted', items: [{ id: 'rehydrated', type: 'userMessage', content: [{ type: 'text', text: 'provider history', text_elements: [] }] }] }] } }, 'turn/start': { turn: { id: 't1', status: 'inProgress' } } })[method] ?? {};
    send({ id, result });
    if (method === 'turn/start') {
      send({ method: 'turn/started', params: { threadId: 'shared-native-id', turn: { id: 't1', status: 'inProgress' } } });
      if (!turnHeld()) setTimeout(() => send({ method: 'turn/completed', params: { threadId: 'shared-native-id', turn: { id: 't1', status: 'completed' } } }), 250);
    }
    if (method === 'turn/interrupt') {
      fs.appendFileSync(process.env.FIXTURE_STATE + '/interrupts', 'interrupt\n');
      send({ method: 'turn/completed', params: { threadId: 'shared-native-id', turn: { id: 't1', status: 'interrupted' } } });
    }
  } else {
    const result = ({ get_state: state, get_messages: { messages: [] }, get_available_commands: { commands: [] }, get_available_models: { models: [{ provider: 'fixture', id: 'test', name: 'Fixture OMP' }] }, get_session_stats: {}, prompt: { agentInvoked: true } })[type] ?? {};
    if (type === 'prompt') count();
    send({ id, type: 'response', command: type, success: true, data: result });
    if (type === 'prompt') {
      send({ type: 'agent_start' });
      const finish = () => {
        if (turnHeld()) setTimeout(finish, 25);
        else send({ type: 'agent_end', messages: [{ role: 'assistant', content: [{ type: 'text', text: 'finished' }] }] });
      };
      setTimeout(finish, 250);
    }
  }
});
`;

const cleanups: Array<() => Promise<void> | void> = [];
afterEach(async () => {
  while (cleanups.length > 0) await cleanups.pop()?.();
});

function openClient(
  port: number,
  host: string,
  password: string,
  sessionPermissions?: ConstructorParameters<typeof DaemonClient>[0]["sessionPermissions"],
) {
  const sdk = new DaemonClient({
    url: `ws://127.0.0.1:${port}/ws`,
    clientId: `test-${host}`,
    clientType: "cli",
    password,
    connectTimeoutMs: 4_000,
    sessionPermissions,
    webSocketFactory: (url, options) =>
      new WebSocket(url, options?.protocols, {
        headers: { ...options?.headers, host },
      }) as unknown as WebSocketLike,
  });
  cleanups.push(() => sdk.close());
  return sdk;
}

function get(port: number, host: string, route: string, password?: string) {
  return new Promise<{ status: number; body: string }>((resolve, reject) => {
    const req = request(
      {
        hostname: "127.0.0.1",
        port,
        path: route,
        headers: {
          host,
          ...(password ? { authorization: `Bearer ${password}` } : {}),
        },
      },
      (res) => {
        let body = "";
        res.setEncoding("utf8");
        res.on("data", (chunk) => {
          body += chunk;
        });
        res.on("end", () => resolve({ status: res.statusCode ?? 0, body }));
        res.on("error", reject);
      },
    );
    req.on("error", reject);
    req.end();
  });
}

it("uses the unchanged SDK against two mTLS residents and isolated native and download paths", async () => {
  const tls = testTls();
  const logger = pino({ level: "warn" });
  const root = await mkdtemp(path.join(os.tmpdir(), "paseo-remote-workers-"));
  cleanups.push(() => rm(root, { recursive: true, force: true }));
  const script = path.join(root, "fixture.cjs");
  await writeFile(script, nativeFixture);
  const failRestart = new Set<string>();
  const restartFailures: string[] = [];
  const residents = new Map<string, Awaited<ReturnType<typeof startResidentExecution>>>();
  const targets = new Map<string, { password: string; target: WorkerTarget }>();
  for (const [label, provider] of [
    ["a", "codex"],
    ["b", "omp"],
  ] as const) {
    const local = path.join(root, label);
    await mkdir(local);
    await mkdir(path.join(local, "projects"));
    const now = new Date().toISOString();
    await writeFile(
      path.join(local, "projects", "projects.json"),
      JSON.stringify([
        {
          projectId: "same-project",
          rootPath: local,
          kind: "non_git",
          displayName: label,
          createdAt: now,
          updatedAt: now,
          archivedAt: null,
        },
      ]),
    );
    await writeFile(
      path.join(local, "projects", "workspaces.json"),
      JSON.stringify([
        {
          workspaceId: "same-workspace",
          projectId: "same-project",
          cwd: local,
          kind: "directory",
          displayName: label,
          createdAt: now,
          updatedAt: now,
          archivedAt: null,
        },
      ]),
    );
    await writeFile(path.join(local, "server-id"), `srv_${label}\n`);
    await writeFile(path.join(local, "marker.txt"), `${label}-only`);
    const pluginDir = path.join(local, "fixture-plugin");
    await mkdir(pluginDir);
    await writeFile(
      path.join(pluginDir, "paseo-plugin.json"),
      JSON.stringify({ id: "fixture-plugin", requirements: { paseo: ">=0.17.4" } }),
    );
    await writeFile(
      path.join(pluginDir, "index.server.ts"),
      `
      import { defineRpc } from "@getpaseo/plugin";
      import { randomUUID } from "node:crypto";
      import { z } from "zod";
      const launchId = randomUUID();
      export default function contribute(server) {
        server.handle(defineRpc({ name: "marker", input: z.object({}), output: z.object({ marker: z.string(), launchId: z.string() }) }),
          () => ({ marker: ${JSON.stringify(label)}, launchId }));
        return () => {};
      }
    `,
    );
    await writeFile(
      path.join(pluginDir, "index.client.ts"),
      `export default () => { console.log(${JSON.stringify(label)}); return () => {}; };`,
    );
    await writeFile(
      path.join(local, "config.json"),
      JSON.stringify({
        pluginsEnabled: true,
        plugins: {
          "fixture-plugin": { source: "directory", path: pluginDir },
        },
      }),
    );
    const native = createNativeExecution({
      paseoHome: local,
      workspaceRoots: [local],
      logger,
      plugins: { daemonVersion: "0.17.4" },
      providers: {
        [provider]: {
          command: { mode: "replace", argv: [process.execPath, script, provider] },
          env: { FIXTURE_STATE: local },
        },
      },
    });
    const binding = { workerId: label, podUid: `pod-${label}`, serverId: `srv_${label}` };
    const resident = await startResidentExecution({
      binding,
      paseoHome: local,
      listen: { host: "127.0.0.1", port: 0 },
      tls,
      authorizeCentral: (fingerprint) => fingerprint === tls.fingerprint,
      async openRuntime(...args) {
        const runtime = await native.openRuntime(...args);
        return {
          ...runtime,
          async restartRuntime(keepTerminals) {
            if (failRestart.has(label)) throw new Error("fixture reset failure");
            await runtime.restartRuntime?.(keepTerminals);
          },
        };
      },
      shutdownRuntime: native.close,
      onError(error) {
        if (error instanceof Error && error.message === "fixture reset failure") {
          restartFailures.push(label);
        } else logger.error({ error });
      },
    });
    residents.set(label, resident);
    cleanups.push(() => resident.close());
    targets.set(`${label}.paseo.test`, {
      password: `password-${label}`,
      target: {
        ...binding,
        endpoint: `wss://127.0.0.1:${resident.port}/execute`,
        certFingerprint256: tls.fingerprint,
        revision: 1,
      },
    });
  }
  const authority: WorkerAuthority = {
    async admit(hostname, credential) {
      const entry = targets.get(hostname);
      if (!entry) return { kind: "unknown_host" };
      if (credential.kind !== "password" || credential.password !== entry.password)
        return { kind: "unauthorized" };
      return {
        kind: "allowed",
        grant: {
          target: entry.target,
          hostname,
          leaseId: randomUUID(),
          scope: "owner",
          principalId: "fixture",
          expiresAt: Date.now() + 60_000,
        },
      };
    },
    async renew(grant) {
      return { kind: "allowed", grant: { ...grant, expiresAt: Date.now() + 60_000 } };
    },
  };
  const central = await startRemoteCentral({
    authority,
    workerTls: tls,
    listen: { host: "127.0.0.1", port: 0 },
    version: "0.17.4",
    logger,
  });
  cleanups.push(() => central.close());
  const maintenanceSignal = new AbortController().signal;
  const grant: WorkerGrant = {
    hostname: "a.paseo.test",
    target: targets.get("a.paseo.test")!.target,
    principalId: "ops-runtime",
    leaseId: "maintenance-fixture",
    scope: ["daemon.read", "workspace.read", "workspace.write"],
    expiresAt: Date.now() + 60_000,
  };
  const runMaintenance = <Operation extends WorkerMaintenanceOperation>(operation: Operation) =>
    central.maintenance(grant, operation, maintenanceSignal);
  const refreshDirectory = await runMaintenance({
    type: "refresh_providers_snapshot_request",
    requestId: "catalog",
    providers: ["codex"],
    cwd: path.join(root, "a"),
  });
  expect(refreshDirectory.acknowledged).toBe(true);
  const maintenanceModels = await runMaintenance({
    type: "list_provider_models_request",
    requestId: "models",
    provider: "codex",
    cwd: path.join(root, "a"),
  });
  expect(maintenanceModels.models?.map(({ id }) => id)).toEqual(["fixture"]);
  let a = openClient(central.port, "a.paseo.test", "password-a");
  const b = openClient(central.port, "b.paseo.test", "password-b");
  await Promise.all([a.connect(), b.connect()]);
  expect(a.getLastServerInfoMessage()?.serverId).toBe("srv_a");
  expect(b.getLastServerInfoMessage()?.serverId).toBe("srv_b");
  expect((await get(central.port, "b.paseo.test", "/api/status", "password-a")).status).toBe(401);
  for (const client of [a, b]) {
    expect(client.getLastServerInfoMessage()?.features).toMatchObject({
      providersSnapshot: true,
      plugins: true,
      pluginManagement: true,
    });
  }
  const catalog = await Promise.all([a.listAvailableProviders(), b.listAvailableProviders()]);
  expect(catalog.map((result) => result.providers.map((entry) => entry.provider))).toEqual([
    ["codex"],
    ["omp"],
  ]);
  const models = await Promise.all([a.listProviderModels("codex"), b.listProviderModels("omp")]);
  expect(models.map((result) => result.error)).toEqual([null, null]);
  expect(models.map((result) => result.models.map((model) => model.id))).toEqual([
    ["fixture"],
    ["fixture/test"],
  ]);
  await Promise.all(
    (
      [
        [a, "a", "codex", "fixture"],
        [b, "b", "omp", "fixture/test"],
      ] as const
    ).map(async ([client, label, provider, modelId]) => {
      // Central must preserve the worker path in both the snapshot key and the update.
      const cwd = `${path.join(root, label)}/.`;
      const events = client.observeEvents(["providers_snapshot_update"]);
      cleanups.push(() => events.release());
      const ready = Promise.withResolvers<void>();
      events.subscribe({
        snapshot() {},
        update(message) {
          if (message.type !== "providers_snapshot_update" || message.payload.cwd !== cwd) return;
          const entry = message.payload.entries.find((item) => item.provider === provider);
          if (entry?.models?.some((model) => model.id === modelId)) ready.resolve();
        },
      });
      await events.ready;
      const snapshot = await client.getProvidersSnapshot({ cwd });
      expect(snapshot.cwd).toBe(cwd);
      expect(snapshot.entries.map((entry) => entry.provider)).toEqual([provider]);
      if (!snapshot.entries[0].models?.some((model) => model.id === modelId)) await ready.promise;
      expect(
        (await client.getProvidersSnapshot({ cwd })).entries[0].models?.map(({ id }) => id),
      ).toEqual([modelId]);
    }),
  );
  const projects = await Promise.all([a.listProjects(), b.listProjects()]);
  expect(
    projects.map((result) => result.projects.map((entry) => entry.projectDisplayName)),
  ).toEqual([["a"], ["b"]]);
  const workspaces = a.observeWorkspaces();
  cleanups.push(() => workspaces.release());
  const observedAgents = a.observeAgents();
  cleanups.push(() => observedAgents.release());
  const directories = await Promise.all([workspaces.ready, b.fetchWorkspaces()]);
  await observedAgents.ready;
  const observedIds: string[] = [];
  observedAgents.subscribe({
    snapshot() {},
    update(message) {
      if (message.type === "agent_update" && message.payload.kind === "upsert")
        observedIds.push(message.payload.agent.id);
    },
  });
  expect(directories.map((result) => result.entries.map((entry) => entry.name))).toEqual([
    ["a"],
    ["b"],
  ]);
  const agentId = randomUUID();
  const start = (client: DaemonClient, label: string, provider: string) =>
    client.createAgent({
      agentId,
      cwd: path.join(root, label),
      workspaceId: "same-workspace",
      provider,
      idempotencyKey: "same-creation-key",
    });
  const agents = await Promise.all([start(a, "a", "codex"), start(b, "b", "omp")]);
  expect(agents.map((agent) => [agent.id, agent.provider])).toEqual([
    [agentId, "codex"],
    [agentId, "omp"],
  ]);
  await expect.poll(() => observedIds).toContain(agentId);
  const received: string[] = [];
  const timeline = a.subscribeAgentTimeline(agentId, (message) => received.push(message.type));
  await timeline.ready;
  cleanups.push(() => timeline.release());
  const send = (client: DaemonClient) =>
    client.sendAgentMessage(agentId, "hello", { messageId: "same-message-key" });
  expect(await Promise.allSettled([send(a), send(b)])).toEqual([
    expect.objectContaining({ status: "fulfilled" }),
    expect.objectContaining({ status: "fulfilled" }),
  ]);
  await send(a);
  await expect.poll(() => received).toContain("agent_stream");
  await timeline.release();
  const reopened = openClient(central.port, "a.paseo.test", "password-a");
  await reopened.connect();
  const resumedTimeline = reopened.subscribeAgentTimeline(agentId, () => {});
  cleanups.push(() => resumedTimeline.release());
  await resumedTimeline.ready;
  const history = await reopened.fetchAgentTimeline(agentId);
  expect(history.entries).toEqual(
    expect.arrayContaining([
      expect.objectContaining({ item: expect.objectContaining({ type: "user_message" }) }),
    ]),
  );
  for (const label of ["a", "b"])
    expect(await readFile(path.join(root, label, "prompts"), "utf8")).toBe("prompt\n");
  await a.cancelAgent(agentId);
  const fetchAgent = { type: "fetch_agent_request", requestId: "fetch", agentId } as const;
  async function checkPermissions() {
    const readOnly = openClient(central.port, "a.paseo.test", "password-a", ["workspace.read"]);
    await readOnly.connect();
    expect((await readOnly.fetchAgent(agentId))?.agent.id).toBe(agentId);
    await expect(readOnly.archiveAgent(agentId)).rejects.toThrow("Permission denied");
    await expect(readOnly.refreshAgent(agentId)).rejects.toThrow("Permission denied");
    await expect(readOnly.restartServer()).rejects.toThrow("Permission denied");
    await expect(a.refreshAgent("missing")).rejects.toMatchObject({ code: "agent_refresh_failed" });
    const page = await runMaintenance({
      type: "fetch_agents_request",
      requestId: "page",
      page: { limit: 1 },
    });
    expect(page.entries.map(({ agent }) => agent.provider)).toEqual(["codex"]);
    expect(
      (await runMaintenance({ type: "fetch_agent_request", requestId: "find", agentId })).agent
        ?.provider,
    ).toBe("codex");
    await expect(
      central.maintenance(
        { ...grant, target: { ...grant.target, podUid: "stale" } },
        fetchAgent,
        maintenanceSignal,
      ),
    ).rejects.toThrow("worker_maintenance_unconfirmed");
    await expect(
      central.maintenance({ ...grant, expiresAt: Date.now() - 1 }, fetchAgent, maintenanceSignal),
    ).rejects.toThrow("worker_maintenance_expired");
    await expect(
      central.maintenance({ ...grant, scope: ["daemon.read"] }, fetchAgent, maintenanceSignal),
    ).rejects.toThrow("worker_maintenance_permission_denied");
    return readOnly;
  }
  const readOnly = await checkPermissions();
  async function checkScopedRefresh() {
    const holdA = path.join(root, "a", "hold-turn");
    const holdB = path.join(root, "b", "hold-turn");
    await Promise.all([writeFile(holdA, ""), writeFile(holdB, "")]);
    await Promise.all(
      [a, b].map((client) => client.sendAgentMessage(agentId, "held", { messageId: "held-turn" })),
    );
    expect((await a.fetchAgent(agentId))?.agent.status).toBe("running");
    expect((await b.fetchAgent(agentId))?.agent.status).toBe("running");
    const countResumes = async () =>
      (
        await readFile(path.join(root, "a", "resumes"), "utf8").catch(
          (error: NodeJS.ErrnoException) => {
            if (error.code === "ENOENT") return "";
            throw error;
          },
        )
      ).split("resume\n").length - 1;
    const beforePublicRefresh = await countResumes();
    expect(await a.refreshAgent(agentId)).toMatchObject({ status: "agent_refreshed", agentId });
    const beforeMaintenance = await countResumes();
    expect(beforeMaintenance).toBeGreaterThan(beforePublicRefresh);
    expect((await a.fetchAgentTimeline(agentId)).entries).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          item: expect.objectContaining({ type: "user_message", text: "provider history" }),
        }),
      ]),
    );
    expect(await readFile(path.join(root, "a", "interrupts"), "utf8")).toContain("interrupt\n");
    expect((await b.fetchAgent(agentId))?.agent.status).toBe("running");
    await Promise.all([rm(holdA), rm(holdB)]);
    await expect.poll(async () => (await b.fetchAgent(agentId))?.agent.status).toBe("idle");
    expect(
      await runMaintenance({ type: "refresh_agent_request", agentId, requestId: "refresh" }),
    ).toMatchObject({
      status: "agent_refreshed",
      agentId,
      requestId: "refresh",
    });
    expect(await countResumes()).toBeGreaterThan(beforeMaintenance);
    for (const label of ["a", "b"])
      expect(await readFile(path.join(root, label, "prompts"), "utf8")).toBe("prompt\nprompt\n");
    const archived = await a.archiveAgent(agentId);
    expect(archived.archivedAt).toBeTruthy();
    expect((await b.fetchAgent(agentId))?.agent.archivedAt).toBeFalsy();
  }
  await checkScopedRefresh();
  const creations = path.join(root, "a", "creations");
  const receipt = (await readdir(creations)).find((file) => file.endsWith(".json"));
  expect(await readFile(path.join(creations, receipt!), "utf8")).toContain("completed");
  await expect(readOnly.listPlugins()).rejects.toThrow("Permission denied");
  await expect(readOnly.reloadPlugin("fixture-plugin")).rejects.toThrow("Permission denied");
  const plugins = await Promise.all([a.listPlugins(), b.listPlugins()]);
  expect(plugins.map((items) => items.map(({ id }) => id))).toEqual([
    ["fixture-plugin"],
    ["fixture-plugin"],
  ]);
  for (const [client, items] of [
    [a, plugins[0]],
    [b, plugins[1]],
  ] as const) {
    expect(items[0].status, JSON.stringify(await client.getPluginLogs("fixture-plugin"))).toBe(
      "running",
    );
  }
  const bundles = await Promise.all([a.getPluginCatalog(), b.getPluginCatalog()]);
  expect(bundles[0][0].clientBundleHash).not.toBe(bundles[1][0].clientBundleHash);
  const rpc = (client: DaemonClient) => client.invokePluginRpc("fixture-plugin", "marker", {});
  const [markerA, markerB] = await Promise.all([rpc(a), rpc(b)]);
  expect(markerA).toEqual(expect.objectContaining({ marker: "a" }));
  expect(markerB).toEqual(expect.objectContaining({ marker: "b" }));
  await a.reloadPlugin("fixture-plugin");
  expect(await rpc(a)).not.toEqual(markerA);
  expect(await rpc(b)).toEqual(markerB);
  const createTerminal = (client: DaemonClient, label: string) =>
    client.createTerminal(path.join(root, label), "fixture", undefined, {
      workspaceId: "same-workspace",
      command: process.execPath,
      args: [
        "-e",
        `console.log(${JSON.stringify("terminal-" + label)});setInterval(() => {}, 1000)`,
      ],
    });
  const terminals = await Promise.all([createTerminal(a, "a"), createTerminal(b, "b")]);
  const [termA, termB] = terminals;
  expect(termA.error).toBeNull();
  expect(termB.error).toBeNull();
  if (!termA.terminal || !termB.terminal) throw new Error("Remote terminals were not created");
  const terminalA = termA.terminal.id;
  const terminalB = termB.terminal.id;
  const view = (client: DaemonClient, terminalId: string) => {
    let contents = "";
    const subscription = client.observeTerminal(terminalId, (event) => {
      if (event.type === "output") contents += Buffer.from(event.data).toString("utf8");
      else if (event.replay.type === "data")
        contents += Buffer.from(event.replay.data).toString("utf8");
    });
    cleanups.push(() => subscription.release());
    return { ready: subscription.ready, contents: () => contents };
  };
  const [viewA, viewB] = [view(a, terminalA), view(b, terminalB)];
  await Promise.all([viewA.ready, viewB.ready]);
  await expect.poll(viewA.contents).toContain("terminal-a");
  await expect.poll(viewB.contents).toContain("terminal-b");
  expect(viewA.contents()).not.toContain("terminal-b");
  const markerBeforeRestart = await rpc(a);
  await a.refreshAgent(agentId);
  const pidPath = path.join(root, "a", "session-pid");
  const pidBeforeRestart = Number(await readFile(pidPath, "utf8"));
  const terminalIds = async (client: DaemonClient) =>
    (
      await client.listTerminals(undefined, undefined, { workspaceId: "same-workspace" })
    ).terminals.map(({ id }) => id);
  async function restartA(killTerminals: boolean, requestId: string) {
    const previous = a;
    expect(
      await previous.restartServer("settings_update", requestId, { killTerminals }),
    ).toMatchObject({
      status: "restart_requested",
      requestId,
      clientId: "test-a.paseo.test",
      reason: "settings_update",
    });
    await expect.poll(() => previous.getConnectionState().status).not.toBe("connected");
    await previous.close();
    a = openClient(central.port, "a.paseo.test", "password-a");
    await a.connect();
  }
  await restartA(false, "restart-keep");
  expect(await terminalIds(a)).toContain(terminalA);
  const reattached = view(a, terminalA);
  await reattached.ready;
  await expect.poll(reattached.contents).toContain("terminal-a");
  expect(await rpc(a)).not.toEqual(markerBeforeRestart);
  expect(await rpc(b)).toEqual(markerB);
  expect(() => process.kill(pidBeforeRestart, 0)).toThrow();
  expect((await a.fetchAgent(agentId))?.agent.id).toBe(agentId);
  await a.refreshAgent(agentId);
  expect(Number(await readFile(pidPath, "utf8"))).not.toBe(pidBeforeRestart);
  await restartA(true, "restart-kill");
  expect(await terminalIds(a)).not.toContain(terminalA);
  expect(await terminalIds(b)).toContain(terminalB);
  expect(await rpc(b)).toEqual(markerB);
  await b.killTerminal(terminalB);
  const download = await a.requestDownloadToken(path.join(root, "a"), "marker.txt");
  const route = `/api/files/download?token=${encodeURIComponent(download.token!)}`;
  expect((await get(central.port, "b.paseo.test", route)).status).toBe(403);
  expect(await get(central.port, "a.paseo.test", route)).toEqual({ status: 200, body: "a-only" });
  expect((await get(central.port, "a.paseo.test", route)).status).toBe(403);
  async function checkDirectoryCreation() {
    const workspaceId = "wks_0123456789abcdef";
    const newWorkspace = (
      client: DaemonClient,
      label: string,
      provider: string,
      onEvent?: (snapshot: { phase: string }) => void,
    ) =>
      client.createWorkspace({
        workspaceId,
        idempotencyKey: "same-workspace-create",
        source: { kind: "directory", path: path.join(root, label), projectId: "same-project" },
        agent: {
          config: { provider, cwd: path.join(root, label) },
          initialPrompt: "directory workspace turn",
        },
        onEvent,
      });
    const workspacePhases: string[] = [];
    const fresh = await Promise.all([
      newWorkspace(a, "a", "codex", (snapshot) => workspacePhases.push(snapshot.phase)),
      newWorkspace(b, "b", "omp"),
    ]);
    expect(fresh.map((result) => result.error)).toEqual([null, null]);
    expect(fresh.map((result) => result.workspace?.id)).toEqual([workspaceId, workspaceId]);
    expect(fresh.map((result) => result.agent?.provider)).toEqual(["codex", "omp"]);
    expect(workspacePhases).toContain("workspace_ready");
    expect((await newWorkspace(a, "a", "codex")).agent?.id).toBe(fresh[0].agent?.id);
    for (const label of ["a", "b"]) {
      expect(await readFile(path.join(root, label, "prompts"), "utf8")).toBe(
        "prompt\nprompt\nprompt\n",
      );
      expect(
        JSON.parse(await readFile(path.join(root, label, "projects", "workspaces.json"), "utf8")),
      ).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ workspaceId, projectId: "same-project" }),
        ]),
      );
    }
  }
  await checkDirectoryCreation();
  await a.close();
  await reopened.close();
  await readOnly.close();
  await residents.get("a")!.close();
  const nextBinding = { workerId: "a", podUid: "pod-a-replacement", serverId: "srv_a" };
  const nextNative = createNativeExecution({
    paseoHome: path.join(root, "a"),
    logger,
    providers: {},
  });
  const nextResident = await startResidentExecution({
    binding: nextBinding,
    paseoHome: path.join(root, "a"),
    listen: { host: "127.0.0.1", port: 0 },
    tls,
    authorizeCentral: (fingerprint) => fingerprint === tls.fingerprint,
    async openRuntime(...args) {
      const { execute, close } = await nextNative.openRuntime(...args);
      return { execute, close };
    },
    shutdownRuntime: nextNative.close,
    onError: (error) => logger.error({ error }),
  });
  cleanups.push(() => nextResident.close());
  const replacement = {
    ...grant,
    target: {
      ...grant.target,
      ...nextBinding,
      endpoint: `wss://127.0.0.1:${nextResident.port}/execute`,
    },
    expiresAt: Date.now() + 30_000,
  };
  expect((await central.maintenance(replacement, fetchAgent, maintenanceSignal)).agent?.id).toBe(
    agentId,
  );
  await expect(central.maintenance(grant, fetchAgent, maintenanceSignal)).rejects.toThrow(
    "worker_maintenance_stale_target",
  );
  expect((await central.maintenance(replacement, fetchAgent, maintenanceSignal)).agent?.id).toBe(
    agentId,
  );
  targets.get("a.paseo.test")!.target = replacement.target;
  const oldRuntime = openClient(central.port, "a.paseo.test", "password-a");
  await oldRuntime.connect();
  expect(oldRuntime.getLastServerInfoMessage()?.features?.terminalsSurviveRestart).toBeUndefined();
  await expect(oldRuntime.restartServer()).rejects.toMatchObject({
    code: "worker_restart_unsupported",
  });
  expect((await oldRuntime.fetchAgent(agentId))?.agent.id).toBe(agentId);
  expect(await rpc(b)).toEqual(markerB);
  failRestart.add("b");
  await expect(b.restartServer()).rejects.toMatchObject({ code: "worker_restart_unconfirmed" });
  await expect.poll(() => restartFailures).toEqual(["b"]);
  expect((await oldRuntime.fetchAgent(agentId))?.agent.id).toBe(agentId);
});
