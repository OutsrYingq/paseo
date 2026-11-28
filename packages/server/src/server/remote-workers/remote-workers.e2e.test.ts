import { createRequire } from "node:module";
import { X509Certificate, randomUUID } from "node:crypto";
import { mkdtemp, mkdir, readdir, readFile, rm, writeFile } from "node:fs/promises";
import { createServer, request } from "node:http";
import { createConnection } from "node:net";
import os from "node:os";
import path from "node:path";
import pino from "pino";
import { afterEach, expect, it } from "vitest";
import { execFileSync } from "node:child_process";
import { WebSocket } from "ws";
import { DaemonClient, type WebSocketLike } from "@getpaseo/client/internal/daemon-client";
import { AgentStorage } from "../agent/agent-storage.js";
import { acquirePidLock, getPidLockInfo, releasePidLock } from "../pid-lock.js";
import { BuiltinPluginLoader } from "../plugins/builtin/index.js";
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
const sessionArg = process.argv.indexOf('--session');
if (provider === 'omp' && sessionArg !== -1 && fs.existsSync(process.argv[sessionArg + 1])) {
  const sessionFile = process.argv[sessionArg + 1];
  const entries = fs.readFileSync(sessionFile, 'utf8').trim().split('\n').map(JSON.parse);
  const header = entries.find((entry) => entry.type === 'session');
  state.sessionId = header.id;
  state.sessionFile = sessionFile;
  state.messageCount = entries.filter((entry) => entry.type === 'message').length;
  fs.appendFileSync(process.env.FIXTURE_STATE + '/omp-history-rpc', JSON.stringify({ type: 'resume', sessionFile, id: header.id }) + '\n');
}
const count = () => fs.appendFileSync(process.env.FIXTURE_STATE + '/prompts', 'prompt\n');
const turnHeld = () => fs.existsSync(process.env.FIXTURE_STATE + '/hold-turn');
if (provider === 'omp') send({ type: 'ready', protocolVersion: 1, supportedProtocolVersions: [1] });
readline.createInterface({ input: process.stdin }).on('line', (line) => {
  const command = JSON.parse(line);
  const { id, method, type } = command;
  if (!id) return;
  if (provider === 'codex') {
    const turnInput = method === 'turn/start' ? JSON.stringify(command.params.input) : '';
    const generateCommit = turnInput.includes('Write a concise git commit message');
    if (method === 'turn/start' && !generateCommit) {
      count();
      fs.writeFileSync(process.env.FIXTURE_STATE + '/last-prompt', turnInput);
    }
    if (method === 'thread/start' || method === 'thread/resume') fs.writeFileSync(process.env.FIXTURE_STATE + '/session-pid', String(process.pid));
    if (method === 'thread/start' && command.params?.config?.mcp_servers?.paseo) {
      fs.writeFileSync(process.env.FIXTURE_STATE + '/agent-mcp', JSON.stringify(command.params.config.mcp_servers.paseo));
    }
    if (method === 'thread/resume') fs.appendFileSync(process.env.FIXTURE_STATE + '/resumes', 'resume\n');
    const result = ({ initialize: { userAgent: 'codex/0.121.0' }, 'model/list': { data: [{ id: 'fixture', model: 'fixture', isDefault: true, displayName: 'Fixture', description: 'fixture', hidden: false, supportedReasoningEfforts: [], defaultReasoningEffort: 'medium' }] }, 'thread/start': { thread: { id: 'shared-native-id' } }, 'thread/resume': { thread: { id: 'shared-native-id' } }, 'thread/read': { thread: { id: 'shared-native-id', turns: [{ id: 't1', status: 'interrupted', items: [{ id: 'rehydrated', type: 'userMessage', content: [{ type: 'text', text: 'provider history', text_elements: [] }] }] }] } }, 'turn/start': { turn: { id: 't1', status: 'inProgress' } } })[method] ?? {};
    if (id === 8001) {
      fs.writeFileSync(process.env.FIXTURE_STATE + '/permission-decision', command.result?.decision ?? 'missing');
      send({ method: 'turn/completed', params: { threadId: 'shared-native-id', turn: { id: 't1', status: 'completed' } } });
      return;
    }
    send({ id, result });
    if (method === 'turn/start') {
      send({ method: 'turn/started', params: { threadId: 'shared-native-id', turn: { id: 't1', status: 'inProgress' } } });
      if (generateCommit) {
        fs.writeFileSync(process.env.FIXTURE_STATE + '/metadata-prompt', turnInput);
        send({ method: 'item/completed', params: {
          threadId: 'shared-native-id', turnId: 't1',
          item: { id: 'generated', type: 'agentMessage', text: JSON.stringify({ message: 'Document generated change' }) }
        } });
        send({ method: 'turn/completed', params: { threadId: 'shared-native-id', turn: { id: 't1', status: 'completed' } } });
        return;
      }
      if (fs.existsSync(process.env.FIXTURE_STATE + '/require-permission')) {
        send({ method: 'item/completed', params: {
          threadId: 'shared-native-id', turnId: 't1',
          item: { type: 'collabAgentToolCall', id: 'child-call', tool: 'spawnAgent', status: 'completed',
            prompt: 'Inspect fixture directory', receiverThreadIds: ['fixture-child'],
            agentsStates: { 'fixture-child': { status: 'pendingInit', message: null } } }
        } });
        send({ method: 'item/completed', params: {
          threadId: 'fixture-child', turnId: 'child-turn',
          item: { type: 'agentMessage', id: 'child-reply', text: 'Found the fixture directory.' }
        } });
        send({ jsonrpc: '2.0', id: 8001, method: 'item/commandExecution/requestApproval',
          params: { itemId: 'fixture-command', threadId: 'shared-native-id', turnId: 't1', command: 'echo fixture', cwd: process.env.FIXTURE_STATE } });
      } else if (!turnHeld()) setTimeout(() => send({ method: 'turn/completed', params: { threadId: 'shared-native-id', turn: { id: 't1', status: 'completed' } } }), 250);
    }
    if (method === 'turn/interrupt') {
      fs.appendFileSync(process.env.FIXTURE_STATE + '/interrupts', 'interrupt\n');
      send({ method: 'turn/completed', params: { threadId: 'shared-native-id', turn: { id: 't1', status: 'interrupted' } } });
    }
  } else {
    if (type === 'branch') {
      fs.appendFileSync(process.env.FIXTURE_STATE + '/omp-history-rpc', JSON.stringify({ type, entryId: command.entryId }) + '\n');
      send({ id, type: 'response', command: type, success: true, data: { text: '' } });
      return;
    }
    const result = ({ get_state: state, get_messages: { messages: [] }, get_available_commands: { commands: [{ name: 'fixture-command', description: 'Worker command', source: 'extension' }] }, get_available_models: { models: [{ provider: 'fixture', id: 'test', name: 'Fixture OMP' }] }, get_session_stats: {}, prompt: { agentInvoked: true } })[type] ?? {};
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

it("shares the standalone writer lock and releases it on resident shutdown", async () => {
  const paseoHome = await mkdtemp(path.join(os.tmpdir(), "paseo-resident-lock-"));
  cleanups.push(() => rm(paseoHome, { recursive: true, force: true }));
  const binding = { workerId: "a", podUid: "pod-a", serverId: "srv_a" };
  await writeFile(path.join(paseoHome, "server-id"), binding.serverId);
  const tls = testTls();
  const start = () =>
    startResidentExecution({
      binding,
      paseoHome,
      listen: { host: "127.0.0.1", port: 0 },
      tls,
      authorizeCentral: (fingerprint) => fingerprint === tls.fingerprint,
      async openRuntime() {
        throw new Error("unexpected execution connection");
      },
      onError: (error) => {
        throw error;
      },
    }).then((resident) => {
      cleanups.push(() => resident.close());
      return resident;
    });

  await acquirePidLock(paseoHome, null);
  try {
    await expect(start()).rejects.toThrow("Another Paseo daemon is already running");
  } finally {
    await releasePidLock(paseoHome);
  }

  const resident = await start();
  expect(await getPidLockInfo(paseoHome)).toMatchObject({ pid: process.pid, listen: null });
  await expect(start()).rejects.toThrow("Another Paseo daemon is already running");
  await expect(acquirePidLock(paseoHome, null, { ownerPid: process.pid + 1 })).rejects.toThrow(
    "Another Paseo daemon is already running",
  );
  await resident.close();
  expect(await getPidLockInfo(paseoHome)).toBeNull();
  expect(await readFile(path.join(paseoHome, "server-id"), "utf8")).toBe(binding.serverId);
});

function probeTcp(port: number): Promise<boolean> {
  return new Promise((resolve) => {
    const socket = createConnection({ host: "127.0.0.1", port });
    socket.on("error", () => resolve(false));
    socket.once("connect", () => {
      socket.destroy();
      resolve(true);
    });
  });
}

it("imports and rewinds an OMP history through the public SDK", async () => {
  const tls = testTls();
  const logger = pino({ level: "silent" });
  const root = await mkdtemp(path.join(os.tmpdir(), "paseo-remote-history-"));
  cleanups.push(() => rm(root, { recursive: true, force: true }));
  const script = path.join(root, "fixture.cjs");
  await writeFile(script, nativeFixture);
  const transcriber = createServer((req, res) => {
    req.resume();
    req.on("end", () => {
      if (req.method !== "POST" || req.url !== "/v1/audio/transcriptions") {
        res.writeHead(404).end();
        return;
      }
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ text: "Resident transcript" }));
    });
  });
  await new Promise<void>((resolve) => transcriber.listen(0, "127.0.0.1", resolve));
  cleanups.push(() => new Promise<void>((resolve) => transcriber.close(() => resolve())));
  const speechAddress = transcriber.address();
  if (!speechAddress || typeof speechAddress === "string") throw new Error("Missing speech fixture address");
  const pushes: Array<{ host: string; tokens: string[]; agentId: unknown }> = [];
  const timestamp = new Date().toISOString();
  const workspaceId = "same-workspace";
  const projectId = "same-project";
  const targets = new Map<string, WorkerTarget>();
  for (const label of ["owner", "other"]) {
    const local = path.join(root, label);
    const sessions = path.join(local, "native-sessions");
    await mkdir(path.join(local, "projects"), { recursive: true });
    await mkdir(sessions);
    await writeFile(
      path.join(local, "config.json"),
      JSON.stringify({
        features: {
          dictation: label === "owner"
            ? {
                enabled: true,
                stt: { provider: "openai", baseUrl: `http://127.0.0.1:${speechAddress.port}/v1`, apiKey: "<fixture>" },
              }
            : { enabled: false },
        },
      }),
    );
    const record = { projectId, createdAt: timestamp, updatedAt: timestamp, archivedAt: null };
    await writeFile(
      path.join(local, "projects", "projects.json"),
      JSON.stringify([{ ...record, rootPath: local, kind: "non_git", displayName: label }]),
    );
    await writeFile(
      path.join(local, "projects", "workspaces.json"),
      JSON.stringify([
        { ...record, workspaceId, cwd: local, kind: "directory", displayName: label },
      ]),
    );
    const native = createNativeExecution({
      paseoHome: local,
      workspaceRoots: [local],
      logger,
      async pushDeliver(tokens, payload) {
        pushes.push({ host: label, tokens, agentId: payload.data?.agentId });
      },
      providers: {
        omp: {
          command: { mode: "replace", argv: [process.execPath, script, "omp"] },
          env: { FIXTURE_STATE: local, PI_CODING_AGENT_SESSION_DIR: sessions },
        },
      },
    });
    const binding = { workerId: label, podUid: `pod-${label}`, serverId: `srv_${label}` };
    await writeFile(path.join(local, "server-id"), `${binding.serverId}\n`);
    const resident = await startResidentExecution({
      binding,
      paseoHome: local,
      listen: { host: "127.0.0.1", port: 0 },
      tls,
      authorizeCentral: (fingerprint) => fingerprint === tls.fingerprint,
      openRuntime: native.openRuntime,
      shutdownRuntime: native.close,
      onError: (error) => logger.error({ error }),
    });
    cleanups.push(() => resident.close());
    targets.set(`${label}.paseo.test`, {
      ...binding,
      endpoint: `wss://127.0.0.1:${resident.port}/execute`,
      certFingerprint256: tls.fingerprint,
      revision: 1,
    });
  }
  const central = await startRemoteCentral({
    authority: {
      async admit(hostname, credential) {
        const target = targets.get(hostname);
        if (!target) return { kind: "unknown_host" };
        if (credential.kind !== "password" || credential.password !== target.workerId) {
          return { kind: "unauthorized" };
        }
        return {
          kind: "allowed",
          grant: {
            target,
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
    },
    workerTls: tls,
    listen: { host: "127.0.0.1", port: 0 },
    version: "0.18.0",
    logger,
  });
  cleanups.push(() => central.close());
  const client = openClient(central.port, "owner.paseo.test", "owner");
  const other = openClient(central.port, "other.paseo.test", "other");
  await Promise.all([client.connect(), other.connect()]);
  const audioFormat = "audio/pcm;rate=16000;bits=16";
  await client.startDictationStream("same-dictation", audioFormat);
  await expect(other.startDictationStream("same-dictation", audioFormat)).rejects.toThrow();
  const pcm = Buffer.alloc(32_000);
  for (let i = 0; i < pcm.length; i += 2) pcm.writeInt16LE(4000 * Math.sin(i / 16), i);
  client.sendDictationStreamChunk("same-dictation", 0, pcm.toString("base64"), audioFormat);
  await expect(client.finishDictationStream("same-dictation", 0)).resolves.toMatchObject({
    text: "Resident transcript",
  });
  const speechConfig = await client.getDaemonConfig();
  expect(speechConfig.config.features?.dictation).toMatchObject({
    enabled: true,
    stt: { provider: "openai", hasApiKey: true },
  });
  expect(speechConfig.config.features?.dictation.stt).not.toHaveProperty("apiKey");
  await client.patchDaemonConfig({ features: { dictation: { enabled: false } } });
  await expect(client.startDictationStream("disabled-dictation", audioFormat)).rejects.toThrow();

  const cwd = path.join(root, "owner");
  const id = randomUUID();
  const sessionFile = path.join(cwd, "native-sessions", `test_${id}.jsonl`);
  const checkpoint = "user-2";
  const archive = [
    { type: "session", version: 3, id, timestamp, cwd },
    {
      type: "message",
      id: "user-1",
      parentId: null,
      timestamp,
      message: { role: "user", content: "first prompt" },
    },
    {
      type: "message",
      id: "reply-1",
      parentId: "user-1",
      timestamp,
      message: { role: "assistant", content: [{ type: "text", text: "first reply" }] },
    },
    {
      type: "message",
      id: checkpoint,
      parentId: "reply-1",
      timestamp,
      message: { role: "user", content: "second prompt" },
    },
    {
      type: "message",
      id: "reply-2",
      parentId: checkpoint,
      timestamp,
      message: { role: "assistant", content: [{ type: "text", text: "second reply" }] },
    },
  ]
    .map((entry) => JSON.stringify(entry))
    .join("\n");
  await writeFile(sessionFile, archive);
  const recent = { providers: ["omp"] };
  expect((await client.fetchRecentProviderSessions(recent)).entries).toContainEqual(
    expect.objectContaining({ providerHandleId: sessionFile, cwd }),
  );
  expect((await other.fetchRecentProviderSessions(recent)).entries).toEqual([]);

  const request = { providerId: "omp", providerHandleId: sessionFile, cwd, workspaceId };
  await expect(other.importAgent({ ...request, cwd: path.join(root, "other") })).rejects.toThrow(
    "worker_omp_import_session_unavailable",
  );
  const imported = await client.importAgent(request);
  expect(imported).toMatchObject({ provider: "omp", cwd, workspaceId });
  await expect(client.importAgent({ ...request, providerHandleId: id })).rejects.toThrow(
    "Provider session is already imported",
  );
  expect((await client.fetchRecentProviderSessions(recent)).entries).toEqual([]);
  const readNativeCalls = async (): Promise<unknown[]> =>
    (await readFile(path.join(cwd, "omp-history-rpc"), "utf8"))
      .trim()
      .split("\n")
      .map((line) => JSON.parse(line));
  expect(await readNativeCalls()).toEqual([{ type: "resume", sessionFile, id }]);

  const before = await client.fetchAgentTimeline(imported.id);
  expect(await client.searchAgentTimeline({ agentId: imported.id, query: "reply" })).toMatchObject({
    epoch: before.epoch,
    locations: [
      { seq: before.entries[1]?.seqEnd, role: "assistant", count: 1 },
      { seq: before.entries[3]?.seqEnd, role: "assistant", count: 1 },
    ],
    nextCursor: null,
  });
  expect((await client.listAgentTimelinePrompts(imported.id)).prompts.map(({ preview }) => preview))
    .toEqual(["first prompt", "second prompt"]);
  await expect(other.searchAgentTimeline({ agentId: imported.id, query: "reply" }))
    .rejects.toThrow("Agent not found");
  const [first, reply] = before.entries;
  if (!first || reply?.item.type !== "assistant_message" || !reply.item.messageId) {
    throw new Error("Imported timeline is missing its first assistant reply");
  }
  expect(first.item).toMatchObject({
    type: "user_message",
    text: "first prompt",
    messageId: "user-1",
  });
  expect(before.entries.map(({ item }) => ("text" in item ? item.text : undefined))).toEqual([
    "first prompt",
    "first reply",
    "second prompt",
    "second reply",
  ]);
  const boundaryCursor = { epoch: before.epoch, seq: reply.seqEnd };
  const fork = await client.buildAgentForkContext(imported.id, { boundaryCursor });
  expect(fork).toMatchObject({ boundaryCursor, itemCount: 2 });
  expect(fork.attachment?.text).toContain("first prompt");
  expect(fork.attachment?.text).toContain("first reply");
  expect(fork.attachment?.text).not.toContain("second prompt");
  const forkByMessage = await client.buildAgentForkContext(imported.id, {
    boundaryMessageId: reply.item.messageId,
  });
  expect(forkByMessage).toMatchObject({
    boundaryMessageId: reply.item.messageId,
    itemCount: 2,
    attachment: fork.attachment,
  });
  await expect(other.buildAgentForkContext(imported.id)).rejects.toThrow("Agent not found");
  await expect(other.rewindAgent(imported.id, checkpoint, "conversation")).rejects.toThrow(
    "Agent not found",
  );

  const replacements: string[] = [];
  const timeline = client.subscribeAgentTimeline(imported.id, (message) => {
    if (message.type === "agent.timeline.replacement") replacements.push(message.payload.epoch);
  });
  cleanups.push(() => timeline.release());
  await timeline.ready;
  await expect(client.rewindAgent(imported.id, checkpoint, "conversation")).resolves.toMatchObject({
    ok: true,
  });
  expect(await readNativeCalls()).toEqual([
    { type: "resume", sessionFile, id },
    { type: "branch", entryId: checkpoint },
  ]);
  const after = await client.fetchAgentTimeline(imported.id);
  expect(after.epoch).not.toBe(before.epoch);
  await expect.poll(() => replacements).toEqual([after.epoch]);
  expect(after.entries.map(({ item }) => ("text" in item ? item.text : undefined))).toEqual([
    "first prompt",
    "first reply",
    "second prompt",
  ]);
  expect(await readFile(sessionFile, "utf8")).toBe(archive);

  const recentProject = path.join(cwd, "recent-project");
  const nested = path.join(recentProject, "src");
  await mkdir(nested, { recursive: true });
  execFileSync("git", ["init", recentProject], { stdio: "pipe" });
  expect((await client.getDirectorySuggestions({ query: "", cwd: recentProject })).entries).toEqual([
    { path: "src", kind: "directory" },
  ]);
  expect((await client.getDirectorySuggestions({ query: recentProject })).directories).toContain(
    recentProject,
  );
  expect((await other.getDirectorySuggestions({ query: recentProject })).entries).toEqual([]);
  expect(await other.getDirectorySuggestions({ query: "", cwd: recentProject })).toMatchObject({
    entries: [],
    error: "worker_workspace_root_denied",
  });
  const recentProjectId = randomUUID();
  await writeFile(
    path.join(cwd, "native-sessions", `test_${recentProjectId}.jsonl`),
    JSON.stringify({ type: "session", version: 3, id: recentProjectId, timestamp, cwd: nested }),
  );
  expect((await client.listProjectSuggestions(recent)).suggestions).toMatchObject([
    { rootPath: recentProject, displayName: "recent-project", sessionCount: 1, providerIds: ["omp"] },
  ]);
  expect((await other.listProjectSuggestions(recent)).suggestions).toEqual([]);
  expect((await client.addProject(recentProject)).project?.projectRootPath).toBe(recentProject);
  expect((await client.listProjectSuggestions(recent)).suggestions).toEqual([]);

  const labels = client.observeWorkspaceLabels();
  cleanups.push(() => labels.release());
  const names = new Set<string>();
  labels.subscribe({
    snapshot() {},
    update(message) {
      if (message.type === "workspace.label.update" && message.payload.kind === "upsert") {
        names.add(message.payload.label.name);
      }
    },
  });
  await labels.ready;
  await client.setWorkspaceLabel({
    workspaceId, label: { name: "Review", color: "blue" }, assigned: true,
  });
  await client.updateWorkspaceLabel({ name: "Review", newName: "Reviewed" });
  await expect.poll(() => names.has("Reviewed")).toBe(true);
  expect((await client.fetchWorkspaces()).entries).toContainEqual(
    expect.objectContaining({ id: workspaceId, labels: ["Reviewed"] }),
  );
  expect(JSON.parse(await readFile(path.join(cwd, "projects", "workspace-labels.json"), "utf8")))
    .toEqual([{ name: "Reviewed", color: "blue" }]);
  expect(await other.listWorkspaceLabels()).toMatchObject({ labels: [] });
  await labels.release();

  const ownerToken = "ExponentPushToken[fixture-owner]";
  const otherToken = "ExponentPushToken[fixture-other]";
  client.registerPushToken(ownerToken);
  other.registerPushToken(otherToken);
  await expect.poll(async () => {
    try {
      return await readFile(path.join(cwd, "push-tokens.json"), "utf8");
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") return "";
      throw error;
    }
  }).toContain(ownerToken);
  await client.sendAgentMessage(imported.id, "notify on finish", { messageId: "push-fixture" });
  await expect.poll(() => pushes).toEqual([
    { host: "owner", tokens: [ownerToken], agentId: imported.id },
  ]);
  await client.unregisterPushToken(ownerToken);
  expect(await readFile(path.join(cwd, "push-tokens.json"), "utf8")).not.toContain(ownerToken);
}, 30_000);

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
  const untitledAgentId = randomUUID();
  for (const [label, provider] of [
    ["a", "codex"],
    ["b", "omp"],
    ["fresh", "omp"],
  ] as const) {
    const local = path.join(root, label);
    await mkdir(local);
    await mkdir(path.join(local, "projects"));
    const now = new Date().toISOString();
    if (label !== "fresh") {
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
    }
    await writeFile(path.join(local, "server-id"), `srv_${label}\n`);
    await writeFile(path.join(local, "marker.txt"), `${label}-only`);
    if (label === "a") {
      const storage = new AgentStorage(path.join(local, "agents"), logger);
      await storage.upsert({
        id: untitledAgentId,
        provider,
        cwd: local,
        workspaceId: "same-workspace",
        createdAt: now,
        updatedAt: now,
        lastUserMessageAt: now,
        lastStatus: "closed",
        title: null,
        labels: {},
        metadata: {},
        background: true,
        persistence: { provider, sessionId: "shared-native-id" },
      });
    }
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
      import { readFile } from "node:fs/promises";
      import { z } from "zod";
      const launchId = randomUUID();
      export default function contribute(server) {
        server.handle(defineRpc({ name: "marker", input: z.object({}), output: z.object({ marker: z.string(), launchId: z.string() }) }),
          () => ({ marker: ${JSON.stringify(label)}, launchId }));
        server.handle(defineRpc({ name: "workspace", input: z.object({ id: z.string() }), output: z.string() }),
          async ({ id }, { paseo }) => (await paseo.workspaces.ref(id).refresh())?.workspaceDirectory);
        server.registerUsageSource({
          id: "fixture-quota",
          label: "Fixture quota",
          input: z.object({ usedPct: z.number() }),
          discover: async (scope) => {
            const state = scope.kind === "session" ? scope.env.FIXTURE_STATE : ${JSON.stringify(local)};
            if (!state) return [];
            const marker = await readFile(state + "/marker.txt", "utf8");
            return [{ key: scope.kind, input: { usedPct: marker === "a-only" ? 37 : 64 } }];
          },
          fetch: async ({ usedPct }) => ({
            status: "available",
            windows: [{ id: "weekly", label: "Weekly", usedPct }],
          }),
        });
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
        daemon: { browserTools: { enabled: provider === "codex" } },
        features: { dictation: { enabled: false }, voiceMode: { enabled: false } },
        ...(provider === "codex" && {
          agents: {
            metadataGeneration: {
              providers: [{ provider: "codex", model: "fixture" }],
              instructions: "Use the resident commit instructions.",
            },
          },
        }),
        plugins: {
          "fixture-plugin": { source: "directory", path: pluginDir },
        },
      }),
    );
    const native = createNativeExecution({
      paseoHome: local,
      workspaceRoots: [local],
      logger,
      plugins: { daemonVersion: "0.17.4", builtinPlugins: new BuiltinPluginLoader(undefined, []) },
      providers: {
        [provider]: {
          command: { mode: "replace", argv: [process.execPath, script, provider] },
          env: { FIXTURE_STATE: local, PI_CODING_AGENT_SESSION_DIR: path.join(local, "native-sessions") },
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
      projectList: true,
      workspaceFileEditing: true,
      fileByteWrites: true,
      daemonDiagnostics: true,
      agentForkContextCursor: true,
    });
  }
  const streamedA: string[] = [];
  const [usageA, usageB] = await Promise.all([
    a.listUsageReports({}, (report) => streamedA.push(report.id)),
    b.listUsageReports(),
  ]);
  expect(streamedA).toEqual(["fixture-quota:global"]);
  expect(usageA.reports[0]?.report.windows?.[0]?.usedPct).toBe(37);
  expect(usageB.reports[0]?.report.windows?.[0]?.usedPct).toBe(64);
  expect((await a.listProviderUsage()).providers[0]?.windows[0]?.usedPct).toBe(37);
  expect((await b.patchDaemonConfig({ usage: { enabled: false } })).config.usage).toEqual({
    enabled: false,
  });
  await expect(b.listUsageReports()).rejects.toThrow("Usage is off on this host.");
  expect((await a.listUsageReports()).reports[0]?.report.windows?.[0]?.usedPct).toBe(37);
  await b.patchDaemonConfig({ usage: { enabled: true } });
  const fresh = openClient(central.port, "fresh.paseo.test", "password-fresh");
  await fresh.connect();
  expect((await fresh.listProjects()).projects).toEqual([]);
  expect((await fresh.fetchWorkspaces()).entries).toEqual([]);
  const parentPath = path.join(root, "fresh");
  const created = await fresh.createProjectDirectory({ parentPath, name: "new-project" });
  expect(created).toMatchObject({
    directoryPath: path.join(parentPath, "new-project"),
    project: { projectDisplayName: "new-project" },
    error: null,
  });
  const folder = created.directoryPath!;
  const opened = await Promise.all([fresh.openProject(folder), fresh.openProject(folder)]);
  expect(opened[0]).toMatchObject({
    workspace: { workspaceDirectory: folder, projectId: created.project!.projectId },
    error: null,
  });
  expect(opened[1].workspace?.id).toBe(opened[0].workspace?.id);
  expect(await fresh.createProjectDirectory({ parentPath, name: "new-project" })).toMatchObject({
    errorCode: "directory_exists",
  });
  expect(await b.createProjectDirectory({ parentPath, name: "outside" })).toMatchObject({
    errorCode: "permission_denied",
  });
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
  async function checkUntitledHistory() {
    expect((await a.fetchAgent(untitledAgentId))?.agent.title).toBeNull();
    const untitledHistory = await a.fetchAgentTimeline(untitledAgentId);
    expect(untitledHistory.agent?.title).toBe("provider history");
    expect((await a.fetchAgent(untitledAgentId))?.agent.title).toBe("provider history");
  }
  await checkUntitledHistory();
  const bPath = path.join(root, "b");
  const sessions = path.join(bPath, "native-sessions");
  await mkdir(sessions);
  const importId = randomUUID();
  const importFile = path.join(sessions, `fixture_${importId}.jsonl`);
  const timestamp = new Date().toISOString();
  await writeFile(
    importFile,
    [
      { type: "session", version: 3, id: importId, timestamp, cwd: bPath },
      { type: "message", id: "hello", timestamp, message: { role: "user", content: "resident import marker" } },
    ].map((value) => JSON.stringify(value)).join("\n"),
  );
  const recent = { cwd: bPath, providers: ["omp"] };
  expect((await b.fetchRecentProviderSessions(recent)).entries).toContainEqual(
    expect.objectContaining({ providerHandleId: importFile, cwd: bPath }),
  );
  expect((await fresh.fetchRecentProviderSessions({ providers: ["omp"] })).entries).toEqual([]);
  await expect(a.importAgent({ providerId: "omp", providerHandleId: importFile, cwd: bPath }))
    .rejects.toThrow("worker_provider_unavailable");
  await fresh.close();
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
  expect((await b.listCommands({ agentId })).commands.map(({ name }) => name)).toContain(
    "fixture-command",
  );
  const browserA = a.registerBrowserHost({
    hostKind: "desktop app", label: "browser-a", supportedCommands: ["list_tabs"],
  });
  const browserB = b.registerBrowserHost({
    hostKind: "desktop app", label: "browser-b", supportedCommands: ["list_tabs"],
  });
  cleanups.push(() => browserA.release(), () => browserB.release());
  await Promise.all([browserA.ready, browserB.ready]);
  const mcp = JSON.parse(await readFile(path.join(root, "a", "agent-mcp"), "utf8"));
  const browserCall = async (name: string) =>
    (await fetch(mcp.url, {
      method: "POST",
      headers: {
        ...mcp.http_headers,
        "Content-Type": "application/json",
        Accept: "application/json, text/event-stream",
        "MCP-Protocol-Version": "2025-06-18",
      },
      body: JSON.stringify({
        jsonrpc: "2.0", id: 1, method: "tools/call", params: { name, arguments: {} },
      }),
    })).text();
  const discoveredBrowsers = await browserCall("browser_list_hosts");
  expect(discoveredBrowsers).toContain("browser-a");
  expect(discoveredBrowsers).not.toContain("browser-b");
  expect(await browserCall("browser_list_tabs")).toContain("Browser workspace ID is shared by multiple Hosts");
  await Promise.all([browserA.release(), browserB.release()]);
  expect(await browserCall("browser_list_hosts")).not.toContain("browser-a");
  async function checkSessionParity() {
    await expect.poll(() => observedIds).toContain(agentId);
    await a.setAgentModel(agentId, "fixture");
    expect((await a.fetchAgent(agentId))?.agent.model).toBe("fixture");
    expect((await b.fetchAgent(agentId))?.agent.model).not.toBe("fixture");
    const received: string[] = [];
    const timeline = a.subscribeAgentTimeline(agentId, (message) => received.push(message.type));
    await timeline.ready;
    cleanups.push(() => timeline.release());
    const permissions = a.observeEvents(["agent_permission_request", "agent_permission_resolved"]);
    cleanups.push(() => permissions.release());
    const pending = Promise.withResolvers<string>();
    const resolved = Promise.withResolvers<string>();
    permissions.subscribe({
      snapshot() {},
      update(message) {
        if (message.type === "agent_permission_request")
          pending.resolve(message.payload.request.id);
        if (message.type === "agent_permission_resolved")
          resolved.resolve(message.payload.requestId);
      },
    });
    await permissions.ready;
    const subagentEvents = a.observeEvents(["agent.provider_subagents.update"]);
    cleanups.push(() => subagentEvents.release());
    const childUpdates: string[] = [];
    subagentEvents.subscribe({
      snapshot() {},
      update(message) {
        if (
          message.type === "agent.provider_subagents.update" &&
          message.payload.kind === "timeline" &&
          message.payload.item.type === "assistant_message"
        ) childUpdates.push(message.payload.item.text);
      },
    });
    await subagentEvents.ready;
    const requirePermission = path.join(root, "a", "require-permission");
    await writeFile(requirePermission, "");
    const send = (client: DaemonClient) =>
      client.sendAgentMessage(agentId, "hello", { messageId: "same-message-key" });
    expect(await Promise.allSettled([send(a), send(b)])).toEqual([
      expect.objectContaining({ status: "fulfilled" }),
      expect.objectContaining({ status: "fulfilled" }),
    ]);
    const permissionId = await pending.promise;
    expect((await a.fetchAgent(agentId))?.agent.pendingPermissions.map(({ id }) => id)).toContain(
      permissionId,
    );
    const ownedChild = (await a.listProviderSubagents(agentId)).subagents[0];
    if (!ownedChild) throw new Error("Codex fixture did not publish a subagent");
    expect((await b.listProviderSubagents(agentId)).subagents).toEqual([]);
    expect(await a.fetchProviderSubagentTimeline(agentId, ownedChild.id)).toMatchObject({
      rows: [{ item: { type: "assistant_message", text: "Found the fixture directory." } }],
    });
    await expect(b.fetchProviderSubagentTimeline(agentId, ownedChild.id)).rejects.toThrow(
      "Provider subagent not found",
    );
    await expect.poll(() => childUpdates).toContain("Found the fixture directory.");
    await subagentEvents.release();
    await a.respondToPermission(agentId, permissionId, { behavior: "allow" });
    const scopedUsage = await a.listUsageReports({ agentId });
    expect(scopedUsage.reports).toMatchObject([
      { id: "fixture-quota:session", report: { windows: [{ usedPct: 37 }] } },
    ]);
    await expect
      .poll(async () => readFile(path.join(root, "a", "permission-decision"), "utf8"))
      .toBe("accept");
    expect(await resolved.promise).toBe(permissionId);
    await rm(requirePermission);
    await send(a);
    const bytes = Buffer.from("attachment contents\n");
    const uploaded = await a.uploadFile({ fileName: "notes.txt", mimeType: "text/plain", bytes, chunkSize: 3 });
    expect(uploaded.error).toBeNull();
    expect(await readFile(uploaded.file!.path)).toEqual(bytes);
    await expect(b.sendAgentMessage(agentId, "read foreign attachment", {
      attachments: [{ ...uploaded.file!, type: "uploaded_file" }],
    })).rejects.toThrow("worker_attachment_unavailable");
    await expect(a.sendAgentMessage(agentId, "read substituted attachment", {
      attachments: [{ ...uploaded.file!, type: "uploaded_file", path: path.join(root, "a", "marker.txt") }],
    })).rejects.toThrow("worker_attachment_unavailable");
    expect((await a.readFile(path.join(root, "a"), "marker.txt", undefined, 128, 5_000)).bytes).toEqual(
      new Uint8Array(Buffer.from("a-only")),
    );
    await expect(a.readFile(path.join(root, "a"), "marker.txt", undefined, 1, 5_000)).rejects.toThrow(
      /too large/i,
    );
    await expect.poll(() => received).toContain("agent_stream");
    await timeline.release();
    const reopened = openClient(central.port, "a.paseo.test", "password-a");
    await reopened.connect();
    const resumedTimeline = reopened.subscribeAgentTimeline(agentId, () => {});
    cleanups.push(() => resumedTimeline.release());
    await resumedTimeline.ready;
    const history = await reopened.fetchAgentTimeline(agentId);
    expect(history.agent?.title).toBe("hello");
    await expect
      .poll(async () => (await reopened.fetchAgent(agentId))?.agent.requiresAttention)
      .toBe(true);
    await expect
      .poll(async () => (await b.fetchAgent(agentId))?.agent.requiresAttention)
      .toBe(true);
    await reopened.clearAgentAttention(agentId);
    expect((await reopened.fetchAgent(agentId))?.agent.requiresAttention).toBe(false);
    expect((await b.fetchAgent(agentId))?.agent.requiresAttention).toBe(true);
    await reopened.updateAgent(agentId, { name: "Renamed session" });
    expect((await reopened.fetchAgentTimeline(agentId)).agent?.title).toBe("Renamed session");
    expect((await b.fetchAgent(agentId))?.agent.title).toBe("hello");
    const matchingHistory = await reopened.fetchAgentHistory({
      search: "Renamed session",
      page: { limit: 1 },
    });
    expect(matchingHistory.entries.map(({ agent }) => agent.title)).toEqual(["Renamed session"]);
    expect((await b.fetchAgentHistory({ search: "Renamed session" })).entries).toEqual([]);
    expect(history.entries).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ item: expect.objectContaining({ type: "user_message" }) }),
      ]),
    );
    return reopened;
  }
  const reopened = await checkSessionParity();
  for (const label of ["a", "b"])
    expect(await readFile(path.join(root, label, "prompts"), "utf8")).toBe("prompt\n");
  await a.cancelAgent(agentId);
  const fetchAgent = { type: "fetch_agent_request", requestId: "fetch", agentId } as const;
  async function checkPermissions() {
    const readOnly = openClient(central.port, "a.paseo.test", "password-a", ["workspace.read"]);
    await readOnly.connect();
    expect((await readOnly.fetchAgent(agentId))?.agent.id).toBe(agentId);
    await expect(readOnly.clearAgentAttention(agentId)).rejects.toThrow("Permission denied");
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
  async function checkFileEdits() {
    const cwd = path.join(root, "a");
    const name = "edit-me.txt";
    const create = (client: DaemonClient, workspace: string) =>
      client.createFileEntry({ cwd: workspace, parentPath: ".", name, kind: "file" });
    await expect(create(readOnly, cwd)).rejects.toThrow("Permission denied");
    await expect(create(b, cwd)).rejects.toThrow("worker_workspace_root_denied");
    expect(await create(a, cwd)).toMatchObject({ success: true, path: name });
    const { modifiedAt, revision } = await a.readFile(cwd, name);
    await expect(
      a.writeFile({
        cwd,
        path: name,
        content: "edited",
        expectedModifiedAt: modifiedAt,
        expectedRevision: revision,
      }),
    ).resolves.toMatchObject({ status: "written" });
    const renamed = await a.renameFileEntry({ cwd, path: name, name: "renamed.txt" });
    expect(renamed).toMatchObject({ success: true, renamedPath: "renamed.txt" });
    expect(await readFile(path.join(cwd, "renamed.txt"), "utf8")).toBe("edited");
    const observed = a.observeFile({ cwd, path: "renamed.txt" });
    cleanups.push(() => observed.release());
    const updates: number[] = [];
    observed.subscribe({
      snapshot() {},
      update(message) {
        if (message.type === "fs.file.update" && message.payload.version.status === "ready")
          updates.push(message.payload.version.size);
      },
    });
    expect((await observed.ready).initial).toMatchObject({ status: "ready", size: 6 });
    await writeFile(path.join(cwd, "renamed.txt"), "edited externally");
    await expect.poll(() => updates).toContain(17);
    await observed.release();
  }
  await checkFileEdits();
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
  const sender = await b.createAgent({
    provider: "omp",
    cwd: path.join(root, "b"),
    workspaceId: "same-workspace",
  });
  const holdQueue = path.join(root, "b", "hold-turn");
  await writeFile(holdQueue, "");
  await b.sendAgentMessage(agentId, "running before queue");
  const beforeQueue = await readFile(path.join(root, "b", "prompts"), "utf8");
  const queued = {
    sourceAgentId: sender.id,
    sourceAgentServerId: "srv_b",
    activeTurnBehavior: "queue",
    messageId: "queued-message",
  } as const;
  const receiptB = await b.sendAgentMessage(agentId, "queued", queued);
  expect(receiptB).toMatchObject({ deliveryStatus: "queued", activeTurnBehavior: "queue" });
  await expect(a.sendAgentMessage(agentId, "foreign", queued)).rejects.toThrow(
    "worker_message_source_unsupported",
  );
  expect(await readFile(path.join(root, "b", "prompts"), "utf8")).toBe(beforeQueue);
  await rm(holdQueue);
  await expect.poll(() => readFile(path.join(root, "b", "prompts"), "utf8"), {
    timeout: 10_000,
  }).toBe(beforeQueue + "prompt\n");
  await b.sendAgentMessage(agentId, "queued", queued);
  expect(await readFile(path.join(root, "b", "prompts"), "utf8")).toBe(beforeQueue + "prompt\n");
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
  await expect(
    a.invokePluginRpc("fixture-plugin", "workspace", { id: "same-workspace" }),
  ).resolves.toBe(path.join(root, "a"));
  await expect(
    b.invokePluginRpc("fixture-plugin", "workspace", { id: "same-workspace" }),
  ).resolves.toBe(path.join(root, "b"));
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
      const expected = label === "b" ? beforeQueue + "prompt\nprompt\n" : "prompt\nprompt\nprompt\n";
      expect(await readFile(path.join(root, label, "prompts"), "utf8")).toBe(expected);
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
  async function checkProjectRegistration() {
    const folder = path.join(root, "a", "new-project");
    await mkdir(folder);
    const git = (...args: string[]) =>
      execFileSync(
        "git",
        [
          "-C", folder,
          "-c", "user.name=Fixture",
          "-c", "user.email=fixture@example.test",
          "-c", "commit.gpgsign=false",
          ...args,
        ],
        { stdio: "pipe" },
      );
    const changed = path.join(folder, "review.txt");
    git("init", "-b", "main");
    await writeFile(changed, "before\n");
    git("add", "review.txt");
    git("commit", "-m", "Initial");
    await writeFile(changed, "after\n");
    await expect(a.getCheckoutStatus(folder)).resolves.toMatchObject({
      isGit: true,
      currentBranch: "main",
      isDirty: true,
    });
    await expect(b.getCheckoutStatus(folder)).rejects.toThrow("worker_workspace_root_denied");
    const diff = await a.getCheckoutDiff(folder, { mode: "uncommitted" });
    expect(diff.files).toMatchObject([
      {
        path: "review.txt",
        hunks: [
          {
            lines: expect.arrayContaining([
              expect.objectContaining({ type: "remove", content: "before" }),
              expect.objectContaining({ type: "add", content: "after" }),
            ]),
          },
        ],
      },
    ]);
    const liveDiff = a.observeDiff({
      cwd: folder,
      base: { kind: "rev", rev: "HEAD" },
      head: { kind: "worktree" },
    });
    cleanups.push(() => liveDiff.release());
    const addedLines = new Set<string>();
    liveDiff.subscribe({
      snapshot(state) {
        if (state.phase !== "ready") return;
        for (const file of state.files) {
          for (const hunk of file.hunks ?? []) {
            for (const line of hunk.lines) if (line.type === "add") addedLines.add(line.content);
          }
        }
      },
      update() {},
    });
    await liveDiff.ready;
    await expect.poll(() => addedLines.has("after")).toBe(true);
    await writeFile(changed, "external change\n");
    await expect.poll(() => addedLines.has("external change"), { timeout: 15_000 }).toBe(true);
    await liveDiff.release();
    const legacyDiff = a.observeCheckoutDiff(folder, { mode: "uncommitted" });
    cleanups.push(() => legacyDiff.release());
    expect((await legacyDiff.ready).files.map(({ path }) => path)).toContain("review.txt");
    await legacyDiff.release();
    expect(await a.checkoutCommit(folder, { message: "Commit from stock SDK" })).toMatchObject({
      success: true,
      error: null,
    });
    await expect(a.getCheckoutStatus(folder)).resolves.toMatchObject({ isDirty: false });
    await expect(b.checkoutCommit(folder, { message: "Wrong worker" })).rejects.toThrow(
      "worker_workspace_root_denied",
    );
    const metadataGeneration = { instructions: "Use these updated resident instructions." };
    expect((await a.patchDaemonConfig({ metadataGeneration })).config.metadataGeneration).toEqual({
      ...metadataGeneration,
      providers: [{ provider: "codex", model: "fixture" }],
    });
    await writeFile(changed, "generated metadata test\n");
    expect(await a.checkoutCommit(folder, {})).toMatchObject({ success: true, error: null });
    expect(git("log", "-1", "--format=%s").toString().trim()).toBe("Document generated change");
    const metadataPrompt = await readFile(path.join(root, "a", "metadata-prompt"), "utf8");
    expect(metadataPrompt).toContain(metadataGeneration.instructions);
    expect(metadataPrompt).toContain("+generated metadata test");
    const other = openClient(central.port, "a.paseo.test", "password-a");
    await other.connect();
    const observation = other.observeWorkspaces();
    cleanups.push(() => observation.release());
    const visible = new Set<string>();
    observation.subscribe({
      snapshot() {},
      update(message) {
        if (message.type === "workspace_update" && message.payload.kind === "upsert")
          visible.add(message.payload.workspace.id);
      },
    });
    await observation.ready;
    expect((await a.addProject(folder)).project).toMatchObject({ projectRootPath: folder });
    expect((await b.addProject(folder)).error).toBe("worker_workspace_root_denied");
    const project = (await a.addProject(folder)).project;
    expect((await a.listProjects()).projects.filter((item) => item.projectRootPath === folder)).toEqual(
      [project],
    );
    const result = await a.createWorkspace({
      source: { kind: "directory", path: folder, projectId: project!.projectId },
    });
    expect(result.workspace?.workspaceDirectory).toBe(folder);
    await expect.poll(() => visible.has(result.workspace!.id)).toBe(true);
    const workspaceId = result.workspace!.id;
    await expect(a.setWorkspaceTitle(workspaceId, " Project review ")).resolves.toEqual({
      title: "Project review",
    });
    expect((await a.setWorkspacePinned(workspaceId, true)).pinnedAt).toBeTruthy();
    await expect(b.setWorkspaceTitle(workspaceId, "Other worker")).rejects.toThrow(
      "Workspace not found",
    );
    const script = "remote-fixture";
    await writeFile(
      path.join(folder, "paseo.json"),
      JSON.stringify({ scripts: { [script]: { command: `${JSON.stringify(process.execPath)} -e "setInterval(() => {}, 1000)"` } } }),
    );
    const status = other.observeEvents(["script_status_update"]);
    cleanups.push(() => status.release());
    const lifecycles = new Set<string>();
    status.subscribe({
      snapshot() {},
      update(event) {
        if (event.type !== "script_status_update" || event.payload.workspaceId !== workspaceId) return;
        for (const item of event.payload.scripts) {
          if (item.scriptName === script) lifecycles.add(item.lifecycle);
        }
      },
    });
    await status.ready;
    expect((await b.listWorkspaceScripts(workspaceId)).error).toMatch(/Workspace not found/);
    expect((await a.listWorkspaceScripts(workspaceId)).scripts).toMatchObject([
      { scriptName: script, lifecycle: "stopped" },
    ]);
    expect(await a.startWorkspaceScriptWithStatus(workspaceId, script)).toMatchObject({
      error: null,
      script: { lifecycle: "running", terminalId: expect.any(String) },
    });
    await expect.poll(() => lifecycles.has("running")).toBe(true);
    expect(await a.stopWorkspaceScript(workspaceId, script)).toMatchObject({
      error: null,
      script: { lifecycle: "stopped" },
    });
    await expect.poll(() => lifecycles.has("stopped")).toBe(true);
    await status.release();
    const worktreeIntent = {
      idempotencyKey: "worktree-fixture",
      source: {
        kind: "worktree",
        projectId: project!.projectId,
        action: "branch-off",
        branchName: "remote-worktree-fixture",
        refName: "main",
      },
    } as const;
    const worktree = await a.createWorkspace(worktreeIntent);
    expect(worktree).toMatchObject({
      workspace: { workspaceKind: "worktree", projectId: project!.projectId },
      error: null,
    });
    expect((await a.createWorkspace(worktreeIntent)).workspace?.id).toBe(worktree.workspace!.id);
    await expect(a.getCheckoutStatus(worktree.workspace!.workspaceDirectory)).resolves.toMatchObject({
      currentBranch: "remote-worktree-fixture",
    });
    await expect(b.openProject(worktree.workspace!.workspaceDirectory)).resolves.toMatchObject({
      error: "worker_workspace_root_denied",
    });
    const worktreeId = worktree.workspace!.id;
    const worktreeCwd = worktree.workspace!.workspaceDirectory;
    await expect(b.archiveWorkspace(worktreeId)).resolves.toMatchObject({ archivedAt: null, error: "Workspace not found" });
    expect(await a.archiveWorkspace(worktreeId)).toMatchObject({ archivedAt: expect.any(String), error: null });
    await expect(readdir(worktreeCwd)).rejects.toMatchObject({ code: "ENOENT" });
    expect(await a.inspectWorkspaceRecovery(worktreeId)).toMatchObject({ kind: "recoverable", action: "restore" });
    await a.restoreWorkspace(worktreeId);
    expect((await a.fetchWorkspaces()).entries).toEqual(expect.arrayContaining([
      expect.objectContaining({ id: worktreeId, workspaceDirectory: worktreeCwd }),
    ]));
    expect((await a.getCheckoutStatus(worktreeCwd)).currentBranch).toBe("remote-worktree-fixture");
    await expect(a.getDaemonStatus({ includeProviders: true })).resolves.toMatchObject({
      serverId: "srv_a",
      pid: process.pid,
    });
    const profile = { id: "fixture-profile", name: "Fixture OMP", provider: "omp" };
    await a.patchDaemonConfig({ agentProfiles: [profile] });
    expect((await other.getDaemonConfig()).config.agentProfiles).toMatchObject([profile]);
    expect((await b.getDaemonConfig()).config.agentProfiles).toEqual([]);
    await expect(a.patchDaemonConfig({ autoArchiveAfterMerge: true })).rejects.toThrow(
      "worker_host_settings_patch_unsupported",
    );
    expect((await a.getDaemonConfig()).config.agentProfiles).toMatchObject([profile]);
    expect((await a.reloadDaemonConfig()).restartRequiredPaths).not.toContain("daemon.agentProfiles");
    const projectId = project!.projectId;
    await expect(b.renameProject(projectId, "Foreign project")).rejects.toThrow("Project not found");
    await expect(a.renameProject(projectId, "Reviewed project")).resolves.toEqual({ customName: "Reviewed project" });
    expect((await other.listProjects()).projects).toEqual(expect.arrayContaining([
      expect.objectContaining({ projectId, projectDisplayName: "Reviewed project" }),
    ]));
    const projectConfig = await a.readProjectConfig(folder);
    if (!projectConfig.ok) throw new Error(projectConfig.error.code);
    const configUpdate = {
      repoRoot: folder,
      expectedRevision: projectConfig.revision,
      config: { worktree: { setup: "echo project-config" } },
    };
    expect(await a.writeProjectConfig(configUpdate)).toMatchObject({ ok: true, config: configUpdate.config });
    expect(await a.writeProjectConfig(configUpdate)).toMatchObject({ ok: false, error: { code: "stale_project_config" } });
    const png = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAIAAAA=";
    await a.setProjectIcon(projectId, { type: "upload", data: png });
    expect((await other.getProjectIcon(projectId)).icon).toEqual({ data: png, mimeType: "image/png" });
    expect((await b.getProjectIcon(projectId)).icon).toBeNull();
    const removed = await a.removeProject(projectId);
    expect(removed.removedWorkspaceIds).toEqual(expect.arrayContaining([workspaceId, worktreeId]));
    expect(await readdir(worktreeCwd)).toContain("review.txt");
    expect(await readdir(folder)).toContain("review.txt");
    expect((await other.listProjects()).projects.some((item) => item.projectId === projectId)).toBe(false);
    expect((await b.listProjects()).projects.map((item) => item.projectDisplayName)).toEqual(["b"]);
    await observation.release();
    await other.close();
  }
  await checkProjectRegistration();
  const attachmentBytes = Buffer.from("retain this completed upload");
  const { file: retainedUpload } = await a.uploadFile({
    fileName: "retained.txt",
    mimeType: "text/plain",
    bytes: attachmentBytes,
  });
  if (!retainedUpload) throw new Error("Upload fixture was not stored");
  const attachments = [{ ...retainedUpload, type: "uploaded_file" as const }];
  const aHome = path.join(root, "a");
  async function assertAttachmentDispatch(client: DaemonClient, messageId: string) {
    await client.sendAgentMessage(agentId, "read retained upload", { messageId, attachments });
    const input = JSON.parse(await readFile(path.join(aHome, "last-prompt"), "utf8"));
    expect(JSON.stringify(input)).toContain(JSON.stringify(retainedUpload!.path).slice(1, -1));
    expect(await readFile(retainedUpload!.path)).toEqual(attachmentBytes);
  }
  await assertAttachmentDispatch(a, "before-pod-replacement");
  await a.close();
  await reopened.close();
  await readOnly.close();
  await residents.get("a")!.close();
  async function startReplacement(podUid: string, supportsWorkspaceOperations: boolean) {
    const binding = { workerId: "a", podUid, serverId: "srv_a" };
    const native = createNativeExecution({
      paseoHome: aHome,
      workspaceRoots: [aHome],
      logger,
      providers: {
        codex: {
          command: { mode: "replace", argv: [process.execPath, script, "codex"] },
          env: { FIXTURE_STATE: aHome },
        },
      },
    });
    const resident = await startResidentExecution({
      binding,
      paseoHome: aHome,
      listen: { host: "127.0.0.1", port: 0 },
      tls,
      authorizeCentral: (fingerprint) => fingerprint === tls.fingerprint,
      async openRuntime(...args) {
        const runtime = await native.openRuntime(...args);
        return supportsWorkspaceOperations ? runtime : { execute: runtime.execute, close: runtime.close };
      },
      shutdownRuntime: native.close,
      onError: (error) => logger.error({ error }),
    });
    cleanups.push(() => resident.close());
    return {
      resident,
      grant: {
        ...grant,
        target: { ...grant.target, ...binding, endpoint: `wss://127.0.0.1:${resident.port}/execute` },
        expiresAt: Date.now() + 30_000,
      },
    };
  }
  const { grant: replacement, resident: oldResident } = await startReplacement("pod-a-old-runtime", false);
  expect((await central.maintenance(replacement, fetchAgent, maintenanceSignal)).agent?.id).toBe(
    agentId,
  );
  await expect(
    central.maintenance({ ...grant, expiresAt: Date.now() + 30_000 }, fetchAgent, maintenanceSignal),
  ).rejects.toThrow("worker_maintenance_stale_target");
  expect((await central.maintenance(replacement, fetchAgent, maintenanceSignal)).agent?.id).toBe(
    agentId,
  );
  targets.get("a.paseo.test")!.target = replacement.target;
  const oldRuntime = openClient(central.port, "a.paseo.test", "password-a");
  await oldRuntime.connect();
  expect(oldRuntime.getLastServerInfoMessage()?.features?.terminalsSurviveRestart).toBeUndefined();
  expect(oldRuntime.getLastServerInfoMessage()?.features?.diffSubscription).toBeUndefined();
  expect(oldRuntime.getLastServerInfoMessage()?.features?.pluginRegistry).toBeUndefined();
  expect(oldRuntime.getLastServerInfoMessage()?.features).toMatchObject({
    projectList: true,
    agentForkContextCursor: true,
  });
  expect(oldRuntime.getLastServerInfoMessage()?.features?.workspaceFileEditing).toBeUndefined();
  expect(oldRuntime.getLastServerInfoMessage()?.features?.fileByteWrites).toBeUndefined();
  expect(oldRuntime.getLastServerInfoMessage()?.features?.daemonDiagnostics).toBeUndefined();
  await expect(oldRuntime.restartServer()).rejects.toMatchObject({
    code: "worker_restart_unsupported",
  });
  expect((await oldRuntime.fetchAgent(agentId))?.agent.id).toBe(agentId);
  await oldRuntime.close();
  await oldResident.close();
  const { grant: current } = await startReplacement("pod-a-replacement", true);
  targets.get("a.paseo.test")!.target = current.target;
  const replaced = openClient(central.port, "a.paseo.test", "password-a");
  await replaced.connect();
  await assertAttachmentDispatch(replaced, "after-pod-replacement");
  expect(await rpc(b)).toEqual(markerB);
  failRestart.add("b");
  await expect(b.restartServer()).rejects.toMatchObject({ code: "worker_restart_unconfirmed" });
  await expect.poll(() => restartFailures).toEqual(["b"]);
  const fencedResident = residents.get("b")!;
  expect(await probeTcp(fencedResident.port)).toBe(false);
  expect(await getPidLockInfo(path.join(root, "b"))).toMatchObject({
    pid: process.pid,
    listen: null,
  });
  expect((await replaced.fetchAgent(agentId))?.agent.id).toBe(agentId);
});
