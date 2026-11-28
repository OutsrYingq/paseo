import { ChildProcess, type ChildProcessWithoutNullStreams } from "node:child_process";
import { randomUUID } from "node:crypto";
import { PassThrough, Writable } from "node:stream";
import { CodexAppServerAgentClient } from "../agent/providers/codex-app-server-agent.js";
import { RemoteOmpAgentClient } from "./remote-omp-agent-client.js";
import { OmpCliRuntime } from "../agent/providers/omp/cli-runtime.js";
import type { AgentClient } from "../agent/agent-sdk-types.js";
import type { Logger } from "pino";
import type { WorkerConnection } from "./worker-connection.js";
import type { NativeLaunch, WorkerEvent } from "./execution-protocol.js";

export class RemoteProviderProcesses {
  private readonly processes = new Map<
    string,
    { event: (event: Extract<WorkerEvent, { processId: string }>) => void; fail: () => void }
  >();
  constructor(private readonly connection: WorkerConnection) {
    connection.signal.addEventListener(
      "abort",
      () => {
        for (const process of this.processes.values()) process.fail();
      },
      { once: true },
    );
  }

  onEvent(event: Extract<WorkerEvent, { processId: string }>): void {
    this.processes.get(event.processId)?.event(event);
  }

  private async *readOmpHistory(sessionFile: string, activeBranchEntryId?: string) {
    const id = await this.connection.call({
      kind: "ompHistoryOpen",
      sessionFile,
      activeBranchEntryId,
    });
    try {
      for (;;) {
        const page = await this.connection.call({ kind: "ompHistoryNext", id });
        yield* page.events;
        if (page.done) return;
      }
    } finally {
      await this.connection.call({ kind: "ompHistoryClose", id }).catch(() => undefined);
    }
  }

  async createClients(logger: Logger): Promise<Record<string, AgentClient>> {
    const providers = await this.connection.call({ kind: "providers" });
    const clients: Record<string, AgentClient> = {};
    if (providers.includes("codex")) {
      clients.codex = new CodexAppServerAgentClient(logger, undefined, {
        appServerProcess: {
          resolvePrefix: async () => ({ command: "codex", args: [] }),
          resolveOptionalFeatures: async () => ({
            goals: false,
            autoReview: false,
          }),
          isAvailable: async () => true,
          spawn: ({ args, agentId }) =>
            this.spawn({
              provider: "codex",
              agentId,
              goals: args.includes("goals"),
            }),
        },
      });
    }
    if (providers.includes("omp")) {
      const runtime = new OmpCliRuntime({
        logger,
        spawnProcess: ({
          cwd,
          agentId,
          protocolMode,
          model,
          thinkingOptionId,
          session,
          noSession,
          systemPrompt,
        }) =>
          this.spawn({
            provider: "omp",
            cwd,
            agentId,
            protocolMode,
            model,
            thinkingOptionId,
            session,
            noSession,
            systemPrompt,
          }),
      });
      clients.omp = new RemoteOmpAgentClient({
        logger,
        runtime,
        executionIsRemote: true,
        resolveCatalogHome: () => this.connection.call({ kind: "providerHome" }),
        readHistory: (file, branch) => this.readOmpHistory(file, branch),
      }, {
        ompImportList: (operation) => this.connection.call(operation),
        ompImportResolve: (operation) => this.connection.call(operation),
      });
    }
    return clients;
  }

  private spawn(launch: NativeLaunch): ChildProcessWithoutNullStreams {
    const processId = randomUUID();
    const child = new ChildProcess() as ChildProcessWithoutNullStreams;
    const output = new PassThrough();
    const errorOutput = new PassThrough();
    let ended = false;
    const started = this.connection.call({ kind: "spawn", processId, launch });
    child.stdout = output;
    child.stderr = errorOutput;
    child.stdin = new Writable({
      write: (data: Buffer, _encoding, done) => {
        void started
          .then(() =>
            this.connection.call({
              kind: "stdin",
              processId,
              bytes: data.toString("base64"),
            }),
          )
          .then(() => done(), done);
      },
      final: (done) => {
        void started
          .then(() => this.connection.call({ kind: "endStdin", processId }))
          .then(() => done(), done);
      },
    });
    child.stdin.on("error", () => {});
    child.kill = (signal = "SIGTERM") => {
      if (ended || (signal !== "SIGTERM" && signal !== "SIGKILL" && signal !== "SIGINT"))
        return false;
      void started
        .then(() => this.connection.call({ kind: "signal", processId, signal }))
        .catch(() => undefined);
      return true;
    };
    const finish = (code: number | null, signal: NodeJS.Signals | null) => {
      if (ended) return;
      ended = true;
      this.processes.delete(processId);
      Object.assign(child, { exitCode: code, signalCode: signal });
      child.emit("exit", code, signal);
      output.end();
      errorOutput.end();
      child.emit("close", code, signal);
    };
    this.processes.set(processId, {
      event(event) {
        if (event.type === "output")
          (event.stream === "stdout" ? output : errorOutput).write(
            Buffer.from(event.bytes, "base64"),
          );
        else finish(event.code, event.signal as NodeJS.Signals | null);
      },
      fail: () => finish(1, null),
    });
    void started.then(
      () => {
        if (!ended) child.emit("spawn");
      },
      () => finish(1, null),
    );
    return child;
  }
}
