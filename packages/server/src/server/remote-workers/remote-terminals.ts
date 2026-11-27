import { randomUUID } from "node:crypto";
import { deriveTerminalActivityStatusBucket } from "@getpaseo/protocol/terminal-activity";
import { isSameOrDescendantPath } from "../path-utils.js";
import type { TerminalManager } from "../../terminal/terminal-manager.js";
import type {
  ServerMessage,
  TerminalActivityTransition,
  TerminalCommandFinishedInfo,
  TerminalExitInfo,
  TerminalSession,
} from "../../terminal/terminal.js";
import type {
  RemoteTerminalEvent,
  RemoteTerminalInfo,
  RemoteTerminalInventory,
  RemoteTerminalOperation,
  RemoteTerminalResult,
} from "./remote-terminals-protocol.js";

export * from "./remote-terminals-protocol.js";
export {
  createResidentTerminals,
  type ResidentTerminalOptions,
} from "./remote-terminals-resident.js";

/** Supplied by the single execution connection for this worker binding. */
export interface RemoteTerminalConnection {
  readonly signal: AbortSignal;
  call<K extends RemoteTerminalOperation["kind"]>(
    operation: RemoteTerminalOperation & { kind: K },
  ): Promise<RemoteTerminalResult<K>>;
}

export interface RemoteTerminalManager extends TerminalManager {
  /** Load the inventory before exposing the manager to a public session. */
  initialize(): Promise<void>;
  handleEvent(event: RemoteTerminalEvent): void;
  /** Retire this projection without closing its execution connection or stopping any PTYs. */
  dispose(): void;
}

function listeners<T>(onError: (error: unknown) => void) {
  const callbacks = new Set<(value: T) => void>();
  return {
    get size() {
      return callbacks.size;
    },
    add(callback: (value: T) => void) {
      callbacks.add(callback);
      return () => {
        callbacks.delete(callback);
      };
    },
    emit(value: T) {
      for (const callback of callbacks) {
        try {
          callback(value);
        } catch (error) {
          onError(error);
        }
      }
    },
    clear() {
      callbacks.clear();
    },
  };
}

type TerminalListeners = {
  [K in
    | "subscribeTerminalsChanged"
    | "subscribeTerminalActivity"
    | "subscribeTerminalWorkspaceContributionChanged"]: Parameters<
    Parameters<TerminalManager[K]>[0]
  >[0];
};

/** Share one manager per worker binding; give each public client its own stream controller. */
export function createRemoteTerminalManager(
  connection: RemoteTerminalConnection,
  onError: (error: unknown) => void,
): RemoteTerminalManager {
  const sessions = new Map<string, ReturnType<typeof createSession>>();
  const streams = new Map<
    string,
    { terminalId: string; receive(event: StreamEvent): void; disconnect(): void }
  >();
  const replays = new Set<string>();
  const changed = listeners<TerminalListeners["subscribeTerminalsChanged"]>(onError);
  const activityChanged = listeners<TerminalListeners["subscribeTerminalActivity"]>(onError);
  const workspaceChanged =
    listeners<TerminalListeners["subscribeTerminalWorkspaceContributionChanged"]>(onError);
  let inventoryRevision = -1;
  let closed = false;
  let initialization: Promise<void> | undefined;
  const unavailable = new Error("terminal_channel_closed");

  type StreamEvent = Exclude<RemoteTerminalEvent, { type: "terminalChanged" }>;

  function call<K extends RemoteTerminalOperation["kind"]>(
    operation: Extract<RemoteTerminalOperation, { kind: K }> & { kind: K },
  ): Promise<RemoteTerminalResult<K>> {
    if (closed) return Promise.reject(unavailable);
    return connection.call<K>(operation);
  }

  function fire(task: Promise<unknown>): void {
    void task.catch((error: unknown) => {
      if (!closed) onError(error);
    });
  }

  function createSession(initial: RemoteTerminalInfo) {
    let info = initial;
    const { id, cwd, name, workspaceId, createdAt } = info;
    const output = listeners<ServerMessage>(onError);
    const exit = listeners<TerminalExitInfo>(onError);
    const command = listeners<TerminalCommandFinishedInfo>(onError);
    const titleChanged = listeners<string | undefined>(onError);
    const activity = listeners<TerminalActivityTransition>(onError);
    let subscriptionId: string | undefined;
    let exited: TerminalExitInfo | null = null;
    let ready: Extract<ServerMessage, { type: "snapshotReady" }> | undefined;
    let hasOutput = info.hasOutput;
    let replayPreamble = "";

    function releaseStream(): void {
      if (!subscriptionId) return;
      const idToRelease = subscriptionId;
      subscriptionId = undefined;
      ready = undefined;
      streams.delete(idToRelease);
      if (!closed)
        fire(call({ kind: "terminalDetach", terminalId: id, subscriptionId: idToRelease }));
    }

    function finish(exitInfo: TerminalExitInfo): void {
      if (exited) return;
      exited = exitInfo;
      releaseStream();
      exit.emit(exitInfo);
      exit.clear();
    }

    function startStream(): void {
      if (subscriptionId || exited || closed) return;
      const streamId = randomUUID();
      subscriptionId = streamId;
      streams.set(streamId, { terminalId: id, receive, disconnect });
      function receive(event: StreamEvent): void {
        if (event.terminalId !== id) return;
        switch (event.type) {
          case "terminalReady":
            replayPreamble = event.replayPreamble;
            ready = {
              type: "snapshotReady",
              revision: event.revision,
              replayPreamble: event.replayPreamble,
            };
            output.emit(ready);
            break;
          case "terminalOutput":
            hasOutput = true;
            replayPreamble = event.replayPreamble;
            output.emit({
              type: "output",
              data: Buffer.from(event.bytes, "base64").toString("utf8"),
              revision: event.revision,
            });
            break;
          case "terminalExit":
            finish(event.info);
            break;
          case "terminalCommandFinished":
            command.emit(event.info);
            break;
        }
      }
      fire(
        call({ kind: "terminalAttach", terminalId: id, subscriptionId: streamId }).then(
          (attached) => {
            if (!attached) return failStream();
            return undefined;
          },
          (error: unknown) => {
            failStream();
            throw error;
          },
        ),
      );
      function failStream(): void {
        if (subscriptionId !== streamId) return;
        disconnect();
        if (sessions.get(id)?.session === session) sessions.set(id, createSession(info));
      }
    }

    function subscribeToStream<T>(
      collection: ReturnType<typeof listeners<T>>,
      callback: (value: T) => void,
    ) {
      const remove = collection.add(callback);
      startStream();
      return () => {
        remove();
        if (output.size + exit.size + command.size === 0) releaseStream();
      };
    }

    function disconnect() {
      finish({ exitCode: null, signal: null, lastOutputLines: [] });
    }

    const session: TerminalSession = {
      id,
      cwd,
      name,
      workspaceId,
      createdAt,
      send(message) {
        if (message.type === "input") {
          fire(
            call({
              kind: "terminalInput",
              terminalId: id,
              bytes: Buffer.from(message.data).toString("base64"),
            }),
          );
        } else if (message.type === "resize") {
          const previous = info;
          const resized = { ...info, rows: message.rows, cols: message.cols };
          info = resized;
          fire(
            call({
              kind: "terminalResize",
              terminalId: id,
              rows: message.rows,
              cols: message.cols,
            }).catch((error: unknown) => {
              if (info === resized) info = previous;
              throw error;
            }),
          );
        }
      },
      subscribe(callback) {
        let active = true;
        const currentReady = ready;
        const unsubscribe = subscribeToStream(output, callback);
        if (currentReady)
          queueMicrotask(() => {
            if (active && ready === currentReady) callback(currentReady);
          });
        return () => {
          active = false;
          unsubscribe();
        };
      },
      onExit(callback) {
        if (exited) {
          const exitInfo = exited;
          let active = true;
          queueMicrotask(() => {
            if (active) callback(exitInfo);
          });
          return () => {
            active = false;
          };
        }
        return subscribeToStream(exit, callback);
      },
      onCommandFinished: (callback) => subscribeToStream(command, callback),
      onTitleChange: titleChanged.add,
      onActivityChange: activity.add,
      getSize: () => ({ rows: info.rows, cols: info.cols }),
      hasOutput: () => hasOutput,
      getReplayPreamble: () => replayPreamble,
      getTitle: () => info.title,
      getActivity: () => info.activity,
      setActivity: (state) => fire(call({ kind: "terminalSetActivity", terminalId: id, state })),
      clearActivityAttention() {
        if (info.activity?.state !== "attention") return false;
        fire(call({ kind: "terminalClearAttention", terminalId: id }));
        return true;
      },
      setTitle: (title) => fire(call({ kind: "terminalRename", terminalId: id, title })),
      getExitInfo: () => exited,
      kill: () => fire(call({ kind: "terminalClose", terminalId: id })),
      async killAndWait(options) {
        await call({ kind: "terminalClose", terminalId: id, ...options });
      },
    };

    return {
      session,
      disconnect,
      update(next: RemoteTerminalInfo) {
        const previous = info;
        info = next;
        hasOutput ||= next.hasOutput;
        if (previous.title !== next.title) titleChanged.emit(next.title);
        if (
          previous.activity?.state !== next.activity?.state ||
          previous.activity?.changedAt !== next.activity?.changedAt ||
          previous.activity?.attentionReason !== next.activity?.attentionReason
        ) {
          const transition = { activity: next.activity, previous: previous.activity };
          activity.emit(transition);
          activityChanged.emit({ terminalId: id, cwd, name, workspaceId, ...transition });
          if (
            deriveTerminalActivityStatusBucket(previous.activity) !==
            deriveTerminalActivityStatusBucket(next.activity)
          ) {
            workspaceChanged.emit({ terminalId: id, cwd, workspaceId });
          }
        }
      },
    };
  }

  function acceptInventory(inventory: RemoteTerminalInventory, cwd?: string): void {
    if (closed || inventoryRevision >= inventory.revision) return;
    inventoryRevision = inventory.revision;
    const live = new Set(inventory.terminals.map(({ id }) => id));
    for (const id of sessions.keys()) if (!live.has(id)) sessions.delete(id);
    for (const info of inventory.terminals) {
      const existing = sessions.get(info.id);
      if (existing) existing.update(info);
      else sessions.set(info.id, createSession(info));
    }
    if (cwd !== undefined)
      changed.emit({
        cwd,
        terminals: inventory.terminals.filter((terminal) => terminal.cwd === cwd),
      });
  }

  function dispose(): void {
    if (closed) return;
    closed = true;
    // The directory entry may already be gone when its attached viewer needs an exit.
    for (const [subscriptionId, stream] of streams) {
      stream.disconnect();
      if (!connection.signal.aborted) {
        fire(
          connection.call({
            kind: "terminalDetach",
            terminalId: stream.terminalId,
            subscriptionId,
          }),
        );
      }
    }
    if (!connection.signal.aborted) {
      for (const replayId of replays) {
        fire(connection.call({ kind: "terminalReplayCancel", replayId }));
      }
    }
    replays.clear();
    connection.signal.removeEventListener("abort", dispose);
    sessions.clear();
    changed.clear();
    activityChanged.clear();
    workspaceChanged.clear();
  }

  if (connection.signal.aborted) dispose();
  else connection.signal.addEventListener("abort", dispose, { once: true });

  return {
    initialize() {
      return (initialization ??= call({ kind: "terminalList" })
        .then((inventory) => acceptInventory(inventory))
        .catch((error: unknown) => {
          initialization = undefined;
          throw error;
        }));
    },
    handleEvent(event) {
      if (closed) return;
      if (event.type === "terminalChanged") acceptInventory(event.inventory, event.cwd);
      else streams.get(event.subscriptionId)?.receive(event);
    },
    dispose,
    getUnavailableError: () => (closed ? unavailable : null),
    async getTerminals(cwd, options) {
      return [...sessions.values()]
        .map(({ session }) => session)
        .filter(
          (session) =>
            (cwd === session.cwd || isSameOrDescendantPath(cwd, session.cwd)) &&
            (options?.workspaceId === undefined || options.workspaceId === session.workspaceId),
        );
    },
    async createTerminal({
      id = randomUUID(),
      cwd,
      workspaceId,
      name,
      title,
      command,
      args,
      rows,
      cols,
    }) {
      const info = await call({
        kind: "terminalCreate",
        terminalId: id,
        cwd,
        workspaceId,
        name,
        title,
        command,
        args,
        rows,
        cols,
      });
      return (sessions.get(info.id) ?? createSession(info)).session;
    },
    registerCwdEnv() {
      throw new Error("Configure terminal cwd environment on the resident");
    },
    validateTerminalActivityToken: () => "unknown",
    getTerminal: (id) => sessions.get(id)?.session,
    async replayTerminal(terminalId, options, sink) {
      const replayId = randomUUID();
      replays.add(replayId);
      let complete = false;
      try {
        const metadata = await call({
          kind: "terminalReplayOpen",
          terminalId,
          replayId,
          scrollbackLines: options?.scrollbackLines,
        });
        if (!metadata) {
          complete = true;
          return null;
        }
        sink({ type: "begin", metadata });
        while (true) {
          const bytes = await call({ kind: "terminalReplayNext", replayId });
          if (bytes === null) break;
          sink({ type: "data", data: Buffer.from(bytes, "base64") });
        }
        complete = true;
        sink({ type: "end" });
        return metadata;
      } finally {
        replays.delete(replayId);
        if (!complete && !closed)
          await call({ kind: "terminalReplayCancel", replayId }).catch(onError);
      }
    },
    setTerminalTitle(id, title) {
      const session = sessions.get(id)?.session;
      if (!session) return false;
      session.setTitle(title);
      return true;
    },
    setTerminalActivity: (terminalId, state) =>
      call({ kind: "terminalSetActivity", terminalId, state }),
    clearTerminalAttention: (terminalId) => call({ kind: "terminalClearAttention", terminalId }),
    killTerminal: (terminalId) => fire(call({ kind: "terminalClose", terminalId })),
    async killTerminalAndWait(terminalId, options) {
      await call({ kind: "terminalClose", terminalId, ...options });
    },
    captureTerminal: (terminalId, options) =>
      call({ kind: "terminalCapture", terminalId, ...options }),
    listDirectories: () => [...new Set([...sessions.values()].map(({ session }) => session.cwd))],
    killAll() {
      for (const { session } of sessions.values()) session.kill();
    },
    subscribeTerminalsChanged: changed.add,
    subscribeTerminalActivity: activityChanged.add,
    subscribeTerminalWorkspaceContributionChanged: workspaceChanged.add,
  };
}
