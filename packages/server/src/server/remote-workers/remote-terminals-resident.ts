import {
  createTerminalManager,
  type TerminalManagerOptions,
} from "../../terminal/terminal-manager.js";
import { TERMINAL_REPLAY_CHUNK_BYTES } from "../../terminal/terminal-replay.js";
import type { LocalTerminalSession } from "../../terminal/terminal.js";
import type {
  RemoteTerminalEvent,
  RemoteTerminalInfo,
  RemoteTerminalOperation,
} from "./remote-terminals-protocol.js";

type Operation<K extends RemoteTerminalOperation["kind"]> = Extract<
  RemoteTerminalOperation,
  { kind: K }
>;

const MAX_TERMINALS = 64;
const MAX_REPLAYS = 16;
const MAX_REPLAY_BYTES = 16 * 1024 * 1024;

export interface ResidentTerminalOptions extends TerminalManagerOptions {
  operationError?: (code: string) => Error;
}

/** Construct once per resident. Opening a channel does not change PTY ownership. */
export function createResidentTerminals(options: ResidentTerminalOptions = {}) {
  const manager = createTerminalManager(options);
  const terminalsByCwd = new Map<string, string[]>();
  const starting = new Map<string, Promise<LocalTerminalSession>>();
  const stopping = new Map<string, Promise<void>>();
  let createTail = Promise.resolve();
  let revision = 0;
  let releaseChannel: (() => void) | undefined;
  let publishChanged: ((cwd: string) => void) | undefined;
  let disposal: Promise<void> | undefined;
  const error = options.operationError ?? ((code: string) => new Error(code));

  function describe(terminal: LocalTerminalSession): RemoteTerminalInfo {
    return {
      id: terminal.id,
      cwd: terminal.cwd,
      workspaceId: terminal.workspaceId,
      name: terminal.name,
      title: terminal.getTitle(),
      createdAt: terminal.createdAt,
      activity: terminal.getActivity(),
      hasOutput: terminal.hasOutput(),
      ...terminal.getSize(),
    };
  }

  function inventory() {
    const live: RemoteTerminalInfo[] = [];
    for (const id of terminalIds()) {
      const terminal = manager.getTerminal(id);
      if (terminal) live.push(describe(terminal));
    }
    return { revision, terminals: live };
  }

  function terminalIds(): string[] {
    return [...terminalsByCwd.values()].flat();
  }

  const unsubscribeRevision = manager.subscribeTerminalsChanged(({ cwd, terminals }) => {
    if (terminals.length)
      terminalsByCwd.set(
        cwd,
        terminals.map(({ id }) => id),
      );
    else terminalsByCwd.delete(cwd);
    revision++;
    publishChanged?.(cwd);
  });

  return {
    registerCwdEnv: manager.registerCwdEnv,
    open(publish: (event: RemoteTerminalEvent) => void, signal: AbortSignal) {
      signal.throwIfAborted();
      if (disposal) throw error("terminal_resident_stopped");
      if (releaseChannel) throw error("terminal_channel_already_open");
      const attachments = new Map<string, { id: string; release(): void }>();
      const replays = new Map<string, { bytes: Uint8Array; offset: number }>();
      let replayBytes = 0;
      publishChanged = (cwd) => publish({ type: "terminalChanged", cwd, inventory: inventory() });
      let closed = false;
      function dispose(): void {
        if (closed) return;
        closed = true;
        signal.removeEventListener("abort", dispose);
        publishChanged = undefined;
        for (const attachment of attachments.values()) attachment.release();
        attachments.clear();
        replays.clear();
        replayBytes = 0;
        releaseChannel = undefined;
      }
      releaseChannel = dispose;
      signal.addEventListener("abort", dispose, { once: true });

      function dropReplay(id: string) {
        const replay = replays.get(id);
        if (replay) replayBytes -= replay.bytes.byteLength;
        replays.delete(id);
      }

      function get(id: string): LocalTerminalSession {
        const terminal = manager.getTerminal(id);
        if (!terminal) throw error("terminal_not_found");
        return terminal;
      }

      async function create(operation: Operation<"terminalCreate">) {
        const { kind: _kind, terminalId, ...createOptions } = operation;
        if (
          starting.has(terminalId) ||
          stopping.has(terminalId) ||
          manager.getTerminal(terminalId)
        ) {
          throw error("terminal_id_in_use");
        }
        if (
          new Set([...terminalIds(), ...starting.keys(), ...stopping.keys()]).size >= MAX_TERMINALS
        )
          throw error("terminal_limit");
        // Concurrent creates must not race the local manager's first list for a cwd.
        const pending = createTail.then(() => {
          if (closed) throw error("terminal_channel_closed");
          return manager.createTerminal({ ...createOptions, id: terminalId });
        });
        createTail = pending.then(
          () => undefined,
          () => undefined,
        );
        starting.set(terminalId, pending);
        try {
          return describe(await pending);
        } finally {
          starting.delete(terminalId);
        }
      }

      function attach(operation: Operation<"terminalAttach">) {
        const { terminalId, subscriptionId } = operation;
        const terminal = manager.getTerminal(terminalId);
        if (!terminal) return false;
        attachments.get(terminalId)?.release();
        const unsubscribeOutput = terminal.subscribe((message) => {
          switch (message.type) {
            case "output":
              publish({
                type: "terminalOutput",
                terminalId,
                subscriptionId,
                bytes: Buffer.from(message.data).toString("base64"),
                revision: message.revision,
                replayPreamble: terminal.getReplayPreamble(),
              });
              break;
            case "snapshotReady":
              publish({
                type: "terminalReady",
                terminalId,
                subscriptionId,
                revision: message.revision ?? 0,
                replayPreamble: message.replayPreamble ?? "",
              });
              break;
          }
        });
        const unsubscribeCommand = terminal.onCommandFinished((info) => {
          publish({ type: "terminalCommandFinished", terminalId, subscriptionId, info });
        });
        const unsubscribeExit = terminal.onExit((info) => {
          publish({ type: "terminalExit", terminalId, subscriptionId, info });
          attachments.get(terminalId)?.release();
        });
        attachments.set(terminalId, {
          id: subscriptionId,
          release() {
            unsubscribeOutput();
            unsubscribeCommand();
            unsubscribeExit();
            attachments.delete(terminalId);
          },
        });
        return true;
      }

      async function close(operation: Operation<"terminalClose">) {
        const { terminalId, gracefulTimeoutMs, forceTimeoutMs } = operation;
        let completion = stopping.get(terminalId);
        if (!completion && manager.getTerminal(terminalId)) {
          completion = manager.killTerminalAndWait(terminalId, {
            gracefulTimeoutMs,
            forceTimeoutMs,
          });
          stopping.set(terminalId, completion);
        }
        try {
          await completion;
        } finally {
          if (stopping.get(terminalId) === completion) stopping.delete(terminalId);
        }
        return null;
      }

      function openReplay(operation: Operation<"terminalReplayOpen">) {
        const terminal = manager.getTerminal(operation.terminalId);
        if (!terminal) return null;
        if (replays.size >= MAX_REPLAYS || replays.has(operation.replayId))
          throw error("terminal_replay_limit");
        const { metadata, data } = terminal.serializeReplay({
          scrollbackLines: operation.scrollbackLines,
        });
        if (replayBytes + data.byteLength > MAX_REPLAY_BYTES) throw error("terminal_replay_limit");
        replays.set(operation.replayId, { bytes: data, offset: 0 });
        replayBytes += data.byteLength;
        return metadata;
      }

      function nextReplay(operation: Operation<"terminalReplayNext">) {
        const replay = replays.get(operation.replayId);
        if (!replay) return null;
        const chunk = replay.bytes.subarray(
          replay.offset,
          replay.offset + TERMINAL_REPLAY_CHUNK_BYTES,
        );
        replay.offset += chunk.byteLength;
        if (chunk.byteLength === 0) {
          dropReplay(operation.replayId);
          return null;
        }
        return Buffer.from(chunk).toString("base64");
      }

      async function capture(operation: Operation<"terminalCapture">) {
        const result = await manager.captureTerminal(operation.terminalId, operation);
        if (Buffer.byteLength(JSON.stringify(result)) > MAX_REPLAY_BYTES)
          throw error("terminal_capture_limit");
        return result;
      }

      return {
        dispose,
        async execute(operation: RemoteTerminalOperation): Promise<unknown> {
          if (closed) throw error("terminal_channel_closed");
          switch (operation.kind) {
            case "terminalList":
              return inventory();
            case "terminalCreate":
              return create(operation);
            case "terminalAttach":
              return attach(operation);
            case "terminalDetach": {
              const attachment = attachments.get(operation.terminalId);
              if (attachment?.id === operation.subscriptionId) attachment.release();
              return null;
            }
            case "terminalInput":
              get(operation.terminalId).send({
                type: "input",
                data: Buffer.from(operation.bytes, "base64").toString("utf8"),
              });
              return null;
            case "terminalResize":
              get(operation.terminalId).send({
                type: "resize",
                rows: operation.rows,
                cols: operation.cols,
              });
              return null;
            case "terminalClose":
              return close(operation);
            case "terminalReplayOpen":
              return openReplay(operation);
            case "terminalReplayNext":
              return nextReplay(operation);
            case "terminalReplayCancel":
              dropReplay(operation.replayId);
              return null;
            case "terminalCapture":
              return capture(operation);
            case "terminalRename":
              return manager.setTerminalTitle(operation.terminalId, operation.title);
            case "terminalSetActivity":
              return manager.setTerminalActivity(operation.terminalId, operation.state);
            case "terminalClearAttention":
              return manager.clearTerminalAttention(operation.terminalId);
          }
        },
      };
    },
    /** Stops this resident's PTYs. Channel and public client teardown call their own dispose. */
    dispose(): Promise<void> {
      disposal ??= (async () => {
        releaseChannel?.();

        await Promise.allSettled(starting.values());
        try {
          await Promise.all([
            ...stopping.values(),
            ...terminalIds()
              .filter((id) => !stopping.has(id))
              .map((id) => manager.killTerminalAndWait(id)),
          ]);
        } finally {
          unsubscribeRevision();
        }
      })();
      return disposal;
    },
  };
}
