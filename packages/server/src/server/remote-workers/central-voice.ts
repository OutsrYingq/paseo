import { randomUUID } from "node:crypto";
import type { SessionInboundMessage, SessionOutboundMessage } from "@getpaseo/protocol/messages";
import type { Logger } from "pino";
import type { openCentralAgents } from "./central-agents.js";
import type { WorkerEvent, WorkerOperation } from "./execution-protocol.js";
import type { WorkerConnection } from "./worker-connection.js";

type VoiceEvent = Extract<WorkerEvent, { type: "voiceOutput" | "voiceAgentAction" }>;
type VoiceInput = Extract<WorkerOperation, { kind: "voiceInput" }>["message"];
type AgentOperations = Pick<Awaited<ReturnType<typeof openCentralAgents>>, "executeVoiceAction" | "releaseVoiceRuntime">;

export function isVoiceInput(message: SessionInboundMessage): message is VoiceInput {
  return message.type === "set_voice_mode" || message.type === "voice_audio_chunk" ||
    message.type === "audio_played" || message.type === "abort_request";
}

export function createCentralVoice(connection: WorkerConnection, logger: Logger) {
  const sources = new Map<string, (event: VoiceEvent) => void>();
  const agentOwners = new Map<string, string>();

  return {
    deliver(event: VoiceEvent) {
      sources.get(event.source)?.(event);
    },
    attach(agents: AgentOperations, signal: AbortSignal, emit: (message: SessionOutboundMessage) => void) {
      let active: { source: string; agentId: string } | undefined;
      let closing = false;
      let modeTail = Promise.resolve();
      let modeRejected = false;

      async function release() {
        const session = active;
        if (!session) return;
        closing = true;
        try {
          if (!connection.signal.aborted) await connection.call({ kind: "voiceClose", source: session.source });
        } finally {
          sources.delete(session.source);
          try {
            await agents.releaseVoiceRuntime(session.agentId);
          } finally {
            agentOwners.delete(session.agentId);
            active = undefined;
            closing = false;
          }
        }
      }

      async function answer(event: Extract<VoiceEvent, { type: "voiceAgentAction" }>) {
        const restoring = event.action.kind === "reload" && event.action.restore;
        let result: { ok: true; value: string | boolean | null } | { ok: false; error: string };
        try {
          if (event.source !== active?.source || event.agentId !== active.agentId ||
            ((closing || signal.aborted) && !restoring)) {
            throw new Error("Voice source is closed");
          }
          result = { ok: true, value: await agents.executeVoiceAction(event.agentId, event.action, restoring ? undefined : signal) };
        } catch (error) {
          result = { ok: false, error: error instanceof Error ? error.message : "Voice action failed" };
        }
        await connection.call({ kind: "voiceAgentResult", source: event.source, id: event.id, result });
      }

      function receive(event: VoiceEvent) {
        if (event.type === "voiceAgentAction") {
          void answer(event).catch((err) => logger.warn({ err }, "Failed to complete resident voice action"));
        } else if (!signal.aborted && !closing) {
          if (event.message.type === "set_voice_mode_response") {
            modeRejected = !event.message.payload.accepted && !event.message.payload.enabled;
          }
          emit(event.message);
        }
      }

      async function setMode(message: Extract<VoiceInput, { type: "set_voice_mode" }>) {
        signal.throwIfAborted();
        if (!connection.supportsVoice) throw new Error("Voice mode unavailable");
        if (message.enabled) {
          const agentId = message.agentId?.trim();
          if (!agentId) throw new Error("An agent is required for voice mode");
          if (active?.agentId !== agentId) {
            if (agentOwners.has(agentId)) throw new Error("Voice mode is already active for this agent");
            await release();
            signal.throwIfAborted();
            if (agentOwners.has(agentId)) throw new Error("Voice mode is already active for this agent");
            const source = randomUUID();
            active = { source, agentId };
            agentOwners.set(agentId, source);
            sources.set(source, receive);
          }
        } else if (!active) {
          if (message.requestId) emit({ type: "set_voice_mode_response", payload: {
            requestId: message.requestId, enabled: false, agentId: null, accepted: true, error: null,
          } });
          return;
        }
        if (!active) return;
        modeRejected = false;
        await connection.call({
          kind: "voiceInput",
          source: active.source,
          message: message.enabled ? { ...message, agentId: active.agentId } : message,
        });
        if (!message.enabled || modeRejected) await release();
      }

      return {
        async dispatch(message: VoiceInput) {
          if (message.type === "set_voice_mode") {
            const operation = modeTail.then(() => setMode(message)).catch(async (error) => {
              if (message.requestId && !signal.aborted) emit({
                type: "set_voice_mode_response", payload: {
                  requestId: message.requestId, enabled: false, agentId: null, accepted: false,
                  error: error instanceof Error ? error.message : "Voice mode unavailable",
                },
              });
              await release();
            });
            modeTail = operation.catch((err) => logger.warn({ err }, "Failed to close voice mode"));
            await operation;
          } else if (active && !closing && !signal.aborted) {
            await connection.call({ kind: "voiceInput", source: active.source, message });
          }
        },
        close() {
          return modeTail.then(release);
        },
      };
    },
  };
}
