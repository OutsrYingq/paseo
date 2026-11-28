import { randomUUID } from "node:crypto";
import type { Logger } from "pino";
import { VoiceSession } from "../session/voice/voice-session.js";
import type { SpeechService } from "../speech/speech-runtime.js";
import type { VoiceSpeakHandler } from "../voice-types.js";
import { WorkerOperationError } from "./execution-protocol.js";
import {
  VoiceOutputSchema,
  type RemoteVoiceEvent,
  type RemoteVoiceOperation,
  type VoiceAgentAction,
  type VoiceAgentResult,
} from "./remote-voice-protocol.js";
import { createResidentVoiceMcp } from "./resident-voice-mcp.js";

const MAX_VOICE_SOURCES = 16;
const MAX_VOICE_CHUNK = 256 * 1024;
const ACTION_TIMEOUT_MS = 15_000;

type VoiceMcpBinding = ReturnType<ReturnType<typeof createResidentVoiceMcp>["register"]>;
type VoiceSource = {
  agentId: string;
  voice: VoiceSession;
  transition: Promise<void>;
  closing?: Promise<void>;
  speak?: { mcp: VoiceMcpBinding; lifetime: AbortController };
};

export function openResidentVoice(
  getSpeech: () => Pick<
    SpeechService,
    | "getReadiness"
    | "onReadinessChange"
    | "resolveStt"
    | "resolveSttLanguage"
    | "resolveTts"
    | "resolveTurnDetection"
  >,
  publish: (event: RemoteVoiceEvent) => void,
  logger: Logger,
  signal: AbortSignal,
) {
  const mcp = createResidentVoiceMcp();
  const sources = new Map<string, VoiceSource>();
  const pending = new Map<string, { source: string; finish(result: VoiceAgentResult): void }>();
  let stopReadiness: (() => void) | undefined;
  let closing: Promise<void> | undefined;

  function readiness() {
    const { realtimeVoice, voiceFeature } = getSpeech().getReadiness();
    const state = !realtimeVoice.enabled || voiceFeature.available ? realtimeVoice : voiceFeature;
    return {
      enabled: state.enabled && state.available,
      reason: state.available ? "" : state.message,
    };
  }

  function request(sourceId: string, agentId: string, action: VoiceAgentAction) {
    const source = sources.get(sourceId);
    const restoring = action.kind === "reload" && action.restore;
    if (signal.aborted || !source || source.agentId !== agentId || (source.closing && !restoring)) {
      return Promise.reject(new WorkerOperationError("worker_voice_source_closed"));
    }
    const id = randomUUID();
    return new Promise<string | boolean | null>((resolve, reject) => {
      const finish = (result: VoiceAgentResult) => {
        clearTimeout(timeout);
        pending.delete(id);
        if (result.ok) resolve(result.value);
        else reject(new WorkerOperationError(result.error));
      };
      const timeout = setTimeout(
        () => finish({ ok: false, error: "worker_voice_agent_timeout" }),
        ACTION_TIMEOUT_MS,
      );
      pending.set(id, { source: sourceId, finish });
      publish({ type: "voiceAgentAction", source: sourceId, id, agentId, action });
    });
  }

  function revokeSpeak(source: VoiceSource) {
    const binding = source.speak;
    source.speak = undefined;
    binding?.mcp.revoke();
    binding?.lifetime.abort();
  }

  function createSource(sourceId: string, agentId: string) {
    const speech = getSpeech();
    const voice = new VoiceSession({
      logger,
      sessionId: sourceId,
      stt: speech.resolveStt,
      sttLanguage: speech.resolveSttLanguage(),
      tts: speech.resolveTts,
      voice: { turnDetection: speech.resolveTurnDetection },
      dictation: { getSpeechReadiness: speech.getReadiness },
      voiceBridge: {
        registerVoiceSpeakHandler(boundAgentId, handler) {
          if (source.closing || signal.aborted || boundAgentId !== source.agentId) {
            throw new WorkerOperationError("worker_voice_source_closed");
          }
          revokeSpeak(source);
          const lifetime = new AbortController();
          const scoped: VoiceSpeakHandler = async (params) => {
            if (lifetime.signal.aborted || !voice.isActiveForAgent(boundAgentId)) {
              throw new WorkerOperationError("worker_voice_speak_unavailable");
            }
            await handler({
              ...params,
              callerAgentId: boundAgentId,
              signal: params.signal
                ? AbortSignal.any([lifetime.signal, params.signal])
                : lifetime.signal,
            });
          };
          source.speak = { lifetime, mcp: mcp.register(boundAgentId, scoped) };
        },
        unregisterVoiceSpeakHandler() {
          revokeSpeak(source);
        },
      },
      host: {
        emit(message) {
          if (source.closing || signal.aborted || message.type === "activity_log") return;
          const parsed = VoiceOutputSchema.safeParse(message);
          if (parsed.success)
            publish({ type: "voiceOutput", source: sourceId, message: parsed.data });
        },
        async loadAgent(id) {
          const value = await request(sourceId, id, { kind: "load" });
          if (value !== null && typeof value !== "string")
            throw new Error("worker_voice_agent_result_invalid");
          return { config: { systemPrompt: value ?? undefined } };
        },
        async reloadAgentSession(id, overrides, restore = false) {
          const value = await request(sourceId, id, {
            kind: "reload",
            systemPrompt: overrides.systemPrompt ?? null,
            restore,
            ...(restore ? {} : { speakMcp: source.speak?.mcp.descriptor }),
          });
          if (value !== id) throw new Error("worker_voice_agent_result_invalid");
          return { id };
        },
        async sendSpokenInput(id, text) {
          await request(sourceId, id, { kind: "submit", text });
        },
        async interruptAgentIfRunning(id) {
          await request(sourceId, id, { kind: "interrupt" });
        },
        async hasActiveAgentRun(id) {
          return id !== null && (await request(sourceId, id, { kind: "hasActiveRun" })) === true;
        },
      },
    });
    const source: VoiceSource = { agentId, voice, transition: Promise.resolve() };
    sources.set(sourceId, source);
    return source;
  }

  function closeSource(sourceId: string) {
    const source = sources.get(sourceId);
    if (!source) return;
    if (source.closing) return source.closing;
    source.closing = source.transition
      .catch(() => undefined)
      .then(async () => {
        try {
          await source.voice.cleanup();
        } finally {
          sources.delete(sourceId);
        }
      });
    revokeSpeak(source);
    try {
      source.voice.cancel();
    } catch {
      // cleanup will still stop the turn controller and restore the prompt.
    }
    return source.closing;
  }

  function close() {
    if (closing) return closing;
    stopReadiness?.();
    if (signal.aborted) {
      for (const entry of pending.values())
        entry.finish({ ok: false, error: "worker_connection_lost" });
    }
    closing = Promise.allSettled([...sources.keys()].map(closeSource)).then(async (results) => {
      await mcp.close();
      const failed = results.find((result) => result.status === "rejected");
      if (failed?.status === "rejected") throw failed.reason;
    });
    return closing;
  }
  signal.addEventListener(
    "abort",
    () => {
      for (const entry of pending.values())
        entry.finish({ ok: false, error: "worker_connection_lost" });
      void close().catch(() => undefined);
    },
    { once: true },
  );

  return {
    close,
    async execute(operation: RemoteVoiceOperation) {
      if (operation.kind === "voiceAgentResult") {
        const entry = pending.get(operation.id);
        if (entry?.source === operation.source) entry.finish(operation.result);
        return null;
      }
      if (operation.kind === "voiceClose") {
        await closeSource(operation.source);
        return null;
      }
      if (closing || signal.aborted) throw new WorkerOperationError("worker_voice_source_closed");
      if (operation.kind === "voiceReadiness") {
        await mcp.start();
        if (closing) throw new WorkerOperationError("worker_voice_source_closed");
        stopReadiness ??= getSpeech().onReadinessChange(() =>
          publish({ type: "voiceReadiness", state: readiness() }),
        );
        return readiness();
      }
      const { source: sourceId } = operation;
      let source = sources.get(sourceId);
      const { message } = operation;
      if (message.type === "set_voice_mode") {
        if (!source && message.enabled) {
          if (
            !message.agentId ||
            sources.size >= MAX_VOICE_SOURCES ||
            [...sources.values()].some((entry) => entry.agentId === message.agentId)
          ) {
            throw new WorkerOperationError("worker_voice_agent_unavailable");
          }
          source = createSource(sourceId, message.agentId);
        }
        if (!source || source.closing || (message.enabled && source.agentId !== message.agentId)) {
          throw new WorkerOperationError("worker_voice_source_unavailable");
        }
        const current = source;
        const transition = current.transition.then(async () => {
          if (current.closing) return;
          if (message.enabled) await mcp.start();
          else revokeSpeak(current);
          if (current.closing) return;
          await current.voice.handleSetVoiceMode(
            message.enabled,
            message.agentId,
            message.requestId,
          );
        });
        current.transition = transition.catch(() => undefined);
        try {
          await transition;
        } finally {
          if (!current.voice.isActiveForAgent(current.agentId)) await closeSource(sourceId);
        }
        return null;
      }
      if (!source || source.closing || !source.voice.isActiveForAgent(source.agentId)) return null;
      if (message.type === "voice_audio_chunk") {
        if (message.audio.length > MAX_VOICE_CHUNK)
          throw new WorkerOperationError("worker_voice_chunk_too_large");
        await source.voice.handleAudioChunk(message);
      } else if (message.type === "audio_played") {
        source.voice.handleAudioPlayed(message.id);
      } else {
        await source.voice.handleAbort();
      }
      return null;
    },
  };
}
