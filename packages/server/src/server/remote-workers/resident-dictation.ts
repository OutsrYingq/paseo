import type { Logger } from "pino";
import { DictationStreamManager } from "../dictation/dictation-stream-manager.js";
import type { SpeechService } from "../speech/speech-runtime.js";
import type { RemoteDictationEvent, RemoteDictationOperation } from "./remote-dictation-protocol.js";

export function openResidentDictation(
  getSpeech: () => SpeechService,
  publish: (event: RemoteDictationEvent) => void,
  logger: Logger,
) {
  const streams = new Map<string, { id: string; manager: DictationStreamManager }>();
  let stopReadiness: (() => void) | undefined;
  let closed = false;

  function unavailable() {
    const { dictation, voiceFeature } = getSpeech().getReadiness();
    if (!dictation.enabled) return dictation;
    if (!voiceFeature.available) return voiceFeature;
    return dictation.available ? null : dictation;
  }

  function readiness() {
    const failure = unavailable();
    return { enabled: !failure, reason: failure?.message ?? "" };
  }

  function closeSource(source: string) {
    const stream = streams.get(source);
    streams.delete(source);
    stream?.manager.cleanupAll();
  }

  return {
    async execute(operation: RemoteDictationOperation) {
      if (closed) throw new Error("Dictation channel is closed");
      if (operation.kind === "dictationReadiness") {
        stopReadiness ??= getSpeech().onReadinessChange(() => {
          publish({ type: "dictationReadiness", state: readiness() });
        });
        return readiness();
      }
      if (operation.kind === "dictationClose") {
        closeSource(operation.source);
        return null;
      }
      const { source, message } = operation;
      if (message.type === "dictation_stream_start") {
        closeSource(source);
        const failure = unavailable();
        if (failure) {
          publish({
            type: "dictationOutput",
            source,
            message: {
              type: "dictation_stream_error",
              payload: {
                dictationId: message.dictationId,
                error: failure.message,
                retryable: failure.retryable,
                reasonCode: failure.reasonCode,
                missingModelIds: [...failure.missingModelIds],
              },
            },
          });
          return null;
        }
        const manager = new DictationStreamManager({
          logger,
          sessionId: source,
          stt: getSpeech().resolveDictationStt,
          language: getSpeech().resolveDictationSttLanguage(),
          emit(event) {
            if (streams.get(source)?.manager === manager && event.type !== "activity_log") {
              publish({ type: "dictationOutput", source, message: event });
            }
          },
          onIdle() {
            if (streams.get(source)?.manager === manager && !manager.hasDemand) streams.delete(source);
          },
        });
        streams.set(source, { id: message.dictationId, manager });
        await manager.handleStart(message.dictationId, message.format);
        if (streams.get(source)?.manager === manager && !manager.hasDemand) streams.delete(source);
        return null;
      }
      const stream = streams.get(source);
      if (stream?.id !== message.dictationId) {
        if (message.type !== "dictation_stream_cancel") {
          publish({ type: "dictationOutput", source, message: {
            type: "dictation_stream_error",
            payload: { dictationId: message.dictationId, error: "Dictation stream not started", retryable: true },
          } });
        }
        return null;
      }
      switch (message.type) {
        case "dictation_stream_chunk":
          await stream.manager.handleChunk({ ...message, audioBase64: message.audio });
          break;
        case "dictation_stream_finish":
          await stream.manager.handleFinish(message.dictationId, message.finalSeq);
          break;
        case "dictation_stream_cancel":
          stream.manager.handleCancel(message.dictationId);
          break;
      }
      return null;
    },
    close() {
      closed = true;
      stopReadiness?.();
      for (const source of streams.keys()) {
        try {
          closeSource(source);
        } catch (err) {
          logger.warn({ err }, "Failed to close resident dictation stream");
        }
      }
    },
  };
}
