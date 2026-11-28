import { randomUUID } from "node:crypto";
import { EventEmitter } from "node:events";
import { Readable } from "node:stream";
import pino from "pino";
import { expect, test, vi } from "vitest";
import type { SpeechReadinessState } from "../speech/speech-runtime.js";
import type { RemoteVoiceEvent } from "./remote-voice-protocol.js";
import { openResidentVoice } from "./resident-voice.js";

class TestAudioSession extends EventEmitter {
  readonly requiredSampleRate = 16_000;
  async connect() {}
  appendPcm16() {}
  clear() {}
  close() {}
  commit() {}
  flush() {}
  reset() {}
}

test("scoped MCP speak streams audio and waits for the owning source to acknowledge playback", async () => {
  const source = randomUUID();
  const agentId = randomUUID();
  const requestId = randomUUID();
  const signal = new AbortController();
  const ready: SpeechReadinessState = {
    enabled: true,
    available: true,
    reasonCode: "ready",
    message: "",
    retryable: false,
    missingModelIds: [],
  };
  type Reload = Extract<
    Extract<RemoteVoiceEvent, { type: "voiceAgentAction" }>["action"],
    { kind: "reload" }
  >;
  let endpoint: Reload["speakMcp"];
  let restored = false;
  const events: RemoteVoiceEvent[] = [];
  const voice = openResidentVoice(
    () => ({
      getReadiness: () => ({
        generatedAt: new Date().toISOString(),
        requiredLocalModelIds: [],
        missingLocalModelIds: [],
        download: { inProgress: false, error: null },
        realtimeVoice: ready,
        dictation: ready,
        voiceFeature: ready,
      }),
      onReadinessChange: () => () => {},
      resolveStt: () => ({ id: "local", createSession: () => new TestAudioSession() }),
      resolveTurnDetection: () => ({ id: "local", createSession: () => new TestAudioSession() }),
      resolveSttLanguage: () => "en",
      resolveTts: () => ({
        async synthesizeSpeech() {
          return { stream: Readable.from([Buffer.from("audio")]), format: "pcm;rate=24000" };
        },
      }),
    }),
    (event) => {
      events.push(event);
      if (event.type !== "voiceAgentAction") return;
      if (event.action.kind === "reload") {
        if (event.action.restore) restored = true;
        else endpoint = event.action.speakMcp;
      }
      void voice.execute({
        kind: "voiceAgentResult",
        source,
        id: event.id,
        result: {
          ok: true,
          value: event.action.kind === "reload" ? agentId : null,
        },
      });
    },
    pino({ level: "silent" }),
    signal.signal,
  );

  try {
    await voice.execute({ kind: "voiceReadiness" });
    await voice.execute({
      kind: "voiceInput",
      source,
      message: {
        type: "set_voice_mode",
        enabled: true,
        agentId,
        requestId,
      },
    });
    expect(events).toContainEqual({
      type: "voiceOutput",
      source,
      message: {
        type: "set_voice_mode_response",
        payload: { requestId, enabled: true, agentId, accepted: true, error: null },
      },
    });
    if (!endpoint) throw new Error("Voice MCP endpoint unavailable");
    const { url, token } = endpoint;
    const call = (authorization: string) =>
      fetch(url, {
        method: "POST",
        signal: signal.signal,
        headers: {
          authorization,
          "content-type": "application/json",
          accept: "application/json, text/event-stream",
          "mcp-protocol-version": "2025-06-18",
        },
        body: JSON.stringify({
          jsonrpc: "2.0",
          id: 1,
          method: "tools/call",
          params: { name: "speak", arguments: { text: "One sentence." } },
        }),
      });
    expect((await call("Bearer invalid")).status).toBe(401);
    const callFinished = vi.fn();
    const playback = call(`Bearer ${token}`).then(async (response) => {
      const data = await response.text();
      callFinished();
      return { status: response.status, data };
    });
    void playback.catch(() => undefined);
    const audio = () =>
      events.flatMap((event) =>
        event.type === "voiceOutput" && event.message.type === "audio_output"
          ? [event.message.payload]
          : [],
      );
    await vi.waitFor(() => expect(audio()).toHaveLength(1));
    expect(Buffer.from(audio()[0].audio, "base64").toString()).toBe("audio");
    const message = { type: "audio_played" as const, id: audio()[0].id };
    await voice.execute({ kind: "voiceInput", source: randomUUID(), message });
    expect(callFinished).not.toHaveBeenCalled();
    await voice.execute({ kind: "voiceInput", source, message });
    const result = await playback;
    expect(result.status).toBe(200);
    expect(result.data).toContain('"structuredContent":{"ok":true}');
    await voice.execute({ kind: "voiceClose", source });
    expect(restored).toBe(true);
    expect((await call(`Bearer ${token}`)).status).toBe(401);
  } finally {
    signal.abort();
    await voice.close();
  }
});
