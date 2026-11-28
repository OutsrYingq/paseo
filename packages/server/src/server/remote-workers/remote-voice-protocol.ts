import { z } from "zod";
import {
  AbortRequestMessageSchema,
  AudioOutputMessageSchema,
  AudioPlayedMessageSchema,
  ServerCapabilityStateSchema,
  SetVoiceModeMessageSchema,
  SetVoiceModeResponseMessageSchema,
  TranscriptionResultMessageSchema,
  VoiceAudioChunkMessageSchema,
  VoiceInputStateMessageSchema,
} from "@getpaseo/protocol/messages";

export const VOICE_HEADER = "x-paseo-voice";
export const VOICE_VERSION = "1";

export const VoiceInputSchema = z.discriminatedUnion("type", [
  SetVoiceModeMessageSchema,
  VoiceAudioChunkMessageSchema,
  AudioPlayedMessageSchema,
  AbortRequestMessageSchema,
]);

export const VoiceOutputSchema = z.discriminatedUnion("type", [
  SetVoiceModeResponseMessageSchema,
  VoiceInputStateMessageSchema,
  TranscriptionResultMessageSchema,
  AudioOutputMessageSchema,
]);

const SpeakMcpSchema = z.strictObject({
  url: z.string().regex(/^http:\/\/127\.0\.0\.1:\d+\/mcp\/voice$/),
  token: z.string().regex(/^[A-Za-z0-9_-]{43}$/),
});

const VoiceAgentActionSchema = z.discriminatedUnion("kind", [
  z.strictObject({ kind: z.literal("load") }),
  z.strictObject({
    kind: z.literal("reload"),
    systemPrompt: z.string().nullable(),
    restore: z.boolean(),
    speakMcp: SpeakMcpSchema.optional(),
  }),
  z.strictObject({ kind: z.literal("submit"), text: z.string() }),
  z.strictObject({ kind: z.literal("interrupt") }),
  z.strictObject({ kind: z.literal("hasActiveRun") }),
]);
export type VoiceAgentAction = z.infer<typeof VoiceAgentActionSchema>;

const VoiceAgentResultSchema = z.discriminatedUnion("ok", [
  z.strictObject({ ok: z.literal(true), value: z.union([z.string(), z.boolean(), z.null()]) }),
  z.strictObject({ ok: z.literal(false), error: z.string() }),
]);
export type VoiceAgentResult = z.infer<typeof VoiceAgentResultSchema>;

const agentTarget = { source: z.uuid(), agentId: z.uuid(), id: z.uuid() };
export const RemoteVoiceOperations = [
  z.strictObject({ kind: z.literal("voiceReadiness") }),
  z.strictObject({ kind: z.literal("voiceInput"), source: z.uuid(), message: VoiceInputSchema }),
  z.strictObject({ kind: z.literal("voiceClose"), source: z.uuid() }),
  z.strictObject({
    kind: z.literal("voiceAgentResult"),
    source: z.uuid(),
    id: z.uuid(),
    result: VoiceAgentResultSchema,
  }),
] as const;
export type RemoteVoiceOperation = z.infer<(typeof RemoteVoiceOperations)[number]>;

export const RemoteVoiceEvents = [
  z.strictObject({ type: z.literal("voiceReadiness"), state: ServerCapabilityStateSchema }),
  z.strictObject({ type: z.literal("voiceOutput"), source: z.uuid(), message: VoiceOutputSchema }),
  z.strictObject({
    type: z.literal("voiceAgentAction"),
    ...agentTarget,
    action: VoiceAgentActionSchema,
  }),
] as const;
export type RemoteVoiceEvent = z.infer<(typeof RemoteVoiceEvents)[number]>;

export const remoteVoiceResults = {
  voiceReadiness: ServerCapabilityStateSchema,
  voiceInput: z.null(),
  voiceClose: z.null(),
  voiceAgentResult: z.null(),
};

export const VOICE_OPERATION_KINDS = new Set<string>(Object.keys(remoteVoiceResults));
