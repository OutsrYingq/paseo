import { z } from "zod";
import {
  DictationStreamAckMessageSchema,
  DictationStreamCancelMessageSchema,
  DictationStreamChunkMessageSchema,
  DictationStreamErrorMessageSchema,
  DictationStreamFinalMessageSchema,
  DictationStreamFinishAcceptedMessageSchema,
  DictationStreamFinishMessageSchema,
  DictationStreamPartialMessageSchema,
  DictationStreamStartMessageSchema,
  ServerCapabilityStateSchema,
} from "@getpaseo/protocol/messages";

export const DICTATION_HEADER = "x-paseo-dictation";
export const DICTATION_VERSION = "1";

export const DictationInputSchema = z.discriminatedUnion("type", [
  DictationStreamStartMessageSchema,
  DictationStreamChunkMessageSchema,
  DictationStreamFinishMessageSchema,
  DictationStreamCancelMessageSchema,
]);

const DictationOutputSchema = z.discriminatedUnion("type", [
  DictationStreamAckMessageSchema,
  DictationStreamFinishAcceptedMessageSchema,
  DictationStreamPartialMessageSchema,
  DictationStreamFinalMessageSchema,
  DictationStreamErrorMessageSchema,
]);
export type DictationOutput = z.infer<typeof DictationOutputSchema>;

export const RemoteDictationOperations = [
  z.strictObject({ kind: z.literal("dictationReadiness") }),
  z.strictObject({ kind: z.literal("dictationInput"), source: z.uuid(), message: DictationInputSchema }),
  z.strictObject({ kind: z.literal("dictationClose"), source: z.uuid() }),
] as const;
export type RemoteDictationOperation = z.infer<(typeof RemoteDictationOperations)[number]>;

export const RemoteDictationEvents = [
  z.strictObject({ type: z.literal("dictationReadiness"), state: ServerCapabilityStateSchema }),
  z.strictObject({ type: z.literal("dictationOutput"), source: z.uuid(), message: DictationOutputSchema }),
] as const;
export type RemoteDictationEvent = z.infer<(typeof RemoteDictationEvents)[number]>;

export const remoteDictationResults = {
  dictationReadiness: ServerCapabilityStateSchema,
  dictationInput: z.null(),
  dictationClose: z.null(),
};
