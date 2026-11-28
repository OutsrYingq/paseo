import {
  SpeechLocalModelsDeleteRequestSchema,
  SpeechLocalModelsDeleteResponseSchema,
  SpeechLocalModelsDownloadRequestSchema,
  SpeechLocalModelsDownloadResponseSchema,
  SpeechLocalModelsListRequestSchema,
  SpeechLocalModelsListResponseSchema,
} from "@getpaseo/protocol/speech/rpc-schemas";
import { z } from "zod";
import { SpeechModelsSession } from "../session/speech/speech-models-session.js";
import type { SpeechService } from "../speech/speech-runtime.js";

const RequestSchema = z.discriminatedUnion("type", [
  SpeechLocalModelsListRequestSchema,
  SpeechLocalModelsDownloadRequestSchema,
  SpeechLocalModelsDeleteRequestSchema,
]);
const ResponseSchema = z.discriminatedUnion("type", [
  SpeechLocalModelsListResponseSchema,
  SpeechLocalModelsDownloadResponseSchema,
  SpeechLocalModelsDeleteResponseSchema,
]);

export const SpeechModelsOperationSchema = z.strictObject({
  kind: z.literal("speechModels"),
  request: RequestSchema,
});
export const speechModelsResult = ResponseSchema;

export async function executeResidentSpeechModels(
  request: z.infer<typeof RequestSchema>,
  runtime: SpeechService,
) {
  let response: unknown;
  await new SpeechModelsSession({ runtime, emit: (message) => { response = message; } }).dispatch(request);
  return ResponseSchema.parse(response);
}
