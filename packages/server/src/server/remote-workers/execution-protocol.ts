import {
  AgentStreamEventPayloadSchema,
  FileExplorerResponseSchema,
} from "@getpaseo/protocol/messages";
import { z } from "zod";
import {
  RemoteTerminalOperationSchema,
  RemoteTerminalEventSchema,
  remoteTerminalOperationResults,
} from "./remote-terminals-protocol.js";
import {
  RemotePluginOperationSchema,
  RemotePluginEventSchema,
  RemotePluginResultSchema,
} from "./remote-plugins-protocol.js";
import { WorkerBindingSchema } from "./leases.js";

export const EXECUTION_PROTOCOL = "paseo.worker.v1";
export const EXECUTION_PATH = "/execute";
export const EXECUTION_FEATURES_HEADER = "x-paseo-execution-features";
export const RUNTIME_RESTART_FEATURE = "restart-runtime";
export const EXECUTION_MAX_FRAME = 32 * 1024 * 1024;
export const EXECUTION_MAX_CALLS = 128;

export const NativeLaunchSchema = z.discriminatedUnion("provider", [
  z.strictObject({
    provider: z.literal("codex"),
    goals: z.boolean(),
    agentId: z.string().min(1).optional(),
  }),
  z.strictObject({
    provider: z.literal("omp"),
    agentId: z.string().min(1).optional(),
    cwd: z.string().min(1),
    protocolMode: z.enum(["rpc", "rpc-ui"]).optional(),
    model: z.string().optional(),
    thinkingOptionId: z.string().optional(),
    session: z.string().optional(),
    noSession: z.boolean().optional(),
    systemPrompt: z.string().optional(),
  }),
]);

const processId = z.uuid();
const provider = z.enum(["codex", "omp"]);
const fileTarget = { cwd: z.string().min(1), path: z.string().min(1) };
export const WorkerOperationSchema = z.discriminatedUnion("kind", [
  ...RemoteTerminalOperationSchema.options,
  RemotePluginOperationSchema,
  z.strictObject({ kind: z.literal("restartRuntime"), keepTerminals: z.boolean() }),
  z.strictObject({
    kind: z.literal("ompHistoryOpen"),
    sessionFile: z.string().min(1),
    activeBranchEntryId: z.string().optional(),
  }),
  z.strictObject({ kind: z.literal("ompHistoryNext"), id: z.uuid() }),
  z.strictObject({ kind: z.literal("ompHistoryClose"), id: z.uuid() }),
  z.strictObject({ kind: z.literal("workspaceRefs") }),
  z.strictObject({
    kind: z.literal("explore"),
    ...fileTarget,
    mode: z.enum(["file", "list"]),
  }),
  z.strictObject({ kind: z.literal("downloadInfo"), ...fileTarget }),
  z.strictObject({ kind: z.literal("downloadOpen"), ...fileTarget }),
  z.strictObject({
    kind: z.literal("downloadRead"),
    id: z.uuid(),
    offset: z.number().int().nonnegative(),
  }),
  z.strictObject({ kind: z.literal("downloadClose"), id: z.uuid() }),
  z.strictObject({ kind: z.literal("inspectDirectory"), cwd: z.string().min(1) }),
  z.strictObject({
    kind: z.literal("workspacePath"),
    cwd: z.string().min(1),
    parent: z.string().min(1),
  }),
  z.strictObject({ kind: z.literal("providers") }),
  z.strictObject({ kind: z.literal("providerHome") }),
  z.strictObject({
    kind: z.literal("spawn"),
    processId,
    launch: NativeLaunchSchema,
  }),
  z.strictObject({ kind: z.literal("stdin"), processId, bytes: z.base64() }),
  z.strictObject({ kind: z.literal("endStdin"), processId }),
  z.strictObject({
    kind: z.literal("signal"),
    processId,
    signal: z.enum(["SIGTERM", "SIGKILL", "SIGINT"]),
  }),
  z.strictObject({ kind: z.literal("readState"), path: z.string() }),
  z.strictObject({ kind: z.literal("listState"), path: z.string() }),
  z.strictObject({
    kind: z.literal("writeState"),
    path: z.string(),
    text: z.string(),
    exclusive: z.boolean().optional(),
  }),
  z.strictObject({ kind: z.literal("deleteState"), path: z.string() }),
]);

export const ExecutionRequestSchema = z.discriminatedUnion("kind", [
  z.strictObject({ kind: z.literal("bind"), binding: WorkerBindingSchema }),
  z.strictObject({
    kind: z.literal("call"),
    id: z.uuid(),
    operation: WorkerOperationSchema,
  }),
]);

const WorkerEventSchema = z.discriminatedUnion("type", [
  ...RemoteTerminalEventSchema.options,
  RemotePluginEventSchema,
  z.strictObject({
    type: z.literal("output"),
    processId,
    stream: z.enum(["stdout", "stderr"]),
    bytes: z.base64(),
  }),
  z.strictObject({
    type: z.literal("exit"),
    processId,
    code: z.number().int().nullable(),
    signal: z.string().nullable(),
  }),
]);

export const ExecutionResponseSchema = z.discriminatedUnion("kind", [
  z.strictObject({
    kind: z.literal("bound"),
    binding: WorkerBindingSchema,
    restartRuntime: z.literal(true).optional(),
  }),
  z.strictObject({
    kind: z.literal("result"),
    id: z.uuid(),
    value: z.unknown(),
  }),
  z.strictObject({ kind: z.literal("error"), id: z.uuid(), code: z.string() }),
  z.strictObject({ kind: z.literal("event"), event: WorkerEventSchema }),
]);

export type NativeLaunch = z.infer<typeof NativeLaunchSchema>;
export type WorkerOperation = z.infer<typeof WorkerOperationSchema>;
export type WorkerEvent = z.infer<typeof WorkerEventSchema>;

export const operationResults = {
  ...remoteTerminalOperationResults,
  plugin: RemotePluginResultSchema,
  restartRuntime: z.null(),
  providerHome: z.string().min(1),
  workspacePath: z.string().nullable(),
  ompHistoryOpen: z.uuid(),
  ompHistoryNext: z.object({ done: z.boolean(), events: z.array(AgentStreamEventPayloadSchema) }),
  ompHistoryClose: z.null(),
  workspaceRefs: z.array(z.strictObject({ workspaceId: z.string(), cwd: z.string() })),
  explore: FileExplorerResponseSchema.shape.payload.pick({
    directory: true,
    file: true,
  }),
  downloadInfo: z.strictObject({
    fileName: z.string(),
    mimeType: z.string(),
    size: z.number().int().nonnegative(),
  }),
  downloadOpen: z.strictObject({
    id: z.uuid(),
    size: z.number().int().nonnegative(),
  }),
  downloadRead: z.base64(),
  downloadClose: z.null(),
  inspectDirectory: z.enum(["directory", "missing", "not_directory"]),
  providers: z.array(provider),
  spawn: z.null(),
  stdin: z.null(),
  endStdin: z.null(),
  signal: z.null(),
  readState: z.string().nullable(),
  listState: z.array(z.strictObject({ name: z.string(), kind: z.enum(["file", "directory"]) })),
  writeState: z.null(),
  deleteState: z.null(),
} satisfies Record<WorkerOperation["kind"], z.ZodType>;

export type WorkerResult<K extends WorkerOperation["kind"]> = z.infer<(typeof operationResults)[K]>;
export type ExecutionResponse = z.infer<typeof ExecutionResponseSchema>;

export class WorkerOperationError extends Error {}

export function decodeJson(data: { toString(): string }): unknown {
  try {
    return JSON.parse(data.toString());
  } catch {
    return undefined;
  }
}
