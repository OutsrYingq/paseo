import {
  AgentStreamEventPayloadSchema,
  DirectorySuggestionsRequestSchema,
  DirectorySuggestionsResponseSchema,
  FileExplorerResponseSchema,
  FileVersionSchema,
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
import {
  WorkspaceFileMutationSchema,
  WorkspaceFileMutationResponseSchema,
} from "../session/files/workspace-file-mutations.js";
import {
  CheckoutQuerySchema,
  CheckoutQueryResponseSchema,
} from "../session/checkout/checkout-queries-protocol.js";
import { CheckoutActionSchema, CheckoutActionResultSchema } from "../session/checkout/checkout-actions-protocol.js";
import { AgentMetadataGenerationSchema } from "../persisted-config.js";
import { PersistedProjectRecordSchema } from "../workspace-registry.js";
import {
  RemoteDiffOperationSchema,
  RemoteDiffEventSchema,
  remoteDiffResults,
} from "./remote-diff-protocol.js";
import {
  RemoteScriptOperationSchema,
  RemoteScriptEventSchema,
  remoteScriptResults,
} from "./remote-scripts-protocol.js";
import { RemoteFileTransferOperationSchema, remoteFileTransferResults } from "./remote-file-transfer-protocol.js";
import {
  RemoteWorktreeProvisioningOperationSchema,
  RemoteWorktreeSetupEventSchema,
  remoteWorktreeProvisioningResults,
} from "./remote-worktree-provisioning-protocol.js";
import { RemoteOmpImportOperationSchema, remoteOmpImportResults } from "./remote-omp-import-protocol.js";
import { RemoteWorkspaceArchiveOperationSchema, remoteWorkspaceArchiveResults } from "./remote-workspace-archive-protocol.js";
import { RemoteHostSettingsOperationSchema, remoteHostSettingsResults } from "./remote-host-settings-protocol.js";
import { ProjectConfigOperationSchema, projectConfigResults } from "./remote-project-config-protocol.js";
import { RemoteProjectGithubOperationSchema, remoteProjectGithubResults } from "./remote-project-github-protocol.js";
import { RemoteProjectIconOperationSchema, remoteProjectIconResults } from "./remote-project-icon-protocol.js";
import {
  RemoteDictationEvents,
  RemoteDictationOperations,
  remoteDictationResults,
} from "./remote-dictation-protocol.js";

import { RemoteVoiceEvents, RemoteVoiceOperations, remoteVoiceResults } from "./remote-voice-protocol.js";
import { RemotePushOperationSchema, remotePushResults } from "./remote-push.js";

export const PUSH_HEADER = "x-paseo-push";
export const PUSH_VERSION = "1";
export const PUSH_OPERATION_KINDS = new Set<string>(Object.keys(remotePushResults));

export const EXECUTION_PROTOCOL = "paseo.worker.v1";
export const EXECUTION_PATH = "/execute";
export const EXECUTION_FEATURES_HEADER = "x-paseo-execution-features";
export const RUNTIME_RESTART_FEATURE = "restart-runtime";
export const WORKSPACE_OPERATIONS_HEADER = "x-paseo-workspace-operations";
export const WORKSPACE_OPERATIONS_VERSION = "1";
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
  ...RemoteOmpImportOperationSchema.options,
  z.strictObject({ kind: z.literal("workspaceRefs") }),
  ...RemoteWorktreeProvisioningOperationSchema.options,
  ...RemoteWorkspaceArchiveOperationSchema.options,
  ...RemoteHostSettingsOperationSchema.options,
  ...RemoteFileTransferOperationSchema.options,
  ...ProjectConfigOperationSchema.options,
  ...RemoteProjectGithubOperationSchema.options,
  ...RemoteProjectIconOperationSchema.options,
  ...RemoteDictationOperations,
  ...RemoteVoiceOperations,
  ...RemotePushOperationSchema.options,
  z.strictObject({
    kind: z.literal("explore"),
    ...fileTarget,
    mode: z.enum(["file", "list"]),
    maxBytes: z.number().int().nonnegative().optional(),
  }),
  z.strictObject({ kind: z.literal("downloadInfo"), ...fileTarget }),
  z.strictObject({ kind: z.literal("mutateWorkspaceFile"), request: WorkspaceFileMutationSchema }),
  z.strictObject({ kind: z.literal("observeWorkspaceFile"), id: z.uuid(), ...fileTarget }),
  z.strictObject({ kind: z.literal("unobserveWorkspaceFile"), id: z.uuid() }),
  z.strictObject({ kind: z.literal("inspectProject"), cwd: z.string().min(1) }),
  z.strictObject({ kind: z.literal("projectSuggestionRoot"), cwd: z.string().min(1) }),
  z.strictObject({ kind: z.literal("directorySuggestions"), request: DirectorySuggestionsRequestSchema }),
  z.strictObject({ kind: z.literal("projectDirectoryParent"), parentPath: z.string().min(1) }),
  z.strictObject({
    kind: z.literal("projectDirectoryChange"),
    parentPath: z.string().min(1),
    name: z.string().min(1),
    action: z.enum(["create", "rollback"]),
  }),
  z.strictObject({ kind: z.literal("checkoutQuery"), request: CheckoutQuerySchema }),
  z.strictObject({ kind: z.literal("checkoutAction"), request: CheckoutActionSchema }),
  z.strictObject({
    kind: z.literal("checkoutMetadata"),
    cwd: z.string().min(1),
    mode: z.enum(["commit", "pullRequest"]),
    baseRef: z.string().optional(),
  }),
  ...RemoteDiffOperationSchema.options,
  ...RemoteScriptOperationSchema.options,
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
  ...RemoteDiffEventSchema.options,
  RemoteScriptEventSchema,
  RemoteWorktreeSetupEventSchema,
  ...RemoteDictationEvents,
  ...RemoteVoiceEvents,
  z.strictObject({ type: z.literal("fileVersion"), id: z.uuid(), version: FileVersionSchema }),
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
    workspaceOperations: z.literal(true).optional(),
    dictation: z.literal(true).optional(),
    voice: z.literal(true).optional(),
    push: z.literal(true).optional(),
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

export const WORKSPACE_OPERATION_KINDS: ReadonlySet<WorkerOperation["kind"]> = new Set([
  "inspectProject",
  "projectSuggestionRoot",
  "directorySuggestions",
  "projectDirectoryParent",
  "projectDirectoryChange",
  "mutateWorkspaceFile",
  "observeWorkspaceFile",
  "unobserveWorkspaceFile",
  "checkoutQuery",
  "checkoutAction",
  "checkoutMetadata",
  ...RemoteDiffOperationSchema.options.map((schema) => schema.shape.kind.value),
  ...RemoteScriptOperationSchema.options.map((schema) => schema.shape.kind.value),
  ...RemoteFileTransferOperationSchema.options.map((schema) => schema.shape.kind.value),
  ...RemoteWorktreeProvisioningOperationSchema.options.map((schema) => schema.shape.kind.value),
  ...RemoteOmpImportOperationSchema.options.map((schema) => schema.shape.kind.value),
  ...RemoteWorkspaceArchiveOperationSchema.options.map((schema) => schema.shape.kind.value),
  ...RemoteHostSettingsOperationSchema.options.map((schema) => schema.shape.kind.value),
  ...ProjectConfigOperationSchema.options.map((schema) => schema.shape.kind.value),
  ...RemoteProjectGithubOperationSchema.options.map((schema) => schema.shape.kind.value),
  ...RemoteProjectIconOperationSchema.options.map((schema) => schema.shape.kind.value),
]);

export const operationResults = {
  ...remoteDiffResults,
  ...remoteScriptResults,
  ...remoteFileTransferResults,
  ...remoteWorktreeProvisioningResults,
  ...remoteOmpImportResults,
  ...remoteWorkspaceArchiveResults,
  ...remoteHostSettingsResults,
  ...projectConfigResults,
  ...remoteProjectGithubResults,
  ...remoteProjectIconResults,
  ...remoteDictationResults,
  ...remoteVoiceResults,
  ...remotePushResults,
  ...remoteTerminalOperationResults,
  plugin: RemotePluginResultSchema,
  restartRuntime: z.null(),
  providerHome: z.string().min(1),
  workspacePath: z.string().nullable(),
  ompHistoryOpen: z.uuid(),
  ompHistoryNext: z.object({ done: z.boolean(), events: z.array(AgentStreamEventPayloadSchema) }),
  ompHistoryClose: z.null(),
  workspaceRefs: z.array(z.strictObject({ workspaceId: z.string(), cwd: z.string() })),
  mutateWorkspaceFile: WorkspaceFileMutationResponseSchema,
  observeWorkspaceFile: FileVersionSchema,
  unobserveWorkspaceFile: z.null(),
  checkoutQuery: CheckoutQueryResponseSchema,
  checkoutAction: CheckoutActionResultSchema,
  checkoutMetadata: z.strictObject({
    cwd: z.string().min(1),
    prompt: z.string(),
    metadataGeneration: AgentMetadataGenerationSchema.pick({ providers: true }).optional(),
  }),
  projectDirectoryParent: z.string().min(1),
  projectDirectoryChange: z.null(),
  projectSuggestionRoot: z.string().nullable(),
  directorySuggestions: DirectorySuggestionsResponseSchema,
  inspectProject: PersistedProjectRecordSchema.pick({
    rootPath: true,
    kind: true,
    displayName: true,
  })
    .extend({ projectKey: z.string() })
    .nullable(),
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
