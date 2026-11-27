import { z } from "zod";
import {
  TERMINAL_ACTIVITY_ATTENTION_REASONS,
  TERMINAL_ACTIVITY_STATES,
} from "@getpaseo/protocol/terminal-activity";

const terminalId = z.string().min(1).max(256);
const subscriptionId = z.uuid();
const replayId = z.uuid();
const dimension = z.number().int().positive().max(1000);
const cwd = z.string().min(1);
const optionalTimeout = z.number().int().min(0).max(10_000).optional();
const safeOffset = z.number().int();
const activityState = z.enum(TERMINAL_ACTIVITY_STATES);
const activity = z
  .strictObject({
    state: activityState,
    attentionReason: z.enum(TERMINAL_ACTIVITY_ATTENTION_REASONS).nullable().optional(),
    changedAt: z.number(),
  })
  .nullable();

const terminalInfo = z.strictObject({
  id: terminalId,
  workspaceId: z.string().min(1),
  cwd,
  name: z.string(),
  title: z.string().optional(),
  createdAt: z.string().optional(),
  activity,
  hasOutput: z.boolean(),
  rows: dimension,
  cols: dimension,
});
const revision = z.number().int().nonnegative();
const inventory = z.strictObject({ revision, terminals: z.array(terminalInfo) });
const replayMetadata = z.strictObject({
  rows: dimension,
  cols: dimension,
  retainedHistoryLines: z.number().int().nonnegative(),
  revision,
  replayPreamble: z.string(),
});
const exitInfo = z.strictObject({
  exitCode: z.number().int().nullable(),
  signal: z.number().int().nullable(),
  lastOutputLines: z.array(z.string()),
});

export const RemoteTerminalOperationSchema = z.discriminatedUnion("kind", [
  z.strictObject({ kind: z.literal("terminalList") }),
  z.strictObject({
    kind: z.literal("terminalCreate"),
    terminalId,
    cwd,
    workspaceId: z.string().min(1),
    name: z.string().optional(),
    title: z.string().optional(),
    command: z.string().min(1).optional(),
    args: z.array(z.string()).optional(),
    rows: dimension.optional(),
    cols: dimension.optional(),
  }),
  z.strictObject({ kind: z.literal("terminalAttach"), terminalId, subscriptionId }),
  z.strictObject({ kind: z.literal("terminalDetach"), terminalId, subscriptionId }),
  z.strictObject({ kind: z.literal("terminalInput"), terminalId, bytes: z.base64() }),
  z.strictObject({
    kind: z.literal("terminalResize"),
    terminalId,
    rows: dimension,
    cols: dimension,
  }),
  z.strictObject({
    kind: z.literal("terminalClose"),
    terminalId,
    gracefulTimeoutMs: optionalTimeout,
    forceTimeoutMs: optionalTimeout,
  }),
  z.strictObject({
    kind: z.literal("terminalReplayOpen"),
    terminalId,
    replayId,
    scrollbackLines: z.number().int().nonnegative().optional(),
  }),
  z.strictObject({ kind: z.literal("terminalReplayNext"), replayId }),
  z.strictObject({ kind: z.literal("terminalReplayCancel"), replayId }),
  z.strictObject({
    kind: z.literal("terminalCapture"),
    terminalId,
    start: safeOffset.optional(),
    end: safeOffset.optional(),
    stripAnsi: z.boolean().optional(),
  }),
  z.strictObject({ kind: z.literal("terminalRename"), terminalId, title: z.string() }),
  z.strictObject({ kind: z.literal("terminalSetActivity"), terminalId, state: activityState }),
  z.strictObject({ kind: z.literal("terminalClearAttention"), terminalId }),
]);

export const RemoteTerminalEventSchema = z.discriminatedUnion("type", [
  z.strictObject({ type: z.literal("terminalChanged"), cwd, inventory }),
  z.strictObject({
    type: z.literal("terminalReady"),
    terminalId,
    subscriptionId,
    revision,
    replayPreamble: z.string(),
  }),
  z.strictObject({
    type: z.literal("terminalOutput"),
    terminalId,
    subscriptionId,
    revision: revision.optional(),
    bytes: z.base64(),
    replayPreamble: z.string(),
  }),
  z.strictObject({ type: z.literal("terminalExit"), terminalId, subscriptionId, info: exitInfo }),
  z.strictObject({
    type: z.literal("terminalCommandFinished"),
    terminalId,
    subscriptionId,
    info: z.strictObject({ exitCode: z.number().int().nullable() }),
  }),
]);

export type RemoteTerminalOperation = z.infer<typeof RemoteTerminalOperationSchema>;
export type RemoteTerminalEvent = z.infer<typeof RemoteTerminalEventSchema>;
export type RemoteTerminalInfo = z.infer<typeof terminalInfo>;
export type RemoteTerminalInventory = z.infer<typeof inventory>;

export const remoteTerminalOperationResults = {
  terminalList: inventory,
  terminalCreate: terminalInfo,
  terminalAttach: z.boolean(),
  terminalDetach: z.null(),
  terminalInput: z.null(),
  terminalResize: z.null(),
  terminalClose: z.null(),
  terminalReplayOpen: replayMetadata.nullable(),
  terminalReplayNext: z.base64().nullable(),
  terminalReplayCancel: z.null(),
  terminalCapture: z.strictObject({
    lines: z.array(z.string()),
    totalLines: z.number().int().nonnegative(),
  }),
  terminalRename: z.boolean(),
  terminalSetActivity: z.boolean(),
  terminalClearAttention: z.boolean(),
} satisfies Record<RemoteTerminalOperation["kind"], z.ZodType>;

export type RemoteTerminalResult<K extends RemoteTerminalOperation["kind"]> = z.infer<
  (typeof remoteTerminalOperationResults)[K]
>;
