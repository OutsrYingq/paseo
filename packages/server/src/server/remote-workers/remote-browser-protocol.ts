import { z } from "zod";
import { ProviderPaseoToolsPolicySchema } from "@getpaseo/protocol/provider-config";

export const BROWSER_HEADER = "x-paseo-browser";
export const BROWSER_VERSION = "1";

const BrowserMcpSchema = z.strictObject({
  url: z.string().regex(/^http:\/\/127\.0\.0\.1:\d+\/mcp\/voice$/),
  token: z.string().regex(/^[A-Za-z0-9_-]{43}$/),
});
const agentId = z.string().min(1);
const requestId = z.uuid();
const ToolResultSchema = z.strictObject({
  content: z.array(z.looseObject({ type: z.string(), text: z.string().optional() })),
  structuredContent: z.unknown().optional(),
  isError: z.boolean().optional(),
});
const BrowserResultSchema = z.discriminatedUnion("ok", [
  z.strictObject({ ok: z.literal(true), value: ToolResultSchema }),
  z.strictObject({ ok: z.literal(false), error: z.string() }),
]);
export type RemoteBrowserResult = z.infer<typeof BrowserResultSchema>;

export const RemoteBrowserOperations = [
  z.strictObject({ kind: z.literal("browserBind"), agentId, provider: z.enum(["codex", "omp"]) }),
  z.strictObject({ kind: z.literal("browserPolicy"), provider: z.literal("omp") }),
  z.strictObject({ kind: z.literal("browserUnbind"), agentId }),
  z.strictObject({ kind: z.literal("browserResult"), requestId, result: BrowserResultSchema }),
] as const;
export type RemoteBrowserOperation = z.infer<(typeof RemoteBrowserOperations)[number]>;

export const RemoteBrowserEvents = [
  z.strictObject({
    type: z.literal("browserAction"),
    agentId,
    requestId,
    tool: z.string().regex(/^browser_[a-z_]+$/),
    input: z.record(z.string(), z.unknown()),
  }),
] as const;
export type RemoteBrowserEvent = z.infer<(typeof RemoteBrowserEvents)[number]>;

export const remoteBrowserResults = {
  browserBind: BrowserMcpSchema.nullable(),
  browserPolicy: z.strictObject({
    enabled: z.boolean(),
    providerPolicy: ProviderPaseoToolsPolicySchema.optional(),
  }),
  browserUnbind: z.null(),
  browserResult: z.null(),
};
export const BROWSER_OPERATION_KINDS = new Set<string>(Object.keys(remoteBrowserResults));
