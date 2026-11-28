import { assertPluginWireValue } from "@getpaseo/protocol/binary-frames/index";
import {
  PluginIdSchema,
  PluginListItemSchema,
  PluginLogEntrySchema,
  PluginRegistryGetRequestSchema,
  PluginRegistryGetResponseSchema,
  PluginRegistryListResponseSchema,
  PluginRequirementsSchema,
  PluginSourceInstallRequestSchema,
  PluginSourceStatusItemSchema,
  PluginSourceUpdateItemSchema,
  PluginUpdateApplyRequestSchema,
  PluginUpdatePreviewRequestSchema,
  PluginUpdatePreviewSchema,
  PluginUpdateResultSchema,
  ProviderUsageSchema,
  UsageReportEntrySchema,
} from "@getpaseo/protocol/messages";
import {
  ProviderEventSchema,
  ProviderInputSchema,
  ProviderStatusSchema,
} from "@getpaseo/plugin/server/provider";
import { z } from "zod";

const id = z.uuid();
const name = z.string().min(1);
const plugin = z.strictObject({ id: PluginIdSchema, token: id });
const frame = z.discriminatedUnion("encoding", [
  z.strictObject({ encoding: z.literal("text"), data: z.string() }),
  z.strictObject({ encoding: z.literal("binary"), data: z.base64() }),
]);
const rpcPayload = z.discriminatedUnion("encoding", [
  z.strictObject({ encoding: z.literal("json"), data: z.json() }),
  z.strictObject({ encoding: z.literal("binary"), data: z.base64() }),
  z.strictObject({ encoding: z.literal("undefined") }),
]);
const rpc = { plugin, method: name, input: rpcPayload };

export const RemotePluginActionSchema = z.discriminatedUnion("action", [
  z.strictObject({
    action: z.literal("usageReports"),
    queryId: id,
    forceRefresh: z.boolean().optional(),
    reportIds: z.array(name).optional(),
    sessionId: name.optional(),
  }),
  z.strictObject({ action: z.literal("usageLegacy") }),
  z.strictObject({ action: z.literal("list") }),
  z.strictObject({ action: z.literal("inspectDirectory"), path: name }),
  z.strictObject({
    action: z.literal("installDirectory"),
    path: name,
    id: PluginIdSchema.optional(),
  }),
  z.strictObject({
    action: z.literal("installSource"),
    source: PluginSourceInstallRequestSchema.shape.source,
    id: PluginSourceInstallRequestSchema.shape.id,
    ref: PluginSourceInstallRequestSchema.shape.ref,
    pluginPath: PluginSourceInstallRequestSchema.shape.pluginPath,
  }),
  z.strictObject({ action: z.literal("statusSources"), pluginId: PluginIdSchema.optional() }),
  z.strictObject({ action: z.literal("updateSources"), pluginId: PluginIdSchema.optional() }),
  z.strictObject({
    action: z.literal("previewUpdates"),
    pluginId: PluginUpdatePreviewRequestSchema.shape.pluginId,
    target: PluginUpdatePreviewRequestSchema.shape.target,
  }),
  z.strictObject({
    action: z.literal("applyUpdates"),
    proposals: PluginUpdateApplyRequestSchema.shape.proposals,
  }),
  z.strictObject({ action: z.literal("listRegistry") }),
  z.strictObject({
    action: z.literal("getRegistryPlugin"),
    id: PluginRegistryGetRequestSchema.shape.id,
  }),
  z.strictObject({ action: z.literal("logs"), plugin }),
  z.strictObject({
    action: z.literal("catalog"),
    known: z.array(z.strictObject({ id: PluginIdSchema, clientBundleHash: name })).optional(),
  }),
  z.strictObject({ action: z.literal("reload"), plugin }),
  z.strictObject({ action: z.literal("enable"), plugin }),
  z.strictObject({ action: z.literal("disable"), plugin }),
  z.strictObject({ action: z.literal("remove"), plugin }),
  z.strictObject({ action: z.literal("invoke"), ...rpc }),
  z.strictObject({ action: z.literal("subscribe"), ...rpc, subscriptionId: id }),
  z.strictObject({ action: z.literal("unsubscribe"), subscriptionId: id }),
  z.strictObject({ action: z.literal("providers") }),
  z.strictObject({ action: z.literal("providerStatus"), token: id }),
  z.strictObject({
    action: z.literal("providerCatalogKey"),
    token: id,
    options: z.discriminatedUnion("scope", [
      z.strictObject({ scope: z.literal("global"), force: z.boolean().optional() }),
      z.strictObject({
        scope: z.literal("workspace"),
        cwd: z.string(),
        force: z.boolean().optional(),
      }),
    ]),
  }),
  z.strictObject({
    action: z.literal("providerConnect"),
    token: id,
    connectionId: id,
    request: z.strictObject({
      versions: z.array(z.number().int().positive()).min(1),
      capabilities: z.array(name),
    }),
  }),
  z.strictObject({
    action: z.literal("providerSend"),
    connectionId: id,
    input: ProviderInputSchema,
  }),
  z.strictObject({ action: z.literal("providerClose"), connectionId: id }),
  z.strictObject({ action: z.literal("sessionFrame"), sessionId: id, frame }),
  z.strictObject({ action: z.literal("sessionClose"), sessionId: id }),
]);

export const RemotePluginOperationSchema = z.strictObject({
  kind: z.literal("plugin"),
  request: RemotePluginActionSchema,
});
export type RemotePluginAction = z.infer<typeof RemotePluginActionSchema>;
export type RemotePluginOperation = z.infer<typeof RemotePluginOperationSchema>;

const listItem = z.strictObject({ plugin, item: PluginListItemSchema.strict() });
const provider = z.strictObject({
  id: name,
  token: id,
  label: name,
  description: z.string().optional(),
  docsUrl: z.string().optional(),
  icon: z.string().optional(),
  requiresConfiguration: z.boolean().optional(),
  hasStatus: z.boolean(),
  hasCatalogCacheKey: z.boolean(),
});

export const RemotePluginValueSchemas = {
  list: z.array(listItem),
  inspectDirectory: z.strictObject({ id: PluginIdSchema }),
  installDirectory: listItem,
  installSource: listItem,
  statusSources: z.array(PluginSourceStatusItemSchema),
  updateSources: z.array(PluginSourceUpdateItemSchema),
  previewUpdates: z.array(PluginUpdatePreviewSchema),
  applyUpdates: z.array(PluginUpdateResultSchema),
  listRegistry: PluginRegistryListResponseSchema.shape.payload.shape.index,
  getRegistryPlugin: PluginRegistryGetResponseSchema.shape.payload.shape.plugin,
  logs: z.array(PluginLogEntrySchema.strict()),
  usageReports: z.array(UsageReportEntrySchema),
  usageLegacy: z.strictObject({ fetchedAt: z.string(), providers: z.array(ProviderUsageSchema) }),
  catalog: z.array(
    z.strictObject({
      plugin,
      clientBundleHash: name,
      clientBundle: z.string().optional(),
      requirements: PluginRequirementsSchema.strict().optional(),
    }),
  ),
  reload: listItem,
  enable: listItem,
  disable: listItem,
  remove: z.null(),
  invoke: rpcPayload,
  subscribe: z.null(),
  unsubscribe: z.null(),
  providers: z.array(provider),
  providerStatus: ProviderStatusSchema,
  providerCatalogKey: z.string().nullable(),
  providerConnect: z.strictObject({
    version: z.number().int().positive(),
    capabilities: z.array(name),
  }),
  providerSend: z.null(),
  providerClose: z.null(),
  sessionFrame: z.null(),
  sessionClose: z.null(),
} satisfies Record<RemotePluginAction["action"], z.ZodType>;

const resultAction = z.enum(Object.keys(RemotePluginValueSchemas) as RemotePluginAction["action"][]);

// Keep result validation keyed to the originating action, including at the Central boundary.
export const RemotePluginResultSchema = z.union([
  z.strictObject({
    action: resultAction,
    error: z.string(),
  }),
  z.strictObject({
    action: resultAction,
    value: z.unknown(),
  }).superRefine(({ action, value }, context) => {
    if (!RemotePluginValueSchemas[action].safeParse(value).success) {
      context.addIssue({
        code: "custom",
        path: ["value"],
        message: "Invalid remote plugin result",
      });
    }
  }),
]);
export type RemotePluginResult = z.infer<typeof RemotePluginResultSchema>;
export type RemotePluginValue<K extends RemotePluginAction["action"]> = z.infer<
  (typeof RemotePluginValueSchemas)[K]
>;
export type RemotePluginRef = z.infer<typeof plugin>;
export type RemotePluginFrame = z.infer<typeof frame>;

export const RemotePluginEventSchema = z.strictObject({
  type: z.literal("plugin"),
  event: z.discriminatedUnion("type", [
    z.strictObject({ type: z.literal("catalogChanged"), plugin }),
    z.strictObject({ type: z.literal("settingsChanged"), plugin, settingsId: name }),
    z.strictObject({ type: z.literal("providersChanged") }),
    z.strictObject({ type: z.literal("usageReport"), queryId: id, report: UsageReportEntrySchema }),
    z.strictObject({ type: z.literal("subscriptionEvent"), subscriptionId: id, value: rpcPayload }),
    z.strictObject({
      type: z.literal("subscriptionClosed"),
      subscriptionId: id,
      error: z.string().optional(),
    }),
    z.strictObject({
      type: z.literal("providerEvent"),
      connectionId: id,
      value: ProviderEventSchema,
    }),
    z.strictObject({ type: z.literal("sessionOpened"), sessionId: id, plugin }),
    z.strictObject({ type: z.literal("sessionFrame"), sessionId: id, frame }),
    z.strictObject({ type: z.literal("sessionClosed"), sessionId: id }),
  ]),
});
export type RemotePluginEvent = z.infer<typeof RemotePluginEventSchema>;

export function encodeRemotePluginFrame(data: string | Uint8Array): RemotePluginFrame {
  return typeof data === "string"
    ? { encoding: "text", data }
    : { encoding: "binary", data: Buffer.from(data).toString("base64") };
}

export function decodeRemotePluginFrame(frame: RemotePluginFrame): [string | Uint8Array, boolean] {
  return frame.encoding === "text"
    ? [frame.data, false]
    : [Buffer.from(frame.data, "base64"), true];
}

export type RemotePluginPayload = z.infer<typeof rpcPayload>;

export function encodeRemotePluginPayload(value: unknown): RemotePluginPayload {
  assertPluginWireValue(value, "remote plugin payload");
  if (value instanceof Uint8Array)
    return { encoding: "binary", data: Buffer.from(value).toString("base64") };
  const encoded = JSON.stringify(value);
  if (encoded === undefined) return { encoding: "undefined" };
  return rpcPayload.parse({ encoding: "json", data: JSON.parse(encoded) });
}

export function decodeRemotePluginPayload(payload: RemotePluginPayload): unknown {
  if (payload.encoding === "undefined") return undefined;
  if (payload.encoding === "json") return payload.data;
  const bytes = Buffer.from(payload.data, "base64");
  assertPluginWireValue(bytes, "remote plugin payload");
  return bytes;
}
