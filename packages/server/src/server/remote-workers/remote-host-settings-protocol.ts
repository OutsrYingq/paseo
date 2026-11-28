import {
  AgentProfileSchema,
  DaemonConfigReloadResponseSchema,
  DaemonGetStatusResponseSchema,
  DiagnosticsResponseSchema,
  TerminalProfileSchema,
} from "@getpaseo/protocol/messages";
import { z } from "zod";
import { AgentMetadataGenerationSchema } from "../persisted-config.js";
import { SpeechSettingsPatchSchema, SpeechSettingsSchema } from "@getpaseo/protocol/speech/rpc-schemas";

// These are the composer controls implemented by the remote native providers.
const profileControls = z.object({
  fast_mode: z.boolean().nullable().optional(),
  plan_mode: z.boolean().nullable().optional(),
  service_tier: z.string().nullable().optional(),
});
export const remoteHostProfileControlKeys = Object.keys(profileControls.shape);
const agentProfile = AgentProfileSchema.strip();
const terminalProfile = TerminalProfileSchema.strip();
const usage = z.strictObject({ enabled: z.boolean() });
const browserTools = z.strictObject({ enabled: z.boolean() });
const browserSettings = z.literal(true).optional();
const speechSettings = z.literal(true).optional();

export const RemoteHostConfigPatchSchema = z.strictObject({
  agentProfiles: z
    .array(
      agentProfile
        .extend({
          options: profileControls.strict().optional(),
          featureValues: profileControls.strict().optional(),
        })
        .strict(),
    )
    .optional(),
  terminalProfiles: z.array(terminalProfile.strict()).optional(),
  usage: usage.optional(),
  metadataGeneration: AgentMetadataGenerationSchema.optional(),
  browserTools: browserTools.optional(),
  features: SpeechSettingsPatchSchema.optional(),
});

export const RemoteHostConfigSchema = z.strictObject({
  mcp: z.strictObject({ injectIntoAgents: z.literal(false) }),
  agentProfiles: z.array(
    agentProfile.extend({
      options: profileControls.optional(),
      featureValues: profileControls.optional(),
    }),
  ),
  terminalProfiles: z.array(terminalProfile).optional(),
  pluginsEnabled: z.boolean().optional(),
  browserTools,
  providers: z.strictObject({}),
  metadataGeneration: AgentMetadataGenerationSchema.required({ providers: true }),
  usage: usage.optional(),
  features: SpeechSettingsSchema.optional(),
  autoArchiveAfterMerge: z.literal(false),
  enableTerminalAgentHooks: z.literal(false),
  appendSystemPrompt: z.literal(""),
});

export type RemoteHostConfig = z.output<typeof RemoteHostConfigSchema>;

export const RemoteHostSettingsOperationSchema = z.discriminatedUnion("kind", [
  z.strictObject({ kind: z.literal("hostSettingsGet"), browserSettings, speechSettings }),
  z.strictObject({ kind: z.literal("hostSettingsPatch"), config: RemoteHostConfigPatchSchema, browserSettings, speechSettings }),
  z.strictObject({ kind: z.literal("hostSettingsReload"), browserSettings, speechSettings }),
  z.strictObject({ kind: z.literal("hostStatus"), includeProviders: z.boolean().optional() }),
  z.strictObject({ kind: z.literal("hostDiagnostics") }),
]);

export const remoteHostSettingsResults = {
  hostSettingsGet: RemoteHostConfigSchema,
  hostSettingsPatch: RemoteHostConfigSchema,
  hostSettingsReload: DaemonConfigReloadResponseSchema.shape.payload
    .omit({ requestId: true })
    .extend({ config: RemoteHostConfigSchema })
    .strict(),
  hostStatus: DaemonGetStatusResponseSchema.shape.payload
    .pick({
      version: true,
      pid: true,
      nodePath: true,
      startedAt: true,
      providers: true,
    })
    .strict(),
  hostDiagnostics: DiagnosticsResponseSchema.shape.payload.pick({ diagnostic: true }).strict(),
};

export type RemoteHostSettingsOperation = z.infer<typeof RemoteHostSettingsOperationSchema>;
export type RemoteHostSettingsCall = (operation: RemoteHostSettingsOperation) => Promise<unknown>;
