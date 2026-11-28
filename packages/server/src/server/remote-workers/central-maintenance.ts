import type { Logger } from "pino";
import { z } from "zod";
import {
  AgentRefreshedStatusPayloadSchema,
  FetchAgentRequestMessageSchema,
  FetchAgentsRequestMessageSchema,
  ListProviderModelsRequestMessageSchema,
  RefreshAgentRequestMessageSchema,
  RefreshProvidersSnapshotRequestMessageSchema,
  type DaemonPermission,
  type SessionOutboundMessage,
} from "@getpaseo/protocol/messages";
import { ProviderCatalogSession } from "../session/provider/provider-catalog-session.js";
import type { openCentralAgents } from "./central-agents.js";
import type { createCentralDirectory } from "./central-directory.js";
import { WorkerOperationError } from "./execution-protocol.js";
import { WorkerGrantSchema, type WorkerGrant } from "./leases.js";

const provider = z.enum(["codex", "omp"]);
const cwd = z.string().trim().min(1);
export const WorkerMaintenanceOperationSchema = z.discriminatedUnion("type", [
  FetchAgentsRequestMessageSchema.extend({
    subscribe: z.never().optional(),
    sync: z.never().optional(),
  }),
  FetchAgentRequestMessageSchema,
  RefreshAgentRequestMessageSchema,
  RefreshProvidersSnapshotRequestMessageSchema.extend({ cwd, providers: z.tuple([provider]) }),
  ListProviderModelsRequestMessageSchema.extend({ cwd, provider }),
]);
export type WorkerMaintenanceOperation = z.infer<typeof WorkerMaintenanceOperationSchema>;

const permissions = {
  fetch_agents_request: "workspace.read",
  fetch_agent_request: "workspace.read",
  refresh_agent_request: "workspace.write",
  refresh_providers_snapshot_request: "daemon.read",
  list_provider_models_request: "daemon.read",
} as const satisfies Record<WorkerMaintenanceOperation["type"], DaemonPermission>;
const allowedPermissions = new Set<DaemonPermission>(Object.values(permissions));

export function parseMaintenance(grant: WorkerGrant, operation: WorkerMaintenanceOperation) {
  const parsedOperation = WorkerMaintenanceOperationSchema.safeParse(operation);
  const parsedGrant = WorkerGrantSchema.safeParse(grant);
  if (!parsedOperation.success || !parsedGrant.success) {
    throw new WorkerOperationError("worker_maintenance_invalid_request");
  }
  const input = parsedOperation.data;
  const verified = parsedGrant.data;
  if (
    verified.scope === "owner" ||
    !verified.scope.includes(permissions[input.type]) ||
    !verified.scope.every((permission) => allowedPermissions.has(permission))
  ) {
    throw new WorkerOperationError("worker_maintenance_permission_denied");
  }
  return { grant: verified, operation: input };
}

type Scope = Awaited<ReturnType<typeof openCentralAgents>> & {
  directory: ReturnType<typeof createCentralDirectory>;
};
interface AgentRefreshResponse {
  type: "status";
  payload: z.infer<typeof AgentRefreshedStatusPayloadSchema>;
}
type FetchAgentResponse = Extract<SessionOutboundMessage, { type: "fetch_agent_response" }>;
type ProviderResponse = Extract<
  SessionOutboundMessage,
  { type: "refresh_providers_snapshot_response" | "list_provider_models_response" }
>;
type WorkerMaintenanceResponse =
  | AgentRefreshResponse
  | FetchAgentResponse
  | ProviderResponse
  | Extract<SessionOutboundMessage, { type: "fetch_agents_response" }>;

export interface WorkerMaintenanceResult {
  fetch_agents_request: Extract<
    WorkerMaintenanceResponse,
    { type: "fetch_agents_response" }
  >["payload"];
  fetch_agent_request: FetchAgentResponse["payload"];
  refresh_agent_request: AgentRefreshResponse["payload"];
  refresh_providers_snapshot_request: Extract<
    ProviderResponse,
    { type: "refresh_providers_snapshot_response" }
  >["payload"];
  list_provider_models_request: Extract<
    ProviderResponse,
    { type: "list_provider_models_response" }
  >["payload"];
}

export async function readWorkerAgent(
  scope: Scope,
  { agentId, requestId }: z.infer<typeof FetchAgentRequestMessageSchema>,
): Promise<FetchAgentResponse> {
  const agent = await scope.readSnapshot(agentId);
  const project = agent?.workspaceId
    ? await scope.directory.projectForWorkspace(agent.workspaceId)
    : null;
  return { type: "fetch_agent_response", payload: { requestId, agent, project, error: null } };
}

export async function refreshWorkerAgent(
  scope: Scope,
  { agentId, requestId }: z.infer<typeof RefreshAgentRequestMessageSchema>,
  signal: AbortSignal,
): Promise<AgentRefreshResponse> {
  const timelineSize = await scope.refreshAgent(agentId, signal);
  return {
    type: "status",
    payload: { status: "agent_refreshed", agentId, requestId, timelineSize },
  };
}

export async function dispatchMaintenance(
  scope: Scope,
  operation: WorkerMaintenanceOperation,
  signal: AbortSignal,
  logger: Logger,
): Promise<WorkerMaintenanceResponse> {
  switch (operation.type) {
    case "fetch_agents_request": {
      const response = await scope.directory.read(operation);
      if (response.type !== "fetch_agents_response")
        throw new Error("unexpected_directory_response");
      return response;
    }
    case "fetch_agent_request":
      return readWorkerAgent(scope, operation);
    case "refresh_agent_request":
      try {
        return await refreshWorkerAgent(scope, operation, signal);
      } catch (error) {
        signal.throwIfAborted();
        logger.warn(
          { err: error, agentId: operation.agentId },
          "Worker maintenance refresh failed",
        );
        throw new WorkerOperationError("agent_refresh_failed");
      }
    case "refresh_providers_snapshot_request":
    case "list_provider_models_request": {
      let response: ProviderResponse | undefined;
      const catalog = new ProviderCatalogSession({
        logger,
        providerSnapshotManager: scope.catalog,
        host: {
          emit(message) {
            if (
              message.type === "refresh_providers_snapshot_response" ||
              message.type === "list_provider_models_response"
            ) {
              response = message;
            }
          },
          resolveCwd: (value) => value,
          isProviderVisibleToClient: () => true,
          supportsCustomModeIcons: () => false,
          supportsCompactProviderSnapshots: () => false,
          supportsProviderSnapshotReferences: () => false,
          listProviderAvailability: () => scope.manager.listProviderAvailability(),
          listDraftControls: (config) => scope.manager.listDraftControls(config),
          publishSnapshot() {},
        },
      });
      await catalog.dispatch(operation);
      if (!response) throw new Error("missing_provider_catalog_response");
      return response;
    }
  }
}
