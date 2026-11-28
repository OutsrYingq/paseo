import type { SessionInboundMessage, SessionOutboundMessage } from "@getpaseo/protocol/messages";
import { WorkerOperationError } from "./execution-protocol.js";
import {
  RemoteHostConfigPatchSchema,
  remoteHostSettingsResults,
  type RemoteHostConfig,
  type RemoteHostSettingsCall,
} from "./remote-host-settings-protocol.js";

export function createCentralHostSettings({
  call,
  serverId,
  onConfigChanged,
}: {
  call: RemoteHostSettingsCall;
  serverId: string;
  onConfigChanged?(event: {
    type: "status";
    payload: { status: "daemon_config_changed"; config: RemoteHostConfig };
  }): void;
}) {
  function publishConfig(config: RemoteHostConfig): void {
    onConfigChanged?.({ type: "status", payload: { status: "daemon_config_changed", config } });
  }

  return function dispatch(
    request: SessionInboundMessage,
  ): Promise<SessionOutboundMessage> | undefined {
    switch (request.type) {
      case "get_daemon_config_request":
        return call({ kind: "hostSettingsGet" }).then((result) => ({
          type: "get_daemon_config_response",
          payload: {
            requestId: request.requestId,
            config: remoteHostSettingsResults.hostSettingsGet.parse(result),
          },
        }));
      case "set_daemon_config_request": {
        const patch = RemoteHostConfigPatchSchema.safeParse(request.config);
        if (!patch.success) {
          return Promise.reject(new WorkerOperationError("worker_host_settings_patch_unsupported"));
        }
        return call({ kind: "hostSettingsPatch", config: patch.data }).then((result) => {
          const config = remoteHostSettingsResults.hostSettingsPatch.parse(result);
          if (Object.keys(patch.data).length) publishConfig(config);
          return {
            type: "set_daemon_config_response",
            payload: { requestId: request.requestId, config },
          };
        });
      }
      case "daemon.config.reload.request":
        return call({ kind: "hostSettingsReload" }).then((response) => {
          const { config, ...result } =
            remoteHostSettingsResults.hostSettingsReload.parse(response);
          if (result.appliedPaths.length) publishConfig(config);
          return {
            type: "daemon.config.reload.response",
            payload: { requestId: request.requestId, ...result },
          };
        });
      case "daemon.get_status.request":
        return call({ kind: "hostStatus", includeProviders: request.includeProviders }).then(
          (result) => ({
            type: "daemon.get_status.response",
            payload: {
              ...remoteHostSettingsResults.hostStatus.parse(result),
              requestId: request.requestId,
              serverId,
              listen: null,
              relay: null,
            },
          }),
        );
      case "diagnostics.request":
        return call({ kind: "hostDiagnostics" }).then((result) => ({
          type: "diagnostics.response",
          payload: {
            requestId: request.requestId,
            ...remoteHostSettingsResults.hostDiagnostics.parse(result),
          },
        }));
      default:
        return undefined;
    }
  };
}
