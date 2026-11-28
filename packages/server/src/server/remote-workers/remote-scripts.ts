import type {
  SessionInboundMessage,
  SessionOutboundMessage,
  WorkspaceScriptPayload,
} from "@getpaseo/protocol/messages";
import type { WorkerConnection } from "./worker-connection.js";

export type RemoteScriptRequest = Extract<
  SessionInboundMessage,
  {
    type:
      | "workspace.script.list.request"
      | "workspace.script.start.request"
      | "workspace.script.stop.request"
      | "start_workspace_script_request";
  }
>;

export function createRemoteScripts(connection: WorkerConnection) {
  const list = (workspaceId: string) => connection.call({ kind: "scriptList", workspaceId });
  return {
    list,
    async dispatch(request: RemoteScriptRequest): Promise<SessionOutboundMessage> {
      const { requestId, workspaceId } = request;
      const base = { requestId, workspaceId };
      let error: string | null = null;
      if (request.type === "workspace.script.list.request") {
        let scripts: WorkspaceScriptPayload[] = [];
        try {
          scripts = await list(workspaceId);
        } catch (err) {
          error = err instanceof Error ? err.message : String(err);
        }
        return { type: "workspace.script.list.response", payload: { ...base, scripts, error } };
      }

      const { scriptName } = request;
      const kind = request.type === "workspace.script.stop.request" ? "scriptStop" : "scriptStart";
      let script: WorkspaceScriptPayload | null = null;
      try {
        script = await connection.call({ kind, workspaceId, scriptName });
      } catch (err) {
        error = err instanceof Error ? err.message : String(err);
      }
      if (request.type === "start_workspace_script_request") {
        return {
          type: "start_workspace_script_response",
          payload: { ...base, scriptName, terminalId: script?.terminalId ?? null, error },
        };
      }
      return {
        type: kind === "scriptStop" ? "workspace.script.stop.response" : "workspace.script.start.response",
        payload: { ...base, scriptName, script, error },
      };
    },
  };
}
