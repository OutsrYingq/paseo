import { randomBytes } from "node:crypto";
import { createServer } from "node:http";
import { toNodeHandler } from "@modelcontextprotocol/node";
import { createMcpHandler, McpServer } from "@modelcontextprotocol/server";
import express from "express";
import { z } from "zod/v4";
import type { VoiceSpeakHandler } from "../voice-types.js";

type AgentBinding = {
  agentId: string;
  token: string;
  speak?: VoiceSpeakHandler;
  registerBrowser?: (server: McpServer) => void;
};

export interface ResidentMcpBinding {
  descriptor: { url: string; token: string };
  revoke(): boolean;
}

export interface ResidentAgentMcp {
  start(): Promise<void>;
  register(agentId: string, speak: VoiceSpeakHandler): ResidentMcpBinding;
  registerBrowser(agentId: string, register: NonNullable<AgentBinding["registerBrowser"]>): ResidentMcpBinding;
  close(): Promise<void>;
}

export function createResidentVoiceMcp(): ResidentAgentMcp {
  const agents = new Map<string, AgentBinding>();
  const bindings = new Map<string, AgentBinding>();
  const app = express();
  const server = createServer(app);
  let ready: Promise<void> | undefined;
  const path = "/mcp/voice";

  app.post(
    path,
    (req, res, next) => {
      const authorization = req.get("authorization");
      const binding = authorization?.startsWith("Bearer ") && bindings.get(authorization.slice(7));
      if (!binding || req.get("host") !== `127.0.0.1:${port()}`) {
        res.sendStatus(401);
        return;
      }
      res.locals.agentBinding = binding;
      next();
    },
    express.json({ limit: "32kb" }),
    async (req, res) => {
      const binding: AgentBinding = res.locals.agentBinding;
      const mcp = new McpServer({ name: "agent-mcp", version: "2.0.0" });
      const speak = binding.speak;
      if (speak) mcp.registerTool(
        "speak",
        {
          title: "Speak",
          description:
            "Speak text to the user via daemon-managed voice output. Blocks until playback completes.",
          inputSchema: z.object({ text: z.string().trim().min(1).max(4000) }),
        },
        async ({ text }, context) => {
          await speak({
            text,
            callerAgentId: binding.agentId,
            signal: context.mcpReq?.signal,
          });
          return {
            content: [{ type: "text", text: '{"ok":true}' }],
            structuredContent: { ok: true },
          };
        },
      );
      binding.registerBrowser?.(mcp);
      const handler = createMcpHandler(() => mcp, { legacy: "stateless" });
      try {
        await toNodeHandler(handler)(req, res, req.body);
      } catch {
        if (!res.headersSent) res.sendStatus(500);
        else res.destroy();
      } finally {
        await handler.close();
      }
    },
  );
  app.all(path, (_req, res) => res.sendStatus(405));

  function port(): number {
    const address = server.address();
    if (!address || typeof address === "string") throw new Error("voice_mcp_unavailable");
    return address.port;
  }

  function bind(agentId: string) {
    let binding = agents.get(agentId);
    if (!binding) {
      binding = { agentId, token: randomBytes(32).toString("base64url") };
      agents.set(agentId, binding);
      bindings.set(binding.token, binding);
    }
    return binding;
  }

  function release(binding: AgentBinding): boolean {
    if (binding.speak || binding.registerBrowser || agents.get(binding.agentId) !== binding)
      return false;
    agents.delete(binding.agentId);
    return bindings.delete(binding.token);
  }

  function descriptor(binding: AgentBinding) {
    return { url: `http://127.0.0.1:${port()}${path}`, token: binding.token };
  }

  return {
    start(): Promise<void> {
      return (ready ??= new Promise((resolve, reject) => {
        server.once("error", reject);
        server.listen(0, "127.0.0.1", () => {
          server.off("error", reject);
          resolve();
        });
      }));
    },
    register(agentId: string, speak: VoiceSpeakHandler) {
      const binding = bind(agentId);
      binding.speak = speak;
      return {
        descriptor: descriptor(binding),
        revoke() {
          if (binding.speak !== speak) return false;
          binding.speak = undefined;
          return release(binding);
        },
      };
    },
    registerBrowser(agentId: string, register: NonNullable<AgentBinding["registerBrowser"]>) {
      const binding = bind(agentId);
      binding.registerBrowser = register;
      return {
        descriptor: descriptor(binding),
        revoke() {
          if (binding.registerBrowser !== register) return false;
          binding.registerBrowser = undefined;
          return release(binding);
        },
      };
    },
    async close() {
      agents.clear();
      bindings.clear();
      await ready?.catch(() => undefined);
      if (!server.listening) return;
      const closed = new Promise<void>((resolve) => server.close(() => resolve()));
      server.closeAllConnections();
      await closed;
    },
  };
}
