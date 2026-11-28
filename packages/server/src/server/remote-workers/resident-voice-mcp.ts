import { randomBytes } from "node:crypto";
import { createServer } from "node:http";
import { toNodeHandler } from "@modelcontextprotocol/node";
import { createMcpHandler, McpServer } from "@modelcontextprotocol/server";
import express from "express";
import { z } from "zod/v4";
import type { VoiceSpeakHandler } from "../voice-types.js";

export function createResidentVoiceMcp() {
  const bindings = new Map<string, { agentId: string; speak: VoiceSpeakHandler }>();
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
      res.locals.voiceBinding = binding;
      next();
    },
    express.json({ limit: "32kb" }),
    async (req, res) => {
      const binding: { agentId: string; speak: VoiceSpeakHandler } = res.locals.voiceBinding;
      const mcp = new McpServer({ name: "agent-mcp", version: "2.0.0" });
      mcp.registerTool(
        "speak",
        {
          title: "Speak",
          description:
            "Speak text to the user via daemon-managed voice output. Blocks until playback completes.",
          inputSchema: z.object({ text: z.string().trim().min(1).max(4000) }),
        },
        async ({ text }, context) => {
          await binding.speak({
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
      const token = randomBytes(32).toString("base64url");
      bindings.set(token, { agentId, speak });
      return {
        descriptor: { url: `http://127.0.0.1:${port()}${path}`, token },
        revoke: () => bindings.delete(token),
      };
    },
    async close() {
      bindings.clear();
      await ready?.catch(() => undefined);
      if (!server.listening) return;
      const closed = new Promise<void>((resolve) => server.close(() => resolve()));
      server.closeAllConnections();
      await closed;
    },
  };
}
