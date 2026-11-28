import { randomUUID } from "node:crypto";
import { once } from "node:events";
import type { ServerResponse } from "node:http";
import { formatAttachmentContentDisposition } from "../file-download/content-disposition.js";
import type { WorkerLease } from "./leases.js";
import type { WorkerConnection } from "./worker-connection.js";

interface Download {
  connection: WorkerConnection;
  lease: WorkerLease;
  cwd: string;
  path: string;
  fileName: string;
  mimeType: string;
  size: number;
  expiresAt: number;
}

export class RemoteDownloads {
  private readonly tokens = new Map<string, Download>();

  async issue(connection: WorkerConnection, lease: WorkerLease, cwd: string, path: string) {
    const info = await connection.call({ kind: "downloadInfo", cwd, path });
    const now = Date.now();
    for (const [token, entry] of this.tokens)
      if (entry.expiresAt <= now || entry.lease.signal.aborted) this.tokens.delete(token);
    if (lease.signal.aborted || connection.signal.aborted || this.tokens.size >= 4096)
      throw new Error("worker_download_unavailable");
    const token = randomUUID();
    this.tokens.set(token, {
      connection,
      lease,
      cwd,
      path,
      ...info,
      expiresAt: now + 60_000,
    });
    return { token, ...info };
  }

  async consume(hostname: string, token: string, response: ServerResponse): Promise<boolean> {
    const entry = this.tokens.get(token);
    if (
      !entry ||
      entry.lease.grant.hostname !== hostname ||
      entry.expiresAt <= Date.now() ||
      entry.lease.signal.aborted ||
      entry.connection.signal.aborted
    )
      return false;
    this.tokens.delete(token);
    const request = new AbortController();
    const stop = () => request.abort();
    response.once("close", stop);
    const signal = AbortSignal.any([request.signal, entry.lease.signal, entry.connection.signal]);
    let id: string | undefined;
    try {
      const opened = await entry.connection.call({
        kind: "downloadOpen",
        cwd: entry.cwd,
        path: entry.path,
      });
      id = opened.id;
      signal.throwIfAborted();
      response.writeHead(200, {
        "content-type": entry.mimeType,
        "content-disposition": formatAttachmentContentDisposition(entry.fileName),
        "content-length": opened.size,
        "cache-control": "no-store",
      });
      for (let offset = 0; offset < opened.size; ) {
        const bytes = await entry.connection.call({
          kind: "downloadRead",
          id,
          offset,
        });
        signal.throwIfAborted();
        const chunk = Buffer.from(bytes, "base64").subarray(0, opened.size - offset);
        if (chunk.length === 0) throw new Error("worker_download_truncated");
        offset += chunk.length;
        if (!response.write(chunk)) await once(response, "drain", { signal });
      }
      response.end();
    } catch {
      if (!response.headersSent && !response.destroyed) response.writeHead(404).end();
      else response.destroy();
    } finally {
      response.removeListener("close", stop);
      if (id) void entry.connection.call({ kind: "downloadClose", id }).catch(() => undefined);
    }
    return true;
  }
}
