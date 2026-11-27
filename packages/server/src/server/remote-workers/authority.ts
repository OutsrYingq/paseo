import type { IncomingMessage } from "node:http";

/** A DNS authority, optionally with the HTTP listener port. Never use forwarded headers. */
export function parseWorkerAuthority(value: string): string | undefined {
  const [name, port, ...rest] = value.split(":");
  if (
    !name ||
    rest.length ||
    (port !== undefined && (!/^\d{1,5}$/.test(port) || Number(port) < 1 || Number(port) > 65535))
  ) {
    return undefined;
  }
  const hostname = name.toLowerCase().replace(/\.$/, "");
  if (
    hostname.length > 253 ||
    !hostname.split(".").every((label) => /^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/.test(label))
  ) {
    return undefined;
  }
  return hostname;
}

export function workerHost(request: Pick<IncomingMessage, "rawHeaders">): string | undefined {
  let host: string | undefined;
  for (let index = 0; index < request.rawHeaders.length; index += 2) {
    if (request.rawHeaders[index]?.toLowerCase() !== "host") continue;
    if (host !== undefined) return undefined;
    host = request.rawHeaders[index + 1] ?? "";
  }
  return host === undefined ? undefined : parseWorkerAuthority(host);
}
