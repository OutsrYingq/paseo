// Runnable role entry for isolated fixtures and embedding environments supplying a local JS adapter.
// The host application owns credentials, CP protocol and listener addresses.
import { pathToFileURL } from "node:url";
import { startRemoteCentral, startResidentExecution } from "./index.js";

const [role, modulePath] = process.argv.slice(2);
if ((role !== "central" && role !== "resident") || !modulePath) {
  throw new Error("Usage: node remote-entry.js <central|resident> <absolute adapter module path>");
}
const { default: config } = await import(pathToFileURL(modulePath).href);
const handle =
  role === "central" ? await startRemoteCentral(config) : await startResidentExecution(config);
console.log(`${role}_ready ${handle.port}`);
let closing = false;
function close() {
  if (closing) return;
  closing = true;
  void handle.close().catch((error: unknown) => {
    console.error(error);
    process.exitCode = 1;
  });
}
process.on("SIGTERM", close);
process.on("SIGINT", close);
