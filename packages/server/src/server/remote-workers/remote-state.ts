import type { AgentStorageFiles } from "../agent/agent-storage.js";
import type { CreationFiles } from "../creation/index.js";
import type { MessageReceiptFiles } from "../message-receipts/index.js";
import type { WorkerConnection } from "./worker-connection.js";

export function createRemoteState(connection: WorkerConnection) {
  const normalize = (file: string) => file.replaceAll("\\", "/");
  const read = (file: string) => connection.call({ kind: "readState", path: normalize(file) });
  const write = async (file: string, text: string, exclusive = false) => {
    await connection.call({
      kind: "writeState",
      path: normalize(file),
      text,
      exclusive,
    });
  };
  async function remove(file: string) {
    await connection.call({ kind: "deleteState", path: normalize(file) });
  }
  const agents: AgentStorageFiles = {
    async list(directory) {
      const entries = await connection.call({
        kind: "listState",
        path: normalize(directory),
      });
      return entries.map((entry) => ({
        name: entry.name,
        isFile: () => entry.kind === "file",
        isDirectory: () => entry.kind === "directory",
      }));
    },
    async read(file) {
      const text = await read(file);
      if (text === null)
        throw Object.assign(new Error("worker_state_missing"), {
          code: "ENOENT",
        });
      return text;
    },
    write: (file, record) => write(file, JSON.stringify(record, null, 2)),
    unlink: remove,
    // State uses the worker's POSIX volume in remote mode.
    async isSameFile(left, right) {
      return left === right;
    },
  };
  const creation: CreationFiles = {
    prepare: async () => {},
    read,
    write,
    claim: (file, identity) => write(file, identity, true),
  };
  const receipts: MessageReceiptFiles = {
    read,
    write: (file, value) => write(file, JSON.stringify(value, null, 2)),
  };
  return { agents, creation, receipts, read, write, remove };
}
