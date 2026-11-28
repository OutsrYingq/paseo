import { randomUUID } from "node:crypto";
import { mkdir, mkdtemp, readdir, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, it } from "vitest";
import { remoteFileTransferResults } from "./remote-file-transfer-protocol.js";
import { openResidentFileTransfer } from "./resident-file-transfer.js";

it("publishes only completed uploads, cancels partial bytes, and resolves the token under its owner on reconnect", async () => {
  const home = await mkdtemp(join(tmpdir(), "paseo-transfer-"));
  const options = { paseoHome: home, roots: [] };
  const resident = openResidentFileTransfer(options);
  const owner = "a".repeat(64);
  const contents = Buffer.from([0, 187, 10, 255]);
  const metadata = { mime: "application/octet-stream", size: contents.length, encoding: "binary", modifiedAt: "" } as const;
  async function begin(id: string) {
    await resident.execute({
      kind: "uploadOpen", id, owner,
      request: { type: "file.upload.request", requestId: "public", fileName: "attachment.bin", mimeType: metadata.mime, size: metadata.size, modifiedAt: "" },
    });
    await resident.execute({ kind: "uploadBegin", id, metadata });
  }
  try {
    const cancelled = randomUUID();
    await begin(cancelled);
    await resident.execute({ kind: "uploadChunk", id: cancelled, bytes: contents.subarray(0, 1).toString("base64") });
    await resident.execute({ kind: "uploadCancel", id: cancelled });
    expect(await readdir(join(home, "remote-attachments", owner, "uploads"))).toEqual([]);
    await expect(resident.execute({ kind: "uploadEnd", id: cancelled })).rejects.toThrow("worker_upload_unavailable");

    const complete = randomUUID();
    await begin(complete);
    await resident.execute({ kind: "uploadChunk", id: complete, bytes: contents.toString("base64") });
    const result = await resident.execute({ kind: "uploadEnd", id: complete });
    const file = remoteFileTransferResults.uploadEnd.parse(result);
    expect(file).toMatchObject({ fileName: "attachment.bin", size: contents.length });
    expect(await readFile(file.path)).toEqual(contents);

    await begin(randomUUID());
    await resident.close();
    expect(await readdir(join(home, "remote-attachments", owner, "uploads"))).toEqual([file.id]);
    const reopened = openResidentFileTransfer(options);
    try {
      const resolve = { kind: "attachmentResolve", id: file.id, owner, expectedPath: file.path } as const;
      await expect(reopened.execute({ ...resolve, owner: "b".repeat(64) })).rejects.toThrow("worker_attachment_unavailable");
      await expect(reopened.execute({ ...resolve, expectedPath: join(home, "other.bin") })).rejects.toThrow("worker_attachment_unavailable");
      await expect(reopened.execute(resolve)).resolves.toEqual(file);
    } finally {
      await reopened.close();
    }
  } finally {
    await resident.close();
    await rm(home, { recursive: true, force: true });
  }
});

it("streams bytes and revision from a resident file while enforcing workspace scope and maxBytes", async () => {
  const home = await mkdtemp(join(tmpdir(), "paseo-explorer-transfer-"));
  const cwd = join(home, "root");
  const resident = openResidentFileTransfer({ paseoHome: home, roots: [cwd] });
  try {
    await mkdir(cwd);
    const contents = Buffer.alloc(640 * 1024, 187);
    await writeFile(join(cwd, "image.png"), contents);
    const id = randomUUID();
    const open = { kind: "explorerStreamOpen", id, cwd, path: "image.png" } as const;
    await expect(resident.execute({ ...open, cwd: home })).rejects.toThrow("worker_workspace_root_denied");
    await expect(resident.execute({ ...open, maxBytes: contents.length - 1 })).rejects.toThrow("File is too large to display");
    await expect(resident.execute({ ...open, maxBytes: contents.length })).resolves.toMatchObject({ mime: "image/png", encoding: "binary", size: contents.length, revision: expect.any(String) });
    const chunks: Buffer[] = [];
    for (;;) {
      const bytes = remoteFileTransferResults.explorerStreamNext.parse(await resident.execute({ kind: "explorerStreamNext", id }));
      if (bytes === null) break;
      chunks.push(Buffer.from(bytes, "base64"));
    }
    expect(chunks.length).toBeGreaterThan(1);
    expect(Buffer.concat(chunks)).toEqual(contents);
  } finally {
    await resident.close();
    await rm(home, { recursive: true, force: true });
  }
});
