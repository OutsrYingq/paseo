import { describe, expect, it, vi } from "vitest";
import { workerHost } from "./authority.js";
import { NativeTurnOwnership } from "./native-turn-ownership.js";
import { WorkerLeases, type WorkerAuthority, type WorkerGrant } from "./leases.js";

function grant(podUid: string, leaseId: string, lifetime = 100): WorkerGrant {
  return {
    hostname: "worker.example",
    leaseId,
    scope: "owner",
    principalId: leaseId,
    expiresAt: Date.now() + lifetime,
    target: {
      workerId: "worker",
      serverId: "srv_preserved",
      podUid,
      revision: 5,
      endpoint: "wss://localhost/execute",
      certFingerprint256: new Array(32).fill("AA").join(":"),
    },
  };
}

function allowed(value: WorkerGrant) {
  return { kind: "allowed", grant: value } as const;
}

const hostname = "worker.example";
const credential = { kind: "password", password: "synthetic" } as const;

describe("remote worker authority", () => {
  it("canonicalizes one raw Host and ignores forwarded authorities", () => {
    expect(
      workerHost({
        rawHeaders: ["Host", "WORKER.example.:443", "X-Forwarded-Host", "other.example"],
      }),
    ).toBe(hostname);
    expect(workerHost({ rawHeaders: ["Host", hostname, "hOsT", hostname] })).toBeUndefined();
    expect(workerHost({ rawHeaders: ["Host", "worker.example:0"] })).toBeUndefined();
    expect(workerHost({ rawHeaders: ["Host", "worker.example,other.example"] })).toBeUndefined();
  });

  it("accepts a new Pod at the same CP revision and invalidates the old Pod", async () => {
    let current = grant("old", "old");
    const authority: WorkerAuthority = {
      admit: async () => allowed(current),
      renew: async () => allowed(current),
    };
    const registry = new WorkerLeases(authority);
    try {
      const original = await registry.admit(hostname, credential);
      if (original.kind !== "allowed") throw new Error("initial admission failed");
      current = grant("new", "new");
      const replacement = await registry.admit(hostname, credential);
      expect(replacement.kind).toBe("allowed");
      expect(original.lease.signal.aborted).toBe(true);
      expect(original.lease.execution.signal.aborted).toBe(true);
      current = grant("old", "stale");
      expect((await registry.admit(hostname, credential)).kind).toBe("unavailable");
    } finally {
      registry.close();
    }
  });

  it("revokes individual leases and permits fresh auth after expiry on the same Pod", async () => {
    vi.useFakeTimers();
    let current = grant("same", "a");
    const authority: WorkerAuthority = {
      admit: async () => allowed(current),
      renew: async (previous) =>
        previous.leaseId === "a" ? { kind: "revoked" } : allowed(previous),
    };
    const registry = new WorkerLeases(authority);
    try {
      const a = await registry.admit(hostname, credential);
      current = grant("same", "b", 200);
      const b = await registry.admit(hostname, credential);
      if (a.kind !== "allowed" || b.kind !== "allowed") throw new Error("initial admission failed");
      expect(a.lease.execution).toBe(b.lease.execution);
      await vi.advanceTimersByTimeAsync(80);
      expect(a.lease.signal.aborted).toBe(true);
      expect(b.lease.signal.aborted).toBe(false);
      await vi.advanceTimersByTimeAsync(125);
      expect(b.lease.signal.aborted).toBe(true);
      current = grant("same", "c");
      expect((await registry.admit(hostname, credential)).kind).toBe("allowed");
    } finally {
      registry.close();
      vi.useRealTimers();
    }
  });

  it("expires a lease when the authority ignores the renewal deadline", async () => {
    vi.useFakeTimers();
    const lateRenewal = Promise.withResolvers<ReturnType<typeof allowed>>();
    let current = grant("same", "a");
    const registry = new WorkerLeases({
      admit: async () => allowed(current),
      renew: () => lateRenewal.promise,
    });
    try {
      const admission = await registry.admit(hostname, credential);
      if (admission.kind !== "allowed") throw new Error("initial admission failed");
      await vi.advanceTimersByTimeAsync(100);
      expect(admission.lease.signal.aborted).toBe(true);
      expect(admission.lease.execution.signal.aborted).toBe(true);

      lateRenewal.resolve(allowed(grant("same", "a")));
      current = grant("same", "b");
      const fresh = await registry.admit(hostname, credential);
      if (fresh.kind !== "allowed") throw new Error("fresh admission failed");
      expect(fresh.lease.signal.aborted).toBe(false);
      expect(admission.lease.signal.aborted).toBe(true);
    } finally {
      registry.close();
      vi.useRealTimers();
    }
  });

  it("releases Host admission after a stalled authority call and discards its late result", async () => {
    vi.useFakeTimers();
    const delayedAdmission = Promise.withResolvers<ReturnType<typeof allowed>>();
    const admit = vi
      .fn<WorkerAuthority["admit"]>()
      .mockImplementationOnce(() => delayedAdmission.promise)
      .mockImplementationOnce(async () => allowed(grant("current", "a")));
    const registry = new WorkerLeases({
      admit,
      renew: async () => ({ kind: "revoked" }),
    });
    try {
      const stalled = registry.admit(hostname, credential);
      const queued = registry.admit(hostname, credential);
      await vi.advanceTimersByTimeAsync(5_000);
      expect(await stalled).toEqual({ kind: "unavailable" });
      const current = await queued;
      if (current.kind !== "allowed") throw new Error("current admission failed");
      delayedAdmission.resolve(allowed(grant("stale", "b")));
      await delayedAdmission.promise;
      expect(current.lease.signal.aborted).toBe(false);
      expect(current.lease.execution.target.podUid).toBe("current");
    } finally {
      registry.close();
      vi.useRealTimers();
    }
  });

  it("fences a renewal that broadens a grant containing duplicate permissions", async () => {
    vi.useFakeTimers();
    const original: WorkerGrant = {
      ...grant("same", "a"),
      scope: ["workspace.read", "workspace.read"],
    };
    const registry = new WorkerLeases({
      admit: async () => allowed(original),
      renew: async () =>
        allowed({
          ...original,
          expiresAt: Date.now() + 100,
          scope: ["workspace.read", "workspace.write"],
        }),
    });
    try {
      const admission = await registry.admit(hostname, credential);
      if (admission.kind !== "allowed") throw new Error("initial admission failed");
      await vi.advanceTimersByTimeAsync(50);
      expect(admission.lease.signal.aborted).toBe(true);
    } finally {
      registry.close();
      vi.useRealTimers();
    }
  });

  it("holds ownership when a turn submission was sent but the result is unknown", () => {
    const native = new NativeTurnOwnership("codex");
    native.input.write('{"id":12,"method":"turn/start"}\n');
    expect(native.mayBeRunning).toBe(true);
    native.output.write('{"id":12,"result":{"turn":{"id":"t"}}}\n');
    expect(native.mayBeRunning).toBe(true);
    native.output.write('{"method":"turn/completed","params":{"turn":{"id":"t"}}}\n');
    expect(native.mayBeRunning).toBe(false);
  });
});
