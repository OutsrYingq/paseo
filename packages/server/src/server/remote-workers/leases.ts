import { z } from "zod";
import { DaemonPermissionSchema } from "@getpaseo/protocol/messages";
import { parseWorkerAuthority } from "./authority.js";

export const WorkerBindingSchema = z.strictObject({
  workerId: z.string().min(1),
  podUid: z.string().min(1),
  serverId: z.string().min(1),
});

export const WorkerTargetSchema = WorkerBindingSchema.extend({
  revision: z.number().int().nonnegative().optional(),
  endpoint: z.url({ protocol: /^wss$/ }),
  certFingerprint256: z.string().regex(/^(?:[0-9A-F]{2}:){31}[0-9A-F]{2}$/),
});

export const WorkerGrantSchema = z.strictObject({
  target: WorkerTargetSchema,
  hostname: z.string().refine((host) => parseWorkerAuthority(host) === host),
  leaseId: z.string().min(1),
  principalId: z.string().min(1),
  scope: z.union([z.literal("owner"), z.array(DaemonPermissionSchema)]),
  expiresAt: z.number().int().positive(),
});

export type WorkerBinding = z.infer<typeof WorkerBindingSchema>;
export type WorkerTarget = z.infer<typeof WorkerTargetSchema>;
export type WorkerGrant = z.infer<typeof WorkerGrantSchema>;
export type WorkerCredential =
  | { kind: "password"; password: string }
  | { kind: "capability"; token: string }
  | { kind: "none" };
export type WorkerAdmission =
  | { kind: "allowed"; grant: WorkerGrant }
  | { kind: "unknown_host" | "unauthorized" | "unavailable" };

/** Authenticates the CP response and verifies the original credential for the requested host. */
export interface WorkerAuthority {
  admit(
    hostname: string,
    credential: WorkerCredential,
    signal: AbortSignal,
  ): Promise<WorkerAdmission>;
  renew(
    grant: WorkerGrant,
    signal: AbortSignal,
  ): Promise<{ kind: "allowed"; grant: WorkerGrant } | { kind: "revoked" | "target_revoked" }>;
}

function sameScope(left: WorkerGrant["scope"], right: WorkerGrant["scope"]): boolean {
  if (left === "owner" || right === "owner") return left === right;
  const granted = new Set(left);
  const renewed = new Set(right);
  return granted.size === renewed.size && left.every((permission) => renewed.has(permission));
}

async function callAuthority<T>(
  parent: AbortSignal,
  timeoutMs: number,
  request: (signal: AbortSignal) => Promise<T>,
): Promise<T> {
  const deadline = new AbortController();
  const signal = AbortSignal.any([parent, deadline.signal]);
  signal.throwIfAborted();
  const timer = setTimeout(() => deadline.abort(), timeoutMs);
  timer.unref();
  let onAbort: (() => void) | undefined;
  const aborted = new Promise<never>((_, reject) => {
    onAbort = () => reject(signal.reason);
    signal.addEventListener("abort", onAbort, { once: true });
  });
  try {
    return await Promise.race([
      aborted,
      Promise.resolve().then(() => {
        signal.throwIfAborted();
        return request(signal);
      }),
    ]);
  } finally {
    clearTimeout(timer);
    if (onAbort) signal.removeEventListener("abort", onAbort);
  }
}

export function sameWorkerBinding(a: WorkerBinding, b: WorkerBinding): boolean {
  return a.workerId === b.workerId && a.podUid === b.podUid && a.serverId === b.serverId;
}

function sameTarget(a: WorkerTarget, b: WorkerTarget): boolean {
  return (
    sameWorkerBinding(a, b) &&
    a.endpoint === b.endpoint &&
    a.certFingerprint256 === b.certFingerprint256
  );
}

export interface ActiveWorkerTarget {
  readonly target: WorkerTarget;
  readonly signal: AbortSignal;
}

class ActiveTarget implements ActiveWorkerTarget {
  readonly abort = new AbortController();
  readonly signal = this.abort.signal;
  readonly sessions = new Map<string, ActiveGrant>();
  constructor(readonly target: WorkerTarget) {}
}

export interface WorkerLease {
  readonly grant: WorkerGrant;
  readonly execution: ActiveWorkerTarget;
  readonly signal: AbortSignal;
}

class ActiveGrant implements WorkerLease {
  readonly abort = new AbortController();
  readonly signal = this.abort.signal;
  timer?: NodeJS.Timeout;
  constructor(
    public grant: WorkerGrant,
    readonly execution: ActiveTarget,
  ) {}
}

interface KnownTarget {
  target: WorkerTarget;
  active?: ActiveTarget;
  revokedPods: Set<string>;
}

export class WorkerLeases {
  private readonly targets = new Map<string, KnownTarget>();
  private readonly shutdown = new AbortController();
  private readonly admissionByHost = new Map<string, Promise<unknown>>();

  constructor(private readonly authority: WorkerAuthority) {}

  admit(hostname: string, credential: WorkerCredential) {
    const previous = this.admissionByHost.get(hostname) ?? Promise.resolve();
    const admitted = previous.then(() => this.admitCurrent(hostname, credential));
    this.admissionByHost.set(hostname, admitted);
    void admitted
      .finally(() => {
        if (this.admissionByHost.get(hostname) === admitted) this.admissionByHost.delete(hostname);
      })
      .catch(() => undefined);
    return admitted;
  }

  private async admitCurrent(
    hostname: string,
    credential: WorkerCredential,
  ): Promise<
    | { kind: "allowed"; lease: WorkerLease }
    | { kind: "unknown_host" | "unauthorized" | "unavailable" }
  > {
    try {
      const result = await callAuthority(this.shutdown.signal, 5_000, (signal) =>
        this.authority.admit(hostname, credential, signal),
      );
      if (this.shutdown.signal.aborted) return { kind: "unavailable" };
      if (result.kind !== "allowed") return result;
      const grant = WorkerGrantSchema.parse(result.grant);
      if (grant.hostname !== hostname || grant.expiresAt <= Date.now())
        return { kind: "unavailable" };
      const { target } = grant;
      let known = this.targets.get(target.workerId);
      if (!known) {
        known = { target, revokedPods: new Set() };
        this.targets.set(target.workerId, known);
      }
      if (known.revokedPods.has(target.podUid) || known.target.serverId !== target.serverId) {
        return { kind: "unavailable" };
      }
      if (known.target.podUid !== target.podUid) {
        known.revokedPods.add(known.target.podUid);
        if (known.active) this.dropTarget(known.active);
        known.target = target;
      } else if (!sameTarget(known.target, target)) {
        return { kind: "unavailable" };
      }
      const execution = (known.active ??= new ActiveTarget(target));
      const existing = execution.sessions.get(grant.leaseId);
      if (existing) {
        if (
          existing.grant.hostname !== hostname ||
          existing.grant.principalId !== grant.principalId ||
          !sameScope(existing.grant.scope, grant.scope)
        ) {
          return { kind: "unavailable" };
        }
        if (grant.expiresAt > existing.grant.expiresAt) existing.grant = grant;
        this.schedule(existing);
        return { kind: "allowed", lease: existing };
      }
      const lease = new ActiveGrant(grant, execution);
      execution.sessions.set(grant.leaseId, lease);
      this.schedule(lease);
      return { kind: "allowed", lease };
    } catch {
      return { kind: "unavailable" };
    }
  }

  close(): void {
    this.shutdown.abort();
    for (const known of this.targets.values()) if (known.active) this.dropTarget(known.active);
    this.targets.clear();
    this.admissionByHost.clear();
  }

  private schedule(lease: ActiveGrant): void {
    clearTimeout(lease.timer);
    const remaining = lease.grant.expiresAt - Date.now();
    if (remaining <= 0) {
      this.dropLease(lease);
      return;
    }
    const delay = Math.min(2_147_483_647, Math.max(1, Math.floor(remaining / 2)));
    lease.timer = setTimeout(() => void this.renew(lease), delay);
    lease.timer.unref();
  }

  private async renew(lease: ActiveGrant): Promise<void> {
    const remaining = lease.grant.expiresAt - Date.now();
    if (remaining <= 0) {
      this.dropLease(lease);
      return;
    }
    const previous = lease.grant;
    try {
      const result = await callAuthority(lease.signal, Math.min(remaining, 5_000), (signal) =>
        this.authority.renew(previous, signal),
      );
      if (lease.signal.aborted) return;
      if (lease.grant.expiresAt <= Date.now()) {
        this.dropLease(lease);
        return;
      }
      if (result.kind === "target_revoked") {
        const known = this.targets.get(previous.target.workerId);
        if (known?.active === lease.execution) known.revokedPods.add(previous.target.podUid);
        this.dropTarget(lease.execution);
        return;
      }
      if (result.kind !== "allowed") {
        this.dropLease(lease);
        return;
      }
      const grant = WorkerGrantSchema.parse(result.grant);
      if (
        !sameTarget(previous.target, grant.target) ||
        previous.hostname !== grant.hostname ||
        previous.leaseId !== grant.leaseId ||
        previous.principalId !== grant.principalId ||
        !sameScope(previous.scope, grant.scope)
      ) {
        this.dropLease(lease);
        return;
      }
      if (grant.expiresAt > lease.grant.expiresAt) lease.grant = grant;
    } catch {
      // Failed renewal leaves the last verified expiry in force.
    } finally {
      if (!lease.signal.aborted) this.schedule(lease);
    }
  }

  private dropLease(lease: ActiveGrant): void {
    clearTimeout(lease.timer);
    lease.abort.abort();
    const { execution } = lease;
    execution.sessions.delete(lease.grant.leaseId);
    if (execution.sessions.size === 0) this.dropTarget(execution);
  }

  private dropTarget(execution: ActiveTarget): void {
    execution.abort.abort();
    for (const lease of execution.sessions.values()) {
      clearTimeout(lease.timer);
      lease.abort.abort();
    }
    execution.sessions.clear();
    const known = this.targets.get(execution.target.workerId);
    if (known?.active === execution) known.active = undefined;
  }
}
