// Leases are the actual collision guard. An agent holds a lease on every scope its
// task touches; another agent's edit into that scope is refused while it is live.
//
// A lease that stops being heart-beaten — because the session crashed, the window was
// closed, or the machine went down — becomes reclaimable, which is what stops crashed
// work from parking a scope forever.
import * as fs from "node:fs";
import * as path from "node:path";
import { loadConfig } from "./config.ts";
import type { HarnessConfig } from "./config.ts";
import { agentName, isProcessAlive, resolvePid } from "./identity.ts";
import { ensureDirs, leasesDir, toRepoRelative } from "./paths.ts";
import { leaseKey, matchesPath, normalizeScope, scopesOverlap } from "./scope.ts";
import {
  appendJournal,
  listRecords,
  nowIso,
  readJson,
  withFileLock,
  writeJsonAtomic,
} from "./store.ts";
import type { Versioned } from "./store.ts";

export interface Lease extends Versioned {
  key: string;
  scope: string;
  taskId: string | null;
  agentId: string;
  agentName: string;
  pid: number | null;
  acquiredAt: string;
  heartbeatAt: string;
  ttlMs: number;
}

export interface AcquireResult {
  ok: boolean;
  acquired: Lease[];
  conflicts: Lease[];
}

function leaseFile(key: string): string {
  return path.join(leasesDir(), `${key}.json`);
}

/** Serializes acquisition so two agents cannot both pass the conflict check. */
function withLeaseLock<T>(fn: () => T): T {
  ensureDirs();
  return withFileLock(path.join(leasesDir(), "_leases"), fn);
}

export function listLeases(): Lease[] {
  return listRecords<Lease>(leasesDir());
}

/**
 * A lease is reclaimable once its heartbeat ages past the TTL, or as soon as the owning
 * Claude Code process is provably gone. The pid check is an accelerator, never the sole
 * criterion — an unknown pid falls back to the TTL.
 */
export function isLeaseStale(lease: Lease, config: HarnessConfig, now: number): boolean {
  const ttl = lease.ttlMs > 0 ? lease.ttlMs : config.leaseTtlMs;
  const age = now - Date.parse(lease.heartbeatAt);
  if (Number.isFinite(age) && age > ttl) return true;
  return !isProcessAlive(lease.pid);
}

function deleteLease(lease: Lease): void {
  try {
    fs.unlinkSync(leaseFile(lease.key));
  } catch {
    // Somebody released or reaped it first; the desired end state already holds.
  }
}

/** Drops every lease nobody is keeping alive any more. Returns what was reclaimed. */
export function reapStaleLeases(options: { dryRun?: boolean } = {}): Lease[] {
  const config = loadConfig();
  return withLeaseLock(() => {
    const now = Date.now();
    const reclaimed: Lease[] = [];
    for (const lease of listLeases()) {
      if (!isLeaseStale(lease, config, now)) continue;
      reclaimed.push(lease);
      if (options.dryRun !== true) {
        deleteLease(lease);
        appendJournal({
          event: "lease.reaped",
          scope: lease.scope,
          taskId: lease.taskId,
          previousOwner: lease.agentId,
          heartbeatAt: lease.heartbeatAt,
        });
      }
    }
    return reclaimed;
  });
}

/** Live leases held by somebody other than `agentId`, after clearing dead ones. */
function liveForeignLeases(agentId: string, config: HarnessConfig, now: number): Lease[] {
  return listLeases().filter(
    (lease) => lease.agentId !== agentId && !isLeaseStale(lease, config, now),
  );
}

/**
 * All-or-nothing acquisition across a task's scopes: if any scope collides, nothing is
 * taken, so a refused claim never leaves half a task locked.
 */
export function acquireScopes(
  scopes: string[],
  owner: { agentId: string; taskId: string | null; ttlMs?: number },
): AcquireResult {
  const config = loadConfig();
  const normalized = [...new Set(scopes.map(normalizeScope))];

  return withLeaseLock(() => {
    const now = Date.now();
    // Clear dead leases first so a crashed agent never blocks a live one.
    for (const lease of listLeases()) {
      if (isLeaseStale(lease, config, now)) deleteLease(lease);
    }

    const foreign = liveForeignLeases(owner.agentId, config, now);
    const conflicts: Lease[] = [];
    for (const scope of normalized) {
      for (const lease of foreign) {
        if (scopesOverlap(scope, lease.scope)) conflicts.push(lease);
      }
    }
    if (conflicts.length > 0) return { ok: false, acquired: [], conflicts };

    const at = nowIso();
    const name = agentName(owner.agentId);
    const pid = resolvePid();
    const acquired: Lease[] = [];
    for (const scope of normalized) {
      const key = leaseKey(scope);
      const existing = readJson<Lease>(leaseFile(key));
      const lease: Lease = {
        version: existing === null ? 1 : existing.version + 1,
        key,
        scope,
        taskId: owner.taskId,
        agentId: owner.agentId,
        agentName: name,
        pid,
        acquiredAt: existing?.acquiredAt ?? at,
        heartbeatAt: at,
        ttlMs: owner.ttlMs ?? config.leaseTtlMs,
      };
      writeJsonAtomic(leaseFile(key), lease);
      acquired.push(lease);
      appendJournal({ event: "lease.acquired", scope, taskId: owner.taskId, agentId: owner.agentId });
    }
    return { ok: true, acquired, conflicts: [] };
  });
}

export interface ReleaseFilter {
  agentId: string;
  taskId?: string;
  scope?: string;
}

/** Releases the caller's own leases. An agent can never release somebody else's. */
export function releaseLeases(filter: ReleaseFilter): Lease[] {
  return withLeaseLock(() => {
    const released: Lease[] = [];
    for (const lease of listLeases()) {
      if (lease.agentId !== filter.agentId) continue;
      if (filter.taskId !== undefined && lease.taskId !== filter.taskId) continue;
      if (filter.scope !== undefined && normalizeScope(filter.scope) !== lease.scope) continue;
      deleteLease(lease);
      released.push(lease);
      appendJournal({
        event: "lease.released",
        scope: lease.scope,
        taskId: lease.taskId,
        agentId: lease.agentId,
      });
    }
    return released;
  });
}

/** Keeps this agent's leases alive. Called from PostToolUse, so it must stay cheap. */
export function heartbeatLeases(agentId: string): number {
  return withLeaseLock(() => {
    const at = nowIso();
    let touched = 0;
    for (const lease of listLeases()) {
      if (lease.agentId !== agentId) continue;
      writeJsonAtomic(leaseFile(lease.key), { ...lease, version: lease.version + 1, heartbeatAt: at });
      touched += 1;
    }
    return touched;
  });
}

/** Live leases held by other agents that cover `filePath`. Empty means the edit is fine. */
export function conflictsForPath(filePath: string, agentId: string | null): Lease[] {
  const config = loadConfig();
  const now = Date.now();
  const target = toRepoRelative(filePath);
  return listLeases().filter((lease) => {
    if (agentId !== null && lease.agentId === agentId) return false;
    if (isLeaseStale(lease, config, now)) return false;
    return matchesPath(lease.scope, target);
  });
}

export function leasesForAgent(agentId: string): Lease[] {
  return listLeases().filter((lease) => lease.agentId === agentId);
}
