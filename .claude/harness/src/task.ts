// The shared task board. One JSON file per task, so two agents working two tasks never
// write the same file — the board itself cannot become a contention point.
import * as fs from "node:fs";
import * as path from "node:path";
import { loadConfig } from "./config.ts";
import type { HarnessConfig } from "./config.ts";
import { agentName, getAgent, isProcessAlive, registerAgent } from "./identity.ts";
import type { Lease } from "./lease.ts";
import { acquireScopes, leasesForAgent, listLeases, releaseLeases, reapStaleLeases } from "./lease.ts";
import { ensureDirs, stateDir, tasksDir } from "./paths.ts";
import { normalizeScope } from "./scope.ts";
import { appendJournal, listRecords, nowIso, readJson, updateJson, withFileLock } from "./store.ts";
import type { Versioned } from "./store.ts";

export const TASK_STATUSES = [
  "backlog",
  "ready",
  "in_progress",
  "blocked",
  "review",
  "done",
  "abandoned",
] as const;

export type TaskStatus = (typeof TASK_STATUSES)[number];

/** Statuses that mean nobody should still be holding the task. */
const TERMINAL: TaskStatus[] = ["done", "abandoned"];

export type CheckpointKind = "created" | "claim" | "note" | "block" | "drop" | "reclaim" | "done";

export interface Checkpoint {
  at: string;
  by: string;
  kind: CheckpointKind;
  /** What actually happened. */
  did: string;
  /** The single most useful field on the board: where a replacement agent picks up. */
  next: string | null;
}

export interface TaskOwner {
  agentId: string;
  agentName: string;
  pid: number | null;
  since: string;
}

export interface Task extends Versioned {
  id: string;
  title: string;
  status: TaskStatus;
  /** 1 is most urgent. */
  priority: number;
  scopes: string[];
  dependsOn: string[];
  acceptance: string[];
  verified: string[];
  owner: TaskOwner | null;
  checkpoints: Checkpoint[];
  /** Times this task has been reclaimed from a vanished agent. */
  attempts: number;
  notes: string;
  createdAt: string;
  updatedAt: string;
}

export interface TaskOutcome {
  ok: boolean;
  task: Task | null;
  reason?: string;
  conflicts?: Lease[];
}

function taskFile(id: string): string {
  return path.join(tasksDir(), `${id}.json`);
}

export function listTasks(): Task[] {
  return listRecords<Task>(tasksDir()).sort((a, b) => a.id.localeCompare(b.id));
}

export function getTask(id: string): Task | null {
  return readJson<Task>(taskFile(id.toUpperCase()));
}

/** Monotonic ids under a lock, so two agents adding tasks at once cannot collide. */
function allocateId(): string {
  ensureDirs();
  const counter = path.join(stateDir(), "counter.json");
  return withFileLock(counter, () => {
    const current = readJson<{ next: number }>(counter);
    const next = current === null || !Number.isInteger(current.next) ? 1 : current.next;
    fs.writeFileSync(counter, `${JSON.stringify({ next: next + 1 }, null, 2)}\n`, "utf8");
    return `T-${String(next).padStart(4, "0")}`;
  });
}

function checkpoint(kind: CheckpointKind, by: string, did: string, next: string | null): Checkpoint {
  return { at: nowIso(), by, kind, did, next };
}

/** The most recent explicit "next step", which is what a resuming agent needs. */
export function lastNext(task: Task): string | null {
  for (let i = task.checkpoints.length - 1; i >= 0; i -= 1) {
    const entry = task.checkpoints[i];
    if (entry !== undefined && entry.next !== null && entry.next.length > 0) return entry.next;
  }
  return null;
}

export interface CreateTaskInput {
  title: string;
  scopes?: string[];
  dependsOn?: string[];
  acceptance?: string[];
  priority?: number;
  notes?: string;
  status?: TaskStatus;
  agentId?: string;
}

export function createTask(input: CreateTaskInput): Task {
  const config = loadConfig();
  const at = nowIso();
  const id = allocateId();
  const by = input.agentId ?? "unknown";
  const acceptance = input.acceptance !== undefined && input.acceptance.length > 0
    ? input.acceptance
    : config.defaultAcceptance;

  const task: Task = {
    version: 0,
    id,
    title: input.title,
    status: input.status ?? "ready",
    priority: input.priority ?? 2,
    scopes: [...new Set((input.scopes ?? []).map(normalizeScope))],
    dependsOn: (input.dependsOn ?? []).map((dep) => dep.toUpperCase()),
    acceptance: [...acceptance],
    verified: [],
    owner: null,
    checkpoints: [checkpoint("created", by, `created "${input.title}"`, null)],
    attempts: 0,
    notes: input.notes ?? "",
    createdAt: at,
    updatedAt: at,
  };

  const written = updateJson<Task>(taskFile(id), () => task);
  if (written === null) throw new Error(`failed to write task ${id}`);
  appendJournal({ event: "task.created", taskId: id, title: input.title, agentId: by });
  return written;
}

function mutateTask(id: string, mutate: (task: Task) => Task): Task {
  const updated = updateJson<Task>(taskFile(id), (current) => {
    if (current === null) throw new Error(`no such task: ${id}`);
    return mutate(current);
  });
  if (updated === null) throw new Error(`failed to update task ${id}`);
  return updated;
}

/** Has the agent that owns this task stopped showing up? */
function isAgentStale(agentId: string, config: HarnessConfig, now: number): boolean {
  const record = getAgent(agentId);
  if (record === null) return true;
  if (!isProcessAlive(record.pid)) return true;
  const age = now - Date.parse(record.heartbeatAt);
  return Number.isFinite(age) && age > config.leaseTtlMs;
}

function unfinishedDependencies(task: Task): string[] {
  return task.dependsOn.filter((dep) => {
    const other = getTask(dep);
    return other === null || other.status !== "done";
  });
}

/**
 * Takes ownership of a task and locks its scopes. Refuses — rather than stealing — when
 * a live agent already holds it, and refuses when another agent's scopes overlap.
 */
export function claimTask(id: string, agentId: string, options: { force?: boolean } = {}): TaskOutcome {
  const config = loadConfig();
  registerAgent(agentId);
  // Clear anything a dead session left behind before deciding this is a conflict.
  reapStaleLeases();
  reapOrphanedTasks();

  const taskId = id.toUpperCase();
  const task = getTask(taskId);
  if (task === null) return { ok: false, task: null, reason: `no such task: ${taskId}` };
  if (TERMINAL.includes(task.status)) {
    return { ok: false, task, reason: `task ${taskId} is already ${task.status}` };
  }

  const now = Date.now();
  if (
    task.owner !== null &&
    task.owner.agentId !== agentId &&
    !isAgentStale(task.owner.agentId, config, now) &&
    options.force !== true
  ) {
    return {
      ok: false,
      task,
      reason: `task ${taskId} is held by ${task.owner.agentName} (${task.owner.agentId}). ` +
        "Pick another task, or use --force only if you know that agent is gone.",
    };
  }

  const blocking = unfinishedDependencies(task);
  if (blocking.length > 0 && options.force !== true) {
    return { ok: false, task, reason: `blocked by unfinished dependencies: ${blocking.join(", ")}` };
  }

  if (task.scopes.length > 0) {
    const acquisition = acquireScopes(task.scopes, { agentId, taskId });
    if (!acquisition.ok) {
      const held = acquisition.conflicts
        .map((lease) => `${lease.scope} (held by ${lease.agentName} for ${lease.taskId ?? "no task"})`)
        .join(", ");
      return {
        ok: false,
        task,
        conflicts: acquisition.conflicts,
        reason: `scope conflict: ${held}`,
      };
    }
  }

  try {
    const updated = mutateTask(taskId, (current) => {
      // Re-check inside the record lock. Scope leases arbitrate the common case, but a
      // task that declares no scopes has no lease to lose the race on — this does.
      if (
        current.owner !== null &&
        current.owner.agentId !== agentId &&
        !isAgentStale(current.owner.agentId, config, Date.now()) &&
        options.force !== true
      ) {
        throw new Error(`task ${taskId} was claimed by ${current.owner.agentName} a moment ago`);
      }
      return {
        ...current,
        status: "in_progress",
        owner: {
          agentId,
          agentName: agentName(agentId),
          pid: getAgent(agentId)?.pid ?? null,
          since: nowIso(),
        },
        updatedAt: nowIso(),
        checkpoints: [
          ...current.checkpoints,
          checkpoint("claim", agentId, `claimed by ${agentName(agentId)}`, lastNext(current)),
        ],
      };
    });
    appendJournal({ event: "task.claimed", taskId, agentId });
    return { ok: true, task: updated };
  } catch (err) {
    // Never leave scopes locked for a claim that did not land.
    releaseLeases({ agentId, taskId });
    return { ok: false, task, reason: err instanceof Error ? err.message : String(err) };
  }
}

function requireOwnership(task: Task, agentId: string): string | null {
  if (task.owner === null) return `task ${task.id} has no owner — claim it first`;
  if (task.owner.agentId !== agentId) {
    return `task ${task.id} is owned by ${task.owner.agentName}, not you`;
  }
  return null;
}

/**
 * Records progress and refreshes the lease. This is the crash-resilience primitive:
 * whatever is written here is what a replacement agent gets to start from.
 */
export function noteTask(id: string, agentId: string, did: string, next: string | null): TaskOutcome {
  const taskId = id.toUpperCase();
  const task = getTask(taskId);
  if (task === null) return { ok: false, task: null, reason: `no such task: ${taskId}` };
  const ownershipError = requireOwnership(task, agentId);
  if (ownershipError !== null) return { ok: false, task, reason: ownershipError };

  registerAgent(agentId);
  // Self-heal: an idle stretch can expire a lease while the agent is still alive.
  if (task.scopes.length > 0) {
    const held = new Set(leasesForAgent(agentId).map((lease) => lease.scope));
    const missing = task.scopes.filter((scope) => !held.has(scope));
    if (missing.length > 0) acquireScopes(missing, { agentId, taskId });
  }

  const updated = mutateTask(taskId, (current) => ({
    ...current,
    updatedAt: nowIso(),
    checkpoints: [...current.checkpoints, checkpoint("note", agentId, did, next)],
  }));
  appendJournal({ event: "task.note", taskId, agentId, did, next });
  return { ok: true, task: updated };
}

function releaseAndSet(
  id: string,
  agentId: string,
  status: TaskStatus,
  kind: CheckpointKind,
  did: string,
  next: string | null,
  extra: Partial<Task> = {},
): TaskOutcome {
  const taskId = id.toUpperCase();
  const task = getTask(taskId);
  if (task === null) return { ok: false, task: null, reason: `no such task: ${taskId}` };
  const ownershipError = requireOwnership(task, agentId);
  if (ownershipError !== null) return { ok: false, task, reason: ownershipError };

  releaseLeases({ agentId, taskId });
  const updated = mutateTask(taskId, (current) => ({
    ...current,
    ...extra,
    status,
    owner: null,
    updatedAt: nowIso(),
    checkpoints: [...current.checkpoints, checkpoint(kind, agentId, did, next)],
  }));
  appendJournal({ event: `task.${kind}`, taskId, agentId, status });
  return { ok: true, task: updated };
}

export function blockTask(id: string, agentId: string, reason: string): TaskOutcome {
  return releaseAndSet(id, agentId, "blocked", "block", `blocked: ${reason}`, reason);
}

export function dropTask(id: string, agentId: string, why: string, next: string | null): TaskOutcome {
  return releaseAndSet(id, agentId, "ready", "drop", `handed back: ${why}`, next);
}

export function finishTask(id: string, agentId: string, verified: string[]): TaskOutcome {
  const summary = verified.length > 0 ? `verified with: ${verified.join("; ")}` : "completed";
  return releaseAndSet(id, agentId, "done", "done", summary, null, { verified });
}

export function unblockTask(id: string, agentId: string): TaskOutcome {
  const taskId = id.toUpperCase();
  const task = getTask(taskId);
  if (task === null) return { ok: false, task: null, reason: `no such task: ${taskId}` };
  const updated = mutateTask(taskId, (current) => ({
    ...current,
    status: "ready",
    owner: null,
    updatedAt: nowIso(),
    checkpoints: [...current.checkpoints, checkpoint("note", agentId, "unblocked", lastNext(current))],
  }));
  appendJournal({ event: "task.unblocked", taskId, agentId });
  return { ok: true, task: updated };
}

export function removeTask(id: string): boolean {
  const taskId = id.toUpperCase();
  try {
    fs.unlinkSync(taskFile(taskId));
    appendJournal({ event: "task.removed", taskId });
    return true;
  } catch {
    return false;
  }
}

export interface ReclaimedTask {
  task: Task;
  parked: boolean;
}

/**
 * Returns tasks whose owner vanished. This is the whole point of the harness: a session
 * that dies mid-task hands the work back with its last checkpoint intact, instead of
 * leaving it wedged in `in_progress` forever.
 */
export function reapOrphanedTasks(options: { dryRun?: boolean } = {}): ReclaimedTask[] {
  const config = loadConfig();
  const now = Date.now();
  const reclaimed: ReclaimedTask[] = [];

  for (const task of listTasks()) {
    if (task.status !== "in_progress" || task.owner === null) continue;
    if (!isAgentStale(task.owner.agentId, config, now)) continue;

    const attempts = task.attempts + 1;
    const parked = attempts >= config.maxAttempts;
    reclaimed.push({ task, parked });
    if (options.dryRun === true) continue;

    const carriedNext = lastNext(task);
    const previousOwner = task.owner.agentName;
    releaseLeases({ agentId: task.owner.agentId, taskId: task.id });
    mutateTask(task.id, (current) => ({
      ...current,
      status: parked ? "blocked" : "ready",
      owner: null,
      attempts,
      updatedAt: nowIso(),
      checkpoints: [
        ...current.checkpoints,
        checkpoint(
          "reclaim",
          "harness",
          parked
            ? `reclaimed from ${previousOwner} after ${attempts} attempts — parked for a human`
            : `reclaimed from ${previousOwner}: that session stopped reporting in`,
          carriedNext,
        ),
      ],
    }));
    appendJournal({ event: "task.reclaimed", taskId: task.id, previousOwner: task.owner.agentId, parked });
  }

  return reclaimed;
}

export interface ReapReport {
  leases: Lease[];
  tasks: ReclaimedTask[];
}

/** Full recovery sweep. Runs on every SessionStart, so a fresh session heals the board. */
export function reap(options: { dryRun?: boolean } = {}): ReapReport {
  const leases = reapStaleLeases(options);
  const tasks = reapOrphanedTasks(options);
  return { leases, tasks };
}

export function tasksForAgent(agentId: string): Task[] {
  return listTasks().filter((task) => task.owner !== null && task.owner.agentId === agentId);
}

export function allLeases(): Lease[] {
  return listLeases();
}
