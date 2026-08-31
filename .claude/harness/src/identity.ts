// Who is this agent? Claude Code exports `CLAUDE_CODE_SESSION_ID` and `CLAUDE_PID`
// into every tool subprocess, so identity resolves on its own and agents never have
// to remember to pass a flag.
import * as path from "node:path";
import { agentsDir, ensureDirs } from "./paths.ts";
import { errorCode, listRecords, nowIso, readJson, updateJson } from "./store.ts";
import type { Versioned } from "./store.ts";

export interface AgentRecord extends Versioned {
  agentId: string;
  /** Short human-facing label, e.g. `storage-worker`. Defaults to the short id. */
  name: string;
  /** The Claude Code process. Used to spot a session that died without cleaning up. */
  pid: number | null;
  startedAt: string;
  heartbeatAt: string;
}

/**
 * Resolution order: explicit flag, `HARNESS_AGENT_ID`, then the session id Claude Code
 * exports. Returns null when none is available — mutating commands refuse rather than
 * inventing an unstable id that two sessions could collide on.
 */
export function resolveAgentId(explicit?: string): string | null {
  const candidates = [explicit, process.env.HARNESS_AGENT_ID, process.env.CLAUDE_CODE_SESSION_ID];
  for (const candidate of candidates) {
    if (typeof candidate === "string" && candidate.trim().length > 0) return candidate.trim();
  }
  return null;
}

export function requireAgentId(explicit?: string): string {
  const agentId = resolveAgentId(explicit);
  if (agentId === null) {
    throw new Error(
      "cannot tell which agent this is. Pass --agent <id>, or set HARNESS_AGENT_ID. " +
        "Inside Claude Code this normally comes from CLAUDE_CODE_SESSION_ID automatically.",
    );
  }
  return agentId;
}

/** Compact display form — full session ids are UUIDs and swamp a terminal table. */
export function shortId(agentId: string): string {
  return agentId.length <= 8 ? agentId : agentId.slice(0, 8);
}

export function resolvePid(): number | null {
  const raw = process.env.CLAUDE_PID;
  if (raw === undefined) return null;
  const pid = Number.parseInt(raw, 10);
  return Number.isInteger(pid) && pid > 0 ? pid : null;
}

/**
 * Liveness probe. An unknown pid counts as alive so the TTL stays the only thing that
 * can reclaim work — guessing "dead" would let one agent yank another's lease.
 */
export function isProcessAlive(pid: number | null): boolean {
  if (pid === null || !Number.isInteger(pid) || pid <= 0) return true;
  try {
    process.kill(pid, 0);
    return true;
  } catch (err) {
    // EPERM means the process exists but belongs to somebody else.
    return errorCode(err) === "EPERM";
  }
}

function agentFile(agentId: string): string {
  return path.join(agentsDir(), `${encodeURIComponent(agentId)}.json`);
}

export function getAgent(agentId: string): AgentRecord | null {
  return readJson<AgentRecord>(agentFile(agentId));
}

export function listAgents(): AgentRecord[] {
  return listRecords<AgentRecord>(agentsDir());
}

/** Records the agent and refreshes its heartbeat. Safe to call on every hook. */
export function registerAgent(agentId: string, name?: string): AgentRecord {
  ensureDirs();
  const at = nowIso();
  const pid = resolvePid();
  const updated = updateJson<AgentRecord>(agentFile(agentId), (current) => ({
    version: 0,
    agentId,
    name: name ?? current?.name ?? shortId(agentId),
    pid: pid ?? current?.pid ?? null,
    startedAt: current?.startedAt ?? at,
    heartbeatAt: at,
  }));
  if (updated === null) throw new Error(`failed to register agent ${agentId}`);
  return updated;
}

export function agentName(agentId: string): string {
  return getAgent(agentId)?.name ?? shortId(agentId);
}
