// Claude Code hook entrypoints.
//
// Every one of these fails open. A harness bug, a corrupt state file, or a half-written
// record must never wedge somebody's editing session — the worst acceptable outcome is
// that coordination silently stops working, not that work stops.
import * as fs from "node:fs";
import { loadConfig } from "./config.ts";
import { registerAgent, resolveAgentId, shortId } from "./identity.ts";
import { conflictsForPath, heartbeatLeases, leasesForAgent, releaseLeases } from "./lease.ts";
import { toRepoRelative } from "./paths.ts";
import { matchesPath } from "./scope.ts";
import { renderBoard, writeProgress } from "./board.ts";
import { appendJournal } from "./store.ts";
import { lastNext, noteTask, reap, tasksForAgent } from "./task.ts";

export interface HookInput {
  session_id?: string;
  cwd?: string;
  hook_event_name?: string;
  tool_name?: string;
  tool_input?: Record<string, unknown>;
  source?: string;
  reason?: string;
}

export function readHookInput(): HookInput {
  try {
    const raw = fs.readFileSync(0, "utf8");
    if (raw.trim().length === 0) return {};
    const parsed: unknown = JSON.parse(raw);
    return typeof parsed === "object" && parsed !== null ? (parsed as HookInput) : {};
  } catch {
    return {};
  }
}

function emit(payload: unknown): void {
  process.stdout.write(`${JSON.stringify(payload)}\n`);
}

function agentFor(input: HookInput): string | null {
  return resolveAgentId(input.session_id);
}

/** The file an edit-shaped tool call is about to write, in repo-relative form. */
function targetPath(input: HookInput): string | null {
  const toolInput = input.tool_input;
  if (toolInput === undefined) return null;
  for (const field of ["file_path", "notebook_path", "path"]) {
    const value = toolInput[field];
    if (typeof value === "string" && value.length > 0) return toRepoRelative(value);
  }
  return null;
}

/** Registers the agent, heals whatever a crashed session left behind, and shows the board. */
export function sessionStart(input: HookInput): void {
  const agentId = agentFor(input);
  if (agentId !== null) registerAgent(agentId);

  const report = reap();
  writeProgress();

  const lines: string[] = [];
  lines.push("Multi-agent harness is active for this repository.");
  if (agentId !== null) {
    lines.push(`Your agent id is ${agentId} (shown as ${shortId(agentId)} on the board).`);
  }
  lines.push(
    "Claim a task before editing shared code: `node .claude/harness/cli.ts task claim <id>`. " +
      "See .claude/README.md for the protocol.",
  );

  if (report.leases.length > 0 || report.tasks.length > 0) {
    lines.push("");
    lines.push("Recovered from a previous session:");
    for (const lease of report.leases) {
      lines.push(`- released the stale lease on ${lease.scope} (was ${lease.agentName})`);
    }
    for (const entry of report.tasks) {
      const next = lastNext(entry.task);
      lines.push(
        `- ${entry.task.id} "${entry.task.title}" is ${entry.parked ? "parked as blocked" : "available again"}` +
          (next === null ? "" : ` — next step was: ${next}`),
      );
    }
  }

  lines.push("");
  lines.push(renderBoard({ agentId }));

  emit({
    hookSpecificOutput: {
      hookEventName: "SessionStart",
      additionalContext: lines.join("\n"),
    },
  });
}

/** Refuses an edit that lands inside a scope another live agent holds. */
export function preToolUse(input: HookInput): void {
  const config = loadConfig();
  if (config.guard.mode === "off") return;

  const target = targetPath(input);
  if (target === null) return;
  const agentId = agentFor(input);

  for (const glob of config.guard.protectedPaths) {
    if (!matchesPath(glob, target)) continue;
    emit({
      hookSpecificOutput: {
        hookEventName: "PreToolUse",
        permissionDecision: "deny",
        permissionDecisionReason:
          `${target} is harness state owned by the coordination layer. ` +
          "Change it through `node .claude/harness/cli.ts` instead of editing it directly.",
      },
    });
    return;
  }

  const conflicts = conflictsForPath(target, agentId);
  if (conflicts.length === 0) return;

  const first = conflicts[0];
  if (first === undefined) return;
  const reason =
    `${target} is inside ${first.scope}, leased by ${first.agentName} for ` +
    `${first.taskId ?? "an unnamed task"}. Coordinate instead of editing: pick a different task, ` +
    "or run `node .claude/harness/cli.ts board` to see what is in flight. " +
    "If that agent is genuinely gone, `node .claude/harness/cli.ts reap` releases it.";

  appendJournal({ event: "guard.conflict", path: target, agentId, holder: first.agentId, scope: first.scope });

  if (config.guard.mode === "warn") {
    emit({ systemMessage: `Harness warning: ${reason}` });
    return;
  }

  emit({
    hookSpecificOutput: {
      hookEventName: "PreToolUse",
      permissionDecision: "deny",
      permissionDecisionReason: reason,
    },
  });
}

/** Keeps this agent's leases alive while it is demonstrably working. */
export function postToolUse(input: HookInput): void {
  const agentId = agentFor(input);
  if (agentId === null) return;
  registerAgent(agentId);
  heartbeatLeases(agentId);
}

/** Nudges an agent that is stopping while still holding work, without blocking it. */
export function stop(input: HookInput): void {
  const agentId = agentFor(input);
  if (agentId === null) return;
  registerAgent(agentId);

  const held = tasksForAgent(agentId);
  if (held.length === 0) return;

  const missingNext = held.filter((task) => lastNext(task) === null);
  if (missingNext.length === 0) return;

  const ids = missingNext.map((task) => task.id).join(", ");
  emit({
    systemMessage:
      `Harness: you still hold ${ids} with no recorded next step. ` +
      "Run `node .claude/harness/cli.ts task note <id> \"<what you did>\" --next \"<what is left>\"` " +
      "so another agent can pick this up if this session goes away.",
  });
}

/** Clean shutdown: check the work in and let go of the scopes. */
export function sessionEnd(input: HookInput): void {
  const agentId = agentFor(input);
  if (agentId === null) return;

  const held = tasksForAgent(agentId);
  for (const task of held) {
    noteTask(
      task.id,
      agentId,
      `session ended (${input.reason ?? "unknown reason"}) while holding this task`,
      lastNext(task),
    );
  }

  const leases = leasesForAgent(agentId);
  if (leases.length > 0) releaseLeases({ agentId });
  writeProgress();
  appendJournal({ event: "session.end", agentId, reason: input.reason ?? null, released: leases.length });
}

const HANDLERS: Record<string, (input: HookInput) => void> = {
  "session-start": sessionStart,
  "pre-tool-use": preToolUse,
  "post-tool-use": postToolUse,
  stop,
  "session-end": sessionEnd,
};

/** Dispatch with the fail-open guarantee applied once, around everything. */
export function runHook(name: string): void {
  try {
    const handler = HANDLERS[name];
    if (handler === undefined) return;
    handler(readHookInput());
  } catch {
    // Deliberately silent: a broken harness must not become a broken session.
  }
}
