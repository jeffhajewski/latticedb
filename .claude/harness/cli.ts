// Harness CLI. Run it with plain Node — no install and no build step:
//
//   node .claude/harness/cli.ts board
//
// Every command is one short, locked transaction against `.claude/state`, so several
// agent sessions can drive it at the same time.
import { boardJson, renderBoard, writeProgress } from "./src/board.ts";
import { loadConfig } from "./src/config.ts";
import { listAgents, registerAgent, requireAgentId, resolveAgentId, shortId } from "./src/identity.ts";
import {
  conflictsForPath,
  isLeaseStale,
  leasesForAgent,
  listLeases,
  releaseLeases,
} from "./src/lease.ts";
import { ensureDirs, progressPath, stateDir } from "./src/paths.ts";
import { runSelfTest } from "./src/selftest.ts";
import { runHook } from "./src/hooks.ts";
import {
  blockTask,
  claimTask,
  createTask,
  dropTask,
  finishTask,
  getTask,
  lastNext,
  listTasks,
  noteTask,
  reap,
  removeTask,
  tasksForAgent,
  unblockTask,
} from "./src/task.ts";
import type { Task, TaskOutcome } from "./src/task.ts";

const BOOLEAN_FLAGS = new Set(["force", "json", "dry-run", "mine", "all", "backlog", "quiet"]);

interface ParsedArgs {
  positional: string[];
  values: Map<string, string[]>;
  flags: Set<string>;
}

function parseArgs(argv: string[]): ParsedArgs {
  const positional: string[] = [];
  const values = new Map<string, string[]>();
  const flags = new Set<string>();

  for (let i = 0; i < argv.length; i += 1) {
    const token = argv[i];
    if (token === undefined) continue;
    if (!token.startsWith("--")) {
      positional.push(token);
      continue;
    }
    const body = token.slice(2);
    const eq = body.indexOf("=");
    if (eq !== -1) {
      const name = body.slice(0, eq);
      const list = values.get(name) ?? [];
      list.push(body.slice(eq + 1));
      values.set(name, list);
      continue;
    }
    if (BOOLEAN_FLAGS.has(body)) {
      flags.add(body);
      continue;
    }
    const next = argv[i + 1];
    if (next === undefined || next.startsWith("--")) {
      flags.add(body);
      continue;
    }
    const list = values.get(body) ?? [];
    list.push(next);
    values.set(body, list);
    i += 1;
  }

  return { positional, values, flags };
}

function first(args: ParsedArgs, name: string): string | undefined {
  return args.values.get(name)?.[0];
}

function all(args: ParsedArgs, name: string): string[] {
  return args.values.get(name) ?? [];
}

function out(text: string): void {
  process.stdout.write(`${text}\n`);
}

function fail(message: string, code = 1): never {
  process.stderr.write(`${message}\n`);
  process.exit(code);
}

/** Keeps PROGRESS.md in step with state after anything that changes the board. */
function refreshBoard(): void {
  try {
    writeProgress();
  } catch {
    // The board is a convenience view; failing to render it must not fail the command.
  }
}

function describeTask(task: Task): string {
  const lines: string[] = [];
  lines.push(`${task.id}  ${task.title}`);
  lines.push(`  status:     ${task.status} (priority ${task.priority}, attempts ${task.attempts})`);
  lines.push(`  owner:      ${task.owner === null ? "unowned" : `${task.owner.agentName} (${task.owner.agentId})`}`);
  lines.push(`  scopes:     ${task.scopes.length === 0 ? "(none)" : task.scopes.join(", ")}`);
  if (task.dependsOn.length > 0) lines.push(`  depends on: ${task.dependsOn.join(", ")}`);
  if (task.acceptance.length > 0) lines.push(`  acceptance: ${task.acceptance.join(" ; ")}`);
  if (task.verified.length > 0) lines.push(`  verified:   ${task.verified.join(" ; ")}`);
  if (task.notes.length > 0) lines.push(`  notes:      ${task.notes}`);
  const next = lastNext(task);
  lines.push(`  next step:  ${next ?? "(none recorded)"}`);
  lines.push("  history:");
  for (const entry of task.checkpoints.slice(-8)) {
    lines.push(`    ${entry.at}  ${entry.kind.padEnd(8)}  ${entry.did}`);
    if (entry.next !== null && entry.next.length > 0) lines.push(`${" ".repeat(30)}next: ${entry.next}`);
  }
  return lines.join("\n");
}

function reportOutcome(outcome: TaskOutcome, success: string): void {
  if (!outcome.ok) fail(`refused: ${outcome.reason ?? "unknown reason"}`);
  refreshBoard();
  out(success);
  if (outcome.task !== null) out(describeTask(outcome.task));
}

function commandTask(args: ParsedArgs): void {
  const sub = args.positional[1];
  const agentIdFlag = first(args, "agent");

  if (sub === "add") {
    const title = args.positional.slice(2).join(" ").trim();
    if (title.length === 0) fail('usage: task add "<title>" [--scope <glob>]... [--accept "<cmd>"]...', 2);
    const agentId = resolveAgentId(agentIdFlag);
    if (agentId !== null) registerAgent(agentId);
    const priorityRaw = first(args, "prio") ?? first(args, "priority");
    const task = createTask({
      title,
      scopes: all(args, "scope"),
      dependsOn: all(args, "dep"),
      acceptance: all(args, "accept"),
      priority: priorityRaw === undefined ? undefined : Number.parseInt(priorityRaw, 10),
      notes: first(args, "notes"),
      status: args.flags.has("backlog") ? "backlog" : "ready",
      agentId: agentId ?? undefined,
    });
    refreshBoard();
    out(`created ${task.id}`);
    out(describeTask(task));
    if (task.scopes.length === 0) {
      out("");
      out("note: this task declares no scopes, so it locks nothing. Add --scope so other");
      out("      agents know which files to stay out of.");
    }
    return;
  }

  if (sub === "list" || sub === undefined) {
    const status = first(args, "status");
    const agentId = resolveAgentId(agentIdFlag);
    let tasks = listTasks();
    if (status !== undefined) tasks = tasks.filter((task) => task.status === status);
    if (args.flags.has("mine")) {
      tasks = tasks.filter((task) => agentId !== null && task.owner?.agentId === agentId);
    }
    if (args.flags.has("json")) {
      out(JSON.stringify(tasks, null, 2));
      return;
    }
    if (tasks.length === 0) {
      out("no matching tasks");
      return;
    }
    for (const task of tasks) {
      const owner = task.owner === null ? "" : ` [${task.owner.agentName}]`;
      out(`${task.id}  ${task.status.padEnd(11)} p${task.priority}  ${task.title}${owner}`);
    }
    return;
  }

  const id = args.positional[2];
  if (id === undefined) fail(`usage: task ${sub} <id> ...`, 2);

  if (sub === "show") {
    const task = getTask(id);
    if (task === null) fail(`no such task: ${id}`);
    out(args.flags.has("json") ? JSON.stringify(task, null, 2) : describeTask(task));
    return;
  }

  if (sub === "rm") {
    if (!removeTask(id)) fail(`no such task: ${id}`);
    refreshBoard();
    out(`removed ${id.toUpperCase()}`);
    return;
  }

  const agentId = requireAgentId(agentIdFlag);

  if (sub === "claim") {
    reportOutcome(claimTask(id, agentId, { force: args.flags.has("force") }), `claimed ${id.toUpperCase()}`);
    return;
  }

  if (sub === "note") {
    const did = args.positional.slice(3).join(" ").trim();
    if (did.length === 0) fail('usage: task note <id> "<what you did>" --next "<what is left>"', 2);
    reportOutcome(noteTask(id, agentId, did, first(args, "next") ?? null), `recorded progress on ${id.toUpperCase()}`);
    return;
  }

  if (sub === "block") {
    const reason = args.positional.slice(3).join(" ").trim();
    if (reason.length === 0) fail('usage: task block <id> "<reason>"', 2);
    reportOutcome(blockTask(id, agentId, reason), `blocked ${id.toUpperCase()}`);
    return;
  }

  if (sub === "unblock") {
    reportOutcome(unblockTask(id, agentId), `unblocked ${id.toUpperCase()}`);
    return;
  }

  if (sub === "drop") {
    const why = args.positional.slice(3).join(" ").trim() || "handed back";
    reportOutcome(dropTask(id, agentId, why, first(args, "next") ?? null), `handed back ${id.toUpperCase()}`);
    return;
  }

  if (sub === "done") {
    reportOutcome(finishTask(id, agentId, all(args, "verified")), `finished ${id.toUpperCase()}`);
    return;
  }

  fail(`unknown task subcommand: ${sub}`, 2);
}

function commandLease(args: ParsedArgs): void {
  const sub = args.positional[1] ?? "list";
  const config = loadConfig();
  const now = Date.now();

  if (sub === "list") {
    const leases = listLeases();
    if (args.flags.has("json")) {
      out(JSON.stringify(leases, null, 2));
      return;
    }
    if (leases.length === 0) {
      out("no leases held");
      return;
    }
    for (const lease of leases) {
      const state = isLeaseStale(lease, config, now) ? "STALE" : "live";
      out(`${state.padEnd(5)}  ${lease.scope}  ${lease.agentName}  ${lease.taskId ?? "-"}  heartbeat ${lease.heartbeatAt}`);
    }
    return;
  }

  if (sub === "check") {
    const target = args.positional[2];
    if (target === undefined) fail("usage: lease check <path>", 2);
    const conflicts = conflictsForPath(target, resolveAgentId(first(args, "agent")));
    if (conflicts.length === 0) {
      out(`clear: nobody else holds ${target}`);
      return;
    }
    for (const lease of conflicts) {
      out(`conflict: ${target} is inside ${lease.scope}, held by ${lease.agentName} for ${lease.taskId ?? "no task"}`);
    }
    process.exit(1);
  }

  if (sub === "release") {
    const agentId = requireAgentId(first(args, "agent"));
    const released = releaseLeases({
      agentId,
      taskId: first(args, "task"),
      scope: args.flags.has("all") ? undefined : first(args, "scope"),
    });
    refreshBoard();
    out(released.length === 0 ? "nothing to release" : `released ${released.length}: ${released.map((l) => l.scope).join(", ")}`);
    return;
  }

  fail(`unknown lease subcommand: ${sub}`, 2);
}

function commandHandoff(args: ParsedArgs): void {
  const agentId = requireAgentId(first(args, "agent"));
  const next = first(args, "next") ?? null;
  const why = args.positional.slice(1).join(" ").trim() || "handing off";

  const held = tasksForAgent(agentId);
  if (held.length === 0) {
    out("you hold no tasks; nothing to hand off");
    return;
  }
  for (const task of held) {
    const outcome = dropTask(task.id, agentId, why, next ?? lastNext(task));
    out(outcome.ok ? `handed back ${task.id}` : `could not hand back ${task.id}: ${outcome.reason ?? ""}`);
  }
  releaseLeases({ agentId });
  refreshBoard();
  out(`board written to ${progressPath()}`);
}

function commandWhoami(args: ParsedArgs): void {
  const agentId = resolveAgentId(first(args, "agent"));
  if (agentId === null) {
    out("no agent identity available. Pass --agent <id> or set HARNESS_AGENT_ID.");
    process.exit(1);
  }
  const record = registerAgent(agentId, first(args, "name"));
  out(`agent:   ${record.name} (${record.agentId})`);
  out(`short:   ${shortId(record.agentId)}`);
  out(`process: ${record.pid ?? "unknown"}`);
  const tasks = tasksForAgent(agentId);
  out(`tasks:   ${tasks.length === 0 ? "(none)" : tasks.map((task) => task.id).join(", ")}`);
  const leases = leasesForAgent(agentId);
  out(`leases:  ${leases.length === 0 ? "(none)" : leases.map((lease) => lease.scope).join(", ")}`);
}

function commandReap(args: ParsedArgs): void {
  const dryRun = args.flags.has("dry-run");
  const report = reap({ dryRun });
  if (report.leases.length === 0 && report.tasks.length === 0) {
    out("nothing to reclaim");
    return;
  }
  const prefix = dryRun ? "would reclaim" : "reclaimed";
  for (const lease of report.leases) {
    out(`${prefix} lease ${lease.scope} from ${lease.agentName} (last heartbeat ${lease.heartbeatAt})`);
  }
  for (const entry of report.tasks) {
    const next = lastNext(entry.task);
    out(
      `${prefix} task ${entry.task.id} "${entry.task.title}" -> ${entry.parked ? "blocked (too many attempts)" : "ready"}` +
        (next === null ? "" : `; next step: ${next}`),
    );
  }
  if (!dryRun) refreshBoard();
}

function commandInit(): void {
  ensureDirs();
  const file = writeProgress();
  out(`harness state ready at ${stateDir()}`);
  out(`board written to ${file}`);
  const agentId = resolveAgentId();
  out(agentId === null ? "no agent identity detected yet" : `this session is agent ${agentId}`);
}

function commandAgents(args: ParsedArgs): void {
  const agents = listAgents();
  if (args.flags.has("json")) {
    out(JSON.stringify(agents, null, 2));
    return;
  }
  if (agents.length === 0) {
    out("no agents registered");
    return;
  }
  for (const agent of agents) {
    out(`${agent.name.padEnd(16)} ${shortId(agent.agentId)}  pid ${agent.pid ?? "?"}  last seen ${agent.heartbeatAt}`);
  }
}

const USAGE = `Multi-agent harness for this repository.

  node .claude/harness/cli.ts <command>

Board
  init                                    create state directories and render the board
  board [--json]                          show the board (also refreshes PROGRESS.md)
  agents [--json]                         list registered agents
  whoami [--name <label>]                 show this session's identity and holdings

Tasks
  task add "<title>" [--scope <glob>]... [--dep <id>]... [--accept "<cmd>"]...
                     [--prio 1..3] [--notes "<text>"] [--backlog]
  task list [--status <s>] [--mine] [--json]
  task show <id> [--json]
  task claim <id> [--force]
  task note <id> "<what you did>" --next "<what is left>"
  task block <id> "<reason>"      task unblock <id>
  task drop <id> "<why>" [--next "<next step>"]
  task done <id> [--verified "<command you ran>"]...
  task rm <id>

Scopes
  lease list [--json]
  lease check <path>                      exit 1 if another agent holds it
  lease release [--task <id>] [--scope <glob>] [--all]

Recovery
  reap [--dry-run]                        reclaim leases and tasks from vanished agents
  handoff "<why>" [--next "<next step>"]  check work in and release everything

Other
  selftest                                run the harness's own tests
  hook <name>                             internal: Claude Code hook entrypoint

Every command accepts --agent <id>; inside Claude Code the identity is detected
automatically from CLAUDE_CODE_SESSION_ID.`;

function main(): void {
  const args = parseArgs(process.argv.slice(2));
  const command = args.positional[0] ?? "board";

  switch (command) {
    case "hook": {
      const name = args.positional[1];
      if (name !== undefined) runHook(name);
      return;
    }
    case "init":
      commandInit();
      return;
    case "board": {
      ensureDirs();
      if (args.flags.has("json")) {
        out(JSON.stringify(boardJson(), null, 2));
        return;
      }
      refreshBoard();
      out(renderBoard({ agentId: resolveAgentId(first(args, "agent")) }));
      return;
    }
    case "agents":
      commandAgents(args);
      return;
    case "whoami":
      commandWhoami(args);
      return;
    case "task":
      commandTask(args);
      return;
    case "lease":
      commandLease(args);
      return;
    case "reap":
      commandReap(args);
      return;
    case "handoff":
      commandHandoff(args);
      return;
    case "selftest":
      // The only asynchronous command: it drives real child processes to prove that the
      // locking holds across separate agents, not just across calls in one process.
      void runSelfTest().then((ok) => {
        process.exit(ok ? 0 : 1);
      });
      return;
    case "help":
    case "--help":
    case "-h":
      out(USAGE);
      return;
    default:
      fail(`unknown command: ${command}\n\n${USAGE}`, 2);
  }
}

try {
  main();
} catch (err) {
  fail(`harness error: ${err instanceof Error ? err.message : String(err)}`);
}
