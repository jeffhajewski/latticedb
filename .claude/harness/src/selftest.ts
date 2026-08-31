// The harness's own test suite. It runs against a throwaway state directory, so it is
// always safe to run — `node .claude/harness/cli.ts selftest` never touches the live board.
//
// The properties worth proving are the ones the harness exists for: two agents cannot
// take the same scope, a crashed agent's work comes back with its notes intact, and a
// broken harness still lets people edit files.
import { spawn } from "node:child_process";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { randomBytes } from "node:crypto";
import { HARNESS_DIR } from "./paths.ts";

const CLI = path.join(HARNESS_DIR, "cli.ts");

interface RunResult {
  code: number | null;
  stdout: string;
  stderr: string;
}

interface TestContext {
  stateDir: string;
  configFile: string;
}

function runCli(context: TestContext, argv: string[], stdin?: string): Promise<RunResult> {
  return new Promise((resolve) => {
    const child = spawn(process.execPath, [CLI, ...argv], {
      env: {
        ...process.env,
        HARNESS_STATE_DIR: context.stateDir,
        HARNESS_CONFIG: context.configFile,
      },
      stdio: ["pipe", "pipe", "pipe"],
    });
    let stdout = "";
    let stderr = "";
    child.stdout.on("data", (chunk: Buffer) => {
      stdout += chunk.toString("utf8");
    });
    child.stderr.on("data", (chunk: Buffer) => {
      stderr += chunk.toString("utf8");
    });
    child.on("close", (code) => resolve({ code, stdout, stderr }));
    if (stdin !== undefined) child.stdin.write(stdin);
    child.stdin.end();
  });
}

function readState<T>(context: TestContext, ...parts: string[]): T | null {
  try {
    return JSON.parse(fs.readFileSync(path.join(context.stateDir, ...parts), "utf8")) as T;
  } catch {
    return null;
  }
}

/** Ages every heartbeat belonging to an agent, standing in for a session that died. */
function simulateDeath(context: TestContext, agentId: string): void {
  const longAgo = new Date(Date.now() - 60 * 60 * 1000).toISOString();
  const agentFile = path.join(context.stateDir, "agents", `${encodeURIComponent(agentId)}.json`);
  const agent = readState<Record<string, unknown>>(context, "agents", `${encodeURIComponent(agentId)}.json`);
  if (agent !== null) {
    fs.writeFileSync(agentFile, JSON.stringify({ ...agent, heartbeatAt: longAgo }, null, 2), "utf8");
  }
  const leasesDir = path.join(context.stateDir, "leases");
  for (const name of fs.existsSync(leasesDir) ? fs.readdirSync(leasesDir) : []) {
    if (!name.endsWith(".json")) continue;
    const file = path.join(leasesDir, name);
    const lease = JSON.parse(fs.readFileSync(file, "utf8")) as Record<string, unknown>;
    if (lease["agentId"] !== agentId) continue;
    fs.writeFileSync(file, JSON.stringify({ ...lease, heartbeatAt: longAgo }, null, 2), "utf8");
  }
}

interface TaskRecord {
  id: string;
  status: string;
  attempts: number;
  owner: { agentId: string } | null;
  checkpoints: { next: string | null }[];
}

function taskRecord(context: TestContext, id: string): TaskRecord | null {
  return readState<TaskRecord>(context, "tasks", `${id}.json`);
}

function lastNextOf(task: TaskRecord): string | null {
  for (let i = task.checkpoints.length - 1; i >= 0; i -= 1) {
    const entry = task.checkpoints[i];
    if (entry !== undefined && entry.next !== null && entry.next.length > 0) return entry.next;
  }
  return null;
}

type Check = (context: TestContext) => Promise<string[]>;

/** Each test returns a list of failure messages; empty means it passed. */
const TESTS: { name: string; run: Check }[] = [
  {
    name: "init creates the state directories",
    run: async (context) => {
      const result = await runCli(context, ["init"]);
      const failures: string[] = [];
      if (result.code !== 0) failures.push(`init exited ${result.code}: ${result.stderr}`);
      for (const dir of ["tasks", "leases", "agents"]) {
        if (!fs.existsSync(path.join(context.stateDir, dir))) failures.push(`missing ${dir}/`);
      }
      return failures;
    },
  },
  {
    name: "a claimed task locks its scope",
    run: async (context) => {
      const failures: string[] = [];
      const added = await runCli(context, [
        "task", "add", "storage work", "--scope", "src/storage/**", "--agent", "agent-a",
      ]);
      if (!added.stdout.includes("created T-0001")) failures.push(`unexpected add output: ${added.stdout}`);

      const claimed = await runCli(context, ["task", "claim", "T-0001", "--agent", "agent-a"]);
      if (claimed.code !== 0) failures.push(`claim failed: ${claimed.stderr}`);

      const task = taskRecord(context, "T-0001");
      if (task?.status !== "in_progress") failures.push(`status is ${task?.status ?? "missing"}`);
      if (task?.owner?.agentId !== "agent-a") failures.push("owner was not recorded");

      const leases = await runCli(context, ["lease", "list"]);
      if (!leases.stdout.includes("src/storage/**")) failures.push("scope lease was not created");
      return failures;
    },
  },
  {
    name: "an overlapping claim by another agent is refused",
    run: async (context) => {
      const failures: string[] = [];
      await runCli(context, [
        "task", "add", "btree fix", "--scope", "src/storage/btree.zig", "--agent", "agent-b",
      ]);
      const claimed = await runCli(context, ["task", "claim", "T-0002", "--agent", "agent-b"]);
      if (claimed.code === 0) failures.push("the overlapping claim was allowed");
      if (!claimed.stderr.includes("scope conflict")) {
        failures.push(`expected a scope conflict, got: ${claimed.stderr.trim()}`);
      }
      const task = taskRecord(context, "T-0002");
      if (task?.status === "in_progress") failures.push("the refused task was still marked in progress");
      return failures;
    },
  },
  {
    name: "lease check sees a foreign scope but not your own",
    run: async (context) => {
      const failures: string[] = [];
      const foreign = await runCli(context, [
        "lease", "check", "src/storage/btree.zig", "--agent", "agent-b",
      ]);
      if (foreign.code !== 1) failures.push("a foreign path did not report a conflict");
      if (!foreign.stdout.includes("agent-a")) failures.push("the conflict did not name the holder");

      const own = await runCli(context, ["lease", "check", "src/storage/btree.zig", "--agent", "agent-a"]);
      if (own.code !== 0) failures.push("the holder was blocked from its own scope");

      const unrelated = await runCli(context, ["lease", "check", "src/graph/node.zig", "--agent", "agent-b"]);
      if (unrelated.code !== 0) failures.push("an unrelated path reported a conflict");
      return failures;
    },
  },
  {
    name: "progress notes survive as a resumable next step",
    run: async (context) => {
      const failures: string[] = [];
      const noted = await runCli(context, [
        "task", "note", "T-0001", "rewrote the page cache eviction path",
        "--next", "run zig build test and fix the fallout",
        "--agent", "agent-a",
      ]);
      if (noted.code !== 0) failures.push(`note failed: ${noted.stderr}`);
      const task = taskRecord(context, "T-0001");
      if (task === null || lastNextOf(task) !== "run zig build test and fix the fallout") {
        failures.push("the next step was not recorded");
      }
      return failures;
    },
  },
  {
    name: "a crashed agent's task is reclaimed with its notes intact",
    run: async (context) => {
      const failures: string[] = [];
      simulateDeath(context, "agent-a");

      const reaped = await runCli(context, ["reap"]);
      if (!reaped.stdout.includes("T-0001")) failures.push(`reap did not mention the task: ${reaped.stdout}`);

      const task = taskRecord(context, "T-0001");
      if (task?.status !== "ready") failures.push(`status after reap is ${task?.status ?? "missing"}`);
      if (task?.owner !== null) failures.push("the dead agent still owns the task");
      if (task?.attempts !== 1) failures.push(`attempts is ${task?.attempts ?? "missing"}, expected 1`);
      if (task !== null && lastNextOf(task) !== "run zig build test and fix the fallout") {
        failures.push("the next step was lost during reclamation");
      }

      const leases = await runCli(context, ["lease", "list"]);
      if (leases.stdout.includes("src/storage/**")) failures.push("the dead agent's lease survived");
      return failures;
    },
  },
  {
    name: "the freed scope can be claimed by somebody else",
    run: async (context) => {
      const failures: string[] = [];
      const claimed = await runCli(context, ["task", "claim", "T-0002", "--agent", "agent-b"]);
      if (claimed.code !== 0) failures.push(`the freed scope was still blocked: ${claimed.stderr}`);
      return failures;
    },
  },
  {
    name: "repeated reclamation parks a task instead of looping",
    run: async (context) => {
      const failures: string[] = [];
      // maxAttempts is 2 in the test config, and T-0002 has been reclaimed zero times.
      simulateDeath(context, "agent-b");
      await runCli(context, ["reap"]);
      const once = taskRecord(context, "T-0002");
      if (once?.status !== "ready") failures.push(`after one reclaim the status is ${once?.status ?? "missing"}`);

      await runCli(context, ["task", "claim", "T-0002", "--agent", "agent-c"]);
      simulateDeath(context, "agent-c");
      await runCli(context, ["reap"]);
      const twice = taskRecord(context, "T-0002");
      if (twice?.status !== "blocked") {
        failures.push(`expected the task to park as blocked, got ${twice?.status ?? "missing"}`);
      }
      return failures;
    },
  },
  {
    name: "finishing a task releases its scope",
    run: async (context) => {
      const failures: string[] = [];
      await runCli(context, ["task", "claim", "T-0001", "--agent", "agent-d"]);
      const done = await runCli(context, [
        "task", "done", "T-0001", "--verified", "zig build test", "--agent", "agent-d",
      ]);
      if (done.code !== 0) failures.push(`done failed: ${done.stderr}`);
      const task = taskRecord(context, "T-0001");
      if (task?.status !== "done") failures.push(`status is ${task?.status ?? "missing"}`);
      const leases = await runCli(context, ["lease", "list"]);
      if (leases.stdout.includes("src/storage/**")) failures.push("the scope was not released");
      return failures;
    },
  },
  {
    name: "the guard denies an edit inside somebody else's scope",
    run: async (context) => {
      const failures: string[] = [];
      await runCli(context, [
        "task", "add", "guarded work", "--scope", "src/query/**", "--agent", "agent-e",
      ]);
      await runCli(context, ["task", "claim", "T-0003", "--agent", "agent-e"]);

      const payload = JSON.stringify({
        session_id: "agent-f",
        hook_event_name: "PreToolUse",
        tool_name: "Edit",
        tool_input: { file_path: "src/query/parser.zig" },
      });
      const denied = await runCli(context, ["hook", "pre-tool-use"], payload);
      if (!denied.stdout.includes('"deny"')) failures.push(`expected a deny decision, got: ${denied.stdout}`);
      if (!denied.stdout.includes("agent-e")) failures.push("the denial did not name the holder");

      const ownPayload = JSON.stringify({
        session_id: "agent-e",
        hook_event_name: "PreToolUse",
        tool_name: "Edit",
        tool_input: { file_path: "src/query/parser.zig" },
      });
      const allowed = await runCli(context, ["hook", "pre-tool-use"], ownPayload);
      if (allowed.stdout.trim().length !== 0) failures.push(`the holder was blocked from its own scope: ${allowed.stdout}`);

      const elsewhere = JSON.stringify({
        session_id: "agent-f",
        hook_event_name: "PreToolUse",
        tool_name: "Edit",
        tool_input: { file_path: "README.md" },
      });
      const clear = await runCli(context, ["hook", "pre-tool-use"], elsewhere);
      if (clear.stdout.trim().length !== 0) failures.push(`an unleased path was blocked: ${clear.stdout}`);
      return failures;
    },
  },
  {
    name: "harness state cannot be edited by hand",
    run: async (context) => {
      const payload = JSON.stringify({
        session_id: "agent-f",
        hook_event_name: "PreToolUse",
        tool_name: "Write",
        tool_input: { file_path: ".claude/state/tasks/T-0001.json" },
      });
      const result = await runCli(context, ["hook", "pre-tool-use"], payload);
      return result.stdout.includes('"deny"') ? [] : ["direct edits to harness state were allowed"];
    },
  },
  {
    name: "the guard fails open on malformed input and broken state",
    run: async (context) => {
      const failures: string[] = [];
      const garbage = await runCli(context, ["hook", "pre-tool-use"], "not json at all");
      if (garbage.code !== 0) failures.push(`malformed input exited ${garbage.code}`);
      if (garbage.stdout.trim().length !== 0) failures.push("malformed input produced a decision");

      // A corrupt lease must not be able to block anybody.
      const leasesDir = path.join(context.stateDir, "leases");
      const corrupt = path.join(leasesDir, "corrupt.json");
      fs.mkdirSync(leasesDir, { recursive: true });
      fs.writeFileSync(corrupt, "{ this is not json", "utf8");
      const payload = JSON.stringify({
        session_id: "agent-f",
        hook_event_name: "PreToolUse",
        tool_name: "Edit",
        tool_input: { file_path: "src/fts/scorer.zig" },
      });
      const result = await runCli(context, ["hook", "pre-tool-use"], payload);
      if (result.code !== 0) failures.push(`a corrupt lease broke the guard (exit ${result.code})`);
      if (result.stdout.includes('"deny"')) failures.push("a corrupt lease denied an unrelated edit");
      fs.rmSync(corrupt, { force: true });

      const board = await runCli(context, ["board"]);
      if (board.code !== 0) failures.push("the board could not render");
      return failures;
    },
  },
  {
    name: "concurrent task creation allocates unique ids",
    run: async (context) => {
      const results = await Promise.all(
        [1, 2, 3, 4, 5, 6].map((n) =>
          runCli(context, ["task", "add", `parallel task ${n}`, "--agent", `bulk-${n}`]),
        ),
      );
      const ids = results
        .map((result) => /created (T-\d+)/.exec(result.stdout)?.[1])
        .filter((id): id is string => id !== undefined);
      const failures: string[] = [];
      if (ids.length !== 6) failures.push(`only ${ids.length} of 6 creations succeeded`);
      if (new Set(ids).size !== ids.length) failures.push(`duplicate ids allocated: ${ids.join(", ")}`);
      return failures;
    },
  },
  {
    name: "concurrent claims on one task have exactly one winner",
    run: async (context) => {
      const add = await runCli(context, [
        "task", "add", "contended", "--scope", "src/vector/**", "--agent", "race-setup",
      ]);
      const id = /created (T-\d+)/.exec(add.stdout)?.[1];
      if (id === undefined) return ["could not create the contended task"];

      const results = await Promise.all(
        ["r1", "r2", "r3", "r4", "r5"].map((agent) =>
          runCli(context, ["task", "claim", id, "--agent", agent]),
        ),
      );
      const winners = results.filter((result) => result.code === 0);
      const failures: string[] = [];
      if (winners.length !== 1) {
        failures.push(`${winners.length} agents won the same task; exactly one should have`);
      }
      const task = taskRecord(context, id);
      if (task?.owner === null) failures.push("nobody ended up owning the contended task");
      return failures;
    },
  },
  {
    name: "handoff releases everything the agent holds",
    run: async (context) => {
      const failures: string[] = [];
      const add = await runCli(context, [
        "task", "add", "handoff subject", "--scope", "src/cli/**", "--agent", "agent-h",
      ]);
      const id = /created (T-\d+)/.exec(add.stdout)?.[1];
      if (id === undefined) return ["could not create the handoff task"];
      await runCli(context, ["task", "claim", id, "--agent", "agent-h"]);

      const handoff = await runCli(context, [
        "handoff", "stopping for the day", "--next", "wire up the REPL flag", "--agent", "agent-h",
      ]);
      if (handoff.code !== 0) failures.push(`handoff failed: ${handoff.stderr}`);

      const task = taskRecord(context, id);
      if (task?.status !== "ready") failures.push(`status after handoff is ${task?.status ?? "missing"}`);
      if (task !== null && lastNextOf(task) !== "wire up the REPL flag") {
        failures.push("the handoff note was not recorded");
      }
      const leases = await runCli(context, ["lease", "list"]);
      if (leases.stdout.includes("src/cli/**")) failures.push("the scope was not released");
      return failures;
    },
  },
  {
    name: "the rendered board reflects the state",
    run: async (context) => {
      const failures: string[] = [];
      const result = await runCli(context, ["board"]);
      if (result.code !== 0) failures.push(`board exited ${result.code}: ${result.stderr}`);
      const progress = path.join(context.stateDir, "PROGRESS.md");
      if (!fs.existsSync(progress)) {
        failures.push("PROGRESS.md was not written");
        return failures;
      }
      const markdown = fs.readFileSync(progress, "utf8");
      if (!markdown.includes("# Harness progress board")) failures.push("PROGRESS.md has no heading");
      if (!markdown.includes("T-0001")) failures.push("PROGRESS.md is missing a known task");

      const json = await runCli(context, ["board", "--json"]);
      try {
        JSON.parse(json.stdout);
      } catch {
        failures.push("board --json did not produce valid JSON");
      }
      return failures;
    },
  },
];

const TEST_CONFIG = {
  leaseTtlMs: 60_000,
  heartbeatIntervalMs: 1_000,
  maxAttempts: 2,
  guard: { mode: "deny", protectedPaths: [".claude/state/**"] },
  defaultAcceptance: [],
};

export async function runSelfTest(): Promise<boolean> {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), `harness-selftest-${randomBytes(3).toString("hex")}-`));
  const context: TestContext = {
    stateDir: path.join(root, "state"),
    configFile: path.join(root, "config.json"),
  };
  fs.writeFileSync(context.configFile, JSON.stringify(TEST_CONFIG, null, 2), "utf8");

  let failed = 0;
  process.stdout.write(`harness selftest — state in ${context.stateDir}\n\n`);

  // The tests run in order and share one board on purpose: the interesting properties
  // are about a board that has been lived in, not a fresh one.
  for (const test of TESTS) {
    let failures: string[];
    try {
      failures = await test.run(context);
    } catch (err) {
      failures = [`threw: ${err instanceof Error ? err.message : String(err)}`];
    }
    if (failures.length === 0) {
      process.stdout.write(`  PASS  ${test.name}\n`);
      continue;
    }
    failed += 1;
    process.stdout.write(`  FAIL  ${test.name}\n`);
    for (const failure of failures) process.stdout.write(`          ${failure}\n`);
  }

  process.stdout.write(`\n${TESTS.length - failed}/${TESTS.length} passed\n`);
  if (failed === 0) {
    fs.rmSync(root, { recursive: true, force: true });
  } else {
    process.stdout.write(`state kept for inspection: ${root}\n`);
  }
  return failed === 0;
}
