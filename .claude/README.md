# Multi-agent harness

Several Claude Code sessions work this repository at the same time. This directory holds
the coordination layer that keeps them from overwriting each other, records what each one
is doing, and makes a crashed session's work recoverable instead of lost.

It has no dependencies and no build step. Node runs the TypeScript directly:

```
node .claude/harness/cli.ts board
node .claude/harness/cli.ts help
```

Requires Node 22.18 or newer (native type stripping). Nothing is installed, and no
`node_modules` is created.

## What it solves

| Problem | Mechanism |
| --- | --- |
| Two agents editing the same code | Each task declares the path globs it will touch; claiming it takes an exclusive **lease** on them, and a `PreToolUse` hook refuses edits into somebody else's lease |
| Nobody knows what is in flight | A shared **task board** with owners, scopes and dependencies, rendered to `.claude/PROGRESS.md` |
| A session dies mid-task | Leases are heart-beaten. One that stops being refreshed — or whose process is gone — is **reclaimed**, and the task returns to the board with its last checkpoint intact |
| Work resumes from zero | Every checkpoint records a `next` step written for a stranger, so a replacement agent continues rather than restarts |
| Retry loops on a poisoned task | After `maxAttempts` reclaims a task parks as `blocked` for a human instead of cycling |

## Layout

```
.claude/
├── settings.json                  hook wiring (committed)
├── PROGRESS.md                    generated board — do not hand-edit (ignored by git)
├── state/                         runtime coordination state (ignored by git)
│   ├── tasks/<id>.json            one file per task
│   ├── leases/<key>.json          one file per locked scope
│   ├── agents/<id>.json           registered sessions and heartbeats
│   └── journal.jsonl              append-only audit trail
├── harness/
│   ├── cli.ts                     entrypoint
│   ├── config.json                tunables (committed)
│   └── src/                       the implementation
├── skills/harness-protocol/       the protocol agents follow
├── commands/                      /board /task /handoff /reap
└── agents/                        harness-worker, harness-verifier
```

State is deliberately **not committed**. Task ownership and leases are machine-local
coordination; committing them would produce exactly the merge conflicts the harness
exists to prevent.

## Daily use

```bash
# What is happening?
node .claude/harness/cli.ts board

# Put work on the board with the paths it will touch and how it will be proven
node .claude/harness/cli.ts task add "Fix BTree.get() lifetime contract" \
  --scope "src/storage/btree.zig" --accept "zig build test" --prio 1

# Take it — this locks the scopes
node .claude/harness/cli.ts task claim T-0001

# Checkpoint as you go; --next is what a replacement agent reads
node .claude/harness/cli.ts task note T-0001 "get() now returns owned slices" \
  --next "update src/fts/dictionary.zig callers, then zig build test"

# Finish, recording what you actually ran
node .claude/harness/cli.ts task done T-0001 --verified "zig build test"
```

`node .claude/harness/cli.ts help` lists everything.

## How conflicts are prevented

A scope is a path glob. `src/storage/**`, `src/storage`, and `src/storage/btree.zig` all
overlap, and overlap is decided conservatively — the harness would rather report a
conflict that is not real than miss one that is.

While agent A holds `src/storage/**`, an edit by agent B to `src/storage/btree.zig` is
refused by the hook, with a message naming A and the task. B is not blocked from anything
else in the repository.

This is inert when you are working alone: with no other agent holding leases, nothing is
ever denied.

Declare scopes as narrowly as the work truly needs. `src/**` locks the whole codebase.

## How crash recovery works

Every lease carries a heartbeat, refreshed whenever its owner uses an editing tool. A
lease becomes reclaimable when either:

- its heartbeat is older than `leaseTtlMs` (default 30 minutes), or
- the Claude Code process that owns it is provably gone.

`reap` then releases the lease and returns the task to `ready` — carrying its last
recorded `next` step forward — so any agent can pick it up. This runs automatically at
the start of every session, so opening a new window heals whatever the last crash left
behind. `node .claude/harness/cli.ts reap --dry-run` shows what it would do first.

A clean exit does not wait for the TTL: the `SessionEnd` hook checkpoints and releases
immediately.

## Hooks

`settings.json` wires five events. **All of them fail open** — a harness bug, a corrupt
state file, or a crash inside a hook results in the edit being allowed. A broken
coordination layer must never become a broken editing session.

| Event | What it does |
| --- | --- |
| `SessionStart` | registers the session, reaps stale work, injects the board into context |
| `PreToolUse` | refuses an edit into another live agent's scope |
| `PostToolUse` | refreshes this session's heartbeats |
| `Stop` | warns if you are holding a task with no recorded next step |
| `SessionEnd` | checkpoints and releases everything this session holds |

The hook commands use paths relative to the repository root. If you start Claude Code
from a subdirectory and the hooks appear to do nothing, that is why.

## Configuration

`.claude/harness/config.json`:

| Key | Default | Meaning |
| --- | --- | --- |
| `leaseTtlMs` | `1800000` | how long a lease survives without a heartbeat |
| `maxAttempts` | `3` | reclaims before a task parks as `blocked` |
| `guard.mode` | `"deny"` | `deny` refuses conflicting edits, `warn` only comments, `off` disables the guard |
| `guard.protectedPaths` | `[".claude/state/**"]` | never editable by hand |
| `defaultAcceptance` | `[]` | commands every task inherits when it declares none |

## Limits worth knowing

- **Subagents share their parent's identity.** Two subagents of one session cannot use
  leases against each other; the parent has to serialize them.
- **One working copy is still one working copy.** Leases stop agents from editing the
  same files, but genuinely parallel work on the same area needs a `git worktree`.
- **Coordination is machine-local.** Two clones on two machines do not see one board.
- **The guard covers file-editing tools.** A `Bash` command that rewrites a file goes
  around it. Do not use shell redirection to dodge a refusal.

## Testing the harness

```
node .claude/harness/cli.ts selftest
```

Runs against a throwaway state directory — never the live board. It drives real child
processes to prove the properties that matter: overlapping claims are refused, exactly
one of five racing agents wins a contested task, concurrent creates allocate unique ids,
a simulated crash returns the task with its notes intact, repeated reclaims park it, and
malformed input or corrupt state still lets edits through.

Optional type check, using the TypeScript already installed for the TS binding:

```
npm --prefix bindings/typescript install
node bindings/typescript/node_modules/typescript/bin/tsc -p .claude/harness/tsconfig.check.json
```
