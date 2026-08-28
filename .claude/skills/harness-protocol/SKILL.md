---
name: harness-protocol
description: The multi-agent working protocol for this repository. Use when starting work, picking a task off the board, hitting a scope conflict with another agent, resuming work another session left half-finished, or handing work off. Covers claiming tasks, locking file scopes, checkpointing progress so a crashed session can be recovered, and releasing cleanly.
---

# Multi-agent protocol

Several agent sessions work this repository at once. The harness keeps them from
overwriting each other and keeps a crashed session's work recoverable. All of it runs
through one command:

```
node .claude/harness/cli.ts <command>
```

Identity is automatic inside Claude Code — the CLI reads `CLAUDE_CODE_SESSION_ID`. You
never need `--agent` unless you are driving the harness from outside a session.

## The loop

**1. Look before you start.**

```
node .claude/harness/cli.ts board
```

Shows what is in flight, who holds it, what is free, and what was reclaimed from a
session that went away. Anything under "Ready to claim" is yours to take.

**2. Claim before you edit.**

```
node .claude/harness/cli.ts task claim T-0007
```

This locks the task's scopes. Until you release them, other agents are refused edits
inside them — and you are refused edits inside theirs. A refusal is not a bug; it means
another agent is live in that code. Pick different work rather than forcing past it.

If the work is not on the board yet, put it there first, with the paths it will touch:

```
node .claude/harness/cli.ts task add "Fix BTree.get() lifetime contract" \
  --scope "src/storage/btree.zig" --scope "tests/unit/btree_test.zig" \
  --accept "zig build test" --prio 1
```

Declare scopes as narrowly as the work truly needs. `src/**` locks the whole codebase
and stalls everyone else; `src/storage/**` usually does not.

**3. Checkpoint as you go — this is the important one.**

```
node .claude/harness/cli.ts task note T-0007 \
  "rewrote the eviction path; get() now returns owned slices" \
  --next "update src/fts/dictionary.zig callers, then run zig build test"
```

`--next` is what a different agent reads if this session dies. Write it as an
instruction to a stranger, not a reminder to yourself. Checkpoint after each meaningful
step, not just at the end.

**4. Finish and release.**

```
node .claude/harness/cli.ts task done T-0007 --verified "zig build test"
```

Record what you actually ran. Do not mark a task done on a red build — use `task block`
with the reason instead, which also releases the scopes.

**5. Stopping early?** Hand the work back so somebody else can take it:

```
node .claude/harness/cli.ts task drop T-0007 "out of context budget" \
  --next "the parser change is done; the executor side is untouched"
```

## When another agent has vanished

A lease stops being refreshed when its session dies. Any agent may reclaim it:

```
node .claude/harness/cli.ts reap --dry-run   # see what would be reclaimed
node .claude/harness/cli.ts reap             # actually reclaim it
```

Reclaimed tasks go back to `ready` with their last `--next` note intact, so you can pick
one up and continue. This also runs automatically at the start of every session.

A task reclaimed too many times parks itself as `blocked` rather than cycling forever —
that is a signal for a human, not something to force back into `ready`.

## Rules

- **Claim before editing shared code.** Trivial single-file fixes on an unclaimed file
  are fine without a task; anything spanning files or lasting more than a few minutes
  is not.
- **Never edit `.claude/state/` by hand.** The harness owns it, and direct edits are
  blocked. Go through the CLI.
- **Do not `--force` past another live agent.** Force exists for the case where you have
  positively confirmed a session is gone and `reap` has not caught up yet.
- **Do not release somebody else's lease.** The CLI will not let you, and working around
  it defeats the point.
- **Truly parallel edits to one area need a git worktree**, not a shared checkout. Two
  agents in one working copy on one directory will collide no matter what the board says.

## Commit discipline

The repository rule applies unchanged: finish a stage, then commit it — build green,
relevant tests green, docs updated. Stage only the files your task touched; never
`git add -A`, because another agent's work is probably in the tree next to yours.
