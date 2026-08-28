---
description: Recover tasks and scope locks left behind by an agent session that died
allowed-tools: Bash(node .claude/harness/cli.ts:*)
---

What would be reclaimed:

!`node .claude/harness/cli.ts reap --dry-run`

If that list is empty, say so and stop — nothing is stuck.

Otherwise:

1. Run `node .claude/harness/cli.ts reap` to reclaim it.
2. For each recovered task, report its recorded next step. That note is the whole point:
   it says where the vanished agent got to.
3. If a task came back parked as `blocked` because it has been reclaimed too many times,
   flag it to the user as needing a human decision. Do not simply unblock and retry it —
   something about that task keeps killing sessions.

Then ask whether the user wants you to pick one of the recovered tasks up.
