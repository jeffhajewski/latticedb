---
description: Show the multi-agent task board — who holds what, and what is free to claim
allowed-tools: Bash(node .claude/harness/cli.ts:*)
---

Current board:

!`node .claude/harness/cli.ts board`

Summarise this for the user in a few lines: what is in flight and by whom, what is ready
to claim, and anything that looks stuck — a task reclaimed more than once, a task parked
as blocked, or a lease flagged reclaimable. Do not repeat the table back verbatim.

If anything is reclaimable, say so and mention that `/reap` recovers it.
