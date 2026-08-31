---
description: Check in the work you hold, release its scopes, and leave a resumable note
argument-hint: [why you are stopping]
allowed-tools: Bash(node .claude/harness/cli.ts:*)
---

You are handing off. Here is what you currently hold:

!`node .claude/harness/cli.ts whoami`

Do this in order:

1. **Work out the real state of each task you hold.** Check the working tree
   (`git status --short`, `git diff --stat`) so the note reflects what is actually on
   disk, not what you intended to do.
2. **Write the handoff note.** For each held task, run:

   ```
   node .claude/harness/cli.ts task note <id> "<what you actually finished>" --next "<what the next agent should do first>"
   ```

   The `--next` text is read by an agent with none of this context. Name files, name the
   command to run, and say what is known-broken.
3. **Release everything:**

   ```
   node .claude/harness/cli.ts handoff "$ARGUMENTS"
   ```

4. **Report back** which tasks went back on the board and what the next agent is expected
   to pick up.

If a task is finished rather than handed off, use `task done <id> --verified "<command>"`
instead — but only if that command actually passed.
