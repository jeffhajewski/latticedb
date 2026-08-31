---
description: Drive the harness task board — add, claim, checkpoint, or finish a task
argument-hint: add "<title>" --scope <glob> | claim <id> | note <id> "<did>" --next "<next>" | done <id> | list
allowed-tools: Bash(node .claude/harness/cli.ts:*)
---

Run the harness task command with these arguments: `$ARGUMENTS`

That is: `node .claude/harness/cli.ts task $ARGUMENTS`

Rules for how to handle the result:

- **If the arguments are empty or ambiguous**, run `node .claude/harness/cli.ts task list`
  first and ask the user which task they mean rather than guessing.
- **If a claim is refused for a scope conflict**, do not retry with `--force`. Report who
  holds the scope and offer the alternatives: pick a different task, or wait.
- **If a claim succeeds**, state the scopes now locked, so the user knows what other
  agents can no longer touch.
- **When adding a task**, make sure it declares `--scope` for every area it will edit and
  an `--accept` command that proves it is done. If the user did not give them, infer
  them from the title and say what you inferred. Keep scopes as narrow as the work
  genuinely needs.
- **When recording a note**, `--next` must read as an instruction to a different agent
  who has none of this conversation's context.

The full protocol is in `.claude/skills/harness-protocol/SKILL.md`.
