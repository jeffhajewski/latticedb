# Repository Guidelines

## Project Structure & Module Organization
`src/` contains the Zig core: `storage/`, `graph/`, `query/`, `vector/`, `fts/`, `api/`, and `cli/`. Public C headers live in `include/`. Tests are split by intent under `tests/`: `unit/`, `integration/`, `crash/`, `fuzz/`, `benchmark/`, and `container/`. Language bindings live in `bindings/python` and `bindings/typescript`; runnable examples are under `examples/`, and design notes live in `docs/`.

## Build, Test, and Development Commands
Use Zig for all core builds from the repo root:

- `zig build` builds the static library, shared library, and CLI.
- `zig build test` runs library and unit tests.
- `zig build integration-test` runs `tests/integration/`.
- `zig build crash-test` validates recovery behavior.
- `zig build shared` builds the dynamic library used by bindings.
- `zig build fuzz -- --fuzz` starts continuous fuzzing.
- `zig build benchmark` or `zig build vector-benchmark` runs performance suites.

Binding workflows are local to each package:

- `cd bindings/python && pip install -e ".[dev]" && pytest tests`
- `cd bindings/typescript && npm install && npm test`

## Coding Style & Naming Conventions
Run `zig fmt build.zig src tests` before submitting Zig changes. Keep Zig filenames and helpers in `snake_case`; follow existing test names like `buffer_pool_test.zig`. Python uses 4-space indentation, strict `mypy`, `ruff check`, 100-character lines, and `test_*.py`. TypeScript uses 2-space indentation, strict mode, `npm run lint`, and Jest; keep source in `src/` and tests as `*.test.ts`.

If you change the C API in `include/lattice.h` or `src/api/c_api.zig`, update both bindings in the same branch.

## Testing Guidelines
Add tests beside the affected area: core behavior in `tests/unit/`, cross-module flows in `tests/integration/`, durability work in `tests/crash/`, parser/storage edge cases in `tests/fuzz/`. Prefer targeted runs while iterating, then finish with `zig build test` and any binding-specific tests your change touches.

## Multi-agent Harness
Several agent sessions work this repository at once, coordinated through `.claude/`. Before editing shared code, claim a task so its file scopes are locked against other agents:

- `node .claude/harness/cli.ts board` shows what is in flight, who holds it, and what is free.
- `node .claude/harness/cli.ts task claim <id>` takes a task and locks its scopes; an edit into somebody else's scope is refused with the holder's name.
- `node .claude/harness/cli.ts task note <id> "<did>" --next "<next step>"` checkpoints progress. The `--next` text is what a replacement agent reads if this session dies.
- `node .claude/harness/cli.ts reap` reclaims tasks and locks from sessions that vanished; it also runs automatically at the start of every session.
- `node .claude/harness/cli.ts selftest` tests the harness itself against a throwaway state directory.

The full protocol is `.claude/skills/harness-protocol/SKILL.md`; the design and its limits are documented in `.claude/README.md`. Requires Node 22.18+; nothing is installed or built. `.claude/state/` and `.claude/PROGRESS.md` are generated and stay out of git.

## Commit & Pull Request Guidelines
Recent history favors short, imperative subjects such as `Fix Linux CI failures in Python and TypeScript binding tests` and `Add container-based cross-platform integration tests`. Keep commits focused and descriptive. PRs should state the problem, summarize the approach, list commands run, and link any issue. Include screenshots only for `website/` or other user-facing visual changes.

After you complete an atomic set of changes for a feature, add the changes and create a commit. NEVER say Co-authored by Codex or anything along those lines. Commits should always appear as the registered git profile on this system.
