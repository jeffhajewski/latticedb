# Running Tests

LatticeDB's test suite covers unit tests (including multi-threaded concurrency tests of the latches, locks and transactions, in `tests/unit/concurrency_test.zig`), integration tests, crash recovery tests, fuzzing, binding tests, an engine conformance suite, and benchmarks.

## Test Commands

```bash
zig build test                 # Run unit tests
zig build integration-test     # Run integration tests
zig build crash-test           # Run crash recovery tests
zig build fuzz                 # Run fuzz tests once (add -- --fuzz to fuzz continuously)
zig build shared               # Build shared library used by bindings
```

## Benchmarks

```bash
zig build benchmark                        # Core operation benchmarks
zig build vector-benchmark -- --quick      # Vector benchmarks (1K/10K/100K, ~7 min)
zig build vector-benchmark                 # Full vector benchmarks including 1M (~70 min)
zig build graph-benchmark -- --quick       # Graph traversal benchmarks
zig build sqlite-benchmark                 # LatticeDB vs SQLite
zig build fts-benchmark                    # Repeated-term FTS indexing
zig build stress                           # Stress tests
zig build benchmark-build                  # Compile every benchmark without running (CI does this)
```

## Test Structure

```text
tests/
├── unit/           # Unit tests for individual modules
├── integration/    # End-to-end integration tests
├── fuzz/           # Fuzzing targets for parser and serialization
├── crash/          # Crash recovery tests (simulated crashes, see below)
├── container/      # Linux package/shared-library smoke tests
└── benchmark/      # Performance benchmarks
```

## Testing Standards

- Fuzzing is mandatory for the parser and serialization code
- Crash recovery tests simulate a crash in-process: they close the database
  cleanly, then reset the file to an earlier state (for example truncating it to
  its header page) so that reopening has to recover from the write-ahead log.
  No test kills a process, and none injects torn page writes.
- CI enforces coverage only for the Python binding (`--cov-fail-under=85`); the
  TypeScript binding runs `npm run test:coverage`. There is no Zig coverage
  tooling.

## TypeScript Binding Tests

```bash
cd bindings/typescript
npm test
```

## Python Binding Tests

```bash
cd bindings/python
uv run --extra dev pytest tests -q
```

## Go, Java and Conformance Tests

```bash
cd bindings/go && go test -tags repolocal ./...
cd bindings/java && mvn test
cd conformance/go && go test ./...   # needs PKG_CONFIG_PATH pointing at an installed lattice.pc
```

## Release Checks

Before tagging a release, validate version consistency and the binding smoke
paths:

```bash
python3 scripts/bump_version.py --check <version> --strict-lockfile
zig build test
zig build integration-test
zig build shared
cd bindings/typescript && npm test -- --runInBand
```
