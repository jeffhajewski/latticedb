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
├── crash/          # Crash recovery and power-loss tests (see below)
├── container/      # Linux package/shared-library smoke tests
└── benchmark/      # Performance benchmarks
```

## Testing Standards

- Fuzzing is mandatory for the parser and serialization code
- Crash recovery tests simulate a crash in-process. Some copy the database and
  its log while a handle is still open, which is what a killed process leaves;
  some reset the file to an earlier state (for example truncating it to its header
  page) so that reopening has to recover from the write-ahead log; some build the
  state a crash part way through a flush leaves. No test kills a real process.
- Power loss is tested by simulation, described below.
- CI enforces coverage only for the Python binding (`--cov-fail-under=85`); the
  TypeScript binding runs `npm run test:coverage`. There is no Zig coverage
  tooling.

## Power-Loss Tests

A process kill leaves every write the process made, synced or not. A power cut does
more: it loses what was not synced, can keep later writes while losing earlier ones,
can tear a write at a sector boundary, and can lose the name of a file whose
directory was not synced. The crash suite tests that with a simulated disk:

- `FaultVfs` (`src/storage/fault_vfs.zig`) is an in-memory filesystem that records
  every create, delete, write, truncate, file sync and directory sync.
- `DiskModel` replays a prefix of that record and cuts the power. Synced data and
  synced names survive. Each unsynced change survives in one of four styles: all of
  it, as a process kill leaves it; a prefix in order with the last write torn; any
  subset in any order; or any subset of 512-byte sectors, with truncations possibly
  lost and file lengths possibly grown with zeroes. A name created or removed since
  its directory was last synced comes back old or new at random.
- `Database.openWithVfs` (Zig API only) opens a database on any `Vfs`, treated as a
  disk, journal included, so the harness runs the same engine as a file on disk.
- `tests/crash/ldur_power.zig` runs a seeded random workload through the Zig API
  against a model of what the database should hold: labelled nodes, properties
  including values in overflow pages, edges and their deletion, node deletes, a
  property index, a full-text index, vectors, rolled-back transactions, explicit
  syncs and checkpoints, reopens, and optionally a log removed while the database
  is closed. It notes where each commit returned, then cuts the power before and
  after every sync and at random points, opens each result and checks it:
  - the contents are exactly the state after some whole number of transactions, no
    fewer than had been acknowledged at the cut (in `normal` sync mode, no fewer
    than had been acknowledged when the log was last synced), and at most one more;
  - node and edge counts agree with scans, every edge's ends exist, adjacency
    agrees with the edges, the label, property and full-text indexes agree exactly
    with the graph, and every vector is reachable by search;
  - every fifth cut is followed by a second cut during that recovery, which must
    reach the same state.

`zig build crash-test` runs five configurations: the default sync mode over the
graph and a property index; overflow values with transactions over several log
frames; `normal` sync mode; vectors with a full-text index; and a log removed while
the database was closed. Together they take about a minute and a half of CPU in a
debug build. For a longer search, environment variables control the harness:

| Variable | Effect |
|----------|--------|
| `LDUR_SEEDS=<n>` | Run each configuration at three times the size, over `n` more seeds |
| `LDUR_SEED_BASE=<n>` | First of those seeds (default 100) |
| `LDUR_SEED=<n>` | Rerun the configurations at one seed |
| `LDUR_ONLY_CUT=<n>` | Run only one cut |
| `LDUR_VERBOSE=1` | Print each cut |
| `LDUR_KEEP_GOING=1` | Count failures instead of stopping at the first |

```bash
LDUR_SEEDS=4 zig build crash-test
```

The harness does not reach everything. `lattice check`, backup, restore and
replication use the filesystem directly rather than the `Vfs`, `deserialize` writes
a temporary file directly, and the workload drives the Zig API, not the C API,
Cypher or streams.

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
