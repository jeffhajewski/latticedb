# Go Conformance Suite

This module contains the first extracted black-box conformance cases from the current LatticeDB integration tests.

It currently runs against the Go cgo binding through a small adapter, but the suite is written against a local engine interface so a future `latticedb-go` implementation can reuse the same tests without depending on the current cgo-backed package internally.

## Current Coverage

The initial suite covers:

- persistence across reopen
- stable monotonic edge identity across rollback and reopen
- nested value round-trips
- a `nil` property written through the direct property APIs storing no property (since 0.18.1)
- read-only rejection, single-writer enforcement, own-write visibility, commit visibility to newly started transactions, and rollback cleanup
- rollback behavior
- query mutation atomicity
- query `SET ... = null` removal semantics
- parallel-edge targeting via stable edge ID
- direct vector search and full-text search
- vector and full-text query operators preserving additional `MATCH` bindings
- query cache management behavior
- crash recovery for committed graph state, secondary labels, committed node-property updates, committed edge-property updates, and aborted tail inserts through an adapter-provided recovery harness
- canonical dump/export invariants through the public CLI surface

The portable contract permits one live read-write transaction per database
handle. A second writer must fail with the public lock-timeout/contention error;
read-only transactions may remain active alongside that writer.

## Running

Install the shared library, public header, and `pkg-config` metadata from the repo root:

```bash
zig build install --prefix /tmp/lattice-install -Doptimize=ReleaseSafe
```

Then point `pkg-config` at that prefix and run the suite:

```bash
export PKG_CONFIG_PATH=/tmp/lattice-install/lib/pkgconfig
export DYLD_LIBRARY_PATH=/tmp/lattice-install/lib
cd conformance/go
go test ./...
```

On Linux, use `LD_LIBRARY_PATH` instead of `DYLD_LIBRARY_PATH`.

The module still uses a local `replace` to point at `../../bindings/go` so the suite exercises the binding code in this checkout, but native linking follows the installed `pkg-config` workflow rather than the repo-local `zig-out/lib` path.

The export and dump cases build the `lattice` CLI on demand during the test run.
The crash cases use a recovery harness adapter; the current `lattice` adapter snapshots the pre-close WAL, then simulates a crash by resetting the main database file and replaying that WAL on reopen. This keeps the recovery cases meaningful even though public close may checkpoint and truncate the live WAL.

## Future Direction

The current adapters live in:

- `driver_latticedb_test.go` for the public database surface
- `exporter_test.go` for export/dump behavior
- `recovery_test.go` for crash simulation

A future pure-Go engine should be able to run this suite by adding adapters that satisfy the same local interfaces.
