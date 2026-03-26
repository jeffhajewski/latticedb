# Go Conformance Suite

This module contains the first extracted black-box conformance cases from the current LatticeDB integration tests.

It currently runs against the Go cgo binding through a small adapter, but the suite is written against a local engine interface so a future `latticedb-go` implementation can reuse the same tests without depending on the current cgo-backed package internally.

## Current Coverage

The initial suite covers:

- persistence across reopen
- stable monotonic edge identity across rollback and reopen
- nested value round-trips
- missing versus stored `NULL` semantics in direct property APIs
- read-only rejection, own-write visibility, commit visibility to new transactions, and rollback cleanup
- rollback behavior
- query mutation atomicity
- query `SET ... = null` removal semantics
- parallel-edge targeting via stable edge ID
- direct vector search and full-text search
- vector and full-text query operators preserving additional `MATCH` bindings
- query cache management behavior
- crash recovery for committed graph state, committed node-property updates, and aborted tail inserts through an adapter-provided recovery harness
- export and dump invariants through the public CLI surface

## Running

Build the shared library from the repo root first:

```bash
zig build shared
```

Then run the suite:

```bash
cd conformance/go
go test ./...
```

The export and dump cases build the `lattice` CLI on demand during the test run.
The crash cases use a recovery harness adapter; the current `lattice` adapter simulates a crash by resetting the main database file and replaying the WAL on reopen.

## Future Direction

The current adapters live in:

- `driver_latticedb_test.go` for the public database surface
- `exporter_test.go` for export/dump behavior
- `recovery_test.go` for crash simulation

A future pure-Go engine should be able to run this suite by adding adapters that satisfy the same local interfaces.
