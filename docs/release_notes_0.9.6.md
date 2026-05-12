# LatticeDB 0.9.6 Release Notes

## Summary

`0.9.6` is an API ergonomics and storage-maintenance patch release. It adds
bounded typed edge traversal, stream publish sequence introspection, an explicit
adjacency-cache open option, and transaction-scoped all-node enumeration while
keeping existing APIs source-compatible.

The release also truncates checkpointed WAL files on close so reopened
databases do not keep scanning frames that are already durable in the data file.

## Highlights

### Bounded Typed Edge Traversal

The C API now supports typed traversal with a caller-specified limit:

```c
lattice_edge_get_outgoing_by_type(txn, node_id, "KNOWS", limit, &edges);
lattice_edge_get_incoming_by_type(txn, node_id, "KNOWS", limit, &edges);
```

`limit == 0` means unlimited. Limits are applied while collecting visible edge
references, before edge type strings are materialized for the public result
handle.

`lattice_edge_scan(txn, edge_type_or_null, limit, &edges)` is available for
administrative work such as index rebuilds, consistency checks, and export jobs.
It is intentionally documented as a scan, not a hot-path graph expansion API.

Python, TypeScript, and Go bindings expose matching typed traversal helpers:

- Python: `get_outgoing_edges_by_type`, `get_incoming_edges_by_type`
- TypeScript: `getOutgoingEdgesByType`, `getIncomingEdgesByType`
- Go: `GetOutgoingEdgesByType`, `GetIncomingEdgesByType`

### Stream Publish Sequence APIs

Publish callers can now retrieve the stream sequence assigned inside the active
transaction:

```c
lattice_stream_publish_get_sequence(txn, stream, stream_len, kind, kind_len, payload, &sequence);
```

The existing `lattice_stream_publish` API remains source-compatible and ignores
the returned sequence. The assigned sequence is durable only after commit; a
rolled-back transaction does not make the record visible.

Bindings expose the same capability:

- Python: `publish_stream_get_sequence`
- TypeScript: `publishStreamGetSequence`
- Go: `PublishStreamGetSequence`

`lattice_stream_get_last_sequence` is available as read-side introspection for
the last durable sequence in a stream.

### Adjacency Cache Open Option

The C API adds `lattice_open_options_v3` and `lattice_open_v3` with
`enable_adjacency_cache`. Existing `lattice_open` and `lattice_open_v2`
behavior is unchanged.

Bindings now expose the option as:

- Python: `enable_adjacency_cache`
- TypeScript: `enableAdjacencyCache`
- Go: `EnableAdjacencyCache`

### Transaction All-Nodes Snapshot

`lattice_get_all_nodes_txn` returns every node visible to a transaction
snapshot. It complements label-scoped transaction reads and keeps overlapping
readers isolated from uncommitted writer changes.

### WAL Truncation After Checkpoint

Close now syncs and checkpoints before releasing the database handle, then
truncates the WAL to a header-only file after a full checkpoint. Future writes
continue with monotonic LSNs, while recovery no longer scans checkpointed frames
that are already durable.

## API Notes

This release adds public API surface in C, Python, TypeScript, and Go. Existing
unfiltered edge traversal and stream publish APIs are unchanged.

Property lookup/index work is intentionally design-only in this release. See
`docs/property_index_design.md` for the planned explicit property-index and
scan-naming model. No scan-backed property lookup convenience API was added.

## Upgrade Notes

This is a patch release with no database file-format bump beyond the format 3
layout introduced in `0.9.0`.

Applications that want bounded graph expansion should prefer the typed traversal
APIs over client-side filtering of unbounded edge result sets. Use
`lattice_edge_scan` only for administrative and rebuild workflows.

Applications that need stream cursors immediately after publish can adopt the
new sequence-returning publish APIs without changing the existing commit model.

## Validation Notes

Validation for this release included:

```bash
zig build test
zig build
python3 -m compileall -q bindings/python/src/latticedb bindings/python/tests
python3 -m pytest bindings/python/tests
cd bindings/typescript && npm run build
cd bindings/typescript && npm test -- --runInBand
cd bindings/go && go test -count=1 -tags repolocal ./...
python3 scripts/bump_version.py --check 0.9.6 --strict-lockfile
```

Regression coverage includes typed outgoing/incoming edge traversal with limits,
native edge scans with and without type filters, stream sequence assignment and
rollback behavior, `lattice_open_v3` ABI layout, adjacency-cache traversal, and
transaction-scoped all-node visibility.
