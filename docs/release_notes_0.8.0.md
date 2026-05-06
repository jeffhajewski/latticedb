# LatticeDB 0.8.0 Release Notes

## Summary

`0.8.0` hardens the B+Tree storage layer against a leaf-page integer-overflow panic, adds full-fidelity JSON import/export for nested data types, and introduces first-class Go bindings for durable streams.

This release is recommended for anyone running workloads that mix large values with small keys in graph or stream writes.

## Highlights

### B+Tree leaf insertion hardening

A latent bug in the leaf-node insertion path could panic with `integer overflow` when a B+Tree page was full or when a leaf split produced halves of wildly different byte sizes.

Root cause: `insertLeafEntry` computed `new_offset = min_offset - entry_size` as a `u16` without first checking that the page had enough free space. If `min_offset < entry_size` (e.g. because a previous split did not account for actual entry sizes), the subtraction underflowed and crashed the process.

The split routine also used a naive `total / 2` split point by entry count, not by bytes. With variable-sized keys and values this could place a few large entries in one half and many small entries in the other, causing one half to exceed page capacity when entries were reinserted.

Fixes:

- `insertLeafEntry` now returns `BTreeError!void` and validates free space before writing.
- `splitLeafAndInsert` uses a new `chooseLeafSplitPoint` helper that walks entries and picks the split with the smallest byte-size imbalance while guaranteeing both halves fit in a page.
- Frame leaks on allocation failure during split were closed.
- CanFitLeafEntry and space calculations now use checked `usize` arithmetic to avoid silent overflow.

A regression test (`btree leaf split accounts for variable-sized entries`) fills a leaf with small values and then inserts a 3600-byte value to force a split, verifying that all entries remain readable afterward.

### Full-fidelity JSON import/export for nested types

JSON import and export now preserve nested property values — maps, arrays, and mixed structures — rather than flattening them to strings.

Behavior:

- Nodes and edges export nested properties as typed JSON objects and arrays.
- JSON import round-trips nested structures back into native `PropertyValue` maps.
- Comma-separated label filters are handled correctly during import.
- Dangling edges and unreadable graph data are rejected or skipped deterministically.

This makes `lattice import` and `lattice export` suitable for backing up and migrating complex graph datasets that use rich property payloads.

### Go stream bindings

The Go client now exposes ergonomic wrappers for durable streams and the graph changefeed.

API additions:

- `Transaction.PublishStream`
- `Transaction.SetStreamOffset`
- `Transaction.TrimStream`
- `Database.ReadStream`
- `Database.GetStreamOffset`
- `Database.Changes`

The binding is cgo-backed and supports both the installed workflow (via `pkg-config`) and a repo-local `repolocal` build tag for development inside this repository. Stream types and helpers are documented in [bindings/go/README.md](bindings/go/README.md).

## Upgrade Notes

### Databases

The database file format remains `2`, introduced by `0.5.0`. Existing databases open normally.

The B+Tree fix is fully backward-compatible — it only changes internal page-layout logic, not on-disk format. However, if your workload was previously tickling the overflow panic, upgrading removes that crash risk.

### Bindings

Rebuild C consumers and language bindings against the `0.8.0` headers and shared library if you use the new Go stream wrappers or JSON nested-type round-tripping.

Python and TypeScript packages include the updated import/export behavior. Deprecated compatibility aliases remain available in `0.8.0`; this release does not remove them.

## Validation Notes

Release-surface validation for this cycle included:

```bash
zig build test
zig build integration-test
zig build crash-test
zig build shared
cd bindings/python && uv run pytest tests -q
cd bindings/typescript && npm test
cd bindings/go && go test ./...
```

700+ tests passed, including the new B+Tree regression test for variable-sized entry splits.
