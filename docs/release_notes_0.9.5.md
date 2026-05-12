# LatticeDB 0.9.5 Release Notes

## Summary

`0.9.5` is a storage-correctness patch release focused on large graph property
writes, especially default 4 KiB page databases used by LongMemEval/Quipu-style
workloads. It does not add new public C, Python, TypeScript, or Go API calls.

This note also captures the related `0.9.2` through `0.9.5` patch train because
those releases were cut close together while hardening release automation,
version consistency checks, and large-value storage paths.

## Highlights

### 4 KiB B+Tree Large-Value Updates

The B+Tree now routes large inline-capable values through overflow records when
keeping the value inline would make a future leaf split impossible to divide
into two valid ordered pages. This preserves split boundaries on 4 KiB pages
while keeping point reads and range scans transparent to callers.

Deletes now compact surviving leaf entries before returning, so replacing or
reinserting large values can reuse the freed leaf payload space immediately.

### Large Node and Edge Properties

Node and edge serialization no longer depends on fixed 4 KiB stack buffers.
Records are serialized into heap buffers sized from the actual labels and
properties, then stored through the B+Tree inline/overflow path.

Edge payloads are stored once in the edge-ID index while the traversal tree
stores empty outgoing/incoming keys. Stable edge IDs remain the public handle
for edge property updates.

### Transaction and Changefeed Payload Sizing

Commit-time WAL logging avoids serializing redundant old and new large property
values for graph state changes when redo does not need both. The graph
changefeed summarizes very large `old_value` or `new_value` fields with
`__lattice_value_omitted`, `type`, and `encoded_bytes` when including the full
value would exceed internal stream/WAL sizing limits.

### Release and CI Hardening

The release workflow now verifies tracked version values before publishing. The
version check covers native source/header values, package metadata, lockfiles,
client fallback version strings, examples, and docs examples. CI also validates
the shared-library target and container coverage needed by the release path.

## API Notes

There are no new public API functions in `0.9.5`.

The documented API surface now explicitly calls out stable edge IDs in traversal
results and edge property APIs:

- C: `lattice_edge_result_get_id`, `lattice_edge_set_property`,
  `lattice_edge_get_property`, and `lattice_edge_remove_property`
- Python: `set_edge_property`, `get_edge_property`, and
  `remove_edge_property`
- TypeScript: `setEdgeProperty`, `getEdgeProperty`, and
  `removeEdgeProperty`

Durable stream and graph changefeed APIs remain the same; only oversized
changefeed value representation changed.

## Upgrade Notes

This is a patch release with no database file-format bump beyond the format 3
layout introduced in `0.9.0`.

Databases that already contain generic B+Tree overflow values should continue
to be opened with `0.9.0` or newer. Applications that previously hit
`ValueTooLarge` for large node or edge properties should rebuild bindings
against the `0.9.5` library and rerun their workload with the default 4 KiB page
configuration before increasing page size as a workaround.

`ValueTooLarge` is still the expected error when a fully serialized value or
stream record exceeds the engine's hard limits.

## Validation Notes

Validation for the patch train included:

```bash
zig build test
zig build integration-test
zig build shared
python3 scripts/bump_version.py --check 0.9.5 --strict-lockfile
```

Additional regression coverage exercises large node and edge string properties,
transactional large-property replacement, B+Tree split-boundary behavior on
4 KiB pages, leaf delete compaction, and the LongMemEval-shaped unit repro in
`tests/unit/quipu_longmemeval_repro.zig`.
