# LatticeDB 0.8.5 Release Notes

## Summary

`0.8.5` is a correctness patch for B+Tree page lifecycle handling during
transactional commits.

The release fixes an internal-page split error path where a page fetched from
the buffer pool could remain pinned if an allocation failed while restructuring
a B+Tree. That could leave `pin_count` elevated after `lattice_commit` touched
multiple indexes such as node storage, label indexes, FTS, and vector indexes.

## Highlights

### B+Tree internal split pin cleanup

Internal B+Tree splits now consistently unpin both the original internal page
and the newly allocated split page on allocation failures. The cleanup tracks
whether each frame is still pinned and whether it was dirtied before the error
occurred, so `unpinPage()` receives the correct dirty flag.

If fetching the newly allocated internal page fails, the page manager now
returns that page to the freelist before propagating the buffer-pool error.

### Commit-path regression coverage

New tests cover both the low-level and database-level failure surfaces:

- a B+Tree unit regression forces an out-of-memory error during an internal
  split and verifies the buffer pool returns to zero pinned frames
- a transactional commit regression stages node, label, FTS, and vector index
  writes in one transaction and verifies commit leaves no pinned pages

## Upgrade Notes

This is intended as a patch release. There is no database file-format change.

Existing databases continue to open normally. Applications that saw buffer pool
pressure or pinned-page exhaustion during write-heavy commits should upgrade to
`0.8.5`.

## Validation Notes

Release validation included:

```bash
ZIG_GLOBAL_CACHE_DIR=.zig-cache/global zig build test --summary all
cd bindings/python && UV_CACHE_DIR=../../.zig-cache/uv uv run --extra dev pytest tests -q
cd bindings/typescript && npm test -- --runInBand
```

The Zig test run completed with `710/715` tests passing and `5` skipped tests.
The Python binding tests completed with `114` passing tests. The TypeScript
binding tests completed with `84` passing tests.
