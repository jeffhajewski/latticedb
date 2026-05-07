# LatticeDB 0.8.6 Release Notes

## Summary

`0.8.6` is a correctness patch for buffer-pool pin cleanup under write-heavy
FTS indexing workloads.

The release fixes a posting-list append path that could keep each full posting
page pinned while recursively appending to the overflow tail. Repeated-term FTS
indexing could therefore create high transient pin pressure and exhaust small
or busy buffer pools even though the append ultimately succeeded.

## Highlights

### FTS posting append pin cleanup

Posting-list append now releases a full page before moving to the next page in
the overflow chain. When a new overflow page is linked, the parent posting page
is marked dirty and unpinned before the append continues on the new tail page.

The path also tracks whether the current frame is still pinned and whether it
was dirtied, so error cleanup continues to call `unpinPage()` with the correct
dirty flag.

### Clock eviction second-chance fix

The buffer-pool clock eviction loop no longer reports `BufferPoolFull` after one
rotation just because it encountered a pinned frame. It now lets the existing
two-rotation pass clear second-chance bits and evict an available unpinned frame
when one exists.

### Regression coverage

New regression tests cover both sides of the failure mode:

- a posting-store test appends position-heavy entries through a two-frame buffer
  pool and verifies every successful append leaves zero pinned frames
- a buffer-pool test verifies a pinned frame does not hide another frame that is
  evictable after its second chance is consumed

## Upgrade Notes

This is intended as a patch release. There is no database file-format change.

Existing databases continue to open normally. Applications with repeated-term
FTS writes or other write-heavy workloads that saw buffer-pool exhaustion should
upgrade to `0.8.6`.

## Validation Notes

Release validation included:

```bash
zig build test
zig build integration-test
```

The Zig unit and integration test suites completed successfully. The unit test
run emitted the existing FTS reverse-index warnings.
