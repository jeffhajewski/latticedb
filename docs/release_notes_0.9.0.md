# LatticeDB 0.9.0 Release Notes

## Summary

`0.9.0` adds generic B-tree overflow values, allowing large graph properties to
round-trip on the default 4 KiB page size.

The headline behavior change is that 100 KB and 1 MB string properties no
longer need a larger page size. They are stored out of the leaf page in B-tree
overflow pages and are materialized transparently by point reads, range scans,
transactions, and crash recovery.

## Highlights

### B-tree overflow values

Small values keep the existing inline leaf-entry layout. Large values now store
a compact overflow descriptor in the leaf entry, plus a local prefix for split
and scan locality. The rest of the value is stored in linked overflow pages.

Overflow pages are written before the publishing leaf entry is updated. Deletes
free the referenced overflow chain, and leaf splits copy overflow descriptors
without taking ownership of their chains. This keeps insert, split, delete, and
rollback cleanup paths from leaking or prematurely freeing large-value storage.

The maximum serialized B-tree value is now 64 MiB. Values above that cap still
return `ValueTooLarge`.

### Default-page large graph properties

The graph node and edge storage paths no longer reject records simply because a
serialized property blob does not fit in one 4 KiB leaf entry. Large string
properties now round-trip through the same B-tree APIs as small properties.

This includes autocommit writes, explicit transactions, rollback, delete,
large-to-small updates, reopen persistence, and WAL crash recovery.

### WAL large-record support

The WAL format is bumped to version 2. Logical records that are too large for a
single physical WAL payload are fragmented into chunked physical records and
reassembled during recovery.

Existing version 1 WALs remain readable. Recovery ignores incomplete tail
fragments, preserving the existing truncated-tail behavior for uncommitted or
partially written records.

### File format version 3

New databases are created with file format version 3. Existing version 2
databases still open read-only without modification. Opening a version 2
database read-write upgrades the file header to version 3 before new writes.

After a database is upgraded or writes overflow values, use `0.9.0` or newer to
open it.

### CLI updater fix

The `lattice update` path now uses the installer in install mode, matching the
public install flow and avoiding the updater mode bug fixed for this release.

## Upgrade Notes

This release includes a database file-format bump from version 2 to version 3.
Back up production databases before opening them read-write with `0.9.0`.

Applications that previously increased `page_size` only to fit large node or
edge property values can usually return to the default 4 KiB page size for new
databases. The per-value cap is 64 MiB; larger application-level blob storage is
still out of scope for this release.

## Validation Notes

Release validation included:

```bash
zig build test --summary all
zig build cli --summary all
zig build integration-test
zig build crash-test
git diff --check
go test -tags repolocal ./...
npm test -- --runInBand
.venv/bin/python -m pytest
```

The Zig unit run completed with 720 passing tests and 5 skipped tests. The Go,
TypeScript, and Python binding suites completed successfully.
