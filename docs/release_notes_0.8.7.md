# LatticeDB 0.8.7 Release Notes

## Summary

`0.8.7` is a correctness patch for large transactional property writes and a
small CLI usability release.

The release fixes the first-write failure seen by LongMemEval/Quipu-style
workloads that create a node with multiple properties such as `qid` and a large
`properties_json` blob. The failure was previously surfaced through the public
C API as `LATTICE_ERROR_FULL`, which made a deterministic value/page-size limit
look like buffer-pool exhaustion.

## Highlights

### Large transactional property writes

LatticeDB stores each node record as a single B+Tree value. With the default
4 KiB page size, a large `properties_json` value can still exceed the maximum
single-leaf record size. That condition now returns the dedicated
`LATTICE_ERROR_VALUE_TOO_LARGE` code instead of `LATTICE_ERROR_FULL`.

Transactional node state is now validated before commit when staged property
changes would produce an oversized node record. The regression test verifies the
rejected write leaves zero pinned buffer-pool frames, confirming this path is
not another retained-pin leak.

### WAL frame size follows database page size

Databases created with a larger page size now create WAL frames with the same
frame size. This allows larger-page databases, such as `page_size = 32768`, to
commit larger transactional property updates instead of being limited by the old
fixed 4 KiB WAL frame.

WAL iteration and crash recovery now read the frame size recorded in the WAL
header, so recovery works for both existing 4 KiB WALs and newly created
larger-frame WALs.

### New public error code

The C API adds:

```c
LATTICE_ERROR_VALUE_TOO_LARGE // -15
```

Python, TypeScript, and Go bindings were updated to expose the new code. Python
also exposes `LatticeValueTooLargeError`.

### CLI update command

The `lattice` CLI now includes:

```bash
lattice update
```

The command runs the public installer sequence:

```bash
curl -fsSL https://raw.githubusercontent.com/jeffhajewski/latticedb/main/dist/install.sh | bash
```

## Upgrade Notes

There is no database file-format bump.

Existing databases continue to open normally. The default 4 KiB page size still
cannot store arbitrarily large single-node property blobs; applications that
need large JSON properties should create the database with a larger `page_size`,
for example `32768`.

If a larger-page database writes larger WAL frames with `0.8.7`, use `0.8.7` or
newer for crash recovery of that WAL.

## Validation Notes

Release validation included:

```bash
zig build test --summary all
zig build integration-test --summary all
zig build crash-test --summary all
zig build --summary all
PYTHONPATH=bindings/python/src python3 -m pytest bindings/python/tests/test_basic.py -q
npm --prefix bindings/typescript run build
PKG_CONFIG_PATH="$PWD/zig-out/lib/pkgconfig" \
  DYLD_LIBRARY_PATH="$PWD/zig-out/lib" \
  go test ./...
git diff --check
```

The Zig unit, integration, and crash recovery suites completed successfully. The
unit test runs emitted the existing FTS reverse-index warnings.
