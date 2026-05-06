# LatticeDB 0.8.2 Release Notes

## Summary

`0.8.2` is a correctness patch for C API opens, WAL-backed transactions, and
durable stream writes.

Writable C API opens now either have a real WAL-backed transaction manager or
fail clearly. Stream writes no longer run on fake no-WAL transaction handles.

## Highlights

### Versioned C open options

The C API now includes a versioned open-options struct:

- `lattice_open_options_v2`
- `LATTICE_OPEN_OPTIONS_V2_DEFAULT`
- `lattice_open_v2`

The original `lattice_open_options` layout and `lattice_open` entrypoint remain
ABI-compatible. Existing callers keep the same source-level open flow, and
legacy `lattice_open` defaults to WAL enabled.

The new v2 options add:

- `struct_size`, for future-safe option validation
- `enable_wal`, defaulting to `true`

### Explicit WAL failures

Writable opens with WAL enabled now fail if WAL initialization fails. Previous
builds could silently continue without WAL, leaving the database without a real
transaction manager even though write transactions appeared to begin.

No-WAL mode is still available, but only explicitly through
`lattice_open_v2(... enable_wal = false ...)` or through read-only opens.

When creating a brand-new database, LatticeDB may replace an orphaned stale WAL
file from a previous database at the same path. Existing databases with invalid
or mismatched WAL state still fail clearly.

### Stream transaction safety

Read-write `lattice_begin` no longer fabricates an `id = 0` transaction when
transactions are unavailable. It now returns `LATTICE_ERROR_UNSUPPORTED`.

Stream mutation APIs now reject non-real transactions directly:

- `lattice_stream_publish`
- `lattice_stream_set_offset`
- `lattice_stream_trim`

Inactive stream transactions map to `LATTICE_ERROR_TXN_ABORTED` instead of
surfacing as invalid arguments.

### Binding updates

The Go, TypeScript, and Python bindings now call `lattice_open_v2` from this
source tree and expose WAL configuration:

- Go: `OpenOptions.EnableWAL` is normalized to `true`; use
  `OpenOptions.DisableWAL` to explicitly disable WAL.
- TypeScript: `enableWal?: boolean`, defaulting to `true`.
- Python: `enable_wal: bool = True`.

The TypeScript and Python development library discovery paths now prefer the
repo-local `zig-out/lib` shared library ahead of generic system installs, while
still honoring explicit environment overrides and bundled package libraries.

## Upgrade Notes

This is intended as a patch release. There is no database file-format change.

Existing C callers using `lattice_open` remain ABI-compatible and now get the
intended WAL-backed transaction behavior by default.

Applications that intentionally open with WAL disabled should expect write
transactions and stream writes to fail with `LATTICE_ERROR_UNSUPPORTED`.
Durable streams remain transaction/WAL-backed only.

Rebuild C consumers and language bindings against the `0.8.2` headers and
shared library to use `lattice_open_v2` and the new binding options.

## Validation Notes

Release-surface validation for this patch included:

```bash
zig build test
zig build integration-test
zig build shared
cd bindings/go && go test -count=1 -tags repolocal ./...
cd bindings/typescript && npm test -- --runInBand
cd bindings/typescript && npm run build
cd bindings/python && PYTHONPATH=src python3 -m pytest
```

The Zig unit test run emits the existing FTS reverse-index warning during the
large-document regression tests; the command exits successfully.
