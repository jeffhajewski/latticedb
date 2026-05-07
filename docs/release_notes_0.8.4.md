# LatticeDB 0.8.4 Release Notes

## Summary

`0.8.4` is a correctness patch for databases created with non-default page
sizes, especially larger pages such as 32 KiB.

The release removes the remaining fixed 4 KiB page buffers in the page manager,
wires configurable page size through the C API and CLI create path, and returns
a clearer "value too large" error when a WAL record cannot fit in one frame.

## Highlights

### Large-page database safety

`PageManager` now sizes temporary page buffers from the database page size
instead of using fixed 4096-byte stack buffers. This prevents partial page
writes and data corruption when creating or reopening databases with larger
page sizes.

Page sizes are now validated on create and while reading existing file headers.
Invalid headers with impossible page sizes or file lengths are rejected instead
of being used for later page math.

### Configurable page size surface

The configured page size now flows through all supported open/create surfaces:

- Core Zig `OpenOptions.page_size`
- C API `lattice_open_options.page_size`
- C API `lattice_open_options_v2.page_size`
- CLI `lattice create --page-size=<bytes>`

For C compatibility, `page_size = 0` is treated as the default 4096-byte page
size. This preserves common zero-initialized option usage such as
`lattice_open_options opts = {0};`.

### Oversized WAL records report clearly

WAL `RecordTooLarge` errors now propagate through the transaction manager and
database layer as `DatabaseError.ValueTooLarge`.

C callers receive `LATTICE_ERROR_FULL`, matching the existing "cannot fit"
surface for payloads that exceed the current page/WAL record capacity.

### ABI coverage

The release adds C API ABI layout tests and container-level static assertions
for the open-options structs. This guards field offsets and struct sizes for
both `lattice_open_options` and `lattice_open_options_v2`.

## Upgrade Notes

This is intended as a patch release. There is no database file-format change.

Existing 4 KiB databases continue to open normally. Databases created with
larger page sizes should be reopened with `0.8.4` or newer to avoid the fixed
buffer corruption bug fixed in this release.

C callers that leave `page_size` as zero now get the default 4096-byte page size
instead of an invalid-argument error.

## Validation Notes

Release validation included:

```bash
zig build test --summary all
```

The run completed with `708/713` tests passing and `5` skipped tests. The Zig
unit test run emits the existing FTS reverse-index warnings during the
large-document regression tests; the command exits successfully.
