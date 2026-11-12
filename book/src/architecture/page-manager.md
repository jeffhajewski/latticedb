# Page Manager

## What It Is

The Page Manager handles the lowest level of database storage: fixed-size pages. It manages the database file structure, page allocation/deallocation, and data integrity through checksums.

## Why Fixed-Size Pages?

### Simplicity

Every piece of data in the database file lives in a fixed-size page. The size is
4096 bytes by default and is chosen when the database is created (anything from
4096 to 65535 bytes, through `page_size` in the open options or the C API). It is
stored in the file header and fixed for the life of the file. This uniformity
simplifies everything:

<img class="diagram" src="../assets/diagrams/page-manager-file-layout.svg"
     alt="The database file as a row of fixed-size pages, shown at the default 4 KB: page 0 is the header at offset 0, pages 1 and 2 hold B+Tree data at offsets 4096 and 8192, page 3 is free at offset 12288">

To read page N: `offset = N * page_size`

### Alignment with Hardware

- **SSD pages**: Modern SSDs have 4KB or 8KB internal pages
- **OS pages**: Most operating systems use 4KB virtual memory pages
- **Disk sectors**: Traditional HDDs use 512B or 4KB sectors

4KB aligns well with all of these, which in general enables:
- Direct I/O (bypassing OS cache)
- Atomic writes on some hardware
- Efficient memory mapping

LatticeDB uses neither direct I/O nor memory mapping today; pages are read and
written through the [VFS](./vfs.md) into the buffer pool.

### Efficient Caching

Fixed-size pages make the buffer pool trivial - every frame is the same size, no fragmentation.

## File Structure

### Page 0: File Header

The first page is special - it contains metadata about the database. It is the
`FileHeader` extern struct from `src/storage/page.zig` (232 bytes, all integers
little-endian); the rest of page 0 is zero.

| Offset | Size | Field |
|--------|------|-------|
| 0–3 | 4 B | Magic number — `0x4C544442` (`"LTDB"`) |
| 4–5 | 2 B | Format version (currently 4) |
| 6–7 | 2 B | Minimum reader version |
| 8–11 | 4 B | Page size |
| 12–15 | 4 B | Flags |
| 16–23 | 8 B | Node count |
| 24–31 | 8 B | Edge count |
| 32–35 | 4 B | Legacy B+Tree root page (unused, kept for compatibility) |
| 36–39 | 4 B | Vector segment page |
| 40–43 | 4 B | FTS segment page |
| 44–47 | 4 B | Freelist head page |
| 48–51 | 4 B | Schema page |
| 52–115 | 64 B | `tree_roots[16]` — root page of each B+Tree |
| 116–119 | 4 B | Padding |
| 120–127 | 8 B | WAL frame count |
| 128–131 | 4 B | `checkpoint_seq` — how many times the WAL has been reset |
| 132–135 | 4 B | Reserved |
| 136–151 | 16 B | File UUID |
| 152–159 | 8 B | Created timestamp |
| 160–167 | 8 B | Modified timestamp |
| 168–199 | 32 B | Application id |

The first 200 bytes are the header 0.16.0 knows. Format 4 added the fields after
them, behind a magic number of their own:

| Offset | Size | Field |
|--------|------|-------|
| 200–203 | 4 B | Extension magic — `0x3458544C` (`"LTX4"`); anything else means the fields below are absent |
| 204–205 | 2 B | Vector dimensions; 0 when no vector index is recorded |
| 206 | 1 B | Vector metric: 1 cosine, 2 euclidean, 3 dot product |
| 207 | 1 B | Reserved |
| 208–215 | 8 B | `next_node_id`, the id the next node created gets; 0 when absent |
| 216–217 | 2 B | HNSW `m`, connections per node |
| 218–219 | 2 B | HNSW `m_max0`, connections on layer 0 (twice `m`) |
| 220–221 | 2 B | HNSW `ef_construction` |
| 222–223 | 2 B | Minimum writer version; 0 means no restriction |
| 224–225 | 2 B | Minimum reader reasons: what raised the minimum reader version (see below) |
| 226–231 | 6 B | Reserved |

0.16.0 copies only its own 200 bytes and writes the rest of page 0 as zeros, so after
0.16.0 has written a file the extension magic no longer matches and the fields are
ignored: the next open by this release finds the node id counter with one scan of
the node tree, reads the vector configuration from the vector pages, and writes both
back. That lets a file go back and forth between the two releases without either one
trusting a value the other did not keep up to date, for as long as 0.16.0 may open it
at all: see [Format Versions](#format-versions) for what keeps it out.

Key fields:

- **Magic number**: Identifies this as a Lattice database file. Opening a random file will fail fast.
- **Format version, minimum reader and minimum writer version**: see
  [Format Versions](#format-versions) below
- **Freelist head**: Points to the first free page (for allocation)
- **Tree roots**: The root page of each of the engine's B+Trees (nodes, edges,
  labels, symbols, FTS, HNSW, streams, property indexes), indexed by `TreeIndex`.
  The database copies the current roots into the in-memory header before every
  flush, so the file's header always names the roots of the pages written with it.
- **Checkpoint sequence**: Incremented by every checkpoint that truncates the WAL,
  so anything following the log can tell frame 40 of one run from frame 40 of the next
- **File UUID**: Unique identifier, used to match WAL files
- **Node id counter**: written with every flush, in the same header image as the
  node tree pages it describes, so opening needs no scan and a committed node's id
  is never handed out again, even after the node is deleted. Ids handed out by a
  transaction that rolled back, or that had not committed when the process died,
  can be handed out again.
- **Vector index configuration**: dimensions, metric and HNSW parameters, recorded
  from the moment the index exists. A database that has a vector index opens with
  it whatever the open options say, and options that contradict it are refused
  with `VectorConfigMismatch` before anything is read. An index added to an existing
  file is written to the header at that open, so the log can never hold vector
  records for an index the file does not record.

The header page is written raw: unlike every other page it has no page header and
no checksum. Between flushes it changes only in memory; it reaches the file as the
first page of each journaled flush (see
[Buffer Pool](./buffer-pool.md#writing-pages-back)).

### Data Pages

Every other page starts with a common header:

| Offset | Size | Field |
|--------|------|-------|
| 0 | 1 B | Page type — `btree_internal`, `btree_leaf`, and so on |
| 1 | 1 B | Flags |
| 2–3 | 2 B | Reserved |
| 4–7 | 4 B | Checksum — CRC32C of bytes 8 to the end of the page |
| 8 → | page size − 8 | Page-specific data |

## Page Types

```zig
pub const PageType = enum(u8) {
    free = 0x00,              // Unallocated, on freelist
    btree_internal = 0x01,    // B+Tree internal node
    btree_leaf = 0x02,        // B+Tree leaf node
    overflow = 0x03,          // Large value overflow
    vector_data = 0x04,       // Stored vectors
    hnsw_layer = 0x05,        // HNSW neighbour lists
    fts_dictionary = 0x06,
    fts_posting = 0x07,       // FTS posting lists
    freelist = 0x08,
    schema = 0x09,
    segment_directory = 0x0A,
};
```

## Page Allocation

### The Freelist

Free pages are linked together in a freelist:

<img class="diagram" src="../assets/diagrams/page-manager-freelist.svg"
     alt="The file header holds freelist head 5. Page 5 points to page 3, page 3 points to page 9, and page 9 has next 0, marking the end of the list">

### Allocating a Page

The snippets in this section are simplified from `src/storage/page_manager.zig`:
`allocPage` and `allocZeroedPage` stand in for the page-sized, 4096-aligned buffer
the real code allocates (and frees) inline, and error mapping is left out.

```zig
pub fn allocatePage(self: *Self) !PageId {
    if (self.read_only) return error.PermissionDenied;

    // 1. A page freed since the last flush: the file has not heard it was freed
    if (self.deferred_frees.pop()) |page_id| return page_id;

    // 2. The freelist
    if (self.header.freelist_page != NULL_PAGE) {
        return self.allocateFromFreelist();
    }

    // 3. Extend the file with a zeroed page (type free, checksum 0). This only
    //    makes the file longer; nothing in the file refers to the page until
    //    a flush writes the pages that use it.
    const page_id = self.pageCount();
    const zeros = try allocZeroedPage(self.page_size);
    const header: *PageHeader = @ptrCast(zeros.ptr);
    header.* = PageHeader.init(.free);
    try self.file.write(self.pageOffset(page_id), zeros);

    return page_id;
}
```

The file header is not rewritten here. A file whose last page was cut short by a
crash during such an extension is still opened; the partial page is not counted.

### Allocating from Freelist

```zig
fn allocateFromFreelist(self: *Self) !PageId {
    const page_id = self.header.freelist_page;

    // The head must be inside the file, read back with a valid checksum, and be
    // a free page. Otherwise the freelist is dropped and the page comes from the
    // end of the file instead.
    if (page_id >= self.pageCount()) return self.dropFreelistAndExtend();
    const buf = try allocPage(self.page_size);
    self.readPage(page_id, buf) catch |err| switch (err) {
        error.ChecksumMismatch => return self.dropFreelistAndExtend(),
        else => return err,
    };
    if (pageType(buf) != .free) return self.dropFreelistAndExtend();

    // Next pointer is stored after the 8-byte header; one that points at the
    // page itself or past the end of the file ends the list
    var next_free = std.mem.readInt(u32, buf[8..12], .little);
    if (next_free == page_id or next_free >= self.pageCount()) next_free = NULL_PAGE;

    // Update the freelist head, in memory; the next flush writes the header
    self.header.freelist_page = next_free;
    self.header_dirty = true;

    return page_id;
}
```

Checking the head matters because a stale or damaged header could otherwise hand
out a live page, giving it two owners. Dropping the list leaks the pages on it
(`lattice compact` rebuilds the list from page types) rather than corrupt a tree.

### Freeing a Page

Freeing writes nothing to the file. The page joins a list of deferred frees:

```zig
pub fn freePage(self: *Self, page_id: PageId) !void {
    // Can't free the header page, a page past the end of the file, the current
    // freelist head, or a page already freed since the last flush
    if (page_id == 0 or page_id >= self.pageCount()) return error.InvalidPageId;
    if (page_id == self.header.freelist_page) return error.InvalidPageId;
    if (self.deferred_free_set.contains(page_id)) return error.InvalidPageId;

    try self.deferred_free_set.put(page_id, {});
    try self.deferred_frees.append(page_id);
}
```

The buffer pool gives the freed page a blank free-page image in memory, so whoever
is handed it next finds a free page, not what its last owner left. At the next
flush the deferred pages are formatted as freelist pages, each linking to the next
and the last to the current head, and written with the other dirty pages and the
header. Writing a freelist link into a page the moment it was freed was the other
way a page could reach the file on its own, over a page the file's image still
used; that no longer happens.

The freelist is LIFO (a stack): the most recently freed pages are handed out first.

### Physical Tail Reclamation

Normal allocation reuses freelist pages without moving live pages, so file size
does not need to grow again after deletes. To return space to the operating
system, `lattice compact <path>` flushes and evicts the buffer pool, rebuilds
the retained freelist, persists a head that excludes tail pages, and truncates
the contiguous free run at physical EOF. Live pages and logical IDs never move.

The header is synced before truncation. A crash between those steps can leave
temporarily unreclaimed free pages, but cannot leave a freelist pointer beyond
the new end of the file.

## Checksums

Every page except the header page carries a CRC32C (Castagnoli) checksum in bytes
4–7 of its page header. It covers bytes 8 to the end of the page — everything after
the page header — so the page type and flags in bytes 0–1 are not covered.

### Why Checksums?

1. **Detect corruption**: Hardware failures, cosmic rays, bugs
2. **Detect torn writes**: Partial page writes from crashes
3. **Validate reads**: Catch errors before using bad data

0.16.0 extended a file with pages of zeros, checksum 0 included, and a crash can
leave such a page behind. A file this release creates says in its header that it
never does (flag `SEALED_PAGES`, 0x20): the free page it extends the file with is
written with its checksum, so every page in the file carries one, and any page whose
checksum does not match fails verification, a page of zeros included. A B+Tree or
overflow pointer to a page that a fault turned to zeros is then an error, not an
empty free page.

A file without the flag, one that 0.16.0 created, keeps the older rule: a stored
checksum of 0 is accepted only for a page that is exactly what 0.16.0's allocation
wrote, the free page type and every other byte zero. Any other page with a checksum
of 0 fails, which catches damage that zeroed the first sector of a written page. A
page that is entirely zero still passes there. 0.16.0 copies the header's flags when
it writes the header but extends the file with unsealed pages, so the flag counts
only while the format-4 header fields are present, and a read-write open that finds
them gone clears it. Only a new file gets the flag: no operation rewrites every page
of an existing one, so an older file gains it by export and import into a new
database.

Scans over every page, which meet pages that nothing refers to (`lattice check`,
truncating the free tail in `compact`), still take an all-zero page for a blank one
(`readPageForScan`); `lattice check` counts them. The header page has no checksum
at all (the journal checksums its copy of it during a flush).

### Checksum Calculation

```zig
pub fn calculateChecksum(data: []const u8) u32 {
    return crc32c.hash(data); // CRC32C (Castagnoli)
}
```

`src/storage/crc32c.zig` uses the CPU's CRC-32C instruction where there is one
(SSE4.2 on x86_64, the CRC extension on aarch64), chosen at run time because release
binaries are built for baseline CPUs, and a slicing-by-8 table walk otherwise. The
result is bit-identical to the byte-at-a-time `std.hash.crc.Crc32Iscsi` that earlier
releases used, so files and logs verify in both directions.

The buffer pool stamps the checksum when it seals a page for a flush, so the same
bytes go to the journal and then in place. `writePage`, below, does the same for
the page manager used on its own.

### Writing with Checksum

```zig
pub fn writePage(self: *Self, page_id: PageId, buf: []u8) !void {
    if (self.read_only) return error.PermissionDenied;
    if (buf.len != self.page_size) return error.InvalidPageId;
    if (page_id == 0) return error.InvalidPageId; // the header is not a data page

    // Calculate checksum of everything after the 8-byte page header
    const header: *PageHeader = @ptrCast(@alignCast(buf.ptr));
    header.checksum = calculateChecksum(buf[8..]);

    // Write to disk
    try self.file.write(self.pageOffset(page_id), buf);
}
```

### Reading with Verification

```zig
pub fn readPage(self: *Self, page_id: PageId, buf: []u8) !void {
    try self.readPageRaw(page_id, buf); // reads page_size bytes at pageOffset(page_id)

    // Verify checksum
    const header: *const PageHeader = @ptrCast(@alignCast(buf.ptr));
    const expected = calculateChecksum(buf[8..]);

    // In a file whose pages are all sealed, any mismatch is damage. Otherwise
    // one passes only for a page as 0.16.0's allocation wrote it: type free,
    // every other byte zero (checksum 0 included)
    if (header.checksum != expected) {
        if (self.header.pagesSealed() or !isUnsealedFreePage(buf)) {
            return error.ChecksumMismatch;
        }
    }
}
```

The extra check runs only when the checksum does not match, so a read that verifies
costs what it always did. Every page except one freshly allocated by 0.16.0 has been
sealed with its checksum by every release, and only files this release creates
refuse an unsealed one, so no file that opened before is refused.

## Read-Only Mode

The Page Manager supports opening databases read-only:

```zig
var pm = try PageManager.init(allocator, vfs, "db.file", .{
    .read_only = true,
});

// This fails:
_ = try pm.allocatePage();  // error.PermissionDenied
try pm.writePage(1, &buf);  // error.PermissionDenied
```

Useful for:
- Backup tools
- Read replicas
- Forensic analysis

## Finishing an Interrupted Flush

When it opens a file, before reading the header, the page manager looks for
`<database>-journal`. If a flush was interrupted, the journal holds every page it
was writing; a complete journal for this database is written in place and synced,
and an incomplete one is discarded. A read-only open of the database applies the
journal to an in-memory overlay of the file instead, and writes nothing. See
[Recovery](./recovery.md#finishing-an-interrupted-flush).

## File Locking

The page manager locks the file before it reads anything. A read-write open takes
an exclusive lock and a read-only open takes a shared one, using `tryLock`, so a
conflicting holder makes the open fail at once with `DatabaseLocked` rather than
wait. A reader is therefore refused while another process has the database open
for writing. The `lock` option turns this off for filesystems where locking does
not work; it does not make cross-process sharing safe.

## Format Versions

The header has two gates for older releases:

- **Minimum reader version.** A release whose format version is lower may not open
  the file at all; the page manager refuses it with `VersionTooNew`
  (`LATTICE_ERROR_VERSION_MISMATCH`, -11, through the C API; the CLI says the file
  was written by a newer LatticeDB). A log whose version is newer than this release
  knows is refused the same way.
- **Minimum writer version.** A release whose format version is lower may open the
  file read-only but not for writing. A writable open is refused with
  `NeedsNewerWriter` (`LATTICE_ERROR_NEEDS_NEWER_WRITER`) before anything is
  written; a read-only open works. 0 means no restriction.

This release is format 4. A read-write open of an older file sets the format version
to 4 and writes the format-4 fields; it never lowers either gate, and raises the
minimum writer version to 4. The minimum reader version stays 3, so 0.16.0 can still
open the file, until the file holds something 0.16.0 would misread rather than
refuse. Then it becomes 4, and the bit for the reason is set in the minimum reader
reasons:

| Bit | Reason | Raised when |
|-----|--------|-------------|
| `0x01` | vector index settings | the vector index uses a metric other than cosine or an `m` other than 16 (`ef_construction` only affects how new vectors are inserted, so it does not count) |
| `0x02` | ordered property index | a property index is declared; 0.16.0 would take its keys for digests and find nothing |
| `0x04` | changefeed events | a commit records its first changefeed event; 0.16.0's `changes()` would skip events in this release's format without an error, and its trim would leave them |
| `0x08` | full-text positions | a full-text index that stores word positions takes its first document; 0.16.0 would append posting entries without a position count and garble the page |

Opening never raises it: a read-only open changes nothing, and a read-write open that
writes nothing leaves the version at 3. A changefeed with retention `off` records no
events, so it does not raise it either. Nothing lowers it again, not even a trim that
drops every event, because the file cannot tell that no copy of an event survives in
a log or a backup taken in between. `lattice info` and `lattice check` print the
version and the reasons.

The changefeed's raise is the one that happens in the middle of ordinary writing, so
it is written ahead of everything else: the commit that stores the first event writes
the two fields in place in page 0 and syncs the file before it logs the commit, or,
without a log, before it writes a page. Only those two fields are written, because
the header in memory runs ahead of the pages in the file (tree roots, the freelist)
until the next flush; that flush writes the whole header with the same values. So
every state a crash can leave holds either no durable event or a header that already
refuses 0.16.0. Replaying a log's changefeed records raises the version again in the
header the replay's flushes write, in case a stale journal put an older header back.
The other three raise the header in memory, and it reaches the file no later than
what they describe: in the same flush for a vector index and for a full-text index's
first postings, and before the first entry for a property index.

0.16.0 predates the minimum writer version: it opens and writes a file that asks for
a newer writer, and zeroes the field as it writes the header. So a later feature that
must keep 0.16.0 from writing a file has to raise the minimum reader version as well.

## Error Handling

```zig
pub const PageManagerError = error{
    InvalidHeader,      // File header is malformed
    InvalidMagic,       // Not a Lattice database file
    InvalidPageSize,    // Page size outside 4096..65535
    VersionTooNew,      // A newer reader is needed (min_reader_version)
    WriterVersionTooNew, // Writable open of a file that needs a newer writer
    ChecksumMismatch,   // Data corruption detected
    InvalidPageId,      // Page ID out of range
    PageNotAllocated,   // Accessing unallocated page
    IoError,            // Underlying I/O failed
    PermissionDenied,   // Write to read-only database
    OutOfMemory,        // Page buffer allocation failed
    Unexpected,
    FileNotFound,       // Database file doesn't exist
    DiskFull,           // No space for new pages
    DatabaseLocked,     // Another process holds a conflicting lock
};
```

## Thread Safety

The Page Manager itself is NOT thread-safe (only its page count is atomic, so
readers can check page ids while a writer allocates). Each operation directly
touches the file. In a multi-threaded context, the Buffer Pool sits in front of it:

1. Caching pages in memory
2. Using latches (reader-writer locks) per frame
3. Guarding its page table with a mutex

Writes are serialised above both layers: a database allows one read-write
transaction at a time, and the C API serialises the calls on a handle.

## Usage Pattern

```zig
// Open or create database
var pm = try PageManager.init(allocator, vfs, "my.db", .{
    .create = true,
    .page_size = 4096,
});
defer pm.deinit();

// Allocate a page
const page_id = try pm.allocatePage();

// Write data
var buf: [4096]u8 align(4096) = undefined;
const header: *PageHeader = @ptrCast(&buf);
header.* = PageHeader.init(.btree_leaf);
// ... fill in page data ...
try pm.writePage(page_id, &buf);

// Read it back
var read_buf: [4096]u8 align(4096) = undefined;
try pm.readPage(page_id, &read_buf);

// Free the page (deferred until the next flush)
try pm.freePage(page_id);

// Without a buffer pool to flush through, write the deferred frees and the
// header directly (not atomic), then make it durable
try pm.persistDeferredFrees();
try pm.sync();
```

## Key Design Decisions

### Fixed-Size Pages, 4KB by Default

4KB is the default for hardware alignment. The size can be raised at creation (up
to 65535 bytes), for example to fit larger values inline; it is stored in the
header and the same for every page in the file.

### Freelist in Pages

The freelist uses the free pages themselves for storage. No external data structure needed. Elegant and space-efficient.

### Checksum Covers the Body, Not the Header

The checksum doesn't cover itself (circular dependency). It sits in bytes 4–7 and
covers bytes 8 onward, the whole page after its 8-byte header, in one pass. The
price of that choice is that the page type and flags in bytes 0–1 are unprotected.

### No Internal Fragmentation Tracking

We don't track free space within pages - that's the responsibility of higher layers (B+Tree). The Page Manager only deals with whole pages.
