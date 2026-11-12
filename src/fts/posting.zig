//! Posting List Storage for Full-Text Search.
//!
//! Stores document IDs with term frequencies.
//! Features:
//! - Each entry is varints: the document id itself (not a delta from the one
//!   before), the term frequency, and, when the list has positions, a count
//!   followed by each position, again as itself
//! - Entries are not kept in doc id order: an update removes a document's
//!   entries and appends its new ones
//! - Optional position lists for phrase queries
//! - A skip-pointer area between the header and the entries, which releases
//!   up to 0.16 filled every 128 entries. Nothing reads it: skip pointers
//!   assume doc id order, which these lists do not have. Pages written now
//!   leave it empty, and a page rewritten by a removal has it cleared.

const std = @import("std");
const lattice = @import("lattice");

const Allocator = std.mem.Allocator;
const ArrayList = std.ArrayList;

const buffer_pool = lattice.storage.buffer_pool;
const page = lattice.storage.page;
const locking = lattice.concurrency.locking;
const BufferPool = buffer_pool.BufferPool;
const BufferFrame = buffer_pool.BufferFrame;
const LatchMode = locking.LatchMode;
const PageId = lattice.core.types.PageId;
const NodeId = lattice.core.types.NodeId;
const PageHeader = page.PageHeader;
const PageType = page.PageType;

const dictionary = @import("dictionary.zig");
const TokenId = dictionary.TokenId;

/// Document ID (alias for NodeId)
pub const DocId = NodeId;

/// Bytes of one skip pointer (max doc id, byte offset, entry count) in the
/// area earlier releases filled. Kept only to size that area, which fixes
/// where entries start.
const SKIP_POINTER_SIZE: usize = 16;

/// Posting page flag in the shared PageHeader indicating the v2 on-disk layout.
const POSTING_PAGE_FLAG_V2_LAYOUT: u8 = 0x01;

/// Posting entry flags stored inside posting-page headers.
const POSTING_ENTRY_FLAG_HAS_POSITIONS: u16 = 0x01;

/// File format version required for v2 posting pages.
const POSTING_PAGE_FORMAT_VERSION_V2: u16 = 2;

/// The part of a page this code lays out, which is not the database's page
/// size. Databases can use pages of 4096 to 65535 bytes; these pages use
/// their first 4096 bytes whatever the page size and leave the rest unused.
/// That is the layout files are written with, so it is a format constant:
/// sizing the layout from the real page size would misread every page an
/// existing large-page database already holds.
const PAGE_SIZE: usize = 4096;

/// Posting list errors
pub const PostingError = error{
    /// Page is full
    PageFull,
    /// Entry not found
    NotFound,
    /// I/O error
    IoError,
    /// Out of memory
    OutOfMemory,
    /// Invalid data
    InvalidData,
    /// Buffer pool error
    BufferPoolError,
};

/// Posting entry in memory
pub const PostingEntry = struct {
    doc_id: DocId,
    term_freq: u32,
    positions: ?[]const u32, // Optional position list for phrase queries
};

/// Legacy posting page header (follows base PageHeader).
pub const PostingPageHeaderV1 = extern struct {
    /// Which token this posting list belongs to
    token_id: TokenId,
    /// Number of posting entries in this page
    num_entries: u32,
    /// Next overflow page (0 = none)
    next_page: PageId,
    /// Number of skip pointers (for fast seeking)
    num_skip_pointers: u16,
    /// Flags: 0x01 = has positions for phrase queries
    flags: u16,
    /// Byte offset where posting data starts (after skip pointers)
    data_start: u32,

    pub const OFFSET: usize = @sizeOf(PageHeader);

    comptime {
        std.debug.assert(@sizeOf(PostingPageHeaderV1) == 20);
    }

    pub fn read(data: []const u8) PostingPageHeaderV1 {
        return std.mem.bytesAsValue(
            PostingPageHeaderV1,
            data[OFFSET..][0..@sizeOf(PostingPageHeaderV1)],
        ).*;
    }

    pub fn write(self: *const PostingPageHeaderV1, data: []u8) void {
        const dest = data[OFFSET..][0..@sizeOf(PostingPageHeaderV1)];
        @memcpy(dest, std.mem.asBytes(self));
    }
};

/// Posting page header v2 (follows base PageHeader).
pub const PostingPageHeaderV2 = extern struct {
    /// Which token this posting list belongs to
    token_id: TokenId,
    /// Number of posting entries in this page
    num_entries: u32,
    /// Next overflow page (0 = none)
    next_page: PageId,
    /// Number of skip pointers (for fast seeking)
    num_skip_pointers: u16,
    /// Flags: 0x01 = has positions for phrase queries
    flags: u16,
    /// Byte offset where posting data starts (after skip pointers)
    data_start: u32,
    /// Byte offset where posting data currently ends
    data_end: u32,

    pub const OFFSET: usize = @sizeOf(PageHeader);

    comptime {
        std.debug.assert(@sizeOf(PostingPageHeaderV2) == 24);
    }

    pub fn read(data: []const u8) PostingPageHeaderV2 {
        return std.mem.bytesAsValue(
            PostingPageHeaderV2,
            data[OFFSET..][0..@sizeOf(PostingPageHeaderV2)],
        ).*;
    }

    pub fn write(self: *const PostingPageHeaderV2, data: []u8) void {
        const dest = data[OFFSET..][0..@sizeOf(PostingPageHeaderV2)];
        @memcpy(dest, std.mem.asBytes(self));
    }
};

/// Public posting-page header alias now points at the v2 layout.
pub const PostingPageHeader = PostingPageHeaderV2;

const PostingPageLayout = enum {
    v1,
    v2,
};

const PostingPageMeta = struct {
    layout: PostingPageLayout,
    token_id: TokenId,
    num_entries: u32,
    next_page: PageId,
    num_skip_pointers: u16,
    flags: u16,
    data_start: u32,
    data_end: u32,

    fn hasPositions(self: PostingPageMeta) bool {
        return (self.flags & POSTING_ENTRY_FLAG_HAS_POSITIONS) != 0;
    }

    fn headerEnd(self: PostingPageMeta) usize {
        return switch (self.layout) {
            .v1 => HEADER_END_V1,
            .v2 => HEADER_END_V2,
        };
    }

    fn write(self: PostingPageMeta, data: []u8) void {
        setPostingPageLayout(data, self.layout);
        switch (self.layout) {
            .v1 => {
                (PostingPageHeaderV1{
                    .token_id = self.token_id,
                    .num_entries = self.num_entries,
                    .next_page = self.next_page,
                    .num_skip_pointers = self.num_skip_pointers,
                    .flags = self.flags,
                    .data_start = self.data_start,
                }).write(data);
            },
            .v2 => {
                (PostingPageHeaderV2{
                    .token_id = self.token_id,
                    .num_entries = self.num_entries,
                    .next_page = self.next_page,
                    .num_skip_pointers = self.num_skip_pointers,
                    .flags = self.flags,
                    .data_start = self.data_start,
                    .data_end = self.data_end,
                }).write(data);
            },
        }
    }
};

fn pageHeader(data: []u8) *PageHeader {
    return @ptrCast(@alignCast(data.ptr));
}

fn pageHeaderConst(data: []const u8) *const PageHeader {
    return @ptrCast(@alignCast(data.ptr));
}

fn getPostingPageLayout(data: []const u8) PostingPageLayout {
    return if ((pageHeaderConst(data).flags & POSTING_PAGE_FLAG_V2_LAYOUT) != 0) .v2 else .v1;
}

fn setPostingPageLayout(data: []u8, layout: PostingPageLayout) void {
    const header = pageHeader(data);
    switch (layout) {
        .v1 => header.flags &= ~POSTING_PAGE_FLAG_V2_LAYOUT,
        .v2 => header.flags |= POSTING_PAGE_FLAG_V2_LAYOUT,
    }
}

/// Data offset after headers (before skip pointers).
const HEADER_END_V1: usize = @sizeOf(PageHeader) + @sizeOf(PostingPageHeaderV1);
const HEADER_END_V2: usize = @sizeOf(PageHeader) + @sizeOf(PostingPageHeaderV2);

/// Maximum skip pointers reserved in the v1 layout (256 bytes).
const MAX_SKIP_POINTERS_V1: usize = 16;

/// Reserved space for skip pointers in the v1 layout.
const SKIP_POINTER_AREA_SIZE_V1: usize = MAX_SKIP_POINTERS_V1 * SKIP_POINTER_SIZE;

/// Data offset after headers and skip pointer area. This remains stable across
/// v1 and v2 so lazy page upgrades do not need to move posting payload bytes.
const DATA_OFFSET: usize = HEADER_END_V1 + SKIP_POINTER_AREA_SIZE_V1;

/// Reserved space for skip pointers in the v2 layout. The v2 header is 4 bytes
/// larger, so the skip reserve shrinks by 4 bytes while keeping DATA_OFFSET
/// unchanged.
const SKIP_POINTER_AREA_SIZE_V2: usize = DATA_OFFSET - HEADER_END_V2;

comptime {
    std.debug.assert(DATA_OFFSET == HEADER_END_V2 + SKIP_POINTER_AREA_SIZE_V2);
}

/// Posting list storage manager
pub const PostingStore = struct {
    allocator: Allocator,
    bp: *BufferPool,

    const Self = @This();

    /// Initialize posting store
    pub fn init(allocator: Allocator, bp: *BufferPool) Self {
        return Self{
            .allocator = allocator,
            .bp = bp,
        };
    }

    fn ensureFormatV2(self: *Self) PostingError!void {
        var header = self.bp.pm.getHeader().*;
        if (header.format_version >= POSTING_PAGE_FORMAT_VERSION_V2 and
            header.min_reader_version >= POSTING_PAGE_FORMAT_VERSION_V2)
        {
            return;
        }

        header.format_version = POSTING_PAGE_FORMAT_VERSION_V2;
        header.min_reader_version = POSTING_PAGE_FORMAT_VERSION_V2;
        self.bp.pm.updateHeader(&header) catch {
            return PostingError.IoError;
        };
    }

    fn scanDataEnd(self: *Self, data: []const u8, num_entries: u32, data_start: u32, has_positions: bool) usize {
        _ = self;

        var offset: usize = data_start;
        var entries_read: u32 = 0;

        while (entries_read < num_entries and offset < PAGE_SIZE) {
            const doc_result = decodeVarint(data[offset..]);
            offset += doc_result.bytes;

            const freq_result = decodeVarint(data[offset..]);
            offset += freq_result.bytes;

            if (has_positions) {
                const pos_count_result = decodeVarint(data[offset..]);
                offset += pos_count_result.bytes;
                var pos_idx: usize = 0;
                while (pos_idx < pos_count_result.value) : (pos_idx += 1) {
                    const pos_result = decodeVarint(data[offset..]);
                    offset += pos_result.bytes;
                }
            }

            entries_read += 1;
        }

        return offset;
    }

    fn readPageMeta(self: *Self, data: []const u8, include_data_end: bool) PostingPageMeta {
        return switch (getPostingPageLayout(data)) {
            .v1 => blk: {
                const header = PostingPageHeaderV1.read(data);
                break :blk .{
                    .layout = .v1,
                    .token_id = header.token_id,
                    .num_entries = header.num_entries,
                    .next_page = header.next_page,
                    .num_skip_pointers = header.num_skip_pointers,
                    .flags = header.flags,
                    .data_start = header.data_start,
                    .data_end = if (include_data_end)
                        @intCast(self.scanDataEnd(data, header.num_entries, header.data_start, (header.flags & POSTING_ENTRY_FLAG_HAS_POSITIONS) != 0))
                    else
                        header.data_start,
                };
            },
            .v2 => blk: {
                const header = PostingPageHeaderV2.read(data);
                break :blk .{
                    .layout = .v2,
                    .token_id = header.token_id,
                    .num_entries = header.num_entries,
                    .next_page = header.next_page,
                    .num_skip_pointers = header.num_skip_pointers,
                    .flags = header.flags,
                    .data_start = header.data_start,
                    .data_end = header.data_end,
                };
            },
        };
    }

    fn clearSkipPointers(self: *Self, data: []u8, meta: PostingPageMeta) void {
        _ = self;
        @memset(data[meta.headerEnd()..@as(usize, meta.data_start)], 0);
    }

    fn upgradePageToV2(self: *Self, data: []u8, meta: *PostingPageMeta) PostingError!void {
        if (meta.layout == .v2) return;

        try self.ensureFormatV2();

        var upgraded = meta.*;
        upgraded.layout = .v2;
        upgraded.data_start = @intCast(DATA_OFFSET);
        if (upgraded.data_end < upgraded.data_start) {
            upgraded.data_end = upgraded.data_start;
        }

        self.clearSkipPointers(data, upgraded);
        upgraded.num_skip_pointers = 0;
        upgraded.write(data);
        meta.* = upgraded;
    }

    /// Create a new posting list, returns first page ID
    pub fn create(self: *Self, token_id: TokenId) PostingError!PageId {
        try self.ensureFormatV2();

        const page_id = self.bp.pm.allocatePage() catch {
            return PostingError.BufferPoolError;
        };

        const frame = self.bp.fetchPage(page_id, .exclusive) catch {
            return PostingError.BufferPoolError;
        };
        errdefer self.bp.unpinPage(frame, false);

        const data = frame.data;

        // Initialize page header
        var header = PageHeader.init(.fts_posting);
        header.flags |= POSTING_PAGE_FLAG_V2_LAYOUT;
        @memcpy(data[0..@sizeOf(PageHeader)], std.mem.asBytes(&header));

        // Initialize posting page header
        const posting_header = PostingPageHeader{
            .token_id = token_id,
            .num_entries = 0,
            .next_page = 0,
            .num_skip_pointers = 0,
            .flags = 0,
            .data_start = @intCast(DATA_OFFSET),
            .data_end = @intCast(DATA_OFFSET),
        };
        posting_header.write(data);

        self.bp.unpinPage(frame, true);

        return page_id;
    }

    /// Create the first page of a new list, carrying its own page id as the
    /// list's identity (see `appendAt`).
    pub fn createList(self: *Self) PostingError!PageId {
        const page_id = try self.create(0);
        const frame = self.bp.fetchPage(page_id, .exclusive) catch {
            return PostingError.BufferPoolError;
        };
        var meta = self.readPageMeta(frame.data, false);
        meta.token_id = page_id;
        meta.write(frame.data);
        self.bp.unpinPage(frame, true);
        return page_id;
    }

    /// Append a posting entry to a list
    /// Returns the page ID where the entry was stored (may be different if overflow)
    pub fn append(self: *Self, page_id: PageId, entry: PostingEntry) PostingError!PageId {
        return self.appendWithPositions(page_id, entry, false);
    }

    /// Append from the head of the list, with no remembered tail.
    pub fn appendWithPositions(self: *Self, page_id: PageId, entry: PostingEntry, store_positions: bool) PostingError!PageId {
        return self.appendAt(page_id, 0, 0, entry, store_positions);
    }

    /// Append a posting entry, optionally storing its positions, to the first
    /// page with room at or after where the walk starts, and return that page.
    ///
    /// The walk starts at `tail_hint` when the caller has one (0 for none),
    /// which should be the page the previous append returned: the list's last
    /// page, or one near it. Starting there makes an append cost a page or two
    /// however long the list is; starting at the head reads every page, which
    /// made indexing a document with a common term cost time in proportion to
    /// that term's list. The hint is trusted only if it is a posting page
    /// carrying `stamp`, the identity this list's pages carry in their
    /// header's token id field (the head page id, for lists `createList`
    /// made); otherwise the walk starts from the head. The page the entry goes
    /// on, and any page added to the list, is given `stamp`, so a list written
    /// by an earlier release, whose pages carry a counter value instead, takes
    /// it on from its first append. A `stamp` of 0 means none: no hint is
    /// trusted and nothing is stamped.
    ///
    /// Whether a page's entries carry positions is a property of the page, set by
    /// its first entry. An entry whose positions disagree with the page is written
    /// the page's way — with an empty position list, or without its positions —
    /// because one entry encoded differently would make every later entry on the
    /// page decode as garbage. That can happen when `store_positions` changes
    /// between opens of the same database.
    ///
    /// An entry always fits in an empty page: positions beyond what one page can
    /// hold are not stored (see `encodeEntry`).
    pub fn appendAt(
        self: *Self,
        head: PageId,
        stamp: TokenId,
        tail_hint: PageId,
        entry: PostingEntry,
        store_positions: bool,
    ) PostingError!PageId {
        const wants_positions = store_positions and entry.positions != null;
        var encode_buf: [PAGE_DATA_CAPACITY]u8 = undefined;

        var current_page = head;
        var checking_hint = false;
        if (stamp != 0 and tail_hint != 0 and tail_hint != head and self.bp.pm.isValidPage(tail_hint)) {
            current_page = tail_hint;
            checking_hint = true;
        }
        while (true) {
            const frame = self.bp.fetchPage(current_page, .exclusive) catch {
                if (checking_hint) {
                    checking_hint = false;
                    current_page = head;
                    continue;
                }
                return PostingError.BufferPoolError;
            };
            var frame_dirty = false;
            errdefer self.bp.unpinPage(frame, frame_dirty);

            const data = frame.data;
            if (checking_hint) {
                checking_hint = false;
                if (pageHeaderConst(data).page_type != .fts_posting or
                    self.readPageMeta(data, false).token_id != stamp)
                {
                    self.bp.unpinPage(frame, false);
                    current_page = head;
                    continue;
                }
            }
            var posting_header = self.readPageMeta(data, true);

            if (posting_header.layout == .v1) {
                try self.upgradePageToV2(data, &posting_header);
                frame_dirty = true;
            }

            const page_positions = if (posting_header.num_entries == 0)
                wants_positions
            else
                posting_header.hasPositions();
            const encode_len = encodeEntry(entry, page_positions, &encode_buf);

            // Check if there's space
            const current_end: usize = posting_header.data_end;
            if (current_end + encode_len > PAGE_SIZE) {
                if (posting_header.next_page != 0) {
                    const next_page = posting_header.next_page;
                    self.bp.unpinPage(frame, frame_dirty);
                    current_page = next_page;
                    continue;
                }

                // Need to allocate overflow page
                if (stamp != 0) posting_header.token_id = stamp;
                const new_page_id = try self.create(posting_header.token_id);
                posting_header.next_page = new_page_id;
                posting_header.write(data);
                self.bp.unpinPage(frame, true);
                current_page = new_page_id;
                continue;
            }

            if (page_positions) {
                posting_header.flags |= POSTING_ENTRY_FLAG_HAS_POSITIONS;
            } else {
                posting_header.flags &= ~POSTING_ENTRY_FLAG_HAS_POSITIONS;
            }
            if (stamp != 0) posting_header.token_id = stamp;

            // Write entry
            @memcpy(data[current_end..][0..encode_len], encode_buf[0..encode_len]);

            // Update header
            posting_header.num_entries += 1;
            posting_header.data_end = @intCast(current_end + encode_len);
            posting_header.write(data);

            self.bp.unpinPage(frame, true);
            return current_page;
        }
    }

    /// Get iterator for reading posting list
    pub fn iterate(self: *Self, page_id: PageId) PostingError!PostingIterator {
        return PostingIterator.init(self, page_id);
    }

    /// Result of removing an entry from posting list
    pub const RemovalResult = struct {
        found: bool,
        term_freq: u32,
    };

    /// Remove an entry from a posting list by doc_id
    /// Returns the term frequency of the removed entry if found
    pub fn removeEntry(self: *Self, posting_page: PageId, target_doc_id: DocId) PostingError!RemovalResult {
        var current_page = posting_page;

        while (current_page != 0) {
            const frame = self.bp.fetchPage(current_page, .exclusive) catch {
                return PostingError.BufferPoolError;
            };

            const data = frame.data;
            var header = self.readPageMeta(data, false);
            const has_positions = header.hasPositions();

            // Collect all entries except the target
            var kept_entries: std.ArrayList(StoredEntry) = .empty;
            defer {
                for (kept_entries.items) |entry| {
                    if (entry.positions) |pos| {
                        self.allocator.free(pos);
                    }
                }
                kept_entries.deinit(self.allocator);
            }

            var found_entry: ?StoredEntry = null;
            var offset: usize = header.data_start;

            for (0..header.num_entries) |_| {
                const doc_result = decodeVarint(data[offset..]);
                offset += doc_result.bytes;

                const freq_result = decodeVarint(data[offset..]);
                offset += freq_result.bytes;

                var positions: ?[]u32 = null;
                if (has_positions) {
                    const pos_count_result = decodeVarint(data[offset..]);
                    offset += pos_count_result.bytes;
                    const pos_count: usize = @intCast(pos_count_result.value);

                    if (pos_count > 0) {
                        positions = self.allocator.alloc(u32, pos_count) catch {
                            self.bp.unpinPage(frame, false);
                            return PostingError.OutOfMemory;
                        };
                        for (0..pos_count) |i| {
                            const pos_result = decodeVarint(data[offset..]);
                            offset += pos_result.bytes;
                            positions.?[i] = @intCast(pos_result.value);
                        }
                    }
                }

                const entry = StoredEntry{
                    .doc_id = doc_result.value,
                    .term_freq = @intCast(freq_result.value),
                    .positions = positions,
                };

                if (doc_result.value == target_doc_id) {
                    found_entry = entry;
                } else {
                    kept_entries.append(self.allocator, entry) catch {
                        if (positions) |pos| self.allocator.free(pos);
                        self.bp.unpinPage(frame, false);
                        return PostingError.OutOfMemory;
                    };
                }
            }

            if (found_entry) |removed| {
                if (header.layout == .v1) {
                    try self.ensureFormatV2();
                    header.layout = .v2;
                }

                // Rebuild page with remaining entries
                header.data_end = self.rebuildPage(data, &header, kept_entries.items, has_positions);

                // Update header
                header.num_entries = @intCast(kept_entries.items.len);

                // Skip pointers, on a page an earlier release filled them
                // on, name byte offsets the rewrite has moved.
                self.clearSkipPointers(data, header);
                header.num_skip_pointers = 0;

                header.write(data);
                self.bp.unpinPage(frame, true);

                // Free removed entry's positions
                if (removed.positions) |pos| {
                    self.allocator.free(pos);
                }

                return .{ .found = true, .term_freq = removed.term_freq };
            }

            // Entry not found in this page, try next
            const next_page = header.next_page;
            self.bp.unpinPage(frame, false);
            current_page = next_page;
        }

        return .{ .found = false, .term_freq = 0 };
    }

    /// Temporary storage for entry during rebuild
    const StoredEntry = struct {
        doc_id: DocId,
        term_freq: u32,
        positions: ?[]u32,
    };

    /// Rebuild page data from entries
    fn rebuildPage(self: *Self, data: []u8, header: *PostingPageMeta, entries: []const StoredEntry, has_positions: bool) u32 {
        _ = self;
        var offset: usize = header.data_start;

        for (entries) |entry| {
            // Encode doc_id
            offset += encodeVarint(entry.doc_id, data[offset..]);

            // Encode term_freq
            offset += encodeVarint(entry.term_freq, data[offset..]);

            // Encode positions if present
            if (has_positions) {
                if (entry.positions) |positions| {
                    offset += encodeVarint(positions.len, data[offset..]);
                    for (positions) |pos| {
                        offset += encodeVarint(pos, data[offset..]);
                    }
                } else {
                    offset += encodeVarint(0, data[offset..]);
                }
            }
        }

        // Zero out remaining data area (optional, for cleanliness)
        if (offset < PAGE_SIZE) {
            @memset(data[offset..PAGE_SIZE], 0);
        }

        return @intCast(offset);
    }
};

/// Iterator for reading posting entries
pub const PostingIterator = struct {
    store: *PostingStore,
    current_page: PageId,
    current_offset: usize,
    entries_read: u32,
    total_entries: u32,
    data_start: usize, // Start of posting data (after the skip-pointer area)
    last_doc_id: DocId, // Never read: entries store whole doc ids, not deltas
    has_positions: bool,
    done: bool,
    /// Entries decoded ahead by `next`, so a page is pinned once per batch
    /// rather than once per entry. `entries_read` counts what was decoded,
    /// so it is ahead of what was returned while the batch is not drained.
    batch: [BATCH_SIZE]BatchEntry = undefined,
    batch_len: u16 = 0,
    batch_pos: u16 = 0,

    const Self = @This();
    const BATCH_SIZE = 128;
    const BatchEntry = struct { doc_id: DocId, term_freq: u32 };

    pub fn init(store: *PostingStore, page_id: PageId) PostingError!Self {
        // Read header to get entry count and flags
        const frame = store.bp.fetchPage(page_id, .shared) catch {
            return PostingError.BufferPoolError;
        };
        defer store.bp.unpinPage(frame, false);

        const header = store.readPageMeta(frame.data, false);

        return Self{
            .store = store,
            .current_page = page_id,
            .current_offset = header.data_start,
            .entries_read = 0,
            .total_entries = header.num_entries,
            .data_start = header.data_start,
            .last_doc_id = 0,
            .has_positions = header.hasPositions(),
            // Not `num_entries == 0`: removals can empty the head page while
            // later pages still hold entries. Reading follows the chain past
            // empty pages and decides when the list is finished.
            .done = false,
        };
    }

    /// Get the next posting entry (without positions)
    ///
    /// Decodes up to BATCH_SIZE entries per page pin. Do not mix with
    /// `nextWithPositions` on one iterator.
    pub fn next(self: *Self) PostingError!?PostingEntry {
        if (self.batch_pos == self.batch_len) {
            try self.refill();
            if (self.batch_len == 0) return null;
        }
        const e = self.batch[self.batch_pos];
        self.batch_pos += 1;
        return .{ .doc_id = e.doc_id, .term_freq = e.term_freq, .positions = null };
    }

    /// Decode the next run of entries, following the chain to the next page
    /// when this one is used up. Leaves an empty batch at the end of the list.
    fn refill(self: *Self) PostingError!void {
        self.batch_len = 0;
        self.batch_pos = 0;
        while (!self.done) {
            const frame = self.store.bp.fetchPage(self.current_page, .shared) catch {
                return PostingError.BufferPoolError;
            };
            const data = frame.data;
            const header = self.store.readPageMeta(data, false);

            if (self.entries_read >= header.num_entries) {
                const next_page = header.next_page;
                self.store.bp.unpinPage(frame, false);
                if (next_page == 0) {
                    self.done = true;
                    return;
                }
                const next_frame = self.store.bp.fetchPage(next_page, .shared) catch {
                    return PostingError.BufferPoolError;
                };
                const next_header = self.store.readPageMeta(next_frame.data, false);
                self.store.bp.unpinPage(next_frame, false);
                self.current_page = next_page;
                self.entries_read = 0;
                self.current_offset = next_header.data_start;
                self.total_entries = next_header.num_entries;
                self.data_start = next_header.data_start;
                self.has_positions = next_header.hasPositions();
                continue;
            }

            const page_has_positions = header.hasPositions();
            var offset = self.current_offset;
            while (self.entries_read < header.num_entries and self.batch_len < BATCH_SIZE) {
                const doc_result = decodeVarint(data[offset..]);
                offset += doc_result.bytes;
                const freq_result = decodeVarint(data[offset..]);
                offset += freq_result.bytes;
                if (page_has_positions) {
                    const pos_count_result = decodeVarint(data[offset..]);
                    offset += pos_count_result.bytes;
                    for (0..@intCast(pos_count_result.value)) |_| {
                        offset += decodeVarint(data[offset..]).bytes;
                    }
                }
                self.batch[self.batch_len] = .{
                    .doc_id = doc_result.value,
                    .term_freq = @intCast(freq_result.value),
                };
                self.batch_len += 1;
                self.entries_read += 1;
            }
            self.current_offset = offset;
            self.store.bp.unpinPage(frame, false);
            return;
        }
    }

    /// Get the next posting entry with positions (caller must free positions)
    pub fn nextWithPositions(self: *Self, allocator: Allocator) PostingError!?PostingEntry {
        std.debug.assert(self.batch_pos == self.batch_len); // do not mix with next()
        return self.nextInternal(allocator);
    }

    fn nextInternal(self: *Self, allocator: ?Allocator) PostingError!?PostingEntry {
        if (self.done) return null;

        const frame = self.store.bp.fetchPage(self.current_page, .shared) catch {
            return PostingError.BufferPoolError;
        };
        defer self.store.bp.unpinPage(frame, false);

        const data = frame.data;
        const header = self.store.readPageMeta(data, false);
        const page_has_positions = header.hasPositions();

        if (self.entries_read >= header.num_entries) {
            // Check for next page
            if (header.next_page != 0) {
                self.current_page = header.next_page;
                self.entries_read = 0;

                // Update has_positions for new page
                const next_frame = self.store.bp.fetchPage(header.next_page, .shared) catch {
                    return PostingError.BufferPoolError;
                };
                defer self.store.bp.unpinPage(next_frame, false);
                const next_header = self.store.readPageMeta(next_frame.data, false);
                self.current_offset = next_header.data_start;
                self.total_entries = next_header.num_entries;
                self.data_start = next_header.data_start;
                self.has_positions = next_header.hasPositions();

                // Recursively get from next page
                return self.nextInternal(allocator);
            }
            self.done = true;
            return null;
        }

        // Decode entry
        const doc_result = decodeVarint(data[self.current_offset..]);
        self.current_offset += doc_result.bytes;

        const freq_result = decodeVarint(data[self.current_offset..]);
        self.current_offset += freq_result.bytes;

        // Decode positions if present
        var positions: ?[]u32 = null;
        if (page_has_positions) {
            const pos_count_result = decodeVarint(data[self.current_offset..]);
            self.current_offset += pos_count_result.bytes;
            const pos_count: usize = @intCast(pos_count_result.value);

            if (allocator != null and pos_count > 0) {
                // Allocate and read positions
                positions = allocator.?.alloc(u32, pos_count) catch {
                    return PostingError.OutOfMemory;
                };
                for (0..pos_count) |i| {
                    const pos_result = decodeVarint(data[self.current_offset..]);
                    self.current_offset += pos_result.bytes;
                    positions.?[i] = @intCast(pos_result.value);
                }
            } else {
                // Skip positions
                for (0..pos_count) |_| {
                    const pos_result = decodeVarint(data[self.current_offset..]);
                    self.current_offset += pos_result.bytes;
                }
            }
        }

        self.entries_read += 1;

        return PostingEntry{
            .doc_id = doc_result.value,
            .term_freq = @intCast(freq_result.value),
            .positions = if (positions) |p| p else null,
        };
    }

    /// Clean up iterator resources
    pub fn deinit(self: *Self) void {
        _ = self;
        // No resources to clean up currently
    }
};

// ============================================================================
// Variable-Length Integer Encoding (Varint)
// ============================================================================

/// Bytes of posting data one page can hold.
const PAGE_DATA_CAPACITY: usize = PAGE_SIZE - DATA_OFFSET;

/// Number of bytes `encodeVarint` writes for a value.
pub fn varintLen(value: u64) usize {
    var v = value;
    var len: usize = 1;
    while (v >= 0x80) : (v >>= 7) len += 1;
    return len;
}

/// Encode one posting entry into `buf`, returning its length.
///
/// With `with_positions`, the position count and positions follow the doc id
/// and term frequency; an entry that has none records a count of zero. A term
/// can occur in one document more often than one page can record, and an entry
/// is never split across pages, so positions that would not fit are dropped from
/// the end. The term frequency still counts every occurrence, so scoring is
/// unaffected; only phrase matching past that point is lost. The alternative
/// was an entry no page could hold, which used to overrun a 512-byte stack
/// buffer long before reaching that size.
fn encodeEntry(entry: PostingEntry, with_positions: bool, buf: *[PAGE_DATA_CAPACITY]u8) usize {
    var len: usize = 0;
    len += encodeVarint(entry.doc_id, buf[len..]);
    len += encodeVarint(entry.term_freq, buf[len..]);
    if (!with_positions) return len;

    const positions = entry.positions orelse &[_]u32{};
    // The kept count is at most the full count, so its varint is no longer.
    const budget = PAGE_DATA_CAPACITY - len - varintLen(positions.len);
    var kept: usize = 0;
    var kept_bytes: usize = 0;
    while (kept < positions.len) : (kept += 1) {
        const size = varintLen(positions[kept]);
        if (kept_bytes + size > budget) break;
        kept_bytes += size;
    }

    len += encodeVarint(kept, buf[len..]);
    for (positions[0..kept]) |pos| {
        len += encodeVarint(pos, buf[len..]);
    }
    return len;
}

/// Encode a u64 as a varint
/// Returns the number of bytes written
pub fn encodeVarint(value: u64, buf: []u8) usize {
    var v = value;
    var i: usize = 0;

    while (v >= 0x80) {
        buf[i] = @as(u8, @truncate(v)) | 0x80;
        v >>= 7;
        i += 1;
    }
    buf[i] = @as(u8, @truncate(v));
    return i + 1;
}

/// Decode a varint from buffer
/// Returns the value and number of bytes read
pub fn decodeVarint(buf: []const u8) struct { value: u64, bytes: usize } {
    var value: u64 = 0;
    var shift: u6 = 0;
    var i: usize = 0;

    while (i < buf.len and i < 10) {
        const byte = buf[i];
        value |= @as(u64, byte & 0x7F) << shift;
        i += 1;

        if (byte & 0x80 == 0) {
            break;
        }
        shift += 7;
    }

    return .{ .value = value, .bytes = i };
}

// ============================================================================
// Tests
// ============================================================================

test "varint encoding small values" {
    var buf: [10]u8 = undefined;

    // Single byte values (0-127)
    try std.testing.expectEqual(@as(usize, 1), encodeVarint(0, &buf));
    try std.testing.expectEqual(@as(u8, 0), buf[0]);

    try std.testing.expectEqual(@as(usize, 1), encodeVarint(1, &buf));
    try std.testing.expectEqual(@as(u8, 1), buf[0]);

    try std.testing.expectEqual(@as(usize, 1), encodeVarint(127, &buf));
    try std.testing.expectEqual(@as(u8, 127), buf[0]);

    // Two byte values (128-16383)
    try std.testing.expectEqual(@as(usize, 2), encodeVarint(128, &buf));
    try std.testing.expectEqual(@as(usize, 2), encodeVarint(16383, &buf));
}

test "varint roundtrip" {
    var buf: [10]u8 = undefined;

    const test_values = [_]u64{ 0, 1, 127, 128, 255, 256, 16383, 16384, 1000000, 0xFFFFFFFF, 0xFFFFFFFFFFFFFFFF };

    for (test_values) |value| {
        const written = encodeVarint(value, &buf);
        const result = decodeVarint(&buf);

        try std.testing.expectEqual(value, result.value);
        try std.testing.expectEqual(written, result.bytes);
    }
}

test "posting store create and append" {
    const allocator = std.testing.allocator;

    const vfs = lattice.storage.vfs;
    const page_manager = lattice.storage.page_manager;

    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const db_path = "/tmp/lattice_posting_test.db";
    vfs_impl.delete(db_path) catch {};

    var pm = try page_manager.PageManager.init(allocator, vfs_impl, db_path, .{ .create = true });
    defer {
        pm.deinit();
        vfs_impl.delete(db_path) catch {};
    }

    var bp = try BufferPool.init(allocator, &pm, 64 * 4096);
    defer bp.deinit();

    var store = PostingStore.init(allocator, &bp);

    // Create posting list for token 1
    const page_id = try store.create(1);

    // Append some entries
    _ = try store.append(page_id, .{ .doc_id = 100, .term_freq = 3, .positions = null });
    _ = try store.append(page_id, .{ .doc_id = 200, .term_freq = 1, .positions = null });
    _ = try store.append(page_id, .{ .doc_id = 350, .term_freq = 5, .positions = null });

    // Read back entries
    var iter = try store.iterate(page_id);
    defer iter.deinit();

    const entry1 = try iter.next();
    try std.testing.expect(entry1 != null);
    try std.testing.expectEqual(@as(DocId, 100), entry1.?.doc_id);
    try std.testing.expectEqual(@as(u32, 3), entry1.?.term_freq);

    const entry2 = try iter.next();
    try std.testing.expect(entry2 != null);
    try std.testing.expectEqual(@as(DocId, 200), entry2.?.doc_id);
    try std.testing.expectEqual(@as(u32, 1), entry2.?.term_freq);

    const entry3 = try iter.next();
    try std.testing.expect(entry3 != null);
    try std.testing.expectEqual(@as(DocId, 350), entry3.?.doc_id);
    try std.testing.expectEqual(@as(u32, 5), entry3.?.term_freq);

    // No more entries
    const entry4 = try iter.next();
    try std.testing.expect(entry4 == null);
}

test "posting pages keep no skip pointers and read pages that have them" {
    const allocator = std.testing.allocator;

    const vfs = lattice.storage.vfs;
    const page_manager = lattice.storage.page_manager;

    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const db_path = "/tmp/lse_posting_skip_area_test.db";
    vfs_impl.delete(db_path) catch {};

    var pm = try page_manager.PageManager.init(allocator, vfs_impl, db_path, .{ .create = true });
    defer {
        pm.deinit();
        vfs_impl.delete(db_path) catch {};
    }

    var bp = try BufferPool.init(allocator, &pm, 128 * 4096);
    defer bp.deinit();

    var store = PostingStore.init(allocator, &bp);
    const page_id = try store.create(1);

    // 300 entries fit one page; releases up to 0.16 wrote two skip pointers
    // for them. None are written now.
    for (1..301) |i| {
        _ = try store.append(page_id, .{ .doc_id = @intCast(i * 10), .term_freq = 1, .positions = null });
    }
    {
        const frame = try bp.fetchPage(page_id, .exclusive);
        defer bp.unpinPage(frame, true);
        var header = PostingPageHeader.read(frame.data);
        try std.testing.expectEqual(@as(u32, 300), header.num_entries);
        try std.testing.expectEqual(@as(u16, 0), header.num_skip_pointers);
        // Make it look like a page 0.16.0 wrote: one skip pointer at entry 128.
        var offset: usize = header.data_start;
        for (0..128) |_| {
            offset += decodeVarint(frame.data[offset..]).bytes;
            offset += decodeVarint(frame.data[offset..]).bytes;
        }
        const area = frame.data[HEADER_END_V2..][0..SKIP_POINTER_SIZE];
        std.mem.writeInt(u64, area[0..8], 1290, .little);
        std.mem.writeInt(u32, area[8..12], @intCast(offset - header.data_start), .little);
        std.mem.writeInt(u32, area[12..16], 128, .little);
        header.num_skip_pointers = 1;
        header.write(frame.data);
    }

    // Appending leaves an earlier release's pointers as they are: they name
    // entries that do not move.
    _ = try store.append(page_id, .{ .doc_id = 5, .term_freq = 2, .positions = null });
    {
        const frame = try bp.fetchPage(page_id, .shared);
        defer bp.unpinPage(frame, false);
        try std.testing.expectEqual(@as(u16, 1), PostingPageHeader.read(frame.data).num_skip_pointers);
    }

    var iter = try store.iterate(page_id);
    var count: usize = 0;
    while (try iter.next()) |entry| {
        count += 1;
        if (count <= 300) try std.testing.expectEqual(@as(DocId, count * 10), entry.doc_id);
    }
    try std.testing.expectEqual(@as(usize, 301), count);

    // A removal moves entries, so it clears the area.
    try std.testing.expect((try store.removeEntry(page_id, 10)).found);
    {
        const frame = try bp.fetchPage(page_id, .shared);
        defer bp.unpinPage(frame, false);
        const header = PostingPageHeader.read(frame.data);
        try std.testing.expectEqual(@as(u16, 0), header.num_skip_pointers);
        try std.testing.expectEqual(@as(u32, 300), header.num_entries);
        for (frame.data[HEADER_END_V2..header.data_start]) |byte| try std.testing.expectEqual(@as(u8, 0), byte);
    }
    var after = try store.iterate(page_id);
    count = 0;
    while (try after.next()) |entry| {
        try std.testing.expect(entry.doc_id != 10);
        count += 1;
    }
    try std.testing.expectEqual(@as(usize, 300), count);
}

test "posting store lazily upgrades legacy v1 pages on append" {
    const allocator = std.testing.allocator;

    const vfs = lattice.storage.vfs;
    const page_manager = lattice.storage.page_manager;

    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const db_path = "/tmp/lattice_posting_upgrade_test.db";
    vfs_impl.delete(db_path) catch {};

    var pm = try page_manager.PageManager.init(allocator, vfs_impl, db_path, .{ .create = true });
    defer {
        pm.deinit();
        vfs_impl.delete(db_path) catch {};
    }

    var bp = try BufferPool.init(allocator, &pm, 64 * 4096);
    defer bp.deinit();

    var store = PostingStore.init(allocator, &bp);

    var file_header = pm.getHeader().*;
    file_header.format_version = 1;
    file_header.min_reader_version = 1;
    try pm.updateHeader(&file_header);

    const page_id = try pm.allocatePage();
    const frame = try bp.fetchPage(page_id, .exclusive);
    {
        const data = frame.data;
        const header = PageHeader.init(.fts_posting);
        @memcpy(data[0..@sizeOf(PageHeader)], std.mem.asBytes(&header));

        var legacy_header = PostingPageHeaderV1{
            .token_id = 1,
            .num_entries = 0,
            .next_page = 0,
            .num_skip_pointers = 0,
            .flags = 0,
            .data_start = @intCast(DATA_OFFSET),
        };

        var offset: usize = DATA_OFFSET;
        offset += encodeVarint(10, data[offset..]);
        offset += encodeVarint(1, data[offset..]);
        offset += encodeVarint(20, data[offset..]);
        offset += encodeVarint(2, data[offset..]);
        legacy_header.num_entries = 2;
        legacy_header.write(data);
    }
    bp.unpinPage(frame, true);

    _ = try store.append(page_id, .{ .doc_id = 30, .term_freq = 3, .positions = null });

    const upgraded_file_header = pm.getHeader().*;
    try std.testing.expectEqual(@as(u16, 2), upgraded_file_header.format_version);
    try std.testing.expectEqual(@as(u16, 2), upgraded_file_header.min_reader_version);

    const upgraded_frame = try bp.fetchPage(page_id, .shared);
    defer bp.unpinPage(upgraded_frame, false);

    try std.testing.expect((pageHeaderConst(upgraded_frame.data).flags & POSTING_PAGE_FLAG_V2_LAYOUT) != 0);

    const upgraded_header = PostingPageHeader.read(upgraded_frame.data);
    try std.testing.expectEqual(@as(u32, 3), upgraded_header.num_entries);
    try std.testing.expectEqual(@as(u32, DATA_OFFSET), upgraded_header.data_start);
    try std.testing.expect(upgraded_header.data_end > upgraded_header.data_start);

    var iter = try store.iterate(page_id);
    defer iter.deinit();

    const e1 = try iter.next();
    try std.testing.expect(e1 != null);
    try std.testing.expectEqual(@as(DocId, 10), e1.?.doc_id);
    try std.testing.expectEqual(@as(u32, 1), e1.?.term_freq);

    const e2 = try iter.next();
    try std.testing.expect(e2 != null);
    try std.testing.expectEqual(@as(DocId, 20), e2.?.doc_id);
    try std.testing.expectEqual(@as(u32, 2), e2.?.term_freq);

    const e3 = try iter.next();
    try std.testing.expect(e3 != null);
    try std.testing.expectEqual(@as(DocId, 30), e3.?.doc_id);
    try std.testing.expectEqual(@as(u32, 3), e3.?.term_freq);
}

test "posting store appends thousands of entries without stalling" {
    const allocator = std.testing.allocator;

    const vfs = lattice.storage.vfs;
    const page_manager = lattice.storage.page_manager;

    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const db_path = "/tmp/lattice_posting_stress_test.db";
    vfs_impl.delete(db_path) catch {};

    var pm = try page_manager.PageManager.init(allocator, vfs_impl, db_path, .{ .create = true });
    defer {
        pm.deinit();
        vfs_impl.delete(db_path) catch {};
    }

    var bp = try BufferPool.init(allocator, &pm, 128 * 4096);
    defer bp.deinit();

    var store = PostingStore.init(allocator, &bp);
    const page_id = try store.create(1);

    for (1..3001) |i| {
        _ = try store.append(page_id, .{
            .doc_id = @intCast(i),
            .term_freq = 1,
            .positions = null,
        });
    }

    var iter = try store.iterate(page_id);
    defer iter.deinit();

    var count: usize = 0;
    var last_doc: DocId = 0;
    while (try iter.next()) |entry| {
        count += 1;
        last_doc = entry.doc_id;
    }

    try std.testing.expectEqual(@as(usize, 3000), count);
    try std.testing.expectEqual(@as(DocId, 3000), last_doc);
}

test "posting iterator reads past a head page that removals emptied" {
    const allocator = std.testing.allocator;

    const vfs = lattice.storage.vfs;
    const page_manager = lattice.storage.page_manager;

    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const db_path = "/tmp/s3_lattice_posting_empty_head_test.db";
    vfs_impl.delete(db_path) catch {};

    var pm = try page_manager.PageManager.init(allocator, vfs_impl, db_path, .{ .create = true });
    defer {
        pm.deinit();
        vfs_impl.delete(db_path) catch {};
    }

    var bp = try BufferPool.init(allocator, &pm, 64 * 4096);
    defer bp.deinit();

    var store = PostingStore.init(allocator, &bp);
    const head = try store.create(1);

    // Three pages, so that both the head and a page in the middle of the chain
    // can be emptied.
    const total: DocId = 4000;
    for (1..total + 1) |i| {
        _ = try store.append(head, .{ .doc_id = @intCast(i), .term_freq = 1, .positions = null });
    }

    var emptied: u32 = 0;
    var page_id = head;
    for (0..2) |_| {
        const frame = try bp.fetchPage(page_id, .shared);
        const header = PostingPageHeader.read(frame.data);
        bp.unpinPage(frame, false);
        try std.testing.expect(header.next_page != 0);
        emptied += header.num_entries;
        page_id = header.next_page;
    }
    const head_count = emptied;

    // Remove everything the first two pages hold.
    for (1..head_count + 1) |i| {
        const removed = try store.removeEntry(head, @intCast(i));
        try std.testing.expect(removed.found);
    }

    var iter = try store.iterate(head);
    defer iter.deinit();
    var count: usize = 0;
    var expected: DocId = head_count + 1;
    while (try iter.next()) |entry| {
        try std.testing.expectEqual(expected, entry.doc_id);
        expected += 1;
        count += 1;
    }
    try std.testing.expectEqual(@as(usize, total - head_count), count);

    // The position-reading path walks the chain on its own.
    var pos_iter = try store.iterate(head);
    defer pos_iter.deinit();
    const first = try pos_iter.nextWithPositions(allocator);
    try std.testing.expect(first != null);
    try std.testing.expectEqual(@as(DocId, head_count + 1), first.?.doc_id);
    if (first.?.positions) |p| allocator.free(p);
}

test "posting store releases full pages before appending to overflow tail" {
    const allocator = std.testing.allocator;

    const vfs = lattice.storage.vfs;
    const page_manager = lattice.storage.page_manager;

    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const db_path = "/tmp/lattice_posting_overflow_pin_test.db";
    vfs_impl.delete(db_path) catch {};

    var pm = try page_manager.PageManager.init(allocator, vfs_impl, db_path, .{ .create = true });
    defer {
        pm.deinit();
        vfs_impl.delete(db_path) catch {};
    }

    var bp = try BufferPool.init(allocator, &pm, 2 * 4096);
    defer bp.deinit();

    var store = PostingStore.init(allocator, &bp);
    const page_id = try store.create(1);

    var positions: [256]u32 = undefined;
    for (&positions, 0..) |*position, i| {
        position.* = @intCast(i);
    }

    for (1..41) |i| {
        _ = try store.appendWithPositions(page_id, .{
            .doc_id = @intCast(i),
            .term_freq = 1,
            .positions = positions[0..],
        }, true);

        try std.testing.expectEqual(@as(usize, 0), bp.getStats().pinned_frames);
    }
}

test "posting append trusts a tail hint only on a page of its own list" {
    const allocator = std.testing.allocator;

    const vfs = lattice.storage.vfs;
    const page_manager = lattice.storage.page_manager;

    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const db_path = "/tmp/lse_posting_tail_hint_test.db";
    vfs_impl.delete(db_path) catch {};

    var pm = try page_manager.PageManager.init(allocator, vfs_impl, db_path, .{ .create = true });
    defer {
        pm.deinit();
        vfs_impl.delete(db_path) catch {};
    }

    var bp = try BufferPool.init(allocator, &pm, 128 * 4096);
    defer bp.deinit();

    var store = PostingStore.init(allocator, &bp);

    // A list as 0.16.0 made it: its pages carry a counter value, not the head.
    const legacy = try store.create(7);
    for (1..1501) |i| {
        _ = try store.append(legacy, .{ .doc_id = @intCast(i), .term_freq = 1, .positions = null });
    }
    // Another list, made here, several pages long.
    const other = try store.createList();
    var other_tail = other;
    for (1..1501) |i| {
        other_tail = try store.appendAt(other, other, other_tail, .{ .doc_id = @intCast(i), .term_freq = 1, .positions = null }, false);
    }
    try std.testing.expect(other_tail != other);

    // A hint at another list's page is ignored: the entry goes on the
    // legacy list's own last page, which takes on the list's head as its id.
    var tail = try store.appendAt(legacy, legacy, other_tail, .{ .doc_id = 5000, .term_freq = 1, .positions = null }, false);
    try std.testing.expect(tail != legacy and tail != other_tail);
    // A hint at a page that is not a posting page, or past the file, too.
    tail = try store.appendAt(legacy, legacy, pm.pageCount() + 10, .{ .doc_id = 5001, .term_freq = 1, .positions = null }, false);
    const freed = try store.createList();
    try bp.freePage(freed);
    try std.testing.expectEqual(tail, try store.appendAt(legacy, legacy, freed, .{ .doc_id = 5002, .term_freq = 1, .positions = null }, false));
    // The stamped tail is now trusted.
    try std.testing.expectEqual(tail, try store.appendAt(legacy, legacy, tail, .{ .doc_id = 5003, .term_freq = 1, .positions = null }, false));
    // And 0.16.0, appending from the head, still reaches the same tail.
    try std.testing.expectEqual(tail, try store.append(legacy, .{ .doc_id = 5004, .term_freq = 1, .positions = null }));

    for ([_]struct { head: PageId, want: usize, last: DocId }{
        .{ .head = legacy, .want = 1505, .last = 5004 },
        .{ .head = other, .want = 1500, .last = 1500 },
    }) |list| {
        var iter = try store.iterate(list.head);
        defer iter.deinit();
        var count: usize = 0;
        var last: DocId = 0;
        while (try iter.next()) |entry| {
            count += 1;
            last = entry.doc_id;
        }
        try std.testing.expectEqual(list.want, count);
        try std.testing.expectEqual(list.last, last);
    }
}
