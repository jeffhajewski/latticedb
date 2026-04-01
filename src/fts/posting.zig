//! Posting List Storage for Full-Text Search.
//!
//! Stores document IDs with term frequencies using delta encoding.
//! Features:
//! - Delta-varint encoding for 3-5x compression
//! - Skip pointers every 128 entries for fast intersection
//! - Optional position lists for phrase queries

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

/// Skip pointer interval (create skip pointer every N entries)
pub const SKIP_INTERVAL: u32 = 128;

/// Posting page flag in the shared PageHeader indicating the v2 on-disk layout.
const POSTING_PAGE_FLAG_V2_LAYOUT: u8 = 0x01;

/// Posting entry flags stored inside posting-page headers.
const POSTING_ENTRY_FLAG_HAS_POSITIONS: u16 = 0x01;

/// File format version required for v2 posting pages.
const POSTING_PAGE_FORMAT_VERSION_V2: u16 = 2;

/// Page size
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

/// Skip pointer for fast intersection
pub const SkipPointer = extern struct {
    /// Max doc_id in this block
    doc_id: DocId,
    /// Offset in posting data
    byte_offset: u32,
    /// Number of entries before this pointer
    entry_count: u32,

    comptime {
        std.debug.assert(@sizeOf(SkipPointer) == 16);
    }

    pub fn serialize(self: *const SkipPointer, buf: *[@sizeOf(SkipPointer)]u8) void {
        @memcpy(buf, std.mem.asBytes(self));
    }

    pub fn deserialize(buf: []const u8) SkipPointer {
        return std.mem.bytesAsValue(SkipPointer, buf[0..@sizeOf(SkipPointer)]).*;
    }
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

    fn maxSkipPointers(self: PostingPageMeta) usize {
        return switch (self.layout) {
            .v1 => MAX_SKIP_POINTERS_V1,
            .v2 => MAX_SKIP_POINTERS_V2,
        };
    }

    fn skipPointerOffset(self: PostingPageMeta, index: usize) usize {
        return self.headerEnd() + index * @sizeOf(SkipPointer);
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
const SKIP_POINTER_AREA_SIZE_V1: usize = MAX_SKIP_POINTERS_V1 * @sizeOf(SkipPointer);

/// Data offset after headers and skip pointer area. This remains stable across
/// v1 and v2 so lazy page upgrades do not need to move posting payload bytes.
const DATA_OFFSET: usize = HEADER_END_V1 + SKIP_POINTER_AREA_SIZE_V1;

/// Reserved space for skip pointers in the v2 layout. The v2 header is 4 bytes
/// larger, so the skip reserve shrinks by 4 bytes while keeping DATA_OFFSET
/// unchanged.
const SKIP_POINTER_AREA_SIZE_V2: usize = DATA_OFFSET - HEADER_END_V2;
const MAX_SKIP_POINTERS_V2: usize = SKIP_POINTER_AREA_SIZE_V2 / @sizeOf(SkipPointer);

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

    fn rebuildSkipPointersFromEncoded(self: *Self, data: []u8, meta: *PostingPageMeta) void {
        self.clearSkipPointers(data, meta.*);
        meta.num_skip_pointers = 0;

        if (meta.num_entries < SKIP_INTERVAL) return;

        var offset: usize = meta.data_start;
        var entries_read: u32 = 0;
        var skip_count: usize = 0;

        while (entries_read < meta.num_entries and offset < meta.data_end) {
            const entry_offset = offset;
            const doc_result = decodeVarint(data[offset..]);
            offset += doc_result.bytes;

            if (entries_read > 0 and entries_read % SKIP_INTERVAL == 0 and skip_count < meta.maxSkipPointers()) {
                const skip_ptr = SkipPointer{
                    .doc_id = doc_result.value,
                    .byte_offset = @intCast(entry_offset - meta.data_start),
                    .entry_count = entries_read,
                };
                const skip_offset = meta.skipPointerOffset(skip_count);
                var skip_buf: [@sizeOf(SkipPointer)]u8 = undefined;
                skip_ptr.serialize(&skip_buf);
                @memcpy(data[skip_offset..][0..@sizeOf(SkipPointer)], &skip_buf);
                skip_count += 1;
            }

            const freq_result = decodeVarint(data[offset..]);
            offset += freq_result.bytes;

            if (meta.hasPositions()) {
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

        meta.num_skip_pointers = @intCast(skip_count);
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

        self.rebuildSkipPointersFromEncoded(data, &upgraded);
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

    /// Append a posting entry to a list
    /// Returns the page ID where the entry was stored (may be different if overflow)
    pub fn append(self: *Self, page_id: PageId, entry: PostingEntry) PostingError!PageId {
        return self.appendWithPositions(page_id, entry, false);
    }

    /// Append a posting entry with optional position storage
    pub fn appendWithPositions(self: *Self, page_id: PageId, entry: PostingEntry, store_positions: bool) PostingError!PageId {
        const frame = self.bp.fetchPage(page_id, .exclusive) catch {
            return PostingError.BufferPoolError;
        };
        defer self.bp.unpinPage(frame, true);

        const data = frame.data;
        var posting_header = self.readPageMeta(data, true);

        if (posting_header.layout == .v1) {
            try self.upgradePageToV2(data, &posting_header);
        }

        // Encode the entry - use larger buffer for positions
        var encode_buf: [512]u8 = undefined;
        var encode_len: usize = 0;

        // Encode doc_id and term_freq
        encode_len += encodeVarint(entry.doc_id, encode_buf[encode_len..]);
        encode_len += encodeVarint(entry.term_freq, encode_buf[encode_len..]);

        // Encode positions if requested and available
        const has_positions = store_positions and entry.positions != null;
        if (has_positions) {
            const positions = entry.positions.?;
            // Encode position count
            encode_len += encodeVarint(positions.len, encode_buf[encode_len..]);
            // Encode each position (absolute for now, could delta-encode later)
            for (positions) |pos| {
                encode_len += encodeVarint(pos, encode_buf[encode_len..]);
            }
            // Set has_positions flag
            posting_header.flags |= POSTING_ENTRY_FLAG_HAS_POSITIONS;
        }

        // Check if there's space
        const current_end: usize = posting_header.data_end;
        if (current_end + encode_len > PAGE_SIZE) {
            if (posting_header.next_page != 0) {
                return self.appendWithPositions(posting_header.next_page, entry, store_positions);
            }

            // Need to allocate overflow page
            const new_page_id = try self.create(posting_header.token_id);

            // Link pages and preserve flags
            const new_flags = posting_header.flags;
            posting_header.next_page = new_page_id;
            posting_header.write(data);

            // Create new page with same flags
            const new_frame = self.bp.fetchPage(new_page_id, .exclusive) catch {
                return PostingError.BufferPoolError;
            };
            var new_header = self.readPageMeta(new_frame.data, false);
            new_header.flags = new_flags;
            new_header.write(new_frame.data);
            self.bp.unpinPage(new_frame, true);

            // Recursively append to new page
            return self.appendWithPositions(new_page_id, entry, store_positions);
        }

        // Write skip pointer if at SKIP_INTERVAL boundary
        // Skip pointers point to entries at positions: SKIP_INTERVAL, 2*SKIP_INTERVAL, etc.
        const entry_index = posting_header.num_entries;
        if (entry_index > 0 and entry_index % SKIP_INTERVAL == 0) {
            const skip_index = entry_index / SKIP_INTERVAL - 1;
            if (skip_index < posting_header.maxSkipPointers()) {
                const skip_ptr = SkipPointer{
                    .doc_id = entry.doc_id,
                    .byte_offset = @intCast(current_end - posting_header.data_start),
                    .entry_count = entry_index,
                };
                const skip_offset = posting_header.skipPointerOffset(skip_index);
                var skip_buf: [@sizeOf(SkipPointer)]u8 = undefined;
                skip_ptr.serialize(&skip_buf);
                @memcpy(data[skip_offset..][0..@sizeOf(SkipPointer)], &skip_buf);
                posting_header.num_skip_pointers = @intCast(skip_index + 1);
            }
        }

        // Write entry
        @memcpy(data[current_end..][0..encode_len], encode_buf[0..encode_len]);

        // Update header
        posting_header.num_entries += 1;
        posting_header.data_end = @intCast(current_end + encode_len);
        posting_header.write(data);

        return page_id;
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

                // Rebuild skip pointers
                self.rebuildSkipPointers(data, &header, kept_entries.items, has_positions);

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

    /// Rebuild skip pointers after page modification
    fn rebuildSkipPointers(self: *Self, data: []u8, header: *PostingPageMeta, entries: []const StoredEntry, has_positions: bool) void {
        // Clear existing skip pointers
        self.clearSkipPointers(data, header.*);
        header.num_skip_pointers = 0;

        if (entries.len < SKIP_INTERVAL) {
            return;
        }

        // Scan through entries to build skip pointers
        var offset: usize = header.data_start;
        var skip_count: u16 = 0;

        for (entries, 0..) |entry, idx| {
            // Create skip pointer at SKIP_INTERVAL boundaries (e.g., at entry 128, 256, etc.)
            if (idx > 0 and idx % SKIP_INTERVAL == 0 and skip_count < header.maxSkipPointers()) {
                const skip_ptr = SkipPointer{
                    .doc_id = entry.doc_id,
                    .byte_offset = @intCast(offset - header.data_start),
                    .entry_count = @intCast(idx),
                };
                const skip_offset = header.skipPointerOffset(skip_count);
                var skip_buf: [@sizeOf(SkipPointer)]u8 = undefined;
                skip_ptr.serialize(&skip_buf);
                @memcpy(data[skip_offset..][0..@sizeOf(SkipPointer)], &skip_buf);
                skip_count += 1;
            }

            // Calculate size of this entry to advance offset
            var entry_buf: [256]u8 = undefined;
            var entry_size: usize = 0;
            entry_size += encodeVarint(entry.doc_id, entry_buf[entry_size..]);
            entry_size += encodeVarint(entry.term_freq, entry_buf[entry_size..]);
            if (has_positions) {
                if (entry.positions) |positions| {
                    entry_size += encodeVarint(positions.len, entry_buf[entry_size..]);
                    for (positions) |pos| {
                        entry_size += encodeVarint(pos, entry_buf[entry_size..]);
                    }
                } else {
                    entry_size += encodeVarint(0, entry_buf[entry_size..]);
                }
            }
            offset += entry_size;
        }

        header.num_skip_pointers = skip_count;
    }
};

/// Iterator for reading posting entries
pub const PostingIterator = struct {
    store: *PostingStore,
    current_page: PageId,
    current_offset: usize,
    entries_read: u32,
    total_entries: u32,
    data_start: usize, // Start of posting data (after skip pointers)
    num_skip_pointers: u16,
    skip_pointer_base: usize,
    last_doc_id: DocId, // For delta decoding (not used in simplified version)
    has_positions: bool,
    done: bool,

    const Self = @This();

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
            .num_skip_pointers = header.num_skip_pointers,
            .skip_pointer_base = header.headerEnd(),
            .last_doc_id = 0,
            .has_positions = header.hasPositions(),
            .done = header.num_entries == 0,
        };
    }

    /// Skip to the first entry with doc_id >= target
    /// Returns the entry if found, null if no more entries >= target
    pub fn skipTo(self: *Self, target: DocId) PostingError!?PostingEntry {
        if (self.done) return null;

        // Try to use skip pointers if available
        if (self.num_skip_pointers > 0) {
            const frame = self.store.bp.fetchPage(self.current_page, .shared) catch {
                return PostingError.BufferPoolError;
            };
            defer self.store.bp.unpinPage(frame, false);

            // Binary search skip pointers to find largest doc_id < target
            var best_skip: ?SkipPointer = null;
            var low: usize = 0;
            var high: usize = self.num_skip_pointers;

            while (low < high) {
                const mid = low + (high - low) / 2;
                const skip_offset = self.skip_pointer_base + mid * @sizeOf(SkipPointer);
                const skip_ptr = SkipPointer.deserialize(frame.data[skip_offset..]);

                if (skip_ptr.doc_id < target) {
                    // This skip pointer is before target, could be useful
                    best_skip = skip_ptr;
                    low = mid + 1;
                } else {
                    // This skip pointer is >= target, look earlier
                    high = mid;
                }
            }

            // If we found a useful skip pointer, jump to it
            if (best_skip) |skip| {
                // Only use if it advances us past current position
                const new_offset = self.data_start + skip.byte_offset;
                if (skip.entry_count > self.entries_read) {
                    self.current_offset = new_offset;
                    self.entries_read = skip.entry_count;
                }
            }
        }

        // Linear scan from current position to find target
        while (true) {
            const entry = try self.next();
            if (entry == null) return null;
            if (entry.?.doc_id >= target) return entry;
        }
    }

    /// Skip to target and return entry with positions
    pub fn skipToWithPositions(self: *Self, target: DocId, allocator: Allocator) PostingError!?PostingEntry {
        if (self.done) return null;

        // Try to use skip pointers if available
        if (self.num_skip_pointers > 0) {
            const frame = self.store.bp.fetchPage(self.current_page, .shared) catch {
                return PostingError.BufferPoolError;
            };
            defer self.store.bp.unpinPage(frame, false);

            var best_skip: ?SkipPointer = null;
            var low: usize = 0;
            var high: usize = self.num_skip_pointers;

            while (low < high) {
                const mid = low + (high - low) / 2;
                const skip_offset = self.skip_pointer_base + mid * @sizeOf(SkipPointer);
                const skip_ptr = SkipPointer.deserialize(frame.data[skip_offset..]);

                if (skip_ptr.doc_id < target) {
                    best_skip = skip_ptr;
                    low = mid + 1;
                } else {
                    high = mid;
                }
            }

            if (best_skip) |skip| {
                const new_offset = self.data_start + skip.byte_offset;
                if (skip.entry_count > self.entries_read) {
                    self.current_offset = new_offset;
                    self.entries_read = skip.entry_count;
                }
            }
        }

        // Linear scan from current position to find target
        while (true) {
            const entry = try self.nextWithPositions(allocator);
            if (entry == null) return null;
            if (entry.?.doc_id >= target) return entry;
        }
    }

    /// Get the next posting entry (without positions)
    pub fn next(self: *Self) PostingError!?PostingEntry {
        return self.nextInternal(null);
    }

    /// Get the next posting entry with positions (caller must free positions)
    pub fn nextWithPositions(self: *Self, allocator: Allocator) PostingError!?PostingEntry {
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
                self.num_skip_pointers = next_header.num_skip_pointers;
                self.skip_pointer_base = next_header.headerEnd();
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

test "skip pointers and skipTo" {
    const allocator = std.testing.allocator;

    const vfs = lattice.storage.vfs;
    const page_manager = lattice.storage.page_manager;

    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const db_path = "/tmp/lattice_skip_pointer_test.db";
    vfs_impl.delete(db_path) catch {};

    var pm = try page_manager.PageManager.init(allocator, vfs_impl, db_path, .{ .create = true });
    defer {
        pm.deinit();
        vfs_impl.delete(db_path) catch {};
    }

    var bp = try BufferPool.init(allocator, &pm, 128 * 4096);
    defer bp.deinit();

    var store = PostingStore.init(allocator, &bp);

    // Create posting list
    const page_id = try store.create(1);

    // Add 200 entries (more than SKIP_INTERVAL = 128 to trigger skip pointer creation)
    // doc_ids: 10, 20, 30, ..., 2000
    var i: u64 = 1;
    while (i <= 200) : (i += 1) {
        _ = try store.append(page_id, .{
            .doc_id = i * 10,
            .term_freq = @intCast(i % 5 + 1),
            .positions = null,
        });
    }

    // Verify skip pointer was created (at entry 128)
    const frame = try bp.fetchPage(page_id, .shared);
    defer bp.unpinPage(frame, false);
    const header = PostingPageHeader.read(frame.data);
    try std.testing.expect(header.num_skip_pointers >= 1);

    // Test skipTo - skip to doc_id 1500
    var iter = try store.iterate(page_id);
    defer iter.deinit();

    const entry = try iter.skipTo(1500);
    try std.testing.expect(entry != null);
    try std.testing.expectEqual(@as(DocId, 1500), entry.?.doc_id);

    // Continue iteration should give next entry
    const next = try iter.next();
    try std.testing.expect(next != null);
    try std.testing.expectEqual(@as(DocId, 1510), next.?.doc_id);

    // Test skipTo past all entries
    var iter2 = try store.iterate(page_id);
    defer iter2.deinit();

    const no_entry = try iter2.skipTo(3000);
    try std.testing.expect(no_entry == null);

    // Test skipTo to exact entry
    var iter3 = try store.iterate(page_id);
    defer iter3.deinit();

    const exact = try iter3.skipTo(500);
    try std.testing.expect(exact != null);
    try std.testing.expectEqual(@as(DocId, 500), exact.?.doc_id);
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
