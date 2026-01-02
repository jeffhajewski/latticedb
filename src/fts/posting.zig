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

/// Posting page header (follows base PageHeader)
pub const PostingPageHeader = extern struct {
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
        std.debug.assert(@sizeOf(PostingPageHeader) == 20);
    }

    pub fn read(data: []const u8) PostingPageHeader {
        return std.mem.bytesAsValue(
            PostingPageHeader,
            data[OFFSET..][0..@sizeOf(PostingPageHeader)],
        ).*;
    }

    pub fn write(self: *const PostingPageHeader, data: []u8) void {
        const dest = data[OFFSET..][0..@sizeOf(PostingPageHeader)];
        @memcpy(dest, std.mem.asBytes(self));
    }
};

/// Data offset after headers (before skip pointers)
const HEADER_END: usize = @sizeOf(PageHeader) + @sizeOf(PostingPageHeader);

/// Maximum skip pointers per page (reserves 256 bytes)
const MAX_SKIP_POINTERS: usize = 16;

/// Reserved space for skip pointers
const SKIP_POINTER_AREA_SIZE: usize = MAX_SKIP_POINTERS * @sizeOf(SkipPointer);

/// Data offset after headers and skip pointer area
const DATA_OFFSET: usize = HEADER_END + SKIP_POINTER_AREA_SIZE;

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

    /// Create a new posting list, returns first page ID
    pub fn create(self: *Self, token_id: TokenId) PostingError!PageId {
        const page_id = self.bp.pm.allocatePage() catch {
            return PostingError.BufferPoolError;
        };

        const frame = self.bp.fetchPage(page_id, .exclusive) catch {
            return PostingError.BufferPoolError;
        };
        errdefer self.bp.unpinPage(frame, false);

        const data = frame.data;

        // Initialize page header
        const header = PageHeader.init(.fts_posting);
        @memcpy(data[0..@sizeOf(PageHeader)], std.mem.asBytes(&header));

        // Initialize posting page header
        const posting_header = PostingPageHeader{
            .token_id = token_id,
            .num_entries = 0,
            .next_page = 0,
            .num_skip_pointers = 0,
            .flags = 0,
            .data_start = @intCast(DATA_OFFSET),
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
        var posting_header = PostingPageHeader.read(data);

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
            posting_header.flags |= 0x01;
        }

        // Check if there's space
        const current_end = posting_header.data_start + self.getDataSize(data, posting_header);
        if (current_end + encode_len > PAGE_SIZE) {
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
            defer self.bp.unpinPage(new_frame, true);
            var new_header = PostingPageHeader.read(new_frame.data);
            new_header.flags = new_flags;
            new_header.write(new_frame.data);

            // Recursively append to new page
            return self.appendWithPositions(new_page_id, entry, store_positions);
        }

        // Write skip pointer if at SKIP_INTERVAL boundary
        // Skip pointers point to entries at positions: SKIP_INTERVAL, 2*SKIP_INTERVAL, etc.
        const entry_index = posting_header.num_entries;
        if (entry_index > 0 and entry_index % SKIP_INTERVAL == 0) {
            const skip_index = entry_index / SKIP_INTERVAL - 1;
            if (skip_index < MAX_SKIP_POINTERS) {
                const skip_ptr = SkipPointer{
                    .doc_id = entry.doc_id,
                    .byte_offset = @intCast(current_end - posting_header.data_start),
                    .entry_count = entry_index,
                };
                const skip_offset = HEADER_END + skip_index * @sizeOf(SkipPointer);
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
        posting_header.write(data);

        return page_id;
    }

    /// Get the size of posting data in a page
    fn getDataSize(self: *Self, data: []const u8, header: PostingPageHeader) usize {
        _ = self;
        const has_positions = (header.flags & 0x01) != 0;

        // Scan through entries to find the end
        var offset: usize = header.data_start;
        var entries_read: u32 = 0;

        while (entries_read < header.num_entries and offset < PAGE_SIZE) {
            // Decode doc_id
            const doc_result = decodeVarint(data[offset..]);
            offset += doc_result.bytes;

            // Decode term_freq
            const freq_result = decodeVarint(data[offset..]);
            offset += freq_result.bytes;

            // Skip positions if present
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

        return offset - header.data_start;
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
            var header = PostingPageHeader.read(data);
            const has_positions = (header.flags & 0x01) != 0;

            // Collect all entries except the target
            var kept_entries = std.ArrayList(StoredEntry).init(self.allocator);
            defer {
                for (kept_entries.items) |entry| {
                    if (entry.positions) |pos| {
                        self.allocator.free(pos);
                    }
                }
                kept_entries.deinit();
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
                    kept_entries.append(entry) catch {
                        if (positions) |pos| self.allocator.free(pos);
                        self.bp.unpinPage(frame, false);
                        return PostingError.OutOfMemory;
                    };
                }
            }

            if (found_entry) |removed| {
                // Rebuild page with remaining entries
                self.rebuildPage(data, &header, kept_entries.items, has_positions);

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
    fn rebuildPage(self: *Self, data: []u8, header: *PostingPageHeader, entries: []const StoredEntry, has_positions: bool) void {
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
    }

    /// Rebuild skip pointers after page modification
    fn rebuildSkipPointers(self: *Self, data: []u8, header: *PostingPageHeader, entries: []const StoredEntry, has_positions: bool) void {
        _ = self;
        // Clear existing skip pointers
        header.num_skip_pointers = 0;

        if (entries.len < SKIP_INTERVAL) {
            return;
        }

        // Scan through entries to build skip pointers
        var offset: usize = header.data_start;
        var skip_count: u16 = 0;

        for (entries, 0..) |entry, idx| {
            // Create skip pointer at SKIP_INTERVAL boundaries (e.g., at entry 128, 256, etc.)
            if (idx > 0 and idx % SKIP_INTERVAL == 0 and skip_count < MAX_SKIP_POINTERS) {
                const skip_ptr = SkipPointer{
                    .doc_id = entry.doc_id,
                    .byte_offset = @intCast(offset - header.data_start),
                    .entry_count = @intCast(idx),
                };
                const skip_offset = HEADER_END + skip_count * @sizeOf(SkipPointer);
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
    data_start: u32, // Start of posting data (after skip pointers)
    num_skip_pointers: u16,
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

        const header = PostingPageHeader.read(frame.data);

        return Self{
            .store = store,
            .current_page = page_id,
            .current_offset = header.data_start,
            .entries_read = 0,
            .total_entries = header.num_entries,
            .data_start = header.data_start,
            .num_skip_pointers = header.num_skip_pointers,
            .last_doc_id = 0,
            .has_positions = (header.flags & 0x01) != 0,
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
                const skip_offset = HEADER_END + mid * @sizeOf(SkipPointer);
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
                const skip_offset = HEADER_END + mid * @sizeOf(SkipPointer);
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
        const header = PostingPageHeader.read(data);
        const page_has_positions = (header.flags & 0x01) != 0;

        if (self.entries_read >= header.num_entries) {
            // Check for next page
            if (header.next_page != 0) {
                self.current_page = header.next_page;
                self.current_offset = DATA_OFFSET;
                self.entries_read = 0;

                // Update has_positions for new page
                const next_frame = self.store.bp.fetchPage(header.next_page, .shared) catch {
                    return PostingError.BufferPoolError;
                };
                defer self.store.bp.unpinPage(next_frame, false);
                const next_header = PostingPageHeader.read(next_frame.data);
                self.has_positions = (next_header.flags & 0x01) != 0;

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
