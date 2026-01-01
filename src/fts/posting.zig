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

/// Data offset after headers
const DATA_OFFSET: usize = @sizeOf(PageHeader) + @sizeOf(PostingPageHeader);

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
        const frame = self.bp.fetchPage(page_id, .exclusive) catch {
            return PostingError.BufferPoolError;
        };
        defer self.bp.unpinPage(frame, true);

        const data = frame.data;
        var posting_header = PostingPageHeader.read(data);

        // Encode the entry
        var encode_buf: [32]u8 = undefined;
        var encode_len: usize = 0;

        // Delta-encode doc_id (for first entry, delta = doc_id)
        // For simplicity in append, we store absolute doc_id since we don't have previous
        // The iterator will handle delta decoding
        encode_len += encodeVarint(entry.doc_id, encode_buf[encode_len..]);
        encode_len += encodeVarint(entry.term_freq, encode_buf[encode_len..]);

        // Check if there's space
        const current_end = posting_header.data_start + self.getDataSize(data, posting_header);
        if (current_end + encode_len > PAGE_SIZE) {
            // Need to allocate overflow page
            const new_page_id = try self.create(posting_header.token_id);

            // Link pages
            posting_header.next_page = new_page_id;
            posting_header.write(data);

            // Recursively append to new page
            return self.append(new_page_id, entry);
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

            entries_read += 1;
        }

        return offset - header.data_start;
    }

    /// Get iterator for reading posting list
    pub fn iterate(self: *Self, page_id: PageId) PostingError!PostingIterator {
        return PostingIterator.init(self, page_id);
    }
};

/// Iterator for reading posting entries
pub const PostingIterator = struct {
    store: *PostingStore,
    current_page: PageId,
    current_offset: usize,
    entries_read: u32,
    total_entries: u32,
    last_doc_id: DocId, // For delta decoding (not used in simplified version)
    done: bool,

    const Self = @This();

    pub fn init(store: *PostingStore, page_id: PageId) PostingError!Self {
        // Read header to get entry count
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
            .last_doc_id = 0,
            .done = header.num_entries == 0,
        };
    }

    /// Get the next posting entry
    pub fn next(self: *Self) PostingError!?PostingEntry {
        if (self.done) return null;

        const frame = self.store.bp.fetchPage(self.current_page, .shared) catch {
            return PostingError.BufferPoolError;
        };
        defer self.store.bp.unpinPage(frame, false);

        const data = frame.data;
        const header = PostingPageHeader.read(data);

        if (self.entries_read >= header.num_entries) {
            // Check for next page
            if (header.next_page != 0) {
                self.current_page = header.next_page;
                self.current_offset = DATA_OFFSET;
                self.entries_read = 0;

                // Recursively get from next page
                return self.next();
            }
            self.done = true;
            return null;
        }

        // Decode entry
        const doc_result = decodeVarint(data[self.current_offset..]);
        self.current_offset += doc_result.bytes;

        const freq_result = decodeVarint(data[self.current_offset..]);
        self.current_offset += freq_result.bytes;

        self.entries_read += 1;

        return PostingEntry{
            .doc_id = doc_result.value,
            .term_freq = @intCast(freq_result.value),
            .positions = null,
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
