//! Term Dictionary for Full-Text Search.
//!
//! Maps normalized tokens to TokenId with document frequency tracking.
//! Uses a B+Tree for persistent, sorted token storage.
//!
//! Key: token ([]const u8)
//! Value: DictionaryEntry (serialized)

const std = @import("std");
const lattice = @import("lattice");

const Allocator = std.mem.Allocator;

const btree = lattice.storage.btree;
const BTree = btree.BTree;
const ScopedTree = @import("scoped_tree.zig").ScopedTree;
const BTreeError = btree.BTreeError;
const PageId = lattice.core.types.PageId;

/// Token identifier type (supports ~4 billion unique tokens)
pub const TokenId = u32;

/// First user token ID (0 is reserved for invalid/null)
pub const FIRST_TOKEN_ID: TokenId = 1;

/// Dictionary errors
pub const DictionaryError = error{
    /// Token not found
    NotFound,
    /// I/O error
    IoError,
    /// Out of memory
    OutOfMemory,
    /// B+Tree error
    BTreeError,
    /// Invalid data format
    InvalidData,
};

/// Dictionary entry stored in B+Tree (24 bytes)
/// Field order optimized for alignment (u64 first to avoid internal padding)
pub const DictionaryEntry = extern struct {
    /// Occurrences across the documents in the posting list. Releases up to
    /// 0.16 never added to it, so an entry they wrote undercounts (removal
    /// stops at zero). Nothing reads it.
    total_freq: u64,
    /// Identifies the posting list. Entries written now use the head page
    /// id, which is unique while the list exists, and the list's pages carry
    /// it in their headers (see `PostingStore.appendAt`). Earlier releases
    /// assigned a counter that restarted at every open, so their ids are
    /// labels and nothing more.
    token_id: TokenId,
    /// Number of documents containing this token
    doc_freq: u32,
    /// First page of posting list (0 = no postings yet)
    posting_page: PageId,
    /// The list's last page as last recorded, so an append goes straight to
    /// it; 0 when not known. This was padding, and releases up to 0.16 write
    /// it as 0 whenever they rewrite an entry, so a write by one of them
    /// leaves no stale tail behind. The page is checked before it is used.
    tail_page: PageId = 0,

    comptime {
        std.debug.assert(@sizeOf(DictionaryEntry) == 24);
        std.debug.assert(@offsetOf(DictionaryEntry, "tail_page") == 20);
    }

    /// Serialize entry to bytes
    pub fn serialize(self: *const DictionaryEntry, buf: *[@sizeOf(DictionaryEntry)]u8) void {
        @memcpy(buf, std.mem.asBytes(self));
    }

    /// Deserialize entry from bytes
    pub fn deserialize(buf: []const u8) DictionaryEntry {
        return std.mem.bytesAsValue(
            DictionaryEntry,
            buf[0..@sizeOf(DictionaryEntry)],
        ).*;
    }
};

/// Term dictionary manager
pub const Dictionary = struct {
    allocator: Allocator,
    tree: ScopedTree,

    const Self = @This();

    /// Initialize dictionary with a B+Tree
    pub fn init(allocator: Allocator, tree: ScopedTree) Self {
        return Self{
            .allocator = allocator,
            .tree = tree,
        };
    }

    /// Get token entry (returns null if not found)
    pub fn get(self: *Self, token: []const u8) DictionaryError!?DictionaryEntry {
        var borrowed = (self.tree.getBorrowed(token) catch |err| {
            return mapBTreeError(err);
        }) orelse return null;
        defer borrowed.release();
        if (borrowed.bytes.len < @sizeOf(DictionaryEntry)) {
            return DictionaryError.InvalidData;
        }
        return DictionaryEntry.deserialize(borrowed.bytes);
    }

    /// Write an entry, replacing any the token has: one tree descent.
    pub fn put(self: *Self, token: []const u8, entry: DictionaryEntry) DictionaryError!void {
        var buf: [@sizeOf(DictionaryEntry)]u8 = undefined;
        entry.serialize(&buf);
        self.tree.put(token, &buf) catch |err| {
            return mapBTreeError(err);
        };
    }

    /// Remove a token's entry. `NotFound` if it has none.
    pub fn remove(self: *Self, token: []const u8) DictionaryError!void {
        self.tree.delete(token) catch |err| {
            return mapBTreeError(err);
        };
    }

    /// Check if a token exists in the dictionary
    pub fn contains(self: *Self, token: []const u8) bool {
        return if (self.get(token)) |entry| entry != null else |_| false;
    }

    /// Number of terms this index holds. Walks the dictionary: the count is
    /// not stored, and only statistics ask for it.
    pub fn countTerms(self: *Self) DictionaryError!u32 {
        var iter: DictionaryIterator = undefined;
        try self.iterate(&iter);
        defer iter.deinit();
        var count: u32 = 0;
        while (try iter.next()) |_| count += 1;
        return count;
    }

    /// Iterator over all dictionary entries
    pub const DictionaryIterator = struct {
        /// Owns its bounds. The tree's iterator compares against the end key on
        /// every step, so a copy of `inner` alone would be left pointing at a
        /// buffer that died with the call that built it.
        scoped_iter: ScopedTree.Iterator,
        /// The view the walk came from, to take the scope prefix back off the
        /// keys it returns.
        view: ScopedTree,

        const IterSelf = @This();

        /// Item returned by iterator
        pub const Item = struct {
            term: []const u8,
            entry: DictionaryEntry,
        };

        /// Get next entry from dictionary
        pub fn next(self: *IterSelf) DictionaryError!?Item {
            const tree_item = self.scoped_iter.next() catch |err| {
                return mapBTreeError(err);
            };

            if (tree_item) |item| {
                if (item.value.len < @sizeOf(DictionaryEntry)) {
                    return DictionaryError.InvalidData;
                }
                // Keys come out of the tree carrying the prefix that scopes
                // them to one index. Callers want the term, and one of them
                // measures edit distance against it: four bytes of index
                // identity on the front puts every term far outside any
                // sensible distance, so nothing ever matches.
                const term = self.view.callerKey(item.key) orelse {
                    return DictionaryError.InvalidData;
                };
                return Item{
                    .term = term,
                    .entry = DictionaryEntry.deserialize(item.value),
                };
            }

            return null;
        }

        /// Clean up iterator resources
        pub fn deinit(self: *IterSelf) void {
            self.scoped_iter.deinit();
        }
    };

    /// Create an iterator over all dictionary entries
    /// Iterator must be deinit'd when done
    /// Walk this index's terms, writing the iterator into `out`.
    ///
    /// The iterator carries the bounds its walk compares against, so it cannot be
    /// returned by value: copying the struct moves those buffers and leaves the
    /// underlying tree iterator comparing against the copy that was left behind.
    /// This function used to do exactly that, and it went unnoticed because the
    /// only caller was fuzzy search, which ran against the unscoped index where
    /// there are no bounds to dangle. Scoping fuzzy search is what surfaced it.
    pub fn iterate(self: *Self, out: *DictionaryIterator) DictionaryError!void {
        out.view = self.tree;
        self.tree.iterateAll(&out.scoped_iter) catch |err| {
            return mapBTreeError(err);
        };
    }

    /// The same over a key range. Both bounds are optional; null is unbounded.
    pub fn iterateRange(
        self: *Self,
        start_key: ?[]const u8,
        end_key: ?[]const u8,
        out: *DictionaryIterator,
    ) DictionaryError!void {
        out.view = self.tree;
        self.tree.rangeOwned(start_key, end_key, &out.scoped_iter) catch |err| {
            return mapBTreeError(err);
        };
    }
};

/// Map B+Tree errors to Dictionary errors
fn mapBTreeError(err: BTreeError) DictionaryError {
    return switch (err) {
        BTreeError.KeyNotFound => DictionaryError.NotFound,
        BTreeError.OutOfMemory => DictionaryError.OutOfMemory,
        else => DictionaryError.BTreeError,
    };
}

// ============================================================================
// Tests
// ============================================================================

test "dictionary entry serialization" {
    const entry = DictionaryEntry{
        .token_id = 12345,
        .doc_freq = 100,
        .total_freq = 50000,
        .posting_page = 42,
    };

    var buf: [@sizeOf(DictionaryEntry)]u8 = undefined;
    entry.serialize(&buf);

    const parsed = DictionaryEntry.deserialize(&buf);
    try std.testing.expectEqual(entry.token_id, parsed.token_id);
    try std.testing.expectEqual(entry.doc_freq, parsed.doc_freq);
    try std.testing.expectEqual(entry.total_freq, parsed.total_freq);
    try std.testing.expectEqual(entry.posting_page, parsed.posting_page);
}

test "dictionary put, get, remove and count" {
    const allocator = std.testing.allocator;

    const vfs = lattice.storage.vfs;
    const buffer_pool = lattice.storage.buffer_pool;
    const page_manager = lattice.storage.page_manager;

    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const db_path = "/tmp/lattice_dictionary_test.db";
    vfs_impl.delete(db_path) catch {};

    var pm = try page_manager.PageManager.init(allocator, vfs_impl, db_path, .{ .create = true });
    defer {
        pm.deinit();
        vfs_impl.delete(db_path) catch {};
    }

    var bp = try buffer_pool.BufferPool.init(allocator, &pm, 64 * 4096);
    defer bp.deinit();

    var tree = try BTree.init(allocator, &bp);
    var dict = Dictionary.init(allocator, ScopedTree.unscoped(&tree));

    try dict.put("hello", .{ .token_id = 7, .doc_freq = 1, .total_freq = 2, .posting_page = 7, .tail_page = 9 });
    try dict.put("world", .{ .token_id = 8, .doc_freq = 1, .total_freq = 1, .posting_page = 8 });
    try std.testing.expectEqual(@as(u32, 2), try dict.countTerms());

    // A put on an existing token replaces its entry.
    try dict.put("hello", .{ .token_id = 7, .doc_freq = 2, .total_freq = 5, .posting_page = 7, .tail_page = 11 });
    const entry = (try dict.get("hello")).?;
    try std.testing.expectEqual(@as(u32, 2), entry.doc_freq);
    try std.testing.expectEqual(@as(u64, 5), entry.total_freq);
    try std.testing.expectEqual(@as(PageId, 11), entry.tail_page);
    try std.testing.expectEqual(@as(u32, 2), try dict.countTerms());

    try std.testing.expect((try dict.get("nonexistent")) == null);

    try dict.remove("hello");
    try std.testing.expect((try dict.get("hello")) == null);
    try std.testing.expectError(DictionaryError.NotFound, dict.remove("hello"));
    try std.testing.expectEqual(@as(u32, 1), try dict.countTerms());
}

test "an entry written by 0.16.0 reads with no tail page" {
    // 0.16.0 wrote the last four bytes as zero padding.
    var buf = [_]u8{0} ** @sizeOf(DictionaryEntry);
    std.mem.writeInt(u64, buf[0..8], 3, .little);
    std.mem.writeInt(u32, buf[8..12], 1, .little);
    std.mem.writeInt(u32, buf[12..16], 2, .little);
    std.mem.writeInt(u32, buf[16..20], 42, .little);
    const entry = DictionaryEntry.deserialize(&buf);
    try std.testing.expectEqual(@as(PageId, 42), entry.posting_page);
    try std.testing.expectEqual(@as(u32, 2), entry.doc_freq);
    try std.testing.expectEqual(@as(PageId, 0), entry.tail_page);
}
