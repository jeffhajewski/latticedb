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
    /// Total occurrences across all documents
    total_freq: u64,
    /// Unique token identifier
    token_id: TokenId,
    /// Number of documents containing this token
    doc_freq: u32,
    /// First page of posting list (0 = no postings yet)
    posting_page: PageId,
    /// Explicit padding for 8-byte alignment
    _padding: u32 = 0,

    comptime {
        std.debug.assert(@sizeOf(DictionaryEntry) == 24);
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
    tree: *BTree,
    next_token_id: TokenId,

    const Self = @This();

    /// Initialize dictionary with a B+Tree
    pub fn init(allocator: Allocator, tree: *BTree) Self {
        return Self{
            .allocator = allocator,
            .tree = tree,
            .next_token_id = FIRST_TOKEN_ID,
        };
    }

    /// Get or create a token entry
    /// Returns the TokenId (existing or newly created)
    pub fn getOrCreate(self: *Self, token: []const u8) DictionaryError!TokenId {
        // Check if token already exists
        if (try self.get(token)) |entry| {
            return entry.token_id;
        }

        // Create new entry
        const new_id = self.next_token_id;
        self.next_token_id += 1;

        const entry = DictionaryEntry{
            .token_id = new_id,
            .doc_freq = 0,
            .total_freq = 0,
            .posting_page = 0,
        };

        var buf: [@sizeOf(DictionaryEntry)]u8 = undefined;
        entry.serialize(&buf);

        self.tree.insert(token, &buf) catch |err| {
            return mapBTreeError(err);
        };

        return new_id;
    }

    /// Get token entry (returns null if not found)
    pub fn get(self: *Self, token: []const u8) DictionaryError!?DictionaryEntry {
        const result = self.tree.get(token) catch |err| {
            return mapBTreeError(err);
        };

        if (result) |value| {
            if (value.len < @sizeOf(DictionaryEntry)) {
                return DictionaryError.InvalidData;
            }
            return DictionaryEntry.deserialize(value);
        }

        return null;
    }

    /// Update an entry in the dictionary
    pub fn update(self: *Self, token: []const u8, entry: DictionaryEntry) DictionaryError!void {
        var buf: [@sizeOf(DictionaryEntry)]u8 = undefined;
        entry.serialize(&buf);

        // Delete existing entry first (ignore NotFound)
        self.tree.delete(token) catch |err| {
            if (err != BTreeError.KeyNotFound) {
                return mapBTreeError(err);
            }
        };

        // Insert updated entry
        self.tree.insert(token, &buf) catch |err| {
            return mapBTreeError(err);
        };
    }

    /// Increment document frequency for a token
    pub fn incrementDocFreq(self: *Self, token: []const u8) DictionaryError!void {
        const entry = try self.get(token) orelse return DictionaryError.NotFound;

        const updated = DictionaryEntry{
            .token_id = entry.token_id,
            .doc_freq = entry.doc_freq + 1,
            .total_freq = entry.total_freq,
            .posting_page = entry.posting_page,
        };

        try self.update(token, updated);
    }

    /// Decrement document frequency for a token
    pub fn decrementDocFreq(self: *Self, token: []const u8) DictionaryError!void {
        const entry = try self.get(token) orelse return DictionaryError.NotFound;

        const updated = DictionaryEntry{
            .token_id = entry.token_id,
            .doc_freq = if (entry.doc_freq > 0) entry.doc_freq - 1 else 0,
            .total_freq = entry.total_freq,
            .posting_page = entry.posting_page,
        };

        try self.update(token, updated);
    }

    /// Add to total frequency count
    pub fn addTotalFreq(self: *Self, token: []const u8, count: u64) DictionaryError!void {
        const entry = try self.get(token) orelse return DictionaryError.NotFound;

        const updated = DictionaryEntry{
            .token_id = entry.token_id,
            .doc_freq = entry.doc_freq,
            .total_freq = entry.total_freq + count,
            .posting_page = entry.posting_page,
        };

        try self.update(token, updated);
    }

    /// Subtract from total frequency count (for document removal)
    pub fn subtractTotalFreq(self: *Self, token: []const u8, count: u64) DictionaryError!void {
        const entry = try self.get(token) orelse return DictionaryError.NotFound;

        const updated = DictionaryEntry{
            .token_id = entry.token_id,
            .doc_freq = entry.doc_freq,
            .total_freq = if (entry.total_freq >= count) entry.total_freq - count else 0,
            .posting_page = entry.posting_page,
        };

        try self.update(token, updated);
    }

    /// Set the posting page for a token
    pub fn setPostingPage(self: *Self, token: []const u8, page_id: PageId) DictionaryError!void {
        const entry = try self.get(token) orelse return DictionaryError.NotFound;

        const updated = DictionaryEntry{
            .token_id = entry.token_id,
            .doc_freq = entry.doc_freq,
            .total_freq = entry.total_freq,
            .posting_page = page_id,
        };

        try self.update(token, updated);
    }

    /// Check if a token exists in the dictionary
    pub fn contains(self: *Self, token: []const u8) bool {
        return if (self.get(token)) |entry| entry != null else |_| false;
    }

    /// Get the number of unique tokens
    pub fn tokenCount(self: *Self) u32 {
        if (self.next_token_id <= FIRST_TOKEN_ID) return 0;
        return self.next_token_id - FIRST_TOKEN_ID;
    }

    /// Iterator over all dictionary entries
    pub const DictionaryIterator = struct {
        tree_iter: BTree.Iterator,

        const IterSelf = @This();

        /// Item returned by iterator
        pub const Item = struct {
            term: []const u8,
            entry: DictionaryEntry,
        };

        /// Get next entry from dictionary
        pub fn next(self: *IterSelf) DictionaryError!?Item {
            const tree_item = self.tree_iter.next() catch |err| {
                return mapBTreeError(err);
            };

            if (tree_item) |item| {
                if (item.value.len < @sizeOf(DictionaryEntry)) {
                    return DictionaryError.InvalidData;
                }
                return Item{
                    .term = item.key,
                    .entry = DictionaryEntry.deserialize(item.value),
                };
            }

            return null;
        }

        /// Clean up iterator resources
        pub fn deinit(self: *IterSelf) void {
            self.tree_iter.deinit();
        }
    };

    /// Create an iterator over all dictionary entries
    /// Iterator must be deinit'd when done
    pub fn iterate(self: *Self) DictionaryError!DictionaryIterator {
        const tree_iter = self.tree.range(null, null) catch |err| {
            return mapBTreeError(err);
        };

        return DictionaryIterator{
            .tree_iter = tree_iter,
        };
    }

    /// Create an iterator over dictionary entries in a key range
    /// Both start_key and end_key are optional (null means unbounded)
    /// Iterator must be deinit'd when done
    pub fn iterateRange(
        self: *Self,
        start_key: ?[]const u8,
        end_key: ?[]const u8,
    ) DictionaryError!DictionaryIterator {
        const tree_iter = self.tree.range(start_key, end_key) catch |err| {
            return mapBTreeError(err);
        };

        return DictionaryIterator{
            .tree_iter = tree_iter,
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

test "dictionary basic operations" {
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
    var dict = Dictionary.init(allocator, &tree);

    // Get or create tokens
    const id1 = try dict.getOrCreate("hello");
    const id2 = try dict.getOrCreate("world");
    const id3 = try dict.getOrCreate("hello"); // Should return existing

    try std.testing.expectEqual(FIRST_TOKEN_ID, id1);
    try std.testing.expectEqual(FIRST_TOKEN_ID + 1, id2);
    try std.testing.expectEqual(id1, id3); // Same as first

    // Verify token count
    try std.testing.expectEqual(@as(u32, 2), dict.tokenCount());

    // Lookup existing token
    const entry = try dict.get("hello");
    try std.testing.expect(entry != null);
    try std.testing.expectEqual(id1, entry.?.token_id);

    // Lookup non-existent token
    const missing = try dict.get("nonexistent");
    try std.testing.expect(missing == null);
}

test "dictionary doc freq operations" {
    const allocator = std.testing.allocator;

    const vfs = lattice.storage.vfs;
    const buffer_pool = lattice.storage.buffer_pool;
    const page_manager = lattice.storage.page_manager;

    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const db_path = "/tmp/lattice_dictionary_docfreq_test.db";
    vfs_impl.delete(db_path) catch {};

    var pm = try page_manager.PageManager.init(allocator, vfs_impl, db_path, .{ .create = true });
    defer {
        pm.deinit();
        vfs_impl.delete(db_path) catch {};
    }

    var bp = try buffer_pool.BufferPool.init(allocator, &pm, 64 * 4096);
    defer bp.deinit();

    var tree = try BTree.init(allocator, &bp);
    var dict = Dictionary.init(allocator, &tree);

    // Create a token
    _ = try dict.getOrCreate("test");

    // Initial doc_freq should be 0
    var entry = (try dict.get("test")).?;
    try std.testing.expectEqual(@as(u32, 0), entry.doc_freq);

    // Increment doc freq
    try dict.incrementDocFreq("test");
    try dict.incrementDocFreq("test");

    entry = (try dict.get("test")).?;
    try std.testing.expectEqual(@as(u32, 2), entry.doc_freq);

    // Decrement doc freq
    try dict.decrementDocFreq("test");

    entry = (try dict.get("test")).?;
    try std.testing.expectEqual(@as(u32, 1), entry.doc_freq);
}
