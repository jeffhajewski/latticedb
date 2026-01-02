//! Reverse Index for FTS Document Deletion.
//!
//! Maps doc_id -> [terms] to enable efficient document removal.
//! Uses B+Tree storage with length-prefixed term lists.
//!
//! Key: doc_id (8 bytes, little-endian)
//! Value: num_terms (u16) | [term_len (u16) | term_bytes]...

const std = @import("std");
const lattice = @import("lattice");

const Allocator = std.mem.Allocator;
const ArrayList = std.ArrayList;

const btree = lattice.storage.btree;
const BTree = btree.BTree;
const BTreeError = btree.BTreeError;
const NodeId = lattice.core.types.NodeId;

/// Document ID (alias for NodeId)
pub const DocId = NodeId;

/// Reverse index errors
pub const ReverseIndexError = error{
    /// Document not found
    NotFound,
    /// I/O error
    IoError,
    /// Out of memory
    OutOfMemory,
    /// B+Tree error
    BTreeError,
    /// Invalid data format
    InvalidData,
    /// Value too large to store
    ValueTooLarge,
};

/// Maximum total size for serialized term list (limit to keep values reasonable)
const MAX_VALUE_SIZE: usize = 32768; // 32KB max per document

/// Reverse index for document term tracking
pub const ReverseIndex = struct {
    allocator: Allocator,
    tree: *BTree,

    const Self = @This();

    /// Initialize reverse index with a B+Tree
    pub fn init(allocator: Allocator, tree: *BTree) Self {
        return Self{
            .allocator = allocator,
            .tree = tree,
        };
    }

    /// Store the list of terms for a document
    /// Overwrites any existing entry for this doc_id
    pub fn setDocTerms(self: *Self, doc_id: DocId, terms: []const []const u8) ReverseIndexError!void {
        // Calculate required size
        var total_size: usize = 2; // num_terms (u16)
        for (terms) |term| {
            total_size += 2 + term.len; // term_len (u16) + term bytes
        }

        if (total_size > MAX_VALUE_SIZE) {
            return ReverseIndexError.ValueTooLarge;
        }

        // Serialize key (doc_id as 8 bytes little-endian)
        var key_buf: [8]u8 = undefined;
        std.mem.writeInt(u64, &key_buf, doc_id, .little);

        // Serialize value
        const value_buf = self.allocator.alloc(u8, total_size) catch {
            return ReverseIndexError.OutOfMemory;
        };
        defer self.allocator.free(value_buf);

        var offset: usize = 0;

        // Write num_terms
        const num_terms: u16 = @intCast(terms.len);
        std.mem.writeInt(u16, value_buf[offset..][0..2], num_terms, .little);
        offset += 2;

        // Write each term
        for (terms) |term| {
            const term_len: u16 = @intCast(term.len);
            std.mem.writeInt(u16, value_buf[offset..][0..2], term_len, .little);
            offset += 2;
            @memcpy(value_buf[offset..][0..term.len], term);
            offset += term.len;
        }

        // Delete existing entry first (ignore NotFound)
        self.tree.delete(&key_buf) catch |err| {
            if (err != BTreeError.KeyNotFound) {
                return mapBTreeError(err);
            }
        };

        // Insert new entry
        self.tree.insert(&key_buf, value_buf) catch |err| {
            return mapBTreeError(err);
        };
    }

    /// Get the list of terms for a document
    /// Caller must free the returned slice and each term within it
    pub fn getDocTerms(self: *Self, doc_id: DocId) ReverseIndexError!?[][]const u8 {
        // Serialize key
        var key_buf: [8]u8 = undefined;
        std.mem.writeInt(u64, &key_buf, doc_id, .little);

        const result = self.tree.get(&key_buf) catch |err| {
            return mapBTreeError(err);
        };

        if (result) |value| {
            return try self.deserializeTerms(value);
        }

        return null;
    }

    /// Remove a document's entry from the reverse index
    pub fn removeDoc(self: *Self, doc_id: DocId) ReverseIndexError!void {
        var key_buf: [8]u8 = undefined;
        std.mem.writeInt(u64, &key_buf, doc_id, .little);

        self.tree.delete(&key_buf) catch |err| {
            if (err != BTreeError.KeyNotFound) {
                return mapBTreeError(err);
            }
        };
    }

    /// Check if a document exists in the reverse index
    pub fn contains(self: *Self, doc_id: DocId) bool {
        var key_buf: [8]u8 = undefined;
        std.mem.writeInt(u64, &key_buf, doc_id, .little);

        const result = self.tree.get(&key_buf) catch {
            return false;
        };

        return result != null;
    }

    /// Deserialize terms from stored value
    /// Caller owns the returned memory
    fn deserializeTerms(self: *Self, value: []const u8) ReverseIndexError![][]const u8 {
        if (value.len < 2) {
            return ReverseIndexError.InvalidData;
        }

        var offset: usize = 0;

        // Read num_terms
        const num_terms = std.mem.readInt(u16, value[offset..][0..2], .little);
        offset += 2;

        // Allocate term slice
        const terms = self.allocator.alloc([]const u8, num_terms) catch {
            return ReverseIndexError.OutOfMemory;
        };
        errdefer self.allocator.free(terms);

        var terms_allocated: usize = 0;
        errdefer {
            for (terms[0..terms_allocated]) |term| {
                self.allocator.free(term);
            }
        }

        // Read each term
        for (0..num_terms) |i| {
            if (offset + 2 > value.len) {
                return ReverseIndexError.InvalidData;
            }

            const term_len = std.mem.readInt(u16, value[offset..][0..2], .little);
            offset += 2;

            if (offset + term_len > value.len) {
                return ReverseIndexError.InvalidData;
            }

            // Allocate and copy term
            const term = self.allocator.alloc(u8, term_len) catch {
                return ReverseIndexError.OutOfMemory;
            };
            @memcpy(term, value[offset..][0..term_len]);
            offset += term_len;

            terms[i] = term;
            terms_allocated += 1;
        }

        return terms;
    }

    /// Free terms returned by getDocTerms
    pub fn freeTerms(self: *Self, terms: [][]const u8) void {
        for (terms) |term| {
            self.allocator.free(term);
        }
        self.allocator.free(terms);
    }
};

/// Map B+Tree errors to ReverseIndex errors
fn mapBTreeError(err: BTreeError) ReverseIndexError {
    return switch (err) {
        BTreeError.KeyNotFound => ReverseIndexError.NotFound,
        BTreeError.OutOfMemory => ReverseIndexError.OutOfMemory,
        else => ReverseIndexError.BTreeError,
    };
}

// ============================================================================
// Tests
// ============================================================================

test "reverse index basic operations" {
    const allocator = std.testing.allocator;

    const vfs = lattice.storage.vfs;
    const buffer_pool = lattice.storage.buffer_pool;
    const page_manager = lattice.storage.page_manager;

    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const db_path = "/tmp/lattice_reverse_index_test.db";
    vfs_impl.delete(db_path) catch {};

    var pm = try page_manager.PageManager.init(allocator, vfs_impl, db_path, .{ .create = true });
    defer {
        pm.deinit();
        vfs_impl.delete(db_path) catch {};
    }

    var bp = try buffer_pool.BufferPool.init(allocator, &pm, 64 * 4096);
    defer bp.deinit();

    var tree = try BTree.init(allocator, &bp);
    var ri = ReverseIndex.init(allocator, &tree);

    // Store terms for a document
    const doc_id: DocId = 42;
    const terms = [_][]const u8{ "hello", "world", "test" };
    try ri.setDocTerms(doc_id, &terms);

    // Verify document exists
    try std.testing.expect(ri.contains(doc_id));
    try std.testing.expect(!ri.contains(999));

    // Retrieve terms
    const retrieved = (try ri.getDocTerms(doc_id)).?;
    defer ri.freeTerms(retrieved);

    try std.testing.expectEqual(@as(usize, 3), retrieved.len);
    try std.testing.expectEqualStrings("hello", retrieved[0]);
    try std.testing.expectEqualStrings("world", retrieved[1]);
    try std.testing.expectEqualStrings("test", retrieved[2]);

    // Remove document
    try ri.removeDoc(doc_id);
    try std.testing.expect(!ri.contains(doc_id));

    // Verify terms are gone
    const gone = try ri.getDocTerms(doc_id);
    try std.testing.expect(gone == null);
}

test "reverse index overwrite" {
    const allocator = std.testing.allocator;

    const vfs = lattice.storage.vfs;
    const buffer_pool = lattice.storage.buffer_pool;
    const page_manager = lattice.storage.page_manager;

    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const db_path = "/tmp/lattice_reverse_index_overwrite_test.db";
    vfs_impl.delete(db_path) catch {};

    var pm = try page_manager.PageManager.init(allocator, vfs_impl, db_path, .{ .create = true });
    defer {
        pm.deinit();
        vfs_impl.delete(db_path) catch {};
    }

    var bp = try buffer_pool.BufferPool.init(allocator, &pm, 64 * 4096);
    defer bp.deinit();

    var tree = try BTree.init(allocator, &bp);
    var ri = ReverseIndex.init(allocator, &tree);

    const doc_id: DocId = 100;

    // Store initial terms
    const terms1 = [_][]const u8{ "alpha", "beta" };
    try ri.setDocTerms(doc_id, &terms1);

    // Overwrite with new terms
    const terms2 = [_][]const u8{ "gamma", "delta", "epsilon" };
    try ri.setDocTerms(doc_id, &terms2);

    // Verify new terms
    const retrieved = (try ri.getDocTerms(doc_id)).?;
    defer ri.freeTerms(retrieved);

    try std.testing.expectEqual(@as(usize, 3), retrieved.len);
    try std.testing.expectEqualStrings("gamma", retrieved[0]);
    try std.testing.expectEqualStrings("delta", retrieved[1]);
    try std.testing.expectEqualStrings("epsilon", retrieved[2]);
}
