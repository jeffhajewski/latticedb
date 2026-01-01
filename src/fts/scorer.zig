//! BM25 Scoring for Full-Text Search.
//!
//! Implements the Okapi BM25 ranking function with configurable parameters.
//! Also provides document length storage for normalization.

const std = @import("std");
const lattice = @import("lattice");

const Allocator = std.mem.Allocator;

const btree = lattice.storage.btree;
const BTree = btree.BTree;
const BTreeError = btree.BTreeError;
const NodeId = lattice.core.types.NodeId;

const posting = @import("posting.zig");
const DocId = posting.DocId;

/// Scorer errors
pub const ScorerError = error{
    /// Document not found
    NotFound,
    /// I/O error
    IoError,
    /// Out of memory
    OutOfMemory,
    /// B+Tree error
    BTreeError,
};

/// BM25 configuration parameters
pub const Bm25Config = struct {
    /// Term frequency saturation parameter (typical: 1.2-2.0)
    k1: f32 = 1.2,
    /// Length normalization parameter (0 = no normalization, 1 = full normalization)
    b: f32 = 0.75,
};

/// Document statistics for scoring
pub const DocStats = struct {
    /// Total number of documents indexed
    total_docs: u64,
    /// Total tokens across all documents
    total_tokens: u64,
    /// Average document length
    avg_doc_length: f32,

    /// Update average after document changes
    pub fn recalculate(self: *DocStats) void {
        if (self.total_docs > 0) {
            self.avg_doc_length = @as(f32, @floatFromInt(self.total_tokens)) /
                @as(f32, @floatFromInt(self.total_docs));
        } else {
            self.avg_doc_length = 0;
        }
    }
};

/// Document length entry (serialized in B+Tree)
const DocLengthEntry = extern struct {
    length: u32,

    comptime {
        std.debug.assert(@sizeOf(DocLengthEntry) == 4);
    }

    pub fn serialize(self: *const DocLengthEntry, buf: *[@sizeOf(DocLengthEntry)]u8) void {
        @memcpy(buf, std.mem.asBytes(self));
    }

    pub fn deserialize(buf: []const u8) DocLengthEntry {
        return std.mem.bytesAsValue(DocLengthEntry, buf[0..@sizeOf(DocLengthEntry)]).*;
    }
};

/// Document length storage (DocId -> length)
pub const DocLengthStore = struct {
    allocator: Allocator,
    tree: *BTree,
    stats: DocStats,

    const Self = @This();

    /// Initialize document length store with a B+Tree
    pub fn init(allocator: Allocator, tree: *BTree) Self {
        return Self{
            .allocator = allocator,
            .tree = tree,
            .stats = DocStats{
                .total_docs = 0,
                .total_tokens = 0,
                .avg_doc_length = 0,
            },
        };
    }

    /// Set document length (updates stats)
    pub fn setLength(self: *Self, doc_id: DocId, length: u32) ScorerError!void {
        var key_buf: [8]u8 = undefined;
        std.mem.writeInt(u64, &key_buf, doc_id, .little);

        // Check if document already exists and get old length
        var old_length: ?u32 = null;
        const existing = self.tree.get(&key_buf) catch |err| {
            return mapBTreeError(err);
        };
        if (existing) |old_value| {
            const old_entry = DocLengthEntry.deserialize(old_value);
            old_length = old_entry.length;
        }

        var value_buf: [@sizeOf(DocLengthEntry)]u8 = undefined;
        const entry = DocLengthEntry{ .length = length };
        entry.serialize(&value_buf);

        // If key exists, delete it first (BTree doesn't support upsert)
        if (old_length != null) {
            self.tree.delete(&key_buf) catch |err| {
                if (err != BTreeError.KeyNotFound) {
                    return mapBTreeError(err);
                }
            };
        }

        self.tree.insert(&key_buf, &value_buf) catch |err| {
            return mapBTreeError(err);
        };

        // Update stats
        if (old_length) |old_len| {
            // Updating existing document
            self.stats.total_tokens -= old_len;
            self.stats.total_tokens += length;
        } else {
            // New document
            self.stats.total_docs += 1;
            self.stats.total_tokens += length;
        }

        self.stats.recalculate();
    }

    /// Get document length
    pub fn getLength(self: *Self, doc_id: DocId) ScorerError!u32 {
        var key_buf: [8]u8 = undefined;
        std.mem.writeInt(u64, &key_buf, doc_id, .little);

        const result = self.tree.get(&key_buf) catch |err| {
            return mapBTreeError(err);
        };

        if (result) |value| {
            const entry = DocLengthEntry.deserialize(value);
            return entry.length;
        }

        return ScorerError.NotFound;
    }

    /// Remove document and update stats
    pub fn removeDoc(self: *Self, doc_id: DocId) ScorerError!void {
        var key_buf: [8]u8 = undefined;
        std.mem.writeInt(u64, &key_buf, doc_id, .little);

        // Get current length before removing
        const result = self.tree.get(&key_buf) catch |err| {
            return mapBTreeError(err);
        };

        if (result) |value| {
            const entry = DocLengthEntry.deserialize(value);

            self.tree.delete(&key_buf) catch |err| {
                return mapBTreeError(err);
            };

            // Update stats
            self.stats.total_docs -= 1;
            self.stats.total_tokens -= entry.length;
            self.stats.recalculate();
        }
    }

    /// Check if a document exists
    pub fn contains(self: *Self, doc_id: DocId) bool {
        var key_buf: [8]u8 = undefined;
        std.mem.writeInt(u64, &key_buf, doc_id, .little);

        const result = self.tree.get(&key_buf) catch return false;
        return result != null;
    }

    /// Get document statistics
    pub fn getStats(self: *Self) DocStats {
        return self.stats;
    }
};

/// BM25 scorer
pub const Bm25Scorer = struct {
    config: Bm25Config,
    doc_lengths: *DocLengthStore,

    const Self = @This();

    /// Initialize BM25 scorer
    pub fn init(config: Bm25Config, doc_lengths: *DocLengthStore) Self {
        return Self{
            .config = config,
            .doc_lengths = doc_lengths,
        };
    }

    /// Calculate BM25 score for a single term in a document
    pub fn scoreTerm(
        self: *Self,
        term_freq: u32,
        doc_freq: u32,
        doc_length: u32,
    ) f32 {
        const stats = self.doc_lengths.stats;

        // Handle edge cases
        if (stats.total_docs == 0 or doc_freq == 0) {
            return 0;
        }

        // IDF component: log((N - df + 0.5) / (df + 0.5) + 1)
        const n = @as(f32, @floatFromInt(stats.total_docs));
        const df = @as(f32, @floatFromInt(doc_freq));
        const idf = @log((n - df + 0.5) / (df + 0.5) + 1.0);

        // TF component with length normalization
        const tf = @as(f32, @floatFromInt(term_freq));
        const dl = @as(f32, @floatFromInt(doc_length));
        const avgdl = if (stats.avg_doc_length > 0) stats.avg_doc_length else 1.0;
        const k1 = self.config.k1;
        const b = self.config.b;

        // BM25 TF formula: (tf * (k1 + 1)) / (tf + k1 * (1 - b + b * (dl / avgdl)))
        const length_norm = 1.0 - b + b * (dl / avgdl);
        const tf_norm = (tf * (k1 + 1.0)) / (tf + k1 * length_norm);

        return idf * tf_norm;
    }

    /// Calculate BM25 score with fuzzy distance penalty
    /// Applies quadratic penalty: score * (1 - (distance/max_distance)^2)
    pub fn scoreTermFuzzy(
        self: *Self,
        term_freq: u32,
        doc_freq: u32,
        doc_length: u32,
        edit_distance: u32,
        max_distance: u32,
    ) f32 {
        const base_score = self.scoreTerm(term_freq, doc_freq, doc_length);

        // Apply distance penalty
        if (max_distance == 0) {
            return if (edit_distance == 0) base_score else 0.0;
        }
        if (edit_distance >= max_distance) {
            return 0.0;
        }

        const ratio = @as(f32, @floatFromInt(edit_distance)) / @as(f32, @floatFromInt(max_distance));
        const penalty = 1.0 - (ratio * ratio);

        return base_score * penalty;
    }

    /// Calculate BM25 score for a document with multiple terms
    pub fn scoreDocument(
        self: *Self,
        doc_id: DocId,
        term_scores: []const TermScore,
    ) ScorerError!f32 {
        const doc_length = try self.doc_lengths.getLength(doc_id);

        var total: f32 = 0.0;
        for (term_scores) |ts| {
            total += self.scoreTerm(ts.term_freq, ts.doc_freq, doc_length);
        }
        return total;
    }
};

/// Term score input for multi-term scoring
pub const TermScore = struct {
    term_freq: u32,
    doc_freq: u32,
};

/// Scored document result
pub const ScoredDoc = struct {
    doc_id: DocId,
    score: f32,

    /// Compare for sorting (descending by score)
    pub fn lessThan(_: void, a: ScoredDoc, b: ScoredDoc) bool {
        return a.score > b.score; // Higher score first
    }
};

/// Map B+Tree errors to Scorer errors
fn mapBTreeError(err: BTreeError) ScorerError {
    return switch (err) {
        BTreeError.KeyNotFound => ScorerError.NotFound,
        BTreeError.OutOfMemory => ScorerError.OutOfMemory,
        else => ScorerError.BTreeError,
    };
}

// ============================================================================
// Tests
// ============================================================================

test "bm25 score calculation" {
    const allocator = std.testing.allocator;

    const vfs = lattice.storage.vfs;
    const buffer_pool = lattice.storage.buffer_pool;
    const page_manager = lattice.storage.page_manager;

    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const db_path = "/tmp/lattice_scorer_test.db";
    vfs_impl.delete(db_path) catch {};

    var pm = try page_manager.PageManager.init(allocator, vfs_impl, db_path, .{ .create = true });
    defer {
        pm.deinit();
        vfs_impl.delete(db_path) catch {};
    }

    var bp = try buffer_pool.BufferPool.init(allocator, &pm, 64 * 4096);
    defer bp.deinit();

    var tree = try BTree.init(allocator, &bp);
    var doc_lengths = DocLengthStore.init(allocator, &tree);

    // Add some documents
    try doc_lengths.setLength(1, 100); // Doc 1: 100 tokens
    try doc_lengths.setLength(2, 200); // Doc 2: 200 tokens
    try doc_lengths.setLength(3, 50); // Doc 3: 50 tokens

    // Verify stats
    try std.testing.expectEqual(@as(u64, 3), doc_lengths.stats.total_docs);
    try std.testing.expectEqual(@as(u64, 350), doc_lengths.stats.total_tokens);

    // Average should be ~116.67
    const avg = doc_lengths.stats.avg_doc_length;
    try std.testing.expect(avg > 116.0 and avg < 117.0);

    // Test scorer
    var scorer = Bm25Scorer.init(.{}, &doc_lengths);

    // Score a term that appears in 1 of 3 docs (high IDF)
    const score1 = scorer.scoreTerm(2, 1, 100);
    try std.testing.expect(score1 > 0);

    // Score a term that appears in all docs (low IDF)
    const score2 = scorer.scoreTerm(2, 3, 100);
    try std.testing.expect(score2 > 0);

    // Rare term should have higher score
    try std.testing.expect(score1 > score2);
}

test "doc length store operations" {
    const allocator = std.testing.allocator;

    const vfs = lattice.storage.vfs;
    const buffer_pool = lattice.storage.buffer_pool;
    const page_manager = lattice.storage.page_manager;

    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const db_path = "/tmp/lattice_doclength_test.db";
    vfs_impl.delete(db_path) catch {};

    var pm = try page_manager.PageManager.init(allocator, vfs_impl, db_path, .{ .create = true });
    defer {
        pm.deinit();
        vfs_impl.delete(db_path) catch {};
    }

    var bp = try buffer_pool.BufferPool.init(allocator, &pm, 64 * 4096);
    defer bp.deinit();

    var tree = try BTree.init(allocator, &bp);
    var store = DocLengthStore.init(allocator, &tree);

    // Initially empty
    try std.testing.expectEqual(@as(u64, 0), store.stats.total_docs);
    try std.testing.expect(!store.contains(1));

    // Add document
    try store.setLength(1, 100);
    try std.testing.expectEqual(@as(u64, 1), store.stats.total_docs);
    try std.testing.expect(store.contains(1));
    try std.testing.expectEqual(@as(u32, 100), try store.getLength(1));

    // Update document length
    try store.setLength(1, 150);
    try std.testing.expectEqual(@as(u64, 1), store.stats.total_docs); // Still 1 doc
    try std.testing.expectEqual(@as(u64, 150), store.stats.total_tokens);
    try std.testing.expectEqual(@as(u32, 150), try store.getLength(1));

    // Remove document
    try store.removeDoc(1);
    try std.testing.expectEqual(@as(u64, 0), store.stats.total_docs);
    try std.testing.expect(!store.contains(1));
}

test "scored doc sorting" {
    var docs = [_]ScoredDoc{
        .{ .doc_id = 1, .score = 2.5 },
        .{ .doc_id = 2, .score = 5.0 },
        .{ .doc_id = 3, .score = 1.0 },
        .{ .doc_id = 4, .score = 3.5 },
    };

    std.mem.sort(ScoredDoc, &docs, {}, ScoredDoc.lessThan);

    // Should be sorted by score descending
    try std.testing.expectEqual(@as(DocId, 2), docs[0].doc_id); // 5.0
    try std.testing.expectEqual(@as(DocId, 4), docs[1].doc_id); // 3.5
    try std.testing.expectEqual(@as(DocId, 1), docs[2].doc_id); // 2.5
    try std.testing.expectEqual(@as(DocId, 3), docs[3].doc_id); // 1.0
}
