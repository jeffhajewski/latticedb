//! Label Index for efficient label-based queries.
//!
//! Stores a secondary index for finding nodes by label.
//!
//! Key format: (label_id: u16, node_id: u64)
//! Value: empty (existence only)
//!
//! This enables efficient MATCH (n:Label) queries.

const std = @import("std");
const lattice = @import("lattice");

const Allocator = std.mem.Allocator;

const btree = lattice.storage.btree;
const types = lattice.core.types;
const symbols = lattice.graph.symbols;

const BTree = btree.BTree;
const BTreeError = btree.BTreeError;
const NodeId = types.NodeId;
const SymbolId = symbols.SymbolId;

/// Label index errors
pub const LabelIndexError = error{
    /// Entry not found
    NotFound,
    /// I/O error
    IoError,
    /// Out of memory
    OutOfMemory,
    /// B+Tree error
    BTreeError,
};

/// Label index key
pub const LabelKey = struct {
    label_id: SymbolId,
    node_id: NodeId,

    /// Serialize key to bytes (10 bytes total)
    /// Uses big-endian for lexicographic ordering
    pub fn toBytes(self: LabelKey) [10]u8 {
        var buf: [10]u8 = undefined;
        std.mem.writeInt(u16, buf[0..2], self.label_id, .big);
        std.mem.writeInt(u64, buf[2..10], self.node_id, .big);
        return buf;
    }

    /// Parse key from bytes
    pub fn fromBytes(bytes: []const u8) LabelKey {
        return LabelKey{
            .label_id = std.mem.readInt(u16, bytes[0..2], .big),
            .node_id = std.mem.readInt(u64, bytes[2..10], .big),
        };
    }

    /// Create a prefix key for scanning all nodes with a label
    pub fn labelPrefix(label_id: SymbolId) [2]u8 {
        var buf: [2]u8 = undefined;
        std.mem.writeInt(u16, &buf, label_id, .big);
        return buf;
    }
};

/// Label index manager
pub const LabelIndex = struct {
    allocator: Allocator,
    tree: *BTree,

    const Self = @This();

    /// Initialize label index with a B+Tree
    pub fn init(allocator: Allocator, tree: *BTree) Self {
        return Self{
            .allocator = allocator,
            .tree = tree,
        };
    }

    /// Add a label to a node
    pub fn add(self: *Self, label_id: SymbolId, node_id: NodeId) LabelIndexError!void {
        const key = LabelKey{ .label_id = label_id, .node_id = node_id };
        const key_bytes = key.toBytes();

        // Value is empty - we only care about key existence
        self.tree.insert(&key_bytes, &[_]u8{}) catch |err| {
            return mapBTreeError(err);
        };
    }

    /// Remove a label from a node
    pub fn remove(self: *Self, label_id: SymbolId, node_id: NodeId) LabelIndexError!void {
        const key = LabelKey{ .label_id = label_id, .node_id = node_id };
        const key_bytes = key.toBytes();

        self.tree.delete(&key_bytes) catch |err| {
            return mapBTreeError(err);
        };
    }

    /// Check if a node has a label
    pub fn hasLabel(self: *Self, label_id: SymbolId, node_id: NodeId) bool {
        const key = LabelKey{ .label_id = label_id, .node_id = node_id };
        const key_bytes = key.toBytes();

        const result = self.tree.get(&key_bytes) catch return false;
        return result != null;
    }

    /// Add multiple labels to a node
    pub fn addLabels(self: *Self, labels: []const SymbolId, node_id: NodeId) LabelIndexError!void {
        for (labels) |label_id| {
            try self.add(label_id, node_id);
        }
    }

    /// Remove multiple labels from a node
    pub fn removeLabels(self: *Self, labels: []const SymbolId, node_id: NodeId) LabelIndexError!void {
        for (labels) |label_id| {
            self.remove(label_id, node_id) catch {}; // Ignore not found errors
        }
    }

    /// Get all node IDs with a given label
    /// Returns an allocated slice that the caller must free
    pub fn getNodesByLabel(self: *Self, label_id: SymbolId) LabelIndexError![]NodeId {
        // Create start and end keys for the range
        // Start: (label_id, 0)
        // End: (label_id + 1, 0) - exclusive
        const start_key = LabelKey{ .label_id = label_id, .node_id = 0 };
        const end_key = LabelKey{ .label_id = label_id +| 1, .node_id = 0 };

        const start_bytes = start_key.toBytes();
        const end_bytes = end_key.toBytes();

        var iter = self.tree.range(&start_bytes, &end_bytes) catch |err| {
            return mapBTreeError(err);
        };
        defer iter.deinit();

        // Collect results
        var results: std.ArrayList(NodeId) = .empty;
        errdefer results.deinit(self.allocator);

        while (true) {
            const entry = iter.next() catch |err| {
                results.deinit(self.allocator);
                return mapBTreeError(err);
            };

            if (entry) |e| {
                const key = LabelKey.fromBytes(e.key);
                // Verify we're still in the correct label range
                if (key.label_id != label_id) break;
                results.append(self.allocator, key.node_id) catch return LabelIndexError.OutOfMemory;
            } else {
                break;
            }
        }

        return results.toOwnedSlice(self.allocator) catch return LabelIndexError.OutOfMemory;
    }

    /// Iterator for scanning nodes with a label (lazy evaluation)
    pub const NodeIterator = struct {
        tree_iter: btree.BTree.Iterator,
        label_id: SymbolId,
        done: bool,

        /// Get the next node ID with this label
        pub fn next(self: *NodeIterator) LabelIndexError!?NodeId {
            if (self.done) return null;

            const entry = self.tree_iter.next() catch |err| {
                self.done = true;
                return mapBTreeError(err);
            };

            if (entry) |e| {
                const key = LabelKey.fromBytes(e.key);
                // Check if we've moved past this label
                if (key.label_id != self.label_id) {
                    self.done = true;
                    return null;
                }
                return key.node_id;
            } else {
                self.done = true;
                return null;
            }
        }

        /// Clean up iterator resources
        pub fn deinit(self: *NodeIterator) void {
            self.tree_iter.deinit();
        }
    };

    /// Create an iterator for nodes with a given label (lazy evaluation)
    pub fn iterNodesByLabel(self: *Self, label_id: SymbolId) LabelIndexError!NodeIterator {
        const start_key = LabelKey{ .label_id = label_id, .node_id = 0 };
        const start_bytes = start_key.toBytes();

        // No end key - we check label_id in the iterator
        const iter = self.tree.range(&start_bytes, null) catch |err| {
            return mapBTreeError(err);
        };

        return NodeIterator{
            .tree_iter = iter,
            .label_id = label_id,
            .done = false,
        };
    }

    /// Count nodes with a given label
    pub fn countNodesByLabel(self: *Self, label_id: SymbolId) LabelIndexError!u64 {
        var iter = try self.iterNodesByLabel(label_id);
        defer iter.deinit();

        var count: u64 = 0;
        while (try iter.next() != null) {
            count += 1;
        }
        return count;
    }
};

/// Map B+Tree errors to LabelIndex errors
fn mapBTreeError(err: BTreeError) LabelIndexError {
    return switch (err) {
        BTreeError.KeyNotFound => LabelIndexError.NotFound,
        BTreeError.OutOfMemory => LabelIndexError.OutOfMemory,
        else => LabelIndexError.BTreeError,
    };
}

// ============================================================================
// Tests
// ============================================================================

test "label index add and check" {
    const allocator = std.testing.allocator;

    const vfs = lattice.storage.vfs;
    const buffer_pool = lattice.storage.buffer_pool;
    const page_manager = lattice.storage.page_manager;

    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const db_path = "/tmp/lattice_label_index_test.db";
    vfs_impl.delete(db_path) catch {};

    var pm = try page_manager.PageManager.init(allocator, vfs_impl, db_path, .{ .create = true });
    defer {
        pm.deinit();
        vfs_impl.delete(db_path) catch {};
    }

    var bp = try buffer_pool.BufferPool.init(allocator, &pm, 64 * 4096);
    defer bp.deinit();

    var tree = try BTree.init(allocator, &bp);

    var index = LabelIndex.init(allocator, &tree);

    const label_person: SymbolId = 1000;
    const label_employee: SymbolId = 1001;
    const node1: NodeId = 1;
    const node2: NodeId = 2;

    // Initially no labels
    try std.testing.expect(!index.hasLabel(label_person, node1));

    // Add labels
    try index.add(label_person, node1);
    try index.add(label_employee, node1);
    try index.add(label_person, node2);

    // Check labels
    try std.testing.expect(index.hasLabel(label_person, node1));
    try std.testing.expect(index.hasLabel(label_employee, node1));
    try std.testing.expect(index.hasLabel(label_person, node2));
    try std.testing.expect(!index.hasLabel(label_employee, node2));
}

test "label index remove" {
    const allocator = std.testing.allocator;

    const vfs = lattice.storage.vfs;
    const buffer_pool = lattice.storage.buffer_pool;
    const page_manager = lattice.storage.page_manager;

    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const db_path = "/tmp/lattice_label_index_remove_test.db";
    vfs_impl.delete(db_path) catch {};

    var pm = try page_manager.PageManager.init(allocator, vfs_impl, db_path, .{ .create = true });
    defer {
        pm.deinit();
        vfs_impl.delete(db_path) catch {};
    }

    var bp = try buffer_pool.BufferPool.init(allocator, &pm, 64 * 4096);
    defer bp.deinit();

    var tree = try BTree.init(allocator, &bp);

    var index = LabelIndex.init(allocator, &tree);

    const label: SymbolId = 1000;
    const node: NodeId = 1;

    try index.add(label, node);
    try std.testing.expect(index.hasLabel(label, node));

    // Remove the label
    try index.remove(label, node);
    // Label should no longer exist
    try std.testing.expect(!index.hasLabel(label, node));
}

test "label index add multiple" {
    const allocator = std.testing.allocator;

    const vfs = lattice.storage.vfs;
    const buffer_pool = lattice.storage.buffer_pool;
    const page_manager = lattice.storage.page_manager;

    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const db_path = "/tmp/lattice_label_index_multi_test.db";
    vfs_impl.delete(db_path) catch {};

    var pm = try page_manager.PageManager.init(allocator, vfs_impl, db_path, .{ .create = true });
    defer {
        pm.deinit();
        vfs_impl.delete(db_path) catch {};
    }

    var bp = try buffer_pool.BufferPool.init(allocator, &pm, 64 * 4096);
    defer bp.deinit();

    var tree = try BTree.init(allocator, &bp);

    var index = LabelIndex.init(allocator, &tree);

    const labels = [_]SymbolId{ 1000, 1001, 1002 };
    const node: NodeId = 1;

    try index.addLabels(&labels, node);

    try std.testing.expect(index.hasLabel(1000, node));
    try std.testing.expect(index.hasLabel(1001, node));
    try std.testing.expect(index.hasLabel(1002, node));
}

test "label key serialization" {
    const key = LabelKey{
        .label_id = 1234,
        .node_id = 5678,
    };

    const bytes = key.toBytes();
    const parsed = LabelKey.fromBytes(&bytes);

    try std.testing.expectEqual(key.label_id, parsed.label_id);
    try std.testing.expectEqual(key.node_id, parsed.node_id);
}
