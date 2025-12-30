//! Label Index for efficient label-based queries.
//!
//! Stores a secondary index for finding nodes by label.
//!
//! Key format: (label_id: u16, node_id: u64)
//! Value: empty (existence only)
//!
//! This enables efficient MATCH (n:Label) queries.

const std = @import("std");
const btree = @import("../storage/btree.zig");
const types = @import("../core/types.zig");
const symbols = @import("symbols.zig");

const Allocator = std.mem.Allocator;
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

    // TODO: Add range scan for getNodesByLabel(label_id) -> []NodeId
    // This requires B+Tree range iteration which isn't implemented yet
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

    const vfs = @import("../storage/vfs.zig");
    const buffer_pool = @import("../storage/buffer_pool.zig");
    const page_manager = @import("../storage/page_manager.zig");

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

    const vfs = @import("../storage/vfs.zig");
    const buffer_pool = @import("../storage/buffer_pool.zig");
    const page_manager = @import("../storage/page_manager.zig");

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

    const vfs = @import("../storage/vfs.zig");
    const buffer_pool = @import("../storage/buffer_pool.zig");
    const page_manager = @import("../storage/page_manager.zig");

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
