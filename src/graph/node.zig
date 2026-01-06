//! Node Storage for Property Graph.
//!
//! Stores nodes using a B+Tree with:
//!   Key: node_id (u64)
//!   Value: NodeData (serialized)
//!
//! NodeData format:
//!   - num_labels: u16
//!   - labels: [label_id: u16] × num_labels
//!   - num_properties: u16
//!   - properties: [PropertyEntry] × num_properties

const std = @import("std");
const lattice = @import("lattice");

const Allocator = std.mem.Allocator;

const btree = lattice.storage.btree;
const types = lattice.core.types;
const symbols = lattice.graph.symbols;

const BTree = btree.BTree;
const BTreeError = btree.BTreeError;
const NodeId = types.NodeId;
const PropertyValue = types.PropertyValue;
const SymbolId = symbols.SymbolId;

/// Node storage errors
pub const NodeError = error{
    /// Node not found
    NotFound,
    /// Node already exists
    AlreadyExists,
    /// Serialization buffer too small
    BufferTooSmall,
    /// Invalid node data
    InvalidData,
    /// I/O error
    IoError,
    /// Out of memory
    OutOfMemory,
    /// B+Tree error
    BTreeError,
};

/// A property entry (key-value pair)
pub const Property = struct {
    key_id: SymbolId,
    value: PropertyValue,
};

/// A node in the graph
pub const Node = struct {
    id: NodeId,
    labels: []SymbolId,
    properties: []Property,

    /// Free all allocated memory
    pub fn deinit(self: *Node, allocator: Allocator) void {
        allocator.free(self.labels);
        for (self.properties) |*prop| {
            var val = prop.value;
            val.deinit(allocator);
        }
        allocator.free(self.properties);
    }
};

/// Node storage manager
pub const NodeStore = struct {
    allocator: Allocator,
    tree: *BTree,
    next_id: NodeId,

    const Self = @This();

    /// Initialize node store with a B+Tree
    pub fn init(allocator: Allocator, tree: *BTree) Self {
        return Self{
            .allocator = allocator,
            .tree = tree,
            .next_id = 1, // Start from 1, 0 is reserved for NULL
        };
    }

    /// Create a new node with labels and properties
    pub fn create(self: *Self, labels: []const SymbolId, properties: []const Property) NodeError!NodeId {
        const node_id = self.next_id;
        self.next_id += 1;

        // Serialize node data
        var buf: [4096]u8 = undefined;
        const serialized = serializeNode(labels, properties, &buf) catch {
            return NodeError.BufferTooSmall;
        };

        // Create key from node_id
        var key_buf: [8]u8 = undefined;
        std.mem.writeInt(u64, &key_buf, node_id, .little);

        // Insert into B+Tree
        self.tree.insert(&key_buf, serialized) catch |err| {
            return mapBTreeError(err);
        };

        return node_id;
    }

    /// Get a node by ID
    pub fn get(self: *Self, node_id: NodeId) NodeError!Node {
        var key_buf: [8]u8 = undefined;
        std.mem.writeInt(u64, &key_buf, node_id, .little);

        const data = self.tree.get(&key_buf) catch |err| {
            return mapBTreeError(err);
        };

        if (data) |serialized| {
            return deserializeNode(self.allocator, node_id, serialized) catch {
                return NodeError.InvalidData;
            };
        }

        return NodeError.NotFound;
    }

    /// Update a node's properties
    pub fn update(self: *Self, node_id: NodeId, labels: []const SymbolId, properties: []const Property) NodeError!void {
        // First check if node exists
        var key_buf: [8]u8 = undefined;
        std.mem.writeInt(u64, &key_buf, node_id, .little);

        const existing = self.tree.get(&key_buf) catch |err| {
            return mapBTreeError(err);
        };

        if (existing == null) {
            return NodeError.NotFound;
        }

        // Delete the old entry
        self.tree.delete(&key_buf) catch |err| {
            return mapBTreeError(err);
        };

        // Serialize new data
        var buf: [4096]u8 = undefined;
        const serialized = serializeNode(labels, properties, &buf) catch {
            return NodeError.BufferTooSmall;
        };

        // Insert the new data
        self.tree.insert(&key_buf, serialized) catch |err| {
            return mapBTreeError(err);
        };
    }

    /// Delete a node
    pub fn delete(self: *Self, node_id: NodeId) NodeError!void {
        var key_buf: [8]u8 = undefined;
        std.mem.writeInt(u64, &key_buf, node_id, .little);

        self.tree.delete(&key_buf) catch |err| {
            return mapBTreeError(err);
        };
    }

    /// Check if a node exists
    pub fn exists(self: *Self, node_id: NodeId) bool {
        var key_buf: [8]u8 = undefined;
        std.mem.writeInt(u64, &key_buf, node_id, .little);

        const result = self.tree.get(&key_buf) catch return false;
        return result != null;
    }
};

// ============================================================================
// Serialization
// ============================================================================

/// Serialize node data to bytes
fn serializeNode(labels: []const SymbolId, properties: []const Property, buf: []u8) ![]u8 {
    var stream = std.io.fixedBufferStream(buf);
    const writer = stream.writer();

    // Write labels
    try writer.writeInt(u16, @intCast(labels.len), .little);
    for (labels) |label| {
        try writer.writeInt(u16, label, .little);
    }

    // Write properties
    try writer.writeInt(u16, @intCast(properties.len), .little);
    for (properties) |prop| {
        try writer.writeInt(u16, prop.key_id, .little);
        try serializeValue(writer, prop.value);
    }

    return buf[0..stream.pos];
}

/// Serialize a property value
fn serializeValue(writer: anytype, value: PropertyValue) !void {
    switch (value) {
        .null_val => try writer.writeByte(0),
        .bool_val => |b| {
            try writer.writeByte(1);
            try writer.writeByte(if (b) 1 else 0);
        },
        .int_val => |i| {
            try writer.writeByte(2);
            try writer.writeInt(i64, i, .little);
        },
        .float_val => |f| {
            try writer.writeByte(3);
            try writer.writeInt(u64, @bitCast(f), .little);
        },
        .string_val => |s| {
            try writer.writeByte(4);
            try writer.writeInt(u32, @intCast(s.len), .little);
            try writer.writeAll(s);
        },
        .bytes_val => |b| {
            try writer.writeByte(5);
            try writer.writeInt(u32, @intCast(b.len), .little);
            try writer.writeAll(b);
        },
        .list_val => {
            try writer.writeByte(6);
            // TODO: Implement list serialization
            try writer.writeInt(u32, 0, .little);
        },
        .map_val => {
            try writer.writeByte(7);
            // TODO: Implement map serialization
            try writer.writeInt(u32, 0, .little);
        },
    }
}

/// Deserialize node data from bytes
fn deserializeNode(allocator: Allocator, node_id: NodeId, data: []const u8) !Node {
    var stream = std.io.fixedBufferStream(data);
    const reader = stream.reader();

    // Read labels
    const num_labels = try reader.readInt(u16, .little);
    const labels = try allocator.alloc(SymbolId, num_labels);
    errdefer allocator.free(labels);

    for (labels) |*label| {
        label.* = try reader.readInt(u16, .little);
    }

    // Read properties
    const num_props = try reader.readInt(u16, .little);
    const properties = try allocator.alloc(Property, num_props);
    errdefer {
        for (properties) |*prop| {
            var val = prop.value;
            val.deinit(allocator);
        }
        allocator.free(properties);
    }

    for (properties) |*prop| {
        prop.key_id = try reader.readInt(u16, .little);
        prop.value = try deserializeValue(allocator, reader);
    }

    return Node{
        .id = node_id,
        .labels = labels,
        .properties = properties,
    };
}

/// Deserialize a property value
fn deserializeValue(allocator: Allocator, reader: anytype) !PropertyValue {
    const type_tag = try reader.readByte();

    return switch (type_tag) {
        0 => PropertyValue{ .null_val = {} },
        1 => PropertyValue{ .bool_val = (try reader.readByte()) != 0 },
        2 => PropertyValue{ .int_val = try reader.readInt(i64, .little) },
        3 => PropertyValue{ .float_val = @bitCast(try reader.readInt(u64, .little)) },
        4 => blk: {
            const len = try reader.readInt(u32, .little);
            const str = try allocator.alloc(u8, len);
            errdefer allocator.free(str);
            const bytes_read = try reader.readAll(str);
            if (bytes_read != len) {
                allocator.free(str);
                return error.EndOfStream;
            }
            break :blk PropertyValue{ .string_val = str };
        },
        5 => blk: {
            const len = try reader.readInt(u32, .little);
            const bytes = try allocator.alloc(u8, len);
            errdefer allocator.free(bytes);
            const bytes_read = try reader.readAll(bytes);
            if (bytes_read != len) {
                allocator.free(bytes);
                return error.EndOfStream;
            }
            break :blk PropertyValue{ .bytes_val = bytes };
        },
        6 => {
            // List - read count and skip for now
            _ = try reader.readInt(u32, .little);
            return PropertyValue{ .list_val = &[_]PropertyValue{} };
        },
        7 => {
            // Map - read count and skip for now
            _ = try reader.readInt(u32, .little);
            return PropertyValue{ .map_val = &[_]PropertyValue.MapEntry{} };
        },
        else => error.InvalidData,
    };
}

/// Map B+Tree errors to Node errors
fn mapBTreeError(err: BTreeError) NodeError {
    return switch (err) {
        BTreeError.KeyNotFound => NodeError.NotFound,
        BTreeError.OutOfMemory => NodeError.OutOfMemory,
        else => NodeError.BTreeError,
    };
}

// ============================================================================
// Tests
// ============================================================================

test "node store create and get" {
    const allocator = std.testing.allocator;

    const vfs = lattice.storage.vfs;
    const buffer_pool = lattice.storage.buffer_pool;
    const page_manager = lattice.storage.page_manager;

    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const db_path = "/tmp/lattice_node_test.db";
    vfs_impl.delete(db_path) catch {};

    var pm = try page_manager.PageManager.init(allocator, vfs_impl, db_path, .{ .create = true });
    defer {
        pm.deinit();
        vfs_impl.delete(db_path) catch {};
    }

    var bp = try buffer_pool.BufferPool.init(allocator, &pm, 64 * 4096);
    defer bp.deinit();

    var tree = try BTree.init(allocator, &bp);

    var store = NodeStore.init(allocator, &tree);

    // Create a node with labels and properties
    const labels = [_]SymbolId{ 1000, 1001 }; // "Person", "Employee"
    const properties = [_]Property{
        .{ .key_id = 2000, .value = .{ .string_val = "Alice" } },
        .{ .key_id = 2001, .value = .{ .int_val = 30 } },
    };

    const node_id = try store.create(&labels, &properties);
    try std.testing.expectEqual(@as(NodeId, 1), node_id);

    // Get the node back
    var node = try store.get(node_id);
    defer node.deinit(allocator);

    try std.testing.expectEqual(@as(NodeId, 1), node.id);
    try std.testing.expectEqual(@as(usize, 2), node.labels.len);
    try std.testing.expectEqual(@as(SymbolId, 1000), node.labels[0]);
    try std.testing.expectEqual(@as(SymbolId, 1001), node.labels[1]);
    try std.testing.expectEqual(@as(usize, 2), node.properties.len);
}

test "node store not found" {
    const allocator = std.testing.allocator;

    const vfs = lattice.storage.vfs;
    const buffer_pool = lattice.storage.buffer_pool;
    const page_manager = lattice.storage.page_manager;

    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const db_path = "/tmp/lattice_node_notfound_test.db";
    vfs_impl.delete(db_path) catch {};

    var pm = try page_manager.PageManager.init(allocator, vfs_impl, db_path, .{ .create = true });
    defer {
        pm.deinit();
        vfs_impl.delete(db_path) catch {};
    }

    var bp = try buffer_pool.BufferPool.init(allocator, &pm, 64 * 4096);
    defer bp.deinit();

    var tree = try BTree.init(allocator, &bp);

    var store = NodeStore.init(allocator, &tree);

    try std.testing.expectError(NodeError.NotFound, store.get(999));
    try std.testing.expect(!store.exists(999));
}

test "node store exists" {
    const allocator = std.testing.allocator;

    const vfs = lattice.storage.vfs;
    const buffer_pool = lattice.storage.buffer_pool;
    const page_manager = lattice.storage.page_manager;

    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const db_path = "/tmp/lattice_node_exists_test.db";
    vfs_impl.delete(db_path) catch {};

    var pm = try page_manager.PageManager.init(allocator, vfs_impl, db_path, .{ .create = true });
    defer {
        pm.deinit();
        vfs_impl.delete(db_path) catch {};
    }

    var bp = try buffer_pool.BufferPool.init(allocator, &pm, 64 * 4096);
    defer bp.deinit();

    var tree = try BTree.init(allocator, &bp);

    var store = NodeStore.init(allocator, &tree);

    try std.testing.expect(!store.exists(1));

    const node_id = try store.create(&[_]SymbolId{}, &[_]Property{});
    try std.testing.expectEqual(@as(NodeId, 1), node_id);
    try std.testing.expect(store.exists(1));
}

test "node store delete" {
    const allocator = std.testing.allocator;

    const vfs = lattice.storage.vfs;
    const buffer_pool = lattice.storage.buffer_pool;
    const page_manager = lattice.storage.page_manager;

    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const db_path = "/tmp/lattice_node_delete_test.db";
    vfs_impl.delete(db_path) catch {};

    var pm = try page_manager.PageManager.init(allocator, vfs_impl, db_path, .{ .create = true });
    defer {
        pm.deinit();
        vfs_impl.delete(db_path) catch {};
    }

    var bp = try buffer_pool.BufferPool.init(allocator, &pm, 64 * 4096);
    defer bp.deinit();

    var tree = try BTree.init(allocator, &bp);

    var store = NodeStore.init(allocator, &tree);

    // Create a node
    const node_id = try store.create(&[_]SymbolId{1000}, &[_]Property{});
    try std.testing.expect(store.exists(node_id));

    // Delete the node
    try store.delete(node_id);
    try std.testing.expect(!store.exists(node_id));

    // Deleting again should fail with NotFound
    try std.testing.expectError(NodeError.NotFound, store.delete(node_id));
}
