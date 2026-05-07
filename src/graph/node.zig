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
    /// Buffer pool is full
    BufferPoolFull,
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
        var self = Self{
            .allocator = allocator,
            .tree = tree,
            .next_id = 1, // Start from 1, 0 is reserved for NULL
        };
        self.next_id = self.loadNextId() catch 1;
        return self;
    }

    fn loadNextId(self: *Self) NodeError!NodeId {
        var max_id: NodeId = 0;
        var iter = self.tree.range(null, null) catch |err| {
            return mapBTreeError(err);
        };
        defer iter.deinit();

        while (iter.next() catch |err| {
            return mapBTreeError(err);
        }) |entry| {
            if (entry.key.len != 8) continue;
            const node_id = std.mem.readInt(u64, entry.key[0..8], .little);
            if (node_id > max_id) max_id = node_id;
        }

        return max_id + 1;
    }

    /// Create a new node with labels and properties
    pub fn create(self: *Self, labels: []const SymbolId, properties: []const Property) NodeError!NodeId {
        const node_id = self.next_id;
        self.next_id += 1;

        const serialized = try self.serializeNodeAlloc(labels, properties);
        defer self.allocator.free(serialized);

        // Create key from node_id
        var key_buf: [8]u8 = undefined;
        std.mem.writeInt(u64, &key_buf, node_id, .little);

        // Insert into B+Tree
        self.tree.insert(&key_buf, serialized) catch |err| {
            return mapBTreeError(err);
        };

        return node_id;
    }

    /// Create a node with a specific ID (for recovery)
    /// Updates next_id if the provided ID is >= current next_id
    pub fn createWithId(
        self: *Self,
        node_id: NodeId,
        labels: []const SymbolId,
        properties: []const Property,
    ) NodeError!void {
        // Update next_id to avoid collisions
        if (node_id >= self.next_id) {
            self.next_id = node_id + 1;
        }

        const serialized = try self.serializeNodeAlloc(labels, properties);
        defer self.allocator.free(serialized);

        // Create key from node_id
        var key_buf: [8]u8 = undefined;
        std.mem.writeInt(u64, &key_buf, node_id, .little);

        // Insert into B+Tree
        self.tree.insert(&key_buf, serialized) catch |err| {
            return mapBTreeError(err);
        };
    }

    /// Heap-allocate and serialize a node's labels + properties.
    /// Replaces the previous fixed 4 KiB stack buffer, which capped the
    /// total serialized node size (and therefore also limited how much
    /// content a single STRING/BYTES property could carry). Caller owns
    /// the returned slice.
    fn serializeNodeAlloc(
        self: *Self,
        labels: []const SymbolId,
        properties: []const Property,
    ) NodeError![]u8 {
        const size = serializedNodeSize(labels, properties);
        const buf = self.allocator.alloc(u8, size) catch return NodeError.OutOfMemory;
        errdefer self.allocator.free(buf);
        const written = serializeNode(labels, properties, buf) catch {
            return NodeError.BufferTooSmall;
        };
        if (written.len != size) return NodeError.InvalidData;
        return buf;
    }

    /// Get a node by ID
    pub fn get(self: *Self, node_id: NodeId) NodeError!Node {
        var key_buf: [8]u8 = undefined;
        std.mem.writeInt(u64, &key_buf, node_id, .little);

        const data = self.tree.get(&key_buf) catch |err| {
            return mapBTreeError(err);
        };

        if (data) |serialized| {
            defer self.tree.freeValue(serialized);
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

        const node_exists = self.tree.contains(&key_buf) catch |err| {
            return mapBTreeError(err);
        };

        if (!node_exists) {
            return NodeError.NotFound;
        }

        // Serialize new data AND bail out up-front if it is too big for
        // a single btree leaf page. Doing the size check before the
        // delete keeps the store consistent if an oversized update is
        // rejected: the existing node is still reachable and callers see
        // a clean `BufferTooSmall` error.
        const serialized = try self.serializeNodeAlloc(labels, properties);
        defer self.allocator.free(serialized);
        if (!self.tree.canFitLeafEntry(&key_buf, serialized.len)) {
            return NodeError.BufferTooSmall;
        }

        self.tree.delete(&key_buf) catch |err| {
            return mapBTreeError(err);
        };
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
    pub fn exists(self: *Self, node_id: NodeId) NodeError!bool {
        var key_buf: [8]u8 = undefined;
        std.mem.writeInt(u64, &key_buf, node_id, .little);

        return self.tree.contains(&key_buf) catch |err| {
            return switch (err) {
                BTreeError.BufferPoolFull => NodeError.BufferPoolFull,
                else => NodeError.BTreeError,
            };
        };
    }

    /// Check whether a fully serialized node record can fit in this store's
    /// backing B+Tree. Nodes are stored as one leaf value, so callers use this
    /// before staging transactional state that would later fail at commit.
    pub fn canFitNode(
        self: *const Self,
        node_id: NodeId,
        labels: []const SymbolId,
        properties: []const Property,
    ) bool {
        var key_buf: [8]u8 = undefined;
        std.mem.writeInt(u64, &key_buf, node_id, .little);
        return self.tree.canFitLeafEntry(&key_buf, serializedNodeSize(labels, properties));
    }
};

// ============================================================================
// Serialization
// ============================================================================

/// Exact byte count that `serializeNode` will emit for the given inputs.
/// Mirrors the wire format below so callers can allocate exact-fit
/// buffers instead of guessing with a fixed stack array.
fn serializedNodeSize(labels: []const SymbolId, properties: []const Property) usize {
    // 2 (labels.len) + labels.len * 2 + 2 (properties.len)
    var size: usize = 2 + labels.len * 2 + 2;
    for (properties) |prop| {
        size += 2; // key_id u16
        size += serializedValueSize(prop.value);
    }
    return size;
}

/// Exact byte count that `serializeValue` will emit for a PropertyValue.
fn serializedValueSize(value: PropertyValue) usize {
    return switch (value) {
        .null_val => 1,
        .bool_val => 2,
        .int_val => 9,
        .float_val => 9,
        .string_val => |s| 5 + s.len,
        .bytes_val => |b| 5 + b.len,
        .vector_val => |v| 5 + v.len * 4,
        .list_val => |list| blk: {
            var s: usize = 5;
            for (list) |item| s += serializedValueSize(item);
            break :blk s;
        },
        .map_val => |map| blk: {
            var s: usize = 5;
            for (map) |entry| s += 4 + entry.key.len + serializedValueSize(entry.value);
            break :blk s;
        },
    };
}

/// Serialize node data to bytes
fn serializeNode(labels: []const SymbolId, properties: []const Property, buf: []u8) ![]u8 {
    var stream = @import("compat").fixedBufferStream(buf);
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

/// Serialize a property value to binary format.
///
/// Wire format (all integers little-endian):
/// | Tag | Type    | Payload                                    |
/// |-----|---------|---------------------------------------------|
/// |  0  | null    | (none)                                      |
/// |  1  | bool    | u8 (0=false, 1=true)                        |
/// |  2  | int     | i64                                         |
/// |  3  | float   | u64 (bitcast f64)                           |
/// |  4  | string  | u32 len, [len]u8                            |
/// |  5  | bytes   | u32 len, [len]u8                            |
/// |  6  | vector  | u32 len, [len]u32 (bitcast f32)             |
/// |  7  | list    | u32 count, [count]value (recursive)         |
/// |  8  | map     | u32 count, [count](u32 key_len, key, value) |
///
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
        .vector_val => |v| {
            try writer.writeByte(6);
            try writer.writeInt(u32, @intCast(v.len), .little);
            for (v) |f| {
                try writer.writeInt(u32, @bitCast(f), .little);
            }
        },
        .list_val => |list| {
            try writer.writeByte(7);
            try writer.writeInt(u32, @intCast(list.len), .little);
            for (list) |item| {
                try serializeValue(writer, item);
            }
        },
        .map_val => |map| {
            try writer.writeByte(8);
            try writer.writeInt(u32, @intCast(map.len), .little);
            for (map) |entry| {
                // Write key (length-prefixed string)
                try writer.writeInt(u32, @intCast(entry.key.len), .little);
                try writer.writeAll(entry.key);
                // Write value (recursive)
                try serializeValue(writer, entry.value);
            }
        },
    }
}

/// Deserialize node data from bytes
fn deserializeNode(allocator: Allocator, node_id: NodeId, data: []const u8) !Node {
    var stream = @import("compat").fixedBufferStream(data);
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
        6 => blk: {
            // Vector
            const len = try reader.readInt(u32, .little);
            const vec = try allocator.alloc(f32, len);
            errdefer allocator.free(vec);
            for (0..len) |i| {
                vec[i] = @bitCast(try reader.readInt(u32, .little));
            }
            break :blk PropertyValue{ .vector_val = vec };
        },
        7 => blk: {
            // List
            const count = try reader.readInt(u32, .little);
            if (count == 0) {
                break :blk PropertyValue{ .list_val = &[_]PropertyValue{} };
            }
            const list = try allocator.alloc(PropertyValue, count);
            errdefer {
                for (list) |*item| {
                    var mutable = item.*;
                    mutable.deinit(allocator);
                }
                allocator.free(list);
            }
            for (0..count) |i| {
                list[i] = try deserializeValue(allocator, reader);
            }
            break :blk PropertyValue{ .list_val = list };
        },
        8 => blk: {
            // Map
            const count = try reader.readInt(u32, .little);
            if (count == 0) {
                break :blk PropertyValue{ .map_val = &[_]PropertyValue.MapEntry{} };
            }
            const map = try allocator.alloc(PropertyValue.MapEntry, count);
            errdefer {
                for (map) |*entry| {
                    allocator.free(entry.key);
                    var mutable = entry.value;
                    mutable.deinit(allocator);
                }
                allocator.free(map);
            }
            for (0..count) |i| {
                // Read key
                const key_len = try reader.readInt(u32, .little);
                const key = try allocator.alloc(u8, key_len);
                errdefer allocator.free(key);
                const key_bytes_read = try reader.readAll(key);
                if (key_bytes_read != key_len) {
                    allocator.free(key);
                    return error.EndOfStream;
                }
                // Read value
                const value = try deserializeValue(allocator, reader);
                map[i] = .{ .key = key, .value = value };
            }
            break :blk PropertyValue{ .map_val = map };
        },
        else => error.InvalidData,
    };
}

/// Map B+Tree errors to Node errors
fn mapBTreeError(err: BTreeError) NodeError {
    return switch (err) {
        BTreeError.KeyNotFound => NodeError.NotFound,
        BTreeError.OutOfMemory => NodeError.OutOfMemory,
        // PageFull from the leaf layer surfaces when a single node's
        // serialized payload would not fit inside one btree leaf page.
        // Translate it into BufferTooSmall so callers see a clear "value
        // too large for page_size" signal instead of a generic BTreeError.
        BTreeError.PageFull => NodeError.BufferTooSmall,
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
    try std.testing.expect(!(try store.exists(999)));
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

    try std.testing.expect(!(try store.exists(1)));

    const node_id = try store.create(&[_]SymbolId{}, &[_]Property{});
    try std.testing.expectEqual(@as(NodeId, 1), node_id);
    try std.testing.expect(try store.exists(1));
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
    try std.testing.expect(try store.exists(node_id));

    // Delete the node
    try store.delete(node_id);
    try std.testing.expect(!(try store.exists(node_id)));

    // Deleting again should fail with NotFound
    try std.testing.expectError(NodeError.NotFound, store.delete(node_id));
}

test "node store reload continues monotonic ids" {
    const allocator = std.testing.allocator;

    const vfs = lattice.storage.vfs;
    const buffer_pool = lattice.storage.buffer_pool;
    const page_manager = lattice.storage.page_manager;

    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const db_path = "/tmp/lattice_node_reload_test.db";
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
    try std.testing.expectEqual(@as(NodeId, 1), try store.create(&[_]SymbolId{1000}, &[_]Property{}));

    var reopened = NodeStore.init(allocator, &tree);
    try std.testing.expectEqual(@as(NodeId, 2), try reopened.create(&[_]SymbolId{1000}, &[_]Property{}));
}

test "property value list serialization round-trip" {
    const allocator = std.testing.allocator;

    // Create a list with mixed types
    var list_items = [_]PropertyValue{
        PropertyValue{ .int_val = 42 },
        PropertyValue{ .string_val = "hello" },
        PropertyValue{ .bool_val = true },
    };
    const list_val = PropertyValue{ .list_val = &list_items };

    // Serialize
    var buf: [1024]u8 = undefined;
    var stream = @import("compat").fixedBufferStream(&buf);
    try serializeValue(stream.writer(), list_val);

    // Deserialize
    stream.pos = 0;
    var result = try deserializeValue(allocator, stream.reader());
    defer result.deinit(allocator);

    // Verify
    try std.testing.expectEqual(@as(usize, 3), result.list_val.len);
    try std.testing.expectEqual(@as(i64, 42), result.list_val[0].int_val);
    try std.testing.expectEqualStrings("hello", result.list_val[1].string_val);
    try std.testing.expectEqual(true, result.list_val[2].bool_val);
}

test "property value map serialization round-trip" {
    const allocator = std.testing.allocator;

    // Create a map
    var map_entries = [_]PropertyValue.MapEntry{
        .{ .key = "name", .value = PropertyValue{ .string_val = "Alice" } },
        .{ .key = "age", .value = PropertyValue{ .int_val = 30 } },
    };
    const map_val = PropertyValue{ .map_val = &map_entries };

    // Serialize
    var buf: [1024]u8 = undefined;
    var stream = @import("compat").fixedBufferStream(&buf);
    try serializeValue(stream.writer(), map_val);

    // Deserialize
    stream.pos = 0;
    var result = try deserializeValue(allocator, stream.reader());
    defer result.deinit(allocator);

    // Verify
    try std.testing.expectEqual(@as(usize, 2), result.map_val.len);
    try std.testing.expectEqualStrings("name", result.map_val[0].key);
    try std.testing.expectEqualStrings("Alice", result.map_val[0].value.string_val);
    try std.testing.expectEqualStrings("age", result.map_val[1].key);
    try std.testing.expectEqual(@as(i64, 30), result.map_val[1].value.int_val);
}

test "property value nested list/map serialization" {
    const allocator = std.testing.allocator;

    // Create nested structure: { "items": [1, 2, 3] }
    var inner_list = [_]PropertyValue{
        PropertyValue{ .int_val = 1 },
        PropertyValue{ .int_val = 2 },
        PropertyValue{ .int_val = 3 },
    };
    var map_entries = [_]PropertyValue.MapEntry{
        .{ .key = "items", .value = PropertyValue{ .list_val = &inner_list } },
    };
    const nested_val = PropertyValue{ .map_val = &map_entries };

    // Serialize
    var buf: [1024]u8 = undefined;
    var stream = @import("compat").fixedBufferStream(&buf);
    try serializeValue(stream.writer(), nested_val);

    // Deserialize
    stream.pos = 0;
    var result = try deserializeValue(allocator, stream.reader());
    defer result.deinit(allocator);

    // Verify
    try std.testing.expectEqual(@as(usize, 1), result.map_val.len);
    try std.testing.expectEqualStrings("items", result.map_val[0].key);
    const items = result.map_val[0].value.list_val;
    try std.testing.expectEqual(@as(usize, 3), items.len);
    try std.testing.expectEqual(@as(i64, 1), items[0].int_val);
    try std.testing.expectEqual(@as(i64, 2), items[1].int_val);
    try std.testing.expectEqual(@as(i64, 3), items[2].int_val);
}
