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

    /// `init` for a tree that is known to hold a database's nodes.
    ///
    /// `init` starts the counter at 1 when the tree cannot be scanned, which
    /// is right for a tree that does not exist yet and wrong for one that
    /// does: the next node created would take the id of an existing node and
    /// replace it. Here a failed scan is an error.
    pub fn initChecked(allocator: Allocator, tree: *BTree) NodeError!Self {
        var self = Self{
            .allocator = allocator,
            .tree = tree,
            .next_id = 1,
        };
        self.next_id = try self.loadNextId();
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

        const data = self.tree.getBorrowed(&key_buf) catch |err| {
            return mapBTreeError(err);
        };

        if (data) |borrowed| {
            var value = borrowed;
            defer value.release();
            return deserializeNode(self.allocator, node_id, value.bytes) catch {
                return NodeError.InvalidData;
            };
        }

        return NodeError.NotFound;
    }

    /// One property of a node, decoded into `allocator`, or null when the
    /// node has no property `key_id`.
    ///
    /// `get` decodes and copies every label and property of the node; a
    /// query reading `n.name` needs one value, so this walks the stored
    /// record and decodes only that one.
    pub fn getProperty(self: *Self, node_id: NodeId, key_id: SymbolId, allocator: Allocator) NodeError!?PropertyValue {
        var key_buf: [8]u8 = undefined;
        std.mem.writeInt(u64, &key_buf, node_id, .little);

        const serialized = (self.tree.get(&key_buf) catch |err| return mapBTreeError(err)) orelse
            return NodeError.NotFound;
        defer self.tree.freeValue(serialized);
        return decodeProperty(allocator, serialized, key_id);
    }

    /// `getProperty` through a cache of the last record read, so reading
    /// several properties of one node finds it in the B-tree once.
    ///
    /// The cache's buffer comes from `allocator`, which must outlive it
    /// (a query arena). The caller must not use a cache across a write to
    /// the store.
    pub fn getPropertyCached(
        self: *Self,
        cache: *RecordCache,
        node_id: NodeId,
        key_id: SymbolId,
        allocator: Allocator,
    ) NodeError!?PropertyValue {
        if (cache.node_id != node_id) {
            cache.node_id = null;
            var key_buf: [8]u8 = undefined;
            std.mem.writeInt(u64, &key_buf, node_id, .little);
            const serialized = (self.tree.get(&key_buf) catch |err| return mapBTreeError(err)) orelse
                return NodeError.NotFound;
            defer self.tree.freeValue(serialized);
            if (serialized.len > cache.buffer.len) {
                const new_len = @max(serialized.len, 2 * cache.buffer.len, 256);
                allocator.free(cache.buffer);
                cache.buffer = &.{};
                cache.buffer = allocator.alloc(u8, new_len) catch return NodeError.OutOfMemory;
            }
            @memcpy(cache.buffer[0..serialized.len], serialized);
            cache.len = serialized.len;
            cache.node_id = node_id;
        }
        return decodeProperty(allocator, cache.buffer[0..cache.len], key_id);
    }

    /// Update a node's properties
    pub fn update(self: *Self, node_id: NodeId, labels: []const SymbolId, properties: []const Property) NodeError!void {
        var key_buf: [8]u8 = undefined;
        std.mem.writeInt(u64, &key_buf, node_id, .little);

        const serialized = try self.serializeNodeAlloc(labels, properties);
        defer self.allocator.free(serialized);

        // One descent, and all or nothing: a missing node is NotFound, an
        // oversized value is BufferTooSmall, and any failure leaves the old
        // record in place.
        self.tree.replace(&key_buf, serialized) catch |err| {
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

    /// Check whether a fully serialized node record is representable in this
    /// store's backing B+Tree.
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
    var filled: usize = 0;
    errdefer {
        for (properties[0..filled]) |*prop| {
            var val = prop.value;
            val.deinit(allocator);
        }
        allocator.free(properties);
    }

    for (properties) |*prop| {
        prop.key_id = try reader.readInt(u16, .little);
        prop.value = try deserializeValue(allocator, reader);
        filled += 1;
    }

    return Node{
        .id = node_id,
        .labels = labels,
        .properties = properties,
    };
}

/// The stored record of the node read last; see `NodeStore.getPropertyCached`.
pub const RecordCache = struct {
    node_id: ?NodeId = null,
    buffer: []u8 = &.{},
    len: usize = 0,
};

fn decodeProperty(allocator: Allocator, serialized: []const u8, key_id: SymbolId) NodeError!?PropertyValue {
    return findSerializedProperty(allocator, serialized, key_id) catch |err| switch (err) {
        error.OutOfMemory => NodeError.OutOfMemory,
        else => NodeError.InvalidData,
    };
}

/// Decode the value of property `key_id` from a serialized node, skipping
/// the labels and every other property without allocating.
fn findSerializedProperty(allocator: Allocator, data: []const u8, key_id: SymbolId) !?PropertyValue {
    var stream = @import("compat").fixedBufferStream(data);
    const reader = stream.reader();

    const num_labels = try reader.readInt(u16, .little);
    try skipBytes(&stream, @as(usize, num_labels) * 2);

    const num_props = try reader.readInt(u16, .little);
    for (0..num_props) |_| {
        const prop_key = try reader.readInt(u16, .little);
        if (prop_key == key_id) return try deserializeValue(allocator, reader);
        try skipValue(&stream);
    }
    return null;
}

fn skipBytes(stream: anytype, n: usize) error{EndOfStream}!void {
    if (n > stream.buffer.len - stream.pos) return error.EndOfStream;
    stream.pos += n;
}

/// Move past one serialized value (see `serializeValue` for the format).
fn skipValue(stream: anytype) error{ EndOfStream, InvalidData }!void {
    const reader = stream.reader();
    switch (try reader.readByte()) {
        0 => {},
        1 => try skipBytes(stream, 1),
        2, 3 => try skipBytes(stream, 8),
        4, 5 => try skipBytes(stream, try reader.readInt(u32, .little)),
        6 => try skipBytes(stream, @as(usize, try reader.readInt(u32, .little)) * 4),
        7 => {
            const count = try reader.readInt(u32, .little);
            for (0..count) |_| try skipValue(stream);
        },
        8 => {
            const count = try reader.readInt(u32, .little);
            for (0..count) |_| {
                try skipBytes(stream, try reader.readInt(u32, .little));
                try skipValue(stream);
            }
        },
        else => return error.InvalidData,
    }
}

/// Read a u32 length or count whose items take at least `min_item_size`
/// bytes each, refusing one the rest of the record cannot hold. A corrupt
/// length would otherwise allocate up to 4 GiB (16 GiB for a vector)
/// before the short read was noticed.
fn readLength(reader: anytype, min_item_size: usize) error{EndOfStream}!u32 {
    const len = try reader.readInt(u32, .little);
    const remaining = reader.stream.buffer.len - reader.stream.pos;
    if (@as(usize, len) * min_item_size > remaining) return error.EndOfStream;
    return len;
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
            const len = try readLength(reader, 1);
            const str = try allocator.alloc(u8, len);
            errdefer allocator.free(str);
            const bytes_read = try reader.readAll(str);
            if (bytes_read != len) {
                return error.EndOfStream;
            }
            break :blk PropertyValue{ .string_val = str };
        },
        5 => blk: {
            const len = try readLength(reader, 1);
            const bytes = try allocator.alloc(u8, len);
            errdefer allocator.free(bytes);
            const bytes_read = try reader.readAll(bytes);
            if (bytes_read != len) {
                return error.EndOfStream;
            }
            break :blk PropertyValue{ .bytes_val = bytes };
        },
        6 => blk: {
            // Vector
            const len = try readLength(reader, 4);
            const vec = try allocator.alloc(f32, len);
            errdefer allocator.free(vec);
            for (0..len) |i| {
                vec[i] = @bitCast(try reader.readInt(u32, .little));
            }
            break :blk PropertyValue{ .vector_val = vec };
        },
        7 => blk: {
            // List
            // Every value takes at least its one-byte tag.
            const count = try readLength(reader, 1);
            if (count == 0) {
                break :blk PropertyValue{ .list_val = &[_]PropertyValue{} };
            }
            const list = try allocator.alloc(PropertyValue, count);
            var filled: usize = 0;
            errdefer {
                for (list[0..filled]) |*item| {
                    var mutable = item.*;
                    mutable.deinit(allocator);
                }
                allocator.free(list);
            }
            for (0..count) |i| {
                list[i] = try deserializeValue(allocator, reader);
                filled += 1;
            }
            break :blk PropertyValue{ .list_val = list };
        },
        8 => blk: {
            // Map
            // Every entry takes at least a key length and a value tag.
            const count = try readLength(reader, 5);
            if (count == 0) {
                break :blk PropertyValue{ .map_val = &[_]PropertyValue.MapEntry{} };
            }
            const map = try allocator.alloc(PropertyValue.MapEntry, count);
            var filled: usize = 0;
            errdefer {
                for (map[0..filled]) |*entry| {
                    allocator.free(entry.key);
                    var mutable = entry.value;
                    mutable.deinit(allocator);
                }
                allocator.free(map);
            }
            for (0..count) |i| {
                // Read key
                const key_len = try readLength(reader, 1);
                const key = try allocator.alloc(u8, key_len);
                errdefer allocator.free(key);
                const key_bytes_read = try reader.readAll(key);
                if (key_bytes_read != key_len) {
                    return error.EndOfStream;
                }
                // Read value
                const value = try deserializeValue(allocator, reader);
                map[i] = .{ .key = key, .value = value };
                filled += 1;
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
        // Translate storage-layer size rejections into the existing
        // BufferTooSmall signal consumed by Database.
        BTreeError.PageFull, BTreeError.ValueTooLarge => NodeError.BufferTooSmall,
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

test "one property is read from a serialized node past every value kind" {
    const allocator = std.testing.allocator;

    var nested = [_]PropertyValue{ .{ .int_val = 1 }, .{ .string_val = "two" } };
    var entries = [_]PropertyValue.MapEntry{.{ .key = "k", .value = .{ .list_val = &nested } }};
    var vector = [_]f32{ 0.5, -1.5 };
    const labels = [_]SymbolId{ 3, 4, 5 };
    const properties = [_]Property{
        .{ .key_id = 10, .value = .{ .null_val = {} } },
        .{ .key_id = 11, .value = .{ .bool_val = true } },
        .{ .key_id = 12, .value = .{ .int_val = -7 } },
        .{ .key_id = 13, .value = .{ .float_val = 2.5 } },
        .{ .key_id = 14, .value = .{ .string_val = "name" } },
        .{ .key_id = 15, .value = .{ .bytes_val = "\x00\x01" } },
        .{ .key_id = 16, .value = .{ .vector_val = &vector } },
        .{ .key_id = 17, .value = .{ .list_val = &nested } },
        .{ .key_id = 18, .value = .{ .map_val = &entries } },
        .{ .key_id = 19, .value = .{ .string_val = "last" } },
    };
    var buf: [512]u8 = undefined;
    const data = try serializeNode(&labels, &properties, &buf);

    // Every property comes back equal to what a full decode gives.
    var node = try deserializeNode(allocator, 1, data);
    defer node.deinit(allocator);
    for (node.properties) |prop| {
        var found = (try findSerializedProperty(allocator, data, prop.key_id)).?;
        defer found.deinit(allocator);
        var want_buf: [128]u8 = undefined;
        var got_buf: [128]u8 = undefined;
        var want = @import("compat").fixedBufferStream(&want_buf);
        var got = @import("compat").fixedBufferStream(&got_buf);
        try serializeValue(want.writer(), prop.value);
        try serializeValue(got.writer(), found);
        try std.testing.expectEqualSlices(u8, want_buf[0..want.pos], got_buf[0..got.pos]);
    }
    try std.testing.expect((try findSerializedProperty(allocator, data, 99)) == null);

    // A record cut short is an error, not a wrong answer.
    try std.testing.expectError(error.EndOfStream, findSerializedProperty(allocator, data[0 .. data.len - 2], 19));
}

test "a truncated or corrupt node record fails cleanly" {
    const allocator = std.testing.allocator;

    var nested = [_]PropertyValue{ .{ .string_val = "a" }, .{ .string_val = "bc" } };
    var entries = [_]PropertyValue.MapEntry{
        .{ .key = "k1", .value = .{ .string_val = "v1" } },
        .{ .key = "k2", .value = .{ .list_val = &nested } },
    };
    const properties = [_]Property{
        .{ .key_id = 1, .value = .{ .string_val = "first" } },
        .{ .key_id = 2, .value = .{ .list_val = &nested } },
        .{ .key_id = 3, .value = .{ .map_val = &entries } },
        .{ .key_id = 4, .value = .{ .bytes_val = "xyz" } },
    };
    var buf: [256]u8 = undefined;
    const data = try serializeNode(&[_]SymbolId{7}, &properties, &buf);

    // Every proper prefix is an error, and whatever was decoded before the
    // cut is freed exactly once (the testing allocator checks both).
    for (0..data.len) |cut| {
        if (deserializeNode(allocator, 1, data[0..cut])) |node| {
            var owned = node;
            owned.deinit(allocator);
            return error.TestUnexpectedResult;
        } else |_| {}
    }

    // A length the record cannot hold is refused before allocating it.
    var corrupt: [16]u8 = undefined;
    var stream = @import("compat").fixedBufferStream(&corrupt);
    const writer = stream.writer();
    try writer.writeByte(4);
    try writer.writeInt(u32, std.math.maxInt(u32), .little);
    try writer.writeAll("abc");
    var reader_stream = @import("compat").fixedBufferStream(corrupt[0..stream.pos]);
    try std.testing.expectError(error.EndOfStream, deserializeValue(std.testing.failing_allocator, reader_stream.reader()));
}

/// Properties holding every allocated kind of value, nested.
fn allocatedKindsTestProperties(list_items: []PropertyValue, map_entries: []PropertyValue.MapEntry) [4]Property {
    list_items[0] = .{ .string_val = "first" };
    list_items[1] = .{ .bytes_val = "second" };
    map_entries[0] = .{ .key = "inner", .value = .{ .list_val = list_items } };
    map_entries[1] = .{ .key = "text", .value = .{ .string_val = "value" } };
    return .{
        .{ .key_id = 1, .value = .{ .string_val = "a string" } },
        .{ .key_id = 2, .value = .{ .list_val = list_items } },
        .{ .key_id = 3, .value = .{ .map_val = map_entries } },
        .{ .key_id = 4, .value = .{ .string_val = "last" } },
    };
}

test "deserializeNode frees exactly what it decoded when it fails part way" {
    var list_items: [2]PropertyValue = undefined;
    var map_entries: [2]PropertyValue.MapEntry = undefined;
    const properties = allocatedKindsTestProperties(&list_items, &map_entries);
    var buf: [512]u8 = undefined;
    const data = try serializeNode(&[_]SymbolId{ 7, 8 }, &properties, &buf);

    // Truncated anywhere: an error, and nothing leaked or freed twice.
    for (0..data.len) |len| {
        if (deserializeNode(std.testing.allocator, 1, data[0..len])) |node| {
            var n = node;
            n.deinit(std.testing.allocator);
            return error.TestExpectedError;
        } else |_| {}
    }

    // Out of memory at every allocation in turn: the same.
    var fail_index: usize = 0;
    while (true) : (fail_index += 1) {
        var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = fail_index });
        var node = deserializeNode(failing.allocator(), 1, data) catch |err| {
            try std.testing.expectEqual(error.OutOfMemory, err);
            continue;
        };
        node.deinit(failing.allocator());
        break;
    }
}
