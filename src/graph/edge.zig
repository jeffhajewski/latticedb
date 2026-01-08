//! Edge Storage for Property Graph.
//!
//! Stores edges using a B+Tree with composite keys for efficient traversal.
//!
//! Key format: (source_id: u64, direction: u8, type_id: u16, target_id: u64)
//!   - source_id: The node we're querying from
//!   - direction: 0 = outgoing, 1 = incoming
//!   - type_id: Edge type (interned string)
//!   - target_id: The node on the other end
//!
//! Double-write rule: Each edge is stored twice:
//!   (A, 0, TYPE, B) -> edge_data  (outgoing from A)
//!   (B, 1, TYPE, A) -> edge_data  (incoming to B)
//!
//! This enables efficient traversal in both directions.

const std = @import("std");
const lattice = @import("lattice");

const Allocator = std.mem.Allocator;

const btree = lattice.storage.btree;
const types = lattice.core.types;
const symbols = lattice.graph.symbols;
const node_mod = lattice.graph.node;

const BTree = btree.BTree;
const BTreeError = btree.BTreeError;
const NodeId = types.NodeId;
const PropertyValue = types.PropertyValue;
const SymbolId = symbols.SymbolId;
const Property = node_mod.Property;

/// Edge direction
pub const Direction = enum(u8) {
    outgoing = 0,
    incoming = 1,
};

/// Edge storage errors
pub const EdgeError = error{
    /// Edge not found
    NotFound,
    /// Edge already exists
    AlreadyExists,
    /// Source node does not exist
    SourceNotFound,
    /// Target node does not exist
    TargetNotFound,
    /// Serialization buffer too small
    BufferTooSmall,
    /// Invalid edge data
    InvalidData,
    /// I/O error
    IoError,
    /// Out of memory
    OutOfMemory,
    /// B+Tree error
    BTreeError,
};

/// An edge in the graph
pub const Edge = struct {
    source: NodeId,
    target: NodeId,
    edge_type: SymbolId,
    properties: []Property,

    /// Free all allocated memory
    pub fn deinit(self: *Edge, allocator: Allocator) void {
        for (self.properties) |*prop| {
            var val = prop.value;
            val.deinit(allocator);
        }
        allocator.free(self.properties);
    }
};

/// Edge key for B+Tree lookups
pub const EdgeKey = struct {
    source: NodeId,
    direction: Direction,
    edge_type: SymbolId,
    target: NodeId,

    /// Serialize key to bytes (19 bytes total)
    pub fn toBytes(self: EdgeKey) [19]u8 {
        var buf: [19]u8 = undefined;
        std.mem.writeInt(u64, buf[0..8], self.source, .big); // big-endian for lexicographic order
        buf[8] = @intFromEnum(self.direction);
        std.mem.writeInt(u16, buf[9..11], self.edge_type, .big);
        std.mem.writeInt(u64, buf[11..19], self.target, .big);
        return buf;
    }

    /// Parse key from bytes
    pub fn fromBytes(bytes: []const u8) EdgeKey {
        return EdgeKey{
            .source = std.mem.readInt(u64, bytes[0..8], .big),
            .direction = @enumFromInt(bytes[8]),
            .edge_type = std.mem.readInt(u16, bytes[9..11], .big),
            .target = std.mem.readInt(u64, bytes[11..19], .big),
        };
    }
};

/// Edge storage manager
pub const EdgeStore = struct {
    allocator: Allocator,
    tree: *BTree,

    const Self = @This();

    /// Initialize edge store with a B+Tree
    pub fn init(allocator: Allocator, tree: *BTree) Self {
        return Self{
            .allocator = allocator,
            .tree = tree,
        };
    }

    /// Create a new edge between two nodes
    /// Uses double-write pattern: stores both outgoing and incoming entries
    pub fn create(
        self: *Self,
        source: NodeId,
        target: NodeId,
        edge_type: SymbolId,
        properties: []const Property,
    ) EdgeError!void {
        // Serialize edge data
        var buf: [4096]u8 = undefined;
        const serialized = serializeEdge(properties, &buf) catch {
            return EdgeError.BufferTooSmall;
        };

        // Create outgoing key (source, outgoing, type, target)
        const outgoing_key = EdgeKey{
            .source = source,
            .direction = .outgoing,
            .edge_type = edge_type,
            .target = target,
        };
        const outgoing_bytes = outgoing_key.toBytes();

        // Create incoming key (target, incoming, type, source)
        const incoming_key = EdgeKey{
            .source = target,
            .direction = .incoming,
            .edge_type = edge_type,
            .target = source,
        };
        const incoming_bytes = incoming_key.toBytes();

        // Insert both entries (double-write)
        self.tree.insert(&outgoing_bytes, serialized) catch |err| {
            return mapBTreeError(err);
        };

        self.tree.insert(&incoming_bytes, serialized) catch |err| {
            // TODO: Should rollback the outgoing insert on failure
            return mapBTreeError(err);
        };
    }

    /// Get an edge by source, target, and type
    pub fn get(
        self: *Self,
        source: NodeId,
        target: NodeId,
        edge_type: SymbolId,
    ) EdgeError!Edge {
        const key = EdgeKey{
            .source = source,
            .direction = .outgoing,
            .edge_type = edge_type,
            .target = target,
        };
        const key_bytes = key.toBytes();

        const data = self.tree.get(&key_bytes) catch |err| {
            return mapBTreeError(err);
        };

        if (data) |serialized| {
            return deserializeEdge(self.allocator, source, target, edge_type, serialized) catch {
                return EdgeError.InvalidData;
            };
        }

        return EdgeError.NotFound;
    }

    /// Delete an edge
    /// Uses double-delete pattern: removes both outgoing and incoming entries
    pub fn delete(
        self: *Self,
        source: NodeId,
        target: NodeId,
        edge_type: SymbolId,
    ) EdgeError!void {
        // Delete outgoing key (source, outgoing, type, target)
        const outgoing_key = EdgeKey{
            .source = source,
            .direction = .outgoing,
            .edge_type = edge_type,
            .target = target,
        };
        const outgoing_bytes = outgoing_key.toBytes();

        self.tree.delete(&outgoing_bytes) catch |err| {
            return mapBTreeError(err);
        };

        // Delete incoming key (target, incoming, type, source)
        const incoming_key = EdgeKey{
            .source = target,
            .direction = .incoming,
            .edge_type = edge_type,
            .target = source,
        };
        const incoming_bytes = incoming_key.toBytes();

        self.tree.delete(&incoming_bytes) catch |err| {
            // Outgoing already deleted but incoming failed - inconsistent state
            // In production, this would need proper transaction rollback
            return mapBTreeError(err);
        };
    }

    /// Check if an edge exists
    pub fn exists(
        self: *Self,
        source: NodeId,
        target: NodeId,
        edge_type: SymbolId,
    ) bool {
        const key = EdgeKey{
            .source = source,
            .direction = .outgoing,
            .edge_type = edge_type,
            .target = target,
        };
        const key_bytes = key.toBytes();

        const result = self.tree.get(&key_bytes) catch return false;
        return result != null;
    }

    // ========================================================================
    // Range Queries
    // ========================================================================

    /// Edge iterator for range scans
    pub const EdgeIterator = struct {
        tree_iter: btree.BTree.Iterator,
        allocator: Allocator,
        done: bool,
        // Owned copy of end_key for manual checking (BTree Iterator's end_key slice would dangle)
        end_key_storage: [19]u8,

        /// Get the next edge in the range
        /// Caller owns the returned Edge and must call deinit() on it
        pub fn next(self: *EdgeIterator) EdgeError!?Edge {
            if (self.done) return null;

            const entry = self.tree_iter.next() catch |err| {
                self.done = true;
                return mapBTreeError(err);
            };

            if (entry) |e| {
                // Manual end_key check since we can't use the BTree iterator's end_key
                // (it would be a dangling pointer after the struct is returned)
                if (std.mem.order(u8, e.key, &self.end_key_storage) != .lt) {
                    self.done = true;
                    return null;
                }

                const key = EdgeKey.fromBytes(e.key);
                // Reconstruct source/target based on direction
                const source = if (key.direction == .outgoing) key.source else key.target;
                const target = if (key.direction == .outgoing) key.target else key.source;
                const edge = deserializeEdge(self.allocator, source, target, key.edge_type, e.value) catch {
                    return EdgeError.InvalidData;
                };
                return edge;
            } else {
                self.done = true;
                return null;
            }
        }

        /// Clean up iterator resources
        pub fn deinit(self: *EdgeIterator) void {
            self.tree_iter.deinit();
        }
    };

    /// Get all outgoing edges from a node
    pub fn getOutgoing(self: *Self, node_id: NodeId) EdgeError!EdgeIterator {
        // Range: (node_id, OUTGOING, 0, 0) to (node_id, INCOMING, 0, 0)
        const start_key = EdgeKey{
            .source = node_id,
            .direction = .outgoing,
            .edge_type = 0,
            .target = 0,
        };
        const end_key = EdgeKey{
            .source = node_id,
            .direction = .incoming, // Stop before incoming edges
            .edge_type = 0,
            .target = 0,
        };

        const start_bytes = start_key.toBytes();
        const end_bytes = end_key.toBytes();

        // Pass null as end_key to BTree - we do the check ourselves in EdgeIterator.next()
        const tree_iter = self.tree.range(&start_bytes, null) catch |err| {
            return mapBTreeError(err);
        };

        return EdgeIterator{
            .tree_iter = tree_iter,
            .allocator = self.allocator,
            .done = false,
            .end_key_storage = end_bytes,
        };
    }

    /// Get all incoming edges to a node
    pub fn getIncoming(self: *Self, node_id: NodeId) EdgeError!EdgeIterator {
        // Range: (node_id, INCOMING, 0, 0) to (node_id + 1, OUTGOING, 0, 0)
        const start_key = EdgeKey{
            .source = node_id,
            .direction = .incoming,
            .edge_type = 0,
            .target = 0,
        };
        const end_key = EdgeKey{
            .source = node_id +| 1,
            .direction = .outgoing,
            .edge_type = 0,
            .target = 0,
        };

        const start_bytes = start_key.toBytes();
        const end_bytes = end_key.toBytes();

        // Pass null as end_key to BTree - we do the check ourselves in EdgeIterator.next()
        const tree_iter = self.tree.range(&start_bytes, null) catch |err| {
            return mapBTreeError(err);
        };

        return EdgeIterator{
            .tree_iter = tree_iter,
            .allocator = self.allocator,
            .done = false,
            .end_key_storage = end_bytes,
        };
    }

    /// Get outgoing edges of a specific type
    pub fn getOutgoingByType(self: *Self, node_id: NodeId, edge_type: SymbolId) EdgeError!EdgeIterator {
        // Range: (node_id, OUTGOING, type, 0) to (node_id, OUTGOING, type + 1, 0)
        const start_key = EdgeKey{
            .source = node_id,
            .direction = .outgoing,
            .edge_type = edge_type,
            .target = 0,
        };
        const end_key = EdgeKey{
            .source = node_id,
            .direction = .outgoing,
            .edge_type = edge_type +| 1,
            .target = 0,
        };

        const start_bytes = start_key.toBytes();
        const end_bytes = end_key.toBytes();

        const tree_iter = self.tree.range(&start_bytes, null) catch |err| {
            return mapBTreeError(err);
        };

        return EdgeIterator{
            .tree_iter = tree_iter,
            .allocator = self.allocator,
            .done = false,
            .end_key_storage = end_bytes,
        };
    }

    /// Get incoming edges of a specific type
    pub fn getIncomingByType(self: *Self, node_id: NodeId, edge_type: SymbolId) EdgeError!EdgeIterator {
        // Range: (node_id, INCOMING, type, 0) to (node_id, INCOMING, type + 1, 0)
        const start_key = EdgeKey{
            .source = node_id,
            .direction = .incoming,
            .edge_type = edge_type,
            .target = 0,
        };
        const end_key = EdgeKey{
            .source = node_id,
            .direction = .incoming,
            .edge_type = edge_type +| 1,
            .target = 0,
        };

        const start_bytes = start_key.toBytes();
        const end_bytes = end_key.toBytes();

        const tree_iter = self.tree.range(&start_bytes, null) catch |err| {
            return mapBTreeError(err);
        };

        return EdgeIterator{
            .tree_iter = tree_iter,
            .allocator = self.allocator,
            .done = false,
            .end_key_storage = end_bytes,
        };
    }

    /// Get all edges (both directions) for a node
    pub fn getAllEdges(self: *Self, node_id: NodeId) EdgeError!EdgeIterator {
        // Range: (node_id, 0, 0, 0) to (node_id + 1, 0, 0, 0)
        const start_key = EdgeKey{
            .source = node_id,
            .direction = .outgoing,
            .edge_type = 0,
            .target = 0,
        };
        const end_key = EdgeKey{
            .source = node_id +| 1,
            .direction = .outgoing,
            .edge_type = 0,
            .target = 0,
        };

        const start_bytes = start_key.toBytes();
        const end_bytes = end_key.toBytes();

        const tree_iter = self.tree.range(&start_bytes, null) catch |err| {
            return mapBTreeError(err);
        };

        return EdgeIterator{
            .tree_iter = tree_iter,
            .allocator = self.allocator,
            .done = false,
            .end_key_storage = end_bytes,
        };
    }

    /// Count outgoing edges from a node
    pub fn countOutgoing(self: *Self, node_id: NodeId) EdgeError!u64 {
        var iter = try self.getOutgoing(node_id);
        defer iter.deinit();

        var count: u64 = 0;
        while (true) {
            if (try iter.next()) |edge| {
                var e = edge;
                e.deinit(self.allocator);
                count += 1;
            } else break;
        }
        return count;
    }

    /// Count incoming edges to a node
    pub fn countIncoming(self: *Self, node_id: NodeId) EdgeError!u64 {
        var iter = try self.getIncoming(node_id);
        defer iter.deinit();

        var count: u64 = 0;
        while (true) {
            if (try iter.next()) |edge| {
                var e = edge;
                e.deinit(self.allocator);
                count += 1;
            } else break;
        }
        return count;
    }
};

// ============================================================================
// Serialization
// ============================================================================

/// Serialize edge data (properties only, key info is in the B+Tree key)
fn serializeEdge(properties: []const Property, buf: []u8) ![]u8 {
    var stream = std.io.fixedBufferStream(buf);
    const writer = stream.writer();

    // Write properties
    try writer.writeInt(u16, @intCast(properties.len), .little);
    for (properties) |prop| {
        try writer.writeInt(u16, prop.key_id, .little);
        try serializeValue(writer, prop.value);
    }

    return buf[0..stream.pos];
}

/// Serialize a property value (same as node.zig)
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
                try writer.writeInt(u32, @intCast(entry.key.len), .little);
                try writer.writeAll(entry.key);
                try serializeValue(writer, entry.value);
            }
        },
    }
}

/// Deserialize edge data
fn deserializeEdge(
    allocator: Allocator,
    source: NodeId,
    target: NodeId,
    edge_type: SymbolId,
    data: []const u8,
) !Edge {
    var stream = std.io.fixedBufferStream(data);
    const reader = stream.reader();

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

    return Edge{
        .source = source,
        .target = target,
        .edge_type = edge_type,
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
                const key_len = try reader.readInt(u32, .little);
                const key = try allocator.alloc(u8, key_len);
                errdefer allocator.free(key);
                const key_bytes_read = try reader.readAll(key);
                if (key_bytes_read != key_len) {
                    allocator.free(key);
                    return error.EndOfStream;
                }
                const value = try deserializeValue(allocator, reader);
                map[i] = .{ .key = key, .value = value };
            }
            break :blk PropertyValue{ .map_val = map };
        },
        else => PropertyValue{ .null_val = {} },
    };
}

/// Map B+Tree errors to Edge errors
fn mapBTreeError(err: BTreeError) EdgeError {
    return switch (err) {
        BTreeError.KeyNotFound => EdgeError.NotFound,
        BTreeError.OutOfMemory => EdgeError.OutOfMemory,
        else => EdgeError.BTreeError,
    };
}

// ============================================================================
// Tests
// ============================================================================

test "edge store create and get" {
    const allocator = std.testing.allocator;

    const vfs = lattice.storage.vfs;
    const buffer_pool = lattice.storage.buffer_pool;
    const page_manager = lattice.storage.page_manager;

    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const db_path = "/tmp/lattice_edge_test.db";
    vfs_impl.delete(db_path) catch {};

    var pm = try page_manager.PageManager.init(allocator, vfs_impl, db_path, .{ .create = true });
    defer {
        pm.deinit();
        vfs_impl.delete(db_path) catch {};
    }

    var bp = try buffer_pool.BufferPool.init(allocator, &pm, 64 * 4096);
    defer bp.deinit();

    var tree = try BTree.init(allocator, &bp);

    var store = EdgeStore.init(allocator, &tree);

    // Create an edge: (1)-[:KNOWS {since: 2020}]->(2)
    const edge_type: SymbolId = 1000; // KNOWS
    const properties = [_]Property{
        .{ .key_id = 2000, .value = .{ .int_val = 2020 } }, // since: 2020
    };

    try store.create(1, 2, edge_type, &properties);

    // Get the edge back
    var edge = try store.get(1, 2, edge_type);
    defer edge.deinit(allocator);

    try std.testing.expectEqual(@as(NodeId, 1), edge.source);
    try std.testing.expectEqual(@as(NodeId, 2), edge.target);
    try std.testing.expectEqual(edge_type, edge.edge_type);
    try std.testing.expectEqual(@as(usize, 1), edge.properties.len);
    try std.testing.expectEqual(@as(i64, 2020), edge.properties[0].value.int_val);
}

test "edge store double-write" {
    const allocator = std.testing.allocator;

    const vfs = lattice.storage.vfs;
    const buffer_pool = lattice.storage.buffer_pool;
    const page_manager = lattice.storage.page_manager;

    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const db_path = "/tmp/lattice_edge_doublewrite_test.db";
    vfs_impl.delete(db_path) catch {};

    var pm = try page_manager.PageManager.init(allocator, vfs_impl, db_path, .{ .create = true });
    defer {
        pm.deinit();
        vfs_impl.delete(db_path) catch {};
    }

    var bp = try buffer_pool.BufferPool.init(allocator, &pm, 64 * 4096);
    defer bp.deinit();

    var tree = try BTree.init(allocator, &bp);

    var store = EdgeStore.init(allocator, &tree);

    // Create an edge
    const edge_type: SymbolId = 1000;
    try store.create(1, 2, edge_type, &[_]Property{});

    // Verify outgoing key exists
    const outgoing_key = EdgeKey{
        .source = 1,
        .direction = .outgoing,
        .edge_type = edge_type,
        .target = 2,
    };
    const outgoing_bytes = outgoing_key.toBytes();
    const outgoing_result = tree.get(&outgoing_bytes) catch null;
    try std.testing.expect(outgoing_result != null);

    // Verify incoming key exists
    const incoming_key = EdgeKey{
        .source = 2,
        .direction = .incoming,
        .edge_type = edge_type,
        .target = 1,
    };
    const incoming_bytes = incoming_key.toBytes();
    const incoming_result = tree.get(&incoming_bytes) catch null;
    try std.testing.expect(incoming_result != null);
}

test "edge store getOutgoing iteration" {
    const allocator = std.testing.allocator;

    const vfs = lattice.storage.vfs;
    const buffer_pool = lattice.storage.buffer_pool;
    const page_manager = lattice.storage.page_manager;

    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const db_path = "/tmp/lattice_edge_getoutgoing_test.db";
    vfs_impl.delete(db_path) catch {};

    var pm = try page_manager.PageManager.init(allocator, vfs_impl, db_path, .{ .create = true });
    defer {
        pm.deinit();
        vfs_impl.delete(db_path) catch {};
    }

    var bp = try buffer_pool.BufferPool.init(allocator, &pm, 64 * 4096);
    defer bp.deinit();

    var tree = try BTree.init(allocator, &bp);

    var store = EdgeStore.init(allocator, &tree);

    // Create edges: 1 -> 2 (KNOWS), 1 -> 3 (LIKES)
    const knows_type: SymbolId = 1000;
    const likes_type: SymbolId = 1001;
    try store.create(1, 2, knows_type, &[_]Property{});
    try store.create(1, 3, likes_type, &[_]Property{});

    // Verify edges exist
    try std.testing.expect(store.exists(1, 2, knows_type));
    try std.testing.expect(store.exists(1, 3, likes_type));

    // Iterate outgoing edges from node 1
    var iter = try store.getOutgoing(1);
    defer iter.deinit();

    var count: usize = 0;
    while (try iter.next()) |edge| {
        var e = edge;
        e.deinit(allocator);
        count += 1;
    }

    try std.testing.expectEqual(@as(usize, 2), count);
}

test "edge store exists" {
    const allocator = std.testing.allocator;

    const vfs = lattice.storage.vfs;
    const buffer_pool = lattice.storage.buffer_pool;
    const page_manager = lattice.storage.page_manager;

    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const db_path = "/tmp/lattice_edge_exists_test.db";
    vfs_impl.delete(db_path) catch {};

    var pm = try page_manager.PageManager.init(allocator, vfs_impl, db_path, .{ .create = true });
    defer {
        pm.deinit();
        vfs_impl.delete(db_path) catch {};
    }

    var bp = try buffer_pool.BufferPool.init(allocator, &pm, 64 * 4096);
    defer bp.deinit();

    var tree = try BTree.init(allocator, &bp);

    var store = EdgeStore.init(allocator, &tree);

    const edge_type: SymbolId = 1000;

    try std.testing.expect(!store.exists(1, 2, edge_type));

    try store.create(1, 2, edge_type, &[_]Property{});

    try std.testing.expect(store.exists(1, 2, edge_type));
}

test "edge key serialization" {
    const key = EdgeKey{
        .source = 123,
        .direction = .outgoing,
        .edge_type = 456,
        .target = 789,
    };

    const bytes = key.toBytes();
    const parsed = EdgeKey.fromBytes(&bytes);

    try std.testing.expectEqual(key.source, parsed.source);
    try std.testing.expectEqual(key.direction, parsed.direction);
    try std.testing.expectEqual(key.edge_type, parsed.edge_type);
    try std.testing.expectEqual(key.target, parsed.target);
}

test "edge store delete" {
    const allocator = std.testing.allocator;

    const vfs = lattice.storage.vfs;
    const buffer_pool = lattice.storage.buffer_pool;
    const page_manager = lattice.storage.page_manager;

    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const db_path = "/tmp/lattice_edge_delete_test.db";
    vfs_impl.delete(db_path) catch {};

    var pm = try page_manager.PageManager.init(allocator, vfs_impl, db_path, .{ .create = true });
    defer {
        pm.deinit();
        vfs_impl.delete(db_path) catch {};
    }

    var bp = try buffer_pool.BufferPool.init(allocator, &pm, 64 * 4096);
    defer bp.deinit();

    var tree = try BTree.init(allocator, &bp);

    var store = EdgeStore.init(allocator, &tree);

    const edge_type: SymbolId = 1000;

    // Create an edge
    try store.create(1, 2, edge_type, &[_]Property{});
    try std.testing.expect(store.exists(1, 2, edge_type));

    // Verify both outgoing and incoming keys exist
    const outgoing_key = EdgeKey{ .source = 1, .direction = .outgoing, .edge_type = edge_type, .target = 2 };
    const incoming_key = EdgeKey{ .source = 2, .direction = .incoming, .edge_type = edge_type, .target = 1 };
    try std.testing.expect((try tree.get(&outgoing_key.toBytes())) != null);
    try std.testing.expect((try tree.get(&incoming_key.toBytes())) != null);

    // Delete the edge
    try store.delete(1, 2, edge_type);
    try std.testing.expect(!store.exists(1, 2, edge_type));

    // Verify both keys are removed (double-delete)
    try std.testing.expect((try tree.get(&outgoing_key.toBytes())) == null);
    try std.testing.expect((try tree.get(&incoming_key.toBytes())) == null);

    // Deleting again should fail with NotFound
    try std.testing.expectError(EdgeError.NotFound, store.delete(1, 2, edge_type));
}
