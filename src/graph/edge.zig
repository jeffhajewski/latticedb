//! Edge Storage for Property Graph.
//!
//! Stores edges using a B+Tree with composite keys for efficient traversal.
//!
//! Traversal key format: (source_id: u64, direction: u8, type_id: u16, target_id: u64, edge_id: u64)
//!   - source_id: The node we're querying from
//!   - direction: 0 = outgoing, 1 = incoming
//!   - type_id: Edge type (interned string)
//!   - target_id: The node on the other end
//!   - edge_id: Stable, monotonic edge identity (supports parallel edges)
//!
//! Double-write rule: Each edge is stored twice in the traversal tree:
//!   (A, 0, TYPE, B, EID) -> edge_data (outgoing from A)
//!   (B, 1, TYPE, A, EID) -> empty     (incoming to B)
//!
//! Edge ID index:
//!   edge_id -> (source, target, type_id)
//!
//! This layout preserves efficient traversal while storing edge payload once.

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
const EdgeId = types.EdgeId;
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
    id: EdgeId,
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

/// Lightweight edge reference containing only traversal-relevant fields.
/// No allocations - all data extracted from the traversal key.
pub const EdgeRef = struct {
    id: EdgeId,
    source: NodeId,
    target: NodeId,
    edge_type: SymbolId,
};

/// Result for a single node's edges in batch operation
pub const BatchEdgeResult = struct {
    node_id: NodeId,
    edges_start: usize, // Index into shared edge buffer
    edges_len: usize,
};

/// Full batch result with ownership. Contains edges for multiple nodes
/// collected in a single B+Tree scan, exploiting key ordering.
pub const BatchEdgeResults = struct {
    results: []BatchEdgeResult, // One per input node (in sorted order)
    edges: []EdgeRef, // All edges, contiguous
    allocator: Allocator,

    pub fn deinit(self: *BatchEdgeResults) void {
        self.allocator.free(self.results);
        self.allocator.free(self.edges);
    }

    /// Get edges for a specific result index
    pub fn getEdges(self: *const BatchEdgeResults, idx: usize) []const EdgeRef {
        const r = self.results[idx];
        return self.edges[r.edges_start..][0..r.edges_len];
    }
};

/// Edge key for B+Tree lookups
pub const EdgeKey = struct {
    source: NodeId,
    direction: Direction,
    edge_type: SymbolId,
    target: NodeId,
    edge_id: EdgeId,

    /// Serialize key to bytes (27 bytes total)
    pub fn toBytes(self: EdgeKey) [27]u8 {
        var buf: [27]u8 = undefined;
        std.mem.writeInt(u64, buf[0..8], self.source, .big); // big-endian for lexicographic order
        buf[8] = @intFromEnum(self.direction);
        std.mem.writeInt(u16, buf[9..11], self.edge_type, .big);
        std.mem.writeInt(u64, buf[11..19], self.target, .big);
        std.mem.writeInt(u64, buf[19..27], self.edge_id, .big);
        return buf;
    }

    /// Parse key from bytes
    pub fn fromBytes(bytes: []const u8) EdgeKey {
        return EdgeKey{
            .source = std.mem.readInt(u64, bytes[0..8], .big),
            .direction = @enumFromInt(bytes[8]),
            .edge_type = std.mem.readInt(u16, bytes[9..11], .big),
            .target = std.mem.readInt(u64, bytes[11..19], .big),
            .edge_id = std.mem.readInt(u64, bytes[19..27], .big),
        };
    }
};

/// Edge storage manager
pub const EdgeStore = struct {
    allocator: Allocator,
    tree: *BTree,
    edge_id_index: *BTree,
    next_edge_id: EdgeId,

    const Self = @This();
    const EDGE_ID_META_KEY: EdgeId = 0;

    /// Initialize edge store with traversal and edge_id index B+Trees.
    pub fn init(allocator: Allocator, tree: *BTree, edge_id_index: *BTree) Self {
        var self = Self{
            .allocator = allocator,
            .tree = tree,
            .edge_id_index = edge_id_index,
            .next_edge_id = 1,
        };
        self.next_edge_id = self.loadNextEdgeId() catch 1;
        return self;
    }

    fn edgeIdToKey(edge_id: EdgeId) [8]u8 {
        var key: [8]u8 = undefined;
        std.mem.writeInt(u64, &key, edge_id, .little);
        return key;
    }

    fn loadNextEdgeId(self: *Self) EdgeError!EdgeId {
        const meta_key = edgeIdToKey(EDGE_ID_META_KEY);
        const meta = self.edge_id_index.get(&meta_key) catch |err| {
            return mapBTreeError(err);
        };
        if (meta) |data| {
            defer self.edge_id_index.freeValue(data);
            if (data.len == 8) {
                const next = std.mem.readInt(u64, data[0..8], .little);
                return if (next == 0) 1 else next;
            }
        }

        // Fallback when metadata is missing: derive from max indexed edge_id.
        var max_id: EdgeId = 0;
        var iter = self.edge_id_index.range(null, null) catch |err| {
            return mapBTreeError(err);
        };
        defer iter.deinit();

        while (iter.next() catch null) |entry| {
            if (entry.key.len != 8) continue;
            const edge_id = std.mem.readInt(u64, entry.key[0..8], .little);
            if (edge_id == EDGE_ID_META_KEY) continue;
            if (edge_id > max_id) max_id = edge_id;
        }

        const next = max_id + 1;
        try self.persistNextEdgeId(next);
        return next;
    }

    fn persistNextEdgeId(self: *Self, next_edge_id: EdgeId) EdgeError!void {
        const meta_key = edgeIdToKey(EDGE_ID_META_KEY);
        var meta_value: [8]u8 = undefined;
        std.mem.writeInt(u64, &meta_value, next_edge_id, .little);

        self.edge_id_index.delete(&meta_key) catch |err| switch (err) {
            BTreeError.KeyNotFound => {},
            else => return mapBTreeError(err),
        };
        self.edge_id_index.insert(&meta_key, &meta_value) catch |err| {
            return mapBTreeError(err);
        };
    }

    fn findMatchingEdgeId(
        self: *Self,
        source: NodeId,
        target: NodeId,
        edge_type: SymbolId,
    ) EdgeError!?EdgeId {
        const start_key = EdgeKey{
            .source = source,
            .direction = .outgoing,
            .edge_type = edge_type,
            .target = target,
            .edge_id = 0,
        };
        const start_bytes = start_key.toBytes();

        var iter = self.tree.range(&start_bytes, null) catch |err| {
            return mapBTreeError(err);
        };
        defer iter.deinit();

        while (iter.next() catch null) |entry| {
            const key = EdgeKey.fromBytes(entry.key);
            if (key.source == source and key.direction == .outgoing and key.edge_type == edge_type and key.target == target) {
                return key.edge_id;
            }
            break;
        }

        return null;
    }

    /// Create a new edge between two nodes
    /// Uses double-write pattern: outgoing stores payload, incoming stores empty payload.
    pub fn createAndGetId(
        self: *Self,
        source: NodeId,
        target: NodeId,
        edge_type: SymbolId,
        properties: []const Property,
    ) EdgeError!EdgeId {
        const edge_id = self.next_edge_id;
        const next_edge_id = edge_id + 1;

        // Serialize edge record (stored once in edge_id_index tree).
        var buf: [4096]u8 = undefined;
        const serialized = serializeEdge(source, target, edge_type, properties, &buf) catch {
            return EdgeError.BufferTooSmall;
        };

        // Create outgoing key (source, outgoing, type, target)
        const outgoing_key = EdgeKey{
            .source = source,
            .direction = .outgoing,
            .edge_type = edge_type,
            .target = target,
            .edge_id = edge_id,
        };
        const outgoing_bytes = outgoing_key.toBytes();

        // Create incoming key (target, incoming, type, source)
        const incoming_key = EdgeKey{
            .source = target,
            .direction = .incoming,
            .edge_type = edge_type,
            .target = source,
            .edge_id = edge_id,
        };
        const incoming_bytes = incoming_key.toBytes();

        const id_key = edgeIdToKey(edge_id);

        // Insert all entries with rollback on failure
        self.tree.insert(&outgoing_bytes, &[_]u8{}) catch |err| {
            return mapBTreeError(err);
        };

        self.tree.insert(&incoming_bytes, &[_]u8{}) catch |err| {
            self.tree.delete(&outgoing_bytes) catch {};
            return mapBTreeError(err);
        };

        self.edge_id_index.insert(&id_key, serialized) catch |err| {
            self.tree.delete(&incoming_bytes) catch {};
            self.tree.delete(&outgoing_bytes) catch {};
            return mapBTreeError(err);
        };

        self.persistNextEdgeId(next_edge_id) catch |err| {
            self.edge_id_index.delete(&id_key) catch {};
            self.tree.delete(&incoming_bytes) catch {};
            self.tree.delete(&outgoing_bytes) catch {};
            return err;
        };

        self.next_edge_id = next_edge_id;
        return edge_id;
    }

    pub fn create(
        self: *Self,
        source: NodeId,
        target: NodeId,
        edge_type: SymbolId,
        properties: []const Property,
    ) EdgeError!void {
        _ = try self.createAndGetId(source, target, edge_type, properties);
    }

    /// Create an edge with an explicit edge_id (used by recovery/undo).
    pub fn createWithId(
        self: *Self,
        edge_id: EdgeId,
        source: NodeId,
        target: NodeId,
        edge_type: SymbolId,
        properties: []const Property,
    ) EdgeError!void {
        if (edge_id == EDGE_ID_META_KEY) return EdgeError.InvalidData;

        // Reject duplicate edge IDs
        const existing = self.getById(edge_id);
        if (existing) |edge| {
            var e = edge;
            e.deinit(self.allocator);
            return EdgeError.AlreadyExists;
        } else |err| switch (err) {
            EdgeError.NotFound => {},
            else => return err,
        }

        var buf: [4096]u8 = undefined;
        const serialized = serializeEdge(source, target, edge_type, properties, &buf) catch {
            return EdgeError.BufferTooSmall;
        };

        const outgoing_key = EdgeKey{
            .source = source,
            .direction = .outgoing,
            .edge_type = edge_type,
            .target = target,
            .edge_id = edge_id,
        };
        const outgoing_bytes = outgoing_key.toBytes();

        const incoming_key = EdgeKey{
            .source = target,
            .direction = .incoming,
            .edge_type = edge_type,
            .target = source,
            .edge_id = edge_id,
        };
        const incoming_bytes = incoming_key.toBytes();

        const id_key = edgeIdToKey(edge_id);

        self.tree.insert(&outgoing_bytes, &[_]u8{}) catch |err| {
            return mapBTreeError(err);
        };
        self.tree.insert(&incoming_bytes, &[_]u8{}) catch |err| {
            self.tree.delete(&outgoing_bytes) catch {};
            return mapBTreeError(err);
        };
        self.edge_id_index.insert(&id_key, serialized) catch |err| {
            self.tree.delete(&incoming_bytes) catch {};
            self.tree.delete(&outgoing_bytes) catch {};
            return mapBTreeError(err);
        };

        if (edge_id >= self.next_edge_id) {
            const new_next = edge_id + 1;
            try self.persistNextEdgeId(new_next);
            self.next_edge_id = new_next;
        }
    }

    /// Get an edge by source, target, and type
    pub fn get(
        self: *Self,
        source: NodeId,
        target: NodeId,
        edge_type: SymbolId,
    ) EdgeError!Edge {
        const edge_id = (try self.findMatchingEdgeId(source, target, edge_type)) orelse {
            return EdgeError.NotFound;
        };
        return self.getById(edge_id);
    }

    /// Get an edge by stable edge_id.
    pub fn getById(self: *Self, edge_id: EdgeId) EdgeError!Edge {
        const id_key = edgeIdToKey(edge_id);
        const edge_data = self.edge_id_index.get(&id_key) catch |err| {
            return mapBTreeError(err);
        };
        if (edge_data == null or edge_data.?.len == 0) {
            return EdgeError.NotFound;
        }
        defer self.edge_id_index.freeValue(edge_data.?);
        return deserializeEdge(self.allocator, edge_id, edge_data.?) catch {
            return EdgeError.InvalidData;
        };
    }

    /// Delete an edge
    /// Uses double-delete pattern: removes both outgoing and incoming entries
    /// Atomic: if incoming delete fails, outgoing is restored
    pub fn delete(
        self: *Self,
        source: NodeId,
        target: NodeId,
        edge_type: SymbolId,
    ) EdgeError!void {
        const edge_id = (try self.findMatchingEdgeId(source, target, edge_type)) orelse {
            return EdgeError.NotFound;
        };
        return self.deleteById(edge_id);
    }

    /// Delete an edge by stable edge_id.
    pub fn deleteById(self: *Self, edge_id: EdgeId) EdgeError!void {
        const id_key = edgeIdToKey(edge_id);
        const edge_data = self.edge_id_index.get(&id_key) catch |err| {
            return mapBTreeError(err);
        };
        if (edge_data == null or edge_data.?.len == 0) {
            return EdgeError.NotFound;
        }
        defer self.edge_id_index.freeValue(edge_data.?);
        var edge = deserializeEdge(self.allocator, edge_id, edge_data.?) catch {
            return EdgeError.InvalidData;
        };
        defer edge.deinit(self.allocator);

        const outgoing_key = EdgeKey{
            .source = edge.source,
            .direction = .outgoing,
            .edge_type = edge.edge_type,
            .target = edge.target,
            .edge_id = edge_id,
        };
        const outgoing_bytes = outgoing_key.toBytes();

        // Delete outgoing entry
        self.tree.delete(&outgoing_bytes) catch |err| {
            return mapBTreeError(err);
        };

        // Build incoming key (target, incoming, type, source, edge_id)
        const incoming_key = EdgeKey{
            .source = edge.target,
            .direction = .incoming,
            .edge_type = edge.edge_type,
            .target = edge.source,
            .edge_id = edge_id,
        };
        const incoming_bytes = incoming_key.toBytes();

        // Delete incoming entry with rollback on failure
        self.tree.delete(&incoming_bytes) catch |err| {
            self.tree.insert(&outgoing_bytes, &[_]u8{}) catch {};
            return mapBTreeError(err);
        };

        // Delete edge data entry with rollback on failure
        self.edge_id_index.delete(&id_key) catch |err| {
            self.tree.insert(&outgoing_bytes, &[_]u8{}) catch {};
            self.tree.insert(&incoming_bytes, &[_]u8{}) catch {};
            self.edge_id_index.insert(&id_key, edge_data.?) catch {};
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
        const edge_id = self.findMatchingEdgeId(source, target, edge_type) catch return false;
        return edge_id != null;
    }

    // ========================================================================
    // Range Queries
    // ========================================================================

    /// Edge iterator for range scans
    pub const EdgeIterator = struct {
        tree_iter: btree.BTree.Iterator,
        allocator: Allocator,
        store: *Self,
        done: bool,
        // Owned copy of end_key for manual checking (BTree Iterator's end_key slice would dangle)
        end_key_storage: [27]u8,

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
                const edge = try self.store.getById(key.edge_id);
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

    /// Lightweight iterator that returns EdgeRef without deserializing properties.
    /// Zero allocations per iteration - ideal for graph traversal (BFS/DFS).
    pub const EdgeRefIterator = struct {
        tree_iter: btree.BTree.Iterator,
        done: bool,
        /// Owned copy of end_key for manual checking (BTree Iterator's end_key slice would dangle)
        end_key_storage: [27]u8,

        /// Get the next edge reference in the range.
        /// Returns EdgeRef with source, target, and edge_type_id.
        /// No allocations - caller does not need to free anything.
        pub fn next(self: *EdgeRefIterator) EdgeError!?EdgeRef {
            if (self.done) return null;

            const entry = self.tree_iter.next() catch |err| {
                self.done = true;
                return mapBTreeError(err);
            };

            if (entry) |e| {
                // Manual end_key check since we can't use the BTree iterator's end_key
                if (std.mem.order(u8, e.key, &self.end_key_storage) != .lt) {
                    self.done = true;
                    return null;
                }

                const key = EdgeKey.fromBytes(e.key);
                // Reconstruct source/target based on direction
                const source = if (key.direction == .outgoing) key.source else key.target;
                const target = if (key.direction == .outgoing) key.target else key.source;

                return EdgeRef{
                    .id = key.edge_id,
                    .source = source,
                    .target = target,
                    .edge_type = key.edge_type,
                };
            } else {
                self.done = true;
                return null;
            }
        }

        /// Clean up iterator resources
        pub fn deinit(self: *EdgeRefIterator) void {
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
            .edge_id = 0,
        };
        const end_key = EdgeKey{
            .source = node_id,
            .direction = .incoming, // Stop before incoming edges
            .edge_type = 0,
            .target = 0,
            .edge_id = 0,
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
            .store = self,
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
            .edge_id = 0,
        };
        const end_key = EdgeKey{
            .source = node_id +| 1,
            .direction = .outgoing,
            .edge_type = 0,
            .target = 0,
            .edge_id = 0,
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
            .store = self,
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
            .edge_id = 0,
        };
        const end_key = EdgeKey{
            .source = node_id,
            .direction = .outgoing,
            .edge_type = edge_type +| 1,
            .target = 0,
            .edge_id = 0,
        };

        const start_bytes = start_key.toBytes();
        const end_bytes = end_key.toBytes();

        const tree_iter = self.tree.range(&start_bytes, null) catch |err| {
            return mapBTreeError(err);
        };

        return EdgeIterator{
            .tree_iter = tree_iter,
            .allocator = self.allocator,
            .store = self,
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
            .edge_id = 0,
        };
        const end_key = EdgeKey{
            .source = node_id,
            .direction = .incoming,
            .edge_type = edge_type +| 1,
            .target = 0,
            .edge_id = 0,
        };

        const start_bytes = start_key.toBytes();
        const end_bytes = end_key.toBytes();

        const tree_iter = self.tree.range(&start_bytes, null) catch |err| {
            return mapBTreeError(err);
        };

        return EdgeIterator{
            .tree_iter = tree_iter,
            .allocator = self.allocator,
            .store = self,
            .done = false,
            .end_key_storage = end_bytes,
        };
    }

    // ========================================================================
    // Lightweight Edge Reference Queries (No Property Deserialization)
    // ========================================================================

    /// Get lightweight iterator for outgoing edges (no property deserialization).
    /// Returns EdgeRef containing only (source, target, edge_type_id).
    /// Ideal for graph traversal (BFS/DFS) where properties are not needed.
    pub fn getOutgoingRefs(self: *Self, node_id: NodeId) EdgeError!EdgeRefIterator {
        // Range: (node_id, OUTGOING, 0, 0) to (node_id, INCOMING, 0, 0)
        const start_key = EdgeKey{
            .source = node_id,
            .direction = .outgoing,
            .edge_type = 0,
            .target = 0,
            .edge_id = 0,
        };
        const end_key = EdgeKey{
            .source = node_id,
            .direction = .incoming, // Stop before incoming edges
            .edge_type = 0,
            .target = 0,
            .edge_id = 0,
        };

        const start_bytes = start_key.toBytes();
        const end_bytes = end_key.toBytes();

        const tree_iter = self.tree.range(&start_bytes, null) catch |err| {
            return mapBTreeError(err);
        };

        return EdgeRefIterator{
            .tree_iter = tree_iter,
            .done = false,
            .end_key_storage = end_bytes,
        };
    }

    /// Get lightweight iterator for incoming edges (no property deserialization).
    /// Returns EdgeRef containing only (source, target, edge_type_id).
    pub fn getIncomingRefs(self: *Self, node_id: NodeId) EdgeError!EdgeRefIterator {
        // Range: (node_id, INCOMING, 0, 0) to (node_id + 1, OUTGOING, 0, 0)
        const start_key = EdgeKey{
            .source = node_id,
            .direction = .incoming,
            .edge_type = 0,
            .target = 0,
            .edge_id = 0,
        };
        const end_key = EdgeKey{
            .source = node_id +| 1,
            .direction = .outgoing,
            .edge_type = 0,
            .target = 0,
            .edge_id = 0,
        };

        const start_bytes = start_key.toBytes();
        const end_bytes = end_key.toBytes();

        const tree_iter = self.tree.range(&start_bytes, null) catch |err| {
            return mapBTreeError(err);
        };

        return EdgeRefIterator{
            .tree_iter = tree_iter,
            .done = false,
            .end_key_storage = end_bytes,
        };
    }

    /// Get lightweight iterator for outgoing edges of a specific type.
    pub fn getOutgoingRefsByType(self: *Self, node_id: NodeId, edge_type: SymbolId) EdgeError!EdgeRefIterator {
        // Range: (node_id, OUTGOING, type, 0) to (node_id, OUTGOING, type + 1, 0)
        const start_key = EdgeKey{
            .source = node_id,
            .direction = .outgoing,
            .edge_type = edge_type,
            .target = 0,
            .edge_id = 0,
        };
        const end_key = EdgeKey{
            .source = node_id,
            .direction = .outgoing,
            .edge_type = edge_type +| 1,
            .target = 0,
            .edge_id = 0,
        };

        const start_bytes = start_key.toBytes();
        const end_bytes = end_key.toBytes();

        const tree_iter = self.tree.range(&start_bytes, null) catch |err| {
            return mapBTreeError(err);
        };

        return EdgeRefIterator{
            .tree_iter = tree_iter,
            .done = false,
            .end_key_storage = end_bytes,
        };
    }

    /// Get lightweight iterator for incoming edges of a specific type.
    pub fn getIncomingRefsByType(self: *Self, node_id: NodeId, edge_type: SymbolId) EdgeError!EdgeRefIterator {
        // Range: (node_id, INCOMING, type, 0) to (node_id, INCOMING, type + 1, 0)
        const start_key = EdgeKey{
            .source = node_id,
            .direction = .incoming,
            .edge_type = edge_type,
            .target = 0,
            .edge_id = 0,
        };
        const end_key = EdgeKey{
            .source = node_id,
            .direction = .incoming,
            .edge_type = edge_type +| 1,
            .target = 0,
            .edge_id = 0,
        };

        const start_bytes = start_key.toBytes();
        const end_bytes = end_key.toBytes();

        const tree_iter = self.tree.range(&start_bytes, null) catch |err| {
            return mapBTreeError(err);
        };

        return EdgeRefIterator{
            .tree_iter = tree_iter,
            .done = false,
            .end_key_storage = end_bytes,
        };
    }

    // ========================================================================
    // Batch Edge Queries (Optimized for BFS)
    // ========================================================================

    /// Get outgoing edge refs for multiple nodes in a single B+Tree scan.
    /// Exploits B+Tree key ordering: sorted node IDs produce sorted key ranges.
    /// Significantly faster than individual getOutgoingRefs calls for BFS.
    ///
    /// Returns BatchEdgeResults with edges for all input nodes. The results
    /// are ordered by sorted node_id, not the original input order.
    pub fn getOutgoingRefsBatch(
        self: *Self,
        node_ids: []const NodeId,
        alloc: Allocator,
    ) EdgeError!BatchEdgeResults {
        if (node_ids.len == 0) {
            return BatchEdgeResults{
                .results = &.{},
                .edges = &.{},
                .allocator = alloc,
            };
        }

        // 1. Sort node IDs (copy to avoid mutating input)
        const sorted_ids = alloc.dupe(NodeId, node_ids) catch {
            return EdgeError.OutOfMemory;
        };
        defer alloc.free(sorted_ids);
        std.mem.sort(NodeId, sorted_ids, {}, std.sort.asc(NodeId));

        // 2. Allocate results array (one per input node)
        var results = alloc.alloc(BatchEdgeResult, sorted_ids.len) catch {
            return EdgeError.OutOfMemory;
        };
        errdefer alloc.free(results);

        // 3. Collect edges via single scan using unmanaged list
        var edges = std.ArrayListUnmanaged(EdgeRef){};
        errdefer edges.deinit(alloc);

        // 4. Start scan from first node's key
        const start_edge_key = EdgeKey{
            .source = sorted_ids[0],
            .direction = .outgoing,
            .edge_type = 0,
            .target = 0,
            .edge_id = 0,
        };
        const start_key = start_edge_key.toBytes();

        var tree_iter = self.tree.range(&start_key, null) catch |err| {
            return mapBTreeError(err);
        };
        defer tree_iter.deinit();

        // 5. Scan through B+Tree collecting edges for each node.
        // Use a pending_key buffer to avoid losing entries consumed from the iterator.
        var node_idx: usize = 0;
        var edges_start: usize = 0;
        var pending_key: ?[27]u8 = null; // Buffered key from lookahead
        var iter_exhausted = false;

        while (node_idx < sorted_ids.len) {
            const current_node = sorted_ids[node_idx];

            // End boundary: first incoming edge for this node
            const end_edge_key = EdgeKey{
                .source = current_node,
                .direction = .incoming,
                .edge_type = 0,
                .target = 0,
                .edge_id = 0,
            };
            const end_key = end_edge_key.toBytes();

            // Collect outgoing edges for current_node
            while (true) {
                // Get next key: from pending buffer or from iterator
                var key_bytes: [27]u8 = undefined;

                if (pending_key) |pk| {
                    key_bytes = pk;
                    pending_key = null;
                } else if (iter_exhausted) {
                    break;
                } else {
                    const entry = tree_iter.next() catch |err| {
                        return mapBTreeError(err);
                    };
                    if (entry) |e| {
                        @memcpy(&key_bytes, e.key[0..27]);
                    } else {
                        iter_exhausted = true;
                        break;
                    }
                }

                // Check if past current node's outgoing range
                if (std.mem.order(u8, &key_bytes, &end_key) != .lt) {
                    // Save for next node and stop collecting
                    pending_key = key_bytes;
                    break;
                }

                // Only collect outgoing edges from current_node
                const key = EdgeKey.fromBytes(&key_bytes);
                if (key.source == current_node and key.direction == .outgoing) {
                    edges.append(alloc, EdgeRef{
                        .id = key.edge_id,
                        .source = key.source,
                        .target = key.target,
                        .edge_type = key.edge_type,
                    }) catch {
                        return EdgeError.OutOfMemory;
                    };
                }
            }

            // Record result for this node
            results[node_idx] = .{
                .node_id = current_node,
                .edges_start = edges_start,
                .edges_len = edges.items.len - edges_start,
            };
            edges_start = edges.items.len;
            node_idx += 1;

            // Skip nodes that are before the pending key's source
            if (pending_key) |pk| {
                const pk_source = std.mem.readInt(u64, pk[0..8], .big);
                while (node_idx < sorted_ids.len and sorted_ids[node_idx] < pk_source) {
                    results[node_idx] = .{
                        .node_id = sorted_ids[node_idx],
                        .edges_start = edges_start,
                        .edges_len = 0,
                    };
                    node_idx += 1;
                }
            } else if (iter_exhausted) {
                // Fill remaining nodes with empty results
                while (node_idx < sorted_ids.len) {
                    results[node_idx] = .{
                        .node_id = sorted_ids[node_idx],
                        .edges_start = edges_start,
                        .edges_len = 0,
                    };
                    node_idx += 1;
                }
            }
        }

        return BatchEdgeResults{
            .results = results,
            .edges = edges.toOwnedSlice(alloc) catch {
                return EdgeError.OutOfMemory;
            },
            .allocator = alloc,
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
            .edge_id = 0,
        };
        const end_key = EdgeKey{
            .source = node_id +| 1,
            .direction = .outgoing,
            .edge_type = 0,
            .target = 0,
            .edge_id = 0,
        };

        const start_bytes = start_key.toBytes();
        const end_bytes = end_key.toBytes();

        const tree_iter = self.tree.range(&start_bytes, null) catch |err| {
            return mapBTreeError(err);
        };

        return EdgeIterator{
            .tree_iter = tree_iter,
            .allocator = self.allocator,
            .store = self,
            .done = false,
            .end_key_storage = end_bytes,
        };
    }

    /// Count outgoing edges from a node
    pub fn countOutgoing(self: *Self, node_id: NodeId) EdgeError!u64 {
        var iter = try self.getOutgoingRefs(node_id);
        defer iter.deinit();

        var count: u64 = 0;
        while (try iter.next()) |_| {
            count += 1;
        }
        return count;
    }

    /// Count incoming edges to a node
    pub fn countIncoming(self: *Self, node_id: NodeId) EdgeError!u64 {
        var iter = try self.getIncomingRefs(node_id);
        defer iter.deinit();

        var count: u64 = 0;
        while (try iter.next()) |_| {
            count += 1;
        }
        return count;
    }
};

// ============================================================================
// Serialization
// ============================================================================

/// Serialize full edge data for edge_id lookup tree.
fn serializeEdge(
    source: NodeId,
    target: NodeId,
    edge_type: SymbolId,
    properties: []const Property,
    buf: []u8,
) ![]u8 {
    var stream = std.io.fixedBufferStream(buf);
    const writer = stream.writer();

    try writer.writeInt(u64, source, .little);
    try writer.writeInt(u64, target, .little);
    try writer.writeInt(u16, edge_type, .little);

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
    edge_id: EdgeId,
    data: []const u8,
) !Edge {
    var stream = std.io.fixedBufferStream(data);
    const reader = stream.reader();

    const source = try reader.readInt(u64, .little);
    const target = try reader.readInt(u64, .little);
    const edge_type = try reader.readInt(u16, .little);

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
        .id = edge_id,
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
    var edge_id_tree = try BTree.init(allocator, &bp);

    var store = EdgeStore.init(allocator, &tree, &edge_id_tree);

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
    var edge_id_tree = try BTree.init(allocator, &bp);

    var store = EdgeStore.init(allocator, &tree, &edge_id_tree);

    // Create an edge
    const edge_type: SymbolId = 1000;
    try store.create(1, 2, edge_type, &[_]Property{});

    // Verify outgoing key exists
    const outgoing_key = EdgeKey{
        .source = 1,
        .direction = .outgoing,
        .edge_type = edge_type,
        .target = 2,
        .edge_id = 1,
    };
    const outgoing_bytes = outgoing_key.toBytes();
    const outgoing_result = tree.get(&outgoing_bytes) catch null;
    try std.testing.expect(outgoing_result != null);
    defer if (outgoing_result) |owned| tree.freeValue(owned);
    try std.testing.expectEqual(@as(usize, 0), outgoing_result.?.len);

    // Verify incoming key exists
    const incoming_key = EdgeKey{
        .source = 2,
        .direction = .incoming,
        .edge_type = edge_type,
        .target = 1,
        .edge_id = 1,
    };
    const incoming_bytes = incoming_key.toBytes();
    const incoming_result = tree.get(&incoming_bytes) catch null;
    try std.testing.expect(incoming_result != null);
    defer if (incoming_result) |owned| tree.freeValue(owned);
    try std.testing.expectEqual(@as(usize, 0), incoming_result.?.len);

    // Payload is stored once in edge_id index (space optimization).
    var id_key: [8]u8 = undefined;
    std.mem.writeInt(u64, &id_key, 1, .little);
    const id_result = edge_id_tree.get(&id_key) catch null;
    try std.testing.expect(id_result != null);
    defer if (id_result) |owned| edge_id_tree.freeValue(owned);
    try std.testing.expect(id_result.?.len > 0);
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
    var edge_id_tree = try BTree.init(allocator, &bp);

    var store = EdgeStore.init(allocator, &tree, &edge_id_tree);

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
    var edge_id_tree = try BTree.init(allocator, &bp);

    var store = EdgeStore.init(allocator, &tree, &edge_id_tree);

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
        .edge_id = 42,
    };

    const bytes = key.toBytes();
    const parsed = EdgeKey.fromBytes(&bytes);

    try std.testing.expectEqual(key.source, parsed.source);
    try std.testing.expectEqual(key.direction, parsed.direction);
    try std.testing.expectEqual(key.edge_type, parsed.edge_type);
    try std.testing.expectEqual(key.target, parsed.target);
    try std.testing.expectEqual(key.edge_id, parsed.edge_id);
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
    var edge_id_tree = try BTree.init(allocator, &bp);

    var store = EdgeStore.init(allocator, &tree, &edge_id_tree);

    const edge_type: SymbolId = 1000;

    // Create an edge
    try store.create(1, 2, edge_type, &[_]Property{});
    try std.testing.expect(store.exists(1, 2, edge_type));

    // Verify both outgoing and incoming keys exist
    const outgoing_key = EdgeKey{ .source = 1, .direction = .outgoing, .edge_type = edge_type, .target = 2, .edge_id = 1 };
    const incoming_key = EdgeKey{ .source = 2, .direction = .incoming, .edge_type = edge_type, .target = 1, .edge_id = 1 };
    try std.testing.expect(try tree.contains(&outgoing_key.toBytes()));
    try std.testing.expect(try tree.contains(&incoming_key.toBytes()));

    // Delete the edge
    try store.delete(1, 2, edge_type);
    try std.testing.expect(!store.exists(1, 2, edge_type));

    // Verify both keys are removed (double-delete)
    try std.testing.expect(!(try tree.contains(&outgoing_key.toBytes())));
    try std.testing.expect(!(try tree.contains(&incoming_key.toBytes())));

    // Deleting again should fail with NotFound
    try std.testing.expectError(EdgeError.NotFound, store.delete(1, 2, edge_type));
}

test "batch edge fetch matches individual fetches" {
    const allocator = std.testing.allocator;

    const vfs = lattice.storage.vfs;
    const buffer_pool = lattice.storage.buffer_pool;
    const page_manager = lattice.storage.page_manager;

    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const db_path = "/tmp/lattice_edge_batch_test.db";
    vfs_impl.delete(db_path) catch {};

    var pm = try page_manager.PageManager.init(allocator, vfs_impl, db_path, .{ .create = true });
    defer {
        pm.deinit();
        vfs_impl.delete(db_path) catch {};
    }

    var bp = try buffer_pool.BufferPool.init(allocator, &pm, 64 * 4096);
    defer bp.deinit();

    var tree = try BTree.init(allocator, &bp);
    var edge_id_tree = try BTree.init(allocator, &bp);

    var store = EdgeStore.init(allocator, &tree, &edge_id_tree);

    // Create a graph with gaps in node IDs to test sparse batching:
    // Node 1 -> 10, 20
    // Node 5 -> 30
    // Node 10 -> 1, 5
    // Node 50 -> 1
    // Node 100 (no outgoing edges)
    const type_a: SymbolId = 100;
    const type_b: SymbolId = 200;

    try store.create(1, 10, type_a, &[_]Property{});
    try store.create(1, 20, type_b, &[_]Property{});
    try store.create(5, 30, type_a, &[_]Property{});
    try store.create(10, 1, type_a, &[_]Property{});
    try store.create(10, 5, type_b, &[_]Property{});
    try store.create(50, 1, type_a, &[_]Property{});

    // Test batch fetch for nodes [1, 5, 10, 50, 100] (includes gaps and a node with no edges)
    const batch_nodes = [_]NodeId{ 100, 5, 1, 50, 10 }; // Unsorted to test sorting
    var batch_result = try store.getOutgoingRefsBatch(&batch_nodes, allocator);
    defer batch_result.deinit();

    // Verify we got 5 results (one per node)
    try std.testing.expectEqual(@as(usize, 5), batch_result.results.len);

    // Results should be sorted by node_id
    try std.testing.expectEqual(@as(NodeId, 1), batch_result.results[0].node_id);
    try std.testing.expectEqual(@as(NodeId, 5), batch_result.results[1].node_id);
    try std.testing.expectEqual(@as(NodeId, 10), batch_result.results[2].node_id);
    try std.testing.expectEqual(@as(NodeId, 50), batch_result.results[3].node_id);
    try std.testing.expectEqual(@as(NodeId, 100), batch_result.results[4].node_id);

    // Verify edge counts match individual fetches
    // Node 1: 2 outgoing edges
    try std.testing.expectEqual(@as(usize, 2), batch_result.getEdges(0).len);
    // Node 5: 1 outgoing edge
    try std.testing.expectEqual(@as(usize, 1), batch_result.getEdges(1).len);
    // Node 10: 2 outgoing edges
    try std.testing.expectEqual(@as(usize, 2), batch_result.getEdges(2).len);
    // Node 50: 1 outgoing edge
    try std.testing.expectEqual(@as(usize, 1), batch_result.getEdges(3).len);
    // Node 100: 0 outgoing edges
    try std.testing.expectEqual(@as(usize, 0), batch_result.getEdges(4).len);

    // Verify actual edge data matches individual iterator results
    // Node 1's edges
    const node1_edges = batch_result.getEdges(0);
    try std.testing.expectEqual(@as(NodeId, 1), node1_edges[0].source);
    try std.testing.expectEqual(@as(NodeId, 10), node1_edges[0].target);
    try std.testing.expectEqual(type_a, node1_edges[0].edge_type);
    try std.testing.expectEqual(@as(NodeId, 1), node1_edges[1].source);
    try std.testing.expectEqual(@as(NodeId, 20), node1_edges[1].target);
    try std.testing.expectEqual(type_b, node1_edges[1].edge_type);

    // Node 5's edges
    const node5_edges = batch_result.getEdges(1);
    try std.testing.expectEqual(@as(NodeId, 5), node5_edges[0].source);
    try std.testing.expectEqual(@as(NodeId, 30), node5_edges[0].target);

    // Node 10's edges
    const node10_edges = batch_result.getEdges(2);
    try std.testing.expectEqual(@as(NodeId, 10), node10_edges[0].source);
    try std.testing.expectEqual(@as(NodeId, 1), node10_edges[0].target);
    try std.testing.expectEqual(@as(NodeId, 10), node10_edges[1].source);
    try std.testing.expectEqual(@as(NodeId, 5), node10_edges[1].target);

    // Cross-check against individual getOutgoingRefs
    for (batch_result.results, 0..) |result, i| {
        var iter = try store.getOutgoingRefs(result.node_id);
        defer iter.deinit();

        const batch_edges = batch_result.getEdges(i);
        var individual_count: usize = 0;
        while (try iter.next()) |ref| {
            try std.testing.expect(individual_count < batch_edges.len);
            try std.testing.expectEqual(ref.source, batch_edges[individual_count].source);
            try std.testing.expectEqual(ref.target, batch_edges[individual_count].target);
            try std.testing.expectEqual(ref.edge_type, batch_edges[individual_count].edge_type);
            individual_count += 1;
        }
        try std.testing.expectEqual(batch_edges.len, individual_count);
    }
}

test "batch edge fetch empty input" {
    const allocator = std.testing.allocator;

    const vfs = lattice.storage.vfs;
    const buffer_pool = lattice.storage.buffer_pool;
    const page_manager = lattice.storage.page_manager;

    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const db_path = "/tmp/lattice_edge_batch_empty_test.db";
    vfs_impl.delete(db_path) catch {};

    var pm = try page_manager.PageManager.init(allocator, vfs_impl, db_path, .{ .create = true });
    defer {
        pm.deinit();
        vfs_impl.delete(db_path) catch {};
    }

    var bp = try buffer_pool.BufferPool.init(allocator, &pm, 64 * 4096);
    defer bp.deinit();

    var tree = try BTree.init(allocator, &bp);
    var edge_id_tree = try BTree.init(allocator, &bp);

    var store = EdgeStore.init(allocator, &tree, &edge_id_tree);

    // Empty batch
    const empty: []const NodeId = &.{};
    var result = try store.getOutgoingRefsBatch(empty, allocator);
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 0), result.results.len);
    try std.testing.expectEqual(@as(usize, 0), result.edges.len);
}

test "batch edge fetch nodes with no edges" {
    const allocator = std.testing.allocator;

    const vfs = lattice.storage.vfs;
    const buffer_pool = lattice.storage.buffer_pool;
    const page_manager = lattice.storage.page_manager;

    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const db_path = "/tmp/lattice_edge_batch_noedges_test.db";
    vfs_impl.delete(db_path) catch {};

    var pm = try page_manager.PageManager.init(allocator, vfs_impl, db_path, .{ .create = true });
    defer {
        pm.deinit();
        vfs_impl.delete(db_path) catch {};
    }

    var bp = try buffer_pool.BufferPool.init(allocator, &pm, 64 * 4096);
    defer bp.deinit();

    var tree = try BTree.init(allocator, &bp);
    var edge_id_tree = try BTree.init(allocator, &bp);

    var store = EdgeStore.init(allocator, &tree, &edge_id_tree);

    // Query nodes that don't exist in the tree
    const nodes = [_]NodeId{ 999, 1000, 1001 };
    var result = try store.getOutgoingRefsBatch(&nodes, allocator);
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 3), result.results.len);
    for (0..3) |i| {
        try std.testing.expectEqual(@as(usize, 0), result.getEdges(i).len);
    }
}

test "edge store supports parallel edges and deleteById" {
    const allocator = std.testing.allocator;

    const vfs = lattice.storage.vfs;
    const buffer_pool = lattice.storage.buffer_pool;
    const page_manager = lattice.storage.page_manager;

    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const db_path = "/tmp/lattice_edge_parallel_test.db";
    vfs_impl.delete(db_path) catch {};

    var pm = try page_manager.PageManager.init(allocator, vfs_impl, db_path, .{ .create = true });
    defer {
        pm.deinit();
        vfs_impl.delete(db_path) catch {};
    }

    var bp = try buffer_pool.BufferPool.init(allocator, &pm, 64 * 4096);
    defer bp.deinit();

    var tree = try BTree.init(allocator, &bp);
    var edge_id_tree = try BTree.init(allocator, &bp);
    var store = EdgeStore.init(allocator, &tree, &edge_id_tree);

    const edge_type: SymbolId = 1000;
    const props_a = [_]Property{.{ .key_id = 2000, .value = .{ .int_val = 1 } }};
    const props_b = [_]Property{.{ .key_id = 2000, .value = .{ .int_val = 2 } }};

    try store.create(1, 2, edge_type, &props_a);
    try store.create(1, 2, edge_type, &props_b);

    var refs_iter = try store.getOutgoingRefs(1);

    const ref1 = (try refs_iter.next()).?;
    const ref2 = (try refs_iter.next()).?;
    refs_iter.deinit();
    try std.testing.expect(ref1.id != ref2.id);

    var edge1 = try store.getById(ref1.id);
    defer edge1.deinit(allocator);
    var edge2 = try store.getById(ref2.id);
    defer edge2.deinit(allocator);

    try std.testing.expectEqual(@as(i64, 1), edge1.properties[0].value.int_val);
    try std.testing.expectEqual(@as(i64, 2), edge2.properties[0].value.int_val);

    try store.deleteById(ref1.id);
    try std.testing.expectEqual(@as(u64, 1), try store.countOutgoing(1));
    try std.testing.expect(store.exists(1, 2, edge_type));

    try store.delete(1, 2, edge_type);
    try std.testing.expectEqual(@as(u64, 0), try store.countOutgoing(1));
    try std.testing.expect(!store.exists(1, 2, edge_type));
}

test "edge id allocator persists across reopen" {
    const allocator = std.testing.allocator;

    const vfs = lattice.storage.vfs;
    const buffer_pool = lattice.storage.buffer_pool;
    const page_manager = lattice.storage.page_manager;

    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const db_path = "/tmp/lattice_edge_id_persist_test.db";
    vfs_impl.delete(db_path) catch {};

    var root_edge: types.PageId = types.NULL_PAGE;
    var root_edge_ids: types.PageId = types.NULL_PAGE;

    {
        var pm = try page_manager.PageManager.init(allocator, vfs_impl, db_path, .{ .create = true });
        defer pm.deinit();

        var bp = try buffer_pool.BufferPool.init(allocator, &pm, 64 * 4096);
        defer bp.deinit();

        var tree = try BTree.init(allocator, &bp);
        var edge_id_tree = try BTree.init(allocator, &bp);
        var store = EdgeStore.init(allocator, &tree, &edge_id_tree);

        try store.create(1, 2, 1000, &[_]Property{});
        try store.create(1, 3, 1000, &[_]Property{});

        root_edge = tree.getRootPage();
        root_edge_ids = edge_id_tree.getRootPage();
    }

    {
        var pm = try page_manager.PageManager.init(allocator, vfs_impl, db_path, .{});
        defer {
            pm.deinit();
            vfs_impl.delete(db_path) catch {};
        }

        var bp = try buffer_pool.BufferPool.init(allocator, &pm, 64 * 4096);
        defer bp.deinit();

        var tree = BTree.open(allocator, &bp, root_edge);
        var edge_id_tree = BTree.open(allocator, &bp, root_edge_ids);
        var store = EdgeStore.init(allocator, &tree, &edge_id_tree);

        try store.create(1, 4, 1000, &[_]Property{});

        var refs_iter = try store.getOutgoingRefs(1);
        defer refs_iter.deinit();

        var max_id: EdgeId = 0;
        while (try refs_iter.next()) |ref| {
            max_id = @max(max_id, ref.id);
        }
        try std.testing.expectEqual(@as(EdgeId, 3), max_id);
    }
}

test "edge id allocator falls back to max id when metadata key is missing" {
    const allocator = std.testing.allocator;

    const vfs = lattice.storage.vfs;
    const buffer_pool = lattice.storage.buffer_pool;
    const page_manager = lattice.storage.page_manager;

    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const db_path = "/tmp/lattice_edge_id_fallback_meta_missing_test.db";
    vfs_impl.delete(db_path) catch {};

    var pm = try page_manager.PageManager.init(allocator, vfs_impl, db_path, .{ .create = true });
    defer {
        pm.deinit();
        vfs_impl.delete(db_path) catch {};
    }

    var bp = try buffer_pool.BufferPool.init(allocator, &pm, 64 * 4096);
    defer bp.deinit();

    var tree = try BTree.init(allocator, &bp);
    var edge_id_tree = try BTree.init(allocator, &bp);
    var store = EdgeStore.init(allocator, &tree, &edge_id_tree);

    try store.createWithId(10, 1, 2, 1000, &[_]Property{});
    try store.createWithId(25, 1, 3, 1000, &[_]Property{});

    // Simulate metadata loss: remove the allocator metadata key.
    const meta_key = EdgeStore.edgeIdToKey(EdgeStore.EDGE_ID_META_KEY);
    try edge_id_tree.delete(&meta_key);

    // Reinitialize store against same trees; allocator should scan max edge_id.
    var reloaded = EdgeStore.init(allocator, &tree, &edge_id_tree);
    const next = try reloaded.createAndGetId(1, 4, 1000, &[_]Property{});
    try std.testing.expectEqual(@as(EdgeId, 26), next);
}

test "createWithId rejects duplicate edge_id" {
    const allocator = std.testing.allocator;

    const vfs = lattice.storage.vfs;
    const buffer_pool = lattice.storage.buffer_pool;
    const page_manager = lattice.storage.page_manager;

    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const db_path = "/tmp/lattice_edge_duplicate_id_test.db";
    vfs_impl.delete(db_path) catch {};

    var pm = try page_manager.PageManager.init(allocator, vfs_impl, db_path, .{ .create = true });
    defer {
        pm.deinit();
        vfs_impl.delete(db_path) catch {};
    }

    var bp = try buffer_pool.BufferPool.init(allocator, &pm, 64 * 4096);
    defer bp.deinit();

    var tree = try BTree.init(allocator, &bp);
    var edge_id_tree = try BTree.init(allocator, &bp);
    var store = EdgeStore.init(allocator, &tree, &edge_id_tree);

    try store.createWithId(7, 1, 2, 1000, &[_]Property{});
    try std.testing.expectError(
        EdgeError.AlreadyExists,
        store.createWithId(7, 1, 3, 1000, &[_]Property{}),
    );
}
