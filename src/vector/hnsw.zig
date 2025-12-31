//! HNSW (Hierarchical Navigable Small World) vector index.
//!
//! Provides approximate nearest neighbor search for vector embeddings.
//!
//! The index uses a multi-layer graph structure where:
//! - Layer 0 contains all vectors with M_max0 connections each
//! - Upper layers contain exponentially fewer vectors with M connections each
//! - Search proceeds from top layer down, using greedy traversal

const std = @import("std");
const types = @import("../core/types.zig");
const page = @import("../storage/page.zig");
const buffer_pool = @import("../storage/buffer_pool.zig");
const storage = @import("storage.zig");
const simd_distance = @import("distance.zig");

const Allocator = std.mem.Allocator;
const PageId = types.PageId;
const NULL_PAGE = types.NULL_PAGE;
const PageHeader = page.PageHeader;
const BufferPool = buffer_pool.BufferPool;
const VectorStorage = storage.VectorStorage;
const VectorLocation = storage.VectorLocation;
const LatchMode = @import("../concurrency/locking.zig").LatchMode;

pub const NodeId = types.NodeId;
pub const VectorDimension = types.VectorDimension;

/// Distance metric for vector similarity
pub const DistanceMetric = enum {
    /// Euclidean (L2) distance
    euclidean,
    /// Cosine similarity (1 - cosine)
    cosine,
    /// Inner product (negative for similarity)
    inner_product,
};

/// HNSW index configuration
pub const HnswConfig = struct {
    /// Number of dimensions in vectors
    dimensions: VectorDimension,
    /// Maximum number of connections per node per layer
    m: u16 = 16,
    /// Maximum connections for layer 0 (typically 2*M)
    m_max0: u16 = 32,
    /// Size of dynamic candidate list during construction
    ef_construction: u16 = 200,
    /// Size of dynamic candidate list during search
    ef_search: u16 = 64,
    /// Distance metric
    metric: DistanceMetric = .cosine,
    /// Normalization factor for level generation
    ml: f32 = 1.0 / @log(2.0),
};

/// HNSW node entry (per layer)
pub const HnswNode = struct {
    /// Associated database node ID
    node_id: NodeId,
    /// Layer this entry is on
    layer: u8,
    /// Connections to other nodes at this layer
    connections: []NodeId,
};

/// HNSW layer metadata
pub const HnswLayerInfo = struct {
    /// Layer number (0 is bottom)
    layer: u8,
    /// Number of nodes at this layer
    node_count: u64,
    /// Entry point for this layer
    entry_point: NodeId,
};

/// Search result with distance
pub const SearchResult = struct {
    node_id: NodeId,
    distance: f32,
};

/// HNSW index statistics
pub const HnswStats = struct {
    dimensions: VectorDimension,
    total_vectors: u64,
    max_layer: u8,
    entry_point: NodeId,
    memory_bytes: u64,
};

/// Calculate Euclidean distance between two vectors (SIMD-optimized)
pub const euclideanDistance = simd_distance.euclideanDistance;

/// Calculate cosine distance between two vectors (SIMD-optimized)
pub const cosineDistance = simd_distance.cosineDistance;

/// Calculate negative inner product (SIMD-optimized)
pub const innerProductDistance = simd_distance.innerProductDistance;

/// Distance function type
pub const DistanceFn = *const fn ([]const f32, []const f32) f32;

/// Get the distance function for a metric (returns SIMD-optimized version)
pub fn getDistanceFn(metric: DistanceMetric) DistanceFn {
    return switch (metric) {
        .euclidean => simd_distance.euclideanDistance,
        .cosine => simd_distance.cosineDistance,
        .inner_product => simd_distance.innerProductDistance,
    };
}

// ============================================================================
// HNSW Index Errors
// ============================================================================

/// HNSW index errors
pub const HnswError = error{
    /// Vector not found in index
    NotFound,
    /// Dimension mismatch
    DimensionMismatch,
    /// Index is empty
    EmptyIndex,
    /// I/O error
    IoError,
    /// Out of memory
    OutOfMemory,
    /// Buffer pool error
    BufferPoolError,
    /// Vector storage error
    StorageError,
};

// ============================================================================
// Connection Page Types
// ============================================================================

/// Node entry stored in the node index B+Tree
/// Key: vector_id (u64)
/// Value: HnswNodeEntry (serialized)
pub const HnswNodeEntry = struct {
    /// Vector ID (same as key, for verification)
    vector_id: u64,
    /// Maximum layer this node appears on
    max_layer: u8,
    /// Location of vector data
    vector_loc: VectorLocation,
    /// Page containing connections
    connections_page: PageId,

    pub const SERIALIZED_SIZE: usize = 8 + 1 + 6 + 4; // 19 bytes

    pub fn serialize(self: HnswNodeEntry, buf: []u8) void {
        std.mem.writeInt(u64, buf[0..8], self.vector_id, .little);
        buf[8] = self.max_layer;
        std.mem.writeInt(u32, buf[9..13], self.vector_loc.page_id, .little);
        std.mem.writeInt(u16, buf[13..15], self.vector_loc.slot_index, .little);
        std.mem.writeInt(u32, buf[15..19], self.connections_page, .little);
    }

    pub fn deserialize(buf: []const u8) HnswNodeEntry {
        return HnswNodeEntry{
            .vector_id = std.mem.readInt(u64, buf[0..8], .little),
            .max_layer = buf[8],
            .vector_loc = VectorLocation{
                .page_id = std.mem.readInt(u32, buf[9..13], .little),
                .slot_index = std.mem.readInt(u16, buf[13..15], .little),
            },
            .connections_page = std.mem.readInt(u32, buf[15..19], .little),
        };
    }
};

/// Connection page header (after base PageHeader)
/// Stores all layer connections for a single node
pub const ConnectionPageHeader = struct {
    /// Node this page belongs to
    node_id: u64,
    /// Number of layers stored
    layer_count: u8,
    /// Reserved
    _reserved: [7]u8,

    pub const SIZE: usize = 16;
    pub const OFFSET: usize = @sizeOf(PageHeader);

    pub fn read(buf: []const u8) ConnectionPageHeader {
        const data = buf[OFFSET..][0..SIZE];
        return ConnectionPageHeader{
            .node_id = std.mem.readInt(u64, data[0..8], .little),
            .layer_count = data[8],
            ._reserved = data[9..16].*,
        };
    }

    pub fn write(self: ConnectionPageHeader, buf: []u8) void {
        const data = buf[OFFSET..][0..SIZE];
        std.mem.writeInt(u64, data[0..8], self.node_id, .little);
        data[8] = self.layer_count;
        @memset(data[9..16], 0);
    }
};

/// Offset where connection data starts in a connection page
const CONNECTION_DATA_OFFSET: usize = @sizeOf(PageHeader) + ConnectionPageHeader.SIZE;

// ============================================================================
// HNSW Index
// ============================================================================

/// HNSW Index structure
pub const HnswIndex = struct {
    allocator: Allocator,
    bp: *BufferPool,
    config: HnswConfig,

    // Index state
    entry_point: ?u64,
    max_layer: u8,
    vector_count: u64,

    // Storage
    vector_storage: *VectorStorage,

    // Node registry: vector_id -> HnswNodeEntry
    nodes: std.AutoHashMap(u64, HnswNodeEntry),

    // Distance function
    distance_fn: DistanceFn,

    // Random number generator for level assignment
    rng: std.Random.DefaultPrng,

    const Self = @This();
    const PAGE_SIZE: usize = 4096;

    /// Initialize a new HNSW index
    pub fn init(
        allocator: Allocator,
        bp: *BufferPool,
        vector_storage: *VectorStorage,
        config: HnswConfig,
    ) Self {
        return Self{
            .allocator = allocator,
            .bp = bp,
            .config = config,
            .entry_point = null,
            .max_layer = 0,
            .vector_count = 0,
            .vector_storage = vector_storage,
            .nodes = std.AutoHashMap(u64, HnswNodeEntry).init(allocator),
            .distance_fn = getDistanceFn(config.metric),
            .rng = std.Random.DefaultPrng.init(@intCast(std.time.timestamp())),
        };
    }

    /// Deinitialize the index
    pub fn deinit(self: *Self) void {
        self.nodes.deinit();
    }

    /// Get a node entry by ID
    pub fn getNode(self: *Self, vector_id: u64) ?HnswNodeEntry {
        return self.nodes.get(vector_id);
    }

    /// Generate a random level for a new node
    /// Uses exponential distribution: level = floor(-ln(uniform) * ml)
    pub fn randomLevel(self: *Self) u8 {
        const r = self.rng.random().float(f32);
        if (r == 0.0) return 0;
        const level_f = -@log(r) * self.config.ml;
        const level: u8 = @intFromFloat(@min(level_f, 255.0));
        return level;
    }

    /// Get vector data by ID from vector storage
    fn getVector(self: *Self, vector_id: u64, loc: VectorLocation) HnswError![]f32 {
        _ = vector_id;
        return self.vector_storage.getByLocation(loc) catch return HnswError.StorageError;
    }

    /// Free vector data
    fn freeVector(self: *Self, vector: []f32) void {
        self.vector_storage.free(vector);
    }

    /// Calculate distance between query and a stored vector
    fn distanceTo(self: *Self, query: []const f32, loc: VectorLocation) HnswError!f32 {
        const vec = try self.getVector(0, loc);
        defer self.freeVector(vec);
        return self.distance_fn(query, vec);
    }

    // ========================================================================
    // Connection Management
    // ========================================================================

    /// Read connections for a node at a specific layer
    /// Returns allocated slice that caller must free
    pub fn getConnections(
        self: *Self,
        connections_page: PageId,
        layer: u8,
    ) HnswError![]u64 {
        const frame = self.bp.fetchPage(connections_page, .shared) catch return HnswError.BufferPoolError;
        defer self.bp.unpinPage(frame, false);

        const header = ConnectionPageHeader.read(frame.data);
        if (layer >= header.layer_count) {
            return self.allocator.alloc(u64, 0) catch return HnswError.OutOfMemory;
        }

        // Parse connection data
        // Format: For each layer: [layer_num: u8][count: u16][neighbors: u64 × count]
        var offset: usize = CONNECTION_DATA_OFFSET;
        var i: u8 = 0;
        while (i < header.layer_count and offset < PAGE_SIZE) : (i += 1) {
            const layer_num = frame.data[offset];
            const count = std.mem.readInt(u16, frame.data[offset + 1 ..][0..2], .little);
            offset += 3;

            if (layer_num == layer) {
                // Found the layer
                const neighbors = self.allocator.alloc(u64, count) catch return HnswError.OutOfMemory;
                for (0..count) |j| {
                    neighbors[j] = std.mem.readInt(u64, frame.data[offset + j * 8 ..][0..8], .little);
                }
                return neighbors;
            }

            // Skip this layer's data
            offset += @as(usize, count) * 8;
        }

        // Layer not found
        return self.allocator.alloc(u64, 0) catch return HnswError.OutOfMemory;
    }

    /// Write connections for a node at a specific layer
    pub fn setConnections(
        self: *Self,
        connections_page: PageId,
        node_id: u64,
        layer: u8,
        neighbors: []const u64,
    ) HnswError!void {
        const frame = self.bp.fetchPage(connections_page, .exclusive) catch return HnswError.BufferPoolError;

        var header = ConnectionPageHeader.read(frame.data);

        // If this is a new page, initialize it
        if (header.node_id == 0) {
            header.node_id = node_id;
            header.layer_count = 0;
            header._reserved = [_]u8{0} ** 7;

            // Write page header directly (PageHeader is extern struct)
            frame.data[0] = @intFromEnum(page.PageType.hnsw_layer);
            frame.data[1] = 0; // flags
            std.mem.writeInt(u16, frame.data[2..4], 0, .little); // reserved
            std.mem.writeInt(u32, frame.data[4..8], 0, .little); // checksum
        }

        // Read existing layers and rebuild with new/updated layer
        var layers_data = std.array_list.Managed(u8).init(self.allocator);
        defer layers_data.deinit();

        var offset: usize = CONNECTION_DATA_OFFSET;
        var new_layer_count: u8 = 0;

        // Copy existing layers (excluding the one we're updating)
        var i: u8 = 0;
        while (i < header.layer_count and offset < PAGE_SIZE) : (i += 1) {
            const layer_num = frame.data[offset];
            const count = std.mem.readInt(u16, frame.data[offset + 1 ..][0..2], .little);

            if (layer_num == layer) {
                // Skip this layer, we'll add updated version below
                offset += 3 + @as(usize, count) * 8;
            } else {
                // Copy this layer
                const layer_size = 3 + @as(usize, count) * 8;
                layers_data.appendSlice(frame.data[offset .. offset + layer_size]) catch {
                    self.bp.unpinPage(frame, false);
                    return HnswError.OutOfMemory;
                };
                new_layer_count += 1;
                offset += layer_size;
            }
        }

        // Add the new/updated layer
        if (neighbors.len > 0) {
            layers_data.append(layer) catch {
                self.bp.unpinPage(frame, false);
                return HnswError.OutOfMemory;
            };

            var count_bytes: [2]u8 = undefined;
            std.mem.writeInt(u16, &count_bytes, @intCast(neighbors.len), .little);
            layers_data.appendSlice(&count_bytes) catch {
                self.bp.unpinPage(frame, false);
                return HnswError.OutOfMemory;
            };

            for (neighbors) |neighbor| {
                var neighbor_bytes: [8]u8 = undefined;
                std.mem.writeInt(u64, &neighbor_bytes, neighbor, .little);
                layers_data.appendSlice(&neighbor_bytes) catch {
                    self.bp.unpinPage(frame, false);
                    return HnswError.OutOfMemory;
                };
            }
            new_layer_count += 1;
        }

        // Write everything back
        header.layer_count = new_layer_count;
        header.write(frame.data);

        @memcpy(frame.data[CONNECTION_DATA_OFFSET..][0..layers_data.items.len], layers_data.items);

        self.bp.unpinPage(frame, true);
    }

    /// Add a single connection to a node's layer
    pub fn addConnection(
        self: *Self,
        connections_page: PageId,
        node_id: u64,
        layer: u8,
        neighbor: u64,
        max_connections: u16,
    ) HnswError!void {
        const neighbors = try self.getConnections(connections_page, layer);
        defer self.allocator.free(neighbors);

        // Check if already connected
        for (neighbors) |n| {
            if (n == neighbor) return;
        }

        // Add new connection
        if (neighbors.len < max_connections) {
            var new_neighbors = self.allocator.alloc(u64, neighbors.len + 1) catch return HnswError.OutOfMemory;
            defer self.allocator.free(new_neighbors);

            @memcpy(new_neighbors[0..neighbors.len], neighbors);
            new_neighbors[neighbors.len] = neighbor;

            try self.setConnections(connections_page, node_id, layer, new_neighbors);
        }
        // If at max, would need pruning (handled elsewhere)
    }

    /// Get statistics about the index
    pub fn getStats(self: *Self) HnswStats {
        return HnswStats{
            .dimensions = self.config.dimensions,
            .total_vectors = self.vector_count,
            .max_layer = self.max_layer,
            .entry_point = self.entry_point orelse 0,
            .memory_bytes = 0, // TODO: Calculate actual memory usage
        };
    }

    // ========================================================================
    // Search Algorithms
    // ========================================================================

    /// Search for k nearest neighbors
    pub fn search(self: *Self, query: []const f32, k: u32, ef_override: ?u16) HnswError![]SearchResult {
        if (self.entry_point == null) {
            return self.allocator.alloc(SearchResult, 0) catch return HnswError.OutOfMemory;
        }

        const ef = ef_override orelse self.config.ef_search;
        std.debug.assert(query.len == self.config.dimensions);

        var current = self.entry_point.?;
        var current_layer: i16 = @intCast(self.max_layer);

        // Phase 1: Greedy descent through upper layers
        while (current_layer > 0) : (current_layer -= 1) {
            current = try self.searchLayerGreedy(query, current, @intCast(current_layer));
        }

        // Phase 2: Search layer 0 with beam width ef
        const candidates = try self.searchLayer(query, current, 0, ef);
        defer self.allocator.free(candidates);

        // Return top k results
        const result_count = @min(k, candidates.len);
        const results = self.allocator.alloc(SearchResult, result_count) catch return HnswError.OutOfMemory;
        @memcpy(results, candidates[0..result_count]);

        return results;
    }

    /// Greedy search within a single layer (returns closest node)
    fn searchLayerGreedy(self: *Self, query: []const f32, entry: u64, layer: u8) HnswError!u64 {
        const entry_node = self.getNode(entry) orelse return HnswError.NotFound;

        var current = entry;
        var current_node = entry_node;
        var current_dist = try self.distanceTo(query, current_node.vector_loc);

        var improved = true;
        while (improved) {
            improved = false;
            const neighbors = try self.getConnections(current_node.connections_page, layer);
            defer self.allocator.free(neighbors);

            for (neighbors) |neighbor| {
                const neighbor_node = self.getNode(neighbor) orelse continue;
                const dist = try self.distanceTo(query, neighbor_node.vector_loc);
                if (dist < current_dist) {
                    current = neighbor;
                    current_node = neighbor_node;
                    current_dist = dist;
                    improved = true;
                }
            }
        }
        return current;
    }

    /// Search layer with beam width (returns sorted candidates)
    fn searchLayer(self: *Self, query: []const f32, entry: u64, layer: u8, ef: u16) HnswError![]SearchResult {
        const entry_node = self.getNode(entry) orelse return HnswError.NotFound;

        // Candidates and results stored as dynamic arrays
        var candidates = std.array_list.Managed(SearchResult).init(self.allocator);
        defer candidates.deinit();

        var results = std.array_list.Managed(SearchResult).init(self.allocator);
        errdefer results.deinit();

        var visited = std.AutoHashMap(u64, void).init(self.allocator);
        defer visited.deinit();

        // Initialize with entry point
        const entry_dist = try self.distanceTo(query, entry_node.vector_loc);
        candidates.append(.{ .node_id = entry, .distance = entry_dist }) catch return HnswError.OutOfMemory;
        results.append(.{ .node_id = entry, .distance = entry_dist }) catch return HnswError.OutOfMemory;
        visited.put(entry, {}) catch return HnswError.OutOfMemory;

        while (candidates.items.len > 0) {
            // Find and remove closest candidate
            var closest_idx: usize = 0;
            var closest_dist = candidates.items[0].distance;
            for (candidates.items[1..], 1..) |c, i| {
                if (c.distance < closest_dist) {
                    closest_idx = i;
                    closest_dist = c.distance;
                }
            }
            const closest = candidates.orderedRemove(closest_idx);

            // Find furthest in results
            var furthest_dist: f32 = 0;
            for (results.items) |r| {
                if (r.distance > furthest_dist) {
                    furthest_dist = r.distance;
                }
            }

            // Stop if closest candidate is worse than worst result
            if (results.items.len >= ef and closest.distance > furthest_dist) {
                break;
            }

            // Explore neighbors
            const node = self.getNode(closest.node_id) orelse continue;
            const neighbors = try self.getConnections(node.connections_page, layer);
            defer self.allocator.free(neighbors);

            for (neighbors) |neighbor| {
                if (visited.contains(neighbor)) continue;
                visited.put(neighbor, {}) catch return HnswError.OutOfMemory;

                const neighbor_node = self.getNode(neighbor) orelse continue;
                const dist = try self.distanceTo(query, neighbor_node.vector_loc);

                // Recalculate furthest after potential modifications
                var current_furthest: f32 = 0;
                for (results.items) |r| {
                    if (r.distance > current_furthest) {
                        current_furthest = r.distance;
                    }
                }

                if (results.items.len < ef or dist < current_furthest) {
                    candidates.append(.{ .node_id = neighbor, .distance = dist }) catch return HnswError.OutOfMemory;
                    results.append(.{ .node_id = neighbor, .distance = dist }) catch return HnswError.OutOfMemory;

                    // Remove furthest if over ef
                    if (results.items.len > ef) {
                        var furthest_idx: usize = 0;
                        var max_dist: f32 = results.items[0].distance;
                        for (results.items[1..], 1..) |r, i| {
                            if (r.distance > max_dist) {
                                furthest_idx = i;
                                max_dist = r.distance;
                            }
                        }
                        _ = results.orderedRemove(furthest_idx);
                    }
                }
            }
        }

        // Sort results by distance
        std.mem.sort(SearchResult, results.items, {}, struct {
            fn lessThan(_: void, a: SearchResult, b: SearchResult) bool {
                return a.distance < b.distance;
            }
        }.lessThan);

        return results.toOwnedSlice() catch return HnswError.OutOfMemory;
    }

    // ========================================================================
    // Insert Algorithm
    // ========================================================================

    /// Insert a vector into the index
    pub fn insert(self: *Self, vector_id: u64, vector: []const f32) HnswError!void {
        std.debug.assert(vector.len == self.config.dimensions);

        // Store the vector data
        const vector_loc = self.vector_storage.store(vector_id, vector) catch return HnswError.StorageError;

        // Assign random level
        const level = self.randomLevel();

        // Allocate connection page
        const connections_page = self.bp.pm.allocatePage() catch return HnswError.IoError;

        // Create node entry
        const entry = HnswNodeEntry{
            .vector_id = vector_id,
            .max_layer = level,
            .vector_loc = vector_loc,
            .connections_page = connections_page,
        };

        // Register node
        self.nodes.put(vector_id, entry) catch return HnswError.OutOfMemory;

        // First vector becomes entry point
        if (self.entry_point == null) {
            self.entry_point = vector_id;
            self.max_layer = level;
            self.vector_count = 1;
            return;
        }

        var current = self.entry_point.?;

        // Phase 1: Descend from top to insertion level (greedy)
        var current_layer: i16 = @intCast(self.max_layer);
        while (current_layer > level) : (current_layer -= 1) {
            current = try self.searchLayerGreedy(vector, current, @intCast(current_layer));
        }

        // Phase 2: Insert at each layer from level down to 0
        var insert_layer: i16 = @min(@as(i16, level), @as(i16, @intCast(self.max_layer)));
        while (insert_layer >= 0) : (insert_layer -= 1) {
            const layer_u8: u8 = @intCast(insert_layer);

            // Find ef_construction nearest neighbors
            const candidates = try self.searchLayer(vector, current, layer_u8, self.config.ef_construction);
            defer self.allocator.free(candidates);

            // Select M best neighbors
            const max_conn: u16 = if (layer_u8 == 0) self.config.m_max0 else self.config.m;
            const neighbor_count = @min(max_conn, @as(u16, @intCast(candidates.len)));

            // Set forward connections (from new node to neighbors)
            const neighbors = self.allocator.alloc(u64, neighbor_count) catch return HnswError.OutOfMemory;
            defer self.allocator.free(neighbors);

            for (0..neighbor_count) |i| {
                neighbors[i] = candidates[i].node_id;
            }

            try self.setConnections(connections_page, vector_id, layer_u8, neighbors);

            // Add backlinks (from neighbors to new node) and prune if needed
            for (neighbors) |neighbor| {
                const neighbor_entry = self.getNode(neighbor) orelse continue;
                try self.addConnection(
                    neighbor_entry.connections_page,
                    neighbor,
                    layer_u8,
                    vector_id,
                    max_conn,
                );
            }

            // Use best candidate as entry for next layer
            if (candidates.len > 0) {
                current = candidates[0].node_id;
            }

            if (insert_layer == 0) break;
        }

        // Update entry point if new node is higher
        if (level > self.max_layer) {
            self.entry_point = vector_id;
            self.max_layer = level;
        }

        self.vector_count += 1;
    }

    /// Free search results allocated by search()
    pub fn freeResults(self: *Self, results: []SearchResult) void {
        self.allocator.free(results);
    }
};

// ============================================================================
// Tests
// ============================================================================

test "euclidean distance" {
    const a = [_]f32{ 0.0, 0.0 };
    const b = [_]f32{ 3.0, 4.0 };
    try std.testing.expectApproxEqAbs(@as(f32, 5.0), euclideanDistance(&a, &b), 0.001);
}

test "cosine distance identical" {
    const a = [_]f32{ 1.0, 0.0 };
    const b = [_]f32{ 1.0, 0.0 };
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), cosineDistance(&a, &b), 0.001);
}

test "inner product distance" {
    const a = [_]f32{ 1.0, 2.0, 3.0 };
    const b = [_]f32{ 4.0, 5.0, 6.0 };
    // dot = 1*4 + 2*5 + 3*6 = 4 + 10 + 18 = 32
    // inner product distance = -32
    try std.testing.expectApproxEqAbs(@as(f32, -32.0), innerProductDistance(&a, &b), 0.001);
}

test "hnsw index init and stats" {
    const allocator = std.testing.allocator;

    const vfs = @import("../storage/vfs.zig");
    const page_manager = @import("../storage/page_manager.zig");

    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const db_path = "/tmp/lattice_hnsw_init_test.db";
    vfs_impl.delete(db_path) catch {};

    var pm = try page_manager.PageManager.init(allocator, vfs_impl, db_path, .{ .create = true });
    defer {
        pm.deinit();
        vfs_impl.delete(db_path) catch {};
    }

    var bp = try buffer_pool.BufferPool.init(allocator, &pm, 64 * 4096);
    defer bp.deinit();

    var vs = try VectorStorage.init(allocator, &bp, 4);

    const config = HnswConfig{
        .dimensions = 4,
        .m = 4,
        .m_max0 = 8,
        .ef_construction = 16,
        .ef_search = 8,
        .metric = .euclidean,
    };

    var index = HnswIndex.init(allocator, &bp, &vs, config);
    defer index.deinit();

    const stats = index.getStats();
    try std.testing.expectEqual(@as(u64, 0), stats.total_vectors);
    try std.testing.expectEqual(@as(u8, 0), stats.max_layer);
}

test "hnsw insert single vector" {
    const allocator = std.testing.allocator;

    const vfs = @import("../storage/vfs.zig");
    const page_manager = @import("../storage/page_manager.zig");

    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const db_path = "/tmp/lattice_hnsw_insert_test.db";
    vfs_impl.delete(db_path) catch {};

    var pm = try page_manager.PageManager.init(allocator, vfs_impl, db_path, .{ .create = true });
    defer {
        pm.deinit();
        vfs_impl.delete(db_path) catch {};
    }

    var bp = try buffer_pool.BufferPool.init(allocator, &pm, 64 * 4096);
    defer bp.deinit();

    var vs = try VectorStorage.init(allocator, &bp, 4);

    const config = HnswConfig{
        .dimensions = 4,
        .m = 4,
        .m_max0 = 8,
        .ef_construction = 16,
        .ef_search = 8,
        .metric = .euclidean,
    };

    var index = HnswIndex.init(allocator, &bp, &vs, config);
    defer index.deinit();

    // Insert a vector
    const v1 = [_]f32{ 1.0, 0.0, 0.0, 0.0 };
    try index.insert(1, &v1);

    try std.testing.expectEqual(@as(u64, 1), index.vector_count);
    try std.testing.expectEqual(@as(u64, 1), index.entry_point.?);
}

test "hnsw insert and search" {
    const allocator = std.testing.allocator;

    const vfs = @import("../storage/vfs.zig");
    const page_manager = @import("../storage/page_manager.zig");

    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const db_path = "/tmp/lattice_hnsw_search_test.db";
    vfs_impl.delete(db_path) catch {};

    var pm = try page_manager.PageManager.init(allocator, vfs_impl, db_path, .{ .create = true });
    defer {
        pm.deinit();
        vfs_impl.delete(db_path) catch {};
    }

    var bp = try buffer_pool.BufferPool.init(allocator, &pm, 64 * 4096);
    defer bp.deinit();

    var vs = try VectorStorage.init(allocator, &bp, 4);

    const config = HnswConfig{
        .dimensions = 4,
        .m = 4,
        .m_max0 = 8,
        .ef_construction = 16,
        .ef_search = 8,
        .metric = .euclidean,
    };

    var index = HnswIndex.init(allocator, &bp, &vs, config);
    defer index.deinit();

    // Insert several vectors
    const v1 = [_]f32{ 1.0, 0.0, 0.0, 0.0 };
    const v2 = [_]f32{ 0.0, 1.0, 0.0, 0.0 };
    const v3 = [_]f32{ 0.0, 0.0, 1.0, 0.0 };
    const v4 = [_]f32{ 0.0, 0.0, 0.0, 1.0 };

    try index.insert(1, &v1);
    try index.insert(2, &v2);
    try index.insert(3, &v3);
    try index.insert(4, &v4);

    try std.testing.expectEqual(@as(u64, 4), index.vector_count);

    // Search for vector closest to v1
    const query = [_]f32{ 0.9, 0.1, 0.0, 0.0 };
    const results = try index.search(&query, 1, null);
    defer index.freeResults(results);

    try std.testing.expectEqual(@as(usize, 1), results.len);
    try std.testing.expectEqual(@as(u64, 1), results[0].node_id);
}

test "hnsw search empty index" {
    const allocator = std.testing.allocator;

    const vfs = @import("../storage/vfs.zig");
    const page_manager = @import("../storage/page_manager.zig");

    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const db_path = "/tmp/lattice_hnsw_empty_test.db";
    vfs_impl.delete(db_path) catch {};

    var pm = try page_manager.PageManager.init(allocator, vfs_impl, db_path, .{ .create = true });
    defer {
        pm.deinit();
        vfs_impl.delete(db_path) catch {};
    }

    var bp = try buffer_pool.BufferPool.init(allocator, &pm, 64 * 4096);
    defer bp.deinit();

    var vs = try VectorStorage.init(allocator, &bp, 4);

    const config = HnswConfig{
        .dimensions = 4,
        .metric = .euclidean,
    };

    var index = HnswIndex.init(allocator, &bp, &vs, config);
    defer index.deinit();

    // Search empty index
    const query = [_]f32{ 1.0, 0.0, 0.0, 0.0 };
    const results = try index.search(&query, 5, null);
    defer index.freeResults(results);

    try std.testing.expectEqual(@as(usize, 0), results.len);
}
