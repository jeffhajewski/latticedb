//! HNSW (Hierarchical Navigable Small World) vector index.
//!
//! Provides approximate nearest neighbor search for vector embeddings.
//!
//! The index uses a multi-layer graph structure where:
//! - Layer 0 contains all vectors with M_max0 connections each
//! - Upper layers contain exponentially fewer vectors with M connections each
//! - Search proceeds from top layer down, using greedy traversal

const std = @import("std");
const lattice = @import("lattice");

const Allocator = std.mem.Allocator;

const types = lattice.core.types;
const page = lattice.storage.page;
const buffer_pool = lattice.storage.buffer_pool;
const vec_storage = lattice.vector.storage;
const simd_distance = lattice.vector.distance;
const locking = lattice.concurrency.locking;

const PageId = types.PageId;
const NULL_PAGE = types.NULL_PAGE;
const PageHeader = page.PageHeader;
const BufferPool = buffer_pool.BufferPool;
const VectorStorage = vec_storage.VectorStorage;
const VectorLocation = vec_storage.VectorLocation;
const LatchMode = locking.LatchMode;

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

/// Location of a node's connections within a connection pool page
pub const ConnectionLocation = struct {
    page_id: PageId,
    slot_index: u16,
};

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
    /// Location of connections in pool page
    connections_loc: ConnectionLocation,

    pub const SERIALIZED_SIZE: usize = 8 + 1 + 6 + 6; // 21 bytes

    pub fn serialize(self: HnswNodeEntry, buf: []u8) void {
        std.mem.writeInt(u64, buf[0..8], self.vector_id, .little);
        buf[8] = self.max_layer;
        std.mem.writeInt(u32, buf[9..13], self.vector_loc.page_id, .little);
        std.mem.writeInt(u16, buf[13..15], self.vector_loc.slot_index, .little);
        std.mem.writeInt(u32, buf[15..19], self.connections_loc.page_id, .little);
        std.mem.writeInt(u16, buf[19..21], self.connections_loc.slot_index, .little);
    }

    pub fn deserialize(buf: []const u8) HnswNodeEntry {
        return HnswNodeEntry{
            .vector_id = std.mem.readInt(u64, buf[0..8], .little),
            .max_layer = buf[8],
            .vector_loc = VectorLocation{
                .page_id = std.mem.readInt(u32, buf[9..13], .little),
                .slot_index = std.mem.readInt(u16, buf[13..15], .little),
            },
            .connections_loc = ConnectionLocation{
                .page_id = std.mem.readInt(u32, buf[15..19], .little),
                .slot_index = std.mem.readInt(u16, buf[19..21], .little),
            },
        };
    }
};

/// Connection pool page header (after base PageHeader)
/// Each pool page contains fixed-size slots for nodes at a given level
pub const ConnectionPoolPageHeader = struct {
    /// max_layer this pool serves
    pool_level: u8,
    /// Number of slots currently used
    slot_count: u16,
    /// Maximum slots that fit in this page
    max_slots: u16,
    /// Bytes per slot
    slot_size: u16,
    /// Next page in this pool's chain (NULL_PAGE if none)
    next_page: PageId,
    /// Reserved for alignment
    _reserved: [3]u8,

    pub const SIZE: usize = 16;
    pub const OFFSET: usize = @sizeOf(PageHeader);

    pub fn read(buf: []const u8) ConnectionPoolPageHeader {
        const data = buf[OFFSET..][0..SIZE];
        return ConnectionPoolPageHeader{
            .pool_level = data[0],
            .slot_count = std.mem.readInt(u16, data[1..3], .little),
            .max_slots = std.mem.readInt(u16, data[3..5], .little),
            .slot_size = std.mem.readInt(u16, data[5..7], .little),
            .next_page = std.mem.readInt(u32, data[7..11], .little),
            ._reserved = data[11..14].*,
        };
    }

    pub fn write(self: ConnectionPoolPageHeader, buf: []u8) void {
        const data = buf[OFFSET..][0..SIZE];
        data[0] = self.pool_level;
        std.mem.writeInt(u16, data[1..3], self.slot_count, .little);
        std.mem.writeInt(u16, data[3..5], self.max_slots, .little);
        std.mem.writeInt(u16, data[5..7], self.slot_size, .little);
        std.mem.writeInt(u32, data[7..11], self.next_page, .little);
        @memset(data[11..14], 0);
    }
};

/// Offset where slot data starts in a connection pool page
const POOL_DATA_OFFSET: usize = @sizeOf(PageHeader) + ConnectionPoolPageHeader.SIZE;

// ============================================================================
// Connection Pool
// ============================================================================

/// Connection pool that packs multiple nodes' connections into shared pages.
/// Nodes are grouped by max_layer, with each level getting fixed-size slots.
const ConnectionPool = struct {
    allocator: Allocator,
    bp: *BufferPool,
    m: u16,
    m_max0: u16,
    /// Level -> head page of the pool chain for that level
    pool_heads: std.AutoHashMap(u8, PageId),
    /// Total pages allocated across all pools
    total_pages: u64,

    const Self = @This();
    const PAGE_SIZE: usize = 4096;

    fn init(allocator: Allocator, bp: *BufferPool, m: u16, m_max0: u16) Self {
        return Self{
            .allocator = allocator,
            .bp = bp,
            .m = m,
            .m_max0 = m_max0,
            .pool_heads = std.AutoHashMap(u8, PageId).init(allocator),
            .total_pages = 0,
        };
    }

    fn deinit(self: *Self) void {
        self.pool_heads.deinit();
    }

    /// Compute the slot size for a given max_layer.
    /// slot_size = 1 (layer_count) + (3 + m_max0 * 8) for layer 0
    ///           + level * (3 + m * 8) for upper layers
    fn slotSizeForLevel(level: u8, m: u16, m_max0: u16) u16 {
        const layer0_size: u32 = 3 + @as(u32, m_max0) * 8;
        const upper_layer_size: u32 = 3 + @as(u32, m) * 8;
        const total: u32 = 1 + layer0_size + @as(u32, level) * upper_layer_size;
        return @intCast(total);
    }

    /// Allocate a slot for a node with the given max_layer.
    fn allocateSlot(self: *Self, max_layer: u8) HnswError!ConnectionLocation {
        const slot_size = slotSizeForLevel(max_layer, self.m, self.m_max0);
        const usable = PAGE_SIZE - POOL_DATA_OFFSET;
        const max_slots: u16 = @intCast(usable / slot_size);

        // Try to find a page with space in the pool for this level
        if (self.pool_heads.get(max_layer)) |head_page_id| {
            const frame = self.bp.fetchPage(head_page_id, .exclusive) catch return HnswError.BufferPoolError;
            var hdr = ConnectionPoolPageHeader.read(frame.data);

            if (hdr.slot_count < hdr.max_slots) {
                const slot_index = hdr.slot_count;
                hdr.slot_count += 1;
                hdr.write(frame.data);

                // Zero the slot
                const slot_offset = POOL_DATA_OFFSET + @as(usize, slot_index) * @as(usize, slot_size);
                @memset(frame.data[slot_offset..][0..slot_size], 0);

                self.bp.unpinPage(frame, true);
                return ConnectionLocation{ .page_id = head_page_id, .slot_index = slot_index };
            }
            self.bp.unpinPage(frame, false);
        }

        // Need a new page — allocate and initialize
        const new_page_id = self.bp.pm.allocatePage() catch return HnswError.IoError;
        self.total_pages += 1;

        const frame = self.bp.fetchPage(new_page_id, .exclusive) catch return HnswError.BufferPoolError;

        // Initialize page header
        frame.data[0] = @intFromEnum(page.PageType.hnsw_layer);
        frame.data[1] = 0;
        std.mem.writeInt(u16, frame.data[2..4], 0, .little);
        std.mem.writeInt(u32, frame.data[4..8], 0, .little);

        // Link to previous head (if any)
        const prev_head = self.pool_heads.get(max_layer) orelse NULL_PAGE;

        // Initialize pool page header
        const hdr = ConnectionPoolPageHeader{
            .pool_level = max_layer,
            .slot_count = 1, // we're about to use the first slot
            .max_slots = max_slots,
            .slot_size = slot_size,
            .next_page = prev_head,
            ._reserved = [_]u8{0} ** 3,
        };
        hdr.write(frame.data);

        // Zero all slot data
        @memset(frame.data[POOL_DATA_OFFSET..], 0);

        self.bp.unpinPage(frame, true);

        // Update head pointer
        self.pool_heads.put(max_layer, new_page_id) catch return HnswError.OutOfMemory;

        return ConnectionLocation{ .page_id = new_page_id, .slot_index = 0 };
    }

    /// Read connections for a node at a specific layer from its slot
    fn getConnections(self: *Self, loc: ConnectionLocation, layer: u8) HnswError![]u64 {
        const frame = self.bp.fetchPage(loc.page_id, .shared) catch return HnswError.BufferPoolError;
        defer self.bp.unpinPage(frame, false);

        const hdr = ConnectionPoolPageHeader.read(frame.data);
        const slot_offset = POOL_DATA_OFFSET + @as(usize, loc.slot_index) * @as(usize, hdr.slot_size);

        // Read layer_count at start of slot
        const layer_count = frame.data[slot_offset];
        if (layer_count == 0) {
            return self.allocator.alloc(u64, 0) catch return HnswError.OutOfMemory;
        }

        // Parse layers within slot
        var offset = slot_offset + 1;
        const slot_end = slot_offset + @as(usize, hdr.slot_size);
        var i: u8 = 0;
        while (i < layer_count and offset < slot_end) : (i += 1) {
            const layer_num = frame.data[offset];
            const count = std.mem.readInt(u16, frame.data[offset + 1 ..][0..2], .little);
            offset += 3;

            if (layer_num == layer) {
                const neighbors = self.allocator.alloc(u64, count) catch return HnswError.OutOfMemory;
                for (0..count) |j| {
                    neighbors[j] = std.mem.readInt(u64, frame.data[offset + j * 8 ..][0..8], .little);
                }
                return neighbors;
            }

            offset += @as(usize, count) * 8;
        }

        return self.allocator.alloc(u64, 0) catch return HnswError.OutOfMemory;
    }

    /// Write connections for a node at a specific layer into its slot
    fn setConnections(self: *Self, loc: ConnectionLocation, node_id: u64, layer: u8, neighbors: []const u64) HnswError!void {
        _ = node_id;
        const frame = self.bp.fetchPage(loc.page_id, .exclusive) catch return HnswError.BufferPoolError;

        const hdr = ConnectionPoolPageHeader.read(frame.data);
        const slot_size = @as(usize, hdr.slot_size);
        const slot_offset = POOL_DATA_OFFSET + @as(usize, loc.slot_index) * slot_size;
        const slot_end = slot_offset + slot_size;

        // Read existing layers, rebuild with updated layer
        var layers_data = std.array_list.Managed(u8).init(self.allocator);
        defer layers_data.deinit();

        const old_layer_count = frame.data[slot_offset];
        var offset = slot_offset + 1;
        var new_layer_count: u8 = 0;

        // Copy existing layers (skip the one being updated)
        var i: u8 = 0;
        while (i < old_layer_count and offset < slot_end) : (i += 1) {
            const layer_num = frame.data[offset];
            const count = std.mem.readInt(u16, frame.data[offset + 1 ..][0..2], .little);

            if (layer_num == layer) {
                offset += 3 + @as(usize, count) * 8;
            } else {
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

        // Assert data fits within slot
        std.debug.assert(1 + layers_data.items.len <= slot_size);

        // Write back: layer_count + layer data + zero remaining
        frame.data[slot_offset] = new_layer_count;
        @memcpy(frame.data[slot_offset + 1 ..][0..layers_data.items.len], layers_data.items);
        // Zero remaining slot bytes
        const used = 1 + layers_data.items.len;
        if (used < slot_size) {
            @memset(frame.data[slot_offset + used .. slot_end], 0);
        }

        self.bp.unpinPage(frame, true);
    }
};

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

    // Connection pool for packed connection storage
    connection_pool: ConnectionPool,

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
            .connection_pool = ConnectionPool.init(allocator, bp, config.m, config.m_max0),
            .distance_fn = getDistanceFn(config.metric),
            .rng = std.Random.DefaultPrng.init(@intCast(std.time.timestamp())),
        };
    }

    /// Deinitialize the index
    pub fn deinit(self: *Self) void {
        self.connection_pool.deinit();
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

    /// Calculate distance between two stored nodes by their IDs
    fn distanceBetweenNodes(self: *Self, id_a: u64, id_b: u64) HnswError!f32 {
        const node_a = self.getNode(id_a) orelse return HnswError.NotFound;
        const node_b = self.getNode(id_b) orelse return HnswError.NotFound;
        const vec_a = try self.getVector(id_a, node_a.vector_loc);
        defer self.freeVector(vec_a);
        return try self.distanceTo(vec_a, node_b.vector_loc);
    }

    /// Select neighbors using the heuristic from HNSW paper (Algorithm 4).
    /// Picks diverse neighbors that cover different angular regions around the target.
    /// candidates must be sorted by distance ascending.
    /// Returns allocated []u64 that caller must free.
    fn selectNeighborsHeuristic(
        self: *Self,
        target_id: u64,
        candidates: []const SearchResult,
        max_neighbors: u16,
    ) HnswError![]u64 {
        if (candidates.len == 0) {
            return self.allocator.alloc(u64, 0) catch return HnswError.OutOfMemory;
        }

        var selected = std.array_list.Managed(u64).init(self.allocator);
        defer selected.deinit();

        // Track which candidates have been used
        var used = self.allocator.alloc(bool, candidates.len) catch return HnswError.OutOfMemory;
        defer self.allocator.free(used);
        @memset(used, false);

        // Phase 1 (diversity): select candidate only if closer to target than
        // to any already-selected neighbor
        for (candidates, 0..) |candidate, idx| {
            if (selected.items.len >= max_neighbors) break;
            if (candidate.node_id == target_id) {
                used[idx] = true;
                continue;
            }

            var is_diverse = true;
            for (selected.items) |sel_id| {
                const dist_to_selected = self.distanceBetweenNodes(candidate.node_id, sel_id) catch candidate.distance + 1.0;
                if (dist_to_selected <= candidate.distance) {
                    is_diverse = false;
                    break;
                }
            }

            if (is_diverse) {
                selected.append(candidate.node_id) catch return HnswError.OutOfMemory;
                used[idx] = true;
            }
        }

        // Phase 2 (backfill): fill remaining slots with closest unused candidates
        if (selected.items.len < max_neighbors) {
            for (candidates, 0..) |candidate, idx| {
                if (selected.items.len >= max_neighbors) break;
                if (used[idx]) continue;
                if (candidate.node_id == target_id) continue;
                selected.append(candidate.node_id) catch return HnswError.OutOfMemory;
            }
        }

        return selected.toOwnedSlice() catch return HnswError.OutOfMemory;
    }

    // ========================================================================
    // Connection Management
    // ========================================================================

    /// Read connections for a node at a specific layer
    /// Returns allocated slice that caller must free
    pub fn getConnections(
        self: *Self,
        loc: ConnectionLocation,
        layer: u8,
    ) HnswError![]u64 {
        return self.connection_pool.getConnections(loc, layer);
    }

    /// Write connections for a node at a specific layer
    pub fn setConnections(
        self: *Self,
        loc: ConnectionLocation,
        node_id: u64,
        layer: u8,
        neighbors: []const u64,
    ) HnswError!void {
        return self.connection_pool.setConnections(loc, node_id, layer, neighbors);
    }

    /// Add a single connection to a node's layer, using heuristic pruning
    /// when at max_connections capacity.
    pub fn addConnection(
        self: *Self,
        loc: ConnectionLocation,
        node_id: u64,
        layer: u8,
        neighbor: u64,
        max_connections: u16,
    ) HnswError!void {
        const neighbors = try self.getConnections(loc, layer);
        defer self.allocator.free(neighbors);

        // Check if already connected
        for (neighbors) |n| {
            if (n == neighbor) return;
        }

        if (neighbors.len < max_connections) {
            // Room available — just append
            var new_neighbors = self.allocator.alloc(u64, neighbors.len + 1) catch return HnswError.OutOfMemory;
            defer self.allocator.free(new_neighbors);

            @memcpy(new_neighbors[0..neighbors.len], neighbors);
            new_neighbors[neighbors.len] = neighbor;

            try self.setConnections(loc, node_id, layer, new_neighbors);
        } else {
            // At capacity — use heuristic pruning to select diverse neighbors
            const node_entry = self.getNode(node_id) orelse return;
            const node_vec = try self.getVector(node_id, node_entry.vector_loc);
            defer self.freeVector(node_vec);

            // Build candidate list: existing neighbors + new neighbor
            var candidates = self.allocator.alloc(SearchResult, neighbors.len + 1) catch return HnswError.OutOfMemory;
            defer self.allocator.free(candidates);

            for (neighbors, 0..) |n, i| {
                const n_entry = self.getNode(n) orelse {
                    candidates[i] = .{ .node_id = n, .distance = std.math.inf(f32) };
                    continue;
                };
                candidates[i] = .{
                    .node_id = n,
                    .distance = try self.distanceTo(node_vec, n_entry.vector_loc),
                };
            }
            // Add the new neighbor candidate
            const nb_entry = self.getNode(neighbor) orelse return;
            candidates[neighbors.len] = .{
                .node_id = neighbor,
                .distance = try self.distanceTo(node_vec, nb_entry.vector_loc),
            };

            // Sort by distance ascending
            std.mem.sort(SearchResult, candidates, {}, struct {
                fn lessThan(_: void, a: SearchResult, b: SearchResult) bool {
                    return a.distance < b.distance;
                }
            }.lessThan);

            const selected = try self.selectNeighborsHeuristic(node_id, candidates, max_connections);
            defer self.allocator.free(selected);

            try self.setConnections(loc, node_id, layer, selected);
        }
    }

    /// Estimate total memory usage of the HNSW index
    fn calculateMemoryUsage(self: *Self) u64 {
        var total: u64 = @sizeOf(Self);

        // HashMap: key + value + 1-byte metadata per slot
        const cap: u64 = self.nodes.capacity();
        total += cap * (@sizeOf(u64) + @sizeOf(HnswNodeEntry) + 1);

        // Connection pool pages (packed, shared across vectors)
        total += self.connection_pool.total_pages * PAGE_SIZE;

        // Vector storage pages
        if (self.vector_count > 0) {
            const vpp: u64 = self.vector_storage.vectors_per_page;
            const vector_pages = (self.vector_count + vpp - 1) / vpp;
            total += vector_pages * PAGE_SIZE;
        }

        return total;
    }

    /// Get statistics about the index
    pub fn getStats(self: *Self) HnswStats {
        return HnswStats{
            .dimensions = self.config.dimensions,
            .total_vectors = self.vector_count,
            .max_layer = self.max_layer,
            .entry_point = self.entry_point orelse 0,
            .memory_bytes = self.calculateMemoryUsage(),
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
            const neighbors = try self.getConnections(current_node.connections_loc, layer);
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
            const neighbors = try self.getConnections(node.connections_loc, layer);
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

        // Allocate connection slot in pool
        const connections_loc = try self.connection_pool.allocateSlot(level);

        // Create node entry
        const entry = HnswNodeEntry{
            .vector_id = vector_id,
            .max_layer = level,
            .vector_loc = vector_loc,
            .connections_loc = connections_loc,
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

            // Select diverse neighbors using heuristic (Algorithm 4)
            const max_conn: u16 = if (layer_u8 == 0) self.config.m_max0 else self.config.m;

            // Set forward connections (from new node to neighbors)
            const neighbors = try self.selectNeighborsHeuristic(vector_id, candidates, max_conn);
            defer self.allocator.free(neighbors);

            try self.setConnections(connections_loc, vector_id, layer_u8, neighbors);

            // Add backlinks (from neighbors to new node) and prune if needed
            for (neighbors) |neighbor| {
                const neighbor_entry = self.getNode(neighbor) orelse continue;
                try self.addConnection(
                    neighbor_entry.connections_loc,
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

    const vfs = lattice.storage.vfs;
    const page_manager = lattice.storage.page_manager;

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

    const vfs = lattice.storage.vfs;
    const page_manager = lattice.storage.page_manager;

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

    const vfs = lattice.storage.vfs;
    const page_manager = lattice.storage.page_manager;

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

    const vfs = lattice.storage.vfs;
    const page_manager = lattice.storage.page_manager;

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
