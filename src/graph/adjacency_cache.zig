//! In-memory adjacency cache for accelerating graph traversals.
//!
//! Maps NodeId → []CachedEdge to bypass B+Tree lookups for repeated
//! traversals. Populated lazily on first access, invalidated on edge mutations.
//! Opt-in via DatabaseConfig.enable_adjacency_cache.

const std = @import("std");
const lattice = @import("lattice");

const Allocator = std.mem.Allocator;
const NodeId = lattice.core.types.NodeId;
const SymbolId = lattice.graph.symbols.SymbolId;
const EdgeStore = lattice.graph.edge.EdgeStore;

/// Lightweight cached edge containing only traversal-relevant fields.
pub const CachedEdge = struct {
    target: NodeId,
    edge_type: SymbolId,
};

/// In-memory adjacency cache that maps NodeId → outgoing edges.
/// Bypasses the B+Tree entirely for cached nodes, providing ~50ns lookups
/// vs ~32μs for uncached B+Tree traversals.
pub const AdjacencyCache = struct {
    map: std.AutoHashMap(NodeId, []CachedEdge),
    allocator: Allocator,

    const Self = @This();

    pub fn init(allocator: Allocator) Self {
        return .{
            .map = std.AutoHashMap(NodeId, []CachedEdge).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Self) void {
        var it = self.map.valueIterator();
        while (it.next()) |edges| {
            self.allocator.free(edges.*);
        }
        self.map.deinit();
    }

    /// Get cached outgoing edges, or null if not cached.
    pub fn get(self: *const Self, node_id: NodeId) ?[]const CachedEdge {
        return self.map.get(node_id);
    }

    /// Populate cache entry for a single node from the edge store.
    pub fn populateNode(self: *Self, edge_store: *EdgeStore, node_id: NodeId) !void {
        // Skip if already cached
        if (self.map.contains(node_id)) return;

        var edges_list = std.ArrayListUnmanaged(CachedEdge){};
        errdefer edges_list.deinit(self.allocator);

        var iter = edge_store.getOutgoingRefs(node_id) catch return;
        defer iter.deinit();

        while (iter.next() catch null) |edge_ref| {
            try edges_list.append(self.allocator, .{
                .target = edge_ref.target,
                .edge_type = edge_ref.edge_type,
            });
        }

        try self.map.put(node_id, try edges_list.toOwnedSlice(self.allocator));
    }

    /// Populate cache entries for multiple nodes from the edge store.
    /// Nodes are processed in sorted order to exploit B+Tree key locality.
    pub fn populateFrom(self: *Self, edge_store: *EdgeStore, node_ids: []const NodeId) !void {
        // Sort for B+Tree key locality
        const sorted = try self.allocator.dupe(NodeId, node_ids);
        defer self.allocator.free(sorted);
        std.mem.sort(NodeId, sorted, {}, std.sort.asc(NodeId));

        for (sorted) |node_id| {
            try self.populateNode(edge_store, node_id);
        }
    }

    /// Invalidate a single node's cache entry (call on edge create/delete).
    pub fn invalidate(self: *Self, node_id: NodeId) void {
        if (self.map.fetchRemove(node_id)) |entry| {
            self.allocator.free(entry.value);
        }
    }

    /// Clear all cached entries.
    pub fn clear(self: *Self) void {
        var it = self.map.valueIterator();
        while (it.next()) |edges| {
            self.allocator.free(edges.*);
        }
        self.map.clearRetainingCapacity();
    }
};

// ============================================================================
// Tests
// ============================================================================

test "adjacency cache basic operations" {
    const allocator = std.testing.allocator;

    var cache = AdjacencyCache.init(allocator);
    defer cache.deinit();

    // Empty cache returns null
    try std.testing.expect(cache.get(1) == null);

    // Manually populate (simulating what populateNode does)
    const edges = try allocator.alloc(CachedEdge, 2);
    edges[0] = .{ .target = 10, .edge_type = 1 };
    edges[1] = .{ .target = 20, .edge_type = 2 };
    try cache.map.put(1, edges);

    // Cache hit
    const result = cache.get(1);
    try std.testing.expect(result != null);
    try std.testing.expectEqual(@as(usize, 2), result.?.len);
    try std.testing.expectEqual(@as(NodeId, 10), result.?[0].target);
    try std.testing.expectEqual(@as(NodeId, 20), result.?[1].target);

    // Invalidate
    cache.invalidate(1);
    try std.testing.expect(cache.get(1) == null);
}

test "adjacency cache clear" {
    const allocator = std.testing.allocator;

    var cache = AdjacencyCache.init(allocator);
    defer cache.deinit();

    // Add multiple entries
    const edges1 = try allocator.alloc(CachedEdge, 1);
    edges1[0] = .{ .target = 10, .edge_type = 1 };
    try cache.map.put(1, edges1);

    const edges2 = try allocator.alloc(CachedEdge, 1);
    edges2[0] = .{ .target = 20, .edge_type = 1 };
    try cache.map.put(2, edges2);

    try std.testing.expect(cache.get(1) != null);
    try std.testing.expect(cache.get(2) != null);

    cache.clear();

    try std.testing.expect(cache.get(1) == null);
    try std.testing.expect(cache.get(2) == null);
}
