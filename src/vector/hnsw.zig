//! HNSW (Hierarchical Navigable Small World) vector index.
//!
//! Provides approximate nearest neighbor search for vector embeddings.

const std = @import("std");
const types = @import("../core/types.zig");

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

/// Calculate Euclidean distance between two vectors
pub fn euclideanDistance(a: []const f32, b: []const f32) f32 {
    std.debug.assert(a.len == b.len);
    var sum: f32 = 0.0;
    for (a, b) |ai, bi| {
        const diff = ai - bi;
        sum += diff * diff;
    }
    return @sqrt(sum);
}

/// Calculate cosine distance between two vectors
pub fn cosineDistance(a: []const f32, b: []const f32) f32 {
    std.debug.assert(a.len == b.len);
    var dot: f32 = 0.0;
    var norm_a: f32 = 0.0;
    var norm_b: f32 = 0.0;
    for (a, b) |ai, bi| {
        dot += ai * bi;
        norm_a += ai * ai;
        norm_b += bi * bi;
    }
    const denom = @sqrt(norm_a) * @sqrt(norm_b);
    if (denom == 0.0) return 1.0;
    return 1.0 - (dot / denom);
}

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
