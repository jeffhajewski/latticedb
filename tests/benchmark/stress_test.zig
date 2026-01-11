//! LatticeDB Stress Tests - Finding the Limits
//!
//! This file tests:
//! - Maximum graph size (nodes and edges)
//! - Query performance at scale
//! - Memory usage patterns
//! - Graph algorithm feasibility (centrality, clustering, etc.)

const std = @import("std");
const lattice = @import("lattice");
const Database = lattice.storage.database.Database;

const Allocator = std.mem.Allocator;

// ============================================================================
// Timing Utilities
// ============================================================================

fn formatDuration(ns: u64) struct { value: f64, unit: []const u8 } {
    if (ns < 1_000) return .{ .value = @floatFromInt(ns), .unit = "ns" };
    if (ns < 1_000_000) return .{ .value = @as(f64, @floatFromInt(ns)) / 1_000.0, .unit = "μs" };
    if (ns < 1_000_000_000) return .{ .value = @as(f64, @floatFromInt(ns)) / 1_000_000.0, .unit = "ms" };
    return .{ .value = @as(f64, @floatFromInt(ns)) / 1_000_000_000.0, .unit = "s" };
}

fn printTiming(label: []const u8, ns: u64) void {
    const t = formatDuration(ns);
    std.debug.print("  {s}: {d:.2} {s}\n", .{ label, t.value, t.unit });
}

// ============================================================================
// Graph Statistics
// ============================================================================

const GraphStats = struct {
    node_count: u64,
    edge_count: u64,
    avg_out_degree: f64,
    max_out_degree: u64,
    density: f64,

    fn print(self: GraphStats) void {
        std.debug.print("\n  Graph Statistics:\n", .{});
        std.debug.print("    Nodes: {d}\n", .{self.node_count});
        std.debug.print("    Edges: {d}\n", .{self.edge_count});
        std.debug.print("    Avg out-degree: {d:.2}\n", .{self.avg_out_degree});
        std.debug.print("    Max out-degree: {d}\n", .{self.max_out_degree});
        std.debug.print("    Density: {d:.6}\n", .{self.density});
    }
};

fn computeGraphStats(db: *Database, node_ids: []const u64) !GraphStats {
    var total_edges: u64 = 0;
    var max_degree: u64 = 0;

    for (node_ids) |node_id| {
        const edges = db.getOutgoingEdges(node_id) catch continue;
        defer db.freeEdgeInfos(edges);

        total_edges += edges.len;
        if (edges.len > max_degree) max_degree = edges.len;
    }

    const n = node_ids.len;
    const avg_degree = if (n > 0) @as(f64, @floatFromInt(total_edges)) / @as(f64, @floatFromInt(n)) else 0;
    const max_possible = n * (n - 1);
    const density = if (max_possible > 0) @as(f64, @floatFromInt(total_edges)) / @as(f64, @floatFromInt(max_possible)) else 0;

    return GraphStats{
        .node_count = n,
        .edge_count = total_edges,
        .avg_out_degree = avg_degree,
        .max_out_degree = max_degree,
        .density = density,
    };
}

// ============================================================================
// Graph Algorithms (Testing Feasibility)
// ============================================================================

/// Compute degree centrality for all nodes
/// Returns map of node_id -> centrality score
fn computeDegreeCentrality(allocator: Allocator, db: *Database, node_ids: []const u64) !std.AutoHashMap(u64, f64) {
    var centrality = std.AutoHashMap(u64, f64).init(allocator);
    errdefer centrality.deinit();

    const n = node_ids.len;
    if (n <= 1) return centrality;

    const normalizer = 1.0 / @as(f64, @floatFromInt(n - 1));

    for (node_ids) |node_id| {
        const out_edges = db.getOutgoingEdges(node_id) catch continue;
        defer db.freeEdgeInfos(out_edges);

        const in_edges = db.getIncomingEdges(node_id) catch continue;
        defer db.freeEdgeInfos(in_edges);

        const total_degree = out_edges.len + in_edges.len;
        const score = @as(f64, @floatFromInt(total_degree)) * normalizer;
        try centrality.put(node_id, score);
    }

    return centrality;
}

const NodeScore = struct { id: u64, score: f64 };

/// Find top-k nodes by centrality
fn topKByCentrality(allocator: Allocator, centrality: *std.AutoHashMap(u64, f64), k: usize) ![]NodeScore {
    var items = try allocator.alloc(NodeScore, centrality.count());
    defer allocator.free(items);

    var i: usize = 0;
    var iter = centrality.iterator();
    while (iter.next()) |entry| {
        items[i] = .{ .id = entry.key_ptr.*, .score = entry.value_ptr.* };
        i += 1;
    }

    // Sort by score descending
    std.mem.sort(NodeScore, items, {}, struct {
        fn lessThan(_: void, a: NodeScore, b: NodeScore) bool {
            return a.score > b.score;
        }
    }.lessThan);

    const result_size = @min(k, items.len);
    const result = try allocator.alloc(NodeScore, result_size);
    @memcpy(result, items[0..result_size]);
    return result;
}

/// Compute local clustering coefficient for a node
fn localClusteringCoefficient(db: *Database, node_id: u64) !f64 {
    const neighbors = db.getOutgoingEdges(node_id) catch return 0;
    defer db.freeEdgeInfos(neighbors);

    const k = neighbors.len;
    if (k < 2) return 0;

    // Count edges between neighbors
    var triangles: u64 = 0;
    for (neighbors) |n1| {
        for (neighbors) |n2| {
            if (n1.target != n2.target) {
                // Check if n1.target -> n2.target edge exists
                if (db.edgeExists(n1.target, n2.target, "KNOWS")) {
                    triangles += 1;
                }
            }
        }
    }

    // Clustering coefficient = triangles / (k * (k-1))
    const max_triangles = k * (k - 1);
    return @as(f64, @floatFromInt(triangles)) / @as(f64, @floatFromInt(max_triangles));
}

/// Compute average clustering coefficient for the graph
fn averageClusteringCoefficient(db: *Database, node_ids: []const u64) !f64 {
    var total: f64 = 0;
    var count: u64 = 0;

    for (node_ids) |node_id| {
        const cc = localClusteringCoefficient(db, node_id) catch continue;
        total += cc;
        count += 1;
    }

    return if (count > 0) total / @as(f64, @floatFromInt(count)) else 0;
}

/// BFS shortest path between two nodes
fn shortestPath(allocator: Allocator, db: *Database, start: u64, end: u64, max_depth: u32) !?u32 {
    if (start == end) return 0;

    var visited = std.AutoHashMap(u64, void).init(allocator);
    defer visited.deinit();

    var current_level: std.ArrayList(u64) = .empty;
    defer current_level.deinit(allocator);
    var next_level: std.ArrayList(u64) = .empty;
    defer next_level.deinit(allocator);

    try current_level.append(allocator, start);
    try visited.put(start, {});

    var depth: u32 = 0;
    while (depth < max_depth and current_level.items.len > 0) {
        depth += 1;
        next_level.clearRetainingCapacity();

        for (current_level.items) |node_id| {
            const edges = db.getOutgoingEdges(node_id) catch continue;
            defer db.freeEdgeInfos(edges);

            for (edges) |edge| {
                if (edge.target == end) return depth;

                if (!visited.contains(edge.target)) {
                    try visited.put(edge.target, {});
                    try next_level.append(allocator, edge.target);
                }
            }
        }

        std.mem.swap(std.ArrayList(u64), &current_level, &next_level);
    }

    return null; // No path found
}

// ============================================================================
// Stress Tests
// ============================================================================

fn stressTestNodeCreation(allocator: Allocator, target_count: usize) !void {
    std.debug.print("\n═══ Node Creation Stress Test ({d} nodes) ═══\n", .{target_count});

    const path = "/tmp/lattice_stress_nodes.ltdb";
    std.fs.cwd().deleteFile(path) catch {};

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{
            .enable_wal = false,
            .buffer_pool_size = 64 * 1024 * 1024, // 64MB buffer pool
        },
    });
    defer {
        db.close();
        std.fs.cwd().deleteFile(path) catch {};
    }

    const labels = &[_][]const u8{"StressNode"};

    var node_ids = try allocator.alloc(u64, target_count);
    defer allocator.free(node_ids);

    // Create nodes in batches and measure
    const batch_size: usize = 10000;
    var total_time: u64 = 0;
    var batch_start = std.time.nanoTimestamp();
    var actual_count: usize = 0;

    for (0..target_count) |i| {
        node_ids[i] = db.createNode(null, labels) catch |err| {
            std.debug.print("  ⚠ Hit limit at {d} nodes: {}\n", .{ i, err });
            break;
        };
        actual_count = i + 1;

        if (actual_count % batch_size == 0) {
            const batch_end = std.time.nanoTimestamp();
            const batch_time = @as(u64, @intCast(batch_end - batch_start));
            total_time += batch_time;

            const ops_per_sec = @as(f64, @floatFromInt(batch_size)) / (@as(f64, @floatFromInt(batch_time)) / 1_000_000_000.0);
            std.debug.print("  Created {d} nodes ({d:.0} nodes/sec)\n", .{ actual_count, ops_per_sec });

            batch_start = std.time.nanoTimestamp();
        }
    }

    std.debug.print("  Final count: {d} nodes\n", .{actual_count});
    printTiming("Total creation time", total_time);

    if (actual_count == 0) return;

    // Measure lookup performance at scale
    const lookup_start = std.time.nanoTimestamp();
    const lookup_count: usize = @min(10000, actual_count);
    const rng = std.crypto.random;

    for (0..lookup_count) |_| {
        const idx = rng.intRangeAtMost(usize, 0, actual_count - 1);
        _ = db.getNode(node_ids[idx]) catch {};
    }

    const lookup_end = std.time.nanoTimestamp();
    const lookup_time = @as(u64, @intCast(lookup_end - lookup_start));
    const avg_lookup = lookup_time / lookup_count;

    printTiming("Avg lookup time", avg_lookup);

    // Check file size
    const file = try std.fs.cwd().openFile(path, .{});
    defer file.close();
    const stat = try file.stat();
    std.debug.print("  Database size: {d:.2} MB\n", .{@as(f64, @floatFromInt(stat.size)) / (1024.0 * 1024.0)});
}

fn stressTestEdgeCreation(allocator: Allocator, node_count: usize, edges_per_node: usize) !void {
    const total_edges = node_count * edges_per_node;
    std.debug.print("\n═══ Edge Creation Stress Test ({d} nodes, {d} edges) ═══\n", .{ node_count, total_edges });

    const path = "/tmp/lattice_stress_edges.ltdb";
    std.fs.cwd().deleteFile(path) catch {};

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{ .enable_wal = false },
    });
    defer {
        db.close();
        std.fs.cwd().deleteFile(path) catch {};
    }

    // Create nodes first
    std.debug.print("  Creating {d} nodes...\n", .{node_count});
    const labels = &[_][]const u8{"Person"};
    var node_ids = try allocator.alloc(u64, node_count);
    defer allocator.free(node_ids);

    for (0..node_count) |i| {
        node_ids[i] = try db.createNode(null, labels);
    }

    // Create edges
    std.debug.print("  Creating edges ({d} per node)...\n", .{edges_per_node});
    const rng = std.crypto.random;
    const edge_start = std.time.nanoTimestamp();
    var edge_count: u64 = 0;

    for (node_ids) |source| {
        for (0..edges_per_node) |_| {
            const target_idx = rng.intRangeAtMost(usize, 0, node_count - 1);
            const target = node_ids[target_idx];
            if (source != target) {
                _ = db.createEdge(null, source, target, "KNOWS") catch continue;
                edge_count += 1;
            }
        }
    }

    const edge_end = std.time.nanoTimestamp();
    const edge_time = @as(u64, @intCast(edge_end - edge_start));

    printTiming("Total edge creation time", edge_time);
    std.debug.print("  Edges created: {d}\n", .{edge_count});

    // Compute and print graph stats
    const stats = try computeGraphStats(db, node_ids);
    stats.print();

    // Test traversal performance
    std.debug.print("\n  Traversal Performance:\n", .{});
    const traversal_start = std.time.nanoTimestamp();
    const traversal_count: usize = 1000;

    for (0..traversal_count) |i| {
        const edges = db.getOutgoingEdges(node_ids[i % node_count]) catch continue;
        db.freeEdgeInfos(edges);
    }

    const traversal_end = std.time.nanoTimestamp();
    const avg_traversal = @as(u64, @intCast(traversal_end - traversal_start)) / traversal_count;
    printTiming("    Avg edge traversal", avg_traversal);
}

fn stressTestGraphAlgorithms(allocator: Allocator, node_count: usize, edges_per_node: usize) !void {
    std.debug.print("\n═══ Graph Algorithm Stress Test ({d} nodes) ═══\n", .{node_count});

    const path = "/tmp/lattice_stress_algo.ltdb";
    std.fs.cwd().deleteFile(path) catch {};

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{ .enable_wal = false },
    });
    defer {
        db.close();
        std.fs.cwd().deleteFile(path) catch {};
    }

    // Create graph
    std.debug.print("  Building graph...\n", .{});
    const labels = &[_][]const u8{"Person"};
    var node_ids = try allocator.alloc(u64, node_count);
    defer allocator.free(node_ids);

    for (0..node_count) |i| {
        node_ids[i] = try db.createNode(null, labels);
    }

    // Create edges with some clustering (preferential attachment-like)
    const rng = std.crypto.random;
    for (node_ids, 0..) |source, i| {
        for (0..edges_per_node) |_| {
            // Mix of random and local connections
            const target_idx = if (rng.boolean())
                rng.intRangeAtMost(usize, 0, node_count - 1)
            else
                @mod(i + rng.intRangeAtMost(usize, 1, 10), node_count);
            const target = node_ids[target_idx];
            if (source != target) {
                _ = db.createEdge(null, source, target, "KNOWS") catch {};
            }
        }
    }

    // Test Degree Centrality
    std.debug.print("\n  Degree Centrality:\n", .{});
    const cent_start = std.time.nanoTimestamp();
    var centrality = try computeDegreeCentrality(allocator, db, node_ids);
    defer centrality.deinit();
    const cent_end = std.time.nanoTimestamp();

    printTiming("    Computation time", @intCast(cent_end - cent_start));

    // Show top 5 central nodes
    const top5 = try topKByCentrality(allocator, &centrality, 5);
    defer allocator.free(top5);
    std.debug.print("    Top 5 most central nodes:\n", .{});
    for (top5, 0..) |item, i| {
        std.debug.print("      {d}. Node {d}: {d:.4}\n", .{ i + 1, item.id, item.score });
    }

    // Test Clustering Coefficient (sample for large graphs)
    std.debug.print("\n  Clustering Coefficient:\n", .{});
    const sample_size = @min(node_count, 100);
    const cc_start = std.time.nanoTimestamp();

    var total_cc: f64 = 0;
    for (0..sample_size) |i| {
        const cc = localClusteringCoefficient(db, node_ids[i]) catch 0;
        total_cc += cc;
    }
    const avg_cc = total_cc / @as(f64, @floatFromInt(sample_size));

    const cc_end = std.time.nanoTimestamp();
    printTiming("    Sample computation time", @intCast(cc_end - cc_start));
    std.debug.print("    Average clustering coefficient: {d:.4}\n", .{avg_cc});

    // Test Shortest Path (BFS)
    std.debug.print("\n  Shortest Path (BFS):\n", .{});
    const path_start = std.time.nanoTimestamp();

    // Find paths between random pairs
    var paths_found: u32 = 0;
    var total_path_length: u64 = 0;
    const path_tests: u32 = 100;

    for (0..path_tests) |_| {
        const start_idx = rng.intRangeAtMost(usize, 0, node_count - 1);
        const end_idx = rng.intRangeAtMost(usize, 0, node_count - 1);

        if (shortestPath(allocator, db, node_ids[start_idx], node_ids[end_idx], 10) catch null) |dist| {
            paths_found += 1;
            total_path_length += @as(u64, dist);
        }
    }

    const path_end = std.time.nanoTimestamp();
    printTiming("    100 BFS searches", @intCast(path_end - path_start));
    std.debug.print("    Paths found: {d}/{d}\n", .{ paths_found, path_tests });
    if (paths_found > 0) {
        std.debug.print("    Avg path length: {d:.2}\n", .{@as(f64, @floatFromInt(total_path_length)) / @as(f64, @floatFromInt(paths_found))});
    }
}

fn stressTestQueries(allocator: Allocator, node_count: usize, edges_per_node: usize) !void {
    std.debug.print("\n═══ Cypher Query Stress Test ({d} nodes) ═══\n", .{node_count});

    const path = "/tmp/lattice_stress_query.ltdb";
    std.fs.cwd().deleteFile(path) catch {};

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{ .enable_wal = false },
    });
    defer {
        db.close();
        std.fs.cwd().deleteFile(path) catch {};
    }

    // Create graph with properties
    std.debug.print("  Building graph with properties...\n", .{});
    const labels = &[_][]const u8{"Person"};
    var node_ids = try allocator.alloc(u64, node_count);
    defer allocator.free(node_ids);

    for (0..node_count) |i| {
        node_ids[i] = try db.createNode(null, labels);
        try db.setNodeProperty(null, node_ids[i], "age", .{ .int_val = @intCast(i % 100) });
    }

    // Create edges
    const rng = std.crypto.random;
    for (node_ids) |source| {
        for (0..edges_per_node) |_| {
            const target_idx = rng.intRangeAtMost(usize, 0, node_count - 1);
            const target = node_ids[target_idx];
            if (source != target) {
                _ = db.createEdge(null, source, target, "KNOWS") catch {};
            }
        }
    }

    // Test different query patterns
    std.debug.print("\n  Query Performance:\n", .{});

    // Simple node match
    {
        const start = std.time.nanoTimestamp();
        var result = try db.query("MATCH (n:Person) RETURN n LIMIT 100");
        defer result.deinit();
        const end = std.time.nanoTimestamp();
        printTiming("    MATCH (n:Person) LIMIT 100", @intCast(end - start));
        std.debug.print("      Returned {d} rows\n", .{result.rowCount()});
    }

    // Filtered query
    {
        const start = std.time.nanoTimestamp();
        var result = try db.query("MATCH (n:Person) WHERE n.age > 50 RETURN n LIMIT 100");
        defer result.deinit();
        const end = std.time.nanoTimestamp();
        printTiming("    MATCH WHERE age > 50 LIMIT 100", @intCast(end - start));
        std.debug.print("      Returned {d} rows\n", .{result.rowCount()});
    }

    // Single-hop traversal
    {
        const start = std.time.nanoTimestamp();
        var result = try db.query("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a, b LIMIT 100");
        defer result.deinit();
        const end = std.time.nanoTimestamp();
        printTiming("    Single-hop traversal LIMIT 100", @intCast(end - start));
        std.debug.print("      Returned {d} rows\n", .{result.rowCount()});
    }

    // Two-hop traversal
    {
        const start = std.time.nanoTimestamp();
        const query_result = db.query("MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) RETURN a, b, c LIMIT 100");
        const end = std.time.nanoTimestamp();
        if (query_result) |_| {
            var result = query_result catch unreachable;
            defer result.deinit();
            printTiming("    Two-hop traversal LIMIT 100", @intCast(end - start));
            std.debug.print("      Returned {d} rows\n", .{result.rowCount()});
        } else |err| {
            printTiming("    Two-hop traversal LIMIT 100 (FAILED)", @intCast(end - start));
            std.debug.print("      Error: {}\n", .{err});
        }
    }

    // Aggregation
    {
        const start = std.time.nanoTimestamp();
        const query_result = db.query("MATCH (n:Person) RETURN count(n)");
        const end = std.time.nanoTimestamp();
        if (query_result) |_| {
            var result = query_result catch unreachable;
            defer result.deinit();
            printTiming("    COUNT(*) all nodes", @intCast(end - start));
        } else |err| {
            printTiming("    COUNT(*) all nodes (FAILED)", @intCast(end - start));
            std.debug.print("      Error: {}\n", .{err});
        }
    }
}

fn stressTestVectorSearch(allocator: Allocator, vector_count: usize, dimensions: usize) !void {
    std.debug.print("\n═══ Vector Search Stress Test ({d} vectors, {d}D) ═══\n", .{ vector_count, dimensions });

    const path = "/tmp/lattice_stress_vector.ltdb";
    std.fs.cwd().deleteFile(path) catch {};

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{
            .enable_wal = false,
            .enable_vector = true,
            .vector_dimensions = @intCast(dimensions),
        },
    });
    defer {
        db.close();
        std.fs.cwd().deleteFile(path) catch {};
    }

    // Create nodes with vectors
    std.debug.print("  Indexing {d} vectors...\n", .{vector_count});
    const labels = &[_][]const u8{"Vector"};
    var node_ids = try allocator.alloc(u64, vector_count);
    defer allocator.free(node_ids);

    var vector = try allocator.alloc(f32, dimensions);
    defer allocator.free(vector);

    const rng = std.crypto.random;
    const index_start = std.time.nanoTimestamp();

    for (0..vector_count) |i| {
        node_ids[i] = try db.createNode(null, labels);

        // Generate random unit vector
        var norm: f32 = 0;
        for (0..dimensions) |j| {
            vector[j] = @as(f32, @floatFromInt(rng.int(u16))) / 32767.5 - 1.0;
            norm += vector[j] * vector[j];
        }
        norm = @sqrt(norm);
        for (0..dimensions) |j| {
            vector[j] /= norm;
        }

        db.setNodeVector(node_ids[i], vector) catch continue;

        if ((i + 1) % 1000 == 0) {
            std.debug.print("    Indexed {d} vectors...\n", .{i + 1});
        }
    }

    const index_end = std.time.nanoTimestamp();
    printTiming("  Total indexing time", @intCast(index_end - index_start));

    // Create query vector
    var query = try allocator.alloc(f32, dimensions);
    defer allocator.free(query);
    var norm: f32 = 0;
    for (0..dimensions) |j| {
        query[j] = @as(f32, @floatFromInt(rng.int(u16))) / 32767.5 - 1.0;
        norm += query[j] * query[j];
    }
    norm = @sqrt(norm);
    for (0..dimensions) |j| {
        query[j] /= norm;
    }

    // Test search performance
    std.debug.print("\n  Search Performance:\n", .{});

    const search_iterations: usize = 100;
    const search_start = std.time.nanoTimestamp();

    for (0..search_iterations) |_| {
        const results = db.vectorSearch(query, 10, null) catch continue;
        db.freeVectorSearchResults(results);
    }

    const search_end = std.time.nanoTimestamp();
    const avg_search = @as(u64, @intCast(search_end - search_start)) / search_iterations;
    printTiming("    Avg 10-NN search time", avg_search);

    // Check file size
    const file = try std.fs.cwd().openFile(path, .{});
    defer file.close();
    const stat = try file.stat();
    std.debug.print("  Database size: {d:.2} MB\n", .{@as(f64, @floatFromInt(stat.size)) / (1024.0 * 1024.0)});
}

// ============================================================================
// Main
// ============================================================================

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    std.debug.print("\n", .{});
    std.debug.print("╔══════════════════════════════════════════════════════════════════════════════╗\n", .{});
    std.debug.print("║                        LatticeDB Stress Tests                                ║\n", .{});
    std.debug.print("║                     Finding the Limits of Your Graph                         ║\n", .{});
    std.debug.print("╚══════════════════════════════════════════════════════════════════════════════╝\n", .{});

    // Progressively larger tests

    // Small: 10K nodes
    try stressTestNodeCreation(allocator, 10_000);
    try stressTestEdgeCreation(allocator, 1_000, 20);
    try stressTestGraphAlgorithms(allocator, 1_000, 10);
    try stressTestQueries(allocator, 1_000, 10);
    try stressTestVectorSearch(allocator, 1_000, 128);

    // Medium: 100K nodes
    std.debug.print("\n\n▶▶▶ SCALING UP TO 100K NODES ◀◀◀\n", .{});
    try stressTestNodeCreation(allocator, 100_000);
    try stressTestEdgeCreation(allocator, 10_000, 20);
    try stressTestGraphAlgorithms(allocator, 10_000, 10);
    try stressTestQueries(allocator, 10_000, 10);
    try stressTestVectorSearch(allocator, 10_000, 128);

    // Large: 500K nodes (may take a while)
    std.debug.print("\n\n▶▶▶ SCALING UP TO 500K NODES ◀◀◀\n", .{});
    try stressTestNodeCreation(allocator, 500_000);
    try stressTestEdgeCreation(allocator, 50_000, 10);

    std.debug.print("\n", .{});
    std.debug.print("════════════════════════════════════════════════════════════════════════════════\n", .{});
    std.debug.print("Stress tests complete!\n", .{});
    std.debug.print("\n", .{});
}
