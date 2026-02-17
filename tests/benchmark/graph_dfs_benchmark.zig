//! Graph Traversal Benchmark — LatticeDB vs SQLite
//!
//! Benchmarks depth-limited graph reachability at depths 10, 15, 25, and 50,
//! comparing LatticeDB's native BFS traversal against SQLite's recursive CTEs.
//! Both engines compute the same result: count of unique nodes reachable within N hops.
//!
//! Run with: zig build graph-benchmark
//! Quick mode: zig build graph-benchmark -- --quick

const std = @import("std");
const lattice = @import("lattice");
const Database = lattice.storage.database.Database;

// SQLite C bindings
const c = @cImport({
    @cInclude("sqlite3.h");
});

// ============================================================================
// Configuration
// ============================================================================

const Config = struct {
    warmup_iterations: u32 = 5,
    measure_iterations: u32 = 50,
    seed: u64 = 42,
    num_start_nodes: u32 = 20,
};

const Scale = enum {
    small, // 10K nodes, 50K edges
    medium, // 100K nodes, 500K edges

    fn nodeCount(self: Scale) u32 {
        return switch (self) {
            .small => 10_000,
            .medium => 100_000,
        };
    }

    fn edgeMultiplier(_: Scale) u32 {
        return 5;
    }

    fn name(self: Scale) []const u8 {
        return switch (self) {
            .small => "10K nodes, 50K edges",
            .medium => "100K nodes, 500K edges",
        };
    }
};

const depths = [_]u32{ 10, 15, 25, 50 };

// ============================================================================
// Result Types
// ============================================================================

const PercentileResult = struct {
    p50: u64,
    p99: u64,
    mean: u64,

    fn fromSamples(samples: []u64) PercentileResult {
        if (samples.len == 0) return .{ .p50 = 0, .p99 = 0, .mean = 0 };

        std.mem.sort(u64, samples, {}, std.sort.asc(u64));

        var sum: u64 = 0;
        for (samples) |s| sum += s;

        return .{
            .p50 = samples[samples.len / 2],
            .p99 = samples[@min((samples.len * 99) / 100, samples.len - 1)],
            .mean = sum / samples.len,
        };
    }
};

const DfsResult = struct {
    depth: u32,
    lattice_stats: PercentileResult,
    sqlite_stats: PercentileResult,
    avg_nodes_visited: u64,

    fn speedup(self: DfsResult) f64 {
        if (self.lattice_stats.mean == 0) return 0;
        return @as(f64, @floatFromInt(self.sqlite_stats.mean)) / @as(f64, @floatFromInt(self.lattice_stats.mean));
    }
};

// ============================================================================
// Graph Generation (same as sqlite_comparison.zig)
// ============================================================================

const Edge = struct {
    source: u32,
    target: u32,
};

const Graph = struct {
    node_count: u32,
    edges: []Edge,
    allocator: std.mem.Allocator,

    fn deinit(self: *Graph) void {
        self.allocator.free(self.edges);
    }
};

fn generateSocialNetwork(allocator: std.mem.Allocator, node_count: u32, avg_edges: u32, seed: u64) !Graph {
    var prng = std.Random.DefaultPrng.init(seed);
    const random = prng.random();

    const total_edges = node_count * avg_edges;
    var edges = try allocator.alloc(Edge, total_edges);
    var edge_count: usize = 0;

    var degree = try allocator.alloc(u32, node_count);
    defer allocator.free(degree);
    @memset(degree, 1);

    var total_degree: u64 = node_count;

    for (0..total_edges) |_| {
        const source = random.intRangeAtMost(u32, 0, node_count - 1);

        const target_roll = random.intRangeLessThan(u64, 0, total_degree);
        var target: u32 = 0;
        var cumulative: u64 = 0;
        for (degree, 0..) |d, i| {
            cumulative += d;
            if (cumulative > target_roll) {
                target = @intCast(i);
                break;
            }
        }

        if (source != target and edge_count < edges.len) {
            edges[edge_count] = .{ .source = source, .target = target };
            edge_count += 1;
            degree[target] += 1;
            total_degree += 1;
        }
    }

    return .{
        .node_count = node_count,
        .edges = edges[0..edge_count],
        .allocator = allocator,
    };
}

// ============================================================================
// SQLite Database
// ============================================================================

const SqliteDb = struct {
    db: *c.sqlite3,

    fn open(path: [:0]const u8) !SqliteDb {
        var db: ?*c.sqlite3 = null;
        const rc = c.sqlite3_open(path.ptr, &db);
        if (rc != c.SQLITE_OK) {
            if (db) |d| _ = c.sqlite3_close(d);
            return error.SqliteOpenFailed;
        }

        var self = SqliteDb{ .db = db.? };

        try self.exec("CREATE TABLE IF NOT EXISTS nodes (id INTEGER PRIMARY KEY);");
        try self.exec(
            \\CREATE TABLE IF NOT EXISTS edges (
            \\    source_id INTEGER NOT NULL,
            \\    target_id INTEGER NOT NULL
            \\);
        );
        try self.exec("CREATE INDEX IF NOT EXISTS idx_edges_source ON edges(source_id);");

        return self;
    }

    fn close(self: *SqliteDb) void {
        _ = c.sqlite3_close(self.db);
    }

    fn exec(self: *SqliteDb, sql: [:0]const u8) !void {
        var err_msg: [*c]u8 = null;
        const rc = c.sqlite3_exec(self.db, sql.ptr, null, null, &err_msg);
        if (rc != c.SQLITE_OK) {
            if (err_msg) |msg| {
                std.debug.print("SQLite error: {s}\n", .{msg});
                c.sqlite3_free(msg);
            }
            return error.SqliteExecFailed;
        }
    }

    fn populateGraph(self: *SqliteDb, graph: *const Graph) !void {
        try self.exec("BEGIN TRANSACTION;");
        errdefer self.exec("ROLLBACK;") catch {};

        var insert_node_stmt: ?*c.sqlite3_stmt = null;
        _ = c.sqlite3_prepare_v2(self.db, "INSERT INTO nodes (id) VALUES (?);", -1, &insert_node_stmt, null);
        defer _ = c.sqlite3_finalize(insert_node_stmt);

        if (insert_node_stmt) |stmt| {
            for (0..graph.node_count) |i| {
                _ = c.sqlite3_reset(stmt);
                _ = c.sqlite3_bind_int64(stmt, 1, @intCast(i));
                _ = c.sqlite3_step(stmt);
            }
        }

        var insert_edge_stmt: ?*c.sqlite3_stmt = null;
        _ = c.sqlite3_prepare_v2(self.db, "INSERT INTO edges (source_id, target_id) VALUES (?, ?);", -1, &insert_edge_stmt, null);
        defer _ = c.sqlite3_finalize(insert_edge_stmt);

        if (insert_edge_stmt) |stmt| {
            for (graph.edges) |edge| {
                _ = c.sqlite3_reset(stmt);
                _ = c.sqlite3_bind_int64(stmt, 1, edge.source);
                _ = c.sqlite3_bind_int64(stmt, 2, edge.target);
                _ = c.sqlite3_step(stmt);
            }
        }

        try self.exec("COMMIT;");
        try self.exec("ANALYZE;");
    }

    /// Prepare a DFS statement for a given max depth. Must be finalized by caller.
    fn prepareDfsStmt(self: *SqliteDb, max_depth: u32) ?*c.sqlite3_stmt {
        _ = max_depth;
        // UNION deduplicates on (node_id, depth), so we SELECT DISTINCT node_id
        // to count unique reachable nodes — matching LatticeDB's visited-set semantics.
        const sql: [:0]const u8 =
            \\WITH RECURSIVE reach(node_id, depth) AS (
            \\    SELECT ?, 0
            \\    UNION
            \\    SELECT e.target_id, reach.depth + 1
            \\    FROM reach
            \\    JOIN edges e ON e.source_id = reach.node_id
            \\    WHERE reach.depth < ?
            \\)
            \\SELECT COUNT(DISTINCT node_id) FROM reach;
        ;

        var stmt: ?*c.sqlite3_stmt = null;
        _ = c.sqlite3_prepare_v2(self.db, sql.ptr, -1, &stmt, null);
        return stmt;
    }

    fn runDfs(self: *SqliteDb, stmt: *c.sqlite3_stmt, start_node: u32, max_depth: u32) u64 {
        _ = self;
        _ = c.sqlite3_reset(stmt);
        _ = c.sqlite3_bind_int64(stmt, 1, start_node);
        _ = c.sqlite3_bind_int(stmt, 2, @intCast(max_depth));

        if (c.sqlite3_step(stmt) == c.SQLITE_ROW) {
            return @intCast(c.sqlite3_column_int64(stmt, 0));
        }
        return 0;
    }
};

// ============================================================================
// LatticeDB Database
// ============================================================================

const LatticeDb = struct {
    db: *Database,
    allocator: std.mem.Allocator,
    node_ids: []u64,

    fn open(allocator: std.mem.Allocator, path: []const u8) !LatticeDb {
        std.fs.cwd().deleteFile(path) catch {};

        const db = try Database.open(allocator, path, .{
            .create = true,
            .config = .{
                .enable_wal = false,
                .enable_fts = false,
                .enable_vector = false,
                .enable_adjacency_cache = true,
            },
        });

        return .{
            .db = db,
            .allocator = allocator,
            .node_ids = &[_]u64{},
        };
    }

    fn close(self: *LatticeDb) void {
        if (self.node_ids.len > 0) {
            self.allocator.free(self.node_ids);
        }
        self.db.close();
    }

    fn populateGraph(self: *LatticeDb, graph: *const Graph) !void {
        const labels = &[_][]const u8{"Person"};

        self.node_ids = try self.allocator.alloc(u64, graph.node_count);
        for (0..graph.node_count) |i| {
            self.node_ids[i] = try self.db.createNode(null, labels);
        }

        for (graph.edges) |edge| {
            _ = self.db.createEdge(null, self.node_ids[edge.source], self.node_ids[edge.target], "FOLLOWS") catch {};
        }
    }

    /// BFS up to max_depth hops. Returns count of unique reachable nodes (including start).
    /// Matches SQLite recursive CTE semantics (all nodes within N hops).
    fn runDfs(self: *LatticeDb, node_idx: u32, max_depth: u32, allocator: std.mem.Allocator) u64 {
        const node_id = self.node_ids[@intCast(node_idx)];

        const bitset_size = self.node_ids.len + 1;
        var visited = std.DynamicBitSet.initEmpty(allocator, bitset_size) catch return 0;
        defer visited.deinit();

        var current_level = std.ArrayListUnmanaged(u64){};
        defer current_level.deinit(allocator);

        var next_level = std.ArrayListUnmanaged(u64){};
        defer next_level.deinit(allocator);

        current_level.append(allocator, node_id) catch return 0;
        visited.set(@intCast(node_id));
        var count: u64 = 1;

        var depth: u32 = 0;
        while (depth < max_depth and current_level.items.len > 0) {
            next_level.clearRetainingCapacity();

            for (current_level.items) |current| {
                // Try adjacency cache first
                if (self.db.getOutgoingEdgesCached(current) catch null) |cached_edges| {
                    for (cached_edges) |edge| {
                        const target: usize = @intCast(edge.target);
                        if (!visited.isSet(target)) {
                            visited.set(target);
                            count += 1;
                            next_level.append(allocator, edge.target) catch continue;
                        }
                    }
                } else {
                    var iter = self.db.getOutgoingEdgeRefs(current) catch continue;
                    defer iter.deinit();

                    while (iter.next() catch null) |ref| {
                        const target: usize = @intCast(ref.target);
                        if (!visited.isSet(target)) {
                            visited.set(target);
                            count += 1;
                            next_level.append(allocator, ref.target) catch continue;
                        }
                    }
                }
            }

            std.mem.swap(std.ArrayListUnmanaged(u64), &current_level, &next_level);
            depth += 1;
        }

        return count;
    }
};

// ============================================================================
// Benchmark Runner
// ============================================================================

fn runDfsBenchmark(
    config: Config,
    depth: u32,
    start_nodes: []const u32,
    sqlite_db: *SqliteDb,
    lattice_db: *LatticeDb,
    allocator: std.mem.Allocator,
) !DfsResult {
    const total_iters = config.warmup_iterations + config.measure_iterations;

    var lattice_samples = try allocator.alloc(u64, config.measure_iterations);
    defer allocator.free(lattice_samples);

    var sqlite_samples = try allocator.alloc(u64, config.measure_iterations);
    defer allocator.free(sqlite_samples);

    var total_nodes_visited: u64 = 0;

    // Prepare SQLite statement once
    const sqlite_stmt = sqlite_db.prepareDfsStmt(depth) orelse return error.SqlitePrepareFailed;
    defer _ = c.sqlite3_finalize(sqlite_stmt);

    // Cross-validate: verify both engines agree on reachable node count
    {
        const lattice_count = lattice_db.runDfs(start_nodes[0], depth, allocator);
        const sqlite_count = sqlite_db.runDfs(sqlite_stmt, start_nodes[0], depth);
        if (lattice_count != sqlite_count) {
            std.debug.print("      WARNING: count mismatch depth={d} node={d}: Lattice={d} SQLite={d}\n", .{
                depth, start_nodes[0], lattice_count, sqlite_count,
            });
        }
    }

    // Warmup + measure LatticeDB
    var measure_idx: usize = 0;
    for (0..total_iters) |i| {
        const node = start_nodes[i % start_nodes.len];
        const start = std.time.nanoTimestamp();
        const visited = lattice_db.runDfs(node, depth, allocator);
        const elapsed: u64 = @intCast(std.time.nanoTimestamp() - start);

        if (i >= config.warmup_iterations) {
            lattice_samples[measure_idx] = elapsed;
            total_nodes_visited += visited;
            measure_idx += 1;
        }
    }

    // Warmup + measure SQLite
    measure_idx = 0;
    for (0..total_iters) |i| {
        const node = start_nodes[i % start_nodes.len];
        const start = std.time.nanoTimestamp();
        _ = sqlite_db.runDfs(sqlite_stmt, node, depth);
        const elapsed: u64 = @intCast(std.time.nanoTimestamp() - start);

        if (i >= config.warmup_iterations) {
            sqlite_samples[measure_idx] = elapsed;
            measure_idx += 1;
        }
    }

    return .{
        .depth = depth,
        .lattice_stats = PercentileResult.fromSamples(lattice_samples),
        .sqlite_stats = PercentileResult.fromSamples(sqlite_samples),
        .avg_nodes_visited = total_nodes_visited / config.measure_iterations,
    };
}

// ============================================================================
// Output Formatting
// ============================================================================

fn formatDuration(ns: u64) []const u8 {
    const S = struct {
        var bufs: [8][32]u8 = undefined;
        var idx: usize = 0;
    };

    const buf = &S.bufs[S.idx];
    S.idx = (S.idx + 1) % 8;

    if (ns < 1_000) {
        return std.fmt.bufPrint(buf, "{d} ns", .{ns}) catch "?";
    } else if (ns < 1_000_000) {
        const us = @as(f64, @floatFromInt(ns)) / 1000.0;
        return std.fmt.bufPrint(buf, "{d:.0} us", .{us}) catch "?";
    } else if (ns < 1_000_000_000) {
        const ms = @as(f64, @floatFromInt(ns)) / 1_000_000.0;
        return std.fmt.bufPrint(buf, "{d:.1} ms", .{ms}) catch "?";
    } else {
        const s = @as(f64, @floatFromInt(ns)) / 1_000_000_000.0;
        return std.fmt.bufPrint(buf, "{d:.1} s", .{s}) catch "?";
    }
}

fn printResults(scale: Scale, results: []const DfsResult) void {
    std.debug.print("\n", .{});
    std.debug.print("=== Graph Traversal Benchmark ({s}) ===\n", .{scale.name()});
    std.debug.print("\n", .{});
    std.debug.print("Depth | Lattice (mean) | Lattice (p99) | SQLite (mean) | SQLite (p99) | Speedup | Avg Nodes\n", .{});
    std.debug.print("------+----------------+---------------+---------------+--------------+---------+----------\n", .{});

    for (results) |r| {
        const speedup = r.speedup();
        std.debug.print("{d:>5} | {s:>14} | {s:>13} | {s:>13} | {s:>12} | {d:>5.1}x  | {d:>8}\n", .{
            r.depth,
            formatDuration(r.lattice_stats.mean),
            formatDuration(r.lattice_stats.p99),
            formatDuration(r.sqlite_stats.mean),
            formatDuration(r.sqlite_stats.p99),
            speedup,
            r.avg_nodes_visited,
        });
    }

    std.debug.print("\n", .{});
}

// ============================================================================
// Main
// ============================================================================

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Parse CLI args
    const argv = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, argv);

    var quick = false;
    for (argv[1..]) |arg| {
        if (std.mem.eql(u8, arg, "--quick")) {
            quick = true;
        }
    }

    const scale: Scale = if (quick) .small else .medium;
    const config = Config{};

    std.debug.print("\n", .{});
    std.debug.print("===========================================================================\n", .{});
    std.debug.print("          Graph Traversal Benchmark — LatticeDB vs SQLite\n", .{});
    std.debug.print("===========================================================================\n", .{});
    std.debug.print("\n", .{});

    const node_count = scale.nodeCount();
    const edge_count = node_count * scale.edgeMultiplier();

    std.debug.print("  Scale: {s}\n", .{scale.name()});
    std.debug.print("  Depths: 10, 15, 25, 50\n", .{});
    std.debug.print("  Start nodes: {d}, Warmup: {d}, Measure: {d}\n", .{ config.num_start_nodes, config.warmup_iterations, config.measure_iterations });
    std.debug.print("\n", .{});

    // Generate graph
    std.debug.print("  Generating graph: {d} nodes, ~{d} edges...\n", .{ node_count, edge_count });
    var graph = try generateSocialNetwork(allocator, node_count, scale.edgeMultiplier(), config.seed);
    defer graph.deinit();
    std.debug.print("  Generated {d} edges\n", .{graph.edges.len});

    // Setup SQLite
    std.debug.print("  Setting up SQLite...\n", .{});
    const sqlite_path: [:0]const u8 = "/tmp/bench_dfs_sqlite.db";
    std.fs.cwd().deleteFile(sqlite_path) catch {};

    var sqlite_db = try SqliteDb.open(sqlite_path);
    defer {
        sqlite_db.close();
        std.fs.cwd().deleteFile(sqlite_path) catch {};
    }

    try sqlite_db.populateGraph(&graph);

    // Setup LatticeDB
    std.debug.print("  Setting up LatticeDB...\n", .{});
    const lattice_path = "/tmp/bench_dfs_lattice.ltdb";

    var lattice_db = try LatticeDb.open(allocator, lattice_path);
    defer {
        lattice_db.close();
        std.fs.cwd().deleteFile(lattice_path) catch {};
    }

    try lattice_db.populateGraph(&graph);

    // Warm adjacency cache
    std.debug.print("  Warming adjacency cache...\n", .{});
    lattice_db.db.warmAdjacencyCache(lattice_db.node_ids);

    // Generate deterministic start nodes
    var prng = std.Random.DefaultPrng.init(config.seed + 100);
    const random = prng.random();

    var start_nodes: [20]u32 = undefined;
    for (&start_nodes) |*node| {
        node.* = random.intRangeAtMost(u32, 0, node_count - 1);
    }

    // Run benchmarks at each depth
    std.debug.print("  Running traversal benchmarks...\n", .{});

    var results: [depths.len]DfsResult = undefined;
    for (depths, 0..) |depth, i| {
        std.debug.print("    Depth {d}...\n", .{depth});
        results[i] = try runDfsBenchmark(config, depth, &start_nodes, &sqlite_db, &lattice_db, allocator);
    }

    printResults(scale, &results);

    std.debug.print("Benchmark complete.\n", .{});
}
