//! SQLite vs LatticeDB Graph Traversal Benchmark
//!
//! Compares LatticeDB's native graph traversal against SQLite's SQL-based
//! approach using JOINs and Recursive CTEs.
//!
//! Run with: zig build sqlite-benchmark

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
    warmup_iterations: u32 = 10,
    measure_iterations: u32 = 100,
    /// Random seed for reproducible graph generation
    seed: u64 = 42,
};

const Scale = enum {
    small, // 10K nodes, 50K edges
    medium, // 100K nodes, 500K edges
    large, // 1M nodes, 5M edges

    fn nodeCount(self: Scale) u32 {
        return switch (self) {
            .small => 10_000,
            .medium => 100_000,
            .large => 1_000_000,
        };
    }

    fn edgeMultiplier(_: Scale) u32 {
        return 5; // avg 5 edges per node
    }

    fn name(self: Scale) []const u8 {
        return switch (self) {
            .small => "Small (10K nodes)",
            .medium => "Medium (100K nodes)",
            .large => "Large (1M nodes)",
        };
    }
};

// ============================================================================
// Benchmark Result Types
// ============================================================================

const BenchmarkResult = struct {
    name: []const u8,
    lattice_ns: u64,
    sqlite_ns: u64,
    iterations: u32,

    fn speedup(self: BenchmarkResult) f64 {
        if (self.lattice_ns == 0) return 0;
        return @as(f64, @floatFromInt(self.sqlite_ns)) / @as(f64, @floatFromInt(self.lattice_ns));
    }

    fn latticeUs(self: BenchmarkResult) f64 {
        return @as(f64, @floatFromInt(self.lattice_ns)) / 1000.0;
    }

    fn sqliteUs(self: BenchmarkResult) f64 {
        return @as(f64, @floatFromInt(self.sqlite_ns)) / 1000.0;
    }
};

const PercentileResult = struct {
    p50: u64,
    p95: u64,
    p99: u64,
    mean: u64,

    fn fromSamples(samples: []u64) PercentileResult {
        if (samples.len == 0) return .{ .p50 = 0, .p95 = 0, .p99 = 0, .mean = 0 };

        std.mem.sort(u64, samples, {}, std.sort.asc(u64));

        var sum: u64 = 0;
        for (samples) |s| sum += s;

        const p50_idx = samples.len / 2;
        const p95_idx = (samples.len * 95) / 100;
        const p99_idx = (samples.len * 99) / 100;

        return .{
            .p50 = samples[p50_idx],
            .p95 = samples[@min(p95_idx, samples.len - 1)],
            .p99 = samples[@min(p99_idx, samples.len - 1)],
            .mean = sum / samples.len,
        };
    }
};

const ScaleResults = struct {
    scale: Scale,
    results: []const BenchmarkResult,
};

// ============================================================================
// Graph Generation
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

/// Generate a social network graph with power-law degree distribution
fn generateSocialNetwork(allocator: std.mem.Allocator, node_count: u32, avg_edges: u32, seed: u64) !Graph {
    var prng = std.Random.DefaultPrng.init(seed);
    const random = prng.random();

    const total_edges = node_count * avg_edges;
    var edges = try allocator.alloc(Edge, total_edges);
    var edge_count: usize = 0;

    // Power-law: some nodes have many more connections
    // Use preferential attachment: nodes with more edges are more likely to get new edges
    var degree = try allocator.alloc(u32, node_count);
    defer allocator.free(degree);
    @memset(degree, 1); // Start with degree 1 to avoid division by zero

    var total_degree: u64 = node_count;

    for (0..total_edges) |_| {
        // Pick source uniformly
        const source = random.intRangeAtMost(u32, 0, node_count - 1);

        // Pick target with preferential attachment (higher degree = more likely)
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
    allocator: std.mem.Allocator,

    // Prepared statements for workloads
    stmt_1hop: ?*c.sqlite3_stmt = null,
    stmt_2hop: ?*c.sqlite3_stmt = null,
    stmt_3hop: ?*c.sqlite3_stmt = null,
    stmt_var_path: ?*c.sqlite3_stmt = null,

    fn open(allocator: std.mem.Allocator, path: [:0]const u8) !SqliteDb {
        var db: ?*c.sqlite3 = null;
        const rc = c.sqlite3_open(path.ptr, &db);
        if (rc != c.SQLITE_OK) {
            if (db) |d| _ = c.sqlite3_close(d);
            return error.SqliteOpenFailed;
        }

        var self = SqliteDb{
            .db = db.?,
            .allocator = allocator,
        };

        // Create schema
        try self.exec(
            \\CREATE TABLE IF NOT EXISTS nodes (
            \\    id INTEGER PRIMARY KEY,
            \\    label TEXT,
            \\    followers INTEGER DEFAULT 0
            \\);
        );

        try self.exec(
            \\CREATE TABLE IF NOT EXISTS edges (
            \\    id INTEGER PRIMARY KEY AUTOINCREMENT,
            \\    source_id INTEGER NOT NULL,
            \\    target_id INTEGER NOT NULL,
            \\    type TEXT NOT NULL,
            \\    FOREIGN KEY (source_id) REFERENCES nodes(id),
            \\    FOREIGN KEY (target_id) REFERENCES nodes(id)
            \\);
        );

        // Create indexes for traversal performance
        try self.exec("CREATE INDEX IF NOT EXISTS idx_edges_source ON edges(source_id, type);");
        try self.exec("CREATE INDEX IF NOT EXISTS idx_edges_target ON edges(target_id, type);");

        return self;
    }

    fn close(self: *SqliteDb) void {
        if (self.stmt_1hop) |stmt| _ = c.sqlite3_finalize(stmt);
        if (self.stmt_2hop) |stmt| _ = c.sqlite3_finalize(stmt);
        if (self.stmt_3hop) |stmt| _ = c.sqlite3_finalize(stmt);
        if (self.stmt_var_path) |stmt| _ = c.sqlite3_finalize(stmt);
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
        // Use transaction for faster inserts
        try self.exec("BEGIN TRANSACTION;");
        errdefer self.exec("ROLLBACK;") catch {};

        // Insert nodes
        var insert_node_stmt: ?*c.sqlite3_stmt = null;
        _ = c.sqlite3_prepare_v2(
            self.db,
            "INSERT INTO nodes (id, label, followers) VALUES (?, 'Person', ?);",
            -1,
            &insert_node_stmt,
            null,
        );
        defer _ = c.sqlite3_finalize(insert_node_stmt);

        if (insert_node_stmt) |stmt| {
            for (0..graph.node_count) |i| {
                _ = c.sqlite3_reset(stmt);
                _ = c.sqlite3_bind_int64(stmt, 1, @intCast(i));
                _ = c.sqlite3_bind_int(stmt, 2, @intCast(i % 1000));
                _ = c.sqlite3_step(stmt);
            }
        }

        // Insert edges
        var insert_edge_stmt: ?*c.sqlite3_stmt = null;
        _ = c.sqlite3_prepare_v2(
            self.db,
            "INSERT INTO edges (source_id, target_id, type) VALUES (?, ?, 'FOLLOWS');",
            -1,
            &insert_edge_stmt,
            null,
        );
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

        // Analyze for query optimizer
        try self.exec("ANALYZE;");
    }

    fn prepareStatements(self: *SqliteDb) !void {
        // 1-hop traversal
        _ = c.sqlite3_prepare_v2(
            self.db,
            "SELECT n2.id FROM edges e " ++
                "JOIN nodes n2 ON e.target_id = n2.id " ++
                "WHERE e.source_id = ? AND e.type = 'FOLLOWS';",
            -1,
            &self.stmt_1hop,
            null,
        );

        // 2-hop traversal
        _ = c.sqlite3_prepare_v2(
            self.db,
            "SELECT DISTINCT n3.id FROM edges e1 " ++
                "JOIN edges e2 ON e2.source_id = e1.target_id " ++
                "JOIN nodes n3 ON e2.target_id = n3.id " ++
                "WHERE e1.source_id = ? AND e1.type = 'FOLLOWS' AND e2.type = 'FOLLOWS';",
            -1,
            &self.stmt_2hop,
            null,
        );

        // 3-hop traversal
        _ = c.sqlite3_prepare_v2(
            self.db,
            "SELECT DISTINCT n4.id FROM edges e1 " ++
                "JOIN edges e2 ON e2.source_id = e1.target_id " ++
                "JOIN edges e3 ON e3.source_id = e2.target_id " ++
                "JOIN nodes n4 ON e3.target_id = n4.id " ++
                "WHERE e1.source_id = ? AND e1.type = 'FOLLOWS' " ++
                "AND e2.type = 'FOLLOWS' AND e3.type = 'FOLLOWS';",
            -1,
            &self.stmt_3hop,
            null,
        );

        // Variable-length path (1..5 hops) using recursive CTE
        _ = c.sqlite3_prepare_v2(
            self.db,
            "WITH RECURSIVE paths(node_id, depth) AS (" ++
                "  SELECT target_id, 1 FROM edges WHERE source_id = ? AND type = 'FOLLOWS'" ++
                "  UNION" ++
                "  SELECT e.target_id, p.depth + 1" ++
                "  FROM paths p" ++
                "  JOIN edges e ON e.source_id = p.node_id" ++
                "  WHERE p.depth < 5 AND e.type = 'FOLLOWS'" ++
                ") SELECT DISTINCT node_id FROM paths;",
            -1,
            &self.stmt_var_path,
            null,
        );
    }

    fn run1Hop(self: *SqliteDb, start_node: u32) u32 {
        if (self.stmt_1hop) |stmt| {
            _ = c.sqlite3_reset(stmt);
            _ = c.sqlite3_bind_int64(stmt, 1, start_node);

            var count: u32 = 0;
            while (c.sqlite3_step(stmt) == c.SQLITE_ROW) {
                count += 1;
            }
            return count;
        }
        return 0;
    }

    fn run2Hop(self: *SqliteDb, start_node: u32) u32 {
        if (self.stmt_2hop) |stmt| {
            _ = c.sqlite3_reset(stmt);
            _ = c.sqlite3_bind_int64(stmt, 1, start_node);

            var count: u32 = 0;
            while (c.sqlite3_step(stmt) == c.SQLITE_ROW) {
                count += 1;
            }
            return count;
        }
        return 0;
    }

    fn run3Hop(self: *SqliteDb, start_node: u32) u32 {
        if (self.stmt_3hop) |stmt| {
            _ = c.sqlite3_reset(stmt);
            _ = c.sqlite3_bind_int64(stmt, 1, start_node);

            var count: u32 = 0;
            while (c.sqlite3_step(stmt) == c.SQLITE_ROW) {
                count += 1;
            }
            return count;
        }
        return 0;
    }

    fn runVarPath(self: *SqliteDb, start_node: u32) u32 {
        if (self.stmt_var_path) |stmt| {
            _ = c.sqlite3_reset(stmt);
            _ = c.sqlite3_bind_int64(stmt, 1, start_node);

            var count: u32 = 0;
            while (c.sqlite3_step(stmt) == c.SQLITE_ROW) {
                count += 1;
            }
            return count;
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
        // Remove existing file
        std.fs.cwd().deleteFile(path) catch {};

        const db = try Database.open(allocator, path, .{
            .create = true,
            .config = .{
                .enable_wal = false,
                .enable_fts = false,
                .enable_vector = false,
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

        // Create nodes
        self.node_ids = try self.allocator.alloc(u64, graph.node_count);
        for (0..graph.node_count) |i| {
            self.node_ids[i] = try self.db.createNode(null, labels);
        }

        // Create edges
        for (graph.edges) |edge| {
            _ = self.db.createEdge(
                null,
                self.node_ids[edge.source],
                self.node_ids[edge.target],
                "FOLLOWS",
            ) catch {};
        }
    }

    fn run1Hop(self: *LatticeDb, node_idx: u32) u32 {
        const node_id = self.node_ids[@intCast(node_idx)];
        var iter = self.db.getOutgoingEdgeRefs(node_id) catch return 0;
        defer iter.deinit();

        var count: u32 = 0;
        while (iter.next() catch null) |_| {
            count += 1;
        }
        return count;
    }

    fn run2Hop(self: *LatticeDb, node_idx: u32) u32 {
        const node_id = self.node_ids[@intCast(node_idx)];
        var count: u32 = 0;

        // First hop
        var iter1 = self.db.getOutgoingEdgeRefs(node_id) catch return 0;
        defer iter1.deinit();

        while (iter1.next() catch null) |e1| {
            // Second hop
            var iter2 = self.db.getOutgoingEdgeRefs(e1.target) catch continue;
            defer iter2.deinit();

            while (iter2.next() catch null) |_| {
                count += 1;
            }
        }

        return count;
    }

    fn run3Hop(self: *LatticeDb, node_idx: u32) u32 {
        const node_id = self.node_ids[@intCast(node_idx)];
        var count: u32 = 0;

        // First hop
        var iter1 = self.db.getOutgoingEdgeRefs(node_id) catch return 0;
        defer iter1.deinit();

        while (iter1.next() catch null) |e1| {
            // Second hop
            var iter2 = self.db.getOutgoingEdgeRefs(e1.target) catch continue;
            defer iter2.deinit();

            while (iter2.next() catch null) |e2| {
                // Third hop
                var iter3 = self.db.getOutgoingEdgeRefs(e2.target) catch continue;
                defer iter3.deinit();

                while (iter3.next() catch null) |_| {
                    count += 1;
                }
            }
        }

        return count;
    }

    fn runVarPath(self: *LatticeDb, node_idx: u32, allocator: std.mem.Allocator) u32 {
        const node_id = self.node_ids[@intCast(node_idx)];

        // BFS up to depth 5 - use DynamicBitSet for visited tracking
        // Node IDs are 1-based and sequential, so max_node_id = node_ids.len
        const bitset_size = self.node_ids.len + 1;
        var visited = std.DynamicBitSet.initEmpty(allocator, bitset_size) catch return 0;
        defer visited.deinit();

        var current_level = std.ArrayListUnmanaged(u64){};
        defer current_level.deinit(allocator);

        var next_level = std.ArrayListUnmanaged(u64){};
        defer next_level.deinit(allocator);

        current_level.append(allocator, node_id) catch return 0;
        visited.set(@intCast(node_id));
        var visited_count: u32 = 1; // Track count manually for O(1) access

        var depth: u32 = 0;
        while (depth < 5 and current_level.items.len > 0) {
            next_level.clearRetainingCapacity();

            for (current_level.items) |current| {
                var iter = self.db.getOutgoingEdgeRefs(current) catch continue;
                defer iter.deinit();

                while (iter.next() catch null) |edge_ref| {
                    const target: usize = @intCast(edge_ref.target);
                    if (!visited.isSet(target)) {
                        visited.set(target);
                        visited_count += 1;
                        next_level.append(allocator, edge_ref.target) catch continue;
                    }
                }
            }

            std.mem.swap(std.ArrayListUnmanaged(u64), &current_level, &next_level);
            depth += 1;
        }

        // Subtract 1 for the starting node
        return if (visited_count > 0) visited_count - 1 else 0;
    }

    /// Instrumented version of runVarPath for profiling bottlenecks
    fn runVarPathInstrumented(self: *LatticeDb, node_idx: u32, allocator: std.mem.Allocator) struct { count: u32, timings: VarPathTimings } {
        const node_id = self.node_ids[@intCast(node_idx)];

        var timings = VarPathTimings{};
        var timer = std.time.Timer.start() catch return .{ .count = 0, .timings = timings };

        // Initialization
        const bitset_size = self.node_ids.len + 1;
        var visited = std.DynamicBitSet.initEmpty(allocator, bitset_size) catch return .{ .count = 0, .timings = timings };
        defer visited.deinit();

        var current_level = std.ArrayListUnmanaged(u64){};
        defer current_level.deinit(allocator);

        var next_level = std.ArrayListUnmanaged(u64){};
        defer next_level.deinit(allocator);

        current_level.append(allocator, node_id) catch return .{ .count = 0, .timings = timings };
        visited.set(@intCast(node_id));
        var visited_count: u32 = 1;

        timings.init_ns = timer.lap();

        var depth: u32 = 0;
        while (depth < 5 and current_level.items.len > 0) {
            next_level.clearRetainingCapacity();

            for (current_level.items) |current| {
                // Time: getOutgoingEdgeRefs
                _ = timer.lap();
                var iter = self.db.getOutgoingEdgeRefs(current) catch continue;
                timings.get_edges_ns += timer.lap();
                defer iter.deinit();

                // Time: iteration
                while (true) {
                    _ = timer.lap();
                    const edge_ref = iter.next() catch null;
                    timings.iter_next_ns += timer.lap();

                    if (edge_ref == null) break;

                    const target: usize = @intCast(edge_ref.?.target);

                    // Time: bitset check
                    _ = timer.lap();
                    const already_visited = visited.isSet(target);
                    timings.bitset_check_ns += timer.lap();

                    if (!already_visited) {
                        // Time: bitset set
                        _ = timer.lap();
                        visited.set(target);
                        timings.bitset_set_ns += timer.lap();

                        visited_count += 1;

                        // Time: append
                        _ = timer.lap();
                        next_level.append(allocator, edge_ref.?.target) catch continue;
                        timings.append_ns += timer.lap();
                    }
                }
                timings.iter_deinit_count += 1;
            }

            std.mem.swap(std.ArrayListUnmanaged(u64), &current_level, &next_level);
            depth += 1;
        }

        timings.visited_count = visited_count;
        return .{ .count = if (visited_count > 0) visited_count - 1 else 0, .timings = timings };
    }

    const VarPathTimings = struct {
        init_ns: u64 = 0,
        get_edges_ns: u64 = 0,
        iter_next_ns: u64 = 0,
        bitset_check_ns: u64 = 0,
        bitset_set_ns: u64 = 0,
        append_ns: u64 = 0,
        iter_deinit_count: u64 = 0,
        visited_count: u32 = 0,

        fn add(self: *VarPathTimings, other: VarPathTimings) void {
            self.init_ns += other.init_ns;
            self.get_edges_ns += other.get_edges_ns;
            self.iter_next_ns += other.iter_next_ns;
            self.bitset_check_ns += other.bitset_check_ns;
            self.bitset_set_ns += other.bitset_set_ns;
            self.append_ns += other.append_ns;
            self.iter_deinit_count += other.iter_deinit_count;
            self.visited_count += other.visited_count;
        }

        fn print(self: VarPathTimings, iterations: u64) void {
            const total = self.init_ns + self.get_edges_ns + self.iter_next_ns +
                self.bitset_check_ns + self.bitset_set_ns + self.append_ns;

            std.debug.print("\n  === VarPath Timing Breakdown ({} iterations, avg {} visited) ===\n", .{ iterations, self.visited_count / @as(u32, @intCast(iterations)) });
            std.debug.print("  | Operation          |    Time    |   %   |\n", .{});
            std.debug.print("  |--------------------+------------+-------|\n", .{});
            printRow("Initialization", self.init_ns, total);
            printRow("getOutgoingEdgeRefs", self.get_edges_ns, total);
            printRow("iter.next()", self.iter_next_ns, total);
            printRow("bitset.isSet()", self.bitset_check_ns, total);
            printRow("bitset.set()", self.bitset_set_ns, total);
            printRow("next_level.append", self.append_ns, total);
            std.debug.print("  |--------------------+------------+-------|\n", .{});
            std.debug.print("  | TOTAL              | {:>7.2} ms | 100%  |\n", .{@as(f64, @floatFromInt(total)) / 1_000_000.0});
            std.debug.print("\n", .{});
        }

        fn printRow(name: []const u8, ns: u64, total: u64) void {
            const pct = if (total > 0) @as(f64, @floatFromInt(ns)) / @as(f64, @floatFromInt(total)) * 100.0 else 0.0;
            const ms = @as(f64, @floatFromInt(ns)) / 1_000_000.0;
            std.debug.print("  | {s:<18} | {:>7.2} ms | {:>4.1}% |\n", .{ name, ms, pct });
        }
    };
};

// ============================================================================
// Benchmark Runner
// ============================================================================

fn runWorkload(
    comptime name: []const u8,
    config: Config,
    test_nodes: []const u32,
    sqlite_db: *SqliteDb,
    lattice_db: *LatticeDb,
    allocator: std.mem.Allocator,
    comptime sqliteFn: fn (*SqliteDb, u32) u32,
    comptime latticeFn: fn (*LatticeDb, u32, std.mem.Allocator) u32,
) BenchmarkResult {
    const total_iters = config.warmup_iterations + config.measure_iterations;

    // Warmup + measure SQLite
    var sqlite_total: u64 = 0;
    for (0..total_iters) |i| {
        const node = test_nodes[i % test_nodes.len];
        const start = std.time.nanoTimestamp();
        _ = sqliteFn(sqlite_db, node);
        const elapsed = @as(u64, @intCast(std.time.nanoTimestamp() - start));

        if (i >= config.warmup_iterations) {
            sqlite_total += elapsed;
        }
    }

    // Warmup + measure LatticeDB
    var lattice_total: u64 = 0;
    for (0..total_iters) |i| {
        const node = test_nodes[i % test_nodes.len];
        const start = std.time.nanoTimestamp();
        _ = latticeFn(lattice_db, node, allocator);
        const elapsed = @as(u64, @intCast(std.time.nanoTimestamp() - start));

        if (i >= config.warmup_iterations) {
            lattice_total += elapsed;
        }
    }

    return .{
        .name = name,
        .lattice_ns = lattice_total / config.measure_iterations,
        .sqlite_ns = sqlite_total / config.measure_iterations,
        .iterations = config.measure_iterations,
    };
}

// Wrapper functions to match function signatures
fn sqlite1HopWrapper(db: *SqliteDb, node: u32) u32 {
    return db.run1Hop(node);
}

fn sqlite2HopWrapper(db: *SqliteDb, node: u32) u32 {
    return db.run2Hop(node);
}

fn sqlite3HopWrapper(db: *SqliteDb, node: u32) u32 {
    return db.run3Hop(node);
}

fn sqliteVarPathWrapper(db: *SqliteDb, node: u32) u32 {
    return db.runVarPath(node);
}

fn lattice1HopWrapper(db: *LatticeDb, node: u32, _: std.mem.Allocator) u32 {
    return db.run1Hop(node);
}

fn lattice2HopWrapper(db: *LatticeDb, node: u32, _: std.mem.Allocator) u32 {
    return db.run2Hop(node);
}

fn lattice3HopWrapper(db: *LatticeDb, node: u32, _: std.mem.Allocator) u32 {
    return db.run3Hop(node);
}

fn latticeVarPathWrapper(db: *LatticeDb, node: u32, allocator: std.mem.Allocator) u32 {
    return db.runVarPath(node, allocator);
}

fn runVarPathProfiling(lattice_db: *LatticeDb, test_nodes: []const u32, allocator: std.mem.Allocator, config: Config) void {
    std.debug.print("\n  Running VarPath profiling...\n", .{});

    var total_timings = LatticeDb.VarPathTimings{};
    const total_iters = config.warmup_iterations + config.measure_iterations;

    for (0..total_iters) |i| {
        const node = test_nodes[i % test_nodes.len];
        const result = lattice_db.runVarPathInstrumented(node, allocator);

        // Only accumulate after warmup
        if (i >= config.warmup_iterations) {
            total_timings.add(result.timings);
        }
    }

    total_timings.print(config.measure_iterations);
}

fn runBenchmarkSuite(
    allocator: std.mem.Allocator,
    scale: Scale,
    config: Config,
) ![]BenchmarkResult {
    const node_count = scale.nodeCount();
    const edge_count = node_count * scale.edgeMultiplier();

    std.debug.print("\n  Generating graph: {d} nodes, ~{d} edges...\n", .{ node_count, edge_count });

    // Generate graph
    var graph = try generateSocialNetwork(allocator, node_count, scale.edgeMultiplier(), config.seed);
    defer graph.deinit();

    std.debug.print("  Generated {d} edges\n", .{graph.edges.len});

    // Setup SQLite
    std.debug.print("  Setting up SQLite...\n", .{});
    const sqlite_path: [:0]const u8 = "/tmp/bench_sqlite.db";
    std.fs.cwd().deleteFile(sqlite_path) catch {};

    var sqlite_db = try SqliteDb.open(allocator, sqlite_path);
    defer {
        sqlite_db.close();
        std.fs.cwd().deleteFile(sqlite_path) catch {};
    }

    try sqlite_db.populateGraph(&graph);
    try sqlite_db.prepareStatements();

    // Setup LatticeDB
    std.debug.print("  Setting up LatticeDB...\n", .{});
    const lattice_path = "/tmp/bench_lattice.ltdb";

    var lattice_db = try LatticeDb.open(allocator, lattice_path);
    defer {
        lattice_db.close();
        std.fs.cwd().deleteFile(lattice_path) catch {};
    }

    try lattice_db.populateGraph(&graph);

    // Generate test nodes (sample nodes to query)
    var prng = std.Random.DefaultPrng.init(config.seed + 1);
    const random = prng.random();

    const test_nodes = try allocator.alloc(u32, 100);
    defer allocator.free(test_nodes);

    for (test_nodes) |*node| {
        node.* = random.intRangeAtMost(u32, 0, node_count - 1);
    }

    std.debug.print("  Running workloads...\n", .{});

    // Run benchmarks
    var results = try allocator.alloc(BenchmarkResult, 4);

    results[0] = runWorkload(
        "1-hop traversal",
        config,
        test_nodes,
        &sqlite_db,
        &lattice_db,
        allocator,
        sqlite1HopWrapper,
        lattice1HopWrapper,
    );

    results[1] = runWorkload(
        "2-hop traversal",
        config,
        test_nodes,
        &sqlite_db,
        &lattice_db,
        allocator,
        sqlite2HopWrapper,
        lattice2HopWrapper,
    );

    // Use fewer iterations for expensive workloads
    var reduced_config = config;
    reduced_config.measure_iterations = @max(10, config.measure_iterations / 10);
    reduced_config.warmup_iterations = @max(2, config.warmup_iterations / 5);

    results[2] = runWorkload(
        "3-hop traversal",
        reduced_config,
        test_nodes,
        &sqlite_db,
        &lattice_db,
        allocator,
        sqlite3HopWrapper,
        lattice3HopWrapper,
    );

    results[3] = runWorkload(
        "Variable path (1..5)",
        reduced_config,
        test_nodes,
        &sqlite_db,
        &lattice_db,
        allocator,
        sqliteVarPathWrapper,
        latticeVarPathWrapper,
    );

    // Run instrumented profiling for medium+ scale
    if (scale == .medium) {
        runVarPathProfiling(&lattice_db, test_nodes, allocator, reduced_config);
    }

    return results;
}

// ============================================================================
// Output Formatting
// ============================================================================

fn printResults(scale: Scale, results: []const BenchmarkResult) void {
    std.debug.print("\n", .{});
    std.debug.print("## Graph Traversal Benchmark - {s}\n", .{scale.name()});
    std.debug.print("\n", .{});
    std.debug.print("| Workload | LatticeDB | SQLite | Speedup |\n", .{});
    std.debug.print("|----------|-----------|--------|--------:|\n", .{});

    for (results) |r| {
        const lattice_str = formatDuration(r.lattice_ns);
        const sqlite_str = formatDuration(r.sqlite_ns);
        const speedup = r.speedup();

        if (speedup >= 1.0) {
            std.debug.print("| {s:<22} | {s:<9} | {s:<6} | **{d:.1}x** |\n", .{
                r.name,
                lattice_str,
                sqlite_str,
                speedup,
            });
        } else {
            std.debug.print("| {s:<22} | {s:<9} | {s:<6} | {d:.2}x |\n", .{
                r.name,
                lattice_str,
                sqlite_str,
                speedup,
            });
        }
    }

    std.debug.print("\n", .{});
}

fn formatDuration(ns: u64) []const u8 {
    // Static buffers for formatted strings (not thread-safe, but OK for single-threaded benchmark)
    const S = struct {
        var bufs: [8][32]u8 = undefined;
        var idx: usize = 0;
    };

    const buf = &S.bufs[S.idx];
    S.idx = (S.idx + 1) % 8;

    if (ns < 1_000) {
        const result = std.fmt.bufPrint(buf, "{d}ns", .{ns}) catch "?";
        return result;
    } else if (ns < 1_000_000) {
        const us = @as(f64, @floatFromInt(ns)) / 1000.0;
        const result = std.fmt.bufPrint(buf, "{d:.1}μs", .{us}) catch "?";
        return result;
    } else if (ns < 1_000_000_000) {
        const ms = @as(f64, @floatFromInt(ns)) / 1_000_000.0;
        const result = std.fmt.bufPrint(buf, "{d:.1}ms", .{ms}) catch "?";
        return result;
    } else {
        const s = @as(f64, @floatFromInt(ns)) / 1_000_000_000.0;
        const result = std.fmt.bufPrint(buf, "{d:.1}s", .{s}) catch "?";
        return result;
    }
}

fn printMarkdownSummary(all_results: []const ScaleResults) void {
    std.debug.print("\n", .{});
    std.debug.print("═══════════════════════════════════════════════════════════════════════════════\n", .{});
    std.debug.print("                          README-Ready Output\n", .{});
    std.debug.print("═══════════════════════════════════════════════════════════════════════════════\n", .{});
    std.debug.print("\n", .{});
    std.debug.print("```markdown\n", .{});
    std.debug.print("## Performance: LatticeDB vs SQLite Graph Traversal\n", .{});
    std.debug.print("\n", .{});
    std.debug.print("LatticeDB's native graph traversal compared to SQLite using JOINs and recursive CTEs.\n", .{});
    std.debug.print("\n", .{});

    for (all_results) |entry| {
        std.debug.print("### {s}\n", .{entry.scale.name()});
        std.debug.print("\n", .{});
        std.debug.print("| Workload | LatticeDB | SQLite | Speedup |\n", .{});
        std.debug.print("|----------|-----------|--------|--------:|\n", .{});

        for (entry.results) |r| {
            const lattice_str = formatDuration(r.lattice_ns);
            const sqlite_str = formatDuration(r.sqlite_ns);
            const speedup = r.speedup();

            std.debug.print("| {s} | {s} | {s} | {d:.1}x |\n", .{
                r.name,
                lattice_str,
                sqlite_str,
                speedup,
            });
        }
        std.debug.print("\n", .{});
    }

    std.debug.print("*Benchmark: Social network graph with power-law degree distribution. ", .{});
    std.debug.print("SQLite uses optimal indexes. Measured on [hardware info].*\n", .{});
    std.debug.print("```\n", .{});
    std.debug.print("\n", .{});
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
    std.debug.print("║              LatticeDB vs SQLite Graph Traversal Benchmark                   ║\n", .{});
    std.debug.print("╚══════════════════════════════════════════════════════════════════════════════╝\n", .{});
    std.debug.print("\n", .{});

    const config = Config{
        .warmup_iterations = 10,
        .measure_iterations = 100,
        .seed = 42,
    };

    // Run benchmarks at different scales
    const scales = [_]Scale{ .small, .medium };

    var all_results: [2]ScaleResults = undefined;

    for (scales, 0..) |scale, i| {
        std.debug.print("─── {s} ───────────────────────────────────────────────────────────────────\n", .{scale.name()});

        const results = try runBenchmarkSuite(allocator, scale, config);
        printResults(scale, results);

        all_results[i] = .{ .scale = scale, .results = results };
    }

    // Print README-ready markdown
    printMarkdownSummary(&all_results);

    // Cleanup
    for (&all_results) |*entry| {
        allocator.free(entry.results);
    }

    std.debug.print("Benchmark complete.\n", .{});
}
