//! Vector search among a set of nodes: `<=>` under a MATCH that narrows the
//! candidates, and `Database.vectorSearchAmongInTxn`, give the nearest of
//! the set itself rather than whichever of the nearest overall happen to be
//! in it.

const std = @import("std");
const lattice = @import("lattice");

const Database = lattice.storage.database.Database;
const VectorSearchResult = lattice.storage.database.VectorSearchResult;

const DIMS = 16;
const COUNT = 1000;
/// A build list this short keeps the graph good enough for these sizes and
/// the inserts cheap in a Debug build.
const EF_CONSTRUCTION = 48;

fn removeFiles(comptime path: []const u8) void {
    @import("compat").fs.cwd().deleteFile(path) catch {};
    @import("compat").fs.cwd().deleteFile(path ++ "-wal") catch {};
}

fn openDb(allocator: std.mem.Allocator, comptime path: []const u8) !*Database {
    removeFiles(path);
    return Database.open(allocator, path, .{
        .create = true,
        .config = .{ .enable_vector = true, .vector_dimensions = DIMS, .vector_ef_construction = EF_CONSTRUCTION },
    });
}

/// COUNT nodes labelled Item with random vectors; every `rare_every`th is
/// also Rare. Returns the Rare ids.
fn populate(db: *Database, allocator: std.mem.Allocator, rare_every: usize) ![]u64 {
    var prng = std.Random.DefaultPrng.init(5);
    const rnd = prng.random();
    var rare: std.ArrayListUnmanaged(u64) = .empty;
    errdefer rare.deinit(allocator);
    var txn = try db.beginTransaction(.read_write);
    errdefer db.abortTransaction(&txn) catch {};
    for (0..COUNT) |i| {
        const is_rare = i % rare_every == 0;
        const labels: []const []const u8 = if (is_rare) &.{ "Item", "Rare" } else &.{"Item"};
        const id = try db.createNode(&txn, labels);
        var v: [DIMS]f32 = undefined;
        for (&v) |*x| x.* = rnd.float(f32) * 2 - 1;
        try db.setNodeVectorInTxn(&txn, id, &v);
        if (is_rare) try rare.append(allocator, id);
    }
    try db.commitTransaction(&txn);
    return rare.toOwnedSlice(allocator);
}

/// The k nearest of `ids` by measuring every one, as the transaction sees
/// them.
fn exactNearest(db: *Database, allocator: std.mem.Allocator, txn: ?*lattice.Transaction, query: []const f32, ids: []const u64, k: usize) ![]VectorSearchResult {
    var all: std.ArrayListUnmanaged(VectorSearchResult) = .empty;
    defer all.deinit(allocator);
    for (ids) |id| {
        if (!(try db.nodeExistsInTxn(txn, id))) continue;
        const v = (try db.getNodeVectorInTxn(txn, id)) orelse continue;
        defer db.allocator.free(v);
        try all.append(allocator, .{ .node_id = id, .distance = db.hnsw_index.?.distanceForSearch(query, v) });
    }
    std.mem.sort(VectorSearchResult, all.items, {}, struct {
        fn lt(_: void, a: VectorSearchResult, b: VectorSearchResult) bool {
            return a.distance < b.distance;
        }
    }.lt);
    return allocator.dupe(VectorSearchResult, all.items[0..@min(k, all.items.len)]);
}

fn expectSame(want: []const VectorSearchResult, got: []const VectorSearchResult) !void {
    try std.testing.expectEqual(want.len, got.len);
    for (want, got) |w, g| {
        try std.testing.expectEqual(w.node_id, g.node_id);
        try std.testing.expectApproxEqAbs(w.distance, g.distance, 1e-5);
    }
}

test "vector filter: a small set is answered exactly, in and out of a transaction" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lse_vector_filter_small.ltdb";
    const db = try openDb(allocator, path);
    defer {
        db.close();
        removeFiles(path);
    }
    const rare = try populate(db, allocator, 100);
    defer allocator.free(rare);

    const query = [_]f32{0.25} ** DIMS;
    {
        const want = try exactNearest(db, allocator, null, &query, rare, 5);
        defer allocator.free(want);
        const got = try db.vectorSearchAmongInTxn(null, &query, 5, null, rare, null);
        defer db.freeVectorSearchResults(got);
        try expectSame(want, got);
    }

    // A transaction deletes the nearest, moves another, and its view is
    // what counts.
    var txn = try db.beginTransaction(.read_write);
    defer db.abortTransaction(&txn) catch {};
    {
        const before = try exactNearest(db, allocator, null, &query, rare, 1);
        defer allocator.free(before);
        try db.deleteNode(&txn, before[0].node_id);
    }
    try db.setNodeVectorInTxn(&txn, rare[7], &query);
    const want = try exactNearest(db, allocator, &txn, &query, rare, 5);
    defer allocator.free(want);
    try std.testing.expectEqual(rare[7], want[0].node_id);
    const got = try db.vectorSearchAmongInTxn(&txn, &query, 5, null, rare, null);
    defer db.freeVectorSearchResults(got);
    try expectSame(want, got);
}

test "vector filter: a large set goes through the graph and still holds only its members" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lse_vector_filter_large.ltdb";
    const db = try openDb(allocator, path);
    defer {
        db.close();
        removeFiles(path);
    }
    // Nine nodes in ten: too many to measure one by one at this size.
    const all = try populate(db, allocator, 1);
    defer allocator.free(all);
    var kept: std.ArrayListUnmanaged(u64) = .empty;
    defer kept.deinit(allocator);
    for (all, 0..) |id, i| {
        if (i % 10 != 0) try kept.append(allocator, id);
    }
    const half = kept.items;
    const hnsw = &db.hnsw_index.?;
    // A narrow beam makes the graph cheaper than measuring each at this size.
    const ef: u16 = 16;
    try std.testing.expect(!Database.exactScanPays(half.len, hnsw.vector_count, ef, hnsw.config.m_max0));

    var members: std.AutoHashMapUnmanaged(u64, void) = .{};
    defer members.deinit(allocator);
    for (half) |id| try members.put(allocator, id, {});

    var prng = std.Random.DefaultPrng.init(17);
    const rnd = prng.random();
    var hits: usize = 0;
    for (0..20) |_| {
        var query: [DIMS]f32 = undefined;
        for (&query) |*x| x.* = rnd.float(f32) * 2 - 1;
        // The exact answer, by measuring every member (the scan the small
        // set takes; checked against brute force in the hnsw tests).
        const want = try hnsw.searchExact(&query, 10, half);
        defer hnsw.freeResults(want);
        const got = try db.vectorSearchAmongInTxn(null, &query, 10, ef, half, null);
        defer db.freeVectorSearchResults(got);
        try std.testing.expectEqual(@as(usize, 10), got.len);
        for (got) |g| {
            try std.testing.expect(members.contains(g.node_id));
            for (want) |w| {
                if (w.node_id == g.node_id) hits += 1;
            }
        }
    }
    // Approximate, but not far off.
    try std.testing.expect(hits * 100 >= 20 * 10 * 80);
}

test "vector filter: <=> under a narrow MATCH returns that label's nearest" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lse_vector_filter_cypher.ltdb";
    const db = try openDb(allocator, path);
    defer {
        db.close();
        removeFiles(path);
    }
    const rare = try populate(db, allocator, 300);
    defer allocator.free(rare);

    const query = [_]f32{-0.5} ** DIMS;
    const want = try exactNearest(db, allocator, null, &query, rare, 3);
    defer allocator.free(want);

    var params = std.StringHashMap(lattice.core.types.PropertyValue).init(allocator);
    defer params.deinit();
    try params.put("q", .{ .vector_val = &query });
    var result = try db.queryWithParams("MATCH (n:Rare) WHERE n.embedding <=> $q RETURN id(n) LIMIT 3", &params);
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 3), result.rows.len);
    for (result.rows, want) |row, w| {
        try std.testing.expectEqual(w.node_id, @as(u64, @intCast(row.values[0].int_val)));
    }

    // With a threshold and no LIMIT: every member within it, nearest first.
    var buf: [128]u8 = undefined;
    const cypher = try std.fmt.bufPrint(&buf, "MATCH (n:Rare) WHERE n.embedding <=> $q < {d} RETURN id(n)", .{want[2].distance + 1e-4});
    var within = try db.queryWithParams(cypher, &params);
    defer within.deinit();
    try std.testing.expectEqual(@as(usize, 3), within.rows.len);
    for (within.rows, want) |row, w| {
        try std.testing.expectEqual(w.node_id, @as(u64, @intCast(row.values[0].int_val)));
    }
}

/// One database per metric: the search among a set measures with the
/// database's metric on both of its paths, and `<=>` under the MATCH returns
/// the distance the search measured, which is the metric's distance.
fn checkMetric(comptime metric: lattice.storage.database.VectorMetric, comptime path: []const u8) !void {
    const allocator = std.testing.allocator;
    const nodes = 1000;
    removeFiles(path);
    const db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{ .enable_vector = true, .vector_dimensions = DIMS, .vector_metric = metric, .vector_ef_construction = EF_CONSTRUCTION },
    });
    defer {
        db.close();
        removeFiles(path);
    }
    try std.testing.expectEqual(metric, db.vectorConfig().?.metric);

    var prng = std.Random.DefaultPrng.init(11);
    const rnd = prng.random();
    var ids: [nodes]u64 = undefined;
    var vectors: [nodes][DIMS]f32 = undefined;
    {
        var txn = try db.beginTransaction(.read_write);
        errdefer db.abortTransaction(&txn) catch {};
        for (&ids, &vectors, 0..) |*id, *v, i| {
            const labels: []const []const u8 = if (i % 50 == 0) &.{ "Item", "Rare" } else &.{"Item"};
            id.* = try db.createNode(&txn, labels);
            // Lengths vary, so cosine, euclidean and dot product rank differently.
            const scale = 0.2 + 2 * rnd.float(f32);
            for (v) |*x| x.* = (rnd.float(f32) * 2 - 1) * scale;
            try db.setNodeVectorInTxn(&txn, id.*, v);
        }
        try db.commitTransaction(&txn);
    }

    // The truth, measured as `<=>` measures: on the vectors as given.
    const Truth = struct {
        fn nearest(d: *Database, alloc: std.mem.Allocator, q: []const f32, vs: []const [DIMS]f32, is: []const u64, member: []const bool, k: usize) ![]VectorSearchResult {
            var all: std.ArrayListUnmanaged(VectorSearchResult) = .empty;
            defer all.deinit(alloc);
            for (vs, is, member) |*v, id, m| {
                if (m) try all.append(alloc, .{ .node_id = id, .distance = d.vectorDistance(q, v) });
            }
            std.mem.sort(VectorSearchResult, all.items, {}, struct {
                fn lt(_: void, a: VectorSearchResult, b: VectorSearchResult) bool {
                    return a.distance < b.distance;
                }
            }.lt);
            return alloc.dupe(VectorSearchResult, all.items[0..@min(k, all.items.len)]);
        }
    };

    var rare_member: [nodes]bool = undefined;
    var most_member: [nodes]bool = undefined;
    var rare: std.ArrayListUnmanaged(u64) = .empty;
    defer rare.deinit(allocator);
    var most: std.ArrayListUnmanaged(u64) = .empty;
    defer most.deinit(allocator);
    for (ids, 0..) |id, i| {
        rare_member[i] = i % 50 == 0;
        most_member[i] = i % 10 != 0;
        if (rare_member[i]) try rare.append(allocator, id);
        if (most_member[i]) try most.append(allocator, id);
    }

    // Small set: measured one by one, exact.
    const hnsw = &db.hnsw_index.?;
    try std.testing.expect(Database.exactScanPays(rare.items.len, hnsw.vector_count, hnsw.config.ef_search, hnsw.config.m_max0));
    var query: [DIMS]f32 = undefined;
    for (&query) |*x| x.* = rnd.float(f32) * 2 - 1;
    {
        const want = try Truth.nearest(db, allocator, &query, &vectors, &ids, &rare_member, 5);
        defer allocator.free(want);
        const got = try db.vectorSearchAmongInTxn(null, &query, 5, null, rare.items, null);
        defer db.freeVectorSearchResults(got);
        try std.testing.expectEqual(want.len, got.len);
        for (want, got) |w, g| {
            try std.testing.expectEqual(w.node_id, g.node_id);
            try std.testing.expectApproxEqAbs(w.distance, g.distance, 1e-4);
        }
    }

    // Large set, with a narrow beam so the graph is cheaper than measuring
    // each: members only, each at its metric distance, nearest first.
    const ef: u16 = 16;
    try std.testing.expect(!Database.exactScanPays(most.items.len, hnsw.vector_count, ef, hnsw.config.m_max0));
    var hits: usize = 0;
    for (0..10) |_| {
        for (&query) |*x| x.* = rnd.float(f32) * 2 - 1;
        const want = try Truth.nearest(db, allocator, &query, &vectors, &ids, &most_member, 10);
        defer allocator.free(want);
        const got = try db.vectorSearchAmongInTxn(null, &query, 10, ef, most.items, null);
        defer db.freeVectorSearchResults(got);
        try std.testing.expectEqual(@as(usize, 10), got.len);
        var last = -std.math.inf(f32);
        for (got) |g| {
            const i = std.mem.indexOfScalar(u64, &ids, g.node_id).?;
            try std.testing.expect(most_member[i]);
            try std.testing.expectApproxEqAbs(db.vectorDistance(&query, &vectors[i]), g.distance, 1e-4);
            try std.testing.expect(g.distance >= last);
            last = g.distance;
            for (want) |w| {
                if (w.node_id == g.node_id) hits += 1;
            }
        }
    }
    try std.testing.expect(hits * 100 >= 10 * 10 * 60);

    // Cypher: the nearest of the label, and the distance `<=>` returns is
    // the one the search measured.
    for (&query) |*x| x.* = rnd.float(f32) * 2 - 1;
    const want = try Truth.nearest(db, allocator, &query, &vectors, &ids, &rare_member, 3);
    defer allocator.free(want);
    var params = std.StringHashMap(lattice.core.types.PropertyValue).init(allocator);
    defer params.deinit();
    try params.put("q", .{ .vector_val = &query });
    var result = try db.queryWithParams("MATCH (n:Rare) WHERE n.embedding <=> $q RETURN id(n), n.embedding <=> $q LIMIT 3", &params);
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 3), result.rows.len);
    for (result.rows, want) |row, w| {
        try std.testing.expectEqual(w.node_id, @as(u64, @intCast(row.values[0].int_val)));
        try std.testing.expectApproxEqAbs(w.distance, @as(f32, @floatCast(row.values[1].float_val)), 1e-4);
    }
}

test "vector filter: cosine measures the set by cosine" {
    try checkMetric(.cosine, "/tmp/lse_vector_filter_cosine.ltdb");
}

test "vector filter: euclidean measures the set by euclidean distance" {
    try checkMetric(.euclidean, "/tmp/lse_vector_filter_euclidean.ltdb");
}

test "vector filter: dot product measures the set by negative dot product" {
    try checkMetric(.inner_product, "/tmp/lse_vector_filter_dot.ltdb");
}
