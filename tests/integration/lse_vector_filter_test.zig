//! Vector search among a set of nodes: `<=>` under a MATCH that narrows the
//! candidates, and `Database.vectorSearchAmongInTxn`, give the nearest of
//! the set itself rather than whichever of the nearest overall happen to be
//! in it.

const std = @import("std");
const lattice = @import("lattice");

const Database = lattice.storage.database.Database;
const VectorSearchResult = lattice.storage.database.VectorSearchResult;

const DIMS = 16;
const COUNT = 3000;

fn removeFiles(comptime path: []const u8) void {
    @import("compat").fs.cwd().deleteFile(path) catch {};
    @import("compat").fs.cwd().deleteFile(path ++ "-wal") catch {};
}

fn openDb(allocator: std.mem.Allocator, comptime path: []const u8) !*Database {
    removeFiles(path);
    return Database.open(allocator, path, .{
        .create = true,
        .config = .{ .enable_vector = true, .vector_dimensions = DIMS },
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
    try std.testing.expect(!Database.exactScanPays(half.len, hnsw.vector_count, hnsw.config.ef_search, hnsw.config.m_max0));

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
        const got = try db.vectorSearchAmongInTxn(null, &query, 10, null, half, null);
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
