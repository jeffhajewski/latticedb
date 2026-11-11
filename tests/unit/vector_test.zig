//! Behavioural tests for the vector index: failure atomicity, persistence,
//! graph repair and the database-level vector API.

const std = @import("std");
const lattice = @import("lattice");
const helpers = @import("helpers.zig");

const hnsw_mod = lattice.vector.hnsw;
const VectorStorage = lattice.vector.storage.VectorStorage;
const HnswIndex = hnsw_mod.HnswIndex;
const HnswConfig = hnsw_mod.HnswConfig;
const Database = lattice.storage.database.Database;

const DIMS = 8;

fn randomVector(random: std.Random) [DIMS]f32 {
    var v: [DIMS]f32 = undefined;
    for (&v) |*x| x.* = random.float(f32) * 2.0 - 1.0;
    return v;
}

/// Every neighbour list of every node, keyed by (id, layer).
const GraphSnapshot = struct {
    lists: std.AutoHashMap([2]u64, []u64),
    allocator: std.mem.Allocator,
    count: u64,
    entry_point: ?u64,
    max_layer: u8,

    fn take(allocator: std.mem.Allocator, index: *HnswIndex) !GraphSnapshot {
        var snap = GraphSnapshot{
            .lists = std.AutoHashMap([2]u64, []u64).init(allocator),
            .allocator = allocator,
            .count = index.vector_count,
            .entry_point = index.entry_point,
            .max_layer = index.max_layer,
        };
        errdefer snap.deinit();
        var it = index.nodes.iterator();
        while (it.next()) |entry| {
            var layer: u16 = 0;
            while (layer <= entry.value_ptr.max_layer) : (layer += 1) {
                const conns = try index.getConnections(entry.value_ptr.connections_loc, @intCast(layer));
                defer index.allocator.free(conns);
                const copy = try allocator.dupe(u64, conns);
                std.mem.sort(u64, copy, {}, std.sort.asc(u64));
                try snap.lists.put(.{ entry.key_ptr.*, layer }, copy);
            }
        }
        return snap;
    }

    fn deinit(self: *GraphSnapshot) void {
        var it = self.lists.valueIterator();
        while (it.next()) |v| self.allocator.free(v.*);
        self.lists.deinit();
    }

    fn expectEqual(self: *GraphSnapshot, other: *GraphSnapshot) !void {
        try std.testing.expectEqual(self.count, other.count);
        try std.testing.expectEqual(self.entry_point, other.entry_point);
        try std.testing.expectEqual(self.max_layer, other.max_layer);
        try std.testing.expectEqual(self.lists.count(), other.lists.count());
        var it = self.lists.iterator();
        while (it.next()) |entry| {
            const theirs = other.lists.get(entry.key_ptr.*) orelse return error.TestExpectedEqual;
            try std.testing.expectEqualSlices(u64, entry.value_ptr.*, theirs);
        }
    }
};

/// Runs `op` with an allocator that fails on its n-th allocation, for every n
/// until `op` succeeds, and checks after each failure that the index is
/// exactly as it was.
fn expectAllOrNothing(
    comptime op: fn (*HnswIndex, std.Random) anyerror!void,
    name: []const u8,
) !void {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.init(allocator, name);
    defer db.deinit();

    var vs = try VectorStorage.init(allocator, db.bp, DIMS);
    defer vs.deinit();
    var failing = std.testing.FailingAllocator.init(allocator, .{});
    var index = HnswIndex.init(failing.allocator(), db.bp, &vs, .{
        .dimensions = DIMS,
        .m = 4,
        .m_max0 = 8,
        .ef_construction = 16,
        .ef_search = 16,
        .metric = .cosine,
    });
    defer index.deinit();

    var prng = std.Random.DefaultPrng.init(7);
    for (1..61) |id| {
        const v = randomVector(prng.random());
        try index.insert(id, &v);
    }

    var fail_at: usize = 0;
    while (true) : (fail_at += 1) {
        var before = try GraphSnapshot.take(allocator, &index);
        defer before.deinit();
        const stored_before = try vs.count();

        failing.fail_index = failing.alloc_index + fail_at;
        var op_prng = std.Random.DefaultPrng.init(99);
        const result = op(&index, op_prng.random());
        failing.fail_index = std.math.maxInt(usize);
        if (result) |_| break else |_| {}

        var after = try GraphSnapshot.take(allocator, &index);
        defer after.deinit();
        try before.expectEqual(&after);
        try std.testing.expectEqual(stored_before, try vs.count());

        // Every node the index holds must still be searchable.
        const q = randomVector(prng.random());
        const results = try index.search(&q, 10, null);
        index.freeResults(results);
        if (fail_at > 10_000) return error.TestUnexpectedResult;
    }
    try std.testing.expect(fail_at > 0);
}

fn opInsertNew(index: *HnswIndex, random: std.Random) anyerror!void {
    const v = randomVector(random);
    try index.insert(1000, &v);
}

fn opReplace(index: *HnswIndex, random: std.Random) anyerror!void {
    const v = randomVector(random);
    try index.insert(17, &v);
}

fn opRemove(index: *HnswIndex, _: std.Random) anyerror!void {
    try index.remove(23);
}

test "hnsw insert of a new vector is all or nothing" {
    try expectAllOrNothing(opInsertNew, "hnsw_atomic_insert");
}

test "hnsw replacement of a vector is all or nothing" {
    try expectAllOrNothing(opReplace, "hnsw_atomic_replace");
}

test "hnsw remove is all or nothing" {
    try expectAllOrNothing(opRemove, "hnsw_atomic_remove");
}

fn openVectorDb(path: []const u8) !*Database {
    @import("compat").fs.cwd().deleteFile(path) catch {};
    return Database.open(std.testing.allocator, path, .{
        .create = true,
        .config = .{
            .enable_wal = true,
            .enable_vector = true,
            .vector_dimensions = 4,
        },
    });
}

test "database rejects NaN and infinite vectors" {
    const path = "/tmp/lattice_s2_vector_nonfinite.ltdb";
    const db = try openVectorDb(path);
    defer {
        db.close();
        @import("compat").fs.cwd().deleteFile(path) catch {};
        @import("compat").fs.cwd().deleteFile(path ++ "-wal") catch {};
    }

    const labels = [_][]const u8{"Doc"};
    const node = try db.createNode(null, &labels);
    try db.setNodeVector(node, &[_]f32{ 1, 0, 0, 0 });

    const bad = [_][4]f32{
        .{ std.math.nan(f32), 0, 0, 0 },
        .{ 0, std.math.inf(f32), 0, 0 },
        .{ 0, 0, -std.math.inf(f32), 0 },
    };
    for (bad) |v| {
        try std.testing.expectError(error.InvalidArgument, db.setNodeVector(node, &v));
        try std.testing.expectError(error.InvalidArgument, db.vectorSearch(&v, 1, null));

        var txn = try db.beginTransaction(.read_write);
        try std.testing.expectError(error.InvalidArgument, db.setNodeVectorInTxn(&txn, node, &v));
        try db.abortTransaction(&txn);
    }

    // The good vector is still there and still found.
    const results = try db.vectorSearch(&[_]f32{ 1, 0, 0, 0 }, 1, null);
    defer db.freeVectorSearchResults(results);
    try std.testing.expectEqual(@as(usize, 1), results.len);
    try std.testing.expectEqual(node, results[0].node_id);
}

test "pending vectors score on the committed scale under cosine" {
    const path = "/tmp/lattice_s2_vector_pending_cosine.ltdb";
    const db = try openVectorDb(path);
    defer {
        db.close();
        @import("compat").fs.cwd().deleteFile(path) catch {};
        @import("compat").fs.cwd().deleteFile(path ++ "-wal") catch {};
    }

    const labels = [_][]const u8{"Doc"};
    const a = try db.createNode(null, &labels);
    const b = try db.createNode(null, &labels);
    try db.setNodeVector(a, &[_]f32{ 1, 0.2, 0, 0 });

    // b points exactly along the query; a does not.
    const query = [_]f32{ 0.1, 0, 0, 0 };
    var txn = try db.beginTransaction(.read_write);
    try db.setNodeVectorInTxn(&txn, b, &[_]f32{ 1, 0, 0, 0 });

    const pending = try db.vectorSearchInTxn(&txn, &query, 2, null);
    defer db.freeVectorSearchResults(pending);
    try std.testing.expectEqual(@as(usize, 2), pending.len);
    try std.testing.expectEqual(b, pending[0].node_id);
    try std.testing.expectEqual(a, pending[1].node_id);

    try db.commitTransaction(&txn);
    const committed = try db.vectorSearch(&query, 2, null);
    defer db.freeVectorSearchResults(committed);
    try std.testing.expectEqual(b, committed[0].node_id);
    for (pending, committed) |p, c| {
        try std.testing.expectEqual(p.node_id, c.node_id);
        try std.testing.expectApproxEqAbs(c.distance, p.distance, 1e-5);
    }
}

test "transactional vector search with the largest k does not overflow" {
    const path = "/tmp/lattice_s2_vector_txn_max_k.ltdb";
    const db = try openVectorDb(path);
    defer {
        db.close();
        @import("compat").fs.cwd().deleteFile(path) catch {};
        @import("compat").fs.cwd().deleteFile(path ++ "-wal") catch {};
    }

    const labels = [_][]const u8{"Doc"};
    const a = try db.createNode(null, &labels);
    const b = try db.createNode(null, &labels);
    try db.setNodeVector(a, &[_]f32{ 1, 0, 0, 0 });

    var txn = try db.beginTransaction(.read_write);
    defer db.abortTransaction(&txn) catch {};
    try db.setNodeVectorInTxn(&txn, b, &[_]f32{ 0, 1, 0, 0 });

    const all = try db.vectorSearchInTxn(&txn, &[_]f32{ 1, 1, 0, 0 }, std.math.maxInt(u32), null);
    defer db.freeVectorSearchResults(all);
    try std.testing.expectEqual(@as(usize, 2), all.len);
}

/// The tree must hold exactly the node map, and load back into the same graph.
fn expectTreeMatches(index: *HnswIndex, vs: *VectorStorage, tree: *helpers.BTree, db: *helpers.TempDb) !void {
    const allocator = std.testing.allocator;
    var seen: usize = 0;
    {
        var it = try tree.range(null, null);
        defer it.deinit();
        while (try it.next()) |e| {
            if (std.mem.eql(u8, e.key, &([_]u8{0xFF} ** 8))) continue;
            const id = std.mem.readInt(u64, e.key[0..8], .little);
            const entry = index.getNode(id) orelse return error.StaleTreeEntry;
            var buf: [hnsw_mod.HnswNodeEntry.SERIALIZED_SIZE]u8 = undefined;
            entry.serialize(&buf);
            try std.testing.expectEqualSlices(u8, &buf, e.value);
            seen += 1;
        }
    }
    try std.testing.expectEqual(@as(usize, index.nodes.count()), seen);

    var loaded = HnswIndex.init(allocator, db.bp, vs, index.config);
    defer loaded.deinit();
    try std.testing.expect(try loaded.loadFromTree(tree));
    var a = try GraphSnapshot.take(allocator, index);
    defer a.deinit();
    var b = try GraphSnapshot.take(allocator, &loaded);
    defer b.deinit();
    try a.expectEqual(&b);
}

test "incremental hnsw saves keep the tree equal to the node map" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.init(allocator, "hnsw_incremental_save");
    defer db.deinit();

    var vs = try VectorStorage.init(allocator, db.bp, DIMS);
    defer vs.deinit();
    var failing = std.testing.FailingAllocator.init(allocator, .{});
    var index = HnswIndex.init(failing.allocator(), db.bp, &vs, .{
        .dimensions = DIMS,
        .m = 4,
        .m_max0 = 8,
        .ef_construction = 16,
        .metric = .cosine,
    });
    defer index.deinit();
    var tree = try db.createBTree();

    var prng = std.Random.DefaultPrng.init(3);
    const random = prng.random();
    for (1..41) |id| {
        const v = randomVector(random);
        try index.insert(id, &v);
    }
    try index.saveToTree(&tree);
    try expectTreeMatches(&index, &vs, &tree, &db);

    for (0..30) |round| {
        // A mix of new ids, replacements, removals and re-insertions.
        for (0..5) |_| {
            const id = random.intRangeAtMost(u64, 1, 60);
            switch (random.uintLessThan(u8, 3)) {
                0, 1 => {
                    const v = randomVector(random);
                    try index.insert(id, &v);
                },
                else => try index.remove(id),
            }
        }
        // A failed insert in between must not leave a trace either.
        if (round % 3 == 0) {
            failing.fail_index = failing.alloc_index + 2;
            const v = randomVector(random);
            if (index.insert(500 + round, &v)) |_| {} else |_| {}
            failing.fail_index = std.math.maxInt(usize);
        }
        try index.saveToTree(&tree);
        try expectTreeMatches(&index, &vs, &tree, &db);
    }

    // A rebuilt map is written out in full, dropping ids it no longer has.
    index.resetForRebuild();
    for (1..11) |id| {
        const v = randomVector(random);
        try index.insert(id, &v);
    }
    try index.saveToTree(&tree);
    try expectTreeMatches(&index, &vs, &tree, &db);
}

fn bruteForceTopIds(allocator: std.mem.Allocator, query: []const f32, vectors: []const [16]f32, alive: []const bool, k: usize) ![]u64 {
    const Pair = struct { id: u64, d: f32 };
    var all: std.ArrayListUnmanaged(Pair) = .empty;
    defer all.deinit(allocator);
    for (vectors, alive, 0..) |*v, live, i| {
        if (!live) continue;
        try all.append(allocator, .{ .id = i + 1, .d = hnsw_mod.cosineDistance(query, v) });
    }
    std.mem.sort(Pair, all.items, {}, struct {
        fn lt(_: void, a: Pair, b: Pair) bool {
            return a.d < b.d;
        }
    }.lt);
    const out = try allocator.alloc(u64, k);
    for (out, all.items[0..k]) |*o, p| o.* = p.id;
    return out;
}

/// Recall of `index`, whose ids are the vector's position + 1 + `id_offset`.
fn recallAt10(allocator: std.mem.Allocator, index: *HnswIndex, vectors: []const [16]f32, alive: []const bool, random: std.Random, id_offset: u64) !f64 {
    var hits: usize = 0;
    const queries = 100;
    for (0..queries) |_| {
        var q: [16]f32 = undefined;
        for (&q) |*x| x.* = random.float(f32) * 2.0 - 1.0;
        const truth = try bruteForceTopIds(allocator, &q, vectors, alive, 10);
        defer allocator.free(truth);
        const results = try index.search(&q, 10, 32);
        defer index.freeResults(results);
        for (truth) |t| {
            for (results) |r| {
                if (r.node_id == t + id_offset) {
                    hits += 1;
                    break;
                }
            }
        }
    }
    return @as(f64, @floatFromInt(hits)) / (queries * 10.0);
}

test "hnsw recall holds up under delete and re-insert churn" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.initWithPoolSize(allocator, "hnsw_churn_recall", 32 * 1024 * 1024);
    defer db.deinit();

    var vs = try VectorStorage.init(allocator, db.bp, 16);
    defer vs.deinit();
    var index = HnswIndex.init(allocator, db.bp, &vs, .{
        .dimensions = 16,
        .m = 6,
        .m_max0 = 12,
        .ef_construction = 40,
        .metric = .cosine,
    });
    defer index.deinit();
    var tree = try db.createBTree();

    const n = 1500;
    var prng = std.Random.DefaultPrng.init(11);
    const random = prng.random();
    const vectors = try allocator.alloc([16]f32, n);
    defer allocator.free(vectors);
    const alive = try allocator.alloc(bool, n);
    defer allocator.free(alive);
    for (vectors, alive, 1..) |*v, *live, id| {
        for (v) |*x| x.* = random.float(f32) * 2.0 - 1.0;
        try index.insert(id, v);
        live.* = true;
    }
    const fresh = try recallAt10(allocator, &index, vectors, alive, random, 0);

    // Twenty rounds, each removing a fifth of the index and inserting as
    // many back, so every node is touched several times over.
    for (0..20) |_| {
        for (0..n / 5) |_| {
            const i = random.uintLessThan(usize, n);
            if (!alive[i]) continue;
            try index.remove(i + 1);
            alive[i] = false;
        }
        for (vectors, alive, 1..) |*v, *live, id| {
            if (live.* or random.boolean()) continue;
            try index.insert(id, v);
            live.* = true;
        }
        try index.saveToTree(&tree);
    }

    // Compare with an index built from scratch over the same live vectors,
    // asked the same queries.
    var rebuilt = HnswIndex.init(allocator, db.bp, &vs, index.config);
    defer rebuilt.deinit();
    for (vectors, alive, 1..) |*v, live, id| {
        if (live) try rebuilt.insert(id + n, v);
    }
    var query_seed = std.Random.DefaultPrng.init(12);
    const churned = try recallAt10(allocator, &index, vectors, alive, query_seed.random(), 0);
    query_seed = std.Random.DefaultPrng.init(12);
    const baseline = try recallAt10(allocator, &rebuilt, vectors, alive, query_seed.random(), n);
    std.debug.print("\n    recall@10: first build {d:.3}; after churn {d:.3}; rebuilt from scratch {d:.3}\n", .{ fresh, churned, baseline });
    try std.testing.expect(churned >= baseline - 0.03);

    // Every live node is still reachable: searching for its own vector
    // finds it.
    var missing: usize = 0;
    for (vectors, alive, 1..) |*v, live, id| {
        if (!live) continue;
        const results = try index.search(v, 1, 32);
        defer index.freeResults(results);
        if (results.len == 0 or results[0].node_id != id) missing += 1;
    }
    try std.testing.expect(missing <= n / 200);
}

test "hnsw with more neighbours per list than its read buffers still searches" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.initWithPoolSize(allocator, "hnsw_wide_lists", 16 * 1024 * 1024);
    defer db.deinit();
    var vs = try VectorStorage.init(allocator, db.bp, 4);
    defer vs.deinit();
    var index = HnswIndex.init(allocator, db.bp, &vs, .{
        .dimensions = 4,
        .m = 80,
        .m_max0 = 160,
        .ef_construction = 200,
        .metric = .euclidean,
    });
    defer index.deinit();

    var prng = std.Random.DefaultPrng.init(4);
    for (1..201) |id| {
        var v: [4]f32 = undefined;
        for (&v) |*x| x.* = prng.random().float(f32);
        try index.insert(id, &v);
    }
    const results = try index.search(&[_]f32{ 0.5, 0.5, 0.5, 0.5 }, 10, null);
    defer index.freeResults(results);
    try std.testing.expectEqual(@as(usize, 10), results.len);
}
