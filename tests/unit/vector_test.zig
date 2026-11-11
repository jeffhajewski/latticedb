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
