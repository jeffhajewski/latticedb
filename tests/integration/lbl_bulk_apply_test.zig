//! Large transactions: a commit that creates many edges inserts their
//! traversal keys in key order ahead of their records. These tests hold that
//! path to what the edge-by-edge path produces.

const std = @import("std");
const lattice = @import("lattice");

const Database = lattice.storage.database.Database;
const EdgeRef = lattice.storage.database.EdgeRef;
const NodeId = lattice.core.types.NodeId;
const EdgeId = lattice.core.types.EdgeId;

const allocator = std.testing.allocator;
const threshold = Database.sorted_edge_apply_min;

fn removeFiles(path: []const u8) void {
    const cwd = @import("compat").fs.cwd();
    cwd.deleteFile(path) catch {};
    var buf: [256]u8 = undefined;
    const wal = std.fmt.bufPrint(&buf, "{s}-wal", .{path}) catch return;
    cwd.deleteFile(wal) catch {};
    const journal = std.fmt.bufPrint(&buf, "{s}-journal", .{path}) catch return;
    cwd.deleteFile(journal) catch {};
}

fn openDb(path: []const u8, create: bool, wal: bool) !*Database {
    if (create) removeFiles(path);
    return Database.open(allocator, path, .{
        .create = create,
        .config = .{ .enable_wal = wal, .enable_fts = false, .enable_vector = false },
    });
}

/// What a test expects an edge to be.
const Expected = struct {
    id: EdgeId,
    source: NodeId,
    target: NodeId,
    knows: bool,
    weight: ?i64,
};

/// A small deterministic generator, so a failure names the same graph twice.
const Lcg = struct {
    state: u64,
    fn next(self: *Lcg, below: usize) usize {
        self.state = self.state *% 6364136223846793005 +% 1442695040888963407;
        return @intCast((self.state >> 33) % below);
    }
};

/// Create `count` edges between random pairs of `nodes` in `txn`. Every
/// third one carries a weight; the types alternate.
fn createEdges(db: *Database, txn: anytype, nodes: []const NodeId, count: usize, rng: *Lcg, out: *std.ArrayListUnmanaged(Expected)) !void {
    for (0..count) |i| {
        const source = nodes[rng.next(nodes.len)];
        const target = nodes[rng.next(nodes.len)];
        const knows = i % 2 == 0;
        const id = try db.createEdgeAndGetId(txn, source, target, if (knows) "KNOWS" else "LIKES");
        const weight: ?i64 = if (i % 3 == 0) @intCast(i) else null;
        if (weight) |w| try db.setEdgePropertyById(txn, id, "w", .{ .int_val = w });
        try out.append(allocator, .{ .id = id, .source = source, .target = target, .knows = knows, .weight = weight });
    }
}

fn countWhere(refs: []const EdgeRef, id: EdgeId) usize {
    var n: usize = 0;
    for (refs) |ref| {
        if (ref.id == id) n += 1;
    }
    return n;
}

/// The store holds exactly `expected`: each edge's record, each edge once in
/// its source's outgoing list and once in its target's incoming list, no
/// other edge anywhere, and two traversal keys and one record per edge.
fn expectGraph(db: *Database, nodes: []const NodeId, expected: []const Expected) !void {
    for (expected) |want| {
        var edge = try db.edge_store.getById(want.id);
        defer edge.deinit(allocator);
        try std.testing.expectEqual(want.source, edge.source);
        try std.testing.expectEqual(want.target, edge.target);
        if (want.weight) |w| {
            try std.testing.expectEqual(@as(usize, 1), edge.properties.len);
            try std.testing.expectEqual(w, edge.properties[0].value.int_val);
        } else {
            try std.testing.expectEqual(@as(usize, 0), edge.properties.len);
        }
    }

    var out_total: usize = 0;
    var in_total: usize = 0;
    for (nodes) |node| {
        const out = try db.getOutgoingEdgeRefsInTxn(null, node);
        defer allocator.free(out);
        const in = try db.getIncomingEdgeRefsInTxn(null, node);
        defer allocator.free(in);
        out_total += out.len;
        in_total += in.len;
        for (expected) |want| {
            try std.testing.expectEqual(@as(usize, @intFromBool(want.source == node)), countWhere(out, want.id));
            try std.testing.expectEqual(@as(usize, @intFromBool(want.target == node)), countWhere(in, want.id));
        }
    }
    try std.testing.expectEqual(expected.len, out_total);
    try std.testing.expectEqual(expected.len, in_total);

    const keys = try db.edge_tree.spaceStats();
    try std.testing.expectEqual(@as(u64, expected.len * 2), keys.entries);
    // The id index also holds the next-id counter under key 0.
    const records = try db.edge_id_tree.spaceStats();
    try std.testing.expectEqual(@as(u64, expected.len + 1), records.entries);
}

fn createNodes(db: *Database, comptime count: usize) ![count]NodeId {
    var nodes: [count]NodeId = undefined;
    var txn = try db.beginTransaction(.read_write);
    for (&nodes) |*node| node.* = try db.createNode(&txn, &.{"P"});
    try db.commitTransaction(&txn);
    return nodes;
}

test "lbl: edges created on either side of the sorting threshold come out the same" {
    const path = "/tmp/lbl_threshold.ltdb";
    defer removeFiles(path);

    for ([_]usize{ 1, threshold - 1, threshold, threshold + 1, 5 * threshold }) |count| {
        var expected: std.ArrayListUnmanaged(Expected) = .empty;
        defer expected.deinit(allocator);
        var nodes: [24]NodeId = undefined;
        {
            var db = try openDb(path, true, true);
            defer db.close();
            nodes = try createNodes(db, 24);

            var rng = Lcg{ .state = count };
            var txn = try db.beginTransaction(.read_write);
            try createEdges(db, &txn, &nodes, count, &rng, &expected);
            try db.commitTransaction(&txn);
            try expectGraph(db, &nodes, expected.items);
        }
        var db = try openDb(path, false, true);
        defer db.close();
        try expectGraph(db, &nodes, expected.items);
    }
}

test "lbl: a large transaction that also updates and deletes edges" {
    const path = "/tmp/lbl_mixed.ltdb";
    defer removeFiles(path);

    var expected: std.ArrayListUnmanaged(Expected) = .empty;
    defer expected.deinit(allocator);
    var nodes: [30]NodeId = undefined;
    {
        var db = try openDb(path, true, true);
        defer db.close();
        nodes = try createNodes(db, 30);

        var rng = Lcg{ .state = 7 };
        var txn = try db.beginTransaction(.read_write);
        try createEdges(db, &txn, &nodes, 40, &rng, &expected);
        try db.commitTransaction(&txn);

        // One transaction: far more new edges than the threshold, a new
        // weight on one old edge, a first weight on another, and two old
        // edges deleted, one of them recreated between the same nodes.
        txn = try db.beginTransaction(.read_write);
        try db.setEdgePropertyById(&txn, expected.items[0].id, "w", .{ .int_val = -1 });
        expected.items[0].weight = -1;
        try db.setEdgePropertyById(&txn, expected.items[1].id, "w", .{ .int_val = -2 });
        expected.items[1].weight = -2;

        const gone = expected.orderedRemove(10);
        try db.deleteEdgeById(&txn, gone.id);
        const again = expected.orderedRemove(20);
        try db.deleteEdgeById(&txn, again.id);
        const reborn = try db.createEdgeAndGetId(&txn, again.source, again.target, if (again.knows) "KNOWS" else "LIKES");
        try expected.append(allocator, .{ .id = reborn, .source = again.source, .target = again.target, .knows = again.knows, .weight = null });

        try createEdges(db, &txn, &nodes, 6 * threshold, &rng, &expected);
        try db.commitTransaction(&txn);

        try expectGraph(db, &nodes, expected.items);
        try std.testing.expectError(error.NotFound, db.edge_store.getById(gone.id));
        try std.testing.expectError(error.NotFound, db.edge_store.getById(again.id));
    }
    var db = try openDb(path, false, true);
    defer db.close();
    try expectGraph(db, &nodes, expected.items);
}

test "lbl: a reader that began before a large commit does not see its edges" {
    const path = "/tmp/lbl_reader.ltdb";
    defer removeFiles(path);
    var db = try openDb(path, true, true);
    defer db.close();
    const nodes = try createNodes(db, 8);

    var reader = try db.beginTransaction(.read_only);

    var expected: std.ArrayListUnmanaged(Expected) = .empty;
    defer expected.deinit(allocator);
    var rng = Lcg{ .state = 3 };
    var writer = try db.beginTransaction(.read_write);
    try createEdges(db, &writer, &nodes, 3 * threshold, &rng, &expected);
    try db.commitTransaction(&writer);

    for (nodes) |node| {
        const out = try db.getOutgoingEdgeRefsInTxn(&reader, node);
        defer allocator.free(out);
        try std.testing.expectEqual(@as(usize, 0), out.len);
        const in = try db.getIncomingEdgeRefsInTxn(&reader, node);
        defer allocator.free(in);
        try std.testing.expectEqual(@as(usize, 0), in.len);
    }
    try db.commitTransaction(&reader);

    try expectGraph(db, &nodes, expected.items);
}

test "lbl: a large transaction without a log" {
    const path = "/tmp/lbl_nolog.ltdb";
    defer removeFiles(path);

    var expected: std.ArrayListUnmanaged(Expected) = .empty;
    defer expected.deinit(allocator);
    var nodes: [16]NodeId = undefined;
    {
        var db = try openDb(path, true, false);
        defer db.close();
        nodes = try createNodes(db, 16);
        var rng = Lcg{ .state = 11 };
        var txn = try db.beginTransaction(.read_write);
        try createEdges(db, &txn, &nodes, 4 * threshold, &rng, &expected);
        try db.commitTransaction(&txn);
        try expectGraph(db, &nodes, expected.items);
    }
    var db = try openDb(path, false, false);
    defer db.close();
    try expectGraph(db, &nodes, expected.items);
}

test "lbl: a rolled back large transaction leaves nothing" {
    const path = "/tmp/lbl_rollback.ltdb";
    defer removeFiles(path);
    var db = try openDb(path, true, true);
    defer db.close();
    const nodes = try createNodes(db, 8);

    var expected: std.ArrayListUnmanaged(Expected) = .empty;
    defer expected.deinit(allocator);
    var rng = Lcg{ .state = 5 };
    var txn = try db.beginTransaction(.read_write);
    try createEdges(db, &txn, &nodes, 2 * threshold, &rng, &expected);
    try db.abortTransaction(&txn);

    try expectGraph(db, &nodes, &.{});
}
