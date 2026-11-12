//! A node's vector belongs to the node, whatever labels it has.
//!
//! Removing a node's last label used to delete its vector, on both the
//! transactional and the direct path. The node was still there, still
//! reachable by `MATCH (n)` and by id, and a node created with no labels could
//! hold a vector all along, so nothing about being unlabelled made a vector
//! unreachable: the rule only lost data on a label change.

const std = @import("std");
const lattice = @import("lattice");

const Database = lattice.storage.database.Database;
const NodeId = lattice.core.types.NodeId;

const dims = 4;

fn cleanup(path: []const u8) void {
    const cwd = @import("compat").fs.cwd();
    var buf: [256]u8 = undefined;
    cwd.deleteFile(path) catch {};
    cwd.deleteFile(std.fmt.bufPrint(&buf, "{s}-wal", .{path}) catch return) catch {};
    cwd.deleteFile(std.fmt.bufPrint(&buf, "{s}-journal", .{path}) catch return) catch {};
}

fn openDb(allocator: std.mem.Allocator, path: []const u8, create: bool) !*Database {
    return Database.open(allocator, path, .{ .create = create, .config = .{
        .enable_fts = false,
        .enable_vector = true,
        .vector_dimensions = dims,
    } });
}

fn expectHasVector(db: *Database, node_id: NodeId) !void {
    const vector = (try db.getNodeVectorInTxn(null, node_id)) orelse return error.VectorMissing;
    defer db.allocator.free(vector);
    try std.testing.expectEqual(@as(usize, dims), vector.len);
}

/// The nearest node to `query` must be `node_id`: the index still holds it.
fn expectNearest(db: *Database, query: []const f32, node_id: NodeId) !void {
    const results = try db.vectorSearch(query, 1, null);
    defer db.freeVectorSearchResults(results);
    try std.testing.expectEqual(@as(usize, 1), results.len);
    try std.testing.expectEqual(node_id, results[0].node_id);
}

test "llv: removing a node's last label in a transaction keeps its vector" {
    const allocator = std.testing.allocator;
    const path = "/tmp/llv_label_vector_txn.ltdb";
    cleanup(path);
    defer cleanup(path);

    const near = [_]f32{ 1, 0, 0, 0 };
    const far = [_]f32{ 0, 0, 0, 1 };
    var target: NodeId = undefined;
    {
        var db = try openDb(allocator, path, true);
        defer db.close();

        var setup = try db.beginTransaction(.read_write);
        target = try db.createNode(&setup, &.{"Doc"});
        try db.setNodeVectorInTxn(&setup, target, &near);
        const other = try db.createNode(&setup, &.{"Doc"});
        try db.setNodeVectorInTxn(&setup, other, &far);
        try db.commitTransaction(&setup);

        var txn = try db.beginTransaction(.read_write);
        try db.removeNodeLabel(&txn, target, "Doc");
        // Visible inside the transaction that removed the label...
        const inside = (try db.getNodeVectorInTxn(&txn, target)) orelse return error.VectorMissing;
        allocator.free(inside);
        try db.commitTransaction(&txn);

        // ...and after it commits, by read and by search.
        try expectHasVector(db, target);
        try expectNearest(db, &near, target);

        // A label added back changes nothing about the vector either.
        var relabel = try db.beginTransaction(.read_write);
        try db.addNodeLabel(&relabel, target, "Other");
        try db.commitTransaction(&relabel);
        try expectHasVector(db, target);
    }

    // What the file holds agrees with what the process held.
    var db = try openDb(allocator, path, false);
    defer db.close();
    try expectHasVector(db, target);
    try expectNearest(db, &near, target);
}

test "llv: removing a node's last label outside a transaction keeps its vector" {
    const allocator = std.testing.allocator;
    const path = "/tmp/llv_label_vector_direct.ltdb";
    cleanup(path);
    defer cleanup(path);

    const near = [_]f32{ 0, 1, 0, 0 };
    const far = [_]f32{ 0, 0, 1, 0 };
    var target: NodeId = undefined;
    {
        var db = try openDb(allocator, path, true);
        defer db.close();

        target = try db.createNode(null, &.{ "Doc", "Draft" });
        try db.setNodeVector(target, &near);
        const other = try db.createNode(null, &.{"Doc"});
        try db.setNodeVector(other, &far);

        // One label left: nothing was ever removed here.
        try db.removeNodeLabel(null, target, "Draft");
        try expectHasVector(db, target);
        // The last label: this is the case that dropped the vector.
        try db.removeNodeLabel(null, target, "Doc");
        try expectHasVector(db, target);
        try expectNearest(db, &near, target);
    }

    var db = try openDb(allocator, path, false);
    defer db.close();
    try expectHasVector(db, target);
    try expectNearest(db, &near, target);
}
