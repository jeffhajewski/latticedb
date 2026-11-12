//! Deleting a node never leaves its relationships behind, and an edge never
//! points at a node that is not there.

const std = @import("std");
const lattice = @import("lattice");

const Database = lattice.storage.database.Database;
const DatabaseError = lattice.storage.database.DatabaseError;
const DatabaseConfig = lattice.storage.database.DatabaseConfig;

const plain_config = DatabaseConfig{ .enable_fts = false };

fn cleanup(path: []const u8) void {
    const cwd = @import("compat").fs.cwd();
    var buf: [256]u8 = undefined;
    cwd.deleteFile(path) catch {};
    cwd.deleteFile(std.fmt.bufPrint(&buf, "{s}-wal", .{path}) catch return) catch {};
    cwd.deleteFile(std.fmt.bufPrint(&buf, "{s}-journal", .{path}) catch return) catch {};
}

test "lfmt: deleteNode refuses a node with relationships, detachDeleteNode removes them" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lfmt_delete_rule.ltdb";
    cleanup(path);
    defer cleanup(path);
    var db = try Database.open(allocator, path, .{ .create = true, .config = plain_config });
    defer db.close();

    var txn = try db.beginTransaction(.read_write);
    const a = try db.createNode(&txn, &.{"N"});
    const b = try db.createNode(&txn, &.{"N"});
    const c = try db.createNode(&txn, &.{"N"});
    _ = try db.createEdgeAndGetId(&txn, a, b, "R");
    _ = try db.createEdgeAndGetId(&txn, c, a, "R");
    _ = try db.createEdgeAndGetId(&txn, a, a, "SELF");
    // Refused for an edge the transaction created and has not committed.
    try std.testing.expectError(DatabaseError.NodeHasRelationships, db.deleteNode(&txn, b));
    try db.commitTransaction(&txn);

    // Refused for committed edges in either direction, and nothing changes.
    txn = try db.beginTransaction(.read_write);
    try std.testing.expectError(DatabaseError.NodeHasRelationships, db.deleteNode(&txn, a));
    try std.testing.expectError(DatabaseError.NodeHasRelationships, db.deleteNode(&txn, b));
    try std.testing.expectError(DatabaseError.NodeHasRelationships, db.deleteNode(&txn, c));
    try std.testing.expect(try db.nodeExistsInTxn(&txn, a));

    // Once the transaction has deleted b's only edge, b can go.
    try db.deleteEdge(&txn, a, b, "R");
    try db.deleteNode(&txn, b);

    // detach takes the rest: an incoming edge, and a self-loop listed both ways.
    try db.detachDeleteNode(&txn, a);
    try std.testing.expect(!try db.nodeExistsInTxn(&txn, a));
    try db.commitTransaction(&txn);

    try std.testing.expect(!try db.nodeExists(a));
    try std.testing.expect(!try db.nodeExists(b));
    const c_out = try db.getOutgoingEdges(c);
    defer db.freeEdgeInfos(c_out);
    try std.testing.expectEqual(@as(usize, 0), c_out.len);
    try db.deleteNode(null, c);

    // Deleting a node that is not there is still not an error.
    try db.deleteNode(null, 999_999);
}

test "lfmt: Cypher DELETE and DETACH DELETE keep their rules" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lfmt_delete_cypher.ltdb";
    cleanup(path);
    defer cleanup(path);
    var db = try Database.open(allocator, path, .{ .create = true, .config = plain_config });
    defer db.close();

    var r = try db.query("CREATE (a:P {n: 1})-[:R]->(b:P {n: 2})");
    r.deinit();
    try std.testing.expectError(error.ExecutionError, db.query("MATCH (a:P {n: 1}) DELETE a"));
    var r2 = try db.query("MATCH (a:P {n: 1}) DETACH DELETE a");
    r2.deinit();
    var r3 = try db.query("MATCH (b:P {n: 2}) DELETE b");
    r3.deinit();
    try std.testing.expectEqual(@as(u64, 0), db.nodeCount());
}

test "lfmt: an edge needs both of its ends to exist" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lfmt_edge_endpoints.ltdb";
    cleanup(path);
    defer cleanup(path);
    var db = try Database.open(allocator, path, .{ .create = true, .config = plain_config });
    defer db.close();

    const a = try db.createNode(null, &.{"N"});
    try std.testing.expectError(DatabaseError.NotFound, db.createEdgeAndGetId(null, a, 4242, "R"));
    try std.testing.expectError(DatabaseError.NotFound, db.createEdgeAndGetId(null, 4242, a, "R"));

    var txn = try db.beginTransaction(.read_write);
    const b = try db.createNode(&txn, &.{"N"});
    // A node the transaction created counts.
    _ = try db.createEdgeAndGetId(&txn, a, b, "R");
    const d = try db.createNode(&txn, &.{"N"});
    try db.deleteNode(&txn, d);
    // One it deleted does not.
    try std.testing.expectError(DatabaseError.NotFound, db.createEdgeAndGetId(&txn, a, d, "R"));
    try std.testing.expectError(DatabaseError.NotFound, db.createEdgeAndGetId(&txn, a, 4242, "R"));
    try db.commitTransaction(&txn);

    const out = try db.getOutgoingEdges(a);
    defer db.freeEdgeInfos(out);
    try std.testing.expectEqual(@as(usize, 1), out.len);
}
