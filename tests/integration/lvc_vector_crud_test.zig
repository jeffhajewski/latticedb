//! Removing a node's vector, inside a transaction and outside one.
//!
//! A node holds at most one vector. Until 0.18.0 nothing could take it away
//! again short of deleting the node: the overlay had a state for "no vector",
//! but only node deletion staged it.

const std = @import("std");
const lattice = @import("lattice");

const Database = lattice.storage.database.Database;
const NodeId = lattice.core.types.NodeId;

const dims = 4;
const near = [_]f32{ 1, 0, 0, 0 };
const far = [_]f32{ 0, 0, 0, 1 };

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
        .vector_metric = .euclidean,
    } });
}

fn hasVector(db: *Database, txn: ?*lattice.transaction.manager.Transaction, node_id: NodeId) !bool {
    const vector = (try db.getNodeVectorInTxn(txn, node_id)) orelse return false;
    db.allocator.free(vector);
    return true;
}

/// Whether a search wide enough to return every node returns `node_id`.
fn searchFinds(db: *Database, txn: ?*lattice.transaction.manager.Transaction, node_id: NodeId) !bool {
    const results = try db.vectorSearchInTxn(txn, &near, 10, 64);
    defer db.freeVectorSearchResults(results);
    for (results) |result| {
        if (result.node_id == node_id) return true;
    }
    return false;
}

test "lvc: removing a vector in a transaction" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lvc_vector_remove_txn.ltdb";
    cleanup(path);
    defer cleanup(path);

    var target: NodeId = undefined;
    var other: NodeId = undefined;
    {
        var db = try openDb(allocator, path, true);
        defer db.close();

        var setup = try db.beginTransaction(.read_write);
        target = try db.createNode(&setup, &.{"Doc"});
        try db.setNodeProperty(&setup, target, "title", .{ .string_val = "kept" });
        try db.setNodeVectorInTxn(&setup, target, &near);
        other = try db.createNode(&setup, &.{"Doc"});
        try db.setNodeVectorInTxn(&setup, other, &far);
        try db.commitTransaction(&setup);

        var txn = try db.beginTransaction(.read_write);
        try db.removeNodeVectorInTxn(&txn, target);
        // The transaction sees the removal before it commits...
        try std.testing.expect(!try hasVector(db, &txn, target));
        try std.testing.expect(!try searchFinds(db, &txn, target));
        try std.testing.expect(try searchFinds(db, &txn, other));
        // ...and nobody else does yet.
        try std.testing.expect(try hasVector(db, null, target));
        // Removing it again in the same transaction changes nothing.
        try db.removeNodeVectorInTxn(&txn, target);
        try db.commitTransaction(&txn);

        try std.testing.expect(!try hasVector(db, null, target));
        try std.testing.expect(!try searchFinds(db, null, target));
        try std.testing.expect(try searchFinds(db, null, other));
        // The node and its property stay.
        try std.testing.expect(try db.nodeExists(target));
        var title = (try db.getNodePropertyInTxn(null, target, "title")).?;
        defer title.deinit(db.allocator);
        try std.testing.expectEqualStrings("kept", title.string_val);

        // A node with no vector: removing succeeds and changes nothing.
        var again = try db.beginTransaction(.read_write);
        try db.removeNodeVectorInTxn(&again, target);
        try db.commitTransaction(&again);
        try std.testing.expect(!try hasVector(db, null, target));
        try std.testing.expect(try searchFinds(db, null, other));
    }

    // The file agrees once reopened, and the node can take a vector again.
    {
        var db = try openDb(allocator, path, false);
        defer db.close();
        try std.testing.expect(!try hasVector(db, null, target));
        try std.testing.expect(!try searchFinds(db, null, target));
        try std.testing.expect(try hasVector(db, null, other));

        var txn = try db.beginTransaction(.read_write);
        try db.setNodeVectorInTxn(&txn, target, &near);
        try db.commitTransaction(&txn);
        try std.testing.expect(try searchFinds(db, null, target));
    }
    var db = try openDb(allocator, path, false);
    defer db.close();
    try std.testing.expect(try hasVector(db, null, target));
    try std.testing.expect(try searchFinds(db, null, target));
}

test "lvc: a removal rolled back leaves the vector" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lvc_vector_remove_rollback.ltdb";
    cleanup(path);
    defer cleanup(path);

    var db = try openDb(allocator, path, true);
    defer db.close();
    var setup = try db.beginTransaction(.read_write);
    const target = try db.createNode(&setup, &.{"Doc"});
    try db.setNodeVectorInTxn(&setup, target, &near);
    try db.commitTransaction(&setup);

    var txn = try db.beginTransaction(.read_write);
    try db.removeNodeVectorInTxn(&txn, target);
    try db.abortTransaction(&txn);
    try std.testing.expect(try hasVector(db, null, target));
    try std.testing.expect(try searchFinds(db, null, target));
}

test "lvc: a vector set and removed in one transaction is never stored" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lvc_vector_set_then_remove.ltdb";
    cleanup(path);
    defer cleanup(path);

    var target: NodeId = undefined;
    {
        var db = try openDb(allocator, path, true);
        defer db.close();
        var txn = try db.beginTransaction(.read_write);
        target = try db.createNode(&txn, &.{"Doc"});
        try db.setNodeVectorInTxn(&txn, target, &near);
        try db.removeNodeVectorInTxn(&txn, target);
        try std.testing.expect(!try hasVector(db, &txn, target));
        try std.testing.expect(!try searchFinds(db, &txn, target));
        try db.commitTransaction(&txn);
        try std.testing.expect(!try hasVector(db, null, target));
    }
    var db = try openDb(allocator, path, false);
    defer db.close();
    try std.testing.expect(try db.nodeExists(target));
    try std.testing.expect(!try hasVector(db, null, target));
}

test "lvc: removing a vector outside a transaction" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lvc_vector_remove_direct.ltdb";
    cleanup(path);
    defer cleanup(path);

    var target: NodeId = undefined;
    var other: NodeId = undefined;
    {
        var db = try openDb(allocator, path, true);
        defer db.close();
        target = try db.createNode(null, &.{"Doc"});
        try db.setNodeVector(target, &near);
        other = try db.createNode(null, &.{"Doc"});
        try db.setNodeVector(other, &far);

        try db.removeNodeVectorInTxn(null, target);
        try std.testing.expect(!try hasVector(db, null, target));
        try std.testing.expect(!try searchFinds(db, null, target));
        try std.testing.expect(try searchFinds(db, null, other));
        // Idempotent.
        try db.removeNodeVector(target);
        try std.testing.expect(try db.nodeExists(target));
    }
    {
        var db = try openDb(allocator, path, false);
        defer db.close();
        try std.testing.expect(!try hasVector(db, null, target));
        try std.testing.expect(!try searchFinds(db, null, target));
        try std.testing.expect(try hasVector(db, null, other));
        try db.setNodeVector(target, &near);
        try std.testing.expect(try searchFinds(db, null, target));
    }
    var db = try openDb(allocator, path, false);
    defer db.close();
    try std.testing.expect(try hasVector(db, null, target));
}

test "lvc: removing a vector refuses a missing node and a read-only transaction" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lvc_vector_remove_errors.ltdb";
    cleanup(path);
    defer cleanup(path);

    var db = try openDb(allocator, path, true);
    defer db.close();
    var setup = try db.beginTransaction(.read_write);
    const target = try db.createNode(&setup, &.{"Doc"});
    try db.setNodeVectorInTxn(&setup, target, &near);
    try db.commitTransaction(&setup);

    var txn = try db.beginTransaction(.read_write);
    try std.testing.expectError(error.NotFound, db.removeNodeVectorInTxn(&txn, target + 1000));
    // A node this transaction deleted is missing to it.
    const doomed = try db.createNode(&txn, &.{"Doc"});
    try db.deleteNode(&txn, doomed);
    try std.testing.expectError(error.NotFound, db.removeNodeVectorInTxn(&txn, doomed));
    try db.abortTransaction(&txn);
    try std.testing.expectError(error.NotFound, db.removeNodeVectorInTxn(null, target + 1000));

    var reader = try db.beginTransaction(.read_only);
    try std.testing.expectError(error.TransactionReadOnly, db.removeNodeVectorInTxn(&reader, target));
    try db.abortTransaction(&reader);
    try std.testing.expect(try hasVector(db, null, target));
}

test "lvc: removing a vector on a database without a vector index does nothing" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lvc_vector_remove_noindex.ltdb";
    cleanup(path);
    defer cleanup(path);

    var db = try Database.open(allocator, path, .{ .create = true, .config = .{ .enable_fts = false } });
    defer db.close();
    var txn = try db.beginTransaction(.read_write);
    const node = try db.createNode(&txn, &.{"Doc"});
    try db.removeNodeVectorInTxn(&txn, node);
    try db.commitTransaction(&txn);
    try db.removeNodeVectorInTxn(null, node);
    try std.testing.expect(try db.nodeExists(node));
    try std.testing.expect(!try hasVector(db, null, node));
}
