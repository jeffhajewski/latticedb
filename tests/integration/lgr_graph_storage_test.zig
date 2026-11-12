//! Graph storage: what an edge property update writes, ordered property
//! indexes and the scans built on them, and type-only relationship patterns.

const std = @import("std");
const lattice = @import("lattice");

const Database = lattice.storage.database.Database;
const DatabaseError = lattice.storage.database.DatabaseError;
const QueryResult = lattice.storage.database.QueryResult;
const PropertyValue = lattice.core.types.PropertyValue;
const BTree = lattice.storage.btree.BTree;

fn removeFiles(path: []const u8) void {
    const cwd = @import("compat").fs.cwd();
    cwd.deleteFile(path) catch {};
    var buf: [256]u8 = undefined;
    const wal = std.fmt.bufPrint(&buf, "{s}-wal", .{path}) catch return;
    cwd.deleteFile(wal) catch {};
    const journal = std.fmt.bufPrint(&buf, "{s}-journal", .{path}) catch return;
    cwd.deleteFile(journal) catch {};
}

fn openDb(path: []const u8, wal: bool) !*Database {
    removeFiles(path);
    return Database.open(std.testing.allocator, path, .{
        .create = true,
        .config = .{ .enable_wal = wal, .enable_fts = false, .enable_vector = false },
    });
}

fn reopenDb(path: []const u8) !*Database {
    return Database.open(std.testing.allocator, path, .{
        .config = .{ .enable_fts = false, .enable_vector = false },
    });
}

/// Whether the root page of `tree` is dirty in the pool. The trees in these
/// tests are one page, so that is the whole tree.
fn treeHasDirtyPage(db: *Database, tree: *BTree) !bool {
    const frame = db.buffer_pool.getPageIfPresent(tree.getRootPage()) orelse return false;
    return frame.dirty;
}

test "lgr: an edge property update leaves the traversal keys alone" {
    const path = "/tmp/lgr_edge_update_keys.ltdb";
    defer removeFiles(path);
    var db = try openDb(path, true);
    defer db.close();

    var txn = try db.beginTransaction(.read_write);
    const a = try db.createNode(&txn, &.{"P"});
    const b = try db.createNode(&txn, &.{"P"});
    const edge_id = try db.createEdgeAndGetId(&txn, a, b, "KNOWS");
    try db.setEdgePropertyById(&txn, edge_id, "w", .{ .int_val = 1 });
    try db.commitTransaction(&txn);

    // Everything on disk, so a page dirtied from here on was written by
    // the update.
    try db.sync();
    try std.testing.expect(!try treeHasDirtyPage(db, &db.edge_tree));

    txn = try db.beginTransaction(.read_write);
    try db.setEdgePropertyById(&txn, edge_id, "w", .{ .int_val = 2 });
    try db.setEdgePropertyById(&txn, edge_id, "note", .{ .string_val = "x" });
    try db.commitTransaction(&txn);

    try std.testing.expect(!try treeHasDirtyPage(db, &db.edge_tree));
    try std.testing.expect(try treeHasDirtyPage(db, &db.edge_id_tree));

    var edge = try db.edge_store.getById(edge_id);
    defer edge.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 2), edge.properties.len);
    try std.testing.expectEqual(a, edge.source);
    try std.testing.expectEqual(b, edge.target);

    // The edge is still found from both ends.
    const out = try db.getOutgoingEdgeRefsInTxn(null, a);
    defer std.testing.allocator.free(out);
    try std.testing.expectEqual(@as(usize, 1), out.len);
    try std.testing.expectEqual(edge_id, out[0].id);
    const in = try db.getIncomingEdgeRefsInTxn(null, b);
    defer std.testing.allocator.free(in);
    try std.testing.expectEqual(@as(usize, 1), in.len);
}

test "lgr: edge property updates and removals survive recovery" {
    const path = "/tmp/lgr_edge_update_recovery.ltdb";
    defer removeFiles(path);
    var edge_id: u64 = 0;
    var a: u64 = 0;
    {
        var db = try openDb(path, true);
        var txn = try db.beginTransaction(.read_write);
        a = try db.createNode(&txn, &.{"P"});
        const b = try db.createNode(&txn, &.{"P"});
        edge_id = try db.createEdgeAndGetId(&txn, a, b, "KNOWS");
        try db.setEdgePropertyById(&txn, edge_id, "w", .{ .int_val = 1 });
        try db.setEdgePropertyById(&txn, edge_id, "gone", .{ .int_val = 9 });
        try db.commitTransaction(&txn);
        try db.sync();

        txn = try db.beginTransaction(.read_write);
        try db.setEdgePropertyById(&txn, edge_id, "w", .{ .int_val = 7 });
        try db.removeEdgePropertyById(&txn, edge_id, "gone");
        // The commit reaches the log and is not applied: recovery redoes it.
        db.fail_next_apply_for_testing = true;
        try std.testing.expectError(error.IoError, db.commitTransaction(&txn));
        db.close();
    }
    var db = try reopenDb(path);
    defer db.close();
    var edge = try db.edge_store.getById(edge_id);
    defer edge.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), edge.properties.len);
    try std.testing.expectEqual(@as(i64, 7), edge.properties[0].value.int_val);
    const out = try db.getOutgoingEdgeRefsInTxn(null, a);
    defer std.testing.allocator.free(out);
    try std.testing.expectEqual(@as(usize, 1), out.len);
}

test "lgr: an edge property update without a transaction rewrites the record only" {
    const path = "/tmp/lgr_edge_update_direct.ltdb";
    defer removeFiles(path);
    var db = try openDb(path, false);
    defer db.close();

    const a = try db.createNode(null, &.{"P"});
    const b = try db.createNode(null, &.{"P"});
    const edge_id = try db.createEdgeAndGetId(null, a, b, "KNOWS");
    try db.setEdgePropertyById(null, edge_id, "w", .{ .int_val = 1 });
    try db.sync();
    try std.testing.expect(!try treeHasDirtyPage(db, &db.edge_tree));

    try db.setEdgePropertyById(null, edge_id, "w", .{ .int_val = 2 });
    try db.removeEdgePropertyById(null, edge_id, "w");
    try std.testing.expect(!try treeHasDirtyPage(db, &db.edge_tree));

    var edge = try db.edge_store.getById(edge_id);
    defer edge.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 0), edge.properties.len);
}
