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

test "lgr: setting and removing properties in a transaction changes only that property" {
    const path = "/tmp/lgr_txn_property_edits.ltdb";
    defer removeFiles(path);
    var db = try openDb(path, true);
    defer db.close();
    const allocator = std.testing.allocator;

    var txn = try db.beginTransaction(.read_write);
    const n = try db.createNode(&txn, &.{"P"});
    const m = try db.createNode(&txn, &.{"P"});
    const e = try db.createEdgeAndGetId(&txn, n, m, "R");
    for ([_][]const u8{ "a", "b", "c" }, 0..) |key, i| {
        try db.setNodeProperty(&txn, n, key, .{ .int_val = @intCast(i) });
        try db.setEdgePropertyById(&txn, e, key, .{ .int_val = @intCast(i) });
    }
    try db.commitTransaction(&txn);

    txn = try db.beginTransaction(.read_write);
    // Overwrite, add, remove, and remove something absent, on committed
    // records and then again on the transaction's own copies.
    try db.setNodeProperty(&txn, n, "b", .{ .string_val = "bee" });
    try db.setNodeProperty(&txn, n, "d", .{ .int_val = 3 });
    try db.removeNodeProperty(&txn, n, "a");
    try db.removeNodeProperty(&txn, n, "zz");
    try db.setNodeProperty(&txn, n, "b", .{ .string_val = "bumble" });
    try db.setEdgePropertyById(&txn, e, "c", .{ .float_val = 1.5 });
    try db.removeEdgePropertyById(&txn, e, "a");
    try db.setEdgePropertyById(&txn, e, "e", .{ .bool_val = true });
    try db.removeEdgePropertyById(&txn, e, "missing");
    try db.commitTransaction(&txn);

    const props = try db.getNodeProperties(n);
    defer db.freePropertyEntries(props);
    try std.testing.expectEqual(@as(usize, 3), props.len);
    try std.testing.expectEqualStrings("b", props[0].key);
    try std.testing.expectEqualStrings("bumble", props[0].value.string_val);
    try std.testing.expectEqualStrings("c", props[1].key);
    try std.testing.expectEqual(@as(i64, 2), props[1].value.int_val);
    try std.testing.expectEqualStrings("d", props[2].key);

    var edge = try db.edge_store.getById(e);
    defer edge.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 3), edge.properties.len);
    try std.testing.expectEqual(@as(f64, 1.5), edge.properties[1].value.float_val);
    try std.testing.expect(edge.properties[2].value.bool_val);

    // A node or edge that does not exist is still refused.
    txn = try db.beginTransaction(.read_write);
    try std.testing.expectError(DatabaseError.NotFound, db.setNodeProperty(&txn, 999, "a", .{ .int_val = 1 }));
    try std.testing.expectError(DatabaseError.NotFound, db.removeEdgePropertyById(&txn, 999, "a"));
    try db.abortTransaction(&txn);
}
