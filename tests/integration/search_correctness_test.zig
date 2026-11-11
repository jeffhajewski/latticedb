//! Search results that must be complete and correct, end to end.
//!
//! Each test here pins down a case where search used to return the wrong set
//! of rows: missing matches, extra matches, or fewer results than asked for.

const std = @import("std");
const lattice = @import("lattice");

const Database = lattice.storage.database.Database;

fn freeFtsResults(allocator: std.mem.Allocator, results: []lattice.ScoredDoc) void {
    if (results.len == 0) return;
    allocator.free(results);
}

fn ftsContains(results: []const lattice.ScoredDoc, doc_id: u64) bool {
    for (results) |result| {
        if (result.doc_id == doc_id) return true;
    }
    return false;
}

fn openFtsDatabase(allocator: std.mem.Allocator) !*Database {
    const db = try Database.open(allocator, ":memory:", .{
        .create = true,
        .config = .{ .enable_fts = true, .enable_vector = false },
    });
    errdefer db.close();
    try db.createNodeFtsIndex("Doc", "text");
    return db;
}

fn createDoc(db: *Database, txn: ?*lattice.Transaction, text: []const u8) !u64 {
    const node = try db.createNode(txn, &[_][]const u8{"Doc"});
    try db.setNodeProperty(txn, node, "text", .{ .string_val = text });
    return node;
}

test "search: full-text AND finds a document whose text was updated" {
    const allocator = std.testing.allocator;
    const db = try openFtsDatabase(allocator);
    defer db.close();

    const doc1 = try createDoc(db, null, "banana");
    const doc2 = try createDoc(db, null, "apple banana");
    _ = try createDoc(db, null, "apple");

    // The update re-indexes doc1 after doc3, so the posting list for "apple"
    // no longer runs in doc_id order.
    try db.setNodeProperty(null, doc1, "text", .{ .string_val = "apple banana" });

    const results = try db.ftsSearchIndex(.node, "Doc", "text", "apple banana", 10);
    defer freeFtsResults(allocator, results);
    try std.testing.expectEqual(@as(usize, 2), results.len);
    try std.testing.expect(ftsContains(results, doc1));
    try std.testing.expect(ftsContains(results, doc2));
}

test "search: full-text AND finds every document one transaction wrote" {
    const allocator = std.testing.allocator;
    const db = try openFtsDatabase(allocator);
    defer db.close();

    // One transaction commits its documents in whatever order its pending
    // state iterates, not necessarily in doc_id order.
    var ids: [64]u64 = undefined;
    var txn = try db.beginTransaction(.read_write);
    for (&ids, 0..) |*id, i| {
        id.* = try createDoc(db, &txn, if (i % 2 == 0) "apple banana" else "apple");
    }
    try db.commitTransaction(&txn);

    const results = try db.ftsSearchIndex(.node, "Doc", "text", "banana apple", 100);
    defer freeFtsResults(allocator, results);
    try std.testing.expectEqual(@as(usize, ids.len / 2), results.len);
    for (ids, 0..) |id, i| {
        try std.testing.expectEqual(i % 2 == 0, ftsContains(results, id));
    }
}
