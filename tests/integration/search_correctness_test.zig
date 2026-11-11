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

test "search: full-text AND with a word no document has matches nothing" {
    const allocator = std.testing.allocator;
    const db = try openFtsDatabase(allocator);
    defer db.close();

    _ = try createDoc(db, null, "banana");
    _ = try createDoc(db, null, "apple banana");
    _ = try createDoc(db, null, "apple");

    const results = try db.ftsSearchIndex(.node, "Doc", "text", "apple zebra", 10);
    defer freeFtsResults(allocator, results);
    try std.testing.expectEqual(@as(usize, 0), results.len);
}

test "search: vector search returns k results when k exceeds ef_search" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_vector_k_exceeds_ef_test.ltdb";
    @import("compat").fs.cwd().deleteFile(path) catch {};
    defer @import("compat").fs.cwd().deleteFile(path) catch {};

    const dims = 16;
    const db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{ .enable_wal = false, .enable_fts = false, .enable_vector = true, .vector_dimensions = dims },
    });
    defer db.close();

    var prng = std.Random.DefaultPrng.init(0x5eed);
    const random = prng.random();
    var vec: [dims]f32 = undefined;
    for (0..300) |_| {
        for (&vec) |*x| x.* = random.float(f32) * 2.0 - 1.0;
        const node = try db.createNode(null, &[_][]const u8{"Embedding"});
        try db.setNodeVector(node, &vec);
    }

    for (&vec) |*x| x.* = random.float(f32) * 2.0 - 1.0;

    // The default ef_search is 64, and a search used to stop at ef results
    // however many were asked for.
    const ef_cases = [_]?u16{ null, 16, 64, 200 };
    for (ef_cases) |ef| {
        const results = try db.vectorSearch(&vec, 100, ef);
        defer db.freeVectorSearchResults(results);
        try std.testing.expectEqual(@as(usize, 100), results.len);
        for (results[1..], results[0 .. results.len - 1]) |later, earlier| {
            try std.testing.expect(earlier.distance <= later.distance);
        }
    }
}

/// Write `count` papers in one transaction: every even one mentions both
/// "knowledge" and "graphs", the rest only one of the two.
fn writePapersInOneTransaction(db: *Database, ids: []u64) !void {
    var txn = try db.beginTransaction(.read_write);
    for (ids, 0..) |*id, i| {
        const text = if (i % 2 == 0)
            "Knowledge graphs for retrieval. We build knowledge graphs from papers."
        else if (i % 4 == 1)
            "Knowledge distillation for small language models."
        else
            "Graphs of citations between papers.";
        id.* = try db.createNode(&txn, &[_][]const u8{"Paper"});
        try db.setNodeProperty(&txn, id.*, "key", .{ .int_val = @intCast(i) });
        try db.setNodeProperty(&txn, id.*, "text", .{ .string_val = text });
    }
    try db.commitTransaction(&txn);
}

fn expectEvenPapersFound(allocator: std.mem.Allocator, db: *Database, ids: []const u64) !void {
    const results = try db.ftsSearchIndex(.node, "Paper", "text", "knowledge graphs", 100);
    defer freeFtsResults(allocator, results);
    try std.testing.expectEqual(ids.len / 2, results.len);
    for (ids, 0..) |id, i| {
        try std.testing.expectEqual(i % 2 == 0, ftsContains(results, id));
    }

    // The Cypher predicate goes through the same index search.
    var rows = try db.query("MATCH (p:Paper) WHERE p.text @@ 'knowledge graphs' RETURN p.key AS k");
    defer rows.deinit();
    try std.testing.expectEqual(ids.len / 2, rows.rowCount());
    for (rows.rows) |row| {
        const key = switch (row.values[0]) {
            .int_val => |v| v,
            else => return error.TestUnexpectedResult,
        };
        try std.testing.expectEqual(@as(i64, 0), @mod(key, 2));
    }
}

test "search: full-text AND over papers written by one transaction, index declared first" {
    const allocator = std.testing.allocator;
    const db = try Database.open(allocator, ":memory:", .{
        .create = true,
        .config = .{ .enable_fts = true, .enable_vector = false },
    });
    defer db.close();
    try db.createNodeFtsIndex("Paper", "text");

    // The commit applies the transaction's nodes in hash-map order, so their
    // postings are appended out of doc_id order.
    var ids: [40]u64 = undefined;
    try writePapersInOneTransaction(db, &ids);
    try expectEvenPapersFound(allocator, db, &ids);
}

test "search: full-text AND over papers written by one transaction, index declared after" {
    const allocator = std.testing.allocator;
    const db = try Database.open(allocator, ":memory:", .{
        .create = true,
        .config = .{ .enable_fts = true, .enable_vector = false },
    });
    defer db.close();

    var ids: [40]u64 = undefined;
    try writePapersInOneTransaction(db, &ids);
    try db.createNodeFtsIndex("Paper", "text");
    try expectEvenPapersFound(allocator, db, &ids);
}
