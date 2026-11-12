//! The full-text query language means the same wherever it is asked: the
//! C-API-shaped search, `@@` through an index scan, `@@` checked row by row
//! inside a larger condition, text a transaction has not committed, and
//! `"text" @@ "query"` with no index at all.

const std = @import("std");
const lattice = @import("lattice");

const Database = lattice.storage.database.Database;

const texts = [_][]const u8{
    "the quick brown fox jumps over the lazy dog",
    "a quick red fox and a slow brown bear",
    "databases store graphs and documents",
    "a graph database stores nodes and edges",
    "brown bread with butter",
    "network latency and packet loss",
};

const queries = [_][]const u8{
    "quick",            "quick fox",             "quick OR database",      "fox -dog",
    "\"quick brown\"",  "brown -\"brown bear\"", "data*",                  "databse~",
    "fox dog OR bread", "graph* -edges",         "packet~ network OR qu*",
};

fn removeFiles(comptime path: []const u8) void {
    @import("compat").fs.cwd().deleteFile(path) catch {};
    @import("compat").fs.cwd().deleteFile(path ++ "-wal") catch {};
}

fn openDb(allocator: std.mem.Allocator, comptime path: []const u8) !*Database {
    removeFiles(path);
    return Database.open(allocator, path, .{
        .create = true,
        .config = .{ .enable_wal = true, .enable_fts = true, .enable_vector = false },
    });
}

/// Which of `ids` a Cypher query returned, as a bit set over their order.
fn cypherMatches(db: *Database, txn: ?*lattice.Transaction, cypher: []const u8, ids: []const u64) !u32 {
    var result = try db.queryInTxn(txn, cypher);
    defer result.deinit();
    var set: u32 = 0;
    for (result.rows) |row| {
        const id: u64 = switch (row.values[0]) {
            .int_val => |v| @intCast(v),
            else => return error.UnexpectedValueType,
        };
        for (ids, 0..) |want, i| {
            if (want == id) set |= @as(u32, 1) << @intCast(i);
        }
    }
    return set;
}

test "fts query: every path answers each query form the same way" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lse_fts_query_paths.ltdb";
    const db = try openDb(allocator, path);
    defer {
        db.close();
        removeFiles(path);
    }
    try db.createNodeFtsIndex("Doc", "text");

    var ids: [texts.len]u64 = undefined;
    for (texts, &ids) |text, *id| {
        id.* = try db.createNode(null, &[_][]const u8{"Doc"});
        try db.setNodeProperty(null, id.*, "text", .{ .string_val = text });
        try db.setNodeProperty(null, id.*, "n", .{ .int_val = 1 });
    }

    for (queries) |q| {
        // What the text alone says, with no index behind it.
        var want: u32 = 0;
        for (texts, 0..) |text, i| {
            if (try lattice.fts.query.textMatches(allocator, q, text, .{})) want |= @as(u32, 1) << @intCast(i);
        }

        // The search the C API runs.
        const found = try db.ftsSearchIndex(.node, "Doc", "text", q, 100);
        defer db.freeFtsSearchResults(found);
        var searched: u32 = 0;
        for (found) |r| {
            for (ids, 0..) |id, i| {
                if (id == r.doc_id) searched |= @as(u32, 1) << @intCast(i);
            }
        }

        var buf: [256]u8 = undefined;
        // Escape the query as a Cypher string literal.
        var lit_buf: [128]u8 = undefined;
        var lit: std.Io.Writer = .fixed(&lit_buf);
        for (q) |c| {
            if (c == '"' or c == '\\') try lit.writeByte('\\');
            try lit.writeByte(c);
        }
        const seek = try std.fmt.bufPrint(&buf, "MATCH (d:Doc) WHERE d.text @@ \"{s}\" RETURN id(d)", .{lit.buffered()});
        const by_seek = try cypherMatches(db, null, seek, &ids);
        var buf2: [256]u8 = undefined;
        // An OR with another condition is checked row by row.
        const per_row = try std.fmt.bufPrint(&buf2, "MATCH (d:Doc) WHERE d.text @@ \"{s}\" OR d.n < 0 RETURN id(d)", .{lit.buffered()});
        const by_row = try cypherMatches(db, null, per_row, &ids);

        if (searched != want or by_seek != want or by_row != want) {
            std.debug.print("query \"{s}\": text {b}, search {b}, seek {b}, per-row {b}\n", .{ q, want, searched, by_seek, by_row });
            return error.TestUnexpectedResult;
        }

        // Text a transaction has written and not committed.
        var txn = try db.beginTransaction(.read_write);
        defer db.abortTransaction(&txn) catch {};
        var pending_ids: [texts.len]u64 = undefined;
        for (texts, &pending_ids) |text, *id| {
            id.* = try db.createNode(&txn, &[_][]const u8{"Doc"});
            try db.setNodeProperty(&txn, id.*, "text", .{ .string_val = text });
        }
        const in_txn = try db.ftsSearchIndexInTxn(&txn, .node, "Doc", "text", q, 100);
        defer db.freeFtsSearchResults(in_txn);
        var pending: u32 = 0;
        var committed: u32 = 0;
        for (in_txn) |r| {
            for (pending_ids, 0..) |id, i| {
                if (id == r.doc_id) pending |= @as(u32, 1) << @intCast(i);
            }
            for (ids, 0..) |id, i| {
                if (id == r.doc_id) committed |= @as(u32, 1) << @intCast(i);
            }
        }
        if (pending != want or committed != want) {
            std.debug.print("query \"{s}\" in a transaction: text {b}, pending {b}, committed {b}\n", .{ q, want, pending, committed });
            return error.TestUnexpectedResult;
        }
        // Each pending copy scores as its committed twin does, but for the
        // statistics its own document would add.
        for (in_txn) |r| {
            for (pending_ids, ids) |pid, cid| {
                if (pid != r.doc_id) continue;
                const twin = for (in_txn) |o| {
                    if (o.doc_id == cid) break o.score;
                } else unreachable;
                try std.testing.expectApproxEqRel(twin, r.score, 1e-4);
            }
        }
    }

    // Only exclusions is refused by the search, not answered with nothing.
    try std.testing.expectError(error.InvalidArgument, db.ftsSearchIndex(.node, "Doc", "text", "-fox", 10));
}

test "fts query: committing a document changes its score only as the statistics change" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lse_fts_query_commit.ltdb";
    const db = try openDb(allocator, path);
    defer {
        db.close();
        removeFiles(path);
    }
    try db.createNodeFtsIndex("Doc", "text");
    for (texts) |text| {
        const id = try db.createNode(null, &[_][]const u8{"Doc"});
        try db.setNodeProperty(null, id, "text", .{ .string_val = text });
    }

    var txn = try db.beginTransaction(.read_write);
    const added = try db.createNode(&txn, &[_][]const u8{"Doc"});
    // Both words, in a long text, so BM25 puts it below the shorter
    // committed documents that hold them.
    try db.setNodeProperty(&txn, added, "text", .{ .string_val = "quick brown " ++ "filler words that say nothing much " ** 6 });
    const before = try db.ftsSearchIndexInTxn(&txn, .node, "Doc", "text", "quick brown", 100);
    defer db.freeFtsSearchResults(before);
    try db.commitTransaction(&txn);
    const after = try db.ftsSearchIndex(.node, "Doc", "text", "quick brown", 100);
    defer db.freeFtsSearchResults(after);

    // The same documents in the same order. Before, the pending document
    // scored the number of query words (2) against BM25 scores below 2, so
    // it came first until the commit, and then dropped to where BM25 put it.
    try std.testing.expect(before.len >= 2);
    try std.testing.expectEqual(before.len, after.len);
    for (before, after) |b, a| try std.testing.expectEqual(b.doc_id, a.doc_id);
}
