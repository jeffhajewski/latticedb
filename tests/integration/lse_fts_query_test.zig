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

/// What the planner made of `cypher`: how many index seeks it let a LIMIT
/// bound, and how its last MATCH reads its first node.
const PlanFacts = struct {
    bounded_seeks: u32,
    scan: lattice.query.planner.QueryPlanner.ScanKind,
};

/// How many index seeks the planner let a LIMIT bound for `cypher`.
fn boundedSeeks(db: *Database, cypher: []const u8) !u32 {
    return (try planFacts(db, cypher)).bounded_seeks;
}

fn planFacts(db: *Database, cypher: []const u8) !PlanFacts {
    const allocator = std.testing.allocator;
    var parser = lattice.query.parser.Parser.init(allocator, cypher);
    defer parser.deinit();
    const parse_result = parser.parse();
    if (parse_result.query == null) return error.ParseFailed;

    var analyzer = lattice.query.semantic.SemanticAnalyzer.init(allocator);
    defer analyzer.deinit();
    const analysis = analyzer.analyze(parse_result.query.?);
    if (!analysis.success) return error.SemanticFailed;

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    var planner = lattice.query.planner.QueryPlanner.init(arena.allocator(), .{
        .node_tree = &db.node_tree,
        .label_index = &db.label_index,
        .edge_store = &db.edge_store,
        .symbol_table = &db.symbol_table,
        .hnsw_index = if (db.hnsw_index) |*hnsw| hnsw else null,
        .fts_index = if (db.fts_index) |*fts| fts else null,
        .database = db,
    });
    defer planner.deinit();
    const analysis_result = lattice.query.semantic.AnalysisResult{
        .success = true,
        .errors = &[_]lattice.query.semantic.SemanticError{},
        .variables = analysis.variables,
        .errors_dropped = false,
    };
    _ = try planner.plan(parse_result.query.?, &analysis_result);
    return .{ .bounded_seeks = planner.fts_bounded_seeks, .scan = planner.last_scan_kind };
}

/// The scores a query returned in its second column.
fn scoresOf(db: *Database, txn: ?*lattice.Transaction, cypher: []const u8, params: ?*const std.StringHashMap(lattice.core.types.PropertyValue), out: []f64) !usize {
    var result = if (params) |p| try db.queryWithParamsInTxn(txn, cypher, p) else try db.queryInTxn(txn, cypher);
    defer result.deinit();
    for (result.rows, 0..) |row, i| out[i] = row.values[1].float_val;
    return result.rows.len;
}

test "fts query: ORDER BY score() DESC with a LIMIT keeps only the best matches" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lse_fts_query_topk.ltdb";
    const db = try openDb(allocator, path);
    defer {
        db.close();
        removeFiles(path);
    }
    try db.createNodeFtsIndex("Doc", "text");
    // 120 documents that all say "common", as often and in texts as long
    // as makes their scores differ.
    var buf: [512]u8 = undefined;
    for (0..120) |i| {
        var w: std.Io.Writer = .fixed(&buf);
        for (0..i % 7 + 1) |_| try w.writeAll("common ");
        for (0..i % 11) |j| try w.print("filler{d} ", .{j});
        const id = try db.createNode(null, &[_][]const u8{"Doc"});
        try db.setNodeProperty(null, id, "text", .{ .string_val = w.buffered() });
        try db.setNodeProperty(null, id, "i", .{ .int_val = @intCast(i) });
    }

    var all: [200]f64 = undefined;
    const n = try scoresOf(db, null, "MATCH (d:Doc) WHERE d.text @@ 'common' RETURN d.i, score(d) ORDER BY score(d) DESC", null, &all);
    try std.testing.expectEqual(@as(usize, 120), n);

    var params = std.StringHashMap(lattice.core.types.PropertyValue).init(allocator);
    defer params.deinit();
    try params.put("k", .{ .int_val = 4 });
    const bounded = [_]struct { cypher: []const u8, skip: usize, count: usize }{
        .{ .cypher = "MATCH (d:Doc) WHERE d.text @@ 'common' RETURN d.i, score(d) ORDER BY score(d) DESC LIMIT 5", .skip = 0, .count = 5 },
        .{ .cypher = "MATCH (d:Doc) WHERE d.text @@ 'common' RETURN d.i, score(d) AS s ORDER BY s DESC SKIP 7 LIMIT 3", .skip = 7, .count = 3 },
        .{ .cypher = "MATCH (d:Doc) WHERE d.text @@ 'common' RETURN d.i, score(d) ORDER BY score(d) DESC LIMIT $k", .skip = 0, .count = 4 },
        // With no ORDER BY the seek's own order, best first, reaches the LIMIT.
        .{ .cypher = "MATCH (d:Doc) WHERE d.text @@ 'common' RETURN d.i, score(d) LIMIT 6", .skip = 0, .count = 6 },
    };
    for (bounded) |case| {
        try std.testing.expectEqual(@as(u32, 1), try boundedSeeks(db, case.cypher));
        var got: [200]f64 = undefined;
        const m = try scoresOf(db, null, case.cypher, &params, &got);
        try std.testing.expectEqual(case.count, m);
        for (got[0..m], all[case.skip .. case.skip + m]) |g, want| try std.testing.expectApproxEqAbs(want, g, 1e-6);
    }

    // Orders the seek does not follow, and rows the LIMIT does not take one
    // for one, leave the search unbounded.
    const unbounded = [_][]const u8{
        "MATCH (d:Doc) WHERE d.text @@ 'common' RETURN d.i, score(d) ORDER BY score(d) LIMIT 5",
        "MATCH (d:Doc) WHERE d.text @@ 'common' RETURN d.i, score(d) ORDER BY d.i DESC LIMIT 5",
        "MATCH (d:Doc) WHERE d.text @@ 'common' RETURN DISTINCT d.i, score(d) LIMIT 5",
        "MATCH (d:Doc) WHERE d.text @@ 'common' AND d.i > 3 RETURN d.i, score(d) LIMIT 5",
        "MATCH (d:Doc) WHERE d.text @@ 'common' RETURN d.i, score(d) ORDER BY score(d) DESC",
    };
    for (unbounded) |cypher| try std.testing.expectEqual(@as(u32, 0), try boundedSeeks(db, cypher));

    // Inside a transaction, documents it wrote compete on the same terms.
    var txn = try db.beginTransaction(.read_write);
    defer db.abortTransaction(&txn) catch {};
    const best = try db.createNode(&txn, &[_][]const u8{"Doc"});
    try db.setNodeProperty(&txn, best, "text", .{ .string_val = "common common common common common common common common" });
    try db.setNodeProperty(&txn, best, "i", .{ .int_val = 1000 });
    var in_txn: [200]f64 = undefined;
    const t = try scoresOf(db, &txn, "MATCH (d:Doc) WHERE d.text @@ 'common' RETURN d.i, score(d) ORDER BY score(d) DESC", null, &in_txn);
    try std.testing.expectEqual(@as(usize, 121), t);
    var top: [200]f64 = undefined;
    const m = try scoresOf(db, &txn, "MATCH (d:Doc) WHERE d.text @@ 'common' RETURN d.i, score(d) ORDER BY score(d) DESC LIMIT 3", null, &top);
    try std.testing.expectEqual(@as(usize, 3), m);
    for (top[0..3], in_txn[0..3]) |g, want| try std.testing.expectApproxEqAbs(want, g, 1e-6);
}

test "fts query: text @@ a query of only exclusions fails, as it does against an index" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lse_fts_query_literal.ltdb";
    const db = try openDb(allocator, path);
    defer {
        db.close();
        removeFiles(path);
    }
    var yes = try db.query("RETURN 'the quick fox' @@ 'fox -dog'");
    defer yes.deinit();
    try std.testing.expect(yes.rows[0].values[0].bool_val);
    // Before, a refused query read as null, so a WHERE dropped every row
    // and the query succeeded.
    try std.testing.expectError(error.ExecutionError, db.query("RETURN 'the quick fox' @@ '-dog'"));
}

/// The rows of a query, as (rank, score) pairs.
fn rankScores(db: *Database, cypher: []const u8, out: [][2]f64) !usize {
    var result = try db.query(cypher);
    defer result.deinit();
    for (result.rows, 0..) |row, i| {
        out[i][0] = @floatFromInt(row.values[0].int_val);
        out[i][1] = switch (row.values[1]) {
            .float_val => |f| f,
            else => -1,
        };
    }
    return result.rows.len;
}

test "fts query: a range on an indexed property beside @@ answers and scores as without the index" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lse_fts_query_range.ltdb";
    const db = try openDb(allocator, path);
    defer {
        db.close();
        removeFiles(path);
    }
    try db.createNodeFtsIndex("Doc", "text");
    var buf: [512]u8 = undefined;
    for (0..200) |i| {
        var w: std.Io.Writer = .fixed(&buf);
        for (0..i % 7 + 1) |_| try w.writeAll("common ");
        if (i % 5 == 0) try w.writeAll("rare ");
        for (0..i % 11) |j| try w.print("filler{d} ", .{j});
        const id = try db.createNode(null, &[_][]const u8{"Doc"});
        try db.setNodeProperty(null, id, "text", .{ .string_val = w.buffered() });
        try db.setNodeProperty(null, id, "rank", .{ .int_val = @intCast(i) });
    }

    const cases = [_][]const u8{
        "MATCH (d:Doc) WHERE d.rank >= 50 AND d.text @@ 'common' RETURN d.rank, score(d) ORDER BY score(d) DESC LIMIT 5",
        "MATCH (d:Doc) WHERE d.text @@ 'common' AND d.rank < 120 AND d.rank > 30 RETURN d.rank, score(d) ORDER BY score(d) DESC LIMIT 5",
        "MATCH (d:Doc) WHERE d.rank >= 50 AND d.text @@ 'common' RETURN d.rank, score(d) ORDER BY score(d) DESC SKIP 3 LIMIT 4",
        "MATCH (d:Doc) WHERE d.rank >= 50 AND (d.text @@ 'rare' OR d.text @@ 'filler9') RETURN d.rank, score(d) ORDER BY score(d) DESC LIMIT 5",
        "MATCH (d:Doc) WHERE d.rank >= 50 AND d.text @@ 'common' RETURN d.rank, score(d) ORDER BY d.rank LIMIT 5",
    };
    var want: [cases.len][16][2]f64 = undefined;
    var want_len: [cases.len]usize = undefined;
    var want_plan: [cases.len]PlanFacts = undefined;
    for (cases, 0..) |cypher, c| {
        want_len[c] = try rankScores(db, cypher, &want[c]);
        want_plan[c] = try planFacts(db, cypher);
    }

    try db.createNodePropertyIndex("Doc", "rank");
    for (cases, 0..) |cypher, c| {
        var got: [16][2]f64 = undefined;
        const n = try rankScores(db, cypher, &got);
        const plan = try planFacts(db, cypher);
        try std.testing.expectEqual(want_len[c], n);
        try std.testing.expect(n > 0);
        for (got[0..n], want[c][0..n]) |g, w| {
            try std.testing.expectEqual(w[0], g[0]);
            try std.testing.expectApproxEqAbs(w[1], g[1], 1e-6);
            try std.testing.expect(g[1] > 0);
        }
        // A LIMIT bounds the seek the same way with the index or without.
        try std.testing.expectEqual(want_plan[c].bounded_seeks, plan.bounded_seeks);
        // An OR of searches beside the range leaves the range to the index,
        // and the searches still score what they match.
        if (std.mem.indexOf(u8, cypher, " OR ") != null) {
            try std.testing.expectEqual(.property_range_scan, plan.scan);
        }
    }
}

test "fts query: a fuzzy match at the full edit distance scores, below closer ones" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lse_fts_query_fuzzy_edge.ltdb";
    const db = try openDb(allocator, path);
    defer {
        db.close();
        removeFiles(path);
    }
    try db.createNodeFtsIndex("Doc", "text");
    // One word each, each in one document: only the edit distance from
    // "zorbex" tells their scores apart. The last is three edits away.
    const words = [_][]const u8{ "zorbex", "zorbix", "zarbix", "zxrbiy" };
    var ids: [words.len]u64 = undefined;
    for (words, &ids) |word, *id| {
        id.* = try db.createNode(null, &[_][]const u8{"Doc"});
        try db.setNodeProperty(null, id.*, "text", .{ .string_val = word });
    }

    var result = try db.query("MATCH (d:Doc) WHERE d.text @@ 'zorbex~2' RETURN id(d), score(d) ORDER BY score(d) DESC");
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 3), result.rows.len);
    var previous = std.math.inf(f64);
    for (result.rows, ids[0..3]) |row, id| {
        try std.testing.expectEqual(id, @as(u64, @intCast(row.values[0].int_val)));
        const score = row.values[1].float_val;
        try std.testing.expect(score > 0);
        try std.testing.expect(score < previous);
        previous = score;
    }

    // The direct search gives the same scores.
    const direct = try db.ftsSearchIndexFuzzy(null, .node, "Doc", "text", "zorbex", 10, 2, 0);
    defer db.freeFtsSearchResults(direct);
    try std.testing.expectEqual(@as(usize, 3), direct.len);
    for (direct, result.rows) |hit, row| {
        try std.testing.expectApproxEqAbs(row.values[1].float_val, @as(f64, hit.score), 1e-6);
    }
}
