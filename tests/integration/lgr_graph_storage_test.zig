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

test "lgr: an edge record with an unknown value tag is refused, not read as null" {
    const path = "/tmp/lgr_edge_unknown_tag.ltdb";
    defer removeFiles(path);
    var db = try openDb(path, false);
    defer db.close();

    const a = try db.createNode(null, &.{"P"});
    const b = try db.createNode(null, &.{"P"});
    const edge_id = try db.createEdgeAndGetId(null, a, b, "R");
    const key_id = try db.symbol_table.intern("w");

    // source, target, type, one property: key id, then tag 0x2a, which no
    // writer produces.
    var record: [8 + 8 + 2 + 2 + 2 + 1]u8 = undefined;
    std.mem.writeInt(u64, record[0..8], a, .little);
    std.mem.writeInt(u64, record[8..16], b, .little);
    std.mem.writeInt(u16, record[16..18], try db.symbol_table.lookup("R"), .little);
    std.mem.writeInt(u16, record[18..20], 1, .little);
    std.mem.writeInt(u16, record[20..22], key_id, .little);
    record[22] = 0x2a;
    var id_key: [8]u8 = undefined;
    std.mem.writeInt(u64, &id_key, edge_id, .little);
    try db.edge_store.edge_id_index.replace(&id_key, &record);

    try std.testing.expectError(lattice.graph.edge.EdgeError.InvalidData, db.edge_store.getById(edge_id));
}

fn expectIdsAscending(result: QueryResult, expected_rows: usize) !void {
    try std.testing.expectEqual(expected_rows, result.rows.len);
    var previous: ?u64 = null;
    for (result.rows) |row| {
        const id = row.values[0].node_id;
        if (previous) |p| try std.testing.expect(id > p);
        previous = id;
    }
}

test "lgr: MATCH (n) returns nodes in id order, as a label scan does" {
    const path = "/tmp/lgr_all_nodes_order.ltdb";
    defer removeFiles(path);
    var db = try openDb(path, true);
    defer db.close();

    var txn = try db.beginTransaction(.read_write);
    var ids: [300]u64 = undefined;
    for (&ids) |*id| id.* = try db.createNode(&txn, &.{"P"});
    try db.commitTransaction(&txn);

    {
        var result = try db.query("MATCH (n) RETURN n");
        defer result.deinit();
        try expectIdsAscending(result, 300);
        try std.testing.expectEqual(ids[0], result.rows[0].values[0].node_id);
    }

    // With the transaction's own changes merged in.
    txn = try db.beginTransaction(.read_write);
    try db.deleteNode(&txn, ids[3]);
    _ = try db.createNode(&txn, &.{"Q"});
    {
        var result = try db.queryInTxn(&txn, "MATCH (n) RETURN n");
        defer result.deinit();
        try expectIdsAscending(result, 300);
    }
    try db.abortTransaction(&txn);
}

// ============================================================================
// Ordered property indexes
// ============================================================================

/// Plan `cypher` against `db` and report which scan the planner chose.
fn planScanKind(db: *Database, cypher: []const u8) !lattice.query.planner.QueryPlanner.ScanKind {
    const allocator = std.testing.allocator;
    var parser = lattice.query.parser.Parser.init(allocator, cypher);
    defer parser.deinit();
    const parse_result = parser.parse();
    if (parse_result.query == null) return error.ParseFailed;

    var analyzer = lattice.query.semantic.SemanticAnalyzer.init(allocator);
    defer analyzer.deinit();
    const analysis = analyzer.analyze(parse_result.query.?);
    if (!analysis.success) return error.SemanticFailed;

    const storage_ctx = lattice.query.planner.StorageContext{
        .node_tree = &db.node_tree,
        .label_index = &db.label_index,
        .edge_store = &db.edge_store,
        .symbol_table = &db.symbol_table,
        .hnsw_index = if (db.hnsw_index) |*hnsw| hnsw else null,
        .fts_index = null,
        .database = db,
    };
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    var planner = lattice.query.planner.QueryPlanner.init(arena.allocator(), storage_ctx);
    defer planner.deinit();
    const analysis_result = lattice.query.semantic.AnalysisResult{
        .success = true,
        .errors = &[_]lattice.query.semantic.SemanticError{},
        .variables = analysis.variables,
        .errors_dropped = false,
    };
    _ = try planner.plan(parse_result.query.?, &analysis_result);
    return planner.last_scan_kind;
}

fn printValue(out: *std.ArrayList(u8), value: lattice.storage.database.ResultValue) !void {
    const a = std.testing.allocator;
    switch (value) {
        .null_val => try out.appendSlice(a, "null"),
        .bool_val => |b| try out.appendSlice(a, if (b) "true" else "false"),
        .int_val => |i| try out.print(a, "i{d}", .{i}),
        .float_val => |f| if (std.math.isNan(f)) try out.appendSlice(a, "NaN") else try out.print(a, "f{d}", .{f}),
        .string_val => |s| try out.print(a, "s{d}:{s}", .{ s.len, s }),
        .bytes_val => |s| try out.print(a, "b{d}", .{s.len}),
        .node_id => |id| try out.print(a, "n{d}", .{id}),
        .edge_id => |id| try out.print(a, "e{d}", .{id}),
        .vector_val => |v| try out.print(a, "v{d}", .{v.len}),
        .list_val => |items| {
            try out.append(a, '[');
            for (items) |item| {
                try printValue(out, item);
                try out.append(a, ',');
            }
            try out.append(a, ']');
        },
        .map_val => |entries| try out.print(a, "map{d}", .{entries.len}),
    }
}

fn render(result: QueryResult) ![]u8 {
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(std.testing.allocator);
    for (result.rows) |row| {
        for (row.values) |value| {
            try printValue(&out, value);
            try out.append(std.testing.allocator, ' ');
        }
        try out.append(std.testing.allocator, '\n');
    }
    return out.toOwnedSlice(std.testing.allocator);
}

/// The same query on a database with the index and one without must give
/// the same rows in the same order.
fn expectSameAnswer(indexed: *Database, plain: *Database, ti: ?*lattice.transaction.manager.Transaction, tp: ?*lattice.transaction.manager.Transaction, cypher: []const u8) !void {
    var a = try indexed.queryInTxn(ti, cypher);
    defer a.deinit();
    var b = try plain.queryInTxn(tp, cypher);
    defer b.deinit();
    const ra = try render(a);
    defer std.testing.allocator.free(ra);
    const rb = try render(b);
    defer std.testing.allocator.free(rb);
    if (!std.mem.eql(u8, ra, rb)) {
        std.debug.print("query: {s}\nindexed:\n{s}\nplain:\n{s}\n", .{ cypher, ra, rb });
        return error.TestExpectedEqual;
    }
}

fn fillMixed(db: *Database, txn: ?*lattice.transaction.manager.Transaction, count: usize) !void {
    var long_a: [300]u8 = undefined;
    @memset(&long_a, 'm');
    for (0..count) |i| {
        const id = try db.createNode(txn, &.{"P"});
        try db.setNodeProperty(txn, id, "id", .{ .int_val = @intCast(i) });
        const value: ?PropertyValue = switch (i % 13) {
            0 => .{ .int_val = @as(i64, @intCast(i % 50)) - 20 },
            1 => .{ .float_val = @as(f64, @floatFromInt(i % 40)) / 4.0 - 3.0 },
            2 => .{ .float_val = @floatFromInt(i % 30) },
            3 => .{ .string_val = ([_][]const u8{ "Jo", "John", "Joan", "Jp", "J", "", "Jo\x00x", "b", "ab" })[i % 9] },
            4 => .{ .string_val = long_a[0 .. 120 + (i % 20)] },
            5 => .{ .bool_val = i % 2 == 0 },
            6 => null,
            7 => .{ .float_val = std.math.nan(f64) },
            8 => .{ .int_val = (@as(i64, 1) << 60) + @as(i64, @intCast(i % 3)) },
            9 => .{ .float_val = 0x1p60 },
            10 => .{ .int_val = std.math.maxInt(i64) - @as(i64, @intCast(i % 2)) },
            11 => .{ .float_val = -0.0 },
            else => .{ .int_val = @intCast(i % 7) },
        };
        if (value) |v| try db.setNodeProperty(txn, id, "v", v);
        if (i % 5 == 0) try db.setNodeProperty(txn, id, "w", .{ .int_val = @intCast(i % 11) });
    }
}

const range_queries = [_][]const u8{
    "MATCH (n:P) WHERE n.v > 3 RETURN n.id, n.v ORDER BY id(n)",
    "MATCH (n:P) WHERE n.v >= 3 RETURN n.id, n.v ORDER BY id(n)",
    "MATCH (n:P) WHERE n.v < 3 RETURN n.id, n.v ORDER BY id(n)",
    "MATCH (n:P) WHERE n.v <= 3.0 RETURN n.id, n.v ORDER BY id(n)",
    "MATCH (n:P) WHERE n.v >= -2 AND n.v < 5.5 RETURN n.id, n.v ORDER BY id(n)",
    "MATCH (n:P) WHERE 5 > n.v AND n.v > 0 RETURN n.id, n.v ORDER BY id(n)",
    "MATCH (n:P) WHERE n.v > 1152921504606846976 RETURN n.id, n.v ORDER BY id(n)",
    "MATCH (n:P) WHERE n.v >= 1152921504606846976 AND n.v <= 1152921504606846977 RETURN n.id, n.v ORDER BY id(n)",
    "MATCH (n:P) WHERE n.v > 9223372036854775806 RETURN n.id, n.v ORDER BY id(n)",
    "MATCH (n:P) WHERE n.v STARTS WITH 'Jo' RETURN n.id, n.v ORDER BY id(n)",
    "MATCH (n:P) WHERE n.v STARTS WITH '' RETURN n.id ORDER BY id(n)",
    "MATCH (n:P) WHERE n.v > 'J' AND n.v < 'Jp' RETURN n.id, n.v ORDER BY id(n)",
    "MATCH (n:P) WHERE n.v >= 'mmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmm' RETURN n.id ORDER BY id(n)",
    "MATCH (n:P) WHERE n.v < 'mmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmm' RETURN n.id ORDER BY id(n)",
    "MATCH (n:P) WHERE n.v STARTS WITH 'mmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmm' RETURN n.id ORDER BY id(n)",
    "MATCH (n:P) WHERE n.v > false RETURN n.id, n.v ORDER BY id(n)",
    "MATCH (n:P) WHERE n.v > 1 AND n.v < 'x' RETURN n.id ORDER BY id(n)",
    "MATCH (n:P) WHERE n.v > null RETURN n.id ORDER BY id(n)",
    "MATCH (n:P) WHERE n.v > 2 AND n.w = 3 RETURN n.id, n.v ORDER BY id(n)",
    "MATCH (n:P) WHERE n.v > 2 OR n.w = 3 RETURN n.id ORDER BY id(n)",
    "MATCH (n:P) WHERE n.v > 2 AND n.v > 4 AND n.v <= 6 RETURN n.id, n.v ORDER BY id(n)",
    "MATCH (n:P) RETURN n.id, n.v ORDER BY n.v LIMIT 15",
    "MATCH (n:P) RETURN n.id, n.v ORDER BY n.v, n.id SKIP 20 LIMIT 30",
    "MATCH (n:P) RETURN n.id, n.v ORDER BY n.v LIMIT 100000",
    "MATCH (n:P) RETURN n.v AS v, n.id ORDER BY v LIMIT 40",
    "MATCH (n:P) WHERE n.v > 0 RETURN n.id, n.v ORDER BY n.v LIMIT 12",
    "MATCH (n:P) WHERE n.w > 3 RETURN n.id, n.v ORDER BY n.v LIMIT 12",
    "MATCH (n:P) WHERE n.v STARTS WITH 'J' RETURN n.id, n.v ORDER BY n.v LIMIT 3",
    "MATCH (n:P) RETURN n.id, n.v ORDER BY n.v LIMIT 0",
    // Descending: nulls first, then NaN, then values down; equal values,
    // nulls among them, in id order, as the sort leaves a label scan's.
    "MATCH (n:P) RETURN n.id, n.v ORDER BY n.v DESC LIMIT 15",
    "MATCH (n:P) RETURN n.id, n.v ORDER BY n.v DESC LIMIT 120",
    "MATCH (n:P) RETURN n.id, n.v ORDER BY n.v DESC, n.id SKIP 60 LIMIT 30",
    "MATCH (n:P) RETURN n.id, n.v ORDER BY n.v DESC, n.id DESC LIMIT 90",
    "MATCH (n:P) RETURN n.id, n.v ORDER BY n.v DESC LIMIT 100000",
    "MATCH (n:P) RETURN n.v AS v, n.id ORDER BY v DESC LIMIT 70",
    "MATCH (n:P) WHERE n.v > 0 RETURN n.id, n.v ORDER BY n.v DESC LIMIT 12",
    "MATCH (n:P) WHERE n.v < 3 RETURN n.id, n.v ORDER BY n.v DESC LIMIT 40",
    "MATCH (n:P) WHERE n.w > 3 RETURN n.id, n.v ORDER BY n.v DESC LIMIT 12",
    "MATCH (n:P) WHERE n.v STARTS WITH 'J' RETURN n.id, n.v ORDER BY n.v DESC LIMIT 3",
    "MATCH (n:P) WHERE n.v STARTS WITH 'mmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmm' RETURN n.id ORDER BY n.v DESC LIMIT 7",
    "MATCH (n:P) WHERE n.v < 'mmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmm' RETURN n.id, n.v ORDER BY n.v DESC LIMIT 9",
    "MATCH (n:P) RETURN n.id, n.v ORDER BY n.v DESC LIMIT 0",
    "MATCH (n:P) RETURN n.id ORDER BY n.v DESC SKIP 50 LIMIT 10",
    "MATCH (n:P) RETURN count(n)",
    "MATCH (n:P) WHERE n.v >= 0 RETURN count(n)",
    // Integers against floats, where rounding the integer would answer
    // differently from comparing exactly (2^60 + 1 > 2^60.0).
    "MATCH (n:P) WHERE n.v > 1152921504606846976.0 RETURN n.id, n.v ORDER BY id(n)",
    "MATCH (n:P) WHERE n.v >= 1152921504606846976.0 AND n.v < 1152921504606846978 RETURN n.id, n.v ORDER BY id(n)",
    "MATCH (n:P) WHERE n.v <= 1152921504606846976.0 AND n.v > 1e18 RETURN n.id, n.v ORDER BY id(n)",
    "MATCH (n:P) WHERE n.v > 9.223372036854775e18 RETURN n.id, n.v ORDER BY id(n)",
    "MATCH (n:P) WHERE n.v < 9223372036854775807.0 AND n.v >= 9.2e18 RETURN n.id, n.v ORDER BY id(n)",
    "MATCH (n:P) WHERE n.v = 1152921504606846977 RETURN n.id, n.v ORDER BY id(n)",
    "MATCH (n:P) WHERE n.v = 1152921504606846976 RETURN n.id, n.v ORDER BY id(n)",
    "MATCH (n:P) WHERE n.v = 1152921504606846976.0 RETURN n.id, n.v ORDER BY id(n)",
    "MATCH (n:P) WHERE n.v >= -0.0 AND n.v <= 0 RETURN n.id, n.v ORDER BY id(n)",
    "MATCH (n:P) WHERE n.v > -0.5 AND n.v < 0.5 RETURN n.id, n.v ORDER BY id(n)",
};

test "lgr: number keys order as the engine compares integers with floats" {
    const encode = lattice.graph.property_index.encodeOrderedValue;
    const compareIntFloat = lattice.query.expression.compareIntFloat;
    const two60: i64 = 1 << 60;
    const ints = [_]i64{
        std.math.minInt(i64),     std.math.minInt(i64) + 1, -two60 - 257, -two60 - 256, -two60 - 1,  -two60,
        -(1 << 53) - 1,           -3,                       -1,           0,            1,           2,
        (1 << 53) + 1,            two60 - 1,                two60,        two60 + 1,    two60 + 255, two60 + 256,
        std.math.maxInt(i64) - 1, std.math.maxInt(i64),
    };
    const floats = [_]f64{
        -std.math.inf(f64), -0x1p63, -0x1p60 - 256.0, -0x1p60, -0x1p53, -2.5,           -1.0,                 -0.5,   -0.0,              0.0,
        0.5,                1.0,     2.0,             0x1p53,  0x1p60,  0x1p60 + 256.0, 9.223372036854775e18, 0x1p63, std.math.inf(f64),
    };
    for (ints) |i| {
        const ei = encode(.{ .int_val = i }).?;
        for (floats) |f| {
            const ef = encode(.{ .float_val = f }).?;
            const want = compareIntFloat(i, f).?;
            const got = std.mem.order(u8, ei.bytes(), ef.bytes());
            if (want != got) {
                std.debug.print("{d} vs {d}: engine {s}, keys {s}\n", .{ i, f, @tagName(want), @tagName(got) });
                return error.TestExpectedEqual;
            }
        }
        for (ints) |j| {
            const ej = encode(.{ .int_val = j }).?;
            try std.testing.expectEqual(std.math.order(i, j), std.mem.order(u8, ei.bytes(), ej.bytes()));
        }
    }
    for (floats) |f| {
        for (floats) |g| {
            const want = std.math.order(f, g);
            try std.testing.expectEqual(want, std.mem.order(u8, encode(.{ .float_val = f }).?.bytes(), encode(.{ .float_val = g }).?.bytes()));
        }
    }
}

fn compareAll(indexed: *Database, plain: *Database, ti: ?*lattice.transaction.manager.Transaction, tp: ?*lattice.transaction.manager.Transaction) !void {
    for (range_queries) |cypher| try expectSameAnswer(indexed, plain, ti, tp, cypher);
}

test "lgr: an ordered index answers ranges and ORDER BY ... LIMIT exactly as a scan does" {
    const path_i = "/tmp/lgr_range_indexed.ltdb";
    const path_p = "/tmp/lgr_range_plain.ltdb";
    defer removeFiles(path_i);
    defer removeFiles(path_p);
    var indexed = try openDb(path_i, true);
    defer indexed.close();
    var plain = try openDb(path_p, true);
    defer plain.close();

    for ([_]*Database{ indexed, plain }) |db| {
        var txn = try db.beginTransaction(.read_write);
        try fillMixed(db, &txn, 700);
        try db.commitTransaction(&txn);
    }
    try indexed.createNodePropertyIndex("P", "v");

    const Kind = lattice.query.planner.QueryPlanner.ScanKind;
    try std.testing.expectEqual(Kind.property_range_scan, try planScanKind(indexed, "MATCH (n:P) WHERE n.v > 3 RETURN n"));
    try std.testing.expectEqual(Kind.property_range_scan, try planScanKind(indexed, "MATCH (n:P) WHERE n.v STARTS WITH 'Jo' RETURN n"));
    try std.testing.expectEqual(Kind.property_order_scan, try planScanKind(indexed, "MATCH (n:P) RETURN n ORDER BY n.v LIMIT 10"));
    try std.testing.expectEqual(Kind.property_order_scan, try planScanKind(indexed, "MATCH (n:P) RETURN n ORDER BY n.v DESC LIMIT 3"));
    try std.testing.expectEqual(Kind.property_index_scan, try planScanKind(indexed, "MATCH (n:P) WHERE n.v = 3 AND n.v > 1 RETURN n"));
    // Not without a LIMIT, not through an OR.
    try std.testing.expectEqual(Kind.label_scan, try planScanKind(indexed, "MATCH (n:P) RETURN n ORDER BY n.v"));
    try std.testing.expectEqual(Kind.label_scan, try planScanKind(indexed, "MATCH (n:P) RETURN n ORDER BY n.v DESC"));
    try std.testing.expectEqual(Kind.label_scan, try planScanKind(indexed, "MATCH (n:P) WHERE n.v > 3 OR n.w = 1 RETURN n"));
    try std.testing.expectEqual(Kind.label_scan, try planScanKind(plain, "MATCH (n:P) WHERE n.v > 3 RETURN n"));

    try compareAll(indexed, plain, null, null);

    // Inside a transaction that changed some of the values: the scan merges
    // the transaction's own nodes in.
    var ti = try indexed.beginTransaction(.read_write);
    var tp = try plain.beginTransaction(.read_write);
    for ([_]*Database{ indexed, plain }, [_]*lattice.transaction.manager.Transaction{ &ti, &tp }) |db, txn| {
        try fillMixed(db, txn, 60);
        var id: u64 = 1;
        while (id < 700) : (id += 37) {
            try db.setNodeProperty(txn, id, "v", .{ .int_val = @intCast(id % 9) });
        }
        id = 5;
        while (id < 700) : (id += 41) try db.removeNodeProperty(txn, id, "v");
        id = 7;
        while (id < 700) : (id += 53) try db.deleteNode(txn, id);
        // Nodes entering and leaving the label, with and without the
        // property: the nodes without it are in a label's index too.
        id = 11;
        while (id < 700) : (id += 47) try db.removeNodeLabel(txn, id, "P");
        for (0..12) |i| {
            const q = try db.createNode(txn, &.{"Q"});
            if (i % 2 == 0) try db.setNodeProperty(txn, q, "v", .{ .int_val = @intCast(i) });
            try db.addNodeLabel(txn, q, "P");
        }
    }
    try compareAll(indexed, plain, &ti, &tp);
    try indexed.commitTransaction(&ti);
    try plain.commitTransaction(&tp);
    try compareAll(indexed, plain, null, null);
}

test "lgr: ORDER BY ... LIMIT is not read in index order under a vector search" {
    const path = "/tmp/lgr_order_vector.ltdb";
    removeFiles(path);
    defer removeFiles(path);
    var db = try Database.open(std.testing.allocator, path, .{
        .create = true,
        .config = .{ .enable_wal = false, .enable_fts = false, .enable_vector = true, .vector_dimensions = 4 },
    });
    defer db.close();
    // Nearest to [1, 0, 0, 0] first is highest rank first.
    for (0..100) |i| {
        const node = try db.createNode(null, &.{"Doc"});
        try db.setNodeProperty(null, node, "rank", .{ .int_val = @intCast(99 - i) });
        const angle = @as(f32, @floatFromInt(i)) * (std.math.pi / 2.0) / 100.0;
        try db.setNodeVector(node, &[_]f32{ @cos(angle), @sin(angle), 0, 0 });
    }
    try db.createNodePropertyIndex("Doc", "rank");

    // The search emits its rows nearest first, so the scan below it cannot
    // be read in the ORDER BY's order and cut off.
    const cypher = "MATCH (d:Doc) WHERE d.embedding <=> [1.0, 0.0, 0.0, 0.0] < 2.0 RETURN d.rank ORDER BY d.rank LIMIT 3";
    const Kind = lattice.query.planner.QueryPlanner.ScanKind;
    try std.testing.expectEqual(Kind.label_scan, try planScanKind(db, cypher));
    var result = try db.query(cypher);
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 3), result.rowCount());
    for (result.rows, 0..) |row, want| try std.testing.expectEqual(@as(i64, @intCast(want)), row.values[0].int_val);
}

test "lgr: a query that writes takes the range up front" {
    const path = "/tmp/lgr_range_write.ltdb";
    defer removeFiles(path);
    var db = try openDb(path, true);
    defer db.close();
    var txn = try db.beginTransaction(.read_write);
    for (0..50) |i| {
        const id = try db.createNode(&txn, &.{"P"});
        try db.setNodeProperty(&txn, id, "v", .{ .int_val = @intCast(i) });
    }
    try db.commitTransaction(&txn);
    try db.createNodePropertyIndex("P", "v");

    // Moving each node past the end of the range must not bring it back.
    {
        var result = try db.query("MATCH (n:P) WHERE n.v >= 10 SET n.v = n.v + 100 RETURN count(n)");
        defer result.deinit();
        try std.testing.expectEqual(@as(i64, 40), result.rows[0].values[0].int_val);
    }
    {
        var result = try db.query("MATCH (n:P) WHERE n.v >= 110 RETURN count(n)");
        defer result.deinit();
        try std.testing.expectEqual(@as(i64, 40), result.rows[0].values[0].int_val);
    }
}

test "lgr: an ordered index raises the minimum reader version, an old index keeps working" {
    const path = "/tmp/lgr_index_versions.ltdb";
    defer removeFiles(path);
    {
        var db = try openDb(path, true);
        defer db.close();
        try std.testing.expectEqual(lattice.core.types.MIN_READER_VERSION, db.page_manager.getHeader().min_reader_version);
        var txn = try db.beginTransaction(.read_write);
        for (0..30) |i| {
            const id = try db.createNode(&txn, &.{"P"});
            try db.setNodeProperty(&txn, id, "v", .{ .int_val = @intCast(i) });
            try db.setNodeProperty(&txn, id, "u", .{ .int_val = @intCast(i % 3) });
        }
        try db.commitTransaction(&txn);

        // An index as 0.16.0 declared it: an empty catalog value, digests.
        const label = try db.symbol_table.intern("P");
        const prop = try db.symbol_table.intern("u");
        var index = &db.property_index.?;
        const legacy = lattice.graph.property_index.Definition{ .kind = .node, .scope_id = label, .property_id = prop };
        const it = try db.label_index.getNodesByLabel(label);
        defer std.testing.allocator.free(it);
        for (it) |node_id| {
            const value = (try db.getNodePropertyByKeyInTxn(null, node_id, prop, std.testing.allocator, null)).?;
            try index.add(legacy, node_id, value);
        }
        try index.createDefinition(legacy);
        try db.sync();
    }
    {
        var db = try reopenDb(path);
        defer db.close();
        // Still format 3's minimum: nothing here 0.16.0 would misread.
        try std.testing.expectEqual(lattice.core.types.MIN_READER_VERSION, db.page_manager.getHeader().min_reader_version);
        // The old index answers equality and is not used for ranges.
        const Kind = lattice.query.planner.QueryPlanner.ScanKind;
        try std.testing.expectEqual(Kind.property_index_scan, try planScanKind(db, "MATCH (n:P) WHERE n.u = 1 RETURN n"));
        try std.testing.expectEqual(Kind.label_scan, try planScanKind(db, "MATCH (n:P) WHERE n.u > 1 RETURN n"));
        var result = try db.query("MATCH (n:P) WHERE n.u = 1 RETURN count(n)");
        defer result.deinit();
        try std.testing.expectEqual(@as(i64, 10), result.rows[0].values[0].int_val);

        try db.createNodePropertyIndex("P", "v");
        try std.testing.expectEqual(lattice.core.types.FORMAT_VERSION, db.page_manager.getHeader().min_reader_version);
    }
    {
        var db = try reopenDb(path);
        defer db.close();
        try std.testing.expectEqual(lattice.core.types.FORMAT_VERSION, db.page_manager.getHeader().min_reader_version);
        var result = try db.query("MATCH (n:P) WHERE n.v >= 25 RETURN count(n)");
        defer result.deinit();
        try std.testing.expectEqual(@as(i64, 5), result.rows[0].values[0].int_val);
    }
}

fn expectFoundNodes(db: *Database, txn: anytype, value: PropertyValue, expected: []const u64) !void {
    const found = try db.findNodesByLabelProperty(txn, "P", "k", value, 100);
    defer std.testing.allocator.free(found);
    try std.testing.expectEqualSlices(u64, expected, found);
}

fn expectFoundEdges(db: *Database, txn: anytype, value: PropertyValue, expected: []const u64) !void {
    const found = try db.findEdgesByTypeProperty(txn, "R", "k", value, 100);
    defer std.testing.allocator.free(found);
    try std.testing.expectEqualSlices(u64, expected, found);
}

test "lgr: index lookups in a transaction follow its later writes" {
    const path = "/tmp/lgr_overlay_lookup.ltdb";
    defer removeFiles(path);
    var db = try openDb(path, true);
    defer db.close();
    try db.createNodePropertyIndex("P", "k");
    try db.createEdgePropertyIndex("R", "k");

    var txn = try db.beginTransaction(.read_write);
    const committed = try db.createNode(&txn, &.{"P"});
    try db.setNodeProperty(&txn, committed, "k", .{ .int_val = 5 });
    try db.commitTransaction(&txn);

    txn = try db.beginTransaction(.read_write);
    var ids: [6]u64 = undefined;
    for (&ids, 0..) |*id, i| {
        id.* = try db.createNode(&txn, &.{"P"});
        try db.setNodeProperty(&txn, id.*, "k", .{ .int_val = @intCast(i) });
    }
    // The first lookup notes what is staged; the writes after it are noted
    // as they happen.
    try expectFoundNodes(db, &txn, .{ .int_val = 5 }, &.{ committed, ids[5] });
    try db.setNodeProperty(&txn, ids[5], "k", .{ .int_val = 9 });
    try db.setNodeProperty(&txn, ids[1], "k", .{ .int_val = 5 });
    try db.setNodeProperty(&txn, committed, "k", .{ .int_val = 9 });
    try expectFoundNodes(db, &txn, .{ .int_val = 5 }, &.{ids[1]});
    try expectFoundNodes(db, &txn, .{ .int_val = 9 }, &.{ committed, ids[5] });
    try db.deleteNode(&txn, ids[5]);
    try db.removeNodeLabel(&txn, committed, "P");
    try expectFoundNodes(db, &txn, .{ .int_val = 9 }, &.{});
    try db.addNodeLabel(&txn, committed, "P");
    try db.removeNodeProperty(&txn, ids[1], "k");
    try expectFoundNodes(db, &txn, .{ .int_val = 9 }, &.{committed});
    try expectFoundNodes(db, &txn, .{ .int_val = 5 }, &.{});
    const late = try db.createNode(&txn, &.{"P"});
    try db.setNodeProperty(&txn, late, "k", .{ .int_val = 5 });
    try expectFoundNodes(db, &txn, .{ .int_val = 5 }, &.{late});

    // Edges, the same way.
    const e1 = try db.createEdgeAndGetId(&txn, ids[0], ids[2], "R");
    const e2 = try db.createEdgeAndGetId(&txn, ids[2], ids[3], "R");
    try db.setEdgePropertyById(&txn, e1, "k", .{ .string_val = "a" });
    try db.setEdgePropertyById(&txn, e2, "k", .{ .string_val = "b" });
    try expectFoundEdges(db, &txn, .{ .string_val = "a" }, &.{e1});
    try db.setEdgePropertyById(&txn, e2, "k", .{ .string_val = "a" });
    try expectFoundEdges(db, &txn, .{ .string_val = "a" }, &.{ e1, e2 });
    try db.deleteEdgeById(&txn, e1);
    try expectFoundEdges(db, &txn, .{ .string_val = "a" }, &.{e2});
    try db.commitTransaction(&txn);

    try expectFoundNodes(db, null, .{ .int_val = 5 }, &.{late});
    try expectFoundNodes(db, null, .{ .int_val = 9 }, &.{committed});
    try expectFoundEdges(db, null, .{ .string_val = "a" }, &.{e2});
}

test "lgr: declaring and dropping indexes keeps the others maintained" {
    const path = "/tmp/lgr_index_declarations.ltdb";
    defer removeFiles(path);
    var db = try openDb(path, true);
    defer db.close();

    var txn = try db.beginTransaction(.read_write);
    const a = try db.createNode(&txn, &.{"P"});
    try db.setNodeProperty(&txn, a, "k", .{ .int_val = 1 });
    try db.commitTransaction(&txn);

    try db.createNodePropertyIndex("P", "k");
    txn = try db.beginTransaction(.read_write);
    const b = try db.createNode(&txn, &.{"P"});
    try db.setNodeProperty(&txn, b, "k", .{ .int_val = 1 });
    try db.setNodeProperty(&txn, b, "j", .{ .int_val = 2 });
    try db.commitTransaction(&txn);

    try db.createNodePropertyIndex("P", "j");
    txn = try db.beginTransaction(.read_write);
    const c = try db.createNode(&txn, &.{"P"});
    try db.setNodeProperty(&txn, c, "k", .{ .int_val = 1 });
    try db.setNodeProperty(&txn, c, "j", .{ .int_val = 2 });
    // An update that leaves the indexed values alone leaves their entries.
    try db.setNodeProperty(&txn, a, "other", .{ .int_val = 0 });
    try db.commitTransaction(&txn);
    try expectFoundNodes(db, null, .{ .int_val = 1 }, &.{ a, b, c });
    {
        const found = try db.findNodesByLabelProperty(null, "P", "j", .{ .int_val = 2 }, 100);
        defer std.testing.allocator.free(found);
        try std.testing.expectEqualSlices(u64, &.{ b, c }, found);
    }

    try db.dropNodePropertyIndex("P", "k");
    txn = try db.beginTransaction(.read_write);
    const d = try db.createNode(&txn, &.{"P"});
    try db.setNodeProperty(&txn, d, "k", .{ .int_val = 1 });
    try db.setNodeProperty(&txn, d, "j", .{ .int_val = 2 });
    try db.commitTransaction(&txn);
    try std.testing.expectError(DatabaseError.MissingIndex, db.findNodesByLabelProperty(null, "P", "k", .{ .int_val = 1 }, 100));
    {
        const found = try db.findNodesByLabelProperty(null, "P", "j", .{ .int_val = 2 }, 100);
        defer std.testing.allocator.free(found);
        try std.testing.expectEqualSlices(u64, &.{ b, c, d }, found);
    }
    // Nothing of the dropped index is left behind to come back with it.
    try db.createNodePropertyIndex("P", "k");
    try expectFoundNodes(db, null, .{ .int_val = 1 }, &.{ a, b, c, d });
}

// ============================================================================
// Patterns that start from a relationship type
// ============================================================================

fn sortedRender(db: *Database, txn: ?*lattice.transaction.manager.Transaction, cypher: []const u8) ![]u8 {
    var result = try db.queryInTxn(txn, cypher);
    defer result.deinit();
    const text = try render(result);
    defer std.testing.allocator.free(text);
    var lines: std.ArrayList([]const u8) = .empty;
    defer lines.deinit(std.testing.allocator);
    var it = std.mem.splitScalar(u8, text, '\n');
    while (it.next()) |line| if (line.len > 0) try lines.append(std.testing.allocator, line);
    std.mem.sort([]const u8, lines.items, {}, struct {
        fn lessThan(_: void, a: []const u8, b: []const u8) bool {
            return std.mem.order(u8, a, b) == .lt;
        }
    }.lessThan);
    var out: std.ArrayList(u8) = .empty;
    for (lines.items) |line| {
        try out.appendSlice(std.testing.allocator, line);
        try out.append(std.testing.allocator, '\n');
    }
    return out.toOwnedSlice(std.testing.allocator);
}

fn expectSameRows(db: *Database, txn: ?*lattice.transaction.manager.Transaction, typed: []const u8, untyped: []const u8) !void {
    const a = try sortedRender(db, txn, typed);
    defer std.testing.allocator.free(a);
    const b = try sortedRender(db, txn, untyped);
    defer std.testing.allocator.free(b);
    if (!std.mem.eql(u8, a, b)) {
        std.debug.print("typed: {s}\n{s}\nuntyped: {s}\n{s}\n", .{ typed, a, untyped, b });
        return error.TestExpectedEqual;
    }
}

fn compareTypePatterns(db: *Database, txn: ?*lattice.transaction.manager.Transaction) !void {
    // Each typed pattern against the same pattern expanded from every node.
    try expectSameRows(db, txn, "MATCH (a)-[r:KNOWS]->(b) RETURN id(a), id(r), id(b)", "MATCH (a)-[r]->(b) WHERE type(r) = 'KNOWS' RETURN id(a), id(r), id(b)");
    try expectSameRows(db, txn, "MATCH (a)<-[r:KNOWS]-(b) RETURN id(a), id(r), id(b)", "MATCH (a)<-[r]-(b) WHERE type(r) = 'KNOWS' RETURN id(a), id(r), id(b)");
    try expectSameRows(db, txn, "MATCH ()-[r:KNOWS|LIKES]->() RETURN id(r)", "MATCH ()-[r]->() WHERE type(r) = 'KNOWS' OR type(r) = 'LIKES' RETURN id(r)");
    try expectSameRows(db, txn, "MATCH ()-[:KNOWS]->(b) RETURN id(b)", "MATCH ()-[r]->(b) WHERE type(r) = 'KNOWS' RETURN id(b)");
    try expectSameRows(db, txn, "MATCH ()-[r:KNOWS {w: 1}]->() RETURN id(r)", "MATCH ()-[r]->() WHERE type(r) = 'KNOWS' AND r.w = 1 RETURN id(r)");
    try expectSameRows(db, txn, "MATCH (a)-[r:KNOWS]->(b)-[s:LIKES]->(c) RETURN id(r), id(s), id(c)", "MATCH (a)-[r]->(b)-[s:LIKES]->(c) WHERE type(r) = 'KNOWS' RETURN id(r), id(s), id(c)");
    try expectSameRows(db, txn, "MATCH ()-[r:NOPE]->() RETURN id(r)", "MATCH ()-[r]->() WHERE type(r) = 'NOPE' RETURN id(r)");
    try expectSameRows(db, txn, "MATCH ()-[r:KNOWS]->() RETURN count(r)", "MATCH ()-[r]->() WHERE type(r) = 'KNOWS' RETURN count(r)");
}

test "lgr: a pattern from a relationship type walks the edges once, with the same rows" {
    const path = "/tmp/lgr_edge_type_scan.ltdb";
    defer removeFiles(path);
    var db = try openDb(path, true);
    defer db.close();

    var txn = try db.beginTransaction(.read_write);
    var nodes: [60]u64 = undefined;
    for (&nodes) |*n| n.* = try db.createNode(&txn, &.{"N"});
    for (0..600) |i| {
        const a = nodes[(i * 7) % nodes.len];
        const b = nodes[(i * 13 + 5) % nodes.len];
        const t: []const u8 = switch (i % 3) {
            0 => "KNOWS",
            1 => "LIKES",
            else => "OTHER",
        };
        const e = try db.createEdgeAndGetId(&txn, a, if (i % 50 == 0) a else b, t);
        if (i % 4 == 0) try db.setEdgePropertyById(&txn, e, "w", .{ .int_val = 1 });
    }
    try db.commitTransaction(&txn);

    const Kind = lattice.query.planner.QueryPlanner.ScanKind;
    try std.testing.expectEqual(Kind.edge_type_scan, try planScanKind(db, "MATCH ()-[r:KNOWS]->() RETURN count(r)"));
    try std.testing.expectEqual(Kind.label_scan, try planScanKind(db, "MATCH (a:N)-[r:KNOWS]->() RETURN count(r)"));

    try compareTypePatterns(db, null);

    // A node gone with its edges left behind, as 0.16.0's deleteNode left
    // them (this release refuses that delete): the expand never started
    // from it.
    try db.label_index.remove(try db.symbol_table.lookup("N"), nodes[3]);
    try db.node_store.delete(nodes[3]);
    try compareTypePatterns(db, null);

    // The transaction's own edges, created and deleted.
    txn = try db.beginTransaction(.read_write);
    for (4..44) |i| _ = try db.createEdgeAndGetId(&txn, nodes[i], nodes[i + 1], "KNOWS");
    var dropped: u64 = 1;
    while (dropped < 600) : (dropped += 29) db.deleteEdgeById(&txn, dropped) catch {};
    try db.detachDeleteNode(&txn, nodes[10]);
    try compareTypePatterns(db, &txn);

    // A query that writes sees the edges as they were when it started.
    {
        var result = try db.queryInTxn(&txn, "MATCH (a)-[r:KNOWS]->(b) CREATE (a)-[:KNOWS]->(b) RETURN count(r)");
        defer result.deinit();
        var before = try db.queryInTxn(&txn, "MATCH ()-[r:KNOWS]->() RETURN count(r)");
        defer before.deinit();
        try std.testing.expectEqual(result.rows[0].values[0].int_val * 2, before.rows[0].values[0].int_val);
    }
    try db.commitTransaction(&txn);
    try compareTypePatterns(db, null);
}

fn cachedTargets(db: *Database, node: u64) ![]u64 {
    const edges = (try db.getOutgoingEdgesCached(node)) orelse return error.TestUnexpectedResult;
    const targets = try std.testing.allocator.alloc(u64, edges.len);
    for (edges, targets) |edge, *target| target.* = edge.target;
    std.mem.sort(u64, targets, {}, std.sort.asc(u64));
    return targets;
}

test "lgr: the adjacency cache follows commits, not staged writes" {
    const path = "/tmp/lgr_adjacency_cache.ltdb";
    defer removeFiles(path);
    removeFiles(path);
    var db = try Database.open(std.testing.allocator, path, .{
        .create = true,
        .config = .{ .enable_fts = false, .enable_vector = false, .enable_adjacency_cache = true },
    });
    defer db.close();

    var txn = try db.beginTransaction(.read_write);
    const a = try db.createNode(&txn, &.{"P"});
    const b = try db.createNode(&txn, &.{"P"});
    const c = try db.createNode(&txn, &.{"P"});
    _ = try db.createEdgeAndGetId(&txn, a, b, "KNOWS");
    try db.commitTransaction(&txn);

    // An edge staged and then committed, with the cache read in between.
    txn = try db.beginTransaction(.read_write);
    const ac = try db.createEdgeAndGetId(&txn, a, c, "KNOWS");
    {
        const targets = try cachedTargets(db, a);
        defer std.testing.allocator.free(targets);
        try std.testing.expectEqualSlices(u64, &.{b}, targets);
    }
    try db.commitTransaction(&txn);
    {
        const targets = try cachedTargets(db, a);
        defer std.testing.allocator.free(targets);
        try std.testing.expectEqualSlices(u64, &.{ b, c }, targets);
    }

    // The same for a delete.
    txn = try db.beginTransaction(.read_write);
    try db.deleteEdgeById(&txn, ac);
    {
        const targets = try cachedTargets(db, a);
        defer std.testing.allocator.free(targets);
        try std.testing.expectEqualSlices(u64, &.{ b, c }, targets);
    }
    try db.commitTransaction(&txn);
    {
        const targets = try cachedTargets(db, a);
        defer std.testing.allocator.free(targets);
        try std.testing.expectEqualSlices(u64, &.{b}, targets);
    }

    // A transaction that is rolled back leaves the cache as it was.
    txn = try db.beginTransaction(.read_write);
    _ = try db.createEdgeAndGetId(&txn, a, c, "KNOWS");
    try db.abortTransaction(&txn);
    {
        const targets = try cachedTargets(db, a);
        defer std.testing.allocator.free(targets);
        try std.testing.expectEqualSlices(u64, &.{b}, targets);
    }
}

// ============================================================================
// NaN in comparisons
// ============================================================================

test "lgr: every ordering comparison with NaN is false, and <> is true" {
    const path = "/tmp/lgr_nan_compare.ltdb";
    removeFiles(path);
    defer removeFiles(path);
    var db = try openDb(path, false);
    defer db.close();
    const nan_node = try db.createNode(null, &.{"N"});
    try db.setNodeProperty(null, nan_node, "v", .{ .float_val = std.math.nan(f64) });
    try db.setNodeProperty(null, nan_node, "i", .{ .int_val = 0 });
    for ([_]f64{ -1.0, 2.5 }, 1..) |f, i| {
        const id = try db.createNode(null, &.{"N"});
        try db.setNodeProperty(null, id, "v", .{ .float_val = f });
        try db.setNodeProperty(null, id, "i", .{ .int_val = @intCast(i) });
    }
    const id = try db.createNode(null, &.{"N"});
    try db.setNodeProperty(null, id, "v", .{ .int_val = 7 });
    try db.setNodeProperty(null, id, "i", .{ .int_val = 3 });

    // Both ways round, against a float, an integer and NaN itself. Before,
    // `>` and `>=` were `NOT <=` and `NOT <`, so NaN was above everything
    // and below nothing.
    const falses = [_][]const u8{
        "n.v > 1.0, n.v >= 1.0, n.v < 1.0, n.v <= 1.0, 1.0 > n.v, 1.0 >= n.v, 1.0 < n.v, 1.0 <= n.v",
        "n.v > 1, 1 > n.v, n.v >= m.v, m.v >= n.v, n.v > n.v, n.v >= n.v, n.v = n.v",
    };
    for (falses) |items| {
        const cypher = try std.fmt.allocPrint(std.testing.allocator, "MATCH (n:N {{i: 0}}) MATCH (m:N {{i: 3}}) RETURN {s}", .{items});
        defer std.testing.allocator.free(cypher);
        var result = try db.query(cypher);
        defer result.deinit();
        for (result.rows[0].values, 0..) |value, i| {
            if (value != .bool_val or value.bool_val) {
                std.debug.print("{s}: column {d} is not false\n", .{ items, i });
                return error.TestExpectedEqual;
            }
        }
    }
    var unequal = try db.query("MATCH (n:N {i: 0}) RETURN n.v <> n.v, n.v <> 1");
    defer unequal.deinit();
    try std.testing.expect(unequal.rows[0].values[0].bool_val and unequal.rows[0].values[1].bool_val);

    // A WHERE keeps NaN on neither side of a bound.
    var above = try db.query("MATCH (n:N) WHERE n.v > 0 RETURN n.i ORDER BY n.i");
    defer above.deinit();
    try std.testing.expectEqual(@as(usize, 2), above.rowCount());
    var below = try db.query("MATCH (n:N) WHERE 0 >= n.v RETURN n.i");
    defer below.deinit();
    try std.testing.expectEqual(@as(usize, 1), below.rowCount());
    try std.testing.expectEqual(@as(i64, 1), below.rows[0].values[0].int_val);

    // ORDER BY still has to put NaN somewhere: after every number, the
    // place the ordered index keys it.
    var sorted = try db.query("MATCH (n:N) RETURN n.i ORDER BY n.v");
    defer sorted.deinit();
    for (sorted.rows, [_]i64{ 1, 2, 3, 0 }) |r, want| try std.testing.expectEqual(want, r.values[0].int_val);
    try db.createNodePropertyIndex("N", "v");
    var indexed = try db.query("MATCH (n:N) RETURN n.i ORDER BY n.v LIMIT 4");
    defer indexed.deinit();
    for (indexed.rows, [_]i64{ 1, 2, 3, 0 }) |r, want| try std.testing.expectEqual(want, r.values[0].int_val);
    var ranged = try db.query("MATCH (n:N) WHERE n.v > -100 RETURN count(n)");
    defer ranged.deinit();
    try std.testing.expectEqual(@as(i64, 3), ranged.rows[0].values[0].int_val);
}
