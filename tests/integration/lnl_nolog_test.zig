//! A database opened without a log has transactions too.
//!
//! `enable_wal = false` means that nothing is logged, so nothing survives a
//! crash until the next flush. It used to mean as well that there was no
//! transaction manager: a writing query ran straight against the trees, and
//! one that failed part-way kept what it had done. These tests run each case
//! on a database with a log and on one without, and require the same result.

const std = @import("std");
const lattice = @import("lattice");

const Database = lattice.storage.database.Database;
const DatabaseError = lattice.storage.database.DatabaseError;
const DatabaseConfig = lattice.storage.database.DatabaseConfig;
const Transaction = lattice.transaction.manager.Transaction;

const allocator = std.testing.allocator;

const Mode = enum { log, no_log };
const modes = [_]Mode{ .log, .no_log };

fn removeFiles(path: []const u8) void {
    const cwd = @import("compat").fs.cwd();
    cwd.deleteFile(path) catch {};
    var buf: [256]u8 = undefined;
    for ([_][]const u8{ "-wal", "-journal" }) |suffix| {
        const p = std.fmt.bufPrint(&buf, "{s}{s}", .{ path, suffix }) catch unreachable;
        cwd.deleteFile(p) catch {};
    }
}

fn pathFor(buf: []u8, name: []const u8, mode: Mode) []const u8 {
    return std.fmt.bufPrint(buf, "/tmp/lnl_{s}_{s}.ltdb", .{ name, @tagName(mode) }) catch unreachable;
}

fn configFor(mode: Mode) DatabaseConfig {
    return .{ .enable_wal = mode == .log, .enable_fts = true };
}

fn openFresh(path: []const u8, config: DatabaseConfig) !*Database {
    removeFiles(path);
    return Database.open(allocator, path, .{ .create = true, .config = config });
}

fn reopen(path: []const u8, config: DatabaseConfig) !*Database {
    return Database.open(allocator, path, .{ .config = config });
}

fn run(db: *Database, cypher: []const u8) !void {
    var result = try db.query(cypher);
    result.deinit();
}

fn runInTxn(db: *Database, txn: *Transaction, cypher: []const u8) !void {
    var result = try db.queryInTxn(txn, cypher);
    result.deinit();
}

fn countIn(db: *Database, txn: ?*Transaction, cypher: []const u8) !i64 {
    var result = try db.queryInTxn(txn, cypher);
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), result.rows.len);
    return result.rows[0].values[0].int_val;
}

fn count(db: *Database, cypher: []const u8) !i64 {
    return countIn(db, null, cypher);
}

// ============================================================================
// A failed query is rolled back
// ============================================================================

test "lnl: a query that fails after deleting a node leaves the node, with or without a log" {
    for (modes) |mode| {
        var buf: [128]u8 = undefined;
        const path = pathFor(&buf, "repro", mode);
        const db = try openFresh(path, configFor(mode));
        defer {
            db.close();
            removeFiles(path);
        }
        try run(db, "CREATE (:D {i: 1})-[:R]->(:D {i: 2}), (:D {i: 3})");

        // The DELETE runs, then the CREATE refuses a relationship to the node
        // it deleted.
        try std.testing.expectError(
            error.ExecutionError,
            run(db, "MATCH (a:D {i: 3}), (b:D {i: 2}) DELETE a CREATE (b)-[:R]->(a)"),
        );
        try std.testing.expectEqual(@as(i64, 3), try count(db, "MATCH (n:D) RETURN count(n)"));
        try std.testing.expectEqual(@as(i64, 1), try count(db, "MATCH ()-[r:R]->() RETURN count(r)"));
    }
}

test "lnl: a write over many rows that fails on a later row changes nothing" {
    for (modes) |mode| {
        var buf: [128]u8 = undefined;
        const path = pathFor(&buf, "rows", mode);
        const db = try openFresh(path, configFor(mode));
        defer {
            db.close();
            removeFiles(path);
        }
        // The last M has a relationship, so deleting it is refused, after the
        // first two have gone.
        try run(db, "CREATE (:M {i: 1}), (:M {i: 2}), (:M {i: 3})-[:R]->(:X)");
        try std.testing.expectError(error.ExecutionError, run(db, "MATCH (m:M) WITH m ORDER BY m.i DELETE m"));
        try std.testing.expectEqual(@as(i64, 3), try count(db, "MATCH (m:M) RETURN count(m)"));

        // Rows that set properties and create nodes, then divide by zero.
        try std.testing.expectError(
            error.ExecutionError,
            run(db, "UNWIND [1, 2, 0] AS x MATCH (m:M {i: 1}) SET m.v = 10 / x CREATE (:Made {x: 10 / x})"),
        );
        try std.testing.expectEqual(@as(i64, 0), try count(db, "MATCH (m:M) WHERE m.v IS NOT NULL RETURN count(m)"));
        try std.testing.expectEqual(@as(i64, 0), try count(db, "MATCH (n:Made) RETURN count(n)"));

        // The same query without the bad row commits whole.
        try run(db, "UNWIND [1, 2, 5] AS x MATCH (m:M {i: 1}) SET m.v = 10 / x CREATE (:Made {x: 10 / x})");
        try std.testing.expectEqual(@as(i64, 3), try count(db, "MATCH (n:Made) RETURN count(n)"));
        try std.testing.expectEqual(@as(i64, 2), try count(db, "MATCH (m:M {i: 1}) RETURN m.v"));
    }
}

// ============================================================================
// Explicit transactions
// ============================================================================

test "lnl: explicit transactions without a log commit, roll back and keep one writer" {
    var buf: [128]u8 = undefined;
    const path = pathFor(&buf, "explicit", .no_log);
    const config = configFor(.no_log);
    var db = try openFresh(path, config);
    defer removeFiles(path);
    {
        defer db.close();
        var txn = try db.beginTransaction(.read_write);
        const a = try db.createNode(&txn, &.{"P"});
        try db.setNodeProperty(&txn, a, "name", .{ .string_val = "a" });
        try runInTxn(db, &txn, "CREATE (:P {name: 'b'})");
        // The transaction sees its own writes; nothing else does yet.
        try std.testing.expectEqual(@as(i64, 2), try countIn(db, &txn, "MATCH (n:P) RETURN count(n)"));
        try std.testing.expectEqual(@as(i64, 0), try count(db, "MATCH (n:P) RETURN count(n)"));
        // A second writer waits its turn, with or without a log.
        try std.testing.expectError(DatabaseError.TransactionConflict, db.beginTransaction(.read_write));
        try db.commitTransaction(&txn);
        try std.testing.expectEqual(@as(i64, 2), try count(db, "MATCH (n:P) RETURN count(n)"));

        var rolled = try db.beginTransaction(.read_write);
        _ = try db.createNode(&rolled, &.{"P"});
        try runInTxn(db, &rolled, "MATCH (n:P {name: 'a'}) SET n.name = 'changed'");
        try db.abortTransaction(&rolled);
        try std.testing.expectEqual(@as(i64, 2), try count(db, "MATCH (n:P) RETURN count(n)"));
        try std.testing.expectEqual(@as(i64, 1), try count(db, "MATCH (n:P {name: 'a'}) RETURN count(n)"));
        try std.testing.expectError(DatabaseError.TransactionNotActive, db.commitTransaction(&rolled));
    }
    // Close flushes, so the commit is in the file; nothing was logged.
    var wal_buf: [160]u8 = undefined;
    const wal_path = try std.fmt.bufPrint(&wal_buf, "{s}-wal", .{path});
    try std.testing.expectError(error.FileNotFound, @import("compat").fs.cwd().access(wal_path, .{}));
    db = try reopen(path, config);
    defer db.close();
    try std.testing.expectEqual(@as(i64, 2), try count(db, "MATCH (n:P) RETURN count(n)"));
}

// ============================================================================
// Direct writes
// ============================================================================

test "lnl: a refused direct write changes nothing, in a transaction or outside one" {
    for (modes) |mode| {
        var buf: [128]u8 = undefined;
        const path = pathFor(&buf, "direct", mode);
        const db = try openFresh(path, configFor(mode));
        defer {
            db.close();
            removeFiles(path);
        }
        // Outside a transaction a direct write is applied as it is made, in
        // both modes; one that is refused is refused before it changes
        // anything.
        const a = try db.createNode(null, &.{"P"});
        const b = try db.createNode(null, &.{"P"});
        try db.createEdge(null, a, b, "R");
        try std.testing.expectError(DatabaseError.NodeHasRelationships, db.deleteNode(null, a));
        try std.testing.expectError(DatabaseError.NotFound, db.createEdge(null, a, 999_999, "R"));
        try std.testing.expectError(DatabaseError.NotFound, db.setNodeProperty(null, 999_999, "x", .{ .int_val = 1 }));
        try std.testing.expectEqual(@as(i64, 2), try count(db, "MATCH (n:P) RETURN count(n)"));
        try std.testing.expectEqual(@as(i64, 1), try count(db, "MATCH ()-[r:R]->() RETURN count(r)"));

        // In a transaction, the writes before a refused one are the
        // transaction's, and go with it when it is rolled back.
        var txn = try db.beginTransaction(.read_write);
        const c = try db.createNode(&txn, &.{"P"});
        try db.setNodeProperty(&txn, a, "x", .{ .int_val = 1 });
        try db.deleteEdge(&txn, a, b, "R");
        try std.testing.expectError(DatabaseError.NotFound, db.createEdge(&txn, c, 999_999, "R"));
        try db.abortTransaction(&txn);
        try std.testing.expectEqual(@as(i64, 2), try count(db, "MATCH (n:P) RETURN count(n)"));
        try std.testing.expectEqual(@as(i64, 1), try count(db, "MATCH ()-[r:R]->() RETURN count(r)"));
        try std.testing.expectEqual(@as(i64, 0), try count(db, "MATCH (n:P) WHERE n.x IS NOT NULL RETURN count(n)"));
    }
}

// ============================================================================
// Readers
// ============================================================================

test "lnl: readers keep their snapshot while a transaction without a log writes and commits" {
    for (modes) |mode| {
        var buf: [128]u8 = undefined;
        const path = pathFor(&buf, "readers", mode);
        const db = try openFresh(path, configFor(mode));
        defer {
            db.close();
            removeFiles(path);
        }
        try run(db, "CREATE (:P {v: 1})");

        var reader = try db.beginTransaction(.read_only);
        var writer = try db.beginTransaction(.read_write);
        try runInTxn(db, &writer, "MATCH (n:P) SET n.v = 2 CREATE (:P {v: 3})");

        // Before the commit: the reader, and a read outside any transaction,
        // see the committed state.
        try std.testing.expectEqual(@as(i64, 1), try countIn(db, &reader, "MATCH (n:P) RETURN count(n)"));
        try std.testing.expectEqual(@as(i64, 1), try count(db, "MATCH (n:P) RETURN count(n)"));
        try std.testing.expectEqual(@as(i64, 1), try countIn(db, &writer, "MATCH (n:P {v: 2}) RETURN count(n)"));

        try db.commitTransaction(&writer);

        // After it: the reader still sees what it began with; new reads see
        // the commit.
        try std.testing.expectEqual(@as(i64, 1), try countIn(db, &reader, "MATCH (n:P) RETURN count(n)"));
        try std.testing.expectEqual(@as(i64, 1), try countIn(db, &reader, "MATCH (n:P) RETURN n.v"));
        try std.testing.expectEqual(@as(i64, 2), try count(db, "MATCH (n:P) RETURN count(n)"));
        try std.testing.expectEqual(@as(i64, 1), try count(db, "MATCH (n:P {v: 2}) RETURN count(n)"));
        try db.abortTransaction(&reader);
    }
}

// ============================================================================
// Indexes and the changefeed
// ============================================================================

const Observed = struct {
    by_index: usize,
    by_text: usize,
    by_vector: usize,
    changes: usize,
};

fn observe(db: *Database) !Observed {
    const found = try db.findNodesByLabelProperty(null, "A", "n", .{ .int_val = 7 }, 10);
    defer allocator.free(found);
    const hits = try db.ftsSearchIndex(.node, "Doc", "text", "lantern", 10);
    defer db.freeFtsSearchResults(hits);
    const near = try db.vectorSearch(&[_]f32{ 1, 0, 0, 0 }, 10, null);
    defer db.freeVectorSearchResults(near);
    var batch = try db.readChanges(0, 1000, 0);
    defer batch.deinit();
    return .{ .by_index = found.len, .by_text = hits.len, .by_vector = near.len, .changes = batch.records.len };
}

fn indexedWrite(db: *Database, commit: bool) !void {
    var txn = try db.beginTransaction(.read_write);
    const id = try db.createNode(&txn, &.{ "A", "Doc" });
    try db.setNodeProperty(&txn, id, "n", .{ .int_val = 7 });
    try db.setNodeProperty(&txn, id, "text", .{ .string_val = "a lantern in the window" });
    try db.setNodeVectorInTxn(&txn, id, &[_]f32{ 1, 0, 0, 0 });
    if (commit) try db.commitTransaction(&txn) else try db.abortTransaction(&txn);
}

test "lnl: indexes, full-text, vectors and the changefeed follow commit and rollback the same way in both modes" {
    var seen: [2][3]Observed = undefined;
    for (modes, 0..) |mode, m| {
        var buf: [128]u8 = undefined;
        const path = pathFor(&buf, "indexes", mode);
        var config = configFor(mode);
        config.enable_vector = true;
        config.vector_dimensions = 4;
        const db = try openFresh(path, config);
        defer {
            db.close();
            removeFiles(path);
        }
        try db.createNodePropertyIndex("A", "n");
        try db.createNodeFtsIndex("Doc", "text");

        try indexedWrite(db, false);
        seen[m][0] = try observe(db);
        try indexedWrite(db, true);
        seen[m][1] = try observe(db);
        // A query outside a transaction is committed the same way.
        try run(db, "MATCH (n:A {n: 7}) SET n.n = 8");
        seen[m][2] = try observe(db);

        try std.testing.expectEqual(Observed{ .by_index = 0, .by_text = 0, .by_vector = 0, .changes = 0 }, seen[m][0]);
        try std.testing.expectEqual(@as(usize, 1), seen[m][1].by_index);
        try std.testing.expectEqual(@as(usize, 1), seen[m][1].by_text);
        try std.testing.expectEqual(@as(usize, 1), seen[m][1].by_vector);
        try std.testing.expect(seen[m][1].changes > 0);
        try std.testing.expectEqual(@as(usize, 0), seen[m][2].by_index);
        try std.testing.expect(seen[m][2].changes > seen[m][1].changes);
    }
    try std.testing.expectEqualDeep(seen[0], seen[1]);
}

// ============================================================================
// Failure and crashes
// ============================================================================

test "lnl: a commit that cannot be applied without a log fails the handle, and the file keeps the last flush" {
    var buf: [128]u8 = undefined;
    const path = pathFor(&buf, "failed_apply", .no_log);
    const config = configFor(.no_log);
    var db = try openFresh(path, config);
    defer removeFiles(path);
    {
        try run(db, "CREATE (:P {i: 1})");
        try db.sync();
        try run(db, "CREATE (:P {i: 2})");

        var txn = try db.beginTransaction(.read_write);
        _ = try db.createNode(&txn, &.{"P"});
        db.fail_next_apply_for_testing = true;
        try std.testing.expectError(DatabaseError.IoError, db.commitTransaction(&txn));
        // Its trees may hold part of the transaction, and no log can finish
        // it, so the handle writes nothing more.
        try std.testing.expectError(DatabaseError.IoError, db.sync());
        try std.testing.expectError(DatabaseError.IoError, db.beginTransaction(.read_write));
        db.close();
    }
    // What the file holds is the state at the last flush: whole
    // transactions, the later commit lost with the failed one.
    db = try reopen(path, config);
    defer db.close();
    try std.testing.expectEqual(@as(i64, 1), try count(db, "MATCH (n:P) RETURN count(n)"));
}
