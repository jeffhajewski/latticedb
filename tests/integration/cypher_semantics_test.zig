//! End-to-end tests for Cypher expression semantics: null handling, integer
//! arithmetic, aggregates and projection limits, run through db.query().
//!
//! The evaluator's own rules are covered by unit tests. These check that the
//! rules reach the user: that WHERE keeps a row only when its predicate is
//! true, that grouping still treats nulls as one value, and that errors come
//! back as errors rather than as wrong answers.

const std = @import("std");
const lattice = @import("lattice");

const Database = lattice.storage.database.Database;
const QueryError = lattice.storage.database.QueryError;
const QueryResult = lattice.storage.database.QueryResult;

fn openTestDb(path: []const u8) !*Database {
    @import("compat").fs.cwd().deleteFile(path) catch {};
    return try Database.open(std.testing.allocator, path, .{
        .create = true,
        .config = .{ .enable_wal = false },
    });
}

fn cleanupTestDb(db: *Database, path: []const u8) void {
    db.close();
    @import("compat").fs.cwd().deleteFile(path) catch {};
}

fn run(db: *Database, cypher: []const u8) !void {
    var result = try db.query(cypher);
    result.deinit();
}

/// Three :Rare nodes: x = 5, x = 1, and one with no x at all.
fn seedRare(db: *Database) !void {
    try run(db, "CREATE (n:Rare {name: \"five\", x: 5})");
    try run(db, "CREATE (n:Rare {name: \"one\", x: 1})");
    try run(db, "CREATE (n:Rare {name: \"none\"})");
}

fn queryCount(db: *Database, cypher: []const u8) !i64 {
    var result = try db.query(cypher);
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), result.rowCount());
    return switch (result.rows[0].values[0]) {
        .int_val => |v| v,
        else => error.UnexpectedValueType,
    };
}

fn expectInt(result: QueryResult, row: usize, col: usize, expected: i64) !void {
    switch (result.rows[row].values[col]) {
        .int_val => |v| try std.testing.expectEqual(expected, v),
        else => return error.UnexpectedValueType,
    }
}

fn expectNull(result: QueryResult, row: usize, col: usize) !void {
    try std.testing.expect(result.rows[row].values[col] == .null_val);
}

// ============================================================================
// Three-valued logic
// ============================================================================

test "cypher: comparisons and NOT with null return null" {
    const path = "/tmp/lattice_cs_null_literals.ltdb";
    const db = try openTestDb(path);
    defer cleanupTestDb(db, path);

    var result = try db.query(
        "UNWIND [1] AS x RETURN null = null AS a, null <> 1 AS b, NOT null AS c, true AND null AS d, true OR null AS e",
    );
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 1), result.rowCount());
    try expectNull(result, 0, 0);
    try expectNull(result, 0, 1);
    try expectNull(result, 0, 2);
    try expectNull(result, 0, 3);
    switch (result.rows[0].values[4]) {
        .bool_val => |v| try std.testing.expect(v),
        else => return error.UnexpectedValueType,
    }
}

test "cypher: WHERE drops rows whose predicate is null" {
    const path = "/tmp/lattice_cs_where_null.ltdb";
    const db = try openTestDb(path);
    defer cleanupTestDb(db, path);
    try seedRare(db);

    // n.missing > 3 is null for every node, so NOT of it is null too.
    try std.testing.expectEqual(@as(i64, 0), try queryCount(db, "MATCH (n:Rare) WHERE NOT n.missing > 3 RETURN count(n)"));
    // A node with no x is neither equal nor unequal to 5.
    try std.testing.expectEqual(@as(i64, 1), try queryCount(db, "MATCH (n:Rare) WHERE n.x <> 5 RETURN count(n)"));
    try std.testing.expectEqual(@as(i64, 1), try queryCount(db, "MATCH (n:Rare) WHERE NOT n.x = 5 RETURN count(n)"));
    // x = 5: true AND null is null, NOT null is null: dropped.
    // x = 1: false AND null is false, NOT false is true: kept.
    // no x: null AND null is null: dropped.
    try std.testing.expectEqual(@as(i64, 1), try queryCount(db, "MATCH (n:Rare) WHERE NOT (n.x > 3 AND n.missing = 1) RETURN count(n)"));
    // true OR null is true, false OR null is null.
    try std.testing.expectEqual(@as(i64, 1), try queryCount(db, "MATCH (n:Rare) WHERE n.x > 3 OR n.missing = 1 RETURN count(n)"));
    // IS NULL remains two-valued.
    try std.testing.expectEqual(@as(i64, 1), try queryCount(db, "MATCH (n:Rare) WHERE n.x IS NULL RETURN count(n)"));
    try std.testing.expectEqual(@as(i64, 2), try queryCount(db, "MATCH (n:Rare) WHERE n.x IS NOT NULL RETURN count(n)"));
    // WITH ... WHERE filters the same way.
    try std.testing.expectEqual(@as(i64, 1), try queryCount(db, "MATCH (n:Rare) WITH n WHERE n.x <> 5 RETURN count(n)"));
}

test "cypher: an inline null property never matches" {
    const path = "/tmp/lattice_cs_inline_null.ltdb";
    const db = try openTestDb(path);
    defer cleanupTestDb(db, path);
    try seedRare(db);

    // `(n {x: null})` means `n.x = null`, which is never true.
    try std.testing.expectEqual(@as(i64, 0), try queryCount(db, "MATCH (n:Rare {x: null}) RETURN count(n)"));
    // A non-null inline property behaves as before.
    try std.testing.expectEqual(@as(i64, 1), try queryCount(db, "MATCH (n:Rare {x: 5}) RETURN count(n)"));
}

test "cypher: DISTINCT, grouping and ORDER BY still treat nulls as one value" {
    const path = "/tmp/lattice_cs_null_grouping.ltdb";
    const db = try openTestDb(path);
    defer cleanupTestDb(db, path);
    try seedRare(db);
    try run(db, "CREATE (n:Rare {name: \"none2\"})");

    {
        var result = try db.query("MATCH (n:Rare) RETURN DISTINCT n.missing");
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), result.rowCount());
        try expectNull(result, 0, 0);
    }
    {
        var result = try db.query("MATCH (n:Rare) RETURN n.missing AS k, count(n) AS c");
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), result.rowCount());
        try expectNull(result, 0, 0);
        try expectInt(result, 0, 1, 4);
    }
    {
        var result = try db.query("MATCH (n:Rare) RETURN n.x AS k, count(n) AS c ORDER BY k");
        defer result.deinit();
        // 1, 5, and one group for the two nodes with no x.
        try std.testing.expectEqual(@as(usize, 3), result.rowCount());
        try expectInt(result, 0, 0, 1);
        try expectInt(result, 1, 0, 5);
        try expectNull(result, 2, 0);
        try expectInt(result, 2, 1, 2);
    }
}

test "cypher: MERGE on an existing property still matches" {
    const path = "/tmp/lattice_cs_merge_match.ltdb";
    const db = try openTestDb(path);
    defer cleanupTestDb(db, path);
    try seedRare(db);

    try run(db, "MERGE (n:Rare {x: 5})");
    try std.testing.expectEqual(@as(i64, 3), try queryCount(db, "MATCH (n:Rare) RETURN count(n)"));
}
