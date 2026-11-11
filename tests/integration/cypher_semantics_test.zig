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

// ============================================================================
// Integer arithmetic
// ============================================================================

test "cypher: % takes the sign of the dividend" {
    const path = "/tmp/lattice_cs_modulo.ltdb";
    const db = try openTestDb(path);
    defer cleanupTestDb(db, path);

    var result = try db.query("UNWIND [-7] AS x RETURN x % 2 AS a, 7 % -2 AS b, x % -2 AS c");
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), result.rowCount());
    try expectInt(result, 0, 0, -1);
    try expectInt(result, 0, 1, 1);
    try expectInt(result, 0, 2, -1);
}

test "cypher: integer division or remainder by zero fails the query" {
    const path = "/tmp/lattice_cs_div_zero.ltdb";
    const db = try openTestDb(path);
    defer cleanupTestDb(db, path);

    try std.testing.expectError(QueryError.ExecutionError, db.query("UNWIND [0] AS z RETURN 7 % z"));
    try std.testing.expectError(QueryError.ExecutionError, db.query("UNWIND [0] AS z RETURN 7 / z"));
}

// ============================================================================
// count(*)
// ============================================================================

test "cypher: count(*) counts rows" {
    const path = "/tmp/lattice_cs_count_star.ltdb";
    const db = try openTestDb(path);
    defer cleanupTestDb(db, path);
    try seedRare(db);

    {
        var result = try db.query("MATCH (n:Rare) RETURN count(*)");
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), result.rowCount());
        try std.testing.expectEqualStrings("count(*)", result.columns[0]);
        try expectInt(result, 0, 0, 3);
    }
    {
        // Unlike count(n.x), count(*) counts the node with no x.
        var result = try db.query("MATCH (n:Rare) RETURN count(*) AS rows, count(n.x) AS xs");
        defer result.deinit();
        try expectInt(result, 0, 0, 3);
        try expectInt(result, 0, 1, 2);
    }
    {
        var result = try db.query("MATCH (n:Rare) RETURN n.x > 3 AS big, count(*) AS c ORDER BY c DESC");
        defer result.deinit();
        // Groups true, false and null, one row each.
        try std.testing.expectEqual(@as(usize, 3), result.rowCount());
        for (0..3) |i| try expectInt(result, i, 1, 1);
    }
    try std.testing.expectEqual(@as(i64, 0), try queryCount(db, "MATCH (n:Nothing) RETURN count(*)"));
}

// ============================================================================
// Projection width
// ============================================================================

test "cypher: RETURN and WITH accept up to 16 items" {
    const path = "/tmp/lattice_cs_sixteen_items.ltdb";
    const db = try openTestDb(path);
    defer cleanupTestDb(db, path);

    var result = try db.query("UNWIND [7] AS x RETURN x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x");
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), result.rowCount());
    try std.testing.expectEqual(@as(usize, 16), result.columns.len);
    try expectInt(result, 0, 15, 7);

    var with_result = try db.query(
        "UNWIND [7] AS x WITH x AS a, x AS b, x AS c, x AS d, x AS e, x AS f, x AS g, x AS h, " ++
            "x AS i, x AS j, x AS k, x AS l, x AS m, x AS n, x AS o, x AS p RETURN p",
    );
    defer with_result.deinit();
    try expectInt(with_result, 0, 0, 7);
}

test "cypher: more than 16 RETURN or WITH items is a plan error" {
    const path = "/tmp/lattice_cs_seventeen_items.ltdb";
    const db = try openTestDb(path);
    defer cleanupTestDb(db, path);

    try std.testing.expectError(
        QueryError.PlanError,
        db.query("UNWIND [7] AS x RETURN x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x"),
    );
    try std.testing.expectError(
        QueryError.PlanError,
        db.query("UNWIND [7] AS x RETURN x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, count(x)"),
    );
    try std.testing.expectError(
        QueryError.PlanError,
        db.query("UNWIND [7] AS x WITH x AS a, x AS b, x AS c, x AS d, x AS e, x AS f, x AS g, x AS h, " ++
            "x AS i, x AS j, x AS k, x AS l, x AS m, x AS n, x AS o, x AS p, x AS q RETURN q"),
    );

    var detailed = try db.queryDetailed("UNWIND [7] AS x RETURN x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x");
    defer detailed.deinit();
    try std.testing.expect(detailed == .failure);
    try std.testing.expectEqualStrings("RETURN has 17 items, but at most 16 are supported.", detailed.failure.message);
}

test "cypher: integer overflow fails the query" {
    const path = "/tmp/lattice_cs_overflow.ltdb";
    const db = try openTestDb(path);
    defer cleanupTestDb(db, path);

    try run(db, "CREATE (n:Big {v: 9223372036854775807})");
    try std.testing.expectError(QueryError.ExecutionError, db.query("MATCH (n:Big) RETURN n.v + 1"));
    try std.testing.expectError(QueryError.ExecutionError, db.query("MATCH (n:Big) RETURN n.v * 2"));
    try std.testing.expectError(QueryError.ExecutionError, db.query("MATCH (n:Big) RETURN -n.v - 2"));
    try std.testing.expectError(QueryError.ExecutionError, db.query("MATCH (n:Big) WHERE n.v + 1 > 0 RETURN n"));

    var result = try db.query("MATCH (n:Big) RETURN n.v - 1");
    defer result.deinit();
    try expectInt(result, 0, 0, 9223372036854775806);
}

// ============================================================================
// Queries that start with RETURN or WITH
// ============================================================================

test "cypher: a leading RETURN or WITH evaluates against one empty row" {
    const path = "/tmp/lattice_cs_leading_return.ltdb";
    const db = try openTestDb(path);
    defer cleanupTestDb(db, path);

    {
        var result = try db.query("RETURN 1");
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), result.rowCount());
        try expectInt(result, 0, 0, 1);
    }
    {
        var result = try db.query("RETURN 1 + 2 AS three, -7 % 2 AS rem, null = null AS unknown");
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), result.rowCount());
        try std.testing.expectEqualStrings("three", result.columns[0]);
        try expectInt(result, 0, 0, 3);
        try expectInt(result, 0, 1, -1);
        try expectNull(result, 0, 2);
    }
    {
        var result = try db.query("WITH 2 AS x RETURN x * 3 AS y");
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), result.rowCount());
        try expectInt(result, 0, 0, 6);
    }
    {
        var result = try db.query("WITH 2 AS x WHERE x > 5 RETURN x");
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 0), result.rowCount());
    }
    try std.testing.expectEqual(@as(i64, 1), try queryCount(db, "RETURN count(*)"));
    {
        var params = std.StringHashMap(lattice.core.types.PropertyValue).init(std.testing.allocator);
        defer params.deinit();
        try params.put("p", .{ .int_val = 41 });
        var result = try db.queryWithParams("RETURN $p + 1 AS answer", &params);
        defer result.deinit();
        try expectInt(result, 0, 0, 42);
    }
}

// ============================================================================
// Variables bound to null
// ============================================================================

test "cypher: a variable bound to null reads as null" {
    const path = "/tmp/lattice_cs_bound_null.ltdb";
    const db = try openTestDb(path);
    defer cleanupTestDb(db, path);
    try seedRare(db);

    {
        var result = try db.query("UNWIND [1, null] AS v RETURN v");
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 2), result.rowCount());
        try expectInt(result, 0, 0, 1);
        try expectNull(result, 1, 0);
    }
    {
        var result = try db.query("MATCH (n:Rare) WITH n.x AS x RETURN x ORDER BY x");
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 3), result.rowCount());
        try expectInt(result, 0, 0, 1);
        try expectInt(result, 1, 0, 5);
        try expectNull(result, 2, 0);
    }
    {
        // The null group key comes out of the aggregate and is read again.
        var result = try db.query("MATCH (n:Rare) WITH n.x > 3 AS big, count(n) AS c RETURN big, c ORDER BY big");
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 3), result.rowCount());
        try expectNull(result, 2, 0);
        try expectInt(result, 2, 1, 1);
    }
    try std.testing.expectEqual(@as(i64, 1), try queryCount(db, "UNWIND [1, null] AS v WITH v WHERE v IS NULL RETURN count(*)"));
    try std.testing.expectEqual(@as(i64, 1), try queryCount(db, "MATCH (n:Rare) WITH n.x AS x WHERE x IS NULL RETURN count(x IS NULL)"));
}

// ============================================================================
// Column names
// ============================================================================

test "cypher: an unaliased expression column is named by its text" {
    const path = "/tmp/lattice_cs_column_text.ltdb";
    const db = try openTestDb(path);
    defer cleanupTestDb(db, path);

    {
        // The column used to be called `y`, after the variable that shared
        // its slot.
        var result = try db.query("UNWIND [1] AS y RETURN null = null");
        defer result.deinit();
        try std.testing.expectEqualStrings("null = null", result.columns[0]);
    }
    {
        var result = try db.query("UNWIND [1] AS y RETURN y, y + 1, coalesce(null, y), y AS z, 'a'");
        defer result.deinit();
        try std.testing.expectEqualStrings("y", result.columns[0]);
        try std.testing.expectEqualStrings("y + 1", result.columns[1]);
        try std.testing.expectEqualStrings("coalesce(null, y)", result.columns[2]);
        try std.testing.expectEqualStrings("z", result.columns[3]);
        try std.testing.expectEqualStrings("'a'", result.columns[4]);
    }
    {
        var result = try db.query("UNWIND [1, 2] AS y RETURN y % 2 = 0, count(*)");
        defer result.deinit();
        try std.testing.expectEqualStrings("y % 2 = 0", result.columns[0]);
        try std.testing.expectEqualStrings("count(*)", result.columns[1]);
    }
}

// ============================================================================
// Failing queries release what they acquired
// ============================================================================

test "cypher: a query that fails while an operator opens leaks nothing" {
    // The testing allocator reports a leak, so each query failing here used
    // to fail this test: the label scan's id list was freed only on close,
    // and an operator that drains its input while opening never closed it.
    const path = "/tmp/lattice_cs_open_leak.ltdb";
    const db = try openTestDb(path);
    defer cleanupTestDb(db, path);
    try seedRare(db);
    try run(db, "CREATE (n:Other {x: 2})");

    // Aggregate drains its input in open.
    try std.testing.expectError(QueryError.ExecutionError, db.query("MATCH (n:Rare) RETURN count(n.x / 0)"));
    // ORDER BY drains its input in open.
    try std.testing.expectError(QueryError.ExecutionError, db.query("MATCH (n:Rare) RETURN n.x / 0 AS q ORDER BY q"));
    // DISTINCT drains its input in open.
    try std.testing.expectError(QueryError.ExecutionError, db.query("MATCH (n:Rare) RETURN DISTINCT n.x / 0"));
    // A cross product drains its inner side in open.
    try std.testing.expectError(QueryError.ExecutionError, db.query("MATCH (n:Rare), (m:Other) WITH n, m, m.x / 0 AS q RETURN count(q)"));
    try std.testing.expectError(QueryError.ExecutionError, db.query("MATCH (m:Other) WITH m, m.x / 0 AS q MATCH (n:Rare) RETURN count(*)"));
    // The same database still answers.
    try std.testing.expectEqual(@as(i64, 3), try queryCount(db, "MATCH (n:Rare) RETURN count(n)"));
}

// ============================================================================
// Float-to-integer conversion and range()
// ============================================================================

test "cypher: toInteger of an out-of-range float fails the query" {
    const path = "/tmp/lattice_cs_to_integer_range.ltdb";
    const db = try openTestDb(path);
    defer cleanupTestDb(db, path);

    // These were undefined behaviour in a release build and a panic in a
    // debug one.
    try std.testing.expectError(QueryError.ExecutionError, db.query("RETURN toInteger(1e300)"));
    try std.testing.expectError(QueryError.ExecutionError, db.query("RETURN toInteger(-1e300)"));
    try std.testing.expectError(QueryError.ExecutionError, db.query("RETURN toInteger(9223372036854775808.0)"));
    try std.testing.expectError(QueryError.ExecutionError, db.query("RETURN substring('abc', 1e300)"));
    try std.testing.expectError(QueryError.ExecutionError, db.query("RETURN range(0, 1e300)"));

    var result = try db.query("RETURN toInteger(2.9) AS a, toInteger(-2.9) AS b, toInteger(-9223372036854775807.0) AS c");
    defer result.deinit();
    try expectInt(result, 0, 0, 2);
    try expectInt(result, 0, 1, -2);
    try expectInt(result, 0, 2, -9223372036854775807 - 1);
}

test "cypher: range() is bounded and cannot overflow" {
    const path = "/tmp/lattice_cs_range_bounds.ltdb";
    const db = try openTestDb(path);
    defer cleanupTestDb(db, path);

    // An unbounded allocation before; now an error, also through UNWIND,
    // which used to swallow evaluation errors and return no rows.
    try std.testing.expectError(QueryError.ExecutionError, db.query("RETURN size(range(0, 9223372036854775807))"));
    try std.testing.expectError(QueryError.ExecutionError, db.query("UNWIND range(0, 99999999) AS x RETURN count(x)"));

    {
        // `i += step` overflowed past the top of the i64 range.
        var result = try db.query("RETURN range(9223372036854775806, 9223372036854775807) AS r");
        defer result.deinit();
        const items = result.rows[0].values[0].list_val;
        try std.testing.expectEqual(@as(usize, 2), items.len);
        try std.testing.expectEqual(@as(i64, 9223372036854775807), items[1].int_val);
    }
    {
        var result = try db.query("RETURN size(range(5, 1, -2)) AS a, size(range(1, 0)) AS b, size(range(0, 10, 4)) AS c");
        defer result.deinit();
        try expectInt(result, 0, 0, 3);
        try expectInt(result, 0, 1, 0);
        try expectInt(result, 0, 2, 3);
    }
    try std.testing.expectEqual(@as(i64, 3), try queryCount(db, "UNWIND range(1, 3) AS x RETURN count(x)"));
}

test "cypher: UNWIND of an expression that fails fails the query" {
    const path = "/tmp/lattice_cs_unwind_error.ltdb";
    const db = try openTestDb(path);
    defer cleanupTestDb(db, path);

    // A missing parameter used to produce zero rows.
    try std.testing.expectError(QueryError.ExecutionError, db.query("UNWIND $missing AS x RETURN x"));
    try std.testing.expectError(QueryError.ExecutionError, db.query("UNWIND [1 / 0] AS x RETURN x"));
}
