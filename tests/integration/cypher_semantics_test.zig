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
// Aggregates
// ============================================================================

test "cypher: sum of integers is an integer, and overflow fails" {
    const path = "/tmp/lattice_cs_sum_int.ltdb";
    const db = try openTestDb(path);
    defer cleanupTestDb(db, path);

    {
        var result = try db.query("UNWIND [10, 25, null] AS x RETURN sum(x) AS s");
        defer result.deinit();
        try expectInt(result, 0, 0, 35);
    }
    {
        // Exact past 2^53, where a float sum loses the 1.
        var result = try db.query("UNWIND [9007199254740992, 1] AS x RETURN sum(x) AS s");
        defer result.deinit();
        try expectInt(result, 0, 0, 9007199254740993);
    }
    {
        var result = try db.query("UNWIND [1, 2.5] AS x RETURN sum(x) AS s");
        defer result.deinit();
        try std.testing.expectEqual(@as(f64, 3.5), result.rows[0].values[0].float_val);
    }
    try std.testing.expectEqual(@as(i64, 0), try queryCount(db, "UNWIND [] AS x RETURN sum(x)"));
    try std.testing.expectError(QueryError.ExecutionError, db.query("UNWIND [9223372036854775807, 1] AS x RETURN sum(x)"));
    try std.testing.expectError(QueryError.ExecutionError, db.query("UNWIND [1, 'a'] AS x RETURN sum(x)"));
}

test "cypher: aggregates skip nulls" {
    const path = "/tmp/lattice_cs_agg_nulls.ltdb";
    const db = try openTestDb(path);
    defer cleanupTestDb(db, path);

    var result = try db.query("UNWIND [10, null, 30] AS x RETURN avg(x) AS a, collect(x) AS c, min(x) AS lo, max(x) AS hi");
    defer result.deinit();
    try std.testing.expectEqual(@as(f64, 20.0), result.rows[0].values[0].float_val);
    try std.testing.expectEqual(@as(usize, 2), result.rows[0].values[1].list_val.len);
    try expectInt(result, 0, 2, 10);
    try expectInt(result, 0, 3, 30);

    var none = try db.query("UNWIND [null] AS x RETURN avg(x) AS a, collect(x) AS c");
    defer none.deinit();
    try expectNull(none, 0, 0);
    try std.testing.expectEqual(@as(usize, 0), none.rows[0].values[1].list_val.len);
}

// ============================================================================
// Projection width
// ============================================================================

test "cypher: RETURN and WITH accept up to 16 items" {
    const path = "/tmp/lattice_cs_sixteen_items.ltdb";
    const db = try openTestDb(path);
    defer cleanupTestDb(db, path);

    var result = try db.query("UNWIND [7] AS x RETURN " ++
        "x AS c1, x AS c2, x AS c3, x AS c4, x AS c5, x AS c6, x AS c7, x AS c8, x AS c9, " ++
        "x AS c10, x AS c11, x AS c12, x AS c13, x AS c14, x AS c15, x AS c16");
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
        db.query("UNWIND [7] AS x RETURN " ++
            "x AS c1, x AS c2, x AS c3, x AS c4, x AS c5, x AS c6, x AS c7, x AS c8, x AS c9, " ++
            "x AS c10, x AS c11, x AS c12, x AS c13, x AS c14, x AS c15, x AS c16, x AS c17"),
    );
    try std.testing.expectError(
        QueryError.PlanError,
        db.query("UNWIND [7] AS x RETURN " ++
            "x AS c1, x AS c2, x AS c3, x AS c4, x AS c5, x AS c6, x AS c7, x AS c8, x AS c9, " ++
            "x AS c10, x AS c11, x AS c12, x AS c13, x AS c14, x AS c15, x AS c16, count(x)"),
    );
    try std.testing.expectError(
        QueryError.PlanError,
        db.query("UNWIND [7] AS x WITH x AS a, x AS b, x AS c, x AS d, x AS e, x AS f, x AS g, x AS h, " ++
            "x AS i, x AS j, x AS k, x AS l, x AS m, x AS n, x AS o, x AS p, x AS q RETURN q"),
    );

    var detailed = try db.queryDetailed("UNWIND [7] AS x RETURN " ++
        "x AS c1, x AS c2, x AS c3, x AS c4, x AS c5, x AS c6, x AS c7, x AS c8, x AS c9, " ++
        "x AS c10, x AS c11, x AS c12, x AS c13, x AS c14, x AS c15, x AS c16, x AS c17");
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

// ============================================================================
// Relationship patterns
// ============================================================================

/// a-[:A]->b, a-[:B]->c, a-[:C]->d, b-[:B]->d
fn seedTypes(db: *Database) !void {
    try run(db, "CREATE (a:P {name: 'a'}), (b:P {name: 'b'}), (c:P {name: 'c'}), (d:P {name: 'd'}), " ++
        "(a)-[:A]->(b), (a)-[:B]->(c), (a)-[:C]->(d), (b)-[:B]->(d)");
}

fn expectNames(db: *Database, cypher: []const u8, expected: []const []const u8) !void {
    var result = try db.query(cypher);
    defer result.deinit();
    try std.testing.expectEqual(expected.len, result.rowCount());
    for (expected, 0..) |name, i| {
        switch (result.rows[i].values[0]) {
            .string_val => |s| try std.testing.expectEqualStrings(name, s),
            else => return error.UnexpectedValueType,
        }
    }
}

test "cypher: a relationship pattern can name alternative types" {
    const path = "/tmp/lattice_cs_type_alternation.ltdb";
    const db = try openTestDb(path);
    defer cleanupTestDb(db, path);
    try seedTypes(db);

    try expectNames(db, "MATCH (s:P {name: 'a'})-[:A|B]->(x) RETURN x.name ORDER BY x.name", &.{ "b", "c" });
    // The older `:A|:C` spelling means the same.
    try expectNames(db, "MATCH (s:P {name: 'a'})-[r:A|:C]->(x) RETURN type(r) ORDER BY x.name", &.{ "A", "C" });
    try expectNames(db, "MATCH (s:P {name: 'd'})<-[:A|B|C]-(x) RETURN x.name ORDER BY x.name", &.{ "a", "b" });
    try expectNames(db, "MATCH (s:P {name: 'a'})-[:A|Unknown]-(x) RETURN x.name", &.{"b"});
    try expectNames(db, "MATCH (s:P {name: 'a'})-[:A|B*2]->(x) RETURN x.name", &.{"d"});

    // `:A:B` would ask for an edge with two types, which none has.
    try std.testing.expectError(QueryError.ParseError, db.query("MATCH (s)-[:A:B]->(x) RETURN x"));
    // CREATE and MERGE have to know which single type to make.
    try std.testing.expectError(QueryError.SemanticError, db.query("MATCH (a:P {name: 'a'}), (b:P {name: 'b'}) CREATE (a)-[:A|B]->(b)"));
    try std.testing.expectError(QueryError.SemanticError, db.query("MATCH (a:P {name: 'a'}), (b:P {name: 'b'}) MERGE (a)-[:A|B]->(b)"));
}

// ============================================================================
// Variable-length patterns
// ============================================================================

/// a->b, b->c, a->c, c->d, all :R.
fn seedDiamond(db: *Database) !void {
    try run(db, "CREATE (a:N {name: 'a'}), (b:N {name: 'b'}), (c:N {name: 'c'}), (d:N {name: 'd'}), " ++
        "(a)-[:R]->(b), (b)-[:R]->(c), (a)-[:R]->(c), (c)-[:R]->(d)");
}

test "cypher: a variable-length pattern returns every path within its bounds" {
    const path = "/tmp/lattice_cs_varlength_paths.ltdb";
    const db = try openTestDb(path);
    defer cleanupTestDb(db, path);
    try seedDiamond(db);

    // One row per path: c is reached two ways, and d only through a->c->d.
    // The old walk marked nodes visited at the last hop and never unmarked
    // them, so it returned b and c and missed the rest.
    try expectNames(db, "MATCH (s:N {name: 'a'})-[:R*1..2]->(x) RETURN x.name ORDER BY x.name", &.{ "b", "c", "c", "d" });
    try expectNames(db, "MATCH (s:N {name: 'a'})-[:R*]->(x) RETURN x.name ORDER BY x.name", &.{ "b", "c", "c", "d", "d" });
    try expectNames(db, "MATCH (s:N {name: 'a'})-[:R*0..1]->(x) RETURN x.name ORDER BY x.name", &.{ "a", "b", "c" });
    try expectNames(db, "MATCH (s:N {name: 'd'})<-[:R*2..3]-(x) RETURN x.name ORDER BY x.name", &.{ "a", "a", "b" });
    // Undirected: nodes repeat across paths, edges never repeat within one.
    try expectNames(db, "MATCH (s:N {name: 'a'})-[:R*1..2]-(x) RETURN x.name ORDER BY x.name", &.{ "b", "b", "c", "c", "d" });
}

test "cypher: a variable-length path may revisit a node but not reuse an edge" {
    const path = "/tmp/lattice_cs_varlength_cycle.ltdb";
    const db = try openTestDb(path);
    defer cleanupTestDb(db, path);
    try run(db, "CREATE (a:N {name: 'a'}), (b:N {name: 'b'}), (a)-[:R]->(b), (b)-[:R]->(a)");

    // a->b->a comes back to the source, which node uniqueness refused.
    try expectNames(db, "MATCH (s:N {name: 'a'})-[:R*]->(x) RETURN x.name ORDER BY x.name", &.{ "a", "b" });
    // Undirected, from a: a-b by either edge, then back to a by the other.
    try expectNames(db, "MATCH (s:N {name: 'a'})-[:R*]-(x) RETURN x.name ORDER BY x.name", &.{ "a", "a", "b", "b" });
}

test "cypher: an unbounded variable-length pattern is not cut off at 100 hops" {
    const path = "/tmp/lattice_cs_varlength_long.ltdb";
    const db = try openTestDb(path);
    defer cleanupTestDb(db, path);
    // A chain of 120 :NEXT edges, built eight nodes per CREATE and then
    // stitched together, which keeps every statement small.
    const per_batch = 8;
    const batches = 15;
    var buf: [512]u8 = undefined;
    for (0..batches) |b| {
        var stream = std.Io.Writer.fixed(&buf);
        try stream.writeAll("CREATE ");
        for (0..per_batch) |k| {
            const i = b * per_batch + k + 1;
            if (k > 0) try stream.writeAll("-[:NEXT]->");
            try stream.print("(:C {{i: {d}}})", .{i});
        }
        try run(db, stream.buffered());
    }
    for (1..batches) |b| {
        const last = b * per_batch;
        const q = try std.fmt.bufPrint(&buf, "MATCH (a:C {{i: {d}}}), (b:C {{i: {d}}}) CREATE (a)-[:NEXT]->(b)", .{ last, last + 1 });
        try run(db, q);
    }

    // The walk used to stop silently at 100 hops.
    try std.testing.expectEqual(@as(i64, 119), try queryCount(db, "MATCH (s:C {i: 1})-[:NEXT*]->(x) RETURN count(x)"));
    try std.testing.expectEqual(@as(i64, 120), try queryCount(db, "MATCH (s:C {i: 1})-[:NEXT*]->(x) RETURN max(x.i)"));
}

// ============================================================================
// Relationship uniqueness
// ============================================================================

/// Alice-KNOWS->Bob-KNOWS->Carol, and a LOOP from Alice to herself.
fn seedKnows(db: *Database) !void {
    try run(db, "CREATE (a:P {name: 'Alice'}), (b:P {name: 'Bob'}), (c:P {name: 'Carol'}), " ++
        "(a)-[:KNOWS]->(b), (b)-[:KNOWS]->(c), (a)-[:LOOP]->(a)");
}

test "cypher: a MATCH never binds one relationship twice" {
    const path = "/tmp/lattice_cs_rel_uniqueness.ltdb";
    const db = try openTestDb(path);
    defer cleanupTestDb(db, path);
    try seedKnows(db);

    // Walking back over the Alice-Bob edge used to return Alice herself.
    try expectNames(db, "MATCH (a:P {name: 'Alice'})-[:KNOWS]-(b)-[:KNOWS]-(c) RETURN c.name", &.{"Carol"});
    // Across comma-separated patterns of one MATCH too: two KNOWS edges
    // make two ordered pairs of distinct edges, not four.
    try std.testing.expectEqual(@as(i64, 2), try queryCount(db, "MATCH (a)-[:KNOWS]->(b), (c)-[:KNOWS]->(d) RETURN count(*)"));
    try std.testing.expectEqual(@as(i64, 2), try queryCount(db, "MATCH (a)-[r:KNOWS]->(b), (c)-[s:KNOWS]->(d) RETURN count(*)"));
    // A variable-length relationship skips edges bound before it.
    try expectNames(db, "MATCH (x:P {name: 'Alice'})-[:KNOWS]-(y)-[:KNOWS*1..2]-(z) RETURN z.name", &.{"Carol"});
    // Separate MATCH clauses may reuse an edge.
    try std.testing.expectEqual(@as(i64, 4), try queryCount(db, "MATCH (a)-[:KNOWS]->(b) MATCH (c)-[:KNOWS]->(d) RETURN count(*)"));
}

test "cypher: an undirected pattern matches a self-loop once" {
    const path = "/tmp/lattice_cs_self_loop.ltdb";
    const db = try openTestDb(path);
    defer cleanupTestDb(db, path);
    try seedKnows(db);

    try expectNames(db, "MATCH (a:P {name: 'Alice'})-[:LOOP]-(x) RETURN x.name", &.{"Alice"});
    try expectNames(db, "MATCH (a:P {name: 'Alice'})-[r]-(x) RETURN type(r) ORDER BY x.name", &.{ "LOOP", "KNOWS" });
}

// ============================================================================
// MERGE on nodes
// ============================================================================

test "cypher: MERGE without a label matches any node with the properties" {
    const path = "/tmp/lattice_cs_merge_no_label.ltdb";
    const db = try openTestDb(path);
    defer cleanupTestDb(db, path);

    // It used to create a node every time.
    try run(db, "MERGE (n {email: 'x'})");
    try run(db, "MERGE (n {email: 'x'})");
    try std.testing.expectEqual(@as(i64, 1), try queryCount(db, "MATCH (n) RETURN count(n)"));
    try run(db, "CREATE (:User {email: 'y'})");
    try run(db, "MERGE (n {email: 'y'})");
    try std.testing.expectEqual(@as(i64, 2), try queryCount(db, "MATCH (n) RETURN count(n)"));
}

test "cypher: MERGE checks every label" {
    const path = "/tmp/lattice_cs_merge_labels.ltdb";
    const db = try openTestDb(path);
    defer cleanupTestDb(db, path);
    try run(db, "CREATE (:A {k: 1})");

    // A node with only :A used to match `:A:B` and come back without :B.
    try std.testing.expectEqual(@as(i64, 2), try queryCount(db, "MERGE (n:A:B {k: 1}) RETURN size(labels(n))"));
    try std.testing.expectEqual(@as(i64, 2), try queryCount(db, "MATCH (n:A) RETURN count(n)"));
    try std.testing.expectEqual(@as(i64, 1), try queryCount(db, "MATCH (n:B) RETURN count(n)"));
    // Merging again finds the :A:B node.
    try run(db, "MERGE (n:A:B {k: 1})");
    try std.testing.expectEqual(@as(i64, 1), try queryCount(db, "MATCH (n:B) RETURN count(n)"));
}

test "cypher: MERGE returns every matching node" {
    const path = "/tmp/lattice_cs_merge_many.ltdb";
    const db = try openTestDb(path);
    defer cleanupTestDb(db, path);
    try run(db, "CREATE (:A {k: 1}), (:A {k: 1}), (:A {k: 2})");

    try std.testing.expectEqual(@as(i64, 2), try queryCount(db, "MERGE (n:A {k: 1}) ON MATCH SET n.seen = true RETURN count(n)"));
    try std.testing.expectEqual(@as(i64, 2), try queryCount(db, "MATCH (n:A) WHERE n.seen RETURN count(n)"));
    // An integer property matches the equal float, as `=` does.
    try std.testing.expectEqual(@as(i64, 1), try queryCount(db, "MERGE (n:A {k: 2.0}) RETURN count(n)"));
    try std.testing.expectEqual(@as(i64, 3), try queryCount(db, "MATCH (n:A) RETURN count(n)"));
    // Lists compare by value, so a second merge finds the first node.
    try run(db, "MERGE (n:L {tags: [1, 2]})");
    try run(db, "MERGE (n:L {tags: [1, 2]})");
    try std.testing.expectEqual(@as(i64, 1), try queryCount(db, "MATCH (n:L) RETURN count(n)"));
}

test "cypher: MERGE within one query sees nodes it created earlier" {
    const path = "/tmp/lattice_cs_merge_rows.ltdb";
    const db = try openTestDb(path);
    defer cleanupTestDb(db, path);

    try std.testing.expectEqual(@as(i64, 5), try queryCount(db, "UNWIND [1, 2, 2, 3, 1] AS i MERGE (p:Person {id: i}) RETURN count(*)"));
    try std.testing.expectEqual(@as(i64, 3), try queryCount(db, "MATCH (p:Person) RETURN count(p)"));
}

test "cypher: MERGE with a null property value fails" {
    const path = "/tmp/lattice_cs_merge_null.ltdb";
    const db = try openTestDb(path);
    defer cleanupTestDb(db, path);

    try std.testing.expectError(QueryError.ExecutionError, db.query("MERGE (n:A {k: null})"));
    try std.testing.expectEqual(@as(i64, 0), try queryCount(db, "MATCH (n) RETURN count(n)"));
}

test "cypher: MERGE uses a property index and still sees uncommitted nodes" {
    const path = "/tmp/lattice_cs_merge_index.ltdb";
    const db = try openTestDb(path);
    defer cleanupTestDb(db, path);
    try db.createNodePropertyIndex("Person", "id");

    try run(db, "UNWIND range(1, 50) AS i CREATE (:Person {id: i})");
    try std.testing.expectEqual(@as(i64, 60), try queryCount(db, "UNWIND range(1, 60) AS i MERGE (p:Person {id: i}) RETURN count(*)"));
    try std.testing.expectEqual(@as(i64, 60), try queryCount(db, "MATCH (p:Person) RETURN count(p)"));
    try std.testing.expectEqual(@as(i64, 60), try queryCount(db, "UNWIND range(1, 60) AS i MERGE (p:Person {id: i}) RETURN count(*)"));
    try std.testing.expectEqual(@as(i64, 60), try queryCount(db, "MATCH (p:Person) RETURN count(p)"));
}

// ============================================================================
// DELETE and relationships
// ============================================================================

test "cypher: DELETE refuses a node that still has relationships" {
    const path = "/tmp/lattice_cs_delete_connected.ltdb";
    const db = try openTestDb(path);
    defer cleanupTestDb(db, path);
    try run(db, "CREATE (x:Q {n: 1})-[:R]->(y:Q {n: 2}), (x)-[:R]->(y), (z:Q {n: 3})");

    // Used to succeed and leave both edges pointing at deleted nodes.
    var detailed = try db.queryDetailed("MATCH (n:Q) DELETE n");
    defer detailed.deinit();
    try std.testing.expect(detailed == .failure);
    try std.testing.expectEqualStrings("NodeHasRelationships", detailed.failure.code.?);
    // Nothing was deleted, not even the unconnected node.
    try std.testing.expectEqual(@as(i64, 3), try queryCount(db, "MATCH (n:Q) RETURN count(n)"));

    // An unconnected node deletes as before.
    try run(db, "MATCH (n:Q {n: 3}) DELETE n");
    // Deleting the relationships in the same statement makes it legal.
    try run(db, "MATCH (n:Q {n: 1})-[r]-() DELETE r, n");
    try std.testing.expectEqual(@as(i64, 1), try queryCount(db, "MATCH (n:Q) RETURN count(n)"));
    try std.testing.expectEqual(@as(i64, 0), try queryCount(db, "MATCH (n:Q)-[r]-() RETURN count(r)"));
}

test "cypher: DETACH DELETE removes parallel edges and self-loops" {
    const path = "/tmp/lattice_cs_detach_delete.ltdb";
    const db = try openTestDb(path);
    defer cleanupTestDb(db, path);
    try run(db, "CREATE (a:S {n: 1})-[:R]->(a), (a)-[:R]->(b:S {n: 2}), (a)-[:R]->(b)");

    var ids = try db.query("MATCH (b:S {n: 2}) RETURN b");
    defer ids.deinit();
    const b_id: u64 = switch (ids.rows[0].values[0]) {
        .node_id => |id| id,
        .int_val => |id| @intCast(id),
        else => return error.UnexpectedValueType,
    };

    try run(db, "MATCH (a:S {n: 1}) DETACH DELETE a");
    try std.testing.expectEqual(@as(i64, 1), try queryCount(db, "MATCH (n:S) RETURN count(n)"));
    // Both parallel edges into b are gone.
    const incoming = try db.getIncomingEdgeRefsInTxn(null, b_id);
    defer db.allocator.free(incoming);
    try std.testing.expectEqual(@as(usize, 0), incoming.len);
}

test "cypher: deleting a relationship matched twice deletes it once" {
    const path = "/tmp/lattice_cs_delete_rel_twice.ltdb";
    const db = try openTestDb(path);
    defer cleanupTestDb(db, path);
    try run(db, "CREATE (a:T)-[:R]->(b:T)");

    // An undirected pattern lists the edge from both ends; the second delete
    // used to fail the query.
    try run(db, "MATCH (:T)-[r]-(:T) DELETE r");
    try std.testing.expectEqual(@as(i64, 0), try queryCount(db, "MATCH (n:T)-[r]-() RETURN count(r)"));
}

// ============================================================================
// Reusing a node variable
// ============================================================================

/// A triangle a -> b -> c -> a, a tail c -> d, and Dan who knows nobody.
fn seedTriangle(db: *Database) !void {
    try run(db, "CREATE (a:P {name: 'a'}), (b:P {name: 'b'}), (c:P {name: 'c'}), (d:P {name: 'd'}), " ++
        "(e:Q {name: 'e'}), (a)-[:R]->(b), (b)-[:R]->(c), (c)-[:R]->(a), (c)-[:R]->(d)");
}

test "cypher: a pattern can return to a node it already bound" {
    const path = "/tmp/lattice_cs_reuse_cycle.ltdb";
    const db = try openTestDb(path);
    defer cleanupTestDb(db, path);
    try seedTriangle(db);

    // Each rotation of the triangle closes on its start.
    try expectNames(db, "MATCH (a)-[:R]->(b)-[:R]->(c)-[:R]->(a) RETURN a.name ORDER BY a.name", &.{ "a", "b", "c" });
    // A variable-length closing hop.
    try expectNames(db, "MATCH (a {name: 'a'})-[:R*1..3]->(a) RETURN a.name", &.{"a"});
    // Comma-separated patterns share the variable.
    try expectNames(db, "MATCH (a)-[:R]->(b), (b)-[:R]->(c) WHERE a.name = 'a' RETURN c.name", &.{"c"});
    // The same variable again adds its labels as a check.
    try std.testing.expectEqual(@as(i64, 0), try queryCount(db, "MATCH (n:P), (n:Q) RETURN count(*)"));
    try std.testing.expectEqual(@as(i64, 4), try queryCount(db, "MATCH (n:P), (n) RETURN count(*)"));
}

test "cypher: a later MATCH or a MATCH after WITH continues from a bound node" {
    const path = "/tmp/lattice_cs_reuse_clauses.ltdb";
    const db = try openTestDb(path);
    defer cleanupTestDb(db, path);
    try seedTriangle(db);

    try expectNames(db, "MATCH (a:P {name: 'c'}) MATCH (a)-[:R]->(b) RETURN b.name ORDER BY b.name", &.{ "a", "d" });
    try expectNames(db, "MATCH (a:P {name: 'c'}) WITH a MATCH (a)-[:R]->(b) RETURN b.name ORDER BY b.name", &.{ "a", "d" });
    // Expanding into a node bound by an earlier clause.
    try expectNames(db, "MATCH (x {name: 'a'}) MATCH (y)-[:R]->(x) RETURN y.name", &.{"c"});
    // Inline properties on the reused node filter it.
    try std.testing.expectEqual(@as(i64, 0), try queryCount(db, "MATCH (a {name: 'c'}) MATCH (a {name: 'b'}) RETURN count(*)"));
}

test "cypher: WITH carries each variable's own value" {
    const path = "/tmp/lattice_cs_with_slots.ltdb";
    const db = try openTestDb(path);
    defer cleanupTestDb(db, path);
    try run(db, "CREATE (c:P {name: 'c'})-[:R {w: 1}]->(d:P {name: 'd'})");

    // `b` used to come out of the WITH as `a`.
    try expectNames(db, "MATCH (a)-[:R]->(b) WITH b RETURN b.name", &.{"d"});
    try expectNames(db, "MATCH (a)-[:R]->(b) WITH b, a RETURN a.name", &.{"c"});
    try expectNames(db, "MATCH (a)-[:R]->(b) WITH b AS a, a AS b RETURN a.name", &.{"d"});
    try expectNames(db, "MATCH (a)-[:R]->(b) WITH a, count(b) AS n RETURN a.name", &.{"c"});
    // A predicate before the WITH on a variable it drops.
    try expectNames(db, "MATCH (n)-[r:R]->(m) WHERE r.w = 1 WITH m RETURN m.name", &.{"d"});
    try expectNames(db, "MATCH (:P {name: 'c'})-[:R]->(m) WITH m RETURN m.name", &.{"d"});
    // A name bound again after the WITH dropped it.
    try std.testing.expectEqual(@as(i64, 2), try queryCount(db, "MATCH (a:P) WITH count(a) AS c MATCH (a:P) RETURN count(a)"));
}

test "cypher: a relationship carried through WITH is still a relationship" {
    const path = "/tmp/lattice_cs_with_edge.ltdb";
    const db = try openTestDb(path);
    defer cleanupTestDb(db, path);
    try seedTriangle(db);

    // Planned as a node delete before, which failed with a type error.
    try run(db, "MATCH (:P {name: 'c'})-[r:R]->(:P {name: 'd'}) WITH r DELETE r");
    try std.testing.expectEqual(@as(i64, 3), try queryCount(db, "MATCH ()-[r:R]->() RETURN count(r)"));
}

test "cypher: a variable cannot be matched as a different kind" {
    const path = "/tmp/lattice_cs_reuse_errors.ltdb";
    const db = try openTestDb(path);
    defer cleanupTestDb(db, path);
    try seedTriangle(db);

    try std.testing.expectError(QueryError.SemanticError, db.query("MATCH ()-[r]->() MATCH ()-[r]->() RETURN r"));
    try std.testing.expectError(QueryError.SemanticError, db.query("MATCH ()-[r]->() MATCH (r) RETURN r"));
    try std.testing.expectError(QueryError.SemanticError, db.query("UNWIND [1] AS x MATCH (x) RETURN x"));
}

// ============================================================================
// Regular expressions
// ============================================================================

test "cypher: =~ matches a regular expression against the whole string" {
    const path = "/tmp/lattice_cs_regex.ltdb";
    const db = try openTestDb(path);
    defer cleanupTestDb(db, path);
    try run(db, "CREATE (:W {name: 'Alice'}), (:W {name: 'alice'}), (:W {name: 'Malice'}), (:W {name: 'Bob'})");

    try expectNames(db, "MATCH (w:W) WHERE w.name =~ 'A.*' RETURN w.name", &.{"Alice"});
    try expectNames(db, "MATCH (w:W) WHERE w.name =~ '(?i)alice' RETURN w.name ORDER BY w.name", &.{ "Alice", "alice" });
    try expectNames(db, "MATCH (w:W) WHERE w.name=~'.*lice' AND NOT w.name =~ 'M.*' RETURN w.name ORDER BY w.name", &.{ "Alice", "alice" });
    {
        var result = try db.query("UNWIND [1] AS x RETURN 'abc' =~ '(a|ab)c' AS a, null =~ 'a' AS b, 'x' =~ 'x|y' AS c");
        defer result.deinit();
        try std.testing.expectEqual(true, result.rows[0].values[0].bool_val);
        try expectNull(result, 0, 1);
        try std.testing.expectEqual(true, result.rows[0].values[2].bool_val);
    }
    // A pattern the engine does not accept fails the query.
    try std.testing.expectError(QueryError.ExecutionError, db.query("MATCH (w:W) WHERE w.name =~ '(' RETURN w"));
}

// ============================================================================
// Smaller semantic gaps
// ============================================================================

test "cypher: UNWIND null produces no rows" {
    const path = "/tmp/lattice_cs_unwind_null.ltdb";
    const db = try openTestDb(path);
    defer cleanupTestDb(db, path);
    try run(db, "CREATE (:Post {title: 'untagged'})");

    try std.testing.expectEqual(@as(i64, 0), try queryCount(db, "UNWIND null AS x RETURN count(*)"));
    // A missing list property used to MERGE a tag named null.
    try run(db, "MATCH (p:Post) UNWIND p.tags AS t MERGE (:Tag {name: t})");
    try std.testing.expectEqual(@as(i64, 0), try queryCount(db, "MATCH (t:Tag) RETURN count(t)"));
}

test "cypher: DELETE of a value is an error, not a no-op" {
    const path = "/tmp/lattice_cs_delete_value.ltdb";
    const db = try openTestDb(path);
    defer cleanupTestDb(db, path);

    try std.testing.expectError(QueryError.SemanticError, db.query("UNWIND [1] AS x DELETE x"));
    try std.testing.expectError(QueryError.SemanticError, db.query("MATCH (n) WITH n.x AS v DELETE v"));
}

test "cypher: the smallest integer can be written as a literal" {
    const path = "/tmp/lattice_cs_min_int.ltdb";
    const db = try openTestDb(path);
    defer cleanupTestDb(db, path);

    var result = try db.query("UNWIND [1] AS x RETURN -9223372036854775808 AS m, -9223372036854775807 - 1 AS n");
    defer result.deinit();
    try expectInt(result, 0, 0, std.math.minInt(i64));
    try expectInt(result, 0, 1, std.math.minInt(i64));
}

test "cypher: reading a node's embedding leaks nothing" {
    const path = "/tmp/lattice_cs_embedding_leak.ltdb";
    @import("compat").fs.cwd().deleteFile(path) catch {};
    const db = try Database.open(std.testing.allocator, path, .{
        .create = true,
        .config = .{ .enable_wal = false, .enable_vector = true, .vector_dimensions = 2 },
    });
    defer cleanupTestDb(db, path);
    const node = try db.createNode(null, &[_][]const u8{"Doc"});
    try db.setNodeVector(node, &[_]f32{ 1.0, 2.0 });

    // The testing allocator fails the test on a leak.
    var result = try db.query("MATCH (d:Doc) RETURN d.embedding AS e");
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), result.rowCount());
}

test "cypher: an indexed property equals across integer and float" {
    const path = "/tmp/lattice_cs_index_numeric.ltdb";
    const db = try openTestDb(path);
    defer cleanupTestDb(db, path);
    try db.createNodePropertyIndex("Item", "x");
    try run(db, "CREATE (:Item {x: 1}), (:Item {x: 2.0}), (:Item {x: 2.5})");

    // Without the index both rows match, as 1 = 1.0 in Cypher.
    try std.testing.expectEqual(@as(i64, 1), try queryCount(db, "MATCH (i:Item {x: 1.0}) RETURN count(i)"));
    try std.testing.expectEqual(@as(i64, 1), try queryCount(db, "MATCH (i:Item) WHERE i.x = 2 RETURN count(i)"));
    try std.testing.expectEqual(@as(i64, 1), try queryCount(db, "MATCH (i:Item {x: 2.5}) RETURN count(i)"));
    // MERGE finds the integer-valued node instead of creating a twin.
    try run(db, "MERGE (:Item {x: 1.0})");
    try std.testing.expectEqual(@as(i64, 3), try queryCount(db, "MATCH (i:Item) RETURN count(i)"));
}

test "cypher: lists and maps compare by value" {
    const path = "/tmp/lattice_cs_list_equality.ltdb";
    const db = try openTestDb(path);
    defer cleanupTestDb(db, path);

    var result = try db.query("UNWIND [1] AS x RETURN [1, 2] = [1, 2] AS a, [1, 2] = [1, 3] AS b, [1, 2] <> [2, 1] AS c, " ++
        "{k: 1, j: 'x'} = {j: 'x', k: 1.0} AS d, [1, null] = [1, null] AS e, [1, null] = [2, null] AS f");
    defer result.deinit();
    try std.testing.expectEqual(true, result.rows[0].values[0].bool_val);
    try std.testing.expectEqual(false, result.rows[0].values[1].bool_val);
    try std.testing.expectEqual(true, result.rows[0].values[2].bool_val);
    try std.testing.expectEqual(true, result.rows[0].values[3].bool_val);
    try expectNull(result, 0, 4);
    try std.testing.expectEqual(false, result.rows[0].values[5].bool_val);
}

test "cypher: function names ignore case, toString(null) is null, booleans order" {
    const path = "/tmp/lattice_cs_function_case.ltdb";
    const db = try openTestDb(path);
    defer cleanupTestDb(db, path);

    var result = try db.query("UNWIND [1] AS x RETURN TOUPPER('a') AS a, toString(null) AS b, false < true AS c, true <= false AS d");
    defer result.deinit();
    try std.testing.expectEqualStrings("A", result.rows[0].values[0].string_val);
    try expectNull(result, 0, 1);
    try std.testing.expectEqual(true, result.rows[0].values[2].bool_val);
    try std.testing.expectEqual(false, result.rows[0].values[3].bool_val);
}

test "cypher: RETURN DISTINCT applies SKIP and LIMIT to the distinct rows" {
    const path = "/tmp/lattice_cs_distinct_limit.ltdb";
    const db = try openTestDb(path);
    defer cleanupTestDb(db, path);

    // The limit used to cut the input to 1, 1 before duplicates went.
    var limited = try db.query("UNWIND [1, 1, 1, 2, 3] AS x RETURN DISTINCT x LIMIT 2");
    defer limited.deinit();
    try std.testing.expectEqual(@as(usize, 2), limited.rows.len);
    try std.testing.expectEqual(@as(i64, 1), limited.rows[0].values[0].int_val);
    try std.testing.expectEqual(@as(i64, 2), limited.rows[1].values[0].int_val);

    var skipped = try db.query("UNWIND [1, 1, 2, 2, 3] AS x RETURN DISTINCT x SKIP 1 LIMIT 1");
    defer skipped.deinit();
    try std.testing.expectEqual(@as(usize, 1), skipped.rows.len);
    try std.testing.expectEqual(@as(i64, 2), skipped.rows[0].values[0].int_val);

    // A sort bounded by the limit would be the same cut one step earlier.
    var ordered = try db.query("UNWIND [3, 3, 3, 2, 1] AS x RETURN DISTINCT x ORDER BY x DESC LIMIT 2");
    defer ordered.deinit();
    try std.testing.expectEqual(@as(usize, 2), ordered.rows.len);
    try std.testing.expectEqual(@as(i64, 3), ordered.rows[0].values[0].int_val);
    try std.testing.expectEqual(@as(i64, 2), ordered.rows[1].values[0].int_val);
}

test "cypher: min and max order values as ORDER BY does" {
    const path = "/tmp/lattice_cs_min_max_order.ltdb";
    const db = try openTestDb(path);
    defer cleanupTestDb(db, path);

    // Different types compared as equal, so the first value seen won both.
    var mixed = try db.query("UNWIND ['b', 2, 'a', 1] AS x RETURN min(x) AS lo, max(x) AS hi");
    defer mixed.deinit();
    try std.testing.expectEqualStrings("a", mixed.rows[0].values[0].string_val);
    try std.testing.expectEqual(@as(i64, 2), mixed.rows[0].values[1].int_val);

    // 2^53 + 1 and 2^53 are the same f64.
    var exact = try db.query("UNWIND [9007199254740992.0, 9007199254740993] AS x RETURN max(x) AS hi, min(x) AS lo");
    defer exact.deinit();
    try std.testing.expectEqual(@as(i64, 9007199254740993), exact.rows[0].values[0].int_val);
    try std.testing.expectEqual(@as(f64, 9007199254740992.0), exact.rows[0].values[1].float_val);
}

test "cypher: functions of null are null and a map reads by key" {
    const path = "/tmp/lattice_cs_null_functions.ltdb";
    const db = try openTestDb(path);
    defer cleanupTestDb(db, path);

    // Each of these was a TypeError.
    var nulls = try db.query("UNWIND [1] AS x RETURN size(null) AS a, abs(null) AS b, id(null) AS c, range(null, 2) AS d, range(1, 3, null) AS e");
    defer nulls.deinit();
    for (0..5) |column| try expectNull(nulls, 0, column);

    // A map literal's entry read as null whatever the key.
    var maps = try db.query("UNWIND [1] AS x RETURN {a: 1, b: 'two'}.a AS a, {a: 1, b: 'two'}.b AS b, {a: 1}.missing AS c");
    defer maps.deinit();
    try std.testing.expectEqual(@as(i64, 1), maps.rows[0].values[0].int_val);
    try std.testing.expectEqualStrings("two", maps.rows[0].values[1].string_val);
    try expectNull(maps, 0, 2);
}

test "cypher: CREATE on a bound node cannot add labels or properties" {
    const path = "/tmp/lattice_cs_create_bound.ltdb";
    const db = try openTestDb(path);
    defer cleanupTestDb(db, path);
    try run(db, "CREATE (:A)");

    // The label used to be dropped without a word.
    try std.testing.expectError(QueryError.SemanticError, db.query("MATCH (a:A) CREATE (a:B)-[:R]->(:C)"));
    try std.testing.expectError(QueryError.SemanticError, db.query("MATCH (a:A) CREATE (a {x: 1})-[:R]->(:C)"));
    try run(db, "MATCH (a:A) CREATE (a)-[:R]->(:C)");
    try std.testing.expectEqual(@as(i64, 1), try queryCount(db, "MATCH (:A)-[:R]->(c:C) RETURN count(c)"));
}

test "cypher: keywords can name properties, labels, types and map keys" {
    const path = "/tmp/lattice_cs_keyword_names.ltdb";
    const db = try openTestDb(path);
    defer cleanupTestDb(db, path);

    try run(db, "CREATE (n:Order {limit: 3, order: 'x'})-[:WITH]->(:Set)");
    try std.testing.expectEqual(@as(i64, 1), try queryCount(db, "MATCH (n:Order)-[:WITH]->(:Set) WHERE n.limit = 3 AND n.order = 'x' RETURN count(n)"));
    try run(db, "MATCH (n:Order) SET n.match = 1 REMOVE n.order");
    try std.testing.expectEqual(@as(i64, 1), try queryCount(db, "MATCH (n:Order) RETURN n.match"));
    var result = try db.query("UNWIND [1] AS x RETURN {limit: 1}.limit AS a");
    result.deinit();
}

// ============================================================================
// Reads inside a transaction
// ============================================================================

fn runIn(db: *Database, txn: *lattice.transaction.manager.Transaction, cypher: []const u8) !void {
    var result = try db.queryInTxn(txn, cypher);
    result.deinit();
}

fn expectString(result: QueryResult, row: usize, col: usize, expected: []const u8) !void {
    switch (result.rows[row].values[col]) {
        .string_val => |v| try std.testing.expectEqualStrings(expected, v),
        else => return error.UnexpectedValueType,
    }
}

test "cypher: property and label reads in a transaction see its own writes" {
    const path = "/tmp/lattice_cs_txn_reads.ltdb";
    const compat_fs = @import("compat").fs;
    compat_fs.cwd().deleteFile(path) catch {};
    compat_fs.cwd().deleteFile(path ++ "-wal") catch {};
    const db = try Database.open(std.testing.allocator, path, .{
        .create = true,
        .config = .{ .enable_wal = true },
    });
    defer {
        db.close();
        compat_fs.cwd().deleteFile(path) catch {};
        compat_fs.cwd().deleteFile(path ++ "-wal") catch {};
    }

    try run(db, "CREATE (:P {name: 'a', n: 1})-[:R]->(:Q {name: 'b'})");

    var txn = try db.beginTransaction(.read_write);
    var open = true;
    defer if (open) db.abortTransaction(&txn) catch {};

    try runIn(db, &txn, "MATCH (p:P) SET p.n = 2, p.extra = 'x'");
    try runIn(db, &txn, "CREATE (:P {name: 'c', n: 3})");
    try runIn(db, &txn, "MATCH (q:Q) REMOVE q:Q SET q:Z");
    {
        var result = try db.queryInTxn(&txn, "MATCH (p:P) RETURN p.name, p.n, p.extra ORDER BY p.name");
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 2), result.rowCount());
        try expectString(result, 0, 0, "a");
        try expectInt(result, 0, 1, 2);
        try expectString(result, 0, 2, "x");
        try expectString(result, 1, 0, "c");
        try expectInt(result, 1, 1, 3);
        try expectNull(result, 1, 2);
    }
    {
        // The label on the far end of an expansion is checked as the
        // transaction sees it.
        var with_z = try db.queryInTxn(&txn, "MATCH (:P)-[:R]->(q:Z) RETURN q.name");
        defer with_z.deinit();
        try std.testing.expectEqual(@as(usize, 1), with_z.rowCount());
        try expectString(with_z, 0, 0, "b");
        var with_q = try db.queryInTxn(&txn, "MATCH (:P)-[:R]->(q:Q) RETURN q.name");
        defer with_q.deinit();
        try std.testing.expectEqual(@as(usize, 0), with_q.rowCount());
    }
    {
        // A node deleted earlier in the transaction has no properties.
        var result = try db.queryInTxn(&txn, "MATCH (c:P {name: 'c'}) WITH c DELETE c RETURN c.name");
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), result.rowCount());
        try expectNull(result, 0, 0);
    }

    // Nothing above is visible outside the transaction, and none of it
    // survives the abort.
    {
        var result = try db.query("MATCH (p:P) RETURN p.n, p.extra");
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), result.rowCount());
        try expectInt(result, 0, 0, 1);
        try expectNull(result, 0, 1);
    }
    open = false;
    try db.abortTransaction(&txn);
    try std.testing.expectEqual(@as(i64, 1), try queryCount(db, "MATCH (:P)-[:R]->(q:Q) RETURN count(q)"));
}

// ============================================================================
// ORDER BY
// ============================================================================

test "cypher: ORDER BY orders values of different types consistently" {
    const path = "/tmp/lattice_cs_order_mixed.ltdb";
    const db = try openTestDb(path);
    defer cleanupTestDb(db, path);

    var result = try db.query("UNWIND [3, 'a', null, true, 1.5, [1], 2, 'b', false] AS x RETURN x ORDER BY x");
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 9), result.rowCount());
    try std.testing.expect(result.rows[0].values[0] == .list_val);
    try expectString(result, 1, 0, "a");
    try expectString(result, 2, 0, "b");
    try std.testing.expect(result.rows[3].values[0].bool_val == false);
    try std.testing.expect(result.rows[4].values[0].bool_val == true);
    try std.testing.expectEqual(@as(f64, 1.5), result.rows[5].values[0].float_val);
    try expectInt(result, 6, 0, 2);
    try expectInt(result, 7, 0, 3);
    try expectNull(result, 8, 0);
}

test "cypher: ORDER BY compares integers and floats by exact value" {
    const path = "/tmp/lattice_cs_order_exact.ltdb";
    const db = try openTestDb(path);
    defer cleanupTestDb(db, path);

    // 2^53 + 1 is not a float; converting it rounds to 2^53 and ties.
    var result = try db.query("UNWIND [9007199254740993, 9007199254740992.0] AS x RETURN x ORDER BY x");
    defer result.deinit();
    try std.testing.expect(result.rows[0].values[0] == .float_val);
    try expectInt(result, 1, 0, 9007199254740993);
}

test "cypher: an error in an ORDER BY key fails the query" {
    const path = "/tmp/lattice_cs_order_error.ltdb";
    const db = try openTestDb(path);
    defer cleanupTestDb(db, path);

    try std.testing.expectError(QueryError.ExecutionError, db.query("UNWIND [1, 0, 2] AS x RETURN x ORDER BY 10 / x"));
}

test "cypher: ORDER BY with SKIP and LIMIT returns the same rows as sorting everything" {
    const path = "/tmp/lattice_cs_order_topk.ltdb";
    const db = try openTestDb(path);
    defer cleanupTestDb(db, path);

    // Every key repeats, some are null, and the second key ties too, so
    // the order of equal rows is tested along with the order of keys.
    try run(db, "UNWIND range(0, 199) AS i CREATE (:S {i: i, k: (i * 37) % 50})");
    try run(db, "MATCH (s:S) WHERE s.i % 9 = 0 SET s.k = null");

    const order = "MATCH (s:S) RETURN s.i ORDER BY s.k DESC, s.i % 3";
    var full = try db.query(order);
    defer full.deinit();
    try std.testing.expectEqual(@as(usize, 200), full.rowCount());
    const Case = struct { suffix: []const u8, skip: usize, limit: usize };
    const cases = [_]Case{
        .{ .suffix = " LIMIT 1", .skip = 0, .limit = 1 },
        .{ .suffix = " LIMIT 10", .skip = 0, .limit = 10 },
        .{ .suffix = " SKIP 7 LIMIT 13", .skip = 7, .limit = 13 },
        .{ .suffix = " SKIP 195 LIMIT 10", .skip = 195, .limit = 5 },
        .{ .suffix = " LIMIT 500", .skip = 0, .limit = 200 },
        .{ .suffix = " LIMIT 0", .skip = 0, .limit = 0 },
        .{ .suffix = " SKIP 190", .skip = 190, .limit = 10 },
    };
    for (cases) |case| {
        var buf: [128]u8 = undefined;
        const cypher = try std.fmt.bufPrint(&buf, "{s}{s}", .{ order, case.suffix });
        var bounded = try db.query(cypher);
        defer bounded.deinit();
        try std.testing.expectEqual(case.limit, bounded.rowCount());
        for (0..case.limit) |r| {
            try std.testing.expectEqual(full.rows[case.skip + r].values[0].int_val, bounded.rows[r].values[0].int_val);
        }
    }

    // A limit given as a parameter bounds the sort the same way.
    var params = std.StringHashMap(lattice.core.types.PropertyValue).init(std.testing.allocator);
    defer params.deinit();
    try params.put("n", .{ .int_val = 4 });
    var with_param = try db.queryWithParams(order ++ " LIMIT $n", &params);
    defer with_param.deinit();
    try std.testing.expectEqual(@as(usize, 4), with_param.rowCount());
    for (0..4) |r| try std.testing.expectEqual(full.rows[r].values[0].int_val, with_param.rows[r].values[0].int_val);

    // Rows the limit drops are still produced, so writes below the sort
    // all happen.
    try run(db, "UNWIND range(1, 5) AS i CREATE (t:T {i: i}) RETURN t.i ORDER BY t.i LIMIT 0");
    try std.testing.expectEqual(@as(i64, 5), try queryCount(db, "MATCH (t:T) RETURN count(t)"));
}

test "cypher: a streamed label scan sees what a materialised one does" {
    const path = "/tmp/lattice_cs_label_stream.ltdb";
    const compat_fs = @import("compat").fs;
    compat_fs.cwd().deleteFile(path) catch {};
    compat_fs.cwd().deleteFile(path ++ "-wal") catch {};
    const db = try Database.open(std.testing.allocator, path, .{
        .create = true,
        .config = .{ .enable_wal = true },
    });
    defer {
        db.close();
        compat_fs.cwd().deleteFile(path) catch {};
        compat_fs.cwd().deleteFile(path ++ "-wal") catch {};
    }

    // Several index chunks of :C, with unlabelled :D nodes between them.
    try run(db, "UNWIND range(0, 899) AS i CREATE (:C {i: i}), (:D {i: 1000 + i})");

    var txn = try db.beginTransaction(.read_write);
    defer db.abortTransaction(&txn) catch {};

    // The overlay removes, relabels, deletes and adds nodes on both sides
    // of chunk boundaries.
    try runIn(db, &txn, "MATCH (n:C) WHERE n.i % 7 = 0 DELETE n");
    try runIn(db, &txn, "MATCH (n:C) WHERE n.i % 11 = 0 REMOVE n:C");
    try runIn(db, &txn, "MATCH (n:D) WHERE n.i % 13 = 0 SET n:C");
    try runIn(db, &txn, "UNWIND range(0, 99) AS i CREATE (:C {i: 5000 + i})");

    // A query that writes reads every id when it opens; one that only reads
    // streams them. Both must visit the same nodes in the same order.
    var materialised = try db.queryInTxn(&txn, "MATCH (n:C) SET n.seen = true RETURN n.i");
    defer materialised.deinit();
    var streamed = try db.queryInTxn(&txn, "MATCH (n:C) RETURN n.i");
    defer streamed.deinit();
    try std.testing.expect(materialised.rowCount() > 800);
    try std.testing.expectEqual(materialised.rowCount(), streamed.rowCount());
    for (0..streamed.rowCount()) |r| {
        try std.testing.expectEqual(materialised.rows[r].values[0].int_val, streamed.rows[r].values[0].int_val);
    }

    var limited = try db.queryInTxn(&txn, "MATCH (n:C) RETURN n.i LIMIT 300");
    defer limited.deinit();
    try std.testing.expectEqual(@as(usize, 300), limited.rowCount());
    for (0..300) |r| {
        try std.testing.expectEqual(streamed.rows[r].values[0].int_val, limited.rows[r].values[0].int_val);
    }

    // Outside the transaction the committed 900 are all there.
    try std.testing.expectEqual(@as(i64, 900), try queryCount(db, "MATCH (n:C) RETURN count(n)"));
}
