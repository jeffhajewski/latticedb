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
