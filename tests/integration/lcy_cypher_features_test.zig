//! End-to-end tests for the Cypher features added after 0.16: list indexing
//! and slicing, CASE, aggregates inside expressions, UNION, OPTIONAL MATCH,
//! existence predicates, named paths, comprehensions, CALL and FOREACH.
//!
//! Results are compared as text, one row per line, so that a test states the
//! whole answer: `[1, "a", null]` is a list, `{k: 1}` a map, `N` a node and
//! `E` a relationship (their ids vary with creation order, so they are not
//! shown).

const std = @import("std");
const lattice = @import("lattice");

const Database = lattice.storage.database.Database;
const QueryResult = lattice.storage.database.QueryResult;
const ResultValue = lattice.storage.database.ResultValue;
const QueryFailureStage = lattice.storage.database.QueryFailureStage;

fn openTestDb(path: []const u8) !*Database {
    @import("compat").fs.cwd().deleteFile(path) catch {};
    return try Database.open(std.testing.allocator, path, .{
        .create = true,
        .config = .{ .enable_wal = false },
    });
}

fn closeTestDb(db: *Database, path: []const u8) void {
    db.close();
    @import("compat").fs.cwd().deleteFile(path) catch {};
}

fn run(db: *Database, cypher: []const u8) !void {
    var result = try db.query(cypher);
    result.deinit();
}

fn writeValue(out: *std.ArrayList(u8), value: ResultValue) !void {
    const a = std.testing.allocator;
    switch (value) {
        .null_val => try out.appendSlice(a, "null"),
        .bool_val => |b| try out.appendSlice(a, if (b) "true" else "false"),
        .int_val => |i| try out.print(a, "{d}", .{i}),
        .float_val => |f| try out.print(a, "{d}", .{f}),
        .string_val => |s| try out.print(a, "\"{s}\"", .{s}),
        .node_id => try out.appendSlice(a, "N"),
        .edge_id => try out.appendSlice(a, "E"),
        .bytes_val => try out.appendSlice(a, "<bytes>"),
        .vector_val => try out.appendSlice(a, "<vector>"),
        .list_val => |items| {
            try out.append(a, '[');
            for (items, 0..) |item, i| {
                if (i > 0) try out.appendSlice(a, ", ");
                try writeValue(out, item);
            }
            try out.append(a, ']');
        },
        .map_val => |entries| {
            try out.append(a, '{');
            for (entries, 0..) |entry, i| {
                if (i > 0) try out.appendSlice(a, ", ");
                try out.print(a, "{s}: ", .{entry.key});
                try writeValue(out, entry.value);
            }
            try out.append(a, '}');
        },
    }
}

/// The result as text: the column names on the first line, then one line
/// per row with the values separated by " | ".
fn render(result: QueryResult) ![]u8 {
    const a = std.testing.allocator;
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(a);
    for (result.columns, 0..) |column, i| {
        if (i > 0) try out.appendSlice(a, " | ");
        try out.appendSlice(a, column);
    }
    for (result.rows) |row| {
        try out.append(a, '\n');
        for (row.values, 0..) |value, i| {
            if (i > 0) try out.appendSlice(a, " | ");
            try writeValue(&out, value);
        }
    }
    return out.toOwnedSlice(a);
}

/// Run a query and compare its whole result, columns first, as text.
fn expectResult(db: *Database, cypher: []const u8, expected: []const u8) !void {
    var detailed = try db.queryDetailed(cypher);
    defer detailed.deinit();
    switch (detailed) {
        .failure => |f| {
            std.debug.print("\nquery failed: {s}\n  {s}: {s}\n", .{ cypher, @tagName(f.stage), f.message });
            return error.QueryFailed;
        },
        .success => |result| {
            const text = try render(result);
            defer std.testing.allocator.free(text);
            if (!std.mem.eql(u8, text, expected)) {
                std.debug.print("\nquery: {s}\nexpected:\n{s}\nactual:\n{s}\n", .{ cypher, expected, text });
                return error.TestUnexpectedResult;
            }
        },
    }
}

/// Run a query that must fail at `stage` with a message containing `needle`.
fn expectFailure(db: *Database, cypher: []const u8, stage: QueryFailureStage, needle: []const u8) !void {
    var detailed = try db.queryDetailed(cypher);
    defer detailed.deinit();
    switch (detailed) {
        .success => |result| {
            const text = try render(result);
            defer std.testing.allocator.free(text);
            std.debug.print("\nquery should have failed: {s}\nresult:\n{s}\n", .{ cypher, text });
            return error.TestUnexpectedResult;
        },
        .failure => |f| {
            if (f.stage != stage or std.mem.indexOf(u8, f.message, needle) == null) {
                std.debug.print("\nquery: {s}\nexpected {s} failure containing \"{s}\"\nactual {s}: {s} ({s})\n", .{
                    cypher, @tagName(stage), needle, @tagName(f.stage), f.message, f.code orelse "",
                });
                return error.TestUnexpectedResult;
            }
        },
    }
}

// ============================================================================
// List indexing and slicing
// ============================================================================

test "lcy: list index from the front and the back" {
    const path = "/tmp/lcy_index.ltdb";
    const db = try openTestDb(path);
    defer closeTestDb(db, path);

    try expectResult(db, "WITH [10, 20, 30] AS l RETURN l[0], l[2], l[-1], l[-3]", "l[0] | l[2] | l[-1] | l[-3]\n10 | 30 | 30 | 10");
    // Out of range either way is null, not an error.
    try expectResult(db, "WITH [10, 20, 30] AS l RETURN l[3] AS a, l[-4] AS b, l[9223372036854775807] AS c, l[-9223372036854775808] AS d", "a | b | c | d\nnull | null | null | null");
    // A null list or a null index is null.
    try expectResult(db, "WITH [1] AS l RETURN null[0] AS a, l[null] AS b", "a | b\nnull | null");
    // An expression as the index, and chains of subscripts.
    try expectResult(db, "WITH [[1, 2], [3, 4]] AS l RETURN l[1][0] AS a, l[size(l) - 1][-1] AS b, [5, 6, 7][1 + 1] AS c", "a | b | c\n3 | 4 | 7");
    // An index of the wrong type is an error.
    try expectFailure(db, "WITH [1] AS l RETURN l[1.5]", .execution, "an index must be an integer or a string, but this is 1.5");
    try expectFailure(db, "WITH 'abc' AS s RETURN s[0]", .execution, "string[integer] is not defined");
    try expectFailure(db, "WITH [1] AS l RETURN l['x']", .execution, "list[string] is not defined");
}

test "lcy: list slices" {
    const path = "/tmp/lcy_slice.ltdb";
    const db = try openTestDb(path);
    defer closeTestDb(db, path);

    try expectResult(db, "WITH [1, 2, 3, 4, 5] AS l RETURN l[1..3] AS a, l[..2] AS b, l[3..] AS c, l[..] AS d", "a | b | c | d\n[2, 3] | [1, 2] | [4, 5] | [1, 2, 3, 4, 5]");
    // Negative bounds count from the end; bounds past an end are clamped.
    try expectResult(db, "WITH [1, 2, 3, 4, 5] AS l RETURN l[-2..] AS a, l[..-1] AS b, l[-10..2] AS c, l[2..99] AS d", "a | b | c | d\n[4, 5] | [1, 2, 3, 4] | [1, 2] | [3, 4, 5]");
    // A range that ends before it starts is empty.
    try expectResult(db, "WITH [1, 2, 3] AS l RETURN l[2..1] AS a, l[5..9] AS b, [][0..1] AS c", "a | b | c\n[] | [] | []");
    // A null bound or a null list is null.
    try expectResult(db, "WITH [1, 2, 3] AS l RETURN l[null..2] AS a, l[1..null] AS b, null[0..1] AS c", "a | b | c\nnull | null | null");
    try expectFailure(db, "WITH [1, 2] AS l RETURN l['a'..1]", .execution, "the bounds of a list range must be integers, but this is 'a'");
}

test "lcy: map and property access by key" {
    const path = "/tmp/lcy_keyaccess.ltdb";
    const db = try openTestDb(path);
    defer closeTestDb(db, path);
    try run(db, "CREATE (a:P {name: 'Ann', age: 30})-[:R {w: 2}]->(b:P {name: 'Bob'})");

    try expectResult(db, "WITH {a: 1, b: [2, 3]} AS m RETURN m['a'] AS a, m['b'][1] AS b, m['zz'] AS c", "a | b | c\n1 | 3 | null");
    try expectResult(db, "MATCH (n:P {name: 'Ann'}) WITH n, 'age' AS k RETURN n['name'] AS a, n[k] AS b, n['missing'] AS c", "a | b | c\n\"Ann\" | 30 | null");
    try expectResult(db, "MATCH ()-[r:R]->() RETURN r['w'] AS w", "w\n2");
    // Index access agrees with property access, in WHERE too.
    try expectResult(db, "MATCH (n:P) WHERE n['name'] = 'Bob' RETURN n.name", "n.name\n\"Bob\"");
    try expectFailure(db, "WITH {a: 1} AS m RETURN m[0]", .execution, "map[integer] is not defined");
}

// ============================================================================
// CASE
// ============================================================================

/// :Item nodes: a (n = 1), b (n = 2), c (n = 3) and d with no n.
fn seedItems(db: *Database) !void {
    try run(db, "CREATE (:Item {name: 'a', n: 1}), (:Item {name: 'b', n: 2}), (:Item {name: 'c', n: 3}), (:Item {name: 'd'})");
}

test "lcy: CASE, simple and searched" {
    const path = "/tmp/lcy_case.ltdb";
    const db = try openTestDb(path);
    defer closeTestDb(db, path);
    try seedItems(db);

    try expectResult(db,
        \\MATCH (i:Item) RETURN i.name, CASE i.n WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'many' END AS w ORDER BY i.name
    , "i.name | w\n\"a\" | \"one\"\n\"b\" | \"two\"\n\"c\" | \"many\"\n\"d\" | \"many\"");
    try expectResult(db,
        \\MATCH (i:Item) RETURN i.name, CASE WHEN i.n < 2 THEN 'low' WHEN i.n >= 2 THEN 'high' END AS w ORDER BY i.name
    , "i.name | w\n\"a\" | \"low\"\n\"b\" | \"high\"\n\"c\" | \"high\"\n\"d\" | null");
    // The first branch that applies wins.
    try expectResult(db, "RETURN CASE WHEN true THEN 1 WHEN true THEN 2 END AS x", "x\n1");
    // A null subject matches no WHEN, not even null; a null condition is not true.
    try expectResult(db, "RETURN CASE null WHEN null THEN 'm' ELSE 'e' END AS a, CASE WHEN null THEN 'm' ELSE 'e' END AS b", "a | b\n\"e\" | \"e\"");
    // Simple CASE compares with =, so 1 matches 1.0.
    try expectResult(db, "RETURN CASE 1 WHEN 1.0 THEN 'eq' END AS x", "x\n\"eq\"");
    // Only the chosen branch is evaluated.
    try expectResult(db, "UNWIND [0, 2] AS z RETURN CASE WHEN z = 0 THEN 0 ELSE 10 / z END AS q", "q\n0\n5");
    // Nested, and an unaliased column is named after its text.
    try expectResult(db, "RETURN CASE WHEN 1 > 0 THEN CASE 2 WHEN 2 THEN 'in' END ELSE 'out' END", "CASE WHEN 1 > 0 THEN CASE 2 WHEN 2 THEN 'in' END ELSE 'out' END\n\"in\"");
    try expectFailure(db, "RETURN CASE END", .parse, "Expected WHEN in CASE");
    try expectFailure(db, "RETURN CASE WHEN true THEN 1", .parse, "Expected END to close CASE");
    try expectFailure(db, "RETURN CASE WHEN true 1 END", .parse, "Expected THEN after WHEN condition");
}

test "lcy: CASE in WHERE, ORDER BY and inside aggregates" {
    const path = "/tmp/lcy_case_places.ltdb";
    const db = try openTestDb(path);
    defer closeTestDb(db, path);
    try seedItems(db);

    try expectResult(db, "MATCH (i:Item) WHERE CASE WHEN i.n IS NULL THEN true ELSE i.n > 2 END RETURN i.name ORDER BY i.name", "i.name\n\"c\"\n\"d\"");
    try expectResult(db, "MATCH (i:Item) RETURN i.name ORDER BY CASE i.name WHEN 'c' THEN 0 ELSE 1 END, i.name DESC", "i.name\n\"c\"\n\"d\"\n\"b\"\n\"a\"");
    try expectResult(db, "MATCH (i:Item) RETURN sum(CASE WHEN i.n > 1 THEN 1 ELSE 0 END) AS big, count(CASE WHEN i.n IS NULL THEN 1 END) AS missing", "big | missing\n2 | 1");
    // Grouping by a CASE, and ordering by the grouped CASE after aggregation.
    try expectResult(db,
        \\MATCH (i:Item) RETURN CASE WHEN i.n > 1 THEN 'big' ELSE 'small' END AS size, count(*) AS c ORDER BY CASE WHEN i.n > 1 THEN 'big' ELSE 'small' END
    , "size | c\n\"big\" | 2\n\"small\" | 2");
}

test "lcy: the new keywords stay usable as names" {
    const path = "/tmp/lcy_keyword_names.ltdb";
    const db = try openTestDb(path);
    defer closeTestDb(db, path);

    // As property names, labels, relationship types and map keys.
    try run(db, "CREATE (:Case:When {case: 1, when: 2, then: 3, else: 4, end: 5, union: 6, all: 7, optional: 8, exists: 9, call: 10, foreach: 11})");
    try expectResult(db, "MATCH (n:Case:When) RETURN n.case, n.when, n.then, n.else, n.end", "n.case | n.when | n.then | n.else | n.end\n1 | 2 | 3 | 4 | 5");
    try expectResult(db, "MATCH (n:Case) RETURN n.union, n.all, n.optional, n.exists, n.call, n.foreach", "n.union | n.all | n.optional | n.exists | n.call | n.foreach\n6 | 7 | 8 | 9 | 10 | 11");
    try expectResult(db, "RETURN {case: 1, end: 2, union: 3}.end AS e", "e\n2");
    try run(db, "MATCH (a:Case) CREATE (a)-[:END]->(:Then {else: 'x'})");
    try expectResult(db, "MATCH (:Case)-[r:END]->(t:Then) RETURN t.else, type(r)", "t.else | type(r)\n\"x\" | \"END\"");
    // As variable names, the way `(start)-->(end)` is commonly written.
    try expectResult(db, "MATCH (start:Case)-[when]->(end) RETURN end.else AS x, type(when) AS t", "x | t\n\"x\" | \"END\"");
    try expectResult(db, "UNWIND [1, 2] AS then RETURN then ORDER BY then DESC", "then\n2\n1");
}

// ============================================================================
// Aggregates inside expressions
// ============================================================================

test "lcy: aggregates inside larger expressions" {
    const path = "/tmp/lcy_agg_expr.ltdb";
    const db = try openTestDb(path);
    defer closeTestDb(db, path);
    try seedItems(db);

    try expectResult(db, "MATCH (i:Item) RETURN count(i) + 1 AS c", "c\n5");
    try expectResult(db, "MATCH (i:Item) RETURN sum(i.n) / count(i.n) AS mean, toFloat(count(i)) AS f, count(*) AS n", "mean | f | n\n2 | 4 | 4");
    try expectResult(db, "MATCH (i:Item) RETURN CASE WHEN count(i.n) = count(*) THEN 'all' ELSE 'some' END AS has_n", "has_n\n\"some\"");
    try expectResult(db, "MATCH (i:Item) RETURN collect(i.name)[0..2] AS first, size(collect(i.n)) AS sized", "first | sized\n[\"a\", \"b\"] | 3");
    // The same aggregate written twice is computed once and used in both places.
    try expectResult(db, "MATCH (i:Item) RETURN count(*), count(*) * 10 AS ten", "count(*) | ten\n4 | 40");
    // Grouping keys that are expressions, and keys used inside an aggregating item.
    try expectResult(db,
        \\MATCH (i:Item) RETURN i.n % 2 AS parity, count(*) * 100 + coalesce(i.n % 2, -1) AS code ORDER BY parity
    , "parity | code\n0 | 100\n1 | 201\nnull | 99");
    try expectResult(db, "MATCH (i:Item) WHERE i.n IS NOT NULL RETURN i, i.name AS name, max(i.n) + i.n AS twice ORDER BY name", "i | name | twice\nN | \"a\" | 2\nN | \"b\" | 4\nN | \"c\" | 6");
    // Over no rows, a global aggregate still produces its one row.
    try expectResult(db, "MATCH (i:Nothing) RETURN count(i) + 1 AS c, coalesce(max(i.n), 0) AS m", "c | m\n1 | 0");
    // In WITH, then on.
    try expectResult(db, "MATCH (i:Item) WITH count(i) * 2 AS doubled, collect(i.n) AS ns RETURN doubled, size(ns) AS s", "doubled | s\n8 | 3");
    try expectResult(db, "MATCH (i:Item) WITH i.n IS NULL AS missing, count(*) + 0 AS c WHERE c > 1 RETURN missing, c", "missing | c\nfalse | 3");
    // DISTINCT and ORDER BY over the computed column.
    try expectResult(db, "UNWIND [1, 1, 2] AS x RETURN DISTINCT count(*) - count(*) AS z", "z\n0");
    try expectResult(db, "UNWIND [1, 1, 2] AS x RETURN x, count(*) * -1 AS c ORDER BY c", "x | c\n1 | -2\n2 | -1");
}

test "lcy: aggregates in expressions that cannot be planned say why" {
    const path = "/tmp/lcy_agg_expr_errors.ltdb";
    const db = try openTestDb(path);
    defer closeTestDb(db, path);
    try seedItems(db);

    try expectFailure(db, "MATCH (i:Item) RETURN i.n + count(*)", .plan, "RETURN item `i.n + count(*)` uses `i` outside an aggregate, but `i` is not a grouping key");
    try expectFailure(db, "MATCH (i:Item) RETURN count(count(*)) + 1", .plan, "count() cannot take an aggregate as its argument");
    try expectFailure(db, "MATCH (i:Item) RETURN sum(count(*))", .plan, "sum() cannot take an aggregate as its argument");
    try expectFailure(db, "MATCH (i:Item) RETURN sum(i.n, 2) + 1", .plan, "sum() takes one argument");
    try expectFailure(db, "MATCH (i:Item) WITH count(*) + i.n AS x RETURN x", .plan, "WITH item `count(*) + i.n` uses `i`");
    // More aggregates and keys than a row has slots.
    try expectFailure(db,
        \\MATCH (i:Item) RETURN i.n AS a, i.name AS b, count(*) + sum(i.n) + min(i.n) + max(i.n) + avg(i.n) + count(i.n) + sum(i.n + 1) + sum(i.n + 2) + sum(i.n + 3) + sum(i.n + 4) + sum(i.n + 5) + sum(i.n + 6) + sum(i.n + 7) + sum(i.n + 8) + sum(i.n + 9) AS big
    , .plan, "RETURN needs more than 16 values for its grouping keys and aggregates");
}

test "lcy: UNION ALL keeps every row, UNION removes duplicates" {
    const path = "/tmp/lcy_union.ltdb";
    const db = try openTestDb(path);
    defer closeTestDb(db, path);
    try seedItems(db);
    try run(db, "CREATE (:Tag {name: 'b'}), (:Tag {name: 'x'})");

    try expectResult(db, "RETURN 1 AS x UNION ALL RETURN 1 AS x UNION ALL RETURN 2 AS x", "x\n1\n1\n2");
    try expectResult(db, "RETURN 1 AS x UNION RETURN 1 AS x UNION RETURN 2 AS x", "x\n1\n2");
    // Rows come from the queries in the order they are written.
    try expectResult(db,
        \\MATCH (i:Item) WHERE i.n >= 2 RETURN i.name AS name ORDER BY name
        \\UNION ALL MATCH (t:Tag) RETURN t.name AS name ORDER BY name DESC
    , "name\n\"b\"\n\"c\"\n\"x\"\n\"b\"");
    try expectResult(db,
        \\MATCH (i:Item) WHERE i.n >= 2 RETURN i.name AS name
        \\UNION MATCH (t:Tag) RETURN t.name AS name
    , "name\n\"b\"\n\"c\"\n\"x\"");
    // Duplicates within one query go too, and null equals null here.
    try expectResult(db, "UNWIND [1, 1, null, null] AS v RETURN v UNION UNWIND [null] AS v RETURN v", "v\n1\nnull");
    // ORDER BY, SKIP and LIMIT belong to the query they follow.
    try expectResult(db, "MATCH (i:Item) RETURN i.name AS n ORDER BY n LIMIT 1 UNION ALL MATCH (i:Item) RETURN i.name AS n ORDER BY n DESC LIMIT 1", "n\n\"a\"\n\"d\"");
    // The same variable name may mean different things in each query, and an
    // unaliased column is named after its text in each.
    try expectResult(db, "MATCH (n:Tag) RETURN n.name AS v UNION ALL UNWIND ['z'] AS n RETURN n AS v", "v\n\"b\"\n\"x\"\n\"z\"");
    try expectResult(db, "MATCH (n:Tag) RETURN n.name UNION ALL MATCH (n:Item {name: 'a'}) RETURN n.name", "n.name\n\"b\"\n\"x\"\n\"a\"");
    try expectResult(db, "MATCH (a:Item {name: 'a'}) RETURN a AS v, 1 AS k UNION ALL MATCH (b:Tag {name: 'x'}) RETURN b AS v, 2 AS k", "v | k\nN | 1\nN | 2");
    // Aggregates and WITH inside the queries.
    try expectResult(db, "MATCH (i:Item) RETURN count(*) AS c UNION ALL MATCH (t:Tag) WITH t.name AS name RETURN count(name) AS c", "c\n4\n2");
    // A query that matches nothing adds nothing.
    try expectResult(db, "MATCH (i:Nope) RETURN i.name AS n UNION ALL RETURN 'only' AS n", "n\n\"only\"");
    // Keywords spelled in any case, UNION as a name stays usable.
    try expectResult(db, "RETURN 1 AS union union all RETURN 2 AS union", "union\n1\n2");
    try run(db, "CREATE (:Union {union: 'u'})");
    try expectResult(db, "MATCH (u:Union) RETURN u.union, u['union'] AS k, {union: 1}.union AS m", "u.union | k | m\n\"u\" | \"u\" | 1");
}

test "lcy: UNION writes run in order and one query cannot see another's variables" {
    const path = "/tmp/lcy_union_writes.ltdb";
    const db = try openTestDb(path);
    defer closeTestDb(db, path);

    try expectResult(db, "CREATE (a:U {v: 1}) RETURN a.v AS v UNION ALL CREATE (b:U {v: 2}) RETURN b.v AS v", "v\n1\n2");
    // The second query runs after the first, so it sees what the first created.
    try expectResult(db, "CREATE (:U {v: 3}) RETURN 0 AS c UNION ALL MATCH (u:U) RETURN count(u) AS c", "c\n0\n3");
    try expectFailure(db, "MATCH (a:U) RETURN a.v AS v UNION RETURN a.v AS v", .semantic, "Variable 'a' is not defined");
}

test "lcy: UNION that cannot be planned says why" {
    const path = "/tmp/lcy_union_errors.ltdb";
    const db = try openTestDb(path);
    defer closeTestDb(db, path);

    try expectFailure(db, "RETURN 1 AS x UNION RETURN 1 AS x UNION ALL RETURN 1 AS x", .parse, "UNION and UNION ALL cannot be mixed in one query");
    try expectFailure(db, "RETURN 1 AS x UNION ALL RETURN 1 AS x UNION RETURN 1 AS x", .parse, "UNION and UNION ALL cannot be mixed in one query");
    try expectFailure(db, "UNION RETURN 1 AS x", .parse, "UNION needs a query before it");
    try expectFailure(db, "RETURN 1 AS x UNION", .parse, "Expected a query after UNION");
    try expectFailure(db, "RETURN 1 AS x UNION UNION RETURN 1 AS x", .parse, "UNION needs a query before it");
    try expectFailure(db, "RETURN 1 AS x UNION RETURN 1 AS x, 2 AS y", .plan, "All queries in a UNION must return the same number of columns; query 1 returns 1, query 2 returns 2");
    try expectFailure(db, "RETURN 1 AS x UNION RETURN 1 AS y", .plan, "column 1 is `x` in query 1 and `y` in query 2");
    try expectFailure(db, "RETURN 1 AS x UNION CREATE (:U)", .plan, "Every query in a UNION must end with RETURN; query 2 does not");
    try expectFailure(db, "CREATE (:U) UNION RETURN 1 AS x", .plan, "Every query in a UNION must end with RETURN; query 1 does not");
}

/// People and who they know: ann knows bob and cy, bob knows cy, cy and dee
/// know nobody, and dee is not a Person's friend at all.
fn seedPeople(db: *Database) !void {
    try run(db, "CREATE (:P {name: 'ann'}), (:P {name: 'bob'}), (:P {name: 'cy'}), (:P {name: 'dee'})");
    try run(db, "MATCH (a:P {name: 'ann'}), (b:P {name: 'bob'}) CREATE (a)-[:KNOWS {since: 2001}]->(b)");
    try run(db, "MATCH (a:P {name: 'ann'}), (c:P {name: 'cy'}) CREATE (a)-[:KNOWS {since: 2010}]->(c)");
    try run(db, "MATCH (b:P {name: 'bob'}), (c:P {name: 'cy'}) CREATE (b)-[:KNOWS {since: 2020}]->(c)");
}

test "lcy: OPTIONAL MATCH keeps rows that do not match, with nulls" {
    const path = "/tmp/lcy_optional.ltdb";
    const db = try openTestDb(path);
    defer closeTestDb(db, path);
    try seedPeople(db);

    try expectResult(db,
        \\MATCH (p:P) OPTIONAL MATCH (p)-[k:KNOWS]->(f) RETURN p.name, f.name, k.since ORDER BY p.name, f.name
    , "p.name | f.name | k.since\n\"ann\" | \"bob\" | 2001\n\"ann\" | \"cy\" | 2010\n\"bob\" | \"cy\" | 2020\n\"cy\" | null | null\n\"dee\" | null | null");
    // The WHERE belongs to the OPTIONAL MATCH: a rejected match becomes
    // nulls, the row stays.
    try expectResult(db,
        \\MATCH (p:P) OPTIONAL MATCH (p)-[k:KNOWS]->(f) WHERE k.since > 2005 RETURN p.name, f.name ORDER BY p.name
    , "p.name | f.name\n\"ann\" | \"cy\"\n\"bob\" | \"cy\"\n\"cy\" | null\n\"dee\" | null");
    // A WHERE after a WITH that follows filters rows away as usual.
    try expectResult(db,
        \\MATCH (p:P) OPTIONAL MATCH (p)-->(f) WITH p, f WHERE f IS NULL RETURN p.name ORDER BY p.name
    , "p.name\n\"cy\"\n\"dee\"");
    // Counting the matches: nulls are not counted.
    try expectResult(db,
        \\MATCH (p:P) OPTIONAL MATCH (p)-->(f) RETURN p.name, count(f) AS friends ORDER BY p.name
    , "p.name | friends\n\"ann\" | 2\n\"bob\" | 1\n\"cy\" | 0\n\"dee\" | 0");
    // As the first clause, and with nothing to match: one row of nulls.
    try expectResult(db, "OPTIONAL MATCH (n:Nope) RETURN n", "n\nnull");
    try expectResult(db, "OPTIONAL MATCH (n:Nope) RETURN n.name, count(n) AS c", "n.name | c\nnull | 0");
    // A node not connected to the input is a cross product per row.
    try expectResult(db,
        \\MATCH (p:P {name: 'dee'}) OPTIONAL MATCH (q:P) WHERE q.name < 'bz' RETURN p.name, q.name ORDER BY q.name
    , "p.name | q.name\n\"dee\" | \"ann\"\n\"dee\" | \"bob\"");
    // Undirected, once per input row: each round starts from its own row.
    try expectResult(db,
        \\MATCH (p:P) OPTIONAL MATCH (p)-[:KNOWS]-(f) RETURN p.name, count(f) AS c ORDER BY p.name
    , "p.name | c\n\"ann\" | 2\n\"bob\" | 2\n\"cy\" | 2\n\"dee\" | 0");
    // Undirected, both ends bound, and variable length.
    try expectResult(db,
        \\MATCH (p:P {name: 'cy'}) OPTIONAL MATCH (p)-[:KNOWS]-(f) RETURN f.name ORDER BY f.name
    , "f.name\n\"ann\"\n\"bob\"");
    try expectResult(db,
        \\MATCH (a:P {name: 'cy'}), (b:P) OPTIONAL MATCH (a)<-[k]-(b) RETURN b.name, k.since ORDER BY b.name
    , "b.name | k.since\n\"ann\" | 2010\n\"bob\" | 2020\n\"cy\" | null\n\"dee\" | null");
    try expectResult(db,
        \\MATCH (p:P) WHERE p.name IN ['ann', 'dee'] OPTIONAL MATCH (p)-[*2]->(f) RETURN p.name, f.name ORDER BY p.name
    , "p.name | f.name\n\"ann\" | \"cy\"\n\"dee\" | null");
}

test "lcy: nulls from OPTIONAL MATCH flow into later clauses" {
    const path = "/tmp/lcy_optional_chain.ltdb";
    const db = try openTestDb(path);
    defer closeTestDb(db, path);
    try seedPeople(db);

    // A second OPTIONAL MATCH from a null node matches nothing.
    try expectResult(db,
        \\MATCH (p:P) OPTIONAL MATCH (p)-->(f {name: 'bob'}) OPTIONAL MATCH (f)-->(g) RETURN p.name, f.name, g.name ORDER BY p.name
    , "p.name | f.name | g.name\n\"ann\" | \"bob\" | \"cy\"\n\"bob\" | null | null\n\"cy\" | null | null\n\"dee\" | null | null");
    // A MATCH from a null node matches nothing either (the row goes).
    try expectResult(db,
        \\MATCH (p:P) OPTIONAL MATCH (p)-->(f {name: 'bob'}) MATCH (f)-->(g) RETURN p.name, g.name
    , "p.name | g.name\n\"ann\" | \"cy\"");
    try expectResult(db,
        \\MATCH (p:P {name: 'dee'}) OPTIONAL MATCH (p)-->(f) MATCH (f)-[*1..2]->(g) RETURN g
    , "g");
    try expectResult(db,
        \\MATCH (p:P {name: 'dee'}) OPTIONAL MATCH (p)-->(f) MATCH (a:P {name: 'ann'})-->(f) RETURN a
    , "a");
    try expectResult(db,
        \\MATCH (p:P {name: 'dee'}) OPTIONAL MATCH (p)-->(f) RETURN f, f.name, f IS NULL AS missing, coalesce(f.name, 'none') AS name
    , "f | f.name | missing | name\nnull | null | true | \"none\"");
}

test "lcy: writing to what OPTIONAL MATCH did not find does nothing" {
    const path = "/tmp/lcy_optional_writes.ltdb";
    const db = try openTestDb(path);
    defer closeTestDb(db, path);
    try seedPeople(db);

    try expectResult(db,
        \\MATCH (p:P) OPTIONAL MATCH (p)-[k:KNOWS]->(f {name: 'bob'})
        \\SET f.seen = true, k.seen = true, f:Seen, f.gone = 1 REMOVE f.gone, k.since, f:Seen
        \\RETURN p.name, f IS NULL AS missing ORDER BY p.name
    , "p.name | missing\n\"ann\" | false\n\"bob\" | true\n\"cy\" | true\n\"dee\" | true");
    try expectResult(db, "MATCH (n) WHERE n.seen RETURN n.name, n.gone, labels(n) AS l", "n.name | n.gone | l\n\"bob\" | null | [\"P\"]");
    try expectResult(db, "MATCH ()-[k]->() WHERE k.seen RETURN k.since", "k.since\nnull");
    try expectResult(db,
        \\MATCH (p:P {name: 'dee'}) OPTIONAL MATCH (p)-[k]->(f) SET f += {a: 1} SET k = {b: 2} DELETE k DETACH DELETE f RETURN p.name
    , "p.name\n\"dee\"");
    try expectResult(db, "MATCH (n) RETURN count(n) AS c", "c\n4");
    // Creating a relationship to null is an error, as in openCypher.
    try expectFailure(db, "MATCH (p:P {name: 'dee'}) OPTIONAL MATCH (p)-->(f) CREATE (p)-[:R]->(f)", .execution, "CREATE needs a node at each end of a relationship, but one end is null");
}

test "lcy: OPTIONAL stays usable as a name and needs MATCH" {
    const path = "/tmp/lcy_optional_names.ltdb";
    const db = try openTestDb(path);
    defer closeTestDb(db, path);

    try run(db, "CREATE (optional:Optional {optional: 1})");
    try expectResult(db, "MATCH (optional:Optional) RETURN optional.optional", "optional.optional\n1");
    try expectResult(db, "optional match (o:Optional) return o.optional", "o.optional\n1");
    try expectFailure(db, "OPTIONAL (n) RETURN n", .parse, "Expected MATCH after OPTIONAL");
}

test "lcy: exists() on a property is IS NOT NULL" {
    const path = "/tmp/lcy_exists_prop.ltdb";
    const db = try openTestDb(path);
    defer closeTestDb(db, path);
    try seedItems(db);

    try expectResult(db, "MATCH (i:Item) WHERE exists(i.n) RETURN i.name ORDER BY i.name", "i.name\n\"a\"\n\"b\"\n\"c\"");
    try expectResult(db, "MATCH (i:Item) WHERE NOT exists(i.n) RETURN i.name", "i.name\n\"d\"");
    try expectResult(db, "MATCH (i:Item {name: 'd'}) RETURN exists(i.n) AS e, exists(i.name) AS f, EXISTS({k: 1}.k) AS g", "e | f | g\nfalse | true | true");
}

test "lcy: named paths" {
    const path = "/tmp/lcy_paths.ltdb";
    const db = try openTestDb(path);
    defer closeTestDb(db, path);
    try seedPeople(db);

    // Fixed length: the nodes in order, the relationships between them.
    try expectResult(db,
        \\MATCH p = (a:P {name: 'ann'})-[:KNOWS]->(b)-[:KNOWS]->(c)
        \\RETURN length(p) AS len, [nodes(p)[0].name, nodes(p)[1].name, nodes(p)[2].name] AS names, [relationships(p)[0].since, relationships(p)[1].since] AS since
    , "len | names | since\n2 | [\"ann\", \"bob\", \"cy\"] | [2001, 2020]");
    // A path as a value: a map of its nodes and relationships.
    try expectResult(db, "MATCH p = (:P {name: 'bob'})-->() RETURN p", "p\n{nodes: [N, N], relationships: [E]}");
    // Anonymous relationships and an incoming hop keep the order written.
    try expectResult(db,
        \\MATCH p = (c:P {name: 'cy'})<--(b:P {name: 'bob'}) RETURN [nodes(p)[0].name, nodes(p)[1].name, nodes(p)[2].name] AS names, length(p) AS len
    , "names | len\n[\"cy\", \"bob\", null] | 1");
    // A single node is a path of length 0.
    try expectResult(db, "MATCH p = (d:P {name: 'dee'}) RETURN length(p), size(nodes(p)) AS n, relationships(p) AS r", "length(p) | n | r\n0 | 1 | []");
    // Variable length: each walk is its own path.
    try expectResult(db,
        \\MATCH p = (a:P {name: 'ann'})-[:KNOWS*1..2]->(x)
        \\RETURN [nodes(p)[0].name, nodes(p)[1].name, nodes(p)[2].name] AS names, length(p) AS len ORDER BY len, names
    , "names | len\n[\"ann\", \"bob\", null] | 1\n[\"ann\", \"cy\", null] | 1\n[\"ann\", \"bob\", \"cy\"] | 2");
    try expectResult(db,
        \\MATCH p = (a:P {name: 'ann'})-[*0..1]->(x)-->(y) RETURN [nodes(p)[0].name, nodes(p)[1].name, nodes(p)[2].name] AS names ORDER BY names
    , "names\n[\"ann\", \"bob\", \"cy\"]\n[\"ann\", \"bob\", null]\n[\"ann\", \"cy\", null]");
    // Paths through WITH, compared, grouped and filtered on.
    try expectResult(db,
        \\MATCH p = (:P)-[:KNOWS*]->(c:P {name: 'cy'}) WITH p WHERE length(p) > 1 RETURN length(p) AS len, count(*) AS c
    , "len | c\n2 | 1");
    try expectResult(db, "MATCH p = (a)-->(b) MATCH q = (a)-->(b) RETURN count(*) AS c, sum(CASE WHEN p = q THEN 1 ELSE 0 END) AS same", "c | same\n3 | 3");
    try expectResult(db, "MATCH p = (a:P)-->() RETURN DISTINCT length(p) AS l", "l\n1");
    // From an unlabelled node by type, which walks the relationships of
    // that type instead of expanding every node: the hop is still there.
    try expectResult(db, "MATCH p = (a)-[:KNOWS]->(b) RETURN length(p) AS len, size(nodes(p)) AS n", "len | n\n1 | 2\n1 | 2\n1 | 2");
    try expectResult(db,
        \\MATCH p = (a)<-[:KNOWS]-(b) RETURN [nodes(p)[0].name, nodes(p)[1].name] AS names, relationships(p)[0] IS NOT NULL AS r ORDER BY names
    , "names | r\n[\"bob\", \"ann\"] | true\n[\"cy\", \"ann\"] | true\n[\"cy\", \"bob\"] | true");
    // In OPTIONAL MATCH a path that is not there is null.
    try expectResult(db,
        \\MATCH (d:P {name: 'dee'}) OPTIONAL MATCH p = (d)-->() RETURN p, length(p) AS len, nodes(p) AS ns
    , "p | len | ns\nnull | null | null");
    try expectFailure(db, "CREATE p = (:X)-[:R]->(:Y)", .parse, "A named path in CREATE is not supported");
    try expectFailure(db, "MERGE p = (:X)", .parse, "A named path in MERGE is not supported");
    try expectFailure(db, "MATCH p (a) RETURN a", .parse, "Expected '=' after a path variable");
}

test "lcy: list comprehensions" {
    const path = "/tmp/lcy_comprehension.ltdb";
    const db = try openTestDb(path);
    defer closeTestDb(db, path);
    try seedItems(db);

    try expectResult(db, "RETURN [x IN [1, 2, 3, 4] WHERE x % 2 = 0 | x * 10] AS l", "l\n[20, 40]");
    try expectResult(db, "RETURN [x IN [1, 2, 3] | x + 1] AS a, [x IN [1, null, 3] WHERE x > 1] AS b, [x IN [1, 2]] AS c", "a | b | c\n[2, 3, 4] | [3] | [1, 2]");
    try expectResult(db, "RETURN [x IN [] | x] AS a, [x IN null | x] AS b", "a | b\n[] | null");
    // Nested, with an outer variable, and shadowing one.
    try expectResult(db, "WITH 10 AS k RETURN [x IN [1, 2] | [y IN [x, x + 1] | y * k]] AS l", "l\n[[10, 20], [20, 30]]");
    try expectResult(db, "WITH 5 AS x RETURN [x IN [1, 2] | x] AS inner, x AS outer", "inner | outer\n[1, 2] | 5");
    // Over nodes, into aggregates, and in WHERE.
    try expectResult(db, "MATCH (i:Item) WITH collect(i) AS items RETURN [i IN items WHERE i.n > 1 | i.name] AS names", "names\n[\"b\", \"c\"]");
    try expectResult(db, "MATCH (i:Item) RETURN [n IN collect(i.n) WHERE n >= 2] AS big, size([n IN collect(i.n) | n]) AS c", "big | c\n[2, 3] | 3");
    try expectResult(db, "MATCH (i:Item) WHERE size([c IN [i.name, 'z'] WHERE c = 'b']) > 0 RETURN i.name", "i.name\n\"b\"");
    // A non-list is a type error.
    try expectFailure(db, "RETURN [x IN 5 | x] AS l", .execution, "`x IN integer` is not defined");
    try expectFailure(db, "RETURN [x IN [1] | y] AS l", .semantic, "Variable 'y' is not defined");
    try expectFailure(db, "RETURN [x IN [1] | x] AS l, x", .semantic, "Variable 'x' is not defined");
}

test "lcy: all, any, none and single" {
    const path = "/tmp/lcy_quantifiers.ltdb";
    const db = try openTestDb(path);
    defer closeTestDb(db, path);
    try seedItems(db);

    try expectResult(db,
        \\RETURN all(x IN [1, 2] WHERE x > 0) AS a, any(x IN [1, 2] WHERE x > 1) AS b, none(x IN [1, 2] WHERE x > 5) AS c, single(x IN [1, 2] WHERE x > 1) AS d
    , "a | b | c | d\ntrue | true | true | true");
    try expectResult(db,
        \\RETURN all(x IN [] WHERE false) AS a, any(x IN [] WHERE true) AS b, none(x IN [] WHERE true) AS c, single(x IN [] WHERE true) AS d
    , "a | b | c | d\ntrue | false | true | false");
    // Three-valued: a null condition is unknown.
    try expectResult(db,
        \\RETURN all(x IN [1, null] WHERE x > 0) AS a, any(x IN [1, null] WHERE x > 0) AS b, any(x IN [0, null] WHERE x > 0) AS c, none(x IN [0, null] WHERE x > 0) AS d, single(x IN [1, null] WHERE x > 0) AS e, all(x IN [0, null] WHERE x > 0) AS f
    , "a | b | c | d | e | f\nnull | true | null | null | null | false");
    try expectResult(db, "RETURN all(x IN null WHERE true) AS a, single(x IN [1, 1] WHERE x = 1) AS b", "a | b\nnull | false");
    try expectResult(db, "MATCH (i:Item) WHERE any(k IN [2, 3] WHERE i.n = k) RETURN i.name ORDER BY i.name", "i.name\n\"b\"\n\"c\"");
    // The names stay usable: a property, a variable, a function-like call.
    try expectResult(db, "WITH {all: 1, any: 2} AS m, 3 AS single RETURN m.all, m.any, single", "m.all | m.any | single\n1 | 2 | 3");
    try expectFailure(db, "RETURN all(x IN [1]) AS a", .parse, "Expected WHERE in all(), any(), none() or single()");
}

test "lcy: reduce" {
    const path = "/tmp/lcy_reduce.ltdb";
    const db = try openTestDb(path);
    defer closeTestDb(db, path);
    try seedItems(db);

    try expectResult(db, "RETURN reduce(s = 0, x IN [1, 2, 3] | s + x) AS sum, reduce(s = '', x IN ['a', 'b'] | s + x) AS cat", "sum | cat\n6 | \"ab\"");
    try expectResult(db, "RETURN reduce(s = 7, x IN [] | s + x) AS a, reduce(s = 7, x IN null | s + x) AS b", "a | b\n7 | null");
    try expectResult(db, "MATCH (i:Item) RETURN reduce(t = 0, n IN collect(i.n) | t + n * n) AS squares", "squares\n14");
    try expectResult(db, "RETURN reduce(acc = [], x IN [1, 2, 3] | [x] + acc) AS rev", "rev\n[3, 2, 1]");
    try expectFailure(db, "RETURN reduce(s = 0, s IN [1] | s) AS a", .semantic, "reduce() needs two different names");
    try expectFailure(db, "RETURN reduce(s = 0, x IN [1]) AS a", .parse, "Expected '|' before the expression in reduce()");
    try expectFailure(db, "RETURN reduce(s 0, x IN [1] | s) AS a", .parse, "");
}

test "lcy: nodes and relationships inside lists stay nodes and relationships" {
    const path = "/tmp/lcy_ref_lists.ltdb";
    const db = try openTestDb(path);
    defer closeTestDb(db, path);
    try seedPeople(db);

    // They used to become their ids once a list of them was stored in a
    // row, so `n.name` on an element was null.
    try expectResult(db, "MATCH (p:P) WITH collect(p) AS ps UNWIND ps AS q RETURN q.name ORDER BY q.name", "q.name\n\"ann\"\n\"bob\"\n\"cy\"\n\"dee\"");
    try expectResult(db, "MATCH (p:P {name: 'ann'}) RETURN collect(p) AS ps, [p] AS l, {who: p} AS m", "ps | l | m\n[N] | [N] | {who: N}");
    try expectResult(db, "MATCH (p:P {name: 'ann'}) WITH {who: p} AS m RETURN m.who.name AS name", "name\n\"ann\"");
    try expectResult(db,
        \\MATCH p = (:P {name: 'ann'})-->(:P {name: 'bob'})-->() WITH nodes(p) AS ns, relationships(p) AS rs
        \\RETURN [n IN ns | n.name] AS names, [r IN rs | r.since] AS since
    , "names | since\n[\"ann\", \"bob\", \"cy\"] | [2001, 2020]");
    try expectResult(db, "MATCH (p:P) WITH collect(p) AS ps RETURN DISTINCT size(ps) AS n", "n\n4");
    // Plain lists are unchanged.
    try expectResult(db, "WITH [1, 'a'] AS l RETURN l, l[1]", "l | l[1]\n[1, \"a\"] | \"a\"");
}

test "lcy: pattern predicates, EXISTS and COUNT" {
    const path = "/tmp/lcy_pattern_predicates.ltdb";
    const db = try openTestDb(path);
    defer closeTestDb(db, path);
    try seedPeople(db);

    try expectResult(db, "MATCH (p:P) WHERE (p)-[:KNOWS]->() RETURN p.name ORDER BY p.name", "p.name\n\"ann\"\n\"bob\"");
    try expectResult(db, "MATCH (p:P) WHERE NOT (p)-->() RETURN p.name ORDER BY p.name", "p.name\n\"cy\"\n\"dee\"");
    try expectResult(db, "MATCH (p:P) WHERE exists((p)<--()) RETURN p.name ORDER BY p.name", "p.name\n\"bob\"\n\"cy\"");
    try expectResult(db,
        \\MATCH (p:P) WHERE EXISTS { MATCH (p)-[k:KNOWS]->(f) WHERE k.since > 2015 } RETURN p.name
    , "p.name\n\"bob\"");
    try expectResult(db, "MATCH (p:P) WHERE EXISTS { (p)-->(:P {name: 'cy'}) } RETURN p.name ORDER BY p.name", "p.name\n\"ann\"\n\"bob\"");
    // Both ends bound.
    try expectResult(db, "MATCH (a:P), (b:P) WHERE a.name < b.name AND (a)-->(b) RETURN a.name, b.name ORDER BY a.name, b.name", "a.name | b.name\n\"ann\" | \"bob\"\n\"ann\" | \"cy\"\n\"bob\" | \"cy\"");
    // As values, counted, and in an aggregate.
    try expectResult(db,
        \\MATCH (p:P) RETURN p.name, (p)-->() AS knows, COUNT { (p)-->() } AS out, COUNT { MATCH (p)<--(q) WHERE q.name <> 'ann' } AS fans ORDER BY p.name
    , "p.name | knows | out | fans\n\"ann\" | true | 2 | 0\n\"bob\" | true | 1 | 0\n\"cy\" | false | 0 | 1\n\"dee\" | false | 0 | 0");
    try expectResult(db, "MATCH (p:P) RETURN sum(COUNT { (p)-->() }) AS edges", "edges\n3");
    // After WITH, in its WHERE, and in OPTIONAL MATCH's WHERE.
    try expectResult(db, "MATCH (p:P) WITH p AS q WHERE (q)-->() RETURN count(*) AS c", "c\n2");
    try expectResult(db,
        \\MATCH (p:P {name: 'dee'}) OPTIONAL MATCH (q:P) WHERE (q)-->(:P {name: 'bob'}) RETURN q.name
    , "q.name\n\"ann\"");
    // A variable the subquery introduces stays inside it.
    try expectFailure(db, "MATCH (p:P) WHERE (p)-->(f) RETURN f", .semantic, "Variable 'f' is not defined");
    // Not a pattern: still a parenthesised expression.
    try expectResult(db, "WITH 3 AS a RETURN (a) - 1 AS x, (a)-1 AS y, (a) < -1 AS z", "x | y | z\n2 | 2 | false");
    try expectFailure(db, "MATCH (p) WHERE EXISTS { MATCH (p)-->(q) RETURN q } RETURN p", .parse, "EXISTS { ... } supports only a MATCH pattern and WHERE");
    try expectFailure(db, "MATCH (p {x: EXISTS { (p)-->() }}) RETURN p", .plan, "is not supported here");
    try expectFailure(db, "MATCH (p:P) RETURN [x IN [1] WHERE (p)-->()] AS l", .plan, "inside a list comprehension, quantifier or reduce() is not supported");
}

test "lcy: pattern comprehensions" {
    const path = "/tmp/lcy_pattern_comprehension.ltdb";
    const db = try openTestDb(path);
    defer closeTestDb(db, path);
    try seedPeople(db);

    try expectResult(db,
        \\MATCH (p:P) RETURN p.name, [(p)-[k:KNOWS]->(f) WHERE k.since < 2015 | f.name] AS early ORDER BY p.name
    , "p.name | early\n\"ann\" | [\"bob\", \"cy\"]\n\"bob\" | []\n\"cy\" | []\n\"dee\" | []");
    try expectResult(db, "MATCH (p:P {name: 'ann'}) RETURN size([(p)-->(f) | f]) AS n, [x = (p)-->()-->() | length(x)] AS lens", "n | lens\n2 | [2]");
    // Nested inside another subquery, and over the result of OPTIONAL MATCH.
    try expectResult(db,
        \\MATCH (p:P {name: 'ann'}) RETURN [(p)-->(f) WHERE (f)-->() | f.name] AS hubs
    , "hubs\n[\"bob\"]");
    try expectResult(db,
        \\MATCH (p:P {name: 'dee'}) OPTIONAL MATCH (p)-->(f) RETURN [(f)-->(g) | g] AS l, (f)-->() AS e
    , "l | e\n[] | false");
    // A list literal that starts with a parenthesis is still a list.
    try expectResult(db, "WITH 2 AS a RETURN [(a), (a) + 1] AS l", "l\n[2, 3]");
}

test "lcy: CALL built-in procedures" {
    const path = "/tmp/lcy_call.ltdb";
    const db = try openTestDb(path);
    defer closeTestDb(db, path);
    try seedPeople(db);
    try run(db, "CREATE (:Tag {label: 'x'})");
    try db.createNodePropertyIndex("P", "name");
    try db.createNodeFtsIndex("Tag", "label");

    try expectResult(db, "CALL db.labels()", "label\n\"P\"\n\"Tag\"");
    try expectResult(db, "CALL db.relationshipTypes()", "relationshipType\n\"KNOWS\"");
    try expectResult(db, "CALL db.propertyKeys()", "propertyKey\n\"label\"\n\"name\"\n\"since\"");
    try expectResult(db, "CALL db.indexes()", "type | entityType | labelOrType | property\n\"fulltext\" | \"NODE\" | \"Tag\" | \"label\"\n\"property\" | \"NODE\" | \"P\" | \"name\"");
    // YIELD picks, renames and orders columns; WHERE filters them.
    try expectResult(db, "CALL db.indexes() YIELD property, type AS kind WHERE kind = 'property'", "property | kind\n\"name\" | \"property\"");
    try expectResult(db, "call DB.LABELS() yield *", "label\n\"P\"\n\"Tag\"");
    // Inside a query.
    try expectResult(db, "CALL db.labels() YIELD label MATCH (n) WHERE label IN labels(n) RETURN label, count(n) AS c ORDER BY label", "label | c\n\"P\" | 4\n\"Tag\" | 1");
    try expectResult(db, "MATCH (p:P {name: 'ann'}) CALL db.relationshipTypes() YIELD relationshipType AS t RETURN p.name, t", "p.name | t\n\"ann\" | \"KNOWS\"");
    try expectResult(db, "UNWIND [1, 2] AS i CALL db.labels() YIELD label RETURN i, label ORDER BY i, label", "i | label\n1 | \"P\"\n1 | \"Tag\"\n2 | \"P\"\n2 | \"Tag\"");
    // CALL and YIELD stay usable as names.
    try expectResult(db, "WITH 1 AS call, 2 AS yield RETURN call + yield AS s", "s\n3");

    try expectFailure(db, "CALL db.nope()", .plan, "Unknown procedure 'db.nope'");
    try expectFailure(db, "CALL db.labels(1)", .plan, "db.labels() takes no arguments");
    try expectFailure(db, "CALL db.labels() YIELD name", .plan, "db.labels() has no column `name`; it yields `label`");
    try expectFailure(db, "CALL db.labels() RETURN 1 AS x", .plan, "CALL db.labels() inside a larger query needs YIELD");
    try expectFailure(db, "CALL db.labels() YIELD * RETURN 1 AS x", .plan, "YIELD * is only allowed when CALL db.labels() is the whole query");
    try expectFailure(db, "CALL db.labels() YIELD label, label", .semantic, "already defined");
    try expectFailure(db, "CALL { MATCH (n) RETURN n } RETURN 1", .parse, "CALL { ... } subqueries are not supported");
}

test "lcy: CALL on an empty database" {
    const path = "/tmp/lcy_call_empty.ltdb";
    const db = try openTestDb(path);
    defer closeTestDb(db, path);
    try expectResult(db, "CALL db.labels()", "label");
    try expectResult(db, "CALL db.indexes()", "type | entityType | labelOrType | property");
    try expectResult(db, "CALL db.propertyKeys()", "propertyKey");
}
