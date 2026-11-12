//! End-to-end tests for the second round of Cypher features: the variable
//! of a variable-length relationship, `RETURN *`, map projection,
//! shortestPath, `CALL { }`, label expressions, full subqueries in EXISTS,
//! COUNT and COLLECT, and the planning errors and small fixes that came with
//! them.
//!
//! Results are compared as text, as in lcy_cypher_features_test.zig: one row
//! per line, `N` for a node and `E` for a relationship.

const std = @import("std");
const lattice = @import("lattice");

const Database = lattice.storage.database.Database;
const QueryResult = lattice.storage.database.QueryResult;
const ResultValue = lattice.storage.database.ResultValue;
const QueryFailureStage = lattice.storage.database.QueryFailureStage;
const Params = std.StringHashMap(lattice.core.types.PropertyValue);

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
    return expectResultWith(db, null, cypher, expected);
}

fn expectResultWith(db: *Database, params: ?*const Params, cypher: []const u8, expected: []const u8) !void {
    var detailed = if (params) |p| try db.queryWithParamsDetailed(cypher, p) else try db.queryDetailed(cypher);
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
    return expectFailureWith(db, null, cypher, stage, needle);
}

fn expectFailureWith(db: *Database, params: ?*const Params, cypher: []const u8, stage: QueryFailureStage, needle: []const u8) !void {
    var detailed = if (params) |p| try db.queryWithParamsDetailed(cypher, p) else try db.queryDetailed(cypher);
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
// The variable of a variable-length relationship
// ============================================================================

/// a -R{w:1}-> b -R{w:2}-> c -R{w:3}-> d, and a -S-> d.
fn seedChain(db: *Database) !void {
    try run(db,
        \\CREATE (a:V {name: 'a'}), (b:V {name: 'b'}), (c:V {name: 'c'}), (d:V {name: 'd'}),
        \\       (a)-[:R {w: 1}]->(b), (b)-[:R {w: 2}]->(c), (c)-[:R {w: 3}]->(d), (a)-[:S {w: 9}]->(d)
    );
}

test "lcy: a variable-length relationship's variable is the list of its relationships" {
    const path = "/tmp/lcy_varlen_rels.ltdb";
    const db = try openTestDb(path);
    defer closeTestDb(db, path);
    try seedChain(db);

    try expectResult(db,
        \\MATCH (:V {name: 'a'})-[r:R*1..3]->(x) RETURN x.name, size(r), [e IN r | e.w] AS ws ORDER BY x.name
    , "x.name | size(r) | ws\n\"b\" | 1 | [1]\n\"c\" | 2 | [1, 2]\n\"d\" | 3 | [1, 2, 3]");
    // The list itself, its elements and their types.
    try expectResult(db,
        \\MATCH (:V {name: 'a'})-[r:R*2]->(x) RETURN r, type(r[0]), r[-1].w
    , "r | type(r[0]) | r[-1].w\n[E, E] | \"R\" | 2");
    // Zero hops: an empty list.
    try expectResult(db,
        \\MATCH (a:V {name: 'a'})-[r:R*0..1]->(x) RETURN x.name, r ORDER BY x.name
    , "x.name | r\n\"a\" | []\n\"b\" | [E]");
    // In predicates.
    try expectResult(db,
        \\MATCH (:V {name: 'a'})-[r*1..3]->(x) WHERE all(e IN r WHERE e.w < 3) RETURN x.name ORDER BY x.name
    , "x.name\n\"b\"\n\"c\"");
    try expectResult(db,
        \\MATCH (:V {name: 'a'})-[r*]->(x) WHERE any(e IN r WHERE e.w = 9) RETURN x.name, size(r)
    , "x.name | size(r)\n\"d\" | 1");
    // Through WITH, and next to a named path over the same relationship.
    try expectResult(db,
        \\MATCH p = (:V {name: 'a'})-[r:R*]->(x {name: 'd'}) WITH p, r RETURN length(p), size(r), relationships(p) = r AS same
    , "length(p) | size(r) | same\n3 | 3 | true");
    // Incoming and undirected walks list the relationships in walk order.
    try expectResult(db,
        \\MATCH (:V {name: 'd'})<-[r:R*3]-(x) RETURN x.name, [e IN r | e.w] AS ws
    , "x.name | ws\n\"a\" | [3, 2, 1]");
    try expectResult(db,
        \\MATCH (:V {name: 'c'})-[r:R*2]-(x) RETURN x.name, [e IN r | e.w] AS ws ORDER BY x.name
    , "x.name | ws\n\"a\" | [2, 1]");
    // OPTIONAL MATCH that finds nothing leaves it null.
    try expectResult(db,
        \\MATCH (d:V {name: 'd'}) OPTIONAL MATCH (d)-[r:R*1..2]->(x) RETURN d.name, r
    , "d.name | r\n\"d\" | null");
}

test "lcy: a variable-length relationship's property map holds for every relationship" {
    const path = "/tmp/lcy_varlen_props.ltdb";
    const db = try openTestDb(path);
    defer closeTestDb(db, path);
    try seedChain(db);
    try run(db, "MATCH (c:V {name: 'c'}) CREATE (c)-[:R {w: 1}]->(:V {name: 'e'})");

    try expectResult(db,
        \\MATCH (:V {name: 'a'})-[:R*1..3 {w: 1}]->(x) RETURN x.name
    , "x.name\n\"b\"");
    try expectResult(db,
        \\MATCH (:V {name: 'b'})-[*1.. {w: 2}]->(x) RETURN x.name
    , "x.name\n\"c\"");
    // A parameter or an earlier variable can give the value.
    try expectResult(db,
        \\WITH 1 AS one MATCH (:V {name: 'c'})-[r:R* {w: one}]->(x) RETURN x.name, size(r)
    , "x.name | size(r)\n\"e\" | 1");
    // Zero hops has no relationship to fail the map.
    try expectResult(db,
        \\MATCH (:V {name: 'a'})-[:R*0..1 {w: 5}]->(x) RETURN x.name
    , "x.name\n\"a\"");
    // A null in the map matches nothing, as `{w: null}` does on a node.
    try expectResult(db,
        \\MATCH (:V {name: 'a'})-[:R*1..2 {w: null}]->(x) RETURN x.name
    , "x.name");
}

test "lcy: a variable-length relationship's variable is a list, not a relationship" {
    const path = "/tmp/lcy_varlen_errors.ltdb";
    const db = try openTestDb(path);
    defer closeTestDb(db, path);
    try seedChain(db);

    try expectFailure(db, "MATCH (a)-[r*1..2]->(b) DELETE r", .semantic, "DELETE needs a node or relationship; 'r' is a value");
    try expectFailure(db, "MATCH (a)-[r*1..2]->(b)-[r*1..2]->(c) RETURN r", .semantic, "Variable 'r' is already bound");
    try expectFailure(db, "MATCH (a)-[r*1..2]->(b) MATCH (b)-[r]->(c) RETURN r", .semantic, "Variable 'r' is already bound");
    try expectFailure(db, "MATCH (a)-[r*1..2]->(b) SET r.w = 1", .execution, "");
    try expectFailure(db, "CREATE (a)-[r:R*1..2]->(b)", .semantic, "Variable-length relationships are only supported in MATCH");
}

// ============================================================================
// RETURN * and WITH *
// ============================================================================

test "lcy: RETURN * returns every variable in scope, in alphabetical order" {
    const path = "/tmp/lcy_return_star.ltdb";
    const db = try openTestDb(path);
    defer closeTestDb(db, path);
    try seedChain(db);

    try expectResult(db,
        \\MATCH (x:V {name: 'a'})-[rel:S]->(b) RETURN *
    , "b | rel | x\nN | E | N");
    try expectResult(db,
        \\MATCH p = (x:V {name: 'a'})-[:S]->(b) UNWIND [1, 2] AS k RETURN * ORDER BY k
    , "b | k | p | x\nN | 1 | {nodes: [N, N], relationships: [E]} | N\nN | 2 | {nodes: [N, N], relationships: [E]} | N");
    // With more items after it, and DISTINCT.
    try expectResult(db,
        \\MATCH (x:V {name: 'a'}) RETURN *, x.name AS name
    , "x | name\nN | \"a\"");
    try expectResult(db,
        \\UNWIND [1, 1, 2] AS k RETURN DISTINCT * ORDER BY k
    , "k\n1\n2");
    // WITH * keeps every variable and adds the new ones; WITH narrows again.
    try expectResult(db,
        \\MATCH (x:V {name: 'a'}) WITH *, x.name AS name WITH *, size(name) AS len RETURN *
    , "len | name | x\n1 | \"a\" | N");
    try expectResult(db,
        \\MATCH (x:V {name: 'a'})-->(y) WITH y.name AS n RETURN * ORDER BY n
    , "n\n\"b\"\n\"d\"");
    try expectResult(db,
        \\MATCH (x:V {name: 'a'}) WITH * WHERE x.name = 'a' RETURN x.name
    , "x.name\n\"a\"");
    // Aggregating over it groups by every variable.
    try expectResult(db,
        \\UNWIND [1, 1, 2] AS k RETURN *, count(*) AS c ORDER BY k
    , "k | c\n1 | 2\n2 | 1");
    // OPTIONAL MATCH, CALL ... YIELD, UNWIND, CREATE and MERGE bring variables in.
    try expectResult(db,
        \\MATCH (x:V {name: 'd'}) OPTIONAL MATCH (x)-[o]->(y) RETURN *
    , "o | x | y\nnull | N | null");
    try expectResult(db,
        \\CALL db.labels() YIELD label AS l RETURN *
    , "l\n\"V\"");
    try expectResult(db,
        \\CREATE (c:Tmp {v: 1}) MERGE (m:Tmp {v: 2}) RETURN *
    , "c | m\nN | N");
    // Each query of a UNION has its own variables.
    try expectResult(db,
        \\UNWIND [1] AS a RETURN * UNION UNWIND [2] AS a RETURN *
    , "a\n1\n2");
    // FOREACH's variable stays inside it.
    try expectResult(db,
        \\UNWIND [1] AS a FOREACH (i IN [1] | CREATE (:Tmp)) RETURN *
    , "a\n1");
}

test "lcy: RETURN * with nothing in scope is an error" {
    const path = "/tmp/lcy_return_star_errors.ltdb";
    const db = try openTestDb(path);
    defer closeTestDb(db, path);

    try expectFailure(db, "RETURN *", .parse, "RETURN * is not allowed when there are no variables in scope");
    try expectFailure(db, "MATCH () RETURN *", .parse, "RETURN * is not allowed when there are no variables in scope");
    try expectFailure(db, "MATCH (n) WITH n.v AS v, 1 AS w WITH 1 AS z UNION RETURN *", .parse, "RETURN * is not allowed");
    try expectFailure(db, "WITH * RETURN 1", .parse, "WITH * is not allowed when there are no variables in scope");
    try expectFailure(db, "MATCH (n) RETURN n, *", .parse, "* must come first in RETURN or WITH");
    try expectFailure(db, "MATCH (n) RETURN *, n", .semantic, "Column `n` is returned twice; give one of them another name with AS");
    try expectFailure(db, "MATCH (n) RETURN n.v, n.v", .semantic, "Column `n.v` is returned twice");
    try expectFailure(db, "MATCH (n) RETURN n.v AS a, n.w AS a", .semantic, "Column `a` is returned twice");
    try expectFailure(db, "MATCH (n) WITH *, n RETURN 1", .semantic, "Variable 'n' is already defined");
    // With other items, an empty * is fine.
    try expectResult(db, "RETURN *, 1 AS one", "one\n1");
}

// ============================================================================
// Map projection
// ============================================================================

test "lcy: map projection of a node, a relationship and a map" {
    const path = "/tmp/lcy_map_projection.ltdb";
    const db = try openTestDb(path);
    defer closeTestDb(db, path);
    try run(db, "CREATE (:P {name: 'ann', age: 30})-[:K {since: 2020}]->(:P {name: 'bo'})");

    try expectResult(db,
        \\MATCH (n:P {name: 'ann'}) RETURN n {.name, .missing, k: 1 + 1}
    , "n {.name, .missing, k: 1 + 1}\n{name: \"ann\", missing: null, k: 2}");
    // .* is every property; a later entry replaces an earlier one in place.
    try expectResult(db,
        \\MATCH (n:P {name: 'ann'}) RETURN n {.*, name: 'zed', extra: true} AS m
    , "m\n{name: \"zed\", age: 30, extra: true}");
    try expectResult(db,
        \\MATCH (n:P {name: 'ann'}) RETURN n {name: 'first', .name} AS m
    , "m\n{name: \"ann\"}");
    // A variable stands for an entry of its own name.
    try expectResult(db,
        \\WITH 5 AS x MATCH (n:P {name: 'bo'}) RETURN n {.name, x} AS m
    , "m\n{name: \"bo\", x: 5}");
    try expectResult(db,
        \\MATCH (:P)-[r:K]->(b) RETURN r {.since, to: b.name} AS m
    , "m\n{since: 2020, to: \"bo\"}");
    try expectResult(db,
        \\WITH {a: 1, b: 2} AS m RETURN m {.b, .c} AS x, m {.*} AS y, m {} AS z
    , "x | y | z\n{b: 2, c: null} | {a: 1, b: 2} | {}");
    // A null subject gives null, not a map of nulls.
    try expectResult(db,
        \\MATCH (n:P {name: 'bo'}) OPTIONAL MATCH (n)-[:K]->(o) RETURN o {.name} AS m
    , "m\nnull");
    // Anywhere an expression is: aggregates, property access, ORDER BY,
    // comprehensions, and with keywords and backticks as names.
    try expectResult(db,
        \\MATCH (n:P) RETURN collect(n {.name}) AS ms
    , "ms\n[{name: \"ann\"}, {name: \"bo\"}]");
    try expectResult(db,
        \\MATCH (n:P) RETURN (n {.name}).name AS name ORDER BY n {.age}.age
    , "name\n\"ann\"\n\"bo\"");
    try expectResult(db,
        \\MATCH (n:P {name: 'ann'}) RETURN [k IN [1] | n {.age, k}] AS l, n {end: 1, `a b`: 2} AS q, `n` {.age} AS r
    , "l | q | r\n[{age: 30, k: 1}] | {end: 1, a b: 2} | {age: 30}");
    // The same projection twice is one grouping key.
    try expectResult(db,
        \\UNWIND [1, 2] AS i MATCH (n:P) RETURN n {.name} AS m, count(*) AS c, n.name AS k ORDER BY k
    , "m | c | k\n{name: \"ann\"} | 2 | \"ann\"\n{name: \"bo\"} | 2 | \"bo\"");
}

test "lcy: map projection errors" {
    const path = "/tmp/lcy_map_projection_errors.ltdb";
    const db = try openTestDb(path);
    defer closeTestDb(db, path);

    try expectFailure(db, "WITH 1 AS x RETURN x {.a}", .execution, "A map projection needs a node, relationship or map, but `x` is integer");
    try expectFailure(db, "RETURN q {.a}", .semantic, "Variable 'q' is not defined");
    try expectFailure(db, "WITH {} AS m RETURN m {.a, y}", .semantic, "Variable 'y' is not defined");
    try expectFailure(db, "WITH {} AS m RETURN m {.}", .parse, "Expected a property name or * after '.' in a map projection");
    try expectFailure(db, "WITH {} AS m RETURN m {.a", .parse, "Expected '}' to close the map projection");
    try expectFailure(db, "WITH {} AS m RETURN m {1}", .parse, "Expected .property, .*, key: value or a variable in a map projection");
    try expectFailure(db, "WITH {} AS m RETURN m {end}", .parse, "Expected ':' after a key in a map projection");
}

// ============================================================================
// shortestPath and allShortestPaths
// ============================================================================

/// a -> b -> c -> d, a -> x -> d, a -> y -> d, d -Q-> z, and a lone node w.
fn seedRoutes(db: *Database) !void {
    try run(db,
        \\CREATE (a:S {n: 'a'}), (b:S {n: 'b'}), (c:S {n: 'c'}), (d:S {n: 'd'}), (x:S {n: 'x'}), (y:S {n: 'y'}),
        \\       (z:S {n: 'z'}), (:S {n: 'w'}),
        \\       (a)-[:R]->(b), (b)-[:R]->(c), (c)-[:R]->(d), (a)-[:R]->(x), (x)-[:R]->(d), (a)-[:R]->(y), (y)-[:R]->(d),
        \\       (d)-[:Q]->(z)
    );
}

test "lcy: shortestPath finds one shortest path between bound ends" {
    const path = "/tmp/lcy_shortest.ltdb";
    const db = try openTestDb(path);
    defer closeTestDb(db, path);
    try seedRoutes(db);

    try expectResult(db,
        \\MATCH p = shortestPath((a:S {n: 'a'})-[:R*]->(d:S {n: 'd'})) RETURN length(p), [v IN nodes(p) | v.n][0], [v IN nodes(p) | v.n][2]
    , "length(p) | [v IN nodes(p) | v.n][0] | [v IN nodes(p) | v.n][2]\n2 | \"a\" | \"d\"");
    // allShortestPaths: both paths of length 2, not the longer one.
    try expectResult(db,
        \\MATCH p = allShortestPaths((a:S {n: 'a'})-[:R*]->(d:S {n: 'd'})) RETURN [v IN nodes(p) | v.n] AS route ORDER BY route
    , "route\n[\"a\", \"x\", \"d\"]\n[\"a\", \"y\", \"d\"]");
    // The relationship variable is the list of the path's relationships.
    try expectResult(db,
        \\MATCH shortestPath((:S {n: 'a'})-[r*]->(:S {n: 'z'})) RETURN [e IN r | type(e)] AS types
    , "types\n[\"R\", \"R\", \"Q\"]");
    // Incoming and undirected; the path still runs from the first end.
    try expectResult(db,
        \\MATCH p = shortestPath((d:S {n: 'd'})<-[:R*]-(a:S {n: 'a'})) RETURN length(p), [v IN nodes(p) | v.n][0] AS first
    , "length(p) | first\n2 | \"d\"");
    try expectResult(db,
        \\MATCH p = shortestPath((b:S {n: 'b'})-[*]-(z:S {n: 'z'})) RETURN [v IN nodes(p) | v.n] AS route
    , "route\n[\"b\", \"c\", \"d\", \"z\"]");
    // The upper bound: none within one hop; types restrict every hop.
    try expectResult(db,
        \\MATCH p = shortestPath((a:S {n: 'a'})-[*..1]->(d:S {n: 'd'})) RETURN length(p)
    , "length(p)");
    try expectResult(db,
        \\MATCH p = shortestPath((a:S {n: 'a'})-[*..2]->(d:S {n: 'd'})) RETURN length(p)
    , "length(p)\n2");
    try expectResult(db,
        \\MATCH p = shortestPath((a:S {n: 'a'})-[:Q*]->(d:S {n: 'z'})) RETURN length(p)
    , "length(p)");
    try expectResult(db,
        \\MATCH p = shortestPath((a:S {n: 'a'})-[:R|Q*]->(z:S {n: 'z'})) RETURN length(p)
    , "length(p)\n3");
    // A single hop without a quantifier.
    try expectResult(db,
        \\MATCH p = shortestPath((a:S {n: 'a'})-[:R]->(x:S {n: 'x'})) RETURN length(p)
    , "length(p)\n1");
    // From a node to itself: a path of no relationships when the minimum is 0.
    try expectResult(db,
        \\MATCH p = shortestPath((a:S {n: 'a'})-[*0..]->(a)) RETURN length(p), size(nodes(p))
    , "length(p) | size(nodes(p))\n0 | 1");
    try expectResult(db,
        \\MATCH p = shortestPath((a:S {n: 'a'})-[*]->(a)) RETURN length(p)
    , "length(p)");
    // Not connected: no row, or null after OPTIONAL MATCH.
    try expectResult(db,
        \\MATCH p = shortestPath((a:S {n: 'a'})-[*]-(w:S {n: 'w'})) RETURN length(p)
    , "length(p)");
    try expectResult(db,
        \\MATCH (a:S {n: 'a'}), (w:S {n: 'w'}) OPTIONAL MATCH p = shortestPath((a)-[*]-(w)) RETURN p
    , "p\nnull");
    // One search per pair of ends, and a WHERE on the ends.
    try expectResult(db,
        \\MATCH (s:S {n: 'a'}), (t:S) WHERE t.n IN ['c', 'd', 'z'] MATCH p = shortestPath((s)-[*]->(t)) RETURN t.n, length(p) ORDER BY t.n
    , "t.n | length(p)\n\"c\" | 2\n\"d\" | 2\n\"z\" | 3");
    try expectResult(db,
        \\MATCH p = shortestPath((s:S)-[*]->(t:S)) WHERE s.n = 'b' AND t.n = 'z' RETURN length(p)
    , "length(p)\n3");
}

test "lcy: shortestPath errors name what is not supported" {
    const path = "/tmp/lcy_shortest_errors.ltdb";
    const db = try openTestDb(path);
    defer closeTestDb(db, path);

    try expectFailure(db, "MATCH p = shortestPath((a)-[*]->(b)-[*]->(c)) RETURN p", .semantic, "shortestPath() needs one relationship between two nodes");
    try expectFailure(db, "MATCH p = shortestPath((a)) RETURN p", .semantic, "shortestPath() needs one relationship between two nodes");
    try expectFailure(db, "MATCH p = shortestPath((a)-[*2..]->(b)) RETURN p", .semantic, "shortestPath() finds paths of at least 0 or 1 relationships, not 2");
    try expectFailure(db, "MATCH p = shortestPath((a)-[* {w: 1}]->(b)) RETURN p", .semantic, "shortestPath() does not support a property map");
    try expectFailure(db, "MATCH p = shortestPath((a)-[*]->(b)) WHERE length(p) > 1 RETURN p", .semantic, "A WHERE that reads `p` of a shortestPath() is not supported");
    try expectFailure(db, "MATCH shortestPath((a)-[r*]->(b)) WHERE all(x IN r WHERE x.w > 1) RETURN a", .semantic, "A WHERE that reads `r` of a shortestPath()");
    try expectFailure(db, "CREATE p = shortestPath((a)-[:R]->(b))", .parse, "A named path in CREATE is not supported");
    try expectFailure(db, "CREATE shortestPath((a)-[:R]->(b))", .semantic, "shortestPath() and allShortestPaths() are only supported in MATCH");
    try expectFailure(db, "MATCH (a), (b) RETURN shortestPath((a)-[*]->(b))", .parse, "only supported as a pattern in MATCH");
    try expectFailure(db, "MATCH (a), (b) WHERE EXISTS { MATCH shortestPath((a)-[*]->(b)) } RETURN a", .semantic, "only supported in MATCH");
    try expectFailure(db, "MATCH (a)-->shortestPath((b)-->(c)) RETURN a", .parse, "take a whole pattern");
    try expectFailure(db, "MATCH p = shortestPath((a)-[*]->(b) RETURN p", .parse, "Expected ')' to close shortestPath()");
    // The name is not reserved.
    try expectResult(db, "WITH 1 AS shortestPath RETURN shortestPath", "shortestPath\n1");
}

// ============================================================================
// CALL { } subqueries
// ============================================================================

/// Two teams: red with ann (3 goals) and bo (5), blue with cy (2); and a
/// team with no players.
fn seedTeams(db: *Database) !void {
    try run(db,
        \\CREATE (r:Team {name: 'red'}), (b:Team {name: 'blue'}), (:Team {name: 'empty'}),
        \\       (:P {name: 'ann', goals: 3})-[:IN]->(r), (:P {name: 'bo', goals: 5})-[:IN]->(r),
        \\       (:P {name: 'cy', goals: 2})-[:IN]->(b)
    );
}

test "lcy: CALL { } runs a correlated subquery for each row" {
    const path = "/tmp/lcy_call_subquery.ltdb";
    const db = try openTestDb(path);
    defer closeTestDb(db, path);
    try seedTeams(db);

    // Aggregates per outer row: the reason to use it.
    try expectResult(db,
        \\MATCH (t:Team) CALL { WITH t MATCH (p:P)-[:IN]->(t) RETURN count(p) AS players, sum(p.goals) AS goals }
        \\RETURN t.name, players, goals ORDER BY t.name
    , "t.name | players | goals\n\"blue\" | 1 | 2\n\"empty\" | 0 | 0\n\"red\" | 2 | 8");
    // Top one per team, with ORDER BY and LIMIT inside; a team with no
    // row from the subquery is dropped.
    try expectResult(db,
        \\MATCH (t:Team) CALL { WITH t MATCH (p:P)-[:IN]->(t) RETURN p ORDER BY p.goals DESC LIMIT 1 }
        \\RETURN t.name, p.name ORDER BY t.name
    , "t.name | p.name\n\"blue\" | \"cy\"\n\"red\" | \"bo\"");
    // Several rows per outer row, and a returned node used afterwards.
    try expectResult(db,
        \\MATCH (t:Team {name: 'red'}) CALL { WITH t MATCH (p:P)-[:IN]->(t) RETURN p } MATCH (p)-[:IN]->(u) RETURN p.name, u.name ORDER BY p.name
    , "p.name | u.name\n\"ann\" | \"red\"\n\"bo\" | \"red\"");
    // Operators that read their whole input when opened start afresh for
    // each outer row: DISTINCT, a cross product, a sort.
    try expectResult(db,
        \\MATCH (t:Team) CALL { WITH t MATCH (p:P)-[:IN]->(t) RETURN DISTINCT 1 AS one } RETURN t.name, one ORDER BY t.name
    , "t.name | one\n\"blue\" | 1\n\"red\" | 1");
    try expectResult(db,
        \\MATCH (t:Team) CALL { WITH t MATCH (x:Team), (y:P) WHERE x = t RETURN count(y) AS c } RETURN t.name, c ORDER BY t.name
    , "t.name | c\n\"blue\" | 3\n\"empty\" | 3\n\"red\" | 3");
    // Uncorrelated: the same rows for every outer row.
    try expectResult(db,
        \\UNWIND [1, 2] AS i CALL { MATCH (p:P) RETURN max(p.goals) AS best } RETURN i, best
    , "i | best\n1 | 5\n2 | 5");
    // DISTINCT, WITH and WHERE inside, and the outer variables untouched.
    try expectResult(db,
        \\MATCH (t:Team {name: 'red'}) CALL { WITH t MATCH (p:P)-[:IN]->(t) WITH p.goals AS g WHERE g > 3 RETURN collect(DISTINCT g) AS big }
        \\RETURN t.name, big
    , "t.name | big\n\"red\" | [5]");
    // UNION inside: each query imports on its own.
    try expectResult(db,
        \\MATCH (t:Team {name: 'red'}) CALL { WITH t MATCH (p:P)-[:IN]->(t) RETURN p.name AS x UNION RETURN 'none' AS x UNION RETURN 'none' AS x }
        \\RETURN x ORDER BY x
    , "x\n\"ann\"\n\"bo\"\n\"none\"");
    try expectResult(db,
        \\CALL { RETURN 1 AS x UNION ALL RETURN 1 AS x } RETURN count(*) AS c
    , "c\n2");
    // Nested, and RETURN * on both sides.
    try expectResult(db,
        \\MATCH (t:Team {name: 'blue'}) CALL { WITH t CALL { WITH t MATCH (p)-[:IN]->(t) RETURN p.name AS n } RETURN n } RETURN *
    , "n | t\n\"cy\" | N");
    // An OPTIONAL MATCH inside keeps a row for the empty team.
    try expectResult(db,
        \\MATCH (t:Team) CALL { WITH t OPTIONAL MATCH (p:P)-[:IN]->(t) RETURN p.name AS n } RETURN t.name, n ORDER BY t.name, n
    , "t.name | n\n\"blue\" | \"cy\"\n\"empty\" | null\n\"red\" | \"ann\"\n\"red\" | \"bo\"");
}

test "lcy: CALL { } without RETURN runs for its writes" {
    const path = "/tmp/lcy_call_unit.ltdb";
    const db = try openTestDb(path);
    defer closeTestDb(db, path);
    try seedTeams(db);

    try expectResult(db,
        \\MATCH (t:Team) CALL { WITH t CREATE (:Audit {team: t.name}) } RETURN count(*) AS rows
    , "rows\n3");
    try expectResult(db, "MATCH (a:Audit) RETURN a.team ORDER BY a.team", "a.team\n\"blue\"\n\"empty\"\n\"red\"");
    try expectResult(db,
        \\MATCH (t:Team) CALL { WITH t MATCH (p:P)-[:IN]->(t) SET p.team = t.name } RETURN count(*) AS rows
    , "rows\n3");
    try expectResult(db, "MATCH (p:P) RETURN p.name, p.team ORDER BY p.name", "p.name | p.team\n\"ann\" | \"red\"\n\"bo\" | \"red\"\n\"cy\" | \"blue\"");
    // Writes that return.
    try expectResult(db,
        \\UNWIND [1, 2] AS i CALL { WITH i CREATE (n:Made {v: i}) RETURN n.v * 10 AS w } RETURN i, w ORDER BY i
    , "i | w\n1 | 10\n2 | 20");
}

test "lcy: CALL { } errors" {
    const path = "/tmp/lcy_call_errors.ltdb";
    const db = try openTestDb(path);
    defer closeTestDb(db, path);
    try seedTeams(db);

    try expectFailure(db, "MATCH (t:Team) CALL { MATCH (t) RETURN t.name AS n } RETURN n", .semantic, "Variable 't' is already defined outside CALL { ... }; import it with WITH t, or use another name");
    try expectFailure(db, "MATCH (t:Team) CALL { WITH t RETURN t } RETURN t", .semantic, "Variable 't' returned by CALL { ... } is already defined outside it; rename it with AS");
    try expectFailure(db, "MATCH (t:Team) CALL { WITH t RETURN t.name } RETURN 1", .semantic, "CALL { ... } must name what it returns: add AS to `t.name`");
    try expectFailure(db, "MATCH (t:Team) CALL { WITH t.name AS n RETURN n AS m } RETURN m", .semantic, "The WITH that starts CALL { ... } imports variables and can only list them");
    try expectFailure(db, "MATCH (t:Team) CALL { WITH t WHERE t.name = 'red' RETURN 1 AS one } RETURN one", .semantic, "imports variables and can only list them");
    try expectFailure(db, "CALL { WITH q RETURN 1 AS one } RETURN one", .semantic, "Variable 'q' is not defined");
    try expectFailure(db, "MATCH (t:Team) CALL { RETURN t.name AS n } RETURN n", .semantic, "Variable 't' is not defined");
    try expectFailure(db, "CALL { RETURN 1 AS x UNION CREATE (:N) } RETURN x", .semantic, "must end with RETURN, or none");
    try expectFailure(db, "CALL { RETURN 1 AS x UNION RETURN 2 AS y } RETURN 1", .plan, "All queries in a UNION must return the same column names");
    try expectFailure(db, "UNWIND [1] AS x CALL { WITH x CREATE (:N) } IN TRANSACTIONS RETURN x", .parse, "CALL { ... } IN TRANSACTIONS is not supported");
    try expectFailure(db, "CALL { } RETURN 1", .parse, "CALL { } needs a query inside the braces");
    try expectFailure(db, "CALL { RETURN 1 AS x", .parse, "Expected '}' to close CALL { ... }");
    // The words stay names.
    try expectResult(db, "WITH 1 AS call, 2 AS transactions RETURN call + transactions AS s", "s\n3");
}

// ============================================================================
// Label expressions and relationship type tests
// ============================================================================

fn seedLabels(db: *Database) !void {
    try run(db, "CREATE (a:A:B {k: 1}), (b:A {k: 2}), (c:B {k: 3}), (d {k: 4}), (e:C {k: 5})");
    try run(db, "MATCH (a {k: 1}), (b {k: 2}), (c {k: 3}) CREATE (a)-[:R]->(b), (b)-[:S]->(c), (c)-[:R]->(a)");
}

test "lcy: label expressions in node patterns" {
    const path = "/tmp/lcy_label_expressions.ltdb";
    const db = try openTestDb(path);
    defer closeTestDb(db, path);
    try seedLabels(db);

    try expectResult(db, "MATCH (n:A|B) RETURN n.k ORDER BY n.k", "n.k\n1\n2\n3");
    try expectResult(db, "MATCH (n:A|:C) RETURN n.k ORDER BY n.k", "n.k\n1\n2\n5");
    try expectResult(db, "MATCH (n:A&B) RETURN n.k", "n.k\n1");
    try expectResult(db, "MATCH (n:!A) RETURN n.k ORDER BY n.k", "n.k\n3\n4\n5");
    try expectResult(db, "MATCH (n:A&!B) RETURN n.k", "n.k\n2");
    try expectResult(db, "MATCH (n:%) RETURN count(n) AS c", "c\n4");
    try expectResult(db, "MATCH (n:!%) RETURN n.k", "n.k\n4");
    try expectResult(db, "MATCH (n:(A|B)&!(A&B)) RETURN n.k ORDER BY n.k", "n.k\n2\n3");
    // `!` binds tighter than `&`, and `&` than `|`.
    try expectResult(db, "MATCH (n:!A&B|C) RETURN n.k ORDER BY n.k", "n.k\n3\n5");
    // A label no node has.
    try expectResult(db, "MATCH (n:Nope|C) RETURN n.k", "n.k\n5");
    try expectResult(db, "MATCH (n:!Nope) RETURN count(n) AS c", "c\n5");
    // Anonymous nodes, ends of relationships, with properties, and OPTIONAL MATCH.
    try expectResult(db, "MATCH (:A|C)-[r]->(:!A) RETURN type(r) AS t", "t\n\"S\"");
    try expectResult(db, "MATCH (a)-->(b:B|C {k: 3}) RETURN a.k", "a.k\n2");
    try expectResult(db, "MATCH (n {k: 4}) OPTIONAL MATCH (n)-->(m:A|B) RETURN m", "m\nnull");
    try expectResult(db, "MATCH (a {k: 2}) MATCH (a:!B) RETURN a.k", "a.k\n2");
    try expectResult(db, "MATCH (a {k: 1}) MATCH (a:!B) RETURN a.k", "a.k");
    // In EXISTS and pattern comprehensions.
    try expectResult(db, "MATCH (n) WHERE EXISTS { (n)-->(:A|C) } RETURN n.k ORDER BY n.k", "n.k\n1\n3");
    try expectResult(db, "MATCH (n {k: 2}) RETURN [(n)--(m:!A) | m.k] AS ks", "ks\n[3]");
    // CREATE and MERGE give the labels they name: `&` is every one of them.
    try expectResult(db, "CREATE (x:X&Y {k: 9}) RETURN labels(x) AS l", "l\n[\"X\", \"Y\"]");
    try expectResult(db, "MERGE (x:X&Y {k: 9}) RETURN x.k", "x.k\n9");
    try expectFailure(db, "CREATE (:A|B)", .semantic, "CREATE gives a node the labels it names, as in (n:A:B) or (n:A&B)");
    try expectFailure(db, "MERGE (:!A {k: 1})", .semantic, "MERGE gives a node the labels it names");
    try expectFailure(db, "MATCH (a {k: 1}) CREATE (a)-[:R]->(:%)", .semantic, "CREATE gives a node the labels it names");
}

test "lcy: label and relationship type tests in expressions" {
    const path = "/tmp/lcy_label_tests.ltdb";
    const db = try openTestDb(path);
    defer closeTestDb(db, path);
    try seedLabels(db);

    try expectResult(db, "MATCH (n) RETURN n.k, n:A|B AS ab, n:!A AS na ORDER BY n.k", "n.k | ab | na\n1 | true | false\n2 | true | false\n3 | true | true\n4 | false | true\n5 | false | true");
    try expectResult(db, "MATCH (n) WHERE n:A&!B OR n:C RETURN n.k ORDER BY n.k", "n.k\n2\n5");
    try expectResult(db, "MATCH (n) WHERE n:% RETURN count(*) AS c", "c\n4");
    // A relationship's type: it has one, so `r:R:S` and `r:R&S` are false.
    try expectResult(db, "MATCH ()-[r]->() RETURN type(r) AS t, r:R AS isr, r:R|S AS rs, r:!R AS notr, r:% AS any, r:R:S AS both ORDER BY t", "t | isr | rs | notr | any | both\n\"R\" | true | true | false | true | false\n\"R\" | true | true | false | true | false\n\"S\" | false | true | true | true | false");
    try expectResult(db, "MATCH (a)-[r]->(b) WHERE r:S RETURN a.k, b.k", "a.k | b.k\n2 | 3");
    try expectResult(db, "MATCH (a)-[r*2]->(b) WHERE all(x IN r WHERE x:R) RETURN a.k, b.k", "a.k | b.k\n3 | 2");
    try expectResult(db, "MATCH (a)-[r*2]->(b) WHERE any(x IN r WHERE x:S) RETURN a.k, b.k ORDER BY a.k", "a.k | b.k\n1 | 3\n2 | 1");
    // Null gives null; any other value is a type error.
    try expectResult(db, "MATCH (n {k: 4}) OPTIONAL MATCH (n)-[r]->(m) RETURN m:A AS a, r:R AS b", "a | b\nnull | null");
    try expectFailure(db, "WITH 1 AS x RETURN x:A AS y", .execution, "A label test needs a node or relationship, but this is integer");
    // In a comprehension a '|' ends the condition; parentheses keep it.
    try expectResult(db, "MATCH (n) RETURN [x IN collect(n) WHERE x:A | x.k] AS ks", "ks\n[1, 2]");
    try expectResult(db, "MATCH (n) RETURN [x IN collect(n) WHERE x:(A|C) | x.k] AS ks", "ks\n[1, 2, 5]");
    try expectResult(db, "MATCH (n) RETURN [x IN collect(n) WHERE x:A&!B | x.k] AS ks", "ks\n[2]");
    try expectResult(db, "MATCH (n) RETURN [x IN collect(n) WHERE coalesce(x:A|C, false) | x.k] AS ks", "ks\n[1, 2, 5]");
    try expectResult(db, "MATCH (n) RETURN [x IN collect(n) | x:A|C] AS ls", "ls\n[true, true, false, false, true]");
    try expectResult(db, "MATCH (n) WITH collect(n) AS ns RETURN reduce(c = 0, x IN ns | c + CASE WHEN x:A|B THEN 1 ELSE 0 END) AS c", "c\n3");
    // Grouping on a label test.
    try expectResult(db, "MATCH (n) RETURN n:A|B AS ab, count(*) AS c ORDER BY ab", "ab | c\nfalse | 2\ntrue | 3");
}

test "lcy: label expression errors" {
    const path = "/tmp/lcy_label_expression_errors.ltdb";
    const db = try openTestDb(path);
    defer closeTestDb(db, path);

    try expectFailure(db, "MATCH (n:A|) RETURN n", .parse, "Expected a label after ':'");
    try expectFailure(db, "MATCH (n:(A|B RETURN n", .parse, "Expected ')' to close the label expression");
    try expectFailure(db, "MATCH (n:!) RETURN n", .parse, "Expected a label after ':'");
    try expectFailure(db, "MATCH (n:A:B|C) RETURN n", .parse, "A label expression cannot follow :A:B; write it with &, as in :A&B|C");
    try expectFailure(db, "MATCH (n:A:!B) RETURN n", .parse, "A label expression cannot follow :A:B; write it with &, as in :A&!B");
    try expectFailure(db, "MATCH (n) WHERE n:A:B&C RETURN n", .parse, "A label expression cannot follow :A:B");
    try expectFailure(db, "MATCH (a)-[:!R]->(b) RETURN a", .parse, "A relationship's types take only '|', as in -[:A|B]->");
    try expectFailure(db, "MATCH (a)-[:R&S]->(b) RETURN a", .parse, "A relationship's types take only '|'");
    try expectFailure(db, "MATCH (a)-[:R|%]->(b) RETURN a", .parse, "A relationship's types take only '|'");
}

// ============================================================================
// Whole queries in EXISTS { }, COUNT { } and COLLECT { }
// ============================================================================

/// a -> b, a -> c, b -> c, with names.
fn seedTriangle(db: *Database) !void {
    try run(db, "CREATE (a:P {n: 'a'}), (b:P {n: 'b'}), (c:P {n: 'c'}), (a)-[:K]->(b), (a)-[:K]->(c), (b)-[:K]->(c)");
}

test "lcy: EXISTS, COUNT and COLLECT take a whole query" {
    const path = "/tmp/lcy_full_subqueries.ltdb";
    const db = try openTestDb(path);
    defer closeTestDb(db, path);
    try seedTriangle(db);

    // RETURN, WITH, aggregation, ORDER BY and LIMIT inside, per row.
    try expectResult(db, "MATCH (p:P) RETURN p.n, COUNT { MATCH (p)-->(m) RETURN m } AS c ORDER BY p.n", "p.n | c\n\"a\" | 2\n\"b\" | 1\n\"c\" | 0");
    try expectResult(db, "MATCH (p:P) WHERE EXISTS { MATCH (p)-->(m) WITH m WHERE m.n = 'c' RETURN m } RETURN p.n ORDER BY p.n", "p.n\n\"a\"\n\"b\"");
    try expectResult(db, "MATCH (p:P) RETURN p.n, COLLECT { MATCH (p)-->(m) RETURN m.n ORDER BY m.n DESC } AS ms ORDER BY p.n", "p.n | ms\n\"a\" | [\"c\", \"b\"]\n\"b\" | [\"c\"]\n\"c\" | []");
    try expectResult(db, "MATCH (p:P) RETURN p.n, COLLECT { MATCH (p)-->(m) RETURN m.n ORDER BY m.n LIMIT 1 } AS first ORDER BY p.n", "p.n | first\n\"a\" | [\"b\"]\n\"b\" | [\"c\"]\n\"c\" | []");
    try expectResult(db, "MATCH (p:P) RETURN p.n, COLLECT { MATCH (p)-->(m) WITH count(m) AS k RETURN k } AS ks ORDER BY p.n", "p.n | ks\n\"a\" | [2]\n\"b\" | [1]\n\"c\" | [0]");
    try expectResult(db, "MATCH (p:P) RETURN p.n, COUNT { MATCH (p)-[:K]-(m) WHERE m.n <> 'a' } AS c ORDER BY p.n", "p.n | c\n\"a\" | 2\n\"b\" | 1\n\"c\" | 1");
    // OPTIONAL MATCH and UNWIND inside; a query without MATCH.
    try expectResult(db, "MATCH (p:P {n: 'c'}) RETURN COUNT { OPTIONAL MATCH (p)-->(m) RETURN m } AS c", "c\n1");
    try expectResult(db, "MATCH (p:P {n: 'a'}) RETURN COLLECT { UNWIND [1, 2] AS x RETURN x * 2 } AS xs", "xs\n[2, 4]");
    // UNION and UNION ALL.
    try expectResult(db, "MATCH (p:P) RETURN p.n, COUNT { MATCH (p)-->(m) RETURN m.n AS x UNION MATCH (p)<--(m) RETURN m.n AS x } AS c ORDER BY p.n", "p.n | c\n\"a\" | 2\n\"b\" | 2\n\"c\" | 2");
    try expectResult(db, "MATCH (p:P {n: 'b'}) RETURN COLLECT { MATCH (p)-->(m) RETURN m.n AS x UNION ALL MATCH (p)<--(m) RETURN m.n AS x } AS c", "c\n[\"c\", \"a\"]");
    // Nested, and in a WITH's WHERE.
    try expectResult(db, "MATCH (p:P) RETURN p.n, COLLECT { MATCH (p)-->(m) RETURN COLLECT { MATCH (m)-->(o) RETURN o.n } AS x } AS c ORDER BY p.n", "p.n | c\n\"a\" | [[\"c\"], []]\n\"b\" | [[]]\n\"c\" | []");
    try expectResult(db, "MATCH (p:P) WITH p, COUNT { MATCH (p)-->(m) RETURN m } AS c WHERE c > 0 RETURN p.n, c ORDER BY p.n", "p.n | c\n\"a\" | 2\n\"b\" | 1");
    // Beside aggregation, when its outer variable is a grouping key.
    try expectResult(db, "MATCH (p:P)-->() RETURN p.n, count(*) AS out, COLLECT { MATCH (p)<--(q) RETURN q.n } AS ins ORDER BY p.n", "p.n | out | ins\n\"a\" | 2 | []\n\"b\" | 1 | [\"a\"]");
    // Without outer variables.
    try expectResult(db, "RETURN COLLECT { MATCH (n:P) RETURN n.n ORDER BY n.n } AS c", "c\n[\"a\", \"b\", \"c\"]");
    // The short form is unchanged, with or without MATCH.
    try expectResult(db, "MATCH (p:P) RETURN p.n, EXISTS { MATCH (p)-->() } AS e, COUNT { (p)-->() } AS c ORDER BY p.n", "p.n | e | c\n\"a\" | true | 2\n\"b\" | true | 1\n\"c\" | false | 0");
    try expectResult(db, "MATCH (p:P {n: 'a'}) RETURN EXISTS { path = (p)-->(:P {n: 'c'}) } AS e", "e\ntrue");
    // What it binds stays inside.
    try expectFailure(db, "MATCH (p:P) WHERE COUNT { MATCH (p)-->(m) RETURN m } > 0 RETURN m", .semantic, "Variable 'm' is not defined");
}

test "lcy: EXISTS, COUNT and COLLECT errors" {
    const path = "/tmp/lcy_full_subquery_errors.ltdb";
    const db = try openTestDb(path);
    defer closeTestDb(db, path);
    try seedTriangle(db);

    try expectFailure(db, "MATCH (p:P) RETURN COLLECT { MATCH (p)-->(m) RETURN m.n, m } AS x", .semantic, "COLLECT { ... } must end with a RETURN of one column, the values it collects");
    try expectFailure(db, "MATCH (p:P) RETURN COLLECT { MATCH (p)-->(m) } AS x", .semantic, "COLLECT { ... } must end with a RETURN of one column");
    try expectFailure(db, "MATCH (p:P) RETURN COUNT { MATCH (p)-->(m) CREATE (:X) } AS x", .semantic, "COUNT { ... } cannot change the database; use CALL { ... } to write for each row");
    try expectFailure(db, "MATCH (p:P) WHERE EXISTS { MATCH (p) SET p.x = 1 } RETURN p", .semantic, "EXISTS { ... } cannot change the database");
    try expectFailure(db, "MATCH (p:P) RETURN COUNT { MATCH (p)-->(m) WITH m MATCH (p) RETURN p } AS x", .semantic, "Variable 'p' is already defined outside COUNT { ... }; use another name");
    try expectFailure(db, "MATCH (p:P) RETURN COUNT { MATCH (p)-->(m) RETURN m UNION MATCH (p)<--(m) } AS x", .semantic, "Every query in a UNION inside COUNT { ... } must end with RETURN, or none");
    try expectFailure(db, "MATCH (p:P) RETURN COUNT { MATCH (p)-->(m) RETURN m.n AS a UNION MATCH (p)<--(m) RETURN m.n AS b } AS x", .plan, "All queries in a UNION must return the same column names");
    try expectFailure(db, "RETURN COUNT { } AS x", .parse, "COUNT { } needs a query inside the braces");
    try expectFailure(db, "RETURN COLLECT { MATCH (n) RETURN n AS x", .parse, "Expected '}' to close COLLECT { ... }");
    try expectFailure(db, "MATCH (p) WHERE EXISTS { (p)-->(q) RETURN q } RETURN p", .parse, "a subquery with more clauses starts with MATCH");
    // collect() and COLLECT stay apart, and COLLECT is still a name.
    try expectResult(db, "MATCH (p:P) RETURN collect(p.n) AS c", "c\n[\"a\", \"b\", \"c\"]");
    try expectResult(db, "WITH 1 AS collect RETURN collect", "collect\n1");
}

test "lcy: search predicates inside EXISTS, COUNT and COLLECT" {
    const path = "/tmp/lcy_search_subqueries.ltdb";
    const db = try openSearchDb(path);
    defer closeSearchDb(db, path);
    var params = Params.init(std.testing.allocator);
    defer params.deinit();
    const origin = [_]f32{ 0, 1 };
    const five = [_]f32{ 5, 1 };
    try params.put("q", .{ .vector_val = &origin });
    try params.put("r", .{ .vector_val = &five });

    // A whole query searches as its MATCH would alone, for each row: the
    // right documents, scored, with its own ORDER BY and LIMIT.
    try expectResult(db, "MATCH (p:Person) RETURN p.name, COLLECT { MATCH (p)-[:WROTE]->(d:Doc) WHERE d.text @@ 'rare' RETURN d.i ORDER BY d.i } AS ds ORDER BY p.name", "p.name | ds\n\"A\" | [0, 6]\n\"B\" | [0]");
    try expectResult(db, "MATCH (p:Person) RETURN p.name, COLLECT { MATCH (p)-[:WROTE]->(d:Doc) WHERE d.text @@ 'rare' RETURN score(d) > 0 } AS scored ORDER BY p.name", "p.name | scored\n\"A\" | [true, true]\n\"B\" | [true]");
    try expectResult(db, "MATCH (p:Person) RETURN p.name, COUNT { MATCH (d:Doc) WHERE d.text @@ 'rare' RETURN d } AS c ORDER BY p.name", "p.name | c\n\"A\" | 4\n\"B\" | 4");
    try expectResultWith(db, &params, "MATCH (p:Person) RETURN p.name, COLLECT { MATCH (p)-[:WROTE]->(d:Doc) WHERE d.embedding <=> $r RETURN d.i LIMIT 1 } AS nearest ORDER BY p.name", "p.name | nearest\n\"A\" | [6]\n\"B\" | [5]");
    try expectResultWith(db, &params, "MATCH (p:Person) WHERE EXISTS { MATCH (p)-[:WROTE]->(d:Doc) WHERE d.embedding <=> $q < 0.1 RETURN d } RETURN p.name ORDER BY p.name", "p.name\n\"A\"\n\"B\"");
    // The short form's WHERE is checked row by row: no score, and a bare
    // <=> is refused, as before.
    try expectResult(db, "MATCH (p:Person) RETURN p.name, COUNT { MATCH (p)-[:WROTE]->(d:Doc) WHERE d.text @@ 'rare' } AS c ORDER BY p.name", "p.name | c\n\"A\" | 2\n\"B\" | 1");
    try expectFailureWith(db, &params, "MATCH (p:Person) WHERE EXISTS { MATCH (p)-[:WROTE]->(d:Doc) WHERE d.embedding <=> $q } RETURN p", .plan, "A WHERE of only `<=>` ranks the rows of a MATCH by distance, which a subquery or pattern comprehension cannot do");
    // A WITH's WHERE inside is a WITH's: no score, bare <=> refused.
    try expectResult(db, "MATCH (p:Person {name: 'A'}) RETURN COLLECT { MATCH (p)-[:WROTE]->(d:Doc) WITH d WHERE d.text @@ 'rare' RETURN score(d) } AS s", "s\n[null, null]");
    try expectFailureWith(db, &params, "MATCH (p:Person) RETURN COUNT { MATCH (p)-[:WROTE]->(d:Doc) WITH d WHERE d.embedding <=> $q RETURN d } AS c", .plan, "which WITH cannot do");
}

// ============================================================================
// Search predicates where phase 2 adds a WHERE: CALL { } and WITH *
// ============================================================================

/// A full-text and vector database: Docs 0..11 with text "rare common" for
/// every third and "common" otherwise, and the vector [i, 1]; Person A
/// wrote the even ones, B every fifth.
fn openSearchDb(comptime path: []const u8) !*Database {
    @import("compat").fs.cwd().deleteFile(path) catch {};
    @import("compat").fs.cwd().deleteFile(path ++ "-wal") catch {};
    const db = try Database.open(std.testing.allocator, path, .{
        .create = true,
        .config = .{ .enable_fts = true, .enable_vector = true, .vector_dimensions = 2 },
    });
    errdefer db.close();
    try db.createNodeFtsIndex("Doc", "text");
    var txn = try db.beginTransaction(.read_write);
    errdefer db.abortTransaction(&txn) catch {};
    var people: [2]u64 = undefined;
    for (&people, [_][]const u8{ "A", "B" }) |*p, name| {
        p.* = try db.createNode(&txn, &.{"Person"});
        try db.setNodeProperty(&txn, p.*, "name", .{ .string_val = name });
    }
    for (0..12) |i| {
        const d = try db.createNode(&txn, &.{"Doc"});
        try db.setNodeProperty(&txn, d, "i", .{ .int_val = @intCast(i) });
        try db.setNodeProperty(&txn, d, "text", .{ .string_val = if (i % 3 == 0) "rare common" else "common" });
        try db.setNodeVectorInTxn(&txn, d, &[_]f32{ @floatFromInt(i), 1 });
        if (i % 2 == 0) try db.createEdge(&txn, people[0], d, "WROTE");
        if (i % 5 == 0) try db.createEdge(&txn, people[1], d, "WROTE");
    }
    try db.commitTransaction(&txn);
    return db;
}

fn closeSearchDb(db: *Database, comptime path: []const u8) void {
    db.close();
    @import("compat").fs.cwd().deleteFile(path) catch {};
    @import("compat").fs.cwd().deleteFile(path ++ "-wal") catch {};
}

test "lcy: search predicates inside CALL { } and after WITH *" {
    const path = "/tmp/lcy_search_constructs.ltdb";
    const db = try openSearchDb(path);
    defer closeSearchDb(db, path);
    var params = Params.init(std.testing.allocator);
    defer params.deinit();
    const origin = [_]f32{ 0, 1 };
    const five = [_]f32{ 5, 1 };
    try params.put("q", .{ .vector_val = &origin });
    try params.put("r", .{ .vector_val = &five });

    // The plain search, for its score.
    var plain = try db.query("MATCH (d:Doc) WHERE d.text @@ 'rare' RETURN score(d) AS s LIMIT 1");
    defer plain.deinit();
    const s = plain.rows[0].values[0].float_val;
    try std.testing.expect(s > 0);
    var buf: [512]u8 = undefined;

    // CALL { } runs its MATCH as that MATCH would run alone, for each
    // incoming row: the same documents, scored.
    var w: std.Io.Writer = .fixed(&buf);
    try w.writeAll("p.name | i | scored");
    for ([_][]const u8{ "A", "B" }) |name| {
        for ([_]i64{ 0, 3, 6, 9 }) |i| try w.print("\n\"{s}\" | {d} | true", .{ name, i });
    }
    try expectResult(db, "MATCH (p:Person) CALL { MATCH (d:Doc) WHERE d.text @@ 'rare' RETURN d.i AS i, score(d) AS s } RETURN p.name, i, s > 0 AS scored ORDER BY p.name, i", w.buffered());
    // Correlated through a relationship: each person's own documents, still
    // scored, and a LIMIT inside bounds each run.
    try expectResult(db, "MATCH (p:Person) CALL { WITH p MATCH (p)-[:WROTE]->(d:Doc) WHERE d.text @@ 'rare' RETURN d.i AS i, score(d) > 0 AS scored } RETURN p.name, i, scored ORDER BY p.name, i", "p.name | i | scored\n\"A\" | 0 | true\n\"A\" | 6 | true\n\"B\" | 0 | true");
    // A bare <=> is a search per incoming row: each person's nearest to [5, 1].
    try expectResultWith(db, &params, "MATCH (p:Person) CALL { WITH p MATCH (p)-[:WROTE]->(d:Doc) WHERE d.embedding <=> $r RETURN d.i AS i LIMIT 1 } RETURN p.name, i ORDER BY p.name", "p.name | i\n\"A\" | 6\n\"B\" | 5");
    try expectResultWith(db, &params, "MATCH (p:Person) CALL { WITH p MATCH (p)-[:WROTE]->(d:Doc) WHERE d.embedding <=> $r RETURN d.i AS i } RETURN p.name, collect(i) AS near ORDER BY p.name", "p.name | near\n\"A\" | [6, 4, 8, 10, 2, 0]\n\"B\" | [5, 10, 0]");
    // A bound: only [0, 1] itself is that close to [0, 1].
    try expectResultWith(db, &params, "MATCH (p:Person) CALL { WITH p MATCH (p)-[:WROTE]->(d:Doc) WHERE d.embedding <=> $q < 0.1 RETURN d.i AS i } RETURN p.name, i ORDER BY p.name", "p.name | i\n\"A\" | 0\n\"B\" | 0");

    // WITH * WHERE is a WITH's WHERE: checked row by row, so the right
    // documents and no score, and a bare <=> there is refused.
    try expectResult(db, "MATCH (d:Doc) WITH * WHERE d.text @@ 'rare' RETURN d.i, score(d) AS s ORDER BY d.i", "d.i | s\n0 | null\n3 | null\n6 | null\n9 | null");
    try expectFailureWith(db, &params, "MATCH (d:Doc) WITH * WHERE d.embedding <=> $q RETURN d.i", .plan, "A WHERE of only `<=>` ranks the rows of a MATCH by distance, which WITH cannot do");
    try expectFailureWith(db, &params, "MATCH (p:Person) CALL { WITH p MATCH (p)-[:WROTE]->(d:Doc) WITH * WHERE d.embedding <=> $q RETURN d.i AS i } RETURN i", .plan, "which WITH cannot do");
    try expectResultWith(db, &params, "MATCH (d:Doc) WITH * WHERE d.embedding <=> $q < 0.1 RETURN d.i", "d.i\n0");
    // A score passes through WITH * as through WITH d.
    try expectResult(db, "MATCH (d:Doc) WHERE d.text @@ 'rare' WITH * RETURN d.i, score(d) > 0 AS scored ORDER BY d.i", "d.i | scored\n0 | true\n3 | true\n6 | true\n9 | true");
}

// ============================================================================
// Planning errors that say what is wrong, and what they used to refuse
// ============================================================================

test "lcy: ORDER BY after aggregation takes expressions over returned columns" {
    const path = "/tmp/lcy_order_after_aggregation.ltdb";
    const db = try openTestDb(path);
    defer closeTestDb(db, path);
    try seedTriangle(db);

    try expectResult(db, "MATCH (p:P)-->(q) RETURN p AS m, count(q) AS c ORDER BY m.n DESC", "m | c\nN | 1\nN | 2");
    try expectResult(db, "MATCH (p:P)-->(q) RETURN p.n AS nm, count(q) AS c ORDER BY toUpper(nm) DESC", "nm | c\n\"b\" | 1\n\"a\" | 2");
    try expectResult(db, "MATCH (p:P)-->(q) RETURN p.n AS nm, count(q) AS c ORDER BY c * -1, nm", "nm | c\n\"a\" | 2\n\"b\" | 1");
    try expectResult(db, "MATCH (p:P)-->(q) RETURN p.n, count(q) AS c ORDER BY p.n DESC", "p.n | c\n\"b\" | 1\n\"a\" | 2");
    try expectResult(db, "MATCH (p:P)-->(q) RETURN p, count(q) AS c ORDER BY p.n DESC", "p | c\nN | 1\nN | 2");
    // Before aggregation too: an alias inside an expression.
    try expectResult(db, "MATCH (p:P) RETURN p.n AS nm ORDER BY toUpper(nm) DESC", "nm\n\"c\"\n\"b\"\n\"a\"");
    try expectResult(db, "MATCH (p:P) RETURN DISTINCT p.n AS nm ORDER BY toUpper(nm) DESC LIMIT 1", "nm\n\"c\"");
    // What the RETURN does not keep has no value after grouping.
    try expectFailure(db, "MATCH (p:P) RETURN count(p) AS c ORDER BY p.n", .plan, "ORDER BY item 1 uses `p`, which the aggregating RETURN does not return; return it, or order by a returned column");
    try expectFailure(db, "MATCH (p:P) RETURN p.n AS nm, count(*) AS c ORDER BY nm, sum(p.v)", .plan, "ORDER BY item 2 aggregates, but the RETURN does not return that aggregate");
}

test "lcy: a subquery beside an aggregate reads grouping keys" {
    const path = "/tmp/lcy_subquery_beside_aggregate.ltdb";
    const db = try openTestDb(path);
    defer closeTestDb(db, path);
    try seedTriangle(db);

    try expectResult(db, "MATCH (p:P)-->(q) RETURN p.n AS nm, p, count(q) + COUNT { (p)<--() } AS c ORDER BY nm", "nm | p | c\n\"a\" | N | 2\n\"b\" | N | 2");
    try expectResult(db, "MATCH (p:P)-->(q) RETURN p, collect(q.n) + COLLECT { MATCH (p)<--(r) RETURN r.n } AS c ORDER BY c[0]", "p | c\nN | [\"b\", \"c\"]\nN | [\"c\", \"a\"]");
    // A grouping key that is a property does not make its variable one.
    try expectFailure(db, "MATCH (p:P) RETURN p.n, count(p) + COUNT { (p)-->() } AS c", .plan, "RETURN uses `p` in COUNT { ... } beside an aggregate, but `p` is not a grouping key");
    try expectFailure(db, "MATCH (p:P) RETURN p.n, count(*) * size([(p)-->(x) | x]) AS c", .plan, "in a pattern comprehension beside an aggregate");
}

test "lcy: aggregates where they cannot aggregate, and their argument counts" {
    const path = "/tmp/lcy_misplaced_aggregates.ltdb";
    const db = try openTestDb(path);
    defer closeTestDb(db, path);
    try seedTriangle(db);

    // These failed at run time as an unknown function, or not at all over
    // an empty graph.
    try expectFailure(db, "MATCH (p:P) FOREACH (k IN collect(p.n) | CREATE (:Q {k: k}))", .semantic, "collect() aggregates rows, which FOREACH cannot do; aggregate in a WITH first");
    try expectFailure(db, "MATCH (p:P) WHERE count(p) > 1 RETURN p", .semantic, "count() aggregates rows, which WHERE cannot do");
    try expectFailure(db, "MATCH (p:P) WITH p, count(*) AS c WHERE count(*) > 1 RETURN p", .semantic, "which WHERE cannot do");
    try expectFailure(db, "MATCH (p:P) SET p.c = count(*)", .semantic, "which SET cannot do");
    try expectFailure(db, "UNWIND collect(1) AS x RETURN x", .semantic, "which UNWIND cannot do");
    try expectFailure(db, "MATCH (p:P {n: max(1)}) RETURN p", .semantic, "which MATCH cannot do");
    try expectFailure(db, "RETURN 1 LIMIT count(*)", .semantic, "which LIMIT cannot do");
    // Where they can.
    try expectResult(db, "MATCH (p:P) WITH collect(p.n) AS ns FOREACH (k IN ns | CREATE (:Q {k: k})) RETURN size(ns) AS s", "s\n3");
    try expectResult(db, "MATCH (p:P) RETURN [x IN collect(p.n) WHERE x <> 'a'] AS l", "l\n[\"b\", \"c\"]");
    // Argument counts, found before the query runs.
    try expectFailure(db, "RETURN count(1, 2)", .semantic, "count takes one argument or *, but was given 2 arguments");
    try expectFailure(db, "RETURN sum()", .semantic, "sum takes one argument, but was given 0 arguments");
    try expectFailure(db, "RETURN collect(1, 2)", .semantic, "collect takes one argument, but was given 2 arguments");
}

test "lcy: a pattern is a test, not a value" {
    const path = "/tmp/lcy_pattern_not_value.ltdb";
    const db = try openTestDb(path);
    defer closeTestDb(db, path);
    try seedTriangle(db);

    const message = "A pattern, or EXISTS { ... }, is a test, true or false, not a value; COUNT { ... } counts its matches";
    try expectFailure(db, "MATCH (p:P) RETURN (p)-->() - 1 AS x", .semantic, message);
    try expectFailure(db, "MATCH (p:P) RETURN 1 + (p)-->() AS x", .semantic, message);
    try expectFailure(db, "MATCH (p:P) RETURN EXISTS { (p)-->() } * 2 AS x", .semantic, message);
    try expectFailure(db, "RETURN 'a' - 1 AS x", .semantic, "Operator not valid for these types: string and integer");
    // As a test it works, and COUNT gives the number.
    try expectResult(db, "MATCH (p:P) RETURN p.n, (p)-->() AS out, COUNT { (p)-->() } - 1 AS less ORDER BY p.n", "p.n | out | less\n\"a\" | true | 1\n\"b\" | true | 0\n\"c\" | false | -1");
    try expectFailure(db, "MATCH (a)-(b) RETURN a", .parse, "Expected '->' or '-' to end relationship");
}

test "lcy: YIELD * brings the procedure's columns into scope" {
    const path = "/tmp/lcy_yield_star.ltdb";
    const db = try openTestDb(path);
    defer closeTestDb(db, path);
    try seedTriangle(db);

    try expectResult(db, "CALL db.labels() YIELD *", "label\n\"P\"");
    // It was "RETURN * is not allowed when there are no variables in scope".
    try expectFailure(db, "CALL db.labels() YIELD * RETURN *", .plan, "YIELD * is only allowed when CALL db.labels() is the whole query");
}

// ============================================================================
// Small fixes
// ============================================================================

test "lcy: a query without RETURN returns no columns and no rows" {
    const path = "/tmp/lcy_write_only_result.ltdb";
    const db = try openTestDb(path);
    defer closeTestDb(db, path);

    // These returned their working slots, as `col0 | col1` or `vs` with a
    // row of null per input row.
    try expectResult(db, "CREATE (:P {v: 1})-[:R]->(:P {v: 2})", "");
    try expectResult(db, "UNWIND [1, 2] AS x MERGE (:M {x: x})", "");
    try expectResult(db, "MATCH (p:P) WITH collect(p.v) AS vs FOREACH (v IN vs | CREATE (:Q {v: v}))", "");
    try expectResult(db, "MATCH (p:P) CALL { WITH p RETURN p.v * 10 AS w } SET p.w = w", "");
    try expectResult(db, "MATCH (q:Q) DELETE q", "");
    // The writes happened, and RETURN still names its columns.
    try expectResult(db, "MATCH (p:P) RETURN p.w AS w ORDER BY w", "w\n10\n20");
    try expectResult(db, "MATCH (n) RETURN count(n) AS c", "c\n4");
    try expectResult(db, "CREATE (n:Z) RETURN n.v AS v", "v\nnull");
    try expectResult(db, "CALL db.labels() YIELD label WITH label WHERE label = 'Z' RETURN label", "label\n\"Z\"");
}

test "lcy: octal integers, and a leading zero refused" {
    const path = "/tmp/lcy_octal.ltdb";
    const db = try openTestDb(path);
    defer closeTestDb(db, path);

    try expectResult(db, "RETURN 0o17 AS o, 0O7 AS p, -0o10 AS n, 0o0 AS z, 0o017 AS l", "o | p | n | z | l\n15 | 7 | -8 | 0 | 15");
    try expectResult(db, "RETURN 0o777777777777777777777 AS max", "max\n9223372036854775807");
    try expectFailure(db, "RETURN 0o1000000000000000000000 AS o", .parse, "Invalid integer literal");
    try expectFailure(db, "RETURN 0o18 AS o", .parse, "An octal integer takes only the digits 0 to 7");
    // A leading zero was read as decimal: 012 was 12, where openCypher 9
    // read 10.
    try expectFailure(db, "RETURN 012 AS a", .parse, "An integer cannot start with 0, as in 012: write 12 for the decimal number, or 0o12 for the octal one");
    try expectFailure(db, "RETURN -019 AS a", .parse, "An integer cannot start with 0, as in 019: write 19");
    try expectFailure(db, "RETURN 00 AS a", .parse, "An integer cannot start with 0, as in 00: write 0");
    try expectFailure(db, "WITH [1, 2] AS l RETURN l[01] AS a", .parse, "An integer cannot start with 0");
    // Zero itself, floats with a leading zero, hexadecimal and strings are
    // as they were.
    try expectResult(db, "RETURN 0 AS z, 0.5 AS h, 01.5 AS f, 007e1 AS e, 0x0F AS x, '012' AS s, [0, 1][0..1] AS l", "z | h | f | e | x | s | l\n0 | 0.5 | 1.5 | 70 | 15 | \"012\" | [0]");
    // The o is part of the number only before a digit.
    try expectResult(db, "WITH 0 AS o RETURN o, 0 AS other", "o | other\n0 | 0");
}

test "lcy: quantified path patterns and LOAD CSV are refused by name" {
    const path = "/tmp/lcy_qpp_refused.ltdb";
    const db = try openTestDb(path);
    defer closeTestDb(db, path);

    // These forms fell through to "Expected MATCH, OPTIONAL MATCH, ..." or
    // "Expected '(' to start node pattern".
    const segment = "A '(' after a pattern starts a quantified path pattern, such as (x) ((a)-->(b)){1,3}, which is not supported";
    try expectFailure(db, "MATCH (x) ((a)-[:R]->(b))+ RETURN a", .parse, segment);
    try expectFailure(db, "MATCH (x) ((a)-[:R]->(b)){1,3} (y) RETURN a", .parse, segment);
    try expectFailure(db, "MATCH (a) (b) RETURN a", .parse, "separate two patterns with a comma");
    const quantified = "Quantified relationships, such as -[:R]->{1,3} or -[:R]->+, are not supported; write -[:R*1..3]->";
    try expectFailure(db, "MATCH p = (a)-->{2}(b) RETURN p", .parse, quantified);
    try expectFailure(db, "MATCH (a)<-[:R]-+(b) RETURN a", .parse, quantified);
    try expectFailure(db, "MATCH (a) WHERE EXISTS { (a)-[:R]->{1,2}(b) } RETURN a", .parse, quantified);
    // The leading form and LOAD CSV kept their messages.
    try expectFailure(db, "MATCH ((a)-->(b)){1,3} RETURN a", .parse, "Parenthesised path patterns, such as the quantified ((a)-->(b)){1,3}, are not supported");
    try expectFailure(db, "LOAD CSV WITH HEADERS FROM 'file:///x.csv' AS row RETURN row", .parse, "LOAD CSV is not supported; import files with the lattice CLI or the API");
    // Patterns that are not quantified are untouched, in clauses and in
    // expressions.
    try expectResult(db, "CREATE (a:A)-[:R]->(b:B), (b)-[:R]->(:C) RETURN count(*) AS c", "c\n1");
    try expectResult(db, "MATCH (a:A), (b:B) WHERE (a)-->(b) RETURN [(a)-->(x) | labels(x)] AS l, COUNT { (b)-->() } + 1 AS n", "l | n\n[[\"B\"]] | 2");
}

test "lcy: properties() of a map is the map" {
    const path = "/tmp/lcy_properties_map.ltdb";
    const db = try openTestDb(path);
    defer closeTestDb(db, path);

    try expectResult(db, "RETURN properties({a: 1, b: 'x'}) AS p", "p\n{a: 1, b: \"x\"}");
    try expectResult(db, "RETURN properties({}) AS p", "p\n{}");
    try expectResult(db, "WITH {a: {b: 2}} AS m RETURN properties(m.a) AS p", "p\n{b: 2}");
    try expectResult(db, "RETURN properties(null) AS p", "p\nnull");
    try expectFailure(db, "RETURN properties([1])", .execution, "properties(list) is not defined");
}
