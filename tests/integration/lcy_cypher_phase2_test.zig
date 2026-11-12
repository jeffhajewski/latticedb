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
