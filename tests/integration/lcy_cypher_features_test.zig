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
