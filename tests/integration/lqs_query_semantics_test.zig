//! End-to-end tests for what a query reports when it cannot answer, and for
//! the expression and matching semantics that used to give wrong answers
//! quietly: errors that name their cause, storage and memory failures that
//! fail instead of turning into nulls, and the rules that came with them.

const std = @import("std");
const lattice = @import("lattice");

const Database = lattice.storage.database.Database;
const QueryError = lattice.storage.database.QueryError;
const QueryResult = lattice.storage.database.QueryResult;
const QueryDetailedResult = lattice.storage.database.QueryDetailedResult;
const ResultValue = lattice.storage.database.ResultValue;

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

/// Run a query that must fail, and check the stage, code and message the
/// caller is given.
fn expectFailure(
    db: *Database,
    cypher: []const u8,
    stage: lattice.storage.database.QueryFailureStage,
    code: []const u8,
    message: []const u8,
) !void {
    var detailed = try db.queryDetailed(cypher);
    defer detailed.deinit();
    switch (detailed) {
        .success => {
            std.debug.print("query succeeded but should have failed: {s}\n", .{cypher});
            return error.TestUnexpectedResult;
        },
        .failure => |failure| {
            if (failure.stage != stage or !std.mem.eql(u8, failure.code orelse "", code) or
                !std.mem.eql(u8, failure.message, message))
            {
                std.debug.print("query: {s}\n  got:  {s} {s} \"{s}\"\n  want: {s} {s} \"{s}\"\n", .{
                    cypher,          @tagName(failure.stage), failure.code orelse "-", failure.message,
                    @tagName(stage), code,                    message,
                });
                return error.TestUnexpectedResult;
            }
            try std.testing.expectEqual(lattice.storage.database.QueryFailure.Cause.query, failure.cause);
        },
    }
}

// ============================================================================
// Errors name their cause
// ============================================================================

test "lqs: execution errors say what failed and where" {
    const path = "/tmp/lattice_lqs_messages.ltdb";
    const db = try openTestDb(path);
    defer cleanupTestDb(db, path);
    try run(db, "CREATE (n:Big {v: 9223372036854775807, s: 'x'})");

    // All of these used to say only "Execution error: query failed".
    try expectFailure(db, "RETURN 1 / 0", .execution, "DivisionByZero", "Division by zero: 1 / 0");
    try expectFailure(
        db,
        "MATCH (n:Big) RETURN n.v + 1",
        .execution,
        "IntegerOverflow",
        "Integer overflow: 9223372036854775807 + 1 does not fit in a 64-bit integer",
    );
    try expectFailure(
        db,
        "RETURN toUpper(1)",
        .execution,
        "TypeError",
        "Type error: toUpper(integer) is not defined; toUpper takes (string)",
    );
    try expectFailure(db, "RETURN $nope", .execution, "ParameterNotFound", "Parameter $nope was not given a value");
    try expectFailure(db, "RETURN 1 LIMIT -1", .execution, "TypeError", "SKIP and LIMIT take a non-negative integer, but this is -1");

    var detailed = try db.queryDetailed("RETURN 1 / 0");
    defer detailed.deinit();
    const location = detailed.failure.location.?;
    try std.testing.expectEqual(@as(u32, 1), location.line);
    try std.testing.expectEqual(@as(u32, 10), location.column); // the '/'

}

test "lqs: unknown functions and wrong argument counts fail before execution" {
    const path = "/tmp/lattice_lqs_functions.ltdb";
    const db = try openTestDb(path);
    defer cleanupTestDb(db, path);
    try run(db, "CREATE (n:F {v: 1})");

    // An unknown function used to be found only when a row reached it, so a
    // query over no rows succeeded and one over rows failed with "query failed".
    try expectFailure(db, "MATCH (n:Nothing) RETURN foo(n)", .semantic, "unknown_function", "Unknown function 'foo'");
    try expectFailure(db, "MATCH (n:F) RETURN foo(n)", .semantic, "unknown_function", "Unknown function 'foo'");
    try expectFailure(
        db,
        "RETURN substring('abc')",
        .semantic,
        "wrong_argument_count",
        "substring takes (string, integer[, integer]), but was given 1 argument",
    );
    try expectFailure(db, "RETURN toLower('a', 'b')", .semantic, "wrong_argument_count", "toLower takes (string), but was given 2 arguments");

    // Case does not matter, and aggregates are functions too.
    var ok = try db.query("MATCH (n:F) RETURN TOUPPER('a') AS u, Count(n) AS c");
    defer ok.deinit();
    try std.testing.expectEqualStrings("A", ok.rows[0].values[0].string_val);
}

// ============================================================================
// Memory and storage failures fail; they never become null
// ============================================================================

/// Runs `cypher` with every allocation, one at a time, made to fail, and
/// checks each run either fails with an out-of-memory error or returns
/// `expected`. Before, several reads turned an allocation failure into a null
/// value, so the query "succeeded" with a wrong answer.
fn sweepAllocationFailures(
    failing: *std.testing.FailingAllocator,
    db: *Database,
    cypher: []const u8,
    expected: []const []const u8,
) !void {
    var i: usize = 0;
    while (true) : (i += 1) {
        failing.fail_index = failing.alloc_index + i;
        failing.has_induced_failure = false;
        const outcome = db.queryDetailed(cypher);
        const reached = failing.has_induced_failure;
        failing.fail_index = std.math.maxInt(usize);

        var detailed: QueryDetailedResult = outcome catch |err| {
            try std.testing.expect(reached);
            try std.testing.expectEqual(error.OutOfMemory, err);
            continue;
        };
        defer detailed.deinit();
        switch (detailed) {
            .failure => |failure| {
                if (!reached or failure.cause != .out_of_memory) {
                    std.debug.print("allocation {d}: {s}: failed with \"{s}\"\n", .{ i, cypher, failure.message });
                    return error.TestUnexpectedResult;
                }
            },
            .success => |result| {
                // Every row must be the full right answer.
                try std.testing.expectEqual(expected.len, result.rowCount());
                for (result.rows, expected) |row, want| {
                    var buf: [256]u8 = undefined;
                    var w = std.Io.Writer.fixed(&buf);
                    for (row.values, 0..) |value, col| {
                        if (col > 0) try w.writeAll(",");
                        try writeValue(&w, value);
                    }
                    if (!std.mem.eql(u8, w.buffered(), want)) {
                        std.debug.print("allocation {d}: {s}: row \"{s}\", want \"{s}\"\n", .{ i, cypher, w.buffered(), want });
                        return error.TestUnexpectedResult;
                    }
                }
                if (!reached) return;
            },
        }
    }
}

fn writeValue(w: *std.Io.Writer, value: ResultValue) !void {
    switch (value) {
        .null_val => try w.writeAll("null"),
        .bool_val => |b| try w.print("{}", .{b}),
        .int_val => |v| try w.print("{d}", .{v}),
        .float_val => |v| try w.print("{d}", .{v}),
        .string_val => |s| try w.writeAll(s),
        .list_val => |items| {
            try w.writeAll("[");
            for (items, 0..) |item, idx| {
                if (idx > 0) try w.writeAll(" ");
                try writeValue(w, item);
            }
            try w.writeAll("]");
        },
        .map_val => |entries| {
            try w.writeAll("{");
            for (entries, 0..) |entry, idx| {
                if (idx > 0) try w.writeAll(" ");
                try w.print("{s}:", .{entry.key});
                try writeValue(w, entry.value);
            }
            try w.writeAll("}");
        },
        else => try w.writeAll("?"),
    }
}

test "lqs: an allocation failure fails the query instead of reading null" {
    const path = "/tmp/lattice_lqs_oom.ltdb";
    @import("compat").fs.cwd().deleteFile(path) catch {};
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    const db = try Database.open(failing.allocator(), path, .{
        .create = true,
        .config = .{ .enable_wal = false },
    });
    defer cleanupTestDb(db, path);
    try run(db, "CREATE (a:P {name: 'ada', tags: ['x', 'y']})-[:KNOWS {since: 'long ago'}]->(b:P {name: 'bo'})");

    try sweepAllocationFailures(&failing, db, "MATCH (a:P {name: 'ada'}) RETURN a.name, a.tags", &.{"ada,[x y]"});
    try sweepAllocationFailures(&failing, db, "MATCH (a:P)-[r]->(b) RETURN type(r), r.since, b.name", &.{"KNOWS,long ago,bo"});
    try sweepAllocationFailures(&failing, db, "MATCH (a:P {name: 'ada'}) RETURN labels(a)", &.{"[P]"});
    try sweepAllocationFailures(&failing, db, "MATCH (a:P) WITH a.name AS n ORDER BY n RETURN collect(n)", &.{"[ada bo]"});
    // properties() reads the whole node or relationship; a failure part way
    // through that read must not leak what it had copied.
    try sweepAllocationFailures(&failing, db, "MATCH (a:P {name: 'bo'}) RETURN properties(a)", &.{"{name:bo}"});
    try sweepAllocationFailures(&failing, db, "MATCH (:P)-[r]->() RETURN properties(r)", &.{"{since:long ago}"});
}

// ============================================================================
// Integers and floats compare exactly
// ============================================================================

fn expectBools(db: *Database, cypher: []const u8, expected: []const ?bool) !void {
    var result = try db.query(cypher);
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), result.rowCount());
    try std.testing.expectEqual(expected.len, result.rows[0].values.len);
    for (expected, result.rows[0].values, 0..) |want, got, col| {
        if (want) |b| {
            if (got != .bool_val or got.bool_val != b) {
                std.debug.print("{s}: column {d} want {}, got {any}\n", .{ cypher, col, b, got });
                return error.TestUnexpectedResult;
            }
        } else try std.testing.expect(got == .null_val);
    }
}

fn queryInt(db: *Database, cypher: []const u8) !i64 {
    var result = try db.query(cypher);
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), result.rowCount());
    return result.rows[0].values[0].int_val;
}

test "lqs: an integer and a float compare by their exact values" {
    const path = "/tmp/lattice_lqs_int_float.ltdb";
    const db = try openTestDb(path);
    defer cleanupTestDb(db, path);

    // 2^53 + 1 has no float of its own: converting it to compare made it
    // equal to 2^53.0, and i64 max equal to 2^63.0.
    try expectBools(db, "RETURN 9007199254740993 = 9007199254740992.0, 9007199254740993 > 9007199254740992.0, " ++
        "9007199254740993 <> 9007199254740992.0, 9007199254740992.0 < 9007199254740993", &.{ false, true, true, true });
    try expectBools(db, "RETURN 9223372036854775807 < 9223372036854775808.0, 9223372036854775807 = 9223372036854775808.0, " ++
        "-9223372036854775808 = -9223372036854775808.0, -9223372036854775807 > -9223372036854775808.0", &.{ true, false, true, true });
    try expectBools(db, "RETURN 9007199254740993 IN [9007199254740992.0], 9007199254740992 IN [9007199254740992.0], " ++
        "1 = 1.0, 1 < 1.5, 2 >= 1.5, 0 = -0.0, 3 <= 2.9999999999999996", &.{ false, true, true, true, true, true, false });

    // ORDER BY already compared exactly; WHERE now agrees with it.
    var ordered = try db.query("UNWIND [9007199254740992.0, 9007199254740993, 9007199254740991] AS x " ++
        "WITH x ORDER BY x WHERE x > 9007199254740992.0 RETURN x");
    defer ordered.deinit();
    try std.testing.expectEqual(@as(usize, 1), ordered.rowCount());
    try std.testing.expectEqual(@as(i64, 9007199254740993), ordered.rows[0].values[0].int_val);
}

test "lqs: DISTINCT and grouping treat equal integers and floats as one value" {
    const path = "/tmp/lattice_lqs_int_float_groups.ltdb";
    const db = try openTestDb(path);
    defer cleanupTestDb(db, path);

    // 1 = 1.0 is true, so they are one value; they used to be two.
    try std.testing.expectEqual(@as(i64, 2), try queryInt(db, "UNWIND [1, 1.0, 2] AS x RETURN count(DISTINCT x)"));
    try std.testing.expectEqual(@as(i64, 1), try queryInt(db, "UNWIND [0, -0.0, 0.0] AS x RETURN count(DISTINCT x)"));
    try std.testing.expectEqual(@as(i64, 2), try queryInt(db, "UNWIND [[1, 2], [1.0, 2.0], [1, 2.5]] AS x RETURN count(DISTINCT x)"));
    // Exactly: 2^53 + 1 is not 2^53.0.
    try std.testing.expectEqual(@as(i64, 2), try queryInt(
        db,
        "UNWIND [9007199254740993, 9007199254740992.0] AS x RETURN count(DISTINCT x)",
    ));

    var distinct = try db.query("UNWIND [1, 1.0, 2.5] AS x RETURN DISTINCT x");
    defer distinct.deinit();
    try std.testing.expectEqual(@as(usize, 2), distinct.rowCount());

    var groups = try db.query("UNWIND [1.0, 1, 2.5, 9007199254740993, 9007199254740992.0] AS x " ++
        "RETURN x, count(*) AS c ORDER BY x");
    defer groups.deinit();
    try std.testing.expectEqual(@as(usize, 4), groups.rowCount());
    try std.testing.expectEqual(@as(i64, 2), groups.rows[0].values[1].int_val);
}

test "lqs: MERGE matches an integer and a float exactly" {
    const path = "/tmp/lattice_lqs_int_float_merge.ltdb";
    const db = try openTestDb(path);
    defer cleanupTestDb(db, path);

    try run(db, "CREATE (n:M {v: 9007199254740992.0})");
    // Used to match the 2^53.0 node and create nothing.
    try run(db, "MERGE (n:M {v: 9007199254740993})");
    try std.testing.expectEqual(@as(i64, 2), try queryInt(db, "MATCH (n:M) RETURN count(n)"));
    // A number equal to the float still matches it.
    try run(db, "MERGE (n:M {v: 9007199254740992})");
    try std.testing.expectEqual(@as(i64, 2), try queryInt(db, "MATCH (n:M) RETURN count(n)"));

    // A relationship's properties used to need the same type as well as the
    // same value, so 1 did not find 1.0 and a second relationship was made.
    try run(db, "CREATE (a:E {k: 1})-[:R {w: 1.0}]->(b:E {k: 2})");
    try run(db, "MATCH (a:E {k: 1}), (b:E {k: 2}) MERGE (a)-[:R {w: 1}]->(b)");
    try std.testing.expectEqual(@as(i64, 1), try queryInt(db, "MATCH (:E)-[r:R]->(:E) RETURN count(r)"));
    try run(db, "MATCH (a:E {k: 1}), (b:E {k: 2}) MERGE (a)-[:R {w: 1.5}]->(b)");
    try std.testing.expectEqual(@as(i64, 2), try queryInt(db, "MATCH (:E)-[r:R]->(:E) RETURN count(r)"));
}

// ============================================================================
// Strings are characters, not bytes; functions of null are null
// ============================================================================

fn expectStrings(db: *Database, cypher: []const u8, expected: []const ?[]const u8) !void {
    var result = try db.query(cypher);
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), result.rowCount());
    try std.testing.expectEqual(expected.len, result.rows[0].values.len);
    for (expected, result.rows[0].values, 0..) |want, got, col| {
        if (want) |w| {
            if (got != .string_val or !std.mem.eql(u8, got.string_val, w)) {
                std.debug.print("{s}: column {d} want \"{s}\", got {any}\n", .{ cypher, col, w, got });
                return error.TestUnexpectedResult;
            }
        } else if (got != .null_val) {
            std.debug.print("{s}: column {d} want null, got {any}\n", .{ cypher, col, got });
            return error.TestUnexpectedResult;
        }
    }
}

test "lqs: string functions count and cut characters, not bytes" {
    const path = "/tmp/lattice_lqs_unicode.ltdb";
    const db = try openTestDb(path);
    defer cleanupTestDb(db, path);

    // size() counted bytes: 'é' was 2 and '€' 3.
    try std.testing.expectEqual(@as(i64, 5), try queryInt(db, "RETURN size('héllo')"));
    try std.testing.expectEqual(@as(i64, 1), try queryInt(db, "RETURN size('€')"));
    try std.testing.expectEqual(@as(i64, 2), try queryInt(db, "RETURN size('東京')"));
    try std.testing.expectEqual(@as(i64, 3), try queryInt(db, "RETURN size(['é', 1, null])"));

    // substring() cut bytes, so it could split a character in two.
    try expectStrings(db, "RETURN substring('héllo wörld', 1, 4), substring('東京タワー', 2), " ++
        "substring('aé', 1, 1), substring('abc', 5), substring('abc', 1, 0)", &.{ "éllo", "タワー", "é", "", "" });
    try expectStrings(db, "RETURN substring('abc', 0, 2), substring('abc', 1)", &.{ "ab", "bc" });

    // toUpper() and toLower() changed ASCII letters only.
    try expectStrings(db, "RETURN toUpper('héllo wörld'), toLower('ÀÉÎ İSTANBUL'), toUpper('москва'), " ++
        "toLower('ΑΘΗΝΑ'), toUpper('ßtraße'), toUpper('ı')", &.{
        "HÉLLO WÖRLD",
        "àéî istanbul",
        "МОСКВА",
        "αθηνα",
        "ßTRAßE",
        "I",
    });
    // Unchanged where nothing has case, and ASCII as before.
    try expectStrings(db, "RETURN toUpper('東京 123 abc'), toLower('MiXeD')", &.{ "東京 123 ABC", "mixed" });

    // split() with an empty delimiter splits between characters; it used to
    // reach a library call that requires a non-empty one.
    var parts = try db.query("RETURN split('aé€', '') AS p");
    defer parts.deinit();
    const list = parts.rows[0].values[0].list_val;
    try std.testing.expectEqual(@as(usize, 3), list.len);
    try std.testing.expectEqualStrings("é", list[1].string_val);
    try std.testing.expectEqualStrings("€", list[2].string_val);

    // Byte-wise operators agree with characters on valid UTF-8.
    try expectBools(db, "RETURN 'héllo' STARTS WITH 'hé', 'héllo' ENDS WITH 'llo', 'wörld' CONTAINS 'ö', " ++
        "'é' CONTAINS 'e'", &.{ true, true, true, false });
}

test "lqs: a null argument makes a function null" {
    const path = "/tmp/lattice_lqs_null_args.ltdb";
    const db = try openTestDb(path);
    defer cleanupTestDb(db, path);

    // substring, replace and split were type errors when a later argument
    // was null, and null only when the string was.
    try expectStrings(db, "RETURN substring('abc', null), substring('abc', 1, null), substring(null, 1), " ++
        "replace('abc', null, 'x'), replace('abc', 'b', null), replace(null, 'b', 'x')", &.{ null, null, null, null, null, null });
    var split = try db.query("RETURN split('a,b', null) AS a, split(null, ',') AS b");
    defer split.deinit();
    try std.testing.expect(split.rows[0].values[0] == .null_val);
    try std.testing.expect(split.rows[0].values[1] == .null_val);
    // Null still wins over an argument of the wrong type.
    try expectStrings(db, "RETURN substring(null, 'x'), replace(1, null, 'x')", &.{ null, null });

    // The rest already were, and stay so.
    try expectStrings(db, "RETURN toUpper(null), toLower(null), trim(null), toString(null), type(null)", &.{ null, null, null, null, null });
    var others = try db.query("RETURN size(null), abs(null), head(null), last(null), tail(null), " ++
        "toInteger(null), toFloat(null), range(1, null), id(null), labels(null), properties(null), coalesce(null, null)");
    defer others.deinit();
    for (others.rows[0].values) |value| try std.testing.expect(value == .null_val);
    // A wrong type that is not null is still an error.
    try expectFailure(db, "RETURN substring('abc', 'x')", .execution, "TypeError", "Type error: substring(string, string) is not defined; substring takes (string, integer[, integer])");
}

// ============================================================================
// Relationship uniqueness across variable-length relationships
// ============================================================================

test "lqs: a MATCH never binds one relationship twice, variable-length or not" {
    const path = "/tmp/lattice_lqs_var_unique.ltdb";
    const db = try openTestDb(path);
    defer cleanupTestDb(db, path);
    try run(db, "CREATE (:U {n: 1})-[:R]->(:U {n: 2})-[:R]->(:U {n: 3})");

    // A single hop after a variable-length one could walk back along the
    // path's own edge; so could a second variable-length one.
    try std.testing.expectEqual(@as(i64, 0), try queryInt(db, "MATCH (:U {n: 1})-[:R*1..1]->(b)<-[:R]-(c) RETURN count(*)"));
    try std.testing.expectEqual(@as(i64, 0), try queryInt(db, "MATCH (:U {n: 1})-[:R*1..2]->(b)<-[:R*1..2]-(c) RETURN count(*)"));
    // The other way round already held.
    try std.testing.expectEqual(@as(i64, 0), try queryInt(db, "MATCH (:U {n: 1})-[:R]->(b)<-[:R*1..1]-(c) RETURN count(*)"));
    // Undirected: from 1 the path takes 1-2, and the next hop may take only 2-3.
    var undirected = try db.query("MATCH (:U {n: 1})-[:R*1..1]-(y)-[:R]-(z) RETURN z.n");
    defer undirected.deinit();
    try std.testing.expectEqual(@as(usize, 1), undirected.rowCount());
    try std.testing.expectEqual(@as(i64, 3), undirected.rows[0].values[0].int_val);
    // Across comma-separated patterns of one MATCH: the path [1-2] leaves
    // 2->3 for the second pattern, the path [1-2, 2-3] leaves nothing.
    try std.testing.expectEqual(@as(i64, 1), try queryInt(db, "MATCH (:U {n: 1})-[:R*1..2]-(y), (p)-[:R]->(q) RETURN count(*)"));
    // A zero-length path uses no edge.
    var zero = try db.query("MATCH (:U {n: 1})-[:R*0..2]->(y)-[:R]->(z) RETURN y.n, z.n ORDER BY y.n");
    defer zero.deinit();
    try std.testing.expectEqual(@as(usize, 2), zero.rowCount());
    try std.testing.expectEqual(@as(i64, 1), zero.rows[0].values[0].int_val);
    try std.testing.expectEqual(@as(i64, 2), zero.rows[1].values[0].int_val);
    // Separate MATCH clauses may bind the same relationship.
    try std.testing.expectEqual(@as(i64, 1), try queryInt(db, "MATCH (:U {n: 1})-[:R*1..1]->(b) MATCH (b)<-[:R]-(c) RETURN count(*)"));
    // Relationships whose types cannot overlap need no check, and still match.
    try run(db, "MATCH (a:U {n: 3}) CREATE (a)-[:S]->(:U {n: 4})");
    try std.testing.expectEqual(@as(i64, 1), try queryInt(db, "MATCH (:U {n: 1})-[:R*2..2]->(b)-[:S]->(c) RETURN count(*)"));
}
