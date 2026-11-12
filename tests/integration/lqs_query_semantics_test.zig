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
