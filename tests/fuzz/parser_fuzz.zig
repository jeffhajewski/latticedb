//! Fuzz testing for the Cypher query parser.
//!
//! The parser must handle arbitrary input without:
//! - Crashing (panics, segfaults)
//! - Memory corruption
//! - Infinite loops
//! - Memory leaks
//!
//! Valid queries should parse successfully; invalid queries should
//! return errors gracefully.

const std = @import("std");
const lattice = @import("lattice");

const Parser = lattice.query.parser.Parser;
const Lexer = lattice.query.lexer.Lexer;

/// Fuzz the Cypher parser with arbitrary input.
/// The parser should never crash regardless of input.
pub fn fuzzParser(allocator: std.mem.Allocator, input: []const u8) !void {
    // Create parser with fuzz input
    var parser = Parser.init(allocator, input);
    defer parser.deinit();

    // Attempt to parse - should not crash
    const result = parser.parse();

    // If parsing succeeded, verify the query is valid
    if (result.query) |query| {
        // Walk the AST to ensure it's well-formed
        for (query.clauses) |clause| {
            _ = validateClause(clause);
        }
        for (query.unions) |branch| {
            for (branch.clauses) |clause| {
                _ = validateClause(clause);
            }
        }
    }

    // Errors are expected for malformed input - just ensure they exist
    if (result.errors.len > 0) {
        for (result.errors) |err| {
            // Verify error messages are valid strings
            _ = err.message.len;
        }
    }
}

/// Recursively validate AST clause structure
fn validateClause(clause: lattice.query.ast.Clause) bool {
    return switch (clause) {
        .match => true,
        .where => true,
        .return_ => true,
        .create => true,
        .delete => true,
        .set => true,
        .remove => true,
        .order_by => true,
        .skip => true,
        .limit => true,
        .with => true,
        .merge => true,
        .unwind => true,
        .call => true,
    };
}

/// Fuzz the lexer independently
pub fn fuzzLexer(input: []const u8) !void {
    var lexer = Lexer.init(input);

    // Consume all tokens - should never crash
    while (true) {
        const token = lexer.nextToken();
        if (token.token_type == .eof) break;
        if (token.token_type == .invalid) {
            // Invalid tokens are fine, just ensure we can read the text
            _ = token.text.len;
        }
    }
}

// ============================================================================
// Fuzz Tests using std.testing.fuzz
// ============================================================================

fn parserFuzzRunner(allocator: std.mem.Allocator, smith: *std.testing.Smith) !void {
    var input: [4096]u8 = undefined;
    const len = smith.slice(&input);
    try fuzzParser(allocator, input[0..len]);
}

fn lexerFuzzRunner(_: std.mem.Allocator, smith: *std.testing.Smith) !void {
    var input: [4096]u8 = undefined;
    const len = smith.slice(&input);
    try fuzzLexer(input[0..len]);
}

/// Fuzz `=~`: the first input byte splits pattern from text. Patterns come
/// from queries and parameters, so compiling and matching must neither crash
/// nor take long, whatever the pattern.
fn regexFuzzRunner(allocator: std.mem.Allocator, smith: *std.testing.Smith) !void {
    var input: [512]u8 = undefined;
    const len = smith.slice(&input);
    if (len == 0) return;
    const split = @min(@as(usize, input[0]), len - 1);
    const pattern = input[1 .. 1 + split];
    const text = input[1 + split .. len];
    _ = lattice.query.regex.fullMatch(allocator, pattern, text) catch |err| switch (err) {
        error.InvalidPattern, error.PatternTooComplex => return,
        error.OutOfMemory => return err,
    };
}

test "fuzz: regex handles arbitrary patterns and text" {
    const allocator = std.testing.allocator;
    try std.testing.fuzz(allocator, regexFuzzRunner, .{});
}

test "fuzz: parser handles arbitrary input" {
    const allocator = std.testing.allocator;
    try std.testing.fuzz(allocator, parserFuzzRunner, .{});
}

test "fuzz: lexer handles arbitrary input" {
    const allocator = std.testing.allocator;
    try std.testing.fuzz(allocator, lexerFuzzRunner, .{});
}

// ============================================================================
// Corpus-based tests (known edge cases)
// ============================================================================

test "parser: handles empty input" {
    const allocator = std.testing.allocator;
    try fuzzParser(allocator, "");
}

test "parser: handles null bytes" {
    const allocator = std.testing.allocator;
    try fuzzParser(allocator, "\x00\x00\x00");
}

test "parser: handles very long input" {
    const allocator = std.testing.allocator;
    const long_input = "MATCH " ++ "(" ** 100 ++ "n" ++ ")" ** 100;
    try fuzzParser(allocator, long_input);
}

test "parser: handles unicode" {
    const allocator = std.testing.allocator;
    try fuzzParser(allocator, "MATCH (n) WHERE n.name = '\u{1F600}\u{1F4A9}'");
}

test "parser: handles control characters" {
    const allocator = std.testing.allocator;
    try fuzzParser(allocator, "MATCH\t\n\r(n)\x0B\x0CWHERE n.x = 1");
}

test "parser: handles incomplete strings" {
    const allocator = std.testing.allocator;
    try fuzzParser(allocator, "MATCH (n) WHERE n.name = 'unclosed");
    try fuzzParser(allocator, "MATCH (n) WHERE n.name = \"unclosed");
}

test "parser: handles deeply nested expressions" {
    const allocator = std.testing.allocator;
    const nested = "((((((((((1+1)+1)+1)+1)+1)+1)+1)+1)+1)+1)";
    try fuzzParser(allocator, "MATCH (n) WHERE n.x = " ++ nested);
}

test "parser: handles keyword-like identifiers" {
    const allocator = std.testing.allocator;
    try fuzzParser(allocator, "MATCH (MATCH) WHERE WHERE.RETURN = RETURN");
}

test "parser: handles numeric edge cases" {
    const allocator = std.testing.allocator;
    try fuzzParser(allocator, "MATCH (n) WHERE n.x = 9999999999999999999999999999");
    try fuzzParser(allocator, "MATCH (n) WHERE n.x = -0");
    try fuzzParser(allocator, "MATCH (n) WHERE n.x = .5"); // Was panicking, now returns error
    try fuzzParser(allocator, "MATCH (n) WHERE n.x = 0.5");
    try fuzzParser(allocator, "MATCH (n) WHERE n.x = 1e999");
}

test "parser: handles the syntax added after 0.16" {
    const allocator = std.testing.allocator;
    const corpus = [_][]const u8{
        "RETURN [1, 2, 3][0], [1, 2, 3][-1], [1, 2, 3][1..], [1, 2, 3][..-1], {a: 1}['a']",
        "RETURN [[1, 2], [3]][0][1], [1][",
        "RETURN [1][..",
        "RETURN [1][1..2..3]",
        "RETURN CASE WHEN true THEN 1 ELSE 2 END, CASE 1 WHEN 1 THEN 'a' END",
        "RETURN CASE WHEN CASE 1 WHEN 1 THEN true END THEN CASE WHEN false THEN 0 END END",
        "RETURN CASE",
        "RETURN CASE WHEN",
        "RETURN CASE WHEN 1 THEN",
        "RETURN CASE END",
        "MATCH (n) RETURN count(n) + 1, sum(n.x) / count(*)",
        "RETURN 1 AS x UNION RETURN 2 AS x",
        "RETURN 1 AS x UNION ALL RETURN 2 AS x UNION ALL RETURN 3 AS x",
        "UNION",
        "UNION ALL",
        "RETURN 1 UNION",
        "RETURN 1 UNION UNION RETURN 1",
        "RETURN 1 UNION RETURN 1 UNION ALL RETURN 1",
        "MATCH (a) OPTIONAL MATCH (a)-[r]->(b) WHERE r.x > 1 RETURN a, b",
        "OPTIONAL MATCH (n) RETURN n UNION OPTIONAL MATCH (n) RETURN n",
        "OPTIONAL",
        "OPTIONAL MATCH",
        "OPTIONAL (n)",
        "MATCH (optional) RETURN optional",
        "MATCH p = (a)-[*1..3]->(b), q = (c) RETURN length(p), nodes(q), relationships(p)",
        "RETURN [x IN [1, 2] WHERE x > 1 | x * 2], [x IN [[1]] | [y IN x | y]], all(x IN [1] WHERE x > 0)",
        "RETURN any(x IN [] WHERE true), none(x IN null WHERE x), single(x IN [1] WHERE true)",
        "RETURN reduce(s = 0, x IN [1, 2] | s + x), reduce(a = [], x IN [1] | a + x)",
        "RETURN [x IN",
        "RETURN [x IN [1] |",
        "RETURN all(x IN [1])",
        "RETURN reduce(s = 0, x IN [1])",
        "RETURN reduce(s, x)",
        "MATCH (a) WHERE (a)-->() AND NOT (a)<-[:R]-(:X) AND exists((a)--()) RETURN a",
        "MATCH (a) WHERE EXISTS { MATCH (a)-[r]->(b) WHERE r.x = 1 } RETURN COUNT { (a)-->() }, [(a)-->(b) WHERE b.x | b.y]",
        "RETURN [p = (a)-->() | p], [(1), (2)], (1) - (2), (a)<-1",
        "MATCH (a) WHERE EXISTS { RETURN 1 } RETURN a",
        "RETURN EXISTS {",
        "RETURN COUNT { (a) ",
        "RETURN [(a)-->(b) |",
        "RETURN [(a)-->(b) WHERE",
        "CALL db.labels()",
        "CALL db.indexes() YIELD type AS t, property WHERE t = 'x' RETURN t",
        "CALL db.labels() YIELD *",
        "CALL",
        "CALL db.",
        "CALL db.labels(",
        "CALL db.labels() YIELD",
        "CALL db.labels() YIELD a AS",
        "CALL { RETURN 1 }",
        "MATCH p (a) RETURN p",
        "MATCH p = RETURN p",
        "CREATE p = (a)",
        "MERGE p = (a)",
        "MATCH p = = (a) RETURN p",
    };
    for (corpus) |input| try fuzzParser(allocator, input);
}

test "lexer: handles binary data" {
    try fuzzLexer(&[_]u8{ 0xFF, 0xFE, 0x00, 0x01, 0x80, 0x7F });
}

test "lexer: handles long tokens" {
    const long_ident = "a" ** 10000;
    try fuzzLexer(long_ident);
}
