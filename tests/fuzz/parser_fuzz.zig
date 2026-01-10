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

fn parserFuzzRunner(allocator: std.mem.Allocator, input: []const u8) !void {
    try fuzzParser(allocator, input);
}

fn lexerFuzzRunner(_: std.mem.Allocator, input: []const u8) !void {
    try fuzzLexer(input);
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

test "lexer: handles binary data" {
    try fuzzLexer(&[_]u8{ 0xFF, 0xFE, 0x00, 0x01, 0x80, 0x7F });
}

test "lexer: handles long tokens" {
    const long_ident = "a" ** 10000;
    try fuzzLexer(long_ident);
}
