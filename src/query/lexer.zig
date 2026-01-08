//! Cypher query lexer for Lattice database.
//!
//! Tokenizes Cypher queries with support for:
//! - Case-insensitive keywords (MATCH, WHERE, RETURN, etc.)
//! - Special operators: <=> (vector distance), @@ (FTS match)
//! - Graph pattern syntax: ->, <-, -[]->, etc.
//! - Parameters: $name
//! - Literals: integers, floats, strings

const std = @import("std");
const parser = @import("parser.zig");

pub const TokenType = parser.TokenType;
pub const Token = parser.Token;

/// Cypher lexer - tokenizes query strings
pub const Lexer = struct {
    source: []const u8,
    position: usize,
    line: u32,
    line_start: usize,

    const Self = @This();

    /// Initialize lexer with source query
    pub fn init(source: []const u8) Self {
        return .{
            .source = source,
            .position = 0,
            .line = 1,
            .line_start = 0,
        };
    }

    /// Get the next token
    pub fn nextToken(self: *Self) Token {
        self.skipWhitespaceAndComments();

        if (self.isAtEnd()) {
            return self.makeToken(.eof);
        }

        const start = self.position;
        const c = self.advance();

        // Single-character tokens
        switch (c) {
            '(' => return self.makeTokenAt(.lparen, start),
            ')' => return self.makeTokenAt(.rparen, start),
            '[' => return self.makeTokenAt(.lbracket, start),
            ']' => return self.makeTokenAt(.rbracket, start),
            '{' => return self.makeTokenAt(.lbrace, start),
            '}' => return self.makeTokenAt(.rbrace, start),
            ':' => return self.makeTokenAt(.colon, start),
            ',' => return self.makeTokenAt(.comma, start),
            '.' => return self.makeTokenAt(.dot, start),
            '+' => {
                if (self.peek() == '=') {
                    _ = self.advance();
                    return self.makeTokenAt(.plus_equal, start);
                }
                return self.makeTokenAt(.plus, start);
            },
            '*' => return self.makeTokenAt(.star, start),
            '/' => return self.makeTokenAt(.slash, start),
            '%' => return self.makeTokenAt(.percent, start),
            '^' => return self.makeTokenAt(.caret, start),
            '|' => return self.makeTokenAt(.pipe, start),
            '=' => return self.makeTokenAt(.eq, start),
            '-' => return self.scanDash(start),
            '<' => return self.scanLessThan(start),
            '>' => return self.scanGreaterThan(start),
            '@' => return self.scanAt(start),
            '$' => return self.scanParameter(start),
            '"', '\'' => return self.scanString(c, start),
            '!' => return self.scanBang(start),
            else => {
                if (isDigit(c)) return self.scanNumber(start);
                if (isAlpha(c) or c == '_') return self.scanIdentifierOrKeyword(start);
                return self.makeTokenAt(.invalid, start);
            },
        }
    }

    /// Peek at current character without advancing
    fn peek(self: *const Self) ?u8 {
        if (self.isAtEnd()) return null;
        return self.source[self.position];
    }

    /// Peek at character n positions ahead
    fn peekAhead(self: *const Self, n: usize) ?u8 {
        if (self.position + n >= self.source.len) return null;
        return self.source[self.position + n];
    }

    /// Advance position and return current character
    fn advance(self: *Self) u8 {
        const c = self.source[self.position];
        self.position += 1;
        return c;
    }

    /// Check if at end of source
    fn isAtEnd(self: *const Self) bool {
        return self.position >= self.source.len;
    }

    /// Skip whitespace and comments
    fn skipWhitespaceAndComments(self: *Self) void {
        while (!self.isAtEnd()) {
            const c = self.peek().?;
            switch (c) {
                ' ', '\t', '\r' => {
                    _ = self.advance();
                },
                '\n' => {
                    _ = self.advance();
                    self.line += 1;
                    self.line_start = self.position;
                },
                '/' => {
                    // Check for // comment
                    if (self.peekAhead(1) == '/') {
                        while (!self.isAtEnd() and self.peek() != '\n') {
                            _ = self.advance();
                        }
                    } else {
                        return;
                    }
                },
                else => return,
            }
        }
    }

    /// Create token at current position
    fn makeToken(self: *Self, token_type: TokenType) Token {
        return self.makeTokenAt(token_type, self.position);
    }

    /// Create token at specified start position
    fn makeTokenAt(self: *Self, token_type: TokenType, start: usize) Token {
        return Token{
            .token_type = token_type,
            .text = self.source[start..self.position],
            .line = self.line,
            .column = @intCast(start - self.line_start + 1),
        };
    }

    /// Scan dash: -, ->, -[
    fn scanDash(self: *Self, start: usize) Token {
        if (self.peek() == '>') {
            _ = self.advance();
            return self.makeTokenAt(.arrow_right, start);
        }
        return self.makeTokenAt(.dash, start);
    }

    /// Scan less-than: <, <=, <=>, <-, <>
    fn scanLessThan(self: *Self, start: usize) Token {
        if (self.peek() == '=') {
            _ = self.advance();
            if (self.peek() == '>') {
                _ = self.advance();
                return self.makeTokenAt(.vector_distance, start); // <=>
            }
            return self.makeTokenAt(.lte, start); // <=
        }
        if (self.peek() == '-') {
            _ = self.advance();
            return self.makeTokenAt(.arrow_left, start); // <-
        }
        if (self.peek() == '>') {
            _ = self.advance();
            return self.makeTokenAt(.neq, start); // <>
        }
        return self.makeTokenAt(.lt, start); // <
    }

    /// Scan greater-than: >, >=
    fn scanGreaterThan(self: *Self, start: usize) Token {
        if (self.peek() == '=') {
            _ = self.advance();
            return self.makeTokenAt(.gte, start);
        }
        return self.makeTokenAt(.gt, start);
    }

    /// Scan @: @@
    fn scanAt(self: *Self, start: usize) Token {
        if (self.peek() == '@') {
            _ = self.advance();
            return self.makeTokenAt(.fts_match, start);
        }
        return self.makeTokenAt(.invalid, start);
    }

    /// Scan !: != (alternative to <>)
    fn scanBang(self: *Self, start: usize) Token {
        if (self.peek() == '=') {
            _ = self.advance();
            return self.makeTokenAt(.neq, start);
        }
        return self.makeTokenAt(.invalid, start);
    }

    /// Scan parameter: $name
    fn scanParameter(self: *Self, start: usize) Token {
        while (!self.isAtEnd()) {
            const c = self.peek().?;
            if (isAlphaNumeric(c) or c == '_') {
                _ = self.advance();
            } else {
                break;
            }
        }
        return self.makeTokenAt(.parameter, start);
    }

    /// Scan string literal
    fn scanString(self: *Self, quote: u8, start: usize) Token {
        while (!self.isAtEnd()) {
            const c = self.peek().?;
            if (c == quote) {
                _ = self.advance();
                return self.makeTokenAt(.string, start);
            }
            if (c == '\\' and !self.isAtEnd()) {
                _ = self.advance(); // skip escape char
                if (!self.isAtEnd()) {
                    _ = self.advance(); // skip escaped char
                }
            } else if (c == '\n') {
                // Unterminated string at newline
                return self.makeTokenAt(.invalid, start);
            } else {
                _ = self.advance();
            }
        }
        // Unterminated string at EOF
        return self.makeTokenAt(.invalid, start);
    }

    /// Scan number (integer or float)
    fn scanNumber(self: *Self, start: usize) Token {
        // Integer part
        while (!self.isAtEnd() and isDigit(self.peek().?)) {
            _ = self.advance();
        }

        var is_float = false;

        // Check for decimal point
        if (self.peek() == '.' and self.peekAhead(1) != null and isDigit(self.peekAhead(1).?)) {
            is_float = true;
            _ = self.advance(); // consume '.'
            while (!self.isAtEnd() and isDigit(self.peek().?)) {
                _ = self.advance();
            }
        }

        // Check for exponent (valid for both integer and decimal forms: 1e10, 3.14e-2)
        if (self.peek() == 'e' or self.peek() == 'E') {
            is_float = true;
            _ = self.advance();
            if (self.peek() == '+' or self.peek() == '-') {
                _ = self.advance();
            }
            while (!self.isAtEnd() and isDigit(self.peek().?)) {
                _ = self.advance();
            }
        }

        return self.makeTokenAt(if (is_float) .float else .integer, start);
    }

    /// Scan identifier or keyword
    fn scanIdentifierOrKeyword(self: *Self, start: usize) Token {
        while (!self.isAtEnd()) {
            const c = self.peek().?;
            if (isAlphaNumeric(c) or c == '_') {
                _ = self.advance();
            } else {
                break;
            }
        }

        const text = self.source[start..self.position];

        // Check for keyword (case-insensitive)
        if (lookupKeyword(text)) |kw| {
            return Token{
                .token_type = kw,
                .text = text,
                .line = self.line,
                .column = @intCast(start - self.line_start + 1),
            };
        }

        return self.makeTokenAt(.identifier, start);
    }
};

/// Check if character is a digit
fn isDigit(c: u8) bool {
    return c >= '0' and c <= '9';
}

/// Check if character is alphabetic
fn isAlpha(c: u8) bool {
    return (c >= 'a' and c <= 'z') or (c >= 'A' and c <= 'Z');
}

/// Check if character is alphanumeric
fn isAlphaNumeric(c: u8) bool {
    return isAlpha(c) or isDigit(c);
}

/// Lookup keyword (case-insensitive)
fn lookupKeyword(text: []const u8) ?TokenType {
    // Normalize to lowercase for comparison
    var lower_buf: [32]u8 = undefined;
    if (text.len > 32) return null;

    for (text, 0..) |c, i| {
        lower_buf[i] = std.ascii.toLower(c);
    }
    const lower = lower_buf[0..text.len];

    // Keywords
    if (std.mem.eql(u8, lower, "match")) return .kw_match;
    if (std.mem.eql(u8, lower, "where")) return .kw_where;
    if (std.mem.eql(u8, lower, "return")) return .kw_return;
    if (std.mem.eql(u8, lower, "create")) return .kw_create;
    if (std.mem.eql(u8, lower, "delete")) return .kw_delete;
    if (std.mem.eql(u8, lower, "detach")) return .kw_detach;
    if (std.mem.eql(u8, lower, "set")) return .kw_set;
    if (std.mem.eql(u8, lower, "merge")) return .kw_merge;
    if (std.mem.eql(u8, lower, "with")) return .kw_with;
    if (std.mem.eql(u8, lower, "as")) return .kw_as;
    if (std.mem.eql(u8, lower, "order")) return .kw_order;
    if (std.mem.eql(u8, lower, "by")) return .kw_by;
    if (std.mem.eql(u8, lower, "limit")) return .kw_limit;
    if (std.mem.eql(u8, lower, "skip")) return .kw_skip;
    if (std.mem.eql(u8, lower, "and")) return .kw_and;
    if (std.mem.eql(u8, lower, "or")) return .kw_or;
    if (std.mem.eql(u8, lower, "not")) return .kw_not;
    if (std.mem.eql(u8, lower, "null")) return .kw_null;
    if (std.mem.eql(u8, lower, "true")) return .kw_true;
    if (std.mem.eql(u8, lower, "false")) return .kw_false;
    if (std.mem.eql(u8, lower, "in")) return .kw_in;
    if (std.mem.eql(u8, lower, "is")) return .kw_is;
    if (std.mem.eql(u8, lower, "contains")) return .kw_contains;
    if (std.mem.eql(u8, lower, "starts")) return .kw_starts;
    if (std.mem.eql(u8, lower, "ends")) return .kw_ends;
    if (std.mem.eql(u8, lower, "distinct")) return .kw_distinct;
    if (std.mem.eql(u8, lower, "asc")) return .kw_asc;
    if (std.mem.eql(u8, lower, "desc")) return .kw_desc;
    if (std.mem.eql(u8, lower, "xor")) return .kw_xor;

    return null;
}

// ============================================================================
// Tests
// ============================================================================

test "tokenize simple match return" {
    const source = "MATCH (n) RETURN n";
    var lexer = Lexer.init(source);

    try std.testing.expectEqual(TokenType.kw_match, lexer.nextToken().token_type);
    try std.testing.expectEqual(TokenType.lparen, lexer.nextToken().token_type);
    try std.testing.expectEqual(TokenType.identifier, lexer.nextToken().token_type);
    try std.testing.expectEqual(TokenType.rparen, lexer.nextToken().token_type);
    try std.testing.expectEqual(TokenType.kw_return, lexer.nextToken().token_type);
    try std.testing.expectEqual(TokenType.identifier, lexer.nextToken().token_type);
    try std.testing.expectEqual(TokenType.eof, lexer.nextToken().token_type);
}

test "tokenize node pattern with label" {
    const source = "(n:Person)";
    var lexer = Lexer.init(source);

    try std.testing.expectEqual(TokenType.lparen, lexer.nextToken().token_type);
    try std.testing.expectEqual(TokenType.identifier, lexer.nextToken().token_type);
    try std.testing.expectEqual(TokenType.colon, lexer.nextToken().token_type);
    try std.testing.expectEqual(TokenType.identifier, lexer.nextToken().token_type);
    try std.testing.expectEqual(TokenType.rparen, lexer.nextToken().token_type);
}

test "tokenize relationship pattern" {
    const source = "-[:KNOWS]->";
    var lexer = Lexer.init(source);

    try std.testing.expectEqual(TokenType.dash, lexer.nextToken().token_type);
    try std.testing.expectEqual(TokenType.lbracket, lexer.nextToken().token_type);
    try std.testing.expectEqual(TokenType.colon, lexer.nextToken().token_type);
    try std.testing.expectEqual(TokenType.identifier, lexer.nextToken().token_type);
    try std.testing.expectEqual(TokenType.rbracket, lexer.nextToken().token_type);
    try std.testing.expectEqual(TokenType.arrow_right, lexer.nextToken().token_type);
}

test "tokenize vector distance operator" {
    const source = "n.embedding <=> $query";
    var lexer = Lexer.init(source);

    try std.testing.expectEqual(TokenType.identifier, lexer.nextToken().token_type);
    try std.testing.expectEqual(TokenType.dot, lexer.nextToken().token_type);
    try std.testing.expectEqual(TokenType.identifier, lexer.nextToken().token_type);
    try std.testing.expectEqual(TokenType.vector_distance, lexer.nextToken().token_type);
    try std.testing.expectEqual(TokenType.parameter, lexer.nextToken().token_type);
}

test "tokenize fts match operator" {
    const source = "d.text @@ \"search terms\"";
    var lexer = Lexer.init(source);

    try std.testing.expectEqual(TokenType.identifier, lexer.nextToken().token_type);
    try std.testing.expectEqual(TokenType.dot, lexer.nextToken().token_type);
    try std.testing.expectEqual(TokenType.identifier, lexer.nextToken().token_type);
    try std.testing.expectEqual(TokenType.fts_match, lexer.nextToken().token_type);
    try std.testing.expectEqual(TokenType.string, lexer.nextToken().token_type);
}

test "tokenize case insensitive keywords" {
    const cases = [_][]const u8{ "match", "MATCH", "Match", "mAtCh" };
    for (cases) |source| {
        var lexer = Lexer.init(source);
        try std.testing.expectEqual(TokenType.kw_match, lexer.nextToken().token_type);
    }
}

test "tokenize comparison operators" {
    var lexer = Lexer.init("< <= > >= = <> !=");

    try std.testing.expectEqual(TokenType.lt, lexer.nextToken().token_type);
    try std.testing.expectEqual(TokenType.lte, lexer.nextToken().token_type);
    try std.testing.expectEqual(TokenType.gt, lexer.nextToken().token_type);
    try std.testing.expectEqual(TokenType.gte, lexer.nextToken().token_type);
    try std.testing.expectEqual(TokenType.eq, lexer.nextToken().token_type);
    try std.testing.expectEqual(TokenType.neq, lexer.nextToken().token_type);
    try std.testing.expectEqual(TokenType.neq, lexer.nextToken().token_type);
}

test "tokenize numbers" {
    var lexer = Lexer.init("42 3.14 1e10 2.5e-3");

    const int_tok = lexer.nextToken();
    try std.testing.expectEqual(TokenType.integer, int_tok.token_type);
    try std.testing.expectEqualStrings("42", int_tok.text);

    const float_tok = lexer.nextToken();
    try std.testing.expectEqual(TokenType.float, float_tok.token_type);
    try std.testing.expectEqualStrings("3.14", float_tok.text);

    try std.testing.expectEqual(TokenType.float, lexer.nextToken().token_type);
    try std.testing.expectEqual(TokenType.float, lexer.nextToken().token_type);
}

test "tokenize strings" {
    var lexer = Lexer.init("\"hello\" 'world'");

    const str1 = lexer.nextToken();
    try std.testing.expectEqual(TokenType.string, str1.token_type);
    try std.testing.expectEqualStrings("\"hello\"", str1.text);

    const str2 = lexer.nextToken();
    try std.testing.expectEqual(TokenType.string, str2.token_type);
    try std.testing.expectEqualStrings("'world'", str2.text);
}

test "tokenize property map" {
    const source = "{name: \"Alice\", age: 30}";
    var lexer = Lexer.init(source);

    try std.testing.expectEqual(TokenType.lbrace, lexer.nextToken().token_type);
    try std.testing.expectEqual(TokenType.identifier, lexer.nextToken().token_type);
    try std.testing.expectEqual(TokenType.colon, lexer.nextToken().token_type);
    try std.testing.expectEqual(TokenType.string, lexer.nextToken().token_type);
    try std.testing.expectEqual(TokenType.comma, lexer.nextToken().token_type);
    try std.testing.expectEqual(TokenType.identifier, lexer.nextToken().token_type);
    try std.testing.expectEqual(TokenType.colon, lexer.nextToken().token_type);
    try std.testing.expectEqual(TokenType.integer, lexer.nextToken().token_type);
    try std.testing.expectEqual(TokenType.rbrace, lexer.nextToken().token_type);
}

test "line and column tracking" {
    const source = "MATCH\n  (n)";
    var lexer = Lexer.init(source);

    const t1 = lexer.nextToken();
    try std.testing.expectEqual(@as(u32, 1), t1.line);
    try std.testing.expectEqual(@as(u32, 1), t1.column);

    const t2 = lexer.nextToken();
    try std.testing.expectEqual(@as(u32, 2), t2.line);
    try std.testing.expectEqual(@as(u32, 3), t2.column);
}

test "skip comments" {
    const source = "MATCH // this is a comment\n(n)";
    var lexer = Lexer.init(source);

    try std.testing.expectEqual(TokenType.kw_match, lexer.nextToken().token_type);
    try std.testing.expectEqual(TokenType.lparen, lexer.nextToken().token_type);
}

test "tokenize arrows" {
    var lexer = Lexer.init("-> <- -");

    try std.testing.expectEqual(TokenType.arrow_right, lexer.nextToken().token_type);
    try std.testing.expectEqual(TokenType.arrow_left, lexer.nextToken().token_type);
    try std.testing.expectEqual(TokenType.dash, lexer.nextToken().token_type);
}
