//! Full-text search tokenizer for Lattice database.
//!
//! Provides text tokenization and normalization for the inverted index.
//! Supports configurable tokenization with stop word filtering and normalization.

const std = @import("std");
const Allocator = std.mem.Allocator;

/// Token type classification
pub const TokenType = enum {
    /// Regular word token
    word,
    /// Numeric token
    number,
    /// Email address
    email,
    /// URL
    url,
    /// Punctuation (typically filtered)
    punctuation,
    /// Whitespace (typically filtered)
    whitespace,
};

/// A single token from text
pub const Token = struct {
    /// Token text (normalized)
    text: []const u8,
    /// Original position in source text
    position: u32,
    /// Byte offset in source
    byte_offset: u32,
    /// Token type
    token_type: TokenType,
};

/// Tokenizer configuration
pub const TokenizerConfig = struct {
    /// Minimum token length to include
    min_token_length: u8 = 2,
    /// Maximum token length to include
    max_token_length: u8 = 64,
    /// Convert to lowercase
    lowercase: bool = true,
    /// Remove accents/diacritics
    remove_accents: bool = true,
    /// Remove stop words
    remove_stop_words: bool = true,
    /// Language for stop words and stemming
    language: Language = .english,
};

/// Supported languages
pub const Language = enum {
    english,
    german,
    french,
    spanish,
    italian,
    portuguese,
    dutch,
    swedish,
    norwegian,
    danish,
    finnish,
    russian,
};

/// BM25 scoring parameters
pub const Bm25Config = struct {
    /// Term frequency saturation parameter
    k1: f32 = 1.2,
    /// Length normalization parameter
    b: f32 = 0.75,
};

/// Posting list entry
pub const Posting = struct {
    /// Document (node) ID
    doc_id: u64,
    /// Term frequency in document
    term_freq: u32,
    /// Positions of term in document
    positions: []const u32,
};

/// Term dictionary entry
pub const TermEntry = struct {
    /// Term text
    term: []const u8,
    /// Document frequency (number of docs containing term)
    doc_freq: u32,
    /// Page ID of posting list
    posting_page: u32,
};

// ============================================================================
// Stop Words
// ============================================================================

/// Common English stop words (filtered during tokenization)
pub const ENGLISH_STOP_WORDS = [_][]const u8{
    "a",     "an",    "and",   "are",   "as",    "at",    "be",    "by",
    "for",   "from",  "has",   "have",  "he",    "in",    "is",    "it",
    "its",   "of",    "on",    "or",    "that",  "the",   "to",    "was",
    "were",  "will",  "with",  "this",  "but",   "they",  "had",   "not",
    "you",   "which", "can",   "if",    "their", "said",  "each",  "she",
    "do",    "how",   "we",    "so",    "up",    "out",   "about", "who",
    "been",  "would", "there", "what",  "when",  "your",  "all",   "no",
    "just",  "more",  "some",  "into",  "than",  "could", "other", "then",
    "only",  "over",  "such",  "our",   "also",  "may",   "these", "after",
    "any",   "most",  "very",  "where", "much",  "should","those", "being",
};

/// Check if a token is a stop word
pub fn isStopWord(token: []const u8) bool {
    for (&ENGLISH_STOP_WORDS) |stop_word| {
        if (std.mem.eql(u8, token, stop_word)) {
            return true;
        }
    }
    return false;
}

// ============================================================================
// Tokenizer
// ============================================================================

/// Streaming tokenizer for text
pub const Tokenizer = struct {
    allocator: Allocator,
    config: TokenizerConfig,
    text: []const u8,
    pos: usize,
    byte_offset: usize,
    token_position: u32,

    const Self = @This();

    /// Initialize a tokenizer for the given text
    pub fn init(allocator: Allocator, text: []const u8, config: TokenizerConfig) Self {
        return Self{
            .allocator = allocator,
            .config = config,
            .text = text,
            .pos = 0,
            .byte_offset = 0,
            .token_position = 0,
        };
    }

    /// Get the next token from the text
    /// Returns null when no more tokens are available
    pub fn next(self: *Self) ?Token {
        while (self.pos < self.text.len) {
            // Skip non-word characters
            while (self.pos < self.text.len and !isWordChar(self.text[self.pos])) {
                self.pos += 1;
            }

            if (self.pos >= self.text.len) {
                return null;
            }

            // Record start of token
            const start = self.pos;
            const start_offset = self.pos;

            // Scan to end of token
            while (self.pos < self.text.len and isWordChar(self.text[self.pos])) {
                self.pos += 1;
            }

            const token_text = self.text[start..self.pos];
            const token_len = token_text.len;

            // Check length constraints
            if (token_len < self.config.min_token_length or
                token_len > self.config.max_token_length)
            {
                continue;
            }

            // Determine token type
            const token_type = classifyToken(token_text);

            // Skip stop words if configured
            if (self.config.remove_stop_words) {
                // Need to check lowercase version for stop words
                if (self.config.lowercase) {
                    var lower_buf: [64]u8 = undefined;
                    if (token_len <= 64) {
                        const lower = toLowerSlice(token_text, &lower_buf);
                        if (isStopWord(lower)) {
                            continue;
                        }
                    }
                } else {
                    if (isStopWord(token_text)) {
                        continue;
                    }
                }
            }

            const position = self.token_position;
            self.token_position += 1;

            return Token{
                .text = token_text,
                .position = position,
                .byte_offset = @intCast(start_offset),
                .token_type = token_type,
            };
        }

        return null;
    }

    /// Tokenize all text and return array of tokens
    /// Caller owns the returned slice and must free it
    pub fn tokenizeAll(self: *Self) ![]Token {
        // First pass: count tokens
        const saved_pos = self.pos;
        const saved_offset = self.byte_offset;
        const saved_token_pos = self.token_position;

        var count: usize = 0;
        while (self.next() != null) {
            count += 1;
        }

        // Reset for second pass
        self.pos = saved_pos;
        self.byte_offset = saved_offset;
        self.token_position = saved_token_pos;

        // Allocate exact size needed
        const tokens = try self.allocator.alloc(Token, count);
        errdefer self.allocator.free(tokens);

        // Second pass: collect tokens
        var i: usize = 0;
        while (self.next()) |token| {
            tokens[i] = token;
            i += 1;
        }

        return tokens;
    }

    /// Reset the tokenizer to the beginning
    pub fn reset(self: *Self) void {
        self.pos = 0;
        self.byte_offset = 0;
        self.token_position = 0;
    }
};

/// Classify a token based on its content
fn classifyToken(text: []const u8) TokenType {
    if (text.len == 0) return .word;

    var all_digits = true;
    var has_at = false;
    var has_dot = false;
    var has_slash = false;

    for (text) |c| {
        if (!std.ascii.isDigit(c)) {
            all_digits = false;
        }
        if (c == '@') has_at = true;
        if (c == '.') has_dot = true;
        if (c == '/') has_slash = true;
    }

    if (all_digits) return .number;
    if (has_at and has_dot) return .email;
    if (has_slash and has_dot) return .url;

    return .word;
}

/// Convert a slice to lowercase in-place into a buffer
fn toLowerSlice(text: []const u8, buf: []u8) []const u8 {
    const len = @min(text.len, buf.len);
    for (text[0..len], 0..) |c, i| {
        buf[i] = std.ascii.toLower(c);
    }
    return buf[0..len];
}

/// Normalize a token: lowercase and allocate a copy
/// Caller owns the returned slice
pub fn normalize(allocator: Allocator, text: []const u8) ![]u8 {
    const result = try allocator.alloc(u8, text.len);
    for (text, 0..) |c, i| {
        result[i] = std.ascii.toLower(c);
    }
    return result;
}

// ============================================================================
// Character Classification
// ============================================================================

/// Check if a character is a word character
pub fn isWordChar(c: u8) bool {
    return std.ascii.isAlphanumeric(c) or c == '_';
}

/// Convert character to lowercase ASCII
pub fn toLowerAscii(c: u8) u8 {
    return std.ascii.toLower(c);
}

test "is word char" {
    try std.testing.expect(isWordChar('a'));
    try std.testing.expect(isWordChar('Z'));
    try std.testing.expect(isWordChar('5'));
    try std.testing.expect(isWordChar('_'));
    try std.testing.expect(!isWordChar(' '));
    try std.testing.expect(!isWordChar('.'));
}

test "to lower ascii" {
    try std.testing.expectEqual(@as(u8, 'a'), toLowerAscii('A'));
    try std.testing.expectEqual(@as(u8, 'z'), toLowerAscii('Z'));
    try std.testing.expectEqual(@as(u8, 'a'), toLowerAscii('a'));
}

test "stop word detection" {
    try std.testing.expect(isStopWord("the"));
    try std.testing.expect(isStopWord("and"));
    try std.testing.expect(isStopWord("is"));
    try std.testing.expect(!isStopWord("hello"));
    try std.testing.expect(!isStopWord("world"));
    try std.testing.expect(!isStopWord("machine"));
}

test "tokenizer basic" {
    const allocator = std.testing.allocator;
    const text = "Hello world, this is a test!";

    var tokenizer = Tokenizer.init(allocator, text, .{
        .remove_stop_words = false,
        .min_token_length = 1,
    });

    // First token
    const token1 = tokenizer.next();
    try std.testing.expect(token1 != null);
    try std.testing.expectEqualStrings("Hello", token1.?.text);
    try std.testing.expectEqual(@as(u32, 0), token1.?.position);

    // Second token
    const token2 = tokenizer.next();
    try std.testing.expect(token2 != null);
    try std.testing.expectEqualStrings("world", token2.?.text);
    try std.testing.expectEqual(@as(u32, 1), token2.?.position);
}

test "tokenizer with stop words" {
    const allocator = std.testing.allocator;
    const text = "The quick brown fox jumps over the lazy dog";

    var tokenizer = Tokenizer.init(allocator, text, .{
        .remove_stop_words = true,
        .min_token_length = 2,
    });

    const tokens = try tokenizer.tokenizeAll();
    defer allocator.free(tokens);

    // "the" (x2), "over" are filtered out as stop words
    // "quick", "brown", "fox", "jumps", "lazy", "dog" should remain (6 tokens)
    try std.testing.expectEqual(@as(usize, 6), tokens.len);
    try std.testing.expectEqualStrings("quick", tokens[0].text);
    try std.testing.expectEqualStrings("brown", tokens[1].text);
    try std.testing.expectEqualStrings("fox", tokens[2].text);
    try std.testing.expectEqualStrings("jumps", tokens[3].text);
    try std.testing.expectEqualStrings("lazy", tokens[4].text);
    try std.testing.expectEqualStrings("dog", tokens[5].text);
}

test "tokenizer length filtering" {
    const allocator = std.testing.allocator;
    const text = "a bb ccc dddd";

    var tokenizer = Tokenizer.init(allocator, text, .{
        .remove_stop_words = false,
        .min_token_length = 2,
        .max_token_length = 3,
    });

    const tokens = try tokenizer.tokenizeAll();
    defer allocator.free(tokens);

    // "a" too short, "dddd" too long
    try std.testing.expectEqual(@as(usize, 2), tokens.len);
    try std.testing.expectEqualStrings("bb", tokens[0].text);
    try std.testing.expectEqualStrings("ccc", tokens[1].text);
}

test "normalize function" {
    const allocator = std.testing.allocator;

    const result = try normalize(allocator, "HeLLo WoRLD");
    defer allocator.free(result);

    try std.testing.expectEqualStrings("hello world", result);
}

test "tokenizer reset" {
    const allocator = std.testing.allocator;
    const text = "hello world";

    var tokenizer = Tokenizer.init(allocator, text, .{
        .remove_stop_words = false,
        .min_token_length = 1,
    });

    // Consume all tokens
    _ = tokenizer.next();
    _ = tokenizer.next();
    try std.testing.expect(tokenizer.next() == null);

    // Reset and verify we can tokenize again
    tokenizer.reset();
    const token = tokenizer.next();
    try std.testing.expect(token != null);
    try std.testing.expectEqualStrings("hello", token.?.text);
}
