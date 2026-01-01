//! Full-text search tokenizer for Lattice database.
//!
//! Provides text tokenization and normalization for the inverted index.
//! Supports configurable tokenization with stop word filtering and normalization.

const std = @import("std");
const Allocator = std.mem.Allocator;
const stemmer = @import("stemmer.zig");
const stopwords = @import("stopwords.zig");

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
    /// Apply Porter stemming (reduces words to root form)
    use_stemming: bool = false,
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

/// Re-export English stop words for backwards compatibility
pub const ENGLISH_STOP_WORDS = stopwords.ENGLISH_STOP_WORDS;

/// Check if a token is a stop word (English only, for backwards compatibility)
/// Use stopwords.isStopWord(token, language) for multi-language support
pub fn isStopWord(token: []const u8) bool {
    return stopwords.isStopWord(token, .english);
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
                        if (stopwords.isStopWord(lower, self.config.language)) {
                            continue;
                        }
                    }
                } else {
                    if (stopwords.isStopWord(token_text, self.config.language)) {
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

/// Normalize and optionally stem a token (English only, for backwards compatibility)
/// Returns the processed token in the output buffer, and length
pub fn normalizeAndStem(
    text: []const u8,
    buf: *[64]u8,
    use_stemming: bool,
) []const u8 {
    return normalizeAndStemWithLanguage(text, buf, use_stemming, .english);
}

/// Normalize and optionally stem a token with language-aware processing
/// Returns the processed token in the output buffer
pub fn normalizeAndStemWithLanguage(
    text: []const u8,
    buf: *[64]u8,
    use_stemming: bool,
    language: Language,
) []const u8 {
    if (text.len == 0 or text.len > 64) {
        return text;
    }

    // First lowercase
    for (text, 0..) |c, i| {
        buf[i] = std.ascii.toLower(c);
    }
    const lowercased = buf[0..text.len];

    // Then stem if requested (language-aware)
    if (use_stemming) {
        var stem_buf: [64]u8 = undefined;
        const stemmed = stemmer.stemWithLanguage(lowercased, &stem_buf, language);
        @memcpy(buf[0..stemmed.len], stemmed);
        return buf[0..stemmed.len];
    }

    return lowercased;
}

/// Normalize, stem, and allocate a copy (English only, for backwards compatibility)
/// Caller owns the returned slice
pub fn normalizeAndStemAlloc(
    allocator: Allocator,
    text: []const u8,
    use_stemming: bool,
) ![]u8 {
    return normalizeAndStemAllocWithLanguage(allocator, text, use_stemming, .english);
}

/// Normalize, stem, and allocate a copy with language-aware processing
/// Caller owns the returned slice
pub fn normalizeAndStemAllocWithLanguage(
    allocator: Allocator,
    text: []const u8,
    use_stemming: bool,
    language: Language,
) ![]u8 {
    var buf: [64]u8 = undefined;
    const processed = normalizeAndStemWithLanguage(text, &buf, use_stemming, language);
    return allocator.dupe(u8, processed);
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

test "normalizeAndStem without stemming" {
    var buf: [64]u8 = undefined;

    // Without stemming, just lowercases
    const result1 = normalizeAndStem("HELLO", &buf, false);
    try std.testing.expectEqualStrings("hello", result1);

    const result2 = normalizeAndStem("Running", &buf, false);
    try std.testing.expectEqualStrings("running", result2);
}

test "normalizeAndStem with stemming" {
    var buf: [64]u8 = undefined;

    // With stemming, reduces to root form
    const result1 = normalizeAndStem("running", &buf, true);
    try std.testing.expectEqualStrings("run", result1);

    const result2 = normalizeAndStem("CONNECTED", &buf, true);
    try std.testing.expectEqualStrings("connect", result2);

    const result3 = normalizeAndStem("happily", &buf, true);
    try std.testing.expectEqualStrings("happili", result3);
}

test "normalizeAndStemAlloc" {
    const allocator = std.testing.allocator;

    const result = try normalizeAndStemAlloc(allocator, "RUNNING", true);
    defer allocator.free(result);
    try std.testing.expectEqualStrings("run", result);
}

test "tokenizer with german stop words" {
    const allocator = std.testing.allocator;
    // "Der schnelle Fuchs" - "Der" is a German stop word
    const text = "Der schnelle Fuchs";

    var tokenizer_de = Tokenizer.init(allocator, text, .{
        .remove_stop_words = true,
        .min_token_length = 2,
        .language = .german,
    });

    const tokens = try tokenizer_de.tokenizeAll();
    defer allocator.free(tokens);

    // "Der" should be filtered as German stop word
    // "schnelle", "Fuchs" should remain
    try std.testing.expectEqual(@as(usize, 2), tokens.len);
    try std.testing.expectEqualStrings("schnelle", tokens[0].text);
    try std.testing.expectEqualStrings("Fuchs", tokens[1].text);
}

test "tokenizer with french stop words" {
    const allocator = std.testing.allocator;
    // "Le chat noir" - "Le" is a French stop word
    const text = "Le chat noir";

    var tokenizer_fr = Tokenizer.init(allocator, text, .{
        .remove_stop_words = true,
        .min_token_length = 2,
        .language = .french,
    });

    const tokens = try tokenizer_fr.tokenizeAll();
    defer allocator.free(tokens);

    // "Le" should be filtered as French stop word
    // "chat", "noir" should remain
    try std.testing.expectEqual(@as(usize, 2), tokens.len);
    try std.testing.expectEqualStrings("chat", tokens[0].text);
    try std.testing.expectEqualStrings("noir", tokens[1].text);
}

test "tokenizer language isolation" {
    const allocator = std.testing.allocator;
    // "the cat" - "the" is English, should NOT be filtered for German
    const text = "the cat der";

    // German tokenizer should NOT filter "the"
    var tokenizer_de = Tokenizer.init(allocator, text, .{
        .remove_stop_words = true,
        .min_token_length = 2,
        .language = .german,
    });

    const tokens_de = try tokenizer_de.tokenizeAll();
    defer allocator.free(tokens_de);

    // "the" stays (not German stop word), "cat" stays, "der" filtered
    try std.testing.expectEqual(@as(usize, 2), tokens_de.len);
    try std.testing.expectEqualStrings("the", tokens_de[0].text);
    try std.testing.expectEqualStrings("cat", tokens_de[1].text);

    // English tokenizer should filter "the" but not "der"
    var tokenizer_en = Tokenizer.init(allocator, text, .{
        .remove_stop_words = true,
        .min_token_length = 2,
        .language = .english,
    });

    const tokens_en = try tokenizer_en.tokenizeAll();
    defer allocator.free(tokens_en);

    // "the" filtered, "cat" stays, "der" stays
    try std.testing.expectEqual(@as(usize, 2), tokens_en.len);
    try std.testing.expectEqualStrings("cat", tokens_en[0].text);
    try std.testing.expectEqualStrings("der", tokens_en[1].text);
}

test "normalizeAndStemWithLanguage" {
    var buf: [64]u8 = undefined;

    // English stemming works
    const en_result = normalizeAndStemWithLanguage("RUNNING", &buf, true, .english);
    try std.testing.expectEqualStrings("run", en_result);

    // German stemming doesn't change the word (no German stemmer)
    const de_result = normalizeAndStemWithLanguage("RUNNING", &buf, true, .german);
    try std.testing.expectEqualStrings("running", de_result);
}
