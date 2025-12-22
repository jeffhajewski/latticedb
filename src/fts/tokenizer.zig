//! Full-text search tokenizer for Lattice database.
//!
//! Provides text tokenization and normalization for the inverted index.

const std = @import("std");

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
