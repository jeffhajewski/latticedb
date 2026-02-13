//! Stop Words for Full-Text Search.
//!
//! Contains stop word list for English used in full-text search.
//! Stop words are common words filtered during tokenization to improve
//! search relevance and reduce index size.
//!
//! Sources: NLTK, Snowball, and ISO standard stop word lists.

const std = @import("std");
const tokenizer = @import("tokenizer.zig");
const Language = tokenizer.Language;

// ============================================================================
// English Stop Words (100 words)
// ============================================================================

pub const ENGLISH_STOP_WORDS = [_][]const u8{
    "a",      "an",     "and",    "are",    "as",     "at",     "be",     "by",
    "for",    "from",   "has",    "have",   "he",     "in",     "is",     "it",
    "its",    "of",     "on",     "or",     "that",   "the",    "to",     "was",
    "were",   "will",   "with",   "this",   "but",    "they",   "had",    "not",
    "you",    "which",  "can",    "if",     "their",  "said",   "each",   "she",
    "do",     "how",    "we",     "so",     "up",     "out",    "about",  "who",
    "been",   "would",  "there",  "what",   "when",   "your",   "all",    "no",
    "just",   "more",   "some",   "into",   "than",   "could",  "other",  "then",
    "only",   "over",   "such",   "our",    "also",   "may",    "these",  "after",
    "any",    "most",   "very",   "where",  "much",   "should", "those",  "being",
    "because","before", "between","both",   "come",   "could",  "did",    "does",
    "done",   "during", "get",    "got",    "him",    "her",    "here",   "his",
    "i",      "me",     "my",     "myself",
};

// ============================================================================
// Language Dispatch Functions
// ============================================================================

/// Get the stop word list for a language.
/// Currently only English stop words are included to minimize binary size.
/// All languages fall back to the English stop word list.
pub fn getStopWords(language: Language) []const []const u8 {
    _ = language;
    return &ENGLISH_STOP_WORDS;
}

/// Check if a token is a stop word for the given language
pub fn isStopWord(token: []const u8, language: Language) bool {
    const stop_words = getStopWords(language);
    for (stop_words) |stop_word| {
        if (std.mem.eql(u8, token, stop_word)) {
            return true;
        }
    }
    return false;
}

/// Get the number of stop words for a language
pub fn stopWordCount(language: Language) usize {
    return getStopWords(language).len;
}

// ============================================================================
// Tests
// ============================================================================

test "english stop words" {
    try std.testing.expect(isStopWord("the", .english));
    try std.testing.expect(isStopWord("and", .english));
    try std.testing.expect(isStopWord("is", .english));
    try std.testing.expect(!isStopWord("hello", .english));
    try std.testing.expect(!isStopWord("database", .english));
}

test "stop word counts" {
    try std.testing.expect(stopWordCount(.english) >= 80);
}

test "all languages return english stop words" {
    const english = getStopWords(.english);
    try std.testing.expectEqual(english, getStopWords(.german));
    try std.testing.expectEqual(english, getStopWords(.french));
    try std.testing.expectEqual(english, getStopWords(.spanish));
    try std.testing.expectEqual(english, getStopWords(.russian));
}
