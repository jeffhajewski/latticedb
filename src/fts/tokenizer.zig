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
    /// Fold accented Latin letters to their base letters ("café" is stored as
    /// "cafe", "straße" as "strasse") and Greek letters to their unaccented
    /// forms.
    remove_accents: bool = true,
    /// Remove stop words
    remove_stop_words: bool = true,
    /// Apply Porter stemming (reduces words to root form)
    use_stemming: bool = false,
    /// Language for stop words and stemming
    language: Language = .english,
    /// Split and normalize words the way 0.16.0 did: only ASCII letters,
    /// digits and '_' are word characters, so every other byte splits a word
    /// ("Müller" gives "ller"), and terms are ASCII-lowercased with no accent
    /// folding or stemming. An index written that way is read that way, so
    /// its stored terms stay findable; `FtsIndex` sets this from what the
    /// index recorded and callers should leave it alone.
    ascii_only: bool = false,
};

/// The longest term, in bytes, the index stores.
pub const MAX_TERM_LEN = 64;

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
            // Skip non-word characters and scan to the end of the word
            const start = if (self.config.ascii_only) self.scanAsciiWord() else self.scanWord();
            if (start == null) return null;
            const start_offset = start.?;

            const token_text = self.text[start.?..self.pos];
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
            if (self.config.remove_stop_words and !self.config.ascii_only) {
                // Stop words are matched against the term before stemming.
                var fold_buf: [MAX_TERM_LEN]u8 = undefined;
                if (token_len <= MAX_TERM_LEN and
                    stopwords.isStopWord(foldTerm(self.config, token_text, &fold_buf), self.config.language))
                {
                    continue;
                }
            } else if (self.config.remove_stop_words) {
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

    /// Advance past separators and then one word, returning where the word
    /// starts, or null at the end of the text. The 0.16.0 rule: a word is a
    /// run of ASCII letters, digits and '_'.
    fn scanAsciiWord(self: *Self) ?usize {
        while (self.pos < self.text.len and !isWordChar(self.text[self.pos])) self.pos += 1;
        if (self.pos >= self.text.len) return null;
        const start = self.pos;
        while (self.pos < self.text.len and isWordChar(self.text[self.pos])) self.pos += 1;
        return start;
    }

    /// Advance past separators and then one word, returning where the word
    /// starts, or null at the end of the text.
    ///
    /// A word is a run of code points `isWordCodepoint` accepts. Han
    /// ideographs and hiragana are a word each, since those scripts do not
    /// separate words with spaces and a whole sentence would otherwise be one
    /// over-long term. Bytes that are not valid UTF-8 separate words. On
    /// ASCII text this is exactly `scanAsciiWord`.
    fn scanWord(self: *Self) ?usize {
        var cp = decodeAt(self.text, self.pos);
        while (self.pos < self.text.len and !(cp.word)) {
            self.pos += cp.len;
            cp = decodeAt(self.text, self.pos);
        }
        if (self.pos >= self.text.len) return null;
        const start = self.pos;
        self.pos += cp.len;
        if (cp.single) return start;
        while (self.pos < self.text.len) {
            cp = decodeAt(self.text, self.pos);
            if (!cp.word or cp.single) break;
            self.pos += cp.len;
        }
        return start;
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

/// One code point read from text, as the tokenizer classifies it.
const Decoded = struct {
    len: usize,
    cp: u21,
    /// Part of a word.
    word: bool,
    /// A word on its own (an ideograph or hiragana).
    single: bool,
};

fn decodeAt(text: []const u8, pos: usize) Decoded {
    if (pos >= text.len) return .{ .len = 1, .cp = 0, .word = false, .single = false };
    const first = text[pos];
    if (first < 0x80) return .{ .len = 1, .cp = first, .word = isWordChar(first), .single = false };
    const n = std.unicode.utf8ByteSequenceLength(first) catch return .{ .len = 1, .cp = 0, .word = false, .single = false };
    if (pos + n > text.len) return .{ .len = 1, .cp = 0, .word = false, .single = false };
    const cp = std.unicode.utf8Decode(text[pos .. pos + n]) catch return .{ .len = 1, .cp = 0, .word = false, .single = false };
    return .{ .len = n, .cp = cp, .word = isWordCodepoint(cp), .single = isSingleCharWord(cp) };
}

/// Whether a code point can be part of a word: ASCII letters, digits and
/// '_', and above ASCII everything except spaces, punctuation and symbols in
/// the blocks that hold them. Letters, digits and combining marks of every
/// script count, so "Müller", "москва" and "東京" are words.
pub fn isWordCodepoint(cp: u21) bool {
    if (cp < 0x80) return isWordChar(@intCast(cp));
    return switch (cp) {
        // ª µ º are letters; the rest of U+0080..U+00BF, × and ÷ are not.
        0xAA, 0xB5, 0xBA => true,
        0x80...0xA9, 0xAB...0xB4, 0xB6...0xB9, 0xBB...0xBF, 0xD7, 0xF7 => false,
        // Greek, Armenian, Hebrew, Arabic and Devanagari punctuation.
        0x37E, 0x387, 0x55A...0x55F, 0x589, 0x5BE, 0x5C0, 0x5C3, 0x5C6, 0x5F3, 0x5F4 => false,
        0x60C, 0x61B, 0x61F, 0x66A...0x66D, 0x6D4, 0x964, 0x965 => false,
        // General punctuation (including spaces, dashes, quotes, zero-width
        // characters), currency, arrows, mathematical operators, technical
        // symbols, box drawing, shapes, dingbats, supplemental punctuation.
        0x2000...0x206F, 0x20A0...0x20CF, 0x2190...0x2BFF, 0x2E00...0x2E7F => false,
        // CJK punctuation; 々 〆 〇 are letters.
        0x3000...0x3004, 0x3008...0x3020, 0x3030, 0x303D => false,
        // Vertical, small and full-width forms of punctuation; byte order mark.
        0xFE10...0xFE1F, 0xFE30...0xFE6F, 0xFEFF => false,
        0xFF00...0xFF0F, 0xFF1A...0xFF20, 0xFF3B...0xFF40, 0xFF5B...0xFF65 => false,
        // Emoji and other pictographs.
        0x1F000...0x1FAFF => false,
        else => true,
    };
}

/// Han ideographs and hiragana, which are indexed one character per term.
fn isSingleCharWord(cp: u21) bool {
    return switch (cp) {
        0x3041...0x309F, 0x3400...0x4DBF, 0x4E00...0x9FFF, 0xF900...0xFAFF, 0x20000...0x3134F => true,
        else => false,
    };
}

/// Lowercase one code point, for the scripts with case that are written
/// with Latin, Greek, Cyrillic or Armenian letters. Every mapping keeps the
/// UTF-8 length except İ, which becomes the one-byte 'i'.
fn foldCase(cp: u21) u21 {
    return switch (cp) {
        'A'...'Z' => cp + 0x20,
        0xC0...0xD6, 0xD8...0xDE => cp + 0x20,
        0x130 => 'i',
        0x100...0x12F, 0x132...0x137, 0x14A...0x177 => cp | 1,
        0x139...0x148, 0x179...0x17E => if (cp & 1 == 1) cp + 1 else cp,
        0x178 => 0xFF,
        0x1A0, 0x1AF => cp + 1,
        0x1CD...0x1DC => if (cp & 1 == 1) cp + 1 else cp,
        0x218...0x21B => cp | 1,
        0x386 => 0x3AC,
        0x388...0x38A => cp + 0x25,
        0x38C => 0x3CC,
        0x38E...0x38F => cp + 0x3F,
        0x391...0x3A1, 0x3A3...0x3AB => cp + 0x20,
        0x400...0x40F => cp + 0x50,
        0x410...0x42F => cp + 0x20,
        0x460...0x481, 0x48A...0x4BF, 0x4D0...0x52F => cp | 1,
        0x4C1...0x4CE => if (cp & 1 == 1) cp + 1 else cp,
        0x531...0x556 => cp + 0x30,
        0x1E00...0x1E95, 0x1EA0...0x1EFF => cp | 1,
        0x1E9E => 0xDF,
        0xFF21...0xFF3A => cp + 0x20,
        else => cp,
    };
}

/// The lowercase form of one code point, as `toLower` in a query writes it:
/// the same table full-text search folds words with, so a string the query
/// lowercases matches what the index stored.
pub fn lowerCodepoint(cp: u21) u21 {
    return foldCase(cp);
}

/// The uppercase form of one code point: `foldCase` run backwards, over the
/// same scripts. A letter with no single uppercase code point, like 'ß',
/// keeps its case. Every mapping keeps the UTF-8 length except 'ı', which
/// becomes the one-byte 'I'.
pub fn upperCodepoint(cp: u21) u21 {
    return switch (cp) {
        'a'...'z' => cp - 0x20,
        0xE0...0xF6, 0xF8...0xFE => cp - 0x20,
        0xFF => 0x178,
        0x131 => 'I',
        0x101...0x12F, 0x133...0x137, 0x14B...0x177 => if (cp & 1 == 1) cp - 1 else cp,
        0x13A...0x148, 0x17A...0x17E => if (cp & 1 == 0) cp - 1 else cp,
        0x1A1, 0x1B0 => cp - 1,
        0x1CE...0x1DC => if (cp & 1 == 0) cp - 1 else cp,
        0x219, 0x21B => cp - 1,
        0x3AC => 0x386,
        0x3AD...0x3AF => cp - 0x25,
        0x3CC => 0x38C,
        0x3CD...0x3CE => cp - 0x3F,
        0x3C2 => 0x3A3,
        0x3B1...0x3C1, 0x3C3...0x3CB => cp - 0x20,
        0x450...0x45F => cp - 0x50,
        0x430...0x44F => cp - 0x20,
        0x461...0x481, 0x48B...0x4BF, 0x4D1...0x52F => if (cp & 1 == 1) cp - 1 else cp,
        0x4C2...0x4CE => if (cp & 1 == 0) cp - 1 else cp,
        0x561...0x586 => cp - 0x30,
        0x1E01...0x1E95, 0x1EA1...0x1EFF => if (cp & 1 == 1) cp - 1 else cp,
        0xFF41...0xFF5A => cp - 0x20,
        else => cp,
    };
}

/// The unaccented form of a lowercase letter, or null when it has none.
/// Latin letters fold to ASCII ("ß" to "ss", "æ" to "ae"); Greek letters
/// lose their tonos and dialytika. Never longer than the letter's UTF-8.
fn foldAccent(cp: u21) ?[]const u8 {
    const latin1 = "aaaaaaeceeeeiiiidnooooo/ouuuuyty";
    return switch (cp) {
        0xDF => "ss",
        0xE6 => "ae",
        0xFE => "th",
        0xE0...0xE5, 0xE7...0xF6, 0xF8...0xFD, 0xFF => latin1[cp - 0xE0 ..][0..1],
        0x100...0x105 => "a",
        0x106...0x10D => "c",
        0x10E...0x111 => "d",
        0x112...0x11B => "e",
        0x11C...0x123 => "g",
        0x124...0x127 => "h",
        0x128...0x131 => "i",
        0x132...0x133 => "ij",
        0x134...0x135 => "j",
        0x136...0x138 => "k",
        0x139...0x142 => "l",
        0x143...0x14B => "n",
        0x14C...0x151 => "o",
        0x152...0x153 => "oe",
        0x154...0x159 => "r",
        0x15A...0x161, 0x17F => "s",
        0x162...0x167 => "t",
        0x168...0x173 => "u",
        0x174...0x175 => "w",
        0x176...0x178 => "y",
        0x179...0x17E => "z",
        0x1A0...0x1A1 => "o",
        0x1AF...0x1B0 => "u",
        0x1CD...0x1CE => "a",
        0x1CF...0x1D0 => "i",
        0x1D1...0x1D2 => "o",
        0x1D3...0x1DC => "u",
        0x218...0x219 => "s",
        0x21A...0x21B => "t",
        0x1E00...0x1E01, 0x1E9A, 0x1EA0...0x1EB7 => "a",
        0x1E02...0x1E07 => "b",
        0x1E08...0x1E09 => "c",
        0x1E0A...0x1E13 => "d",
        0x1E14...0x1E1D, 0x1EB8...0x1EC7 => "e",
        0x1E1E...0x1E1F => "f",
        0x1E20...0x1E21 => "g",
        0x1E22...0x1E2B, 0x1E96 => "h",
        0x1E2C...0x1E2F, 0x1EC8...0x1ECB => "i",
        0x1E30...0x1E35 => "k",
        0x1E36...0x1E3D => "l",
        0x1E3E...0x1E43 => "m",
        0x1E44...0x1E4B => "n",
        0x1E4C...0x1E53, 0x1ECC...0x1EE3 => "o",
        0x1E54...0x1E57 => "p",
        0x1E58...0x1E5F => "r",
        0x1E60...0x1E69, 0x1E9B => "s",
        0x1E6A...0x1E71, 0x1E97 => "t",
        0x1E72...0x1E7B, 0x1EE4...0x1EF1 => "u",
        0x1E7C...0x1E7F => "v",
        0x1E80...0x1E89, 0x1E98 => "w",
        0x1E8A...0x1E8D => "x",
        0x1E8E...0x1E8F, 0x1E99, 0x1EF2...0x1EF9 => "y",
        0x1E90...0x1E95 => "z",
        0x1E9E, 0x1E9F => "ss",
        0x3AC => "\u{3B1}",
        0x3AD => "\u{3B5}",
        0x3AE => "\u{3B7}",
        0x3AF, 0x390, 0x3CA => "\u{3B9}",
        0x3CC => "\u{3BF}",
        0x3CD, 0x3B0, 0x3CB => "\u{3C5}",
        0x3CE => "\u{3C9}",
        else => null,
    };
}

/// Case-fold a word, and fold its accents when the config asks, without
/// stemming. `text` must fit in `buf`; the result is never longer.
pub fn foldTerm(config: TokenizerConfig, text: []const u8, buf: *[MAX_TERM_LEN]u8) []const u8 {
    std.debug.assert(text.len <= buf.len);
    var out: usize = 0;
    var i: usize = 0;
    while (i < text.len) {
        const d = decodeAt(text, i);
        if (text[i] < 0x80 or d.cp == 0) {
            // ASCII, or a byte that is not UTF-8, which is kept as it is.
            buf[out] = std.ascii.toLower(text[i]);
            out += 1;
            i += 1;
            continue;
        }
        i += d.len;
        const cp = foldCase(d.cp);
        if (config.remove_accents) {
            // Combining diacritical marks, as decomposed text spells accents.
            if (cp >= 0x300 and cp <= 0x36F) continue;
            if (foldAccent(cp)) |plain| {
                @memcpy(buf[out..][0..plain.len], plain);
                out += plain.len;
                continue;
            }
        }
        out += std.unicode.utf8Encode(cp, buf[out..]) catch unreachable;
    }
    return buf[0..out];
}

/// The term a token is stored under and looked up by: lowercased, then,
/// as configured, accent-folded and stemmed. Null when the token is too long
/// to be a term or folds away to nothing.
///
/// Index time and query time must both come through here with the same
/// config, or a word is stored as one term and searched for as another.
pub fn normalizeTerm(config: TokenizerConfig, text: []const u8, buf: *[MAX_TERM_LEN]u8) ?[]const u8 {
    if (text.len > buf.len) return null;
    if (config.ascii_only) {
        for (text, 0..) |c, i| buf[i] = std.ascii.toLower(c);
        return buf[0..text.len];
    }
    const folded = foldTerm(config, text, buf);
    if (folded.len == 0) return null;
    if (!config.use_stemming) return folded;
    var stem_buf: [MAX_TERM_LEN]u8 = undefined;
    const stemmed = stemmer.stemWithLanguage(folded, &stem_buf, config.language);
    std.mem.copyForwards(u8, buf[0..stemmed.len], stemmed);
    return buf[0..stemmed.len];
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

    // All languages use English stop words for binary size.
    // "Der" is NOT an English stop word, so all 3 tokens remain.
    try std.testing.expectEqual(@as(usize, 3), tokens.len);
    try std.testing.expectEqualStrings("Der", tokens[0].text);
    try std.testing.expectEqualStrings("schnelle", tokens[1].text);
    try std.testing.expectEqualStrings("Fuchs", tokens[2].text);
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

    // All languages use English stop words for binary size.
    // "Le" is NOT an English stop word, so all 3 tokens remain.
    try std.testing.expectEqual(@as(usize, 3), tokens.len);
    try std.testing.expectEqualStrings("Le", tokens[0].text);
    try std.testing.expectEqualStrings("chat", tokens[1].text);
    try std.testing.expectEqualStrings("noir", tokens[2].text);
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

    // All languages use English stop words now.
    // "the" is filtered (English stop word), "cat" stays, "der" stays (not English stop word)
    try std.testing.expectEqual(@as(usize, 2), tokens_de.len);
    try std.testing.expectEqualStrings("cat", tokens_de[0].text);
    try std.testing.expectEqualStrings("der", tokens_de[1].text);

    // English tokenizer should also filter "the" but not "der"
    var tokenizer_en = Tokenizer.init(allocator, text, .{
        .remove_stop_words = true,
        .min_token_length = 2,
        .language = .english,
    });

    const tokens_en = try tokenizer_en.tokenizeAll();
    defer allocator.free(tokens_en);

    // Same behavior: "the" filtered, "cat" stays, "der" stays
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

/// The terms text produces: tokens through `normalizeTerm`, joined by '|'.
fn termsOf(config: TokenizerConfig, text: []const u8, out: []u8) ![]const u8 {
    var tok = Tokenizer.init(std.testing.allocator, text, config);
    var w: std.Io.Writer = .fixed(out);
    var first = true;
    while (tok.next()) |token| {
        var buf: [MAX_TERM_LEN]u8 = undefined;
        const term = normalizeTerm(config, token.text, &buf) orelse continue;
        if (!first) try w.writeByte('|');
        first = false;
        try w.writeAll(term);
    }
    return w.buffered();
}

test "unicode words are words, case-folded and accent-folded" {
    var out: [256]u8 = undefined;
    try std.testing.expectEqualStrings(
        "muller|zoller|cafe|naive|strasse|москва|東|京|ελλαδα",
        try termsOf(.{}, "Müller, Zöller; CAFÉ naïve Straße «Москва» 東京。 Ελλάδα", &out),
    );
    // Decomposed accents fold the same as precomposed ones.
    try std.testing.expectEqualStrings("cafe", try termsOf(.{}, "cafe\u{301}", &out));
    // Without accent folding, case still folds and the accents stay.
    try std.testing.expectEqualStrings("müller|café", try termsOf(.{ .remove_accents = false }, "MÜLLER Café", &out));
    // Bytes that are not UTF-8 separate words rather than joining them.
    try std.testing.expectEqualStrings("ab|cd", try termsOf(.{}, "ab\xffcd\xc3", &out));
    // Emoji and general punctuation are separators.
    try std.testing.expectEqualStrings("hello|world", try termsOf(.{}, "hello\u{1F600}world\u{2014}", &out));
}

test "ascii_only splits words and folds case as 0.16.0 did" {
    var out: [256]u8 = undefined;
    try std.testing.expectEqualStrings("ller|caf", try termsOf(.{ .ascii_only = true }, "Müller café Москва", &out));
    // Settings 0.16.0 ignored are still ignored, so its terms stay the same.
    try std.testing.expectEqualStrings("running", try termsOf(.{ .ascii_only = true, .use_stemming = true }, "Running", &out));
}

test "ascii text gives the same tokens and terms in both modes" {
    var prng = std.Random.DefaultPrng.init(0x5eed);
    const rnd = prng.random();
    const alphabet = "abcXYZ019_ .,;:-'\"!?()\t\n@/";
    var text: [200]u8 = undefined;
    for (0..2000) |_| {
        const len = rnd.uintLessThan(usize, text.len);
        for (text[0..len]) |*c| c.* = alphabet[rnd.uintLessThan(usize, alphabet.len)];
        var a = Tokenizer.init(std.testing.allocator, text[0..len], .{ .ascii_only = true });
        var b = Tokenizer.init(std.testing.allocator, text[0..len], .{});
        while (true) {
            const x = a.next();
            const y = b.next();
            try std.testing.expectEqual(x == null, y == null);
            const tx = x orelse break;
            const ty = y.?;
            try std.testing.expectEqualStrings(tx.text, ty.text);
            try std.testing.expectEqual(tx.position, ty.position);
            try std.testing.expectEqual(tx.byte_offset, ty.byte_offset);
            var bx: [MAX_TERM_LEN]u8 = undefined;
            var by: [MAX_TERM_LEN]u8 = undefined;
            const nx = normalizeTerm(.{ .ascii_only = true }, tx.text, &bx);
            const ny = normalizeTerm(.{}, ty.text, &by);
            try std.testing.expectEqual(nx == null, ny == null);
            if (nx) |n| try std.testing.expectEqualStrings(n, ny.?);
        }
    }
}

test "case and accent folding never lengthen a word" {
    var cp: u21 = 0x80;
    while (cp < 0x3000) : (cp += 1) {
        if (cp >= 0xD800 and cp <= 0xDFFF) continue;
        var in: [4]u8 = undefined;
        const n = std.unicode.utf8Encode(cp, &in) catch continue;
        var buf: [MAX_TERM_LEN]u8 = undefined;
        try std.testing.expect(foldTerm(.{}, in[0..n], &buf).len <= n);
        try std.testing.expect(foldTerm(.{ .remove_accents = false }, in[0..n], &buf).len <= n);
    }
}

test "arbitrary bytes tokenize and normalize without overrunning a term" {
    var prng = std.Random.DefaultPrng.init(0xb17e5);
    const rnd = prng.random();
    // Mostly multi-byte lead and continuation bytes, so sequences are often
    // valid, often truncated and sometimes over-long.
    const pool = "aZ_ .\xc3\xa9\xc3\x9f\xe1\xba\x9e\xc4\xb0\xd0\x9c\xe6\x9d\xb1\xf0\x9f\x98\x80\xcc\x81\xff\x80";
    var text: [300]u8 = undefined;
    for (0..3000) |_| {
        const len = rnd.uintLessThan(usize, text.len);
        for (text[0..len]) |*c| c.* = pool[rnd.uintLessThan(usize, pool.len)];
        for ([_]TokenizerConfig{ .{}, .{ .use_stemming = true }, .{ .remove_accents = false }, .{ .ascii_only = true } }) |config| {
            var tok = Tokenizer.init(std.testing.allocator, text[0..len], config);
            while (tok.next()) |token| {
                try std.testing.expect(token.text.len >= config.min_token_length);
                var buf: [MAX_TERM_LEN]u8 = undefined;
                if (normalizeTerm(config, token.text, &buf)) |term| {
                    try std.testing.expect(term.len <= token.text.len);
                }
            }
        }
    }
}
