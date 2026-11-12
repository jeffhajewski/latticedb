//! The full-text query language, and matching it against one piece of text.
//!
//! Every way of asking a full-text question goes through `parse`: searching
//! an index, `@@` in Cypher (the index scan, the per-row check, and text a
//! transaction has not committed yet), and `"text" @@ "query"` on a string
//! with no index behind it. So a query means the same thing wherever it is
//! asked.
//!
//! ## Grammar
//!
//! ```
//! query   := branch ("OR" branch)*
//! branch  := unit+
//! unit    := ["-"] (phrase | word)
//! phrase  := '"' text '"'            a closing quote may be left off
//! word    := chars ["*" | "~" digits?]
//! ```
//!
//! - Words side by side are all required (AND). `OR`, in capitals and standing
//!   alone, separates alternatives, and binds looser than the AND: `a b OR c`
//!   is `(a AND b) OR c`. There are no parentheses. An `OR` with nothing on one
//!   side is ignored.
//! - `-word` and `-"a phrase"` exclude documents that match them. The `-` must
//!   start the unit; inside a word (`e-mail`) it is part of the word. An
//!   exclusion applies to its own branch. A branch made only of exclusions is
//!   an error (`InvalidQuery`): "everything except" would have to list every
//!   document. In Cypher, write `NOT x.text @@ "word"` instead.
//! - `"a phrase"` matches its words at consecutive positions, after the
//!   analysis the index applies, so stop words dropped at indexing are dropped
//!   from the phrase too. Inside quotes nothing is an operator. A phrase needs
//!   an index that stores positions; asking one that does not is an error
//!   (`PhraseNeedsPositions`) rather than a silent AND of the words.
//! - `word*` matches every term starting with the word, folded but not stemmed.
//!   A prefix shorter than two characters matches nothing.
//! - `word~` matches terms within two edits of the word, `word~1` within one,
//!   `word~0` the word exactly. Distances above two are taken as two. Words
//!   shorter than four characters are matched exactly.
//! - A backslash makes the next character literal: `\-word`, `\OR`, `word\*`,
//!   `word\~`, and `\"` inside or outside a phrase. Quoting does the same:
//!   `"OR"` is the word "or".
//!
//! Words go through the index's tokenizer, so a word that splits (`well-known`)
//! requires each part, and a word the tokenizer drops (a stop word, a word too
//! short or too long) asks for nothing. A branch whose every word was dropped
//! matches nothing, as a query of only stop words always has.
//!
//! Scores are BM25 summed over a branch's units, and over the branches a
//! document matches. A prefix unit sums its matching terms; a fuzzy unit takes
//! its best match, with the distance penalty.

const std = @import("std");
const Allocator = std.mem.Allocator;

const tokenizer_mod = @import("tokenizer.zig");
const Tokenizer = tokenizer_mod.Tokenizer;
const TokenizerConfig = tokenizer_mod.TokenizerConfig;
const stopwords_mod = @import("stopwords.zig");
const fuzzy_mod = @import("fuzzy.zig");

pub const ParseError = error{ OutOfMemory, InvalidQuery };

/// One requirement of a branch, analysed as the index analyses text.
pub const Unit = union(enum) {
    term: []const u8,
    /// Two or more terms at consecutive positions.
    phrase: []const []const u8,
    /// Every term starting with this.
    prefix: []const u8,
    fuzzy: Fuzzy,

    pub const Fuzzy = struct { term: []const u8, distance: u32 };
};

/// Documents that match every required unit and no exclusion group. An
/// exclusion group rules a document out when it matches every unit in it.
pub const Branch = struct {
    required: []const Unit,
    excluded: []const []const Unit,
};

/// Alternatives: a document matches when it matches any branch.
pub const Query = struct {
    branches: []const Branch,

    /// Whether any unit, required or excluded, is a phrase.
    pub fn hasPhrase(self: Query) bool {
        for (self.branches) |branch| {
            for (branch.required) |unit| if (unit == .phrase) return true;
            for (branch.excluded) |group| {
                for (group) |unit| if (unit == .phrase) return true;
            }
        }
        return false;
    }

    /// The terms of a query that is nothing but required terms, which the
    /// index answers with its plain AND search.
    pub fn plainTerms(self: Query, arena: Allocator) ParseError!?[]const []const u8 {
        if (self.branches.len != 1) return null;
        const branch = self.branches[0];
        if (branch.excluded.len != 0) return null;
        const terms = try arena.alloc([]const u8, branch.required.len);
        for (branch.required, terms) |unit, *t| {
            if (unit != .term) return null;
            t.* = unit.term;
        }
        return terms;
    }

    /// The same requirements with each one an alternative of its own, each
    /// keeping its branch's exclusions: what OR mode makes of side-by-side
    /// words.
    pub fn eachUnitAlone(self: Query, arena: Allocator) ParseError!Query {
        var branches: std.ArrayListUnmanaged(Branch) = .empty;
        for (self.branches) |branch| {
            for (branch.required) |unit| {
                const one = try arena.alloc(Unit, 1);
                one[0] = unit;
                try branches.append(arena, .{ .required = one, .excluded = branch.excluded });
            }
        }
        return .{ .branches = branches.items };
    }

    /// Every required term made fuzzy, as `lattice_fts_search_fuzzy` asks.
    pub fn fuzzyTerms(self: Query, arena: Allocator, max_distance: u32, min_length: u32) ParseError!Query {
        const branches = try arena.alloc(Branch, self.branches.len);
        for (self.branches, branches) |branch, *out| {
            const required = try arena.dupe(Unit, branch.required);
            for (required) |*unit| {
                if (unit.* == .term and max_distance > 0 and unit.term.len >= min_length) {
                    const term = unit.term;
                    unit.* = .{ .fuzzy = .{ .term = term, .distance = max_distance } };
                }
            }
            out.* = .{ .required = required, .excluded = branch.excluded };
        }
        return .{ .branches = branches };
    }
};

pub const ParseOptions = struct {
    /// How the index splits and normalises text.
    analysis: TokenizerConfig,
    min_prefix_length: u32 = 2,
    min_fuzzy_length: u32 = 4,
    default_fuzzy_distance: u32 = 2,
    max_fuzzy_distance: u32 = 2,
};

// ============================================================================
// Lexing
// ============================================================================

const Suffix = union(enum) { none, prefix, fuzzy: ?u32 };

const Lexeme = union(enum) {
    or_op,
    unit: struct {
        excluded: bool,
        quoted: bool,
        /// With escapes removed.
        text: []const u8,
        suffix: Suffix,
    },
};

fn isSpace(c: u8) bool {
    return c == ' ' or c == '\t' or c == '\r' or c == '\n';
}

fn lex(arena: Allocator, text: []const u8) ParseError![]Lexeme {
    var out: std.ArrayListUnmanaged(Lexeme) = .empty;
    var i: usize = 0;
    while (i < text.len) {
        while (i < text.len and isSpace(text[i])) i += 1;
        if (i >= text.len) break;

        var excluded = false;
        if (text[i] == '-' and i + 1 < text.len and !isSpace(text[i + 1])) {
            excluded = true;
            i += 1;
        }

        var buf: std.ArrayListUnmanaged(u8) = .empty;
        if (text[i] == '"') {
            i += 1;
            while (i < text.len and text[i] != '"') {
                if (text[i] == '\\' and i + 1 < text.len) {
                    try buf.append(arena, text[i + 1]);
                    i += 2;
                } else {
                    try buf.append(arena, text[i]);
                    i += 1;
                }
            }
            if (i < text.len) i += 1;
            try out.append(arena, .{ .unit = .{ .excluded = excluded, .quoted = true, .text = buf.items, .suffix = .none } });
            continue;
        }

        // A word runs to whitespace or a quote. `escaped` marks the
        // characters a backslash made literal, so they are never operators.
        var escaped: std.ArrayListUnmanaged(bool) = .empty;
        var any_escape = false;
        while (i < text.len and !isSpace(text[i]) and text[i] != '"') {
            if (text[i] == '\\' and i + 1 < text.len and !isSpace(text[i + 1])) {
                try buf.append(arena, text[i + 1]);
                try escaped.append(arena, true);
                any_escape = true;
                i += 2;
            } else {
                try buf.append(arena, text[i]);
                try escaped.append(arena, false);
                i += 1;
            }
        }
        var word = buf.items;
        if (!excluded and !any_escape and std.mem.eql(u8, word, "OR")) {
            try out.append(arena, .or_op);
            continue;
        }

        var suffix: Suffix = .none;
        if (word.len > 0 and word[word.len - 1] == '*' and !escaped.items[word.len - 1]) {
            suffix = .prefix;
            word = word[0 .. word.len - 1];
        } else {
            var end = word.len;
            while (end > 0 and std.ascii.isDigit(word[end - 1]) and !escaped.items[end - 1]) end -= 1;
            if (end > 0 and word[end - 1] == '~' and !escaped.items[end - 1]) {
                const digits = word[end..];
                const distance: ?u32 = if (digits.len == 0)
                    null
                else
                    std.fmt.parseInt(u32, digits, 10) catch std.math.maxInt(u32);
                suffix = .{ .fuzzy = distance };
                word = word[0 .. end - 1];
            }
        }
        try out.append(arena, .{ .unit = .{ .excluded = excluded, .quoted = false, .text = word, .suffix = suffix } });
    }
    return out.items;
}

// ============================================================================
// Parsing
// ============================================================================

/// Parse `text` into a query whose terms are analysed with
/// `options.analysis`. Everything is allocated in `arena`.
pub fn parse(arena: Allocator, text: []const u8, options: ParseOptions) ParseError!Query {
    const lexemes = try lex(arena, text);

    var branches: std.ArrayListUnmanaged(Branch) = .empty;
    var start: usize = 0;
    while (start <= lexemes.len) {
        var end = start;
        while (end < lexemes.len and lexemes[end] != .or_op) end += 1;
        if (end > start) {
            if (try parseBranch(arena, lexemes[start..end], options)) |branch| {
                try branches.append(arena, branch);
            }
        }
        start = end + 1;
    }
    return .{ .branches = branches.items };
}

/// Null when the branch can match nothing: every word it requires was
/// dropped by analysis, or it requires a prefix too short to use.
fn parseBranch(arena: Allocator, lexemes: []const Lexeme, options: ParseOptions) ParseError!?Branch {
    var written: usize = 0;
    for (lexemes) |lexeme| {
        if (!lexeme.unit.excluded) written += 1;
    }
    if (written == 0) return ParseError.InvalidQuery;

    var required: std.ArrayListUnmanaged(Unit) = .empty;
    var excluded: std.ArrayListUnmanaged([]const Unit) = .empty;
    var impossible = false;
    for (lexemes) |lexeme| {
        const unit = lexeme.unit;
        var units: std.ArrayListUnmanaged(Unit) = .empty;
        const possible = try analyseUnit(arena, unit.text, unit.quoted, unit.suffix, options, &units);
        if (unit.excluded) {
            // An exclusion that can match nothing excludes nothing.
            if (possible and units.items.len > 0) try excluded.append(arena, units.items);
            continue;
        }
        if (!possible) impossible = true;
        for (units.items) |u| {
            if (u == .term and containsTerm(required.items, u.term)) continue;
            try required.append(arena, u);
        }
    }
    if (impossible or required.items.len == 0) return null;
    return .{ .required = required.items, .excluded = excluded.items };
}

fn containsTerm(units: []const Unit, term: []const u8) bool {
    for (units) |u| {
        if (u == .term and std.mem.eql(u8, u.term, term)) return true;
    }
    return false;
}

/// The analysed terms of `text` in order, each with its position.
fn analysedTerms(arena: Allocator, text: []const u8, config: TokenizerConfig) ParseError![]const []const u8 {
    var terms: std.ArrayListUnmanaged([]const u8) = .empty;
    var tok = Tokenizer.init(arena, text, config);
    while (tok.next()) |token| {
        var buf: [tokenizer_mod.MAX_TERM_LEN]u8 = undefined;
        const term = tokenizer_mod.normalizeTerm(config, token.text, &buf) orelse continue;
        try terms.append(arena, try arena.dupe(u8, term));
    }
    return terms.items;
}

/// Append what one written unit requires. False when it can match nothing.
fn analyseUnit(
    arena: Allocator,
    text: []const u8,
    quoted: bool,
    suffix: Suffix,
    options: ParseOptions,
    out: *std.ArrayListUnmanaged(Unit),
) ParseError!bool {
    const config = options.analysis;
    if (quoted) {
        const terms = try analysedTerms(arena, text, config);
        switch (terms.len) {
            0 => {},
            1 => try out.append(arena, .{ .term = terms[0] }),
            else => try out.append(arena, .{ .phrase = terms }),
        }
        return true;
    }
    switch (suffix) {
        .none => {
            for (try analysedTerms(arena, text, config)) |term| try out.append(arena, .{ .term = term });
            return true;
        },
        .fuzzy => |written| {
            const distance = @min(written orelse options.default_fuzzy_distance, options.max_fuzzy_distance);
            for (try analysedTerms(arena, text, config)) |term| {
                if (distance == 0 or term.len < options.min_fuzzy_length) {
                    try out.append(arena, .{ .term = term });
                } else {
                    try out.append(arena, .{ .fuzzy = .{ .term = term, .distance = distance } });
                }
            }
            return true;
        },
        .prefix => return analysePrefix(arena, text, options, out),
    }
}

/// `word*`: the words before the last are plain terms, and the last is the
/// prefix, folded but not stemmed (a stem is not a prefix of the stems of
/// longer words).
fn analysePrefix(arena: Allocator, text: []const u8, options: ParseOptions, out: *std.ArrayListUnmanaged(Unit)) ParseError!bool {
    const config = options.analysis;
    // Every word, so the last can be told apart from the rest.
    var split = config;
    split.min_token_length = 1;
    split.max_token_length = std.math.maxInt(u8);
    split.remove_stop_words = false;

    var last_end: usize = 0;
    var last: ?[]const u8 = null;
    var tok = Tokenizer.init(arena, text, split);
    var words: std.ArrayListUnmanaged([]const u8) = .empty;
    while (tok.next()) |token| {
        try words.append(arena, token.text);
        last_end = token.byte_offset + token.text.len;
    }
    if (words.items.len > 0 and last_end == text.len) last = words.pop();

    for (words.items) |word| {
        if (word.len < config.min_token_length or word.len > config.max_token_length) continue;
        var buf: [tokenizer_mod.MAX_TERM_LEN]u8 = undefined;
        const term = tokenizer_mod.normalizeTerm(config, word, &buf) orelse continue;
        if (config.remove_stop_words and stopwords_mod.isStopWord(term, config.language)) continue;
        try out.append(arena, .{ .term = try arena.dupe(u8, term) });
    }

    const word = last orelse {
        // The '*' followed punctuation ("foo,*"): nothing to extend. But a
        // word too long for the tokenizer to keep ended right at it, and no
        // stored term starts with that.
        if (text.len > 0 and isWordByte(text[text.len - 1])) return false;
        return true;
    };
    var fold = config;
    fold.use_stemming = false;
    var buf: [tokenizer_mod.MAX_TERM_LEN]u8 = undefined;
    // Longer than any stored term: nothing starts with it.
    const folded = tokenizer_mod.normalizeTerm(fold, word, &buf) orelse return false;
    if (folded.len < options.min_prefix_length) return false;
    try out.append(arena, .{ .prefix = try arena.dupe(u8, folded) });
    return true;
}

fn isWordByte(c: u8) bool {
    return std.ascii.isAlphanumeric(c) or c == '_' or c >= 0x80;
}

// ============================================================================
// Matching one text
// ============================================================================

/// A text's terms, with how often and where each occurs, analysed as
/// indexing analyses it.
pub const TextTerms = struct {
    terms: std.StringHashMapUnmanaged(Occurrences) = .empty,
    /// What indexing records as the document's length.
    length: u32 = 0,

    pub const Occurrences = struct {
        freq: u32 = 0,
        positions: std.ArrayListUnmanaged(u32) = .empty,
    };

    pub fn init(arena: Allocator, text: []const u8, config: TokenizerConfig) Allocator.Error!TextTerms {
        var self: TextTerms = .{};
        var tok = Tokenizer.init(arena, text, config);
        while (tok.next()) |token| {
            self.length += 1;
            var buf: [tokenizer_mod.MAX_TERM_LEN]u8 = undefined;
            const term = tokenizer_mod.normalizeTerm(config, token.text, &buf) orelse continue;
            const slot = try self.terms.getOrPut(arena, term);
            if (!slot.found_existing) {
                slot.key_ptr.* = try arena.dupe(u8, term);
                slot.value_ptr.* = .{};
            }
            slot.value_ptr.freq += 1;
            try slot.value_ptr.positions.append(arena, token.position);
        }
        return self;
    }
};

/// What a match is worth, for `evaluate`. `Presence` counts every match as 1.
pub const Presence = struct {
    pub fn term(_: Presence, _: []const u8, _: u32, _: u32) anyerror!f32 {
        return 1;
    }
    pub fn fuzzy(_: Presence, _: []const u8, _: u32, _: u32, _: u32, _: u32) anyerror!f32 {
        return 1;
    }
};

/// The score `text` earns for `query`, or null when it does not match.
/// `scorer` has `term(term, tf, doc_length)` and
/// `fuzzy(term, tf, doc_length, distance, max_distance)`.
pub fn evaluate(query: Query, text: *const TextTerms, scorer: anytype) !?f32 {
    var total: f32 = 0;
    var matched = false;
    branches: for (query.branches) |branch| {
        for (branch.excluded) |group| {
            var all = true;
            for (group) |unit| {
                if (try unitScore(unit, text, scorer) == null) {
                    all = false;
                    break;
                }
            }
            if (all) continue :branches;
        }
        var score: f32 = 0;
        for (branch.required) |unit| {
            score += (try unitScore(unit, text, scorer)) orelse continue :branches;
        }
        total += score;
        matched = true;
    }
    return if (matched) total else null;
}

fn unitScore(unit: Unit, text: *const TextTerms, scorer: anytype) !?f32 {
    switch (unit) {
        .term => |t| {
            const occ = text.terms.get(t) orelse return null;
            return try scorer.term(t, occ.freq, text.length);
        },
        .phrase => |terms| {
            const first = text.terms.get(terms[0]) orelse return null;
            for (terms[1..]) |t| {
                if (text.terms.get(t) == null) return null;
            }
            starts: for (first.positions.items) |start| {
                for (terms[1..], 1..) |t, offset| {
                    const positions = text.terms.get(t).?.positions.items;
                    if (std.mem.indexOfScalar(u32, positions, start + @as(u32, @intCast(offset))) == null) continue :starts;
                }
                var score: f32 = 0;
                for (terms) |t| score += try scorer.term(t, text.terms.get(t).?.freq, text.length);
                return score;
            }
            return null;
        },
        .prefix => |p| {
            var score: f32 = 0;
            var any = false;
            var it = text.terms.iterator();
            while (it.next()) |entry| {
                if (!std.mem.startsWith(u8, entry.key_ptr.*, p)) continue;
                score += try scorer.term(entry.key_ptr.*, entry.value_ptr.freq, text.length);
                any = true;
            }
            return if (any) score else null;
        },
        .fuzzy => |f| {
            var best: ?f32 = null;
            var it = text.terms.iterator();
            while (it.next()) |entry| {
                const distance = fuzzy_mod.levenshteinDistanceBounded(f.term, entry.key_ptr.*, f.distance) orelse continue;
                const score = try scorer.fuzzy(entry.key_ptr.*, entry.value_ptr.freq, text.length, distance, f.distance);
                best = if (best) |b| @max(b, score) else score;
            }
            return best;
        },
    }
}

/// Whether `text` matches `query_text`, both analysed with `config`. For
/// text with no index behind it, as `"some text" @@ "query"` in Cypher.
pub fn textMatches(allocator: Allocator, query_text: []const u8, text: []const u8, config: TokenizerConfig) ParseError!bool {
    var arena_state = std.heap.ArenaAllocator.init(allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const query = try parse(arena, query_text, .{ .analysis = config });
    const terms = try TextTerms.init(arena, text, config);
    return (evaluate(query, &terms, Presence{}) catch |err| switch (err) {
        error.OutOfMemory => return ParseError.OutOfMemory,
        else => unreachable,
    }) != null;
}

// ============================================================================
// Tests
// ============================================================================

fn testConfig() TokenizerConfig {
    var config: TokenizerConfig = .{};
    config.ascii_only = false;
    return config;
}

fn expectMatches(query_text: []const u8, text: []const u8, want: bool) !void {
    const got = try textMatches(std.testing.allocator, query_text, text, testConfig());
    if (got != want) {
        std.debug.print("query \"{s}\" on \"{s}\": got {}, want {}\n", .{ query_text, text, got, want });
        return error.TestUnexpectedResult;
    }
}

test "query: words side by side are all required, and OR separates alternatives" {
    try expectMatches("quick fox", "the quick brown fox", true);
    try expectMatches("quick cat", "the quick brown fox", false);
    try expectMatches("quick cat OR fox", "the quick brown fox", true);
    try expectMatches("cat OR dog", "the quick brown fox", false);
    // AND binds tighter: (cat AND quick) OR dog.
    try expectMatches("cat quick OR dog", "the quick brown fox", false);
    // Only capital OR is an operator; "or" is a stop word.
    try expectMatches("cat or fox", "the quick brown fox", false);
    // A dangling OR is ignored.
    try expectMatches("OR fox OR", "the quick brown fox", true);
}

test "query: exclusions, phrases, prefixes and fuzzy words" {
    try expectMatches("fox -cat", "the quick brown fox", true);
    try expectMatches("fox -quick", "the quick brown fox", false);
    try expectMatches("fox -\"brown quick\"", "the quick brown fox", true);
    try expectMatches("fox -\"quick brown\"", "the quick brown fox", false);
    try expectMatches("\"quick brown\"", "the quick brown fox", true);
    try expectMatches("\"brown quick\"", "the quick brown fox", false);
    // The stop word took no position, so the phrase skips it as indexing did.
    try expectMatches("\"bank of america\"", "Bank of America", true);
    try expectMatches("qui*", "the quick brown fox", true);
    try expectMatches("quo*", "the quick brown fox", false);
    try expectMatches("q*", "the quick brown fox", false);
    try expectMatches("qxicx~", "the quick brown fox", true);
    try expectMatches("qxicx~1", "the quick brown fox", false);
    try expectMatches("qxick~1", "the quick brown fox", true);
    try expectMatches("qxick~0", "the quick brown fox", false);
    // "co-op" requires both parts; '-' excludes only at the start of a unit.
    try expectMatches("co-op", "a co op shop", true);
    try expectMatches("co-op", "an op shop", false);
}

test "query: escapes and quotes make operators literal" {
    try expectMatches("\\-quick", "the quick brown fox", true);
    try expectMatches("quick\\*", "the quick brown fox", true);
    try expectMatches("quic\\*", "the quick brown fox", false);
    try expectMatches("fox \\OR cat", "the quick brown fox", false);
    try expectMatches("\"quick \\\" brown\" fox", "the quick brown fox", true);
    // Operators inside a phrase are text: '-' and '*' are punctuation.
    try expectMatches("\"quick -brown*\"", "the quick brown fox", true);
}

test "query: a branch of only exclusions is refused, and stop words ask for nothing" {
    try std.testing.expectError(ParseError.InvalidQuery, textMatches(std.testing.allocator, "-fox", "fox", testConfig()));
    try std.testing.expectError(ParseError.InvalidQuery, textMatches(std.testing.allocator, "fox OR -cat", "fox", testConfig()));
    try expectMatches("the", "the quick brown fox", false);
    try expectMatches("the fox", "the quick brown fox", true);
    try expectMatches("", "the quick brown fox", false);
    try expectMatches("-the fox", "the quick brown fox", true);
}

test "query: a prefix longer than any stored term matches nothing" {
    var long: [300]u8 = undefined;
    @memset(&long, 'a');
    long[long.len - 1] = '*';
    try expectMatches(&long, "aaaa", false);
    try expectMatches("fox,*", "the quick brown fox", true);
}
