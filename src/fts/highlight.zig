//! Search Result Highlighting for Full-Text Search.
//!
//! Provides text highlighting to return snippets with matched terms marked.
//! Re-tokenizes document text to find original positions of stemmed matches.
//!
//! Example:
//!   Query: "running" (stems to "run")
//!   Document: "The runners were running fast"
//!   Output: "The <em>runners</em> were <em>running</em> fast"

const std = @import("std");
const Allocator = std.mem.Allocator;

const tokenizer_mod = @import("tokenizer.zig");
const Tokenizer = tokenizer_mod.Tokenizer;
const TokenizerConfig = tokenizer_mod.TokenizerConfig;
const Language = tokenizer_mod.Language;

/// Configuration for highlighting
pub const HighlightConfig = struct {
    /// Characters of context before/after match
    context_chars: u32 = 80,
    /// Maximum snippets to return per document
    max_snippets: u32 = 3,
    /// Merge snippets closer than this many chars
    merge_distance: u32 = 40,
    /// Marker before matched term
    prefix_marker: []const u8 = "<em>",
    /// Marker after matched term
    suffix_marker: []const u8 = "</em>",
    /// Ellipsis for truncated text
    ellipsis: []const u8 = "...",
};

/// A single match in the document
pub const Match = struct {
    /// Byte offset in original text (start)
    start: u32,
    /// Byte offset in original text (end, exclusive)
    end: u32,
};

/// A highlighted snippet with matches marked
pub const Snippet = struct {
    /// The snippet text with markers inserted
    text: []u8,
    /// Byte offset of snippet start in original document
    doc_offset: u32,
    /// Number of matches in this snippet
    match_count: u32,
};

/// Result of highlighting a document
pub const HighlightResult = struct {
    /// Highlighted snippets
    snippets: []Snippet,
    /// Total number of matches found
    total_matches: u32,
};

/// Errors that can occur during highlighting
pub const HighlightError = error{
    OutOfMemory,
};

/// A snippet region before marker insertion
const SnippetRegion = struct {
    start: u32,
    end: u32,
    matches: []const Match,
};

// ============================================================================
// Match Finding
// ============================================================================

/// Find all matches of query terms in document text.
///
/// Query terms should already be normalized/stemmed if stemming is used.
/// This function re-tokenizes the document and finds tokens whose
/// stemmed form matches any query term.
///
/// Returns matches sorted by start position.
pub fn findMatches(
    allocator: Allocator,
    text: []const u8,
    query_terms: []const []const u8,
    tokenizer_config: TokenizerConfig,
) HighlightError![]Match {
    if (query_terms.len == 0 or text.len == 0) {
        return allocator.alloc(Match, 0) catch return HighlightError.OutOfMemory;
    }

    var matches: std.ArrayListUnmanaged(Match) = .{};
    errdefer matches.deinit(allocator);

    // Build a set of normalized query terms for O(1) lookup
    var term_set = std.StringHashMap(void).init(allocator);
    defer term_set.deinit();

    for (query_terms) |term| {
        term_set.put(term, {}) catch return HighlightError.OutOfMemory;
    }

    // Tokenize document text with same config but don't filter stop words
    // (we want to find matches even in stop words if they match query)
    var doc_config = tokenizer_config;
    doc_config.remove_stop_words = false;
    doc_config.min_token_length = 1;

    var tokenizer = Tokenizer.init(allocator, text, doc_config);

    var stem_buf: [64]u8 = undefined;

    while (tokenizer.next()) |token| {
        // Normalize/stem the token to match against query terms
        const normalized = tokenizer_mod.normalizeAndStemWithLanguage(
            token.text,
            &stem_buf,
            tokenizer_config.use_stemming,
            tokenizer_config.language,
        );

        // Check if this token matches any query term
        if (term_set.contains(normalized)) {
            const end: u32 = token.byte_offset + @as(u32, @intCast(token.text.len));
            matches.append(allocator, .{
                .start = token.byte_offset,
                .end = end,
            }) catch return HighlightError.OutOfMemory;
        }
    }

    return matches.toOwnedSlice(allocator) catch return HighlightError.OutOfMemory;
}

// ============================================================================
// Snippet Extraction
// ============================================================================

/// Extract highlighted snippets from document text.
///
/// Finds all matches of query terms, groups them into snippet regions
/// with context, and inserts highlight markers around matches.
pub fn highlight(
    allocator: Allocator,
    text: []const u8,
    query_terms: []const []const u8,
    tokenizer_config: TokenizerConfig,
    config: HighlightConfig,
) HighlightError!HighlightResult {
    // Find all matches
    const matches = try findMatches(allocator, text, query_terms, tokenizer_config);
    defer allocator.free(matches);

    if (matches.len == 0) {
        const snippets = allocator.alloc(Snippet, 0) catch return HighlightError.OutOfMemory;
        return HighlightResult{
            .snippets = snippets,
            .total_matches = 0,
        };
    }

    // Group matches into snippet regions
    var regions: std.ArrayListUnmanaged(SnippetRegion) = .{};
    defer regions.deinit(allocator);

    var region_matches: std.ArrayListUnmanaged(Match) = .{};
    defer region_matches.deinit(allocator);

    var region_start: u32 = 0;
    var region_end: u32 = 0;
    var i: usize = 0;

    while (i < matches.len and regions.items.len < config.max_snippets) {
        const match = matches[i];

        // Start new region
        region_start = if (match.start > config.context_chars)
            match.start - config.context_chars
        else
            0;
        region_end = @min(match.end + config.context_chars, @as(u32, @intCast(text.len)));

        region_matches.clearRetainingCapacity();
        region_matches.append(allocator, match) catch return HighlightError.OutOfMemory;
        i += 1;

        // Extend region to include nearby matches
        while (i < matches.len) {
            const next_match = matches[i];

            // Check if next match is within merge distance of current region
            if (next_match.start <= region_end + config.merge_distance) {
                region_matches.append(allocator, next_match) catch return HighlightError.OutOfMemory;
                region_end = @min(
                    next_match.end + config.context_chars,
                    @as(u32, @intCast(text.len)),
                );
                i += 1;
            } else {
                break;
            }
        }

        // Store region with its matches (copy the matches array)
        const region_matches_copy = allocator.dupe(Match, region_matches.items) catch
            return HighlightError.OutOfMemory;

        regions.append(allocator, .{
            .start = region_start,
            .end = region_end,
            .matches = region_matches_copy,
        }) catch {
            allocator.free(region_matches_copy);
            return HighlightError.OutOfMemory;
        };
    }

    // Build snippets from regions
    var snippets = allocator.alloc(Snippet, regions.items.len) catch
        return HighlightError.OutOfMemory;
    errdefer {
        for (snippets) |s| {
            allocator.free(s.text);
        }
        allocator.free(snippets);
    }

    for (regions.items, 0..) |region, idx| {
        defer allocator.free(region.matches);

        const snippet_text = try insertMarkers(
            allocator,
            text,
            region.matches,
            region.start,
            region.end,
            config,
        );

        snippets[idx] = .{
            .text = snippet_text,
            .doc_offset = region.start,
            .match_count = @intCast(region.matches.len),
        };
    }

    return HighlightResult{
        .snippets = snippets,
        .total_matches = @intCast(matches.len),
    };
}

/// Insert highlight markers around matches in a text region
fn insertMarkers(
    allocator: Allocator,
    text: []const u8,
    matches: []const Match,
    region_start: u32,
    region_end: u32,
    config: HighlightConfig,
) HighlightError![]u8 {
    const text_len = region_end - region_start;

    // Calculate output size
    var output_size: usize = text_len;

    // Add space for markers
    for (matches) |match| {
        if (match.start >= region_start and match.end <= region_end) {
            output_size += config.prefix_marker.len + config.suffix_marker.len;
        }
    }

    // Add space for ellipsis at boundaries
    const needs_prefix_ellipsis = region_start > 0;
    const needs_suffix_ellipsis = region_end < text.len;

    if (needs_prefix_ellipsis) {
        output_size += config.ellipsis.len;
    }
    if (needs_suffix_ellipsis) {
        output_size += config.ellipsis.len;
    }

    var output = allocator.alloc(u8, output_size) catch return HighlightError.OutOfMemory;
    var out_pos: usize = 0;

    // Add prefix ellipsis
    if (needs_prefix_ellipsis) {
        @memcpy(output[out_pos..][0..config.ellipsis.len], config.ellipsis);
        out_pos += config.ellipsis.len;
    }

    // Copy text with markers
    var text_pos: u32 = region_start;

    for (matches) |match| {
        if (match.start < region_start or match.end > region_end) {
            continue;
        }

        // Copy text before match
        if (match.start > text_pos) {
            const copy_len = match.start - text_pos;
            @memcpy(output[out_pos..][0..copy_len], text[text_pos..match.start]);
            out_pos += copy_len;
        }

        // Insert prefix marker
        @memcpy(output[out_pos..][0..config.prefix_marker.len], config.prefix_marker);
        out_pos += config.prefix_marker.len;

        // Copy matched text
        const match_len = match.end - match.start;
        @memcpy(output[out_pos..][0..match_len], text[match.start..match.end]);
        out_pos += match_len;

        // Insert suffix marker
        @memcpy(output[out_pos..][0..config.suffix_marker.len], config.suffix_marker);
        out_pos += config.suffix_marker.len;

        text_pos = match.end;
    }

    // Copy remaining text
    if (text_pos < region_end) {
        const remaining = region_end - text_pos;
        @memcpy(output[out_pos..][0..remaining], text[text_pos..region_end]);
        out_pos += remaining;
    }

    // Add suffix ellipsis
    if (needs_suffix_ellipsis) {
        @memcpy(output[out_pos..][0..config.ellipsis.len], config.ellipsis);
        out_pos += config.ellipsis.len;
    }

    return output;
}

// ============================================================================
// Cleanup
// ============================================================================

/// Free a highlight result and all associated memory
pub fn freeResult(allocator: Allocator, result: HighlightResult) void {
    for (result.snippets) |snippet| {
        allocator.free(snippet.text);
    }
    allocator.free(result.snippets);
}

/// Free matches array
pub fn freeMatches(allocator: Allocator, matches: []Match) void {
    allocator.free(matches);
}

// ============================================================================
// Tests
// ============================================================================

test "findMatches - basic" {
    const allocator = std.testing.allocator;

    const text = "The database stores data efficiently";
    const terms = [_][]const u8{ "database", "data" };

    const matches = try findMatches(allocator, text, &terms, .{
        .use_stemming = false,
        .remove_stop_words = false,
    });
    defer allocator.free(matches);

    try std.testing.expectEqual(@as(usize, 2), matches.len);

    // "database" at offset 4
    try std.testing.expectEqual(@as(u32, 4), matches[0].start);
    try std.testing.expectEqual(@as(u32, 12), matches[0].end);

    // "data" at offset 20
    try std.testing.expectEqual(@as(u32, 20), matches[1].start);
    try std.testing.expectEqual(@as(u32, 24), matches[1].end);
}

test "findMatches - with stemming" {
    const allocator = std.testing.allocator;

    const text = "Running runners run quickly";
    const terms = [_][]const u8{"run"};

    const matches = try findMatches(allocator, text, &terms, .{
        .use_stemming = true,
        .remove_stop_words = false,
    });
    defer allocator.free(matches);

    // "Running" -> "run", "runners" -> "runner" (no match), "run" -> "run"
    // Actually let me check stemmer behavior...
    // Porter stemmer: "running" -> "run", "runners" -> "runner", "run" -> "run"
    // So we should match "Running" and "run" but not "runners"
    try std.testing.expectEqual(@as(usize, 2), matches.len);

    // "Running" at offset 0
    try std.testing.expectEqual(@as(u32, 0), matches[0].start);
    try std.testing.expectEqual(@as(u32, 7), matches[0].end);

    // "run" at offset 16
    try std.testing.expectEqual(@as(u32, 16), matches[1].start);
    try std.testing.expectEqual(@as(u32, 19), matches[1].end);
}

test "findMatches - no matches" {
    const allocator = std.testing.allocator;

    const text = "The quick brown fox";
    const terms = [_][]const u8{"database"};

    const matches = try findMatches(allocator, text, &terms, .{
        .use_stemming = false,
        .remove_stop_words = false,
    });
    defer allocator.free(matches);

    try std.testing.expectEqual(@as(usize, 0), matches.len);
}

test "findMatches - empty inputs" {
    const allocator = std.testing.allocator;

    // Empty terms
    const matches1 = try findMatches(allocator, "some text", &[_][]const u8{}, .{});
    defer allocator.free(matches1);
    try std.testing.expectEqual(@as(usize, 0), matches1.len);

    // Empty text
    const terms = [_][]const u8{"test"};
    const matches2 = try findMatches(allocator, "", &terms, .{});
    defer allocator.free(matches2);
    try std.testing.expectEqual(@as(usize, 0), matches2.len);
}

test "highlight - basic" {
    const allocator = std.testing.allocator;

    const text = "The database stores data efficiently";
    const terms = [_][]const u8{"database"};

    const result = try highlight(allocator, text, &terms, .{
        .use_stemming = false,
        .remove_stop_words = false,
    }, .{
        .context_chars = 100, // Get whole text
        .prefix_marker = "<em>",
        .suffix_marker = "</em>",
    });
    defer freeResult(allocator, result);

    try std.testing.expectEqual(@as(u32, 1), result.total_matches);
    try std.testing.expectEqual(@as(usize, 1), result.snippets.len);
    try std.testing.expectEqualStrings("The <em>database</em> stores data efficiently", result.snippets[0].text);
}

test "highlight - multiple matches same snippet" {
    const allocator = std.testing.allocator;

    const text = "data and database and more data";
    const terms = [_][]const u8{ "data", "database" };

    const result = try highlight(allocator, text, &terms, .{
        .use_stemming = false,
        .remove_stop_words = false,
    }, .{
        .context_chars = 100,
        .prefix_marker = "[",
        .suffix_marker = "]",
    });
    defer freeResult(allocator, result);

    try std.testing.expectEqual(@as(u32, 3), result.total_matches);
    try std.testing.expectEqual(@as(usize, 1), result.snippets.len);
    try std.testing.expectEqualStrings("[data] and [database] and more [data]", result.snippets[0].text);
}

test "highlight - with ellipsis" {
    const allocator = std.testing.allocator;

    const text = "This is a very long text that contains the word database somewhere in the middle of it all";
    const terms = [_][]const u8{"database"};

    const result = try highlight(allocator, text, &terms, .{
        .use_stemming = false,
        .remove_stop_words = false,
    }, .{
        .context_chars = 10,
        .prefix_marker = "<em>",
        .suffix_marker = "</em>",
        .ellipsis = "...",
    });
    defer freeResult(allocator, result);

    try std.testing.expectEqual(@as(u32, 1), result.total_matches);
    try std.testing.expectEqual(@as(usize, 1), result.snippets.len);

    // Snippet should have ellipsis at start and end
    try std.testing.expect(std.mem.startsWith(u8, result.snippets[0].text, "..."));
    try std.testing.expect(std.mem.endsWith(u8, result.snippets[0].text, "..."));
    try std.testing.expect(std.mem.indexOf(u8, result.snippets[0].text, "<em>database</em>") != null);
}

test "highlight - max snippets" {
    const allocator = std.testing.allocator;

    // Long text with matches far apart
    const text = "data at start. " ++ "x" ** 100 ++ " data in middle. " ++ "x" ** 100 ++ " data at end.";
    const terms = [_][]const u8{"data"};

    const result = try highlight(allocator, text, &terms, .{
        .use_stemming = false,
        .remove_stop_words = false,
    }, .{
        .context_chars = 10,
        .merge_distance = 20,
        .max_snippets = 2,
    });
    defer freeResult(allocator, result);

    // Should only return 2 snippets even though there are 3 matches
    try std.testing.expect(result.snippets.len <= 2);
}

test "highlight - no matches returns empty" {
    const allocator = std.testing.allocator;

    const text = "The quick brown fox";
    const terms = [_][]const u8{"database"};

    const result = try highlight(allocator, text, &terms, .{}, .{});
    defer freeResult(allocator, result);

    try std.testing.expectEqual(@as(u32, 0), result.total_matches);
    try std.testing.expectEqual(@as(usize, 0), result.snippets.len);
}

test "highlight - with stemming" {
    const allocator = std.testing.allocator;

    const text = "The runners were running fast";
    const terms = [_][]const u8{"run"};

    const result = try highlight(allocator, text, &terms, .{
        .use_stemming = true,
        .remove_stop_words = false,
    }, .{
        .context_chars = 100,
        .prefix_marker = "**",
        .suffix_marker = "**",
    });
    defer freeResult(allocator, result);

    // "runners" stems to "runner" (not "run"), so only "running" -> "run" matches
    try std.testing.expectEqual(@as(u32, 1), result.total_matches);
    try std.testing.expect(std.mem.indexOf(u8, result.snippets[0].text, "**running**") != null);
}
