//! Fuzzy matching for full-text search.
//!
//! Implements Levenshtein distance for typo-tolerant search queries.
//! Uses Wagner-Fischer dynamic programming with space optimization.

const std = @import("std");
const Allocator = std.mem.Allocator;

const dictionary_mod = @import("dictionary.zig");
const Dictionary = dictionary_mod.Dictionary;
const DictionaryEntry = dictionary_mod.DictionaryEntry;

/// Maximum supported term length for fuzzy matching
pub const MAX_TERM_LENGTH: usize = 64;

/// Fuzzy match result
pub const FuzzyMatch = struct {
    /// The matched term from dictionary
    term: []const u8,
    /// Edit distance from query term
    distance: u32,
    /// Dictionary entry for the matched term
    entry: DictionaryEntry,
};

/// Configuration for fuzzy search
pub const FuzzyConfig = struct {
    /// Maximum edit distance (1-2 recommended)
    max_distance: u32 = 2,
    /// Minimum term length to apply fuzzy matching (short terms match too many)
    min_term_length: u32 = 4,
};

// ============================================================================
// Levenshtein Distance
// ============================================================================

/// Compute Levenshtein edit distance between two strings.
/// Uses Wagner-Fischer algorithm with rolling array for O(min(m,n)) space.
///
/// Operations counted:
/// - Insertion: adding a character
/// - Deletion: removing a character
/// - Substitution: replacing a character
///
/// Returns the minimum number of single-character edits needed to transform a into b.
pub fn levenshteinDistance(a: []const u8, b: []const u8) u32 {
    // Optimization: ensure a is the shorter string for better cache locality
    if (a.len > b.len) {
        return levenshteinDistance(b, a);
    }

    const m = a.len;
    const n = b.len;

    // Edge cases
    if (m == 0) return @intCast(n);
    if (n == 0) return @intCast(m);

    // Use stack buffer for small strings, heap for larger
    var stack_buf: [MAX_TERM_LENGTH + 1]u32 = undefined;
    const prev_row = stack_buf[0 .. m + 1];

    // Initialize first row: distance from empty string to a[0..i]
    for (prev_row, 0..) |*cell, i| {
        cell.* = @intCast(i);
    }

    // Fill in the matrix row by row
    for (b, 0..) |b_char, j| {
        var prev_diag = prev_row[0];
        prev_row[0] = @intCast(j + 1);

        for (a, 0..) |a_char, i| {
            const cost: u32 = if (a_char == b_char) 0 else 1;

            const delete = prev_row[i + 1] + 1;
            const insert = prev_row[i] + 1;
            const substitute = prev_diag + cost;

            const old_val = prev_row[i + 1];
            prev_row[i + 1] = @min(@min(delete, insert), substitute);
            prev_diag = old_val;
        }
    }

    return prev_row[m];
}

/// Compute Levenshtein distance with early termination.
/// Returns null if the distance exceeds max_distance.
/// This is more efficient when you only care about matches within a threshold.
pub fn levenshteinDistanceBounded(a: []const u8, b: []const u8, max_distance: u32) ?u32 {
    // Quick length-based rejection
    const len_diff = if (a.len > b.len) a.len - b.len else b.len - a.len;
    if (len_diff > max_distance) return null;

    // Optimization: ensure a is the shorter string
    if (a.len > b.len) {
        return levenshteinDistanceBounded(b, a, max_distance);
    }

    const m = a.len;
    const n = b.len;

    // Edge cases
    if (m == 0) {
        return if (n <= max_distance) @intCast(n) else null;
    }

    var stack_buf: [MAX_TERM_LENGTH + 1]u32 = undefined;
    const prev_row = stack_buf[0 .. m + 1];

    // Initialize first row
    for (prev_row, 0..) |*cell, i| {
        cell.* = @intCast(i);
    }

    // Fill matrix with early termination check
    for (b, 0..) |b_char, j| {
        var prev_diag = prev_row[0];
        prev_row[0] = @intCast(j + 1);

        var row_min: u32 = prev_row[0];

        for (a, 0..) |a_char, i| {
            const cost: u32 = if (a_char == b_char) 0 else 1;

            const delete = prev_row[i + 1] + 1;
            const insert = prev_row[i] + 1;
            const substitute = prev_diag + cost;

            const old_val = prev_row[i + 1];
            const new_val = @min(@min(delete, insert), substitute);
            prev_row[i + 1] = new_val;
            prev_diag = old_val;

            row_min = @min(row_min, new_val);
        }

        // Early termination: if minimum in this row exceeds threshold,
        // no way to get a valid match
        if (row_min > max_distance) return null;
    }

    const result = prev_row[m];
    return if (result <= max_distance) result else null;
}

// ============================================================================
// Fuzzy Term Expansion
// ============================================================================

/// Find all dictionary terms within edit distance of the query term.
/// Returns matches sorted by distance (closest first), then alphabetically.
///
/// Note: This performs a full dictionary scan, which is O(V) where V is vocabulary size.
/// For very large dictionaries, consider using a BK-tree or n-gram index.
pub fn expandFuzzyTerms(
    allocator: Allocator,
    query_term: []const u8,
    dictionary: *Dictionary,
    max_distance: u32,
) ![]FuzzyMatch {
    var matches: std.ArrayListUnmanaged(FuzzyMatch) = .{};
    errdefer matches.deinit(allocator);

    // Iterate all dictionary terms
    var iter = try dictionary.iterate();
    defer iter.deinit();

    while (try iter.next()) |item| {
        // Check if within edit distance
        if (levenshteinDistanceBounded(query_term, item.term, max_distance)) |distance| {
            try matches.append(allocator, .{
                .term = item.term,
                .distance = distance,
                .entry = item.entry,
            });
        }
    }

    // Sort by distance (ascending), then by term (alphabetically)
    std.mem.sort(FuzzyMatch, matches.items, {}, struct {
        fn lessThan(_: void, a: FuzzyMatch, b: FuzzyMatch) bool {
            if (a.distance != b.distance) {
                return a.distance < b.distance;
            }
            return std.mem.lessThan(u8, a.term, b.term);
        }
    }.lessThan);

    return matches.toOwnedSlice(allocator);
}

/// Free fuzzy match results
pub fn freeMatches(allocator: Allocator, matches: []FuzzyMatch) void {
    allocator.free(matches);
}

// ============================================================================
// Scoring
// ============================================================================

/// Calculate the distance penalty for fuzzy matches.
/// Uses quadratic decay: penalty = 1.0 - (distance / max_distance)^2
///
/// Examples (max_distance = 2):
/// - distance 0: 1.0 (exact match, no penalty)
/// - distance 1: 0.75 (25% penalty)
/// - distance 2: 0.0 (full penalty, effectively filtered)
pub fn distancePenalty(distance: u32, max_distance: u32) f32 {
    if (max_distance == 0) return if (distance == 0) 1.0 else 0.0;
    if (distance >= max_distance) return 0.0;

    const ratio = @as(f32, @floatFromInt(distance)) / @as(f32, @floatFromInt(max_distance));
    return 1.0 - (ratio * ratio);
}

// ============================================================================
// Tests
// ============================================================================

test "levenshtein distance - identical strings" {
    try std.testing.expectEqual(@as(u32, 0), levenshteinDistance("hello", "hello"));
    try std.testing.expectEqual(@as(u32, 0), levenshteinDistance("", ""));
    try std.testing.expectEqual(@as(u32, 0), levenshteinDistance("a", "a"));
}

test "levenshtein distance - empty string" {
    try std.testing.expectEqual(@as(u32, 5), levenshteinDistance("hello", ""));
    try std.testing.expectEqual(@as(u32, 5), levenshteinDistance("", "hello"));
}

test "levenshtein distance - single edit" {
    // Insertion
    try std.testing.expectEqual(@as(u32, 1), levenshteinDistance("helo", "hello"));
    try std.testing.expectEqual(@as(u32, 1), levenshteinDistance("hello", "helloo"));

    // Deletion
    try std.testing.expectEqual(@as(u32, 1), levenshteinDistance("hello", "helo"));
    try std.testing.expectEqual(@as(u32, 1), levenshteinDistance("hello", "ello"));

    // Substitution
    try std.testing.expectEqual(@as(u32, 1), levenshteinDistance("hello", "hallo"));
    try std.testing.expectEqual(@as(u32, 1), levenshteinDistance("hello", "jello"));
}

test "levenshtein distance - multiple edits" {
    try std.testing.expectEqual(@as(u32, 2), levenshteinDistance("hello", "helo!"));
    try std.testing.expectEqual(@as(u32, 3), levenshteinDistance("kitten", "sitting"));
    try std.testing.expectEqual(@as(u32, 3), levenshteinDistance("saturday", "sunday"));
}

test "levenshtein distance - typos" {
    // Common typos
    try std.testing.expectEqual(@as(u32, 1), levenshteinDistance("database", "databse")); // missing a
    try std.testing.expectEqual(@as(u32, 2), levenshteinDistance("receive", "recieve")); // i/e swap (2 substitutions in Levenshtein)
    try std.testing.expectEqual(@as(u32, 2), levenshteinDistance("quick", "quikc")); // c/k swap
    try std.testing.expectEqual(@as(u32, 1), levenshteinDistance("quick", "quik")); // missing c
}

test "levenshtein distance bounded - within threshold" {
    try std.testing.expectEqual(@as(?u32, 0), levenshteinDistanceBounded("hello", "hello", 2));
    try std.testing.expectEqual(@as(?u32, 1), levenshteinDistanceBounded("hello", "helo", 2));
    try std.testing.expectEqual(@as(?u32, 2), levenshteinDistanceBounded("hello", "heo", 2));
}

test "levenshtein distance bounded - exceeds threshold" {
    try std.testing.expectEqual(@as(?u32, null), levenshteinDistanceBounded("hello", "world", 2));
    try std.testing.expectEqual(@as(?u32, null), levenshteinDistanceBounded("abc", "xyz", 2));
    // Length difference alone exceeds threshold
    try std.testing.expectEqual(@as(?u32, null), levenshteinDistanceBounded("hi", "hello", 2));
}

test "distance penalty" {
    // max_distance = 2
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), distancePenalty(0, 2), 0.001);
    try std.testing.expectApproxEqAbs(@as(f32, 0.75), distancePenalty(1, 2), 0.001);
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), distancePenalty(2, 2), 0.001);

    // max_distance = 3
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), distancePenalty(0, 3), 0.001);
    try std.testing.expectApproxEqAbs(@as(f32, 0.889), distancePenalty(1, 3), 0.001); // 1 - (1/3)^2
    try std.testing.expectApproxEqAbs(@as(f32, 0.556), distancePenalty(2, 3), 0.001); // 1 - (2/3)^2
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), distancePenalty(3, 3), 0.001);

    // Edge cases
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), distancePenalty(0, 0), 0.001);
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), distancePenalty(1, 0), 0.001);
}
