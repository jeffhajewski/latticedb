//! Prefix/Wildcard Search for Full-Text Search.
//!
//! Implements prefix matching for queries like "optim*" that match
//! "optimize", "optimization", "optimizer", etc.
//!
//! Uses B+Tree range queries for efficient prefix expansion.

const std = @import("std");
const Allocator = std.mem.Allocator;

const dictionary_mod = @import("dictionary.zig");
const Dictionary = dictionary_mod.Dictionary;
const DictionaryEntry = dictionary_mod.DictionaryEntry;

/// Maximum supported prefix length
pub const MAX_PREFIX_LENGTH: usize = 64;

/// Configuration for prefix search
pub const PrefixConfig = struct {
    /// Minimum prefix length (prevents "a*" matching everything)
    min_prefix_length: u32 = 2,
    /// Maximum number of terms to expand (prevents explosion)
    max_expansions: u32 = 50,
};

/// Prefix match result
pub const PrefixMatch = struct {
    /// The matched term from dictionary
    term: []const u8,
    /// Dictionary entry for the matched term
    entry: DictionaryEntry,
};

// ============================================================================
// Upper Bound Calculation
// ============================================================================

/// Compute the exclusive upper bound for a prefix range query.
///
/// For prefix "optim", returns "optin" (increment last byte).
/// Handles edge cases where last byte is 0xFF by truncating and incrementing.
///
/// Returns null if all bytes are 0xFF (prefix matches everything after it).
///
/// Examples:
///   "hello" -> "hellp"
///   "test"  -> "tesu"
///   "ab\xff" -> "ac"
///   "\xff\xff" -> null
pub fn prefixUpperBound(prefix: []const u8, buf: *[MAX_PREFIX_LENGTH]u8) ?[]const u8 {
    if (prefix.len == 0 or prefix.len > MAX_PREFIX_LENGTH) {
        return null;
    }

    // Copy prefix to buffer
    @memcpy(buf[0..prefix.len], prefix);
    var len = prefix.len;

    // Work backwards, incrementing bytes
    while (len > 0) {
        const idx = len - 1;
        if (buf[idx] < 0xFF) {
            // Can increment this byte
            buf[idx] += 1;
            return buf[0..len];
        }
        // This byte is 0xFF, truncate and try previous
        len -= 1;
    }

    // All bytes were 0xFF, no upper bound
    return null;
}

// ============================================================================
// Prefix Expansion
// ============================================================================

/// Expand a prefix to all matching dictionary terms.
///
/// Uses B+Tree range query for efficient iteration.
/// Returns matches sorted alphabetically by term.
///
/// Note: For very common prefixes, results are capped at max_expansions.
pub fn expandPrefixTerms(
    allocator: Allocator,
    prefix: []const u8,
    dictionary: *Dictionary,
    config: PrefixConfig,
) ![]PrefixMatch {
    var matches: std.ArrayListUnmanaged(PrefixMatch) = .{};
    errdefer matches.deinit(allocator);

    // Check minimum prefix length
    if (prefix.len < config.min_prefix_length) {
        return matches.toOwnedSlice(allocator);
    }

    // Calculate upper bound for range query
    var upper_buf: [MAX_PREFIX_LENGTH]u8 = undefined;
    const upper_bound = prefixUpperBound(prefix, &upper_buf);

    // Iterate dictionary terms in range [prefix, upper_bound)
    var iter = try dictionary.iterateRange(prefix, upper_bound);
    defer iter.deinit();

    var count: u32 = 0;
    while (try iter.next()) |item| {
        // Verify term actually starts with prefix (range might include edge cases)
        if (!std.mem.startsWith(u8, item.term, prefix)) {
            continue;
        }

        try matches.append(allocator, .{
            .term = item.term,
            .entry = item.entry,
        });

        count += 1;
        if (count >= config.max_expansions) {
            break;
        }
    }

    return matches.toOwnedSlice(allocator);
}

/// Free prefix match results
pub fn freeMatches(allocator: Allocator, matches: []PrefixMatch) void {
    allocator.free(matches);
}

// ============================================================================
// Utilities
// ============================================================================

/// Check if a term contains a wildcard suffix
pub fn hasWildcard(term: []const u8) bool {
    return term.len > 0 and term[term.len - 1] == '*';
}

/// Extract prefix from a wildcard term (strips trailing *)
pub fn extractPrefix(term: []const u8) []const u8 {
    if (hasWildcard(term)) {
        return term[0 .. term.len - 1];
    }
    return term;
}

// ============================================================================
// Tests
// ============================================================================

test "prefix upper bound - basic" {
    var buf: [MAX_PREFIX_LENGTH]u8 = undefined;

    // Normal cases
    const result1 = prefixUpperBound("hello", &buf);
    try std.testing.expect(result1 != null);
    try std.testing.expectEqualStrings("hellp", result1.?);

    const result2 = prefixUpperBound("test", &buf);
    try std.testing.expect(result2 != null);
    try std.testing.expectEqualStrings("tesu", result2.?);

    const result3 = prefixUpperBound("a", &buf);
    try std.testing.expect(result3 != null);
    try std.testing.expectEqualStrings("b", result3.?);
}

test "prefix upper bound - 0xFF handling" {
    var buf: [MAX_PREFIX_LENGTH]u8 = undefined;

    // Single 0xFF at end - truncate and increment previous
    const input1 = "ab\xff";
    const result1 = prefixUpperBound(input1, &buf);
    try std.testing.expect(result1 != null);
    try std.testing.expectEqualStrings("ac", result1.?);

    // Multiple 0xFF at end
    const input2 = "a\xff\xff";
    const result2 = prefixUpperBound(input2, &buf);
    try std.testing.expect(result2 != null);
    try std.testing.expectEqualStrings("b", result2.?);

    // All 0xFF - no upper bound
    const input3 = "\xff\xff";
    const result3 = prefixUpperBound(input3, &buf);
    try std.testing.expect(result3 == null);
}

test "prefix upper bound - edge cases" {
    var buf: [MAX_PREFIX_LENGTH]u8 = undefined;

    // Empty prefix
    const result1 = prefixUpperBound("", &buf);
    try std.testing.expect(result1 == null);

    // Single character
    const result2 = prefixUpperBound("z", &buf);
    try std.testing.expect(result2 != null);
    try std.testing.expectEqualStrings("{", result2.?); // 'z' + 1 = '{'
}

test "hasWildcard" {
    try std.testing.expect(hasWildcard("test*"));
    try std.testing.expect(hasWildcard("*"));
    try std.testing.expect(!hasWildcard("test"));
    try std.testing.expect(!hasWildcard(""));
    try std.testing.expect(!hasWildcard("te*st"));
}

test "extractPrefix" {
    try std.testing.expectEqualStrings("test", extractPrefix("test*"));
    try std.testing.expectEqualStrings("", extractPrefix("*"));
    try std.testing.expectEqualStrings("test", extractPrefix("test"));
    try std.testing.expectEqualStrings("te*st", extractPrefix("te*st"));
}
