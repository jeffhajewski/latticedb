//! Porter Stemmer for English.
//!
//! Implements the Porter stemming algorithm to reduce words to their root forms.
//! This improves search recall by matching different forms of the same word.
//!
//! Examples:
//!   - "running", "runs", "ran" -> "run"
//!   - "happily", "happiness" -> "happi"
//!   - "connected", "connecting" -> "connect"

const std = @import("std");
const Allocator = std.mem.Allocator;

/// Stem a word using the Porter algorithm.
/// Returns a slice into the provided buffer with the stemmed word.
/// The input word should be lowercase ASCII.
pub fn stem(word: []const u8, buf: *[64]u8) []const u8 {
    if (word.len == 0 or word.len > 64) {
        return word;
    }

    // Copy word to mutable buffer
    @memcpy(buf[0..word.len], word);
    var len = word.len;

    // Step 1a: Remove plurals
    len = step1a(buf, len);

    // Step 1b: Remove -ed, -ing
    len = step1b(buf, len);

    // Step 1c: Turn terminal y to i when there is another vowel in the stem
    len = step1c(buf, len);

    // Step 2: Map double suffixes to single ones
    len = step2(buf, len);

    // Step 3: Deal with -ic-, -full, -ness, etc.
    len = step3(buf, len);

    // Step 4: Remove -ant, -ence, etc.
    len = step4(buf, len);

    // Step 5: Remove a final -e
    len = step5(buf, len);

    return buf[0..len];
}

/// Check if a character is a vowel (a, e, i, o, u)
fn isVowel(c: u8) bool {
    return switch (c) {
        'a', 'e', 'i', 'o', 'u' => true,
        else => false,
    };
}

/// Check if character at position is a vowel (y is vowel if preceded by consonant)
fn isVowelAt(word: []const u8, pos: usize) bool {
    if (pos >= word.len) return false;
    const c = word[pos];
    if (isVowel(c)) return true;
    if (c == 'y' and pos > 0) return !isVowel(word[pos - 1]);
    return false;
}

/// Count the number of consonant sequences (m value in Porter algorithm)
fn measure(word: []const u8) usize {
    if (word.len == 0) return 0;

    var m: usize = 0;
    var i: usize = 0;

    // Skip initial consonants
    while (i < word.len and !isVowelAt(word, i)) : (i += 1) {}

    while (i < word.len) {
        // Skip vowels
        while (i < word.len and isVowelAt(word, i)) : (i += 1) {}

        // Count consonant sequence
        if (i < word.len) {
            m += 1;
            while (i < word.len and !isVowelAt(word, i)) : (i += 1) {}
        }
    }

    return m;
}

/// Check if word contains a vowel
fn containsVowel(word: []const u8) bool {
    for (word, 0..) |_, i| {
        if (isVowelAt(word, i)) return true;
    }
    return false;
}

/// Check if word ends with double consonant
fn endsWithDoubleConsonant(word: []const u8) bool {
    if (word.len < 2) return false;
    const c = word[word.len - 1];
    return c == word[word.len - 2] and !isVowelAt(word, word.len - 1);
}

/// Check if word ends with consonant-vowel-consonant (not w, x, or y)
fn endsCvc(word: []const u8) bool {
    if (word.len < 3) return false;
    const c = word[word.len - 1];
    if (c == 'w' or c == 'x' or c == 'y') return false;
    return !isVowelAt(word, word.len - 1) and
        isVowelAt(word, word.len - 2) and
        !isVowelAt(word, word.len - 3);
}

/// Check if word ends with given suffix
fn endsWith(word: []const u8, suffix: []const u8) bool {
    if (word.len < suffix.len) return false;
    return std.mem.eql(u8, word[word.len - suffix.len ..], suffix);
}

/// Replace suffix with replacement if conditions are met
fn replaceSuffix(buf: *[64]u8, len: usize, suffix: []const u8, replacement: []const u8, min_measure: usize) usize {
    if (!endsWith(buf[0..len], suffix)) return len;

    const stem_len = len - suffix.len;
    if (measure(buf[0..stem_len]) > min_measure) {
        @memcpy(buf[stem_len .. stem_len + replacement.len], replacement);
        return stem_len + replacement.len;
    }
    return len;
}

// Step 1a: Plural forms
fn step1a(buf: *[64]u8, len: usize) usize {
    if (endsWith(buf[0..len], "sses")) {
        return len - 2; // sses -> ss
    }
    if (endsWith(buf[0..len], "ies")) {
        return len - 2; // ies -> i
    }
    if (endsWith(buf[0..len], "ss")) {
        return len; // ss -> ss
    }
    if (len > 0 and buf[len - 1] == 's') {
        return len - 1; // s ->
    }
    return len;
}

// Step 1b: -ed and -ing
fn step1b(buf: *[64]u8, len: usize) usize {
    if (endsWith(buf[0..len], "eed")) {
        if (measure(buf[0 .. len - 3]) > 0) {
            return len - 1; // eed -> ee
        }
        return len;
    }

    var new_len = len;
    var changed = false;

    if (endsWith(buf[0..len], "ed")) {
        const stem_len = len - 2;
        if (containsVowel(buf[0..stem_len])) {
            new_len = stem_len;
            changed = true;
        }
    } else if (endsWith(buf[0..len], "ing")) {
        const stem_len = len - 3;
        if (containsVowel(buf[0..stem_len])) {
            new_len = stem_len;
            changed = true;
        }
    }

    if (changed) {
        // Additional fixes for stems
        if (endsWith(buf[0..new_len], "at")) {
            buf[new_len] = 'e';
            return new_len + 1;
        }
        if (endsWith(buf[0..new_len], "bl")) {
            buf[new_len] = 'e';
            return new_len + 1;
        }
        if (endsWith(buf[0..new_len], "iz")) {
            buf[new_len] = 'e';
            return new_len + 1;
        }
        if (endsWithDoubleConsonant(buf[0..new_len])) {
            const c = buf[new_len - 1];
            if (c != 'l' and c != 's' and c != 'z') {
                return new_len - 1;
            }
        }
        if (measure(buf[0..new_len]) == 1 and endsCvc(buf[0..new_len])) {
            buf[new_len] = 'e';
            return new_len + 1;
        }
    }

    return new_len;
}

// Step 1c: Turn y to i
fn step1c(buf: *[64]u8, len: usize) usize {
    if (len > 1 and buf[len - 1] == 'y' and containsVowel(buf[0 .. len - 1])) {
        buf[len - 1] = 'i';
    }
    return len;
}

// Step 2: Double suffix to single suffix
fn step2(buf: *[64]u8, len: usize) usize {
    if (len < 3) return len;

    // Table of (suffix, replacement) pairs for m > 0
    const replacements = [_]struct { suffix: []const u8, replacement: []const u8 }{
        .{ .suffix = "ational", .replacement = "ate" },
        .{ .suffix = "tional", .replacement = "tion" },
        .{ .suffix = "enci", .replacement = "ence" },
        .{ .suffix = "anci", .replacement = "ance" },
        .{ .suffix = "izer", .replacement = "ize" },
        .{ .suffix = "abli", .replacement = "able" },
        .{ .suffix = "alli", .replacement = "al" },
        .{ .suffix = "entli", .replacement = "ent" },
        .{ .suffix = "eli", .replacement = "e" },
        .{ .suffix = "ousli", .replacement = "ous" },
        .{ .suffix = "ization", .replacement = "ize" },
        .{ .suffix = "ation", .replacement = "ate" },
        .{ .suffix = "ator", .replacement = "ate" },
        .{ .suffix = "alism", .replacement = "al" },
        .{ .suffix = "iveness", .replacement = "ive" },
        .{ .suffix = "fulness", .replacement = "ful" },
        .{ .suffix = "ousness", .replacement = "ous" },
        .{ .suffix = "aliti", .replacement = "al" },
        .{ .suffix = "iviti", .replacement = "ive" },
        .{ .suffix = "biliti", .replacement = "ble" },
    };

    for (replacements) |r| {
        const new_len = replaceSuffix(buf, len, r.suffix, r.replacement, 0);
        if (new_len != len) return new_len;
    }

    return len;
}

// Step 3: -ic-, -full, -ness, etc.
fn step3(buf: *[64]u8, len: usize) usize {
    const replacements = [_]struct { suffix: []const u8, replacement: []const u8 }{
        .{ .suffix = "icate", .replacement = "ic" },
        .{ .suffix = "ative", .replacement = "" },
        .{ .suffix = "alize", .replacement = "al" },
        .{ .suffix = "iciti", .replacement = "ic" },
        .{ .suffix = "ical", .replacement = "ic" },
        .{ .suffix = "ful", .replacement = "" },
        .{ .suffix = "ness", .replacement = "" },
    };

    for (replacements) |r| {
        const new_len = replaceSuffix(buf, len, r.suffix, r.replacement, 0);
        if (new_len != len) return new_len;
    }

    return len;
}

// Step 4: Remove -ant, -ence, etc. (m > 1)
fn step4(buf: *[64]u8, len: usize) usize {
    const suffixes = [_][]const u8{
        "al",    "ance",  "ence", "er",   "ic",   "able",
        "ible",  "ant",   "ement", "ment", "ent",  "ion",
        "ou",    "ism",   "ate",  "iti",  "ous",  "ive",
        "ize",
    };

    for (suffixes) |suffix| {
        if (endsWith(buf[0..len], suffix)) {
            const stem_len = len - suffix.len;
            // Special case for -ion: must be preceded by s or t
            if (std.mem.eql(u8, suffix, "ion") and stem_len > 0) {
                const c = buf[stem_len - 1];
                if (c != 's' and c != 't') continue;
            }
            if (measure(buf[0..stem_len]) > 1) {
                return stem_len;
            }
        }
    }

    return len;
}

// Step 5: Final cleanup
fn step5(buf: *[64]u8, len: usize) usize {
    if (len == 0) return len;

    var new_len = len;

    // Step 5a: Remove final -e if m > 1, or m = 1 and not *o
    if (buf[len - 1] == 'e') {
        const m = measure(buf[0 .. len - 1]);
        if (m > 1 or (m == 1 and !endsCvc(buf[0 .. len - 1]))) {
            new_len = len - 1;
        }
    }

    // Step 5b: ll -> l if m > 1
    if (new_len > 1 and buf[new_len - 1] == 'l' and buf[new_len - 2] == 'l') {
        if (measure(buf[0..new_len]) > 1) {
            new_len -= 1;
        }
    }

    return new_len;
}

// ============================================================================
// Tests
// ============================================================================

test "porter stemmer basic" {
    var buf: [64]u8 = undefined;

    // Test plural removal
    try std.testing.expectEqualStrings("cat", stem("cats", &buf));
    try std.testing.expectEqualStrings("caress", stem("caresses", &buf));
    try std.testing.expectEqualStrings("poni", stem("ponies", &buf));

    // Test -ed removal
    try std.testing.expectEqualStrings("agre", stem("agreed", &buf));
    try std.testing.expectEqualStrings("plaster", stem("plastered", &buf));

    // Test -ing removal
    try std.testing.expectEqualStrings("motor", stem("motoring", &buf));
    try std.testing.expectEqualStrings("sing", stem("sing", &buf));

    // Test common words
    try std.testing.expectEqualStrings("connect", stem("connected", &buf));
    try std.testing.expectEqualStrings("connect", stem("connecting", &buf));
    try std.testing.expectEqualStrings("connect", stem("connection", &buf));
}

test "porter stemmer step1" {
    var buf: [64]u8 = undefined;

    // Step 1a tests - plural removal
    try std.testing.expectEqualStrings("caress", stem("caresses", &buf));
    try std.testing.expectEqualStrings("poni", stem("ponies", &buf));
    try std.testing.expectEqualStrings("ti", stem("ties", &buf));
    try std.testing.expectEqualStrings("caress", stem("caress", &buf));
    try std.testing.expectEqualStrings("cat", stem("cats", &buf));

    // Step 1b tests - ed/ing removal
    try std.testing.expectEqualStrings("feed", stem("feed", &buf));
    try std.testing.expectEqualStrings("agre", stem("agreed", &buf)); // agreed -> agre
    try std.testing.expectEqualStrings("disabl", stem("disabled", &buf)); // disabled -> disabl
    try std.testing.expectEqualStrings("mat", stem("matting", &buf)); // matting -> mat
    try std.testing.expectEqualStrings("rate", stem("rating", &buf));

    // Step 1c tests - y -> i
    try std.testing.expectEqualStrings("happi", stem("happy", &buf));
    try std.testing.expectEqualStrings("sky", stem("sky", &buf)); // no vowel before y
}

test "porter stemmer advanced" {
    var buf: [64]u8 = undefined;

    // Step 2 tests - suffix mapping
    try std.testing.expectEqualStrings("relat", stem("relational", &buf)); // relational -> relat
    try std.testing.expectEqualStrings("hope", stem("hopeful", &buf));

    // Step 3 tests - -ic, -full, -ness
    try std.testing.expectEqualStrings("triplic", stem("triplicate", &buf));
    try std.testing.expectEqualStrings("formal", stem("formalize", &buf));
    try std.testing.expectEqualStrings("formal", stem("formality", &buf));
    try std.testing.expectEqualStrings("electr", stem("electrical", &buf)); // electrical -> electr

    // Step 4 tests - remove -ant, -ence, etc.
    try std.testing.expectEqualStrings("reviv", stem("revival", &buf));

    // Step 5 tests - final cleanup
    try std.testing.expectEqualStrings("probat", stem("probate", &buf));
    try std.testing.expectEqualStrings("rate", stem("rate", &buf));
    try std.testing.expectEqualStrings("control", stem("controll", &buf));
    try std.testing.expectEqualStrings("roll", stem("roll", &buf));
}
