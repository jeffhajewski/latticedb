//! Character classes of the Turtle grammar (RDF 1.1 Turtle, section 6.5).

pub fn isPnCharsBase(cp: u21) bool {
    return switch (cp) {
        'A'...'Z', 'a'...'z' => true,
        0xC0...0xD6, 0xD8...0xF6, 0xF8...0x2FF, 0x370...0x37D, 0x37F...0x1FFF => true,
        0x200C...0x200D, 0x2070...0x218F, 0x2C00...0x2FEF, 0x3001...0xD7FF => true,
        0xF900...0xFDCF, 0xFDF0...0xFFFD, 0x10000...0xEFFFF => true,
        else => false,
    };
}

pub fn isPnCharsU(cp: u21) bool {
    return cp == '_' or isPnCharsBase(cp);
}

pub fn isPnChars(cp: u21) bool {
    return switch (cp) {
        '-', '0'...'9', 0xB7, 0x300...0x36F, 0x203F...0x2040 => true,
        else => isPnCharsU(cp),
    };
}

/// Characters that may follow a backslash in a local name.
pub fn isLocalEscape(c: u8) bool {
    return switch (c) {
        '_', '~', '.', '-', '!', '$', '&', '\'', '(', ')', '*', '+', ',', ';', '=', '/', '?', '#', '@', '%' => true,
        else => false,
    };
}

/// Characters an IRIREF may not hold unescaped.
pub fn isIriForbidden(c: u8) bool {
    return c <= 0x20 or switch (c) {
        '<', '>', '"', '{', '}', '|', '^', '`', '\\' => true,
        else => false,
    };
}

pub fn isHex(c: u8) bool {
    return switch (c) {
        '0'...'9', 'a'...'f', 'A'...'F' => true,
        else => false,
    };
}

/// Whether a string is a valid PN_LOCAL with no escapes needed, so a writer
/// can use `prefix:local`.
pub fn isPlainLocalName(s: []const u8) bool {
    if (s.len == 0) return true;
    const view = std.unicode.Utf8View.init(s) catch return false;
    var it = view.iterator();
    var first = true;
    var last: u21 = 0;
    while (it.nextCodepoint()) |cp| {
        const ok = if (first)
            isPnCharsU(cp) or cp == ':' or (cp >= '0' and cp <= '9')
        else
            isPnChars(cp) or cp == ':' or cp == '.';
        if (!ok) return false;
        first = false;
        last = cp;
    }
    return last != '.';
}

/// Whether a string is a valid PN_PREFIX (or empty).
pub fn isPrefixName(s: []const u8) bool {
    if (s.len == 0) return true;
    const view = std.unicode.Utf8View.init(s) catch return false;
    var it = view.iterator();
    var first = true;
    var last: u21 = 0;
    while (it.nextCodepoint()) |cp| {
        const ok = if (first) isPnCharsBase(cp) else isPnChars(cp) or cp == '.';
        if (!ok) return false;
        first = false;
        last = cp;
    }
    return last != '.';
}

const std = @import("std");
