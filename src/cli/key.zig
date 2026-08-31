//! Terminal key decoding: bytes in, keys out.
//!
//! This file knows nothing about how the bytes arrived. It is handed something
//! that can produce a byte and, when asked, wait a short while for one; from that
//! it produces whole keypresses. That separation is the point: a Windows console
//! in virtual-terminal mode emits the same escape sequences as a POSIX terminal,
//! so the decoding belongs somewhere neither platform owns. Only the byte source
//! and the mode-setting differ.
//!
//! ## Why a state machine rather than a switch
//!
//! The REPL used to decode escapes inline, recognising `ESC [ A`-style sequences
//! and ignoring the rest. Ignoring is the problem: an unrecognised sequence was
//! ignored one byte at a time, so its tail arrived as ordinary input. Pressing
//! Ctrl-Right, which sends `ESC [ 1 ; 5 C`, put a literal `;5C` in the line.
//!
//! A sequence has a shape — parameters, then optional intermediates, then one
//! final byte — so this consumes the whole of it before deciding what it was. An
//! unknown sequence is reported as unknown and leaves nothing behind.

const std = @import("std");

/// One keypress.
pub const Key = union(enum) {
    /// A printable character, as a codepoint rather than a byte, so the caller
    /// can hold a cursor in characters and not split one.
    char: u21,
    enter,
    tab,
    backspace,
    delete,
    up,
    down,
    left,
    right,
    home,
    end,
    page_up,
    page_down,
    /// Control-and-a-letter, normalised to the lowercase letter: Ctrl-A is 'a'.
    ctrl: u8,
    /// Escape pressed on its own, rather than beginning a sequence.
    escape,
    /// A well-formed sequence with no meaning assigned here. It has been read in
    /// full; nothing is left in the stream.
    unknown,
    eof,
};

/// How long to wait for the byte after an Escape before calling it a bare
/// Escape keypress.
///
/// A terminal sends the bytes of a sequence together, so anything arriving this
/// much later came from a person. Too short and a slow connection turns arrow
/// keys into stray Escapes; too long and the Escape key feels stuck.
pub const escape_timeout_ms: u32 = 25;

/// Decode keys from `Source`.
///
/// `Source` must provide:
///   - `readByte(*Source) anyerror!?u8` — blocking; null at end of input
///   - `readByteTimeout(*Source, u32) anyerror!?u8` — null on timeout or end
pub fn Reader(comptime Source: type) type {
    return struct {
        source: *Source,

        const Self = @This();

        pub fn init(source: *Source) Self {
            return .{ .source = source };
        }

        /// Read one keypress, consuming exactly the bytes that make it up.
        pub fn readKey(self: *Self) !Key {
            const b = (try self.source.readByte()) orelse return .eof;
            return switch (b) {
                0x1b => self.readEscape(),
                '\r', '\n' => .enter,
                '\t' => .tab,
                0x7f, 0x08 => .backspace,
                // C0 controls. Ctrl-A is 1, so add 'a' - 1 to name the letter.
                // Enter, Tab and Backspace are handled above and never reach here.
                0x01...0x07, 0x0b...0x0c, 0x0e...0x1a => .{ .ctrl = b + 'a' - 1 },
                // Remaining controls carry no key this program acts on.
                0x00, 0x1c...0x1f => .unknown,
                else => self.readUtf8(b),
            };
        }

        /// After an Escape byte.
        fn readEscape(self: *Self) !Key {
            const b = (try self.source.readByteTimeout(escape_timeout_ms)) orelse return .escape;
            return switch (b) {
                '[' => self.readCsi(),
                // SS3, which terminals in application cursor mode use for the
                // arrows and which tmux sends by default. Dropping these is why
                // arrow keys appeared dead under some terminals.
                'O' => self.readSs3(),
                // Alt-and-a-key. Consumed and reported rather than left to arrive
                // as a stray character.
                else => .unknown,
            };
        }

        /// A CSI sequence: `ESC [` then parameters, intermediates, and a final.
        fn readCsi(self: *Self) !Key {
            var params: [16]u8 = undefined;
            var params_len: usize = 0;

            var b = (try self.source.readByte()) orelse return .eof;

            // Parameter bytes.
            while (b >= 0x30 and b <= 0x3f) {
                if (params_len < params.len) {
                    params[params_len] = b;
                    params_len += 1;
                }
                b = (try self.source.readByte()) orelse return .eof;
            }
            // Intermediate bytes, which nothing here needs but which have to be
            // consumed to reach the final byte.
            while (b >= 0x20 and b <= 0x2f) {
                b = (try self.source.readByte()) orelse return .eof;
            }
            // Anything outside the final-byte range means the sequence was
            // malformed; it has still been consumed up to here.
            if (b < 0x40 or b > 0x7e) return .unknown;

            return csiKey(params[0..params_len], b);
        }

        /// `ESC O` then one byte.
        fn readSs3(self: *Self) !Key {
            const b = (try self.source.readByte()) orelse return .eof;
            return switch (b) {
                'A' => .up,
                'B' => .down,
                'C' => .right,
                'D' => .left,
                'H' => .home,
                'F' => .end,
                else => .unknown,
            };
        }

        /// A printable character, possibly several bytes of one.
        fn readUtf8(self: *Self, first: u8) !Key {
            const len = std.unicode.utf8ByteSequenceLength(first) catch return .unknown;
            if (len == 1) return .{ .char = first };

            var buf: [4]u8 = undefined;
            buf[0] = first;
            for (1..len) |i| {
                const cont = (try self.source.readByte()) orelse return .eof;
                // A byte that is not a continuation means the input was not valid
                // UTF-8. Report it rather than decoding nonsense.
                if (cont & 0xc0 != 0x80) return .unknown;
                buf[i] = cont;
            }
            const cp = std.unicode.utf8Decode(buf[0..len]) catch return .unknown;
            return .{ .char = cp };
        }
    };
}

/// What a CSI sequence means, given its parameters and final byte.
///
/// Modifiers arrive as extra parameters — Ctrl-Right is `1;5C` — and are ignored
/// here, so a modified arrow still moves. The alternative, treating it as
/// unknown, would make Ctrl-Right do nothing at all, which is worse than doing
/// the unmodified thing.
fn csiKey(params: []const u8, final: u8) Key {
    switch (final) {
        'A' => return .up,
        'B' => return .down,
        'C' => return .right,
        'D' => return .left,
        'H' => return .home,
        'F' => return .end,
        '~' => {
            const n = leadingNumber(params) orelse return .unknown;
            return switch (n) {
                1, 7 => .home,
                3 => .delete,
                4, 8 => .end,
                5 => .page_up,
                6 => .page_down,
                else => .unknown,
            };
        },
        else => return .unknown,
    }
}

/// The first numeric parameter, up to the first `;`.
fn leadingNumber(params: []const u8) ?u16 {
    var value: u16 = 0;
    var seen = false;
    for (params) |c| {
        if (c == ';') break;
        if (c < '0' or c > '9') return null;
        value = value *| 10 +| (c - '0');
        seen = true;
    }
    return if (seen) value else null;
}

// ============================================================================
// Tests
//
// The parser is tested against byte slices, which is the point of separating it:
// no terminal, no platform, and the exact bytes a Windows console would deliver.
// ============================================================================

const TestSource = struct {
    bytes: []const u8,
    pos: usize = 0,

    fn readByte(self: *TestSource) !?u8 {
        if (self.pos >= self.bytes.len) return null;
        defer self.pos += 1;
        return self.bytes[self.pos];
    }

    /// Nothing further has "arrived", so a timeout read behaves like the stream
    /// having gone quiet — which is what makes a trailing Escape a bare Escape.
    fn readByteTimeout(self: *TestSource, _: u32) !?u8 {
        return self.readByte();
    }
};

fn keysOf(bytes: []const u8, out: []Key) ![]Key {
    var source = TestSource{ .bytes = bytes };
    var reader = Reader(TestSource).init(&source);
    var n: usize = 0;
    while (n < out.len) {
        const key = try reader.readKey();
        if (key == .eof) break;
        out[n] = key;
        n += 1;
    }
    return out[0..n];
}

test "printable ASCII arrives as characters" {
    var buf: [8]Key = undefined;
    const keys = try keysOf("hi", &buf);
    try std.testing.expectEqual(@as(usize, 2), keys.len);
    try std.testing.expectEqual(@as(u21, 'h'), keys[0].char);
    try std.testing.expectEqual(@as(u21, 'i'), keys[1].char);
}

test "arrows in both CSI and SS3 forms" {
    var buf: [8]Key = undefined;
    // A terminal in normal cursor mode, then application mode. Only the first
    // used to work; the second made arrow keys look broken under tmux.
    const keys = try keysOf("\x1b[A\x1b[B\x1bOC\x1bOD", &buf);
    try std.testing.expectEqual(@as(usize, 4), keys.len);
    try std.testing.expectEqual(Key.up, keys[0]);
    try std.testing.expectEqual(Key.down, keys[1]);
    try std.testing.expectEqual(Key.right, keys[2]);
    try std.testing.expectEqual(Key.left, keys[3]);
}

test "a modified arrow still moves, and leaves nothing behind" {
    var buf: [8]Key = undefined;
    // Ctrl-Right. The old decoder ignored the '1', then read ';', '5' and 'C' as
    // ordinary characters and inserted ";5C" into the line.
    const keys = try keysOf("\x1b[1;5C", &buf);
    try std.testing.expectEqual(@as(usize, 1), keys.len);
    try std.testing.expectEqual(Key.right, keys[0]);
}

test "an unknown sequence is consumed whole" {
    var buf: [8]Key = undefined;
    // A cursor position report, which nothing here wants. What matters is that
    // its digits and semicolon do not become input.
    const keys = try keysOf("\x1b[12;40Rx", &buf);
    try std.testing.expectEqual(@as(usize, 2), keys.len);
    try std.testing.expectEqual(Key.unknown, keys[0]);
    try std.testing.expectEqual(@as(u21, 'x'), keys[1].char);
}

test "tilde sequences" {
    var buf: [8]Key = undefined;
    const keys = try keysOf("\x1b[3~\x1b[5~\x1b[6~\x1b[1~\x1b[4~", &buf);
    try std.testing.expectEqual(@as(usize, 5), keys.len);
    try std.testing.expectEqual(Key.delete, keys[0]);
    try std.testing.expectEqual(Key.page_up, keys[1]);
    try std.testing.expectEqual(Key.page_down, keys[2]);
    try std.testing.expectEqual(Key.home, keys[3]);
    try std.testing.expectEqual(Key.end, keys[4]);
}

test "multi-byte UTF-8 is one key, not several" {
    var buf: [8]Key = undefined;
    // Each of these used to arrive as separate bytes, so the cursor could land
    // between them and backspace could cut one in half.
    const keys = try keysOf("é→🙂", &buf);
    try std.testing.expectEqual(@as(usize, 3), keys.len);
    try std.testing.expectEqual(@as(u21, 0xe9), keys[0].char);
    try std.testing.expectEqual(@as(u21, 0x2192), keys[1].char);
    try std.testing.expectEqual(@as(u21, 0x1f642), keys[2].char);
}

test "control keys" {
    var buf: [8]Key = undefined;
    const keys = try keysOf("\x01\x05\x04\x7f\t\r", &buf);
    try std.testing.expectEqual(@as(usize, 6), keys.len);
    try std.testing.expectEqual(@as(u8, 'a'), keys[0].ctrl);
    try std.testing.expectEqual(@as(u8, 'e'), keys[1].ctrl);
    try std.testing.expectEqual(@as(u8, 'd'), keys[2].ctrl);
    try std.testing.expectEqual(Key.backspace, keys[3]);
    try std.testing.expectEqual(Key.tab, keys[4]);
    try std.testing.expectEqual(Key.enter, keys[5]);
}

test "a lone escape is a keypress, not a stuck parser" {
    var buf: [8]Key = undefined;
    const keys = try keysOf("\x1b", &buf);
    try std.testing.expectEqual(@as(usize, 1), keys.len);
    try std.testing.expectEqual(Key.escape, keys[0]);
}

test "truncated sequences end the stream rather than hanging" {
    var buf: [8]Key = undefined;
    const keys = try keysOf("\x1b[", &buf);
    try std.testing.expectEqual(@as(usize, 0), keys.len);
}
