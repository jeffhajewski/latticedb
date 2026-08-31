//! The two things reading a key needs from the operating system: a byte, and a
//! terminal willing to hand over raw ones.
//!
//! Everything above this — deciding that `ESC [ 1 ; 5 C` means Ctrl-Right — lives
//! in key.zig and is shared. This file is the whole of what differs between
//! platforms, which is why it is small.
//!
//! ## What "raw mode" has to arrange
//!
//! Two separate things, and forgetting the second is the usual way a port half
//! works:
//!
//!   * **Input** must arrive unbuffered, unechoed, a byte at a time, with escape
//!     sequences passed through rather than interpreted.
//!   * **Output** must interpret the escape sequences the line editor writes to
//!     move the cursor and clear to end of line. On a POSIX terminal that is
//!     always true. On Windows it is off by default, so the editor's redraw
//!     would appear as literal `←[K` text.

const std = @import("std");

/// Raw mode, and the promise to put the terminal back.
///
/// One implementation today. A Windows port adds a second and selects between
/// them on the target; see the note at the bottom of this file.
pub const RawMode = PosixRawMode;

/// A source of bytes for the key decoder.
pub const Source = PosixSource;

/// Whether input is a terminal at all.
///
/// When it is not — a pipe, a file, a test harness — the REPL reads lines
/// plainly and never enters raw mode.
/// A Windows port answers this with GetConsoleMode succeeding on the input
/// handle: only a console has a console mode, so a redirected stdin fails it,
/// which is the question being asked.
pub fn stdinIsTty() bool {
    if (@hasDecl(std.posix, "isatty")) {
        return std.posix.isatty(std.posix.STDIN_FILENO);
    }
    return std.c.isatty(std.c.STDIN_FILENO) != 0;
}

// ============================================================================
// POSIX
// ============================================================================

const PosixRawMode = struct {
    enabled: bool = false,
    original: std.posix.termios = undefined,

    pub fn enableIfTty() !PosixRawMode {
        if (!stdinIsTty()) return .{};

        const original = try std.posix.tcgetattr(std.posix.STDIN_FILENO);
        var raw = original;
        raw.lflag.ECHO = false;
        raw.lflag.ICANON = false;
        raw.lflag.IEXTEN = false;
        // Keep ISIG enabled so Ctrl-C still behaves as expected.
        raw.iflag.ICRNL = false;
        raw.iflag.IXON = false;
        raw.cc[@intFromEnum(std.posix.V.MIN)] = 1;
        raw.cc[@intFromEnum(std.posix.V.TIME)] = 0;

        try std.posix.tcsetattr(std.posix.STDIN_FILENO, .FLUSH, raw);
        return .{ .enabled = true, .original = original };
    }

    pub fn restore(self: *PosixRawMode) void {
        if (!self.enabled) return;
        std.posix.tcsetattr(std.posix.STDIN_FILENO, .FLUSH, self.original) catch {};
        self.enabled = false;
    }
};

const PosixSource = struct {
    pub fn readByte(_: *PosixSource) !?u8 {
        var buf: [1]u8 = undefined;
        const n = try std.posix.read(std.posix.STDIN_FILENO, &buf);
        if (n == 0) return null;
        return buf[0];
    }

    pub fn readByteTimeout(self: *PosixSource, timeout_ms: u32) !?u8 {
        var fds = [_]std.posix.pollfd{.{
            .fd = std.posix.STDIN_FILENO,
            .events = std.posix.POLL.IN,
            .revents = 0,
        }};
        const ready = std.posix.poll(&fds, @intCast(timeout_ms)) catch return null;
        if (ready == 0) return null;
        return self.readByte();
    }
};

// ============================================================================
// Windows
//
// Not implemented. This is the whole of what a Windows port has to supply:
// a `WindowsRawMode` with `enableIfTty` and `restore`, and a `WindowsSource`
// with `readByte` and `readByteTimeout`. Nothing above this file changes —
// key.zig already decodes the escape sequences a Windows console emits in
// virtual-terminal mode, and is tested against exactly those byte sequences.
//
// Two things worth knowing before starting.
//
// Raw mode has to arrange input *and* output. Input must arrive unbuffered,
// unechoed, one byte at a time, with escape sequences passed through rather
// than digested into console events — that is ENABLE_VIRTUAL_TERMINAL_INPUT
// with ENABLE_LINE_INPUT and ENABLE_ECHO_INPUT cleared. Output must then
// interpret the escape sequences the line editor writes to move the cursor and
// clear the line, which is ENABLE_VIRTUAL_TERMINAL_PROCESSING on the output
// handle. On a POSIX terminal that second half is simply true, so it is the
// half a port tends to forget; without it the redraw prints its own escapes as
// text.
//
// `readByteTimeout` is the only place the two platforms differ in shape rather
// than spelling. It exists so a lone Escape keypress can be told from the start
// of a sequence. POSIX answers that with poll; Windows would use
// WaitForSingleObject on the console handle.
//
// Note that std.os.windows in this Zig version exposes almost none of the
// console API — one kernel32 extern in total — so GetStdHandle, GetConsoleMode,
// SetConsoleMode, ReadFile and WaitForSingleObject need declaring.
// ============================================================================
