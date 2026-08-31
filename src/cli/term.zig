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
const builtin = @import("builtin");

const is_windows = builtin.os.tag == .windows;

/// Raw mode, and the promise to put the terminal back.
pub const RawMode = if (is_windows) WindowsRawMode else PosixRawMode;

/// A source of bytes for the key decoder.
pub const Source = if (is_windows) WindowsSource else PosixSource;

/// Whether input is a terminal at all.
///
/// When it is not — a pipe, a file, a test harness — the REPL reads lines
/// plainly and never enters raw mode.
pub fn stdinIsTty() bool {
    if (is_windows) {
        var mode: DWORD = undefined;
        const handle = GetStdHandle(STD_INPUT_HANDLE);
        if (handle == INVALID_HANDLE_VALUE) return false;
        // Only a console has a console mode. A redirected stdin fails here,
        // which is exactly the question being asked.
        return GetConsoleMode(handle, &mode).toBool();
    }
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
// std.os.windows in this Zig version exposes almost none of the console API, so
// the handful of calls needed are declared here rather than waiting for it.
// ============================================================================

const HANDLE = std.os.windows.HANDLE;
const DWORD = std.os.windows.DWORD;
const BOOL = std.os.windows.BOOL;

const STD_INPUT_HANDLE: DWORD = @bitCast(@as(i32, -10));
const STD_OUTPUT_HANDLE: DWORD = @bitCast(@as(i32, -11));
const INVALID_HANDLE_VALUE: HANDLE = @ptrFromInt(std.math.maxInt(usize));

const ENABLE_PROCESSED_INPUT: DWORD = 0x0001;
const ENABLE_LINE_INPUT: DWORD = 0x0002;
const ENABLE_ECHO_INPUT: DWORD = 0x0004;
const ENABLE_VIRTUAL_TERMINAL_INPUT: DWORD = 0x0200;
const ENABLE_VIRTUAL_TERMINAL_PROCESSING: DWORD = 0x0004;

const WAIT_OBJECT_0: DWORD = 0;

extern "kernel32" fn GetStdHandle(nStdHandle: DWORD) callconv(.winapi) HANDLE;
extern "kernel32" fn GetConsoleMode(hConsoleHandle: HANDLE, lpMode: *DWORD) callconv(.winapi) BOOL;
extern "kernel32" fn SetConsoleMode(hConsoleHandle: HANDLE, dwMode: DWORD) callconv(.winapi) BOOL;
extern "kernel32" fn ReadFile(
    hFile: HANDLE,
    lpBuffer: [*]u8,
    nNumberOfBytesToRead: DWORD,
    lpNumberOfBytesRead: *DWORD,
    lpOverlapped: ?*anyopaque,
) callconv(.winapi) BOOL;
extern "kernel32" fn WaitForSingleObject(hHandle: HANDLE, dwMilliseconds: DWORD) callconv(.winapi) DWORD;

const WindowsRawMode = struct {
    enabled: bool = false,
    input: HANDLE = undefined,
    output: HANDLE = undefined,
    original_input: DWORD = 0,
    original_output: DWORD = 0,

    pub fn enableIfTty() !WindowsRawMode {
        if (!stdinIsTty()) return .{};

        const input = GetStdHandle(STD_INPUT_HANDLE);
        const output = GetStdHandle(STD_OUTPUT_HANDLE);
        if (input == INVALID_HANDLE_VALUE or output == INVALID_HANDLE_VALUE) return .{};

        var original_input: DWORD = 0;
        var original_output: DWORD = 0;
        if (!GetConsoleMode(input, &original_input).toBool()) return .{};
        if (!GetConsoleMode(output, &original_output).toBool()) return .{};

        // Input: no line buffering, no echo, and escape sequences delivered as
        // bytes rather than turned into console events. ENABLE_PROCESSED_INPUT
        // stays on so Ctrl-C keeps raising an interrupt, matching the POSIX side
        // where ISIG is deliberately left enabled.
        var raw_input = original_input;
        raw_input &= ~(ENABLE_LINE_INPUT | ENABLE_ECHO_INPUT);
        raw_input |= ENABLE_VIRTUAL_TERMINAL_INPUT | ENABLE_PROCESSED_INPUT;
        if (!SetConsoleMode(input, raw_input).toBool()) return .{};

        // Output: interpret the cursor movement and erase sequences the line
        // editor writes. Without this the redraw prints its own escapes.
        const raw_output = original_output | ENABLE_VIRTUAL_TERMINAL_PROCESSING;
        if (!SetConsoleMode(output, raw_output).toBool()) {
            _ = SetConsoleMode(input, original_input);
            return .{};
        }

        return .{
            .enabled = true,
            .input = input,
            .output = output,
            .original_input = original_input,
            .original_output = original_output,
        };
    }

    pub fn restore(self: *WindowsRawMode) void {
        if (!self.enabled) return;
        _ = SetConsoleMode(self.input, self.original_input);
        _ = SetConsoleMode(self.output, self.original_output);
        self.enabled = false;
    }
};

const WindowsSource = struct {
    pub fn readByte(_: *WindowsSource) !?u8 {
        const handle = GetStdHandle(STD_INPUT_HANDLE);
        if (handle == INVALID_HANDLE_VALUE) return null;
        var buf: [1]u8 = undefined;
        var read: DWORD = 0;
        if (!ReadFile(handle, &buf, 1, &read, null).toBool()) return null;
        if (read == 0) return null;
        return buf[0];
    }

    pub fn readByteTimeout(self: *WindowsSource, timeout_ms: u32) !?u8 {
        const handle = GetStdHandle(STD_INPUT_HANDLE);
        if (handle == INVALID_HANDLE_VALUE) return null;
        // The console handle signals when input is available, which is the same
        // question poll answers on the other side.
        if (WaitForSingleObject(handle, timeout_ms) != WAIT_OBJECT_0) return null;
        return self.readByte();
    }
};
