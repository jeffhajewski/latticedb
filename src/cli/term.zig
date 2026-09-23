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
const compat = @import("compat");

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
        // Only a console has a console mode, so a redirected stdin fails this,
        // which is the question being asked.
        return win.getMode(compat.fs.stdin().handle()) != null;
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
// Nothing above this line changes for the port: key.zig already decodes the
// escape sequences a Windows console emits in virtual-terminal mode, and is
// tested against exactly those byte sequences.
//
// std.os.windows in this Zig version exposes almost none of the console API, so
// the calls this needs are declared here.
// ============================================================================

const win = struct {
    const windows = std.os.windows;

    const ENABLE_LINE_INPUT: windows.DWORD = 0x0002;
    const ENABLE_ECHO_INPUT: windows.DWORD = 0x0004;
    const ENABLE_VIRTUAL_TERMINAL_INPUT: windows.DWORD = 0x0200;

    const WAIT_OBJECT_0: windows.DWORD = 0;
    const KEY_EVENT: u16 = 0x0001;

    // Virtual key codes for the keys that produce an escape sequence while
    // carrying no character: PageUp through the arrows, Insert, Delete, and the
    // function keys.
    const VK_PRIOR: u16 = 0x21;
    const VK_DOWN: u16 = 0x28;
    const VK_INSERT: u16 = 0x2d;
    const VK_DELETE: u16 = 0x2e;
    const VK_F1: u16 = 0x70;
    const VK_F24: u16 = 0x87;

    const KeyEventRecord = extern struct {
        bKeyDown: windows.BOOL,
        wRepeatCount: u16,
        wVirtualKeyCode: u16,
        wVirtualScanCode: u16,
        /// The union of UnicodeChar and AsciiChar; only the wide form is read.
        uChar: u16,
        dwControlKeyState: windows.DWORD,
    };

    /// KEY_EVENT_RECORD is the widest of the five event bodies, so naming it
    /// directly gives this record the size and alignment of the C union.
    const InputRecord = extern struct {
        EventType: u16,
        Event: KeyEventRecord,
    };

    extern "kernel32" fn GetConsoleMode(handle: windows.HANDLE, mode: *windows.DWORD) callconv(.winapi) windows.BOOL;
    extern "kernel32" fn SetConsoleMode(handle: windows.HANDLE, mode: windows.DWORD) callconv(.winapi) windows.BOOL;
    extern "kernel32" fn PeekConsoleInputW(handle: windows.HANDLE, buffer: [*]InputRecord, length: windows.DWORD, read: *windows.DWORD) callconv(.winapi) windows.BOOL;
    extern "kernel32" fn WaitForSingleObject(handle: windows.HANDLE, milliseconds: windows.DWORD) callconv(.winapi) windows.DWORD;

    /// The current console mode, or null when the handle is not a console.
    fn getMode(handle: windows.HANDLE) ?windows.DWORD {
        var mode: windows.DWORD = 0;
        if (!GetConsoleMode(handle, &mode).toBool()) return null;
        return mode;
    }

    fn setMode(handle: windows.HANDLE, mode: windows.DWORD) bool {
        return SetConsoleMode(handle, mode).toBool();
    }
};

const WindowsRawMode = struct {
    enabled: bool = false,
    original_input: win.windows.DWORD = 0,
    original_output: win.windows.DWORD = 0,
    output_changed: bool = false,

    pub fn enableIfTty() !WindowsRawMode {
        const input = compat.fs.stdin().handle();
        // Doubles as the tty check: a redirected stdin has no console mode.
        const original_input = win.getMode(input) orelse return .{};

        // Line assembly and echo become our job, and virtual terminal input
        // delivers the arrow keys as the escape sequences the decoder parses.
        var raw = original_input & ~(win.ENABLE_LINE_INPUT | win.ENABLE_ECHO_INPUT);
        raw |= win.ENABLE_VIRTUAL_TERMINAL_INPUT;
        if (!win.setMode(input, raw)) return .{};

        var self = WindowsRawMode{ .enabled = true, .original_input = original_input };

        // The output half, which is the one a port forgets: the editor redraws
        // with escape sequences the classic console prints as text until it is
        // told to interpret them.
        const out = compat.fs.stdoutFile().handle();
        if (win.getMode(out)) |original_output| {
            const with_vt = original_output | win.windows.ENABLE_VIRTUAL_TERMINAL_PROCESSING;
            if (with_vt != original_output and win.setMode(out, with_vt)) {
                self.original_output = original_output;
                self.output_changed = true;
            }
        }

        return self;
    }

    pub fn restore(self: *WindowsRawMode) void {
        if (!self.enabled) return;
        _ = win.setMode(compat.fs.stdin().handle(), self.original_input);
        if (self.output_changed) {
            _ = win.setMode(compat.fs.stdoutFile().handle(), self.original_output);
        }
        self.enabled = false;
    }
};

const WindowsSource = struct {
    pub fn readByte(_: *WindowsSource) !?u8 {
        var buf: [1]u8 = undefined;
        while (true) {
            const n = compat.fs.stdin().readSome(&buf) catch |err| switch (err) {
                error.EndOfStream => return null,
                else => return err,
            };
            if (n != 0) return buf[0];
        }
    }

    /// Wait up to `timeout_ms` for a byte, so a lone Escape can be told from the
    /// start of a sequence.
    ///
    /// The console signals its input handle for every event in the queue, key
    /// releases included, and a release yields no byte — so waiting alone would
    /// announce a byte that is not there and then block in the read until the
    /// next keypress, which is the Escape key appearing to stick. Each wake is
    /// therefore checked against the queue itself, without consuming it, and
    /// only a keypress the console turns into bytes ends the wait.
    pub fn readByteTimeout(self: *WindowsSource, timeout_ms: u32) !?u8 {
        const handle = compat.fs.stdin().handle();
        const deadline = compat.nanoTimestamp() + @as(i128, timeout_ms) * std.time.ns_per_ms;

        while (true) {
            switch (pendingBytes(handle)) {
                .yes => return self.readByte(),
                // Not a console, so there is nothing to peek at; the plain read
                // is both the only answer available and the one poll gives.
                .unknown => return self.readByte(),
                .no => {},
            }

            const remaining_ns = deadline - compat.nanoTimestamp();
            if (remaining_ns <= 0) return null;
            // Rounded up, and capped at the original budget so that a clock
            // stepping backwards cannot stretch the wait.
            const remaining_ms: win.windows.DWORD = @intCast(@min(
                @as(i128, timeout_ms),
                @divTrunc(remaining_ns, std.time.ns_per_ms) + 1,
            ));
            if (win.WaitForSingleObject(handle, remaining_ms) != win.WAIT_OBJECT_0) return null;
        }
    }

    const Pending = enum { yes, no, unknown };

    /// Whether the queue holds a keypress the console will turn into bytes.
    fn pendingBytes(handle: win.windows.HANDLE) Pending {
        var records: [16]win.InputRecord = undefined;
        var count: win.windows.DWORD = 0;
        if (!win.PeekConsoleInputW(handle, &records, records.len, &count).toBool()) return .unknown;

        for (records[0..count]) |record| {
            if (record.EventType != win.KEY_EVENT) continue;
            if (!record.Event.bKeyDown.toBool()) continue;
            if (record.Event.uChar != 0) return .yes;
            // A modifier pressed on its own is a key-down that produces
            // nothing; these are the ones that produce a sequence.
            const vk = record.Event.wVirtualKeyCode;
            if (vk >= win.VK_PRIOR and vk <= win.VK_DOWN) return .yes;
            if (vk == win.VK_INSERT or vk == win.VK_DELETE) return .yes;
            if (vk >= win.VK_F1 and vk <= win.VK_F24) return .yes;
        }
        return .no;
    }
};
