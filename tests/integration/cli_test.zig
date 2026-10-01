//! The `lattice` command-line tool, run as a separate process.
//!
//! These drive the built binary through pipes, the way a script or another
//! program would, so they see exactly what reaches the other end: what was
//! printed is not the same as what was delivered.

const std = @import("std");
const lattice = @import("lattice");
const build_options = @import("build_options");
const compat = @import("compat");

const Database = lattice.storage.database.Database;
const Child = std.process.Child;

const io = std.testing.io;

/// How long the REPL gets to show its prompt before the test stops waiting.
const prompt_timeout_ms = 10_000;

/// Closes the child's input if the prompt has not arrived in time.
///
/// Without this a REPL that never flushes would leave the test blocked in a
/// read forever. Closing its input ends the REPL, which then exits and prints
/// everything it was holding, so the test sees the prompt either way and has
/// to know whether it arrived before the deadline or only because of it.
const Watchdog = struct {
    child: *Child,
    state: std.atomic.Value(u8) = .init(waiting),

    const waiting: u8 = 0;
    const answered: u8 = 1;
    const timed_out: u8 = 2;

    fn run(self: *Watchdog) void {
        var waited_ms: u32 = 0;
        while (waited_ms < prompt_timeout_ms) : (waited_ms += 10) {
            if (self.state.load(.acquire) != waiting) return;
            compat.sleep(10 * std.time.ns_per_ms);
        }
        if (self.claim(timed_out)) closeInput(self.child);
    }

    /// Whoever moves the state off `waiting` owns the child's input.
    fn claim(self: *Watchdog, outcome: u8) bool {
        return self.state.cmpxchgStrong(waiting, outcome, .acq_rel, .acquire) == null;
    }
};

fn closeInput(child: *Child) void {
    if (child.stdin) |stdin| {
        stdin.close(io);
        // `wait` closes whatever is left, so this one must not be left.
        child.stdin = null;
    }
}

test "cli: the REPL shows its prompt while it is waiting for input" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_cli_repl.ltdb";
    const wal_path = "/tmp/lattice_cli_repl.ltdb-wal";

    compat.fs.cwd().deleteFile(path) catch {};
    compat.fs.cwd().deleteFile(wal_path) catch {};
    defer compat.fs.cwd().deleteFile(path) catch {};
    defer compat.fs.cwd().deleteFile(wal_path) catch {};

    {
        const db = try Database.open(allocator, path, .{
            .create = true,
            .config = .{ .enable_fts = false, .enable_vector = false },
        });
        db.close();
    }

    // Without a home directory the REPL keeps its history in memory, so the
    // test leaves the history file of whoever runs it alone.
    var environ = try std.testing.environ.createMap(allocator);
    defer environ.deinit();
    _ = environ.swapRemove("HOME");
    _ = environ.swapRemove("USERPROFILE");

    var child = try std.process.spawn(io, .{
        .argv = &.{ build_options.cli_path, "query", path },
        .environ_map = &environ,
        .stdin = .pipe,
        .stdout = .pipe,
        .stderr = .ignore,
    });
    defer child.kill(io);

    var watchdog = Watchdog{ .child = &child };
    const thread = try std.Thread.spawn(.{}, Watchdog.run, .{&watchdog});

    // Nothing is written to the REPL, so its input stays open and it sits at
    // the prompt. Whatever it has shown by then is what a user would see.
    var seen: [4096]u8 = undefined;
    var len: usize = 0;
    const prompt_seen = while (len < seen.len) {
        const n = child.stdout.?.readStreaming(io, &.{seen[len..]}) catch |err| switch (err) {
            error.EndOfStream => break false,
            else => return err,
        };
        len += n;
        if (std.mem.indexOf(u8, seen[0..len], "lattice> ") != null) break true;
    } else false;

    const in_time = watchdog.claim(Watchdog.answered);
    if (in_time) {
        try child.stdin.?.writeStreamingAll(io, ".exit\n");
        closeInput(&child);
    }
    thread.join();
    _ = try child.wait(io);

    try std.testing.expect(prompt_seen);
    try std.testing.expect(std.mem.indexOf(u8, seen[0..len], "Connected to:") != null);
    // Arriving only once the input was closed means the REPL held it until
    // exit, which at a terminal is a blank screen.
    try std.testing.expect(in_time);
}
