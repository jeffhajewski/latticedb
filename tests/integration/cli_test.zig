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

/// What one run of the command-line tool produced.
const CliRun = struct {
    exit_code: ?u8,
    stdout: []u8,
    stderr: []u8,

    fn deinit(self: CliRun, allocator: std.mem.Allocator) void {
        allocator.free(self.stdout);
        allocator.free(self.stderr);
    }

    fn succeeded(self: CliRun) bool {
        return self.exit_code != null and self.exit_code.? == 0;
    }
};

/// Run `lattice` with these arguments and collect everything it printed.
fn runCli(allocator: std.mem.Allocator, args: []const []const u8) !CliRun {
    var argv: std.ArrayList([]const u8) = .empty;
    defer argv.deinit(allocator);
    try argv.append(allocator, build_options.cli_path);
    try argv.appendSlice(allocator, args);

    const result = try std.process.run(allocator, io, .{ .argv = argv.items });
    return .{
        .exit_code = switch (result.term) {
            .exited => |code| code,
            else => null,
        },
        .stdout = result.stdout,
        .stderr = result.stderr,
    };
}

fn deleteDatabaseFiles(path: []const u8) void {
    compat.fs.cwd().deleteFile(path) catch {};
    var wal_buf: [256]u8 = undefined;
    const wal = std.fmt.bufPrint(&wal_buf, "{s}-wal", .{path}) catch return;
    compat.fs.cwd().deleteFile(wal) catch {};
}

fn copyFile(allocator: std.mem.Allocator, src: []const u8, dst: []const u8) !void {
    const in = try compat.fs.cwd().openFile(src, .{});
    defer in.close();
    const bytes = try in.readToEndAlloc(allocator, 1 << 30);
    defer allocator.free(bytes);

    const out = try compat.fs.cwd().createFile(dst, .{ .truncate = true });
    defer out.close();
    try out.pwriteAll(bytes, 0);
}

/// Copy a database and its log while the handle is still open, so the copy is
/// what the next process finds after the writer was killed.
fn copyAsIfKilled(allocator: std.mem.Allocator, src: []const u8, dst: []const u8) !void {
    try copyFile(allocator, src, dst);
    var src_wal_buf: [256]u8 = undefined;
    var dst_wal_buf: [256]u8 = undefined;
    try copyFile(
        allocator,
        try std.fmt.bufPrint(&src_wal_buf, "{s}-wal", .{src}),
        try std.fmt.bufPrint(&dst_wal_buf, "{s}-wal", .{dst}),
    );
}

const killed_vector_dims = 8;

/// A vector database whose log still holds a committed vector, as a killed
/// writer leaves it. Opening it replays that vector, which needs the index.
fn makeKilledVectorDatabase(allocator: std.mem.Allocator, source: []const u8, killed: []const u8) !void {
    deleteDatabaseFiles(source);
    deleteDatabaseFiles(killed);
    defer deleteDatabaseFiles(source);

    var db = try Database.open(allocator, source, .{
        .create = true,
        .config = .{
            .enable_fts = false,
            .enable_vector = true,
            .vector_dimensions = killed_vector_dims,
            .auto_checkpoint = null,
        },
    });
    defer db.close();

    var vector = [_]f32{0} ** killed_vector_dims;
    vector[0] = 1;
    var txn = try db.beginTransaction(.read_write);
    const node_id = try db.createNode(&txn, &.{"Chunk"});
    try db.setNodeVectorInTxn(&txn, node_id, &vector);
    try db.commitTransaction(&txn);
    try copyAsIfKilled(allocator, source, killed);
}

test "cli: every command that opens a database can enable vectors, and says to when it must" {
    const allocator = std.testing.allocator;
    const source = "/tmp/lattice_cli_vecopen_source.ltdb";
    const killed = "/tmp/lattice_cli_vecopen_killed.ltdb";
    const backup = "/tmp/lattice_cli_vecopen_backup.ltdb";
    const replica = "/tmp/lattice_cli_vecopen_replica";
    const import_file = "/tmp/lattice_cli_vecopen_import.json";
    defer deleteDatabaseFiles(killed);
    defer deleteDatabaseFiles(backup);
    defer compat.fs.cwd().deleteTree(replica) catch {};
    defer compat.fs.cwd().deleteFile(import_file) catch {};

    {
        const file = try compat.fs.cwd().createFile(import_file, .{ .truncate = true });
        defer file.close();
        try file.deprecatedWriter().writeAll("{\"nodes\":[],\"edges\":[]}\n");
    }

    const commands = [_][]const []const u8{
        &.{ "exec", killed, "--query=MATCH (n) RETURN count(n)" },
        &.{ "query", killed },
        &.{ "checkpoint", killed },
        &.{ "compact", killed },
        &.{ "backup", killed, "--file=" ++ backup },
        &.{ "replicate", killed, "--to=" ++ replica },
        &.{ "import", killed, "--file=" ++ import_file },
    };

    for (commands) |command| {
        try makeKilledVectorDatabase(allocator, source, killed);
        deleteDatabaseFiles(backup);
        compat.fs.cwd().deleteTree(replica) catch {};

        const refused = try runCli(allocator, command);
        defer refused.deinit(allocator);
        if (refused.succeeded() or std.mem.indexOf(u8, refused.stderr, "--enable-vector --vector-dims") == null) {
            std.debug.print("{s} without vector options: exit {?d}\nstderr: {s}\n", .{ command[0], refused.exit_code, refused.stderr });
            return error.TestUnexpectedResult;
        }

        var with_vectors: std.ArrayList([]const u8) = .empty;
        defer with_vectors.deinit(allocator);
        try with_vectors.appendSlice(allocator, command);
        try with_vectors.appendSlice(allocator, &.{ "--enable-vector", "--vector-dims=8" });

        const opened = try runCli(allocator, with_vectors.items);
        defer opened.deinit(allocator);
        if (!opened.succeeded()) {
            std.debug.print("{s} with vector options: exit {?d}\nstderr: {s}\n", .{ command[0], opened.exit_code, opened.stderr });
            return error.TestUnexpectedResult;
        }
    }
}

test "cli: help for every command that opens a database lists the vector options" {
    const allocator = std.testing.allocator;
    const commands = [_][]const u8{
        "info",   "compact", "checkpoint", "backup", "replicate", "query",  "exec",
        "import", "export",  "dump",       "labels", "types",     "schema", "count",
    };
    for (commands) |command| {
        const help = try runCli(allocator, &.{ command, "--help" });
        defer help.deinit(allocator);
        if (std.mem.indexOf(u8, help.stdout, "--enable-vector") == null or
            std.mem.indexOf(u8, help.stdout, "--vector-dims=<n>") == null)
        {
            std.debug.print("help for {s} does not list the vector options:\n{s}\n", .{ command, help.stdout });
            return error.TestUnexpectedResult;
        }
    }
}

fn readFileAlloc(allocator: std.mem.Allocator, path: []const u8) ![]u8 {
    const file = try compat.fs.cwd().openFile(path, .{});
    defer file.close();
    return file.readToEndAlloc(allocator, 1 << 30);
}

test "cli: export includes vectors when told the dimensions, and warns when it leaves them out" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_cli_export_vectors.ltdb";
    const out = "/tmp/lattice_cli_export_vectors.json";
    deleteDatabaseFiles(path);
    defer deleteDatabaseFiles(path);
    defer compat.fs.cwd().deleteFile(out) catch {};

    {
        var db = try Database.open(allocator, path, .{
            .create = true,
            .config = .{ .enable_fts = false, .enable_vector = true, .vector_dimensions = 4 },
        });
        defer db.close();
        const node = try db.createNode(null, &.{"Chunk"});
        try db.setNodeVector(node, &[_]f32{ 1, 0, 0, 0 });
    }

    const without = try runCli(allocator, &.{ "export", path, "--file=" ++ out });
    defer without.deinit(allocator);
    try std.testing.expect(without.succeeded());
    try std.testing.expect(std.mem.indexOf(u8, without.stderr, "--enable-vector") != null);
    const plain = try readFileAlloc(allocator, out);
    defer allocator.free(plain);
    try std.testing.expect(std.mem.indexOf(u8, plain, "\"vector\"") == null);

    const with = try runCli(allocator, &.{ "export", path, "--file=" ++ out, "--enable-vector", "--vector-dims=4" });
    defer with.deinit(allocator);
    try std.testing.expect(with.succeeded());
    try std.testing.expectEqual(@as(usize, 0), with.stderr.len);
    const full = try readFileAlloc(allocator, out);
    defer allocator.free(full);
    try std.testing.expect(std.mem.indexOf(u8, full, "\"vector\":[1.0,0.0,0.0,0.0]") != null);
}
