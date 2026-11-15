//! What the changefeed returns, byte for byte, for a workload with every
//! kind of event, compared with what 0.17.0-dev returned before the
//! changefeed's storage changed (lcf_changefeed_golden.txt, captured from the
//! code before that change). The encoding on disk and in the log may change;
//! what an application reads may not. One deliberate change since: within a
//! transaction, an edge's deletion now comes before its node's (events 23
//! and 24; see lcf_changefeed_order_test.zig).

const std = @import("std");
const lattice = @import("lattice");
const compat = @import("compat");

const Database = lattice.storage.database.Database;
const PropertyValue = lattice.core.types.PropertyValue;
const stream_payload = lattice.stream.payload;

const expected = @embedFile("lcf_changefeed_golden.txt");

fn cleanup(path: []const u8) void {
    compat.fs.cwd().deleteFile(path) catch {};
    var buf: [256]u8 = undefined;
    for ([_][]const u8{ "-wal", "-journal" }) |suffix| {
        const p = std.fmt.bufPrint(&buf, "{s}{s}", .{ path, suffix }) catch return;
        compat.fs.cwd().deleteFile(p) catch {};
    }
}

/// Windows enforces the database's file lock on reads, so the file of a
/// database that is still open cannot be copied there. A test that needs such
/// a copy is skipped on Windows.
fn skipIfLockedOnWindows(err: anyerror) anyerror {
    if (@import("builtin").os.tag == .windows and err == error.LockViolation) return error.SkipZigTest;
    return err;
}

fn copyFile(from: []const u8, to: []const u8) !void {
    const cwd = compat.fs.cwd();
    const in = try cwd.openFile(from, .{});
    defer in.close();
    const out = try cwd.createFile(to, .{ .truncate = true });
    defer out.close();
    var buf: [65536]u8 = undefined;
    var offset: u64 = 0;
    while (true) {
        const n = in.preadAll(&buf, offset) catch |err| return skipIfLockedOnWindows(err);
        if (n == 0) break;
        try out.pwriteAll(buf[0..n], offset);
        offset += n;
    }
}

fn open(allocator: std.mem.Allocator, path: []const u8) !*Database {
    return Database.open(allocator, path, .{
        .create = true,
        .config = .{ .enable_fts = false, .auto_checkpoint = null },
    });
}

/// The workload: every event kind, every value type, several transactions,
/// and a user stream written alongside.
fn runWorkload(db: *Database) !void {
    var a: u64 = 0;
    var b: u64 = 0;
    var e: u64 = 0;
    {
        var txn = try db.beginTransaction(.read_write);
        a = try db.createNode(&txn, &[_][]const u8{ "Person", "Employee" });
        try db.setNodeProperty(&txn, a, "name", .{ .string_val = "Ada" });
        try db.setNodeProperty(&txn, a, "age", .{ .int_val = 36 });
        try db.setNodeProperty(&txn, a, "score", .{ .float_val = 1.5 });
        try db.setNodeProperty(&txn, a, "active", .{ .bool_val = true });
        try db.setNodeProperty(&txn, a, "blob", .{ .bytes_val = "\x00\x01\xff" });
        var items = [_]PropertyValue{ .{ .string_val = "x" }, .{ .int_val = 2 } };
        try db.setNodeProperty(&txn, a, "tags", .{ .list_val = &items });
        b = try db.createNode(&txn, &[_][]const u8{"Person"});
        e = try db.createEdgeAndGetId(&txn, a, b, "KNOWS");
        try db.setEdgePropertyById(&txn, e, "since", .{ .int_val = 1843 });
        try db.setEdgePropertyById(&txn, e, "weight", .{ .float_val = 0.5 });
        try db.publishStream(&txn, "audit", "note", .{ .string_val = "first" });
        try db.commitTransaction(&txn);
    }
    {
        var txn = try db.beginTransaction(.read_write);
        try db.addNodeLabel(&txn, b, "Admin");
        try db.removeNodeLabel(&txn, a, "Employee");
        try db.setNodeProperty(&txn, a, "name", .{ .string_val = "Ada L." });
        try db.removeNodeProperty(&txn, a, "score");
        try db.setEdgePropertyById(&txn, e, "since", .{ .int_val = 1844 });
        try db.removeEdgePropertyById(&txn, e, "weight");
        try db.commitTransaction(&txn);
    }
    {
        var txn = try db.beginTransaction(.read_write);
        const big = try std.testing.allocator.alloc(u8, 6000);
        defer std.testing.allocator.free(big);
        @memset(big, 'z');
        try db.setNodeProperty(&txn, b, "essay", .{ .string_val = big });
        _ = try db.createNode(&txn, &[_][]const u8{});
        try db.publishStream(&txn, "audit", "note", .{ .string_val = "second" });
        try db.commitTransaction(&txn);
    }
    {
        var txn = try db.beginTransaction(.read_write);
        try db.deleteEdgeById(&txn, e);
        try db.deleteNode(&txn, a);
        try db.commitTransaction(&txn);
    }
    // A rolled-back transaction leaves no events.
    {
        var txn = try db.beginTransaction(.read_write);
        _ = try db.createNode(&txn, &[_][]const u8{"Ghost"});
        try db.abortTransaction(&txn);
    }
}

/// One line per record: sequence, kind, and the payload in the stream
/// payload encoding (the engine's value encoding), as hex.
fn render(allocator: std.mem.Allocator, db: *Database) ![]u8 {
    var out: std.ArrayListUnmanaged(u8) = .empty;
    errdefer out.deinit(allocator);
    var batch = try db.readChanges(0, 1000, 0);
    defer batch.deinit();
    for (batch.records) |record| {
        const bytes = try stream_payload.encodeAlloc(allocator, record.payload);
        defer allocator.free(bytes);
        const line = try std.fmt.allocPrint(allocator, "{d} {s} {x}\n", .{ record.sequence, record.kind, bytes });
        defer allocator.free(line);
        try out.appendSlice(allocator, line);
    }
    return out.toOwnedSlice(allocator);
}

fn expectGolden(allocator: std.mem.Allocator, db: *Database, when: []const u8) !void {
    const actual = try render(allocator, db);
    defer allocator.free(actual);
    if (!std.mem.eql(u8, expected, actual)) {
        std.debug.print("changefeed differs ({s}); it reads:\n{s}", .{ when, actual });
        return error.ChangefeedChanged;
    }
}

test "lcf the changefeed reads the same, byte for byte, as before its encoding changed" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lcfz_changefeed_golden.ltdb";
    const crashed = "/tmp/lcfz_changefeed_golden_crashed.ltdb";
    cleanup(path);
    cleanup(crashed);
    defer cleanup(path);
    defer cleanup(crashed);

    {
        var db = try open(allocator, path);
        defer db.close();
        try runWorkload(db);
        try expectGolden(allocator, db, "after commit");

        // The file as a crash would leave it: the commits are in the log
        // only, so opening the copy replays them.
        try copyFile(path, crashed);
        var wal_from: [256]u8 = undefined;
        var wal_to: [256]u8 = undefined;
        try copyFile(
            try std.fmt.bufPrint(&wal_from, "{s}-wal", .{path}),
            try std.fmt.bufPrint(&wal_to, "{s}-wal", .{crashed}),
        );
    }
    {
        var db = try open(allocator, path);
        defer db.close();
        try expectGolden(allocator, db, "after reopen");
    }
    {
        var db = try open(allocator, crashed);
        defer db.close();
        try expectGolden(allocator, db, "after recovery from the log");
    }
}
