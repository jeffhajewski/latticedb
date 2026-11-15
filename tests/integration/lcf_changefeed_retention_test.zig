//! Changefeed retention: what each policy keeps, how a reader learns it
//! missed events, how the policy is recorded in the file, and that
//! dropped events stay dropped through recovery.

const std = @import("std");
const lattice = @import("lattice");
const compat = @import("compat");

const Database = lattice.storage.database.Database;
const DatabaseConfig = lattice.storage.database.DatabaseConfig;
const DatabaseError = lattice.storage.database.DatabaseError;
const changefeed = lattice.stream.changefeed;
const Retention = changefeed.Retention;

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

fn open(path: []const u8, retention: ?Retention) !*Database {
    return Database.open(std.testing.allocator, path, .{
        .create = true,
        .config = .{ .enable_fts = false, .auto_checkpoint = null, .changefeed_retention = retention },
    });
}

/// Commit `count` transactions of one new node each: two events apiece
/// (node.insert, node.label_add).
fn commitNodes(db: *Database, count: usize) !void {
    for (0..count) |_| {
        var txn = try db.beginTransaction(.read_write);
        _ = try db.createNode(&txn, &.{"N"});
        try db.commitTransaction(&txn);
    }
}

fn sequences(db: *Database, after: u64) ![]u64 {
    var batch = try db.readChanges(after, 10_000, 0);
    defer batch.deinit();
    const out = try std.testing.allocator.alloc(u64, batch.records.len);
    for (batch.records, out) |record, *s| s.* = record.sequence;
    return out;
}

fn trees(db: *Database) lattice.stream.store.TreeSet {
    return .{
        .meta = &db.stream_meta_tree.?,
        .events = &db.stream_events_tree.?,
        .offsets = &db.stream_offsets_tree.?,
    };
}

test "lcf keep_last keeps the newest events and tells a reader that fell behind" {
    const path = "/tmp/lcfz_retention_last.ltdb";
    cleanup(path);
    defer cleanup(path);

    var db = try open(path, .{ .keep_last = 5 });
    defer db.close();
    try commitNodes(db, 10); // events 1..20
    try std.testing.expectEqual(@as(u64, 15), try db.trimChangefeed());

    const status = try db.changefeedStatus();
    try std.testing.expectEqual(@as(?u64, 16), status.oldest_sequence);
    try std.testing.expectEqual(@as(u64, 20), status.last_sequence);

    // From the start means from the oldest event held.
    const seqs = try sequences(db, 0);
    defer std.testing.allocator.free(seqs);
    try std.testing.expectEqualSlices(u64, &.{ 16, 17, 18, 19, 20 }, seqs);

    // A reader whose next event is still held reads on; one whose next
    // event was dropped is told so.
    const on = try sequences(db, 15);
    defer std.testing.allocator.free(on);
    try std.testing.expectEqual(@as(usize, 5), on.len);
    try std.testing.expectError(DatabaseError.ChangesTrimmed, db.readChanges(14, 10, 0));
    try std.testing.expectError(DatabaseError.ChangesTrimmed, db.readChanges(1, 10, 0));

    // Event 16 is the second event of its transaction: it now carries the
    // transaction's commit time.
    var buf: [changefeed.MAX_KEY_LEN]u8 = undefined;
    const value = (try trees(db).events.get(changefeed.encodeKey(&buf, 16))).?;
    defer trees(db).events.freeValue(value);
    const event = try changefeed.decode(value);
    try std.testing.expectEqual(changefeed.Op.node_label_add, event.op);
    try std.testing.expect(event.commit_ms != null);
}

test "lcf the file keeps its policy until an open names another" {
    const path = "/tmp/lcfz_retention_stored.ltdb";
    cleanup(path);
    defer cleanup(path);

    {
        var db = try open(path, .{ .keep_last = 3 });
        defer db.close();
    }
    {
        var db = try open(path, null);
        defer db.close();
        try std.testing.expect((try db.changefeedStatus()).retention.eql(.{ .keep_last = 3 }));
        try commitNodes(db, 4);
        _ = try db.trimChangefeed();
        try std.testing.expectEqual(@as(?u64, 6), (try db.changefeedStatus()).oldest_sequence);
    }
    {
        var db = try open(path, .keep_all);
        defer db.close();
        try std.testing.expect((try db.changefeedStatus()).retention == .keep_all);
    }
    {
        // A read-only open reports the file's policy and changes nothing.
        var db = try Database.open(std.testing.allocator, path, .{
            .read_only = true,
            .config = .{ .enable_fts = false, .changefeed_retention = .off },
        });
        defer db.close();
        try std.testing.expect((try db.changefeedStatus()).retention == .keep_all);
    }
}

test "lcf keep_for_ms drops events older than the window" {
    const path = "/tmp/lcfz_retention_age.ltdb";
    cleanup(path);
    defer cleanup(path);

    var db = try open(path, .{ .keep_for_ms = 60 * 60 * 1000 });
    defer db.close();
    try commitNodes(db, 3);
    try std.testing.expectEqual(@as(u64, 0), try db.trimChangefeed());
    db.close();

    db = try open(path, .{ .keep_for_ms = 200 });
    compat.sleep(400 * std.time.ns_per_ms);
    try commitNodes(db, 1); // events 7, 8
    try std.testing.expectEqual(@as(u64, 6), try db.trimChangefeed());
    const seqs = try sequences(db, 0);
    defer std.testing.allocator.free(seqs);
    try std.testing.expectEqualSlices(u64, &.{ 7, 8 }, seqs);
}

test "lcf off writes no events, and switching back on leaves a gap readers see" {
    const path = "/tmp/lcfz_retention_off.ltdb";
    cleanup(path);
    defer cleanup(path);

    {
        var db = try open(path, null);
        defer db.close();
        try commitNodes(db, 2); // events 1..4
    }
    {
        var db = try open(path, .off);
        defer db.close();
        try commitNodes(db, 2);
        try std.testing.expectError(DatabaseError.ChangefeedOff, db.readChanges(0, 10, 0));
        try std.testing.expectEqual(@as(u64, 4), try db.trimChangefeed());
        const status = try db.changefeedStatus();
        try std.testing.expectEqual(@as(?u64, null), status.oldest_sequence);
        try std.testing.expectEqual(@as(u64, 4), status.last_sequence);
    }
    {
        var db = try open(path, .keep_all);
        defer db.close();
        try commitNodes(db, 1);
        // Sequence 5 marks the time the feed was off; the next event is 6.
        const seqs = try sequences(db, 0);
        defer std.testing.allocator.free(seqs);
        try std.testing.expectEqualSlices(u64, &.{ 6, 7 }, seqs);
        // A reader that had read through 4 missed what happened while off.
        try std.testing.expectError(DatabaseError.ChangesTrimmed, db.readChanges(4, 10, 0));
        const on = try sequences(db, 5);
        defer std.testing.allocator.free(on);
        try std.testing.expectEqual(@as(usize, 2), on.len);
    }
}

test "lcf trimming the changefeed by hand moves the floor" {
    const path = "/tmp/lcfz_retention_manual.ltdb";
    cleanup(path);
    defer cleanup(path);

    var db = try open(path, null);
    defer db.close();
    try commitNodes(db, 3); // events 1..6
    var txn = try db.beginTransaction(.read_write);
    try db.trimStream(&txn, "__lattice_changes", 3);
    try db.commitTransaction(&txn);

    const seqs = try sequences(db, 0);
    defer std.testing.allocator.free(seqs);
    try std.testing.expectEqualSlices(u64, &.{ 4, 5, 6 }, seqs);
    try std.testing.expectError(DatabaseError.ChangesTrimmed, db.readChanges(1, 10, 0));
}

test "lcf a trim past the last event stops there, and later events are kept" {
    const path = "/tmp/lcfz_retention_trim_past_end.ltdb";
    cleanup(path);
    defer cleanup(path);

    {
        var db = try open(path, null);
        defer db.close();
        try commitNodes(db, 3); // events 1..6
        var txn = try db.beginTransaction(.read_write);
        try db.trimStream(&txn, "__lattice_changes", 1000);
        try db.commitTransaction(&txn);

        // Everything there was is gone, and the floor is the next sequence,
        // not 1001: a reader that had read through 6 has missed nothing.
        const none = try sequences(db, 0);
        defer std.testing.allocator.free(none);
        try std.testing.expectEqual(@as(usize, 0), none.len);
        var caught_up = try db.readChanges(6, 10, 0);
        caught_up.deinit();

        try commitNodes(db, 2); // events 7..10
        const seqs = try sequences(db, 0);
        defer std.testing.allocator.free(seqs);
        try std.testing.expectEqualSlices(u64, &.{ 7, 8, 9, 10 }, seqs);
        const after = try sequences(db, 6);
        defer std.testing.allocator.free(after);
        try std.testing.expectEqualSlices(u64, &.{ 7, 8, 9, 10 }, after);

        // Keeping everything, a retention pass drops nothing.
        try std.testing.expectEqual(@as(u64, 0), try db.trimChangefeed());
    }

    var db = try open(path, null);
    defer db.close();
    const seqs = try sequences(db, 0);
    defer std.testing.allocator.free(seqs);
    try std.testing.expectEqualSlices(u64, &.{ 7, 8, 9, 10 }, seqs);
}

test "lcf a trim past the last event replays to the same floor" {
    const path = "/tmp/lcfz_retention_trim_past_end_replay.ltdb";
    const crashed = "/tmp/lcfz_retention_trim_past_end_crashed.ltdb";

    // The copy is taken with everything still in the log, and again after a
    // flush has put the later events in the file under the log that trims.
    for ([_]bool{ false, true }) |flush_first| {
        cleanup(path);
        cleanup(crashed);
        defer cleanup(path);
        defer cleanup(crashed);
        {
            var db = try open(path, null);
            defer db.close();
            try commitNodes(db, 3); // events 1..6
            var txn = try db.beginTransaction(.read_write);
            try db.trimStream(&txn, "__lattice_changes", 1000);
            try db.commitTransaction(&txn);
            try commitNodes(db, 2); // events 7..10
            if (flush_first) try db.sync();
            try copyFile(path, crashed);
            var from: [256]u8 = undefined;
            var to: [256]u8 = undefined;
            try copyFile(
                try std.fmt.bufPrint(&from, "{s}-wal", .{path}),
                try std.fmt.bufPrint(&to, "{s}-wal", .{crashed}),
            );
        }
        var db = try open(crashed, null);
        defer db.close();
        const seqs = try sequences(db, 0);
        defer std.testing.allocator.free(seqs);
        try std.testing.expectEqualSlices(u64, &.{ 7, 8, 9, 10 }, seqs);
    }
}

test "lcf events dropped by retention stay dropped when the log is replayed" {
    const path = "/tmp/lcfz_retention_replay.ltdb";
    const crashed = "/tmp/lcfz_retention_replay_crashed.ltdb";
    cleanup(path);
    cleanup(crashed);
    defer cleanup(path);
    defer cleanup(crashed);

    {
        var db = try open(path, .{ .keep_last = 4 });
        defer db.close();
        // The commits are in the log, and the trim, which is not logged,
        // is in the file: replay must not bring the dropped events back.
        try commitNodes(db, 5); // events 1..10
        try std.testing.expectEqual(@as(u64, 6), try db.trimChangefeed());
        try copyFile(path, crashed);
        var from: [256]u8 = undefined;
        var to: [256]u8 = undefined;
        try copyFile(
            try std.fmt.bufPrint(&from, "{s}-wal", .{path}),
            try std.fmt.bufPrint(&to, "{s}-wal", .{crashed}),
        );
    }
    var db = try open(crashed, null);
    defer db.close();
    const seqs = try sequences(db, 0);
    defer std.testing.allocator.free(seqs);
    try std.testing.expectEqualSlices(u64, &.{ 7, 8, 9, 10 }, seqs);
}

test "lcf checkpoints apply the policy as the feed grows" {
    const path = "/tmp/lcfz_retention_checkpoint.ltdb";
    cleanup(path);
    defer cleanup(path);

    var db = try Database.open(std.testing.allocator, path, .{
        .create = true,
        .config = .{
            .enable_fts = false,
            .auto_checkpoint = .{ .max_wal_frames = 20, .min_interval_ns = 0 },
            .changefeed_retention = .{ .keep_last = 100 },
        },
    });
    defer db.close();
    try commitNodes(db, 500);
    const size = try db.changefeedSize();
    // At most the policy plus what was committed since the last checkpoint.
    try std.testing.expect(size.events >= 100 and size.events < 200);
    try std.testing.expectEqual(@as(u64, 0), size.legacy_events);
}
