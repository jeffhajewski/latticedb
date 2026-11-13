//! Checkpoints on demand, and the interval commits checkpoint at, through
//! the engine and the C API.

const std = @import("std");
const lattice = @import("lattice");

const c_api = lattice.c_api;
const lattice_database = c_api.lattice_database;
const lattice_txn = c_api.lattice_txn;
const lattice_error = c_api.lattice_error;
const lattice_node_id = c_api.lattice_node_id;
const Database = lattice.storage.database.Database;

fn cleanup(comptime path: []const u8) void {
    @import("compat").fs.cwd().deleteFile(path) catch {};
    @import("compat").fs.cwd().deleteFile(path ++ "-wal") catch {};
}

fn walSize(comptime path: []const u8) !u64 {
    const file = try @import("compat").fs.cwd().openFile(path ++ "-wal", .{});
    defer file.close();
    return (try file.stat()).size;
}

fn commitNodes(db: ?*lattice_database, count: usize) !void {
    for (0..count) |_| {
        var txn: ?*lattice_txn = null;
        try std.testing.expectEqual(lattice_error.ok, c_api.lattice_begin(db, .read_write, &txn));
        var id: lattice_node_id = 0;
        try std.testing.expectEqual(lattice_error.ok, c_api.lattice_node_create(txn, "N", &id));
        try std.testing.expectEqual(lattice_error.ok, c_api.lattice_commit(txn));
    }
}

fn countNodes(db: ?*lattice_database) !usize {
    var ids: ?[*]lattice_node_id = null;
    var count: usize = 0;
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_get_nodes_by_label(db, "N", 1, &ids, &count));
    c_api.lattice_free_node_ids(ids, count);
    return count;
}

test "lfmt checkpoint: with automatic checkpoints off the log grows, and lattice_checkpoint empties it" {
    const path = "/tmp/lfmt_ckpt_capi_off.db";
    cleanup(path);
    defer cleanup(path);

    var db: ?*lattice_database = null;
    var options = c_api.lattice_open_options{ .create = true };
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_open(path, &options, &db));

    // A small interval keeps the log small.
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_set_auto_checkpoint(db, 8, 0));
    try commitNodes(db, 100);
    const bounded = try walSize(path);

    // Off: no commit checkpoints, so the log keeps everything.
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_set_auto_checkpoint(db, 0, 0));
    try commitNodes(db, 100);
    const grown = try walSize(path);
    try commitNodes(db, 100);
    try std.testing.expect(try walSize(path) > grown);
    try std.testing.expect(grown > bounded);

    // FULL writes the pages and leaves the log.
    var stats: c_api.lattice_checkpoint_stats = undefined;
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_checkpoint(db, .full, &stats));
    try std.testing.expect(stats.pages_flushed > 0);
    try std.testing.expect(!stats.wal_truncated);
    try std.testing.expect(try walSize(path) > grown);

    // TRUNCATE empties it.
    try commitNodes(db, 1);
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_checkpoint(db, .truncate, &stats));
    try std.testing.expect(stats.wal_truncated);
    try std.testing.expect(try walSize(path) < bounded);
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_checkpoint(db, .passive, null));

    try std.testing.expectEqual(@as(usize, 301), try countNodes(db));
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_close(db));

    db = null;
    options.create = false;
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_open(path, &options, &db));
    defer _ = c_api.lattice_close(db);
    try std.testing.expectEqual(@as(usize, 301), try countNodes(db));
}

test "lfmt checkpoint: close folds the log in even with automatic checkpoints off" {
    const path = "/tmp/lfmt_ckpt_capi_close.db";
    cleanup(path);
    defer cleanup(path);

    var db: ?*lattice_database = null;
    var options = c_api.lattice_open_options{ .create = true };
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_open(path, &options, &db));
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_set_auto_checkpoint(db, 0, 0));
    try commitNodes(db, 50);
    const before_close = try walSize(path);
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_close(db));
    try std.testing.expect(try walSize(path) < before_close);
}

test "lfmt checkpoint: lattice_checkpoint refuses what it cannot do" {
    const path = "/tmp/lfmt_ckpt_capi_refuse.db";
    cleanup(path);
    defer cleanup(path);

    var db: ?*lattice_database = null;
    var options = c_api.lattice_open_options{ .create = true };
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_open(path, &options, &db));

    try std.testing.expectEqual(lattice_error.err_invalid_arg, c_api.lattice_checkpoint(db, @enumFromInt(7), null));
    try std.testing.expectEqual(lattice_error.err_invalid_arg, c_api.lattice_checkpoint(null, .full, null));
    try std.testing.expectEqual(lattice_error.err_invalid_arg, c_api.lattice_set_auto_checkpoint(null, 10, 0));

    // A writer whose changes are still in memory does not hold a
    // checkpoint back, and its commit after the checkpoint is kept.
    var writer: ?*lattice_txn = null;
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_begin(db, .read_write, &writer));
    var id: lattice_node_id = 0;
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_node_create(writer, "N", &id));
    for ([_]c_api.lattice_checkpoint_mode{ .passive, .full, .truncate }) |mode| {
        try std.testing.expectEqual(lattice_error.ok, c_api.lattice_checkpoint(db, mode, null));
    }
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_commit(writer));
    try std.testing.expectEqual(@as(usize, 1), try countNodes(db));

    var reader: ?*lattice_txn = null;
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_begin(db, .read_only, &reader));
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_checkpoint(db, .truncate, null));
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_rollback(reader));
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_close(db));

    // A read-only handle never writes the file.
    db = null;
    options.create = false;
    options.read_only = true;
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_open(path, &options, &db));
    defer _ = c_api.lattice_close(db);
    try std.testing.expectEqual(lattice_error.err_read_only, c_api.lattice_checkpoint(db, .full, null));
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_set_auto_checkpoint(db, 0, 0));
}

const env = @import("compat").env;

test "lfmt checkpoint: LATTICE_AUTO_CHECKPOINT_FRAMES sets the default, and the setter wins" {
    const path = "/tmp/lfmt_ckpt_capi_env.db";
    cleanup(path);
    defer cleanup(path);
    const page = 4096;
    const options = c_api.lattice_open_options{ .create = true };

    // Without the variable: 1000 frames, and a small commit is one frame.
    {
        var db: ?*lattice_database = null;
        try std.testing.expectEqual(lattice_error.ok, c_api.lattice_open(path, &options, &db));
        defer _ = c_api.lattice_close(db);
        try commitNodes(db, 100);
        try std.testing.expect(try walSize(path) > 90 * page);
    }

    env.set("LATTICE_AUTO_CHECKPOINT_FRAMES", "4");
    defer env.unset("LATTICE_AUTO_CHECKPOINT_FRAMES");
    var db: ?*lattice_database = null;
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_open(path, &options, &db));
    defer _ = c_api.lattice_close(db);
    for (0..100) |_| {
        try commitNodes(db, 1);
        try std.testing.expect(try walSize(path) < 8 * page);
    }

    // The setter wins over the variable.
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_set_auto_checkpoint(db, 0, 0));
    try commitNodes(db, 100);
    try std.testing.expect(try walSize(path) > 90 * page);

    // An hour between checkpoints: none yet.
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_set_auto_checkpoint(db, 16, 3_600_000));
    const before = try walSize(path);
    try commitNodes(db, 20);
    try std.testing.expect(try walSize(path) > before);

    // No minimum: the next commit checkpoints.
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_set_auto_checkpoint(db, 16, 0));
    try commitNodes(db, 1);
    try std.testing.expect(try walSize(path) < 16 * page);
}

fn commitNode(db: *Database) !void {
    var txn = try db.beginTransaction(.read_write);
    _ = try db.createNode(&txn, &.{"N"});
    try db.commitTransaction(&txn);
}

test "lfmt checkpoint: a policy set on an open database decides when commits checkpoint" {
    const allocator = std.testing.allocator;
    var memory = lattice.storage.memory_vfs.MemoryVfs.init(allocator);
    defer memory.deinit();

    const db = try Database.openWithVfs(allocator, "db", .{ .create = true, .config = .{ .enable_fts = false } }, memory.vfs());
    defer db.close();
    const wal = &db.wal.?;

    db.setAutoCheckpoint(null);
    for (0..1100) |_| try commitNode(db);
    // The default policy would have checkpointed at 1000 frames.
    try std.testing.expect(wal.header.frame_count > 1000);

    db.setAutoCheckpoint(.{ .max_wal_frames = 32 });
    for (0..400) |_| {
        try commitNode(db);
        try std.testing.expect(wal.header.frame_count < 32 + 16);
    }

    const stats = (try db.checkpoint(.truncate)).?;
    try std.testing.expect(stats.wal_truncated);
    try std.testing.expectEqual(@as(u64, 0), wal.header.frame_count);
}

test "lfmt checkpoint: the C stats struct and modes keep the header's layout and values" {
    const Stats = c_api.lattice_checkpoint_stats;
    try std.testing.expectEqual(@as(usize, 0), @offsetOf(Stats, "pages_flushed"));
    try std.testing.expectEqual(@as(usize, 8), @offsetOf(Stats, "duration_ns"));
    try std.testing.expectEqual(@as(usize, 16), @offsetOf(Stats, "checkpoint_lsn"));
    try std.testing.expectEqual(@as(usize, 24), @offsetOf(Stats, "wal_truncated"));
    try std.testing.expectEqual(@as(usize, 32), @sizeOf(Stats));
    try std.testing.expectEqual(@as(c_int, 0), @intFromEnum(c_api.lattice_checkpoint_mode.passive));
    try std.testing.expectEqual(@as(c_int, 1), @intFromEnum(c_api.lattice_checkpoint_mode.full));
    try std.testing.expectEqual(@as(c_int, 2), @intFromEnum(c_api.lattice_checkpoint_mode.truncate));
}
