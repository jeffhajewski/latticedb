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

    // Without the variable: the default interval, 3200 frames for the
    // 100 MB cache, and a small commit is one frame.
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
    // The default policy for this 16 MB pool would have checkpointed at
    // 1000 frames.
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

const AutoCheckpointConfig = lattice.storage.checkpoint.AutoCheckpointConfig;
const mib: u64 = 1024 * 1024;

test "lfmt checkpoint: the default interval is an eighth of the cache, between 1000 frames and 64 MB" {
    const frames = AutoCheckpointConfig.defaultMaxWalFrames;
    // Small caches keep the old 1000 frames.
    try std.testing.expectEqual(@as(u64, 1000), frames(0, 4096));
    try std.testing.expectEqual(@as(u64, 1000), frames(256 * 1024, 4096));
    try std.testing.expectEqual(@as(u64, 1000), frames(28 * mib, 4096));
    // In between, an eighth of the cache.
    try std.testing.expectEqual(@as(u64, 1280), frames(40 * mib, 4096));
    try std.testing.expectEqual(@as(u64, 3200), frames(100 * mib, 4096));
    // 64 MB of log at most.
    try std.testing.expectEqual(@as(u64, 16384), frames(512 * mib, 4096));
    try std.testing.expectEqual(@as(u64, 16384), frames(1024 * mib, 4096));
    try std.testing.expectEqual(@as(u64, 16384), frames(64 * 1024 * mib, 4096));
    // Bytes of log, counted in the log's own frames.
    try std.testing.expectEqual(@as(u64, 250), frames(0, 16384));
    try std.testing.expectEqual(@as(u64, 800), frames(100 * mib, 16384));
    try std.testing.expectEqual(@as(u64, 4096), frames(1024 * mib, 16384));

    // A frame count that was chosen is never scaled.
    const chosen = (AutoCheckpointConfig{ .max_wal_frames = 7 }).resolved(1024 * mib, 4096);
    try std.testing.expectEqual(@as(?u64, 7), chosen.max_wal_frames);
    const default = (AutoCheckpointConfig{ .min_interval_ns = 5 }).resolved(100 * mib, 4096);
    try std.testing.expectEqual(@as(?u64, 3200), default.max_wal_frames);
    try std.testing.expectEqual(@as(u64, 5), default.min_interval_ns);
}

fn openInMemory(memory: *lattice.storage.memory_vfs.MemoryVfs, options: lattice.storage.database.OpenOptions) !*Database {
    return Database.openWithVfs(std.testing.allocator, "db", options, memory.vfs());
}

fn defaultFrames(db: *const Database) ?u64 {
    const policy = db.autoCheckpoint() orelse return null;
    return policy.max_wal_frames;
}

test "lfmt checkpoint: an interval that was chosen is used as given, and the default is scaled to the pool" {
    var memory = lattice.storage.memory_vfs.MemoryVfs.init(std.testing.allocator);
    defer memory.deinit();
    const pool = 40 * mib;

    // Chosen when opened: kept, whatever the pool.
    {
        const db = try openInMemory(&memory, .{ .create = true, .config = .{
            .enable_fts = false,
            .buffer_pool_size = pool,
            .auto_checkpoint = .{ .max_wal_frames = 1000, .mode = .full },
        } });
        defer db.close();
        try std.testing.expectEqual(@as(?u64, 1000), defaultFrames(db));
        try std.testing.expectEqual(lattice.storage.checkpoint.CheckpointMode.full, db.autoCheckpoint().?.mode);

        // Back to the default: scaled to this handle's pool.
        db.setAutoCheckpoint(.{});
        try std.testing.expectEqual(@as(?u64, 1280), defaultFrames(db));
        // Chosen later: kept.
        db.setAutoCheckpoint(.{ .max_wal_frames = 7 });
        try std.testing.expectEqual(@as(?u64, 7), defaultFrames(db));
        db.setAutoCheckpoint(null);
        try std.testing.expect(db.autoCheckpoint() == null);
    }

    // Not chosen: an eighth of the 40 MB pool.
    {
        const db = try openInMemory(&memory, .{ .config = .{ .enable_fts = false, .buffer_pool_size = pool } });
        defer db.close();
        try std.testing.expectEqual(@as(?u64, 1280), defaultFrames(db));
    }

    // A read-only handle and one without a log never checkpoint on commit,
    // and say what the default would be.
    {
        const db = try openInMemory(&memory, .{ .read_only = true, .config = .{ .enable_fts = false, .buffer_pool_size = pool } });
        defer db.close();
        try std.testing.expect(db.wal == null);
        try std.testing.expectEqual(@as(?u64, 1280), defaultFrames(db));
    }
    {
        const db = try openInMemory(&memory, .{ .config = .{ .enable_fts = false, .enable_wal = false, .buffer_pool_size = pool } });
        defer db.close();
        try std.testing.expect(db.wal == null);
        try std.testing.expectEqual(@as(?u64, 1280), defaultFrames(db));
        try commitNode(db);
    }

    // An in-memory database keeps a small pool whatever is asked for, so
    // its default stays at 1000 frames.
    {
        const db = try Database.open(std.testing.allocator, ":memory:", .{ .config = .{ .enable_fts = false, .buffer_pool_size = pool } });
        defer db.close();
        try std.testing.expectEqual(@as(?u64, 1000), defaultFrames(db));
    }
}

test "lfmt checkpoint: by default commits checkpoint once the log reaches the scaled size, not before" {
    var memory = lattice.storage.memory_vfs.MemoryVfs.init(std.testing.allocator);
    defer memory.deinit();
    const db = try openInMemory(&memory, .{ .create = true, .config = .{ .enable_fts = false, .buffer_pool_size = 40 * mib } });
    defer db.close();
    const wal = &db.wal.?;
    const threshold = defaultFrames(db).?;
    try std.testing.expectEqual(@as(u64, 1280), threshold);

    // Up to just under the threshold, and past the old 1000: no commit
    // checkpoints, so the log only grows.
    var last = wal.header.frame_count;
    while (wal.header.frame_count + 20 < threshold) {
        try commitNode(db);
        try std.testing.expect(wal.header.frame_count >= last);
        last = wal.header.frame_count;
    }
    try std.testing.expect(last > 1000);

    // Across it: a commit checkpoints and empties the log.
    var checkpointed = false;
    for (0..100) |_| {
        try commitNode(db);
        if (wal.header.frame_count < last) {
            checkpointed = true;
            break;
        }
        last = wal.header.frame_count;
    }
    try std.testing.expect(checkpointed);
    // The commit before it left the log just short of the threshold, and
    // this one, a frame or two, took it across.
    try std.testing.expect(last < threshold and last + 8 >= threshold);
    try std.testing.expect(wal.header.frame_count < 16);
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
