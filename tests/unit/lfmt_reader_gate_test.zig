//! The file's reader version, raised when it first stores something 0.16.0
//! would misread: compact changefeed events, a vector index 0.16.0 cannot
//! describe, an ordered property index, a full-text index that stores word
//! positions. 0.16.0 refuses a file whose `min_reader_version` is above its
//! format (3).

const std = @import("std");
const lattice = @import("lattice");
const compat = @import("compat");

const Database = lattice.storage.database.Database;
const DatabaseConfig = lattice.storage.database.DatabaseConfig;
const FileHeader = lattice.storage.page.FileHeader;
const Reason = FileHeader.ReaderReason;
const types = lattice.core.types;

fn cleanup(comptime path: []const u8) void {
    compat.fs.cwd().deleteFile(path) catch {};
    compat.fs.cwd().deleteFile(path ++ "-wal") catch {};
    compat.fs.cwd().deleteFile(path ++ "-journal") catch {};
}

const OnDisk = struct { version: u16, reasons: u16 };

/// The two fields as the database file holds them, whatever the process has
/// in memory.
fn onDisk(comptime path: []const u8) !OnDisk {
    const file = try compat.fs.cwd().openFile(path, .{});
    defer file.close();
    var buf: [@sizeOf(FileHeader)]u8 = undefined;
    const n = try file.preadAll(&buf, 0);
    try std.testing.expectEqual(buf.len, n);
    return .{
        .version = std.mem.readInt(u16, buf[@offsetOf(FileHeader, "min_reader_version")..][0..2], .little),
        .reasons = std.mem.readInt(u16, buf[@offsetOf(FileHeader, "min_reader_reasons")..][0..2], .little),
    };
}

fn config(enable_wal: bool) DatabaseConfig {
    return .{ .enable_wal = enable_wal, .enable_fts = false, .enable_vector = false, .enable_query_cache = false };
}

fn openRw(comptime path: []const u8, cfg: DatabaseConfig, create: bool) !*Database {
    return Database.open(std.testing.allocator, path, .{ .create = create, .config = cfg });
}

fn commitNode(db: *Database) !void {
    var txn = try db.beginTransaction(.read_write);
    errdefer db.abortTransaction(&txn) catch {};
    _ = try db.createNode(&txn, &.{"Person"});
    try db.commitTransaction(&txn);
}

fn countChanges(db: *Database) !usize {
    var batch = try db.readChanges(0, 1000, 0);
    defer batch.deinit();
    return batch.records.len;
}

test "lfmt gate: a new file, and opens that store no event, leave the reader version at 3" {
    inline for (.{ true, false }) |wal| {
        const path = if (wal) "/tmp/lfmt_gate_untouched.db" else "/tmp/lfmt_gate_untouched_nolog.db";
        cleanup(path);
        defer cleanup(path);

        {
            const db = try openRw(path, config(wal), true);
            db.close();
        }
        try std.testing.expectEqual(types.MIN_READER_VERSION, (try onDisk(path)).version);

        // Read-write, writing nothing.
        {
            const db = try openRw(path, config(wal), false);
            db.close();
        }
        try std.testing.expectEqual(types.MIN_READER_VERSION, (try onDisk(path)).version);

        // Read-only.
        {
            const db = try Database.open(std.testing.allocator, path, .{ .read_only = true, .config = config(wal) });
            db.close();
        }
        try std.testing.expectEqual(OnDisk{ .version = types.MIN_READER_VERSION, .reasons = 0 }, try onDisk(path));
    }
}

test "lfmt gate: the first changefeed event raises the reader version on disk before its commit returns" {
    inline for (.{ true, false }) |wal| {
        const path = if (wal) "/tmp/lfmt_gate_event.db" else "/tmp/lfmt_gate_event_nolog.db";
        cleanup(path);
        defer cleanup(path);

        const db = try openRw(path, config(wal), true);
        defer db.close();
        try std.testing.expectEqual(types.MIN_READER_VERSION, (try onDisk(path)).version);
        try commitNode(db);
        try std.testing.expect(try countChanges(db) > 0);
        // On disk already, with the database still open and nothing
        // checkpointed.
        try std.testing.expectEqual(OnDisk{ .version = types.FORMAT_VERSION, .reasons = Reason.changefeed_events }, try onDisk(path));
        try std.testing.expectEqual(types.FORMAT_VERSION, db.page_manager.getHeader().min_reader_version);
    }
}

test "lfmt gate: with retention off no event is stored and the version stays" {
    inline for (.{ true, false }) |wal| {
        const path = if (wal) "/tmp/lfmt_gate_off.db" else "/tmp/lfmt_gate_off_nolog.db";
        cleanup(path);
        defer cleanup(path);

        var cfg = config(wal);
        cfg.changefeed_retention = .off;
        {
            const db = try openRw(path, cfg, true);
            defer db.close();
            for (0..5) |_| try commitNode(db);
        }
        try std.testing.expectEqual(OnDisk{ .version = types.MIN_READER_VERSION, .reasons = 0 }, try onDisk(path));
    }
}

test "lfmt gate: a user stream is not the changefeed and does not raise the version" {
    const path = "/tmp/lfmt_gate_stream.db";
    cleanup(path);
    defer cleanup(path);

    {
        const db = try openRw(path, config(true), true);
        defer db.close();
        var txn = try db.beginTransaction(.read_write);
        try db.publishStream(&txn, "orders", "created", .{ .int_val = 1 });
        try db.commitTransaction(&txn);
    }
    try std.testing.expectEqual(types.MIN_READER_VERSION, (try onDisk(path)).version);
}

test "lfmt gate: once raised the version stays, also after every event is dropped" {
    const path = "/tmp/lfmt_gate_stays.db";
    cleanup(path);
    defer cleanup(path);

    {
        const db = try openRw(path, config(true), true);
        defer db.close();
        try commitNode(db);
    }
    try std.testing.expectEqual(types.FORMAT_VERSION, (try onDisk(path)).version);

    // Switching the changefeed off drops every event it holds.
    var cfg = config(true);
    cfg.changefeed_retention = .off;
    {
        const db = try openRw(path, cfg, false);
        defer db.close();
        _ = try db.trimChangefeed();
        const status = try db.changefeedStatus();
        try std.testing.expect(status.oldest_sequence == null);
        try commitNode(db);
    }
    try std.testing.expectEqual(OnDisk{ .version = types.FORMAT_VERSION, .reasons = Reason.changefeed_events }, try onDisk(path));
}

test "lfmt gate: the other raises record their reason beside the changefeed's" {
    const path = "/tmp/lfmt_gate_reasons.db";
    cleanup(path);
    defer cleanup(path);

    var cfg = config(true);
    cfg.enable_vector = true;
    cfg.vector_dimensions = 2;
    cfg.vector_metric = .euclidean;
    {
        const db = try openRw(path, cfg, true);
        defer db.close();
        try db.createNodePropertyIndex("Person", "name");
        try commitNode(db);
    }
    const disk = try onDisk(path);
    try std.testing.expectEqual(types.FORMAT_VERSION, disk.version);
    try std.testing.expectEqual(Reason.vector_index | Reason.ordered_property_index | Reason.changefeed_events, disk.reasons);
}

test "lfmt gate: a full-text index that stores positions records its reason, with the changefeed off" {
    const path = "/tmp/lfmt_gate_positions.db";
    cleanup(path);
    defer cleanup(path);

    var cfg = config(true);
    cfg.enable_fts = true;
    cfg.fts_config.store_positions = true;
    cfg.changefeed_retention = .off;
    {
        const db = try openRw(path, cfg, true);
        defer db.close();
        try db.createNodeFtsIndex("Doc", "text");
        var txn = try db.beginTransaction(.read_write);
        const id = try db.createNode(&txn, &.{"Doc"});
        try db.setNodeProperty(&txn, id, "text", .{ .string_val = "the quick brown fox" });
        try db.commitTransaction(&txn);
    }
    try std.testing.expectEqual(OnDisk{ .version = types.FORMAT_VERSION, .reasons = Reason.fulltext_positions }, try onDisk(path));
}
