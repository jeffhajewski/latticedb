//! A power cut around the first changefeed event a file stores.
//!
//! 0.16.0 opens a file whose `min_reader_version` is 3 and reads the database
//! file as it stands, and it would misread compact changefeed events. So no
//! state the disk can be left in may hold a committed event while the file's
//! header still says 3. Recovery by this release shows whatever the disk
//! holds durably (in the log or in the file); the header is read raw, before
//! recovery touches it.

const std = @import("std");
const lattice = @import("lattice");

const Database = lattice.storage.database.Database;
const FileHeader = lattice.storage.page.FileHeader;
const fault = lattice.storage.fault_vfs;
const FaultVfs = fault.FaultVfs;
const types = lattice.core.types;

const DB_PATH = "db";

fn open(allocator: std.mem.Allocator, fs: *FaultVfs, create: bool, enable_wal: bool) !*Database {
    return Database.openWithVfs(allocator, DB_PATH, .{
        .create = create,
        .config = .{ .enable_wal = enable_wal, .enable_fts = false, .enable_vector = false, .enable_query_cache = false },
    }, fs.vfs());
}

fn commitNode(db: *Database) !void {
    var txn = try db.beginTransaction(.read_write);
    errdefer db.abortTransaction(&txn) catch {};
    _ = try db.createNode(&txn, &.{"Person"});
    try db.commitTransaction(&txn);
}

fn rawReaderVersion(image: *const fault.Image) u16 {
    const data = image.get(DB_PATH) orelse return 0;
    const at = @offsetOf(FileHeader, "min_reader_version");
    if (data.len < at + 2) return 0;
    return std.mem.readInt(u16, data[at..][0..2], .little);
}

fn runGateCrash(enable_wal: bool) !void {
    const allocator = std.testing.allocator;
    var fs = FaultVfs.init(allocator);
    defer fs.deinit();

    // A file with no events, closed cleanly: 0.16.0 may open it.
    {
        var db = try open(allocator, &fs, true, enable_wal);
        db.close();
    }
    const start = fs.traceLen();
    {
        var db = try open(allocator, &fs, false, enable_wal);
        defer db.close();
        try commitNode(db);
        _ = try db.checkpoint(.truncate);
        try commitNode(db);
    }
    const trace = fs.trace.items;

    var before_raise: usize = 0;
    var with_events: usize = 0;
    var cut = start;
    while (cut <= trace.len) : (cut += 1) {
        for ([_]fault.DiskModel.Style{ .in_order, .reordered }) |style| {
            for (0..2) |seed| {
                var prng = std.Random.DefaultPrng.init(seed * 7919 + cut);
                var image = try fault.crashImage(allocator, null, trace[0..cut], prng.random(), style);
                defer image.deinit(allocator);
                const version = rawReaderVersion(&image);

                var crashed = try FaultVfs.initFromImage(allocator, &image);
                defer crashed.deinit();
                var db = try open(allocator, &crashed, false, enable_wal);
                defer db.close();
                var batch = try db.readChanges(0, 100, 0);
                defer batch.deinit();

                if (batch.records.len > 0) {
                    with_events += 1;
                    if (version < types.FORMAT_VERSION) {
                        std.debug.print("cut {d}/{d} {s} seed {d}: {d} events durable, header still says {d}\n", .{
                            cut, trace.len, @tagName(style), seed, batch.records.len, version,
                        });
                        return error.TestUnexpectedResult;
                    }
                } else if (version < types.FORMAT_VERSION) {
                    before_raise += 1;
                }
            }
        }
    }
    // The cuts reach both sides of the raise.
    try std.testing.expect(before_raise > 0);
    try std.testing.expect(with_events > 0);
}

test "lfmt gate: no power cut leaves a durable changefeed event under a header 0.16.0 opens" {
    try runGateCrash(true);
}

test "lfmt gate: no power cut leaves a durable changefeed event under a header 0.16.0 opens, without a log" {
    try runGateCrash(false);
}
