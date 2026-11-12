//! A power cut while a log is being created.
//!
//! An open that finds no log creates one: it writes the header and syncs
//! it before anything is logged. A cut before that sync can leave the
//! header partly on the disk, in any subset of its sectors. Such a log
//! holds nothing, and the database must open; it used to fail with an I/O
//! error, which the power-loss harness found after a log was removed while
//! the database was closed (seed 5, cut 304: seven of eight sectors).

const std = @import("std");
const lattice = @import("lattice");

const Allocator = std.mem.Allocator;
const Database = lattice.storage.database.Database;
const DatabaseConfig = lattice.storage.database.DatabaseConfig;
const FaultVfs = lattice.storage.fault_vfs.FaultVfs;
const WAL_HEADER_SIZE = lattice.storage.wal.WAL_HEADER_SIZE;

const DB_PATH = "db";
const WAL_PATH = "db-wal";
const SECTOR = 512;

const config: DatabaseConfig = .{
    .enable_fts = false,
    .enable_vector = false,
    .enable_query_cache = false,
};

fn open(fs: *FaultVfs, create: bool) !*Database {
    return Database.openWithVfs(std.testing.allocator, DB_PATH, .{ .create = create, .config = config }, fs.vfs());
}

/// A database with one committed node, closed, and the header of the log
/// it left behind.
fn setUp(fs: *FaultVfs, header: *[WAL_HEADER_SIZE]u8) !void {
    {
        var db = try open(fs, true);
        defer db.close();
        var txn = try db.beginTransaction(.read_write);
        _ = try db.createNode(&txn, &.{"Kept"});
        try db.commitTransaction(&txn);
    }
    const file = try fs.vfs().open(WAL_PATH, .{ .read = true, .write = true });
    defer file.close();
    try std.testing.expectEqual(@as(usize, WAL_HEADER_SIZE), try file.read(0, header));
}

/// Replace the log with these sectors of `header`, at their offsets.
fn writeLog(fs: *FaultVfs, header: *const [WAL_HEADER_SIZE]u8, sectors: []const usize) !void {
    try fs.vfs().delete(WAL_PATH);
    const file = try fs.vfs().open(WAL_PATH, .{ .read = true, .write = true, .create = true });
    defer file.close();
    for (sectors) |i| try file.write(i * SECTOR, header[i * SECTOR ..][0..SECTOR]);
}

fn expectOpensWithData(fs: *FaultVfs) !void {
    {
        var db = try open(fs, false);
        defer db.close();
        const kept = try db.getNodesByLabel("Kept");
        defer std.testing.allocator.free(kept);
        try std.testing.expectEqual(@as(usize, 1), kept.len);

        // The new log takes commits, and they survive a reopen.
        var txn = try db.beginTransaction(.read_write);
        _ = try db.createNode(&txn, &.{"After"});
        try db.commitTransaction(&txn);
    }
    var db = try open(fs, false);
    defer db.close();
    const after = try db.getNodesByLabel("After");
    defer std.testing.allocator.free(after);
    try std.testing.expectEqual(@as(usize, 1), after.len);
}

test "lcf a log whose header lost its last sector opens as an empty log" {
    var fs = FaultVfs.init(std.testing.allocator);
    defer fs.deinit();
    var header: [WAL_HEADER_SIZE]u8 = undefined;
    try setUp(&fs, &header);
    try writeLog(&fs, &header, &.{ 0, 1, 2, 3, 4, 5, 6 });
    try expectOpensWithData(&fs);
}

test "lcf a log whose header lost its first sector opens as an empty log" {
    var fs = FaultVfs.init(std.testing.allocator);
    defer fs.deinit();
    var header: [WAL_HEADER_SIZE]u8 = undefined;
    try setUp(&fs, &header);
    try writeLog(&fs, &header, &.{ 1, 2, 3, 4, 5, 6, 7 });
    try expectOpensWithData(&fs);
}

test "lcf a log with only its first sector of header opens as an empty log" {
    var fs = FaultVfs.init(std.testing.allocator);
    defer fs.deinit();
    var header: [WAL_HEADER_SIZE]u8 = undefined;
    try setUp(&fs, &header);
    try writeLog(&fs, &header, &.{0});
    try expectOpensWithData(&fs);
}
