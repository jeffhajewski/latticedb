//! A power cut with automatic checkpoints switched off.
//!
//! With them off the log is the only place the commits are, however long it
//! gets: recovery has to replay all of it.

const std = @import("std");
const lattice = @import("lattice");

const Database = lattice.storage.database.Database;
const fault = lattice.storage.fault_vfs;
const FaultVfs = fault.FaultVfs;
const NodeId = lattice.core.types.NodeId;

const DB_PATH = "db";

fn open(allocator: std.mem.Allocator, fs: *FaultVfs, create: bool) !*Database {
    return Database.openWithVfs(allocator, DB_PATH, .{
        .create = create,
        .config = .{ .enable_fts = false, .enable_vector = false, .enable_query_cache = false },
    }, fs.vfs());
}

test "lfmt commits survive a power cut with automatic checkpoints off, and an explicit one" {
    const allocator = std.testing.allocator;
    var fs = FaultVfs.init(allocator);
    defer fs.deinit();

    const count = 1200;
    var ids: [count]NodeId = undefined;
    var model = fault.DiskModel.init(allocator);
    defer model.deinit();
    {
        var db = try open(allocator, &fs, true);
        defer db.close();
        db.setAutoCheckpoint(null);
        for (&ids, 0..) |*id, i| {
            var txn = try db.beginTransaction(.read_write);
            id.* = try db.createNode(&txn, &.{"N"});
            try db.commitTransaction(&txn);
            // An explicit checkpoint part of the way through: the commits
            // after it are in the log alone again.
            if (i == count / 2) _ = try db.checkpoint(.truncate);
        }
        // Nothing has checkpointed since the explicit one.
        try std.testing.expect(db.wal.?.header.frame_count >= count / 2 - 1);
        // Every commit has returned. The power goes before close.
        try model.applyAll(fs.trace.items);
    }

    for (0..4) |seed| {
        var prng = std.Random.DefaultPrng.init(seed);
        var image = try model.crash(prng.random(), .in_order);
        defer image.deinit(allocator);
        var crashed = try FaultVfs.initFromImage(allocator, &image);
        defer crashed.deinit();
        var db = try open(allocator, &crashed, false);
        defer db.close();
        for (ids) |id| try std.testing.expect(try db.nodeExists(id));
    }
}
