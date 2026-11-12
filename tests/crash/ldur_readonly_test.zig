//! Read-only opens of a database a crash left with work to recover.
//!
//! Built on the in-memory fault VFS: a process is killed (every write kept,
//! no close), or the power is cut in the middle of a flush, and the result
//! is opened read-only. The open must show every committed transaction and
//! must not change a byte of the files.

const std = @import("std");
const lattice = @import("lattice");

const Allocator = std.mem.Allocator;
const database_mod = lattice.storage.database;
const Database = database_mod.Database;
const DatabaseConfig = database_mod.DatabaseConfig;
const fault = lattice.storage.fault_vfs;
const FaultVfs = fault.FaultVfs;
const Image = fault.Image;
const NodeId = lattice.core.types.NodeId;

const DB_PATH = "db";
const WAL_PATH = "db-wal";
const JOURNAL_PATH = "db-journal";

const config: DatabaseConfig = .{
    .enable_fts = false,
    .enable_vector = false,
    .auto_checkpoint = null,
    .enable_query_cache = false,
    .buffer_pool_size = 64 * 4096,
};

fn open(allocator: Allocator, fs: *FaultVfs, read_only: bool) !*Database {
    return Database.openWithVfs(allocator, DB_PATH, .{
        .create = !read_only,
        .read_only = read_only,
        .config = config,
    }, fs.vfs());
}

/// The files after the trace up to `cut`, cut as a process kill leaves them.
fn imageAt(allocator: Allocator, fs: *FaultVfs, cut: usize) !Image {
    var model = fault.DiskModel.init(allocator);
    defer model.deinit();
    try model.applyAll(fs.trace.items[0..cut]);
    var prng = std.Random.DefaultPrng.init(0);
    return model.crash(prng.random(), .everything);
}

fn expectSameFiles(a: *const Image, b: *const Image) !void {
    try std.testing.expectEqual(a.files.len, b.files.len);
    for (a.files, b.files) |x, y| {
        try std.testing.expectEqualStrings(x.name, y.name);
        try std.testing.expectEqualSlices(u8, x.data, y.data);
    }
}

/// Commit `count` nodes labelled A, each in its own transaction, and return
/// the files as a kill right after the last commit leaves them.
fn killedAfterCommits(allocator: Allocator, fs: *FaultVfs, ids: []NodeId) !Image {
    var db = try open(allocator, fs, false);
    defer db.close();
    for (ids) |*id| {
        var txn = try db.beginTransaction(.read_write);
        id.* = try db.createNode(&txn, &.{"A"});
        try db.setNodeProperty(&txn, id.*, "n", .{ .int_val = 7 });
        try db.commitTransaction(&txn);
    }
    return imageAt(allocator, fs, fs.traceLen());
}

test "a read-only open shows the commits a crash left only in the log" {
    const allocator = std.testing.allocator;
    var fs = FaultVfs.init(allocator);
    defer fs.deinit();
    var ids: [5]NodeId = undefined;
    var image = try killedAfterCommits(allocator, &fs, &ids);
    defer image.deinit(allocator);
    try std.testing.expect(image.get(WAL_PATH).?.len > lattice.storage.wal.WAL_HEADER_SIZE);

    var crashed = try FaultVfs.initFromImage(allocator, &image);
    defer crashed.deinit();
    {
        var db = try open(allocator, &crashed, true);
        defer db.close();
        for (ids) |id| try std.testing.expect(try db.nodeExists(id));
        const listed = try db.getNodesByLabel("A");
        defer allocator.free(listed);
        try std.testing.expectEqual(ids.len, listed.len);

        // Still a read-only handle.
        try std.testing.expect(db.isReadOnly());
        var txn = try db.beginTransaction(.read_write);
        try std.testing.expectError(error.PermissionDenied, db.createNode(&txn, &.{"A"}));
        db.abortTransaction(&txn) catch {};
    }

    // Not a byte of the files changed.
    try std.testing.expectEqual(@as(usize, 0), countWrites(&crashed));

    // And a writable open recovers them as it always did.
    var db = try open(allocator, &crashed, false);
    defer db.close();
    for (ids) |id| try std.testing.expect(try db.nodeExists(id));
}

fn countWrites(fs: *FaultVfs) usize {
    var n: usize = 0;
    for (fs.trace.items) |op| switch (op) {
        .write, .truncate, .create, .delete => n += 1,
        else => {},
    };
    return n;
}

test "a read-only open finishes a flush the journal holds" {
    const allocator = std.testing.allocator;
    var fs = FaultVfs.init(allocator);
    defer fs.deinit();

    var ids: [40]NodeId = undefined;
    var flush_cut: ?usize = null;
    {
        var db = try open(allocator, &fs, false);
        defer db.close();
        for (&ids) |*id| {
            var txn = try db.beginTransaction(.read_write);
            id.* = try db.createNode(&txn, &.{"A"});
            try db.commitTransaction(&txn);
        }
        const before = fs.traceLen();
        try db.sync();
        // The power goes once the journal is synced, before the pages it
        // protects are all in place.
        const journal_inode = fs.names.get(JOURNAL_PATH).?;
        for (fs.trace.items[before..], before..) |op, i| switch (op) {
            .sync => |sync_op| if (flush_cut == null and sync_op.inode == journal_inode) {
                flush_cut = i + 2;
            },
            else => {},
        };
    }
    var image = try imageAt(allocator, &fs, flush_cut.?);
    defer image.deinit(allocator);
    try std.testing.expect(if (image.get(JOURNAL_PATH)) |j| j.len > 0 else false);

    var crashed = try FaultVfs.initFromImage(allocator, &image);
    defer crashed.deinit();
    {
        var db = try open(allocator, &crashed, true);
        defer db.close();
        for (ids) |id| try std.testing.expect(try db.nodeExists(id));
    }
    try std.testing.expectEqual(@as(usize, 0), countWrites(&crashed));
}

test "a read-only open refuses a log damaged where it was synced" {
    const allocator = std.testing.allocator;
    var fs = FaultVfs.init(allocator);
    defer fs.deinit();
    var ids: [4]NodeId = undefined;
    var image = try killedAfterCommits(allocator, &fs, &ids);
    defer image.deinit(allocator);

    // Damage the first frame's data; every later frame vouches for it.
    var damaged = try image.clone(allocator);
    defer damaged.deinit(allocator);
    for (damaged.files) |*file| {
        if (std.mem.eql(u8, file.name, WAL_PATH)) {
            @memset(file.data[lattice.storage.wal.WAL_HEADER_SIZE + 40 ..][0..16], 0xFF);
        }
    }
    var crashed = try FaultVfs.initFromImage(allocator, &damaged);
    defer crashed.deinit();
    try std.testing.expectError(error.InvalidDatabase, open(allocator, &crashed, true));
}

test "a read-only open that would need too much memory to recover says so" {
    const allocator = std.testing.allocator;
    var fs = FaultVfs.init(allocator);
    defer fs.deinit();
    var ids: [30]NodeId = undefined;
    var image = try killedAfterCommits(allocator, &fs, &ids);
    defer image.deinit(allocator);

    database_mod.read_only_recovery_budget_for_testing = 2 * 4096;
    defer database_mod.read_only_recovery_budget_for_testing = null;
    var crashed = try FaultVfs.initFromImage(allocator, &image);
    defer crashed.deinit();
    try std.testing.expectError(error.BufferPoolFull, open(allocator, &crashed, true));
    try std.testing.expectEqual(@as(usize, 0), countWrites(&crashed));
}

test "a read-only open of a cleanly closed database reads the file as it is" {
    const allocator = std.testing.allocator;
    var fs = FaultVfs.init(allocator);
    defer fs.deinit();
    var id: NodeId = undefined;
    {
        var db = try open(allocator, &fs, false);
        defer db.close();
        var txn = try db.beginTransaction(.read_write);
        id = try db.createNode(&txn, &.{"A"});
        try db.commitTransaction(&txn);
        _ = try db.checkpoint(.truncate);
    }
    var image = try imageAt(allocator, &fs, fs.traceLen());
    defer image.deinit(allocator);
    var copy = try FaultVfs.initFromImage(allocator, &image);
    defer copy.deinit();

    database_mod.read_only_recovery_budget_for_testing = 0;
    defer database_mod.read_only_recovery_budget_for_testing = null;
    var db = try open(allocator, &copy, true);
    defer db.close();
    try std.testing.expect(try db.nodeExists(id));
}
