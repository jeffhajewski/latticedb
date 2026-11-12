//! Power cuts and the names of the database's files.
//!
//! Syncing a file makes its contents durable, not its name: a file created
//! since its directory was last synced can be gone after a power cut, with
//! everything in it. These tests cut the power on the in-memory fault VFS,
//! where a name that was not synced into its directory survives a cut or
//! not at random.

const std = @import("std");
const lattice = @import("lattice");

const Allocator = std.mem.Allocator;
const Database = lattice.storage.database.Database;
const DatabaseConfig = lattice.storage.database.DatabaseConfig;
const fault = lattice.storage.fault_vfs;
const FaultVfs = fault.FaultVfs;
const NodeId = lattice.core.types.NodeId;

const DB_PATH = "db";
const WAL_PATH = "db-wal";

const config: DatabaseConfig = .{
    .enable_fts = false,
    .enable_vector = false,
    .enable_query_cache = false,
};

fn open(allocator: Allocator, fs: *FaultVfs, create: bool) !*Database {
    return Database.openWithVfs(allocator, DB_PATH, .{ .create = create, .config = config }, fs.vfs());
}

test "a log created for a database opened without one survives a power cut" {
    const allocator = std.testing.allocator;

    var fs = FaultVfs.init(allocator);
    defer fs.deinit();
    {
        var db = try open(allocator, &fs, true);
        db.close();
    }
    // A backup put back without its log, which a clean close leaves
    // empty.
    try fs.vfs().delete(WAL_PATH);
    fs.vfs().syncParentDir(WAL_PATH);
    const before_open = fs.traceLen();

    var id: NodeId = undefined;
    var model = fault.DiskModel.init(allocator);
    defer model.deinit();
    {
        var db = try open(allocator, &fs, false);
        var txn = try db.beginTransaction(.read_write);
        id = try db.createNode(&txn, &.{"A"});
        try db.commitTransaction(&txn);
        // The commit has returned. The power goes before anything else.
        try model.applyAll(fs.trace.items);
        db.close();
    }
    try std.testing.expect(fs.traceLen() > before_open);

    // Whether the name survives is a coin toss in the model; enough tosses
    // that a name left unsynced is lost in one of them.
    for (0..16) |seed| {
        var prng = std.Random.DefaultPrng.init(seed);
        var image = try model.crash(prng.random(), .in_order);
        defer image.deinit(allocator);
        var crashed = try FaultVfs.initFromImage(allocator, &image);
        defer crashed.deinit();
        var db = try open(allocator, &crashed, false);
        defer db.close();
        try std.testing.expect(try db.nodeExists(id));
    }
}
