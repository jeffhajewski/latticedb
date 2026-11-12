//! Replay of the log into a file that already holds some of it.
//!
//! Pages reach the file whenever the buffer pool is flushed, and the log is
//! only emptied by a checkpoint, so after a crash recovery replays records
//! whose changes may already be in the file. Each test here builds such a
//! file on the in-memory fault VFS, kills the process there (keeps every
//! write, runs no close), and opens what is left.

const std = @import("std");
const lattice = @import("lattice");

const Allocator = std.mem.Allocator;
const Database = lattice.storage.database.Database;
const DatabaseConfig = lattice.storage.database.DatabaseConfig;
const fault = lattice.storage.fault_vfs;
const FaultVfs = fault.FaultVfs;
const Image = fault.Image;
const wal_mod = lattice.storage.wal;
const wal_payload = lattice.transaction.wal_payload;
const NodeId = lattice.core.types.NodeId;

const DB_PATH = "db";
const WAL_PATH = "db-wal";

const config: DatabaseConfig = .{
    .enable_fts = false,
    .enable_vector = false,
    // Only the test decides when the log is emptied.
    .auto_checkpoint = null,
    .enable_query_cache = false,
};

fn open(allocator: Allocator, fs: *FaultVfs, create: bool) !*Database {
    return Database.openWithVfs(allocator, DB_PATH, .{ .create = create, .config = config }, fs.vfs());
}

/// The files as a process killed now would leave them.
fn killImage(allocator: Allocator, fs: *FaultVfs) !Image {
    var model = fault.DiskModel.init(allocator);
    defer model.deinit();
    try model.applyAll(fs.trace.items);
    var prng = std.Random.DefaultPrng.init(0);
    return model.crash(prng.random(), .everything);
}

fn labelled(allocator: Allocator, db: *Database, label: []const u8) ![]NodeId {
    _ = allocator;
    return db.getNodesByLabel(label);
}

test "replay of a label removal the file already holds leaves the label index alone" {
    const allocator = std.testing.allocator;
    var fs = FaultVfs.init(allocator);
    defer fs.deinit();

    var image = blk: {
        var db = try open(allocator, &fs, true);
        defer db.close();

        var txn = try db.beginTransaction(.read_write);
        const id = try db.createNode(&txn, &.{"A"});
        try db.commitTransaction(&txn);

        txn = try db.beginTransaction(.read_write);
        try db.removeNodeLabel(&txn, id, "A");
        try db.commitTransaction(&txn);

        // Both commits in the file, both still in the log.
        try db.sync();
        break :blk try killImage(allocator, &fs);
    };
    defer image.deinit(allocator);

    var reopened = try FaultVfs.initFromImage(allocator, &image);
    defer reopened.deinit();
    var db = try open(allocator, &reopened, false);
    defer db.close();

    // Replaying the insert put the node back under A in the index, and
    // the removal, finding the node without A, left the entry there.
    const listed = try labelled(allocator, db, "A");
    defer allocator.free(listed);
    try std.testing.expectEqual(@as(usize, 0), listed.len);
}

test "replay of a node insert the file already holds does not keep later properties twice" {
    const allocator = std.testing.allocator;
    var fs = FaultVfs.init(allocator);
    defer fs.deinit();

    var id: NodeId = undefined;
    var image = blk: {
        var db = try open(allocator, &fs, true);
        defer db.close();

        var txn = try db.beginTransaction(.read_write);
        id = try db.createNode(&txn, &.{ "A", "B" });
        try db.setNodeProperty(&txn, id, "n", .{ .int_val = 1 });
        try db.commitTransaction(&txn);

        txn = try db.beginTransaction(.read_write);
        try db.removeNodeProperty(&txn, id, "n");
        try db.removeNodeLabel(&txn, id, "B");
        try db.setNodeProperty(&txn, id, "m", .{ .int_val = 2 });
        try db.commitTransaction(&txn);

        try db.sync();
        break :blk try killImage(allocator, &fs);
    };
    defer image.deinit(allocator);

    var reopened = try FaultVfs.initFromImage(allocator, &image);
    defer reopened.deinit();
    var db = try open(allocator, &reopened, false);
    defer db.close();

    const props = try db.getNodePropertiesInTxn(null, id);
    defer db.freePropertyEntries(props);
    try std.testing.expectEqual(@as(usize, 1), props.len);
    try std.testing.expectEqualStrings("m", props[0].key);

    const labels = try db.getNodeLabelsInTxn(null, id);
    defer {
        for (labels) |l| allocator.free(l);
        allocator.free(labels);
    }
    try std.testing.expectEqual(@as(usize, 1), labels.len);
    try std.testing.expectEqualStrings("A", labels[0]);

    const under_b = try labelled(allocator, db, "B");
    defer allocator.free(under_b);
    try std.testing.expectEqual(@as(usize, 0), under_b.len);
}

test "a node deleted later in the log may be missing when its changes replay" {
    const allocator = std.testing.allocator;
    var fs = FaultVfs.init(allocator);
    defer fs.deinit();

    var image = blk: {
        var db = try open(allocator, &fs, true);
        defer db.close();

        var txn = try db.beginTransaction(.read_write);
        const gone = try db.createNode(&txn, &.{"A"});
        const kept = try db.createNode(&txn, &.{"A"});
        try db.commitTransaction(&txn);

        txn = try db.beginTransaction(.read_write);
        try db.setNodeProperty(&txn, gone, "n", .{ .int_val = 5 });
        try db.addNodeLabel(&txn, gone, "B");
        const edge = try db.createEdgeAndGetId(&txn, gone, kept, "T");
        try db.setEdgePropertyById(&txn, edge, "w", .{ .int_val = 1 });
        try db.commitTransaction(&txn);

        txn = try db.beginTransaction(.read_write);
        try db.deleteEdgeById(&txn, edge);
        try db.deleteNode(&txn, gone);
        try db.commitTransaction(&txn);

        try db.sync();
        break :blk try killImage(allocator, &fs);
    };
    defer image.deinit(allocator);

    var reopened = try FaultVfs.initFromImage(allocator, &image);
    defer reopened.deinit();
    var db = try open(allocator, &reopened, false);
    defer db.close();
    try std.testing.expectEqual(@as(u64, 1), db.nodeCount());
    try std.testing.expectEqual(@as(u64, 0), db.edgeCount());
}

/// A database whose log ends with one committed transaction holding the
/// given record, put there by hand.
fn imageWithRecord(allocator: Allocator, record_type: wal_mod.WalRecordType, payload: []const u8) !Image {
    var fs = FaultVfs.init(allocator);
    defer fs.deinit();
    {
        var db = try open(allocator, &fs, true);
        defer db.close();
        var txn = try db.beginTransaction(.read_write);
        _ = try db.createNode(&txn, &.{"A"});
        try db.commitTransaction(&txn);
    }
    // The close emptied the log; the database file has a uuid the log
    // must carry.
    var db = try open(allocator, &fs, false);
    const uuid = db.page_manager.getHeader().file_uuid;
    const frame_size = db.page_manager.getPageSize();
    db.close();

    var wal = try wal_mod.WalManager.initWithFrameSize(allocator, fs.vfs(), WAL_PATH, uuid, frame_size);
    defer wal.deinit();
    const begin = try wal.appendRecord(.txn_begin, 1000, 0, &.{});
    const rec = try wal.appendRecord(record_type, 1000, begin, payload);
    _ = try wal.appendRecord(.txn_commit, 1000, rec, &.{});
    try wal.sync();
    return killImage(allocator, &fs);
}

fn expectOpenRefused(allocator: Allocator, image: *const Image) !void {
    var fs = try FaultVfs.initFromImage(allocator, image);
    defer fs.deinit();
    try std.testing.expectError(error.InvalidDatabase, open(allocator, &fs, false));
}

test "a committed record that does not decode refuses the open" {
    const allocator = std.testing.allocator;
    // A property update cut short after its type byte.
    var image = try imageWithRecord(allocator, .update, &.{@intFromEnum(wal_payload.PayloadType.node_update)});
    defer image.deinit(allocator);
    try expectOpenRefused(allocator, &image);
}

test "a committed record of a type no release writes refuses the open" {
    const allocator = std.testing.allocator;
    var image = try imageWithRecord(allocator, .update, &.{ 0xEE, 1, 2, 3 });
    defer image.deinit(allocator);
    try expectOpenRefused(allocator, &image);
}

test "a committed page image refuses the open" {
    const allocator = std.testing.allocator;
    var image = try imageWithRecord(allocator, .page_write, &([_]u8{0} ** 64));
    defer image.deinit(allocator);
    try expectOpenRefused(allocator, &image);
}

test "a committed change to a node that is not there and is not deleted later refuses the open" {
    const allocator = std.testing.allocator;
    var buf: [256]u8 = undefined;
    const payload = try wal_payload.serializePropertyUpdate(&buf, 987654, "n", null, &.{});
    var image = try imageWithRecord(allocator, .update, payload);
    defer image.deinit(allocator);
    try expectOpenRefused(allocator, &image);
}
