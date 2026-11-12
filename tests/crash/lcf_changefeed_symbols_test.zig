//! Changefeed events name labels, property keys and edge types by symbol
//! id, and interning a symbol is not logged. After a crash, replay interns
//! the names again, and the ids it hands out can differ from the ones the
//! events were written with: a symbol interned by a transaction that rolled
//! back is gone, and every later name moves down. The log therefore carries
//! names, and replay maps each event to the id its name has now.

const std = @import("std");
const lattice = @import("lattice");

const Allocator = std.mem.Allocator;
const Database = lattice.storage.database.Database;
const DatabaseConfig = lattice.storage.database.DatabaseConfig;
const fault = lattice.storage.fault_vfs;
const FaultVfs = fault.FaultVfs;
const Image = fault.Image;
const PropertyValue = lattice.core.types.PropertyValue;

const DB_PATH = "db";

const config: DatabaseConfig = .{
    .enable_fts = false,
    .enable_vector = false,
    // Only the test decides when anything reaches the file.
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

fn payloadString(record: anytype, key: []const u8) ?[]const u8 {
    for (record.payload.map_val) |entry| {
        if (std.mem.eql(u8, entry.key, key)) return entry.value.string_val;
    }
    return null;
}

/// The kind of each event, and the symbol name it carries, if any.
fn expectEvents(db: *Database, expected: []const [2][]const u8) !void {
    var batch = try db.readChanges(0, 100, 0);
    defer batch.deinit();
    try std.testing.expectEqual(expected.len, batch.records.len);
    for (batch.records, expected) |record, want| {
        try std.testing.expectEqualStrings(want[0], record.kind);
        const name = payloadString(record, "label") orelse
            payloadString(record, "key") orelse
            payloadString(record, "type") orelse "";
        try std.testing.expectEqualStrings(want[1], name);
    }
}

test "lcf a transaction that interns a label and logs an event naming it replays with the label's new id" {
    const allocator = std.testing.allocator;
    var fs = FaultVfs.init(allocator);
    defer fs.deinit();

    var ids_before: [3]u16 = undefined;
    var image = blk: {
        var db = try open(allocator, &fs, true);
        defer db.close();

        // Interns three names, then rolls back. The names stay in the
        // symbol table in memory; nothing about them is logged.
        var ghost = try db.beginTransaction(.read_write);
        const g = try db.createNode(&ghost, &.{"Ghost"});
        try db.setNodeProperty(&ghost, g, "phantom", .{ .int_val = 1 });
        _ = try db.createEdgeAndGetId(&ghost, g, g, "HAUNTS");
        try db.abortTransaction(&ghost);

        // Interns new names and logs events that refer to them by id.
        var txn = try db.beginTransaction(.read_write);
        const a = try db.createNode(&txn, &.{"Person"});
        try db.setNodeProperty(&txn, a, "name", .{ .string_val = "Ada" });
        const b = try db.createNode(&txn, &.{});
        _ = try db.createEdgeAndGetId(&txn, a, b, "KNOWS");
        try db.commitTransaction(&txn);

        ids_before = .{
            try db.symbol_table.lookup("Person"),
            try db.symbol_table.lookup("name"),
            try db.symbol_table.lookup("KNOWS"),
        };
        // Killed before anything but the log reached the disk.
        break :blk try killImage(allocator, &fs);
    };
    defer image.deinit(allocator);

    const expected = [_][2][]const u8{
        .{ "node.insert", "" },
        .{ "node.label_add", "Person" },
        .{ "node.property_set", "name" },
        .{ "node.insert", "" },
        .{ "edge.insert", "KNOWS" },
    };

    var reopened = try FaultVfs.initFromImage(allocator, &image);
    defer reopened.deinit();
    {
        var db = try open(allocator, &reopened, false);
        defer db.close();

        // The rolled-back names are gone, so the committed ones moved.
        try std.testing.expectError(error.NotFound, db.symbol_table.lookup("Ghost"));
        const ids_after = [_]u16{
            try db.symbol_table.lookup("Person"),
            try db.symbol_table.lookup("name"),
            try db.symbol_table.lookup("KNOWS"),
        };
        for (ids_before, ids_after) |before, after| try std.testing.expect(before != after);

        try expectEvents(db, &expected);
        try db.sync();
    }
    // And from the file alone, once the replayed events are in it.
    var db = try open(allocator, &reopened, false);
    defer db.close();
    try expectEvents(db, &expected);
}
