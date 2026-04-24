const std = @import("std");
const lattice = @import("lattice");

const Database = lattice.storage.database.Database;
const PropertyValue = lattice.core.types.PropertyValue;

fn cleanup(path: []const u8) void {
    std.fs.cwd().deleteFile(path) catch {};
    var wal_buf: [256]u8 = undefined;
    const wal_path = std.fmt.bufPrint(&wal_buf, "{s}-wal", .{path}) catch return;
    std.fs.cwd().deleteFile(wal_path) catch {};
}

fn openDb(allocator: std.mem.Allocator, path: []const u8) !*Database {
    return Database.open(allocator, path, .{
        .create = true,
        .config = .{
            .enable_wal = true,
            .enable_fts = false,
            .enable_vector = false,
        },
    });
}

fn findPayloadEntry(value: PropertyValue, key: []const u8) ?PropertyValue {
    if (value != .map_val) return null;
    for (value.map_val) |entry| {
        if (std.mem.eql(u8, entry.key, key)) return entry.value;
    }
    return null;
}

fn hasKind(batch: lattice.storage.database.StreamBatch, kind: []const u8) bool {
    for (batch.records) |record| {
        if (std.mem.eql(u8, record.kind, kind)) return true;
    }
    return false;
}

test "streams: publish read cursor offsets trim and reserved names" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_stream_integration.ltdb";
    cleanup(path);
    defer cleanup(path);

    var db = try openDb(allocator, path);
    defer db.close();

    {
        var txn = try db.beginTransaction(.read_write);
        try db.publishStream(&txn, "orders", "created", .{ .string_val = "one" });
        try db.publishStream(&txn, "orders", null, .{ .int_val = 2 });
        try std.testing.expectError(
            lattice.storage.database.DatabaseError.InvalidArgument,
            db.publishStream(&txn, "__lattice_user", null, .{ .null_val = {} }),
        );
        try db.commitTransaction(&txn);
    }

    var first = try db.readStream("orders", 0, 10, 0);
    defer first.deinit();
    try std.testing.expectEqual(@as(usize, 2), first.records.len);
    try std.testing.expectEqual(@as(u64, 1), first.records[0].sequence);
    try std.testing.expectEqualStrings("created", first.records[0].kind);
    try std.testing.expectEqualStrings("one", first.records[0].payload.string_val);
    try std.testing.expectEqual(@as(u64, 2), first.records[1].sequence);
    try std.testing.expectEqualStrings("message", first.records[1].kind);
    try std.testing.expectEqual(@as(i64, 2), first.records[1].payload.int_val);

    var after_first = try db.readStream("orders", 1, 10, 0);
    defer after_first.deinit();
    try std.testing.expectEqual(@as(usize, 1), after_first.records.len);
    try std.testing.expectEqual(@as(u64, 2), after_first.records[0].sequence);

    {
        var txn = try db.beginTransaction(.read_write);
        try db.setStreamOffset(&txn, "orders", "consumer-a", 2);
        try db.trimStream(&txn, "orders", 1);
        try db.commitTransaction(&txn);
    }

    const offset = try db.getStreamOffset("orders", "consumer-a");
    try std.testing.expect(offset != null);
    try std.testing.expectEqual(@as(u64, 2), offset.?);

    var after_trim = try db.readStream("orders", 0, 10, 0);
    defer after_trim.deinit();
    try std.testing.expectEqual(@as(usize, 1), after_trim.records.len);
    try std.testing.expectEqual(@as(u64, 2), after_trim.records[0].sequence);
}

test "streams: rollback keeps records invisible" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_stream_rollback.ltdb";
    cleanup(path);
    defer cleanup(path);

    var db = try openDb(allocator, path);
    defer db.close();

    {
        var txn = try db.beginTransaction(.read_write);
        try db.publishStream(&txn, "events", null, .{ .string_val = "hidden" });
        try db.abortTransaction(&txn);
    }

    var batch = try db.readStream("events", 0, 10, 0);
    defer batch.deinit();
    try std.testing.expectEqual(@as(usize, 0), batch.records.len);
}

test "streams: graph changefeed emits semantic events" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_stream_changefeed.ltdb";
    cleanup(path);
    defer cleanup(path);

    var db = try openDb(allocator, path);
    defer db.close();

    var node_id: u64 = 0;
    var edge_id: u64 = 0;
    {
        var txn = try db.beginTransaction(.read_write);
        node_id = try db.createNode(&txn, &[_][]const u8{"Person"});
        try db.setNodeProperty(&txn, node_id, "name", .{ .string_val = "Ada" });
        const other = try db.createNode(&txn, &[_][]const u8{"Person"});
        edge_id = try db.createEdgeAndGetId(&txn, node_id, other, "KNOWS");
        try db.setEdgePropertyById(&txn, edge_id, "since", .{ .int_val = 1843 });
        try db.commitTransaction(&txn);
    }

    var changes = try db.readChanges(0, 20, 0);
    defer changes.deinit();

    try std.testing.expect(hasKind(changes, "node.insert"));
    try std.testing.expect(hasKind(changes, "node.label_add"));
    try std.testing.expect(hasKind(changes, "node.property_set"));
    try std.testing.expect(hasKind(changes, "edge.insert"));
    try std.testing.expect(hasKind(changes, "edge.property_set"));

    var saw_node_property = false;
    for (changes.records) |record| {
        if (!std.mem.eql(u8, record.kind, "node.property_set")) continue;
        const key = findPayloadEntry(record.payload, "key").?;
        if (!std.mem.eql(u8, key.string_val, "name")) continue;
        const id = findPayloadEntry(record.payload, "node_id").?;
        try std.testing.expectEqual(@as(i64, @intCast(node_id)), id.int_val);
        saw_node_property = true;
    }
    try std.testing.expect(saw_node_property);
}
