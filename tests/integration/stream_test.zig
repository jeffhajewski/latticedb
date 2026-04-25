const std = @import("std");
const lattice = @import("lattice");

const Database = lattice.storage.database.Database;
const PropertyValue = lattice.core.types.PropertyValue;
const FileHeader = lattice.storage.page.FileHeader;
const NULL_PAGE = lattice.core.types.NULL_PAGE;

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

fn openReadOnlyDb(allocator: std.mem.Allocator, path: []const u8) !*Database {
    return Database.open(allocator, path, .{
        .create = false,
        .read_only = true,
        .config = .{
            .enable_wal = true,
            .enable_fts = false,
            .enable_vector = false,
        },
    });
}

fn clearStreamRoots(path: []const u8) !void {
    const file = try std.fs.cwd().openFile(path, .{ .mode = .read_write });
    defer file.close();

    var header_buf: [4096]u8 = undefined;
    const n = try file.preadAll(&header_buf, 0);
    if (n < @sizeOf(FileHeader)) return error.HeaderTooSmall;

    const header: *FileHeader = @ptrCast(@alignCast(&header_buf));
    header.setTreeRoot(.stream_meta, NULL_PAGE);
    header.setTreeRoot(.stream_events, NULL_PAGE);
    header.setTreeRoot(.stream_offsets, NULL_PAGE);

    try file.pwriteAll(&header_buf, 0);
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

test "streams: read waits and wakes after same-process commit" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_stream_wake.ltdb";
    cleanup(path);
    defer cleanup(path);

    var db = try openDb(allocator, path);
    defer db.close();

    const Reader = struct {
        db: *Database,
        status: std.atomic.Value(u8) = std.atomic.Value(u8).init(0),
        elapsed_ms: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),

        fn run(self: *@This()) void {
            const start_ns = std.time.nanoTimestamp();
            var batch = self.db.readStream("wake", 0, 10, 5000) catch {
                self.status.store(2, .release);
                return;
            };
            defer batch.deinit();
            const elapsed_ns = std.time.nanoTimestamp() - start_ns;
            self.elapsed_ms.store(@intCast(@divTrunc(elapsed_ns, std.time.ns_per_ms)), .release);
            if (batch.records.len != 1) {
                self.status.store(3, .release);
                return;
            }
            if (batch.records[0].sequence != 1) {
                self.status.store(4, .release);
                return;
            }
            if (batch.records[0].payload != .string_val or
                !std.mem.eql(u8, batch.records[0].payload.string_val, "ready"))
            {
                self.status.store(5, .release);
                return;
            }
            self.status.store(1, .release);
        }
    };

    var reader = Reader{ .db = db };
    const thread = try std.Thread.spawn(.{}, Reader.run, .{&reader});
    std.Thread.sleep(50 * std.time.ns_per_ms);

    {
        var txn = try db.beginTransaction(.read_write);
        try db.publishStream(&txn, "wake", null, .{ .string_val = "ready" });
        try db.commitTransaction(&txn);
    }

    thread.join();
    try std.testing.expectEqual(@as(u8, 1), reader.status.load(.acquire));
    try std.testing.expect(reader.elapsed_ms.load(.acquire) < 2000);
}

test "streams: read-only database without stream roots reads empty and rejects writes" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_stream_readonly_old.ltdb";
    cleanup(path);
    defer cleanup(path);

    {
        var db = try openDb(allocator, path);
        var txn = try db.beginTransaction(.read_write);
        _ = try db.createNode(&txn, &[_][]const u8{"Legacy"});
        try db.commitTransaction(&txn);
        db.close();
    }

    try clearStreamRoots(path);

    var db = try openReadOnlyDb(allocator, path);
    defer db.close();

    var batch = try db.readStream("missing", 0, 10, 0);
    defer batch.deinit();
    try std.testing.expectEqual(@as(usize, 0), batch.records.len);

    const offset = try db.getStreamOffset("missing", "reader");
    try std.testing.expect(offset == null);

    try std.testing.expectError(
        lattice.storage.database.DatabaseError.TransactionsNotEnabled,
        db.beginTransaction(.read_write),
    );
}

test "streams: invalid limit and name validation return invalid argument" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_stream_validation.ltdb";
    cleanup(path);
    defer cleanup(path);

    var db = try openDb(allocator, path);
    defer db.close();

    try std.testing.expectError(
        lattice.storage.database.DatabaseError.InvalidArgument,
        db.readStream("events", 0, 0, 0),
    );

    const invalid_utf8 = [_]u8{0xff};
    try std.testing.expectError(
        lattice.storage.database.DatabaseError.InvalidArgument,
        db.readStream(invalid_utf8[0..], 0, 10, 0),
    );

    var long_name = [_]u8{'a'} ** 256;
    try std.testing.expectError(
        lattice.storage.database.DatabaseError.InvalidArgument,
        db.readStream(long_name[0..], 0, 10, 0),
    );

    {
        var txn = try db.beginTransaction(.read_write);
        defer db.abortTransaction(&txn) catch {};

        try std.testing.expectError(
            lattice.storage.database.DatabaseError.InvalidArgument,
            db.publishStream(&txn, "", null, .{ .null_val = {} }),
        );
        try std.testing.expectError(
            lattice.storage.database.DatabaseError.InvalidArgument,
            db.publishStream(&txn, "events", "", .{ .null_val = {} }),
        );
        try std.testing.expectError(
            lattice.storage.database.DatabaseError.InvalidArgument,
            db.publishStream(&txn, "events", invalid_utf8[0..], .{ .null_val = {} }),
        );
        try std.testing.expectError(
            lattice.storage.database.DatabaseError.InvalidArgument,
            db.setStreamOffset(&txn, "events", "", 1),
        );
        try std.testing.expectError(
            lattice.storage.database.DatabaseError.InvalidArgument,
            db.setStreamOffset(&txn, "events", invalid_utf8[0..], 1),
        );
        try std.testing.expectError(
            lattice.storage.database.DatabaseError.InvalidArgument,
            db.trimStream(&txn, invalid_utf8[0..], 1),
        );
    }
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
