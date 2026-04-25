//! Behavioral tests for durable stream storage helpers.

const std = @import("std");
const helpers = @import("helpers.zig");
const lattice = @import("lattice");

const PropertyValue = lattice.core.types.PropertyValue;
const stream_payload = lattice.stream.payload;
const stream_store = lattice.stream.store;

test "stream_store: offset key encodes stream and consumer names" {
    const allocator = std.testing.allocator;

    const key = try stream_store.encodeOffsetKey(allocator, "orders", "worker-a");
    defer allocator.free(key);

    try std.testing.expectEqual(@as(usize, 16), key.len);
    try std.testing.expectEqual(@as(u8, 6), key[0]);
    try std.testing.expectEqualStrings("orders", key[1..7]);
    try std.testing.expectEqual(@as(u8, 8), key[7]);
    try std.testing.expectEqualStrings("worker-a", key[8..16]);
}

test "stream_store: WAL payloads round trip append offset and trim records" {
    const allocator = std.testing.allocator;

    const append = try stream_store.serializeAppendWalAlloc(
        allocator,
        "orders",
        42,
        "created",
        PropertyValue{ .int_val = 7 },
    );
    defer allocator.free(append);

    const decoded_append = try stream_store.deserializeAppendWal(append);
    try std.testing.expectEqualStrings("orders", decoded_append.stream);
    try std.testing.expectEqual(@as(u64, 42), decoded_append.sequence);
    try std.testing.expectEqualStrings("created", decoded_append.kind);

    var decoded_payload = try stream_payload.decode(allocator, decoded_append.payload_bytes);
    defer decoded_payload.deinit(allocator);
    try std.testing.expectEqual(@as(i64, 7), decoded_payload.int_val);

    const offset = try stream_store.serializeOffsetWalAlloc(allocator, "orders", "worker-a", 41);
    defer allocator.free(offset);

    const decoded_offset = try stream_store.deserializeOffsetWal(offset);
    try std.testing.expectEqualStrings("orders", decoded_offset.stream);
    try std.testing.expectEqualStrings("worker-a", decoded_offset.consumer);
    try std.testing.expectEqual(@as(u64, 41), decoded_offset.sequence);

    const trim = try stream_store.serializeTrimWalAlloc(allocator, "orders", 40);
    defer allocator.free(trim);

    const decoded_trim = try stream_store.deserializeTrimWal(trim);
    try std.testing.expectEqualStrings("orders", decoded_trim.stream);
    try std.testing.expectEqual(@as(u64, 40), decoded_trim.through_sequence);

    try std.testing.expectError(stream_store.StreamError.InvalidRecord, stream_store.deserializeAppendWal(offset));
    try std.testing.expectError(stream_store.StreamError.InvalidRecord, stream_store.deserializeOffsetWal(trim));
    try std.testing.expectError(stream_store.StreamError.InvalidRecord, stream_store.deserializeTrimWal(append));
}

test "stream_store: trim deletes records through sequence without moving offsets" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.init(allocator, "stream_trim");
    defer db.deinit();

    var meta = try db.createBTree();
    var events = try db.createBTree();
    var offsets = try db.createBTree();
    const trees = stream_store.TreeSet{
        .meta = &meta,
        .events = &events,
        .offsets = &offsets,
    };

    try stream_store.appendWithSequence(trees, allocator, "orders", 1, "message", .{ .string_val = "one" });
    try stream_store.appendWithSequence(trees, allocator, "orders", 2, "message", .{ .string_val = "two" });
    try stream_store.appendWithSequence(trees, allocator, "orders", 3, "message", .{ .string_val = "three" });
    try stream_store.setOffset(trees, allocator, "orders", "consumer-a", 2);

    try std.testing.expectEqual(@as(u64, 3), try stream_store.getLastSequence(trees, allocator, "orders"));

    try stream_store.trim(trees, allocator, "orders", 2);

    var batch = try stream_store.read(trees, allocator, "orders", 0, 10);
    defer batch.deinit();
    try std.testing.expectEqual(@as(usize, 1), batch.records.len);
    try std.testing.expectEqual(@as(u64, 3), batch.records[0].sequence);
    try std.testing.expectEqualStrings("three", batch.records[0].payload.string_val);

    const offset = try stream_store.getOffset(trees, allocator, "orders", "consumer-a");
    try std.testing.expect(offset != null);
    try std.testing.expectEqual(@as(u64, 2), offset.?);
    try std.testing.expectEqual(@as(u64, 3), try stream_store.getLastSequence(trees, allocator, "orders"));
}

test "stream_store: validation rejects empty long and invalid utf8 names" {
    try stream_store.validateStreamNameForRead("orders");
    try stream_store.validateKind("message");
    try stream_store.validateConsumer("worker-a");

    try std.testing.expectError(stream_store.StreamError.InvalidName, stream_store.validateStreamNameForRead(""));
    try std.testing.expectError(stream_store.StreamError.InvalidKind, stream_store.validateKind(""));
    try std.testing.expectError(stream_store.StreamError.InvalidConsumer, stream_store.validateConsumer(""));

    var long_name = [_]u8{'a'} ** 256;
    try std.testing.expectError(
        stream_store.StreamError.InvalidName,
        stream_store.validateStreamNameForRead(long_name[0..]),
    );

    const invalid_utf8 = [_]u8{0xff};
    try std.testing.expectError(
        stream_store.StreamError.InvalidName,
        stream_store.validateStreamNameForRead(invalid_utf8[0..]),
    );
    try std.testing.expectError(
        stream_store.StreamError.InvalidKind,
        stream_store.validateKind(invalid_utf8[0..]),
    );
    try std.testing.expectError(
        stream_store.StreamError.InvalidConsumer,
        stream_store.validateConsumer(invalid_utf8[0..]),
    );
}
