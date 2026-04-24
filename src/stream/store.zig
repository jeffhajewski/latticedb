//! Durable named stream storage.
//!
//! Streams are internal system B+Trees:
//! - meta:    stream name -> last sequence
//! - events:  stream name + sequence -> kind + PropertyValue payload bytes
//! - offsets: stream name + consumer name -> committed sequence

const std = @import("std");
const lattice = @import("lattice");

const Allocator = std.mem.Allocator;
const PropertyValue = lattice.core.types.PropertyValue;
const BTree = lattice.storage.btree.BTree;
const BTreeError = lattice.storage.btree.BTreeError;
const wal_payload = lattice.transaction.wal_payload;
const stream_payload = lattice.stream.payload;

pub const changes_stream = "__lattice_changes";
pub const reserved_prefix = "__lattice_";
pub const default_kind = "message";
pub const max_name_len: usize = 255;

pub const StreamError = error{
    InvalidName,
    ReservedName,
    InvalidKind,
    InvalidConsumer,
    InvalidRecord,
    OutOfMemory,
    IoError,
    ValueTooLarge,
};

pub const TreeSet = struct {
    meta: *BTree,
    events: *BTree,
    offsets: *BTree,
};

pub const Record = struct {
    sequence: u64,
    kind: []u8,
    payload: PropertyValue,

    pub fn deinit(self: *Record, allocator: Allocator) void {
        allocator.free(self.kind);
        self.payload.deinit(allocator);
        self.* = undefined;
    }
};

pub const Batch = struct {
    allocator: Allocator,
    records: []Record,

    pub fn deinit(self: *Batch) void {
        for (self.records) |*record| {
            record.deinit(self.allocator);
        }
        self.allocator.free(self.records);
        self.* = undefined;
    }
};

pub const AppendWalPayload = struct {
    stream: []const u8,
    sequence: u64,
    kind: []const u8,
    payload_bytes: []const u8,
};

pub const OffsetWalPayload = struct {
    stream: []const u8,
    consumer: []const u8,
    sequence: u64,
};

pub const TrimWalPayload = struct {
    stream: []const u8,
    through_sequence: u64,
};

pub fn validateStreamNameForRead(name: []const u8) StreamError!void {
    try validateUtf8Name(name, StreamError.InvalidName);
}

pub fn validateUserStreamName(name: []const u8) StreamError!void {
    try validateUtf8Name(name, StreamError.InvalidName);
    if (std.mem.startsWith(u8, name, reserved_prefix)) return StreamError.ReservedName;
}

pub fn validateKind(kind: []const u8) StreamError!void {
    try validateUtf8Name(kind, StreamError.InvalidKind);
}

pub fn validateConsumer(consumer: []const u8) StreamError!void {
    try validateUtf8Name(consumer, StreamError.InvalidConsumer);
}

fn validateUtf8Name(value: []const u8, comptime err: StreamError) StreamError!void {
    if (value.len == 0 or value.len > max_name_len) return err;
    if (!std.unicode.utf8ValidateSlice(value)) return err;
}

fn mapBTreeError(err: BTreeError) StreamError {
    return switch (err) {
        BTreeError.OutOfMemory => StreamError.OutOfMemory,
        BTreeError.PageFull => StreamError.ValueTooLarge,
        else => StreamError.IoError,
    };
}

pub fn encodeNameKey(allocator: Allocator, name: []const u8) StreamError![]u8 {
    var key = allocator.alloc(u8, 1 + name.len) catch return StreamError.OutOfMemory;
    key[0] = @intCast(name.len);
    @memcpy(key[1..], name);
    return key;
}

pub fn encodeEventKey(allocator: Allocator, stream: []const u8, sequence: u64) StreamError![]u8 {
    var key = allocator.alloc(u8, 1 + stream.len + 8) catch return StreamError.OutOfMemory;
    key[0] = @intCast(stream.len);
    @memcpy(key[1..][0..stream.len], stream);
    std.mem.writeInt(u64, key[1 + stream.len ..][0..8], sequence, .big);
    return key;
}

pub fn encodeEventEndKey(allocator: Allocator, stream: []const u8) StreamError![]u8 {
    var key = allocator.alloc(u8, 1 + stream.len + 9) catch return StreamError.OutOfMemory;
    key[0] = @intCast(stream.len);
    @memcpy(key[1..][0..stream.len], stream);
    @memset(key[1 + stream.len ..], 0xff);
    return key;
}

pub fn decodeEventSequence(key: []const u8) StreamError!u64 {
    if (key.len < 9) return StreamError.InvalidRecord;
    const stream_len: usize = key[0];
    if (key.len != 1 + stream_len + 8) return StreamError.InvalidRecord;
    return std.mem.readInt(u64, key[1 + stream_len ..][0..8], .big);
}

pub fn encodeOffsetKey(allocator: Allocator, stream: []const u8, consumer: []const u8) StreamError![]u8 {
    var key = allocator.alloc(u8, 2 + stream.len + consumer.len) catch return StreamError.OutOfMemory;
    key[0] = @intCast(stream.len);
    @memcpy(key[1..][0..stream.len], stream);
    key[1 + stream.len] = @intCast(consumer.len);
    @memcpy(key[2 + stream.len ..], consumer);
    return key;
}

fn encodeU64Alloc(allocator: Allocator, value: u64) StreamError![]u8 {
    const bytes = allocator.alloc(u8, 8) catch return StreamError.OutOfMemory;
    std.mem.writeInt(u64, bytes[0..8], value, .little);
    return bytes;
}

fn readU64Value(bytes: []const u8) StreamError!u64 {
    if (bytes.len != 8) return StreamError.InvalidRecord;
    return std.mem.readInt(u64, bytes[0..8], .little);
}

fn upsert(tree: *BTree, key: []const u8, value: []const u8) StreamError!void {
    if (!tree.canFitLeafEntry(key, value.len)) return StreamError.ValueTooLarge;
    if (tree.contains(key) catch |err| return mapBTreeError(err)) {
        tree.delete(key) catch |err| switch (err) {
            BTreeError.KeyNotFound => {},
            else => return mapBTreeError(err),
        };
    }
    tree.insert(key, value) catch |err| return mapBTreeError(err);
}

pub fn getLastSequence(trees: TreeSet, allocator: Allocator, stream: []const u8) StreamError!u64 {
    try validateStreamNameForRead(stream);
    const key = try encodeNameKey(allocator, stream);
    defer allocator.free(key);

    const value = trees.meta.get(key) catch |err| return mapBTreeError(err);
    if (value) |bytes| {
        defer trees.meta.freeValue(bytes);
        return readU64Value(bytes);
    }
    return 0;
}

fn setLastSequence(trees: TreeSet, allocator: Allocator, stream: []const u8, sequence: u64) StreamError!void {
    const key = try encodeNameKey(allocator, stream);
    defer allocator.free(key);
    const value = try encodeU64Alloc(allocator, sequence);
    defer allocator.free(value);
    try upsert(trees.meta, key, value);
}

pub fn serializeRecordValueAlloc(
    allocator: Allocator,
    kind: []const u8,
    payload_value: PropertyValue,
) StreamError![]u8 {
    try validateKind(kind);

    const payload_bytes = stream_payload.encodeAlloc(allocator, payload_value) catch |err| switch (err) {
        error.OutOfMemory => return StreamError.OutOfMemory,
        else => return StreamError.InvalidRecord,
    };
    defer allocator.free(payload_bytes);

    if (payload_bytes.len > std.math.maxInt(u32)) return StreamError.ValueTooLarge;
    const total = 1 + kind.len + 4 + payload_bytes.len;
    const bytes = allocator.alloc(u8, total) catch return StreamError.OutOfMemory;
    errdefer allocator.free(bytes);

    bytes[0] = @intCast(kind.len);
    @memcpy(bytes[1..][0..kind.len], kind);
    std.mem.writeInt(u32, bytes[1 + kind.len ..][0..4], @intCast(payload_bytes.len), .little);
    @memcpy(bytes[1 + kind.len + 4 ..], payload_bytes);
    return bytes;
}

fn deserializeRecordValue(
    allocator: Allocator,
    sequence: u64,
    bytes: []const u8,
) StreamError!Record {
    if (bytes.len < 5) return StreamError.InvalidRecord;
    const kind_len: usize = bytes[0];
    if (kind_len == 0 or bytes.len < 1 + kind_len + 4) return StreamError.InvalidRecord;
    const payload_len = std.mem.readInt(u32, bytes[1 + kind_len ..][0..4], .little);
    if (bytes.len != 1 + kind_len + 4 + payload_len) return StreamError.InvalidRecord;

    const kind = allocator.dupe(u8, bytes[1..][0..kind_len]) catch return StreamError.OutOfMemory;
    errdefer allocator.free(kind);

    var value = stream_payload.decode(allocator, bytes[1 + kind_len + 4 ..]) catch |err| switch (err) {
        error.OutOfMemory => return StreamError.OutOfMemory,
        else => return StreamError.InvalidRecord,
    };
    errdefer value.deinit(allocator);

    return .{
        .sequence = sequence,
        .kind = kind,
        .payload = value,
    };
}

pub fn appendWithSequence(
    trees: TreeSet,
    allocator: Allocator,
    stream: []const u8,
    sequence: u64,
    kind: []const u8,
    payload_value: PropertyValue,
) StreamError!void {
    try validateStreamNameForRead(stream);
    if (sequence == 0) return StreamError.InvalidRecord;

    const key = try encodeEventKey(allocator, stream, sequence);
    defer allocator.free(key);
    const value = try serializeRecordValueAlloc(allocator, kind, payload_value);
    defer allocator.free(value);

    if (!trees.events.canFitLeafEntry(key, value.len)) return StreamError.ValueTooLarge;
    trees.events.insert(key, value) catch |err| switch (err) {
        BTreeError.DuplicateKey => {},
        else => return mapBTreeError(err),
    };

    const last = try getLastSequence(trees, allocator, stream);
    if (sequence > last) {
        try setLastSequence(trees, allocator, stream, sequence);
    }
}

pub fn read(
    trees: ?TreeSet,
    allocator: Allocator,
    stream: []const u8,
    after_sequence: u64,
    limit: usize,
) StreamError!Batch {
    try validateStreamNameForRead(stream);
    if (limit == 0) return StreamError.InvalidRecord;

    if (trees == null or after_sequence == std.math.maxInt(u64)) {
        return .{
            .allocator = allocator,
            .records = allocator.alloc(Record, 0) catch return StreamError.OutOfMemory,
        };
    }

    const t = trees.?;
    const start = try encodeEventKey(allocator, stream, after_sequence + 1);
    defer allocator.free(start);
    const end = try encodeEventEndKey(allocator, stream);
    defer allocator.free(end);

    var iter = t.events.range(start, end) catch |err| return mapBTreeError(err);
    defer iter.deinit();

    var records: std.ArrayListUnmanaged(Record) = .empty;
    errdefer {
        for (records.items) |*record| record.deinit(allocator);
        records.deinit(allocator);
    }

    while (records.items.len < limit) {
        const entry = iter.next() catch |err| return mapBTreeError(err);
        const e = entry orelse break;
        const sequence = try decodeEventSequence(e.key);
        const record = try deserializeRecordValue(allocator, sequence, e.value);
        records.append(allocator, record) catch {
            var owned = record;
            owned.deinit(allocator);
            return StreamError.OutOfMemory;
        };
    }

    return .{
        .allocator = allocator,
        .records = records.toOwnedSlice(allocator) catch return StreamError.OutOfMemory,
    };
}

pub fn getOffset(
    trees: ?TreeSet,
    allocator: Allocator,
    stream: []const u8,
    consumer: []const u8,
) StreamError!?u64 {
    try validateStreamNameForRead(stream);
    try validateConsumer(consumer);

    const t = trees orelse return null;
    const key = try encodeOffsetKey(allocator, stream, consumer);
    defer allocator.free(key);

    const value = t.offsets.get(key) catch |err| return mapBTreeError(err);
    if (value) |bytes| {
        defer t.offsets.freeValue(bytes);
        return try readU64Value(bytes);
    }
    return null;
}

pub fn setOffset(
    trees: TreeSet,
    allocator: Allocator,
    stream: []const u8,
    consumer: []const u8,
    sequence: u64,
) StreamError!void {
    try validateStreamNameForRead(stream);
    try validateConsumer(consumer);

    const key = try encodeOffsetKey(allocator, stream, consumer);
    defer allocator.free(key);
    const value = try encodeU64Alloc(allocator, sequence);
    defer allocator.free(value);
    try upsert(trees.offsets, key, value);
}

pub fn trim(
    trees: TreeSet,
    allocator: Allocator,
    stream: []const u8,
    through_sequence: u64,
) StreamError!void {
    try validateStreamNameForRead(stream);
    if (through_sequence == 0) return;

    const start = try encodeEventKey(allocator, stream, 1);
    defer allocator.free(start);
    const end = if (through_sequence == std.math.maxInt(u64))
        try encodeEventEndKey(allocator, stream)
    else
        try encodeEventKey(allocator, stream, through_sequence + 1);
    defer allocator.free(end);

    var iter = trees.events.range(start, end) catch |err| return mapBTreeError(err);
    errdefer iter.deinit();

    var keys: std.ArrayListUnmanaged([]u8) = .empty;
    defer {
        for (keys.items) |key| allocator.free(key);
        keys.deinit(allocator);
    }

    while (true) {
        const entry = iter.next() catch |err| return mapBTreeError(err);
        const e = entry orelse break;
        const key_copy = allocator.dupe(u8, e.key) catch return StreamError.OutOfMemory;
        keys.append(allocator, key_copy) catch {
            allocator.free(key_copy);
            return StreamError.OutOfMemory;
        };
    }

    iter.deinit();

    for (keys.items) |key| {
        trees.events.delete(key) catch |err| switch (err) {
            BTreeError.KeyNotFound => {},
            else => return mapBTreeError(err),
        };
    }
}

pub fn serializeAppendWalAlloc(
    allocator: Allocator,
    stream: []const u8,
    sequence: u64,
    kind: []const u8,
    payload_value: PropertyValue,
) StreamError![]u8 {
    const payload_bytes = stream_payload.encodeAlloc(allocator, payload_value) catch |err| switch (err) {
        error.OutOfMemory => return StreamError.OutOfMemory,
        else => return StreamError.InvalidRecord,
    };
    defer allocator.free(payload_bytes);
    if (payload_bytes.len > std.math.maxInt(u32)) return StreamError.ValueTooLarge;

    const total = 1 + 1 + stream.len + 8 + 1 + kind.len + 4 + payload_bytes.len;
    const out = allocator.alloc(u8, total) catch return StreamError.OutOfMemory;
    errdefer allocator.free(out);

    var offset: usize = 0;
    out[offset] = @intFromEnum(wal_payload.PayloadType.stream_append);
    offset += 1;
    out[offset] = @intCast(stream.len);
    offset += 1;
    @memcpy(out[offset..][0..stream.len], stream);
    offset += stream.len;
    std.mem.writeInt(u64, out[offset..][0..8], sequence, .little);
    offset += 8;
    out[offset] = @intCast(kind.len);
    offset += 1;
    @memcpy(out[offset..][0..kind.len], kind);
    offset += kind.len;
    std.mem.writeInt(u32, out[offset..][0..4], @intCast(payload_bytes.len), .little);
    offset += 4;
    @memcpy(out[offset..], payload_bytes);
    return out;
}

pub fn deserializeAppendWal(bytes: []const u8) StreamError!AppendWalPayload {
    if (bytes.len < 1 + 1 + 8 + 1 + 4) return StreamError.InvalidRecord;
    if (bytes[0] != @intFromEnum(wal_payload.PayloadType.stream_append)) return StreamError.InvalidRecord;
    var offset: usize = 1;
    const stream_len: usize = bytes[offset];
    offset += 1;
    if (stream_len == 0 or bytes.len < offset + stream_len + 8 + 1 + 4) return StreamError.InvalidRecord;
    const stream = bytes[offset..][0..stream_len];
    offset += stream_len;
    const sequence = std.mem.readInt(u64, bytes[offset..][0..8], .little);
    offset += 8;
    const kind_len: usize = bytes[offset];
    offset += 1;
    if (kind_len == 0 or bytes.len < offset + kind_len + 4) return StreamError.InvalidRecord;
    const kind = bytes[offset..][0..kind_len];
    offset += kind_len;
    const payload_len = std.mem.readInt(u32, bytes[offset..][0..4], .little);
    offset += 4;
    if (bytes.len != offset + payload_len) return StreamError.InvalidRecord;
    return .{
        .stream = stream,
        .sequence = sequence,
        .kind = kind,
        .payload_bytes = bytes[offset..],
    };
}

pub fn serializeOffsetWalAlloc(
    allocator: Allocator,
    stream: []const u8,
    consumer: []const u8,
    sequence: u64,
) StreamError![]u8 {
    const total = 1 + 1 + stream.len + 1 + consumer.len + 8;
    const out = allocator.alloc(u8, total) catch return StreamError.OutOfMemory;
    errdefer allocator.free(out);
    var offset: usize = 0;
    out[offset] = @intFromEnum(wal_payload.PayloadType.stream_offset_set);
    offset += 1;
    out[offset] = @intCast(stream.len);
    offset += 1;
    @memcpy(out[offset..][0..stream.len], stream);
    offset += stream.len;
    out[offset] = @intCast(consumer.len);
    offset += 1;
    @memcpy(out[offset..][0..consumer.len], consumer);
    offset += consumer.len;
    std.mem.writeInt(u64, out[offset..][0..8], sequence, .little);
    return out;
}

pub fn deserializeOffsetWal(bytes: []const u8) StreamError!OffsetWalPayload {
    if (bytes.len < 1 + 1 + 1 + 8) return StreamError.InvalidRecord;
    if (bytes[0] != @intFromEnum(wal_payload.PayloadType.stream_offset_set)) return StreamError.InvalidRecord;
    var offset: usize = 1;
    const stream_len: usize = bytes[offset];
    offset += 1;
    if (stream_len == 0 or bytes.len < offset + stream_len + 1 + 8) return StreamError.InvalidRecord;
    const stream = bytes[offset..][0..stream_len];
    offset += stream_len;
    const consumer_len: usize = bytes[offset];
    offset += 1;
    if (consumer_len == 0 or bytes.len != offset + consumer_len + 8) return StreamError.InvalidRecord;
    const consumer = bytes[offset..][0..consumer_len];
    offset += consumer_len;
    const sequence = std.mem.readInt(u64, bytes[offset..][0..8], .little);
    return .{ .stream = stream, .consumer = consumer, .sequence = sequence };
}

pub fn serializeTrimWalAlloc(
    allocator: Allocator,
    stream: []const u8,
    through_sequence: u64,
) StreamError![]u8 {
    const total = 1 + 1 + stream.len + 8;
    const out = allocator.alloc(u8, total) catch return StreamError.OutOfMemory;
    errdefer allocator.free(out);
    out[0] = @intFromEnum(wal_payload.PayloadType.stream_trim);
    out[1] = @intCast(stream.len);
    @memcpy(out[2..][0..stream.len], stream);
    std.mem.writeInt(u64, out[2 + stream.len ..][0..8], through_sequence, .little);
    return out;
}

pub fn deserializeTrimWal(bytes: []const u8) StreamError!TrimWalPayload {
    if (bytes.len < 1 + 1 + 8) return StreamError.InvalidRecord;
    if (bytes[0] != @intFromEnum(wal_payload.PayloadType.stream_trim)) return StreamError.InvalidRecord;
    const stream_len: usize = bytes[1];
    if (stream_len == 0 or bytes.len != 2 + stream_len + 8) return StreamError.InvalidRecord;
    const stream = bytes[2..][0..stream_len];
    const through = std.mem.readInt(u64, bytes[2 + stream_len ..][0..8], .little);
    return .{ .stream = stream, .through_sequence = through };
}

test "stream event key orders by sequence within stream" {
    const allocator = std.testing.allocator;
    const a = try encodeEventKey(allocator, "events", 1);
    defer allocator.free(a);
    const b = try encodeEventKey(allocator, "events", 2);
    defer allocator.free(b);
    try std.testing.expect(std.mem.order(u8, a, b) == .lt);
    try std.testing.expectEqual(@as(u64, 1), try decodeEventSequence(a));
}

test "stream name validation rejects reserved user names" {
    try validateUserStreamName("orders");
    try std.testing.expectError(StreamError.ReservedName, validateUserStreamName("__lattice_changes"));
    try std.testing.expectError(StreamError.InvalidName, validateUserStreamName(""));
}
