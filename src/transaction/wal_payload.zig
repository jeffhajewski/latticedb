//! WAL Payload Serialization for Database Operations
//!
//! Each operation type has a defined payload format for redo/undo.
//! Payloads are serialized to be stored in WAL records and deserialized
//! during recovery or abort.

const std = @import("std");

/// Payload type identifiers for WAL records
pub const PayloadType = enum(u8) {
    node_insert = 0x01,
    node_delete = 0x02,
    node_update = 0x03, // Property change
    edge_insert = 0x04,
    edge_delete = 0x05,
    label_add = 0x06,
    label_remove = 0x07,
};

/// Errors that can occur during payload serialization/deserialization
pub const PayloadError = error{
    BufferTooSmall,
    InvalidPayload,
    UnexpectedPayloadType,
};

// ============================================================================
// Node Insert Payload
// ============================================================================

/// Serialize a node insert operation
/// Format: type(u8) + node_id(u64) + label_count(u16) + labels(u16[])
pub fn serializeNodeInsert(
    buf: []u8,
    node_id: u64,
    label_ids: []const u16,
) PayloadError![]u8 {
    const required_size = 1 + 8 + 2 + (label_ids.len * 2);
    if (buf.len < required_size) return PayloadError.BufferTooSmall;

    var stream = std.io.fixedBufferStream(buf);
    const writer = stream.writer();

    writer.writeByte(@intFromEnum(PayloadType.node_insert)) catch unreachable;
    writer.writeInt(u64, node_id, .little) catch unreachable;
    writer.writeInt(u16, @intCast(label_ids.len), .little) catch unreachable;
    for (label_ids) |lid| {
        writer.writeInt(u16, lid, .little) catch unreachable;
    }

    return buf[0..stream.pos];
}

/// Deserialized node insert payload
pub const NodeInsertPayload = struct {
    node_id: u64,
    label_count: u16,
    label_bytes: []const u8,

    /// Get a label ID at the given index
    pub fn getLabelId(self: NodeInsertPayload, index: usize) u16 {
        const offset = index * 2;
        return std.mem.readInt(u16, self.label_bytes[offset..][0..2], .little);
    }
};

/// Deserialize a node insert payload
pub fn deserializeNodeInsert(payload: []const u8) PayloadError!NodeInsertPayload {
    if (payload.len < 11) return PayloadError.InvalidPayload;
    if (payload[0] != @intFromEnum(PayloadType.node_insert)) {
        return PayloadError.UnexpectedPayloadType;
    }

    const node_id = std.mem.readInt(u64, payload[1..9], .little);
    const label_count = std.mem.readInt(u16, payload[9..11], .little);

    const expected_size = 11 + (label_count * 2);
    if (payload.len < expected_size) return PayloadError.InvalidPayload;

    // Return a view into the payload for label bytes
    const label_bytes = payload[11..][0 .. label_count * 2];

    return NodeInsertPayload{
        .node_id = node_id,
        .label_count = label_count,
        .label_bytes = label_bytes,
    };
}

// ============================================================================
// Node Delete Payload
// ============================================================================

/// Serialize a node delete operation (includes full node data for undo)
/// Format: type(u8) + node_id(u64) + label_count(u16) + labels(u16[]) + props_len(u32) + props(u8[])
pub fn serializeNodeDelete(
    buf: []u8,
    node_id: u64,
    label_ids: []const u16,
    properties: []const u8,
) PayloadError![]u8 {
    const required_size = 1 + 8 + 2 + (label_ids.len * 2) + 4 + properties.len;
    if (buf.len < required_size) return PayloadError.BufferTooSmall;

    var stream = std.io.fixedBufferStream(buf);
    const writer = stream.writer();

    writer.writeByte(@intFromEnum(PayloadType.node_delete)) catch unreachable;
    writer.writeInt(u64, node_id, .little) catch unreachable;
    writer.writeInt(u16, @intCast(label_ids.len), .little) catch unreachable;
    for (label_ids) |lid| {
        writer.writeInt(u16, lid, .little) catch unreachable;
    }
    writer.writeInt(u32, @intCast(properties.len), .little) catch unreachable;
    if (properties.len > 0) {
        writer.writeAll(properties) catch unreachable;
    }

    return buf[0..stream.pos];
}

/// Deserialized node delete payload
pub const NodeDeletePayload = struct {
    node_id: u64,
    label_count: u16,
    label_bytes: []const u8,
    properties: []const u8,

    /// Get a label ID at the given index
    pub fn getLabelId(self: NodeDeletePayload, index: usize) u16 {
        const offset = index * 2;
        return std.mem.readInt(u16, self.label_bytes[offset..][0..2], .little);
    }
};

/// Deserialize a node delete payload
pub fn deserializeNodeDelete(payload: []const u8) PayloadError!NodeDeletePayload {
    if (payload.len < 15) return PayloadError.InvalidPayload;
    if (payload[0] != @intFromEnum(PayloadType.node_delete)) {
        return PayloadError.UnexpectedPayloadType;
    }

    const node_id = std.mem.readInt(u64, payload[1..9], .little);
    const label_count = std.mem.readInt(u16, payload[9..11], .little);

    var offset: usize = 11;
    const labels_size = label_count * 2;
    if (payload.len < offset + labels_size + 4) return PayloadError.InvalidPayload;

    const label_bytes = payload[offset..][0..labels_size];
    offset += labels_size;

    const props_len = std.mem.readInt(u32, payload[offset..][0..4], .little);
    offset += 4;

    if (payload.len < offset + props_len) return PayloadError.InvalidPayload;
    const properties = payload[offset..][0..props_len];

    return NodeDeletePayload{
        .node_id = node_id,
        .label_count = label_count,
        .label_bytes = label_bytes,
        .properties = properties,
    };
}

// ============================================================================
// Edge Insert Payload
// ============================================================================

/// Serialize an edge insert operation
/// Format: type(u8) + source(u64) + target(u64) + type_id(u16)
pub fn serializeEdgeInsert(
    buf: []u8,
    source: u64,
    target: u64,
    type_id: u16,
) PayloadError![]u8 {
    const required_size = 1 + 8 + 8 + 2;
    if (buf.len < required_size) return PayloadError.BufferTooSmall;

    var stream = std.io.fixedBufferStream(buf);
    const writer = stream.writer();

    writer.writeByte(@intFromEnum(PayloadType.edge_insert)) catch unreachable;
    writer.writeInt(u64, source, .little) catch unreachable;
    writer.writeInt(u64, target, .little) catch unreachable;
    writer.writeInt(u16, type_id, .little) catch unreachable;

    return buf[0..stream.pos];
}

/// Deserialized edge insert payload
pub const EdgeInsertPayload = struct {
    source: u64,
    target: u64,
    type_id: u16,
};

/// Deserialize an edge insert payload
pub fn deserializeEdgeInsert(payload: []const u8) PayloadError!EdgeInsertPayload {
    if (payload.len < 19) return PayloadError.InvalidPayload;
    if (payload[0] != @intFromEnum(PayloadType.edge_insert)) {
        return PayloadError.UnexpectedPayloadType;
    }

    return EdgeInsertPayload{
        .source = std.mem.readInt(u64, payload[1..9], .little),
        .target = std.mem.readInt(u64, payload[9..17], .little),
        .type_id = std.mem.readInt(u16, payload[17..19], .little),
    };
}

// ============================================================================
// Edge Delete Payload
// ============================================================================

/// Serialize an edge delete operation (includes edge data for undo)
/// Format: type(u8) + source(u64) + target(u64) + type_id(u16) + props_len(u32) + props(u8[])
pub fn serializeEdgeDelete(
    buf: []u8,
    source: u64,
    target: u64,
    type_id: u16,
    properties: []const u8,
) PayloadError![]u8 {
    const required_size = 1 + 8 + 8 + 2 + 4 + properties.len;
    if (buf.len < required_size) return PayloadError.BufferTooSmall;

    var stream = std.io.fixedBufferStream(buf);
    const writer = stream.writer();

    writer.writeByte(@intFromEnum(PayloadType.edge_delete)) catch unreachable;
    writer.writeInt(u64, source, .little) catch unreachable;
    writer.writeInt(u64, target, .little) catch unreachable;
    writer.writeInt(u16, type_id, .little) catch unreachable;
    writer.writeInt(u32, @intCast(properties.len), .little) catch unreachable;
    if (properties.len > 0) {
        writer.writeAll(properties) catch unreachable;
    }

    return buf[0..stream.pos];
}

/// Deserialized edge delete payload
pub const EdgeDeletePayload = struct {
    source: u64,
    target: u64,
    type_id: u16,
    properties: []const u8,
};

/// Deserialize an edge delete payload
pub fn deserializeEdgeDelete(payload: []const u8) PayloadError!EdgeDeletePayload {
    if (payload.len < 23) return PayloadError.InvalidPayload;
    if (payload[0] != @intFromEnum(PayloadType.edge_delete)) {
        return PayloadError.UnexpectedPayloadType;
    }

    const source = std.mem.readInt(u64, payload[1..9], .little);
    const target = std.mem.readInt(u64, payload[9..17], .little);
    const type_id = std.mem.readInt(u16, payload[17..19], .little);
    const props_len = std.mem.readInt(u32, payload[19..23], .little);

    if (payload.len < 23 + props_len) return PayloadError.InvalidPayload;
    const properties = payload[23..][0..props_len];

    return EdgeDeletePayload{
        .source = source,
        .target = target,
        .type_id = type_id,
        .properties = properties,
    };
}

// ============================================================================
// Property Update Payload
// ============================================================================

/// Serialize a property update operation
/// Format: type(u8) + node_id(u64) + key_id(u16) + old_len(u32) + old(u8[]) + new_len(u32) + new(u8[])
pub fn serializePropertyUpdate(
    buf: []u8,
    node_id: u64,
    key_id: u16,
    old_value: ?[]const u8,
    new_value: []const u8,
) PayloadError![]u8 {
    const old_len = if (old_value) |ov| ov.len else 0;
    const required_size = 1 + 8 + 2 + 4 + old_len + 4 + new_value.len;
    if (buf.len < required_size) return PayloadError.BufferTooSmall;

    var stream = std.io.fixedBufferStream(buf);
    const writer = stream.writer();

    writer.writeByte(@intFromEnum(PayloadType.node_update)) catch unreachable;
    writer.writeInt(u64, node_id, .little) catch unreachable;
    writer.writeInt(u16, key_id, .little) catch unreachable;

    // Old value (for undo) - length 0 means was null/absent
    writer.writeInt(u32, @intCast(old_len), .little) catch unreachable;
    if (old_value) |ov| {
        writer.writeAll(ov) catch unreachable;
    }

    // New value
    writer.writeInt(u32, @intCast(new_value.len), .little) catch unreachable;
    writer.writeAll(new_value) catch unreachable;

    return buf[0..stream.pos];
}

/// Deserialized property update payload
pub const PropertyUpdatePayload = struct {
    node_id: u64,
    key_id: u16,
    old_value: ?[]const u8, // null if property was absent before
    new_value: []const u8,
};

/// Deserialize a property update payload
pub fn deserializePropertyUpdate(payload: []const u8) PayloadError!PropertyUpdatePayload {
    if (payload.len < 15) return PayloadError.InvalidPayload;
    if (payload[0] != @intFromEnum(PayloadType.node_update)) {
        return PayloadError.UnexpectedPayloadType;
    }

    const node_id = std.mem.readInt(u64, payload[1..9], .little);
    const key_id = std.mem.readInt(u16, payload[9..11], .little);

    var offset: usize = 11;

    // Old value
    const old_len = std.mem.readInt(u32, payload[offset..][0..4], .little);
    offset += 4;
    if (payload.len < offset + old_len + 4) return PayloadError.InvalidPayload;

    const old_value: ?[]const u8 = if (old_len > 0) payload[offset..][0..old_len] else null;
    offset += old_len;

    // New value
    const new_len = std.mem.readInt(u32, payload[offset..][0..4], .little);
    offset += 4;
    if (payload.len < offset + new_len) return PayloadError.InvalidPayload;

    const new_value = payload[offset..][0..new_len];

    return PropertyUpdatePayload{
        .node_id = node_id,
        .key_id = key_id,
        .old_value = old_value,
        .new_value = new_value,
    };
}

// ============================================================================
// Tests
// ============================================================================

test "node_insert: serialize and deserialize round-trip" {
    var buf: [256]u8 = undefined;
    const label_ids = [_]u16{ 1, 2, 3 };

    const serialized = try serializeNodeInsert(&buf, 42, &label_ids);
    const result = try deserializeNodeInsert(serialized);

    try std.testing.expectEqual(@as(u64, 42), result.node_id);
    try std.testing.expectEqual(@as(u16, 3), result.label_count);
    try std.testing.expectEqual(@as(u16, 1), result.getLabelId(0));
    try std.testing.expectEqual(@as(u16, 2), result.getLabelId(1));
    try std.testing.expectEqual(@as(u16, 3), result.getLabelId(2));
}

test "node_insert: empty labels" {
    var buf: [256]u8 = undefined;
    const label_ids = [_]u16{};

    const serialized = try serializeNodeInsert(&buf, 100, &label_ids);
    const result = try deserializeNodeInsert(serialized);

    try std.testing.expectEqual(@as(u64, 100), result.node_id);
    try std.testing.expectEqual(@as(u16, 0), result.label_count);
}

test "node_insert: buffer too small" {
    var buf: [5]u8 = undefined;
    const label_ids = [_]u16{ 1, 2 };

    const result = serializeNodeInsert(&buf, 42, &label_ids);
    try std.testing.expectError(PayloadError.BufferTooSmall, result);
}

test "node_delete: serialize and deserialize with properties" {
    var buf: [256]u8 = undefined;
    const label_ids = [_]u16{ 10, 20 };
    const props = "test_properties";

    const serialized = try serializeNodeDelete(&buf, 99, &label_ids, props);
    const result = try deserializeNodeDelete(serialized);

    try std.testing.expectEqual(@as(u64, 99), result.node_id);
    try std.testing.expectEqual(@as(u16, 2), result.label_count);
    try std.testing.expectEqual(@as(u16, 10), result.getLabelId(0));
    try std.testing.expectEqual(@as(u16, 20), result.getLabelId(1));
    try std.testing.expectEqualStrings(props, result.properties);
}

test "node_delete: empty properties" {
    var buf: [256]u8 = undefined;
    const label_ids = [_]u16{5};

    const serialized = try serializeNodeDelete(&buf, 50, &label_ids, "");
    const result = try deserializeNodeDelete(serialized);

    try std.testing.expectEqual(@as(u64, 50), result.node_id);
    try std.testing.expectEqual(@as(usize, 0), result.properties.len);
}

test "edge_insert: serialize and deserialize" {
    var buf: [256]u8 = undefined;

    const serialized = try serializeEdgeInsert(&buf, 1, 2, 100);
    const result = try deserializeEdgeInsert(serialized);

    try std.testing.expectEqual(@as(u64, 1), result.source);
    try std.testing.expectEqual(@as(u64, 2), result.target);
    try std.testing.expectEqual(@as(u16, 100), result.type_id);
}

test "edge_delete: serialize and deserialize with properties" {
    var buf: [256]u8 = undefined;
    const props = "edge_props";

    const serialized = try serializeEdgeDelete(&buf, 10, 20, 5, props);
    const result = try deserializeEdgeDelete(serialized);

    try std.testing.expectEqual(@as(u64, 10), result.source);
    try std.testing.expectEqual(@as(u64, 20), result.target);
    try std.testing.expectEqual(@as(u16, 5), result.type_id);
    try std.testing.expectEqualStrings(props, result.properties);
}

test "property_update: serialize and deserialize with old value" {
    var buf: [256]u8 = undefined;
    const old_val = "old";
    const new_val = "new_value";

    const serialized = try serializePropertyUpdate(&buf, 42, 7, old_val, new_val);
    const result = try deserializePropertyUpdate(serialized);

    try std.testing.expectEqual(@as(u64, 42), result.node_id);
    try std.testing.expectEqual(@as(u16, 7), result.key_id);
    try std.testing.expectEqualStrings(old_val, result.old_value.?);
    try std.testing.expectEqualStrings(new_val, result.new_value);
}

test "property_update: serialize and deserialize without old value" {
    var buf: [256]u8 = undefined;
    const new_val = "created";

    const serialized = try serializePropertyUpdate(&buf, 100, 3, null, new_val);
    const result = try deserializePropertyUpdate(serialized);

    try std.testing.expectEqual(@as(u64, 100), result.node_id);
    try std.testing.expectEqual(@as(u16, 3), result.key_id);
    try std.testing.expect(result.old_value == null);
    try std.testing.expectEqualStrings(new_val, result.new_value);
}

test "wrong payload type returns error" {
    var buf: [256]u8 = undefined;

    // Serialize as edge_insert (19 bytes)
    const serialized = try serializeEdgeInsert(&buf, 1, 2, 3);

    // Try to deserialize as node_insert (requires different type byte)
    try std.testing.expectError(
        PayloadError.UnexpectedPayloadType,
        deserializeNodeInsert(serialized),
    );
}

test "truncated payload returns error" {
    const truncated = [_]u8{ @intFromEnum(PayloadType.node_insert), 0, 0, 0 };
    try std.testing.expectError(
        PayloadError.InvalidPayload,
        deserializeNodeInsert(&truncated),
    );
}
