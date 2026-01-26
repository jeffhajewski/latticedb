//! WAL Payload Serialization for Database Operations
//!
//! Each operation type has a defined payload format for redo/undo.
//! Payloads are serialized to be stored in WAL records and deserialized
//! during recovery or abort.

const std = @import("std");
const Allocator = std.mem.Allocator;
const types = @import("../core/types.zig");
const PropertyValue = types.PropertyValue;
const symbols = @import("../graph/symbols.zig");
const SymbolId = symbols.SymbolId;

/// Property struct for serialization (matches node.zig)
pub const Property = struct {
    key_id: SymbolId,
    value: PropertyValue,
};

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
/// Format: type(u8) + node_id(u64) + label_count(u16) + [label_len(u16) + label_str(u8[])]...
pub fn serializeNodeInsert(
    buf: []u8,
    node_id: u64,
    labels: []const []const u8,
) PayloadError![]u8 {
    // Calculate required size: header + sum of (len + string) for each label
    var required_size: usize = 1 + 8 + 2; // type + node_id + label_count
    for (labels) |label| {
        required_size += 2 + label.len; // len(u16) + string bytes
    }
    if (buf.len < required_size) return PayloadError.BufferTooSmall;

    var stream = std.io.fixedBufferStream(buf);
    const writer = stream.writer();

    writer.writeByte(@intFromEnum(PayloadType.node_insert)) catch return PayloadError.BufferTooSmall;
    writer.writeInt(u64, node_id, .little) catch return PayloadError.BufferTooSmall;
    writer.writeInt(u16, @intCast(labels.len), .little) catch return PayloadError.BufferTooSmall;
    for (labels) |label| {
        writer.writeInt(u16, @intCast(label.len), .little) catch return PayloadError.BufferTooSmall;
        writer.writeAll(label) catch return PayloadError.BufferTooSmall;
    }

    return buf[0..stream.pos];
}

/// Deserialized node insert payload
pub const NodeInsertPayload = struct {
    node_id: u64,
    label_count: u16,
    /// Raw bytes containing length-prefixed label strings
    label_data: []const u8,

    /// Iterator for label strings
    pub const LabelIterator = struct {
        data: []const u8,
        offset: usize,
        remaining: u16,

        pub fn next(self: *LabelIterator) ?[]const u8 {
            if (self.remaining == 0) return null;
            if (self.offset + 2 > self.data.len) return null;

            const len = std.mem.readInt(u16, self.data[self.offset..][0..2], .little);
            self.offset += 2;

            if (self.offset + len > self.data.len) return null;
            const label = self.data[self.offset..][0..len];
            self.offset += len;
            self.remaining -= 1;
            return label;
        }
    };

    /// Get an iterator over label strings
    pub fn labelIterator(self: NodeInsertPayload) LabelIterator {
        return LabelIterator{
            .data = self.label_data,
            .offset = 0,
            .remaining = self.label_count,
        };
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

    // Remaining bytes are label data (length-prefixed strings)
    const label_data = payload[11..];

    return NodeInsertPayload{
        .node_id = node_id,
        .label_count = label_count,
        .label_data = label_data,
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

    writer.writeByte(@intFromEnum(PayloadType.node_delete)) catch return PayloadError.BufferTooSmall;
    writer.writeInt(u64, node_id, .little) catch return PayloadError.BufferTooSmall;
    writer.writeInt(u16, @intCast(label_ids.len), .little) catch return PayloadError.BufferTooSmall;
    for (label_ids) |lid| {
        writer.writeInt(u16, lid, .little) catch return PayloadError.BufferTooSmall;
    }
    writer.writeInt(u32, @intCast(properties.len), .little) catch return PayloadError.BufferTooSmall;
    if (properties.len > 0) {
        writer.writeAll(properties) catch return PayloadError.BufferTooSmall;
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
/// Format: type(u8) + source(u64) + target(u64) + type_len(u16) + type_str(u8[])
pub fn serializeEdgeInsert(
    buf: []u8,
    source: u64,
    target: u64,
    edge_type: []const u8,
) PayloadError![]u8 {
    const required_size = 1 + 8 + 8 + 2 + edge_type.len;
    if (buf.len < required_size) return PayloadError.BufferTooSmall;

    var stream = std.io.fixedBufferStream(buf);
    const writer = stream.writer();

    writer.writeByte(@intFromEnum(PayloadType.edge_insert)) catch return PayloadError.BufferTooSmall;
    writer.writeInt(u64, source, .little) catch return PayloadError.BufferTooSmall;
    writer.writeInt(u64, target, .little) catch return PayloadError.BufferTooSmall;
    writer.writeInt(u16, @intCast(edge_type.len), .little) catch return PayloadError.BufferTooSmall;
    writer.writeAll(edge_type) catch return PayloadError.BufferTooSmall;

    return buf[0..stream.pos];
}

/// Deserialized edge insert payload
pub const EdgeInsertPayload = struct {
    source: u64,
    target: u64,
    edge_type: []const u8,
};

/// Deserialize an edge insert payload
pub fn deserializeEdgeInsert(payload: []const u8) PayloadError!EdgeInsertPayload {
    if (payload.len < 19) return PayloadError.InvalidPayload;
    if (payload[0] != @intFromEnum(PayloadType.edge_insert)) {
        return PayloadError.UnexpectedPayloadType;
    }

    const source = std.mem.readInt(u64, payload[1..9], .little);
    const target = std.mem.readInt(u64, payload[9..17], .little);
    const type_len = std.mem.readInt(u16, payload[17..19], .little);

    if (payload.len < 19 + type_len) return PayloadError.InvalidPayload;
    const edge_type = payload[19..][0..type_len];

    return EdgeInsertPayload{
        .source = source,
        .target = target,
        .edge_type = edge_type,
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

    writer.writeByte(@intFromEnum(PayloadType.edge_delete)) catch return PayloadError.BufferTooSmall;
    writer.writeInt(u64, source, .little) catch return PayloadError.BufferTooSmall;
    writer.writeInt(u64, target, .little) catch return PayloadError.BufferTooSmall;
    writer.writeInt(u16, type_id, .little) catch return PayloadError.BufferTooSmall;
    writer.writeInt(u32, @intCast(properties.len), .little) catch return PayloadError.BufferTooSmall;
    if (properties.len > 0) {
        writer.writeAll(properties) catch return PayloadError.BufferTooSmall;
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
/// Format: type(u8) + node_id(u64) + key_len(u16) + key(u8[]) + old_len(u32) + old(u8[]) + new_len(u32) + new(u8[])
pub fn serializePropertyUpdate(
    buf: []u8,
    node_id: u64,
    key: []const u8,
    old_value: ?[]const u8,
    new_value: []const u8,
) PayloadError![]u8 {
    const old_len = if (old_value) |ov| ov.len else 0;
    const required_size = 1 + 8 + 2 + key.len + 4 + old_len + 4 + new_value.len;
    if (buf.len < required_size) return PayloadError.BufferTooSmall;

    var stream = std.io.fixedBufferStream(buf);
    const writer = stream.writer();

    writer.writeByte(@intFromEnum(PayloadType.node_update)) catch return PayloadError.BufferTooSmall;
    writer.writeInt(u64, node_id, .little) catch return PayloadError.BufferTooSmall;
    writer.writeInt(u16, @intCast(key.len), .little) catch return PayloadError.BufferTooSmall;
    writer.writeAll(key) catch return PayloadError.BufferTooSmall;

    // Old value (for undo) - length 0 means was null/absent
    writer.writeInt(u32, @intCast(old_len), .little) catch return PayloadError.BufferTooSmall;
    if (old_value) |ov| {
        writer.writeAll(ov) catch return PayloadError.BufferTooSmall;
    }

    // New value
    writer.writeInt(u32, @intCast(new_value.len), .little) catch return PayloadError.BufferTooSmall;
    writer.writeAll(new_value) catch return PayloadError.BufferTooSmall;

    return buf[0..stream.pos];
}

/// Deserialized property update payload
pub const PropertyUpdatePayload = struct {
    node_id: u64,
    key: []const u8,
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
    const key_len = std.mem.readInt(u16, payload[9..11], .little);

    var offset: usize = 11;
    if (payload.len < offset + key_len + 8) return PayloadError.InvalidPayload;

    const key = payload[offset..][0..key_len];
    offset += key_len;

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
        .key = key,
        .old_value = old_value,
        .new_value = new_value,
    };
}

// ============================================================================
// Property Serialization for Undo
// ============================================================================

/// Serialize properties to bytes for undo storage.
/// Format: num_props(u16) + [key_id(u16) + value]...
/// Uses same value format as node.zig serializeValue.
pub fn serializeProperties(allocator: Allocator, properties: []const Property) ![]u8 {
    // Calculate required size
    var size: usize = 2; // num_properties: u16
    for (properties) |prop| {
        size += 2; // key_id: u16
        size += propertyValueSize(prop.value);
    }

    const buf = try allocator.alloc(u8, size);
    errdefer allocator.free(buf);

    var stream = std.io.fixedBufferStream(buf);
    const writer = stream.writer();

    writer.writeInt(u16, @intCast(properties.len), .little) catch return error.BufferTooSmall;
    for (properties) |prop| {
        writer.writeInt(u16, prop.key_id, .little) catch return error.BufferTooSmall;
        serializePropertyValue(writer, prop.value) catch return error.BufferTooSmall;
    }

    return buf;
}

/// Deserialize properties from bytes.
/// Caller owns returned slice and must free property values.
pub fn deserializeProperties(allocator: Allocator, data: []const u8) ![]Property {
    if (data.len < 2) return error.InvalidPayload;

    var stream = std.io.fixedBufferStream(data);
    const reader = stream.reader();

    const num_props = try reader.readInt(u16, .little);
    if (num_props == 0) return &[_]Property{};

    const properties = try allocator.alloc(Property, num_props);
    var initialized_count: usize = 0;
    errdefer {
        // Only deinit properties that were successfully initialized
        for (properties[0..initialized_count]) |*prop| {
            var val = prop.value;
            val.deinit(allocator);
        }
        allocator.free(properties);
    }

    for (properties) |*prop| {
        prop.key_id = try reader.readInt(u16, .little);
        prop.value = try deserializePropertyValue(allocator, reader);
        initialized_count += 1;
    }

    return properties;
}

/// Serialize a single PropertyValue to a fixed buffer.
/// Returns number of bytes written, or error if buffer too small.
pub fn serializePropertyValueToBuf(buf: []u8, value: PropertyValue) !usize {
    var stream = std.io.fixedBufferStream(buf);
    try serializePropertyValue(stream.writer(), value);
    return stream.pos;
}

/// Deserialize a single PropertyValue from bytes.
pub fn deserializePropertyValueFromBytes(allocator: Allocator, data: []const u8) !PropertyValue {
    var stream = std.io.fixedBufferStream(data);
    return deserializePropertyValue(allocator, stream.reader());
}

/// Calculate size needed for a PropertyValue.
fn propertyValueSize(value: PropertyValue) usize {
    return switch (value) {
        .null_val => 1,
        .bool_val => 2,
        .int_val => 9,
        .float_val => 9,
        .string_val => |s| 5 + s.len,
        .bytes_val => |b| 5 + b.len,
        .vector_val => |v| 5 + v.len * 4,
        .list_val => |l| blk: {
            var s: usize = 5;
            for (l) |item| s += propertyValueSize(item);
            break :blk s;
        },
        .map_val => |m| blk: {
            var s: usize = 5;
            for (m) |entry| {
                s += 4 + entry.key.len + propertyValueSize(entry.value);
            }
            break :blk s;
        },
    };
}

/// Serialize a property value to a writer.
/// Wire format matches node.zig serializeValue.
fn serializePropertyValue(writer: anytype, value: PropertyValue) !void {
    switch (value) {
        .null_val => try writer.writeByte(0),
        .bool_val => |b| {
            try writer.writeByte(1);
            try writer.writeByte(if (b) 1 else 0);
        },
        .int_val => |i| {
            try writer.writeByte(2);
            try writer.writeInt(i64, i, .little);
        },
        .float_val => |f| {
            try writer.writeByte(3);
            try writer.writeInt(u64, @bitCast(f), .little);
        },
        .string_val => |s| {
            try writer.writeByte(4);
            try writer.writeInt(u32, @intCast(s.len), .little);
            try writer.writeAll(s);
        },
        .bytes_val => |b| {
            try writer.writeByte(5);
            try writer.writeInt(u32, @intCast(b.len), .little);
            try writer.writeAll(b);
        },
        .vector_val => |v| {
            try writer.writeByte(6);
            try writer.writeInt(u32, @intCast(v.len), .little);
            for (v) |f| {
                try writer.writeInt(u32, @bitCast(f), .little);
            }
        },
        .list_val => |list| {
            try writer.writeByte(7);
            try writer.writeInt(u32, @intCast(list.len), .little);
            for (list) |item| {
                try serializePropertyValue(writer, item);
            }
        },
        .map_val => |map| {
            try writer.writeByte(8);
            try writer.writeInt(u32, @intCast(map.len), .little);
            for (map) |entry| {
                try writer.writeInt(u32, @intCast(entry.key.len), .little);
                try writer.writeAll(entry.key);
                try serializePropertyValue(writer, entry.value);
            }
        },
    }
}

/// Deserialize a property value from a reader.
fn deserializePropertyValue(allocator: Allocator, reader: anytype) !PropertyValue {
    const type_tag = try reader.readByte();

    return switch (type_tag) {
        0 => PropertyValue{ .null_val = {} },
        1 => PropertyValue{ .bool_val = (try reader.readByte()) != 0 },
        2 => PropertyValue{ .int_val = try reader.readInt(i64, .little) },
        3 => PropertyValue{ .float_val = @bitCast(try reader.readInt(u64, .little)) },
        4 => blk: {
            const len = try reader.readInt(u32, .little);
            const str = try allocator.alloc(u8, len);
            errdefer allocator.free(str);
            const bytes_read = try reader.readAll(str);
            if (bytes_read != len) {
                allocator.free(str);
                return error.EndOfStream;
            }
            break :blk PropertyValue{ .string_val = str };
        },
        5 => blk: {
            const len = try reader.readInt(u32, .little);
            const bytes = try allocator.alloc(u8, len);
            errdefer allocator.free(bytes);
            const bytes_read = try reader.readAll(bytes);
            if (bytes_read != len) {
                allocator.free(bytes);
                return error.EndOfStream;
            }
            break :blk PropertyValue{ .bytes_val = bytes };
        },
        6 => blk: {
            const count = try reader.readInt(u32, .little);
            const vec = try allocator.alloc(f32, count);
            errdefer allocator.free(vec);
            for (vec) |*f| {
                f.* = @bitCast(try reader.readInt(u32, .little));
            }
            break :blk PropertyValue{ .vector_val = vec };
        },
        7 => blk: {
            const count = try reader.readInt(u32, .little);
            if (count == 0) {
                break :blk PropertyValue{ .list_val = &[_]PropertyValue{} };
            }
            const list = try allocator.alloc(PropertyValue, count);
            errdefer {
                for (list) |*item| {
                    var v = item.*;
                    v.deinit(allocator);
                }
                allocator.free(list);
            }
            for (list) |*item| {
                item.* = try deserializePropertyValue(allocator, reader);
            }
            break :blk PropertyValue{ .list_val = list };
        },
        8 => blk: {
            const count = try reader.readInt(u32, .little);
            if (count == 0) {
                break :blk PropertyValue{ .map_val = &[_]PropertyValue.MapEntry{} };
            }
            const map = try allocator.alloc(PropertyValue.MapEntry, count);
            errdefer {
                for (map) |*entry| {
                    allocator.free(entry.key);
                    var v = entry.value;
                    v.deinit(allocator);
                }
                allocator.free(map);
            }
            for (map) |*entry| {
                const key_len = try reader.readInt(u32, .little);
                const key = try allocator.alloc(u8, key_len);
                errdefer allocator.free(key);
                const key_read = try reader.readAll(key);
                if (key_read != key_len) {
                    allocator.free(key);
                    return error.EndOfStream;
                }
                entry.key = key;
                entry.value = try deserializePropertyValue(allocator, reader);
            }
            break :blk PropertyValue{ .map_val = map };
        },
        else => error.InvalidPayload,
    };
}

// ============================================================================
// Tests
// ============================================================================

test "node_insert: serialize and deserialize round-trip" {
    var buf: [256]u8 = undefined;
    const labels = [_][]const u8{ "Person", "Employee", "Manager" };

    const serialized = try serializeNodeInsert(&buf, 42, &labels);
    const result = try deserializeNodeInsert(serialized);

    try std.testing.expectEqual(@as(u64, 42), result.node_id);
    try std.testing.expectEqual(@as(u16, 3), result.label_count);

    var iter = result.labelIterator();
    try std.testing.expectEqualStrings("Person", iter.next().?);
    try std.testing.expectEqualStrings("Employee", iter.next().?);
    try std.testing.expectEqualStrings("Manager", iter.next().?);
    try std.testing.expect(iter.next() == null);
}

test "node_insert: empty labels" {
    var buf: [256]u8 = undefined;
    const labels = [_][]const u8{};

    const serialized = try serializeNodeInsert(&buf, 100, &labels);
    const result = try deserializeNodeInsert(serialized);

    try std.testing.expectEqual(@as(u64, 100), result.node_id);
    try std.testing.expectEqual(@as(u16, 0), result.label_count);
}

test "node_insert: buffer too small" {
    var buf: [5]u8 = undefined;
    const labels = [_][]const u8{ "Person", "Employee" };

    const result = serializeNodeInsert(&buf, 42, &labels);
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

    const serialized = try serializeEdgeInsert(&buf, 1, 2, "KNOWS");
    const result = try deserializeEdgeInsert(serialized);

    try std.testing.expectEqual(@as(u64, 1), result.source);
    try std.testing.expectEqual(@as(u64, 2), result.target);
    try std.testing.expectEqualStrings("KNOWS", result.edge_type);
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

    const serialized = try serializePropertyUpdate(&buf, 42, "name", old_val, new_val);
    const result = try deserializePropertyUpdate(serialized);

    try std.testing.expectEqual(@as(u64, 42), result.node_id);
    try std.testing.expectEqualStrings("name", result.key);
    try std.testing.expectEqualStrings(old_val, result.old_value.?);
    try std.testing.expectEqualStrings(new_val, result.new_value);
}

test "property_update: serialize and deserialize without old value" {
    var buf: [256]u8 = undefined;
    const new_val = "created";

    const serialized = try serializePropertyUpdate(&buf, 100, "status", null, new_val);
    const result = try deserializePropertyUpdate(serialized);

    try std.testing.expectEqual(@as(u64, 100), result.node_id);
    try std.testing.expectEqualStrings("status", result.key);
    try std.testing.expect(result.old_value == null);
    try std.testing.expectEqualStrings(new_val, result.new_value);
}

test "wrong payload type returns error" {
    var buf: [256]u8 = undefined;

    // Serialize as edge_insert
    const serialized = try serializeEdgeInsert(&buf, 1, 2, "KNOWS");

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

test "node_delete: buffer too small" {
    var buf: [10]u8 = undefined;
    const label_ids = [_]u16{ 1, 2 };
    const props = "some_properties";

    const result = serializeNodeDelete(&buf, 42, &label_ids, props);
    try std.testing.expectError(PayloadError.BufferTooSmall, result);
}

test "edge_insert: buffer too small" {
    var buf: [10]u8 = undefined;

    const result = serializeEdgeInsert(&buf, 1, 2, "KNOWS");
    try std.testing.expectError(PayloadError.BufferTooSmall, result);
}

test "edge_delete: buffer too small" {
    var buf: [15]u8 = undefined;
    const props = "edge_properties";

    const result = serializeEdgeDelete(&buf, 1, 2, 100, props);
    try std.testing.expectError(PayloadError.BufferTooSmall, result);
}

test "property_update: buffer too small" {
    var buf: [10]u8 = undefined;
    const old_val = "old_value";
    const new_val = "new_value";

    const result = serializePropertyUpdate(&buf, 42, "name", old_val, new_val);
    try std.testing.expectError(PayloadError.BufferTooSmall, result);
}
