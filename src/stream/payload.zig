//! Stream payload encoding.
//!
//! Stream records store user payloads using the same PropertyValue byte codec
//! used by logical WAL payloads so nested values have one serialization format.

const std = @import("std");
const lattice = @import("lattice");

const Allocator = std.mem.Allocator;
const PropertyValue = lattice.core.types.PropertyValue;
const wal_payload = lattice.transaction.wal_payload;

pub fn encodedSize(value: PropertyValue) usize {
    return wal_payload.propertyValueSize(value);
}

pub fn encodeAlloc(allocator: Allocator, value: PropertyValue) ![]u8 {
    const size = encodedSize(value);
    const bytes = try allocator.alloc(u8, size);
    errdefer allocator.free(bytes);
    const written = try wal_payload.serializePropertyValueToBuf(bytes, value);
    if (written != size) return error.InvalidPayload;
    return bytes;
}

pub fn decode(allocator: Allocator, bytes: []const u8) !PropertyValue {
    return wal_payload.deserializePropertyValueFromBytes(allocator, bytes);
}

test "stream payload round trip" {
    const allocator = std.testing.allocator;

    var entries = [_]PropertyValue.MapEntry{
        .{ .key = "name", .value = .{ .string_val = "Ada" } },
        .{ .key = "count", .value = .{ .int_val = 3 } },
    };
    const value = PropertyValue{ .map_val = &entries };

    const encoded = try encodeAlloc(allocator, value);
    defer allocator.free(encoded);

    var decoded = try decode(allocator, encoded);
    defer decoded.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 2), decoded.map_val.len);
    try std.testing.expectEqualStrings("name", decoded.map_val[0].key);
    try std.testing.expectEqualStrings("Ada", decoded.map_val[0].value.string_val);
    try std.testing.expectEqualStrings("count", decoded.map_val[1].key);
    try std.testing.expectEqual(@as(i64, 3), decoded.map_val[1].value.int_val);
}
