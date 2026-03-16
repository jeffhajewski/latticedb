//! Core type definitions for Lattice database.
//!
//! These types form the foundation of the database, defining identifiers,
//! property values, and constants used throughout the codebase.

const std = @import("std");

/// Page identifier - supports up to 16TB at 4KB pages
pub const PageId = u32;

/// Node identifier
pub const NodeId = u64;

/// Edge identifier
pub const EdgeId = u64;

/// Label identifier (max 65535 labels)
pub const LabelId = u16;

/// Property key identifier (max 65535 properties per node/edge)
pub const PropertyId = u16;

/// Null page marker
pub const NULL_PAGE: PageId = 0;

/// Null node marker
pub const NULL_NODE: NodeId = 0;

/// Null edge marker
pub const NULL_EDGE: EdgeId = 0;

/// Database file magic number: "LTDB" (0x4C544442)
pub const MAGIC_NUMBER: u32 = 0x4C544442;

/// WAL file magic number: "WLOG" (0x574C4F47)
pub const WAL_MAGIC_NUMBER: u32 = 0x574C4F47;

/// Default page size in bytes
pub const DEFAULT_PAGE_SIZE: u32 = 4096;

/// File format version
pub const FORMAT_VERSION: u16 = 1;

/// Property value - a tagged union supporting multiple types
pub const PropertyValue = union(enum) {
    null_val: void,
    bool_val: bool,
    int_val: i64,
    float_val: f64,
    string_val: []const u8,
    bytes_val: []const u8,
    vector_val: []const f32,
    list_val: []const PropertyValue,
    map_val: []const MapEntry,

    pub const MapEntry = struct {
        key: []const u8,
        value: PropertyValue,
    };

    pub fn clone(self: PropertyValue, allocator: std.mem.Allocator) std.mem.Allocator.Error!PropertyValue {
        return switch (self) {
            .null_val => .{ .null_val = {} },
            .bool_val => |b| .{ .bool_val = b },
            .int_val => |i| .{ .int_val = i },
            .float_val => |f| .{ .float_val = f },
            .string_val => |s| .{ .string_val = try allocator.dupe(u8, s) },
            .bytes_val => |b| .{ .bytes_val = try allocator.dupe(u8, b) },
            .vector_val => |v| .{ .vector_val = try allocator.dupe(f32, v) },
            .list_val => |list| blk: {
                const cloned_list = try allocator.alloc(PropertyValue, list.len);
                var initialized: usize = 0;
                errdefer {
                    for (cloned_list[0..initialized]) |*item| {
                        item.deinit(allocator);
                    }
                    allocator.free(cloned_list);
                }

                for (list, 0..) |item, i| {
                    cloned_list[i] = try item.clone(allocator);
                    initialized += 1;
                }

                break :blk .{ .list_val = cloned_list };
            },
            .map_val => |map| blk: {
                const cloned_map = try allocator.alloc(MapEntry, map.len);
                var initialized: usize = 0;
                errdefer {
                    for (cloned_map[0..initialized]) |*entry| {
                        allocator.free(entry.key);
                        var mutable_value = entry.value;
                        mutable_value.deinit(allocator);
                    }
                    allocator.free(cloned_map);
                }

                for (map, 0..) |entry, i| {
                    const key = try allocator.dupe(u8, entry.key);
                    errdefer allocator.free(key);

                    const value = try entry.value.clone(allocator);
                    errdefer {
                        var mutable_value = value;
                        mutable_value.deinit(allocator);
                    }

                    cloned_map[i] = .{
                        .key = key,
                        .value = value,
                    };
                    initialized += 1;
                }

                break :blk .{ .map_val = cloned_map };
            },
        };
    }

    pub fn deinit(self: *PropertyValue, allocator: std.mem.Allocator) void {
        switch (self.*) {
            .string_val => |s| allocator.free(s),
            .bytes_val => |b| allocator.free(b),
            .vector_val => |v| allocator.free(v),
            .list_val => |list| {
                for (list) |*item| {
                    var mutable_item = item.*;
                    mutable_item.deinit(allocator);
                }
                allocator.free(list);
            },
            .map_val => |map| {
                for (map) |*entry| {
                    allocator.free(entry.key);
                    var mutable_value = entry.value;
                    mutable_value.deinit(allocator);
                }
                allocator.free(map);
            },
            else => {},
        }
    }
};

/// Vector dimension type
pub const VectorDimension = u16;

/// Maximum supported vector dimensions
pub const MAX_VECTOR_DIMENSIONS: VectorDimension = 4096;

/// Error codes for database operations
pub const Error = error{
    OutOfMemory,
    IoError,
    Corruption,
    InvalidArgument,
    NotFound,
    AlreadyExists,
    TransactionAborted,
    LockTimeout,
    ReadOnly,
    Full,
    VersionMismatch,
    ChecksumMismatch,
};

test "property value null" {
    const val = PropertyValue{ .null_val = {} };
    _ = val;
}

test "property value int" {
    const val = PropertyValue{ .int_val = 42 };
    try std.testing.expectEqual(@as(i64, 42), val.int_val);
}

test "property value clone deep copies heap-backed variants" {
    const allocator = std.testing.allocator;

    const payload = [_]u8{ 0xAB, 0xCD };
    const embedding = [_]f32{ 0.25, 0.5, 0.75 };
    var tags = [_]PropertyValue{
        .{ .string_val = "graph" },
        .{ .bytes_val = &payload },
    };
    var entries = [_]PropertyValue.MapEntry{
        .{ .key = "name", .value = .{ .string_val = "Alice" } },
        .{ .key = "embedding", .value = .{ .vector_val = &embedding } },
        .{ .key = "tags", .value = .{ .list_val = &tags } },
    };

    var cloned = try (PropertyValue{ .map_val = &entries }).clone(allocator);
    defer cloned.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 3), cloned.map_val.len);
    try std.testing.expectEqualStrings("name", cloned.map_val[0].key);
    try std.testing.expectEqualStrings("Alice", cloned.map_val[0].value.string_val);
    try std.testing.expect(cloned.map_val[0].key.ptr != entries[0].key.ptr);
    try std.testing.expect(cloned.map_val[0].value.string_val.ptr != entries[0].value.string_val.ptr);

    try std.testing.expectEqualSlices(f32, embedding[0..], cloned.map_val[1].value.vector_val);
    try std.testing.expect(cloned.map_val[1].value.vector_val.ptr != embedding[0..].ptr);

    const cloned_tags = cloned.map_val[2].value.list_val;
    try std.testing.expectEqual(@as(usize, 2), cloned_tags.len);
    try std.testing.expect(cloned_tags.ptr != tags[0..].ptr);
    try std.testing.expectEqualStrings("graph", cloned_tags[0].string_val);
    try std.testing.expect(cloned_tags[0].string_val.ptr != tags[0].string_val.ptr);
    try std.testing.expectEqualSlices(u8, payload[0..], cloned_tags[1].bytes_val);
    try std.testing.expect(cloned_tags[1].bytes_val.ptr != payload[0..].ptr);
}
