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
    list_val: []const PropertyValue,
    map_val: []const MapEntry,

    pub const MapEntry = struct {
        key: []const u8,
        value: PropertyValue,
    };

    pub fn deinit(self: *PropertyValue, allocator: std.mem.Allocator) void {
        switch (self.*) {
            .string_val => |s| allocator.free(s),
            .bytes_val => |b| allocator.free(b),
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
