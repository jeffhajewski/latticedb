//! Page management for Lattice database.
//!
//! Defines page types, headers, and low-level page operations.

const std = @import("std");
const types = @import("../core/types.zig");

pub const PageId = types.PageId;
pub const DEFAULT_PAGE_SIZE = types.DEFAULT_PAGE_SIZE;

/// Page type identifier
pub const PageType = enum(u8) {
    free = 0x00,
    btree_internal = 0x01,
    btree_leaf = 0x02,
    overflow = 0x03,
    vector_data = 0x04,
    hnsw_layer = 0x05,
    fts_dictionary = 0x06,
    fts_posting = 0x07,
    freelist = 0x08,
    schema = 0x09,
    segment_directory = 0x0A,
};

/// Common page header (8 bytes)
pub const PageHeader = extern struct {
    page_type: PageType,
    flags: u8,
    reserved: u16,
    checksum: u32,

    pub fn init(page_type: PageType) PageHeader {
        return .{
            .page_type = page_type,
            .flags = 0,
            .reserved = 0,
            .checksum = 0,
        };
    }
};

/// File header structure (first 4KB of database file)
pub const FileHeader = extern struct {
    magic: u32,
    format_version: u16,
    min_reader_version: u16,
    page_size: u32,
    flags: u32,
    node_count: u64,
    edge_count: u64,
    btree_root_page: PageId,
    vector_segment_page: PageId,
    fts_segment_page: PageId,
    freelist_page: PageId,
    schema_page: PageId,
    wal_frame_count: u64,
    checkpoint_seq: u32,
    reserved1: [20]u8,
    file_uuid: [16]u8,
    created_timestamp: u64,
    modified_timestamp: u64,
    application_id: [32]u8,

    pub fn init() FileHeader {
        return .{
            .magic = types.MAGIC_NUMBER,
            .format_version = types.FORMAT_VERSION,
            .min_reader_version = types.FORMAT_VERSION,
            .page_size = DEFAULT_PAGE_SIZE,
            .flags = 0x08, // Checksums enabled by default
            .node_count = 0,
            .edge_count = 0,
            .btree_root_page = types.NULL_PAGE,
            .vector_segment_page = types.NULL_PAGE,
            .fts_segment_page = types.NULL_PAGE,
            .freelist_page = types.NULL_PAGE,
            .schema_page = types.NULL_PAGE,
            .wal_frame_count = 0,
            .checkpoint_seq = 0,
            .reserved1 = [_]u8{0} ** 20,
            .file_uuid = [_]u8{0} ** 16,
            .created_timestamp = 0,
            .modified_timestamp = 0,
            .application_id = [_]u8{0} ** 32,
        };
    }
};

/// File header flags
pub const FileFlags = struct {
    pub const WAL_ENABLED: u32 = 0x01;
    pub const ENCRYPTION_ENABLED: u32 = 0x02;
    pub const COMPRESSION_ENABLED: u32 = 0x04;
    pub const CHECKSUMS_ENABLED: u32 = 0x08;
    pub const LARGE_FILE_MODE: u32 = 0x10;
};

/// Calculate CRC32C checksum (Castagnoli polynomial)
pub fn calculateChecksum(data: []const u8) u32 {
    return std.hash.crc.Crc32Iscsi.hash(data);
}

test "page header size" {
    try std.testing.expectEqual(@as(usize, 8), @sizeOf(PageHeader));
}

test "page type values" {
    try std.testing.expectEqual(@as(u8, 0x00), @intFromEnum(PageType.free));
    try std.testing.expectEqual(@as(u8, 0x01), @intFromEnum(PageType.btree_internal));
    try std.testing.expectEqual(@as(u8, 0x02), @intFromEnum(PageType.btree_leaf));
}
