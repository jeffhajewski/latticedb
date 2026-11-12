//! Page management for Lattice database.
//!
//! Defines page types, headers, and low-level page operations.

const std = @import("std");
const lattice = @import("lattice");

const types = lattice.core.types;

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

/// Tree index for accessing tree_roots array
pub const TreeIndex = enum(u8) {
    node = 0,
    edge = 1,
    label = 2,
    symbol_forward = 3,
    symbol_reverse = 4,
    fts_dict = 5,
    fts_lengths = 6,
    fts_reverse = 7,
    hnsw_node = 8,
    edge_id_index = 9,
    stream_meta = 10,
    stream_events = 11,
    stream_offsets = 12,
    property_index_catalog = 13,
    node_property_index = 14,
    edge_property_index = 15,
};

/// Maximum number of B+Tree root pages stored in header
pub const MAX_TREES: usize = 16;

/// File header structure (first 4KB of database file)
pub const FileHeader = extern struct {
    magic: u32,
    format_version: u16,
    min_reader_version: u16,
    page_size: u32,
    flags: u32,
    node_count: u64,
    edge_count: u64,
    btree_root_page: PageId, // Legacy, kept for compatibility
    vector_segment_page: PageId,
    fts_segment_page: PageId,
    freelist_page: PageId,
    schema_page: PageId,
    tree_roots: [MAX_TREES]PageId, // B+Tree root pages
    wal_frame_count: u64,
    /// How many times the write-ahead log has been reset. Frame numbering
    /// restarts on every reset, so anything following the log needs this to tell
    /// frame 40 of one run from frame 40 of the next. It lives in the database
    /// file rather than the log because the log is exactly what gets thrown
    /// away, and because a follower has to survive the writer restarting.
    checkpoint_seq: u32,
    reserved1: [4]u8,
    file_uuid: [16]u8,
    created_timestamp: u64,
    modified_timestamp: u64,
    application_id: [32]u8,

    // Everything below was added in format 4 (0.17.0), past the end of the
    // header 0.16.0 knows. 0.16.0 copies only its own fields and writes the
    // rest of the header page as zeros, so once it has written to a file
    // these fields read back as zero, `extension_magic` no longer matches, and
    // they are ignored: the node id counter is recomputed by one scan and the
    // vector configuration is read from the vector pages again. That is how a
    // file can go back and forth between releases without either one trusting
    // a value the other did not keep up to date.

    /// `EXTENSION_MAGIC` when this release wrote the fields below.
    extension_magic: u32,
    /// Dimensions of the vector index, or 0 when no configuration is stored.
    vector_dimensions: u16,
    /// `VectorMetric` of the index, meaningful when `vector_dimensions` is set.
    vector_metric: u8,
    reserved2: u8,
    /// The id the next node created will get. Never lowered, so a deleted
    /// node's id is not handed out again. 0 when not stored.
    next_node_id: u64,
    /// HNSW connections per node on the upper layers and on layer 0. They
    /// fix the layout of the index's connection pages.
    vector_m: u16,
    vector_m_max0: u16,
    /// HNSW candidate list size while building. Affects quality, not layout.
    vector_ef_construction: u16,
    /// The oldest format version allowed to write to this file. A release
    /// whose FORMAT_VERSION is lower may still read it (that is
    /// `min_reader_version`'s call) but must open it read-only: the file can
    /// hold something such a release would not keep correct when it writes,
    /// such as a rule it does not know to enforce. 0, which is what a 0.16.0
    /// header has here, means no restriction. 0.16.0 predates this field and
    /// does not honour it; a file that must be kept from 0.16.0's writes has
    /// to raise `min_reader_version` instead.
    min_writer_version: u16,
    reserved3: [8]u8,

    /// Marks the format-4 fields as written by a release that keeps them.
    pub const EXTENSION_MAGIC: u32 = 0x3458_544C; // "LTX4"
    /// Where 0.16.0's header ends and the format-4 fields begin.
    pub const LEGACY_SIZE: usize = 200;

    /// How the vector index measures distance.
    pub const VectorMetric = enum(u8) {
        cosine = 1,
        euclidean = 2,
        inner_product = 3,
        _,
    };

    /// The vector index's configuration as the file records it.
    pub const VectorConfig = struct {
        dimensions: u16,
        metric: VectorMetric,
        m: u16,
        m_max0: u16,
        ef_construction: u16,

        /// What every database with vectors written before format 4 used:
        /// 0.16.0 could set only the dimensions.
        pub fn legacy(dimensions: u16) VectorConfig {
            return .{ .dimensions = dimensions, .metric = .cosine, .m = 16, .m_max0 = 32, .ef_construction = 200 };
        }

        /// Whether a release that predates format 4 would read the index
        /// correctly. It assumes cosine and m = 16 whatever the file holds.
        pub fn legacyReadable(self: VectorConfig) bool {
            return self.metric == .cosine and self.m == 16 and self.m_max0 == 32;
        }
    };

    pub fn hasExtension(self: *const FileHeader) bool {
        return self.extension_magic == EXTENSION_MAGIC;
    }

    /// Start keeping the format-4 fields, discarding whatever they held.
    pub fn resetExtension(self: *FileHeader) void {
        self.extension_magic = EXTENSION_MAGIC;
        self.vector_dimensions = 0;
        self.vector_metric = 0;
        self.reserved2 = 0;
        self.next_node_id = 0;
        self.vector_m = 0;
        self.vector_m_max0 = 0;
        self.vector_ef_construction = 0;
        self.min_writer_version = 0;
        self.reserved3 = [_]u8{0} ** 8;
    }

    /// The stored node id counter, or null when the file does not have one.
    pub fn storedNextNodeId(self: *const FileHeader) ?u64 {
        if (!self.hasExtension() or self.next_node_id == 0) return null;
        return self.next_node_id;
    }

    /// The stored vector configuration, or null when the file does not have
    /// one. A configuration this release cannot interpret is not returned.
    pub fn storedVectorConfig(self: *const FileHeader) ?VectorConfig {
        if (!self.hasExtension() or self.vector_dimensions == 0) return null;
        const metric: VectorMetric = @enumFromInt(self.vector_metric);
        switch (metric) {
            .cosine, .euclidean, .inner_product => {},
            _ => return null,
        }
        return .{
            .dimensions = self.vector_dimensions,
            .metric = metric,
            .m = self.vector_m,
            .m_max0 = self.vector_m_max0,
            .ef_construction = self.vector_ef_construction,
        };
    }

    /// Record the vector configuration. A configuration an older release
    /// would misread raises `min_reader_version`, so that release refuses the
    /// file instead.
    pub fn setVectorConfig(self: *FileHeader, config: VectorConfig) void {
        if (!self.hasExtension()) self.resetExtension();
        self.vector_dimensions = config.dimensions;
        self.vector_metric = @intFromEnum(config.metric);
        self.vector_m = config.m;
        self.vector_m_max0 = config.m_max0;
        self.vector_ef_construction = config.ef_construction;
        if (!config.legacyReadable() and self.min_reader_version < types.FORMAT_VERSION) {
            self.min_reader_version = types.FORMAT_VERSION;
        }
    }

    pub fn init() FileHeader {
        return .{
            .magic = types.MAGIC_NUMBER,
            .format_version = types.FORMAT_VERSION,
            .min_reader_version = types.MIN_READER_VERSION,
            .page_size = DEFAULT_PAGE_SIZE,
            .flags = 0x08, // Checksums enabled by default
            .node_count = 0,
            .edge_count = 0,
            .btree_root_page = types.NULL_PAGE,
            .vector_segment_page = types.NULL_PAGE,
            .fts_segment_page = types.NULL_PAGE,
            .freelist_page = types.NULL_PAGE,
            .schema_page = types.NULL_PAGE,
            .tree_roots = [_]PageId{types.NULL_PAGE} ** MAX_TREES,
            .wal_frame_count = 0,
            .checkpoint_seq = 0,
            .reserved1 = [_]u8{0} ** 4,
            .file_uuid = [_]u8{0} ** 16,
            .created_timestamp = 0,
            .modified_timestamp = 0,
            .application_id = [_]u8{0} ** 32,
            .extension_magic = EXTENSION_MAGIC,
            .vector_dimensions = 0,
            .vector_metric = 0,
            .reserved2 = 0,
            .next_node_id = 0,
            .vector_m = 0,
            .vector_m_max0 = 0,
            .vector_ef_construction = 0,
            .min_writer_version = types.FORMAT_VERSION,
            .reserved3 = [_]u8{0} ** 8,
        };
    }

    /// Get tree root page by index
    pub fn getTreeRoot(self: *const FileHeader, index: TreeIndex) PageId {
        return self.tree_roots[@intFromEnum(index)];
    }

    /// Set tree root page by index
    pub fn setTreeRoot(self: *FileHeader, index: TreeIndex, page_id: PageId) void {
        self.tree_roots[@intFromEnum(index)] = page_id;
    }

    /// Check if trees have been initialized
    pub fn hasInitializedTrees(self: *const FileHeader) bool {
        return self.tree_roots[@intFromEnum(TreeIndex.node)] != types.NULL_PAGE;
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

/// Calculate CRC32C checksum (Castagnoli polynomial).
///
/// Bit-identical to `std.hash.crc.Crc32Iscsi`, which every release up to
/// 0.16.0 used, but computed with the CPU's CRC instruction where there is one
/// (see crc32c.zig).
pub fn calculateChecksum(data: []const u8) u32 {
    return crc32c.hash(data);
}

pub const crc32c = @import("crc32c.zig");

test {
    _ = crc32c;
}

test "page checksums match the 0.16.0 byte-wise CRC32C" {
    var prng = std.Random.DefaultPrng.init(0x0160);
    const random = prng.random();
    var buf: [65536]u8 align(4096) = undefined;
    for ([_]usize{ 4096, 8192, 16384, 32768, 65535 }) |page_size| {
        for (0..20) |round| {
            switch (round % 4) {
                0 => random.bytes(buf[0..page_size]),
                1 => @memset(buf[0..page_size], 0),
                2 => @memset(buf[0..page_size], 0xFF),
                else => {
                    @memset(buf[0..page_size], 0);
                    buf[random.uintLessThan(usize, page_size)] = random.int(u8);
                },
            }
            // The page manager checksums bytes 8..end; the WAL whole frames.
            try std.testing.expectEqual(std.hash.crc.Crc32Iscsi.hash(buf[8..page_size]), calculateChecksum(buf[8..page_size]));
            try std.testing.expectEqual(std.hash.crc.Crc32Iscsi.hash(buf[0..page_size]), calculateChecksum(buf[0..page_size]));
        }
    }
}

test "file header keeps 0.16.0's layout and puts the format-4 fields after it" {
    // 0.16.0 reads and writes exactly the first LEGACY_SIZE bytes.
    try std.testing.expectEqual(@as(usize, 168), @offsetOf(FileHeader, "application_id"));
    try std.testing.expectEqual(FileHeader.LEGACY_SIZE, @offsetOf(FileHeader, "extension_magic"));
    try std.testing.expectEqual(@as(usize, 204), @offsetOf(FileHeader, "vector_dimensions"));
    try std.testing.expectEqual(@as(usize, 206), @offsetOf(FileHeader, "vector_metric"));
    try std.testing.expectEqual(@as(usize, 208), @offsetOf(FileHeader, "next_node_id"));
    try std.testing.expectEqual(@as(usize, 216), @offsetOf(FileHeader, "vector_m"));
    try std.testing.expectEqual(@as(usize, 218), @offsetOf(FileHeader, "vector_m_max0"));
    try std.testing.expectEqual(@as(usize, 220), @offsetOf(FileHeader, "vector_ef_construction"));
    try std.testing.expectEqual(@as(usize, 222), @offsetOf(FileHeader, "min_writer_version"));
    try std.testing.expectEqual(@as(usize, 232), @sizeOf(FileHeader));
    try std.testing.expect(@sizeOf(FileHeader) <= DEFAULT_PAGE_SIZE);
}

test "format-4 header fields read as absent once an older release zeroes them" {
    var header = FileHeader.init();
    header.next_node_id = 42;
    header.setVectorConfig(FileHeader.VectorConfig.legacy(8));
    try std.testing.expectEqual(@as(?u64, 42), header.storedNextNodeId());
    try std.testing.expectEqual(@as(u16, 8), header.storedVectorConfig().?.dimensions);
    try std.testing.expectEqual(types.MIN_READER_VERSION, header.min_reader_version);

    // What 0.16.0's header write does to the bytes past its own struct.
    const bytes = std.mem.asBytes(&header);
    @memset(bytes[FileHeader.LEGACY_SIZE..], 0);
    try std.testing.expectEqual(@as(?u64, null), header.storedNextNodeId());
    try std.testing.expect(header.storedVectorConfig() == null);
}

test "a vector configuration 0.16.0 would misread raises min_reader_version" {
    var header = FileHeader.init();
    header.setVectorConfig(.{ .dimensions = 4, .metric = .inner_product, .m = 16, .m_max0 = 32, .ef_construction = 200 });
    try std.testing.expectEqual(types.FORMAT_VERSION, header.min_reader_version);

    var other = FileHeader.init();
    other.setVectorConfig(.{ .dimensions = 4, .metric = .cosine, .m = 24, .m_max0 = 48, .ef_construction = 100 });
    try std.testing.expectEqual(types.FORMAT_VERSION, other.min_reader_version);

    var tuned = FileHeader.init();
    tuned.setVectorConfig(.{ .dimensions = 4, .metric = .cosine, .m = 16, .m_max0 = 32, .ef_construction = 64 });
    try std.testing.expectEqual(types.MIN_READER_VERSION, tuned.min_reader_version);
}

test "page header size" {
    try std.testing.expectEqual(@as(usize, 8), @sizeOf(PageHeader));
}

test "page type values" {
    try std.testing.expectEqual(@as(u8, 0x00), @intFromEnum(PageType.free));
    try std.testing.expectEqual(@as(u8, 0x01), @intFromEnum(PageType.btree_internal));
    try std.testing.expectEqual(@as(u8, 0x02), @intFromEnum(PageType.btree_leaf));
}

test "min_writer_version is the current format in a new header and no restriction once zeroed" {
    var header = FileHeader.init();
    try std.testing.expectEqual(types.FORMAT_VERSION, header.min_writer_version);
    header.resetExtension();
    try std.testing.expectEqual(@as(u16, 0), header.min_writer_version);
    const bytes = std.mem.asBytes(&header);
    try std.testing.expectEqual(@as(u16, 0), std.mem.readInt(u16, bytes[222..224], .little));
}
