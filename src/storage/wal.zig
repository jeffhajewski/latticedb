//! Write-Ahead Log (WAL) for Lattice database.
//!
//! Ensures durability by logging changes before they're written to data pages.
//! The WAL is append-only and organized into fixed-size frames.

const std = @import("std");
const lattice = @import("lattice");

const Allocator = std.mem.Allocator;

const vfs = lattice.storage.vfs;
const page = lattice.storage.page;
const types = lattice.core.types;

const File = vfs.File;
const Vfs = vfs.Vfs;
const VfsError = vfs.VfsError;

const calculateChecksum = page.calculateChecksum;

/// WAL file magic number: "WLOG" (0x574C4F47)
pub const WAL_MAGIC: u32 = types.WAL_MAGIC_NUMBER;

/// Default WAL frame size. New databases opened through Database use the
/// database page size for WAL frames so larger page configurations can log
/// larger property values transactionally.
pub const FRAME_SIZE: u32 = 4096;
pub const MAX_FRAME_SIZE: u32 = std.math.maxInt(u16);
pub const WAL_FORMAT_VERSION: u16 = 2;
pub const MIN_READABLE_WAL_VERSION: u16 = 1;
pub const MAX_LOGICAL_RECORD_PAYLOAD_SIZE: usize = 64 * 1024 * 1024;

const LARGE_FRAGMENT_MAGIC: u32 = 0x31464c57; // "WLF1"
const LARGE_FRAGMENT_HEADER_SIZE: usize = 28;

/// WAL errors
pub const WalError = error{
    InvalidMagic,
    UuidMismatch,
    VersionMismatch,
    ChecksumMismatch,
    CorruptedFrame,
    RecordTooLarge,
    IoError,
    EndOfLog,
};

const WAL_HEADER_SIZE: u64 = @sizeOf(WalHeader);
const MAX_FRAME_DATA_SIZE: usize = MAX_FRAME_SIZE - @sizeOf(WalFrameHeader);
pub const MAX_RECORD_PAYLOAD_SIZE: usize = MAX_FRAME_DATA_SIZE - @sizeOf(WalRecordHeader);

fn isValidFrameSize(frame_size: u32) bool {
    return frame_size >= FRAME_SIZE and frame_size <= MAX_FRAME_SIZE;
}

fn frameDataSizeFor(frame_size: u32) usize {
    return @as(usize, frame_size) - @sizeOf(WalFrameHeader);
}

// ============================================================================
// WAL Structures
// ============================================================================

/// WAL file header (first 4KB)
/// Field order is chosen to avoid alignment padding
pub const WalHeader = extern struct {
    /// Magic number for identification
    magic: u32 = WAL_MAGIC,
    /// Size of each frame
    frame_size: u32 = FRAME_SIZE,
    /// Total number of frames written (8-byte aligned at offset 8)
    frame_count: u64 = 0,
    /// LSN of last checkpoint (for recovery starting point)
    checkpoint_lsn: u64 = 0,
    /// Must match the main database file UUID
    database_uuid: [16]u8,
    /// WAL format version
    version: u16 = WAL_FORMAT_VERSION,
    /// Reserved for future use
    _reserved: u16 = 0,
    /// Padding to 4096 bytes (4096 - 44 - 4 = 4048)
    _padding: [4048]u8 = [_]u8{0} ** 4048,
    /// Checksum of header (excluding this field)
    checksum: u32 = 0,

    comptime {
        std.debug.assert(@sizeOf(WalHeader) == 4096);
    }

    pub fn calculateHeaderChecksum(self: *const WalHeader) u32 {
        const bytes = std.mem.asBytes(self);
        return calculateChecksum(bytes[0 .. bytes.len - 4]);
    }
};

/// WAL frame header (32 bytes)
pub const WalFrameHeader = extern struct {
    /// Sequential frame number
    frame_number: u64,
    /// Number of records in this frame
    record_count: u16,
    /// Bytes of record data (excluding header)
    data_size: u16,
    /// LSN of last record in previous frame (for backward traversal)
    prev_frame_lsn: u64,
    /// CRC32C of frame data
    checksum: u32,
    /// Reserved
    _reserved: u32 = 0,

    comptime {
        std.debug.assert(@sizeOf(WalFrameHeader) == 32);
    }
};

/// Size of frame data area
pub const FRAME_DATA_SIZE: usize = FRAME_SIZE - @sizeOf(WalFrameHeader);

/// WAL record types
pub const WalRecordType = enum(u8) {
    // Transaction control
    txn_begin = 0x01,
    txn_commit = 0x02,
    txn_abort = 0x03,

    // Data modifications
    insert = 0x10,
    update = 0x11,
    delete = 0x12,

    // Page-level operations
    page_write = 0x20,

    // Checkpointing
    checkpoint_begin = 0x30,
    checkpoint_end = 0x31,

    // Savepoints
    savepoint = 0x40,
    savepoint_rollback = 0x41,

    // Compensation (for undo during recovery)
    clr = 0x50,

    // Physical record used to fragment a larger logical WAL record.
    large_fragment = 0x60,
};

/// WAL record header (32 bytes)
pub const WalRecordHeader = extern struct {
    /// Type of this record
    record_type: WalRecordType,
    /// Flags (reserved)
    flags: u8 = 0,
    /// Size of payload following this header
    payload_size: u16,
    /// Transaction that generated this record
    txn_id: u64,
    /// Log Sequence Number (assigned by WAL manager)
    lsn: u64,
    /// Previous LSN for this transaction (for rollback chain)
    prev_lsn: u64,

    comptime {
        std.debug.assert(@sizeOf(WalRecordHeader) == 32);
    }
};

/// Complete WAL record (header + payload)
pub const WalRecord = struct {
    header: WalRecordHeader,
    payload: []const u8,

    /// Total size of this record when serialized
    pub fn serializedSize(self: *const WalRecord) usize {
        return @sizeOf(WalRecordHeader) + self.payload.len;
    }
};

pub const OwnedWalRecord = struct {
    header: WalRecordHeader,
    payload: []u8,
    allocator: Allocator,

    pub fn deinit(self: *OwnedWalRecord) void {
        self.allocator.free(self.payload);
        self.* = undefined;
    }
};

const LargeFragmentInfo = struct {
    record_type: WalRecordType,
    total_len: usize,
    fragment_offset: usize,
    fragment_len: usize,
    fragment_index: u16,
    fragment_count: u16,
    logical_prev_lsn: u64,
    fragment: []const u8,
};

fn maxRecordPayloadSizeFor(frame_size: u32) usize {
    return frameDataSizeFor(frame_size) - @sizeOf(WalRecordHeader);
}

fn largeFragmentPayloadCapacityFor(frame_size: u32) usize {
    const physical_payload_size = maxRecordPayloadSizeFor(frame_size);
    if (physical_payload_size <= LARGE_FRAGMENT_HEADER_SIZE) return 0;
    return physical_payload_size - LARGE_FRAGMENT_HEADER_SIZE;
}

fn writeLargeFragmentPayload(
    buf: []u8,
    record_type: WalRecordType,
    total_len: usize,
    fragment_offset: usize,
    fragment_index: u16,
    fragment_count: u16,
    logical_prev_lsn: u64,
    fragment: []const u8,
) WalError![]const u8 {
    if (record_type == .large_fragment) return WalError.CorruptedFrame;
    if (total_len > MAX_LOGICAL_RECORD_PAYLOAD_SIZE) return WalError.RecordTooLarge;
    if (fragment.len > std.math.maxInt(u16)) return WalError.RecordTooLarge;
    if (buf.len < LARGE_FRAGMENT_HEADER_SIZE + fragment.len) return WalError.RecordTooLarge;

    std.mem.writeInt(u32, buf[0..4], LARGE_FRAGMENT_MAGIC, .little);
    buf[4] = @intFromEnum(record_type);
    buf[5] = 0;
    std.mem.writeInt(u16, buf[6..8], fragment_index, .little);
    std.mem.writeInt(u16, buf[8..10], fragment_count, .little);
    std.mem.writeInt(u16, buf[10..12], @intCast(fragment.len), .little);
    std.mem.writeInt(u32, buf[12..16], @intCast(total_len), .little);
    std.mem.writeInt(u32, buf[16..20], @intCast(fragment_offset), .little);
    std.mem.writeInt(u64, buf[20..28], logical_prev_lsn, .little);
    if (fragment.len > 0) {
        @memcpy(buf[LARGE_FRAGMENT_HEADER_SIZE..][0..fragment.len], fragment);
    }
    return buf[0 .. LARGE_FRAGMENT_HEADER_SIZE + fragment.len];
}

fn parseLargeFragmentPayload(payload: []const u8) WalError!LargeFragmentInfo {
    if (payload.len < LARGE_FRAGMENT_HEADER_SIZE) return WalError.CorruptedFrame;
    const magic = std.mem.readInt(u32, payload[0..4], .little);
    if (magic != LARGE_FRAGMENT_MAGIC) return WalError.CorruptedFrame;

    const record_type: WalRecordType = switch (payload[4]) {
        @intFromEnum(WalRecordType.txn_begin) => .txn_begin,
        @intFromEnum(WalRecordType.txn_commit) => .txn_commit,
        @intFromEnum(WalRecordType.txn_abort) => .txn_abort,
        @intFromEnum(WalRecordType.insert) => .insert,
        @intFromEnum(WalRecordType.update) => .update,
        @intFromEnum(WalRecordType.delete) => .delete,
        @intFromEnum(WalRecordType.page_write) => .page_write,
        @intFromEnum(WalRecordType.checkpoint_begin) => .checkpoint_begin,
        @intFromEnum(WalRecordType.checkpoint_end) => .checkpoint_end,
        @intFromEnum(WalRecordType.savepoint) => .savepoint,
        @intFromEnum(WalRecordType.savepoint_rollback) => .savepoint_rollback,
        @intFromEnum(WalRecordType.clr) => .clr,
        else => return WalError.CorruptedFrame,
    };
    if (record_type == .large_fragment) return WalError.CorruptedFrame;

    const fragment_index = std.mem.readInt(u16, payload[6..8], .little);
    const fragment_count = std.mem.readInt(u16, payload[8..10], .little);
    const fragment_len = std.mem.readInt(u16, payload[10..12], .little);
    const total_len = std.mem.readInt(u32, payload[12..16], .little);
    const fragment_offset = std.mem.readInt(u32, payload[16..20], .little);
    const logical_prev_lsn = std.mem.readInt(u64, payload[20..28], .little);

    if (total_len > MAX_LOGICAL_RECORD_PAYLOAD_SIZE) return WalError.RecordTooLarge;
    if (fragment_count == 0 or fragment_index >= fragment_count) return WalError.CorruptedFrame;
    if (payload.len != LARGE_FRAGMENT_HEADER_SIZE + fragment_len) return WalError.CorruptedFrame;
    if (@as(usize, fragment_offset) + @as(usize, fragment_len) > @as(usize, total_len)) {
        return WalError.CorruptedFrame;
    }

    return .{
        .record_type = record_type,
        .total_len = total_len,
        .fragment_offset = fragment_offset,
        .fragment_len = fragment_len,
        .fragment_index = fragment_index,
        .fragment_count = fragment_count,
        .logical_prev_lsn = logical_prev_lsn,
        .fragment = payload[LARGE_FRAGMENT_HEADER_SIZE..],
    };
}

// ============================================================================
// WAL Manager
// ============================================================================

/// Manages the Write-Ahead Log
pub const WalManager = struct {
    allocator: Allocator,
    file: File,
    header: WalHeader,
    frame_size: u32,
    /// Current frame being built
    current_frame_header: WalFrameHeader,
    current_frame_data: [MAX_FRAME_DATA_SIZE]u8,
    /// Offset within current frame's data area
    current_offset: usize,
    /// Next LSN to assign
    next_lsn: u64,
    /// Mutex for thread safety
    mutex: @import("compat").Mutex,

    const Self = @This();

    /// Open or create a WAL file
    pub fn init(allocator: Allocator, vfs_impl: Vfs, path: []const u8, db_uuid: [16]u8) WalError!Self {
        return initWithFrameSize(allocator, vfs_impl, path, db_uuid, FRAME_SIZE);
    }

    /// Open or create a WAL file using a requested frame size for new logs.
    /// Existing logs keep the frame size recorded in their header.
    pub fn initWithFrameSize(
        allocator: Allocator,
        vfs_impl: Vfs,
        path: []const u8,
        db_uuid: [16]u8,
        requested_frame_size: u32,
    ) WalError!Self {
        if (!isValidFrameSize(requested_frame_size)) return WalError.CorruptedFrame;

        const file = vfs_impl.open(path, .{ .read = true, .write = true, .create = true }) catch {
            return WalError.IoError;
        };
        errdefer file.close();

        const file_size = file.size() catch return WalError.IoError;

        var self = Self{
            .allocator = allocator,
            .file = file,
            .header = undefined,
            .frame_size = requested_frame_size,
            .current_frame_header = undefined,
            .current_frame_data = [_]u8{0} ** MAX_FRAME_DATA_SIZE,
            .current_offset = 0,
            .next_lsn = 1,
            .mutex = .{},
        };

        if (file_size == 0) {
            try self.initNewWal(db_uuid);
        } else {
            try self.loadWal(db_uuid);
        }

        return self;
    }

    fn frameDataSize(self: *const Self) usize {
        return frameDataSizeFor(self.frame_size);
    }

    fn maxRecordPayloadSize(self: *const Self) usize {
        return maxRecordPayloadSizeFor(self.frame_size);
    }

    /// Close the WAL file
    pub fn deinit(self: *Self) void {
        // Flush any pending records
        self.sync() catch {};
        self.file.close();
    }

    /// Initialize a new WAL file
    fn initNewWal(self: *Self, db_uuid: [16]u8) WalError!void {
        self.header = WalHeader{
            .frame_size = self.frame_size,
            .database_uuid = db_uuid,
        };
        self.header.checksum = self.header.calculateHeaderChecksum();

        self.writeHeader() catch return WalError.IoError;
        self.startNewFrame();
    }

    /// Load existing WAL file
    fn loadWal(self: *Self, db_uuid: [16]u8) WalError!void {
        // Read header
        var header_buf: [4096]u8 = undefined;
        const n = self.file.read(0, &header_buf) catch return WalError.IoError;
        if (n != 4096) return WalError.IoError;

        self.header = std.mem.bytesAsValue(WalHeader, header_buf[0..@sizeOf(WalHeader)]).*;
        if (!isValidFrameSize(self.header.frame_size)) return WalError.CorruptedFrame;
        self.frame_size = self.header.frame_size;

        // Validate magic
        if (self.header.magic != WAL_MAGIC) {
            return WalError.InvalidMagic;
        }

        if (self.header.version < MIN_READABLE_WAL_VERSION or self.header.version > WAL_FORMAT_VERSION) {
            return WalError.VersionMismatch;
        }

        // Validate UUID
        if (!std.mem.eql(u8, &self.header.database_uuid, &db_uuid)) {
            return WalError.UuidMismatch;
        }

        // Validate checksum
        const expected = self.header.calculateHeaderChecksum();
        if (self.header.checksum != expected) {
            return WalError.ChecksumMismatch;
        }

        // Find the end of valid frames and set next_lsn
        try self.findLogEnd();
    }

    /// Find the end of valid log entries
    fn findLogEnd(self: *Self) WalError!void {
        var max_lsn: u64 = 0;
        var frame_num: u64 = 0;

        while (frame_num < self.header.frame_count) {
            const offset = WAL_HEADER_SIZE + frame_num * self.frame_size;

            var frame_buf: [MAX_FRAME_SIZE]u8 = undefined;
            const frame = frame_buf[0..self.frame_size];
            const n = self.file.read(offset, frame) catch break;
            if (n != self.frame_size) break;

            const header = std.mem.bytesAsValue(WalFrameHeader, frame[0..@sizeOf(WalFrameHeader)]);
            if (header.data_size > self.frameDataSize()) break;

            // Validate checksum
            const data = frame[@sizeOf(WalFrameHeader)..][0..header.data_size];
            const expected = calculateChecksum(data);
            if (header.checksum != expected) break;

            // Find max LSN in this frame
            var record_offset: usize = 0;
            while (record_offset < header.data_size) {
                if (record_offset + @sizeOf(WalRecordHeader) > header.data_size) break;
                const rec_header = std.mem.bytesAsValue(
                    WalRecordHeader,
                    data[record_offset..][0..@sizeOf(WalRecordHeader)],
                );
                const record_size = @sizeOf(WalRecordHeader) + @as(usize, rec_header.payload_size);
                if (record_offset + record_size > header.data_size) break;
                if (rec_header.lsn > max_lsn) {
                    max_lsn = rec_header.lsn;
                }
                record_offset += record_size;
            }

            frame_num += 1;
        }

        self.next_lsn = max_lsn + 1;
        self.startNewFrame();
    }

    /// Write the WAL header to disk
    fn writeHeader(self: *Self) !void {
        self.header.checksum = self.header.calculateHeaderChecksum();
        const header_bytes = std.mem.asBytes(&self.header);
        self.file.write(0, header_bytes) catch return WalError.IoError;
    }

    /// Start a new frame
    fn startNewFrame(self: *Self) void {
        self.current_frame_header = WalFrameHeader{
            .frame_number = self.header.frame_count,
            .record_count = 0,
            .data_size = 0,
            .prev_frame_lsn = if (self.next_lsn > 1) self.next_lsn - 1 else 0,
            .checksum = 0,
        };
        self.current_offset = 0;
        @memset(self.current_frame_data[0..self.frameDataSize()], 0);
    }

    /// Append a record to the WAL
    /// Returns the assigned LSN
    pub fn appendRecord(self: *Self, record_type: WalRecordType, txn_id: u64, prev_lsn: u64, payload: []const u8) WalError!u64 {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (record_type == .large_fragment) return WalError.CorruptedFrame;

        if (payload.len <= self.maxRecordPayloadSize()) {
            return self.appendPhysicalRecordLocked(record_type, txn_id, prev_lsn, payload);
        }

        return self.appendFragmentedRecordLocked(record_type, txn_id, prev_lsn, payload);
    }

    fn appendPhysicalRecordLocked(
        self: *Self,
        record_type: WalRecordType,
        txn_id: u64,
        prev_lsn: u64,
        payload: []const u8,
    ) WalError!u64 {
        if (payload.len > std.math.maxInt(u16)) return WalError.RecordTooLarge;
        const record_size = @sizeOf(WalRecordHeader) + payload.len;

        if (record_size > self.frameDataSize()) {
            return WalError.RecordTooLarge;
        }

        // Check if record fits in current frame
        if (self.current_offset + record_size > self.frameDataSize()) {
            try self.flushCurrentFrame();
            self.startNewFrame();
        }

        const lsn = self.next_lsn;
        self.next_lsn += 1;

        // Build record header
        const header = WalRecordHeader{
            .record_type = record_type,
            .payload_size = @intCast(payload.len),
            .txn_id = txn_id,
            .lsn = lsn,
            .prev_lsn = prev_lsn,
        };

        // Write header to frame buffer
        const header_bytes = std.mem.asBytes(&header);
        @memcpy(self.current_frame_data[self.current_offset..][0..@sizeOf(WalRecordHeader)], header_bytes);
        self.current_offset += @sizeOf(WalRecordHeader);

        // Write payload to frame buffer
        if (payload.len > 0) {
            @memcpy(self.current_frame_data[self.current_offset..][0..payload.len], payload);
            self.current_offset += payload.len;
        }

        self.current_frame_header.record_count += 1;
        self.current_frame_header.data_size = @intCast(self.current_offset);

        return lsn;
    }

    fn appendFragmentedRecordLocked(
        self: *Self,
        record_type: WalRecordType,
        txn_id: u64,
        prev_lsn: u64,
        payload: []const u8,
    ) WalError!u64 {
        if (payload.len > MAX_LOGICAL_RECORD_PAYLOAD_SIZE) return WalError.RecordTooLarge;

        const fragment_capacity = largeFragmentPayloadCapacityFor(self.frame_size);
        if (fragment_capacity == 0) return WalError.RecordTooLarge;

        const fragment_count_usize = (payload.len + fragment_capacity - 1) / fragment_capacity;
        if (fragment_count_usize == 0 or fragment_count_usize > std.math.maxInt(u16)) {
            return WalError.RecordTooLarge;
        }

        var fragment_buf: [MAX_RECORD_PAYLOAD_SIZE]u8 = undefined;
        var offset: usize = 0;
        var fragment_index: u16 = 0;
        var physical_prev_lsn = prev_lsn;
        var last_lsn: u64 = prev_lsn;

        while (offset < payload.len) : (fragment_index += 1) {
            const chunk_len = @min(fragment_capacity, payload.len - offset);
            const fragment_payload = try writeLargeFragmentPayload(
                fragment_buf[0..self.maxRecordPayloadSize()],
                record_type,
                payload.len,
                offset,
                fragment_index,
                @intCast(fragment_count_usize),
                prev_lsn,
                payload[offset..][0..chunk_len],
            );
            last_lsn = try self.appendPhysicalRecordLocked(.large_fragment, txn_id, physical_prev_lsn, fragment_payload);
            physical_prev_lsn = last_lsn;
            offset += chunk_len;
        }

        return last_lsn;
    }

    /// Flush current frame to disk
    fn flushCurrentFrame(self: *Self) WalError!void {
        if (self.current_frame_header.record_count == 0) {
            return; // Nothing to flush
        }

        // Calculate checksum of data
        self.current_frame_header.checksum = calculateChecksum(
            self.current_frame_data[0..self.current_frame_header.data_size],
        );

        // Build complete frame
        var frame_buf: [MAX_FRAME_SIZE]u8 = [_]u8{0} ** MAX_FRAME_SIZE;
        const frame = frame_buf[0..self.frame_size];
        const header_bytes = std.mem.asBytes(&self.current_frame_header);
        @memcpy(frame[0..@sizeOf(WalFrameHeader)], header_bytes);
        @memcpy(frame[@sizeOf(WalFrameHeader)..][0..self.frameDataSize()], self.current_frame_data[0..self.frameDataSize()]);

        // Write frame to disk
        const offset = WAL_HEADER_SIZE + self.header.frame_count * self.frame_size;
        self.file.write(offset, frame) catch return WalError.IoError;

        self.header.frame_count += 1;

        // Update header on disk
        self.writeHeader() catch return WalError.IoError;
    }

    /// Sync WAL to disk (force durability)
    pub fn sync(self: *Self) WalError!void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.current_frame_header.record_count > 0) {
            try self.flushCurrentFrame();
            self.startNewFrame();
        }

        self.file.sync() catch return WalError.IoError;
    }

    /// Get current LSN (for tracking)
    pub fn getCurrentLsn(self: *Self) u64 {
        return self.next_lsn - 1;
    }

    /// Get checkpoint LSN
    pub fn getCheckpointLsn(self: *const Self) u64 {
        return self.header.checkpoint_lsn;
    }

    /// Set checkpoint LSN
    pub fn setCheckpointLsn(self: *Self, lsn: u64) WalError!void {
        self.mutex.lock();
        defer self.mutex.unlock();

        self.header.checkpoint_lsn = lsn;
        self.writeHeader() catch return WalError.IoError;
    }

    /// Create an iterator for reading WAL records
    pub fn iterate(self: *Self, start_lsn: u64) WalIterator {
        return WalIterator.init(self, start_lsn);
    }
};

// ============================================================================
// WAL Iterator
// ============================================================================

/// Iterator for reading WAL records
pub const WalIterator = struct {
    wal: *WalManager,
    current_frame: u64,
    current_offset: usize,
    frame_data: [MAX_FRAME_SIZE]u8,
    frame_header: WalFrameHeader,
    frame_loaded: bool,
    start_lsn: u64,

    const Self = @This();

    fn init(wal: *WalManager, start_lsn: u64) Self {
        return Self{
            .wal = wal,
            .current_frame = 0,
            .current_offset = 0,
            .frame_data = undefined,
            .frame_header = undefined,
            .frame_loaded = false,
            .start_lsn = start_lsn,
        };
    }

    /// Get the next record
    pub fn next(self: *Self, payload_buf: []u8) WalError!?WalRecord {
        var physical_buf: [MAX_RECORD_PAYLOAD_SIZE]u8 = undefined;
        const physical = (try self.nextPhysical(&physical_buf)) orelse return null;
        if (physical.header.record_type != .large_fragment) {
            if (physical.payload.len > payload_buf.len) return WalError.RecordTooLarge;
            if (physical.payload.len > 0) {
                @memcpy(payload_buf[0..physical.payload.len], physical.payload);
            }
            return .{
                .header = physical.header,
                .payload = payload_buf[0..physical.payload.len],
            };
        }

        return try self.reassembleLargeRecord(physical, &physical_buf, payload_buf);
    }

    /// Get the next logical record with an owned payload.
    pub fn nextAlloc(self: *Self, allocator: Allocator) WalError!?OwnedWalRecord {
        const physical_buf = allocator.alloc(u8, self.wal.maxRecordPayloadSize()) catch return WalError.IoError;
        defer allocator.free(physical_buf);

        const physical = (try self.nextPhysical(physical_buf)) orelse return null;
        if (physical.header.record_type != .large_fragment) {
            const owned = allocator.alloc(u8, physical.payload.len) catch return WalError.IoError;
            if (physical.payload.len > 0) @memcpy(owned, physical.payload);
            return .{
                .header = physical.header,
                .payload = owned,
                .allocator = allocator,
            };
        }

        const first = try parseLargeFragmentPayload(physical.payload);
        const owned = allocator.alloc(u8, first.total_len) catch return WalError.IoError;
        errdefer allocator.free(owned);

        const logical = (try self.reassembleLargeRecordInto(physical, first, physical_buf, owned)) orelse {
            allocator.free(owned);
            return null;
        };

        return .{
            .header = logical,
            .payload = owned,
            .allocator = allocator,
        };
    }

    fn nextPhysical(self: *Self, payload_buf: []u8) WalError!?WalRecord {
        while (true) {
            // Load frame if needed
            if (!self.frame_loaded or self.current_offset >= self.frame_header.data_size) {
                if (!try self.loadNextFrame()) {
                    return null; // End of WAL
                }
            }

            // Read record header
            const data_start = @sizeOf(WalFrameHeader);
            const record_start = data_start + self.current_offset;

            if (self.current_offset + @sizeOf(WalRecordHeader) > self.frame_header.data_size) {
                // Corrupt frame or end of data
                if (!try self.loadNextFrame()) {
                    return null;
                }
                continue;
            }

            const header = std.mem.bytesAsValue(
                WalRecordHeader,
                self.frame_data[record_start..][0..@sizeOf(WalRecordHeader)],
            ).*;

            // Check if this record meets the start_lsn criteria
            if (header.lsn < self.start_lsn) {
                self.current_offset += @sizeOf(WalRecordHeader) + header.payload_size;
                continue;
            }

            // Read payload
            const payload_start = record_start + @sizeOf(WalRecordHeader);
            const payload_len = header.payload_size;

            if (payload_len > payload_buf.len) {
                return WalError.RecordTooLarge;
            }

            if (payload_len > 0) {
                @memcpy(payload_buf[0..payload_len], self.frame_data[payload_start..][0..payload_len]);
            }

            self.current_offset += @sizeOf(WalRecordHeader) + payload_len;

            return WalRecord{
                .header = header,
                .payload = payload_buf[0..payload_len],
            };
        }
    }

    fn reassembleLargeRecord(
        self: *Self,
        first_record: WalRecord,
        physical_buf: []u8,
        payload_buf: []u8,
    ) WalError!?WalRecord {
        const first = try parseLargeFragmentPayload(first_record.payload);
        if (first.total_len > payload_buf.len) return WalError.RecordTooLarge;
        const header = (try self.reassembleLargeRecordInto(
            first_record,
            first,
            physical_buf,
            payload_buf[0..first.total_len],
        )) orelse return null;
        return .{
            .header = header,
            .payload = payload_buf[0..first.total_len],
        };
    }

    fn reassembleLargeRecordInto(
        self: *Self,
        first_record: WalRecord,
        first: LargeFragmentInfo,
        physical_buf: []u8,
        output: []u8,
    ) WalError!?WalRecordHeader {
        if (first.fragment_index != 0 or first.fragment_offset != 0) return WalError.CorruptedFrame;
        if (first.total_len != output.len) return WalError.RecordTooLarge;

        if (first.fragment_len > 0) {
            @memcpy(output[0..first.fragment_len], first.fragment);
        }

        var logical_header = first_record.header;
        logical_header.record_type = first.record_type;
        logical_header.prev_lsn = first.logical_prev_lsn;
        logical_header.payload_size = @intCast(@min(first.total_len, std.math.maxInt(u16)));

        var expected_offset = first.fragment_len;
        var expected_index: u16 = 1;
        var last_lsn = first_record.header.lsn;

        while (expected_index < first.fragment_count) : (expected_index += 1) {
            const next_record = (try self.nextPhysical(physical_buf)) orelse return null;
            if (next_record.header.record_type != .large_fragment) return WalError.CorruptedFrame;
            if (next_record.header.txn_id != first_record.header.txn_id) return WalError.CorruptedFrame;

            const fragment = try parseLargeFragmentPayload(next_record.payload);
            if (fragment.record_type != first.record_type or
                fragment.total_len != first.total_len or
                fragment.fragment_index != expected_index or
                fragment.fragment_count != first.fragment_count or
                fragment.fragment_offset != expected_offset or
                fragment.logical_prev_lsn != first.logical_prev_lsn)
            {
                return WalError.CorruptedFrame;
            }

            if (fragment.fragment_len > 0) {
                @memcpy(output[fragment.fragment_offset..][0..fragment.fragment_len], fragment.fragment);
            }
            expected_offset += fragment.fragment_len;
            last_lsn = next_record.header.lsn;
        }

        if (expected_offset != first.total_len) return WalError.CorruptedFrame;
        logical_header.lsn = last_lsn;
        return logical_header;
    }

    /// Load the next frame
    fn loadNextFrame(self: *Self) WalError!bool {
        if (self.current_frame >= self.wal.header.frame_count) {
            return false; // No more frames
        }

        const offset = WAL_HEADER_SIZE + self.current_frame * self.wal.frame_size;
        const frame = self.frame_data[0..self.wal.frame_size];
        const n = self.wal.file.read(offset, frame) catch return WalError.IoError;
        if (n != self.wal.frame_size) return WalError.IoError;

        self.frame_header = std.mem.bytesAsValue(
            WalFrameHeader,
            self.frame_data[0..@sizeOf(WalFrameHeader)],
        ).*;

        // Validate checksum
        if (self.frame_header.data_size > self.wal.frameDataSize()) return WalError.CorruptedFrame;
        const data = self.frame_data[@sizeOf(WalFrameHeader)..][0..self.frame_header.data_size];
        const expected = calculateChecksum(data);
        if (self.frame_header.checksum != expected) {
            return WalError.ChecksumMismatch;
        }

        self.current_frame += 1;
        self.current_offset = 0;
        self.frame_loaded = true;

        return true;
    }
};

// ============================================================================
// Tests
// ============================================================================

test "wal create new" {
    const allocator = std.testing.allocator;

    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const path = "/tmp/lattice_wal_test_create.wal";
    vfs_impl.delete(path) catch {};

    var uuid: [16]u8 = undefined;
    @import("compat").randomBytes(&uuid);

    var wal = try WalManager.init(allocator, vfs_impl, path, uuid);
    defer {
        wal.deinit();
        vfs_impl.delete(path) catch {};
    }

    try std.testing.expectEqual(WAL_MAGIC, wal.header.magic);
    try std.testing.expectEqual(@as(u64, 1), wal.next_lsn);
}

test "wal append and read records" {
    const allocator = std.testing.allocator;

    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const path = "/tmp/lattice_wal_test_append.wal";
    vfs_impl.delete(path) catch {};

    var uuid: [16]u8 = undefined;
    @import("compat").randomBytes(&uuid);

    var wal = try WalManager.init(allocator, vfs_impl, path, uuid);
    defer {
        wal.deinit();
        vfs_impl.delete(path) catch {};
    }

    // Append some records
    const lsn1 = try wal.appendRecord(.txn_begin, 1, 0, &[_]u8{});
    try std.testing.expectEqual(@as(u64, 1), lsn1);

    const payload = "test data";
    const lsn2 = try wal.appendRecord(.insert, 1, lsn1, payload);
    try std.testing.expectEqual(@as(u64, 2), lsn2);

    const lsn3 = try wal.appendRecord(.txn_commit, 1, lsn2, &[_]u8{});
    try std.testing.expectEqual(@as(u64, 3), lsn3);

    // Sync to disk
    try wal.sync();

    // Read back
    var iter = wal.iterate(1);
    var buf: [256]u8 = undefined;

    const rec1 = (try iter.next(&buf)).?;
    try std.testing.expectEqual(WalRecordType.txn_begin, rec1.header.record_type);
    try std.testing.expectEqual(@as(u64, 1), rec1.header.lsn);

    const rec2 = (try iter.next(&buf)).?;
    try std.testing.expectEqual(WalRecordType.insert, rec2.header.record_type);
    try std.testing.expectEqual(@as(u64, 2), rec2.header.lsn);
    try std.testing.expectEqualStrings(payload, rec2.payload);

    const rec3 = (try iter.next(&buf)).?;
    try std.testing.expectEqual(WalRecordType.txn_commit, rec3.header.record_type);
    try std.testing.expectEqual(@as(u64, 3), rec3.header.lsn);

    const rec4 = try iter.next(&buf);
    try std.testing.expect(rec4 == null);
}

test "wal reopen and continue" {
    const allocator = std.testing.allocator;

    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const path = "/tmp/lattice_wal_test_reopen.wal";
    vfs_impl.delete(path) catch {};

    var uuid: [16]u8 = undefined;
    @import("compat").randomBytes(&uuid);

    // Create and write
    {
        var wal = try WalManager.init(allocator, vfs_impl, path, uuid);
        _ = try wal.appendRecord(.txn_begin, 1, 0, &[_]u8{});
        _ = try wal.appendRecord(.txn_commit, 1, 1, &[_]u8{});
        try wal.sync();
        wal.deinit();
    }

    // Reopen and continue
    {
        var wal = try WalManager.init(allocator, vfs_impl, path, uuid);
        defer {
            wal.deinit();
            vfs_impl.delete(path) catch {};
        }

        // Next LSN should continue from where we left off
        try std.testing.expectEqual(@as(u64, 3), wal.next_lsn);

        // Append more
        const lsn = try wal.appendRecord(.txn_begin, 2, 0, &[_]u8{});
        try std.testing.expectEqual(@as(u64, 3), lsn);
    }
}

test "wal uuid mismatch" {
    const allocator = std.testing.allocator;

    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const path = "/tmp/lattice_wal_test_uuid.wal";
    vfs_impl.delete(path) catch {};

    var uuid1: [16]u8 = undefined;
    @import("compat").randomBytes(&uuid1);

    // Create with uuid1
    {
        var wal = try WalManager.init(allocator, vfs_impl, path, uuid1);
        try wal.sync();
        wal.deinit();
    }

    // Try to open with different uuid
    var uuid2 = uuid1;
    uuid2[0] ^= 0xff;

    const result = WalManager.init(allocator, vfs_impl, path, uuid2);
    try std.testing.expectError(WalError.UuidMismatch, result);

    vfs_impl.delete(path) catch {};
}

test "wal multiple frames" {
    const allocator = std.testing.allocator;

    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const path = "/tmp/lattice_wal_test_frames.wal";
    vfs_impl.delete(path) catch {};

    var uuid: [16]u8 = undefined;
    @import("compat").randomBytes(&uuid);

    var wal = try WalManager.init(allocator, vfs_impl, path, uuid);
    defer {
        wal.deinit();
        vfs_impl.delete(path) catch {};
    }

    // Write enough records to fill multiple frames
    // Each record is ~32 bytes header + payload
    // Frame data size is ~4064 bytes, so ~100 records per frame
    var i: u64 = 0;
    while (i < 300) : (i += 1) {
        _ = try wal.appendRecord(.insert, 1, 0, "some payload data");
    }

    try wal.sync();

    // Should have multiple frames
    try std.testing.expect(wal.header.frame_count >= 2);

    // Read all back
    var iter = wal.iterate(1);
    var buf: [256]u8 = undefined;
    var count: u64 = 0;

    while (try iter.next(&buf)) |_| {
        count += 1;
    }

    try std.testing.expectEqual(@as(u64, 300), count);
}
