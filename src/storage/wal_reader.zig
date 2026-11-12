//! Reading a write-ahead log from outside the process that writes it.
//!
//! `WalIterator` in wal.zig needs a live `WalManager`, so it is only available to
//! the writer. This is the read-only counterpart: it opens the file directly and
//! hands back whole frames, which is what a backup or replication follower needs.
//!
//! ## Why this is safe without coordinating with the writer
//!
//! The writer assembles a frame in memory, writes it in one call at a computed
//! offset, and only then increments `frame_count` in the header. So the header is
//! a publish barrier: every frame below `frame_count` is already on disk in full.
//! A reader that trusts the header never has to guess whether it is looking at a
//! frame still being written.
//!
//! Frames are fixed size, so frame `n` always lives at
//! `WAL_HEADER_SIZE + n * frame_size`. No index or scan is needed to find one.
//!
//! ## Two failures that look alike and are not
//!
//! `FrameChecksumMismatch` is usually **transient**. The publish barrier makes it
//! unlikely, but a large write is not necessarily atomic, so a reader can still
//! catch a frame mid-flight. The right response is to wait and read it again, not
//! to declare the log corrupt. A mismatch that survives several attempts is a real
//! problem; one that clears on retry never was.
//!
//! `Rewound` means the log was truncated while the reader was following it, which
//! happens whenever the writer checkpoints. Frame numbering restarts from zero, so
//! frames the reader has already seen are gone and the numbers it holds no longer
//! refer to the same records. A follower has to start a new generation from a
//! fresh snapshot rather than carry on counting.

const std = @import("std");
const wal_mod = @import("wal.zig");
const page = @import("page.zig");
const vfs_mod = @import("vfs.zig");

const WalHeader = wal_mod.WalHeader;
const WalFrameHeader = wal_mod.WalFrameHeader;

pub const WalReaderError = error{
    /// The file does not begin with a write-ahead log header.
    InvalidMagic,
    /// Written by a newer format than this build understands.
    UnsupportedVersion,
    /// The log belongs to a different database than the caller expected.
    UuidMismatch,
    /// The header did not survive its own checksum. The file is damaged, or was
    /// caught mid-update; refresh again before concluding anything.
    HeaderChecksumMismatch,
    /// Asked for a frame the writer has not published yet.
    FrameNotYetDurable,
    /// The frame did not match its checksum. Usually a torn read: retry.
    FrameChecksumMismatch,
    /// The log was truncated, so earlier frame numbers no longer mean anything.
    Rewound,
    /// The header claims a frame size this build cannot handle.
    InvalidFrameSize,
    IoError,
    OutOfMemory,
};

/// One frame, borrowed from the reader's buffer.
///
/// Valid until the next call to `readFrame` on the same reader. Copy anything
/// that needs to outlive that.
pub const Frame = struct {
    /// Position in the log, counting from zero.
    number: u64,
    /// How many records the frame carries.
    record_count: u16,
    /// LSN of the last record in the previous frame, for walking backwards.
    prev_frame_lsn: u64,
    /// The record bytes, without the frame header.
    data: []const u8,
    /// The whole frame including its header, which is what a replicator ships.
    raw: []const u8,
};

pub const WalReader = struct {
    allocator: std.mem.Allocator,
    file: vfs_mod.File,
    /// Frame size this log was written with.
    frame_size: u32,
    /// Frames the writer has published as of the last refresh.
    frame_count: u64,
    /// Highest frame count ever seen, used to notice a truncation.
    high_water: u64,
    /// LSN recovery would replay from.
    checkpoint_lsn: u64,
    /// The database this log belongs to.
    database_uuid: [16]u8,
    buffer: []u8,

    const Self = @This();

    /// Open a log for reading.
    ///
    /// Pass `expected_uuid` to refuse a log belonging to a different database,
    /// which is worth doing whenever the caller already knows which database it
    /// is following. A stray log replayed into the wrong file is not a failure
    /// that announces itself later.
    pub fn open(
        allocator: std.mem.Allocator,
        vfs: vfs_mod.Vfs,
        path: []const u8,
        expected_uuid: ?[16]u8,
    ) WalReaderError!Self {
        var file = vfs.open(path, .{ .read = true }) catch return WalReaderError.IoError;
        errdefer file.close();

        var header: WalHeader = undefined;
        try readHeaderInto(file, &header);

        if (expected_uuid) |want| {
            if (!std.mem.eql(u8, &header.database_uuid, &want)) {
                return WalReaderError.UuidMismatch;
            }
        }

        const buffer = allocator.alloc(u8, header.frame_size) catch {
            return WalReaderError.OutOfMemory;
        };

        return Self{
            .allocator = allocator,
            .file = file,
            .frame_size = header.frame_size,
            .frame_count = header.frame_count,
            .high_water = header.frame_count,
            .checkpoint_lsn = header.checkpoint_lsn,
            .database_uuid = header.database_uuid,
            .buffer = buffer,
        };
    }

    pub fn close(self: *Self) void {
        self.allocator.free(self.buffer);
        self.file.close();
    }

    /// Re-read the header to pick up frames written since the last look.
    ///
    /// Returns `Rewound` if the log has been truncated, which means the reader's
    /// frame numbers no longer refer to the records it thinks they do.
    pub fn refresh(self: *Self) WalReaderError!void {
        var header: WalHeader = undefined;
        try readHeaderInto(self.file, &header);

        if (!std.mem.eql(u8, &header.database_uuid, &self.database_uuid)) {
            return WalReaderError.UuidMismatch;
        }

        if (header.frame_count < self.high_water) {
            // Record the new state anyway, so a caller that decides to start a
            // fresh generation can carry on with this same reader.
            self.frame_count = header.frame_count;
            self.high_water = header.frame_count;
            self.checkpoint_lsn = header.checkpoint_lsn;
            return WalReaderError.Rewound;
        }

        self.frame_count = header.frame_count;
        self.high_water = header.frame_count;
        self.checkpoint_lsn = header.checkpoint_lsn;
    }

    /// Read one published frame.
    ///
    /// The returned slices borrow the reader's buffer and are valid until the
    /// next call.
    pub fn readFrame(self: *Self, number: u64) WalReaderError!Frame {
        if (number >= self.frame_count) return WalReaderError.FrameNotYetDurable;

        const offset = wal_mod.WAL_HEADER_SIZE + number * @as(u64, self.frame_size);
        const read = self.file.read(offset, self.buffer) catch return WalReaderError.IoError;
        if (read != self.buffer.len) return WalReaderError.IoError;

        const frame_header = std.mem.bytesAsValue(
            WalFrameHeader,
            self.buffer[0..@sizeOf(WalFrameHeader)],
        ).*;

        const max_data = self.buffer.len - @sizeOf(WalFrameHeader);
        if (frame_header.data_size > max_data) {
            // A plausible frame cannot claim more data than the frame holds. This
            // is the same shape of problem as a bad checksum: retry, and treat it
            // as damage only if it persists.
            return WalReaderError.FrameChecksumMismatch;
        }

        const data = self.buffer[@sizeOf(WalFrameHeader)..][0..frame_header.data_size];
        if (wal_mod.classifyFrame(self.buffer) == .invalid) {
            return WalReaderError.FrameChecksumMismatch;
        }

        return Frame{
            .number = number,
            .record_count = frame_header.record_count,
            .prev_frame_lsn = frame_header.prev_frame_lsn,
            .data = data,
            .raw = self.buffer,
        };
    }

    /// Read a frame, retrying a checksum mismatch a few times.
    ///
    /// A frame caught mid-write fails its checksum and then succeeds a moment
    /// later. `sleep_ns` is called between attempts so the caller decides how to
    /// wait. A mismatch that survives every attempt is returned as-is: at that
    /// point it is damage rather than timing.
    pub fn readFrameRetrying(
        self: *Self,
        number: u64,
        attempts: usize,
        sleep_ns: u64,
    ) WalReaderError!Frame {
        var attempt: usize = 0;
        while (true) : (attempt += 1) {
            return self.readFrame(number) catch |err| {
                if (err != WalReaderError.FrameChecksumMismatch) return err;
                if (attempt + 1 >= attempts) return err;
                if (sleep_ns > 0) @import("compat").sleep(sleep_ns);
                continue;
            };
        }
    }
};

fn readHeaderInto(file: vfs_mod.File, header: *WalHeader) WalReaderError!void {
    var buf: [@sizeOf(WalHeader)]u8 = undefined;
    const read = file.read(0, &buf) catch return WalReaderError.IoError;
    if (read != buf.len) return WalReaderError.IoError;

    header.* = std.mem.bytesAsValue(WalHeader, &buf).*;

    if (header.magic != wal_mod.WAL_MAGIC) return WalReaderError.InvalidMagic;
    if (header.version > wal_mod.WAL_FORMAT_VERSION) return WalReaderError.UnsupportedVersion;
    if (header.version < wal_mod.MIN_READABLE_WAL_VERSION) return WalReaderError.UnsupportedVersion;
    if (header.frame_size < wal_mod.FRAME_SIZE) return WalReaderError.InvalidFrameSize;
    if (header.calculateHeaderChecksum() != header.checksum) {
        return WalReaderError.HeaderChecksumMismatch;
    }
}
