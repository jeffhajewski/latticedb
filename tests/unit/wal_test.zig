//! Behavioral tests for Write-Ahead Log (WAL).
//!
//! These tests verify the durability and recovery contracts of the WAL.

const std = @import("std");
const lattice = @import("lattice");

const wal_mod = lattice.storage.wal;
const vfs_mod = lattice.storage.vfs;
const page = lattice.storage.page;

const WalManager = wal_mod.WalManager;
const WalError = wal_mod.WalError;
const WalRecordType = wal_mod.WalRecordType;
const PosixVfs = vfs_mod.PosixVfs;

fn generateUuid() [16]u8 {
    var uuid: [16]u8 = undefined;
    @import("compat").randomBytes(&uuid);
    return uuid;
}

fn createTempPath(allocator: std.mem.Allocator, name: []const u8) ![]u8 {
    const timestamp = @import("compat").milliTimestamp();
    const random = @import("compat").randomInt(u32);
    var buf: [128]u8 = undefined;
    const path = try std.fmt.bufPrint(&buf, "/tmp/lattice_wal_test_{s}_{d}_{x}.wal", .{ name, timestamp, random });
    return allocator.dupe(u8, path);
}

// ============================================================================
// Contract: Written records are recoverable
// ============================================================================

test "wal: written records recoverable after reopen" {
    const allocator = std.testing.allocator;

    var posix_vfs = PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const path = try createTempPath(allocator, "reopen");
    defer allocator.free(path);
    defer vfs_impl.delete(path) catch {};

    const uuid = generateUuid();

    // Write records
    {
        var wal = try WalManager.init(allocator, vfs_impl, path, uuid);
        defer wal.deinit();

        const lsn1 = try wal.appendRecord(.txn_begin, 1, 0, "payload1");
        const lsn2 = try wal.appendRecord(.insert, 1, lsn1, "payload2");
        const lsn3 = try wal.appendRecord(.txn_commit, 1, lsn2, "");

        try std.testing.expectEqual(@as(u64, 1), lsn1);
        try std.testing.expectEqual(@as(u64, 2), lsn2);
        try std.testing.expectEqual(@as(u64, 3), lsn3);

        try wal.sync();
    }

    // Reopen and verify records exist
    {
        var wal = try WalManager.init(allocator, vfs_impl, path, uuid);
        defer wal.deinit();

        // Next LSN should be 4 (after the 3 we wrote)
        const next_lsn = wal.next_lsn;
        try std.testing.expectEqual(@as(u64, 4), next_lsn);

        // Iterate and count records
        var iter = wal.iterate(1);
        var payload_buf: [4096]u8 = undefined;
        var count: usize = 0;
        while (try iter.next(&payload_buf)) |_| {
            count += 1;
        }
        try std.testing.expectEqual(@as(usize, 3), count);
    }
}

test "wal: records survive multiple sync cycles" {
    const allocator = std.testing.allocator;

    var posix_vfs = PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const path = try createTempPath(allocator, "multisync");
    defer allocator.free(path);
    defer vfs_impl.delete(path) catch {};

    const uuid = generateUuid();

    {
        var wal = try WalManager.init(allocator, vfs_impl, path, uuid);
        defer wal.deinit();

        // Write and sync multiple times
        _ = try wal.appendRecord(.txn_begin, 1, 0, "batch1");
        try wal.sync();

        _ = try wal.appendRecord(.insert, 1, 1, "batch2");
        try wal.sync();

        _ = try wal.appendRecord(.txn_commit, 1, 2, "batch3");
        try wal.sync();
    }

    // Reopen and verify all records
    {
        var wal = try WalManager.init(allocator, vfs_impl, path, uuid);
        defer wal.deinit();

        var iter = wal.iterate(1);
        var payload_buf: [4096]u8 = undefined;
        var count: usize = 0;
        while (try iter.next(&payload_buf)) |_| {
            count += 1;
        }
        try std.testing.expectEqual(@as(usize, 3), count);
    }
}

// ============================================================================
// Contract: Frame numbers are monotonic
// ============================================================================

test "wal: frame numbers monotonically increasing" {
    const allocator = std.testing.allocator;

    var posix_vfs = PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const path = try createTempPath(allocator, "frameno");
    defer allocator.free(path);
    defer vfs_impl.delete(path) catch {};

    const uuid = generateUuid();

    var wal = try WalManager.init(allocator, vfs_impl, path, uuid);
    defer wal.deinit();

    // Write enough to span multiple frames
    // Frame data size is ~4064 bytes, record header is 32 bytes
    const large_payload = [_]u8{'x'} ** 1000;

    for (0..20) |i| {
        _ = try wal.appendRecord(.insert, 1, @intCast(i), &large_payload);
    }

    try wal.sync();

    // Header frame_count should be > 1
    try std.testing.expect(wal.header.frame_count > 1);
}

// ============================================================================
// Contract: UUID mismatch is detected
// ============================================================================

test "wal: rejects uuid mismatch" {
    const allocator = std.testing.allocator;

    var posix_vfs = PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const path = try createTempPath(allocator, "uuid");
    defer allocator.free(path);
    defer vfs_impl.delete(path) catch {};

    const uuid1 = generateUuid();
    var uuid2 = uuid1;
    uuid2[0] ^= 0xff;

    // Create WAL with uuid1
    {
        var wal = try WalManager.init(allocator, vfs_impl, path, uuid1);
        _ = try wal.appendRecord(.txn_begin, 1, 0, "data");
        try wal.sync();
        wal.deinit();
    }

    // Try to open with uuid2 - should fail
    const result = WalManager.init(allocator, vfs_impl, path, uuid2);
    try std.testing.expectError(WalError.UuidMismatch, result);
}

// ============================================================================
// Contract: Empty WAL is readable
// ============================================================================

test "wal: empty wal readable" {
    const allocator = std.testing.allocator;

    var posix_vfs = PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const path = try createTempPath(allocator, "empty");
    defer allocator.free(path);
    defer vfs_impl.delete(path) catch {};

    const uuid = generateUuid();

    // Create empty WAL
    {
        var wal = try WalManager.init(allocator, vfs_impl, path, uuid);
        wal.deinit();
    }

    // Reopen - should work
    {
        var wal = try WalManager.init(allocator, vfs_impl, path, uuid);
        defer wal.deinit();

        // Iterate - should return nothing
        var iter = wal.iterate(1);
        var payload_buf: [4096]u8 = undefined;
        const record = try iter.next(&payload_buf);
        try std.testing.expect(record == null);
    }
}

// ============================================================================
// Contract: LSN assignment is sequential
// ============================================================================

test "wal: lsn assigned sequentially" {
    const allocator = std.testing.allocator;

    var posix_vfs = PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const path = try createTempPath(allocator, "lsn");
    defer allocator.free(path);
    defer vfs_impl.delete(path) catch {};

    const uuid = generateUuid();

    var wal = try WalManager.init(allocator, vfs_impl, path, uuid);
    defer wal.deinit();

    var prev_lsn: u64 = 0;
    for (0..100) |_| {
        const lsn = try wal.appendRecord(.insert, 1, prev_lsn, "data");
        try std.testing.expectEqual(prev_lsn + 1, lsn);
        prev_lsn = lsn;
    }
}

// ============================================================================
// Contract: Checkpoint LSN can be set and retrieved
// ============================================================================

test "wal: checkpoint lsn updated correctly" {
    const allocator = std.testing.allocator;

    var posix_vfs = PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const path = try createTempPath(allocator, "ckpt");
    defer allocator.free(path);
    defer vfs_impl.delete(path) catch {};

    const uuid = generateUuid();

    // Write records and set checkpoint
    {
        var wal = try WalManager.init(allocator, vfs_impl, path, uuid);
        defer wal.deinit();

        _ = try wal.appendRecord(.txn_begin, 1, 0, "");
        _ = try wal.appendRecord(.insert, 1, 1, "data");
        const lsn = try wal.appendRecord(.txn_commit, 1, 2, "");

        try wal.setCheckpointLsn(lsn);
        try wal.sync();

        try std.testing.expectEqual(lsn, wal.getCheckpointLsn());
    }

    // Reopen and verify checkpoint persisted
    {
        var wal = try WalManager.init(allocator, vfs_impl, path, uuid);
        defer wal.deinit();

        try std.testing.expectEqual(@as(u64, 3), wal.getCheckpointLsn());
    }
}

// ============================================================================
// Contract: Large logical records are fragmented and reassembled
// ============================================================================

test "wal: 1MB logical payload fragmented and reassembled" {
    const allocator = std.testing.allocator;

    var posix_vfs = PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const path = try createTempPath(allocator, "toolarge");
    defer allocator.free(path);
    defer vfs_impl.delete(path) catch {};

    const uuid = generateUuid();

    var wal = try WalManager.init(allocator, vfs_impl, path, uuid);
    defer wal.deinit();

    const payload = try allocator.alloc(u8, 1024 * 1024);
    defer allocator.free(payload);
    @memset(payload, 'x');

    const lsn = try wal.appendRecord(.insert, 1, 0, payload);
    try std.testing.expect(lsn > 1);
    try wal.sync();

    var iter = wal.iterate(1);
    const read_buf = try allocator.alloc(u8, payload.len);
    defer allocator.free(read_buf);
    const record = (try iter.next(read_buf)).?;
    try std.testing.expectEqual(WalRecordType.insert, record.header.record_type);
    try std.testing.expectEqualSlices(u8, payload, record.payload);

    const end = try iter.next(read_buf);
    try std.testing.expect(end == null);
}

test "wal: logical record above max rejected" {
    const allocator = std.testing.allocator;

    var posix_vfs = PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const path = try createTempPath(allocator, "toolarge");
    defer allocator.free(path);
    defer vfs_impl.delete(path) catch {};

    const uuid = generateUuid();

    var wal = try WalManager.init(allocator, vfs_impl, path, uuid);
    defer wal.deinit();

    const huge_payload = try allocator.alloc(u8, wal_mod.MAX_LOGICAL_RECORD_PAYLOAD_SIZE + 1);
    defer allocator.free(huge_payload);

    const result = wal.appendRecord(.insert, 1, 0, huge_payload);
    try std.testing.expectError(WalError.RecordTooLarge, result);
}

test "wal: larger frame size preserves larger records" {
    const allocator = std.testing.allocator;

    var posix_vfs = PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const path = try createTempPath(allocator, "largeframe");
    defer allocator.free(path);
    defer vfs_impl.delete(path) catch {};

    const uuid = generateUuid();

    var large_payload: [12000]u8 = undefined;
    @memset(&large_payload, 'x');

    {
        var wal = try WalManager.initWithFrameSize(allocator, vfs_impl, path, uuid, 32768);
        defer wal.deinit();

        const lsn = try wal.appendRecord(.insert, 1, 0, &large_payload);
        try std.testing.expectEqual(@as(u64, 1), lsn);
        try wal.sync();
    }

    {
        var wal = try WalManager.init(allocator, vfs_impl, path, uuid);
        defer wal.deinit();

        try std.testing.expectEqual(@as(u32, 32768), wal.header.frame_size);

        var iter = wal.iterate(1);
        var read_buf: [12000]u8 = undefined;
        const record = (try iter.next(&read_buf)).?;
        try std.testing.expectEqual(WalRecordType.insert, record.header.record_type);
        try std.testing.expectEqualSlices(u8, &large_payload, record.payload);
    }
}

// ============================================================================
// Contract: Different record types are preserved
// ============================================================================

test "wal: record types preserved" {
    const allocator = std.testing.allocator;

    var posix_vfs = PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const path = try createTempPath(allocator, "types");
    defer allocator.free(path);
    defer vfs_impl.delete(path) catch {};

    const uuid = generateUuid();

    const expected_types = [_]WalRecordType{
        .txn_begin,
        .insert,
        .update,
        .delete,
        .txn_commit,
    };

    {
        var wal = try WalManager.init(allocator, vfs_impl, path, uuid);
        defer wal.deinit();

        var prev_lsn: u64 = 0;
        for (expected_types) |record_type| {
            const lsn = try wal.appendRecord(record_type, 1, prev_lsn, "");
            prev_lsn = lsn;
        }

        try wal.sync();
    }

    // Reopen and verify types
    {
        var wal = try WalManager.init(allocator, vfs_impl, path, uuid);
        defer wal.deinit();

        var iter = wal.iterate(1);
        var payload_buf: [4096]u8 = undefined;
        var i: usize = 0;
        while (try iter.next(&payload_buf)) |record| {
            try std.testing.expectEqual(expected_types[i], record.header.record_type);
            i += 1;
        }
        try std.testing.expectEqual(@as(usize, 5), i);
    }
}

// ============================================================================
// Contract: Transaction ID is preserved
// ============================================================================

test "wal: transaction id preserved" {
    const allocator = std.testing.allocator;

    var posix_vfs = PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const path = try createTempPath(allocator, "txnid");
    defer allocator.free(path);
    defer vfs_impl.delete(path) catch {};

    const uuid = generateUuid();

    {
        var wal = try WalManager.init(allocator, vfs_impl, path, uuid);
        defer wal.deinit();

        // Write records from different transactions
        _ = try wal.appendRecord(.txn_begin, 100, 0, "");
        _ = try wal.appendRecord(.txn_begin, 200, 0, "");
        _ = try wal.appendRecord(.insert, 100, 1, "data");
        _ = try wal.appendRecord(.insert, 200, 2, "data");
        _ = try wal.appendRecord(.txn_commit, 100, 3, "");
        _ = try wal.appendRecord(.txn_commit, 200, 4, "");

        try wal.sync();
    }

    // Reopen and verify transaction IDs
    {
        var wal = try WalManager.init(allocator, vfs_impl, path, uuid);
        defer wal.deinit();

        var iter = wal.iterate(1);
        var payload_buf: [4096]u8 = undefined;
        const expected_txns = [_]u64{ 100, 200, 100, 200, 100, 200 };
        var i: usize = 0;

        while (try iter.next(&payload_buf)) |record| {
            try std.testing.expectEqual(expected_txns[i], record.header.txn_id);
            i += 1;
        }
    }
}

// ============================================================================
// Contract: a log damaged at its tail by a power failure still opens
// ============================================================================

/// Write three frames, one record each, and return how many frames the log holds.
fn writeThreeFrames(wal: *WalManager) !u64 {
    _ = try wal.appendRecord(.insert, 1, 0, "first");
    try wal.sync();
    _ = try wal.appendRecord(.insert, 1, 0, "second");
    try wal.sync();
    _ = try wal.appendRecord(.insert, 1, 0, "third");
    try wal.sync();
    return wal.header.frame_count;
}

fn expectPayloads(wal: *WalManager, expected: []const []const u8) !void {
    var iter = wal.iterate(1);
    var payload_buf: [4096]u8 = undefined;
    for (expected) |payload| {
        const record = (try iter.next(&payload_buf)) orelse return error.TestExpectedEqual;
        try std.testing.expectEqualStrings(payload, record.payload);
    }
    try std.testing.expect(try iter.next(&payload_buf) == null);
}

fn frameOffset(wal: *const WalManager, frame: u64) u64 {
    return wal_mod.WAL_HEADER_SIZE + frame * wal.frame_size;
}

test "wal: a torn last frame is cut off and new frames replace it" {
    const allocator = std.testing.allocator;
    var posix_vfs = PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();
    const path = try createTempPath(allocator, "torn_tail");
    defer allocator.free(path);
    defer vfs_impl.delete(path) catch {};
    const uuid = generateUuid();

    {
        var wal = try WalManager.init(allocator, vfs_impl, path, uuid);
        defer wal.deinit();
        try std.testing.expectEqual(@as(u64, 3), try writeThreeFrames(&wal));
        // Damage the data of the last frame, as a write cut short would.
        const garbage = [_]u8{0xFF} ** 64;
        try wal.file.write(frameOffset(&wal, 2) + @sizeOf(wal_mod.WalFrameHeader), &garbage);
    }

    {
        var wal = try WalManager.init(allocator, vfs_impl, path, uuid);
        defer wal.deinit();
        try std.testing.expectEqual(@as(?u64, 2), wal.trimmed_tail_frame);
        try std.testing.expectEqual(@as(u64, 2), wal.header.frame_count);
        try expectPayloads(&wal, &.{ "first", "second" });

        _ = try wal.appendRecord(.insert, 2, 0, "fourth");
        try wal.sync();
    }

    // The new frame took the torn one's place, so the log reads straight
    // through and nothing looks like damage in the middle.
    var wal = try WalManager.init(allocator, vfs_impl, path, uuid);
    defer wal.deinit();
    try std.testing.expectEqual(@as(?u64, null), wal.trimmed_tail_frame);
    try expectPayloads(&wal, &.{ "first", "second", "fourth" });
}

test "wal: a header that counts a frame the file does not hold still opens" {
    const allocator = std.testing.allocator;
    var posix_vfs = PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();
    const path = try createTempPath(allocator, "short_file");
    defer allocator.free(path);
    defer vfs_impl.delete(path) catch {};
    const uuid = generateUuid();

    {
        var wal = try WalManager.init(allocator, vfs_impl, path, uuid);
        defer wal.deinit();
        _ = try writeThreeFrames(&wal);
        // The header reached the disk; half of the last frame did not.
        try wal.file.truncate(frameOffset(&wal, 2) + wal.frame_size / 2);
    }

    var wal = try WalManager.init(allocator, vfs_impl, path, uuid);
    defer wal.deinit();
    try std.testing.expectEqual(@as(u64, 2), wal.header.frame_count);
    try expectPayloads(&wal, &.{ "first", "second" });
}

test "wal: a torn header is rebuilt from the frames" {
    const allocator = std.testing.allocator;
    var posix_vfs = PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();
    const path = try createTempPath(allocator, "torn_header");
    defer allocator.free(path);
    defer vfs_impl.delete(path) catch {};
    const uuid = generateUuid();

    {
        var wal = try WalManager.init(allocator, vfs_impl, path, uuid);
        defer wal.deinit();
        _ = try writeThreeFrames(&wal);
        // The fields of the header from before the last frame, with the
        // checksum from after it: what a header write cut in half leaves.
        var stale = wal.header;
        stale.frame_count = 2;
        const bytes = std.mem.asBytes(&stale);
        try wal.file.write(0, bytes[0 .. bytes.len - 4]);
    }

    {
        var wal = try WalManager.init(allocator, vfs_impl, path, uuid);
        defer wal.deinit();
        try std.testing.expectEqual(@as(u64, 3), wal.header.frame_count);
        try expectPayloads(&wal, &.{ "first", "second", "third" });
    }

    // The repaired header was written back.
    var wal = try WalManager.init(allocator, vfs_impl, path, uuid);
    defer wal.deinit();
    try std.testing.expectEqual(wal.header.checksum, wal.header.calculateHeaderChecksum());
    try expectPayloads(&wal, &.{ "first", "second", "third" });
}

test "wal: damage in the middle of the log is left for recovery to report" {
    const allocator = std.testing.allocator;
    var posix_vfs = PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();
    const path = try createTempPath(allocator, "mid_damage");
    defer allocator.free(path);
    defer vfs_impl.delete(path) catch {};
    const uuid = generateUuid();

    {
        var wal = try WalManager.init(allocator, vfs_impl, path, uuid);
        defer wal.deinit();
        _ = try writeThreeFrames(&wal);
        const garbage = [_]u8{0xFF} ** 64;
        try wal.file.write(frameOffset(&wal, 1) + @sizeOf(wal_mod.WalFrameHeader), &garbage);
    }

    var wal = try WalManager.init(allocator, vfs_impl, path, uuid);
    defer wal.deinit();
    try std.testing.expectEqual(@as(?u64, null), wal.trimmed_tail_frame);
    try std.testing.expectEqual(@as(?u64, 1), wal.damaged_frame);
    try std.testing.expectEqual(@as(u64, 3), wal.header.frame_count);

    var iter = wal.iterate(1);
    var payload_buf: [4096]u8 = undefined;
    _ = try iter.next(&payload_buf);
    try std.testing.expectError(WalError.ChecksumMismatch, iter.next(&payload_buf));
}

/// Write one frame of one record, flushed but not synced.
fn writeUnsyncedFrame(wal: *WalManager, payload: []const u8) !void {
    _ = try wal.appendRecord(.insert, 1, 0, payload);
    try wal.flush();
}

/// Close a log without the sync that `deinit` does, as a crash would.
fn crashClose(wal: *WalManager) void {
    wal.file.close();
}

fn damageFrame(wal: *WalManager, frame: u64) !void {
    const garbage = [_]u8{0xFF} ** 64;
    try wal.file.write(frameOffset(wal, frame) + @sizeOf(wal_mod.WalFrameHeader), &garbage);
}

fn readFrameHeader(wal: *WalManager, frame: u64) !wal_mod.WalFrameHeader {
    var buf: [@sizeOf(wal_mod.WalFrameHeader)]u8 = undefined;
    _ = try wal.file.read(frameOffset(wal, frame), &buf);
    return std.mem.bytesToValue(wal_mod.WalFrameHeader, &buf);
}

/// Rewrite every frame of a log, and its header, as 0.16.0 wrote them:
/// checksums of the data alone, no sync statement, version 2.
fn rewriteAsVersion2(wal: *WalManager) !void {
    var frame_buf: [wal_mod.MAX_FRAME_SIZE]u8 = undefined;
    const frame = frame_buf[0..wal.frame_size];
    var n: u64 = 0;
    while (n < wal.header.frame_count) : (n += 1) {
        _ = try wal.file.read(frameOffset(wal, n), frame);
        const header = std.mem.bytesAsValue(wal_mod.WalFrameHeader, frame[0..@sizeOf(wal_mod.WalFrameHeader)]);
        header.synced_frames = 0;
        header.checksum = page.calculateChecksum(frame[@sizeOf(wal_mod.WalFrameHeader)..][0..header.data_size]);
        try std.testing.expectEqual(wal_mod.FrameKind.legacy, wal_mod.classifyFrame(frame));
        try wal.file.write(frameOffset(wal, n), frame);
    }
    var header = wal.header;
    header.version = 2;
    header.checksum = header.calculateHeaderChecksum();
    try wal.file.write(0, std.mem.asBytes(&header));
}

test "wal: frames say how much of the log was synced before them" {
    const allocator = std.testing.allocator;
    var posix_vfs = PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();
    const path = try createTempPath(allocator, "ldur_synced_claims");
    defer allocator.free(path);
    defer vfs_impl.delete(path) catch {};
    const uuid = generateUuid();

    var wal = try WalManager.init(allocator, vfs_impl, path, uuid);
    defer wal.deinit();
    try std.testing.expectEqual(@as(u16, 2), wal.header.version);

    _ = try writeThreeFrames(&wal);
    try writeUnsyncedFrame(&wal, "fourth");
    try writeUnsyncedFrame(&wal, "fifth");

    try std.testing.expectEqual(@as(u32, 0), (try readFrameHeader(&wal, 0)).synced_frames);
    try std.testing.expectEqual(@as(u32, 1), (try readFrameHeader(&wal, 1)).synced_frames);
    try std.testing.expectEqual(@as(u32, 2), (try readFrameHeader(&wal, 2)).synced_frames);
    try std.testing.expectEqual(@as(u32, 3), (try readFrameHeader(&wal, 3)).synced_frames);
    try std.testing.expectEqual(@as(u32, 3), (try readFrameHeader(&wal, 4)).synced_frames);
    // Frames are in the log, which an older release must refuse.
    try std.testing.expectEqual(wal_mod.WAL_FORMAT_VERSION, wal.header.version);

    // An emptied log is one every release can read.
    try wal.truncateToEmpty();
    try std.testing.expectEqual(wal_mod.EMPTY_LOG_VERSION, wal.header.version);
    _ = try wal.appendRecord(.insert, 1, 0, "after");
    try wal.sync();
    try std.testing.expectEqual(@as(u32, 0), (try readFrameHeader(&wal, 0)).synced_frames);
}

test "wal: a frame the disk lost behind whole unsynced frames is a torn tail" {
    const allocator = std.testing.allocator;
    var posix_vfs = PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();
    const path = try createTempPath(allocator, "ldur_reordered_tail");
    defer allocator.free(path);
    defer vfs_impl.delete(path) catch {};
    const uuid = generateUuid();

    {
        var wal = try WalManager.init(allocator, vfs_impl, path, uuid);
        _ = try writeThreeFrames(&wal);
        try writeUnsyncedFrame(&wal, "fourth");
        try writeUnsyncedFrame(&wal, "fifth");
        try writeUnsyncedFrame(&wal, "sixth");
        // Power cut: the disk wrote the fifth and sixth frames and lost
        // the fourth. None of the three was synced.
        try damageFrame(&wal, 3);
        crashClose(&wal);
    }

    {
        var wal = try WalManager.init(allocator, vfs_impl, path, uuid);
        defer wal.deinit();
        try std.testing.expectEqual(@as(?u64, null), wal.damaged_frame);
        try std.testing.expectEqual(@as(?u64, 3), wal.trimmed_tail_frame);
        try std.testing.expectEqual(@as(u64, 3), wal.header.frame_count);
        try expectPayloads(&wal, &.{ "first", "second", "third" });
        _ = try wal.appendRecord(.insert, 1, 0, "seventh");
        try wal.sync();
    }

    // The frames behind the lost one went with it, and nothing older
    // than the new frame can turn up after it.
    var wal = try WalManager.init(allocator, vfs_impl, path, uuid);
    defer wal.deinit();
    try std.testing.expectEqual(@as(?u64, null), wal.trimmed_tail_frame);
    try expectPayloads(&wal, &.{ "first", "second", "third", "seventh" });
}

test "wal: damage to a frame a later frame says was synced is corruption" {
    const allocator = std.testing.allocator;
    var posix_vfs = PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();
    const path = try createTempPath(allocator, "ldur_synced_damage");
    defer allocator.free(path);
    defer vfs_impl.delete(path) catch {};
    const uuid = generateUuid();

    {
        var wal = try WalManager.init(allocator, vfs_impl, path, uuid);
        _ = try writeThreeFrames(&wal);
        try writeUnsyncedFrame(&wal, "fourth");
        // The third frame was synced, and the unsynced fourth says so.
        // Only the fourth vouches for it.
        try damageFrame(&wal, 2);
        crashClose(&wal);
    }

    var wal = try WalManager.init(allocator, vfs_impl, path, uuid);
    defer wal.deinit();
    try std.testing.expectEqual(@as(?u64, 2), wal.damaged_frame);
    try std.testing.expectEqual(@as(?u64, null), wal.trimmed_tail_frame);
    // Left exactly as found.
    try std.testing.expectEqual(@as(u64, 4), wal.header.frame_count);
}

test "wal: frames found at open are synced before new frames vouch for them" {
    const allocator = std.testing.allocator;
    var posix_vfs = PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();
    const path = try createTempPath(allocator, "ldur_open_sync");
    defer allocator.free(path);
    defer vfs_impl.delete(path) catch {};
    const uuid = generateUuid();

    {
        var wal = try WalManager.init(allocator, vfs_impl, path, uuid);
        try writeUnsyncedFrame(&wal, "first");
        try writeUnsyncedFrame(&wal, "second");
        crashClose(&wal);
    }

    var wal = try WalManager.init(allocator, vfs_impl, path, uuid);
    defer wal.deinit();
    try std.testing.expectEqual(@as(u64, 2), wal.durable_frames);
    try writeUnsyncedFrame(&wal, "third");
    try std.testing.expectEqual(@as(u32, 2), (try readFrameHeader(&wal, 2)).synced_frames);
}

test "wal: bytes past the counted frames are removed at open" {
    const allocator = std.testing.allocator;
    var posix_vfs = PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();
    const path = try createTempPath(allocator, "ldur_stale_frames");
    defer allocator.free(path);
    defer vfs_impl.delete(path) catch {};
    const uuid = generateUuid();

    {
        var wal = try WalManager.init(allocator, vfs_impl, path, uuid);
        _ = try writeThreeFrames(&wal);
        // The frame reached the disk and the header that counts it did
        // not.
        const counted = wal.header;
        try writeUnsyncedFrame(&wal, "fourth");
        try wal.file.write(0, std.mem.asBytes(&counted));
        crashClose(&wal);
    }

    {
        var wal = try WalManager.init(allocator, vfs_impl, path, uuid);
        defer wal.deinit();
        try std.testing.expectEqual(@as(u64, 3), wal.header.frame_count);
        try std.testing.expectEqual(frameOffset(&wal, 3), try wal.file.size());
        try expectPayloads(&wal, &.{ "first", "second", "third" });
    }
}

test "wal: a version 2 log still opens, and keeps the rule it was written under" {
    const allocator = std.testing.allocator;
    var posix_vfs = PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();
    const path = try createTempPath(allocator, "ldur_v2_log");
    defer allocator.free(path);
    defer vfs_impl.delete(path) catch {};
    const uuid = generateUuid();

    {
        var wal = try WalManager.init(allocator, vfs_impl, path, uuid);
        _ = try writeThreeFrames(&wal);
        try rewriteAsVersion2(&wal);
        crashClose(&wal);
    }

    // Read as written, then appended to: the new frames are version 3
    // and vouch for the old ones.
    {
        var wal = try WalManager.init(allocator, vfs_impl, path, uuid);
        defer wal.deinit();
        try expectPayloads(&wal, &.{ "first", "second", "third" });
        _ = try wal.appendRecord(.insert, 1, 0, "fourth");
        try wal.sync();
        try std.testing.expectEqual(@as(u32, 3), (try readFrameHeader(&wal, 3)).synced_frames);
        try std.testing.expectEqual(wal_mod.WAL_FORMAT_VERSION, wal.header.version);
    }
    {
        var wal = try WalManager.init(allocator, vfs_impl, path, uuid);
        defer wal.deinit();
        try expectPayloads(&wal, &.{ "first", "second", "third", "fourth" });
    }

    // Damage in a log of legacy frames: whole frames after it make it
    // corruption, as in 0.16.0.
    {
        var wal = try WalManager.init(allocator, vfs_impl, path, uuid);
        try wal.truncateToEmpty();
        _ = try writeThreeFrames(&wal);
        try rewriteAsVersion2(&wal);
        try damageFrame(&wal, 1);
        crashClose(&wal);
    }
    {
        var wal = try WalManager.init(allocator, vfs_impl, path, uuid);
        defer wal.deinit();
        try std.testing.expectEqual(@as(?u64, 1), wal.damaged_frame);
    }

    // And with nothing whole after it, a torn tail.
    {
        var wal = try WalManager.init(allocator, vfs_impl, path, uuid);
        try wal.truncateToEmpty();
        _ = try writeThreeFrames(&wal);
        try rewriteAsVersion2(&wal);
        try damageFrame(&wal, 2);
        crashClose(&wal);
    }
    var wal = try WalManager.init(allocator, vfs_impl, path, uuid);
    defer wal.deinit();
    try std.testing.expectEqual(@as(?u64, 2), wal.trimmed_tail_frame);
    try expectPayloads(&wal, &.{ "first", "second" });
}

test "wal: a version 3 frame with a damaged header fails its checksum" {
    const allocator = std.testing.allocator;
    var posix_vfs = PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();
    const path = try createTempPath(allocator, "ldur_header_damage");
    defer allocator.free(path);
    defer vfs_impl.delete(path) catch {};
    const uuid = generateUuid();

    var wal = try WalManager.init(allocator, vfs_impl, path, uuid);
    defer wal.deinit();
    _ = try writeThreeFrames(&wal);

    var frame_buf: [wal_mod.MAX_FRAME_SIZE]u8 = undefined;
    const frame = frame_buf[0..wal.frame_size];
    _ = try wal.file.read(frameOffset(&wal, 2), frame);
    try std.testing.expectEqual(wal_mod.FrameKind.current, wal_mod.classifyFrame(frame));
    // A larger sync statement would turn damage before it into corruption.
    frame[28] +%= 1;
    try std.testing.expectEqual(wal_mod.FrameKind.invalid, wal_mod.classifyFrame(frame));
}

test "wal: a frame that never reached the disk is not read as an empty one" {
    const allocator = std.testing.allocator;
    var posix_vfs = PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();
    const path = try createTempPath(allocator, "ldur_zero_frame");
    defer allocator.free(path);
    defer vfs_impl.delete(path) catch {};
    const uuid = generateUuid();

    {
        var wal = try WalManager.init(allocator, vfs_impl, path, uuid);
        _ = try writeThreeFrames(&wal);
        // One transaction over two frames, the second holding its commit.
        try writeUnsyncedFrame(&wal, "fourth");
        try writeUnsyncedFrame(&wal, "fifth");
        // Power cut: the disk wrote the fifth frame and not the fourth,
        // whose space reads back as zeroes. A frame of zeroes has a zero
        // checksum over no data, which is the checksum of no data.
        var zeroes: [wal_mod.MAX_FRAME_SIZE]u8 = @splat(0);
        try wal.file.write(frameOffset(&wal, 3), zeroes[0..wal.frame_size]);
        crashClose(&wal);
    }

    var wal = try WalManager.init(allocator, vfs_impl, path, uuid);
    defer wal.deinit();
    // Read as an empty frame, it let the fifth frame be replayed without
    // the fourth: a transaction applied in part.
    try std.testing.expectEqual(@as(?u64, 3), wal.trimmed_tail_frame);
    try expectPayloads(&wal, &.{ "first", "second", "third" });
}
