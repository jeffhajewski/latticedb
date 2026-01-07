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
    std.crypto.random.bytes(&uuid);
    return uuid;
}

fn createTempPath(allocator: std.mem.Allocator, name: []const u8) ![]u8 {
    const timestamp = std.time.milliTimestamp();
    const random = std.crypto.random.int(u32);
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
    const uuid2 = generateUuid();

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
// Contract: Large records that exceed frame size are rejected
// ============================================================================

test "wal: record too large rejected" {
    const allocator = std.testing.allocator;

    var posix_vfs = PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const path = try createTempPath(allocator, "toolarge");
    defer allocator.free(path);
    defer vfs_impl.delete(path) catch {};

    const uuid = generateUuid();

    var wal = try WalManager.init(allocator, vfs_impl, path, uuid);
    defer wal.deinit();

    // Frame data size is ~4064 bytes, try to write something larger
    const huge_payload = [_]u8{'x'} ** 5000;

    const result = wal.appendRecord(.insert, 1, 0, &huge_payload);
    try std.testing.expectError(WalError.RecordTooLarge, result);
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
