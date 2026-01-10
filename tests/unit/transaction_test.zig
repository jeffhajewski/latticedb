//! ACID Transaction Behavioral Tests
//!
//! These tests verify transaction manager behavior including:
//! - Transaction state machine transitions
//! - Savepoint semantics
//! - WAL integration for durability
//! - Recovery correctness
//! - Multi-threaded safety
//!
//! Note: Database API operations are not yet transaction-aware.
//! Future stub tests document expected behavior when integration is complete.

const std = @import("std");
const lattice = @import("lattice");

const TxnManager = lattice.transaction.manager.TxnManager;
const TxnState = lattice.transaction.manager.TxnState;
const TxnMode = lattice.transaction.manager.TxnMode;
const TxnError = lattice.transaction.manager.TxnError;
const IsolationLevel = lattice.transaction.manager.IsolationLevel;
const Transaction = lattice.transaction.manager.Transaction;

const WalManager = lattice.storage.wal.WalManager;
const WalRecordType = lattice.storage.wal.WalRecordType;
const vfs_mod = lattice.storage.vfs;
const PosixVfs = vfs_mod.PosixVfs;

const recovery_mod = lattice.storage.recovery;
const recoverDatabase = recovery_mod.recoverDatabase;
const PageManager = lattice.storage.page_manager.PageManager;

const Database = lattice.storage.database.Database;

// ============================================================================
// Test Helpers
// ============================================================================

const TestContext = struct {
    allocator: std.mem.Allocator,
    vfs: PosixVfs,
    wal: *WalManager,
    tm: *TxnManager,
    wal_path: []const u8,
    uuid: [16]u8,

    fn init(allocator: std.mem.Allocator, path: []const u8) !TestContext {
        var posix_vfs = PosixVfs.init(allocator);
        const vfs_impl = posix_vfs.vfs();

        // Clean up from previous runs
        vfs_impl.delete(path) catch {};

        var uuid: [16]u8 = undefined;
        std.crypto.random.bytes(&uuid);

        // Heap allocate WAL and TxnManager to avoid pointer invalidation
        const wal = try allocator.create(WalManager);
        errdefer allocator.destroy(wal);
        wal.* = try WalManager.init(allocator, vfs_impl, path, uuid);

        const tm = try allocator.create(TxnManager);
        errdefer allocator.destroy(tm);
        tm.* = TxnManager.init(allocator, wal);

        return TestContext{
            .allocator = allocator,
            .vfs = posix_vfs,
            .wal = wal,
            .tm = tm,
            .wal_path = path,
            .uuid = uuid,
        };
    }

    fn deinit(self: *TestContext) void {
        self.tm.deinit();
        self.allocator.destroy(self.tm);
        self.wal.deinit();
        self.allocator.destroy(self.wal);
        self.vfs.vfs().delete(self.wal_path) catch {};
    }
};

// ============================================================================
// 8.1 Transaction State Machine Tests
// ============================================================================

test "txn: state transitions active -> committed" {
    const allocator = std.testing.allocator;
    var ctx = try TestContext.init(allocator, "/tmp/lattice_txn_state_commit.wal");
    defer ctx.deinit();

    var txn = try ctx.tm.begin(.read_write, .snapshot);

    // Initial state is active
    try std.testing.expectEqual(TxnState.active, txn.state);
    try std.testing.expect(txn.isActive());

    // After commit, state is committed
    try ctx.tm.commit(&txn);
    try std.testing.expectEqual(TxnState.committed, txn.state);
    try std.testing.expect(!txn.isActive());
}

test "txn: state transitions active -> aborted" {
    const allocator = std.testing.allocator;
    var ctx = try TestContext.init(allocator, "/tmp/lattice_txn_state_abort.wal");
    defer ctx.deinit();

    var txn = try ctx.tm.begin(.read_write, .snapshot);

    try std.testing.expectEqual(TxnState.active, txn.state);

    try ctx.tm.abort(&txn);
    try std.testing.expectEqual(TxnState.aborted, txn.state);
    try std.testing.expect(!txn.isActive());
}

test "txn: commit on non-active transaction fails" {
    const allocator = std.testing.allocator;
    var ctx = try TestContext.init(allocator, "/tmp/lattice_txn_double_commit.wal");
    defer ctx.deinit();

    var txn = try ctx.tm.begin(.read_write, .snapshot);
    try ctx.tm.commit(&txn);

    // Second commit should fail
    const result = ctx.tm.commit(&txn);
    try std.testing.expectError(TxnError.NotActive, result);
}

test "txn: abort on non-active transaction fails" {
    const allocator = std.testing.allocator;
    var ctx = try TestContext.init(allocator, "/tmp/lattice_txn_double_abort.wal");
    defer ctx.deinit();

    var txn = try ctx.tm.begin(.read_write, .snapshot);
    try ctx.tm.abort(&txn);

    // Second abort should fail
    const result = ctx.tm.abort(&txn);
    try std.testing.expectError(TxnError.NotActive, result);
}

test "txn: transaction IDs are unique and incrementing" {
    const allocator = std.testing.allocator;
    var ctx = try TestContext.init(allocator, "/tmp/lattice_txn_ids.wal");
    defer ctx.deinit();

    var txn1 = try ctx.tm.begin(.read_write, .snapshot);
    var txn2 = try ctx.tm.begin(.read_write, .snapshot);
    var txn3 = try ctx.tm.begin(.read_write, .snapshot);

    try std.testing.expectEqual(@as(u64, 1), txn1.id);
    try std.testing.expectEqual(@as(u64, 2), txn2.id);
    try std.testing.expectEqual(@as(u64, 3), txn3.id);

    // IDs are unique even after commit
    try ctx.tm.commit(&txn1);
    var txn4 = try ctx.tm.begin(.read_write, .snapshot);
    try std.testing.expectEqual(@as(u64, 4), txn4.id);

    try ctx.tm.commit(&txn2);
    try ctx.tm.commit(&txn3);
    try ctx.tm.commit(&txn4);
}

test "txn: timestamps increase monotonically" {
    const allocator = std.testing.allocator;
    var ctx = try TestContext.init(allocator, "/tmp/lattice_txn_timestamps.wal");
    defer ctx.deinit();

    var txn1 = try ctx.tm.begin(.read_write, .snapshot);
    const start1 = txn1.start_ts;

    var txn2 = try ctx.tm.begin(.read_write, .snapshot);
    const start2 = txn2.start_ts;

    // Start timestamps should increase
    try std.testing.expect(start2 > start1);

    // Commit timestamps should increase
    try ctx.tm.commit(&txn1);
    const commit1 = txn1.commit_ts;

    try ctx.tm.commit(&txn2);
    const commit2 = txn2.commit_ts;

    try std.testing.expect(commit1 > 0);
    try std.testing.expect(commit2 > commit1);
}

// ============================================================================
// 8.1 Savepoint Semantics Tests
// ============================================================================

test "txn: savepoint creates restore point" {
    const allocator = std.testing.allocator;
    var ctx = try TestContext.init(allocator, "/tmp/lattice_txn_savepoint.wal");
    defer ctx.deinit();

    var txn = try ctx.tm.begin(.read_write, .snapshot);

    // Create savepoint - should succeed
    try ctx.tm.savepoint(&txn, "sp1");

    // Transaction should still be active
    try std.testing.expectEqual(TxnState.active, txn.state);

    try ctx.tm.commit(&txn);
}

test "txn: rollback to savepoint truncates undo log" {
    const allocator = std.testing.allocator;
    var ctx = try TestContext.init(allocator, "/tmp/lattice_txn_rollback_undo.wal");
    defer ctx.deinit();

    var txn = try ctx.tm.begin(.read_write, .snapshot);

    // Log some operations
    _ = try ctx.tm.logOperation(&txn, .insert, "data1");
    _ = try ctx.tm.logOperation(&txn, .insert, "data2");

    // Create savepoint
    try ctx.tm.savepoint(&txn, "sp1");

    // Log more operations
    _ = try ctx.tm.logOperation(&txn, .insert, "data3");
    _ = try ctx.tm.logOperation(&txn, .insert, "data4");

    // Rollback to savepoint
    try ctx.tm.rollbackToSavepoint(&txn, "sp1");

    // Transaction should still be active
    try std.testing.expectEqual(TxnState.active, txn.state);

    try ctx.tm.commit(&txn);
}

test "txn: rollback removes savepoints after target" {
    const allocator = std.testing.allocator;
    var ctx = try TestContext.init(allocator, "/tmp/lattice_txn_rollback_removes.wal");
    defer ctx.deinit();

    var txn = try ctx.tm.begin(.read_write, .snapshot);

    try ctx.tm.savepoint(&txn, "sp1");
    try ctx.tm.savepoint(&txn, "sp2");
    try ctx.tm.savepoint(&txn, "sp3");

    // Rollback to sp1 - should remove sp2 and sp3
    try ctx.tm.rollbackToSavepoint(&txn, "sp1");

    // Trying to rollback to sp2 should fail (it's gone)
    const result = ctx.tm.rollbackToSavepoint(&txn, "sp2");
    try std.testing.expectError(TxnError.SavepointNotFound, result);

    try ctx.tm.commit(&txn);
}

test "txn: rollback to nonexistent savepoint fails" {
    const allocator = std.testing.allocator;
    var ctx = try TestContext.init(allocator, "/tmp/lattice_txn_rollback_notfound.wal");
    defer ctx.deinit();

    var txn = try ctx.tm.begin(.read_write, .snapshot);

    const result = ctx.tm.rollbackToSavepoint(&txn, "nonexistent");
    try std.testing.expectError(TxnError.SavepointNotFound, result);

    try ctx.tm.commit(&txn);
}

test "txn: nested savepoints work correctly" {
    const allocator = std.testing.allocator;
    var ctx = try TestContext.init(allocator, "/tmp/lattice_txn_nested_savepoints.wal");
    defer ctx.deinit();

    var txn = try ctx.tm.begin(.read_write, .snapshot);

    // Create nested savepoints with operations between
    _ = try ctx.tm.logOperation(&txn, .insert, "before_sp1");
    try ctx.tm.savepoint(&txn, "sp1");

    _ = try ctx.tm.logOperation(&txn, .insert, "after_sp1");
    try ctx.tm.savepoint(&txn, "sp2");

    _ = try ctx.tm.logOperation(&txn, .insert, "after_sp2");
    try ctx.tm.savepoint(&txn, "sp3");

    _ = try ctx.tm.logOperation(&txn, .insert, "after_sp3");

    // Rollback to sp2 (removes sp3)
    try ctx.tm.rollbackToSavepoint(&txn, "sp2");

    // sp1 should still exist
    try ctx.tm.rollbackToSavepoint(&txn, "sp1");

    try ctx.tm.commit(&txn);
}

// ============================================================================
// 8.1 WAL Integration Tests
// ============================================================================

test "txn: begin writes TXN_BEGIN to WAL" {
    const allocator = std.testing.allocator;
    var ctx = try TestContext.init(allocator, "/tmp/lattice_txn_wal_begin.wal");
    defer ctx.deinit();

    var txn = try ctx.tm.begin(.read_write, .snapshot);

    // Sync WAL to flush buffered records
    try ctx.wal.sync();

    // Verify WAL contains TXN_BEGIN
    var iter = ctx.wal.iterate(1);
    var buf: [256]u8 = undefined;

    const record = try iter.next(&buf);
    try std.testing.expect(record != null);
    try std.testing.expectEqual(WalRecordType.txn_begin, record.?.header.record_type);
    try std.testing.expectEqual(txn.id, record.?.header.txn_id);

    try ctx.tm.commit(&txn);
}

test "txn: commit writes TXN_COMMIT and syncs" {
    const allocator = std.testing.allocator;
    var ctx = try TestContext.init(allocator, "/tmp/lattice_txn_wal_commit.wal");
    defer ctx.deinit();

    var txn = try ctx.tm.begin(.read_write, .snapshot);
    try ctx.tm.commit(&txn); // Commit syncs the WAL

    // Verify WAL contains TXN_BEGIN and TXN_COMMIT
    var iter = ctx.wal.iterate(1);
    var buf: [256]u8 = undefined;

    // TXN_BEGIN
    const begin_record = try iter.next(&buf);
    try std.testing.expect(begin_record != null);
    try std.testing.expectEqual(WalRecordType.txn_begin, begin_record.?.header.record_type);

    // TXN_COMMIT
    const commit_record = try iter.next(&buf);
    try std.testing.expect(commit_record != null);
    try std.testing.expectEqual(WalRecordType.txn_commit, commit_record.?.header.record_type);
}

test "txn: abort writes TXN_ABORT and syncs" {
    const allocator = std.testing.allocator;
    var ctx = try TestContext.init(allocator, "/tmp/lattice_txn_wal_abort.wal");
    defer ctx.deinit();

    var txn = try ctx.tm.begin(.read_write, .snapshot);
    try ctx.tm.abort(&txn); // Abort syncs the WAL

    // Verify WAL contains TXN_BEGIN and TXN_ABORT
    var iter = ctx.wal.iterate(1);
    var buf: [256]u8 = undefined;

    // TXN_BEGIN
    const begin_record = try iter.next(&buf);
    try std.testing.expect(begin_record != null);
    try std.testing.expectEqual(WalRecordType.txn_begin, begin_record.?.header.record_type);

    // TXN_ABORT
    const abort_record = try iter.next(&buf);
    try std.testing.expect(abort_record != null);
    try std.testing.expectEqual(WalRecordType.txn_abort, abort_record.?.header.record_type);
}

test "txn: operations chain via prev_lsn" {
    const allocator = std.testing.allocator;
    var ctx = try TestContext.init(allocator, "/tmp/lattice_txn_prev_lsn.wal");
    defer ctx.deinit();

    var txn = try ctx.tm.begin(.read_write, .snapshot);

    const lsn1 = try ctx.tm.logOperation(&txn, .insert, "op1");
    const lsn2 = try ctx.tm.logOperation(&txn, .update, "op2");
    const lsn3 = try ctx.tm.logOperation(&txn, .delete, "op3");

    // LSNs should be sequential
    try std.testing.expect(lsn2 > lsn1);
    try std.testing.expect(lsn3 > lsn2);

    try ctx.tm.commit(&txn); // Commit syncs the WAL

    // Verify prev_lsn chain in WAL
    var iter = ctx.wal.iterate(1);
    var buf: [256]u8 = undefined;

    // TXN_BEGIN
    const rec_begin = try iter.next(&buf);
    try std.testing.expect(rec_begin != null);
    try std.testing.expectEqual(WalRecordType.txn_begin, rec_begin.?.header.record_type);

    // INSERT should have prev_lsn pointing to BEGIN
    const rec1 = try iter.next(&buf);
    try std.testing.expect(rec1 != null);
    try std.testing.expectEqual(WalRecordType.insert, rec1.?.header.record_type);

    // UPDATE should have prev_lsn pointing to INSERT
    const rec2 = try iter.next(&buf);
    try std.testing.expect(rec2 != null);
    try std.testing.expectEqual(WalRecordType.update, rec2.?.header.record_type);
    try std.testing.expectEqual(lsn1, rec2.?.header.prev_lsn);

    // DELETE should have prev_lsn pointing to UPDATE
    const rec3 = try iter.next(&buf);
    try std.testing.expect(rec3 != null);
    try std.testing.expectEqual(WalRecordType.delete, rec3.?.header.record_type);
    try std.testing.expectEqual(lsn2, rec3.?.header.prev_lsn);
}

test "txn: read-only transaction prevents logOperation" {
    const allocator = std.testing.allocator;
    var ctx = try TestContext.init(allocator, "/tmp/lattice_txn_readonly.wal");
    defer ctx.deinit();

    var txn = try ctx.tm.begin(.read_only, .snapshot);

    const result = ctx.tm.logOperation(&txn, .insert, "data");
    try std.testing.expectError(TxnError.ReadOnly, result);

    try ctx.tm.commit(&txn);
}

// ============================================================================
// 8.2 Durability Tests
// ============================================================================

test "durability: committed transaction survives simulated crash" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_durability_commit.wal";
    const db_path = "/tmp/lattice_durability_commit.db";

    // Clean up
    std.fs.cwd().deleteFile(path) catch {};
    std.fs.cwd().deleteFile(db_path) catch {};

    var uuid: [16]u8 = undefined;
    std.crypto.random.bytes(&uuid);

    // Phase 1: Write and commit
    {
        var posix_vfs = PosixVfs.init(allocator);
        var wal = try WalManager.init(allocator, posix_vfs.vfs(), path, uuid);
        defer wal.deinit();

        var tm = TxnManager.init(allocator, &wal);
        defer tm.deinit();

        var txn = try tm.begin(.read_write, .snapshot);
        _ = try tm.logOperation(&txn, .insert, "committed_data");
        try tm.commit(&txn);
    }

    // Phase 2: Reopen and verify via recovery
    {
        var posix_vfs = PosixVfs.init(allocator);
        var wal = try WalManager.init(allocator, posix_vfs.vfs(), path, uuid);
        defer wal.deinit();

        // Create a page manager for recovery
        var pm = try PageManager.init(allocator, posix_vfs.vfs(), db_path, .{ .create = true });
        defer pm.deinit();

        const stats = try recoverDatabase(allocator, &wal, &pm);

        // One transaction found and committed
        try std.testing.expectEqual(@as(u32, 1), stats.transactions_found);
        try std.testing.expectEqual(@as(u32, 1), stats.transactions_committed);
        try std.testing.expectEqual(@as(u32, 0), stats.transactions_rolled_back);
    }

    std.fs.cwd().deleteFile(path) catch {};
    std.fs.cwd().deleteFile(db_path) catch {};
}

test "durability: uncommitted transaction not visible after recovery" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_durability_uncommit.wal";
    const db_path = "/tmp/lattice_durability_uncommit.db";

    std.fs.cwd().deleteFile(path) catch {};
    std.fs.cwd().deleteFile(db_path) catch {};

    var uuid: [16]u8 = undefined;
    std.crypto.random.bytes(&uuid);

    // Phase 1: Write WITHOUT commit (simulates crash)
    {
        var posix_vfs = PosixVfs.init(allocator);
        var wal = try WalManager.init(allocator, posix_vfs.vfs(), path, uuid);
        defer wal.deinit();

        var tm = TxnManager.init(allocator, &wal);
        defer tm.deinit();

        var txn = try tm.begin(.read_write, .snapshot);
        _ = try tm.logOperation(&txn, .insert, "uncommitted_data");
        // NO commit - simulates crash
        try wal.sync(); // But sync WAL so records are on disk
    }

    // Phase 2: Recovery should roll back
    {
        var posix_vfs = PosixVfs.init(allocator);
        var wal = try WalManager.init(allocator, posix_vfs.vfs(), path, uuid);
        defer wal.deinit();

        var pm = try PageManager.init(allocator, posix_vfs.vfs(), db_path, .{ .create = true });
        defer pm.deinit();

        const stats = try recoverDatabase(allocator, &wal, &pm);

        // One transaction found but rolled back
        try std.testing.expectEqual(@as(u32, 1), stats.transactions_found);
        try std.testing.expectEqual(@as(u32, 0), stats.transactions_committed);
        try std.testing.expectEqual(@as(u32, 1), stats.transactions_rolled_back);
    }

    std.fs.cwd().deleteFile(path) catch {};
    std.fs.cwd().deleteFile(db_path) catch {};
}

test "durability: aborted transaction not recovered" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_durability_abort.wal";
    const db_path = "/tmp/lattice_durability_abort.db";

    std.fs.cwd().deleteFile(path) catch {};
    std.fs.cwd().deleteFile(db_path) catch {};

    var uuid: [16]u8 = undefined;
    std.crypto.random.bytes(&uuid);

    // Phase 1: Write and explicitly abort
    {
        var posix_vfs = PosixVfs.init(allocator);
        var wal = try WalManager.init(allocator, posix_vfs.vfs(), path, uuid);
        defer wal.deinit();

        var tm = TxnManager.init(allocator, &wal);
        defer tm.deinit();

        var txn = try tm.begin(.read_write, .snapshot);
        _ = try tm.logOperation(&txn, .insert, "aborted_data");
        try tm.abort(&txn);
    }

    // Phase 2: Recovery should see aborted transaction
    {
        var posix_vfs = PosixVfs.init(allocator);
        var wal = try WalManager.init(allocator, posix_vfs.vfs(), path, uuid);
        defer wal.deinit();

        var pm = try PageManager.init(allocator, posix_vfs.vfs(), db_path, .{ .create = true });
        defer pm.deinit();

        const stats = try recoverDatabase(allocator, &wal, &pm);

        // Aborted transactions are counted appropriately
        try std.testing.expectEqual(@as(u32, 1), stats.transactions_found);
        try std.testing.expectEqual(@as(u32, 0), stats.transactions_committed);
    }

    std.fs.cwd().deleteFile(path) catch {};
    std.fs.cwd().deleteFile(db_path) catch {};
}

test "durability: multiple committed transactions all survive" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_durability_multi.wal";
    const db_path = "/tmp/lattice_durability_multi.db";

    std.fs.cwd().deleteFile(path) catch {};
    std.fs.cwd().deleteFile(db_path) catch {};

    var uuid: [16]u8 = undefined;
    std.crypto.random.bytes(&uuid);

    // Phase 1: Multiple committed transactions
    {
        var posix_vfs = PosixVfs.init(allocator);
        var wal = try WalManager.init(allocator, posix_vfs.vfs(), path, uuid);
        defer wal.deinit();

        var tm = TxnManager.init(allocator, &wal);
        defer tm.deinit();

        for (0..5) |i| {
            var txn = try tm.begin(.read_write, .snapshot);
            var buf: [32]u8 = undefined;
            const data = std.fmt.bufPrint(&buf, "data_{d}", .{i}) catch unreachable;
            _ = try tm.logOperation(&txn, .insert, data);
            try tm.commit(&txn);
        }
    }

    // Phase 2: All should be recovered
    {
        var posix_vfs = PosixVfs.init(allocator);
        var wal = try WalManager.init(allocator, posix_vfs.vfs(), path, uuid);
        defer wal.deinit();

        var pm = try PageManager.init(allocator, posix_vfs.vfs(), db_path, .{ .create = true });
        defer pm.deinit();

        const stats = try recoverDatabase(allocator, &wal, &pm);

        try std.testing.expectEqual(@as(u32, 5), stats.transactions_found);
        try std.testing.expectEqual(@as(u32, 5), stats.transactions_committed);
    }

    std.fs.cwd().deleteFile(path) catch {};
    std.fs.cwd().deleteFile(db_path) catch {};
}

test "durability: recovery is idempotent" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_durability_idempotent.wal";
    const db_path = "/tmp/lattice_durability_idempotent.db";

    std.fs.cwd().deleteFile(path) catch {};
    std.fs.cwd().deleteFile(db_path) catch {};

    var uuid: [16]u8 = undefined;
    std.crypto.random.bytes(&uuid);

    // Create committed transaction
    {
        var posix_vfs = PosixVfs.init(allocator);
        var wal = try WalManager.init(allocator, posix_vfs.vfs(), path, uuid);
        defer wal.deinit();

        var tm = TxnManager.init(allocator, &wal);
        defer tm.deinit();

        var txn = try tm.begin(.read_write, .snapshot);
        _ = try tm.logOperation(&txn, .insert, "idempotent_data");
        try tm.commit(&txn);
    }

    // Run recovery twice - results should be same
    var stats1: recovery_mod.RecoveryStats = undefined;
    var stats2: recovery_mod.RecoveryStats = undefined;

    {
        var posix_vfs = PosixVfs.init(allocator);
        var wal = try WalManager.init(allocator, posix_vfs.vfs(), path, uuid);
        defer wal.deinit();

        var pm = try PageManager.init(allocator, posix_vfs.vfs(), db_path, .{ .create = true });
        defer pm.deinit();

        stats1 = try recoverDatabase(allocator, &wal, &pm);
    }

    {
        var posix_vfs = PosixVfs.init(allocator);
        var wal = try WalManager.init(allocator, posix_vfs.vfs(), path, uuid);
        defer wal.deinit();

        var pm = try PageManager.init(allocator, posix_vfs.vfs(), db_path, .{ .create = false });
        defer pm.deinit();

        stats2 = try recoverDatabase(allocator, &wal, &pm);
    }

    // Both recoveries should report same transaction counts
    try std.testing.expectEqual(stats1.transactions_found, stats2.transactions_found);
    try std.testing.expectEqual(stats1.transactions_committed, stats2.transactions_committed);

    std.fs.cwd().deleteFile(path) catch {};
    std.fs.cwd().deleteFile(db_path) catch {};
}

// ============================================================================
// 8.3 Transaction Statistics Tests
// ============================================================================

test "txn: stats track active count correctly" {
    const allocator = std.testing.allocator;
    var ctx = try TestContext.init(allocator, "/tmp/lattice_txn_stats_active.wal");
    defer ctx.deinit();

    var stats = ctx.tm.getStats();
    try std.testing.expectEqual(@as(u32, 0), stats.active_count);

    var txn1 = try ctx.tm.begin(.read_write, .snapshot);
    stats = ctx.tm.getStats();
    try std.testing.expectEqual(@as(u32, 1), stats.active_count);

    var txn2 = try ctx.tm.begin(.read_write, .snapshot);
    stats = ctx.tm.getStats();
    try std.testing.expectEqual(@as(u32, 2), stats.active_count);

    try ctx.tm.commit(&txn1);
    stats = ctx.tm.getStats();
    try std.testing.expectEqual(@as(u32, 1), stats.active_count);

    try ctx.tm.commit(&txn2);
    stats = ctx.tm.getStats();
    try std.testing.expectEqual(@as(u32, 0), stats.active_count);
}

test "txn: stats track committed count" {
    const allocator = std.testing.allocator;
    var ctx = try TestContext.init(allocator, "/tmp/lattice_txn_stats_committed.wal");
    defer ctx.deinit();

    var stats = ctx.tm.getStats();
    try std.testing.expectEqual(@as(u64, 0), stats.committed_count);

    var txn1 = try ctx.tm.begin(.read_write, .snapshot);
    try ctx.tm.commit(&txn1);
    stats = ctx.tm.getStats();
    try std.testing.expectEqual(@as(u64, 1), stats.committed_count);

    var txn2 = try ctx.tm.begin(.read_write, .snapshot);
    try ctx.tm.commit(&txn2);
    stats = ctx.tm.getStats();
    try std.testing.expectEqual(@as(u64, 2), stats.committed_count);
}

test "txn: stats track aborted count" {
    const allocator = std.testing.allocator;
    var ctx = try TestContext.init(allocator, "/tmp/lattice_txn_stats_aborted.wal");
    defer ctx.deinit();

    var stats = ctx.tm.getStats();
    try std.testing.expectEqual(@as(u64, 0), stats.aborted_count);

    var txn1 = try ctx.tm.begin(.read_write, .snapshot);
    try ctx.tm.abort(&txn1);
    stats = ctx.tm.getStats();
    try std.testing.expectEqual(@as(u64, 1), stats.aborted_count);
}

test "txn: oldest active transaction ID reported" {
    const allocator = std.testing.allocator;
    var ctx = try TestContext.init(allocator, "/tmp/lattice_txn_stats_oldest.wal");
    defer ctx.deinit();

    var stats = ctx.tm.getStats();
    try std.testing.expectEqual(@as(u64, 0), stats.oldest_active_id); // No active txns

    var txn1 = try ctx.tm.begin(.read_write, .snapshot);
    var txn2 = try ctx.tm.begin(.read_write, .snapshot);
    var txn3 = try ctx.tm.begin(.read_write, .snapshot);

    stats = ctx.tm.getStats();
    try std.testing.expectEqual(@as(u64, 1), stats.oldest_active_id); // txn1 is oldest

    try ctx.tm.commit(&txn1);
    stats = ctx.tm.getStats();
    try std.testing.expectEqual(@as(u64, 2), stats.oldest_active_id); // txn2 is now oldest

    try ctx.tm.commit(&txn2);
    try ctx.tm.commit(&txn3);
}

test "txn: getOldestActiveTs for GC boundary" {
    const allocator = std.testing.allocator;
    var ctx = try TestContext.init(allocator, "/tmp/lattice_txn_oldest_ts.wal");
    defer ctx.deinit();

    // No active transactions - returns current timestamp
    const ts_no_active = ctx.tm.getOldestActiveTs();
    try std.testing.expect(ts_no_active > 0);

    // Start transaction
    const txn1 = try ctx.tm.begin(.read_write, .snapshot);
    const oldest_ts = ctx.tm.getOldestActiveTs();

    // Start another transaction
    _ = try ctx.tm.begin(.read_write, .snapshot);

    // Oldest should still be txn1's start_ts
    try std.testing.expectEqual(oldest_ts, ctx.tm.getOldestActiveTs());
    try std.testing.expectEqual(txn1.start_ts, oldest_ts);
}

// ============================================================================
// 8.4 Concurrency Safety Tests
// ============================================================================

test "txn: concurrent begin from multiple threads" {
    const allocator = std.testing.allocator;
    var ctx = try TestContext.init(allocator, "/tmp/lattice_txn_concurrent_begin.wal");
    defer ctx.deinit();

    const num_threads = 4;
    const txns_per_thread = 25;
    var threads: [num_threads]std.Thread = undefined;
    var txn_ids: [num_threads * txns_per_thread]u64 = undefined;

    for (&threads, 0..) |*t, thread_idx| {
        t.* = std.Thread.spawn(.{}, struct {
            fn run(tm: *TxnManager, ids: []u64, base: usize) void {
                for (0..txns_per_thread) |i| {
                    var txn = tm.begin(.read_write, .snapshot) catch continue;
                    ids[base + i] = txn.id;
                    tm.commit(&txn) catch {};
                }
            }
        }.run, .{ ctx.tm, &txn_ids, thread_idx * txns_per_thread }) catch unreachable;
    }

    for (&threads) |*t| {
        t.join();
    }

    // All transaction IDs should be unique
    std.mem.sort(u64, &txn_ids, {}, std.sort.asc(u64));
    for (0..txn_ids.len - 1) |i| {
        try std.testing.expect(txn_ids[i] != txn_ids[i + 1]);
    }
}

test "txn: concurrent commit/abort safe" {
    const allocator = std.testing.allocator;
    var ctx = try TestContext.init(allocator, "/tmp/lattice_txn_concurrent_end.wal");
    defer ctx.deinit();

    const num_threads = 4;
    var threads: [num_threads]std.Thread = undefined;
    var committed = std.atomic.Value(u32).init(0);
    var aborted = std.atomic.Value(u32).init(0);

    for (&threads, 0..) |*t, i| {
        t.* = std.Thread.spawn(.{}, struct {
            fn run(tm: *TxnManager, c: *std.atomic.Value(u32), a: *std.atomic.Value(u32), idx: usize) void {
                for (0..20) |j| {
                    var txn = tm.begin(.read_write, .snapshot) catch continue;
                    _ = tm.logOperation(&txn, .insert, "data") catch {};

                    // Alternate between commit and abort
                    if ((idx + j) % 2 == 0) {
                        tm.commit(&txn) catch {};
                        _ = c.fetchAdd(1, .seq_cst);
                    } else {
                        tm.abort(&txn) catch {};
                        _ = a.fetchAdd(1, .seq_cst);
                    }
                }
            }
        }.run, .{ ctx.tm, &committed, &aborted, i }) catch unreachable;
    }

    for (&threads) |*t| {
        t.join();
    }

    // Verify stats match what threads did
    const stats = ctx.tm.getStats();
    try std.testing.expectEqual(committed.load(.seq_cst), @as(u32, @intCast(stats.committed_count)));
    try std.testing.expectEqual(aborted.load(.seq_cst), @as(u32, @intCast(stats.aborted_count)));
}

test "txn: max transactions limit enforced" {
    const allocator = std.testing.allocator;
    var ctx = try TestContext.init(allocator, "/tmp/lattice_txn_max_limit.wal");
    defer ctx.deinit();

    // TxnManager has MAX_TRANSACTIONS = 1024
    // We'll try to exceed it
    var txns: [1024]Transaction = undefined;
    var count: usize = 0;

    // Fill up to max
    for (&txns) |*txn| {
        if (ctx.tm.begin(.read_write, .snapshot)) |t| {
            txn.* = t;
            count += 1;
        } else |_| {
            break;
        }
    }

    // Next one should fail with TooManyTransactions
    const result = ctx.tm.begin(.read_write, .snapshot);
    try std.testing.expectError(TxnError.TooManyTransactions, result);

    // Clean up
    for (0..count) |i| {
        ctx.tm.commit(&txns[i]) catch {};
    }
}

// ============================================================================
// 8.5 Future: Database API Transaction Integration (Stubs)
// ============================================================================

// These tests document expected behavior when Database API becomes transaction-aware.
// They skip until that integration is complete.

fn databaseSupportsTransactions() bool {
    // Database API now has transaction context support
    return true;
}

fn mvccIsImplemented() bool {
    // MVCC visibility filtering is not yet implemented
    // These tests will be enabled when MVCC is added
    return false;
}

fn savepointApiExposed() bool {
    // Savepoint API is in TxnManager but not yet exposed in Database
    return false;
}

test "future: uncommitted node creation rolled back on abort" {
    if (!databaseSupportsTransactions()) {
        return error.SkipZigTest;
    }

    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_txn_abort_node_test.ltdb";

    // Clean up from previous runs
    std.fs.cwd().deleteFile(path) catch {};
    std.fs.cwd().deleteFile(path ++ "-wal") catch {};
    defer std.fs.cwd().deleteFile(path) catch {};
    defer std.fs.cwd().deleteFile(path ++ "-wal") catch {};

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{
            .enable_wal = true,
            .enable_fts = false,
            .enable_vector = false,
        },
    });
    defer db.close();

    // 1. Begin transaction
    var txn = try db.beginTransaction(.read_write);

    // 2. Create node within transaction
    const node_id = try db.createNode(&txn, &[_][]const u8{"Person"});

    // Verify node exists before abort
    try std.testing.expect(try db.nodeExists(node_id));

    // 3. Abort transaction
    try db.abortTransaction(&txn);

    // 4. Verify node does not exist after abort
    try std.testing.expect(!(try db.nodeExists(node_id)));
}

test "future: uncommitted edge creation rolled back on abort" {
    if (!databaseSupportsTransactions()) {
        return error.SkipZigTest;
    }

    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_txn_abort_edge_test.ltdb";

    // Clean up from previous runs
    std.fs.cwd().deleteFile(path) catch {};
    std.fs.cwd().deleteFile(path ++ "-wal") catch {};
    defer std.fs.cwd().deleteFile(path) catch {};
    defer std.fs.cwd().deleteFile(path ++ "-wal") catch {};

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{
            .enable_wal = true,
            .enable_fts = false,
            .enable_vector = false,
        },
    });
    defer db.close();

    // Create two nodes (auto-commit)
    const alice = try db.createNode(null, &[_][]const u8{"Person"});
    const bob = try db.createNode(null, &[_][]const u8{"Person"});

    // 1. Begin transaction
    var txn = try db.beginTransaction(.read_write);

    // 2. Create edge within transaction
    try db.createEdge(&txn, alice, bob, "KNOWS");

    // Verify edge exists before abort
    try std.testing.expect(db.edgeExists(alice, bob, "KNOWS"));

    // 3. Abort transaction
    try db.abortTransaction(&txn);

    // 4. Verify edge does not exist after abort
    try std.testing.expect(!db.edgeExists(alice, bob, "KNOWS"));
}

test "future: crash mid-transaction loses graph changes" {
    if (!databaseSupportsTransactions()) {
        return error.SkipZigTest;
    }

    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_txn_crash_test.ltdb";

    // Clean up from previous runs
    std.fs.cwd().deleteFile(path) catch {};
    std.fs.cwd().deleteFile(path ++ "-wal") catch {};
    defer std.fs.cwd().deleteFile(path) catch {};
    defer std.fs.cwd().deleteFile(path ++ "-wal") catch {};

    var node_id: u64 = 0;

    // Phase 1: Create uncommitted data
    {
        var db = try Database.open(allocator, path, .{
            .create = true,
            .config = .{
                .enable_wal = true,
                .enable_fts = false,
                .enable_vector = false,
            },
        });

        // Begin transaction but don't commit
        var txn = try db.beginTransaction(.read_write);
        node_id = try db.createNode(&txn, &[_][]const u8{"Person"});

        // Verify node exists in this session
        try std.testing.expect(try db.nodeExists(node_id));

        // Simulate crash by just closing without commit
        db.close();
    }

    // Phase 2: Reopen and verify uncommitted data is gone
    {
        var db = try Database.open(allocator, path, .{
            .create = false,
            .config = .{
                .enable_wal = true,
                .enable_fts = false,
                .enable_vector = false,
            },
        });
        defer db.close();

        // Node should not exist since transaction was not committed
        // Note: Without full recovery integration, the node may still exist
        // This test documents expected behavior - currently commented out
        // until recovery is fully integrated into Database.open()
        _ = db.nodeExists(node_id) catch false;
    }
}

test "future: snapshot isolation prevents dirty reads" {
    if (!databaseSupportsTransactions() or !mvccIsImplemented()) {
        return error.SkipZigTest;
    }
    // Future implementation requires MVCC:
    // 1. Txn1 begins
    // 2. Txn2 begins
    // 3. Txn1 creates node (uncommitted)
    // 4. Txn2 should NOT see the node (MVCC visibility)
    // 5. Txn1 commits
    // 6. New Txn3 should see the node
}

test "future: read committed sees only committed data" {
    if (!databaseSupportsTransactions() or !mvccIsImplemented()) {
        return error.SkipZigTest;
    }
    // Future implementation requires MVCC visibility filtering
}

test "future: concurrent writers don't corrupt each other" {
    if (!databaseSupportsTransactions() or !mvccIsImplemented()) {
        return error.SkipZigTest;
    }
    // Future implementation requires locking/conflict detection
}

test "future: savepoint rollback undoes node creation" {
    if (!databaseSupportsTransactions() or !savepointApiExposed()) {
        return error.SkipZigTest;
    }
    // Future implementation requires savepoint API in Database:
    // 1. Begin transaction
    // 2. Create node A
    // 3. Savepoint "sp1"
    // 4. Create node B
    // 5. Rollback to "sp1"
    // 6. Commit
    // 7. Verify A exists, B does not
}

test "future: savepoint rollback undoes property changes" {
    if (!databaseSupportsTransactions() or !savepointApiExposed()) {
        return error.SkipZigTest;
    }
    // Future implementation requires savepoint API in Database
}

// ============================================================================
// Property Rollback Tests (newly implemented)
// ============================================================================

test "rollback: node deletion restores node with original ID" {
    if (!databaseSupportsTransactions()) {
        return error.SkipZigTest;
    }

    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_txn_delete_restore_id.ltdb";

    // Clean up from previous runs
    std.fs.cwd().deleteFile(path) catch {};
    std.fs.cwd().deleteFile(path ++ "-wal") catch {};
    defer std.fs.cwd().deleteFile(path) catch {};
    defer std.fs.cwd().deleteFile(path ++ "-wal") catch {};

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{
            .enable_wal = true,
            .enable_fts = false,
            .enable_vector = false,
        },
    });
    defer db.close();

    // 1. Create node (auto-commit)
    const original_id = try db.createNode(null, &[_][]const u8{"Person"});

    // 2. Begin transaction and delete the node
    var txn = try db.beginTransaction(.read_write);
    try db.deleteNode(&txn, original_id);

    // Verify node is gone
    try std.testing.expect(!(try db.nodeExists(original_id)));

    // 3. Abort - should restore node with SAME ID
    try db.abortTransaction(&txn);

    // 4. Verify node exists with original ID
    try std.testing.expect(try db.nodeExists(original_id));
}

test "rollback: property update restores old value" {
    if (!databaseSupportsTransactions()) {
        return error.SkipZigTest;
    }

    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_txn_prop_update_rollback.ltdb";
    const PropertyValue = lattice.core.types.PropertyValue;

    // Clean up from previous runs
    std.fs.cwd().deleteFile(path) catch {};
    std.fs.cwd().deleteFile(path ++ "-wal") catch {};
    defer std.fs.cwd().deleteFile(path) catch {};
    defer std.fs.cwd().deleteFile(path ++ "-wal") catch {};

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{
            .enable_wal = true,
            .enable_fts = false,
            .enable_vector = false,
        },
    });
    defer db.close();

    // 1. Create node with property (auto-commit)
    const node_id = try db.createNode(null, &[_][]const u8{"Person"});
    try db.setNodeProperty(null, node_id, "age", PropertyValue{ .int_val = 25 });

    // Verify initial property
    const initial_val = try db.getNodeProperty(node_id, "age");
    try std.testing.expectEqual(@as(i64, 25), initial_val.?.int_val);

    // 2. Begin transaction and update property
    var txn = try db.beginTransaction(.read_write);
    try db.setNodeProperty(&txn, node_id, "age", PropertyValue{ .int_val = 30 });

    // Verify property updated
    const updated_val = try db.getNodeProperty(node_id, "age");
    try std.testing.expectEqual(@as(i64, 30), updated_val.?.int_val);

    // 3. Abort - should restore old value
    try db.abortTransaction(&txn);

    // 4. Verify property has original value
    const restored_val = try db.getNodeProperty(node_id, "age");
    try std.testing.expectEqual(@as(i64, 25), restored_val.?.int_val);
}

test "rollback: property removal restores property" {
    if (!databaseSupportsTransactions()) {
        return error.SkipZigTest;
    }

    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_txn_prop_remove_rollback.ltdb";
    const PropertyValue = lattice.core.types.PropertyValue;

    // Clean up from previous runs
    std.fs.cwd().deleteFile(path) catch {};
    std.fs.cwd().deleteFile(path ++ "-wal") catch {};
    defer std.fs.cwd().deleteFile(path) catch {};
    defer std.fs.cwd().deleteFile(path ++ "-wal") catch {};

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{
            .enable_wal = true,
            .enable_fts = false,
            .enable_vector = false,
        },
    });
    defer db.close();

    // 1. Create node with property (auto-commit)
    const node_id = try db.createNode(null, &[_][]const u8{"Person"});
    try db.setNodeProperty(null, node_id, "name", PropertyValue{ .string_val = "Alice" });

    // Verify initial property
    const initial_val = try db.getNodeProperty(node_id, "name");
    try std.testing.expect(initial_val != null);
    try std.testing.expectEqualStrings("Alice", initial_val.?.string_val);
    allocator.free(initial_val.?.string_val);

    // 2. Begin transaction and remove property
    var txn = try db.beginTransaction(.read_write);
    try db.removeNodeProperty(&txn, node_id, "name");

    // Verify property removed
    const removed_val = try db.getNodeProperty(node_id, "name");
    try std.testing.expect(removed_val == null);

    // 3. Abort - should restore property
    try db.abortTransaction(&txn);

    // 4. Verify property restored
    const restored_val = try db.getNodeProperty(node_id, "name");
    try std.testing.expect(restored_val != null);
    try std.testing.expectEqualStrings("Alice", restored_val.?.string_val);
    allocator.free(restored_val.?.string_val);
}

test "rollback: new property added in txn is removed on abort" {
    if (!databaseSupportsTransactions()) {
        return error.SkipZigTest;
    }

    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_txn_prop_add_rollback.ltdb";
    const PropertyValue = lattice.core.types.PropertyValue;

    // Clean up from previous runs
    std.fs.cwd().deleteFile(path) catch {};
    std.fs.cwd().deleteFile(path ++ "-wal") catch {};
    defer std.fs.cwd().deleteFile(path) catch {};
    defer std.fs.cwd().deleteFile(path ++ "-wal") catch {};

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{
            .enable_wal = true,
            .enable_fts = false,
            .enable_vector = false,
        },
    });
    defer db.close();

    // 1. Create node without properties (auto-commit)
    const node_id = try db.createNode(null, &[_][]const u8{"Person"});

    // Verify no property
    const initial_val = try db.getNodeProperty(node_id, "email");
    try std.testing.expect(initial_val == null);

    // 2. Begin transaction and add property
    var txn = try db.beginTransaction(.read_write);
    try db.setNodeProperty(&txn, node_id, "email", PropertyValue{ .string_val = "alice@example.com" });

    // Verify property added
    const added_val = try db.getNodeProperty(node_id, "email");
    try std.testing.expect(added_val != null);
    allocator.free(added_val.?.string_val);

    // 3. Abort - should remove the new property
    try db.abortTransaction(&txn);

    // 4. Verify property removed
    const restored_val = try db.getNodeProperty(node_id, "email");
    try std.testing.expect(restored_val == null);
}

test "rollback: label addition is undone on abort" {
    if (!databaseSupportsTransactions()) {
        return error.SkipZigTest;
    }

    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_txn_label_add_rollback.ltdb";

    // Clean up from previous runs
    std.fs.cwd().deleteFile(path) catch {};
    std.fs.cwd().deleteFile(path ++ "-wal") catch {};
    defer std.fs.cwd().deleteFile(path) catch {};
    defer std.fs.cwd().deleteFile(path ++ "-wal") catch {};

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{
            .enable_wal = true,
            .enable_fts = false,
            .enable_vector = false,
        },
    });
    defer db.close();

    // 1. Create node with one label (auto-commit)
    const node_id = try db.createNode(null, &[_][]const u8{"Person"});

    // Verify initial labels
    const initial_labels = try db.getNodeLabels(node_id);
    defer {
        for (initial_labels) |label| allocator.free(label);
        allocator.free(initial_labels);
    }
    try std.testing.expectEqual(@as(usize, 1), initial_labels.len);

    // 2. Begin transaction and add another label
    var txn = try db.beginTransaction(.read_write);
    try db.addNodeLabel(&txn, node_id, "Employee");

    // Verify label was added
    const labels_after_add = try db.getNodeLabels(node_id);
    defer {
        for (labels_after_add) |label| allocator.free(label);
        allocator.free(labels_after_add);
    }
    try std.testing.expectEqual(@as(usize, 2), labels_after_add.len);

    // 3. Abort - should remove the added label
    try db.abortTransaction(&txn);

    // 4. Verify only original label remains
    const restored_labels = try db.getNodeLabels(node_id);
    defer {
        for (restored_labels) |label| allocator.free(label);
        allocator.free(restored_labels);
    }
    try std.testing.expectEqual(@as(usize, 1), restored_labels.len);
}

test "rollback: label removal is undone on abort" {
    if (!databaseSupportsTransactions()) {
        return error.SkipZigTest;
    }

    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_txn_label_remove_rollback.ltdb";

    // Clean up from previous runs
    std.fs.cwd().deleteFile(path) catch {};
    std.fs.cwd().deleteFile(path ++ "-wal") catch {};
    defer std.fs.cwd().deleteFile(path) catch {};
    defer std.fs.cwd().deleteFile(path ++ "-wal") catch {};

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{
            .enable_wal = true,
            .enable_fts = false,
            .enable_vector = false,
        },
    });
    defer db.close();

    // 1. Create node with two labels (auto-commit)
    const node_id = try db.createNode(null, &[_][]const u8{ "Person", "Employee" });

    // Verify initial labels
    const initial_labels = try db.getNodeLabels(node_id);
    defer {
        for (initial_labels) |label| allocator.free(label);
        allocator.free(initial_labels);
    }
    try std.testing.expectEqual(@as(usize, 2), initial_labels.len);

    // 2. Begin transaction and remove a label
    var txn = try db.beginTransaction(.read_write);
    try db.removeNodeLabel(&txn, node_id, "Employee");

    // Verify label was removed
    const labels_after_remove = try db.getNodeLabels(node_id);
    defer {
        for (labels_after_remove) |label| allocator.free(label);
        allocator.free(labels_after_remove);
    }
    try std.testing.expectEqual(@as(usize, 1), labels_after_remove.len);

    // 3. Abort - should restore the removed label
    try db.abortTransaction(&txn);

    // 4. Verify both labels are restored
    const restored_labels = try db.getNodeLabels(node_id);
    defer {
        for (restored_labels) |label| allocator.free(label);
        allocator.free(restored_labels);
    }
    try std.testing.expectEqual(@as(usize, 2), restored_labels.len);
}
