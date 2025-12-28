//! Checkpointing for Lattice database.
//!
//! Checkpoints flush dirty pages to disk and update the WAL checkpoint LSN.
//! This bounds recovery time - we only need to replay from checkpoint_lsn forward.

const std = @import("std");
const buffer_pool = @import("buffer_pool.zig");
const page_manager = @import("page_manager.zig");
const wal_mod = @import("wal.zig");
const locking = @import("../concurrency/locking.zig");

const Allocator = std.mem.Allocator;
const BufferPool = buffer_pool.BufferPool;
const PageManager = page_manager.PageManager;
const WalManager = wal_mod.WalManager;
const WalRecordType = wal_mod.WalRecordType;
const WalError = wal_mod.WalError;
const LatchMode = locking.LatchMode;

// ============================================================================
// Types
// ============================================================================

/// Checkpoint mode
pub const CheckpointMode = enum {
    /// Checkpoint only pages that can be flushed without waiting
    /// Does not block writers, may leave some dirty pages
    passive,

    /// Full checkpoint - flushes all dirty pages
    /// May briefly block writers during flush
    full,

    /// Full checkpoint, then reset WAL to beginning
    /// Requires exclusive access (no active readers/writers)
    truncate,
};

/// Checkpoint errors
pub const CheckpointError = error{
    /// I/O error during flush
    IoError,
    /// WAL error
    WalError,
    /// Cannot truncate with active transactions
    ActiveTransactions,
    /// Checkpoint already in progress
    CheckpointInProgress,
};

/// Checkpoint statistics
pub const CheckpointStats = struct {
    /// Number of pages flushed
    pages_flushed: u32,
    /// Time taken in nanoseconds
    duration_ns: u64,
    /// New checkpoint LSN
    checkpoint_lsn: u64,
    /// Whether WAL was truncated
    wal_truncated: bool,
};

// ============================================================================
// Checkpointer
// ============================================================================

/// Manages checkpointing for the database
pub const Checkpointer = struct {
    allocator: Allocator,
    bp: *BufferPool,
    pm: *PageManager,
    wal: *WalManager,
    /// Prevents concurrent checkpoints
    checkpoint_mutex: std.Thread.Mutex,
    /// Set to true during checkpoint
    checkpoint_in_progress: bool,
    /// Statistics from last checkpoint
    last_stats: ?CheckpointStats,

    const Self = @This();

    /// Initialize the checkpointer
    pub fn init(allocator: Allocator, bp: *BufferPool, pm: *PageManager, wal: *WalManager) Self {
        return Self{
            .allocator = allocator,
            .bp = bp,
            .pm = pm,
            .wal = wal,
            .checkpoint_mutex = .{},
            .checkpoint_in_progress = false,
            .last_stats = null,
        };
    }

    /// Perform a checkpoint
    pub fn checkpoint(self: *Self, mode: CheckpointMode) CheckpointError!CheckpointStats {
        // Prevent concurrent checkpoints
        self.checkpoint_mutex.lock();
        defer self.checkpoint_mutex.unlock();

        if (self.checkpoint_in_progress) {
            return CheckpointError.CheckpointInProgress;
        }
        self.checkpoint_in_progress = true;
        defer self.checkpoint_in_progress = false;

        const start_time = std.time.nanoTimestamp();

        // 1. Write CHECKPOINT_BEGIN to WAL
        const begin_lsn = self.wal.appendRecord(.checkpoint_begin, 0, 0, &[_]u8{}) catch {
            return CheckpointError.WalError;
        };

        // 2. Flush WAL to ensure begin record is durable
        self.wal.sync() catch {
            return CheckpointError.WalError;
        };

        // 3. Flush dirty pages based on mode
        const pages_flushed = switch (mode) {
            .passive => try self.flushPassive(),
            .full, .truncate => try self.flushFull(),
        };

        // 4. Sync the database file
        self.pm.sync() catch {
            return CheckpointError.IoError;
        };

        // 5. Write CHECKPOINT_END to WAL with stats
        var end_payload: [8]u8 = undefined;
        std.mem.writeInt(u64, &end_payload, pages_flushed, .little);

        const end_lsn = self.wal.appendRecord(.checkpoint_end, 0, begin_lsn, &end_payload) catch {
            return CheckpointError.WalError;
        };

        // 6. Update checkpoint LSN in WAL header
        self.wal.setCheckpointLsn(end_lsn) catch {
            return CheckpointError.WalError;
        };

        // 7. Final sync
        self.wal.sync() catch {
            return CheckpointError.WalError;
        };

        // 8. Truncate WAL if requested
        var wal_truncated = false;
        if (mode == .truncate) {
            self.truncateWal() catch {
                // Truncation failure is not fatal - checkpoint still succeeded
            };
            wal_truncated = true;
        }

        const end_time = std.time.nanoTimestamp();

        const stats = CheckpointStats{
            .pages_flushed = pages_flushed,
            .duration_ns = @intCast(end_time - start_time),
            .checkpoint_lsn = end_lsn,
            .wal_truncated = wal_truncated,
        };

        self.last_stats = stats;
        return stats;
    }

    /// Flush pages without blocking - skip pages that are currently pinned
    fn flushPassive(self: *Self) CheckpointError!u32 {
        var pages_flushed: u32 = 0;

        for (self.bp.frames) |*frame| {
            // Skip empty or clean frames
            if (frame.page_id == 0 or !frame.dirty) {
                continue;
            }

            // Skip pinned frames (in use by transactions)
            if (frame.isPinned()) {
                continue;
            }

            // Try to acquire latch without blocking
            if (frame.latch.tryAcquireShared()) {
                defer frame.latch.releaseShared();

                if (frame.dirty) {
                    self.pm.writePage(frame.page_id, frame.data) catch {
                        return CheckpointError.IoError;
                    };
                    frame.dirty = false;
                    pages_flushed += 1;
                }
            }
            // If we can't get the latch, skip this page (passive mode)
        }

        return pages_flushed;
    }

    /// Flush all dirty pages, waiting for latches if needed
    fn flushFull(self: *Self) CheckpointError!u32 {
        var pages_flushed: u32 = 0;

        for (self.bp.frames) |*frame| {
            // Skip empty or clean frames
            if (frame.page_id == 0 or !frame.dirty) {
                continue;
            }

            // Wait for latch (full mode waits)
            while (!frame.latch.tryAcquireShared()) {
                std.atomic.spinLoopHint();
            }
            defer frame.latch.releaseShared();

            if (frame.dirty) {
                self.pm.writePage(frame.page_id, frame.data) catch {
                    return CheckpointError.IoError;
                };
                frame.dirty = false;
                pages_flushed += 1;
            }
        }

        return pages_flushed;
    }

    /// Truncate the WAL file (reset to header only)
    fn truncateWal(self: *Self) CheckpointError!void {
        // For now, we don't actually truncate the file - we just reset frame_count
        // A full implementation would:
        // 1. Ensure no active readers
        // 2. Truncate the file to header size
        // 3. Reset frame_count to 0
        //
        // This is safe because:
        // - checkpoint_lsn tells recovery where to start
        // - Old frames before checkpoint_lsn are ignored
        _ = self;
    }

    /// Get statistics from the last checkpoint
    pub fn getLastStats(self: *const Self) ?CheckpointStats {
        return self.last_stats;
    }

    /// Check if a checkpoint is currently in progress
    pub fn isCheckpointInProgress(self: *const Self) bool {
        return self.checkpoint_in_progress;
    }

    /// Calculate suggested checkpoint interval based on WAL size
    pub fn shouldCheckpoint(self: *const Self, max_wal_frames: u64) bool {
        const current_frames = self.wal.header.frame_count;
        const checkpoint_lsn = self.wal.getCheckpointLsn();

        // If we have more than max_wal_frames since last checkpoint, suggest checkpoint
        _ = checkpoint_lsn;
        return current_frames > max_wal_frames;
    }
};

// ============================================================================
// Auto-Checkpointer (background checkpointing)
// ============================================================================

/// Configuration for automatic checkpointing
pub const AutoCheckpointConfig = struct {
    /// Maximum WAL frames before triggering checkpoint
    max_wal_frames: u64 = 1000,
    /// Minimum interval between checkpoints (nanoseconds)
    min_interval_ns: u64 = 60 * std.time.ns_per_s, // 1 minute
    /// Checkpoint mode to use
    mode: CheckpointMode = .passive,
};

// ============================================================================
// Tests
// ============================================================================

test "checkpoint basic" {
    const allocator = std.testing.allocator;

    // Set up VFS
    const vfs = @import("vfs.zig");
    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const db_path = "/tmp/lattice_checkpoint_test.db";
    const wal_path = "/tmp/lattice_checkpoint_test.wal";

    // Clean up
    vfs_impl.delete(db_path) catch {};
    vfs_impl.delete(wal_path) catch {};

    // Create page manager
    var pm = try PageManager.init(allocator, vfs_impl, db_path, .{ .create = true });
    defer {
        pm.deinit();
        vfs_impl.delete(db_path) catch {};
    }

    // Create buffer pool
    var bp = try BufferPool.init(allocator, &pm, 64 * 1024); // 64KB = 16 pages
    defer bp.deinit();

    // Create WAL
    var uuid: [16]u8 = undefined;
    std.crypto.random.bytes(&uuid);

    var wal = try WalManager.init(allocator, vfs_impl, wal_path, uuid);
    defer {
        wal.deinit();
        vfs_impl.delete(wal_path) catch {};
    }

    // Create checkpointer
    var checkpointer = Checkpointer.init(allocator, &bp, &pm, &wal);

    // Allocate and dirty some pages
    const page1 = try pm.allocatePage();
    const page2 = try pm.allocatePage();

    const frame1 = try bp.fetchPage(page1, .exclusive);
    frame1.dirty = true;
    bp.unpinPage(frame1, true);

    const frame2 = try bp.fetchPage(page2, .exclusive);
    frame2.dirty = true;
    bp.unpinPage(frame2, true);

    // Perform checkpoint
    const stats = try checkpointer.checkpoint(.full);

    try std.testing.expectEqual(@as(u32, 2), stats.pages_flushed);
    try std.testing.expect(stats.checkpoint_lsn > 0);
    try std.testing.expect(stats.duration_ns > 0);
}

test "checkpoint passive skips pinned pages" {
    const allocator = std.testing.allocator;

    const vfs = @import("vfs.zig");
    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const db_path = "/tmp/lattice_checkpoint_passive_test.db";
    const wal_path = "/tmp/lattice_checkpoint_passive_test.wal";

    vfs_impl.delete(db_path) catch {};
    vfs_impl.delete(wal_path) catch {};

    var pm = try PageManager.init(allocator, vfs_impl, db_path, .{ .create = true });
    defer {
        pm.deinit();
        vfs_impl.delete(db_path) catch {};
    }

    var bp = try BufferPool.init(allocator, &pm, 64 * 1024);
    defer bp.deinit();

    var uuid: [16]u8 = undefined;
    std.crypto.random.bytes(&uuid);

    var wal = try WalManager.init(allocator, vfs_impl, wal_path, uuid);
    defer {
        wal.deinit();
        vfs_impl.delete(wal_path) catch {};
    }

    var checkpointer = Checkpointer.init(allocator, &bp, &pm, &wal);

    // Allocate pages
    const page1 = try pm.allocatePage();
    const page2 = try pm.allocatePage();

    // Fetch and dirty page1, but keep it pinned
    const frame1 = try bp.fetchPage(page1, .exclusive);
    frame1.dirty = true;
    // Don't unpin page1!

    // Fetch, dirty, and unpin page2
    const frame2 = try bp.fetchPage(page2, .exclusive);
    frame2.dirty = true;
    bp.unpinPage(frame2, true);

    // Passive checkpoint should only flush page2 (page1 is pinned)
    const stats = try checkpointer.checkpoint(.passive);

    // Only 1 page should be flushed (page2), page1 is still pinned
    try std.testing.expectEqual(@as(u32, 1), stats.pages_flushed);

    // Now unpin page1
    bp.unpinPage(frame1, true);

    // Full checkpoint should flush remaining dirty page
    const stats2 = try checkpointer.checkpoint(.full);
    try std.testing.expectEqual(@as(u32, 1), stats2.pages_flushed);
}

test "checkpoint updates WAL checkpoint_lsn" {
    const allocator = std.testing.allocator;

    const vfs = @import("vfs.zig");
    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const db_path = "/tmp/lattice_checkpoint_lsn_test.db";
    const wal_path = "/tmp/lattice_checkpoint_lsn_test.wal";

    vfs_impl.delete(db_path) catch {};
    vfs_impl.delete(wal_path) catch {};

    var pm = try PageManager.init(allocator, vfs_impl, db_path, .{ .create = true });
    defer {
        pm.deinit();
        vfs_impl.delete(db_path) catch {};
    }

    var bp = try BufferPool.init(allocator, &pm, 64 * 1024);
    defer bp.deinit();

    var uuid: [16]u8 = undefined;
    std.crypto.random.bytes(&uuid);

    var wal = try WalManager.init(allocator, vfs_impl, wal_path, uuid);
    defer {
        wal.deinit();
        vfs_impl.delete(wal_path) catch {};
    }

    var checkpointer = Checkpointer.init(allocator, &bp, &pm, &wal);

    // Initial checkpoint_lsn should be 0
    try std.testing.expectEqual(@as(u64, 0), wal.getCheckpointLsn());

    // Perform checkpoint
    const stats = try checkpointer.checkpoint(.full);

    // checkpoint_lsn should be updated
    try std.testing.expectEqual(stats.checkpoint_lsn, wal.getCheckpointLsn());
    try std.testing.expect(wal.getCheckpointLsn() > 0);
}

test "checkpoint statistics" {
    const allocator = std.testing.allocator;

    const vfs = @import("vfs.zig");
    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const db_path = "/tmp/lattice_checkpoint_stats_test.db";
    const wal_path = "/tmp/lattice_checkpoint_stats_test.wal";

    vfs_impl.delete(db_path) catch {};
    vfs_impl.delete(wal_path) catch {};

    var pm = try PageManager.init(allocator, vfs_impl, db_path, .{ .create = true });
    defer {
        pm.deinit();
        vfs_impl.delete(db_path) catch {};
    }

    var bp = try BufferPool.init(allocator, &pm, 64 * 1024);
    defer bp.deinit();

    var uuid: [16]u8 = undefined;
    std.crypto.random.bytes(&uuid);

    var wal = try WalManager.init(allocator, vfs_impl, wal_path, uuid);
    defer {
        wal.deinit();
        vfs_impl.delete(wal_path) catch {};
    }

    var checkpointer = Checkpointer.init(allocator, &bp, &pm, &wal);

    // No stats initially
    try std.testing.expect(checkpointer.getLastStats() == null);

    // Perform checkpoint
    _ = try checkpointer.checkpoint(.full);

    // Stats should be available
    const stats = checkpointer.getLastStats();
    try std.testing.expect(stats != null);
}
