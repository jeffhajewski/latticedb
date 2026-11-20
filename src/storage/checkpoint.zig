//! Checkpointing for Lattice database.
//!
//! Checkpoints flush dirty pages to disk and update the WAL checkpoint LSN.
//! This bounds recovery time - we only need to replay from checkpoint_lsn forward.

const std = @import("std");
const lattice = @import("lattice");

const Allocator = std.mem.Allocator;

const buffer_pool = lattice.storage.buffer_pool;
const page_manager = lattice.storage.page_manager;
const wal_mod = lattice.storage.wal;
const locking = lattice.concurrency.locking;

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
    checkpoint_mutex: @import("compat").Mutex,
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

        const start_time = @import("compat").nanoTimestamp();

        // 1. Write CHECKPOINT_BEGIN to WAL
        const begin_lsn = self.wal.appendRecord(.checkpoint_begin, 0, 0, &[_]u8{}) catch {
            return CheckpointError.WalError;
        };

        // 2. Flush WAL to ensure begin record is durable
        self.wal.sync() catch {
            return CheckpointError.WalError;
        };

        // 3. Record the coming reset in the header, so that it reaches the
        //    file in the same flush as the pages. The counter has to be
        //    durable before the log is truncated in step 8, because a
        //    follower that misses the reset would keep counting through
        //    frame numbers that no longer mean what it thinks they do.
        if (mode == .truncate) {
            self.pm.advanceCheckpointSeq() catch {
                return CheckpointError.IoError;
            };
        }

        // 4. Flush every dirty page and the header as one atomic step, which
        //    also syncs the database file. `.passive` gives way to anything
        //    holding a dirty page; it then writes nothing at all, because a
        //    flush of some pages and not others is exactly what must never
        //    reach the file, and the checkpoint position stays where it was.
        const pages_flushed: u32 = switch (mode) {
            .passive => (try self.flushPassive()) orelse {
                const skipped = CheckpointStats{
                    .pages_flushed = 0,
                    .duration_ns = @intCast(@import("compat").nanoTimestamp() - start_time),
                    .checkpoint_lsn = self.wal.getCheckpointLsn(),
                    .wal_truncated = false,
                };
                self.last_stats = skipped;
                return skipped;
            },
            .full, .truncate => try self.flushFull(),
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
            // Every dirty page is durable by this point, so losing the truncation
            // costs disk space rather than data. Report what actually happened
            // instead of what was asked for.
            if (self.truncateWal()) |_| {
                wal_truncated = true;
            } else |_| {}
        }

        const end_time = @import("compat").nanoTimestamp();

        const stats = CheckpointStats{
            .pages_flushed = pages_flushed,
            .duration_ns = @intCast(end_time - start_time),
            .checkpoint_lsn = end_lsn,
            .wal_truncated = wal_truncated,
        };

        self.last_stats = stats;
        return stats;
    }

    /// Flush without waiting: null if any dirty page is in use.
    fn flushPassive(self: *Self) CheckpointError!?u32 {
        for (self.bp.frames) |*frame| {
            if (frame.page_id != 0 and frame.dirty and frame.isPinned()) return null;
        }
        for (self.bp.overflow.items) |slot| {
            const frame = slot orelse continue;
            if (frame.page_id != 0 and frame.dirty and frame.isPinned()) return null;
        }
        return try self.flushFull();
    }

    /// Flush all dirty pages, waiting for latches if needed
    fn flushFull(self: *Self) CheckpointError!u32 {
        return self.bp.flushAll() catch return CheckpointError.IoError;
    }

    /// Truncate the WAL file (reset to header only)
    fn truncateWal(self: *Self) CheckpointError!void {
        self.wal.truncateToEmpty() catch {
            return CheckpointError.WalError;
        };
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
    /// The variable that sets the default for the C API, the bindings and
    /// the CLI: frames between automatic checkpoints, 0 for none. A frame
    /// count set this way is used as it is, not scaled to the cache.
    pub const environment_variable = "LATTICE_AUTO_CHECKPOINT_FRAMES";

    /// The policy `LATTICE_AUTO_CHECKPOINT_FRAMES` asks for: the default one
    /// with its frame count, or `.off` for 0. Null when it is unset or not a
    /// whole number.
    pub fn fromEnvironment() ?EnvironmentSetting {
        const text = std.c.getenv(environment_variable) orelse return null;
        const frames = std.fmt.parseInt(u64, std.mem.span(text), 10) catch return null;
        if (frames == 0) return .off;
        return .{ .policy = .{ .max_wal_frames = frames } };
    }

    pub const EnvironmentSetting = union(enum) {
        off,
        policy: AutoCheckpointConfig,

        pub fn get(self: EnvironmentSetting) ?AutoCheckpointConfig {
            return switch (self) {
                .off => null,
                .policy => |p| p,
            };
        }
    };

    /// Where the default interval comes from, when no one chose one.
    ///
    /// A checkpoint writes every page dirtied since the last one twice (into
    /// the journal, then in place) and syncs, so checkpointing often throws
    /// away what a large page cache could hold. A bulk load of 1M nodes and
    /// 2M relationships, 10K rows per transaction, took 12.8 s with a 1 GB
    /// cache at the old fixed 1000 frames (4 MB of log) and 9.2 s at 64 MB of
    /// log. With the 100 MB default cache the interval made no measurable
    /// difference, since the flush that runs when half the cache is dirty
    /// dominates there. So the log may grow to an eighth of the cache:
    ///
    /// - the floor is the old default, 1000 frames of 4096 bytes, so a small
    ///   cache checkpoints as often as it always did;
    /// - the cap, 64 MB, bounds the log left to replay after a crash and the
    ///   size of the `-wal` file on disk.
    pub const scaled_default = struct {
        /// The log may grow to `cache bytes / cache_divisor`, between the two
        /// bounds below.
        pub const cache_divisor: u64 = 8;
        pub const min_log_bytes: u64 = 1000 * 4096;
        pub const max_log_bytes: u64 = 64 * 1024 * 1024;
    };

    /// The frame count commits checkpoint at by default, for a page cache of
    /// `cache_bytes` and a log whose frames are `frame_size` bytes.
    pub fn defaultMaxWalFrames(cache_bytes: u64, frame_size: u32) u64 {
        const d = scaled_default;
        const log_bytes = std.math.clamp(cache_bytes / d.cache_divisor, d.min_log_bytes, d.max_log_bytes);
        return @max(1, log_bytes / @max(1, frame_size));
    }

    /// This policy with `max_wal_frames` filled in: as given when it was
    /// chosen, otherwise scaled to the page cache.
    pub fn resolved(self: AutoCheckpointConfig, cache_bytes: u64, frame_size: u32) AutoCheckpointConfig {
        var policy = self;
        if (policy.max_wal_frames == null) policy.max_wal_frames = defaultMaxWalFrames(cache_bytes, frame_size);
        return policy;
    }

    /// WAL frames written before a checkpoint is considered. Null, the
    /// default, scales it to the page cache (`defaultMaxWalFrames`): from
    /// 1000 frames for a small cache up to 64 MB of log for a cache of
    /// 512 MB or more. A number set here is used as it is.
    max_wal_frames: ?u64 = null,
    /// Shortest gap between two automatic checkpoints, in nanoseconds.
    ///
    /// Zero by default, because under `.truncate` the frame threshold already
    /// limits the rate: a checkpoint resets `frame_count`, so the next one
    /// cannot happen until another `max_wal_frames` have been written. Adding a
    /// time gate on top of that does not prevent thrash, it just lets the WAL
    /// grow without limit during a burst of writes, which is the thing this is
    /// supposed to stop.
    ///
    /// It is worth setting for modes that leave the WAL in place, where the
    /// frame count keeps climbing and nothing else would slow the loop down.
    min_interval_ns: u64 = 0,
    /// Checkpoint mode to use.
    ///
    /// Only `.truncate` resets the WAL, so it is the only mode that bounds the
    /// file's growth. `.passive` and `.full` flush pages but leave every frame in
    /// place, which is useful for durability and useless for size.
    mode: CheckpointMode = .truncate,
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
    @import("compat").randomBytes(&uuid);

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
    @import("compat").randomBytes(&uuid);

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
    var frame1_pinned = true;
    errdefer if (frame1_pinned) bp.unpinPage(frame1, true);
    frame1.dirty = true;
    // Don't unpin page1!

    // Fetch, dirty, and unpin page2
    const frame2 = try bp.fetchPage(page2, .exclusive);
    frame2.dirty = true;
    bp.unpinPage(frame2, true);

    // A passive checkpoint gives way to the pinned page. It cannot write
    // page2 on its own either: some pages and not others is a torn image.
    // So it writes nothing, and the checkpoint position does not move.
    const lsn_before = wal.getCheckpointLsn();
    const stats = try checkpointer.checkpoint(.passive);
    try std.testing.expectEqual(@as(u32, 0), stats.pages_flushed);
    try std.testing.expectEqual(lsn_before, wal.getCheckpointLsn());
    try std.testing.expectEqual(lsn_before, stats.checkpoint_lsn);

    // Now unpin page1
    bp.unpinPage(frame1, true);
    frame1_pinned = false;

    // With nothing in use, the same request flushes both pages together.
    const stats2 = try checkpointer.checkpoint(.passive);
    try std.testing.expectEqual(@as(u32, 2), stats2.pages_flushed);
    try std.testing.expect(wal.getCheckpointLsn() > lsn_before);
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
    @import("compat").randomBytes(&uuid);

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
    @import("compat").randomBytes(&uuid);

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
