//! Buffer Pool for Lattice database.
//!
//! Caches pages in memory with pin/unpin reference counting,
//! dirty page tracking, and Clock eviction algorithm.

const std = @import("std");
const page_manager = @import("page_manager.zig");
const page = @import("page.zig");
const locking = @import("../concurrency/locking.zig");
const types = @import("../core/types.zig");

const Allocator = std.mem.Allocator;
const Alignment = std.mem.Alignment;
const PageManager = page_manager.PageManager;
const PageManagerError = page_manager.PageManagerError;
const PageHeader = page.PageHeader;
const RwLatch = locking.RwLatch;
const LatchMode = locking.LatchMode;
const PageId = types.PageId;
const NULL_PAGE = types.NULL_PAGE;

/// Frame identifier within the buffer pool
pub const FrameId = u32;

/// Invalid frame marker
pub const INVALID_FRAME: FrameId = std.math.maxInt(FrameId);

/// Buffer Pool errors
pub const BufferPoolError = error{
    BufferPoolFull,
    PageNotFound,
    IoError,
    InvalidPageId,
    ChecksumMismatch,
};

/// A single frame in the buffer pool
pub const BufferFrame = struct {
    /// The page ID currently stored in this frame (0 if empty)
    page_id: PageId,
    /// Page data buffer (4KB aligned)
    data: []align(4096) u8,
    /// Reference count - page cannot be evicted while pinned
    pin_count: std.atomic.Value(u32),
    /// Whether the page has been modified
    dirty: bool,
    /// Usage count for Clock algorithm (second chance)
    usage_count: u8,
    /// Reader-writer latch for thread-safe access
    latch: RwLatch,

    /// Check if frame is in use (has a page loaded)
    pub fn isInUse(self: *const BufferFrame) bool {
        return self.page_id != NULL_PAGE;
    }

    /// Check if frame is pinned
    pub fn isPinned(self: *const BufferFrame) bool {
        return self.pin_count.load(.monotonic) > 0;
    }
};

/// Buffer Pool - caches database pages in memory
pub const BufferPool = struct {
    /// All frames in the pool
    frames: []BufferFrame,
    /// Maps page IDs to frame IDs for quick lookup
    page_table: std.AutoHashMap(PageId, FrameId),
    /// List of free (unused) frame IDs
    free_list: std.ArrayList(FrameId),
    /// Clock hand position for eviction algorithm
    clock_hand: usize,
    /// Underlying page manager for disk I/O
    pm: *PageManager,
    /// Allocator for dynamic memory
    allocator: Allocator,
    /// Mutex protecting page_table and free_list
    mutex: std.Thread.Mutex,
    /// Number of frames in the pool
    frame_count: usize,

    const Self = @This();

    /// Initialize a buffer pool with the given size in bytes.
    /// The pool will contain (pool_size / page_size) frames.
    pub fn init(allocator: Allocator, pm: *PageManager, pool_size: usize) !Self {
        const page_size = pm.getPageSize();
        const frame_count = pool_size / page_size;

        if (frame_count == 0) {
            return BufferPoolError.BufferPoolFull;
        }

        var frames = try allocator.alloc(BufferFrame, frame_count);
        errdefer allocator.free(frames);

        var free_list: std.ArrayList(FrameId) = .{};
        errdefer free_list.deinit(allocator);

        // Initialize all frames
        var initialized_count: usize = 0;
        errdefer {
            for (frames[0..initialized_count]) |*frame| {
                allocator.free(frame.data);
            }
        }

        // 4096-byte alignment for page I/O
        const page_alignment = comptime Alignment.fromByteUnits(4096);

        for (frames, 0..) |*frame, i| {
            const data = try allocator.alignedAlloc(u8, page_alignment, page_size);
            frame.* = BufferFrame{
                .page_id = NULL_PAGE,
                .data = data,
                .pin_count = std.atomic.Value(u32).init(0),
                .dirty = false,
                .usage_count = 0,
                .latch = RwLatch{},
            };
            try free_list.append(allocator, @intCast(i));
            initialized_count += 1;
        }

        return Self{
            .frames = frames,
            .page_table = std.AutoHashMap(PageId, FrameId).init(allocator),
            .free_list = free_list,
            .clock_hand = 0,
            .pm = pm,
            .allocator = allocator,
            .mutex = .{},
            .frame_count = frame_count,
        };
    }

    /// Clean up the buffer pool, flushing dirty pages.
    pub fn deinit(self: *Self) void {
        // Flush all dirty pages before cleanup
        self.flushAll() catch {};

        // Free all frame data buffers
        for (self.frames) |*frame| {
            self.allocator.free(frame.data);
        }

        self.allocator.free(self.frames);
        self.page_table.deinit();
        self.free_list.deinit(self.allocator);
    }

    /// Fetch a page into the buffer pool and pin it.
    /// The page is latched according to the specified mode.
    /// Caller must call unpinPage when done with the page.
    pub fn fetchPage(self: *Self, page_id: PageId, mode: LatchMode) !*BufferFrame {
        if (page_id == NULL_PAGE) {
            return BufferPoolError.InvalidPageId;
        }

        self.mutex.lock();

        // 1. Check if page is already in buffer pool
        if (self.page_table.get(page_id)) |frame_id| {
            const frame = &self.frames[frame_id];

            // Increment pin count
            _ = frame.pin_count.fetchAdd(1, .monotonic);

            // Set usage count for Clock algorithm
            frame.usage_count = 1;

            self.mutex.unlock();

            // Acquire latch based on mode (outside mutex to avoid deadlock)
            self.acquireLatch(frame, mode);

            return frame;
        }

        // 2. Get a free frame (from free list or evict)
        const frame_id = self.getFreeFrame() catch |err| {
            self.mutex.unlock();
            return err;
        };
        const frame = &self.frames[frame_id];

        // 3. Update metadata before unlocking
        frame.page_id = page_id;
        frame.dirty = false;
        frame.usage_count = 1;
        _ = frame.pin_count.fetchAdd(1, .monotonic);
        self.page_table.put(page_id, frame_id) catch {
            self.mutex.unlock();
            return BufferPoolError.IoError;
        };

        self.mutex.unlock();

        // 4. Read page from disk (outside mutex)
        self.pm.readPage(page_id, frame.data) catch |err| {
            // On read failure, clean up
            self.mutex.lock();
            _ = self.page_table.remove(page_id);
            frame.page_id = NULL_PAGE;
            _ = frame.pin_count.fetchSub(1, .monotonic);
            self.free_list.append(self.allocator, frame_id) catch {};
            self.mutex.unlock();

            return switch (err) {
                PageManagerError.ChecksumMismatch => BufferPoolError.ChecksumMismatch,
                else => BufferPoolError.IoError,
            };
        };

        // 5. Acquire latch
        self.acquireLatch(frame, mode);

        return frame;
    }

    /// Unpin a page and optionally mark it dirty.
    /// Releases the latch held on the frame.
    pub fn unpinPage(self: *Self, frame: *BufferFrame, dirty: bool) void {
        if (dirty) {
            frame.dirty = true;
        }

        // Release latch (we don't know which mode, but release works for both)
        // The caller is responsible for knowing which mode they acquired
        // For simplicity, we release based on current state
        self.releaseLatch(frame);

        // Decrement pin count
        const prev = frame.pin_count.fetchSub(1, .monotonic);
        std.debug.assert(prev > 0);
    }

    /// Flush a specific page to disk if it's dirty.
    pub fn flushPage(self: *Self, page_id: PageId) !void {
        self.mutex.lock();
        const frame_id = self.page_table.get(page_id) orelse {
            self.mutex.unlock();
            return; // Page not in buffer pool
        };
        self.mutex.unlock();

        const frame = &self.frames[frame_id];

        // Acquire shared latch for reading
        while (!frame.latch.tryAcquireShared()) {
            std.atomic.spinLoopHint();
        }
        defer frame.latch.releaseShared();

        if (frame.dirty) {
            self.pm.writePage(frame.page_id, frame.data) catch {
                return BufferPoolError.IoError;
            };
            frame.dirty = false;
        }
    }

    /// Flush all dirty pages to disk.
    pub fn flushAll(self: *Self) !void {
        for (self.frames) |*frame| {
            if (frame.page_id != NULL_PAGE and frame.dirty) {
                // Acquire shared latch for reading
                while (!frame.latch.tryAcquireShared()) {
                    std.atomic.spinLoopHint();
                }
                defer frame.latch.releaseShared();

                if (frame.dirty) {
                    self.pm.writePage(frame.page_id, frame.data) catch {
                        return BufferPoolError.IoError;
                    };
                    frame.dirty = false;
                }
            }
        }
    }

    /// Get a page directly without pinning (for internal use).
    /// Returns null if page is not in the buffer pool.
    pub fn getPageIfPresent(self: *Self, page_id: PageId) ?*BufferFrame {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.page_table.get(page_id)) |frame_id| {
            return &self.frames[frame_id];
        }
        return null;
    }

    /// Get statistics about the buffer pool.
    pub fn getStats(self: *Self) BufferPoolStats {
        self.mutex.lock();
        defer self.mutex.unlock();

        var stats = BufferPoolStats{
            .total_frames = self.frame_count,
            .free_frames = self.free_list.items.len,
            .pinned_frames = 0,
            .dirty_frames = 0,
        };

        for (self.frames) |*frame| {
            if (frame.isPinned()) {
                stats.pinned_frames += 1;
            }
            if (frame.dirty) {
                stats.dirty_frames += 1;
            }
        }

        return stats;
    }

    // ========================================================================
    // Internal methods
    // ========================================================================

    /// Get a free frame, either from free list or by eviction.
    /// Caller must hold mutex.
    fn getFreeFrame(self: *Self) BufferPoolError!FrameId {
        // Try free list first
        if (self.free_list.pop()) |frame_id| {
            return frame_id;
        }

        // Clock eviction algorithm
        const start = self.clock_hand;
        var iterations: usize = 0;
        const max_iterations = self.frame_count * 2; // Allow two full rotations

        while (iterations < max_iterations) {
            const frame = &self.frames[self.clock_hand];
            const frame_id: FrameId = @intCast(self.clock_hand);

            // Skip if pinned
            if (frame.pin_count.load(.monotonic) > 0) {
                self.advanceClock();
                iterations += 1;

                // Check if we've gone full circle with all pinned
                if (self.clock_hand == start and iterations >= self.frame_count) {
                    return BufferPoolError.BufferPoolFull;
                }
                continue;
            }

            // Second chance: check usage count
            if (frame.usage_count > 0) {
                frame.usage_count -= 1;
                self.advanceClock();
                iterations += 1;
                continue;
            }

            // Found victim - evict it
            if (frame.dirty) {
                // Flush dirty page before eviction
                self.pm.writePage(frame.page_id, frame.data) catch {
                    return BufferPoolError.IoError;
                };
            }

            // Remove from page table
            _ = self.page_table.remove(frame.page_id);

            // Reset frame state
            frame.page_id = NULL_PAGE;
            frame.dirty = false;

            self.advanceClock();
            return frame_id;
        }

        return BufferPoolError.BufferPoolFull;
    }

    /// Advance the clock hand to the next frame.
    fn advanceClock(self: *Self) void {
        self.clock_hand = (self.clock_hand + 1) % self.frame_count;
    }

    /// Acquire latch based on mode.
    fn acquireLatch(self: *Self, frame: *BufferFrame, mode: LatchMode) void {
        _ = self;
        switch (mode) {
            .none => {},
            .shared => {
                while (!frame.latch.tryAcquireShared()) {
                    std.atomic.spinLoopHint();
                }
            },
            .exclusive => {
                while (!frame.latch.tryAcquireExclusive()) {
                    std.atomic.spinLoopHint();
                }
            },
        }
    }

    /// Release latch (tries exclusive first, then shared).
    fn releaseLatch(self: *Self, frame: *BufferFrame) void {
        _ = self;
        // We need to know which type of latch was held
        // Since RwLatch tracks state, check if writer bit is set
        const state = frame.latch.state.load(.monotonic);
        const WRITER_BIT: u32 = 1 << 31;
        if (state & WRITER_BIT != 0) {
            frame.latch.releaseExclusive();
        } else if (state > 0) {
            frame.latch.releaseShared();
        }
        // If state is 0, no latch was held (LatchMode.none)
    }
};

/// Buffer pool statistics
pub const BufferPoolStats = struct {
    total_frames: usize,
    free_frames: usize,
    pinned_frames: usize,
    dirty_frames: usize,
};

// ============================================================================
// Tests
// ============================================================================

test "buffer pool init and deinit" {
    const allocator = std.testing.allocator;

    const vfs = @import("vfs.zig");
    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const path = "/tmp/lattice_bp_test_init.db";
    vfs_impl.delete(path) catch {};

    var pm = try PageManager.init(allocator, vfs_impl, path, .{ .create = true });
    defer {
        pm.deinit();
        vfs_impl.delete(path) catch {};
    }

    // Create buffer pool with 16KB (4 frames)
    var bp = try BufferPool.init(allocator, &pm, 16384);
    defer bp.deinit();

    const stats = bp.getStats();
    try std.testing.expectEqual(@as(usize, 4), stats.total_frames);
    try std.testing.expectEqual(@as(usize, 4), stats.free_frames);
}

test "buffer pool fetch and unpin" {
    const allocator = std.testing.allocator;

    const vfs = @import("vfs.zig");
    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const path = "/tmp/lattice_bp_test_fetch.db";
    vfs_impl.delete(path) catch {};

    var pm = try PageManager.init(allocator, vfs_impl, path, .{ .create = true });
    defer {
        pm.deinit();
        vfs_impl.delete(path) catch {};
    }

    // Allocate a page to fetch
    const page_id = try pm.allocatePage();

    var bp = try BufferPool.init(allocator, &pm, 16384);
    defer bp.deinit();

    // Fetch the page
    const frame = try bp.fetchPage(page_id, .exclusive);
    try std.testing.expect(frame.isPinned());
    try std.testing.expectEqual(page_id, frame.page_id);

    // Unpin without marking dirty
    bp.unpinPage(frame, false);
    try std.testing.expect(!frame.isPinned());
}

test "buffer pool dirty page tracking" {
    const allocator = std.testing.allocator;

    const vfs = @import("vfs.zig");
    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const path = "/tmp/lattice_bp_test_dirty.db";
    vfs_impl.delete(path) catch {};

    var pm = try PageManager.init(allocator, vfs_impl, path, .{ .create = true });
    defer {
        pm.deinit();
        vfs_impl.delete(path) catch {};
    }

    const page_id = try pm.allocatePage();

    var bp = try BufferPool.init(allocator, &pm, 16384);
    defer bp.deinit();

    // Fetch and modify
    const frame = try bp.fetchPage(page_id, .exclusive);

    // Write some data
    const test_data = "Hello, Buffer Pool!";
    @memcpy(frame.data[8..][0..test_data.len], test_data);

    // Unpin as dirty
    bp.unpinPage(frame, true);

    var stats = bp.getStats();
    try std.testing.expectEqual(@as(usize, 1), stats.dirty_frames);

    // Flush
    try bp.flushPage(page_id);

    stats = bp.getStats();
    try std.testing.expectEqual(@as(usize, 0), stats.dirty_frames);
}

test "buffer pool eviction" {
    const allocator = std.testing.allocator;

    const vfs = @import("vfs.zig");
    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const path = "/tmp/lattice_bp_test_evict.db";
    vfs_impl.delete(path) catch {};

    var pm = try PageManager.init(allocator, vfs_impl, path, .{ .create = true });
    defer {
        pm.deinit();
        vfs_impl.delete(path) catch {};
    }

    // Create pool with only 2 frames
    var bp = try BufferPool.init(allocator, &pm, 8192);
    defer bp.deinit();

    // Allocate 3 pages
    const page1 = try pm.allocatePage();
    const page2 = try pm.allocatePage();
    const page3 = try pm.allocatePage();

    // Fetch page1, unpin it
    const frame1 = try bp.fetchPage(page1, .shared);
    bp.unpinPage(frame1, false);

    // Fetch page2, unpin it
    const frame2 = try bp.fetchPage(page2, .shared);
    bp.unpinPage(frame2, false);

    // Now pool is full, fetching page3 should evict one of the previous
    const frame3 = try bp.fetchPage(page3, .shared);
    try std.testing.expectEqual(page3, frame3.page_id);
    bp.unpinPage(frame3, false);

    // One of page1 or page2 should have been evicted
    const stats = bp.getStats();
    try std.testing.expectEqual(@as(usize, 0), stats.free_frames);
}

test "buffer pool full error" {
    const allocator = std.testing.allocator;

    const vfs = @import("vfs.zig");
    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const path = "/tmp/lattice_bp_test_full.db";
    vfs_impl.delete(path) catch {};

    var pm = try PageManager.init(allocator, vfs_impl, path, .{ .create = true });
    defer {
        pm.deinit();
        vfs_impl.delete(path) catch {};
    }

    // Create pool with only 2 frames
    var bp = try BufferPool.init(allocator, &pm, 8192);
    defer bp.deinit();

    const page1 = try pm.allocatePage();
    const page2 = try pm.allocatePage();
    const page3 = try pm.allocatePage();

    // Fetch and keep pinned
    _ = try bp.fetchPage(page1, .shared);
    _ = try bp.fetchPage(page2, .shared);

    // Pool is full with all pinned - should fail
    const result = bp.fetchPage(page3, .shared);
    try std.testing.expectError(BufferPoolError.BufferPoolFull, result);
}

test "buffer pool page caching" {
    const allocator = std.testing.allocator;

    const vfs = @import("vfs.zig");
    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const path = "/tmp/lattice_bp_test_cache.db";
    vfs_impl.delete(path) catch {};

    var pm = try PageManager.init(allocator, vfs_impl, path, .{ .create = true });
    defer {
        pm.deinit();
        vfs_impl.delete(path) catch {};
    }

    var bp = try BufferPool.init(allocator, &pm, 16384);
    defer bp.deinit();

    const page_id = try pm.allocatePage();

    // First fetch
    const frame1 = try bp.fetchPage(page_id, .exclusive);
    const frame_ptr = frame1;
    bp.unpinPage(frame1, false);

    // Second fetch should return same frame (cached)
    const frame2 = try bp.fetchPage(page_id, .shared);
    try std.testing.expectEqual(frame_ptr, frame2);
    bp.unpinPage(frame2, false);
}
