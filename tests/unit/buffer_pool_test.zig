//! Behavioral tests for Buffer Pool.
//!
//! These tests verify the memory management contracts of the buffer pool.

const std = @import("std");
const lattice = @import("lattice");

const buffer_pool = lattice.storage.buffer_pool;
const page_manager = lattice.storage.page_manager;
const vfs = lattice.storage.vfs;
const locking = lattice.concurrency.locking;

const BufferPool = buffer_pool.BufferPool;
const BufferPoolError = buffer_pool.BufferPoolError;
const PageManager = page_manager.PageManager;
const PosixVfs = vfs.PosixVfs;
const LatchMode = locking.LatchMode;

fn createTempPath(allocator: std.mem.Allocator, name: []const u8) ![]u8 {
    const timestamp = std.time.milliTimestamp();
    const random = std.crypto.random.int(u32);
    var buf: [128]u8 = undefined;
    const path = try std.fmt.bufPrint(&buf, "/tmp/lattice_bp_test_{s}_{d}_{x}.db", .{ name, timestamp, random });
    return allocator.dupe(u8, path);
}

// ============================================================================
// Contract: Pinned pages are never evicted
// ============================================================================

test "buffer_pool: pinned page retained while pinned" {
    const allocator = std.testing.allocator;

    var posix_vfs = PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const path = try createTempPath(allocator, "pinned");
    defer allocator.free(path);
    defer vfs_impl.delete(path) catch {};

    var pm = try PageManager.init(allocator, vfs_impl, path, .{ .create = true });
    defer pm.deinit();

    // Large buffer pool to avoid triggering eviction
    var bp = try BufferPool.init(allocator, &pm, 65536);
    defer bp.deinit();

    // Allocate and pin a page
    const page1 = pm.allocatePage() catch unreachable;
    const frame1 = try bp.fetchPage(page1, .exclusive);

    // Write distinctive data
    @memset(frame1.data, 0xAA);

    // Verify page is pinned
    try std.testing.expect(frame1.isPinned());

    // Fetch more pages while frame1 is still pinned
    const page2 = pm.allocatePage() catch unreachable;
    const f2 = try bp.fetchPage(page2, .exclusive);
    bp.unpinPage(f2, false);

    // Verify frame1 still has our data
    try std.testing.expectEqual(@as(u8, 0xAA), frame1.data[0]);
    try std.testing.expectEqual(page1, frame1.page_id);
    try std.testing.expect(frame1.isPinned());

    // Now unpin
    bp.unpinPage(frame1, true);
    try std.testing.expect(!frame1.isPinned());
}

// ============================================================================
// Contract: Pin count tracks correctly
// ============================================================================

test "buffer_pool: pin count increments and decrements" {
    const allocator = std.testing.allocator;

    var posix_vfs = PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const path = try createTempPath(allocator, "pincount");
    defer allocator.free(path);
    defer vfs_impl.delete(path) catch {};

    var pm = try PageManager.init(allocator, vfs_impl, path, .{ .create = true });
    defer pm.deinit();

    var bp = try BufferPool.init(allocator, &pm, 65536);
    defer bp.deinit();

    const page1 = pm.allocatePage() catch unreachable;

    // First fetch - pin count should be 1
    const frame1 = try bp.fetchPage(page1, .shared);
    try std.testing.expectEqual(@as(u32, 1), frame1.pin_count.load(.monotonic));

    // Second fetch of same page - pin count should be 2
    const frame2 = try bp.fetchPage(page1, .shared);
    try std.testing.expect(frame1 == frame2); // Same frame
    try std.testing.expectEqual(@as(u32, 2), frame1.pin_count.load(.monotonic));

    // Unpin once - pin count should be 1
    bp.unpinPage(frame2, false);
    try std.testing.expectEqual(@as(u32, 1), frame1.pin_count.load(.monotonic));

    // Unpin again - pin count should be 0
    bp.unpinPage(frame1, false);
    try std.testing.expectEqual(@as(u32, 0), frame1.pin_count.load(.monotonic));
}

// ============================================================================
// Contract: Same page returns same frame
// ============================================================================

test "buffer_pool: same page returns same frame" {
    const allocator = std.testing.allocator;

    var posix_vfs = PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const path = try createTempPath(allocator, "samepage");
    defer allocator.free(path);
    defer vfs_impl.delete(path) catch {};

    var pm = try PageManager.init(allocator, vfs_impl, path, .{ .create = true });
    defer pm.deinit();

    var bp = try BufferPool.init(allocator, &pm, 65536);
    defer bp.deinit();

    const page1 = pm.allocatePage() catch unreachable;

    // Fetch page
    const frame1 = try bp.fetchPage(page1, .shared);
    bp.unpinPage(frame1, false);

    // Fetch again - should get same frame (page still in pool)
    const frame2 = try bp.fetchPage(page1, .shared);
    try std.testing.expect(frame1 == frame2);
    bp.unpinPage(frame2, false);
}

// ============================================================================
// Contract: Dirty pages written on eviction
// ============================================================================

test "buffer_pool: dirty page written on eviction" {
    const allocator = std.testing.allocator;

    var posix_vfs = PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const path = try createTempPath(allocator, "dirty");
    defer allocator.free(path);
    defer vfs_impl.delete(path) catch {};

    var pm = try PageManager.init(allocator, vfs_impl, path, .{ .create = true });
    defer pm.deinit();

    // Small buffer pool: 2 frames
    var bp = try BufferPool.init(allocator, &pm, 8192);
    defer bp.deinit();

    const page1 = pm.allocatePage() catch unreachable;

    // Fetch and modify page1
    {
        const frame = try bp.fetchPage(page1, .exclusive);
        @memset(frame.data, 0xBB);
        bp.unpinPage(frame, true); // Mark dirty
    }

    // Force eviction by fetching more pages than pool can hold
    const page2 = pm.allocatePage() catch unreachable;
    const page3 = pm.allocatePage() catch unreachable;

    {
        const f2 = try bp.fetchPage(page2, .exclusive);
        bp.unpinPage(f2, false);
    }
    {
        const f3 = try bp.fetchPage(page3, .exclusive);
        bp.unpinPage(f3, false);
    }

    // Re-fetch page1 - should read from disk with our data
    {
        const frame = try bp.fetchPage(page1, .shared);
        try std.testing.expectEqual(@as(u8, 0xBB), frame.data[100]); // Check a byte in the data area
        bp.unpinPage(frame, false);
    }
}

// ============================================================================
// Contract: Pool full with all pinned returns error
// ============================================================================

test "buffer_pool: pool full with all pinned returns error" {
    const allocator = std.testing.allocator;

    var posix_vfs = PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const path = try createTempPath(allocator, "poolfull");
    defer allocator.free(path);
    defer vfs_impl.delete(path) catch {};

    var pm = try PageManager.init(allocator, vfs_impl, path, .{ .create = true });
    defer pm.deinit();

    // Tiny buffer pool: only 2 frames
    var bp = try BufferPool.init(allocator, &pm, 8192);
    defer bp.deinit();

    const page1 = pm.allocatePage() catch unreachable;
    const page2 = pm.allocatePage() catch unreachable;
    const page3 = pm.allocatePage() catch unreachable;

    // Pin both frames
    const frame1 = try bp.fetchPage(page1, .exclusive);
    const frame2 = try bp.fetchPage(page2, .exclusive);

    // Try to fetch page3 - should fail (all frames pinned)
    const result = bp.fetchPage(page3, .exclusive);
    try std.testing.expectError(BufferPoolError.BufferPoolFull, result);

    // Cleanup
    bp.unpinPage(frame1, false);
    bp.unpinPage(frame2, false);
}

// ============================================================================
// Contract: Dirty tracking works
// ============================================================================

test "buffer_pool: dirty flag set on unpin with dirty=true" {
    const allocator = std.testing.allocator;

    var posix_vfs = PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const path = try createTempPath(allocator, "dirtyflag");
    defer allocator.free(path);
    defer vfs_impl.delete(path) catch {};

    var pm = try PageManager.init(allocator, vfs_impl, path, .{ .create = true });
    defer pm.deinit();

    var bp = try BufferPool.init(allocator, &pm, 65536);
    defer bp.deinit();

    const page1 = pm.allocatePage() catch unreachable;

    // Fetch - should not be dirty
    const frame = try bp.fetchPage(page1, .exclusive);
    try std.testing.expect(!frame.dirty);

    // Unpin without dirty
    bp.unpinPage(frame, false);

    // Re-fetch
    const frame2 = try bp.fetchPage(page1, .exclusive);
    try std.testing.expect(!frame2.dirty);

    // Unpin with dirty=true
    bp.unpinPage(frame2, true);

    // Re-fetch and check dirty flag is set
    const frame3 = try bp.fetchPage(page1, .shared);
    try std.testing.expect(frame3.dirty);
    bp.unpinPage(frame3, false);
}

// ============================================================================
// Contract: Stats are accurate
// ============================================================================

test "buffer_pool: stats reflect actual state" {
    const allocator = std.testing.allocator;

    var posix_vfs = PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const path = try createTempPath(allocator, "stats");
    defer allocator.free(path);
    defer vfs_impl.delete(path) catch {};

    var pm = try PageManager.init(allocator, vfs_impl, path, .{ .create = true });
    defer pm.deinit();

    // 4 frames
    var bp = try BufferPool.init(allocator, &pm, 16384);
    defer bp.deinit();

    // Initially all free
    {
        const stats = bp.getStats();
        try std.testing.expectEqual(@as(usize, 4), stats.total_frames);
        try std.testing.expectEqual(@as(usize, 4), stats.free_frames);
        try std.testing.expectEqual(@as(usize, 0), stats.pinned_frames);
        try std.testing.expectEqual(@as(usize, 0), stats.dirty_frames);
    }

    const page1 = pm.allocatePage() catch unreachable;
    const page2 = pm.allocatePage() catch unreachable;

    // Fetch and pin 2 pages
    const frame1 = try bp.fetchPage(page1, .exclusive);
    const frame2 = try bp.fetchPage(page2, .exclusive);

    {
        const stats = bp.getStats();
        try std.testing.expectEqual(@as(usize, 2), stats.free_frames);
        try std.testing.expectEqual(@as(usize, 2), stats.pinned_frames);
    }

    // Mark one dirty
    bp.unpinPage(frame1, true);

    {
        const stats = bp.getStats();
        try std.testing.expectEqual(@as(usize, 1), stats.pinned_frames);
        try std.testing.expectEqual(@as(usize, 1), stats.dirty_frames);
    }

    bp.unpinPage(frame2, false);
}

// ============================================================================
// Contract: Invalid page ID rejected
// ============================================================================

test "buffer_pool: invalid page id rejected" {
    const allocator = std.testing.allocator;

    var posix_vfs = PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const path = try createTempPath(allocator, "invalid");
    defer allocator.free(path);
    defer vfs_impl.delete(path) catch {};

    var pm = try PageManager.init(allocator, vfs_impl, path, .{ .create = true });
    defer pm.deinit();

    var bp = try BufferPool.init(allocator, &pm, 65536);
    defer bp.deinit();

    // NULL_PAGE (0) should be rejected
    const result = bp.fetchPage(0, .shared);
    try std.testing.expectError(BufferPoolError.InvalidPageId, result);
}

// ============================================================================
// Contract: Flush all writes dirty pages
// ============================================================================

test "buffer_pool: flush all writes dirty pages" {
    const allocator = std.testing.allocator;

    var posix_vfs = PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const path = try createTempPath(allocator, "flushall");
    defer allocator.free(path);
    defer vfs_impl.delete(path) catch {};

    var pm = try PageManager.init(allocator, vfs_impl, path, .{ .create = true });
    defer pm.deinit();

    var bp = try BufferPool.init(allocator, &pm, 65536);
    defer bp.deinit();

    const page1 = pm.allocatePage() catch unreachable;
    const page2 = pm.allocatePage() catch unreachable;

    // Modify both pages
    {
        const f1 = try bp.fetchPage(page1, .exclusive);
        @memset(f1.data, 0xCC);
        bp.unpinPage(f1, true);
    }
    {
        const f2 = try bp.fetchPage(page2, .exclusive);
        @memset(f2.data, 0xDD);
        bp.unpinPage(f2, true);
    }

    // Check dirty count
    {
        const stats = bp.getStats();
        try std.testing.expectEqual(@as(usize, 2), stats.dirty_frames);
    }

    // Flush all
    try bp.flushAll();

    // Check no dirty pages remain
    {
        const stats = bp.getStats();
        try std.testing.expectEqual(@as(usize, 0), stats.dirty_frames);
    }
}
