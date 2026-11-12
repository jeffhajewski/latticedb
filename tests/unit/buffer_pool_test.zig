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
    const timestamp = @import("compat").milliTimestamp();
    const random = @import("compat").randomInt(u32);
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

test "buffer_pool: pinned frame does not hide evictable second-chance frame" {
    const allocator = std.testing.allocator;

    var posix_vfs = PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const path = try createTempPath(allocator, "second_chance_pinned");
    defer allocator.free(path);
    defer vfs_impl.delete(path) catch {};

    var pm = try PageManager.init(allocator, vfs_impl, path, .{ .create = true });
    defer pm.deinit();

    var bp = try BufferPool.init(allocator, &pm, 8192);
    defer bp.deinit();

    const page1 = pm.allocatePage() catch unreachable;
    const page2 = pm.allocatePage() catch unreachable;
    const page3 = pm.allocatePage() catch unreachable;

    const pinned = try bp.fetchPage(page1, .exclusive);
    const second_chance = try bp.fetchPage(page2, .exclusive);
    bp.unpinPage(second_chance, false);

    const evicted = try bp.fetchPage(page3, .exclusive);
    bp.unpinPage(evicted, false);
    bp.unpinPage(pinned, false);
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

    // Flush all, which reports the two it wrote
    try std.testing.expectEqual(@as(u32, 2), try bp.flushAll());

    // Check no dirty pages remain
    {
        const stats = bp.getStats();
        try std.testing.expectEqual(@as(usize, 0), stats.dirty_frames);
    }
}

// ============================================================================
// Contract: a page found in the table is the page, not what the frame held
// ============================================================================

/// A memory filesystem whose reads of one range stall until let through.
const GatedVfs = struct {
    memory: lattice.storage.memory_vfs.MemoryVfs,
    gate_offset: u64 = std.math.maxInt(u64),
    open_gate: std.atomic.Value(bool) = .init(true),
    reader_waiting: std.atomic.Value(bool) = .init(false),
    inner: vfs.File = undefined,

    fn vfsOf(self: *GatedVfs) vfs.Vfs {
        return .{ .ptr = self, .vtable = &vtable };
    }

    const vtable = vfs.Vfs.VTable{ .open = open, .delete = delete, .exists = exists };

    fn open(ptr: *anyopaque, path: []const u8, flags: vfs.OpenFlags) vfs.VfsError!vfs.File {
        const self: *GatedVfs = @ptrCast(@alignCast(ptr));
        self.inner = try self.memory.vfs().open(path, flags);
        return .{ .ptr = self, .vtable = &file_vtable };
    }
    fn delete(ptr: *anyopaque, path: []const u8) vfs.VfsError!void {
        const self: *GatedVfs = @ptrCast(@alignCast(ptr));
        return self.memory.vfs().delete(path);
    }
    fn exists(ptr: *anyopaque, path: []const u8) bool {
        const self: *GatedVfs = @ptrCast(@alignCast(ptr));
        return self.memory.vfs().exists(path);
    }

    const file_vtable = vfs.File.VTable{
        .read = read,
        .write = write,
        .sync = sync,
        .truncate = truncate,
        .size = size,
        .close = close,
        .lock = lock,
        .tryLock = tryLock,
        .unlock = unlock,
    };
    fn me(ptr: *anyopaque) *GatedVfs {
        return @ptrCast(@alignCast(ptr));
    }
    fn read(ptr: *anyopaque, offset: u64, buf: []u8) vfs.VfsError!usize {
        const self = me(ptr);
        if (offset == self.gate_offset) {
            self.reader_waiting.store(true, .release);
            while (!self.open_gate.load(.acquire)) std.atomic.spinLoopHint();
        }
        return self.inner.read(offset, buf);
    }
    fn write(ptr: *anyopaque, offset: u64, data: []const u8) vfs.VfsError!void {
        return me(ptr).inner.write(offset, data);
    }
    fn sync(ptr: *anyopaque) vfs.VfsError!void {
        return me(ptr).inner.sync();
    }
    fn truncate(ptr: *anyopaque, n: u64) vfs.VfsError!void {
        return me(ptr).inner.truncate(n);
    }
    fn size(ptr: *anyopaque) vfs.VfsError!u64 {
        return me(ptr).inner.size();
    }
    fn close(ptr: *anyopaque) void {
        me(ptr).inner.close();
    }
    fn lock(ptr: *anyopaque, mode: vfs.LockMode) vfs.VfsError!void {
        return me(ptr).inner.lock(mode);
    }
    fn tryLock(ptr: *anyopaque, mode: vfs.LockMode) vfs.VfsError!bool {
        return me(ptr).inner.tryLock(mode);
    }
    fn unlock(ptr: *anyopaque) void {
        me(ptr).inner.unlock();
    }
};

test "buffer_pool: ldur a fetch of a page still being read waits for it" {
    const allocator = std.testing.allocator;
    var gated = GatedVfs{ .memory = lattice.storage.memory_vfs.MemoryVfs.init(allocator) };
    defer gated.memory.deinit();

    var pm = try PageManager.init(allocator, gated.vfsOf(), "db", .{ .create = true });
    defer pm.deinit();
    const page_id = try pm.allocatePage();
    var buf: [4096]u8 align(4096) = undefined;
    @memset(&buf, 0x5A);
    try pm.writePage(page_id, &buf);

    var bp = try BufferPool.init(allocator, &pm, 16 * 4096);
    defer bp.deinit();

    gated.gate_offset = @as(u64, page_id) * 4096;
    gated.open_gate.store(false, .release);

    const Fetch = struct {
        fn run(pool: *BufferPool, id: u32, seen: *std.atomic.Value(u8), done: *std.atomic.Value(bool)) void {
            const frame = pool.fetchPage(id, .shared) catch return;
            seen.store(frame.data[100], .release);
            pool.unpinPage(frame, false);
            done.store(true, .release);
        }
    };
    var first_seen = std.atomic.Value(u8).init(0);
    var first_done = std.atomic.Value(bool).init(false);
    const first = try std.Thread.spawn(.{}, Fetch.run, .{ &bp, page_id, &first_seen, &first_done });
    while (!gated.reader_waiting.load(.acquire)) std.atomic.spinLoopHint();

    // The first fetch is reading the page. A second one finds it in the
    // table, and must not take the frame's bytes until they are the page's.
    var second_seen = std.atomic.Value(u8).init(0);
    var second_done = std.atomic.Value(bool).init(false);
    const second = try std.Thread.spawn(.{}, Fetch.run, .{ &bp, page_id, &second_seen, &second_done });
    @import("compat").sleep(50 * std.time.ns_per_ms);
    const returned_early = second_done.load(.acquire);

    gated.open_gate.store(true, .release);
    first.join();
    second.join();

    try std.testing.expect(!returned_early);
    try std.testing.expectEqual(@as(u8, 0x5A), first_seen.load(.acquire));
    try std.testing.expectEqual(@as(u8, 0x5A), second_seen.load(.acquire));
}

// ============================================================================
// Contract: a load larger than the pool leaves it its usual size, and misses
// stay cheap however long the pool has been evicting
// ============================================================================

test "buffer_pool: ldur a load larger than the pool leaves the page table its usual size" {
    const allocator = std.testing.allocator;

    var posix_vfs = PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const path = try createTempPath(allocator, "bigload");
    defer allocator.free(path);
    defer vfs_impl.delete(path) catch {};

    var pm = try PageManager.init(allocator, vfs_impl, path, .{ .create = true });
    defer pm.deinit();

    const frame_count = 16;
    var bp = try BufferPool.init(allocator, &pm, frame_count * pm.getPageSize());
    defer bp.deinit();

    // Dirty far more pages than the pool holds, as a large commit does. The
    // pool grows past its size to keep them, and its page table with it.
    const page_count = 2000;
    var pages: [page_count]u32 = undefined;
    for (&pages, 0..) |*page_id, i| {
        page_id.* = try pm.allocatePage();
        const frame = try bp.fetchPage(page_id.*, .exclusive);
        @memset(frame.data, @truncate(i));
        bp.unpinPage(frame, true);
    }
    try std.testing.expect(bp.getStats().total_frames >= page_count);
    try std.testing.expect(bp.page_table.capacity() >= page_count);

    _ = try bp.flushAll();

    // The flush gives back the frames beyond the pool's size, and the page
    // table goes back to a size for the pool's own frames: one many times
    // larger, full of tombstones, makes every miss walk it.
    try std.testing.expectEqual(@as(usize, frame_count), bp.getStats().total_frames);
    try std.testing.expectEqual(@as(usize, 0), bp.overflow.items.len);
    try std.testing.expect(bp.page_table.capacity() <= 4 * frame_count);

    // Read everything back twice over, every fetch a miss that evicts. A
    // removed key leaves a tombstone, and a miss probes until it finds a
    // slot that never held one; the table must keep such slots.
    for (0..2) |_| {
        for (pages, 0..) |page_id, i| {
            const frame = try bp.fetchPage(page_id, .shared);
            defer bp.unpinPage(frame, false);
            try std.testing.expectEqual(@as(u8, @truncate(i)), frame.data[100]);
        }
    }
    try std.testing.expect(bp.page_table.capacity() <= 4 * frame_count);
    try std.testing.expect(bp.pageTableFreeSlots() * 5 >= bp.page_table.capacity());
    // Clean pages are evicted again rather than the pool growing: the flush
    // ended the stretch in which every frame was dirty.
    try std.testing.expectEqual(@as(usize, frame_count), bp.getStats().total_frames);
}

test "buffer_pool: ldur long eviction churn keeps free slots in the page table" {
    const allocator = std.testing.allocator;

    var posix_vfs = PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const path = try createTempPath(allocator, "churn");
    defer allocator.free(path);
    defer vfs_impl.delete(path) catch {};

    var pm = try PageManager.init(allocator, vfs_impl, path, .{ .create = true });
    defer pm.deinit();

    const frame_count = 16;
    var bp = try BufferPool.init(allocator, &pm, frame_count * pm.getPageSize());
    defer bp.deinit();

    // Pages written a few at a time, so the pool never grows past its size.
    const page_count = 200;
    var pages: [page_count]u32 = undefined;
    for (&pages, 0..) |*page_id, i| {
        page_id.* = try pm.allocatePage();
        const frame = try bp.fetchPage(page_id.*, .exclusive);
        @memset(frame.data, @truncate(i));
        bp.unpinPage(frame, true);
        if (i % 4 == 3) _ = try bp.flushAll();
    }
    _ = try bp.flushAll();
    try std.testing.expectEqual(@as(usize, 0), bp.overflow.items.len);

    // Thousands of misses, each evicting a page. Each eviction leaves a
    // tombstone; unless the table is rehashed, the slots that never held a
    // key run out and every miss probes the whole table.
    for (0..20) |_| {
        for (pages, 0..) |page_id, i| {
            const frame = try bp.fetchPage(page_id, .shared);
            defer bp.unpinPage(frame, false);
            try std.testing.expectEqual(@as(u8, @truncate(i)), frame.data[100]);
        }
    }
    try std.testing.expect(bp.pageTableFreeSlots() * 5 >= bp.page_table.capacity());
}
