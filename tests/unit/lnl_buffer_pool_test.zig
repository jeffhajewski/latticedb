//! A pool whose frames are all dirty, but for the ones the caller holds,
//! grows without sweeping until the next flush.

const std = @import("std");
const lattice = @import("lattice");

const BufferPool = lattice.storage.buffer_pool.BufferPool;
const PageManager = lattice.storage.page_manager.PageManager;
const PosixVfs = lattice.storage.vfs.PosixVfs;

fn createTempPath(allocator: std.mem.Allocator, name: []const u8) ![]u8 {
    const timestamp = @import("compat").milliTimestamp();
    const random = @import("compat").randomInt(u32);
    var buf: [128]u8 = undefined;
    const path = try std.fmt.bufPrint(&buf, "/tmp/lattice_lnl_bp_{s}_{d}_{x}.db", .{ name, timestamp, random });
    return allocator.dupe(u8, path);
}

test "buffer_pool: lnl a clean page the caller holds does not keep every miss sweeping" {
    const allocator = std.testing.allocator;

    var posix_vfs = PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const path = try createTempPath(allocator, "held");
    defer allocator.free(path);
    defer vfs_impl.delete(path) catch {};

    var pm = try PageManager.init(allocator, vfs_impl, path, .{ .create = true });
    defer pm.deinit();

    const frame_count = 16;
    var bp = try BufferPool.init(allocator, &pm, frame_count * pm.getPageSize());
    defer bp.deinit();

    // A page on disk, then held clean, as a B+Tree holds a leaf it is
    // about to change while it reads the next page.
    const held_id = try pm.allocatePage();
    {
        const frame = try bp.fetchPage(held_id, .exclusive);
        @memset(frame.data, 0xEE);
        bp.unpinPage(frame, true);
    }
    _ = try bp.flushAll();
    const held = try bp.fetchPage(held_id, .shared);
    try std.testing.expect(!held.dirty);

    // Every other frame dirty, as through a commit that is not flushed
    // until it has been applied whole.
    var pages: [frame_count + 1]u32 = undefined;
    for (&pages, 0..) |*page_id, i| {
        page_id.* = try pm.allocatePage();
        const frame = try bp.fetchPage(page_id.*, .exclusive);
        @memset(frame.data, @truncate(i));
        bp.unpinPage(frame, true);
    }

    // The sweep that found no frame to evict saw the held page, which is
    // clean. That frame is the caller's; it is no reason to sweep the
    // whole pool twice on every miss until the flush.
    try std.testing.expect(bp.all_frames_dirty);
    const grown = bp.getStats().total_frames;
    try std.testing.expect(grown > frame_count);

    // Once let go, it is left until the flush: the pool grows instead.
    bp.unpinPage(held, false);
    const extra_id = try pm.allocatePage();
    {
        const frame = try bp.fetchPage(extra_id, .exclusive);
        @memset(frame.data, 0xAB);
        bp.unpinPage(frame, true);
    }
    try std.testing.expectEqual(grown + 1, bp.getStats().total_frames);

    // The flush ends it: the pool is its own size again and evicts.
    _ = try bp.flushAll();
    try std.testing.expect(!bp.all_frames_dirty);
    try std.testing.expectEqual(@as(usize, frame_count), bp.getStats().total_frames);

    for (pages, 0..) |page_id, i| {
        const frame = try bp.fetchPage(page_id, .shared);
        defer bp.unpinPage(frame, false);
        try std.testing.expectEqual(@as(u8, @truncate(i)), frame.data[100]);
    }
    for ([_]struct { u32, u8 }{ .{ held_id, 0xEE }, .{ extra_id, 0xAB } }) |pair| {
        const frame = try bp.fetchPage(pair[0], .shared);
        defer bp.unpinPage(frame, false);
        try std.testing.expectEqual(pair[1], frame.data[100]);
    }
    try std.testing.expectEqual(@as(usize, frame_count), bp.getStats().total_frames);
}

test "buffer_pool: lnl many clean pages held by readers are worth the next sweep" {
    const allocator = std.testing.allocator;

    var posix_vfs = PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const path = try createTempPath(allocator, "readers");
    defer allocator.free(path);
    defer vfs_impl.delete(path) catch {};

    var pm = try PageManager.init(allocator, vfs_impl, path, .{ .create = true });
    defer pm.deinit();

    const frame_count = 16;
    var bp = try BufferPool.init(allocator, &pm, frame_count * pm.getPageSize());
    defer bp.deinit();

    // Half the pool held clean, as by readers, the other half dirty.
    const half = frame_count / 2;
    var read_ids: [half]u32 = undefined;
    for (&read_ids) |*page_id| {
        page_id.* = try pm.allocatePage();
        const frame = try bp.fetchPage(page_id.*, .exclusive);
        bp.unpinPage(frame, true);
    }
    _ = try bp.flushAll();
    var held: [half]*lattice.storage.buffer_pool.BufferFrame = undefined;
    for (read_ids, &held) |page_id, *frame| frame.* = try bp.fetchPage(page_id, .shared);
    for (0..half + 1) |_| {
        const frame = try bp.fetchPage(try pm.allocatePage(), .exclusive);
        bp.unpinPage(frame, true);
    }

    // No frame could go, so the pool grew; but the readers' frames come
    // free soon, and the next miss looks for them.
    try std.testing.expect(!bp.all_frames_dirty);
    const grown = bp.getStats().total_frames;
    for (held) |frame| bp.unpinPage(frame, false);
    {
        const frame = try bp.fetchPage(try pm.allocatePage(), .exclusive);
        bp.unpinPage(frame, true);
    }
    try std.testing.expectEqual(grown, bp.getStats().total_frames);
}
