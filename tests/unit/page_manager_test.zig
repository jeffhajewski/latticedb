//! Behavioral tests for Page Manager.
//!
//! These tests verify the file management and page allocation contracts.

const std = @import("std");
const lattice = @import("lattice");

const page_manager = lattice.storage.page_manager;
const vfs = lattice.storage.vfs;
const types = lattice.core.types;

const PageManager = page_manager.PageManager;
const PageManagerError = page_manager.PageManagerError;
const PosixVfs = vfs.PosixVfs;

fn createTempPath(allocator: std.mem.Allocator, name: []const u8) ![]u8 {
    const timestamp = @import("compat").milliTimestamp();
    const random = @import("compat").randomInt(u32);
    var buf: [128]u8 = undefined;
    const path = try std.fmt.bufPrint(&buf, "/tmp/lattice_pm_test_{s}_{d}_{x}.db", .{ name, timestamp, random });
    return allocator.dupe(u8, path);
}

// ============================================================================
// Contract: New database has valid header
// ============================================================================

test "page_manager: new database has valid header" {
    const allocator = std.testing.allocator;

    var posix_vfs = PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const path = try createTempPath(allocator, "newhdr");
    defer allocator.free(path);
    defer vfs_impl.delete(path) catch {};

    var pm = try PageManager.init(allocator, vfs_impl, path, .{ .create = true });
    defer pm.deinit();

    // Verify magic number
    try std.testing.expectEqual(types.MAGIC_NUMBER, pm.header.magic);

    // Verify format version
    try std.testing.expectEqual(types.FORMAT_VERSION, pm.header.format_version);

    // Verify page size
    try std.testing.expectEqual(@as(u32, 4096), pm.getPageSize());

    // Verify timestamps are set
    try std.testing.expect(pm.header.created_timestamp > 0);
    try std.testing.expect(pm.header.modified_timestamp > 0);

    // Verify UUID is set (not all zeros)
    var all_zero = true;
    for (pm.header.file_uuid) |byte| {
        if (byte != 0) {
            all_zero = false;
            break;
        }
    }
    try std.testing.expect(!all_zero);
}

// ============================================================================
// Contract: Reopened database preserves header
// ============================================================================

test "page_manager: header preserved across reopen" {
    const allocator = std.testing.allocator;

    var posix_vfs = PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const path = try createTempPath(allocator, "reopen");
    defer allocator.free(path);
    defer vfs_impl.delete(path) catch {};

    var original_uuid: [16]u8 = undefined;
    var original_created: u64 = undefined;

    // Create database
    {
        var pm = try PageManager.init(allocator, vfs_impl, path, .{ .create = true });
        original_uuid = pm.header.file_uuid;
        original_created = pm.header.created_timestamp;
        pm.deinit();
    }

    // Reopen and verify
    {
        var pm = try PageManager.init(allocator, vfs_impl, path, .{});
        defer pm.deinit();

        try std.testing.expectEqual(types.MAGIC_NUMBER, pm.header.magic);
        try std.testing.expectEqual(original_uuid, pm.header.file_uuid);
        try std.testing.expectEqual(original_created, pm.header.created_timestamp);
    }
}

// ============================================================================
// Contract: Allocated pages are usable
// ============================================================================

test "page_manager: allocated page is writable and readable" {
    const allocator = std.testing.allocator;

    var posix_vfs = PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const path = try createTempPath(allocator, "allocrw");
    defer allocator.free(path);
    defer vfs_impl.delete(path) catch {};

    var pm = try PageManager.init(allocator, vfs_impl, path, .{ .create = true });
    defer pm.deinit();

    // Allocate a page
    const page_id = try pm.allocatePage();
    try std.testing.expect(page_id > 0); // Page 0 is header

    // Write data to page
    var write_buf: [4096]u8 align(4096) = undefined;
    @memset(&write_buf, 0xEE);

    try pm.writePage(page_id, &write_buf);

    // Read it back
    var read_buf: [4096]u8 align(4096) = undefined;
    try pm.readPage(page_id, &read_buf);

    // Verify data matches (skip header area)
    try std.testing.expectEqual(@as(u8, 0xEE), read_buf[100]);
    try std.testing.expectEqual(@as(u8, 0xEE), read_buf[1000]);
    try std.testing.expectEqual(@as(u8, 0xEE), read_buf[4000]);
}

// ============================================================================
// Contract: Multiple page allocations work
// ============================================================================

test "page_manager: multiple page allocations" {
    const allocator = std.testing.allocator;

    var posix_vfs = PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const path = try createTempPath(allocator, "multialloc");
    defer allocator.free(path);
    defer vfs_impl.delete(path) catch {};

    var pm = try PageManager.init(allocator, vfs_impl, path, .{ .create = true });
    defer pm.deinit();

    // Allocate multiple pages
    var page_ids: [10]types.PageId = undefined;
    for (&page_ids) |*pid| {
        pid.* = try pm.allocatePage();
    }

    // All should be unique and > 0
    for (page_ids, 0..) |pid, i| {
        try std.testing.expect(pid > 0);

        // Check uniqueness
        for (page_ids[i + 1 ..]) |other| {
            try std.testing.expect(pid != other);
        }
    }
}

// ============================================================================
// Contract: Freed pages are reusable
// ============================================================================

test "page_manager: freed page returned to freelist" {
    const allocator = std.testing.allocator;

    var posix_vfs = PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const path = try createTempPath(allocator, "freelist");
    defer allocator.free(path);
    defer vfs_impl.delete(path) catch {};

    var pm = try PageManager.init(allocator, vfs_impl, path, .{ .create = true });
    defer pm.deinit();

    // Allocate page
    const page1 = try pm.allocatePage();

    // Free it
    try pm.freePage(page1);

    // Allocate again - should get the same page back
    const page2 = try pm.allocatePage();
    try std.testing.expectEqual(page1, page2);
}

// ============================================================================
// Contract: Magic number mismatch rejected
// ============================================================================

test "page_manager: magic number mismatch rejected" {
    const allocator = std.testing.allocator;

    var posix_vfs = PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const path = try createTempPath(allocator, "badmagic");
    defer allocator.free(path);
    defer vfs_impl.delete(path) catch {};

    // Create a valid database first
    {
        var pm = try PageManager.init(allocator, vfs_impl, path, .{ .create = true });
        pm.deinit();
    }

    // Corrupt the magic number
    {
        const file = try vfs_impl.open(path, .{ .read = true, .write = true });
        defer file.close();

        var buf: [4]u8 = undefined;
        std.mem.writeInt(u32, &buf, 0xDEADBEEF, .little);
        try file.write(0, &buf);
    }

    // Try to open - should fail
    const result = PageManager.init(allocator, vfs_impl, path, .{});
    try std.testing.expectError(PageManagerError.InvalidMagic, result);
}

// ============================================================================
// Contract: Page size is preserved
// ============================================================================

test "page_manager: page size preserved across reopen" {
    const allocator = std.testing.allocator;

    var posix_vfs = PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const path = try createTempPath(allocator, "pagesize");
    defer allocator.free(path);
    defer vfs_impl.delete(path) catch {};

    // Create with default page size
    {
        var pm = try PageManager.init(allocator, vfs_impl, path, .{ .create = true, .page_size = 4096 });
        try std.testing.expectEqual(@as(u32, 4096), pm.getPageSize());
        pm.deinit();
    }

    // Reopen - page size should be read from file
    {
        var pm = try PageManager.init(allocator, vfs_impl, path, .{});
        defer pm.deinit();
        try std.testing.expectEqual(@as(u32, 4096), pm.getPageSize());
    }
}

// ============================================================================
// Contract: Read-only mode prevents writes
// ============================================================================

test "page_manager: read only mode prevents writes" {
    const allocator = std.testing.allocator;

    var posix_vfs = PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const path = try createTempPath(allocator, "readonly");
    defer allocator.free(path);
    defer vfs_impl.delete(path) catch {};

    // Create database
    {
        var pm = try PageManager.init(allocator, vfs_impl, path, .{ .create = true });
        _ = try pm.allocatePage(); // Allocate at least one page
        pm.deinit();
    }

    // Open read-only
    {
        var pm = try PageManager.init(allocator, vfs_impl, path, .{ .read_only = true });
        defer pm.deinit();

        // Allocate should fail
        const result = pm.allocatePage();
        try std.testing.expectError(PageManagerError.PermissionDenied, result);
    }
}

// ============================================================================
// Contract: File not found is properly reported
// ============================================================================

test "page_manager: file not found error" {
    const allocator = std.testing.allocator;

    var posix_vfs = PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const path = "/tmp/lattice_pm_test_nonexistent_12345.db";

    // Try to open non-existent file without create flag
    const result = PageManager.init(allocator, vfs_impl, path, .{ .create = false });
    try std.testing.expectError(PageManagerError.FileNotFound, result);
}

// ============================================================================
// Contract: Page count is accurate
// ============================================================================

test "page_manager: page count accurate" {
    const allocator = std.testing.allocator;

    var posix_vfs = PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const path = try createTempPath(allocator, "pagecount");
    defer allocator.free(path);
    defer vfs_impl.delete(path) catch {};

    var pm = try PageManager.init(allocator, vfs_impl, path, .{ .create = true });
    defer pm.deinit();

    // Initially just header page
    const initial_count = pm.pageCount();
    try std.testing.expectEqual(@as(u32, 1), initial_count);

    // Allocate 5 pages
    for (0..5) |_| {
        _ = try pm.allocatePage();
    }

    // Should have 6 pages now (header + 5)
    const final_count = pm.pageCount();
    try std.testing.expectEqual(@as(u32, 6), final_count);
}

// ============================================================================
// Contract: Data persists across reopen
// ============================================================================

test "page_manager: data persists across reopen" {
    const allocator = std.testing.allocator;

    var posix_vfs = PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const path = try createTempPath(allocator, "persist");
    defer allocator.free(path);
    defer vfs_impl.delete(path) catch {};

    var page_id: types.PageId = undefined;

    // Write data
    {
        var pm = try PageManager.init(allocator, vfs_impl, path, .{ .create = true });
        page_id = try pm.allocatePage();

        var buf: [4096]u8 align(4096) = undefined;
        @memset(&buf, 0xFF);
        buf[100] = 0x42; // Distinctive byte

        try pm.writePage(page_id, &buf);
        pm.deinit();
    }

    // Reopen and verify
    {
        var pm = try PageManager.init(allocator, vfs_impl, path, .{});
        defer pm.deinit();

        var buf: [4096]u8 align(4096) = undefined;
        try pm.readPage(page_id, &buf);

        try std.testing.expectEqual(@as(u8, 0x42), buf[100]);
        try std.testing.expectEqual(@as(u8, 0xFF), buf[200]);
    }
}

// ============================================================================
// Contract: Page count is kept in memory
// ============================================================================

/// Passes everything through to another VFS, counting how often a file is
/// asked for its size. Only one file may be open through it at a time.
const SizeCountingVfs = struct {
    inner: vfs.Vfs,
    file: vfs.File = undefined,
    size_calls: usize = 0,

    const Self = @This();

    fn get(self: *Self) vfs.Vfs {
        return .{ .ptr = self, .vtable = &vfs_vtable };
    }

    const vfs_vtable = vfs.Vfs.VTable{
        .open = open,
        .delete = delete,
        .exists = exists,
    };

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

    fn from(ptr: *anyopaque) *Self {
        return @ptrCast(@alignCast(ptr));
    }

    fn open(ptr: *anyopaque, path: []const u8, flags: vfs.OpenFlags) vfs.VfsError!vfs.File {
        const self = from(ptr);
        self.file = try self.inner.open(path, flags);
        return .{ .ptr = self, .vtable = &file_vtable };
    }
    fn delete(ptr: *anyopaque, path: []const u8) vfs.VfsError!void {
        return from(ptr).inner.delete(path);
    }
    fn exists(ptr: *anyopaque, path: []const u8) bool {
        return from(ptr).inner.exists(path);
    }
    fn read(ptr: *anyopaque, offset: u64, buf: []u8) vfs.VfsError!usize {
        return from(ptr).file.read(offset, buf);
    }
    fn write(ptr: *anyopaque, offset: u64, data: []const u8) vfs.VfsError!void {
        return from(ptr).file.write(offset, data);
    }
    fn sync(ptr: *anyopaque) vfs.VfsError!void {
        return from(ptr).file.sync();
    }
    fn truncate(ptr: *anyopaque, new_size: u64) vfs.VfsError!void {
        return from(ptr).file.truncate(new_size);
    }
    fn size(ptr: *anyopaque) vfs.VfsError!u64 {
        const self = from(ptr);
        self.size_calls += 1;
        return self.file.size();
    }
    fn close(ptr: *anyopaque) void {
        from(ptr).file.close();
    }
    fn lock(ptr: *anyopaque, mode: vfs.LockMode) vfs.VfsError!void {
        return from(ptr).file.lock(mode);
    }
    fn tryLock(ptr: *anyopaque, mode: vfs.LockMode) vfs.VfsError!bool {
        return from(ptr).file.tryLock(mode);
    }
    fn unlock(ptr: *anyopaque) void {
        from(ptr).file.unlock();
    }
};

/// The page count the file itself implies, which the cached one must match.
fn pagesOnDisk(pm: *PageManager) !u32 {
    return @intCast((try pm.file.size()) / pm.getPageSize());
}

test "page_manager: page checks do not ask the file for its size" {
    // Every HNSW neighbour list and every vector read checks its page id
    // against the page count. A syscall per check made vector search several
    // times slower, so the count has to come from memory.
    const allocator = std.testing.allocator;

    var memory = lattice.storage.memory_vfs.MemoryVfs.init(allocator);
    defer memory.deinit();
    var counting = SizeCountingVfs{ .inner = memory.vfs() };

    var pm = try PageManager.init(allocator, counting.get(), "sizecalls.db", .{ .create = true });
    defer pm.deinit();

    counting.size_calls = 0;

    for (0..8) |_| _ = try pm.allocatePage();
    try pm.freePage(3);
    _ = try pm.allocatePage();

    for (0..1000) |i| {
        _ = pm.isValidPage(@intCast(i % 16));
        _ = pm.pageCount();
    }

    try std.testing.expectEqual(@as(usize, 0), counting.size_calls);
}

test "page_manager: page count follows the file as it grows and shrinks" {
    const allocator = std.testing.allocator;

    var memory = lattice.storage.memory_vfs.MemoryVfs.init(allocator);
    defer memory.deinit();
    const vfs_impl = memory.vfs();

    const page_alignment = comptime std.mem.Alignment.fromByteUnits(4096);
    const buf = try allocator.alignedAlloc(u8, page_alignment, types.DEFAULT_PAGE_SIZE);
    defer allocator.free(buf);
    @memset(buf, 0);
    const header: *lattice.storage.page.PageHeader = @ptrCast(@alignCast(buf.ptr));
    header.* = lattice.storage.page.PageHeader.init(.btree_leaf);

    {
        var pm = try PageManager.init(allocator, vfs_impl, "grow.db", .{ .create = true });
        defer pm.deinit();

        // A new file is just its header.
        try std.testing.expectEqual(@as(u32, 1), pm.pageCount());
        try std.testing.expectEqual(try pagesOnDisk(&pm), pm.pageCount());

        // Growing at the end of the file.
        for (0..4) |_| _ = try pm.allocatePage();
        try std.testing.expectEqual(@as(u32, 5), pm.pageCount());
        try std.testing.expectEqual(try pagesOnDisk(&pm), pm.pageCount());

        // Freeing a page, and reusing it, leaves the file alone.
        try pm.freePage(2);
        try std.testing.expectEqual(@as(u32, 5), pm.pageCount());
        try std.testing.expectEqual(@as(types.PageId, 2), try pm.allocatePage());
        try std.testing.expectEqual(@as(u32, 5), pm.pageCount());

        // Recovery replays pages the file may not reach yet, when the crash
        // came before the extended file was synced. Writing past the end
        // grows the file to cover the page.
        try pm.writePage(9, buf);
        try std.testing.expectEqual(@as(u32, 10), pm.pageCount());
        try std.testing.expectEqual(try pagesOnDisk(&pm), pm.pageCount());
        try std.testing.expect(pm.isValidPage(9));

        // A write inside the file does not move the end.
        for (1..5) |page_id| try pm.writePage(@intCast(page_id), buf);
        try std.testing.expectEqual(@as(u32, 10), pm.pageCount());

        // Pages 5 to 8 were never written, so they read as free, and 9 is
        // live. Free 9 and the whole tail from 5 goes.
        try pm.freePage(9);
        const stats = try pm.truncateFreeTail();
        try std.testing.expectEqual(@as(u32, 5), stats.pages_after);
        try std.testing.expectEqual(@as(u32, 5), pm.pageCount());
        try std.testing.expectEqual(try pagesOnDisk(&pm), pm.pageCount());
        try std.testing.expect(!pm.isValidPage(5));
        try std.testing.expect(!pm.isValidPage(9));

        // Growth resumes at the new end.
        try std.testing.expectEqual(@as(types.PageId, 5), try pm.allocatePage());
        try std.testing.expectEqual(@as(u32, 6), pm.pageCount());
        try std.testing.expectEqual(try pagesOnDisk(&pm), pm.pageCount());
    }

    // Reopening reads the count from the file.
    {
        var pm = try PageManager.init(allocator, vfs_impl, "grow.db", .{});
        defer pm.deinit();
        try std.testing.expectEqual(@as(u32, 6), pm.pageCount());
    }
    {
        var pm = try PageManager.init(allocator, vfs_impl, "grow.db", .{ .read_only = true });
        defer pm.deinit();
        try std.testing.expectEqual(@as(u32, 6), pm.pageCount());
    }
}

test "page_manager: valid pages are those between the header and the end" {
    const allocator = std.testing.allocator;

    var memory = lattice.storage.memory_vfs.MemoryVfs.init(allocator);
    defer memory.deinit();

    var pm = try PageManager.init(allocator, memory.vfs(), "bounds.db", .{ .create = true });
    defer pm.deinit();

    try std.testing.expect(!pm.isValidPage(0));
    try std.testing.expect(!pm.isValidPage(1));

    for (0..3) |_| _ = try pm.allocatePage();

    try std.testing.expect(!pm.isValidPage(0));
    try std.testing.expect(pm.isValidPage(1));
    try std.testing.expect(pm.isValidPage(3));
    try std.testing.expect(!pm.isValidPage(4));
    try std.testing.expect(!pm.isValidPage(std.math.maxInt(types.PageId)));
}
