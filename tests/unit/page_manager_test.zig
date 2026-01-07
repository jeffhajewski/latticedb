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
    const timestamp = std.time.milliTimestamp();
    const random = std.crypto.random.int(u32);
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
