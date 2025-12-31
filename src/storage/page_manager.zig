//! Page Manager for Lattice database.
//!
//! Manages page allocation, I/O, and the file header. All page access
//! goes through this module, which handles checksums and the freelist.

const std = @import("std");
const lattice = @import("lattice");

const Allocator = std.mem.Allocator;
const vfs = lattice.storage.vfs;
const page = lattice.storage.page;
const types = lattice.core.types;

const File = vfs.File;
const Vfs = vfs.Vfs;
const VfsError = vfs.VfsError;
const OpenFlags = vfs.OpenFlags;

const PageId = types.PageId;
const PageType = page.PageType;
const PageHeader = page.PageHeader;
const FileHeader = page.FileHeader;
const calculateChecksum = page.calculateChecksum;

const MAGIC_NUMBER = types.MAGIC_NUMBER;
const FORMAT_VERSION = types.FORMAT_VERSION;
const DEFAULT_PAGE_SIZE = types.DEFAULT_PAGE_SIZE;
const NULL_PAGE = types.NULL_PAGE;

/// Page Manager errors
pub const PageManagerError = error{
    InvalidHeader,
    InvalidMagic,
    VersionTooNew,
    ChecksumMismatch,
    InvalidPageId,
    PageNotAllocated,
    IoError,
    Unexpected,
    FileNotFound,
    PermissionDenied,
    DiskFull,
};

/// Options for opening a database file
pub const OpenOptions = struct {
    /// Create the file if it doesn't exist
    create: bool = false,
    /// Open in read-only mode
    read_only: bool = false,
    /// Page size (only used when creating new file)
    page_size: u32 = DEFAULT_PAGE_SIZE,
};

/// Manages page allocation and I/O for the database file.
pub const PageManager = struct {
    allocator: Allocator,
    file: File,
    header: FileHeader,
    page_size: u32,
    read_only: bool,

    const Self = @This();

    /// Open or create a database file.
    pub fn init(allocator: Allocator, vfs_impl: Vfs, path: []const u8, options: OpenOptions) PageManagerError!Self {
        const flags = OpenFlags{
            .read = true,
            .write = !options.read_only,
            .create = options.create,
        };

        const file = vfs_impl.open(path, flags) catch |err| {
            return switch (err) {
                VfsError.FileNotFound => PageManagerError.FileNotFound,
                VfsError.PermissionDenied => PageManagerError.PermissionDenied,
                else => PageManagerError.IoError,
            };
        };
        errdefer file.close();

        var self = Self{
            .allocator = allocator,
            .file = file,
            .header = undefined,
            .page_size = options.page_size,
            .read_only = options.read_only,
        };

        const file_size = file.size() catch return PageManagerError.IoError;

        if (file_size == 0 and options.create) {
            try self.initNewFile();
        } else if (file_size >= DEFAULT_PAGE_SIZE) {
            try self.loadHeader();
        } else {
            return PageManagerError.InvalidHeader;
        }

        return self;
    }

    /// Close the database file.
    pub fn deinit(self: *Self) void {
        self.file.close();
    }

    /// Initialize a new database file.
    fn initNewFile(self: *Self) PageManagerError!void {
        self.header = FileHeader.init();
        self.header.page_size = self.page_size;
        self.header.created_timestamp = @intCast(std.time.timestamp());
        self.header.modified_timestamp = self.header.created_timestamp;

        // Generate random UUID
        std.crypto.random.bytes(&self.header.file_uuid);

        try self.writeHeader();
    }

    /// Load and validate the file header.
    fn loadHeader(self: *Self) PageManagerError!void {
        var buf: [4096]u8 = undefined;
        const n = self.file.read(0, &buf) catch return PageManagerError.IoError;
        if (n != 4096) return PageManagerError.InvalidHeader;

        // Copy header from buffer
        self.header = std.mem.bytesAsValue(FileHeader, buf[0..@sizeOf(FileHeader)]).*;

        // Validate magic number
        if (self.header.magic != MAGIC_NUMBER) {
            return PageManagerError.InvalidMagic;
        }

        // Validate version
        if (self.header.min_reader_version > FORMAT_VERSION) {
            return PageManagerError.VersionTooNew;
        }

        // Use the page size from the file
        self.page_size = self.header.page_size;
    }

    /// Write the file header to disk.
    fn writeHeader(self: *Self) PageManagerError!void {
        if (self.read_only) return PageManagerError.PermissionDenied;

        self.header.modified_timestamp = @intCast(std.time.timestamp());

        var buf: [4096]u8 = [_]u8{0} ** 4096;
        const header_bytes = std.mem.asBytes(&self.header);
        @memcpy(buf[0..header_bytes.len], header_bytes);

        self.file.write(0, &buf) catch return PageManagerError.IoError;
    }

    /// Allocate a new page.
    pub fn allocatePage(self: *Self) PageManagerError!PageId {
        if (self.read_only) return PageManagerError.PermissionDenied;

        // Try to get from freelist first
        if (self.header.freelist_page != NULL_PAGE) {
            return self.allocateFromFreelist();
        }

        // Allocate new page at end of file
        const page_id: PageId = self.pageCount();
        self.header.freelist_page = self.header.freelist_page; // unchanged

        // Extend file with zeroed page
        const offset = self.pageOffset(page_id);
        var zeros: [4096]u8 align(@alignOf(PageHeader)) = [_]u8{0} ** 4096;

        // Set up as free page initially
        const header_ptr: *PageHeader = @ptrCast(&zeros);
        header_ptr.* = PageHeader.init(.free);

        self.file.write(offset, &zeros) catch return PageManagerError.IoError;

        try self.writeHeader();

        return page_id;
    }

    /// Allocate a page from the freelist.
    fn allocateFromFreelist(self: *Self) PageManagerError!PageId {
        const page_id = self.header.freelist_page;

        // Read the free page to get next pointer
        var buf: [4096]u8 = undefined;
        try self.readPageRaw(page_id, &buf);

        // Next free page is stored at offset 8 (after PageHeader)
        const next_free = std.mem.readInt(u32, buf[8..12], .little);
        self.header.freelist_page = next_free;

        try self.writeHeader();

        return page_id;
    }

    /// Free a page (add to freelist).
    pub fn freePage(self: *Self, page_id: PageId) PageManagerError!void {
        if (self.read_only) return PageManagerError.PermissionDenied;
        if (page_id == 0) return PageManagerError.InvalidPageId; // Can't free header

        var buf: [4096]u8 align(@alignOf(PageHeader)) = [_]u8{0} ** 4096;

        // Set up page header as free
        const header_ptr: *PageHeader = @ptrCast(&buf);
        header_ptr.* = PageHeader.init(.free);

        // Store pointer to current freelist head
        std.mem.writeInt(u32, buf[8..12], self.header.freelist_page, .little);

        // Calculate and set checksum (covers bytes 8 to end)
        header_ptr.checksum = calculateChecksum(buf[8..]);

        // Write the page
        const offset = self.pageOffset(page_id);
        self.file.write(offset, &buf) catch return PageManagerError.IoError;

        // Update freelist head
        self.header.freelist_page = page_id;
        try self.writeHeader();
    }

    /// Read a page from disk (with checksum verification).
    pub fn readPage(self: *Self, page_id: PageId, buf: []u8) PageManagerError!void {
        if (buf.len != self.page_size) return PageManagerError.InvalidPageId;

        try self.readPageRaw(page_id, buf);

        // Verify checksum
        const header_ptr: *const PageHeader = @ptrCast(@alignCast(buf.ptr));
        const expected = calculateChecksum(buf[8..]);
        if (header_ptr.checksum != expected and header_ptr.checksum != 0) {
            return PageManagerError.ChecksumMismatch;
        }
    }

    /// Read a page without checksum verification.
    fn readPageRaw(self: *Self, page_id: PageId, buf: []u8) PageManagerError!void {
        const offset = self.pageOffset(page_id);
        const n = self.file.read(offset, buf) catch return PageManagerError.IoError;
        if (n != self.page_size) return PageManagerError.IoError;
    }

    /// Write a page to disk (calculates checksum automatically).
    pub fn writePage(self: *Self, page_id: PageId, buf: []u8) PageManagerError!void {
        if (self.read_only) return PageManagerError.PermissionDenied;
        if (buf.len != self.page_size) return PageManagerError.InvalidPageId;
        if (page_id == 0) return PageManagerError.InvalidPageId; // Can't overwrite header with writePage

        // Calculate and set checksum (covers bytes 8 to end)
        const header_ptr: *PageHeader = @ptrCast(@alignCast(buf.ptr));
        header_ptr.checksum = calculateChecksum(buf[8..]);

        const offset = self.pageOffset(page_id);
        self.file.write(offset, buf) catch return PageManagerError.IoError;
    }

    /// Sync all changes to disk.
    pub fn sync(self: *Self) PageManagerError!void {
        self.file.sync() catch return PageManagerError.IoError;
    }

    /// Get the file header (read-only).
    pub fn getHeader(self: *const Self) *const FileHeader {
        return &self.header;
    }

    /// Get current page count.
    pub fn pageCount(self: *const Self) u32 {
        const file_size = self.file.size() catch return 1;
        return @intCast(file_size / self.page_size);
    }

    /// Calculate file offset for a page.
    fn pageOffset(self: *const Self, page_id: PageId) u64 {
        return @as(u64, page_id) * @as(u64, self.page_size);
    }

    /// Get the page size.
    pub fn getPageSize(self: *const Self) u32 {
        return self.page_size;
    }

    /// Check if a page ID is valid (allocated).
    pub fn isValidPage(self: *const Self, page_id: PageId) bool {
        return page_id > 0 and page_id < self.pageCount();
    }
};

// ============================================================================
// Tests
// ============================================================================

test "create new database file" {
    const allocator = std.testing.allocator;

    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const path = "/tmp/lattice_pm_test_create.db";

    // Clean up any existing file
    vfs_impl.delete(path) catch {};

    // Create new database
    var pm = try PageManager.init(allocator, vfs_impl, path, .{ .create = true });
    defer {
        pm.deinit();
        vfs_impl.delete(path) catch {};
    }

    // Verify header
    const header = pm.getHeader();
    try std.testing.expectEqual(MAGIC_NUMBER, header.magic);
    try std.testing.expectEqual(FORMAT_VERSION, header.format_version);
    try std.testing.expectEqual(DEFAULT_PAGE_SIZE, header.page_size);
}

test "open existing database file" {
    const allocator = std.testing.allocator;

    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const path = "/tmp/lattice_pm_test_open.db";
    vfs_impl.delete(path) catch {};

    // Create database
    {
        var pm = try PageManager.init(allocator, vfs_impl, path, .{ .create = true });
        pm.deinit();
    }

    // Reopen database
    {
        var pm = try PageManager.init(allocator, vfs_impl, path, .{});
        defer pm.deinit();

        const header = pm.getHeader();
        try std.testing.expectEqual(MAGIC_NUMBER, header.magic);
    }

    vfs_impl.delete(path) catch {};
}

test "allocate and free pages" {
    const allocator = std.testing.allocator;

    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const path = "/tmp/lattice_pm_test_alloc.db";
    vfs_impl.delete(path) catch {};

    var pm = try PageManager.init(allocator, vfs_impl, path, .{ .create = true });
    defer {
        pm.deinit();
        vfs_impl.delete(path) catch {};
    }

    // Allocate some pages
    const page1 = try pm.allocatePage();
    const page2 = try pm.allocatePage();
    const page3 = try pm.allocatePage();

    try std.testing.expectEqual(@as(PageId, 1), page1);
    try std.testing.expectEqual(@as(PageId, 2), page2);
    try std.testing.expectEqual(@as(PageId, 3), page3);

    // Free middle page
    try pm.freePage(page2);

    // Next allocation should reuse freed page
    const page4 = try pm.allocatePage();
    try std.testing.expectEqual(@as(PageId, 2), page4);
}

test "read and write pages with checksum" {
    const allocator = std.testing.allocator;

    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const path = "/tmp/lattice_pm_test_rw.db";
    vfs_impl.delete(path) catch {};

    var pm = try PageManager.init(allocator, vfs_impl, path, .{ .create = true });
    defer {
        pm.deinit();
        vfs_impl.delete(path) catch {};
    }

    // Allocate a page
    const page_id = try pm.allocatePage();

    // Write data
    var write_buf: [4096]u8 align(@alignOf(PageHeader)) = [_]u8{0} ** 4096;
    const header_ptr: *PageHeader = @ptrCast(&write_buf);
    header_ptr.* = PageHeader.init(.btree_leaf);

    // Write some test data after header
    const test_data = "Hello, Lattice!";
    @memcpy(write_buf[8..][0..test_data.len], test_data);

    try pm.writePage(page_id, &write_buf);

    // Read back
    var read_buf: [4096]u8 align(@alignOf(PageHeader)) = undefined;
    try pm.readPage(page_id, &read_buf);

    // Verify header
    const read_header: *const PageHeader = @ptrCast(&read_buf);
    try std.testing.expectEqual(PageType.btree_leaf, read_header.page_type);

    // Verify data
    try std.testing.expectEqualStrings(test_data, read_buf[8..][0..test_data.len]);
}

test "file not found error" {
    const allocator = std.testing.allocator;

    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const result = PageManager.init(allocator, vfs_impl, "/tmp/nonexistent_db_12345.db", .{});
    try std.testing.expectError(PageManagerError.FileNotFound, result);
}

test "invalid magic number" {
    const allocator = std.testing.allocator;

    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const path = "/tmp/lattice_pm_test_invalid.db";
    vfs_impl.delete(path) catch {};

    // Create a file with invalid header
    const file = try vfs_impl.open(path, .{ .read = true, .write = true, .create = true });
    var garbage: [4096]u8 = undefined;
    @memset(&garbage, 0xFF);
    try file.write(0, &garbage);
    file.close();

    // Try to open - should fail with InvalidMagic
    const result = PageManager.init(allocator, vfs_impl, path, .{});
    try std.testing.expectError(PageManagerError.InvalidMagic, result);

    vfs_impl.delete(path) catch {};
}
