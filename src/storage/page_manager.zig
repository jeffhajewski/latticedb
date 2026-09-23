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
const LockMode = vfs.LockMode;
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

pub fn isValidPageSize(page_size: u32) bool {
    return page_size >= DEFAULT_PAGE_SIZE and page_size <= std.math.maxInt(u16);
}

/// Page Manager errors
pub const PageManagerError = error{
    InvalidHeader,
    InvalidMagic,
    InvalidPageSize,
    VersionTooNew,
    ChecksumMismatch,
    InvalidPageId,
    PageNotAllocated,
    IoError,
    OutOfMemory,
    Unexpected,
    FileNotFound,
    PermissionDenied,
    DiskFull,
    /// Another process holds the database in a way that conflicts with how this
    /// one asked for it.
    DatabaseLocked,
};

/// Options for opening a database file
pub const OpenOptions = struct {
    /// Create the file if it doesn't exist
    create: bool = false,
    /// Open in read-only mode
    read_only: bool = false,
    /// Page size (only used when creating new file)
    page_size: u32 = DEFAULT_PAGE_SIZE,
    /// Take a lock on the file, so processes cannot tread on each other.
    ///
    /// Turning this off is for filesystems where locking does not work rather
    /// than for arranging concurrent access, which this engine cannot do across
    /// processes however the lock is set.
    lock: bool = true,
};

pub const TruncateStats = struct {
    pages_before: u32,
    pages_after: u32,
    pages_removed: u32,
    bytes_reclaimed: u64,
};

/// Manages page allocation and I/O for the database file.
pub const PageManager = struct {
    allocator: Allocator,
    file: File,
    header: FileHeader,
    page_size: u32,
    read_only: bool,
    /// Whether this handle took the file lock, so it knows to let go.
    holds_lock: bool,
    /// Pages in the file, header included.
    ///
    /// Kept here rather than asked of the file, because every HNSW neighbour
    /// list and every vector read checks its page id against it, and a syscall
    /// per check made vector search several times slower. It is right as long
    /// as every write goes through this struct, which the file lock guarantees
    /// across processes. Atomic because readers check pages while the writer
    /// allocates them.
    page_count: std.atomic.Value(u32),

    const Self = @This();

    /// Open or create a database file.
    pub fn init(allocator: Allocator, vfs_impl: Vfs, path: []const u8, options: OpenOptions) PageManagerError!Self {
        if (!isValidPageSize(options.page_size)) return PageManagerError.InvalidPageSize;

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

        // Locked before anything is read, because the point of the lock is that
        // nobody else is changing the file while this one looks at it.
        //
        // A writer takes the file exclusively and a reader shares it, which
        // means a reader is refused while a writer holds the database. That is
        // the honest answer rather than a limitation of the lock: a reader in
        // another process cannot see the writer's buffered pages or its log, so
        // what it would read is a stale file that a checkpoint may be rewriting
        // underneath it.
        var holds_lock = false;
        if (options.lock) {
            const mode: LockMode = if (options.read_only) .shared else .exclusive;
            const acquired = file.tryLock(mode) catch return PageManagerError.IoError;
            if (!acquired) return PageManagerError.DatabaseLocked;
            holds_lock = true;
        }
        errdefer if (holds_lock) file.unlock();

        var self = Self{
            .allocator = allocator,
            .file = file,
            .header = undefined,
            .page_size = options.page_size,
            .read_only = options.read_only,
            .holds_lock = holds_lock,
            .page_count = .init(0),
        };

        const file_size = file.size() catch return PageManagerError.IoError;

        if (file_size == 0) {
            // Empty file - initialize as new database (like SQLite behavior)
            if (options.read_only) {
                return PageManagerError.InvalidHeader;
            }
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
        // Closing the handle drops the lock on its own, and releasing it first
        // makes that explicit rather than incidental.
        if (self.holds_lock) {
            self.file.unlock();
            self.holds_lock = false;
        }
        self.file.close();
    }

    /// Initialize a new database file.
    fn initNewFile(self: *Self) PageManagerError!void {
        self.header = FileHeader.init();
        self.header.page_size = self.page_size;
        self.header.created_timestamp = @intCast(@import("compat").timestamp());
        self.header.modified_timestamp = self.header.created_timestamp;

        // Generate random UUID
        @import("compat").randomBytes(&self.header.file_uuid);

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
        if (!isValidPageSize(self.page_size)) return PageManagerError.InvalidPageSize;

        const file_size = self.file.size() catch return PageManagerError.IoError;
        if (file_size < self.page_size or file_size % self.page_size != 0) {
            return PageManagerError.InvalidHeader;
        }
        const page_count = std.math.cast(u32, file_size / self.page_size) orelse {
            return PageManagerError.InvalidHeader;
        };
        self.page_count.store(page_count, .release);

        if (!self.read_only and self.header.format_version < FORMAT_VERSION) {
            self.header.format_version = FORMAT_VERSION;
            self.header.min_reader_version = FORMAT_VERSION;
            try self.writeHeader();
        }
    }

    /// Write the file header to disk.
    fn writeHeader(self: *Self) PageManagerError!void {
        if (self.read_only) return PageManagerError.PermissionDenied;

        self.header.modified_timestamp = @intCast(@import("compat").timestamp());

        const page_alignment = comptime std.mem.Alignment.fromByteUnits(4096);
        const buf = self.allocator.alignedAlloc(u8, page_alignment, self.page_size) catch {
            return PageManagerError.OutOfMemory;
        };
        defer self.allocator.free(buf);
        @memset(buf, 0);

        const header_bytes = std.mem.asBytes(&self.header);
        @memcpy(buf[0..header_bytes.len], header_bytes);

        self.file.write(0, buf) catch return PageManagerError.IoError;
        self.noteWritten(0);
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
        const page_alignment = comptime std.mem.Alignment.fromByteUnits(4096);
        const zeros = self.allocator.alignedAlloc(u8, page_alignment, self.page_size) catch {
            return PageManagerError.OutOfMemory;
        };
        defer self.allocator.free(zeros);
        @memset(zeros, 0);

        // Set up as free page initially
        const header_ptr: *PageHeader = @ptrCast(@alignCast(zeros.ptr));
        header_ptr.* = PageHeader.init(.free);

        self.file.write(offset, zeros) catch return PageManagerError.IoError;
        self.noteWritten(page_id);

        try self.writeHeader();

        return page_id;
    }

    /// Allocate a page from the freelist.
    fn allocateFromFreelist(self: *Self) PageManagerError!PageId {
        const page_id = self.header.freelist_page;

        // Read the free page to get next pointer
        const page_alignment = comptime std.mem.Alignment.fromByteUnits(4096);
        const buf = self.allocator.alignedAlloc(u8, page_alignment, self.page_size) catch {
            return PageManagerError.OutOfMemory;
        };
        defer self.allocator.free(buf);
        try self.readPageRaw(page_id, buf);

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

        const page_alignment = comptime std.mem.Alignment.fromByteUnits(4096);
        const buf = self.allocator.alignedAlloc(u8, page_alignment, self.page_size) catch {
            return PageManagerError.OutOfMemory;
        };
        defer self.allocator.free(buf);
        @memset(buf, 0);

        // Set up page header as free
        const header_ptr: *PageHeader = @ptrCast(@alignCast(buf.ptr));
        header_ptr.* = PageHeader.init(.free);

        // Store pointer to current freelist head
        std.mem.writeInt(u32, buf[8..12], self.header.freelist_page, .little);

        // Calculate and set checksum (covers bytes 8 to end)
        header_ptr.checksum = calculateChecksum(buf[8..]);

        // Write the page
        const offset = self.pageOffset(page_id);
        self.file.write(offset, buf) catch return PageManagerError.IoError;
        self.noteWritten(page_id);

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
        // Recovery replays pages past the end of the file when the crash came
        // before the file's growth reached disk, and the write grows it.
        self.noteWritten(page_id);
    }

    /// Sync all changes to disk.
    pub fn sync(self: *Self) PageManagerError!void {
        self.file.sync() catch return PageManagerError.IoError;
    }

    /// Remove contiguous free pages from the physical end of the database.
    ///
    /// Callers must first flush and evict the buffer pool so this method sees
    /// authoritative page contents and no cached frame can outlive truncation.
    /// The retained freelist and header are persisted before the file shrinks;
    /// a crash between those steps can leak tail pages temporarily but cannot
    /// leave a freelist pointer beyond EOF.
    pub fn truncateFreeTail(self: *Self) PageManagerError!TruncateStats {
        if (self.read_only) return PageManagerError.PermissionDenied;

        const pages_before = self.pageCount();
        if (pages_before <= 1) {
            return .{
                .pages_before = pages_before,
                .pages_after = pages_before,
                .pages_removed = 0,
                .bytes_reclaimed = 0,
            };
        }

        const page_alignment = comptime std.mem.Alignment.fromByteUnits(4096);
        const buf = self.allocator.alignedAlloc(u8, page_alignment, self.page_size) catch {
            return PageManagerError.OutOfMemory;
        };
        defer self.allocator.free(buf);

        var tail_start = pages_before;
        while (tail_start > 1) {
            const candidate = tail_start - 1;
            try self.readPage(candidate, buf);
            const header: *const PageHeader = @ptrCast(@alignCast(buf.ptr));
            if (header.page_type != .free) break;
            tail_start = candidate;
        }

        if (tail_start == pages_before) {
            return .{
                .pages_before = pages_before,
                .pages_after = pages_before,
                .pages_removed = 0,
                .bytes_reclaimed = 0,
            };
        }

        // Reconstruct the retained freelist from page types rather than
        // patching arbitrary links in place. This also heals leaked free pages
        // that were not reachable from the previous freelist head.
        var freelist_head = NULL_PAGE;
        var page_id: PageId = 1;
        while (page_id < tail_start) : (page_id += 1) {
            try self.readPage(page_id, buf);
            const header: *PageHeader = @ptrCast(@alignCast(buf.ptr));
            if (header.page_type != .free) continue;

            @memset(buf, 0);
            header.* = PageHeader.init(.free);
            std.mem.writeInt(u32, buf[8..12], freelist_head, .little);
            try self.writePage(page_id, buf);
            freelist_head = page_id;
        }

        self.header.freelist_page = freelist_head;
        try self.writeHeader();
        try self.sync();

        // The tail is gone as far as the database is concerned once the
        // freelist no longer reaches it, so the count drops before the file
        // does. If the truncate then fails, the next allocation reuses the
        // space, and the leftover pages are only as leaked as a crash here
        // would leave them.
        self.page_count.store(tail_start, .release);
        const new_size = @as(u64, tail_start) * self.page_size;
        self.file.truncate(new_size) catch return PageManagerError.IoError;
        try self.sync();

        const pages_removed = pages_before - tail_start;
        return .{
            .pages_before = pages_before,
            .pages_after = tail_start,
            .pages_removed = pages_removed,
            .bytes_reclaimed = @as(u64, pages_removed) * self.page_size,
        };
    }

    /// Get the file header (read-only).
    pub fn getHeader(self: *const Self) *const FileHeader {
        return &self.header;
    }

    /// Update the file header with new values.
    pub fn updateHeader(self: *Self, new_header: *const FileHeader) PageManagerError!void {
        if (self.read_only) return PageManagerError.PermissionDenied;
        self.header = new_header.*;
        try self.writeHeader();
    }

    /// Record that the write-ahead log has been reset, and persist that fact.
    ///
    /// This has to reach disk before the log is actually truncated. A counter
    /// that lags the truncation would let a follower carry on believing it has
    /// frames it no longer has, whereas one that runs ahead only costs a
    /// follower an extra snapshot it did not strictly need.
    pub fn advanceCheckpointSeq(self: *Self) PageManagerError!void {
        if (self.read_only) return PageManagerError.PermissionDenied;
        self.header.checkpoint_seq +%= 1;
        try self.writeHeader();
    }

    /// Get current page count.
    pub fn pageCount(self: *const Self) u32 {
        return self.page_count.load(.acquire);
    }

    /// Record that a page has been written, which grows the file to reach it.
    fn noteWritten(self: *Self, page_id: PageId) void {
        _ = self.page_count.fetchMax(page_id +| 1, .release);
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

test "truncate free tail rebuilds retained freelist and shrinks file" {
    const allocator = std.testing.allocator;

    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();
    const path = "/tmp/lattice_pm_test_truncate_free_tail.db";
    vfs_impl.delete(path) catch {};

    var pm = try PageManager.init(allocator, vfs_impl, path, .{ .create = true });
    defer {
        pm.deinit();
        vfs_impl.delete(path) catch {};
    }

    for (0..6) |_| _ = try pm.allocatePage();

    const page_alignment = comptime std.mem.Alignment.fromByteUnits(4096);
    const live_page = try allocator.alignedAlloc(u8, page_alignment, pm.getPageSize());
    defer allocator.free(live_page);
    @memset(live_page, 0);
    const live_header: *PageHeader = @ptrCast(@alignCast(live_page.ptr));
    live_header.* = PageHeader.init(.btree_leaf);
    for ([_]PageId{ 1, 3, 4 }) |page_id| try pm.writePage(page_id, live_page);

    try pm.freePage(2);
    try pm.freePage(5);
    try pm.freePage(6);

    const stats = try pm.truncateFreeTail();
    try std.testing.expectEqual(@as(u32, 7), stats.pages_before);
    try std.testing.expectEqual(@as(u32, 5), stats.pages_after);
    try std.testing.expectEqual(@as(u32, 2), stats.pages_removed);
    try std.testing.expectEqual(@as(u64, 2 * DEFAULT_PAGE_SIZE), stats.bytes_reclaimed);
    try std.testing.expectEqual(@as(u32, 5), pm.pageCount());

    // The non-tail free page survives reconstruction and is allocated first.
    try std.testing.expectEqual(@as(PageId, 2), try pm.allocatePage());
    // Once the retained freelist is empty, growth resumes at the new EOF.
    try std.testing.expectEqual(@as(PageId, 5), try pm.allocatePage());
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
