//! Virtual File System abstraction for Lattice database.
//!
//! Provides a platform-independent file I/O interface that enables:
//! - Testing with in-memory file systems
//! - Custom storage backends
//! - Portable file operations across platforms

const std = @import("std");
const Allocator = std.mem.Allocator;

/// Errors that can occur during VFS operations.
pub const VfsError = error{
    FileNotFound,
    PermissionDenied,
    DiskFull,
    IoError,
    FileLocked,
    InvalidPath,
    AlreadyExists,
    NotOpenForWriting,
    Unexpected,
};

/// Flags for opening files.
pub const OpenFlags = struct {
    read: bool = true,
    write: bool = false,
    create: bool = false,
    /// Fail if file exists when creating
    exclusive: bool = false,
    /// Truncate file to zero length
    truncate: bool = false,
};

/// Lock mode for file locking.
pub const LockMode = enum {
    /// Shared lock - multiple readers allowed
    shared,
    /// Exclusive lock - single writer
    exclusive,
};

/// Abstract file handle.
pub const File = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        /// Read bytes from file at offset.
        read: *const fn (ptr: *anyopaque, offset: u64, buf: []u8) VfsError!usize,
        /// Write bytes to file at offset.
        write: *const fn (ptr: *anyopaque, offset: u64, data: []const u8) VfsError!void,
        /// Sync file to disk (fsync).
        sync: *const fn (ptr: *anyopaque) VfsError!void,
        /// Truncate or extend file to size.
        truncate: *const fn (ptr: *anyopaque, size: u64) VfsError!void,
        /// Get current file size.
        size: *const fn (ptr: *anyopaque) VfsError!u64,
        /// Close file handle.
        close: *const fn (ptr: *anyopaque) void,
        /// Acquire file lock.
        lock: *const fn (ptr: *anyopaque, mode: LockMode) VfsError!void,
        /// Release file lock.
        unlock: *const fn (ptr: *anyopaque) void,
    };

    /// Read bytes from file at offset.
    pub fn read(self: File, offset: u64, buf: []u8) VfsError!usize {
        return self.vtable.read(self.ptr, offset, buf);
    }

    /// Write bytes to file at offset.
    pub fn write(self: File, offset: u64, data: []const u8) VfsError!void {
        return self.vtable.write(self.ptr, offset, data);
    }

    /// Sync file to disk.
    pub fn sync(self: File) VfsError!void {
        return self.vtable.sync(self.ptr);
    }

    /// Truncate or extend file to size.
    pub fn truncate(self: File, new_size: u64) VfsError!void {
        return self.vtable.truncate(self.ptr, new_size);
    }

    /// Get current file size.
    pub fn size(self: File) VfsError!u64 {
        return self.vtable.size(self.ptr);
    }

    /// Close file handle.
    pub fn close(self: File) void {
        return self.vtable.close(self.ptr);
    }

    /// Acquire file lock.
    pub fn lock(self: File, mode: LockMode) VfsError!void {
        return self.vtable.lock(self.ptr, mode);
    }

    /// Release file lock.
    pub fn unlock(self: File) void {
        return self.vtable.unlock(self.ptr);
    }
};

/// Abstract virtual file system.
pub const Vfs = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        /// Open a file.
        open: *const fn (ptr: *anyopaque, path: []const u8, flags: OpenFlags) VfsError!File,
        /// Delete a file.
        delete: *const fn (ptr: *anyopaque, path: []const u8) VfsError!void,
        /// Check if file exists.
        exists: *const fn (ptr: *anyopaque, path: []const u8) bool,
    };

    /// Open a file with the given flags.
    pub fn open(self: Vfs, path: []const u8, flags: OpenFlags) VfsError!File {
        return self.vtable.open(self.ptr, path, flags);
    }

    /// Delete a file.
    pub fn delete(self: Vfs, path: []const u8) VfsError!void {
        return self.vtable.delete(self.ptr, path);
    }

    /// Check if a file exists.
    pub fn exists(self: Vfs, path: []const u8) bool {
        return self.vtable.exists(self.ptr, path);
    }
};

// ============================================================================
// POSIX Implementation
// ============================================================================

/// POSIX-based VFS implementation.
pub const PosixVfs = struct {
    allocator: Allocator,

    const Self = @This();

    /// Initialize a POSIX VFS.
    pub fn init(allocator: Allocator) Self {
        return .{ .allocator = allocator };
    }

    /// Get the Vfs interface for this PosixVfs.
    pub fn vfs(self: *Self) Vfs {
        return .{
            .ptr = self,
            .vtable = &vtable,
        };
    }

    const vtable = Vfs.VTable{
        .open = vfsOpen,
        .delete = vfsDelete,
        .exists = vfsExists,
    };

    fn vfsOpen(ptr: *anyopaque, path: []const u8, flags: OpenFlags) VfsError!File {
        const self: *Self = @ptrCast(@alignCast(ptr));
        return self.openFile(path, flags);
    }

    fn vfsDelete(ptr: *anyopaque, path: []const u8) VfsError!void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        return self.deleteFile(path);
    }

    fn vfsExists(ptr: *anyopaque, path: []const u8) bool {
        const self: *Self = @ptrCast(@alignCast(ptr));
        return self.fileExists(path);
    }

    /// Open a file.
    pub fn openFile(self: *Self, path: []const u8, flags: OpenFlags) VfsError!File {
        // Build POSIX open flags
        var posix_flags: std.posix.O = .{};

        if (flags.read and flags.write) {
            posix_flags.ACCMODE = .RDWR;
        } else if (flags.write) {
            posix_flags.ACCMODE = .WRONLY;
        } else {
            posix_flags.ACCMODE = .RDONLY;
        }

        if (flags.create) {
            posix_flags.CREAT = true;
        }
        if (flags.exclusive) {
            posix_flags.EXCL = true;
        }
        if (flags.truncate) {
            posix_flags.TRUNC = true;
        }

        // Need null-terminated path for POSIX
        const path_z = std.posix.toPosixPath(path) catch return VfsError.InvalidPath;

        const fd = std.posix.openatZ(std.posix.AT.FDCWD, &path_z, posix_flags, 0o644) catch |err| {
            return switch (err) {
                error.FileNotFound => VfsError.FileNotFound,
                error.AccessDenied => VfsError.PermissionDenied,
                error.PathAlreadyExists => VfsError.AlreadyExists,
                error.NoSpaceLeft => VfsError.DiskFull,
                else => VfsError.IoError,
            };
        };

        const file = self.allocator.create(PosixFile) catch return VfsError.Unexpected;
        file.* = .{
            .fd = fd,
            .allocator = self.allocator,
            .writable = flags.write,
        };

        return file.file();
    }

    /// Delete a file.
    pub fn deleteFile(self: *Self, path: []const u8) VfsError!void {
        _ = self;
        const path_z = std.posix.toPosixPath(path) catch return VfsError.InvalidPath;

        std.posix.unlinkatZ(std.posix.AT.FDCWD, &path_z, 0) catch |err| {
            return switch (err) {
                error.FileNotFound => VfsError.FileNotFound,
                error.AccessDenied => VfsError.PermissionDenied,
                else => VfsError.IoError,
            };
        };
    }

    /// Check if a file exists.
    pub fn fileExists(self: *Self, path: []const u8) bool {
        _ = self;
        const path_z = std.posix.toPosixPath(path) catch return false;
        _ = std.posix.fstatatZ(std.posix.AT.FDCWD, &path_z, 0) catch return false;
        return true;
    }
};

/// POSIX file handle.
pub const PosixFile = struct {
    fd: std.posix.fd_t,
    allocator: Allocator,
    writable: bool,

    const Self = @This();

    /// Get the File interface for this PosixFile.
    pub fn file(self: *Self) File {
        return .{
            .ptr = self,
            .vtable = &vtable,
        };
    }

    const vtable = File.VTable{
        .read = fileRead,
        .write = fileWrite,
        .sync = fileSync,
        .truncate = fileTruncate,
        .size = fileSize,
        .close = fileClose,
        .lock = fileLock,
        .unlock = fileUnlock,
    };

    fn fileRead(ptr: *anyopaque, offset: u64, buf: []u8) VfsError!usize {
        const self: *Self = @ptrCast(@alignCast(ptr));
        return self.readAt(offset, buf);
    }

    fn fileWrite(ptr: *anyopaque, offset: u64, data: []const u8) VfsError!void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        return self.writeAt(offset, data);
    }

    fn fileSync(ptr: *anyopaque) VfsError!void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        return self.syncFile();
    }

    fn fileTruncate(ptr: *anyopaque, new_size: u64) VfsError!void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        return self.truncateFile(new_size);
    }

    fn fileSize(ptr: *anyopaque) VfsError!u64 {
        const self: *Self = @ptrCast(@alignCast(ptr));
        return self.getSize();
    }

    fn fileClose(ptr: *anyopaque) void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        self.closeFile();
    }

    fn fileLock(ptr: *anyopaque, mode: LockMode) VfsError!void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        return self.lockFile(mode);
    }

    fn fileUnlock(ptr: *anyopaque) void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        self.unlockFile();
    }

    /// Read from file at offset.
    pub fn readAt(self: *Self, offset: u64, buf: []u8) VfsError!usize {
        const n = std.posix.pread(self.fd, buf, offset) catch |err| {
            return switch (err) {
                error.InputOutput => VfsError.IoError,
                else => VfsError.Unexpected,
            };
        };
        return n;
    }

    /// Write to file at offset.
    pub fn writeAt(self: *Self, offset: u64, data: []const u8) VfsError!void {
        if (!self.writable) return VfsError.NotOpenForWriting;

        var written: usize = 0;
        while (written < data.len) {
            const n = std.posix.pwrite(self.fd, data[written..], offset + written) catch |err| {
                return switch (err) {
                    error.InputOutput => VfsError.IoError,
                    error.NoSpaceLeft => VfsError.DiskFull,
                    else => VfsError.Unexpected,
                };
            };
            if (n == 0) return VfsError.IoError;
            written += n;
        }
    }

    /// Sync file to disk.
    pub fn syncFile(self: *Self) VfsError!void {
        std.posix.fsync(self.fd) catch |err| {
            return switch (err) {
                error.InputOutput => VfsError.IoError,
                else => VfsError.Unexpected,
            };
        };
    }

    /// Truncate file to size.
    pub fn truncateFile(self: *Self, new_size: u64) VfsError!void {
        if (!self.writable) return VfsError.NotOpenForWriting;

        std.posix.ftruncate(self.fd, @intCast(new_size)) catch |err| {
            return switch (err) {
                error.AccessDenied, error.PermissionDenied => VfsError.PermissionDenied,
                else => VfsError.Unexpected,
            };
        };
    }

    /// Get file size.
    pub fn getSize(self: *Self) VfsError!u64 {
        const stat = std.posix.fstat(self.fd) catch return VfsError.IoError;
        return @intCast(stat.size);
    }

    /// Close file.
    pub fn closeFile(self: *Self) void {
        std.posix.close(self.fd);
        self.allocator.destroy(self);
    }

    /// Lock file.
    pub fn lockFile(self: *Self, mode: LockMode) VfsError!void {
        const lock_type: i16 = switch (mode) {
            .shared => std.c.F.RDLCK,
            .exclusive => std.c.F.WRLCK,
        };

        var flock = std.c.Flock{
            .type = lock_type,
            .whence = 0, // SEEK_SET
            .start = 0,
            .len = 0, // Entire file
            .pid = 0,
        };

        _ = std.posix.fcntl(self.fd, std.c.F.SETLKW, @intFromPtr(&flock)) catch |err| {
            return switch (err) {
                error.DeadLock => VfsError.FileLocked,
                else => VfsError.Unexpected,
            };
        };
    }

    /// Unlock file.
    pub fn unlockFile(self: *Self) void {
        var flock = std.c.Flock{
            .type = std.c.F.UNLCK,
            .whence = 0, // SEEK_SET
            .start = 0,
            .len = 0,
            .pid = 0,
        };

        _ = std.posix.fcntl(self.fd, std.c.F.SETLK, @intFromPtr(&flock)) catch {};
    }
};

/// Get the default VFS for the current platform.
pub fn defaultVfs(allocator: Allocator) PosixVfs {
    return PosixVfs.init(allocator);
}

// ============================================================================
// Tests
// ============================================================================

test "open and close file" {
    const allocator = std.testing.allocator;
    var posix_vfs = PosixVfs.init(allocator);
    var vfs_instance = posix_vfs.vfs();

    // Create a temp file
    const path = "/tmp/lattice_vfs_test.db";

    // Clean up any existing file
    vfs_instance.delete(path) catch {};

    // Open for write + create
    const file = try vfs_instance.open(path, .{ .read = true, .write = true, .create = true });
    defer {
        file.close();
        vfs_instance.delete(path) catch {};
    }

    try std.testing.expect(vfs_instance.exists(path));
}

test "read and write at offset" {
    const allocator = std.testing.allocator;
    var posix_vfs = PosixVfs.init(allocator);
    var vfs_instance = posix_vfs.vfs();

    const path = "/tmp/lattice_vfs_test_rw.db";
    vfs_instance.delete(path) catch {};

    const file = try vfs_instance.open(path, .{ .read = true, .write = true, .create = true });
    defer {
        file.close();
        vfs_instance.delete(path) catch {};
    }

    // Write at offset 0
    const data1 = "Hello, ";
    try file.write(0, data1);

    // Write at offset 7
    const data2 = "World!";
    try file.write(7, data2);

    // Read back
    var buf: [13]u8 = undefined;
    const n = try file.read(0, &buf);
    try std.testing.expectEqual(@as(usize, 13), n);
    try std.testing.expectEqualStrings("Hello, World!", buf[0..n]);
}

test "file size" {
    const allocator = std.testing.allocator;
    var posix_vfs = PosixVfs.init(allocator);
    var vfs_instance = posix_vfs.vfs();

    const path = "/tmp/lattice_vfs_test_size.db";
    vfs_instance.delete(path) catch {};

    const file = try vfs_instance.open(path, .{ .read = true, .write = true, .create = true });
    defer {
        file.close();
        vfs_instance.delete(path) catch {};
    }

    // Initial size should be 0
    try std.testing.expectEqual(@as(u64, 0), try file.size());

    // Write some data
    try file.write(0, "12345678901234567890");

    // Size should now be 20
    try std.testing.expectEqual(@as(u64, 20), try file.size());
}

test "truncate file" {
    const allocator = std.testing.allocator;
    var posix_vfs = PosixVfs.init(allocator);
    var vfs_instance = posix_vfs.vfs();

    const path = "/tmp/lattice_vfs_test_truncate.db";
    vfs_instance.delete(path) catch {};

    const file = try vfs_instance.open(path, .{ .read = true, .write = true, .create = true });
    defer {
        file.close();
        vfs_instance.delete(path) catch {};
    }

    // Write data
    try file.write(0, "Hello, World!");
    try std.testing.expectEqual(@as(u64, 13), try file.size());

    // Truncate to 5 bytes
    try file.truncate(5);
    try std.testing.expectEqual(@as(u64, 5), try file.size());

    // Read back
    var buf: [10]u8 = undefined;
    const n = try file.read(0, &buf);
    try std.testing.expectEqual(@as(usize, 5), n);
    try std.testing.expectEqualStrings("Hello", buf[0..n]);
}

test "file exists" {
    const allocator = std.testing.allocator;
    var posix_vfs = PosixVfs.init(allocator);
    var vfs_instance = posix_vfs.vfs();

    const path = "/tmp/lattice_vfs_test_exists.db";
    vfs_instance.delete(path) catch {};

    // Should not exist yet
    try std.testing.expect(!vfs_instance.exists(path));

    // Create file
    const file = try vfs_instance.open(path, .{ .read = true, .write = true, .create = true });
    file.close();

    // Now should exist
    try std.testing.expect(vfs_instance.exists(path));

    // Delete
    try vfs_instance.delete(path);

    // Should not exist again
    try std.testing.expect(!vfs_instance.exists(path));
}

test "exclusive create fails if exists" {
    const allocator = std.testing.allocator;
    var posix_vfs = PosixVfs.init(allocator);
    var vfs_instance = posix_vfs.vfs();

    const path = "/tmp/lattice_vfs_test_excl.db";
    vfs_instance.delete(path) catch {};

    // Create file first time
    const file1 = try vfs_instance.open(path, .{ .read = true, .write = true, .create = true, .exclusive = true });
    file1.close();

    // Try to create again with exclusive - should fail
    const result = vfs_instance.open(path, .{ .read = true, .write = true, .create = true, .exclusive = true });
    try std.testing.expectError(VfsError.AlreadyExists, result);

    vfs_instance.delete(path) catch {};
}

test "file not found" {
    const allocator = std.testing.allocator;
    var posix_vfs = PosixVfs.init(allocator);
    var vfs_instance = posix_vfs.vfs();

    const result = vfs_instance.open("/tmp/nonexistent_file_12345.db", .{ .read = true });
    try std.testing.expectError(VfsError.FileNotFound, result);
}
