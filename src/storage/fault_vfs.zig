//! A filesystem in memory that knows what a power cut can leave behind.
//!
//! The crash tests used to simulate a killed process: copy the files of a live
//! database, or cut the database file back. A process kill loses nothing the
//! operating system was handed, so it never tests the case the storage design
//! is argued for, which is the machine losing power with writes still in its
//! caches.
//!
//! This VFS records every change made through it, in order: files created and
//! deleted, writes, truncations, file syncs and directory syncs. A `DiskModel`
//! replays any prefix of that record and keeps, for each file, what has been
//! synced and the changes since. Cutting the power then means choosing a state
//! a real disk could hold at that moment:
//!
//! - everything synced is there;
//! - of the changes since the file's last sync, any subset, applied in any
//!   order, and a write can be torn at any sector boundary, so part of it
//!   lands and part does not;
//! - a truncation may not have happened;
//! - a file's length can have grown without the data that grew it, which then
//!   reads as zeroes;
//! - a name created or removed since the directory was last synced may or may
//!   not be in the directory.
//!
//! The choice is driven by a seeded random generator, so a failing case can be
//! replayed exactly. A state chosen this way is an `Image`, and a new
//! `FaultVfs` can be opened on it, with everything in it synced, to run the
//! next open, and record that too.
//!
//! It is one directory: every path names a file in it, and a directory sync
//! covers all of them. Locks always succeed.

const std = @import("std");
const vfs_mod = @import("vfs.zig");

const Allocator = std.mem.Allocator;
const VfsError = vfs_mod.VfsError;
const OpenFlags = vfs_mod.OpenFlags;
const LockMode = vfs_mod.LockMode;

/// The unit a disk writes atomically, at best.
pub const SECTOR_SIZE: u64 = 512;

/// One change made through the filesystem.
pub const Op = union(enum) {
    /// `name` now leads to a new, empty file, `inode`.
    create: struct { name: []const u8, inode: u32 },
    /// `name` no longer leads anywhere.
    delete: struct { name: []const u8 },
    write: struct { inode: u32, offset: u64, data: []const u8 },
    truncate: struct { inode: u32, size: u64 },
    /// Everything written to `inode` so far is on the disk.
    sync: struct { inode: u32 },
    /// The directory's names, as they are now, are on the disk.
    sync_dir,

    pub fn isSync(self: Op) bool {
        return switch (self) {
            .sync, .sync_dir => true,
            else => false,
        };
    }

    fn deinit(self: Op, allocator: Allocator) void {
        switch (self) {
            .create => |c| allocator.free(c.name),
            .delete => |d| allocator.free(d.name),
            .write => |w| allocator.free(w.data),
            else => {},
        }
    }
};

/// The files a disk holds at one moment, by name, sorted.
pub const Image = struct {
    files: []File,

    pub const File = struct {
        name: []u8,
        data: []u8,
    };

    pub fn deinit(self: *Image, allocator: Allocator) void {
        for (self.files) |file| {
            allocator.free(file.name);
            allocator.free(file.data);
        }
        allocator.free(self.files);
        self.* = undefined;
    }

    pub fn get(self: *const Image, name: []const u8) ?[]const u8 {
        for (self.files) |file| {
            if (std.mem.eql(u8, file.name, name)) return file.data;
        }
        return null;
    }

    pub fn clone(self: *const Image, allocator: Allocator) Allocator.Error!Image {
        var files: std.ArrayListUnmanaged(File) = .empty;
        errdefer {
            for (files.items) |file| {
                allocator.free(file.name);
                allocator.free(file.data);
            }
            files.deinit(allocator);
        }
        for (self.files) |file| try appendFile(allocator, &files, file.name, file.data);
        return .{ .files = try files.toOwnedSlice(allocator) };
    }
};

fn appendFile(allocator: Allocator, files: *std.ArrayListUnmanaged(Image.File), name: []const u8, data: []const u8) Allocator.Error!void {
    const name_copy = try allocator.dupe(u8, name);
    errdefer allocator.free(name_copy);
    const data_copy = try allocator.dupe(u8, data);
    errdefer allocator.free(data_copy);
    try files.append(allocator, .{ .name = name_copy, .data = data_copy });
}

fn imageFileLessThan(_: void, a: Image.File, b: Image.File) bool {
    return std.mem.lessThan(u8, a.name, b.name);
}

fn writeInto(allocator: Allocator, bytes: *std.ArrayListUnmanaged(u8), offset: u64, data: []const u8) Allocator.Error!void {
    const end: usize = @intCast(offset + data.len);
    if (bytes.items.len < end) {
        const old = bytes.items.len;
        try bytes.resize(allocator, end);
        @memset(bytes.items[old..end], 0);
    }
    @memcpy(bytes.items[@intCast(offset)..end], data);
}

fn resizeTo(allocator: Allocator, bytes: *std.ArrayListUnmanaged(u8), size: u64) Allocator.Error!void {
    const new_len: usize = @intCast(size);
    const old = bytes.items.len;
    try bytes.resize(allocator, new_len);
    if (new_len > old) @memset(bytes.items[old..new_len], 0);
}

// ============================================================================
// The filesystem a database runs on
// ============================================================================

pub const FaultVfs = struct {
    allocator: Allocator,
    mutex: @import("compat").Mutex = .{},
    /// File contents, indexed by inode number. Files are never freed: a
    /// deleted file's inode stays, as it might on a disk that lost the delete.
    inodes: std.ArrayListUnmanaged(std.ArrayListUnmanaged(u8)) = .empty,
    names: std.StringHashMapUnmanaged(u32) = .empty,
    /// Every change, in order.
    trace: std.ArrayListUnmanaged(Op) = .empty,

    const Self = @This();

    pub fn init(allocator: Allocator) Self {
        return .{ .allocator = allocator };
    }

    /// A filesystem holding `image`, all of it synced. Inode `i` is
    /// `image.files[i]`, which is what `DiskModel.initFromImage` assumes.
    pub fn initFromImage(allocator: Allocator, image: *const Image) Allocator.Error!Self {
        var self = Self.init(allocator);
        errdefer self.deinit();
        for (image.files, 0..) |file, i| {
            var bytes: std.ArrayListUnmanaged(u8) = .empty;
            errdefer bytes.deinit(allocator);
            try bytes.appendSlice(allocator, file.data);
            try self.inodes.append(allocator, bytes);
            const key = try allocator.dupe(u8, file.name);
            errdefer allocator.free(key);
            try self.names.put(allocator, key, @intCast(i));
        }
        return self;
    }

    pub fn deinit(self: *Self) void {
        for (self.inodes.items) |*bytes| bytes.deinit(self.allocator);
        self.inodes.deinit(self.allocator);
        var it = self.names.keyIterator();
        while (it.next()) |key| self.allocator.free(key.*);
        self.names.deinit(self.allocator);
        for (self.trace.items) |op| op.deinit(self.allocator);
        self.trace.deinit(self.allocator);
        self.* = undefined;
    }

    pub fn vfs(self: *Self) vfs_mod.Vfs {
        return .{ .ptr = self, .vtable = &vtable };
    }

    /// How many changes have been made. A position in the trace.
    pub fn traceLen(self: *Self) usize {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.trace.items.len;
    }

    /// The current contents of a file, or null if no such name.
    pub fn contents(self: *Self, name: []const u8) ?[]const u8 {
        self.mutex.lock();
        defer self.mutex.unlock();
        const inode = self.names.get(name) orelse return null;
        return self.inodes.items[inode].items;
    }

    const vtable = vfs_mod.Vfs.VTable{
        .open = vfsOpen,
        .delete = vfsDelete,
        .exists = vfsExists,
        .syncParentDir = vfsSyncParentDir,
    };

    fn from(ptr: *anyopaque) *Self {
        return @ptrCast(@alignCast(ptr));
    }

    fn record(self: *Self, op: Op) VfsError!void {
        self.trace.append(self.allocator, op) catch {
            op.deinit(self.allocator);
            return VfsError.Unexpected;
        };
    }

    fn vfsOpen(ptr: *anyopaque, path: []const u8, flags: OpenFlags) VfsError!vfs_mod.File {
        const self = from(ptr);
        self.mutex.lock();
        defer self.mutex.unlock();

        const inode: u32 = if (self.names.get(path)) |existing| blk: {
            if (flags.create and flags.exclusive) return VfsError.AlreadyExists;
            if (flags.truncate and flags.write) {
                try self.record(.{ .truncate = .{ .inode = existing, .size = 0 } });
                self.inodes.items[existing].clearRetainingCapacity();
            }
            break :blk existing;
        } else blk: {
            if (!flags.create) return VfsError.FileNotFound;
            const id: u32 = @intCast(self.inodes.items.len);
            self.inodes.append(self.allocator, .empty) catch return VfsError.Unexpected;
            const key = self.allocator.dupe(u8, path) catch return VfsError.Unexpected;
            self.names.put(self.allocator, key, id) catch {
                self.allocator.free(key);
                return VfsError.Unexpected;
            };
            const name = self.allocator.dupe(u8, path) catch return VfsError.Unexpected;
            try self.record(.{ .create = .{ .name = name, .inode = id } });
            break :blk id;
        };

        const handle = self.allocator.create(FaultFile) catch return VfsError.Unexpected;
        handle.* = .{ .owner = self, .inode = inode, .writable = flags.write };
        return handle.file();
    }

    fn vfsDelete(ptr: *anyopaque, path: []const u8) VfsError!void {
        const self = from(ptr);
        self.mutex.lock();
        defer self.mutex.unlock();
        const entry = self.names.fetchRemove(path) orelse return VfsError.FileNotFound;
        self.allocator.free(entry.key);
        const name = self.allocator.dupe(u8, path) catch return VfsError.Unexpected;
        try self.record(.{ .delete = .{ .name = name } });
    }

    fn vfsExists(ptr: *anyopaque, path: []const u8) bool {
        const self = from(ptr);
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.names.contains(path);
    }

    fn vfsSyncParentDir(ptr: *anyopaque, path: []const u8) void {
        _ = path;
        const self = from(ptr);
        self.mutex.lock();
        defer self.mutex.unlock();
        self.record(.sync_dir) catch {};
    }
};

pub const FaultFile = struct {
    owner: *FaultVfs,
    inode: u32,
    writable: bool,

    const Self = @This();

    pub fn file(self: *Self) vfs_mod.File {
        return .{ .ptr = self, .vtable = &vtable };
    }

    const vtable = vfs_mod.File.VTable{
        .read = fileRead,
        .write = fileWrite,
        .sync = fileSync,
        .truncate = fileTruncate,
        .size = fileSize,
        .close = fileClose,
        .lock = fileLock,
        .tryLock = fileTryLock,
        .unlock = fileUnlock,
    };

    fn from(ptr: *anyopaque) *Self {
        return @ptrCast(@alignCast(ptr));
    }

    fn bytes(self: *Self) *std.ArrayListUnmanaged(u8) {
        return &self.owner.inodes.items[self.inode];
    }

    fn fileRead(ptr: *anyopaque, offset: u64, buf: []u8) VfsError!usize {
        const self = from(ptr);
        self.owner.mutex.lock();
        defer self.owner.mutex.unlock();
        const data = self.bytes().items;
        if (offset >= data.len) return 0;
        const n = @min(buf.len, data.len - @as(usize, @intCast(offset)));
        @memcpy(buf[0..n], data[@intCast(offset)..][0..n]);
        return n;
    }

    fn fileWrite(ptr: *anyopaque, offset: u64, data: []const u8) VfsError!void {
        const self = from(ptr);
        if (!self.writable) return VfsError.NotOpenForWriting;
        if (data.len == 0) return;
        const owner = self.owner;
        owner.mutex.lock();
        defer owner.mutex.unlock();
        const copy = owner.allocator.dupe(u8, data) catch return VfsError.Unexpected;
        try owner.record(.{ .write = .{ .inode = self.inode, .offset = offset, .data = copy } });
        writeInto(owner.allocator, self.bytes(), offset, data) catch return VfsError.Unexpected;
    }

    fn fileSync(ptr: *anyopaque) VfsError!void {
        const self = from(ptr);
        self.owner.mutex.lock();
        defer self.owner.mutex.unlock();
        try self.owner.record(.{ .sync = .{ .inode = self.inode } });
    }

    fn fileTruncate(ptr: *anyopaque, new_size: u64) VfsError!void {
        const self = from(ptr);
        if (!self.writable) return VfsError.NotOpenForWriting;
        const owner = self.owner;
        owner.mutex.lock();
        defer owner.mutex.unlock();
        try owner.record(.{ .truncate = .{ .inode = self.inode, .size = new_size } });
        resizeTo(owner.allocator, self.bytes(), new_size) catch return VfsError.Unexpected;
    }

    fn fileSize(ptr: *anyopaque) VfsError!u64 {
        const self = from(ptr);
        self.owner.mutex.lock();
        defer self.owner.mutex.unlock();
        return self.bytes().items.len;
    }

    fn fileClose(ptr: *anyopaque) void {
        const self = from(ptr);
        self.owner.allocator.destroy(self);
    }

    fn fileLock(ptr: *anyopaque, mode: LockMode) VfsError!void {
        _ = ptr;
        _ = mode;
    }

    fn fileTryLock(ptr: *anyopaque, mode: LockMode) VfsError!bool {
        _ = ptr;
        _ = mode;
        return true;
    }

    fn fileUnlock(ptr: *anyopaque) void {
        _ = ptr;
    }
};

// ============================================================================
// What the disk holds
// ============================================================================

/// Replays a trace and keeps, for each file, what is synced and what is not.
///
/// The pending changes point into the trace they came from, which has to
/// outlive the model.
pub const DiskModel = struct {
    allocator: Allocator,
    inodes: std.ArrayListUnmanaged(Inode) = .empty,
    /// The names as of the last directory sync.
    durable_names: std.StringHashMapUnmanaged(u32) = .empty,
    /// The names as the running process sees them.
    current_names: std.StringHashMapUnmanaged(u32) = .empty,

    const Pending = union(enum) {
        write: struct { offset: u64, data: []const u8 },
        truncate: u64,
    };

    const Inode = struct {
        durable: std.ArrayListUnmanaged(u8) = .empty,
        pending: std.ArrayListUnmanaged(Pending) = .empty,
    };

    /// How the disk behaved when the power went.
    pub const Style = enum {
        /// Everything written made it, as after a process kill.
        everything,
        /// Changes landed in the order they were made, up to some point,
        /// and the one at that point is torn.
        in_order,
        /// Any subset of the changes, in any order, the last one torn.
        reordered,
        /// Any subset of the sectors written, in any order.
        sectors,
    };

    const Self = @This();

    pub fn init(allocator: Allocator) Self {
        return .{ .allocator = allocator };
    }

    /// The disk holding `image`, all synced. Inode `i` is `image.files[i]`,
    /// as in `FaultVfs.initFromImage`.
    pub fn initFromImage(allocator: Allocator, image: *const Image) Allocator.Error!Self {
        var self = Self.init(allocator);
        errdefer self.deinit();
        for (image.files, 0..) |file, i| {
            var inode = Inode{};
            errdefer inode.durable.deinit(allocator);
            try inode.durable.appendSlice(allocator, file.data);
            try self.inodes.append(allocator, inode);
            try self.bind(&self.durable_names, file.name, @intCast(i));
            try self.bind(&self.current_names, file.name, @intCast(i));
        }
        return self;
    }

    pub fn deinit(self: *Self) void {
        for (self.inodes.items) |*inode| {
            inode.durable.deinit(self.allocator);
            inode.pending.deinit(self.allocator);
        }
        self.inodes.deinit(self.allocator);
        freeNames(self.allocator, &self.durable_names);
        freeNames(self.allocator, &self.current_names);
        self.* = undefined;
    }

    fn freeNames(allocator: Allocator, names: *std.StringHashMapUnmanaged(u32)) void {
        var it = names.keyIterator();
        while (it.next()) |key| allocator.free(key.*);
        names.deinit(allocator);
    }

    fn bind(self: *Self, names: *std.StringHashMapUnmanaged(u32), name: []const u8, inode: u32) Allocator.Error!void {
        if (names.getPtr(name)) |existing| {
            existing.* = inode;
            return;
        }
        const key = try self.allocator.dupe(u8, name);
        errdefer self.allocator.free(key);
        try names.put(self.allocator, key, inode);
    }

    fn unbind(self: *Self, names: *std.StringHashMapUnmanaged(u32), name: []const u8) void {
        if (names.fetchRemove(name)) |entry| self.allocator.free(entry.key);
    }

    fn inodeAt(self: *Self, id: u32) Allocator.Error!*Inode {
        while (self.inodes.items.len <= id) try self.inodes.append(self.allocator, .{});
        return &self.inodes.items[id];
    }

    /// Apply one recorded change.
    pub fn apply(self: *Self, op: Op) Allocator.Error!void {
        switch (op) {
            .create => |c| {
                _ = try self.inodeAt(c.inode);
                try self.bind(&self.current_names, c.name, c.inode);
            },
            .delete => |d| self.unbind(&self.current_names, d.name),
            .write => |w| {
                const inode = try self.inodeAt(w.inode);
                try inode.pending.append(self.allocator, .{ .write = .{ .offset = w.offset, .data = w.data } });
            },
            .truncate => |t| {
                const inode = try self.inodeAt(t.inode);
                try inode.pending.append(self.allocator, .{ .truncate = t.size });
            },
            .sync => |s| {
                const inode = try self.inodeAt(s.inode);
                for (inode.pending.items) |pending| try applyPending(self.allocator, &inode.durable, pending);
                inode.pending.clearRetainingCapacity();
            },
            .sync_dir => {
                freeNames(self.allocator, &self.durable_names);
                self.durable_names = .empty;
                var it = self.current_names.iterator();
                while (it.next()) |entry| try self.bind(&self.durable_names, entry.key_ptr.*, entry.value_ptr.*);
            },
        }
    }

    pub fn applyAll(self: *Self, ops: []const Op) Allocator.Error!void {
        for (ops) |op| try self.apply(op);
    }

    fn applyPending(allocator: Allocator, bytes: *std.ArrayListUnmanaged(u8), pending: Pending) Allocator.Error!void {
        switch (pending) {
            .write => |w| try writeInto(allocator, bytes, w.offset, w.data),
            .truncate => |size| try resizeTo(allocator, bytes, size),
        }
    }

    /// Whether anything is not yet on the disk.
    pub fn hasUnsynced(self: *const Self) bool {
        for (self.inodes.items) |inode| {
            if (inode.pending.items.len > 0) return true;
        }
        return !namesEqual(&self.durable_names, &self.current_names);
    }

    fn namesEqual(a: *const std.StringHashMapUnmanaged(u32), b: *const std.StringHashMapUnmanaged(u32)) bool {
        if (a.count() != b.count()) return false;
        var it = a.iterator();
        while (it.next()) |entry| {
            const other = b.get(entry.key_ptr.*) orelse return false;
            if (other != entry.value_ptr.*) return false;
        }
        return true;
    }

    /// Cut the power: choose a state the disk could be in, and return it.
    pub fn crash(self: *Self, random: std.Random, style: Style) Allocator.Error!Image {
        const allocator = self.allocator;

        // Each file's contents after the cut.
        const crashed = try allocator.alloc(std.ArrayListUnmanaged(u8), self.inodes.items.len);
        for (crashed) |*bytes| bytes.* = .empty;
        defer {
            for (crashed) |*bytes| bytes.deinit(allocator);
            allocator.free(crashed);
        }
        for (self.inodes.items, crashed) |*inode, *out| {
            try out.appendSlice(allocator, inode.durable.items);
            try crashFile(allocator, random, style, inode.pending.items, out);
        }

        // Which name leads where.
        var files: std.ArrayListUnmanaged(Image.File) = .empty;
        errdefer {
            for (files.items) |file| {
                allocator.free(file.name);
                allocator.free(file.data);
            }
            files.deinit(allocator);
        }
        var seen: std.StringHashMapUnmanaged(void) = .empty;
        defer seen.deinit(allocator);
        for ([_]*std.StringHashMapUnmanaged(u32){ &self.current_names, &self.durable_names }) |names| {
            var it = names.keyIterator();
            while (it.next()) |key| {
                const name = key.*;
                if (seen.contains(name)) continue;
                try seen.put(allocator, name, {});
                const now = self.current_names.get(name);
                const before = self.durable_names.get(name);
                const chosen = if (now == before or style == .everything)
                    now
                else if (random.boolean()) now else before;
                if (chosen) |inode| try appendFile(allocator, &files, name, crashed[inode].items);
            }
        }
        std.mem.sort(Image.File, files.items, {}, imageFileLessThan);
        return .{ .files = try files.toOwnedSlice(allocator) };
    }

    fn crashFile(
        allocator: Allocator,
        random: std.Random,
        style: Style,
        pending: []const Pending,
        out: *std.ArrayListUnmanaged(u8),
    ) Allocator.Error!void {
        if (pending.len == 0) return;

        // How long the file would be if everything had landed, for a length
        // that grew without its data.
        var full_len: u64 = out.items.len;
        for (pending) |p| switch (p) {
            .write => |w| full_len = @max(full_len, w.offset + w.data.len),
            .truncate => |size| full_len = size,
        };

        var pieces: std.ArrayListUnmanaged(Pending) = .empty;
        defer pieces.deinit(allocator);

        switch (style) {
            .everything => try pieces.appendSlice(allocator, pending),
            .in_order => {
                const landed = random.uintAtMost(usize, pending.len);
                try pieces.appendSlice(allocator, pending[0..landed]);
                if (landed < pending.len) {
                    // The next one, torn: some leading sectors of it.
                    switch (pending[landed]) {
                        .write => |w| {
                            var sectors: std.ArrayListUnmanaged(Pending) = .empty;
                            defer sectors.deinit(allocator);
                            try splitSectors(allocator, w.offset, w.data, &sectors);
                            const keep = random.uintLessThan(usize, sectors.items.len + 1);
                            try pieces.appendSlice(allocator, sectors.items[0..keep]);
                        },
                        .truncate => {},
                    }
                }
            },
            .reordered => {
                for (pending) |p| {
                    if (random.boolean()) try pieces.append(allocator, p);
                }
                random.shuffle(Pending, pieces.items);
                // The last to land is torn.
                if (pieces.items.len > 0 and random.boolean()) {
                    const last = pieces.pop().?;
                    switch (last) {
                        .write => |w| {
                            var sectors: std.ArrayListUnmanaged(Pending) = .empty;
                            defer sectors.deinit(allocator);
                            try splitSectors(allocator, w.offset, w.data, &sectors);
                            for (sectors.items) |sector| {
                                if (random.boolean()) try pieces.append(allocator, sector);
                            }
                        },
                        .truncate => {},
                    }
                }
            },
            .sectors => {
                const keep_percent = random.uintAtMost(u8, 100);
                for (pending) |p| switch (p) {
                    .write => |w| {
                        var sectors: std.ArrayListUnmanaged(Pending) = .empty;
                        defer sectors.deinit(allocator);
                        try splitSectors(allocator, w.offset, w.data, &sectors);
                        for (sectors.items) |sector| {
                            if (random.uintLessThan(u8, 100) < keep_percent) try pieces.append(allocator, sector);
                        }
                    },
                    .truncate => if (random.uintLessThan(u8, 100) < keep_percent) try pieces.append(allocator, p),
                };
                random.shuffle(Pending, pieces.items);
            },
        }

        for (pieces.items) |piece| try applyPending(allocator, out, piece);

        // The length can reach the disk ahead of the data.
        if (style != .everything and full_len > out.items.len and random.uintLessThan(u8, 8) == 0) {
            try resizeTo(allocator, out, full_len);
        }
    }

    fn splitSectors(allocator: Allocator, offset: u64, data: []const u8, out: *std.ArrayListUnmanaged(Pending)) Allocator.Error!void {
        var at: u64 = offset;
        const end = offset + data.len;
        while (at < end) {
            const boundary = (at / SECTOR_SIZE + 1) * SECTOR_SIZE;
            const stop = @min(boundary, end);
            try out.append(allocator, .{ .write = .{
                .offset = at,
                .data = data[@intCast(at - offset)..@intCast(stop - offset)],
            } });
            at = stop;
        }
    }
};

/// The state of every file after a cut at `cut` changes into `trace`, on a
/// disk that started as `base` (null for an empty disk).
pub fn crashImage(
    allocator: Allocator,
    base: ?*const Image,
    trace: []const Op,
    random: std.Random,
    style: DiskModel.Style,
) Allocator.Error!Image {
    var model = if (base) |image| try DiskModel.initFromImage(allocator, image) else DiskModel.init(allocator);
    defer model.deinit();
    try model.applyAll(trace);
    return model.crash(random, style);
}

// ============================================================================
// Tests
// ============================================================================

test "fault vfs: synced data survives every cut, unsynced data may not" {
    const allocator = std.testing.allocator;
    var fs = FaultVfs.init(allocator);
    defer fs.deinit();
    const v = fs.vfs();

    const f = try v.open("a", .{ .read = true, .write = true, .create = true });
    defer f.close();
    try f.write(0, &([_]u8{1} ** 1024));
    try f.sync();
    v.syncParentDir("a");
    try f.write(1024, &([_]u8{2} ** 1024));

    var prng = std.Random.DefaultPrng.init(1);
    var saw_without = false;
    var saw_with = false;
    for (0..200) |i| {
        const style: DiskModel.Style = @enumFromInt(i % 4);
        var image = try crashImage(allocator, null, fs.trace.items, prng.random(), style);
        defer image.deinit(allocator);
        const data = image.get("a").?;
        try std.testing.expect(data.len >= 1024);
        for (data[0..1024]) |b| try std.testing.expectEqual(@as(u8, 1), b);
        if (data.len == 1024) saw_without = true;
        if (data.len == 2048 and data[2047] == 2) saw_with = true;
        if (style == .everything) try std.testing.expectEqual(@as(usize, 2048), data.len);
    }
    try std.testing.expect(saw_without and saw_with);
}

test "fault vfs: a name is durable only once its directory is synced" {
    const allocator = std.testing.allocator;
    var fs = FaultVfs.init(allocator);
    defer fs.deinit();
    const v = fs.vfs();

    const f = try v.open("new", .{ .read = true, .write = true, .create = true });
    try f.write(0, "data");
    try f.sync();
    f.close();

    var prng = std.Random.DefaultPrng.init(2);
    var missing = false;
    var present = false;
    for (0..64) |_| {
        var image = try crashImage(allocator, null, fs.trace.items, prng.random(), .reordered);
        defer image.deinit(allocator);
        if (image.get("new")) |data| {
            try std.testing.expectEqualStrings("data", data);
            present = true;
        } else missing = true;
    }
    try std.testing.expect(missing and present);

    v.syncParentDir("new");
    for (0..16) |_| {
        var image = try crashImage(allocator, null, fs.trace.items, prng.random(), .reordered);
        defer image.deinit(allocator);
        try std.testing.expect(image.get("new") != null);
    }
}

test "fault vfs: an unsynced truncation may be lost, and a torn write is cut at a sector" {
    const allocator = std.testing.allocator;
    var fs = FaultVfs.init(allocator);
    defer fs.deinit();
    const v = fs.vfs();

    const f = try v.open("t", .{ .read = true, .write = true, .create = true });
    defer f.close();
    try f.write(0, &([_]u8{7} ** 4096));
    try f.sync();
    v.syncParentDir("t");
    try f.truncate(0);
    try f.write(0, &([_]u8{9} ** 4096));

    var prng = std.Random.DefaultPrng.init(3);
    var mixed = false;
    for (0..400) |i| {
        const style: DiskModel.Style = @enumFromInt(1 + i % 3);
        var image = try crashImage(allocator, null, fs.trace.items, prng.random(), style);
        defer image.deinit(allocator);
        const data = image.get("t").?;
        // Every sector is wholly old, wholly new or zero; never part of each.
        var sector: usize = 0;
        while (sector * SECTOR_SIZE < data.len) : (sector += 1) {
            const s = data[sector * SECTOR_SIZE ..][0..SECTOR_SIZE];
            for (s) |b| try std.testing.expectEqual(s[0], b);
        }
        if (data.len == 4096 and data[0] != data[4095]) mixed = true;
    }
    try std.testing.expect(mixed);
}

test "fault vfs: an image opens as a filesystem with everything synced" {
    const allocator = std.testing.allocator;
    var fs = FaultVfs.init(allocator);
    defer fs.deinit();
    const v = fs.vfs();
    const f = try v.open("x", .{ .read = true, .write = true, .create = true });
    try f.write(0, "hello");
    try f.sync();
    f.close();
    v.syncParentDir("x");

    var prng = std.Random.DefaultPrng.init(4);
    var image = try crashImage(allocator, null, fs.trace.items, prng.random(), .sectors);
    defer image.deinit(allocator);

    var next = try FaultVfs.initFromImage(allocator, &image);
    defer next.deinit();
    const g = try next.vfs().open("x", .{ .read = true, .write = true });
    defer g.close();
    var buf: [5]u8 = undefined;
    try std.testing.expectEqual(@as(usize, 5), try g.read(0, &buf));
    try std.testing.expectEqualStrings("hello", &buf);
    try g.write(0, "j");

    // A cut on the new filesystem starts from the image.
    var again = try crashImage(allocator, &image, next.trace.items, prng.random(), .everything);
    defer again.deinit(allocator);
    try std.testing.expectEqualStrings("jello", again.get("x").?);
}
