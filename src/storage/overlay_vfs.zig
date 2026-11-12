//! A filesystem that reads another one and keeps its own writes in memory.
//!
//! A read-only open of a database that a crash left with work in its log or
//! its flush journal has to apply that work to see the committed state, and
//! must not change the files to do it. Opened on this, the database runs its
//! ordinary recovery: the journal is applied, the log is replayed and folded
//! in, and every byte that writes goes into memory, in 4 KB blocks laid over
//! the files underneath, which are only ever opened for reading.
//!
//! What it holds is bounded by a budget. A write that would take it past the
//! budget fails and sets `budget_exceeded`, so the caller can tell a recovery
//! that was too large to do in memory from any other failure.
//!
//! Locks go to the file underneath as shared locks, whatever was asked for:
//! the database is only being read, and a writer must still be kept out.

const std = @import("std");
const vfs_mod = @import("vfs.zig");

const Allocator = std.mem.Allocator;
const Vfs = vfs_mod.Vfs;
const File = vfs_mod.File;
const VfsError = vfs_mod.VfsError;
const OpenFlags = vfs_mod.OpenFlags;
const LockMode = vfs_mod.LockMode;

pub const BLOCK_SIZE: usize = 4096;
const Block = [BLOCK_SIZE]u8;

/// One file: what the file underneath shows, and the blocks written over it.
const Node = struct {
    /// The file underneath, opened for reading, if there is one.
    base: ?File = null,
    /// How much of the file underneath is still visible. A truncation hides
    /// whatever lies past it, even if the file grows again.
    base_len: u64 = 0,
    size: u64 = 0,
    /// False once deleted.
    present: bool = true,
    blocks: std.AutoHashMapUnmanaged(u64, *Block) = .empty,
};

pub const OverlayVfs = struct {
    allocator: Allocator,
    base: Vfs,
    budget: usize,
    used: usize = 0,
    /// Set when a write was refused for want of budget.
    budget_exceeded: bool = false,
    /// Also set then, if given: outlives the VFS when a failed open frees it.
    exceeded_out: ?*bool = null,
    files: std.StringHashMapUnmanaged(*Node) = .empty,
    mutex: @import("compat").Mutex = .{},

    const Self = @This();

    pub fn init(allocator: Allocator, base: Vfs, budget: usize) Self {
        return .{ .allocator = allocator, .base = base, .budget = budget };
    }

    pub fn deinit(self: *Self) void {
        var it = self.files.iterator();
        while (it.next()) |entry| {
            const node = entry.value_ptr.*;
            var blocks = node.blocks.valueIterator();
            while (blocks.next()) |block| self.allocator.destroy(block.*);
            node.blocks.deinit(self.allocator);
            if (node.base) |f| f.close();
            self.allocator.destroy(node);
            self.allocator.free(entry.key_ptr.*);
        }
        self.files.deinit(self.allocator);
    }

    pub fn vfs(self: *Self) Vfs {
        return .{ .ptr = self, .vtable = &vtable };
    }

    const vtable = Vfs.VTable{
        .open = vfsOpen,
        .delete = vfsDelete,
        .exists = vfsExists,
    };

    fn vfsOpen(ptr: *anyopaque, path: []const u8, flags: OpenFlags) VfsError!File {
        const self: *Self = @ptrCast(@alignCast(ptr));
        self.mutex.lock();
        defer self.mutex.unlock();

        const node = if (self.files.get(path)) |node| blk: {
            if (!node.present) {
                if (!flags.create) return VfsError.FileNotFound;
                node.present = true;
            } else if (flags.exclusive) {
                return VfsError.AlreadyExists;
            }
            break :blk node;
        } else blk: {
            var node = Node{};
            if (self.base.exists(path)) {
                if (flags.create and flags.exclusive) return VfsError.AlreadyExists;
                // A file that is there and cannot be read is an error, not
                // an empty file: an unreadable log must not read as no log.
                const f = try self.base.open(path, .{ .read = true });
                errdefer f.close();
                node.base = f;
                node.base_len = try f.size();
                node.size = node.base_len;
            } else if (!flags.create) {
                return VfsError.FileNotFound;
            }
            break :blk try self.addNode(path, node);
        };
        if (flags.truncate) try self.truncateNode(node, 0);
        const handle = self.allocator.create(Handle) catch return VfsError.Unexpected;
        handle.* = .{ .owner = self, .node = node };
        return .{ .ptr = handle, .vtable = &file_vtable };
    }

    fn addNode(self: *Self, path: []const u8, node: Node) VfsError!*Node {
        const key = self.allocator.dupe(u8, path) catch return VfsError.Unexpected;
        errdefer self.allocator.free(key);
        const ptr = self.allocator.create(Node) catch return VfsError.Unexpected;
        errdefer self.allocator.destroy(ptr);
        ptr.* = node;
        self.files.put(self.allocator, key, ptr) catch return VfsError.Unexpected;
        return ptr;
    }

    fn vfsDelete(ptr: *anyopaque, path: []const u8) VfsError!void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        self.mutex.lock();
        defer self.mutex.unlock();
        if (self.files.get(path)) |node| {
            if (!node.present) return VfsError.FileNotFound;
            try self.truncateNode(node, 0);
            node.present = false;
            return;
        }
        if (!self.base.exists(path)) return VfsError.FileNotFound;
        _ = try self.addNode(path, .{ .present = false });
    }

    fn vfsExists(ptr: *anyopaque, path: []const u8) bool {
        const self: *Self = @ptrCast(@alignCast(ptr));
        self.mutex.lock();
        defer self.mutex.unlock();
        if (self.files.get(path)) |node| return node.present;
        return self.base.exists(path);
    }

    const file_vtable = File.VTable{
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

    /// Bytes of the file underneath, zero past what is still visible.
    fn readBase(node: *Node, offset: u64, dst: []u8) VfsError!void {
        @memset(dst, 0);
        if (offset >= node.base_len) return;
        const f = node.base orelse return;
        const want: usize = @intCast(@min(@as(u64, dst.len), node.base_len - offset));
        var done: usize = 0;
        while (done < want) {
            const n = try f.read(offset + done, dst[done..want]);
            if (n == 0) break;
            done += n;
        }
    }

    fn truncateNode(self: *Self, node: *Node, new_size: u64) VfsError!void {
        if (new_size < node.size) {
            const keep_blocks = (new_size + BLOCK_SIZE - 1) / BLOCK_SIZE;
            var doomed: std.ArrayListUnmanaged(u64) = .empty;
            defer doomed.deinit(self.allocator);
            var it = node.blocks.iterator();
            while (it.next()) |entry| {
                if (entry.key_ptr.* >= keep_blocks) {
                    doomed.append(self.allocator, entry.key_ptr.*) catch return VfsError.Unexpected;
                }
            }
            for (doomed.items) |key| {
                const block = node.blocks.fetchRemove(key).?.value;
                self.allocator.destroy(block);
                self.used -= BLOCK_SIZE;
            }
            if (new_size % BLOCK_SIZE != 0) {
                if (node.blocks.get(new_size / BLOCK_SIZE)) |block| {
                    @memset(block[@intCast(new_size % BLOCK_SIZE)..], 0);
                }
            }
            node.base_len = @min(node.base_len, new_size);
        }
        node.size = new_size;
    }

    fn blockFor(self: *Self, node: *Node, index: u64) VfsError!*Block {
        if (node.blocks.get(index)) |block| return block;
        if (self.used + BLOCK_SIZE > self.budget) {
            self.budget_exceeded = true;
            if (self.exceeded_out) |out| out.* = true;
            return VfsError.DiskFull;
        }
        const block = self.allocator.create(Block) catch return VfsError.Unexpected;
        errdefer self.allocator.destroy(block);
        try readBase(node, index * BLOCK_SIZE, block);
        node.blocks.put(self.allocator, index, block) catch return VfsError.Unexpected;
        self.used += BLOCK_SIZE;
        return block;
    }
};

// The handle needs both the VFS and the node. Files are opened rarely (a
// database opens three), so each handle is its own small allocation.
const Handle = struct {
    owner: *OverlayVfs,
    node: *Node,
};

fn handleOf(ptr: *anyopaque) *Handle {
    return @ptrCast(@alignCast(ptr));
}

fn fileRead(ptr: *anyopaque, offset: u64, buf: []u8) VfsError!usize {
    const h = handleOf(ptr);
    h.owner.mutex.lock();
    defer h.owner.mutex.unlock();
    const node = h.node;
    if (offset >= node.size) return 0;
    const n: usize = @intCast(@min(@as(u64, buf.len), node.size - offset));
    var done: usize = 0;
    while (done < n) {
        const at = offset + done;
        const index = at / BLOCK_SIZE;
        const within: usize = @intCast(at % BLOCK_SIZE);
        const take = @min(n - done, BLOCK_SIZE - within);
        if (node.blocks.get(index)) |block| {
            @memcpy(buf[done..][0..take], block[within..][0..take]);
        } else {
            try OverlayVfs.readBase(node, at, buf[done..][0..take]);
        }
        done += take;
    }
    return n;
}

fn fileWrite(ptr: *anyopaque, offset: u64, data: []const u8) VfsError!void {
    const h = handleOf(ptr);
    h.owner.mutex.lock();
    defer h.owner.mutex.unlock();
    const node = h.node;
    var done: usize = 0;
    while (done < data.len) {
        const at = offset + done;
        const within: usize = @intCast(at % BLOCK_SIZE);
        const take = @min(data.len - done, BLOCK_SIZE - within);
        const block = try h.owner.blockFor(node, at / BLOCK_SIZE);
        @memcpy(block[within..][0..take], data[done..][0..take]);
        done += take;
    }
    node.size = @max(node.size, offset + data.len);
}

fn fileSync(ptr: *anyopaque) VfsError!void {
    _ = ptr;
}

fn fileTruncate(ptr: *anyopaque, new_size: u64) VfsError!void {
    const h = handleOf(ptr);
    h.owner.mutex.lock();
    defer h.owner.mutex.unlock();
    try h.owner.truncateNode(h.node, new_size);
}

fn fileSize(ptr: *anyopaque) VfsError!u64 {
    const h = handleOf(ptr);
    h.owner.mutex.lock();
    defer h.owner.mutex.unlock();
    return h.node.size;
}

fn fileClose(ptr: *anyopaque) void {
    const h = handleOf(ptr);
    h.owner.allocator.destroy(h);
}

fn fileLock(ptr: *anyopaque, mode: LockMode) VfsError!void {
    _ = mode;
    const h = handleOf(ptr);
    if (h.node.base) |f| try f.lock(.shared);
}

fn fileTryLock(ptr: *anyopaque, mode: LockMode) VfsError!bool {
    _ = mode;
    const h = handleOf(ptr);
    if (h.node.base) |f| return f.tryLock(.shared);
    return true;
}

fn fileUnlock(ptr: *anyopaque) void {
    const h = handleOf(ptr);
    if (h.node.base) |f| f.unlock();
}

// ============================================================================
// Tests
// ============================================================================

const MemoryVfs = @import("memory_vfs.zig").MemoryVfs;

fn seed(base: *MemoryVfs, path: []const u8, bytes: []const u8) !void {
    const f = try base.vfs().open(path, .{ .write = true, .create = true });
    defer f.close();
    try f.write(0, bytes);
}

fn readAll(f: File, buf: []u8) ![]u8 {
    const n = try f.read(0, buf);
    return buf[0..n];
}

test "overlay: writes stay in memory and read back over the file" {
    const allocator = std.testing.allocator;
    var base = MemoryVfs.init(allocator);
    defer base.deinit();
    var bytes: [10000]u8 = undefined;
    for (&bytes, 0..) |*b, i| b.* = @truncate(i);
    try seed(&base, "f", &bytes);

    var overlay = OverlayVfs.init(allocator, base.vfs(), 1 << 20);
    defer overlay.deinit();
    const f = try overlay.vfs().open("f", .{ .write = true });
    defer f.close();
    try f.write(5000, "hello");

    var buf: [20000]u8 = undefined;
    const got = try readAll(f, &buf);
    try std.testing.expectEqual(@as(usize, 10000), got.len);
    try std.testing.expectEqualStrings("hello", got[5000..5005]);
    try std.testing.expectEqualSlices(u8, bytes[0..5000], got[0..5000]);
    try std.testing.expectEqualSlices(u8, bytes[5005..], got[5005..]);

    // The file underneath is as it was.
    const under = try base.vfs().open("f", .{});
    defer under.close();
    try std.testing.expectEqualSlices(u8, &bytes, try readAll(under, &buf));
}

test "overlay: a truncation hides the file's tail even after the file grows" {
    const allocator = std.testing.allocator;
    var base = MemoryVfs.init(allocator);
    defer base.deinit();
    try seed(&base, "f", &([_]u8{0xAA} ** 9000));

    var overlay = OverlayVfs.init(allocator, base.vfs(), 1 << 20);
    defer overlay.deinit();
    const f = try overlay.vfs().open("f", .{ .write = true });
    defer f.close();
    try f.truncate(4100);
    try f.truncate(9000);
    var buf: [9000]u8 = undefined;
    const got = try readAll(f, &buf);
    try std.testing.expectEqual(@as(usize, 9000), got.len);
    try std.testing.expect(std.mem.allEqual(u8, got[0..4100], 0xAA));
    try std.testing.expect(std.mem.allEqual(u8, got[4100..], 0));
}

test "overlay: delete, create and exists stay in memory" {
    const allocator = std.testing.allocator;
    var base = MemoryVfs.init(allocator);
    defer base.deinit();
    try seed(&base, "f", "data");

    var overlay = OverlayVfs.init(allocator, base.vfs(), 1 << 20);
    defer overlay.deinit();
    const v = overlay.vfs();
    try v.delete("f");
    try std.testing.expect(!v.exists("f"));
    try std.testing.expectError(VfsError.FileNotFound, v.open("f", .{}));
    try std.testing.expect(base.vfs().exists("f"));

    const g = try v.open("g", .{ .write = true, .create = true });
    defer g.close();
    try g.write(0, "new");
    try std.testing.expect(v.exists("g"));
    try std.testing.expect(!base.vfs().exists("g"));
}

test "overlay: a write past the budget fails and says so" {
    const allocator = std.testing.allocator;
    var base = MemoryVfs.init(allocator);
    defer base.deinit();

    var exceeded = false;
    var overlay = OverlayVfs.init(allocator, base.vfs(), 2 * BLOCK_SIZE);
    overlay.exceeded_out = &exceeded;
    defer overlay.deinit();
    const f = try overlay.vfs().open("f", .{ .write = true, .create = true });
    defer f.close();
    try f.write(0, &([_]u8{1} ** (2 * BLOCK_SIZE)));
    try std.testing.expectError(VfsError.DiskFull, f.write(2 * BLOCK_SIZE, "x"));
    try std.testing.expect(overlay.budget_exceeded);
    try std.testing.expect(exceeded);
    // Space given back by a truncation can be used again.
    try f.truncate(BLOCK_SIZE);
    try f.write(BLOCK_SIZE, "x");
}
