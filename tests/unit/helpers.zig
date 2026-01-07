//! Test helpers for Lattice database tests.
//!
//! Provides utilities for creating temporary databases, generating test data,
//! and asserting invariants.

const std = @import("std");
const lattice = @import("lattice");

const Allocator = std.mem.Allocator;

const vfs = lattice.storage.vfs;
const page_manager = lattice.storage.page_manager;
const buffer_pool = lattice.storage.buffer_pool;
const btree = lattice.storage.btree;

pub const Vfs = vfs.Vfs;
pub const PosixVfs = vfs.PosixVfs;
pub const PageManager = page_manager.PageManager;
pub const BufferPool = buffer_pool.BufferPool;
pub const BTree = btree.BTree;

/// A temporary database that cleans up after itself.
/// Uses heap allocation to ensure stable pointers for BufferPool.
pub const TempDb = struct {
    allocator: Allocator,
    posix_vfs: PosixVfs,
    pm: *PageManager,
    bp: *BufferPool,
    path: []const u8,
    wal_path: []const u8,

    const Self = @This();

    /// Create a new temporary database at a unique path.
    pub fn init(allocator: Allocator, name: []const u8) !Self {
        // Generate unique path using timestamp and random
        var path_buf: [128]u8 = undefined;
        var wal_path_buf: [128]u8 = undefined;

        const timestamp = std.time.milliTimestamp();
        const random = std.crypto.random.int(u32);

        const path = try std.fmt.bufPrint(&path_buf, "/tmp/lattice_test_{s}_{d}_{x}.db", .{ name, timestamp, random });
        const wal_path = try std.fmt.bufPrint(&wal_path_buf, "/tmp/lattice_test_{s}_{d}_{x}.wal", .{ name, timestamp, random });

        // Allocate copies
        const path_copy = try allocator.dupe(u8, path);
        errdefer allocator.free(path_copy);

        const wal_path_copy = try allocator.dupe(u8, wal_path);
        errdefer allocator.free(wal_path_copy);

        var posix_vfs = PosixVfs.init(allocator);
        const vfs_impl = posix_vfs.vfs();

        // Clean up any existing files
        vfs_impl.delete(path_copy) catch {};
        vfs_impl.delete(wal_path_copy) catch {};

        // Allocate PageManager on heap for stable pointer
        const pm = try allocator.create(PageManager);
        errdefer allocator.destroy(pm);

        pm.* = try PageManager.init(allocator, vfs_impl, path_copy, .{ .create = true });
        errdefer pm.deinit();

        // Allocate BufferPool on heap
        const bp = try allocator.create(BufferPool);
        errdefer allocator.destroy(bp);

        bp.* = try BufferPool.init(allocator, pm, 256 * 1024); // 256KB buffer pool

        return Self{
            .allocator = allocator,
            .posix_vfs = posix_vfs,
            .pm = pm,
            .bp = bp,
            .path = path_copy,
            .wal_path = wal_path_copy,
        };
    }

    /// Clean up the temporary database.
    pub fn deinit(self: *Self) void {
        self.bp.deinit();
        self.pm.deinit();

        const vfs_impl = self.posix_vfs.vfs();
        vfs_impl.delete(self.path) catch {};
        vfs_impl.delete(self.wal_path) catch {};

        self.allocator.destroy(self.bp);
        self.allocator.destroy(self.pm);
        self.allocator.free(self.path);
        self.allocator.free(self.wal_path);
    }

    /// Create a new B+Tree using this database.
    pub fn createBTree(self: *Self) !BTree {
        return BTree.init(self.allocator, self.bp);
    }
};

/// Generate a sequence of test keys in sorted order.
pub fn generateSortedKeys(allocator: Allocator, count: usize, prefix: []const u8) ![][]u8 {
    var keys = try allocator.alloc([]u8, count);
    errdefer {
        for (keys) |key| {
            allocator.free(key);
        }
        allocator.free(keys);
    }

    var initialized: usize = 0;
    errdefer {
        for (keys[0..initialized]) |key| {
            allocator.free(key);
        }
    }

    for (0..count) |i| {
        var buf: [64]u8 = undefined;
        const key_str = try std.fmt.bufPrint(&buf, "{s}{d:08}", .{ prefix, i });
        keys[i] = try allocator.dupe(u8, key_str);
        initialized += 1;
    }

    return keys;
}

/// Free keys allocated by generateSortedKeys.
pub fn freeKeys(allocator: Allocator, keys: [][]u8) void {
    for (keys) |key| {
        allocator.free(key);
    }
    allocator.free(keys);
}

/// Generate random keys (not sorted).
pub fn generateRandomKeys(allocator: Allocator, count: usize, key_length: usize) ![][]u8 {
    var keys = try allocator.alloc([]u8, count);
    errdefer allocator.free(keys);

    var initialized: usize = 0;
    errdefer {
        for (keys[0..initialized]) |key| {
            allocator.free(key);
        }
    }

    for (0..count) |i| {
        const key = try allocator.alloc(u8, key_length);
        std.crypto.random.bytes(key);
        // Make keys printable for debugging
        for (key) |*byte| {
            byte.* = 'a' + (byte.* % 26);
        }
        keys[i] = key;
        initialized += 1;
    }

    return keys;
}

/// Assert that keys are in sorted order.
pub fn assertSortedOrder(keys: []const []const u8) !void {
    if (keys.len <= 1) return;

    for (1..keys.len) |i| {
        const prev = keys[i - 1];
        const curr = keys[i];
        const order = std.mem.order(u8, prev, curr);
        if (order != .lt) {
            std.debug.print("Keys not sorted at index {d}: '{s}' should be < '{s}'\n", .{ i, prev, curr });
            return error.NotSorted;
        }
    }
}

/// Assert that a B+Tree iterator returns keys in sorted order.
pub fn assertBTreeSortedIteration(tree: *BTree, expected_count: usize) !void {
    var iter = try tree.range(null, null);
    defer iter.deinit();

    var count: usize = 0;
    var prev_key: ?[]const u8 = null;

    while (try iter.next()) |entry| {
        if (prev_key) |pk| {
            const order = std.mem.order(u8, pk, entry.key);
            if (order != .lt) {
                std.debug.print("B+Tree iteration not sorted: '{s}' should be < '{s}'\n", .{ pk, entry.key });
                return error.NotSorted;
            }
        }
        prev_key = entry.key;
        count += 1;
    }

    if (count != expected_count) {
        std.debug.print("Expected {d} entries, got {d}\n", .{ expected_count, count });
        return error.WrongCount;
    }
}

/// Count entries in a B+Tree via iteration.
pub fn countBTreeEntries(tree: *BTree) !usize {
    var iter = try tree.range(null, null);
    defer iter.deinit();

    var count: usize = 0;
    while (try iter.next()) |_| {
        count += 1;
    }
    return count;
}

/// Test that all keys in the list exist in the tree.
pub fn assertAllKeysExist(tree: *BTree, keys: []const []const u8) !void {
    for (keys, 0..) |key, i| {
        const value = try tree.get(key);
        if (value == null) {
            std.debug.print("Key at index {d} not found: '{s}'\n", .{ i, key });
            return error.KeyNotFound;
        }
    }
}

/// Test that none of the keys exist in the tree.
pub fn assertNoKeysExist(tree: *BTree, keys: []const []const u8) !void {
    for (keys, 0..) |key, i| {
        const value = try tree.get(key);
        if (value != null) {
            std.debug.print("Key at index {d} should not exist: '{s}'\n", .{ i, key });
            return error.KeyShouldNotExist;
        }
    }
}
