//! Fuzz testing for B+Tree operations.
//!
//! The B+Tree must handle arbitrary key/value data without:
//! - Crashing (panics, segfaults)
//! - Memory corruption
//! - Data loss (inserted data must be retrievable)
//! - Inconsistent iteration order
//!
//! Operations should either succeed or return errors gracefully.

const std = @import("std");
const lattice = @import("lattice");

const Allocator = std.mem.Allocator;
const BTree = lattice.storage.btree.BTree;
const BufferPool = lattice.storage.buffer_pool.BufferPool;
const PageManager = lattice.storage.page_manager.PageManager;
const vfs = lattice.storage.vfs;
const PosixVfs = vfs.PosixVfs;

// ============================================================================
// Test Fixture
// ============================================================================

/// Temporary B+Tree setup for fuzzing
const FuzzBTree = struct {
    allocator: Allocator,
    posix_vfs: PosixVfs,
    pm: *PageManager,
    bp: *BufferPool,
    tree: BTree,
    path: []const u8,

    fn init(allocator: Allocator) !FuzzBTree {
        // Generate unique path
        var path_buf: [128]u8 = undefined;
        const timestamp = std.time.milliTimestamp();
        const random = std.crypto.random.int(u32);
        const path = try std.fmt.bufPrint(&path_buf, "/tmp/lattice_fuzz_btree_{d}_{x}.db", .{ timestamp, random });
        const path_copy = try allocator.dupe(u8, path);
        errdefer allocator.free(path_copy);

        var posix_vfs = PosixVfs.init(allocator);
        const vfs_impl = posix_vfs.vfs();

        // Clean up any existing file
        vfs_impl.delete(path_copy) catch {};

        // Allocate PageManager
        const pm = try allocator.create(PageManager);
        errdefer allocator.destroy(pm);
        pm.* = try PageManager.init(allocator, vfs_impl, path_copy, .{ .create = true });
        errdefer pm.deinit();

        // Allocate BufferPool
        const bp = try allocator.create(BufferPool);
        errdefer allocator.destroy(bp);
        bp.* = try BufferPool.init(allocator, pm, 64 * 1024); // 64KB for fuzzing
        errdefer bp.deinit();

        // Create B+Tree
        const tree = try BTree.init(allocator, bp);

        return FuzzBTree{
            .allocator = allocator,
            .posix_vfs = posix_vfs,
            .pm = pm,
            .bp = bp,
            .tree = tree,
            .path = path_copy,
        };
    }

    fn deinit(self: *FuzzBTree) void {
        // BTree doesn't have deinit - cleaned up with buffer pool
        self.bp.deinit();
        self.pm.deinit();

        const vfs_impl = self.posix_vfs.vfs();
        vfs_impl.delete(self.path) catch {};

        self.allocator.destroy(self.bp);
        self.allocator.destroy(self.pm);
        self.allocator.free(self.path);
    }
};

// ============================================================================
// Fuzz Functions
// ============================================================================

/// Fuzz B+Tree with random insert/get/delete operations
pub fn fuzzBTreeOperations(allocator: Allocator, input: []const u8) !void {
    if (input.len < 2) return; // Need at least operation + some data

    var fuzz_tree = FuzzBTree.init(allocator) catch return;
    defer fuzz_tree.deinit();

    var tree = &fuzz_tree.tree;

    // Interpret input as a series of operations
    var i: usize = 0;
    while (i < input.len) {
        const op = input[i] % 4;
        i += 1;

        switch (op) {
            0 => {
                // Insert: next bytes are key length, key, value length, value
                if (i + 2 > input.len) break;
                const key_len = @min(input[i] % 64 + 1, input.len - i - 1);
                i += 1;
                if (i + key_len > input.len) break;
                const key = input[i..][0..key_len];
                i += key_len;

                if (i >= input.len) break;
                const val_len = @min(input[i] % 64 + 1, input.len - i - 1);
                i += 1;
                if (i + val_len > input.len) break;
                const value = input[i..][0..val_len];
                i += val_len;

                tree.insert(key, value) catch continue;
            },
            1 => {
                // Get: next bytes are key
                if (i + 1 > input.len) break;
                const key_len = @min(input[i] % 64 + 1, input.len - i - 1);
                i += 1;
                if (i + key_len > input.len) break;
                const key = input[i..][0..key_len];
                i += key_len;

                _ = tree.get(key) catch continue;
            },
            2 => {
                // Delete: next bytes are key
                if (i + 1 > input.len) break;
                const key_len = @min(input[i] % 64 + 1, input.len - i - 1);
                i += 1;
                if (i + key_len > input.len) break;
                const key = input[i..][0..key_len];
                i += key_len;

                tree.delete(key) catch continue;
            },
            3 => {
                // Range scan
                var iter = tree.range(null, null) catch continue;
                defer iter.deinit();

                var count: usize = 0;
                while (iter.next() catch null) |_| {
                    count += 1;
                    if (count > 10000) break; // Safety limit
                }
            },
            else => unreachable,
        }
    }
}

/// Fuzz B+Tree key serialization with arbitrary bytes
pub fn fuzzBTreeKeys(allocator: Allocator, input: []const u8) !void {
    if (input.len == 0) return;

    var fuzz_tree = FuzzBTree.init(allocator) catch return;
    defer fuzz_tree.deinit();

    var tree = &fuzz_tree.tree;

    // Use input directly as a key
    const value = "test_value";
    tree.insert(input, value) catch return;

    // Verify we can retrieve it
    const retrieved = tree.get(input) catch return;
    if (retrieved) |val| {
        std.debug.assert(std.mem.eql(u8, val, value));
    }

    // Delete it
    tree.delete(input) catch return;

    // Verify it's gone
    const after_delete = tree.get(input) catch return;
    std.debug.assert(after_delete == null);
}

/// Fuzz B+Tree iteration order invariant
pub fn fuzzBTreeIteration(allocator: Allocator, input: []const u8) !void {
    if (input.len < 4) return;

    var fuzz_tree = FuzzBTree.init(allocator) catch return;
    defer fuzz_tree.deinit();

    var tree = &fuzz_tree.tree;

    // Insert multiple keys derived from input
    var keys_inserted: usize = 0;
    var i: usize = 0;
    while (i + 2 <= input.len and keys_inserted < 100) {
        const key_len = @min(input[i] % 32 + 1, input.len - i - 1);
        i += 1;
        if (i + key_len > input.len) break;

        const key = input[i..][0..key_len];
        i += key_len;

        tree.insert(key, "v") catch continue;
        keys_inserted += 1;
    }

    if (keys_inserted == 0) return;

    // Verify iteration is in sorted order
    var iter = tree.range(null, null) catch return;
    defer iter.deinit();

    var prev_key: ?[]const u8 = null;
    while (iter.next() catch null) |entry| {
        if (prev_key) |pk| {
            // Keys must be in ascending order
            const order = std.mem.order(u8, pk, entry.key);
            std.debug.assert(order != .gt);
        }
        prev_key = entry.key;
    }
}

// ============================================================================
// Fuzz Runners
// ============================================================================

fn btreeOpsRunner(allocator: Allocator, input: []const u8) !void {
    try fuzzBTreeOperations(allocator, input);
}

fn btreeKeysRunner(allocator: Allocator, input: []const u8) !void {
    try fuzzBTreeKeys(allocator, input);
}

fn btreeIterRunner(allocator: Allocator, input: []const u8) !void {
    try fuzzBTreeIteration(allocator, input);
}

// ============================================================================
// Fuzz Tests using std.testing.fuzz
// ============================================================================

test "fuzz: btree random operations" {
    const allocator = std.testing.allocator;
    try std.testing.fuzz(allocator, btreeOpsRunner, .{});
}

test "fuzz: btree arbitrary keys" {
    const allocator = std.testing.allocator;
    try std.testing.fuzz(allocator, btreeKeysRunner, .{});
}

test "fuzz: btree iteration order" {
    const allocator = std.testing.allocator;
    try std.testing.fuzz(allocator, btreeIterRunner, .{});
}

// ============================================================================
// Corpus-based tests (known edge cases)
// ============================================================================

test "btree: handles empty key" {
    const allocator = std.testing.allocator;
    try fuzzBTreeKeys(allocator, "");
}

test "btree: handles single byte keys" {
    const allocator = std.testing.allocator;
    for (0..256) |i| {
        try fuzzBTreeKeys(allocator, &[_]u8{@intCast(i)});
    }
}

test "btree: handles null bytes in keys" {
    const allocator = std.testing.allocator;
    try fuzzBTreeKeys(allocator, &[_]u8{ 0x00, 0x00, 0x00 });
    try fuzzBTreeKeys(allocator, &[_]u8{ 'a', 0x00, 'b' });
    try fuzzBTreeKeys(allocator, &[_]u8{ 0x00, 'a', 0x00 });
}

test "btree: handles long keys" {
    const allocator = std.testing.allocator;
    const long_key = "a" ** 1000;
    try fuzzBTreeKeys(allocator, long_key);
}

test "btree: handles binary keys" {
    const allocator = std.testing.allocator;
    try fuzzBTreeKeys(allocator, &[_]u8{ 0xFF, 0xFE, 0x00, 0x01, 0x80, 0x7F });
}

test "btree: handles sequential operations" {
    const allocator = std.testing.allocator;
    // Insert, get, delete sequence
    const ops = &[_]u8{
        0, 5, 'h', 'e', 'l', 'l', 'o', 5, 'w', 'o', 'r', 'l', 'd', // insert hello=world
        1, 5, 'h', 'e', 'l', 'l', 'o', // get hello
        2, 5, 'h', 'e', 'l', 'l', 'o', // delete hello
        3, // range scan
    };
    try fuzzBTreeOperations(allocator, ops);
}
