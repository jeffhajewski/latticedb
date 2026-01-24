//! Test helpers for Lattice database tests.
//!
//! Provides utilities for creating temporary databases, generating test data,
//! and asserting invariants.

const std = @import("std");
const lattice = @import("lattice");

const Allocator = std.mem.Allocator;

const vfs = lattice.storage.vfs;
const page_mod = lattice.storage.page;
const page_manager = lattice.storage.page_manager;
const buffer_pool = lattice.storage.buffer_pool;
const btree = lattice.storage.btree;
const locking = lattice.concurrency.locking;

const PageHeader = page_mod.PageHeader;
const LatchMode = locking.LatchMode;
const InternalNode = btree.InternalNode;
const LeafNode = btree.LeafNode;
const PageId = btree.PageId;
const NULL_PAGE = btree.NULL_PAGE;

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

    /// Create a new temporary database with a custom buffer pool size.
    pub fn initWithPoolSize(allocator: Allocator, name: []const u8, pool_bytes: u32) !Self {
        var path_buf: [128]u8 = undefined;
        var wal_path_buf: [128]u8 = undefined;

        const timestamp = std.time.milliTimestamp();
        const random = std.crypto.random.int(u32);

        const path = try std.fmt.bufPrint(&path_buf, "/tmp/lattice_test_{s}_{d}_{x}.db", .{ name, timestamp, random });
        const wal_path = try std.fmt.bufPrint(&wal_path_buf, "/tmp/lattice_test_{s}_{d}_{x}.wal", .{ name, timestamp, random });

        const path_copy = try allocator.dupe(u8, path);
        errdefer allocator.free(path_copy);

        const wal_path_copy = try allocator.dupe(u8, wal_path);
        errdefer allocator.free(wal_path_copy);

        var posix_vfs = PosixVfs.init(allocator);
        const vfs_impl = posix_vfs.vfs();

        vfs_impl.delete(path_copy) catch {};
        vfs_impl.delete(wal_path_copy) catch {};

        const pm = try allocator.create(PageManager);
        errdefer allocator.destroy(pm);

        pm.* = try PageManager.init(allocator, vfs_impl, path_copy, .{ .create = true });
        errdefer pm.deinit();

        const bp = try allocator.create(BufferPool);
        errdefer allocator.destroy(bp);

        bp.* = try BufferPool.init(allocator, pm, pool_bytes);

        return Self{
            .allocator = allocator,
            .posix_vfs = posix_vfs,
            .pm = pm,
            .bp = bp,
            .path = path_copy,
            .wal_path = wal_path_copy,
        };
    }

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

// ============================================================================
// Structural Inspection Helpers
// ============================================================================

/// Statistics about B+Tree structure.
pub const TreeStats = struct {
    height: u16,
    leaf_count: usize,
    internal_count: usize,
    total_entries: usize,
    min_leaf_entries: usize,
    max_leaf_entries: usize,
    root_is_leaf: bool,
};

/// Walk the B+Tree structure and collect statistics.
pub fn getTreeStats(tree: *BTree) !TreeStats {
    const bp = tree.bp;
    const root_page = tree.root_page;

    // Determine if root is a leaf
    const root_frame = bp.fetchPage(root_page, .shared) catch return error.BufferPoolFull;
    const root_header: *const PageHeader = @ptrCast(@alignCast(root_frame.data.ptr));
    const root_is_leaf = root_header.page_type == .btree_leaf;

    if (root_is_leaf) {
        // Single-leaf tree
        const num_entries = LeafNode.getNumEntries(root_frame.data);
        bp.unpinPage(root_frame, false);
        return TreeStats{
            .height = 1,
            .leaf_count = 1,
            .internal_count = 0,
            .total_entries = num_entries,
            .min_leaf_entries = num_entries,
            .max_leaf_entries = num_entries,
            .root_is_leaf = true,
        };
    }
    bp.unpinPage(root_frame, false);

    // Walk root→leftmost leaf to determine height
    var height: u16 = 1; // root counts
    var page_id = root_page;
    while (true) {
        const frame = bp.fetchPage(page_id, .shared) catch return error.BufferPoolFull;
        const header: *const PageHeader = @ptrCast(@alignCast(frame.data.ptr));
        if (header.page_type == .btree_leaf) {
            bp.unpinPage(frame, false);
            break;
        }
        height += 1;
        // Go to leftmost child
        if (InternalNode.getNumKeys(frame.data) > 0) {
            page_id = InternalNode.getChild(frame.data, 0);
        } else {
            page_id = InternalNode.getRightChild(frame.data);
        }
        bp.unpinPage(frame, false);
    }

    // Traverse leaf chain to count entries
    // Find first leaf by walking left from the leftmost leaf we found
    var first_leaf = page_id;
    while (true) {
        const frame = bp.fetchPage(first_leaf, .shared) catch return error.BufferPoolFull;
        const prev = LeafNode.getPrevLeaf(frame.data);
        bp.unpinPage(frame, false);
        if (prev == NULL_PAGE) break;
        first_leaf = prev;
    }

    var leaf_count: usize = 0;
    var total_entries: usize = 0;
    var min_leaf_entries: usize = std.math.maxInt(usize);
    var max_leaf_entries: usize = 0;
    var current_leaf = first_leaf;

    while (current_leaf != NULL_PAGE) {
        const frame = bp.fetchPage(current_leaf, .shared) catch return error.BufferPoolFull;
        const num_entries = LeafNode.getNumEntries(frame.data);
        const next = LeafNode.getNextLeaf(frame.data);
        bp.unpinPage(frame, false);

        leaf_count += 1;
        total_entries += num_entries;
        if (num_entries < min_leaf_entries) min_leaf_entries = num_entries;
        if (num_entries > max_leaf_entries) max_leaf_entries = num_entries;
        current_leaf = next;
    }

    if (leaf_count == 0) min_leaf_entries = 0;

    // Count internal nodes via recursive walk
    const internal_count = try countInternalNodes(bp, root_page);

    return TreeStats{
        .height = height,
        .leaf_count = leaf_count,
        .internal_count = internal_count,
        .total_entries = total_entries,
        .min_leaf_entries = min_leaf_entries,
        .max_leaf_entries = max_leaf_entries,
        .root_is_leaf = false,
    };
}

fn countInternalNodes(bp: *BufferPool, page_id: PageId) !usize {
    const frame = bp.fetchPage(page_id, .shared) catch return error.BufferPoolFull;
    const header: *const PageHeader = @ptrCast(@alignCast(frame.data.ptr));

    if (header.page_type == .btree_leaf) {
        bp.unpinPage(frame, false);
        return 0;
    }

    const num_keys = InternalNode.getNumKeys(frame.data);
    var children: [256]PageId = undefined;
    var child_count: usize = 0;

    var i: u16 = 0;
    while (i < num_keys) : (i += 1) {
        children[child_count] = InternalNode.getChild(frame.data, i);
        child_count += 1;
    }
    children[child_count] = InternalNode.getRightChild(frame.data);
    child_count += 1;
    bp.unpinPage(frame, false);

    var count: usize = 1; // this node
    for (children[0..child_count]) |child| {
        count += try countInternalNodes(bp, child);
    }
    return count;
}

/// Validate all B+Tree structural invariants.
/// Checks key ordering, bound correctness, and leaf chain integrity.
pub fn validateBTreeInvariants(tree: *BTree) !void {
    const bp = tree.bp;
    const root_page = tree.root_page;

    // Validate tree structure recursively
    try validateNode(bp, root_page, null, null, tree.comparator);

    // Validate leaf doubly-linked list
    try validateLeafChain(bp, root_page);
}

fn validateNode(
    bp: *BufferPool,
    page_id: PageId,
    lower_bound: ?[]const u8,
    upper_bound: ?[]const u8,
    cmp: btree.KeyComparator,
) !void {
    const frame = bp.fetchPage(page_id, .shared) catch return error.BufferPoolFull;
    const header: *const PageHeader = @ptrCast(@alignCast(frame.data.ptr));

    if (header.page_type == .btree_leaf) {
        try validateLeafNode(frame.data, lower_bound, upper_bound, cmp);
        bp.unpinPage(frame, false);
        return;
    }

    // Internal node validation
    const num_keys = InternalNode.getNumKeys(frame.data);

    // Check keys are sorted
    var i: u16 = 1;
    while (i < num_keys) : (i += 1) {
        const prev_key = InternalNode.getKey(frame.data, i - 1);
        const curr_key = InternalNode.getKey(frame.data, i);
        if (cmp(prev_key, curr_key) != .lt) {
            std.debug.print("Internal node keys not sorted at slot {d}\n", .{i});
            bp.unpinPage(frame, false);
            return error.InvariantViolation;
        }
    }

    // Check keys within bounds
    var j: u16 = 0;
    while (j < num_keys) : (j += 1) {
        const key = InternalNode.getKey(frame.data, j);
        if (lower_bound) |lb| {
            if (cmp(key, lb) == .lt) {
                std.debug.print("Internal key below lower bound\n", .{});
                bp.unpinPage(frame, false);
                return error.InvariantViolation;
            }
        }
        if (upper_bound) |ub| {
            if (cmp(key, ub) != .lt) {
                std.debug.print("Internal key not below upper bound\n", .{});
                bp.unpinPage(frame, false);
                return error.InvariantViolation;
            }
        }
    }

    // Recursively validate children while frame is pinned (key slices point into frame.data)
    // First child (child[0]): lower_bound to key[0]
    if (num_keys > 0) {
        try validateNode(bp, InternalNode.getChild(frame.data, 0), lower_bound, InternalNode.getKey(frame.data, 0), cmp);
    }

    // Middle children: key[i-1] to key[i]
    var k: u16 = 1;
    while (k < num_keys) : (k += 1) {
        try validateNode(bp, InternalNode.getChild(frame.data, k), InternalNode.getKey(frame.data, k - 1), InternalNode.getKey(frame.data, k), cmp);
    }

    // Right child: key[n-1] to upper_bound
    const right_child = InternalNode.getRightChild(frame.data);
    const right_lower = if (num_keys > 0) InternalNode.getKey(frame.data, num_keys - 1) else lower_bound;
    try validateNode(bp, right_child, right_lower, upper_bound, cmp);

    bp.unpinPage(frame, false);
}

fn validateLeafNode(
    buf: []const u8,
    lower_bound: ?[]const u8,
    upper_bound: ?[]const u8,
    cmp: btree.KeyComparator,
) !void {
    const num_entries = LeafNode.getNumEntries(buf);

    // Check keys are sorted
    var i: u16 = 1;
    while (i < num_entries) : (i += 1) {
        const prev_key = LeafNode.getKey(buf, i - 1);
        const curr_key = LeafNode.getKey(buf, i);
        if (cmp(prev_key, curr_key) != .lt) {
            std.debug.print("Leaf keys not sorted at slot {d}\n", .{i});
            return error.InvariantViolation;
        }
    }

    // Check all keys within bounds
    var j: u16 = 0;
    while (j < num_entries) : (j += 1) {
        const key = LeafNode.getKey(buf, j);
        if (lower_bound) |lb| {
            if (cmp(key, lb) == .lt) {
                std.debug.print("Leaf key below lower bound\n", .{});
                return error.InvariantViolation;
            }
        }
        if (upper_bound) |ub| {
            if (cmp(key, ub) != .lt) {
                std.debug.print("Leaf key not below upper bound\n", .{});
                return error.InvariantViolation;
            }
        }
    }
}

fn validateLeafChain(bp: *BufferPool, root_page: PageId) !void {
    // Find first leaf
    var page_id = root_page;
    while (true) {
        const frame = bp.fetchPage(page_id, .shared) catch return error.BufferPoolFull;
        const header: *const PageHeader = @ptrCast(@alignCast(frame.data.ptr));
        if (header.page_type == .btree_leaf) {
            bp.unpinPage(frame, false);
            break;
        }
        if (InternalNode.getNumKeys(frame.data) > 0) {
            page_id = InternalNode.getChild(frame.data, 0);
        } else {
            page_id = InternalNode.getRightChild(frame.data);
        }
        bp.unpinPage(frame, false);
    }

    // Walk forward through leaf chain, verifying prev pointers
    var prev_page: PageId = NULL_PAGE;
    var current = page_id;

    while (current != NULL_PAGE) {
        const frame = bp.fetchPage(current, .shared) catch return error.BufferPoolFull;
        const actual_prev = LeafNode.getPrevLeaf(frame.data);
        const next = LeafNode.getNextLeaf(frame.data);
        bp.unpinPage(frame, false);

        if (actual_prev != prev_page) {
            std.debug.print("Leaf chain broken: page {d} prev={d}, expected={d}\n", .{ current, actual_prev, prev_page });
            return error.InvariantViolation;
        }

        prev_page = current;
        current = next;
    }
}
