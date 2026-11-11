//! Behavioral tests for B+Tree.
//!
//! These tests verify the behavioral contracts of the B+Tree, not its internal
//! implementation. Each test answers: "Given precondition X, when I do Y,
//! the system should be in state Z."

const std = @import("std");
const helpers = @import("helpers.zig");
const lattice = @import("lattice");

const btree = lattice.storage.btree;
const BTree = btree.BTree;
const BTreeError = btree.BTreeError;

fn longSplitKey(buf: *[256]u8, index: usize) []const u8 {
    @memset(buf, 'z');
    const prefix = std.fmt.bufPrint(buf[0..32], "key{d:08}", .{index}) catch unreachable;
    buf[prefix.len] = ':';
    return buf[0..180];
}

fn firstInsertThatRaisesHeightToThree(allocator: std.mem.Allocator) !usize {
    var db = try helpers.TempDb.initWithPoolSize(allocator, "internal_split_probe", 1024 * 1024);
    defer db.deinit();

    var tree = try db.createBTree();

    var key_buf: [256]u8 = undefined;
    for (0..5000) |i| {
        try tree.insert(longSplitKey(&key_buf, i), "value");
        const stats = try helpers.getTreeStats(&tree);
        if (stats.height >= 3) return i;
    }

    return error.InternalSplitNotReached;
}

// ============================================================================
// Contract: Keys remain sorted after any operation
// ============================================================================

test "btree: sorted order maintained after sequential inserts" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.init(allocator, "sorted_seq");
    defer db.deinit();

    var tree = try db.createBTree();

    // Insert keys in order
    const keys = try helpers.generateSortedKeys(allocator, 50, "key");
    defer helpers.freeKeys(allocator, keys);

    for (keys) |key| {
        try tree.insert(key, "value");
    }

    // Verify iteration returns sorted order
    try helpers.assertBTreeSortedIteration(&tree, 50);
}

test "btree: sorted order maintained after reverse inserts" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.init(allocator, "sorted_rev");
    defer db.deinit();

    var tree = try db.createBTree();

    // Insert keys in reverse order
    const keys = try helpers.generateSortedKeys(allocator, 50, "key");
    defer helpers.freeKeys(allocator, keys);

    var i: usize = keys.len;
    while (i > 0) {
        i -= 1;
        try tree.insert(keys[i], "value");
    }

    // Verify iteration still returns sorted order
    try helpers.assertBTreeSortedIteration(&tree, 50);
}

test "btree: sorted order maintained after random inserts" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.init(allocator, "sorted_rand");
    defer db.deinit();

    var tree = try db.createBTree();

    // Insert keys in random order
    const keys = try helpers.generateRandomKeys(allocator, 100, 16);
    defer helpers.freeKeys(allocator, keys);

    for (keys) |key| {
        tree.insert(key, "value") catch |err| {
            // Skip duplicates from random generation
            if (err == BTreeError.DuplicateKey) continue;
            return err;
        };
    }

    // Verify iteration returns sorted order (count may vary due to duplicates)
    var iter = try tree.range(null, null);
    defer iter.deinit();

    var prev_key: ?[]const u8 = null;
    while (try iter.next()) |entry| {
        if (prev_key) |pk| {
            const order = std.mem.order(u8, pk, entry.key);
            try std.testing.expect(order == .lt);
        }
        prev_key = entry.key;
    }
}

// ============================================================================
// Contract: Duplicate keys are rejected
// ============================================================================

test "btree: duplicate key rejected with error" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.init(allocator, "dup_reject");
    defer db.deinit();

    var tree = try db.createBTree();

    // Insert a key
    try tree.insert("mykey", "value1");

    // Attempt to insert same key again
    const result = tree.insert("mykey", "value2");
    try std.testing.expectError(BTreeError.DuplicateKey, result);

    // Original value should be unchanged
    const value = try tree.get("mykey");
    defer if (value) |owned| tree.freeValue(owned);
    try std.testing.expectEqualStrings("value1", value.?);
}

test "btree: duplicate detection works after many inserts" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.init(allocator, "dup_many");
    defer db.deinit();

    var tree = try db.createBTree();

    // Insert many keys
    const keys = try helpers.generateSortedKeys(allocator, 100, "key");
    defer helpers.freeKeys(allocator, keys);

    for (keys) |key| {
        try tree.insert(key, "value");
    }

    // Try to insert a key from the middle
    const result = tree.insert(keys[50], "newvalue");
    try std.testing.expectError(BTreeError.DuplicateKey, result);
}

// ============================================================================
// Contract: Deleted keys are not findable
// ============================================================================

test "btree: deleted key returns null on get" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.init(allocator, "del_null");
    defer db.deinit();

    var tree = try db.createBTree();

    // Insert and verify
    try tree.insert("target", "value");
    try std.testing.expect(try tree.contains("target"));

    // Delete and verify gone
    try tree.delete("target");
    try std.testing.expect(!(try tree.contains("target")));
}

test "btree: deleted key not in range scan" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.init(allocator, "del_range");
    defer db.deinit();

    var tree = try db.createBTree();

    // Insert multiple keys
    try tree.insert("aaa", "1");
    try tree.insert("bbb", "2");
    try tree.insert("ccc", "3");

    // Delete middle key
    try tree.delete("bbb");

    // Range scan should not include deleted key
    var iter = try tree.range(null, null);
    defer iter.deinit();

    var count: usize = 0;
    var found_bbb = false;
    while (try iter.next()) |entry| {
        if (std.mem.eql(u8, entry.key, "bbb")) {
            found_bbb = true;
        }
        count += 1;
    }

    try std.testing.expectEqual(@as(usize, 2), count);
    try std.testing.expect(!found_bbb);
}

test "btree: delete non-existent key returns KeyNotFound" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.init(allocator, "del_notfound");
    defer db.deinit();

    var tree = try db.createBTree();

    // Insert some keys
    try tree.insert("exists", "value");

    // Try to delete non-existent key
    const result = tree.delete("nonexistent");
    try std.testing.expectError(BTreeError.KeyNotFound, result);
}

// ============================================================================
// Contract: Range scans return all keys in [start, end)
// ============================================================================

test "btree: range scan returns complete results" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.init(allocator, "range_complete");
    defer db.deinit();

    var tree = try db.createBTree();

    // Insert known keys
    const test_keys = [_][]const u8{ "a", "b", "c", "d", "e", "f", "g" };
    for (test_keys) |key| {
        try tree.insert(key, "value");
    }

    // Full range scan
    const full_count = try helpers.countBTreeEntries(&tree);
    try std.testing.expectEqual(@as(usize, 7), full_count);

    // Partial range scan [c, f) - should include c, d, e
    var iter = try tree.range("c", "f");
    defer iter.deinit();

    var count: usize = 0;
    while (try iter.next()) |entry| {
        // Verify key is in expected range
        try std.testing.expect(std.mem.order(u8, entry.key, "c") != .lt);
        try std.testing.expect(std.mem.order(u8, entry.key, "f") == .lt);
        count += 1;
    }
    try std.testing.expectEqual(@as(usize, 3), count);
}

test "btree: range scan with no matches returns empty" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.init(allocator, "range_empty");
    defer db.deinit();

    var tree = try db.createBTree();

    try tree.insert("aaa", "1");
    try tree.insert("bbb", "2");

    // Range that doesn't match any keys
    var iter = try tree.range("ccc", "ddd");
    defer iter.deinit();

    const entry = try iter.next();
    try std.testing.expect(entry == null);
}

// ============================================================================
// Contract: Tree survives page splits
// ============================================================================

test "btree: survives single leaf split" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.init(allocator, "split_single");
    defer db.deinit();

    var tree = try db.createBTree();

    // Insert enough to trigger one split (small keys, ~100-200 fit per page)
    const count: usize = 150;
    const keys = try helpers.generateSortedKeys(allocator, count, "k");
    defer helpers.freeKeys(allocator, keys);

    for (keys) |key| {
        try tree.insert(key, "value");
    }

    // All keys should still be retrievable
    try helpers.assertAllKeysExist(&tree, keys);

    // Iteration should return sorted order
    try helpers.assertBTreeSortedIteration(&tree, count);
}

test "btree: survives multiple page splits" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.init(allocator, "split_multi");
    defer db.deinit();

    var tree = try db.createBTree();

    const count: usize = 500;
    const keys = try helpers.generateSortedKeys(allocator, count, "key");
    defer helpers.freeKeys(allocator, keys);

    for (keys) |key| {
        try tree.insert(key, "value");
    }

    // All keys should still be retrievable
    try helpers.assertAllKeysExist(&tree, keys);

    // Verify count
    const actual_count = try helpers.countBTreeEntries(&tree);
    try std.testing.expectEqual(count, actual_count);
}

// ============================================================================
// Contract: Empty tree operations are safe
// ============================================================================

test "btree: get on empty tree returns null" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.init(allocator, "empty_get");
    defer db.deinit();

    var tree = try db.createBTree();

    const result = try tree.get("anykey");
    try std.testing.expect(result == null);
}

test "btree: delete on empty tree returns KeyNotFound" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.init(allocator, "empty_del");
    defer db.deinit();

    var tree = try db.createBTree();

    const result = tree.delete("anykey");
    try std.testing.expectError(BTreeError.KeyNotFound, result);
}

test "btree: range scan on empty tree returns nothing" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.init(allocator, "empty_range");
    defer db.deinit();

    var tree = try db.createBTree();

    var iter = try tree.range(null, null);
    defer iter.deinit();

    const entry = try iter.next();
    try std.testing.expect(entry == null);
}

// ============================================================================
// State Invariant: Entry count accuracy
// ============================================================================

test "btree: entry count increases by 1 per insert" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.init(allocator, "count_insert");
    defer db.deinit();

    var tree = try db.createBTree();

    for (0..20) |i| {
        var buf: [32]u8 = undefined;
        const key = std.fmt.bufPrint(&buf, "key{d:03}", .{i}) catch unreachable;
        try tree.insert(key, "value");

        const count = try helpers.countBTreeEntries(&tree);
        try std.testing.expectEqual(i + 1, count);
    }
}

test "btree: entry count decreases by 1 per delete" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.init(allocator, "count_delete");
    defer db.deinit();

    var tree = try db.createBTree();

    // Insert 10 keys
    const keys = try helpers.generateSortedKeys(allocator, 10, "key");
    defer helpers.freeKeys(allocator, keys);

    for (keys) |key| {
        try tree.insert(key, "value");
    }

    // Delete one by one and verify count
    for (0..10) |i| {
        const expected = 10 - i;
        const count = try helpers.countBTreeEntries(&tree);
        try std.testing.expectEqual(expected, count);

        try tree.delete(keys[i]);
    }

    // Should be empty now
    const final_count = try helpers.countBTreeEntries(&tree);
    try std.testing.expectEqual(@as(usize, 0), final_count);
}

// ============================================================================
// Contract: Delete then reinsert works correctly
// ============================================================================

test "btree: can reinsert deleted key" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.init(allocator, "reinsert");
    defer db.deinit();

    var tree = try db.createBTree();

    // Insert
    try tree.insert("key", "value1");
    const first_value = (try tree.get("key")).?;
    defer tree.freeValue(first_value);
    try std.testing.expectEqualStrings("value1", first_value);

    // Delete
    try tree.delete("key");
    try std.testing.expect(!(try tree.contains("key")));

    // Reinsert with different value
    try tree.insert("key", "value2");
    const second_value = (try tree.get("key")).?;
    defer tree.freeValue(second_value);
    try std.testing.expectEqualStrings("value2", second_value);
}

// ============================================================================
// Contract: Large values work correctly
// ============================================================================

test "btree: handles large values" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.init(allocator, "large_val");
    defer db.deinit();

    var tree = try db.createBTree();

    // Create a large value (1KB)
    var large_value: [1024]u8 = undefined;
    @memset(&large_value, 'x');

    // Insert multiple entries with large values
    for (0..10) |i| {
        var buf: [32]u8 = undefined;
        const key = std.fmt.bufPrint(&buf, "key{d:02}", .{i}) catch unreachable;
        try tree.insert(key, &large_value);
    }

    // Verify all are retrievable with correct value
    for (0..10) |i| {
        var buf: [32]u8 = undefined;
        const key = std.fmt.bufPrint(&buf, "key{d:02}", .{i}) catch unreachable;
        const value = try tree.get(key);
        try std.testing.expect(value != null);
        try std.testing.expectEqual(@as(usize, 1024), value.?.len);
        try std.testing.expectEqual(@as(u8, 'x'), value.?[0]);
        if (value) |owned| tree.freeValue(owned);
    }
}

// ============================================================================
// Contract: Many keys with interleaved deletes
// ============================================================================

test "btree: interleaved insert and delete maintains consistency" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.init(allocator, "interleave");
    defer db.deinit();

    var tree = try db.createBTree();

    // Insert keys 0-99
    for (0..100) |i| {
        var buf: [32]u8 = undefined;
        const key = std.fmt.bufPrint(&buf, "key{d:05}", .{i}) catch unreachable;
        try tree.insert(key, "value");
    }

    // Delete even keys
    for (0..50) |i| {
        var buf: [32]u8 = undefined;
        const key = std.fmt.bufPrint(&buf, "key{d:05}", .{i * 2}) catch unreachable;
        try tree.delete(key);
    }

    // Insert new keys 100-149
    for (100..150) |i| {
        var buf: [32]u8 = undefined;
        const key = std.fmt.bufPrint(&buf, "key{d:05}", .{i}) catch unreachable;
        try tree.insert(key, "value");
    }

    // Verify count: 50 odd + 50 new = 100
    const count = try helpers.countBTreeEntries(&tree);
    try std.testing.expectEqual(@as(usize, 100), count);

    // Verify even keys (0-98) don't exist
    for (0..50) |i| {
        var buf: [32]u8 = undefined;
        const key = std.fmt.bufPrint(&buf, "key{d:05}", .{i * 2}) catch unreachable;
        try std.testing.expect(!(try tree.contains(key)));
    }

    // Verify odd keys (1-99) exist
    for (0..50) |i| {
        var buf: [32]u8 = undefined;
        const key = std.fmt.bufPrint(&buf, "key{d:05}", .{i * 2 + 1}) catch unreachable;
        try std.testing.expect(try tree.contains(key));
    }

    // Verify new keys (100-149) exist
    for (100..150) |i| {
        var buf: [32]u8 = undefined;
        const key = std.fmt.bufPrint(&buf, "key{d:05}", .{i}) catch unreachable;
        try std.testing.expect(try tree.contains(key));
    }

    // Verify sorted order
    try helpers.assertBTreeSortedIteration(&tree, 100);
}

// ============================================================================
// Structural: Deep B+Tree split verification
// ============================================================================

test "btree: root transitions from leaf to internal after first split" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.init(allocator, "root_trans");
    defer db.deinit();

    var tree = try db.createBTree();

    // Initially root is a leaf
    const stats_before = try helpers.getTreeStats(&tree);
    try std.testing.expect(stats_before.root_is_leaf);
    try std.testing.expectEqual(@as(u16, 1), stats_before.height);

    // Insert enough to trigger at least one split
    for (0..250) |i| {
        var buf: [32]u8 = undefined;
        const key = std.fmt.bufPrint(&buf, "key{d:08}", .{i}) catch unreachable;
        try tree.insert(key, "value");
    }

    // After splits, root should be internal
    const stats_after = try helpers.getTreeStats(&tree);
    try std.testing.expect(!stats_after.root_is_leaf);
    try std.testing.expectEqual(@as(u16, 2), stats_after.height);
    try std.testing.expectEqual(@as(usize, 250), stats_after.total_entries);
}

test "btree: leaf split distributes entries approximately evenly" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.init(allocator, "split_dist");
    defer db.deinit();

    var tree = try db.createBTree();

    for (0..250) |i| {
        var buf: [32]u8 = undefined;
        const key = std.fmt.bufPrint(&buf, "key{d:08}", .{i}) catch unreachable;
        try tree.insert(key, "value");
    }

    const stats = try helpers.getTreeStats(&tree);
    try std.testing.expect(stats.leaf_count > 1);
    // Verify min/max ratio is within 3x (reasonable split distribution)
    try std.testing.expect(stats.min_leaf_entries > 0);
    try std.testing.expect(stats.max_leaf_entries <= stats.min_leaf_entries * 3);
}

test "btree: internal node contains correct separator keys" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.init(allocator, "sep_keys");
    defer db.deinit();

    var tree = try db.createBTree();

    const count: usize = 1000;
    for (0..count) |i| {
        var buf: [32]u8 = undefined;
        const key = std.fmt.bufPrint(&buf, "key{d:08}", .{i}) catch unreachable;
        try tree.insert(key, "value");
    }

    // Validate all structural invariants
    try helpers.validateBTreeInvariants(&tree);

    // Verify all keys retrievable
    for (0..count) |i| {
        var buf: [32]u8 = undefined;
        const key = std.fmt.bufPrint(&buf, "key{d:08}", .{i}) catch unreachable;
        const value = try tree.get(key);
        try std.testing.expect(value != null);
        if (value) |owned| tree.freeValue(owned);
    }
}

test "btree: leaf sibling pointers form valid doubly-linked list" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.init(allocator, "leaf_chain");
    defer db.deinit();

    var tree = try db.createBTree();

    const count: usize = 1000;
    for (0..count) |i| {
        var buf: [32]u8 = undefined;
        const key = std.fmt.bufPrint(&buf, "key{d:08}", .{i}) catch unreachable;
        try tree.insert(key, "value");
    }

    // Validate leaf chain integrity
    try helpers.validateBTreeInvariants(&tree);

    // Verify leaf count is > 3
    const stats = try helpers.getTreeStats(&tree);
    try std.testing.expect(stats.leaf_count > 3);
    try std.testing.expectEqual(@as(usize, count), stats.total_entries);
}

test "btree: height grows to 3 with sufficient entries" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.initWithPoolSize(allocator, "height3", 1024 * 1024);
    defer db.deinit();

    var tree = try db.createBTree();

    const count: usize = 50_000;
    for (0..count) |i| {
        var buf: [32]u8 = undefined;
        const key = std.fmt.bufPrint(&buf, "key{d:08}", .{i}) catch unreachable;
        try tree.insert(key, "val");
    }

    const stats = try helpers.getTreeStats(&tree);
    try std.testing.expect(stats.height >= 3);
    try std.testing.expectEqual(@as(usize, count), stats.total_entries);

    // Validate invariants hold at height 3+
    try helpers.validateBTreeInvariants(&tree);
}

test "btree: cascading split propagates leaf through internal to new root" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.initWithPoolSize(allocator, "cascade", 1024 * 1024);
    defer db.deinit();

    var tree = try db.createBTree();

    var saw_height_1 = false;
    var saw_height_2 = false;
    var saw_height_3 = false;

    const count: usize = 50_000;
    for (0..count) |i| {
        var buf: [32]u8 = undefined;
        const key = std.fmt.bufPrint(&buf, "key{d:08}", .{i}) catch unreachable;
        try tree.insert(key, "val");

        // Check frequently at the start to catch height=1, then less often
        const check = if (i < 500) (i % 50 == 49) else (i % 2000 == 1999);
        if (check) {
            const stats = try helpers.getTreeStats(&tree);
            if (stats.height == 1) saw_height_1 = true;
            if (stats.height == 2) saw_height_2 = true;
            if (stats.height >= 3) saw_height_3 = true;
        }
    }

    // We should have observed the transitions
    try std.testing.expect(saw_height_1);
    try std.testing.expect(saw_height_2);
    try std.testing.expect(saw_height_3);

    try helpers.validateBTreeInvariants(&tree);
}

test "btree: failed internal split releases all pinned pages" {
    const allocator = std.testing.allocator;
    const split_index = try firstInsertThatRaisesHeightToThree(allocator);

    var db = try helpers.TempDb.initWithPoolSize(allocator, "internal_split_pin_cleanup", 1024 * 1024);
    defer db.deinit();

    var failing_allocator_state = std.testing.FailingAllocator.init(allocator, .{});
    const failing_allocator = failing_allocator_state.allocator();
    var tree = try BTree.init(failing_allocator, db.bp);

    var key_buf: [256]u8 = undefined;
    for (0..split_index) |i| {
        try tree.insert(longSplitKey(&key_buf, i), "value");
    }

    try std.testing.expectEqual(@as(usize, 0), db.bp.getStats().pinned_frames);

    // The next insert first splits a leaf, then attempts to split the full
    // root internal page. Let the leaf split allocations succeed and fail the
    // first allocation inside the internal split path.
    failing_allocator_state.fail_index = failing_allocator_state.alloc_index + 3;

    const result = tree.insert(longSplitKey(&key_buf, split_index), "value");
    try std.testing.expectError(BTreeError.OutOfMemory, result);
    try std.testing.expect(failing_allocator_state.has_induced_failure);
    try std.testing.expectEqual(@as(usize, 0), db.bp.getStats().pinned_frames);
}

test "btree: range scan across split boundaries returns all entries" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.init(allocator, "range_split");
    defer db.deinit();

    var tree = try db.createBTree();

    const count: usize = 1000;
    for (0..count) |i| {
        var buf: [32]u8 = undefined;
        const key = std.fmt.bufPrint(&buf, "key{d:08}", .{i}) catch unreachable;
        try tree.insert(key, "value");
    }

    // Range from 25% to 75%: keys 250..750
    var start_buf: [32]u8 = undefined;
    var end_buf: [32]u8 = undefined;
    const start_key = std.fmt.bufPrint(&start_buf, "key{d:08}", .{@as(usize, 250)}) catch unreachable;
    const end_key = std.fmt.bufPrint(&end_buf, "key{d:08}", .{@as(usize, 750)}) catch unreachable;

    var iter = try tree.range(start_key, end_key);
    defer iter.deinit();

    var range_count: usize = 0;
    var prev_key_buf: [32]u8 = undefined;
    var prev_key_len: usize = 0;
    while (try iter.next()) |entry| {
        // Verify sorted order within range
        if (range_count > 0) {
            const prev = prev_key_buf[0..prev_key_len];
            const order = std.mem.order(u8, prev, entry.key);
            try std.testing.expect(order == .lt);
        }
        @memcpy(prev_key_buf[0..entry.key.len], entry.key);
        prev_key_len = entry.key.len;
        range_count += 1;
    }

    try std.testing.expectEqual(@as(usize, 500), range_count);
}

test "btree: reverse-order inserts produce valid tree structure" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.init(allocator, "rev_struct");
    defer db.deinit();

    var tree = try db.createBTree();

    const count: usize = 1000;
    var i: usize = count;
    while (i > 0) {
        i -= 1;
        var buf: [32]u8 = undefined;
        const key = std.fmt.bufPrint(&buf, "key{d:08}", .{i}) catch unreachable;
        try tree.insert(key, "value");
    }

    // Validate structural invariants hold with reverse insertion
    try helpers.validateBTreeInvariants(&tree);

    const stats = try helpers.getTreeStats(&tree);
    try std.testing.expectEqual(@as(usize, count), stats.total_entries);
    try std.testing.expect(!stats.root_is_leaf);
}

test "btree: interleaved pattern inserts produce valid tree structure" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.init(allocator, "interlv_struct");
    defer db.deinit();

    var tree = try db.createBTree();

    const count: usize = 1000;
    // Insert even numbers first
    var i: usize = 0;
    while (i < count) : (i += 2) {
        var buf: [32]u8 = undefined;
        const key = std.fmt.bufPrint(&buf, "key{d:08}", .{i}) catch unreachable;
        try tree.insert(key, "value");
    }
    // Then odd numbers
    i = 1;
    while (i < count) : (i += 2) {
        var buf: [32]u8 = undefined;
        const key = std.fmt.bufPrint(&buf, "key{d:08}", .{i}) catch unreachable;
        try tree.insert(key, "value");
    }

    // Validate invariants with interleaved insertion pattern
    try helpers.validateBTreeInvariants(&tree);

    const stats = try helpers.getTreeStats(&tree);
    try std.testing.expectEqual(@as(usize, count), stats.total_entries);

    // Verify sorted iteration
    try helpers.assertBTreeSortedIteration(&tree, count);
}

test "btree: delete after split maintains valid tree structure" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.init(allocator, "del_split");
    defer db.deinit();

    var tree = try db.createBTree();

    const count: usize = 500;
    for (0..count) |i| {
        var buf: [32]u8 = undefined;
        const key = std.fmt.bufPrint(&buf, "key{d:08}", .{i}) catch unreachable;
        try tree.insert(key, "value");
    }

    // Delete every other entry (250 deletions)
    for (0..count / 2) |i| {
        var buf: [32]u8 = undefined;
        const key = std.fmt.bufPrint(&buf, "key{d:08}", .{i * 2}) catch unreachable;
        try tree.delete(key);
    }

    // Verify remaining count
    const remaining = try helpers.countBTreeEntries(&tree);
    try std.testing.expectEqual(@as(usize, 250), remaining);

    // Verify sorted iteration of remaining entries
    try helpers.assertBTreeSortedIteration(&tree, 250);
}

// ============================================================================
// Contract: internal splits divide by bytes, so long keys cannot overflow a page
// ============================================================================

fn paddedKey(buf: []u8, prefix: u8, index: usize, len: usize) []const u8 {
    @memset(buf[0..len], 'x');
    buf[0] = prefix;
    _ = std.fmt.bufPrint(buf[1..9], "{d:08}", .{index}) catch unreachable;
    return buf[0..len];
}

fn rootKeyCount(tree: *BTree) !?u16 {
    const frame = try tree.bp.fetchPage(tree.root_page, .shared);
    defer tree.bp.unpinPage(frame, false);
    const header: *const lattice.storage.page.PageHeader = @ptrCast(@alignCast(frame.data.ptr));
    if (header.page_type != .btree_internal) return null;
    return btree.InternalNode.getNumKeys(frame.data);
}

test "btree: internal split with long separators divides by bytes" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.init(allocator, "internal_split_long_keys");
    defer db.deinit();
    var tree = try db.createBTree();

    // A root internal node with twenty short separators.
    var key_buf: [1100]u8 = undefined;
    var value_buf: [300]u8 = undefined;
    @memset(&value_buf, 'v');
    var short_count: usize = 0;
    while ((try rootKeyCount(&tree)) orelse 0 < 20) : (short_count += 1) {
        try tree.insert(paddedKey(&key_buf, 'b', short_count, 9), &value_buf);
    }
    try std.testing.expectEqual(@as(u16, 2), (try helpers.getTreeStats(&tree)).height);

    // Maximum-length keys sorting before all of them. Every second one splits
    // the leftmost leaf and adds a long separator to the root. Three fit next
    // to the short ones; the fourth splits the root, and a split by key count
    // would put all four long keys plus a dozen short ones on one page.
    const long_len = btree.maxKeySize(db.pm.getPageSize());
    var long_count: usize = 0;
    while (long_count < 12) : (long_count += 1) {
        try tree.insert(paddedKey(&key_buf, 'a', long_count, long_len), "v");
    }

    try helpers.validateBTreeInvariants(&tree);
    try std.testing.expectEqual(short_count + long_count, try helpers.countBTreeEntries(&tree));
    for (0..short_count) |i| {
        try std.testing.expect(try tree.contains(paddedKey(&key_buf, 'b', i, 9)));
    }
    for (0..long_count) |i| {
        try std.testing.expect(try tree.contains(paddedKey(&key_buf, 'a', i, long_len)));
    }
}

test "btree: keys longer than the maximum key size are rejected cleanly" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.init(allocator, "max_key_size");
    defer db.deinit();
    var tree = try db.createBTree();

    const max = btree.maxKeySize(db.pm.getPageSize());
    try std.testing.expectEqual(@as(usize, 1012), max);

    var key_buf: [4096]u8 = undefined;
    @memset(&key_buf, 'k');
    try std.testing.expect(tree.canFitLeafEntry(key_buf[0..max], 10));
    try std.testing.expect(!tree.canFitLeafEntry(key_buf[0 .. max + 1], 10));
    try tree.insert(key_buf[0..max], "ok");
    try std.testing.expectError(BTreeError.PageFull, tree.insert(key_buf[0 .. max + 1], "no"));
    try std.testing.expectError(BTreeError.PageFull, tree.insert(key_buf[0..3000], "no"));
    try std.testing.expectEqual(@as(usize, 1), try helpers.countBTreeEntries(&tree));
    try helpers.validateBTreeInvariants(&tree);
}

/// Model-checked random workload with keys and values of every size the tree
/// accepts, over 4 KB pages. Every separator length, overflow value and
/// split/merge shape is exercised; the tree must agree with a hash map and
/// keep its structural invariants throughout.
fn randomLongKeyWorkload(allocator: std.mem.Allocator, seed: u64, ops: usize) !void {
    var db = try helpers.TempDb.initWithPoolSize(allocator, "random_long_keys", 2 * 1024 * 1024);
    defer db.deinit();
    var tree = try db.createBTree();

    var model = std.StringHashMap([]u8).init(allocator);
    defer {
        var it = model.iterator();
        while (it.next()) |e| {
            allocator.free(e.key_ptr.*);
            allocator.free(e.value_ptr.*);
        }
        model.deinit();
    }

    var prng = std.Random.DefaultPrng.init(seed);
    const random = prng.random();
    const max_key = btree.maxKeySize(db.pm.getPageSize());
    var key_buf: [4096]u8 = undefined;
    var value_buf: [9000]u8 = undefined;

    for (0..ops) |op_index| {
        // Mostly short keys, with a heavy tail up to the maximum.
        const key_len = switch (random.uintLessThan(u8, 10)) {
            0...4 => 1 + random.uintLessThan(usize, 24),
            5...7 => 1 + random.uintLessThan(usize, 300),
            else => 1 + random.uintLessThan(usize, max_key),
        };
        // A small alphabet makes repeats (duplicates, deletes of live keys) common.
        for (key_buf[0..key_len]) |*b| b.* = 'a' + random.uintLessThan(u8, 3);
        const key = key_buf[0..key_len];

        const action = random.uintLessThan(u8, 10);
        if (action < 6) {
            const value_len = switch (random.uintLessThan(u8, 10)) {
                0...6 => random.uintLessThan(usize, 40),
                7, 8 => random.uintLessThan(usize, 2500),
                else => random.uintLessThan(usize, value_buf.len),
            };
            random.bytes(value_buf[0..value_len]);
            const value = value_buf[0..value_len];
            if (tree.insert(key, value)) |_| {
                try std.testing.expect(!model.contains(key));
                try model.put(try allocator.dupe(u8, key), try allocator.dupe(u8, value));
            } else |err| {
                try std.testing.expectEqual(BTreeError.DuplicateKey, err);
                try std.testing.expect(model.contains(key));
            }
        } else if (action < 9) {
            if (tree.delete(key)) |_| {
                const removed = model.fetchRemove(key) orelse return error.TestUnexpectedResult;
                allocator.free(removed.key);
                allocator.free(removed.value);
            } else |err| {
                try std.testing.expectEqual(BTreeError.KeyNotFound, err);
                try std.testing.expect(!model.contains(key));
            }
        } else {
            const got = try tree.get(key);
            defer if (got) |v| tree.freeValue(v);
            if (model.get(key)) |expected| {
                try std.testing.expectEqualSlices(u8, expected, got.?);
            } else {
                try std.testing.expect(got == null);
            }
        }

        if (op_index % 250 == 0) try helpers.validateBTreeInvariants(&tree);
    }

    try helpers.validateBTreeInvariants(&tree);
    try std.testing.expectEqual(model.count(), try helpers.countBTreeEntries(&tree));
    var it = model.iterator();
    while (it.next()) |e| {
        const got = (try tree.get(e.key_ptr.*)).?;
        defer tree.freeValue(got);
        try std.testing.expectEqualSlices(u8, e.value_ptr.*, got);
    }
    try std.testing.expectEqual(@as(usize, 0), db.bp.getStats().pinned_frames);
}

test "btree: random workload with keys up to the maximum size matches a model" {
    for ([_]u64{ 1, 2, 3, 0xfeed }) |seed| {
        try randomLongKeyWorkload(std.testing.allocator, seed, 3000);
    }
}

// ============================================================================
// Contract: a failed insert leaves the tree exactly as it was
// ============================================================================

fn expectLongKeyTreeHolds(tree: *BTree, count: usize) !void {
    var key_buf: [256]u8 = undefined;
    try helpers.validateBTreeInvariants(tree);
    try std.testing.expectEqual(count, try helpers.countBTreeEntries(tree));
    for (0..count) |i| {
        try std.testing.expect(try tree.contains(longSplitKey(&key_buf, i)));
    }
}

test "btree: an insert failing at any allocation during a split leaves the tree unchanged" {
    const allocator = std.testing.allocator;
    const split_index = try firstInsertThatRaisesHeightToThree(allocator);

    // Fail the first, second, ... allocation of the insert that splits a
    // leaf, its parent and the root, until the insert gets through.
    var fail_offset: usize = 0;
    while (true) : (fail_offset += 1) {
        var db = try helpers.TempDb.initWithPoolSize(allocator, "split_oom_atomic", 1024 * 1024);
        defer db.deinit();

        var failing_state = std.testing.FailingAllocator.init(allocator, .{});
        var tree = try BTree.init(failing_state.allocator(), db.bp);
        var key_buf: [256]u8 = undefined;
        for (0..split_index) |i| try tree.insert(longSplitKey(&key_buf, i), "value");

        failing_state.fail_index = failing_state.alloc_index + fail_offset;
        const result = tree.insert(longSplitKey(&key_buf, split_index), "value");
        try std.testing.expectEqual(@as(usize, 0), db.bp.getStats().pinned_frames);

        if (!failing_state.has_induced_failure) {
            try result;
            try expectLongKeyTreeHolds(&tree, split_index + 1);
            break;
        }

        try std.testing.expectError(BTreeError.OutOfMemory, result);
        try expectLongKeyTreeHolds(&tree, split_index);

        failing_state.fail_index = std.math.maxInt(usize);
        try tree.insert(longSplitKey(&key_buf, split_index), "value");
        try expectLongKeyTreeHolds(&tree, split_index + 1);
    }
    try std.testing.expect(fail_offset >= 3);
}

test "btree: an insert running out of buffer frames during a split leaves the tree unchanged" {
    const allocator = std.testing.allocator;
    const split_index = try firstInsertThatRaisesHeightToThree(allocator);
    const frame_count = 16;

    // Hold more and more of a small pool pinned so the split runs out of
    // frames at every step it fetches one.
    var saw_failure = false;
    var held_count: usize = 0;
    while (held_count < frame_count) : (held_count += 1) {
        var db = try helpers.TempDb.initWithPoolSize(allocator, "split_frames_atomic", frame_count * 4096);
        defer db.deinit();

        var tree = try db.createBTree();
        var key_buf: [256]u8 = undefined;
        for (0..split_index) |i| try tree.insert(longSplitKey(&key_buf, i), "value");

        // Pin pages that are not part of the tree.
        var held: [frame_count]*lattice.storage.buffer_pool.BufferFrame = undefined;
        var pinned: usize = 0;
        defer for (held[0..pinned]) |frame| db.bp.unpinPage(frame, false);
        while (pinned < held_count) : (pinned += 1) {
            const page_id = try db.pm.allocatePage();
            held[pinned] = try db.bp.fetchPage(page_id, .shared);
        }

        const result = tree.insert(longSplitKey(&key_buf, split_index), "value");
        if (result) |_| {
            for (held[0..pinned]) |frame| db.bp.unpinPage(frame, false);
            pinned = 0;
            try expectLongKeyTreeHolds(&tree, split_index + 1);
            continue;
        } else |err| {
            try std.testing.expectEqual(BTreeError.BufferPoolFull, err);
            saw_failure = true;
        }

        for (held[0..pinned]) |frame| db.bp.unpinPage(frame, false);
        pinned = 0;
        try std.testing.expectEqual(@as(usize, 0), db.bp.getStats().pinned_frames);
        try expectLongKeyTreeHolds(&tree, split_index);
        try tree.insert(longSplitKey(&key_buf, split_index), "value");
        try expectLongKeyTreeHolds(&tree, split_index + 1);
    }
    try std.testing.expect(saw_failure);
}
