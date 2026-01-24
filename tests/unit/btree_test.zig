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
    try std.testing.expect((try tree.get("target")) != null);

    // Delete and verify gone
    try tree.delete("target");
    try std.testing.expect((try tree.get("target")) == null);
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
    try std.testing.expectEqualStrings("value1", (try tree.get("key")).?);

    // Delete
    try tree.delete("key");
    try std.testing.expect((try tree.get("key")) == null);

    // Reinsert with different value
    try tree.insert("key", "value2");
    try std.testing.expectEqualStrings("value2", (try tree.get("key")).?);
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
        try std.testing.expect((try tree.get(key)) == null);
    }

    // Verify odd keys (1-99) exist
    for (0..50) |i| {
        var buf: [32]u8 = undefined;
        const key = std.fmt.bufPrint(&buf, "key{d:05}", .{i * 2 + 1}) catch unreachable;
        try std.testing.expect((try tree.get(key)) != null);
    }

    // Verify new keys (100-149) exist
    for (100..150) |i| {
        var buf: [32]u8 = undefined;
        const key = std.fmt.bufPrint(&buf, "key{d:05}", .{i}) catch unreachable;
        try std.testing.expect((try tree.get(key)) != null);
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
