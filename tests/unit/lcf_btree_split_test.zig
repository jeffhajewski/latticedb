//! Where a full leaf splits: inserts that arrive in key order leave full
//! pages behind them; random inserts split down the middle as before.

const std = @import("std");
const helpers = @import("helpers.zig");
const lattice = @import("lattice");

const BTree = lattice.storage.btree.BTree;

const VALUE = "v" ** 32;
const POOL: u32 = 16 * 1024 * 1024;

fn beKey(buf: *[8]u8, id: u64) []const u8 {
    std.mem.writeInt(u64, buf, id, .big);
    return buf;
}

fn expectAll(tree: *BTree, ids: []const u64) !void {
    var key_buf: [8]u8 = undefined;
    for (ids) |id| {
        const v = (try tree.get(beKey(&key_buf, id))) orelse return error.KeyMissing;
        tree.freeValue(v);
    }
}

fn countEntries(tree: *BTree) !usize {
    var iter = try tree.range(null, null);
    defer iter.deinit();
    var n: usize = 0;
    var previous: [8]u8 = undefined;
    var have_previous = false;
    while (try iter.next()) |entry| {
        if (have_previous) try std.testing.expect(std.mem.order(u8, &previous, entry.key) == .lt);
        @memcpy(&previous, entry.key[0..8]);
        have_previous = true;
        n += 1;
    }
    return n;
}

test "lcf ascending inserts leave full leaves" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.initWithPoolSize(allocator, "lcf_split_asc", POOL);
    defer db.deinit();
    var tree = try db.createBTree();

    const n: u64 = 20_000;
    var key_buf: [8]u8 = undefined;
    for (1..n + 1) |id| try tree.insert(beKey(&key_buf, id), VALUE);

    try helpers.validateBTreeInvariants(&tree);
    try std.testing.expectEqual(@as(usize, n), try countEntries(&tree));
    const stats = try tree.spaceStats();
    try std.testing.expect(stats.leafFill() > 0.95);
    try std.testing.expect(stats.internalFill() > 0.5);
}

test "lcf descending inserts leave full leaves" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.initWithPoolSize(allocator, "lcf_split_desc", POOL);
    defer db.deinit();
    var tree = try db.createBTree();

    const n: u64 = 20_000;
    var key_buf: [8]u8 = undefined;
    var id: u64 = n;
    while (id > 0) : (id -= 1) try tree.insert(beKey(&key_buf, id), VALUE);

    try helpers.validateBTreeInvariants(&tree);
    try std.testing.expectEqual(@as(usize, n), try countEntries(&tree));
    try std.testing.expect((try tree.spaceStats()).leafFill() > 0.95);
}

test "lcf random inserts still split down the middle" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.initWithPoolSize(allocator, "lcf_split_rand", POOL);
    defer db.deinit();
    var tree = try db.createBTree();

    const ids = try allocator.alloc(u64, 20_000);
    defer allocator.free(ids);
    for (ids, 0..) |*id, i| id.* = i + 1;
    var prng = std.Random.DefaultPrng.init(42);
    prng.random().shuffle(u64, ids);

    var key_buf: [8]u8 = undefined;
    for (ids) |id| try tree.insert(beKey(&key_buf, id), VALUE);

    try helpers.validateBTreeInvariants(&tree);
    try expectAll(&tree, ids);
    // About ln 2 for random order, as before the change.
    const fill = (try tree.spaceStats()).leafFill();
    try std.testing.expect(fill > 0.6 and fill < 0.8);
}

test "lcf ordered runs filled in turn split at the insert" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.initWithPoolSize(allocator, "lcf_split_runs", POOL);
    defer db.deinit();
    var tree = try db.createBTree();

    // (run, id) keys, the runs filled round-robin: a label index's shape
    // when nodes of a few labels are created in turn.
    var key: [10]u8 = undefined;
    const n: u64 = 20_000;
    for (1..n + 1) |id| {
        std.mem.writeInt(u16, key[0..2], @intCast(id % 4), .big);
        std.mem.writeInt(u64, key[2..10], id, .big);
        try tree.insert(&key, "");
    }
    try helpers.validateBTreeInvariants(&tree);
    var iter = try tree.range(null, null);
    defer iter.deinit();
    var count: usize = 0;
    while (try iter.next()) |_| count += 1;
    try std.testing.expectEqual(@as(usize, n), count);
    // Middle splits alone leave these at one half.
    try std.testing.expect((try tree.spaceStats()).leafFill() > 0.6);
}

test "lcf a tree reopened for appending keeps filling densely" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.initWithPoolSize(allocator, "lcf_split_reopen", POOL);
    defer db.deinit();
    var first = try db.createBTree();

    var key_buf: [8]u8 = undefined;
    for (1..10_001) |id| try first.insert(beKey(&key_buf, id), VALUE);

    // A new handle on the same root remembers no earlier insert; appends
    // past the rightmost leaf need no history.
    var tree = BTree.open(allocator, db.bp, first.getRootPage());
    for (10_001..20_001) |id| try tree.insert(beKey(&key_buf, id), VALUE);

    try helpers.validateBTreeInvariants(&tree);
    try std.testing.expectEqual(@as(usize, 20_000), try countEntries(&tree));
    try std.testing.expect((try tree.spaceStats()).leafFill() > 0.95);
}

test "lcf an ordered run filling in front of existing keys fills its own pages" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.initWithPoolSize(allocator, "lcf_split_front", POOL);
    defer db.deinit();
    var tree = try db.createBTree();

    // A block of high keys, then a run of low keys in order in front of it.
    // Each run insert follows the previous one; carrying the block to the
    // new leaf on every split would leave a leaf per run entry.
    var key_buf: [8]u8 = undefined;
    for (1_000_000..1_000_100) |id| try tree.insert(beKey(&key_buf, id), VALUE);
    for (1..5_001) |id| try tree.insert(beKey(&key_buf, id), VALUE);

    try helpers.validateBTreeInvariants(&tree);
    try std.testing.expectEqual(@as(usize, 5_100), try countEntries(&tree));
    try std.testing.expect((try tree.spaceStats()).leafFill() > 0.9);
}

test "lcf an in-order split whose side would not fit a page takes the other side" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.initWithPoolSize(allocator, "lcf_split_fallback", POOL);
    defer db.deinit();
    var tree = try db.createBTree();

    // One small high key, then a run of low keys in order until the leaf
    // is nearly full, then one more run key with a value too big to join
    // the run's side. It has to start the new leaf instead.
    var key_buf: [8]u8 = undefined;
    try tree.insert(beKey(&key_buf, 1_000_000), "h");
    var id: u64 = 1;
    while (true) : (id += 1) {
        const stats = try tree.spaceStats();
        if (stats.leaf_capacity - stats.leaf_used < 200) break;
        try tree.insert(beKey(&key_buf, id), VALUE);
    }
    try std.testing.expectEqual(@as(u64, 1), (try tree.spaceStats()).leaf_pages);
    const big = "b" ** 1500;
    try tree.insert(beKey(&key_buf, id), big);

    try helpers.validateBTreeInvariants(&tree);
    try std.testing.expectEqual(@as(u64, 2), (try tree.spaceStats()).leaf_pages);
    const v = (try tree.get(beKey(&key_buf, id))).?;
    defer tree.freeValue(v);
    try std.testing.expectEqualStrings(big, v);
    const h = (try tree.get(beKey(&key_buf, 1_000_000))).?;
    defer tree.freeValue(h);
    try std.testing.expectEqualStrings("h", h);
    try std.testing.expectEqual(@as(usize, id + 1), try countEntries(&tree));
}

test "lcf deleting the oldest entries of a dense tree returns its pages" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.initWithPoolSize(allocator, "lcf_split_trim", POOL);
    defer db.deinit();
    var tree = try db.createBTree();

    var key_buf: [8]u8 = undefined;
    for (1..20_001) |id| try tree.insert(beKey(&key_buf, id), VALUE);
    const before = try tree.spaceStats();

    // Oldest first, as retention trims a feed.
    for (1..15_001) |id| try tree.delete(beKey(&key_buf, id));
    try helpers.validateBTreeInvariants(&tree);
    try std.testing.expectEqual(@as(usize, 5_000), try countEntries(&tree));
    const after = try tree.spaceStats();
    // A quarter of the entries on not much more than a quarter of the pages.
    try std.testing.expect(after.leaf_pages * 4 <= before.leaf_pages + 8);
    try std.testing.expect(db.pm.deferredFrees().len > 0);

    // Appending goes on from where it was.
    for (20_001..30_001) |id| try tree.insert(beKey(&key_buf, id), VALUE);
    try helpers.validateBTreeInvariants(&tree);
    try std.testing.expectEqual(@as(usize, 15_000), try countEntries(&tree));
    try std.testing.expect((try tree.spaceStats()).leafFill() > 0.9);
}
