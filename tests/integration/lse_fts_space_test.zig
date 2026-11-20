//! Full-text search gives back the space of what leaves it.
//!
//! Posting pages used to stay allocated once written: deleting documents
//! emptied them in place, dropping an index left them behind, and a term no
//! document held any more kept its dictionary entry. A database whose indexed
//! text turned over grew without bound.

const std = @import("std");
const lattice = @import("lattice");

const Database = lattice.storage.database.Database;
const FtsIndex = lattice.fts.index.FtsIndex;

fn dbPath(buf: []u8, name: []const u8) ![]const u8 {
    return std.fmt.bufPrint(buf, "/tmp/lse_fts_space_{s}.db", .{name});
}

fn removeFile(path: []const u8) void {
    @import("compat").fs.cwd().deleteFile(path) catch {};
    var wal_buf: [256]u8 = undefined;
    const wal = std.fmt.bufPrint(&wal_buf, "{s}-wal", .{path}) catch return;
    @import("compat").fs.cwd().deleteFile(wal) catch {};
}

fn openDb(allocator: std.mem.Allocator, path: []const u8) !*Database {
    return Database.open(allocator, path, .{
        .create = true,
        .config = .{ .enable_fts = true, .enable_vector = false },
    });
}

/// Documents whose words are drawn from a vocabulary that starts at `offset`,
/// so each round brings terms the last did not have.
fn indexRound(db: *Database, ids: []u64, offset: u32, seed: u64) !void {
    var prng = std.Random.DefaultPrng.init(seed);
    const rnd = prng.random();
    var buf: [256]u8 = undefined;
    for (ids) |*id| {
        var w: std.Io.Writer = .fixed(&buf);
        try w.writeAll("common");
        for (0..6) |_| try w.print(" w{d}", .{offset + rnd.uintLessThan(u32, 2000)});
        id.* = try db.createNode(null, &[_][]const u8{"Doc"});
        try db.setNodeProperty(null, id.*, "body", .{ .string_val = w.buffered() });
    }
    try db.sync();
}

fn treeKeyCount(tree: *lattice.storage.btree.BTree) !usize {
    var iter = try tree.range(null, null);
    defer iter.deinit();
    var count: usize = 0;
    while (try iter.next()) |_| count += 1;
    return count;
}

test "fts space: deleting every document frees its posting pages and terms" {
    const allocator = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = try dbPath(&path_buf, "churn");
    removeFile(path);
    defer removeFile(path);

    const db = try openDb(allocator, path);
    defer db.close();
    try db.createNodeFtsIndex("Doc", "body");

    var ids: [2000]u64 = undefined;
    try indexRound(db, &ids, 0, 1);
    const after_first = db.page_manager.pageCount();

    for (ids) |id| try db.deleteNode(null, id);
    try db.sync();
    // No term is held by any document, so none is left in the dictionary.
    try std.testing.expectEqual(@as(usize, 0), try treeKeyCount(&db.fts_dict_tree));

    // A second round of new terms reuses the pages the first gave back.
    try indexRound(db, &ids, 100_000, 2);
    const after_second = db.page_manager.pageCount();
    // Before, the second round took as many pages again as the first.
    try std.testing.expect(after_second < after_first + after_first / 10);

    const results = try db.ftsSearchIndex(.node, "Doc", "body", "common", 5000);
    defer db.freeFtsSearchResults(results);
    try std.testing.expectEqual(@as(usize, ids.len), results.len);
}

test "fts space: dropping an index frees its posting pages" {
    const allocator = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = try dbPath(&path_buf, "drop");
    removeFile(path);
    defer removeFile(path);

    const db = try openDb(allocator, path);
    defer db.close();
    try db.createNodeFtsIndex("Doc", "body");
    var ids: [2000]u64 = undefined;
    try indexRound(db, &ids, 0, 3);
    const indexed = db.page_manager.pageCount();

    try db.dropNodeFtsIndex("Doc", "body");
    try std.testing.expectEqual(@as(usize, 0), try treeKeyCount(&db.fts_dict_tree));
    try db.createNodeFtsIndex("Doc", "body");
    // Declaring it again indexes the same text into the pages the drop freed.
    try std.testing.expect(db.page_manager.pageCount() < indexed + indexed / 10);

    const results = try db.ftsSearchIndex(.node, "Doc", "body", "common", 5000);
    defer db.freeFtsSearchResults(results);
    try std.testing.expectEqual(@as(usize, ids.len), results.len);
}

test "fts space: compact removes the index releases before 0.15.0 kept, and nothing declared" {
    const allocator = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = try dbPath(&path_buf, "legacy");
    removeFile(path);
    defer removeFile(path);

    const db = try openDb(allocator, path);
    defer db.close();

    // The single unscoped index of 0.14.x, written into the same trees.
    var legacy = FtsIndex.init(
        allocator,
        &db.buffer_pool,
        &db.fts_dict_tree,
        &db.fts_lengths_tree,
        if (db.fts_reverse_tree) |*t| t else null,
        .{},
        .big,
    );
    for (1..801) |doc| {
        var buf: [64]u8 = undefined;
        const text = try std.fmt.bufPrint(&buf, "legacy common w{d} x{d}", .{ doc % 97, doc % 13 });
        _ = try legacy.indexDocument(@intCast(doc), text);
    }
    try db.sync();

    try db.createNodeFtsIndex("Doc", "body");
    var ids: [300]u64 = undefined;
    try indexRound(db, &ids, 0, 4);
    const prefix = lattice.fts.catalog.scopePrefix(.{
        .kind = .node,
        .scope_id = try db.symbol_table.lookup("Doc"),
        .property_id = try db.symbol_table.lookup("body"),
    });
    const declared_terms = blk: {
        var count: usize = 0;
        var iter = try db.fts_dict_tree.range(null, null);
        defer iter.deinit();
        while (try iter.next()) |entry| {
            if (entry.key.len > 4 and std.mem.eql(u8, entry.key[0..4], &prefix)) count += 1;
        }
        break :blk count;
    };
    try std.testing.expect(try treeKeyCount(&db.fts_dict_tree) > declared_terms);

    // The heads of the legacy posting lists, to see them freed.
    var heads: std.ArrayListUnmanaged(u32) = .empty;
    defer heads.deinit(allocator);
    {
        var iter = try db.fts_dict_tree.range(null, null);
        defer iter.deinit();
        while (try iter.next()) |entry| {
            if (entry.key.len > 4 and std.mem.eql(u8, entry.key[0..4], &prefix)) continue;
            const dict_entry = lattice.fts.dictionary.DictionaryEntry.deserialize(entry.value);
            try heads.append(allocator, @intCast(dict_entry.posting_page));
        }
    }
    try std.testing.expect(heads.items.len > 100);

    _ = try db.compact();

    // Every key left belongs to the declared index: its prefixed terms, and
    // its ten- and twelve-byte length and reverse keys.
    try std.testing.expectEqual(declared_terms, try treeKeyCount(&db.fts_dict_tree));
    {
        var iter = try db.fts_lengths_tree.range(null, null);
        defer iter.deinit();
        while (try iter.next()) |entry| try std.testing.expect(entry.key.len == 10 or entry.key.len == 12);
    }
    if (db.fts_reverse_tree) |*tree| {
        var iter = try tree.range(null, null);
        defer iter.deinit();
        while (try iter.next()) |entry| try std.testing.expectEqual(@as(usize, 12), entry.key.len);
    }
    // And the legacy lists' pages are free pages now, unless the compact
    // cut them off the end of the file.
    for (heads.items) |head| {
        if (head >= db.page_manager.pageCount()) continue;
        const frame = try db.buffer_pool.fetchPage(head, .shared);
        defer db.buffer_pool.unpinPage(frame, false);
        const header: *const lattice.storage.page.PageHeader = @ptrCast(@alignCast(frame.data.ptr));
        try std.testing.expectEqual(lattice.storage.page.PageType.free, header.page_type);
    }

    const results = try db.ftsSearchIndex(.node, "Doc", "body", "common", 5000);
    defer db.freeFtsSearchResults(results);
    try std.testing.expectEqual(@as(usize, ids.len), results.len);

    // A second compact finds nothing more to take.
    _ = try db.compact();
    try std.testing.expectEqual(declared_terms, try treeKeyCount(&db.fts_dict_tree));
}
