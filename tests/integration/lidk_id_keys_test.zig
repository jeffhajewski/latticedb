//! Id keys: format 5 writes node, edge and document ids as big-endian B-tree
//! keys, so sequential ids are stored in order; a file created before it, or
//! with `new_database_id_keys = .little`, keeps little-endian keys for good.
//!
//! These tests check both layouts end to end: that a format-5 file scans in
//! ascending id order and fills its trees, and that a little-endian file does
//! everything it did before and still opens in 0.17 and 0.18.

const std = @import("std");
const lattice = @import("lattice");
const compat = @import("compat");

const Database = lattice.storage.database.Database;
const DatabaseConfig = lattice.storage.database.DatabaseConfig;
const FileHeader = lattice.storage.page.FileHeader;
const FileFlags = lattice.storage.page.FileFlags;
const IdKeyOrder = lattice.core.id_key.IdKeyOrder;
const NodeId = lattice.core.types.NodeId;
const EdgeId = lattice.core.types.EdgeId;
const types = lattice.core.types;

fn cleanup(path: []const u8) void {
    compat.fs.cwd().deleteFile(path) catch {};
    var buf: [256]u8 = undefined;
    for ([_][]const u8{ "-wal", "-journal" }) |suffix| {
        const p = std.fmt.bufPrint(&buf, "{s}{s}", .{ path, suffix }) catch return;
        compat.fs.cwd().deleteFile(p) catch {};
    }
}

fn readHeader(path: []const u8) !FileHeader {
    const file = try compat.fs.cwd().openFile(path, .{});
    defer file.close();
    var buf: [@sizeOf(FileHeader)]u8 = undefined;
    _ = try file.preadAll(&buf, 0);
    var header: FileHeader = undefined;
    @memcpy(std.mem.asBytes(&header), &buf);
    return header;
}

/// `node_count` nodes and `edge_count` edges, created in order, a few hundred
/// to a transaction as a loader would.
fn loadSequential(db: *Database, node_count: usize, edge_count: usize) !void {
    const batch = 500;
    var created: usize = 0;
    while (created < node_count) {
        var txn = try db.beginTransaction(.read_write);
        const end = @min(node_count, created + batch);
        while (created < end) : (created += 1) {
            const id = try db.createNode(&txn, &.{"P"});
            try db.setNodeProperty(&txn, id, "n", .{ .int_val = @intCast(created) });
        }
        try db.commitTransaction(&txn);
    }
    var edges: usize = 0;
    while (edges < edge_count) {
        var txn = try db.beginTransaction(.read_write);
        const end = @min(edge_count, edges + batch);
        while (edges < end) : (edges += 1) {
            const source: NodeId = @intCast(edges % node_count + 1);
            const target: NodeId = @intCast((edges * 7 + 1) % node_count + 1);
            _ = try db.createEdgeAndGetId(&txn, source, target, "R");
        }
        try db.commitTransaction(&txn);
    }
}

fn isAscending(ids: []const u64) bool {
    for (ids[1..], ids[0 .. ids.len - 1]) |id, previous| {
        if (id <= previous) return false;
    }
    return true;
}

fn expectAscending(ids: []const u64) !void {
    try std.testing.expect(isAscending(ids));
}

/// Every id the node tree holds, in the tree's own order.
fn nodeTreeIds(db: *Database) ![]u64 {
    var ids: std.ArrayList(u64) = .empty;
    errdefer ids.deinit(std.testing.allocator);
    var iter = try db.node_tree.range(null, null);
    defer iter.deinit();
    while (try iter.next()) |entry| {
        try std.testing.expectEqual(@as(usize, 8), entry.key.len);
        try ids.append(std.testing.allocator, db.id_key_order.decode(entry.key[0..8]));
    }
    return ids.toOwnedSlice(std.testing.allocator);
}

/// Every edge id in the edge id index, in the index's own order.
fn edgeIndexIds(db: *Database) ![]u64 {
    var ids: std.ArrayList(u64) = .empty;
    errdefer ids.deinit(std.testing.allocator);
    var iter = try db.edge_store.scanRefs();
    defer iter.deinit();
    while (try iter.next()) |ref| try ids.append(std.testing.allocator, ref.id);
    return ids.toOwnedSlice(std.testing.allocator);
}

const Fill = struct { node_tree: f64, edge_id_index: f64 };

fn loadAndMeasure(path: []const u8, order: IdKeyOrder) !Fill {
    cleanup(path);
    defer cleanup(path);
    const node_count = 4000;
    const edge_count = 8000;
    var db = try Database.open(std.testing.allocator, path, .{ .create = true, .config = .{
        .enable_fts = false,
        .enable_query_cache = false,
        .new_database_id_keys = order,
    } });
    defer db.close();
    try std.testing.expectEqual(order, db.id_key_order);
    try loadSequential(db, node_count, edge_count);

    const node_ids = try nodeTreeIds(db);
    defer std.testing.allocator.free(node_ids);
    const edge_ids = try edgeIndexIds(db);
    defer std.testing.allocator.free(edge_ids);
    try std.testing.expectEqual(@as(usize, node_count), node_ids.len);
    try std.testing.expectEqual(@as(usize, edge_count), edge_ids.len);

    // `MATCH (n)` is sorted whatever the layout.
    {
        var result = try db.query("MATCH (n:P) RETURN id(n)");
        defer result.deinit();
        var ids: std.ArrayList(u64) = .empty;
        defer ids.deinit(std.testing.allocator);
        for (result.rows) |row| try ids.append(std.testing.allocator, @intCast(row.values[0].int_val));
        try std.testing.expectEqual(@as(usize, node_count), ids.items.len);
        try expectAscending(ids.items);
    }

    // A type-only pattern walks the edge id index: id order only in format 5.
    var edge_rows: std.ArrayList(u64) = .empty;
    defer edge_rows.deinit(std.testing.allocator);
    {
        var result = try db.query("MATCH ()-[r:R]->() RETURN id(r)");
        defer result.deinit();
        for (result.rows) |row| try edge_rows.append(std.testing.allocator, @intCast(row.values[0].int_val));
    }
    try std.testing.expectEqual(@as(usize, edge_count), edge_rows.items.len);

    switch (order) {
        .big => {
            try expectAscending(node_ids);
            try expectAscending(edge_ids);
            try expectAscending(edge_rows.items);
            try std.testing.expectEqual(@as(u64, 1), node_ids[0]);
            try std.testing.expectEqual(@as(u64, node_count), node_ids[node_ids.len - 1]);
        },
        .little => {
            // Past 256 the little-endian order is not id order: 256 comes
            // right after 0 and before 1.
            try std.testing.expect(!isAscending(node_ids));
            try std.testing.expect(!isAscending(edge_ids));
            try std.testing.expectEqual(@as(u64, 256), node_ids[0]);
            try std.testing.expectEqualSlices(u64, edge_ids, edge_rows.items);
        },
    }

    const node_stats = try db.node_tree.spaceStats();
    const edge_stats = try db.edge_id_tree.spaceStats();
    return .{ .node_tree = node_stats.leafFill(), .edge_id_index = edge_stats.leafFill() };
}

test "id keys: a format-5 file scans in ascending id order and its id trees fill, where little-endian keys do not" {
    const ordered = try loadAndMeasure("/tmp/lidk_fill_big.ltdb", .big);
    const legacy = try loadAndMeasure("/tmp/lidk_fill_little.ltdb", .little);
    if (ordered.node_tree < 0.9 or ordered.edge_id_index < 0.9 or
        legacy.node_tree >= ordered.node_tree or legacy.edge_id_index >= ordered.edge_id_index)
    {
        std.debug.print("leaf fill: big-endian node tree {d:.3}, edge id index {d:.3}; little-endian {d:.3}, {d:.3}\n", .{
            ordered.node_tree, ordered.edge_id_index, legacy.node_tree, legacy.edge_id_index,
        });
        return error.TestUnexpectedResult;
    }
}

test "id keys: a new database records ordered id keys and requires a format-5 reader, and keeps them on reopen" {
    const path = "/tmp/lidk_new_header.ltdb";
    cleanup(path);
    defer cleanup(path);
    {
        var db = try Database.open(std.testing.allocator, path, .{ .create = true, .config = .{ .enable_fts = false } });
        defer db.close();
        try std.testing.expectEqual(IdKeyOrder.big, db.id_key_order);
        _ = try db.createNode(null, &.{"P"});
    }
    const header = try readHeader(path);
    try std.testing.expect(header.flags & FileFlags.ORDERED_ID_KEYS != 0);
    try std.testing.expectEqual(types.ORDERED_ID_KEYS_READER_VERSION, header.min_reader_version);
    try std.testing.expectEqual(types.FORMAT_VERSION, header.format_version);
    try std.testing.expect(header.readerReasons() & FileHeader.ReaderReason.ordered_id_keys != 0);

    // Reopened, read-only and read-write, asking for nothing in particular
    // and for the other order: the file's order wins.
    inline for (.{ true, false }) |read_only| {
        var db = try Database.open(std.testing.allocator, path, .{
            .read_only = read_only,
            .config = .{ .enable_fts = false, .new_database_id_keys = .little },
        });
        defer db.close();
        try std.testing.expectEqual(IdKeyOrder.big, db.id_key_order);
        try std.testing.expect(try db.nodeExists(1));
    }
    try std.testing.expect((try readHeader(path)).flags & FileFlags.ORDERED_ID_KEYS != 0);
}

/// A different direction for every node.
fn unitVector(i: usize) [4]f32 {
    const x: f32 = @floatFromInt(i);
    return .{ 1, x / 50, @sin(x), @cos(x * 1.7) };
}

const legacy_config = DatabaseConfig{
    .enable_fts = true,
    .enable_vector = true,
    .vector_dimensions = 4,
    .new_database_id_keys = .little,
};

const DOCS = 300;

fn checkLegacyContents(db: *Database, deleted_node: NodeId, deleted_edge: EdgeId, edge_total: usize) !void {
    try std.testing.expectEqual(IdKeyOrder.little, db.id_key_order);

    // Full scans.
    const ids = try db.getAllNodeIdsInTxn(null);
    defer std.testing.allocator.free(ids);
    try std.testing.expectEqual(@as(usize, DOCS - 1), ids.len);
    try expectAscending(ids);
    for (ids) |id| try std.testing.expect(id != deleted_node);
    const edges = try db.scanEdgesInTxn(null, null, 0);
    defer db.freeEdgeInfos(edges);
    try std.testing.expectEqual(edge_total - 1, edges.len);
    for (edges) |e| try std.testing.expect(e.id != deleted_edge);

    // Point reads, past 256.
    var text = (try db.getNodeProperty(270, "text")).?;
    defer text.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("updated two seventy", text.string_val);
    try std.testing.expect(!try db.nodeExists(deleted_node));

    // The node tree's keys are little-endian on disk.
    const little = IdKeyOrder.little.encode(257);
    const big = IdKeyOrder.big.encode(257);
    try std.testing.expect(try db.node_tree.contains(&little));
    try std.testing.expect(!try db.node_tree.contains(&big));

    // Full-text: one document in fifty says "special".
    {
        const found = try db.ftsSearchIndex(.node, "Doc", "text", "special", 100);
        defer db.freeFtsSearchResults(found);
        try std.testing.expectEqual(@as(usize, DOCS / 50), found.len);
        for (found) |doc| try std.testing.expectEqual(@as(u64, 0), (doc.doc_id - 1) % 50);
        const moved = try db.ftsSearchIndex(.node, "Doc", "text", "seventy", 10);
        defer db.freeFtsSearchResults(moved);
        try std.testing.expectEqual(@as(usize, 1), moved.len);
        try std.testing.expectEqual(@as(u64, 270), moved[0].doc_id);
    }

    // Vectors, past 256 too.
    for ([_]usize{ 3, 280 }) |i| {
        const q = unitVector(i);
        const results = try db.vectorSearch(&q, 1, 200);
        defer db.freeVectorSearchResults(results);
        try std.testing.expectEqual(@as(NodeId, i), results[0].node_id);
    }
}

test "id keys: a little-endian database does everything, reopens as one, and stays readable by format 4" {
    const path = "/tmp/lidk_legacy.ltdb";
    cleanup(path);
    defer cleanup(path);

    const deleted_node: NodeId = 260;
    var deleted_edge: EdgeId = 0;
    var edge_total: usize = 0;
    {
        var db = try Database.open(std.testing.allocator, path, .{ .create = true, .config = legacy_config });
        defer db.close();
        try std.testing.expectEqual(IdKeyOrder.little, db.id_key_order);
        try db.createNodeFtsIndex("Doc", "text");

        var txn = try db.beginTransaction(.read_write);
        for (0..DOCS) |i| {
            const id = try db.createNode(&txn, &.{"Doc"});
            try std.testing.expectEqual(@as(NodeId, i + 1), id);
            var buf: [64]u8 = undefined;
            const text = try std.fmt.bufPrint(&buf, "document {d} common{s}", .{ i, if (i % 50 == 0) " special" else "" });
            try db.setNodeProperty(&txn, id, "text", .{ .string_val = text });
        }
        // Edges among the first hundred nodes, so node 260 has none.
        for (0..300) |i| {
            const id = try db.createEdgeAndGetId(&txn, @intCast(i % 100 + 1), @intCast((i * 3) % 100 + 1), "R");
            if (i == 4) deleted_edge = id;
            edge_total += 1;
        }
        try db.commitTransaction(&txn);
        for (1..DOCS + 1) |i| {
            const v = unitVector(i);
            try db.setNodeVector(@intCast(i), &v);
        }

        // Update, delete.
        txn = try db.beginTransaction(.read_write);
        try db.setNodeProperty(&txn, 270, "text", .{ .string_val = "updated two seventy" });
        try db.deleteNode(&txn, deleted_node);
        try db.deleteEdgeById(&txn, deleted_edge);
        try db.commitTransaction(&txn);

        try checkLegacyContents(db, deleted_node, deleted_edge, edge_total);
    }

    const header = try readHeader(path);
    try std.testing.expect(header.flags & FileFlags.ORDERED_ID_KEYS == 0);
    try std.testing.expectEqual(IdKeyOrder.little, header.idKeyOrder());
    // The changefeed, the full-text positions: things 0.17 reads.
    try std.testing.expect(header.min_reader_version <= types.REASONS_READER_VERSION);
    try std.testing.expect(header.readerReasons() & FileHeader.ReaderReason.ordered_id_keys == 0);
    try std.testing.expectEqual(types.FORMAT_VERSION, header.min_writer_version);

    // Reopened without options, read-only and read-write, and written to.
    {
        var db = try Database.open(std.testing.allocator, path, .{ .read_only = true, .config = .{} });
        defer db.close();
        try checkLegacyContents(db, deleted_node, deleted_edge, edge_total);
    }
    {
        var db = try Database.open(std.testing.allocator, path, .{ .config = .{} });
        defer db.close();
        try checkLegacyContents(db, deleted_node, deleted_edge, edge_total);
        try std.testing.expectEqual(@as(NodeId, DOCS + 1), try db.createNode(null, &.{"Doc"}));
        const next_edge = try db.createEdgeAndGetId(null, 1, 2, "R");
        try std.testing.expectEqual(@as(EdgeId, edge_total + 1), next_edge);
        try db.deleteEdgeById(null, next_edge);
        try db.deleteNode(null, DOCS + 1);
    }
    try std.testing.expect((try readHeader(path)).min_reader_version <= types.REASONS_READER_VERSION);
    try std.testing.expect((try readHeader(path)).flags & FileFlags.ORDERED_ID_KEYS == 0);
}
