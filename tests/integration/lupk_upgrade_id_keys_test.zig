//! Upgrading id keys: `Database.upgradeIdKeysTo` writes a format-5 copy of a
//! database created with little-endian id keys, checks it against the
//! original, and `id_key_upgrade.replaceWithUpgraded` puts it in the
//! original's place.
//!
//! The tests build an old-layout database with something in every tree that
//! the upgrade rewrites, upgrade it, and compare what queries, searches and
//! the changefeed return before and after; then fill the pages the upgrade
//! freed and compare again, because a page freed while still in use shows up
//! only once it is reused.

const std = @import("std");
const lattice = @import("lattice");
const compat = @import("compat");

const Database = lattice.storage.database.Database;
const DatabaseConfig = lattice.storage.database.DatabaseConfig;
const ResultValue = lattice.storage.database.ResultValue;
const FtsSearchResult = lattice.storage.database.FtsSearchResult;
const FileHeader = lattice.storage.page.FileHeader;
const FileFlags = lattice.storage.page.FileFlags;
const IdKeyOrder = lattice.core.id_key.IdKeyOrder;
const NodeId = lattice.core.types.NodeId;
const EdgeId = lattice.core.types.EdgeId;
const PropertyValue = lattice.core.types.PropertyValue;
const types = lattice.core.types;
const upgrade = lattice.storage.id_key_upgrade;

const allocator = std.testing.allocator;

pub fn cleanup(path: []const u8) void {
    var buf: [512]u8 = undefined;
    for ([_][]const u8{ "", "-wal", "-journal", ".upgrade-id-keys", ".upgrade-id-keys-wal", ".upgrade-id-keys-journal", ".upgrade-id-keys.partial", ".copy", ".copy-wal", ".copy-journal", ".copy.partial" }) |suffix| {
        const p = std.fmt.bufPrint(&buf, "{s}{s}", .{ path, suffix }) catch continue;
        compat.fs.cwd().deleteFile(p) catch {};
    }
}

pub fn exists(path: []const u8) bool {
    compat.fs.cwd().access(path, .{}) catch return false;
    return true;
}

pub fn readHeader(path: []const u8) !FileHeader {
    const file = try compat.fs.cwd().openFile(path, .{});
    defer file.close();
    var buf: [@sizeOf(FileHeader)]u8 = undefined;
    _ = try file.preadAll(&buf, 0);
    var header: FileHeader = undefined;
    @memcpy(std.mem.asBytes(&header), &buf);
    return header;
}

pub fn fileSize(path: []const u8) !u64 {
    const file = try compat.fs.cwd().openFile(path, .{});
    defer file.close();
    return (try file.stat()).size;
}

/// SHA-256 of a database file, except the header's modification time, which
/// every read-write open and close sets.
pub fn hashMain(path: []const u8) ![32]u8 {
    return hashFiles(path, &.{""});
}

/// SHA-256 of a database file and the files beside it with these suffixes,
/// as found, except the header's modification time.
pub fn hashFiles(path: []const u8, suffixes: []const []const u8) ![32]u8 {
    const stamp = @offsetOf(FileHeader, "modified_timestamp");
    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    var name_buf: [512]u8 = undefined;
    for (suffixes) |suffix| {
        const p = try std.fmt.bufPrint(&name_buf, "{s}{s}", .{ path, suffix });
        const file = compat.fs.cwd().openFile(p, .{}) catch {
            hasher.update("absent|");
            continue;
        };
        defer file.close();
        var buf: [65536]u8 = undefined;
        var offset: u64 = 0;
        while (true) {
            const n = try file.preadAll(&buf, offset);
            if (n == 0) break;
            if (offset == 0 and suffix.len == 0 and n >= stamp + 8) @memset(buf[stamp..][0..8], 0);
            hasher.update(buf[0..n]);
            offset += n;
        }
        hasher.update("|");
    }
    var out: [32]u8 = undefined;
    hasher.final(&out);
    return out;
}

// ============================================================================
// What a database holds, as its queries, searches and changefeed tell it
// ============================================================================

const Section = enum {
    doc_nodes,
    bulk_nodes,
    extra_label,
    node_ids,
    edges,
    incoming,
    fts_text,
    fts_title,
    fts_edges,
    vectors,
    property_index,
    changefeed,
};

/// A digest per section, so a difference says where it is.
pub const Fingerprint = struct {
    digests: [@typeInfo(Section).@"enum".fields.len][32]u8 = undefined,
    rows: [@typeInfo(Section).@"enum".fields.len]u64 = [_]u64{0} ** @typeInfo(Section).@"enum".fields.len,

    pub fn expectEqual(expected: Fingerprint, actual: Fingerprint) !void {
        for (std.enums.values(Section)) |section| {
            const i = @intFromEnum(section);
            if (expected.rows[i] != actual.rows[i] or !std.mem.eql(u8, &expected.digests[i], &actual.digests[i])) {
                std.debug.print("upgrade changed {s}: {d} rows before, {d} after\n", .{ @tagName(section), expected.rows[i], actual.rows[i] });
                return error.TestExpectedEqual;
            }
        }
    }
};

const Hasher = std.crypto.hash.sha2.Sha256;

/// How many changefeed events a fingerprint reads at each end.
const CHANGEFEED_SPAN = 4000;

fn hashInt(h: *Hasher, v: anytype) void {
    h.update(std.mem.asBytes(&v));
}

fn hashResult(h: *Hasher, value: ResultValue) void {
    h.update(&[_]u8{@intFromEnum(std.meta.activeTag(value))});
    switch (value) {
        .null_val => {},
        .bool_val => |v| hashInt(h, @as(u8, @intFromBool(v))),
        .int_val => |v| hashInt(h, v),
        .float_val => |v| hashInt(h, v),
        .string_val, .bytes_val => |v| {
            hashInt(h, v.len);
            h.update(v);
        },
        .node_id => |v| hashInt(h, v),
        .edge_id => |v| hashInt(h, v),
        .vector_val => |v| h.update(std.mem.sliceAsBytes(v)),
        .list_val => |v| {
            hashInt(h, v.len);
            for (v) |item| hashResult(h, item);
        },
        .map_val => |v| {
            hashInt(h, v.len);
            for (v) |entry| {
                h.update(entry.key);
                hashResult(h, entry.value);
            }
        },
    }
}

fn hashProperty(h: *Hasher, value: PropertyValue) void {
    h.update(&[_]u8{@intFromEnum(std.meta.activeTag(value))});
    switch (value) {
        .null_val => {},
        .bool_val => |v| hashInt(h, @as(u8, @intFromBool(v))),
        .int_val => |v| hashInt(h, v),
        .float_val => |v| hashInt(h, v),
        .string_val, .bytes_val => |v| {
            hashInt(h, v.len);
            h.update(v);
        },
        .vector_val => |v| h.update(std.mem.sliceAsBytes(v)),
        .list_val => |v| {
            hashInt(h, v.len);
            for (v) |item| hashProperty(h, item);
        },
        .map_val => |v| {
            hashInt(h, v.len);
            for (v) |entry| {
                h.update(entry.key);
                hashProperty(h, entry.value);
            }
        },
    }
}

fn hashQuery(h: *Hasher, db: *Database, cypher: []const u8) !u64 {
    var result = db.query(cypher) catch |err| {
        std.debug.print("query failed: {s}: {s}\n", .{ cypher, @errorName(err) });
        return err;
    };
    defer result.deinit();
    for (result.rows) |row| {
        for (row.values) |value| hashResult(h, value);
        h.update("\n");
    }
    return result.rows.len;
}

fn hashFts(h: *Hasher, db: *Database, kind: anytype, scope: []const u8, property: []const u8, terms: []const []const u8) !u64 {
    var rows: u64 = 0;
    for (terms) |term| {
        const found = try db.ftsSearchIndex(kind, scope, property, term, 100_000);
        defer db.freeFtsSearchResults(found);
        // Ties may come back in any order; what matters is which documents
        // match and with what score.
        std.mem.sort(FtsSearchResult, found, {}, struct {
            fn lessThan(_: void, a: FtsSearchResult, b: FtsSearchResult) bool {
                return a.doc_id < b.doc_id;
            }
        }.lessThan);
        h.update(term);
        for (found) |doc| {
            hashInt(h, doc.doc_id);
            hashInt(h, doc.score);
        }
        rows += found.len;
    }
    return rows;
}

/// Everything the tests compare. Nodes past `last_node` (in the store
/// section; the queries name labels) and changefeed events after
/// `last_event` are left out, so the same fingerprint can be taken after
/// more data is added.
pub fn fingerprintUpTo(db: *Database, last_event: u64, last_node: NodeId) !Fingerprint {
    var fp = Fingerprint{};
    for (std.enums.values(Section)) |section| {
        var h = Hasher.init(.{});
        const rows: u64 = switch (section) {
            .doc_nodes => try hashQuery(&h, db, "MATCH (n:Doc) RETURN id(n), n.text, n.title, n.rank, n.big, n.tags, n.score ORDER BY id(n)"),
            .bulk_nodes => try hashQuery(&h, db, "MATCH (n:Bulk) RETURN id(n), n.k ORDER BY id(n)"),
            .extra_label => try hashQuery(&h, db, "MATCH (n:Doc:Extra) RETURN id(n) ORDER BY id(n)"),
            .node_ids => blk: {
                // Through the store rather than a query: every id, sorted.
                const ids = try db.getAllNodeIdsInTxn(null);
                defer allocator.free(ids);
                var n: u64 = 0;
                for (ids) |id| {
                    if (id > last_node) continue;
                    var node = try db.node_store.get(id);
                    defer node.deinit(db.allocator);
                    hashInt(&h, id);
                    h.update(std.mem.sliceAsBytes(node.labels));
                    for (node.properties) |prop| {
                        hashInt(&h, prop.key_id);
                        hashProperty(&h, prop.value);
                    }
                    n += 1;
                }
                break :blk n;
            },
            .edges => try hashQuery(&h, db, "MATCH (a)-[r]->(b) RETURN id(r), id(a), id(b), type(r), r.w, r.note ORDER BY id(r)"),
            .incoming => try hashQuery(&h, db, "MATCH (a:Doc)<-[r:R]-(b) RETURN id(a), id(r), id(b) ORDER BY id(a), id(r)"),
            .fts_text => try hashFts(&h, db, .node, "Doc", "text", &.{ "special", "common", "document", "seventy" }),
            .fts_title => try hashFts(&h, db, .node, "Doc", "title", &.{ "alpha", "beta" }),
            .fts_edges => try hashFts(&h, db, .edge, "R", "note", &.{ "link", "strong" }),
            .vectors => blk: {
                var n: u64 = 0;
                for ([_]usize{ 1, 3, 77, 280, HIGH_DOC_START + 5, HIGH_DOC_START + 40 }) |i| {
                    const q = unitVector(i);
                    const results = try db.vectorSearch(&q, 5, 200);
                    defer db.freeVectorSearchResults(results);
                    for (results) |r| {
                        hashInt(&h, r.node_id);
                        hashInt(&h, r.distance);
                    }
                    n += results.len;
                }
                break :blk n;
            },
            .property_index => (try hashQuery(&h, db, "MATCH (n:Doc) WHERE n.rank >= 100 AND n.rank < 160 RETURN id(n), n.rank ORDER BY n.rank, id(n)")) +
                (try hashQuery(&h, db, "MATCH (n:Doc) WHERE n.rank = 7 RETURN id(n) ORDER BY id(n)")),
            .changefeed => blk: {
                // Read from the start, and the end up to `last_event`. The
                // middle is tens of thousands of bulk-node events, which take
                // minutes to decode in a debug build; the stream trees that
                // hold them are not converted, and their pages are covered by
                // the page accounting.
                const status = try db.changefeedStatus();
                hashInt(&h, status.oldest_sequence orelse 0);
                var n: u64 = 0;
                const tail_from = if (last_event > CHANGEFEED_SPAN) last_event - CHANGEFEED_SPAN else 0;
                for ([_][2]u64{ .{ 0, @min(CHANGEFEED_SPAN, last_event) }, .{ @max(tail_from, CHANGEFEED_SPAN), last_event } }) |span| {
                    var after = span[0];
                    while (after < span[1]) {
                        var batch = try db.readChanges(after, 1000, 0);
                        defer batch.deinit();
                        if (batch.records.len == 0) break;
                        for (batch.records) |record| {
                            if (record.sequence > span[1]) break;
                            hashInt(&h, record.sequence);
                            h.update(record.kind);
                            hashProperty(&h, record.payload);
                            n += 1;
                        }
                        after = batch.records[batch.records.len - 1].sequence;
                    }
                }
                break :blk n;
            },
        };
        h.final(&fp.digests[@intFromEnum(section)]);
        fp.rows[@intFromEnum(section)] = rows;
    }
    return fp;
}

pub fn fingerprint(db: *Database, last_event: u64) !Fingerprint {
    return fingerprintUpTo(db, last_event, std.math.maxInt(NodeId));
}

// ============================================================================
// An old-layout database with something in every tree the upgrade rewrites
// ============================================================================

/// A different direction for every node.
fn unitVector(i: usize) [4]f32 {
    const x: f32 = @floatFromInt(i);
    return .{ 1, x / 50, @sin(x), @cos(x * 1.7) };
}

pub const legacy_config = DatabaseConfig{
    .enable_fts = true,
    .enable_vector = true,
    .vector_dimensions = 4,
    .new_database_id_keys = .little,
};

const DOCS = 300;
/// Bulk nodes push the ids past 65,536, so a key has three significant bytes.
const BULK = 66_000;
/// Documents created after the bulk nodes, with ids past 65,536.
const HIGH_DOCS = 60;
const HIGH_DOC_START = DOCS + BULK + 1;
const DELETED_DOC: NodeId = 260;

pub const Built = struct {
    next_node: NodeId,
    next_edge: EdgeId,
    last_event: u64,
};

fn addDoc(db: *Database, txn: anytype, i: usize) !NodeId {
    const extra = i % 3 == 0;
    const id = if (extra) try db.createNode(txn, &.{ "Doc", "Extra" }) else try db.createNode(txn, &.{"Doc"});
    var buf: [96]u8 = undefined;
    const text = try std.fmt.bufPrint(&buf, "document {d} common{s}", .{ i, if (i % 50 == 0) " special" else "" });
    try db.setNodeProperty(txn, id, "text", .{ .string_val = text });
    try db.setNodeProperty(txn, id, "title", .{ .string_val = if (i % 2 == 0) "alpha title" else "beta title" });
    try db.setNodeProperty(txn, id, "rank", .{ .int_val = @intCast(i % 200) });
    try db.setNodeProperty(txn, id, "score", .{ .float_val = @as(f64, @floatFromInt(i)) / 7.0 });
    const tags = [_]PropertyValue{ .{ .string_val = "t" }, .{ .int_val = @intCast(i) } };
    try db.setNodeProperty(txn, id, "tags", .{ .list_val = &tags });
    if (i % 37 == 0) {
        // Larger than a page, so the record overflows.
        const big = try allocator.alloc(u8, 9000);
        defer allocator.free(big);
        @memset(big, @intCast('a' + i % 26));
        try db.setNodeProperty(txn, id, "big", .{ .string_val = big });
    }
    return id;
}

/// Build the database at `path` with little-endian id keys and close it.
pub fn buildLegacy(path: []const u8, bulk: usize) !Built {
    // Each vector is its own commit; without a sync on each the build takes
    // seconds rather than minutes. The close syncs everything.
    var build_config = legacy_config;
    build_config.sync_mode = .normal;
    var db = try Database.open(allocator, path, .{ .create = true, .config = build_config });
    defer db.close();
    try std.testing.expectEqual(IdKeyOrder.little, db.id_key_order);
    try db.createNodeFtsIndex("Doc", "text");
    try db.createNodeFtsIndex("Doc", "title");
    try db.createEdgeFtsIndex("R", "note");
    try db.createNodePropertyIndex("Doc", "rank");

    var docs: std.ArrayList(NodeId) = .empty;
    defer docs.deinit(allocator);
    {
        var txn = try db.beginTransaction(.read_write);
        for (0..DOCS) |i| try docs.append(allocator, try addDoc(db, &txn, i));
        try db.commitTransaction(&txn);
    }
    var made: usize = 0;
    while (made < bulk) {
        var txn = try db.beginTransaction(.read_write);
        const end = @min(bulk, made + 6000);
        while (made < end) : (made += 1) {
            const id = try db.createNode(&txn, &.{"Bulk"});
            if (made % 10 == 0) try db.setNodeProperty(&txn, id, "k", .{ .int_val = @intCast(made) });
        }
        try db.commitTransaction(&txn);
    }
    {
        var txn = try db.beginTransaction(.read_write);
        for (0..HIGH_DOCS) |i| try docs.append(allocator, try addDoc(db, &txn, DOCS + bulk + i));
        try db.commitTransaction(&txn);
    }
    for (docs.items, 0..) |id, i| {
        const v = unitVector(if (i < DOCS) i + 1 else HIGH_DOC_START + (i - DOCS));
        try db.setNodeVector(id, &v);
    }

    // Edges among the documents, past 256 of them, some with properties
    // and a full-text note. Document 260 gets none, so it can be deleted.
    var deleted_edges: [3]EdgeId = undefined;
    {
        var txn = try db.beginTransaction(.read_write);
        for (0..700) |i| {
            const a = docs.items[i % docs.items.len];
            const b = docs.items[(i * 7 + 3) % docs.items.len];
            if (a == DELETED_DOC or b == DELETED_DOC) continue;
            const id = try db.createEdgeAndGetId(&txn, a, b, if (i % 5 == 0) "S" else "R");
            if (i % 4 == 0) try db.setEdgePropertyById(&txn, id, "w", .{ .int_val = @intCast(i) });
            if (i % 3 == 0) try db.setEdgePropertyById(&txn, id, "note", .{ .string_val = if (i % 2 == 0) "strong link" else "link" });
            if (i == 10) deleted_edges[0] = id;
            if (i == 300) deleted_edges[1] = id;
            if (i == 650) deleted_edges[2] = id;
        }
        try db.commitTransaction(&txn);
    }

    // Gaps: a document, some bulk nodes, some edges.
    {
        var txn = try db.beginTransaction(.read_write);
        try db.deleteNode(&txn, DELETED_DOC);
        var k: NodeId = DOCS + 1000;
        while (k < DOCS + bulk) : (k += 4999) try db.deleteNode(&txn, k);
        for (deleted_edges) |id| try db.deleteEdgeById(&txn, id);
        try db.setNodeProperty(&txn, 270, "text", .{ .string_val = "updated two seventy common" });
        try db.commitTransaction(&txn);
    }

    const status = try db.changefeedStatus();
    return .{
        .next_node = db.node_store.next_id,
        .next_edge = db.edge_store.next_edge_id,
        .last_event = status.last_sequence,
    };
}

/// Upgrade the closed database at `path` the way `lattice compact
/// --upgrade-id-keys` does, without the compact.
pub fn upgradeInPlace(path: []const u8, config: DatabaseConfig) !upgrade.UpgradeStats {
    var copy_buf: [512]u8 = undefined;
    const copy = try std.fmt.bufPrint(&copy_buf, "{s}.upgrade-id-keys", .{path});
    var db = try Database.open(allocator, path, .{ .config = config });
    var open = true;
    defer if (open) db.close();
    var diag = upgrade.Diagnostic{};
    const stats = db.upgradeIdKeysTo(copy, &diag) catch |err| {
        std.debug.print("upgrade failed: {s}: {s}\n", .{ @errorName(err), diag.message() });
        return err;
    };
    if (stats.already_ordered) return stats;
    _ = try db.checkpoint(.truncate);
    const held = try db.closeKeepingLock();
    open = false;
    try upgrade.replaceWithUpgraded(allocator, path, copy, held, null);
    return stats;
}

/// Write the upgraded copy of the database at `path` to `copy`, and close
/// the database keeping its file locked, as `compact --upgrade-id-keys`
/// does before the swap. Returns the held file.
pub fn upgradeCopyHeld(path: []const u8, copy: []const u8) !lattice.storage.vfs.File {
    var db = try Database.open(allocator, path, .{ .config = legacy_config });
    var open = true;
    defer if (open) db.close();
    _ = try db.upgradeIdKeysTo(copy, null);
    _ = try db.checkpoint(.truncate);
    const held = try db.closeKeepingLock();
    open = false;
    return held;
}

fn expectAscending(ids: []const u64) !void {
    if (ids.len < 2) return;
    for (ids[1..], ids[0 .. ids.len - 1]) |id, previous| try std.testing.expect(id > previous);
}

fn nodeTreeIds(db: *Database) ![]u64 {
    var ids: std.ArrayList(u64) = .empty;
    errdefer ids.deinit(allocator);
    var iter = try db.node_tree.range(null, null);
    defer iter.deinit();
    while (try iter.next()) |entry| try ids.append(allocator, db.id_key_order.decode(entry.key[0..8]));
    return ids.toOwnedSlice(allocator);
}

fn edgeIndexIds(db: *Database) ![]u64 {
    var ids: std.ArrayList(u64) = .empty;
    errdefer ids.deinit(allocator);
    var iter = try db.edge_store.scanRefs();
    defer iter.deinit();
    while (try iter.next()) |ref| try ids.append(allocator, ref.id);
    return ids.toOwnedSlice(allocator);
}

/// Every page reads back with its checksum, and none belongs to two
/// structures.
pub fn expectPagesSound(db: *Database) !upgrade.PageAccount {
    const buf = try allocator.alignedAlloc(u8, std.mem.Alignment.fromByteUnits(4096), db.page_manager.getPageSize());
    defer allocator.free(buf);
    var page_id: u32 = 1;
    while (page_id < db.page_manager.pageCount()) : (page_id += 1) {
        _ = try db.page_manager.readPageForScan(page_id, buf);
    }
    var diag = upgrade.Diagnostic{};
    return upgrade.accountPages(allocator, &db.buffer_pool, db.page_manager.getHeader(), &diag) catch |err| {
        std.debug.print("page accounting failed: {s}\n", .{diag.message()});
        return err;
    };
}

fn expectFormat5(path: []const u8) !void {
    const header = try readHeader(path);
    try std.testing.expect(header.flags & FileFlags.ORDERED_ID_KEYS != 0);
    try std.testing.expectEqual(IdKeyOrder.big, header.idKeyOrder());
    try std.testing.expectEqual(types.ORDERED_ID_KEYS_READER_VERSION, header.min_reader_version);
    try std.testing.expect(header.readerReasons() & FileHeader.ReaderReason.ordered_id_keys != 0);
}

test "upgrade id keys: an old-layout database with everything in it comes through whole, and stays whole once the freed pages are reused" {
    const path = "/tmp/lupk_full.ltdb";
    cleanup(path);
    defer cleanup(path);

    const built = try buildLegacy(path, BULK);
    const header_before = try readHeader(path);
    try std.testing.expectEqual(IdKeyOrder.little, header_before.idKeyOrder());
    const uuid_before = header_before.file_uuid;

    var before: Fingerprint = undefined;
    {
        var db = try Database.open(allocator, path, .{ .config = legacy_config });
        defer db.close();
        before = try fingerprint(db, built.last_event);
    }
    // The bulk nodes and documents are all there, past 65,536.
    try std.testing.expectEqual(@as(u64, DOCS + HIGH_DOCS - 1), before.rows[@intFromEnum(Section.doc_nodes)]);
    try std.testing.expect(built.next_node > 65_536);

    const size_before = try fileSize(path);
    const stats = try upgradeInPlace(path, legacy_config);
    try std.testing.expect(!stats.already_ordered);
    for (upgrade.IdTree.all) |tree| {
        const t = stats.tree(tree);
        try std.testing.expect(t.present);
        try std.testing.expect(t.entries > 0);
    }
    try std.testing.expectEqual(@as(u64, DOCS + BULK + HIGH_DOCS - 1 - 14), stats.tree(.node).entries);
    try std.testing.expect(stats.entries_verified > stats.tree(.node).entries);
    try std.testing.expect(!exists(path ++ ".upgrade-id-keys"));
    try expectFormat5(path);
    const header_after = try readHeader(path);
    try std.testing.expect(!std.mem.eql(u8, &uuid_before, &header_after.file_uuid));

    var after_writes: Fingerprint = undefined;
    var last_event: u64 = built.last_event;
    var last_node: NodeId = 0;
    {
        var db = try Database.open(allocator, path, .{ .config = legacy_config });
        defer db.close();
        try std.testing.expectEqual(IdKeyOrder.big, db.id_key_order);
        try Fingerprint.expectEqual(before, try fingerprint(db, built.last_event));

        // Scans come back in id order now.
        const node_ids = try nodeTreeIds(db);
        defer allocator.free(node_ids);
        try expectAscending(node_ids);
        const edge_ids = try edgeIndexIds(db);
        defer allocator.free(edge_ids);
        try expectAscending(edge_ids);
        {
            var result = try db.query("MATCH ()-[r:R]->() RETURN id(r)");
            defer result.deinit();
            var ids: std.ArrayList(u64) = .empty;
            defer ids.deinit(allocator);
            for (result.rows) |row| try ids.append(allocator, @intCast(row.values[0].int_val));
            try expectAscending(ids.items);
        }

        // The trees the upgrade rebuilt are full.
        try std.testing.expect((try db.node_tree.spaceStats()).leafFill() > 0.9);
        try std.testing.expect((try db.edge_id_tree.spaceStats()).leafFill() > 0.85);

        var account = try expectPagesSound(db);
        const freed_pages = account.free_pages;
        account.deinit();
        std.debug.print(
            "upgrade id keys: {d} bytes before, {d} upgraded ({d} pages freed, {d} on the freelist)\n",
            .{ size_before, try fileSize(path), stats.pages_freed, freed_pages },
        );

        // New writes continue the ids, and work in every index.
        var txn = try db.beginTransaction(.read_write);
        const doc = try addDoc(db, &txn, 9999);
        try std.testing.expectEqual(built.next_node, doc);
        const edge = try db.createEdgeAndGetId(&txn, doc, 1, "R");
        try std.testing.expectEqual(built.next_edge, edge);
        try db.setEdgePropertyById(&txn, edge, "note", .{ .string_val = "fresh link" });
        try db.commitTransaction(&txn);
        const v = unitVector(5000);
        try db.setNodeVector(doc, &v);
        {
            const found = try db.ftsSearchIndex(.node, "Doc", "text", "9999", 10);
            defer db.freeFtsSearchResults(found);
            try std.testing.expectEqual(@as(usize, 1), found.len);
            try std.testing.expectEqual(doc, found[0].doc_id);
            const results = try db.vectorSearch(&v, 1, 200);
            defer db.freeVectorSearchResults(results);
            try std.testing.expectEqual(doc, results[0].node_id);
        }
        last_event = (try db.changefeedStatus()).last_sequence;
        last_node = doc;
        after_writes = try fingerprintUpTo(db, last_event, last_node);
    }

    // Reopened (compared below, once the freed pages are full).
    {
        var db = try Database.open(allocator, path, .{ .config = legacy_config });
        defer db.close();
        try std.testing.expectEqual(IdKeyOrder.big, db.id_key_order);

        // Use up every page the upgrade freed: filler nodes with records of
        // a few kilobytes, until the freelist is empty. A page freed while
        // something still used it would now hold filler, and the database
        // would read back differently or fail the page accounting.
        var account = try expectPagesSound(db);
        account.deinit();
        const filler = try allocator.alloc(u8, 3000);
        defer allocator.free(filler);
        @memset(filler, 'f');
        var batches: usize = 0;
        var filler_nodes: usize = 0;
        while (batches < 100) : (batches += 1) {
            _ = try db.checkpoint(.truncate);
            if (db.page_manager.getHeader().freelist_page == types.NULL_PAGE and
                db.page_manager.deferredFrees().len == 0) break;
            var txn = try db.beginTransaction(.read_write);
            for (0..100) |_| {
                const id = try db.createNode(&txn, &.{"Filler"});
                try db.setNodeProperty(&txn, id, "f", .{ .string_val = filler });
                filler_nodes += 1;
            }
            try db.commitTransaction(&txn);
        }
        try std.testing.expectEqual(types.NULL_PAGE, db.page_manager.getHeader().freelist_page);
        var full = try expectPagesSound(db);
        try std.testing.expectEqual(@as(u64, 0), full.free_pages);
        full.deinit();
        try Fingerprint.expectEqual(after_writes, try fingerprintUpTo(db, last_event, last_node));
        var result = try db.query("MATCH (n:Filler) RETURN count(n)");
        defer result.deinit();
        try std.testing.expectEqual(@as(i64, @intCast(filler_nodes)), result.rows[0].values[0].int_val);
    }

    // And once more after a reopen.
    {
        var db = try Database.open(allocator, path, .{ .config = legacy_config });
        defer db.close();
        try Fingerprint.expectEqual(after_writes, try fingerprintUpTo(db, last_event, last_node));
        var account = try expectPagesSound(db);
        account.deinit();
    }
}

test "upgrade id keys: an empty database, one with only nodes, and one without full-text or vectors" {
    const path = "/tmp/lupk_small.ltdb";
    const configs = [_]DatabaseConfig{
        legacy_config,
        .{ .enable_fts = false, .new_database_id_keys = .little },
        .{ .enable_fts = false, .enable_vector = false, .new_database_id_keys = .little },
    };
    for (configs, 0..) |config, case| {
        cleanup(path);
        defer cleanup(path);
        var expected_nodes: usize = 0;
        var expected_edges: usize = 0;
        {
            var db = try Database.open(allocator, path, .{ .create = true, .config = config });
            defer db.close();
            if (case >= 1) {
                var txn = try db.beginTransaction(.read_write);
                for (0..400) |i| {
                    const id = try db.createNode(&txn, &.{"N"});
                    try db.setNodeProperty(&txn, id, "i", .{ .int_val = @intCast(i) });
                }
                try db.commitTransaction(&txn);
                expected_nodes = 400;
            }
            if (case == 2) {
                var txn = try db.beginTransaction(.read_write);
                for (0..300) |i| _ = try db.createEdgeAndGetId(&txn, @intCast(i % 400 + 1), @intCast((i * 13) % 400 + 1), "E");
                try db.commitTransaction(&txn);
                expected_edges = 300;
            }
        }
        const stats = try upgradeInPlace(path, config);
        try std.testing.expect(!stats.already_ordered);
        try std.testing.expectEqual(@as(u64, expected_nodes), stats.tree(.node).entries);
        try expectFormat5(path);

        var db = try Database.open(allocator, path, .{ .config = config });
        defer db.close();
        try std.testing.expectEqual(IdKeyOrder.big, db.id_key_order);
        const ids = try db.getAllNodeIdsInTxn(null);
        defer allocator.free(ids);
        try std.testing.expectEqual(expected_nodes, ids.len);
        const node_ids = try nodeTreeIds(db);
        defer allocator.free(node_ids);
        try expectAscending(node_ids);
        const edges = try db.scanEdgesInTxn(null, null, 0);
        defer db.freeEdgeInfos(edges);
        try std.testing.expectEqual(expected_edges, edges.len);
        if (expected_nodes > 0) {
            var v = (try db.getNodeProperty(399, "i")).?;
            defer v.deinit(allocator);
            try std.testing.expectEqual(@as(i64, 398), v.int_val);
        }
        try std.testing.expectEqual(@as(NodeId, expected_nodes + 1), try db.createNode(null, &.{"N"}));
        var account = try expectPagesSound(db);
        account.deinit();
    }
}

test "upgrade id keys: refused for a format-5 file (nothing to do), with a transaction open, in memory, read-only, and onto an existing file" {
    const path = "/tmp/lupk_refuse.ltdb";
    const copy = "/tmp/lupk_refuse.ltdb.copy";
    cleanup(path);
    defer cleanup(path);

    // Already big-endian: nothing happens and no copy is written.
    {
        var db = try Database.open(allocator, path, .{ .create = true, .config = .{ .enable_fts = false } });
        defer db.close();
        _ = try db.createNode(null, &.{"N"});
        const stats = try db.upgradeIdKeysTo(copy, null);
        try std.testing.expect(stats.already_ordered);
        try std.testing.expect(!exists(copy));
    }
    cleanup(path);

    {
        var db = try Database.open(allocator, path, .{ .create = true, .config = .{ .enable_fts = false, .new_database_id_keys = .little } });
        defer db.close();
        _ = try db.createNode(null, &.{"N"});

        var txn = try db.beginTransaction(.read_write);
        try std.testing.expectError(error.TransactionConflict, db.upgradeIdKeysTo(copy, null));
        try db.abortTransaction(&txn);
        try std.testing.expect(!exists(copy));

        // Something already at the destination, or beside it.
        {
            const file = try compat.fs.cwd().createFile(copy ++ "-wal", .{ .truncate = true });
            file.close();
        }
        var diag = upgrade.Diagnostic{};
        try std.testing.expectError(error.AlreadyExists, db.upgradeIdKeysTo(copy, &diag));
        try std.testing.expect(std.mem.indexOf(u8, diag.message(), "-wal") != null);
        try std.testing.expect(exists(copy ++ "-wal"));
        try std.testing.expect(!exists(copy));
        compat.fs.cwd().deleteFile(copy ++ "-wal") catch {};

        // A copy into a directory that does not exist fails at the copy.
        try std.testing.expectError(error.IoError, db.upgradeIdKeysTo("/tmp/lupk_no_such_dir/copy.ltdb", null));
    }
    {
        var db = try Database.open(allocator, path, .{ .read_only = true, .config = .{ .enable_fts = false } });
        defer db.close();
        try std.testing.expectError(error.ReadOnly, db.upgradeIdKeysTo(copy, null));
    }
    {
        var db = try Database.open(allocator, ":memory:", .{ .config = .{ .enable_fts = false, .new_database_id_keys = .little } });
        defer db.close();
        try std.testing.expectError(error.InMemoryDatabase, db.upgradeIdKeysTo(copy, null));
    }
    try std.testing.expect(!exists(copy));
    try std.testing.expectEqual(IdKeyOrder.little, (try readHeader(path)).idKeyOrder());
}

test "upgrade id keys: a failure at any stage leaves the source file byte for byte as it was, and no files behind" {
    const path = "/tmp/lupk_fault.ltdb";
    const copy = "/tmp/lupk_fault.ltdb.copy";
    cleanup(path);
    defer cleanup(path);
    const built = try buildLegacy(path, 0);

    // A plain open and close settles anything an open writes, so what
    // follows is compared with a file that a further open leaves alone
    // (but for the header's modification time, which `hashMain` skips).
    for (0..2) |_| {
        var db = try Database.open(allocator, path, .{ .config = legacy_config });
        db.close();
    }
    const settled = try hashMain(path);
    var expected: Fingerprint = undefined;
    {
        var db = try Database.open(allocator, path, .{ .config = legacy_config });
        defer db.close();
        expected = try fingerprint(db, built.last_event);
    }
    try std.testing.expectEqualSlices(u8, &settled, &(try hashMain(path)));

    const faults = [_]upgrade.Fault{ .after_copy, .mid_conversion, .swap_node_values, .scribble_untouched_page };
    for (faults) |fault| {
        {
            var db = try Database.open(allocator, path, .{ .config = legacy_config });
            defer db.close();
            db.upgrade_fault_for_testing = fault;
            var diag = upgrade.Diagnostic{};
            const result = db.upgradeIdKeysTo(copy, &diag);
            if (fault == .swap_node_values) {
                // The check after the conversion catches the swapped records,
                // in the node tree, before the stores are even asked.
                try std.testing.expectError(error.VerificationFailed, result);
                try std.testing.expectEqualStrings("an entry of the node tree has a different value in the copy", diag.message());
            } else if (fault == .scribble_untouched_page) {
                // One changed byte, checksum intact, on a page none of the
                // five trees owns: only the page comparison sees it.
                try std.testing.expectError(error.VerificationFailed, result);
                if (std.mem.indexOf(u8, diag.message(), "differs from the original, and the upgrade does not change it") == null) {
                    std.debug.print("diagnostic: {s}\n", .{diag.message()});
                    return error.TestUnexpectedResult;
                }
            } else {
                try std.testing.expectError(error.IoError, result);
            }
            for ([_][]const u8{ "", ".partial", "-wal", "-journal" }) |suffix| {
                var buf: [256]u8 = undefined;
                try std.testing.expect(!exists(try std.fmt.bufPrint(&buf, "{s}{s}", .{ copy, suffix })));
            }
            try std.testing.expectEqual(IdKeyOrder.little, db.id_key_order);
        }
        // The copy is taken with `backup`, which checkpoints first and so
        // appends a checkpoint record to the source's log. The database file
        // itself is unchanged, holds the same data, and stays unchanged when
        // it is opened again, so the record carried no work.
        try std.testing.expectEqualSlices(u8, &settled, &(try hashMain(path)));
        {
            var db = try Database.open(allocator, path, .{ .config = legacy_config });
            defer db.close();
            try Fingerprint.expectEqual(expected, try fingerprint(db, built.last_event));
        }
        try std.testing.expectEqualSlices(u8, &settled, &(try hashMain(path)));
    }
}

test "upgrade id keys: the swap leaves a whole database at the path after a stop at any step" {
    const path = "/tmp/lupk_swap.ltdb";
    const steps = [_]?upgrade.SwapStep{ .checked, .sidecars_removed, .renamed };
    for (steps) |step| {
        cleanup(path);
        defer cleanup(path);
        const built = try buildLegacy(path, 0);
        var expected: Fingerprint = undefined;
        {
            var db = try Database.open(allocator, path, .{ .config = legacy_config });
            defer db.close();
            expected = try fingerprint(db, built.last_event);
        }

        var copy_buf: [256]u8 = undefined;
        const copy = try std.fmt.bufPrint(&copy_buf, "{s}.upgrade-id-keys", .{path});
        const held = try upgradeCopyHeld(path, copy);
        try std.testing.expectError(error.StoppedForTesting, upgrade.replaceWithUpgraded(allocator, path, copy, held, step));

        // Whichever file is at the path, it opens whole.
        const renamed = step.? == .renamed;
        try std.testing.expectEqual(!renamed, exists(copy));
        {
            var db = try Database.open(allocator, path, .{ .config = legacy_config });
            defer db.close();
            try std.testing.expectEqual(if (renamed) IdKeyOrder.big else IdKeyOrder.little, db.id_key_order);
            try Fingerprint.expectEqual(expected, try fingerprint(db, built.last_event));
        }

        // Running the upgrade again finishes it, whatever was left.
        upgrade.removeCopy(allocator, copy);
        const stats = try upgradeInPlace(path, legacy_config);
        try std.testing.expectEqual(renamed, stats.already_ordered);
        try expectFormat5(path);
        {
            var db = try Database.open(allocator, path, .{ .config = legacy_config });
            defer db.close();
            try Fingerprint.expectEqual(expected, try fingerprint(db, built.last_event));
        }
    }
}

test "upgrade id keys: the database stays locked from the upgrade's open to the rename, and a log with work in it stops the swap" {
    const path = "/tmp/lupk_swap_lock.ltdb";
    cleanup(path);
    defer cleanup(path);
    const built = try buildLegacy(path, 0);
    var copy_buf: [256]u8 = undefined;
    const copy = try std.fmt.bufPrint(&copy_buf, "{s}.upgrade-id-keys", .{path});

    // Closed, but held: no other handle gets in, to write or to read.
    {
        const held = try upgradeCopyHeld(path, copy);
        var released = false;
        defer if (!released) {
            held.unlock();
            held.close();
        };
        try std.testing.expectError(error.DatabaseLocked, Database.open(allocator, path, .{ .config = legacy_config }));
        try std.testing.expectError(error.DatabaseLocked, Database.open(allocator, path, .{ .read_only = true, .config = legacy_config }));

        // A log with frames in it, as a close that could not fold everything
        // in would leave. The swap refuses and lets go of the file.
        {
            var buf: [256]u8 = undefined;
            const wal = try std.fmt.bufPrint(&buf, "{s}-wal", .{path});
            const file = try compat.fs.cwd().createFile(wal, .{ .truncate = false });
            defer file.close();
            const size = (try file.stat()).size;
            const extra = [_]u8{1} ** 8192;
            try file.pwriteAll(&extra, @max(size, lattice.storage.wal.WAL_HEADER_SIZE));
        }
        released = true;
        try std.testing.expectError(error.LogNotEmpty, upgrade.replaceWithUpgraded(allocator, path, copy, held, null));
    }
    try std.testing.expect(exists(copy));
    try std.testing.expectEqual(IdKeyOrder.little, (try readHeader(path)).idKeyOrder());

    // Released: the original opens again. (The scribbled log is not a log
    // this file wrote, so it goes first.)
    {
        var buf: [256]u8 = undefined;
        compat.fs.cwd().deleteFile(try std.fmt.bufPrint(&buf, "{s}-wal", .{path})) catch {};
    }
    upgrade.removeCopy(allocator, copy);
    var expected: Fingerprint = undefined;
    {
        var db = try Database.open(allocator, path, .{ .config = legacy_config });
        defer db.close();
        expected = try fingerprint(db, built.last_event);
    }

    // And a swap that goes through holds the lock to the end: the held
    // handle is the one the rename replaces.
    {
        const held = try upgradeCopyHeld(path, copy);
        try std.testing.expectError(error.DatabaseLocked, Database.open(allocator, path, .{ .config = legacy_config }));
        try upgrade.replaceWithUpgraded(allocator, path, copy, held, null);
    }
    try expectFormat5(path);
    var db = try Database.open(allocator, path, .{ .config = legacy_config });
    defer db.close();
    try Fingerprint.expectEqual(expected, try fingerprint(db, built.last_event));
}

test "upgrade id keys: a replication directory of the old file refuses the upgraded one and still restores, and a new directory works" {
    const path = "/tmp/lupk_repl.ltdb";
    const old_dir = "/tmp/lupk_repl_old";
    const new_dir = "/tmp/lupk_repl_new";
    const restored = "/tmp/lupk_repl_restored.ltdb";
    cleanup(path);
    cleanup(restored);
    compat.fs.cwd().deleteTree(old_dir) catch {};
    compat.fs.cwd().deleteTree(new_dir) catch {};
    defer {
        cleanup(path);
        cleanup(restored);
        compat.fs.cwd().deleteTree(old_dir) catch {};
        compat.fs.cwd().deleteTree(new_dir) catch {};
    }

    const built = try buildLegacy(path, 0);
    var expected: Fingerprint = undefined;
    {
        var db = try Database.open(allocator, path, .{ .config = legacy_config });
        defer db.close();
        expected = try fingerprint(db, built.last_event);
        _ = try db.replicateTo(old_dir);
    }
    const manifest_before = try hashFiles(old_dir ++ "/manifest.json", &.{""});

    _ = try upgradeInPlace(path, legacy_config);

    {
        var db = try Database.open(allocator, path, .{ .config = legacy_config });
        defer db.close();
        try std.testing.expectError(error.UuidMismatch, db.replicateTo(old_dir));
        _ = try db.replicateTo(new_dir);
    }
    try std.testing.expectEqualSlices(u8, &manifest_before, &(try hashFiles(old_dir ++ "/manifest.json", &.{""})));

    // The old directory still restores the database as it was before.
    _ = try lattice.storage.restore.restore(allocator, old_dir, restored, .{});
    {
        var db = try Database.open(allocator, restored, .{ .config = legacy_config });
        defer db.close();
        try std.testing.expectEqual(IdKeyOrder.little, db.id_key_order);
        try Fingerprint.expectEqual(expected, try fingerprint(db, built.last_event));
    }
    cleanup(restored);

    // The new one restores the upgraded database.
    _ = try lattice.storage.restore.restore(allocator, new_dir, restored, .{});
    {
        var db = try Database.open(allocator, restored, .{ .config = legacy_config });
        defer db.close();
        try std.testing.expectEqual(IdKeyOrder.big, db.id_key_order);
        try Fingerprint.expectEqual(expected, try fingerprint(db, built.last_event));
    }
}
