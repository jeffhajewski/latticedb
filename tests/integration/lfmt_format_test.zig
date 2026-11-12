//! The node id counter and the vector index configuration kept in the file
//! header, files from before they were kept there, and the vector metrics.

const std = @import("std");
const lattice = @import("lattice");

const Database = lattice.storage.database.Database;
const DatabaseError = lattice.storage.database.DatabaseError;
const DatabaseConfig = lattice.storage.database.DatabaseConfig;
const VectorMetric = lattice.storage.database.VectorMetric;
const FileHeader = lattice.storage.page.FileHeader;
const NodeId = lattice.core.types.NodeId;
const PropertyValue = lattice.core.types.PropertyValue;
const types = lattice.core.types;

fn cleanup(path: []const u8) void {
    const cwd = @import("compat").fs.cwd();
    var buf: [256]u8 = undefined;
    cwd.deleteFile(path) catch {};
    cwd.deleteFile(std.fmt.bufPrint(&buf, "{s}-wal", .{path}) catch return) catch {};
    cwd.deleteFile(std.fmt.bufPrint(&buf, "{s}-journal", .{path}) catch return) catch {};
}

fn readHeader(path: []const u8) !FileHeader {
    const file = try @import("compat").fs.cwd().openFile(path, .{});
    defer file.close();
    var buf: [4096]u8 = undefined;
    _ = try file.preadAll(&buf, 0);
    var header: FileHeader = undefined;
    @memcpy(std.mem.asBytes(&header), buf[0..@sizeOf(FileHeader)]);
    return header;
}

/// Leave the header page as 0.16.0 writes it: format 3, and nothing past the
/// end of its own header struct. This is a file 0.16.0 created, or one it has
/// written to since this release did.
fn writeHeaderAs016(path: []const u8) !void {
    const file = try @import("compat").fs.cwd().openFile(path, .{ .mode = .read_write });
    defer file.close();
    var buf: [4096]u8 = undefined;
    _ = try file.preadAll(&buf, 0);
    std.mem.writeInt(u16, buf[4..6], 3, .little); // format_version
    std.mem.writeInt(u16, buf[6..8], 3, .little); // min_reader_version
    @memset(buf[FileHeader.LEGACY_SIZE..], 0);
    try file.pwriteAll(&buf, 0);
}

/// Change one field of the header in place.
fn patchHeader(path: []const u8, comptime field: []const u8, value: anytype) !void {
    const file = try @import("compat").fs.cwd().openFile(path, .{ .mode = .read_write });
    defer file.close();
    var buf: [4096]u8 = undefined;
    _ = try file.preadAll(&buf, 0);
    var header: FileHeader = undefined;
    @memcpy(std.mem.asBytes(&header), buf[0..@sizeOf(FileHeader)]);
    @field(header, field) = value;
    @memcpy(buf[0..@sizeOf(FileHeader)], std.mem.asBytes(&header));
    try file.pwriteAll(&buf, 0);
}

const plain_config = DatabaseConfig{ .enable_fts = false };

// ============================================================================
// Node id counter
// ============================================================================

test "lfmt: a deleted node's id is not handed out again, across reopen" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lfmt_node_id_reuse.ltdb";
    cleanup(path);
    defer cleanup(path);

    var highest: NodeId = undefined;
    {
        var db = try Database.open(allocator, path, .{ .create = true, .config = plain_config });
        defer db.close();
        var txn = try db.beginTransaction(.read_write);
        _ = try db.createNode(&txn, &.{"A"});
        highest = try db.createNode(&txn, &.{"A"});
        try db.commitTransaction(&txn);
        txn = try db.beginTransaction(.read_write);
        try db.deleteNode(&txn, highest);
        try db.commitTransaction(&txn);
    }
    try std.testing.expectEqual(@as(?u64, highest + 1), (try readHeader(path)).storedNextNodeId());

    var db = try Database.open(allocator, path, .{ .config = plain_config });
    defer db.close();
    var txn = try db.beginTransaction(.read_write);
    const next = try db.createNode(&txn, &.{"A"});
    try db.commitTransaction(&txn);
    // 0.16.0 recomputed the counter from the highest id left and gave the
    // deleted node's id to this one.
    try std.testing.expectEqual(highest + 1, next);
}

test "lfmt: opening takes the node id counter from the header without scanning" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lfmt_node_id_header.ltdb";
    cleanup(path);
    defer cleanup(path);

    {
        var db = try Database.open(allocator, path, .{ .create = true, .config = plain_config });
        defer db.close();
        _ = try db.createNode(null, &.{"A"});
    }
    // A counter only the header can have produced: no node is anywhere near it.
    try patchHeader(path, "next_node_id", @as(u64, 1000));

    var db = try Database.open(allocator, path, .{ .config = plain_config });
    defer db.close();
    try std.testing.expectEqual(@as(NodeId, 1000), try db.createNode(null, &.{"A"}));
}

test "lfmt: a 0.16.0 file gets its node id counter by one scan, then keeps it" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lfmt_node_id_legacy.ltdb";
    cleanup(path);
    defer cleanup(path);

    {
        var db = try Database.open(allocator, path, .{ .create = true, .config = plain_config });
        defer db.close();
        for (0..5) |_| _ = try db.createNode(null, &.{"A"});
    }
    try writeHeaderAs016(path);
    const legacy = try readHeader(path);
    try std.testing.expect(legacy.storedNextNodeId() == null);
    try std.testing.expectEqual(@as(u16, 3), legacy.format_version);

    // Read-only, the file is left as it is.
    {
        var db = try Database.open(allocator, path, .{ .read_only = true, .config = plain_config });
        defer db.close();
        try std.testing.expectEqual(@as(u64, 5), db.nodeCount());
    }
    try std.testing.expect((try readHeader(path)).storedNextNodeId() == null);

    {
        var db = try Database.open(allocator, path, .{ .config = plain_config });
        defer db.close();
        try std.testing.expectEqual(@as(NodeId, 6), try db.createNode(null, &.{"A"}));
    }
    const upgraded = try readHeader(path);
    try std.testing.expectEqual(types.FORMAT_VERSION, upgraded.format_version);
    // Upgrading does not lock 0.16.0 out.
    try std.testing.expectEqual(@as(u16, 3), upgraded.min_reader_version);
    try std.testing.expectEqual(@as(?u64, 7), upgraded.storedNextNodeId());
}

test "lfmt: node ids handed out before a crash are not reused after recovery" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lfmt_node_id_crash.ltdb";
    const killed = "/tmp/lfmt_node_id_crash_killed.ltdb";
    cleanup(path);
    defer cleanup(path);
    cleanup(killed);
    defer cleanup(killed);

    var last: NodeId = undefined;
    {
        var db = try Database.open(allocator, path, .{ .create = true, .config = .{ .enable_fts = false, .auto_checkpoint = null } });
        defer db.close();
        try db.sync();
        var txn = try db.beginTransaction(.read_write);
        for (0..3) |_| last = try db.createNode(&txn, &.{"A"});
        try db.commitTransaction(&txn);
        txn = try db.beginTransaction(.read_write);
        try db.deleteNode(&txn, last);
        try db.commitTransaction(&txn);
        try copyFile(path, killed);
        try copyFile(path ++ "-wal", killed ++ "-wal");
    }

    var db = try Database.open(allocator, killed, .{ .config = plain_config });
    defer db.close();
    try std.testing.expect(!try db.nodeExists(last));
    try std.testing.expectEqual(last + 1, try db.createNode(null, &.{"A"}));
}

test "lfmt: after a crash, ids resume past the last committed creation; only uncommitted ones come back" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lfmt_node_id_uncommitted.ltdb";
    const killed = "/tmp/lfmt_node_id_uncommitted_killed.ltdb";
    cleanup(path);
    defer cleanup(path);
    cleanup(killed);
    defer cleanup(killed);

    var rolled_back: NodeId = undefined;
    var committed: NodeId = undefined;
    var uncommitted: NodeId = undefined;
    {
        var db = try Database.open(allocator, path, .{ .create = true, .config = .{ .enable_fts = false, .auto_checkpoint = null } });
        defer db.close();
        try db.sync();

        var txn = try db.beginTransaction(.read_write);
        rolled_back = try db.createNode(&txn, &.{"A"});
        try db.abortTransaction(&txn);

        txn = try db.beginTransaction(.read_write);
        committed = try db.createNode(&txn, &.{"A"});
        try db.commitTransaction(&txn);
        try std.testing.expect(committed > rolled_back);

        // Killed while this transaction is open: nothing of it is in the
        // log or the file.
        txn = try db.beginTransaction(.read_write);
        uncommitted = try db.createNode(&txn, &.{"A"});
        try copyFile(path, killed);
        try copyFile(path ++ "-wal", killed ++ "-wal");
        try db.abortTransaction(&txn);
    }

    var db = try Database.open(allocator, killed, .{ .config = plain_config });
    defer db.close();
    try std.testing.expect(try db.nodeExists(committed));
    try std.testing.expect(!try db.nodeExists(rolled_back));
    // The rolled-back id lies below a committed creation and stays unused.
    // The id the uncommitted transaction had is the next one handed out: it
    // was never in the database, in the log or in any commit's result.
    try std.testing.expectEqual(uncommitted, try db.createNode(null, &.{"A"}));
    try std.testing.expectEqual(committed + 1, uncommitted);
}

fn copyFile(src: []const u8, dst: []const u8) !void {
    const cwd = @import("compat").fs.cwd();
    const in = try cwd.openFile(src, .{});
    defer in.close();
    const out = try cwd.createFile(dst, .{ .truncate = true });
    defer out.close();
    var buf: [65536]u8 = undefined;
    var offset: u64 = 0;
    while (true) {
        const n = try in.preadAll(&buf, offset);
        if (n == 0) break;
        try out.pwriteAll(buf[0..n], offset);
        offset += n;
    }
}

// ============================================================================
// Writer version gate
// ============================================================================

fn readHeaderPage(path: []const u8) ![4096]u8 {
    const file = try @import("compat").fs.cwd().openFile(path, .{});
    defer file.close();
    var buf: [4096]u8 = undefined;
    _ = try file.preadAll(&buf, 0);
    return buf;
}

test "lfmt: a file that needs a newer writer opens read-only and refuses writable opens" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lfmt_min_writer.ltdb";
    cleanup(path);
    defer cleanup(path);

    {
        var db = try Database.open(allocator, path, .{ .create = true, .config = plain_config });
        defer db.close();
        _ = try db.createNode(null, &.{"A"});
    }
    const created = try readHeader(path);
    try std.testing.expectEqual(types.FORMAT_VERSION, created.min_writer_version);
    try std.testing.expectEqual(types.MIN_READER_VERSION, created.min_reader_version);

    // What a later release writes when the file holds something this one
    // would not keep correct: readable here, not writable.
    try patchHeader(path, "min_writer_version", types.FORMAT_VERSION + 1);
    const before = try readHeaderPage(path);

    {
        var db = try Database.open(allocator, path, .{ .read_only = true, .config = plain_config });
        defer db.close();
        try std.testing.expectEqual(@as(u64, 1), db.nodeCount());
    }
    try std.testing.expectError(DatabaseError.NeedsNewerWriter, Database.open(allocator, path, .{ .config = plain_config }));
    try std.testing.expectError(DatabaseError.NeedsNewerWriter, Database.open(allocator, path, .{ .config = .{ .enable_fts = false, .enable_vector = true, .vector_dimensions = 4 } }));

    // Neither open changed the header.
    const after = try readHeaderPage(path);
    try std.testing.expectEqualSlices(u8, &before, &after);
}

test "lfmt: no min_writer_version means no restriction, and a write records this release's" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lfmt_min_writer_zero.ltdb";
    cleanup(path);
    defer cleanup(path);

    {
        var db = try Database.open(allocator, path, .{ .create = true, .config = plain_config });
        db.close();
    }
    // Zero with the format-4 fields present, as well as a 0.16.0 header.
    try patchHeader(path, "min_writer_version", @as(u16, 0));
    {
        var db = try Database.open(allocator, path, .{ .config = plain_config });
        _ = try db.createNode(null, &.{"A"});
        db.close();
    }
    try std.testing.expectEqual(types.FORMAT_VERSION, (try readHeader(path)).min_writer_version);

    try writeHeaderAs016(path);
    try std.testing.expectEqual(@as(u16, 0), (try readHeader(path)).min_writer_version);
    {
        var db = try Database.open(allocator, path, .{ .config = plain_config });
        _ = try db.createNode(null, &.{"A"});
        db.close();
    }
    try std.testing.expectEqual(types.FORMAT_VERSION, (try readHeader(path)).min_writer_version);
}

test "lfmt: a file or log in a format too new to read is VersionTooNew, read-only or not" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lfmt_min_reader.ltdb";
    cleanup(path);
    defer cleanup(path);

    {
        var db = try Database.open(allocator, path, .{ .create = true, .config = plain_config });
        defer db.close();
        _ = try db.createNode(null, &.{"A"});
    }
    try patchHeader(path, "min_reader_version", types.FORMAT_VERSION + 1);
    const before = try readHeaderPage(path);
    try std.testing.expectError(DatabaseError.VersionTooNew, Database.open(allocator, path, .{ .config = plain_config }));
    try std.testing.expectError(DatabaseError.VersionTooNew, Database.open(allocator, path, .{ .read_only = true, .config = plain_config }));
    const after = try readHeaderPage(path);
    try std.testing.expectEqualSlices(u8, &before, &after);

    // The log has a version of its own.
    try patchHeader(path, "min_reader_version", types.MIN_READER_VERSION);
    {
        const file = try @import("compat").fs.cwd().openFile(path ++ "-wal", .{ .mode = .read_write });
        defer file.close();
        var bytes: [2]u8 = undefined;
        std.mem.writeInt(u16, &bytes, lattice.storage.wal.WAL_FORMAT_VERSION + 1, .little);
        try file.pwriteAll(&bytes, @offsetOf(lattice.storage.wal.WalHeader, "version"));
    }
    try std.testing.expectError(DatabaseError.VersionTooNew, Database.open(allocator, path, .{ .config = plain_config }));
}

// ============================================================================
// Vector configuration
// ============================================================================

fn unitVector(comptime dims: usize, i: usize) [dims]f32 {
    var v = [_]f32{0.01} ** dims;
    v[i % dims] = 1;
    v[(i * 7 + 3) % dims] += @floatFromInt(i % 5);
    return v;
}

test "lfmt: a database with vectors opens with them without vector options" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lfmt_vector_config.ltdb";
    cleanup(path);
    defer cleanup(path);

    const dims = 8;
    var ids: [20]NodeId = undefined;
    {
        var db = try Database.open(allocator, path, .{ .create = true, .config = .{
            .enable_fts = false,
            .enable_vector = true,
            .vector_dimensions = dims,
            .vector_m = 12,
            .vector_ef_construction = 80,
        } });
        defer db.close();
        for (&ids, 0..) |*id, i| {
            id.* = try db.createNode(null, &.{"Doc"});
            const v = unitVector(dims, i);
            try db.setNodeVector(id.*, &v);
        }
    }
    const header = try readHeader(path);
    const stored = header.storedVectorConfig().?;
    try std.testing.expectEqual(@as(u16, dims), stored.dimensions);
    try std.testing.expectEqual(VectorMetric.cosine, stored.metric);
    try std.testing.expectEqual(@as(u16, 12), stored.m);
    try std.testing.expectEqual(@as(u16, 24), stored.m_max0);
    try std.testing.expectEqual(@as(u16, 80), stored.ef_construction);
    // m = 12 is not what 0.16.0 assumes, so 0.16.0 must refuse the file.
    try std.testing.expectEqual(types.FORMAT_VERSION, header.min_reader_version);

    // No vector options at all, read-write and read-only.
    inline for (.{ false, true }) |read_only| {
        var db = try Database.open(allocator, path, .{ .read_only = read_only, .config = plain_config });
        defer db.close();
        const config = db.vectorConfig().?;
        try std.testing.expectEqual(@as(u16, dims), config.dimensions);
        try std.testing.expectEqual(@as(u16, 12), config.m);
        const q = unitVector(dims, 4);
        const results = try db.vectorSearch(&q, 3, null);
        defer db.freeVectorSearchResults(results);
        try std.testing.expectEqual(ids[4], results[0].node_id);
    }

    // Options that agree with the file are fine, including dimensions 0 for
    // "the file's".
    {
        var db = try Database.open(allocator, path, .{ .config = .{ .enable_fts = false, .enable_vector = true, .vector_dimensions = dims, .vector_m = 12 } });
        db.close();
        db = try Database.open(allocator, path, .{ .config = .{ .enable_fts = false, .enable_vector = true, .vector_dimensions = 0 } });
        db.close();
    }

    // Options that contradict it are refused, and nothing is misread.
    try std.testing.expectError(DatabaseError.VectorConfigMismatch, Database.open(allocator, path, .{ .config = .{ .enable_fts = false, .enable_vector = true, .vector_dimensions = 16 } }));
    try std.testing.expectError(DatabaseError.VectorConfigMismatch, Database.open(allocator, path, .{ .config = .{ .enable_fts = false, .vector_metric = .euclidean } }));
    try std.testing.expectError(DatabaseError.VectorConfigMismatch, Database.open(allocator, path, .{ .config = .{ .enable_fts = false, .vector_m = 16 } }));
    try std.testing.expectError(DatabaseError.VectorConfigMismatch, Database.open(allocator, path, .{ .read_only = true, .config = .{ .enable_fts = false, .vector_ef_construction = 200 } }));
    // Values out of range are invalid whatever the file holds.
    try std.testing.expectError(DatabaseError.InvalidArgument, Database.open(allocator, path, .{ .config = .{ .enable_fts = false, .vector_m = 1 } }));
    try std.testing.expectError(DatabaseError.InvalidArgument, Database.open(allocator, path, .{ .config = .{ .enable_fts = false, .vector_ef_construction = 5000 } }));
}

test "lfmt: vector dimensions 0 cannot create an index" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lfmt_vector_dims_zero.ltdb";
    cleanup(path);
    defer cleanup(path);
    {
        var db = try Database.open(allocator, path, .{ .create = true, .config = plain_config });
        db.close();
    }
    try std.testing.expectError(DatabaseError.InvalidArgument, Database.open(allocator, path, .{ .config = .{ .enable_fts = false, .enable_vector = true, .vector_dimensions = 0 } }));
    var db = try Database.open(allocator, path, .{ .config = plain_config });
    defer db.close();
    try std.testing.expect(db.vectorConfig() == null);
}

fn exists(path: []const u8) bool {
    @import("compat").fs.cwd().access(path, .{}) catch return false;
    return true;
}

test "lfmt: options a new database cannot be created with are refused before any file exists" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lfmt_refused_create.ltdb";
    cleanup(path);
    defer cleanup(path);

    const refused = [_]struct { options: lattice.storage.database.OpenOptions, err: DatabaseError }{
        // Vectors on with "the file's dimensions", and no file to ask.
        .{ .options = .{ .create = true, .config = .{ .enable_fts = false, .enable_vector = true, .vector_dimensions = 0 } }, .err = DatabaseError.InvalidArgument },
        // Out-of-range values.
        .{ .options = .{ .create = true, .config = .{ .enable_fts = false, .enable_vector = true, .vector_dimensions = 5000 } }, .err = DatabaseError.InvalidArgument },
        .{ .options = .{ .create = true, .config = .{ .enable_fts = false, .enable_vector = true, .vector_dimensions = 4, .vector_m = 64 } }, .err = DatabaseError.InvalidArgument },
        .{ .options = .{ .create = true, .page_size = 1000, .config = plain_config }, .err = DatabaseError.InvalidArgument },
        // Nothing to read, and a read-only handle may not create it.
        .{ .options = .{ .create = true, .read_only = true, .config = plain_config }, .err = DatabaseError.FileNotFound },
    };
    for (refused) |case| {
        try std.testing.expectError(case.err, Database.open(allocator, path, case.options));
        try std.testing.expect(!exists(path));
        try std.testing.expect(!exists(path ++ "-wal"));
        try std.testing.expect(!exists(path ++ "-journal"));
    }
    var db = try Database.open(allocator, path, .{ .create = true, .config = plain_config });
    db.close();
}

test "lfmt: a 0.16.0 file with vectors opens with them, and records its configuration when written" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lfmt_vector_legacy.ltdb";
    cleanup(path);
    defer cleanup(path);

    const dims = 6;
    var ids: [10]NodeId = undefined;
    {
        var db = try Database.open(allocator, path, .{ .create = true, .config = .{ .enable_fts = false, .enable_vector = true, .vector_dimensions = dims } });
        defer db.close();
        for (&ids, 0..) |*id, i| {
            id.* = try db.createNode(null, &.{"Doc"});
            const v = unitVector(dims, i);
            try db.setNodeVector(id.*, &v);
        }
    }
    try writeHeaderAs016(path);
    try std.testing.expect((try readHeader(path)).storedVectorConfig() == null);

    // 0.16.0 recorded only the dimensions, in each vector page, and nothing
    // else could vary; that is enough to open the index.
    {
        var db = try Database.open(allocator, path, .{ .read_only = true, .config = plain_config });
        defer db.close();
        try std.testing.expectEqual(@as(u16, dims), db.vectorConfig().?.dimensions);
        const q = unitVector(dims, 2);
        const results = try db.vectorSearch(&q, 1, null);
        defer db.freeVectorSearchResults(results);
        try std.testing.expectEqual(ids[2], results[0].node_id);
    }
    try std.testing.expect((try readHeader(path)).storedVectorConfig() == null);

    // Wrong dimensions are refused before the pages are read.
    try std.testing.expectError(DatabaseError.VectorConfigMismatch, Database.open(allocator, path, .{ .config = .{ .enable_fts = false, .enable_vector = true, .vector_dimensions = dims + 1 } }));

    {
        var db = try Database.open(allocator, path, .{ .config = plain_config });
        db.close();
    }
    const header = try readHeader(path);
    const stored = header.storedVectorConfig().?;
    try std.testing.expectEqual(FileHeader.VectorConfig.legacy(dims), stored);
    // A cosine index with m = 16 is one 0.16.0 reads correctly.
    try std.testing.expectEqual(@as(u16, 3), header.min_reader_version);
}

// ============================================================================
// Metrics
// ============================================================================

fn checkMetric(comptime metric: VectorMetric, path: []const u8) !void {
    const allocator = std.testing.allocator;
    cleanup(path);
    defer cleanup(path);

    const dims = 16;
    const count = 600;
    var prng = std.Random.DefaultPrng.init(0x1f3a);
    const random = prng.random();
    const vectors = try allocator.alloc([dims]f32, count);
    defer allocator.free(vectors);
    for (vectors) |*v| {
        // Lengths that vary, so cosine, L2 and dot product rank differently.
        const scale = 0.2 + 3 * random.float(f32);
        for (v) |*x| x.* = (random.float(f32) * 2 - 1) * scale;
    }
    const ids = try allocator.alloc(NodeId, count);
    defer allocator.free(ids);

    {
        var db = try Database.open(allocator, path, .{ .create = true, .config = .{
            .enable_fts = false,
            .enable_vector = true,
            .vector_dimensions = dims,
            .vector_metric = metric,
        } });
        defer db.close();
        var txn = try db.beginTransaction(.read_write);
        for (ids, vectors) |*id, *v| {
            id.* = try db.createNode(&txn, &.{"V"});
            try db.setNodeVectorInTxn(&txn, id.*, v);
        }
        try db.commitTransaction(&txn);
    }

    // Reopened without options: the metric is the file's.
    var db = try Database.open(allocator, path, .{ .config = plain_config });
    defer db.close();
    try std.testing.expectEqual(metric, db.vectorConfig().?.metric);

    const k = 10;
    var hits: usize = 0;
    var total: usize = 0;
    const exact = try allocator.alloc(struct { id: NodeId, d: f32 }, count);
    defer allocator.free(exact);
    for (0..20) |_| {
        var q: [dims]f32 = undefined;
        for (&q) |*x| x.* = random.float(f32) * 2 - 1;

        for (exact, ids, vectors) |*e, id, *v| e.* = .{ .id = id, .d = db.vectorDistance(&q, v) };
        std.mem.sort(@TypeOf(exact[0]), exact, {}, struct {
            fn lt(_: void, a: @TypeOf(exact[0]), b: @TypeOf(exact[0])) bool {
                return a.d < b.d;
            }
        }.lt);

        const results = try db.vectorSearch(&q, k, 100);
        defer db.freeVectorSearchResults(results);
        try std.testing.expectEqual(@as(usize, k), results.len);
        for (results, 0..) |r, i| {
            // Ordered nearest first, on the scale vectorDistance reports.
            if (i > 0) try std.testing.expect(results[i - 1].distance <= r.distance);
            const idx = std.mem.indexOfScalar(NodeId, ids, r.node_id).?;
            try std.testing.expectApproxEqAbs(db.vectorDistance(&q, &vectors[idx]), r.distance, 1e-3);
            for (exact[0..k]) |e| {
                if (e.id == r.node_id) {
                    hits += 1;
                    break;
                }
            }
        }
        total += k;
    }
    // HNSW is approximate; at this size it finds nearly all true neighbours.
    try std.testing.expect(hits * 100 >= total * 90);

    // Cypher's <=> measures with the same metric.
    var params = std.StringHashMap(PropertyValue).init(allocator);
    defer params.deinit();
    const q0 = vectors[7];
    try params.put("q", .{ .vector_val = &q0 });
    var result = try db.queryWithParams("MATCH (n:V) WHERE n.embedding <=> $q RETURN id(n) LIMIT 1", &params);
    defer result.deinit();
    // The nearest under the metric: for cosine and L2 the vector itself, for
    // dot product whichever vector has the largest dot product with it.
    var best: usize = 0;
    for (vectors, 0..) |*v, i| {
        if (db.vectorDistance(&q0, v) < db.vectorDistance(&q0, &vectors[best])) best = i;
    }
    if (metric != .dot_product) try std.testing.expectEqual(@as(usize, 7), best);
    try std.testing.expectEqual(@as(usize, 1), result.rows.len);
    try std.testing.expectEqual(@as(i64, @intCast(ids[best])), result.rows[0].values[0].int_val);

    var lit = try db.query("RETURN [1.0, 0.0] <=> [0.0, 2.0] AS d");
    defer lit.deinit();
    const expected: f64 = switch (metric) {
        .cosine => 1.0,
        .euclidean => @sqrt(5.0),
        .dot_product => 0.0,
        else => unreachable,
    };
    try std.testing.expectApproxEqAbs(expected, lit.rows[0].values[0].float_val, 1e-5);

    // A vector pending in a transaction is scored with the metric too.
    var txn = try db.beginTransaction(.read_write);
    const pending = try db.createNode(&txn, &.{"V"});
    var far = [_]f32{50} ** dims;
    try db.setNodeVectorInTxn(&txn, pending, &far);
    const in_txn = try db.vectorSearchInTxn(&txn, &far, 1, null);
    defer db.freeVectorSearchResults(in_txn);
    try std.testing.expectEqual(pending, in_txn[0].node_id);
    try std.testing.expectApproxEqAbs(db.vectorDistance(&far, &far), in_txn[0].distance, 1e-2);
    try db.abortTransaction(&txn);
}

test "lfmt: cosine search ranks by cosine distance" {
    try checkMetric(.cosine, "/tmp/lfmt_metric_cosine.ltdb");
}

test "lfmt: euclidean search ranks by L2 distance and stores vectors as given" {
    try checkMetric(.euclidean, "/tmp/lfmt_metric_l2.ltdb");
}

test "lfmt: dot-product search ranks by largest dot product" {
    try checkMetric(.dot_product, "/tmp/lfmt_metric_dot.ltdb");
}

test "lfmt: a euclidean index reads vectors back as written" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lfmt_metric_readback.ltdb";
    cleanup(path);
    defer cleanup(path);
    var db = try Database.open(allocator, path, .{ .create = true, .config = .{ .enable_fts = false, .enable_vector = true, .vector_dimensions = 2, .vector_metric = .euclidean } });
    defer db.close();
    const id = try db.createNode(null, &.{"V"});
    try db.setNodeVector(id, &[_]f32{ 3, 4 });
    const back = (try db.getNodeVectorInTxn(null, id)).?;
    defer allocator.free(back);
    try std.testing.expectEqualSlices(f32, &[_]f32{ 3, 4 }, back);
}
