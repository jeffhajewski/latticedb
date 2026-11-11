//! Crash recovery tests for LatticeDB.
//!
//! These tests simulate crashes by resetting the database file to header-only
//! state (preserving UUID for WAL matching) after a normal close. On reopen,
//! recovery replays committed WAL records into fresh B+Trees.

const std = @import("std");
const lattice = @import("lattice");

const Database = lattice.storage.database.Database;
const DatabaseConfig = lattice.storage.database.DatabaseConfig;
const OpenOptions = lattice.storage.database.OpenOptions;
const FileHeader = lattice.storage.page.FileHeader;
const NodeId = lattice.core.types.NodeId;
const EdgeId = lattice.core.types.EdgeId;
const PropertyValue = lattice.core.types.PropertyValue;
const NULL_PAGE = lattice.core.types.NULL_PAGE;

// ============================================================================
// Helpers
// ============================================================================

/// Simulate a crash by resetting the database file to header-only state.
/// Preserves the file UUID so the WAL still matches on reopen.
/// Zeros tree_roots (so initNewTrees runs), node_count, and edge_count.
/// Truncates the file to a single page (4096 bytes).
/// Leaves the WAL file untouched.
fn simulateCrash(path: []const u8) !void {
    const file = try @import("compat").fs.cwd().openFile(path, .{ .mode = .read_write });
    defer file.close();

    // Read the full header page
    var header_buf: [4096]u8 = undefined;
    const n = try file.preadAll(&header_buf, 0);
    if (n < @sizeOf(FileHeader)) return error.HeaderTooSmall;

    // Stack byte arrays only have byte alignment on some targets. Copy into a
    // naturally aligned value instead of casting the file buffer directly.
    var header: FileHeader = undefined;
    @memcpy(std.mem.asBytes(&header), header_buf[0..@sizeOf(FileHeader)]);

    // Zero tree_roots so hasInitializedTrees() returns false on reopen
    header.tree_roots = [_]u32{NULL_PAGE} ** 16;

    // Zero counts
    header.node_count = 0;
    header.edge_count = 0;

    // Zero page references that point to now-truncated pages
    header.btree_root_page = NULL_PAGE;
    header.freelist_page = NULL_PAGE;
    header.schema_page = NULL_PAGE;
    header.fts_segment_page = NULL_PAGE;
    header.vector_segment_page = NULL_PAGE;

    // Write modified header back
    @memcpy(header_buf[0..@sizeOf(FileHeader)], std.mem.asBytes(&header));
    try file.pwriteAll(&header_buf, 0);

    // Truncate file to just the header page (4096 bytes)
    try file.setLength(4096);
}

/// Leave the files exactly as a killed process would.
///
/// Copies the database and its WAL while the handle is still open, so the copy
/// holds what had been written at that moment and nothing `close()` adds
/// later: no final root save, no flush. Opening the copy is what the next
/// process sees after `kill -9`.
fn copyAsIfKilled(allocator: std.mem.Allocator, src: []const u8, dst: []const u8) !void {
    try copyFile(allocator, src, dst);

    var src_wal_buf: [256]u8 = undefined;
    var dst_wal_buf: [256]u8 = undefined;
    const src_wal = try std.fmt.bufPrint(&src_wal_buf, "{s}-wal", .{src});
    const dst_wal = try std.fmt.bufPrint(&dst_wal_buf, "{s}-wal", .{dst});
    try copyFile(allocator, src_wal, dst_wal);
}

fn copyFile(allocator: std.mem.Allocator, src: []const u8, dst: []const u8) !void {
    const fs = @import("compat").fs;
    const in = try fs.cwd().openFile(src, .{});
    defer in.close();
    const bytes = try in.readToEndAlloc(allocator, 1 << 30);
    defer allocator.free(bytes);

    const out = try fs.cwd().createFile(dst, .{ .truncate = true });
    defer out.close();
    try out.pwriteAll(bytes, 0);
}

/// Clean up database and WAL files.
fn cleanup(path: []const u8) void {
    @import("compat").fs.cwd().deleteFile(path) catch {};
    // Delete WAL file (path + "-wal")
    var wal_buf: [256]u8 = undefined;
    const wal_path = std.fmt.bufPrint(&wal_buf, "{s}-wal", .{path}) catch return;
    @import("compat").fs.cwd().deleteFile(wal_path) catch {};
}

/// Open a database with WAL enabled for crash testing.
fn openCrashTestDb(allocator: std.mem.Allocator, path: []const u8, create: bool) !*Database {
    return Database.open(allocator, path, .{
        .create = create,
        .config = .{
            .enable_wal = true,
            .enable_fts = false,
            .enable_vector = false,
            // These tests wipe the data file after a close and expect the
            // log to rebuild it, so nothing may truncate the log for them.
            .auto_checkpoint = null,
        },
    });
}

// ============================================================================
// Tests
// ============================================================================

test "committed nodes survive a kill after an automatic checkpoint" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_crash_autockpt.ltdb";
    const killed = "/tmp/lattice_crash_autockpt_killed.ltdb";
    cleanup(path);
    defer cleanup(path);
    cleanup(killed);
    defer cleanup(killed);

    // Enough single-node commits to split the node tree's root several times
    // and to cross the checkpoint threshold many times over.
    const config = DatabaseConfig{
        .enable_fts = false,
        .enable_vector = false,
        .auto_checkpoint = .{ .max_wal_frames = 64 },
    };
    const count = 1500;
    const ids = try allocator.alloc(NodeId, count);
    defer allocator.free(ids);

    {
        var db = try Database.open(allocator, path, .{ .create = true, .config = config });
        defer db.close();
        for (ids, 0..) |*id, i| {
            var txn = try db.beginTransaction(.read_write);
            id.* = try db.createNode(&txn, &.{"Person"});
            try db.setNodeProperty(&txn, id.*, "seq", .{ .int_val = @intCast(i) });
            try db.commitTransaction(&txn);
        }
        // The checkpoints really ran: the log holds far fewer frames than
        // 1500 commits wrote.
        try std.testing.expect(db.wal.?.header.frame_count < 200);
        try copyAsIfKilled(allocator, path, killed);
    }

    var db = try Database.open(allocator, killed, .{ .config = config });
    defer db.close();
    for (ids) |id| {
        try std.testing.expect(try db.nodeExists(id));
    }
    const people = try db.getNodesByLabel("Person");
    defer allocator.free(people);
    try std.testing.expectEqual(@as(usize, count), people.len);

    // The id allocator must not hand out an id that is already taken.
    var txn = try db.beginTransaction(.read_write);
    const fresh = try db.createNode(&txn, &.{"Person"});
    try db.commitTransaction(&txn);
    for (ids) |id| try std.testing.expect(id != fresh);
}

test "committed vectors survive a kill after an automatic checkpoint" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_crash_autockpt_vec.ltdb";
    const killed = "/tmp/lattice_crash_autockpt_vec_killed.ltdb";
    cleanup(path);
    defer cleanup(path);
    cleanup(killed);
    defer cleanup(killed);

    const dims = 8;
    const config = DatabaseConfig{
        .enable_fts = false,
        .enable_vector = true,
        .vector_dimensions = dims,
        .auto_checkpoint = .{ .max_wal_frames = 64 },
    };
    const count = 400;
    const ids = try allocator.alloc(NodeId, count);
    defer allocator.free(ids);

    {
        var db = try Database.open(allocator, path, .{ .create = true, .config = config });
        defer db.close();
        for (ids, 0..) |*id, i| {
            var vector = [_]f32{0} ** dims;
            vector[i % dims] = 1;
            vector[(i + 1) % dims] = @floatFromInt(i + 1);
            var txn = try db.beginTransaction(.read_write);
            id.* = try db.createNode(&txn, &.{"Chunk"});
            try db.setNodeVectorInTxn(&txn, id.*, &vector);
            try db.commitTransaction(&txn);
        }
        try std.testing.expect(db.wal.?.header.frame_count < 200);
        try copyAsIfKilled(allocator, path, killed);
    }

    var db = try Database.open(allocator, killed, .{ .config = config });
    defer db.close();
    // Every vector is reachable: a search wide enough to return them all does.
    var probe = [_]f32{1} ** dims;
    const results = try db.vectorSearch(&probe, count, count);
    defer db.freeVectorSearchResults(results);
    try std.testing.expectEqual(@as(usize, count), results.len);
}

test "deleting a node leaves a database without vectors openable" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_crash_delete_novec.ltdb";
    const killed = "/tmp/lattice_crash_delete_novec_killed.ltdb";
    cleanup(path);
    defer cleanup(path);
    cleanup(killed);
    defer cleanup(killed);

    var kept: NodeId = undefined;
    var deleted: NodeId = undefined;
    {
        var db = try openCrashTestDb(allocator, path, true);
        defer db.close();
        var txn = try db.beginTransaction(.read_write);
        kept = try db.createNode(&txn, &.{"Person"});
        deleted = try db.createNode(&txn, &.{"Person"});
        try db.commitTransaction(&txn);
        txn = try db.beginTransaction(.read_write);
        try db.deleteNode(&txn, deleted);
        try db.commitTransaction(&txn);
        try copyAsIfKilled(allocator, path, killed);
    }

    // After a kill.
    {
        var db = try openCrashTestDb(allocator, killed, false);
        defer db.close();
        try std.testing.expect(try db.nodeExists(kept));
        try std.testing.expect(!try db.nodeExists(deleted));
    }
    // After a clean close, which leaves the log in place for the next open.
    {
        var db = try openCrashTestDb(allocator, path, false);
        defer db.close();
        try std.testing.expect(try db.nodeExists(kept));
        try std.testing.expect(!try db.nodeExists(deleted));
    }
}

test "a vector removal logged by 0.16.0 is skipped when the file has no vectors" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_crash_legacy_vector_remove.ltdb";
    const killed = "/tmp/lattice_crash_legacy_vector_remove_killed.ltdb";
    cleanup(path);
    defer cleanup(path);
    cleanup(killed);
    defer cleanup(killed);

    var node_id: NodeId = undefined;
    {
        var db = try openCrashTestDb(allocator, path, true);
        defer db.close();
        var txn = try db.beginTransaction(.read_write);
        node_id = try db.createNode(&txn, &.{"Person"});
        // What 0.16.0 wrote for every node delete, vectors or not.
        const payload = try lattice.transaction.wal_payload.serializeVectorMutationAlloc(allocator, node_id, null);
        defer allocator.free(payload);
        _ = try db.txn_manager.?.logOperation(&txn, .update, payload);
        try db.commitTransaction(&txn);
        try copyAsIfKilled(allocator, path, killed);
    }

    var db = try openCrashTestDb(allocator, killed, false);
    defer db.close();
    try std.testing.expect(try db.nodeExists(node_id));
}

test "a vector removal is still refused when the file has vectors and the open does not" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_crash_vector_remove_refused.ltdb";
    const killed = "/tmp/lattice_crash_vector_remove_refused_killed.ltdb";
    cleanup(path);
    defer cleanup(path);
    cleanup(killed);
    defer cleanup(killed);

    const dims = 8;
    const config = DatabaseConfig{
        .enable_fts = false,
        .enable_vector = true,
        .vector_dimensions = dims,
        .auto_checkpoint = null,
    };
    {
        var db = try Database.open(allocator, path, .{ .create = true, .config = config });
        defer db.close();
        var vector = [_]f32{0} ** dims;
        vector[0] = 1;
        var txn = try db.beginTransaction(.read_write);
        const node_id = try db.createNode(&txn, &.{"Chunk"});
        try db.setNodeVectorInTxn(&txn, node_id, &vector);
        try db.commitTransaction(&txn);
        // Fold the vector into the file and empty the log, so the removal
        // below is the only vector record left in it.
        _ = try db.checkpoint(.truncate);
        txn = try db.beginTransaction(.read_write);
        try db.deleteNode(&txn, node_id);
        try db.commitTransaction(&txn);
        try copyAsIfKilled(allocator, path, killed);
    }

    // Skipping this removal would leave the vector in the index for good.
    try std.testing.expectError(error.IoError, openCrashTestDb(allocator, killed, false));
}

test "a transaction that writes nothing leaves the log untouched" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_crash_readonly_nolog.ltdb";
    cleanup(path);
    defer cleanup(path);

    var db = try openCrashTestDb(allocator, path, true);
    defer db.close();

    var txn = try db.beginTransaction(.read_write);
    const node_id = try db.createNode(&txn, &.{"Person"});
    try db.commitTransaction(&txn);

    const wal = &db.wal.?;
    const lsn_before = wal.getCurrentLsn();
    const frames_before = wal.header.frame_count;

    // Committed and aborted readers, and a writer that changed nothing.
    var reader = try db.beginTransaction(.read_only);
    try std.testing.expect(try db.nodeExistsInTxn(&reader, node_id));
    try db.commitTransaction(&reader);
    reader = try db.beginTransaction(.read_only);
    try db.abortTransaction(&reader);
    var idle_writer = try db.beginTransaction(.read_write);
    try db.commitTransaction(&idle_writer);

    try std.testing.expectEqual(lsn_before, wal.getCurrentLsn());
    try std.testing.expectEqual(frames_before, wal.header.frame_count);

    // A real write still gets its begin record, its data and its commit.
    txn = try db.beginTransaction(.read_write);
    _ = try db.createNode(&txn, &.{"Person"});
    try db.commitTransaction(&txn);
    try std.testing.expect(wal.getCurrentLsn() >= lsn_before + 3);
}

test "the log is empty after a clean close and after recovery" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_crash_log_emptied.ltdb";
    const killed = "/tmp/lattice_crash_log_emptied_killed.ltdb";
    const killed_again = "/tmp/lattice_crash_log_emptied_killed2.ltdb";
    cleanup(path);
    defer cleanup(path);
    cleanup(killed);
    defer cleanup(killed);
    cleanup(killed_again);
    defer cleanup(killed_again);

    // Default checkpoint policy: the threshold is far away, so only open and
    // close can be what empties the log here.
    const config = DatabaseConfig{ .enable_fts = false, .enable_vector = false };
    var node_id: NodeId = undefined;
    {
        var db = try Database.open(allocator, path, .{ .create = true, .config = config });
        defer db.close();
        var txn = try db.beginTransaction(.read_write);
        node_id = try db.createNode(&txn, &.{"Person"});
        try db.commitTransaction(&txn);
        try std.testing.expect(db.wal.?.header.frame_count > 0);
        try copyAsIfKilled(allocator, path, killed);
    }

    // Clean close: nothing left to replay.
    {
        var db = try Database.open(allocator, path, .{ .config = config });
        defer db.close();
        try std.testing.expectEqual(@as(u64, 0), db.wal.?.header.frame_count);
        try std.testing.expect(try db.nodeExists(node_id));
    }

    // Kill: recovery replays the log, then folds it into the file. A second
    // kill straight after that open must find the node in the file itself.
    {
        var db = try Database.open(allocator, killed, .{ .config = config });
        defer db.close();
        try std.testing.expectEqual(@as(u64, 0), db.wal.?.header.frame_count);
        try std.testing.expect(try db.nodeExists(node_id));
        try copyAsIfKilled(allocator, killed, killed_again);
    }
    {
        var db = try Database.open(allocator, killed_again, .{ .config = config });
        defer db.close();
        try std.testing.expect(try db.nodeExists(node_id));
    }
}

test "transaction ids continue above those already in the log" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_crash_txn_ids.ltdb";
    const killed = "/tmp/lattice_crash_txn_ids_killed.ltdb";
    cleanup(path);
    defer cleanup(path);
    cleanup(killed);
    defer cleanup(killed);

    var last_id: u64 = 0;
    {
        var db = try openCrashTestDb(allocator, path, true);
        defer db.close();
        for (0..3) |_| {
            var txn = try db.beginTransaction(.read_write);
            _ = try db.createNode(&txn, &.{"Person"});
            last_id = txn.id;
            try db.commitTransaction(&txn);
        }
        try copyAsIfKilled(allocator, path, killed);
    }

    // The helper keeps the log in place, so this session appends to a log
    // that still holds the first session's transactions.
    var db = try openCrashTestDb(allocator, killed, false);
    defer db.close();
    var txn = try db.beginTransaction(.read_write);
    defer db.abortTransaction(&txn) catch {};
    try std.testing.expect(txn.id > last_id);
}

test "an open reader does not hold back automatic checkpoints" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_crash_reader_ckpt.ltdb";
    cleanup(path);
    defer cleanup(path);

    var db = try Database.open(allocator, path, .{ .create = true, .config = .{
        .enable_fts = false,
        .enable_vector = false,
        .auto_checkpoint = .{ .max_wal_frames = 16 },
    } });
    defer db.close();

    var reader = try db.beginTransaction(.read_only);
    for (0..200) |_| {
        var txn = try db.beginTransaction(.read_write);
        _ = try db.createNode(&txn, &.{"Person"});
        try db.commitTransaction(&txn);
    }
    try std.testing.expect(db.wal.?.header.frame_count < 64);
    try db.commitTransaction(&reader);
}

test "a checkpoint is refused while a transaction has records in the log" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_crash_ckpt_refused.ltdb";
    cleanup(path);
    defer cleanup(path);

    var db = try openCrashTestDb(allocator, path, true);
    defer db.close();

    // Log through the transaction manager directly, the way the paths
    // without an overlay do: the record is in the log before commit.
    const tm = &db.txn_manager.?;
    var txn = try tm.begin(.read_write, .snapshot);
    const payload = try lattice.transaction.wal_payload.serializeVectorMutationAlloc(allocator, 1, null);
    defer allocator.free(payload);
    _ = try tm.logOperation(&txn, .update, payload);

    try std.testing.expectError(error.TransactionConflict, db.checkpoint(.truncate));

    try tm.abort(&txn);
    _ = try db.checkpoint(.truncate);
}

test "a commit is durable before it is applied, and a failed apply stops the handle" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_crash_commit_point.ltdb";
    const killed = "/tmp/lattice_crash_commit_point_killed.ltdb";
    cleanup(path);
    defer cleanup(path);
    cleanup(killed);
    defer cleanup(killed);

    var before: NodeId = undefined;
    var during: NodeId = undefined;
    {
        var db = try openCrashTestDb(allocator, path, true);
        defer db.close();

        var txn = try db.beginTransaction(.read_write);
        before = try db.createNode(&txn, &.{"Person"});
        try db.commitTransaction(&txn);

        // This commit reaches the log and then fails where it would have
        // been applied to the trees.
        txn = try db.beginTransaction(.read_write);
        during = try db.createNode(&txn, &.{"Person"});
        db.fail_next_apply_for_testing = true;
        try std.testing.expectError(error.IoError, db.commitTransaction(&txn));

        // The handle no longer matches its log, so it takes no more work,
        // and it does not write that state into the file.
        try std.testing.expectError(error.IoError, db.beginTransaction(.read_only));
        try std.testing.expectError(error.IoError, db.beginTransaction(.read_write));
        try std.testing.expectError(error.IoError, db.checkpoint(.truncate));
        try std.testing.expect(!try db.nodeExists(during));

        try copyAsIfKilled(allocator, path, killed);
    }

    // The commit record was durable before the failure, so both a kill and a
    // close leave a database that recovers the transaction.
    for ([_][]const u8{ killed, path }) |reopened| {
        var db = try openCrashTestDb(allocator, reopened, false);
        defer db.close();
        try std.testing.expect(try db.nodeExists(before));
        try std.testing.expect(try db.nodeExists(during));
        var txn = try db.beginTransaction(.read_write);
        _ = try db.createNode(&txn, &.{"Person"});
        try db.commitTransaction(&txn);
    }
}

test "commits in normal sync mode survive a kill" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_crash_sync_normal.ltdb";
    const killed = "/tmp/lattice_crash_sync_normal_killed.ltdb";
    cleanup(path);
    defer cleanup(path);
    cleanup(killed);
    defer cleanup(killed);

    // Past the checkpoint threshold several times, so both the commits still
    // in the log and the ones a checkpoint folded into the file are covered.
    const config = DatabaseConfig{
        .enable_fts = false,
        .enable_vector = false,
        .sync_mode = .normal,
        .auto_checkpoint = .{ .max_wal_frames = 64 },
    };
    const count = 300;
    const ids = try allocator.alloc(NodeId, count);
    defer allocator.free(ids);
    {
        var db = try Database.open(allocator, path, .{ .create = true, .config = config });
        defer db.close();
        try std.testing.expect(!db.txn_manager.?.sync_on_commit);
        for (ids) |*id| {
            var txn = try db.beginTransaction(.read_write);
            id.* = try db.createNode(&txn, &.{"Person"});
            try db.commitTransaction(&txn);
        }
        try copyAsIfKilled(allocator, path, killed);
    }

    var db = try Database.open(allocator, killed, .{ .config = config });
    defer db.close();
    for (ids) |id| try std.testing.expect(try db.nodeExists(id));
}

test "fragmented vector replacements and removals replay from WAL" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_crash_vectors.ltdb";
    cleanup(path);
    defer cleanup(path);
    const config = DatabaseConfig{
        .enable_fts = false,
        .enable_vector = true,
        .vector_dimensions = 1536,
        .auto_checkpoint = null,
    };
    var original = [_]f32{0} ** 1536;
    original[0] = 1;
    var replacement = [_]f32{0} ** 1536;
    replacement[1] = 1;
    var keep_id: NodeId = undefined;
    var remove_id: NodeId = undefined;
    {
        var db = try Database.open(allocator, path, .{ .create = true, .config = config });
        defer db.close();
        var txn = try db.beginTransaction(.read_write);
        keep_id = try db.createNode(&txn, &.{"EvidenceChunk"});
        remove_id = try db.createNode(&txn, &.{"EvidenceChunk"});
        try db.setNodeVectorInTxn(&txn, keep_id, &original);
        try db.setNodeVectorInTxn(&txn, remove_id, &original);
        try db.commitTransaction(&txn);
        txn = try db.beginTransaction(.read_write);
        try db.setNodeVectorInTxn(&txn, keep_id, &replacement);
        try db.deleteNode(&txn, remove_id);
        try db.commitTransaction(&txn);
    }
    // Discard all persisted trees/vector pages so the answer must come from WAL.
    try simulateCrash(path);
    var db = try Database.open(allocator, path, .{ .config = config });
    defer db.close();
    try std.testing.expect(try db.nodeExists(keep_id));
    try std.testing.expect(!try db.nodeExists(remove_id));
    const results = try db.vectorSearch(&replacement, 10, null);
    defer db.freeVectorSearchResults(results);
    try std.testing.expectEqual(@as(usize, 1), results.len);
    try std.testing.expectEqual(keep_id, results[0].node_id);
    try std.testing.expectApproxEqAbs(@as(f32, 0), results[0].distance, 0.00001);
}

test "committed transaction recovered after crash" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_crash_committed.ltdb";
    cleanup(path);
    defer cleanup(path);

    var node_id: NodeId = undefined;

    // Create a node in a committed transaction
    {
        var db = try openCrashTestDb(allocator, path, true);
        var txn = try db.beginTransaction(.read_write);
        node_id = try db.createNode(&txn, &[_][]const u8{});
        try db.commitTransaction(&txn);
        db.close();
    }

    // Simulate crash
    try simulateCrash(path);

    // Reopen - recovery should replay the committed transaction
    {
        var db = try openCrashTestDb(allocator, path, false);
        defer db.close();
        try std.testing.expect(try db.nodeExists(node_id));
    }
}

test "committed 1MB property recovered from fragmented WAL after crash" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_crash_large_property.ltdb";
    cleanup(path);
    defer cleanup(path);

    const payload = try allocator.alloc(u8, 1024 * 1024);
    defer allocator.free(payload);
    @memset(payload, 'r');

    var node_id: NodeId = undefined;

    {
        var db = try openCrashTestDb(allocator, path, true);
        var txn = try db.beginTransaction(.read_write);
        node_id = try db.createNode(&txn, &[_][]const u8{"Doc"});
        try db.setNodeProperty(&txn, node_id, "properties_json", .{ .string_val = payload });
        try db.commitTransaction(&txn);
        db.close();
    }

    try simulateCrash(path);

    {
        var db = try openCrashTestDb(allocator, path, false);
        defer db.close();

        var got = (try db.getNodeProperty(node_id, "properties_json")).?;
        defer got.deinit(allocator);
        try std.testing.expectEqualSlices(u8, payload, got.string_val);
    }
}

test "uncommitted transaction not recovered" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_crash_uncommitted.ltdb";
    cleanup(path);
    defer cleanup(path);

    var node_id: NodeId = undefined;

    // Create a node but do NOT commit
    {
        var db = try openCrashTestDb(allocator, path, true);
        var txn = try db.beginTransaction(.read_write);
        node_id = try db.createNode(&txn, &[_][]const u8{});
        // Abort instead of commit
        try db.abortTransaction(&txn);
        db.close();
    }

    // Simulate crash
    try simulateCrash(path);

    // Reopen - uncommitted node should NOT be present
    {
        var db = try openCrashTestDb(allocator, path, false);
        defer db.close();
        try std.testing.expect(!(try db.nodeExists(node_id)));
    }
}

test "multiple committed transactions all recovered" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_crash_multi_commit.ltdb";
    cleanup(path);
    defer cleanup(path);

    var ids: [3]NodeId = undefined;

    // Create 3 nodes in 3 separate committed transactions
    {
        var db = try openCrashTestDb(allocator, path, true);
        for (0..3) |i| {
            var txn = try db.beginTransaction(.read_write);
            ids[i] = try db.createNode(&txn, &[_][]const u8{});
            try db.commitTransaction(&txn);
        }
        db.close();
    }

    // Simulate crash
    try simulateCrash(path);

    // Reopen - all 3 nodes should exist
    {
        var db = try openCrashTestDb(allocator, path, false);
        defer db.close();
        for (ids) |id| {
            try std.testing.expect(try db.nodeExists(id));
        }
    }
}

test "only committed transactions recovered in mixed workload" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_crash_mixed.ltdb";
    cleanup(path);
    defer cleanup(path);

    var committed_ids: [2]NodeId = undefined;
    var aborted_id: NodeId = undefined;

    {
        var db = try openCrashTestDb(allocator, path, true);

        // First committed transaction
        {
            var txn = try db.beginTransaction(.read_write);
            committed_ids[0] = try db.createNode(&txn, &[_][]const u8{});
            try db.commitTransaction(&txn);
        }

        // Aborted transaction
        {
            var txn = try db.beginTransaction(.read_write);
            aborted_id = try db.createNode(&txn, &[_][]const u8{});
            try db.abortTransaction(&txn);
        }

        // Second committed transaction
        {
            var txn = try db.beginTransaction(.read_write);
            committed_ids[1] = try db.createNode(&txn, &[_][]const u8{});
            try db.commitTransaction(&txn);
        }

        db.close();
    }

    // Simulate crash
    try simulateCrash(path);

    // Reopen - only committed nodes should exist
    {
        var db = try openCrashTestDb(allocator, path, false);
        defer db.close();
        try std.testing.expect(try db.nodeExists(committed_ids[0]));
        try std.testing.expect(try db.nodeExists(committed_ids[1]));
        try std.testing.expect(!(try db.nodeExists(aborted_id)));
    }
}

test "committed stream records and offsets recovered after crash" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_crash_streams.ltdb";
    cleanup(path);
    defer cleanup(path);

    {
        var db = try openCrashTestDb(allocator, path, true);
        var txn = try db.beginTransaction(.read_write);
        try db.publishStream(&txn, "jobs", "queued", .{ .string_val = "job-1" });
        try db.setStreamOffset(&txn, "jobs", "worker-a", 1);
        try db.commitTransaction(&txn);
        db.close();
    }

    try simulateCrash(path);

    {
        var db = try openCrashTestDb(allocator, path, false);
        defer db.close();

        var batch = try db.readStream("jobs", 0, 10, 0);
        defer batch.deinit();
        try std.testing.expectEqual(@as(usize, 1), batch.records.len);
        try std.testing.expectEqual(@as(u64, 1), batch.records[0].sequence);
        try std.testing.expectEqualStrings("queued", batch.records[0].kind);
        try std.testing.expectEqualStrings("job-1", batch.records[0].payload.string_val);

        const offset = try db.getStreamOffset("jobs", "worker-a");
        try std.testing.expect(offset != null);
        try std.testing.expectEqual(@as(u64, 1), offset.?);
    }
}

test "aborted stream records and changefeed records are not recovered" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_crash_streams_aborted.ltdb";
    cleanup(path);
    defer cleanup(path);

    {
        var db = try openCrashTestDb(allocator, path, true);
        var txn = try db.beginTransaction(.read_write);
        _ = try db.createNode(&txn, &[_][]const u8{"Hidden"});
        try db.publishStream(&txn, "events", null, .{ .string_val = "hidden" });
        try db.abortTransaction(&txn);
        db.close();
    }

    try simulateCrash(path);

    {
        var db = try openCrashTestDb(allocator, path, false);
        defer db.close();

        var events = try db.readStream("events", 0, 10, 0);
        defer events.deinit();
        try std.testing.expectEqual(@as(usize, 0), events.records.len);

        var changes = try db.readChanges(0, 10, 0);
        defer changes.deinit();
        try std.testing.expectEqual(@as(usize, 0), changes.records.len);
    }
}

test "edges recovered with source and target nodes" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_crash_edges.ltdb";
    cleanup(path);
    defer cleanup(path);

    var src_id: NodeId = undefined;
    var dst_id: NodeId = undefined;

    // Create nodes and edge in a committed transaction
    {
        var db = try openCrashTestDb(allocator, path, true);
        var txn = try db.beginTransaction(.read_write);
        src_id = try db.createNode(&txn, &[_][]const u8{});
        dst_id = try db.createNode(&txn, &[_][]const u8{});
        try db.createEdge(&txn, src_id, dst_id, "KNOWS");
        try db.commitTransaction(&txn);
        db.close();
    }

    // Simulate crash
    try simulateCrash(path);

    // Reopen - both nodes should exist after recovery
    {
        var db = try openCrashTestDb(allocator, path, false);
        defer db.close();
        try std.testing.expect(try db.nodeExists(src_id));
        try std.testing.expect(try db.nodeExists(dst_id));
    }
}

test "multi-label nodes recover secondary label index after crash" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_crash_secondary_labels.ltdb";
    cleanup(path);
    defer cleanup(path);

    var node_id: NodeId = undefined;

    {
        var db = try openCrashTestDb(allocator, path, true);
        var txn = try db.beginTransaction(.read_write);
        node_id = try db.createNode(&txn, &[_][]const u8{ "Person", "Employee" });
        try db.commitTransaction(&txn);
        db.close();
    }

    try simulateCrash(path);

    {
        var db = try openCrashTestDb(allocator, path, false);
        defer db.close();

        try std.testing.expect(try db.nodeExists(node_id));

        var node = (try db.getNode(node_id)).?;
        defer node.deinit(allocator);
        try std.testing.expectEqual(@as(usize, 2), node.labels.len);

        const employee_nodes = try db.getNodesByLabel("Employee");
        defer allocator.free(employee_nodes);
        try std.testing.expectEqual(@as(usize, 1), employee_nodes.len);
        try std.testing.expectEqual(node_id, employee_nodes[0]);
    }
}

test "label additions recovered after crash" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_crash_label_addition.ltdb";
    cleanup(path);
    defer cleanup(path);

    var node_id: NodeId = undefined;

    {
        var db = try openCrashTestDb(allocator, path, true);
        var txn = try db.beginTransaction(.read_write);
        node_id = try db.createNode(&txn, &[_][]const u8{"Person"});
        try db.addNodeLabel(&txn, node_id, "Employee");
        try db.commitTransaction(&txn);
        db.close();
    }

    try simulateCrash(path);

    {
        var db = try openCrashTestDb(allocator, path, false);
        defer db.close();

        const employee_nodes = try db.getNodesByLabel("Employee");
        defer allocator.free(employee_nodes);
        try std.testing.expectEqual(@as(usize, 1), employee_nodes.len);
        try std.testing.expectEqual(node_id, employee_nodes[0]);
    }
}

test "edge properties recovered after crash" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_crash_edge_properties.ltdb";
    cleanup(path);
    defer cleanup(path);

    var src_id: NodeId = undefined;
    var dst_id: NodeId = undefined;
    var edge_id: EdgeId = undefined;

    {
        var db = try openCrashTestDb(allocator, path, true);
        var txn = try db.beginTransaction(.read_write);
        src_id = try db.createNode(&txn, &[_][]const u8{});
        dst_id = try db.createNode(&txn, &[_][]const u8{});
        edge_id = try db.createEdgeAndGetId(&txn, src_id, dst_id, "KNOWS");
        try db.setEdgePropertyById(&txn, edge_id, "since", .{ .int_val = 2026 });
        try db.setEdgePropertyById(&txn, edge_id, "note", .{ .string_val = "stable" });
        try db.commitTransaction(&txn);
        db.close();
    }

    try simulateCrash(path);

    {
        var db = try openCrashTestDb(allocator, path, false);
        defer db.close();

        try std.testing.expect(try db.nodeExists(src_id));
        try std.testing.expect(try db.nodeExists(dst_id));

        const since = (try db.getEdgePropertyById(edge_id, "since")).?;
        switch (since) {
            .int_val => |value| try std.testing.expectEqual(@as(i64, 2026), value),
            else => return error.UnexpectedPropertyValue,
        }

        const note = (try db.getEdgePropertyById(edge_id, "note")).?;
        defer {
            var owned_note = note;
            owned_note.deinit(allocator);
        }
        switch (note) {
            .string_val => |value| try std.testing.expectEqualStrings("stable", value),
            else => return error.UnexpectedPropertyValue,
        }
    }
}

test "parallel edges recovered with stable edge ids" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_crash_parallel_edges.ltdb";
    cleanup(path);
    defer cleanup(path);

    var src_id: NodeId = undefined;
    var dst_id: NodeId = undefined;
    var edge1: EdgeId = undefined;
    var edge2: EdgeId = undefined;

    {
        var db = try openCrashTestDb(allocator, path, true);
        var txn = try db.beginTransaction(.read_write);
        src_id = try db.createNode(&txn, &[_][]const u8{});
        dst_id = try db.createNode(&txn, &[_][]const u8{});
        edge1 = try db.createEdgeAndGetId(&txn, src_id, dst_id, "KNOWS");
        edge2 = try db.createEdgeAndGetId(&txn, src_id, dst_id, "KNOWS");
        try db.commitTransaction(&txn);
        db.close();
    }

    try simulateCrash(path);

    {
        var db = try openCrashTestDb(allocator, path, false);
        defer db.close();

        try std.testing.expect(try db.nodeExists(src_id));
        try std.testing.expect(try db.nodeExists(dst_id));

        var restored1 = try db.edge_store.getById(edge1);
        defer restored1.deinit(allocator);
        var restored2 = try db.edge_store.getById(edge2);
        defer restored2.deinit(allocator);

        var iter = try db.getOutgoingEdgeRefs(src_id);
        defer iter.deinit();
        var count: usize = 0;
        var found1 = false;
        var found2 = false;
        while (try iter.next()) |edge_ref| {
            count += 1;
            if (edge_ref.id == edge1) found1 = true;
            if (edge_ref.id == edge2) found2 = true;
        }

        try std.testing.expectEqual(@as(usize, 2), count);
        try std.testing.expect(found1);
        try std.testing.expect(found2);
    }
}

test "committed deleteEdgeById survives crash recovery with parallel edges" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_crash_delete_edge_id.ltdb";
    cleanup(path);
    defer cleanup(path);

    var edge1: EdgeId = undefined;
    var edge2: EdgeId = undefined;

    {
        var db = try openCrashTestDb(allocator, path, true);

        var create_txn = try db.beginTransaction(.read_write);
        const src_id = try db.createNode(&create_txn, &[_][]const u8{});
        const dst_id = try db.createNode(&create_txn, &[_][]const u8{});
        edge1 = try db.createEdgeAndGetId(&create_txn, src_id, dst_id, "REL");
        edge2 = try db.createEdgeAndGetId(&create_txn, src_id, dst_id, "REL");
        try db.commitTransaction(&create_txn);

        var delete_txn = try db.beginTransaction(.read_write);
        try db.deleteEdgeById(&delete_txn, edge1);
        try db.commitTransaction(&delete_txn);

        db.close();
    }

    try simulateCrash(path);

    {
        var db = try openCrashTestDb(allocator, path, false);
        defer db.close();

        try std.testing.expectError(lattice.graph.edge.EdgeError.NotFound, db.edge_store.getById(edge1));
        var kept = try db.edge_store.getById(edge2);
        defer kept.deinit(allocator);
    }
}

test "committed endpoint delete survives crash recovery with parallel edges" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_crash_delete_edge_endpoint.ltdb";
    cleanup(path);
    defer cleanup(path);

    var edge1: EdgeId = undefined;
    var edge2: EdgeId = undefined;
    var src_id: NodeId = undefined;
    var dst_id: NodeId = undefined;

    {
        var db = try openCrashTestDb(allocator, path, true);

        var create_txn = try db.beginTransaction(.read_write);
        src_id = try db.createNode(&create_txn, &[_][]const u8{});
        dst_id = try db.createNode(&create_txn, &[_][]const u8{});
        edge1 = try db.createEdgeAndGetId(&create_txn, src_id, dst_id, "REL");
        edge2 = try db.createEdgeAndGetId(&create_txn, src_id, dst_id, "REL");
        try db.commitTransaction(&create_txn);

        var delete_txn = try db.beginTransaction(.read_write);
        try db.deleteEdge(&delete_txn, src_id, dst_id, "REL");
        try db.commitTransaction(&delete_txn);

        db.close();
    }

    try simulateCrash(path);

    {
        var db = try openCrashTestDb(allocator, path, false);
        defer db.close();

        // Endpoint delete should remove first matching edge (lowest edge_id).
        try std.testing.expectError(lattice.graph.edge.EdgeError.NotFound, db.edge_store.getById(edge1));
        var kept = try db.edge_store.getById(edge2);
        defer kept.deinit(allocator);

        const new_edge = try db.createEdgeAndGetId(null, src_id, dst_id, "REL");
        try std.testing.expect(new_edge > edge2);
    }
}

test "edge id allocator stays monotonic after crash replay with deleted max id" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_crash_edge_id_allocator_monotonic.ltdb";
    cleanup(path);
    defer cleanup(path);

    var src_id: NodeId = undefined;
    var dst_id: NodeId = undefined;
    var edge1: EdgeId = undefined;
    var edge2: EdgeId = undefined;
    var deleted_max_edge: EdgeId = undefined;

    {
        var db = try openCrashTestDb(allocator, path, true);

        var create_txn = try db.beginTransaction(.read_write);
        src_id = try db.createNode(&create_txn, &[_][]const u8{});
        dst_id = try db.createNode(&create_txn, &[_][]const u8{});
        edge1 = try db.createEdgeAndGetId(&create_txn, src_id, dst_id, "REL");
        edge2 = try db.createEdgeAndGetId(&create_txn, src_id, dst_id, "REL");
        deleted_max_edge = try db.createEdgeAndGetId(&create_txn, src_id, dst_id, "REL");
        try db.commitTransaction(&create_txn);

        var delete_txn = try db.beginTransaction(.read_write);
        try db.deleteEdgeById(&delete_txn, deleted_max_edge);
        try db.commitTransaction(&delete_txn);

        db.close();
    }

    try simulateCrash(path);

    {
        var db = try openCrashTestDb(allocator, path, false);
        defer db.close();

        // Replay must preserve exact delete and keep lower IDs.
        var kept1 = try db.edge_store.getById(edge1);
        defer kept1.deinit(allocator);
        var kept2 = try db.edge_store.getById(edge2);
        defer kept2.deinit(allocator);
        try std.testing.expectError(lattice.graph.edge.EdgeError.NotFound, db.edge_store.getById(deleted_max_edge));

        // New IDs must continue past the highest previously allocated ID,
        // even when that max ID was deleted before crash.
        const new_edge = try db.createEdgeAndGetId(null, src_id, dst_id, "REL");
        try std.testing.expect(new_edge > deleted_max_edge);

        var iter = try db.getOutgoingEdgeRefs(src_id);
        defer iter.deinit();
        var count: usize = 0;
        while (try iter.next()) |_| {
            count += 1;
        }
        try std.testing.expectEqual(@as(usize, 3), count);
    }
}

test "aborted edge delete is not replayed after crash" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_crash_aborted_delete_edge_id.ltdb";
    cleanup(path);
    defer cleanup(path);

    var edge1: EdgeId = undefined;
    var edge2: EdgeId = undefined;

    {
        var db = try openCrashTestDb(allocator, path, true);

        var create_txn = try db.beginTransaction(.read_write);
        const src_id = try db.createNode(&create_txn, &[_][]const u8{});
        const dst_id = try db.createNode(&create_txn, &[_][]const u8{});
        edge1 = try db.createEdgeAndGetId(&create_txn, src_id, dst_id, "REL");
        edge2 = try db.createEdgeAndGetId(&create_txn, src_id, dst_id, "REL");
        try db.commitTransaction(&create_txn);

        var delete_txn = try db.beginTransaction(.read_write);
        try db.deleteEdgeById(&delete_txn, edge1);
        try db.abortTransaction(&delete_txn);

        db.close();
    }

    try simulateCrash(path);

    {
        var db = try openCrashTestDb(allocator, path, false);
        defer db.close();

        var restored1 = try db.edge_store.getById(edge1);
        defer restored1.deinit(allocator);
        var restored2 = try db.edge_store.getById(edge2);
        defer restored2.deinit(allocator);
    }
}

test "aborted endpoint delete is not replayed after crash" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_crash_aborted_endpoint_delete_edge.ltdb";
    cleanup(path);
    defer cleanup(path);

    var edge1: EdgeId = undefined;
    var edge2: EdgeId = undefined;
    var src_id: NodeId = undefined;
    var dst_id: NodeId = undefined;

    {
        var db = try openCrashTestDb(allocator, path, true);

        var create_txn = try db.beginTransaction(.read_write);
        src_id = try db.createNode(&create_txn, &[_][]const u8{});
        dst_id = try db.createNode(&create_txn, &[_][]const u8{});
        edge1 = try db.createEdgeAndGetId(&create_txn, src_id, dst_id, "REL");
        edge2 = try db.createEdgeAndGetId(&create_txn, src_id, dst_id, "REL");
        try db.commitTransaction(&create_txn);

        var delete_txn = try db.beginTransaction(.read_write);
        try db.deleteEdge(&delete_txn, src_id, dst_id, "REL");
        try db.abortTransaction(&delete_txn);

        db.close();
    }

    try simulateCrash(path);

    {
        var db = try openCrashTestDb(allocator, path, false);
        defer db.close();

        var restored1 = try db.edge_store.getById(edge1);
        defer restored1.deinit(allocator);
        var restored2 = try db.edge_store.getById(edge2);
        defer restored2.deinit(allocator);

        var iter = try db.getOutgoingEdgeRefs(src_id);
        defer iter.deinit();
        var count: usize = 0;
        while (try iter.next()) |_| {
            count += 1;
        }
        try std.testing.expectEqual(@as(usize, 2), count);
    }
}

test "insert and delete same edge in one committed txn leaves no edge after crash" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_crash_insert_delete_same_txn_edge.ltdb";
    cleanup(path);
    defer cleanup(path);

    var src_id: NodeId = undefined;
    var dst_id: NodeId = undefined;
    var deleted_edge: EdgeId = undefined;

    {
        var db = try openCrashTestDb(allocator, path, true);

        var txn = try db.beginTransaction(.read_write);
        src_id = try db.createNode(&txn, &[_][]const u8{});
        dst_id = try db.createNode(&txn, &[_][]const u8{});
        deleted_edge = try db.createEdgeAndGetId(&txn, src_id, dst_id, "REL");
        try db.deleteEdgeById(&txn, deleted_edge);
        try db.commitTransaction(&txn);

        db.close();
    }

    try simulateCrash(path);

    {
        var db = try openCrashTestDb(allocator, path, false);
        defer db.close();

        try std.testing.expectError(lattice.graph.edge.EdgeError.NotFound, db.edge_store.getById(deleted_edge));

        var iter = try db.getOutgoingEdgeRefs(src_id);
        defer iter.deinit();
        try std.testing.expect((try iter.next()) == null);

        const new_edge = try db.createEdgeAndGetId(null, src_id, dst_id, "REL");
        try std.testing.expect(new_edge > deleted_edge);
    }
}

test "delete all committed parallel edges then recover keeps allocator monotonic" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_crash_delete_all_edges_allocator.ltdb";
    cleanup(path);
    defer cleanup(path);

    var src_id: NodeId = undefined;
    var dst_id: NodeId = undefined;
    var edge1: EdgeId = undefined;
    var edge2: EdgeId = undefined;

    {
        var db = try openCrashTestDb(allocator, path, true);

        var create_txn = try db.beginTransaction(.read_write);
        src_id = try db.createNode(&create_txn, &[_][]const u8{});
        dst_id = try db.createNode(&create_txn, &[_][]const u8{});
        edge1 = try db.createEdgeAndGetId(&create_txn, src_id, dst_id, "REL");
        edge2 = try db.createEdgeAndGetId(&create_txn, src_id, dst_id, "REL");
        try db.commitTransaction(&create_txn);

        var delete_txn = try db.beginTransaction(.read_write);
        try db.deleteEdgeById(&delete_txn, edge1);
        try db.deleteEdgeById(&delete_txn, edge2);
        try db.commitTransaction(&delete_txn);

        db.close();
    }

    try simulateCrash(path);

    {
        var db = try openCrashTestDb(allocator, path, false);
        defer db.close();

        try std.testing.expectError(lattice.graph.edge.EdgeError.NotFound, db.edge_store.getById(edge1));
        try std.testing.expectError(lattice.graph.edge.EdgeError.NotFound, db.edge_store.getById(edge2));

        var iter = try db.getOutgoingEdgeRefs(src_id);
        defer iter.deinit();
        try std.testing.expect((try iter.next()) == null);

        const new_edge = try db.createEdgeAndGetId(null, src_id, dst_id, "REL");
        try std.testing.expect(new_edge > edge2);
    }
}

test "aborted edge create is not replayed and allocator remains monotonic after crash" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_crash_aborted_create_edge_allocator.ltdb";
    cleanup(path);
    defer cleanup(path);

    var src_id: NodeId = undefined;
    var dst_id: NodeId = undefined;
    var committed_edge: EdgeId = undefined;
    var aborted_edge: EdgeId = undefined;

    {
        var db = try openCrashTestDb(allocator, path, true);

        var txn1 = try db.beginTransaction(.read_write);
        src_id = try db.createNode(&txn1, &[_][]const u8{});
        dst_id = try db.createNode(&txn1, &[_][]const u8{});
        committed_edge = try db.createEdgeAndGetId(&txn1, src_id, dst_id, "REL");
        try db.commitTransaction(&txn1);

        var txn2 = try db.beginTransaction(.read_write);
        aborted_edge = try db.createEdgeAndGetId(&txn2, src_id, dst_id, "REL");
        try db.abortTransaction(&txn2);

        db.close();
    }

    try simulateCrash(path);

    {
        var db = try openCrashTestDb(allocator, path, false);
        defer db.close();

        var kept = try db.edge_store.getById(committed_edge);
        defer kept.deinit(allocator);
        try std.testing.expectError(lattice.graph.edge.EdgeError.NotFound, db.edge_store.getById(aborted_edge));

        const new_edge = try db.createEdgeAndGetId(null, src_id, dst_id, "REL");
        try std.testing.expect(new_edge > committed_edge);
    }
}

test "committed edge property update survives crash recovery" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_crash_committed_edge_property_update.ltdb";
    cleanup(path);
    defer cleanup(path);

    var edge_id: EdgeId = undefined;

    {
        var db = try openCrashTestDb(allocator, path, true);

        var create_txn = try db.beginTransaction(.read_write);
        const src_id = try db.createNode(&create_txn, &[_][]const u8{});
        const dst_id = try db.createNode(&create_txn, &[_][]const u8{});
        edge_id = try db.createEdgeAndGetId(&create_txn, src_id, dst_id, "REL");
        try db.setEdgePropertyById(&create_txn, edge_id, "w", .{ .int_val = 1 });
        try db.commitTransaction(&create_txn);

        var update_txn = try db.beginTransaction(.read_write);
        try db.setEdgePropertyById(&update_txn, edge_id, "w", .{ .int_val = 7 });
        try db.commitTransaction(&update_txn);

        db.close();
    }

    try simulateCrash(path);

    {
        var db = try openCrashTestDb(allocator, path, false);
        defer db.close();

        var restored = try db.edge_store.getById(edge_id);
        defer restored.deinit(allocator);
        try std.testing.expectEqual(@as(usize, 1), restored.properties.len);
        try std.testing.expectEqual(@as(i64, 7), restored.properties[0].value.int_val);
    }
}

test "aborted edge property update is not replayed after crash" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_crash_aborted_edge_property_update.ltdb";
    cleanup(path);
    defer cleanup(path);

    var edge_id: EdgeId = undefined;

    {
        var db = try openCrashTestDb(allocator, path, true);

        var create_txn = try db.beginTransaction(.read_write);
        const src_id = try db.createNode(&create_txn, &[_][]const u8{});
        const dst_id = try db.createNode(&create_txn, &[_][]const u8{});
        edge_id = try db.createEdgeAndGetId(&create_txn, src_id, dst_id, "REL");
        try db.setEdgePropertyById(&create_txn, edge_id, "w", .{ .int_val = 1 });
        try db.commitTransaction(&create_txn);

        var update_txn = try db.beginTransaction(.read_write);
        try db.setEdgePropertyById(&update_txn, edge_id, "w", .{ .int_val = 7 });
        try db.abortTransaction(&update_txn);

        db.close();
    }

    try simulateCrash(path);

    {
        var db = try openCrashTestDb(allocator, path, false);
        defer db.close();

        var restored = try db.edge_store.getById(edge_id);
        defer restored.deinit(allocator);
        try std.testing.expectEqual(@as(usize, 1), restored.properties.len);
        try std.testing.expectEqual(@as(i64, 1), restored.properties[0].value.int_val);
    }
}

test "recovery is idempotent" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_crash_idempotent.ltdb";
    cleanup(path);
    defer cleanup(path);

    var node_id: NodeId = undefined;

    // Create and commit a node
    {
        var db = try openCrashTestDb(allocator, path, true);
        var txn = try db.beginTransaction(.read_write);
        node_id = try db.createNode(&txn, &[_][]const u8{});
        try db.commitTransaction(&txn);
        db.close();
    }

    // First crash + recover
    try simulateCrash(path);
    {
        var db = try openCrashTestDb(allocator, path, false);
        try std.testing.expect(try db.nodeExists(node_id));
        db.close();
    }

    // Second crash + recover (same WAL replayed again)
    try simulateCrash(path);
    {
        var db = try openCrashTestDb(allocator, path, false);
        defer db.close();
        try std.testing.expect(try db.nodeExists(node_id));
    }
}

test "large transaction with many nodes recovered" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_crash_large.ltdb";
    cleanup(path);
    defer cleanup(path);

    const count = 100;
    var ids: [count]NodeId = undefined;

    // Create 100 nodes in a single committed transaction
    {
        var db = try openCrashTestDb(allocator, path, true);
        var txn = try db.beginTransaction(.read_write);
        for (0..count) |i| {
            ids[i] = try db.createNode(&txn, &[_][]const u8{});
        }
        try db.commitTransaction(&txn);
        db.close();
    }

    // Simulate crash
    try simulateCrash(path);

    // Reopen - all 100 nodes should exist
    {
        var db = try openCrashTestDb(allocator, path, false);
        defer db.close();
        for (ids) |id| {
            try std.testing.expect(try db.nodeExists(id));
        }
    }
}

test "data persists across multiple crash-recover cycles" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_crash_cycles.ltdb";
    cleanup(path);
    defer cleanup(path);

    var batch1_ids: [5]NodeId = undefined;
    var batch2_ids: [5]NodeId = undefined;

    // Batch 1: create 5 nodes, commit
    {
        var db = try openCrashTestDb(allocator, path, true);
        var txn = try db.beginTransaction(.read_write);
        for (0..5) |i| {
            batch1_ids[i] = try db.createNode(&txn, &[_][]const u8{});
        }
        try db.commitTransaction(&txn);
        db.close();
    }

    // First crash + recover
    try simulateCrash(path);
    {
        var db = try openCrashTestDb(allocator, path, false);
        // Verify batch 1 recovered
        for (batch1_ids) |id| {
            try std.testing.expect(try db.nodeExists(id));
        }
        db.close();
    }

    // Batch 2: create 5 more nodes, commit
    {
        var db = try openCrashTestDb(allocator, path, false);
        var txn = try db.beginTransaction(.read_write);
        for (0..5) |i| {
            batch2_ids[i] = try db.createNode(&txn, &[_][]const u8{});
        }
        try db.commitTransaction(&txn);
        db.close();
    }

    // Second crash + recover
    try simulateCrash(path);
    {
        var db = try openCrashTestDb(allocator, path, false);
        defer db.close();
        // Both batches should be present
        for (batch1_ids) |id| {
            try std.testing.expect(try db.nodeExists(id));
        }
        for (batch2_ids) |id| {
            try std.testing.expect(try db.nodeExists(id));
        }
    }
}

test "property updates recovered after crash" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_crash_property.ltdb";
    cleanup(path);
    defer cleanup(path);

    var node_id: NodeId = undefined;

    // Create a node and set a property
    {
        var db = try openCrashTestDb(allocator, path, true);
        var txn = try db.beginTransaction(.read_write);
        node_id = try db.createNode(&txn, &[_][]const u8{});
        try db.setNodeProperty(&txn, node_id, "age", .{ .int_val = 42 });
        try db.commitTransaction(&txn);
        db.close();
    }

    // Simulate crash
    try simulateCrash(path);

    // Reopen - recovery should replay both insert and property update
    {
        var db = try openCrashTestDb(allocator, path, false);
        defer db.close();

        // Node should exist
        try std.testing.expect(try db.nodeExists(node_id));

        // Get the raw node and verify property is present
        var node = try db.node_store.get(node_id);
        defer node.deinit(allocator);

        // Should have exactly 1 property with int value 42
        try std.testing.expectEqual(@as(usize, 1), node.properties.len);
        try std.testing.expectEqual(@as(i64, 42), node.properties[0].value.int_val);
    }
}

test "multiple property updates on same node recovered" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_crash_multi_prop.ltdb";
    cleanup(path);
    defer cleanup(path);

    var node_id: NodeId = undefined;

    // Create a node and set multiple properties
    {
        var db = try openCrashTestDb(allocator, path, true);
        var txn = try db.beginTransaction(.read_write);
        node_id = try db.createNode(&txn, &[_][]const u8{});
        try db.setNodeProperty(&txn, node_id, "x", .{ .int_val = 10 });
        try db.setNodeProperty(&txn, node_id, "y", .{ .int_val = 20 });
        try db.setNodeProperty(&txn, node_id, "z", .{ .int_val = 30 });
        try db.commitTransaction(&txn);
        db.close();
    }

    // Simulate crash
    try simulateCrash(path);

    // Reopen - all 3 properties should be recovered
    {
        var db = try openCrashTestDb(allocator, path, false);
        defer db.close();

        try std.testing.expect(try db.nodeExists(node_id));

        var node = try db.node_store.get(node_id);
        defer node.deinit(allocator);

        // Should have 3 properties
        try std.testing.expectEqual(@as(usize, 3), node.properties.len);

        // Verify values (order may vary, so check by summing)
        var sum: i64 = 0;
        for (node.properties) |prop| {
            sum += prop.value.int_val;
        }
        try std.testing.expectEqual(@as(i64, 60), sum);
    }
}

test "property overwrite recovered with latest value" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_crash_prop_overwrite.ltdb";
    cleanup(path);
    defer cleanup(path);

    var node_id: NodeId = undefined;

    // Create a node, set a property, then overwrite it
    {
        var db = try openCrashTestDb(allocator, path, true);
        var txn = try db.beginTransaction(.read_write);
        node_id = try db.createNode(&txn, &[_][]const u8{});
        try db.setNodeProperty(&txn, node_id, "score", .{ .int_val = 100 });
        try db.setNodeProperty(&txn, node_id, "score", .{ .int_val = 999 });
        try db.commitTransaction(&txn);
        db.close();
    }

    // Simulate crash
    try simulateCrash(path);

    // Reopen - should have the latest value (999)
    {
        var db = try openCrashTestDb(allocator, path, false);
        defer db.close();

        try std.testing.expect(try db.nodeExists(node_id));

        var node = try db.node_store.get(node_id);
        defer node.deinit(allocator);

        // Should have exactly 1 property with the overwritten value
        try std.testing.expectEqual(@as(usize, 1), node.properties.len);
        try std.testing.expectEqual(@as(i64, 999), node.properties[0].value.int_val);
    }
}

test "symbol table recovered - property keys resolve after crash" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_crash_symbol_prop.ltdb";
    cleanup(path);
    defer cleanup(path);

    var node_id: NodeId = undefined;

    // Create a node and set multiple properties with string keys
    {
        var db = try openCrashTestDb(allocator, path, true);
        var txn = try db.beginTransaction(.read_write);
        node_id = try db.createNode(&txn, &[_][]const u8{});
        try db.setNodeProperty(&txn, node_id, "name", .{ .string_val = "Alice" });
        try db.setNodeProperty(&txn, node_id, "age", .{ .int_val = 30 });
        try db.setNodeProperty(&txn, node_id, "active", .{ .bool_val = true });
        try db.commitTransaction(&txn);
        db.close();
    }

    // Simulate crash
    try simulateCrash(path);

    // Reopen - symbol table should be recovered from WAL, allowing property lookup by key string
    {
        var db = try openCrashTestDb(allocator, path, false);
        defer db.close();

        try std.testing.expect(try db.nodeExists(node_id));

        // Test getNodeProperty by resolving the existing key string through the recovered symbol table
        var name_val = (try db.getNodeProperty(node_id, "name")).?;
        defer name_val.deinit(allocator);
        try std.testing.expectEqualStrings("Alice", name_val.string_val);

        const age_val = try db.getNodeProperty(node_id, "age");
        try std.testing.expect(age_val != null);
        try std.testing.expectEqual(@as(i64, 30), age_val.?.int_val);

        const active_val = try db.getNodeProperty(node_id, "active");
        try std.testing.expect(active_val != null);
        try std.testing.expect(active_val.?.bool_val);
    }
}

test "symbol table recovered - node labels resolved after crash" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_crash_symbol_label.ltdb";
    cleanup(path);
    defer cleanup(path);

    var node_id: NodeId = undefined;

    // Create a node with labels
    {
        var db = try openCrashTestDb(allocator, path, true);
        var txn = try db.beginTransaction(.read_write);
        node_id = try db.createNode(&txn, &[_][]const u8{ "Person", "Employee" });
        try db.commitTransaction(&txn);
        db.close();
    }

    // Simulate crash
    try simulateCrash(path);

    // Reopen - symbol table should be recovered, labels resolvable
    {
        var db = try openCrashTestDb(allocator, path, false);
        defer db.close();

        try std.testing.expect(try db.nodeExists(node_id));

        // Get raw node and verify labels can be resolved
        var node = try db.node_store.get(node_id);
        defer node.deinit(allocator);

        try std.testing.expectEqual(@as(usize, 2), node.labels.len);

        // Resolve labels via symbol table
        const label1 = try db.symbol_table.resolve(node.labels[0]);
        defer db.symbol_table.freeString(label1);
        try std.testing.expectEqualStrings("Person", label1);

        const label2 = try db.symbol_table.resolve(node.labels[1]);
        defer db.symbol_table.freeString(label2);
        try std.testing.expectEqualStrings("Employee", label2);
    }
}

test "symbol table recovered - edge types resolved after crash" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_crash_symbol_edge.ltdb";
    cleanup(path);
    defer cleanup(path);

    var src_id: NodeId = undefined;
    var dst_id: NodeId = undefined;

    // Create nodes and an edge with a specific type
    {
        var db = try openCrashTestDb(allocator, path, true);
        var txn = try db.beginTransaction(.read_write);
        src_id = try db.createNode(&txn, &[_][]const u8{});
        dst_id = try db.createNode(&txn, &[_][]const u8{});
        try db.createEdge(&txn, src_id, dst_id, "KNOWS");
        try db.commitTransaction(&txn);
        db.close();
    }

    // Simulate crash
    try simulateCrash(path);

    // Reopen - edge type should be resolvable via symbol table
    {
        var db = try openCrashTestDb(allocator, path, false);
        defer db.close();

        try std.testing.expect(try db.nodeExists(src_id));
        try std.testing.expect(try db.nodeExists(dst_id));

        // Verify "KNOWS" is in the symbol table after recovery
        const knows_id = try db.symbol_table.intern("KNOWS");
        const resolved = try db.symbol_table.resolve(knows_id);
        defer db.symbol_table.freeString(resolved);
        try std.testing.expectEqualStrings("KNOWS", resolved);
    }
}

test "committed data survives a kill while the buffer pool is evicting" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_crash_evict.ltdb";
    const killed = "/tmp/lattice_crash_evict_killed.ltdb";
    cleanup(path);
    defer cleanup(path);
    cleanup(killed);
    defer cleanup(killed);

    // A pool far smaller than the data, so dirty pages are pushed out of it
    // all the time, between checkpoints that each empty the log. Whatever
    // eviction writes after a checkpoint lands on top of the image that
    // checkpoint left, with nothing in the log to rebuild what it displaces.
    const config = DatabaseConfig{
        .enable_fts = false,
        .enable_vector = false,
        .buffer_pool_size = 48 * 4096,
        .auto_checkpoint = .{ .max_wal_frames = 300 },
    };
    const count = 4000;
    const snapshots = [_]usize{ 250, 700, 1130, 1500, 1870, 2600, 3140, 3999 };
    const ids = try allocator.alloc(NodeId, count);
    defer allocator.free(ids);

    var payload: [300]u8 = undefined;
    @memset(&payload, 'x');

    var db = try Database.open(allocator, path, .{ .create = true, .config = config });
    defer db.close();
    var next_snapshot: usize = 0;
    for (ids, 0..) |*id, i| {
        var txn = try db.beginTransaction(.read_write);
        id.* = try db.createNode(&txn, &.{"Person"});
        try db.setNodeProperty(&txn, id.*, "seq", .{ .int_val = @intCast(i) });
        try db.setNodeProperty(&txn, id.*, "pad", .{ .string_val = &payload });
        try db.commitTransaction(&txn);

        if (next_snapshot < snapshots.len and i == snapshots[next_snapshot]) {
            next_snapshot += 1;
            cleanup(killed);
            try copyAsIfKilled(allocator, path, killed);

            var reopened = try Database.open(allocator, killed, .{ .config = config });
            defer reopened.close();
            for (ids[0 .. i + 1], 0..) |committed, seq| {
                try std.testing.expect(try reopened.nodeExists(committed));
                var value = (try reopened.getNodeProperty(committed, "seq")).?;
                defer value.deinit(allocator);
                try std.testing.expectEqual(@as(i64, @intCast(seq)), value.int_val);
            }
            const people = try reopened.getNodesByLabel("Person");
            defer allocator.free(people);
            try std.testing.expectEqual(i + 1, people.len);
        }
    }
}

test "committed creates, updates and deletes survive a kill while the buffer pool is evicting" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_crash_evict_mixed.ltdb";
    const killed = "/tmp/lattice_crash_evict_mixed_killed.ltdb";
    cleanup(path);
    defer cleanup(path);
    cleanup(killed);
    defer cleanup(killed);

    // Values large enough to live in overflow chains, so that updates and
    // deletes free pages and later writes reuse them, all between checkpoints
    // that empty the log, in a pool that cannot hold the data.
    const config = DatabaseConfig{
        .enable_fts = false,
        .enable_vector = false,
        .buffer_pool_size = 48 * 4096,
        .auto_checkpoint = .{ .max_wal_frames = 200 },
    };

    const Expected = struct { id: NodeId, version: i64, alive: bool };
    var model: std.ArrayListUnmanaged(Expected) = .empty;
    defer model.deinit(allocator);

    var prng = std.Random.DefaultPrng.init(0x5EED);
    const random = prng.random();
    var payload: [6000]u8 = undefined;

    var db = try Database.open(allocator, path, .{ .create = true, .config = config });
    defer db.close();

    const rounds = 1200;
    for (0..rounds) |round| {
        var txn = try db.beginTransaction(.read_write);
        const choice = random.uintLessThan(u8, 10);
        if (model.items.len < 20 or choice < 5) {
            const id = try db.createNode(&txn, &.{"Doc"});
            const size = 200 + random.uintLessThan(usize, payload.len - 200);
            @memset(payload[0..size], 'a' + @as(u8, @intCast(round % 26)));
            try db.setNodeProperty(&txn, id, "version", .{ .int_val = 0 });
            try db.setNodeProperty(&txn, id, "body", .{ .string_val = payload[0..size] });
            try model.append(allocator, .{ .id = id, .version = 0, .alive = true });
        } else {
            const entry = &model.items[random.uintLessThan(usize, model.items.len)];
            if (!entry.alive) {
                // Nothing to do to a deleted node; commit an empty transaction.
            } else if (choice < 8) {
                const size = 200 + random.uintLessThan(usize, payload.len - 200);
                @memset(payload[0..size], 'A' + @as(u8, @intCast(round % 26)));
                entry.version += 1;
                try db.setNodeProperty(&txn, entry.id, "version", .{ .int_val = entry.version });
                try db.setNodeProperty(&txn, entry.id, "body", .{ .string_val = payload[0..size] });
            } else {
                try db.deleteNode(&txn, entry.id);
                entry.alive = false;
            }
        }
        try db.commitTransaction(&txn);

        if (round % 97 != 96 and round != rounds - 1) continue;

        cleanup(killed);
        try copyAsIfKilled(allocator, path, killed);
        var reopened = try Database.open(allocator, killed, .{ .config = config });
        defer reopened.close();
        var alive: usize = 0;
        for (model.items) |expected| {
            try std.testing.expectEqual(expected.alive, try reopened.nodeExists(expected.id));
            if (!expected.alive) continue;
            alive += 1;
            var version = (try reopened.getNodeProperty(expected.id, "version")).?;
            defer version.deinit(allocator);
            try std.testing.expectEqual(expected.version, version.int_val);
            var body = (try reopened.getNodeProperty(expected.id, "body")).?;
            defer body.deinit(allocator);
            try std.testing.expect(body.string_val.len >= 200);
        }
        const docs = try reopened.getNodesByLabel("Doc");
        defer allocator.free(docs);
        try std.testing.expectEqual(alive, docs.len);
    }
}

test "a flush interrupted partway is finished from the journal at the next open" {
    const allocator = std.testing.allocator;
    const journal_mod = lattice.storage.journal;
    const path = "/tmp/lattice_crash_torn_flush.ltdb";
    const torn = "/tmp/lattice_crash_torn_flush_torn.ltdb";
    const torn_journal = torn ++ "-journal";
    cleanup(path);
    defer cleanup(path);
    cleanup(torn);
    defer cleanup(torn);
    @import("compat").fs.cwd().deleteFile(torn_journal) catch {};
    defer @import("compat").fs.cwd().deleteFile(torn_journal) catch {};

    const config = DatabaseConfig{ .enable_fts = false, .enable_vector = false };
    const first = 300;
    const total = 900;
    const ids = try allocator.alloc(NodeId, total);
    defer allocator.free(ids);
    var payload: [300]u8 = undefined;
    @memset(&payload, 'x');

    // The image before the flush: a closed database.
    {
        var db = try Database.open(allocator, path, .{ .create = true, .config = config });
        defer db.close();
        for (ids[0..first], 0..) |*id, i| {
            var txn = try db.beginTransaction(.read_write);
            id.* = try db.createNode(&txn, &.{"Person"});
            try db.setNodeProperty(&txn, id.*, "seq", .{ .int_val = @intCast(i) });
            try db.setNodeProperty(&txn, id.*, "pad", .{ .string_val = &payload });
            try db.commitTransaction(&txn);
        }
    }
    try copyAsIfKilled(allocator, path, torn);

    // The image after it: the same database with more in it, closed again.
    {
        var db = try Database.open(allocator, path, .{ .config = config });
        defer db.close();
        for (ids[first..], first..) |*id, i| {
            var txn = try db.beginTransaction(.read_write);
            id.* = try db.createNode(&txn, &.{"Person"});
            try db.setNodeProperty(&txn, id.*, "seq", .{ .int_val = @intCast(i) });
            try db.setNodeProperty(&txn, id.*, "pad", .{ .string_val = &payload });
            try db.commitTransaction(&txn);
        }
    }

    // What a crash in the middle of the flush between the two leaves behind:
    // the journal complete, and only some of the pages written in place.
    const fs = @import("compat").fs;
    const after_file = try fs.cwd().openFile(path, .{});
    const after = try after_file.readToEndAlloc(allocator, 1 << 30);
    after_file.close();
    defer allocator.free(after);
    const page_size = 4096;
    const page_count = after.len / page_size;
    try std.testing.expect(page_count > 20);

    {
        var posix = lattice.storage.vfs.PosixVfs.init(allocator);
        const images = try allocator.alloc(journal_mod.PageImage, page_count);
        defer allocator.free(images);
        for (images, 0..) |*image, i| {
            image.* = .{ .page_id = @intCast(i), .data = after[i * page_size ..][0..page_size] };
        }
        var header: FileHeader = undefined;
        @memcpy(std.mem.asBytes(&header), after[0..@sizeOf(FileHeader)]);
        var journal = try journal_mod.Journal.open(allocator, posix.vfs(), torn_journal);
        defer journal.deinit();
        try journal.write(header.file_uuid, page_size, images);

        const torn_file = try fs.cwd().openFile(torn, .{ .mode = .read_write });
        defer torn_file.close();
        var i: usize = 0;
        while (i < page_count) : (i += 3) {
            try torn_file.pwriteAll(after[i * page_size ..][0..page_size], i * page_size);
        }
    }

    var db = try Database.open(allocator, torn, .{ .config = config });
    defer db.close();
    for (ids, 0..) |id, i| {
        try std.testing.expect(try db.nodeExists(id));
        var value = (try db.getNodeProperty(id, "seq")).?;
        defer value.deinit(allocator);
        try std.testing.expectEqual(@as(i64, @intCast(i)), value.int_val);
    }
    const people = try db.getNodesByLabel("Person");
    defer allocator.free(people);
    try std.testing.expectEqual(@as(usize, total), people.len);
}
