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
    const file = try std.fs.cwd().openFile(path, .{ .mode = .read_write });
    defer file.close();

    // Read the full header page
    var header_buf: [4096]u8 = undefined;
    const n = try file.preadAll(&header_buf, 0);
    if (n < @sizeOf(FileHeader)) return error.HeaderTooSmall;

    // Get a mutable pointer to the header within the buffer
    const header: *FileHeader = @ptrCast(@alignCast(&header_buf));

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
    try file.pwriteAll(&header_buf, 0);

    // Truncate file to just the header page (4096 bytes)
    try file.setEndPos(4096);
}

/// Clean up database and WAL files.
fn cleanup(path: []const u8) void {
    std.fs.cwd().deleteFile(path) catch {};
    // Delete WAL file (path + "-wal")
    var wal_buf: [256]u8 = undefined;
    const wal_path = std.fmt.bufPrint(&wal_buf, "{s}-wal", .{path}) catch return;
    std.fs.cwd().deleteFile(wal_path) catch {};
}

/// Open a database with WAL enabled for crash testing.
fn openCrashTestDb(allocator: std.mem.Allocator, path: []const u8, create: bool) !*Database {
    return Database.open(allocator, path, .{
        .create = create,
        .config = .{
            .enable_wal = true,
            .enable_fts = false,
            .enable_vector = false,
        },
    });
}

// ============================================================================
// Tests
// ============================================================================

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

        // Test getNodeProperty - this interns the key and looks up the property
        // If symbol table wasn't recovered, this wouldn't work
        const name_val = try db.getNodeProperty(node_id, "name");
        try std.testing.expect(name_val != null);
        try std.testing.expectEqualStrings("Alice", name_val.?.string_val);
        allocator.free(name_val.?.string_val);

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
