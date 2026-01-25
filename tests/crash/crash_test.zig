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
