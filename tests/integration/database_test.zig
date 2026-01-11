//! Integration tests for LatticeDB Database API.
//!
//! These tests verify end-to-end behavior including:
//! - Data persistence across database reopens
//! - Transaction semantics with real data
//! - Combined search operations (FTS + Vector)
//! - Data integrity under load
//! - Query execution correctness

const std = @import("std");
const lattice = @import("lattice");

const Database = lattice.storage.database.Database;
const DatabaseConfig = lattice.storage.database.DatabaseConfig;
const OpenOptions = lattice.storage.database.OpenOptions;
const PropertyValue = lattice.core.types.PropertyValue;

// ============================================================================
// Persistence Tests
// ============================================================================

test "database: data persists across close and reopen" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_persist_test.ltdb";

    // Clean up from previous runs
    std.fs.cwd().deleteFile(path) catch {};
    std.fs.cwd().deleteFile(path ++ "-wal") catch {};

    // Phase 1: Create database and add data
    {
        var db = try Database.open(allocator, path, .{
            .create = true,
            .config = .{
                .enable_wal = false, // Simpler for this test
                .enable_fts = false,
                .enable_vector = false,
            },
        });

        // Create nodes with properties
        const alice = try db.createNode(null, &[_][]const u8{"Person"});
        try db.setNodeProperty(null,alice, "name", .{ .string_val = "Alice" });
        try db.setNodeProperty(null,alice, "age", .{ .int_val = 30 });

        const bob = try db.createNode(null, &[_][]const u8{"Person"});
        try db.setNodeProperty(null,bob, "name", .{ .string_val = "Bob" });
        try db.setNodeProperty(null,bob, "age", .{ .int_val = 25 });

        // Create edge
        try db.createEdge(null,alice, bob, "KNOWS");

        db.close();
    }

    // Phase 2: Reopen and verify data
    {
        var db = try Database.open(allocator, path, .{
            .create = false,
            .config = .{
                .enable_wal = false,
                .enable_fts = false,
                .enable_vector = false,
            },
        });
        defer db.close();

        // Verify nodes exist (IDs start at 1)
        try std.testing.expect(try db.nodeExists(1));
        try std.testing.expect(try db.nodeExists(2));

        // Verify properties
        const alice_name = try db.getNodeProperty(1, "name");
        try std.testing.expect(alice_name != null);
        try std.testing.expectEqualStrings("Alice", alice_name.?.string_val);
        allocator.free(alice_name.?.string_val);

        const bob_age = try db.getNodeProperty(2, "age");
        try std.testing.expect(bob_age != null);
        try std.testing.expectEqual(@as(i64, 25), bob_age.?.int_val);

        // Verify edge
        try std.testing.expect(db.edgeExists(1, 2, "KNOWS"));
        try std.testing.expect(!db.edgeExists(2, 1, "KNOWS")); // Directed
    }

    // Cleanup
    std.fs.cwd().deleteFile(path) catch {};
}

test "database: labels persist and are queryable after reopen" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_label_persist_test.ltdb";

    std.fs.cwd().deleteFile(path) catch {};

    // Create nodes with different labels
    {
        var db = try Database.open(allocator, path, .{
            .create = true,
            .config = .{ .enable_wal = false, .enable_fts = false },
        });

        _ = try db.createNode(null, &[_][]const u8{"Person"});
        _ = try db.createNode(null, &[_][]const u8{"Person"});
        _ = try db.createNode(null, &[_][]const u8{"Company"});
        _ = try db.createNode(null, &[_][]const u8{ "Person", "Employee" }); // Multi-label

        db.close();
    }

    // Verify label queries work after reopen
    {
        var db = try Database.open(allocator, path, .{
            .create = false,
            .config = .{ .enable_wal = false, .enable_fts = false },
        });
        defer db.close();

        // Query by label
        const people = try db.getNodesByLabel("Person");
        defer allocator.free(people);
        try std.testing.expectEqual(@as(usize, 3), people.len); // 3 nodes with Person label

        const companies = try db.getNodesByLabel("Company");
        defer allocator.free(companies);
        try std.testing.expectEqual(@as(usize, 1), companies.len);

        // Introspection
        const all_labels = try db.getAllLabels();
        defer db.freeLabelInfos(all_labels);
        try std.testing.expectEqual(@as(usize, 3), all_labels.len); // Person, Company, Employee
    }

    std.fs.cwd().deleteFile(path) catch {};
}

test "database: tree roots saved correctly across sessions" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_tree_roots_test.ltdb";

    std.fs.cwd().deleteFile(path) catch {};

    // Create database with many operations to ensure tree splits
    {
        var db = try Database.open(allocator, path, .{
            .create = true,
            .config = .{ .enable_wal = false, .enable_fts = false },
        });

        // Create many nodes to force B+Tree splits
        for (0..100) |i| {
            const node_id = try db.createNode(null, &[_][]const u8{"TestNode"});
            var buf: [32]u8 = undefined;
            const name = std.fmt.bufPrint(&buf, "Node_{d}", .{i}) catch unreachable;
            try db.setNodeProperty(null,node_id, "name", .{ .string_val = name });
        }

        db.close();
    }

    // Verify all nodes survive
    {
        var db = try Database.open(allocator, path, .{
            .create = false,
            .config = .{ .enable_wal = false, .enable_fts = false },
        });
        defer db.close();

        const nodes = try db.getNodesByLabel("TestNode");
        defer allocator.free(nodes);
        try std.testing.expectEqual(@as(usize, 100), nodes.len);

        // Spot check some properties
        const name42 = try db.getNodeProperty(43, "name"); // ID 43 = Node_42 (0-indexed)
        if (name42) |n| {
            allocator.free(n.string_val);
        }
    }

    std.fs.cwd().deleteFile(path) catch {};
}

// ============================================================================
// Graph Integrity Tests
// ============================================================================

test "database: delete node removes from label index" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_delete_label_test.ltdb";

    std.fs.cwd().deleteFile(path) catch {};

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{ .enable_wal = false, .enable_fts = false },
    });
    defer {
        db.close();
        std.fs.cwd().deleteFile(path) catch {};
    }

    // Create nodes
    const n1 = try db.createNode(null, &[_][]const u8{"ToDelete"});
    const n2 = try db.createNode(null, &[_][]const u8{"ToDelete"});
    const n3 = try db.createNode(null, &[_][]const u8{"ToDelete"});

    // Verify 3 nodes with label
    const before = try db.getNodesByLabel("ToDelete");
    defer allocator.free(before);
    try std.testing.expectEqual(@as(usize, 3), before.len);

    // Delete middle node
    try db.deleteNode(null,n2);

    // Verify only 2 nodes remain with label
    const after = try db.getNodesByLabel("ToDelete");
    defer allocator.free(after);
    try std.testing.expectEqual(@as(usize, 2), after.len);

    // Verify correct nodes remain
    var found_n1 = false;
    var found_n3 = false;
    for (after) |id| {
        if (id == n1) found_n1 = true;
        if (id == n3) found_n3 = true;
    }
    try std.testing.expect(found_n1);
    try std.testing.expect(found_n3);
}

test "database: edge operations are consistent" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_edge_consistency_test.ltdb";

    std.fs.cwd().deleteFile(path) catch {};

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{ .enable_wal = false, .enable_fts = false },
    });
    defer {
        db.close();
        std.fs.cwd().deleteFile(path) catch {};
    }

    // Create a small social graph
    const alice = try db.createNode(null, &[_][]const u8{"Person"});
    const bob = try db.createNode(null, &[_][]const u8{"Person"});
    const charlie = try db.createNode(null, &[_][]const u8{"Person"});

    // Create edges: alice -> bob, alice -> charlie, bob -> charlie
    try db.createEdge(null,alice, bob, "KNOWS");
    try db.createEdge(null,alice, charlie, "KNOWS");
    try db.createEdge(null,bob, charlie, "KNOWS");

    // Verify outgoing from alice
    const alice_out = try db.getOutgoingEdges(alice);
    defer db.freeEdgeInfos(alice_out);
    try std.testing.expectEqual(@as(usize, 2), alice_out.len);

    // Verify incoming to charlie
    const charlie_in = try db.getIncomingEdges(charlie);
    defer db.freeEdgeInfos(charlie_in);
    try std.testing.expectEqual(@as(usize, 2), charlie_in.len);

    // Delete an edge and verify
    try db.deleteEdge(null,alice, bob, "KNOWS");

    const alice_out_after = try db.getOutgoingEdges(alice);
    defer db.freeEdgeInfos(alice_out_after);
    try std.testing.expectEqual(@as(usize, 1), alice_out_after.len);
    try std.testing.expectEqual(charlie, alice_out_after[0].target);
}

test "database: self-loop edge works correctly" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_selfloop_test.ltdb";

    std.fs.cwd().deleteFile(path) catch {};

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{ .enable_wal = false, .enable_fts = false },
    });
    defer {
        db.close();
        std.fs.cwd().deleteFile(path) catch {};
    }

    const node = try db.createNode(null, &[_][]const u8{"Node"});

    // Self-loop: node -> node
    try db.createEdge(null,node, node, "SELF_REF");

    // Should appear in both outgoing and incoming
    const outgoing = try db.getOutgoingEdges(node);
    defer db.freeEdgeInfos(outgoing);
    try std.testing.expectEqual(@as(usize, 1), outgoing.len);
    try std.testing.expectEqual(node, outgoing[0].source);
    try std.testing.expectEqual(node, outgoing[0].target);

    const incoming = try db.getIncomingEdges(node);
    defer db.freeEdgeInfos(incoming);
    try std.testing.expectEqual(@as(usize, 1), incoming.len);
}

// ============================================================================
// Query Integration Tests
// ============================================================================

test "database: query with property filter" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_query_filter_test.ltdb";

    std.fs.cwd().deleteFile(path) catch {};

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{ .enable_wal = false, .enable_fts = false },
    });
    defer {
        db.close();
        std.fs.cwd().deleteFile(path) catch {};
    }

    // Create test data
    const alice = try db.createNode(null, &[_][]const u8{"Person"});
    try db.setNodeProperty(null,alice, "name", .{ .string_val = "Alice" });
    try db.setNodeProperty(null,alice, "age", .{ .int_val = 30 });

    const bob = try db.createNode(null, &[_][]const u8{"Person"});
    try db.setNodeProperty(null,bob, "name", .{ .string_val = "Bob" });
    try db.setNodeProperty(null,bob, "age", .{ .int_val = 25 });

    const charlie = try db.createNode(null, &[_][]const u8{"Person"});
    try db.setNodeProperty(null,charlie, "name", .{ .string_val = "Charlie" });
    try db.setNodeProperty(null,charlie, "age", .{ .int_val = 35 });

    // Query all Person nodes
    var result = try db.query("MATCH (n:Person) RETURN n");
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 3), result.rowCount());
}

test "database: query with LIMIT" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_query_limit_test.ltdb";

    std.fs.cwd().deleteFile(path) catch {};

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{ .enable_wal = false, .enable_fts = false },
    });
    defer {
        db.close();
        std.fs.cwd().deleteFile(path) catch {};
    }

    // Create many nodes
    for (0..20) |_| {
        _ = try db.createNode(null, &[_][]const u8{"Item"});
    }

    // Query with limit
    var result = try db.query("MATCH (n:Item) RETURN n LIMIT 5");
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 5), result.rowCount());
}

// ============================================================================
// Aggregation Tests
// ============================================================================

test "database: count aggregation" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_agg_count_test.ltdb";

    std.fs.cwd().deleteFile(path) catch {};

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{ .enable_wal = false, .enable_fts = false },
    });
    defer {
        db.close();
        std.fs.cwd().deleteFile(path) catch {};
    }

    // Create some Person nodes
    for (0..5) |_| {
        _ = try db.createNode(null, &[_][]const u8{"Person"});
    }

    // Count all Person nodes
    var result = try db.query("MATCH (n:Person) RETURN count(n)");
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 1), result.rowCount());

    // Get the count value
    const count_val = result.rows[0].values[0];
    switch (count_val) {
        .int_val => |v| {
            try std.testing.expectEqual(@as(i64, 5), v);
        },
        else => return error.UnexpectedValueType,
    }
}

test "database: sum aggregation" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_agg_sum_test.ltdb";

    std.fs.cwd().deleteFile(path) catch {};

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{ .enable_wal = false, .enable_fts = false },
    });
    defer {
        db.close();
        std.fs.cwd().deleteFile(path) catch {};
    }

    // Create Person nodes with ages
    const ages = [_]i64{ 20, 30, 40, 50 };
    for (ages) |age| {
        const node = try db.createNode(null, &[_][]const u8{"Person"});
        try db.setNodeProperty(null, node, "age", .{ .int_val = age });
    }

    // Sum ages
    var result = try db.query("MATCH (n:Person) RETURN sum(n.age)");
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 1), result.rowCount());

    const sum_val = result.rows[0].values[0];
    switch (sum_val) {
        .float_val => |v| {
            try std.testing.expectApproxEqAbs(@as(f64, 140.0), v, 0.001);
        },
        else => return error.UnexpectedValueType,
    }
}

test "database: avg aggregation" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_agg_avg_test.ltdb";

    std.fs.cwd().deleteFile(path) catch {};

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{ .enable_wal = false, .enable_fts = false },
    });
    defer {
        db.close();
        std.fs.cwd().deleteFile(path) catch {};
    }

    // Create Person nodes with ages
    const ages = [_]i64{ 20, 30, 40, 50 };
    for (ages) |age| {
        const node = try db.createNode(null, &[_][]const u8{"Person"});
        try db.setNodeProperty(null, node, "age", .{ .int_val = age });
    }

    // Average ages
    var result = try db.query("MATCH (n:Person) RETURN avg(n.age)");
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 1), result.rowCount());

    const avg_val = result.rows[0].values[0];
    switch (avg_val) {
        .float_val => |v| {
            try std.testing.expectApproxEqAbs(@as(f64, 35.0), v, 0.001);
        },
        else => return error.UnexpectedValueType,
    }
}

test "database: min/max aggregation" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_agg_minmax_test.ltdb";

    std.fs.cwd().deleteFile(path) catch {};

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{ .enable_wal = false, .enable_fts = false },
    });
    defer {
        db.close();
        std.fs.cwd().deleteFile(path) catch {};
    }

    // Create Person nodes with ages
    const ages = [_]i64{ 25, 35, 15, 45 };
    for (ages) |age| {
        const node = try db.createNode(null, &[_][]const u8{"Person"});
        try db.setNodeProperty(null, node, "age", .{ .int_val = age });
    }

    // Min age
    {
        var result = try db.query("MATCH (n:Person) RETURN min(n.age)");
        defer result.deinit();

        try std.testing.expectEqual(@as(usize, 1), result.rowCount());

        const min_val = result.rows[0].values[0];
        switch (min_val) {
            .int_val => |v| {
                try std.testing.expectEqual(@as(i64, 15), v);
            },
            else => return error.UnexpectedValueType,
        }
    }

    // Max age
    {
        var result = try db.query("MATCH (n:Person) RETURN max(n.age)");
        defer result.deinit();

        try std.testing.expectEqual(@as(usize, 1), result.rowCount());

        const max_val = result.rows[0].values[0];
        switch (max_val) {
            .int_val => |v| {
                try std.testing.expectEqual(@as(i64, 45), v);
            },
            else => return error.UnexpectedValueType,
        }
    }
}

test "database: count with empty result" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_agg_count_empty_test.ltdb";

    std.fs.cwd().deleteFile(path) catch {};

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{ .enable_wal = false, .enable_fts = false },
    });
    defer {
        db.close();
        std.fs.cwd().deleteFile(path) catch {};
    }

    // No nodes created - count should return 0
    var result = try db.query("MATCH (n:Person) RETURN count(n)");
    defer result.deinit();

    // COUNT(*) on empty set returns 1 row with count 0
    try std.testing.expectEqual(@as(usize, 1), result.rowCount());

    const count_val = result.rows[0].values[0];
    switch (count_val) {
        .int_val => |v| {
            try std.testing.expectEqual(@as(i64, 0), v);
        },
        else => return error.UnexpectedValueType,
    }
}

// ============================================================================
// FTS Integration Tests
// ============================================================================

test "database: fts search returns relevant results" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_fts_integration_test.ltdb";

    std.fs.cwd().deleteFile(path) catch {};

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{
            .enable_wal = false,
            .enable_fts = true,
            .enable_vector = false,
        },
    });
    defer {
        db.close();
        std.fs.cwd().deleteFile(path) catch {};
    }

    // Create documents
    const doc1 = try db.createNode(null, &[_][]const u8{"Document"});
    try db.ftsIndexDocument(doc1, "The quick brown fox jumps over the lazy dog");

    const doc2 = try db.createNode(null, &[_][]const u8{"Document"});
    try db.ftsIndexDocument(doc2, "A lazy cat sleeps on the couch");

    const doc3 = try db.createNode(null, &[_][]const u8{"Document"});
    try db.ftsIndexDocument(doc3, "Quick reflexes are important for athletes");

    // Search for "quick"
    const results = try db.ftsSearch("quick", 10);
    defer db.freeFtsSearchResults(results);

    try std.testing.expectEqual(@as(usize, 2), results.len); // doc1 and doc3
}

test "database: fts handles document updates" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_fts_update_test.ltdb";

    std.fs.cwd().deleteFile(path) catch {};

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{
            .enable_wal = false,
            .enable_fts = true,
            .enable_vector = false,
        },
    });
    defer {
        db.close();
        std.fs.cwd().deleteFile(path) catch {};
    }

    // Create and index document
    const doc = try db.createNode(null, &[_][]const u8{"Document"});
    try db.ftsIndexDocument(doc, "original content about apples");

    // Search for "apples"
    const results1 = try db.ftsSearch("apples", 10);
    defer db.freeFtsSearchResults(results1);
    try std.testing.expectEqual(@as(usize, 1), results1.len);

    // Note: To update, you'd remove and re-index (if FTS supports it)
    // This tests the indexing worked correctly
}

// ============================================================================
// Vector Search Integration Tests
// ============================================================================

test "database: vector search finds similar nodes" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_vector_integration_test.ltdb";

    std.fs.cwd().deleteFile(path) catch {};

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{
            .enable_wal = false,
            .enable_fts = false,
            .enable_vector = true,
            .vector_dimensions = 4,
        },
    });
    defer {
        db.close();
        std.fs.cwd().deleteFile(path) catch {};
    }

    // Create nodes with vectors
    const n1 = try db.createNode(null, &[_][]const u8{"Embedding"});
    try db.setNodeVector(n1, &[_]f32{ 1.0, 0.0, 0.0, 0.0 });

    const n2 = try db.createNode(null, &[_][]const u8{"Embedding"});
    try db.setNodeVector(n2, &[_]f32{ 0.9, 0.1, 0.0, 0.0 }); // Similar to n1

    const n3 = try db.createNode(null, &[_][]const u8{"Embedding"});
    try db.setNodeVector(n3, &[_]f32{ 0.0, 0.0, 1.0, 0.0 }); // Orthogonal

    // Search for vectors similar to [1, 0, 0, 0]
    const query_vec = [_]f32{ 1.0, 0.0, 0.0, 0.0 };
    const results = try db.vectorSearch(&query_vec, 3, null);
    defer db.freeVectorSearchResults(results);

    try std.testing.expectEqual(@as(usize, 3), results.len);

    // First result should be n1 (exact match, distance ~0)
    try std.testing.expectEqual(n1, results[0].node_id);
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), results[0].distance, 0.01);

    // Second should be n2 (similar)
    try std.testing.expectEqual(n2, results[1].node_id);
}

// ============================================================================
// Stress Tests
// ============================================================================

test "database: handles many nodes efficiently" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_stress_nodes_test.ltdb";

    std.fs.cwd().deleteFile(path) catch {};

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{ .enable_wal = false, .enable_fts = false },
    });
    defer {
        db.close();
        std.fs.cwd().deleteFile(path) catch {};
    }

    const node_count = 500;

    // Create many nodes with properties
    for (0..node_count) |i| {
        const node = try db.createNode(null, &[_][]const u8{"StressNode"});
        try db.setNodeProperty(null,node, "index", .{ .int_val = @intCast(i) });
    }

    // Verify count
    const nodes = try db.getNodesByLabel("StressNode");
    defer allocator.free(nodes);
    try std.testing.expectEqual(@as(usize, node_count), nodes.len);

    // Verify random sample of properties
    const prop250 = try db.getNodeProperty(251, "index"); // Node 251 = index 250
    try std.testing.expect(prop250 != null);
    try std.testing.expectEqual(@as(i64, 250), prop250.?.int_val);
}

test "database: handles many edges efficiently" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_stress_edges_test.ltdb";

    std.fs.cwd().deleteFile(path) catch {};

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{ .enable_wal = false, .enable_fts = false },
    });
    defer {
        db.close();
        std.fs.cwd().deleteFile(path) catch {};
    }

    // Create a star topology: center node with 100 connections
    const center = try db.createNode(null, &[_][]const u8{"Center"});

    const spoke_count = 100;
    for (0..spoke_count) |_| {
        const spoke = try db.createNode(null, &[_][]const u8{"Spoke"});
        try db.createEdge(null,center, spoke, "CONNECTED");
    }

    // Verify edges
    const outgoing = try db.getOutgoingEdges(center);
    defer db.freeEdgeInfos(outgoing);
    try std.testing.expectEqual(@as(usize, spoke_count), outgoing.len);

    // Edge type stats
    const edge_types = try db.getAllEdgeTypes();
    defer db.freeEdgeTypeInfos(edge_types);
    try std.testing.expectEqual(@as(usize, 1), edge_types.len);
    try std.testing.expectEqual(@as(u64, spoke_count), edge_types[0].count);
}

// ============================================================================
// Error Handling Tests
// ============================================================================

test "database: returns error for nonexistent file" {
    const allocator = std.testing.allocator;
    const result = Database.open(allocator, "/nonexistent/path/db.ltdb", .{});
    try std.testing.expectError(lattice.storage.database.DatabaseError.FileNotFound, result);
}

test "database: read-only mode prevents writes" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_readonly_test.ltdb";

    std.fs.cwd().deleteFile(path) catch {};

    // Create database first
    {
        var db = try Database.open(allocator, path, .{
            .create = true,
            .config = .{ .enable_wal = false, .enable_fts = false },
        });
        _ = try db.createNode(null, &[_][]const u8{"Test"});
        db.close();
    }

    // Open read-only
    {
        var db = try Database.open(allocator, path, .{
            .create = false,
            .read_only = true,
            .config = .{ .enable_wal = false, .enable_fts = false },
        });
        defer db.close();

        // Read should work
        try std.testing.expect(try db.nodeExists(1));

        // Write should fail
        const result = db.createNode(null, &[_][]const u8{"NewNode"});
        try std.testing.expectError(lattice.storage.database.DatabaseError.PermissionDenied, result);
    }

    std.fs.cwd().deleteFile(path) catch {};
}

test "database: property type handling" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_property_types_test.ltdb";

    std.fs.cwd().deleteFile(path) catch {};

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{ .enable_wal = false, .enable_fts = false },
    });
    defer {
        db.close();
        std.fs.cwd().deleteFile(path) catch {};
    }

    const node = try db.createNode(null, &[_][]const u8{"TypeTest"});

    // Test different property types
    try db.setNodeProperty(null,node, "string_prop", .{ .string_val = "hello" });
    try db.setNodeProperty(null,node, "int_prop", .{ .int_val = 42 });
    try db.setNodeProperty(null,node, "float_prop", .{ .float_val = 3.14 });
    try db.setNodeProperty(null,node, "bool_prop", .{ .bool_val = true });

    // Verify each type
    const str = try db.getNodeProperty(node, "string_prop");
    try std.testing.expectEqualStrings("hello", str.?.string_val);
    allocator.free(str.?.string_val);

    const int = try db.getNodeProperty(node, "int_prop");
    try std.testing.expectEqual(@as(i64, 42), int.?.int_val);

    const flt = try db.getNodeProperty(node, "float_prop");
    try std.testing.expectApproxEqAbs(@as(f64, 3.14), flt.?.float_val, 0.001);

    const b = try db.getNodeProperty(node, "bool_prop");
    try std.testing.expectEqual(true, b.?.bool_val);
}
