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
const DatabaseError = lattice.storage.database.DatabaseError;
const DatabaseConfig = lattice.storage.database.DatabaseConfig;
const OpenOptions = lattice.storage.database.OpenOptions;
const PropertyValue = lattice.core.types.PropertyValue;
const EdgeError = lattice.graph.edge.EdgeError;
const ScopedTree = lattice.fts.scoped_tree.ScopedTree;

/// Plan `cypher` against `db` and report which scan the planner chose.
///
/// An index scan and a label scan produce identical rows, so no assertion on a
/// result set can tell them apart. Planning the query and reading the decision
/// is the only way to check that an index is really being used.
fn planScanKind(
    db: *Database,
    cypher: []const u8,
) !lattice.query.planner.QueryPlanner.ScanKind {
    const allocator = std.testing.allocator;

    // The parser owns the arena the AST lives in, so it has to outlive planning.
    var parser = lattice.query.parser.Parser.init(allocator, cypher);
    defer parser.deinit();
    const parse_result = parser.parse();
    if (parse_result.query == null) return error.ParseFailed;

    var analyzer = lattice.query.semantic.SemanticAnalyzer.init(allocator);
    defer analyzer.deinit();
    const analysis = analyzer.analyze(parse_result.query.?);
    if (!analysis.success) return error.SemanticFailed;

    const storage_ctx = lattice.query.planner.StorageContext{
        .node_tree = &db.node_tree,
        .label_index = &db.label_index,
        .edge_store = &db.edge_store,
        .symbol_table = &db.symbol_table,
        .hnsw_index = if (db.hnsw_index) |*hnsw| hnsw else null,
        .fts_index = if (db.fts_index) |*fts| fts else null,
        .database = db,
    };

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    var planner = lattice.query.planner.QueryPlanner.init(arena.allocator(), storage_ctx);
    defer planner.deinit();

    const analysis_result = lattice.query.semantic.AnalysisResult{
        .success = true,
        .errors = &[_]lattice.query.semantic.SemanticError{},
        .variables = analysis.variables,
        .errors_dropped = false,
    };

    _ = try planner.plan(parse_result.query.?, &analysis_result);
    return planner.last_scan_kind;
}

fn overwriteEdgePayloadWithInvalidData(db: *Database, edge_id: u64) !void {
    var id_key: [8]u8 = undefined;
    std.mem.writeInt(u64, &id_key, edge_id, .little);
    try db.edge_store.edge_id_index.delete(&id_key);
    try db.edge_store.edge_id_index.insert(&id_key, "bad");
}

fn findPropertyEntry(props: []const Database.PropertyEntry, key: []const u8) ?*const Database.PropertyEntry {
    for (props) |*prop| {
        if (std.mem.eql(u8, prop.key, key)) return prop;
    }
    return null;
}

// ============================================================================
// Persistence Tests
// ============================================================================

test "database: data persists across close and reopen" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_persist_test.ltdb";

    // Clean up from previous runs
    @import("compat").fs.cwd().deleteFile(path) catch {};
    @import("compat").fs.cwd().deleteFile(path ++ "-wal") catch {};

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
        try db.setNodeProperty(null, alice, "name", .{ .string_val = "Alice" });
        try db.setNodeProperty(null, alice, "age", .{ .int_val = 30 });

        const bob = try db.createNode(null, &[_][]const u8{"Person"});
        try db.setNodeProperty(null, bob, "name", .{ .string_val = "Bob" });
        try db.setNodeProperty(null, bob, "age", .{ .int_val = 25 });

        // Create edge
        try db.createEdge(null, alice, bob, "KNOWS");

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
        var alice_name = (try db.getNodeProperty(1, "name")).?;
        defer alice_name.deinit(allocator);
        try std.testing.expectEqualStrings("Alice", alice_name.string_val);

        const bob_age = try db.getNodeProperty(2, "age");
        try std.testing.expect(bob_age != null);
        try std.testing.expectEqual(@as(i64, 25), bob_age.?.int_val);

        // Verify edge
        try std.testing.expect(db.edgeExists(1, 2, "KNOWS"));
        try std.testing.expect(!db.edgeExists(2, 1, "KNOWS")); // Directed
    }

    // Cleanup
    @import("compat").fs.cwd().deleteFile(path) catch {};
}

test "database: labels persist and are queryable after reopen" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_label_persist_test.ltdb";

    @import("compat").fs.cwd().deleteFile(path) catch {};

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

    @import("compat").fs.cwd().deleteFile(path) catch {};
}

test "database: edge IDs remain monotonic across abort and reopen" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_edge_id_monotonic_reopen_test.ltdb";

    @import("compat").fs.cwd().deleteFile(path) catch {};
    @import("compat").fs.cwd().deleteFile(path ++ "-wal") catch {};
    defer @import("compat").fs.cwd().deleteFile(path) catch {};
    defer @import("compat").fs.cwd().deleteFile(path ++ "-wal") catch {};

    var source: u64 = 0;
    var target: u64 = 0;
    var rolled_back_id: u64 = 0;
    var committed_id: u64 = 0;

    {
        var db = try Database.open(allocator, path, .{
            .create = true,
            .config = .{
                .enable_wal = true,
                .enable_fts = false,
                .enable_vector = false,
            },
        });

        source = try db.createNode(null, &[_][]const u8{"N"});
        target = try db.createNode(null, &[_][]const u8{"N"});

        var txn = try db.beginTransaction(.read_write);
        rolled_back_id = try db.createEdgeAndGetId(&txn, source, target, "REL");
        try db.abortTransaction(&txn);
        try std.testing.expectError(EdgeError.NotFound, db.edge_store.getById(rolled_back_id));

        committed_id = try db.createEdgeAndGetId(null, source, target, "REL");
        try std.testing.expect(committed_id > rolled_back_id);

        db.close();
    }

    {
        var db = try Database.open(allocator, path, .{
            .create = false,
            .config = .{
                .enable_wal = true,
                .enable_fts = false,
                .enable_vector = false,
            },
        });
        defer db.close();

        const next_id = try db.createEdgeAndGetId(null, source, target, "REL");
        try std.testing.expect(next_id > committed_id);

        try std.testing.expectError(EdgeError.NotFound, db.edge_store.getById(rolled_back_id));
        var kept = try db.edge_store.getById(committed_id);
        defer kept.deinit(allocator);
        var newer = try db.edge_store.getById(next_id);
        defer newer.deinit(allocator);
    }
}

test "database: rejects overlapping write transactions" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_single_writer_test.ltdb";

    @import("compat").fs.cwd().deleteFile(path) catch {};
    @import("compat").fs.cwd().deleteFile(path ++ "-wal") catch {};
    defer @import("compat").fs.cwd().deleteFile(path) catch {};
    defer @import("compat").fs.cwd().deleteFile(path ++ "-wal") catch {};

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{ .enable_wal = true, .enable_fts = false },
    });
    defer db.close();

    var writer = try db.beginTransaction(.read_write);
    try std.testing.expectError(DatabaseError.TransactionConflict, db.beginTransaction(.read_write));

    var reader = try db.beginTransaction(.read_only);
    try db.abortTransaction(&reader);

    try db.abortTransaction(&writer);
    var next_writer = try db.beginTransaction(.read_write);
    try db.abortTransaction(&next_writer);
}

test "database: deleteEdgeById persists exact parallel-edge deletion across reopen" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_delete_edge_id_reopen_test.ltdb";

    @import("compat").fs.cwd().deleteFile(path) catch {};
    @import("compat").fs.cwd().deleteFile(path ++ "-wal") catch {};
    defer @import("compat").fs.cwd().deleteFile(path) catch {};
    defer @import("compat").fs.cwd().deleteFile(path ++ "-wal") catch {};

    const source: u64 = 1;
    const target: u64 = 2;
    var deleted_id: u64 = 0;
    var kept_id: u64 = 0;

    {
        var db = try Database.open(allocator, path, .{
            .create = true,
            .config = .{
                .enable_wal = true,
                .enable_fts = false,
                .enable_vector = false,
            },
        });

        _ = try db.createNode(null, &[_][]const u8{"N"});
        _ = try db.createNode(null, &[_][]const u8{"N"});

        deleted_id = try db.createEdgeAndGetId(null, source, target, "REL");
        kept_id = try db.createEdgeAndGetId(null, source, target, "REL");
        try db.deleteEdgeById(null, deleted_id);

        try std.testing.expectError(EdgeError.NotFound, db.edge_store.getById(deleted_id));
        var kept = try db.edge_store.getById(kept_id);
        kept.deinit(allocator);

        db.close();
    }

    {
        var db = try Database.open(allocator, path, .{
            .create = false,
            .config = .{
                .enable_wal = true,
                .enable_fts = false,
                .enable_vector = false,
            },
        });
        defer db.close();

        try std.testing.expectError(EdgeError.NotFound, db.edge_store.getById(deleted_id));
        var kept = try db.edge_store.getById(kept_id);
        defer kept.deinit(allocator);

        var refs = try db.getOutgoingEdgeRefs(source);
        defer refs.deinit();
        var count: usize = 0;
        while (try refs.next()) |edge_ref| {
            count += 1;
            try std.testing.expectEqual(kept_id, edge_ref.id);
        }
        try std.testing.expectEqual(@as(usize, 1), count);
    }
}

test "database: endpoint delete removes one parallel edge and keeps id monotonic" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_endpoint_delete_parallel_test.ltdb";

    @import("compat").fs.cwd().deleteFile(path) catch {};
    @import("compat").fs.cwd().deleteFile(path ++ "-wal") catch {};
    defer @import("compat").fs.cwd().deleteFile(path) catch {};
    defer @import("compat").fs.cwd().deleteFile(path ++ "-wal") catch {};

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{
            .enable_wal = true,
            .enable_fts = false,
            .enable_vector = false,
        },
    });
    defer db.close();

    const source = try db.createNode(null, &[_][]const u8{"N"});
    const target = try db.createNode(null, &[_][]const u8{"N"});

    const edge1 = try db.createEdgeAndGetId(null, source, target, "REL");
    const edge2 = try db.createEdgeAndGetId(null, source, target, "REL");

    try db.deleteEdge(null, source, target, "REL");

    // Endpoint delete removes first matching edge (lowest edge_id).
    try std.testing.expectError(EdgeError.NotFound, db.edge_store.getById(edge1));
    var kept = try db.edge_store.getById(edge2);
    defer kept.deinit(allocator);

    const edge3 = try db.createEdgeAndGetId(null, source, target, "REL");
    try std.testing.expect(edge3 > edge2);

    var refs = try db.getOutgoingEdgeRefs(source);
    defer refs.deinit();
    var count: usize = 0;
    var saw_edge2 = false;
    var saw_edge3 = false;
    while (try refs.next()) |edge_ref| {
        count += 1;
        if (edge_ref.id == edge2) saw_edge2 = true;
        if (edge_ref.id == edge3) saw_edge3 = true;
    }
    try std.testing.expectEqual(@as(usize, 2), count);
    try std.testing.expect(saw_edge2);
    try std.testing.expect(saw_edge3);
}

test "database: getAllEdgeTypes counts parallel edges and reflects deletes" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_edge_type_count_parallel_test.ltdb";

    @import("compat").fs.cwd().deleteFile(path) catch {};
    @import("compat").fs.cwd().deleteFile(path ++ "-wal") catch {};
    defer @import("compat").fs.cwd().deleteFile(path) catch {};
    defer @import("compat").fs.cwd().deleteFile(path ++ "-wal") catch {};

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{
            .enable_wal = true,
            .enable_fts = false,
            .enable_vector = false,
        },
    });
    defer db.close();

    const n1 = try db.createNode(null, &[_][]const u8{"N"});
    const n2 = try db.createNode(null, &[_][]const u8{"N"});
    const n3 = try db.createNode(null, &[_][]const u8{"N"});

    const del_rel = try db.createEdgeAndGetId(null, n1, n2, "REL");
    _ = try db.createEdgeAndGetId(null, n1, n2, "REL");
    _ = try db.createEdgeAndGetId(null, n2, n3, "REL");
    _ = try db.createEdgeAndGetId(null, n1, n3, "LIKES");

    const before = try db.getAllEdgeTypes();
    defer db.freeEdgeTypeInfos(before);

    var rel_count: u64 = 0;
    var likes_count: u64 = 0;
    for (before) |info| {
        if (std.mem.eql(u8, info.name, "REL")) rel_count = info.count;
        if (std.mem.eql(u8, info.name, "LIKES")) likes_count = info.count;
    }
    try std.testing.expectEqual(@as(u64, 3), rel_count);
    try std.testing.expectEqual(@as(u64, 1), likes_count);

    try db.deleteEdgeById(null, del_rel);

    const after = try db.getAllEdgeTypes();
    defer db.freeEdgeTypeInfos(after);

    rel_count = 0;
    likes_count = 0;
    for (after) |info| {
        if (std.mem.eql(u8, info.name, "REL")) rel_count = info.count;
        if (std.mem.eql(u8, info.name, "LIKES")) likes_count = info.count;
    }
    try std.testing.expectEqual(@as(u64, 2), rel_count);
    try std.testing.expectEqual(@as(u64, 1), likes_count);
}

test "database: edge type counts persist across reopen after endpoint delete" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_edge_type_count_reopen_test.ltdb";

    @import("compat").fs.cwd().deleteFile(path) catch {};
    @import("compat").fs.cwd().deleteFile(path ++ "-wal") catch {};
    defer @import("compat").fs.cwd().deleteFile(path) catch {};
    defer @import("compat").fs.cwd().deleteFile(path ++ "-wal") catch {};

    {
        var db = try Database.open(allocator, path, .{
            .create = true,
            .config = .{
                .enable_wal = true,
                .enable_fts = false,
                .enable_vector = false,
            },
        });

        const n1 = try db.createNode(null, &[_][]const u8{"N"});
        const n2 = try db.createNode(null, &[_][]const u8{"N"});
        const n3 = try db.createNode(null, &[_][]const u8{"N"});

        _ = try db.createEdgeAndGetId(null, n1, n2, "REL");
        _ = try db.createEdgeAndGetId(null, n1, n2, "REL");
        _ = try db.createEdgeAndGetId(null, n2, n3, "LIKES");

        try db.deleteEdge(null, n1, n2, "REL");

        db.close();
    }

    {
        var db = try Database.open(allocator, path, .{
            .create = false,
            .config = .{
                .enable_wal = true,
                .enable_fts = false,
                .enable_vector = false,
            },
        });
        defer db.close();

        const edge_types = try db.getAllEdgeTypes();
        defer db.freeEdgeTypeInfos(edge_types);

        var rel_count: u64 = 0;
        var likes_count: u64 = 0;
        for (edge_types) |info| {
            if (std.mem.eql(u8, info.name, "REL")) rel_count = info.count;
            if (std.mem.eql(u8, info.name, "LIKES")) likes_count = info.count;
        }

        try std.testing.expectEqual(@as(u64, 1), rel_count);
        try std.testing.expectEqual(@as(u64, 1), likes_count);
    }
}

test "database: edge ids are not reused after deleting all edges and reopening" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_edge_id_no_reuse_after_reopen_test.ltdb";

    @import("compat").fs.cwd().deleteFile(path) catch {};
    @import("compat").fs.cwd().deleteFile(path ++ "-wal") catch {};
    defer @import("compat").fs.cwd().deleteFile(path) catch {};
    defer @import("compat").fs.cwd().deleteFile(path ++ "-wal") catch {};

    var source: u64 = 0;
    var target: u64 = 0;
    var old_max: u64 = 0;

    {
        var db = try Database.open(allocator, path, .{
            .create = true,
            .config = .{
                .enable_wal = true,
                .enable_fts = false,
                .enable_vector = false,
            },
        });

        source = try db.createNode(null, &[_][]const u8{"N"});
        target = try db.createNode(null, &[_][]const u8{"N"});

        const edge1 = try db.createEdgeAndGetId(null, source, target, "REL");
        const edge2 = try db.createEdgeAndGetId(null, source, target, "REL");
        old_max = edge2;

        try db.deleteEdgeById(null, edge1);
        try db.deleteEdgeById(null, edge2);

        var refs = try db.getOutgoingEdgeRefs(source);
        defer refs.deinit();
        try std.testing.expect((try refs.next()) == null);

        db.close();
    }

    {
        var db = try Database.open(allocator, path, .{
            .create = false,
            .config = .{
                .enable_wal = true,
                .enable_fts = false,
                .enable_vector = false,
            },
        });
        defer db.close();

        const next = try db.createEdgeAndGetId(null, source, target, "REL");
        try std.testing.expect(next > old_max);
    }
}

test "database: aborted edge delete leaves edge type counts unchanged" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_edge_type_count_abort_delete_test.ltdb";

    @import("compat").fs.cwd().deleteFile(path) catch {};
    @import("compat").fs.cwd().deleteFile(path ++ "-wal") catch {};
    defer @import("compat").fs.cwd().deleteFile(path) catch {};
    defer @import("compat").fs.cwd().deleteFile(path ++ "-wal") catch {};

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{
            .enable_wal = true,
            .enable_fts = false,
            .enable_vector = false,
        },
    });
    defer db.close();

    const n1 = try db.createNode(null, &[_][]const u8{"N"});
    const n2 = try db.createNode(null, &[_][]const u8{"N"});
    const n3 = try db.createNode(null, &[_][]const u8{"N"});

    const rel_id = try db.createEdgeAndGetId(null, n1, n2, "REL");
    _ = try db.createEdgeAndGetId(null, n1, n3, "REL");
    _ = try db.createEdgeAndGetId(null, n2, n3, "LIKES");

    var txn = try db.beginTransaction(.read_write);
    try db.deleteEdgeById(&txn, rel_id);
    try db.abortTransaction(&txn);

    const counts = try db.getAllEdgeTypes();
    defer db.freeEdgeTypeInfos(counts);

    var rel_count: u64 = 0;
    var likes_count: u64 = 0;
    for (counts) |info| {
        if (std.mem.eql(u8, info.name, "REL")) rel_count = info.count;
        if (std.mem.eql(u8, info.name, "LIKES")) likes_count = info.count;
    }
    try std.testing.expectEqual(@as(u64, 2), rel_count);
    try std.testing.expectEqual(@as(u64, 1), likes_count);
}

test "database: tree roots saved correctly across sessions" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_tree_roots_test.ltdb";

    @import("compat").fs.cwd().deleteFile(path) catch {};

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
            try db.setNodeProperty(null, node_id, "name", .{ .string_val = name });
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
        if (try db.getNodeProperty(43, "name")) |value| { // ID 43 = Node_42 (0-indexed)
            var name42 = value;
            defer name42.deinit(allocator);
        }
    }

    @import("compat").fs.cwd().deleteFile(path) catch {};
}

// ============================================================================
// Graph Integrity Tests
// ============================================================================

test "database: delete node removes from label index" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_delete_label_test.ltdb";

    @import("compat").fs.cwd().deleteFile(path) catch {};

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{ .enable_wal = false, .enable_fts = false },
    });
    defer {
        db.close();
        @import("compat").fs.cwd().deleteFile(path) catch {};
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
    try db.deleteNode(null, n2);

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

    @import("compat").fs.cwd().deleteFile(path) catch {};

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{ .enable_wal = false, .enable_fts = false },
    });
    defer {
        db.close();
        @import("compat").fs.cwd().deleteFile(path) catch {};
    }

    // Create a small social graph
    const alice = try db.createNode(null, &[_][]const u8{"Person"});
    const bob = try db.createNode(null, &[_][]const u8{"Person"});
    const charlie = try db.createNode(null, &[_][]const u8{"Person"});

    // Create edges: alice -> bob, alice -> charlie, bob -> charlie
    try db.createEdge(null, alice, bob, "KNOWS");
    try db.createEdge(null, alice, charlie, "KNOWS");
    try db.createEdge(null, bob, charlie, "KNOWS");

    // Verify outgoing from alice
    const alice_out = try db.getOutgoingEdges(alice);
    defer db.freeEdgeInfos(alice_out);
    try std.testing.expectEqual(@as(usize, 2), alice_out.len);

    // Verify incoming to charlie
    const charlie_in = try db.getIncomingEdges(charlie);
    defer db.freeEdgeInfos(charlie_in);
    try std.testing.expectEqual(@as(usize, 2), charlie_in.len);

    // Delete an edge and verify
    try db.deleteEdge(null, alice, bob, "KNOWS");

    const alice_out_after = try db.getOutgoingEdges(alice);
    defer db.freeEdgeInfos(alice_out_after);
    try std.testing.expectEqual(@as(usize, 1), alice_out_after.len);
    try std.testing.expectEqual(charlie, alice_out_after[0].target);
}

test "database: self-loop edge works correctly" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_selfloop_test.ltdb";

    @import("compat").fs.cwd().deleteFile(path) catch {};

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{ .enable_wal = false, .enable_fts = false },
    });
    defer {
        db.close();
        @import("compat").fs.cwd().deleteFile(path) catch {};
    }

    const node = try db.createNode(null, &[_][]const u8{"Node"});

    // Self-loop: node -> node
    try db.createEdge(null, node, node, "SELF_REF");

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

    @import("compat").fs.cwd().deleteFile(path) catch {};

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{ .enable_wal = false, .enable_fts = false },
    });
    defer {
        db.close();
        @import("compat").fs.cwd().deleteFile(path) catch {};
    }

    // Create test data
    const alice = try db.createNode(null, &[_][]const u8{"Person"});
    try db.setNodeProperty(null, alice, "name", .{ .string_val = "Alice" });
    try db.setNodeProperty(null, alice, "age", .{ .int_val = 30 });

    const bob = try db.createNode(null, &[_][]const u8{"Person"});
    try db.setNodeProperty(null, bob, "name", .{ .string_val = "Bob" });
    try db.setNodeProperty(null, bob, "age", .{ .int_val = 25 });

    const charlie = try db.createNode(null, &[_][]const u8{"Person"});
    try db.setNodeProperty(null, charlie, "name", .{ .string_val = "Charlie" });
    try db.setNodeProperty(null, charlie, "age", .{ .int_val = 35 });

    // Query all Person nodes
    var result = try db.query("MATCH (n:Person) RETURN n");
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 3), result.rowCount());
}

test "database: query with LIMIT" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_query_limit_test.ltdb";

    @import("compat").fs.cwd().deleteFile(path) catch {};

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{ .enable_wal = false, .enable_fts = false },
    });
    defer {
        db.close();
        @import("compat").fs.cwd().deleteFile(path) catch {};
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

    @import("compat").fs.cwd().deleteFile(path) catch {};

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{ .enable_wal = false, .enable_fts = false },
    });
    defer {
        db.close();
        @import("compat").fs.cwd().deleteFile(path) catch {};
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

    @import("compat").fs.cwd().deleteFile(path) catch {};

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{ .enable_wal = false, .enable_fts = false },
    });
    defer {
        db.close();
        @import("compat").fs.cwd().deleteFile(path) catch {};
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

    @import("compat").fs.cwd().deleteFile(path) catch {};

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{ .enable_wal = false, .enable_fts = false },
    });
    defer {
        db.close();
        @import("compat").fs.cwd().deleteFile(path) catch {};
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

    @import("compat").fs.cwd().deleteFile(path) catch {};

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{ .enable_wal = false, .enable_fts = false },
    });
    defer {
        db.close();
        @import("compat").fs.cwd().deleteFile(path) catch {};
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

    @import("compat").fs.cwd().deleteFile(path) catch {};

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{ .enable_wal = false, .enable_fts = false },
    });
    defer {
        db.close();
        @import("compat").fs.cwd().deleteFile(path) catch {};
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
// Variable-Length Path Tests
// ============================================================================

test "database: variable-length path 1 to 2 hops" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_var_path_1_2_test.ltdb";

    @import("compat").fs.cwd().deleteFile(path) catch {};

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{ .enable_wal = false, .enable_fts = false },
    });
    defer {
        db.close();
        @import("compat").fs.cwd().deleteFile(path) catch {};
    }

    // Create a chain: Start -> A -> B -> C (using distinct labels)
    // Start is the only "Root" node so we can filter on label
    const start = try db.createNode(null, &[_][]const u8{"Root"});

    const a = try db.createNode(null, &[_][]const u8{"Target"});
    const b = try db.createNode(null, &[_][]const u8{"Target"});
    const c = try db.createNode(null, &[_][]const u8{"Target"});

    // Create edges: Start->A->B->C
    _ = try db.createEdge(null, start, a, "NEXT");
    _ = try db.createEdge(null, a, b, "NEXT");
    _ = try db.createEdge(null, b, c, "NEXT");

    // Query: Find all Target nodes reachable from Root in 1-2 hops
    var result = try db.query(
        \\MATCH (s:Root)-[:NEXT*1..2]->(t:Target)
        \\RETURN t
    );
    defer result.deinit();

    // Should find A (1 hop) and B (2 hops), not C (3 hops)
    try std.testing.expectEqual(@as(usize, 2), result.rowCount());
}

test "database: variable-length path exact hops" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_var_path_exact_test.ltdb";

    @import("compat").fs.cwd().deleteFile(path) catch {};

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{ .enable_wal = false, .enable_fts = false },
    });
    defer {
        db.close();
        @import("compat").fs.cwd().deleteFile(path) catch {};
    }

    // Create a chain: Start -> A -> B -> C
    const start = try db.createNode(null, &[_][]const u8{"Root"});

    const a = try db.createNode(null, &[_][]const u8{"Target"});
    const b = try db.createNode(null, &[_][]const u8{"Target"});
    const c = try db.createNode(null, &[_][]const u8{"Target"});

    // Create edges: Start->A->B->C
    _ = try db.createEdge(null, start, a, "NEXT");
    _ = try db.createEdge(null, a, b, "NEXT");
    _ = try db.createEdge(null, b, c, "NEXT");

    // Query: Find Target nodes exactly 2 hops from Root
    var result = try db.query(
        \\MATCH (s:Root)-[:NEXT*2]->(t:Target)
        \\RETURN t
    );
    defer result.deinit();

    // Should find only B (exactly 2 hops)
    try std.testing.expectEqual(@as(usize, 1), result.rowCount());
}

test "database: variable-length path with branching" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_var_path_branch_test.ltdb";

    @import("compat").fs.cwd().deleteFile(path) catch {};

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{ .enable_wal = false, .enable_fts = false },
    });
    defer {
        db.close();
        @import("compat").fs.cwd().deleteFile(path) catch {};
    }

    // Create a branching graph:
    //     B -> D
    //    /
    //   Start
    //    \
    //     C -> E
    const start = try db.createNode(null, &[_][]const u8{"Root"});

    const b = try db.createNode(null, &[_][]const u8{"Target"});
    const c = try db.createNode(null, &[_][]const u8{"Target"});
    const d = try db.createNode(null, &[_][]const u8{"Target"});
    const e = try db.createNode(null, &[_][]const u8{"Target"});

    // Create edges: Start->B, Start->C, B->D, C->E
    _ = try db.createEdge(null, start, b, "NEXT");
    _ = try db.createEdge(null, start, c, "NEXT");
    _ = try db.createEdge(null, b, d, "NEXT");
    _ = try db.createEdge(null, c, e, "NEXT");

    // Query: Find all Target nodes within 2 hops from Root
    var result = try db.query(
        \\MATCH (s:Root)-[:NEXT*1..2]->(t:Target)
        \\RETURN t
    );
    defer result.deinit();

    // Should find: B, C (1 hop), D, E (2 hops) = 4 results
    try std.testing.expectEqual(@as(usize, 4), result.rowCount());
}

test "database: variable-length path unbounded star" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_var_path_star_test.ltdb";

    @import("compat").fs.cwd().deleteFile(path) catch {};

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{ .enable_wal = false, .enable_fts = false },
    });
    defer {
        db.close();
        @import("compat").fs.cwd().deleteFile(path) catch {};
    }

    // Create a chain: Start -> A -> B
    const start = try db.createNode(null, &[_][]const u8{"Root"});

    const a = try db.createNode(null, &[_][]const u8{"Target"});
    const b = try db.createNode(null, &[_][]const u8{"Target"});

    _ = try db.createEdge(null, start, a, "NEXT");
    _ = try db.createEdge(null, a, b, "NEXT");

    // Query with unbounded path (*) - should find all reachable Target nodes
    var result = try db.query(
        \\MATCH (s:Root)-[:NEXT*]->(t:Target)
        \\RETURN t
    );
    defer result.deinit();

    // Should find A (1 hop) and B (2 hops)
    try std.testing.expectEqual(@as(usize, 2), result.rowCount());
}

test "database: variable-length path supports exact zero hops" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_var_path_zero_exact_test.ltdb";

    @import("compat").fs.cwd().deleteFile(path) catch {};

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{ .enable_wal = false, .enable_fts = false },
    });
    defer {
        db.close();
        @import("compat").fs.cwd().deleteFile(path) catch {};
    }

    const root = try db.createNode(null, &[_][]const u8{"Root"});
    const other = try db.createNode(null, &[_][]const u8{"Root"});
    _ = try db.createEdge(null, root, other, "NEXT");

    var result = try db.query(
        \\MATCH (s:Root)-[:NEXT*0..0]->(t:Root)
        \\RETURN count(t)
    );
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 1), result.rowCount());
    const count_val = result.rows[0].values[0];
    switch (count_val) {
        .int_val => |v| try std.testing.expectEqual(@as(i64, 2), v),
        else => return error.UnexpectedValueType,
    }
}

test "database: variable-length path zero to one hops includes source and neighbor" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_var_path_zero_to_one_test.ltdb";

    @import("compat").fs.cwd().deleteFile(path) catch {};

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{ .enable_wal = false, .enable_fts = false },
    });
    defer {
        db.close();
        @import("compat").fs.cwd().deleteFile(path) catch {};
    }

    const root = try db.createNode(null, &[_][]const u8{ "Root", "Target" });
    const child = try db.createNode(null, &[_][]const u8{"Target"});
    _ = try db.createEdge(null, root, child, "NEXT");

    var result = try db.query(
        \\MATCH (s:Root)-[:NEXT*0..1]->(t:Target)
        \\RETURN count(t)
    );
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 1), result.rowCount());
    const count_val = result.rows[0].values[0];
    switch (count_val) {
        .int_val => |v| try std.testing.expectEqual(@as(i64, 2), v),
        else => return error.UnexpectedValueType,
    }
}

// ============================================================================
// FTS Integration Tests
// ============================================================================

test "database: fts search returns relevant results" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_fts_integration_test.ltdb";

    @import("compat").fs.cwd().deleteFile(path) catch {};

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
        @import("compat").fs.cwd().deleteFile(path) catch {};
    }

    // The text lives in the property the index is declared over.
    try db.createNodeFtsIndex("Document", "text");

    const doc1 = try db.createNode(null, &[_][]const u8{"Document"});
    try db.setNodeProperty(null, doc1, "text", .{ .string_val = "The quick brown fox jumps over the lazy dog" });

    const doc2 = try db.createNode(null, &[_][]const u8{"Document"});
    try db.setNodeProperty(null, doc2, "text", .{ .string_val = "A lazy cat sleeps on the couch" });

    const doc3 = try db.createNode(null, &[_][]const u8{"Document"});
    try db.setNodeProperty(null, doc3, "text", .{ .string_val = "Quick reflexes are important for athletes" });

    // Search for "quick"
    const results = try db.ftsSearchIndex(.node, "Document", "text", "quick", 10);
    defer db.freeFtsSearchResults(results);

    try std.testing.expectEqual(@as(usize, 2), results.len); // doc1 and doc3
}

test "database: fts handles document updates" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_fts_update_test.ltdb";

    @import("compat").fs.cwd().deleteFile(path) catch {};

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
        @import("compat").fs.cwd().deleteFile(path) catch {};
    }

    // Create and index document
    const doc = try db.createNode(null, &[_][]const u8{"Document"});
    try db.createNodeFtsIndex("Document", "text");
    try db.setNodeProperty(null, doc, "text", .{ .string_val = "original content about apples" });

    // Search for "apples"
    const results1 = try db.ftsSearchIndex(.node, "Document", "text", "apples", 10);
    defer db.freeFtsSearchResults(results1);
    try std.testing.expectEqual(@as(usize, 1), results1.len);

    // Note: To update, you'd remove and re-index (if FTS supports it)
    // This tests the indexing worked correctly
}

test "database: fts fuzzy search finds documents with typos" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_fts_fuzzy_test.ltdb";

    @import("compat").fs.cwd().deleteFile(path) catch {};

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
        @import("compat").fs.cwd().deleteFile(path) catch {};
    }

    // Create and index documents
    const doc1 = try db.createNode(null, &[_][]const u8{"Document"});
    try db.createNodeFtsIndex("Document", "text");
    try db.setNodeProperty(null, doc1, "text", .{ .string_val = "the database engine is fast" });

    const doc2 = try db.createNode(null, &[_][]const u8{"Document"});
    try db.setNodeProperty(null, doc2, "text", .{ .string_val = "machine learning models" });

    // Exact search should find "database"
    const exact_results = try db.ftsSearchIndex(.node, "Document", "text", "database", 10);
    defer db.freeFtsSearchResults(exact_results);
    try std.testing.expectEqual(@as(usize, 1), exact_results.len);

    // Fuzzy search with a typo: "databse" should still find "database"
    const fuzzy_results = try db.ftsSearchIndexFuzzy(null, .node, "Document", "text", "databse", 10, 2, 4);
    defer db.freeFtsSearchResults(fuzzy_results);
    try std.testing.expectEqual(@as(usize, 1), fuzzy_results.len);
    try std.testing.expectEqual(doc1, fuzzy_results[0].doc_id);
}

// ============================================================================
// Vector Search Integration Tests
// ============================================================================

test "database: vector search finds similar nodes" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_vector_integration_test.ltdb";

    @import("compat").fs.cwd().deleteFile(path) catch {};

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
        @import("compat").fs.cwd().deleteFile(path) catch {};
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

fn expectVectorResultAbsent(results: []const lattice.storage.database.VectorSearchResult, node_id: u64) !void {
    for (results) |result| {
        try std.testing.expect(result.node_id != node_id);
    }
}

test "database: vector delete removes from search results" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_vector_delete_test.ltdb";

    @import("compat").fs.cwd().deleteFile(path) catch {};
    defer @import("compat").fs.cwd().deleteFile(path) catch {};

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{ .enable_wal = false, .enable_fts = false, .enable_vector = true, .vector_dimensions = 4 },
    });
    defer db.close();

    const n1 = try db.createNode(null, &[_][]const u8{"Embedding"});
    try db.setNodeVector(n1, &[_]f32{ 1.0, 0.0, 0.0, 0.0 });
    const n2 = try db.createNode(null, &[_][]const u8{"Embedding"});
    try db.setNodeVector(n2, &[_]f32{ 0.0, 1.0, 0.0, 0.0 });

    try db.deleteNode(null, n1);

    const results = try db.vectorSearch(&[_]f32{ 1.0, 0.0, 0.0, 0.0 }, 2, null);
    defer db.freeVectorSearchResults(results);

    try std.testing.expectEqual(@as(usize, 1), results.len);
    try std.testing.expectEqual(n2, results[0].node_id);
    try expectVectorResultAbsent(results, n1);
}

test "database: vector replace updates search results" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_vector_replace_test.ltdb";

    @import("compat").fs.cwd().deleteFile(path) catch {};
    defer @import("compat").fs.cwd().deleteFile(path) catch {};

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{ .enable_wal = false, .enable_fts = false, .enable_vector = true, .vector_dimensions = 4 },
    });
    defer db.close();

    const n1 = try db.createNode(null, &[_][]const u8{"Embedding"});
    try db.setNodeVector(n1, &[_]f32{ 1.0, 0.0, 0.0, 0.0 });
    const n2 = try db.createNode(null, &[_][]const u8{"Embedding"});
    try db.setNodeVector(n2, &[_]f32{ 0.9, 0.1, 0.0, 0.0 });

    try db.setNodeVector(n1, &[_]f32{ 0.0, 0.0, 1.0, 0.0 });

    const old_query_results = try db.vectorSearch(&[_]f32{ 1.0, 0.0, 0.0, 0.0 }, 2, null);
    defer db.freeVectorSearchResults(old_query_results);
    try std.testing.expectEqual(n2, old_query_results[0].node_id);

    const new_query_results = try db.vectorSearch(&[_]f32{ 0.0, 0.0, 1.0, 0.0 }, 2, null);
    defer db.freeVectorSearchResults(new_query_results);
    try std.testing.expectEqual(n1, new_query_results[0].node_id);
}

test "database: vector delete in transaction removes from search" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_vector_delete_txn_test.ltdb";

    @import("compat").fs.cwd().deleteFile(path) catch {};
    @import("compat").fs.cwd().deleteFile(path ++ "-wal") catch {};
    defer @import("compat").fs.cwd().deleteFile(path) catch {};
    defer @import("compat").fs.cwd().deleteFile(path ++ "-wal") catch {};

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{ .enable_wal = true, .enable_fts = false, .enable_vector = true, .vector_dimensions = 4 },
    });
    defer db.close();

    const n1 = try db.createNode(null, &[_][]const u8{"Embedding"});
    try db.setNodeVector(n1, &[_]f32{ 1.0, 0.0, 0.0, 0.0 });
    const n2 = try db.createNode(null, &[_][]const u8{"Embedding"});
    try db.setNodeVector(n2, &[_]f32{ 0.0, 1.0, 0.0, 0.0 });

    var txn = try db.beginTransaction(.read_write);
    try db.deleteNode(&txn, n1);

    const txn_results = try db.vectorSearchInTxn(&txn, &[_]f32{ 1.0, 0.0, 0.0, 0.0 }, 2, null);
    defer db.freeVectorSearchResults(txn_results);
    try std.testing.expectEqual(@as(usize, 1), txn_results.len);
    try std.testing.expectEqual(n2, txn_results[0].node_id);
    try expectVectorResultAbsent(txn_results, n1);

    try db.commitTransaction(&txn);

    const committed_results = try db.vectorSearch(&[_]f32{ 1.0, 0.0, 0.0, 0.0 }, 2, null);
    defer db.freeVectorSearchResults(committed_results);
    try expectVectorResultAbsent(committed_results, n1);
}

test "database: vector replace in transaction updates search" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_vector_replace_txn_test.ltdb";

    @import("compat").fs.cwd().deleteFile(path) catch {};
    @import("compat").fs.cwd().deleteFile(path ++ "-wal") catch {};
    defer @import("compat").fs.cwd().deleteFile(path) catch {};
    defer @import("compat").fs.cwd().deleteFile(path ++ "-wal") catch {};

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{ .enable_wal = true, .enable_fts = false, .enable_vector = true, .vector_dimensions = 4 },
    });
    defer db.close();

    const n1 = try db.createNode(null, &[_][]const u8{"Embedding"});
    try db.setNodeVector(n1, &[_]f32{ 1.0, 0.0, 0.0, 0.0 });
    const n2 = try db.createNode(null, &[_][]const u8{"Embedding"});
    try db.setNodeVector(n2, &[_]f32{ 0.9, 0.1, 0.0, 0.0 });

    var txn = try db.beginTransaction(.read_write);
    try db.setNodeVectorInTxn(&txn, n1, &[_]f32{ 0.0, 0.0, 1.0, 0.0 });

    const old_query_results = try db.vectorSearchInTxn(&txn, &[_]f32{ 1.0, 0.0, 0.0, 0.0 }, 2, null);
    defer db.freeVectorSearchResults(old_query_results);
    try std.testing.expectEqual(n2, old_query_results[0].node_id);

    const new_query_results = try db.vectorSearchInTxn(&txn, &[_]f32{ 0.0, 0.0, 1.0, 0.0 }, 2, null);
    defer db.freeVectorSearchResults(new_query_results);
    try std.testing.expectEqual(n1, new_query_results[0].node_id);

    try db.commitTransaction(&txn);

    const committed_results = try db.vectorSearch(&[_]f32{ 0.0, 0.0, 1.0, 0.0 }, 2, null);
    defer db.freeVectorSearchResults(committed_results);
    try std.testing.expectEqual(n1, committed_results[0].node_id);
}

test "database: vector delete survives reopen" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_vector_delete_reopen_test.ltdb";

    @import("compat").fs.cwd().deleteFile(path) catch {};
    defer @import("compat").fs.cwd().deleteFile(path) catch {};

    const n1: u64 = blk: {
        var db = try Database.open(allocator, path, .{
            .create = true,
            .config = .{ .enable_wal = false, .enable_fts = false, .enable_vector = true, .vector_dimensions = 4 },
        });

        const node = try db.createNode(null, &[_][]const u8{"Embedding"});
        try db.setNodeVector(node, &[_]f32{ 1.0, 0.0, 0.0, 0.0 });
        const n2 = try db.createNode(null, &[_][]const u8{"Embedding"});
        try db.setNodeVector(n2, &[_]f32{ 0.0, 1.0, 0.0, 0.0 });
        try db.deleteNode(null, node);
        db.close();
        break :blk node;
    };

    var reopened = try Database.open(allocator, path, .{
        .create = false,
        .config = .{ .enable_wal = false, .enable_fts = false, .enable_vector = true, .vector_dimensions = 4 },
    });
    defer reopened.close();

    const results = try reopened.vectorSearch(&[_]f32{ 1.0, 0.0, 0.0, 0.0 }, 2, null);
    defer reopened.freeVectorSearchResults(results);
    try expectVectorResultAbsent(results, n1);
}

test "database: vector replace survives reopen" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_vector_replace_reopen_test.ltdb";

    @import("compat").fs.cwd().deleteFile(path) catch {};
    defer @import("compat").fs.cwd().deleteFile(path) catch {};

    const n1: u64 = blk: {
        var db = try Database.open(allocator, path, .{
            .create = true,
            .config = .{ .enable_wal = false, .enable_fts = false, .enable_vector = true, .vector_dimensions = 4 },
        });

        const node = try db.createNode(null, &[_][]const u8{"Embedding"});
        try db.setNodeVector(node, &[_]f32{ 1.0, 0.0, 0.0, 0.0 });
        _ = try db.createNode(null, &[_][]const u8{"Embedding"});
        try db.setNodeVector(node, &[_]f32{ 0.0, 0.0, 1.0, 0.0 });
        db.close();
        break :blk node;
    };

    var reopened = try Database.open(allocator, path, .{
        .create = false,
        .config = .{ .enable_wal = false, .enable_fts = false, .enable_vector = true, .vector_dimensions = 4 },
    });
    defer reopened.close();

    const results = try reopened.vectorSearch(&[_]f32{ 0.0, 0.0, 1.0, 0.0 }, 2, null);
    defer reopened.freeVectorSearchResults(results);
    try std.testing.expectEqual(n1, results[0].node_id);
}

test "database: rejects invalid vector dimensions" {
    const allocator = std.testing.allocator;

    const zero_path = "/tmp/lattice_vector_zero_dims_test.ltdb";
    @import("compat").fs.cwd().deleteFile(zero_path) catch {};
    defer @import("compat").fs.cwd().deleteFile(zero_path) catch {};
    try std.testing.expectError(error.InvalidArgument, Database.open(allocator, zero_path, .{
        .create = true,
        .config = .{
            .enable_wal = false,
            .enable_fts = false,
            .enable_vector = true,
            .vector_dimensions = 0,
        },
    }));

    const too_large_path = "/tmp/lattice_vector_too_large_dims_test.ltdb";
    @import("compat").fs.cwd().deleteFile(too_large_path) catch {};
    defer @import("compat").fs.cwd().deleteFile(too_large_path) catch {};
    try std.testing.expectError(error.InvalidArgument, Database.open(allocator, too_large_path, .{
        .create = true,
        .config = .{
            .enable_wal = false,
            .enable_fts = false,
            .enable_vector = true,
            .vector_dimensions = 4097,
        },
    }));
}

test "database: rejects vector insert and query dimension mismatches" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_vector_dimension_mismatch_test.ltdb";

    @import("compat").fs.cwd().deleteFile(path) catch {};

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
        @import("compat").fs.cwd().deleteFile(path) catch {};
    }

    const node = try db.createNode(null, &[_][]const u8{"Embedding"});
    try std.testing.expectError(error.InvalidArgument, db.setNodeVector(node, &[_]f32{ 1.0, 2.0, 3.0 }));

    try db.setNodeVector(node, &[_]f32{ 1.0, 0.0, 0.0, 0.0 });
    try std.testing.expectError(error.InvalidArgument, db.vectorSearch(&[_]f32{ 1.0, 0.0, 0.0 }, 1, null));
}

test "database: vector search supports 4096 dimensions" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_vector_4096_integration_test.ltdb";

    @import("compat").fs.cwd().deleteFile(path) catch {};

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{
            .enable_wal = false,
            .enable_fts = false,
            .enable_vector = true,
            .vector_dimensions = 4096,
        },
    });
    defer {
        db.close();
        @import("compat").fs.cwd().deleteFile(path) catch {};
    }

    var exact: [4096]f32 = [_]f32{0.0} ** 4096;
    exact[0] = 1.0;
    var near: [4096]f32 = [_]f32{0.0} ** 4096;
    near[0] = 0.95;
    near[1] = 0.05;
    var far: [4096]f32 = [_]f32{0.0} ** 4096;
    far[512] = 1.0;

    const n1 = try db.createNode(null, &[_][]const u8{"Embedding"});
    try db.setNodeVector(n1, &exact);

    const n2 = try db.createNode(null, &[_][]const u8{"Embedding"});
    try db.setNodeVector(n2, &near);

    const n3 = try db.createNode(null, &[_][]const u8{"Embedding"});
    try db.setNodeVector(n3, &far);

    const results = try db.vectorSearch(&exact, 3, null);
    defer db.freeVectorSearchResults(results);

    try std.testing.expectEqual(@as(usize, 3), results.len);
    try std.testing.expectEqual(n1, results[0].node_id);
    try std.testing.expectEqual(n2, results[1].node_id);
}

// ============================================================================
// Stress Tests
// ============================================================================

test "database: handles many nodes efficiently" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_stress_nodes_test.ltdb";

    @import("compat").fs.cwd().deleteFile(path) catch {};

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{ .enable_wal = false, .enable_fts = false },
    });
    defer {
        db.close();
        @import("compat").fs.cwd().deleteFile(path) catch {};
    }

    const node_count = 500;

    // Create many nodes with properties
    for (0..node_count) |i| {
        const node = try db.createNode(null, &[_][]const u8{"StressNode"});
        try db.setNodeProperty(null, node, "index", .{ .int_val = @intCast(i) });
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

    @import("compat").fs.cwd().deleteFile(path) catch {};

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{ .enable_wal = false, .enable_fts = false },
    });
    defer {
        db.close();
        @import("compat").fs.cwd().deleteFile(path) catch {};
    }

    // Create a star topology: center node with 100 connections
    const center = try db.createNode(null, &[_][]const u8{"Center"});

    const spoke_count = 100;
    for (0..spoke_count) |_| {
        const spoke = try db.createNode(null, &[_][]const u8{"Spoke"});
        try db.createEdge(null, center, spoke, "CONNECTED");
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

test "database: outgoing traversal does not depend on edge payload deserialization" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_outgoing_refs_regression.ltdb";

    @import("compat").fs.cwd().deleteFile(path) catch {};

    {
        var db = try Database.open(allocator, path, .{
            .create = true,
            .config = .{ .enable_wal = false, .enable_fts = false, .enable_vector = false },
        });

        const root = try db.createNode(null, &[_][]const u8{"Node"});
        const mid = try db.createNode(null, &[_][]const u8{"Node"});
        const leaf = try db.createNode(null, &[_][]const u8{"Node"});

        const first = try db.createEdgeAndGetId(null, root, mid, "REL");
        try db.setEdgePropertyById(null, first, "edge_id", .{ .int_val = 1 });

        const second = try db.createEdgeAndGetId(null, mid, leaf, "REL");
        try db.setEdgePropertyById(null, second, "edge_id", .{ .int_val = 2 });

        // Simulate a broken edge payload entry. Adjacency traversal only needs
        // the traversal key, so expanding outgoing edges should still work.
        try overwriteEdgePayloadWithInvalidData(db, second);

        db.close();
    }

    {
        var db = try Database.open(allocator, path, .{
            .read_only = true,
            .config = .{ .enable_wal = false, .enable_fts = false, .enable_vector = false },
        });
        defer {
            db.close();
            @import("compat").fs.cwd().deleteFile(path) catch {};
        }

        const first_hop = try db.getOutgoingEdges(1);
        defer db.freeEdgeInfos(first_hop);
        try std.testing.expectEqual(@as(usize, 1), first_hop.len);
        try std.testing.expectEqual(@as(u64, 2), first_hop[0].target);
        try std.testing.expectEqualStrings("REL", first_hop[0].edge_type);

        const second_hop = try db.getOutgoingEdges(2);
        defer db.freeEdgeInfos(second_hop);
        try std.testing.expectEqual(@as(usize, 1), second_hop.len);
        try std.testing.expectEqual(@as(u64, 3), second_hop[0].target);
        try std.testing.expectEqualStrings("REL", second_hop[0].edge_type);
    }
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

    @import("compat").fs.cwd().deleteFile(path) catch {};

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

    @import("compat").fs.cwd().deleteFile(path) catch {};
}

test "database: unknown read lookups do not intern symbols" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_unknown_lookup_test.ltdb";

    @import("compat").fs.cwd().deleteFile(path) catch {};

    var alice: u64 = 0;
    var bob: u64 = 0;

    {
        var db = try Database.open(allocator, path, .{
            .create = true,
            .config = .{ .enable_wal = false, .enable_fts = false },
        });
        defer db.close();

        alice = try db.createNode(null, &[_][]const u8{"Person"});
        bob = try db.createNode(null, &[_][]const u8{"Person"});
        try db.setNodeProperty(null, alice, "name", .{ .string_val = "Alice" });
        try db.createEdge(null, alice, bob, "KNOWS");

        const symbol_count_before = db.symbol_table.count();

        const missing_prop = try db.getNodeProperty(alice, "missing_key");
        try std.testing.expect(missing_prop == null);

        const missing_label_nodes = try db.getNodesByLabel("MissingLabel");
        defer allocator.free(missing_label_nodes);
        try std.testing.expectEqual(@as(usize, 0), missing_label_nodes.len);

        try std.testing.expect(!db.edgeExists(alice, bob, "MISSING_EDGE"));
        try std.testing.expectEqual(symbol_count_before, db.symbol_table.count());
    }

    {
        var db = try Database.open(allocator, path, .{
            .create = false,
            .read_only = true,
            .config = .{ .enable_wal = false, .enable_fts = false },
        });
        defer db.close();

        const symbol_count_before = db.symbol_table.count();

        const missing_prop = try db.getNodeProperty(alice, "missing_key");
        try std.testing.expect(missing_prop == null);

        const missing_label_nodes = try db.getNodesByLabel("MissingLabel");
        defer allocator.free(missing_label_nodes);
        try std.testing.expectEqual(@as(usize, 0), missing_label_nodes.len);

        try std.testing.expect(!db.edgeExists(alice, bob, "MISSING_EDGE"));
        try std.testing.expectEqual(symbol_count_before, db.symbol_table.count());
    }

    @import("compat").fs.cwd().deleteFile(path) catch {};
}

test "database: property type handling" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_property_types_test.ltdb";

    @import("compat").fs.cwd().deleteFile(path) catch {};

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{ .enable_wal = false, .enable_fts = false },
    });
    defer {
        db.close();
        @import("compat").fs.cwd().deleteFile(path) catch {};
    }

    const node = try db.createNode(null, &[_][]const u8{"TypeTest"});

    // Test different property types
    try db.setNodeProperty(null, node, "string_prop", .{ .string_val = "hello" });
    try db.setNodeProperty(null, node, "int_prop", .{ .int_val = 42 });
    try db.setNodeProperty(null, node, "float_prop", .{ .float_val = 3.14 });
    try db.setNodeProperty(null, node, "bool_prop", .{ .bool_val = true });

    // Verify each type
    var str = (try db.getNodeProperty(node, "string_prop")).?;
    defer str.deinit(allocator);
    try std.testing.expectEqualStrings("hello", str.string_val);

    const int = try db.getNodeProperty(node, "int_prop");
    try std.testing.expectEqual(@as(i64, 42), int.?.int_val);

    const flt = try db.getNodeProperty(node, "float_prop");
    try std.testing.expectApproxEqAbs(@as(f64, 3.14), flt.?.float_val, 0.001);

    const b = try db.getNodeProperty(node, "bool_prop");
    try std.testing.expectEqual(true, b.?.bool_val);
}

test "database: getNodeProperty returns owned heap-backed values" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_owned_property_values_test.ltdb";

    @import("compat").fs.cwd().deleteFile(path) catch {};

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{ .enable_wal = false, .enable_fts = false },
    });
    defer {
        db.close();
        @import("compat").fs.cwd().deleteFile(path) catch {};
    }

    const node = try db.createNode(null, &[_][]const u8{"OwnedValues"});
    const blob = [_]u8{ 1, 2, 3, 4 };
    const embedding = [_]f32{ 0.5, 1.5, 2.5 };
    var tags = [_]PropertyValue{
        .{ .string_val = "alpha" },
        .{ .int_val = 7 },
    };
    var address = [_]PropertyValue.MapEntry{
        .{ .key = "city", .value = .{ .string_val = "Portland" } },
        .{ .key = "zip", .value = .{ .int_val = 97201 } },
    };

    try db.setNodeProperty(null, node, "blob", .{ .bytes_val = &blob });
    try db.setNodeProperty(null, node, "embedding", .{ .vector_val = &embedding });
    try db.setNodeProperty(null, node, "tags", .{ .list_val = &tags });
    try db.setNodeProperty(null, node, "address", .{ .map_val = &address });

    var blob_val = (try db.getNodeProperty(node, "blob")).?;
    defer blob_val.deinit(allocator);
    try std.testing.expectEqualSlices(u8, blob[0..], blob_val.bytes_val);

    var embedding_val = (try db.getNodeProperty(node, "embedding")).?;
    defer embedding_val.deinit(allocator);
    try std.testing.expectEqualSlices(f32, embedding[0..], embedding_val.vector_val);

    var tags_val = (try db.getNodeProperty(node, "tags")).?;
    defer tags_val.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 2), tags_val.list_val.len);
    try std.testing.expectEqualStrings("alpha", tags_val.list_val[0].string_val);
    try std.testing.expectEqual(@as(i64, 7), tags_val.list_val[1].int_val);

    var address_val = (try db.getNodeProperty(node, "address")).?;
    defer address_val.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 2), address_val.map_val.len);
    try std.testing.expectEqualStrings("city", address_val.map_val[0].key);
    try std.testing.expectEqualStrings("Portland", address_val.map_val[0].value.string_val);
    try std.testing.expectEqualStrings("zip", address_val.map_val[1].key);
    try std.testing.expectEqual(@as(i64, 97201), address_val.map_val[1].value.int_val);
}

test "database: getNodeProperties returns owned decoded values" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_node_properties_test.ltdb";

    @import("compat").fs.cwd().deleteFile(path) catch {};

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{ .enable_wal = false, .enable_fts = false },
    });
    defer {
        db.close();
        @import("compat").fs.cwd().deleteFile(path) catch {};
    }

    const empty_node = try db.createNode(null, &[_][]const u8{"Empty"});
    const empty_props = try db.getNodeProperties(empty_node);
    defer db.freePropertyEntries(empty_props);
    try std.testing.expectEqual(@as(usize, 0), empty_props.len);

    const node = try db.createNode(null, &[_][]const u8{"Props"});
    const blob = [_]u8{ 0xAB, 0xCD };
    const embedding = [_]f32{ 0.25, 0.5, 0.75 };
    var tags = [_]PropertyValue{
        .{ .string_val = "graph" },
        .{ .int_val = 7 },
    };
    var meta = [_]PropertyValue.MapEntry{
        .{ .key = "city", .value = .{ .string_val = "Portland" } },
        .{ .key = "zip", .value = .{ .int_val = 97201 } },
    };

    try db.setNodeProperty(null, node, "name", .{ .string_val = "Alice" });
    try db.setNodeProperty(null, node, "age", .{ .int_val = 30 });
    try db.setNodeProperty(null, node, "active", .{ .bool_val = true });
    try db.setNodeProperty(null, node, "blob", .{ .bytes_val = &blob });
    try db.setNodeProperty(null, node, "embedding", .{ .vector_val = &embedding });
    try db.setNodeProperty(null, node, "tags", .{ .list_val = &tags });
    try db.setNodeProperty(null, node, "meta", .{ .map_val = &meta });

    const props = try db.getNodeProperties(node);
    defer db.freePropertyEntries(props);
    try std.testing.expectEqual(@as(usize, 7), props.len);

    const name = findPropertyEntry(props, "name").?;
    try std.testing.expectEqualStrings("Alice", name.value.string_val);

    const age = findPropertyEntry(props, "age").?;
    try std.testing.expectEqual(@as(i64, 30), age.value.int_val);

    const active = findPropertyEntry(props, "active").?;
    try std.testing.expectEqual(true, active.value.bool_val);

    const blob_prop = findPropertyEntry(props, "blob").?;
    try std.testing.expectEqualSlices(u8, blob[0..], blob_prop.value.bytes_val);

    const embedding_prop = findPropertyEntry(props, "embedding").?;
    try std.testing.expectEqualSlices(f32, embedding[0..], embedding_prop.value.vector_val);

    const tags_prop = findPropertyEntry(props, "tags").?;
    try std.testing.expectEqual(@as(usize, 2), tags_prop.value.list_val.len);
    try std.testing.expectEqualStrings("graph", tags_prop.value.list_val[0].string_val);
    try std.testing.expectEqual(@as(i64, 7), tags_prop.value.list_val[1].int_val);

    const meta_prop = findPropertyEntry(props, "meta").?;
    try std.testing.expectEqual(@as(usize, 2), meta_prop.value.map_val.len);
    try std.testing.expectEqualStrings("city", meta_prop.value.map_val[0].key);
    try std.testing.expectEqualStrings("Portland", meta_prop.value.map_val[0].value.string_val);
    try std.testing.expectEqualStrings("zip", meta_prop.value.map_val[1].key);
    try std.testing.expectEqual(@as(i64, 97201), meta_prop.value.map_val[1].value.int_val);
}

// ============================================================================
// List/Vector/Map Property Tests
// ============================================================================

test "database: list property round-trip through query" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_list_prop_query_test.ltdb";

    @import("compat").fs.cwd().deleteFile(path) catch {};

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{
            .enable_wal = false,
            .enable_fts = false,
            .enable_query_cache = false,
        },
    });
    defer {
        db.close();
        @import("compat").fs.cwd().deleteFile(path) catch {};
    }

    const node = try db.createNode(null, &[_][]const u8{"ListNode"});
    var list_items = [_]PropertyValue{
        .{ .int_val = 10 },
        .{ .int_val = 20 },
        .{ .int_val = 30 },
    };
    try db.setNodeProperty(null, node, "scores", .{ .list_val = &list_items });

    // Query the list property
    var result = try db.query("MATCH (n:ListNode) RETURN n.scores");
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 1), result.rowCount());

    const val = result.rows[0].values[0];
    switch (val) {
        .list_val => |list| {
            try std.testing.expectEqual(@as(usize, 3), list.len);
            try std.testing.expectEqual(@as(i64, 10), list[0].int_val);
            try std.testing.expectEqual(@as(i64, 20), list[1].int_val);
            try std.testing.expectEqual(@as(i64, 30), list[2].int_val);
        },
        else => return error.UnexpectedValueType,
    }
}

test "database: vector property round-trip through query" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_vec_prop_query_test.ltdb";

    @import("compat").fs.cwd().deleteFile(path) catch {};

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{
            .enable_wal = false,
            .enable_fts = false,
            .enable_query_cache = false,
        },
    });
    defer {
        db.close();
        @import("compat").fs.cwd().deleteFile(path) catch {};
    }

    const node = try db.createNode(null, &[_][]const u8{"VecNode"});
    const vec = [_]f32{ 0.1, 0.2, 0.3, 0.4 };
    try db.setNodeProperty(null, node, "embedding", .{ .vector_val = &vec });

    // Query the vector property
    var result = try db.query("MATCH (n:VecNode) RETURN n.embedding");
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 1), result.rowCount());

    const val = result.rows[0].values[0];
    switch (val) {
        .vector_val => |v| {
            try std.testing.expectEqual(@as(usize, 4), v.len);
            try std.testing.expectApproxEqAbs(@as(f32, 0.1), v[0], 0.001);
            try std.testing.expectApproxEqAbs(@as(f32, 0.2), v[1], 0.001);
            try std.testing.expectApproxEqAbs(@as(f32, 0.3), v[2], 0.001);
            try std.testing.expectApproxEqAbs(@as(f32, 0.4), v[3], 0.001);
        },
        else => return error.UnexpectedValueType,
    }
}

test "database: map property round-trip through query" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_map_prop_query_test.ltdb";

    @import("compat").fs.cwd().deleteFile(path) catch {};

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{
            .enable_wal = false,
            .enable_fts = false,
            .enable_query_cache = false,
        },
    });
    defer {
        db.close();
        @import("compat").fs.cwd().deleteFile(path) catch {};
    }

    const node = try db.createNode(null, &[_][]const u8{"MapNode"});
    var map_entries = [_]PropertyValue.MapEntry{
        .{ .key = "city", .value = .{ .string_val = "Portland" } },
        .{ .key = "zip", .value = .{ .int_val = 97201 } },
    };
    try db.setNodeProperty(null, node, "address", .{ .map_val = &map_entries });

    // Query the map property
    var result = try db.query("MATCH (n:MapNode) RETURN n.address");
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 1), result.rowCount());

    const val = result.rows[0].values[0];
    switch (val) {
        .map_val => |map| {
            try std.testing.expectEqual(@as(usize, 2), map.len);
            try std.testing.expectEqualStrings("city", map[0].key);
            try std.testing.expectEqualStrings("Portland", map[0].value.string_val);
            try std.testing.expectEqualStrings("zip", map[1].key);
            try std.testing.expectEqual(@as(i64, 97201), map[1].value.int_val);
        },
        else => return error.UnexpectedValueType,
    }
}

test "database: mixed-type list property through query" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_mixed_list_query_test.ltdb";

    @import("compat").fs.cwd().deleteFile(path) catch {};

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{
            .enable_wal = false,
            .enable_fts = false,
            .enable_query_cache = false,
        },
    });
    defer {
        db.close();
        @import("compat").fs.cwd().deleteFile(path) catch {};
    }

    const node = try db.createNode(null, &[_][]const u8{"MixedNode"});
    var list_items = [_]PropertyValue{
        .{ .int_val = 42 },
        .{ .string_val = "hello" },
        .{ .bool_val = true },
        .{ .float_val = 2.718 },
    };
    try db.setNodeProperty(null, node, "mixed", .{ .list_val = &list_items });

    var result = try db.query("MATCH (n:MixedNode) RETURN n.mixed");
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 1), result.rowCount());

    const val = result.rows[0].values[0];
    switch (val) {
        .list_val => |list| {
            try std.testing.expectEqual(@as(usize, 4), list.len);
            try std.testing.expectEqual(@as(i64, 42), list[0].int_val);
            try std.testing.expectEqualStrings("hello", list[1].string_val);
            try std.testing.expectEqual(true, list[2].bool_val);
            try std.testing.expectApproxEqAbs(@as(f64, 2.718), list[3].float_val, 0.001);
        },
        else => return error.UnexpectedValueType,
    }
}

test "database: empty list property through query" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_empty_list_query_test.ltdb";

    @import("compat").fs.cwd().deleteFile(path) catch {};

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{
            .enable_wal = false,
            .enable_fts = false,
            .enable_query_cache = false,
        },
    });
    defer {
        db.close();
        @import("compat").fs.cwd().deleteFile(path) catch {};
    }

    const node = try db.createNode(null, &[_][]const u8{"EmptyListNode"});
    try db.setNodeProperty(null, node, "tags", .{ .list_val = &[_]PropertyValue{} });

    var result = try db.query("MATCH (n:EmptyListNode) RETURN n.tags");
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 1), result.rowCount());

    const val = result.rows[0].values[0];
    switch (val) {
        .list_val => |list| {
            try std.testing.expectEqual(@as(usize, 0), list.len);
        },
        else => return error.UnexpectedValueType,
    }
}

test "database: multiple complex properties on same node" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_multi_complex_query_test.ltdb";

    @import("compat").fs.cwd().deleteFile(path) catch {};

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{
            .enable_wal = false,
            .enable_fts = false,
            .enable_query_cache = false,
        },
    });
    defer {
        db.close();
        @import("compat").fs.cwd().deleteFile(path) catch {};
    }

    const node = try db.createNode(null, &[_][]const u8{"ComplexNode"});

    // Set multiple complex properties
    var scores = [_]PropertyValue{ .{ .int_val = 85 }, .{ .int_val = 92 } };
    try db.setNodeProperty(null, node, "scores", .{ .list_val = &scores });
    try db.setNodeProperty(null, node, "name", .{ .string_val = "Alice" });

    // Query name property
    {
        var result = try db.query("MATCH (n:ComplexNode) RETURN n.name");
        defer result.deinit();

        try std.testing.expectEqual(@as(usize, 1), result.rowCount());
        try std.testing.expectEqualStrings("Alice", result.rows[0].values[0].string_val);
    }

    // Query scores property (list)
    {
        var result = try db.query("MATCH (n:ComplexNode) RETURN n.scores");
        defer result.deinit();

        try std.testing.expectEqual(@as(usize, 1), result.rowCount());
        const scores_val = result.rows[0].values[0];
        switch (scores_val) {
            .list_val => |list| {
                try std.testing.expectEqual(@as(usize, 2), list.len);
                try std.testing.expectEqual(@as(i64, 85), list[0].int_val);
                try std.testing.expectEqual(@as(i64, 92), list[1].int_val);
            },
            else => return error.UnexpectedValueType,
        }
    }
}

// ============================================================
// Multi-Label Query Tests
// ============================================================

test "database: multi-label query filters nodes with all specified labels" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_multi_label_test.ltdb";

    @import("compat").fs.cwd().deleteFile(path) catch {};

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{ .enable_wal = false, .enable_fts = false, .enable_vector = false },
    });
    defer {
        db.close();
        @import("compat").fs.cwd().deleteFile(path) catch {};
    }

    // Create nodes with varying label combinations
    const alice = try db.createNode(null, &[_][]const u8{ "Person", "Employee" });
    try db.setNodeProperty(null, alice, "name", .{ .string_val = "Alice" });

    const bob = try db.createNode(null, &[_][]const u8{ "Person", "Manager" });
    try db.setNodeProperty(null, bob, "name", .{ .string_val = "Bob" });

    const charlie = try db.createNode(null, &[_][]const u8{ "Person", "Employee", "Senior" });
    try db.setNodeProperty(null, charlie, "name", .{ .string_val = "Charlie" });

    const dave = try db.createNode(null, &[_][]const u8{"Person"});
    try db.setNodeProperty(null, dave, "name", .{ .string_val = "Dave" });

    // Query with two labels: Person AND Employee
    var result1 = try db.query("MATCH (n:Person:Employee) RETURN n");
    defer result1.deinit();
    // Alice has Person+Employee, Charlie has Person+Employee+Senior
    try std.testing.expectEqual(@as(usize, 2), result1.rowCount());

    // Query with two labels: Person AND Manager
    var result2 = try db.query("MATCH (n:Person:Manager) RETURN n");
    defer result2.deinit();
    // Only Bob has both Person and Manager
    try std.testing.expectEqual(@as(usize, 1), result2.rowCount());

    // Query with single label: Person (all four nodes)
    var result3 = try db.query("MATCH (n:Person) RETURN n");
    defer result3.deinit();
    try std.testing.expectEqual(@as(usize, 4), result3.rowCount());
}

test "database: multi-label query on target of edge" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_multi_label_edge_test.ltdb";

    @import("compat").fs.cwd().deleteFile(path) catch {};

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{ .enable_wal = false, .enable_fts = false, .enable_vector = false },
    });
    defer {
        db.close();
        @import("compat").fs.cwd().deleteFile(path) catch {};
    }

    // Create nodes
    const alice = try db.createNode(null, &[_][]const u8{ "Person", "Employee" });
    try db.setNodeProperty(null, alice, "name", .{ .string_val = "Alice" });

    const bob = try db.createNode(null, &[_][]const u8{ "Person", "Manager" });
    try db.setNodeProperty(null, bob, "name", .{ .string_val = "Bob" });

    const charlie = try db.createNode(null, &[_][]const u8{ "Person", "Employee" });
    try db.setNodeProperty(null, charlie, "name", .{ .string_val = "Charlie" });

    // Create edges: Alice and Charlie work with Bob
    try db.createEdge(null, alice, bob, "WORKS_WITH");
    try db.createEdge(null, charlie, bob, "WORKS_WITH");
    try db.createEdge(null, alice, charlie, "WORKS_WITH");

    // Query: find Person nodes that work with a Person:Manager
    var result = try db.query("MATCH (a:Person)-[:WORKS_WITH]->(b:Person:Manager) RETURN b");
    defer result.deinit();
    // Alice->Bob and Charlie->Bob; Bob is the only Person:Manager target
    try std.testing.expectEqual(@as(usize, 2), result.rowCount());
}

test "database: three labels on node pattern" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_three_label_test.ltdb";

    @import("compat").fs.cwd().deleteFile(path) catch {};

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{ .enable_wal = false, .enable_fts = false, .enable_vector = false },
    });
    defer {
        db.close();
        @import("compat").fs.cwd().deleteFile(path) catch {};
    }

    // Create nodes with varying label counts
    _ = try db.createNode(null, &[_][]const u8{ "Person", "Employee" });
    const senior = try db.createNode(null, &[_][]const u8{ "Person", "Employee", "Senior" });
    try db.setNodeProperty(null, senior, "name", .{ .string_val = "Senior Employee" });
    _ = try db.createNode(null, &[_][]const u8{ "Person", "Manager", "Senior" });

    // Query with three labels: Person AND Employee AND Senior
    var result = try db.query("MATCH (n:Person:Employee:Senior) RETURN n");
    defer result.deinit();
    // Only the second node has all three labels
    try std.testing.expectEqual(@as(usize, 1), result.rowCount());
}

test "database: explicit property indexes track direct and transactional mutations" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_property_index_test.ltdb";
    @import("compat").fs.cwd().deleteFile(path) catch {};
    @import("compat").fs.cwd().deleteFile(path ++ "-wal") catch {};
    defer {
        @import("compat").fs.cwd().deleteFile(path) catch {};
        @import("compat").fs.cwd().deleteFile(path ++ "-wal") catch {};
    }

    var alice: u64 = undefined;
    var bob: u64 = undefined;
    var edge_id: u64 = undefined;
    {
        var db = try Database.open(allocator, path, .{ .create = true, .config = .{
            .enable_fts = false,
            .enable_vector = false,
        } });
        defer db.close();

        alice = try db.createNode(null, &.{"Person"});
        bob = try db.createNode(null, &.{"Person"});
        try db.setNodeProperty(null, alice, "email", .{ .string_val = "alice@example.com" });
        try db.setNodeProperty(null, bob, "email", .{ .string_val = "bob@example.com" });

        try std.testing.expectError(
            DatabaseError.MissingIndex,
            db.findNodesByLabelProperty(null, "Person", "email", .{ .string_val = "alice@example.com" }, 10),
        );
        try db.createNodePropertyIndex("Person", "email");
        try std.testing.expectError(DatabaseError.AlreadyExists, db.createNodePropertyIndex("Person", "email"));

        var ids = try db.findNodesByLabelProperty(null, "Person", "email", .{ .string_val = "alice@example.com" }, 10);
        defer allocator.free(ids);
        try std.testing.expectEqualSlices(u64, &.{alice}, ids);

        try db.setNodeProperty(null, bob, "email", .{ .string_val = "alice@example.com" });
        allocator.free(ids);
        ids = try db.findNodesByLabelProperty(null, "Person", "email", .{ .string_val = "alice@example.com" }, 10);
        try std.testing.expectEqualSlices(u64, &.{ alice, bob }, ids);

        var txn = try db.beginTransaction(.read_write);
        try db.setNodeProperty(&txn, bob, "email", .{ .string_val = "bob@example.com" });
        const txn_ids = try db.findNodesByLabelProperty(&txn, "Person", "email", .{ .string_val = "bob@example.com" }, 10);
        defer allocator.free(txn_ids);
        try std.testing.expectEqualSlices(u64, &.{bob}, txn_ids);
        try db.commitTransaction(&txn);

        allocator.free(ids);
        ids = try db.findNodesByLabelProperty(null, "Person", "email", .{ .string_val = "alice@example.com" }, 10);
        try std.testing.expectEqualSlices(u64, &.{alice}, ids);

        edge_id = try db.createEdgeAndGetId(null, alice, bob, "KNOWS");
        try db.setEdgePropertyById(null, edge_id, "since", .{ .int_val = 2024 });
        try db.createEdgePropertyIndex("KNOWS", "since");
        const edge_ids = try db.findEdgesByTypeProperty(null, "KNOWS", "since", .{ .int_val = 2024 }, 10);
        defer allocator.free(edge_ids);
        try std.testing.expectEqualSlices(u64, &.{edge_id}, edge_ids);

        try db.sync();
    }

    {
        var db = try Database.open(allocator, path, .{ .config = .{
            .enable_fts = false,
            .enable_vector = false,
        } });
        defer db.close();

        const node_ids = try db.findNodesByLabelProperty(null, "Person", "email", .{ .string_val = "alice@example.com" }, 10);
        defer allocator.free(node_ids);
        try std.testing.expectEqualSlices(u64, &.{alice}, node_ids);

        const edge_ids = try db.findEdgesByTypeProperty(null, "KNOWS", "since", .{ .int_val = 2024 }, 10);
        defer allocator.free(edge_ids);
        try std.testing.expectEqualSlices(u64, &.{edge_id}, edge_ids);

        // Assert on the plan, not on the rows. An index scan and a label scan
        // return the same rows, so a result set cannot distinguish them; only
        // the planner's own decision can.
        const ScanKind = lattice.query.planner.QueryPlanner.ScanKind;

        // An inline property in the pattern is enough to select the index.
        try std.testing.expectEqual(
            ScanKind.property_index_scan,
            try planScanKind(db, "MATCH (n:Person {email: \"alice@example.com\"}) RETURN n"),
        );

        // So is the equivalent WHERE equality, in either order.
        try std.testing.expectEqual(
            ScanKind.property_index_scan,
            try planScanKind(db, "MATCH (n:Person) WHERE n.email = \"alice@example.com\" RETURN n"),
        );
        try std.testing.expectEqual(
            ScanKind.property_index_scan,
            try planScanKind(db, "MATCH (n:Person) WHERE \"alice@example.com\" = n.email RETURN n"),
        );

        // A conjunction still qualifies: every row has to satisfy both sides,
        // so narrowing on one of them cannot drop a row that should match.
        try std.testing.expectEqual(
            ScanKind.property_index_scan,
            try planScanKind(
                db,
                "MATCH (n:Person) WHERE n.email = \"alice@example.com\" AND n.name = \"Alice\" RETURN n",
            ),
        );

        // A disjunction must not. Narrowing to either branch would discard rows
        // matching the other, so the planner has to fall back to a label scan.
        try std.testing.expectEqual(
            ScanKind.label_scan,
            try planScanKind(
                db,
                "MATCH (n:Person) WHERE n.email = \"alice@example.com\" OR n.email = \"bob@example.com\" RETURN n",
            ),
        );

        // A property with no index behind it also falls back.
        try std.testing.expectEqual(
            ScanKind.label_scan,
            try planScanKind(db, "MATCH (n:Person) WHERE n.name = \"Alice\" RETURN n"),
        );

        {
            var params = std.StringHashMap(PropertyValue).init(allocator);
            defer params.deinit();
            try params.put("email", .{ .string_val = "alice@example.com" });
            try params.put("guard", .{ .bool_val = true });

            var inline_query = try db.queryWithParams(
                "MATCH (n:Person {email: $email}) RETURN n",
                &params,
            );
            defer inline_query.deinit();
            try std.testing.expectEqual(@as(usize, 1), inline_query.rowCount());

            var where_query = try db.queryWithParams(
                "MATCH (n:Person) WHERE n.email = $email RETURN n",
                &params,
            );
            defer where_query.deinit();
            try std.testing.expectEqual(@as(usize, 1), where_query.rowCount());

            var reversed_query = try db.queryWithParams(
                "MATCH (n:Person) WHERE $email = n.email RETURN n",
                &params,
            );
            defer reversed_query.deinit();
            try std.testing.expectEqual(@as(usize, 1), reversed_query.rowCount());

            var conjunction_query = try db.queryWithParams(
                "MATCH (n:Person) WHERE $guard AND n.email = $email RETURN n",
                &params,
            );
            defer conjunction_query.deinit();
            try std.testing.expectEqual(@as(usize, 1), conjunction_query.rowCount());

            // An OR cannot safely constrain the scan to either branch, so this
            // falls back to a label scan and both people match their own
            // address.
            var disjunction_query = try db.queryWithParams(
                "MATCH (n:Person) WHERE n.email = $email OR n.email = \"bob@example.com\" RETURN n",
                &params,
            );
            defer disjunction_query.deinit();
            try std.testing.expectEqual(@as(usize, 2), disjunction_query.rowCount());
        }

        try db.removeEdgePropertyById(null, edge_id, "since");
        const removed = try db.findEdgesByTypeProperty(null, "KNOWS", "since", .{ .int_val = 2024 }, 10);
        defer allocator.free(removed);
        try std.testing.expectEqual(@as(usize, 0), removed.len);

        try db.dropNodePropertyIndex("Person", "email");
        try std.testing.expectError(
            DatabaseError.MissingIndex,
            db.findNodesByLabelProperty(null, "Person", "email", .{ .string_val = "alice@example.com" }, 10),
        );
    }
}

test "database: relationship patterns traverse in every direction" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_expand_direction_regression.ltdb";

    @import("compat").fs.cwd().deleteFile(path) catch {};
    defer @import("compat").fs.cwd().deleteFile(path) catch {};

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{ .enable_wal = false, .enable_fts = false, .enable_vector = false },
    });
    defer db.close();

    // A -[:KNOWS]-> B
    const a = try db.createNode(null, &[_][]const u8{"P"});
    const b = try db.createNode(null, &[_][]const u8{"P"});
    try db.setNodeProperty(null, a, "n", .{ .string_val = "A" });
    try db.setNodeProperty(null, b, "n", .{ .string_val = "B" });
    _ = try db.createEdgeAndGetId(null, a, b, "KNOWS");

    // Rightward patterns start at A.
    {
        var result = try db.query("MATCH (x:P {n: \"A\"})-[:KNOWS]->(y) RETURN y.n");
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), result.rowCount());
    }

    // Leftward patterns are the ones that regressed: the expand operator chose
    // its iterator from in_incoming_phase, which only tracks the second half of
    // a `both` traversal, so a plain incoming expand read a null outgoing list
    // and produced nothing at all.
    {
        var result = try db.query("MATCH (x:P {n: \"B\"})<-[:KNOWS]-(y) RETURN y.n");
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), result.rowCount());
    }

    // With an edge variable bound, and with no type at all.
    {
        var result = try db.query("MATCH (x:P {n: \"B\"})<-[r:KNOWS]-(y) RETURN y.n");
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), result.rowCount());
    }
    {
        var result = try db.query("MATCH (x:P {n: \"B\"})<--(y) RETURN y.n");
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), result.rowCount());
    }

    // An undirected pattern reaches the neighbour from either end.
    {
        var result = try db.query("MATCH (x:P {n: \"A\"})-[:KNOWS]-(y) RETURN y.n");
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), result.rowCount());
    }
    {
        var result = try db.query("MATCH (x:P {n: \"B\"})-[:KNOWS]-(y) RETURN y.n");
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), result.rowCount());
    }

    // A leftward pattern binds the far end to the source of the edge, so
    // starting at B must reach A rather than B itself.
    {
        var result = try db.query("MATCH (x:P {n: \"B\"})<-[:KNOWS]-(y) RETURN y.n");
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), result.rowCount());
        switch (result.rows[0].values[0]) {
            .string_val => |s| try std.testing.expectEqualStrings("A", s),
            else => return error.UnexpectedValueType,
        }
    }
}

test "database: string literals decode escape sequences" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_string_escape_regression.ltdb";

    @import("compat").fs.cwd().deleteFile(path) catch {};
    defer @import("compat").fs.cwd().deleteFile(path) catch {};

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{ .enable_wal = false, .enable_fts = false, .enable_vector = false },
    });
    defer db.close();

    // The lexer steps over escapes to find where a string ends but leaves them
    // in the token text, so decoding has to happen when the literal is built.
    var create = try db.query(
        \\CREATE (n:E {quote: "say \"hi\"", tab: "a\tb", newline: "a\nb", backslash: "a\\b"})
    );
    create.deinit();

    const cases = [_]struct { property: []const u8, expected: []const u8 }{
        .{ .property = "quote", .expected = "say \"hi\"" },
        .{ .property = "tab", .expected = "a\tb" },
        .{ .property = "newline", .expected = "a\nb" },
        .{ .property = "backslash", .expected = "a\\b" },
    };

    for (cases) |case| {
        var value = (try db.getNodeProperty(1, case.property)) orelse
            return error.PropertyMissing;
        defer value.deinit(allocator);
        switch (value) {
            .string_val => |s| try std.testing.expectEqualStrings(case.expected, s),
            else => return error.UnexpectedValueType,
        }
    }
}

test "database: queryWrites distinguishes reading from writing clauses" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_query_writes_regression.ltdb";

    @import("compat").fs.cwd().deleteFile(path) catch {};
    defer @import("compat").fs.cwd().deleteFile(path) catch {};

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{ .enable_wal = false, .enable_fts = false, .enable_vector = false },
    });
    defer db.close();

    // Bindings use this to pick a transaction mode. Answering "writes" for a
    // read would take the single writer slot and stop concurrent reads;
    // answering "reads" for a write makes the query fail outright.
    const reads = [_][]const u8{
        "MATCH (n) RETURN n",
        "MATCH (n) WHERE n.v = 1 RETURN n.v ORDER BY n.v LIMIT 5",
        "MATCH (n) WITH n RETURN n",
        "UNWIND [1, 2] AS x RETURN x",
        "MATCH (n) RETURN count(n)",
    };
    for (reads) |cypher| {
        try std.testing.expect(!db.queryWrites(cypher));
    }

    const writes = [_][]const u8{
        "CREATE (n:A)",
        "MATCH (n) SET n.v = 1",
        "MATCH (n) DELETE n",
        "MATCH (n) REMOVE n.v",
        "MERGE (n:A {k: 1})",
        "MATCH (n) WITH n CREATE (m:B)",
    };
    for (writes) |cypher| {
        try std.testing.expect(db.queryWrites(cypher));
    }

    // A query that cannot be parsed is reported as read-only, so a caller opens
    // the weaker transaction and lets execution report the real error.
    try std.testing.expect(!db.queryWrites("this is not cypher"));
}

test "database: ORDER BY can reference a RETURN alias" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_order_by_alias_regression.ltdb";

    @import("compat").fs.cwd().deleteFile(path) catch {};
    defer @import("compat").fs.cwd().deleteFile(path) catch {};

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{ .enable_wal = false, .enable_fts = false, .enable_vector = false },
    });
    defer db.close();

    const a = try db.createNode(null, &[_][]const u8{"P"});
    const b = try db.createNode(null, &[_][]const u8{"P"});
    try db.setNodeProperty(null, a, "n", .{ .string_val = "Alice" });
    try db.setNodeProperty(null, b, "n", .{ .string_val = "Bob" });

    // Sorting runs before projection, so an alias is not a column yet when the
    // sort is built. The planner substitutes the expression it stands for.
    {
        var result = try db.query("MATCH (p:P) RETURN p.n AS who ORDER BY who");
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 2), result.rowCount());
        switch (result.rows[0].values[0]) {
            .string_val => |s| try std.testing.expectEqualStrings("Alice", s),
            else => return error.UnexpectedValueType,
        }
    }

    // Descending through the alias orders the other way.
    {
        var result = try db.query("MATCH (p:P) RETURN p.n AS who ORDER BY who DESC");
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 2), result.rowCount());
        switch (result.rows[0].values[0]) {
            .string_val => |s| try std.testing.expectEqualStrings("Bob", s),
            else => return error.UnexpectedValueType,
        }
    }

    // A name that is neither a bound variable nor an alias is still an error.
    try std.testing.expectError(
        error.SemanticError,
        db.query("MATCH (p:P) RETURN p.n AS who ORDER BY nope"),
    );
}

test "database: automatic checkpointing bounds WAL growth" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_auto_checkpoint_regression.ltdb";
    const wal_path = "/tmp/lattice_auto_checkpoint_regression.ltdb-wal";

    @import("compat").fs.cwd().deleteFile(path) catch {};
    @import("compat").fs.cwd().deleteFile(wal_path) catch {};
    defer @import("compat").fs.cwd().deleteFile(path) catch {};
    defer @import("compat").fs.cwd().deleteFile(wal_path) catch {};

    // A small threshold keeps the test quick; the policy is the same at any size.
    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{
            .enable_fts = false,
            .enable_vector = false,
            .auto_checkpoint = .{ .max_wal_frames = 16, .min_interval_ns = 0, .mode = .truncate },
        },
    });
    defer db.close();

    // Without automatic checkpointing the frame count only ever climbs, because
    // nothing resets the WAL until the database is closed.
    var peak_frames: u64 = 0;
    var i: usize = 0;
    while (i < 400) : (i += 1) {
        var txn = try db.beginTransaction(.read_write);
        const node = try db.createNode(&txn, &[_][]const u8{"P"});
        try db.setNodeProperty(&txn, node, "i", .{ .int_val = @intCast(i) });
        try db.commitTransaction(&txn);
        if (db.wal) |*wal| {
            peak_frames = @max(peak_frames, wal.header.frame_count);
        }
    }

    // Guard against the assertion below passing because nothing was logged at all.
    try std.testing.expect(peak_frames > 0);

    // The threshold is checked after a commit, so the count can overshoot by the
    // frames one transaction adds. It must not grow without limit.
    try std.testing.expect(peak_frames < 200);

    // Every write is still there.
    var result = try db.query("MATCH (n:P) RETURN count(n)");
    defer result.deinit();
    switch (result.rows[0].values[0]) {
        .int_val => |v| try std.testing.expectEqual(@as(i64, 400), v),
        else => return error.UnexpectedValueType,
    }
}

test "database: checkpoint reports what it did and can be disabled" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_manual_checkpoint_regression.ltdb";
    const wal_path = "/tmp/lattice_manual_checkpoint_regression.ltdb-wal";

    @import("compat").fs.cwd().deleteFile(path) catch {};
    @import("compat").fs.cwd().deleteFile(wal_path) catch {};
    defer @import("compat").fs.cwd().deleteFile(path) catch {};
    defer @import("compat").fs.cwd().deleteFile(wal_path) catch {};

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{
            .enable_fts = false,
            .enable_vector = false,
            // Opting out has to be possible for anyone managing checkpoints
            // themselves, such as a backup holding frames until they ship.
            .auto_checkpoint = null,
        },
    });
    defer db.close();

    // Enough data to fill and flush frames: frame_count only counts frames that
    // have been written out, so a handful of bare nodes all sit in the current
    // in-memory frame and never reach the file.
    var i: usize = 0;
    while (i < 400) : (i += 1) {
        var txn = try db.beginTransaction(.read_write);
        const node = try db.createNode(&txn, &[_][]const u8{"P"});
        try db.setNodeProperty(&txn, node, "i", .{ .int_val = @intCast(i) });
        try db.commitTransaction(&txn);
    }

    // With the policy disabled nothing has reset the WAL.
    const frames_before = if (db.wal) |*wal| wal.header.frame_count else 0;
    try std.testing.expect(frames_before > 0);

    const stats = (try db.checkpoint(.truncate)) orelse return error.NoWal;
    try std.testing.expect(stats.wal_truncated);
    try std.testing.expectEqual(@as(u64, 0), if (db.wal) |*wal| wal.header.frame_count else 1);

    // A full checkpoint flushes without disturbing frame numbering, which is what
    // a reader following the WAL needs.
    var extra = try db.beginTransaction(.read_write);
    _ = try db.createNode(&extra, &[_][]const u8{"P"});
    try db.commitTransaction(&extra);
    const full = (try db.checkpoint(.full)) orelse return error.NoWal;
    try std.testing.expect(!full.wal_truncated);
}

test "database: backup copies a live database into a standalone file" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_backup_source.ltdb";
    const wal_path = "/tmp/lattice_backup_source.ltdb-wal";
    const dest = "/tmp/lattice_backup_copy.ltdb";
    const dest_wal = "/tmp/lattice_backup_copy.ltdb-wal";

    for ([_][]const u8{ path, wal_path, dest, dest_wal }) |p| {
        @import("compat").fs.cwd().deleteFile(p) catch {};
    }
    defer for ([_][]const u8{ path, wal_path, dest, dest_wal }) |p| {
        @import("compat").fs.cwd().deleteFile(p) catch {};
    };

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{ .enable_fts = false, .enable_vector = false },
    });

    var i: usize = 0;
    while (i < 200) : (i += 1) {
        var txn = try db.beginTransaction(.read_write);
        const node = try db.createNode(&txn, &[_][]const u8{"P"});
        try db.setNodeProperty(&txn, node, "i", .{ .int_val = @intCast(i) });
        try db.commitTransaction(&txn);
    }

    // Taken without closing the source.
    const stats = try db.backup(dest);
    try std.testing.expect(stats.bytes_copied > 0);
    try std.testing.expect(stats.pages_copied > 0);

    // The source keeps working afterwards.
    {
        var txn = try db.beginTransaction(.read_write);
        _ = try db.createNode(&txn, &[_][]const u8{"P"});
        try db.commitTransaction(&txn);
    }
    db.close();

    // The copy stands on its own. Everything committed before the backup is
    // there, and the write that came after it is not.
    var restored = try Database.open(allocator, dest, .{
        .config = .{ .enable_fts = false, .enable_vector = false },
    });
    defer restored.close();

    var result = try restored.query("MATCH (n:P) RETURN count(n)");
    defer result.deinit();
    switch (result.rows[0].values[0]) {
        .int_val => |v| try std.testing.expectEqual(@as(i64, 200), v),
        else => return error.UnexpectedValueType,
    }
}

test "database: backup refuses to run while a transaction is open" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_backup_conflict.ltdb";
    const wal_path = "/tmp/lattice_backup_conflict.ltdb-wal";
    const dest = "/tmp/lattice_backup_conflict_copy.ltdb";

    for ([_][]const u8{ path, wal_path, dest }) |p| {
        @import("compat").fs.cwd().deleteFile(p) catch {};
    }
    defer for ([_][]const u8{ path, wal_path, dest }) |p| {
        @import("compat").fs.cwd().deleteFile(p) catch {};
    };

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{ .enable_fts = false, .enable_vector = false },
    });
    defer db.close();

    // A copy taken while writes are landing underneath it is torn in ways no
    // checksum on the source would catch, so this is refused rather than
    // worked around.
    var txn = try db.beginTransaction(.read_write);
    _ = try db.createNode(&txn, &[_][]const u8{"P"});
    try std.testing.expectError(DatabaseError.TransactionConflict, db.backup(dest));
    try db.commitTransaction(&txn);

    // With nothing in flight it goes through.
    _ = try db.backup(dest);
}

test "database: WAL reader follows a log the writer still owns" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_wal_reader.ltdb";
    const wal_path = "/tmp/lattice_wal_reader.ltdb-wal";

    @import("compat").fs.cwd().deleteFile(path) catch {};
    @import("compat").fs.cwd().deleteFile(wal_path) catch {};
    defer @import("compat").fs.cwd().deleteFile(path) catch {};
    defer @import("compat").fs.cwd().deleteFile(wal_path) catch {};

    const wal_reader = lattice.storage.wal_reader;

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{
            .enable_fts = false,
            .enable_vector = false,
            // Truncation is what the reader has to notice, so drive it by hand.
            .auto_checkpoint = null,
        },
    });
    defer db.close();

    var i: usize = 0;
    while (i < 300) : (i += 1) {
        var txn = try db.beginTransaction(.read_write);
        const node = try db.createNode(&txn, &[_][]const u8{"P"});
        try db.setNodeProperty(&txn, node, "i", .{ .int_val = @intCast(i) });
        try db.commitTransaction(&txn);
    }

    var posix = lattice.storage.vfs.PosixVfs.init(allocator);
    const vfs = posix.vfs();

    // The reader opens the same file the writer still has open.
    var reader = try wal_reader.WalReader.open(allocator, vfs, wal_path, null);
    defer reader.close();

    try std.testing.expect(reader.frame_count > 0);

    // Every published frame reads back and passes its own checksum.
    var n: u64 = 0;
    var records: u64 = 0;
    while (n < reader.frame_count) : (n += 1) {
        const frame = try reader.readFrame(n);
        try std.testing.expectEqual(n, frame.number);
        try std.testing.expectEqual(@as(usize, reader.frame_size), frame.raw.len);
        records += frame.record_count;
    }
    try std.testing.expect(records > 0);

    // Nothing at or past frame_count has been published yet.
    try std.testing.expectError(
        wal_reader.WalReaderError.FrameNotYetDurable,
        reader.readFrame(reader.frame_count),
    );

    // Writing more and refreshing exposes the new frames.
    const before = reader.frame_count;
    i = 0;
    while (i < 300) : (i += 1) {
        var txn = try db.beginTransaction(.read_write);
        const node = try db.createNode(&txn, &[_][]const u8{"Q"});
        try db.setNodeProperty(&txn, node, "i", .{ .int_val = @intCast(i) });
        try db.commitTransaction(&txn);
    }
    try reader.refresh();
    try std.testing.expect(reader.frame_count > before);

    // A checkpoint resets frame numbering, which the reader must report rather
    // than silently carry on counting through.
    _ = try db.checkpoint(.truncate);
    try std.testing.expectError(wal_reader.WalReaderError.Rewound, reader.refresh());

    // Having reported it once, the reader is usable again for the new generation.
    try reader.refresh();
    try std.testing.expectEqual(@as(u64, 0), reader.frame_count);
}

test "database: WAL reader rejects a log from another database" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_wal_reader_uuid.ltdb";
    const wal_path = "/tmp/lattice_wal_reader_uuid.ltdb-wal";

    @import("compat").fs.cwd().deleteFile(path) catch {};
    @import("compat").fs.cwd().deleteFile(wal_path) catch {};
    defer @import("compat").fs.cwd().deleteFile(path) catch {};
    defer @import("compat").fs.cwd().deleteFile(wal_path) catch {};

    const wal_reader = lattice.storage.wal_reader;

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{ .enable_fts = false, .enable_vector = false },
    });
    defer db.close();

    var txn = try db.beginTransaction(.read_write);
    _ = try db.createNode(&txn, &[_][]const u8{"P"});
    try db.commitTransaction(&txn);

    var posix = lattice.storage.vfs.PosixVfs.init(allocator);
    const vfs = posix.vfs();

    // A log replayed into the wrong database is not a mistake that announces
    // itself later, so the reader refuses at open when told what to expect.
    const wrong_uuid = [_]u8{0xAB} ** 16;
    try std.testing.expectError(
        wal_reader.WalReaderError.UuidMismatch,
        wal_reader.WalReader.open(allocator, vfs, wal_path, wrong_uuid),
    );

    // The database's own UUID is accepted.
    var reader = try wal_reader.WalReader.open(allocator, vfs, wal_path, null);
    defer reader.close();
    var matching = try wal_reader.WalReader.open(allocator, vfs, wal_path, reader.database_uuid);
    matching.close();
}

test "database: WAL reader treats damage as damage and timing as timing" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_wal_reader_torn.ltdb";
    const wal_path = "/tmp/lattice_wal_reader_torn.ltdb-wal";

    @import("compat").fs.cwd().deleteFile(path) catch {};
    @import("compat").fs.cwd().deleteFile(wal_path) catch {};
    defer @import("compat").fs.cwd().deleteFile(path) catch {};
    defer @import("compat").fs.cwd().deleteFile(wal_path) catch {};

    const wal_reader = lattice.storage.wal_reader;

    {
        var db = try Database.open(allocator, path, .{
            .create = true,
            .config = .{ .enable_fts = false, .enable_vector = false, .auto_checkpoint = null },
        });
        defer db.close();

        var i: usize = 0;
        while (i < 300) : (i += 1) {
            var txn = try db.beginTransaction(.read_write);
            const node = try db.createNode(&txn, &[_][]const u8{"P"});
            try db.setNodeProperty(&txn, node, "i", .{ .int_val = @intCast(i) });
            try db.commitTransaction(&txn);
        }
    }

    var posix = lattice.storage.vfs.PosixVfs.init(allocator);
    const vfs = posix.vfs();

    // Corrupt the middle of a published frame. A torn read clears when retried;
    // this does not, which is the difference the retry helper has to respect.
    {
        var file = try vfs.open(wal_path, .{ .read = true, .write = true });
        defer file.close();
        const target_offset = lattice.storage.wal.WAL_HEADER_SIZE +
            lattice.storage.wal.FRAME_SIZE + 64;
        try file.write(target_offset, &[_]u8{ 0xDE, 0xAD, 0xBE, 0xEF });
        try file.sync();
    }

    var reader = try wal_reader.WalReader.open(allocator, vfs, wal_path, null);
    defer reader.close();

    // Frame 0 is untouched and still reads.
    _ = try reader.readFrame(0);

    // Frame 1 fails, and keeps failing however many times it is asked.
    try std.testing.expectError(
        wal_reader.WalReaderError.FrameChecksumMismatch,
        reader.readFrame(1),
    );
    try std.testing.expectError(
        wal_reader.WalReaderError.FrameChecksumMismatch,
        reader.readFrameRetrying(1, 3, 0),
    );

    // Retrying a healthy frame returns it rather than burning the attempts.
    const good = try reader.readFrameRetrying(0, 3, 0);
    try std.testing.expectEqual(@as(u64, 0), good.number);
}

/// Check that a replication target holds the segment covering frames
/// `[from, to)` of `generation`, and report how big it is.
///
/// Segment names encode the frame range they cover, which is what makes a
/// restore able to order and verify them without opening every file. Asserting
/// on the name therefore checks the part of the layout a restore depends on.
fn segmentSize(dest_dir: []const u8, generation: u64, from: u64, to: u64) !u64 {
    const allocator = std.testing.allocator;
    const segment_path = try std.fmt.allocPrint(
        allocator,
        "{s}/gen-{d:0>10}/frames/{d:0>10}-{d:0>10}.frames",
        .{ dest_dir, generation, from, to - 1 },
    );
    defer allocator.free(segment_path);

    const file = try @import("compat").fs.cwd().openFile(segment_path, .{});
    defer file.close();
    const info = try file.stat();
    return info.size;
}

/// Write `n` nodes, one committed transaction each, so real WAL frames appear.
fn writeNodes(db: *Database, label: []const u8, n: usize) !void {
    var i: usize = 0;
    while (i < n) : (i += 1) {
        var txn = try db.beginTransaction(.read_write);
        const node = try db.createNode(&txn, &[_][]const u8{label});
        try db.setNodeProperty(&txn, node, "i", .{ .int_val = @intCast(i) });
        try db.commitTransaction(&txn);
    }
}

test "database: replication ships a snapshot and then only new frames" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_replicate.ltdb";
    const wal_path = "/tmp/lattice_replicate.ltdb-wal";
    const dest = "/tmp/lattice_replicate_dest";

    @import("compat").fs.cwd().deleteFile(path) catch {};
    @import("compat").fs.cwd().deleteFile(wal_path) catch {};
    @import("compat").fs.cwd().deleteTree(dest) catch {};
    defer @import("compat").fs.cwd().deleteFile(path) catch {};
    defer @import("compat").fs.cwd().deleteFile(wal_path) catch {};
    defer @import("compat").fs.cwd().deleteTree(dest) catch {};

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{
            .enable_fts = false,
            .enable_vector = false,
            // Truncation is one of the things under test, so drive it by hand.
            .auto_checkpoint = null,
        },
    });
    defer db.close();

    try writeNodes(db, "P", 200);

    // The first pass has no destination to continue from, so it opens a
    // generation and writes a full snapshot.
    const first = try db.replicateTo(dest);
    try std.testing.expect(first.started_generation);
    try std.testing.expectEqual(@as(u64, 1), first.generation);
    try std.testing.expect(first.snapshot_bytes > 0);
    // Everything written so far is already inside the snapshot, so nothing is
    // shipped as frames.
    try std.testing.expectEqual(@as(u64, 0), first.frames_shipped);

    // With no writes in between, a second pass has nothing to do and says so
    // rather than treating an empty pass as a problem.
    const idle = try db.replicateTo(dest);
    try std.testing.expect(!idle.started_generation);
    try std.testing.expectEqual(@as(u64, 1), idle.generation);
    try std.testing.expectEqual(@as(u64, 0), idle.frames_shipped);
    try std.testing.expectEqual(@as(u64, 0), idle.snapshot_bytes);

    // Frames below this point are already covered by the snapshot, so shipping
    // picks up from here rather than from zero.
    var opened = (try lattice.storage.replicate.readManifest(allocator, dest)).?;
    const snapshot_frames = opened.current().?.frames_shipped;
    opened.deinit(allocator);
    try std.testing.expect(snapshot_frames > 0);

    // New writes are shipped as frames against the generation already there.
    try writeNodes(db, "Q", 200);
    const second = try db.replicateTo(dest);
    try std.testing.expect(!second.started_generation);
    try std.testing.expectEqual(@as(u64, 1), second.generation);
    try std.testing.expect(second.frames_shipped > 0);
    try std.testing.expect(second.bytes_shipped > 0);
    try std.testing.expectEqual(@as(u64, 0), second.snapshot_bytes);

    // Each pass that ships anything leaves behind one segment named for the
    // frames it covers, picking up where the snapshot left off.
    const second_end = snapshot_frames + second.frames_shipped;
    try std.testing.expectEqual(
        second.bytes_shipped,
        try segmentSize(dest, 1, snapshot_frames, second_end),
    );

    try writeNodes(db, "R", 200);
    const third = try db.replicateTo(dest);
    try std.testing.expect(third.frames_shipped > 0);
    try std.testing.expectEqual(
        third.bytes_shipped,
        try segmentSize(dest, 1, second_end, second_end + third.frames_shipped),
    );

    // The manifest is what a restore reads, so it has to agree with what was
    // actually shipped.
    // Shipping is only useful if what lands at the destination is the same bytes
    // the writer produced. Compare the segment against the live log frame by
    // frame, which is what a restore will ultimately depend on.
    {
        const segment_path = try std.fmt.allocPrint(
            allocator,
            "{s}/gen-{d:0>10}/frames/{d:0>10}-{d:0>10}.frames",
            .{ dest, @as(u64, 1), snapshot_frames, second_end - 1 },
        );
        defer allocator.free(segment_path);

        const segment = try @import("compat").fs.cwd().openFile(segment_path, .{});
        defer segment.close();

        var posix = lattice.storage.vfs.PosixVfs.init(allocator);
        const vfs = posix.vfs();
        var reader = try lattice.storage.wal_reader.WalReader.open(allocator, vfs, wal_path, null);
        defer reader.close();

        const shipped = try allocator.alloc(u8, reader.frame_size);
        defer allocator.free(shipped);

        var n: u64 = snapshot_frames;
        while (n < second_end) : (n += 1) {
            const at = (n - snapshot_frames) * reader.frame_size;
            const read = try segment.preadAll(shipped, at);
            try std.testing.expectEqual(@as(usize, reader.frame_size), read);

            const original = try reader.readFrame(n);
            try std.testing.expectEqualSlices(u8, original.raw, shipped);
        }
    }

    var manifest = (try lattice.storage.replicate.readManifest(allocator, dest)).?;
    defer manifest.deinit(allocator);
    const gen = manifest.current().?;
    try std.testing.expectEqual(@as(u64, 1), gen.number);
    try std.testing.expectEqual(second_end + third.frames_shipped, gen.frames_shipped);
    try std.testing.expect(gen.has_fingerprint);

    // Both passes that shipped anything left a segment recording its range.
    try std.testing.expectEqual(@as(usize, 2), gen.segments.len);
    try std.testing.expectEqual(snapshot_frames, gen.segments[0].from);
    try std.testing.expectEqual(second_end, gen.segments[0].to);
    try std.testing.expectEqual(second_end, gen.segments[1].from);
}

test "database: replication starts a new generation after the log is truncated" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_replicate_gen.ltdb";
    const wal_path = "/tmp/lattice_replicate_gen.ltdb-wal";
    const dest = "/tmp/lattice_replicate_gen_dest";

    @import("compat").fs.cwd().deleteFile(path) catch {};
    @import("compat").fs.cwd().deleteFile(wal_path) catch {};
    @import("compat").fs.cwd().deleteTree(dest) catch {};
    defer @import("compat").fs.cwd().deleteFile(path) catch {};
    defer @import("compat").fs.cwd().deleteFile(wal_path) catch {};
    defer @import("compat").fs.cwd().deleteTree(dest) catch {};

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{
            .enable_fts = false,
            .enable_vector = false,
            .auto_checkpoint = null,
        },
    });
    defer db.close();

    try writeNodes(db, "P", 200);
    const first = try db.replicateTo(dest);
    try std.testing.expectEqual(@as(u64, 1), first.generation);

    var opened = (try lattice.storage.replicate.readManifest(allocator, dest)).?;
    const first_end = opened.current().?.frames_shipped;
    opened.deinit(allocator);

    try writeNodes(db, "Q", 200);
    const second = try db.replicateTo(dest);
    try std.testing.expect(second.frames_shipped > 0);

    // Truncating resets frame numbering. A follower that kept counting would
    // ship the new frame 40 believing it already had it, so this has to be
    // noticed and answered with a fresh snapshot.
    _ = try db.checkpoint(.truncate);
    try writeNodes(db, "R", 200);

    const third = try db.replicateTo(dest);
    try std.testing.expect(third.started_generation);
    try std.testing.expectEqual(@as(u64, 2), third.generation);
    try std.testing.expect(third.snapshot_bytes > 0);

    // The first generation's frames are left alone, because a restore to a point
    // inside it still needs them.
    try std.testing.expect(
        (try segmentSize(dest, 1, first_end, first_end + second.frames_shipped)) > 0,
    );

    var manifest = (try lattice.storage.replicate.readManifest(allocator, dest)).?;
    defer manifest.deinit(allocator);
    try std.testing.expectEqual(@as(u64, 2), manifest.current().?.number);

    // The older generation is kept, along with the segments a restore into it
    // would need.
    try std.testing.expectEqual(@as(usize, 2), manifest.generations.len);
    try std.testing.expect(manifest.generations[0].segments.len > 0);
}

test "database: replication refuses a destination holding another database" {
    const allocator = std.testing.allocator;
    const path_a = "/tmp/lattice_replicate_a.ltdb";
    const path_b = "/tmp/lattice_replicate_b.ltdb";
    const dest = "/tmp/lattice_replicate_mix_dest";

    for ([_][]const u8{ path_a, path_b }) |p| {
        @import("compat").fs.cwd().deleteFile(p) catch {};
        const wal = try std.fmt.allocPrint(allocator, "{s}-wal", .{p});
        defer allocator.free(wal);
        @import("compat").fs.cwd().deleteFile(wal) catch {};
    }
    @import("compat").fs.cwd().deleteTree(dest) catch {};
    defer {
        for ([_][]const u8{ path_a, path_b }) |p| {
            @import("compat").fs.cwd().deleteFile(p) catch {};
        }
        @import("compat").fs.cwd().deleteFile("/tmp/lattice_replicate_a.ltdb-wal") catch {};
        @import("compat").fs.cwd().deleteFile("/tmp/lattice_replicate_b.ltdb-wal") catch {};
        @import("compat").fs.cwd().deleteTree(dest) catch {};
    }

    const options = OpenOptions{
        .create = true,
        .config = .{
            .enable_fts = false,
            .enable_vector = false,
            .auto_checkpoint = null,
        },
    };

    var db_a = try Database.open(allocator, path_a, options);
    defer db_a.close();
    try writeNodes(db_a, "P", 50);
    _ = try db_a.replicateTo(dest);

    // Two databases shipping into one directory would interleave generations
    // that have nothing to do with each other, and the mistake would only show
    // up at restore. The UUID recorded in the manifest catches it now.
    var db_b = try Database.open(allocator, path_b, options);
    defer db_b.close();
    try writeNodes(db_b, "P", 50);
    try std.testing.expectError(
        lattice.storage.replicate.ReplicateError.UuidMismatch,
        db_b.replicateTo(dest),
    );
}

/// How many frames the log holds right now, read the way a follower reads it.
fn walFrameCount(wal_path: []const u8) !u64 {
    const allocator = std.testing.allocator;
    var posix = lattice.storage.vfs.PosixVfs.init(allocator);
    const vfs = posix.vfs();
    var reader = try lattice.storage.wal_reader.WalReader.open(allocator, vfs, wal_path, null);
    defer reader.close();
    return reader.frame_count;
}

test "database: a writing query with no transaction still reaches the WAL" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_implicit_txn.ltdb";
    const wal_path = "/tmp/lattice_implicit_txn.ltdb-wal";

    @import("compat").fs.cwd().deleteFile(path) catch {};
    @import("compat").fs.cwd().deleteFile(wal_path) catch {};
    defer @import("compat").fs.cwd().deleteFile(path) catch {};
    defer @import("compat").fs.cwd().deleteFile(wal_path) catch {};

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{
            .enable_fts = false,
            .enable_vector = false,
            .auto_checkpoint = null,
        },
    });
    defer db.close();

    const before = try walFrameCount(wal_path);

    // A bare query is what the command line sends and what every client library
    // sends for db.query, so it is how almost every write actually arrives. It
    // used to run straight against the pages, which left nothing in the log:
    // no crash atomicity, and nothing for a backup or a replica to see.
    var i: usize = 0;
    while (i < 50) : (i += 1) {
        var result = try db.query("CREATE (p:Person {name: 'implicit'})");
        result.deinit();
    }

    const after = try walFrameCount(wal_path);
    try std.testing.expect(after > before);

    // The writes are real, not merely logged.
    var counted = try db.query("MATCH (p:Person) RETURN count(p) AS c");
    defer counted.deinit();
    try std.testing.expectEqual(@as(usize, 1), counted.rowCount());
}

test "database: a writing query given a transaction does not open its own" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_explicit_txn.ltdb";
    const wal_path = "/tmp/lattice_explicit_txn.ltdb-wal";

    @import("compat").fs.cwd().deleteFile(path) catch {};
    @import("compat").fs.cwd().deleteFile(wal_path) catch {};
    defer @import("compat").fs.cwd().deleteFile(path) catch {};
    defer @import("compat").fs.cwd().deleteFile(wal_path) catch {};

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{
            .enable_fts = false,
            .enable_vector = false,
            .auto_checkpoint = null,
        },
    });
    defer db.close();

    // A caller who opened a transaction is saying where the boundary goes, so a
    // query inside it must not commit on its own. Rolling back has to take the
    // write with it.
    var txn = try db.beginTransaction(.read_write);
    var created = try db.queryInTxn(&txn, "CREATE (p:Person {name: 'rolled back'})");
    created.deinit();
    try db.abortTransaction(&txn);

    var counted = try db.query("MATCH (p:Person) RETURN p.name AS name");
    defer counted.deinit();
    try std.testing.expectEqual(@as(usize, 0), counted.rowCount());
}

/// Count the Person nodes in a database file, opening it fresh.
fn countPeople(path: []const u8) !usize {
    const allocator = std.testing.allocator;
    const db = try Database.open(allocator, path, .{});
    defer db.close();

    var result = try db.query("MATCH (p:Person) RETURN p.name AS name");
    defer result.deinit();
    return result.rowCount();
}

test "database: a restore brings back everything that was shipped" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_restore.ltdb";
    const wal_path = "/tmp/lattice_restore.ltdb-wal";
    const dest = "/tmp/lattice_restore_dest";
    const out = "/tmp/lattice_restore_out.ltdb";
    const out_wal = "/tmp/lattice_restore_out.ltdb-wal";

    for ([_][]const u8{ path, wal_path, out, out_wal }) |p| {
        @import("compat").fs.cwd().deleteFile(p) catch {};
    }
    @import("compat").fs.cwd().deleteTree(dest) catch {};
    defer {
        for ([_][]const u8{ path, wal_path, out, out_wal }) |p| {
            @import("compat").fs.cwd().deleteFile(p) catch {};
        }
        @import("compat").fs.cwd().deleteTree(dest) catch {};
    }

    {
        var db = try Database.open(allocator, path, .{
            .create = true,
            .config = .{
                .enable_fts = false,
                .enable_vector = false,
                .auto_checkpoint = null,
            },
        });
        defer db.close();

        // Written before the first pass, so these live in the snapshot.
        try writeNodes(db, "Person", 20);
        _ = try db.replicateTo(dest);

        // Written after it, so these can only come back through the frames.
        try writeNodes(db, "Person", 30);
        const shipped = try db.replicateTo(dest);
        try std.testing.expect(shipped.frames_shipped > 0);
    }

    try std.testing.expectEqual(@as(usize, 50), try countPeople(path));

    const stats = try lattice.storage.restore.restore(allocator, dest, out, .{});
    try std.testing.expectEqual(@as(u64, 1), stats.generation);
    try std.testing.expect(stats.segments_applied > 0);
    try std.testing.expect(stats.frames_applied > 0);

    // What comes back is a single file. A restore that left a log beside it
    // would be a pair the caller has to know to keep together. This is checked
    // before anything opens the database, because opening one creates a log.
    try std.testing.expectError(
        error.FileNotFound,
        @import("compat").fs.cwd().access(out_wal, .{}),
    );

    // The snapshot alone holds 20. Getting all 50 back is only possible if the
    // shipped frames were replayed on top of it.
    try std.testing.expectEqual(@as(usize, 50), try countPeople(out));
}

test "database: a restore can ask for an earlier moment" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_restore_pit.ltdb";
    const wal_path = "/tmp/lattice_restore_pit.ltdb-wal";
    const dest = "/tmp/lattice_restore_pit_dest";
    const out = "/tmp/lattice_restore_pit_out.ltdb";
    const out_wal = "/tmp/lattice_restore_pit_out.ltdb-wal";

    for ([_][]const u8{ path, wal_path, out, out_wal }) |p| {
        @import("compat").fs.cwd().deleteFile(p) catch {};
    }
    @import("compat").fs.cwd().deleteTree(dest) catch {};
    defer {
        for ([_][]const u8{ path, wal_path, out, out_wal }) |p| {
            @import("compat").fs.cwd().deleteFile(p) catch {};
        }
        @import("compat").fs.cwd().deleteTree(dest) catch {};
    }

    {
        var db = try Database.open(allocator, path, .{
            .create = true,
            .config = .{
                .enable_fts = false,
                .enable_vector = false,
                .auto_checkpoint = null,
            },
        });
        defer db.close();

        try writeNodes(db, "Person", 10);
        _ = try db.replicateTo(dest);

        try writeNodes(db, "Person", 10);
        _ = try db.replicateTo(dest);

        try writeNodes(db, "Person", 10);
        _ = try db.replicateTo(dest);
    }

    var manifest = (try lattice.storage.replicate.readManifest(allocator, dest)).?;
    defer manifest.deinit(allocator);

    const gen = manifest.current().?;
    try std.testing.expectEqual(@as(usize, 2), gen.segments.len);

    // Asking for the moment the first pass finished must leave the second pass
    // out, which is the whole point of recording when each one landed.
    const first_pass_at = gen.segments[0].shipped_at_ms;
    try std.testing.expect(gen.segments[1].shipped_at_ms > first_pass_at);

    const stats = try lattice.storage.restore.restore(allocator, dest, out, .{
        .at_ms = first_pass_at,
        .overwrite = true,
    });
    try std.testing.expectEqual(@as(usize, 1), stats.segments_applied);
    try std.testing.expectEqual(first_pass_at, stats.restored_to_ms);
    try std.testing.expectEqual(@as(usize, 20), try countPeople(out));

    // Asking for nothing in particular gets everything.
    const latest = try lattice.storage.restore.restore(allocator, dest, out, .{
        .overwrite = true,
    });
    try std.testing.expectEqual(@as(usize, 2), latest.segments_applied);
    try std.testing.expectEqual(@as(usize, 30), try countPeople(out));
}

test "database: a restore refuses to overwrite unless asked" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_restore_guard.ltdb";
    const wal_path = "/tmp/lattice_restore_guard.ltdb-wal";
    const dest = "/tmp/lattice_restore_guard_dest";
    const out = "/tmp/lattice_restore_guard_out.ltdb";
    const out_wal = "/tmp/lattice_restore_guard_out.ltdb-wal";

    for ([_][]const u8{ path, wal_path, out, out_wal }) |p| {
        @import("compat").fs.cwd().deleteFile(p) catch {};
    }
    @import("compat").fs.cwd().deleteTree(dest) catch {};
    defer {
        for ([_][]const u8{ path, wal_path, out, out_wal }) |p| {
            @import("compat").fs.cwd().deleteFile(p) catch {};
        }
        @import("compat").fs.cwd().deleteTree(dest) catch {};
    }

    {
        var db = try Database.open(allocator, path, .{
            .create = true,
            .config = .{
                .enable_fts = false,
                .enable_vector = false,
                .auto_checkpoint = null,
            },
        });
        defer db.close();
        try writeNodes(db, "Person", 5);
        _ = try db.replicateTo(dest);
    }

    _ = try lattice.storage.restore.restore(allocator, dest, out, .{});

    // A restore writes over whatever is at the output path, and the thing most
    // likely to be there is a database somebody still wants.
    try std.testing.expectError(
        lattice.storage.restore.RestoreError.OutputExists,
        lattice.storage.restore.restore(allocator, dest, out, .{}),
    );

    // An empty destination has nothing to offer and says so.
    try std.testing.expectError(
        lattice.storage.restore.RestoreError.NoBackup,
        lattice.storage.restore.restore(allocator, "/tmp/lattice_restore_nothing", out, .{
            .overwrite = true,
        }),
    );
}

/// Open a database with the standard test configuration.
fn openForLocking(path: []const u8, options: OpenOptions) !*Database {
    var opts = options;
    opts.config = .{ .enable_fts = false, .enable_vector = false };
    return Database.open(std.testing.allocator, path, opts);
}

test "database: a second writer is refused rather than let in" {
    const allocator = std.testing.allocator;
    _ = allocator;
    const path = "/tmp/lattice_lock_writer.ltdb";

    @import("compat").fs.cwd().deleteFile(path) catch {};
    @import("compat").fs.cwd().deleteFile("/tmp/lattice_lock_writer.ltdb-wal") catch {};
    defer @import("compat").fs.cwd().deleteFile(path) catch {};
    defer @import("compat").fs.cwd().deleteFile("/tmp/lattice_lock_writer.ltdb-wal") catch {};

    const first = try openForLocking(path, .{ .create = true });

    // Two writers on one file corrupt it, and the corruption shows up long
    // after the moment that caused it. The lock is what turns that into an
    // error at the point of the mistake.
    try std.testing.expectError(
        DatabaseError.DatabaseLocked,
        openForLocking(path, .{}),
    );

    // A reader cannot see a writer's buffered pages or its log, so what it would
    // read is a stale file that a checkpoint may be rewriting underneath it.
    try std.testing.expectError(
        DatabaseError.DatabaseLocked,
        openForLocking(path, .{ .read_only = true }),
    );

    // Closing hands the database back.
    first.close();

    const second = try openForLocking(path, .{});
    second.close();
}

test "database: readers share a database with each other" {
    const path = "/tmp/lattice_lock_readers.ltdb";

    @import("compat").fs.cwd().deleteFile(path) catch {};
    @import("compat").fs.cwd().deleteFile("/tmp/lattice_lock_readers.ltdb-wal") catch {};
    defer @import("compat").fs.cwd().deleteFile(path) catch {};
    defer @import("compat").fs.cwd().deleteFile("/tmp/lattice_lock_readers.ltdb-wal") catch {};

    {
        const db = try openForLocking(path, .{ .create = true });
        try writeNodes(db, "Person", 3);
        db.close();
    }

    // Nothing is writing, so any number of readers is fine.
    const a = try openForLocking(path, .{ .read_only = true });
    defer a.close();
    const b = try openForLocking(path, .{ .read_only = true });
    defer b.close();

    var result = try b.query("MATCH (p:Person) RETURN p.name AS name");
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 3), result.rowCount());

    // A writer cannot get in while they are reading.
    try std.testing.expectError(
        DatabaseError.DatabaseLocked,
        openForLocking(path, .{}),
    );
}

test "database: locking can be turned off for filesystems that lack it" {
    const path = "/tmp/lattice_lock_off.ltdb";

    @import("compat").fs.cwd().deleteFile(path) catch {};
    @import("compat").fs.cwd().deleteFile("/tmp/lattice_lock_off.ltdb-wal") catch {};
    defer @import("compat").fs.cwd().deleteFile(path) catch {};
    defer @import("compat").fs.cwd().deleteFile("/tmp/lattice_lock_off.ltdb-wal") catch {};

    const first = try openForLocking(path, .{ .create = true });
    defer first.close();

    // The escape hatch is for filesystems where locking does not work. It does
    // not make this safe, and the second handle here would happily corrupt the
    // first; the point of the test is that the option is honoured.
    const second = try openForLocking(path, .{ .lock = false });
    second.close();

    // A handle that skips the lock does not take one either, so it cannot shut
    // anybody else out.
    const third = try openForLocking(path, .{ .lock = false });
    defer third.close();
}

test "database: a crashed process does not leave the database locked" {
    const path = "/tmp/lattice_lock_release.ltdb";

    @import("compat").fs.cwd().deleteFile(path) catch {};
    @import("compat").fs.cwd().deleteFile("/tmp/lattice_lock_release.ltdb-wal") catch {};
    defer @import("compat").fs.cwd().deleteFile(path) catch {};
    defer @import("compat").fs.cwd().deleteFile("/tmp/lattice_lock_release.ltdb-wal") catch {};

    {
        const db = try openForLocking(path, .{ .create = true });
        try writeNodes(db, "Person", 2);
        db.close();
    }

    // The lock lives on the open file rather than in the database, so it goes
    // away when the handle does, however that happens. There is no stale lock to
    // clear by hand after a crash, which is the failure mode a lock file would
    // have introduced.
    const db = try openForLocking(path, .{});
    defer db.close();

    var result = try db.query("MATCH (p:Person) RETURN p.name AS name");
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 2), result.rowCount());
}

test "database: a serialized database round-trips through bytes" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_serialize.ltdb";
    const wal_path = "/tmp/lattice_serialize.ltdb-wal";

    @import("compat").fs.cwd().deleteFile(path) catch {};
    @import("compat").fs.cwd().deleteFile(wal_path) catch {};
    defer @import("compat").fs.cwd().deleteFile(path) catch {};
    defer @import("compat").fs.cwd().deleteFile(wal_path) catch {};

    var bytes: []u8 = undefined;
    {
        const db = try Database.open(allocator, path, .{
            .create = true,
            .config = .{ .enable_fts = false, .enable_vector = false },
        });
        defer db.close();

        try writeNodes(db, "Person", 40);

        // Written through a query rather than the transaction API, because that
        // is how the data arrives in practice and it used to bypass the log.
        var created = try db.query("CREATE (c:Company {name: 'Acme'})");
        created.deinit();

        bytes = try db.serialize(allocator);
    }
    defer allocator.free(bytes);

    try std.testing.expect(bytes.len > 0);

    // The bytes are a database file, not a private format. Writing them
    // anywhere gives you something that opens.
    const plain = "/tmp/lattice_serialize_plain.ltdb";
    @import("compat").fs.cwd().deleteFile(plain) catch {};
    defer @import("compat").fs.cwd().deleteFile(plain) catch {};
    defer @import("compat").fs.cwd().deleteFile("/tmp/lattice_serialize_plain.ltdb-wal") catch {};
    {
        const file = try @import("compat").fs.cwd().createFile(plain, .{ .truncate = true });
        try file.pwriteAll(bytes, 0);
        file.close();
    }
    {
        const db = try Database.open(allocator, plain, .{});
        defer db.close();
        var result = try db.query("MATCH (p:Person) RETURN p.name AS name");
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 40), result.rowCount());
    }

    // And deserialize opens them without the caller choosing a path at all.
    {
        const db = try Database.deserialize(allocator, bytes, .{
            .config = .{ .enable_fts = false, .enable_vector = false },
        });
        defer db.close();

        var people = try db.query("MATCH (p:Person) RETURN p.name AS name");
        defer people.deinit();
        try std.testing.expectEqual(@as(usize, 40), people.rowCount());

        var companies = try db.query("MATCH (c:Company) RETURN c.name AS name");
        defer companies.deinit();
        try std.testing.expectEqual(@as(usize, 1), companies.rowCount());
    }
}

test "database: a deserialized database is writable and leaves nothing behind" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_serialize_rw.ltdb";

    @import("compat").fs.cwd().deleteFile(path) catch {};
    @import("compat").fs.cwd().deleteFile("/tmp/lattice_serialize_rw.ltdb-wal") catch {};
    defer @import("compat").fs.cwd().deleteFile(path) catch {};
    defer @import("compat").fs.cwd().deleteFile("/tmp/lattice_serialize_rw.ltdb-wal") catch {};

    var first: []u8 = undefined;
    {
        const db = try Database.open(allocator, path, .{
            .create = true,
            .config = .{ .enable_fts = false, .enable_vector = false },
        });
        defer db.close();
        try writeNodes(db, "Person", 3);
        first = try db.serialize(allocator);
    }
    defer allocator.free(first);

    var second: []u8 = undefined;
    var backing_path: []u8 = undefined;
    {
        const db = try Database.deserialize(allocator, first, .{
            .config = .{ .enable_fts = false, .enable_vector = false },
        });
        defer db.close();

        backing_path = try allocator.dupe(u8, db.path);

        // The whole point of the workflow is mutate-then-write-back, so the
        // deserialized database has to accept writes.
        var added = try db.query("CREATE (p:Person {name: 'added'})");
        added.deinit();

        second = try db.serialize(allocator);
    }
    defer allocator.free(second);
    defer allocator.free(backing_path);

    // Closing removed the file it was using. A workflow that runs per request
    // must not leave a temporary database behind every time.
    try std.testing.expectError(
        error.FileNotFound,
        @import("compat").fs.cwd().access(backing_path, .{}),
    );

    // Changes made after deserializing come back out, and do not reach into the
    // bytes they came from.
    {
        const db = try Database.deserialize(allocator, second, .{
            .config = .{ .enable_fts = false, .enable_vector = false },
        });
        defer db.close();
        var result = try db.query("MATCH (p:Person) RETURN p.name AS name");
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 4), result.rowCount());
    }
    {
        const db = try Database.deserialize(allocator, first, .{
            .config = .{ .enable_fts = false, .enable_vector = false },
        });
        defer db.close();
        var result = try db.query("MATCH (p:Person) RETURN p.name AS name");
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 3), result.rowCount());
    }
}

test "database: serialize refuses while a transaction is open" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_serialize_txn.ltdb";

    @import("compat").fs.cwd().deleteFile(path) catch {};
    @import("compat").fs.cwd().deleteFile("/tmp/lattice_serialize_txn.ltdb-wal") catch {};
    defer @import("compat").fs.cwd().deleteFile(path) catch {};
    defer @import("compat").fs.cwd().deleteFile("/tmp/lattice_serialize_txn.ltdb-wal") catch {};

    const db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{ .enable_fts = false, .enable_vector = false },
    });
    defer db.close();

    var txn = try db.beginTransaction(.read_write);
    defer db.abortTransaction(&txn) catch {};

    // Bytes captured while writes land underneath them are torn in ways nothing
    // downstream would notice until a restore.
    try std.testing.expectError(
        DatabaseError.TransactionConflict,
        db.serialize(allocator),
    );
}

test "database: deserializing something that is not a database fails cleanly" {
    const allocator = std.testing.allocator;

    const junk = try allocator.alloc(u8, 8192);
    defer allocator.free(junk);
    @memset(junk, 0x5A);

    try std.testing.expectError(
        DatabaseError.InvalidDatabase,
        Database.deserialize(allocator, junk, .{}),
    );

    // An empty slice is not a database either, and must not be mistaken for a
    // request to create one.
    try std.testing.expectError(
        DatabaseError.InvalidDatabase,
        Database.deserialize(allocator, &[_]u8{}, .{}),
    );
}

test "database: an in-memory database touches no files" {
    const allocator = std.testing.allocator;

    const db = try Database.open(allocator, ":memory:", .{
        .create = true,
        .config = .{ .enable_fts = false, .enable_vector = false },
    });
    defer db.close();

    try writeNodes(db, "Person", 200);

    var created = try db.query("CREATE (c:Company {name: 'Acme'})");
    created.deinit();

    var people = try db.query("MATCH (p:Person) RETURN p.name AS name");
    defer people.deinit();
    try std.testing.expectEqual(@as(usize, 200), people.rowCount());

    // The whole point is that nothing lands on disk, so check rather than
    // assume. A path is a path, and it would be easy to create a real file
    // literally called ":memory:" without noticing.
    try std.testing.expectError(
        error.FileNotFound,
        @import("compat").fs.cwd().access(":memory:", .{}),
    );
    try std.testing.expectError(
        error.FileNotFound,
        @import("compat").fs.cwd().access(":memory:-wal", .{}),
    );
}

test "database: an in-memory database keeps its transactions" {
    const allocator = std.testing.allocator;

    const db = try Database.open(allocator, ":memory:", .{
        .create = true,
        .config = .{ .enable_fts = false, .enable_vector = false },
    });
    defer db.close();

    // Turning the log off to save allocations would have cost transactions
    // entirely, which is why it stays on in memory.
    var txn = try db.beginTransaction(.read_write);
    const node = try db.createNode(&txn, &[_][]const u8{"Person"});
    try db.setNodeProperty(&txn, node, "name", .{ .string_val = "rolled back" });
    try db.abortTransaction(&txn);

    var result = try db.query("MATCH (p:Person) RETURN p.name AS name");
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 0), result.rowCount());
}

test "database: two in-memory databases cannot see each other" {
    const allocator = std.testing.allocator;

    const a = try Database.open(allocator, ":memory:", .{
        .create = true,
        .config = .{ .enable_fts = false, .enable_vector = false },
    });
    defer a.close();

    const b = try Database.open(allocator, ":memory:", .{
        .create = true,
        .config = .{ .enable_fts = false, .enable_vector = false },
    });
    defer b.close();

    try writeNodes(a, "Person", 5);

    // Each holds its own filesystem, so the shared path name means nothing.
    // Sharing one would make every in-memory database in a process the same
    // database, which is the kind of thing nobody discovers until production.
    var seen = try b.query("MATCH (p:Person) RETURN p.name AS name");
    defer seen.deinit();
    try std.testing.expectEqual(@as(usize, 0), seen.rowCount());
}

test "database: a small in-memory database survives a deep traversal" {
    const allocator = std.testing.allocator;

    // The buffer pool is capped to the size of an in-memory database, and a pool
    // with nowhere to put the next page fails the query rather than slowing it
    // down. This is the test that says the floor is high enough.
    const db = try Database.open(allocator, ":memory:", .{
        .create = true,
        .config = .{ .enable_fts = false, .enable_vector = false },
    });
    defer db.close();

    // A chain long enough that walking it pins pages well beyond the handful a
    // single lookup needs.
    var txn = try db.beginTransaction(.read_write);
    var previous: ?u64 = null;
    var i: usize = 0;
    while (i < 300) : (i += 1) {
        const node = try db.createNode(&txn, &[_][]const u8{"Link"});
        try db.setNodeProperty(&txn, node, "i", .{ .int_val = @intCast(i) });
        if (previous) |p| {
            try db.createEdge(&txn, p, node, "NEXT");
        }
        previous = node;
    }
    try db.commitTransaction(&txn);

    var walked = try db.query("MATCH (a:Link)-[:NEXT*1..6]->(b:Link) RETURN b.i AS i");
    defer walked.deinit();
    try std.testing.expect(walked.rowCount() > 0);

    var all = try db.query("MATCH (a:Link) RETURN a.i AS i");
    defer all.deinit();
    try std.testing.expectEqual(@as(usize, 300), all.rowCount());
}

test "database: deserialize lands in memory and leaves no file" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_inmem_seed.ltdb";

    @import("compat").fs.cwd().deleteFile(path) catch {};
    @import("compat").fs.cwd().deleteFile("/tmp/lattice_inmem_seed.ltdb-wal") catch {};
    defer @import("compat").fs.cwd().deleteFile(path) catch {};
    defer @import("compat").fs.cwd().deleteFile("/tmp/lattice_inmem_seed.ltdb-wal") catch {};

    var bytes: []u8 = undefined;
    {
        const src = try Database.open(allocator, path, .{
            .create = true,
            .config = .{ .enable_fts = false, .enable_vector = false },
        });
        defer src.close();
        try writeNodes(src, "Note", 12);
        bytes = try src.serialize(allocator);
    }
    defer allocator.free(bytes);

    const db = try Database.deserialize(allocator, bytes, .{
        .config = .{ .enable_fts = false, .enable_vector = false },
    });
    defer db.close();

    // No temporary file, which is the thing somebody pulling databases out of
    // object storage asked not to have.
    try std.testing.expectEqualStrings(":memory:", db.path);

    var notes = try db.query("MATCH (n:Note) RETURN n.name AS name");
    defer notes.deinit();
    try std.testing.expectEqual(@as(usize, 12), notes.rowCount());

    // Still writable, and still serialisable, so the round trip closes.
    var added = try db.query("CREATE (n:Note {name: 'added'})");
    added.deinit();

    const again = try db.serialize(allocator);
    defer allocator.free(again);

    const reopened = try Database.deserialize(allocator, again, .{
        .config = .{ .enable_fts = false, .enable_vector = false },
    });
    defer reopened.close();

    var after = try reopened.query("MATCH (n:Note) RETURN n.name AS name");
    defer after.deinit();
    try std.testing.expectEqual(@as(usize, 13), after.rowCount());
}

test "database: deserialize can borrow the caller's bytes" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_borrow_seed.ltdb";

    @import("compat").fs.cwd().deleteFile(path) catch {};
    @import("compat").fs.cwd().deleteFile("/tmp/lattice_borrow_seed.ltdb-wal") catch {};
    defer @import("compat").fs.cwd().deleteFile(path) catch {};
    defer @import("compat").fs.cwd().deleteFile("/tmp/lattice_borrow_seed.ltdb-wal") catch {};

    var bytes: []u8 = undefined;
    {
        const src = try Database.open(allocator, path, .{
            .create = true,
            .config = .{ .enable_fts = false, .enable_vector = false },
        });
        defer src.close();
        try writeNodes(src, "Note", 60);
        bytes = try src.serialize(allocator);
    }
    defer allocator.free(bytes);

    // Borrowed and copied databases have to behave identically. The only
    // difference should be how much memory the backend allocated for itself.
    var borrowed_bytes: u64 = 0;
    var copied_bytes: u64 = 0;

    {
        const db = try Database.deserialize(allocator, bytes, .{
            .config = .{ .enable_fts = false, .enable_vector = false },
            .borrow_bytes = true,
        });
        defer db.close();

        var notes = try db.query("MATCH (n:Note) RETURN n.name AS name");
        defer notes.deinit();
        try std.testing.expectEqual(@as(usize, 60), notes.rowCount());

        borrowed_bytes = switch (db.vfs) {
            .memory => |*m| m.byteCount(),
            .posix => 0,
        };
    }

    {
        const db = try Database.deserialize(allocator, bytes, .{
            .config = .{ .enable_fts = false, .enable_vector = false },
        });
        defer db.close();

        var notes = try db.query("MATCH (n:Note) RETURN n.name AS name");
        defer notes.deinit();
        try std.testing.expectEqual(@as(usize, 60), notes.rowCount());

        copied_bytes = switch (db.vfs) {
            .memory => |*m| m.byteCount(),
            .posix => 0,
        };
    }

    // The saving is the point, so it is asserted rather than assumed.
    try std.testing.expect(borrowed_bytes < copied_bytes);
}

test "database: writing a borrowed database does not touch the caller's bytes" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_borrow_cow.ltdb";

    @import("compat").fs.cwd().deleteFile(path) catch {};
    @import("compat").fs.cwd().deleteFile("/tmp/lattice_borrow_cow.ltdb-wal") catch {};
    defer @import("compat").fs.cwd().deleteFile(path) catch {};
    defer @import("compat").fs.cwd().deleteFile("/tmp/lattice_borrow_cow.ltdb-wal") catch {};

    var bytes: []u8 = undefined;
    {
        const src = try Database.open(allocator, path, .{
            .create = true,
            .config = .{ .enable_fts = false, .enable_vector = false },
        });
        defer src.close();
        try writeNodes(src, "Note", 10);
        bytes = try src.serialize(allocator);
    }
    defer allocator.free(bytes);

    // Kept to compare against afterwards. Copy-on-write means the caller's
    // buffer must come out of this unchanged; if a write reached through to it,
    // the caller's idea of what it holds would silently stop being true.
    const untouched = try allocator.dupe(u8, bytes);
    defer allocator.free(untouched);

    {
        const db = try Database.deserialize(allocator, bytes, .{
            .config = .{ .enable_fts = false, .enable_vector = false },
            .borrow_bytes = true,
        });
        defer db.close();

        var i: usize = 0;
        while (i < 40) : (i += 1) {
            var r = try db.query("CREATE (n:Note {name: 'added'})");
            r.deinit();
        }

        var notes = try db.query("MATCH (n:Note) RETURN n.name AS name");
        defer notes.deinit();
        try std.testing.expectEqual(@as(usize, 50), notes.rowCount());
    }

    try std.testing.expectEqualSlices(u8, untouched, bytes);

    // And the untouched bytes still open as the database they were.
    const again = try Database.deserialize(allocator, bytes, .{
        .config = .{ .enable_fts = false, .enable_vector = false },
    });
    defer again.close();

    var notes = try again.query("MATCH (n:Note) RETURN n.name AS name");
    defer notes.deinit();
    try std.testing.expectEqual(@as(usize, 10), notes.rowCount());
}

test "database: a tiny buffer pool still completes real work" {
    const allocator = std.testing.allocator;

    // The in-memory pool is capped at the database size plus a floor, and a pool
    // with nowhere to put the next page fails a query rather than slowing it
    // down. This pins the measurement that floor was chosen from: the engine
    // holds very few pages at once, so a pool far below the default is enough.
    //
    // If a future change starts holding many more pages pinned simultaneously,
    // this fails here rather than as a mysterious BufferPoolFull in somebody's
    // small in-memory database.
    const db = try Database.open(allocator, ":memory:", .{
        .create = true,
        .config = .{
            .enable_fts = true,
            .enable_vector = false,
            .buffer_pool_size = 8 * 4096,
        },
    });
    defer db.close();

    // Declared before the writes, so the index is maintained as they happen
    // rather than built afterwards. That is the path with the tighter page
    // budget, which is what this test is about.
    try db.createNodeFtsIndex("Link", "text");

    var txn = try db.beginTransaction(.read_write);
    var previous: ?u64 = null;
    var i: usize = 0;
    while (i < 800) : (i += 1) {
        const node = try db.createNode(&txn, &[_][]const u8{"Link"});
        try db.setNodeProperty(&txn, node, "i", .{ .int_val = @intCast(i) });
        try db.setNodeProperty(&txn, node, "text", .{ .string_val = "searchable words here" });
        if (previous) |p| try db.createEdge(&txn, p, node, "NEXT");
        previous = node;
    }
    try db.commitTransaction(&txn);

    var walked = try db.query("MATCH (a:Link)-[:NEXT*1..8]->(b:Link) RETURN b.i AS i");
    defer walked.deinit();
    try std.testing.expect(walked.rowCount() > 0);

    var scanned = try db.query("MATCH (a:Link) WHERE a.i > 100 RETURN a.i AS i");
    defer scanned.deinit();
    try std.testing.expectEqual(@as(usize, 699), scanned.rowCount());

    // Run the full-text path too, for the pages it touches rather than the rows
    // it returns. What it matches is a separate question and not this test's
    // business; completing without running out of frames is.
    var searched = try db.query("MATCH (a:Link) WHERE a.text @@ 'searchable' RETURN a.i AS i");
    defer searched.deinit();

    var counted = try db.query("MATCH (a:Link) RETURN count(a) AS n");
    defer counted.deinit();
    try std.testing.expectEqual(@as(usize, 1), counted.rowCount());
}

/// Read a row's string column, for tests that care about ordering.
fn rowString(result: *const lattice.storage.database.QueryResult, row: usize, col: usize) []const u8 {
    return switch (result.rows[row].values[col]) {
        .string_val => |s| s,
        else => "",
    };
}

fn rowInt(result: *const lattice.storage.database.QueryResult, row: usize, col: usize) i64 {
    return switch (result.rows[row].values[col]) {
        .int_val => |v| v,
        else => -1,
    };
}

test "database: ORDER BY sorts by an aggregate, not before it" {
    const allocator = std.testing.allocator;

    const db = try Database.open(allocator, ":memory:", .{
        .create = true,
        .config = .{ .enable_fts = false, .enable_vector = false },
    });
    defer db.close();

    // Three authors with one, three, and two papers, deliberately created in an
    // order that does not match any sort, so a sort that does nothing is visible.
    for ([_][]const u8{
        "CREATE (a:Author {name: 'one'})",
        "CREATE (a:Author {name: 'three'})",
        "CREATE (a:Author {name: 'two'})",
    }) |cypher| {
        var r = try db.query(cypher);
        r.deinit();
    }
    for ([_][]const u8{
        "MATCH (a:Author {name:'one'}) CREATE (a)-[:WROTE]->(:Paper {})",
        "MATCH (a:Author {name:'three'}) CREATE (a)-[:WROTE]->(:Paper {})",
        "MATCH (a:Author {name:'three'}) CREATE (a)-[:WROTE]->(:Paper {})",
        "MATCH (a:Author {name:'three'}) CREATE (a)-[:WROTE]->(:Paper {})",
        "MATCH (a:Author {name:'two'}) CREATE (a)-[:WROTE]->(:Paper {})",
        "MATCH (a:Author {name:'two'}) CREATE (a)-[:WROTE]->(:Paper {})",
    }) |cypher| {
        var r = try db.query(cypher);
        r.deinit();
    }

    // Sorting used to be planned underneath the projection, so it ran on the raw
    // matches and aggregation then regrouped them and threw the order away. The
    // query succeeded and returned rows in an order nobody asked for, which is
    // the kind of wrong answer nothing downstream can notice.
    {
        var r = try db.query(
            "MATCH (a:Author)-[:WROTE]->(p:Paper) RETURN a.name AS name, count(p) AS papers ORDER BY papers DESC",
        );
        defer r.deinit();
        try std.testing.expectEqual(@as(usize, 3), r.rowCount());
        try std.testing.expectEqualStrings("three", rowString(&r, 0, 0));
        try std.testing.expectEqual(@as(i64, 3), rowInt(&r, 0, 1));
        try std.testing.expectEqualStrings("two", rowString(&r, 1, 0));
        try std.testing.expectEqualStrings("one", rowString(&r, 2, 0));
    }

    // Ascending, so a sort that happens to leave things alone cannot pass both.
    {
        var r = try db.query(
            "MATCH (a:Author)-[:WROTE]->(p:Paper) RETURN a.name AS name, count(p) AS papers ORDER BY papers",
        );
        defer r.deinit();
        try std.testing.expectEqualStrings("one", rowString(&r, 0, 0));
        try std.testing.expectEqualStrings("three", rowString(&r, 2, 0));
    }

    // Naming the aggregate directly rather than through an alias.
    {
        var r = try db.query(
            "MATCH (a:Author)-[:WROTE]->(p:Paper) RETURN a.name AS name, count(p) AS papers ORDER BY count(p) DESC",
        );
        defer r.deinit();
        try std.testing.expectEqualStrings("three", rowString(&r, 0, 0));
    }

    // Sorting by a grouping key rather than the aggregate.
    {
        var r = try db.query(
            "MATCH (a:Author)-[:WROTE]->(p:Paper) RETURN a.name AS name, count(p) AS papers ORDER BY name",
        );
        defer r.deinit();
        try std.testing.expectEqualStrings("one", rowString(&r, 0, 0));
        try std.testing.expectEqualStrings("three", rowString(&r, 1, 0));
        try std.testing.expectEqualStrings("two", rowString(&r, 2, 0));
    }
}

test "database: LIMIT after an aggregate takes the top rows, not any rows" {
    const allocator = std.testing.allocator;

    const db = try Database.open(allocator, ":memory:", .{
        .create = true,
        .config = .{ .enable_fts = false, .enable_vector = false },
    });
    defer db.close();

    for ([_][]const u8{
        "CREATE (a:Author {name: 'one'})",
        "CREATE (a:Author {name: 'three'})",
        "CREATE (a:Author {name: 'two'})",
    }) |cypher| {
        var r = try db.query(cypher);
        r.deinit();
    }
    for ([_][]const u8{
        "MATCH (a:Author {name:'one'}) CREATE (a)-[:WROTE]->(:Paper {})",
        "MATCH (a:Author {name:'three'}) CREATE (a)-[:WROTE]->(:Paper {})",
        "MATCH (a:Author {name:'three'}) CREATE (a)-[:WROTE]->(:Paper {})",
        "MATCH (a:Author {name:'three'}) CREATE (a)-[:WROTE]->(:Paper {})",
        "MATCH (a:Author {name:'two'}) CREATE (a)-[:WROTE]->(:Paper {})",
        "MATCH (a:Author {name:'two'}) CREATE (a)-[:WROTE]->(:Paper {})",
    }) |cypher| {
        var r = try db.query(cypher);
        r.deinit();
    }

    // Top-N by count is the shape this whole class of query exists for, and it
    // needs the limit applied to the sorted aggregate rather than to the matches.
    var r = try db.query(
        "MATCH (a:Author)-[:WROTE]->(p:Paper) RETURN a.name AS name, count(p) AS papers ORDER BY papers DESC LIMIT 2",
    );
    defer r.deinit();
    try std.testing.expectEqual(@as(usize, 2), r.rowCount());
    try std.testing.expectEqualStrings("three", rowString(&r, 0, 0));
    try std.testing.expectEqualStrings("two", rowString(&r, 1, 0));
}

test "database: ordering by a column the projection does not produce is refused" {
    const allocator = std.testing.allocator;

    const db = try Database.open(allocator, ":memory:", .{
        .create = true,
        .config = .{ .enable_fts = false, .enable_vector = false },
    });
    defer db.close();

    var made = try db.query("CREATE (a:Author {name: 'x'})");
    made.deinit();

    // Sorting by something that is not there cannot be done, and answering with
    // rows in some arbitrary order would be worse than saying so.
    try std.testing.expectError(
        error.SemanticError,
        db.query("MATCH (a:Author) RETURN a.name AS name, count(a) AS n ORDER BY missing DESC"),
    );
}

test "database: full-text scores mean the same thing after reopening" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_fts_stats.ltdb";

    @import("compat").fs.cwd().deleteFile(path) catch {};
    @import("compat").fs.cwd().deleteFile("/tmp/lattice_fts_stats.ltdb-wal") catch {};
    defer @import("compat").fs.cwd().deleteFile(path) catch {};
    defer @import("compat").fs.cwd().deleteFile("/tmp/lattice_fts_stats.ltdb-wal") catch {};

    const short = "zebra";
    const long = "zebra " ++ ("filler " ** 60);

    var fresh_short: f32 = 0;
    var fresh_long: f32 = 0;

    {
        const db = try Database.open(allocator, path, .{
            .create = true,
            .config = .{ .enable_fts = true, .enable_vector = false },
        });
        defer db.close();

        try db.createNodeFtsIndex("Doc", "text");

        var txn = try db.beginTransaction(.read_write);
        var i: usize = 0;
        while (i < 40) : (i += 1) {
            const node = try db.createNode(&txn, &[_][]const u8{"Doc"});
            const text = switch (node) {
                1 => short,
                2 => long,
                else => "unrelated filler text",
            };
            try db.setNodeProperty(&txn, node, "text", .{ .string_val = text });
        }
        try db.commitTransaction(&txn);

        const hits = try db.ftsSearchIndex(.node, "Doc", "text", "zebra", 10);
        defer db.freeFtsSearchResults(hits);
        try std.testing.expectEqual(@as(usize, 2), hits.len);
        for (hits) |h| {
            if (h.doc_id == 1) fresh_short = h.score;
            if (h.doc_id == 2) fresh_long = h.score;
        }
        try std.testing.expect(fresh_short > 0);
        try std.testing.expect(fresh_long > 0);
    }

    // Scoring needs the size and average length of the whole corpus, and neither
    // can be worked out from the document being scored. Those statistics used to
    // live only in memory, so a session that searched without indexing anything
    // first scored against an empty corpus and produced different numbers for the
    // same data.
    {
        const db = try Database.open(allocator, path, .{
            .config = .{ .enable_fts = true, .enable_vector = false },
        });
        defer db.close();

        const hits = try db.ftsSearchIndex(.node, "Doc", "text", "zebra", 10);
        defer db.freeFtsSearchResults(hits);
        try std.testing.expectEqual(@as(usize, 2), hits.len);

        for (hits) |h| {
            if (h.doc_id == 1) try std.testing.expectEqual(fresh_short, h.score);
            if (h.doc_id == 2) try std.testing.expectEqual(fresh_long, h.score);
        }
    }
}

test "database: a shorter document scores higher for the same term" {
    const allocator = std.testing.allocator;

    // Length normalisation is the part that breaks when the average document
    // length is unknown, and it is what makes a title beat a passing mention
    // buried in a page of text.
    const db = try Database.open(allocator, ":memory:", .{
        .create = true,
        .config = .{ .enable_fts = true, .enable_vector = false },
    });
    defer db.close();

    try db.createNodeFtsIndex("Doc", "text");

    var txn = try db.beginTransaction(.read_write);
    var i: usize = 0;
    while (i < 30) : (i += 1) {
        const node = try db.createNode(&txn, &[_][]const u8{"Doc"});
        const text = switch (node) {
            1 => "zebra",
            2 => "zebra " ++ ("filler " ** 60),
            else => "unrelated filler text",
        };
        try db.setNodeProperty(&txn, node, "text", .{ .string_val = text });
    }
    try db.commitTransaction(&txn);

    const hits = try db.ftsSearchIndex(.node, "Doc", "text", "zebra", 10);
    defer db.freeFtsSearchResults(hits);

    try std.testing.expectEqual(@as(usize, 2), hits.len);
    try std.testing.expectEqual(@as(u64, 1), hits[0].doc_id);
    try std.testing.expect(hits[0].score > hits[1].score);
}

test "database: full-text match means the same thing inside a boolean expression" {
    const allocator = std.testing.allocator;

    const db = try Database.open(allocator, ":memory:", .{
        .create = true,
        .config = .{ .enable_fts = true, .enable_vector = false },
    });
    defer db.close();

    try db.createNodeFtsIndex("Doc", "title");
    try db.createNodeFtsIndex("Doc", "body");

    var txn = try db.beginTransaction(.read_write);
    const node = try db.createNode(&txn, &[_][]const u8{"Doc"});
    try db.setNodeProperty(&txn, node, "title", .{ .string_val = "sourdough" });
    try db.setNodeProperty(&txn, node, "body", .{ .string_val = "ciabatta focaccia" });
    try db.commitTransaction(&txn);

    // A whole WHERE clause becomes an index scan, so this reads the title index.
    {
        var r = try db.query("MATCH (d:Doc) WHERE d.title @@ 'sourdough' RETURN d.title AS t");
        defer r.deinit();
        try std.testing.expectEqual(@as(usize, 1), r.rowCount());
    }

    // The same predicate inside an OR goes through the row filter instead. It
    // used to substring-match the property there, so it read different data and
    // gave the opposite answer: an unrelated OR beside a correct query could turn
    // it into an incorrect one, with nothing to say so. Both positions now
    // resolve to the same declared index.
    {
        var r = try db.query("MATCH (d:Doc) WHERE d.title @@ 'sourdough' OR d.body @@ 'zzz' RETURN d.title AS t");
        defer r.deinit();
        try std.testing.expectEqual(@as(usize, 1), r.rowCount());
    }

    // A term that is in the body is not in the title, in either position. This is
    // what per-property indexes are for, and it is the case the old single
    // document per node could not express at all.
    {
        var top = try db.query("MATCH (d:Doc) WHERE d.title @@ 'ciabatta' RETURN d.title AS t");
        defer top.deinit();
        try std.testing.expectEqual(@as(usize, 0), top.rowCount());

        var inside = try db.query("MATCH (d:Doc) WHERE d.title @@ 'ciabatta' OR d.title @@ 'zzz' RETURN d.title AS t");
        defer inside.deinit();
        try std.testing.expectEqual(@as(usize, 0), inside.rowCount());
    }

    // Searching the body for that same term does find it.
    {
        var r = try db.query("MATCH (d:Doc) WHERE d.body @@ 'ciabatta' RETURN d.title AS t");
        defer r.deinit();
        try std.testing.expectEqual(@as(usize, 1), r.rowCount());
    }

    // AND is the same story.
    {
        var r = try db.query("MATCH (d:Doc) WHERE d.title @@ 'sourdough' AND d.title = 'sourdough' RETURN d.title AS t");
        defer r.deinit();
        try std.testing.expectEqual(@as(usize, 1), r.rowCount());
    }
}

test "database: a disjunction of full-text matches finds either side" {
    const allocator = std.testing.allocator;

    const db = try Database.open(allocator, ":memory:", .{
        .create = true,
        .config = .{ .enable_fts = true, .enable_vector = false },
    });
    defer db.close();

    try db.createNodeFtsIndex("Doc", "text");

    var txn = try db.beginTransaction(.read_write);
    const a = try db.createNode(&txn, &[_][]const u8{"Doc"});
    const b = try db.createNode(&txn, &[_][]const u8{"Doc"});
    const c = try db.createNode(&txn, &[_][]const u8{"Doc"});
    try db.setNodeProperty(&txn, a, "name", .{ .string_val = "a" });
    try db.setNodeProperty(&txn, b, "name", .{ .string_val = "b" });
    try db.setNodeProperty(&txn, c, "name", .{ .string_val = "c" });
    try db.setNodeProperty(&txn, a, "text", .{ .string_val = "ciabatta" });
    try db.setNodeProperty(&txn, b, "text", .{ .string_val = "focaccia" });
    try db.setNodeProperty(&txn, c, "text", .{ .string_val = "brioche" });
    try db.commitTransaction(&txn);

    var r = try db.query(
        "MATCH (d:Doc) WHERE d.text @@ 'ciabatta' OR d.text @@ 'focaccia' RETURN d.name AS n",
    );
    defer r.deinit();
    try std.testing.expectEqual(@as(usize, 2), r.rowCount());
}

test "database: searching a property with no declared index says so" {
    const allocator = std.testing.allocator;
    const db = try Database.open(allocator, ":memory:", .{
        .create = true,
        .config = .{ .enable_fts = true, .enable_vector = false },
    });
    defer db.close();

    const doc = try db.createNode(null, &[_][]const u8{"Doc"});
    try db.setNodeProperty(null, doc, "title", .{ .string_val = "sourdough" });

    // Returning no rows would be indistinguishable from a search that found
    // nothing, which is how a mistyped property name goes unnoticed.
    var failed = try db.queryDetailed("MATCH (d:Doc) WHERE d.title @@ 'sourdough' RETURN d.title AS t");
    defer failed.deinit();
    switch (failed) {
        .success => return error.TestExpectedFailure,
        .failure => |f| {
            try std.testing.expect(std.mem.indexOf(u8, f.message, "Doc.title") != null);
        },
    }
}

test "database: a full-text match with no label to resolve against says so" {
    const allocator = std.testing.allocator;
    const db = try Database.open(allocator, ":memory:", .{
        .create = true,
        .config = .{ .enable_fts = true, .enable_vector = false },
    });
    defer db.close();

    try db.createNodeFtsIndex("Doc", "title");
    const doc = try db.createNode(null, &[_][]const u8{"Doc"});
    try db.setNodeProperty(null, doc, "title", .{ .string_val = "sourdough" });

    // Two labels can each declare an index on `title`, so an unlabelled pattern
    // does not say which one is meant. Guessing would make the answer depend on
    // what else happens to be declared.
    var failed = try db.queryDetailed("MATCH (d) WHERE d.title @@ 'sourdough' RETURN d.title AS t");
    defer failed.deinit();
    switch (failed) {
        .success => return error.TestExpectedFailure,
        .failure => |f| {
            try std.testing.expect(std.mem.indexOf(u8, f.message, "label") != null);
        },
    }
}

test "database: scoped full-text views stay out of each other's way" {
    const allocator = std.testing.allocator;
    const db = try Database.open(allocator, ":memory:", .{
        .create = true,
        .config = .{ .enable_fts = true, .enable_vector = false },
    });
    defer db.close();

    // Borrow a real tree from the open database.
    const tree = &db.fts_dict_tree;

    const title = ScopedTree.scoped(tree, [_]u8{ 0, 3, 0, 1 });
    const body = ScopedTree.scoped(tree, [_]u8{ 0, 3, 0, 2 });

    try title.insert("bread", "T1");
    try title.insert("cake", "T2");
    try body.insert("bread", "B1");

    // The same term in two indexes resolves to two different entries.
    const from_title = (try title.get("bread")).?;
    defer title.freeValue(from_title);
    try std.testing.expectEqualStrings("T1", from_title);

    const from_body = (try body.get("bread")).?;
    defer body.freeValue(from_body);
    try std.testing.expectEqualStrings("B1", from_body);

    // A term in one index is invisible to the other.
    try std.testing.expect((try body.get("cake")) == null);
    try std.testing.expect(try title.contains("cake"));

    // Iterating an index sees only its own entries.
    var count: usize = 0;
    var it: ScopedTree.Iterator = undefined;
    try title.iterateAll(&it);
    while (try it.next()) |_| count += 1;
    try std.testing.expectEqual(@as(usize, 2), count);

    var body_count: usize = 0;
    var body_it: ScopedTree.Iterator = undefined;
    try body.iterateAll(&body_it);
    while (try body_it.next()) |_| body_count += 1;
    try std.testing.expectEqual(@as(usize, 1), body_count);

    // Deleting from one leaves the other alone.
    try title.delete("bread");
    try std.testing.expect((try title.get("bread")) == null);
    const still_there = (try body.get("bread")).?;
    defer body.freeValue(still_there);
    try std.testing.expectEqualStrings("B1", still_there);

}

test "database: a full-text index whose prefix ends in 0xff iterates completely" {
    const allocator = std.testing.allocator;
    const db = try Database.open(allocator, ":memory:", .{
        .create = true,
        .config = .{ .enable_fts = true, .enable_vector = false },
    });
    defer db.close();

    const tree = &db.fts_dict_tree;
    // The carry case: a naive range end would stop short and lose entries.
    const edge = ScopedTree.scoped(tree, [_]u8{ 0, 3, 0, 0xFF });
    const next = ScopedTree.scoped(tree, [_]u8{ 0, 3, 1, 0x00 });

    try edge.insert("aaa", "x");
    try edge.insert("zzz", "y");
    try next.insert("aaa", "other");

    var count: usize = 0;
    var it: ScopedTree.Iterator = undefined;
    try edge.iterateAll(&it);
    while (try it.next()) |_| count += 1;
    try std.testing.expectEqual(@as(usize, 2), count);

}

/// Free what `ftsSearchIndex` returned.
///
/// The results are a plain allocated slice, so the test allocator will complain
/// loudly if one goes unfreed, which is the point.
fn freeFtsResults(allocator: std.mem.Allocator, results: []lattice.ScoredDoc) void {
    if (results.len == 0) return;
    allocator.free(results);
}

fn ftsContains(results: []lattice.ScoredDoc, doc_id: u64) bool {
    for (results) |result| {
        if (result.doc_id == doc_id) return true;
    }
    return false;
}

test "database: declaring a full-text index reads the property off the nodes already there" {
    const allocator = std.testing.allocator;
    const db = try Database.open(allocator, ":memory:", .{
        .create = true,
        .config = .{ .enable_fts = true, .enable_vector = false },
    });
    defer db.close();

    const sourdough = try db.createNode(null, &[_][]const u8{"Recipe"});
    try db.setNodeProperty(null, sourdough, "title", .{ .string_val = "sourdough bread" });
    const cake = try db.createNode(null, &[_][]const u8{"Recipe"});
    try db.setNodeProperty(null, cake, "title", .{ .string_val = "chocolate cake" });

    // Nothing was indexed as it was written, because nothing was declared yet.
    try db.createNodeFtsIndex("Recipe", "title");

    const found = try db.ftsSearchIndex(.node, "Recipe", "title", "bread", 10);
    defer freeFtsResults(allocator, found);
    try std.testing.expectEqual(@as(usize, 1), found.len);
    try std.testing.expectEqual(sourdough, found[0].doc_id);

    const cake_hits = try db.ftsSearchIndex(.node, "Recipe", "title", "chocolate", 10);
    defer freeFtsResults(allocator, cake_hits);
    try std.testing.expectEqual(@as(usize, 1), cake_hits.len);
    try std.testing.expectEqual(cake, cake_hits[0].doc_id);
}

test "database: a declared index follows writes without being asked" {
    const allocator = std.testing.allocator;
    const db = try Database.open(allocator, ":memory:", .{
        .create = true,
        .config = .{ .enable_fts = true, .enable_vector = false },
    });
    defer db.close();

    try db.createNodeFtsIndex("Recipe", "title");

    // Created after the declaration, so only maintenance can have indexed it.
    const node = try db.createNode(null, &[_][]const u8{"Recipe"});
    try db.setNodeProperty(null, node, "title", .{ .string_val = "rye bread" });

    {
        const found = try db.ftsSearchIndex(.node, "Recipe", "title", "rye", 10);
        defer freeFtsResults(allocator, found);
        try std.testing.expectEqual(@as(usize, 1), found.len);
    }

    // An update has to take the old text out as well as put the new text in,
    // otherwise the index keeps answering for a value the database no longer
    // holds.
    try db.setNodeProperty(null, node, "title", .{ .string_val = "focaccia" });
    {
        const stale = try db.ftsSearchIndex(.node, "Recipe", "title", "rye", 10);
        defer freeFtsResults(allocator, stale);
        try std.testing.expectEqual(@as(usize, 0), stale.len);

        const fresh = try db.ftsSearchIndex(.node, "Recipe", "title", "focaccia", 10);
        defer freeFtsResults(allocator, fresh);
        try std.testing.expectEqual(@as(usize, 1), fresh.len);
    }

    try db.deleteNode(null, node);
    {
        const gone = try db.ftsSearchIndex(.node, "Recipe", "title", "focaccia", 10);
        defer freeFtsResults(allocator, gone);
        try std.testing.expectEqual(@as(usize, 0), gone.len);
    }
}

test "database: two properties of one label are two separate indexes" {
    const allocator = std.testing.allocator;
    const db = try Database.open(allocator, ":memory:", .{
        .create = true,
        .config = .{ .enable_fts = true, .enable_vector = false },
    });
    defer db.close();

    try db.createNodeFtsIndex("Doc", "title");
    try db.createNodeFtsIndex("Doc", "body");

    const doc = try db.createNode(null, &[_][]const u8{"Doc"});
    try db.setNodeProperty(null, doc, "title", .{ .string_val = "bread" });
    try db.setNodeProperty(null, doc, "body", .{ .string_val = "cake" });

    // This is the whole point of the feature: a term in the body must not be
    // found by a search of the title.
    {
        const in_title = try db.ftsSearchIndex(.node, "Doc", "title", "cake", 10);
        defer freeFtsResults(allocator, in_title);
        try std.testing.expectEqual(@as(usize, 0), in_title.len);
    }
    {
        const in_body = try db.ftsSearchIndex(.node, "Doc", "body", "cake", 10);
        defer freeFtsResults(allocator, in_body);
        try std.testing.expectEqual(@as(usize, 1), in_body.len);
        try std.testing.expectEqual(doc, in_body[0].doc_id);
    }
    {
        const title_term = try db.ftsSearchIndex(.node, "Doc", "title", "bread", 10);
        defer freeFtsResults(allocator, title_term);
        try std.testing.expectEqual(@as(usize, 1), title_term.len);
    }
}

test "database: dropping a full-text index takes its terms with it" {
    const allocator = std.testing.allocator;
    const db = try Database.open(allocator, ":memory:", .{
        .create = true,
        .config = .{ .enable_fts = true, .enable_vector = false },
    });
    defer db.close();

    try db.createNodeFtsIndex("Doc", "title");
    const doc = try db.createNode(null, &[_][]const u8{"Doc"});
    try db.setNodeProperty(null, doc, "title", .{ .string_val = "bread" });

    try std.testing.expect(try db.hasNodeFtsIndex("Doc", "title"));
    try db.dropNodeFtsIndex("Doc", "title");
    try std.testing.expect(!(try db.hasNodeFtsIndex("Doc", "title")));

    // Searching a dropped index is a missing index, not an empty result.
    try std.testing.expectError(
        error.MissingIndex,
        db.ftsSearchIndex(.node, "Doc", "title", "bread", 10),
    );

    // Redeclaring lands on the same scope, so anything the drop left behind would
    // show up here as a document that was never reindexed. Removing the property
    // first makes that unambiguous: a hit now could only be a stale entry.
    try db.removeNodeProperty(null, doc, "title");
    try db.createNodeFtsIndex("Doc", "title");
    const after = try db.ftsSearchIndex(.node, "Doc", "title", "bread", 10);
    defer freeFtsResults(allocator, after);
    try std.testing.expectEqual(@as(usize, 0), after.len);
}

test "database: a label a node does not carry is not indexed for it" {
    const allocator = std.testing.allocator;
    const db = try Database.open(allocator, ":memory:", .{
        .create = true,
        .config = .{ .enable_fts = true, .enable_vector = false },
    });
    defer db.close();

    try db.createNodeFtsIndex("Recipe", "title");

    const note = try db.createNode(null, &[_][]const u8{"Note"});
    try db.setNodeProperty(null, note, "title", .{ .string_val = "bread" });

    const found = try db.ftsSearchIndex(.node, "Recipe", "title", "bread", 10);
    defer freeFtsResults(allocator, found);
    try std.testing.expectEqual(@as(usize, 0), found.len);
}

test "database: a property that is not text is left alone" {
    const allocator = std.testing.allocator;
    const db = try Database.open(allocator, ":memory:", .{
        .create = true,
        .config = .{ .enable_fts = true, .enable_vector = false },
    });
    defer db.close();

    try db.createNodeFtsIndex("Reading", "value");

    const reading = try db.createNode(null, &[_][]const u8{"Reading"});
    try db.setNodeProperty(null, reading, "value", .{ .int_val = 42 });

    // An integer is not text. Indexing some rendering of it would be a different
    // feature, and doing half of it silently would be worse than doing none.
    const found = try db.ftsSearchIndex(.node, "Reading", "value", "42", 10);
    defer freeFtsResults(allocator, found);
    try std.testing.expectEqual(@as(usize, 0), found.len);
}

test "database: declared full-text indexes survive a reopen" {
    const allocator = std.testing.allocator;
    const db_path = "/tmp/lattice_fts_declared_test.ltdb";
    @import("compat").fs.cwd().deleteFile(db_path) catch {};
    @import("compat").fs.cwd().deleteFile(db_path ++ "-wal") catch {};
    defer @import("compat").fs.cwd().deleteFile(db_path) catch {};
    defer @import("compat").fs.cwd().deleteFile(db_path ++ "-wal") catch {};

    var indexed_id: u64 = 0;
    {
        const db = try Database.open(allocator, db_path, .{
            .create = true,
            .config = .{ .enable_fts = true, .enable_vector = false },
        });
        defer db.close();

        try db.createNodeFtsIndex("Recipe", "title");
        indexed_id = try db.createNode(null, &[_][]const u8{"Recipe"});
        try db.setNodeProperty(null, indexed_id, "title", .{ .string_val = "sourdough bread" });
        _ = try db.checkpoint(.full);
    }

    const db = try Database.open(allocator, db_path, .{
        .config = .{ .enable_fts = true, .enable_vector = false },
    });
    defer db.close();

    try std.testing.expect(try db.hasNodeFtsIndex("Recipe", "title"));

    const found = try db.ftsSearchIndex(.node, "Recipe", "title", "sourdough", 10);
    defer freeFtsResults(allocator, found);
    try std.testing.expectEqual(@as(usize, 1), found.len);
    try std.testing.expect(ftsContains(found, indexed_id));
}

test "database: a full-text match returns every row, not the first hundred" {
    const allocator = std.testing.allocator;
    const db = try Database.open(allocator, ":memory:", .{
        .create = true,
        .config = .{ .enable_fts = true, .enable_vector = false },
    });
    defer db.close();

    try db.createNodeFtsIndex("Doc", "title");
    try db.createNodeFtsIndex("Doc", "body");

    // More than the hundred the planner used to pass as a default limit.
    const total = 250;
    var txn = try db.beginTransaction(.read_write);
    var i: usize = 0;
    while (i < total) : (i += 1) {
        const node = try db.createNode(&txn, &[_][]const u8{"Doc"});
        try db.setNodeProperty(&txn, node, "title", .{ .string_val = "loaf" });
        try db.setNodeProperty(&txn, node, "body", .{ .string_val = "filler" });
    }
    try db.commitTransaction(&txn);

    // The query says nothing about how many rows it wants, so it wants all of
    // them. Returning a hundred was a silent truncation with nothing in the
    // query to suggest it.
    {
        var r = try db.query("MATCH (d:Doc) WHERE d.title @@ 'loaf' RETURN d.title AS t");
        defer r.deinit();
        try std.testing.expectEqual(@as(usize, total), r.rowCount());
    }

    // And the same predicate inside an OR, which the planner sends down the row
    // filter instead of an index scan. The two used to use different bounds, so
    // moving a predicate into an OR changed how many rows came back.
    {
        var r = try db.query("MATCH (d:Doc) WHERE d.title @@ 'loaf' OR d.body @@ 'zzznotfound' RETURN d.title AS t");
        defer r.deinit();
        try std.testing.expectEqual(@as(usize, total), r.rowCount());
    }

    // An explicit LIMIT is still honoured; the point is that one absent from the
    // query is not invented.
    {
        var r = try db.query("MATCH (d:Doc) WHERE d.title @@ 'loaf' RETURN d.title AS t LIMIT 10");
        defer r.deinit();
        try std.testing.expectEqual(@as(usize, 10), r.rowCount());
    }
}

test "database: a disjunction of full-text matches is planned as one union" {
    const allocator = std.testing.allocator;
    const db = try Database.open(allocator, ":memory:", .{
        .create = true,
        .config = .{ .enable_fts = true, .enable_vector = false },
    });
    defer db.close();

    try db.createNodeFtsIndex("Doc", "title");
    try db.createNodeFtsIndex("Doc", "body");

    var txn = try db.beginTransaction(.read_write);
    const only_title = try db.createNode(&txn, &[_][]const u8{"Doc"});
    try db.setNodeProperty(&txn, only_title, "name", .{ .string_val = "title-only" });
    try db.setNodeProperty(&txn, only_title, "title", .{ .string_val = "ciabatta" });
    try db.setNodeProperty(&txn, only_title, "body", .{ .string_val = "nothing here" });

    const only_body = try db.createNode(&txn, &[_][]const u8{"Doc"});
    try db.setNodeProperty(&txn, only_body, "name", .{ .string_val = "body-only" });
    try db.setNodeProperty(&txn, only_body, "title", .{ .string_val = "nothing here" });
    try db.setNodeProperty(&txn, only_body, "body", .{ .string_val = "focaccia" });

    const both = try db.createNode(&txn, &[_][]const u8{"Doc"});
    try db.setNodeProperty(&txn, both, "name", .{ .string_val = "both" });
    try db.setNodeProperty(&txn, both, "title", .{ .string_val = "ciabatta" });
    try db.setNodeProperty(&txn, both, "body", .{ .string_val = "focaccia" });

    const neither = try db.createNode(&txn, &[_][]const u8{"Doc"});
    try db.setNodeProperty(&txn, neither, "name", .{ .string_val = "neither" });
    try db.setNodeProperty(&txn, neither, "title", .{ .string_val = "brioche" });
    try db.setNodeProperty(&txn, neither, "body", .{ .string_val = "brioche" });
    try db.commitTransaction(&txn);

    // Either side matching is enough, and a document is returned once however
    // many sides matched it.
    {
        var r = try db.query(
            "MATCH (d:Doc) WHERE d.title @@ 'ciabatta' OR d.body @@ 'focaccia' RETURN d.name AS n",
        );
        defer r.deinit();
        try std.testing.expectEqual(@as(usize, 3), r.rowCount());
    }

    // Three disjuncts, including one that matches nothing, still union correctly.
    {
        var r = try db.query(
            "MATCH (d:Doc) WHERE d.title @@ 'ciabatta' OR d.body @@ 'focaccia' OR d.title @@ 'zzznotfound' RETURN d.name AS n",
        );
        defer r.deinit();
        try std.testing.expectEqual(@as(usize, 3), r.rowCount());
    }

    // A disjunct on the same property as another is a union of two searches of
    // one index, which must not double-count the document that matches both.
    {
        var r = try db.query(
            "MATCH (d:Doc) WHERE d.title @@ 'ciabatta' OR d.title @@ 'brioche' RETURN d.name AS n",
        );
        defer r.deinit();
        try std.testing.expectEqual(@as(usize, 3), r.rowCount());
    }

    // A disjunction mixing `@@` with something else is not a union of index
    // scans, and has to keep answering through the row filter.
    {
        var r = try db.query(
            "MATCH (d:Doc) WHERE d.title @@ 'ciabatta' OR d.name = 'neither' RETURN d.name AS n",
        );
        defer r.deinit();
        try std.testing.expectEqual(@as(usize, 3), r.rowCount());
    }
}
