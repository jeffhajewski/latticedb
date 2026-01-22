//! Behavioral tests for Graph Layer.
//!
//! These tests verify the contracts of the graph storage components:
//! - Symbol Table: string interning and deduplication
//! - Node Store: node CRUD operations
//! - Edge Store: edge storage with double-write pattern
//! - Label Index: secondary index for label-based queries

const std = @import("std");
const lattice = @import("lattice");
const helpers = @import("helpers.zig");

const symbols_mod = lattice.graph.symbols;
const node_mod = lattice.graph.node;
const edge_mod = lattice.graph.edge;
const label_index_mod = lattice.graph.label_index;
const types = lattice.core.types;
const btree = lattice.storage.btree;

const Allocator = std.mem.Allocator;
const SymbolTable = symbols_mod.SymbolTable;
const SymbolError = symbols_mod.SymbolError;
const SymbolId = symbols_mod.SymbolId;
const NodeStore = node_mod.NodeStore;
const NodeError = node_mod.NodeError;
const Node = node_mod.Node;
const Property = node_mod.Property;
const EdgeStore = edge_mod.EdgeStore;
const EdgeError = edge_mod.EdgeError;
const EdgeRef = edge_mod.EdgeRef;
const BatchEdgeResults = edge_mod.BatchEdgeResults;
const LabelIndex = label_index_mod.LabelIndex;
const PropertyValue = types.PropertyValue;
const NodeId = types.NodeId;
const BTree = btree.BTree;

// ============================================================================
// Symbol Table Tests
// ============================================================================

test "symbols: same string always returns same id" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.init(allocator, "sym_same");
    defer db.deinit();

    var forward_tree = try BTree.init(allocator, db.bp);
    var reverse_tree = try BTree.init(allocator, db.bp);
    var table = SymbolTable.init(allocator, &forward_tree, &reverse_tree);

    // Intern a string
    const id1 = try table.intern("Person");
    // Intern again - should return same ID
    const id2 = try table.intern("Person");
    const id3 = try table.intern("Person");

    try std.testing.expectEqual(id1, id2);
    try std.testing.expectEqual(id2, id3);
}

test "symbols: different strings get different ids" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.init(allocator, "sym_diff");
    defer db.deinit();

    var forward_tree = try BTree.init(allocator, db.bp);
    var reverse_tree = try BTree.init(allocator, db.bp);
    var table = SymbolTable.init(allocator, &forward_tree, &reverse_tree);

    const id1 = try table.intern("Person");
    const id2 = try table.intern("Company");
    const id3 = try table.intern("Location");

    try std.testing.expect(id1 != id2);
    try std.testing.expect(id2 != id3);
    try std.testing.expect(id1 != id3);
}

test "symbols: id resolves back to original string" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.init(allocator, "sym_resolve");
    defer db.deinit();

    var forward_tree = try BTree.init(allocator, db.bp);
    var reverse_tree = try BTree.init(allocator, db.bp);
    var table = SymbolTable.init(allocator, &forward_tree, &reverse_tree);

    const test_strings = [_][]const u8{ "Person", "name", "KNOWS", "email" };
    var ids: [4]SymbolId = undefined;

    // Intern all strings
    for (test_strings, 0..) |s, i| {
        ids[i] = try table.intern(s);
    }

    // Resolve and verify each
    for (test_strings, 0..) |expected, i| {
        const resolved = try table.resolve(ids[i]);
        defer table.freeString(resolved);
        try std.testing.expectEqualStrings(expected, resolved);
    }
}

test "symbols: empty string handled" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.init(allocator, "sym_empty");
    defer db.deinit();

    var forward_tree = try BTree.init(allocator, db.bp);
    var reverse_tree = try BTree.init(allocator, db.bp);
    var table = SymbolTable.init(allocator, &forward_tree, &reverse_tree);

    // Empty string should be valid
    const id = try table.intern("");

    // Should be able to resolve back
    const resolved = try table.resolve(id);
    defer table.freeString(resolved);
    try std.testing.expectEqualStrings("", resolved);

    // Interning again should return same ID
    try std.testing.expectEqual(id, try table.intern(""));
}

test "symbols: unicode strings interned correctly" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.init(allocator, "sym_unicode");
    defer db.deinit();

    var forward_tree = try BTree.init(allocator, db.bp);
    var reverse_tree = try BTree.init(allocator, db.bp);
    var table = SymbolTable.init(allocator, &forward_tree, &reverse_tree);

    const unicode_strings = [_][]const u8{
        "日本語",      // Japanese
        "Ελληνικά",  // Greek
        "العربية",     // Arabic
        "emoji🎉",   // Emoji
    };

    for (unicode_strings) |s| {
        const id = try table.intern(s);
        const resolved = try table.resolve(id);
        defer table.freeString(resolved);
        try std.testing.expectEqualStrings(s, resolved);
    }
}

test "symbols: contains returns correct status" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.init(allocator, "sym_contains");
    defer db.deinit();

    var forward_tree = try BTree.init(allocator, db.bp);
    var reverse_tree = try BTree.init(allocator, db.bp);
    var table = SymbolTable.init(allocator, &forward_tree, &reverse_tree);

    try std.testing.expect(!table.contains("test"));

    _ = try table.intern("test");

    try std.testing.expect(table.contains("test"));
    try std.testing.expect(!table.contains("other"));
}

test "symbols: count reflects actual interned symbols" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.init(allocator, "sym_count");
    defer db.deinit();

    var forward_tree = try BTree.init(allocator, db.bp);
    var reverse_tree = try BTree.init(allocator, db.bp);
    var table = SymbolTable.init(allocator, &forward_tree, &reverse_tree);

    try std.testing.expectEqual(@as(u32, 0), table.count());

    _ = try table.intern("one");
    try std.testing.expectEqual(@as(u32, 1), table.count());

    _ = try table.intern("two");
    try std.testing.expectEqual(@as(u32, 2), table.count());

    // Re-interning doesn't increase count
    _ = try table.intern("one");
    try std.testing.expectEqual(@as(u32, 2), table.count());
}

// ============================================================================
// Node Store Tests
// ============================================================================

test "nodes: create returns incrementing ids" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.init(allocator, "node_ids");
    defer db.deinit();

    var tree = try BTree.init(allocator, db.bp);
    var store = NodeStore.init(allocator, &tree);

    const id1 = try store.create(&[_]SymbolId{}, &[_]Property{});
    const id2 = try store.create(&[_]SymbolId{}, &[_]Property{});
    const id3 = try store.create(&[_]SymbolId{}, &[_]Property{});

    try std.testing.expectEqual(@as(NodeId, 1), id1);
    try std.testing.expectEqual(@as(NodeId, 2), id2);
    try std.testing.expectEqual(@as(NodeId, 3), id3);
}

test "nodes: get returns node with correct labels" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.init(allocator, "node_labels");
    defer db.deinit();

    var tree = try BTree.init(allocator, db.bp);
    var store = NodeStore.init(allocator, &tree);

    const labels = [_]SymbolId{ 1000, 1001, 1002 };
    const id = try store.create(&labels, &[_]Property{});

    var node = try store.get(id);
    defer node.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 3), node.labels.len);
    try std.testing.expectEqual(@as(SymbolId, 1000), node.labels[0]);
    try std.testing.expectEqual(@as(SymbolId, 1001), node.labels[1]);
    try std.testing.expectEqual(@as(SymbolId, 1002), node.labels[2]);
}

test "nodes: get returns node with correct properties" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.init(allocator, "node_props");
    defer db.deinit();

    var tree = try BTree.init(allocator, db.bp);
    var store = NodeStore.init(allocator, &tree);

    const properties = [_]Property{
        .{ .key_id = 2000, .value = .{ .string_val = "Alice" } },
        .{ .key_id = 2001, .value = .{ .int_val = 30 } },
        .{ .key_id = 2002, .value = .{ .bool_val = true } },
    };

    const id = try store.create(&[_]SymbolId{1000}, &properties);

    var node = try store.get(id);
    defer node.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 3), node.properties.len);
    try std.testing.expectEqual(@as(SymbolId, 2000), node.properties[0].key_id);
    try std.testing.expectEqualStrings("Alice", node.properties[0].value.string_val);
    try std.testing.expectEqual(@as(i64, 30), node.properties[1].value.int_val);
    try std.testing.expectEqual(true, node.properties[2].value.bool_val);
}

test "nodes: node with zero labels valid" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.init(allocator, "node_nolabels");
    defer db.deinit();

    var tree = try BTree.init(allocator, db.bp);
    var store = NodeStore.init(allocator, &tree);

    const id = try store.create(&[_]SymbolId{}, &[_]Property{
        .{ .key_id = 1000, .value = .{ .string_val = "test" } },
    });

    var node = try store.get(id);
    defer node.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 0), node.labels.len);
    try std.testing.expectEqual(@as(usize, 1), node.properties.len);
}

test "nodes: node with zero properties valid" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.init(allocator, "node_noprops");
    defer db.deinit();

    var tree = try BTree.init(allocator, db.bp);
    var store = NodeStore.init(allocator, &tree);

    const id = try store.create(&[_]SymbolId{ 1000, 1001 }, &[_]Property{});

    var node = try store.get(id);
    defer node.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 2), node.labels.len);
    try std.testing.expectEqual(@as(usize, 0), node.properties.len);
}

test "nodes: delete makes node unfindable" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.init(allocator, "node_delete");
    defer db.deinit();

    var tree = try BTree.init(allocator, db.bp);
    var store = NodeStore.init(allocator, &tree);

    const id = try store.create(&[_]SymbolId{1000}, &[_]Property{});
    try std.testing.expect(try store.exists(id));

    try store.delete(id);

    try std.testing.expect(!(try store.exists(id)));
    try std.testing.expectError(NodeError.NotFound, store.get(id));
}

test "nodes: delete nonexistent returns NotFound" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.init(allocator, "node_del_nf");
    defer db.deinit();

    var tree = try BTree.init(allocator, db.bp);
    var store = NodeStore.init(allocator, &tree);

    try std.testing.expectError(NodeError.NotFound, store.delete(999));
}

test "nodes: update modifies node" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.init(allocator, "node_update");
    defer db.deinit();

    var tree = try BTree.init(allocator, db.bp);
    var store = NodeStore.init(allocator, &tree);

    // Create node
    const id = try store.create(
        &[_]SymbolId{1000},
        &[_]Property{.{ .key_id = 2000, .value = .{ .int_val = 10 } }},
    );

    // Update with new labels and properties
    try store.update(
        id,
        &[_]SymbolId{ 1000, 1001 },
        &[_]Property{.{ .key_id = 2000, .value = .{ .int_val = 20 } }},
    );

    // Verify update
    var node = try store.get(id);
    defer node.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 2), node.labels.len);
    try std.testing.expectEqual(@as(i64, 20), node.properties[0].value.int_val);
}

test "nodes: update nonexistent returns NotFound" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.init(allocator, "node_upd_nf");
    defer db.deinit();

    var tree = try BTree.init(allocator, db.bp);
    var store = NodeStore.init(allocator, &tree);

    try std.testing.expectError(
        NodeError.NotFound,
        store.update(999, &[_]SymbolId{}, &[_]Property{}),
    );
}

// ============================================================================
// Edge Store Tests
// ============================================================================

test "edges: create stores both directions" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.init(allocator, "edge_double");
    defer db.deinit();

    var tree = try BTree.init(allocator, db.bp);
    var store = EdgeStore.init(allocator, &tree);

    const edge_type: SymbolId = 1000;
    try store.create(1, 2, edge_type, &[_]Property{});

    // Outgoing from node 1
    const out_count = try store.countOutgoing(1);
    try std.testing.expectEqual(@as(u64, 1), out_count);

    // Incoming to node 2
    const in_count = try store.countIncoming(2);
    try std.testing.expectEqual(@as(u64, 1), in_count);
}

test "edges: delete removes both directions" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.init(allocator, "edge_del_both");
    defer db.deinit();

    var tree = try BTree.init(allocator, db.bp);
    var store = EdgeStore.init(allocator, &tree);

    const edge_type: SymbolId = 1000;
    try store.create(1, 2, edge_type, &[_]Property{});

    try std.testing.expect(store.exists(1, 2, edge_type));

    try store.delete(1, 2, edge_type);

    try std.testing.expect(!store.exists(1, 2, edge_type));
    try std.testing.expectEqual(@as(u64, 0), try store.countOutgoing(1));
    try std.testing.expectEqual(@as(u64, 0), try store.countIncoming(2));
}

test "edges: outgoing traversal finds all edges from node" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.init(allocator, "edge_out");
    defer db.deinit();

    var tree = try BTree.init(allocator, db.bp);
    var store = EdgeStore.init(allocator, &tree);

    // Create multiple outgoing edges from node 1
    try store.create(1, 2, 1000, &[_]Property{});
    try store.create(1, 3, 1000, &[_]Property{});
    try store.create(1, 4, 1001, &[_]Property{});

    // Also create edge from different node (should not appear)
    try store.create(5, 6, 1000, &[_]Property{});

    var iter = try store.getOutgoing(1);
    defer iter.deinit();

    var count: usize = 0;
    while (try iter.next()) |edge| {
        var e = edge;
        try std.testing.expectEqual(@as(NodeId, 1), e.source);
        e.deinit(allocator);
        count += 1;
    }

    try std.testing.expectEqual(@as(usize, 3), count);
}

test "edges: incoming traversal finds all edges to node" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.init(allocator, "edge_in");
    defer db.deinit();

    var tree = try BTree.init(allocator, db.bp);
    var store = EdgeStore.init(allocator, &tree);

    // Create multiple incoming edges to node 5
    try store.create(1, 5, 1000, &[_]Property{});
    try store.create(2, 5, 1000, &[_]Property{});
    try store.create(3, 5, 1001, &[_]Property{});

    // Edge to different node (should not appear)
    try store.create(4, 6, 1000, &[_]Property{});

    var iter = try store.getIncoming(5);
    defer iter.deinit();

    var count: usize = 0;
    while (try iter.next()) |edge| {
        var e = edge;
        try std.testing.expectEqual(@as(NodeId, 5), e.target);
        e.deinit(allocator);
        count += 1;
    }

    try std.testing.expectEqual(@as(usize, 3), count);
}

test "edges: self-loop works correctly" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.init(allocator, "edge_loop");
    defer db.deinit();

    var tree = try BTree.init(allocator, db.bp);
    var store = EdgeStore.init(allocator, &tree);

    // Create self-loop: node 1 -> node 1
    const edge_type: SymbolId = 1000;
    try store.create(1, 1, edge_type, &[_]Property{});

    // Should be retrievable
    var edge = try store.get(1, 1, edge_type);
    defer edge.deinit(allocator);

    try std.testing.expectEqual(@as(NodeId, 1), edge.source);
    try std.testing.expectEqual(@as(NodeId, 1), edge.target);

    // Should appear in both outgoing and incoming
    try std.testing.expectEqual(@as(u64, 1), try store.countOutgoing(1));
    try std.testing.expectEqual(@as(u64, 1), try store.countIncoming(1));
}

test "edges: properties stored and retrieved" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.init(allocator, "edge_props");
    defer db.deinit();

    var tree = try BTree.init(allocator, db.bp);
    var store = EdgeStore.init(allocator, &tree);

    const properties = [_]Property{
        .{ .key_id = 2000, .value = .{ .int_val = 2020 } },
        .{ .key_id = 2001, .value = .{ .string_val = "work" } },
    };

    try store.create(1, 2, 1000, &properties);

    var edge = try store.get(1, 2, 1000);
    defer edge.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 2), edge.properties.len);
    try std.testing.expectEqual(@as(i64, 2020), edge.properties[0].value.int_val);
    try std.testing.expectEqualStrings("work", edge.properties[1].value.string_val);
}

test "edges: delete nonexistent returns NotFound" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.init(allocator, "edge_del_nf");
    defer db.deinit();

    var tree = try BTree.init(allocator, db.bp);
    var store = EdgeStore.init(allocator, &tree);

    try std.testing.expectError(EdgeError.NotFound, store.delete(1, 2, 1000));
}

test "edges: get nonexistent returns NotFound" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.init(allocator, "edge_get_nf");
    defer db.deinit();

    var tree = try BTree.init(allocator, db.bp);
    var store = EdgeStore.init(allocator, &tree);

    try std.testing.expectError(EdgeError.NotFound, store.get(1, 2, 1000));
}

test "edges: filter by type with getOutgoingByType" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.init(allocator, "edge_bytype");
    defer db.deinit();

    var tree = try BTree.init(allocator, db.bp);
    var store = EdgeStore.init(allocator, &tree);

    // Create edges with different types
    const knows: SymbolId = 1000;
    const likes: SymbolId = 1001;

    try store.create(1, 2, knows, &[_]Property{});
    try store.create(1, 3, knows, &[_]Property{});
    try store.create(1, 4, likes, &[_]Property{});

    // Get only KNOWS edges
    var iter = try store.getOutgoingByType(1, knows);
    defer iter.deinit();

    var count: usize = 0;
    while (try iter.next()) |edge| {
        var e = edge;
        try std.testing.expectEqual(knows, e.edge_type);
        e.deinit(allocator);
        count += 1;
    }

    try std.testing.expectEqual(@as(usize, 2), count);
}

// ============================================================================
// Batch Edge Query Tests
//
// These tests ensure that getOutgoingRefsBatch produces identical results to
// calling getOutgoingRefs individually for each node. This catches bugs in
// optimized scan implementations that might skip nodes or collect wrong edges.
// ============================================================================

/// Helper: collect all outgoing EdgeRefs for a node using the individual iterator API.
fn collectOutgoingRefs(store: *EdgeStore, node_id: NodeId, allocator: Allocator) ![]EdgeRef {
    var refs = std.ArrayListUnmanaged(EdgeRef){};
    var iter = try store.getOutgoingRefs(node_id);
    defer iter.deinit();
    while (try iter.next()) |ref| {
        refs.append(allocator, ref) catch return EdgeError.OutOfMemory;
    }
    return refs.toOwnedSlice(allocator) catch return EdgeError.OutOfMemory;
}

test "edges: batch fetch matches individual for sparse node IDs" {
    // This is the exact scenario that triggered the stale-end_key bug:
    // Nodes with large gaps in ID space, where many B+Tree entries for
    // intermediate nodes exist between the requested nodes.
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.init(allocator, "batch_sparse");
    defer db.deinit();

    var tree = try BTree.init(allocator, db.bp);
    var store = EdgeStore.init(allocator, &tree);

    const type_a: SymbolId = 100;
    const type_b: SymbolId = 200;

    // Create a graph where edges exist for many nodes, but we only batch-query a few.
    // This ensures the scan must skip over entries for non-requested nodes.
    //
    // Node 10 -> 20, 30 (type_a)
    // Node 20 -> 10 (type_a)          <-- between requested nodes 10 and 50
    // Node 30 -> 40 (type_b)          <-- between requested nodes 10 and 50
    // Node 50 -> 60, 70, 80 (type_a)
    // Node 60 -> 10 (type_a)          <-- between requested nodes 50 and 100
    // Node 100 -> 10 (type_b)
    try store.create(10, 20, type_a, &[_]Property{});
    try store.create(10, 30, type_a, &[_]Property{});
    try store.create(20, 10, type_a, &[_]Property{});
    try store.create(30, 40, type_b, &[_]Property{});
    try store.create(50, 60, type_a, &[_]Property{});
    try store.create(50, 70, type_a, &[_]Property{});
    try store.create(50, 80, type_a, &[_]Property{});
    try store.create(60, 10, type_a, &[_]Property{});
    try store.create(100, 10, type_b, &[_]Property{});

    // Batch fetch for nodes [10, 50, 100] - with gaps containing other nodes' edges
    const batch_nodes = [_]NodeId{ 50, 10, 100 }; // Intentionally unsorted
    var batch = try store.getOutgoingRefsBatch(&batch_nodes, allocator);
    defer batch.deinit();

    try std.testing.expectEqual(@as(usize, 3), batch.results.len);

    // Cross-validate each node against individual API
    for (batch.results, 0..) |result, i| {
        const individual = try collectOutgoingRefs(&store, result.node_id, allocator);
        defer allocator.free(individual);

        const batch_edges = batch.getEdges(i);
        try std.testing.expectEqual(individual.len, batch_edges.len);

        for (individual, 0..) |ref, j| {
            try std.testing.expectEqual(ref.source, batch_edges[j].source);
            try std.testing.expectEqual(ref.target, batch_edges[j].target);
            try std.testing.expectEqual(ref.edge_type, batch_edges[j].edge_type);
        }
    }

    // Also verify specific expected counts
    try std.testing.expectEqual(@as(NodeId, 10), batch.results[0].node_id);
    try std.testing.expectEqual(@as(usize, 2), batch.getEdges(0).len); // 10->20, 10->30
    try std.testing.expectEqual(@as(NodeId, 50), batch.results[1].node_id);
    try std.testing.expectEqual(@as(usize, 3), batch.getEdges(1).len); // 50->60, 50->70, 50->80
    try std.testing.expectEqual(@as(NodeId, 100), batch.results[2].node_id);
    try std.testing.expectEqual(@as(usize, 1), batch.getEdges(2).len); // 100->10
}

test "edges: batch fetch does not include edges from non-requested nodes" {
    // Regression test: a scan-based batch implementation must not accidentally
    // include edges from nodes that happen to fall between requested nodes in
    // the B+Tree key space.
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.init(allocator, "batch_noleak");
    defer db.deinit();

    var tree = try BTree.init(allocator, db.bp);
    var store = EdgeStore.init(allocator, &tree);

    const edge_type: SymbolId = 42;

    // Node 1 has edges
    try store.create(1, 100, edge_type, &[_]Property{});

    // Nodes 2-9 each have edges (these are NOT in our batch query)
    for (2..10) |i| {
        try store.create(@intCast(i), 200, edge_type, &[_]Property{});
    }

    // Node 10 has edges
    try store.create(10, 300, edge_type, &[_]Property{});

    // Batch query only nodes 1 and 10 - must not include edges from nodes 2-9
    const batch_nodes = [_]NodeId{ 1, 10 };
    var batch = try store.getOutgoingRefsBatch(&batch_nodes, allocator);
    defer batch.deinit();

    // Node 1: exactly 1 edge (to 100)
    const node1_edges = batch.getEdges(0);
    try std.testing.expectEqual(@as(usize, 1), node1_edges.len);
    try std.testing.expectEqual(@as(NodeId, 1), node1_edges[0].source);
    try std.testing.expectEqual(@as(NodeId, 100), node1_edges[0].target);

    // Node 10: exactly 1 edge (to 300)
    const node10_edges = batch.getEdges(1);
    try std.testing.expectEqual(@as(usize, 1), node10_edges.len);
    try std.testing.expectEqual(@as(NodeId, 10), node10_edges[0].source);
    try std.testing.expectEqual(@as(NodeId, 300), node10_edges[0].target);

    // Total edges in batch should be exactly 2 (not 10)
    try std.testing.expectEqual(@as(usize, 2), batch.edges.len);
}

test "edges: batch fetch handles nodes with no outgoing edges" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.init(allocator, "batch_noout");
    defer db.deinit();

    var tree = try BTree.init(allocator, db.bp);
    var store = EdgeStore.init(allocator, &tree);

    const edge_type: SymbolId = 1;

    // Node 5 has edges TO it, but none FROM it
    try store.create(1, 5, edge_type, &[_]Property{});
    try store.create(10, 5, edge_type, &[_]Property{});

    // Node 20 has outgoing edges
    try store.create(20, 30, edge_type, &[_]Property{});

    // Batch includes node with only incoming edges (5), and nodes not in tree at all (999)
    const batch_nodes = [_]NodeId{ 5, 20, 999 };
    var batch = try store.getOutgoingRefsBatch(&batch_nodes, allocator);
    defer batch.deinit();

    try std.testing.expectEqual(@as(usize, 3), batch.results.len);

    // Node 5: has incoming edges but no outgoing
    try std.testing.expectEqual(@as(NodeId, 5), batch.results[0].node_id);
    try std.testing.expectEqual(@as(usize, 0), batch.getEdges(0).len);

    // Node 20: has outgoing edge
    try std.testing.expectEqual(@as(NodeId, 20), batch.results[1].node_id);
    try std.testing.expectEqual(@as(usize, 1), batch.getEdges(1).len);

    // Node 999: doesn't exist at all
    try std.testing.expectEqual(@as(NodeId, 999), batch.results[2].node_id);
    try std.testing.expectEqual(@as(usize, 0), batch.getEdges(2).len);
}

test "edges: batch fetch single node matches individual fetch" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.init(allocator, "batch_single");
    defer db.deinit();

    var tree = try BTree.init(allocator, db.bp);
    var store = EdgeStore.init(allocator, &tree);

    const type_a: SymbolId = 10;
    const type_b: SymbolId = 20;

    try store.create(42, 1, type_a, &[_]Property{});
    try store.create(42, 2, type_b, &[_]Property{});
    try store.create(42, 3, type_a, &[_]Property{});

    // Single-node batch
    const batch_nodes = [_]NodeId{42};
    var batch = try store.getOutgoingRefsBatch(&batch_nodes, allocator);
    defer batch.deinit();

    // Cross-validate
    const individual = try collectOutgoingRefs(&store, 42, allocator);
    defer allocator.free(individual);

    try std.testing.expectEqual(individual.len, batch.getEdges(0).len);
    for (individual, 0..) |ref, i| {
        try std.testing.expectEqual(ref.source, batch.getEdges(0)[i].source);
        try std.testing.expectEqual(ref.target, batch.getEdges(0)[i].target);
        try std.testing.expectEqual(ref.edge_type, batch.getEdges(0)[i].edge_type);
    }
}

test "edges: batch fetch with many nodes cross-validates against individual" {
    // Property-style test: create a non-trivial graph and verify batch
    // results match individual results for a large random subset of nodes.
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.init(allocator, "batch_many");
    defer db.deinit();

    var tree = try BTree.init(allocator, db.bp);
    var store = EdgeStore.init(allocator, &tree);

    const edge_type: SymbolId = 1;

    // Create a graph with 50 nodes, each having 0-5 outgoing edges
    var prng = std.Random.DefaultPrng.init(12345);
    const random = prng.random();

    const num_nodes: u64 = 50;
    for (1..num_nodes + 1) |source| {
        const num_edges = random.intRangeAtMost(u32, 0, 5);
        for (0..num_edges) |_| {
            const target = random.intRangeAtMost(u64, 1, num_nodes);
            if (target != source) {
                store.create(@intCast(source), @intCast(target), edge_type, &[_]Property{}) catch {
                    // Duplicate edges are fine, just skip
                    continue;
                };
            }
        }
    }

    // Batch fetch ALL nodes
    var all_nodes: [50]NodeId = undefined;
    for (0..50) |i| {
        all_nodes[i] = @intCast(i + 1);
    }

    // Shuffle to test unsorted input
    random.shuffle(NodeId, &all_nodes);

    var batch = try store.getOutgoingRefsBatch(&all_nodes, allocator);
    defer batch.deinit();

    try std.testing.expectEqual(@as(usize, 50), batch.results.len);

    // Verify every node's batch result matches individual fetch
    for (batch.results, 0..) |result, i| {
        const individual = try collectOutgoingRefs(&store, result.node_id, allocator);
        defer allocator.free(individual);

        const batch_edges = batch.getEdges(i);
        try std.testing.expectEqual(individual.len, batch_edges.len);

        for (individual, 0..) |ref, j| {
            try std.testing.expectEqual(ref.source, batch_edges[j].source);
            try std.testing.expectEqual(ref.target, batch_edges[j].target);
            try std.testing.expectEqual(ref.edge_type, batch_edges[j].edge_type);
        }
    }
}

test "edges: batch fetch with duplicate node IDs" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.init(allocator, "batch_dup");
    defer db.deinit();

    var tree = try BTree.init(allocator, db.bp);
    var store = EdgeStore.init(allocator, &tree);

    try store.create(5, 10, 1, &[_]Property{});
    try store.create(5, 20, 1, &[_]Property{});

    // Same node twice in batch - should get results for both entries
    const batch_nodes = [_]NodeId{ 5, 5 };
    var batch = try store.getOutgoingRefsBatch(&batch_nodes, allocator);
    defer batch.deinit();

    try std.testing.expectEqual(@as(usize, 2), batch.results.len);

    // Both results should reference node 5
    try std.testing.expectEqual(@as(NodeId, 5), batch.results[0].node_id);
    try std.testing.expectEqual(@as(NodeId, 5), batch.results[1].node_id);

    // First occurrence should get the edges (scan collects them)
    try std.testing.expectEqual(@as(usize, 2), batch.getEdges(0).len);

    // Second occurrence: edges already consumed by scan, so it gets 0
    // (this is expected behavior for duplicate inputs - caller should deduplicate)
    try std.testing.expectEqual(@as(usize, 0), batch.getEdges(1).len);
}

test "edges: batch fetch adjacent nodes (dense IDs)" {
    // Test the best-case scenario for batch scanning: nodes with adjacent IDs
    // where no entries need to be skipped between them.
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.init(allocator, "batch_dense");
    defer db.deinit();

    var tree = try BTree.init(allocator, db.bp);
    var store = EdgeStore.init(allocator, &tree);

    const edge_type: SymbolId = 1;

    // Create consecutive nodes with edges
    try store.create(1, 10, edge_type, &[_]Property{});
    try store.create(2, 20, edge_type, &[_]Property{});
    try store.create(2, 21, edge_type, &[_]Property{});
    try store.create(3, 30, edge_type, &[_]Property{});
    try store.create(4, 40, edge_type, &[_]Property{});
    try store.create(4, 41, edge_type, &[_]Property{});
    try store.create(4, 42, edge_type, &[_]Property{});

    const batch_nodes = [_]NodeId{ 1, 2, 3, 4 };
    var batch = try store.getOutgoingRefsBatch(&batch_nodes, allocator);
    defer batch.deinit();

    // Cross-validate each
    for (batch.results, 0..) |result, i| {
        const individual = try collectOutgoingRefs(&store, result.node_id, allocator);
        defer allocator.free(individual);

        const batch_edges = batch.getEdges(i);
        try std.testing.expectEqual(individual.len, batch_edges.len);
    }

    // Verify counts: 1, 2, 1, 3
    try std.testing.expectEqual(@as(usize, 1), batch.getEdges(0).len);
    try std.testing.expectEqual(@as(usize, 2), batch.getEdges(1).len);
    try std.testing.expectEqual(@as(usize, 1), batch.getEdges(2).len);
    try std.testing.expectEqual(@as(usize, 3), batch.getEdges(3).len);
}

// ============================================================================
// Label Index Tests
// ============================================================================

test "label_index: nodes by label returns correct set" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.init(allocator, "label_nodes");
    defer db.deinit();

    var tree = try BTree.init(allocator, db.bp);
    var index = LabelIndex.init(allocator, &tree);

    const person: SymbolId = 1000;
    const employee: SymbolId = 1001;

    // Add Person label to nodes 1, 3, 5
    try index.add(person, 1);
    try index.add(person, 3);
    try index.add(person, 5);

    // Add Employee to nodes 1, 2
    try index.add(employee, 1);
    try index.add(employee, 2);

    // Get all Person nodes
    const person_nodes = try index.getNodesByLabel(person);
    defer allocator.free(person_nodes);

    try std.testing.expectEqual(@as(usize, 3), person_nodes.len);

    // Get all Employee nodes
    const employee_nodes = try index.getNodesByLabel(employee);
    defer allocator.free(employee_nodes);

    try std.testing.expectEqual(@as(usize, 2), employee_nodes.len);
}

test "label_index: count returns correct value" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.init(allocator, "label_count");
    defer db.deinit();

    var tree = try BTree.init(allocator, db.bp);
    var index = LabelIndex.init(allocator, &tree);

    const label: SymbolId = 1000;

    try std.testing.expectEqual(@as(u64, 0), try index.countNodesByLabel(label));

    try index.add(label, 1);
    try index.add(label, 2);
    try index.add(label, 3);

    try std.testing.expectEqual(@as(u64, 3), try index.countNodesByLabel(label));
}

test "label_index: empty label returns empty result" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.init(allocator, "label_empty");
    defer db.deinit();

    var tree = try BTree.init(allocator, db.bp);
    var index = LabelIndex.init(allocator, &tree);

    const nodes = try index.getNodesByLabel(9999);
    defer allocator.free(nodes);

    try std.testing.expectEqual(@as(usize, 0), nodes.len);
}

test "label_index: iterator lazy evaluation" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.init(allocator, "label_iter");
    defer db.deinit();

    var tree = try BTree.init(allocator, db.bp);
    var index = LabelIndex.init(allocator, &tree);

    const label: SymbolId = 1000;
    try index.add(label, 10);
    try index.add(label, 20);
    try index.add(label, 30);

    var iter = try index.iterNodesByLabel(label);
    defer iter.deinit();

    // Should get nodes in order (big-endian sorting)
    const first = try iter.next();
    try std.testing.expectEqual(@as(?NodeId, 10), first);

    const second = try iter.next();
    try std.testing.expectEqual(@as(?NodeId, 20), second);

    const third = try iter.next();
    try std.testing.expectEqual(@as(?NodeId, 30), third);

    const none = try iter.next();
    try std.testing.expectEqual(@as(?NodeId, null), none);
}
