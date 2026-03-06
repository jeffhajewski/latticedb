//! End-to-end tests for Cypher mutation queries (CREATE, SET, DELETE, REMOVE, MERGE)
//! and search operators (<=> vector search, @@ full-text search) executed through db.query().
//!
//! These tests verify the full pipeline: parsing → planning → execution → storage,
//! ensuring that mutations and searches work correctly through the query interface,
//! not just through the direct API.

const std = @import("std");
const lattice = @import("lattice");

const Database = lattice.storage.database.Database;
const QueryError = lattice.storage.database.QueryError;
const PropertyValue = lattice.core.types.PropertyValue;
const SymbolId = lattice.graph.symbols.SymbolId;
const NodeProperty = lattice.graph.node.Property;

fn openTestDb(path: []const u8, opts: struct {
    enable_vector: bool = false,
    vector_dimensions: u16 = 0,
    enable_fts: bool = false,
}) !*Database {
    std.fs.cwd().deleteFile(path) catch {};
    return try Database.open(std.testing.allocator, path, .{
        .create = true,
        .config = .{
            .enable_wal = false,
            .enable_fts = opts.enable_fts,
            .enable_vector = opts.enable_vector,
            .vector_dimensions = opts.vector_dimensions,
        },
    });
}

fn cleanupTestDb(db: *Database, path: []const u8) void {
    db.close();
    std.fs.cwd().deleteFile(path) catch {};
}

// ============================================================================
// CREATE Node Tests
// ============================================================================

test "query: CREATE node with label" {
    const path = "/tmp/lattice_qm_create_label.ltdb";
    var db = try openTestDb(path, .{});
    defer cleanupTestDb(db, path);

    var r = try db.query("CREATE (n:Person)");
    r.deinit();

    var result = try db.query("MATCH (n:Person) RETURN count(n)");
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), result.rowCount());
    try expectInt(result, 0, 0, 1);
}

test "query: CREATE node with properties" {
    const path = "/tmp/lattice_qm_create_props.ltdb";
    var db = try openTestDb(path, .{});
    defer cleanupTestDb(db, path);

    var r = try db.query("CREATE (n:Person {name: \"Alice\", age: 30})");
    r.deinit();

    var result = try db.query("MATCH (n:Person) RETURN n.name, n.age");
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), result.rowCount());
    try expectString(result, 0, 0, "Alice");
    try expectInt(result, 0, 1, 30);
}

test "query: CREATE node with float property" {
    const path = "/tmp/lattice_qm_create_float.ltdb";
    var db = try openTestDb(path, .{});
    defer cleanupTestDb(db, path);

    var r = try db.query("CREATE (n:Measurement {value: 3.14})");
    r.deinit();

    var result = try db.query("MATCH (n:Measurement) RETURN n.value");
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), result.rowCount());
    try expectFloat(result, 0, 0, 3.14, 0.01);
}

test "query: CREATE node with boolean property" {
    const path = "/tmp/lattice_qm_create_bool.ltdb";
    var db = try openTestDb(path, .{});
    defer cleanupTestDb(db, path);

    var r = try db.query("CREATE (n:Flag {active: true, deleted: false})");
    r.deinit();

    var result = try db.query("MATCH (n:Flag) RETURN n.active, n.deleted");
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), result.rowCount());
    try expectBool(result, 0, 0, true);
    try expectBool(result, 0, 1, false);
}

test "query: CREATE node with string containing special characters" {
    const path = "/tmp/lattice_qm_create_special.ltdb";
    var db = try openTestDb(path, .{});
    defer cleanupTestDb(db, path);

    var r = try db.query("CREATE (n:Text {content: \"hello world\"})");
    r.deinit();

    var result = try db.query("MATCH (n:Text) RETURN n.content");
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), result.rowCount());
    try expectString(result, 0, 0, "hello world");
}

test "query: CREATE multiple nodes sequentially" {
    const path = "/tmp/lattice_qm_create_multi.ltdb";
    var db = try openTestDb(path, .{});
    defer cleanupTestDb(db, path);

    var r1 = try db.query("CREATE (n:Person {name: \"Alice\"})");
    r1.deinit();
    var r2 = try db.query("CREATE (n:Person {name: \"Bob\"})");
    r2.deinit();
    var r3 = try db.query("CREATE (n:Person {name: \"Carol\"})");
    r3.deinit();

    var result = try db.query("MATCH (n:Person) RETURN count(n)");
    defer result.deinit();
    try expectInt(result, 0, 0, 3);
}

test "query: CREATE node with multiple labels" {
    const path = "/tmp/lattice_qm_create_multilabel.ltdb";
    var db = try openTestDb(path, .{});
    defer cleanupTestDb(db, path);

    var r = try db.query("CREATE (n:Person:Employee {name: \"Alice\"})");
    r.deinit();

    // Should match both labels
    var r1 = try db.query("MATCH (n:Person) RETURN count(n)");
    defer r1.deinit();
    try expectInt(r1, 0, 0, 1);

    var r2 = try db.query("MATCH (n:Employee) RETURN count(n)");
    defer r2.deinit();
    try expectInt(r2, 0, 0, 1);
}

// ============================================================================
// CREATE Relationship Tests
// ============================================================================

test "query: CREATE relationship between new nodes" {
    const path = "/tmp/lattice_qm_create_rel.ltdb";
    var db = try openTestDb(path, .{});
    defer cleanupTestDb(db, path);

    var r = try db.query("CREATE (a:Person {name: \"Alice\"})-[:KNOWS]->(b:Person {name: \"Bob\"})");
    r.deinit();

    var result = try db.query("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name");
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), result.rowCount());
    try expectString(result, 0, 0, "Alice");
    try expectString(result, 0, 1, "Bob");
}

test "query: CREATE relationship between existing nodes via MATCH" {
    const path = "/tmp/lattice_qm_match_create_rel.ltdb";
    var db = try openTestDb(path, .{});
    defer cleanupTestDb(db, path);

    var r1 = try db.query("CREATE (n:Person {name: \"Alice\"})");
    r1.deinit();
    var r2 = try db.query("CREATE (n:Person {name: \"Bob\"})");
    r2.deinit();

    var r3 = try db.query("MATCH (a:Person {name: \"Alice\"}), (b:Person {name: \"Bob\"}) CREATE (a)-[:FRIENDS_WITH]->(b)");
    r3.deinit();

    var result = try db.query("MATCH (a:Person)-[:FRIENDS_WITH]->(b:Person) RETURN a.name, b.name");
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), result.rowCount());
    try expectString(result, 0, 0, "Alice");
    try expectString(result, 0, 1, "Bob");
}

test "query: CREATE reverse direction relationship" {
    const path = "/tmp/lattice_qm_create_rev_rel.ltdb";
    var db = try openTestDb(path, .{});
    defer cleanupTestDb(db, path);

    var r = try db.query("CREATE (a:Person {name: \"Alice\"})<-[:FOLLOWS]-(b:Person {name: \"Bob\"})");
    r.deinit();

    // Bob follows Alice, so direction matters
    var result = try db.query("MATCH (b:Person)-[:FOLLOWS]->(a:Person) RETURN b.name, a.name");
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), result.rowCount());
    try expectString(result, 0, 0, "Bob");
    try expectString(result, 0, 1, "Alice");
}

test "query: CREATE multiple relationships forming a chain" {
    const path = "/tmp/lattice_qm_create_chain.ltdb";
    var db = try openTestDb(path, .{});
    defer cleanupTestDb(db, path);

    var r1 = try db.query("CREATE (a:Person {name: \"Alice\"})");
    r1.deinit();
    var r2 = try db.query("CREATE (b:Person {name: \"Bob\"})");
    r2.deinit();
    var r3 = try db.query("CREATE (c:Person {name: \"Carol\"})");
    r3.deinit();

    var r4 = try db.query("MATCH (a:Person {name: \"Alice\"}), (b:Person {name: \"Bob\"}) CREATE (a)-[:KNOWS]->(b)");
    r4.deinit();
    var r5 = try db.query("MATCH (b:Person {name: \"Bob\"}), (c:Person {name: \"Carol\"}) CREATE (b)-[:KNOWS]->(c)");
    r5.deinit();

    // Traverse 2-hop path: Alice -> Bob -> Carol
    var result = try db.query("MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) RETURN a.name, b.name, c.name");
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), result.rowCount());
    try expectString(result, 0, 0, "Alice");
    try expectString(result, 0, 1, "Bob");
    try expectString(result, 0, 2, "Carol");
}

test "query: CREATE node then connect to different label" {
    const path = "/tmp/lattice_qm_cross_label.ltdb";
    var db = try openTestDb(path, .{});
    defer cleanupTestDb(db, path);

    var r1 = try db.query("CREATE (p:Person {name: \"Alice\"})");
    r1.deinit();
    var r2 = try db.query("CREATE (c:Company {name: \"Acme\", industry: \"Tech\"})");
    r2.deinit();

    var r3 = try db.query("MATCH (p:Person {name: \"Alice\"}), (c:Company {name: \"Acme\"}) CREATE (p)-[:WORKS_AT]->(c)");
    r3.deinit();

    var result = try db.query("MATCH (p:Person)-[:WORKS_AT]->(c:Company) RETURN p.name, c.name, c.industry");
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), result.rowCount());
    try expectString(result, 0, 0, "Alice");
    try expectString(result, 0, 1, "Acme");
    try expectString(result, 0, 2, "Tech");
}

test "query: CREATE fails on non-property value and avoids partial node creation" {
    const path = "/tmp/lattice_qm_create_type_error.ltdb";
    var db = try openTestDb(path, .{});
    defer cleanupTestDb(db, path);

    var seed = try db.query("CREATE (p:Person {name: \"Alice\"})");
    seed.deinit();

    try std.testing.expectError(
        QueryError.ExecutionError,
        db.query("MATCH (p:Person {name: \"Alice\"}) CREATE (c:Copy {owner: p})"),
    );

    var copies = try db.query("MATCH (c:Copy) RETURN count(c)");
    defer copies.deinit();
    try expectInt(copies, 0, 0, 0);
}

test "query: edge variable resolves properties/type/id with high node ids" {
    const path = "/tmp/lattice_qm_edge_high_id.ltdb";
    var db = try openTestDb(path, .{});
    defer cleanupTestDb(db, path);

    // Seed data directly to exercise edge identity/path decoding end-to-end.
    try db.node_store.createWithId(1, &[_]SymbolId{}, &[_]NodeProperty{});
    try db.node_store.createWithId(70000, &[_]SymbolId{}, &[_]NodeProperty{});

    const rel_type = try db.symbol_table.intern("REL");
    const weight_key = try db.symbol_table.intern("w");
    const props = [_]NodeProperty{
        .{ .key_id = weight_key, .value = .{ .int_val = 42 } },
    };
    try db.edge_store.createWithId(1, 1, 70000, rel_type, &props);

    var result = try db.query("MATCH (a)-[r:REL]->(b) RETURN id(b), r.w, type(r), id(r)");
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), result.rowCount());
    try expectInt(result, 0, 0, 70000);
    try expectInt(result, 0, 1, 42);
    try expectString(result, 0, 2, "REL");
    try expectInt(result, 0, 3, 1);
}

test "query: DELETE r removes only the matched parallel edge" {
    const path = "/tmp/lattice_qm_delete_parallel_edge.ltdb";
    var db = try openTestDb(path, .{});
    defer cleanupTestDb(db, path);

    const source = try db.createNode(null, &[_][]const u8{});
    const target = try db.createNode(null, &[_][]const u8{});

    const rel_type = try db.symbol_table.intern("REL");
    const weight_key = try db.symbol_table.intern("w");
    const props1 = [_]NodeProperty{
        .{ .key_id = weight_key, .value = .{ .int_val = 1 } },
    };
    const props2 = [_]NodeProperty{
        .{ .key_id = weight_key, .value = .{ .int_val = 2 } },
    };
    const edge1 = try db.edge_store.createAndGetId(source, target, rel_type, &props1);
    const edge2 = try db.edge_store.createAndGetId(source, target, rel_type, &props2);

    var before = try db.query("MATCH (a)-[r:REL]->(b) RETURN count(r)");
    defer before.deinit();
    try expectInt(before, 0, 0, 2);

    var ids_before = try db.query("MATCH (a)-[r:REL]->(b) RETURN id(r), r.w ORDER BY r.w ASC");
    defer ids_before.deinit();
    try std.testing.expectEqual(@as(usize, 2), ids_before.rowCount());
    try expectInt(ids_before, 0, 0, @intCast(edge1));
    try expectInt(ids_before, 0, 1, 1);
    try expectInt(ids_before, 1, 0, @intCast(edge2));
    try expectInt(ids_before, 1, 1, 2);

    var del = try db.query("MATCH (a)-[r:REL]->(b) WHERE r.w = 1 DELETE r");
    del.deinit();

    var after = try db.query("MATCH (a)-[r:REL]->(b) RETURN count(r), min(r.w), max(r.w), id(r)");
    defer after.deinit();
    try expectInt(after, 0, 0, 1);
    try expectInt(after, 0, 1, 2);
    try expectInt(after, 0, 2, 2);
    try expectInt(after, 0, 3, @intCast(edge2));
}

test "query: edge variable preserves large edge ids end-to-end" {
    const path = "/tmp/lattice_qm_large_edge_id.ltdb";
    var db = try openTestDb(path, .{});
    defer cleanupTestDb(db, path);

    try db.node_store.createWithId(1, &[_]SymbolId{}, &[_]NodeProperty{});
    try db.node_store.createWithId(2, &[_]SymbolId{}, &[_]NodeProperty{});

    const rel_type = try db.symbol_table.intern("REL");
    const weight_key = try db.symbol_table.intern("w");
    const props = [_]NodeProperty{
        .{ .key_id = weight_key, .value = .{ .int_val = 99 } },
    };
    const large_edge_id: u64 = (@as(u64, 1) << 40) + 123;
    try db.edge_store.createWithId(large_edge_id, 1, 2, rel_type, &props);

    var result = try db.query("MATCH (a)-[r:REL]->(b) RETURN id(r), r.w, type(r)");
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), result.rowCount());
    try expectInt(result, 0, 0, @intCast(large_edge_id));
    try expectInt(result, 0, 1, 99);
    try expectString(result, 0, 2, "REL");
}

test "query: DELETE by id(r) removes exactly one parallel edge" {
    const path = "/tmp/lattice_qm_delete_by_edge_id.ltdb";
    var db = try openTestDb(path, .{});
    defer cleanupTestDb(db, path);

    const source = try db.createNode(null, &[_][]const u8{});
    const target = try db.createNode(null, &[_][]const u8{});

    const rel_type = try db.symbol_table.intern("REL");
    const weight_key = try db.symbol_table.intern("w");
    const props1 = [_]NodeProperty{
        .{ .key_id = weight_key, .value = .{ .int_val = 10 } },
    };
    const props2 = [_]NodeProperty{
        .{ .key_id = weight_key, .value = .{ .int_val = 20 } },
    };
    const edge1 = try db.edge_store.createAndGetId(source, target, rel_type, &props1);
    const edge2 = try db.edge_store.createAndGetId(source, target, rel_type, &props2);

    var delete_q_buf: [128]u8 = undefined;
    const delete_query = try std.fmt.bufPrint(
        &delete_q_buf,
        "MATCH (a)-[r:REL]->(b) WHERE id(r) = {d} DELETE r",
        .{edge1},
    );
    var delete_result = try db.query(delete_query);
    delete_result.deinit();

    var after = try db.query("MATCH (a)-[r:REL]->(b) RETURN count(r), id(r), r.w");
    defer after.deinit();
    try expectInt(after, 0, 0, 1);
    try expectInt(after, 0, 1, @intCast(edge2));
    try expectInt(after, 0, 2, 20);
}

test "query: SET property on edge variable" {
    const path = "/tmp/lattice_qm_set_edge_property.ltdb";
    var db = try openTestDb(path, .{});
    defer cleanupTestDb(db, path);

    var seed = try db.query("CREATE (a:Person {name: \"Alice\"})-[:REL]->(b:Person {name: \"Bob\"})");
    seed.deinit();

    var set_q = try db.query("MATCH (a)-[r:REL]->(b) SET r.since = 2024");
    set_q.deinit();

    var result = try db.query("MATCH (a)-[r:REL]->(b) RETURN r.since");
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), result.rowCount());
    try expectInt(result, 0, 0, 2024);
}

test "query: SET edge property updates only matched parallel edge" {
    const path = "/tmp/lattice_qm_set_edge_property_parallel_selective.ltdb";
    var db = try openTestDb(path, .{});
    defer cleanupTestDb(db, path);

    const source = try db.createNode(null, &[_][]const u8{});
    const target = try db.createNode(null, &[_][]const u8{});
    const rel_type = try db.symbol_table.intern("REL");
    const weight_key = try db.symbol_table.intern("w");

    const props1 = [_]NodeProperty{
        .{ .key_id = weight_key, .value = .{ .int_val = 1 } },
    };
    const props2 = [_]NodeProperty{
        .{ .key_id = weight_key, .value = .{ .int_val = 2 } },
    };
    _ = try db.edge_store.createAndGetId(source, target, rel_type, &props1);
    _ = try db.edge_store.createAndGetId(source, target, rel_type, &props2);

    var set_q = try db.query("MATCH (a)-[r:REL]->(b) WHERE r.w = 1 SET r.tag = \"selected\"");
    set_q.deinit();

    var tagged_count = try db.query("MATCH (a)-[r:REL]->(b) WHERE r.tag = \"selected\" RETURN count(r)");
    defer tagged_count.deinit();
    try expectInt(tagged_count, 0, 0, 1);

    var untouched = try db.query("MATCH (a)-[r:REL]->(b) WHERE r.w = 2 RETURN r.tag");
    defer untouched.deinit();
    try std.testing.expectEqual(@as(usize, 1), untouched.rowCount());
    try std.testing.expect(untouched.rows[0].values[0] == .null_val);
}

test "query: DELETE untyped edge variable removes all matched edges" {
    const path = "/tmp/lattice_qm_delete_untyped_edge_var.ltdb";
    var db = try openTestDb(path, .{});
    defer cleanupTestDb(db, path);

    var n1 = try db.query("CREATE (a:Person {name: \"Alice\"})");
    n1.deinit();
    var n2 = try db.query("CREATE (b:Person {name: \"Bob\"})");
    n2.deinit();
    var e1 = try db.query("MATCH (a:Person {name: \"Alice\"}), (b:Person {name: \"Bob\"}) CREATE (a)-[:KNOWS]->(b)");
    e1.deinit();
    var e2 = try db.query("MATCH (a:Person {name: \"Alice\"}), (b:Person {name: \"Bob\"}) CREATE (a)-[:COLLEAGUE]->(b)");
    e2.deinit();

    var before = try db.query("MATCH (:Person)-[r]->(:Person) RETURN count(r)");
    defer before.deinit();
    try expectInt(before, 0, 0, 2);

    var del = try db.query("MATCH (:Person)-[r]->(:Person) DELETE r");
    del.deinit();

    var after = try db.query("MATCH (:Person)-[r]->(:Person) RETURN count(r)");
    defer after.deinit();
    try expectInt(after, 0, 0, 0);
}

test "query: WITH pass-through variable remains bound" {
    const path = "/tmp/lattice_qm_with_passthrough_binding.ltdb";
    var db = try openTestDb(path, .{});
    defer cleanupTestDb(db, path);

    var r1 = try db.query("CREATE (n:Person {name: \"Alice\"})");
    r1.deinit();
    var r2 = try db.query("CREATE (n:Person {name: \"Bob\"})");
    r2.deinit();

    var result = try db.query("MATCH (n:Person) WITH n RETURN count(n)");
    defer result.deinit();
    try expectInt(result, 0, 0, 2);
}

test "query: WITH alias is visible in same-clause WHERE" {
    const path = "/tmp/lattice_qm_with_alias_where.ltdb";
    var db = try openTestDb(path, .{});
    defer cleanupTestDb(db, path);

    var r1 = try db.query("CREATE (n:Person {name: \"Alice\", age: 31})");
    r1.deinit();
    var r2 = try db.query("CREATE (n:Person {name: \"Bob\", age: 24})");
    r2.deinit();
    var r3 = try db.query("CREATE (n:Person {name: \"Carol\", age: 35})");
    r3.deinit();

    var result = try db.query(
        "MATCH (n:Person) WITH n AS p WHERE p.age >= 30 RETURN count(p)",
    );
    defer result.deinit();
    try expectInt(result, 0, 0, 2);
}

test "query: WITH drops variables not projected forward" {
    const path = "/tmp/lattice_qm_with_scope_drop.ltdb";
    var db = try openTestDb(path, .{});
    defer cleanupTestDb(db, path);

    var r1 = try db.query("CREATE (n:Person {name: \"Alice\"})");
    r1.deinit();

    try std.testing.expectError(
        QueryError.SemanticError,
        db.query("MATCH (n:Person) WITH n.name AS name RETURN n"),
    );
}

// ============================================================================
// SET Tests
// ============================================================================

test "query: SET updates existing property" {
    const path = "/tmp/lattice_qm_set_update.ltdb";
    var db = try openTestDb(path, .{});
    defer cleanupTestDb(db, path);

    var r1 = try db.query("CREATE (n:Person {name: \"Alice\", age: 30})");
    r1.deinit();

    var r2 = try db.query("MATCH (n:Person {name: \"Alice\"}) SET n.age = 31");
    r2.deinit();

    var result = try db.query("MATCH (n:Person {name: \"Alice\"}) RETURN n.age");
    defer result.deinit();
    try expectInt(result, 0, 0, 31);
}

test "query: SET adds new property" {
    const path = "/tmp/lattice_qm_set_new.ltdb";
    var db = try openTestDb(path, .{});
    defer cleanupTestDb(db, path);

    var r1 = try db.query("CREATE (n:Person {name: \"Alice\"})");
    r1.deinit();

    var r2 = try db.query("MATCH (n:Person {name: \"Alice\"}) SET n.email = \"alice@example.com\"");
    r2.deinit();

    var result = try db.query("MATCH (n:Person {name: \"Alice\"}) RETURN n.email");
    defer result.deinit();
    try expectString(result, 0, 0, "alice@example.com");
}

test "query: SET multiple properties in sequence" {
    const path = "/tmp/lattice_qm_set_multi.ltdb";
    var db = try openTestDb(path, .{});
    defer cleanupTestDb(db, path);

    var r1 = try db.query("CREATE (n:Person {name: \"Alice\"})");
    r1.deinit();

    var r2 = try db.query("MATCH (n:Person {name: \"Alice\"}) SET n.age = 30, n.city = \"NYC\"");
    r2.deinit();

    var result = try db.query("MATCH (n:Person {name: \"Alice\"}) RETURN n.age, n.city");
    defer result.deinit();
    try expectInt(result, 0, 0, 30);
    try expectString(result, 0, 1, "NYC");
}

test "query: SET replace fails fast on non-property value and preserves existing properties" {
    const path = "/tmp/lattice_qm_set_replace_type_error.ltdb";
    var db = try openTestDb(path, .{});
    defer cleanupTestDb(db, path);

    var seed = try db.query("CREATE (n:Person {name: \"Alice\", city: \"NYC\"})");
    seed.deinit();

    try std.testing.expectError(
        QueryError.ExecutionError,
        db.query("MATCH (n:Person {name: \"Alice\"}) SET n = {self: n}"),
    );

    var result = try db.query("MATCH (n:Person {name: \"Alice\"}) RETURN n.name, n.city");
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), result.rowCount());
    try expectString(result, 0, 0, "Alice");
    try expectString(result, 0, 1, "NYC");
}

test "query: SET merge fails fast on non-property value and preserves row state" {
    const path = "/tmp/lattice_qm_set_merge_type_error.ltdb";
    var db = try openTestDb(path, .{});
    defer cleanupTestDb(db, path);

    var seed = try db.query("CREATE (n:Person {name: \"Alice\", age: 30})");
    seed.deinit();

    try std.testing.expectError(
        QueryError.ExecutionError,
        db.query("MATCH (n:Person {name: \"Alice\"}) SET n += {self: n}"),
    );

    var unchanged = try db.query("MATCH (n:Person {name: \"Alice\"}) RETURN n.age");
    defer unchanged.deinit();
    try expectInt(unchanged, 0, 0, 30);

    var self_count = try db.query("MATCH (n:Person {name: \"Alice\"}) WHERE n.self IS NOT NULL RETURN count(n)");
    defer self_count.deinit();
    try expectInt(self_count, 0, 0, 0);
}

test "query: SET merge accepts parameterized map expression" {
    const path = "/tmp/lattice_qm_set_merge_param_map.ltdb";
    var db = try openTestDb(path, .{});
    defer cleanupTestDb(db, path);

    var seed = try db.query("CREATE (n:Person {name: \"Alice\", age: 30})");
    seed.deinit();

    var map_entries = [_]PropertyValue.MapEntry{
        .{ .key = "age", .value = .{ .int_val = 31 } },
        .{ .key = "city", .value = .{ .string_val = "SF" } },
    };
    var params = std.StringHashMap(PropertyValue).init(std.testing.allocator);
    defer params.deinit();
    try params.put("props", .{ .map_val = &map_entries });

    var set_q = try db.queryWithParams(
        "MATCH (n:Person {name: \"Alice\"}) SET n += $props",
        &params,
    );
    set_q.deinit();

    var result = try db.query("MATCH (n:Person {name: \"Alice\"}) RETURN n.age, n.city");
    defer result.deinit();
    try expectInt(result, 0, 0, 31);
    try expectString(result, 0, 1, "SF");
}

test "query: SET replace accepts parameterized map expression" {
    const path = "/tmp/lattice_qm_set_replace_param_map.ltdb";
    var db = try openTestDb(path, .{});
    defer cleanupTestDb(db, path);

    var seed = try db.query("CREATE (n:Person {name: \"Alice\", age: 30})");
    seed.deinit();

    var map_entries = [_]PropertyValue.MapEntry{
        .{ .key = "city", .value = .{ .string_val = "LA" } },
    };
    var params = std.StringHashMap(PropertyValue).init(std.testing.allocator);
    defer params.deinit();
    try params.put("props", .{ .map_val = &map_entries });

    var set_q = try db.queryWithParams(
        "MATCH (n:Person {name: \"Alice\"}) SET n = $props",
        &params,
    );
    set_q.deinit();

    var city_count = try db.query("MATCH (n:Person) WHERE n.city = \"LA\" RETURN count(n)");
    defer city_count.deinit();
    try expectInt(city_count, 0, 0, 1);

    var name_count = try db.query("MATCH (n:Person) WHERE n.name IS NOT NULL RETURN count(n)");
    defer name_count.deinit();
    try expectInt(name_count, 0, 0, 0);
}

test "query: SET affects only matched nodes" {
    const path = "/tmp/lattice_qm_set_selective.ltdb";
    var db = try openTestDb(path, .{});
    defer cleanupTestDb(db, path);

    var r1 = try db.query("CREATE (n:Person {name: \"Alice\", age: 30})");
    r1.deinit();
    var r2 = try db.query("CREATE (n:Person {name: \"Bob\", age: 25})");
    r2.deinit();

    // Only update Alice
    var r3 = try db.query("MATCH (n:Person {name: \"Alice\"}) SET n.age = 31");
    r3.deinit();

    // Bob should be unchanged
    var result = try db.query("MATCH (n:Person {name: \"Bob\"}) RETURN n.age");
    defer result.deinit();
    try expectInt(result, 0, 0, 25);
}

test "query: SET label on node" {
    const path = "/tmp/lattice_qm_set_label.ltdb";
    var db = try openTestDb(path, .{});
    defer cleanupTestDb(db, path);

    var r1 = try db.query("CREATE (n:Person {name: \"Alice\"})");
    r1.deinit();

    var r2 = try db.query("MATCH (n:Person {name: \"Alice\"}) SET n:Admin");
    r2.deinit();

    // Should now match Admin label
    var result = try db.query("MATCH (n:Admin) RETURN n.name");
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), result.rowCount());
    try expectString(result, 0, 0, "Alice");
}

// ============================================================================
// DELETE Tests
// ============================================================================

test "query: DELETE removes node" {
    const path = "/tmp/lattice_qm_delete.ltdb";
    var db = try openTestDb(path, .{});
    defer cleanupTestDb(db, path);

    var r1 = try db.query("CREATE (n:Person {name: \"Alice\"})");
    r1.deinit();
    var r2 = try db.query("CREATE (n:Person {name: \"Bob\"})");
    r2.deinit();

    var r3 = try db.query("MATCH (n:Person {name: \"Alice\"}) DELETE n");
    r3.deinit();

    var result = try db.query("MATCH (n:Person) RETURN count(n)");
    defer result.deinit();
    try expectInt(result, 0, 0, 1);
}

test "query: DELETE with WHERE clause" {
    const path = "/tmp/lattice_qm_delete_where.ltdb";
    var db = try openTestDb(path, .{});
    defer cleanupTestDb(db, path);

    var r1 = try db.query("CREATE (n:Person {name: \"Alice\", age: 30})");
    r1.deinit();
    var r2 = try db.query("CREATE (n:Person {name: \"Bob\", age: 25})");
    r2.deinit();
    var r3 = try db.query("CREATE (n:Person {name: \"Carol\", age: 35})");
    r3.deinit();

    // Delete people under 30
    var r4 = try db.query("MATCH (n:Person) WHERE n.age < 30 DELETE n");
    r4.deinit();

    var result = try db.query("MATCH (n:Person) RETURN count(n)");
    defer result.deinit();
    try expectInt(result, 0, 0, 2);
}

test "query: DETACH DELETE removes node and edges" {
    const path = "/tmp/lattice_qm_detach_delete.ltdb";
    var db = try openTestDb(path, .{});
    defer cleanupTestDb(db, path);

    // Create a graph: Alice -KNOWS-> Bob -KNOWS-> Carol
    var r1 = try db.query("CREATE (a:Person {name: \"Alice\"})-[:KNOWS]->(b:Person {name: \"Bob\"})");
    r1.deinit();
    var r3 = try db.query("CREATE (c:Person {name: \"Carol\"})");
    r3.deinit();
    var r4 = try db.query("MATCH (b:Person {name: \"Bob\"}), (c:Person {name: \"Carol\"}) CREATE (b)-[:KNOWS]->(c)");
    r4.deinit();

    // DETACH DELETE Bob (should also remove edges)
    var r5 = try db.query("MATCH (n:Person {name: \"Bob\"}) DETACH DELETE n");
    r5.deinit();

    // Only Alice and Carol remain, with no KNOWS relationships
    var count_result = try db.query("MATCH (n:Person) RETURN count(n)");
    defer count_result.deinit();
    try expectInt(count_result, 0, 0, 2);

    var edge_result = try db.query("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN count(a)");
    defer edge_result.deinit();
    try expectInt(edge_result, 0, 0, 0);
}

test "query: DELETE all nodes of a label" {
    const path = "/tmp/lattice_qm_delete_all.ltdb";
    var db = try openTestDb(path, .{});
    defer cleanupTestDb(db, path);

    var r1 = try db.query("CREATE (n:Temp {val: 1})");
    r1.deinit();
    var r2 = try db.query("CREATE (n:Temp {val: 2})");
    r2.deinit();
    var r3 = try db.query("CREATE (n:Temp {val: 3})");
    r3.deinit();
    var r4 = try db.query("CREATE (n:Permanent {val: 99})");
    r4.deinit();

    // Delete all Temp nodes
    var r5 = try db.query("MATCH (n:Temp) DELETE n");
    r5.deinit();

    // Temp nodes gone
    var result1 = try db.query("MATCH (n:Temp) RETURN count(n)");
    defer result1.deinit();
    try expectInt(result1, 0, 0, 0);

    // Permanent node still there
    var result2 = try db.query("MATCH (n:Permanent) RETURN count(n)");
    defer result2.deinit();
    try expectInt(result2, 0, 0, 1);
}

// ============================================================================
// REMOVE Tests
// ============================================================================

test "query: REMOVE property from node" {
    const path = "/tmp/lattice_qm_remove_prop.ltdb";
    var db = try openTestDb(path, .{});
    defer cleanupTestDb(db, path);

    var r1 = try db.query("CREATE (n:Person {name: \"Alice\", age: 30, temp: \"remove me\"})");
    r1.deinit();

    var r2 = try db.query("MATCH (n:Person {name: \"Alice\"}) REMOVE n.temp");
    r2.deinit();

    // name and age should still exist
    var result = try db.query("MATCH (n:Person {name: \"Alice\"}) RETURN n.name, n.age");
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), result.rowCount());
    try expectString(result, 0, 0, "Alice");
    try expectInt(result, 0, 1, 30);
}

test "query: REMOVE property from edge" {
    const path = "/tmp/lattice_qm_remove_edge_prop.ltdb";
    var db = try openTestDb(path, .{});
    defer cleanupTestDb(db, path);

    var seed = try db.query("CREATE (a:Person {name: \"Alice\"})-[:REL]->(b:Person {name: \"Bob\"})");
    seed.deinit();

    var set_q = try db.query("MATCH (a)-[r:REL]->(b) SET r.temp = \"x\"");
    set_q.deinit();

    var remove_q = try db.query("MATCH (a)-[r:REL]->(b) REMOVE r.temp");
    remove_q.deinit();

    var still_edges = try db.query("MATCH (a)-[r:REL]->(b) RETURN count(r)");
    defer still_edges.deinit();
    try expectInt(still_edges, 0, 0, 1);

    var temp_count = try db.query("MATCH (a)-[r:REL]->(b) WHERE r.temp = \"x\" RETURN count(r)");
    defer temp_count.deinit();
    try expectInt(temp_count, 0, 0, 0);
}

test "query: REMOVE label from node" {
    const path = "/tmp/lattice_qm_remove_label.ltdb";
    var db = try openTestDb(path, .{});
    defer cleanupTestDb(db, path);

    var r1 = try db.query("CREATE (n:Person:Admin {name: \"Alice\"})");
    r1.deinit();

    var r2 = try db.query("MATCH (n:Admin {name: \"Alice\"}) REMOVE n:Admin");
    r2.deinit();

    // Should no longer match Admin
    var result1 = try db.query("MATCH (n:Admin) RETURN count(n)");
    defer result1.deinit();
    try expectInt(result1, 0, 0, 0);

    // But still matches Person
    var result2 = try db.query("MATCH (n:Person) RETURN count(n)");
    defer result2.deinit();
    try expectInt(result2, 0, 0, 1);
}

// ============================================================================
// MERGE Tests
// ============================================================================

test "query: MERGE creates node when not exists" {
    const path = "/tmp/lattice_qm_merge_create.ltdb";
    var db = try openTestDb(path, .{});
    defer cleanupTestDb(db, path);

    var r = try db.query("MERGE (n:Person {name: \"Alice\"})");
    r.deinit();

    var result = try db.query("MATCH (n:Person) RETURN n.name");
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), result.rowCount());
    try expectString(result, 0, 0, "Alice");
}

test "query: MERGE finds existing node and does not duplicate" {
    const path = "/tmp/lattice_qm_merge_exists.ltdb";
    var db = try openTestDb(path, .{});
    defer cleanupTestDb(db, path);

    var r1 = try db.query("CREATE (n:Person {name: \"Alice\"})");
    r1.deinit();

    // MERGE should find existing, not create a second
    var r2 = try db.query("MERGE (n:Person {name: \"Alice\"})");
    r2.deinit();

    var result = try db.query("MATCH (n:Person) RETURN count(n)");
    defer result.deinit();
    try expectInt(result, 0, 0, 1);
}

test "query: MERGE with ON CREATE SET" {
    const path = "/tmp/lattice_qm_merge_oncreate.ltdb";
    var db = try openTestDb(path, .{});
    defer cleanupTestDb(db, path);

    var r = try db.query("MERGE (n:Person {name: \"Alice\"}) ON CREATE SET n.status = \"new\"");
    r.deinit();

    var result = try db.query("MATCH (n:Person {name: \"Alice\"}) RETURN n.status");
    defer result.deinit();
    try expectString(result, 0, 0, "new");
}

test "query: MERGE with ON MATCH SET" {
    const path = "/tmp/lattice_qm_merge_onmatch.ltdb";
    var db = try openTestDb(path, .{});
    defer cleanupTestDb(db, path);

    var r1 = try db.query("CREATE (n:Person {name: \"Alice\", visits: 1})");
    r1.deinit();

    // MERGE matches existing Alice, ON MATCH SET should fire
    var r2 = try db.query("MERGE (n:Person {name: \"Alice\"}) ON MATCH SET n.visits = 2");
    r2.deinit();

    var result = try db.query("MATCH (n:Person {name: \"Alice\"}) RETURN n.visits");
    defer result.deinit();
    try expectInt(result, 0, 0, 2);
}

test "query: MERGE fails on non-property pattern value and does not create partial node" {
    const path = "/tmp/lattice_qm_merge_type_error.ltdb";
    var db = try openTestDb(path, .{});
    defer cleanupTestDb(db, path);

    var seed = try db.query("CREATE (p:Person {name: \"Alice\"})");
    seed.deinit();

    try std.testing.expectError(
        QueryError.ExecutionError,
        db.query("MATCH (p:Person {name: \"Alice\"}) MERGE (m:Mirror {owner: p})"),
    );

    var mirrors = try db.query("MATCH (m:Mirror) RETURN count(m)");
    defer mirrors.deinit();
    try expectInt(mirrors, 0, 0, 0);
}

test "query: MERGE relationship with bound nodes is idempotent" {
    const path = "/tmp/lattice_qm_merge_rel_bound_idempotent.ltdb";
    var db = try openTestDb(path, .{});
    defer cleanupTestDb(db, path);

    var n1 = try db.query("CREATE (n:Person {name: \"Alice\"})");
    n1.deinit();
    var n2 = try db.query("CREATE (n:Person {name: \"Bob\"})");
    n2.deinit();

    var q1 = try db.query(
        "MATCH (a:Person {name: \"Alice\"}), (b:Person {name: \"Bob\"}) MERGE (a)-[r:REL]->(b) RETURN id(r)",
    );
    defer q1.deinit();
    try std.testing.expectEqual(@as(usize, 1), q1.rowCount());
    const edge_id = q1.rows[0].values[0].int_val;

    var q2 = try db.query(
        "MATCH (a:Person {name: \"Alice\"}), (b:Person {name: \"Bob\"}) MERGE (a)-[r:REL]->(b) RETURN id(r)",
    );
    defer q2.deinit();
    try std.testing.expectEqual(@as(usize, 1), q2.rowCount());
    try expectInt(q2, 0, 0, edge_id);

    var count = try db.query("MATCH (a)-[r:REL]->(b) RETURN count(r)");
    defer count.deinit();
    try expectInt(count, 0, 0, 1);
}

test "query: MERGE relationship supports ON CREATE and ON MATCH set" {
    const path = "/tmp/lattice_qm_merge_rel_on_create_on_match.ltdb";
    var db = try openTestDb(path, .{});
    defer cleanupTestDb(db, path);

    var n1 = try db.query("CREATE (n:Person {name: \"Alice\"})");
    n1.deinit();
    var n2 = try db.query("CREATE (n:Person {name: \"Bob\"})");
    n2.deinit();

    var q1 = try db.query(
        "MATCH (a:Person {name: \"Alice\"}), (b:Person {name: \"Bob\"}) MERGE (a)-[r:REL]->(b) ON CREATE SET r.since = 2020 ON MATCH SET r.since = 2021 RETURN id(r), r.since",
    );
    defer q1.deinit();
    try std.testing.expectEqual(@as(usize, 1), q1.rowCount());
    const edge_id = q1.rows[0].values[0].int_val;
    try expectInt(q1, 0, 1, 2020);

    var q2 = try db.query(
        "MATCH (a:Person {name: \"Alice\"}), (b:Person {name: \"Bob\"}) MERGE (a)-[r:REL]->(b) ON CREATE SET r.since = 2020 ON MATCH SET r.since = 2021 RETURN id(r), r.since",
    );
    defer q2.deinit();
    try std.testing.expectEqual(@as(usize, 1), q2.rowCount());
    try expectInt(q2, 0, 0, edge_id);
    try expectInt(q2, 0, 1, 2021);
}

test "query: MERGE relationship pattern properties match correct parallel edge" {
    const path = "/tmp/lattice_qm_merge_rel_pattern_props_parallel.ltdb";
    var db = try openTestDb(path, .{});
    defer cleanupTestDb(db, path);

    const source = try db.createNode(null, &[_][]const u8{"Person"});
    const target = try db.createNode(null, &[_][]const u8{"Person"});
    try db.setNodeProperty(null, source, "name", .{ .string_val = "Alice" });
    try db.setNodeProperty(null, target, "name", .{ .string_val = "Bob" });

    const rel_type = try db.symbol_table.intern("REL");
    const weight_key = try db.symbol_table.intern("w");
    const props1 = [_]NodeProperty{
        .{ .key_id = weight_key, .value = .{ .int_val = 1 } },
    };
    const props2 = [_]NodeProperty{
        .{ .key_id = weight_key, .value = .{ .int_val = 2 } },
    };
    const edge1 = try db.edge_store.createAndGetId(source, target, rel_type, &props1);
    const edge2 = try db.edge_store.createAndGetId(source, target, rel_type, &props2);

    var result = try db.query(
        "MERGE (a:Person {name: \"Alice\"})-[r:REL {w: 2}]->(b:Person {name: \"Bob\"}) RETURN id(r), r.w",
    );
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), result.rowCount());
    try expectInt(result, 0, 0, @intCast(edge2));
    try expectInt(result, 0, 1, 2);

    var count_after_match = try db.query("MATCH (a)-[r:REL]->(b) RETURN count(r)");
    defer count_after_match.deinit();
    try expectInt(count_after_match, 0, 0, 2);

    var created = try db.query(
        "MERGE (a:Person {name: \"Alice\"})-[r:REL {w: 3}]->(b:Person {name: \"Bob\"}) RETURN id(r), r.w",
    );
    defer created.deinit();
    try std.testing.expectEqual(@as(usize, 1), created.rowCount());
    try expectInt(created, 0, 1, 3);
    try std.testing.expect(created.rows[0].values[0].int_val > @as(i64, @intCast(edge2)));

    var count_after_create = try db.query("MATCH (a)-[r:REL]->(b) RETURN count(r)");
    defer count_after_create.deinit();
    try expectInt(count_after_create, 0, 0, 3);

    _ = edge1;
}

test "query: standalone MERGE relationship creates graph once" {
    const path = "/tmp/lattice_qm_merge_rel_standalone_once.ltdb";
    var db = try openTestDb(path, .{});
    defer cleanupTestDb(db, path);

    var q1 = try db.query(
        "MERGE (a:Person {name: \"Alice\"})-[r:KNOWS]->(b:Person {name: \"Bob\"}) RETURN id(a), id(b), id(r)",
    );
    defer q1.deinit();
    try std.testing.expectEqual(@as(usize, 1), q1.rowCount());
    const edge_id = q1.rows[0].values[2].int_val;

    var q2 = try db.query(
        "MERGE (a:Person {name: \"Alice\"})-[r:KNOWS]->(b:Person {name: \"Bob\"}) RETURN id(a), id(b), id(r)",
    );
    defer q2.deinit();
    try std.testing.expectEqual(@as(usize, 1), q2.rowCount());
    try expectInt(q2, 0, 2, edge_id);

    var node_count = try db.query("MATCH (n:Person) RETURN count(n)");
    defer node_count.deinit();
    try expectInt(node_count, 0, 0, 2);

    var edge_count = try db.query("MATCH (:Person)-[r:KNOWS]->(:Person) RETURN count(r)");
    defer edge_count.deinit();
    try expectInt(edge_count, 0, 0, 1);
}

// ============================================================================
// Combined / Multi-clause Tests
// ============================================================================

test "query: MATCH + SET + RETURN in sequence" {
    const path = "/tmp/lattice_qm_match_set.ltdb";
    var db = try openTestDb(path, .{});
    defer cleanupTestDb(db, path);

    var r1 = try db.query("CREATE (n:Counter {name: \"hits\", value: 0})");
    r1.deinit();

    // Increment the counter
    var r2 = try db.query("MATCH (n:Counter {name: \"hits\"}) SET n.value = 1");
    r2.deinit();
    var r3 = try db.query("MATCH (n:Counter {name: \"hits\"}) SET n.value = 2");
    r3.deinit();
    var r4 = try db.query("MATCH (n:Counter {name: \"hits\"}) SET n.value = 3");
    r4.deinit();

    var result = try db.query("MATCH (n:Counter {name: \"hits\"}) RETURN n.value");
    defer result.deinit();
    try expectInt(result, 0, 0, 3);
}

test "query: create graph, query with ORDER BY and LIMIT" {
    const path = "/tmp/lattice_qm_order_limit.ltdb";
    var db = try openTestDb(path, .{});
    defer cleanupTestDb(db, path);

    var r1 = try db.query("CREATE (n:Person {name: \"Alice\", age: 30})");
    r1.deinit();
    var r2 = try db.query("CREATE (n:Person {name: \"Bob\", age: 25})");
    r2.deinit();
    var r3 = try db.query("CREATE (n:Person {name: \"Carol\", age: 35})");
    r3.deinit();

    var result = try db.query("MATCH (n:Person) RETURN n.name ORDER BY n.age LIMIT 2");
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 2), result.rowCount());
    // Youngest first: Bob (25), Alice (30)
    try expectString(result, 0, 0, "Bob");
    try expectString(result, 1, 0, "Alice");
}

test "query: ORDER BY supports non-returned keys with SKIP and LIMIT" {
    const path = "/tmp/lattice_qm_order_skip_limit_non_returned_key.ltdb";
    var db = try openTestDb(path, .{});
    defer cleanupTestDb(db, path);

    var r1 = try db.query("CREATE (n:Person {name: \"Alice\", age: 20})");
    r1.deinit();
    var r2 = try db.query("CREATE (n:Person {name: \"Bob\", age: 30})");
    r2.deinit();
    var r3 = try db.query("CREATE (n:Person {name: \"Carol\", age: 40})");
    r3.deinit();

    // Sorted by age: Alice, Bob, Carol -> SKIP 1 LIMIT 1 returns Bob
    var result = try db.query("MATCH (n:Person) RETURN n.name ORDER BY n.age SKIP 1 LIMIT 1");
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), result.rowCount());
    try expectString(result, 0, 0, "Bob");
}

test "query: LIMIT supports arithmetic expression" {
    const path = "/tmp/lattice_qm_limit_arithmetic_expr.ltdb";
    var db = try openTestDb(path, .{});
    defer cleanupTestDb(db, path);

    var r1 = try db.query("CREATE (n:Person {name: \"Alice\", age: 30})");
    r1.deinit();
    var r2 = try db.query("CREATE (n:Person {name: \"Bob\", age: 25})");
    r2.deinit();
    var r3 = try db.query("CREATE (n:Person {name: \"Carol\", age: 35})");
    r3.deinit();

    var result = try db.query("MATCH (n:Person) RETURN n.name ORDER BY n.age LIMIT 1 + 1");
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 2), result.rowCount());
    try expectString(result, 0, 0, "Bob");
    try expectString(result, 1, 0, "Alice");
}

test "query: SKIP and LIMIT support parameters" {
    const path = "/tmp/lattice_qm_skip_limit_params.ltdb";
    var db = try openTestDb(path, .{});
    defer cleanupTestDb(db, path);

    var r1 = try db.query("CREATE (n:Person {name: \"Alice\", age: 20})");
    r1.deinit();
    var r2 = try db.query("CREATE (n:Person {name: \"Bob\", age: 30})");
    r2.deinit();
    var r3 = try db.query("CREATE (n:Person {name: \"Carol\", age: 40})");
    r3.deinit();

    var params = std.StringHashMap(PropertyValue).init(std.testing.allocator);
    defer params.deinit();
    try params.put("skip", .{ .int_val = 1 });
    try params.put("limit", .{ .int_val = 1 });

    var result = try db.queryWithParams(
        "MATCH (n:Person) RETURN n.name ORDER BY n.age SKIP $skip LIMIT $limit",
        &params,
    );
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), result.rowCount());
    try expectString(result, 0, 0, "Bob");
}

test "query: create and query graph cycle" {
    const path = "/tmp/lattice_qm_cycle.ltdb";
    var db = try openTestDb(path, .{});
    defer cleanupTestDb(db, path);

    // Create A -> B -> C -> A (cycle)
    var r1 = try db.query("CREATE (a:Node {id: 1})");
    r1.deinit();
    var r2 = try db.query("CREATE (b:Node {id: 2})");
    r2.deinit();
    var r3 = try db.query("CREATE (c:Node {id: 3})");
    r3.deinit();

    var r4 = try db.query("MATCH (a:Node {id: 1}), (b:Node {id: 2}) CREATE (a)-[:NEXT]->(b)");
    r4.deinit();
    var r5 = try db.query("MATCH (b:Node {id: 2}), (c:Node {id: 3}) CREATE (b)-[:NEXT]->(c)");
    r5.deinit();
    var r6 = try db.query("MATCH (c:Node {id: 3}), (a:Node {id: 1}) CREATE (c)-[:NEXT]->(a)");
    r6.deinit();

    // Each node should have exactly one outgoing NEXT edge
    var result = try db.query("MATCH (a:Node)-[:NEXT]->(b:Node) RETURN count(a)");
    defer result.deinit();
    try expectInt(result, 0, 0, 3);
}

test "query: RETURN DISTINCT distinguishes long shared-prefix strings" {
    const path = "/tmp/lattice_qm_distinct_long_strings.ltdb";
    var db = try openTestDb(path, .{});
    defer cleanupTestDb(db, path);

    const allocator = std.testing.allocator;
    const prefix_len: usize = 1300;
    var s1 = try allocator.alloc(u8, prefix_len + 1);
    defer allocator.free(s1);
    var s2 = try allocator.alloc(u8, prefix_len + 1);
    defer allocator.free(s2);
    @memset(s1[0..prefix_len], 'a');
    @memset(s2[0..prefix_len], 'a');
    s1[prefix_len] = 'x';
    s2[prefix_len] = 'y';

    const n1 = try db.createNode(null, &[_][]const u8{"Doc"});
    try db.setNodeProperty(null, n1, "name", .{ .string_val = s1 });
    const n2 = try db.createNode(null, &[_][]const u8{"Doc"});
    try db.setNodeProperty(null, n2, "name", .{ .string_val = s2 });

    var result = try db.query("MATCH (n:Doc) RETURN DISTINCT n.name ORDER BY n.name");
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 2), result.rowCount());
    const first = switch (result.rows[0].values[0]) {
        .string_val => |v| v,
        else => return error.UnexpectedValueType,
    };
    const second = switch (result.rows[1].values[0]) {
        .string_val => |v| v,
        else => return error.UnexpectedValueType,
    };
    try std.testing.expect(!std.mem.eql(u8, first, second));
    const saw_s1_s2 = std.mem.eql(u8, first, s1) and std.mem.eql(u8, second, s2);
    const saw_s2_s1 = std.mem.eql(u8, first, s2) and std.mem.eql(u8, second, s1);
    try std.testing.expect(saw_s1_s2 or saw_s2_s1);
}

test "query: GROUP BY and COUNT DISTINCT separate long shared-prefix strings" {
    const path = "/tmp/lattice_qm_group_long_strings.ltdb";
    var db = try openTestDb(path, .{});
    defer cleanupTestDb(db, path);

    const allocator = std.testing.allocator;
    const prefix_len: usize = 1300;
    var s1 = try allocator.alloc(u8, prefix_len + 1);
    defer allocator.free(s1);
    var s2 = try allocator.alloc(u8, prefix_len + 1);
    defer allocator.free(s2);
    @memset(s1[0..prefix_len], 'a');
    @memset(s2[0..prefix_len], 'a');
    s1[prefix_len] = 'x';
    s2[prefix_len] = 'y';

    const n1 = try db.createNode(null, &[_][]const u8{"Doc"});
    try db.setNodeProperty(null, n1, "name", .{ .string_val = s1 });
    const n2 = try db.createNode(null, &[_][]const u8{"Doc"});
    try db.setNodeProperty(null, n2, "name", .{ .string_val = s2 });

    var grouped = try db.query("MATCH (n:Doc) RETURN n.name, count(n) ORDER BY n.name");
    defer grouped.deinit();

    try std.testing.expectEqual(@as(usize, 2), grouped.rowCount());
    const g0 = switch (grouped.rows[0].values[0]) {
        .string_val => |v| v,
        else => return error.UnexpectedValueType,
    };
    const g1 = switch (grouped.rows[1].values[0]) {
        .string_val => |v| v,
        else => return error.UnexpectedValueType,
    };
    try std.testing.expect(!std.mem.eql(u8, g0, g1));
    const grouped_has_both = (std.mem.eql(u8, g0, s1) and std.mem.eql(u8, g1, s2)) or
        (std.mem.eql(u8, g0, s2) and std.mem.eql(u8, g1, s1));
    try std.testing.expect(grouped_has_both);
    try expectInt(grouped, 0, 1, 1);
    try expectInt(grouped, 1, 1, 1);

    var distinct_count = try db.query("MATCH (n:Doc) RETURN count(DISTINCT n.name)");
    defer distinct_count.deinit();
    try expectInt(distinct_count, 0, 0, 2);
}

test "query: full lifecycle - create, read, update, delete" {
    const path = "/tmp/lattice_qm_lifecycle.ltdb";
    var db = try openTestDb(path, .{});
    defer cleanupTestDb(db, path);

    // CREATE
    var r1 = try db.query("CREATE (n:Task {title: \"Write tests\", status: \"todo\"})");
    r1.deinit();

    // READ
    var r2 = try db.query("MATCH (n:Task {title: \"Write tests\"}) RETURN n.status");
    defer r2.deinit();
    try expectString(r2, 0, 0, "todo");

    // UPDATE
    var r3 = try db.query("MATCH (n:Task {title: \"Write tests\"}) SET n.status = \"done\"");
    r3.deinit();

    var r4 = try db.query("MATCH (n:Task {title: \"Write tests\"}) RETURN n.status");
    defer r4.deinit();
    try expectString(r4, 0, 0, "done");

    // DELETE
    var r5 = try db.query("MATCH (n:Task {title: \"Write tests\"}) DELETE n");
    r5.deinit();

    var r6 = try db.query("MATCH (n:Task) RETURN count(n)");
    defer r6.deinit();
    try expectInt(r6, 0, 0, 0);
}

// ============================================================================
// Vector Search via Cypher (<=> operator)
// ============================================================================

test "query: vector search with <=> operator" {
    const path = "/tmp/lattice_qm_vector_search.ltdb";
    var db = try openTestDb(path, .{ .enable_vector = true, .vector_dimensions = 4 });
    defer cleanupTestDb(db, path);

    // Create nodes with vectors via direct API (vectors aren't yet settable via Cypher CREATE)
    const n1 = try db.createNode(null, &[_][]const u8{"Document"});
    try db.setNodeProperty(null, n1, "title", .{ .string_val = "Neural Networks" });
    try db.setNodeVector(n1, &[_]f32{ 1.0, 0.0, 0.0, 0.0 });

    const n2 = try db.createNode(null, &[_][]const u8{"Document"});
    try db.setNodeProperty(null, n2, "title", .{ .string_val = "Deep Learning" });
    try db.setNodeVector(n2, &[_]f32{ 0.9, 0.1, 0.0, 0.0 });

    const n3 = try db.createNode(null, &[_][]const u8{"Document"});
    try db.setNodeProperty(null, n3, "title", .{ .string_val = "Cooking Recipes" });
    try db.setNodeVector(n3, &[_]f32{ 0.0, 0.0, 1.0, 0.0 });

    // Search for vectors similar to [1, 0, 0, 0] via Cypher
    var result = try db.query("MATCH (d:Document) WHERE d.embedding <=> [1.0, 0.0, 0.0, 0.0] RETURN d.title LIMIT 2");
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 2), result.rowCount());
    // Closest should be "Neural Networks" (exact match), then "Deep Learning"
    try expectString(result, 0, 0, "Neural Networks");
    try expectString(result, 1, 0, "Deep Learning");
}

test "query: vector distance works inside generic WHERE predicate" {
    const path = "/tmp/lattice_qm_vector_generic_where.ltdb";
    var db = try openTestDb(path, .{});
    defer cleanupTestDb(db, path);

    const n1 = try db.createNode(null, &[_][]const u8{"Document"});
    try db.setNodeProperty(null, n1, "title", .{ .string_val = "Neural Networks" });

    const n2 = try db.createNode(null, &[_][]const u8{"Document"});
    try db.setNodeProperty(null, n2, "title", .{ .string_val = "Deep Learning" });

    // Constant vector expression forces generic filter path (planner won't use specialized vector operator).
    var result = try db.query(
        "MATCH (d:Document) WHERE ([1.0, 0.0, 0.0, 0.0] <=> [1.0, 0.0, 0.0, 0.0]) = 0.0 RETURN d.title",
    );
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 2), result.rowCount());
}

test "query: vector search respects MATCH input constraints" {
    const path = "/tmp/lattice_qm_vector_input_constraints.ltdb";
    var db = try openTestDb(path, .{ .enable_vector = true, .vector_dimensions = 4 });
    defer cleanupTestDb(db, path);

    const doc = try db.createNode(null, &[_][]const u8{"Document"});
    try db.setNodeProperty(null, doc, "name", .{ .string_val = "Doc Candidate" });
    try db.setNodeVector(doc, &[_]f32{ 0.99, 0.01, 0.0, 0.0 });

    const person = try db.createNode(null, &[_][]const u8{"Person"});
    try db.setNodeProperty(null, person, "name", .{ .string_val = "Person Closest" });
    try db.setNodeVector(person, &[_]f32{ 1.0, 0.0, 0.0, 0.0 });

    const far_doc = try db.createNode(null, &[_][]const u8{"Document"});
    try db.setNodeProperty(null, far_doc, "name", .{ .string_val = "Doc Far" });
    try db.setNodeVector(far_doc, &[_]f32{ 0.0, 1.0, 0.0, 0.0 });

    var params = std.StringHashMap(PropertyValue).init(std.testing.allocator);
    defer params.deinit();
    const query_vec = [_]f32{ 1.0, 0.0, 0.0, 0.0 };
    try params.put("query", .{ .vector_val = &query_vec });

    var result = try db.queryWithParams(
        "MATCH (d:Document) WHERE d.embedding <=> $query RETURN d.name LIMIT 1",
        &params,
    );
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 1), result.rowCount());
    try expectString(result, 0, 0, "Doc Candidate");
}

// ============================================================================
// Full-Text Search via Cypher (@@ operator)
// ============================================================================

test "query: full-text search with @@ operator" {
    const path = "/tmp/lattice_qm_fts_search.ltdb";
    var db = try openTestDb(path, .{ .enable_fts = true });
    defer cleanupTestDb(db, path);

    // Create documents with text content
    const n1 = try db.createNode(null, &[_][]const u8{"Article"});
    try db.setNodeProperty(null, n1, "title", .{ .string_val = "Graph Databases" });
    try db.ftsIndexDocument(n1, "Graph databases store data as nodes and edges for efficient traversal");

    const n2 = try db.createNode(null, &[_][]const u8{"Article"});
    try db.setNodeProperty(null, n2, "title", .{ .string_val = "Relational Databases" });
    try db.ftsIndexDocument(n2, "Relational databases use tables and SQL for data management");

    const n3 = try db.createNode(null, &[_][]const u8{"Article"});
    try db.setNodeProperty(null, n3, "title", .{ .string_val = "Neural Networks" });
    try db.ftsIndexDocument(n3, "Deep learning and neural networks for pattern recognition");

    // Search via Cypher
    var result = try db.query("MATCH (a:Article) WHERE a.text @@ \"graph databases\" RETURN a.title");
    defer result.deinit();
    // Should find the graph databases article
    try std.testing.expect(result.rowCount() >= 1);
    try expectString(result, 0, 0, "Graph Databases");
}

test "query: full-text operator works in generic WHERE predicate" {
    const path = "/tmp/lattice_qm_fts_generic_where.ltdb";
    var db = try openTestDb(path, .{});
    defer cleanupTestDb(db, path);

    var r1 = try db.query("CREATE (d:Doc {title: \"Graph Databases\", content: \"Graph databases store data as nodes and edges\"})");
    r1.deinit();
    var r2 = try db.query("CREATE (d:Doc {title: \"Relational Databases\", content: \"Relational databases use tables and SQL\"})");
    r2.deinit();

    // Constant @@ expression forces generic filter path (planner won't use specialized FTS operator).
    var result = try db.query(
        "MATCH (d:Doc) WHERE \"Graph databases store data\" @@ \"graph databases\" RETURN d.title",
    );
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 2), result.rowCount());
}

// ============================================================================
// WHERE Clause with Mutation Queries
// ============================================================================

test "query: WHERE with comparison operators" {
    const path = "/tmp/lattice_qm_where_ops.ltdb";
    var db = try openTestDb(path, .{});
    defer cleanupTestDb(db, path);

    var r1 = try db.query("CREATE (n:Product {name: \"A\", price: 10})");
    r1.deinit();
    var r2 = try db.query("CREATE (n:Product {name: \"B\", price: 20})");
    r2.deinit();
    var r3 = try db.query("CREATE (n:Product {name: \"C\", price: 30})");
    r3.deinit();

    // Greater than
    var result1 = try db.query("MATCH (n:Product) WHERE n.price > 15 RETURN count(n)");
    defer result1.deinit();
    try expectInt(result1, 0, 0, 2);

    // Less than or equal
    var result2 = try db.query("MATCH (n:Product) WHERE n.price <= 20 RETURN count(n)");
    defer result2.deinit();
    try expectInt(result2, 0, 0, 2);

    // Equality
    var result3 = try db.query("MATCH (n:Product) WHERE n.price = 20 RETURN n.name");
    defer result3.deinit();
    try std.testing.expectEqual(@as(usize, 1), result3.rowCount());
    try expectString(result3, 0, 0, "B");
}

test "query: incompatible > and >= comparisons evaluate to null (no panic)" {
    const path = "/tmp/lattice_qm_where_incompatible_comparison.ltdb";
    var db = try openTestDb(path, .{});
    defer cleanupTestDb(db, path);

    var seed = try db.query("CREATE (n:Person {name: \"Alice\"})");
    seed.deinit();

    var gt_result = try db.query("MATCH (n:Person) WHERE n.name > 10 RETURN count(n)");
    defer gt_result.deinit();
    try expectInt(gt_result, 0, 0, 0);

    var gte_result = try db.query("MATCH (n:Person) WHERE n.name >= 10 RETURN count(n)");
    defer gte_result.deinit();
    try expectInt(gte_result, 0, 0, 0);
}

test "query: WHERE with string matching" {
    const path = "/tmp/lattice_qm_where_string.ltdb";
    var db = try openTestDb(path, .{});
    defer cleanupTestDb(db, path);

    var r1 = try db.query("CREATE (n:Person {name: \"Alice\", city: \"New York\"})");
    r1.deinit();
    var r2 = try db.query("CREATE (n:Person {name: \"Bob\", city: \"Boston\"})");
    r2.deinit();

    var result = try db.query("MATCH (n:Person) WHERE n.city = \"New York\" RETURN n.name");
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), result.rowCount());
    try expectString(result, 0, 0, "Alice");
}

// ============================================================================
// Edge Cases
// ============================================================================

test "query: CREATE then immediately query same label" {
    const path = "/tmp/lattice_qm_create_query.ltdb";
    var db = try openTestDb(path, .{});
    defer cleanupTestDb(db, path);

    // Ensure that a CREATE is visible to a subsequent query in the same session
    var r = try db.query("CREATE (n:Ephemeral {val: 42})");
    r.deinit();

    var result = try db.query("MATCH (n:Ephemeral) RETURN n.val");
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), result.rowCount());
    try expectInt(result, 0, 0, 42);
}

test "query: DELETE on empty result set is no-op" {
    const path = "/tmp/lattice_qm_delete_empty.ltdb";
    var db = try openTestDb(path, .{});
    defer cleanupTestDb(db, path);

    // This should not error - deleting nothing is fine
    var r = try db.query("MATCH (n:NonExistent) DELETE n");
    r.deinit();
}

test "query: SET on empty result set is no-op" {
    const path = "/tmp/lattice_qm_set_empty.ltdb";
    var db = try openTestDb(path, .{});
    defer cleanupTestDb(db, path);

    var r = try db.query("MATCH (n:NonExistent) SET n.x = 1");
    r.deinit();
}

test "query: CREATE many nodes then count" {
    const path = "/tmp/lattice_qm_bulk_create.ltdb";
    var db = try openTestDb(path, .{});
    defer cleanupTestDb(db, path);

    const count = 50;
    for (0..count) |i| {
        var buf: [128]u8 = undefined;
        const q = std.fmt.bufPrint(&buf, "CREATE (n:Item {{idx: {d}}})", .{i}) catch unreachable;
        var r = try db.query(q);
        r.deinit();
    }

    var result = try db.query("MATCH (n:Item) RETURN count(n)");
    defer result.deinit();
    try expectInt(result, 0, 0, count);
}

test "query: UNWIND binds output variable for RETURN" {
    const path = "/tmp/lattice_qm_unwind_return_binding.ltdb";
    var db = try openTestDb(path, .{});
    defer cleanupTestDb(db, path);

    var seed = try db.query("CREATE (n:Seed {id: 1})");
    seed.deinit();

    var result = try db.query("MATCH (n:Seed) UNWIND [1, 2, 3] AS x RETURN x ORDER BY x");
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 3), result.rowCount());
    try expectInt(result, 0, 0, 1);
    try expectInt(result, 1, 0, 2);
    try expectInt(result, 2, 0, 3);
}

test "query: UNWIND variable is usable in downstream WHERE" {
    const path = "/tmp/lattice_qm_unwind_where_binding.ltdb";
    var db = try openTestDb(path, .{});
    defer cleanupTestDb(db, path);

    var seed = try db.query("CREATE (n:Seed {id: 1})");
    seed.deinit();

    var result = try db.query("MATCH (n:Seed) UNWIND [1, 2, 3] AS x WHERE x >= 2 RETURN x ORDER BY x");
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 2), result.rowCount());
    try expectInt(result, 0, 0, 2);
    try expectInt(result, 1, 0, 3);
}

test "query: standalone UNWIND without MATCH produces rows" {
    const path = "/tmp/lattice_qm_unwind_standalone.ltdb";
    var db = try openTestDb(path, .{});
    defer cleanupTestDb(db, path);

    var result = try db.query("UNWIND [1, 2, 3] AS x RETURN x ORDER BY x");
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 3), result.rowCount());
    try expectInt(result, 0, 0, 1);
    try expectInt(result, 1, 0, 2);
    try expectInt(result, 2, 0, 3);
}

test "query: standalone UNWIND supports list parameter" {
    const path = "/tmp/lattice_qm_unwind_param_list.ltdb";
    var db = try openTestDb(path, .{});
    defer cleanupTestDb(db, path);

    var items = [_]PropertyValue{
        .{ .int_val = 3 },
        .{ .int_val = 1 },
        .{ .int_val = 2 },
    };
    var params = std.StringHashMap(PropertyValue).init(std.testing.allocator);
    defer params.deinit();
    try params.put("vals", .{ .list_val = &items });

    var result = try db.queryWithParams("UNWIND $vals AS x RETURN x ORDER BY x", &params);
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 3), result.rowCount());
    try expectInt(result, 0, 0, 1);
    try expectInt(result, 1, 0, 2);
    try expectInt(result, 2, 0, 3);
}

// ============================================================================
// Test Helpers
// ============================================================================

fn expectInt(result: anytype, row: usize, col: usize, expected: i64) !void {
    switch (result.rows[row].values[col]) {
        .int_val => |v| try std.testing.expectEqual(expected, v),
        else => return error.UnexpectedValueType,
    }
}

fn expectString(result: anytype, row: usize, col: usize, expected: []const u8) !void {
    switch (result.rows[row].values[col]) {
        .string_val => |v| try std.testing.expectEqualStrings(expected, v),
        else => return error.UnexpectedValueType,
    }
}

fn expectFloat(result: anytype, row: usize, col: usize, expected: f64, tolerance: f64) !void {
    switch (result.rows[row].values[col]) {
        .float_val => |v| try std.testing.expectApproxEqAbs(expected, v, tolerance),
        else => return error.UnexpectedValueType,
    }
}

fn expectBool(result: anytype, row: usize, col: usize, expected: bool) !void {
    switch (result.rows[row].values[col]) {
        .bool_val => |v| try std.testing.expectEqual(expected, v),
        else => return error.UnexpectedValueType,
    }
}
