//! Cypher statements that write `n.embedding`, which reads a node's vector.
//!
//! Until 0.18.0 every write to `embedding` went to an ordinary property of
//! that name, which the vector shadows on read: on a node with a vector,
//! `REMOVE n.embedding` and `SET n.embedding = null` succeeded and appeared to
//! do nothing.

const std = @import("std");
const lattice = @import("lattice");

const Database = lattice.storage.database.Database;
const NodeId = lattice.core.types.NodeId;
const ResultValue = lattice.storage.database.ResultValue;

const dims = 2;

fn cleanup(path: []const u8) void {
    const cwd = @import("compat").fs.cwd();
    var buf: [256]u8 = undefined;
    cwd.deleteFile(path) catch {};
    cwd.deleteFile(std.fmt.bufPrint(&buf, "{s}-wal", .{path}) catch return) catch {};
    cwd.deleteFile(std.fmt.bufPrint(&buf, "{s}-journal", .{path}) catch return) catch {};
}

fn openDb(path: []const u8, create: bool, vectors: bool) !*Database {
    return Database.open(std.testing.allocator, path, .{ .create = create, .config = .{
        .enable_fts = false,
        .enable_vector = vectors,
        .vector_dimensions = dims,
        .vector_metric = .euclidean,
    } });
}

fn run(db: *Database, cypher: []const u8) !void {
    var result = try db.query(cypher);
    result.deinit();
}

fn runInTxn(db: *Database, txn: *lattice.transaction.manager.Transaction, cypher: []const u8) !void {
    var result = try db.queryInTxn(txn, cypher);
    result.deinit();
}

/// The kind of value `MATCH (n) WHERE id(n) = <node> RETURN n.embedding`
/// returns.
fn readEmbedding(db: *Database, node: NodeId) !std.meta.Tag(ResultValue) {
    var buf: [128]u8 = undefined;
    const cypher = try std.fmt.bufPrint(&buf, "MATCH (n) WHERE id(n) = {d} RETURN n.embedding", .{node});
    var result = try db.query(cypher);
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), result.rowCount());
    return result.rows[0].values[0];
}

fn hasVector(db: *Database, txn: ?*lattice.transaction.manager.Transaction, node: NodeId) !bool {
    const vector = (try db.getNodeVectorInTxn(txn, node)) orelse return false;
    db.allocator.free(vector);
    return true;
}

fn searchFinds(db: *Database, node: NodeId) !bool {
    const results = try db.vectorSearch(&[_]f32{ 1, 0 }, 10, 64);
    defer db.freeVectorSearchResults(results);
    for (results) |result| {
        if (result.node_id == node) return true;
    }
    return false;
}

fn addNodeWithVector(db: *Database, label: []const u8, vector: []const f32) !NodeId {
    var txn = try db.beginTransaction(.read_write);
    const node = try db.createNode(&txn, &.{label});
    try db.setNodeVectorInTxn(&txn, node, vector);
    try db.commitTransaction(&txn);
    return node;
}

test "lvc: REMOVE n.embedding and SET n.embedding = null remove the vector" {
    const path = "/tmp/lvc_cypher_vector_remove.ltdb";
    cleanup(path);
    defer cleanup(path);

    var removed: NodeId = undefined;
    var nulled: NodeId = undefined;
    var kept: NodeId = undefined;
    {
        var db = try openDb(path, true, true);
        defer db.close();
        removed = try addNodeWithVector(db, "A", &.{ 1, 0 });
        nulled = try addNodeWithVector(db, "B", &.{ 0.9, 0.1 });
        kept = try addNodeWithVector(db, "C", &.{ 0, 1 });

        try run(db, "MATCH (n:A) REMOVE n.embedding");
        try run(db, "MATCH (n:B) SET n.embedding = null");

        for ([_]NodeId{ removed, nulled }) |node| {
            try std.testing.expect((try readEmbedding(db, node)) == .null_val);
            try std.testing.expect(!try hasVector(db, null, node));
            try std.testing.expect(!try searchFinds(db, node));
            try std.testing.expect(try db.nodeExists(node));
        }
        try std.testing.expect(try searchFinds(db, kept));

        // Removing it again is not an error and changes nothing.
        try run(db, "MATCH (n:A) REMOVE n.embedding");
        try std.testing.expect(try searchFinds(db, kept));
    }

    var db = try openDb(path, false, true);
    defer db.close();
    try std.testing.expect(!try hasVector(db, null, removed));
    try std.testing.expect(!try hasVector(db, null, nulled));
    try std.testing.expect(try hasVector(db, null, kept));
}

test "lvc: REMOVE n.embedding in a transaction is visible to it before commit" {
    const path = "/tmp/lvc_cypher_vector_remove_txn.ltdb";
    cleanup(path);
    defer cleanup(path);

    var db = try openDb(path, true, true);
    defer db.close();
    const node = try addNodeWithVector(db, "A", &.{ 1, 0 });

    var txn = try db.beginTransaction(.read_write);
    try runInTxn(db, &txn, "MATCH (n:A) REMOVE n.embedding");
    try std.testing.expect(!try hasVector(db, &txn, node));
    try std.testing.expect(try hasVector(db, null, node));
    try db.commitTransaction(&txn);
    try std.testing.expect(!try hasVector(db, null, node));
    try std.testing.expect(!try searchFinds(db, node));
}

test "lvc: REMOVE n.embedding also removes an ordinary property of that name" {
    const path = "/tmp/lvc_cypher_vector_remove_property.ltdb";
    cleanup(path);
    defer cleanup(path);

    var db = try openDb(path, true, true);
    defer db.close();
    var txn = try db.beginTransaction(.read_write);
    const node = try db.createNode(&txn, &.{"A"});
    var items = [_]lattice.core.types.PropertyValue{ .{ .int_val = 1 }, .{ .int_val = 2 } };
    try db.setNodeProperty(&txn, node, "embedding", .{ .list_val = &items });
    try db.commitTransaction(&txn);
    // A node with no vector reads the property back...
    try std.testing.expect((try readEmbedding(db, node)) == .list_val);

    try run(db, "MATCH (n:A) REMOVE n.embedding");
    // ...until it is removed.
    try std.testing.expect((try readEmbedding(db, node)) == .null_val);
    try std.testing.expect((try db.getNodePropertyInTxn(null, node, "embedding")) == null);
}

test "lvc: without a vector index REMOVE n.embedding removes the property only" {
    const path = "/tmp/lvc_cypher_vector_remove_noindex.ltdb";
    cleanup(path);
    defer cleanup(path);

    var db = try openDb(path, true, false);
    defer db.close();
    try run(db, "CREATE (:A {embedding: [1, 2], name: 'a'})");
    var result = try db.query("MATCH (n:A) RETURN n.embedding");
    try std.testing.expect(result.rows[0].values[0] == .list_val);
    result.deinit();
    try run(db, "MATCH (n:A) REMOVE n.embedding");
    result = try db.query("MATCH (n:A) RETURN n.embedding, n.name");
    defer result.deinit();
    try std.testing.expect(result.rows[0].values[0] == .null_val);
    try std.testing.expectEqualStrings("a", result.rows[0].values[1].string_val);
}

// ============================================================================
// Writing the vector from Cypher
// ============================================================================

fn vectorOf(db: *Database, txn: ?*lattice.transaction.manager.Transaction, node: NodeId) ![]f32 {
    return (try db.getNodeVectorInTxn(txn, node)) orelse error.VectorMissing;
}

fn expectVector(db: *Database, txn: ?*lattice.transaction.manager.Transaction, node: NodeId, expected: []const f32) !void {
    const vector = try vectorOf(db, txn, node);
    defer db.allocator.free(vector);
    try std.testing.expectEqualSlices(f32, expected, vector);
}

fn countNodes(db: *Database, cypher: []const u8) !i64 {
    var result = try db.query(cypher);
    defer result.deinit();
    return result.rows[0].values[0].int_val;
}

test "lvc: SET n.embedding = <list> sets the node's vector" {
    const path = "/tmp/lvc_cypher_vector_set.ltdb";
    cleanup(path);
    defer cleanup(path);

    var node: NodeId = undefined;
    {
        var db = try openDb(path, true, true);
        defer db.close();
        var txn = try db.beginTransaction(.read_write);
        node = try db.createNode(&txn, &.{"A"});
        // An ordinary property of the same name, from before vectors were set
        // from Cypher, goes too: it must not come back when the vector does.
        var items = [_]lattice.core.types.PropertyValue{ .{ .int_val = 7 }, .{ .int_val = 7 } };
        try db.setNodeProperty(&txn, node, "embedding", .{ .list_val = &items });
        try db.commitTransaction(&txn);

        txn = try db.beginTransaction(.read_write);
        try runInTxn(db, &txn, "MATCH (n:A) SET n.embedding = [1, 0]");
        try expectVector(db, &txn, node, &.{ 1, 0 });
        try std.testing.expect(!try hasVector(db, null, node));
        try db.commitTransaction(&txn);

        try expectVector(db, null, node, &.{ 1, 0 });
        try std.testing.expect(try searchFinds(db, node));
        try std.testing.expect((try db.getNodePropertyInTxn(null, node, "embedding")) == null);

        // Replaced, with floats this time.
        try run(db, "MATCH (n:A) SET n.embedding = [0.5, 0.25]");
        try expectVector(db, null, node, &.{ 0.5, 0.25 });
    }

    var db = try openDb(path, false, true);
    defer db.close();
    try expectVector(db, null, node, &.{ 0.5, 0.25 });
    try run(db, "MATCH (n:A) REMOVE n.embedding");
    try std.testing.expect((try readEmbedding(db, node)) == .null_val);
}

test "lvc: SET n.embedding = $vector takes a vector parameter" {
    const path = "/tmp/lvc_cypher_vector_set_param.ltdb";
    cleanup(path);
    defer cleanup(path);

    var db = try openDb(path, true, true);
    defer db.close();
    try run(db, "CREATE (:A)");
    var params = std.StringHashMap(lattice.core.types.PropertyValue).init(std.testing.allocator);
    defer params.deinit();
    const vector = [_]f32{ 0, 2 };
    try params.put("v", .{ .vector_val = &vector });
    var result = try db.queryWithParams("MATCH (n:A) SET n.embedding = $v RETURN id(n)", &params);
    defer result.deinit();
    try expectVector(db, null, @intCast(result.rows[0].values[0].int_val), &vector);
}

test "lvc: CREATE, MERGE and SET += write the vector from a property map" {
    const path = "/tmp/lvc_cypher_vector_maps.ltdb";
    cleanup(path);
    defer cleanup(path);

    var db = try openDb(path, true, true);
    defer db.close();

    var result = try db.query("CREATE (n:Doc {name: 'a', embedding: [1, 0]}) RETURN id(n)");
    const created: NodeId = @intCast(result.rows[0].values[0].int_val);
    result.deinit();
    try expectVector(db, null, created, &.{ 1, 0 });
    try std.testing.expect(try searchFinds(db, created));
    var name = (try db.getNodePropertyInTxn(null, created, "name")).?;
    name.deinit(db.allocator);
    try std.testing.expect((try db.getNodePropertyInTxn(null, created, "embedding")) == null);

    try run(db, "MATCH (n:Doc) SET n += {embedding: [0, 1], name: 'b'}");
    try expectVector(db, null, created, &.{ 0, 1 });
    try run(db, "MATCH (n:Doc) SET n += {embedding: null}");
    try std.testing.expect(!try hasVector(db, null, created));
    try std.testing.expect((try db.getNodePropertyInTxn(null, created, "embedding")) == null);

    // MERGE creates with the vector, then matches on it.
    try run(db, "MERGE (n:Item {sku: 1, embedding: [3, 4]})");
    try run(db, "MERGE (n:Item {sku: 1, embedding: [3, 4]})");
    try std.testing.expectEqual(@as(i64, 1), try countNodes(db, "MATCH (n:Item) RETURN count(n)"));
    // A different vector is a different node.
    try run(db, "MERGE (n:Item {sku: 1, embedding: [4, 3]})");
    try std.testing.expectEqual(@as(i64, 2), try countNodes(db, "MATCH (n:Item) RETURN count(n)"));

    // ON CREATE SET and ON MATCH SET write it too.
    result = try db.query("MERGE (n:Tag {name: 't'}) ON CREATE SET n.embedding = [1, 1] RETURN id(n)");
    const tag: NodeId = @intCast(result.rows[0].values[0].int_val);
    result.deinit();
    try expectVector(db, null, tag, &.{ 1, 1 });
    try run(db, "MERGE (n:Tag {name: 't'}) ON MATCH SET n.embedding = [2, 2]");
    try expectVector(db, null, tag, &.{ 2, 2 });
}

test "lvc: MERGE on a vector matches the stored, normalised copy" {
    const path = "/tmp/lvc_cypher_vector_merge_cosine.ltdb";
    cleanup(path);
    defer cleanup(path);

    var db = try Database.open(std.testing.allocator, path, .{ .create = true, .config = .{
        .enable_fts = false,
        .enable_vector = true,
        .vector_dimensions = dims,
    } });
    defer db.close();
    // Within one transaction the pending vector compares as written...
    var txn = try db.beginTransaction(.read_write);
    try runInTxn(db, &txn, "MERGE (:Item {embedding: [3, 4]})");
    try runInTxn(db, &txn, "MERGE (:Item {embedding: [3, 4]})");
    try db.commitTransaction(&txn);
    // ...and once committed, as the index stores it: (0.6, 0.8).
    try run(db, "MERGE (:Item {embedding: [3, 4]})");
    try std.testing.expectEqual(@as(i64, 1), try countNodes(db, "MATCH (n:Item) RETURN count(n)"));
}

test "lvc: a value that is not a vector fails the statement and writes nothing" {
    const path = "/tmp/lvc_cypher_vector_invalid.ltdb";
    cleanup(path);
    defer cleanup(path);

    var db = try openDb(path, true, true);
    defer db.close();
    const node = try addNodeWithVector(db, "A", &.{ 1, 0 });
    try run(db, "MATCH (n:A) SET n.name = 'kept'");

    const bad = [_][]const u8{
        "MATCH (n:A) SET n.embedding = [1, 0, 0]",
        "MATCH (n:A) SET n.embedding = []",
        "MATCH (n:A) SET n.embedding = [1, 'x']",
        "MATCH (n:A) SET n.embedding = 'text'",
        "MATCH (n:A) SET n.embedding = [1e300, 0]",
        "MATCH (n:A) SET n += {embedding: [1]}",
        "MATCH (n:A) SET n = {embedding: true}",
        "MATCH (n:A) SET n.embedding = $nan",
    };
    var params = std.StringHashMap(lattice.core.types.PropertyValue).init(std.testing.allocator);
    defer params.deinit();
    const nan_vector = [_]f32{ std.math.nan(f32), 0 };
    try params.put("nan", .{ .vector_val = &nan_vector });
    for (bad) |cypher| {
        if (db.queryWithParams(cypher, &params)) |result| {
            var r = result;
            r.deinit();
            std.debug.print("accepted: {s}\n", .{cypher});
            return error.TestUnexpectedResult;
        } else |_| {}
        try expectVector(db, null, node, &.{ 1, 0 });
        var name = (try db.getNodePropertyInTxn(null, node, "name")) orelse return error.PropertyMissing;
        name.deinit(db.allocator);
    }

    // A CREATE whose vector is refused creates no node.
    try std.testing.expect(std.meta.isError(db.query("CREATE (:B {embedding: [1, 2, 3]})")));
    try std.testing.expectEqual(@as(i64, 0), try countNodes(db, "MATCH (n:B) RETURN count(n)"));
}

test "lvc: without a vector index SET n.embedding is an ordinary property" {
    const path = "/tmp/lvc_cypher_vector_set_noindex.ltdb";
    cleanup(path);
    defer cleanup(path);

    var db = try openDb(path, true, false);
    defer db.close();
    try run(db, "CREATE (:A)");
    try run(db, "MATCH (n:A) SET n.embedding = [1, 2, 3]");
    try run(db, "MATCH (n:A) SET n += {embedding: 'text'}");
    var result = try db.query("MATCH (n:A) RETURN n.embedding");
    defer result.deinit();
    try std.testing.expectEqualStrings("text", result.rows[0].values[0].string_val);
}
