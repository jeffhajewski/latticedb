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
