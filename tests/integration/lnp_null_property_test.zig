//! A null in a Cypher property map means "no property", as `SET n.x = null`
//! always has.
//!
//! Up to 0.18.0 every write path but `SET n.x = null` stored the null as a
//! property: `SET n += {x: null}` left `x: null` on the node instead of
//! removing `x`, and `SET n = {...}`, `CREATE`, `ON CREATE SET` and
//! `ON MATCH SET` did the same on nodes and relationships, so
//! `properties(n)`, the changefeed and a dump all showed a null property that
//! Cypher says cannot exist. A relationship MERGE with a null pattern property
//! matched the null it had stored before, where a node MERGE refuses one.

const std = @import("std");
const lattice = @import("lattice");

const Database = lattice.storage.database.Database;
const NodeId = lattice.core.types.NodeId;
const EdgeId = lattice.core.types.EdgeId;
const PropertyValue = lattice.core.types.PropertyValue;
const QueryFailureStage = lattice.storage.database.QueryFailureStage;
const Params = std.StringHashMap(PropertyValue);

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
        .vector_dimensions = 2,
        .vector_metric = .euclidean,
    } });
}

fn run(db: *Database, cypher: []const u8) !void {
    var result = try db.query(cypher);
    result.deinit();
}

fn runWith(db: *Database, cypher: []const u8, params: *const Params) !void {
    var result = try db.queryWithParams(cypher, params);
    result.deinit();
}

/// The one integer a query returns.
fn queryInt(db: *Database, cypher: []const u8) !i64 {
    var result = try db.query(cypher);
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), result.rowCount());
    return result.rows[0].values[0].int_val;
}

fn nodeNamed(db: *Database, name: []const u8) !NodeId {
    var buf: [128]u8 = undefined;
    const cypher = try std.fmt.bufPrint(&buf, "MATCH (n {{name: '{s}'}}) RETURN id(n)", .{name});
    return @intCast(try queryInt(db, cypher));
}

fn edgeNamed(db: *Database, name: []const u8) !EdgeId {
    var buf: [128]u8 = undefined;
    const cypher = try std.fmt.bufPrint(&buf, "MATCH ()-[r {{name: '{s}'}}]->() RETURN id(r)", .{name});
    return @intCast(try queryInt(db, cypher));
}

/// The stored property keys, sorted and space-separated, with "=null" after
/// any whose stored value is null.
fn describe(props: []const Database.PropertyEntry) ![]u8 {
    const a = std.testing.allocator;
    const keys = try a.alloc([]const u8, props.len);
    defer a.free(keys);
    for (props, keys) |prop, *key| key.* = prop.key;
    std.mem.sort([]const u8, keys, {}, struct {
        fn lessThan(_: void, l: []const u8, r: []const u8) bool {
            return std.mem.lessThan(u8, l, r);
        }
    }.lessThan);
    var out: std.ArrayListUnmanaged(u8) = .empty;
    errdefer out.deinit(a);
    for (keys, 0..) |key, i| {
        if (i > 0) try out.append(a, ' ');
        try out.appendSlice(a, key);
        for (props) |prop| {
            if (std.mem.eql(u8, prop.key, key) and prop.value == .null_val) try out.appendSlice(a, "=null");
        }
    }
    return out.toOwnedSlice(a);
}

fn expectNodeKeys(db: *Database, node: NodeId, want: []const u8) !void {
    const props = try db.getNodePropertiesInTxn(null, node);
    defer db.freePropertyEntries(props);
    const got = try describe(props);
    defer std.testing.allocator.free(got);
    try std.testing.expectEqualStrings(want, got);
}

fn expectEdgeKeys(db: *Database, edge: EdgeId, want: []const u8) !void {
    const props = try db.getEdgePropertiesInTxn(null, edge);
    defer db.freePropertyEntries(props);
    const got = try describe(props);
    defer std.testing.allocator.free(got);
    try std.testing.expectEqualStrings(want, got);
}

fn lastSequence(db: *Database) !u64 {
    var batch = try db.readChanges(0, 100_000, 0);
    defer batch.deinit();
    if (batch.records.len == 0) return 0;
    return batch.records[batch.records.len - 1].sequence;
}

fn payloadField(payload: PropertyValue, key: []const u8) ?PropertyValue {
    for (payload.map_val) |entry| {
        if (std.mem.eql(u8, entry.key, key)) return entry.value;
    }
    return null;
}

/// The property events after `after`, one per line as "kind key".
fn propertyEvents(db: *Database, after: u64) ![]u8 {
    const a = std.testing.allocator;
    var out: std.ArrayListUnmanaged(u8) = .empty;
    errdefer out.deinit(a);
    var batch = try db.readChanges(after, 100_000, 0);
    defer batch.deinit();
    for (batch.records) |record| {
        const key = payloadField(record.payload, "key") orelse continue;
        try out.print(a, "{s} {s}\n", .{ record.kind, key.string_val });
    }
    return out.toOwnedSlice(a);
}

fn expectPropertyEvents(db: *Database, after: u64, want: []const u8) !void {
    const got = try propertyEvents(db, after);
    defer std.testing.allocator.free(got);
    try std.testing.expectEqualStrings(want, got);
}

fn expectFailure(db: *Database, cypher: []const u8, needle: []const u8) !void {
    var detailed = try db.queryDetailed(cypher);
    defer detailed.deinit();
    switch (detailed) {
        .success => {
            std.debug.print("\nquery should have failed: {s}\n", .{cypher});
            return error.TestUnexpectedResult;
        },
        .failure => |f| {
            if (f.stage != QueryFailureStage.execution or std.mem.indexOf(u8, f.message, needle) == null) {
                std.debug.print("\nquery: {s}\nexpected failure containing \"{s}\"\nactual {s}: {s}\n", .{ cypher, needle, @tagName(f.stage), f.message });
                return error.TestUnexpectedResult;
            }
        },
    }
}

test "lnp: SET n += {x: null} removes x, logs a removal and stays removed after reopening" {
    const path = "/tmp/lnp_set_plus_null.ltdb";
    cleanup(path);
    defer cleanup(path);

    var node: NodeId = undefined;
    {
        var db = try openDb(path, true, false);
        defer db.close();
        try run(db, "CREATE (:P {name: 'a', x: 1, y: 2})");
        node = try nodeNamed(db, "a");

        const before = try lastSequence(db);
        // z was never there: nothing to remove, and nothing to log.
        try run(db, "MATCH (n:P {name: 'a'}) SET n += {x: null, z: null, y: 3}");
        try expectNodeKeys(db, node, "name y");
        try expectPropertyEvents(db, before, "node.property_remove x\nnode.property_set y\n");

        try std.testing.expectEqual(@as(i64, 1), try queryInt(db, "MATCH (n:P) WHERE n.x IS NULL RETURN count(n)"));
        try std.testing.expectEqual(@as(i64, 0), try queryInt(db, "MATCH (n:P) WHERE exists(n.x) RETURN count(n)"));
    }

    var db = try openDb(path, false, false);
    defer db.close();
    try expectNodeKeys(db, node, "name y");
}

test "lnp: the other node write paths store no null either" {
    const path = "/tmp/lnp_node_paths.ltdb";
    cleanup(path);
    defer cleanup(path);

    var db = try openDb(path, true, false);
    defer db.close();

    try run(db, "CREATE (:P {name: 'replace', x: 1, y: 2}), (:P {name: 'matched', x: 1, y: 2})");
    const before = try lastSequence(db);

    try run(db, "MATCH (n:P {name: 'replace'}) SET n = {name: 'replace', x: null}");
    try expectNodeKeys(db, try nodeNamed(db, "replace"), "name");

    try run(db, "CREATE (:P {name: 'created', x: null})");
    try expectNodeKeys(db, try nodeNamed(db, "created"), "name");

    try run(db, "MERGE (n:P {name: 'on create'}) ON CREATE SET n.x = null, n.y = 1");
    try expectNodeKeys(db, try nodeNamed(db, "on create"), "name y");

    // ON CREATE SET can take back a pattern property, as SET can.
    try run(db, "MERGE (n:P {name: 'taken back', x: 1}) ON CREATE SET n.x = null");
    try expectNodeKeys(db, try nodeNamed(db, "taken back"), "name");

    try run(db, "MERGE (n:P {name: 'matched'}) ON MATCH SET n.x = null");
    try expectNodeKeys(db, try nodeNamed(db, "matched"), "name y");

    {
        var params = Params.init(std.testing.allocator);
        defer params.deinit();
        try params.put("v", .{ .null_val = {} });
        const entries = [_]PropertyValue.MapEntry{
            .{ .key = "y", .value = .{ .null_val = {} } },
            .{ .key = "z", .value = .{ .int_val = 7 } },
        };
        try params.put("m", .{ .map_val = &entries });
        try runWith(db, "CREATE (:P {name: 'param', x: $v})", &params);
        try expectNodeKeys(db, try nodeNamed(db, "param"), "name");
        try runWith(db, "MATCH (n:P {name: 'matched'}) SET n += $m", &params);
        try expectNodeKeys(db, try nodeNamed(db, "matched"), "name z");
        try runWith(db, "UNWIND [$v] AS v CREATE (:P {name: 'unwound', x: v})", &params);
        try expectNodeKeys(db, try nodeNamed(db, "unwound"), "name");
    }

    // No event anywhere sets a property to null.
    var batch = try db.readChanges(before, 100_000, 0);
    defer batch.deinit();
    for (batch.records) |record| {
        if (!std.mem.endsWith(u8, record.kind, "property_set")) continue;
        try std.testing.expect(payloadField(record.payload, "new_value").? != .null_val);
    }

    // A null in a MERGE pattern still fails, as documented.
    try expectFailure(db, "MERGE (n:P {name: 'merge', x: null})", "MERGE cannot match a null property value: 'x' is null");
}

test "lnp: relationships store no null, and a null in a relationship MERGE pattern fails" {
    const path = "/tmp/lnp_edge_paths.ltdb";
    cleanup(path);
    defer cleanup(path);

    {
        var db = try openDb(path, true, false);
        defer db.close();

        try run(db, "CREATE (a:Q {name: 'a'}), (b:Q {name: 'b'})");
        try run(db,
            \\MATCH (a:Q {name: 'a'}), (b:Q {name: 'b'})
            \\CREATE (a)-[:R {name: 'plus', x: 1, y: 2}]->(b), (a)-[:R {name: 'replace', x: 1}]->(b),
            \\       (a)-[:R {name: 'created', x: null}]->(b)
        );
        try expectEdgeKeys(db, try edgeNamed(db, "created"), "name");

        const before = try lastSequence(db);
        try run(db, "MATCH ()-[r:R {name: 'plus'}]->() SET r += {x: null}");
        try expectEdgeKeys(db, try edgeNamed(db, "plus"), "name y");
        try expectPropertyEvents(db, before, "edge.property_remove x\n");

        try run(db, "MATCH ()-[r:R {name: 'replace'}]->() SET r = {name: 'replace', x: null}");
        try expectEdgeKeys(db, try edgeNamed(db, "replace"), "name");

        try run(db, "MATCH (a:Q {name: 'a'}), (b:Q {name: 'b'}) MERGE (a)-[r:M {name: 'merged'}]->(b) ON CREATE SET r.x = null, r.y = 1");
        try expectEdgeKeys(db, try edgeNamed(db, "merged"), "name y");
        try run(db, "MATCH (a:Q {name: 'a'}), (b:Q {name: 'b'}) MERGE (a)-[r:M {name: 'merged'}]->(b) ON MATCH SET r.y = null");
        try expectEdgeKeys(db, try edgeNamed(db, "merged"), "name");

        // Once nulls are not stored, a null pattern property could never
        // match, so the MERGE would make a new relationship every run.
        const edges_before = try queryInt(db, "MATCH ()-[r]->() RETURN count(r)");
        try expectFailure(db, "MATCH (a:Q {name: 'a'}), (b:Q {name: 'b'}) MERGE (a)-[:N {w: null}]->(b)", "MERGE cannot match a null property value: 'w' is null");
        try expectFailure(db, "MATCH (a:Q {name: 'a'}), (b:Q {name: 'b'}) MERGE (a)-[:R {name: 'plus', w: null}]->(b)", "'w' is null");
        try std.testing.expectEqual(edges_before, try queryInt(db, "MATCH ()-[r]->() RETURN count(r)"));
    }

    var db = try openDb(path, false, false);
    defer db.close();
    try expectEdgeKeys(db, try edgeNamed(db, "plus"), "name y");
    try expectEdgeKeys(db, try edgeNamed(db, "created"), "name");
}

test "lnp: a null embedding removes the vector on a database with a vector index, and the property without one" {
    const vector_path = "/tmp/lnp_embedding_vector.ltdb";
    cleanup(vector_path);
    defer cleanup(vector_path);
    {
        var db = try openDb(vector_path, true, true);
        defer db.close();
        try run(db, "CREATE (:D {name: 'a', embedding: [1.0, 0.0]}), (:D {name: 'b', embedding: [0.0, 1.0]})");
        const a = try nodeNamed(db, "a");
        const b = try nodeNamed(db, "b");

        try run(db, "MATCH (n:D {name: 'a'}) SET n += {embedding: null, x: null}");
        try std.testing.expect((try db.getNodeVectorInTxn(null, a)) == null);
        try expectNodeKeys(db, a, "name");

        try run(db, "MATCH (n:D {name: 'b'}) SET n = {name: 'b', embedding: null}");
        try std.testing.expect((try db.getNodeVectorInTxn(null, b)) == null);
        try expectNodeKeys(db, b, "name");

        try run(db, "CREATE (:D {name: 'c', embedding: null})");
        const c = try nodeNamed(db, "c");
        try std.testing.expect((try db.getNodeVectorInTxn(null, c)) == null);
        try expectNodeKeys(db, c, "name");
    }

    const plain_path = "/tmp/lnp_embedding_plain.ltdb";
    cleanup(plain_path);
    defer cleanup(plain_path);
    var db = try openDb(plain_path, true, false);
    defer db.close();
    try run(db, "CREATE (:D {name: 'a', embedding: [1.0, 0.0]})");
    const a = try nodeNamed(db, "a");
    try expectNodeKeys(db, a, "embedding name");
    try run(db, "MATCH (n:D {name: 'a'}) SET n += {embedding: null}");
    try expectNodeKeys(db, a, "name");
}

/// Store `key: null` on a node the way 0.18.0 did, below every write path
/// that now refuses to: it stands in for a file written by 0.18.0.
fn storeLegacyNull(db: *Database, node_id: NodeId, key: []const u8) !void {
    const a = std.testing.allocator;
    var node = try db.node_store.get(node_id);
    defer node.deinit(a);
    var props: std.ArrayListUnmanaged(lattice.graph.node.Property) = .empty;
    defer props.deinit(a);
    try props.appendSlice(a, node.properties);
    try props.append(a, .{ .key_id = try db.symbol_table.intern(key), .value = .{ .null_val = {} } });
    try db.node_store.update(node_id, node.labels, props.items);
}

test "lnp: a null property already stored reads back as before, and any removal takes it away" {
    const path = "/tmp/lnp_stored_null.ltdb";
    cleanup(path);
    defer cleanup(path);

    var db = try openDb(path, true, false);
    defer db.close();
    try run(db, "CREATE (:P {name: 'plus'}), (:P {name: 'set'}), (:P {name: 'removed'}), (:P {name: 'api'}), (:P {name: 'kept'})");
    for ([_][]const u8{ "plus", "set", "removed", "api", "kept" }) |name| {
        try storeLegacyNull(db, try nodeNamed(db, name), "x");
    }
    try expectNodeKeys(db, try nodeNamed(db, "kept"), "name x=null");
    try std.testing.expectEqual(@as(i64, 5), try queryInt(db, "MATCH (n:P) WHERE n.x IS NULL RETURN count(n)"));

    try run(db, "MATCH (n:P {name: 'plus'}) SET n += {x: null}");
    try run(db, "MATCH (n:P {name: 'set'}) SET n.x = null");
    try run(db, "MATCH (n:P {name: 'removed'}) REMOVE n.x");
    try db.setNodeProperty(null, try nodeNamed(db, "api"), "x", .{ .null_val = {} });
    try expectNodeKeys(db, try nodeNamed(db, "plus"), "name");
    try expectNodeKeys(db, try nodeNamed(db, "api"), "name");
    try expectNodeKeys(db, try nodeNamed(db, "set"), "name");
    try expectNodeKeys(db, try nodeNamed(db, "removed"), "name");
    try expectNodeKeys(db, try nodeNamed(db, "kept"), "name x=null");
}

test "lnp: the property setters remove a property given null, and log what Cypher logs" {
    // The C API, every binding and the JSON and CSV importers write through
    // these two calls. Up to 0.18.0 they stored the null.
    const path = "/tmp/lnp_api_null.ltdb";
    cleanup(path);
    defer cleanup(path);

    var db = try openDb(path, true, false);
    defer db.close();
    try run(db, "CREATE (:A {name: 'a', y: 1})-[:R {name: 'r', v: 1}]->(:A {name: 'b', y: 1})");
    const a = try nodeNamed(db, "a");
    const b = try nodeNamed(db, "b");
    const r = try edgeNamed(db, "r");
    const null_val: PropertyValue = .{ .null_val = {} };

    // In a transaction: a present key is removed, an absent one is left alone.
    var seq = try lastSequence(db);
    {
        var txn = try db.beginTransaction(.read_write);
        errdefer db.abortTransaction(&txn) catch {};
        try db.setNodeProperty(&txn, a, "y", null_val);
        try db.setNodeProperty(&txn, a, "z", null_val);
        try db.setEdgePropertyById(&txn, r, "v", null_val);
        try db.setEdgePropertyById(&txn, r, "u", null_val);
        try db.commitTransaction(&txn);
    }
    try expectNodeKeys(db, a, "name");
    try expectEdgeKeys(db, r, "name");
    try expectPropertyEvents(db, seq, "node.property_remove y\nedge.property_remove v\n");

    // Cypher logs the same for the same change.
    seq = try lastSequence(db);
    try run(db, "MATCH (n:A {name: 'b'}) SET n.y = null, n.z = null");
    try expectPropertyEvents(db, seq, "node.property_remove y\n");

    // Without a transaction too.
    try db.setNodeProperty(null, b, "name", null_val);
    try db.setNodeProperty(null, b, "never", null_val);
    try expectNodeKeys(db, b, "");

    // A node or edge that does not exist is still an error.
    try std.testing.expectError(error.NotFound, db.setNodeProperty(null, 999_999, "x", null_val));
    try std.testing.expectError(error.NotFound, db.setEdgePropertyById(null, 999_999, "x", null_val));
}
