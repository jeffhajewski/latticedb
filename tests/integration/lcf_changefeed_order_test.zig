//! The order of a transaction's changefeed events. A consumer that applies
//! events in sequence order can rely on:
//!   1. the deletion of every edge the transaction deletes,
//!   2. then every node that exists after the transaction: a new one's
//!      insert, labels and properties; an existing one's label removals,
//!      label additions, property removals and property sets,
//!   3. then every edge that exists after it, in the same way,
//!   4. then the deletion of every node the transaction deletes,
//! with nodes and edges in ascending id order within each group. So an
//! edge is created after both its nodes, a property or label change comes
//! after its entity's creation, an edge is deleted before its nodes and a
//! node's deletion is the last event that names it. Events describe the
//! transaction's net effect: a property set several times appears once,
//! from its value before the transaction to its value after.

const std = @import("std");
const lattice = @import("lattice");
const compat = @import("compat");

const Database = lattice.storage.database.Database;
const PropertyValue = lattice.core.types.PropertyValue;

fn cleanup(path: []const u8) void {
    compat.fs.cwd().deleteFile(path) catch {};
    var buf: [256]u8 = undefined;
    for ([_][]const u8{ "-wal", "-journal" }) |suffix| {
        const p = std.fmt.bufPrint(&buf, "{s}{s}", .{ path, suffix }) catch return;
        compat.fs.cwd().deleteFile(p) catch {};
    }
}

fn open(allocator: std.mem.Allocator, path: []const u8) !*Database {
    return Database.open(allocator, path, .{
        .create = true,
        .config = .{ .enable_fts = false },
    });
}

fn field(payload: PropertyValue, key: []const u8) ?PropertyValue {
    for (payload.map_val) |entry| {
        if (std.mem.eql(u8, entry.key, key)) return entry.value;
    }
    return null;
}

fn int(payload: PropertyValue, key: []const u8) ?u64 {
    const v = field(payload, key) orelse return null;
    return @intCast(v.int_val);
}

/// The events after `after`, one line each: "kind id", then "src>dst" for
/// edge inserts and deletes, the label for label events, and "key=old>new"
/// (ints, null as "-") for property events.
fn render(allocator: std.mem.Allocator, db: *Database, after: u64) ![]u8 {
    var out: std.ArrayListUnmanaged(u8) = .empty;
    errdefer out.deinit(allocator);
    var batch = try db.readChanges(after, 10_000, 0);
    defer batch.deinit();
    for (batch.records) |record| {
        const p = record.payload;
        const id = int(p, "node_id") orelse int(p, "edge_id").?;
        var line: std.ArrayListUnmanaged(u8) = .empty;
        defer line.deinit(allocator);
        try line.print(allocator, "{s} {d}", .{ record.kind, id });
        if (int(p, "source_id")) |s| try line.print(allocator, " {d}>{d}", .{ s, int(p, "target_id").? });
        if (field(p, "label")) |l| try line.print(allocator, " {s}", .{l.string_val});
        if (field(p, "key")) |k| {
            try line.print(allocator, " {s}=", .{k.string_val});
            for ([_][]const u8{ "old_value", "new_value" }, 0..) |name, i| {
                if (i == 1) try line.appendSlice(allocator, ">");
                const v = field(p, name).?;
                switch (v) {
                    .int_val => |x| try line.print(allocator, "{d}", .{x}),
                    else => try line.appendSlice(allocator, "-"),
                }
            }
        }
        try out.appendSlice(allocator, line.items);
        try out.append(allocator, '\n');
    }
    return out.toOwnedSlice(allocator);
}

fn expectEvents(allocator: std.mem.Allocator, db: *Database, after: u64, comptime fmt: []const u8, args: anytype) !void {
    const actual = try render(allocator, db, after);
    defer allocator.free(actual);
    const want = try std.fmt.allocPrint(allocator, fmt, args);
    defer allocator.free(want);
    if (!std.mem.eql(u8, want, actual)) {
        std.debug.print("expected:\n{s}got:\n{s}", .{ want, actual });
        return error.WrongOrder;
    }
}

/// The four rules, checked over any feed a consumer could replay from an
/// empty graph: nothing names a node or edge before it is created or after
/// it is deleted, and no node is deleted while an edge still uses it.
fn expectReplayable(allocator: std.mem.Allocator, db: *Database) !void {
    var nodes: std.AutoHashMapUnmanaged(u64, void) = .empty;
    defer nodes.deinit(allocator);
    var edges: std.AutoHashMapUnmanaged(u64, [2]u64) = .empty;
    defer edges.deinit(allocator);
    var batch = try db.readChanges(0, 100_000, 0);
    defer batch.deinit();
    for (batch.records) |record| {
        const p = record.payload;
        const kind = record.kind;
        if (std.mem.startsWith(u8, kind, "node.")) {
            const id = int(p, "node_id").?;
            if (std.mem.eql(u8, kind, "node.insert")) {
                try std.testing.expect(!nodes.contains(id));
                try nodes.put(allocator, id, {});
            } else {
                if (!nodes.contains(id)) {
                    std.debug.print("{d}: {s} names node {d}, which does not exist\n", .{ record.sequence, kind, id });
                    return error.NotReplayable;
                }
                if (std.mem.eql(u8, kind, "node.delete")) {
                    var it = edges.valueIterator();
                    while (it.next()) |ends| {
                        if (ends[0] == id or ends[1] == id) {
                            std.debug.print("{d}: node {d} deleted while an edge still uses it\n", .{ record.sequence, id });
                            return error.NotReplayable;
                        }
                    }
                    _ = nodes.remove(id);
                }
            }
        } else {
            const id = int(p, "edge_id").?;
            if (std.mem.eql(u8, kind, "edge.insert")) {
                const s = int(p, "source_id").?;
                const t = int(p, "target_id").?;
                if (!nodes.contains(s) or !nodes.contains(t)) {
                    std.debug.print("{d}: edge {d} created before its nodes\n", .{ record.sequence, id });
                    return error.NotReplayable;
                }
                try edges.put(allocator, id, .{ s, t });
            } else {
                if (!edges.contains(id)) {
                    std.debug.print("{d}: {s} names edge {d}, which does not exist\n", .{ record.sequence, kind, id });
                    return error.NotReplayable;
                }
                if (std.mem.eql(u8, kind, "edge.delete")) _ = edges.remove(id);
            }
        }
    }
}

test "lcf changefeed order: edges go before their nodes, and ids ascend within each group (Zig API)" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lcfo_order_api.ltdb";
    cleanup(path);
    defer cleanup(path);
    var db = try open(allocator, path);
    defer db.close();

    var n: [3]u64 = undefined;
    var e: [2]u64 = undefined;
    {
        var txn = try db.beginTransaction(.read_write);
        for (&n) |*id| {
            id.* = try db.createNode(&txn, &[_][]const u8{"P"});
            try db.setNodeProperty(&txn, id.*, "v", .{ .int_val = 1 });
        }
        e[0] = try db.createEdgeAndGetId(&txn, n[0], n[1], "R");
        e[1] = try db.createEdgeAndGetId(&txn, n[1], n[2], "R");
        try db.commitTransaction(&txn);
    }
    var batch0 = try db.readChanges(0, 1000, 0);
    const before = batch0.records[batch0.records.len - 1].sequence;
    batch0.deinit();

    var txn = try db.beginTransaction(.read_write);
    const n4 = try db.createNode(&txn, &[_][]const u8{"P"});
    try db.setNodeProperty(&txn, n[0], "v", .{ .int_val = 2 });
    try db.detachDeleteNode(&txn, n[1]);
    try db.setNodeProperty(&txn, n[0], "v", .{ .int_val = 3 });
    const e3 = try db.createEdgeAndGetId(&txn, n[0], n4, "R");
    try db.setEdgePropertyById(&txn, e3, "w", .{ .int_val = 1 });
    const n5 = try db.createNode(&txn, &[_][]const u8{});
    const e4 = try db.createEdgeAndGetId(&txn, n5, n4, "R");
    try db.addNodeLabel(&txn, n[2], "Q");
    try db.removeNodeProperty(&txn, n[2], "v");
    // Created and deleted in the same transaction: no events.
    const ghost = try db.createNode(&txn, &[_][]const u8{"P"});
    try db.deleteNode(&txn, ghost);
    try db.commitTransaction(&txn);

    try expectEvents(allocator, db, before,
        \\edge.delete {d} {d}>{d}
        \\edge.delete {d} {d}>{d}
        \\node.property_set {d} v=1>3
        \\node.label_add {d} Q
        \\node.property_remove {d} v=1>-
        \\node.insert {d}
        \\node.label_add {d} P
        \\node.insert {d}
        \\edge.insert {d} {d}>{d}
        \\edge.property_set {d} w=->1
        \\edge.insert {d} {d}>{d}
        \\node.delete {d}
        \\
    , .{
        e[0], n[0], n[1], e[1], n[1], n[2],
        n[0], n[2], n[2], n4,   n4,   n5,
        e3,   n[0], n4,   e3,   e4,   n5,
        n4,   n[1],
    });
    try expectReplayable(allocator, db);
}

test "lcf changefeed order: many new nodes and edges come in id order" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lcfo_order_many.ltdb";
    cleanup(path);
    defer cleanup(path);
    var db = try open(allocator, path);
    defer db.close();

    var ids: [40]u64 = undefined;
    var txn = try db.beginTransaction(.read_write);
    for (&ids) |*id| id.* = try db.createNode(&txn, &[_][]const u8{});
    for (ids[0 .. ids.len - 1], ids[1..]) |a, b| _ = try db.createEdgeAndGetId(&txn, b, a, "PREV");
    try db.commitTransaction(&txn);

    var batch = try db.readChanges(0, 1000, 0);
    defer batch.deinit();
    try std.testing.expectEqual(@as(usize, 40 + 39), batch.records.len);
    var last_node: u64 = 0;
    var last_edge: u64 = 0;
    for (batch.records, 0..) |record, i| {
        if (i < 40) {
            try std.testing.expectEqualStrings("node.insert", record.kind);
            const id = int(record.payload, "node_id").?;
            try std.testing.expect(id > last_node);
            last_node = id;
        } else {
            try std.testing.expectEqualStrings("edge.insert", record.kind);
            const id = int(record.payload, "edge_id").?;
            try std.testing.expect(id > last_edge);
            last_edge = id;
        }
    }
    try expectReplayable(allocator, db);
}

test "lcf changefeed order: a Cypher statement's events follow the same rules" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lcfo_order_cypher.ltdb";
    cleanup(path);
    defer cleanup(path);
    var db = try open(allocator, path);
    defer db.close();

    var r0 = try db.query("CREATE (:X {k: 1}), (:X {k: 2})");
    r0.deinit();
    var r1 = try db.query("MATCH (x:X {k: 1}), (y:X {k: 2}) CREATE (y)-[:L]->(x)");
    r1.deinit();
    var batch0 = try db.readChanges(0, 1000, 0);
    const before = batch0.records[batch0.records.len - 1].sequence;
    batch0.deinit();

    var r2 = try db.query(
        "MATCH (x:X {k: 1}), (y:X {k: 2}) " ++
            "CREATE (x)-[:L]->(z:Z {k: 3}) " ++
            "SET x.k = 10, x.k = 11 " ++
            "DETACH DELETE y",
    );
    r2.deinit();
    // Nodes 1 (x) and 2 (y), edge 1 (y->x); new node 3 (z), edge 2 (x->z).
    try expectEvents(allocator, db, before,
        \\edge.delete 1 2>1
        \\node.property_set 1 k=1>11
        \\node.insert 3
        \\node.label_add 3 Z
        \\node.property_set 3 k=->3
        \\edge.insert 2 1>3
        \\node.delete 2
        \\
    , .{});
    try expectReplayable(allocator, db);
}
