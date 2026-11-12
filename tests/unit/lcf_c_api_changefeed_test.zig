//! The changefeed retention option of the v5 open, and the status and
//! trim functions.

const std = @import("std");
const lattice = @import("lattice");

const c_api = lattice.c_api;
const lattice_database = c_api.lattice_database;
const lattice_txn = c_api.lattice_txn;
const lattice_error = c_api.lattice_error;
const lattice_node_id = c_api.lattice_node_id;
const lattice_stream_batch = c_api.lattice_stream_batch;
const Options = c_api.lattice_open_options_v5;
const Retention = c_api.lattice_changefeed_retention;

fn cleanup(comptime path: []const u8) void {
    @import("compat").fs.cwd().deleteFile(path) catch {};
    @import("compat").fs.cwd().deleteFile(path ++ "-wal") catch {};
}

fn commitNode(db: ?*lattice_database) !void {
    var txn: ?*lattice_txn = null;
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_begin(db, .read_write, &txn));
    var id: lattice_node_id = 0;
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_node_create(txn, "N", &id));
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_commit(txn));
}

fn readChanges(db: ?*lattice_database, after: u64) lattice_error {
    var batch: ?*lattice_stream_batch = null;
    const rc = c_api.lattice_stream_read(db, "__lattice_changes", "__lattice_changes".len, after, 100, 0, &batch);
    c_api.lattice_stream_batch_free(batch);
    return rc;
}

test "lcf c_api: a v5 open sets the changefeed policy, and status and trim report it" {
    const path = "/tmp/lcf_capi_changefeed.db";
    cleanup(path);
    defer cleanup(path);

    var db: ?*lattice_database = null;
    const options = Options{
        .create = true,
        .changefeed_retention = @intFromEnum(Retention.last),
        .changefeed_retention_value = 4,
    };
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_open_v5(path, &options, &db));
    defer _ = c_api.lattice_close(db);

    for (0..5) |_| try commitNode(db); // events 1..10

    var removed: u64 = 0;
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_changefeed_trim(db, &removed));
    try std.testing.expectEqual(@as(u64, 6), removed);

    var mode: u8 = 0;
    var value: u64 = 0;
    var oldest: u64 = 0;
    var last: u64 = 0;
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_changefeed_status(db, &mode, &value, &oldest, &last));
    try std.testing.expectEqual(@as(u8, @intFromEnum(Retention.last)), mode);
    try std.testing.expectEqual(@as(u64, 4), value);
    try std.testing.expectEqual(@as(u64, 7), oldest);
    try std.testing.expectEqual(@as(u64, 10), last);
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_changefeed_status(db, null, null, null, null));

    // Reading on from a dropped event says so; from the start reads on.
    try std.testing.expectEqual(lattice_error.err_not_found, readChanges(db, 2));
    try std.testing.expectEqual(lattice_error.ok, readChanges(db, 0));
    try std.testing.expectEqual(lattice_error.ok, readChanges(db, 6));

    // Not while a write transaction is open.
    var txn: ?*lattice_txn = null;
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_begin(db, .read_write, &txn));
    try std.testing.expectEqual(lattice_error.err_lock_timeout, c_api.lattice_changefeed_trim(db, null));
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_rollback(txn));
}

test "lcf c_api: off makes reading the changefeed unsupported" {
    const path = "/tmp/lcf_capi_changefeed_off.db";
    cleanup(path);
    defer cleanup(path);

    var db: ?*lattice_database = null;
    const options = Options{ .create = true, .changefeed_retention = @intFromEnum(Retention.off) };
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_open_v5(path, &options, &db));
    defer _ = c_api.lattice_close(db);
    try commitNode(db);
    try std.testing.expectEqual(lattice_error.err_unsupported, readChanges(db, 0));
    var last: u64 = 1;
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_changefeed_status(db, null, null, null, &last));
    try std.testing.expectEqual(@as(u64, 0), last);
}

test "lcf c_api: an invalid changefeed policy is refused" {
    const path = "/tmp/lcf_capi_changefeed_bad.db";
    cleanup(path);
    defer cleanup(path);

    var db: ?*lattice_database = null;
    const zero = Options{ .create = true, .changefeed_retention = @intFromEnum(Retention.last) };
    try std.testing.expectEqual(lattice_error.err_invalid_arg, c_api.lattice_open_v5(path, &zero, &db));
    const unknown = Options{ .create = true, .changefeed_retention = 9, .changefeed_retention_value = 1 };
    try std.testing.expectEqual(lattice_error.err_invalid_arg, c_api.lattice_open_v5(path, &unknown, &db));
    try std.testing.expect(db == null);
}

/// "kind id" for each changefeed event after `after`, one per line.
fn renderChanges(buf: []u8, db: ?*lattice_database, after: u64) ![]const u8 {
    var batch: ?*lattice_stream_batch = null;
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_stream_read(db, "__lattice_changes", "__lattice_changes".len, after, 1000, 0, &batch));
    defer c_api.lattice_stream_batch_free(batch);
    var len: usize = 0;
    for (0..c_api.lattice_stream_batch_count(batch)) |i| {
        var sequence: u64 = 0;
        var kind: [*c]const u8 = null;
        var kind_len: usize = 0;
        var payload: ?*const c_api.lattice_value = null;
        try std.testing.expectEqual(lattice_error.ok, c_api.lattice_stream_batch_get(batch, i, &sequence, &kind, &kind_len, &payload));
        const map = payload.?.data.map_val.?;
        var id: i64 = -1;
        for (map.entries[0..map.len]) |entry| {
            const key = entry.key[0..entry.key_len];
            if (std.mem.eql(u8, key, "node_id") or std.mem.eql(u8, key, "edge_id")) id = entry.value.data.int_val;
        }
        const line = try std.fmt.bufPrint(buf[len..], "{s} {d}\n", .{ kind[0..kind_len], id });
        len += line.len;
    }
    return buf[0..len];
}

test "lcf c_api: a transaction's events come edges deleted, nodes, edges, nodes deleted, by id" {
    const path = "/tmp/lcfo_capi_order.db";
    cleanup(path);
    defer cleanup(path);

    var db: ?*lattice_database = null;
    const options = Options{ .create = true };
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_open_v5(path, &options, &db));
    defer _ = c_api.lattice_close(db);

    // Ten nodes in a ring, made in one transaction: nodes 1..10, then edges
    // 1..10, whatever order the transaction keeps them in.
    var txn: ?*lattice_txn = null;
    var ids: [10]lattice_node_id = undefined;
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_begin(db, .read_write, &txn));
    for (&ids) |*id| try std.testing.expectEqual(lattice_error.ok, c_api.lattice_node_create(txn, "N", id));
    for (ids, 0..) |id, i| {
        var edge: c_api.lattice_edge_id = 0;
        try std.testing.expectEqual(lattice_error.ok, c_api.lattice_edge_create(txn, id, ids[(i + 1) % ids.len], "NEXT", &edge));
    }
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_commit(txn));

    var buf: [4096]u8 = undefined;
    var want: std.ArrayListUnmanaged(u8) = .empty;
    defer want.deinit(std.testing.allocator);
    for (1..11) |id| try want.print(std.testing.allocator, "node.insert {d}\nnode.label_add {d}\n", .{ id, id });
    for (1..11) |id| try want.print(std.testing.allocator, "edge.insert {d}\n", .{id});
    try std.testing.expectEqualStrings(want.items, try renderChanges(&buf, db, 0));

    // Deleting node 3 with its two edges: the edges go first.
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_begin(db, .read_write, &txn));
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_node_detach_delete(txn, ids[2]));
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_commit(txn));
    try std.testing.expectEqualStrings("edge.delete 2\nedge.delete 3\nnode.delete 3\n", try renderChanges(&buf, db, 30));
}
