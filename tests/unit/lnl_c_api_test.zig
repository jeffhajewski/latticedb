//! Transactions through the C API on a database opened without a log.
//!
//! `lattice_begin` in read-write mode used to return
//! LATTICE_ERROR_UNSUPPORTED on such a database, so no binding could write
//! to one at all.

const std = @import("std");
const lattice = @import("lattice");

const c_api = lattice.c_api;
const lattice_database = c_api.lattice_database;
const lattice_txn = c_api.lattice_txn;
const lattice_query = c_api.lattice_query;
const lattice_result = c_api.lattice_result;
const lattice_value = c_api.lattice_value;
const lattice_error = c_api.lattice_error;
const lattice_open_options_v2 = c_api.lattice_open_options_v2;

fn removeFiles(comptime path: []const u8) void {
    const cwd = @import("compat").fs.cwd();
    cwd.deleteFile(path) catch {};
    cwd.deleteFile(path ++ "-wal") catch {};
    cwd.deleteFile(path ++ "-journal") catch {};
}

fn openNoLog(comptime path: [:0]const u8) !?*lattice_database {
    removeFiles(path);
    var db: ?*lattice_database = null;
    const options = lattice_open_options_v2{
        .struct_size = @sizeOf(lattice_open_options_v2),
        .create = true,
        .read_only = false,
        .cache_size_mb = 4,
        .page_size = 4096,
        .enable_vector = false,
        .vector_dimensions = 0,
        .enable_wal = false,
    };
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_open_v2(path, &options, &db));
    return db;
}

/// Run `cypher` in `txn` and return the error, freeing any result.
fn exec(db: ?*lattice_database, txn: ?*lattice_txn, cypher: [*:0]const u8) lattice_error {
    var query: ?*lattice_query = null;
    const prepared = c_api.lattice_query_prepare(db, cypher, &query);
    if (prepared != .ok) return prepared;
    defer c_api.lattice_query_free(query);
    var result: ?*lattice_result = null;
    const executed = c_api.lattice_query_execute(query, txn, &result);
    if (result != null) c_api.lattice_result_free(result);
    return executed;
}

/// The single integer a counting query returns, read in a transaction of
/// its own.
fn count(db: ?*lattice_database, cypher: [*:0]const u8) !i64 {
    var txn: ?*lattice_txn = null;
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_begin(db, .read_only, &txn));
    defer _ = c_api.lattice_rollback(txn);
    var query: ?*lattice_query = null;
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_query_prepare(db, cypher, &query));
    defer c_api.lattice_query_free(query);
    var result: ?*lattice_result = null;
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_query_execute(query, txn, &result));
    defer c_api.lattice_result_free(result);
    try std.testing.expect(c_api.lattice_result_next(result));
    var value: lattice_value = undefined;
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_result_get(result, 0, &value));
    return value.data.int_val;
}

fn write(db: ?*lattice_database, cypher: [*:0]const u8) !void {
    var txn: ?*lattice_txn = null;
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_begin(db, .read_write, &txn));
    try std.testing.expectEqual(lattice_error.ok, exec(db, txn, cypher));
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_commit(txn));
}

test "lnl c_api: a database without a log begins, commits and rolls back write transactions" {
    const path = "/tmp/lnl_capi_begin.db";
    const db = try openNoLog(path);
    defer {
        _ = c_api.lattice_close(db);
        removeFiles(path);
    }

    try write(db, "CREATE (:P {i: 1}), (:P {i: 2})");
    try std.testing.expectEqual(@as(i64, 2), try count(db, "MATCH (n:P) RETURN count(n)"));

    // Rolled back: nothing of it stays.
    var txn: ?*lattice_txn = null;
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_begin(db, .read_write, &txn));
    var node: c_api.lattice_node_id = 0;
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_node_create(txn, "P", &node));
    try std.testing.expectEqual(lattice_error.ok, exec(db, txn, "MATCH (n:P {i: 1}) DELETE n"));
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_rollback(txn));
    try std.testing.expectEqual(@as(i64, 2), try count(db, "MATCH (n:P) RETURN count(n)"));

    // One writer at a time, as with a log.
    var first: ?*lattice_txn = null;
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_begin(db, .read_write, &first));
    var second: ?*lattice_txn = null;
    try std.testing.expectEqual(lattice_error.err_lock_timeout, c_api.lattice_begin(db, .read_write, &second));
    try std.testing.expect(second == null);
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_rollback(first));

    // Nothing logged, so no log file.
    try std.testing.expectError(error.FileNotFound, @import("compat").fs.cwd().access(path ++ "-wal", .{}));
}

test "lnl c_api: a query that fails after its delete is rolled back without a log" {
    const path = "/tmp/lnl_capi_repro.db";
    const db = try openNoLog(path);
    defer {
        _ = c_api.lattice_close(db);
        removeFiles(path);
    }
    try write(db, "CREATE (:D {i: 1})-[:R]->(:D {i: 2}), (:D {i: 3})");

    // What every binding does with a writing query: a transaction of its
    // own, rolled back when the query fails.
    var txn: ?*lattice_txn = null;
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_begin(db, .read_write, &txn));
    try std.testing.expectEqual(
        lattice_error.err_not_found,
        exec(db, txn, "MATCH (a:D {i: 3}), (b:D {i: 2}) DELETE a CREATE (b)-[:R]->(a)"),
    );
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_rollback(txn));
    try std.testing.expectEqual(@as(i64, 3), try count(db, "MATCH (n:D) RETURN count(n)"));
}

test "lnl c_api: streams and the changefeed work without a log" {
    const path = "/tmp/lnl_capi_streams.db";
    const db = try openNoLog(path);
    defer {
        _ = c_api.lattice_close(db);
        removeFiles(path);
    }

    var payload = lattice_value{
        .value_type = .string,
        .data = .{ .string_val = .{ .ptr = "payload".ptr, .len = "payload".len } },
    };
    var txn: ?*lattice_txn = null;
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_begin(db, .read_write, &txn));
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_stream_publish(txn, "events".ptr, "events".len, null, 0, &payload));
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_commit(txn));

    var batch: ?*c_api.lattice_stream_batch = null;
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_stream_read(db, "events".ptr, "events".len, 0, 10, 0, &batch));
    try std.testing.expectEqual(@as(usize, 1), c_api.lattice_stream_batch_count(batch));
    c_api.lattice_stream_batch_free(batch);

    // A read transaction is a real one now, and refuses writes as such.
    var reader: ?*lattice_txn = null;
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_begin(db, .read_only, &reader));
    try std.testing.expectEqual(lattice_error.err_read_only, c_api.lattice_stream_publish(reader, "events".ptr, "events".len, null, 0, &payload));
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_rollback(reader));
}
