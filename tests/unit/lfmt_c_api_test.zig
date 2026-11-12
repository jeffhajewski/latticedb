//! The C API for v5 open options, detach delete and the error codes that
//! came with them.

const std = @import("std");
const lattice = @import("lattice");

const c_api = lattice.c_api;
const lattice_database = c_api.lattice_database;
const lattice_txn = c_api.lattice_txn;
const lattice_error = c_api.lattice_error;
const lattice_node_id = c_api.lattice_node_id;
const lattice_edge_id = c_api.lattice_edge_id;
const Options = c_api.lattice_open_options_v5;

fn cleanup(comptime path: []const u8) void {
    @import("compat").fs.cwd().deleteFile(path) catch {};
    @import("compat").fs.cwd().deleteFile(path ++ "-wal") catch {};
}

test "lfmt c_api: v5 open options keep their ABI layout" {
    const word = @sizeOf(usize);
    try std.testing.expectEqual(@as(usize, word + 22), @offsetOf(Options, "vector_m"));
    try std.testing.expectEqual(@as(usize, word + 21), @offsetOf(Options, "vector_metric"));
    try std.testing.expectEqual(@as(usize, word + 24), @offsetOf(Options, "vector_ef_construction"));
    try std.testing.expectEqual(std.mem.alignForward(usize, word + 26, word), @sizeOf(Options));
}

test "lfmt c_api: a v5 open records the metric, and a contradicting open is refused" {
    const path = "/tmp/lfmt_capi_v5_metric.db";
    cleanup(path);
    defer cleanup(path);

    {
        var db: ?*lattice_database = null;
        const options = Options{
            .create = true,
            .enable_vector = true,
            .vector_dimensions = 2,
            .vector_metric = @intFromEnum(c_api.lattice_vector_metric.euclidean),
            .vector_m = 8,
            .vector_ef_construction = 50,
            .sync_mode = @intFromEnum(c_api.lattice_sync_mode.normal),
            .full_fsync = @intFromEnum(c_api.lattice_full_fsync.off),
        };
        try std.testing.expectEqual(lattice_error.ok, c_api.lattice_open_v5(path, &options, &db));
        defer _ = c_api.lattice_close(db);

        var txn: ?*lattice_txn = null;
        try std.testing.expectEqual(lattice_error.ok, c_api.lattice_begin(db, .read_write, &txn));
        var a: lattice_node_id = 0;
        var b: lattice_node_id = 0;
        try std.testing.expectEqual(lattice_error.ok, c_api.lattice_node_create(txn, "V", &a));
        try std.testing.expectEqual(lattice_error.ok, c_api.lattice_node_create(txn, "V", &b));
        const va = [_]f32{ 0, 0 };
        const vb = [_]f32{ 3, 4 };
        try std.testing.expectEqual(lattice_error.ok, c_api.lattice_node_set_vector(txn, a, "embedding", &va, 2));
        try std.testing.expectEqual(lattice_error.ok, c_api.lattice_node_set_vector(txn, b, "embedding", &vb, 2));
        try std.testing.expectEqual(lattice_error.ok, c_api.lattice_commit(txn));
    }

    // Reopened with v5 defaults, vectors off: the index and its metric are the file's.
    {
        var db: ?*lattice_database = null;
        const options = Options{};
        try std.testing.expectEqual(lattice_error.ok, c_api.lattice_open_v5(path, &options, &db));
        defer _ = c_api.lattice_close(db);
        const q = [_]f32{ 0, 0 };
        var results: ?*c_api.lattice_vector_result = null;
        try std.testing.expectEqual(lattice_error.ok, c_api.lattice_vector_search(db, &q, 2, 2, 0, &results));
        defer c_api.lattice_vector_result_free(results);
        try std.testing.expectEqual(@as(u32, 2), c_api.lattice_vector_result_count(results));
        var id: lattice_node_id = 0;
        var distance: f32 = 0;
        try std.testing.expectEqual(lattice_error.ok, c_api.lattice_vector_result_get(results, 1, &id, &distance));
        // L2 from the origin to (3, 4).
        try std.testing.expectApproxEqAbs(@as(f32, 5), distance, 1e-4);
    }

    var db: ?*lattice_database = null;
    const cosine = Options{ .vector_metric = @intFromEnum(c_api.lattice_vector_metric.cosine) };
    try std.testing.expectEqual(lattice_error.err_config_mismatch, c_api.lattice_open_v5(path, &cosine, &db));
    try std.testing.expect(db == null);
    const dims = Options{ .enable_vector = true, .vector_dimensions = 3 };
    try std.testing.expectEqual(lattice_error.err_config_mismatch, c_api.lattice_open_v5(path, &dims, &db));
    // The older option structs reach the same check.
    const v4 = c_api.lattice_open_options_v4{ .enable_vector = true, .vector_dimensions = 3 };
    try std.testing.expectEqual(lattice_error.err_config_mismatch, c_api.lattice_open_v4(path, &v4, &db));

    const bad_metric = Options{ .vector_metric = 9 };
    try std.testing.expectEqual(lattice_error.err_invalid_arg, c_api.lattice_open_v5(path, &bad_metric, &db));
    const bad_sync = Options{ .sync_mode = 3 };
    try std.testing.expectEqual(lattice_error.err_invalid_arg, c_api.lattice_open_v5(path, &bad_sync, &db));
    const bad_m = Options{ .vector_m = 64 };
    try std.testing.expectEqual(lattice_error.err_invalid_arg, c_api.lattice_open_v5(path, &bad_m, &db));
    try std.testing.expect(std.mem.indexOf(u8, std.mem.span(c_api.lattice_error_message(.err_config_mismatch)), "vector") != null);
}

test "lfmt c_api: node_delete refuses a node with edges, node_detach_delete removes them" {
    const path = "/tmp/lfmt_capi_detach.db";
    cleanup(path);
    defer cleanup(path);

    var db: ?*lattice_database = null;
    const options = Options{ .create = true };
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_open_v5(path, &options, &db));
    defer _ = c_api.lattice_close(db);

    var txn: ?*lattice_txn = null;
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_begin(db, .read_write, &txn));
    var a: lattice_node_id = 0;
    var b: lattice_node_id = 0;
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_node_create(txn, "N", &a));
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_node_create(txn, "N", &b));
    var edge_id: lattice_edge_id = 0;
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_edge_create(txn, a, b, "R", &edge_id));

    try std.testing.expectEqual(lattice_error.err_has_relationships, c_api.lattice_node_delete(txn, b));
    var exists = false;
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_node_exists(txn, b, &exists));
    try std.testing.expect(exists);

    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_node_detach_delete(txn, a));
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_node_delete(txn, b));
    // A node that is not there: nothing to do, as before.
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_node_delete(txn, 123_456));
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_node_detach_delete(txn, 123_456));
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_commit(txn));
}

test "lfmt c_api: edge_create reports a missing endpoint as not found" {
    const path = "/tmp/lfmt_capi_edge_endpoint.db";
    cleanup(path);
    defer cleanup(path);

    var db: ?*lattice_database = null;
    const options = Options{ .create = true };
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_open_v5(path, &options, &db));
    defer _ = c_api.lattice_close(db);

    var txn: ?*lattice_txn = null;
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_begin(db, .read_write, &txn));
    var a: lattice_node_id = 0;
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_node_create(txn, "N", &a));
    var edge_id: lattice_edge_id = 0;
    try std.testing.expectEqual(lattice_error.err_not_found, c_api.lattice_edge_create(txn, a, 999, "R", &edge_id));
    try std.testing.expectEqual(lattice_error.err_not_found, c_api.lattice_edge_create(txn, 999, a, "R", &edge_id));
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_commit(txn));
}

test "lfmt c_api: a file that needs a newer writer opens read-only, and a writable open is refused" {
    const path = "/tmp/lfmt_capi_min_writer.db";
    cleanup(path);
    defer cleanup(path);
    {
        var db: ?*lattice_database = null;
        const options = Options{ .create = true };
        try std.testing.expectEqual(lattice_error.ok, c_api.lattice_open_v5(path, &options, &db));
        _ = c_api.lattice_close(db);
    }
    {
        const FileHeader = lattice.storage.page.FileHeader;
        const file = try @import("compat").fs.cwd().openFile(path, .{ .mode = .read_write });
        defer file.close();
        var buf: [4096]u8 = undefined;
        _ = try file.preadAll(&buf, 0);
        const at = @offsetOf(FileHeader, "min_writer_version");
        std.mem.writeInt(u16, buf[at..][0..2], lattice.core.types.FORMAT_VERSION + 1, .little);
        try file.pwriteAll(&buf, 0);
    }

    var db: ?*lattice_database = null;
    const writable = Options{};
    try std.testing.expectEqual(lattice_error.err_needs_newer_writer, c_api.lattice_open_v5(path, &writable, &db));
    try std.testing.expect(db == null);
    const read_only = Options{ .read_only = true };
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_open_v5(path, &read_only, &db));
    _ = c_api.lattice_close(db);
}
