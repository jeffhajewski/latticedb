//! Behavioral tests for the C API.
//!
//! These tests verify the C API works correctly end-to-end, including:
//! - Database lifecycle (open/close)
//! - Transaction semantics
//! - Node/Edge CRUD operations
//! - Query execution
//! - Vector and FTS search
//! - Proper error handling
//! - Memory management

const std = @import("std");
const lattice = @import("lattice");

const c_api = lattice.c_api;

// Import C API types
const lattice_database = c_api.lattice_database;
const lattice_txn = c_api.lattice_txn;
const lattice_query = c_api.lattice_query;
const lattice_result = c_api.lattice_result;
const lattice_error = c_api.lattice_error;
const lattice_value = c_api.lattice_value;
const lattice_value_type = c_api.lattice_value_type;
const lattice_open_options = c_api.lattice_open_options;
const lattice_txn_mode = c_api.lattice_txn_mode;
const lattice_node_id = c_api.lattice_node_id;

// ============================================================================
// Database Lifecycle Tests
// ============================================================================

test "c_api: open and close database" {
    const path = "/tmp/lattice_capi_open_test.db";

    // Clean up from previous runs
    std.fs.cwd().deleteFile(path) catch {};

    var db: ?*lattice_database = null;
    const options = lattice_open_options{
        .create = true,
        .read_only = false,
        .cache_size_mb = 4,
        .page_size = 4096,
        .enable_vector = false,
        .vector_dimensions = 0,
    };

    // Open database
    const open_result = c_api.lattice_open(path, &options, &db);
    try std.testing.expectEqual(lattice_error.ok, open_result);
    try std.testing.expect(db != null);

    // Close database
    const close_result = c_api.lattice_close(db);
    try std.testing.expectEqual(lattice_error.ok, close_result);

    // Cleanup
    std.fs.cwd().deleteFile(path) catch {};
}

test "c_api: open nonexistent file without create fails" {
    var db: ?*lattice_database = null;
    const options = lattice_open_options{
        .create = false,
        .read_only = false,
        .cache_size_mb = 4,
        .page_size = 4096,
        .enable_vector = false,
        .vector_dimensions = 0,
    };

    const result = c_api.lattice_open("/nonexistent/path/db.ltdb", &options, &db);
    try std.testing.expectEqual(lattice_error.err_not_found, result);
    try std.testing.expect(db == null);
}

test "c_api: close null handle returns error" {
    const result = c_api.lattice_close(null);
    try std.testing.expectEqual(lattice_error.err_invalid_arg, result);
}

test "c_api: reopen existing database" {
    const path = "/tmp/lattice_capi_reopen_test.db";
    std.fs.cwd().deleteFile(path) catch {};

    // Create database
    {
        var db: ?*lattice_database = null;
        const options = lattice_open_options{
            .create = true,
            .read_only = false,
            .cache_size_mb = 4,
            .page_size = 4096,
            .enable_vector = false,
            .vector_dimensions = 0,
        };

        try std.testing.expectEqual(lattice_error.ok, c_api.lattice_open(path, &options, &db));
        try std.testing.expectEqual(lattice_error.ok, c_api.lattice_close(db));
    }

    // Reopen without create flag
    {
        var db: ?*lattice_database = null;
        const options = lattice_open_options{
            .create = false,
            .read_only = false,
            .cache_size_mb = 4,
            .page_size = 4096,
            .enable_vector = false,
            .vector_dimensions = 0,
        };

        try std.testing.expectEqual(lattice_error.ok, c_api.lattice_open(path, &options, &db));
        try std.testing.expectEqual(lattice_error.ok, c_api.lattice_close(db));
    }

    std.fs.cwd().deleteFile(path) catch {};
}

// ============================================================================
// Transaction Tests
// ============================================================================

test "c_api: begin and commit transaction" {
    const path = "/tmp/lattice_capi_txn_test.db";
    std.fs.cwd().deleteFile(path) catch {};

    var db: ?*lattice_database = null;
    const options = lattice_open_options{
        .create = true,
        .read_only = false,
        .cache_size_mb = 4,
        .page_size = 4096,
        .enable_vector = false,
        .vector_dimensions = 0,
    };

    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_open(path, &options, &db));
    defer {
        _ = c_api.lattice_close(db);
        std.fs.cwd().deleteFile(path) catch {};
    }

    // Begin transaction
    var txn: ?*lattice_txn = null;
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_begin(db, .read_write, &txn));
    try std.testing.expect(txn != null);

    // Commit transaction
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_commit(txn));
}

test "c_api: begin and rollback transaction" {
    const path = "/tmp/lattice_capi_rollback_test.db";
    std.fs.cwd().deleteFile(path) catch {};

    var db: ?*lattice_database = null;
    const options = lattice_open_options{
        .create = true,
        .read_only = false,
        .cache_size_mb = 4,
        .page_size = 4096,
        .enable_vector = false,
        .vector_dimensions = 0,
    };

    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_open(path, &options, &db));
    defer {
        _ = c_api.lattice_close(db);
        std.fs.cwd().deleteFile(path) catch {};
    }

    var txn: ?*lattice_txn = null;
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_begin(db, .read_write, &txn));

    // Rollback
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_rollback(txn));
}

test "c_api: read-only transaction prevents writes" {
    const path = "/tmp/lattice_capi_readonly_test.db";
    std.fs.cwd().deleteFile(path) catch {};

    var db: ?*lattice_database = null;
    const options = lattice_open_options{
        .create = true,
        .read_only = false,
        .cache_size_mb = 4,
        .page_size = 4096,
        .enable_vector = false,
        .vector_dimensions = 0,
    };

    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_open(path, &options, &db));
    defer {
        _ = c_api.lattice_close(db);
        std.fs.cwd().deleteFile(path) catch {};
    }

    // Begin read-only transaction
    var txn: ?*lattice_txn = null;
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_begin(db, .read_only, &txn));

    // Try to create node - should fail
    var node_id: lattice_node_id = 0;
    const result = c_api.lattice_node_create(txn, "TestLabel", &node_id);
    try std.testing.expectEqual(lattice_error.err_read_only, result);

    _ = c_api.lattice_commit(txn);
}

test "c_api: rollback undoes node creation" {
    const path = "/tmp/lattice_capi_rollback_node_create.db";
    std.fs.cwd().deleteFile(path) catch {};
    std.fs.cwd().deleteFile(path ++ "-wal") catch {};

    var db: ?*lattice_database = null;
    const options = lattice_open_options{
        .create = true,
        .read_only = false,
        .cache_size_mb = 4,
        .page_size = 4096,
        .enable_vector = false,
        .vector_dimensions = 0,
    };

    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_open(path, &options, &db));
    defer {
        _ = c_api.lattice_close(db);
        std.fs.cwd().deleteFile(path) catch {};
        std.fs.cwd().deleteFile(path ++ "-wal") catch {};
    }

    // Begin transaction and create node
    var txn: ?*lattice_txn = null;
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_begin(db, .read_write, &txn));

    var node_id: lattice_node_id = 0;
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_node_create(txn, "Person", &node_id));

    // Verify node exists within transaction
    var exists: bool = false;
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_node_exists(txn, node_id, &exists));
    try std.testing.expect(exists);

    // Rollback transaction
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_rollback(txn));

    // Start new transaction to verify node doesn't exist
    var txn2: ?*lattice_txn = null;
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_begin(db, .read_only, &txn2));

    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_node_exists(txn2, node_id, &exists));
    try std.testing.expect(!exists); // Node should NOT exist after rollback

    _ = c_api.lattice_commit(txn2);
}

test "c_api: rollback undoes node deletion" {
    const path = "/tmp/lattice_capi_rollback_node_delete.db";
    std.fs.cwd().deleteFile(path) catch {};
    std.fs.cwd().deleteFile(path ++ "-wal") catch {};

    var db: ?*lattice_database = null;
    const options = lattice_open_options{
        .create = true,
        .read_only = false,
        .cache_size_mb = 4,
        .page_size = 4096,
        .enable_vector = false,
        .vector_dimensions = 0,
    };

    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_open(path, &options, &db));
    defer {
        _ = c_api.lattice_close(db);
        std.fs.cwd().deleteFile(path) catch {};
        std.fs.cwd().deleteFile(path ++ "-wal") catch {};
    }

    // First, create and commit a node
    var node_id: lattice_node_id = 0;
    {
        var txn: ?*lattice_txn = null;
        try std.testing.expectEqual(lattice_error.ok, c_api.lattice_begin(db, .read_write, &txn));
        try std.testing.expectEqual(lattice_error.ok, c_api.lattice_node_create(txn, "Person", &node_id));
        try std.testing.expectEqual(lattice_error.ok, c_api.lattice_commit(txn));
    }

    // Now delete the node in a new transaction but rollback
    {
        var txn: ?*lattice_txn = null;
        try std.testing.expectEqual(lattice_error.ok, c_api.lattice_begin(db, .read_write, &txn));

        // Verify node exists before delete
        var exists: bool = false;
        try std.testing.expectEqual(lattice_error.ok, c_api.lattice_node_exists(txn, node_id, &exists));
        try std.testing.expect(exists);

        // Delete node
        try std.testing.expectEqual(lattice_error.ok, c_api.lattice_node_delete(txn, node_id));

        // Verify node doesn't exist after delete (within same txn)
        try std.testing.expectEqual(lattice_error.ok, c_api.lattice_node_exists(txn, node_id, &exists));
        try std.testing.expect(!exists);

        // Rollback
        try std.testing.expectEqual(lattice_error.ok, c_api.lattice_rollback(txn));
    }

    // Verify node still exists after rollback
    {
        var txn: ?*lattice_txn = null;
        try std.testing.expectEqual(lattice_error.ok, c_api.lattice_begin(db, .read_only, &txn));

        var exists: bool = false;
        try std.testing.expectEqual(lattice_error.ok, c_api.lattice_node_exists(txn, node_id, &exists));
        try std.testing.expect(exists); // Node should still exist after rollback

        _ = c_api.lattice_commit(txn);
    }
}

test "c_api: rollback undoes property changes" {
    const path = "/tmp/lattice_capi_rollback_property.db";
    std.fs.cwd().deleteFile(path) catch {};
    std.fs.cwd().deleteFile(path ++ "-wal") catch {};

    var db: ?*lattice_database = null;
    const options = lattice_open_options{
        .create = true,
        .read_only = false,
        .cache_size_mb = 4,
        .page_size = 4096,
        .enable_vector = false,
        .vector_dimensions = 0,
    };

    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_open(path, &options, &db));
    defer {
        _ = c_api.lattice_close(db);
        std.fs.cwd().deleteFile(path) catch {};
        std.fs.cwd().deleteFile(path ++ "-wal") catch {};
    }

    // Create node with initial property
    var node_id: lattice_node_id = 0;
    {
        var txn: ?*lattice_txn = null;
        try std.testing.expectEqual(lattice_error.ok, c_api.lattice_begin(db, .read_write, &txn));
        try std.testing.expectEqual(lattice_error.ok, c_api.lattice_node_create(txn, "Person", &node_id));

        var age_value = lattice_value{
            .value_type = .int,
            .data = .{ .int_val = 25 },
        };
        try std.testing.expectEqual(lattice_error.ok, c_api.lattice_node_set_property(txn, node_id, "age", &age_value));
        try std.testing.expectEqual(lattice_error.ok, c_api.lattice_commit(txn));
    }

    // Modify property but rollback
    {
        var txn: ?*lattice_txn = null;
        try std.testing.expectEqual(lattice_error.ok, c_api.lattice_begin(db, .read_write, &txn));

        // Change age to 30
        var new_age = lattice_value{
            .value_type = .int,
            .data = .{ .int_val = 30 },
        };
        try std.testing.expectEqual(lattice_error.ok, c_api.lattice_node_set_property(txn, node_id, "age", &new_age));

        // Verify new value is visible within transaction
        var retrieved: lattice_value = undefined;
        try std.testing.expectEqual(lattice_error.ok, c_api.lattice_node_get_property(txn, node_id, "age", &retrieved));
        try std.testing.expectEqual(@as(i64, 30), retrieved.data.int_val);

        // Rollback
        try std.testing.expectEqual(lattice_error.ok, c_api.lattice_rollback(txn));
    }

    // Verify original value is restored after rollback
    {
        var txn: ?*lattice_txn = null;
        try std.testing.expectEqual(lattice_error.ok, c_api.lattice_begin(db, .read_only, &txn));

        var retrieved: lattice_value = undefined;
        try std.testing.expectEqual(lattice_error.ok, c_api.lattice_node_get_property(txn, node_id, "age", &retrieved));
        try std.testing.expectEqual(@as(i64, 25), retrieved.data.int_val); // Original value restored

        _ = c_api.lattice_commit(txn);
    }
}

test "c_api: committed changes persist across transactions" {
    const path = "/tmp/lattice_capi_commit_persist.db";
    std.fs.cwd().deleteFile(path) catch {};
    std.fs.cwd().deleteFile(path ++ "-wal") catch {};

    var db: ?*lattice_database = null;
    const options = lattice_open_options{
        .create = true,
        .read_only = false,
        .cache_size_mb = 4,
        .page_size = 4096,
        .enable_vector = false,
        .vector_dimensions = 0,
    };

    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_open(path, &options, &db));
    defer {
        _ = c_api.lattice_close(db);
        std.fs.cwd().deleteFile(path) catch {};
        std.fs.cwd().deleteFile(path ++ "-wal") catch {};
    }

    var node_id: lattice_node_id = 0;

    // Create node and set property, then commit
    {
        var txn: ?*lattice_txn = null;
        try std.testing.expectEqual(lattice_error.ok, c_api.lattice_begin(db, .read_write, &txn));

        try std.testing.expectEqual(lattice_error.ok, c_api.lattice_node_create(txn, "Person", &node_id));

        const name = "Alice";
        var name_value = lattice_value{
            .value_type = .string,
            .data = .{ .string_val = .{ .ptr = name.ptr, .len = name.len } },
        };
        try std.testing.expectEqual(lattice_error.ok, c_api.lattice_node_set_property(txn, node_id, "name", &name_value));

        try std.testing.expectEqual(lattice_error.ok, c_api.lattice_commit(txn));
    }

    // Verify in new transaction that changes persisted
    {
        var txn: ?*lattice_txn = null;
        try std.testing.expectEqual(lattice_error.ok, c_api.lattice_begin(db, .read_only, &txn));

        // Node should exist
        var exists: bool = false;
        try std.testing.expectEqual(lattice_error.ok, c_api.lattice_node_exists(txn, node_id, &exists));
        try std.testing.expect(exists);

        // Property should be set
        var retrieved: lattice_value = undefined;
        try std.testing.expectEqual(lattice_error.ok, c_api.lattice_node_get_property(txn, node_id, "name", &retrieved));
        try std.testing.expectEqual(lattice_value_type.string, retrieved.value_type);

        _ = c_api.lattice_commit(txn);
    }
}

test "c_api: rollback undoes edge creation" {
    const path = "/tmp/lattice_capi_rollback_edge.db";
    std.fs.cwd().deleteFile(path) catch {};
    std.fs.cwd().deleteFile(path ++ "-wal") catch {};

    var db: ?*lattice_database = null;
    const options = lattice_open_options{
        .create = true,
        .read_only = false,
        .cache_size_mb = 4,
        .page_size = 4096,
        .enable_vector = false,
        .vector_dimensions = 0,
    };

    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_open(path, &options, &db));
    defer {
        _ = c_api.lattice_close(db);
        std.fs.cwd().deleteFile(path) catch {};
        std.fs.cwd().deleteFile(path ++ "-wal") catch {};
    }

    // Create two nodes first (committed)
    var alice: lattice_node_id = 0;
    var bob: lattice_node_id = 0;
    {
        var txn: ?*lattice_txn = null;
        try std.testing.expectEqual(lattice_error.ok, c_api.lattice_begin(db, .read_write, &txn));
        try std.testing.expectEqual(lattice_error.ok, c_api.lattice_node_create(txn, "Person", &alice));
        try std.testing.expectEqual(lattice_error.ok, c_api.lattice_node_create(txn, "Person", &bob));
        try std.testing.expectEqual(lattice_error.ok, c_api.lattice_commit(txn));
    }

    // Create edge but rollback
    {
        var txn: ?*lattice_txn = null;
        try std.testing.expectEqual(lattice_error.ok, c_api.lattice_begin(db, .read_write, &txn));

        var edge_id: u64 = 0;
        try std.testing.expectEqual(lattice_error.ok, c_api.lattice_edge_create(txn, alice, bob, "KNOWS", &edge_id));

        // Verify edge exists within transaction
        var edge_result: ?*c_api.lattice_edge_result = null;
        try std.testing.expectEqual(lattice_error.ok, c_api.lattice_edge_get_outgoing(txn, alice, &edge_result));
        const count = c_api.lattice_edge_result_count(edge_result);
        try std.testing.expectEqual(@as(u32, 1), count);
        c_api.lattice_edge_result_free(edge_result);

        // Rollback
        try std.testing.expectEqual(lattice_error.ok, c_api.lattice_rollback(txn));
    }

    // Verify edge doesn't exist after rollback
    {
        var txn: ?*lattice_txn = null;
        try std.testing.expectEqual(lattice_error.ok, c_api.lattice_begin(db, .read_only, &txn));

        var edge_result: ?*c_api.lattice_edge_result = null;
        try std.testing.expectEqual(lattice_error.ok, c_api.lattice_edge_get_outgoing(txn, alice, &edge_result));
        const count = c_api.lattice_edge_result_count(edge_result);
        try std.testing.expectEqual(@as(u32, 0), count); // No edges after rollback
        c_api.lattice_edge_result_free(edge_result);

        _ = c_api.lattice_commit(txn);
    }
}

// ============================================================================
// Node Operations Tests
// ============================================================================

test "c_api: create and check node exists" {
    const path = "/tmp/lattice_capi_node_create_test.db";
    std.fs.cwd().deleteFile(path) catch {};

    var db: ?*lattice_database = null;
    const options = lattice_open_options{
        .create = true,
        .read_only = false,
        .cache_size_mb = 4,
        .page_size = 4096,
        .enable_vector = false,
        .vector_dimensions = 0,
    };

    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_open(path, &options, &db));
    defer {
        _ = c_api.lattice_close(db);
        std.fs.cwd().deleteFile(path) catch {};
    }

    var txn: ?*lattice_txn = null;
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_begin(db, .read_write, &txn));

    // Create node
    var node_id: lattice_node_id = 0;
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_node_create(txn, "Person", &node_id));
    try std.testing.expect(node_id > 0);

    // Check exists
    var exists: bool = false;
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_node_exists(txn, node_id, &exists));
    try std.testing.expect(exists);

    // Check nonexistent
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_node_exists(txn, 99999, &exists));
    try std.testing.expect(!exists);

    _ = c_api.lattice_commit(txn);
}

test "c_api: node properties set and get" {
    const path = "/tmp/lattice_capi_node_props_test.db";
    std.fs.cwd().deleteFile(path) catch {};

    var db: ?*lattice_database = null;
    const options = lattice_open_options{
        .create = true,
        .read_only = false,
        .cache_size_mb = 4,
        .page_size = 4096,
        .enable_vector = false,
        .vector_dimensions = 0,
    };

    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_open(path, &options, &db));
    defer {
        _ = c_api.lattice_close(db);
        std.fs.cwd().deleteFile(path) catch {};
    }

    var txn: ?*lattice_txn = null;
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_begin(db, .read_write, &txn));

    // Create node
    var node_id: lattice_node_id = 0;
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_node_create(txn, "Person", &node_id));

    // Set string property
    const name = "Alice";
    var name_value = lattice_value{
        .value_type = .string,
        .data = .{ .string_val = .{ .ptr = name.ptr, .len = name.len } },
    };
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_node_set_property(txn, node_id, "name", &name_value));

    // Set int property
    var age_value = lattice_value{
        .value_type = .int,
        .data = .{ .int_val = 30 },
    };
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_node_set_property(txn, node_id, "age", &age_value));

    // Get string property
    var retrieved_name: lattice_value = undefined;
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_node_get_property(txn, node_id, "name", &retrieved_name));
    try std.testing.expectEqual(lattice_value_type.string, retrieved_name.value_type);

    // Get int property
    var retrieved_age: lattice_value = undefined;
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_node_get_property(txn, node_id, "age", &retrieved_age));
    try std.testing.expectEqual(lattice_value_type.int, retrieved_age.value_type);
    try std.testing.expectEqual(@as(i64, 30), retrieved_age.data.int_val);

    // Get nonexistent property
    var no_prop: lattice_value = undefined;
    try std.testing.expectEqual(lattice_error.err_not_found, c_api.lattice_node_get_property(txn, node_id, "nonexistent", &no_prop));

    _ = c_api.lattice_commit(txn);
}

test "c_api: delete node" {
    const path = "/tmp/lattice_capi_node_delete_test.db";
    std.fs.cwd().deleteFile(path) catch {};

    var db: ?*lattice_database = null;
    const options = lattice_open_options{
        .create = true,
        .read_only = false,
        .cache_size_mb = 4,
        .page_size = 4096,
        .enable_vector = false,
        .vector_dimensions = 0,
    };

    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_open(path, &options, &db));
    defer {
        _ = c_api.lattice_close(db);
        std.fs.cwd().deleteFile(path) catch {};
    }

    var txn: ?*lattice_txn = null;
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_begin(db, .read_write, &txn));

    // Create and delete node
    var node_id: lattice_node_id = 0;
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_node_create(txn, "ToDelete", &node_id));

    var exists: bool = false;
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_node_exists(txn, node_id, &exists));
    try std.testing.expect(exists);

    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_node_delete(txn, node_id));

    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_node_exists(txn, node_id, &exists));
    try std.testing.expect(!exists);

    _ = c_api.lattice_commit(txn);
}

test "c_api: get node labels" {
    const path = "/tmp/lattice_capi_labels_test.db";
    std.fs.cwd().deleteFile(path) catch {};

    var db: ?*lattice_database = null;
    const options = lattice_open_options{
        .create = true,
        .read_only = false,
        .cache_size_mb = 4,
        .page_size = 4096,
        .enable_vector = false,
        .vector_dimensions = 0,
    };

    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_open(path, &options, &db));
    defer {
        _ = c_api.lattice_close(db);
        std.fs.cwd().deleteFile(path) catch {};
    }

    var txn: ?*lattice_txn = null;
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_begin(db, .read_write, &txn));

    var node_id: lattice_node_id = 0;
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_node_create(txn, "Person", &node_id));

    // Get labels
    var labels: [*c]u8 = null;
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_node_get_labels(txn, node_id, &labels));
    try std.testing.expect(labels != null);

    // Verify label string
    const label_str = std.mem.sliceTo(labels, 0);
    try std.testing.expectEqualStrings("Person", label_str);

    // Free the string
    c_api.lattice_free_string(labels);

    _ = c_api.lattice_commit(txn);
}

// ============================================================================
// Edge Operations Tests
// ============================================================================

test "c_api: create edge" {
    const path = "/tmp/lattice_capi_edge_test.db";
    std.fs.cwd().deleteFile(path) catch {};

    var db: ?*lattice_database = null;
    const options = lattice_open_options{
        .create = true,
        .read_only = false,
        .cache_size_mb = 4,
        .page_size = 4096,
        .enable_vector = false,
        .vector_dimensions = 0,
    };

    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_open(path, &options, &db));
    defer {
        _ = c_api.lattice_close(db);
        std.fs.cwd().deleteFile(path) catch {};
    }

    var txn: ?*lattice_txn = null;
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_begin(db, .read_write, &txn));

    // Create two nodes
    var alice: lattice_node_id = 0;
    var bob: lattice_node_id = 0;
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_node_create(txn, "Person", &alice));
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_node_create(txn, "Person", &bob));

    // Create edge
    var edge_id: u64 = 0;
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_edge_create(txn, alice, bob, "KNOWS", &edge_id));

    _ = c_api.lattice_commit(txn);
}

test "c_api: get outgoing edges" {
    const path = "/tmp/lattice_capi_outgoing_test.db";
    std.fs.cwd().deleteFile(path) catch {};

    var db: ?*lattice_database = null;
    const options = lattice_open_options{
        .create = true,
        .read_only = false,
        .cache_size_mb = 4,
        .page_size = 4096,
        .enable_vector = false,
        .vector_dimensions = 0,
    };

    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_open(path, &options, &db));
    defer {
        _ = c_api.lattice_close(db);
        std.fs.cwd().deleteFile(path) catch {};
    }

    var txn: ?*lattice_txn = null;
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_begin(db, .read_write, &txn));

    // Create nodes and edges
    var alice: lattice_node_id = 0;
    var bob: lattice_node_id = 0;
    var charlie: lattice_node_id = 0;
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_node_create(txn, "Person", &alice));
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_node_create(txn, "Person", &bob));
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_node_create(txn, "Person", &charlie));

    var edge_id: u64 = 0;
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_edge_create(txn, alice, bob, "KNOWS", &edge_id));
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_edge_create(txn, alice, charlie, "LIKES", &edge_id));

    // Get outgoing edges from alice
    var edge_result: ?*c_api.lattice_edge_result = null;
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_edge_get_outgoing(txn, alice, &edge_result));
    try std.testing.expect(edge_result != null);

    // Check count
    const count = c_api.lattice_edge_result_count(edge_result);
    try std.testing.expectEqual(@as(u32, 2), count);

    // Free result
    c_api.lattice_edge_result_free(edge_result);

    _ = c_api.lattice_commit(txn);
}

// ============================================================================
// Query Tests
// ============================================================================

test "c_api: prepare and execute query" {
    const path = "/tmp/lattice_capi_query_test.db";
    std.fs.cwd().deleteFile(path) catch {};

    var db: ?*lattice_database = null;
    const options = lattice_open_options{
        .create = true,
        .read_only = false,
        .cache_size_mb = 4,
        .page_size = 4096,
        .enable_vector = false,
        .vector_dimensions = 0,
    };

    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_open(path, &options, &db));
    defer {
        _ = c_api.lattice_close(db);
        std.fs.cwd().deleteFile(path) catch {};
    }

    var txn: ?*lattice_txn = null;
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_begin(db, .read_write, &txn));

    // Create test data
    var node_id: lattice_node_id = 0;
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_node_create(txn, "Person", &node_id));
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_node_create(txn, "Person", &node_id));
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_node_create(txn, "Company", &node_id));

    // Prepare query
    var query: ?*lattice_query = null;
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_query_prepare(db, "MATCH (n:Person) RETURN n", &query));
    try std.testing.expect(query != null);

    // Execute query
    var result: ?*lattice_result = null;
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_query_execute(query, txn, &result));
    try std.testing.expect(result != null);

    // Iterate results
    var row_count: usize = 0;
    while (c_api.lattice_result_next(result)) {
        row_count += 1;
    }
    try std.testing.expectEqual(@as(usize, 2), row_count); // 2 Person nodes

    // Check column count
    try std.testing.expectEqual(@as(u32, 1), c_api.lattice_result_column_count(result));

    // Free resources
    c_api.lattice_result_free(result);
    c_api.lattice_query_free(query);

    _ = c_api.lattice_commit(txn);
}

test "c_api: query with invalid syntax returns error" {
    const path = "/tmp/lattice_capi_bad_query_test.db";
    std.fs.cwd().deleteFile(path) catch {};

    var db: ?*lattice_database = null;
    const options = lattice_open_options{
        .create = true,
        .read_only = false,
        .cache_size_mb = 4,
        .page_size = 4096,
        .enable_vector = false,
        .vector_dimensions = 0,
    };

    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_open(path, &options, &db));
    defer {
        _ = c_api.lattice_close(db);
        std.fs.cwd().deleteFile(path) catch {};
    }

    var txn: ?*lattice_txn = null;
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_begin(db, .read_write, &txn));

    // Prepare query with bad syntax
    var query: ?*lattice_query = null;
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_query_prepare(db, "MATCH (n RETURN n", &query));

    // Execute should fail
    var result: ?*lattice_result = null;
    const exec_result = c_api.lattice_query_execute(query, txn, &result);
    try std.testing.expectEqual(lattice_error.err_invalid_arg, exec_result);

    c_api.lattice_query_free(query);
    _ = c_api.lattice_commit(txn);
}

// ============================================================================
// Utility Function Tests
// ============================================================================

test "c_api: version returns valid string" {
    const version = c_api.lattice_version();
    try std.testing.expect(version != null);
    const ver_str = std.mem.sliceTo(version, 0);
    try std.testing.expectEqualStrings("0.1.0", ver_str);
}

test "c_api: error messages are valid" {
    // Test all error codes have messages
    const errors = [_]lattice_error{
        .ok,
        .err,
        .err_io,
        .err_corruption,
        .err_not_found,
        .err_already_exists,
        .err_invalid_arg,
        .err_txn_aborted,
        .err_lock_timeout,
        .err_read_only,
        .err_full,
        .err_version_mismatch,
        .err_checksum,
        .err_out_of_memory,
    };

    for (errors) |err| {
        const msg = c_api.lattice_error_message(err);
        try std.testing.expect(msg != null);
        const msg_str = std.mem.sliceTo(msg, 0);
        try std.testing.expect(msg_str.len > 0);
    }
}

test "c_api: null argument handling" {
    // Various null argument tests
    try std.testing.expectEqual(lattice_error.err_invalid_arg, c_api.lattice_close(null));

    var txn: ?*lattice_txn = null;
    try std.testing.expectEqual(lattice_error.err_invalid_arg, c_api.lattice_begin(null, .read_write, &txn));
    try std.testing.expectEqual(lattice_error.err_invalid_arg, c_api.lattice_commit(null));
    try std.testing.expectEqual(lattice_error.err_invalid_arg, c_api.lattice_rollback(null));

    var node_id: lattice_node_id = 0;
    try std.testing.expectEqual(lattice_error.err_invalid_arg, c_api.lattice_node_create(null, "Label", &node_id));
}

// ============================================================================
// Memory Management Tests
// ============================================================================

test "c_api: string allocation and free" {
    const path = "/tmp/lattice_capi_string_test.db";
    std.fs.cwd().deleteFile(path) catch {};

    var db: ?*lattice_database = null;
    const options = lattice_open_options{
        .create = true,
        .read_only = false,
        .cache_size_mb = 4,
        .page_size = 4096,
        .enable_vector = false,
        .vector_dimensions = 0,
    };

    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_open(path, &options, &db));
    defer {
        _ = c_api.lattice_close(db);
        std.fs.cwd().deleteFile(path) catch {};
    }

    var txn: ?*lattice_txn = null;
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_begin(db, .read_write, &txn));

    // Create node and get labels multiple times to test memory management
    var node_id: lattice_node_id = 0;
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_node_create(txn, "TestLabel", &node_id));

    // Get and free labels multiple times
    for (0..10) |_| {
        var labels: [*c]u8 = null;
        try std.testing.expectEqual(lattice_error.ok, c_api.lattice_node_get_labels(txn, node_id, &labels));
        c_api.lattice_free_string(labels);
    }

    // Free null should be safe
    c_api.lattice_free_string(null);

    _ = c_api.lattice_commit(txn);
}

test "c_api: result set iteration and cleanup" {
    const path = "/tmp/lattice_capi_result_cleanup_test.db";
    std.fs.cwd().deleteFile(path) catch {};

    var db: ?*lattice_database = null;
    const options = lattice_open_options{
        .create = true,
        .read_only = false,
        .cache_size_mb = 4,
        .page_size = 4096,
        .enable_vector = false,
        .vector_dimensions = 0,
    };

    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_open(path, &options, &db));
    defer {
        _ = c_api.lattice_close(db);
        std.fs.cwd().deleteFile(path) catch {};
    }

    var txn: ?*lattice_txn = null;
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_begin(db, .read_write, &txn));

    // Create test data
    for (0..5) |_| {
        var node_id: lattice_node_id = 0;
        _ = c_api.lattice_node_create(txn, "Item", &node_id);
    }

    // Execute multiple queries to test cleanup
    for (0..3) |_| {
        var query: ?*lattice_query = null;
        try std.testing.expectEqual(lattice_error.ok, c_api.lattice_query_prepare(db, "MATCH (n:Item) RETURN n", &query));

        var result: ?*lattice_result = null;
        try std.testing.expectEqual(lattice_error.ok, c_api.lattice_query_execute(query, txn, &result));

        // Iterate through results
        while (c_api.lattice_result_next(result)) {
            var value: lattice_value = undefined;
            _ = c_api.lattice_result_get(result, 0, &value);
        }

        c_api.lattice_result_free(result);
        c_api.lattice_query_free(query);
    }

    _ = c_api.lattice_commit(txn);
}
