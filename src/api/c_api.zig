//! C API bindings for Lattice database.
//!
//! Provides the stable C ABI interface for language bindings.
//! This is the primary API exposed to C/C++ applications and serves
//! as the foundation for Python and TypeScript bindings.

const std = @import("std");
const Allocator = std.mem.Allocator;

const lattice = @import("lattice");

const types = lattice.core.types;
const PropertyValue = types.PropertyValue;
const database = lattice.storage.database;
const Database = database.Database;
const DatabaseError = database.DatabaseError;
const QueryError = database.QueryError;
const QueryResult = database.QueryResult;
const ResultValue = database.ResultValue;
const OpenOptions = database.OpenOptions;
const DatabaseConfig = database.DatabaseConfig;

// ============================================================================
// Global Allocator
// ============================================================================

/// Global general-purpose allocator for C API.
/// C callers cannot provide Zig allocators, so we use a global one.
var gpa = std.heap.GeneralPurposeAllocator(.{}){};
const global_allocator = gpa.allocator();

// ============================================================================
// Opaque Handle Types
// ============================================================================

/// Internal database handle wrapping the Zig Database
const DatabaseHandle = struct {
    db: *Database,
};

/// Internal transaction handle (placeholder for future MVCC support)
const TxnHandle = struct {
    db_handle: *DatabaseHandle,
    read_only: bool,
};

/// Internal query handle storing prepared query state
const QueryHandle = struct {
    cypher: []const u8,
    cypher_owned: bool,
    db_handle: *DatabaseHandle,
};

/// Internal result handle wrapping query results
const ResultHandle = struct {
    result: QueryResult,
    current_row: usize,
    started: bool,
};

// ============================================================================
// C-Exposed Opaque Types
// ============================================================================

/// Opaque database handle for C API
pub const lattice_database = opaque {};

/// Opaque transaction handle for C API
pub const lattice_txn = opaque {};

/// Opaque query handle for C API
pub const lattice_query = opaque {};

/// Opaque result set handle for C API
pub const lattice_result = opaque {};

/// Node ID type for C API
pub const lattice_node_id = types.NodeId;

/// Edge ID type for C API
pub const lattice_edge_id = types.EdgeId;

// ============================================================================
// Error Codes
// ============================================================================

/// Error codes matching lattice.h
pub const lattice_error = enum(c_int) {
    ok = 0,
    err = -1,
    err_io = -2,
    err_corruption = -3,
    err_not_found = -4,
    err_already_exists = -5,
    err_invalid_arg = -6,
    err_txn_aborted = -7,
    err_lock_timeout = -8,
    err_read_only = -9,
    err_full = -10,
    err_version_mismatch = -11,
    err_checksum = -12,
    err_out_of_memory = -13,
};

/// Map Zig database errors to C error codes
fn mapDatabaseError(err: DatabaseError) lattice_error {
    return switch (err) {
        DatabaseError.FileNotFound => .err_not_found,
        DatabaseError.PermissionDenied => .err_read_only,
        DatabaseError.InvalidDatabase => .err_corruption,
        DatabaseError.IoError => .err_io,
        DatabaseError.OutOfMemory => .err_out_of_memory,
        DatabaseError.BufferPoolFull => .err_full,
        DatabaseError.TreeInitFailed => .err_io,
        DatabaseError.AlreadyExists => .err_already_exists,
        DatabaseError.ReadOnly => .err_read_only,
        DatabaseError.NotFound => .err_not_found,
    };
}

/// Map Zig query errors to C error codes
fn mapQueryError(err: QueryError) lattice_error {
    return switch (err) {
        QueryError.ParseError => .err_invalid_arg,
        QueryError.SemanticError => .err_invalid_arg,
        QueryError.PlanError => .err,
        QueryError.ExecutionError => .err,
        QueryError.OutOfMemory => .err_out_of_memory,
    };
}

/// Map any error to C error code
fn mapAnyError(err: anyerror) lattice_error {
    return switch (err) {
        error.OutOfMemory => .err_out_of_memory,
        error.FileNotFound => .err_not_found,
        error.PermissionDenied => .err_read_only,
        else => .err,
    };
}

// ============================================================================
// Transaction Mode
// ============================================================================

/// Transaction mode for C API
pub const lattice_txn_mode = enum(c_int) {
    read_only = 0,
    read_write = 1,
};

// ============================================================================
// Value Types
// ============================================================================

/// Property value type tags
pub const lattice_value_type = enum(c_int) {
    null = 0,
    bool = 1,
    int = 2,
    float = 3,
    string = 4,
    bytes = 5,
    list = 6,
    map = 7,
};

/// Property value for C API
pub const lattice_value = extern struct {
    value_type: lattice_value_type,
    data: extern union {
        bool_val: bool,
        int_val: i64,
        float_val: f64,
        string_val: extern struct {
            ptr: [*c]const u8,
            len: usize,
        },
        bytes_val: extern struct {
            ptr: [*c]const u8,
            len: usize,
        },
    },
};

/// Open options for C API
pub const lattice_open_options = extern struct {
    create: bool = false,
    read_only: bool = false,
    cache_size_mb: u32 = 100,
    page_size: u32 = 4096,
};

// ============================================================================
// Helper Functions
// ============================================================================

/// Convert C string to Zig slice
fn cStrToSlice(c_str: [*c]const u8) ?[]const u8 {
    if (c_str == null) return null;
    return std.mem.sliceTo(c_str, 0);
}

/// Convert Zig PropertyValue to C lattice_value
fn zigValueToCValue(zig_val: PropertyValue, c_val: *lattice_value) void {
    switch (zig_val) {
        .null_val => c_val.value_type = .null,
        .bool_val => |b| {
            c_val.value_type = .bool;
            c_val.data.bool_val = b;
        },
        .int_val => |i| {
            c_val.value_type = .int;
            c_val.data.int_val = i;
        },
        .float_val => |f| {
            c_val.value_type = .float;
            c_val.data.float_val = f;
        },
        .string_val => |s| {
            c_val.value_type = .string;
            c_val.data.string_val.ptr = s.ptr;
            c_val.data.string_val.len = s.len;
        },
        .bytes_val => |b| {
            c_val.value_type = .bytes;
            c_val.data.bytes_val.ptr = b.ptr;
            c_val.data.bytes_val.len = b.len;
        },
        .list_val, .map_val => {
            // Complex types not supported in MVP
            c_val.value_type = .null;
        },
    }
}

/// Convert ResultValue to C lattice_value
fn resultValueToCValue(result_val: ResultValue, c_val: *lattice_value) void {
    switch (result_val) {
        .null_val => c_val.value_type = .null,
        .bool_val => |b| {
            c_val.value_type = .bool;
            c_val.data.bool_val = b;
        },
        .int_val => |i| {
            c_val.value_type = .int;
            c_val.data.int_val = i;
        },
        .float_val => |f| {
            c_val.value_type = .float;
            c_val.data.float_val = f;
        },
        .string_val => |s| {
            c_val.value_type = .string;
            c_val.data.string_val.ptr = s.ptr;
            c_val.data.string_val.len = s.len;
        },
        .node_id => |id| {
            c_val.value_type = .int;
            c_val.data.int_val = @intCast(id);
        },
    }
}

/// Convert C lattice_value to Zig PropertyValue
fn cValueToZigValue(c_val: *const lattice_value) PropertyValue {
    return switch (c_val.value_type) {
        .null => .{ .null_val = {} },
        .bool => .{ .bool_val = c_val.data.bool_val },
        .int => .{ .int_val = c_val.data.int_val },
        .float => .{ .float_val = c_val.data.float_val },
        .string => .{ .string_val = c_val.data.string_val.ptr[0..c_val.data.string_val.len] },
        .bytes => .{ .bytes_val = c_val.data.bytes_val.ptr[0..c_val.data.bytes_val.len] },
        .list, .map => .{ .null_val = {} },
    };
}

/// Cast opaque C pointer to internal handle
fn toHandle(comptime T: type, ptr: anytype) ?*T {
    if (@intFromPtr(ptr) == 0) return null;
    return @ptrCast(@alignCast(ptr));
}

/// Cast internal handle to opaque C pointer
fn toOpaque(comptime T: type, handle: *anyopaque) *T {
    return @ptrCast(handle);
}

// ============================================================================
// Database Operations
// ============================================================================

/// Open a database file
pub export fn lattice_open(
    path: [*c]const u8,
    options: ?*const lattice_open_options,
    db_out: *?*lattice_database,
) lattice_error {
    db_out.* = null;

    const path_slice = cStrToSlice(path) orelse return .err_invalid_arg;

    // Build Zig open options
    var zig_options = OpenOptions{
        .create = false,
        .read_only = false,
        .config = DatabaseConfig{},
    };

    if (options) |opts| {
        zig_options.create = opts.create;
        zig_options.read_only = opts.read_only;
        zig_options.config.buffer_pool_size = @as(usize, opts.cache_size_mb) * 1024 * 1024;
    }

    // Open the database
    const db = Database.open(global_allocator, path_slice, zig_options) catch |err| {
        return mapDatabaseError(err);
    };

    // Create handle
    const handle = global_allocator.create(DatabaseHandle) catch return .err_out_of_memory;
    handle.* = .{ .db = db };

    db_out.* = @ptrCast(handle);
    return .ok;
}

/// Close a database
pub export fn lattice_close(db: ?*lattice_database) lattice_error {
    const handle = toHandle(DatabaseHandle, db) orelse return .err_invalid_arg;

    handle.db.close();
    global_allocator.destroy(handle);

    return .ok;
}

// ============================================================================
// Transaction Operations
// ============================================================================

/// Begin a transaction
pub export fn lattice_begin(
    db: ?*lattice_database,
    mode: lattice_txn_mode,
    txn_out: *?*lattice_txn,
) lattice_error {
    txn_out.* = null;

    const db_handle = toHandle(DatabaseHandle, db) orelse return .err_invalid_arg;

    // Create transaction handle
    const txn_handle = global_allocator.create(TxnHandle) catch return .err_out_of_memory;
    txn_handle.* = .{
        .db_handle = db_handle,
        .read_only = mode == .read_only,
    };

    txn_out.* = @ptrCast(txn_handle);
    return .ok;
}

/// Commit a transaction
pub export fn lattice_commit(txn: ?*lattice_txn) lattice_error {
    const txn_handle = toHandle(TxnHandle, txn) orelse return .err_invalid_arg;

    // For MVP, no explicit commit needed
    global_allocator.destroy(txn_handle);
    return .ok;
}

/// Rollback a transaction
pub export fn lattice_rollback(txn: ?*lattice_txn) lattice_error {
    const txn_handle = toHandle(TxnHandle, txn) orelse return .err_invalid_arg;

    // For MVP, no rollback support - just free handle
    global_allocator.destroy(txn_handle);
    return .ok;
}

// ============================================================================
// Node Operations
// ============================================================================

/// Create a node with a label
pub export fn lattice_node_create(
    txn: ?*lattice_txn,
    label: [*c]const u8,
    node_out: *lattice_node_id,
) lattice_error {
    const txn_handle = toHandle(TxnHandle, txn) orelse return .err_invalid_arg;

    if (txn_handle.read_only) return .err_read_only;

    const label_slice = cStrToSlice(label) orelse return .err_invalid_arg;
    const labels = [_][]const u8{label_slice};

    const node_id = txn_handle.db_handle.db.createNode(&labels) catch |err| {
        return mapAnyError(err);
    };

    node_out.* = node_id;
    return .ok;
}

/// Delete a node
pub export fn lattice_node_delete(
    txn: ?*lattice_txn,
    node_id: lattice_node_id,
) lattice_error {
    const txn_handle = toHandle(TxnHandle, txn) orelse return .err_invalid_arg;

    if (txn_handle.read_only) return .err_read_only;

    txn_handle.db_handle.db.deleteNode(node_id) catch |err| {
        return mapAnyError(err);
    };

    return .ok;
}

/// Set a property on a node
pub export fn lattice_node_set_property(
    txn: ?*lattice_txn,
    node_id: lattice_node_id,
    key: [*c]const u8,
    value: ?*const lattice_value,
) lattice_error {
    const txn_handle = toHandle(TxnHandle, txn) orelse return .err_invalid_arg;

    if (txn_handle.read_only) return .err_read_only;

    const key_slice = cStrToSlice(key) orelse return .err_invalid_arg;
    const c_val = value orelse return .err_invalid_arg;

    // Convert C value to Zig PropertyValue
    const zig_value = cValueToZigValue(c_val);

    txn_handle.db_handle.db.setNodeProperty(node_id, key_slice, zig_value) catch |err| {
        return mapDatabaseError(err);
    };

    return .ok;
}

/// Get a property from a node
pub export fn lattice_node_get_property(
    txn: ?*lattice_txn,
    node_id: lattice_node_id,
    key: [*c]const u8,
    value_out: *lattice_value,
) lattice_error {
    const txn_handle = toHandle(TxnHandle, txn) orelse return .err_invalid_arg;

    const key_slice = cStrToSlice(key) orelse return .err_invalid_arg;

    const maybe_value = txn_handle.db_handle.db.getNodeProperty(node_id, key_slice) catch |err| {
        return mapDatabaseError(err);
    };

    if (maybe_value) |zig_value| {
        zigValueToCValue(zig_value, value_out);
        return .ok;
    } else {
        return .err_not_found;
    }
}

/// Set a vector on a node (stub for MVP)
pub export fn lattice_node_set_vector(
    txn: ?*lattice_txn,
    node_id: lattice_node_id,
    key: [*c]const u8,
    vector: [*c]const f32,
    dimensions: u32,
) lattice_error {
    _ = txn;
    _ = node_id;
    _ = key;
    _ = vector;
    _ = dimensions;
    // TODO: Implement vector operations
    return .err;
}

// ============================================================================
// Edge Operations
// ============================================================================

/// Create an edge between two nodes
pub export fn lattice_edge_create(
    txn: ?*lattice_txn,
    source: lattice_node_id,
    target: lattice_node_id,
    edge_type: [*c]const u8,
    edge_out: *lattice_edge_id,
) lattice_error {
    const txn_handle = toHandle(TxnHandle, txn) orelse return .err_invalid_arg;

    if (txn_handle.read_only) return .err_read_only;

    const type_slice = cStrToSlice(edge_type) orelse return .err_invalid_arg;

    txn_handle.db_handle.db.createEdge(source, target, type_slice) catch |err| {
        return mapAnyError(err);
    };

    // EdgeStore doesn't return edge IDs currently, use composite key
    edge_out.* = (source << 32) | target;
    return .ok;
}

/// Delete an edge between two nodes
pub export fn lattice_edge_delete(
    txn: ?*lattice_txn,
    source: lattice_node_id,
    target: lattice_node_id,
    edge_type: [*c]const u8,
) lattice_error {
    const txn_handle = toHandle(TxnHandle, txn) orelse return .err_invalid_arg;

    if (txn_handle.read_only) return .err_read_only;

    const type_slice = cStrToSlice(edge_type) orelse return .err_invalid_arg;

    txn_handle.db_handle.db.deleteEdge(source, target, type_slice) catch |err| {
        return mapDatabaseError(err);
    };

    return .ok;
}

// ============================================================================
// Query Operations
// ============================================================================

/// Prepare a Cypher query
pub export fn lattice_query_prepare(
    db: ?*lattice_database,
    cypher: [*c]const u8,
    query_out: *?*lattice_query,
) lattice_error {
    query_out.* = null;

    const db_handle = toHandle(DatabaseHandle, db) orelse return .err_invalid_arg;
    const cypher_slice = cStrToSlice(cypher) orelse return .err_invalid_arg;

    // Copy cypher string
    const cypher_copy = global_allocator.dupe(u8, cypher_slice) catch return .err_out_of_memory;

    // Create query handle
    const query_handle = global_allocator.create(QueryHandle) catch {
        global_allocator.free(cypher_copy);
        return .err_out_of_memory;
    };

    query_handle.* = .{
        .cypher = cypher_copy,
        .cypher_owned = true,
        .db_handle = db_handle,
    };

    query_out.* = @ptrCast(query_handle);
    return .ok;
}

/// Bind a parameter to a query (stub for MVP)
pub export fn lattice_query_bind(
    query: ?*lattice_query,
    name: [*c]const u8,
    value: ?*const lattice_value,
) lattice_error {
    _ = query;
    _ = name;
    _ = value;
    // TODO: Implement parameter binding
    return .ok;
}

/// Bind a vector parameter (stub for MVP)
pub export fn lattice_query_bind_vector(
    query: ?*lattice_query,
    name: [*c]const u8,
    vector: [*c]const f32,
    dimensions: u32,
) lattice_error {
    _ = query;
    _ = name;
    _ = vector;
    _ = dimensions;
    // TODO: Implement vector parameter binding
    return .ok;
}

/// Execute a prepared query
pub export fn lattice_query_execute(
    query: ?*lattice_query,
    txn: ?*lattice_txn,
    result_out: *?*lattice_result,
) lattice_error {
    result_out.* = null;

    const query_handle = toHandle(QueryHandle, query) orelse return .err_invalid_arg;
    _ = toHandle(TxnHandle, txn) orelse return .err_invalid_arg;

    // Execute the query
    const result = query_handle.db_handle.db.query(query_handle.cypher) catch |err| {
        return mapQueryError(err);
    };

    // Create result handle
    const result_handle = global_allocator.create(ResultHandle) catch {
        var mutable_result = result;
        mutable_result.deinit();
        return .err_out_of_memory;
    };

    result_handle.* = .{
        .result = result,
        .current_row = 0,
        .started = false,
    };

    result_out.* = @ptrCast(result_handle);
    return .ok;
}

/// Free a prepared query
pub export fn lattice_query_free(query: ?*lattice_query) void {
    const query_handle = toHandle(QueryHandle, query) orelse return;

    if (query_handle.cypher_owned) {
        global_allocator.free(@constCast(query_handle.cypher));
    }
    global_allocator.destroy(query_handle);
}

// ============================================================================
// Result Operations
// ============================================================================

/// Get next row from result set
pub export fn lattice_result_next(result: ?*lattice_result) bool {
    const result_handle = toHandle(ResultHandle, result) orelse return false;

    if (!result_handle.started) {
        result_handle.started = true;
        return result_handle.result.rows.len > 0;
    }

    result_handle.current_row += 1;
    return result_handle.current_row < result_handle.result.rows.len;
}

/// Get column count
pub export fn lattice_result_column_count(result: ?*lattice_result) u32 {
    const result_handle = toHandle(ResultHandle, result) orelse return 0;
    return @intCast(result_handle.result.columns.len);
}

/// Get column name
pub export fn lattice_result_column_name(result: ?*lattice_result, index: u32) [*c]const u8 {
    const result_handle = toHandle(ResultHandle, result) orelse return null;

    if (index >= result_handle.result.columns.len) return null;

    return result_handle.result.columns[index].ptr;
}

/// Get column value at current row
pub export fn lattice_result_get(
    result: ?*lattice_result,
    index: u32,
    value_out: *lattice_value,
) lattice_error {
    const result_handle = toHandle(ResultHandle, result) orelse return .err_invalid_arg;

    if (!result_handle.started) return .err_invalid_arg;
    if (result_handle.current_row >= result_handle.result.rows.len) return .err_not_found;

    const row = result_handle.result.rows[result_handle.current_row];
    if (index >= row.values.len) return .err_invalid_arg;

    resultValueToCValue(row.values[index], value_out);
    return .ok;
}

/// Free a result set
pub export fn lattice_result_free(result: ?*lattice_result) void {
    const result_handle = toHandle(ResultHandle, result) orelse return;

    result_handle.result.deinit();
    global_allocator.destroy(result_handle);
}

// ============================================================================
// Utility Functions
// ============================================================================

/// Get version string
pub export fn lattice_version() [*c]const u8 {
    return "0.1.0";
}

/// Get error message for error code
pub export fn lattice_error_message(code: lattice_error) [*c]const u8 {
    return switch (code) {
        .ok => "Success",
        .err => "Generic error",
        .err_io => "I/O error",
        .err_corruption => "Database corruption detected",
        .err_not_found => "Not found",
        .err_already_exists => "Already exists",
        .err_invalid_arg => "Invalid argument",
        .err_txn_aborted => "Transaction aborted",
        .err_lock_timeout => "Lock timeout",
        .err_read_only => "Database is read-only",
        .err_full => "Database or buffer pool full",
        .err_version_mismatch => "Version mismatch",
        .err_checksum => "Checksum error",
        .err_out_of_memory => "Out of memory",
    };
}

// ============================================================================
// Tests
// ============================================================================

test "error code values match header" {
    try std.testing.expectEqual(@as(c_int, 0), @intFromEnum(lattice_error.ok));
    try std.testing.expectEqual(@as(c_int, -1), @intFromEnum(lattice_error.err));
    try std.testing.expectEqual(@as(c_int, -13), @intFromEnum(lattice_error.err_out_of_memory));
}

test "value type tags match header" {
    try std.testing.expectEqual(@as(c_int, 0), @intFromEnum(lattice_value_type.null));
    try std.testing.expectEqual(@as(c_int, 1), @intFromEnum(lattice_value_type.bool));
    try std.testing.expectEqual(@as(c_int, 4), @intFromEnum(lattice_value_type.string));
}

test "version returns expected string" {
    const version = lattice_version();
    try std.testing.expect(version != null);
    try std.testing.expectEqualStrings("0.1.0", std.mem.sliceTo(version, 0));
}

test "error message returns valid strings" {
    const msg = lattice_error_message(.ok);
    try std.testing.expect(msg != null);
    try std.testing.expectEqualStrings("Success", std.mem.sliceTo(msg, 0));
}

test "null handle returns error" {
    try std.testing.expectEqual(lattice_error.err_invalid_arg, lattice_close(null));
}

test "c string conversion" {
    const result = cStrToSlice("hello");
    try std.testing.expect(result != null);
    try std.testing.expectEqualStrings("hello", result.?);

    const null_result = cStrToSlice(null);
    try std.testing.expect(null_result == null);
}
