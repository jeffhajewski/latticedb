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
const VectorSearchResult = database.VectorSearchResult;
const FtsSearchResult = database.FtsSearchResult;
const node_mod = lattice.graph.node;
const txn_mod = lattice.transaction.manager;
const Transaction = txn_mod.Transaction;
const TxnMode = txn_mod.TxnMode;

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

/// Internal transaction handle wrapping actual Transaction
const TxnHandle = struct {
    db_handle: *DatabaseHandle,
    txn: Transaction, // Actual Transaction struct from TxnManager
};

/// Internal query handle storing prepared query state
const QueryHandle = struct {
    cypher: []const u8,
    cypher_owned: bool,
    db_handle: *DatabaseHandle,
    /// Bound parameters (name -> value)
    parameters: std.StringHashMap(PropertyValue),

    fn init(cypher: []const u8, cypher_owned: bool, db_handle: *DatabaseHandle) QueryHandle {
        return .{
            .cypher = cypher,
            .cypher_owned = cypher_owned,
            .db_handle = db_handle,
            .parameters = std.StringHashMap(PropertyValue).init(global_allocator),
        };
    }

    fn deinit(self: *QueryHandle) void {
        // Free parameter keys and values (we own copies of them)
        var iter = self.parameters.iterator();
        while (iter.next()) |entry| {
            global_allocator.free(entry.key_ptr.*);
            // Free vector data if this is a vector parameter
            switch (entry.value_ptr.*) {
                .vector_val => |v| global_allocator.free(v),
                else => {},
            }
        }
        self.parameters.deinit();
    }
};

/// Internal result handle wrapping query results
const ResultHandle = struct {
    result: QueryResult,
    current_row: usize,
    started: bool,
};

/// Internal vector search result handle
const VectorResultHandle = struct {
    results: []VectorSearchResult,
    count: usize,
};

/// Internal FTS search result handle
const FtsResultHandle = struct {
    results: []FtsSearchResult,
    count: usize,
    db_handle: *DatabaseHandle,
};

/// Internal edge result handle for edge traversal
const EdgeResultHandle = struct {
    edges: []Database.EdgeInfo,
    count: usize,
    db_handle: *DatabaseHandle,
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

/// Opaque vector search result handle for C API
pub const lattice_vector_result = opaque {};

/// Opaque FTS search result handle for C API
pub const lattice_fts_result = opaque {};

/// Opaque edge result handle for C API
pub const lattice_edge_result = opaque {};

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
        DatabaseError.TransactionNotActive => .err_invalid_arg,
        DatabaseError.TransactionReadOnly => .err_read_only,
        DatabaseError.TransactionsNotEnabled => .err_invalid_arg,
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
    vector = 6,
    list = 7,
    map = 8,
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
        vector_val: extern struct {
            ptr: [*c]const f32,
            dimensions: u32,
        },
    },
};

/// Open options for C API
pub const lattice_open_options = extern struct {
    create: bool = false,
    read_only: bool = false,
    cache_size_mb: u32 = 100,
    page_size: u32 = 4096,
    enable_vector: bool = false,
    vector_dimensions: u16 = 128,
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
        .vector_val => |v| {
            c_val.value_type = .vector;
            c_val.data.vector_val.ptr = v.ptr;
            c_val.data.vector_val.dimensions = @intCast(v.len);
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
        .edge_id => |id| {
            c_val.value_type = .int;
            c_val.data.int_val = @intCast(id);
        },
        .bytes_val => |b| {
            c_val.value_type = .bytes;
            c_val.data.bytes_val.ptr = b.ptr;
            c_val.data.bytes_val.len = b.len;
        },
        .vector_val => |v| {
            c_val.value_type = .vector;
            c_val.data.vector_val.ptr = v.ptr;
            c_val.data.vector_val.dimensions = @intCast(v.len);
        },
        .list_val, .map_val => {
            // Complex nested types require separate iteration API
            // For now, mark as null in C API
            c_val.value_type = .null;
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
        .vector => .{ .vector_val = c_val.data.vector_val.ptr[0..c_val.data.vector_val.dimensions] },
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
        zig_options.config.enable_vector = opts.enable_vector;
        zig_options.config.vector_dimensions = opts.vector_dimensions;
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

    // Sync first to ensure durability, capture any errors
    const sync_result = handle.db.sync();

    // Always close and free resources
    handle.db.close();
    global_allocator.destroy(handle);

    // Return sync error if there was one
    if (sync_result) |_| {
        return .ok;
    } else |_| {
        return .err_io;
    }
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

    // Map C API mode to TxnMode
    const txn_mode: TxnMode = if (mode == .read_only) .read_only else .read_write;

    // Actually begin a transaction in the database
    const txn = db_handle.db.beginTransaction(txn_mode) catch |err| {
        return switch (err) {
            DatabaseError.TransactionsNotEnabled => {
                // Fallback: create handle without transaction for non-WAL databases
                const txn_handle = global_allocator.create(TxnHandle) catch return .err_out_of_memory;
                txn_handle.* = .{
                    .db_handle = db_handle,
                    .txn = Transaction{
                        .id = 0, // Sentinel for "no real txn"
                        .state = .active,
                        .mode = txn_mode,
                        .isolation = .snapshot,
                        .start_ts = 0,
                        .commit_ts = 0,
                    },
                };
                txn_out.* = @ptrCast(txn_handle);
                return .ok;
            },
            DatabaseError.OutOfMemory => .err_out_of_memory,
            else => .err,
        };
    };

    // Create transaction handle with real transaction
    const txn_handle = global_allocator.create(TxnHandle) catch return .err_out_of_memory;
    txn_handle.* = .{
        .db_handle = db_handle,
        .txn = txn,
    };

    txn_out.* = @ptrCast(txn_handle);
    return .ok;
}

/// Commit a transaction
pub export fn lattice_commit(txn: ?*lattice_txn) lattice_error {
    const txn_handle = toHandle(TxnHandle, txn) orelse return .err_invalid_arg;

    // Only commit if this is a real transaction (id != 0)
    if (txn_handle.txn.id != 0) {
        txn_handle.db_handle.db.commitTransaction(&txn_handle.txn) catch |err| {
            // Don't destroy handle on error - let caller retry or rollback
            return switch (err) {
                DatabaseError.TransactionNotActive => .err_txn_aborted,
                DatabaseError.IoError => .err_io,
                else => .err,
            };
        };
    }

    global_allocator.destroy(txn_handle);
    return .ok;
}

/// Rollback a transaction
pub export fn lattice_rollback(txn: ?*lattice_txn) lattice_error {
    const txn_handle = toHandle(TxnHandle, txn) orelse return .err_invalid_arg;

    // Only abort if this is a real transaction (id != 0)
    if (txn_handle.txn.id != 0) {
        txn_handle.db_handle.db.abortTransaction(&txn_handle.txn) catch |err| {
            // Still destroy handle even on error - transaction is unusable
            global_allocator.destroy(txn_handle);
            return switch (err) {
                DatabaseError.TransactionNotActive => .err_txn_aborted,
                else => .err,
            };
        };
    }

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

    if (txn_handle.txn.mode == .read_only) return .err_read_only;

    const label_slice = cStrToSlice(label) orelse return .err_invalid_arg;
    const labels = [_][]const u8{label_slice};

    // Pass transaction if it's a real one (id != 0)
    const txn_ptr: ?*Transaction = if (txn_handle.txn.id != 0) &txn_handle.txn else null;
    const node_id = txn_handle.db_handle.db.createNode(txn_ptr, &labels) catch |err| {
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

    if (txn_handle.txn.mode == .read_only) return .err_read_only;

    // Pass transaction if it's a real one (id != 0)
    const txn_ptr: ?*Transaction = if (txn_handle.txn.id != 0) &txn_handle.txn else null;
    txn_handle.db_handle.db.deleteNode(txn_ptr, node_id) catch |err| {
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

    if (txn_handle.txn.mode == .read_only) return .err_read_only;

    const key_slice = cStrToSlice(key) orelse return .err_invalid_arg;
    const c_val = value orelse return .err_invalid_arg;

    // Convert C value to Zig PropertyValue
    const zig_value = cValueToZigValue(c_val);

    // Pass transaction if it's a real one (id != 0)
    const txn_ptr: ?*Transaction = if (txn_handle.txn.id != 0) &txn_handle.txn else null;
    txn_handle.db_handle.db.setNodeProperty(txn_ptr, node_id, key_slice, zig_value) catch |err| {
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

/// Check if a node exists
pub export fn lattice_node_exists(
    txn: ?*lattice_txn,
    node_id: lattice_node_id,
    exists_out: *bool,
) lattice_error {
    const txn_handle = toHandle(TxnHandle, txn) orelse return .err_invalid_arg;

    exists_out.* = txn_handle.db_handle.db.node_store.exists(node_id) catch |err| {
        return switch (err) {
            node_mod.NodeError.BufferPoolFull => .err_full,
            else => .err_io,
        };
    };
    return .ok;
}

/// Get labels for a node as comma-separated string
pub export fn lattice_node_get_labels(
    txn: ?*lattice_txn,
    node_id: lattice_node_id,
    labels_out: *[*c]u8,
) lattice_error {
    const txn_handle = toHandle(TxnHandle, txn) orelse return .err_invalid_arg;

    // Get the node from storage
    var node = txn_handle.db_handle.db.node_store.get(node_id) catch |err| {
        return switch (err) {
            node_mod.NodeError.NotFound => .err_not_found,
            else => .err,
        };
    };
    defer node.deinit(txn_handle.db_handle.db.allocator);

    // Build comma-separated label string
    var total_len: usize = 0;
    for (node.labels, 0..) |label_id, i| {
        if (i > 0) total_len += 1; // comma
        const label_str = txn_handle.db_handle.db.symbol_table.resolve(label_id) catch {
            continue;
        };
        total_len += label_str.len;
    }

    // Allocate result string (plus null terminator)
    const result = global_allocator.alloc(u8, total_len + 1) catch {
        return .err_out_of_memory;
    };

    // Fill the string
    var pos: usize = 0;
    for (node.labels, 0..) |label_id, i| {
        if (i > 0) {
            result[pos] = ',';
            pos += 1;
        }
        const label_str = txn_handle.db_handle.db.symbol_table.resolve(label_id) catch {
            continue;
        };
        @memcpy(result[pos..][0..label_str.len], label_str);
        pos += label_str.len;
    }
    result[pos] = 0; // null terminator

    labels_out.* = result.ptr;
    return .ok;
}

/// Free a string allocated by lattice
pub export fn lattice_free_string(str: [*c]u8) void {
    if (str == null) return;

    // We need to find the length to free the correct slice
    // Since we always null-terminate, find the length
    var len: usize = 0;
    while (str[len] != 0) : (len += 1) {}

    const slice = str[0 .. len + 1];
    global_allocator.free(slice);
}

/// Set a vector on a node
pub export fn lattice_node_set_vector(
    txn: ?*lattice_txn,
    node_id: lattice_node_id,
    key: [*c]const u8,
    vector: [*c]const f32,
    dimensions: u32,
) lattice_error {
    const txn_handle = toHandle(TxnHandle, txn) orelse return .err_invalid_arg;

    if (txn_handle.txn.mode == .read_only) return .err_read_only;

    // Key is currently ignored - vectors are stored by node_id
    // In future, we could support multiple vectors per node with different keys
    _ = key;

    if (vector == null or dimensions == 0) return .err_invalid_arg;

    // Convert C pointer to Zig slice
    const vector_slice = vector[0..dimensions];

    txn_handle.db_handle.db.setNodeVector(node_id, vector_slice) catch |err| {
        return mapDatabaseError(err);
    };

    return .ok;
}

// ============================================================================
// Batch Insert Operations
// ============================================================================

/// Node spec for batch insert: label + vector
pub const lattice_node_with_vector = extern struct {
    label: [*c]const u8,
    vector: [*c]const f32,
    dimensions: u32,
};

/// Create multiple nodes with vectors in a single call.
/// On success, node_ids_out[0..count_out] contains created node IDs.
/// On partial failure, count_out indicates how many succeeded.
pub export fn lattice_batch_insert(
    txn: ?*lattice_txn,
    nodes: [*c]const lattice_node_with_vector,
    count: u32,
    node_ids_out: [*c]lattice_node_id,
    count_out: *u32,
) lattice_error {
    count_out.* = 0;

    const txn_handle = toHandle(TxnHandle, txn) orelse return .err_invalid_arg;
    if (txn_handle.txn.mode == .read_only) return .err_read_only;
    if (nodes == null or count == 0 or node_ids_out == null) return .err_invalid_arg;

    const txn_ptr: ?*Transaction = if (txn_handle.txn.id != 0) &txn_handle.txn else null;

    var i: u32 = 0;
    while (i < count) : (i += 1) {
        const spec = nodes[i];

        // Validate
        const label_slice = cStrToSlice(spec.label) orelse return .err_invalid_arg;
        if (spec.vector == null or spec.dimensions == 0) return .err_invalid_arg;

        // Create node
        const labels = [_][]const u8{label_slice};
        const node_id = txn_handle.db_handle.db.createNode(txn_ptr, &labels) catch |err| {
            return mapAnyError(err);
        };

        // Set vector
        const vector_slice = spec.vector[0..spec.dimensions];
        txn_handle.db_handle.db.setNodeVector(node_id, vector_slice) catch |err| {
            return mapDatabaseError(err);
        };

        node_ids_out[i] = node_id;
        count_out.* = i + 1;
    }

    return .ok;
}

/// Search for similar vectors using HNSW index.
/// Returns a vector result handle containing node IDs and distances.
pub export fn lattice_vector_search(
    db: ?*lattice_database,
    vector: [*c]const f32,
    dimensions: u32,
    k: u32,
    ef_search: u16,
    result_out: *?*lattice_vector_result,
) lattice_error {
    const db_handle = toHandle(DatabaseHandle, db) orelse return .err_invalid_arg;

    if (vector == null or dimensions == 0 or k == 0) return .err_invalid_arg;

    // Convert C pointer to Zig slice
    const query_vector = vector[0..dimensions];

    // Perform the search
    const ef = if (ef_search == 0) null else ef_search;
    const results = db_handle.db.vectorSearch(query_vector, k, ef) catch |err| {
        return mapDatabaseError(err);
    };

    // Create result handle
    const result_handle = global_allocator.create(VectorResultHandle) catch return .err_out_of_memory;
    result_handle.* = VectorResultHandle{
        .results = results,
        .count = results.len,
    };

    result_out.* = toOpaque(lattice_vector_result, result_handle);
    return .ok;
}

/// Get the number of results in a vector search result set.
pub export fn lattice_vector_result_count(
    result: ?*lattice_vector_result,
) u32 {
    const result_handle = toHandle(VectorResultHandle, result) orelse return 0;
    return @intCast(result_handle.count);
}

/// Get a result from a vector search result set.
/// Returns the node ID and distance at the given index.
pub export fn lattice_vector_result_get(
    result: ?*lattice_vector_result,
    index: u32,
    node_id_out: *lattice_node_id,
    distance_out: *f32,
) lattice_error {
    const result_handle = toHandle(VectorResultHandle, result) orelse return .err_invalid_arg;

    if (index >= result_handle.count) return .err_invalid_arg;

    node_id_out.* = result_handle.results[index].node_id;
    distance_out.* = result_handle.results[index].distance;
    return .ok;
}

/// Free a vector search result set.
pub export fn lattice_vector_result_free(
    result: ?*lattice_vector_result,
) void {
    const result_handle = toHandle(VectorResultHandle, result) orelse return;

    // Free the results slice (allocated by Database.vectorSearch)
    global_allocator.free(result_handle.results);

    // Free the handle itself
    global_allocator.destroy(result_handle);
}

// ============================================================================
// Full-Text Search Operations
// ============================================================================

/// Index a text document for full-text search.
pub export fn lattice_fts_index(
    txn: ?*lattice_txn,
    node_id: lattice_node_id,
    text: [*c]const u8,
    text_len: usize,
) lattice_error {
    const txn_handle = toHandle(TxnHandle, txn) orelse return .err_invalid_arg;

    if (txn_handle.txn.mode == .read_only) return .err_read_only;
    if (text == null or text_len == 0) return .err_invalid_arg;

    const text_slice = text[0..text_len];

    txn_handle.db_handle.db.ftsIndexDocument(node_id, text_slice) catch |err| {
        return mapDatabaseError(err);
    };

    return .ok;
}

/// Search for documents matching a text query using BM25 scoring.
pub export fn lattice_fts_search(
    db: ?*lattice_database,
    query_text: [*c]const u8,
    query_len: usize,
    limit: u32,
    result_out: *?*lattice_fts_result,
) lattice_error {
    const db_handle = toHandle(DatabaseHandle, db) orelse return .err_invalid_arg;

    if (query_text == null or query_len == 0 or limit == 0) return .err_invalid_arg;

    const query_slice = query_text[0..query_len];

    // Perform the search
    const results = db_handle.db.ftsSearch(query_slice, limit) catch |err| {
        return mapDatabaseError(err);
    };

    // Create result handle
    const result_handle = global_allocator.create(FtsResultHandle) catch return .err_out_of_memory;
    result_handle.* = FtsResultHandle{
        .results = results,
        .count = results.len,
        .db_handle = db_handle,
    };

    result_out.* = toOpaque(lattice_fts_result, result_handle);
    return .ok;
}

/// Get the number of FTS search results.
pub export fn lattice_fts_result_count(
    result: ?*lattice_fts_result,
) u32 {
    const result_handle = toHandle(FtsResultHandle, result) orelse return 0;
    return @intCast(result_handle.count);
}

/// Get a result from an FTS search result set.
pub export fn lattice_fts_result_get(
    result: ?*lattice_fts_result,
    index: u32,
    node_id_out: *lattice_node_id,
    score_out: *f32,
) lattice_error {
    const result_handle = toHandle(FtsResultHandle, result) orelse return .err_invalid_arg;

    if (index >= result_handle.count) return .err_invalid_arg;

    node_id_out.* = result_handle.results[index].doc_id;
    score_out.* = result_handle.results[index].score;
    return .ok;
}

/// Free an FTS search result set.
pub export fn lattice_fts_result_free(
    result: ?*lattice_fts_result,
) void {
    const result_handle = toHandle(FtsResultHandle, result) orelse return;

    // Free the results through database (uses FTS index's allocator)
    result_handle.db_handle.db.freeFtsSearchResults(result_handle.results);

    // Free the handle itself
    global_allocator.destroy(result_handle);
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

    if (txn_handle.txn.mode == .read_only) return .err_read_only;

    const type_slice = cStrToSlice(edge_type) orelse return .err_invalid_arg;

    // Pass transaction if it's a real one (id != 0)
    const txn_ptr: ?*Transaction = if (txn_handle.txn.id != 0) &txn_handle.txn else null;
    txn_handle.db_handle.db.createEdge(txn_ptr, source, target, type_slice) catch |err| {
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

    if (txn_handle.txn.mode == .read_only) return .err_read_only;

    const type_slice = cStrToSlice(edge_type) orelse return .err_invalid_arg;

    // Pass transaction if it's a real one (id != 0)
    const txn_ptr: ?*Transaction = if (txn_handle.txn.id != 0) &txn_handle.txn else null;
    txn_handle.db_handle.db.deleteEdge(txn_ptr, source, target, type_slice) catch |err| {
        return mapDatabaseError(err);
    };

    return .ok;
}

/// Get all outgoing edges from a node
pub export fn lattice_edge_get_outgoing(
    txn: ?*lattice_txn,
    node_id: lattice_node_id,
    result_out: *?*lattice_edge_result,
) lattice_error {
    result_out.* = null;

    const txn_handle = toHandle(TxnHandle, txn) orelse return .err_invalid_arg;

    const edges = txn_handle.db_handle.db.getOutgoingEdges(node_id) catch |err| {
        return mapAnyError(err);
    };

    const result_handle = global_allocator.create(EdgeResultHandle) catch {
        txn_handle.db_handle.db.freeEdgeInfos(edges);
        return .err_out_of_memory;
    };

    result_handle.* = EdgeResultHandle{
        .edges = edges,
        .count = edges.len,
        .db_handle = txn_handle.db_handle,
    };

    result_out.* = toOpaque(lattice_edge_result, result_handle);
    return .ok;
}

/// Get all incoming edges to a node
pub export fn lattice_edge_get_incoming(
    txn: ?*lattice_txn,
    node_id: lattice_node_id,
    result_out: *?*lattice_edge_result,
) lattice_error {
    result_out.* = null;

    const txn_handle = toHandle(TxnHandle, txn) orelse return .err_invalid_arg;

    const edges = txn_handle.db_handle.db.getIncomingEdges(node_id) catch |err| {
        return mapAnyError(err);
    };

    const result_handle = global_allocator.create(EdgeResultHandle) catch {
        txn_handle.db_handle.db.freeEdgeInfos(edges);
        return .err_out_of_memory;
    };

    result_handle.* = EdgeResultHandle{
        .edges = edges,
        .count = edges.len,
        .db_handle = txn_handle.db_handle,
    };

    result_out.* = toOpaque(lattice_edge_result, result_handle);
    return .ok;
}

/// Get the number of edges in an edge result set
pub export fn lattice_edge_result_count(result: ?*lattice_edge_result) u32 {
    const result_handle = toHandle(EdgeResultHandle, result) orelse return 0;
    return @intCast(result_handle.count);
}

/// Get an edge from an edge result set by index
pub export fn lattice_edge_result_get(
    result: ?*lattice_edge_result,
    index: u32,
    source_out: *lattice_node_id,
    target_out: *lattice_node_id,
    edge_type_out: *[*c]const u8,
    edge_type_len_out: *c_uint,
) lattice_error {
    const result_handle = toHandle(EdgeResultHandle, result) orelse return .err_invalid_arg;

    if (index >= result_handle.count) return .err_invalid_arg;

    const edge = result_handle.edges[index];
    source_out.* = edge.source;
    target_out.* = edge.target;
    edge_type_out.* = edge.edge_type.ptr;
    edge_type_len_out.* = @intCast(edge.edge_type.len);

    return .ok;
}

/// Free an edge result set
pub export fn lattice_edge_result_free(result: ?*lattice_edge_result) void {
    const result_handle = toHandle(EdgeResultHandle, result) orelse return;

    // Free the edge infos through database
    result_handle.db_handle.db.freeEdgeInfos(result_handle.edges);

    // Free the handle itself
    global_allocator.destroy(result_handle);
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

    query_handle.* = QueryHandle.init(cypher_copy, true, db_handle);

    query_out.* = @ptrCast(query_handle);
    return .ok;
}

/// Bind a parameter to a query
pub export fn lattice_query_bind(
    query: ?*lattice_query,
    name: [*c]const u8,
    value: ?*const lattice_value,
) lattice_error {
    const query_handle = toHandle(QueryHandle, query) orelse return .err_invalid_arg;
    const name_slice = cStrToSlice(name) orelse return .err_invalid_arg;
    const c_value = value orelse return .err_invalid_arg;

    // Convert C value to Zig PropertyValue
    const zig_value = cValueToZigValue(c_value);

    // Copy the name since we need to own it
    const name_copy = global_allocator.dupe(u8, name_slice) catch return .err_out_of_memory;

    // Store in parameters map (may overwrite existing)
    query_handle.parameters.put(name_copy, zig_value) catch {
        global_allocator.free(name_copy);
        return .err_out_of_memory;
    };

    return .ok;
}

/// Bind a vector parameter to a prepared query
pub export fn lattice_query_bind_vector(
    query: ?*lattice_query,
    name: [*c]const u8,
    vector: [*c]const f32,
    dimensions: u32,
) lattice_error {
    const query_handle = toHandle(QueryHandle, query) orelse return .err_invalid_arg;
    const name_slice = cStrToSlice(name) orelse return .err_invalid_arg;

    if (vector == null or dimensions == 0) return .err_invalid_arg;

    // Copy the vector data since we need to own it
    const vector_copy = global_allocator.alloc(f32, dimensions) catch return .err_out_of_memory;
    @memcpy(vector_copy, vector[0..dimensions]);

    // Create PropertyValue with vector
    const zig_value = PropertyValue{ .vector_val = vector_copy };

    // Copy the name since we need to own it
    const name_copy = global_allocator.dupe(u8, name_slice) catch {
        global_allocator.free(vector_copy);
        return .err_out_of_memory;
    };

    // Store in parameters map (may overwrite existing)
    query_handle.parameters.put(name_copy, zig_value) catch {
        global_allocator.free(name_copy);
        global_allocator.free(vector_copy);
        return .err_out_of_memory;
    };

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

    // Execute the query (with or without parameters)
    const result = if (query_handle.parameters.count() > 0)
        query_handle.db_handle.db.queryWithParams(query_handle.cypher, &query_handle.parameters) catch |err| {
            return mapQueryError(err);
        }
    else
        query_handle.db_handle.db.query(query_handle.cypher) catch |err| {
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

    // Clean up parameters
    query_handle.deinit();

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

// ============================================================================
// Query Cache Operations
// ============================================================================

/// Clear the query cache
pub export fn lattice_query_cache_clear(db: ?*lattice_database) lattice_error {
    const db_handle = toHandle(DatabaseHandle, db) orelse return .err_invalid_arg;
    db_handle.db.clearQueryCache();
    return .ok;
}

/// Get query cache statistics
pub export fn lattice_query_cache_stats(
    db: ?*lattice_database,
    entries_out: ?*u32,
    hits_out: ?*u64,
    misses_out: ?*u64,
) lattice_error {
    const db_handle = toHandle(DatabaseHandle, db) orelse return .err_invalid_arg;

    const stats = db_handle.db.queryCacheStats();

    if (entries_out) |p| p.* = stats.entries;
    if (hits_out) |p| p.* = stats.hits;
    if (misses_out) |p| p.* = stats.misses;

    return .ok;
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
    try std.testing.expectEqual(@as(c_int, 6), @intFromEnum(lattice_value_type.vector));
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

test "vector value conversion" {
    // Test Zig to C conversion
    const zig_vector = [_]f32{ 1.0, 2.0, 3.0 };
    const zig_val = PropertyValue{ .vector_val = &zig_vector };

    var c_val: lattice_value = undefined;
    zigValueToCValue(zig_val, &c_val);

    try std.testing.expectEqual(lattice_value_type.vector, c_val.value_type);
    try std.testing.expectEqual(@as(u32, 3), c_val.data.vector_val.dimensions);
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), c_val.data.vector_val.ptr[0], 0.001);
    try std.testing.expectApproxEqAbs(@as(f32, 2.0), c_val.data.vector_val.ptr[1], 0.001);
    try std.testing.expectApproxEqAbs(@as(f32, 3.0), c_val.data.vector_val.ptr[2], 0.001);

    // Test C to Zig conversion
    const back_to_zig = cValueToZigValue(&c_val);
    try std.testing.expectEqual(@as(usize, 3), back_to_zig.vector_val.len);
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), back_to_zig.vector_val[0], 0.001);
}

test "vector parameter binding validation" {
    // Test that null vector returns error
    try std.testing.expectEqual(lattice_error.err_invalid_arg, lattice_query_bind_vector(null, "query", null, 128));

    // Test that zero dimensions returns error (when query handle is null)
    const dummy_vec = [_]f32{1.0};
    try std.testing.expectEqual(lattice_error.err_invalid_arg, lattice_query_bind_vector(null, "query", &dummy_vec, 0));
}
