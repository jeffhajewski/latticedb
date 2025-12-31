//! C API bindings for Lattice database.
//!
//! Provides the stable C ABI interface for language bindings.

const std = @import("std");
const lattice = @import("lattice");

const types = lattice.core.types;
const transaction = lattice.transaction.manager;

/// Opaque database handle
pub const Database = opaque {};

/// Opaque transaction handle
pub const Txn = opaque {};

/// Opaque query handle
pub const Query = opaque {};

/// Opaque result set handle
pub const ResultSet = opaque {};

/// Node ID type for C API
pub const NodeId = types.NodeId;

/// Edge ID type for C API
pub const EdgeId = types.EdgeId;

/// Error codes
pub const ErrorCode = enum(c_int) {
    ok = 0,
    error_generic = -1,
    error_io = -2,
    error_corruption = -3,
    error_not_found = -4,
    error_already_exists = -5,
    error_invalid_arg = -6,
    error_txn_aborted = -7,
    error_lock_timeout = -8,
    error_read_only = -9,
    error_full = -10,
    error_version_mismatch = -11,
    error_checksum = -12,
    error_out_of_memory = -13,
};

/// Transaction mode for C API
pub const TxnMode = enum(c_int) {
    read_only = 0,
    read_write = 1,
};

/// Property value type tags
pub const ValueType = enum(c_int) {
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
pub const Value = extern struct {
    value_type: ValueType,
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

/// Open options
pub const OpenOptions = extern struct {
    create: bool = false,
    read_only: bool = false,
    cache_size_mb: u32 = 100,
    page_size: u32 = 4096,
};

// C API function exports (stubs for now)

/// Open a database file
pub export fn lattice_open(path: [*c]const u8, options: *const OpenOptions, db_out: *?*Database) ErrorCode {
    _ = path;
    _ = options;
    _ = db_out;
    return .ok;
}

/// Close a database
pub export fn lattice_close(db: *Database) ErrorCode {
    _ = db;
    return .ok;
}

/// Begin a transaction
pub export fn lattice_begin(db: *Database, mode: TxnMode, txn_out: *?*Txn) ErrorCode {
    _ = db;
    _ = mode;
    _ = txn_out;
    return .ok;
}

/// Commit a transaction
pub export fn lattice_commit(txn: *Txn) ErrorCode {
    _ = txn;
    return .ok;
}

/// Rollback a transaction
pub export fn lattice_rollback(txn: *Txn) ErrorCode {
    _ = txn;
    return .ok;
}

/// Create a node
pub export fn lattice_node_create(txn: *Txn, label: [*c]const u8, node_out: *NodeId) ErrorCode {
    _ = txn;
    _ = label;
    _ = node_out;
    return .ok;
}

/// Delete a node
pub export fn lattice_node_delete(txn: *Txn, node_id: NodeId) ErrorCode {
    _ = txn;
    _ = node_id;
    return .ok;
}

/// Set a property on a node
pub export fn lattice_node_set_property(txn: *Txn, node_id: NodeId, key: [*c]const u8, value: *const Value) ErrorCode {
    _ = txn;
    _ = node_id;
    _ = key;
    _ = value;
    return .ok;
}

/// Get a property from a node
pub export fn lattice_node_get_property(txn: *Txn, node_id: NodeId, key: [*c]const u8, value_out: *Value) ErrorCode {
    _ = txn;
    _ = node_id;
    _ = key;
    _ = value_out;
    return .error_not_found;
}

/// Set a vector on a node
pub export fn lattice_node_set_vector(txn: *Txn, node_id: NodeId, key: [*c]const u8, vector: [*c]const f32, dimensions: u32) ErrorCode {
    _ = txn;
    _ = node_id;
    _ = key;
    _ = vector;
    _ = dimensions;
    return .ok;
}

/// Create an edge
pub export fn lattice_edge_create(txn: *Txn, source: NodeId, target: NodeId, edge_type: [*c]const u8, edge_out: *EdgeId) ErrorCode {
    _ = txn;
    _ = source;
    _ = target;
    _ = edge_type;
    _ = edge_out;
    return .ok;
}

/// Prepare a query
pub export fn lattice_query_prepare(db: *Database, cypher: [*c]const u8, query_out: *?*Query) ErrorCode {
    _ = db;
    _ = cypher;
    _ = query_out;
    return .ok;
}

/// Execute a prepared query
pub export fn lattice_query_execute(query: *Query, txn: *Txn, result_out: *?*ResultSet) ErrorCode {
    _ = query;
    _ = txn;
    _ = result_out;
    return .ok;
}

/// Free a result set
pub export fn lattice_result_free(result: *ResultSet) void {
    _ = result;
}

/// Free a query
pub export fn lattice_query_free(query: *Query) void {
    _ = query;
}

/// Get version string
pub export fn lattice_version() [*c]const u8 {
    return "0.1.0";
}

test "error code values" {
    try std.testing.expectEqual(@as(c_int, 0), @intFromEnum(ErrorCode.ok));
    try std.testing.expectEqual(@as(c_int, -1), @intFromEnum(ErrorCode.error_generic));
}
