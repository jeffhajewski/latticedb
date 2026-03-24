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
const QueryFailure = database.QueryFailure;
const QueryFailureStage = database.QueryFailureStage;
const OpenOptions = database.OpenOptions;
const DatabaseConfig = database.DatabaseConfig;
const VectorSearchResult = database.VectorSearchResult;
const FtsSearchResult = database.FtsSearchResult;
const hash_embed_mod = lattice.vector.hash_embed;
const embedding_mod = lattice.vector.embedding;
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
    last_error_stage: lattice_query_error_stage,
    last_error_message: ?[:0]u8,
    last_error_code: ?[:0]u8,
    last_error_has_location: bool,
    last_error_line: u32,
    last_error_column: u32,
    last_error_length: u32,

    fn init(cypher: []const u8, cypher_owned: bool, db_handle: *DatabaseHandle) QueryHandle {
        return .{
            .cypher = cypher,
            .cypher_owned = cypher_owned,
            .db_handle = db_handle,
            .parameters = std.StringHashMap(PropertyValue).init(global_allocator),
            .last_error_stage = .none,
            .last_error_message = null,
            .last_error_code = null,
            .last_error_has_location = false,
            .last_error_line = 0,
            .last_error_column = 0,
            .last_error_length = 0,
        };
    }

    fn clearLastError(self: *QueryHandle) void {
        if (self.last_error_message) |msg| {
            global_allocator.free(msg);
        }
        if (self.last_error_code) |code| {
            global_allocator.free(code);
        }

        self.last_error_stage = .none;
        self.last_error_message = null;
        self.last_error_code = null;
        self.last_error_has_location = false;
        self.last_error_line = 0;
        self.last_error_column = 0;
        self.last_error_length = 0;
    }

    fn setLastError(self: *QueryHandle, failure: QueryFailure) lattice_error {
        self.clearLastError();
        self.last_error_stage = mapQueryFailureStage(failure.stage);
        self.last_error_message = global_allocator.dupeZ(u8, failure.message) catch return .err_out_of_memory;

        if (failure.code) |code| {
            self.last_error_code = global_allocator.dupeZ(u8, code) catch {
                self.clearLastError();
                return .err_out_of_memory;
            };
        }

        if (failure.location) |loc| {
            self.last_error_has_location = true;
            self.last_error_line = loc.line;
            self.last_error_column = loc.column;
            self.last_error_length = loc.length;
        }

        return .ok;
    }

    fn storeOwnedParameter(self: *QueryHandle, name: []const u8, value: PropertyValue) lattice_error {
        const gop = self.parameters.getOrPut(name) catch {
            var owned_value = value;
            owned_value.deinit(global_allocator);
            return .err_out_of_memory;
        };

        if (gop.found_existing) {
            var old_value = gop.value_ptr.*;
            old_value.deinit(global_allocator);
            gop.value_ptr.* = value;
            return .ok;
        }

        const name_copy = global_allocator.dupe(u8, name) catch {
            _ = self.parameters.remove(name);
            var owned_value = value;
            owned_value.deinit(global_allocator);
            return .err_out_of_memory;
        };

        gop.key_ptr.* = name_copy;
        gop.value_ptr.* = value;
        return .ok;
    }

    fn deinit(self: *QueryHandle) void {
        self.clearLastError();

        // Free parameter keys and values (we own copies of them)
        var iter = self.parameters.iterator();
        while (iter.next()) |entry| {
            global_allocator.free(entry.key_ptr.*);
            var value = entry.value_ptr.*;
            value.deinit(global_allocator);
        }
        self.parameters.deinit();
    }
};

/// Internal result handle wrapping query results
const ResultHandle = struct {
    result: QueryResult,
    column_names_z: []const [:0]u8,
    current_row: usize,
    started: bool,
    borrowed_value_arena: std.heap.ArenaAllocator,
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

/// Internal embedding client handle
const EmbeddingClientHandle = struct {
    client: embedding_mod.EmbeddingClient,
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

/// Opaque embedding client handle for C API
pub const lattice_embedding_client = opaque {};

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
    err_unsupported = -14,
};

/// Query diagnostic stage for prepared query execution failures.
pub const lattice_query_error_stage = enum(c_int) {
    none = 0,
    parse = 1,
    semantic = 2,
    plan = 3,
    execution = 4,
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

fn mapQueryFailureStage(stage: QueryFailureStage) lattice_query_error_stage {
    return switch (stage) {
        .parse => .parse,
        .semantic => .semantic,
        .plan => .plan,
        .execution => .execution,
    };
}

fn mapQueryFailureToError(failure: QueryFailure) lattice_error {
    return switch (failure.stage) {
        .parse, .semantic => .err_invalid_arg,
        .plan, .execution => .err,
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

pub const lattice_list = extern struct {
    items: [*c]lattice_value,
    len: usize,
};

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
        list_val: ?*lattice_list,
        map_val: ?*lattice_map,
    },
};

pub const lattice_map_entry = extern struct {
    key: [*c]const u8,
    key_len: usize,
    value: lattice_value,
};

pub const lattice_map = extern struct {
    entries: [*c]lattice_map_entry,
    len: usize,
};

const ValueConversionError = error{
    InvalidValue,
    DuplicateMapKey,
    OutOfMemory,
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

fn emptyCValue(c_val: *lattice_value) void {
    c_val.* = std.mem.zeroes(lattice_value);
    c_val.value_type = .null;
}

fn byteSlicePtrOrNull(slice: []const u8) [*c]const u8 {
    return if (slice.len == 0) null else slice.ptr;
}

fn floatSlicePtrOrNull(slice: []const f32) [*c]const f32 {
    return if (slice.len == 0) null else slice.ptr;
}

fn cBytesToSlice(ptr: [*c]const u8, len: usize) ValueConversionError![]const u8 {
    if (len == 0) return &[_]u8{};
    if (ptr == null) return error.InvalidValue;
    return ptr[0..len];
}

fn cVectorToSlice(ptr: [*c]const f32, dimensions: u32) ValueConversionError![]const f32 {
    if (dimensions == 0) return &[_]f32{};
    if (ptr == null) return error.InvalidValue;
    return ptr[0..dimensions];
}

fn freeOwnedCValue(c_val: *lattice_value) void {
    switch (c_val.value_type) {
        .string => {
            if (c_val.data.string_val.ptr != null and c_val.data.string_val.len > 0) {
                global_allocator.free(c_val.data.string_val.ptr[0..c_val.data.string_val.len]);
            }
        },
        .bytes => {
            if (c_val.data.bytes_val.ptr != null and c_val.data.bytes_val.len > 0) {
                global_allocator.free(c_val.data.bytes_val.ptr[0..c_val.data.bytes_val.len]);
            }
        },
        .vector => {
            if (c_val.data.vector_val.ptr != null and c_val.data.vector_val.dimensions > 0) {
                global_allocator.free(c_val.data.vector_val.ptr[0..c_val.data.vector_val.dimensions]);
            }
        },
        .list => {
            if (c_val.data.list_val) |list_ptr| {
                if (list_ptr.items != null and list_ptr.len > 0) {
                    const items = list_ptr.items[0..list_ptr.len];
                    for (items) |*item| {
                        freeOwnedCValue(item);
                    }
                    global_allocator.free(items);
                }
                global_allocator.destroy(list_ptr);
            }
        },
        .map => {
            if (c_val.data.map_val) |map_ptr| {
                if (map_ptr.entries != null and map_ptr.len > 0) {
                    const entries = map_ptr.entries[0..map_ptr.len];
                    for (entries) |*entry| {
                        if (entry.key != null and entry.key_len > 0) {
                            global_allocator.free(entry.key[0..entry.key_len]);
                        }
                        freeOwnedCValue(&entry.value);
                    }
                    global_allocator.free(entries);
                }
                global_allocator.destroy(map_ptr);
            }
        },
        else => {},
    }
}

fn zigValueToOwnedCValue(zig_val: PropertyValue, c_val: *lattice_value) ValueConversionError!void {
    emptyCValue(c_val);
    errdefer {
        freeOwnedCValue(c_val);
        emptyCValue(c_val);
    }

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
            if (s.len == 0) {
                c_val.data.string_val.ptr = null;
                c_val.data.string_val.len = 0;
            } else {
                const owned = global_allocator.dupe(u8, s) catch return error.OutOfMemory;
                c_val.data.string_val.ptr = owned.ptr;
                c_val.data.string_val.len = owned.len;
            }
        },
        .bytes_val => |b| {
            c_val.value_type = .bytes;
            if (b.len == 0) {
                c_val.data.bytes_val.ptr = null;
                c_val.data.bytes_val.len = 0;
            } else {
                const owned = global_allocator.dupe(u8, b) catch return error.OutOfMemory;
                c_val.data.bytes_val.ptr = owned.ptr;
                c_val.data.bytes_val.len = owned.len;
            }
        },
        .vector_val => |v| {
            c_val.value_type = .vector;
            if (v.len == 0) {
                c_val.data.vector_val.ptr = null;
                c_val.data.vector_val.dimensions = 0;
            } else {
                const owned = global_allocator.dupe(f32, v) catch return error.OutOfMemory;
                c_val.data.vector_val.ptr = owned.ptr;
                c_val.data.vector_val.dimensions = @intCast(owned.len);
            }
        },
        .list_val => |list| {
            c_val.value_type = .list;
            const list_ptr = global_allocator.create(lattice_list) catch return error.OutOfMemory;
            list_ptr.* = .{
                .items = null,
                .len = list.len,
            };
            c_val.data.list_val = list_ptr;

            if (list.len > 0) {
                const items = global_allocator.alloc(lattice_value, list.len) catch return error.OutOfMemory;
                list_ptr.items = items.ptr;
                for (items) |*item| emptyCValue(item);

                for (list, 0..) |item, i| {
                    try zigValueToOwnedCValue(item, &items[i]);
                }
            }
        },
        .map_val => |map| {
            c_val.value_type = .map;
            const map_ptr = global_allocator.create(lattice_map) catch return error.OutOfMemory;
            map_ptr.* = .{
                .entries = null,
                .len = map.len,
            };
            c_val.data.map_val = map_ptr;

            if (map.len > 0) {
                const entries = global_allocator.alloc(lattice_map_entry, map.len) catch return error.OutOfMemory;
                map_ptr.entries = entries.ptr;
                for (entries) |*entry| {
                    entry.* = std.mem.zeroes(lattice_map_entry);
                    emptyCValue(&entry.value);
                }

                for (map, 0..) |entry, i| {
                    if (entry.key.len == 0) {
                        entries[i].key = null;
                        entries[i].key_len = 0;
                    } else {
                        const owned_key = global_allocator.dupe(u8, entry.key) catch return error.OutOfMemory;
                        entries[i].key = owned_key.ptr;
                        entries[i].key_len = owned_key.len;
                    }
                    try zigValueToOwnedCValue(entry.value, &entries[i].value);
                }
            }
        },
    }
}

fn resultValueToBorrowedCValue(result_val: ResultValue, c_val: *lattice_value, allocator: Allocator) ValueConversionError!void {
    emptyCValue(c_val);

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
            c_val.data.string_val.ptr = byteSlicePtrOrNull(s);
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
            c_val.data.bytes_val.ptr = byteSlicePtrOrNull(b);
            c_val.data.bytes_val.len = b.len;
        },
        .vector_val => |v| {
            c_val.value_type = .vector;
            c_val.data.vector_val.ptr = floatSlicePtrOrNull(v);
            c_val.data.vector_val.dimensions = @intCast(v.len);
        },
        .list_val => |list| {
            c_val.value_type = .list;
            const list_ptr = allocator.create(lattice_list) catch return error.OutOfMemory;
            list_ptr.* = .{
                .items = null,
                .len = list.len,
            };
            c_val.data.list_val = list_ptr;

            if (list.len > 0) {
                const items = allocator.alloc(lattice_value, list.len) catch return error.OutOfMemory;
                list_ptr.items = items.ptr;
                for (list, 0..) |item, i| {
                    try resultValueToBorrowedCValue(item, &items[i], allocator);
                }
            }
        },
        .map_val => |map| {
            c_val.value_type = .map;
            const map_ptr = allocator.create(lattice_map) catch return error.OutOfMemory;
            map_ptr.* = .{
                .entries = null,
                .len = map.len,
            };
            c_val.data.map_val = map_ptr;

            if (map.len > 0) {
                const entries = allocator.alloc(lattice_map_entry, map.len) catch return error.OutOfMemory;
                map_ptr.entries = entries.ptr;

                for (map, 0..) |entry, i| {
                    entries[i].key = byteSlicePtrOrNull(entry.key);
                    entries[i].key_len = entry.key.len;
                    try resultValueToBorrowedCValue(entry.value, &entries[i].value, allocator);
                }
            }
        },
    }
}

fn cValueToOwnedZigValue(c_val: *const lattice_value, allocator: Allocator) ValueConversionError!PropertyValue {
    return switch (c_val.value_type) {
        .null => .{ .null_val = {} },
        .bool => .{ .bool_val = c_val.data.bool_val },
        .int => .{ .int_val = c_val.data.int_val },
        .float => .{ .float_val = c_val.data.float_val },
        .string => blk: {
            const slice = try cBytesToSlice(c_val.data.string_val.ptr, c_val.data.string_val.len);
            break :blk .{ .string_val = allocator.dupe(u8, slice) catch return error.OutOfMemory };
        },
        .bytes => blk: {
            const slice = try cBytesToSlice(c_val.data.bytes_val.ptr, c_val.data.bytes_val.len);
            break :blk .{ .bytes_val = allocator.dupe(u8, slice) catch return error.OutOfMemory };
        },
        .vector => blk: {
            const slice = try cVectorToSlice(c_val.data.vector_val.ptr, c_val.data.vector_val.dimensions);
            break :blk .{ .vector_val = allocator.dupe(f32, slice) catch return error.OutOfMemory };
        },
        .list => blk: {
            const list_ptr = c_val.data.list_val orelse return error.InvalidValue;
            if (list_ptr.len == 0) break :blk .{ .list_val = &[_]PropertyValue{} };
            if (list_ptr.items == null) return error.InvalidValue;

            const items = allocator.alloc(PropertyValue, list_ptr.len) catch return error.OutOfMemory;
            var initialized: usize = 0;
            errdefer {
                for (items[0..initialized]) |*item| {
                    item.deinit(allocator);
                }
                allocator.free(items);
            }

            for (list_ptr.items[0..list_ptr.len], 0..) |item, i| {
                items[i] = try cValueToOwnedZigValue(&item, allocator);
                initialized += 1;
            }

            break :blk .{ .list_val = items };
        },
        .map => blk: {
            const map_ptr = c_val.data.map_val orelse return error.InvalidValue;
            if (map_ptr.len == 0) break :blk .{ .map_val = &[_]PropertyValue.MapEntry{} };
            if (map_ptr.entries == null) return error.InvalidValue;

            const entries = allocator.alloc(PropertyValue.MapEntry, map_ptr.len) catch return error.OutOfMemory;
            var initialized: usize = 0;
            errdefer {
                for (entries[0..initialized]) |*entry| {
                    allocator.free(entry.key);
                    var value = entry.value;
                    value.deinit(allocator);
                }
                allocator.free(entries);
            }

            for (map_ptr.entries[0..map_ptr.len], 0..) |entry, i| {
                const key_slice = try cBytesToSlice(entry.key, entry.key_len);
                for (entries[0..initialized]) |existing| {
                    if (std.mem.eql(u8, existing.key, key_slice)) {
                        return error.DuplicateMapKey;
                    }
                }

                const key = allocator.dupe(u8, key_slice) catch return error.OutOfMemory;
                errdefer allocator.free(key);

                const value = try cValueToOwnedZigValue(&entry.value, allocator);
                errdefer {
                    var owned_value = value;
                    owned_value.deinit(allocator);
                }

                entries[i] = .{
                    .key = key,
                    .value = value,
                };
                initialized += 1;
            }

            break :blk .{ .map_val = entries };
        },
    };
}

fn mapValueConversionError(err: ValueConversionError) lattice_error {
    return switch (err) {
        error.InvalidValue, error.DuplicateMapKey => .err_invalid_arg,
        error.OutOfMemory => .err_out_of_memory,
    };
}

fn duplicateColumnNamesZ(columns: [][]const u8) ![][:0]u8 {
    const owned = try global_allocator.alloc([:0]u8, columns.len);
    var initialized: usize = 0;
    errdefer {
        for (owned[0..initialized]) |name| {
            global_allocator.free(name);
        }
        global_allocator.free(owned);
    }

    for (columns, 0..) |column, i| {
        owned[i] = try global_allocator.dupeZ(u8, column);
        initialized += 1;
    }

    return owned;
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

/// Create a node with an optional single label.
/// Passing null or an empty string creates an unlabeled node.
pub export fn lattice_node_create(
    txn: ?*lattice_txn,
    label: [*c]const u8,
    node_out: *lattice_node_id,
) lattice_error {
    const txn_handle = toHandle(TxnHandle, txn) orelse return .err_invalid_arg;

    if (txn_handle.txn.mode == .read_only) return .err_read_only;

    const maybe_label_slice = cStrToSlice(label);
    const labels: []const []const u8 = if (maybe_label_slice) |label_slice|
        if (label_slice.len == 0) &.{} else &.{label_slice}
    else
        &.{};

    // Pass transaction if it's a real one (id != 0)
    const txn_ptr: ?*Transaction = if (txn_handle.txn.id != 0) &txn_handle.txn else null;
    const node_id = txn_handle.db_handle.db.createNode(txn_ptr, labels) catch |err| {
        return mapAnyError(err);
    };

    node_out.* = node_id;
    return .ok;
}

/// Add a label to an existing node
pub export fn lattice_node_add_label(
    txn: ?*lattice_txn,
    node_id: lattice_node_id,
    label: [*c]const u8,
) lattice_error {
    const txn_handle = toHandle(TxnHandle, txn) orelse return .err_invalid_arg;

    if (txn_handle.txn.mode == .read_only) return .err_read_only;

    const label_slice = cStrToSlice(label) orelse return .err_invalid_arg;
    if (label_slice.len == 0) return .err_invalid_arg;

    const txn_ptr: ?*Transaction = if (txn_handle.txn.id != 0) &txn_handle.txn else null;
    txn_handle.db_handle.db.addNodeLabel(txn_ptr, node_id, label_slice) catch |err| {
        return mapDatabaseError(err);
    };

    return .ok;
}

/// Remove a label from an existing node
pub export fn lattice_node_remove_label(
    txn: ?*lattice_txn,
    node_id: lattice_node_id,
    label: [*c]const u8,
) lattice_error {
    const txn_handle = toHandle(TxnHandle, txn) orelse return .err_invalid_arg;

    if (txn_handle.txn.mode == .read_only) return .err_read_only;

    const label_slice = cStrToSlice(label) orelse return .err_invalid_arg;
    if (label_slice.len == 0) return .err_invalid_arg;

    const txn_ptr: ?*Transaction = if (txn_handle.txn.id != 0) &txn_handle.txn else null;
    txn_handle.db_handle.db.removeNodeLabel(txn_ptr, node_id, label_slice) catch |err| {
        return mapDatabaseError(err);
    };

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

    // Convert C value to an owned Zig PropertyValue for the duration of this call.
    var zig_value = cValueToOwnedZigValue(c_val, global_allocator) catch |err| return mapValueConversionError(err);
    defer zig_value.deinit(global_allocator);

    // Pass transaction if it's a real one (id != 0)
    const txn_ptr: ?*Transaction = if (txn_handle.txn.id != 0) &txn_handle.txn else null;
    txn_handle.db_handle.db.setNodeProperty(txn_ptr, node_id, key_slice, zig_value) catch |err| {
        return mapDatabaseError(err);
    };

    return .ok;
}

/// Get a property from a node.
/// Heap-backed values transfer ownership to the caller and must be released
/// with lattice_value_free().
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
        zigValueToOwnedCValue(zig_value, value_out) catch |err| {
            var owned_value = zig_value;
            owned_value.deinit(txn_handle.db_handle.db.allocator);
            return mapValueConversionError(err);
        };
        var owned_value = zig_value;
        owned_value.deinit(txn_handle.db_handle.db.allocator);
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

/// Free heap-backed storage inside a lattice_value returned by an owning API.
/// Values returned by lattice_result_get are borrowed from the result handle and
/// must not be passed here.
pub export fn lattice_value_free(value: ?*lattice_value) void {
    const c_value = value orelse return;
    freeOwnedCValue(c_value);
    emptyCValue(c_value);
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

/// Search for documents matching a text query with fuzzy (typo-tolerant) matching.
pub export fn lattice_fts_search_fuzzy(
    db: ?*lattice_database,
    query_text: [*c]const u8,
    query_len: usize,
    limit: u32,
    max_distance: u32,
    min_term_length: u32,
    result_out: *?*lattice_fts_result,
) lattice_error {
    const db_handle = toHandle(DatabaseHandle, db) orelse return .err_invalid_arg;

    if (query_text == null or query_len == 0 or limit == 0) return .err_invalid_arg;

    const query_slice = query_text[0..query_len];

    const eff_max_dist = if (max_distance == 0) 2 else max_distance;
    const eff_min_len = if (min_term_length == 0) 4 else min_term_length;

    const results = db_handle.db.ftsSearchFuzzy(query_slice, limit, eff_max_dist, eff_min_len) catch |err| {
        return mapDatabaseError(err);
    };

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
    const edge_id = txn_handle.db_handle.db.createEdgeAndGetId(txn_ptr, source, target, type_slice) catch |err| {
        return mapAnyError(err);
    };

    edge_out.* = edge_id;
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

/// Set a property on an edge by stable edge ID
pub export fn lattice_edge_set_property(
    txn: ?*lattice_txn,
    edge_id: lattice_edge_id,
    key: [*c]const u8,
    value: ?*const lattice_value,
) lattice_error {
    const txn_handle = toHandle(TxnHandle, txn) orelse return .err_invalid_arg;

    if (txn_handle.txn.mode == .read_only) return .err_read_only;

    const key_slice = cStrToSlice(key) orelse return .err_invalid_arg;
    const c_val = value orelse return .err_invalid_arg;
    var zig_value = cValueToOwnedZigValue(c_val, global_allocator) catch |err| return mapValueConversionError(err);
    defer zig_value.deinit(global_allocator);

    const txn_ptr: ?*Transaction = if (txn_handle.txn.id != 0) &txn_handle.txn else null;
    txn_handle.db_handle.db.setEdgePropertyById(txn_ptr, edge_id, key_slice, zig_value) catch |err| {
        return mapDatabaseError(err);
    };

    return .ok;
}

/// Get a property from an edge by stable edge ID
pub export fn lattice_edge_get_property(
    txn: ?*lattice_txn,
    edge_id: lattice_edge_id,
    key: [*c]const u8,
    value_out: *lattice_value,
) lattice_error {
    const txn_handle = toHandle(TxnHandle, txn) orelse return .err_invalid_arg;
    const key_slice = cStrToSlice(key) orelse return .err_invalid_arg;

    const maybe_value = txn_handle.db_handle.db.getEdgePropertyById(edge_id, key_slice) catch |err| {
        return mapDatabaseError(err);
    };

    if (maybe_value) |zig_value| {
        zigValueToOwnedCValue(zig_value, value_out) catch |err| {
            var owned_value = zig_value;
            owned_value.deinit(txn_handle.db_handle.db.allocator);
            return mapValueConversionError(err);
        };
        var owned_value = zig_value;
        owned_value.deinit(txn_handle.db_handle.db.allocator);
        return .ok;
    }

    return .err_not_found;
}

/// Remove a property from an edge by stable edge ID
pub export fn lattice_edge_remove_property(
    txn: ?*lattice_txn,
    edge_id: lattice_edge_id,
    key: [*c]const u8,
) lattice_error {
    const txn_handle = toHandle(TxnHandle, txn) orelse return .err_invalid_arg;

    if (txn_handle.txn.mode == .read_only) return .err_read_only;

    const key_slice = cStrToSlice(key) orelse return .err_invalid_arg;

    const txn_ptr: ?*Transaction = if (txn_handle.txn.id != 0) &txn_handle.txn else null;
    txn_handle.db_handle.db.removeEdgePropertyById(txn_ptr, edge_id, key_slice) catch |err| {
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

/// Get the stable edge ID for an edge result by index
pub export fn lattice_edge_result_get_id(
    result: ?*lattice_edge_result,
    index: u32,
    edge_id_out: *lattice_edge_id,
) lattice_error {
    const result_handle = toHandle(EdgeResultHandle, result) orelse return .err_invalid_arg;

    if (index >= result_handle.count) return .err_invalid_arg;

    edge_id_out.* = result_handle.edges[index].id;
    return .ok;
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

    const owned_value = cValueToOwnedZigValue(c_value, global_allocator) catch |err| return mapValueConversionError(err);
    return query_handle.storeOwnedParameter(name_slice, owned_value);
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

    const owned_value = (PropertyValue{ .vector_val = vector[0..dimensions] }).clone(global_allocator) catch return .err_out_of_memory;
    return query_handle.storeOwnedParameter(name_slice, owned_value);
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

    // Clear any diagnostics from previous executions.
    query_handle.clearLastError();

    // Execute the query (with or without parameters), retaining structured diagnostics.
    var detailed = if (query_handle.parameters.count() > 0)
        query_handle.db_handle.db.queryWithParamsDetailed(query_handle.cypher, &query_handle.parameters) catch |err| {
            return mapQueryError(err);
        }
    else
        query_handle.db_handle.db.queryDetailed(query_handle.cypher) catch |err| {
            return mapQueryError(err);
        };

    if (detailed == .failure) {
        const mapped_err = mapQueryFailureToError(detailed.failure);
        const set_err = query_handle.setLastError(detailed.failure);
        detailed.failure.deinit();
        if (set_err != .ok) return set_err;
        return mapped_err;
    }

    const result = detailed.success;
    const column_names_z = duplicateColumnNamesZ(result.columns) catch {
        var mutable_result = result;
        mutable_result.deinit();
        return .err_out_of_memory;
    };

    // Create result handle
    const result_handle = global_allocator.create(ResultHandle) catch {
        for (column_names_z) |name| {
            global_allocator.free(name);
        }
        global_allocator.free(column_names_z);
        var mutable_result = result;
        mutable_result.deinit();
        return .err_out_of_memory;
    };

    result_handle.* = .{
        .result = result,
        .column_names_z = column_names_z,
        .current_row = 0,
        .started = false,
        .borrowed_value_arena = std.heap.ArenaAllocator.init(global_allocator),
    };

    result_out.* = @ptrCast(result_handle);
    return .ok;
}

/// Get the last query diagnostic stage for a prepared query handle.
pub export fn lattice_query_last_error_stage(query: ?*lattice_query) lattice_query_error_stage {
    const query_handle = toHandle(QueryHandle, query) orelse return .none;
    return query_handle.last_error_stage;
}

/// Get the last query diagnostic message for a prepared query handle.
/// Returns null if no error details are available.
pub export fn lattice_query_last_error_message(query: ?*lattice_query) [*c]const u8 {
    const query_handle = toHandle(QueryHandle, query) orelse return null;
    return if (query_handle.last_error_message) |msg| msg.ptr else null;
}

/// Get the last query diagnostic code for a prepared query handle.
/// Returns null if no stage-specific code is available.
pub export fn lattice_query_last_error_code(query: ?*lattice_query) [*c]const u8 {
    const query_handle = toHandle(QueryHandle, query) orelse return null;
    return if (query_handle.last_error_code) |code| code.ptr else null;
}

/// Whether the last query diagnostic includes source location fields.
pub export fn lattice_query_last_error_has_location(query: ?*lattice_query) bool {
    const query_handle = toHandle(QueryHandle, query) orelse return false;
    return query_handle.last_error_has_location;
}

/// Get last diagnostic line (1-based). Returns 0 when unavailable.
pub export fn lattice_query_last_error_line(query: ?*lattice_query) u32 {
    const query_handle = toHandle(QueryHandle, query) orelse return 0;
    return query_handle.last_error_line;
}

/// Get last diagnostic column (1-based). Returns 0 when unavailable.
pub export fn lattice_query_last_error_column(query: ?*lattice_query) u32 {
    const query_handle = toHandle(QueryHandle, query) orelse return 0;
    return query_handle.last_error_column;
}

/// Get last diagnostic token span length. Returns 0 when unavailable.
pub export fn lattice_query_last_error_length(query: ?*lattice_query) u32 {
    const query_handle = toHandle(QueryHandle, query) orelse return 0;
    return query_handle.last_error_length;
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

    if (index >= result_handle.column_names_z.len) return null;

    return result_handle.column_names_z[index].ptr;
}

/// Get column value at current row.
/// Heap-backed pointers in value_out are borrowed from the result handle and
/// remain valid until lattice_result_free().
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

    resultValueToBorrowedCValue(row.values[index], value_out, result_handle.borrowed_value_arena.allocator()) catch |err| {
        return mapValueConversionError(err);
    };
    return .ok;
}

/// Free a result set
pub export fn lattice_result_free(result: ?*lattice_result) void {
    const result_handle = toHandle(ResultHandle, result) orelse return;

    result_handle.borrowed_value_arena.deinit();
    for (result_handle.column_names_z) |name| {
        global_allocator.free(name);
    }
    global_allocator.free(result_handle.column_names_z);
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
    return "0.4.0";
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
        .err_unsupported => "Unsupported operation or value type",
    };
}

// ============================================================================
// Embedding Operations
// ============================================================================

/// Embedding API format for C API
pub const lattice_embedding_api_format = enum(c_int) {
    ollama = 0,
    openai = 1,
};

/// Embedding client configuration for C API
pub const lattice_embedding_config = extern struct {
    endpoint: [*c]const u8,
    model: [*c]const u8,
    api_format: lattice_embedding_api_format,
    api_key: [*c]const u8,
    timeout_ms: u32,
};

/// Generate a hash embedding (built-in, no external service).
/// Caller must free the returned vector with lattice_hash_embed_free().
pub export fn lattice_hash_embed(
    text: [*c]const u8,
    text_len: usize,
    dimensions: u16,
    vector_out: *?[*]f32,
    dims_out: *u32,
) lattice_error {
    vector_out.* = null;
    dims_out.* = 0;

    if (text == null or text_len == 0) return .err_invalid_arg;
    if (dimensions == 0) return .err_invalid_arg;

    const text_slice = text[0..text_len];

    const vector = hash_embed_mod.hashEmbed(global_allocator, text_slice, .{
        .dimensions = dimensions,
    }) catch |err| {
        return switch (err) {
            hash_embed_mod.HashEmbedError.OutOfMemory => .err_out_of_memory,
            hash_embed_mod.HashEmbedError.EmptyInput => .err_invalid_arg,
            hash_embed_mod.HashEmbedError.InvalidDimensions => .err_invalid_arg,
        };
    };

    vector_out.* = vector.ptr;
    dims_out.* = @intCast(vector.len);
    return .ok;
}

/// Free a vector returned by lattice_hash_embed or lattice_embedding_client_embed.
pub export fn lattice_hash_embed_free(
    vector_ptr: ?[*]f32,
    dimensions: u32,
) void {
    if (vector_ptr) |ptr| {
        if (dimensions > 0) {
            global_allocator.free(ptr[0..dimensions]);
        }
    }
}

/// Create an HTTP embedding client.
pub export fn lattice_embedding_client_create(
    config: ?*const lattice_embedding_config,
    client_out: *?*lattice_embedding_client,
) lattice_error {
    client_out.* = null;

    const cfg = config orelse return .err_invalid_arg;

    const endpoint = cStrToSlice(cfg.endpoint) orelse return .err_invalid_arg;
    const model = cStrToSlice(cfg.model) orelse return .err_invalid_arg;

    const api_format: embedding_mod.ApiFormat = switch (cfg.api_format) {
        .ollama => .ollama,
        .openai => .openai,
    };

    const api_key: ?[]const u8 = cStrToSlice(cfg.api_key);

    const timeout = if (cfg.timeout_ms == 0) @as(u32, 30_000) else cfg.timeout_ms;

    const handle = global_allocator.create(EmbeddingClientHandle) catch return .err_out_of_memory;
    handle.* = .{
        .client = embedding_mod.EmbeddingClient.init(global_allocator, .{
            .endpoint = endpoint,
            .model = model,
            .api_format = api_format,
            .api_key = api_key,
            .timeout_ms = timeout,
        }),
    };

    client_out.* = @ptrCast(handle);
    return .ok;
}

/// Generate an embedding via HTTP.
/// Caller must free the returned vector with lattice_hash_embed_free().
pub export fn lattice_embedding_client_embed(
    client: ?*lattice_embedding_client,
    text: [*c]const u8,
    text_len: usize,
    vector_out: *?[*]f32,
    dims_out: *u32,
) lattice_error {
    vector_out.* = null;
    dims_out.* = 0;

    const handle = toHandle(EmbeddingClientHandle, client) orelse return .err_invalid_arg;

    if (text == null or text_len == 0) return .err_invalid_arg;

    const text_slice = text[0..text_len];

    const vector = handle.client.embed(text_slice) catch |err| {
        return switch (err) {
            embedding_mod.EmbeddingError.ConnectionFailed,
            embedding_mod.EmbeddingError.RequestFailed,
            => .err_io,
            embedding_mod.EmbeddingError.ServerError,
            embedding_mod.EmbeddingError.ParseError,
            embedding_mod.EmbeddingError.InvalidResponse,
            => .err,
            embedding_mod.EmbeddingError.NotConfigured,
            embedding_mod.EmbeddingError.InvalidUri,
            => .err_invalid_arg,
            embedding_mod.EmbeddingError.OutOfMemory => .err_out_of_memory,
        };
    };

    vector_out.* = vector.ptr;
    dims_out.* = @intCast(vector.len);
    return .ok;
}

/// Free an HTTP embedding client.
pub export fn lattice_embedding_client_free(
    client: ?*lattice_embedding_client,
) void {
    const handle = toHandle(EmbeddingClientHandle, client) orelse return;
    handle.client.deinit();
    global_allocator.destroy(handle);
}

// ============================================================================
// Tests
// ============================================================================

test "error code values match header" {
    try std.testing.expectEqual(@as(c_int, 0), @intFromEnum(lattice_error.ok));
    try std.testing.expectEqual(@as(c_int, -1), @intFromEnum(lattice_error.err));
    try std.testing.expectEqual(@as(c_int, -13), @intFromEnum(lattice_error.err_out_of_memory));
    try std.testing.expectEqual(@as(c_int, -14), @intFromEnum(lattice_error.err_unsupported));
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
    try std.testing.expectEqualStrings("0.4.0", std.mem.sliceTo(version, 0));
}

test "error message returns valid strings" {
    const msg = lattice_error_message(.ok);
    try std.testing.expect(msg != null);
    try std.testing.expectEqualStrings("Success", std.mem.sliceTo(msg, 0));

    const unsupported = lattice_error_message(.err_unsupported);
    try std.testing.expect(unsupported != null);
    try std.testing.expectEqualStrings("Unsupported operation or value type", std.mem.sliceTo(unsupported, 0));
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
    try zigValueToOwnedCValue(zig_val, &c_val);
    defer lattice_value_free(&c_val);

    try std.testing.expectEqual(lattice_value_type.vector, c_val.value_type);
    try std.testing.expectEqual(@as(u32, 3), c_val.data.vector_val.dimensions);
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), c_val.data.vector_val.ptr[0], 0.001);
    try std.testing.expectApproxEqAbs(@as(f32, 2.0), c_val.data.vector_val.ptr[1], 0.001);
    try std.testing.expectApproxEqAbs(@as(f32, 3.0), c_val.data.vector_val.ptr[2], 0.001);

    // Test C to Zig conversion
    var back_to_zig = try cValueToOwnedZigValue(&c_val, std.testing.allocator);
    defer back_to_zig.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 3), back_to_zig.vector_val.len);
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), back_to_zig.vector_val[0], 0.001);
}

test "nested value conversion round-trips" {
    const allocator = std.testing.allocator;

    var inner_list = [_]PropertyValue{
        .{ .string_val = "graph" },
        .{ .int_val = 7 },
    };
    var entries = [_]PropertyValue.MapEntry{
        .{ .key = "tags", .value = .{ .list_val = &inner_list } },
        .{ .key = "enabled", .value = .{ .bool_val = true } },
    };

    const zig_val = PropertyValue{ .map_val = &entries };
    var c_val: lattice_value = undefined;
    try zigValueToOwnedCValue(zig_val, &c_val);
    defer lattice_value_free(&c_val);

    try std.testing.expectEqual(lattice_value_type.map, c_val.value_type);
    try std.testing.expect(c_val.data.map_val != null);
    try std.testing.expectEqual(@as(usize, 2), c_val.data.map_val.?.len);

    var back_to_zig = try cValueToOwnedZigValue(&c_val, allocator);
    defer back_to_zig.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 2), back_to_zig.map_val.len);
    try std.testing.expectEqualStrings("tags", back_to_zig.map_val[0].key);
    try std.testing.expectEqual(@as(usize, 2), back_to_zig.map_val[0].value.list_val.len);
    try std.testing.expectEqualStrings("graph", back_to_zig.map_val[0].value.list_val[0].string_val);
    try std.testing.expectEqual(@as(i64, 7), back_to_zig.map_val[0].value.list_val[1].int_val);
}

test "invalid nested value conversion returns explicit error" {
    var c_val = std.mem.zeroes(lattice_value);
    c_val.value_type = .list;
    c_val.data.list_val = null;
    try std.testing.expectError(error.InvalidValue, cValueToOwnedZigValue(&c_val, std.testing.allocator));

    const dup_key = "city";
    var dup_entries = [_]lattice_map_entry{
        .{
            .key = dup_key.ptr,
            .key_len = dup_key.len,
            .value = .{
                .value_type = .string,
                .data = .{ .string_val = .{ .ptr = "Portland".ptr, .len = "Portland".len } },
            },
        },
        .{
            .key = dup_key.ptr,
            .key_len = dup_key.len,
            .value = .{
                .value_type = .int,
                .data = .{ .int_val = 97201 },
            },
        },
    };
    var map_value = std.mem.zeroes(lattice_value);
    map_value.value_type = .map;
    var map_container = lattice_map{
        .entries = dup_entries[0..].ptr,
        .len = dup_entries.len,
    };
    map_value.data.map_val = &map_container;

    try std.testing.expectError(error.DuplicateMapKey, cValueToOwnedZigValue(&map_value, std.testing.allocator));
}

test "vector parameter binding validation" {
    // Test that null vector returns error
    try std.testing.expectEqual(lattice_error.err_invalid_arg, lattice_query_bind_vector(null, "query", null, 128));

    // Test that zero dimensions returns error (when query handle is null)
    const dummy_vec = [_]f32{1.0};
    try std.testing.expectEqual(lattice_error.err_invalid_arg, lattice_query_bind_vector(null, "query", &dummy_vec, 0));
}
