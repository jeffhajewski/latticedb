//! C API bindings for Lattice database.
//!
//! Provides the stable C ABI interface for language bindings.
//! This is the primary API exposed to C/C++ applications and serves
//! as the foundation for Python and TypeScript bindings.

const std = @import("std");
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;
const compat = @import("compat");
const Mutex = compat.Mutex;

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
const StreamBatch = database.StreamBatch;
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
/// C callers cannot provide Zig allocators, so we use a global one, and it
/// becomes the allocator for the whole engine behind every binding.
///
/// Debug builds use DebugAllocator: it never reuses an address, so a double
/// free or a use after free in this layer or a binding traps instead of
/// corrupting memory. That safety costs a global mutex and bookkeeping on
/// every allocation, about 25x per property read, so every other build uses
/// smp_allocator. Both are thread-safe, which the bindings need: they call in
/// from several threads at once.
var gpa: std.heap.DebugAllocator(.{ .thread_safe = true }) = .init;
const global_allocator: Allocator = if (builtin.mode == .Debug)
    gpa.allocator()
else
    std.heap.smp_allocator;

// ============================================================================
// Opaque Handle Types
// ============================================================================

const HandleMagic: u64 = 0x4c44544243415049; // "LDTBCAPI"

const HandleKind = enum {
    database,
    txn,
    query,
    result,
    vector_result,
    fts_result,
    edge_result,
    stream_batch,
    embedding_client,
};

const HandleState = enum {
    active,
    closed,
};

const HandleHeader = struct {
    magic: u64,
    kind: HandleKind,
    state: HandleState,
    /// Calls currently inside the C API holding this handle. Only database
    /// and transaction handles are pinned. Guarded by `handle_registry_mutex`.
    pins: u32,
    /// The handle has left the registry; the last unpin frees its memory.
    /// Guarded by `handle_registry_mutex`.
    detached: bool,
    /// What the caller holds in place of this handle's address; see
    /// "Handle tokens". Set by `registerHandle`.
    token: usize,

    fn init(kind: HandleKind) HandleHeader {
        return .{
            .magic = HandleMagic,
            .kind = kind,
            .state = .active,
            .pins = 0,
            .detached = false,
            .token = 0,
        };
    }
};

var handle_registry_mutex: Mutex = .{};

/// Internal database handle wrapping the Zig Database
const DatabaseHandle = struct {
    header: HandleHeader,
    db: *Database,
    mutex: Mutex,
    active_children: usize,
    /// Calls inside `lattice_stream_read`, which wait without holding `mutex`.
    /// Close wakes them and waits for them to leave. Guarded by `mutex`.
    stream_readers: usize,
    /// Set by close so waiting stream readers stop waiting.
    closing: std.atomic.Value(bool),
};

/// Internal transaction handle wrapping actual Transaction
const TxnHandle = struct {
    header: HandleHeader,
    db_handle: *DatabaseHandle,
    txn: Transaction, // Actual Transaction struct from TxnManager
    counted_child: bool,
};

/// Internal query handle storing prepared query state
const QueryHandle = struct {
    header: HandleHeader,
    cypher: []const u8,
    cypher_owned: bool,
    db_handle: *DatabaseHandle,
    counted_child: bool,
    /// Bound parameters (name -> value)
    parameters: std.StringHashMap(PropertyValue),
    last_error_stage: lattice_query_error_stage,
    last_error_message: ?[:0]u8,
    last_error_code: ?[:0]u8,
    last_error_has_location: bool,
    last_error_line: u32,
    last_error_column: u32,
    last_error_length: u32,
    /// The parsed query, pinned in the database's query cache, so prepare
    /// parses once and lattice_query_writes and every execute reuse it.
    /// Null when it could not be cached; those calls then parse the text.
    prepared: ?Database.PreparedQuery = null,

    fn init(cypher: []const u8, cypher_owned: bool, db_handle: *DatabaseHandle) QueryHandle {
        return .{
            .header = HandleHeader.init(.query),
            .cypher = cypher,
            .cypher_owned = cypher_owned,
            .db_handle = db_handle,
            .counted_child = true,
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
    header: HandleHeader,
    result: QueryResult,
    column_names_z: []const [:0]u8,
    current_row: usize,
    started: bool,
    borrowed_value_arena: std.heap.ArenaAllocator,
};

/// Internal vector search result handle
const VectorResultHandle = struct {
    header: HandleHeader,
    results: []VectorSearchResult,
    count: usize,
};

/// Internal FTS search result handle
const FtsResultHandle = struct {
    header: HandleHeader,
    results: []FtsSearchResult,
    count: usize,
    db_handle: *DatabaseHandle,
    counted_child: bool,
};

/// Internal edge result handle for edge traversal
const EdgeResultHandle = struct {
    header: HandleHeader,
    edges: []Database.EdgeInfo,
    /// NUL-terminated copies of the edge types, one per edge, sharing a copy
    /// between neighbouring edges of the same type. The engine's strings carry
    /// no terminator, and bindings that read a `const char*` would run past
    /// them. Allocated in `type_arena`.
    edge_types_z: [][:0]const u8,
    type_arena: std.heap.ArenaAllocator,
    count: usize,
    db_handle: *DatabaseHandle,
    counted_child: bool,
};

/// Internal stream batch handle
const StreamBatchHandle = struct {
    header: HandleHeader,
    batch: StreamBatch,
    c_values: []lattice_value,
};

/// Internal embedding client handle
const EmbeddingClientHandle = struct {
    header: HandleHeader,
    client: embedding_mod.EmbeddingClient,
    /// The client's endpoint, model and API key, copied out of the caller's
    /// config: the client keeps slices of them, and the caller's buffers need
    /// not outlive the create call.
    config_arena: std.heap.ArenaAllocator,
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

/// Opaque stream batch handle for C API
pub const lattice_stream_batch = opaque {};

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
    err_value_too_large = -15,
    /// Another process holds the database. Distinct from err_lock_timeout,
    /// which is about a second writer inside this process.
    err_database_locked = -16,
    /// lattice_node_delete was asked to delete a node that still has
    /// relationships; lattice_node_detach_delete removes them with it.
    err_has_relationships = -17,
    /// The open options contradict the vector index configuration the file
    /// records (dimensions, metric or HNSW parameters).
    err_config_mismatch = -18,
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
        DatabaseError.InvalidArgument => .err_invalid_arg,
        DatabaseError.TransactionNotActive => .err_invalid_arg,
        DatabaseError.TransactionReadOnly => .err_read_only,
        DatabaseError.TransactionConflict => .err_lock_timeout,
        DatabaseError.TransactionsNotEnabled => .err_invalid_arg,
        DatabaseError.ValueTooLarge => .err_value_too_large,
        DatabaseError.MissingIndex => .err_unsupported,
        DatabaseError.DatabaseLocked => .err_database_locked,
        DatabaseError.NodeHasRelationships => .err_has_relationships,
        DatabaseError.VectorConfigMismatch => .err_config_mismatch,
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

/// Versioned open options for C API.
/// New fields must be added through a new versioned struct to preserve the
/// ABI of lattice_open_options.
pub const lattice_open_options_v2 = extern struct {
    struct_size: usize = @sizeOf(lattice_open_options_v2),
    create: bool = false,
    read_only: bool = false,
    cache_size_mb: u32 = 100,
    page_size: u32 = 4096,
    enable_vector: bool = false,
    vector_dimensions: u16 = 128,
    enable_wal: bool = true,
};

/// Versioned open options for adjacency cache exposure.
pub const lattice_open_options_v3 = extern struct {
    struct_size: usize = @sizeOf(lattice_open_options_v3),
    create: bool = false,
    read_only: bool = false,
    cache_size_mb: u32 = 100,
    page_size: u32 = 4096,
    enable_vector: bool = false,
    vector_dimensions: u16 = 128,
    enable_wal: bool = true,
    /// Keep an in-memory adjacency cache. Nothing in this API reads it yet:
    /// queries and the edge functions go to the B+Tree either way.
    enable_adjacency_cache: bool = false,
};

/// Versioned open options that can turn the file lock off.
pub const lattice_open_options_v4 = extern struct {
    struct_size: usize = @sizeOf(lattice_open_options_v4),
    create: bool = false,
    read_only: bool = false,
    cache_size_mb: u32 = 100,
    page_size: u32 = 4096,
    enable_vector: bool = false,
    vector_dimensions: u16 = 128,
    enable_wal: bool = true,
    /// Keep an in-memory adjacency cache. Nothing in this API reads it yet:
    /// queries and the edge functions go to the B+Tree either way.
    enable_adjacency_cache: bool = false,
    /// Take a lock on the file so two processes cannot tread on each other.
    ///
    /// Defaults to true. Turn it off only where locking does not work, such as
    /// some network filesystems; it does not make concurrent access safe.
    lock: bool = true,
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

fn cArgBytes(ptr: [*c]const u8, len: usize) ?[]const u8 {
    if (len == 0) return &[_]u8{};
    if (ptr == null) return null;
    return ptr[0..len];
}

fn cVectorToSlice(ptr: [*c]const f32, dimensions: u32) ValueConversionError![]const f32 {
    if (dimensions == 0) return &[_]f32{};
    if (ptr == null) return error.InvalidValue;
    return ptr[0..dimensions];
}

fn isValidEmbeddingDimensions(dimensions: u32) bool {
    return types.isValidVectorDimensions(dimensions);
}

fn cEmbeddingVectorToSlice(ptr: [*c]const f32, dimensions: u32) ?[]const f32 {
    if (ptr == null or !isValidEmbeddingDimensions(dimensions)) return null;
    return ptr[0..dimensions];
}

/// Read a C enum the caller filled in, without trusting it to hold a declared
/// value. Zig treats an exhaustive enum holding any other value as illegal
/// behaviour (a panic in safe builds, undefined in ReleaseFast), so its raw
/// integer is checked first. Null means the value is not one of `E`'s.
fn checkedEnum(comptime E: type, field: *const E) ?E {
    comptime std.debug.assert(@sizeOf(E) == @sizeOf(c_int));
    const raw: c_int = @as(*const c_int, @ptrCast(field)).*;
    return std.enums.fromInt(E, raw);
}

fn freeOwnedCValue(c_val: *lattice_value) void {
    // An unknown type owns nothing this library allocated.
    const value_type = checkedEnum(lattice_value_type, &c_val.value_type) orelse return;
    switch (value_type) {
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
    const value_type = checkedEnum(lattice_value_type, &c_val.value_type) orelse return error.InvalidValue;
    return switch (value_type) {
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

fn kindForHandle(comptime T: type) HandleKind {
    if (T == DatabaseHandle) return .database;
    if (T == TxnHandle) return .txn;
    if (T == QueryHandle) return .query;
    if (T == ResultHandle) return .result;
    if (T == VectorResultHandle) return .vector_result;
    if (T == FtsResultHandle) return .fts_result;
    if (T == EdgeResultHandle) return .edge_result;
    if (T == StreamBatchHandle) return .stream_batch;
    if (T == EmbeddingClientHandle) return .embedding_client;
    @compileError("unknown C API handle type");
}

// ============================================================================
// Handle tokens
// ============================================================================
//
// Handles reach the caller as tokens, not as the addresses of the structs
// behind them. The registry maps a live token to its struct.
//
// The registry used to be keyed by address, and that is only a check while
// the allocator never hands a freed address out again. Allocators that do
// (std.heap.smp_allocator, for one) put the next handle of the same size at
// the address just freed, so a stale handle (a double free, a use after free,
// a transaction reused after commit) would pass the check and act on somebody
// else's handle. Tokens come from a range no allocation can occupy and are not
// reissued until the sequence wraps, so a token that is not live is always a
// handle that has gone, whatever the allocator does with the memory.
//
// On 64-bit targets a token is 1<<62 | kind<<56 | seq<<3: each kind has its
// own range, so an ended transaction is told apart from garbage, and the
// sequence wraps after 2^53 handles. Nothing dereferences a token; bindings
// only pass it back.

const token_stride: usize = 8;
const token_seq_count: usize = if (@sizeOf(usize) >= 8) 1 << 53 else 1 << 16;

/// 32-bit targets have no address range to spare, so tokens there are
/// addresses inside this array, which is never read or written. All kinds
/// share it, and tokens are reissued after 2^16 further handles.
var token_space: [if (@sizeOf(usize) >= 8) 0 else token_seq_count * token_stride]u8 align(token_stride) = undefined;

fn tokenBase(kind: HandleKind) usize {
    if (@sizeOf(usize) >= 8) return (1 << 62) | (@as(usize, @intFromEnum(kind)) << 56);
    return @intFromPtr(&token_space);
}

/// Whether `addr` lies in the token range of `kind`, live or not.
fn isTokenOf(kind: HandleKind, addr: usize) bool {
    const base = tokenBase(kind);
    if (addr < base) return false;
    const offset = addr - base;
    return offset < token_seq_count * token_stride and offset % token_stride == 0;
}

const HandleEntry = struct {
    kind: HandleKind,
    ptr: *anyopaque,
};

/// Live handles by token. Guarded by `handle_registry_mutex`.
var handle_registry: std.AutoHashMapUnmanaged(usize, HandleEntry) = .{};
var next_token_seq: usize = 0;

/// Give a new handle its token and make it live. The caller hands the
/// token out with `toOpaque`.
fn registerHandle(comptime T: type, handle: *T) lattice_error {
    const kind = kindForHandle(T);
    handle_registry_mutex.lock();
    defer handle_registry_mutex.unlock();

    // Skipping live tokens only matters once the sequence has wrapped.
    var token: usize = undefined;
    while (true) {
        token = tokenBase(kind) + (next_token_seq % token_seq_count) * token_stride;
        next_token_seq +%= 1;
        if (!handle_registry.contains(token)) break;
    }
    handle_registry.put(global_allocator, token, .{ .kind = kind, .ptr = handle }) catch {
        return .err_out_of_memory;
    };
    handle.header.token = token;
    return .ok;
}

fn unregisterHandle(comptime T: type, handle: *T) void {
    handle_registry_mutex.lock();
    defer handle_registry_mutex.unlock();

    const token = handle.header.token;
    if (handle_registry.get(token)) |entry| {
        if (entry.kind == kindForHandle(T) and entry.ptr == @as(*anyopaque, handle)) {
            _ = handle_registry.remove(token);
        }
    }
}

/// The struct behind a live token of kind `T`, or null. Called with
/// `handle_registry_mutex` held.
fn lookupLocked(comptime T: type, addr: usize) ?*T {
    const entry = handle_registry.get(addr) orelse return null;
    if (entry.kind != kindForHandle(T)) return null;
    const handle: *T = @ptrCast(@alignCast(entry.ptr));
    if (handle.header.magic != HandleMagic or handle.header.kind != kindForHandle(T)) return null;
    return handle;
}

/// Resolve an opaque C handle to its internal struct. A token that is not
/// live, or is of another kind, gives null.
fn toHandle(comptime T: type, ptr: anytype) ?*T {
    const addr = @intFromPtr(ptr);
    if (addr == 0) return null;

    handle_registry_mutex.lock();
    defer handle_registry_mutex.unlock();
    return lookupLocked(T, addr);
}

/// What the caller is given for `handle`: its token.
fn toOpaque(comptime T: type, handle: anytype) *T {
    std.debug.assert(handle.header.token != 0);
    return @ptrFromInt(handle.header.token);
}

/// Look up a database handle and pin it for the rest of the calling function,
/// which must `releaseDb` it.
///
/// `toHandle` checks the registry and then lets go of the registry lock, so a
/// close on another thread could free the handle between the check and the
/// caller locking `mutex`. A pin taken under the registry lock closes that
/// window: close detaches the handle, and its memory is freed only when the
/// last pinned call releases it. Such a call finds the handle closed and
/// returns without touching the database.
fn acquireDb(ptr: ?*lattice_database) ?*DatabaseHandle {
    const addr = @intFromPtr(ptr);
    if (addr == 0) return null;

    handle_registry_mutex.lock();
    defer handle_registry_mutex.unlock();
    const handle = lookupLocked(DatabaseHandle, addr) orelse return null;
    handle.header.pins += 1;
    return handle;
}

fn releaseDb(handle: *DatabaseHandle) void {
    handle_registry_mutex.lock();
    std.debug.assert(handle.header.pins > 0);
    handle.header.pins -= 1;
    const free_now = handle.header.detached and handle.header.pins == 0;
    handle_registry_mutex.unlock();

    if (free_now) global_allocator.destroy(handle);
}

/// Remove a closed database handle from the registry. Its memory goes when the
/// last pin is released, which may be the closing call's own.
fn detachDb(handle: *DatabaseHandle) void {
    handle_registry_mutex.lock();
    defer handle_registry_mutex.unlock();
    _ = handle_registry.remove(handle.header.token);
    handle.header.detached = true;
}

// ============================================================================
// Ended transactions
// ============================================================================
//
// lattice.h promises that using a transaction handle after commit or rollback
// returns LATTICE_ERROR_TXN_ABORTED. That used to be kept by holding every
// finished TxnHandle, and its registry entry, until the database closed:
// memory and registry size grew with every transaction ever run. Now the
// TxnHandle is freed when the transaction ends, and a transaction token that
// is no longer live resolves to the stand-in below.

/// Stands in for every ended transaction. A stale token resolves to it and
/// takes the path a retained closed handle used to, so callers get the same
/// errors in the same order as before. Its database is permanently closed;
/// every path checks that before touching `db`, which is never set.
var retired_db_handle: DatabaseHandle = .{
    .header = .{ .magic = HandleMagic, .kind = .database, .state = .closed, .pins = 0, .detached = false, .token = 0 },
    .db = undefined,
    .mutex = .{},
    .active_children = 0,
    .stream_readers = 0,
    .closing = .init(true),
};
var retired_txn_handle: TxnHandle = .{
    .header = .{ .magic = HandleMagic, .kind = .txn, .state = .closed, .pins = 0, .detached = false, .token = 0 },
    .db_handle = &retired_db_handle,
    .txn = .{
        .id = 0,
        .state = .aborted,
        .mode = .read_only,
        .isolation = .snapshot,
        .start_ts = 0,
        .commit_ts = 0,
    },
    .counted_child = false,
};

/// Resolve a caller's transaction token and pin it, and its database, for the
/// rest of the calling function, which must `releaseTxn` it. An ended
/// transaction resolves to `retired_txn_handle`; anything else that is not a
/// live transaction token is rejected.
///
/// The pins are what make freeing safe: a commit on one thread can end a
/// transaction another thread is still using, and the memory stays until that
/// call is done with it.
fn acquireTxn(ptr: ?*lattice_txn) ?*TxnHandle {
    const addr = @intFromPtr(ptr);
    if (addr == 0) return null;

    handle_registry_mutex.lock();
    defer handle_registry_mutex.unlock();
    if (lookupLocked(TxnHandle, addr)) |handle| {
        handle.header.pins += 1;
        handle.db_handle.header.pins += 1;
        return handle;
    }
    // On 32-bit targets kinds share one range, and only a live token can be
    // told apart from the others.
    if (isTokenOf(.txn, addr) and !handle_registry.contains(addr)) return &retired_txn_handle;
    return null;
}

fn releaseTxn(handle: *TxnHandle) void {
    if (handle == &retired_txn_handle) return;

    handle_registry_mutex.lock();
    const db_handle = handle.db_handle;
    std.debug.assert(handle.header.pins > 0 and db_handle.header.pins > 0);
    handle.header.pins -= 1;
    db_handle.header.pins -= 1;
    const free_txn = handle.header.detached and handle.header.pins == 0;
    const free_db = db_handle.header.detached and db_handle.header.pins == 0;
    handle_registry_mutex.unlock();

    if (free_txn) global_allocator.destroy(handle);
    if (free_db) global_allocator.destroy(db_handle);
}

fn releaseDbChildLocked(db_handle: *DatabaseHandle, counted_child: *bool) void {
    if (!counted_child.*) return;
    std.debug.assert(db_handle.active_children > 0);
    db_handle.active_children -= 1;
    counted_child.* = false;
}

/// End a transaction handle after commit or rollback. Called with the database
/// mutex held by a call that has the handle pinned; the handle leaves the
/// registry here and its memory goes when that call releases it.
fn retireTxnHandleLocked(txn_handle: *TxnHandle) void {
    const db_handle = txn_handle.db_handle;
    releaseDbChildLocked(db_handle, &txn_handle.counted_child);
    txn_handle.header.state = .closed;

    handle_registry_mutex.lock();
    defer handle_registry_mutex.unlock();
    _ = handle_registry.remove(txn_handle.header.token);
    txn_handle.header.detached = true;
}

fn ensureActiveDbLocked(db_handle: *DatabaseHandle) lattice_error {
    if (db_handle.header.state != .active) return .err_invalid_arg;
    return .ok;
}

fn ensureActiveTxnLocked(txn_handle: *TxnHandle) lattice_error {
    if (txn_handle.header.state != .active) return .err_txn_aborted;
    if (txn_handle.db_handle.header.state != .active) return .err_txn_aborted;
    return .ok;
}

fn lockActiveTxnDb(txn_handle: *TxnHandle) lattice_error {
    const db_handle = txn_handle.db_handle;
    db_handle.mutex.lock();
    const active_txn_result = ensureActiveTxnLocked(txn_handle);
    if (active_txn_result != .ok) {
        db_handle.mutex.unlock();
        return active_txn_result;
    }
    return .ok;
}

fn unlockTxnDb(txn_handle: *TxnHandle) void {
    txn_handle.db_handle.mutex.unlock();
}

fn buildOpenOptionsV1(options: ?*const lattice_open_options) OpenOptions {
    var zig_options = OpenOptions{
        .create = false,
        .read_only = false,
        .config = DatabaseConfig{
            .enable_wal = true,
        },
    };

    if (options) |opts| {
        zig_options.create = opts.create;
        zig_options.read_only = opts.read_only;
        zig_options.page_size = if (opts.page_size == 0) types.DEFAULT_PAGE_SIZE else opts.page_size;
        zig_options.config.buffer_pool_size = @as(usize, opts.cache_size_mb) * 1024 * 1024;
        zig_options.config.enable_vector = opts.enable_vector;
        zig_options.config.vector_dimensions = opts.vector_dimensions;
    }

    return zig_options;
}

fn buildOpenOptionsV2(options: ?*const lattice_open_options_v2) DatabaseError!OpenOptions {
    var zig_options = OpenOptions{
        .create = false,
        .read_only = false,
        .config = DatabaseConfig{
            .enable_wal = true,
        },
    };

    if (options) |opts| {
        if (opts.struct_size < @sizeOf(lattice_open_options_v2)) {
            return DatabaseError.InvalidArgument;
        }

        zig_options.create = opts.create;
        zig_options.read_only = opts.read_only;
        zig_options.page_size = if (opts.page_size == 0) types.DEFAULT_PAGE_SIZE else opts.page_size;
        zig_options.config.buffer_pool_size = @as(usize, opts.cache_size_mb) * 1024 * 1024;
        zig_options.config.enable_vector = opts.enable_vector;
        zig_options.config.vector_dimensions = opts.vector_dimensions;
        zig_options.config.enable_wal = opts.enable_wal;
    }

    return zig_options;
}

fn buildOpenOptionsV3(options: ?*const lattice_open_options_v3) DatabaseError!OpenOptions {
    var zig_options = OpenOptions{
        .create = false,
        .read_only = false,
        .config = DatabaseConfig{
            .enable_wal = true,
        },
    };

    if (options) |opts| {
        if (opts.struct_size < @sizeOf(lattice_open_options_v3)) {
            return DatabaseError.InvalidArgument;
        }

        zig_options.create = opts.create;
        zig_options.read_only = opts.read_only;
        zig_options.page_size = if (opts.page_size == 0) types.DEFAULT_PAGE_SIZE else opts.page_size;
        zig_options.config.buffer_pool_size = @as(usize, opts.cache_size_mb) * 1024 * 1024;
        zig_options.config.enable_vector = opts.enable_vector;
        zig_options.config.vector_dimensions = opts.vector_dimensions;
        zig_options.config.enable_wal = opts.enable_wal;
        zig_options.config.enable_adjacency_cache = opts.enable_adjacency_cache;
    }

    return zig_options;
}

fn buildOpenOptionsV4(options: ?*const lattice_open_options_v4) DatabaseError!OpenOptions {
    var zig_options = OpenOptions{
        .create = false,
        .read_only = false,
        .config = DatabaseConfig{
            .enable_wal = true,
        },
    };

    if (options) |opts| {
        if (opts.struct_size < @sizeOf(lattice_open_options_v4)) {
            return DatabaseError.InvalidArgument;
        }

        zig_options.create = opts.create;
        zig_options.read_only = opts.read_only;
        zig_options.page_size = if (opts.page_size == 0) types.DEFAULT_PAGE_SIZE else opts.page_size;
        zig_options.config.buffer_pool_size = @as(usize, opts.cache_size_mb) * 1024 * 1024;
        zig_options.config.enable_vector = opts.enable_vector;
        zig_options.config.vector_dimensions = opts.vector_dimensions;
        zig_options.config.enable_wal = opts.enable_wal;
        zig_options.config.enable_adjacency_cache = opts.enable_adjacency_cache;
        zig_options.lock = opts.lock;
    }

    return zig_options;
}

fn openDatabase(
    path: [*c]const u8,
    zig_options: OpenOptions,
    db_out: *?*lattice_database,
) lattice_error {
    db_out.* = null;

    const path_slice = cStrToSlice(path) orelse return .err_invalid_arg;

    const db = Database.open(global_allocator, path_slice, zig_options) catch |err| {
        return mapDatabaseError(err);
    };

    return adoptDatabase(db, db_out);
}

/// Wrap an already-open database in a handle the C API can hand out.
///
/// Takes ownership: on any failure the database is closed, so a caller that gets
/// an error never has to wonder whether something is still open.
fn adoptDatabase(db: *Database, db_out: *?*lattice_database) lattice_error {
    const handle = global_allocator.create(DatabaseHandle) catch {
        db.close();
        return .err_out_of_memory;
    };
    handle.* = .{
        .header = HandleHeader.init(.database),
        .db = db,
        .mutex = .{},
        .active_children = 0,
        .stream_readers = 0,
        .closing = .init(false),
    };
    const register_result = registerHandle(DatabaseHandle, handle);
    if (register_result != .ok) {
        db.close();
        global_allocator.destroy(handle);
        return register_result;
    }

    db_out.* = toOpaque(lattice_database, handle);
    return .ok;
}

fn rejectFakeStreamTxn(txn_handle: *TxnHandle) ?lattice_error {
    if (txn_handle.header.state != .active) return .err_txn_aborted;
    if (txn_handle.txn.id == 0) return .err_unsupported;
    if (txn_handle.txn.mode == .read_only) return .err_read_only;
    return null;
}

fn mapStreamMutationError(err: DatabaseError) lattice_error {
    return switch (err) {
        DatabaseError.TransactionNotActive => .err_txn_aborted,
        DatabaseError.TransactionsNotEnabled => .err_unsupported,
        else => mapDatabaseError(err),
    };
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
    return openDatabase(path, buildOpenOptionsV1(options), db_out);
}

/// Open a database file with versioned options
pub export fn lattice_open_v2(
    path: [*c]const u8,
    options: ?*const lattice_open_options_v2,
    db_out: *?*lattice_database,
) lattice_error {
    const zig_options = buildOpenOptionsV2(options) catch |err| return mapDatabaseError(err);
    return openDatabase(path, zig_options, db_out);
}

/// Open a database file with v3 options
pub export fn lattice_open_v3(
    path: [*c]const u8,
    options: ?*const lattice_open_options_v3,
    db_out: *?*lattice_database,
) lattice_error {
    const zig_options = buildOpenOptionsV3(options) catch |err| return mapDatabaseError(err);
    return openDatabase(path, zig_options, db_out);
}

pub export fn lattice_open_v4(
    path: [*c]const u8,
    options: ?*const lattice_open_options_v4,
    db_out: *?*lattice_database,
) lattice_error {
    const zig_options = buildOpenOptionsV4(options) catch |err| return mapDatabaseError(err);
    return openDatabase(path, zig_options, db_out);
}

/// Close a database
pub export fn lattice_close(db: ?*lattice_database) lattice_error {
    const handle = acquireDb(db) orelse return .err_invalid_arg;
    defer releaseDb(handle);

    handle.mutex.lock();
    if (handle.header.state != .active) {
        handle.mutex.unlock();
        return .err_invalid_arg;
    }
    if (handle.active_children != 0) {
        handle.mutex.unlock();
        return .err_invalid_arg;
    }
    handle.header.state = .closed;
    wakeStreamReadersForCloseLocked(handle);
    handle.mutex.unlock();

    var close_error: ?lattice_error = null;
    handle.db.sync() catch {
        close_error = .err_io;
    };
    if (close_error == null) {
        handle.db.checkpointWal(.truncate) catch {
            close_error = .err_io;
        };
    }

    // Always close and free resources. The handle's own memory goes when the
    // last pinned call releases it, which is usually the deferred release above.
    handle.db.close();
    detachDb(handle);

    return close_error orelse .ok;
}

/// Wake every `lattice_stream_read` waiting on this database and wait for them
/// to leave, so the database is not freed under them. Called by close with
/// `mutex` held and the handle already marked closed, which is what the
/// readers check when they wake.
fn wakeStreamReadersForCloseLocked(handle: *DatabaseHandle) void {
    if (handle.stream_readers == 0) return;

    handle.closing.store(true, .release);
    handle.db.stream_mutex.lock();
    handle.db.stream_cond.broadcast();
    handle.db.stream_mutex.unlock();

    while (handle.stream_readers != 0) {
        handle.mutex.unlock();
        compat.sleep(std.time.ns_per_ms);
        handle.mutex.lock();
    }
}

// ============================================================================
// Transaction Operations
// ============================================================================

/// Begin a transaction
pub export fn lattice_begin(
    db: ?*lattice_database,
    mode: lattice_txn_mode,
    txn_out: ?*?*lattice_txn,
) lattice_error {
    const out = txn_out orelse return .err_invalid_arg;
    out.* = null;

    const db_handle = acquireDb(db) orelse return .err_invalid_arg;
    defer releaseDb(db_handle);
    db_handle.mutex.lock();
    defer db_handle.mutex.unlock();
    const active_db_result = ensureActiveDbLocked(db_handle);
    if (active_db_result != .ok) return active_db_result;

    // Map C API mode to TxnMode
    const checked_mode = checkedEnum(lattice_txn_mode, &mode) orelse return .err_invalid_arg;
    const txn_mode: TxnMode = if (checked_mode == .read_only) .read_only else .read_write;

    // Actually begin a transaction in the database
    const txn = db_handle.db.beginTransaction(txn_mode) catch |err| {
        return switch (err) {
            DatabaseError.TransactionsNotEnabled => {
                if (txn_mode == .read_only) {
                    const txn_handle = global_allocator.create(TxnHandle) catch return .err_out_of_memory;
                    txn_handle.* = .{
                        .header = HandleHeader.init(.txn),
                        .db_handle = db_handle,
                        .txn = Transaction{
                            .id = 0,
                            .state = .active,
                            .mode = .read_only,
                            .isolation = .snapshot,
                            .start_ts = 0,
                            .commit_ts = 0,
                        },
                        .counted_child = true,
                    };
                    const register_result = registerHandle(TxnHandle, txn_handle);
                    if (register_result != .ok) {
                        global_allocator.destroy(txn_handle);
                        return register_result;
                    }
                    db_handle.active_children += 1;
                    out.* = toOpaque(lattice_txn, txn_handle);
                    return .ok;
                }
                return .err_unsupported;
            },
            DatabaseError.OutOfMemory => .err_out_of_memory,
            DatabaseError.TransactionConflict => .err_lock_timeout,
            else => .err,
        };
    };

    // Create transaction handle with real transaction
    const txn_handle = global_allocator.create(TxnHandle) catch {
        var mutable_txn = txn;
        db_handle.db.abortTransaction(&mutable_txn) catch {};
        return .err_out_of_memory;
    };
    txn_handle.* = .{
        .header = HandleHeader.init(.txn),
        .db_handle = db_handle,
        .txn = txn,
        .counted_child = true,
    };
    const register_result = registerHandle(TxnHandle, txn_handle);
    if (register_result != .ok) {
        var mutable_txn = txn;
        db_handle.db.abortTransaction(&mutable_txn) catch {};
        global_allocator.destroy(txn_handle);
        return register_result;
    }
    db_handle.active_children += 1;

    out.* = toOpaque(lattice_txn, txn_handle);
    return .ok;
}

/// Commit a transaction
pub export fn lattice_commit(txn: ?*lattice_txn) lattice_error {
    const txn_handle = acquireTxn(txn) orelse return .err_invalid_arg;
    defer releaseTxn(txn_handle);
    const db_handle = txn_handle.db_handle;
    db_handle.mutex.lock();
    defer db_handle.mutex.unlock();

    const active_result = ensureActiveTxnLocked(txn_handle);
    if (active_result != .ok) return active_result;

    // Only commit if this is a real transaction (id != 0)
    if (txn_handle.txn.id != 0) {
        txn_handle.db_handle.db.commitTransaction(&txn_handle.txn) catch |err| {
            // Don't destroy handle on error - let caller retry or rollback
            return switch (err) {
                DatabaseError.TransactionNotActive => blk: {
                    retireTxnHandleLocked(txn_handle);
                    break :blk .err_txn_aborted;
                },
                else => mapDatabaseError(err),
            };
        };
    }

    retireTxnHandleLocked(txn_handle);
    return .ok;
}

/// Rollback a transaction
pub export fn lattice_rollback(txn: ?*lattice_txn) lattice_error {
    const txn_handle = acquireTxn(txn) orelse return .err_invalid_arg;
    defer releaseTxn(txn_handle);
    const db_handle = txn_handle.db_handle;
    db_handle.mutex.lock();
    defer db_handle.mutex.unlock();

    const active_result = ensureActiveTxnLocked(txn_handle);
    if (active_result != .ok) return active_result;

    // Only abort if this is a real transaction (id != 0)
    if (txn_handle.txn.id != 0) {
        txn_handle.db_handle.db.abortTransaction(&txn_handle.txn) catch |err| {
            // Still destroy handle even on error - transaction is unusable
            retireTxnHandleLocked(txn_handle);
            return switch (err) {
                DatabaseError.TransactionNotActive => .err_txn_aborted,
                else => .err,
            };
        };
    }

    retireTxnHandleLocked(txn_handle);
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
    const txn_handle = acquireTxn(txn) orelse return .err_invalid_arg;
    defer releaseTxn(txn_handle);

    const maybe_label_slice = cStrToSlice(label);
    const labels: []const []const u8 = if (maybe_label_slice) |label_slice|
        if (label_slice.len == 0) &.{} else &.{label_slice}
    else
        &.{};

    const active_txn_result = lockActiveTxnDb(txn_handle);
    if (active_txn_result != .ok) return active_txn_result;
    defer unlockTxnDb(txn_handle);

    if (txn_handle.txn.mode == .read_only) return .err_read_only;

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
    const txn_handle = acquireTxn(txn) orelse return .err_invalid_arg;
    defer releaseTxn(txn_handle);

    const label_slice = cStrToSlice(label) orelse return .err_invalid_arg;
    if (label_slice.len == 0) return .err_invalid_arg;

    const active_txn_result = lockActiveTxnDb(txn_handle);
    if (active_txn_result != .ok) return active_txn_result;
    defer unlockTxnDb(txn_handle);

    if (txn_handle.txn.mode == .read_only) return .err_read_only;

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
    const txn_handle = acquireTxn(txn) orelse return .err_invalid_arg;
    defer releaseTxn(txn_handle);

    const label_slice = cStrToSlice(label) orelse return .err_invalid_arg;
    if (label_slice.len == 0) return .err_invalid_arg;

    const active_txn_result = lockActiveTxnDb(txn_handle);
    if (active_txn_result != .ok) return active_txn_result;
    defer unlockTxnDb(txn_handle);

    if (txn_handle.txn.mode == .read_only) return .err_read_only;

    const txn_ptr: ?*Transaction = if (txn_handle.txn.id != 0) &txn_handle.txn else null;
    txn_handle.db_handle.db.removeNodeLabel(txn_ptr, node_id, label_slice) catch |err| {
        return mapDatabaseError(err);
    };

    return .ok;
}

/// Map an error from deleting a node or creating an edge. The two errors
/// these have of their own get their own codes; the rest map as before.
fn mapGraphWriteError(err: anyerror) lattice_error {
    return switch (err) {
        error.NotFound => .err_not_found,
        error.NodeHasRelationships => .err_has_relationships,
        else => mapAnyError(err),
    };
}

/// Delete a node. Refuses with err_has_relationships while the node still
/// has relationships the transaction can see.
pub export fn lattice_node_delete(
    txn: ?*lattice_txn,
    node_id: lattice_node_id,
) lattice_error {
    return deleteNodeFromC(txn, node_id, false);
}

/// Delete a node together with every relationship it has.
pub export fn lattice_node_detach_delete(
    txn: ?*lattice_txn,
    node_id: lattice_node_id,
) lattice_error {
    return deleteNodeFromC(txn, node_id, true);
}

fn deleteNodeFromC(txn: ?*lattice_txn, node_id: lattice_node_id, detach: bool) lattice_error {
    const txn_handle = acquireTxn(txn) orelse return .err_invalid_arg;
    defer releaseTxn(txn_handle);

    const active_txn_result = lockActiveTxnDb(txn_handle);
    if (active_txn_result != .ok) return active_txn_result;
    defer unlockTxnDb(txn_handle);

    if (txn_handle.txn.mode == .read_only) return .err_read_only;

    // Pass transaction if it's a real one (id != 0)
    const txn_ptr: ?*Transaction = if (txn_handle.txn.id != 0) &txn_handle.txn else null;
    const db = txn_handle.db_handle.db;
    const result = if (detach) db.detachDeleteNode(txn_ptr, node_id) else db.deleteNode(txn_ptr, node_id);
    result catch |err| switch (err) {
        // Deleting a node that is not there is not an error, here as before.
        error.NotFound => {},
        else => return mapGraphWriteError(err),
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
    const txn_handle = acquireTxn(txn) orelse return .err_invalid_arg;
    defer releaseTxn(txn_handle);

    const key_slice = cStrToSlice(key) orelse return .err_invalid_arg;
    const c_val = value orelse return .err_invalid_arg;

    // Convert C value to an owned Zig PropertyValue for the duration of this call.
    var zig_value = cValueToOwnedZigValue(c_val, global_allocator) catch |err| return mapValueConversionError(err);
    defer zig_value.deinit(global_allocator);

    const active_txn_result = lockActiveTxnDb(txn_handle);
    if (active_txn_result != .ok) return active_txn_result;
    defer unlockTxnDb(txn_handle);

    if (txn_handle.txn.mode == .read_only) return .err_read_only;

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
    const txn_handle = acquireTxn(txn) orelse return .err_invalid_arg;
    defer releaseTxn(txn_handle);

    const key_slice = cStrToSlice(key) orelse return .err_invalid_arg;

    const active_txn_result = lockActiveTxnDb(txn_handle);
    if (active_txn_result != .ok) return active_txn_result;
    defer unlockTxnDb(txn_handle);

    const txn_ptr: ?*Transaction = if (txn_handle.txn.id != 0) &txn_handle.txn else null;

    const maybe_value = txn_handle.db_handle.db.getNodePropertyInTxn(txn_ptr, node_id, key_slice) catch |err| {
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
    const txn_handle = acquireTxn(txn) orelse return .err_invalid_arg;
    defer releaseTxn(txn_handle);

    const active_txn_result = lockActiveTxnDb(txn_handle);
    if (active_txn_result != .ok) return active_txn_result;
    defer unlockTxnDb(txn_handle);

    const txn_ptr: ?*Transaction = if (txn_handle.txn.id != 0) &txn_handle.txn else null;

    exists_out.* = txn_handle.db_handle.db.nodeExistsInTxn(txn_ptr, node_id) catch |err| {
        return mapDatabaseError(err);
    };
    return .ok;
}

/// Return every node id that currently carries `label`. On success writes
/// a heap-allocated array of node ids into `node_ids_out` and its element
/// count into `count_out`; the caller must release it with
/// `lattice_free_node_ids`. An unknown label is not an error and yields
/// `count_out = 0` with `node_ids_out = null`.
pub export fn lattice_get_nodes_by_label(
    db: ?*lattice_database,
    label: [*c]const u8,
    label_len: usize,
    node_ids_out: *?[*]lattice_node_id,
    count_out: *usize,
) lattice_error {
    const db_handle = acquireDb(db) orelse return .err_invalid_arg;
    defer releaseDb(db_handle);
    if (label == null) return .err_invalid_arg;

    node_ids_out.* = null;
    count_out.* = 0;

    const label_slice = label[0..label_len];

    db_handle.mutex.lock();
    defer db_handle.mutex.unlock();
    const active = ensureActiveDbLocked(db_handle);
    if (active != .ok) return active;

    const owned = db_handle.db.getNodesByLabel(label_slice) catch |err| {
        return mapDatabaseError(err);
    };

    if (owned.len == 0) {
        db_handle.db.allocator.free(owned);
        return .ok;
    }

    // `getNodesByLabel` hands back a slice owned by the database allocator.
    // Copy it into a `global_allocator` buffer so `lattice_free_node_ids`
    // can release it without coupling the caller to lattice internals.
    const out = global_allocator.alloc(lattice_node_id, owned.len) catch {
        db_handle.db.allocator.free(owned);
        return .err_out_of_memory;
    };
    @memcpy(out, owned);
    db_handle.db.allocator.free(owned);

    node_ids_out.* = out.ptr;
    count_out.* = out.len;
    return .ok;
}

pub export fn lattice_get_nodes_by_label_txn(
    txn: ?*lattice_txn,
    label: [*c]const u8,
    label_len: usize,
    node_ids_out: *?[*]lattice_node_id,
    count_out: *usize,
) lattice_error {
    const txn_handle = acquireTxn(txn) orelse return .err_invalid_arg;
    defer releaseTxn(txn_handle);
    if (label == null) return .err_invalid_arg;

    node_ids_out.* = null;
    count_out.* = 0;

    const label_slice = label[0..label_len];

    const active_txn_result = lockActiveTxnDb(txn_handle);
    if (active_txn_result != .ok) return active_txn_result;
    defer unlockTxnDb(txn_handle);

    const txn_ptr: ?*Transaction = if (txn_handle.txn.id != 0) &txn_handle.txn else null;
    const owned = txn_handle.db_handle.db.getNodesByLabelInTxn(txn_ptr, label_slice) catch |err| {
        return mapDatabaseError(err);
    };

    if (owned.len == 0) {
        txn_handle.db_handle.db.allocator.free(owned);
        return .ok;
    }

    const out = global_allocator.alloc(lattice_node_id, owned.len) catch {
        txn_handle.db_handle.db.allocator.free(owned);
        return .err_out_of_memory;
    };
    @memcpy(out, owned);
    txn_handle.db_handle.db.allocator.free(owned);

    node_ids_out.* = out.ptr;
    count_out.* = out.len;
    return .ok;
}

pub export fn lattice_get_all_nodes_txn(
    txn: ?*lattice_txn,
    node_ids_out: *?[*]lattice_node_id,
    count_out: *usize,
) lattice_error {
    const txn_handle = acquireTxn(txn) orelse return .err_invalid_arg;
    defer releaseTxn(txn_handle);

    node_ids_out.* = null;
    count_out.* = 0;

    const active_txn_result = lockActiveTxnDb(txn_handle);
    if (active_txn_result != .ok) return active_txn_result;
    defer unlockTxnDb(txn_handle);

    const txn_ptr: ?*Transaction = if (txn_handle.txn.id != 0) &txn_handle.txn else null;
    const owned = txn_handle.db_handle.db.getAllNodeIdsInTxn(txn_ptr) catch |err| {
        return mapDatabaseError(err);
    };

    if (owned.len == 0) {
        txn_handle.db_handle.db.allocator.free(owned);
        return .ok;
    }

    const out = global_allocator.alloc(lattice_node_id, owned.len) catch {
        txn_handle.db_handle.db.allocator.free(owned);
        return .err_out_of_memory;
    };
    @memcpy(out, owned);
    txn_handle.db_handle.db.allocator.free(owned);

    node_ids_out.* = out.ptr;
    count_out.* = out.len;
    return .ok;
}

pub export fn lattice_node_property_index_create(
    db: ?*lattice_database,
    label: [*c]const u8,
    property: [*c]const u8,
) lattice_error {
    const db_handle = acquireDb(db) orelse return .err_invalid_arg;
    defer releaseDb(db_handle);
    const label_slice = cStrToSlice(label) orelse return .err_invalid_arg;
    const property_slice = cStrToSlice(property) orelse return .err_invalid_arg;

    db_handle.mutex.lock();
    defer db_handle.mutex.unlock();
    const active = ensureActiveDbLocked(db_handle);
    if (active != .ok) return active;
    db_handle.db.createNodePropertyIndex(label_slice, property_slice) catch |err| return mapDatabaseError(err);
    return .ok;
}

pub export fn lattice_node_property_index_drop(
    db: ?*lattice_database,
    label: [*c]const u8,
    property: [*c]const u8,
) lattice_error {
    const db_handle = acquireDb(db) orelse return .err_invalid_arg;
    defer releaseDb(db_handle);
    const label_slice = cStrToSlice(label) orelse return .err_invalid_arg;
    const property_slice = cStrToSlice(property) orelse return .err_invalid_arg;

    db_handle.mutex.lock();
    defer db_handle.mutex.unlock();
    const active = ensureActiveDbLocked(db_handle);
    if (active != .ok) return active;
    db_handle.db.dropNodePropertyIndex(label_slice, property_slice) catch |err| return mapDatabaseError(err);
    return .ok;
}

pub export fn lattice_node_fts_index_create(
    db: ?*lattice_database,
    label: [*c]const u8,
    property: [*c]const u8,
) lattice_error {
    const db_handle = acquireDb(db) orelse return .err_invalid_arg;
    defer releaseDb(db_handle);
    const label_slice = cStrToSlice(label) orelse return .err_invalid_arg;
    const property_slice = cStrToSlice(property) orelse return .err_invalid_arg;

    db_handle.mutex.lock();
    defer db_handle.mutex.unlock();
    const active = ensureActiveDbLocked(db_handle);
    if (active != .ok) return active;
    db_handle.db.createNodeFtsIndex(label_slice, property_slice) catch |err| return mapDatabaseError(err);
    return .ok;
}

pub export fn lattice_node_fts_index_drop(
    db: ?*lattice_database,
    label: [*c]const u8,
    property: [*c]const u8,
) lattice_error {
    const db_handle = acquireDb(db) orelse return .err_invalid_arg;
    defer releaseDb(db_handle);
    const label_slice = cStrToSlice(label) orelse return .err_invalid_arg;
    const property_slice = cStrToSlice(property) orelse return .err_invalid_arg;

    db_handle.mutex.lock();
    defer db_handle.mutex.unlock();
    const active = ensureActiveDbLocked(db_handle);
    if (active != .ok) return active;
    db_handle.db.dropNodeFtsIndex(label_slice, property_slice) catch |err| return mapDatabaseError(err);
    return .ok;
}

pub export fn lattice_node_fts_index_exists(
    db: ?*lattice_database,
    label: [*c]const u8,
    property: [*c]const u8,
    exists_out: *bool,
) lattice_error {
    const db_handle = acquireDb(db) orelse return .err_invalid_arg;
    defer releaseDb(db_handle);
    const label_slice = cStrToSlice(label) orelse return .err_invalid_arg;
    const property_slice = cStrToSlice(property) orelse return .err_invalid_arg;

    db_handle.mutex.lock();
    defer db_handle.mutex.unlock();
    const active = ensureActiveDbLocked(db_handle);
    if (active != .ok) return active;
    exists_out.* = db_handle.db.hasNodeFtsIndex(label_slice, property_slice) catch |err| return mapDatabaseError(err);
    return .ok;
}

pub export fn lattice_edge_fts_index_create(
    db: ?*lattice_database,
    edge_type: [*c]const u8,
    property: [*c]const u8,
) lattice_error {
    const db_handle = acquireDb(db) orelse return .err_invalid_arg;
    defer releaseDb(db_handle);
    const type_slice = cStrToSlice(edge_type) orelse return .err_invalid_arg;
    const property_slice = cStrToSlice(property) orelse return .err_invalid_arg;

    db_handle.mutex.lock();
    defer db_handle.mutex.unlock();
    const active = ensureActiveDbLocked(db_handle);
    if (active != .ok) return active;
    db_handle.db.createEdgeFtsIndex(type_slice, property_slice) catch |err| return mapDatabaseError(err);
    return .ok;
}

pub export fn lattice_edge_fts_index_drop(
    db: ?*lattice_database,
    edge_type: [*c]const u8,
    property: [*c]const u8,
) lattice_error {
    const db_handle = acquireDb(db) orelse return .err_invalid_arg;
    defer releaseDb(db_handle);
    const type_slice = cStrToSlice(edge_type) orelse return .err_invalid_arg;
    const property_slice = cStrToSlice(property) orelse return .err_invalid_arg;

    db_handle.mutex.lock();
    defer db_handle.mutex.unlock();
    const active = ensureActiveDbLocked(db_handle);
    if (active != .ok) return active;
    db_handle.db.dropEdgeFtsIndex(type_slice, property_slice) catch |err| return mapDatabaseError(err);
    return .ok;
}

pub export fn lattice_edge_fts_index_exists(
    db: ?*lattice_database,
    edge_type: [*c]const u8,
    property: [*c]const u8,
    exists_out: *bool,
) lattice_error {
    const db_handle = acquireDb(db) orelse return .err_invalid_arg;
    defer releaseDb(db_handle);
    const type_slice = cStrToSlice(edge_type) orelse return .err_invalid_arg;
    const property_slice = cStrToSlice(property) orelse return .err_invalid_arg;

    db_handle.mutex.lock();
    defer db_handle.mutex.unlock();
    const active = ensureActiveDbLocked(db_handle);
    if (active != .ok) return active;
    exists_out.* = db_handle.db.hasEdgeFtsIndex(type_slice, property_slice) catch |err| return mapDatabaseError(err);
    return .ok;
}

pub export fn lattice_nodes_find_by_label_property(
    txn: ?*lattice_txn,
    label: [*c]const u8,
    property: [*c]const u8,
    value: ?*const lattice_value,
    limit: usize,
    node_ids_out: *?[*]lattice_node_id,
    count_out: *usize,
) lattice_error {
    const txn_handle = acquireTxn(txn) orelse return .err_invalid_arg;
    defer releaseTxn(txn_handle);
    const label_slice = cStrToSlice(label) orelse return .err_invalid_arg;
    const property_slice = cStrToSlice(property) orelse return .err_invalid_arg;
    const c_value = value orelse return .err_invalid_arg;
    if (limit == 0) return .err_invalid_arg;
    node_ids_out.* = null;
    count_out.* = 0;

    var zig_value = cValueToOwnedZigValue(c_value, global_allocator) catch |err| return mapValueConversionError(err);
    defer zig_value.deinit(global_allocator);
    const active = lockActiveTxnDb(txn_handle);
    if (active != .ok) return active;
    defer unlockTxnDb(txn_handle);

    const txn_ptr: ?*Transaction = if (txn_handle.txn.id != 0) &txn_handle.txn else null;
    const owned = txn_handle.db_handle.db.findNodesByLabelProperty(
        txn_ptr,
        label_slice,
        property_slice,
        zig_value,
        limit,
    ) catch |err| return mapDatabaseError(err);
    defer txn_handle.db_handle.db.allocator.free(owned);
    if (owned.len == 0) return .ok;

    const output = global_allocator.dupe(lattice_node_id, owned) catch return .err_out_of_memory;
    node_ids_out.* = output.ptr;
    count_out.* = output.len;
    return .ok;
}

/// Free an array returned by `lattice_get_nodes_by_label`.
pub export fn lattice_free_node_ids(node_ids: ?[*]lattice_node_id, count: usize) void {
    const ptr = node_ids orelse return;
    if (count == 0) return;
    global_allocator.free(ptr[0..count]);
}

/// Get labels for a node as comma-separated string
pub export fn lattice_node_get_labels(
    txn: ?*lattice_txn,
    node_id: lattice_node_id,
    labels_out: *[*c]u8,
) lattice_error {
    const txn_handle = acquireTxn(txn) orelse return .err_invalid_arg;
    defer releaseTxn(txn_handle);

    const active_txn_result = lockActiveTxnDb(txn_handle);
    if (active_txn_result != .ok) return active_txn_result;
    defer unlockTxnDb(txn_handle);

    const txn_ptr: ?*Transaction = if (txn_handle.txn.id != 0) &txn_handle.txn else null;

    const labels = txn_handle.db_handle.db.getNodeLabelsInTxn(txn_ptr, node_id) catch |err| {
        return mapDatabaseError(err);
    };
    defer {
        for (labels) |label| {
            txn_handle.db_handle.db.allocator.free(label);
        }
        txn_handle.db_handle.db.allocator.free(labels);
    }

    // Build comma-separated label string
    var total_len: usize = 0;
    for (labels, 0..) |label_str, i| {
        if (i > 0) total_len += 1; // comma
        total_len += label_str.len;
    }

    // Allocate result string (plus null terminator)
    const result = global_allocator.alloc(u8, total_len + 1) catch {
        return .err_out_of_memory;
    };

    // Fill the string
    var pos: usize = 0;
    for (labels, 0..) |label_str, i| {
        if (i > 0) {
            result[pos] = ',';
            pos += 1;
        }
        @memcpy(result[pos..][0..label_str.len], label_str);
        pos += label_str.len;
    }
    result[pos] = 0; // null terminator

    labels_out.* = result.ptr;
    return .ok;
}

/// Free a string allocated by lattice
pub export fn lattice_serialize(
    db: ?*lattice_database,
    bytes_out: ?*?[*]u8,
    len_out: ?*usize,
) lattice_error {
    const out_bytes = bytes_out orelse return .err_invalid_arg;
    const out_len = len_out orelse return .err_invalid_arg;
    out_bytes.* = null;
    out_len.* = 0;

    const handle = acquireDb(db) orelse return .err_invalid_arg;
    defer releaseDb(handle);

    handle.mutex.lock();
    defer handle.mutex.unlock();
    if (handle.header.state != .active) return .err_invalid_arg;

    const bytes = handle.db.serialize(global_allocator) catch |err| {
        return mapDatabaseError(err);
    };

    out_bytes.* = bytes.ptr;
    out_len.* = bytes.len;
    return .ok;
}

pub export fn lattice_deserialize(
    bytes: [*c]const u8,
    len: usize,
    options: ?*const lattice_open_options_v4,
    db_out: *?*lattice_database,
) lattice_error {
    db_out.* = null;
    if (bytes == null) return .err_invalid_arg;

    const zig_options = buildOpenOptionsV4(options) catch |err| return mapDatabaseError(err);

    const db = Database.deserialize(global_allocator, bytes[0..len], .{
        .config = zig_options.config,
        .lock = zig_options.lock,
    }) catch |err| return mapDatabaseError(err);

    return adoptDatabase(db, db_out);
}

pub export fn lattice_deserialize_borrowed(
    bytes: [*c]const u8,
    len: usize,
    options: ?*const lattice_open_options_v4,
    db_out: *?*lattice_database,
) lattice_error {
    db_out.* = null;
    if (bytes == null) return .err_invalid_arg;

    const zig_options = buildOpenOptionsV4(options) catch |err| return mapDatabaseError(err);

    const db = Database.deserialize(global_allocator, bytes[0..len], .{
        .config = zig_options.config,
        .lock = zig_options.lock,
        .borrow_bytes = true,
    }) catch |err| return mapDatabaseError(err);

    return adoptDatabase(db, db_out);
}

pub export fn lattice_free_bytes(bytes: [*c]u8, len: usize) void {
    if (bytes == null) return;
    global_allocator.free(bytes[0..len]);
}

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

// ============================================================================
// Stream Operations
// ============================================================================

fn streamPublishGetSequenceImpl(
    txn: ?*lattice_txn,
    stream: [*c]const u8,
    stream_len: usize,
    kind: [*c]const u8,
    kind_len: usize,
    payload: ?*const lattice_value,
    sequence_out: ?*u64,
) lattice_error {
    const txn_handle = acquireTxn(txn) orelse return .err_invalid_arg;
    defer releaseTxn(txn_handle);

    const stream_slice = cArgBytes(stream, stream_len) orelse return .err_invalid_arg;
    const kind_slice: ?[]const u8 = if (kind_len == 0)
        null
    else
        cArgBytes(kind, kind_len) orelse return .err_invalid_arg;
    const c_payload = payload orelse return .err_invalid_arg;

    var zig_payload = cValueToOwnedZigValue(c_payload, global_allocator) catch |err| {
        return mapValueConversionError(err);
    };
    defer zig_payload.deinit(global_allocator);

    const db_handle = txn_handle.db_handle;
    db_handle.mutex.lock();
    defer db_handle.mutex.unlock();
    const active_txn_result = ensureActiveTxnLocked(txn_handle);
    if (active_txn_result != .ok) return active_txn_result;
    if (rejectFakeStreamTxn(txn_handle)) |err| return err;

    const sequence = txn_handle.db_handle.db.publishStreamGetSequence(
        &txn_handle.txn,
        stream_slice,
        kind_slice,
        zig_payload,
    ) catch |err| return mapStreamMutationError(err);
    if (sequence_out) |out| out.* = sequence;

    return .ok;
}

pub export fn lattice_stream_publish(
    txn: ?*lattice_txn,
    stream: [*c]const u8,
    stream_len: usize,
    kind: [*c]const u8,
    kind_len: usize,
    payload: ?*const lattice_value,
) lattice_error {
    return streamPublishGetSequenceImpl(txn, stream, stream_len, kind, kind_len, payload, null);
}

pub export fn lattice_stream_publish_get_sequence(
    txn: ?*lattice_txn,
    stream: [*c]const u8,
    stream_len: usize,
    kind: [*c]const u8,
    kind_len: usize,
    payload: ?*const lattice_value,
    sequence_out: ?*u64,
) lattice_error {
    const out = sequence_out orelse return .err_invalid_arg;
    out.* = 0;
    return streamPublishGetSequenceImpl(txn, stream, stream_len, kind, kind_len, payload, out);
}

pub export fn lattice_stream_read(
    db: ?*lattice_database,
    stream: [*c]const u8,
    stream_len: usize,
    after_sequence: u64,
    limit: usize,
    timeout_ms: u32,
    batch_out: *?*lattice_stream_batch,
) lattice_error {
    batch_out.* = null;
    const db_handle = acquireDb(db) orelse return .err_invalid_arg;
    defer releaseDb(db_handle);
    const stream_slice = cArgBytes(stream, stream_len) orelse return .err_invalid_arg;

    // Registered as a reader for the whole call, waiting included, so close
    // wakes this call and waits for it instead of freeing the database under it.
    db_handle.mutex.lock();
    const active = ensureActiveDbLocked(db_handle);
    if (active != .ok) {
        db_handle.mutex.unlock();
        return active;
    }
    db_handle.stream_readers += 1;
    db_handle.mutex.unlock();
    defer {
        db_handle.mutex.lock();
        db_handle.stream_readers -= 1;
        db_handle.mutex.unlock();
    }

    var batch: StreamBatch = undefined;
    const read_result = readStreamWaiting(db_handle, stream_slice, after_sequence, limit, timeout_ms, &batch);
    if (read_result != .ok) return read_result;
    return wrapStreamBatch(batch, batch_out);
}

/// Read a stream for `lattice_stream_read`, waiting up to `timeout_ms` for
/// records to arrive.
///
/// `Database.readStream` can wait by itself, but it reads without the database
/// mutex and so races with commits. Here each attempt reads under the mutex,
/// like every other call, and the wait between attempts does not hold it, so a
/// writer can commit what the reader is waiting for. The commit epoch is
/// sampled before each attempt, so a commit that lands between an attempt and
/// the wait is not slept through. Close sets `closing`, which ends the wait;
/// the next attempt then finds the handle closed.
fn readStreamWaiting(
    db_handle: *DatabaseHandle,
    stream: []const u8,
    after_sequence: u64,
    limit: usize,
    timeout_ms: u32,
    batch_out: *StreamBatch,
) lattice_error {
    const db = db_handle.db;
    const deadline = compat.nanoTimestamp() + @as(i128, timeout_ms) * std.time.ns_per_ms;

    db.stream_mutex.lock();
    var epoch = db.stream_epoch;
    db.stream_mutex.unlock();

    while (true) {
        db_handle.mutex.lock();
        if (db_handle.header.state != .active) {
            db_handle.mutex.unlock();
            return .err_invalid_arg;
        }
        var batch = db.readStream(stream, after_sequence, limit, 0) catch |err| {
            db_handle.mutex.unlock();
            return mapDatabaseError(err);
        };
        db_handle.mutex.unlock();

        if (batch.records.len > 0 or compat.nanoTimestamp() >= deadline) {
            batch_out.* = batch;
            return .ok;
        }
        batch.deinit();

        db.stream_mutex.lock();
        while (db.stream_epoch == epoch and !db_handle.closing.load(.acquire)) {
            const now = compat.nanoTimestamp();
            if (now >= deadline) break;
            db.stream_cond.timedWait(&db.stream_mutex, @intCast(deadline - now)) catch {};
        }
        epoch = db.stream_epoch;
        db.stream_mutex.unlock();
    }
}

/// Hand a stream batch to the caller as a handle. Takes ownership of `batch`;
/// on any failure everything is released before returning.
fn wrapStreamBatch(batch: StreamBatch, batch_out: *?*lattice_stream_batch) lattice_error {
    var owned = batch;
    const c_values = global_allocator.alloc(lattice_value, owned.records.len) catch {
        owned.deinit();
        return .err_out_of_memory;
    };
    for (c_values) |*value| emptyCValue(value);

    for (owned.records, 0..) |record, i| {
        zigValueToOwnedCValue(record.payload, &c_values[i]) catch |err| {
            freeStreamCValues(c_values);
            owned.deinit();
            return mapValueConversionError(err);
        };
    }

    const handle = global_allocator.create(StreamBatchHandle) catch {
        freeStreamCValues(c_values);
        owned.deinit();
        return .err_out_of_memory;
    };
    handle.* = .{
        .header = HandleHeader.init(.stream_batch),
        .batch = owned,
        .c_values = c_values,
    };
    const register_result = registerHandle(StreamBatchHandle, handle);
    if (register_result != .ok) {
        freeStreamCValues(handle.c_values);
        handle.batch.deinit();
        global_allocator.destroy(handle);
        return register_result;
    }

    batch_out.* = toOpaque(lattice_stream_batch, handle);
    return .ok;
}

/// Free payloads converted for a stream batch. Entries never filled in are
/// empty values, which free nothing.
fn freeStreamCValues(values: []lattice_value) void {
    for (values) |*value| freeOwnedCValue(value);
    global_allocator.free(values);
}

pub export fn lattice_stream_batch_count(batch: ?*lattice_stream_batch) usize {
    const handle = toHandle(StreamBatchHandle, batch) orelse return 0;
    return handle.batch.records.len;
}

pub export fn lattice_stream_batch_get(
    batch: ?*lattice_stream_batch,
    index: usize,
    sequence_out: *u64,
    kind_out: *[*c]const u8,
    kind_len_out: *usize,
    payload_out: *?*const lattice_value,
) lattice_error {
    const handle = toHandle(StreamBatchHandle, batch) orelse return .err_invalid_arg;
    if (index >= handle.batch.records.len) return .err_invalid_arg;

    const record = handle.batch.records[index];
    sequence_out.* = record.sequence;
    kind_out.* = record.kind.ptr;
    kind_len_out.* = record.kind.len;
    payload_out.* = &handle.c_values[index];
    return .ok;
}

pub export fn lattice_stream_batch_free(batch: ?*lattice_stream_batch) void {
    const handle = toHandle(StreamBatchHandle, batch) orelse return;
    unregisterHandle(StreamBatchHandle, handle);
    freeStreamCValues(handle.c_values);
    handle.batch.deinit();
    global_allocator.destroy(handle);
}

pub export fn lattice_stream_get_offset(
    db: ?*lattice_database,
    stream: [*c]const u8,
    stream_len: usize,
    consumer: [*c]const u8,
    consumer_len: usize,
    exists_out: *bool,
    sequence_out: *u64,
) lattice_error {
    const db_handle = acquireDb(db) orelse return .err_invalid_arg;
    defer releaseDb(db_handle);
    const stream_slice = cArgBytes(stream, stream_len) orelse return .err_invalid_arg;
    const consumer_slice = cArgBytes(consumer, consumer_len) orelse return .err_invalid_arg;
    exists_out.* = false;
    sequence_out.* = 0;

    db_handle.mutex.lock();
    defer db_handle.mutex.unlock();
    const active = ensureActiveDbLocked(db_handle);
    if (active != .ok) return active;

    const value = db_handle.db.getStreamOffset(stream_slice, consumer_slice) catch |err| {
        return mapDatabaseError(err);
    };
    if (value) |sequence| {
        exists_out.* = true;
        sequence_out.* = sequence;
    }
    return .ok;
}

pub export fn lattice_stream_get_last_sequence(
    db: ?*lattice_database,
    stream: [*c]const u8,
    stream_len: usize,
    sequence_out: *u64,
) lattice_error {
    const db_handle = acquireDb(db) orelse return .err_invalid_arg;
    defer releaseDb(db_handle);
    const stream_slice = cArgBytes(stream, stream_len) orelse return .err_invalid_arg;
    sequence_out.* = 0;

    db_handle.mutex.lock();
    defer db_handle.mutex.unlock();
    const active = ensureActiveDbLocked(db_handle);
    if (active != .ok) return active;

    sequence_out.* = db_handle.db.getStreamLastSequence(stream_slice) catch |err| {
        return mapDatabaseError(err);
    };
    return .ok;
}

pub export fn lattice_stream_set_offset(
    txn: ?*lattice_txn,
    stream: [*c]const u8,
    stream_len: usize,
    consumer: [*c]const u8,
    consumer_len: usize,
    sequence: u64,
) lattice_error {
    const txn_handle = acquireTxn(txn) orelse return .err_invalid_arg;
    defer releaseTxn(txn_handle);
    const stream_slice = cArgBytes(stream, stream_len) orelse return .err_invalid_arg;
    const consumer_slice = cArgBytes(consumer, consumer_len) orelse return .err_invalid_arg;

    const db_handle = txn_handle.db_handle;
    db_handle.mutex.lock();
    defer db_handle.mutex.unlock();
    const active_txn_result = ensureActiveTxnLocked(txn_handle);
    if (active_txn_result != .ok) return active_txn_result;
    if (rejectFakeStreamTxn(txn_handle)) |err| return err;

    txn_handle.db_handle.db.setStreamOffset(
        &txn_handle.txn,
        stream_slice,
        consumer_slice,
        sequence,
    ) catch |err| return mapStreamMutationError(err);
    return .ok;
}

pub export fn lattice_stream_trim(
    txn: ?*lattice_txn,
    stream: [*c]const u8,
    stream_len: usize,
    through_sequence: u64,
) lattice_error {
    const txn_handle = acquireTxn(txn) orelse return .err_invalid_arg;
    defer releaseTxn(txn_handle);
    const stream_slice = cArgBytes(stream, stream_len) orelse return .err_invalid_arg;

    const db_handle = txn_handle.db_handle;
    db_handle.mutex.lock();
    defer db_handle.mutex.unlock();
    const active_txn_result = ensureActiveTxnLocked(txn_handle);
    if (active_txn_result != .ok) return active_txn_result;
    if (rejectFakeStreamTxn(txn_handle)) |err| return err;

    txn_handle.db_handle.db.trimStream(
        &txn_handle.txn,
        stream_slice,
        through_sequence,
    ) catch |err| return mapStreamMutationError(err);
    return .ok;
}

/// Set a vector on a node
pub export fn lattice_node_set_vector(
    txn: ?*lattice_txn,
    node_id: lattice_node_id,
    key: [*c]const u8,
    vector: [*c]const f32,
    dimensions: u32,
) lattice_error {
    const txn_handle = acquireTxn(txn) orelse return .err_invalid_arg;
    defer releaseTxn(txn_handle);

    // A node has one vector, which Cypher reads as `n.embedding`. Any other
    // key used to be accepted and ignored, so a second vector under another
    // name silently replaced the first.
    if (cStrToSlice(key)) |key_slice| {
        if (key_slice.len != 0 and !std.mem.eql(u8, key_slice, "embedding")) return .err_unsupported;
    }

    // Convert C pointer to Zig slice
    const vector_slice = cEmbeddingVectorToSlice(vector, dimensions) orelse return .err_invalid_arg;

    const active_txn_result = lockActiveTxnDb(txn_handle);
    if (active_txn_result != .ok) return active_txn_result;
    defer unlockTxnDb(txn_handle);

    if (txn_handle.txn.mode == .read_only) return .err_read_only;

    const txn_ptr: ?*Transaction = if (txn_handle.txn.id != 0) &txn_handle.txn else null;

    txn_handle.db_handle.db.setNodeVectorInTxn(txn_ptr, node_id, vector_slice) catch |err| {
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

    const txn_handle = acquireTxn(txn) orelse return .err_invalid_arg;
    defer releaseTxn(txn_handle);
    if (nodes == null or count == 0 or node_ids_out == null) return .err_invalid_arg;

    const active_txn_result = lockActiveTxnDb(txn_handle);
    if (active_txn_result != .ok) return active_txn_result;
    defer unlockTxnDb(txn_handle);

    if (txn_handle.txn.mode == .read_only) return .err_read_only;

    const txn_ptr: ?*Transaction = if (txn_handle.txn.id != 0) &txn_handle.txn else null;

    var i: u32 = 0;
    while (i < count) : (i += 1) {
        const spec = nodes[i];

        // Validate
        const label_slice = cStrToSlice(spec.label) orelse return .err_invalid_arg;
        const vector_slice = cEmbeddingVectorToSlice(spec.vector, spec.dimensions) orelse return .err_invalid_arg;

        // Create node. An empty label means no label, as in lattice_node_create.
        const one_label = [_][]const u8{label_slice};
        const labels: []const []const u8 = if (label_slice.len == 0) &.{} else &one_label;
        const node_id = txn_handle.db_handle.db.createNode(txn_ptr, labels) catch |err| {
            return mapAnyError(err);
        };

        // Set vector
        txn_handle.db_handle.db.setNodeVectorInTxn(txn_ptr, node_id, vector_slice) catch |err| {
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
    const db_handle = acquireDb(db) orelse return .err_invalid_arg;
    defer releaseDb(db_handle);

    if (k == 0) return .err_invalid_arg;

    // Convert C pointer to Zig slice
    const query_vector = cEmbeddingVectorToSlice(vector, dimensions) orelse return .err_invalid_arg;

    db_handle.mutex.lock();
    defer db_handle.mutex.unlock();
    const active = ensureActiveDbLocked(db_handle);
    if (active != .ok) return active;

    // Perform the search
    const ef = if (ef_search == 0) null else ef_search;
    const results = db_handle.db.vectorSearch(query_vector, k, ef) catch |err| {
        return mapDatabaseError(err);
    };

    // Create result handle
    const result_handle = global_allocator.create(VectorResultHandle) catch {
        global_allocator.free(results);
        return .err_out_of_memory;
    };
    result_handle.* = VectorResultHandle{
        .header = HandleHeader.init(.vector_result),
        .results = results,
        .count = results.len,
    };
    const register_result = registerHandle(VectorResultHandle, result_handle);
    if (register_result != .ok) {
        global_allocator.free(results);
        global_allocator.destroy(result_handle);
        return register_result;
    }

    result_out.* = toOpaque(lattice_vector_result, result_handle);
    return .ok;
}

pub export fn lattice_vector_search_txn(
    txn: ?*lattice_txn,
    vector: [*c]const f32,
    dimensions: u32,
    k: u32,
    ef_search: u16,
    result_out: *?*lattice_vector_result,
) lattice_error {
    const txn_handle = acquireTxn(txn) orelse return .err_invalid_arg;
    defer releaseTxn(txn_handle);

    if (k == 0) return .err_invalid_arg;

    const query_vector = cEmbeddingVectorToSlice(vector, dimensions) orelse return .err_invalid_arg;
    const ef = if (ef_search == 0) null else ef_search;

    const active_txn_result = lockActiveTxnDb(txn_handle);
    if (active_txn_result != .ok) return active_txn_result;
    defer unlockTxnDb(txn_handle);

    const txn_ptr: ?*Transaction = if (txn_handle.txn.id != 0) &txn_handle.txn else null;
    const results = txn_handle.db_handle.db.vectorSearchInTxn(txn_ptr, query_vector, k, ef) catch |err| {
        return mapDatabaseError(err);
    };

    const result_handle = global_allocator.create(VectorResultHandle) catch {
        global_allocator.free(results);
        return .err_out_of_memory;
    };
    result_handle.* = VectorResultHandle{
        .header = HandleHeader.init(.vector_result),
        .results = results,
        .count = results.len,
    };
    const register_result = registerHandle(VectorResultHandle, result_handle);
    if (register_result != .ok) {
        global_allocator.free(results);
        global_allocator.destroy(result_handle);
        return register_result;
    }

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
    unregisterHandle(VectorResultHandle, result_handle);

    // Free the results slice (allocated by Database.vectorSearch)
    global_allocator.free(result_handle.results);

    // Free the handle itself
    global_allocator.destroy(result_handle);
}

// ============================================================================
// Full-Text Search Operations
// ============================================================================

/// Index a text document for full-text search.
/// Search one declared index using BM25 scoring.
///
/// `lattice_fts_index` used to put text straight into a single index shared by
/// every node. It is gone: text is indexed by declaring an index over the
/// property that holds it, with `lattice_node_fts_index_create`, after which
/// writes maintain it.
pub export fn lattice_fts_search(
    db: ?*lattice_database,
    label: [*c]const u8,
    property: [*c]const u8,
    query_text: [*c]const u8,
    query_len: usize,
    limit: u32,
    result_out: *?*lattice_fts_result,
) lattice_error {
    const db_handle = acquireDb(db) orelse return .err_invalid_arg;
    defer releaseDb(db_handle);
    const label_slice = cStrToSlice(label) orelse return .err_invalid_arg;
    const property_slice = cStrToSlice(property) orelse return .err_invalid_arg;

    if (query_text == null or query_len == 0 or limit == 0) return .err_invalid_arg;

    const query_slice = query_text[0..query_len];

    db_handle.mutex.lock();
    defer db_handle.mutex.unlock();
    const active_db_result = ensureActiveDbLocked(db_handle);
    if (active_db_result != .ok) return active_db_result;

    const results = db_handle.db.ftsSearchIndex(
        .node,
        label_slice,
        property_slice,
        query_slice,
        limit,
    ) catch |err| {
        return mapDatabaseError(err);
    };

    return wrapFtsResults(db_handle, results, result_out);
}

pub export fn lattice_fts_search_txn(
    txn: ?*lattice_txn,
    label: [*c]const u8,
    property: [*c]const u8,
    query_text: [*c]const u8,
    query_len: usize,
    limit: u32,
    result_out: *?*lattice_fts_result,
) lattice_error {
    const txn_handle = acquireTxn(txn) orelse return .err_invalid_arg;
    defer releaseTxn(txn_handle);
    const label_slice = cStrToSlice(label) orelse return .err_invalid_arg;
    const property_slice = cStrToSlice(property) orelse return .err_invalid_arg;
    if (query_text == null or query_len == 0 or limit == 0) return .err_invalid_arg;

    const query_slice = query_text[0..query_len];

    const active_txn_result = lockActiveTxnDb(txn_handle);
    if (active_txn_result != .ok) return active_txn_result;
    defer unlockTxnDb(txn_handle);

    const txn_ptr: ?*Transaction = if (txn_handle.txn.id != 0) &txn_handle.txn else null;
    const results = txn_handle.db_handle.db.ftsSearchIndexInTxn(
        txn_ptr,
        .node,
        label_slice,
        property_slice,
        query_slice,
        limit,
    ) catch |err| {
        return mapDatabaseError(err);
    };

    return wrapFtsResults(txn_handle.db_handle, results, result_out);
}

pub export fn lattice_fts_search_fuzzy(
    db: ?*lattice_database,
    label: [*c]const u8,
    property: [*c]const u8,
    query_text: [*c]const u8,
    query_len: usize,
    limit: u32,
    max_distance: u32,
    min_term_length: u32,
    result_out: *?*lattice_fts_result,
) lattice_error {
    const db_handle = acquireDb(db) orelse return .err_invalid_arg;
    defer releaseDb(db_handle);
    const label_slice = cStrToSlice(label) orelse return .err_invalid_arg;
    const property_slice = cStrToSlice(property) orelse return .err_invalid_arg;
    if (query_text == null or query_len == 0 or limit == 0) return .err_invalid_arg;

    const query_slice = query_text[0..query_len];
    const eff_max_dist: u32 = if (max_distance == 0) 2 else max_distance;
    const eff_min_len: u32 = if (min_term_length == 0) 4 else min_term_length;

    db_handle.mutex.lock();
    defer db_handle.mutex.unlock();
    const active_db_result = ensureActiveDbLocked(db_handle);
    if (active_db_result != .ok) return active_db_result;

    const results = db_handle.db.ftsSearchIndexFuzzy(
        null,
        .node,
        label_slice,
        property_slice,
        query_slice,
        limit,
        eff_max_dist,
        eff_min_len,
    ) catch |err| {
        return mapDatabaseError(err);
    };

    return wrapFtsResults(db_handle, results, result_out);
}

pub export fn lattice_fts_search_fuzzy_txn(
    txn: ?*lattice_txn,
    label: [*c]const u8,
    property: [*c]const u8,
    query_text: [*c]const u8,
    query_len: usize,
    limit: u32,
    max_distance: u32,
    min_term_length: u32,
    result_out: *?*lattice_fts_result,
) lattice_error {
    const txn_handle = acquireTxn(txn) orelse return .err_invalid_arg;
    defer releaseTxn(txn_handle);
    const label_slice = cStrToSlice(label) orelse return .err_invalid_arg;
    const property_slice = cStrToSlice(property) orelse return .err_invalid_arg;
    if (query_text == null or query_len == 0 or limit == 0) return .err_invalid_arg;

    const query_slice = query_text[0..query_len];
    const eff_max_dist: u32 = if (max_distance == 0) 2 else max_distance;
    const eff_min_len: u32 = if (min_term_length == 0) 4 else min_term_length;

    const active_txn_result = lockActiveTxnDb(txn_handle);
    if (active_txn_result != .ok) return active_txn_result;
    defer unlockTxnDb(txn_handle);

    const txn_ptr: ?*Transaction = if (txn_handle.txn.id != 0) &txn_handle.txn else null;
    const results = txn_handle.db_handle.db.ftsSearchIndexFuzzy(
        txn_ptr,
        .node,
        label_slice,
        property_slice,
        query_slice,
        limit,
        eff_max_dist,
        eff_min_len,
    ) catch |err| {
        return mapDatabaseError(err);
    };

    return wrapFtsResults(txn_handle.db_handle, results, result_out);
}

/// Hand a result set to the caller as a counted child handle.
///
/// The four search entry points differed only in which search they called, and
/// each carried its own copy of this bookkeeping. One of them getting the
/// unwinding wrong on an allocation failure would have leaked a result set or
/// left the child count too high.
fn wrapFtsResults(
    db_handle: *DatabaseHandle,
    results: []FtsSearchResult,
    result_out: *?*lattice_fts_result,
) lattice_error {
    const result_handle = global_allocator.create(FtsResultHandle) catch {
        db_handle.db.freeFtsSearchResults(results);
        return .err_out_of_memory;
    };
    result_handle.* = FtsResultHandle{
        .header = HandleHeader.init(.fts_result),
        .results = results,
        .count = results.len,
        .db_handle = db_handle,
        .counted_child = true,
    };
    const register_result = registerHandle(FtsResultHandle, result_handle);
    if (register_result != .ok) {
        db_handle.db.freeFtsSearchResults(results);
        global_allocator.destroy(result_handle);
        return register_result;
    }
    db_handle.active_children += 1;

    result_out.* = toOpaque(lattice_fts_result, result_handle);
    return .ok;
}

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
    const db_handle = result_handle.db_handle;
    db_handle.mutex.lock();
    defer db_handle.mutex.unlock();
    unregisterHandle(FtsResultHandle, result_handle);
    releaseDbChildLocked(db_handle, &result_handle.counted_child);

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
    const txn_handle = acquireTxn(txn) orelse return .err_invalid_arg;
    defer releaseTxn(txn_handle);

    const type_slice = cStrToSlice(edge_type) orelse return .err_invalid_arg;

    const active_txn_result = lockActiveTxnDb(txn_handle);
    if (active_txn_result != .ok) return active_txn_result;
    defer unlockTxnDb(txn_handle);

    if (txn_handle.txn.mode == .read_only) return .err_read_only;

    // Pass transaction if it's a real one (id != 0)
    const txn_ptr: ?*Transaction = if (txn_handle.txn.id != 0) &txn_handle.txn else null;
    const edge_id = txn_handle.db_handle.db.createEdgeAndGetId(txn_ptr, source, target, type_slice) catch |err| {
        // err_not_found when either end does not exist.
        return mapGraphWriteError(err);
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
    const txn_handle = acquireTxn(txn) orelse return .err_invalid_arg;
    defer releaseTxn(txn_handle);

    const type_slice = cStrToSlice(edge_type) orelse return .err_invalid_arg;

    const active_txn_result = lockActiveTxnDb(txn_handle);
    if (active_txn_result != .ok) return active_txn_result;
    defer unlockTxnDb(txn_handle);

    if (txn_handle.txn.mode == .read_only) return .err_read_only;

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
    const txn_handle = acquireTxn(txn) orelse return .err_invalid_arg;
    defer releaseTxn(txn_handle);

    const key_slice = cStrToSlice(key) orelse return .err_invalid_arg;
    const c_val = value orelse return .err_invalid_arg;
    var zig_value = cValueToOwnedZigValue(c_val, global_allocator) catch |err| return mapValueConversionError(err);
    defer zig_value.deinit(global_allocator);

    const active_txn_result = lockActiveTxnDb(txn_handle);
    if (active_txn_result != .ok) return active_txn_result;
    defer unlockTxnDb(txn_handle);

    if (txn_handle.txn.mode == .read_only) return .err_read_only;

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
    const txn_handle = acquireTxn(txn) orelse return .err_invalid_arg;
    defer releaseTxn(txn_handle);
    const key_slice = cStrToSlice(key) orelse return .err_invalid_arg;

    const active_txn_result = lockActiveTxnDb(txn_handle);
    if (active_txn_result != .ok) return active_txn_result;
    defer unlockTxnDb(txn_handle);

    const txn_ptr: ?*Transaction = if (txn_handle.txn.id != 0) &txn_handle.txn else null;

    const maybe_value = txn_handle.db_handle.db.getEdgePropertyByIdInTxn(txn_ptr, edge_id, key_slice) catch |err| {
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
    const txn_handle = acquireTxn(txn) orelse return .err_invalid_arg;
    defer releaseTxn(txn_handle);

    const key_slice = cStrToSlice(key) orelse return .err_invalid_arg;

    const active_txn_result = lockActiveTxnDb(txn_handle);
    if (active_txn_result != .ok) return active_txn_result;
    defer unlockTxnDb(txn_handle);

    if (txn_handle.txn.mode == .read_only) return .err_read_only;

    const txn_ptr: ?*Transaction = if (txn_handle.txn.id != 0) &txn_handle.txn else null;
    txn_handle.db_handle.db.removeEdgePropertyById(txn_ptr, edge_id, key_slice) catch |err| {
        return mapDatabaseError(err);
    };

    return .ok;
}

pub export fn lattice_edge_property_index_create(
    db: ?*lattice_database,
    edge_type: [*c]const u8,
    property: [*c]const u8,
) lattice_error {
    const db_handle = acquireDb(db) orelse return .err_invalid_arg;
    defer releaseDb(db_handle);
    const type_slice = cStrToSlice(edge_type) orelse return .err_invalid_arg;
    const property_slice = cStrToSlice(property) orelse return .err_invalid_arg;

    db_handle.mutex.lock();
    defer db_handle.mutex.unlock();
    const active = ensureActiveDbLocked(db_handle);
    if (active != .ok) return active;
    db_handle.db.createEdgePropertyIndex(type_slice, property_slice) catch |err| return mapDatabaseError(err);
    return .ok;
}

pub export fn lattice_edge_property_index_drop(
    db: ?*lattice_database,
    edge_type: [*c]const u8,
    property: [*c]const u8,
) lattice_error {
    const db_handle = acquireDb(db) orelse return .err_invalid_arg;
    defer releaseDb(db_handle);
    const type_slice = cStrToSlice(edge_type) orelse return .err_invalid_arg;
    const property_slice = cStrToSlice(property) orelse return .err_invalid_arg;

    db_handle.mutex.lock();
    defer db_handle.mutex.unlock();
    const active = ensureActiveDbLocked(db_handle);
    if (active != .ok) return active;
    db_handle.db.dropEdgePropertyIndex(type_slice, property_slice) catch |err| return mapDatabaseError(err);
    return .ok;
}

pub export fn lattice_edges_find_by_type_property(
    txn: ?*lattice_txn,
    edge_type: [*c]const u8,
    property: [*c]const u8,
    value: ?*const lattice_value,
    limit: usize,
    edge_ids_out: *?[*]lattice_edge_id,
    count_out: *usize,
) lattice_error {
    const txn_handle = acquireTxn(txn) orelse return .err_invalid_arg;
    defer releaseTxn(txn_handle);
    const type_slice = cStrToSlice(edge_type) orelse return .err_invalid_arg;
    const property_slice = cStrToSlice(property) orelse return .err_invalid_arg;
    const c_value = value orelse return .err_invalid_arg;
    if (limit == 0) return .err_invalid_arg;
    edge_ids_out.* = null;
    count_out.* = 0;

    var zig_value = cValueToOwnedZigValue(c_value, global_allocator) catch |err| return mapValueConversionError(err);
    defer zig_value.deinit(global_allocator);
    const active = lockActiveTxnDb(txn_handle);
    if (active != .ok) return active;
    defer unlockTxnDb(txn_handle);

    const txn_ptr: ?*Transaction = if (txn_handle.txn.id != 0) &txn_handle.txn else null;
    const owned = txn_handle.db_handle.db.findEdgesByTypeProperty(
        txn_ptr,
        type_slice,
        property_slice,
        zig_value,
        limit,
    ) catch |err| return mapDatabaseError(err);
    defer txn_handle.db_handle.db.allocator.free(owned);
    if (owned.len == 0) return .ok;

    const output = global_allocator.dupe(lattice_edge_id, owned) catch return .err_out_of_memory;
    edge_ids_out.* = output.ptr;
    count_out.* = output.len;
    return .ok;
}

pub export fn lattice_free_edge_ids(edge_ids: ?[*]lattice_edge_id, count: usize) void {
    const ptr = edge_ids orelse return;
    if (count == 0) return;
    global_allocator.free(ptr[0..count]);
}

fn terminatedEdgeTypes(arena: std.mem.Allocator, edges: []const Database.EdgeInfo) ![][:0]const u8 {
    const out = try arena.alloc([:0]const u8, edges.len);
    for (edges, 0..) |edge, i| {
        if (i > 0 and std.mem.eql(u8, out[i - 1], edge.edge_type)) {
            out[i] = out[i - 1];
        } else {
            out[i] = try arena.dupeZ(u8, edge.edge_type);
        }
    }
    return out;
}

fn finishEdgeResultHandleLocked(
    txn_handle: *TxnHandle,
    edges: []Database.EdgeInfo,
    result_out: *?*lattice_edge_result,
) lattice_error {
    var type_arena = std.heap.ArenaAllocator.init(global_allocator);
    const edge_types_z = terminatedEdgeTypes(type_arena.allocator(), edges) catch {
        type_arena.deinit();
        txn_handle.db_handle.db.freeEdgeInfos(edges);
        return .err_out_of_memory;
    };
    const result_handle = global_allocator.create(EdgeResultHandle) catch {
        type_arena.deinit();
        txn_handle.db_handle.db.freeEdgeInfos(edges);
        return .err_out_of_memory;
    };

    result_handle.* = EdgeResultHandle{
        .header = HandleHeader.init(.edge_result),
        .edges = edges,
        .edge_types_z = edge_types_z,
        .type_arena = type_arena,
        .count = edges.len,
        .db_handle = txn_handle.db_handle,
        .counted_child = true,
    };
    const register_result = registerHandle(EdgeResultHandle, result_handle);
    if (register_result != .ok) {
        result_handle.type_arena.deinit();
        txn_handle.db_handle.db.freeEdgeInfos(edges);
        global_allocator.destroy(result_handle);
        return register_result;
    }
    txn_handle.db_handle.active_children += 1;

    result_out.* = toOpaque(lattice_edge_result, result_handle);
    return .ok;
}

/// Get all outgoing edges from a node
pub export fn lattice_edge_get_outgoing(
    txn: ?*lattice_txn,
    node_id: lattice_node_id,
    result_out: *?*lattice_edge_result,
) lattice_error {
    result_out.* = null;

    const txn_handle = acquireTxn(txn) orelse return .err_invalid_arg;
    defer releaseTxn(txn_handle);

    const db_handle = txn_handle.db_handle;
    db_handle.mutex.lock();
    defer db_handle.mutex.unlock();
    const active_txn_result = ensureActiveTxnLocked(txn_handle);
    if (active_txn_result != .ok) return active_txn_result;

    const txn_ptr: ?*Transaction = if (txn_handle.txn.id != 0) &txn_handle.txn else null;

    const edges = txn_handle.db_handle.db.getOutgoingEdgesInTxn(txn_ptr, node_id, 0) catch |err| {
        return mapAnyError(err);
    };

    return finishEdgeResultHandleLocked(txn_handle, edges, result_out);
}

/// Get all incoming edges to a node
pub export fn lattice_edge_get_incoming(
    txn: ?*lattice_txn,
    node_id: lattice_node_id,
    result_out: *?*lattice_edge_result,
) lattice_error {
    result_out.* = null;

    const txn_handle = acquireTxn(txn) orelse return .err_invalid_arg;
    defer releaseTxn(txn_handle);

    const db_handle = txn_handle.db_handle;
    db_handle.mutex.lock();
    defer db_handle.mutex.unlock();
    const active_txn_result = ensureActiveTxnLocked(txn_handle);
    if (active_txn_result != .ok) return active_txn_result;

    const txn_ptr: ?*Transaction = if (txn_handle.txn.id != 0) &txn_handle.txn else null;

    const edges = txn_handle.db_handle.db.getIncomingEdgesInTxn(txn_ptr, node_id, 0) catch |err| {
        return mapAnyError(err);
    };

    return finishEdgeResultHandleLocked(txn_handle, edges, result_out);
}

/// Get outgoing edges from a node filtered by type
pub export fn lattice_edge_get_outgoing_by_type(
    txn: ?*lattice_txn,
    node_id: lattice_node_id,
    edge_type: [*c]const u8,
    limit: usize,
    result_out: *?*lattice_edge_result,
) lattice_error {
    result_out.* = null;

    const txn_handle = acquireTxn(txn) orelse return .err_invalid_arg;
    defer releaseTxn(txn_handle);
    const type_slice = cStrToSlice(edge_type) orelse return .err_invalid_arg;

    const db_handle = txn_handle.db_handle;
    db_handle.mutex.lock();
    defer db_handle.mutex.unlock();
    const active_txn_result = ensureActiveTxnLocked(txn_handle);
    if (active_txn_result != .ok) return active_txn_result;

    const txn_ptr: ?*Transaction = if (txn_handle.txn.id != 0) &txn_handle.txn else null;
    const edges = txn_handle.db_handle.db.getOutgoingEdgesByTypeInTxn(txn_ptr, node_id, type_slice, limit) catch |err| {
        return mapAnyError(err);
    };

    return finishEdgeResultHandleLocked(txn_handle, edges, result_out);
}

/// Get incoming edges to a node filtered by type
pub export fn lattice_edge_get_incoming_by_type(
    txn: ?*lattice_txn,
    node_id: lattice_node_id,
    edge_type: [*c]const u8,
    limit: usize,
    result_out: *?*lattice_edge_result,
) lattice_error {
    result_out.* = null;

    const txn_handle = acquireTxn(txn) orelse return .err_invalid_arg;
    defer releaseTxn(txn_handle);
    const type_slice = cStrToSlice(edge_type) orelse return .err_invalid_arg;

    const db_handle = txn_handle.db_handle;
    db_handle.mutex.lock();
    defer db_handle.mutex.unlock();
    const active_txn_result = ensureActiveTxnLocked(txn_handle);
    if (active_txn_result != .ok) return active_txn_result;

    const txn_ptr: ?*Transaction = if (txn_handle.txn.id != 0) &txn_handle.txn else null;
    const edges = txn_handle.db_handle.db.getIncomingEdgesByTypeInTxn(txn_ptr, node_id, type_slice, limit) catch |err| {
        return mapAnyError(err);
    };

    return finishEdgeResultHandleLocked(txn_handle, edges, result_out);
}

/// Scan visible native edges, optionally filtered by type
pub export fn lattice_edge_scan(
    txn: ?*lattice_txn,
    edge_type: [*c]const u8,
    limit: usize,
    result_out: *?*lattice_edge_result,
) lattice_error {
    result_out.* = null;

    const txn_handle = acquireTxn(txn) orelse return .err_invalid_arg;
    defer releaseTxn(txn_handle);
    const type_slice: ?[]const u8 = if (edge_type == null)
        null
    else
        cStrToSlice(edge_type) orelse return .err_invalid_arg;

    const db_handle = txn_handle.db_handle;
    db_handle.mutex.lock();
    defer db_handle.mutex.unlock();
    const active_txn_result = ensureActiveTxnLocked(txn_handle);
    if (active_txn_result != .ok) return active_txn_result;

    const txn_ptr: ?*Transaction = if (txn_handle.txn.id != 0) &txn_handle.txn else null;
    const edges = txn_handle.db_handle.db.scanEdgesInTxn(txn_ptr, type_slice, limit) catch |err| {
        return mapAnyError(err);
    };

    return finishEdgeResultHandleLocked(txn_handle, edges, result_out);
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
    const edge_type = result_handle.edge_types_z[index];
    edge_type_out.* = edge_type.ptr;
    edge_type_len_out.* = @intCast(edge_type.len);

    return .ok;
}

/// Free an edge result set
pub export fn lattice_edge_result_free(result: ?*lattice_edge_result) void {
    const result_handle = toHandle(EdgeResultHandle, result) orelse return;
    const db_handle = result_handle.db_handle;
    db_handle.mutex.lock();
    defer db_handle.mutex.unlock();
    unregisterHandle(EdgeResultHandle, result_handle);
    releaseDbChildLocked(db_handle, &result_handle.counted_child);

    // Free the edge infos through database
    result_handle.db_handle.db.freeEdgeInfos(result_handle.edges);
    result_handle.type_arena.deinit();

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

    const db_handle = acquireDb(db) orelse return .err_invalid_arg;
    defer releaseDb(db_handle);
    const cypher_slice = cStrToSlice(cypher) orelse return .err_invalid_arg;

    db_handle.mutex.lock();
    defer db_handle.mutex.unlock();
    const active_db_result = ensureActiveDbLocked(db_handle);
    if (active_db_result != .ok) return active_db_result;

    // Copy cypher string
    const cypher_copy = global_allocator.dupe(u8, cypher_slice) catch return .err_out_of_memory;

    // Create query handle
    const query_handle = global_allocator.create(QueryHandle) catch {
        global_allocator.free(cypher_copy);
        return .err_out_of_memory;
    };

    query_handle.* = QueryHandle.init(cypher_copy, true, db_handle);
    const register_result = registerHandle(QueryHandle, query_handle);
    if (register_result != .ok) {
        query_handle.deinit();
        global_allocator.free(cypher_copy);
        global_allocator.destroy(query_handle);
        return register_result;
    }
    db_handle.active_children += 1;

    // Parse now, once. A query that does not parse still prepares: its
    // error is reported by execute, with its location, as it always was.
    query_handle.prepared = db_handle.db.prepareCached(cypher_copy);

    query_out.* = toOpaque(lattice_query, query_handle);
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

    const vector_slice = cEmbeddingVectorToSlice(vector, dimensions) orelse return .err_invalid_arg;

    const owned_value = (PropertyValue{ .vector_val = vector_slice }).clone(global_allocator) catch return .err_out_of_memory;
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
    const txn_handle = acquireTxn(txn) orelse return .err_invalid_arg;
    defer releaseTxn(txn_handle);
    // An ended transaction has no database of its own; the active check below
    // reports it as aborted, as it did when finished handles were retained.
    if (txn_handle != &retired_txn_handle and query_handle.db_handle != txn_handle.db_handle) {
        return .err_invalid_arg;
    }

    const db_handle = query_handle.db_handle;
    db_handle.mutex.lock();
    defer db_handle.mutex.unlock();
    const active_db_result = ensureActiveDbLocked(db_handle);
    if (active_db_result != .ok) return active_db_result;
    const active_txn_result = ensureActiveTxnLocked(txn_handle);
    if (active_txn_result != .ok) return active_txn_result;

    const txn_ptr: ?*Transaction = if (txn_handle.txn.id != 0) &txn_handle.txn else null;

    // Clear any diagnostics from previous executions.
    query_handle.clearLastError();

    // Execute the query (with or without parameters), retaining structured diagnostics.
    const params: ?*const std.StringHashMap(PropertyValue) = if (query_handle.parameters.count() > 0) &query_handle.parameters else null;
    var detailed = if (query_handle.prepared) |prepared|
        db_handle.db.executePreparedDetailedInTxn(txn_ptr, prepared, params) catch |err| {
            return mapQueryError(err);
        }
    else if (query_handle.parameters.count() > 0)
        query_handle.db_handle.db.queryWithParamsDetailedInTxn(txn_ptr, query_handle.cypher, &query_handle.parameters) catch |err| {
            return mapQueryError(err);
        }
    else
        query_handle.db_handle.db.queryDetailedInTxn(txn_ptr, query_handle.cypher) catch |err| {
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
        .header = HandleHeader.init(.result),
        .result = result,
        .column_names_z = column_names_z,
        .current_row = 0,
        .started = false,
        .borrowed_value_arena = std.heap.ArenaAllocator.init(global_allocator),
    };
    const register_result = registerHandle(ResultHandle, result_handle);
    if (register_result != .ok) {
        result_handle.borrowed_value_arena.deinit();
        for (column_names_z) |name| {
            global_allocator.free(name);
        }
        global_allocator.free(column_names_z);
        var mutable_result = result;
        mutable_result.deinit();
        global_allocator.destroy(result_handle);
        return register_result;
    }

    result_out.* = toOpaque(lattice_result, result_handle);
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
/// Whether executing this query could change the database.
///
/// Bindings call this to choose between a read-only and a read-write
/// transaction. Returns false for a query that does not parse; execution will
/// report the parse error, and a read transaction is the weaker thing to have
/// opened in the meantime.
pub export fn lattice_query_writes(query: ?*lattice_query) bool {
    const query_handle = toHandle(QueryHandle, query) orelse return false;

    query_handle.db_handle.mutex.lock();
    defer query_handle.db_handle.mutex.unlock();
    if (ensureActiveDbLocked(query_handle.db_handle) != .ok) return false;

    if (query_handle.prepared) |prepared| return prepared.writes();
    return query_handle.db_handle.db.queryWrites(query_handle.cypher);
}

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
    const db_handle = query_handle.db_handle;
    db_handle.mutex.lock();
    defer db_handle.mutex.unlock();
    unregisterHandle(QueryHandle, query_handle);
    releaseDbChildLocked(db_handle, &query_handle.counted_child);

    // The database cannot have closed: close refuses while a query is open.
    if (query_handle.prepared) |prepared| {
        db_handle.db.releasePrepared(prepared);
        query_handle.prepared = null;
    }

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
    unregisterHandle(ResultHandle, result_handle);

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
    const db_handle = acquireDb(db) orelse return .err_invalid_arg;
    defer releaseDb(db_handle);

    db_handle.mutex.lock();
    defer db_handle.mutex.unlock();
    const active = ensureActiveDbLocked(db_handle);
    if (active != .ok) return active;

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
    const db_handle = acquireDb(db) orelse return .err_invalid_arg;
    defer releaseDb(db_handle);

    db_handle.mutex.lock();
    defer db_handle.mutex.unlock();
    const active = ensureActiveDbLocked(db_handle);
    if (active != .ok) return active;

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
    return "0.16.0";
}

/// Get error message for error code
pub export fn lattice_error_message(code: lattice_error) [*c]const u8 {
    const known = checkedEnum(lattice_error, &code) orelse return "Unknown error";
    return switch (known) {
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
        .err_value_too_large => "Value too large for database page size",
        .err_database_locked => "Database is open in another process",
        .err_has_relationships => "Node still has relationships; delete them first or detach-delete the node",
        .err_config_mismatch => "Open options contradict the vector index configuration stored in the database",
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
    if (!isValidEmbeddingDimensions(dimensions)) return .err_invalid_arg;

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

    const api_format_c = checkedEnum(lattice_embedding_api_format, &cfg.api_format) orelse return .err_invalid_arg;
    const api_format: embedding_mod.ApiFormat = switch (api_format_c) {
        .ollama => .ollama,
        .openai => .openai,
    };

    const api_key_borrowed: ?[]const u8 = cStrToSlice(cfg.api_key);

    const timeout = if (cfg.timeout_ms == 0) @as(u32, 30_000) else cfg.timeout_ms;

    var config_arena = std.heap.ArenaAllocator.init(global_allocator);
    const owned = config_arena.allocator();
    const endpoint_copy = owned.dupe(u8, endpoint) catch {
        config_arena.deinit();
        return .err_out_of_memory;
    };
    const model_copy = owned.dupe(u8, model) catch {
        config_arena.deinit();
        return .err_out_of_memory;
    };
    const api_key_copy: ?[]const u8 = if (api_key_borrowed) |key| owned.dupe(u8, key) catch {
        config_arena.deinit();
        return .err_out_of_memory;
    } else null;

    const handle = global_allocator.create(EmbeddingClientHandle) catch {
        config_arena.deinit();
        return .err_out_of_memory;
    };
    handle.* = .{
        .header = HandleHeader.init(.embedding_client),
        .client = embedding_mod.EmbeddingClient.init(global_allocator, .{
            .endpoint = endpoint_copy,
            .model = model_copy,
            .api_format = api_format,
            .api_key = api_key_copy,
            .timeout_ms = timeout,
        }),
        .config_arena = config_arena,
    };
    const register_result = registerHandle(EmbeddingClientHandle, handle);
    if (register_result != .ok) {
        handle.client.deinit();
        handle.config_arena.deinit();
        global_allocator.destroy(handle);
        return register_result;
    }

    client_out.* = toOpaque(lattice_embedding_client, handle);
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
    unregisterHandle(EmbeddingClientHandle, handle);
    handle.client.deinit();
    handle.config_arena.deinit();
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
    try std.testing.expectEqual(@as(c_int, -15), @intFromEnum(lattice_error.err_value_too_large));
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
    try std.testing.expectEqualStrings("0.16.0", std.mem.sliceTo(version, 0));
}

test "error message returns valid strings" {
    const msg = lattice_error_message(.ok);
    try std.testing.expect(msg != null);
    try std.testing.expectEqualStrings("Success", std.mem.sliceTo(msg, 0));

    const unsupported = lattice_error_message(.err_unsupported);
    try std.testing.expect(unsupported != null);
    try std.testing.expectEqualStrings("Unsupported operation or value type", std.mem.sliceTo(unsupported, 0));

    const too_large = lattice_error_message(.err_value_too_large);
    try std.testing.expect(too_large != null);
    try std.testing.expectEqualStrings("Value too large for database page size", std.mem.sliceTo(too_large, 0));
}

test "finished transaction handles are freed, not kept until close" {
    const path = "/tmp/lattice_capi_internal_txn_handles_freed.db";
    compat.fs.cwd().deleteFile(path) catch {};
    compat.fs.cwd().deleteFile(path ++ "-wal") catch {};
    defer {
        compat.fs.cwd().deleteFile(path) catch {};
        compat.fs.cwd().deleteFile(path ++ "-wal") catch {};
    }

    var db: ?*lattice_database = null;
    const options = lattice_open_options{ .create = true, .cache_size_mb = 4, .vector_dimensions = 0 };
    try std.testing.expectEqual(lattice_error.ok, lattice_open(path, &options, &db));
    defer _ = lattice_close(db);

    const liveHandles = struct {
        fn count() usize {
            handle_registry_mutex.lock();
            defer handle_registry_mutex.unlock();
            return handle_registry.count();
        }
    }.count;

    const before = liveHandles();
    for (0..200) |i| {
        var txn: ?*lattice_txn = null;
        const mode: lattice_txn_mode = if (i % 2 == 0) .read_write else .read_only;
        try std.testing.expectEqual(lattice_error.ok, lattice_begin(db, mode, &txn));
        if (i % 4 == 0) {
            try std.testing.expectEqual(lattice_error.ok, lattice_rollback(txn));
        } else {
            try std.testing.expectEqual(lattice_error.ok, lattice_commit(txn));
        }
    }
    try std.testing.expectEqual(before, liveHandles());
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
