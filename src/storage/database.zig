//! Database: Central coordinator for all LatticeDB components.
//!
//! The Database struct owns and manages all subsystems including storage,
//! graph stores, indexes, and transaction management. It provides the
//! primary API for database operations.

const std = @import("std");
const Allocator = std.mem.Allocator;

const lattice = @import("lattice");

// Core types
const types = lattice.core.types;
const PageId = types.PageId;
const NodeId = types.NodeId;
const EdgeId = types.EdgeId;
const PropertyValue = types.PropertyValue;
const NULL_PAGE = types.NULL_PAGE;

// Storage layer
const vfs_mod = lattice.storage.vfs;
const PosixVfs = vfs_mod.PosixVfs;
const memory_vfs_mod = lattice.storage.memory_vfs;
const Vfs = vfs_mod.Vfs;

const page_mod = lattice.storage.page;
const TreeIndex = page_mod.TreeIndex;

const page_manager = lattice.storage.page_manager;
const PageManager = page_manager.PageManager;
const PageManagerError = page_manager.PageManagerError;

const buffer_pool_mod = lattice.storage.buffer_pool;
const BufferPool = buffer_pool_mod.BufferPool;

const btree_mod = lattice.storage.btree;
const BTree = btree_mod.BTree;
const BTreeError = btree_mod.BTreeError;

const wal_mod = lattice.storage.wal;
const WalManager = wal_mod.WalManager;
const WalError = wal_mod.WalError;

const recovery_mod = lattice.storage.recovery;
const checkpoint_mod = lattice.storage.checkpoint;
const stream_store_mod = lattice.stream.store;
const stream_payload_mod = lattice.stream.payload;
pub const StreamBatch = stream_store_mod.Batch;
pub const StreamRecord = stream_store_mod.Record;

// Graph layer
const symbols_mod = lattice.graph.symbols;
const SymbolTable = symbols_mod.SymbolTable;

const node_mod = lattice.graph.node;
const NodeStore = node_mod.NodeStore;

const edge_mod = lattice.graph.edge;
const EdgeStore = edge_mod.EdgeStore;
pub const EdgeRef = edge_mod.EdgeRef;
pub const EdgeRefIterator = edge_mod.EdgeStore.EdgeRefIterator;
pub const CompactStats = page_manager.TruncateStats;

/// What a backup did.
pub const BackupStats = struct {
    /// Bytes written to the destination.
    bytes_copied: u64,
    /// Pages in the database at the moment it was captured.
    pages_copied: u32,
    /// Dirty pages flushed to make the source consistent first.
    pages_flushed: u32,
    /// Wall-clock time for the whole operation.
    duration_ns: u64,
};

const replicate_mod = @import("replicate.zig");

const label_index_mod = lattice.graph.label_index;
const LabelIndex = label_index_mod.LabelIndex;

const property_index_mod = lattice.graph.property_index;
const PropertyIndex = property_index_mod.PropertyIndex;
const PropertyIndexError = property_index_mod.PropertyIndexError;
const PropertyIndexDefinition = property_index_mod.Definition;
const PropertyIndexEntityKind = property_index_mod.EntityKind;

const adjacency_cache_mod = lattice.graph.adjacency_cache;
const AdjacencyCache = adjacency_cache_mod.AdjacencyCache;
pub const CachedEdge = adjacency_cache_mod.CachedEdge;

// FTS
const fts_mod = lattice.fts.index;
const FtsIndex = fts_mod.FtsIndex;
const FtsConfig = fts_mod.FtsConfig;
const FtsError = fts_mod.FtsError;
const fuzzy_mod = lattice.fts.fuzzy;

const scorer_mod = lattice.fts.scorer;
const fts_catalog_mod = lattice.fts.catalog;
const FtsCatalog = fts_catalog_mod.FtsCatalog;
const FtsCatalogError = fts_catalog_mod.FtsCatalogError;
const FtsDefinition = fts_catalog_mod.FtsDefinition;
const FtsEntityKind = fts_catalog_mod.FtsEntityKind;
pub const FtsSearchResult = scorer_mod.ScoredDoc;

// Vector storage and HNSW
const vector_storage_mod = lattice.vector.storage;
const VectorStorage = vector_storage_mod.VectorStorage;
const VectorStorageError = vector_storage_mod.VectorStorageError;
const VectorLocation = vector_storage_mod.VectorLocation;
const vector_distance = lattice.vector.distance;

const hnsw_mod = lattice.vector.hnsw;
const HnswIndex = hnsw_mod.HnswIndex;
const HnswConfig = hnsw_mod.HnswConfig;
const HnswError = hnsw_mod.HnswError;
pub const VectorSearchResult = hnsw_mod.SearchResult;

// Transactions
const txn_mod = lattice.transaction.manager;
const TxnManager = txn_mod.TxnManager;
const Transaction = txn_mod.Transaction;
const TxnMode = txn_mod.TxnMode;
const TxnError = txn_mod.TxnError;
const UndoEntry = txn_mod.UndoEntry;
const UndoOpType = txn_mod.UndoOpType;
const EntityType = txn_mod.EntityType;

const wal_payload = lattice.transaction.wal_payload;
const WalRecordType = wal_mod.WalRecordType;

// Query system
const parser_mod = lattice.query.parser;
const Parser = parser_mod.Parser;
const semantic_mod = lattice.query.semantic;
const SemanticAnalyzer = semantic_mod.SemanticAnalyzer;
const VariableInfo = semantic_mod.VariableInfo;
const planner_mod = lattice.query.planner;
const QueryPlanner = planner_mod.QueryPlanner;
const StorageContext = planner_mod.StorageContext;
const executor_mod = lattice.query.executor;
const ExecutionContext = executor_mod.ExecutionContext;
const execute = executor_mod.execute;
const cache_mod = lattice.query.cache;
const QueryCache = cache_mod.QueryCache;

/// Database errors
pub const DatabaseError = error{
    FileNotFound,
    PermissionDenied,
    InvalidDatabase,
    IoError,
    OutOfMemory,
    BufferPoolFull,
    TreeInitFailed,
    AlreadyExists,
    ReadOnly,
    NotFound,
    InvalidArgument,
    TransactionNotActive,
    TransactionReadOnly,
    TransactionConflict,
    TransactionsNotEnabled,
    /// Serialized node/edge payload exceeds the B-tree value limit or could
    /// not be represented by the on-disk value layout.
    ValueTooLarge,
    /// The requested explicit property index does not exist.
    MissingIndex,
    /// Another process holds this database. A writer takes it exclusively and a
    /// reader shares it, so a reader is refused while a writer has it open.
    DatabaseLocked,
};

/// Query execution errors
pub const QueryError = error{
    /// Query parsing failed
    ParseError,
    /// Semantic analysis failed
    SemanticError,
    /// Query planning failed
    PlanError,
    /// Query execution failed
    ExecutionError,
    /// Out of memory
    OutOfMemory,
};

/// Query pipeline stage where a failure occurred.
pub const QueryFailureStage = enum {
    parse,
    semantic,
    plan,
    execution,
};

/// Source location for query diagnostics.
pub const QueryFailureLocation = struct {
    line: u32,
    column: u32,
    length: u32,
};

/// Structured query failure details.
pub const QueryFailure = struct {
    allocator: Allocator,
    stage: QueryFailureStage,
    message: []const u8,
    code: ?[]const u8 = null,
    location: ?QueryFailureLocation = null,

    pub fn deinit(self: *QueryFailure) void {
        self.allocator.free(self.message);
        if (self.code) |c| self.allocator.free(c);
    }

    pub fn toLegacyError(self: QueryFailure) QueryError {
        return switch (self.stage) {
            .parse => QueryError.ParseError,
            .semantic => QueryError.SemanticError,
            .plan => QueryError.PlanError,
            .execution => QueryError.ExecutionError,
        };
    }
};

/// Detailed query execution result.
pub const QueryDetailedResult = union(enum) {
    success: QueryResult,
    failure: QueryFailure,

    pub fn deinit(self: *QueryDetailedResult) void {
        switch (self.*) {
            .success => |*result| result.deinit(),
            .failure => |*failure| failure.deinit(),
        }
    }
};

/// A single value in a query result
pub const ResultValue = union(enum) {
    null_val: void,
    bool_val: bool,
    int_val: i64,
    float_val: f64,
    string_val: []const u8,
    node_id: NodeId,
    edge_id: EdgeId,
    bytes_val: []const u8,
    vector_val: []const f32,
    list_val: []const ResultValue,
    map_val: []const MapEntry,

    pub const MapEntry = struct {
        key: []const u8,
        value: ResultValue,
    };

    /// Free all owned memory in this value (recursive for lists/maps)
    pub fn deinit(self: *const ResultValue, allocator: Allocator) void {
        switch (self.*) {
            .string_val => |s| allocator.free(s),
            .bytes_val => |b| allocator.free(b),
            .vector_val => |v| allocator.free(v),
            .list_val => |list| {
                for (list) |*item| {
                    item.deinit(allocator);
                }
                allocator.free(list);
            },
            .map_val => |map| {
                for (map) |*entry| {
                    allocator.free(entry.key);
                    entry.value.deinit(allocator);
                }
                allocator.free(map);
            },
            else => {},
        }
    }

    /// Format for display
    pub fn format(self: ResultValue, writer: anytype) !void {
        switch (self) {
            .null_val => try writer.writeAll("null"),
            .bool_val => |b| try writer.print("{}", .{b}),
            .int_val => |i| try writer.print("{}", .{i}),
            .float_val => |f| try writer.print("{d:.6}", .{f}),
            .string_val => |s| try writer.print("\"{s}\"", .{s}),
            .node_id => |id| try writer.print("Node({d})", .{id}),
            .edge_id => |id| try writer.print("Edge({d})", .{id}),
            .bytes_val => |b| try writer.print("<bytes:{d}>", .{b.len}),
            .vector_val => |v| try writer.print("<vector:{d}>", .{v.len}),
            .list_val => |list| {
                try writer.writeByte('[');
                for (list, 0..) |item, i| {
                    if (i > 0) try writer.writeAll(", ");
                    try item.format(writer);
                }
                try writer.writeByte(']');
            },
            .map_val => |map| {
                try writer.writeByte('{');
                for (map, 0..) |entry, i| {
                    if (i > 0) try writer.writeAll(", ");
                    try writer.print("{s}: ", .{entry.key});
                    try entry.value.format(writer);
                }
                try writer.writeByte('}');
            },
        }
    }
};

/// A row in a query result
pub const ResultRow = struct {
    values: []ResultValue,

    pub fn deinit(self: *ResultRow, allocator: Allocator) void {
        for (self.values) |*val| {
            val.deinit(allocator);
        }
        allocator.free(self.values);
    }
};

/// Result of a query execution
pub const QueryResult = struct {
    /// Column names from RETURN clause
    columns: [][]const u8,
    /// Result rows
    rows: []ResultRow,
    /// Allocator used for results
    allocator: Allocator,

    /// Free the query result
    pub fn deinit(self: *QueryResult) void {
        for (self.rows) |*row| {
            row.deinit(self.allocator);
        }
        self.allocator.free(self.rows);
        // Free each column name string
        for (self.columns) |col| {
            self.allocator.free(col);
        }
        self.allocator.free(self.columns);
    }

    /// Get number of rows
    pub fn rowCount(self: *const QueryResult) usize {
        return self.rows.len;
    }

    /// Get number of columns
    pub fn columnCount(self: *const QueryResult) usize {
        return self.columns.len;
    }
};

/// Database configuration
pub const DatabaseConfig = struct {
    /// Buffer pool size in bytes (default 4MB, auto-scales for vector/FTS)
    /// Set to 0 to use automatic sizing based on enabled features
    buffer_pool_size: usize = 0,
    /// Enable write-ahead logging
    enable_wal: bool = true,
    /// Enable full-text search
    enable_fts: bool = true,
    /// FTS configuration
    fts_config: FtsConfig = .{},
    /// Enable vector storage
    enable_vector: bool = false,
    /// Vector dimensions (required if enable_vector is true)
    vector_dimensions: u16 = 128,
    /// Enable in-memory adjacency cache for accelerated graph traversals
    enable_adjacency_cache: bool = false,
    /// Enable query cache for parsed ASTs
    enable_query_cache: bool = true,
    /// Maximum number of cached query entries
    query_cache_size: u32 = 128,
    /// Policy for checkpointing automatically as the WAL grows.
    ///
    /// Without this the WAL only resets when the database is closed, so a
    /// long-running process accumulates frames without limit and pays for them
    /// again in recovery time. Set to null to opt out and manage checkpoints by
    /// hand.
    auto_checkpoint: ?checkpoint_mod.AutoCheckpointConfig = .{},

    /// Compute effective buffer pool size based on enabled features.
    /// Priority: LATTICE_BUFFER_POOL_MB env var > explicit buffer_pool_size > auto-sizing.
    /// Auto-sizing:
    /// - Base: 16MB for graph operations (covers ~50K edges comfortably)
    /// - FTS enabled: +12MB (dictionary B+Tree, posting lists)
    /// - Vector enabled: +12MB (HNSW connections, vector pages)
    pub fn effectiveBufferPoolSize(self: DatabaseConfig) usize {
        // Environment variable override (useful for tuning without recompilation)
        if (std.c.getenv("LATTICE_BUFFER_POOL_MB")) |mb_z| {
            if (std.fmt.parseInt(usize, std.mem.span(mb_z), 10)) |mb| {
                if (mb > 0) return mb * 1024 * 1024;
            } else |_| {}
        }

        if (self.buffer_pool_size > 0) {
            return self.buffer_pool_size;
        }

        const base_size: usize = 16 * 1024 * 1024; // 16MB
        const fts_size: usize = if (self.enable_fts) 12 * 1024 * 1024 else 0; // +12MB
        const vector_size: usize = if (self.enable_vector) 12 * 1024 * 1024 else 0; // +12MB

        return base_size + fts_size + vector_size;
    }
};

/// Options for opening a database
pub const OpenOptions = struct {
    /// Create the database if it doesn't exist
    create: bool = false,
    /// Open in read-only mode
    read_only: bool = false,
    /// Page size in bytes, only used when creating a new database file
    page_size: u32 = types.DEFAULT_PAGE_SIZE,
    /// Database configuration
    config: DatabaseConfig = .{},
    /// Take a lock on the file so two processes cannot tread on each other.
    ///
    /// A read-write handle takes the file exclusively; a read-only handle shares
    /// it. Opening returns `DatabaseLocked` rather than waiting, because a caller
    /// who cannot have the database wants to be told, not to hang.
    ///
    /// Turn this off only where locking does not work, such as some network
    /// filesystems. It does not enable concurrent access: this engine cannot
    /// share a database between processes however the lock is set, so what
    /// turning it off buys is the old behaviour of finding that out the hard way.
    lock: bool = true,
};

fn mapWalInitError(err: WalError) DatabaseError {
    return switch (err) {
        WalError.IoError => DatabaseError.IoError,
        WalError.InvalidMagic,
        WalError.UuidMismatch,
        WalError.VersionMismatch,
        WalError.ChecksumMismatch,
        WalError.CorruptedFrame,
        => DatabaseError.InvalidDatabase,
        WalError.RecordTooLarge,
        WalError.EndOfLog,
        => DatabaseError.IoError,
    };
}

fn mapTxnError(err: TxnError) DatabaseError {
    return switch (err) {
        TxnError.NotActive => DatabaseError.TransactionNotActive,
        TxnError.ReadOnly => DatabaseError.TransactionReadOnly,
        TxnError.NotFound => DatabaseError.NotFound,
        TxnError.TooManyTransactions => DatabaseError.OutOfMemory,
        TxnError.SavepointNotFound => DatabaseError.InvalidArgument,
        TxnError.RecordTooLarge => DatabaseError.ValueTooLarge,
        TxnError.WalError => DatabaseError.IoError,
        TxnError.Conflict => DatabaseError.TransactionConflict,
        TxnError.OutOfMemory => DatabaseError.OutOfMemory,
    };
}

fn mapPropertyIndexError(err: PropertyIndexError) DatabaseError {
    return switch (err) {
        PropertyIndexError.NotFound => DatabaseError.MissingIndex,
        PropertyIndexError.AlreadyExists => DatabaseError.AlreadyExists,
        PropertyIndexError.OutOfMemory => DatabaseError.OutOfMemory,
        PropertyIndexError.InvalidData => DatabaseError.InvalidDatabase,
        PropertyIndexError.IoError => DatabaseError.IoError,
    };
}

fn mapFtsCatalogError(err: FtsCatalogError) DatabaseError {
    return switch (err) {
        FtsCatalogError.NotFound => DatabaseError.MissingIndex,
        FtsCatalogError.AlreadyExists => DatabaseError.AlreadyExists,
        FtsCatalogError.OutOfMemory => DatabaseError.OutOfMemory,
        FtsCatalogError.InvalidData => DatabaseError.InvalidDatabase,
        FtsCatalogError.IoError => DatabaseError.IoError,
    };
}

fn mapFtsError(err: FtsError) DatabaseError {
    return switch (err) {
        FtsError.NotFound => DatabaseError.NotFound,
        FtsError.OutOfMemory => DatabaseError.OutOfMemory,
        FtsError.InvalidQuery => DatabaseError.InvalidArgument,
        else => DatabaseError.IoError,
    };
}

fn shouldResetWalForNewDatabase(err: WalError) bool {
    return switch (err) {
        WalError.InvalidMagic,
        WalError.UuidMismatch,
        WalError.VersionMismatch,
        WalError.ChecksumMismatch,
        WalError.CorruptedFrame,
        => true,
        else => false,
    };
}

const TxnNodeState = struct {
    exists: bool,
    labels: []symbols_mod.SymbolId,
    properties: []node_mod.Property,

    fn clone(self: TxnNodeState, allocator: Allocator) Allocator.Error!TxnNodeState {
        return .{
            .exists = self.exists,
            .labels = try cloneSymbolIds(allocator, self.labels),
            .properties = try cloneProperties(allocator, self.properties),
        };
    }

    fn toNode(self: TxnNodeState, allocator: Allocator, node_id: NodeId) Allocator.Error!node_mod.Node {
        return .{
            .id = node_id,
            .labels = try cloneSymbolIds(allocator, self.labels),
            .properties = try cloneProperties(allocator, self.properties),
        };
    }

    fn deinit(self: *TxnNodeState, allocator: Allocator) void {
        allocator.free(self.labels);
        freeProperties(allocator, self.properties);
        self.* = undefined;
    }
};

const TxnEdgeState = struct {
    exists: bool,
    source: NodeId,
    target: NodeId,
    edge_type: symbols_mod.SymbolId,
    properties: []node_mod.Property,

    fn clone(self: TxnEdgeState, allocator: Allocator) Allocator.Error!TxnEdgeState {
        return .{
            .exists = self.exists,
            .source = self.source,
            .target = self.target,
            .edge_type = self.edge_type,
            .properties = try cloneProperties(allocator, self.properties),
        };
    }

    fn toEdge(self: TxnEdgeState, allocator: Allocator, edge_id: EdgeId) Allocator.Error!edge_mod.Edge {
        return .{
            .id = edge_id,
            .source = self.source,
            .target = self.target,
            .edge_type = self.edge_type,
            .properties = try cloneProperties(allocator, self.properties),
        };
    }

    fn deinit(self: *TxnEdgeState, allocator: Allocator) void {
        freeProperties(allocator, self.properties);
        self.* = undefined;
    }
};

const TxnVectorState = union(enum) {
    absent: void,
    value: []f32,

    fn clone(self: TxnVectorState, allocator: Allocator) Allocator.Error!TxnVectorState {
        return switch (self) {
            .absent => .{ .absent = {} },
            .value => |vector| .{ .value = try allocator.dupe(f32, vector) },
        };
    }

    fn deinit(self: *TxnVectorState, allocator: Allocator) void {
        switch (self.*) {
            .absent => {},
            .value => |vector| allocator.free(vector),
        }
        self.* = undefined;
    }
};


const TxnStreamAppend = struct {
    stream: []u8,
    kind: []u8,
    payload: PropertyValue,
    sequence: ?u64 = null,

    fn deinit(self: *TxnStreamAppend, allocator: Allocator) void {
        allocator.free(self.stream);
        allocator.free(self.kind);
        self.payload.deinit(allocator);
        self.* = undefined;
    }
};

const TxnStreamOffset = struct {
    stream: []u8,
    consumer: []u8,
    sequence: u64,

    fn deinit(self: *TxnStreamOffset, allocator: Allocator) void {
        allocator.free(self.stream);
        allocator.free(self.consumer);
        self.* = undefined;
    }
};

const TxnStreamTrim = struct {
    stream: []u8,
    through_sequence: u64,

    fn deinit(self: *TxnStreamTrim, allocator: Allocator) void {
        allocator.free(self.stream);
        self.* = undefined;
    }
};

const TxnOverlay = struct {
    node_states: std.AutoHashMapUnmanaged(NodeId, TxnNodeState) = .{},
    edge_states: std.AutoHashMapUnmanaged(EdgeId, TxnEdgeState) = .{},
    vector_states: std.AutoHashMapUnmanaged(NodeId, TxnVectorState) = .{},
    stream_appends: std.ArrayListUnmanaged(TxnStreamAppend) = .empty,
    stream_offsets: std.ArrayListUnmanaged(TxnStreamOffset) = .empty,
    stream_trims: std.ArrayListUnmanaged(TxnStreamTrim) = .empty,
    has_staged_writes: bool = false,

    fn hasChanges(self: *const TxnOverlay) bool {
        return self.has_staged_writes;
    }

    fn markDirty(self: *TxnOverlay) void {
        self.has_staged_writes = true;
    }

    fn deinit(self: *TxnOverlay, allocator: Allocator) void {
        var node_iter = self.node_states.iterator();
        while (node_iter.next()) |entry| {
            entry.value_ptr.deinit(allocator);
        }
        self.node_states.deinit(allocator);

        var edge_iter = self.edge_states.iterator();
        while (edge_iter.next()) |entry| {
            entry.value_ptr.deinit(allocator);
        }
        self.edge_states.deinit(allocator);

        var vector_iter = self.vector_states.iterator();
        while (vector_iter.next()) |entry| {
            entry.value_ptr.deinit(allocator);
        }
        self.vector_states.deinit(allocator);


        for (self.stream_appends.items) |*append| {
            append.deinit(allocator);
        }
        self.stream_appends.deinit(allocator);

        for (self.stream_offsets.items) |*offset| {
            offset.deinit(allocator);
        }
        self.stream_offsets.deinit(allocator);

        for (self.stream_trims.items) |*trim| {
            trim.deinit(allocator);
        }
        self.stream_trims.deinit(allocator);

        self.* = .{};
    }
};

fn cloneSymbolIds(allocator: Allocator, labels: []const symbols_mod.SymbolId) Allocator.Error![]symbols_mod.SymbolId {
    return allocator.dupe(symbols_mod.SymbolId, labels);
}

fn cloneProperties(allocator: Allocator, properties: []const node_mod.Property) Allocator.Error![]node_mod.Property {
    const cloned = try allocator.alloc(node_mod.Property, properties.len);
    var initialized: usize = 0;
    errdefer {
        for (cloned[0..initialized]) |*prop| {
            var value = prop.value;
            value.deinit(allocator);
        }
        allocator.free(cloned);
    }

    for (properties, 0..) |prop, i| {
        cloned[i] = .{
            .key_id = prop.key_id,
            .value = try prop.value.clone(allocator),
        };
        initialized += 1;
    }
    return cloned;
}

fn freeProperties(allocator: Allocator, properties: []node_mod.Property) void {
    for (properties) |*prop| {
        var value = prop.value;
        value.deinit(allocator);
    }
    allocator.free(properties);
}

fn emptySymbolIds(allocator: Allocator) Allocator.Error![]symbols_mod.SymbolId {
    return allocator.alloc(symbols_mod.SymbolId, 0);
}

fn emptyProperties(allocator: Allocator) Allocator.Error![]node_mod.Property {
    return allocator.alloc(node_mod.Property, 0);
}

fn containsSymbolId(values: []const symbols_mod.SymbolId, needle: symbols_mod.SymbolId) bool {
    for (values) |value| {
        if (value == needle) return true;
    }
    return false;
}

fn findPropertyById(
    properties: []const node_mod.Property,
    key_id: symbols_mod.SymbolId,
) ?node_mod.Property {
    for (properties) |prop| {
        if (prop.key_id == key_id) return prop;
    }
    return null;
}

/// The text a full-text definition would index on an entity, if there is any.
///
/// The property has to be present and hold a string. A missing property, a
/// number, or a list is not text and is left out rather than rendered into some
/// string form of itself: a search for `42` matching an integer property would be
/// a different feature with different rules, and quietly doing half of it here
/// would make the results hard to explain.
fn ftsTextOf(definition: FtsDefinition, properties: []const node_mod.Property) ?[]const u8 {
    const property = findPropertyById(properties, definition.property_id) orelse return null;
    return switch (property.value) {
        .string_val => |text| text,
        else => null,
    };
}

/// Frames an in-memory database keeps beyond the size of the database itself.
///
/// This is a floor rather than a tuning knob: a pool with nowhere to put the next
/// page fails the query outright instead of slowing it down, so it has to cover
/// the largest set of pages pinned at once.
///
/// Sixty-four is a measured number with a wide margin, not a guess. Four frames
/// were enough to complete a deep variable-length traversal, a filtered scan, a
/// full-text search, and a bulk write over a fifteen-hundred node graph with
/// edges and a text index. The engine simply does not hold many pages pinned
/// simultaneously.
///
/// The margin covers what that measurement did not: it was single threaded, and
/// concurrent readers each pin pages of their own. Sixteen times the observed
/// requirement is cheap insurance at 256 KB.
///
/// Note also where this floor actually applies. The pool is the smaller of the
/// configured budget and the database plus this floor, so for anything but a very
/// small database the database term dominates and the floor is irrelevant. A
/// database small enough for the floor to matter has too few pages to pin many of
/// them, which is the opposite of the dangerous case.
const MIN_MEMORY_POOL_FRAMES: usize = 64;

/// The path that asks for a database with no files behind it.
///
/// A path rather than an option because every binding already passes a path
/// string straight through, so `Database(":memory:")` works everywhere without a
/// single binding change. An option would mean another versioned C struct and
/// four bindings updated for the same capability.
pub const MEMORY_PATH = ":memory:";

pub fn isMemoryPath(path: []const u8) bool {
    return std.mem.eql(u8, path, MEMORY_PATH);
}

/// Where a database keeps its bytes.
///
/// Held by value so the database owns its storage outright. Both arms present
/// the same `Vfs` interface, and everything above this point is written against
/// that interface rather than either arm.
pub const Backend = union(enum) {
    posix: PosixVfs,
    memory: memory_vfs_mod.MemoryVfs,

    pub fn vfs(self: *Backend) vfs_mod.Vfs {
        return switch (self.*) {
            .posix => |*p| p.vfs(),
            .memory => |*m| m.vfs(),
        };
    }

    pub fn deinit(self: *Backend) void {
        switch (self.*) {
            // A posix VFS holds no state of its own; the files it opened were
            // closed by whoever opened them.
            .posix => {},
            .memory => |*m| m.deinit(),
        }
    }
};

/// Central database coordinator
pub const Database = struct {
    allocator: Allocator,

    // Storage layer (owned, must deinit in reverse order)
    //
    // Owned by value rather than supplied by the caller: the `Vfs` interface is a
    // fat pointer, so a caller-supplied backend would have to outlive the
    // database, which is a lifetime rule nobody should have to think about.
    vfs: Backend,
    page_manager: PageManager,
    buffer_pool: BufferPool,
    wal: ?WalManager,

    // B+Trees (8 trees for different stores)
    node_tree: BTree,
    edge_tree: BTree,
    edge_id_tree: BTree,
    label_tree: BTree,
    symbol_forward_tree: BTree,
    symbol_reverse_tree: BTree,
    fts_dict_tree: BTree,
    fts_lengths_tree: BTree,
    fts_reverse_tree: ?BTree,
    hnsw_node_tree: ?BTree,
    stream_meta_tree: ?BTree,
    stream_events_tree: ?BTree,
    stream_offsets_tree: ?BTree,
    property_index_catalog_tree: ?BTree,
    node_property_index_tree: ?BTree,
    edge_property_index_tree: ?BTree,

    // Graph stores
    symbol_table: SymbolTable,
    node_store: NodeStore,
    edge_store: EdgeStore,
    label_index: LabelIndex,
    property_index: ?PropertyIndex,

    // Indexes
    fts_index: ?FtsIndex,
    /// Whether any full-text index has been declared.
    ///
    /// Every write has to ask what the declared indexes want, and the answer is
    /// almost always "there are none". Keeping it here turns that question into a
    /// boolean rather than a catalog scan on the write path.
    fts_declared: bool = false,
    vector_storage: ?VectorStorage,
    hnsw_index: ?HnswIndex,

    // Transactions
    txn_manager: ?TxnManager,
    txn_overlays: std.AutoHashMap(u64, TxnOverlay),
    /// When the last automatic checkpoint ran, for the minimum-interval check.
    last_auto_checkpoint_ns: i128,
    active_writer_txn_id: ?u64,
    stream_mutex: @import("compat").Mutex,
    stream_cond: @import("compat").Condition,
    stream_epoch: u64,

    // Caches
    adjacency_cache: ?AdjacencyCache,
    query_cache: ?*QueryCache,

    // Configuration
    config: DatabaseConfig,
    path: []const u8,
    read_only: bool,
    /// Set when the file at `path` was created by `deserialize` and exists only
    /// to back this handle, so closing removes it.
    owns_backing_file: bool,

    const Self = @This();

    /// Open or create a database.
    ///
    /// Pass `:memory:` as the path for a database with no files behind it.
    pub fn open(allocator: Allocator, path: []const u8, options: OpenOptions) DatabaseError!*Self {
        return openWithBackend(allocator, path, options, null);
    }

    /// Open a database, optionally against a backend that already holds it.
    ///
    /// `deserialize` uses this to hand over a memory backend it has already
    /// filled with bytes, so the engine opens a complete database rather than
    /// creating an empty one and being written into afterwards.
    fn openWithBackend(
        allocator: Allocator,
        path: []const u8,
        options: OpenOptions,
        prepared: ?Backend,
    ) DatabaseError!*Self {
        if (!page_manager.isValidPageSize(options.page_size)) {
            return DatabaseError.InvalidArgument;
        }
        if (options.config.enable_vector and !types.isValidVectorDimensions(options.config.vector_dimensions)) {
            return DatabaseError.InvalidArgument;
        }

        var self = allocator.create(Self) catch return DatabaseError.OutOfMemory;
        errdefer allocator.destroy(self);

        self.allocator = allocator;
        self.config = options.config;
        self.read_only = options.read_only;
        // Only `deserialize` sets this, once its file has actually been opened.
        self.owns_backing_file = false;

        // Copy path for later use
        self.path = allocator.dupe(u8, path) catch return DatabaseError.OutOfMemory;
        errdefer allocator.free(self.path);

        // 1. Storage backend
        self.vfs = prepared orelse if (isMemoryPath(path))
            Backend{ .memory = memory_vfs_mod.MemoryVfs.init(allocator) }
        else
            Backend{ .posix = PosixVfs.init(allocator) };
        errdefer self.vfs.deinit();

        // 2. Open/create PageManager
        //
        // An in-memory database is always brought into being by opening it: there
        // is no previous one lying around to find, so asking for create would be
        // a formality that every caller has to remember. `lattice exec :memory:`
        // should just work, and so should a binding that opens without create.
        // Where deserialize has already seeded the backend the file is there, and
        // create finds it rather than replacing it.
        const effective_create = options.create or isMemoryPath(path);

        self.page_manager = PageManager.init(allocator, self.vfs.vfs(), path, .{
            .create = effective_create,
            .read_only = options.read_only,
            .page_size = options.page_size,
            .lock = options.lock,
        }) catch |err| {
            return switch (err) {
                PageManagerError.FileNotFound => DatabaseError.FileNotFound,
                PageManagerError.DatabaseLocked => DatabaseError.DatabaseLocked,
                PageManagerError.PermissionDenied => DatabaseError.PermissionDenied,
                PageManagerError.InvalidPageSize => DatabaseError.InvalidArgument,
                PageManagerError.OutOfMemory => DatabaseError.OutOfMemory,
                PageManagerError.InvalidMagic, PageManagerError.InvalidHeader => DatabaseError.InvalidDatabase,
                else => DatabaseError.IoError,
            };
        };
        errdefer self.page_manager.deinit();

        // 3. Initialize BufferPool (auto-scales based on enabled features)
        // An in-memory database gets a small fixed pool, and the reason is that
        // pool size stops mattering once the storage underneath is memory.
        //
        // The pool exists to keep pages off a disk. A miss against a memory
        // backend costs a copy from one part of RAM to another, so a small cache
        // is not the compromise it would be for a file. Measured on a fourteen
        // megabyte in-memory database, twelve full scans took 9.2 seconds against
        // a 256 KB pool and 9.9 against a 32 MB one — the larger cache bought
        // nothing at a hundred and twenty times the memory.
        //
        // So the size is a floor rather than a fraction of anything. Sizing it
        // from the database would also be a lie: at this point a freshly created
        // database is one page, and a formula reading that number would give the
        // same answer as this constant while looking as though it did something.
        //
        // The floor is what matters. When the clock sweep finds nothing
        // evictable the pool returns BufferPoolFull, which surfaces as a failed
        // query rather than a slow one, so there must be room for the largest set
        // of pages pinned at once.
        const configured_pool_size = options.config.effectiveBufferPoolSize();
        const memory_pool_size = MIN_MEMORY_POOL_FRAMES * @as(usize, self.page_manager.getPageSize());
        const effective_pool_size = switch (self.vfs) {
            .posix => configured_pool_size,
            .memory => @min(configured_pool_size, memory_pool_size),
        };
        self.buffer_pool = BufferPool.init(allocator, &self.page_manager, effective_pool_size) catch {
            return DatabaseError.BufferPoolFull;
        };
        errdefer self.buffer_pool.deinit();

        const opened_new_database = !self.page_manager.getHeader().hasInitializedTrees();

        // 4. Initialize WAL (optional)
        self.wal = null;
        if (options.config.enable_wal and !options.read_only) {
            const wal_path = std.fmt.allocPrint(allocator, "{s}-wal", .{path}) catch {
                return DatabaseError.OutOfMemory;
            };
            defer allocator.free(wal_path);

            self.wal = WalManager.initWithFrameSize(
                allocator,
                self.vfs.vfs(),
                wal_path,
                self.page_manager.getHeader().file_uuid,
                self.page_manager.getPageSize(),
            ) catch |err| blk: {
                if (opened_new_database and options.create and shouldResetWalForNewDatabase(err)) {
                    // Through the VFS, so a database with no files behind it does
                    // not try to remove a real one called ":memory:-wal".
                    self.vfs.vfs().delete(wal_path) catch return mapWalInitError(err);
                    break :blk WalManager.initWithFrameSize(
                        allocator,
                        self.vfs.vfs(),
                        wal_path,
                        self.page_manager.getHeader().file_uuid,
                        self.page_manager.getPageSize(),
                    ) catch |retry_err| return mapWalInitError(retry_err);
                }
                return mapWalInitError(err);
            };
        }

        // Initialize these early since saveTreeRoots() checks them
        self.vector_storage = null;
        self.hnsw_index = null;
        self.hnsw_node_tree = null;
        self.stream_meta_tree = null;
        self.stream_events_tree = null;
        self.stream_offsets_tree = null;
        self.property_index_catalog_tree = null;
        self.node_property_index_tree = null;
        self.edge_property_index_tree = null;
        self.stream_mutex = .{};
        self.stream_cond = .{};

        // 5. Initialize or load B+Trees
        const header = self.page_manager.getHeader();
        const is_new = !header.hasInitializedTrees();

        if (is_new) {
            try self.initNewTrees();
        } else {
            try self.loadExistingTrees();
        }

        if (!options.read_only) {
            try self.ensureStreamTreesForWrite();
            try self.ensurePropertyIndexTreesForWrite();
        }

        // 6. Initialize Symbol Table
        self.symbol_table = SymbolTable.init(allocator, &self.symbol_forward_tree, &self.symbol_reverse_tree);

        // 7. Initialize Graph Stores
        self.node_store = NodeStore.init(allocator, &self.node_tree);
        self.edge_store = EdgeStore.init(allocator, &self.edge_tree, &self.edge_id_tree);
        self.label_index = LabelIndex.init(allocator, &self.label_tree);
        self.initializePropertyIndex();

        // 7b. Run WAL recovery if needed
        if (self.wal) |*wal| {
            var rm = recovery_mod.RecoveryManager.initWithLogicalContext(allocator, .{
                .node_store = &self.node_store,
                .edge_store = &self.edge_store,
                .symbol_table = &self.symbol_table,
                .label_index = &self.label_index,
                .stream_trees = self.streamTrees(),
            });
            const stats = rm.recover(wal, &self.page_manager) catch |err| switch (err) {
                error.MidLogCorruption => return DatabaseError.InvalidDatabase,
                else => return DatabaseError.IoError,
            };
            // Persist recovered state if any redo operations were applied
            if (stats.redo_operations > 0) {
                try self.rebuildPropertyIndexes();
                try self.rebuildFtsIndexes();
                self.saveTreeRoots() catch {};
            }
        }

        // 8. Initialize FTS (optional)
        self.fts_index = null;
        if (options.config.enable_fts) {
            self.fts_index = FtsIndex.init(
                allocator,
                &self.buffer_pool,
                &self.fts_dict_tree,
                &self.fts_lengths_tree,
                if (self.fts_reverse_tree) |*t| t else null,
                options.config.fts_config,
            );
        }

        self.fts_declared = self.ftsDeclarationsExist();

        // 8b. Initialize Vector Storage (optional)
        // (vector_storage and hnsw_index already initialized to null above)
        const is_existing_vectors = options.config.enable_vector and header.vector_segment_page != NULL_PAGE;
        if (options.config.enable_vector) {
            // Check if vector storage already exists (from a previous session)
            if (header.vector_segment_page != NULL_PAGE) {
                // Open existing vector storage
                self.vector_storage = VectorStorage.open(
                    allocator,
                    &self.buffer_pool,
                    header.vector_segment_page,
                    options.config.vector_dimensions,
                ) catch |err| return mapVectorStorageError(err);
            } else {
                // Create new vector storage
                self.vector_storage = VectorStorage.init(
                    allocator,
                    &self.buffer_pool,
                    options.config.vector_dimensions,
                ) catch |err| return mapVectorStorageError(err);
            }

            // 8c. Initialize HNSW index if vector storage is available
            if (self.vector_storage) |*vs| {
                self.hnsw_index = HnswIndex.init(
                    allocator,
                    &self.buffer_pool,
                    vs,
                    HnswConfig{
                        .dimensions = options.config.vector_dimensions,
                    },
                );

                // 8d. Load HNSW index from B+Tree, or rebuild from vectors
                if (is_existing_vectors) {
                    if (self.hnsw_index) |*hnsw| {
                        var loaded = false;
                        if (self.hnsw_node_tree) |*tree| {
                            if (hnsw.loadFromTree(tree)) |ok| {
                                loaded = ok;
                            } else |_| {
                                hnsw.resetForRebuild();
                            }
                        }
                        if (!loaded) {
                            try self.rebuildHnswIndex(vs, hnsw);
                        }
                    }
                }
            }
        }

        // 9. Initialize Transaction Manager
        self.txn_manager = null;
        if (self.wal) |*wal| {
            self.txn_manager = TxnManager.init(allocator, wal);
        }
        self.txn_overlays = std.AutoHashMap(u64, TxnOverlay).init(allocator);
        self.last_auto_checkpoint_ns = @import("compat").nanoTimestamp();
        self.active_writer_txn_id = null;
        self.stream_epoch = 0;

        // 10. Initialize Adjacency Cache (optional)
        self.adjacency_cache = if (options.config.enable_adjacency_cache)
            AdjacencyCache.init(allocator)
        else
            null;

        // 11. Initialize Query Cache (optional)
        self.query_cache = if (options.config.enable_query_cache)
            QueryCache.init(allocator, options.config.query_cache_size) catch null
        else
            null;

        return self;
    }

    fn persistHnswIndex(self: *Self) DatabaseError!void {
        if (self.hnsw_index) |*hnsw| {
            if (!hnsw.dirty) return;
            if (self.hnsw_node_tree == null) {
                self.hnsw_node_tree = BTree.init(self.allocator, &self.buffer_pool) catch return DatabaseError.IoError;
            }
            if (self.hnsw_node_tree) |*tree| {
                hnsw.saveToTree(tree) catch return DatabaseError.IoError;
            }
        }
    }

    /// Sync all pending writes to disk.
    /// Call this before close() if you need durability guarantees.
    /// Returns an error if flushing fails - data may not be persisted.
    pub fn sync(self: *Self) DatabaseError!void {
        if (self.read_only) return;

        try self.persistHnswIndex();

        // Save B+Tree root pages
        self.saveTreeRoots() catch {
            return DatabaseError.IoError;
        };

        // Flush buffer pool
        self.buffer_pool.close() catch {
            return DatabaseError.IoError;
        };

        // Sync WAL if present
        if (self.wal) |*wal| {
            wal.sync() catch {
                return DatabaseError.IoError;
            };
        }
    }

    pub fn checkpointWal(self: *Self, mode: checkpoint_mod.CheckpointMode) DatabaseError!void {
        _ = self.checkpoint(mode) catch |err| return err;
    }

    /// Flush dirty pages to the database file and report what happened.
    ///
    /// `.truncate` additionally resets the WAL, which is what bounds its size.
    /// Callers that only want the pages durable, such as a backup taking a
    /// consistent snapshot of a running database, want `.full` instead: it
    /// leaves frame numbering alone so anything reading the WAL keeps its place.
    ///
    /// Returns null stats for a read-only handle or one opened without a WAL,
    /// where there is nothing to do.
    pub fn checkpoint(self: *Self, mode: checkpoint_mod.CheckpointMode) DatabaseError!?checkpoint_mod.CheckpointStats {
        if (self.read_only) return null;
        const wal = if (self.wal) |*w| w else return null;

        var checkpointer = checkpoint_mod.Checkpointer.init(
            self.allocator,
            &self.buffer_pool,
            &self.page_manager,
            wal,
        );
        return checkpointer.checkpoint(mode) catch {
            return DatabaseError.IoError;
        };
    }

    /// Checkpoint if the WAL has grown enough and it is safe to do so.
    ///
    /// Called after a commit. Deliberately best-effort: a commit that has already
    /// succeeded must not be reported as failed because housekeeping could not
    /// run. A skipped checkpoint costs disk space, and the next commit tries
    /// again.
    ///
    /// Truncation discards the whole WAL, so it is only safe once every dirty
    /// page is durable in the main file and no other transaction is relying on
    /// frames still being there. The checkpoint itself handles the first, and
    /// the overlay count handles the second.
    fn maybeAutoCheckpoint(self: *Self) void {
        const policy = self.config.auto_checkpoint orelse return;
        if (self.read_only) return;
        if (self.txn_overlays.count() != 0) return;

        const wal = if (self.wal) |*w| w else return;
        if (wal.header.frame_count < policy.max_wal_frames) return;

        const now = @import("compat").nanoTimestamp();
        const elapsed = now - self.last_auto_checkpoint_ns;
        if (elapsed < @as(i128, @intCast(policy.min_interval_ns))) return;

        // Record the attempt either way. A checkpoint that fails repeatedly
        // should not be retried on every single commit.
        self.last_auto_checkpoint_ns = now;

        var checkpointer = checkpoint_mod.Checkpointer.init(
            self.allocator,
            &self.buffer_pool,
            &self.page_manager,
            wal,
        );
        _ = checkpointer.checkpoint(policy.mode) catch {};
    }

    /// Flush all durable state and remove contiguous freelist pages from EOF.
    ///
    /// Compaction is an exclusive maintenance operation. It never relocates a
    /// live page, so stable IDs and persisted page references remain unchanged.
    /// Copy this database to `dest_path` without closing it.
    ///
    /// The copy is a complete database on its own: a full checkpoint first
    /// flushes every dirty page into the main file, so the write-ahead log is
    /// redundant by the time the bytes are read and the destination needs no log
    /// beside it.
    ///
    /// Exclusive for its duration, like compact. A file copy taken while writes
    /// land underneath it is torn in ways no checksum on the source would catch,
    /// so an open transaction is refused rather than worked around. The
    /// checkpoint uses `.full` rather than `.truncate` so frame numbering is left
    /// alone for anything following the log.
    ///
    /// The destination is written beside the target and renamed into place, so an
    /// interrupted backup never leaves a partial file that looks usable.
    /// Fold everything pending into the database file, so that the bytes on disk
    /// are a complete database on their own.
    ///
    /// The checkpoint is `.full` rather than `.truncate` so frame numbering is
    /// left alone for anything following the log. Derived state that lives in
    /// memory until asked, notably the vector index, has to be persisted first
    /// or the copy comes out missing an index it claims to have.
    ///
    /// Returns the number of pages flushed.
    fn quiesceForCopy(self: *Self) DatabaseError!u32 {
        if (self.read_only) return 0;
        try self.persistHnswIndex();
        self.saveTreeRoots() catch return DatabaseError.IoError;
        if (try self.checkpoint(.full)) |stats| {
            return stats.pages_flushed;
        }
        return 0;
    }

    pub fn backup(self: *Self, dest_path: []const u8) DatabaseError!BackupStats {
        if (self.txn_overlays.count() != 0) return DatabaseError.TransactionConflict;

        const start_ns = @import("compat").nanoTimestamp();

        const pages_flushed = try self.quiesceForCopy();

        const temp_path = std.fmt.allocPrint(self.allocator, "{s}.partial", .{dest_path}) catch {
            return DatabaseError.OutOfMemory;
        };
        defer self.allocator.free(temp_path);

        // The source comes from wherever this database lives; the destination is
        // a path on a real disk by definition. For a file-backed database those
        // are the same filesystem, but for an in-memory one they are not, and
        // writing the backup into memory and then renaming a file that was never
        // created is not a useful thing to do.
        const vfs_handle = self.vfs.vfs();
        var dest_backend = PosixVfs.init(self.allocator);
        const dest_vfs = dest_backend.vfs();

        var source = vfs_handle.open(self.path, .{ .read = true }) catch return DatabaseError.IoError;
        defer source.close();

        const total_bytes = source.size() catch return DatabaseError.IoError;

        var dest = dest_vfs.open(temp_path, .{
            .read = true,
            .write = true,
            .create = true,
            .truncate = true,
        }) catch return DatabaseError.IoError;

        var copied: u64 = 0;
        {
            errdefer {
                dest.close();
                dest_vfs.delete(temp_path) catch {};
            }

            const buffer = self.allocator.alloc(u8, 1024 * 1024) catch return DatabaseError.OutOfMemory;
            defer self.allocator.free(buffer);

            while (copied < total_bytes) {
                const want = @min(buffer.len, total_bytes - copied);
                const read = source.read(copied, buffer[0..want]) catch return DatabaseError.IoError;
                if (read == 0) break;
                dest.write(copied, buffer[0..read]) catch return DatabaseError.IoError;
                copied += read;
            }

            dest.sync() catch return DatabaseError.IoError;
        }
        dest.close();

        // Only now does a file appear at the path the caller asked for.
        //
        // Renaming is a real filesystem operation and the VFS has no equivalent,
        // which is fine: a backup writes to a path on disk whatever the source
        // database is stored in. The temporary file is a real file either way.
        @import("compat").fs.cwd().rename(temp_path, dest_path) catch {
            dest_vfs.delete(temp_path) catch {};
            return DatabaseError.IoError;
        };

        const end_ns = @import("compat").nanoTimestamp();
        return BackupStats{
            .bytes_copied = copied,
            .pages_copied = self.page_manager.pageCount(),
            .pages_flushed = pages_flushed,
            .duration_ns = @intCast(end_ns - start_ns),
        };
    }

    /// Hand back the whole database as a block of bytes.
    ///
    /// The bytes are a complete database file. Write them anywhere and they
    /// open, or pass them to `deserialize`. Pending writes are folded in first,
    /// so nothing is left behind in a log the caller does not have.
    ///
    /// This is the piece that makes a database easy to keep in object storage:
    /// one database is one file, so serialising it is reading that file. The
    /// caller does its own uploading, with whatever client, credentials, and
    /// retry policy it already has.
    ///
    /// Refuses while a transaction is open, for the same reason `backup` does:
    /// a copy taken while writes land underneath it is torn in ways no later
    /// check would catch.
    ///
    /// The caller owns the returned slice.
    pub fn serialize(self: *Self, allocator: Allocator) DatabaseError![]u8 {
        if (self.txn_overlays.count() != 0) return DatabaseError.TransactionConflict;

        _ = try self.quiesceForCopy();

        const vfs_handle = self.vfs.vfs();
        var source = vfs_handle.open(self.path, .{ .read = true }) catch {
            return DatabaseError.IoError;
        };
        defer source.close();

        const total = source.size() catch return DatabaseError.IoError;
        const bytes = allocator.alloc(u8, @intCast(total)) catch {
            return DatabaseError.OutOfMemory;
        };
        errdefer allocator.free(bytes);

        var copied: u64 = 0;
        while (copied < total) {
            const read = source.read(copied, bytes[@intCast(copied)..]) catch {
                return DatabaseError.IoError;
            };
            if (read == 0) break;
            copied += read;
        }
        if (copied != total) return DatabaseError.IoError;

        return bytes;
    }

    /// How `deserialize` should hold the database it opens.
    pub const DeserializeOptions = struct {
        config: DatabaseConfig = .{},
        /// Keep the database in memory rather than writing it to a file.
        ///
        /// This is the point of the whole exercise for anyone pulling small
        /// databases out of object storage: nothing touches the local disk.
        in_memory: bool = true,
        /// Directory to hold the file the bytes are written to, when
        /// `in_memory` is false. Defaults to `/tmp`.
        temp_dir: []const u8 = "/tmp",
        /// Take a lock on the materialised file. Ignored in memory, where there
        /// is no second process to exclude. See `OpenOptions.lock`.
        lock: bool = true,
        /// Point at the caller's bytes instead of copying them.
        ///
        /// Saves holding the database twice while it loads, and each page
        /// becomes a copy of its own the first time it is written, so reading
        /// and lightly editing a database keeps one copy of nearly all of it.
        ///
        /// **The bytes have to outlive the database.** That is why this is off
        /// by default, and why it is unavailable to callers who cannot promise
        /// it: Go may not let C retain a pointer into its heap at all, and
        /// pinning a Java array for the life of a database would hold up the
        /// collector for just as long.
        ///
        /// Only meaningful with `in_memory`.
        borrow_bytes: bool = false,
    };

    /// Open a database from bytes produced by `serialize`.
    ///
    /// The bytes are written to a file of its own choosing, which the returned
    /// database owns and removes when it closes. Callers do not have to know
    /// where it went, and nothing is left behind afterwards.
    ///
    /// Changes made to the returned database do not travel back to the byte
    /// slice. Call `serialize` again to get the new bytes.
    pub fn deserialize(
        allocator: Allocator,
        bytes: []const u8,
        options: DeserializeOptions,
    ) DatabaseError!*Self {
        // Checked before anything is written, because an empty or unrecognisable
        // slice would otherwise be written out, opened, and quietly turned into
        // a brand new empty database: `open` treats a zero-length file as a
        // request to create one. Somebody restoring from a truncated download
        // would get an empty database reported as a success.
        if (bytes.len < types.DEFAULT_PAGE_SIZE) return DatabaseError.InvalidDatabase;
        const magic = std.mem.readInt(u32, bytes[0..4], .little);
        if (magic != types.MAGIC_NUMBER) return DatabaseError.InvalidDatabase;

        if (options.in_memory) return deserializeInMemory(allocator, bytes, options);

        // A name nothing else will pick, so two of these can run at once and a
        // leftover from a crash cannot be mistaken for a live one.
        var suffix: [16]u8 = undefined;
        @import("compat").randomBytes(&suffix);

        const path = std.fmt.allocPrint(
            allocator,
            "{s}/lattice-deserialized-{x}.lattice",
            .{ options.temp_dir, suffix },
        ) catch return DatabaseError.OutOfMemory;
        defer allocator.free(path);

        const wal_path = std.fmt.allocPrint(allocator, "{s}-wal", .{path}) catch {
            return DatabaseError.OutOfMemory;
        };
        defer allocator.free(wal_path);

        {
            const file = @import("compat").fs.cwd().createFile(path, .{ .truncate = true }) catch {
                return DatabaseError.IoError;
            };
            defer file.close();

            errdefer @import("compat").fs.cwd().deleteFile(path) catch {};
            file.pwriteAll(bytes, 0) catch return DatabaseError.IoError;
            file.sync() catch return DatabaseError.IoError;
        }

        // Bytes that are not a database fail here, and the file they were
        // written to goes with them rather than accumulating in the temp
        // directory.
        errdefer {
            @import("compat").fs.cwd().deleteFile(path) catch {};
            @import("compat").fs.cwd().deleteFile(wal_path) catch {};
        }

        const db = try Self.open(allocator, path, .{
            .create = false,
            .config = options.config,
            .lock = options.lock,
        });
        db.owns_backing_file = true;
        return db;
    }

    /// Open bytes as a database held entirely in memory.
    ///
    /// The backend is seeded before the database is opened, so the engine finds a
    /// complete file waiting for it and takes its ordinary path from there.
    fn deserializeInMemory(
        allocator: Allocator,
        bytes: []const u8,
        options: DeserializeOptions,
    ) DatabaseError!*Self {
        var backend = memory_vfs_mod.MemoryVfs.init(allocator);
        // Only until the backend is handed over; from that point the database
        // owns it and releases it on close or on a failed open.
        var handed_over = false;
        errdefer if (!handed_over) backend.deinit();

        if (options.borrow_bytes) {
            backend.borrowWholeFile(MEMORY_PATH, bytes) catch return DatabaseError.OutOfMemory;
        } else {
            backend.writeWholeFile(MEMORY_PATH, bytes) catch return DatabaseError.OutOfMemory;
        }

        handed_over = true;
        const db = try Self.openWithBackend(
            allocator,
            MEMORY_PATH,
            .{ .create = false, .config = options.config, .lock = false },
            Backend{ .memory = backend },
        );
        return db;
    }

    /// Ship whatever has changed since the last pass into `dest_dir`.
    ///
    /// The first call, and any call after the log has been truncated, writes a
    /// full snapshot and begins a new generation. Every other call copies the
    /// write-ahead log frames that have appeared since. Shipping nothing is a
    /// normal outcome rather than an error, so this is safe to call on a timer.
    ///
    /// This lives on the database rather than in a separate process because a
    /// snapshot has to be taken with no writes in flight, and there is no
    /// cross-process locking to arrange that from outside.
    pub fn replicateTo(
        self: *Self,
        dest_dir: []const u8,
    ) replicate_mod.ReplicateError!replicate_mod.ReplicateStats {
        return replicate_mod.replicate(self, dest_dir);
    }

    pub fn compact(self: *Self) DatabaseError!CompactStats {
        if (self.read_only) return DatabaseError.ReadOnly;
        if (self.txn_overlays.count() != 0) return DatabaseError.TransactionConflict;

        try self.persistHnswIndex();
        self.saveTreeRoots() catch return DatabaseError.IoError;
        try self.checkpointWal(.truncate);

        return self.buffer_pool.truncateFreeTail() catch |err| switch (err) {
            error.BufferPoolFull => DatabaseError.BufferPoolFull,
            else => DatabaseError.IoError,
        };
    }

    /// Close the database and release all resources.
    /// For guaranteed durability, call sync() first and handle errors.
    pub fn close(self: *Self) void {
        // Reverse initialization order

        // 11. Query cache
        if (self.query_cache) |cache| {
            cache.deinit();
        }

        // 10. Adjacency cache
        if (self.adjacency_cache) |*cache| {
            cache.deinit();
        }

        // 9b. Transaction overlays
        var overlay_iter = self.txn_overlays.iterator();
        while (overlay_iter.next()) |entry| {
            entry.value_ptr.deinit(self.allocator);
        }
        self.txn_overlays.deinit();

        // 9. Transaction manager
        if (self.txn_manager) |*tm| {
            tm.deinit();
        }

        // 8c. HNSW index
        if (self.hnsw_index) |*hnsw| {
            if (!self.read_only) {
                self.persistHnswIndex() catch {};
            }
            hnsw.deinit();
        }

        // 8. FTS index (no explicit deinit needed - components don't own resources)

        // 7. Graph stores (no explicit deinit needed - they don't own resources)
        // 6. Symbol table (no explicit deinit needed)

        // 5. B+Trees - save root pages before closing (best effort)
        if (!self.read_only) {
            self.saveTreeRoots() catch {};
        }

        // 4. WAL
        if (self.wal) |*wal| {
            wal.deinit();
        }

        // 3. Buffer pool
        self.buffer_pool.deinit();

        // 2. Page manager
        self.page_manager.deinit();

        // 1. Storage backend. A memory backend holds every byte of the database,
        //    so this is where an in-memory database actually goes away.
        self.vfs.deinit();

        // A file that exists only to back this handle goes when the handle does.
        // Done after everything above, so the log has been checkpointed and both
        // files are closed before either is removed.
        if (self.owns_backing_file) {
            const wal_path = std.fmt.allocPrint(self.allocator, "{s}-wal", .{self.path}) catch null;
            if (wal_path) |wp| {
                defer self.allocator.free(wp);
                @import("compat").fs.cwd().deleteFile(wp) catch {};
            }
            @import("compat").fs.cwd().deleteFile(self.path) catch {};
        }

        // Free path
        self.allocator.free(self.path);

        // Free self
        self.allocator.destroy(self);
    }

    // ========================================================================
    // Transaction Management
    // ========================================================================

    /// Begin a new transaction
    /// Returns a Transaction handle that must be passed to mutating operations
    pub fn beginTransaction(self: *Self, mode: TxnMode) DatabaseError!Transaction {
        if (self.txn_manager) |*tm| {
            if (mode == .read_write and self.active_writer_txn_id != null) {
                return DatabaseError.TransactionConflict;
            }

            var txn = tm.begin(mode, .snapshot) catch |err| {
                return switch (err) {
                    TxnError.TooManyTransactions => DatabaseError.OutOfMemory,
                    else => DatabaseError.IoError,
                };
            };
            self.txn_overlays.put(txn.id, .{}) catch {
                tm.abort(&txn) catch {};
                return DatabaseError.OutOfMemory;
            };
            if (mode == .read_write) {
                self.active_writer_txn_id = txn.id;
            }
            return txn;
        }
        return DatabaseError.TransactionsNotEnabled;
    }

    /// Commit a transaction, making all changes durable
    pub fn commitTransaction(self: *Self, txn: *Transaction) DatabaseError!void {
        if (self.txn_manager) |*tm| {
            var should_broadcast_streams = false;
            if (self.txn_overlays.getPtr(txn.id)) |overlay| {
                if (overlay.hasChanges()) {
                    try self.captureTxnSnapshotsForCommit(txn.id, overlay);
                    try self.stageGraphChangefeed(overlay);
                    try self.assignStreamSequences(overlay);
                    should_broadcast_streams = overlay.stream_appends.items.len > 0 or
                        overlay.stream_offsets.items.len > 0 or
                        overlay.stream_trims.items.len > 0;
                    try self.logTxnOverlayForCommit(txn, overlay);
                    try self.applyTxnOverlay(txn.id, overlay);
                }
            }
            tm.commit(txn) catch |err| {
                return switch (err) {
                    TxnError.NotActive => DatabaseError.TransactionNotActive,
                    TxnError.NotFound => DatabaseError.NotFound,
                    else => DatabaseError.IoError,
                };
            };
            if (self.txn_overlays.fetchRemove(txn.id)) |entry| {
                var overlay = entry.value;
                overlay.deinit(self.allocator);
            }
            if (self.active_writer_txn_id == txn.id) {
                self.active_writer_txn_id = null;
            }
            if (should_broadcast_streams) {
                self.stream_mutex.lock();
                self.stream_epoch +%= 1;
                self.stream_cond.broadcast();
                self.stream_mutex.unlock();
            }
            self.maybeAutoCheckpoint();
            return;
        }
        return DatabaseError.TransactionsNotEnabled;
    }

    /// Abort a transaction, rolling back all changes
    pub fn abortTransaction(self: *Self, txn: *Transaction) DatabaseError!void {
        if (self.txn_manager) |*tm| {
            var used_overlay = false;
            if (self.txn_overlays.getPtr(txn.id)) |overlay| {
                if (overlay.hasChanges()) {
                    used_overlay = true;
                }
            }

            if (!used_overlay) {
                // Execute undo operations in reverse order before aborting
                if (tm.getUndoLog(txn)) |undo_log| {
                    var i = undo_log.len;
                    while (i > 0) {
                        i -= 1;
                        self.executeUndo(undo_log[i]) catch {
                            // Log error but continue with remaining undos
                        };
                    }
                }
            }

            tm.abort(txn) catch |err| {
                return switch (err) {
                    TxnError.NotActive => DatabaseError.TransactionNotActive,
                    TxnError.NotFound => DatabaseError.NotFound,
                    else => DatabaseError.IoError,
                };
            };
            if (self.txn_overlays.fetchRemove(txn.id)) |entry| {
                var overlay = entry.value;
                overlay.deinit(self.allocator);
            }
            if (self.active_writer_txn_id == txn.id) {
                self.active_writer_txn_id = null;
            }
            return;
        }
        return DatabaseError.TransactionsNotEnabled;
    }

    fn captureTxnSnapshotsForCommit(
        self: *Self,
        writer_txn_id: u64,
        overlay: *const TxnOverlay,
    ) DatabaseError!void {
        var reader_iter = self.txn_overlays.iterator();
        while (reader_iter.next()) |reader_entry| {
            if (reader_entry.key_ptr.* == writer_txn_id) continue;
            var reader_overlay = reader_entry.value_ptr;

            var node_iter = overlay.node_states.iterator();
            while (node_iter.next()) |node_entry| {
                const node_id = node_entry.key_ptr.*;
                if (reader_overlay.node_states.contains(node_id)) continue;

                if (try self.readBaseNodeState(node_id)) |state| {
                    try self.storeNodeOverlayState(reader_overlay, node_id, state);
                } else {
                    try self.storeNodeOverlayState(reader_overlay, node_id, .{
                        .exists = false,
                        .labels = emptySymbolIds(self.allocator) catch return DatabaseError.OutOfMemory,
                        .properties = emptyProperties(self.allocator) catch return DatabaseError.OutOfMemory,
                    });
                }
            }

            var edge_iter = overlay.edge_states.iterator();
            while (edge_iter.next()) |edge_entry| {
                const edge_id = edge_entry.key_ptr.*;
                if (reader_overlay.edge_states.contains(edge_id)) continue;

                if (try self.readBaseEdgeState(edge_id)) |state| {
                    try self.storeEdgeOverlayState(reader_overlay, edge_id, state);
                } else {
                    try self.storeEdgeOverlayState(reader_overlay, edge_id, .{
                        .exists = false,
                        .source = 0,
                        .target = 0,
                        .edge_type = 0,
                        .properties = emptyProperties(self.allocator) catch return DatabaseError.OutOfMemory,
                    });
                }
            }

            var vector_iter = overlay.vector_states.iterator();
            while (vector_iter.next()) |vector_entry| {
                const node_id = vector_entry.key_ptr.*;
                if (reader_overlay.vector_states.contains(node_id)) continue;
                const state = try self.readBaseVectorState(node_id);
                try self.storeVectorOverlayState(reader_overlay, node_id, state);
            }

        }
    }

    fn logTxnOverlayForCommit(
        self: *Self,
        txn: *Transaction,
        overlay: *const TxnOverlay,
    ) DatabaseError!void {
        var node_iter = overlay.node_states.iterator();
        while (node_iter.next()) |entry| {
            try self.logCommittedNodeState(txn, entry.key_ptr.*, entry.value_ptr.*);
        }

        var edge_iter = overlay.edge_states.iterator();
        while (edge_iter.next()) |entry| {
            try self.logCommittedEdgeState(txn, entry.key_ptr.*, entry.value_ptr.*);
        }

        for (overlay.stream_appends.items) |append| {
            try self.logWalStreamAppend(txn, append);
        }
        for (overlay.stream_offsets.items) |offset| {
            try self.logWalStreamOffset(txn, offset);
        }
        for (overlay.stream_trims.items) |trim_op| {
            try self.logWalStreamTrim(txn, trim_op);
        }
    }

    fn logCommittedNodeState(
        self: *Self,
        txn: *Transaction,
        node_id: NodeId,
        state: TxnNodeState,
    ) DatabaseError!void {
        const current = try self.readBaseNodeState(node_id);
        defer if (current) |existing| {
            var owned_existing = existing;
            owned_existing.deinit(self.allocator);
        };

        if (!state.exists) {
            if (current) |existing| {
                try self.logWalNodeDelete(txn, node_id, existing);
            }
            return;
        }

        if (current) |existing| {
            for (existing.labels) |label_id| {
                if (!containsSymbolId(state.labels, label_id)) {
                    try self.logWalLabelMutation(txn, .label_remove, node_id, label_id);
                }
            }
            for (state.labels) |label_id| {
                if (!containsSymbolId(existing.labels, label_id)) {
                    try self.logWalLabelMutation(txn, .label_add, node_id, label_id);
                }
            }

            for (existing.properties) |prop| {
                if (findPropertyById(state.properties, prop.key_id) == null) {
                    try self.logWalNodePropertyMutation(txn, node_id, prop.key_id, null, null);
                }
            }
            for (state.properties) |prop| {
                const old_prop = findPropertyById(existing.properties, prop.key_id);
                if (old_prop) |old| {
                    if (propertyValueEqual(old.value, prop.value)) continue;
                }
                try self.logWalNodePropertyMutation(
                    txn,
                    node_id,
                    prop.key_id,
                    null,
                    prop.value,
                );
            }
            return;
        }

        try self.logWalNodeInsert(txn, node_id, state.labels);
        for (state.properties) |prop| {
            try self.logWalNodePropertyMutation(txn, node_id, prop.key_id, null, prop.value);
        }
    }

    fn logCommittedEdgeState(
        self: *Self,
        txn: *Transaction,
        edge_id: EdgeId,
        state: TxnEdgeState,
    ) DatabaseError!void {
        const current = try self.readBaseEdgeState(edge_id);
        defer if (current) |existing| {
            var owned_existing = existing;
            owned_existing.deinit(self.allocator);
        };

        if (!state.exists) {
            if (current) |existing| {
                try self.logWalEdgeSnapshot(txn, edge_id, existing, .delete);
            } else if (state.source != 0 or state.target != 0 or state.edge_type != 0) {
                try self.logWalEdgeInsert(txn, edge_id, state.source, state.target, state.edge_type);
                try self.logWalEdgeSnapshot(txn, edge_id, state, .delete);
            }
            return;
        }

        if (current == null) {
            try self.logWalEdgeInsert(txn, edge_id, state.source, state.target, state.edge_type);
            for (state.properties) |prop| {
                try self.logWalEdgePropertyMutation(txn, edge_id, prop.key_id, null, prop.value);
            }
            return;
        }

        const existing = current.?;
        for (existing.properties) |prop| {
            if (findPropertyById(state.properties, prop.key_id) == null) {
                try self.logWalEdgePropertyMutation(txn, edge_id, prop.key_id, null, null);
            }
        }
        for (state.properties) |prop| {
            const old_prop = findPropertyById(existing.properties, prop.key_id);
            if (old_prop) |old| {
                if (propertyValueEqual(old.value, prop.value)) continue;
            }
            try self.logWalEdgePropertyMutation(
                txn,
                edge_id,
                prop.key_id,
                null,
                prop.value,
            );
        }
    }

    fn logWalNodeInsert(
        self: *Self,
        txn: *Transaction,
        node_id: NodeId,
        label_ids: []const symbols_mod.SymbolId,
    ) DatabaseError!void {
        const tm = &(self.txn_manager orelse return DatabaseError.TransactionsNotEnabled);

        var label_names: std.ArrayListUnmanaged([]const u8) = .empty;
        defer {
            for (label_names.items) |label_name| {
                self.allocator.free(label_name);
            }
            label_names.deinit(self.allocator);
        }

        for (label_ids) |label_id| {
            const label_name = self.symbol_table.resolve(label_id) catch return DatabaseError.IoError;
            label_names.append(self.allocator, label_name) catch {
                self.allocator.free(label_name);
                return DatabaseError.OutOfMemory;
            };
        }

        const payload_buf = self.allocator.alloc(
            u8,
            wal_payload.nodeInsertSize(label_names.items),
        ) catch return DatabaseError.OutOfMemory;
        defer self.allocator.free(payload_buf);

        const payload = wal_payload.serializeNodeInsert(
            payload_buf,
            node_id,
            label_names.items,
        ) catch return DatabaseError.IoError;

        _ = tm.logOperation(txn, .insert, payload) catch |err| return mapTxnError(err);
    }

    fn logWalNodeDelete(
        self: *Self,
        txn: *Transaction,
        node_id: NodeId,
        state: TxnNodeState,
    ) DatabaseError!void {
        const tm = &(self.txn_manager orelse return DatabaseError.TransactionsNotEnabled);

        const payload_buf = self.allocator.alloc(
            u8,
            wal_payload.nodeDeleteSize(state.labels.len, 0),
        ) catch return DatabaseError.OutOfMemory;
        defer self.allocator.free(payload_buf);

        const payload = wal_payload.serializeNodeDelete(
            payload_buf,
            node_id,
            state.labels,
            &[_]u8{},
        ) catch return DatabaseError.IoError;

        _ = tm.logOperation(txn, .delete, payload) catch |err| return mapTxnError(err);
    }

    fn logWalNodePropertyMutation(
        self: *Self,
        txn: *Transaction,
        node_id: NodeId,
        key_id: symbols_mod.SymbolId,
        old_value: ?PropertyValue,
        new_value: ?PropertyValue,
    ) DatabaseError!void {
        const tm = &(self.txn_manager orelse return DatabaseError.TransactionsNotEnabled);

        const key = self.symbol_table.resolve(key_id) catch return DatabaseError.IoError;
        defer self.allocator.free(key);

        const old_value_bytes = try self.serializePropertyValueAlloc(old_value);
        defer if (old_value_bytes) |bytes| self.allocator.free(bytes);

        const new_value_bytes = try self.serializePropertyValueAlloc(new_value);
        defer if (new_value_bytes) |bytes| self.allocator.free(bytes);

        const old_len = if (old_value_bytes) |bytes| bytes.len else 0;
        const new_slice = new_value_bytes orelse &[_]u8{};
        const payload_buf = self.allocator.alloc(
            u8,
            wal_payload.propertyUpdateSize(key.len, old_len, new_slice.len),
        ) catch return DatabaseError.OutOfMemory;
        defer self.allocator.free(payload_buf);

        const payload = wal_payload.serializePropertyUpdate(
            payload_buf,
            node_id,
            key,
            old_value_bytes,
            new_slice,
        ) catch return DatabaseError.IoError;

        _ = tm.logOperation(txn, .update, payload) catch |err| return mapTxnError(err);
    }

    fn logWalLabelMutation(
        self: *Self,
        txn: *Transaction,
        payload_type: wal_payload.PayloadType,
        node_id: NodeId,
        label_id: symbols_mod.SymbolId,
    ) DatabaseError!void {
        const tm = &(self.txn_manager orelse return DatabaseError.TransactionsNotEnabled);

        const label = self.symbol_table.resolve(label_id) catch return DatabaseError.IoError;
        defer self.allocator.free(label);

        const payload_buf = self.allocator.alloc(u8, 1 + 8 + 2 + label.len) catch {
            return DatabaseError.OutOfMemory;
        };
        defer self.allocator.free(payload_buf);

        const payload = switch (payload_type) {
            .label_add => wal_payload.serializeLabelAdd(payload_buf, node_id, label),
            .label_remove => wal_payload.serializeLabelRemove(payload_buf, node_id, label),
            else => unreachable,
        } catch return DatabaseError.IoError;

        _ = tm.logOperation(txn, .update, payload) catch |err| return mapTxnError(err);
    }

    fn logWalEdgeInsert(
        self: *Self,
        txn: *Transaction,
        edge_id: EdgeId,
        source: NodeId,
        target: NodeId,
        edge_type_id: symbols_mod.SymbolId,
    ) DatabaseError!void {
        const tm = &(self.txn_manager orelse return DatabaseError.TransactionsNotEnabled);

        const edge_type = self.symbol_table.resolve(edge_type_id) catch return DatabaseError.IoError;
        defer self.allocator.free(edge_type);

        const payload_buf = self.allocator.alloc(
            u8,
            wal_payload.edgeInsertSize(edge_type),
        ) catch return DatabaseError.OutOfMemory;
        defer self.allocator.free(payload_buf);

        const payload = wal_payload.serializeEdgeInsert(
            payload_buf,
            edge_id,
            source,
            target,
            edge_type,
        ) catch return DatabaseError.IoError;

        _ = tm.logOperation(txn, .insert, payload) catch |err| return mapTxnError(err);
    }

    fn logWalEdgeSnapshot(
        self: *Self,
        txn: *Transaction,
        edge_id: EdgeId,
        state: TxnEdgeState,
        record_type: WalRecordType,
    ) DatabaseError!void {
        const tm = &(self.txn_manager orelse return DatabaseError.TransactionsNotEnabled);

        var prop_bytes: []u8 = &[_]u8{};
        var owns_prop_bytes = false;
        if (record_type != .delete and state.properties.len > 0) {
            prop_bytes = wal_payload.serializeProperties(self.allocator, @ptrCast(state.properties)) catch return DatabaseError.OutOfMemory;
            owns_prop_bytes = true;
        }
        defer if (owns_prop_bytes) self.allocator.free(prop_bytes);

        const payload_buf = self.allocator.alloc(
            u8,
            wal_payload.edgeDeleteSize(prop_bytes.len),
        ) catch return DatabaseError.OutOfMemory;
        defer self.allocator.free(payload_buf);

        const payload = wal_payload.serializeEdgeDelete(
            payload_buf,
            edge_id,
            state.source,
            state.target,
            state.edge_type,
            prop_bytes,
        ) catch return DatabaseError.IoError;

        _ = tm.logOperation(txn, record_type, payload) catch |err| return mapTxnError(err);
    }

    fn logWalEdgePropertyMutation(
        self: *Self,
        txn: *Transaction,
        edge_id: EdgeId,
        key_id: symbols_mod.SymbolId,
        old_value: ?PropertyValue,
        new_value: ?PropertyValue,
    ) DatabaseError!void {
        const tm = &(self.txn_manager orelse return DatabaseError.TransactionsNotEnabled);

        const key = self.symbol_table.resolve(key_id) catch return DatabaseError.IoError;
        defer self.allocator.free(key);

        const old_value_bytes = try self.serializePropertyValueAlloc(old_value);
        defer if (old_value_bytes) |bytes| self.allocator.free(bytes);

        const new_value_bytes = try self.serializePropertyValueAlloc(new_value);
        defer if (new_value_bytes) |bytes| self.allocator.free(bytes);

        const old_len = if (old_value_bytes) |bytes| bytes.len else 0;
        const new_slice = new_value_bytes orelse &[_]u8{};
        const payload_buf = self.allocator.alloc(
            u8,
            wal_payload.edgePropertyUpdateSize(key.len, old_len, new_slice.len),
        ) catch return DatabaseError.OutOfMemory;
        defer self.allocator.free(payload_buf);

        const payload = wal_payload.serializeEdgePropertyUpdate(
            payload_buf,
            edge_id,
            key,
            old_value_bytes,
            new_slice,
        ) catch return DatabaseError.IoError;

        _ = tm.logOperation(txn, .update, payload) catch |err| return mapTxnError(err);
    }

    fn logWalStreamAppend(
        self: *Self,
        txn: *Transaction,
        append: TxnStreamAppend,
    ) DatabaseError!void {
        const tm = &(self.txn_manager orelse return DatabaseError.TransactionsNotEnabled);
        const sequence = append.sequence orelse return DatabaseError.IoError;
        const payload = stream_store_mod.serializeAppendWalAlloc(
            self.allocator,
            append.stream,
            sequence,
            append.kind,
            append.payload,
        ) catch |err| return self.mapStreamError(err);
        defer self.allocator.free(payload);
        _ = tm.logOperation(txn, .insert, payload) catch |err| return mapTxnError(err);
    }

    fn logWalStreamOffset(
        self: *Self,
        txn: *Transaction,
        offset: TxnStreamOffset,
    ) DatabaseError!void {
        const tm = &(self.txn_manager orelse return DatabaseError.TransactionsNotEnabled);
        const payload = stream_store_mod.serializeOffsetWalAlloc(
            self.allocator,
            offset.stream,
            offset.consumer,
            offset.sequence,
        ) catch |err| return self.mapStreamError(err);
        defer self.allocator.free(payload);
        _ = tm.logOperation(txn, .update, payload) catch |err| return mapTxnError(err);
    }

    fn logWalStreamTrim(
        self: *Self,
        txn: *Transaction,
        trim_op: TxnStreamTrim,
    ) DatabaseError!void {
        const tm = &(self.txn_manager orelse return DatabaseError.TransactionsNotEnabled);
        const payload = stream_store_mod.serializeTrimWalAlloc(
            self.allocator,
            trim_op.stream,
            trim_op.through_sequence,
        ) catch |err| return self.mapStreamError(err);
        defer self.allocator.free(payload);
        _ = tm.logOperation(txn, .delete, payload) catch |err| return mapTxnError(err);
    }

    fn applyTxnOverlay(
        self: *Self,
        writer_txn_id: u64,
        overlay: *TxnOverlay,
    ) DatabaseError!void {
        _ = writer_txn_id;
        var node_iter = overlay.node_states.iterator();
        while (node_iter.next()) |entry| {
            try self.applyNodeState(entry.key_ptr.*, entry.value_ptr.*);
        }

        var edge_iter = overlay.edge_states.iterator();
        while (edge_iter.next()) |entry| {
            try self.applyEdgeState(entry.key_ptr.*, entry.value_ptr.*);
        }
        if (overlay.edge_states.count() > 0) {
            self.edge_store.syncNextEdgeId() catch return DatabaseError.IoError;
        }

        var vector_iter = overlay.vector_states.iterator();
        while (vector_iter.next()) |entry| {
            switch (entry.value_ptr.*) {
                .absent => {
                    if (self.hnsw_index) |*hnsw| {
                        hnsw.remove(entry.key_ptr.*) catch |err| return mapHnswError(err);
                    }
                },
                .value => |vector| try self.setNodeVector(entry.key_ptr.*, vector),
            }
        }

        if (overlay.stream_appends.items.len > 0 or
            overlay.stream_offsets.items.len > 0 or
            overlay.stream_trims.items.len > 0)
        {
            const trees = try self.streamTreesForWrite();
            for (overlay.stream_appends.items) |append| {
                const sequence = append.sequence orelse return DatabaseError.IoError;
                stream_store_mod.appendWithSequence(
                    trees,
                    self.allocator,
                    append.stream,
                    sequence,
                    append.kind,
                    append.payload,
                ) catch |err| return self.mapStreamError(err);
            }
            for (overlay.stream_offsets.items) |offset| {
                stream_store_mod.setOffset(
                    trees,
                    self.allocator,
                    offset.stream,
                    offset.consumer,
                    offset.sequence,
                ) catch |err| return self.mapStreamError(err);
            }
            for (overlay.stream_trims.items) |trim_op| {
                stream_store_mod.trim(
                    trees,
                    self.allocator,
                    trim_op.stream,
                    trim_op.through_sequence,
                ) catch |err| return self.mapStreamError(err);
            }
        }
    }

    fn replaceNodePropertyIndex(
        self: *Self,
        node_id: NodeId,
        old_labels: []const symbols_mod.SymbolId,
        old_properties: []const node_mod.Property,
        new_labels: []const symbols_mod.SymbolId,
        new_properties: []const node_mod.Property,
    ) DatabaseError!void {
        if (self.property_index) |*index| {
            index.removeNode(node_id, old_labels, old_properties) catch |err| return mapPropertyIndexError(err);
            index.indexNode(node_id, new_labels, new_properties) catch |err| return mapPropertyIndexError(err);
        }
        try self.ftsReplaceNode(node_id, old_labels, old_properties, new_labels, new_properties);
    }

    fn replaceEdgePropertyIndex(
        self: *Self,
        edge_id: EdgeId,
        old_type: symbols_mod.SymbolId,
        old_properties: []const node_mod.Property,
        new_type: symbols_mod.SymbolId,
        new_properties: []const node_mod.Property,
    ) DatabaseError!void {
        if (self.property_index) |*index| {
            index.removeEdge(edge_id, old_type, old_properties) catch |err| return mapPropertyIndexError(err);
            index.indexEdge(edge_id, new_type, new_properties) catch |err| return mapPropertyIndexError(err);
        }
        try self.ftsReplaceEdge(edge_id, old_type, old_properties, new_type, new_properties, true, true);
    }

    fn applyNodeState(self: *Self, node_id: NodeId, state: TxnNodeState) DatabaseError!void {
        const current = try self.readBaseNodeState(node_id);
        defer if (current) |existing| {
            var owned_existing = existing;
            owned_existing.deinit(self.allocator);
        };

        if (!state.exists) {
            if (current) |existing| {
                for (existing.labels) |label_id| {
                    self.label_index.remove(label_id, node_id) catch {};
                }
                self.node_store.delete(node_id) catch |err| switch (err) {
                    node_mod.NodeError.NotFound => {},
                    else => return DatabaseError.IoError,
                };
                if (self.property_index) |*index| {
                    index.removeNode(node_id, existing.labels, existing.properties) catch |err| return mapPropertyIndexError(err);
                }
                try self.ftsReplaceNode(node_id, existing.labels, existing.properties, &.{}, &.{});
            }
            return;
        }

        if (current) |existing| {
            self.node_store.update(node_id, state.labels, state.properties) catch |err| {
                return mapNodeStoreError(err);
            };

            for (existing.labels) |label_id| {
                if (!containsSymbolId(state.labels, label_id)) {
                    self.label_index.remove(label_id, node_id) catch {};
                }
            }
            for (state.labels) |label_id| {
                if (!containsSymbolId(existing.labels, label_id)) {
                    self.label_index.add(label_id, node_id) catch return DatabaseError.IoError;
                }
            }
            if (self.property_index) |*index| {
                index.removeNode(node_id, existing.labels, existing.properties) catch |err| return mapPropertyIndexError(err);
                index.indexNode(node_id, state.labels, state.properties) catch |err| return mapPropertyIndexError(err);
            }
            try self.ftsReplaceNode(node_id, existing.labels, existing.properties, state.labels, state.properties);
            return;
        }

        self.node_store.createWithId(node_id, state.labels, state.properties) catch |err| {
            return mapNodeStoreError(err);
        };
        for (state.labels) |label_id| {
            self.label_index.add(label_id, node_id) catch return DatabaseError.IoError;
        }
        if (self.property_index) |*index| {
            index.indexNode(node_id, state.labels, state.properties) catch |err| return mapPropertyIndexError(err);
        }
        try self.ftsReplaceNode(node_id, &.{}, &.{}, state.labels, state.properties);
    }

    fn applyEdgeState(self: *Self, edge_id: EdgeId, state: TxnEdgeState) DatabaseError!void {
        const current = try self.readBaseEdgeState(edge_id);
        defer if (current) |existing| {
            var owned_existing = existing;
            owned_existing.deinit(self.allocator);
        };

        if (!state.exists) {
            if (current != null) {
                self.edge_store.deleteById(edge_id) catch |err| switch (err) {
                    edge_mod.EdgeError.NotFound => {},
                    else => return DatabaseError.IoError,
                };
                if (self.property_index) |*index| {
                    const existing = current.?;
                    index.removeEdge(edge_id, existing.edge_type, existing.properties) catch |err| return mapPropertyIndexError(err);
                }
                const existing = current.?;
                try self.ftsReplaceEdge(edge_id, existing.edge_type, existing.properties, 0, &.{}, true, false);
            }
            return;
        }

        if (!self.edge_store.canFitEdge(edge_id, state.source, state.target, state.edge_type, state.properties)) {
            return DatabaseError.ValueTooLarge;
        }

        if (current != null) {
            self.edge_store.deleteById(edge_id) catch |err| switch (err) {
                edge_mod.EdgeError.NotFound => {},
                else => return DatabaseError.IoError,
            };
        }

        self.edge_store.createWithId(edge_id, state.source, state.target, state.edge_type, state.properties) catch |err| {
            return mapEdgeStoreError(err);
        };
        if (self.property_index) |*index| {
            if (current) |existing| {
                index.removeEdge(edge_id, existing.edge_type, existing.properties) catch |err| return mapPropertyIndexError(err);
            }
            index.indexEdge(edge_id, state.edge_type, state.properties) catch |err| return mapPropertyIndexError(err);
        }
        if (current) |existing| {
            try self.ftsReplaceEdge(edge_id, existing.edge_type, existing.properties, state.edge_type, state.properties, true, true);
        } else {
            try self.ftsReplaceEdge(edge_id, 0, &.{}, state.edge_type, state.properties, false, true);
        }
    }

    /// Execute a single undo operation
    fn executeUndo(self: *Self, entry: UndoEntry) !void {
        switch (entry.entity_type) {
            .node => switch (entry.op_type) {
                // Undo of insert is delete
                .insert => {
                    // First remove from label index
                    if (self.node_store.get(entry.entity_id)) |existing| {
                        var node = existing;
                        defer node.deinit(self.allocator);
                        for (node.labels) |label_id| {
                            self.label_index.remove(label_id, entry.entity_id) catch {};
                        }
                    } else |_| {}
                    self.node_store.delete(entry.entity_id) catch {};
                },
                // Undo of delete is re-insert using prev_data
                .delete => {
                    if (entry.prev_data) |data| {
                        const payload = wal_payload.deserializeNodeDelete(data) catch return;

                        // Extract label IDs
                        var label_ids = self.allocator.alloc(u16, payload.label_count) catch return;
                        defer self.allocator.free(label_ids);
                        for (0..payload.label_count) |idx| {
                            label_ids[idx] = payload.getLabelId(idx);
                        }

                        // Deserialize properties if present
                        var properties: []node_mod.Property = &[_]node_mod.Property{};
                        var owns_properties = false;
                        if (payload.properties.len > 0) {
                            const wal_props = wal_payload.deserializeProperties(self.allocator, payload.properties) catch &[_]wal_payload.Property{};
                            if (wal_props.len > 0) {
                                // Convert wal_payload.Property to node_mod.Property (same layout)
                                properties = @ptrCast(@constCast(wal_props));
                                owns_properties = true;
                            }
                        }
                        defer if (owns_properties) {
                            for (properties) |*prop| {
                                var val = prop.value;
                                val.deinit(self.allocator);
                            }
                            self.allocator.free(properties);
                        };

                        // Re-create node with ORIGINAL ID using createWithId
                        self.node_store.createWithId(entry.entity_id, label_ids, properties) catch {};

                        // Restore label index entries
                        for (label_ids) |label_id| {
                            self.label_index.add(label_id, entry.entity_id) catch {};
                        }
                    }
                },
                .update => {
                    // Property update undo - restore from prev_data
                    if (entry.prev_data) |data| {
                        const update_payload = wal_payload.deserializePropertyUpdate(data) catch return;

                        // Intern the key to get key_id for comparison
                        const key_id = self.symbol_table.intern(update_payload.key) catch return;

                        // Get current node
                        var node = self.node_store.get(entry.entity_id) catch return;
                        defer node.deinit(self.allocator);

                        // Build new properties array
                        var new_props: std.ArrayListUnmanaged(node_mod.Property) = .empty;
                        defer new_props.deinit(self.allocator);

                        // Copy existing properties, replacing or removing the updated one
                        for (node.properties) |prop| {
                            if (prop.key_id == key_id) {
                                // If there was an old value, restore it
                                if (update_payload.old_value) |old_bytes| {
                                    const old_val = wal_payload.deserializePropertyValueFromBytes(self.allocator, old_bytes) catch continue;
                                    new_props.append(self.allocator, .{ .key_id = prop.key_id, .value = old_val }) catch continue;
                                }
                                // If old_value is null, property didn't exist before - skip it
                            } else {
                                new_props.append(self.allocator, prop) catch continue;
                            }
                        }

                        // Update the node with restored properties
                        self.node_store.update(entry.entity_id, node.labels, new_props.items) catch {};
                    }
                },
            },
            .edge => switch (entry.op_type) {
                // Undo of insert is delete by stable edge_id.
                .insert => {
                    self.edge_store.deleteById(entry.entity_id) catch {};
                },
                // Undo of delete is re-insert with original edge_id and properties.
                .delete => {
                    const data = entry.prev_data orelse return;
                    const payload = wal_payload.deserializeEdgeDelete(data) catch return;

                    var properties: []node_mod.Property = &[_]node_mod.Property{};
                    var owns_properties = false;
                    if (payload.properties.len > 0) {
                        const wal_props = wal_payload.deserializeProperties(self.allocator, payload.properties) catch &[_]wal_payload.Property{};
                        if (wal_props.len > 0) {
                            properties = @ptrCast(@constCast(wal_props));
                            owns_properties = true;
                        }
                    }
                    defer if (owns_properties) {
                        for (properties) |*prop| {
                            var val = prop.value;
                            val.deinit(self.allocator);
                        }
                        self.allocator.free(properties);
                    };

                    self.edge_store.createWithId(
                        payload.edge_id,
                        payload.source,
                        payload.target,
                        payload.type_id,
                        properties,
                    ) catch {};
                },
                .update => {
                    const data = entry.prev_data orelse return;
                    const payload = wal_payload.deserializeEdgeDelete(data) catch return;

                    var properties: []node_mod.Property = &[_]node_mod.Property{};
                    var owns_properties = false;
                    if (payload.properties.len > 0) {
                        const wal_props = wal_payload.deserializeProperties(self.allocator, payload.properties) catch &[_]wal_payload.Property{};
                        if (wal_props.len > 0) {
                            properties = @ptrCast(@constCast(wal_props));
                            owns_properties = true;
                        }
                    }
                    defer if (owns_properties) {
                        for (properties) |*prop| {
                            var val = prop.value;
                            val.deinit(self.allocator);
                        }
                        self.allocator.free(properties);
                    };

                    // Replace edge state with the previous snapshot.
                    self.edge_store.deleteById(entry.entity_id) catch {};
                    self.edge_store.createWithId(
                        payload.edge_id,
                        payload.source,
                        payload.target,
                        payload.type_id,
                        properties,
                    ) catch {};
                },
            },
            .property => switch (entry.op_type) {
                // Undo of property insert is delete the property
                .insert => {
                    // Get current node and remove the property
                    var node = self.node_store.get(entry.entity_id) catch return;
                    defer node.deinit(self.allocator);

                    var new_props: std.ArrayListUnmanaged(node_mod.Property) = .empty;
                    defer new_props.deinit(self.allocator);

                    // Copy all properties except the one being removed
                    for (node.properties) |prop| {
                        if (prop.key_id != entry.type_id) {
                            new_props.append(self.allocator, prop) catch continue;
                        }
                    }

                    self.node_store.update(entry.entity_id, node.labels, new_props.items) catch {};
                },
                .update, .delete => {
                    // Restore from prev_data (same as node .update)
                    if (entry.prev_data) |data| {
                        const update_payload = wal_payload.deserializePropertyUpdate(data) catch return;

                        // Intern the key to get key_id for comparison
                        const key_id = self.symbol_table.intern(update_payload.key) catch return;

                        var node = self.node_store.get(entry.entity_id) catch return;
                        defer node.deinit(self.allocator);

                        var new_props: std.ArrayListUnmanaged(node_mod.Property) = .empty;
                        defer new_props.deinit(self.allocator);

                        // Track deserialized string values that need freeing after update()
                        var deserialized_string: ?[]const u8 = null;

                        var found = false;
                        for (node.properties) |prop| {
                            if (prop.key_id == key_id) {
                                found = true;
                                if (update_payload.old_value) |old_bytes| {
                                    const old_val = wal_payload.deserializePropertyValueFromBytes(self.allocator, old_bytes) catch continue;
                                    if (old_val == .string_val) {
                                        deserialized_string = old_val.string_val;
                                    }
                                    new_props.append(self.allocator, .{ .key_id = prop.key_id, .value = old_val }) catch continue;
                                }
                            } else {
                                new_props.append(self.allocator, prop) catch continue;
                            }
                        }

                        // If property was deleted, we need to add it back
                        if (!found and update_payload.old_value != null) {
                            if (update_payload.old_value) |old_bytes| {
                                const old_val = wal_payload.deserializePropertyValueFromBytes(self.allocator, old_bytes) catch return;
                                if (old_val == .string_val) {
                                    deserialized_string = old_val.string_val;
                                }
                                new_props.append(self.allocator, .{ .key_id = key_id, .value = old_val }) catch return;
                            }
                        }

                        self.node_store.update(entry.entity_id, node.labels, new_props.items) catch {};

                        // Free deserialized string after update() has copied it
                        if (deserialized_string) |s| {
                            self.allocator.free(s);
                        }
                    }
                },
            },
            .label => switch (entry.op_type) {
                // Undo of label insert is remove the label
                .insert => {
                    const label_id: symbols_mod.SymbolId = entry.type_id;

                    // Get current node
                    var node = self.node_store.get(entry.entity_id) catch return;
                    defer node.deinit(self.allocator);

                    // Build new labels array without the removed label
                    var new_labels: std.ArrayListUnmanaged(symbols_mod.SymbolId) = .empty;
                    defer new_labels.deinit(self.allocator);

                    for (node.labels) |l| {
                        if (l != label_id) {
                            new_labels.append(self.allocator, l) catch continue;
                        }
                    }

                    // Update the node
                    self.node_store.update(entry.entity_id, new_labels.items, node.properties) catch {};

                    // Update label index
                    self.label_index.remove(label_id, entry.entity_id) catch {};
                },
                // Undo of label delete is add the label back
                .delete => {
                    const label_id: symbols_mod.SymbolId = entry.type_id;

                    // Get current node
                    var node = self.node_store.get(entry.entity_id) catch return;
                    defer node.deinit(self.allocator);

                    // Check if label already exists
                    for (node.labels) |l| {
                        if (l == label_id) return; // Already has this label
                    }

                    // Build new labels array with the added label
                    var new_labels: std.ArrayListUnmanaged(symbols_mod.SymbolId) = .empty;
                    defer new_labels.deinit(self.allocator);

                    for (node.labels) |l| {
                        new_labels.append(self.allocator, l) catch continue;
                    }
                    new_labels.append(self.allocator, label_id) catch return;

                    // Update the node
                    self.node_store.update(entry.entity_id, new_labels.items, node.properties) catch {};

                    // Update label index
                    self.label_index.add(label_id, entry.entity_id) catch {};
                },
                .update => {},
            },
        }
    }

    // ========================================================================
    // Tree Initialization
    // ========================================================================

    fn initNewTrees(self: *Self) DatabaseError!void {
        // Create all B+Trees
        self.node_tree = BTree.init(self.allocator, &self.buffer_pool) catch {
            return DatabaseError.TreeInitFailed;
        };

        self.edge_tree = BTree.init(self.allocator, &self.buffer_pool) catch {
            return DatabaseError.TreeInitFailed;
        };
        self.edge_id_tree = BTree.init(self.allocator, &self.buffer_pool) catch {
            return DatabaseError.TreeInitFailed;
        };

        self.label_tree = BTree.init(self.allocator, &self.buffer_pool) catch {
            return DatabaseError.TreeInitFailed;
        };

        self.symbol_forward_tree = BTree.init(self.allocator, &self.buffer_pool) catch {
            return DatabaseError.TreeInitFailed;
        };

        self.symbol_reverse_tree = BTree.init(self.allocator, &self.buffer_pool) catch {
            return DatabaseError.TreeInitFailed;
        };

        self.fts_dict_tree = BTree.init(self.allocator, &self.buffer_pool) catch {
            return DatabaseError.TreeInitFailed;
        };

        self.fts_lengths_tree = BTree.init(self.allocator, &self.buffer_pool) catch {
            return DatabaseError.TreeInitFailed;
        };

        if (self.config.enable_fts) {
            self.fts_reverse_tree = BTree.init(self.allocator, &self.buffer_pool) catch {
                return DatabaseError.TreeInitFailed;
            };
        } else {
            self.fts_reverse_tree = null;
        }

        // hnsw_node_tree is created lazily on first sync when vectors exist
        self.hnsw_node_tree = null;

        self.stream_meta_tree = BTree.init(self.allocator, &self.buffer_pool) catch {
            return DatabaseError.TreeInitFailed;
        };
        self.stream_events_tree = BTree.init(self.allocator, &self.buffer_pool) catch {
            return DatabaseError.TreeInitFailed;
        };
        self.stream_offsets_tree = BTree.init(self.allocator, &self.buffer_pool) catch {
            return DatabaseError.TreeInitFailed;
        };
        self.property_index_catalog_tree = BTree.init(self.allocator, &self.buffer_pool) catch {
            return DatabaseError.TreeInitFailed;
        };
        self.node_property_index_tree = BTree.init(self.allocator, &self.buffer_pool) catch {
            return DatabaseError.TreeInitFailed;
        };
        self.edge_property_index_tree = BTree.init(self.allocator, &self.buffer_pool) catch {
            return DatabaseError.TreeInitFailed;
        };

        // Save root pages to header
        try self.saveTreeRoots();
    }

    fn loadExistingTrees(self: *Self) DatabaseError!void {
        const header = self.page_manager.getHeader();

        self.node_tree = BTree.open(
            self.allocator,
            &self.buffer_pool,
            header.getTreeRoot(.node),
        );

        self.edge_tree = BTree.open(
            self.allocator,
            &self.buffer_pool,
            header.getTreeRoot(.edge),
        );
        const edge_id_root = header.getTreeRoot(.edge_id_index);
        if (edge_id_root != NULL_PAGE) {
            self.edge_id_tree = BTree.open(
                self.allocator,
                &self.buffer_pool,
                edge_id_root,
            );
        } else {
            // New index for edge_id -> locator mappings.
            self.edge_id_tree = BTree.init(self.allocator, &self.buffer_pool) catch {
                return DatabaseError.TreeInitFailed;
            };
        }

        self.label_tree = BTree.open(
            self.allocator,
            &self.buffer_pool,
            header.getTreeRoot(.label),
        );

        self.symbol_forward_tree = BTree.open(
            self.allocator,
            &self.buffer_pool,
            header.getTreeRoot(.symbol_forward),
        );

        self.symbol_reverse_tree = BTree.open(
            self.allocator,
            &self.buffer_pool,
            header.getTreeRoot(.symbol_reverse),
        );

        self.fts_dict_tree = BTree.open(
            self.allocator,
            &self.buffer_pool,
            header.getTreeRoot(.fts_dict),
        );

        self.fts_lengths_tree = BTree.open(
            self.allocator,
            &self.buffer_pool,
            header.getTreeRoot(.fts_lengths),
        );

        const fts_reverse_root = header.getTreeRoot(.fts_reverse);
        if (fts_reverse_root != NULL_PAGE) {
            self.fts_reverse_tree = BTree.open(
                self.allocator,
                &self.buffer_pool,
                fts_reverse_root,
            );
        } else {
            self.fts_reverse_tree = null;
        }

        const hnsw_node_root = header.getTreeRoot(.hnsw_node);
        if (hnsw_node_root != NULL_PAGE) {
            self.hnsw_node_tree = BTree.open(
                self.allocator,
                &self.buffer_pool,
                hnsw_node_root,
            );
        } else {
            self.hnsw_node_tree = null;
        }

        const stream_meta_root = header.getTreeRoot(.stream_meta);
        self.stream_meta_tree = if (stream_meta_root != NULL_PAGE)
            BTree.open(self.allocator, &self.buffer_pool, stream_meta_root)
        else
            null;

        const stream_events_root = header.getTreeRoot(.stream_events);
        self.stream_events_tree = if (stream_events_root != NULL_PAGE)
            BTree.open(self.allocator, &self.buffer_pool, stream_events_root)
        else
            null;

        const stream_offsets_root = header.getTreeRoot(.stream_offsets);
        self.stream_offsets_tree = if (stream_offsets_root != NULL_PAGE)
            BTree.open(self.allocator, &self.buffer_pool, stream_offsets_root)
        else
            null;

        const property_catalog_root = header.getTreeRoot(.property_index_catalog);
        self.property_index_catalog_tree = if (property_catalog_root != NULL_PAGE)
            BTree.open(self.allocator, &self.buffer_pool, property_catalog_root)
        else
            null;
        const node_property_root = header.getTreeRoot(.node_property_index);
        self.node_property_index_tree = if (node_property_root != NULL_PAGE)
            BTree.open(self.allocator, &self.buffer_pool, node_property_root)
        else
            null;
        const edge_property_root = header.getTreeRoot(.edge_property_index);
        self.edge_property_index_tree = if (edge_property_root != NULL_PAGE)
            BTree.open(self.allocator, &self.buffer_pool, edge_property_root)
        else
            null;
    }

    fn ensurePropertyIndexTreesForWrite(self: *Self) DatabaseError!void {
        if (self.read_only) return DatabaseError.ReadOnly;
        var created = false;
        if (self.property_index_catalog_tree == null) {
            self.property_index_catalog_tree = BTree.init(self.allocator, &self.buffer_pool) catch return DatabaseError.TreeInitFailed;
            created = true;
        }
        if (self.node_property_index_tree == null) {
            self.node_property_index_tree = BTree.init(self.allocator, &self.buffer_pool) catch return DatabaseError.TreeInitFailed;
            created = true;
        }
        if (self.edge_property_index_tree == null) {
            self.edge_property_index_tree = BTree.init(self.allocator, &self.buffer_pool) catch return DatabaseError.TreeInitFailed;
            created = true;
        }
        if (created) try self.saveTreeRoots();
    }

    fn initializePropertyIndex(self: *Self) void {
        const catalog = if (self.property_index_catalog_tree) |*tree| tree else {
            self.property_index = null;
            return;
        };
        const nodes = if (self.node_property_index_tree) |*tree| tree else {
            self.property_index = null;
            return;
        };
        const edges = if (self.edge_property_index_tree) |*tree| tree else {
            self.property_index = null;
            return;
        };
        self.property_index = PropertyIndex.init(self.allocator, catalog, nodes, edges);
    }

    fn ensureStreamTreesForWrite(self: *Self) DatabaseError!void {
        if (self.read_only) return DatabaseError.ReadOnly;

        var created = false;
        if (self.stream_meta_tree == null) {
            self.stream_meta_tree = BTree.init(self.allocator, &self.buffer_pool) catch {
                return DatabaseError.TreeInitFailed;
            };
            created = true;
        }
        if (self.stream_events_tree == null) {
            self.stream_events_tree = BTree.init(self.allocator, &self.buffer_pool) catch {
                return DatabaseError.TreeInitFailed;
            };
            created = true;
        }
        if (self.stream_offsets_tree == null) {
            self.stream_offsets_tree = BTree.init(self.allocator, &self.buffer_pool) catch {
                return DatabaseError.TreeInitFailed;
            };
            created = true;
        }
        if (created) {
            try self.saveTreeRoots();
        }
    }

    fn streamTrees(self: *Self) ?stream_store_mod.TreeSet {
        const meta = if (self.stream_meta_tree) |*tree| tree else return null;
        const events = if (self.stream_events_tree) |*tree| tree else return null;
        const offsets = if (self.stream_offsets_tree) |*tree| tree else return null;
        return .{
            .meta = meta,
            .events = events,
            .offsets = offsets,
        };
    }

    fn streamTreesForWrite(self: *Self) DatabaseError!stream_store_mod.TreeSet {
        try self.ensureStreamTreesForWrite();
        return self.streamTrees() orelse DatabaseError.IoError;
    }

    fn saveTreeRoots(self: *Self) DatabaseError!void {
        var header = self.page_manager.getHeader().*;

        header.setTreeRoot(.node, self.node_tree.getRootPage());
        header.setTreeRoot(.edge, self.edge_tree.getRootPage());
        header.setTreeRoot(.edge_id_index, self.edge_id_tree.getRootPage());
        header.setTreeRoot(.label, self.label_tree.getRootPage());
        header.setTreeRoot(.symbol_forward, self.symbol_forward_tree.getRootPage());
        header.setTreeRoot(.symbol_reverse, self.symbol_reverse_tree.getRootPage());
        header.setTreeRoot(.fts_dict, self.fts_dict_tree.getRootPage());
        header.setTreeRoot(.fts_lengths, self.fts_lengths_tree.getRootPage());

        if (self.fts_reverse_tree) |*tree| {
            header.setTreeRoot(.fts_reverse, tree.getRootPage());
        }

        if (self.hnsw_node_tree) |*tree| {
            header.setTreeRoot(.hnsw_node, tree.getRootPage());
        }

        if (self.stream_meta_tree) |*tree| {
            header.setTreeRoot(.stream_meta, tree.getRootPage());
        }
        if (self.stream_events_tree) |*tree| {
            header.setTreeRoot(.stream_events, tree.getRootPage());
        }
        if (self.stream_offsets_tree) |*tree| {
            header.setTreeRoot(.stream_offsets, tree.getRootPage());
        }
        if (self.property_index_catalog_tree) |*tree| {
            header.setTreeRoot(.property_index_catalog, tree.getRootPage());
        }
        if (self.node_property_index_tree) |*tree| {
            header.setTreeRoot(.node_property_index, tree.getRootPage());
        }
        if (self.edge_property_index_tree) |*tree| {
            header.setTreeRoot(.edge_property_index, tree.getRootPage());
        }

        // Save vector storage first page for persistence
        if (self.vector_storage) |vs| {
            header.vector_segment_page = vs.first_page;
        }

        self.page_manager.updateHeader(&header) catch {
            return DatabaseError.IoError;
        };
    }

    fn propertyIndexDefinition(
        self: *Self,
        kind: PropertyIndexEntityKind,
        scope: []const u8,
        property: []const u8,
        create_symbols: bool,
    ) DatabaseError!PropertyIndexDefinition {
        const scope_id = if (create_symbols)
            self.symbol_table.intern(scope) catch return DatabaseError.IoError
        else
            self.symbol_table.lookup(scope) catch |err| switch (err) {
                symbols_mod.SymbolError.NotFound => return DatabaseError.MissingIndex,
                else => return DatabaseError.IoError,
            };
        const property_id = if (create_symbols)
            self.symbol_table.intern(property) catch return DatabaseError.IoError
        else
            self.symbol_table.lookup(property) catch |err| switch (err) {
                symbols_mod.SymbolError.NotFound => return DatabaseError.MissingIndex,
                else => return DatabaseError.IoError,
            };
        return .{ .kind = kind, .scope_id = scope_id, .property_id = property_id };
    }

    fn populatePropertyIndexDefinition(self: *Self, definition: PropertyIndexDefinition) DatabaseError!void {
        var index = &(self.property_index orelse return DatabaseError.MissingIndex);
        switch (definition.kind) {
            .node => {
                const node_ids = self.label_index.getNodesByLabel(definition.scope_id) catch return DatabaseError.IoError;
                defer self.allocator.free(node_ids);
                for (node_ids) |node_id| {
                    var state = (try self.readBaseNodeState(node_id)) orelse continue;
                    defer state.deinit(self.allocator);
                    if (findPropertyById(state.properties, definition.property_id)) |property| {
                        index.add(definition, node_id, property.value) catch |err| return mapPropertyIndexError(err);
                    }
                }
            },
            .edge => {
                var iter = self.edge_store.scanRefs() catch return DatabaseError.IoError;
                defer iter.deinit();
                while (iter.next() catch return DatabaseError.IoError) |edge_ref| {
                    if (edge_ref.edge_type != definition.scope_id) continue;
                    var state = (try self.readBaseEdgeState(edge_ref.id)) orelse continue;
                    defer state.deinit(self.allocator);
                    if (findPropertyById(state.properties, definition.property_id)) |property| {
                        index.add(definition, edge_ref.id, property.value) catch |err| return mapPropertyIndexError(err);
                    }
                }
            },
        }
    }

    fn rebuildPropertyIndexes(self: *Self) DatabaseError!void {
        var index = if (self.property_index) |*property_index| property_index else return;
        inline for (.{ PropertyIndexEntityKind.node, PropertyIndexEntityKind.edge }) |kind| {
            var iter: PropertyIndex.DefinitionIterator = undefined;
            index.iterateDefinitions(kind, &iter) catch |err| return mapPropertyIndexError(err);
            defer iter.deinit();
            while (iter.next() catch |err| return mapPropertyIndexError(err)) |definition| {
                index.clearEntries(definition) catch |err| return mapPropertyIndexError(err);
                try self.populatePropertyIndexDefinition(definition);
            }
        }
    }

    fn createPropertyIndex(
        self: *Self,
        kind: PropertyIndexEntityKind,
        scope: []const u8,
        property: []const u8,
    ) DatabaseError!void {
        if (self.read_only) return DatabaseError.ReadOnly;
        if (scope.len == 0 or property.len == 0) return DatabaseError.InvalidArgument;
        if (self.active_writer_txn_id != null) return DatabaseError.TransactionConflict;
        try self.ensurePropertyIndexTreesForWrite();
        self.initializePropertyIndex();

        const definition = try self.propertyIndexDefinition(kind, scope, property, true);
        var index = &(self.property_index orelse return DatabaseError.IoError);
        if (index.hasDefinition(definition) catch |err| return mapPropertyIndexError(err)) {
            return DatabaseError.AlreadyExists;
        }
        index.clearEntries(definition) catch |err| return mapPropertyIndexError(err);
        try self.populatePropertyIndexDefinition(definition);
        index.createDefinition(definition) catch |err| return mapPropertyIndexError(err);
        try self.saveTreeRoots();
        if (self.query_cache) |cache| cache.bumpSchemaVersion();
    }

    fn dropPropertyIndex(
        self: *Self,
        kind: PropertyIndexEntityKind,
        scope: []const u8,
        property: []const u8,
    ) DatabaseError!void {
        if (self.read_only) return DatabaseError.ReadOnly;
        if (scope.len == 0 or property.len == 0) return DatabaseError.InvalidArgument;
        if (self.active_writer_txn_id != null) return DatabaseError.TransactionConflict;
        const definition = try self.propertyIndexDefinition(kind, scope, property, false);
        var index = &(self.property_index orelse return DatabaseError.MissingIndex);
        index.dropDefinition(definition) catch |err| return mapPropertyIndexError(err);
        if (self.query_cache) |cache| cache.bumpSchemaVersion();
    }

    pub fn createNodePropertyIndex(self: *Self, label: []const u8, property: []const u8) DatabaseError!void {
        try self.createPropertyIndex(.node, label, property);
    }

    pub fn hasNodePropertyIndex(self: *Self, label: []const u8, property: []const u8) DatabaseError!bool {
        const definition = self.propertyIndexDefinition(.node, label, property, false) catch |err| switch (err) {
            DatabaseError.MissingIndex => return false,
            else => return err,
        };
        var index = &(self.property_index orelse return false);
        return index.hasDefinition(definition) catch |err| return mapPropertyIndexError(err);
    }

    pub fn dropNodePropertyIndex(self: *Self, label: []const u8, property: []const u8) DatabaseError!void {
        try self.dropPropertyIndex(.node, label, property);
    }

    pub fn createEdgePropertyIndex(self: *Self, edge_type: []const u8, property: []const u8) DatabaseError!void {
        try self.createPropertyIndex(.edge, edge_type, property);
    }

    pub fn dropEdgePropertyIndex(self: *Self, edge_type: []const u8, property: []const u8) DatabaseError!void {
        try self.dropPropertyIndex(.edge, edge_type, property);
    }

    // ========================================================================
    // Declared full-text indexes
    // ========================================================================

    /// The declared full-text indexes.
    ///
    /// They live in the same catalog tree as the property indexes, under their own
    /// kind discriminators, so there is nothing extra to create or load for them.
    /// Null means that tree does not exist yet, which is to say nothing has ever
    /// been declared.
    fn ftsCatalog(self: *Self) ?FtsCatalog {
        const tree = if (self.property_index_catalog_tree) |*t| t else return null;
        return FtsCatalog.init(tree);
    }

    /// Whether the catalog holds any full-text declaration at all.
    ///
    /// Asked once when the database opens, and then kept in `fts_declared` so the
    /// write path does not repeat it.
    fn ftsDeclarationsExist(self: *Self) bool {
        var catalog = self.ftsCatalog() orelse return false;
        inline for (.{ FtsEntityKind.node, FtsEntityKind.edge }) |kind| {
            var iter: FtsCatalog.DefinitionIterator = undefined;
            catalog.iterate(kind, &iter) catch return false;
            defer iter.deinit();
            if ((iter.next() catch null) != null) return true;
        }
        return false;
    }

    /// An index confined to one declaration.
    ///
    /// Built per call rather than kept, because it holds nothing worth keeping:
    /// the trees belong to the database and the scope is four bytes derived from
    /// the definition. The one piece of state it does carry, the dictionary's next
    /// token id, restarts at its first value — which is already what happens on
    /// every reopen, and is harmless because that id is a label written into the
    /// posting page header and never used to find anything.
    fn ftsIndexFor(self: *Self, definition: FtsDefinition) FtsIndex {
        return FtsIndex.initScoped(
            self.allocator,
            &self.buffer_pool,
            &self.fts_dict_tree,
            &self.fts_lengths_tree,
            if (self.fts_reverse_tree) |*t| t else null,
            self.config.fts_config,
            fts_catalog_mod.scopePrefix(definition),
        );
    }

    fn ftsDefinitionFor(
        self: *Self,
        kind: FtsEntityKind,
        scope: []const u8,
        property: []const u8,
        create_symbols: bool,
    ) DatabaseError!FtsDefinition {
        const scope_id = if (create_symbols)
            self.symbol_table.intern(scope) catch return DatabaseError.IoError
        else
            self.symbol_table.lookup(scope) catch |err| switch (err) {
                symbols_mod.SymbolError.NotFound => return DatabaseError.MissingIndex,
                else => return DatabaseError.IoError,
            };
        const property_id = if (create_symbols)
            self.symbol_table.intern(property) catch return DatabaseError.IoError
        else
            self.symbol_table.lookup(property) catch |err| switch (err) {
                symbols_mod.SymbolError.NotFound => return DatabaseError.MissingIndex,
                else => return DatabaseError.IoError,
            };
        return .{ .kind = kind, .scope_id = scope_id, .property_id = property_id };
    }

    fn populateFtsDefinition(self: *Self, definition: FtsDefinition) DatabaseError!void {
        var index = self.ftsIndexFor(definition);
        switch (definition.kind) {
            .node => {
                const node_ids = self.label_index.getNodesByLabel(definition.scope_id) catch return DatabaseError.IoError;
                defer self.allocator.free(node_ids);
                for (node_ids) |node_id| {
                    var state = (try self.readBaseNodeState(node_id)) orelse continue;
                    defer state.deinit(self.allocator);
                    const text = ftsTextOf(definition, state.properties) orelse continue;
                    _ = index.indexDocument(node_id, text) catch |err| return mapFtsError(err);
                }
            },
            .edge => {
                var iter = self.edge_store.scanRefs() catch return DatabaseError.IoError;
                defer iter.deinit();
                while (iter.next() catch return DatabaseError.IoError) |edge_ref| {
                    if (edge_ref.edge_type != definition.scope_id) continue;
                    var state = (try self.readBaseEdgeState(edge_ref.id)) orelse continue;
                    defer state.deinit(self.allocator);
                    const text = ftsTextOf(definition, state.properties) orelse continue;
                    _ = index.indexDocument(edge_ref.id, text) catch |err| return mapFtsError(err);
                }
            },
        }
    }

    fn createFtsIndex(
        self: *Self,
        kind: FtsEntityKind,
        scope: []const u8,
        property: []const u8,
    ) DatabaseError!void {
        if (self.read_only) return DatabaseError.ReadOnly;
        if (scope.len == 0 or property.len == 0) return DatabaseError.InvalidArgument;
        if (self.active_writer_txn_id != null) return DatabaseError.TransactionConflict;
        // Removing a document needs the reverse index to say which terms it wrote
        // and where, and that tree exists only when full-text search is enabled.
        // Declaring an index that could not be maintained would leave a stale term
        // behind on the first update, which is worse than refusing here.
        if (self.fts_reverse_tree == null) return DatabaseError.InvalidArgument;
        try self.ensurePropertyIndexTreesForWrite();

        const definition = try self.ftsDefinitionFor(kind, scope, property, true);
        var catalog = self.ftsCatalog() orelse return DatabaseError.IoError;
        if (catalog.has(definition) catch |err| return mapFtsCatalogError(err)) {
            return DatabaseError.AlreadyExists;
        }
        try self.populateFtsDefinition(definition);
        catalog.create(definition) catch |err| return mapFtsCatalogError(err);
        self.fts_declared = true;
        try self.saveTreeRoots();
        if (self.query_cache) |cache| cache.bumpSchemaVersion();
    }

    fn dropFtsIndex(
        self: *Self,
        kind: FtsEntityKind,
        scope: []const u8,
        property: []const u8,
    ) DatabaseError!void {
        if (self.read_only) return DatabaseError.ReadOnly;
        if (scope.len == 0 or property.len == 0) return DatabaseError.InvalidArgument;
        if (self.active_writer_txn_id != null) return DatabaseError.TransactionConflict;
        const definition = try self.ftsDefinitionFor(kind, scope, property, false);
        var catalog = self.ftsCatalog() orelse return DatabaseError.MissingIndex;
        catalog.drop(definition) catch |err| return mapFtsCatalogError(err);
        try self.clearFtsDefinition(definition);
        self.fts_declared = self.ftsDeclarationsExist();
        try self.saveTreeRoots();
        if (self.query_cache) |cache| cache.bumpSchemaVersion();
    }

    /// Remove everything a dropped index put in the shared trees.
    ///
    /// Leaving it costs space forever and, worse, redeclaring the same label and
    /// property would land on the same scope and find the old terms still sitting
    /// there.
    fn clearFtsDefinition(self: *Self, definition: FtsDefinition) DatabaseError!void {
        var index = self.ftsIndexFor(definition);
        index.clearScope() catch |err| return mapFtsError(err);
    }

    /// Rebuild every declared index from the entities as they now stand.
    ///
    /// Recovery replays into the stores directly rather than through the write
    /// path, so nothing maintained the indexes while it did. Clearing by scope
    /// rather than walking documents matters here: an entity the redo deleted is
    /// no longer there to walk, and its terms would otherwise stay behind and keep
    /// matching.
    fn rebuildFtsIndexes(self: *Self) DatabaseError!void {
        var catalog = self.ftsCatalog() orelse return;
        inline for (.{ FtsEntityKind.node, FtsEntityKind.edge }) |kind| {
            var iter: FtsCatalog.DefinitionIterator = undefined;
            catalog.iterate(kind, &iter) catch |err| return mapFtsCatalogError(err);
            defer iter.deinit();
            while (iter.next() catch |err| return mapFtsCatalogError(err)) |definition| {
                try self.clearFtsDefinition(definition);
                try self.populateFtsDefinition(definition);
            }
        }
    }

    fn hasFtsIndex(
        self: *Self,
        kind: FtsEntityKind,
        scope: []const u8,
        property: []const u8,
    ) DatabaseError!bool {
        const definition = self.ftsDefinitionFor(kind, scope, property, false) catch |err| switch (err) {
            DatabaseError.MissingIndex => return false,
            else => return err,
        };
        var catalog = self.ftsCatalog() orelse return false;
        return catalog.has(definition) catch |err| return mapFtsCatalogError(err);
    }

    /// Search one declared index.
    ///
    /// Results come back scored against that index's own corpus, so a term that is
    /// common in titles and rare in bodies is judged separately in each rather
    /// than averaged across both.
    ///
    /// `MissingIndex` when nothing was declared for this label and property. That
    /// is deliberately not an empty result: no index and no matches are different
    /// situations, and returning nothing for both would leave a typo in a property
    /// name looking like a query that simply found nothing.
    pub fn ftsSearchIndex(
        self: *Self,
        kind: FtsEntityKind,
        scope: []const u8,
        property: []const u8,
        query_text: []const u8,
        limit: u32,
    ) DatabaseError![]FtsSearchResult {
        const definition = try self.ftsDefinitionFor(kind, scope, property, false);
        var catalog = self.ftsCatalog() orelse return DatabaseError.MissingIndex;
        if (!(catalog.has(definition) catch |err| return mapFtsCatalogError(err))) {
            return DatabaseError.MissingIndex;
        }
        var index = self.ftsIndexFor(definition);
        return index.search(query_text, limit) catch |err| return mapFtsError(err);
    }

    /// Search one declared index, including what an open transaction has written
    /// but not yet committed.
    ///
    /// A node whose text changed in this transaction is scored from the pending
    /// text rather than the indexed text, and a node whose text stopped matching
    /// drops out. Without that, a query inside a transaction would answer from
    /// the state before its own writes, which is the one place a reader is
    /// entitled to see them.
    ///
    /// The pending text is scored by term presence rather than BM25, matching what
    /// the unscoped search already does for uncommitted documents. Those scores
    /// are not comparable with the index's, so this is about which rows come back
    /// rather than exactly how they rank among themselves.
    pub fn ftsSearchIndexInTxn(
        self: *Self,
        txn: ?*Transaction,
        kind: FtsEntityKind,
        scope: []const u8,
        property: []const u8,
        query_text: []const u8,
        limit: u32,
    ) DatabaseError![]FtsSearchResult {
        const definition = try self.ftsDefinitionFor(kind, scope, property, false);
        var catalog = self.ftsCatalog() orelse return DatabaseError.MissingIndex;
        if (!(catalog.has(definition) catch |err| return mapFtsCatalogError(err))) {
            return DatabaseError.MissingIndex;
        }
        return self.ftsSearchDefinitionInTxn(txn, definition, query_text, limit);
    }

    /// Search one declared index, tolerating typos in the query.
    ///
    /// The transactional form used to take `max_distance` and `min_term_length`,
    /// discard both, and run an exact search. A query for `quck` found nothing
    /// inside a transaction and the intended document outside one, with the
    /// arguments that were supposed to control that sitting unused. Both forms go
    /// through the same fuzzy matcher now.
    ///
    /// Uncommitted text is matched by term presence rather than edit distance, so
    /// a typo will not find a document this transaction has only just written.
    /// That is worth saying out loud rather than hiding: fuzzy matching against
    /// pending text needs the tokenizer to run over it, which is what committing
    /// does.
    pub fn ftsSearchIndexFuzzy(
        self: *Self,
        txn: ?*Transaction,
        kind: FtsEntityKind,
        scope: []const u8,
        property: []const u8,
        query_text: []const u8,
        limit: u32,
        max_distance: u32,
        min_term_length: u32,
    ) DatabaseError![]FtsSearchResult {
        const definition = try self.ftsDefinitionFor(kind, scope, property, false);
        var catalog = self.ftsCatalog() orelse return DatabaseError.MissingIndex;
        if (!(catalog.has(definition) catch |err| return mapFtsCatalogError(err))) {
            return DatabaseError.MissingIndex;
        }

        var index = self.ftsIndexFor(definition);
        const config = fuzzy_mod.FuzzyConfig{
            .max_distance = max_distance,
            .min_term_length = min_term_length,
        };
        const base = index.searchFuzzy(query_text, config, limit) catch |err| return mapFtsError(err);

        const overlay = blk: {
            if (definition.kind != .node) break :blk null;
            const t = txn orelse break :blk null;
            const found = self.getTxnOverlay(t) orelse break :blk null;
            if (found.node_states.count() == 0) break :blk null;
            break :blk found;
        };
        const pending = overlay orelse return base;
        defer self.freeFtsSearchResults(base);

        return self.mergePendingFts(definition, pending, base, query_text, limit);
    }

    /// The search itself, once the definition is known to exist.
    ///
    /// Callers that already hold a definition use this rather than turning it back
    /// into label and property strings for the other entry point to look up again.
    fn ftsSearchDefinitionInTxn(
        self: *Self,
        txn: ?*Transaction,
        definition: FtsDefinition,
        query_text: []const u8,
        limit: u32,
    ) DatabaseError![]FtsSearchResult {
        const overlay = blk: {
            const t = txn orelse break :blk null;
            const found = self.getTxnOverlay(t) orelse break :blk null;
            const pending = switch (definition.kind) {
                .node => found.node_states.count(),
                .edge => found.edge_states.count(),
            };
            if (pending == 0) break :blk null;
            break :blk found;
        };

        var index = self.ftsIndexFor(definition);
        if (overlay == null) {
            return index.search(query_text, limit) catch |err| return mapFtsError(err);
        }
        const pending = overlay.?;

        const extra: u32 = @intCast(switch (definition.kind) {
            .node => pending.node_states.count(),
            .edge => pending.edge_states.count(),
        });
        // Saturating: a caller wanting every match passes the largest u32 there
        // is, and asking for a few more than that wraps to nearly none.
        const base_results = index.search(query_text, limit +| extra) catch |err| return mapFtsError(err);
        defer self.freeFtsSearchResults(base_results);

        return self.mergePendingFts(definition, pending, base_results, query_text, limit);
    }

    /// Reconcile indexed results with what an open transaction has pending.
    ///
    /// A node the transaction has written is scored from its pending text rather
    /// than the indexed text, and drops out if that text no longer matches. Nodes
    /// the transaction wrote that the index has never seen are scored and added.
    /// `base` stays the caller's to free.
    fn mergePendingFts(
        self: *Self,
        definition: FtsDefinition,
        pending: *TxnOverlay,
        base: []const FtsSearchResult,
        query_text: []const u8,
        limit: u32,
    ) DatabaseError![]FtsSearchResult {
        var merged: std.ArrayListUnmanaged(FtsSearchResult) = .empty;
        errdefer merged.deinit(self.allocator);

        var seen: std.AutoHashMapUnmanaged(NodeId, void) = .{};
        defer seen.deinit(self.allocator);

        for (base) |result| {
            try seen.put(self.allocator, result.doc_id, {});
            if (try self.pendingFtsFor(definition, pending, result.doc_id, query_text)) |staged| {
                if (staged.score) |score| {
                    merged.append(self.allocator, .{ .doc_id = result.doc_id, .score = score }) catch {
                        return DatabaseError.OutOfMemory;
                    };
                }
                continue;
            }
            merged.append(self.allocator, result) catch return DatabaseError.OutOfMemory;
        }

        switch (definition.kind) {
            .node => {
                var overlay_iter = pending.node_states.iterator();
                while (overlay_iter.next()) |entry| {
                    if (seen.contains(entry.key_ptr.*)) continue;
                    if (try self.pendingNodeFtsScore(definition, entry.value_ptr.*, query_text)) |score| {
                        merged.append(self.allocator, .{ .doc_id = entry.key_ptr.*, .score = score }) catch {
                            return DatabaseError.OutOfMemory;
                        };
                    }
                }
            },
            .edge => {
                var overlay_iter = pending.edge_states.iterator();
                while (overlay_iter.next()) |entry| {
                    if (seen.contains(entry.key_ptr.*)) continue;
                    if (try self.pendingEdgeFtsScore(definition, entry.value_ptr.*, query_text)) |score| {
                        merged.append(self.allocator, .{ .doc_id = entry.key_ptr.*, .score = score }) catch {
                            return DatabaseError.OutOfMemory;
                        };
                    }
                }
            },
        }

        std.mem.sort(FtsSearchResult, merged.items, {}, FtsSearchResult.lessThan);
        if (merged.items.len > limit) merged.items.len = limit;
        return merged.toOwnedSlice(self.allocator) catch return DatabaseError.OutOfMemory;
    }

    /// Whether an entity has pending state for this index, and what it scores.
    ///
    /// The outer optional says "this transaction has written this entity", which
    /// is what decides whether the indexed result is replaced. The inner one says
    /// whether the pending state matches, so a document whose text stopped
    /// matching drops out rather than keeping its old score.
    fn pendingFtsFor(
        self: *Self,
        definition: FtsDefinition,
        pending: *TxnOverlay,
        doc_id: NodeId,
        query_text: []const u8,
    ) DatabaseError!?struct { score: ?f32 } {
        switch (definition.kind) {
            .node => {
                const state = pending.node_states.get(doc_id) orelse return null;
                return .{ .score = try self.pendingNodeFtsScore(definition, state, query_text) };
            },
            .edge => {
                const state = pending.edge_states.get(doc_id) orelse return null;
                return .{ .score = try self.pendingEdgeFtsScore(definition, state, query_text) };
            },
        }
    }

    /// What an uncommitted edge state would score, or null when it would not be
    /// in this index at all.
    fn pendingEdgeFtsScore(
        self: *Self,
        definition: FtsDefinition,
        state: TxnEdgeState,
        query_text: []const u8,
    ) DatabaseError!?f32 {
        if (!state.exists) return null;
        if (state.edge_type != definition.scope_id) return null;
        const text = ftsTextOf(definition, state.properties) orelse return null;
        const score = try self.overlayFtsScore(query_text, text);
        return if (score > 0) score else null;
    }

    /// What an uncommitted node state would score for a declared index, or null
    /// when it would not be in that index at all.
    fn pendingNodeFtsScore(
        self: *Self,
        definition: FtsDefinition,
        state: TxnNodeState,
        query_text: []const u8,
    ) DatabaseError!?f32 {
        if (!state.exists) return null;
        if (!containsSymbolId(state.labels, definition.scope_id)) return null;
        const text = ftsTextOf(definition, state.properties) orelse return null;
        const score = try self.overlayFtsScore(query_text, text);
        return if (score > 0) score else null;
    }

    /// Whether one node matches a query through the index declared for a property.
    ///
    /// Used where `@@` sits inside a larger condition and the planner could not
    /// turn the whole WHERE into an index scan. The label comes from the node
    /// itself rather than from the pattern, because by this point there is a real
    /// node in hand and its own labels are the more specific answer.
    ///
    /// Null means no index is declared for this property on any label the node
    /// carries, which the caller reports rather than reading as "did not match".
    pub fn ftsNodeMatches(
        self: *Self,
        txn: ?*Transaction,
        node_id: NodeId,
        property: []const u8,
        query_text: []const u8,
        limit: u32,
    ) DatabaseError!?bool {
        const property_id = self.symbol_table.lookup(property) catch return null;

        var node = (try self.getNodeInTxn(txn, node_id)) orelse return null;
        defer node.deinit(self.allocator);

        return self.ftsScopedMatch(txn, .node, node.labels, node_id, property_id, query_text, limit);
    }

    /// The same for an edge, whose scope is its one type rather than a set of
    /// labels.
    pub fn ftsEdgeMatches(
        self: *Self,
        txn: ?*Transaction,
        edge_id: EdgeId,
        property: []const u8,
        query_text: []const u8,
        limit: u32,
    ) DatabaseError!?bool {
        const property_id = self.symbol_table.lookup(property) catch return null;

        var edge = (try self.getEdgeInTxn(txn, edge_id)) orelse return null;
        defer edge.deinit(self.allocator);

        const scopes = [_]symbols_mod.SymbolId{edge.edge_type};
        return self.ftsScopedMatch(txn, .edge, &scopes, edge_id, property_id, query_text, limit);
    }

    /// Whether one entity matches through whichever of its scopes declares an
    /// index on the property.
    fn ftsScopedMatch(
        self: *Self,
        txn: ?*Transaction,
        kind: FtsEntityKind,
        scopes: []const symbols_mod.SymbolId,
        doc_id: NodeId,
        property_id: symbols_mod.SymbolId,
        query_text: []const u8,
        limit: u32,
    ) DatabaseError!?bool {
        var catalog = self.ftsCatalog() orelse return null;
        for (scopes) |scope_id| {
            const definition = FtsDefinition{
                .kind = kind,
                .scope_id = scope_id,
                .property_id = property_id,
            };
            if (!(catalog.has(definition) catch |err| return mapFtsCatalogError(err))) continue;

            const results = try self.ftsSearchDefinitionInTxn(txn, definition, query_text, limit);
            defer self.freeFtsSearchResults(results);
            for (results) |hit| {
                if (hit.doc_id == doc_id) return true;
            }
            return false;
        }
        return null;
    }

    pub fn createNodeFtsIndex(self: *Self, label: []const u8, property: []const u8) DatabaseError!void {
        try self.createFtsIndex(.node, label, property);
    }

    pub fn dropNodeFtsIndex(self: *Self, label: []const u8, property: []const u8) DatabaseError!void {
        try self.dropFtsIndex(.node, label, property);
    }

    pub fn hasNodeFtsIndex(self: *Self, label: []const u8, property: []const u8) DatabaseError!bool {
        return self.hasFtsIndex(.node, label, property);
    }

    pub fn createEdgeFtsIndex(self: *Self, edge_type: []const u8, property: []const u8) DatabaseError!void {
        try self.createFtsIndex(.edge, edge_type, property);
    }

    pub fn dropEdgeFtsIndex(self: *Self, edge_type: []const u8, property: []const u8) DatabaseError!void {
        try self.dropFtsIndex(.edge, edge_type, property);
    }

    pub fn hasEdgeFtsIndex(self: *Self, edge_type: []const u8, property: []const u8) DatabaseError!bool {
        return self.hasFtsIndex(.edge, edge_type, property);
    }

    /// Bring the declared full-text indexes in line with a change to one node.
    ///
    /// Empty old state means a create and empty new state means a delete, so every
    /// write path calls this one function rather than choosing between three.
    fn ftsReplaceNode(
        self: *Self,
        node_id: NodeId,
        old_labels: []const symbols_mod.SymbolId,
        old_properties: []const node_mod.Property,
        new_labels: []const symbols_mod.SymbolId,
        new_properties: []const node_mod.Property,
    ) DatabaseError!void {
        if (!self.fts_declared) return;
        var catalog = self.ftsCatalog() orelse return;
        var iter: FtsCatalog.DefinitionIterator = undefined;
        catalog.iterate(.node, &iter) catch |err| return mapFtsCatalogError(err);
        defer iter.deinit();
        while (iter.next() catch |err| return mapFtsCatalogError(err)) |definition| {
            const was = if (containsSymbolId(old_labels, definition.scope_id))
                ftsTextOf(definition, old_properties)
            else
                null;
            const now = if (containsSymbolId(new_labels, definition.scope_id))
                ftsTextOf(definition, new_properties)
            else
                null;
            try self.ftsReplaceDocument(definition, node_id, was, now);
        }
    }

    /// The same for an edge, which is scoped by its type rather than its labels.
    fn ftsReplaceEdge(
        self: *Self,
        edge_id: EdgeId,
        old_type: symbols_mod.SymbolId,
        old_properties: []const node_mod.Property,
        new_type: symbols_mod.SymbolId,
        new_properties: []const node_mod.Property,
        had_old: bool,
        has_new: bool,
    ) DatabaseError!void {
        if (!self.fts_declared) return;
        var catalog = self.ftsCatalog() orelse return;
        var iter: FtsCatalog.DefinitionIterator = undefined;
        catalog.iterate(.edge, &iter) catch |err| return mapFtsCatalogError(err);
        defer iter.deinit();
        while (iter.next() catch |err| return mapFtsCatalogError(err)) |definition| {
            const was = if (had_old and old_type == definition.scope_id)
                ftsTextOf(definition, old_properties)
            else
                null;
            const now = if (has_new and new_type == definition.scope_id)
                ftsTextOf(definition, new_properties)
            else
                null;
            try self.ftsReplaceDocument(definition, edge_id, was, now);
        }
    }

    fn ftsReplaceDocument(
        self: *Self,
        definition: FtsDefinition,
        doc_id: NodeId,
        was: ?[]const u8,
        now: ?[]const u8,
    ) DatabaseError!void {
        if (was == null and now == null) return;
        // Text that did not change is already indexed correctly, and reindexing it
        // would make every unrelated property write pay for the longest indexed
        // string on the entity.
        if (was != null and now != null and std.mem.eql(u8, was.?, now.?)) return;

        var index = self.ftsIndexFor(definition);
        if (was != null) {
            index.removeDocument(doc_id) catch |err| switch (err) {
                FtsError.NotFound => {},
                else => return mapFtsError(err),
            };
        }
        if (now) |text| {
            _ = index.indexDocument(doc_id, text) catch |err| return mapFtsError(err);
        }
    }

    fn nodeStateMatchesPropertyIndex(
        state: TxnNodeState,
        definition: PropertyIndexDefinition,
        value: PropertyValue,
    ) bool {
        if (!state.exists or !containsSymbolId(state.labels, definition.scope_id)) return false;
        const property = findPropertyById(state.properties, definition.property_id) orelse return false;
        return propertyValueEqual(property.value, value);
    }

    fn edgeStateMatchesPropertyIndex(
        state: TxnEdgeState,
        definition: PropertyIndexDefinition,
        value: PropertyValue,
    ) bool {
        if (!state.exists or state.edge_type != definition.scope_id) return false;
        const property = findPropertyById(state.properties, definition.property_id) orelse return false;
        return propertyValueEqual(property.value, value);
    }

    pub fn findNodesByLabelProperty(
        self: *Self,
        txn: ?*Transaction,
        label: []const u8,
        property: []const u8,
        value: PropertyValue,
        limit: usize,
    ) DatabaseError![]NodeId {
        if (limit == 0) return self.allocator.alloc(NodeId, 0) catch return DatabaseError.OutOfMemory;
        const definition = try self.propertyIndexDefinition(.node, label, property, false);
        var index = &(self.property_index orelse return DatabaseError.MissingIndex);
        const candidates = index.lookup(definition, value, std.math.maxInt(usize)) catch |err| return mapPropertyIndexError(err);
        defer self.allocator.free(candidates);

        var ids: std.ArrayList(NodeId) = .empty;
        errdefer ids.deinit(self.allocator);
        var seen: std.AutoHashMapUnmanaged(NodeId, void) = .{};
        defer seen.deinit(self.allocator);

        for (candidates) |node_id| {
            var state = (try self.readVisibleNodeState(txn, node_id)) orelse continue;
            defer state.deinit(self.allocator);
            if (!nodeStateMatchesPropertyIndex(state, definition, value)) continue;
            try seen.put(self.allocator, node_id, {});
            ids.append(self.allocator, node_id) catch return DatabaseError.OutOfMemory;
        }
        if (txn) |active_txn| {
            if (self.getTxnOverlay(active_txn)) |overlay| {
                var iter = overlay.node_states.iterator();
                while (iter.next()) |entry| {
                    if (seen.contains(entry.key_ptr.*) or !nodeStateMatchesPropertyIndex(entry.value_ptr.*, definition, value)) continue;
                    try seen.put(self.allocator, entry.key_ptr.*, {});
                    ids.append(self.allocator, entry.key_ptr.*) catch return DatabaseError.OutOfMemory;
                }
            }
        }
        std.mem.sort(NodeId, ids.items, {}, std.sort.asc(NodeId));
        if (ids.items.len > limit) ids.shrinkRetainingCapacity(limit);
        return ids.toOwnedSlice(self.allocator) catch return DatabaseError.OutOfMemory;
    }

    pub fn findEdgesByTypeProperty(
        self: *Self,
        txn: ?*Transaction,
        edge_type: []const u8,
        property: []const u8,
        value: PropertyValue,
        limit: usize,
    ) DatabaseError![]EdgeId {
        if (limit == 0) return self.allocator.alloc(EdgeId, 0) catch return DatabaseError.OutOfMemory;
        const definition = try self.propertyIndexDefinition(.edge, edge_type, property, false);
        var index = &(self.property_index orelse return DatabaseError.MissingIndex);
        const candidates = index.lookup(definition, value, std.math.maxInt(usize)) catch |err| return mapPropertyIndexError(err);
        defer self.allocator.free(candidates);

        var ids: std.ArrayList(EdgeId) = .empty;
        errdefer ids.deinit(self.allocator);
        var seen: std.AutoHashMapUnmanaged(EdgeId, void) = .{};
        defer seen.deinit(self.allocator);

        for (candidates) |edge_id| {
            var state = (try self.readVisibleEdgeState(txn, edge_id)) orelse continue;
            defer state.deinit(self.allocator);
            if (!edgeStateMatchesPropertyIndex(state, definition, value)) continue;
            try seen.put(self.allocator, edge_id, {});
            ids.append(self.allocator, edge_id) catch return DatabaseError.OutOfMemory;
        }
        if (txn) |active_txn| {
            if (self.getTxnOverlay(active_txn)) |overlay| {
                var iter = overlay.edge_states.iterator();
                while (iter.next()) |entry| {
                    if (seen.contains(entry.key_ptr.*) or !edgeStateMatchesPropertyIndex(entry.value_ptr.*, definition, value)) continue;
                    try seen.put(self.allocator, entry.key_ptr.*, {});
                    ids.append(self.allocator, entry.key_ptr.*) catch return DatabaseError.OutOfMemory;
                }
            }
        }
        std.mem.sort(EdgeId, ids.items, {}, std.sort.asc(EdgeId));
        if (ids.items.len > limit) ids.shrinkRetainingCapacity(limit);
        return ids.toOwnedSlice(self.allocator) catch return DatabaseError.OutOfMemory;
    }

    /// Rebuild the HNSW index from persisted vectors
    fn rebuildHnswIndex(self: *Self, vs: *VectorStorage, hnsw: *HnswIndex) DatabaseError!void {
        const entries = vs.getAllEntries(self.allocator) catch |err| return mapVectorStorageError(err);
        defer if (entries.len > 0) self.allocator.free(entries);

        // Legacy databases may contain append-only replacement records. Later
        // records are authoritative, and superseded slots can be reclaimed.
        var latest = std.AutoHashMap(u64, VectorLocation).init(self.allocator);
        defer latest.deinit();
        for (entries) |entry| {
            if (latest.get(entry.id)) |old_location| {
                if (!self.read_only) {
                    vs.delete(old_location, entry.id) catch |err| return mapVectorStorageError(err);
                }
            }
            latest.put(entry.id, entry.location) catch return DatabaseError.OutOfMemory;
        }

        var iter = latest.iterator();
        while (iter.next()) |entry| {
            const vector_id = entry.key_ptr.*;
            const location = entry.value_ptr.*;
            if (!(try self.nodeExists(vector_id))) {
                if (!self.read_only) {
                    vs.delete(location, vector_id) catch |err| return mapVectorStorageError(err);
                }
                continue;
            }

            const vector = vs.getByLocation(location) catch |err| return mapVectorStorageError(err);
            defer vs.free(vector);
            hnsw.insertStored(vector_id, location, vector) catch |err| return mapHnswError(err);
        }
    }

    // ========================================================================
    // Statistics
    // ========================================================================

    /// Get the number of nodes in the database
    /// Whether this database file was built with a vector index.
    ///
    /// This reads the file header rather than the handle's config, because a
    /// handle opened without `enable_vector` still describes a file that has
    /// one. Reporting the config would say "disabled" for every database that
    /// was not explicitly opened for vector use.
    pub fn hasPersistedVectorIndex(self: *const Self) bool {
        return self.page_manager.getHeader().vector_segment_page != NULL_PAGE;
    }

    /// Whether this database file was built with full-text search.
    ///
    /// The dictionary and length trees are created for every database, so they
    /// cannot tell us anything. The reverse tree is the one built only when
    /// full-text search is enabled, which makes its root the honest marker.
    pub fn hasPersistedFtsIndex(self: *const Self) bool {
        return self.page_manager.getHeader().getTreeRoot(.fts_reverse) != NULL_PAGE;
    }

    pub fn nodeCount(self: *Self) u64 {
        var iter = self.node_tree.range(null, null) catch return 0;
        defer iter.deinit();

        var count: u64 = 0;
        while (true) {
            const entry = iter.next() catch break;
            if (entry == null) break;
            count += 1;
        }

        return count;
    }

    /// Get the number of edges in the database
    pub fn edgeCount(self: *Self) u64 {
        var iter = self.edge_tree.range(null, null) catch return 0;
        defer iter.deinit();

        var count: u64 = 0;
        while (true) {
            const entry = iter.next() catch break;
            if (entry) |e| {
                if (e.key.len >= 11 and e.key[8] == 0) {
                    count += 1;
                }
            } else {
                break;
            }
        }

        return count;
    }

    /// Get database file path
    pub fn getPath(self: *const Self) []const u8 {
        return self.path;
    }

    /// Check if database is read-only
    pub fn isReadOnly(self: *const Self) bool {
        return self.read_only;
    }

    // ========================================================================
    // Graph Operations - Nodes
    // ========================================================================

    /// Translate node-store errors into the user-facing DatabaseError set.
    /// Reserved for call sites that want the `BufferTooSmall → ValueTooLarge`
    /// translation; internal recovery paths may still use `catch {}`.
    fn mapNodeStoreError(err: node_mod.NodeError) DatabaseError {
        return switch (err) {
            node_mod.NodeError.NotFound => DatabaseError.NotFound,
            node_mod.NodeError.AlreadyExists => DatabaseError.AlreadyExists,
            node_mod.NodeError.OutOfMemory => DatabaseError.OutOfMemory,
            node_mod.NodeError.BufferPoolFull => DatabaseError.BufferPoolFull,
            node_mod.NodeError.BufferTooSmall => DatabaseError.ValueTooLarge,
            else => DatabaseError.IoError,
        };
    }

    fn mapEdgeStoreError(err: edge_mod.EdgeError) DatabaseError {
        return switch (err) {
            edge_mod.EdgeError.NotFound => DatabaseError.NotFound,
            edge_mod.EdgeError.AlreadyExists => DatabaseError.AlreadyExists,
            edge_mod.EdgeError.OutOfMemory => DatabaseError.OutOfMemory,
            edge_mod.EdgeError.BufferTooSmall => DatabaseError.ValueTooLarge,
            else => DatabaseError.IoError,
        };
    }

    fn mapStreamError(self: *Self, err: stream_store_mod.StreamError) DatabaseError {
        _ = self;
        return switch (err) {
            stream_store_mod.StreamError.InvalidName,
            stream_store_mod.StreamError.ReservedName,
            stream_store_mod.StreamError.InvalidKind,
            stream_store_mod.StreamError.InvalidConsumer,
            stream_store_mod.StreamError.InvalidRecord,
            => DatabaseError.InvalidArgument,
            stream_store_mod.StreamError.OutOfMemory => DatabaseError.OutOfMemory,
            stream_store_mod.StreamError.ValueTooLarge => DatabaseError.ValueTooLarge,
            stream_store_mod.StreamError.IoError => DatabaseError.IoError,
        };
    }

    fn mapVectorStorageError(err: VectorStorageError) DatabaseError {
        return switch (err) {
            VectorStorageError.DimensionMismatch,
            VectorStorageError.InvalidDimensions,
            VectorStorageError.VectorTooLarge,
            => DatabaseError.InvalidArgument,
            VectorStorageError.OutOfMemory => DatabaseError.OutOfMemory,
            VectorStorageError.StorageFull => DatabaseError.BufferPoolFull,
            VectorStorageError.Corruption => DatabaseError.InvalidDatabase,
            else => DatabaseError.IoError,
        };
    }

    fn mapHnswError(err: HnswError) DatabaseError {
        return switch (err) {
            HnswError.DimensionMismatch => DatabaseError.InvalidArgument,
            HnswError.OutOfMemory => DatabaseError.OutOfMemory,
            else => DatabaseError.IoError,
        };
    }

    fn validateVectorValue(self: *Self, vector: []const f32) DatabaseError!void {
        if (!types.isValidVectorDimensions(vector.len)) return DatabaseError.InvalidArgument;
        const vs = self.vector_storage orelse return DatabaseError.IoError;
        if (vector.len != vs.dimensions) return DatabaseError.InvalidArgument;
    }

    fn stageStreamAppend(
        self: *Self,
        overlay: *TxnOverlay,
        stream: []const u8,
        kind: []const u8,
        payload: PropertyValue,
        user_publish: bool,
    ) DatabaseError!u64 {
        if (user_publish) {
            stream_store_mod.validateUserStreamName(stream) catch |err| return self.mapStreamError(err);
        } else {
            stream_store_mod.validateStreamNameForRead(stream) catch |err| return self.mapStreamError(err);
        }
        stream_store_mod.validateKind(kind) catch |err| return self.mapStreamError(err);

        const sequence = try self.nextStreamSequence(overlay.stream_appends.items, stream);
        const stream_copy = self.allocator.dupe(u8, stream) catch return DatabaseError.OutOfMemory;
        errdefer self.allocator.free(stream_copy);
        const kind_copy = self.allocator.dupe(u8, kind) catch return DatabaseError.OutOfMemory;
        errdefer self.allocator.free(kind_copy);
        const payload_copy = payload.clone(self.allocator) catch return DatabaseError.OutOfMemory;
        errdefer {
            var owned = payload_copy;
            owned.deinit(self.allocator);
        }

        overlay.stream_appends.append(self.allocator, .{
            .stream = stream_copy,
            .kind = kind_copy,
            .payload = payload_copy,
            .sequence = sequence,
        }) catch return DatabaseError.OutOfMemory;
        overlay.markDirty();
        return sequence;
    }

    fn nextStreamSequence(self: *Self, prior_appends: []const TxnStreamAppend, stream: []const u8) DatabaseError!u64 {
        const trees = try self.streamTreesForWrite();

        var last = stream_store_mod.getLastSequence(
            trees,
            self.allocator,
            stream,
        ) catch |err| return self.mapStreamError(err);

        for (prior_appends) |previous| {
            if (previous.sequence) |seq| {
                if (std.mem.eql(u8, previous.stream, stream) and seq > last) {
                    last = seq;
                }
            }
        }

        if (last == std.math.maxInt(u64)) return DatabaseError.ValueTooLarge;
        return last + 1;
    }

    fn assignStreamSequences(self: *Self, overlay: *TxnOverlay) DatabaseError!void {
        for (overlay.stream_appends.items, 0..) |*append, i| {
            if (append.sequence != null) continue;

            append.sequence = try self.nextStreamSequence(overlay.stream_appends.items[0..i], append.stream);
        }
    }

    pub fn publishStreamGetSequence(
        self: *Self,
        txn: *Transaction,
        stream: []const u8,
        kind: ?[]const u8,
        payload: PropertyValue,
    ) DatabaseError!u64 {
        if (self.read_only) return DatabaseError.PermissionDenied;
        if (!txn.isActive()) return DatabaseError.TransactionNotActive;
        if (txn.mode == .read_only) return DatabaseError.TransactionReadOnly;
        const overlay = self.getTxnOverlay(txn) orelse return DatabaseError.TransactionNotActive;
        try self.ensureStreamTreesForWrite();
        return self.stageStreamAppend(
            overlay,
            stream,
            kind orelse stream_store_mod.default_kind,
            payload,
            true,
        );
    }

    pub fn publishStream(
        self: *Self,
        txn: *Transaction,
        stream: []const u8,
        kind: ?[]const u8,
        payload: PropertyValue,
    ) DatabaseError!void {
        _ = try self.publishStreamGetSequence(txn, stream, kind, payload);
    }

    pub fn setStreamOffset(
        self: *Self,
        txn: *Transaction,
        stream: []const u8,
        consumer: []const u8,
        sequence: u64,
    ) DatabaseError!void {
        if (self.read_only) return DatabaseError.PermissionDenied;
        if (!txn.isActive()) return DatabaseError.TransactionNotActive;
        if (txn.mode == .read_only) return DatabaseError.TransactionReadOnly;
        stream_store_mod.validateStreamNameForRead(stream) catch |err| return self.mapStreamError(err);
        stream_store_mod.validateConsumer(consumer) catch |err| return self.mapStreamError(err);
        const overlay = self.getTxnOverlay(txn) orelse return DatabaseError.TransactionNotActive;
        try self.ensureStreamTreesForWrite();

        const stream_copy = self.allocator.dupe(u8, stream) catch return DatabaseError.OutOfMemory;
        errdefer self.allocator.free(stream_copy);
        const consumer_copy = self.allocator.dupe(u8, consumer) catch return DatabaseError.OutOfMemory;
        errdefer self.allocator.free(consumer_copy);

        overlay.stream_offsets.append(self.allocator, .{
            .stream = stream_copy,
            .consumer = consumer_copy,
            .sequence = sequence,
        }) catch return DatabaseError.OutOfMemory;
        overlay.markDirty();
    }

    pub fn trimStream(
        self: *Self,
        txn: *Transaction,
        stream: []const u8,
        through_sequence: u64,
    ) DatabaseError!void {
        if (self.read_only) return DatabaseError.PermissionDenied;
        if (!txn.isActive()) return DatabaseError.TransactionNotActive;
        if (txn.mode == .read_only) return DatabaseError.TransactionReadOnly;
        stream_store_mod.validateStreamNameForRead(stream) catch |err| return self.mapStreamError(err);
        const overlay = self.getTxnOverlay(txn) orelse return DatabaseError.TransactionNotActive;
        try self.ensureStreamTreesForWrite();

        const stream_copy = self.allocator.dupe(u8, stream) catch return DatabaseError.OutOfMemory;
        errdefer self.allocator.free(stream_copy);
        overlay.stream_trims.append(self.allocator, .{
            .stream = stream_copy,
            .through_sequence = through_sequence,
        }) catch return DatabaseError.OutOfMemory;
        overlay.markDirty();
    }

    pub fn getStreamOffset(
        self: *Self,
        stream: []const u8,
        consumer: []const u8,
    ) DatabaseError!?u64 {
        return stream_store_mod.getOffset(
            self.streamTrees(),
            self.allocator,
            stream,
            consumer,
        ) catch |err| return self.mapStreamError(err);
    }

    pub fn getStreamLastSequence(
        self: *Self,
        stream: []const u8,
    ) DatabaseError!u64 {
        const trees = self.streamTrees() orelse {
            stream_store_mod.validateStreamNameForRead(stream) catch |err| return self.mapStreamError(err);
            return 0;
        };
        return stream_store_mod.getLastSequence(
            trees,
            self.allocator,
            stream,
        ) catch |err| return self.mapStreamError(err);
    }

    fn readStreamOnce(
        self: *Self,
        stream: []const u8,
        after_sequence: u64,
        limit: usize,
    ) DatabaseError!StreamBatch {
        return stream_store_mod.read(
            self.streamTrees(),
            self.allocator,
            stream,
            after_sequence,
            limit,
        ) catch |err| return self.mapStreamError(err);
    }

    pub fn readStream(
        self: *Self,
        stream: []const u8,
        after_sequence: u64,
        limit: usize,
        timeout_ms: u64,
    ) DatabaseError!StreamBatch {
        if (limit == 0) return DatabaseError.InvalidArgument;

        var batch = try self.readStreamOnce(stream, after_sequence, limit);
        if (batch.records.len > 0 or timeout_ms == 0) return batch;
        batch.deinit();

        const timeout_ns: i128 = @as(i128, @intCast(timeout_ms)) * std.time.ns_per_ms;
        const start_ns = @import("compat").nanoTimestamp();

        self.stream_mutex.lock();
        var observed_epoch = self.stream_epoch;
        self.stream_mutex.unlock();

        while (true) {
            const elapsed = @import("compat").nanoTimestamp() - start_ns;
            if (elapsed >= timeout_ns) {
                return try self.readStreamOnce(stream, after_sequence, limit);
            }
            const remaining_ns: u64 = @intCast(timeout_ns - elapsed);

            self.stream_mutex.lock();
            if (self.stream_epoch == observed_epoch) {
                self.stream_cond.timedWait(&self.stream_mutex, remaining_ns) catch {};
            }
            observed_epoch = self.stream_epoch;
            self.stream_mutex.unlock();

            batch = try self.readStreamOnce(stream, after_sequence, limit);
            if (batch.records.len > 0) return batch;
            batch.deinit();
        }
    }

    pub fn readChanges(self: *Self, after_sequence: u64, limit: usize, timeout_ms: u64) DatabaseError!StreamBatch {
        return self.readStream(stream_store_mod.changes_stream, after_sequence, limit, timeout_ms);
    }

    fn propertyValueEqual(a: PropertyValue, b: PropertyValue) bool {
        return switch (a) {
            .null_val => b == .null_val,
            .bool_val => |v| b == .bool_val and b.bool_val == v,
            .int_val => |v| b == .int_val and b.int_val == v,
            .float_val => |v| b == .float_val and b.float_val == v,
            .string_val => |v| b == .string_val and std.mem.eql(u8, b.string_val, v),
            .bytes_val => |v| b == .bytes_val and std.mem.eql(u8, b.bytes_val, v),
            .vector_val => |v| b == .vector_val and std.mem.eql(f32, b.vector_val, v),
            .list_val => |list| blk: {
                if (b != .list_val or b.list_val.len != list.len) break :blk false;
                for (list, b.list_val) |lhs, rhs| {
                    if (!propertyValueEqual(lhs, rhs)) break :blk false;
                }
                break :blk true;
            },
            .map_val => |map| blk: {
                if (b != .map_val or b.map_val.len != map.len) break :blk false;
                for (map, b.map_val) |lhs, rhs| {
                    if (!std.mem.eql(u8, lhs.key, rhs.key)) break :blk false;
                    if (!propertyValueEqual(lhs.value, rhs.value)) break :blk false;
                }
                break :blk true;
            },
        };
    }

    fn idValue(id: u64) PropertyValue {
        return .{ .int_val = @intCast(id) };
    }

    fn propertyValueTypeName(value: PropertyValue) []const u8 {
        return switch (value) {
            .null_val => "null",
            .bool_val => "bool",
            .int_val => "int",
            .float_val => "float",
            .string_val => "string",
            .bytes_val => "bytes",
            .vector_val => "vector",
            .list_val => "list",
            .map_val => "map",
        };
    }

    fn intValueFromSize(size: usize) PropertyValue {
        const capped = @min(size, @as(usize, @intCast(std.math.maxInt(i64))));
        return .{ .int_val = @intCast(capped) };
    }

    fn summarizedChangefeedValue(
        value: PropertyValue,
        entries: *[3]PropertyValue.MapEntry,
    ) PropertyValue {
        entries.* = .{
            .{ .key = "__lattice_value_omitted", .value = .{ .bool_val = true } },
            .{ .key = "type", .value = .{ .string_val = propertyValueTypeName(value) } },
            .{ .key = "encoded_bytes", .value = intValueFromSize(wal_payload.propertyValueSize(value)) },
        };
        return .{ .map_val = entries[0..] };
    }

    fn streamAppendPayloadFits(kind: []const u8, payload: PropertyValue) bool {
        const payload_len = stream_payload_mod.encodedSize(payload);
        if (payload_len > std.math.maxInt(u32)) return false;

        const record_prefix = std.math.add(usize, 1 + kind.len, 4) catch return false;
        const record_value_len = std.math.add(usize, record_prefix, payload_len) catch return false;
        if (record_value_len > btree_mod.MAX_VALUE_SIZE) return false;

        const wal_prefix = std.math.add(usize, 1 + 1 + stream_store_mod.changes_stream.len + 8 + 1 + kind.len, 4) catch return false;
        const wal_payload_len = std.math.add(usize, wal_prefix, payload_len) catch return false;
        return wal_payload_len <= wal_mod.MAX_LOGICAL_RECORD_PAYLOAD_SIZE;
    }

    fn changePayloadFits(kind: []const u8, entries: []const PropertyValue.MapEntry) bool {
        return streamAppendPayloadFits(kind, .{ .map_val = entries });
    }

    fn stageChangePayload(
        self: *Self,
        overlay: *TxnOverlay,
        kind: []const u8,
        entries: []const PropertyValue.MapEntry,
    ) DatabaseError!void {
        const payload = PropertyValue{ .map_val = entries };
        _ = try self.stageStreamAppend(
            overlay,
            stream_store_mod.changes_stream,
            kind,
            payload,
            false,
        );
    }

    fn stagePropertyChangePayload(
        self: *Self,
        overlay: *TxnOverlay,
        kind: []const u8,
        entries: *[6]PropertyValue.MapEntry,
        old_value: ?PropertyValue,
        new_value: ?PropertyValue,
    ) DatabaseError!void {
        entries[4].value = old_value orelse .{ .null_val = {} };
        entries[5].value = new_value orelse .{ .null_val = {} };
        if (changePayloadFits(kind, entries[0..])) {
            try self.stageChangePayload(overlay, kind, entries[0..]);
            return;
        }

        var old_summary_entries: [3]PropertyValue.MapEntry = undefined;
        if (old_value) |value| {
            entries[4].value = summarizedChangefeedValue(value, &old_summary_entries);
            if (changePayloadFits(kind, entries[0..])) {
                try self.stageChangePayload(overlay, kind, entries[0..]);
                return;
            }
        }

        var new_summary_entries: [3]PropertyValue.MapEntry = undefined;
        if (new_value) |value| {
            entries[5].value = summarizedChangefeedValue(value, &new_summary_entries);
            if (changePayloadFits(kind, entries[0..])) {
                try self.stageChangePayload(overlay, kind, entries[0..]);
                return;
            }
        }

        return DatabaseError.ValueTooLarge;
    }

    fn stageNodeChange(
        self: *Self,
        overlay: *TxnOverlay,
        kind: []const u8,
        op: []const u8,
        node_id: NodeId,
    ) DatabaseError!void {
        var entries = [_]PropertyValue.MapEntry{
            .{ .key = "entity", .value = .{ .string_val = "node" } },
            .{ .key = "op", .value = .{ .string_val = op } },
            .{ .key = "node_id", .value = idValue(node_id) },
        };
        try self.stageChangePayload(overlay, kind, &entries);
    }

    fn stageNodeLabelChange(
        self: *Self,
        overlay: *TxnOverlay,
        kind: []const u8,
        op: []const u8,
        node_id: NodeId,
        label_id: symbols_mod.SymbolId,
    ) DatabaseError!void {
        const label = self.symbol_table.resolve(label_id) catch return DatabaseError.IoError;
        defer self.allocator.free(label);
        var entries = [_]PropertyValue.MapEntry{
            .{ .key = "entity", .value = .{ .string_val = "node" } },
            .{ .key = "op", .value = .{ .string_val = op } },
            .{ .key = "node_id", .value = idValue(node_id) },
            .{ .key = "label", .value = .{ .string_val = label } },
        };
        try self.stageChangePayload(overlay, kind, &entries);
    }

    fn stageNodePropertyChange(
        self: *Self,
        overlay: *TxnOverlay,
        kind: []const u8,
        op: []const u8,
        node_id: NodeId,
        key_id: symbols_mod.SymbolId,
        old_value: ?PropertyValue,
        new_value: ?PropertyValue,
    ) DatabaseError!void {
        const key = self.symbol_table.resolve(key_id) catch return DatabaseError.IoError;
        defer self.allocator.free(key);
        var entries = [_]PropertyValue.MapEntry{
            .{ .key = "entity", .value = .{ .string_val = "node" } },
            .{ .key = "op", .value = .{ .string_val = op } },
            .{ .key = "node_id", .value = idValue(node_id) },
            .{ .key = "key", .value = .{ .string_val = key } },
            .{ .key = "old_value", .value = .{ .null_val = {} } },
            .{ .key = "new_value", .value = .{ .null_val = {} } },
        };
        try self.stagePropertyChangePayload(overlay, kind, &entries, old_value, new_value);
    }

    fn stageEdgeChange(
        self: *Self,
        overlay: *TxnOverlay,
        kind: []const u8,
        op: []const u8,
        edge_id: EdgeId,
        state: TxnEdgeState,
    ) DatabaseError!void {
        const type_name = self.symbol_table.resolve(state.edge_type) catch return DatabaseError.IoError;
        defer self.allocator.free(type_name);
        var entries = [_]PropertyValue.MapEntry{
            .{ .key = "entity", .value = .{ .string_val = "edge" } },
            .{ .key = "op", .value = .{ .string_val = op } },
            .{ .key = "edge_id", .value = idValue(edge_id) },
            .{ .key = "source_id", .value = idValue(state.source) },
            .{ .key = "target_id", .value = idValue(state.target) },
            .{ .key = "type", .value = .{ .string_val = type_name } },
        };
        try self.stageChangePayload(overlay, kind, &entries);
    }

    fn stageEdgePropertyChange(
        self: *Self,
        overlay: *TxnOverlay,
        kind: []const u8,
        op: []const u8,
        edge_id: EdgeId,
        key_id: symbols_mod.SymbolId,
        old_value: ?PropertyValue,
        new_value: ?PropertyValue,
    ) DatabaseError!void {
        const key = self.symbol_table.resolve(key_id) catch return DatabaseError.IoError;
        defer self.allocator.free(key);
        var entries = [_]PropertyValue.MapEntry{
            .{ .key = "entity", .value = .{ .string_val = "edge" } },
            .{ .key = "op", .value = .{ .string_val = op } },
            .{ .key = "edge_id", .value = idValue(edge_id) },
            .{ .key = "key", .value = .{ .string_val = key } },
            .{ .key = "old_value", .value = .{ .null_val = {} } },
            .{ .key = "new_value", .value = .{ .null_val = {} } },
        };
        try self.stagePropertyChangePayload(overlay, kind, &entries, old_value, new_value);
    }

    fn stageGraphChangefeed(self: *Self, overlay: *TxnOverlay) DatabaseError!void {
        var node_iter = overlay.node_states.iterator();
        while (node_iter.next()) |entry| {
            const node_id = entry.key_ptr.*;
            const state = entry.value_ptr.*;
            const current = try self.readBaseNodeState(node_id);
            defer if (current) |existing| {
                var owned_existing = existing;
                owned_existing.deinit(self.allocator);
            };

            if (!state.exists) {
                if (current != null) {
                    try self.stageNodeChange(overlay, "node.delete", "delete", node_id);
                }
                continue;
            }

            if (current) |existing| {
                for (existing.labels) |label_id| {
                    if (!containsSymbolId(state.labels, label_id)) {
                        try self.stageNodeLabelChange(overlay, "node.label_remove", "label_remove", node_id, label_id);
                    }
                }
                for (state.labels) |label_id| {
                    if (!containsSymbolId(existing.labels, label_id)) {
                        try self.stageNodeLabelChange(overlay, "node.label_add", "label_add", node_id, label_id);
                    }
                }

                for (existing.properties) |prop| {
                    if (findPropertyById(state.properties, prop.key_id) == null) {
                        try self.stageNodePropertyChange(
                            overlay,
                            "node.property_remove",
                            "property_remove",
                            node_id,
                            prop.key_id,
                            prop.value,
                            null,
                        );
                    }
                }
                for (state.properties) |prop| {
                    const old_prop = findPropertyById(existing.properties, prop.key_id);
                    if (old_prop) |old| {
                        if (propertyValueEqual(old.value, prop.value)) continue;
                    }
                    try self.stageNodePropertyChange(
                        overlay,
                        "node.property_set",
                        "property_set",
                        node_id,
                        prop.key_id,
                        if (old_prop) |old| old.value else null,
                        prop.value,
                    );
                }
            } else {
                try self.stageNodeChange(overlay, "node.insert", "insert", node_id);
                for (state.labels) |label_id| {
                    try self.stageNodeLabelChange(overlay, "node.label_add", "label_add", node_id, label_id);
                }
                for (state.properties) |prop| {
                    try self.stageNodePropertyChange(
                        overlay,
                        "node.property_set",
                        "property_set",
                        node_id,
                        prop.key_id,
                        null,
                        prop.value,
                    );
                }
            }
        }

        var edge_iter = overlay.edge_states.iterator();
        while (edge_iter.next()) |entry| {
            const edge_id = entry.key_ptr.*;
            const state = entry.value_ptr.*;
            const current = try self.readBaseEdgeState(edge_id);
            defer if (current) |existing| {
                var owned_existing = existing;
                owned_existing.deinit(self.allocator);
            };

            if (!state.exists) {
                if (current) |existing| {
                    try self.stageEdgeChange(overlay, "edge.delete", "delete", edge_id, existing);
                }
                continue;
            }

            if (current) |existing| {
                for (existing.properties) |prop| {
                    if (findPropertyById(state.properties, prop.key_id) == null) {
                        try self.stageEdgePropertyChange(
                            overlay,
                            "edge.property_remove",
                            "property_remove",
                            edge_id,
                            prop.key_id,
                            prop.value,
                            null,
                        );
                    }
                }
                for (state.properties) |prop| {
                    const old_prop = findPropertyById(existing.properties, prop.key_id);
                    if (old_prop) |old| {
                        if (propertyValueEqual(old.value, prop.value)) continue;
                    }
                    try self.stageEdgePropertyChange(
                        overlay,
                        "edge.property_set",
                        "property_set",
                        edge_id,
                        prop.key_id,
                        if (old_prop) |old| old.value else null,
                        prop.value,
                    );
                }
            } else {
                try self.stageEdgeChange(overlay, "edge.insert", "insert", edge_id, state);
                for (state.properties) |prop| {
                    try self.stageEdgePropertyChange(
                        overlay,
                        "edge.property_set",
                        "property_set",
                        edge_id,
                        prop.key_id,
                        null,
                        prop.value,
                    );
                }
            }
        }
    }

    fn getTxnOverlay(self: *Self, txn: *const Transaction) ?*TxnOverlay {
        return self.txn_overlays.getPtr(txn.id);
    }

    fn readBaseNodeState(self: *Self, node_id: NodeId) DatabaseError!?TxnNodeState {
        var node = self.node_store.get(node_id) catch |err| {
            return switch (err) {
                node_mod.NodeError.NotFound => null,
                else => DatabaseError.IoError,
            };
        };
        defer node.deinit(self.allocator);

        return .{
            .exists = true,
            .labels = cloneSymbolIds(self.allocator, node.labels) catch return DatabaseError.OutOfMemory,
            .properties = cloneProperties(self.allocator, node.properties) catch return DatabaseError.OutOfMemory,
        };
    }

    fn readVisibleNodeState(self: *Self, txn: ?*Transaction, node_id: NodeId) DatabaseError!?TxnNodeState {
        if (txn) |t| {
            if (self.getTxnOverlay(t)) |overlay| {
                if (overlay.node_states.get(node_id)) |state| {
                    return state.clone(self.allocator) catch return DatabaseError.OutOfMemory;
                }
            }
        }
        return self.readBaseNodeState(node_id);
    }

    fn storeNodeOverlayState(
        self: *Self,
        overlay: *TxnOverlay,
        node_id: NodeId,
        state: TxnNodeState,
    ) DatabaseError!void {
        if (state.exists and !self.node_store.canFitNode(node_id, state.labels, state.properties)) {
            var owned_state = state;
            owned_state.deinit(self.allocator);
            return DatabaseError.ValueTooLarge;
        }

        if (overlay.node_states.getPtr(node_id)) |existing| {
            existing.deinit(self.allocator);
            existing.* = state;
            return;
        }
        overlay.node_states.put(self.allocator, node_id, state) catch {
            var owned_state = state;
            owned_state.deinit(self.allocator);
            return DatabaseError.OutOfMemory;
        };
    }

    fn readBaseEdgeState(self: *Self, edge_id: EdgeId) DatabaseError!?TxnEdgeState {
        var edge = self.edge_store.getById(edge_id) catch |err| {
            return switch (err) {
                edge_mod.EdgeError.NotFound => null,
                else => DatabaseError.IoError,
            };
        };
        defer edge.deinit(self.allocator);

        return .{
            .exists = true,
            .source = edge.source,
            .target = edge.target,
            .edge_type = edge.edge_type,
            .properties = cloneProperties(self.allocator, edge.properties) catch return DatabaseError.OutOfMemory,
        };
    }

    fn readVisibleEdgeState(self: *Self, txn: ?*Transaction, edge_id: EdgeId) DatabaseError!?TxnEdgeState {
        if (txn) |t| {
            if (self.getTxnOverlay(t)) |overlay| {
                if (overlay.edge_states.get(edge_id)) |state| {
                    return state.clone(self.allocator) catch return DatabaseError.OutOfMemory;
                }
            }
        }
        return self.readBaseEdgeState(edge_id);
    }

    fn storeEdgeOverlayState(
        self: *Self,
        overlay: *TxnOverlay,
        edge_id: EdgeId,
        state: TxnEdgeState,
    ) DatabaseError!void {
        if (state.exists and !self.edge_store.canFitEdge(edge_id, state.source, state.target, state.edge_type, state.properties)) {
            var owned_state = state;
            owned_state.deinit(self.allocator);
            return DatabaseError.ValueTooLarge;
        }

        if (overlay.edge_states.getPtr(edge_id)) |existing| {
            existing.deinit(self.allocator);
            existing.* = state;
            return;
        }
        overlay.edge_states.put(self.allocator, edge_id, state) catch {
            var owned_state = state;
            owned_state.deinit(self.allocator);
            return DatabaseError.OutOfMemory;
        };
    }

    fn readBaseVectorState(self: *Self, node_id: NodeId) DatabaseError!TxnVectorState {
        var hnsw = self.hnsw_index orelse return .{ .absent = {} };
        var vector_storage = self.vector_storage orelse return .{ .absent = {} };
        if (hnsw.getNode(node_id)) |entry| {
            const borrowed = vector_storage.borrowByLocation(entry.vector_loc) catch return DatabaseError.IoError;
            defer borrowed.release();
            return .{ .value = self.allocator.dupe(f32, borrowed.data) catch return DatabaseError.OutOfMemory };
        }
        return .{ .absent = {} };
    }

    fn readVisibleVectorState(self: *Self, txn: ?*Transaction, node_id: NodeId) DatabaseError!TxnVectorState {
        if (txn) |t| {
            if (self.getTxnOverlay(t)) |overlay| {
                if (overlay.vector_states.get(node_id)) |state| {
                    return state.clone(self.allocator) catch return DatabaseError.OutOfMemory;
                }
            }
        }
        return self.readBaseVectorState(node_id);
    }

    fn storeVectorOverlayState(
        self: *Self,
        overlay: *TxnOverlay,
        node_id: NodeId,
        state: TxnVectorState,
    ) DatabaseError!void {
        if (overlay.vector_states.getPtr(node_id)) |existing| {
            existing.deinit(self.allocator);
            existing.* = state;
            return;
        }
        overlay.vector_states.put(self.allocator, node_id, state) catch {
            var owned_state = state;
            owned_state.deinit(self.allocator);
            return DatabaseError.OutOfMemory;
        };
    }

    fn collectVisibleNodeIds(self: *Self, txn: ?*Transaction) DatabaseError![]NodeId {
        var ids: std.ArrayListUnmanaged(NodeId) = .empty;
        errdefer ids.deinit(self.allocator);

        var seen: std.AutoHashMapUnmanaged(NodeId, void) = .{};
        defer seen.deinit(self.allocator);

        var iter = self.node_tree.range(null, null) catch return DatabaseError.IoError;
        defer iter.deinit();

        while (true) {
            const entry = iter.next() catch return DatabaseError.IoError;
            if (entry) |e| {
                if (e.key.len < 8) continue;
                const node_id = std.mem.readInt(u64, e.key[0..8], .little);
                var include = true;
                if (txn) |t| {
                    if (self.getTxnOverlay(t)) |overlay| {
                        if (overlay.node_states.get(node_id)) |state| {
                            include = state.exists;
                        }
                    }
                }
                try seen.put(self.allocator, node_id, {});
                if (include) {
                    try ids.append(self.allocator, node_id);
                }
            } else {
                break;
            }
        }

        if (txn) |t| {
            if (self.getTxnOverlay(t)) |overlay| {
                var overlay_iter = overlay.node_states.iterator();
                while (overlay_iter.next()) |entry| {
                    if (seen.contains(entry.key_ptr.*)) continue;
                    if (!entry.value_ptr.exists) continue;
                    try ids.append(self.allocator, entry.key_ptr.*);
                }
            }
        }

        return ids.toOwnedSlice(self.allocator) catch return DatabaseError.OutOfMemory;
    }

    pub fn getAllNodeIdsInTxn(self: *Self, txn: ?*Transaction) DatabaseError![]NodeId {
        return self.collectVisibleNodeIds(txn);
    }

    pub fn getNodesByLabelIdInTxn(self: *Self, txn: ?*Transaction, label_id: symbols_mod.SymbolId) DatabaseError![]NodeId {
        const visible_ids = try self.collectVisibleNodeIds(txn);
        defer self.allocator.free(visible_ids);

        var matches: std.ArrayListUnmanaged(NodeId) = .empty;
        errdefer matches.deinit(self.allocator);

        for (visible_ids) |node_id| {
            var state = (try self.readVisibleNodeState(txn, node_id)) orelse continue;
            defer state.deinit(self.allocator);
            if (!state.exists) continue;
            if (containsSymbolId(state.labels, label_id)) {
                matches.append(self.allocator, node_id) catch return DatabaseError.OutOfMemory;
            }
        }

        return matches.toOwnedSlice(self.allocator) catch return DatabaseError.OutOfMemory;
    }

    fn edgeStateToRef(edge_id: EdgeId, state: TxnEdgeState) EdgeRef {
        return .{
            .id = edge_id,
            .source = state.source,
            .target = state.target,
            .edge_type = state.edge_type,
        };
    }

    fn edgeStateMatches(
        state: TxnEdgeState,
        node_id: NodeId,
        direction: edge_mod.Direction,
        edge_type: ?symbols_mod.SymbolId,
    ) bool {
        if (!state.exists) return false;
        if (edge_type) |type_id| {
            if (state.edge_type != type_id) return false;
        }
        return switch (direction) {
            .outgoing => state.source == node_id,
            .incoming => state.target == node_id,
        };
    }

    fn edgeStateMatchesType(state: TxnEdgeState, edge_type: ?symbols_mod.SymbolId) bool {
        if (!state.exists) return false;
        if (edge_type) |type_id| {
            if (state.edge_type != type_id) return false;
        }
        return true;
    }

    fn edgeTypeIdOrEmpty(self: *Self, edge_type: []const u8) DatabaseError!?symbols_mod.SymbolId {
        return self.symbol_table.lookup(edge_type) catch |err| switch (err) {
            symbols_mod.SymbolError.NotFound => null,
            else => DatabaseError.IoError,
        };
    }

    fn edgeRefsToInfos(self: *Self, refs: []const EdgeRef) DatabaseError![]EdgeInfo {
        var edges: std.ArrayListUnmanaged(EdgeInfo) = .empty;
        errdefer edges.deinit(self.allocator);

        for (refs) |edge_ref| {
            const edge_type_str = self.symbol_table.resolve(edge_ref.edge_type) catch continue;
            edges.append(self.allocator, .{
                .id = edge_ref.id,
                .source = edge_ref.source,
                .target = edge_ref.target,
                .edge_type = edge_type_str,
            }) catch {
                self.allocator.free(edge_type_str);
                return DatabaseError.OutOfMemory;
            };
        }

        return edges.toOwnedSlice(self.allocator) catch return DatabaseError.OutOfMemory;
    }

    fn edgeLimitReached(count: usize, limit: usize) bool {
        return limit != 0 and count >= limit;
    }

    fn collectVisibleEdgeRefs(
        self: *Self,
        txn: ?*Transaction,
        node_id: NodeId,
        direction: edge_mod.Direction,
        edge_type: ?symbols_mod.SymbolId,
        limit: usize,
    ) DatabaseError![]EdgeRef {
        var refs: std.ArrayListUnmanaged(EdgeRef) = .empty;
        errdefer refs.deinit(self.allocator);

        var seen: std.AutoHashMapUnmanaged(EdgeId, void) = .{};
        defer seen.deinit(self.allocator);

        var base_iter = blk: {
            if (direction == .outgoing) {
                if (edge_type) |type_id| {
                    break :blk self.edge_store.getOutgoingRefsByType(node_id, type_id) catch return DatabaseError.IoError;
                }
                break :blk self.edge_store.getOutgoingRefs(node_id) catch return DatabaseError.IoError;
            }
            if (edge_type) |type_id| {
                break :blk self.edge_store.getIncomingRefsByType(node_id, type_id) catch return DatabaseError.IoError;
            }
            break :blk self.edge_store.getIncomingRefs(node_id) catch return DatabaseError.IoError;
        };
        defer base_iter.deinit();

        while (base_iter.next() catch return DatabaseError.IoError) |base_ref| {
            try seen.put(self.allocator, base_ref.id, {});

            if (txn) |t| {
                if (self.getTxnOverlay(t)) |overlay| {
                    if (overlay.edge_states.get(base_ref.id)) |state| {
                        if (edgeStateMatches(state, node_id, direction, edge_type)) {
                            refs.append(self.allocator, edgeStateToRef(base_ref.id, state)) catch return DatabaseError.OutOfMemory;
                            if (edgeLimitReached(refs.items.len, limit)) break;
                        }
                        continue;
                    }
                }
            }

            refs.append(self.allocator, base_ref) catch return DatabaseError.OutOfMemory;
            if (edgeLimitReached(refs.items.len, limit)) break;
        }

        if (!edgeLimitReached(refs.items.len, limit)) {
            if (txn) |t| {
                if (self.getTxnOverlay(t)) |overlay| {
                    var overlay_iter = overlay.edge_states.iterator();
                    while (overlay_iter.next()) |entry| {
                        if (seen.contains(entry.key_ptr.*)) continue;
                        if (!edgeStateMatches(entry.value_ptr.*, node_id, direction, edge_type)) continue;
                        refs.append(self.allocator, edgeStateToRef(entry.key_ptr.*, entry.value_ptr.*)) catch return DatabaseError.OutOfMemory;
                        if (edgeLimitReached(refs.items.len, limit)) break;
                    }
                }
            }
        }

        return refs.toOwnedSlice(self.allocator) catch return DatabaseError.OutOfMemory;
    }

    fn collectVisibleAllEdgeRefs(
        self: *Self,
        txn: ?*Transaction,
        edge_type: ?symbols_mod.SymbolId,
        limit: usize,
    ) DatabaseError![]EdgeRef {
        var refs: std.ArrayListUnmanaged(EdgeRef) = .empty;
        errdefer refs.deinit(self.allocator);

        var seen: std.AutoHashMapUnmanaged(EdgeId, void) = .{};
        defer seen.deinit(self.allocator);

        var base_iter = self.edge_store.scanRefs() catch return DatabaseError.IoError;
        defer base_iter.deinit();

        while (base_iter.next() catch return DatabaseError.IoError) |base_ref| {
            try seen.put(self.allocator, base_ref.id, {});

            if (txn) |t| {
                if (self.getTxnOverlay(t)) |overlay| {
                    if (overlay.edge_states.get(base_ref.id)) |state| {
                        if (edgeStateMatchesType(state, edge_type)) {
                            refs.append(self.allocator, edgeStateToRef(base_ref.id, state)) catch return DatabaseError.OutOfMemory;
                            if (edgeLimitReached(refs.items.len, limit)) break;
                        }
                        continue;
                    }
                }
            }

            if (edge_type) |type_id| {
                if (base_ref.edge_type != type_id) continue;
            }
            refs.append(self.allocator, base_ref) catch return DatabaseError.OutOfMemory;
            if (edgeLimitReached(refs.items.len, limit)) break;
        }

        if (!edgeLimitReached(refs.items.len, limit)) {
            if (txn) |t| {
                if (self.getTxnOverlay(t)) |overlay| {
                    var overlay_iter = overlay.edge_states.iterator();
                    while (overlay_iter.next()) |entry| {
                        if (seen.contains(entry.key_ptr.*)) continue;
                        if (!edgeStateMatchesType(entry.value_ptr.*, edge_type)) continue;
                        refs.append(self.allocator, edgeStateToRef(entry.key_ptr.*, entry.value_ptr.*)) catch return DatabaseError.OutOfMemory;
                        if (edgeLimitReached(refs.items.len, limit)) break;
                    }
                }
            }
        }

        return refs.toOwnedSlice(self.allocator) catch return DatabaseError.OutOfMemory;
    }

    fn findVisibleEdgeId(
        self: *Self,
        txn: ?*Transaction,
        source: NodeId,
        target: NodeId,
        edge_type: symbols_mod.SymbolId,
    ) DatabaseError!?EdgeId {
        const refs = try self.collectVisibleEdgeRefs(txn, source, .outgoing, edge_type, 0);
        defer self.allocator.free(refs);

        var found: ?EdgeId = null;
        for (refs) |edge_ref| {
            if (edge_ref.target != target) continue;
            if (found == null or edge_ref.id < found.?) {
                found = edge_ref.id;
            }
        }
        return found;
    }

    /// Create a new node with the given labels
    /// Returns the new node's ID
    /// If txn is null, the operation is auto-committed
    pub fn createNode(self: *Self, txn: ?*Transaction, labels: []const []const u8) !NodeId {
        if (self.read_only) return DatabaseError.PermissionDenied;

        // Validate transaction if provided
        if (txn) |t| {
            if (!t.isActive()) return DatabaseError.TransactionNotActive;
            if (t.mode == .read_only) return DatabaseError.TransactionReadOnly;
            if (self.getTxnOverlay(t)) |overlay| {
                var label_ids = self.allocator.alloc(symbols_mod.SymbolId, labels.len) catch {
                    return DatabaseError.OutOfMemory;
                };
                errdefer self.allocator.free(label_ids);

                for (labels, 0..) |label, i| {
                    label_ids[i] = self.symbol_table.intern(label) catch {
                        return DatabaseError.IoError;
                    };
                }

                const node_id = self.node_store.next_id;
                self.node_store.next_id += 1;

                try self.storeNodeOverlayState(overlay, node_id, .{
                    .exists = true,
                    .labels = label_ids,
                    .properties = self.allocator.alloc(node_mod.Property, 0) catch return DatabaseError.OutOfMemory,
                });
                overlay.markDirty();

                if (self.query_cache) |cache| cache.bumpSchemaVersion();
                return node_id;
            }
        }

        // Intern all labels
        var label_ids = self.allocator.alloc(symbols_mod.SymbolId, labels.len) catch {
            return DatabaseError.OutOfMemory;
        };
        defer self.allocator.free(label_ids);

        for (labels, 0..) |label, i| {
            label_ids[i] = self.symbol_table.intern(label) catch {
                return DatabaseError.IoError;
            };
        }

        // Create node with no properties (can be added later with setNodeProperty)
        const node_id = self.node_store.create(label_ids, &[_]node_mod.Property{}) catch |err| {
            return mapNodeStoreError(err);
        };

        // Update label index
        for (label_ids) |label_id| {
            self.label_index.add(label_id, node_id) catch |err| {
                // Note: Node is already created at this point. If label index fails,
                // the node exists but won't be queryable by this label.
                // In a transaction, the caller should rollback.
                return switch (err) {
                    label_index_mod.LabelIndexError.OutOfMemory => DatabaseError.OutOfMemory,
                    else => DatabaseError.IoError,
                };
            };
        }

        // Log to WAL and add undo entry if transaction is provided
        if (txn) |t| {
            if (self.txn_manager) |*tm| {
                var buf: [1024]u8 = undefined;
                const payload = wal_payload.serializeNodeInsert(&buf, node_id, labels) catch {
                    return DatabaseError.IoError;
                };
                _ = tm.logOperation(t, .insert, payload) catch {
                    return DatabaseError.IoError;
                };
                // Undo of insert is delete
                tm.addUndoEntry(t, .insert, .node, node_id, 0, 0, null) catch {
                    return DatabaseError.IoError;
                };
            }
        }

        // Invalidate query cache (labels changed)
        if (self.query_cache) |cache| cache.bumpSchemaVersion();

        return node_id;
    }

    /// Get a node by ID
    pub fn getNode(self: *Self, node_id: NodeId) !?node_mod.Node {
        const node = self.node_store.get(node_id) catch |err| {
            return switch (err) {
                node_mod.NodeError.NotFound => null,
                else => DatabaseError.IoError,
            };
        };
        return node;
    }

    pub fn getNodeInTxn(self: *Self, txn: ?*Transaction, node_id: NodeId) DatabaseError!?node_mod.Node {
        if (try self.readVisibleNodeState(txn, node_id)) |state| {
            defer {
                var owned = state;
                owned.deinit(self.allocator);
            }
            if (!state.exists) return null;
            return state.toNode(self.allocator, node_id) catch return DatabaseError.OutOfMemory;
        }
        return null;
    }

    /// Delete a node
    /// If txn is null, the operation is auto-committed
    pub fn deleteNode(self: *Self, txn: ?*Transaction, node_id: NodeId) !void {
        if (self.read_only) return DatabaseError.PermissionDenied;

        // Validate transaction if provided
        if (txn) |t| {
            if (!t.isActive()) return DatabaseError.TransactionNotActive;
            if (t.mode == .read_only) return DatabaseError.TransactionReadOnly;
            if (self.getTxnOverlay(t)) |overlay| {
                const existing = try self.readVisibleNodeState(txn, node_id);
                if (existing) |state| {
                    var owned_state = state;
                    defer owned_state.deinit(self.allocator);
                    if (!owned_state.exists) return;
                } else {
                    return;
                }

                try self.storeNodeOverlayState(overlay, node_id, .{
                    .exists = false,
                    .labels = self.allocator.alloc(symbols_mod.SymbolId, 0) catch return DatabaseError.OutOfMemory,
                    .properties = self.allocator.alloc(node_mod.Property, 0) catch return DatabaseError.OutOfMemory,
                });
                try self.storeVectorOverlayState(overlay, node_id, .{ .absent = {} });
                overlay.markDirty();

                if (self.query_cache) |cache| cache.bumpSchemaVersion();
                return;
            }
        }

        // Get node to find its labels for index cleanup and WAL logging
        var node_labels: []const u16 = &[_]u16{};
        if (try self.getNode(node_id)) |n| {
            var node = n;
            defer node.deinit(self.allocator);

            node_labels = node.labels;

            // Log to WAL and add undo entry before deletion
            if (txn) |t| {
                if (self.txn_manager) |*tm| {
                    // Serialize properties for undo
                    const prop_bytes = wal_payload.serializeProperties(self.allocator, @ptrCast(node.properties)) catch {
                        return DatabaseError.OutOfMemory;
                    };
                    defer self.allocator.free(prop_bytes);

                    // Sized from wal_payload.nodeDeleteSize so a node with
                    // large property blobs serializes into an exact-fit
                    // buffer rather than hitting the old 4 KiB stack cap.
                    const payload_buf = self.allocator.alloc(
                        u8,
                        wal_payload.nodeDeleteSize(node.labels.len, prop_bytes.len),
                    ) catch return DatabaseError.OutOfMemory;
                    defer self.allocator.free(payload_buf);
                    const payload = wal_payload.serializeNodeDelete(
                        payload_buf,
                        node_id,
                        node.labels,
                        prop_bytes,
                    ) catch return DatabaseError.IoError;
                    _ = tm.logOperation(t, .delete, payload) catch {
                        return DatabaseError.IoError;
                    };
                    // Undo of delete is re-insert; store serialized node data
                    tm.addUndoEntry(t, .delete, .node, node_id, 0, 0, payload) catch {
                        return DatabaseError.IoError;
                    };
                }
            }

            // Remove from label index
            if (self.property_index) |*index| {
                index.removeNode(node_id, node.labels, node.properties) catch |err| return mapPropertyIndexError(err);
            }
            try self.ftsReplaceNode(node_id, node.labels, node.properties, &.{}, &.{});
            for (node.labels) |label_id| {
                self.label_index.remove(label_id, node_id) catch |err| {
                    return switch (err) {
                        label_index_mod.LabelIndexError.OutOfMemory => DatabaseError.OutOfMemory,
                        else => DatabaseError.IoError,
                    };
                };
            }
        }

        // Delete from node store
        self.node_store.delete(node_id) catch |err| {
            return switch (err) {
                node_mod.NodeError.NotFound => {},
                else => DatabaseError.IoError,
            };
        };

        if (self.hnsw_index) |*hnsw| {
            hnsw.remove(node_id) catch |err| return mapHnswError(err);
        }

        // Invalidate query cache (labels changed)
        if (self.query_cache) |cache| cache.bumpSchemaVersion();
    }

    /// Check if a node exists
    pub fn nodeExists(self: *Self, node_id: NodeId) DatabaseError!bool {
        return self.node_store.exists(node_id) catch |err| {
            return switch (err) {
                node_mod.NodeError.BufferPoolFull => DatabaseError.BufferPoolFull,
                else => DatabaseError.IoError,
            };
        };
    }

    pub fn nodeExistsInTxn(self: *Self, txn: ?*Transaction, node_id: NodeId) DatabaseError!bool {
        if (try self.readVisibleNodeState(txn, node_id)) |state| {
            defer {
                var owned = state;
                owned.deinit(self.allocator);
            }
            return state.exists;
        }
        return false;
    }

    /// Set a property on a node
    /// If txn is null, the operation is auto-committed
    pub fn setNodeProperty(
        self: *Self,
        txn: ?*Transaction,
        node_id: NodeId,
        key: []const u8,
        value: PropertyValue,
    ) !void {
        if (self.read_only) return DatabaseError.PermissionDenied;

        // Validate transaction if provided
        if (txn) |t| {
            if (!t.isActive()) return DatabaseError.TransactionNotActive;
            if (t.mode == .read_only) return DatabaseError.TransactionReadOnly;
            if (self.getTxnOverlay(t)) |overlay| {
                var state = (try self.readVisibleNodeState(txn, node_id)) orelse return DatabaseError.NotFound;
                defer state.deinit(self.allocator);
                if (!state.exists) return DatabaseError.NotFound;

                const key_id = self.symbol_table.intern(key) catch return DatabaseError.IoError;

                var new_props: std.ArrayListUnmanaged(node_mod.Property) = .empty;
                errdefer {
                    for (new_props.items) |*prop| {
                        var val = prop.value;
                        val.deinit(self.allocator);
                    }
                    new_props.deinit(self.allocator);
                }

                var found = false;
                for (state.properties) |prop| {
                    if (prop.key_id == key_id) {
                        new_props.append(self.allocator, .{
                            .key_id = key_id,
                            .value = value.clone(self.allocator) catch return DatabaseError.OutOfMemory,
                        }) catch return DatabaseError.OutOfMemory;
                        found = true;
                    } else {
                        new_props.append(self.allocator, .{
                            .key_id = prop.key_id,
                            .value = prop.value.clone(self.allocator) catch return DatabaseError.OutOfMemory,
                        }) catch return DatabaseError.OutOfMemory;
                    }
                }
                if (!found) {
                    new_props.append(self.allocator, .{
                        .key_id = key_id,
                        .value = value.clone(self.allocator) catch return DatabaseError.OutOfMemory,
                    }) catch return DatabaseError.OutOfMemory;
                }

                try self.storeNodeOverlayState(overlay, node_id, .{
                    .exists = true,
                    .labels = cloneSymbolIds(self.allocator, state.labels) catch return DatabaseError.OutOfMemory,
                    .properties = new_props.toOwnedSlice(self.allocator) catch return DatabaseError.OutOfMemory,
                });
                overlay.markDirty();
                return;
            }
        }

        // Get the existing node
        var existing_node = self.node_store.get(node_id) catch |err| {
            return switch (err) {
                node_mod.NodeError.NotFound => DatabaseError.NotFound,
                else => DatabaseError.IoError,
            };
        };
        defer existing_node.deinit(self.allocator);

        const key_id = self.symbol_table.intern(key) catch {
            return DatabaseError.IoError;
        };

        // Build new properties array with the updated/added property
        var new_props: std.ArrayListUnmanaged(node_mod.Property) = .empty;
        defer new_props.deinit(self.allocator);

        // Copy existing properties, replacing if key matches
        // Also capture old value for undo
        var found = false;
        var old_value: ?PropertyValue = null;
        for (existing_node.properties) |prop| {
            if (prop.key_id == key_id) {
                old_value = prop.value; // Capture old value for undo
                new_props.append(self.allocator, .{ .key_id = key_id, .value = value }) catch {
                    return DatabaseError.IoError;
                };
                found = true;
            } else {
                new_props.append(self.allocator, prop) catch {
                    return DatabaseError.IoError;
                };
            }
        }

        // Add new property if not found
        if (!found) {
            new_props.append(self.allocator, .{ .key_id = key_id, .value = value }) catch {
                return DatabaseError.IoError;
            };
        }

        // Update the node
        self.node_store.update(node_id, existing_node.labels, new_props.items) catch |err| {
            return mapNodeStoreError(err);
        };
        try self.replaceNodePropertyIndex(
            node_id,
            existing_node.labels,
            existing_node.properties,
            existing_node.labels,
            new_props.items,
        );

        // Log to WAL and add undo entry if transaction is provided.
        // Every buffer below is sized from `wal_payload.*Size` helpers and
        // heap-allocated so STRING/BYTES property values of arbitrary
        // length round-trip through the write-ahead log. The previous
        // fixed [512]u8 / [2048]u8 stack buffers capped values at ~507
        // bytes and composite payloads at ~2 KiB.
        if (txn) |t| {
            if (self.txn_manager) |*tm| {
                const old_value_bytes = try self.serializePropertyValueAlloc(old_value);
                defer if (old_value_bytes) |b| self.allocator.free(b);

                const new_value_bytes = try self.serializePropertyValueAlloc(value) orelse unreachable;
                defer self.allocator.free(new_value_bytes);

                const old_len = if (old_value_bytes) |b| b.len else 0;
                const payload_buf = self.allocator.alloc(
                    u8,
                    wal_payload.propertyUpdateSize(key.len, old_len, new_value_bytes.len),
                ) catch return DatabaseError.OutOfMemory;
                defer self.allocator.free(payload_buf);
                const payload = wal_payload.serializePropertyUpdate(
                    payload_buf,
                    node_id,
                    key,
                    old_value_bytes,
                    new_value_bytes,
                ) catch return DatabaseError.IoError;

                _ = tm.logOperation(t, .update, payload) catch {
                    return DatabaseError.IoError;
                };
                // Undo of update: store payload containing old value.
                // For new properties (found=false), undo is delete; for existing, restore old value.
                const undo_op: UndoOpType = if (found) .update else .insert;
                tm.addUndoEntry(t, undo_op, .property, node_id, 0, key_id, payload) catch {
                    return DatabaseError.IoError;
                };
            }
        }
    }

    /// Heap-allocate and serialize a PropertyValue into a fresh WAL-format
    /// byte slice. Returns null when `value` is null. Caller owns the
    /// returned memory and must free it.
    fn serializePropertyValueAlloc(self: *Self, value: ?PropertyValue) !?[]u8 {
        const v = value orelse return null;
        const size = wal_payload.propertyValueSize(v);
        const buf = self.allocator.alloc(u8, size) catch return DatabaseError.OutOfMemory;
        errdefer self.allocator.free(buf);
        const written = wal_payload.serializePropertyValueToBuf(buf, v) catch return DatabaseError.IoError;
        if (written != size) return DatabaseError.IoError;
        return buf;
    }

    /// Remove a property from a node.
    pub fn removeNodeProperty(
        self: *Self,
        txn: ?*Transaction,
        node_id: NodeId,
        key: []const u8,
    ) !void {
        if (self.read_only) return DatabaseError.PermissionDenied;

        if (txn) |t| {
            if (!t.isActive()) return DatabaseError.TransactionNotActive;
            if (t.mode == .read_only) return DatabaseError.TransactionReadOnly;
            if (self.getTxnOverlay(t)) |overlay| {
                var state = (try self.readVisibleNodeState(txn, node_id)) orelse return DatabaseError.NotFound;
                defer state.deinit(self.allocator);
                if (!state.exists) return DatabaseError.NotFound;

                const key_id = self.symbol_table.intern(key) catch return DatabaseError.IoError;

                var new_props: std.ArrayListUnmanaged(node_mod.Property) = .empty;
                errdefer {
                    for (new_props.items) |*prop| {
                        var val = prop.value;
                        val.deinit(self.allocator);
                    }
                    new_props.deinit(self.allocator);
                }

                for (state.properties) |prop| {
                    if (prop.key_id != key_id) {
                        new_props.append(self.allocator, .{
                            .key_id = prop.key_id,
                            .value = prop.value.clone(self.allocator) catch return DatabaseError.OutOfMemory,
                        }) catch return DatabaseError.OutOfMemory;
                    }
                }

                try self.storeNodeOverlayState(overlay, node_id, .{
                    .exists = true,
                    .labels = cloneSymbolIds(self.allocator, state.labels) catch return DatabaseError.OutOfMemory,
                    .properties = new_props.toOwnedSlice(self.allocator) catch return DatabaseError.OutOfMemory,
                });
                overlay.markDirty();
                return;
            }
        }

        var existing_node = self.node_store.get(node_id) catch |err| {
            return switch (err) {
                node_mod.NodeError.NotFound => DatabaseError.NotFound,
                else => DatabaseError.IoError,
            };
        };
        defer existing_node.deinit(self.allocator);

        const key_id = self.symbol_table.intern(key) catch {
            return DatabaseError.IoError;
        };

        // Find and capture the old value for undo
        var old_value: ?PropertyValue = null;
        for (existing_node.properties) |prop| {
            if (prop.key_id == key_id) {
                old_value = prop.value;
                break;
            }
        }

        // If property doesn't exist, nothing to remove
        if (old_value == null) return;

        // Build new properties array without the specified key
        var new_props: std.ArrayListUnmanaged(node_mod.Property) = .empty;
        defer new_props.deinit(self.allocator);

        for (existing_node.properties) |prop| {
            if (prop.key_id != key_id) {
                new_props.append(self.allocator, prop) catch {
                    return DatabaseError.IoError;
                };
            }
        }

        // Update the node
        self.node_store.update(node_id, existing_node.labels, new_props.items) catch |err| {
            return mapNodeStoreError(err);
        };
        try self.replaceNodePropertyIndex(
            node_id,
            existing_node.labels,
            existing_node.properties,
            existing_node.labels,
            new_props.items,
        );

        // Log to WAL and add undo entry if transaction is provided.
        // Buffer sizes come from `wal_payload.*Size` helpers so arbitrarily
        // large old-value payloads fit (previously capped by [512]u8 and
        // [2048]u8 stack buffers at ~507 B and ~2 KiB respectively).
        if (txn) |t| {
            if (self.txn_manager) |*tm| {
                const old_value_bytes = try self.serializePropertyValueAlloc(old_value);
                defer if (old_value_bytes) |b| self.allocator.free(b);

                const old_len = if (old_value_bytes) |b| b.len else 0;
                const payload_buf = self.allocator.alloc(
                    u8,
                    wal_payload.propertyUpdateSize(key.len, old_len, 0),
                ) catch return DatabaseError.OutOfMemory;
                defer self.allocator.free(payload_buf);
                // Serialize property delete for WAL (old value, no new value)
                const payload = wal_payload.serializePropertyUpdate(
                    payload_buf,
                    node_id,
                    key,
                    old_value_bytes,
                    &[_]u8{},
                ) catch return DatabaseError.IoError;

                _ = tm.logOperation(t, .delete, payload) catch {
                    return DatabaseError.IoError;
                };
                // Undo of delete is restore the old value
                tm.addUndoEntry(t, .delete, .property, node_id, 0, key_id, payload) catch {
                    return DatabaseError.IoError;
                };
            }
        }
    }

    /// Add a label to a node.
    pub fn addNodeLabel(
        self: *Self,
        txn: ?*Transaction,
        node_id: NodeId,
        label: []const u8,
    ) !void {
        if (self.read_only) return DatabaseError.PermissionDenied;

        if (txn) |t| {
            if (!t.isActive()) return DatabaseError.TransactionNotActive;
            if (t.mode == .read_only) return DatabaseError.TransactionReadOnly;
            if (self.getTxnOverlay(t)) |overlay| {
                var state = (try self.readVisibleNodeState(txn, node_id)) orelse return DatabaseError.NotFound;
                defer state.deinit(self.allocator);
                if (!state.exists) return DatabaseError.NotFound;

                const label_id = self.symbol_table.intern(label) catch return DatabaseError.IoError;
                for (state.labels) |existing| {
                    if (existing == label_id) return;
                }

                var new_labels: std.ArrayListUnmanaged(symbols_mod.SymbolId) = .empty;
                errdefer new_labels.deinit(self.allocator);
                for (state.labels) |existing| {
                    new_labels.append(self.allocator, existing) catch return DatabaseError.OutOfMemory;
                }
                new_labels.append(self.allocator, label_id) catch return DatabaseError.OutOfMemory;

                try self.storeNodeOverlayState(overlay, node_id, .{
                    .exists = true,
                    .labels = new_labels.toOwnedSlice(self.allocator) catch return DatabaseError.OutOfMemory,
                    .properties = cloneProperties(self.allocator, state.properties) catch return DatabaseError.OutOfMemory,
                });
                overlay.markDirty();

                if (self.query_cache) |cache| cache.bumpSchemaVersion();
                return;
            }
        }

        var existing_node = self.node_store.get(node_id) catch |err| {
            return switch (err) {
                node_mod.NodeError.NotFound => DatabaseError.NotFound,
                else => DatabaseError.IoError,
            };
        };
        defer existing_node.deinit(self.allocator);

        const label_id = self.symbol_table.intern(label) catch {
            return DatabaseError.IoError;
        };

        // Check if label already exists
        for (existing_node.labels) |l| {
            if (l == label_id) return; // Already has this label
        }

        // Build new labels array with the added label
        var new_labels: std.ArrayList(symbols_mod.SymbolId) = .empty;
        defer new_labels.deinit(self.allocator);

        for (existing_node.labels) |l| {
            new_labels.append(self.allocator, l) catch {
                return DatabaseError.IoError;
            };
        }
        new_labels.append(self.allocator, label_id) catch {
            return DatabaseError.IoError;
        };

        // Update the node
        self.node_store.update(node_id, new_labels.items, existing_node.properties) catch {
            return DatabaseError.IoError;
        };

        // Update label index
        self.label_index.add(label_id, node_id) catch {
            return DatabaseError.IoError;
        };
        try self.replaceNodePropertyIndex(
            node_id,
            existing_node.labels,
            existing_node.properties,
            new_labels.items,
            existing_node.properties,
        );

        // Log undo entry for transaction rollback
        if (txn) |t| {
            if (self.txn_manager) |*tm| {
                var payload_buf: [512]u8 = undefined;
                const payload = wal_payload.serializeLabelAdd(&payload_buf, node_id, label) catch {
                    self.label_index.remove(label_id, node_id) catch {};
                    self.node_store.update(node_id, existing_node.labels, existing_node.properties) catch {};
                    return DatabaseError.IoError;
                };
                _ = tm.logOperation(t, .update, payload) catch |err| {
                    self.label_index.remove(label_id, node_id) catch {};
                    self.node_store.update(node_id, existing_node.labels, existing_node.properties) catch {};
                    return switch (err) {
                        TxnError.OutOfMemory => DatabaseError.OutOfMemory,
                        else => DatabaseError.IoError,
                    };
                };
                tm.addUndoEntry(t, .insert, .label, node_id, 0, label_id, null) catch |err| {
                    // Rollback: remove label from index and restore node
                    self.label_index.remove(label_id, node_id) catch {};
                    self.node_store.update(node_id, existing_node.labels, existing_node.properties) catch {};
                    return switch (err) {
                        TxnError.OutOfMemory => DatabaseError.OutOfMemory,
                        else => DatabaseError.IoError,
                    };
                };
            }
        }
    }

    /// Remove a label from a node.
    pub fn removeNodeLabel(
        self: *Self,
        txn: ?*Transaction,
        node_id: NodeId,
        label: []const u8,
    ) !void {
        if (self.read_only) return DatabaseError.PermissionDenied;

        if (txn) |t| {
            if (!t.isActive()) return DatabaseError.TransactionNotActive;
            if (t.mode == .read_only) return DatabaseError.TransactionReadOnly;
            if (self.getTxnOverlay(t)) |overlay| {
                var state = (try self.readVisibleNodeState(txn, node_id)) orelse return DatabaseError.NotFound;
                defer state.deinit(self.allocator);
                if (!state.exists) return DatabaseError.NotFound;

                const label_id = self.symbol_table.lookup(label) catch return;

                var new_labels: std.ArrayListUnmanaged(symbols_mod.SymbolId) = .empty;
                errdefer new_labels.deinit(self.allocator);

                var found = false;
                for (state.labels) |existing| {
                    if (existing == label_id) {
                        found = true;
                    } else {
                        new_labels.append(self.allocator, existing) catch return DatabaseError.OutOfMemory;
                    }
                }
                if (!found) return;

                const owned_labels = new_labels.toOwnedSlice(self.allocator) catch return DatabaseError.OutOfMemory;
                const owned_properties = cloneProperties(self.allocator, state.properties) catch {
                    self.allocator.free(owned_labels);
                    return DatabaseError.OutOfMemory;
                };
                try self.storeNodeOverlayState(overlay, node_id, .{
                    .exists = true,
                    .labels = owned_labels,
                    .properties = owned_properties,
                });
                if (owned_labels.len == 0) {
                    try self.storeVectorOverlayState(overlay, node_id, .{ .absent = {} });
                }
                overlay.markDirty();

                if (self.query_cache) |cache| cache.bumpSchemaVersion();
                return;
            }
        }

        var existing_node = self.node_store.get(node_id) catch |err| {
            return switch (err) {
                node_mod.NodeError.NotFound => DatabaseError.NotFound,
                else => DatabaseError.IoError,
            };
        };
        defer existing_node.deinit(self.allocator);

        const label_id = self.symbol_table.lookup(label) catch {
            return; // Label doesn't exist, nothing to remove
        };

        // Build new labels array without the removed label
        var new_labels: std.ArrayList(symbols_mod.SymbolId) = .empty;
        defer new_labels.deinit(self.allocator);

        var found = false;
        for (existing_node.labels) |l| {
            if (l == label_id) {
                found = true;
            } else {
                new_labels.append(self.allocator, l) catch {
                    return DatabaseError.IoError;
                };
            }
        }

        if (!found) return; // Label wasn't on this node

        // Update the node
        self.node_store.update(node_id, new_labels.items, existing_node.properties) catch {
            return DatabaseError.IoError;
        };

        // Update label index
        self.label_index.remove(label_id, node_id) catch {
            return DatabaseError.IoError;
        };
        try self.replaceNodePropertyIndex(
            node_id,
            existing_node.labels,
            existing_node.properties,
            new_labels.items,
            existing_node.properties,
        );

        if (new_labels.items.len == 0) {
            if (self.hnsw_index) |*hnsw| {
                hnsw.remove(node_id) catch |err| return mapHnswError(err);
            }
        }

        // Log undo entry for transaction rollback
        if (txn) |t| {
            if (self.txn_manager) |*tm| {
                var payload_buf: [512]u8 = undefined;
                const payload = wal_payload.serializeLabelRemove(&payload_buf, node_id, label) catch {
                    self.label_index.add(label_id, node_id) catch {};
                    self.node_store.update(node_id, existing_node.labels, existing_node.properties) catch {};
                    return DatabaseError.IoError;
                };
                _ = tm.logOperation(t, .update, payload) catch |err| {
                    self.label_index.add(label_id, node_id) catch {};
                    self.node_store.update(node_id, existing_node.labels, existing_node.properties) catch {};
                    return switch (err) {
                        TxnError.OutOfMemory => DatabaseError.OutOfMemory,
                        else => DatabaseError.IoError,
                    };
                };
                tm.addUndoEntry(t, .delete, .label, node_id, 0, label_id, null) catch |err| {
                    // Rollback: re-add label to index and restore node
                    self.label_index.add(label_id, node_id) catch {};
                    self.node_store.update(node_id, existing_node.labels, existing_node.properties) catch {};
                    return switch (err) {
                        TxnError.OutOfMemory => DatabaseError.OutOfMemory,
                        else => DatabaseError.IoError,
                    };
                };
            }
        }
    }

    /// Clear all properties from a node.
    pub fn clearNodeProperties(
        self: *Self,
        txn: ?*Transaction,
        node_id: NodeId,
    ) !void {
        if (self.read_only) return DatabaseError.PermissionDenied;

        if (txn) |t| {
            if (!t.isActive()) return DatabaseError.TransactionNotActive;
            if (t.mode == .read_only) return DatabaseError.TransactionReadOnly;
            if (self.getTxnOverlay(t)) |overlay| {
                var state = (try self.readVisibleNodeState(txn, node_id)) orelse return DatabaseError.NotFound;
                defer state.deinit(self.allocator);
                if (!state.exists) return DatabaseError.NotFound;

                try self.storeNodeOverlayState(overlay, node_id, .{
                    .exists = true,
                    .labels = cloneSymbolIds(self.allocator, state.labels) catch return DatabaseError.OutOfMemory,
                    .properties = self.allocator.alloc(node_mod.Property, 0) catch return DatabaseError.OutOfMemory,
                });
                overlay.markDirty();
                return;
            }
        }

        var existing_node = self.node_store.get(node_id) catch |err| {
            return switch (err) {
                node_mod.NodeError.NotFound => DatabaseError.NotFound,
                else => DatabaseError.IoError,
            };
        };
        defer existing_node.deinit(self.allocator);

        // Update node with empty properties
        self.node_store.update(node_id, existing_node.labels, &[_]node_mod.Property{}) catch {
            return DatabaseError.IoError;
        };
        try self.replaceNodePropertyIndex(
            node_id,
            existing_node.labels,
            existing_node.properties,
            existing_node.labels,
            &.{},
        );
    }

    /// Get a property from a node.
    /// Note: The caller owns the returned PropertyValue and must call deinit on it
    /// for any heap-backed value to avoid memory leaks.
    pub fn getNodeProperty(
        self: *Self,
        node_id: NodeId,
        key: []const u8,
    ) !?PropertyValue {
        // Get the node
        var existing_node = self.node_store.get(node_id) catch |err| {
            return switch (err) {
                node_mod.NodeError.NotFound => DatabaseError.NotFound,
                else => DatabaseError.IoError,
            };
        };
        defer existing_node.deinit(self.allocator);

        const key_id = self.symbol_table.lookup(key) catch |err| {
            return switch (err) {
                symbols_mod.SymbolError.NotFound => null,
                else => DatabaseError.IoError,
            };
        };

        // Find the property and clone it
        for (existing_node.properties) |prop| {
            if (prop.key_id == key_id) {
                return prop.value.clone(self.allocator) catch return DatabaseError.OutOfMemory;
            }
        }

        return null;
    }

    pub fn getNodePropertyInTxn(
        self: *Self,
        txn: ?*Transaction,
        node_id: NodeId,
        key: []const u8,
    ) DatabaseError!?PropertyValue {
        var state = (try self.readVisibleNodeState(txn, node_id)) orelse return null;
        defer state.deinit(self.allocator);
        if (!state.exists) return null;

        const key_id = self.symbol_table.lookup(key) catch |err| {
            return switch (err) {
                symbols_mod.SymbolError.NotFound => null,
                else => DatabaseError.IoError,
            };
        };

        for (state.properties) |prop| {
            if (prop.key_id == key_id) {
                return prop.value.clone(self.allocator) catch return DatabaseError.OutOfMemory;
            }
        }
        return null;
    }

    // ========================================================================
    // Vector Operations
    // ========================================================================

    /// Set a vector embedding on a node.
    /// The vector is stored in the vector storage and indexed in HNSW for similarity search.
    pub fn setNodeVector(
        self: *Self,
        node_id: NodeId,
        vector: []const f32,
    ) !void {
        if (self.read_only) return DatabaseError.PermissionDenied;

        // Check if vector storage is enabled
        _ = self.vector_storage orelse return DatabaseError.IoError;
        try self.validateVectorValue(vector);

        // Verify node exists
        if (!(try self.nodeExists(node_id))) {
            return DatabaseError.NotFound;
        }

        // HNSW stores the vector internally while indexing it for similarity search.
        if (self.hnsw_index) |*hnsw| {
            hnsw.insert(node_id, vector) catch |err| return mapHnswError(err);
        }
    }

    pub fn setNodeVectorInTxn(
        self: *Self,
        txn: ?*Transaction,
        node_id: NodeId,
        vector: []const f32,
    ) DatabaseError!void {
        if (txn) |t| {
            if (!t.isActive()) return DatabaseError.TransactionNotActive;
            if (t.mode == .read_only) return DatabaseError.TransactionReadOnly;
            if (self.getTxnOverlay(t)) |overlay| {
                try self.validateVectorValue(vector);
                if (!(try self.nodeExistsInTxn(txn, node_id))) return DatabaseError.NotFound;
                try self.storeVectorOverlayState(overlay, node_id, .{
                    .value = self.allocator.dupe(f32, vector) catch return DatabaseError.OutOfMemory,
                });
                overlay.markDirty();
                return;
            }
        }
        try self.setNodeVector(node_id, vector);
    }

    pub fn getNodeVectorInTxn(self: *Self, txn: ?*Transaction, node_id: NodeId) DatabaseError!?[]f32 {
        var state = try self.readVisibleVectorState(txn, node_id);
        return switch (state) {
            .absent => blk: {
                state.deinit(self.allocator);
                break :blk null;
            },
            .value => |vector| vector,
        };
    }

    fn vectorDistanceForSearch(self: *Self, query_vector: []const f32, candidate: []const f32) f32 {
        if (self.hnsw_index) |*hnsw| {
            const distance_fn = hnsw_mod.getDistanceFn(hnsw.config.metric);
            return distance_fn(query_vector, candidate);
        }
        return vector_distance.euclideanDistance(query_vector, candidate);
    }

    pub fn vectorSearchInTxn(
        self: *Self,
        txn: ?*Transaction,
        query_vector: []const f32,
        k: u32,
        ef_search: ?u16,
    ) DatabaseError![]VectorSearchResult {
        try self.validateVectorValue(query_vector);

        if (txn) |t| {
            if (self.getTxnOverlay(t)) |overlay| {
                if (overlay.vector_states.count() == 0) {
                    return self.vectorSearch(query_vector, k, ef_search);
                }

                const extra: u32 = @intCast(overlay.vector_states.count());
                const base_results = try self.vectorSearch(query_vector, k + extra, ef_search);
                defer self.freeVectorSearchResults(base_results);

                var merged: std.ArrayListUnmanaged(VectorSearchResult) = .empty;
                errdefer merged.deinit(self.allocator);

                var seen: std.AutoHashMapUnmanaged(NodeId, void) = .{};
                defer seen.deinit(self.allocator);

                for (base_results) |result| {
                    try seen.put(self.allocator, result.node_id, {});
                    if (!(try self.nodeExistsInTxn(txn, result.node_id))) continue;
                    if (overlay.vector_states.get(result.node_id)) |state| {
                        switch (state) {
                            .absent => continue,
                            .value => |vector| {
                                merged.append(self.allocator, .{
                                    .node_id = result.node_id,
                                    .distance = self.vectorDistanceForSearch(query_vector, vector),
                                }) catch return DatabaseError.OutOfMemory;
                            },
                        }
                        continue;
                    }
                    merged.append(self.allocator, result) catch return DatabaseError.OutOfMemory;
                }

                var overlay_iter = overlay.vector_states.iterator();
                while (overlay_iter.next()) |entry| {
                    if (seen.contains(entry.key_ptr.*)) continue;
                    if (!(try self.nodeExistsInTxn(txn, entry.key_ptr.*))) continue;
                    switch (entry.value_ptr.*) {
                        .absent => {},
                        .value => |vector| {
                            merged.append(self.allocator, .{
                                .node_id = entry.key_ptr.*,
                                .distance = self.vectorDistanceForSearch(query_vector, vector),
                            }) catch return DatabaseError.OutOfMemory;
                        },
                    }
                }

                std.mem.sort(VectorSearchResult, merged.items, {}, struct {
                    fn lessThan(_: void, lhs: VectorSearchResult, rhs: VectorSearchResult) bool {
                        return lhs.distance < rhs.distance;
                    }
                }.lessThan);

                if (merged.items.len > k) {
                    merged.items.len = k;
                }
                return merged.toOwnedSlice(self.allocator) catch return DatabaseError.OutOfMemory;
            }
        }

        return self.vectorSearch(query_vector, k, ef_search);
    }

    /// Search for similar vectors using HNSW index.
    /// Returns node IDs and distances sorted by similarity (closest first).
    pub fn vectorSearch(
        self: *Self,
        query_vector: []const f32,
        k: u32,
        ef_search: ?u16,
    ) DatabaseError![]VectorSearchResult {
        try self.validateVectorValue(query_vector);
        const hnsw = &(self.hnsw_index orelse return DatabaseError.IoError);

        const search_limit: u32 = @intCast(@min(hnsw.vector_count, @as(u64, std.math.maxInt(u32))));
        const raw_results = hnsw.search(query_vector, search_limit, ef_search) catch |err| {
            return switch (err) {
                HnswError.EmptyIndex => self.allocator.alloc(VectorSearchResult, 0) catch return DatabaseError.OutOfMemory,
                HnswError.NotFound => self.allocator.alloc(VectorSearchResult, 0) catch return DatabaseError.OutOfMemory,
                HnswError.DimensionMismatch => DatabaseError.InvalidArgument,
                HnswError.OutOfMemory => DatabaseError.OutOfMemory,
                else => DatabaseError.IoError,
            };
        };
        defer hnsw.freeResults(raw_results);

        var filtered: std.ArrayListUnmanaged(VectorSearchResult) = .empty;
        errdefer filtered.deinit(self.allocator);

        for (raw_results) |result| {
            if (filtered.items.len >= k) break;
            if (!(try self.nodeExists(result.node_id))) continue;
            filtered.append(self.allocator, result) catch return DatabaseError.OutOfMemory;
        }

        return filtered.toOwnedSlice(self.allocator) catch return DatabaseError.OutOfMemory;
    }

    /// Free vector search results allocated by vectorSearch.
    pub fn freeVectorSearchResults(self: *Self, results: []VectorSearchResult) void {
        self.allocator.free(results);
    }

    // ========================================================================
    // Full-Text Search Operations
    // ========================================================================
    //
    // Searching is `ftsSearchIndex` and its transactional form, declared beside
    // the rest of the index catalog. There is no unscoped search: a query names
    // the label and property whose index answers it.

    fn overlayFtsScore(self: *Self, query_text: []const u8, text: []const u8) DatabaseError!f32 {
        const lower_query = self.allocator.dupe(u8, query_text) catch return DatabaseError.OutOfMemory;
        defer self.allocator.free(lower_query);
        for (lower_query) |*ch| ch.* = std.ascii.toLower(ch.*);

        const lower_text = self.allocator.dupe(u8, text) catch return DatabaseError.OutOfMemory;
        defer self.allocator.free(lower_text);
        for (lower_text) |*ch| ch.* = std.ascii.toLower(ch.*);

        var score: f32 = 0;
        var iter = std.mem.tokenizeAny(u8, lower_query, " \t\r\n");
        while (iter.next()) |token| {
            if (token.len == 0) continue;
            if (std.mem.indexOf(u8, lower_text, token) == null) return 0;
            score += 1;
        }
        return score;
    }

    /// Free FTS search results.
    ///
    /// They are a plain slice from this allocator. This used to reach through the
    /// unscoped index to call free on its behalf, which stopped meaning anything
    /// once every index was built per search.
    pub fn freeFtsSearchResults(self: *Self, results: []FtsSearchResult) void {
        if (results.len == 0) return;
        self.allocator.free(results);
    }

    // ========================================================================
    // Graph Operations - Edges
    // ========================================================================

    /// Create an edge between two nodes and return its stable edge ID.
    /// If txn is null, the operation is auto-committed.
    pub fn createEdgeAndGetId(
        self: *Self,
        txn: ?*Transaction,
        source: NodeId,
        target: NodeId,
        edge_type: []const u8,
    ) !EdgeId {
        if (self.read_only) return DatabaseError.PermissionDenied;

        // Validate transaction if provided
        if (txn) |t| {
            if (!t.isActive()) return DatabaseError.TransactionNotActive;
            if (t.mode == .read_only) return DatabaseError.TransactionReadOnly;
            if (self.getTxnOverlay(t)) |overlay| {
                const type_id = self.symbol_table.intern(edge_type) catch return DatabaseError.IoError;
                const edge_id = self.edge_store.reserveNextEdgeId();
                try self.storeEdgeOverlayState(overlay, edge_id, .{
                    .exists = true,
                    .source = source,
                    .target = target,
                    .edge_type = type_id,
                    .properties = emptyProperties(self.allocator) catch return DatabaseError.OutOfMemory,
                });
                overlay.markDirty();

                if (self.adjacency_cache) |*cache| {
                    cache.invalidate(source);
                }
                if (self.query_cache) |cache| cache.bumpSchemaVersion();
                return edge_id;
            }
        }

        // Intern edge type
        const type_id = self.symbol_table.intern(edge_type) catch {
            return DatabaseError.IoError;
        };

        // Create edge with no properties and get stable edge ID.
        const edge_id = self.edge_store.createAndGetId(source, target, type_id, &[_]node_mod.Property{}) catch |err| {
            return mapEdgeStoreError(err);
        };

        // Log to WAL and add undo entry if transaction is provided.
        // Buffer sized via edgeInsertSize so arbitrarily long edge_type
        // names survive (previously capped at ~240 bytes by [256]u8).
        if (txn) |t| {
            if (self.txn_manager) |*tm| {
                const payload_buf = self.allocator.alloc(
                    u8,
                    wal_payload.edgeInsertSize(edge_type),
                ) catch return DatabaseError.OutOfMemory;
                defer self.allocator.free(payload_buf);
                const payload = wal_payload.serializeEdgeInsert(
                    payload_buf,
                    edge_id,
                    source,
                    target,
                    edge_type,
                ) catch return DatabaseError.IoError;
                _ = tm.logOperation(t, .insert, payload) catch {
                    return DatabaseError.IoError;
                };
                // Undo of insert is delete
                tm.addUndoEntry(t, .insert, .edge, edge_id, 0, 0, null) catch {
                    return DatabaseError.IoError;
                };
            }
        }

        // Invalidate adjacency cache for source node
        if (self.adjacency_cache) |*cache| {
            cache.invalidate(source);
        }

        // Invalidate query cache (edge types changed)
        if (self.query_cache) |cache| cache.bumpSchemaVersion();
        return edge_id;
    }

    /// Create an edge between two nodes.
    /// If txn is null, the operation is auto-committed.
    pub fn createEdge(
        self: *Self,
        txn: ?*Transaction,
        source: NodeId,
        target: NodeId,
        edge_type: []const u8,
    ) !void {
        _ = try self.createEdgeAndGetId(txn, source, target, edge_type);
    }

    /// Delete an edge
    /// If txn is null, the operation is auto-committed
    pub fn deleteEdge(
        self: *Self,
        txn: ?*Transaction,
        source: NodeId,
        target: NodeId,
        edge_type: []const u8,
    ) !void {
        if (self.read_only) return DatabaseError.PermissionDenied;

        // Validate transaction if provided
        if (txn) |t| {
            if (!t.isActive()) return DatabaseError.TransactionNotActive;
            if (t.mode == .read_only) return DatabaseError.TransactionReadOnly;
            if (self.getTxnOverlay(t)) |overlay| {
                const type_id = self.symbol_table.intern(edge_type) catch return DatabaseError.IoError;
                const edge_id = (try self.findVisibleEdgeId(txn, source, target, type_id)) orelse return DatabaseError.NotFound;
                var existing = (try self.readVisibleEdgeState(txn, edge_id)) orelse return DatabaseError.NotFound;
                defer existing.deinit(self.allocator);
                if (!existing.exists) return DatabaseError.NotFound;
                try self.storeEdgeOverlayState(overlay, edge_id, .{
                    .exists = false,
                    .source = existing.source,
                    .target = existing.target,
                    .edge_type = existing.edge_type,
                    .properties = cloneProperties(self.allocator, existing.properties) catch return DatabaseError.OutOfMemory,
                });
                overlay.markDirty();

                if (self.adjacency_cache) |*cache| {
                    cache.invalidate(source);
                    cache.invalidate(target);
                }
                if (self.query_cache) |cache| cache.bumpSchemaVersion();
                return;
            }
        }

        // Intern edge type
        const type_id = self.symbol_table.intern(edge_type) catch {
            return DatabaseError.IoError;
        };

        var edge = self.edge_store.get(source, target, type_id) catch {
            return DatabaseError.NotFound;
        };
        defer edge.deinit(self.allocator);
        const edge_id = edge.id;

        // Log to WAL and add undo entry before deletion.
        if (txn) |t| {
            if (self.txn_manager) |*tm| {
                var prop_bytes: []u8 = &[_]u8{};
                var owns_prop_bytes = false;
                if (edge.properties.len > 0) {
                    prop_bytes = wal_payload.serializeProperties(self.allocator, @ptrCast(edge.properties)) catch &[_]u8{};
                    owns_prop_bytes = prop_bytes.len > 0;
                }
                defer if (owns_prop_bytes) self.allocator.free(prop_bytes);

                const payload_buf = self.allocator.alloc(
                    u8,
                    wal_payload.edgeDeleteSize(prop_bytes.len),
                ) catch return DatabaseError.OutOfMemory;
                defer self.allocator.free(payload_buf);
                const payload = wal_payload.serializeEdgeDelete(
                    payload_buf,
                    edge_id,
                    source,
                    target,
                    type_id,
                    prop_bytes,
                ) catch return DatabaseError.IoError;
                _ = tm.logOperation(t, .delete, payload) catch {
                    return DatabaseError.IoError;
                };
                // Undo of delete is re-insert with original edge identity.
                tm.addUndoEntry(t, .delete, .edge, edge_id, 0, 0, payload) catch {
                    return DatabaseError.IoError;
                };
            }
        }

        if (self.property_index) |*index| {
            index.removeEdge(edge_id, edge.edge_type, edge.properties) catch |err| return mapPropertyIndexError(err);
        }
        try self.ftsReplaceEdge(edge_id, edge.edge_type, edge.properties, 0, &.{}, true, false);
        self.edge_store.deleteById(edge_id) catch {
            return DatabaseError.IoError;
        };

        // Invalidate adjacency cache for source node
        if (self.adjacency_cache) |*cache| {
            cache.invalidate(source);
            cache.invalidate(target);
        }

        // Invalidate query cache (edge types changed)
        if (self.query_cache) |cache| cache.bumpSchemaVersion();
    }

    /// Delete an edge by stable edge ID.
    /// If txn is null, the operation is auto-committed.
    pub fn deleteEdgeById(
        self: *Self,
        txn: ?*Transaction,
        edge_id: EdgeId,
    ) !void {
        if (self.read_only) return DatabaseError.PermissionDenied;

        if (txn) |t| {
            if (!t.isActive()) return DatabaseError.TransactionNotActive;
            if (t.mode == .read_only) return DatabaseError.TransactionReadOnly;
            if (self.getTxnOverlay(t)) |overlay| {
                const existing = try self.readVisibleEdgeState(txn, edge_id);
                if (existing) |state| {
                    var owned_state = state;
                    defer owned_state.deinit(self.allocator);
                    if (!owned_state.exists) return DatabaseError.NotFound;

                    try self.storeEdgeOverlayState(overlay, edge_id, .{
                        .exists = false,
                        .source = owned_state.source,
                        .target = owned_state.target,
                        .edge_type = owned_state.edge_type,
                        .properties = cloneProperties(self.allocator, owned_state.properties) catch return DatabaseError.OutOfMemory,
                    });
                    overlay.markDirty();

                    if (self.adjacency_cache) |*cache| {
                        cache.invalidate(owned_state.source);
                        cache.invalidate(owned_state.target);
                    }
                    if (self.query_cache) |cache| cache.bumpSchemaVersion();
                    return;
                }
                return DatabaseError.NotFound;
            }
        }

        // Resolve edge data before deletion for WAL/undo/cache invalidation.
        var edge = self.edge_store.getById(edge_id) catch return DatabaseError.NotFound;
        defer edge.deinit(self.allocator);
        const source = edge.source;
        const target = edge.target;

        if (txn) |t| {
            if (self.txn_manager) |*tm| {
                var prop_bytes: []u8 = &[_]u8{};
                var owns_prop_bytes = false;
                if (edge.properties.len > 0) {
                    prop_bytes = wal_payload.serializeProperties(self.allocator, @ptrCast(edge.properties)) catch &[_]u8{};
                    owns_prop_bytes = prop_bytes.len > 0;
                }
                defer if (owns_prop_bytes) self.allocator.free(prop_bytes);

                const payload_buf = self.allocator.alloc(
                    u8,
                    wal_payload.edgeDeleteSize(prop_bytes.len),
                ) catch return DatabaseError.OutOfMemory;
                defer self.allocator.free(payload_buf);
                const payload = wal_payload.serializeEdgeDelete(
                    payload_buf,
                    edge.id,
                    edge.source,
                    edge.target,
                    edge.edge_type,
                    prop_bytes,
                ) catch return DatabaseError.IoError;
                _ = tm.logOperation(t, .delete, payload) catch return DatabaseError.IoError;
                tm.addUndoEntry(t, .delete, .edge, edge.id, 0, 0, payload) catch return DatabaseError.IoError;
            }
        }

        if (self.property_index) |*index| {
            index.removeEdge(edge_id, edge.edge_type, edge.properties) catch |err| return mapPropertyIndexError(err);
        }
        try self.ftsReplaceEdge(edge_id, edge.edge_type, edge.properties, 0, &.{}, true, false);
        self.edge_store.deleteById(edge_id) catch return DatabaseError.IoError;

        if (self.adjacency_cache) |*cache| {
            cache.invalidate(source);
            cache.invalidate(target);
        }
        if (self.query_cache) |cache| cache.bumpSchemaVersion();
    }

    /// Set a property on an edge by stable edge ID.
    pub fn setEdgePropertyById(
        self: *Self,
        txn: ?*Transaction,
        edge_id: EdgeId,
        key: []const u8,
        value: PropertyValue,
    ) !void {
        if (self.read_only) return DatabaseError.PermissionDenied;

        if (txn) |t| {
            if (!t.isActive()) return DatabaseError.TransactionNotActive;
            if (t.mode == .read_only) return DatabaseError.TransactionReadOnly;
            if (self.getTxnOverlay(t)) |overlay| {
                var state = (try self.readVisibleEdgeState(txn, edge_id)) orelse return DatabaseError.NotFound;
                defer state.deinit(self.allocator);
                if (!state.exists) return DatabaseError.NotFound;

                const key_id = self.symbol_table.intern(key) catch return DatabaseError.IoError;
                var new_props: std.ArrayListUnmanaged(node_mod.Property) = .empty;
                errdefer {
                    for (new_props.items) |*prop| {
                        var val = prop.value;
                        val.deinit(self.allocator);
                    }
                    new_props.deinit(self.allocator);
                }

                var found = false;
                for (state.properties) |prop| {
                    if (prop.key_id == key_id) {
                        new_props.append(self.allocator, .{
                            .key_id = key_id,
                            .value = value.clone(self.allocator) catch return DatabaseError.OutOfMemory,
                        }) catch return DatabaseError.OutOfMemory;
                        found = true;
                    } else {
                        new_props.append(self.allocator, .{
                            .key_id = prop.key_id,
                            .value = prop.value.clone(self.allocator) catch return DatabaseError.OutOfMemory,
                        }) catch return DatabaseError.OutOfMemory;
                    }
                }
                if (!found) {
                    new_props.append(self.allocator, .{
                        .key_id = key_id,
                        .value = value.clone(self.allocator) catch return DatabaseError.OutOfMemory,
                    }) catch return DatabaseError.OutOfMemory;
                }

                try self.storeEdgeOverlayState(overlay, edge_id, .{
                    .exists = true,
                    .source = state.source,
                    .target = state.target,
                    .edge_type = state.edge_type,
                    .properties = new_props.toOwnedSlice(self.allocator) catch return DatabaseError.OutOfMemory,
                });
                overlay.markDirty();
                return;
            }
        }

        var edge = self.edge_store.getById(edge_id) catch |err| {
            return switch (err) {
                edge_mod.EdgeError.NotFound => DatabaseError.NotFound,
                else => DatabaseError.IoError,
            };
        };
        defer edge.deinit(self.allocator);

        const key_id = self.symbol_table.intern(key) catch return DatabaseError.IoError;

        var new_props: std.ArrayListUnmanaged(node_mod.Property) = .empty;
        defer new_props.deinit(self.allocator);

        var found = false;
        var old_value: ?PropertyValue = null;
        for (edge.properties) |prop| {
            if (prop.key_id == key_id) {
                old_value = prop.value;
                new_props.append(self.allocator, .{ .key_id = key_id, .value = value }) catch return DatabaseError.IoError;
                found = true;
            } else {
                new_props.append(self.allocator, prop) catch return DatabaseError.IoError;
            }
        }
        if (!found) {
            new_props.append(self.allocator, .{ .key_id = key_id, .value = value }) catch return DatabaseError.IoError;
        }

        if (!self.edge_store.canFitEdge(edge_id, edge.source, edge.target, edge.edge_type, new_props.items)) {
            return DatabaseError.ValueTooLarge;
        }

        if (txn) |t| {
            if (self.txn_manager) |*tm| {
                const old_value_bytes = try self.serializePropertyValueAlloc(old_value);
                defer if (old_value_bytes) |b| self.allocator.free(b);

                var old_prop_bytes: []u8 = &[_]u8{};
                var owns_old_prop_bytes = false;
                if (edge.properties.len > 0) {
                    old_prop_bytes = wal_payload.serializeProperties(self.allocator, @ptrCast(edge.properties)) catch &[_]u8{};
                    owns_old_prop_bytes = old_prop_bytes.len > 0;
                }
                defer if (owns_old_prop_bytes) self.allocator.free(old_prop_bytes);

                const old_payload_buf = self.allocator.alloc(
                    u8,
                    wal_payload.edgeDeleteSize(old_prop_bytes.len),
                ) catch return DatabaseError.OutOfMemory;
                defer self.allocator.free(old_payload_buf);
                const old_payload = wal_payload.serializeEdgeDelete(
                    old_payload_buf,
                    edge.id,
                    edge.source,
                    edge.target,
                    edge.edge_type,
                    old_prop_bytes,
                ) catch return DatabaseError.IoError;

                const new_value_bytes = try self.serializePropertyValueAlloc(value) orelse unreachable;
                defer self.allocator.free(new_value_bytes);

                const old_len = if (old_value_bytes) |b| b.len else 0;
                const wal_payload_buf = self.allocator.alloc(
                    u8,
                    wal_payload.edgePropertyUpdateSize(key.len, old_len, new_value_bytes.len),
                ) catch return DatabaseError.OutOfMemory;
                defer self.allocator.free(wal_payload_buf);
                const wal_payload_bytes = wal_payload.serializeEdgePropertyUpdate(
                    wal_payload_buf,
                    edge.id,
                    key,
                    old_value_bytes,
                    new_value_bytes,
                ) catch return DatabaseError.IoError;

                _ = tm.logOperation(t, .update, wal_payload_bytes) catch return DatabaseError.IoError;
                tm.addUndoEntry(t, .update, .edge, edge.id, 0, 0, old_payload) catch return DatabaseError.IoError;
            }
        }

        self.edge_store.deleteById(edge_id) catch return DatabaseError.IoError;
        self.edge_store.createWithId(edge_id, edge.source, edge.target, edge.edge_type, new_props.items) catch |err| {
            // Best-effort restore of previous state.
            self.edge_store.createWithId(edge_id, edge.source, edge.target, edge.edge_type, edge.properties) catch {};
            return mapEdgeStoreError(err);
        };
        try self.replaceEdgePropertyIndex(
            edge_id,
            edge.edge_type,
            edge.properties,
            edge.edge_type,
            new_props.items,
        );
    }

    /// Remove a property from an edge by stable edge ID.
    pub fn removeEdgePropertyById(
        self: *Self,
        txn: ?*Transaction,
        edge_id: EdgeId,
        key: []const u8,
    ) !void {
        if (self.read_only) return DatabaseError.PermissionDenied;

        if (txn) |t| {
            if (!t.isActive()) return DatabaseError.TransactionNotActive;
            if (t.mode == .read_only) return DatabaseError.TransactionReadOnly;
            if (self.getTxnOverlay(t)) |overlay| {
                var state = (try self.readVisibleEdgeState(txn, edge_id)) orelse return DatabaseError.NotFound;
                defer state.deinit(self.allocator);
                if (!state.exists) return DatabaseError.NotFound;

                const key_id = self.symbol_table.intern(key) catch return DatabaseError.IoError;
                var new_props: std.ArrayListUnmanaged(node_mod.Property) = .empty;
                errdefer {
                    for (new_props.items) |*prop| {
                        var val = prop.value;
                        val.deinit(self.allocator);
                    }
                    new_props.deinit(self.allocator);
                }

                for (state.properties) |prop| {
                    if (prop.key_id != key_id) {
                        new_props.append(self.allocator, .{
                            .key_id = prop.key_id,
                            .value = prop.value.clone(self.allocator) catch return DatabaseError.OutOfMemory,
                        }) catch return DatabaseError.OutOfMemory;
                    }
                }

                try self.storeEdgeOverlayState(overlay, edge_id, .{
                    .exists = true,
                    .source = state.source,
                    .target = state.target,
                    .edge_type = state.edge_type,
                    .properties = new_props.toOwnedSlice(self.allocator) catch return DatabaseError.OutOfMemory,
                });
                overlay.markDirty();
                return;
            }
        }

        var edge = self.edge_store.getById(edge_id) catch |err| {
            return switch (err) {
                edge_mod.EdgeError.NotFound => DatabaseError.NotFound,
                else => DatabaseError.IoError,
            };
        };
        defer edge.deinit(self.allocator);

        const key_id = self.symbol_table.intern(key) catch return DatabaseError.IoError;

        var new_props: std.ArrayListUnmanaged(node_mod.Property) = .empty;
        defer new_props.deinit(self.allocator);

        var found = false;
        var old_value: ?PropertyValue = null;
        for (edge.properties) |prop| {
            if (prop.key_id == key_id) {
                found = true;
                old_value = prop.value;
            } else {
                new_props.append(self.allocator, prop) catch return DatabaseError.IoError;
            }
        }

        // Property absent: no-op.
        if (!found) return;

        if (!self.edge_store.canFitEdge(edge_id, edge.source, edge.target, edge.edge_type, new_props.items)) {
            return DatabaseError.ValueTooLarge;
        }

        if (txn) |t| {
            if (self.txn_manager) |*tm| {
                const old_value_bytes = try self.serializePropertyValueAlloc(old_value);
                defer if (old_value_bytes) |b| self.allocator.free(b);

                var old_prop_bytes: []u8 = &[_]u8{};
                var owns_old_prop_bytes = false;
                if (edge.properties.len > 0) {
                    old_prop_bytes = wal_payload.serializeProperties(self.allocator, @ptrCast(edge.properties)) catch &[_]u8{};
                    owns_old_prop_bytes = old_prop_bytes.len > 0;
                }
                defer if (owns_old_prop_bytes) self.allocator.free(old_prop_bytes);

                const old_payload_buf = self.allocator.alloc(
                    u8,
                    wal_payload.edgeDeleteSize(old_prop_bytes.len),
                ) catch return DatabaseError.OutOfMemory;
                defer self.allocator.free(old_payload_buf);
                const old_payload = wal_payload.serializeEdgeDelete(
                    old_payload_buf,
                    edge.id,
                    edge.source,
                    edge.target,
                    edge.edge_type,
                    old_prop_bytes,
                ) catch return DatabaseError.IoError;

                const old_len = if (old_value_bytes) |b| b.len else 0;
                const wal_payload_buf = self.allocator.alloc(
                    u8,
                    wal_payload.edgePropertyUpdateSize(key.len, old_len, 0),
                ) catch return DatabaseError.OutOfMemory;
                defer self.allocator.free(wal_payload_buf);
                const wal_payload_bytes = wal_payload.serializeEdgePropertyUpdate(
                    wal_payload_buf,
                    edge.id,
                    key,
                    old_value_bytes,
                    &[_]u8{},
                ) catch return DatabaseError.IoError;

                _ = tm.logOperation(t, .update, wal_payload_bytes) catch return DatabaseError.IoError;
                tm.addUndoEntry(t, .update, .edge, edge.id, 0, 0, old_payload) catch return DatabaseError.IoError;
            }
        }

        self.edge_store.deleteById(edge_id) catch return DatabaseError.IoError;
        self.edge_store.createWithId(edge_id, edge.source, edge.target, edge.edge_type, new_props.items) catch |err| {
            self.edge_store.createWithId(edge_id, edge.source, edge.target, edge.edge_type, edge.properties) catch {};
            return mapEdgeStoreError(err);
        };
        try self.replaceEdgePropertyIndex(
            edge_id,
            edge.edge_type,
            edge.properties,
            edge.edge_type,
            new_props.items,
        );
    }

    /// Check if an edge exists
    pub fn edgeExists(
        self: *Self,
        source: NodeId,
        target: NodeId,
        edge_type: []const u8,
    ) bool {
        const type_id = self.symbol_table.lookup(edge_type) catch return false;
        return self.edge_store.exists(source, target, type_id);
    }

    /// Edge info for traversal results
    pub const EdgeInfo = struct {
        id: EdgeId,
        source: NodeId,
        target: NodeId,
        edge_type: []const u8,
    };

    /// Get all outgoing edges from a node.
    /// Caller owns the returned slice and must free it with freeEdgeInfos.
    pub fn getOutgoingEdges(self: *Self, node_id: NodeId) ![]EdgeInfo {
        var iter = self.edge_store.getOutgoingRefs(node_id) catch {
            return DatabaseError.IoError;
        };
        defer iter.deinit();

        var edges: std.ArrayList(EdgeInfo) = .empty;
        errdefer edges.deinit(self.allocator);

        while (try iter.next()) |edge| {
            const edge_type_str = self.symbol_table.resolve(edge.edge_type) catch {
                continue;
            };
            edges.append(self.allocator, .{
                .id = edge.id,
                .source = edge.source,
                .target = edge.target,
                .edge_type = edge_type_str,
            }) catch {
                self.allocator.free(edge_type_str);
                return DatabaseError.OutOfMemory;
            };
        }

        return edges.toOwnedSlice(self.allocator);
    }

    /// Get all incoming edges to a node.
    /// Caller owns the returned slice and must free it with freeEdgeInfos.
    pub fn getIncomingEdges(self: *Self, node_id: NodeId) ![]EdgeInfo {
        var iter = self.edge_store.getIncomingRefs(node_id) catch {
            return DatabaseError.IoError;
        };
        defer iter.deinit();

        var edges: std.ArrayList(EdgeInfo) = .empty;
        errdefer edges.deinit(self.allocator);

        while (try iter.next()) |edge| {
            const edge_type_str = self.symbol_table.resolve(edge.edge_type) catch {
                continue;
            };
            edges.append(self.allocator, .{
                .id = edge.id,
                .source = edge.source,
                .target = edge.target,
                .edge_type = edge_type_str,
            }) catch {
                self.allocator.free(edge_type_str);
                return DatabaseError.OutOfMemory;
            };
        }

        return edges.toOwnedSlice(self.allocator);
    }

    pub fn getOutgoingEdgesInTxn(self: *Self, txn: ?*Transaction, node_id: NodeId, limit: usize) DatabaseError![]EdgeInfo {
        const refs = try self.collectVisibleEdgeRefs(txn, node_id, .outgoing, null, limit);
        defer self.allocator.free(refs);

        return self.edgeRefsToInfos(refs);
    }

    pub fn getIncomingEdgesInTxn(self: *Self, txn: ?*Transaction, node_id: NodeId, limit: usize) DatabaseError![]EdgeInfo {
        const refs = try self.collectVisibleEdgeRefs(txn, node_id, .incoming, null, limit);
        defer self.allocator.free(refs);

        return self.edgeRefsToInfos(refs);
    }

    pub fn getOutgoingEdgesByTypeInTxn(
        self: *Self,
        txn: ?*Transaction,
        node_id: NodeId,
        edge_type: []const u8,
        limit: usize,
    ) DatabaseError![]EdgeInfo {
        const type_id = (try self.edgeTypeIdOrEmpty(edge_type)) orelse return self.allocator.alloc(EdgeInfo, 0) catch return DatabaseError.OutOfMemory;
        const refs = try self.collectVisibleEdgeRefs(txn, node_id, .outgoing, type_id, limit);
        defer self.allocator.free(refs);

        return self.edgeRefsToInfos(refs);
    }

    pub fn getIncomingEdgesByTypeInTxn(
        self: *Self,
        txn: ?*Transaction,
        node_id: NodeId,
        edge_type: []const u8,
        limit: usize,
    ) DatabaseError![]EdgeInfo {
        const type_id = (try self.edgeTypeIdOrEmpty(edge_type)) orelse return self.allocator.alloc(EdgeInfo, 0) catch return DatabaseError.OutOfMemory;
        const refs = try self.collectVisibleEdgeRefs(txn, node_id, .incoming, type_id, limit);
        defer self.allocator.free(refs);

        return self.edgeRefsToInfos(refs);
    }

    pub fn scanEdgesInTxn(
        self: *Self,
        txn: ?*Transaction,
        edge_type: ?[]const u8,
        limit: usize,
    ) DatabaseError![]EdgeInfo {
        const type_id = if (edge_type) |value|
            (try self.edgeTypeIdOrEmpty(value)) orelse return self.allocator.alloc(EdgeInfo, 0) catch return DatabaseError.OutOfMemory
        else
            null;
        const refs = try self.collectVisibleAllEdgeRefs(txn, type_id, limit);
        defer self.allocator.free(refs);

        return self.edgeRefsToInfos(refs);
    }

    /// Free edge info slice returned by getOutgoingEdges or getIncomingEdges.
    pub fn freeEdgeInfos(self: *Self, edges: []EdgeInfo) void {
        for (edges) |edge| {
            self.allocator.free(edge.edge_type);
        }
        self.allocator.free(edges);
    }

    pub fn getEdgeInTxn(self: *Self, txn: ?*Transaction, edge_id: EdgeId) DatabaseError!?edge_mod.Edge {
        var state = (try self.readVisibleEdgeState(txn, edge_id)) orelse return null;
        defer state.deinit(self.allocator);
        if (!state.exists) return null;

        return .{
            .id = edge_id,
            .source = state.source,
            .target = state.target,
            .edge_type = state.edge_type,
            .properties = cloneProperties(self.allocator, state.properties) catch return DatabaseError.OutOfMemory,
        };
    }

    /// Get a property from an edge by stable edge ID.
    pub fn getEdgePropertyById(
        self: *Self,
        edge_id: EdgeId,
        key: []const u8,
    ) DatabaseError!?PropertyValue {
        var edge = self.edge_store.getById(edge_id) catch |err| {
            return switch (err) {
                edge_mod.EdgeError.NotFound => DatabaseError.NotFound,
                else => DatabaseError.IoError,
            };
        };
        defer edge.deinit(self.allocator);

        const key_id = self.symbol_table.lookup(key) catch |err| {
            return switch (err) {
                symbols_mod.SymbolError.NotFound => null,
                else => DatabaseError.IoError,
            };
        };

        for (edge.properties) |prop| {
            if (prop.key_id == key_id) {
                return prop.value.clone(self.allocator) catch return DatabaseError.OutOfMemory;
            }
        }

        return null;
    }

    pub fn getEdgePropertyByIdInTxn(
        self: *Self,
        txn: ?*Transaction,
        edge_id: EdgeId,
        key: []const u8,
    ) DatabaseError!?PropertyValue {
        var state = (try self.readVisibleEdgeState(txn, edge_id)) orelse return null;
        defer state.deinit(self.allocator);
        if (!state.exists) return null;

        const key_id = self.symbol_table.lookup(key) catch |err| {
            return switch (err) {
                symbols_mod.SymbolError.NotFound => null,
                else => DatabaseError.IoError,
            };
        };

        for (state.properties) |prop| {
            if (prop.key_id == key_id) {
                return prop.value.clone(self.allocator) catch return DatabaseError.OutOfMemory;
            }
        }

        return null;
    }

    /// Get all properties for an edge by stable edge ID.
    /// Returns an allocated slice that the caller must free with freePropertyEntries.
    pub fn getEdgeProperties(self: *Self, edge_id: EdgeId) DatabaseError![]PropertyEntry {
        var edge = self.edge_store.getById(edge_id) catch |err| {
            return switch (err) {
                edge_mod.EdgeError.NotFound => DatabaseError.NotFound,
                else => DatabaseError.IoError,
            };
        };
        defer edge.deinit(self.allocator);

        var props: std.ArrayListUnmanaged(PropertyEntry) = .empty;
        errdefer {
            for (props.items) |prop| {
                self.allocator.free(prop.key);
                var value = prop.value;
                value.deinit(self.allocator);
            }
            props.deinit(self.allocator);
        }

        for (edge.properties) |prop| {
            const key_name = self.symbol_table.resolve(prop.key_id) catch continue;
            errdefer self.allocator.free(key_name);

            const value = prop.value.clone(self.allocator) catch {
                self.allocator.free(key_name);
                return DatabaseError.OutOfMemory;
            };
            errdefer {
                var owned_value = value;
                owned_value.deinit(self.allocator);
            }

            props.append(self.allocator, .{
                .key = key_name,
                .value = value,
            }) catch {
                self.allocator.free(key_name);
                var owned_value = value;
                owned_value.deinit(self.allocator);
                return DatabaseError.OutOfMemory;
            };
        }

        return props.toOwnedSlice(self.allocator) catch return DatabaseError.OutOfMemory;
    }

    pub fn getEdgePropertiesInTxn(self: *Self, txn: ?*Transaction, edge_id: EdgeId) DatabaseError![]PropertyEntry {
        var state = (try self.readVisibleEdgeState(txn, edge_id)) orelse {
            return self.allocator.alloc(PropertyEntry, 0) catch return DatabaseError.OutOfMemory;
        };
        defer state.deinit(self.allocator);
        if (!state.exists) {
            return self.allocator.alloc(PropertyEntry, 0) catch return DatabaseError.OutOfMemory;
        }

        var props: std.ArrayListUnmanaged(PropertyEntry) = .empty;
        errdefer {
            for (props.items) |prop| {
                self.allocator.free(prop.key);
                var value = prop.value;
                value.deinit(self.allocator);
            }
            props.deinit(self.allocator);
        }

        for (state.properties) |prop| {
            const key_name = self.symbol_table.resolve(prop.key_id) catch continue;
            errdefer self.allocator.free(key_name);

            const value = prop.value.clone(self.allocator) catch {
                self.allocator.free(key_name);
                return DatabaseError.OutOfMemory;
            };
            errdefer {
                var owned_value = value;
                owned_value.deinit(self.allocator);
            }

            props.append(self.allocator, .{
                .key = key_name,
                .value = value,
            }) catch {
                self.allocator.free(key_name);
                var owned_value = value;
                owned_value.deinit(self.allocator);
                return DatabaseError.OutOfMemory;
            };
        }

        return props.toOwnedSlice(self.allocator) catch return DatabaseError.OutOfMemory;
    }

    /// Get lightweight iterator for outgoing edges (no property deserialization).
    /// Returns EdgeRef containing (id, source, target, edge_type_id).
    /// Ideal for graph traversal (BFS/DFS) where properties are not needed.
    /// Caller must call iter.deinit() when done.
    pub fn getOutgoingEdgeRefs(self: *Self, node_id: NodeId) !EdgeRefIterator {
        return self.edge_store.getOutgoingRefs(node_id) catch {
            return DatabaseError.IoError;
        };
    }

    pub fn getOutgoingEdgeRefsInTxn(self: *Self, txn: ?*Transaction, node_id: NodeId) DatabaseError![]EdgeRef {
        return self.collectVisibleEdgeRefs(txn, node_id, .outgoing, null, 0);
    }

    pub fn getOutgoingEdgeRefsByTypeInTxn(
        self: *Self,
        txn: ?*Transaction,
        node_id: NodeId,
        edge_type: symbols_mod.SymbolId,
    ) DatabaseError![]EdgeRef {
        return self.collectVisibleEdgeRefs(txn, node_id, .outgoing, edge_type, 0);
    }

    /// Get cached outgoing edges for a node. If the adjacency cache is enabled
    /// and the node is cached, returns the cached slice directly (no B+Tree access).
    /// On cache miss, populates the cache from the B+Tree and returns the result.
    /// Falls back to the uncached iterator path if the cache is disabled.
    pub fn getOutgoingEdgesCached(self: *Self, node_id: NodeId) !?[]const CachedEdge {
        if (self.adjacency_cache) |*cache| {
            if (cache.get(node_id)) |edges| {
                return edges;
            }
            // Cache miss - populate from edge store
            cache.populateNode(&self.edge_store, node_id) catch {
                return null; // Fall back to uncached path
            };
            return cache.get(node_id);
        }
        return null; // Cache not enabled
    }

    /// Pre-warm the adjacency cache for the given node IDs.
    /// Populates cache entries by reading from the edge B+Tree in sorted order
    /// to exploit key locality. No-op if the adjacency cache is disabled.
    pub fn warmAdjacencyCache(self: *Self, node_ids: []const NodeId) void {
        if (self.adjacency_cache) |*cache| {
            cache.populateFrom(&self.edge_store, node_ids) catch {};
        }
    }

    /// Get lightweight iterator for incoming edges (no property deserialization).
    /// Returns EdgeRef containing (id, source, target, edge_type_id).
    /// Caller must call iter.deinit() when done.
    pub fn getIncomingEdgeRefs(self: *Self, node_id: NodeId) !EdgeRefIterator {
        return self.edge_store.getIncomingRefs(node_id) catch {
            return DatabaseError.IoError;
        };
    }

    pub fn getIncomingEdgeRefsInTxn(self: *Self, txn: ?*Transaction, node_id: NodeId) DatabaseError![]EdgeRef {
        return self.collectVisibleEdgeRefs(txn, node_id, .incoming, null, 0);
    }

    pub fn getIncomingEdgeRefsByTypeInTxn(
        self: *Self,
        txn: ?*Transaction,
        node_id: NodeId,
        edge_type: symbols_mod.SymbolId,
    ) DatabaseError![]EdgeRef {
        return self.collectVisibleEdgeRefs(txn, node_id, .incoming, edge_type, 0);
    }

    /// Get outgoing edge refs for multiple nodes in a single B+Tree scan.
    /// Optimized for BFS workloads - exploits B+Tree key ordering.
    /// Returns BatchEdgeResults with edges for all input nodes.
    /// Caller must call result.deinit() when done.
    pub fn getOutgoingEdgeRefsBatch(
        self: *Self,
        node_ids: []const NodeId,
        allocator: Allocator,
    ) DatabaseError!edge_mod.BatchEdgeResults {
        return self.edge_store.getOutgoingRefsBatch(node_ids, allocator) catch {
            return DatabaseError.IoError;
        };
    }

    // ========================================================================
    // Label Operations
    // ========================================================================

    /// Label info with name and count
    pub const LabelInfo = struct {
        name: []const u8,
        count: u64,
    };

    /// Edge type info with name and count
    pub const EdgeTypeInfo = struct {
        name: []const u8,
        count: u64,
    };

    /// Get all labels in the database with their node counts.
    /// Returns an allocated slice that the caller must free with freeLabelInfos.
    pub fn getAllLabels(self: *Self) DatabaseError![]LabelInfo {
        // Iterate the label tree to find unique label_ids
        var label_set = std.AutoHashMap(symbols_mod.SymbolId, u64).init(self.allocator);
        defer label_set.deinit();

        // Iterate all entries in the label tree
        var iter = self.label_tree.range(null, null) catch {
            return DatabaseError.IoError;
        };
        defer iter.deinit();

        while (true) {
            const entry = iter.next() catch {
                break;
            };
            if (entry) |e| {
                // Parse the label key: (label_id: u16, node_id: u64)
                if (e.key.len >= 2) {
                    const label_id = std.mem.readInt(u16, e.key[0..2], .big);
                    // Increment count for this label
                    const gop = label_set.getOrPut(label_id) catch {
                        return DatabaseError.OutOfMemory;
                    };
                    if (gop.found_existing) {
                        gop.value_ptr.* += 1;
                    } else {
                        gop.value_ptr.* = 1;
                    }
                }
            } else {
                break;
            }
        }

        // Convert to array of LabelInfo
        var result: std.ArrayList(LabelInfo) = .empty;
        errdefer {
            for (result.items) |info| {
                self.allocator.free(info.name);
            }
            result.deinit(self.allocator);
        }

        var set_iter = label_set.iterator();
        while (set_iter.next()) |kv| {
            const label_name = self.symbol_table.resolve(kv.key_ptr.*) catch continue;
            result.append(self.allocator, .{
                .name = label_name,
                .count = kv.value_ptr.*,
            }) catch {
                self.allocator.free(label_name);
                return DatabaseError.OutOfMemory;
            };
        }

        return result.toOwnedSlice(self.allocator) catch return DatabaseError.OutOfMemory;
    }

    /// Free label info slice returned by getAllLabels.
    pub fn freeLabelInfos(self: *Self, labels: []LabelInfo) void {
        for (labels) |info| {
            self.allocator.free(info.name);
        }
        self.allocator.free(labels);
    }

    /// Get all labels for a specific node.
    /// Returns an allocated slice that the caller must free.
    pub fn getNodeLabels(self: *Self, node_id: NodeId) DatabaseError![][]const u8 {
        var labels_list: std.ArrayListUnmanaged([]const u8) = .empty;
        errdefer {
            for (labels_list.items) |label| {
                self.allocator.free(label);
            }
            labels_list.deinit(self.allocator);
        }

        // Iterate the label tree looking for entries with this node_id
        var iter = self.label_tree.range(null, null) catch {
            return DatabaseError.IoError;
        };
        defer iter.deinit();

        while (true) {
            const entry = iter.next() catch break;
            if (entry) |e| {
                // Parse label key: (label_id: u16, node_id: u64)
                if (e.key.len >= 10) {
                    const entry_node_id = std.mem.readInt(u64, e.key[2..10], .big);
                    if (entry_node_id == node_id) {
                        const label_id = std.mem.readInt(u16, e.key[0..2], .big);
                        const label_name = self.symbol_table.resolve(label_id) catch continue;
                        labels_list.append(self.allocator, label_name) catch {
                            self.allocator.free(label_name);
                            return DatabaseError.OutOfMemory;
                        };
                    }
                }
            } else {
                break;
            }
        }

        return labels_list.toOwnedSlice(self.allocator) catch return DatabaseError.OutOfMemory;
    }

    pub fn getNodeLabelsInTxn(self: *Self, txn: ?*Transaction, node_id: NodeId) DatabaseError![][]const u8 {
        var labels_list: std.ArrayListUnmanaged([]const u8) = .empty;
        errdefer {
            for (labels_list.items) |label| {
                self.allocator.free(label);
            }
            labels_list.deinit(self.allocator);
        }

        var state = (try self.readVisibleNodeState(txn, node_id)) orelse {
            return labels_list.toOwnedSlice(self.allocator) catch return DatabaseError.OutOfMemory;
        };
        defer state.deinit(self.allocator);
        if (!state.exists) {
            return labels_list.toOwnedSlice(self.allocator) catch return DatabaseError.OutOfMemory;
        }

        for (state.labels) |label_id| {
            const label_name = self.symbol_table.resolve(label_id) catch continue;
            labels_list.append(self.allocator, label_name) catch {
                self.allocator.free(label_name);
                return DatabaseError.OutOfMemory;
            };
        }

        return labels_list.toOwnedSlice(self.allocator) catch return DatabaseError.OutOfMemory;
    }

    /// Property entry for property iteration
    pub const PropertyEntry = struct {
        key: []const u8,
        value: PropertyValue,
    };

    /// Get all properties for a node.
    /// Returns an allocated slice that the caller must free with freePropertyEntries.
    pub fn getNodeProperties(self: *Self, node_id: NodeId) DatabaseError![]PropertyEntry {
        var node = (try self.getNode(node_id)) orelse {
            return self.allocator.alloc(PropertyEntry, 0) catch return DatabaseError.OutOfMemory;
        };
        defer node.deinit(self.allocator);

        var props: std.ArrayListUnmanaged(PropertyEntry) = .empty;
        errdefer {
            for (props.items) |prop| {
                self.allocator.free(prop.key);
                var value = prop.value;
                value.deinit(self.allocator);
            }
            props.deinit(self.allocator);
        }

        for (node.properties) |prop| {
            const key_name = self.symbol_table.resolve(prop.key_id) catch continue;
            errdefer self.allocator.free(key_name);

            const value = prop.value.clone(self.allocator) catch {
                self.allocator.free(key_name);
                return DatabaseError.OutOfMemory;
            };
            errdefer {
                var owned_value = value;
                owned_value.deinit(self.allocator);
            }

            props.append(self.allocator, .{ .key = key_name, .value = value }) catch {
                self.allocator.free(key_name);
                var owned_value = value;
                owned_value.deinit(self.allocator);
                return DatabaseError.OutOfMemory;
            };
        }

        return props.toOwnedSlice(self.allocator) catch return DatabaseError.OutOfMemory;
    }

    pub fn getNodePropertiesInTxn(self: *Self, txn: ?*Transaction, node_id: NodeId) DatabaseError![]PropertyEntry {
        var state = (try self.readVisibleNodeState(txn, node_id)) orelse {
            return self.allocator.alloc(PropertyEntry, 0) catch return DatabaseError.OutOfMemory;
        };
        defer state.deinit(self.allocator);
        if (!state.exists) {
            return self.allocator.alloc(PropertyEntry, 0) catch return DatabaseError.OutOfMemory;
        }

        var props: std.ArrayListUnmanaged(PropertyEntry) = .empty;
        errdefer {
            for (props.items) |prop| {
                self.allocator.free(prop.key);
                var value = prop.value;
                value.deinit(self.allocator);
            }
            props.deinit(self.allocator);
        }

        for (state.properties) |prop| {
            const key_name = self.symbol_table.resolve(prop.key_id) catch continue;
            errdefer self.allocator.free(key_name);

            const value = prop.value.clone(self.allocator) catch {
                self.allocator.free(key_name);
                return DatabaseError.OutOfMemory;
            };
            errdefer {
                var owned_value = value;
                owned_value.deinit(self.allocator);
            }

            props.append(self.allocator, .{ .key = key_name, .value = value }) catch {
                self.allocator.free(key_name);
                var owned_value = value;
                owned_value.deinit(self.allocator);
                return DatabaseError.OutOfMemory;
            };
        }

        return props.toOwnedSlice(self.allocator) catch return DatabaseError.OutOfMemory;
    }

    /// Free property entries returned by getNodeProperties.
    pub fn freePropertyEntries(self: *Self, props: []PropertyEntry) void {
        for (props) |prop| {
            self.allocator.free(prop.key);
            var value = prop.value;
            value.deinit(self.allocator);
        }
        self.allocator.free(props);
    }

    /// Get all edge types in the database with their counts.
    /// Returns an allocated slice that the caller must free with freeEdgeTypeInfos.
    pub fn getAllEdgeTypes(self: *Self) DatabaseError![]EdgeTypeInfo {
        // Iterate the edge tree to find unique type_ids from outgoing edges only
        var type_set = std.AutoHashMap(symbols_mod.SymbolId, u64).init(self.allocator);
        defer type_set.deinit();

        // Iterate all entries in the edge tree
        var iter = self.edge_tree.range(null, null) catch {
            return DatabaseError.IoError;
        };
        defer iter.deinit();

        while (true) {
            const entry = iter.next() catch {
                break;
            };
            if (entry) |e| {
                // Parse edge key: (source_id: u64, direction: u8, type_id: u16, target_id: u64)
                if (e.key.len >= 11) {
                    const direction = e.key[8];
                    // Only count outgoing edges (direction=0) to avoid double counting
                    if (direction == 0) {
                        const type_id = std.mem.readInt(u16, e.key[9..11], .big);
                        // Increment count for this edge type
                        const gop = type_set.getOrPut(type_id) catch {
                            return DatabaseError.OutOfMemory;
                        };
                        if (gop.found_existing) {
                            gop.value_ptr.* += 1;
                        } else {
                            gop.value_ptr.* = 1;
                        }
                    }
                }
            } else {
                break;
            }
        }

        // Convert to array of EdgeTypeInfo
        var result: std.ArrayList(EdgeTypeInfo) = .empty;
        errdefer {
            for (result.items) |info| {
                self.allocator.free(info.name);
            }
            result.deinit(self.allocator);
        }

        var set_iter = type_set.iterator();
        while (set_iter.next()) |kv| {
            const type_name = self.symbol_table.resolve(kv.key_ptr.*) catch continue;
            result.append(self.allocator, .{
                .name = type_name,
                .count = kv.value_ptr.*,
            }) catch {
                self.allocator.free(type_name);
                return DatabaseError.OutOfMemory;
            };
        }

        return result.toOwnedSlice(self.allocator) catch return DatabaseError.OutOfMemory;
    }

    /// Free edge type info slice returned by getAllEdgeTypes.
    pub fn freeEdgeTypeInfos(self: *Self, edge_types: []EdgeTypeInfo) void {
        for (edge_types) |info| {
            self.allocator.free(info.name);
        }
        self.allocator.free(edge_types);
    }

    /// Get all nodes with a specific label
    pub fn getNodesByLabel(self: *Self, label: []const u8) ![]NodeId {
        const label_id = self.symbol_table.lookup(label) catch |err| {
            return switch (err) {
                symbols_mod.SymbolError.NotFound => self.allocator.alloc(NodeId, 0) catch return DatabaseError.OutOfMemory,
                else => DatabaseError.IoError,
            };
        };

        return self.label_index.getNodesByLabel(label_id) catch {
            return DatabaseError.IoError;
        };
    }

    pub fn getNodesByLabelInTxn(self: *Self, txn: ?*Transaction, label: []const u8) DatabaseError![]NodeId {
        const label_id = self.symbol_table.lookup(label) catch |err| {
            return switch (err) {
                symbols_mod.SymbolError.NotFound => self.allocator.alloc(NodeId, 0) catch return DatabaseError.OutOfMemory,
                else => DatabaseError.IoError,
            };
        };

        const visible_ids = try self.collectVisibleNodeIds(txn);
        defer self.allocator.free(visible_ids);

        var matches: std.ArrayListUnmanaged(NodeId) = .empty;
        errdefer matches.deinit(self.allocator);

        for (visible_ids) |node_id| {
            var state = (try self.readVisibleNodeState(txn, node_id)) orelse continue;
            defer state.deinit(self.allocator);
            if (!state.exists) continue;
            if (containsSymbolId(state.labels, label_id)) {
                matches.append(self.allocator, node_id) catch return DatabaseError.OutOfMemory;
            }
        }

        return matches.toOwnedSlice(self.allocator) catch return DatabaseError.OutOfMemory;
    }

    // ========================================================================
    // Query Execution
    // ========================================================================

    /// Execute a Cypher query and return results.
    ///
    /// Example:
    /// ```
    /// const result = try db.query("MATCH (n:Person) RETURN n");
    /// defer result.deinit();
    ///
    /// for (result.rows) |row| {
    ///     for (row.values) |val| {
    ///         // process value
    ///     }
    /// }
    /// ```
    /// Whether running `cypher` could change the database.
    ///
    /// Callers use this to pick a transaction mode. Only one write transaction
    /// may be open at a time, so running every query as a writer serialises
    /// reads that could have run concurrently, while running every query as a
    /// reader makes writes impossible. Answering per query avoids both.
    ///
    /// A query that does not parse is reported as read-only. Execution will
    /// reject it with a parse error either way, and a read transaction is the
    /// weaker thing to have opened.
    pub fn queryWrites(self: *Self, cypher: []const u8) bool {
        var parser = Parser.init(self.allocator, cypher);
        defer parser.deinit();

        const parse_result = parser.parse();
        const parsed = parse_result.query orelse return false;
        return parsed.writesData();
    }

    pub fn query(self: *Self, cypher: []const u8) QueryError!QueryResult {
        var detailed = try self.queryDetailedInTxn(null, cypher);
        switch (detailed) {
            .success => |result| return result,
            .failure => |*failure| {
                const legacy = failure.toLegacyError();
                failure.deinit();
                return legacy;
            },
        }
    }

    /// Execute a Cypher query with bound parameters.
    pub fn queryWithParams(
        self: *Self,
        cypher: []const u8,
        params: *const std.StringHashMap(types.PropertyValue),
    ) QueryError!QueryResult {
        var detailed = try self.queryWithParamsDetailedInTxn(null, cypher, params);
        switch (detailed) {
            .success => |result| return result,
            .failure => |*failure| {
                const legacy = failure.toLegacyError();
                failure.deinit();
                return legacy;
            },
        }
    }

    /// Execute a Cypher query and return either query results or structured failure details.
    pub fn queryDetailed(self: *Self, cypher: []const u8) QueryError!QueryDetailedResult {
        return self.queryDetailedInTxn(null, cypher);
    }

    /// Execute a Cypher query with bound parameters and return structured success/failure output.
    pub fn queryWithParamsDetailed(
        self: *Self,
        cypher: []const u8,
        params: *const std.StringHashMap(types.PropertyValue),
    ) QueryError!QueryDetailedResult {
        return self.queryWithParamsDetailedInTxn(null, cypher, params);
    }

    pub fn queryInTxn(self: *Self, txn: ?*Transaction, cypher: []const u8) QueryError!QueryResult {
        var detailed = try self.queryDetailedInTxn(txn, cypher);
        switch (detailed) {
            .success => |result| return result,
            .failure => |*failure| {
                const legacy = failure.toLegacyError();
                failure.deinit();
                return legacy;
            },
        }
    }

    pub fn queryWithParamsInTxn(
        self: *Self,
        txn: ?*Transaction,
        cypher: []const u8,
        params: *const std.StringHashMap(types.PropertyValue),
    ) QueryError!QueryResult {
        var detailed = try self.queryWithParamsDetailedInTxn(txn, cypher, params);
        switch (detailed) {
            .success => |result| return result,
            .failure => |*failure| {
                const legacy = failure.toLegacyError();
                failure.deinit();
                return legacy;
            },
        }
    }

    pub fn queryDetailedInTxn(
        self: *Self,
        txn: ?*Transaction,
        cypher: []const u8,
    ) QueryError!QueryDetailedResult {
        return self.executeQueryDetailed(txn, cypher, null);
    }

    pub fn queryWithParamsDetailedInTxn(
        self: *Self,
        txn: ?*Transaction,
        cypher: []const u8,
        params: *const std.StringHashMap(types.PropertyValue),
    ) QueryError!QueryDetailedResult {
        return self.executeQueryDetailed(txn, cypher, params);
    }

    /// Clear the query cache (if enabled).
    pub fn clearQueryCache(self: *Self) void {
        if (self.query_cache) |cache| {
            cache.clear();
        }
    }

    /// Get query cache statistics.
    pub fn queryCacheStats(self: *Self) cache_mod.CacheStats {
        if (self.query_cache) |cache| {
            return cache.getStats();
        }
        return .{ .entries = 0, .hits = 0, .misses = 0 };
    }

    /// Internal: Execute a query with optional parameters, using the cache when available.
    fn executeQueryDetailed(
        self: *Self,
        txn: ?*Transaction,
        cypher: []const u8,
        params: ?*const std.StringHashMap(types.PropertyValue),
    ) QueryError!QueryDetailedResult {
        const normalized_cypher = normalizeCypher(cypher);

        var cache_entry: ?*cache_mod.CacheEntry = null;
        defer {
            if (cache_entry) |entry| entry.unpin();
        }
        var uncached_cache_arena: ?std.heap.ArenaAllocator = null;
        defer {
            if (uncached_cache_arena) |arena| arena.deinit();
        }

        var ast_query: *lattice.query.ast.Query = undefined;
        var analysis_vars: []const VariableInfo = &[_]VariableInfo{};

        // Fallback parser/analyzer for cache miss
        var parser: ?Parser = null;
        defer {
            if (parser) |*p| p.deinit();
        }
        var analyzer: ?SemanticAnalyzer = null;
        defer {
            if (analyzer) |*a| a.deinit();
        }

        // Try the cache first
        if (self.query_cache) |cache| {
            cache_entry = cache.get(normalized_cypher);
        }

        if (cache_entry) |entry| {
            // Cache hit: use the cached AST and variables
            ast_query = entry.query;
            analysis_vars = entry.variables;
        } else {
            // Cache miss: parse and analyze
            if (self.query_cache != null) {
                // Parse with a cache-owned arena so the AST can be cached
                var cache_arena = std.heap.ArenaAllocator.init(self.allocator);

                // Copy source into arena so AST slices remain valid
                const owned_source = cache_arena.allocator().dupe(u8, normalized_cypher) catch {
                    cache_arena.deinit();
                    return QueryError.OutOfMemory;
                };

                var cache_parser = Parser.initWithArena(self.allocator, owned_source, cache_arena);
                const parse_result = cache_parser.parse();

                if (parse_result.query == null) {
                    const failure = self.makeParseFailure(parse_result) catch |err| {
                        cache_parser.errors.deinit(self.allocator);
                        cache_parser.arena.deinit();
                        return err;
                    };
                    // Parse failed - clean up and return error
                    cache_parser.errors.deinit(self.allocator);
                    cache_parser.arena.deinit();
                    return failure;
                }
                ast_query = parse_result.query.?;

                // Semantic analysis
                var sem = SemanticAnalyzer.init(self.allocator);
                const analysis = sem.analyze(ast_query);
                if (!analysis.success) {
                    const failure = self.makeSemanticFailure(analysis) catch |err| {
                        sem.deinit();
                        cache_parser.errors.deinit(self.allocator);
                        cache_parser.arena.deinit();
                        return err;
                    };
                    sem.deinit();
                    cache_parser.errors.deinit(self.allocator);
                    cache_parser.arena.deinit();
                    return failure;
                }
                analysis_vars = analysis.variables;

                // Store in cache (cache takes ownership of the arena)
                var arena_to_cache = cache_parser.arena;
                cache_parser.errors.deinit(self.allocator);
                cache_entry = self.query_cache.?.put(normalized_cypher, ast_query, analysis_vars, &arena_to_cache);
                if (cache_entry) |entry| {
                    ast_query = entry.query;
                    analysis_vars = entry.variables;
                } else {
                    // Cache insertion skipped (e.g., all entries pinned). Keep the
                    // parser arena alive for this execution path.
                    uncached_cache_arena = arena_to_cache;
                }

                sem.deinit();
            } else {
                // No cache: standard parse path
                parser = Parser.init(self.allocator, normalized_cypher);
                const parse_result = parser.?.parse();

                if (parse_result.query == null) {
                    return self.makeParseFailure(parse_result);
                }
                ast_query = parse_result.query.?;

                analyzer = SemanticAnalyzer.init(self.allocator);
                const analysis = analyzer.?.analyze(ast_query);
                if (!analysis.success) {
                    return self.makeSemanticFailure(analysis);
                }
                analysis_vars = analysis.variables;
            }
        }

        // Plan the query
        const storage_ctx = StorageContext{
            .node_tree = &self.node_tree,
            .label_index = &self.label_index,
            .edge_store = &self.edge_store,
            .symbol_table = &self.symbol_table,
            .hnsw_index = if (self.hnsw_index) |*hnsw| hnsw else null,
            .fts_index = if (self.fts_index) |*fts| fts else null,
            .database = self,
        };

        // Use an arena for all query execution temporaries (operator structs,
        // expression evaluation results, intermediate PropertyValues).
        // This ensures no leaks from list/map/vector conversions during evaluation.
        var query_arena = std.heap.ArenaAllocator.init(self.allocator);
        defer query_arena.deinit();
        const query_alloc = query_arena.allocator();

        var planner = QueryPlanner.init(query_alloc, storage_ctx);

        const analysis_result = semantic_mod.AnalysisResult{
            .success = true,
            .errors = &[_]semantic_mod.SemanticError{},
            .variables = analysis_vars,
            .errors_dropped = false,
        };

        const root_op = planner.plan(ast_query, &analysis_result) catch |err| {
            return self.makePlanFailure(err, planner.planErrorDetail());
        };

        // A query that changes data and was not given a transaction gets one of
        // its own, committed when it succeeds and abandoned when it does not.
        //
        // Without this the mutation runs straight against the pages, which
        // leaves no write-ahead log record behind. That costs crash atomicity,
        // and it makes the write invisible to anything following the log, which
        // is how backup and replication see changes at all. Almost every write
        // arrives this way, because a bare query is what the command line and
        // every client library send.
        var auto_txn: ?Transaction = null;
        var auto_txn_committed = false;
        if (txn == null and ast_query.writesData() and !self.read_only) {
            auto_txn = self.beginTransaction(.read_write) catch |err| switch (err) {
                // A database built without transactions still has the old
                // behaviour available, and refusing the query outright would be
                // worse than running it the way it has always run.
                DatabaseError.TransactionsNotEnabled => null,
                DatabaseError.OutOfMemory => return QueryError.OutOfMemory,
                else => return QueryError.ExecutionError,
            };
        }
        defer {
            if (auto_txn) |*t| {
                if (!auto_txn_committed) self.abortTransaction(t) catch {};
            }
        }

        // Execute
        var exec_ctx = ExecutionContext.initWithStorage(self.allocator, &self.node_store, &self.symbol_table);
        exec_ctx.database = self;
        exec_ctx.txn = if (auto_txn) |*t| t else txn;
        defer exec_ctx.deinit();

        var binding_iter = planner.bindings.iterator();
        while (binding_iter.next()) |entry| {
            exec_ctx.registerVariable(entry.key_ptr.*, entry.value_ptr.slot) catch {
                return QueryError.OutOfMemory;
            };
        }

        // Set bound parameters if provided
        if (params) |p| {
            var param_iter = p.iterator();
            while (param_iter.next()) |entry| {
                exec_ctx.setParameter(entry.key_ptr.*, entry.value_ptr.*) catch {
                    return QueryError.OutOfMemory;
                };
            }
        }

        var exec_result = execute(query_alloc, root_op, &exec_ctx) catch |err| {
            return self.makeExecutionFailure(err);
        };

        // Built before the commit, because the rows are read back through the
        // transaction's own view of the data.
        const converted = try self.convertResult(&exec_result, &planner);

        if (auto_txn) |*t| {
            self.commitTransaction(t) catch return QueryError.ExecutionError;
            auto_txn_committed = true;
        }

        return .{ .success = converted };
    }

    fn normalizeCypher(cypher: []const u8) []const u8 {
        var trimmed = std.mem.trim(u8, cypher, " \t\r\n");
        while (trimmed.len > 0 and trimmed[trimmed.len - 1] == ';') {
            trimmed = std.mem.trimEnd(u8, trimmed[0 .. trimmed.len - 1], " \t\r\n");
        }
        return trimmed;
    }

    fn makeParseFailure(self: *Self, parse_result: parser_mod.ParserResult) QueryError!QueryDetailedResult {
        if (parse_result.errors.len > 0) {
            const first = parse_result.errors[0];
            return self.makeFailure(
                .parse,
                first.message,
                null,
                .{
                    .line = first.line,
                    .column = first.column,
                    .length = first.length,
                },
            );
        }
        if (parse_result.errors_dropped) {
            return self.makeFailure(
                .parse,
                "Parse failed (errors dropped due to memory pressure)",
                null,
                null,
            );
        }
        return self.makeFailure(.parse, "Parse error: invalid Cypher syntax", null, null);
    }

    fn makeSemanticFailure(self: *Self, analysis: semantic_mod.AnalysisResult) QueryError!QueryDetailedResult {
        if (analysis.errors.len > 0) {
            const first = analysis.errors[0];
            return self.makeFailure(
                .semantic,
                first.message,
                @tagName(first.code),
                .{
                    .line = first.location.line,
                    .column = first.location.column,
                    .length = first.location.length,
                },
            );
        }
        if (analysis.errors_dropped) {
            return self.makeFailure(
                .semantic,
                "Semantic analysis failed (errors dropped due to memory pressure)",
                null,
                null,
            );
        }
        return self.makeFailure(.semantic, "Semantic error: invalid query structure", null, null);
    }

    fn makePlanFailure(self: *Self, err: anyerror, detail: ?[]const u8) QueryError!QueryDetailedResult {
        // A planner that knows why it failed says so. "Could not create execution
        // plan" is true of every plan failure and useful for none of them.
        return self.makeFailure(
            .plan,
            detail orelse "Plan error: could not create execution plan",
            @errorName(err),
            null,
        );
    }

    fn makeExecutionFailure(self: *Self, err: anyerror) QueryError!QueryDetailedResult {
        return self.makeFailure(.execution, "Execution error: query failed", @errorName(err), null);
    }

    fn makeFailure(
        self: *Self,
        stage: QueryFailureStage,
        message: []const u8,
        code: ?[]const u8,
        location: ?QueryFailureLocation,
    ) QueryError!QueryDetailedResult {
        const owned_message = self.allocator.dupe(u8, message) catch return QueryError.OutOfMemory;
        errdefer self.allocator.free(owned_message);

        var owned_code: ?[]const u8 = null;
        if (code) |c| {
            owned_code = self.allocator.dupe(u8, c) catch return QueryError.OutOfMemory;
        }

        return .{
            .failure = .{
                .allocator = self.allocator,
                .stage = stage,
                .message = owned_message,
                .code = owned_code,
                .location = location,
            },
        };
    }

    /// Convert executor result to database-friendly result format
    fn convertResult(
        self: *Self,
        exec_result: *executor_mod.QueryResult,
        planner: *QueryPlanner,
    ) QueryError!QueryResult {
        // Build column names from planner output columns (set by RETURN clause planning)
        // Use output_columns when available (from RETURN), otherwise fall back to next_slot
        const num_cols = if (planner.output_columns > 0) planner.output_columns else planner.next_slot;
        var columns = self.allocator.alloc([]const u8, num_cols) catch {
            return QueryError.OutOfMemory;
        };
        errdefer self.allocator.free(columns);

        // Initialize columns with slot numbers as names (default)
        for (0..num_cols) |i| {
            columns[i] = std.fmt.allocPrint(self.allocator, "col{}", .{i}) catch {
                return QueryError.OutOfMemory;
            };
        }

        // Try to get actual variable names
        var binding_iter = planner.bindings.iterator();
        while (binding_iter.next()) |entry| {
            const slot = entry.value_ptr.slot;
            if (slot < num_cols and planner.output_column_names[slot] == null) {
                // Free default name and use actual variable name
                self.allocator.free(columns[slot]);
                columns[slot] = self.allocator.dupe(u8, entry.key_ptr.*) catch {
                    return QueryError.OutOfMemory;
                };
            }
        }

        // Explicit aliases from RETURN take precedence over inferred binding names.
        for (0..num_cols) |i| {
            if (planner.output_column_names[i]) |name| {
                self.allocator.free(columns[i]);
                columns[i] = self.allocator.dupe(u8, name) catch {
                    return QueryError.OutOfMemory;
                };
            }
        }

        // Convert rows
        var rows = self.allocator.alloc(ResultRow, exec_result.rows.items.len) catch {
            return QueryError.OutOfMemory;
        };
        errdefer {
            for (rows) |*row| {
                row.deinit(self.allocator);
            }
            self.allocator.free(rows);
        }

        for (exec_result.rows.items, 0..) |exec_row, row_idx| {
            var values = self.allocator.alloc(ResultValue, num_cols) catch {
                return QueryError.OutOfMemory;
            };

            for (0..num_cols) |col_idx| {
                const slot: u8 = @intCast(col_idx);
                if (exec_row.getSlot(slot)) |slot_value| {
                    values[col_idx] = try self.slotToResultValue(slot_value);
                } else {
                    values[col_idx] = .{ .null_val = {} };
                }
            }

            rows[row_idx] = ResultRow{ .values = values };
        }

        return QueryResult{
            .columns = columns,
            .rows = rows,
            .allocator = self.allocator,
        };
    }

    /// Convert a slot value to a result value
    fn slotToResultValue(self: *Self, slot: executor_mod.SlotValue) QueryError!ResultValue {
        return switch (slot) {
            .empty => .{ .null_val = {} },
            .node_ref => |id| .{ .node_id = id },
            .edge_ref => |id| .{ .edge_id = id },
            .property => |prop| try self.propertyToResultValue(prop),
        };
    }

    /// Convert a PropertyValue to a ResultValue (recursive for lists/maps).
    /// Clones all slice data so the result is independent of the source lifetime.
    fn propertyToResultValue(self: *Self, prop: PropertyValue) QueryError!ResultValue {
        return switch (prop) {
            .null_val => .{ .null_val = {} },
            .bool_val => |b| .{ .bool_val = b },
            .int_val => |i| .{ .int_val = i },
            .float_val => |f| .{ .float_val = f },
            .string_val => |s| .{ .string_val = self.allocator.dupe(u8, s) catch return QueryError.OutOfMemory },
            .bytes_val => |b| .{ .bytes_val = self.allocator.dupe(u8, b) catch return QueryError.OutOfMemory },
            .vector_val => |v| .{ .vector_val = self.allocator.dupe(f32, v) catch return QueryError.OutOfMemory },
            .list_val => |list| blk: {
                const result_list = self.allocator.alloc(ResultValue, list.len) catch {
                    return QueryError.OutOfMemory;
                };
                for (list, 0..) |item, i| {
                    result_list[i] = try self.propertyToResultValue(item);
                }
                break :blk .{ .list_val = result_list };
            },
            .map_val => |map| blk: {
                const result_map = self.allocator.alloc(ResultValue.MapEntry, map.len) catch {
                    return QueryError.OutOfMemory;
                };
                for (map, 0..) |entry, i| {
                    result_map[i] = .{
                        .key = self.allocator.dupe(u8, entry.key) catch return QueryError.OutOfMemory,
                        .value = try self.propertyToResultValue(entry.value),
                    };
                }
                break :blk .{ .map_val = result_map };
            },
        };
    }
};

// ============================================================================
// Tests
// ============================================================================

test "database open and close" {
    const allocator = std.testing.allocator;

    // Use a temp file
    const path = "/tmp/lattice_test_db.ltdb";

    // Create new database
    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{
            .enable_wal = false, // Disable WAL for simpler testing
            .enable_fts = false,
        },
    });

    try std.testing.expectEqual(@as(u64, 0), db.nodeCount());
    try std.testing.expectEqual(@as(u64, 0), db.edgeCount());
    try std.testing.expect(!db.isReadOnly());

    db.close();

    // Reopen existing database
    var db2 = try Database.open(allocator, path, .{
        .create = false,
        .config = .{
            .enable_wal = false,
            .enable_fts = false,
        },
    });

    // Verify tree roots were persisted
    const header = db2.page_manager.getHeader();
    try std.testing.expect(header.hasInitializedTrees());

    db2.close();

    // Cleanup
    @import("compat").fs.cwd().deleteFile(path) catch {};
}

test "database compact truncates free tail and rejects active transactions" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_database_compact_test.ltdb";
    const wal_path = "/tmp/lattice_database_compact_test.ltdb-wal";
    @import("compat").fs.cwd().deleteFile(path) catch {};
    @import("compat").fs.cwd().deleteFile(wal_path) catch {};
    defer {
        @import("compat").fs.cwd().deleteFile(path) catch {};
        @import("compat").fs.cwd().deleteFile(wal_path) catch {};
    }

    const db = try Database.open(allocator, path, .{ .create = true });
    defer db.close();

    const node_id = try db.createNode(null, &.{"Person"});
    const tail_page = try db.page_manager.allocatePage();
    try db.page_manager.freePage(tail_page);
    const size_before = try db.page_manager.file.size();

    var reader = try db.beginTransaction(.read_only);
    try std.testing.expectError(DatabaseError.TransactionConflict, db.compact());
    try db.abortTransaction(&reader);

    const stats = try db.compact();
    try std.testing.expect(stats.pages_removed >= 1);
    try std.testing.expect(stats.bytes_reclaimed >= db.page_manager.getPageSize());
    try std.testing.expect((try db.page_manager.file.size()) < size_before);
    try std.testing.expect(try db.nodeExists(node_id));
}

test "database file not found" {
    const allocator = std.testing.allocator;
    const result = Database.open(allocator, "/nonexistent/path/db.ltdb", .{});
    try std.testing.expectError(DatabaseError.FileNotFound, result);
}

test "graph crud operations" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_crud_test.ltdb";

    // Create database
    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{
            .enable_wal = false,
            .enable_fts = false,
        },
    });
    defer {
        db.close();
        @import("compat").fs.cwd().deleteFile(path) catch {};
    }

    // Create nodes
    const alice = try db.createNode(null, &[_][]const u8{"Person"});
    const bob = try db.createNode(null, &[_][]const u8{"Person"});

    try std.testing.expect(try db.nodeExists(alice));
    try std.testing.expect(try db.nodeExists(bob));

    // Create edge
    try db.createEdge(null, alice, bob, "KNOWS");
    try std.testing.expect(db.edgeExists(alice, bob, "KNOWS"));
    try std.testing.expect(!db.edgeExists(bob, alice, "KNOWS")); // directed

    // Get nodes by label
    const people = try db.getNodesByLabel("Person");
    defer allocator.free(people);
    try std.testing.expectEqual(@as(usize, 2), people.len);

    // Delete edge
    try db.deleteEdge(null, alice, bob, "KNOWS");
    try std.testing.expect(!db.edgeExists(alice, bob, "KNOWS"));

    // Test setNodeProperty
    try db.setNodeProperty(null, alice, "name", .{ .string_val = "Alice" });
    var name_val = (try db.getNodeProperty(alice, "name")).?;
    defer name_val.deinit(allocator);
    try std.testing.expectEqualStrings("Alice", name_val.string_val);

    // Delete node
    try db.deleteNode(null, alice);
    try std.testing.expect(!(try db.nodeExists(alice)));
    try std.testing.expect(try db.nodeExists(bob));
}

test "setNodeProperty round-trips string values above the old 512-byte WAL buffer" {
    // Regression: `setNodeProperty` used to serialize the new/old values
    // into fixed `[512]u8` stack buffers and the outer WAL payload into a
    // `[2048]u8` buffer, which capped STRING/BYTES properties at ~507
    // bytes and composite payloads at ~2 KiB. Both fixed buffers are
    // now heap-allocated from `wal_payload.*Size`, so values round-trip
    // through the node store and WAL undo path up to the btree page-size
    // ceiling (a separate limitation tracked in `btree.zig`).
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_large_prop_test.ltdb";
    @import("compat").fs.cwd().deleteFile(path) catch {};
    @import("compat").fs.cwd().deleteFile(path ++ "-wal") catch {};

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{
            .enable_wal = true,
            .enable_fts = false,
        },
    });
    defer {
        db.close();
        @import("compat").fs.cwd().deleteFile(path) catch {};
        @import("compat").fs.cwd().deleteFile(path ++ "-wal") catch {};
    }

    const node_id = try db.createNode(null, &[_][]const u8{"Doc"});

    // Walk past the old 512 B per-property cap and the 2 KiB composite
    // WAL-payload cap. 3000 B is comfortably below the 4 KiB btree page
    // size so entries still fit in a single leaf slot.
    const sizes = [_]usize{ 64, 512, 600, 1024, 2048, 3000 };
    var buf: [3000]u8 = undefined;
    for (sizes, 0..) |sz, i| {
        // Distinct byte pattern per offset so ordering regressions fail loud.
        for (buf[0..sz], 0..) |*b, j| b.* = @truncate(j +% (i * 17));
        try db.setNodeProperty(null, node_id, "content", .{ .string_val = buf[0..sz] });

        var got = (try db.getNodeProperty(node_id, "content")).?;
        defer got.deinit(allocator);
        try std.testing.expectEqualSlices(u8, buf[0..sz], got.string_val);
    }
}

test "edge properties above 4KB round-trip with 4KB pages" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_large_edge_prop_test.ltdb";
    @import("compat").fs.cwd().deleteFile(path) catch {};
    @import("compat").fs.cwd().deleteFile(path ++ "-wal") catch {};
    defer {
        @import("compat").fs.cwd().deleteFile(path) catch {};
        @import("compat").fs.cwd().deleteFile(path ++ "-wal") catch {};
    }

    const payload = try allocator.alloc(u8, 8192);
    defer allocator.free(payload);
    for (payload, 0..) |*b, i| b.* = 'a' + @as(u8, @intCast(i % 26));

    var direct_edge_id: EdgeId = undefined;
    var txn_edge_id: EdgeId = undefined;

    {
        var db = try Database.open(allocator, path, .{
            .create = true,
            .page_size = 4096,
            .config = .{
                .enable_wal = true,
                .enable_fts = false,
            },
        });
        defer db.close();

        try std.testing.expectEqual(@as(u32, 4096), db.page_manager.getPageSize());

        const direct_source = try db.createNode(null, &[_][]const u8{"Entity"});
        const direct_target = try db.createNode(null, &[_][]const u8{"Entity"});
        direct_edge_id = try db.createEdgeAndGetId(null, direct_source, direct_target, "RESOLVES_TO");
        try db.setEdgePropertyById(null, direct_edge_id, "propertiesJson", .{ .string_val = payload });

        {
            var got = (try db.getEdgePropertyById(direct_edge_id, "propertiesJson")).?;
            defer got.deinit(allocator);
            switch (got) {
                .string_val => |s| try std.testing.expectEqualSlices(u8, payload, s),
                else => return error.TestUnexpectedResult,
            }
        }

        var txn = try db.beginTransaction(.read_write);
        var committed = false;
        errdefer if (!committed) db.abortTransaction(&txn) catch {};

        const txn_source = try db.createNode(&txn, &[_][]const u8{"Entity"});
        const txn_target = try db.createNode(&txn, &[_][]const u8{"Entity"});
        txn_edge_id = try db.createEdgeAndGetId(&txn, txn_source, txn_target, "RESOLVES_TO");
        try db.setEdgePropertyById(&txn, txn_edge_id, "propertiesJson", .{ .string_val = payload });
        try db.commitTransaction(&txn);
        committed = true;

        {
            var got = (try db.getEdgePropertyById(txn_edge_id, "propertiesJson")).?;
            defer got.deinit(allocator);
            switch (got) {
                .string_val => |s| try std.testing.expectEqualSlices(u8, payload, s),
                else => return error.TestUnexpectedResult,
            }
        }

        try db.sync();
    }

    {
        var db = try Database.open(allocator, path, .{
            .create = false,
            .config = .{
                .enable_wal = true,
                .enable_fts = false,
            },
        });
        defer db.close();

        try std.testing.expectEqual(@as(u32, 4096), db.page_manager.getPageSize());

        for ([_]EdgeId{ direct_edge_id, txn_edge_id }) |edge_id| {
            var got = (try db.getEdgePropertyById(edge_id, "propertiesJson")).?;
            defer got.deinit(allocator);
            switch (got) {
                .string_val => |s| try std.testing.expectEqualSlices(u8, payload, s),
                else => return error.TestUnexpectedResult,
            }
        }
    }
}

test "sustained transactional writes preserve data with large pages" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_sustained_writes_large_page_test.ltdb";
    @import("compat").fs.cwd().deleteFile(path) catch {};
    @import("compat").fs.cwd().deleteFile(path ++ "-wal") catch {};
    defer {
        @import("compat").fs.cwd().deleteFile(path) catch {};
        @import("compat").fs.cwd().deleteFile(path ++ "-wal") catch {};
    }

    const iterations = 128;
    var node_ids: [iterations]NodeId = undefined;
    var edge_ids: [iterations - 1]EdgeId = undefined;

    {
        var db = try Database.open(allocator, path, .{
            .create = true,
            .page_size = 32768,
            .config = .{
                .enable_wal = true,
                .enable_fts = false,
            },
        });
        defer db.close();

        try std.testing.expectEqual(@as(u32, 32768), db.page_manager.getPageSize());

        for (0..iterations) |i| {
            var txn = try db.beginTransaction(.read_write);
            var committed = false;
            errdefer if (!committed) db.abortTransaction(&txn) catch {};

            const label = if (i % 2 == 0) "Entry" else "Event";
            const labels = [_][]const u8{label};
            const node_id = try db.createNode(&txn, &labels);
            node_ids[i] = node_id;

            var payload_buf: [192]u8 = undefined;
            const payload = try std.fmt.bufPrint(
                &payload_buf,
                "node-{d}-payload-{d}",
                .{ i, i * i },
            );
            try db.setNodeProperty(&txn, node_id, "payload", .{ .string_val = payload });
            try db.setNodeProperty(&txn, node_id, "ordinal", .{ .int_val = @intCast(i) });

            if (i > 0) {
                const edge_id = try db.createEdgeAndGetId(&txn, node_ids[i - 1], node_id, "NEXT");
                edge_ids[i - 1] = edge_id;
                try db.setEdgePropertyById(&txn, edge_id, "weight", .{ .int_val = @intCast(i) });
            }

            try db.publishStream(&txn, "events", "write", .{ .int_val = @intCast(i) });
            try db.setStreamOffset(&txn, "events", "consumer", @intCast(i + 1));

            try db.commitTransaction(&txn);
            committed = true;
        }

        try db.sync();
        try std.testing.expectEqual(@as(u64, iterations), db.nodeCount());
        try std.testing.expectEqual(@as(u64, iterations - 1), db.edgeCount());
    }

    {
        var db = try Database.open(allocator, path, .{
            .create = false,
            .config = .{
                .enable_wal = true,
                .enable_fts = false,
            },
        });
        defer db.close();

        try std.testing.expectEqual(@as(u32, 32768), db.page_manager.getPageSize());
        try std.testing.expectEqual(@as(u64, iterations), db.nodeCount());
        try std.testing.expectEqual(@as(u64, iterations - 1), db.edgeCount());

        const entry_nodes = try db.getNodesByLabel("Entry");
        defer allocator.free(entry_nodes);
        try std.testing.expectEqual(@as(usize, (iterations + 1) / 2), entry_nodes.len);

        const check_indices = [_]usize{ 0, 1, 17, 63, 127 };
        for (check_indices) |idx| {
            try std.testing.expect(try db.nodeExists(node_ids[idx]));

            var ordinal = (try db.getNodeProperty(node_ids[idx], "ordinal")).?;
            defer ordinal.deinit(allocator);
            switch (ordinal) {
                .int_val => |value| try std.testing.expectEqual(@as(i64, @intCast(idx)), value),
                else => return error.TestUnexpectedResult,
            }

            if (idx > 0) {
                try std.testing.expect(db.edgeExists(node_ids[idx - 1], node_ids[idx], "NEXT"));
                var weight = (try db.getEdgePropertyById(edge_ids[idx - 1], "weight")).?;
                defer weight.deinit(allocator);
                switch (weight) {
                    .int_val => |value| try std.testing.expectEqual(@as(i64, @intCast(idx)), value),
                    else => return error.TestUnexpectedResult,
                }
            }
        }

        var batch = try db.readStream("events", 0, iterations, 0);
        defer batch.deinit();
        try std.testing.expectEqual(@as(usize, iterations), batch.records.len);
        for (batch.records, 0..) |record, i| {
            try std.testing.expectEqual(@as(u64, i + 1), record.sequence);
            try std.testing.expectEqualStrings("write", record.kind);
            switch (record.payload) {
                .int_val => |value| try std.testing.expectEqual(@as(i64, @intCast(i)), value),
                else => return error.TestUnexpectedResult,
            }
        }

        const offset = try db.getStreamOffset("events", "consumer");
        try std.testing.expectEqual(@as(?u64, iterations), offset);
    }
}

test "getNodesByLabel returns every node with the requested label for reopen indexing" {
    // Regression: nullclaw's LatticeMemory adapter needs to rebuild its
    // in-memory key→node index on database reopen so short-lived CLI
    // invocations don't silently create duplicate Entry nodes. It walks
    // every node labelled "Entry" (plus "Category", "Session", etc.)
    // via getNodesByLabel, so the API has to return all matches even
    // when they were created in earlier process lifetimes.
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_nodes_by_label_reopen_test.ltdb";
    @import("compat").fs.cwd().deleteFile(path) catch {};
    @import("compat").fs.cwd().deleteFile(path ++ "-wal") catch {};

    {
        var db = try Database.open(allocator, path, .{
            .create = true,
            .config = .{ .enable_wal = true, .enable_fts = false },
        });
        defer {
            db.close();
        }

        const ids = [_]NodeId{
            try db.createNode(null, &[_][]const u8{"Entry"}),
            try db.createNode(null, &[_][]const u8{"Entry"}),
            try db.createNode(null, &[_][]const u8{"Entry"}),
        };
        _ = ids;
        // Create an unrelated Category node so the label filter has to
        // actually discriminate, not just return every node in the store.
        _ = try db.createNode(null, &[_][]const u8{"Category"});
    }

    // Reopen the database in a fresh process-equivalent scope. If
    // getNodesByLabel only surfaced nodes from the *current* process
    // the count would be 0 and this test would fail.
    var db = try Database.open(allocator, path, .{
        .create = false,
        .config = .{ .enable_wal = true, .enable_fts = false },
    });
    defer {
        db.close();
        @import("compat").fs.cwd().deleteFile(path) catch {};
        @import("compat").fs.cwd().deleteFile(path ++ "-wal") catch {};
    }

    const entries = try db.getNodesByLabel("Entry");
    defer allocator.free(entries);
    try std.testing.expectEqual(@as(usize, 3), entries.len);

    const cats = try db.getNodesByLabel("Category");
    defer allocator.free(cats);
    try std.testing.expectEqual(@as(usize, 1), cats.len);

    // Unknown labels surface as an empty slice, not an error.
    const missing = try db.getNodesByLabel("Nonexistent");
    defer allocator.free(missing);
    try std.testing.expectEqual(@as(usize, 0), missing.len);
}

test "indexing multi-KiB markdown-shaped documents" {
    // Regression: `nullclaw onboard --memory latticedb` tripped a
    // `panic: integer overflow` inside the indexing path when it
    // reached AGENTS.md (9242 bytes). Reproduce the exact shape of the
    // failing flow — Database open with fts enabled, a sequence of
    // stores ranging from ~1 KiB to ~9 KiB of realistic markdown
    // tokens — and assert each call returns cleanly. The text arrives
    // as a property now rather than through a separate indexing call,
    // which is the only part of the flow that changed.
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_fts_db_large_test.ltdb";
    @import("compat").fs.cwd().deleteFile(path) catch {};
    @import("compat").fs.cwd().deleteFile(path ++ "-wal") catch {};

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{
            .enable_wal = true,
            .enable_fts = true,
        },
    });
    defer {
        db.close();
        @import("compat").fs.cwd().deleteFile(path) catch {};
        @import("compat").fs.cwd().deleteFile(path ++ "-wal") catch {};
    }

    try db.createNodeFtsIndex("Entry", "body");

    // Scaffold a family of varied documents that together exercise the
    // per-call and cumulative FTS code paths. Vocabulary grows with
    // the doc index so posting lists keep extending.
    const sizes = [_]usize{ 1674, 9242, 861, 2112, 637, 478, 169, 1591 };

    var buf: [32768]u8 = undefined;
    var rng = std.Random.DefaultPrng.init(0x1abe1);
    const r = rng.random();

    for (sizes, 0..) |target, idx| {
        var pos: usize = 0;
        while (pos < target) {
            // Emit pseudo-markdown tokens: short punctuation-free words,
            // the occasional '#' heading marker, newlines. Vocab keeps
            // shifting with idx so later docs keep extending dictionary.
            var word_buf: [32]u8 = undefined;
            const wlen: usize = 3 + (r.int(usize) % 10);
            var j: usize = 0;
            while (j < wlen and pos < target) : (j += 1) {
                word_buf[j] = 'a' + @as(u8, @intCast((idx + j + r.int(usize)) % 26));
            }
            const remaining = target - pos;
            const take = @min(j, remaining);
            @memcpy(buf[pos..][0..take], word_buf[0..take]);
            pos += take;
            if (pos < target) {
                buf[pos] = ' ';
                pos += 1;
            }
        }

        const node_id = try db.createNode(null, &[_][]const u8{"Entry"});
        try db.setNodeProperty(null, node_id, "body", .{ .string_val = buf[0..target] });
    }
}

test "transactional commit across graph fts and vector indexes leaves no pinned pages" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_commit_pin_cleanup_test.ltdb";
    @import("compat").fs.cwd().deleteFile(path) catch {};
    @import("compat").fs.cwd().deleteFile(path ++ "-wal") catch {};

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{
            .buffer_pool_size = 4 * 1024 * 1024,
            .enable_wal = true,
            .enable_fts = true,
            .enable_vector = true,
            .vector_dimensions = 4,
        },
    });
    defer {
        db.close();
        @import("compat").fs.cwd().deleteFile(path) catch {};
        @import("compat").fs.cwd().deleteFile(path ++ "-wal") catch {};
    }

    try db.createNodeFtsIndex("Entry", "body");

    var txn = try db.beginTransaction(.read_write);
    var committed = false;
    errdefer if (!committed) db.abortTransaction(&txn) catch {};

    const labels = [_][]const u8{"Entry"};
    for (0..96) |i| {
        const node_id = try db.createNode(&txn, &labels);

        var title_buf: [64]u8 = undefined;
        const title = try std.fmt.bufPrint(&title_buf, "memory record {d:04}", .{i});
        try db.setNodeProperty(&txn, node_id, "title", .{ .string_val = title });
        try db.setNodeProperty(&txn, node_id, "ordinal", .{ .int_val = @intCast(i) });

        var text_buf: [256]u8 = undefined;
        const text = try std.fmt.bufPrint(
            &text_buf,
            "lattice commit pin cleanup graph fts vector document {d:04} token_{d:04}",
            .{ i, i },
        );
        try db.setNodeProperty(&txn, node_id, "body", .{ .string_val = text });

        const base: f32 = @floatFromInt(i);
        const vector = [_]f32{ base, base + 0.25, base + 0.5, base + 0.75 };
        try db.setNodeVectorInTxn(&txn, node_id, &vector);
    }

    try db.commitTransaction(&txn);
    committed = true;

    try std.testing.expectEqual(@as(usize, 0), db.buffer_pool.getStats().pinned_frames);
}

test "setNodeProperty round-trips values larger than one btree leaf page" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_oversized_value_test.ltdb";

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{
            .enable_wal = false,
            .enable_fts = false,
        },
    });
    defer {
        db.close();
        @import("compat").fs.cwd().deleteFile(path) catch {};
    }

    const node_id = try db.createNode(null, &[_][]const u8{"Doc"});

    var big_buf: [16384]u8 = undefined;
    @memset(&big_buf, 'x');
    try db.setNodeProperty(null, node_id, "blob", .{ .string_val = &big_buf });

    var got = (try db.getNodeProperty(node_id, "blob")).?;
    defer got.deinit(allocator);
    try std.testing.expectEqualSlices(u8, &big_buf, got.string_val);
}

test "transactional oversized node property commits without pinned frames" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_txn_oversized_node_value_test.ltdb";

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{
            .buffer_pool_size = 4 * 1024 * 1024,
            .enable_wal = true,
            .enable_fts = false,
        },
    });
    defer {
        db.close();
        @import("compat").fs.cwd().deleteFile(path) catch {};
        @import("compat").fs.cwd().deleteFile(path ++ "-wal") catch {};
    }

    var txn = try db.beginTransaction(.read_write);
    var committed = false;
    errdefer if (!committed) db.abortTransaction(&txn) catch {};

    const node_id = try db.createNode(&txn, &[_][]const u8{"Doc"});
    try db.setNodeProperty(&txn, node_id, "qid", .{ .string_val = "lme-q-000001" });

    var large_json: [8192]u8 = undefined;
    @memset(&large_json, 'x');
    try db.setNodeProperty(&txn, node_id, "properties_json", .{ .string_val = &large_json });

    try std.testing.expectEqual(@as(usize, 0), db.buffer_pool.getStats().pinned_frames);

    try db.commitTransaction(&txn);
    committed = true;
    try std.testing.expectEqual(@as(usize, 0), db.buffer_pool.getStats().pinned_frames);

    var got = (try db.getNodeProperty(node_id, "qid")).?;
    defer got.deinit(allocator);
    try std.testing.expectEqualStrings("lme-q-000001", got.string_val);
    var got_json = (try db.getNodeProperty(node_id, "properties_json")).?;
    defer got_json.deinit(allocator);
    try std.testing.expectEqualSlices(u8, &large_json, got_json.string_val);
}

test "default 4KiB pages round-trip 100KB and 1MB string properties" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_default_page_large_string_values_test.ltdb";
    @import("compat").fs.cwd().deleteFile(path) catch {};
    @import("compat").fs.cwd().deleteFile(path ++ "-wal") catch {};

    var db = try Database.open(allocator, path, .{
        .create = true,
        .page_size = 4096,
        .config = .{
            .enable_wal = true,
            .enable_fts = false,
        },
    });
    defer {
        db.close();
        @import("compat").fs.cwd().deleteFile(path) catch {};
        @import("compat").fs.cwd().deleteFile(path ++ "-wal") catch {};
    }

    try std.testing.expectEqual(@as(u32, 4096), db.page_manager.getPageSize());

    const node_id = try db.createNode(null, &[_][]const u8{"Doc"});
    try db.setNodeProperty(null, node_id, "status", .{ .string_val = "small-ok" });

    const cases = [_]struct {
        name: []const u8,
        len: usize,
    }{
        .{ .name = "blob_100kb", .len = 100 * 1024 },
        .{ .name = "blob_1mb", .len = 1024 * 1024 },
    };

    for (cases) |case| {
        const payload = try allocator.alloc(u8, case.len);
        defer allocator.free(payload);
        @memset(payload, 'x');

        try db.setNodeProperty(null, node_id, case.name, .{ .string_val = payload });

        var got_status = (try db.getNodeProperty(node_id, "status")).?;
        defer got_status.deinit(allocator);
        try std.testing.expectEqualStrings("small-ok", got_status.string_val);
        var got_payload = (try db.getNodeProperty(node_id, case.name)).?;
        defer got_payload.deinit(allocator);
        try std.testing.expectEqualSlices(u8, payload, got_payload.string_val);
        try std.testing.expectEqual(@as(usize, 0), db.buffer_pool.getStats().pinned_frames);
    }
}

test "default 4KiB transactional writes round-trip 100KB and 1MB string properties without pinned frames" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_default_page_txn_large_string_values_test.ltdb";
    @import("compat").fs.cwd().deleteFile(path) catch {};
    @import("compat").fs.cwd().deleteFile(path ++ "-wal") catch {};

    var db = try Database.open(allocator, path, .{
        .create = true,
        .page_size = 4096,
        .config = .{
            .buffer_pool_size = 4 * 1024 * 1024,
            .enable_wal = true,
            .enable_fts = false,
        },
    });
    defer {
        db.close();
        @import("compat").fs.cwd().deleteFile(path) catch {};
        @import("compat").fs.cwd().deleteFile(path ++ "-wal") catch {};
    }

    try std.testing.expectEqual(@as(u32, 4096), db.page_manager.getPageSize());

    const cases = [_]struct {
        qid: []const u8,
        len: usize,
    }{
        .{ .qid = "lme-q-100kb", .len = 100 * 1024 },
        .{ .qid = "lme-q-1mb", .len = 1024 * 1024 },
    };

    for (cases) |case| {
        const payload = try allocator.alloc(u8, case.len);
        defer allocator.free(payload);
        @memset(payload, 'y');

        var txn = try db.beginTransaction(.read_write);
        var committed = false;
        errdefer if (!committed) db.abortTransaction(&txn) catch {};

        const node_id = try db.createNode(&txn, &[_][]const u8{"Doc"});
        try db.setNodeProperty(&txn, node_id, "qid", .{ .string_val = case.qid });
        try db.setNodeProperty(&txn, node_id, "properties_json", .{ .string_val = payload });
        try std.testing.expectEqual(@as(usize, 0), db.buffer_pool.getStats().pinned_frames);

        try db.commitTransaction(&txn);
        committed = true;
        try std.testing.expectEqual(@as(usize, 0), db.buffer_pool.getStats().pinned_frames);

        var got_qid = (try db.getNodeProperty(node_id, "qid")).?;
        defer got_qid.deinit(allocator);
        try std.testing.expectEqualStrings(case.qid, got_qid.string_val);
        var got_payload = (try db.getNodeProperty(node_id, "properties_json")).?;
        defer got_payload.deinit(allocator);
        try std.testing.expectEqualSlices(u8, payload, got_payload.string_val);
    }
}

test "transactional large property updates do not double old and new payloads" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_txn_large_property_update_no_double_payload_test.ltdb";
    @import("compat").fs.cwd().deleteFile(path) catch {};
    @import("compat").fs.cwd().deleteFile(path ++ "-wal") catch {};

    var db = try Database.open(allocator, path, .{
        .create = true,
        .page_size = 4096,
        .config = .{
            .buffer_pool_size = 16 * 1024 * 1024,
            .enable_wal = true,
            .enable_fts = false,
        },
    });
    defer {
        db.close();
        @import("compat").fs.cwd().deleteFile(path) catch {};
        @import("compat").fs.cwd().deleteFile(path ++ "-wal") catch {};
    }

    const node_id = try db.createNode(null, &[_][]const u8{"Doc"});
    const large_len = 33 * 1024 * 1024;

    {
        const initial = try allocator.alloc(u8, large_len);
        defer allocator.free(initial);
        @memset(initial, 'a');
        try db.setNodeProperty(null, node_id, "properties_json", .{ .string_val = initial });
    }

    {
        const replacement = try allocator.alloc(u8, large_len);
        defer allocator.free(replacement);
        @memset(replacement, 'b');

        var txn = try db.beginTransaction(.read_write);
        var committed = false;
        errdefer if (!committed) db.abortTransaction(&txn) catch {};

        try db.setNodeProperty(&txn, node_id, "properties_json", .{ .string_val = replacement });
        try db.commitTransaction(&txn);
        committed = true;
    }

    {
        var txn = try db.beginTransaction(.read_write);
        var committed = false;
        errdefer if (!committed) db.abortTransaction(&txn) catch {};

        try db.setNodeProperty(&txn, node_id, "qid", .{ .string_val = "resolved-0001" });
        try db.commitTransaction(&txn);
        committed = true;
    }

    var got_qid = (try db.getNodeProperty(node_id, "qid")).?;
    defer got_qid.deinit(allocator);
    try std.testing.expectEqualStrings("resolved-0001", got_qid.string_val);

    var got_json = (try db.getNodeProperty(node_id, "properties_json")).?;
    defer got_json.deinit(allocator);
    try std.testing.expectEqual(@as(usize, large_len), got_json.string_val.len);
    try std.testing.expectEqual(@as(u8, 'b'), got_json.string_val[0]);
    try std.testing.expectEqual(@as(u8, 'b'), got_json.string_val[large_len - 1]);
}

test "default 4KiB high-write nodes with qid and large properties_json leave no pinned frames" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_default_page_high_write_large_properties_test.ltdb";
    @import("compat").fs.cwd().deleteFile(path) catch {};
    @import("compat").fs.cwd().deleteFile(path ++ "-wal") catch {};

    var db = try Database.open(allocator, path, .{
        .create = true,
        .page_size = 4096,
        .config = .{
            .buffer_pool_size = 8 * 1024 * 1024,
            .enable_wal = true,
            .enable_fts = false,
        },
    });
    defer {
        db.close();
        @import("compat").fs.cwd().deleteFile(path) catch {};
        @import("compat").fs.cwd().deleteFile(path ++ "-wal") catch {};
    }

    const payload = try allocator.alloc(u8, 64 * 1024);
    defer allocator.free(payload);
    @memset(payload, 'p');

    var txn = try db.beginTransaction(.read_write);
    var committed = false;
    errdefer if (!committed) db.abortTransaction(&txn) catch {};

    const count = 32;
    var ids: [count]NodeId = undefined;
    var qid_buf: [32]u8 = undefined;
    for (0..count) |i| {
        const node_id = try db.createNode(&txn, &[_][]const u8{"Doc"});
        ids[i] = node_id;
        const qid = try std.fmt.bufPrint(&qid_buf, "lme-q-{d:06}", .{i});
        try db.setNodeProperty(&txn, node_id, "qid", .{ .string_val = qid });
        try db.setNodeProperty(&txn, node_id, "properties_json", .{ .string_val = payload });
    }

    try std.testing.expectEqual(@as(usize, 0), db.buffer_pool.getStats().pinned_frames);
    try db.commitTransaction(&txn);
    committed = true;
    try std.testing.expectEqual(@as(usize, 0), db.buffer_pool.getStats().pinned_frames);

    var got_qid = (try db.getNodeProperty(ids[0], "qid")).?;
    defer got_qid.deinit(allocator);
    try std.testing.expectEqualStrings("lme-q-000000", got_qid.string_val);

    var got_payload = (try db.getNodeProperty(ids[count - 1], "properties_json")).?;
    defer got_payload.deinit(allocator);
    try std.testing.expectEqualSlices(u8, payload, got_payload.string_val);
    try std.testing.expectEqual(@as(usize, 0), db.buffer_pool.getStats().pinned_frames);
}

test "query: simple MATCH RETURN" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_query_test.ltdb";

    // Create database
    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{
            .enable_wal = false,
            .enable_fts = false,
        },
    });
    defer {
        db.close();
        @import("compat").fs.cwd().deleteFile(path) catch {};
    }

    // Create some test nodes
    const alice = try db.createNode(null, &[_][]const u8{"Person"});
    const bob = try db.createNode(null, &[_][]const u8{"Person"});
    _ = try db.createNode(null, &[_][]const u8{"Company"});

    // Query for Person nodes
    var result = try db.query("MATCH (n:Person) RETURN n");
    defer result.deinit();

    // Should find 2 Person nodes
    try std.testing.expectEqual(@as(usize, 2), result.rowCount());
    try std.testing.expectEqual(@as(usize, 1), result.columnCount());

    // Verify we got the right node IDs
    var found_alice = false;
    var found_bob = false;
    for (result.rows) |row| {
        if (row.values[0] == .node_id) {
            if (row.values[0].node_id == alice) found_alice = true;
            if (row.values[0].node_id == bob) found_bob = true;
        }
    }
    try std.testing.expect(found_alice);
    try std.testing.expect(found_bob);
}

test "query: parse error" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_query_error_test.ltdb";

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{
            .enable_wal = false,
            .enable_fts = false,
        },
    });
    defer {
        db.close();
        @import("compat").fs.cwd().deleteFile(path) catch {};
    }

    // Invalid query syntax
    const result = db.query("MATCH (n RETURN n");
    try std.testing.expectError(QueryError.ParseError, result);
}

test "query: semantic error" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_query_semantic_test.ltdb";

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{
            .enable_wal = false,
            .enable_fts = false,
        },
    });
    defer {
        db.close();
        @import("compat").fs.cwd().deleteFile(path) catch {};
    }

    // Reference undefined variable
    const result = db.query("MATCH (n) RETURN m");
    try std.testing.expectError(QueryError.SemanticError, result);
}

test "edge traversal" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_edge_traversal_test.ltdb";

    // Create database
    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{
            .enable_wal = false,
            .enable_fts = false,
        },
    });
    defer {
        db.close();
        @import("compat").fs.cwd().deleteFile(path) catch {};
    }

    // Create nodes
    const alice = try db.createNode(null, &[_][]const u8{"Person"});
    const bob = try db.createNode(null, &[_][]const u8{"Person"});
    const charlie = try db.createNode(null, &[_][]const u8{"Person"});

    // Create edges: alice -[KNOWS]-> bob, alice -[LIKES]-> charlie
    try db.createEdge(null, alice, bob, "KNOWS");
    try db.createEdge(null, alice, charlie, "LIKES");

    // Test outgoing edges from alice
    const outgoing = try db.getOutgoingEdges(alice);
    defer db.freeEdgeInfos(outgoing);
    try std.testing.expectEqual(@as(usize, 2), outgoing.len);

    // Test incoming edges to charlie
    const incoming = try db.getIncomingEdges(charlie);
    defer db.freeEdgeInfos(incoming);
    try std.testing.expectEqual(@as(usize, 1), incoming.len);
    try std.testing.expectEqual(alice, incoming[0].source);
    try std.testing.expectEqual(charlie, incoming[0].target);
}

test "introspection: getAllLabels and getAllEdgeTypes" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_introspection_test.ltdb";

    // Create database
    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{
            .enable_wal = false,
            .enable_fts = false,
        },
    });
    defer {
        db.close();
        @import("compat").fs.cwd().deleteFile(path) catch {};
    }

    // Initially no labels or edge types
    const initial_labels = try db.getAllLabels();
    defer db.freeLabelInfos(initial_labels);
    try std.testing.expectEqual(@as(usize, 0), initial_labels.len);

    const initial_types = try db.getAllEdgeTypes();
    defer db.freeEdgeTypeInfos(initial_types);
    try std.testing.expectEqual(@as(usize, 0), initial_types.len);

    // Create nodes with labels
    const alice = try db.createNode(null, &[_][]const u8{"Person"});
    const bob = try db.createNode(null, &[_][]const u8{"Person"});
    const acme = try db.createNode(null, &[_][]const u8{"Company"});

    // Create edges
    try db.createEdge(null, alice, bob, "KNOWS");
    try db.createEdge(null, alice, acme, "WORKS_AT");
    try db.createEdge(null, bob, acme, "WORKS_AT");

    // Check labels
    const labels = try db.getAllLabels();
    defer db.freeLabelInfos(labels);
    try std.testing.expectEqual(@as(usize, 2), labels.len);

    // Check individual labels (order may vary)
    var person_count: u64 = 0;
    var company_count: u64 = 0;
    for (labels) |info| {
        if (std.mem.eql(u8, info.name, "Person")) {
            person_count = info.count;
        } else if (std.mem.eql(u8, info.name, "Company")) {
            company_count = info.count;
        }
    }
    try std.testing.expectEqual(@as(u64, 2), person_count);
    try std.testing.expectEqual(@as(u64, 1), company_count);

    // Check edge types
    const edge_types = try db.getAllEdgeTypes();
    defer db.freeEdgeTypeInfos(edge_types);
    try std.testing.expectEqual(@as(usize, 2), edge_types.len);

    // Check individual edge types (order may vary)
    var knows_count: u64 = 0;
    var works_at_count: u64 = 0;
    for (edge_types) |info| {
        if (std.mem.eql(u8, info.name, "KNOWS")) {
            knows_count = info.count;
        } else if (std.mem.eql(u8, info.name, "WORKS_AT")) {
            works_at_count = info.count;
        }
    }
    try std.testing.expectEqual(@as(u64, 1), knows_count);
    try std.testing.expectEqual(@as(u64, 2), works_at_count);
}

test "vector rebuild keeps latest records and reclaims stale entries" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_vector_rebuild_latest_test.ltdb";
    @import("compat").fs.cwd().deleteFile(path) catch {};
    defer @import("compat").fs.cwd().deleteFile(path) catch {};

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{
            .enable_wal = false,
            .enable_fts = false,
            .enable_vector = true,
            .vector_dimensions = 4,
        },
    });
    defer db.close();

    const node_id = try db.createNode(null, &[_][]const u8{"Embedding"});
    const old_vector = [_]f32{ 1.0, 0.0, 0.0, 0.0 };
    const latest_vector = [_]f32{ 0.0, 0.0, 1.0, 0.0 };
    const orphan_vector = [_]f32{ 0.0, 1.0, 0.0, 0.0 };

    const vs = &(db.vector_storage.?);
    const old_location = try vs.store(node_id, &old_vector);
    const latest_location = try vs.store(node_id, &latest_vector);
    const orphan_location = try vs.store(999_999, &orphan_vector);

    const hnsw = &(db.hnsw_index.?);
    try db.rebuildHnswIndex(vs, hnsw);

    try std.testing.expectEqual(@as(u64, 1), hnsw.vector_count);
    try std.testing.expectEqual(latest_location, hnsw.getNode(node_id).?.vector_loc);
    try std.testing.expectEqual(@as(u64, 1), try vs.count());
    try std.testing.expectError(VectorStorageError.NotFound, vs.getVectorId(old_location));
    try std.testing.expectError(VectorStorageError.NotFound, vs.getVectorId(orphan_location));

    const stored = try vs.getByLocation(latest_location);
    defer vs.free(stored);
    try std.testing.expectEqualSlices(f32, &latest_vector, stored);
}
