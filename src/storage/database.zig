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

const recovery_mod = lattice.storage.recovery;

// Graph layer
const symbols_mod = lattice.graph.symbols;
const SymbolTable = symbols_mod.SymbolTable;

const node_mod = lattice.graph.node;
const NodeStore = node_mod.NodeStore;

const edge_mod = lattice.graph.edge;
const EdgeStore = edge_mod.EdgeStore;
pub const EdgeRef = edge_mod.EdgeRef;
pub const EdgeRefIterator = edge_mod.EdgeStore.EdgeRefIterator;

const label_index_mod = lattice.graph.label_index;
const LabelIndex = label_index_mod.LabelIndex;

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
pub const FtsSearchResult = scorer_mod.ScoredDoc;

// Vector storage and HNSW
const vector_storage_mod = lattice.vector.storage;
const VectorStorage = vector_storage_mod.VectorStorage;
const VectorStorageError = vector_storage_mod.VectorStorageError;

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
    TransactionNotActive,
    TransactionReadOnly,
    TransactionsNotEnabled,
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

    /// Compute effective buffer pool size based on enabled features.
    /// Priority: LATTICE_BUFFER_POOL_MB env var > explicit buffer_pool_size > auto-sizing.
    /// Auto-sizing:
    /// - Base: 16MB for graph operations (covers ~50K edges comfortably)
    /// - FTS enabled: +12MB (dictionary B+Tree, posting lists)
    /// - Vector enabled: +12MB (HNSW connections, vector pages)
    pub fn effectiveBufferPoolSize(self: DatabaseConfig) usize {
        // Environment variable override (useful for tuning without recompilation)
        if (std.posix.getenv("LATTICE_BUFFER_POOL_MB")) |mb_str| {
            if (std.fmt.parseInt(usize, mb_str, 10)) |mb| {
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
    /// Database configuration
    config: DatabaseConfig = .{},
};

/// Central database coordinator
pub const Database = struct {
    allocator: Allocator,

    // Storage layer (owned, must deinit in reverse order)
    vfs: PosixVfs,
    page_manager: PageManager,
    buffer_pool: BufferPool,
    wal: ?WalManager,

    // B+Trees (8 trees for different stores)
    node_tree: BTree,
    edge_tree: BTree,
    label_tree: BTree,
    symbol_forward_tree: BTree,
    symbol_reverse_tree: BTree,
    fts_dict_tree: BTree,
    fts_lengths_tree: BTree,
    fts_reverse_tree: ?BTree,
    hnsw_node_tree: ?BTree,

    // Graph stores
    symbol_table: SymbolTable,
    node_store: NodeStore,
    edge_store: EdgeStore,
    label_index: LabelIndex,

    // Indexes
    fts_index: ?FtsIndex,
    vector_storage: ?VectorStorage,
    hnsw_index: ?HnswIndex,

    // Transactions
    txn_manager: ?TxnManager,

    // Caches
    adjacency_cache: ?AdjacencyCache,
    query_cache: ?*QueryCache,

    // Configuration
    config: DatabaseConfig,
    path: []const u8,
    read_only: bool,

    const Self = @This();

    /// Open or create a database
    pub fn open(allocator: Allocator, path: []const u8, options: OpenOptions) DatabaseError!*Self {
        var self = allocator.create(Self) catch return DatabaseError.OutOfMemory;
        errdefer allocator.destroy(self);

        self.allocator = allocator;
        self.config = options.config;
        self.read_only = options.read_only;

        // Copy path for later use
        self.path = allocator.dupe(u8, path) catch return DatabaseError.OutOfMemory;
        errdefer allocator.free(self.path);

        // 1. Initialize VFS
        self.vfs = PosixVfs.init(allocator);

        // 2. Open/create PageManager
        self.page_manager = PageManager.init(allocator, self.vfs.vfs(), path, .{
            .create = options.create,
            .read_only = options.read_only,
        }) catch |err| {
            return switch (err) {
                PageManagerError.FileNotFound => DatabaseError.FileNotFound,
                PageManagerError.PermissionDenied => DatabaseError.PermissionDenied,
                PageManagerError.InvalidMagic, PageManagerError.InvalidHeader => DatabaseError.InvalidDatabase,
                else => DatabaseError.IoError,
            };
        };
        errdefer self.page_manager.deinit();

        // 3. Initialize BufferPool (auto-scales based on enabled features)
        const effective_pool_size = options.config.effectiveBufferPoolSize();
        self.buffer_pool = BufferPool.init(allocator, &self.page_manager, effective_pool_size) catch {
            return DatabaseError.BufferPoolFull;
        };
        errdefer self.buffer_pool.deinit();

        // 4. Initialize WAL (optional)
        self.wal = null;
        if (options.config.enable_wal and !options.read_only) {
            const wal_path = std.fmt.allocPrint(allocator, "{s}-wal", .{path}) catch {
                return DatabaseError.OutOfMemory;
            };
            defer allocator.free(wal_path);

            self.wal = WalManager.init(
                allocator,
                self.vfs.vfs(),
                wal_path,
                self.page_manager.getHeader().file_uuid,
            ) catch blk: {
                // WAL init failure is non-fatal, continue without WAL
                break :blk null;
            };
        }

        // Initialize these early since saveTreeRoots() checks them
        self.vector_storage = null;
        self.hnsw_index = null;
        self.hnsw_node_tree = null;

        // 5. Initialize or load B+Trees
        const header = self.page_manager.getHeader();
        const is_new = !header.hasInitializedTrees();

        if (is_new) {
            try self.initNewTrees();
        } else {
            try self.loadExistingTrees();
        }

        // 6. Initialize Symbol Table
        self.symbol_table = SymbolTable.init(allocator, &self.symbol_forward_tree, &self.symbol_reverse_tree);

        // 7. Initialize Graph Stores
        self.node_store = NodeStore.init(allocator, &self.node_tree);
        self.edge_store = EdgeStore.init(allocator, &self.edge_tree);
        self.label_index = LabelIndex.init(allocator, &self.label_tree);

        // 7b. Run WAL recovery if needed
        if (self.wal) |*wal| {
            var rm = recovery_mod.RecoveryManager.initWithLogicalContext(allocator, .{
                .node_store = &self.node_store,
                .edge_store = &self.edge_store,
                .symbol_table = &self.symbol_table,
            });
            const stats = rm.recover(wal, &self.page_manager) catch |err| switch (err) {
                error.MidLogCorruption => return DatabaseError.InvalidDatabase,
                else => return DatabaseError.IoError,
            };
            // Persist recovered state if any redo operations were applied
            if (stats.redo_operations > 0) {
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
                ) catch null;
            } else {
                // Create new vector storage
                self.vector_storage = VectorStorage.init(
                    allocator,
                    &self.buffer_pool,
                    options.config.vector_dimensions,
                ) catch null;
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
                            } else |_| {}
                        }
                        if (!loaded) {
                            self.rebuildHnswIndex(vs, hnsw) catch {
                                // Log error but continue - index will be empty
                            };
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

    /// Sync all pending writes to disk.
    /// Call this before close() if you need durability guarantees.
    /// Returns an error if flushing fails - data may not be persisted.
    pub fn sync(self: *Self) DatabaseError!void {
        if (self.read_only) return;

        // Persist HNSW index to B+Tree before saving roots
        if (self.hnsw_index) |*hnsw| {
            if (hnsw.vector_count > 0) {
                if (self.hnsw_node_tree == null) {
                    self.hnsw_node_tree = BTree.init(self.allocator, &self.buffer_pool) catch null;
                }
                if (self.hnsw_node_tree) |*tree| {
                    hnsw.saveToTree(tree) catch {};
                }
            }
        }

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

        // 9. Transaction manager
        if (self.txn_manager) |*tm| {
            tm.deinit();
        }

        // 8c. HNSW index
        if (self.hnsw_index) |*hnsw| {
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

        // 1. VFS (no explicit deinit needed)

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
            return tm.begin(mode, .snapshot) catch |err| {
                return switch (err) {
                    TxnError.TooManyTransactions => DatabaseError.OutOfMemory,
                    else => DatabaseError.IoError,
                };
            };
        }
        return DatabaseError.TransactionsNotEnabled;
    }

    /// Commit a transaction, making all changes durable
    pub fn commitTransaction(self: *Self, txn: *Transaction) DatabaseError!void {
        if (self.txn_manager) |*tm| {
            tm.commit(txn) catch |err| {
                return switch (err) {
                    TxnError.NotActive => DatabaseError.TransactionNotActive,
                    TxnError.NotFound => DatabaseError.NotFound,
                    else => DatabaseError.IoError,
                };
            };
            return;
        }
        return DatabaseError.TransactionsNotEnabled;
    }

    /// Abort a transaction, rolling back all changes
    pub fn abortTransaction(self: *Self, txn: *Transaction) DatabaseError!void {
        if (self.txn_manager) |*tm| {
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

            tm.abort(txn) catch |err| {
                return switch (err) {
                    TxnError.NotActive => DatabaseError.TransactionNotActive,
                    TxnError.NotFound => DatabaseError.NotFound,
                    else => DatabaseError.IoError,
                };
            };
            return;
        }
        return DatabaseError.TransactionsNotEnabled;
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
                                properties = @constCast(@ptrCast(wal_props));
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
                // Undo of insert is delete
                .insert => {
                    self.edge_store.delete(entry.entity_id, entry.secondary_id, entry.type_id) catch {};
                },
                // Undo of delete is re-insert with original properties
                .delete => {
                    var properties: []node_mod.Property = &[_]node_mod.Property{};
                    var owns_properties = false;

                    if (entry.prev_data) |data| {
                        const payload = wal_payload.deserializeEdgeDelete(data) catch {
                            // Fallback: create edge without properties
                            self.edge_store.create(entry.entity_id, entry.secondary_id, entry.type_id, &[_]node_mod.Property{}) catch {};
                            return;
                        };
                        if (payload.properties.len > 0) {
                            const wal_props = wal_payload.deserializeProperties(self.allocator, payload.properties) catch &[_]wal_payload.Property{};
                            if (wal_props.len > 0) {
                                properties = @constCast(@ptrCast(wal_props));
                                owns_properties = true;
                            }
                        }
                    }
                    defer if (owns_properties) {
                        for (properties) |*prop| {
                            var val = prop.value;
                            val.deinit(self.allocator);
                        }
                        self.allocator.free(properties);
                    };

                    self.edge_store.create(entry.entity_id, entry.secondary_id, entry.type_id, properties) catch {};
                },
                .update => {},
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
    }

    fn saveTreeRoots(self: *Self) DatabaseError!void {
        var header = self.page_manager.getHeader().*;

        header.setTreeRoot(.node, self.node_tree.getRootPage());
        header.setTreeRoot(.edge, self.edge_tree.getRootPage());
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

        // Save vector storage first page for persistence
        if (self.vector_storage) |vs| {
            header.vector_segment_page = vs.first_page;
        }

        self.page_manager.updateHeader(&header) catch {
            return DatabaseError.IoError;
        };
    }

    /// Rebuild the HNSW index from persisted vectors
    fn rebuildHnswIndex(self: *Self, vs: *VectorStorage, hnsw: *HnswIndex) !void {
        // Get all stored vector entries
        const entries = vs.getAllEntries(self.allocator) catch return;
        defer if (entries.len > 0) self.allocator.free(entries);

        // Track unique IDs to avoid duplicate insertions
        var seen = std.AutoHashMap(u64, void).init(self.allocator);
        defer seen.deinit();

        // Insert each vector into the HNSW index
        for (entries) |entry| {
            // Skip duplicates
            if (seen.contains(entry.id)) continue;
            seen.put(entry.id, {}) catch continue;

            // Get the vector data from storage
            const vector = vs.getByLocation(entry.location) catch continue;
            defer vs.free(vector);

            // Insert into HNSW index
            hnsw.insert(entry.id, vector) catch continue;
        }
    }

    // ========================================================================
    // Statistics
    // ========================================================================

    /// Get the number of nodes in the database
    pub fn nodeCount(self: *const Self) u64 {
        return self.page_manager.getHeader().node_count;
    }

    /// Get the number of edges in the database
    pub fn edgeCount(self: *const Self) u64 {
        return self.page_manager.getHeader().edge_count;
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

    /// Create a new node with the given labels
    /// Returns the new node's ID
    /// If txn is null, the operation is auto-committed
    pub fn createNode(self: *Self, txn: ?*Transaction, labels: []const []const u8) !NodeId {
        if (self.read_only) return DatabaseError.PermissionDenied;

        // Validate transaction if provided
        if (txn) |t| {
            if (!t.isActive()) return DatabaseError.TransactionNotActive;
            if (t.mode == .read_only) return DatabaseError.TransactionReadOnly;
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
        const node_id = self.node_store.create(label_ids, &[_]node_mod.Property{}) catch {
            return DatabaseError.IoError;
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

    /// Delete a node
    /// If txn is null, the operation is auto-committed
    pub fn deleteNode(self: *Self, txn: ?*Transaction, node_id: NodeId) !void {
        if (self.read_only) return DatabaseError.PermissionDenied;

        // Validate transaction if provided
        if (txn) |t| {
            if (!t.isActive()) return DatabaseError.TransactionNotActive;
            if (t.mode == .read_only) return DatabaseError.TransactionReadOnly;
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

                    var buf: [4096]u8 = undefined;
                    // Serialize node data for WAL and undo (now with properties)
                    const payload = wal_payload.serializeNodeDelete(&buf, node_id, node.labels, prop_bytes) catch {
                        return DatabaseError.IoError;
                    };
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
        }

        // Get the existing node
        var existing_node = self.node_store.get(node_id) catch |err| {
            return switch (err) {
                node_mod.NodeError.NotFound => DatabaseError.NotFound,
                else => DatabaseError.IoError,
            };
        };
        defer existing_node.deinit(self.allocator);

        // Intern the property key
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
        self.node_store.update(node_id, existing_node.labels, new_props.items) catch {
            return DatabaseError.IoError;
        };

        // Log to WAL and add undo entry if transaction is provided
        if (txn) |t| {
            if (self.txn_manager) |*tm| {
                // Serialize old value for undo (if it existed)
                var old_value_bytes: ?[]u8 = null;
                var owns_old_bytes = false;
                if (old_value) |ov| {
                    var old_buf: [512]u8 = undefined;
                    const old_size = wal_payload.serializePropertyValueToBuf(&old_buf, ov) catch null;
                    if (old_size) |sz| {
                        old_value_bytes = self.allocator.dupe(u8, old_buf[0..sz]) catch null;
                        owns_old_bytes = old_value_bytes != null;
                    }
                }
                defer if (owns_old_bytes) self.allocator.free(old_value_bytes.?);

                // Serialize new value
                var new_buf: [512]u8 = undefined;
                const new_size = wal_payload.serializePropertyValueToBuf(&new_buf, value) catch {
                    return DatabaseError.IoError;
                };

                var buf: [2048]u8 = undefined;
                // Serialize property update for WAL (now with old value)
                const payload = wal_payload.serializePropertyUpdate(&buf, node_id, key, old_value_bytes, new_buf[0..new_size]) catch {
                    return DatabaseError.IoError;
                };
                _ = tm.logOperation(t, .update, payload) catch {
                    return DatabaseError.IoError;
                };
                // Undo of update: store payload containing old value
                // For new properties (found=false), undo is delete; for existing, restore old value
                const undo_op: UndoOpType = if (found) .update else .insert;
                tm.addUndoEntry(t, undo_op, .property, node_id, 0, key_id, payload) catch {
                    return DatabaseError.IoError;
                };
            }
        }
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
        self.node_store.update(node_id, existing_node.labels, new_props.items) catch {
            return DatabaseError.IoError;
        };

        // Log to WAL and add undo entry if transaction is provided
        if (txn) |t| {
            if (self.txn_manager) |*tm| {
                // Serialize old value for undo
                var old_value_bytes: ?[]u8 = null;
                var owns_old_bytes = false;
                if (old_value) |ov| {
                    var old_buf: [512]u8 = undefined;
                    const old_size = wal_payload.serializePropertyValueToBuf(&old_buf, ov) catch null;
                    if (old_size) |sz| {
                        old_value_bytes = self.allocator.dupe(u8, old_buf[0..sz]) catch null;
                        owns_old_bytes = old_value_bytes != null;
                    }
                }
                defer if (owns_old_bytes) self.allocator.free(old_value_bytes.?);

                var buf: [2048]u8 = undefined;
                // Serialize property delete for WAL (old value, no new value)
                const payload = wal_payload.serializePropertyUpdate(&buf, node_id, key, old_value_bytes, &[_]u8{}) catch {
                    return DatabaseError.IoError;
                };
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

        // Log undo entry for transaction rollback
        if (txn) |t| {
            if (self.txn_manager) |*tm| {
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

        // Log undo entry for transaction rollback
        if (txn) |t| {
            if (self.txn_manager) |*tm| {
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
    }

    /// Get a property from a node.
    /// Note: The caller owns the returned PropertyValue and must call deinit on it
    /// for string/bytes values to avoid memory leaks.
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

        // Intern the property key
        const key_id = self.symbol_table.intern(key) catch {
            return DatabaseError.IoError;
        };

        // Find the property and clone it
        for (existing_node.properties) |prop| {
            if (prop.key_id == key_id) {
                // Clone the value to avoid returning a pointer to freed memory
                return switch (prop.value) {
                    .string_val => |s| .{ .string_val = self.allocator.dupe(u8, s) catch return DatabaseError.OutOfMemory },
                    .bytes_val => |b| .{ .bytes_val = self.allocator.dupe(u8, b) catch return DatabaseError.OutOfMemory },
                    else => prop.value, // Primitives are copied by value
                };
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
        const vs = &(self.vector_storage orelse return DatabaseError.IoError);

        // Verify node exists
        if (!(try self.nodeExists(node_id))) {
            return DatabaseError.NotFound;
        }

        // Store the vector (using node_id as vector_id)
        _ = vs.store(node_id, vector) catch |err| {
            return switch (err) {
                VectorStorageError.DimensionMismatch => DatabaseError.IoError,
                else => DatabaseError.IoError,
            };
        };

        // Index the vector in HNSW for similarity search
        if (self.hnsw_index) |*hnsw| {
            hnsw.insert(node_id, vector) catch |err| {
                return switch (err) {
                    HnswError.DimensionMismatch => DatabaseError.IoError,
                    HnswError.OutOfMemory => DatabaseError.OutOfMemory,
                    else => DatabaseError.IoError,
                };
            };
        }
    }

    /// Search for similar vectors using HNSW index.
    /// Returns node IDs and distances sorted by similarity (closest first).
    pub fn vectorSearch(
        self: *Self,
        query_vector: []const f32,
        k: u32,
        ef_search: ?u16,
    ) DatabaseError![]VectorSearchResult {
        const hnsw = &(self.hnsw_index orelse return DatabaseError.IoError);

        return hnsw.search(query_vector, k, ef_search) catch |err| {
            return switch (err) {
                HnswError.EmptyIndex => self.allocator.alloc(VectorSearchResult, 0) catch return DatabaseError.OutOfMemory,
                HnswError.NotFound => self.allocator.alloc(VectorSearchResult, 0) catch return DatabaseError.OutOfMemory,
                HnswError.OutOfMemory => DatabaseError.OutOfMemory,
                else => DatabaseError.IoError,
            };
        };
    }

    /// Free vector search results allocated by vectorSearch.
    pub fn freeVectorSearchResults(self: *Self, results: []VectorSearchResult) void {
        self.allocator.free(results);
    }

    // ========================================================================
    // Full-Text Search Operations
    // ========================================================================

    /// Search for documents matching a text query using BM25 scoring.
    /// Returns node IDs and scores sorted by relevance (highest first).
    pub fn ftsSearch(
        self: *Self,
        query_text: []const u8,
        limit: u32,
    ) DatabaseError![]FtsSearchResult {
        const fts = &(self.fts_index orelse return DatabaseError.IoError);

        return fts.search(query_text, limit) catch |err| {
            return switch (err) {
                FtsError.TokenizerError => DatabaseError.IoError,
                FtsError.OutOfMemory => DatabaseError.OutOfMemory,
                else => DatabaseError.IoError,
            };
        };
    }

    /// Search for documents matching a text query with fuzzy (typo-tolerant) matching.
    pub fn ftsSearchFuzzy(
        self: *Self,
        query_text: []const u8,
        limit: u32,
        max_distance: u32,
        min_term_length: u32,
    ) DatabaseError![]FtsSearchResult {
        const fts = &(self.fts_index orelse return DatabaseError.IoError);
        const config = fuzzy_mod.FuzzyConfig{ .max_distance = max_distance, .min_term_length = min_term_length };

        return fts.searchFuzzy(query_text, config, limit) catch |err| {
            return switch (err) {
                FtsError.TokenizerError => DatabaseError.IoError,
                FtsError.OutOfMemory => DatabaseError.OutOfMemory,
                else => DatabaseError.IoError,
            };
        };
    }

    /// Free FTS search results allocated by ftsSearch.
    pub fn freeFtsSearchResults(self: *Self, results: []FtsSearchResult) void {
        if (self.fts_index) |*fts| {
            fts.freeResults(results);
        }
    }

    /// Index a text document for full-text search.
    /// The document is associated with the given node ID.
    pub fn ftsIndexDocument(
        self: *Self,
        node_id: NodeId,
        text: []const u8,
    ) DatabaseError!void {
        if (self.read_only) return DatabaseError.PermissionDenied;

        const fts = &(self.fts_index orelse return DatabaseError.IoError);

        _ = fts.indexDocument(node_id, text) catch |err| {
            return switch (err) {
                FtsError.TokenizerError => DatabaseError.IoError,
                FtsError.OutOfMemory => DatabaseError.OutOfMemory,
                else => DatabaseError.IoError,
            };
        };
    }

    // ========================================================================
    // Graph Operations - Edges
    // ========================================================================

    /// Create an edge between two nodes
    /// If txn is null, the operation is auto-committed
    pub fn createEdge(
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
        }

        // Intern edge type
        const type_id = self.symbol_table.intern(edge_type) catch {
            return DatabaseError.IoError;
        };

        // Create edge with no properties
        self.edge_store.create(source, target, type_id, &[_]node_mod.Property{}) catch {
            return DatabaseError.IoError;
        };

        // Log to WAL and add undo entry if transaction is provided
        if (txn) |t| {
            if (self.txn_manager) |*tm| {
                var buf: [256]u8 = undefined;
                const payload = wal_payload.serializeEdgeInsert(&buf, source, target, edge_type) catch {
                    return DatabaseError.IoError;
                };
                _ = tm.logOperation(t, .insert, payload) catch {
                    return DatabaseError.IoError;
                };
                // Undo of insert is delete
                tm.addUndoEntry(t, .insert, .edge, source, target, type_id, null) catch {
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
        }

        // Intern edge type
        const type_id = self.symbol_table.intern(edge_type) catch {
            return DatabaseError.IoError;
        };

        // Log to WAL and add undo entry before deletion
        if (txn) |t| {
            if (self.txn_manager) |*tm| {
                // Capture edge properties before delete for undo
                var prop_bytes: []u8 = &[_]u8{};
                var owns_prop_bytes = false;
                if (self.edge_store.get(source, target, type_id)) |existing_edge| {
                    var edge = existing_edge;
                    defer edge.deinit(self.allocator);
                    if (edge.properties.len > 0) {
                        prop_bytes = wal_payload.serializeProperties(self.allocator, @ptrCast(edge.properties)) catch &[_]u8{};
                        owns_prop_bytes = prop_bytes.len > 0;
                    }
                } else |_| {}
                defer if (owns_prop_bytes) self.allocator.free(prop_bytes);

                var buf: [2048]u8 = undefined;
                // Serialize edge data for WAL (now with properties)
                const payload = wal_payload.serializeEdgeDelete(&buf, source, target, type_id, prop_bytes) catch {
                    return DatabaseError.IoError;
                };
                _ = tm.logOperation(t, .delete, payload) catch {
                    return DatabaseError.IoError;
                };
                // Undo of delete is re-insert; store serialized edge data for property recovery
                tm.addUndoEntry(t, .delete, .edge, source, target, type_id, payload) catch {
                    return DatabaseError.IoError;
                };
            }
        }

        self.edge_store.delete(source, target, type_id) catch {
            return DatabaseError.IoError;
        };

        // Invalidate adjacency cache for source node
        if (self.adjacency_cache) |*cache| {
            cache.invalidate(source);
        }

        // Invalidate query cache (edge types changed)
        if (self.query_cache) |cache| cache.bumpSchemaVersion();
    }

    /// Check if an edge exists
    pub fn edgeExists(
        self: *Self,
        source: NodeId,
        target: NodeId,
        edge_type: []const u8,
    ) bool {
        const type_id = self.symbol_table.intern(edge_type) catch return false;
        return self.edge_store.exists(source, target, type_id);
    }

    /// Edge info for traversal results
    pub const EdgeInfo = struct {
        source: NodeId,
        target: NodeId,
        edge_type: []const u8,
    };

    /// Get all outgoing edges from a node.
    /// Caller owns the returned slice and must free it with freeEdgeInfos.
    pub fn getOutgoingEdges(self: *Self, node_id: NodeId) ![]EdgeInfo {
        var iter = self.edge_store.getOutgoing(node_id) catch {
            return DatabaseError.IoError;
        };
        defer iter.deinit();

        var edges: std.ArrayList(EdgeInfo) = .empty;
        errdefer edges.deinit(self.allocator);

        while (try iter.next()) |edge| {
            // Free the edge's owned memory after extracting info
            defer {
                var e = edge;
                e.deinit(self.allocator);
            }

            // resolve() returns an allocated string - use it directly, no need to dupe
            const edge_type_str = self.symbol_table.resolve(edge.edge_type) catch {
                continue;
            };
            edges.append(self.allocator, .{
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
        var iter = self.edge_store.getIncoming(node_id) catch {
            return DatabaseError.IoError;
        };
        defer iter.deinit();

        var edges: std.ArrayList(EdgeInfo) = .empty;
        errdefer edges.deinit(self.allocator);

        while (try iter.next()) |edge| {
            // Free the edge's owned memory after extracting info
            defer {
                var e = edge;
                e.deinit(self.allocator);
            }

            // resolve() returns an allocated string - use it directly, no need to dupe
            const edge_type_str = self.symbol_table.resolve(edge.edge_type) catch {
                continue;
            };
            edges.append(self.allocator, .{
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

    /// Free edge info slice returned by getOutgoingEdges or getIncomingEdges.
    pub fn freeEdgeInfos(self: *Self, edges: []EdgeInfo) void {
        for (edges) |edge| {
            self.allocator.free(edge.edge_type);
        }
        self.allocator.free(edges);
    }

    /// Get lightweight iterator for outgoing edges (no property deserialization).
    /// Returns EdgeRef containing only (source, target, edge_type_id).
    /// Ideal for graph traversal (BFS/DFS) where properties are not needed.
    /// Caller must call iter.deinit() when done.
    pub fn getOutgoingEdgeRefs(self: *Self, node_id: NodeId) !EdgeRefIterator {
        return self.edge_store.getOutgoingRefs(node_id) catch {
            return DatabaseError.IoError;
        };
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
    /// Returns EdgeRef containing only (source, target, edge_type_id).
    /// Caller must call iter.deinit() when done.
    pub fn getIncomingEdgeRefs(self: *Self, node_id: NodeId) !EdgeRefIterator {
        return self.edge_store.getIncomingRefs(node_id) catch {
            return DatabaseError.IoError;
        };
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

    /// Property entry for property iteration
    pub const PropertyEntry = struct {
        key: []const u8,
        value: PropertyValue,
    };

    /// Get all properties for a node.
    /// Returns an allocated slice that the caller must free with freePropertyEntries.
    pub fn getNodeProperties(self: *Self, node_id: NodeId) DatabaseError![]PropertyEntry {
        // Get the node data
        var key_buf: [8]u8 = undefined;
        std.mem.writeInt(u64, &key_buf, node_id, .big);

        const node_data = self.node_tree.get(&key_buf) catch {
            return DatabaseError.IoError;
        };

        if (node_data == null) {
            return &[_]PropertyEntry{};
        }

        const data = node_data.?;
        defer self.allocator.free(data);

        // Parse node data to extract properties
        // Node format: label_count(2) + labels(2*count) + prop_count(2) + properties...
        if (data.len < 2) return &[_]PropertyEntry{};

        const label_count = std.mem.readInt(u16, data[0..2], .little);
        var offset: usize = 2 + @as(usize, label_count) * 2;

        if (offset + 2 > data.len) return &[_]PropertyEntry{};

        const prop_count = std.mem.readInt(u16, data[offset..][0..2], .little);
        offset += 2;

        var props: std.ArrayListUnmanaged(PropertyEntry) = .empty;
        errdefer {
            for (props.items) |prop| {
                self.allocator.free(prop.key);
                if (prop.value == .string_val) {
                    self.allocator.free(prop.value.string_val);
                }
            }
            props.deinit(self.allocator);
        }

        for (0..prop_count) |_| {
            if (offset + 4 > data.len) break;

            // Read key symbol ID
            const key_id = std.mem.readInt(u16, data[offset..][0..2], .little);
            offset += 2;

            // Read value type
            const value_type = data[offset];
            offset += 1;

            // Resolve key name
            const key_name = self.symbol_table.resolve(key_id) catch continue;
            errdefer self.allocator.free(key_name);

            // Parse value based on type
            const value: PropertyValue = switch (value_type) {
                0 => .{ .null_val = {} },
                1 => blk: {
                    if (offset >= data.len) break :blk .{ .null_val = {} };
                    const b = data[offset] != 0;
                    offset += 1;
                    break :blk .{ .bool_val = b };
                },
                2 => blk: {
                    if (offset + 8 > data.len) break :blk .{ .null_val = {} };
                    const i = std.mem.readInt(i64, data[offset..][0..8], .little);
                    offset += 8;
                    break :blk .{ .int_val = i };
                },
                3 => blk: {
                    if (offset + 8 > data.len) break :blk .{ .null_val = {} };
                    const f: f64 = @bitCast(std.mem.readInt(u64, data[offset..][0..8], .little));
                    offset += 8;
                    break :blk .{ .float_val = f };
                },
                4 => blk: {
                    if (offset + 2 > data.len) break :blk .{ .null_val = {} };
                    const str_len = std.mem.readInt(u16, data[offset..][0..2], .little);
                    offset += 2;
                    if (offset + str_len > data.len) break :blk .{ .null_val = {} };
                    const str = self.allocator.dupe(u8, data[offset..][0..str_len]) catch {
                        self.allocator.free(key_name);
                        return DatabaseError.OutOfMemory;
                    };
                    offset += str_len;
                    break :blk .{ .string_val = str };
                },
                else => .{ .null_val = {} },
            };

            props.append(self.allocator, .{ .key = key_name, .value = value }) catch {
                self.allocator.free(key_name);
                if (value == .string_val) {
                    self.allocator.free(value.string_val);
                }
                return DatabaseError.OutOfMemory;
            };
        }

        return props.toOwnedSlice(self.allocator) catch return DatabaseError.OutOfMemory;
    }

    /// Free property entries returned by getNodeProperties.
    pub fn freePropertyEntries(self: *Self, props: []PropertyEntry) void {
        for (props) |prop| {
            self.allocator.free(prop.key);
            if (prop.value == .string_val) {
                self.allocator.free(prop.value.string_val);
            }
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
        const label_id = self.symbol_table.intern(label) catch {
            return DatabaseError.IoError;
        };

        return self.label_index.getNodesByLabel(label_id) catch {
            return DatabaseError.IoError;
        };
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
    pub fn query(self: *Self, cypher: []const u8) QueryError!QueryResult {
        return self.executeQuery(cypher, null);
    }

    /// Execute a Cypher query with bound parameters.
    pub fn queryWithParams(
        self: *Self,
        cypher: []const u8,
        params: *const std.StringHashMap(types.PropertyValue),
    ) QueryError!QueryResult {
        return self.executeQuery(cypher, params);
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
    fn executeQuery(
        self: *Self,
        cypher: []const u8,
        params: ?*const std.StringHashMap(types.PropertyValue),
    ) QueryError!QueryResult {
        var cache_entry: ?*cache_mod.CacheEntry = null;
        defer {
            if (cache_entry) |entry| entry.unpin();
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
            cache_entry = cache.get(cypher);
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
                const owned_source = cache_arena.allocator().dupe(u8, cypher) catch {
                    cache_arena.deinit();
                    return QueryError.OutOfMemory;
                };

                var cache_parser = Parser.initWithArena(self.allocator, owned_source, cache_arena);
                const parse_result = cache_parser.parse();

                if (parse_result.query == null) {
                    // Parse failed - clean up and return error
                    cache_parser.errors.deinit(self.allocator);
                    cache_parser.arena.deinit();
                    return QueryError.ParseError;
                }
                ast_query = parse_result.query.?;

                // Semantic analysis
                var sem = SemanticAnalyzer.init(self.allocator);
                const analysis = sem.analyze(ast_query);
                if (!analysis.success) {
                    sem.deinit();
                    cache_parser.errors.deinit(self.allocator);
                    cache_parser.arena.deinit();
                    return QueryError.SemanticError;
                }
                analysis_vars = analysis.variables;

                // Store in cache (cache takes ownership of the arena)
                const arena_to_cache = cache_parser.arena;
                cache_parser.errors.deinit(self.allocator);
                self.query_cache.?.put(cypher, ast_query, analysis_vars, arena_to_cache);

                // Re-fetch from cache to get a pinned entry
                cache_entry = self.query_cache.?.get(cypher);
                if (cache_entry) |entry| {
                    ast_query = entry.query;
                    analysis_vars = entry.variables;
                }

                sem.deinit();
            } else {
                // No cache: standard parse path
                parser = Parser.init(self.allocator, cypher);
                const parse_result = parser.?.parse();

                if (parse_result.query == null) {
                    return QueryError.ParseError;
                }
                ast_query = parse_result.query.?;

                analyzer = SemanticAnalyzer.init(self.allocator);
                const analysis = analyzer.?.analyze(ast_query);
                if (!analysis.success) {
                    return QueryError.SemanticError;
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

        const root_op = planner.plan(ast_query, &analysis_result) catch {
            return QueryError.PlanError;
        };

        // Execute
        var exec_ctx = ExecutionContext.initWithStorage(self.allocator, &self.node_store, &self.symbol_table);
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

        var exec_result = execute(query_alloc, root_op, &exec_ctx) catch {
            return QueryError.ExecutionError;
        };

        return self.convertResult(&exec_result, &planner);
    }

    /// Convert executor result to database-friendly result format
    fn convertResult(
        self: *Self,
        exec_result: *executor_mod.QueryResult,
        planner: *QueryPlanner,
    ) QueryError!QueryResult {
        // Build column names from planner bindings
        const num_cols = planner.next_slot;
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
            if (slot < num_cols) {
                // Free default name and use actual variable name
                self.allocator.free(columns[slot]);
                columns[slot] = self.allocator.dupe(u8, entry.key_ptr.*) catch {
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
    std.fs.cwd().deleteFile(path) catch {};
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
        std.fs.cwd().deleteFile(path) catch {};
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
    const name_val = try db.getNodeProperty(alice, "name");
    try std.testing.expect(name_val != null);
    try std.testing.expectEqualStrings("Alice", name_val.?.string_val);
    // Free the cloned string
    allocator.free(name_val.?.string_val);

    // Delete node
    try db.deleteNode(null, alice);
    try std.testing.expect(!(try db.nodeExists(alice)));
    try std.testing.expect(try db.nodeExists(bob));
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
        std.fs.cwd().deleteFile(path) catch {};
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
        std.fs.cwd().deleteFile(path) catch {};
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
        std.fs.cwd().deleteFile(path) catch {};
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
        std.fs.cwd().deleteFile(path) catch {};
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
        std.fs.cwd().deleteFile(path) catch {};
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
