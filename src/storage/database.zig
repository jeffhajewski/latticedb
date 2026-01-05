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

// Graph layer
const symbols_mod = lattice.graph.symbols;
const SymbolTable = symbols_mod.SymbolTable;

const node_mod = lattice.graph.node;
const NodeStore = node_mod.NodeStore;

const edge_mod = lattice.graph.edge;
const EdgeStore = edge_mod.EdgeStore;

const label_index_mod = lattice.graph.label_index;
const LabelIndex = label_index_mod.LabelIndex;

// FTS
const fts_mod = lattice.fts.index;
const FtsIndex = fts_mod.FtsIndex;
const FtsConfig = fts_mod.FtsConfig;

// Transactions
const txn_mod = lattice.transaction.manager;
const TxnManager = txn_mod.TxnManager;

// Query system
const parser_mod = lattice.query.parser;
const Parser = parser_mod.Parser;
const semantic_mod = lattice.query.semantic;
const SemanticAnalyzer = semantic_mod.SemanticAnalyzer;
const planner_mod = lattice.query.planner;
const QueryPlanner = planner_mod.QueryPlanner;
const StorageContext = planner_mod.StorageContext;
const executor_mod = lattice.query.executor;
const ExecutionContext = executor_mod.ExecutionContext;
const execute = executor_mod.execute;

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

    /// Format for display
    pub fn format(self: ResultValue, writer: anytype) !void {
        switch (self) {
            .null_val => try writer.writeAll("null"),
            .bool_val => |b| try writer.print("{}", .{b}),
            .int_val => |i| try writer.print("{}", .{i}),
            .float_val => |f| try writer.print("{d:.6}", .{f}),
            .string_val => |s| try writer.print("\"{s}\"", .{s}),
            .node_id => |id| try writer.print("Node({d})", .{id}),
        }
    }
};

/// A row in a query result
pub const ResultRow = struct {
    values: []ResultValue,

    pub fn deinit(self: *ResultRow, allocator: Allocator) void {
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
    /// Buffer pool size in bytes (default 4MB)
    buffer_pool_size: usize = 4 * 1024 * 1024,
    /// Enable write-ahead logging
    enable_wal: bool = true,
    /// Enable full-text search
    enable_fts: bool = true,
    /// FTS configuration
    fts_config: FtsConfig = .{},
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

    // Graph stores
    symbol_table: SymbolTable,
    node_store: NodeStore,
    edge_store: EdgeStore,
    label_index: LabelIndex,

    // Indexes
    fts_index: ?FtsIndex,

    // Transactions
    txn_manager: ?TxnManager,

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

        // 3. Initialize BufferPool
        self.buffer_pool = BufferPool.init(allocator, &self.page_manager, options.config.buffer_pool_size) catch {
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
            ) catch {
                // WAL init failure is non-fatal, continue without WAL
                self.wal = null;
            };
        }

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

        // 9. Initialize Transaction Manager
        self.txn_manager = null;
        if (self.wal) |*wal| {
            self.txn_manager = TxnManager.init(allocator, wal);
        }

        return self;
    }

    /// Close the database and release all resources
    pub fn close(self: *Self) void {
        // Reverse initialization order

        // 9. Transaction manager
        if (self.txn_manager) |*tm| {
            tm.deinit();
        }

        // 8. FTS index
        if (self.fts_index) |*fts| {
            fts.deinit();
        }

        // 7. Graph stores (no explicit deinit needed - they don't own resources)
        // 6. Symbol table (no explicit deinit needed)

        // 5. B+Trees - save root pages before closing
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

        self.page_manager.updateHeader(&header) catch {
            return DatabaseError.IoError;
        };
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
    pub fn createNode(self: *Self, labels: []const []const u8) !NodeId {
        if (self.read_only) return DatabaseError.PermissionDenied;

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
            self.label_index.add(label_id, node_id) catch {};
        }

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
    pub fn deleteNode(self: *Self, node_id: NodeId) !void {
        if (self.read_only) return DatabaseError.PermissionDenied;

        // Get node to find its labels for index cleanup
        if (try self.getNode(node_id)) |node| {
            // Remove from label index
            for (node.labels) |label_id| {
                self.label_index.remove(label_id, node_id) catch {};
            }

            // Free node data
            self.allocator.free(node.labels);
            self.allocator.free(node.properties);
        }

        // Delete from node store
        self.node_store.delete(node_id) catch |err| {
            return switch (err) {
                node_mod.NodeError.NotFound => {},
                else => DatabaseError.IoError,
            };
        };
    }

    /// Check if a node exists
    pub fn nodeExists(self: *Self, node_id: NodeId) bool {
        return self.node_store.exists(node_id);
    }

    // ========================================================================
    // Graph Operations - Edges
    // ========================================================================

    /// Create an edge between two nodes
    pub fn createEdge(
        self: *Self,
        source: NodeId,
        target: NodeId,
        edge_type: []const u8,
    ) !void {
        if (self.read_only) return DatabaseError.PermissionDenied;

        // Intern edge type
        const type_id = self.symbol_table.intern(edge_type) catch {
            return DatabaseError.IoError;
        };

        // Create edge with no properties
        self.edge_store.create(source, target, type_id, &[_]node_mod.Property{}) catch {
            return DatabaseError.IoError;
        };
    }

    /// Delete an edge
    pub fn deleteEdge(
        self: *Self,
        source: NodeId,
        target: NodeId,
        edge_type: []const u8,
    ) !void {
        if (self.read_only) return DatabaseError.PermissionDenied;

        // Intern edge type
        const type_id = self.symbol_table.intern(edge_type) catch {
            return DatabaseError.IoError;
        };

        self.edge_store.delete(source, target, type_id) catch {
            return DatabaseError.IoError;
        };
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

    // ========================================================================
    // Label Operations
    // ========================================================================

    /// Get all nodes with a specific label
    pub fn getNodesByLabel(self: *Self, label: []const u8) ![]NodeId {
        const label_id = self.symbol_table.intern(label) catch {
            return DatabaseError.IoError;
        };

        var iter = self.label_index.getNodes(label_id) catch {
            return DatabaseError.IoError;
        };

        var nodes: std.ArrayList(NodeId) = .empty;
        errdefer nodes.deinit(self.allocator);

        while (iter.next() catch null) |node_id| {
            nodes.append(self.allocator, node_id) catch {
                return DatabaseError.OutOfMemory;
            };
        }

        return nodes.toOwnedSlice(self.allocator) catch {
            return DatabaseError.OutOfMemory;
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
        // 1. Parse the query
        var parser = Parser.init(self.allocator, cypher);
        const parse_result = parser.parse();

        if (parse_result.query == null) {
            return QueryError.ParseError;
        }
        const ast_query = parse_result.query.?;
        defer ast_query.deinit(self.allocator);

        // 2. Semantic analysis
        var analyzer = SemanticAnalyzer.init(self.allocator);
        defer analyzer.deinit();

        const analysis = analyzer.analyze(ast_query);
        if (!analysis.success) {
            return QueryError.SemanticError;
        }

        // 3. Create planner with storage context
        const storage_ctx = StorageContext{
            .node_tree = &self.node_tree,
            .label_index = &self.label_index,
            .edge_store = &self.edge_store,
            .symbol_table = &self.symbol_table,
            .hnsw_index = null, // TODO: Add HNSW index to Database when vector storage is integrated
            .fts_index = if (self.fts_index) |*fts| fts else null,
        };

        var planner = QueryPlanner.init(self.allocator, storage_ctx);
        defer planner.deinit();

        // 4. Plan the query
        const root_op = planner.plan(ast_query, &analysis) catch {
            return QueryError.PlanError;
        };
        defer root_op.deinit(self.allocator);

        // 5. Create execution context
        var exec_ctx = ExecutionContext.init(self.allocator);
        defer exec_ctx.deinit();

        // Register variable bindings from planner
        var binding_iter = planner.bindings.iterator();
        while (binding_iter.next()) |entry| {
            exec_ctx.registerVariable(entry.key_ptr.*, entry.value_ptr.slot) catch {
                return QueryError.OutOfMemory;
            };
        }

        // 6. Execute the query
        var exec_result = execute(self.allocator, root_op, &exec_ctx) catch {
            return QueryError.ExecutionError;
        };
        defer exec_result.deinit();

        // 7. Convert executor result to database result
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
                    values[col_idx] = self.slotToResultValue(slot_value);
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
    fn slotToResultValue(self: *Self, slot: executor_mod.SlotValue) ResultValue {
        _ = self;
        return switch (slot) {
            .empty => .{ .null_val = {} },
            .node_ref => |id| .{ .node_id = id },
            .edge_ref => .{ .null_val = {} }, // TODO: Add edge_id to ResultValue if needed
            .property => |prop| switch (prop) {
                .null_value => .{ .null_val = {} },
                .bool_value => |b| .{ .bool_val = b },
                .int_value => |i| .{ .int_val = i },
                .float_value => |f| .{ .float_val = f },
                .string_value => |s| .{ .string_val = s },
                .symbol_id => |id| .{ .int_val = @intCast(id) },
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
    const alice = try db.createNode(&[_][]const u8{"Person"});
    const bob = try db.createNode(&[_][]const u8{"Person"});

    try std.testing.expect(db.nodeExists(alice));
    try std.testing.expect(db.nodeExists(bob));

    // Create edge
    try db.createEdge(alice, bob, "KNOWS");
    try std.testing.expect(db.edgeExists(alice, bob, "KNOWS"));
    try std.testing.expect(!db.edgeExists(bob, alice, "KNOWS")); // directed

    // Get nodes by label
    const people = try db.getNodesByLabel("Person");
    defer allocator.free(people);
    try std.testing.expectEqual(@as(usize, 2), people.len);

    // Delete edge
    try db.deleteEdge(alice, bob, "KNOWS");
    try std.testing.expect(!db.edgeExists(alice, bob, "KNOWS"));

    // Delete node
    try db.deleteNode(alice);
    try std.testing.expect(!db.nodeExists(alice));
    try std.testing.expect(db.nodeExists(bob));
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
    const alice = try db.createNode(&[_][]const u8{"Person"});
    const bob = try db.createNode(&[_][]const u8{"Person"});
    _ = try db.createNode(&[_][]const u8{"Company"});

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
