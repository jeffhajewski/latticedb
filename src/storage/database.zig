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

        var nodes = std.ArrayList(NodeId).init(self.allocator);
        errdefer nodes.deinit();

        while (iter.next() catch null) |node_id| {
            nodes.append(node_id) catch {
                return DatabaseError.OutOfMemory;
            };
        }

        return nodes.toOwnedSlice() catch {
            return DatabaseError.OutOfMemory;
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
