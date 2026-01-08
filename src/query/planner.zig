//! Query planner for Cypher queries.
//!
//! Transforms parsed AST into executable operator trees.
//! Handles MATCH, WHERE, RETURN, ORDER BY, LIMIT, SKIP clauses.
//! Recognizes vector distance (<=>)  and FTS match (@@) operators
//! and creates specialized search operators for them.

const std = @import("std");
const Allocator = std.mem.Allocator;

const ast = @import("ast.zig");
const semantic = @import("semantic.zig");
const executor = @import("executor.zig");
const expression = @import("expression.zig");
const parser = @import("parser.zig");

const Operator = executor.Operator;
const OperatorError = executor.OperatorError;
const Row = executor.Row;
const ExecutionContext = executor.ExecutionContext;
const MAX_SLOTS = executor.MAX_SLOTS;

const scan_ops = @import("operators/scan.zig");
const filter_ops = @import("operators/filter.zig");
const project_ops = @import("operators/project.zig");
const expand_ops = @import("operators/expand.zig");
const limit_ops = @import("operators/limit.zig");
const vector_ops = @import("operators/vector.zig");
const fts_ops = @import("operators/fts.zig");
const mutation_ops = @import("operators/mutation.zig");

const btree = @import("../storage/btree.zig");
const BTree = btree.BTree;

const label_index = @import("../graph/label_index.zig");
const LabelIndex = label_index.LabelIndex;

const edge_mod = @import("../graph/edge.zig");
const EdgeStore = edge_mod.EdgeStore;

const symbols = @import("../graph/symbols.zig");
const SymbolTable = symbols.SymbolTable;
const SymbolId = symbols.SymbolId;

const hnsw = @import("../vector/hnsw.zig");
const HnswIndex = hnsw.HnswIndex;

const fts_index_mod = @import("../fts/index.zig");
const FtsIndex = fts_index_mod.FtsIndex;

const database_mod = @import("../storage/database.zig");
const Database = database_mod.Database;

// ============================================================================
// Types
// ============================================================================

/// Planner errors
pub const PlannerError = error{
    /// Out of memory
    OutOfMemory,
    /// Invalid query structure
    InvalidQuery,
    /// Unknown label
    UnknownLabel,
    /// Unknown edge type
    UnknownEdgeType,
    /// Too many variables
    TooManyVariables,
    /// Missing storage reference
    MissingStorage,
    /// Unsupported operation
    Unsupported,
    /// Internal error (unexpected condition)
    InternalError,
};

/// Storage context for planning (references to storage structures)
pub const StorageContext = struct {
    /// Node B+Tree
    node_tree: ?*BTree,
    /// Label index
    label_index: ?*LabelIndex,
    /// Edge store
    edge_store: ?*EdgeStore,
    /// Symbol table
    symbol_table: ?*SymbolTable,
    /// HNSW vector index (optional)
    hnsw_index: ?*HnswIndex = null,
    /// FTS index (optional)
    fts_index: ?*FtsIndex = null,
    /// Database for mutation operations (optional)
    database: ?*Database = null,
};

/// Variable binding during planning
pub const VarBinding = struct {
    name: []const u8,
    slot: u8,
    kind: semantic.VariableKind,
};

// ============================================================================
// Query Planner
// ============================================================================

/// Query planner that transforms AST to operator trees
pub const QueryPlanner = struct {
    allocator: Allocator,
    storage: StorageContext,
    bindings: std.StringHashMap(VarBinding),
    next_slot: u8,

    const Self = @This();

    /// Create a new query planner
    pub fn init(allocator: Allocator, storage: StorageContext) Self {
        return Self{
            .allocator = allocator,
            .storage = storage,
            .bindings = std.StringHashMap(VarBinding).init(allocator),
            .next_slot = 0,
        };
    }

    /// Free planner resources
    pub fn deinit(self: *Self) void {
        self.bindings.deinit();
    }

    /// Plan a complete query
    pub fn plan(self: *Self, query: *const ast.Query, analysis: *const semantic.AnalysisResult) PlannerError!Operator {
        _ = analysis; // Used for validation, already done

        // Reset bindings for this query
        self.bindings.clearRetainingCapacity();
        self.next_slot = 0;

        var current_op: ?Operator = null;

        // Process each clause
        for (query.clauses) |clause| {
            current_op = switch (clause) {
                .match => |m| try self.planMatch(m, current_op),
                .where => |w| try self.planWhere(w, current_op),
                .return_ => |r| try self.planReturn(r, current_op),
                .order_by => |o| try self.planOrderBy(o, current_op),
                .limit => |l| try self.planLimit(l, current_op),
                .skip => |s| try self.planSkip(s, current_op),
                .create => |c| try self.planCreate(c, current_op),
                .delete => |d| try self.planDelete(d, current_op),
            };
        }

        return current_op orelse PlannerError.InvalidQuery;
    }

    /// Plan a MATCH clause
    fn planMatch(self: *Self, match: *const ast.MatchClause, input: ?Operator) PlannerError!Operator {
        var op = input;

        for (match.patterns) |pattern| {
            op = try self.planPattern(pattern, op);
        }

        return op orelse PlannerError.InvalidQuery;
    }

    /// Plan a single pattern
    fn planPattern(self: *Self, pattern: ast.Pattern, input: ?Operator) PlannerError!Operator {
        var op = input;
        var prev_node_slot: ?u8 = null;

        for (pattern.elements) |element| {
            switch (element) {
                .node => |node_pattern| {
                    const slot = try self.allocateSlot();

                    // Bind variable if present
                    if (node_pattern.variable) |name| {
                        try self.bindVariable(name, slot, .node);
                    }

                    // Create scan operator
                    if (node_pattern.labels.len > 0) {
                        // Label scan
                        const label_index_ptr = self.storage.label_index orelse return PlannerError.MissingStorage;
                        const symbol_table = self.storage.symbol_table orelse return PlannerError.MissingStorage;

                        // Look up first label (TODO: handle multiple labels)
                        // If label is not found in symbol table, use NULL_SYMBOL (0) which has no nodes
                        // This will result in an empty scan rather than an error
                        const label_id = symbol_table.lookup(node_pattern.labels[0]) catch |err| switch (err) {
                            symbols.SymbolError.NotFound => symbols.NULL_SYMBOL, // Label doesn't exist = empty result
                            else => return PlannerError.InternalError,
                        };

                        const label_scan = scan_ops.LabelScan.init(self.allocator, slot, label_id, label_index_ptr) catch {
                            return PlannerError.OutOfMemory;
                        };
                        op = label_scan.operator();
                    } else if (op == null) {
                        // All nodes scan
                        const node_tree = self.storage.node_tree orelse return PlannerError.MissingStorage;
                        const all_scan = scan_ops.AllNodesScan.init(self.allocator, slot, node_tree) catch {
                            return PlannerError.OutOfMemory;
                        };
                        op = all_scan.operator();
                    }

                    prev_node_slot = slot;
                },
                .edge => |edge_pattern| {
                    // Need previous node to expand from
                    const source_slot = prev_node_slot orelse return PlannerError.InvalidQuery;
                    const target_slot = try self.allocateSlot();

                    // Optional edge variable
                    var edge_slot: ?u8 = null;
                    if (edge_pattern.variable) |name| {
                        edge_slot = try self.allocateSlot();
                        try self.bindVariable(name, edge_slot.?, .edge);
                    }

                    // Determine edge type
                    var edge_type: ?SymbolId = null;
                    if (edge_pattern.types.len > 0) {
                        const symbol_table = self.storage.symbol_table orelse return PlannerError.MissingStorage;
                        edge_type = symbol_table.lookup(edge_pattern.types[0]) catch {
                            return PlannerError.UnknownEdgeType;
                        };
                    }

                    // Map direction
                    const expand_dir: expand_ops.ExpandDirection = switch (edge_pattern.direction) {
                        .outgoing => .outgoing,
                        .incoming => .incoming,
                        .both => .both,
                    };

                    // Create expand operator
                    const edge_store = self.storage.edge_store orelse return PlannerError.MissingStorage;
                    const expand = expand_ops.Expand.init(
                        self.allocator,
                        op.?,
                        source_slot,
                        target_slot,
                        edge_slot,
                        edge_type,
                        expand_dir,
                        edge_store,
                    ) catch {
                        return PlannerError.OutOfMemory;
                    };
                    op = expand.operator();

                    // Target becomes the new "previous node" for next edge
                    prev_node_slot = target_slot;
                },
            }
        }

        return op orelse PlannerError.InvalidQuery;
    }

    /// Plan a WHERE clause
    /// Detects vector distance (<=>)  and FTS match (@@) operators and creates
    /// specialized search operators for them instead of generic filters.
    fn planWhere(self: *Self, where: *const ast.WhereClause, input: ?Operator) PlannerError!Operator {
        const input_op = input orelse return PlannerError.InvalidQuery;

        // Check for vector search pattern: x.embedding <=> $query [< threshold]
        if (self.detectVectorSearch(where.condition)) |vector_info| {
            return self.planVectorSearch(input_op, vector_info);
        }

        // Check for FTS pattern: x.text @@ $query
        if (self.detectFtsSearch(where.condition)) |fts_info| {
            return self.planFtsSearch(input_op, fts_info);
        }

        // Default: create a filter operator
        const filter = filter_ops.Filter.init(self.allocator, input_op, where.condition) catch {
            return PlannerError.OutOfMemory;
        };

        return filter.operator();
    }

    /// Information extracted from a vector search pattern
    const VectorSearchInfo = struct {
        /// The variable slot being searched (e.g., slot for 'n' in n.embedding)
        variable_slot: ?u8,
        /// Parameter name for query vector (e.g., "query" from $query)
        param_name: ?[]const u8,
        /// Literal query vector (if provided directly)
        query_vector: ?[]const f32,
        /// Distance threshold (e.g., 0.5 from < 0.5)
        threshold: ?f32,
        /// The property being searched
        property_name: ?[]const u8,
    };

    /// Detect vector search pattern in expression
    /// Patterns: `x.prop <=> $param < threshold` or `x.prop <=> $param`
    fn detectVectorSearch(self: *Self, expr: *const ast.Expression) ?VectorSearchInfo {
        // Pattern 1: (x.prop <=> $param) < threshold
        if (expr.* == .binary) {
            const binary = expr.binary;

            // Check for comparison with threshold: (vector_distance_expr) < threshold
            if (binary.operator == .lt or binary.operator == .lte) {
                if (binary.left.* == .binary) {
                    const inner = binary.left.binary;
                    if (inner.operator == .vector_distance) {
                        var info = self.extractVectorInfo(inner.*) orelse return null;
                        // Extract threshold from right side
                        if (binary.right.* == .literal) {
                            const lit = binary.right.literal;
                            if (lit.value == .float) {
                                info.threshold = @floatCast(lit.value.float);
                            } else if (lit.value == .integer) {
                                info.threshold = @floatFromInt(lit.value.integer);
                            }
                        }
                        return info;
                    }
                }
            }

            // Pattern 2: x.prop <=> $param (no threshold)
            if (binary.operator == .vector_distance) {
                return self.extractVectorInfo(binary.*);
            }
        }

        return null;
    }

    /// Extract vector search info from a binary expression with vector_distance operator
    fn extractVectorInfo(self: *Self, binary: ast.BinaryExpr) ?VectorSearchInfo {
        var info = VectorSearchInfo{
            .variable_slot = null,
            .param_name = null,
            .query_vector = null,
            .threshold = null,
            .property_name = null,
        };

        // Left side should be property access: x.embedding
        if (binary.left.* == .property_access) {
            const prop_access = binary.left.property_access;
            info.property_name = prop_access.property;

            // Get the variable's slot
            if (prop_access.object.* == .variable) {
                const var_name = prop_access.object.variable.name;
                info.variable_slot = self.getSlot(var_name);
            }
        }

        // Right side should be parameter: $query
        if (binary.right.* == .parameter) {
            info.param_name = binary.right.parameter.name;
        }

        // Must have at least variable and parameter
        if (info.variable_slot != null and info.param_name != null) {
            return info;
        }

        return null;
    }

    /// Plan a vector search operator
    fn planVectorSearch(self: *Self, input: Operator, info: VectorSearchInfo) PlannerError!Operator {
        const hnsw_index = self.storage.hnsw_index orelse return PlannerError.MissingStorage;

        const output_slot = info.variable_slot orelse return PlannerError.InvalidQuery;
        const param_name = info.param_name orelse return PlannerError.InvalidQuery;

        // Create VectorSearchWithInput operator
        const k: u32 = 100; // Default k for search
        const vector_search = vector_ops.VectorSearchWithInput.init(
            self.allocator,
            input,
            output_slot,
            param_name,
            k,
            info.threshold,
            hnsw_index,
        ) catch {
            return PlannerError.OutOfMemory;
        };

        return vector_search.operator();
    }

    /// Information extracted from an FTS search pattern
    const FtsSearchInfo = struct {
        /// The variable slot being searched
        variable_slot: ?u8,
        /// Parameter name for query text
        param_name: ?[]const u8,
        /// Literal query text (if provided directly)
        query_text: ?[]const u8,
        /// The property being searched
        property_name: ?[]const u8,
    };

    /// Detect FTS search pattern in expression
    /// Pattern: `x.prop @@ $param` or `x.prop @@ "literal"`
    fn detectFtsSearch(self: *Self, expr: *const ast.Expression) ?FtsSearchInfo {
        if (expr.* == .binary) {
            const binary = expr.binary;

            if (binary.operator == .fts_match) {
                return self.extractFtsInfo(binary.*);
            }
        }

        return null;
    }

    /// Extract FTS search info from a binary expression with fts_match operator
    fn extractFtsInfo(self: *Self, binary: ast.BinaryExpr) ?FtsSearchInfo {
        var info = FtsSearchInfo{
            .variable_slot = null,
            .param_name = null,
            .query_text = null,
            .property_name = null,
        };

        // Left side should be property access: x.text
        if (binary.left.* == .property_access) {
            const prop_access = binary.left.property_access;
            info.property_name = prop_access.property;

            // Get the variable's slot
            if (prop_access.object.* == .variable) {
                const var_name = prop_access.object.variable.name;
                info.variable_slot = self.getSlot(var_name);
            }
        }

        // Right side: parameter ($query) or literal ("search text")
        if (binary.right.* == .parameter) {
            info.param_name = binary.right.parameter.name;
        } else if (binary.right.* == .literal) {
            const lit = binary.right.literal;
            if (lit.value == .string) {
                info.query_text = lit.value.string;
            }
        }

        // Must have variable and either parameter or literal
        if (info.variable_slot != null and (info.param_name != null or info.query_text != null)) {
            return info;
        }

        return null;
    }

    /// Plan an FTS search operator
    fn planFtsSearch(self: *Self, input: Operator, info: FtsSearchInfo) PlannerError!Operator {
        const fts_index = self.storage.fts_index orelse return PlannerError.MissingStorage;

        const output_slot = info.variable_slot orelse return PlannerError.InvalidQuery;

        // If we have literal query text, use standalone FtsSearch
        if (info.query_text) |query_text| {
            // For literal queries, we use a standalone FtsSearch
            // The input operator is not used - this is an optimization where
            // we replace the scan with an index lookup
            // Note: In a full implementation, we'd want to intersect results
            // For now, the FTS index becomes the primary scan source
            input.deinit(self.allocator);

            const fts_search = fts_ops.FtsSearch.init(
                self.allocator,
                output_slot,
                query_text,
                100, // Default limit
                null, // No min score
                fts_index,
            ) catch {
                return PlannerError.OutOfMemory;
            };

            return fts_search.operator();
        }

        // Use parameter-based search with input operator
        const param_name = info.param_name orelse return PlannerError.InvalidQuery;

        const fts_search = fts_ops.FtsSearchWithInput.init(
            self.allocator,
            input,
            output_slot,
            param_name,
            100, // Default limit
            fts_index,
        ) catch {
            return PlannerError.OutOfMemory;
        };

        return fts_search.operator();
    }

    /// Plan a RETURN clause
    fn planReturn(self: *Self, ret: *const ast.ReturnClause, input: ?Operator) PlannerError!Operator {
        const input_op = input orelse return PlannerError.InvalidQuery;

        // Create projection items
        var items = self.allocator.alloc(project_ops.ProjectItem, ret.items.len) catch {
            return PlannerError.OutOfMemory;
        };
        errdefer self.allocator.free(items);

        for (ret.items, 0..) |item, i| {
            items[i] = .{
                .expr = item.expression,
                .output_slot = @intCast(i),
            };
        }

        const project = project_ops.Project.init(self.allocator, input_op, items) catch {
            return PlannerError.OutOfMemory;
        };

        return project.operator();
    }

    /// Plan an ORDER BY clause
    fn planOrderBy(self: *Self, order: *const ast.OrderByClause, input: ?Operator) PlannerError!Operator {
        const input_op = input orelse return PlannerError.InvalidQuery;

        var sort_items = self.allocator.alloc(limit_ops.SortItem, order.items.len) catch {
            return PlannerError.OutOfMemory;
        };
        errdefer self.allocator.free(sort_items);

        for (order.items, 0..) |item, i| {
            sort_items[i] = .{
                .expr = item.expression,
                .descending = item.descending,
            };
        }

        const sort = limit_ops.Sort.init(self.allocator, input_op, sort_items) catch {
            return PlannerError.OutOfMemory;
        };

        return sort.operator();
    }

    /// Plan a LIMIT clause
    fn planLimit(self: *Self, lim: *const ast.LimitClause, input: ?Operator) PlannerError!Operator {
        const input_op = input orelse return PlannerError.InvalidQuery;

        // Evaluate limit expression (should be integer literal)
        const count = switch (lim.count.*) {
            .literal => |lit| switch (lit.value) {
                .integer => |i| @as(u64, @intCast(i)),
                else => return PlannerError.InvalidQuery,
            },
            else => return PlannerError.InvalidQuery,
        };

        const limit = limit_ops.Limit.init(self.allocator, input_op, count) catch {
            return PlannerError.OutOfMemory;
        };

        return limit.operator();
    }

    /// Plan a SKIP clause
    fn planSkip(self: *Self, skip_clause: *const ast.SkipClause, input: ?Operator) PlannerError!Operator {
        const input_op = input orelse return PlannerError.InvalidQuery;

        // Evaluate skip expression (should be integer literal)
        const count = switch (skip_clause.count.*) {
            .literal => |lit| switch (lit.value) {
                .integer => |i| @as(u64, @intCast(i)),
                else => return PlannerError.InvalidQuery,
            },
            else => return PlannerError.InvalidQuery,
        };

        const skip = limit_ops.Skip.init(self.allocator, input_op, count) catch {
            return PlannerError.OutOfMemory;
        };

        return skip.operator();
    }

    /// Plan a CREATE clause
    fn planCreate(self: *Self, create: *const ast.CreateClause, input: ?Operator) PlannerError!Operator {
        const database = self.storage.database orelse return PlannerError.MissingStorage;
        var op = input;

        // Process each pattern in CREATE
        for (create.patterns) |pattern| {
            var prev_node_slot: ?u8 = null;

            for (pattern.elements) |element| {
                switch (element) {
                    .node => |node_pattern| {
                        const slot = try self.allocateSlot();

                        // Bind variable if present
                        if (node_pattern.variable) |name| {
                            try self.bindVariable(name, slot, .node);
                        }

                        // Build properties list
                        var properties: std.ArrayList(mutation_ops.CreateNode.PropertyKV) = .empty;
                        if (node_pattern.properties) |props| {
                            for (props) |prop| {
                                properties.append(self.allocator, .{
                                    .key = prop.key,
                                    .value_expr = prop.value,
                                }) catch return PlannerError.OutOfMemory;
                            }
                        }

                        // Create the CreateNode operator
                        const create_node = mutation_ops.CreateNode.init(
                            self.allocator,
                            op,
                            node_pattern.labels,
                            properties.toOwnedSlice(self.allocator) catch return PlannerError.OutOfMemory,
                            slot,
                            database,
                        ) catch return PlannerError.OutOfMemory;

                        op = create_node.operator();
                        prev_node_slot = slot;
                    },
                    .edge => |edge_pattern| {
                        // Edge requires a source node from previous element
                        if (prev_node_slot == null) {
                            return PlannerError.InvalidQuery;
                        }

                        // The target node must be the next element in the pattern
                        // For simplicity, we require edges to connect nodes already created
                        // A more complete implementation would look ahead
                        if (edge_pattern.types.len == 0) {
                            return PlannerError.InvalidQuery; // Edge type required for CREATE
                        }

                        // Edge creation is deferred until we have the target node
                        // For MVP, we only support simple node creation
                        // Full edge support would require two-pass pattern processing
                    },
                }
            }
        }

        return op orelse PlannerError.InvalidQuery;
    }

    /// Plan a DELETE clause
    fn planDelete(self: *Self, delete: *const ast.DeleteClause, input: ?Operator) PlannerError!Operator {
        const database = self.storage.database orelse return PlannerError.MissingStorage;
        const input_op = input orelse return PlannerError.InvalidQuery;
        var op = input_op;

        // Process each expression to delete
        for (delete.expressions) |expr| {
            // Expression should be a variable reference
            if (expr.* != .variable) {
                return PlannerError.InvalidQuery;
            }

            const var_name = expr.variable.name;
            const binding = self.bindings.get(var_name) orelse return PlannerError.InvalidQuery;

            // Create DeleteNode operator (for now, only support node deletion)
            if (binding.kind == .node) {
                const delete_node = mutation_ops.DeleteNode.init(
                    self.allocator,
                    op,
                    binding.slot,
                    delete.detach,
                    database,
                ) catch return PlannerError.OutOfMemory;

                op = delete_node.operator();
            }
            // Edge deletion would require more complex handling
        }

        return op;
    }

    /// Allocate a new variable slot
    fn allocateSlot(self: *Self) PlannerError!u8 {
        if (self.next_slot >= MAX_SLOTS) {
            return PlannerError.TooManyVariables;
        }
        const slot = self.next_slot;
        self.next_slot += 1;
        return slot;
    }

    /// Bind a variable name to a slot
    fn bindVariable(self: *Self, name: []const u8, slot: u8, kind: semantic.VariableKind) PlannerError!void {
        self.bindings.put(name, .{
            .name = name,
            .slot = slot,
            .kind = kind,
        }) catch {
            return PlannerError.OutOfMemory;
        };
    }

    /// Get the slot for a variable
    pub fn getSlot(self: *const Self, name: []const u8) ?u8 {
        const binding = self.bindings.get(name) orelse return null;
        return binding.slot;
    }

    /// Register all variable bindings in execution context
    pub fn registerBindings(self: *const Self, ctx: *ExecutionContext) !void {
        var iter = self.bindings.iterator();
        while (iter.next()) |entry| {
            try ctx.registerVariable(entry.key_ptr.*, entry.value_ptr.slot);
        }
    }
};

// ============================================================================
// Tests
// ============================================================================

test "planner initialization" {
    const allocator = std.testing.allocator;
    const storage = StorageContext{
        .node_tree = null,
        .label_index = null,
        .edge_store = null,
        .symbol_table = null,
    };

    var planner = QueryPlanner.init(allocator, storage);
    defer planner.deinit();

    try std.testing.expectEqual(@as(u8, 0), planner.next_slot);
}

test "slot allocation" {
    const allocator = std.testing.allocator;
    const storage = StorageContext{
        .node_tree = null,
        .label_index = null,
        .edge_store = null,
        .symbol_table = null,
    };

    var planner = QueryPlanner.init(allocator, storage);
    defer planner.deinit();

    const slot1 = try planner.allocateSlot();
    const slot2 = try planner.allocateSlot();
    const slot3 = try planner.allocateSlot();

    try std.testing.expectEqual(@as(u8, 0), slot1);
    try std.testing.expectEqual(@as(u8, 1), slot2);
    try std.testing.expectEqual(@as(u8, 2), slot3);
}

test "variable binding" {
    const allocator = std.testing.allocator;
    const storage = StorageContext{
        .node_tree = null,
        .label_index = null,
        .edge_store = null,
        .symbol_table = null,
    };

    var planner = QueryPlanner.init(allocator, storage);
    defer planner.deinit();

    const slot = try planner.allocateSlot();
    try planner.bindVariable("n", slot, .node);

    try std.testing.expectEqual(@as(?u8, 0), planner.getSlot("n"));
    try std.testing.expectEqual(@as(?u8, null), planner.getSlot("m"));
}
