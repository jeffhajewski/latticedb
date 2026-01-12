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
const var_expand_ops = @import("operators/var_expand.zig");
const limit_ops = @import("operators/limit.zig");
const vector_ops = @import("operators/vector.zig");
const fts_ops = @import("operators/fts.zig");
const mutation_ops = @import("operators/mutation.zig");
const aggregate_ops = @import("operators/aggregate.zig");

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

/// Edge binding with additional metadata for deletion
pub const EdgeBinding = struct {
    source_slot: u8,
    target_slot: u8,
    edge_type: []const u8,
};

// ============================================================================
// Query Planner
// ============================================================================

/// Query planner that transforms AST to operator trees
pub const QueryPlanner = struct {
    allocator: Allocator,
    storage: StorageContext,
    bindings: std.StringHashMap(VarBinding),
    edge_bindings: std.StringHashMap(EdgeBinding),
    next_slot: u8,

    const Self = @This();

    /// Create a new query planner
    pub fn init(allocator: Allocator, storage: StorageContext) Self {
        return Self{
            .allocator = allocator,
            .storage = storage,
            .bindings = std.StringHashMap(VarBinding).init(allocator),
            .edge_bindings = std.StringHashMap(EdgeBinding).init(allocator),
            .next_slot = 0,
        };
    }

    /// Free planner resources
    pub fn deinit(self: *Self) void {
        self.bindings.deinit();
        self.edge_bindings.deinit();
    }

    /// Plan a complete query
    pub fn plan(self: *Self, query: *const ast.Query, analysis: *const semantic.AnalysisResult) PlannerError!Operator {
        _ = analysis; // Used for validation, already done

        // Reset bindings for this query
        self.bindings.clearRetainingCapacity();
        self.edge_bindings.clearRetainingCapacity();
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
                .set => |s| try self.planSet(s, current_op),
                .remove => |r| try self.planRemove(r, current_op),
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
                    // If prev_node_slot is set, this node follows an edge.
                    // Use the target slot from the Expand operator instead of allocating a new slot.
                    const slot = if (prev_node_slot != null and op != null)
                        prev_node_slot.? // Use target slot from preceding edge's Expand
                    else
                        try self.allocateSlot();

                    // Bind variable if present
                    if (node_pattern.variable) |name| {
                        try self.bindVariable(name, slot, .node);
                    }

                    // Determine if we need to create a scan or filter
                    const is_target_of_edge = prev_node_slot != null and op != null;

                    if (is_target_of_edge) {
                        // This node is the target of an edge - data comes from Expand operator.
                        // If labels are specified, add a filter to check them.
                        if (node_pattern.labels.len > 0) {
                            const symbol_table = self.storage.symbol_table orelse return PlannerError.MissingStorage;
                            const label_id = symbol_table.lookup(node_pattern.labels[0]) catch |err| switch (err) {
                                symbols.SymbolError.NotFound => symbols.NULL_SYMBOL,
                                else => return PlannerError.InternalError,
                            };

                            // Create a label filter for the target node
                            const label_filter = filter_ops.LabelFilter.init(self.allocator, op.?, slot, label_id) catch {
                                return PlannerError.OutOfMemory;
                            };
                            op = label_filter.operator();
                        }
                        // If no labels, no filter needed - just use the Expand output as-is
                    } else if (node_pattern.labels.len > 0) {
                        // First node with labels - create a label scan
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

                        // Store edge binding metadata for DELETE support
                        if (edge_pattern.types.len > 0) {
                            self.edge_bindings.put(name, .{
                                .source_slot = source_slot,
                                .target_slot = target_slot,
                                .edge_type = edge_pattern.types[0],
                            }) catch return PlannerError.OutOfMemory;
                        }
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

                    // Create expand operator (variable-length or regular)
                    const edge_store = self.storage.edge_store orelse return PlannerError.MissingStorage;

                    if (edge_pattern.quantifier) |quant| {
                        // Variable-length path: use VariableLengthExpand
                        const var_expand = var_expand_ops.VariableLengthExpand.init(
                            self.allocator,
                            op.?,
                            source_slot,
                            target_slot,
                            edge_type,
                            expand_dir,
                            edge_store,
                            quant.min_hops,
                            quant.max_hops,
                        ) catch {
                            return PlannerError.OutOfMemory;
                        };
                        op = var_expand.operator();
                    } else {
                        // Regular single-hop expand
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
                    }

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

        // If we have literal query text, use FtsSearchWithInput with literal
        if (info.query_text) |query_text| {
            // Use FtsSearchWithInput which filters FTS results against the input operator
            // This ensures MATCH constraints (like labels) are respected
            const fts_search = fts_ops.FtsSearchWithInput.initWithLiteral(
                self.allocator,
                input,
                output_slot,
                query_text,
                100, // Default limit
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

        // Check if any RETURN items contain aggregate functions
        var has_aggregates = false;
        for (ret.items) |item| {
            if (aggregate_ops.containsAggregate(item.expression)) {
                has_aggregates = true;
                break;
            }
        }

        if (has_aggregates) {
            return self.planAggregateReturn(ret, input_op);
        }

        // No aggregates - create simple projection
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

    /// Plan a RETURN clause with aggregations
    fn planAggregateReturn(self: *Self, ret: *const ast.ReturnClause, input_op: Operator) PlannerError!Operator {
        // Separate items into grouping keys and aggregates
        var group_keys: std.ArrayList(aggregate_ops.GroupKey) = .empty;
        defer group_keys.deinit(self.allocator);

        var agg_items: std.ArrayList(aggregate_ops.AggregateItem) = .empty;
        defer agg_items.deinit(self.allocator);

        // Also build projection items for the final output
        var proj_items: std.ArrayList(project_ops.ProjectItem) = .empty;
        defer proj_items.deinit(self.allocator);

        for (ret.items, 0..) |item, i| {
            const slot: u8 = @intCast(i);

            if (self.isDirectAggregate(item.expression)) |agg_info| {
                // Direct aggregate function call: count(n), sum(n.val), etc.
                agg_items.append(self.allocator, .{
                    .func = agg_info.func,
                    .expr = agg_info.arg,
                    .output_slot = slot,
                    .distinct = false,
                }) catch return PlannerError.OutOfMemory;

                // Projection just passes through the aggregate result
                proj_items.append(self.allocator, .{
                    .expr = item.expression,
                    .output_slot = slot,
                }) catch return PlannerError.OutOfMemory;
            } else if (aggregate_ops.containsAggregate(item.expression)) {
                // Complex expression containing aggregate - not yet supported
                // For now, treat as error
                return PlannerError.InvalidQuery;
            } else {
                // Non-aggregate expression - becomes a grouping key
                group_keys.append(self.allocator, .{
                    .expr = item.expression,
                    .output_slot = slot,
                }) catch return PlannerError.OutOfMemory;

                proj_items.append(self.allocator, .{
                    .expr = item.expression,
                    .output_slot = slot,
                }) catch return PlannerError.OutOfMemory;
            }
        }

        // Create owned slices for the Aggregate operator
        const owned_group_keys = self.allocator.dupe(aggregate_ops.GroupKey, group_keys.items) catch {
            return PlannerError.OutOfMemory;
        };
        errdefer self.allocator.free(owned_group_keys);

        const owned_agg_items = self.allocator.dupe(aggregate_ops.AggregateItem, agg_items.items) catch {
            return PlannerError.OutOfMemory;
        };
        errdefer self.allocator.free(owned_agg_items);

        // Create the aggregate operator
        const aggregate = aggregate_ops.Aggregate.init(
            self.allocator,
            input_op,
            owned_group_keys,
            owned_agg_items,
        ) catch {
            return PlannerError.OutOfMemory;
        };

        // The aggregate operator already outputs to the correct slots,
        // so we can return it directly without an additional projection
        return aggregate.operator();
    }

    /// Check if an expression is a direct aggregate function call (e.g., count(n), sum(n.val))
    /// Returns the aggregate function type and argument if so
    fn isDirectAggregate(self: *Self, expr: *const ast.Expression) ?struct { func: aggregate_ops.AggregateFunc, arg: ?*const ast.Expression } {
        _ = self;
        switch (expr.*) {
            .function_call => |f| {
                if (aggregate_ops.parseAggregateFunc(f.name)) |func| {
                    // COUNT(*) has no arguments
                    if (func == .count and f.arguments.len == 0) {
                        return .{ .func = .count_star, .arg = null };
                    }
                    // Other aggregates need exactly one argument
                    if (f.arguments.len == 1) {
                        return .{ .func = func, .arg = f.arguments[0] };
                    }
                }
            },
            else => {},
        }
        return null;
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

        // Pending edge info for deferred creation
        const PendingEdge = struct {
            source_slot: u8,
            edge_pattern: *const ast.EdgePattern,
        };

        // Process each pattern in CREATE
        for (create.patterns) |pattern| {
            var prev_node_slot: ?u8 = null;
            var pending_edge: ?PendingEdge = null;

            for (pattern.elements) |element| {
                switch (element) {
                    .node => |node_pattern| {
                        var slot: u8 = undefined;
                        var need_create = true;

                        // Check if this references an existing variable (from MATCH)
                        if (node_pattern.variable) |name| {
                            if (self.bindings.get(name)) |existing| {
                                // Variable already bound - use existing slot, don't create
                                slot = existing.slot;
                                need_create = false;
                            } else {
                                // New variable - allocate slot and bind
                                slot = try self.allocateSlot();
                                try self.bindVariable(name, slot, .node);
                            }
                        } else {
                            // Anonymous node - always create
                            slot = try self.allocateSlot();
                        }

                        if (need_create) {
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
                        }

                        // If there's a pending edge, now we have the target - create it
                        if (pending_edge) |pe| {
                            if (pe.edge_pattern.types.len == 0) {
                                return PlannerError.InvalidQuery; // Edge type required
                            }

                            // Bind edge variable if present
                            if (pe.edge_pattern.variable) |edge_name| {
                                const edge_slot = try self.allocateSlot();
                                try self.bindVariable(edge_name, edge_slot, .edge);
                            }

                            // Determine direction and create edge
                            // For incoming edges (<-[]-), the current node is the source
                            const source_slot = if (pe.edge_pattern.direction == .incoming) slot else pe.source_slot;
                            const target_slot = if (pe.edge_pattern.direction == .incoming) pe.source_slot else slot;

                            const create_edge = mutation_ops.CreateEdge.init(
                                self.allocator,
                                op.?,
                                source_slot,
                                target_slot,
                                pe.edge_pattern.types[0], // Use first type
                                null, // Edge slot tracking deferred
                                database,
                            ) catch return PlannerError.OutOfMemory;

                            op = create_edge.operator();
                            pending_edge = null;
                        }

                        prev_node_slot = slot;
                    },
                    .edge => |edge_pattern| {
                        // Edge requires a source node from previous element
                        const source = prev_node_slot orelse return PlannerError.InvalidQuery;

                        // Defer edge creation until we have the target node
                        pending_edge = .{
                            .source_slot = source,
                            .edge_pattern = edge_pattern,
                        };
                    },
                }
            }

            // Check for dangling edge (edge without target node)
            if (pending_edge != null) {
                return PlannerError.InvalidQuery;
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

            if (binding.kind == .node) {
                // Create DeleteNode operator
                const delete_node = mutation_ops.DeleteNode.init(
                    self.allocator,
                    op,
                    binding.slot,
                    delete.detach,
                    database,
                ) catch return PlannerError.OutOfMemory;

                op = delete_node.operator();
            } else if (binding.kind == .edge) {
                // Look up edge metadata for deletion
                const edge_meta = self.edge_bindings.get(var_name) orelse {
                    // Edge without metadata - can't delete (need source/target/type)
                    return PlannerError.InvalidQuery;
                };

                // Create DeleteEdge operator
                const delete_edge = mutation_ops.DeleteEdge.init(
                    self.allocator,
                    op,
                    edge_meta.source_slot,
                    edge_meta.target_slot,
                    edge_meta.edge_type,
                    database,
                ) catch return PlannerError.OutOfMemory;

                op = delete_edge.operator();
            }
        }

        return op;
    }

    /// Plan a SET clause
    fn planSet(self: *Self, set_clause: *const ast.SetClause, input: ?Operator) PlannerError!Operator {
        const database = self.storage.database orelse return PlannerError.MissingStorage;
        var op = input orelse return PlannerError.InvalidQuery;

        for (set_clause.items) |item| {
            switch (item) {
                .property => |p| {
                    // Get variable slot from target expression
                    const var_name = getVariableName(p.target) orelse return PlannerError.InvalidQuery;
                    const binding = self.bindings.get(var_name) orelse return PlannerError.InvalidQuery;

                    const set_prop = mutation_ops.SetProperty.init(
                        self.allocator,
                        op,
                        binding.slot,
                        p.property_name,
                        p.value,
                        database,
                    ) catch return PlannerError.OutOfMemory;

                    op = set_prop.operator();
                },
                .labels => |l| {
                    // Get variable slot
                    const var_name = getVariableName(l.target) orelse return PlannerError.InvalidQuery;
                    const binding = self.bindings.get(var_name) orelse return PlannerError.InvalidQuery;

                    if (binding.kind != .node) return PlannerError.InvalidQuery;

                    const set_labels = mutation_ops.SetLabels.init(
                        self.allocator,
                        op,
                        binding.slot,
                        l.label_names,
                        database,
                    ) catch return PlannerError.OutOfMemory;

                    op = set_labels.operator();
                },
                .replace_properties => |r| {
                    // Get variable slot
                    const var_name = getVariableName(r.target) orelse return PlannerError.InvalidQuery;
                    const binding = self.bindings.get(var_name) orelse return PlannerError.InvalidQuery;

                    const set_replace = mutation_ops.SetPropertiesReplace.init(
                        self.allocator,
                        op,
                        binding.slot,
                        r.map,
                        database,
                    ) catch return PlannerError.OutOfMemory;

                    op = set_replace.operator();
                },
                .merge_properties => |m| {
                    // Get variable slot
                    const var_name = getVariableName(m.target) orelse return PlannerError.InvalidQuery;
                    const binding = self.bindings.get(var_name) orelse return PlannerError.InvalidQuery;

                    const set_merge = mutation_ops.SetPropertiesMerge.init(
                        self.allocator,
                        op,
                        binding.slot,
                        m.map,
                        database,
                    ) catch return PlannerError.OutOfMemory;

                    op = set_merge.operator();
                },
            }
        }

        return op;
    }

    /// Plan a REMOVE clause
    fn planRemove(self: *Self, remove_clause: *const ast.RemoveClause, input: ?Operator) PlannerError!Operator {
        const database = self.storage.database orelse return PlannerError.MissingStorage;
        var op = input orelse return PlannerError.InvalidQuery;

        for (remove_clause.items) |item| {
            switch (item) {
                .property => |p| {
                    // Get variable slot from target expression
                    const var_name = getVariableName(p.target) orelse return PlannerError.InvalidQuery;
                    const binding = self.bindings.get(var_name) orelse return PlannerError.InvalidQuery;

                    const remove_prop = mutation_ops.RemoveProperty.init(
                        self.allocator,
                        op,
                        binding.slot,
                        p.property_name,
                        database,
                    ) catch return PlannerError.OutOfMemory;

                    op = remove_prop.operator();
                },
                .labels => |l| {
                    // Get variable slot
                    const var_name = getVariableName(l.target) orelse return PlannerError.InvalidQuery;
                    const binding = self.bindings.get(var_name) orelse return PlannerError.InvalidQuery;

                    if (binding.kind != .node) return PlannerError.InvalidQuery;

                    const remove_labels = mutation_ops.RemoveLabels.init(
                        self.allocator,
                        op,
                        binding.slot,
                        l.label_names,
                        database,
                    ) catch return PlannerError.OutOfMemory;

                    op = remove_labels.operator();
                },
            }
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

/// Get variable name from an expression (if it's a simple variable reference)
fn getVariableName(expr: *const ast.Expression) ?[]const u8 {
    return switch (expr.*) {
        .variable => |v| v.name,
        else => null,
    };
}

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
