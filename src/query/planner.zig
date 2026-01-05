//! Query planner for Cypher queries.
//!
//! Transforms parsed AST into executable operator trees.
//! Handles MATCH, WHERE, RETURN, ORDER BY, LIMIT, SKIP clauses.

const std = @import("std");
const Allocator = std.mem.Allocator;

const ast = @import("ast.zig");
const semantic = @import("semantic.zig");
const executor = @import("executor.zig");
const expression = @import("expression.zig");

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

const btree = @import("../storage/btree.zig");
const BTree = btree.BTree;

const label_index = @import("../graph/label_index.zig");
const LabelIndex = label_index.LabelIndex;

const edge_mod = @import("../graph/edge.zig");
const EdgeStore = edge_mod.EdgeStore;

const symbols = @import("../graph/symbols.zig");
const SymbolTable = symbols.SymbolTable;
const SymbolId = symbols.SymbolId;

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
                        const label_id = symbol_table.lookup(node_pattern.labels[0]) catch {
                            return PlannerError.UnknownLabel;
                        } orelse return PlannerError.UnknownLabel;

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
    fn planWhere(self: *Self, where: *const ast.WhereClause, input: ?Operator) PlannerError!Operator {
        const input_op = input orelse return PlannerError.InvalidQuery;

        const filter = filter_ops.Filter.init(self.allocator, input_op, where.condition) catch {
            return PlannerError.OutOfMemory;
        };

        return filter.operator();
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
