//! Mutation operators for CREATE and DELETE clauses.
//!
//! These operators modify the graph by creating or deleting nodes and edges.
//! They follow the Volcano iterator model but perform side effects.

const std = @import("std");
const Allocator = std.mem.Allocator;

const lattice = @import("lattice");

const executor = @import("../executor.zig");
const Operator = executor.Operator;
const OperatorError = executor.OperatorError;
const Row = executor.Row;
const ExecutionContext = executor.ExecutionContext;
const SlotValue = executor.SlotValue;

const expression_mod = @import("../expression.zig");
const ExpressionEvaluator = expression_mod.ExpressionEvaluator;
const EvalResult = expression_mod.EvalResult;

const ast = @import("../ast.zig");

const types = lattice.core.types;
const NodeId = types.NodeId;
const EdgeId = types.EdgeId;
const PropertyValue = types.PropertyValue;

const database_mod = lattice.storage.database;
const Database = database_mod.Database;

// ============================================================================
// CreateNode Operator
// ============================================================================

/// Operator that creates a node for each input row (or once if no input).
pub const CreateNode = struct {
    /// Optional input operator (for CREATE after MATCH)
    input: ?Operator,
    /// Node pattern with labels and properties
    labels: []const []const u8,
    /// Property expressions (already evaluated to values at planning time)
    properties: []const PropertyKV,
    /// Output slot for the created node ID
    output_slot: u8,
    /// Database reference for mutations
    database: *Database,
    /// Expression evaluator
    evaluator: ExpressionEvaluator,
    /// Whether the operator has been opened
    opened: bool,
    /// Whether we've already created a node (for no-input case)
    created: bool,
    /// Output row
    current_row: ?*Row,
    /// Allocator
    allocator: Allocator,

    /// Property key-value for creation
    pub const PropertyKV = struct {
        key: []const u8,
        value_expr: *const ast.Expression,
    };

    const Self = @This();

    /// Create a new CreateNode operator
    pub fn init(
        allocator: Allocator,
        input: ?Operator,
        labels: []const []const u8,
        properties: []const PropertyKV,
        output_slot: u8,
        database: *Database,
    ) !*Self {
        const self = try allocator.create(Self);
        self.* = Self{
            .input = input,
            .labels = labels,
            .properties = properties,
            .output_slot = output_slot,
            .database = database,
            .evaluator = ExpressionEvaluator.init(allocator),
            .opened = false,
            .created = false,
            .current_row = null,
            .allocator = allocator,
        };
        return self;
    }

    /// Get the Operator interface
    pub fn operator(self: *Self) Operator {
        return Operator{
            .vtable = &vtable,
            .ptr = self,
        };
    }

    const vtable = Operator.VTable{
        .open = open,
        .next = next,
        .close = close,
        .deinit = deinit,
    };

    fn open(ptr: *anyopaque, ctx: *ExecutionContext) OperatorError!void {
        const self: *Self = @ptrCast(@alignCast(ptr));

        // Allocate output row
        self.current_row = ctx.allocRow() catch return OperatorError.OutOfMemory;

        // Open input if present
        if (self.input) |input| {
            try input.open(ctx);
        }

        self.opened = true;
        self.created = false;
    }

    fn next(ptr: *anyopaque, ctx: *ExecutionContext) OperatorError!?*Row {
        const self: *Self = @ptrCast(@alignCast(ptr));

        if (!self.opened) return OperatorError.NotInitialized;

        const output_row = self.current_row orelse return OperatorError.NotInitialized;

        // If we have an input, create a node for each input row
        if (self.input) |input| {
            const input_row = try input.next(ctx) orelse return null;

            // Copy input row to output
            output_row.copyFrom(input_row);

            // Create node and add to output row
            const node_id = self.createNodeWithProperties(ctx, input_row) catch {
                return OperatorError.StorageError;
            };
            output_row.setSlot(self.output_slot, .{ .node_ref = node_id });

            return output_row;
        }

        // No input - create one node (standalone CREATE)
        if (self.created) return null;
        self.created = true;

        output_row.clear();
        const node_id = self.createNodeWithProperties(ctx, null) catch {
            return OperatorError.StorageError;
        };
        output_row.setSlot(self.output_slot, .{ .node_ref = node_id });

        return output_row;
    }

    fn createNodeWithProperties(self: *Self, ctx: *ExecutionContext, row: ?*const Row) !NodeId {
        // Create node with labels
        const node_id = try self.database.createNode(null, self.labels);

        // Set properties (only if we have a row for expression evaluation)
        if (row) |r| {
            for (self.properties) |prop| {
                const value = self.evaluator.evaluate(prop.value_expr, r, ctx) catch {
                    continue; // Skip property on eval error
                };
                const prop_value = evalResultToPropertyValue(value) orelse continue;
                self.database.setNodeProperty(null, node_id, prop.key, prop_value) catch {
                    continue; // Skip property on error
                };
            }
        } else {
            // No row - evaluate properties without context (literals only)
            var dummy_row = Row.init();
            for (self.properties) |prop| {
                const value = self.evaluator.evaluate(prop.value_expr, &dummy_row, ctx) catch {
                    continue; // Skip property on eval error
                };
                const prop_value = evalResultToPropertyValue(value) orelse continue;
                self.database.setNodeProperty(null, node_id, prop.key, prop_value) catch {
                    continue; // Skip property on error
                };
            }
        }

        return node_id;
    }

    fn close(ptr: *anyopaque, ctx: *ExecutionContext) void {
        const self: *Self = @ptrCast(@alignCast(ptr));

        if (self.opened) {
            if (self.input) |input| {
                input.close(ctx);
            }
            self.opened = false;
        }
    }

    fn deinit(ptr: *anyopaque, allocator: Allocator) void {
        const self: *Self = @ptrCast(@alignCast(ptr));

        if (self.input) |input| {
            input.deinit(allocator);
        }

        allocator.destroy(self);
    }
};

// ============================================================================
// CreateEdge Operator
// ============================================================================

/// Operator that creates an edge between nodes in the input row.
pub const CreateEdge = struct {
    /// Input operator providing source and target nodes
    input: Operator,
    /// Slot containing source node ID
    source_slot: u8,
    /// Slot containing target node ID
    target_slot: u8,
    /// Edge type name
    edge_type: []const u8,
    /// Optional slot for edge variable
    output_slot: ?u8,
    /// Database reference
    database: *Database,
    /// Whether operator is opened
    opened: bool,

    const Self = @This();

    /// Create a new CreateEdge operator
    pub fn init(
        allocator: Allocator,
        input: Operator,
        source_slot: u8,
        target_slot: u8,
        edge_type: []const u8,
        output_slot: ?u8,
        database: *Database,
    ) !*Self {
        const self = try allocator.create(Self);
        self.* = Self{
            .input = input,
            .source_slot = source_slot,
            .target_slot = target_slot,
            .edge_type = edge_type,
            .output_slot = output_slot,
            .database = database,
            .opened = false,
        };
        return self;
    }

    /// Get the Operator interface
    pub fn operator(self: *Self) Operator {
        return Operator{
            .vtable = &vtable,
            .ptr = self,
        };
    }

    const vtable = Operator.VTable{
        .open = open,
        .next = next,
        .close = close,
        .deinit = deinit,
    };

    fn open(ptr: *anyopaque, ctx: *ExecutionContext) OperatorError!void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        try self.input.open(ctx);
        self.opened = true;
    }

    fn next(ptr: *anyopaque, ctx: *ExecutionContext) OperatorError!?*Row {
        const self: *Self = @ptrCast(@alignCast(ptr));

        if (!self.opened) return OperatorError.NotInitialized;

        const row = try self.input.next(ctx) orelse return null;

        // Get source and target node IDs from row
        const source_val = row.getSlot(self.source_slot) orelse return OperatorError.UnboundVariable;
        const target_val = row.getSlot(self.target_slot) orelse return OperatorError.UnboundVariable;

        const source_id = source_val.asNodeId() orelse return OperatorError.TypeError;
        const target_id = target_val.asNodeId() orelse return OperatorError.TypeError;

        // Create the edge
        self.database.createEdge(null, source_id, target_id, self.edge_type) catch {
            return OperatorError.StorageError;
        };

        // Edge variable currently not tracked (edges don't have simple IDs)
        // In future, could store edge info in a slot

        return row;
    }

    fn close(ptr: *anyopaque, ctx: *ExecutionContext) void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        if (self.opened) {
            self.input.close(ctx);
            self.opened = false;
        }
    }

    fn deinit(ptr: *anyopaque, allocator: Allocator) void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        self.input.deinit(allocator);
        allocator.destroy(self);
    }
};

// ============================================================================
// DeleteNode Operator
// ============================================================================

/// Operator that deletes a node for each input row.
pub const DeleteNode = struct {
    /// Input operator
    input: Operator,
    /// Slot containing node to delete
    target_slot: u8,
    /// Whether to detach (delete edges first)
    detach: bool,
    /// Database reference
    database: *Database,
    /// Whether operator is opened
    opened: bool,
    /// Allocator for edge info
    allocator: Allocator,

    const Self = @This();

    /// Create a new DeleteNode operator
    pub fn init(
        allocator: Allocator,
        input: Operator,
        target_slot: u8,
        detach: bool,
        database: *Database,
    ) !*Self {
        const self = try allocator.create(Self);
        self.* = Self{
            .input = input,
            .target_slot = target_slot,
            .detach = detach,
            .database = database,
            .opened = false,
            .allocator = allocator,
        };
        return self;
    }

    /// Get the Operator interface
    pub fn operator(self: *Self) Operator {
        return Operator{
            .vtable = &vtable,
            .ptr = self,
        };
    }

    const vtable = Operator.VTable{
        .open = open,
        .next = next,
        .close = close,
        .deinit = deinit,
    };

    fn open(ptr: *anyopaque, ctx: *ExecutionContext) OperatorError!void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        try self.input.open(ctx);
        self.opened = true;
    }

    fn next(ptr: *anyopaque, ctx: *ExecutionContext) OperatorError!?*Row {
        const self: *Self = @ptrCast(@alignCast(ptr));

        if (!self.opened) return OperatorError.NotInitialized;

        const row = try self.input.next(ctx) orelse return null;

        // Get node ID from row
        const node_val = row.getSlot(self.target_slot) orelse return OperatorError.UnboundVariable;
        const node_id = node_val.asNodeId() orelse return OperatorError.TypeError;

        // If detach, delete all connected edges first
        if (self.detach) {
            self.deleteAllEdges(node_id) catch {
                return OperatorError.StorageError;
            };
        }

        // Delete the node
        self.database.deleteNode(null, node_id) catch {
            return OperatorError.StorageError;
        };

        return row;
    }

    fn deleteAllEdges(self: *Self, node_id: NodeId) !void {
        // Delete outgoing edges
        const outgoing = try self.database.getOutgoingEdges(node_id);
        defer self.database.freeEdgeInfos(outgoing);

        for (outgoing) |edge| {
            self.database.deleteEdge(null, edge.source, edge.target, edge.edge_type) catch {};
        }

        // Delete incoming edges
        const incoming = try self.database.getIncomingEdges(node_id);
        defer self.database.freeEdgeInfos(incoming);

        for (incoming) |edge| {
            self.database.deleteEdge(null, edge.source, edge.target, edge.edge_type) catch {};
        }
    }

    fn close(ptr: *anyopaque, ctx: *ExecutionContext) void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        if (self.opened) {
            self.input.close(ctx);
            self.opened = false;
        }
    }

    fn deinit(ptr: *anyopaque, allocator: Allocator) void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        self.input.deinit(allocator);
        allocator.destroy(self);
    }
};

// ============================================================================
// DeleteEdge Operator
// ============================================================================

/// Operator that deletes an edge for each input row.
/// Note: Edge deletion requires knowing source, target, and type.
pub const DeleteEdge = struct {
    /// Input operator
    input: Operator,
    /// Slot containing edge info (source node for now)
    source_slot: u8,
    /// Slot containing target node
    target_slot: u8,
    /// Edge type to delete
    edge_type: []const u8,
    /// Database reference
    database: *Database,
    /// Whether operator is opened
    opened: bool,

    const Self = @This();

    /// Create a new DeleteEdge operator
    pub fn init(
        allocator: Allocator,
        input: Operator,
        source_slot: u8,
        target_slot: u8,
        edge_type: []const u8,
        database: *Database,
    ) !*Self {
        const self = try allocator.create(Self);
        self.* = Self{
            .input = input,
            .source_slot = source_slot,
            .target_slot = target_slot,
            .edge_type = edge_type,
            .database = database,
            .opened = false,
        };
        return self;
    }

    /// Get the Operator interface
    pub fn operator(self: *Self) Operator {
        return Operator{
            .vtable = &vtable,
            .ptr = self,
        };
    }

    const vtable = Operator.VTable{
        .open = open,
        .next = next,
        .close = close,
        .deinit = deinit,
    };

    fn open(ptr: *anyopaque, ctx: *ExecutionContext) OperatorError!void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        try self.input.open(ctx);
        self.opened = true;
    }

    fn next(ptr: *anyopaque, ctx: *ExecutionContext) OperatorError!?*Row {
        const self: *Self = @ptrCast(@alignCast(ptr));

        if (!self.opened) return OperatorError.NotInitialized;

        const row = try self.input.next(ctx) orelse return null;

        // Get source and target from row
        const source_val = row.getSlot(self.source_slot) orelse return OperatorError.UnboundVariable;
        const target_val = row.getSlot(self.target_slot) orelse return OperatorError.UnboundVariable;

        const source_id = source_val.asNodeId() orelse return OperatorError.TypeError;
        const target_id = target_val.asNodeId() orelse return OperatorError.TypeError;

        // Delete the edge
        self.database.deleteEdge(null, source_id, target_id, self.edge_type) catch {
            return OperatorError.StorageError;
        };

        return row;
    }

    fn close(ptr: *anyopaque, ctx: *ExecutionContext) void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        if (self.opened) {
            self.input.close(ctx);
            self.opened = false;
        }
    }

    fn deinit(ptr: *anyopaque, allocator: Allocator) void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        self.input.deinit(allocator);
        allocator.destroy(self);
    }
};

// ============================================================================
// Helpers
// ============================================================================

/// Convert EvalResult to PropertyValue
fn evalResultToPropertyValue(result: EvalResult) ?PropertyValue {
    return switch (result) {
        .null_val => .{ .null_val = {} },
        .bool_val => |b| .{ .bool_val = b },
        .int_val => |i| .{ .int_val = i },
        .float_val => |f| .{ .float_val = f },
        .string_val => |s| .{ .string_val = s },
        .node_ref, .edge_ref => null, // Can't convert refs to property
        .list_val => null, // List conversion not yet supported
    };
}

// ============================================================================
// Tests
// ============================================================================

test "CreateNode basic structure" {
    const vtable = CreateNode.vtable;
    try std.testing.expect(@TypeOf(vtable.open) != void);
    try std.testing.expect(@TypeOf(vtable.next) != void);
    try std.testing.expect(@TypeOf(vtable.close) != void);
    try std.testing.expect(@TypeOf(vtable.deinit) != void);
}

test "DeleteNode basic structure" {
    const vtable = DeleteNode.vtable;
    try std.testing.expect(@TypeOf(vtable.open) != void);
    try std.testing.expect(@TypeOf(vtable.next) != void);
    try std.testing.expect(@TypeOf(vtable.close) != void);
    try std.testing.expect(@TypeOf(vtable.deinit) != void);
}
