//! Mutation operators for CREATE, DELETE, and SET clauses.
//!
//! These operators modify the graph by creating, deleting, or updating nodes and edges.
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
const EvalError = expression_mod.EvalError;

const ast = @import("../ast.zig");

const types = lattice.core.types;
const NodeId = types.NodeId;
const EdgeId = types.EdgeId;
const PropertyValue = types.PropertyValue;
const SymbolError = lattice.graph.symbols.SymbolError;
const EdgeError = lattice.graph.edge.EdgeError;

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
            const node_id = try self.createNodeWithProperties(ctx, input_row);
            output_row.setSlot(self.output_slot, .{ .node_ref = node_id });

            return output_row;
        }

        // No input - create one node (standalone CREATE)
        if (self.created) return null;
        self.created = true;

        output_row.clear();
        const node_id = try self.createNodeWithProperties(ctx, null);
        output_row.setSlot(self.output_slot, .{ .node_ref = node_id });

        return output_row;
    }

    fn createNodeWithProperties(self: *Self, ctx: *ExecutionContext, row: ?*const Row) OperatorError!NodeId {
        var dummy_row = Row.init();
        const eval_row: *const Row = if (row) |r| r else &dummy_row;

        // Evaluate and type-check all properties before creating a node to avoid
        // silent skips and reduce partial writes.
        var evaluated: std.ArrayList(EvaluatedProperty) = .empty;
        defer evaluated.deinit(self.allocator);
        try evaluatePropertyExprList(
            self.allocator,
            &self.evaluator,
            self.properties,
            eval_row,
            ctx,
            &evaluated,
        );

        // Create node with labels
        const node_id = self.database.createNode(null, self.labels) catch {
            return OperatorError.StorageError;
        };

        try applyEvaluatedNodeProperties(self.database, node_id, evaluated.items);
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
    /// Property expressions
    properties: []const CreateNode.PropertyKV,
    /// Optional slot for edge variable
    output_slot: ?u8,
    /// Database reference
    database: *Database,
    /// Expression evaluator
    evaluator: ExpressionEvaluator,
    /// Whether operator is opened
    opened: bool,
    /// Allocator
    allocator: Allocator,

    const Self = @This();

    /// Create a new CreateEdge operator
    pub fn init(
        allocator: Allocator,
        input: Operator,
        source_slot: u8,
        target_slot: u8,
        edge_type: []const u8,
        properties: []const CreateNode.PropertyKV,
        output_slot: ?u8,
        database: *Database,
    ) !*Self {
        const self = try allocator.create(Self);
        self.* = Self{
            .input = input,
            .source_slot = source_slot,
            .target_slot = target_slot,
            .edge_type = edge_type,
            .properties = properties,
            .output_slot = output_slot,
            .database = database,
            .evaluator = ExpressionEvaluator.init(allocator),
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

        // Get source and target node IDs from row
        const source_val = row.getSlot(self.source_slot) orelse return OperatorError.UnboundVariable;
        const target_val = row.getSlot(self.target_slot) orelse return OperatorError.UnboundVariable;

        const source_id = source_val.asNodeId() orelse return OperatorError.TypeError;
        const target_id = target_val.asNodeId() orelse return OperatorError.TypeError;

        // Evaluate edge properties before mutation to avoid partial writes.
        var evaluated: std.ArrayList(EvaluatedProperty) = .empty;
        defer evaluated.deinit(self.allocator);
        try evaluatePropertyExprList(
            self.allocator,
            &self.evaluator,
            self.properties,
            row,
            ctx,
            &evaluated,
        );

        const edge_id = self.database.createEdgeAndGetId(null, source_id, target_id, self.edge_type) catch {
            return OperatorError.StorageError;
        };

        for (evaluated.items) |item| {
            self.database.setEdgePropertyById(null, edge_id, item.key, item.value) catch {
                // Best-effort rollback to avoid exposing partially initialized edges.
                self.database.deleteEdgeById(null, edge_id) catch {};
                return OperatorError.StorageError;
            };
        }

        if (self.output_slot) |slot| {
            row.setSlot(slot, .{ .edge_ref = edge_id });
        }

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
            try self.database.deleteEdge(null, edge.source, edge.target, edge.edge_type);
        }

        // Delete incoming edges
        const incoming = try self.database.getIncomingEdges(node_id);
        defer self.database.freeEdgeInfos(incoming);

        for (incoming) |edge| {
            try self.database.deleteEdge(null, edge.source, edge.target, edge.edge_type);
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
    /// Slot containing edge ID
    edge_slot: u8,
    /// Database reference
    database: *Database,
    /// Whether operator is opened
    opened: bool,

    const Self = @This();

    /// Create a new DeleteEdge operator
    pub fn init(
        allocator: Allocator,
        input: Operator,
        edge_slot: u8,
        database: *Database,
    ) !*Self {
        const self = try allocator.create(Self);
        self.* = Self{
            .input = input,
            .edge_slot = edge_slot,
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

        // Get edge ID from row
        const edge_val = row.getSlot(self.edge_slot) orelse return OperatorError.UnboundVariable;
        const edge_id = edge_val.asEdgeId() orelse return OperatorError.TypeError;

        self.database.deleteEdgeById(null, edge_id) catch {
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
// SetProperty Operator
// ============================================================================

/// Operator that sets a property on a node or edge for each input row.
pub const SetProperty = struct {
    /// Input operator
    input: Operator,
    /// Slot containing target node/edge
    target_slot: u8,
    /// Property name to set
    property_name: []const u8,
    /// Value expression
    value_expr: *const ast.Expression,
    /// Database reference
    database: *Database,
    /// Expression evaluator
    evaluator: ExpressionEvaluator,
    /// Whether operator is opened
    opened: bool,

    const Self = @This();

    /// Create a new SetProperty operator
    pub fn init(
        allocator: Allocator,
        input: Operator,
        target_slot: u8,
        property_name: []const u8,
        value_expr: *const ast.Expression,
        database: *Database,
    ) !*Self {
        const self = try allocator.create(Self);
        self.* = Self{
            .input = input,
            .target_slot = target_slot,
            .property_name = property_name,
            .value_expr = value_expr,
            .database = database,
            .evaluator = ExpressionEvaluator.init(allocator),
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

        // Get target from row
        const target_val = row.getSlot(self.target_slot) orelse return OperatorError.UnboundVariable;

        // Evaluate value expression
        const value = self.evaluator.evaluate(self.value_expr, row, ctx) catch {
            return OperatorError.EvaluationError;
        };

        // Apply property update based on target type
        switch (target_val) {
            .node_ref => |node_id| {
                if (value == .null_val) {
                    // NULL removes property
                    self.database.removeNodeProperty(null, node_id, self.property_name) catch {
                        return OperatorError.StorageError;
                    };
                } else {
                    const prop_value = evalResultToPropertyValue(value, self.evaluator.allocator) orelse return OperatorError.TypeError;
                    self.database.setNodeProperty(null, node_id, self.property_name, prop_value) catch {
                        return OperatorError.StorageError;
                    };
                }
            },
            .edge_ref => |edge_id| {
                if (value == .null_val) {
                    self.database.removeEdgePropertyById(null, edge_id, self.property_name) catch {
                        return OperatorError.StorageError;
                    };
                } else {
                    const prop_value = evalResultToPropertyValue(value, self.evaluator.allocator) orelse return OperatorError.TypeError;
                    self.database.setEdgePropertyById(null, edge_id, self.property_name, prop_value) catch {
                        return OperatorError.StorageError;
                    };
                }
            },
            else => return OperatorError.TypeError,
        }

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
// SetLabels Operator
// ============================================================================

/// Operator that adds labels to a node for each input row.
pub const SetLabels = struct {
    /// Input operator
    input: Operator,
    /// Slot containing target node
    target_slot: u8,
    /// Labels to add
    label_names: []const []const u8,
    /// Database reference
    database: *Database,
    /// Whether operator is opened
    opened: bool,

    const Self = @This();

    /// Create a new SetLabels operator
    pub fn init(
        allocator: Allocator,
        input: Operator,
        target_slot: u8,
        label_names: []const []const u8,
        database: *Database,
    ) !*Self {
        const self = try allocator.create(Self);
        self.* = Self{
            .input = input,
            .target_slot = target_slot,
            .label_names = label_names,
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

        // Get target node from row
        const target_val = row.getSlot(self.target_slot) orelse return OperatorError.UnboundVariable;
        const node_id = target_val.asNodeId() orelse return OperatorError.TypeError;

        // Add each label
        for (self.label_names) |label| {
            self.database.addNodeLabel(null, node_id, label) catch {
                return OperatorError.StorageError;
            };
        }

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
// SetPropertiesReplace Operator
// ============================================================================

/// Operator that replaces all properties on a node (SET n = {map}).
pub const SetPropertiesReplace = struct {
    /// Input operator
    input: Operator,
    /// Slot containing target node
    target_slot: u8,
    /// Map expression
    map_expr: *const ast.Expression,
    /// Database reference
    database: *Database,
    /// Expression evaluator
    evaluator: ExpressionEvaluator,
    /// Whether operator is opened
    opened: bool,

    const Self = @This();

    /// Create a new SetPropertiesReplace operator
    pub fn init(
        allocator: Allocator,
        input: Operator,
        target_slot: u8,
        map_expr: *const ast.Expression,
        database: *Database,
    ) !*Self {
        const self = try allocator.create(Self);
        self.* = Self{
            .input = input,
            .target_slot = target_slot,
            .map_expr = map_expr,
            .database = database,
            .evaluator = ExpressionEvaluator.init(allocator),
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

        // Get target node from row
        const target_val = row.getSlot(self.target_slot) orelse return OperatorError.UnboundVariable;
        const node_id = target_val.asNodeId() orelse return OperatorError.TypeError;

        // Evaluate all updates first so expression/type failures don't clear
        // existing properties or produce partial row-level writes.
        var evaluated: std.ArrayList(EvaluatedProperty) = .empty;
        defer evaluated.deinit(self.evaluator.allocator);
        try evaluateMapExpression(
            self.evaluator.allocator,
            &self.evaluator,
            self.map_expr,
            row,
            ctx,
            &evaluated,
        );

        // Clear existing properties and apply new values.
        self.database.clearNodeProperties(null, node_id) catch {
            return OperatorError.StorageError;
        };
        try applyEvaluatedNodeProperties(self.database, node_id, evaluated.items);

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
// SetPropertiesMerge Operator
// ============================================================================

/// Operator that merges properties on a node (SET n += {map}).
pub const SetPropertiesMerge = struct {
    /// Input operator
    input: Operator,
    /// Slot containing target node
    target_slot: u8,
    /// Map expression
    map_expr: *const ast.Expression,
    /// Database reference
    database: *Database,
    /// Expression evaluator
    evaluator: ExpressionEvaluator,
    /// Whether operator is opened
    opened: bool,

    const Self = @This();

    /// Create a new SetPropertiesMerge operator
    pub fn init(
        allocator: Allocator,
        input: Operator,
        target_slot: u8,
        map_expr: *const ast.Expression,
        database: *Database,
    ) !*Self {
        const self = try allocator.create(Self);
        self.* = Self{
            .input = input,
            .target_slot = target_slot,
            .map_expr = map_expr,
            .database = database,
            .evaluator = ExpressionEvaluator.init(allocator),
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

        // Get target node from row
        const target_val = row.getSlot(self.target_slot) orelse return OperatorError.UnboundVariable;
        const node_id = target_val.asNodeId() orelse return OperatorError.TypeError;

        // Evaluate all updates first so expression/type failures don't produce
        // partial row-level writes.
        var evaluated: std.ArrayList(EvaluatedProperty) = .empty;
        defer evaluated.deinit(self.evaluator.allocator);
        try evaluateMapExpression(
            self.evaluator.allocator,
            &self.evaluator,
            self.map_expr,
            row,
            ctx,
            &evaluated,
        );
        try applyEvaluatedNodeProperties(self.database, node_id, evaluated.items);

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
// RemoveProperty Operator
// ============================================================================

/// Operator that removes a property from a node or edge for each input row.
pub const RemoveProperty = struct {
    /// Input operator
    input: Operator,
    /// Slot containing target node/edge
    target_slot: u8,
    /// Property name to remove
    property_name: []const u8,
    /// Database reference
    database: *Database,
    /// Whether operator is opened
    opened: bool,

    const Self = @This();

    /// Create a new RemoveProperty operator
    pub fn init(
        allocator: Allocator,
        input: Operator,
        target_slot: u8,
        property_name: []const u8,
        database: *Database,
    ) !*Self {
        const self = try allocator.create(Self);
        self.* = Self{
            .input = input,
            .target_slot = target_slot,
            .property_name = property_name,
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

        // Get target from row
        const target_val = row.getSlot(self.target_slot) orelse return OperatorError.UnboundVariable;

        // Remove property based on target type
        switch (target_val) {
            .node_ref => |node_id| {
                self.database.removeNodeProperty(null, node_id, self.property_name) catch {
                    return OperatorError.StorageError;
                };
            },
            .edge_ref => |edge_id| {
                self.database.removeEdgePropertyById(null, edge_id, self.property_name) catch {
                    return OperatorError.StorageError;
                };
            },
            else => return OperatorError.TypeError,
        }

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
// RemoveLabels Operator
// ============================================================================

/// Operator that removes labels from a node for each input row.
pub const RemoveLabels = struct {
    /// Input operator
    input: Operator,
    /// Slot containing target node
    target_slot: u8,
    /// Labels to remove
    label_names: []const []const u8,
    /// Database reference
    database: *Database,
    /// Whether operator is opened
    opened: bool,

    const Self = @This();

    /// Create a new RemoveLabels operator
    pub fn init(
        allocator: Allocator,
        input: Operator,
        target_slot: u8,
        label_names: []const []const u8,
        database: *Database,
    ) !*Self {
        const self = try allocator.create(Self);
        self.* = Self{
            .input = input,
            .target_slot = target_slot,
            .label_names = label_names,
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

        // Get target node from row
        const target_val = row.getSlot(self.target_slot) orelse return OperatorError.UnboundVariable;
        const node_id = target_val.asNodeId() orelse return OperatorError.TypeError;

        // Remove each label
        for (self.label_names) |label| {
            self.database.removeNodeLabel(null, node_id, label) catch {
                return OperatorError.StorageError;
            };
        }

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
fn evalResultToPropertyValue(result: EvalResult, allocator: Allocator) ?PropertyValue {
    return switch (result) {
        .null_val => .{ .null_val = {} },
        .bool_val => |b| .{ .bool_val = b },
        .int_val => |i| .{ .int_val = i },
        .float_val => |f| .{ .float_val = f },
        .string_val => |s| .{ .string_val = s },
        .vector_val => |v| .{ .vector_val = v },
        .node_ref, .edge_ref => null, // Can't convert refs to property
        .list_val, .map_val => result.toPropertyValue(allocator),
    };
}

/// Fully evaluated key/value pair ready for storage writes.
const EvaluatedProperty = struct {
    key: []const u8,
    value: PropertyValue,
};

fn mapEvalError(err: EvalError) OperatorError {
    return switch (err) {
        EvalError.OutOfMemory => OperatorError.OutOfMemory,
        EvalError.TypeError => OperatorError.TypeError,
        EvalError.UnboundVariable => OperatorError.UnboundVariable,
        EvalError.PropertyNotFound => OperatorError.PropertyNotFound,
        else => OperatorError.EvaluationError,
    };
}

fn evaluatePropertyExprList(
    allocator: Allocator,
    evaluator: *ExpressionEvaluator,
    items: []const CreateNode.PropertyKV,
    row: *const Row,
    ctx: *ExecutionContext,
    out: *std.ArrayList(EvaluatedProperty),
) OperatorError!void {
    out.clearRetainingCapacity();
    for (items) |item| {
        const value = evaluator.evaluate(item.value_expr, row, ctx) catch |err| {
            return mapEvalError(err);
        };
        const prop_value = evalResultToPropertyValue(value, allocator) orelse return OperatorError.TypeError;
        out.append(allocator, .{
            .key = item.key,
            .value = prop_value,
        }) catch return OperatorError.OutOfMemory;
    }
}

fn evaluateMapExpression(
    allocator: Allocator,
    evaluator: *ExpressionEvaluator,
    map_expr: *const ast.Expression,
    row: *const Row,
    ctx: *ExecutionContext,
    out: *std.ArrayList(EvaluatedProperty),
) OperatorError!void {
    out.clearRetainingCapacity();
    const map_value = evaluator.evaluate(map_expr, row, ctx) catch |err| {
        return mapEvalError(err);
    };

    const entries = switch (map_value) {
        .map_val => |m| m,
        else => return OperatorError.TypeError,
    };

    for (entries) |entry| {
        const prop_value = evalResultToPropertyValue(entry.value, allocator) orelse return OperatorError.TypeError;
        out.append(allocator, .{
            .key = entry.key,
            .value = prop_value,
        }) catch return OperatorError.OutOfMemory;
    }
}

fn applyEvaluatedNodeProperties(
    database: *Database,
    node_id: NodeId,
    items: []const EvaluatedProperty,
) OperatorError!void {
    for (items) |item| {
        database.setNodeProperty(null, node_id, item.key, item.value) catch {
            return OperatorError.StorageError;
        };
    }
}

// ============================================================================
// MergeNode Operator
// ============================================================================

/// Operator that implements MERGE for a single node pattern.
/// Finds an existing node matching labels and properties, or creates one.
pub const MergeNode = struct {
    /// Optional input operator
    input: ?Operator,
    /// Labels for the node pattern
    labels: []const []const u8,
    /// Property expressions for matching/creating
    properties: []const CreateNode.PropertyKV,
    /// Output slot for the merged node ID
    output_slot: u8,
    /// Database reference
    database: *Database,
    /// Expression evaluator
    evaluator: ExpressionEvaluator,
    /// ON CREATE SET items (property key/value pairs)
    on_create_props: []const CreateNode.PropertyKV,
    /// ON MATCH SET items (property key/value pairs)
    on_match_props: []const CreateNode.PropertyKV,
    /// Whether opened
    opened: bool,
    /// Whether merged (for no-input case)
    merged: bool,
    /// Output row
    current_row: ?*Row,
    /// Allocator
    allocator: Allocator,

    const Self = @This();

    pub fn init(
        allocator: Allocator,
        input: ?Operator,
        labels: []const []const u8,
        properties: []const CreateNode.PropertyKV,
        output_slot: u8,
        database: *Database,
        on_create_props: []const CreateNode.PropertyKV,
        on_match_props: []const CreateNode.PropertyKV,
    ) !*Self {
        const self = try allocator.create(Self);
        self.* = Self{
            .input = input,
            .labels = labels,
            .properties = properties,
            .output_slot = output_slot,
            .database = database,
            .evaluator = ExpressionEvaluator.init(allocator),
            .on_create_props = on_create_props,
            .on_match_props = on_match_props,
            .opened = false,
            .merged = false,
            .current_row = null,
            .allocator = allocator,
        };
        return self;
    }

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
        self.current_row = ctx.allocRow() catch return OperatorError.OutOfMemory;
        if (self.input) |input| {
            try input.open(ctx);
        }
        self.opened = true;
        self.merged = false;
    }

    fn next(ptr: *anyopaque, ctx: *ExecutionContext) OperatorError!?*Row {
        const self: *Self = @ptrCast(@alignCast(ptr));
        if (!self.opened) return OperatorError.NotInitialized;

        const output_row = self.current_row orelse return OperatorError.NotInitialized;

        if (self.input) |input| {
            const input_row = try input.next(ctx) orelse return null;
            output_row.copyFrom(input_row);
            const node_id = try self.mergeNode(ctx, input_row);
            output_row.setSlot(self.output_slot, .{ .node_ref = node_id });
            return output_row;
        }

        // No input — single merge
        if (self.merged) return null;
        self.merged = true;

        output_row.clear();
        const node_id = try self.mergeNode(ctx, null);
        output_row.setSlot(self.output_slot, .{ .node_ref = node_id });
        return output_row;
    }

    fn mergeNode(self: *Self, ctx: *ExecutionContext, row: ?*const Row) OperatorError!NodeId {
        // Try to find an existing node matching the pattern
        if (self.labels.len > 0) {
            const candidates = self.database.getNodesByLabel(self.labels[0]) catch {
                return OperatorError.StorageError;
            };
            defer self.database.allocator.free(candidates);

            for (candidates) |candidate_id| {
                if (try self.matchesProperties(candidate_id, ctx, row)) {
                    // ON MATCH: apply on_match properties
                    try self.applySetItems(candidate_id, self.on_match_props, ctx, row);
                    return candidate_id;
                }
            }
        }

        var dummy_row = Row.init();
        const eval_row: *const Row = if (row) |r| r else &dummy_row;

        // Evaluate pattern properties before create to avoid silent skips and
        // accidental creation of partially initialized nodes.
        var evaluated_pattern: std.ArrayList(EvaluatedProperty) = .empty;
        defer evaluated_pattern.deinit(self.allocator);
        try evaluatePropertyExprList(
            self.allocator,
            &self.evaluator,
            self.properties,
            eval_row,
            ctx,
            &evaluated_pattern,
        );

        // Not found — create the node
        const node_id = self.database.createNode(null, self.labels) catch {
            return OperatorError.StorageError;
        };

        // Set the pattern properties on the new node
        try applyEvaluatedNodeProperties(self.database, node_id, evaluated_pattern.items);

        // ON CREATE: apply on_create properties
        try self.applySetItems(node_id, self.on_create_props, ctx, row);

        return node_id;
    }

    fn matchesProperties(self: *Self, node_id: NodeId, ctx: *ExecutionContext, row: ?*const Row) OperatorError!bool {
        if (self.properties.len == 0) return true;

        var dummy_row = Row.init();
        const eval_row: *const Row = if (row) |r| r else &dummy_row;

        for (self.properties) |prop| {
            // Evaluate the expected value from the pattern
            const expected = self.evaluator.evaluate(prop.value_expr, eval_row, ctx) catch |err| {
                return mapEvalError(err);
            };
            const expected_pv = evalResultToPropertyValue(expected, self.allocator) orelse return OperatorError.TypeError;

            // Get the actual value from the node
            const actual = self.database.getNodeProperty(node_id, prop.key) catch {
                return OperatorError.StorageError;
            };
            if (actual == null) return false;
            var actual_val = actual.?;
            defer actual_val.deinit(self.database.allocator);

            // Compare values
            if (!propertyValuesEqual(expected_pv, actual_val)) {
                return false;
            }
        }
        return true;
    }

    fn applySetItems(self: *Self, node_id: NodeId, items: []const CreateNode.PropertyKV, ctx: *ExecutionContext, row: ?*const Row) OperatorError!void {
        var dummy_row = Row.init();
        const eval_row: *const Row = if (row) |r| r else &dummy_row;
        var evaluated: std.ArrayList(EvaluatedProperty) = .empty;
        defer evaluated.deinit(self.allocator);
        try evaluatePropertyExprList(
            self.allocator,
            &self.evaluator,
            items,
            eval_row,
            ctx,
            &evaluated,
        );
        try applyEvaluatedNodeProperties(self.database, node_id, evaluated.items);
    }

    fn close(ptr: *anyopaque, ctx: *ExecutionContext) void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        if (self.opened) {
            if (self.input) |input| input.close(ctx);
            self.opened = false;
        }
    }

    fn deinit(ptr: *anyopaque, allocator: Allocator) void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        if (self.input) |input| input.deinit(allocator);
        allocator.destroy(self);
    }
};

// ============================================================================
// MergeEdge Operator
// ============================================================================

/// Operator that implements MERGE for a single relationship pattern.
/// Finds an existing edge matching source/target/type/(optional properties), or creates one.
pub const MergeEdge = struct {
    /// Input operator providing source/target node bindings
    input: Operator,
    /// Slot containing source node ID
    source_slot: u8,
    /// Slot containing target node ID
    target_slot: u8,
    /// Edge type name
    edge_type: []const u8,
    /// Optional slot for merged edge ID
    output_slot: ?u8,
    /// Database reference
    database: *Database,
    /// Pattern properties used for matching and initial create
    properties: []const CreateNode.PropertyKV,
    /// ON CREATE SET properties
    on_create_props: []const CreateNode.PropertyKV,
    /// ON MATCH SET properties
    on_match_props: []const CreateNode.PropertyKV,
    /// Expression evaluator
    evaluator: ExpressionEvaluator,
    /// Whether opened
    opened: bool,
    /// Allocator
    allocator: Allocator,

    const Self = @This();

    pub fn init(
        allocator: Allocator,
        input: Operator,
        source_slot: u8,
        target_slot: u8,
        edge_type: []const u8,
        output_slot: ?u8,
        database: *Database,
        properties: []const CreateNode.PropertyKV,
        on_create_props: []const CreateNode.PropertyKV,
        on_match_props: []const CreateNode.PropertyKV,
    ) !*Self {
        const self = try allocator.create(Self);
        self.* = Self{
            .input = input,
            .source_slot = source_slot,
            .target_slot = target_slot,
            .edge_type = edge_type,
            .output_slot = output_slot,
            .database = database,
            .properties = properties,
            .on_create_props = on_create_props,
            .on_match_props = on_match_props,
            .evaluator = ExpressionEvaluator.init(allocator),
            .opened = false,
            .allocator = allocator,
        };
        return self;
    }

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

        const source_val = row.getSlot(self.source_slot) orelse return OperatorError.UnboundVariable;
        const target_val = row.getSlot(self.target_slot) orelse return OperatorError.UnboundVariable;

        const source_id = source_val.asNodeId() orelse return OperatorError.TypeError;
        const target_id = target_val.asNodeId() orelse return OperatorError.TypeError;

        const edge_id = try self.mergeEdge(ctx, row, source_id, target_id);
        if (self.output_slot) |slot| {
            row.setSlot(slot, .{ .edge_ref = edge_id });
        }

        return row;
    }

    fn mergeEdge(
        self: *Self,
        ctx: *ExecutionContext,
        row: *const Row,
        source_id: NodeId,
        target_id: NodeId,
    ) OperatorError!EdgeId {
        if (try self.findMatchingEdgeId(ctx, row, source_id, target_id)) |edge_id| {
            try self.applyEdgeSetItems(edge_id, self.on_match_props, ctx, row);
            return edge_id;
        }

        const edge_id = self.database.createEdgeAndGetId(null, source_id, target_id, self.edge_type) catch {
            return OperatorError.StorageError;
        };

        // MERGE pattern properties initialize newly created relationships.
        try self.applyEdgeSetItems(edge_id, self.properties, ctx, row);
        try self.applyEdgeSetItems(edge_id, self.on_create_props, ctx, row);

        return edge_id;
    }

    fn findMatchingEdgeId(
        self: *Self,
        ctx: *ExecutionContext,
        row: *const Row,
        source_id: NodeId,
        target_id: NodeId,
    ) OperatorError!?EdgeId {
        const type_id = self.database.symbol_table.lookup(self.edge_type) catch |err| {
            return switch (err) {
                SymbolError.NotFound => null,
                else => OperatorError.StorageError,
            };
        };

        if (self.properties.len == 0) {
            var edge = self.database.edge_store.get(source_id, target_id, type_id) catch |err| {
                return switch (err) {
                    EdgeError.NotFound => null,
                    else => OperatorError.StorageError,
                };
            };
            defer edge.deinit(self.database.allocator);
            return edge.id;
        }

        var refs = self.database.edge_store.getOutgoingRefsByType(source_id, type_id) catch {
            return OperatorError.StorageError;
        };
        defer refs.deinit();

        while (true) {
            const maybe_ref = refs.next() catch return OperatorError.StorageError;
            const edge_ref = maybe_ref orelse break;
            if (edge_ref.target != target_id) continue;
            if (try self.matchesPatternProperties(edge_ref.id, ctx, row)) {
                return edge_ref.id;
            }
        }
        return null;
    }

    fn matchesPatternProperties(
        self: *Self,
        edge_id: EdgeId,
        ctx: *ExecutionContext,
        row: *const Row,
    ) OperatorError!bool {
        if (self.properties.len == 0) return true;

        var edge = self.database.edge_store.getById(edge_id) catch {
            return OperatorError.StorageError;
        };
        defer edge.deinit(self.database.allocator);

        for (self.properties) |prop| {
            const expected = self.evaluator.evaluate(prop.value_expr, row, ctx) catch |err| {
                return mapEvalError(err);
            };
            const expected_pv = evalResultToPropertyValue(expected, self.allocator) orelse return OperatorError.TypeError;

            const key_id = self.database.symbol_table.lookup(prop.key) catch |err| {
                return switch (err) {
                    SymbolError.NotFound => false,
                    else => OperatorError.StorageError,
                };
            };

            var found = false;
            for (edge.properties) |actual_prop| {
                if (actual_prop.key_id != key_id) continue;
                if (!propertyValuesEqual(expected_pv, actual_prop.value)) {
                    return false;
                }
                found = true;
                break;
            }
            if (!found) return false;
        }
        return true;
    }

    fn applyEdgeSetItems(
        self: *Self,
        edge_id: EdgeId,
        items: []const CreateNode.PropertyKV,
        ctx: *ExecutionContext,
        row: *const Row,
    ) OperatorError!void {
        if (items.len == 0) return;

        var evaluated: std.ArrayList(EvaluatedProperty) = .empty;
        defer evaluated.deinit(self.allocator);
        try evaluatePropertyExprList(
            self.allocator,
            &self.evaluator,
            items,
            row,
            ctx,
            &evaluated,
        );

        for (evaluated.items) |item| {
            self.database.setEdgePropertyById(null, edge_id, item.key, item.value) catch {
                return OperatorError.StorageError;
            };
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

/// Compare two PropertyValue values for equality
fn propertyValuesEqual(a: PropertyValue, b: PropertyValue) bool {
    return switch (a) {
        .null_val => b == .null_val,
        .bool_val => |av| switch (b) {
            .bool_val => |bv| av == bv,
            else => false,
        },
        .int_val => |av| switch (b) {
            .int_val => |bv| av == bv,
            else => false,
        },
        .float_val => |av| switch (b) {
            .float_val => |bv| av == bv,
            else => false,
        },
        .string_val => |av| switch (b) {
            .string_val => |bv| std.mem.eql(u8, av, bv),
            else => false,
        },
        else => false,
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

test "evalResultToPropertyValue scalar types" {
    const allocator = std.testing.allocator;

    const null_pv = evalResultToPropertyValue(.{ .null_val = {} }, allocator);
    try std.testing.expect(null_pv != null);
    try std.testing.expect(null_pv.? == .null_val);

    const bool_pv = evalResultToPropertyValue(.{ .bool_val = true }, allocator);
    try std.testing.expectEqual(true, bool_pv.?.bool_val);

    const int_pv = evalResultToPropertyValue(.{ .int_val = 99 }, allocator);
    try std.testing.expectEqual(@as(i64, 99), int_pv.?.int_val);

    const float_pv = evalResultToPropertyValue(.{ .float_val = 1.5 }, allocator);
    try std.testing.expectApproxEqAbs(@as(f64, 1.5), float_pv.?.float_val, 0.001);

    const str_pv = evalResultToPropertyValue(.{ .string_val = "test" }, allocator);
    try std.testing.expectEqualStrings("test", str_pv.?.string_val);
}

test "evalResultToPropertyValue returns null for node/edge refs" {
    const allocator = std.testing.allocator;

    const node_pv = evalResultToPropertyValue(.{ .node_ref = 42 }, allocator);
    try std.testing.expect(node_pv == null);

    const edge_pv = evalResultToPropertyValue(.{ .edge_ref = 7 }, allocator);
    try std.testing.expect(edge_pv == null);
}

test "evalResultToPropertyValue converts vector" {
    const allocator = std.testing.allocator;

    const vec = [_]f32{ 1.0, 2.0, 3.0 };
    const pv = evalResultToPropertyValue(.{ .vector_val = &vec }, allocator);

    try std.testing.expect(pv != null);
    try std.testing.expectEqual(@as(usize, 3), pv.?.vector_val.len);
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), pv.?.vector_val[0], 0.001);
    try std.testing.expectApproxEqAbs(@as(f32, 2.0), pv.?.vector_val[1], 0.001);
    try std.testing.expectApproxEqAbs(@as(f32, 3.0), pv.?.vector_val[2], 0.001);
}

test "evalResultToPropertyValue converts list" {
    const allocator = std.testing.allocator;

    const items = [_]EvalResult{
        .{ .int_val = 1 },
        .{ .int_val = 2 },
        .{ .string_val = "three" },
    };
    const pv = evalResultToPropertyValue(.{ .list_val = &items }, allocator);
    defer allocator.free(pv.?.list_val);

    try std.testing.expect(pv != null);
    try std.testing.expectEqual(@as(usize, 3), pv.?.list_val.len);
    try std.testing.expectEqual(@as(i64, 1), pv.?.list_val[0].int_val);
    try std.testing.expectEqual(@as(i64, 2), pv.?.list_val[1].int_val);
    try std.testing.expectEqualStrings("three", pv.?.list_val[2].string_val);
}

test "evalResultToPropertyValue converts map" {
    const allocator = std.testing.allocator;

    const entries = [_]EvalResult.MapEntry{
        .{ .key = "name", .value = .{ .string_val = "Alice" } },
        .{ .key = "score", .value = .{ .int_val = 95 } },
    };
    const pv = evalResultToPropertyValue(.{ .map_val = &entries }, allocator);
    defer allocator.free(pv.?.map_val);

    try std.testing.expect(pv != null);
    try std.testing.expectEqual(@as(usize, 2), pv.?.map_val.len);
    try std.testing.expectEqualStrings("name", pv.?.map_val[0].key);
    try std.testing.expectEqualStrings("Alice", pv.?.map_val[0].value.string_val);
    try std.testing.expectEqualStrings("score", pv.?.map_val[1].key);
    try std.testing.expectEqual(@as(i64, 95), pv.?.map_val[1].value.int_val);
}
