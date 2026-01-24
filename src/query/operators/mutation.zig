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
                    continue; // Skip property on eval error (expression may reference missing vars)
                };
                const prop_value = evalResultToPropertyValue(value, self.evaluator.allocator) orelse continue;
                self.database.setNodeProperty(null, node_id, prop.key, prop_value) catch {
                    return OperatorError.StorageError;
                };
            }
        } else {
            // No row - evaluate properties without context (literals only)
            var dummy_row = Row.init();
            for (self.properties) |prop| {
                const value = self.evaluator.evaluate(prop.value_expr, &dummy_row, ctx) catch {
                    continue; // Skip property on eval error (expression may reference missing vars)
                };
                const prop_value = evalResultToPropertyValue(value, self.evaluator.allocator) orelse continue;
                self.database.setNodeProperty(null, node_id, prop.key, prop_value) catch {
                    return OperatorError.StorageError;
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
            .edge_ref => |_| {
                // Edge property support would go here
                // For now, edge properties not fully implemented
                return OperatorError.Unsupported;
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

        // Map expression should be a map literal
        if (self.map_expr.* != .map) {
            return OperatorError.TypeError;
        }

        const map = self.map_expr.map;

        // Clear existing properties
        self.database.clearNodeProperties(null, node_id) catch {
            return OperatorError.StorageError;
        };

        // Set properties from map
        for (map.entries) |entry| {
            const value = self.evaluator.evaluate(entry.value, row, ctx) catch continue;
            const prop_value = evalResultToPropertyValue(value, self.evaluator.allocator) orelse continue;
            self.database.setNodeProperty(null, node_id, entry.key, prop_value) catch continue;
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

        // Map expression should be a map literal
        if (self.map_expr.* != .map) {
            return OperatorError.TypeError;
        }

        const map = self.map_expr.map;

        // Merge: set properties from map (keeps existing, overwrites on conflict)
        for (map.entries) |entry| {
            const value = self.evaluator.evaluate(entry.value, row, ctx) catch continue;
            const prop_value = evalResultToPropertyValue(value, self.evaluator.allocator) orelse continue;
            self.database.setNodeProperty(null, node_id, entry.key, prop_value) catch continue;
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
            .edge_ref => |_| {
                // Edge property support would go here
                return OperatorError.Unsupported;
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
            const node_id = self.mergeNode(ctx, input_row) catch {
                return OperatorError.StorageError;
            };
            output_row.setSlot(self.output_slot, .{ .node_ref = node_id });
            return output_row;
        }

        // No input — single merge
        if (self.merged) return null;
        self.merged = true;

        output_row.clear();
        const node_id = self.mergeNode(ctx, null) catch {
            return OperatorError.StorageError;
        };
        output_row.setSlot(self.output_slot, .{ .node_ref = node_id });
        return output_row;
    }

    fn mergeNode(self: *Self, ctx: *ExecutionContext, row: ?*const Row) !NodeId {
        // Try to find an existing node matching the pattern
        if (self.labels.len > 0) {
            const candidates = self.database.getNodesByLabel(self.labels[0]) catch &[_]NodeId{};
            defer self.allocator.free(candidates);

            for (candidates) |candidate_id| {
                if (self.matchesProperties(candidate_id, ctx, row)) {
                    // ON MATCH: apply on_match properties
                    self.applySetItems(candidate_id, self.on_match_props, ctx, row);
                    return candidate_id;
                }
            }
        }

        // Not found — create the node
        const node_id = try self.database.createNode(null, self.labels);

        // Set the pattern properties on the new node
        var dummy_row = Row.init();
        const eval_row: *const Row = if (row) |r| r else &dummy_row;
        for (self.properties) |prop| {
            const value = self.evaluator.evaluate(prop.value_expr, eval_row, ctx) catch continue;
            const prop_value = evalResultToPropertyValue(value, self.allocator) orelse continue;
            self.database.setNodeProperty(null, node_id, prop.key, prop_value) catch {};
        }

        // ON CREATE: apply on_create properties
        self.applySetItems(node_id, self.on_create_props, ctx, row);

        return node_id;
    }

    fn matchesProperties(self: *Self, node_id: NodeId, ctx: *ExecutionContext, row: ?*const Row) bool {
        if (self.properties.len == 0) return true;

        var dummy_row = Row.init();
        const eval_row: *const Row = if (row) |r| r else &dummy_row;

        for (self.properties) |prop| {
            // Evaluate the expected value from the pattern
            const expected = self.evaluator.evaluate(prop.value_expr, eval_row, ctx) catch return false;
            const expected_pv = evalResultToPropertyValue(expected, self.allocator) orelse return false;

            // Get the actual value from the node
            const actual = self.database.getNodeProperty(node_id, prop.key) catch return false;
            if (actual == null) return false;

            // Compare values
            if (!propertyValuesEqual(expected_pv, actual.?)) {
                // Free allocated string from getNodeProperty if needed
                if (actual.? == .string_val) {
                    self.allocator.free(actual.?.string_val);
                }
                return false;
            }
            if (actual.? == .string_val) {
                self.allocator.free(actual.?.string_val);
            }
        }
        return true;
    }

    fn applySetItems(self: *Self, node_id: NodeId, items: []const CreateNode.PropertyKV, ctx: *ExecutionContext, row: ?*const Row) void {
        var dummy_row = Row.init();
        const eval_row: *const Row = if (row) |r| r else &dummy_row;
        for (items) |prop| {
            const value = self.evaluator.evaluate(prop.value_expr, eval_row, ctx) catch continue;
            const prop_value = evalResultToPropertyValue(value, self.allocator) orelse continue;
            self.database.setNodeProperty(null, node_id, prop.key, prop_value) catch {};
        }
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
