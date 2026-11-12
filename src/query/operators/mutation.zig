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
const scan_ops = @import("scan.zig");

const types = lattice.core.types;
const NodeId = types.NodeId;
const EdgeId = types.EdgeId;
const PropertyValue = types.PropertyValue;
const SymbolError = lattice.graph.symbols.SymbolError;
const SymbolId = lattice.graph.symbols.SymbolId;
const Transaction = lattice.transaction.manager.Transaction;

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
        const node_id = self.database.createNode(ctx.txn, self.labels) catch |err|
            return ctx.storageFailure(err, "creating a node", .{});

        try applyEvaluatedNodeProperties(self.database, ctx, node_id, evaluated.items);
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

        const source_id = source_val.asNodeId() orelse return endpointFailure(ctx, "CREATE", source_val);
        const target_id = target_val.asNodeId() orelse return endpointFailure(ctx, "CREATE", target_val);

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

        const edge_id = self.database.createEdgeAndGetId(ctx.txn, source_id, target_id, self.edge_type) catch |err|
            return ctx.storageFailure(err, "creating a relationship from node {d} to node {d}", .{ source_id, target_id });

        for (evaluated.items) |item| {
            self.database.setEdgePropertyById(ctx.txn, edge_id, item.key, item.value) catch |err| {
                // Best-effort rollback to avoid exposing partially initialized
                // edges. The query fails either way, with the first error.
                self.database.deleteEdgeById(ctx.txn, edge_id) catch {};
                return ctx.storageFailure(err, "setting property '{s}' of relationship {d}", .{ item.key, edge_id });
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
///
/// Plain DELETE refuses a node that still has relationships, as Cypher does,
/// rather than leave edges pointing at a node that no longer exists. The
/// check sees the whole statement: the operator drains its input first, so
/// `MATCH (n)-[r]-() DELETE r, n` deletes every `r` (an earlier operator in
/// the chain) before any node is checked. Only then are nodes deleted and
/// rows returned. DETACH DELETE removes the node's relationships first.
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
    /// Input rows, held until every node has been checked and deleted
    rows: std.ArrayList(Row) = .empty,
    /// Whether the deletes have run
    done: bool = false,
    /// Next row to return
    output_index: usize = 0,
    /// Where the query names the node, for the error that refuses it
    location: ?ast.SourceLocation = null,

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
        self.rows.clearRetainingCapacity();
        self.done = false;
        self.output_index = 0;
    }

    fn next(ptr: *anyopaque, ctx: *ExecutionContext) OperatorError!?*Row {
        const self: *Self = @ptrCast(@alignCast(ptr));

        if (!self.opened) return OperatorError.NotInitialized;

        if (!self.done) {
            try self.deleteAll(ctx);
            self.done = true;
        }
        if (self.output_index >= self.rows.items.len) return null;
        self.output_index += 1;
        return &self.rows.items[self.output_index - 1];
    }

    fn deleteAll(self: *Self, ctx: *ExecutionContext) OperatorError!void {
        while (try self.input.next(ctx)) |row| {
            self.rows.append(self.allocator, row.*) catch return OperatorError.OutOfMemory;
        }

        var seen: std.AutoHashMapUnmanaged(NodeId, void) = .{};
        defer seen.deinit(self.allocator);
        var targets: std.ArrayList(NodeId) = .empty;
        defer targets.deinit(self.allocator);
        for (self.rows.items) |*row| {
            const node_val = row.getSlot(self.target_slot) orelse return OperatorError.UnboundVariable;
            // Deleting null is a no-op, as in Cypher.
            if (node_val == .property and node_val.property == .null_val) continue;
            const node_id = node_val.asNodeId() orelse return OperatorError.TypeError;
            const entry = seen.getOrPut(self.allocator, node_id) catch return OperatorError.OutOfMemory;
            if (entry.found_existing) continue;
            targets.append(self.allocator, node_id) catch return OperatorError.OutOfMemory;
        }

        // The rule is the database's: deleteNode refuses a node that still
        // has relationships and detachDeleteNode removes them with it.
        if (self.detach) {
            for (targets.items) |node_id| {
                self.database.detachDeleteNode(ctx.txn, node_id) catch |err|
                    return ctx.storageFailure(err, "deleting node {d} and its relationships", .{node_id});
            }
            return;
        }

        // Check every node before deleting any, so a refusal leaves the
        // nodes in place.
        for (targets.items) |node_id| {
            const has_relationships = self.database.nodeHasRelationshipsInTxn(ctx.txn, node_id) catch |err|
                return ctx.storageFailure(err, "reading the relationships of node {d}", .{node_id});
            if (has_relationships) return self.hasRelationshipsFailure(ctx, node_id);
        }

        for (targets.items) |node_id| {
            self.database.deleteNode(ctx.txn, node_id) catch |err| return switch (err) {
                error.NodeHasRelationships => self.hasRelationshipsFailure(ctx, node_id),
                else => ctx.storageFailure(err, "deleting node {d}", .{node_id}),
            };
        }
    }

    fn hasRelationshipsFailure(self: *const Self, ctx: *ExecutionContext, node_id: NodeId) OperatorError {
        return ctx.fail(
            OperatorError.NodeHasRelationships,
            self.location,
            "Cannot delete node {d} because it still has relationships; delete them first, or use DETACH DELETE",
            .{node_id},
        );
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
        self.rows.deinit(self.allocator);
        allocator.destroy(self);
    }
};

// ============================================================================
// DeleteEdge Operator
// ============================================================================

/// Operator that deletes an edge for each input row. The edge's id is all
/// it needs; deleting one already deleted by an earlier row is not an error.
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
        // Deleting null, an OPTIONAL MATCH that found nothing, does nothing.
        if (edge_val.isNull()) return row;
        const edge_id = edge_val.asEdgeId() orelse return OperatorError.TypeError;

        self.database.deleteEdgeById(ctx.txn, edge_id) catch |err| switch (err) {
            // The edge was matched, so it existed; an earlier row of this
            // statement deleted it. An undirected pattern lists every edge
            // twice, and deleting it twice is not an error in Cypher.
            error.NotFound => {},
            else => return ctx.storageFailure(err, "deleting relationship {d}", .{edge_id}),
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
        // Writing to null, an OPTIONAL MATCH that found nothing, does nothing.
        if (target_val.isNull()) return row;

        // Evaluate value expression
        const value = self.evaluator.evaluate(self.value_expr, row, ctx) catch {
            return OperatorError.EvaluationError;
        };

        // Apply property update based on target type
        switch (target_val) {
            .node_ref => |node_id| {
                if (value == .null_val) {
                    // NULL removes property
                    self.database.removeNodeProperty(ctx.txn, node_id, self.property_name) catch |err|
                        return ctx.storageFailure(err, "removing property '{s}' of node {d}", .{ self.property_name, node_id });
                } else {
                    const prop_value = (evalResultToPropertyValue(value, self.evaluator.allocator) catch return OperatorError.OutOfMemory) orelse return OperatorError.TypeError;
                    self.database.setNodeProperty(ctx.txn, node_id, self.property_name, prop_value) catch |err|
                        return ctx.storageFailure(err, "setting property '{s}' of node {d}", .{ self.property_name, node_id });
                }
            },
            .edge_ref => |edge_id| {
                if (value == .null_val) {
                    self.database.removeEdgePropertyById(ctx.txn, edge_id, self.property_name) catch |err|
                        return ctx.storageFailure(err, "removing property '{s}' of relationship {d}", .{ self.property_name, edge_id });
                } else {
                    const prop_value = (evalResultToPropertyValue(value, self.evaluator.allocator) catch return OperatorError.OutOfMemory) orelse return OperatorError.TypeError;
                    self.database.setEdgePropertyById(ctx.txn, edge_id, self.property_name, prop_value) catch |err|
                        return ctx.storageFailure(err, "setting property '{s}' of relationship {d}", .{ self.property_name, edge_id });
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
        // Writing to null, an OPTIONAL MATCH that found nothing, does nothing.
        if (target_val.isNull()) return row;
        const node_id = target_val.asNodeId() orelse return OperatorError.TypeError;

        // Add each label
        for (self.label_names) |label| {
            self.database.addNodeLabel(ctx.txn, node_id, label) catch |err|
                return ctx.storageFailure(err, "adding label '{s}' to node {d}", .{ label, node_id });
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

/// Operator that replaces all properties on a node/edge (SET x = {map}).
pub const SetPropertiesReplace = struct {
    /// Input operator
    input: Operator,
    /// Slot containing target node/edge
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

        // Get target from row
        const target_val = row.getSlot(self.target_slot) orelse return OperatorError.UnboundVariable;
        // Writing to null, an OPTIONAL MATCH that found nothing, does nothing.
        if (target_val.isNull()) return row;

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

        switch (target_val) {
            .node_ref => |node_id| {
                // Clear existing properties and apply new values.
                self.database.clearNodeProperties(ctx.txn, node_id) catch |err|
                    return ctx.storageFailure(err, "clearing the properties of node {d}", .{node_id});
                try applyEvaluatedNodeProperties(self.database, ctx, node_id, evaluated.items);
            },
            .edge_ref => |edge_id| {
                try clearEdgeProperties(self.database, ctx, edge_id);
                try applyEvaluatedEdgeProperties(self.database, ctx, edge_id, evaluated.items);
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
// SetPropertiesMerge Operator
// ============================================================================

/// Operator that merges properties on a node/edge (SET x += {map}).
pub const SetPropertiesMerge = struct {
    /// Input operator
    input: Operator,
    /// Slot containing target node/edge
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

        // Get target from row
        const target_val = row.getSlot(self.target_slot) orelse return OperatorError.UnboundVariable;
        // Writing to null, an OPTIONAL MATCH that found nothing, does nothing.
        if (target_val.isNull()) return row;

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
        switch (target_val) {
            .node_ref => |node_id| try applyEvaluatedNodeProperties(self.database, ctx, node_id, evaluated.items),
            .edge_ref => |edge_id| try applyEvaluatedEdgeProperties(self.database, ctx, edge_id, evaluated.items),
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
        // Writing to null, an OPTIONAL MATCH that found nothing, does nothing.
        if (target_val.isNull()) return row;

        // Remove property based on target type
        switch (target_val) {
            .node_ref => |node_id| {
                self.database.removeNodeProperty(ctx.txn, node_id, self.property_name) catch |err|
                    return ctx.storageFailure(err, "removing property '{s}' of node {d}", .{ self.property_name, node_id });
            },
            .edge_ref => |edge_id| {
                self.database.removeEdgePropertyById(ctx.txn, edge_id, self.property_name) catch |err|
                    return ctx.storageFailure(err, "removing property '{s}' of relationship {d}", .{ self.property_name, edge_id });
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
        // Writing to null, an OPTIONAL MATCH that found nothing, does nothing.
        if (target_val.isNull()) return row;
        const node_id = target_val.asNodeId() orelse return OperatorError.TypeError;

        // Remove each label
        for (self.label_names) |label| {
            self.database.removeNodeLabel(ctx.txn, node_id, label) catch |err|
                return ctx.storageFailure(err, "removing label '{s}' from node {d}", .{ label, node_id });
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

/// A relationship CREATE or MERGE cannot write because an end is not a node.
/// openCypher makes a null end an error rather than skipping the row: the
/// relationship the query asks for cannot exist.
fn endpointFailure(ctx: *const ExecutionContext, comptime clause: []const u8, value: SlotValue) OperatorError {
    @branchHint(.cold);
    if (value.isNull()) {
        return ctx.fail(OperatorError.TypeError, null, clause ++ " needs a node at each end of a relationship, but one end is null (an OPTIONAL MATCH that found nothing leaves its variables null)", .{});
    }
    return ctx.fail(OperatorError.TypeError, null, clause ++ " needs a node at each end of a relationship, but one end is not a node", .{});
}

/// Convert EvalResult to PropertyValue
fn evalResultToPropertyValue(result: EvalResult, allocator: Allocator) Allocator.Error!?PropertyValue {
    return switch (result) {
        .null_val => .{ .null_val = {} },
        .bool_val => |b| .{ .bool_val = b },
        .int_val => |i| .{ .int_val = i },
        .float_val => |f| .{ .float_val = f },
        .string_val => |s| .{ .string_val = s },
        .bytes_val => |b| .{ .bytes_val = b },
        .vector_val => |v| .{ .vector_val = v },
        .node_ref, .edge_ref, .path_val => null, // Can't convert refs to property
        .list_val, .map_val => try result.toPropertyValue(allocator),
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
        EvalError.StorageError => OperatorError.StorageError,
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
        const prop_value = (evalResultToPropertyValue(value, allocator) catch return OperatorError.OutOfMemory) orelse return OperatorError.TypeError;
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
        const prop_value = (evalResultToPropertyValue(entry.value, allocator) catch return OperatorError.OutOfMemory) orelse return OperatorError.TypeError;
        out.append(allocator, .{
            .key = entry.key,
            .value = prop_value,
        }) catch return OperatorError.OutOfMemory;
    }
}

fn applyEvaluatedNodeProperties(
    database: *Database,
    ctx: *const ExecutionContext,
    node_id: NodeId,
    items: []const EvaluatedProperty,
) OperatorError!void {
    for (items) |item| {
        database.setNodeProperty(ctx.txn, node_id, item.key, item.value) catch |err|
            return ctx.storageFailure(err, "setting property '{s}' of node {d}", .{ item.key, node_id });
    }
}

fn applyEvaluatedEdgeProperties(
    database: *Database,
    ctx: *const ExecutionContext,
    edge_id: EdgeId,
    items: []const EvaluatedProperty,
) OperatorError!void {
    for (items) |item| {
        database.setEdgePropertyById(ctx.txn, edge_id, item.key, item.value) catch |err|
            return ctx.storageFailure(err, "setting property '{s}' of relationship {d}", .{ item.key, edge_id });
    }
}

fn clearEdgeProperties(database: *Database, ctx: *const ExecutionContext, edge_id: EdgeId) OperatorError!void {
    const props = database.getEdgePropertiesInTxn(ctx.txn, edge_id) catch |err|
        return ctx.storageFailure(err, "reading the properties of relationship {d}", .{edge_id});
    defer database.freePropertyEntries(props);

    for (props) |prop| {
        database.removeEdgePropertyById(ctx.txn, edge_id, prop.key) catch |err|
            return ctx.storageFailure(err, "removing property '{s}' of relationship {d}", .{ prop.key, edge_id });
    }
}

// ============================================================================
// MergeNode Operator
// ============================================================================

/// Operator that implements MERGE for a single node pattern.
///
/// Matches every node that carries all of the pattern's labels and has every
/// pattern property equal to its value, and emits one row per match. A
/// pattern without labels considers every node. Only when nothing matches is
/// a node created, with all the labels and properties. A pattern property
/// whose value is null is an error, as in Cypher: no node could ever match
/// it, so the merge would create a new node on every run.
///
/// Candidates come from a property index on one of the labels when one
/// exists, otherwise from the label index, otherwise from every node.
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
    /// The input row the pending matches belong to
    pending_input: ?*Row,
    /// Matched nodes not yet emitted for the current input row
    pending: std.ArrayList(NodeId),
    pending_index: usize,
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
            .pending_input = null,
            .pending = .empty,
            .pending_index = 0,
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

        while (true) {
            if (self.pending_index < self.pending.items.len) {
                const node_id = self.pending.items[self.pending_index];
                self.pending_index += 1;
                output_row.clear();
                if (self.pending_input) |input_row| output_row.copyFrom(input_row);
                output_row.setSlot(self.output_slot, .{ .node_ref = node_id });
                return output_row;
            }

            var input_row: ?*Row = null;
            if (self.input) |input| {
                input_row = try input.next(ctx) orelse return null;
            } else {
                // No input: merge once.
                if (self.merged) return null;
                self.merged = true;
            }
            self.pending_input = input_row;
            self.pending.clearRetainingCapacity();
            self.pending_index = 0;
            try self.mergeNode(ctx, input_row);
        }
    }

    /// Find every node the pattern matches, applying ON MATCH to each, or
    /// create one and apply ON CREATE. Leaves the nodes in `pending`.
    fn mergeNode(self: *Self, ctx: *ExecutionContext, row: ?*const Row) OperatorError!void {
        var dummy_row = Row.init();
        const eval_row: *const Row = if (row) |r| r else &dummy_row;

        // Evaluate the pattern properties once: they drive both the match and
        // the create.
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
        for (evaluated_pattern.items) |prop| {
            if (prop.value == .null_val) return ctx.fail(
                OperatorError.EvaluationError,
                null,
                "MERGE cannot match a null property value: '{s}' is null; use MATCH, or give it a value",
                .{prop.key},
            );
        }

        try self.findMatches(ctx, evaluated_pattern.items);

        if (self.pending.items.len > 0) {
            for (self.pending.items) |node_id| {
                try self.applySetItems(node_id, self.on_match_props, ctx, row);
            }
            return;
        }

        // Not found — create the node
        const node_id = self.database.createNode(ctx.txn, self.labels) catch |err|
            return ctx.storageFailure(err, "creating a node", .{});
        try applyEvaluatedNodeProperties(self.database, ctx, node_id, evaluated_pattern.items);
        try self.applySetItems(node_id, self.on_create_props, ctx, row);
        self.pending.append(self.allocator, node_id) catch return OperatorError.OutOfMemory;
    }

    /// Put every visible node with all the labels and properties in `pending`.
    fn findMatches(self: *Self, ctx: *ExecutionContext, props: []const EvaluatedProperty) OperatorError!void {
        const symbol_table = &self.database.symbol_table;

        // A label or property key the database has never seen cannot be on
        // any node, so nothing matches.
        const label_ids = self.allocator.alloc(SymbolId, self.labels.len) catch return OperatorError.OutOfMemory;
        defer self.allocator.free(label_ids);
        for (self.labels, label_ids) |label, *id| {
            id.* = symbol_table.lookup(label) catch |err| switch (err) {
                SymbolError.NotFound => return,
                else => return ctx.storageFailure(err, "looking up a label or property key of the MERGE pattern", .{}),
            };
        }
        const key_ids = self.allocator.alloc(SymbolId, props.len) catch return OperatorError.OutOfMemory;
        defer self.allocator.free(key_ids);
        for (props, key_ids) |prop, *id| {
            id.* = symbol_table.lookup(prop.key) catch |err| switch (err) {
                SymbolError.NotFound => return,
                else => return ctx.storageFailure(err, "looking up a label or property key of the MERGE pattern", .{}),
            };
        }

        const candidates = try self.candidateIds(ctx, props);
        defer self.database.allocator.free(candidates);

        for (candidates) |candidate_id| {
            var node = (self.database.getNodeInTxn(ctx.txn, candidate_id) catch |err|
                return ctx.storageFailure(err, "reading node {d}", .{candidate_id})) orelse continue;
            defer node.deinit(self.database.allocator);
            if (!nodeMatches(node, label_ids, props, key_ids)) continue;
            self.pending.append(self.allocator, candidate_id) catch return OperatorError.OutOfMemory;
        }
    }

    /// The smallest candidate set available: a property index lookup, the
    /// first label's nodes, or every node. Owned by the database allocator.
    fn candidateIds(self: *Self, ctx: *ExecutionContext, props: []const EvaluatedProperty) OperatorError![]NodeId {
        for (self.labels) |label| {
            for (props) |prop| {
                const indexed = self.database.hasNodePropertyIndex(label, prop.key) catch false;
                if (!indexed) continue;
                return scan_ops.findNodesByIndexedValue(self.database, ctx, label, prop.key, prop.value);
            }
        }
        if (self.labels.len > 0) {
            return self.database.getNodesByLabelInTxn(ctx.txn, self.labels[0]) catch |err|
                return ctx.storageFailure(err, "reading the nodes labelled '{s}'", .{self.labels[0]});
        }
        return self.database.getAllNodeIdsInTxn(ctx.txn) catch |err|
            return ctx.storageFailure(err, "reading every node", .{});
    }

    fn nodeMatches(
        node: lattice.graph.node.Node,
        label_ids: []const SymbolId,
        props: []const EvaluatedProperty,
        key_ids: []const SymbolId,
    ) bool {
        for (label_ids) |wanted| {
            if (std.mem.indexOfScalar(SymbolId, node.labels, wanted) == null) return false;
        }
        outer: for (props, key_ids) |prop, key_id| {
            for (node.properties) |actual| {
                if (actual.key_id != key_id) continue;
                if (mergeValuesEqual(prop.value, actual.value)) continue :outer;
                return false;
            }
            return false;
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
        try applyEvaluatedNodeProperties(self.database, ctx, node_id, evaluated.items);
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
        self.pending.deinit(self.allocator);
        allocator.destroy(self);
    }
};

/// Equality for MERGE matching, as Cypher's `=` sees it: an integer equals
/// the float with the same value, and lists and maps compare element by
/// element.
fn mergeValuesEqual(a: PropertyValue, b: PropertyValue) bool {
    switch (a) {
        .int_val => |av| switch (b) {
            .int_val => |bv| return av == bv,
            .float_val => |bv| return expression_mod.compareIntFloat(av, bv) == .eq,
            else => return false,
        },
        .float_val => |av| switch (b) {
            .float_val => |bv| return av == bv,
            .int_val => |bv| return expression_mod.compareIntFloat(bv, av) == .eq,
            else => return false,
        },
        .list_val => |list| {
            if (b != .list_val or b.list_val.len != list.len) return false;
            for (list, b.list_val) |lhs, rhs| {
                if (!mergeValuesEqual(lhs, rhs)) return false;
            }
            return true;
        },
        .map_val => |map| {
            if (b != .map_val or b.map_val.len != map.len) return false;
            for (map) |entry| {
                for (b.map_val) |other| {
                    if (!std.mem.eql(u8, entry.key, other.key)) continue;
                    if (!mergeValuesEqual(entry.value, other.value)) return false;
                    break;
                } else return false;
            }
            return true;
        },
        .bytes_val => |v| return b == .bytes_val and std.mem.eql(u8, b.bytes_val, v),
        .vector_val => |v| return b == .vector_val and std.mem.eql(f32, b.vector_val, v),
        else => return propertyValuesEqual(a, b),
    }
}

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

        const source_id = source_val.asNodeId() orelse return endpointFailure(ctx, "MERGE", source_val);
        const target_id = target_val.asNodeId() orelse return endpointFailure(ctx, "MERGE", target_val);

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

        const edge_id = self.database.createEdgeAndGetId(ctx.txn, source_id, target_id, self.edge_type) catch |err|
            return ctx.storageFailure(err, "creating a relationship from node {d} to node {d}", .{ source_id, target_id });

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
                else => ctx.storageFailure(err, "looking up relationship type '{s}'", .{self.edge_type}),
            };
        };

        const refs = self.database.getOutgoingEdgeRefsByTypeInTxn(ctx.txn, source_id, type_id) catch |err|
            return ctx.storageFailure(err, "reading the relationships of node {d}", .{source_id});
        defer self.database.allocator.free(refs);

        for (refs) |edge_ref| {
            if (edge_ref.target != target_id) continue;
            if (self.properties.len == 0) return edge_ref.id;
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

        var edge = (self.database.getEdgeInTxn(ctx.txn, edge_id) catch |err|
            return ctx.storageFailure(err, "reading relationship {d}", .{edge_id})) orelse
            return ctx.storageFailure(error.NotFound, "reading relationship {d}", .{edge_id});
        defer edge.deinit(self.database.allocator);

        for (self.properties) |prop| {
            const expected = self.evaluator.evaluate(prop.value_expr, row, ctx) catch |err| {
                return mapEvalError(err);
            };
            const expected_pv = (evalResultToPropertyValue(expected, self.allocator) catch return OperatorError.OutOfMemory) orelse return OperatorError.TypeError;

            const key_id = self.database.symbol_table.lookup(prop.key) catch |err| {
                return switch (err) {
                    SymbolError.NotFound => false,
                    else => ctx.storageFailure(err, "looking up property key '{s}'", .{prop.key}),
                };
            };

            var found = false;
            for (edge.properties) |actual_prop| {
                if (actual_prop.key_id != key_id) continue;
                if (!mergeValuesEqual(expected_pv, actual_prop.value)) {
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
            self.database.setEdgePropertyById(ctx.txn, edge_id, item.key, item.value) catch |err|
                return ctx.storageFailure(err, "setting property '{s}' of relationship {d}", .{ item.key, edge_id });
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

    const null_pv = try evalResultToPropertyValue(.{ .null_val = {} }, allocator);
    try std.testing.expect(null_pv != null);
    try std.testing.expect(null_pv.? == .null_val);

    const bool_pv = try evalResultToPropertyValue(.{ .bool_val = true }, allocator);
    try std.testing.expectEqual(true, bool_pv.?.bool_val);

    const int_pv = try evalResultToPropertyValue(.{ .int_val = 99 }, allocator);
    try std.testing.expectEqual(@as(i64, 99), int_pv.?.int_val);

    const float_pv = try evalResultToPropertyValue(.{ .float_val = 1.5 }, allocator);
    try std.testing.expectApproxEqAbs(@as(f64, 1.5), float_pv.?.float_val, 0.001);

    const str_pv = try evalResultToPropertyValue(.{ .string_val = "test" }, allocator);
    try std.testing.expectEqualStrings("test", str_pv.?.string_val);
}

test "evalResultToPropertyValue returns null for node/edge refs" {
    const allocator = std.testing.allocator;

    const node_pv = try evalResultToPropertyValue(.{ .node_ref = 42 }, allocator);
    try std.testing.expect(node_pv == null);

    const edge_pv = try evalResultToPropertyValue(.{ .edge_ref = 7 }, allocator);
    try std.testing.expect(edge_pv == null);
}

test "evalResultToPropertyValue converts vector" {
    const allocator = std.testing.allocator;

    const vec = [_]f32{ 1.0, 2.0, 3.0 };
    const pv = try evalResultToPropertyValue(.{ .vector_val = &vec }, allocator);

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
    const pv = try evalResultToPropertyValue(.{ .list_val = &items }, allocator);
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
    const pv = try evalResultToPropertyValue(.{ .map_val = &entries }, allocator);
    defer allocator.free(pv.?.map_val);

    try std.testing.expect(pv != null);
    try std.testing.expectEqual(@as(usize, 2), pv.?.map_val.len);
    try std.testing.expectEqualStrings("name", pv.?.map_val[0].key);
    try std.testing.expectEqualStrings("Alice", pv.?.map_val[0].value.string_val);
    try std.testing.expectEqualStrings("score", pv.?.map_val[1].key);
    try std.testing.expectEqual(@as(i64, 95), pv.?.map_val[1].value.int_val);
}
