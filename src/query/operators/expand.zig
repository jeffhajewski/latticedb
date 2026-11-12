//! Expand operator for edge traversal.
//!
//! Traverses edges from input nodes, producing rows for each edge found.
//! Supports outgoing, incoming, and bidirectional traversal.

const std = @import("std");
const Allocator = std.mem.Allocator;

const executor = @import("../executor.zig");
const Operator = executor.Operator;
const OperatorError = executor.OperatorError;
const Row = executor.Row;
const ExecutionContext = executor.ExecutionContext;

const edge_mod = @import("../../graph/edge.zig");

const core_types = @import("../../core/types.zig");
const NodeId = core_types.NodeId;
const EdgeId = core_types.EdgeId;

const symbols = @import("../../graph/symbols.zig");
const SymbolId = symbols.SymbolId;

const database_mod = @import("../../storage/database.zig");
const Database = database_mod.Database;

// ============================================================================
// Expand Direction
// ============================================================================

/// Direction to traverse edges
pub const ExpandDirection = enum {
    /// Traverse outgoing edges only
    outgoing,
    /// Traverse incoming edges only
    incoming,
    /// Traverse both directions
    both,
};

// ============================================================================
// Shared pattern rules
// ============================================================================

/// What a relationship pattern requires of the edges it matches, beyond its
/// direction. Shared by the single-hop and variable-length expands.
pub const EdgeConstraints = struct {
    /// The relationship types the pattern accepts, as in `[:A|B]`. Empty
    /// accepts every type. A type the database has never seen is passed as
    /// `NULL_SYMBOL`, which no edge carries.
    types: []const SymbolId = &.{},
    /// Slots holding edges matched earlier in the same MATCH. Cypher never
    /// binds one relationship twice in a pattern, so an edge already in any of
    /// these slots is skipped. A slot holds one edge, or, for a
    /// variable-length relationship, the list of its path's edge ids.
    distinct_from: []const u8 = &.{},
    /// The far end is already bound in the target slot, as in
    /// `MATCH (a)-->(b)-->(a)` or a variable bound by an earlier clause. Only
    /// edges that lead to that node match, and the slot keeps its value.
    target_bound: bool = false,

    /// Whether an edge of type `edge_type` matches the pattern's types.
    pub fn acceptsType(self: EdgeConstraints, edge_type: SymbolId) bool {
        if (self.types.len <= 1) return true; // the fetch already filtered
        for (self.types) |t| {
            if (t == edge_type) return true;
        }
        return false;
    }

    /// Whether the row already binds `edge_id` in one of the pattern's
    /// earlier relationship slots.
    pub fn usedEarlier(self: EdgeConstraints, row: *const Row, edge_id: EdgeId) bool {
        for (self.distinct_from) |slot| {
            const value = row.getSlot(slot) orelse continue;
            switch (value) {
                .edge_ref => |id| if (id == edge_id) return true,
                .property => |path| if (path == .list_val) {
                    for (path.list_val) |item| {
                        if (item == .int_val and item.int_val == @as(i64, @bitCast(edge_id))) return true;
                    }
                },
                else => {},
            }
        }
        return false;
    }

    /// Whether the pattern expands into a bound node that is null, which no
    /// relationship reaches: a null variable matches nothing.
    pub fn targetIsNull(self: EdgeConstraints, row: *const Row, target_slot: u8) bool {
        if (!self.target_bound) return false;
        const value = row.getSlot(target_slot) orelse return false;
        return value.isNull();
    }

    /// The node the target slot is bound to, when the pattern expands into a
    /// bound node.
    pub fn boundTarget(self: EdgeConstraints, row: *const Row, target_slot: u8) OperatorError!?NodeId {
        if (!self.target_bound) return null;
        const value = row.getSlot(target_slot) orelse return OperatorError.UnboundVariable;
        return value.asNodeId() orelse return OperatorError.TypeError;
    }
};

/// The edges leaving (or, with `incoming`, entering) `node_id` that could
/// match `types`. With one type the storage layer filters; with several, the
/// caller filters each edge through `EdgeConstraints.acceptsType`.
///
/// The slice belongs to `database.allocator`.
pub fn fetchEdgeRefs(
    database: *Database,
    ctx: *ExecutionContext,
    node_id: NodeId,
    types: []const SymbolId,
    incoming: bool,
) OperatorError![]edge_mod.EdgeRef {
    if (types.len == 1) {
        const edge_type = types[0];
        return (if (incoming)
            database.getIncomingEdgeRefsByTypeInTxn(ctx.txn, node_id, edge_type)
        else
            database.getOutgoingEdgeRefsByTypeInTxn(ctx.txn, node_id, edge_type)) catch |err|
            return ctx.storageFailure(err, "reading the relationships of node {d}", .{node_id});
    }
    return (if (incoming)
        database.getIncomingEdgeRefsInTxn(ctx.txn, node_id)
    else
        database.getOutgoingEdgeRefsInTxn(ctx.txn, node_id)) catch |err|
        return ctx.storageFailure(err, "reading the relationships of node {d}", .{node_id});
}

// ============================================================================
// Expand Operator
// ============================================================================

/// Expand operator that traverses edges from input nodes.
pub const Expand = struct {
    /// Input operator providing source nodes
    input: Operator,
    /// Slot containing the source node ID
    source_slot: u8,
    /// Slot to output the target node ID
    target_slot: u8,
    /// Slot to output the edge ID (optional)
    edge_slot: ?u8,
    /// Types, uniqueness and expand-into rules for the pattern
    constraints: EdgeConstraints,
    /// Direction to traverse
    direction: ExpandDirection,
    /// Database for txn-aware traversal
    database: *Database,
    /// Current input row
    current_input: ?*Row,
    /// Current outgoing edges
    outgoing_edges: ?[]edge_mod.EdgeRef,
    /// Current incoming edges
    incoming_edges: ?[]edge_mod.EdgeRef,
    /// Current outgoing index
    outgoing_index: usize,
    /// Current incoming index
    incoming_index: usize,
    /// Output row
    output_row: ?*Row,
    /// Whether doing incoming phase (for both direction)
    in_incoming_phase: bool,
    /// Whether opened
    opened: bool,
    /// Allocator for temporary allocations
    allocator: Allocator,

    const Self = @This();

    /// Create a new Expand operator
    pub fn init(
        allocator: Allocator,
        input: Operator,
        source_slot: u8,
        target_slot: u8,
        edge_slot: ?u8,
        constraints: EdgeConstraints,
        direction: ExpandDirection,
        database: *Database,
    ) !*Self {
        const self = try allocator.create(Self);
        self.* = Self{
            .input = input,
            .source_slot = source_slot,
            .target_slot = target_slot,
            .edge_slot = edge_slot,
            .constraints = constraints,
            .direction = direction,
            .database = database,
            .current_input = null,
            .outgoing_edges = null,
            .incoming_edges = null,
            .outgoing_index = 0,
            .incoming_index = 0,
            .output_row = null,
            .in_incoming_phase = false,
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

        // Allocate output row
        self.output_row = ctx.allocRow() catch return OperatorError.OutOfMemory;

        // Open input operator
        try self.input.open(ctx);
        self.opened = true;
    }

    fn next(ptr: *anyopaque, ctx: *ExecutionContext) OperatorError!?*Row {
        const self: *Self = @ptrCast(@alignCast(ptr));

        if (!self.opened) return OperatorError.NotInitialized;

        while (true) {
            // Try to get next edge from current iterator
            if (try self.nextFromCurrentIterator()) |row| {
                return row;
            }

            // If doing both directions and in outgoing phase, switch to incoming
            if (self.direction == .both and !self.in_incoming_phase and self.current_input != null) {
                self.in_incoming_phase = true;
                try self.startIncomingIterator(ctx);
                continue;
            }

            // Need a new input row
            self.closeCurrentIterators();
            self.in_incoming_phase = false;

            // Cleared first: an input that runs out and later has rows again
            // (an OPTIONAL MATCH's argument, once per outer row) must not
            // leave the last row in place for the incoming half of `both`.
            self.current_input = null;
            self.current_input = try self.input.next(ctx) orelse return null;

            // Start iterator for this node
            try self.startIteratorForCurrentInput(ctx);
        }
    }

    /// Whether the operator is currently walking incoming edges.
    ///
    /// `in_incoming_phase` only tracks which half of a `both` traversal is in
    /// progress, so it stays false for a plain `.incoming` expand. Keying the
    /// iterator choice off it alone sent `.incoming` down the outgoing branch,
    /// where `outgoing_edges` is null, and every left-pointing pattern returned
    /// no rows at all.
    fn readingIncoming(self: *const Self) bool {
        return self.direction == .incoming or self.in_incoming_phase;
    }

    fn nextFromCurrentIterator(self: *Self) OperatorError!?*Row {
        const output_row = self.output_row orelse return OperatorError.NotInitialized;
        const input_row = self.current_input orelse return null;

        // Determine which iterator to use
        var edges: []edge_mod.EdgeRef = undefined;
        var index_ptr: *usize = undefined;
        if (self.readingIncoming()) {
            edges = self.incoming_edges orelse return null;
            index_ptr = &self.incoming_index;
        } else {
            edges = self.outgoing_edges orelse return null;
            index_ptr = &self.outgoing_index;
        }

        if (self.constraints.targetIsNull(input_row, self.target_slot)) return null;
        const bound_target = try self.constraints.boundTarget(input_row, self.target_slot);

        while (index_ptr.* < edges.len) {
            const edge = edges[index_ptr.*];
            index_ptr.* += 1;

            if (!self.constraints.acceptsType(edge.edge_type)) continue;
            // An undirected pattern sees a self-loop from both ends. It is one
            // relationship, so it matches once, in the outgoing half.
            if (self.in_incoming_phase and edge.source == edge.target) continue;
            if (self.constraints.usedEarlier(input_row, edge.id)) continue;

            // Following an incoming edge means the other end is its source.
            const target_id = if (self.readingIncoming()) edge.source else edge.target;
            if (bound_target) |want| {
                if (target_id != want) continue;
            }

            // Build output row - copy ALL populated slots from input
            output_row.clear();
            output_row.copyFrom(input_row);
            output_row.setSlot(self.target_slot, .{ .node_ref = target_id });

            // Set edge slot if requested
            if (self.edge_slot) |slot| {
                output_row.setSlot(slot, .{ .edge_ref = edge.id });
            }

            return output_row;
        }

        return null;
    }

    fn startIteratorForCurrentInput(self: *Self, ctx: *ExecutionContext) OperatorError!void {
        const input_row = self.current_input orelse return;
        const source_val = input_row.getSlot(self.source_slot) orelse return OperatorError.UnboundVariable;
        // A null node, as an OPTIONAL MATCH that found nothing leaves, has no
        // relationships.
        if (source_val.isNull()) return;

        const node_id = source_val.asNodeId() orelse return OperatorError.TypeError;

        // Start appropriate iterator based on direction
        switch (self.direction) {
            .outgoing, .both => {
                self.outgoing_edges = try fetchEdgeRefs(self.database, ctx, node_id, self.constraints.types, false);
                self.outgoing_index = 0;
            },
            .incoming => {
                self.incoming_edges = try fetchEdgeRefs(self.database, ctx, node_id, self.constraints.types, true);
                self.incoming_index = 0;
            },
        }
    }

    fn startIncomingIterator(self: *Self, ctx: *ExecutionContext) OperatorError!void {
        const input_row = self.current_input orelse return;
        const source_val = input_row.getSlot(self.source_slot) orelse return;

        const node_id = source_val.asNodeId() orelse return;

        self.incoming_edges = try fetchEdgeRefs(self.database, ctx, node_id, self.constraints.types, true);
        self.incoming_index = 0;
    }

    fn closeCurrentIterators(self: *Self) void {
        if (self.outgoing_edges) |edges| {
            self.database.allocator.free(edges);
            self.outgoing_edges = null;
            self.outgoing_index = 0;
        }
        if (self.incoming_edges) |edges| {
            self.database.allocator.free(edges);
            self.incoming_edges = null;
            self.incoming_index = 0;
        }
    }

    fn close(ptr: *anyopaque, ctx: *ExecutionContext) void {
        const self: *Self = @ptrCast(@alignCast(ptr));

        self.closeCurrentIterators();

        if (self.opened) {
            self.input.close(ctx);
            self.opened = false;
        }
    }

    fn deinit(ptr: *anyopaque, allocator: Allocator) void {
        const self: *Self = @ptrCast(@alignCast(ptr));

        // Close any open iterators
        self.closeCurrentIterators();

        // Deinit input operator
        self.input.deinit(allocator);

        allocator.destroy(self);
    }
};

// ============================================================================
// Tests
// ============================================================================

test "Expand basic structure" {
    // Verify vtable is properly structured
    const vtable = Expand.vtable;
    try std.testing.expect(@TypeOf(vtable.open) != void);
    try std.testing.expect(@TypeOf(vtable.next) != void);
    try std.testing.expect(@TypeOf(vtable.close) != void);
    try std.testing.expect(@TypeOf(vtable.deinit) != void);
}
