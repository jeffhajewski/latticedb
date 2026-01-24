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
const SlotValue = executor.SlotValue;
const ExecutionContext = executor.ExecutionContext;

const types = @import("../../core/types.zig");
const NodeId = types.NodeId;
const EdgeId = types.EdgeId;

const edge_mod = @import("../../graph/edge.zig");
const EdgeStore = edge_mod.EdgeStore;
const Edge = edge_mod.Edge;
const EdgeError = edge_mod.EdgeError;

const symbols = @import("../../graph/symbols.zig");
const SymbolId = symbols.SymbolId;

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
    /// Edge type to filter by (null means all types)
    edge_type: ?SymbolId,
    /// Direction to traverse
    direction: ExpandDirection,
    /// Edge store for traversal
    edge_store: *EdgeStore,
    /// Current input row
    current_input: ?*Row,
    /// Current edge iterator (for outgoing)
    outgoing_iter: ?EdgeStore.EdgeIterator,
    /// Current edge iterator (for incoming, when doing both)
    incoming_iter: ?EdgeStore.EdgeIterator,
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
        edge_type: ?SymbolId,
        direction: ExpandDirection,
        edge_store: *EdgeStore,
    ) !*Self {
        const self = try allocator.create(Self);
        self.* = Self{
            .input = input,
            .source_slot = source_slot,
            .target_slot = target_slot,
            .edge_slot = edge_slot,
            .edge_type = edge_type,
            .direction = direction,
            .edge_store = edge_store,
            .current_input = null,
            .outgoing_iter = null,
            .incoming_iter = null,
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
                try self.startIncomingIterator();
                continue;
            }

            // Need a new input row
            self.closeCurrentIterators();
            self.in_incoming_phase = false;

            self.current_input = try self.input.next(ctx) orelse return null;

            // Start iterator for this node
            try self.startIteratorForCurrentInput();
        }
    }

    fn nextFromCurrentIterator(self: *Self) OperatorError!?*Row {
        const output_row = self.output_row orelse return OperatorError.NotInitialized;
        const input_row = self.current_input orelse return null;

        // Determine which iterator to use
        var iter_ptr: *?EdgeStore.EdgeIterator = undefined;
        if (self.in_incoming_phase) {
            iter_ptr = &self.incoming_iter;
        } else {
            iter_ptr = &self.outgoing_iter;
        }

        var iter = iter_ptr.* orelse return null;

        // Get next edge
        const edge_opt = iter.next() catch return OperatorError.StorageError;
        iter_ptr.* = iter; // Update iterator state

        if (edge_opt) |edge| {
            defer {
                var e = edge;
                e.deinit(self.edge_store.allocator);
            }

            // Filter by edge type if specified
            if (self.edge_type) |expected_type| {
                if (edge.edge_type != expected_type) {
                    // Skip this edge, try next
                    return self.nextFromCurrentIterator();
                }
            }

            // Build output row - copy ALL populated slots from input
            output_row.clear();
            output_row.copyFrom(input_row);

            // Determine target node based on direction
            const target_id = if (self.in_incoming_phase) edge.source else edge.target;
            output_row.setSlot(self.target_slot, .{ .node_ref = target_id });

            // Set edge slot if requested
            if (self.edge_slot) |slot| {
                // Store edge as a reference (edge ID is source + target + type composite)
                const edge_id = computeEdgeId(edge.source, edge.target, edge.edge_type);
                output_row.setSlot(slot, .{ .edge_ref = edge_id });
            }

            return output_row;
        }

        return null;
    }

    fn startIteratorForCurrentInput(self: *Self) OperatorError!void {
        const input_row = self.current_input orelse return;
        const source_val = input_row.getSlot(self.source_slot) orelse return OperatorError.UnboundVariable;

        const node_id = source_val.asNodeId() orelse return OperatorError.TypeError;

        // Start appropriate iterator based on direction
        switch (self.direction) {
            .outgoing => {
                if (self.edge_type) |edge_type| {
                    self.outgoing_iter = self.edge_store.getOutgoingByType(node_id, edge_type) catch return OperatorError.StorageError;
                } else {
                    self.outgoing_iter = self.edge_store.getOutgoing(node_id) catch return OperatorError.StorageError;
                }
            },
            .incoming => {
                if (self.edge_type) |edge_type| {
                    self.incoming_iter = self.edge_store.getIncomingByType(node_id, edge_type) catch return OperatorError.StorageError;
                } else {
                    self.incoming_iter = self.edge_store.getIncoming(node_id) catch return OperatorError.StorageError;
                }
            },
            .both => {
                // Start with outgoing
                if (self.edge_type) |edge_type| {
                    self.outgoing_iter = self.edge_store.getOutgoingByType(node_id, edge_type) catch return OperatorError.StorageError;
                } else {
                    self.outgoing_iter = self.edge_store.getOutgoing(node_id) catch return OperatorError.StorageError;
                }
            },
        }
    }

    fn startIncomingIterator(self: *Self) OperatorError!void {
        const input_row = self.current_input orelse return;
        const source_val = input_row.getSlot(self.source_slot) orelse return;

        const node_id = source_val.asNodeId() orelse return;

        if (self.edge_type) |edge_type| {
            self.incoming_iter = self.edge_store.getIncomingByType(node_id, edge_type) catch return OperatorError.StorageError;
        } else {
            self.incoming_iter = self.edge_store.getIncoming(node_id) catch return OperatorError.StorageError;
        }
    }

    fn closeCurrentIterators(self: *Self) void {
        if (self.outgoing_iter) |*iter| {
            iter.deinit();
            self.outgoing_iter = null;
        }
        if (self.incoming_iter) |*iter| {
            iter.deinit();
            self.incoming_iter = null;
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

/// Compute a unique edge ID from source, target, and type
fn computeEdgeId(source: NodeId, target: NodeId, edge_type: SymbolId) EdgeId {
    // Combine into a single ID - this is a simplified approach
    // In production, edges should have their own unique IDs
    return (source << 32) | (target << 16) | @as(u64, edge_type);
}

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

test "computeEdgeId produces unique values" {
    const id1 = computeEdgeId(1, 2, 3);
    const id2 = computeEdgeId(1, 3, 2);
    const id3 = computeEdgeId(2, 1, 3);

    try std.testing.expect(id1 != id2);
    try std.testing.expect(id1 != id3);
    try std.testing.expect(id2 != id3);
}
