//! Variable-length expand operator for multi-hop edge traversal.
//!
//! Traverses edges from input nodes with variable path lengths.
//! Supports patterns like (a)-[*1..3]->(b) for 1 to 3 hops.

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

const symbols = @import("../../graph/symbols.zig");
const SymbolId = symbols.SymbolId;

const expand_ops = @import("expand.zig");
const ExpandDirection = expand_ops.ExpandDirection;

// ============================================================================
// Path Frame for DFS traversal
// ============================================================================

/// A frame in the path stack representing one hop
const PathFrame = struct {
    /// Node at this position in the path
    node_id: NodeId,
    /// Current depth (1 = first hop)
    depth: u32,
    /// Iterator for outgoing edges
    outgoing_iter: ?EdgeStore.EdgeIterator,
    /// Iterator for incoming edges (used when direction is .both)
    incoming_iter: ?EdgeStore.EdgeIterator,
    /// Whether we're in the incoming phase
    in_incoming_phase: bool,

    fn deinit(self: *PathFrame) void {
        if (self.outgoing_iter) |*iter| {
            iter.deinit();
        }
        if (self.incoming_iter) |*iter| {
            iter.deinit();
        }
    }
};

// ============================================================================
// Variable-Length Expand Operator
// ============================================================================

/// Expand operator for variable-length path traversal.
pub const VariableLengthExpand = struct {
    /// Input operator providing source nodes
    input: Operator,
    /// Slot containing the source node ID
    source_slot: u8,
    /// Slot to output the target node ID
    target_slot: u8,
    /// Edge type to filter by (null means all types)
    edge_type: ?SymbolId,
    /// Direction to traverse
    direction: ExpandDirection,
    /// Edge store for traversal
    edge_store: *EdgeStore,
    /// Minimum hops (inclusive)
    min_hops: u32,
    /// Maximum hops (inclusive), null means unbounded (with safety limit)
    max_hops: ?u32,

    /// Current input row being processed
    current_input: ?*Row,
    /// Stack of path frames for DFS
    path_stack: std.ArrayList(PathFrame),
    /// Set of visited nodes to prevent cycles
    visited: std.AutoHashMap(NodeId, void),
    /// Output row
    output_row: ?*Row,
    /// Whether we have a pending result to emit
    pending_result: ?NodeId,
    /// Whether opened
    opened: bool,
    /// Allocator
    allocator: Allocator,

    /// Maximum path length if unbounded (safety limit)
    const MAX_UNBOUNDED_DEPTH: u32 = 100;

    const Self = @This();

    /// Create a new VariableLengthExpand operator
    pub fn init(
        allocator: Allocator,
        input: Operator,
        source_slot: u8,
        target_slot: u8,
        edge_type: ?SymbolId,
        direction: ExpandDirection,
        edge_store: *EdgeStore,
        min_hops: u32,
        max_hops: ?u32,
    ) !*Self {
        const self = try allocator.create(Self);
        self.* = Self{
            .input = input,
            .source_slot = source_slot,
            .target_slot = target_slot,
            .edge_type = edge_type,
            .direction = direction,
            .edge_store = edge_store,
            .min_hops = if (min_hops == 0) 1 else min_hops,
            .max_hops = max_hops,
            .current_input = null,
            .path_stack = .empty,
            .visited = std.AutoHashMap(NodeId, void).init(allocator),
            .output_row = null,
            .pending_result = null,
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
        .deinit = deinitOp,
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
            // Check if we have a pending result from previous exploration
            if (self.pending_result) |target_id| {
                self.pending_result = null;
                return self.emitResult(target_id);
            }

            // Try to continue DFS from current path
            if (try self.exploreNext()) |result| {
                // Check if hop count meets min_hops requirement
                if (result.depth >= self.min_hops) {
                    return self.emitResult(result.target_id);
                }
                // Otherwise continue exploring (target already pushed if depth < max)
                continue;
            }

            // Backtrack if possible
            if (self.backtrack()) {
                continue;
            }

            // Need a new input row
            self.resetTraversal();

            self.current_input = try self.input.next(ctx) orelse return null;

            // Start traversal from source node
            try self.startTraversal();
        }
    }

    fn emitResult(self: *Self, target_id: NodeId) ?*Row {
        const output_row = self.output_row orelse return null;
        const input_row = self.current_input orelse return null;

        // Copy ALL populated slots from input (preserves multi-hop chain)
        output_row.clear();
        output_row.copyFrom(input_row);

        // Set target node
        output_row.setSlot(self.target_slot, .{ .node_ref = target_id });

        return output_row;
    }

    fn startTraversal(self: *Self) OperatorError!void {
        const input_row = self.current_input orelse return;
        const source_val = input_row.getSlot(self.source_slot) orelse return OperatorError.UnboundVariable;

        const source_id = source_val.asNodeId() orelse return OperatorError.TypeError;

        // Mark source as visited
        self.visited.put(source_id, {}) catch return OperatorError.OutOfMemory;

        // Create initial frame at depth 0 (source node, before first hop)
        const frame = try self.createFrame(source_id, 0);
        self.path_stack.append(self.allocator, frame) catch return OperatorError.OutOfMemory;
    }

    fn createFrame(self: *Self, node_id: NodeId, depth: u32) OperatorError!PathFrame {
        var frame = PathFrame{
            .node_id = node_id,
            .depth = depth,
            .outgoing_iter = null,
            .incoming_iter = null,
            .in_incoming_phase = false,
        };

        // Start iterator based on direction
        switch (self.direction) {
            .outgoing, .both => {
                if (self.edge_type) |edge_type| {
                    frame.outgoing_iter = self.edge_store.getOutgoingByType(node_id, edge_type) catch return OperatorError.StorageError;
                } else {
                    frame.outgoing_iter = self.edge_store.getOutgoing(node_id) catch return OperatorError.StorageError;
                }
            },
            .incoming => {
                if (self.edge_type) |edge_type| {
                    frame.incoming_iter = self.edge_store.getIncomingByType(node_id, edge_type) catch return OperatorError.StorageError;
                } else {
                    frame.incoming_iter = self.edge_store.getIncoming(node_id) catch return OperatorError.StorageError;
                }
                frame.in_incoming_phase = true;
            },
        }

        return frame;
    }

    /// Result from exploreNext - contains target node and its hop depth
    const ExploreResult = struct {
        target_id: NodeId,
        depth: u32,
    };

    fn exploreNext(self: *Self) OperatorError!?ExploreResult {
        if (self.path_stack.items.len == 0) return null;

        const frame = &self.path_stack.items[self.path_stack.items.len - 1];
        const effective_max = self.max_hops orelse MAX_UNBOUNDED_DEPTH;

        // Check if we've reached max depth
        if (frame.depth >= effective_max) return null;

        while (true) {
            // Get next edge from current iterator
            var iter_ptr: *?EdgeStore.EdgeIterator = undefined;
            if (frame.in_incoming_phase) {
                iter_ptr = &frame.incoming_iter;
            } else {
                iter_ptr = &frame.outgoing_iter;
            }

            var iter = iter_ptr.* orelse {
                // Switch to incoming if doing both directions
                if (self.direction == .both and !frame.in_incoming_phase) {
                    frame.in_incoming_phase = true;
                    if (self.edge_type) |edge_type| {
                        frame.incoming_iter = self.edge_store.getIncomingByType(frame.node_id, edge_type) catch return OperatorError.StorageError;
                    } else {
                        frame.incoming_iter = self.edge_store.getIncoming(frame.node_id) catch return OperatorError.StorageError;
                    }
                    continue;
                }
                return null;
            };

            const edge_opt = iter.next() catch return OperatorError.StorageError;
            iter_ptr.* = iter; // Update iterator state

            if (edge_opt) |edge| {
                defer {
                    var e = edge;
                    e.deinit(self.allocator);
                }

                // Get target node based on direction
                const target_id = if (frame.in_incoming_phase) edge.source else edge.target;

                // Skip if already visited (cycle detection)
                if (self.visited.contains(target_id)) {
                    continue;
                }

                // Mark as visited
                self.visited.put(target_id, {}) catch return OperatorError.OutOfMemory;

                const new_depth = frame.depth + 1;

                // Check if we need to continue exploring (push frame for further traversal)
                if (new_depth < effective_max) {
                    // Push new frame for continued exploration
                    const new_frame = try self.createFrame(target_id, new_depth);
                    self.path_stack.append(self.allocator, new_frame) catch return OperatorError.OutOfMemory;
                }

                // Return this target with its hop depth
                return ExploreResult{ .target_id = target_id, .depth = new_depth };
            } else {
                // No more edges from current iterator
                // Switch to incoming if doing both directions
                if (self.direction == .both and !frame.in_incoming_phase) {
                    frame.in_incoming_phase = true;
                    if (self.edge_type) |edge_type| {
                        frame.incoming_iter = self.edge_store.getIncomingByType(frame.node_id, edge_type) catch return OperatorError.StorageError;
                    } else {
                        frame.incoming_iter = self.edge_store.getIncoming(frame.node_id) catch return OperatorError.StorageError;
                    }
                    continue;
                }
                return null;
            }
        }
    }

    fn backtrack(self: *Self) bool {
        while (self.path_stack.items.len > 1) {
            // Pop the current frame
            var frame = self.path_stack.pop() orelse break;

            // Remove from visited (allow other paths to visit this node)
            _ = self.visited.remove(frame.node_id);

            // Clean up frame resources
            frame.deinit();

            // Check if parent frame has more edges to explore
            if (self.path_stack.items.len > 0) {
                const parent = &self.path_stack.items[self.path_stack.items.len - 1];
                if (parent.outgoing_iter != null or parent.incoming_iter != null) {
                    return true;
                }
            }
        }

        return false;
    }

    fn resetTraversal(self: *Self) void {
        // Clean up path stack
        for (self.path_stack.items) |*frame| {
            frame.deinit();
        }
        self.path_stack.clearRetainingCapacity();

        // Clear visited set
        self.visited.clearRetainingCapacity();

        self.pending_result = null;
    }

    fn close(ptr: *anyopaque, ctx: *ExecutionContext) void {
        const self: *Self = @ptrCast(@alignCast(ptr));

        self.resetTraversal();

        if (self.opened) {
            self.input.close(ctx);
            self.opened = false;
        }
    }

    fn deinitOp(ptr: *anyopaque, allocator: Allocator) void {
        const self: *Self = @ptrCast(@alignCast(ptr));

        // Clean up path stack
        for (self.path_stack.items) |*frame| {
            frame.deinit();
        }
        self.path_stack.deinit(self.allocator);

        // Clean up visited set
        self.visited.deinit();

        // Deinit input operator
        self.input.deinit(allocator);

        allocator.destroy(self);
    }
};

// ============================================================================
// Tests
// ============================================================================

test "VariableLengthExpand basic structure" {
    // Verify vtable is properly structured
    const vtable = VariableLengthExpand.vtable;
    try std.testing.expect(@TypeOf(vtable.open) != void);
    try std.testing.expect(@TypeOf(vtable.next) != void);
    try std.testing.expect(@TypeOf(vtable.close) != void);
    try std.testing.expect(@TypeOf(vtable.deinit) != void);
}
