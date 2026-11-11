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
const ExecutionContext = executor.ExecutionContext;

const core_types = @import("../../core/types.zig");
const NodeId = core_types.NodeId;
const EdgeId = core_types.EdgeId;

const edge_mod = @import("../../graph/edge.zig");

const symbols = @import("../../graph/symbols.zig");
const SymbolId = symbols.SymbolId;

const database_mod = @import("../../storage/database.zig");
const Database = database_mod.Database;

const expand_ops = @import("expand.zig");
const ExpandDirection = expand_ops.ExpandDirection;
const EdgeConstraints = expand_ops.EdgeConstraints;
const fetchEdgeRefs = expand_ops.fetchEdgeRefs;

// ============================================================================
// Path Frame for DFS traversal
// ============================================================================

/// One node on the path being walked, with the edges still to try from it.
const PathFrame = struct {
    /// Node at this position in the path
    node_id: NodeId,
    /// Hops from the source to this node
    depth: u32,
    /// The edge that led here, null for the source
    via_edge: ?EdgeId,
    /// Edges leaving the node, when the direction reads them
    outgoing_edges: ?[]edge_mod.EdgeRef = null,
    /// Edges entering the node, when the direction reads them
    incoming_edges: ?[]edge_mod.EdgeRef = null,
    outgoing_index: usize = 0,
    incoming_index: usize = 0,

    fn deinit(self: *PathFrame, edge_allocator: Allocator) void {
        if (self.outgoing_edges) |edges| {
            edge_allocator.free(edges);
            self.outgoing_edges = null;
        }
        if (self.incoming_edges) |edges| {
            edge_allocator.free(edges);
            self.incoming_edges = null;
        }
    }
};

// ============================================================================
// Variable-Length Expand Operator
// ============================================================================

/// Expand operator for variable-length path traversal.
///
/// Matches Cypher's semantics: every path from the source whose length lies
/// within the hop bounds, using each relationship at most once per path.
/// Nodes may repeat, so a path can come back through a node it has visited,
/// including the source. Each path is one result, so a node reachable by two
/// paths is returned twice.
///
/// There is no hop cap. An unbounded pattern ends because a path cannot use
/// more relationships than exist, but the number of paths can grow
/// exponentially with the size of a dense graph, so unbounded patterns over
/// large, highly connected graphs can be slow.
pub const VariableLengthExpand = struct {
    /// Input operator providing source nodes
    input: Operator,
    /// Slot containing the source node ID
    source_slot: u8,
    /// Slot to output the target node ID
    target_slot: u8,
    /// Types, uniqueness and expand-into rules for the pattern
    constraints: EdgeConstraints,
    /// Direction to traverse
    direction: ExpandDirection,
    /// Database reference for txn-aware traversal
    database: *Database,
    /// Minimum hops (inclusive)
    min_hops: u32,
    /// Maximum hops (inclusive), null means unbounded
    max_hops: ?u32,

    /// Current input row being processed
    current_input: ?*Row,
    /// The far end the path must reach, when the target slot is bound
    bound_target: ?NodeId,
    /// The path being walked, source first
    path_stack: std.ArrayList(PathFrame),
    /// Edges on the current path
    used_edges: std.AutoHashMapUnmanaged(EdgeId, void),
    /// Output row
    output_row: ?*Row,
    /// A zero-hop match of the source waiting to be returned
    pending_result: ?NodeId,
    /// Whether opened
    opened: bool,
    /// Allocator
    allocator: Allocator,

    const Self = @This();

    /// Create a new VariableLengthExpand operator
    pub fn init(
        allocator: Allocator,
        input: Operator,
        source_slot: u8,
        target_slot: u8,
        constraints: EdgeConstraints,
        direction: ExpandDirection,
        database: *Database,
        min_hops: u32,
        max_hops: ?u32,
    ) !*Self {
        const self = try allocator.create(Self);
        self.* = Self{
            .input = input,
            .source_slot = source_slot,
            .target_slot = target_slot,
            .constraints = constraints,
            .direction = direction,
            .database = database,
            .min_hops = min_hops,
            .max_hops = max_hops,
            .current_input = null,
            .bound_target = null,
            .path_stack = .empty,
            .used_edges = .{},
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
            if (self.pending_result) |target_id| {
                self.pending_result = null;
                return self.emitResult(target_id);
            }

            if (self.path_stack.items.len == 0) {
                // Need a new input row
                self.resetTraversal();
                self.current_input = try self.input.next(ctx) orelse return null;
                try self.startTraversal();
                continue;
            }

            if (try self.step(ctx)) |target_id| {
                return self.emitResult(target_id);
            }
        }
    }

    /// Advance the walk by one edge, or retreat one node when the deepest
    /// one has nothing left to try. Returns a node to emit, if the step
    /// reached one that matches.
    fn step(self: *Self, ctx: *ExecutionContext) OperatorError!?NodeId {
        const top = &self.path_stack.items[self.path_stack.items.len - 1];
        const input_row = self.current_input orelse return OperatorError.NotInitialized;

        const can_extend = if (self.max_hops) |max| top.depth < max else true;
        if (can_extend) {
            if (top.outgoing_edges == null and top.incoming_edges == null) {
                try self.loadEdges(ctx, top);
            }
            if (self.nextCandidate(top, input_row)) |candidate| {
                const new_depth = top.depth + 1;
                self.used_edges.put(self.allocator, candidate.edge_id, {}) catch return OperatorError.OutOfMemory;
                self.path_stack.append(self.allocator, .{
                    .node_id = candidate.node_id,
                    .depth = new_depth,
                    .via_edge = candidate.edge_id,
                }) catch {
                    _ = self.used_edges.remove(candidate.edge_id);
                    return OperatorError.OutOfMemory;
                };
                if (new_depth >= self.min_hops and self.reachesTarget(candidate.node_id)) {
                    return candidate.node_id;
                }
                return null;
            }
        }

        // Nothing more from this node: step back.
        var frame = self.path_stack.pop().?;
        if (frame.via_edge) |edge_id| _ = self.used_edges.remove(edge_id);
        frame.deinit(self.database.allocator);
        return null;
    }

    const Candidate = struct { edge_id: EdgeId, node_id: NodeId };

    /// The next edge from `frame` the path may take.
    fn nextCandidate(self: *Self, frame: *PathFrame, input_row: *const Row) ?Candidate {
        if (frame.outgoing_edges) |edges| {
            while (frame.outgoing_index < edges.len) {
                const edge = edges[frame.outgoing_index];
                frame.outgoing_index += 1;
                if (!self.usable(edge, input_row)) continue;
                return .{ .edge_id = edge.id, .node_id = edge.target };
            }
        }
        if (frame.incoming_edges) |edges| {
            while (frame.incoming_index < edges.len) {
                const edge = edges[frame.incoming_index];
                frame.incoming_index += 1;
                if (!self.usable(edge, input_row)) continue;
                // An undirected walk sees a self-loop from both ends; it is
                // one relationship, taken once, from the outgoing side.
                if (self.direction == .both and edge.source == edge.target) continue;
                return .{ .edge_id = edge.id, .node_id = edge.source };
            }
        }
        return null;
    }

    fn usable(self: *Self, edge: edge_mod.EdgeRef, input_row: *const Row) bool {
        if (!self.constraints.acceptsType(edge.edge_type)) return false;
        if (self.used_edges.contains(edge.id)) return false;
        _ = input_row;
        return true;
    }

    fn reachesTarget(self: *const Self, node_id: NodeId) bool {
        const want = self.bound_target orelse return true;
        return want == node_id;
    }

    fn loadEdges(self: *Self, ctx: *ExecutionContext, frame: *PathFrame) OperatorError!void {
        const t = self.constraints.types;
        switch (self.direction) {
            .outgoing => frame.outgoing_edges = try fetchEdgeRefs(self.database, ctx, frame.node_id, t, false),
            .incoming => frame.incoming_edges = try fetchEdgeRefs(self.database, ctx, frame.node_id, t, true),
            .both => {
                frame.outgoing_edges = try fetchEdgeRefs(self.database, ctx, frame.node_id, t, false);
                frame.incoming_edges = try fetchEdgeRefs(self.database, ctx, frame.node_id, t, true);
            },
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

        // Zero-hop quantifiers include the source node itself.
        if (self.min_hops == 0 and self.reachesTarget(source_id)) {
            self.pending_result = source_id;
        }

        self.path_stack.append(self.allocator, .{
            .node_id = source_id,
            .depth = 0,
            .via_edge = null,
        }) catch return OperatorError.OutOfMemory;
    }

    fn resetTraversal(self: *Self) void {
        for (self.path_stack.items) |*frame| {
            frame.deinit(self.database.allocator);
        }
        self.path_stack.clearRetainingCapacity();
        self.used_edges.clearRetainingCapacity();
        self.pending_result = null;
        self.bound_target = null;
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

        for (self.path_stack.items) |*frame| {
            frame.deinit(self.database.allocator);
        }
        self.path_stack.deinit(self.allocator);
        self.used_edges.deinit(self.allocator);

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
