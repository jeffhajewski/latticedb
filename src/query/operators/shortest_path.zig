//! `shortestPath((a)-[:T*..n]->(b))` and `allShortestPaths(...)`.
//!
//! For each input row, with both ends bound, a breadth-first search runs
//! from both ends at once, one whole level at a time, always widening the
//! side whose frontier is smaller. The first level at which the two sides
//! meet gives the length of the shortest paths, so the search reads only
//! the neighbourhoods of the two ends up to about half that length each,
//! and never enumerates paths. shortestPath() returns one shortest path;
//! allShortestPaths() returns every path of that length, which it reads
//! back from the parents each node was reached by.
//!
//! A shortest path never repeats a node, so relationship uniqueness holds
//! by construction. The search state lives in an arena that is reset for
//! every input row.

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

const symbols = @import("../../graph/symbols.zig");
const SymbolId = symbols.SymbolId;

const Database = @import("../../storage/database.zig").Database;
const EvalResult = @import("../expression.zig").EvalResult;

const expand_ops = @import("expand.zig");
const ExpandDirection = expand_ops.ExpandDirection;
const EdgeConstraints = expand_ops.EdgeConstraints;
const fetchEdgeRefs = expand_ops.fetchEdgeRefs;

/// How a node was reached: by `edge`, from `node` one step nearer the end
/// its side started from.
const Parent = struct { edge: EdgeId, node: NodeId };

const Visit = struct {
    depth: u32,
    /// Every parent at the level before, for allShortestPaths; the first
    /// only, for shortestPath.
    parents: std.ArrayListUnmanaged(Parent),
};

const VisitMap = std.AutoHashMapUnmanaged(NodeId, Visit);

/// One side's chain of choices from a meeting node back to its end, as an
/// odometer over each node's parents.
const Chain = struct {
    /// nodes[0] is the meeting node, nodes[len] the end the side started at.
    nodes: []NodeId,
    edges: []EdgeId,
    choice: []u32,

    fn init(arena: Allocator, length: u32) error{OutOfMemory}!Chain {
        return .{
            .nodes = try arena.alloc(NodeId, length + 1),
            .edges = try arena.alloc(EdgeId, length),
            .choice = try arena.alloc(u32, length),
        };
    }

    /// Follow the current choices from position `from` on, resetting them
    /// to the first parent.
    fn fill(self: *Chain, visits: *const VisitMap, from: usize) void {
        var i = from;
        while (i < self.edges.len) : (i += 1) {
            self.choice[i] = 0;
            self.step(visits, i);
        }
    }

    fn step(self: *Chain, visits: *const VisitMap, i: usize) void {
        const parent = visits.getPtr(self.nodes[i]).?.parents.items[self.choice[i]];
        self.edges[i] = parent.edge;
        self.nodes[i + 1] = parent.node;
    }

    /// Move to the next chain; false when there is none.
    fn advance(self: *Chain, visits: *const VisitMap) bool {
        var i = self.edges.len;
        while (i > 0) {
            i -= 1;
            const parents = visits.getPtr(self.nodes[i]).?.parents.items;
            if (self.choice[i] + 1 < parents.len) {
                self.choice[i] += 1;
                self.step(visits, i);
                self.fill(visits, i + 1);
                return true;
            }
        }
        return false;
    }
};

pub const ShortestPath = struct {
    input: Operator,
    source_slot: u8,
    target_slot: u8,
    constraints: EdgeConstraints,
    direction: ExpandDirection,
    database: *Database,
    /// 0 or 1: whether a path from a node to itself, of no relationships,
    /// counts.
    min_hops: u32,
    max_hops: ?u32,
    /// allShortestPaths(): every shortest path, not one.
    all: bool,
    /// Where the path goes, for `p = shortestPath(...)`.
    path_slot: ?u8 = null,
    /// Where the list of its relationships goes, for `-[r*]->`.
    rels_slot: ?u8 = null,

    allocator: Allocator,
    /// The search for the current input row.
    search: std.heap.ArenaAllocator,
    forward: VisitMap = .{},
    backward: VisitMap = .{},
    current_input: ?*Row = null,
    output_row: ?*Row = null,
    opened: bool = false,

    /// The nodes where the two sides met at the shortest length.
    meetings: []NodeId = &.{},
    meeting_index: usize = 0,
    front: Chain = undefined,
    back: Chain = undefined,
    /// Whether front and back hold a path not yet returned.
    pending: bool = false,
    /// A zero-length path from the source to itself waits to be returned.
    pending_self: bool = false,

    const Self = @This();

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
        all: bool,
    ) !*Self {
        const self = try allocator.create(Self);
        self.* = .{
            .input = input,
            .source_slot = source_slot,
            .target_slot = target_slot,
            .constraints = constraints,
            .direction = direction,
            .database = database,
            .min_hops = min_hops,
            .max_hops = max_hops,
            .all = all,
            .allocator = allocator,
            .search = std.heap.ArenaAllocator.init(database.allocator),
        };
        return self;
    }

    pub fn operator(self: *Self) Operator {
        return .{ .vtable = &vtable, .ptr = self };
    }

    const vtable = Operator.VTable{
        .open = open,
        .next = next,
        .close = close,
        .deinit = deinitOp,
    };

    fn open(ptr: *anyopaque, ctx: *ExecutionContext) OperatorError!void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        self.output_row = ctx.allocRow() catch return OperatorError.OutOfMemory;
        self.resetSearch();
        try self.input.open(ctx);
        self.opened = true;
    }

    fn next(ptr: *anyopaque, ctx: *ExecutionContext) OperatorError!?*Row {
        const self: *Self = @ptrCast(@alignCast(ptr));
        if (!self.opened) return OperatorError.NotInitialized;

        while (true) {
            if (self.pending_self) {
                self.pending_self = false;
                const source = self.current_input.?.getSlot(self.source_slot).?.asNodeId().?;
                return try self.emit(ctx, &.{source}, &.{}, &.{}, &.{});
            }
            if (self.pending) {
                const row = try self.emit(ctx, self.front.nodes, self.front.edges, self.back.nodes, self.back.edges);
                self.pending = self.all and self.nextPath();
                return row;
            }

            self.resetSearch();
            self.current_input = try self.input.next(ctx) orelse return null;
            try self.start(ctx);
        }
    }

    /// Search between the ends of the current row and set up the first
    /// path, if there is one.
    fn start(self: *Self, ctx: *ExecutionContext) OperatorError!void {
        const row = self.current_input.?;
        const source_val = row.getSlot(self.source_slot) orelse return OperatorError.UnboundVariable;
        const target_val = row.getSlot(self.target_slot) orelse return OperatorError.UnboundVariable;
        // A null end, as an OPTIONAL MATCH that found nothing leaves, has
        // no path.
        if (source_val.isNull() or target_val.isNull()) return;
        const source = source_val.asNodeId() orelse return OperatorError.TypeError;
        const target = target_val.asNodeId() orelse return OperatorError.TypeError;

        if (source == target) {
            // The shortest path from a node to itself has no relationships;
            // with a minimum of one there is none.
            self.pending_self = self.min_hops == 0;
            return;
        }

        const found = self.meet(ctx, source, target) catch |err| switch (err) {
            error.OutOfMemory => return OperatorError.OutOfMemory,
            else => |e| return e,
        };
        if (!found) return;
        self.meeting_index = 0;
        self.pending = self.setChains() catch return OperatorError.OutOfMemory;
    }

    /// The two-sided search. Fills `meetings` and returns whether the ends
    /// are connected within the bounds.
    fn meet(self: *Self, ctx: *ExecutionContext, source: NodeId, target: NodeId) (OperatorError || error{OutOfMemory})!bool {
        const arena = self.search.allocator();
        try self.forward.put(arena, source, .{ .depth = 0, .parents = .empty });
        try self.backward.put(arena, target, .{ .depth = 0, .parents = .empty });

        var forward_frontier: std.ArrayListUnmanaged(NodeId) = .empty;
        var backward_frontier: std.ArrayListUnmanaged(NodeId) = .empty;
        try forward_frontier.append(arena, source);
        try backward_frontier.append(arena, target);
        var forward_depth: u32 = 0;
        var backward_depth: u32 = 0;

        while (forward_frontier.items.len > 0 and backward_frontier.items.len > 0) {
            if (self.max_hops) |max| {
                if (forward_depth + backward_depth >= max) return false;
            }
            const go_forward = forward_frontier.items.len <= backward_frontier.items.len;
            const frontier = if (go_forward) &forward_frontier else &backward_frontier;
            const depth = if (go_forward) &forward_depth else &backward_depth;
            const mine = if (go_forward) &self.forward else &self.backward;
            const other = if (go_forward) &self.backward else &self.forward;

            var next_frontier: std.ArrayListUnmanaged(NodeId) = .empty;
            var meetings: std.ArrayListUnmanaged(NodeId) = .empty;
            var best: u32 = std.math.maxInt(u32);
            const new_depth = depth.* + 1;

            for (frontier.items) |node| {
                // The backward side walks the pattern's relationships the
                // other way.
                const out = self.direction == .both or (self.direction == .outgoing) == go_forward;
                const in = self.direction == .both or (self.direction == .incoming) == go_forward;
                for ([2]bool{ false, true }) |incoming| {
                    if (incoming and !in) continue;
                    if (!incoming and !out) continue;
                    const edges = try fetchEdgeRefs(self.database, ctx, node, self.constraints.types, incoming);
                    defer self.database.allocator.free(edges);
                    for (edges) |edge| {
                        if (!self.constraints.acceptsType(edge.edge_type)) continue;
                        const neighbour = if (incoming) edge.source else edge.target;
                        if (neighbour == node) continue;
                        const entry = try mine.getOrPut(arena, neighbour);
                        if (!entry.found_existing) {
                            entry.value_ptr.* = .{ .depth = new_depth, .parents = .empty };
                            try entry.value_ptr.parents.append(arena, .{ .edge = edge.id, .node = node });
                            try next_frontier.append(arena, neighbour);
                            if (other.get(neighbour)) |seen| {
                                const total = new_depth + seen.depth;
                                if (total < best) {
                                    best = total;
                                    meetings.clearRetainingCapacity();
                                }
                                if (total == best) try meetings.append(arena, neighbour);
                            }
                        } else if (self.all and entry.value_ptr.depth == new_depth) {
                            try entry.value_ptr.parents.append(arena, .{ .edge = edge.id, .node = node });
                        }
                    }
                }
            }
            depth.* = new_depth;
            if (meetings.items.len > 0) {
                self.meetings = meetings.items;
                return true;
            }
            frontier.* = next_frontier;
        }
        return false;
    }

    /// Start the chains through the current meeting node.
    fn setChains(self: *Self) error{OutOfMemory}!bool {
        if (self.meeting_index >= self.meetings.len) return false;
        const arena = self.search.allocator();
        const meeting = self.meetings[self.meeting_index];
        self.front = try Chain.init(arena, self.forward.get(meeting).?.depth);
        self.back = try Chain.init(arena, self.backward.get(meeting).?.depth);
        self.front.nodes[0] = meeting;
        self.back.nodes[0] = meeting;
        self.front.fill(&self.forward, 0);
        self.back.fill(&self.backward, 0);
        return true;
    }

    /// Move to the next shortest path: the next way back to the target,
    /// then the next way back to the source, then the next meeting node.
    fn nextPath(self: *Self) bool {
        if (self.back.advance(&self.backward)) return true;
        if (self.front.advance(&self.forward)) {
            self.back.fill(&self.backward, 0);
            return true;
        }
        self.meeting_index += 1;
        return self.setChains() catch false;
    }

    /// The row for the path source ... meeting ... target: `front` read
    /// backwards, then `back` after the meeting node.
    fn emit(
        self: *Self,
        ctx: *ExecutionContext,
        front_nodes: []const NodeId,
        front_edges: []const EdgeId,
        back_nodes: []const NodeId,
        back_edges: []const EdgeId,
    ) OperatorError!*Row {
        const output = self.output_row orelse return OperatorError.NotInitialized;
        output.clear();
        output.copyFrom(self.current_input.?);
        if (self.path_slot == null and self.rels_slot == null) return output;

        const keep = ctx.row_arena.allocator();
        const hops = front_edges.len + back_edges.len;
        const edges = keep.alloc(EdgeId, hops) catch return OperatorError.OutOfMemory;
        for (front_edges, 0..) |e, i| edges[front_edges.len - 1 - i] = e;
        @memcpy(edges[front_edges.len..], back_edges);

        if (self.path_slot) |slot| {
            const nodes = keep.alloc(NodeId, hops + 1) catch return OperatorError.OutOfMemory;
            for (front_nodes, 0..) |n, i| nodes[front_nodes.len - 1 - i] = n;
            if (back_nodes.len > 1) @memcpy(nodes[front_nodes.len..], back_nodes[1..]);
            const path = keep.create(executor.Path) catch return OperatorError.OutOfMemory;
            path.* = .{ .nodes = nodes, .edges = edges };
            output.setSlot(slot, .{ .path = path });
        }
        if (self.rels_slot) |slot| {
            const rels = keep.alloc(EvalResult, hops) catch return OperatorError.OutOfMemory;
            for (edges, rels) |e, *r| r.* = .{ .edge_ref = e };
            const list = keep.create(EvalResult) catch return OperatorError.OutOfMemory;
            list.* = .{ .list_val = rels };
            output.setSlot(slot, .{ .value = list });
        }
        return output;
    }

    fn resetSearch(self: *Self) void {
        _ = self.search.reset(.retain_capacity);
        self.forward = .{};
        self.backward = .{};
        self.meetings = &.{};
        self.meeting_index = 0;
        self.pending = false;
        self.pending_self = false;
    }

    fn close(ptr: *anyopaque, ctx: *ExecutionContext) void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        self.resetSearch();
        // The operator itself lives in the query's arena and may never be
        // deinitialised; the search's memory is the database's.
        self.search.deinit();
        self.search = std.heap.ArenaAllocator.init(self.database.allocator);
        if (self.opened) {
            self.input.close(ctx);
            self.opened = false;
        }
    }

    fn deinitOp(ptr: *anyopaque, allocator: Allocator) void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        self.search.deinit();
        self.input.deinit(allocator);
        allocator.destroy(self);
    }
};
