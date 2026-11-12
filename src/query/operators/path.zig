//! BindPath: puts a named path (`p = (a)-[r]->(b)-[*1..3]->(c)`) together
//! from the slots its pattern filled, and stores it in the path's slot.

const std = @import("std");
const Allocator = std.mem.Allocator;

const executor = @import("../executor.zig");
const Operator = executor.Operator;
const OperatorError = executor.OperatorError;
const Row = executor.Row;
const ExecutionContext = executor.ExecutionContext;
const Path = executor.Path;

const types = @import("../../core/types.zig");
const NodeId = types.NodeId;
const EdgeId = types.EdgeId;

/// One relationship of the pattern and the node after it.
pub const Step = union(enum) {
    /// A single relationship: the slot holding it, and the node it reaches.
    single: struct { edge_slot: u8, node_slot: u8 },
    /// A variable-length relationship: the slot holding the walk it took
    /// (a path from the node before it to the node after it).
    walk: u8,
};

pub const BindPath = struct {
    input: Operator,
    start_slot: u8,
    steps: []const Step,
    path_slot: u8,
    /// Where paths are kept: the query's allocator, since a path can be
    /// passed on by any later clause.
    allocator: Allocator,
    output_row: ?*Row = null,

    const Self = @This();

    pub fn init(allocator: Allocator, input: Operator, start_slot: u8, steps: []const Step, path_slot: u8) !*Self {
        const self = try allocator.create(Self);
        self.* = .{
            .input = input,
            .start_slot = start_slot,
            .steps = steps,
            .path_slot = path_slot,
            .allocator = allocator,
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
        try self.input.open(ctx);
    }

    fn next(ptr: *anyopaque, ctx: *ExecutionContext) OperatorError!?*Row {
        const self: *Self = @ptrCast(@alignCast(ptr));
        const out = self.output_row orelse return OperatorError.NotInitialized;
        const row = try self.input.next(ctx) orelse return null;
        out.* = row.*;
        out.setSlot(self.path_slot, try self.build(row));
        return out;
    }

    fn build(self: *Self, row: *const Row) OperatorError!executor.SlotValue {
        var nodes: std.ArrayList(NodeId) = .empty;
        var edges: std.ArrayList(EdgeId) = .empty;
        const a = self.allocator;
        nodes.append(a, try nodeAt(row, self.start_slot)) catch return OperatorError.OutOfMemory;
        for (self.steps) |step| switch (step) {
            .single => |s| {
                const edge = (row.getSlot(s.edge_slot) orelse return OperatorError.UnboundVariable).asEdgeId() orelse
                    return OperatorError.TypeError;
                edges.append(a, edge) catch return OperatorError.OutOfMemory;
                nodes.append(a, try nodeAt(row, s.node_slot)) catch return OperatorError.OutOfMemory;
            },
            .walk => |slot| {
                const walk = switch (row.getSlot(slot) orelse return OperatorError.UnboundVariable) {
                    .path => |p| p,
                    else => return OperatorError.TypeError,
                };
                edges.appendSlice(a, walk.edges) catch return OperatorError.OutOfMemory;
                if (walk.nodes.len > 1) nodes.appendSlice(a, walk.nodes[1..]) catch return OperatorError.OutOfMemory;
            },
        };
        const path = a.create(Path) catch return OperatorError.OutOfMemory;
        path.* = .{
            .nodes = nodes.toOwnedSlice(a) catch return OperatorError.OutOfMemory,
            .edges = edges.toOwnedSlice(a) catch return OperatorError.OutOfMemory,
        };
        return .{ .path = path };
    }

    fn nodeAt(row: *const Row, slot: u8) OperatorError!NodeId {
        const value = row.getSlot(slot) orelse return OperatorError.UnboundVariable;
        return value.asNodeId() orelse OperatorError.TypeError;
    }

    fn close(ptr: *anyopaque, ctx: *ExecutionContext) void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        self.input.close(ctx);
    }

    fn deinitOp(ptr: *anyopaque, allocator: Allocator) void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        self.input.deinit(allocator);
        allocator.free(self.steps);
        allocator.destroy(self);
    }
};

test "BindPath vtable is complete" {
    const v = BindPath.vtable;
    try std.testing.expect(@TypeOf(v.open) != void);
}
