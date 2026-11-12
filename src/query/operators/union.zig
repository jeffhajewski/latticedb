//! Union operator for query execution.
//!
//! Emits every row of its first input, then every row of the next, and so on:
//! the rows of `q1 UNION ALL q2`. `UNION` puts a Distinct on top.
//!
//! An input is opened only when the one before it is exhausted, so the single
//! queries of a union run one after another, in the order they are written,
//! and a write in a later one cannot be seen by an earlier one. Every input
//! that was opened stays open until the union closes, because rows already
//! passed on may point at memory an input releases when it closes.

const std = @import("std");
const Allocator = std.mem.Allocator;

const executor = @import("../executor.zig");
const Operator = executor.Operator;
const OperatorError = executor.OperatorError;
const Row = executor.Row;
const ExecutionContext = executor.ExecutionContext;

pub const Union = struct {
    inputs: []Operator,
    /// How many inputs have been opened. The last of them is the one being
    /// read, unless it is exhausted and it was the final input.
    opened: usize = 0,
    allocator: Allocator,

    const Self = @This();

    /// Create a Union over `inputs`, which it takes ownership of.
    pub fn init(allocator: Allocator, inputs: []Operator) !*Self {
        const self = try allocator.create(Self);
        self.* = .{ .inputs = inputs, .allocator = allocator };
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
        self.opened = 0;
        try self.openNext(ctx);
    }

    fn openNext(self: *Self, ctx: *ExecutionContext) OperatorError!void {
        try self.inputs[self.opened].open(ctx);
        self.opened += 1;
    }

    fn next(ptr: *anyopaque, ctx: *ExecutionContext) OperatorError!?*Row {
        const self: *Self = @ptrCast(@alignCast(ptr));
        while (self.opened > 0) {
            if (try self.inputs[self.opened - 1].next(ctx)) |row| return row;
            if (self.opened == self.inputs.len) return null;
            try self.openNext(ctx);
        }
        return OperatorError.NotInitialized;
    }

    fn close(ptr: *anyopaque, ctx: *ExecutionContext) void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        for (self.inputs[0..self.opened]) |input| input.close(ctx);
        self.opened = 0;
    }

    fn deinitOp(ptr: *anyopaque, allocator: Allocator) void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        for (self.inputs) |input| input.deinit(allocator);
        allocator.free(self.inputs);
        allocator.destroy(self);
    }
};

test "Union vtable is complete" {
    const v = Union.vtable;
    try std.testing.expect(@TypeOf(v.open) != void);
    try std.testing.expect(@TypeOf(v.deinit) != void);
}
