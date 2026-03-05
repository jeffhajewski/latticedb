//! Materialize operator for query execution.
//!
//! Buffers all input rows during open(), closes the input to release
//! page latches, then replays rows from next(). This breaks the
//! read-write latch conflict that causes self-deadlock when mutation
//! operators (SET, DELETE, REMOVE) write to pages still held by
//! upstream scan iterators.

const std = @import("std");
const Allocator = std.mem.Allocator;

const executor = @import("../executor.zig");
const Operator = executor.Operator;
const OperatorError = executor.OperatorError;
const Row = executor.Row;
const ExecutionContext = executor.ExecutionContext;

pub const Materialize = struct {
    input: Operator,
    allocator: Allocator,
    rows: std.ArrayListUnmanaged(Row),
    output_index: usize,
    opened: bool,

    const Self = @This();

    pub fn init(allocator: Allocator, input: Operator) !*Self {
        const self = try allocator.create(Self);
        self.* = Self{
            .input = input,
            .allocator = allocator,
            .rows = .empty,
            .output_index = 0,
            .opened = false,
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
        .deinit = deinitOp,
    };

    fn open(ptr: *anyopaque, ctx: *ExecutionContext) OperatorError!void {
        const self: *Self = @ptrCast(@alignCast(ptr));

        // Open input and drain all rows
        try self.input.open(ctx);

        while (true) {
            const row = try self.input.next(ctx) orelse break;
            self.rows.append(self.allocator, row.clone()) catch return OperatorError.OutOfMemory;
        }

        // Close input to release all page latches
        self.input.close(ctx);

        self.opened = true;
        self.output_index = 0;
    }

    fn next(ptr: *anyopaque, ctx: *ExecutionContext) OperatorError!?*Row {
        const self: *Self = @ptrCast(@alignCast(ptr));

        if (!self.opened) return OperatorError.NotInitialized;

        if (self.output_index >= self.rows.items.len) return null;

        const output_row = ctx.allocRow() catch return OperatorError.OutOfMemory;
        output_row.* = self.rows.items[self.output_index];
        self.output_index += 1;

        return output_row;
    }

    fn close(ptr: *anyopaque, ctx: *ExecutionContext) void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        // Input already closed in open() - nothing to do
        _ = ctx;
        _ = self;
    }

    fn deinitOp(ptr: *anyopaque, allocator: Allocator) void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        self.rows.deinit(allocator);
        self.input.deinit(allocator);
        allocator.destroy(self);
    }
};

test "Materialize basic structure" {
    const vtable_check = Materialize.vtable;
    try std.testing.expect(@TypeOf(vtable_check.open) != void);
    try std.testing.expect(@TypeOf(vtable_check.next) != void);
    try std.testing.expect(@TypeOf(vtable_check.close) != void);
    try std.testing.expect(@TypeOf(vtable_check.deinit) != void);
}
