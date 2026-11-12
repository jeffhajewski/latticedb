//! Operators that run a plan once for each row of another: OPTIONAL MATCH.
//!
//! `Apply` takes a row from its input, hands it to the plan's `Argument` leaf,
//! and passes on every row the plan produces from it. The plan is opened once;
//! its leaf yields the current row once and then reports exhaustion, so each
//! input row is one round through the same open plan. Operators that can sit
//! above an `Argument` therefore have to accept an input that runs out and then
//! has rows again, which plain filters, expands and cross products do.
//!
//! With `optional` set, an input row the plan produces nothing from is passed
//! on once, with the slots the plan would have filled set to null: the row
//! OPTIONAL MATCH keeps when its pattern does not match.

const std = @import("std");
const Allocator = std.mem.Allocator;

const executor = @import("../executor.zig");
const Operator = executor.Operator;
const OperatorError = executor.OperatorError;
const Row = executor.Row;
const ExecutionContext = executor.ExecutionContext;
const MAX_SLOTS = executor.MAX_SLOTS;

/// The leaf of a plan run by `Apply`: the current outer row, once.
pub const Argument = struct {
    row: ?*Row = null,
    pending: bool = false,

    const Self = @This();

    pub fn init(allocator: Allocator) !*Self {
        const self = try allocator.create(Self);
        self.* = .{};
        return self;
    }

    pub fn operator(self: *Self) Operator {
        return .{ .vtable = &vtable, .ptr = self };
    }

    /// Make `row` the row the next round yields.
    pub fn set(self: *Self, row: *Row) void {
        self.row = row;
        self.pending = true;
    }

    const vtable = Operator.VTable{
        .open = open,
        .next = next,
        .close = close,
        .deinit = deinitOp,
    };

    fn open(ptr: *anyopaque, _: *ExecutionContext) OperatorError!void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        self.pending = false;
    }

    fn next(ptr: *anyopaque, _: *ExecutionContext) OperatorError!?*Row {
        const self: *Self = @ptrCast(@alignCast(ptr));
        if (!self.pending) return null;
        self.pending = false;
        return self.row;
    }

    fn close(_: *anyopaque, _: *ExecutionContext) void {}

    fn deinitOp(ptr: *anyopaque, allocator: Allocator) void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        allocator.destroy(self);
    }
};

pub const Apply = struct {
    input: Operator,
    /// The plan run for each input row. Its leaf is `argument`.
    inner: Operator,
    argument: *Argument,
    /// Keep an input row the inner plan produces nothing from.
    optional: bool,
    /// The slots the inner plan binds, set to null in a kept row.
    new_slots: u16,
    /// The input row of the current round, until its last row is passed on.
    current: ?*Row = null,
    /// Whether the current round has produced a row yet.
    matched: bool = false,
    output_row: ?*Row = null,
    opened: bool = false,
    inner_opened: bool = false,

    const Self = @This();

    /// Create an Apply that owns `input` and `inner` (and through it the
    /// argument).
    pub fn init(allocator: Allocator, input: Operator, inner: Operator, argument: *Argument, optional: bool, new_slots: u16) !*Self {
        const self = try allocator.create(Self);
        self.* = .{
            .input = input,
            .inner = inner,
            .argument = argument,
            .optional = optional,
            .new_slots = new_slots,
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
        self.current = null;
        self.matched = false;
        try self.input.open(ctx);
        self.opened = true;
        // The caller closes nothing whose open failed, so the input is
        // closed here if the inner plan cannot open.
        errdefer {
            self.input.close(ctx);
            self.opened = false;
        }
        self.output_row = ctx.allocRow() catch return OperatorError.OutOfMemory;
        try self.inner.open(ctx);
        self.inner_opened = true;
    }

    fn next(ptr: *anyopaque, ctx: *ExecutionContext) OperatorError!?*Row {
        const self: *Self = @ptrCast(@alignCast(ptr));
        if (!self.opened) return OperatorError.NotInitialized;

        while (true) {
            const outer = self.current orelse blk: {
                const row = try self.input.next(ctx) orelse return null;
                self.current = row;
                self.matched = false;
                self.argument.set(row);
                break :blk row;
            };

            if (try self.inner.next(ctx)) |row| {
                self.matched = true;
                return row;
            }

            // The round is over.
            self.current = null;
            if (self.matched or !self.optional) continue;

            const out = self.output_row orelse return OperatorError.NotInitialized;
            out.* = outer.*;
            var slot: u8 = 0;
            while (slot < MAX_SLOTS) : (slot += 1) {
                if (self.new_slots & (@as(u16, 1) << @intCast(slot)) != 0) {
                    out.setSlot(slot, .{ .property = .{ .null_val = {} } });
                }
            }
            return out;
        }
    }

    fn close(ptr: *anyopaque, ctx: *ExecutionContext) void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        if (self.inner_opened) {
            self.inner.close(ctx);
            self.inner_opened = false;
        }
        if (self.opened) {
            self.input.close(ctx);
            self.opened = false;
        }
    }

    fn deinitOp(ptr: *anyopaque, allocator: Allocator) void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        self.input.deinit(allocator);
        // The inner plan's leaf is the argument, which it frees.
        self.inner.deinit(allocator);
        allocator.destroy(self);
    }
};

test "Apply vtable is complete" {
    const v = Apply.vtable;
    try std.testing.expect(@TypeOf(v.open) != void);
    try std.testing.expect(@TypeOf(v.deinit) != void);
}
