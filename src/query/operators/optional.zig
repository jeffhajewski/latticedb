//! Operators that run a plan once for each row of another: OPTIONAL MATCH
//! and FOREACH.
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

    /// A row set before opening is kept: FOREACH sets the row and then
    /// opens its plan, since a Materialize in it reads its input on open.
    fn open(_: *anyopaque, _: *ExecutionContext) OperatorError!void {}

    fn next(ptr: *anyopaque, _: *ExecutionContext) OperatorError!?*Row {
        const self: *Self = @ptrCast(@alignCast(ptr));
        if (!self.pending) return null;
        self.pending = false;
        return self.row;
    }

    fn close(ptr: *anyopaque, _: *ExecutionContext) void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        self.pending = false;
    }

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

/// FOREACH: for each input row, runs the plan to the end for what it
/// writes, and passes the input row on unchanged.
///
/// Unlike `Apply`, it opens and closes the plan once per row: the writes in
/// it read their input in full when opened (see `Materialize`), so the row
/// has to be in the argument by then.
pub const Foreach = struct {
    input: Operator,
    inner: Operator,
    argument: *Argument,
    opened: bool = false,

    const Self = @This();

    pub fn init(allocator: Allocator, input: Operator, inner: Operator, argument: *Argument) !*Self {
        const self = try allocator.create(Self);
        self.* = .{ .input = input, .inner = inner, .argument = argument };
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
        try self.input.open(ctx);
        self.opened = true;
    }

    fn next(ptr: *anyopaque, ctx: *ExecutionContext) OperatorError!?*Row {
        const self: *Self = @ptrCast(@alignCast(ptr));
        if (!self.opened) return OperatorError.NotInitialized;
        const row = try self.input.next(ctx) orelse return null;
        self.argument.set(row);
        try self.inner.open(ctx);
        defer self.inner.close(ctx);
        while (try self.inner.next(ctx)) |_| {}
        return row;
    }

    fn close(ptr: *anyopaque, ctx: *ExecutionContext) void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        if (self.opened) {
            self.input.close(ctx);
            self.opened = false;
        }
    }

    fn deinitOp(ptr: *anyopaque, allocator: Allocator) void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        self.input.deinit(allocator);
        self.inner.deinit(allocator);
        allocator.destroy(self);
    }
};

/// CALL { ... }: for each input row, runs the subquery's plan from the
/// start, opening it afresh, since a plan with aggregation or a sort reads
/// all of its input when opened. With `columns`, each row the plan returns
/// extends the input row: the plan's column i goes to slot `columns[i]`. A
/// unit subquery (no columns) runs to the end for its writes, and the input
/// row passes on once.
pub const CallSubquery = struct {
    input: Operator,
    inner: Operator,
    /// The leaf of each query of the subquery; more than one with UNION.
    arguments: []const *Argument,
    columns: []const u8,
    current: ?*Row = null,
    inner_open: bool = false,
    output_row: ?*Row = null,
    opened: bool = false,

    const Self = @This();

    pub fn init(allocator: Allocator, input: Operator, inner: Operator, arguments: []const *Argument, columns: []const u8) !*Self {
        const self = try allocator.create(Self);
        self.* = .{ .input = input, .inner = inner, .arguments = arguments, .columns = columns };
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
        self.output_row = ctx.allocRow() catch return OperatorError.OutOfMemory;
        try self.input.open(ctx);
        self.opened = true;
    }

    fn next(ptr: *anyopaque, ctx: *ExecutionContext) OperatorError!?*Row {
        const self: *Self = @ptrCast(@alignCast(ptr));
        if (!self.opened) return OperatorError.NotInitialized;

        while (true) {
            const outer = self.current orelse blk: {
                const row = try self.input.next(ctx) orelse return null;
                for (self.arguments) |argument| argument.set(row);
                try self.inner.open(ctx);
                self.inner_open = true;
                self.current = row;
                break :blk row;
            };

            if (self.columns.len == 0) {
                // A unit subquery: run it for its writes.
                while (try self.inner.next(ctx)) |_| {}
                self.finishRound(ctx);
                return outer;
            }

            if (try self.inner.next(ctx)) |row| {
                const out = self.output_row orelse return OperatorError.NotInitialized;
                out.* = outer.*;
                for (self.columns, 0..) |slot, i| {
                    out.setSlot(slot, row.getSlot(@intCast(i)) orelse .{ .property = .{ .null_val = {} } });
                }
                return out;
            }
            self.finishRound(ctx);
        }
    }

    fn finishRound(self: *Self, ctx: *ExecutionContext) void {
        if (self.inner_open) {
            self.inner.close(ctx);
            self.inner_open = false;
        }
        self.current = null;
    }

    fn close(ptr: *anyopaque, ctx: *ExecutionContext) void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        self.finishRound(ctx);
        if (self.opened) {
            self.input.close(ctx);
            self.opened = false;
        }
    }

    fn deinitOp(ptr: *anyopaque, allocator: Allocator) void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        self.input.deinit(allocator);
        self.inner.deinit(allocator);
        allocator.destroy(self);
    }
};

test "Apply vtable is complete" {
    const v = Apply.vtable;
    try std.testing.expect(@TypeOf(v.open) != void);
    try std.testing.expect(@TypeOf(v.deinit) != void);
}
