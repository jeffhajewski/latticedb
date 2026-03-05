//! Cross-product (nested-loop join) operator for query execution.
//!
//! Produces the Cartesian product of two independent scans.
//! Used for multi-pattern MATCH like: MATCH (a:X), (b:Y)

const std = @import("std");
const Allocator = std.mem.Allocator;

const executor = @import("../executor.zig");
const Operator = executor.Operator;
const OperatorError = executor.OperatorError;
const Row = executor.Row;
const ExecutionContext = executor.ExecutionContext;

pub const CrossProduct = struct {
    /// Outer (left) input
    outer: Operator,
    /// Inner (right) input — must be re-scannable via materialize
    inner: Operator,
    allocator: Allocator,
    /// Materialized inner rows
    inner_rows: std.ArrayListUnmanaged(Row),
    /// Current outer row
    outer_row: ?Row,
    /// Index into inner_rows for current outer row
    inner_idx: usize,
    opened: bool,

    const Self = @This();

    pub fn init(allocator: Allocator, outer: Operator, inner: Operator) !*Self {
        const self = try allocator.create(Self);
        self.* = Self{
            .outer = outer,
            .inner = inner,
            .allocator = allocator,
            .inner_rows = .empty,
            .outer_row = null,
            .inner_idx = 0,
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

        // Open and materialize inner side
        try self.inner.open(ctx);
        while (true) {
            const row = try self.inner.next(ctx) orelse break;
            self.inner_rows.append(self.allocator, row.clone()) catch return OperatorError.OutOfMemory;
        }
        self.inner.close(ctx);

        // Open outer side
        try self.outer.open(ctx);
        self.opened = true;
    }

    fn next(ptr: *anyopaque, ctx: *ExecutionContext) OperatorError!?*Row {
        const self: *Self = @ptrCast(@alignCast(ptr));
        if (!self.opened) return OperatorError.NotInitialized;

        while (true) {
            // If no current outer row, fetch one
            if (self.outer_row == null) {
                const row = try self.outer.next(ctx) orelse return null;
                self.outer_row = row.clone();
                self.inner_idx = 0;
            }

            // If we've exhausted inner rows for this outer row, move to next outer
            if (self.inner_idx >= self.inner_rows.items.len) {
                self.outer_row = null;
                continue;
            }

            // Combine outer and inner rows
            const output = ctx.allocRow() catch return OperatorError.OutOfMemory;
            output.* = self.outer_row.?;

            // Merge inner row slots into output
            const inner_row = &self.inner_rows.items[self.inner_idx];
            self.inner_idx += 1;

            var slot: u8 = 0;
            while (slot < executor.MAX_SLOTS) : (slot += 1) {
                if (inner_row.hasSlot(slot)) {
                    output.setSlot(slot, inner_row.slots[slot]);
                }
            }

            return output;
        }
    }

    fn close(ptr: *anyopaque, ctx: *ExecutionContext) void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        self.outer.close(ctx);
        // inner already closed in open()
    }

    fn deinitOp(ptr: *anyopaque, allocator: Allocator) void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        self.inner_rows.deinit(allocator);
        self.outer.deinit(allocator);
        self.inner.deinit(allocator);
        allocator.destroy(self);
    }
};
