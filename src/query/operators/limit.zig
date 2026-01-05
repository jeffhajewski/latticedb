//! Limit, Skip, and Sort operators for query execution.
//!
//! - Limit: Returns at most N rows
//! - Skip: Skips the first N rows
//! - Sort: Orders rows by expression

const std = @import("std");
const Allocator = std.mem.Allocator;

const executor = @import("../executor.zig");
const Operator = executor.Operator;
const OperatorError = executor.OperatorError;
const Row = executor.Row;
const ExecutionContext = executor.ExecutionContext;
const MAX_SLOTS = executor.MAX_SLOTS;

const expression = @import("../expression.zig");
const ExpressionEvaluator = expression.ExpressionEvaluator;
const EvalResult = expression.EvalResult;
const EvalError = expression.EvalError;

const ast = @import("../ast.zig");

// ============================================================================
// Limit Operator
// ============================================================================

/// Limit operator that returns at most N rows.
pub const Limit = struct {
    /// Input operator
    input: Operator,
    /// Maximum rows to return
    count: u64,
    /// Number of rows returned so far
    returned: u64,
    /// Whether opened
    opened: bool,

    const Self = @This();

    /// Create a new Limit operator
    pub fn init(allocator: Allocator, input: Operator, count: u64) !*Self {
        const self = try allocator.create(Self);
        self.* = Self{
            .input = input,
            .count = count,
            .returned = 0,
            .opened = false,
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
        try self.input.open(ctx);
        self.returned = 0;
        self.opened = true;
    }

    fn next(ptr: *anyopaque, ctx: *ExecutionContext) OperatorError!?*Row {
        const self: *Self = @ptrCast(@alignCast(ptr));

        if (!self.opened) return OperatorError.NotInitialized;

        // Check if we've hit the limit
        if (self.returned >= self.count) {
            return null;
        }

        // Get next row from input
        const row = try self.input.next(ctx) orelse return null;
        self.returned += 1;
        return row;
    }

    fn close(ptr: *anyopaque, ctx: *ExecutionContext) void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        if (self.opened) {
            self.input.close(ctx);
            self.opened = false;
        }
    }

    fn deinit(ptr: *anyopaque, allocator: Allocator) void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        self.input.deinit(allocator);
        allocator.destroy(self);
    }
};

// ============================================================================
// Skip Operator
// ============================================================================

/// Skip operator that discards the first N rows.
pub const Skip = struct {
    /// Input operator
    input: Operator,
    /// Number of rows to skip
    count: u64,
    /// Number of rows skipped so far
    skipped: u64,
    /// Whether opened
    opened: bool,

    const Self = @This();

    /// Create a new Skip operator
    pub fn init(allocator: Allocator, input: Operator, count: u64) !*Self {
        const self = try allocator.create(Self);
        self.* = Self{
            .input = input,
            .count = count,
            .skipped = 0,
            .opened = false,
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
        try self.input.open(ctx);
        self.skipped = 0;
        self.opened = true;
    }

    fn next(ptr: *anyopaque, ctx: *ExecutionContext) OperatorError!?*Row {
        const self: *Self = @ptrCast(@alignCast(ptr));

        if (!self.opened) return OperatorError.NotInitialized;

        // Skip rows if needed
        while (self.skipped < self.count) {
            const row = try self.input.next(ctx) orelse return null;
            _ = row;
            self.skipped += 1;
        }

        // Return remaining rows
        return try self.input.next(ctx);
    }

    fn close(ptr: *anyopaque, ctx: *ExecutionContext) void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        if (self.opened) {
            self.input.close(ctx);
            self.opened = false;
        }
    }

    fn deinit(ptr: *anyopaque, allocator: Allocator) void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        self.input.deinit(allocator);
        allocator.destroy(self);
    }
};

// ============================================================================
// Sort Operator
// ============================================================================

/// Sort order item
pub const SortItem = struct {
    /// Expression to sort by
    expr: *const ast.Expression,
    /// Sort descending
    descending: bool,
};

/// Sort operator that orders rows by expression(s).
/// Note: This is a blocking operator that materializes all input.
pub const Sort = struct {
    /// Input operator
    input: Operator,
    /// Sort items
    sort_items: []const SortItem,
    /// Allocator
    allocator: Allocator,
    /// Materialized rows
    rows: std.ArrayList(Row),
    /// Current output index
    output_index: usize,
    /// Expression evaluator
    evaluator: ExpressionEvaluator,
    /// Whether opened
    opened: bool,
    /// Execution context (for sorting comparison)
    ctx: ?*ExecutionContext,

    const Self = @This();

    /// Create a new Sort operator
    pub fn init(allocator: Allocator, input: Operator, sort_items: []const SortItem) !*Self {
        const self = try allocator.create(Self);
        self.* = Self{
            .input = input,
            .sort_items = sort_items,
            .allocator = allocator,
            .rows = .empty,
            .output_index = 0,
            .evaluator = ExpressionEvaluator.init(allocator),
            .opened = false,
            .ctx = null,
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

        // Open input and materialize all rows
        try self.input.open(ctx);
        self.ctx = ctx;

        while (try self.input.next(ctx)) |row| {
            self.rows.append(self.allocator, row.*) catch return OperatorError.OutOfMemory;
        }

        // Sort the rows
        // Note: We can't use the sort comparison function directly with ctx
        // because Zig's sort doesn't support closures. We use a workaround.
        if (self.sort_items.len > 0) {
            self.sortRows();
        }

        self.output_index = 0;
        self.opened = true;
    }

    fn sortRows(self: *Self) void {
        // Simple bubble sort for now (not optimal but works)
        // TODO: Replace with more efficient sorting
        const items = self.rows.items;
        if (items.len <= 1) return;

        var i: usize = 0;
        while (i < items.len - 1) : (i += 1) {
            var j: usize = 0;
            while (j < items.len - 1 - i) : (j += 1) {
                if (self.compareRows(&items[j], &items[j + 1]) > 0) {
                    std.mem.swap(Row, &items[j], &items[j + 1]);
                }
            }
        }
    }

    fn compareRows(self: *Self, a: *const Row, b: *const Row) i32 {
        const ctx = self.ctx orelse return 0;

        for (self.sort_items) |item| {
            const a_val = self.evaluator.evaluate(item.expr, a, ctx) catch {
                return 0;
            };
            const b_val = self.evaluator.evaluate(item.expr, b, ctx) catch {
                return 0;
            };

            const cmp = compareEvalResults(a_val, b_val);
            if (cmp != 0) {
                return if (item.descending) -cmp else cmp;
            }
        }
        return 0;
    }

    fn next(ptr: *anyopaque, _: *ExecutionContext) OperatorError!?*Row {
        const self: *Self = @ptrCast(@alignCast(ptr));

        if (!self.opened) return OperatorError.NotInitialized;

        if (self.output_index >= self.rows.items.len) {
            return null;
        }

        const row = &self.rows.items[self.output_index];
        self.output_index += 1;
        return row;
    }

    fn close(ptr: *anyopaque, ctx: *ExecutionContext) void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        if (self.opened) {
            self.input.close(ctx);
            self.rows.clearAndFree(self.allocator);
            self.opened = false;
        }
    }

    fn deinit(ptr: *anyopaque, allocator: Allocator) void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        self.rows.deinit(self.allocator);
        self.input.deinit(allocator);
        allocator.destroy(self);
    }
};

/// Compare two EvalResults for sorting
fn compareEvalResults(a: EvalResult, b: EvalResult) i32 {
    // Handle nulls first (nulls sort last)
    if (a.isNull() and b.isNull()) return 0;
    if (a.isNull()) return 1;
    if (b.isNull()) return -1;

    // Compare by type
    switch (a) {
        .int_val => |av| {
            switch (b) {
                .int_val => |bv| {
                    if (av < bv) return -1;
                    if (av > bv) return 1;
                    return 0;
                },
                .float_val => |bv| {
                    const af: f64 = @floatFromInt(av);
                    if (af < bv) return -1;
                    if (af > bv) return 1;
                    return 0;
                },
                else => return 0,
            }
        },
        .float_val => |av| {
            switch (b) {
                .float_val => |bv| {
                    if (av < bv) return -1;
                    if (av > bv) return 1;
                    return 0;
                },
                .int_val => |bv| {
                    const bf: f64 = @floatFromInt(bv);
                    if (av < bf) return -1;
                    if (av > bf) return 1;
                    return 0;
                },
                else => return 0,
            }
        },
        .string_val => |av| {
            switch (b) {
                .string_val => |bv| {
                    return switch (std.mem.order(u8, av, bv)) {
                        .lt => -1,
                        .gt => 1,
                        .eq => 0,
                    };
                },
                else => return 0,
            }
        },
        .bool_val => |av| {
            switch (b) {
                .bool_val => |bv| {
                    if (!av and bv) return -1;
                    if (av and !bv) return 1;
                    return 0;
                },
                else => return 0,
            }
        },
        else => return 0,
    }
}

// ============================================================================
// Tests
// ============================================================================

test "Limit basic structure" {
    const vtable = Limit.vtable;
    try std.testing.expect(@TypeOf(vtable.open) != void);
    try std.testing.expect(@TypeOf(vtable.next) != void);
    try std.testing.expect(@TypeOf(vtable.close) != void);
    try std.testing.expect(@TypeOf(vtable.deinit) != void);
}

test "Skip basic structure" {
    const vtable = Skip.vtable;
    try std.testing.expect(@TypeOf(vtable.open) != void);
    try std.testing.expect(@TypeOf(vtable.next) != void);
    try std.testing.expect(@TypeOf(vtable.close) != void);
    try std.testing.expect(@TypeOf(vtable.deinit) != void);
}

test "Sort basic structure" {
    const vtable = Sort.vtable;
    try std.testing.expect(@TypeOf(vtable.open) != void);
    try std.testing.expect(@TypeOf(vtable.next) != void);
    try std.testing.expect(@TypeOf(vtable.close) != void);
    try std.testing.expect(@TypeOf(vtable.deinit) != void);
}

test "compareEvalResults ordering" {
    // Integers
    try std.testing.expectEqual(@as(i32, -1), compareEvalResults(.{ .int_val = 1 }, .{ .int_val = 2 }));
    try std.testing.expectEqual(@as(i32, 1), compareEvalResults(.{ .int_val = 2 }, .{ .int_val = 1 }));
    try std.testing.expectEqual(@as(i32, 0), compareEvalResults(.{ .int_val = 1 }, .{ .int_val = 1 }));

    // Strings
    try std.testing.expectEqual(@as(i32, -1), compareEvalResults(.{ .string_val = "a" }, .{ .string_val = "b" }));
    try std.testing.expectEqual(@as(i32, 1), compareEvalResults(.{ .string_val = "b" }, .{ .string_val = "a" }));

    // Nulls sort last
    try std.testing.expectEqual(@as(i32, 1), compareEvalResults(.{ .null_val = {} }, .{ .int_val = 1 }));
    try std.testing.expectEqual(@as(i32, -1), compareEvalResults(.{ .int_val = 1 }, .{ .null_val = {} }));
}
