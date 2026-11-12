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
const PropertyValue = @import("lattice").core.types.PropertyValue;

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
    /// Maximum rows to return (resolved at open time if count_expr is set)
    count: u64,
    /// Optional runtime LIMIT expression (supports parameters)
    count_expr: ?*const ast.Expression,
    /// Number of rows returned so far
    returned: u64,
    /// Expression evaluator for count_expr
    evaluator: ExpressionEvaluator,
    /// Whether opened
    opened: bool,

    const Self = @This();

    /// Create a new Limit operator
    pub fn init(allocator: Allocator, input: Operator, count: u64) !*Self {
        const self = try allocator.create(Self);
        self.* = Self{
            .input = input,
            .count = count,
            .count_expr = null,
            .returned = 0,
            .evaluator = ExpressionEvaluator.init(allocator),
            .opened = false,
        };
        return self;
    }

    /// Create a new Limit operator with an expression evaluated at open time.
    pub fn initExpr(allocator: Allocator, input: Operator, count_expr: *const ast.Expression) !*Self {
        const self = try allocator.create(Self);
        self.* = Self{
            .input = input,
            .count = 0,
            .count_expr = count_expr,
            .returned = 0,
            .evaluator = ExpressionEvaluator.init(allocator),
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

        if (self.count_expr) |expr| {
            self.count = try evaluateCountExpression(&self.evaluator, expr, ctx);
        }
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
    /// Number of rows to skip (resolved at open time if count_expr is set)
    count: u64,
    /// Optional runtime SKIP expression (supports parameters)
    count_expr: ?*const ast.Expression,
    /// Number of rows skipped so far
    skipped: u64,
    /// Expression evaluator for count_expr
    evaluator: ExpressionEvaluator,
    /// Whether opened
    opened: bool,

    const Self = @This();

    /// Create a new Skip operator
    pub fn init(allocator: Allocator, input: Operator, count: u64) !*Self {
        const self = try allocator.create(Self);
        self.* = Self{
            .input = input,
            .count = count,
            .count_expr = null,
            .skipped = 0,
            .evaluator = ExpressionEvaluator.init(allocator),
            .opened = false,
        };
        return self;
    }

    /// Create a new Skip operator with an expression evaluated at open time.
    pub fn initExpr(allocator: Allocator, input: Operator, count_expr: *const ast.Expression) !*Self {
        const self = try allocator.create(Self);
        self.* = Self{
            .input = input,
            .count = 0,
            .count_expr = count_expr,
            .skipped = 0,
            .evaluator = ExpressionEvaluator.init(allocator),
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

        if (self.count_expr) |expr| {
            self.count = try evaluateCountExpression(&self.evaluator, expr, ctx);
        }
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
    /// Expression to sort by, used when `slot` is null.
    expr: *const ast.Expression,
    /// Sort descending
    descending: bool,
    /// Read this slot of the row instead of evaluating `expr`.
    ///
    /// Sorting the output of an aggregation cannot work by evaluating: the
    /// expression is something like `count(p)`, and the rows reaching the sort
    /// have already been grouped, so there is no longer a `p` to count. The
    /// value is sitting in a column of the row, and this says which one.
    slot: ?u8 = null,
};

/// A SKIP or LIMIT that follows a sort, so the sort knows how many of its
/// rows can ever be read.
pub const RowBound = union(enum) {
    skip: *const ast.Expression,
    limit: *const ast.Expression,
};

/// Sort operator that orders rows by expression(s).
///
/// A blocking operator: it reads all of its input in `open`. Each row's sort
/// keys are evaluated once, when the row arrives, rather than inside the
/// comparator, where every comparison evaluated both rows' keys again. When
/// a LIMIT follows, only the rows that can be returned are kept, in a
/// bounded heap, so `ORDER BY ... LIMIT k` over n rows costs n log k
/// comparisons and k rows of memory instead of a full sort.
///
/// Ties keep input order, so the result is the same as a stable sort of
/// everything followed by SKIP and LIMIT.
pub const Sort = struct {
    /// Input operator
    input: Operator,
    /// Sort items
    sort_items: []const SortItem,
    /// SKIP and LIMIT directly above this sort, in the order they apply.
    bounds: []const RowBound,
    /// Allocator
    allocator: Allocator,
    /// Buffered rows with their sort keys
    entries: std.ArrayList(Entry),
    /// Indices into `entries` in output order (a heap while bounded)
    order: std.ArrayList(u32),
    /// Current output index
    output_index: usize,
    /// Expression evaluator
    evaluator: ExpressionEvaluator,
    /// Whether opened
    opened: bool,

    const Entry = struct {
        row: Row,
        keys: []EvalResult,
        /// Position in the input, the final tie-breaker.
        seq: u64,
    };

    const Self = @This();

    /// Create a new Sort operator
    pub fn init(allocator: Allocator, input: Operator, sort_items: []const SortItem) !*Self {
        return initBounded(allocator, input, sort_items, &.{});
    }

    /// Create a Sort that knows the SKIP and LIMIT applied to its output.
    pub fn initBounded(
        allocator: Allocator,
        input: Operator,
        sort_items: []const SortItem,
        bounds: []const RowBound,
    ) !*Self {
        const self = try allocator.create(Self);
        self.* = Self{
            .input = input,
            .sort_items = sort_items,
            .bounds = bounds,
            .allocator = allocator,
            .entries = .empty,
            .order = .empty,
            .output_index = 0,
            .evaluator = ExpressionEvaluator.init(allocator),
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

    /// How many rows the SKIP and LIMIT above can read, or null for all.
    fn rowsNeeded(self: *Self, ctx: *ExecutionContext) OperatorError!?u64 {
        var needed: ?u64 = null;
        var i = self.bounds.len;
        while (i > 0) {
            i -= 1;
            switch (self.bounds[i]) {
                .limit => |expr| {
                    const n = try evaluateCountExpression(&self.evaluator, expr, ctx);
                    needed = if (needed) |m| @min(m, n) else n;
                },
                .skip => |expr| {
                    const n = try evaluateCountExpression(&self.evaluator, expr, ctx);
                    if (needed) |m| needed = std.math.add(u64, m, n) catch null;
                },
            }
        }
        return needed;
    }

    fn open(ptr: *anyopaque, ctx: *ExecutionContext) OperatorError!void {
        const self: *Self = @ptrCast(@alignCast(ptr));

        // Open input and buffer all rows. If that fails, close the input
        // here: the caller never closes an operator whose open failed.
        try self.input.open(ctx);
        errdefer self.input.close(ctx);

        const keep = try self.rowsNeeded(ctx);
        const key_count = self.sort_items.len;
        const scratch = self.allocator.alloc(EvalResult, key_count) catch return OperatorError.OutOfMemory;
        defer self.allocator.free(scratch);

        // Every input row is read even when none can be kept: the rows
        // below may be writing.
        var seq: u64 = 0;
        while (try self.input.next(ctx)) |row| : (seq += 1) {
            try self.evaluateKeys(row, ctx, scratch);
            const candidate = Entry{ .row = undefined, .keys = scratch, .seq = seq };

            if (keep) |k| {
                if (k == 0) continue;
                if (self.order.items.len == k) {
                    // Full: the candidate replaces the worst kept row if it
                    // sorts before it, and is dropped otherwise.
                    const worst = self.order.items[0];
                    if (self.compareEntries(&candidate, &self.entries.items[worst]) != .lt) continue;
                    const slot = &self.entries.items[worst];
                    slot.row = row.*;
                    @memcpy(slot.keys, scratch);
                    slot.seq = seq;
                    self.siftDown(0);
                    continue;
                }
            }

            const keys = self.allocator.dupe(EvalResult, scratch) catch return OperatorError.OutOfMemory;
            const index: u32 = std.math.cast(u32, self.entries.items.len) orelse return OperatorError.OutOfMemory;
            self.entries.append(self.allocator, .{ .row = row.*, .keys = keys, .seq = seq }) catch
                return OperatorError.OutOfMemory;
            self.order.append(self.allocator, index) catch return OperatorError.OutOfMemory;
            if (keep != null) self.siftUp(self.order.items.len - 1);
        }

        std.mem.sort(u32, self.order.items, self, indexLessThan);

        self.output_index = 0;
        self.opened = true;
    }

    fn evaluateKeys(self: *Self, row: *const Row, ctx: *ExecutionContext, keys: []EvalResult) OperatorError!void {
        for (self.sort_items, keys) |item, *key| {
            key.* = if (item.slot) |slot|
                keyFromSlot(row, slot, self.allocator) catch return OperatorError.OutOfMemory
            else
                self.evaluator.evaluate(item.expr, row, ctx) catch |err| return mapEvalError(err);
        }
    }

    fn compareEntries(self: *const Self, a: *const Entry, b: *const Entry) std.math.Order {
        for (self.sort_items, a.keys, b.keys) |item, a_key, b_key| {
            const cmp = orderValues(a_key, b_key);
            if (cmp != .eq) return if (item.descending) cmp.invert() else cmp;
        }
        return std.math.order(a.seq, b.seq);
    }

    fn indexLessThan(self: *const Self, a: u32, b: u32) bool {
        return self.compareEntries(&self.entries.items[a], &self.entries.items[b]) == .lt;
    }

    // The bounded heap keeps the worst kept row at the top.
    fn heapAbove(self: *const Self, a: usize, b: usize) bool {
        return self.indexLessThan(self.order.items[b], self.order.items[a]);
    }

    fn siftUp(self: *Self, start: usize) void {
        var i = start;
        while (i > 0) {
            const parent = (i - 1) / 2;
            if (!self.heapAbove(i, parent)) break;
            std.mem.swap(u32, &self.order.items[i], &self.order.items[parent]);
            i = parent;
        }
    }

    fn siftDown(self: *Self, start: usize) void {
        const len = self.order.items.len;
        var i = start;
        while (true) {
            var top = i;
            const left = 2 * i + 1;
            const right = left + 1;
            if (left < len and self.heapAbove(left, top)) top = left;
            if (right < len and self.heapAbove(right, top)) top = right;
            if (top == i) break;
            std.mem.swap(u32, &self.order.items[i], &self.order.items[top]);
            i = top;
        }
    }

    fn next(ptr: *anyopaque, _: *ExecutionContext) OperatorError!?*Row {
        const self: *Self = @ptrCast(@alignCast(ptr));

        if (!self.opened) return OperatorError.NotInitialized;

        if (self.output_index >= self.order.items.len) {
            return null;
        }

        const row = &self.entries.items[self.order.items[self.output_index]].row;
        self.output_index += 1;
        return row;
    }

    fn freeBuffers(self: *Self) void {
        for (self.entries.items) |entry| self.allocator.free(entry.keys);
        self.entries.clearAndFree(self.allocator);
        self.order.clearAndFree(self.allocator);
    }

    fn close(ptr: *anyopaque, ctx: *ExecutionContext) void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        if (self.opened) {
            self.input.close(ctx);
            self.freeBuffers();
            self.opened = false;
        }
    }

    fn deinit(ptr: *anyopaque, allocator: Allocator) void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        self.freeBuffers();
        self.input.deinit(allocator);
        allocator.destroy(self);
    }
};

/// A row's column as a sort key. Nodes and relationships order by id, and an
/// empty slot by null.
fn keyFromSlot(row: *const Row, slot: u8, allocator: Allocator) Allocator.Error!EvalResult {
    if (slot >= row.slots.len) return .{ .null_val = {} };
    return switch (row.slots[slot]) {
        .empty => .{ .null_val = {} },
        .node_ref => |id| .{ .node_ref = id },
        .edge_ref => |id| .{ .edge_ref = id },
        .property => |pv| switch (pv) {
            // The row outlives the sort, so its strings can be borrowed.
            .string_val => |v| .{ .string_val = v },
            .bytes_val => |v| .{ .bytes_val = v },
            .vector_val => |v| .{ .vector_val = v },
            else => try EvalResult.fromPropertyValue(pv, allocator),
        },
        .path => |p| .{ .path_val = p },
    };
}

/// Where a value's type falls in the order of values of different types,
/// following Cypher's orderability: maps, nodes, relationships, lists,
/// paths, strings, booleans, numbers, then null. Vectors sort with lists and bytes
/// just before strings; Cypher has neither.
fn typeRank(value: EvalResult) u8 {
    return switch (value) {
        .map_val => 0,
        .node_ref => 1,
        .edge_ref => 2,
        .list_val => 3,
        .path_val => 4,
        .vector_val => 5,
        .bytes_val => 6,
        .string_val => 7,
        .bool_val => 8,
        .int_val, .float_val => 9,
        .null_val => 10,
    };
}

fn orderIds(a: []const u64, b: []const u64) std.math.Order {
    for (a[0..@min(a.len, b.len)], b[0..@min(a.len, b.len)]) |x, y| {
        if (x != y) return std.math.order(x, y);
    }
    return std.math.order(a.len, b.len);
}

/// A total order over all values, used by ORDER BY.
///
/// Values of different types order by `typeRank`, so a sort over a mix of
/// types is well defined rather than depending on which pairs the sort
/// happened to compare. Null is the greatest value: last when ascending,
/// first when descending. Among numbers, integers and floats compare by
/// value exactly, and NaN is greater than every other number.
pub fn orderValues(a: EvalResult, b: EvalResult) std.math.Order {
    const rank_a = typeRank(a);
    const rank_b = typeRank(b);
    if (rank_a != rank_b) return std.math.order(rank_a, rank_b);

    return switch (a) {
        .null_val => .eq,
        .bool_val => |av| std.math.order(@intFromBool(av), @intFromBool(b.bool_val)),
        .int_val => |av| switch (b) {
            .int_val => |bv| std.math.order(av, bv),
            .float_val => |bv| orderIntFloat(av, bv),
            else => unreachable,
        },
        .float_val => |av| switch (b) {
            .float_val => |bv| orderFloats(av, bv),
            .int_val => |bv| orderIntFloat(bv, av).invert(),
            else => unreachable,
        },
        .string_val => |av| std.mem.order(u8, av, b.string_val),
        .bytes_val => |av| std.mem.order(u8, av, b.bytes_val),
        .node_ref => |av| std.math.order(av, b.node_ref),
        .edge_ref => |av| std.math.order(av, b.edge_ref),
        .vector_val => |av| blk: {
            const bv = b.vector_val;
            for (av[0..@min(av.len, bv.len)], bv[0..@min(av.len, bv.len)]) |x, y| {
                const cmp = orderFloats(x, y);
                if (cmp != .eq) break :blk cmp;
            }
            break :blk std.math.order(av.len, bv.len);
        },
        .list_val => |av| blk: {
            const bv = b.list_val;
            for (av[0..@min(av.len, bv.len)], bv[0..@min(av.len, bv.len)]) |x, y| {
                const cmp = orderValues(x, y);
                if (cmp != .eq) break :blk cmp;
            }
            break :blk std.math.order(av.len, bv.len);
        },
        // Paths order by their nodes, then their relationships.
        .path_val => |av| blk: {
            const bv = b.path_val;
            const by_nodes = orderIds(av.nodes, bv.nodes);
            if (by_nodes != .eq) break :blk by_nodes;
            break :blk orderIds(av.edges, bv.edges);
        },
        .map_val => |av| blk: {
            // Maps have no natural order; size, then entry by entry, is
            // at least consistent.
            const bv = b.map_val;
            if (av.len != bv.len) break :blk std.math.order(av.len, bv.len);
            for (av, bv) |x, y| {
                const key_cmp = std.mem.order(u8, x.key, y.key);
                if (key_cmp != .eq) break :blk key_cmp;
                const value_cmp = orderValues(x.value, y.value);
                if (value_cmp != .eq) break :blk value_cmp;
            }
            break :blk .eq;
        },
    };
}

/// Order two floats with NaN above every other value and equal to itself.
fn orderFloats(a: anytype, b: @TypeOf(a)) std.math.Order {
    const a_nan = std.math.isNan(a);
    const b_nan = std.math.isNan(b);
    if (a_nan or b_nan) return std.math.order(@intFromBool(a_nan), @intFromBool(b_nan));
    return if (a < b) .lt else if (a > b) .gt else .eq;
}

/// Order an integer against a float by exact value. Converting the integer
/// to a float would round above 2^53 and call unequal values equal.
fn orderIntFloat(i: i64, f: f64) std.math.Order {
    // NaN sorts after every number.
    return expression.compareIntFloat(i, f) orelse .lt;
}

pub fn evaluateCountExpression(
    evaluator: *ExpressionEvaluator,
    expr: *const ast.Expression,
    ctx: *ExecutionContext,
) OperatorError!u64 {
    var dummy_row = Row.init();
    const result = evaluator.evaluate(expr, &dummy_row, ctx) catch |err| return mapEvalError(err);

    return switch (result) {
        .int_val => |i| {
            if (i < 0) return ctx.fail(OperatorError.TypeError, expr.getLocation(), "SKIP and LIMIT take a non-negative integer, but this is {d}", .{i});
            return @intCast(i);
        },
        else => ctx.fail(OperatorError.TypeError, expr.getLocation(), "SKIP and LIMIT take a non-negative integer, but this is a {s}", .{expression.typeName(result)}),
    };
}

fn mapEvalError(err: EvalError) OperatorError {
    return switch (err) {
        EvalError.OutOfMemory => OperatorError.OutOfMemory,
        EvalError.TypeError => OperatorError.TypeError,
        EvalError.UnboundVariable => OperatorError.UnboundVariable,
        EvalError.PropertyNotFound => OperatorError.PropertyNotFound,
        EvalError.StorageError => OperatorError.StorageError,
        else => OperatorError.EvaluationError,
    };
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

test "orderValues is a total order across types" {
    const Order = std.math.Order;
    const nan = std.math.nan(f64);
    // Ascending, each value strictly before the next.
    var one_list = [_]EvalResult{.{ .int_val = 1 }};
    var two_list = [_]EvalResult{.{ .int_val = 2 }};
    const ascending = [_]EvalResult{
        .{ .map_val = &.{} },
        .{ .node_ref = 1 },
        .{ .node_ref = 2 },
        .{ .edge_ref = 1 },
        .{ .list_val = &one_list },
        .{ .list_val = &two_list },
        .{ .string_val = "a" },
        .{ .string_val = "b" },
        .{ .bool_val = false },
        .{ .bool_val = true },
        .{ .float_val = -std.math.inf(f64) },
        .{ .int_val = std.math.minInt(i64) },
        .{ .int_val = -1 },
        .{ .float_val = -0.5 },
        .{ .int_val = 0 },
        .{ .float_val = 0.5 },
        .{ .int_val = 9007199254740993 },
        .{ .float_val = 9007199254740994.0 },
        .{ .int_val = std.math.maxInt(i64) },
        .{ .float_val = 9223372036854775808.0 },
        .{ .float_val = std.math.inf(f64) },
        .{ .float_val = nan },
        .{ .null_val = {} },
    };
    for (ascending, 0..) |a, i| {
        for (ascending, 0..) |b, j| {
            const want: Order = std.math.order(i, j);
            try std.testing.expectEqual(want, orderValues(a, b));
        }
    }
    try std.testing.expectEqual(Order.eq, orderValues(.{ .int_val = 1 }, .{ .float_val = 1.0 }));
    try std.testing.expectEqual(Order.eq, orderValues(.{ .float_val = nan }, .{ .float_val = nan }));
}
