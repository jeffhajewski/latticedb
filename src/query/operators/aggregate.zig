//! Aggregate operator for query execution.
//!
//! Computes aggregate functions (count, sum, avg, collect, min, max) over input rows.
//! Supports grouping via GROUP BY semantics (implicit from non-aggregate expressions).

const std = @import("std");
const Allocator = std.mem.Allocator;

const executor = @import("../executor.zig");
const Operator = executor.Operator;
const OperatorError = executor.OperatorError;
const Row = executor.Row;
const SlotValue = executor.SlotValue;
const ExecutionContext = executor.ExecutionContext;

const expression = @import("../expression.zig");
const ExpressionEvaluator = expression.ExpressionEvaluator;
const EvalResult = expression.EvalResult;
const EvalError = expression.EvalError;

const ast = @import("../ast.zig");

const types = @import("lattice").core.types;
const PropertyValue = types.PropertyValue;

// ============================================================================
// Aggregate Function Types
// ============================================================================

/// Types of aggregate functions
pub const AggregateFunc = enum {
    count,
    count_star, // COUNT(*) - counts all rows
    sum,
    avg,
    min,
    max,
    collect,
};

/// An aggregate computation specification
pub const AggregateItem = struct {
    /// The aggregate function to apply
    func: AggregateFunc,
    /// Expression to aggregate (null for COUNT(*))
    expr: ?*const ast.Expression,
    /// Output slot for the result
    output_slot: u8,
    /// Whether DISTINCT is specified
    distinct: bool = false,
};

/// A grouping key specification
pub const GroupKey = struct {
    /// Expression for the grouping key
    expr: *const ast.Expression,
    /// Output slot for the group value
    output_slot: u8,
};

// ============================================================================
// Accumulator for tracking aggregate state
// ============================================================================

const Accumulator = struct {
    func: AggregateFunc,
    count: u64 = 0,
    sum: f64 = 0,
    min_val: ?EvalResult = null,
    max_val: ?EvalResult = null,
    collected: std.ArrayList(EvalResult),
    allocator: Allocator,
    distinct: bool = false,
    seen_values: std.StringHashMap(void),

    fn init(allocator: Allocator, func: AggregateFunc, distinct: bool) Accumulator {
        return .{
            .func = func,
            .collected = .empty,
            .allocator = allocator,
            .distinct = distinct,
            .seen_values = std.StringHashMap(void).init(allocator),
        };
    }

    fn deinit(self: *Accumulator) void {
        self.collected.deinit(self.allocator);
        // Free seen_values keys
        var it = self.seen_values.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
        }
        self.seen_values.deinit();
    }

    fn accumulate(self: *Accumulator, value: EvalResult) !void {
        // If DISTINCT, check for duplicate values
        if (self.distinct and !value.isNull()) {
            var fp_buf: [1024]u8 = undefined;
            const fp_len = hashValue(value, &fp_buf);
            const fp = fp_buf[0..fp_len];

            const key_copy = self.allocator.dupe(u8, fp) catch return error.OutOfMemory;
            const gop = self.seen_values.getOrPut(key_copy) catch {
                self.allocator.free(key_copy);
                return error.OutOfMemory;
            };

            if (gop.found_existing) {
                // Already seen this value - free the key copy and skip
                self.allocator.free(key_copy);
                return;
            }
        }

        switch (self.func) {
            .count_star => {
                self.count += 1;
            },
            .count => {
                if (!value.isNull()) {
                    self.count += 1;
                }
            },
            .sum => {
                switch (value) {
                    .int_val => |i| self.sum += @floatFromInt(i),
                    .float_val => |f| self.sum += f,
                    else => {},
                }
                self.count += 1;
            },
            .avg => {
                switch (value) {
                    .int_val => |i| self.sum += @floatFromInt(i),
                    .float_val => |f| self.sum += f,
                    else => {},
                }
                self.count += 1;
            },
            .min => {
                if (!value.isNull()) {
                    if (self.min_val) |current| {
                        if (compareValues(value, current) == .lt) {
                            self.min_val = value;
                        }
                    } else {
                        self.min_val = value;
                    }
                }
            },
            .max => {
                if (!value.isNull()) {
                    if (self.max_val) |current| {
                        if (compareValues(value, current) == .gt) {
                            self.max_val = value;
                        }
                    } else {
                        self.max_val = value;
                    }
                }
            },
            .collect => {
                try self.collected.append(self.allocator, value);
            },
        }
    }

    fn result(self: *const Accumulator) EvalResult {
        return switch (self.func) {
            .count_star, .count => .{ .int_val = @intCast(self.count) },
            .sum => .{ .float_val = self.sum },
            .avg => blk: {
                if (self.count == 0) break :blk .{ .null_val = {} };
                break :blk .{ .float_val = self.sum / @as(f64, @floatFromInt(self.count)) };
            },
            .min => self.min_val orelse .{ .null_val = {} },
            .max => self.max_val orelse .{ .null_val = {} },
            .collect => .{ .list_val = self.collected.items },
        };
    }
};

/// Compare two EvalResults for ordering
fn compareValues(a: EvalResult, b: EvalResult) std.math.Order {
    return switch (a) {
        .int_val => |ai| switch (b) {
            .int_val => |bi| std.math.order(ai, bi),
            .float_val => |bf| std.math.order(@as(f64, @floatFromInt(ai)), bf),
            else => .eq,
        },
        .float_val => |af| switch (b) {
            .float_val => |bf| std.math.order(af, bf),
            .int_val => |bi| std.math.order(af, @as(f64, @floatFromInt(bi))),
            else => .eq,
        },
        .string_val => |as| switch (b) {
            .string_val => |bs| std.mem.order(u8, as, bs),
            else => .eq,
        },
        else => .eq,
    };
}

// ============================================================================
// Group state
// ============================================================================

const GroupState = struct {
    /// Group key values (for output)
    key_values: []EvalResult,
    /// Accumulators for each aggregate
    accumulators: []Accumulator,
    allocator: Allocator,

    fn init(allocator: Allocator, num_keys: usize, agg_items: []const AggregateItem) !GroupState {
        const key_values = try allocator.alloc(EvalResult, num_keys);
        @memset(key_values, .{ .null_val = {} });

        const accumulators = try allocator.alloc(Accumulator, agg_items.len);
        for (accumulators, agg_items) |*acc, item| {
            acc.* = Accumulator.init(allocator, item.func, item.distinct);
        }

        return .{
            .key_values = key_values,
            .accumulators = accumulators,
            .allocator = allocator,
        };
    }

    fn deinit(self: *GroupState) void {
        self.allocator.free(self.key_values);
        for (self.accumulators) |*acc| {
            acc.deinit();
        }
        self.allocator.free(self.accumulators);
    }
};

// ============================================================================
// Aggregate Operator
// ============================================================================

/// Aggregate operator that computes aggregate functions over groups.
pub const Aggregate = struct {
    /// Input operator
    input: Operator,
    /// Grouping keys (empty for global aggregation)
    group_keys: []const GroupKey,
    /// Aggregate items to compute
    agg_items: []const AggregateItem,
    /// Expression evaluator
    evaluator: ExpressionEvaluator,
    /// Allocator
    allocator: Allocator,
    /// Group states (key hash -> state)
    groups: std.StringHashMap(GroupState),
    /// Iterator over groups for output
    group_iterator: ?std.StringHashMap(GroupState).Iterator,
    /// Whether the input has been consumed
    consumed: bool,
    /// Global group for queries without GROUP BY
    global_group: ?GroupState,

    const Self = @This();

    /// Create a new Aggregate operator
    pub fn init(
        allocator: Allocator,
        input: Operator,
        group_keys: []const GroupKey,
        agg_items: []const AggregateItem,
    ) !*Self {
        const self = try allocator.create(Self);
        self.* = Self{
            .input = input,
            .group_keys = group_keys,
            .agg_items = agg_items,
            .evaluator = ExpressionEvaluator.init(allocator),
            .allocator = allocator,
            .groups = std.StringHashMap(GroupState).init(allocator),
            .group_iterator = null,
            .consumed = false,
            .global_group = null,
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

        // Open input
        try self.input.open(ctx);

        // Consume all input rows and accumulate
        while (true) {
            const row = try self.input.next(ctx) orelse break;
            try self.accumulateRow(row, ctx);
        }

        self.consumed = true;

        // If no groups created (empty input), create empty global group for COUNT(*)
        if (self.group_keys.len == 0 and self.global_group == null) {
            self.global_group = GroupState.init(self.allocator, 0, self.agg_items) catch {
                return OperatorError.OutOfMemory;
            };
        }
    }

    fn accumulateRow(self: *Self, row: *const Row, ctx: *const ExecutionContext) OperatorError!void {
        if (self.group_keys.len == 0) {
            // Global aggregation (no GROUP BY)
            if (self.global_group == null) {
                self.global_group = GroupState.init(self.allocator, 0, self.agg_items) catch {
                    return OperatorError.OutOfMemory;
                };
            }

            // Accumulate values for each aggregate
            for (self.agg_items, 0..) |item, i| {
                const value = if (item.expr) |e|
                    self.evaluator.evaluate(e, row, ctx) catch |err| return mapEvalError(err)
                else
                    EvalResult{ .null_val = {} }; // COUNT(*)

                self.global_group.?.accumulators[i].accumulate(value) catch {
                    return OperatorError.OutOfMemory;
                };
            }
        } else {
            // Grouped aggregation - compute group key
            var key_buf: [1024]u8 = undefined;
            var key_len: usize = 0;
            var key_values = self.allocator.alloc(EvalResult, self.group_keys.len) catch {
                return OperatorError.OutOfMemory;
            };
            defer self.allocator.free(key_values);

            for (self.group_keys, 0..) |gk, i| {
                const val = self.evaluator.evaluate(gk.expr, row, ctx) catch |err| {
                    return mapEvalError(err);
                };
                key_values[i] = val;
                key_len += hashValue(val, key_buf[key_len..]);
            }

            const key = key_buf[0..key_len];

            // Get or create group
            const gop = self.groups.getOrPut(self.allocator.dupe(u8, key) catch {
                return OperatorError.OutOfMemory;
            }) catch {
                return OperatorError.OutOfMemory;
            };

            if (!gop.found_existing) {
                gop.value_ptr.* = GroupState.init(self.allocator, self.group_keys.len, self.agg_items) catch {
                    return OperatorError.OutOfMemory;
                };
                // Copy key values
                for (key_values, 0..) |kv, i| {
                    gop.value_ptr.key_values[i] = kv;
                }
            }

            // Accumulate values
            for (self.agg_items, 0..) |item, i| {
                const value = if (item.expr) |e|
                    self.evaluator.evaluate(e, row, ctx) catch |err| return mapEvalError(err)
                else
                    EvalResult{ .null_val = {} };

                gop.value_ptr.accumulators[i].accumulate(value) catch {
                    return OperatorError.OutOfMemory;
                };
            }
        }
    }

    fn next(ptr: *anyopaque, ctx: *ExecutionContext) OperatorError!?*Row {
        const self: *Self = @ptrCast(@alignCast(ptr));

        if (!self.consumed) return OperatorError.NotInitialized;

        if (self.group_keys.len == 0) {
            // Global aggregation - return single row
            if (self.global_group) |*group| {
                const output_row = ctx.allocRow() catch return OperatorError.OutOfMemory;
                output_row.clear();

                // Output aggregate results
                for (self.agg_items, 0..) |item, i| {
                    const result = group.accumulators[i].result();
                    output_row.setSlot(item.output_slot, resultToSlotValue(result, self.allocator));
                }

                // Clear global group so we don't return it again
                group.deinit();
                self.global_group = null;

                return output_row;
            }
            return null;
        } else {
            // Grouped aggregation - iterate groups
            if (self.group_iterator == null) {
                self.group_iterator = self.groups.iterator();
            }

            if (self.group_iterator.?.next()) |entry| {
                const output_row = ctx.allocRow() catch return OperatorError.OutOfMemory;
                output_row.clear();

                // Output group key values
                for (self.group_keys, 0..) |gk, i| {
                    output_row.setSlot(gk.output_slot, resultToSlotValue(entry.value_ptr.key_values[i], self.allocator));
                }

                // Output aggregate results
                for (self.agg_items, 0..) |item, i| {
                    const result = entry.value_ptr.accumulators[i].result();
                    output_row.setSlot(item.output_slot, resultToSlotValue(result, self.allocator));
                }

                return output_row;
            }

            return null;
        }
    }

    fn close(ptr: *anyopaque, ctx: *ExecutionContext) void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        self.input.close(ctx);
    }

    fn deinitOp(ptr: *anyopaque, allocator: Allocator) void {
        const self: *Self = @ptrCast(@alignCast(ptr));

        // Clean up groups
        var it = self.groups.iterator();
        while (it.next()) |entry| {
            allocator.free(entry.key_ptr.*);
            entry.value_ptr.deinit();
        }
        self.groups.deinit();

        if (self.global_group) |*g| {
            g.deinit();
        }

        // Clean up input
        self.input.deinit(allocator);

        // Free arrays
        allocator.free(self.group_keys);
        allocator.free(self.agg_items);

        allocator.destroy(self);
    }
};

/// Hash a value for group key computation
fn hashValue(value: EvalResult, buf: []u8) usize {
    var len: usize = 0;
    switch (value) {
        .null_val => {
            buf[0] = 0;
            len = 1;
        },
        .bool_val => |b| {
            buf[0] = if (b) 1 else 2;
            len = 1;
        },
        .int_val => |i| {
            buf[0] = 3;
            @memcpy(buf[1..9], std.mem.asBytes(&i));
            len = 9;
        },
        .float_val => |f| {
            buf[0] = 4;
            @memcpy(buf[1..9], std.mem.asBytes(&f));
            len = 9;
        },
        .string_val => |s| {
            buf[0] = 5;
            const copy_len = @min(s.len, buf.len - 1);
            @memcpy(buf[1 .. 1 + copy_len], s[0..copy_len]);
            len = 1 + copy_len;
        },
        .node_ref => |id| {
            buf[0] = 6;
            @memcpy(buf[1..9], std.mem.asBytes(&id));
            len = 9;
        },
        .edge_ref => |id| {
            buf[0] = 7;
            @memcpy(buf[1..9], std.mem.asBytes(&id));
            len = 9;
        },
        .list_val => {
            buf[0] = 8;
            len = 1;
        },
        .vector_val => {
            buf[0] = 9;
            len = 1;
        },
        .map_val => {
            buf[0] = 10;
            len = 1;
        },
    }
    return len;
}

/// Convert an EvalResult to a SlotValue
fn resultToSlotValue(result: EvalResult, allocator: Allocator) SlotValue {
    return switch (result) {
        .null_val => .{ .empty = {} },
        .node_ref => |id| .{ .node_ref = id },
        .edge_ref => |id| .{ .edge_ref = id },
        .bool_val => |b| .{ .property = .{ .bool_val = b } },
        .int_val => |i| .{ .property = .{ .int_val = i } },
        .float_val => |f| .{ .property = .{ .float_val = f } },
        .string_val => |s| .{ .property = .{ .string_val = s } },
        .vector_val => |v| .{ .property = .{ .vector_val = v } },
        .list_val => .{ .property = result.toPropertyValue(allocator) },
        .map_val => .{ .property = result.toPropertyValue(allocator) },
    };
}

/// Map EvalError to OperatorError
fn mapEvalError(err: EvalError) OperatorError {
    return switch (err) {
        EvalError.OutOfMemory => OperatorError.OutOfMemory,
        EvalError.TypeError => OperatorError.TypeError,
        EvalError.UnboundVariable => OperatorError.UnboundVariable,
        EvalError.PropertyNotFound => OperatorError.PropertyNotFound,
        else => OperatorError.EvaluationError,
    };
}

// ============================================================================
// Helper to detect if an expression contains aggregate functions
// ============================================================================

/// Check if an expression contains any aggregate function calls
pub fn containsAggregate(expr: *const ast.Expression) bool {
    return switch (expr.*) {
        .function_call => |f| isAggregateFunction(f.name) or blk: {
            for (f.arguments) |arg| {
                if (containsAggregate(arg)) break :blk true;
            }
            break :blk false;
        },
        .binary => |b| containsAggregate(b.left) or containsAggregate(b.right),
        .unary => |u| containsAggregate(u.operand),
        .property_access => |p| containsAggregate(p.object),
        else => false,
    };
}

/// Check if a function name is an aggregate function
pub fn isAggregateFunction(name: []const u8) bool {
    const agg_names = [_][]const u8{ "count", "sum", "avg", "min", "max", "collect" };
    for (agg_names) |agg| {
        if (std.ascii.eqlIgnoreCase(name, agg)) return true;
    }
    return false;
}

/// Parse aggregate function name to enum
pub fn parseAggregateFunc(name: []const u8) ?AggregateFunc {
    if (std.ascii.eqlIgnoreCase(name, "count")) return .count;
    if (std.ascii.eqlIgnoreCase(name, "sum")) return .sum;
    if (std.ascii.eqlIgnoreCase(name, "avg")) return .avg;
    if (std.ascii.eqlIgnoreCase(name, "min")) return .min;
    if (std.ascii.eqlIgnoreCase(name, "max")) return .max;
    if (std.ascii.eqlIgnoreCase(name, "collect")) return .collect;
    return null;
}

// ============================================================================
// Tests
// ============================================================================

test "Aggregate basic structure" {
    const vtable = Aggregate.vtable;
    try std.testing.expect(@TypeOf(vtable.open) != void);
    try std.testing.expect(@TypeOf(vtable.next) != void);
    try std.testing.expect(@TypeOf(vtable.close) != void);
    try std.testing.expect(@TypeOf(vtable.deinit) != void);
}

test "isAggregateFunction" {
    try std.testing.expect(isAggregateFunction("count"));
    try std.testing.expect(isAggregateFunction("COUNT"));
    try std.testing.expect(isAggregateFunction("Sum"));
    try std.testing.expect(isAggregateFunction("avg"));
    try std.testing.expect(isAggregateFunction("min"));
    try std.testing.expect(isAggregateFunction("max"));
    try std.testing.expect(isAggregateFunction("collect"));
    try std.testing.expect(!isAggregateFunction("id"));
    try std.testing.expect(!isAggregateFunction("coalesce"));
}

test "parseAggregateFunc" {
    try std.testing.expectEqual(AggregateFunc.count, parseAggregateFunc("count").?);
    try std.testing.expectEqual(AggregateFunc.sum, parseAggregateFunc("SUM").?);
    try std.testing.expectEqual(AggregateFunc.avg, parseAggregateFunc("Avg").?);
    try std.testing.expect(parseAggregateFunc("unknown") == null);
}

test "Accumulator count" {
    const allocator = std.testing.allocator;
    var acc = Accumulator.init(allocator, .count, false);
    defer acc.deinit();

    try acc.accumulate(.{ .int_val = 1 });
    try acc.accumulate(.{ .int_val = 2 });
    try acc.accumulate(.{ .null_val = {} });
    try acc.accumulate(.{ .int_val = 3 });

    const result = acc.result();
    try std.testing.expectEqual(@as(i64, 3), result.int_val);
}

test "Accumulator sum" {
    const allocator = std.testing.allocator;
    var acc = Accumulator.init(allocator, .sum, false);
    defer acc.deinit();

    try acc.accumulate(.{ .int_val = 10 });
    try acc.accumulate(.{ .int_val = 20 });
    try acc.accumulate(.{ .float_val = 5.5 });

    const result = acc.result();
    try std.testing.expectApproxEqAbs(@as(f64, 35.5), result.float_val, 0.001);
}

test "Accumulator avg" {
    const allocator = std.testing.allocator;
    var acc = Accumulator.init(allocator, .avg, false);
    defer acc.deinit();

    try acc.accumulate(.{ .int_val = 10 });
    try acc.accumulate(.{ .int_val = 20 });
    try acc.accumulate(.{ .int_val = 30 });

    const result = acc.result();
    try std.testing.expectApproxEqAbs(@as(f64, 20.0), result.float_val, 0.001);
}

test "Accumulator min/max" {
    const allocator = std.testing.allocator;

    var min_acc = Accumulator.init(allocator, .min, false);
    defer min_acc.deinit();

    var max_acc = Accumulator.init(allocator, .max, false);
    defer max_acc.deinit();

    try min_acc.accumulate(.{ .int_val = 30 });
    try min_acc.accumulate(.{ .int_val = 10 });
    try min_acc.accumulate(.{ .int_val = 20 });

    try max_acc.accumulate(.{ .int_val = 30 });
    try max_acc.accumulate(.{ .int_val = 10 });
    try max_acc.accumulate(.{ .int_val = 20 });

    try std.testing.expectEqual(@as(i64, 10), min_acc.result().int_val);
    try std.testing.expectEqual(@as(i64, 30), max_acc.result().int_val);
}

test "Accumulator collect" {
    const allocator = std.testing.allocator;
    var acc = Accumulator.init(allocator, .collect, false);
    defer acc.deinit();

    try acc.accumulate(.{ .int_val = 1 });
    try acc.accumulate(.{ .int_val = 2 });
    try acc.accumulate(.{ .int_val = 3 });

    const result = acc.result();
    try std.testing.expectEqual(@as(usize, 3), result.list_val.len);
}

test "Accumulator collect preserves values" {
    const allocator = std.testing.allocator;
    var acc = Accumulator.init(allocator, .collect, false);
    defer acc.deinit();

    try acc.accumulate(.{ .int_val = 10 });
    try acc.accumulate(.{ .string_val = "hello" });
    try acc.accumulate(.{ .bool_val = true });
    try acc.accumulate(.{ .float_val = 2.5 });

    const result = acc.result();
    try std.testing.expectEqual(@as(usize, 4), result.list_val.len);
    try std.testing.expectEqual(@as(i64, 10), result.list_val[0].int_val);
    try std.testing.expectEqualStrings("hello", result.list_val[1].string_val);
    try std.testing.expectEqual(true, result.list_val[2].bool_val);
    try std.testing.expectApproxEqAbs(@as(f64, 2.5), result.list_val[3].float_val, 0.001);
}

test "resultToSlotValue converts list" {
    const allocator = std.testing.allocator;

    const items = [_]EvalResult{
        .{ .int_val = 100 },
        .{ .string_val = "test" },
    };
    const list_result = EvalResult{ .list_val = &items };

    const slot = resultToSlotValue(list_result, allocator);
    defer allocator.free(slot.asProperty().?.list_val);

    const prop = slot.asProperty().?;
    try std.testing.expectEqual(@as(usize, 2), prop.list_val.len);
    try std.testing.expectEqual(@as(i64, 100), prop.list_val[0].int_val);
    try std.testing.expectEqualStrings("test", prop.list_val[1].string_val);
}

test "resultToSlotValue converts vector" {
    const vec = [_]f32{ 0.1, 0.2, 0.3, 0.4 };
    const vec_result = EvalResult{ .vector_val = &vec };

    const slot = resultToSlotValue(vec_result, std.testing.allocator);

    const prop = slot.asProperty().?;
    try std.testing.expectEqual(@as(usize, 4), prop.vector_val.len);
    try std.testing.expectApproxEqAbs(@as(f32, 0.1), prop.vector_val[0], 0.001);
    try std.testing.expectApproxEqAbs(@as(f32, 0.4), prop.vector_val[3], 0.001);
}

test "resultToSlotValue converts map" {
    const allocator = std.testing.allocator;

    const entries = [_]EvalResult.MapEntry{
        .{ .key = "key1", .value = .{ .int_val = 1 } },
    };
    const map_result = EvalResult{ .map_val = &entries };

    const slot = resultToSlotValue(map_result, allocator);
    defer allocator.free(slot.asProperty().?.map_val);

    const prop = slot.asProperty().?;
    try std.testing.expectEqual(@as(usize, 1), prop.map_val.len);
    try std.testing.expectEqualStrings("key1", prop.map_val[0].key);
    try std.testing.expectEqual(@as(i64, 1), prop.map_val[0].value.int_val);
}
