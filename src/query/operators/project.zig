//! Project operator for query execution.
//!
//! Projects specific expressions from input rows to produce output rows.
//! Used to implement the RETURN clause.

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

// ============================================================================
// Project Operator
// ============================================================================

/// A projection item (expression to output slot mapping)
pub const ProjectItem = struct {
    /// Expression to evaluate
    expr: *const ast.Expression,
    /// Output slot for the result
    output_slot: u8,
};

/// Project operator that evaluates expressions and outputs to specific slots.
pub const Project = struct {
    /// Input operator
    input: Operator,
    /// Projection items
    items: []const ProjectItem,
    /// Expression evaluator
    evaluator: ExpressionEvaluator,
    /// Current output row
    output_row: ?*Row,
    /// Whether the input has been opened
    opened: bool,

    const Self = @This();

    /// Create a new Project operator
    pub fn init(allocator: Allocator, input: Operator, items: []const ProjectItem) !*Self {
        const self = try allocator.create(Self);
        self.* = Self{
            .input = input,
            .items = items,
            .evaluator = ExpressionEvaluator.init(allocator),
            .output_row = null,
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

        // Allocate output row
        self.output_row = ctx.allocRow() catch return OperatorError.OutOfMemory;

        // Open the input operator
        try self.input.open(ctx);
        self.opened = true;
    }

    fn next(ptr: *anyopaque, ctx: *ExecutionContext) OperatorError!?*Row {
        const self: *Self = @ptrCast(@alignCast(ptr));

        if (!self.opened) return OperatorError.NotInitialized;

        // Get next input row
        const input_row = try self.input.next(ctx) orelse return null;
        const output_row = self.output_row orelse return OperatorError.NotInitialized;

        // Clear output row
        output_row.clear();

        // Evaluate each projection item
        for (self.items) |item| {
            const result = self.evaluator.evaluate(item.expr, input_row, ctx) catch |err| {
                return mapEvalError(err);
            };

            // Convert result to slot value
            const slot_value = resultToSlotValue(result);
            output_row.setSlot(item.output_slot, slot_value);

            // Copy distances and scores from input if this is a passthrough
            if (input_row.hasSlot(item.output_slot)) {
                output_row.setDistance(item.output_slot, input_row.getDistance(item.output_slot));
                output_row.setScore(item.output_slot, input_row.getScore(item.output_slot));
            }
        }

        return output_row;
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

        // Also deinit the input operator
        self.input.deinit(allocator);

        // Free the items array
        allocator.free(self.items);

        allocator.destroy(self);
    }
};

/// Convert an EvalResult to a SlotValue
fn resultToSlotValue(result: EvalResult) SlotValue {
    return switch (result) {
        .null_val => .{ .empty = {} },
        .node_ref => |id| .{ .node_ref = id },
        .edge_ref => |id| .{ .edge_ref = id },
        .bool_val => |b| .{ .property = .{ .bool_val = b } },
        .int_val => |i| .{ .property = .{ .int_val = i } },
        .float_val => |f| .{ .property = .{ .float_val = f } },
        .string_val => |s| .{ .property = .{ .string_val = s } },
        .list_val => .{ .empty = {} }, // TODO: proper list handling
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
// Tests
// ============================================================================

test "Project basic structure" {
    // Verify vtable is properly structured
    const vtable = Project.vtable;
    try std.testing.expect(@TypeOf(vtable.open) != void);
    try std.testing.expect(@TypeOf(vtable.next) != void);
    try std.testing.expect(@TypeOf(vtable.close) != void);
    try std.testing.expect(@TypeOf(vtable.deinit) != void);
}

test "resultToSlotValue conversions" {
    const node_slot = resultToSlotValue(.{ .node_ref = 123 });
    try std.testing.expectEqual(@as(u64, 123), node_slot.asNodeId().?);

    const bool_slot = resultToSlotValue(.{ .bool_val = true });
    try std.testing.expectEqual(true, bool_slot.asProperty().?.bool_val);

    const int_slot = resultToSlotValue(.{ .int_val = 42 });
    try std.testing.expectEqual(@as(i64, 42), int_slot.asProperty().?.int_val);
}
