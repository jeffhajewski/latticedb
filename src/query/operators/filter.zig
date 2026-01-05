//! Filter operator for query execution.
//!
//! Filters rows from an input operator based on a predicate expression.
//! Only passes through rows where the predicate evaluates to true.

const std = @import("std");
const Allocator = std.mem.Allocator;

const executor = @import("../executor.zig");
const Operator = executor.Operator;
const OperatorError = executor.OperatorError;
const Row = executor.Row;
const ExecutionContext = executor.ExecutionContext;

const expression = @import("../expression.zig");
const ExpressionEvaluator = expression.ExpressionEvaluator;
const EvalError = expression.EvalError;

const ast = @import("../ast.zig");

// ============================================================================
// Filter Operator
// ============================================================================

/// Filter operator that passes through only rows matching a predicate.
pub const Filter = struct {
    /// Input operator
    input: Operator,
    /// Predicate expression to evaluate
    predicate: *const ast.Expression,
    /// Expression evaluator
    evaluator: ExpressionEvaluator,
    /// Whether the input has been opened
    opened: bool,

    const Self = @This();

    /// Create a new Filter operator
    pub fn init(allocator: Allocator, input: Operator, predicate: *const ast.Expression) !*Self {
        const self = try allocator.create(Self);
        self.* = Self{
            .input = input,
            .predicate = predicate,
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

        // Open the input operator
        try self.input.open(ctx);
        self.opened = true;
    }

    fn next(ptr: *anyopaque, ctx: *ExecutionContext) OperatorError!?*Row {
        const self: *Self = @ptrCast(@alignCast(ptr));

        if (!self.opened) return OperatorError.NotInitialized;

        // Pull rows from input until we find one that matches the predicate
        while (true) {
            const row = try self.input.next(ctx) orelse return null;

            // Evaluate the predicate
            const result = self.evaluator.evaluate(self.predicate, row, ctx) catch |err| {
                return mapEvalError(err);
            };

            // Check if predicate is true
            if (result.isTruthy()) {
                return row;
            }
            // Otherwise, continue to next row
        }
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

        allocator.destroy(self);
    }
};

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

test "Filter basic structure" {
    // Verify vtable is properly structured
    const vtable = Filter.vtable;
    try std.testing.expect(@TypeOf(vtable.open) != void);
    try std.testing.expect(@TypeOf(vtable.next) != void);
    try std.testing.expect(@TypeOf(vtable.close) != void);
    try std.testing.expect(@TypeOf(vtable.deinit) != void);
}
