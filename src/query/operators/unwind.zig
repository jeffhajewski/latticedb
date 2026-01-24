//! Unwind operator for expanding lists into individual rows.
//!
//! UNWIND [1, 2, 3] AS x produces one row per element with x bound to each.

const std = @import("std");
const Allocator = std.mem.Allocator;

const executor = @import("../executor.zig");
const Operator = executor.Operator;
const OperatorError = executor.OperatorError;
const Row = executor.Row;
const SlotValue = executor.SlotValue;
const ExecutionContext = executor.ExecutionContext;
const MAX_SLOTS = executor.MAX_SLOTS;

const expression_mod = @import("../expression.zig");
const ExpressionEvaluator = expression_mod.ExpressionEvaluator;
const EvalResult = expression_mod.EvalResult;

const ast = @import("../ast.zig");

// ============================================================================
// Unwind Operator
// ============================================================================

/// Unwind operator that expands a list expression into individual rows.
/// For each input row, evaluates the expression (which should produce a list),
/// then produces one output row per list element with the variable bound.
pub const Unwind = struct {
    /// Input operator
    input: Operator,
    /// Expression to evaluate (should yield a list)
    expr: *const ast.Expression,
    /// Slot for the unwind variable
    output_slot: u8,
    /// Allocator
    allocator: Allocator,
    /// Expression evaluator
    evaluator: ExpressionEvaluator,
    /// Current input row being expanded
    current_row: ?Row,
    /// Current list being iterated
    current_list: ?[]const EvalResult,
    /// Index into current list
    list_index: usize,
    /// Output row buffer
    output_row: Row,
    /// Whether opened
    opened: bool,

    const Self = @This();

    pub fn init(
        allocator: Allocator,
        input: Operator,
        expr: *const ast.Expression,
        output_slot: u8,
    ) !*Self {
        const self = try allocator.create(Self);
        self.* = Self{
            .input = input,
            .expr = expr,
            .output_slot = output_slot,
            .allocator = allocator,
            .evaluator = ExpressionEvaluator.init(allocator),
            .current_row = null,
            .current_list = null,
            .list_index = 0,
            .output_row = Row.init(),
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
        .deinit = deinit,
    };

    fn open(ptr: *anyopaque, ctx: *ExecutionContext) OperatorError!void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        try self.input.open(ctx);
        self.current_row = null;
        self.current_list = null;
        self.list_index = 0;
        self.opened = true;
    }

    fn next(ptr: *anyopaque, ctx: *ExecutionContext) OperatorError!?*Row {
        const self: *Self = @ptrCast(@alignCast(ptr));
        if (!self.opened) return OperatorError.NotInitialized;

        while (true) {
            // If we have elements remaining in the current list, yield them
            if (self.current_list) |list| {
                if (self.list_index < list.len) {
                    // Copy the current input row to output
                    self.output_row = self.current_row.?;

                    // Bind the list element to the output slot
                    self.output_row.slots[self.output_slot] = evalResultToSlot(list[self.list_index]);
                    self.list_index += 1;
                    return &self.output_row;
                }

                // Exhausted current list, get next input row
                self.current_list = null;
            }

            // Get next input row
            const input_row = try self.input.next(ctx) orelse return null;
            self.current_row = input_row.*;

            // Evaluate expression on this row
            const result = self.evaluator.evaluate(self.expr, input_row, ctx) catch {
                // Expression evaluation failed — skip this row
                continue;
            };

            // Get the list from the result
            switch (result) {
                .list_val => |list| {
                    self.current_list = list;
                    self.list_index = 0;
                },
                else => {
                    // Not a list — treat as single-element (Cypher behavior)
                    // Produce one row with the value itself
                    self.output_row = self.current_row.?;
                    self.output_row.slots[self.output_slot] = evalResultToSlot(result);
                    return &self.output_row;
                },
            }
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
        self.input.deinit(allocator);
        allocator.destroy(self);
    }
};

/// Convert an EvalResult to a SlotValue
fn evalResultToSlot(result: EvalResult) SlotValue {
    return switch (result) {
        .node_ref => |id| .{ .node_ref = id },
        .edge_ref => |id| .{ .edge_ref = id },
        .int_val => |v| .{ .property = .{ .int_val = v } },
        .float_val => |v| .{ .property = .{ .float_val = v } },
        .string_val => |v| .{ .property = .{ .string_val = v } },
        .bool_val => |v| .{ .property = .{ .bool_val = v } },
        .null_val => .empty,
        .list_val => .empty, // Nested lists not supported in slots
        .vector_val => .empty, // Vectors not supported in slots
        .map_val => .empty, // Maps not supported in slots
    };
}
