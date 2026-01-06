//! Expression evaluator for query execution.
//!
//! Evaluates AST expressions against rows during query execution.
//! Supports literals, variables, property access, binary/unary operations,
//! and function calls.

const std = @import("std");
const Allocator = std.mem.Allocator;

const ast = @import("ast.zig");
const executor = @import("executor.zig");

const types = @import("../core/types.zig");
const NodeId = types.NodeId;
const EdgeId = types.EdgeId;
const PropertyValue = types.PropertyValue;

const Row = executor.Row;
const SlotValue = executor.SlotValue;
const ExecutionContext = executor.ExecutionContext;

// ============================================================================
// Errors
// ============================================================================

/// Errors that can occur during expression evaluation
pub const EvalError = error{
    /// Variable not bound in context
    UnboundVariable,
    /// Parameter not found
    ParameterNotFound,
    /// Property not found on node/edge
    PropertyNotFound,
    /// Type mismatch in operation
    TypeError,
    /// Null value in non-nullable context
    NullValue,
    /// Division by zero
    DivisionByZero,
    /// Unknown function
    UnknownFunction,
    /// Invalid number of arguments
    InvalidArgumentCount,
    /// Slot is empty
    EmptySlot,
    /// Out of memory
    OutOfMemory,
    /// Invalid operation
    InvalidOperation,
};

// ============================================================================
// Evaluation Result
// ============================================================================

/// Result of expression evaluation
pub const EvalResult = union(enum) {
    /// Null value
    null_val: void,
    /// Boolean result
    bool_val: bool,
    /// Integer result
    int_val: i64,
    /// Float result
    float_val: f64,
    /// String result (borrowed, valid for query lifetime)
    string_val: []const u8,
    /// Node reference
    node_ref: NodeId,
    /// Edge reference
    edge_ref: EdgeId,
    /// List result
    list_val: []const EvalResult,

    const Self = @This();

    /// Check if result is null
    pub fn isNull(self: Self) bool {
        return self == .null_val;
    }

    /// Check if result is truthy
    pub fn isTruthy(self: Self) bool {
        return switch (self) {
            .null_val => false,
            .bool_val => |b| b,
            .int_val => |i| i != 0,
            .float_val => |f| f != 0.0,
            .string_val => |s| s.len > 0,
            .node_ref => true,
            .edge_ref => true,
            .list_val => |l| l.len > 0,
        };
    }

    /// Convert to boolean
    pub fn toBool(self: Self) ?bool {
        return switch (self) {
            .bool_val => |b| b,
            else => null,
        };
    }

    /// Convert to integer
    pub fn toInt(self: Self) ?i64 {
        return switch (self) {
            .int_val => |i| i,
            .float_val => |f| @intFromFloat(f),
            else => null,
        };
    }

    /// Convert to float
    pub fn toFloat(self: Self) ?f64 {
        return switch (self) {
            .float_val => |f| f,
            .int_val => |i| @floatFromInt(i),
            else => null,
        };
    }

    /// Convert from PropertyValue
    pub fn fromPropertyValue(pv: PropertyValue) Self {
        return switch (pv) {
            .null_val => .{ .null_val = {} },
            .bool_val => |b| .{ .bool_val = b },
            .int_val => |i| .{ .int_val = i },
            .float_val => |f| .{ .float_val = f },
            .string_val => |s| .{ .string_val = s },
            .bytes_val => |b| .{ .string_val = b },
            .list_val, .map_val => .{ .null_val = {} }, // TODO: proper list/map conversion
        };
    }

    /// Convert to PropertyValue
    pub fn toPropertyValue(self: Self) PropertyValue {
        return switch (self) {
            .null_val => .{ .null_val = {} },
            .bool_val => |b| .{ .bool_val = b },
            .int_val => |i| .{ .int_val = i },
            .float_val => |f| .{ .float_val = f },
            .string_val => |s| .{ .string_val = s },
            .node_ref => |id| .{ .int_val = @intCast(id) },
            .edge_ref => |id| .{ .int_val = @intCast(id) },
            .list_val => .{ .null_val = {} }, // TODO: proper list conversion
        };
    }
};

// ============================================================================
// Expression Evaluator
// ============================================================================

/// Evaluator for AST expressions
pub const ExpressionEvaluator = struct {
    /// Allocator for evaluation temporaries
    allocator: Allocator,

    const Self = @This();

    /// Create a new expression evaluator
    pub fn init(allocator: Allocator) Self {
        return Self{
            .allocator = allocator,
        };
    }

    /// Evaluate an expression against a row
    pub fn evaluate(
        self: *Self,
        expr: *const ast.Expression,
        row: *const Row,
        ctx: *const ExecutionContext,
    ) EvalError!EvalResult {
        return switch (expr.*) {
            .literal => |lit| self.evaluateLiteral(lit),
            .variable => |v| self.evaluateVariable(v, row, ctx),
            .parameter => |p| self.evaluateParameter(p, ctx),
            .property_access => |pa| self.evaluatePropertyAccess(pa, row, ctx),
            .binary => |b| self.evaluateBinary(b, row, ctx),
            .unary => |u| self.evaluateUnary(u, row, ctx),
            .function_call => |f| self.evaluateFunction(f, row, ctx),
            .list => |l| self.evaluateList(l, row, ctx),
            .map => self.evaluateMap(),
        };
    }

    /// Evaluate a literal expression
    fn evaluateLiteral(_: *Self, lit: ast.Literal) EvalResult {
        return switch (lit.value) {
            .integer => |i| .{ .int_val = i },
            .float => |f| .{ .float_val = f },
            .string => |s| .{ .string_val = s },
            .boolean => |b| .{ .bool_val = b },
            .null_value => .{ .null_val = {} },
        };
    }

    /// Evaluate a variable reference
    fn evaluateVariable(
        _: *Self,
        v: ast.VariableRef,
        row: *const Row,
        ctx: *const ExecutionContext,
    ) EvalError!EvalResult {
        // Look up the slot for this variable
        const slot = ctx.getVariableSlot(v.name) orelse return EvalError.UnboundVariable;

        // Get the value from the row
        const slot_val = row.getSlot(slot) orelse return EvalError.EmptySlot;

        return switch (slot_val) {
            .empty => EvalError.EmptySlot,
            .node_ref => |id| .{ .node_ref = id },
            .edge_ref => |id| .{ .edge_ref = id },
            .property => |pv| EvalResult.fromPropertyValue(pv),
        };
    }

    /// Evaluate a parameter reference
    fn evaluateParameter(
        _: *Self,
        p: ast.ParameterRef,
        ctx: *const ExecutionContext,
    ) EvalError!EvalResult {
        const pv = ctx.getParameter(p.name) orelse return EvalError.ParameterNotFound;
        return EvalResult.fromPropertyValue(pv);
    }

    /// Evaluate a property access expression
    fn evaluatePropertyAccess(
        self: *Self,
        pa: *const ast.PropertyAccess,
        row: *const Row,
        ctx: *const ExecutionContext,
    ) EvalError!EvalResult {
        // Evaluate the object expression
        const obj = try self.evaluate(pa.object, row, ctx);

        // Handle node property access
        if (obj == .node_ref) {
            const node_id = obj.node_ref;

            // Get the node store and symbol table from context
            const node_store = ctx.node_store orelse return .{ .null_val = {} };
            const symbol_table = ctx.symbol_table orelse return .{ .null_val = {} };

            // Look up the property key symbol ID
            const key_id = symbol_table.lookup(pa.property) catch {
                // Property key not found in symbol table means no node has this property
                return .{ .null_val = {} };
            };

            // Get the node from storage
            var node = node_store.get(node_id) catch {
                return .{ .null_val = {} };
            };
            defer node.deinit(self.allocator);

            // Find the property with matching key_id
            for (node.properties) |prop| {
                if (prop.key_id == key_id) {
                    // Clone string/bytes values since node will be freed
                    return switch (prop.value) {
                        .string_val => |s| blk: {
                            const cloned = self.allocator.dupe(u8, s) catch return EvalError.OutOfMemory;
                            break :blk .{ .string_val = cloned };
                        },
                        .bytes_val => |b| blk: {
                            const cloned = self.allocator.dupe(u8, b) catch return EvalError.OutOfMemory;
                            break :blk .{ .string_val = cloned };
                        },
                        else => EvalResult.fromPropertyValue(prop.value),
                    };
                }
            }

            // Property not found on this node
            return .{ .null_val = {} };
        }

        // TODO: Handle edge property access
        return .{ .null_val = {} };
    }

    /// Evaluate a binary expression
    fn evaluateBinary(
        self: *Self,
        b: *const ast.BinaryExpr,
        row: *const Row,
        ctx: *const ExecutionContext,
    ) EvalError!EvalResult {
        // Short-circuit evaluation for logical operators
        if (b.operator == .and_) {
            const left = try self.evaluate(b.left, row, ctx);
            if (!left.isTruthy()) return .{ .bool_val = false };
            const right = try self.evaluate(b.right, row, ctx);
            return .{ .bool_val = right.isTruthy() };
        }

        if (b.operator == .or_) {
            const left = try self.evaluate(b.left, row, ctx);
            if (left.isTruthy()) return .{ .bool_val = true };
            const right = try self.evaluate(b.right, row, ctx);
            return .{ .bool_val = right.isTruthy() };
        }

        // Evaluate both sides
        const left = try self.evaluate(b.left, row, ctx);
        const right = try self.evaluate(b.right, row, ctx);

        // Handle null comparisons
        if (left.isNull() or right.isNull()) {
            return switch (b.operator) {
                .eq => .{ .bool_val = left.isNull() and right.isNull() },
                .neq => .{ .bool_val = !(left.isNull() and right.isNull()) },
                else => .{ .null_val = {} },
            };
        }

        return switch (b.operator) {
            // Comparison operators
            .eq => self.compareEqual(left, right),
            .neq => blk: {
                const eq = self.compareEqual(left, right);
                break :blk .{ .bool_val = !eq.toBool().? };
            },
            .lt => self.compareLessThan(left, right),
            .lte => self.compareLessOrEqual(left, right),
            .gt => blk: {
                const lte = self.compareLessOrEqual(left, right);
                break :blk .{ .bool_val = !lte.toBool().? };
            },
            .gte => blk: {
                const lt = self.compareLessThan(left, right);
                break :blk .{ .bool_val = !lt.toBool().? };
            },

            // Logical operators (already handled above for short-circuit)
            .and_, .or_ => unreachable,
            .xor => .{ .bool_val = left.isTruthy() != right.isTruthy() },

            // Arithmetic operators
            .add => self.arithmeticAdd(left, right),
            .sub => self.arithmeticSub(left, right),
            .mul => self.arithmeticMul(left, right),
            .div => self.arithmeticDiv(left, right),
            .mod => self.arithmeticMod(left, right),
            .pow => self.arithmeticPow(left, right),

            // String operators
            .contains => self.stringContains(left, right),
            .starts_with => self.stringStartsWith(left, right),
            .ends_with => self.stringEndsWith(left, right),
            .regex_match => .{ .null_val = {} }, // TODO: regex support

            // List operator
            .in_ => self.listContains(left, right),

            // Special operators (handled by dedicated operators)
            .vector_distance, .fts_match => .{ .null_val = {} },
        };
    }

    /// Evaluate a unary expression
    fn evaluateUnary(
        self: *Self,
        u: *const ast.UnaryExpr,
        row: *const Row,
        ctx: *const ExecutionContext,
    ) EvalError!EvalResult {
        const operand = try self.evaluate(u.operand, row, ctx);

        return switch (u.operator) {
            .not => .{ .bool_val = !operand.isTruthy() },
            .neg => switch (operand) {
                .int_val => |i| .{ .int_val = -i },
                .float_val => |f| .{ .float_val = -f },
                else => EvalError.TypeError,
            },
            .is_null => .{ .bool_val = operand.isNull() },
            .is_not_null => .{ .bool_val = !operand.isNull() },
        };
    }

    /// Evaluate a function call
    fn evaluateFunction(
        self: *Self,
        f: *const ast.FunctionCall,
        row: *const Row,
        ctx: *const ExecutionContext,
    ) EvalError!EvalResult {
        // Built-in functions
        if (std.mem.eql(u8, f.name, "id")) {
            if (f.arguments.len != 1) return EvalError.InvalidArgumentCount;
            const arg = try self.evaluate(f.arguments[0], row, ctx);
            return switch (arg) {
                .node_ref => |id| .{ .int_val = @intCast(id) },
                .edge_ref => |id| .{ .int_val = @intCast(id) },
                else => EvalError.TypeError,
            };
        }

        if (std.mem.eql(u8, f.name, "coalesce")) {
            for (f.arguments) |arg_expr| {
                const arg = try self.evaluate(arg_expr, row, ctx);
                if (!arg.isNull()) return arg;
            }
            return .{ .null_val = {} };
        }

        if (std.mem.eql(u8, f.name, "abs")) {
            if (f.arguments.len != 1) return EvalError.InvalidArgumentCount;
            const arg = try self.evaluate(f.arguments[0], row, ctx);
            return switch (arg) {
                .int_val => |i| .{ .int_val = if (i < 0) -i else i },
                .float_val => |fv| .{ .float_val = @abs(fv) },
                else => EvalError.TypeError,
            };
        }

        if (std.mem.eql(u8, f.name, "size")) {
            if (f.arguments.len != 1) return EvalError.InvalidArgumentCount;
            const arg = try self.evaluate(f.arguments[0], row, ctx);
            return switch (arg) {
                .string_val => |s| .{ .int_val = @intCast(s.len) },
                .list_val => |l| .{ .int_val = @intCast(l.len) },
                else => EvalError.TypeError,
            };
        }

        if (std.mem.eql(u8, f.name, "toInteger")) {
            if (f.arguments.len != 1) return EvalError.InvalidArgumentCount;
            const arg = try self.evaluate(f.arguments[0], row, ctx);
            return switch (arg) {
                .int_val => arg,
                .float_val => |fv| .{ .int_val = @intFromFloat(fv) },
                .string_val => |s| blk: {
                    const i = std.fmt.parseInt(i64, s, 10) catch return .{ .null_val = {} };
                    break :blk .{ .int_val = i };
                },
                else => .{ .null_val = {} },
            };
        }

        if (std.mem.eql(u8, f.name, "toFloat")) {
            if (f.arguments.len != 1) return EvalError.InvalidArgumentCount;
            const arg = try self.evaluate(f.arguments[0], row, ctx);
            return switch (arg) {
                .float_val => arg,
                .int_val => |i| .{ .float_val = @floatFromInt(i) },
                .string_val => |s| blk: {
                    const fv = std.fmt.parseFloat(f64, s) catch return .{ .null_val = {} };
                    break :blk .{ .float_val = fv };
                },
                else => .{ .null_val = {} },
            };
        }

        if (std.mem.eql(u8, f.name, "toString")) {
            if (f.arguments.len != 1) return EvalError.InvalidArgumentCount;
            // String conversion requires allocation - defer to runtime
            return .{ .null_val = {} };
        }

        return EvalError.UnknownFunction;
    }

    /// Evaluate a list expression
    fn evaluateList(
        self: *Self,
        l: *const ast.ListExpr,
        row: *const Row,
        ctx: *const ExecutionContext,
    ) EvalError!EvalResult {
        const results = self.allocator.alloc(EvalResult, l.elements.len) catch return EvalError.OutOfMemory;
        errdefer self.allocator.free(results);

        for (l.elements, 0..) |elem, i| {
            results[i] = try self.evaluate(elem, row, ctx);
        }

        return .{ .list_val = results };
    }

    /// Evaluate a map expression (returns null for now)
    fn evaluateMap(_: *Self) EvalResult {
        // TODO: implement map evaluation
        return .{ .null_val = {} };
    }

    // ========================================================================
    // Comparison helpers
    // ========================================================================

    fn compareEqual(_: *Self, left: EvalResult, right: EvalResult) EvalResult {
        const result = switch (left) {
            .bool_val => |l| switch (right) {
                .bool_val => |r| l == r,
                else => false,
            },
            .int_val => |l| switch (right) {
                .int_val => |r| l == r,
                .float_val => |r| @as(f64, @floatFromInt(l)) == r,
                else => false,
            },
            .float_val => |l| switch (right) {
                .float_val => |r| l == r,
                .int_val => |r| l == @as(f64, @floatFromInt(r)),
                else => false,
            },
            .string_val => |l| switch (right) {
                .string_val => |r| std.mem.eql(u8, l, r),
                else => false,
            },
            .node_ref => |l| switch (right) {
                .node_ref => |r| l == r,
                else => false,
            },
            .edge_ref => |l| switch (right) {
                .edge_ref => |r| l == r,
                else => false,
            },
            else => false,
        };
        return .{ .bool_val = result };
    }

    fn compareLessThan(_: *Self, left: EvalResult, right: EvalResult) EvalResult {
        const result: ?bool = switch (left) {
            .int_val => |l| switch (right) {
                .int_val => |r| l < r,
                .float_val => |r| @as(f64, @floatFromInt(l)) < r,
                else => null,
            },
            .float_val => |l| switch (right) {
                .float_val => |r| l < r,
                .int_val => |r| l < @as(f64, @floatFromInt(r)),
                else => null,
            },
            .string_val => |l| switch (right) {
                .string_val => |r| std.mem.order(u8, l, r) == .lt,
                else => null,
            },
            else => null,
        };
        if (result) |r| {
            return .{ .bool_val = r };
        }
        return .{ .null_val = {} };
    }

    fn compareLessOrEqual(self: *Self, left: EvalResult, right: EvalResult) EvalResult {
        const lt = self.compareLessThan(left, right);
        const eq = self.compareEqual(left, right);

        if (lt.toBool()) |lt_val| {
            if (eq.toBool()) |eq_val| {
                return .{ .bool_val = lt_val or eq_val };
            }
        }
        return .{ .null_val = {} };
    }

    // ========================================================================
    // Arithmetic helpers
    // ========================================================================

    fn arithmeticAdd(_: *Self, left: EvalResult, right: EvalResult) EvalResult {
        return switch (left) {
            .int_val => |l| switch (right) {
                .int_val => |r| .{ .int_val = l + r },
                .float_val => |r| .{ .float_val = @as(f64, @floatFromInt(l)) + r },
                else => .{ .null_val = {} },
            },
            .float_val => |l| switch (right) {
                .float_val => |r| .{ .float_val = l + r },
                .int_val => |r| .{ .float_val = l + @as(f64, @floatFromInt(r)) },
                else => .{ .null_val = {} },
            },
            .string_val => |l| switch (right) {
                .string_val => |r| blk: {
                    // String concatenation - would need allocation
                    _ = l;
                    _ = r;
                    break :blk .{ .null_val = {} };
                },
                else => .{ .null_val = {} },
            },
            else => .{ .null_val = {} },
        };
    }

    fn arithmeticSub(_: *Self, left: EvalResult, right: EvalResult) EvalResult {
        return switch (left) {
            .int_val => |l| switch (right) {
                .int_val => |r| .{ .int_val = l - r },
                .float_val => |r| .{ .float_val = @as(f64, @floatFromInt(l)) - r },
                else => .{ .null_val = {} },
            },
            .float_val => |l| switch (right) {
                .float_val => |r| .{ .float_val = l - r },
                .int_val => |r| .{ .float_val = l - @as(f64, @floatFromInt(r)) },
                else => .{ .null_val = {} },
            },
            else => .{ .null_val = {} },
        };
    }

    fn arithmeticMul(_: *Self, left: EvalResult, right: EvalResult) EvalResult {
        return switch (left) {
            .int_val => |l| switch (right) {
                .int_val => |r| .{ .int_val = l * r },
                .float_val => |r| .{ .float_val = @as(f64, @floatFromInt(l)) * r },
                else => .{ .null_val = {} },
            },
            .float_val => |l| switch (right) {
                .float_val => |r| .{ .float_val = l * r },
                .int_val => |r| .{ .float_val = l * @as(f64, @floatFromInt(r)) },
                else => .{ .null_val = {} },
            },
            else => .{ .null_val = {} },
        };
    }

    fn arithmeticDiv(_: *Self, left: EvalResult, right: EvalResult) EvalError!EvalResult {
        // Check for division by zero
        switch (right) {
            .int_val => |r| if (r == 0) return EvalError.DivisionByZero,
            .float_val => |r| if (r == 0.0) return EvalError.DivisionByZero,
            else => {},
        }

        return switch (left) {
            .int_val => |l| switch (right) {
                .int_val => |r| .{ .int_val = @divTrunc(l, r) },
                .float_val => |r| .{ .float_val = @as(f64, @floatFromInt(l)) / r },
                else => .{ .null_val = {} },
            },
            .float_val => |l| switch (right) {
                .float_val => |r| .{ .float_val = l / r },
                .int_val => |r| .{ .float_val = l / @as(f64, @floatFromInt(r)) },
                else => .{ .null_val = {} },
            },
            else => .{ .null_val = {} },
        };
    }

    fn arithmeticMod(_: *Self, left: EvalResult, right: EvalResult) EvalError!EvalResult {
        // Check for modulo by zero
        switch (right) {
            .int_val => |r| if (r == 0) return EvalError.DivisionByZero,
            else => {},
        }

        return switch (left) {
            .int_val => |l| switch (right) {
                .int_val => |r| .{ .int_val = @mod(l, r) },
                else => .{ .null_val = {} },
            },
            else => .{ .null_val = {} },
        };
    }

    fn arithmeticPow(_: *Self, left: EvalResult, right: EvalResult) EvalResult {
        const base: f64 = switch (left) {
            .int_val => |l| @floatFromInt(l),
            .float_val => |l| l,
            else => return .{ .null_val = {} },
        };

        const exp: f64 = switch (right) {
            .int_val => |r| @floatFromInt(r),
            .float_val => |r| r,
            else => return .{ .null_val = {} },
        };

        return .{ .float_val = std.math.pow(f64, base, exp) };
    }

    // ========================================================================
    // String helpers
    // ========================================================================

    fn stringContains(_: *Self, left: EvalResult, right: EvalResult) EvalResult {
        const haystack = switch (left) {
            .string_val => |s| s,
            else => return .{ .null_val = {} },
        };

        const needle = switch (right) {
            .string_val => |s| s,
            else => return .{ .null_val = {} },
        };

        return .{ .bool_val = std.mem.indexOf(u8, haystack, needle) != null };
    }

    fn stringStartsWith(_: *Self, left: EvalResult, right: EvalResult) EvalResult {
        const haystack = switch (left) {
            .string_val => |s| s,
            else => return .{ .null_val = {} },
        };

        const needle = switch (right) {
            .string_val => |s| s,
            else => return .{ .null_val = {} },
        };

        return .{ .bool_val = std.mem.startsWith(u8, haystack, needle) };
    }

    fn stringEndsWith(_: *Self, left: EvalResult, right: EvalResult) EvalResult {
        const haystack = switch (left) {
            .string_val => |s| s,
            else => return .{ .null_val = {} },
        };

        const needle = switch (right) {
            .string_val => |s| s,
            else => return .{ .null_val = {} },
        };

        return .{ .bool_val = std.mem.endsWith(u8, haystack, needle) };
    }

    // ========================================================================
    // List helpers
    // ========================================================================

    fn listContains(self: *Self, elem: EvalResult, list: EvalResult) EvalResult {
        const items = switch (list) {
            .list_val => |l| l,
            else => return .{ .null_val = {} },
        };

        for (items) |item| {
            const eq = self.compareEqual(elem, item);
            if (eq.toBool()) |is_eq| {
                if (is_eq) return .{ .bool_val = true };
            }
        }

        return .{ .bool_val = false };
    }
};

// ============================================================================
// Tests
// ============================================================================

test "evaluate integer literal" {
    const allocator = std.testing.allocator;
    var eval = ExpressionEvaluator.init(allocator);
    var row = Row.init();
    var ctx = ExecutionContext.init(allocator);
    defer ctx.deinit();

    const expr = ast.Expression{
        .literal = .{
            .value = .{ .integer = 42 },
            .location = .{ .line = 1, .column = 1 },
        },
    };

    const result = try eval.evaluate(&expr, &row, &ctx);
    try std.testing.expectEqual(@as(i64, 42), result.int_val);
}

test "evaluate float literal" {
    const allocator = std.testing.allocator;
    var eval = ExpressionEvaluator.init(allocator);
    var row = Row.init();
    var ctx = ExecutionContext.init(allocator);
    defer ctx.deinit();

    const expr = ast.Expression{
        .literal = .{
            .value = .{ .float = 3.14 },
            .location = .{ .line = 1, .column = 1 },
        },
    };

    const result = try eval.evaluate(&expr, &row, &ctx);
    try std.testing.expectApproxEqAbs(@as(f64, 3.14), result.float_val, 0.001);
}

test "evaluate string literal" {
    const allocator = std.testing.allocator;
    var eval = ExpressionEvaluator.init(allocator);
    var row = Row.init();
    var ctx = ExecutionContext.init(allocator);
    defer ctx.deinit();

    const expr = ast.Expression{
        .literal = .{
            .value = .{ .string = "hello" },
            .location = .{ .line = 1, .column = 1 },
        },
    };

    const result = try eval.evaluate(&expr, &row, &ctx);
    try std.testing.expectEqualStrings("hello", result.string_val);
}

test "evaluate boolean literal" {
    const allocator = std.testing.allocator;
    var eval = ExpressionEvaluator.init(allocator);
    var row = Row.init();
    var ctx = ExecutionContext.init(allocator);
    defer ctx.deinit();

    const expr = ast.Expression{
        .literal = .{
            .value = .{ .boolean = true },
            .location = .{ .line = 1, .column = 1 },
        },
    };

    const result = try eval.evaluate(&expr, &row, &ctx);
    try std.testing.expectEqual(true, result.bool_val);
}

test "evaluate null literal" {
    const allocator = std.testing.allocator;
    var eval = ExpressionEvaluator.init(allocator);
    var row = Row.init();
    var ctx = ExecutionContext.init(allocator);
    defer ctx.deinit();

    const expr = ast.Expression{
        .literal = .{
            .value = .{ .null_value = {} },
            .location = .{ .line = 1, .column = 1 },
        },
    };

    const result = try eval.evaluate(&expr, &row, &ctx);
    try std.testing.expect(result.isNull());
}

test "evaluate variable reference" {
    const allocator = std.testing.allocator;
    var eval = ExpressionEvaluator.init(allocator);
    var row = Row.init();
    row.setSlot(0, .{ .node_ref = 123 });

    var ctx = ExecutionContext.init(allocator);
    defer ctx.deinit();
    try ctx.registerVariable("n", 0);

    const expr = ast.Expression{
        .variable = .{
            .name = "n",
            .location = .{ .line = 1, .column = 1 },
        },
    };

    const result = try eval.evaluate(&expr, &row, &ctx);
    try std.testing.expectEqual(@as(NodeId, 123), result.node_ref);
}

test "evaluate parameter reference" {
    const allocator = std.testing.allocator;
    var eval = ExpressionEvaluator.init(allocator);
    var row = Row.init();

    var ctx = ExecutionContext.init(allocator);
    defer ctx.deinit();
    try ctx.setParameter("name", .{ .string_val = "Alice" });

    const expr = ast.Expression{
        .parameter = .{
            .name = "name",
            .location = .{ .line = 1, .column = 1 },
        },
    };

    const result = try eval.evaluate(&expr, &row, &ctx);
    try std.testing.expectEqualStrings("Alice", result.string_val);
}

test "eval result truthiness" {
    const null_val = EvalResult{ .null_val = {} };
    const false_val = EvalResult{ .bool_val = false };
    const true_val = EvalResult{ .bool_val = true };
    const zero_val = EvalResult{ .int_val = 0 };
    const one_val = EvalResult{ .int_val = 1 };
    const hello_val = EvalResult{ .string_val = "hello" };
    const empty_val = EvalResult{ .string_val = "" };

    try std.testing.expect(!null_val.isTruthy());
    try std.testing.expect(!false_val.isTruthy());
    try std.testing.expect(true_val.isTruthy());
    try std.testing.expect(!zero_val.isTruthy());
    try std.testing.expect(one_val.isTruthy());
    try std.testing.expect(hello_val.isTruthy());
    try std.testing.expect(!empty_val.isTruthy());
}

test "eval result conversions" {
    const int_result = EvalResult{ .int_val = 42 };
    try std.testing.expectEqual(@as(i64, 42), int_result.toInt().?);
    try std.testing.expectEqual(@as(f64, 42.0), int_result.toFloat().?);

    const float_result = EvalResult{ .float_val = 3.5 };
    try std.testing.expectEqual(@as(i64, 3), float_result.toInt().?);
    try std.testing.expectEqual(@as(f64, 3.5), float_result.toFloat().?);

    const bool_result = EvalResult{ .bool_val = true };
    try std.testing.expectEqual(true, bool_result.toBool().?);
}
