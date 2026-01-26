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

const symbols = @import("../graph/symbols.zig");
const SymbolId = symbols.SymbolId;

const Row = executor.Row;
const SlotValue = executor.SlotValue;
const ExecutionContext = executor.ExecutionContext;

/// Decode an edge ID back to its components.
/// Note: This encoding assumes target < 65536 and source < 4 billion.
fn decodeEdgeId(edge_id: EdgeId) struct { source: NodeId, target: NodeId, edge_type: SymbolId } {
    return .{
        .source = edge_id >> 32,
        .target = (edge_id >> 16) & 0xFFFF,
        .edge_type = @intCast(edge_id & 0xFFFF),
    };
}

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
    /// Vector result (float32 array)
    vector_val: []const f32,
    /// Map result (key-value pairs)
    map_val: []const MapEntry,

    pub const MapEntry = struct {
        key: []const u8,
        value: EvalResult,
    };

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
            .vector_val => |v| v.len > 0,
            .map_val => |m| m.len > 0,
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

    /// Convert from PropertyValue, cloning all borrowed data.
    /// All slice data (strings, vectors, map keys) is duplicated onto the
    /// provided allocator so the result is independent of the source lifetime.
    pub fn fromPropertyValue(pv: PropertyValue, allocator: Allocator) Self {
        return switch (pv) {
            .null_val => .{ .null_val = {} },
            .bool_val => |b| .{ .bool_val = b },
            .int_val => |i| .{ .int_val = i },
            .float_val => |f| .{ .float_val = f },
            .string_val => |s| blk: {
                const cloned = allocator.dupe(u8, s) catch break :blk .{ .null_val = {} };
                break :blk .{ .string_val = cloned };
            },
            .bytes_val => |b| blk: {
                const cloned = allocator.dupe(u8, b) catch break :blk .{ .null_val = {} };
                break :blk .{ .string_val = cloned };
            },
            .vector_val => |v| blk: {
                const cloned = allocator.dupe(f32, v) catch break :blk .{ .null_val = {} };
                break :blk .{ .vector_val = cloned };
            },
            .list_val => |list| blk: {
                const results = allocator.alloc(EvalResult, list.len) catch
                    break :blk .{ .null_val = {} };
                for (list, 0..) |item, i| {
                    results[i] = fromPropertyValue(item, allocator);
                }
                break :blk .{ .list_val = results };
            },
            .map_val => |map| blk: {
                const entries = allocator.alloc(MapEntry, map.len) catch
                    break :blk .{ .null_val = {} };
                for (map, 0..) |entry, i| {
                    entries[i] = .{
                        .key = allocator.dupe(u8, entry.key) catch break :blk .{ .null_val = {} },
                        .value = fromPropertyValue(entry.value, allocator),
                    };
                }
                break :blk .{ .map_val = entries };
            },
        };
    }

    /// Convert to PropertyValue (allocator needed for list/map conversion)
    pub fn toPropertyValue(self: Self, allocator: Allocator) PropertyValue {
        return switch (self) {
            .null_val => .{ .null_val = {} },
            .bool_val => |b| .{ .bool_val = b },
            .int_val => |i| .{ .int_val = i },
            .float_val => |f| .{ .float_val = f },
            .string_val => |s| .{ .string_val = s },
            .node_ref => |id| .{ .int_val = @intCast(id) },
            .edge_ref => |id| .{ .int_val = @intCast(id) },
            .vector_val => |v| .{ .vector_val = v },
            .list_val => |list| blk: {
                const props = allocator.alloc(PropertyValue, list.len) catch
                    break :blk .{ .null_val = {} };
                for (list, 0..) |item, i| {
                    props[i] = item.toPropertyValue(allocator);
                }
                break :blk .{ .list_val = props };
            },
            .map_val => |map| blk: {
                const entries = allocator.alloc(PropertyValue.MapEntry, map.len) catch
                    break :blk .{ .null_val = {} };
                for (map, 0..) |entry, i| {
                    entries[i] = .{
                        .key = entry.key,
                        .value = entry.value.toPropertyValue(allocator),
                    };
                }
                break :blk .{ .map_val = entries };
            },
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
            .map => |m| self.evaluateMap(m, row, ctx),
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
        self: *Self,
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
            .property => |pv| EvalResult.fromPropertyValue(pv, self.allocator),
        };
    }

    /// Evaluate a parameter reference
    fn evaluateParameter(
        self: *Self,
        p: ast.ParameterRef,
        ctx: *const ExecutionContext,
    ) EvalError!EvalResult {
        const pv = ctx.getParameter(p.name) orelse return EvalError.ParameterNotFound;
        return EvalResult.fromPropertyValue(pv, self.allocator);
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
            defer node.deinit(ctx.allocator);

            // Find the property with matching key_id
            for (node.properties) |prop| {
                if (prop.key_id == key_id) {
                    return EvalResult.fromPropertyValue(prop.value, self.allocator);
                }
            }

            // Property not found on this node
            return .{ .null_val = {} };
        }

        // Handle edge property access
        if (obj == .edge_ref) {
            const edge_id = obj.edge_ref;

            // Get the database and symbol table from context
            const database = ctx.database orelse return .{ .null_val = {} };
            const symbol_table = ctx.symbol_table orelse return .{ .null_val = {} };

            // Decode edge ID to get source, target, and edge type
            const decoded = decodeEdgeId(edge_id);

            // Look up the property key symbol ID
            const key_id = symbol_table.lookup(pa.property) catch {
                // Property key not found in symbol table
                return .{ .null_val = {} };
            };

            // Get the edge from storage
            var edge = database.edge_store.get(decoded.source, decoded.target, decoded.edge_type) catch {
                return .{ .null_val = {} };
            };
            defer edge.deinit(ctx.allocator);

            // Find the property with matching key_id
            for (edge.properties) |prop| {
                if (prop.key_id == key_id) {
                    return EvalResult.fromPropertyValue(prop.value, self.allocator);
                }
            }

            // Property not found on this edge
            return .{ .null_val = {} };
        }

        // Unknown object type
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
            .regex_match => self.regexMatch(left, right),

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
            const arg = try self.evaluate(f.arguments[0], row, ctx);
            return switch (arg) {
                .string_val => arg,
                .int_val => |i| blk: {
                    const s = std.fmt.allocPrint(self.allocator, "{d}", .{i}) catch return EvalError.OutOfMemory;
                    break :blk .{ .string_val = s };
                },
                .float_val => |fv| blk: {
                    const s = std.fmt.allocPrint(self.allocator, "{d}", .{fv}) catch return EvalError.OutOfMemory;
                    break :blk .{ .string_val = s };
                },
                .bool_val => |b| .{ .string_val = if (b) "true" else "false" },
                .null_val => .{ .string_val = "null" },
                else => .{ .null_val = {} },
            };
        }

        // String functions
        if (std.mem.eql(u8, f.name, "toLower")) {
            if (f.arguments.len != 1) return EvalError.InvalidArgumentCount;
            const arg = try self.evaluate(f.arguments[0], row, ctx);
            return switch (arg) {
                .string_val => |s| blk: {
                    const copy = self.allocator.dupe(u8, s) catch return EvalError.OutOfMemory;
                    for (copy) |*c| {
                        c.* = std.ascii.toLower(c.*);
                    }
                    break :blk .{ .string_val = copy };
                },
                .null_val => .{ .null_val = {} },
                else => EvalError.TypeError,
            };
        }

        if (std.mem.eql(u8, f.name, "toUpper")) {
            if (f.arguments.len != 1) return EvalError.InvalidArgumentCount;
            const arg = try self.evaluate(f.arguments[0], row, ctx);
            return switch (arg) {
                .string_val => |s| blk: {
                    const copy = self.allocator.dupe(u8, s) catch return EvalError.OutOfMemory;
                    for (copy) |*c| {
                        c.* = std.ascii.toUpper(c.*);
                    }
                    break :blk .{ .string_val = copy };
                },
                .null_val => .{ .null_val = {} },
                else => EvalError.TypeError,
            };
        }

        if (std.mem.eql(u8, f.name, "trim")) {
            if (f.arguments.len != 1) return EvalError.InvalidArgumentCount;
            const arg = try self.evaluate(f.arguments[0], row, ctx);
            return switch (arg) {
                .string_val => |s| .{ .string_val = std.mem.trim(u8, s, " \t\n\r") },
                .null_val => .{ .null_val = {} },
                else => EvalError.TypeError,
            };
        }

        if (std.mem.eql(u8, f.name, "substring")) {
            if (f.arguments.len < 2 or f.arguments.len > 3) return EvalError.InvalidArgumentCount;
            const arg = try self.evaluate(f.arguments[0], row, ctx);
            const start_arg = try self.evaluate(f.arguments[1], row, ctx);
            const s = switch (arg) {
                .string_val => |sv| sv,
                .null_val => return .{ .null_val = {} },
                else => return EvalError.TypeError,
            };
            const start_raw = start_arg.toInt() orelse return EvalError.TypeError;
            const start: usize = if (start_raw < 0) 0 else @intCast(@min(start_raw, @as(i64, @intCast(s.len))));
            if (f.arguments.len == 3) {
                const len_arg = try self.evaluate(f.arguments[2], row, ctx);
                const length_raw = len_arg.toInt() orelse return EvalError.TypeError;
                const length: usize = if (length_raw < 0) 0 else @intCast(@min(length_raw, @as(i64, @intCast(s.len - start))));
                return .{ .string_val = s[start .. start + length] };
            }
            return .{ .string_val = s[start..] };
        }

        if (std.mem.eql(u8, f.name, "replace")) {
            if (f.arguments.len != 3) return EvalError.InvalidArgumentCount;
            const arg = try self.evaluate(f.arguments[0], row, ctx);
            const search_arg = try self.evaluate(f.arguments[1], row, ctx);
            const replace_arg = try self.evaluate(f.arguments[2], row, ctx);
            const s = switch (arg) {
                .string_val => |sv| sv,
                .null_val => return .{ .null_val = {} },
                else => return EvalError.TypeError,
            };
            const search = switch (search_arg) {
                .string_val => |sv| sv,
                else => return EvalError.TypeError,
            };
            const replacement = switch (replace_arg) {
                .string_val => |sv| sv,
                else => return EvalError.TypeError,
            };
            if (search.len == 0) {
                return .{ .string_val = s };
            }
            var result_buf: std.ArrayListUnmanaged(u8) = .empty;
            errdefer result_buf.deinit(self.allocator);
            var i: usize = 0;
            while (i < s.len) {
                if (i + search.len <= s.len and std.mem.eql(u8, s[i .. i + search.len], search)) {
                    result_buf.appendSlice(self.allocator, replacement) catch return EvalError.OutOfMemory;
                    i += search.len;
                } else {
                    result_buf.append(self.allocator, s[i]) catch return EvalError.OutOfMemory;
                    i += 1;
                }
            }
            return .{ .string_val = result_buf.toOwnedSlice(self.allocator) catch return EvalError.OutOfMemory };
        }

        if (std.mem.eql(u8, f.name, "split")) {
            if (f.arguments.len != 2) return EvalError.InvalidArgumentCount;
            const arg = try self.evaluate(f.arguments[0], row, ctx);
            const delim_arg = try self.evaluate(f.arguments[1], row, ctx);
            const s = switch (arg) {
                .string_val => |sv| sv,
                .null_val => return .{ .null_val = {} },
                else => return EvalError.TypeError,
            };
            const delimiter = switch (delim_arg) {
                .string_val => |sv| sv,
                else => return EvalError.TypeError,
            };
            var parts: std.ArrayListUnmanaged(EvalResult) = .empty;
            errdefer parts.deinit(self.allocator);
            var iter = std.mem.splitSequence(u8, s, delimiter);
            while (iter.next()) |part| {
                parts.append(self.allocator, .{ .string_val = part }) catch return EvalError.OutOfMemory;
            }
            return .{ .list_val = parts.toOwnedSlice(self.allocator) catch return EvalError.OutOfMemory };
        }

        // List functions
        if (std.mem.eql(u8, f.name, "head")) {
            if (f.arguments.len != 1) return EvalError.InvalidArgumentCount;
            const arg = try self.evaluate(f.arguments[0], row, ctx);
            return switch (arg) {
                .list_val => |l| if (l.len > 0) l[0] else .{ .null_val = {} },
                .null_val => .{ .null_val = {} },
                else => EvalError.TypeError,
            };
        }

        if (std.mem.eql(u8, f.name, "tail")) {
            if (f.arguments.len != 1) return EvalError.InvalidArgumentCount;
            const arg = try self.evaluate(f.arguments[0], row, ctx);
            return switch (arg) {
                .list_val => |l| if (l.len > 1)
                    .{ .list_val = l[1..] }
                else
                    .{ .list_val = &[_]EvalResult{} },
                .null_val => .{ .null_val = {} },
                else => EvalError.TypeError,
            };
        }

        if (std.mem.eql(u8, f.name, "last")) {
            if (f.arguments.len != 1) return EvalError.InvalidArgumentCount;
            const arg = try self.evaluate(f.arguments[0], row, ctx);
            return switch (arg) {
                .list_val => |l| if (l.len > 0) l[l.len - 1] else .{ .null_val = {} },
                .null_val => .{ .null_val = {} },
                else => EvalError.TypeError,
            };
        }

        if (std.mem.eql(u8, f.name, "range")) {
            if (f.arguments.len < 2 or f.arguments.len > 3) return EvalError.InvalidArgumentCount;
            const start_arg = try self.evaluate(f.arguments[0], row, ctx);
            const end_arg = try self.evaluate(f.arguments[1], row, ctx);
            const start = start_arg.toInt() orelse return EvalError.TypeError;
            const end = end_arg.toInt() orelse return EvalError.TypeError;
            var step: i64 = 1;
            if (f.arguments.len == 3) {
                const step_arg = try self.evaluate(f.arguments[2], row, ctx);
                step = step_arg.toInt() orelse return EvalError.TypeError;
                if (step == 0) return EvalError.InvalidOperation;
            }
            var items: std.ArrayListUnmanaged(EvalResult) = .empty;
            errdefer items.deinit(self.allocator);
            // Cypher range() is inclusive on both ends
            if (step > 0) {
                var i = start;
                while (i <= end) : (i += step) {
                    items.append(self.allocator, .{ .int_val = i }) catch return EvalError.OutOfMemory;
                }
            } else {
                var i = start;
                while (i >= end) : (i += step) {
                    items.append(self.allocator, .{ .int_val = i }) catch return EvalError.OutOfMemory;
                }
            }
            return .{ .list_val = items.toOwnedSlice(self.allocator) catch return EvalError.OutOfMemory };
        }

        // Type/graph functions
        if (std.mem.eql(u8, f.name, "type")) {
            if (f.arguments.len != 1) return EvalError.InvalidArgumentCount;
            const arg = try self.evaluate(f.arguments[0], row, ctx);
            return switch (arg) {
                .edge_ref => |edge_id| blk: {
                    const symbol_table = ctx.symbol_table orelse break :blk .{ .null_val = {} };
                    const decoded = decodeEdgeId(edge_id);
                    const resolved = symbol_table.resolve(decoded.edge_type) catch break :blk .{ .null_val = {} };
                    break :blk .{ .string_val = resolved };
                },
                .null_val => .{ .null_val = {} },
                else => EvalError.TypeError,
            };
        }

        if (std.mem.eql(u8, f.name, "labels")) {
            if (f.arguments.len != 1) return EvalError.InvalidArgumentCount;
            const arg = try self.evaluate(f.arguments[0], row, ctx);
            return switch (arg) {
                .node_ref => |node_id| blk: {
                    const node_store = ctx.node_store orelse break :blk .{ .null_val = {} };
                    const symbol_table = ctx.symbol_table orelse break :blk .{ .null_val = {} };
                    var node = node_store.get(node_id) catch break :blk .{ .null_val = {} };
                    defer node.deinit(ctx.allocator);
                    var label_list: std.ArrayListUnmanaged(EvalResult) = .empty;
                    for (node.labels) |label_id| {
                        const resolved = symbol_table.resolve(label_id) catch continue;
                        label_list.append(self.allocator, .{ .string_val = resolved }) catch return EvalError.OutOfMemory;
                    }
                    break :blk .{ .list_val = label_list.toOwnedSlice(self.allocator) catch return EvalError.OutOfMemory };
                },
                .null_val => .{ .null_val = {} },
                else => EvalError.TypeError,
            };
        }

        if (std.mem.eql(u8, f.name, "properties")) {
            if (f.arguments.len != 1) return EvalError.InvalidArgumentCount;
            const arg = try self.evaluate(f.arguments[0], row, ctx);
            if (arg == .node_ref) {
                const node_id = arg.node_ref;
                const node_store = ctx.node_store orelse return .{ .null_val = {} };
                const symbol_table = ctx.symbol_table orelse return .{ .null_val = {} };
                var node = node_store.get(node_id) catch return .{ .null_val = {} };
                defer node.deinit(ctx.allocator);
                var entries: std.ArrayListUnmanaged(EvalResult.MapEntry) = .empty;
                for (node.properties) |prop| {
                    const key = symbol_table.resolve(prop.key_id) catch continue;
                    entries.append(self.allocator, .{
                        .key = key,
                        .value = EvalResult.fromPropertyValue(prop.value, self.allocator),
                    }) catch return EvalError.OutOfMemory;
                }
                return .{ .map_val = entries.toOwnedSlice(self.allocator) catch return EvalError.OutOfMemory };
            } else if (arg == .edge_ref) {
                const edge_id = arg.edge_ref;
                const database = ctx.database orelse return .{ .null_val = {} };
                const symbol_table = ctx.symbol_table orelse return .{ .null_val = {} };
                const decoded = decodeEdgeId(edge_id);
                var edge = database.edge_store.get(decoded.source, decoded.target, decoded.edge_type) catch return .{ .null_val = {} };
                defer edge.deinit(ctx.allocator);
                var entries: std.ArrayListUnmanaged(EvalResult.MapEntry) = .empty;
                for (edge.properties) |prop| {
                    const key = symbol_table.resolve(prop.key_id) catch continue;
                    entries.append(self.allocator, .{
                        .key = key,
                        .value = EvalResult.fromPropertyValue(prop.value, self.allocator),
                    }) catch return EvalError.OutOfMemory;
                }
                return .{ .map_val = entries.toOwnedSlice(self.allocator) catch return EvalError.OutOfMemory };
            } else if (arg == .null_val) {
                return .{ .null_val = {} };
            }
            return EvalError.TypeError;
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

    /// Evaluate a map expression {key: value, ...}
    fn evaluateMap(
        self: *Self,
        m: *const ast.MapExpr,
        row: *const Row,
        ctx: *const ExecutionContext,
    ) EvalError!EvalResult {
        const entries = self.allocator.alloc(EvalResult.MapEntry, m.entries.len) catch return EvalError.OutOfMemory;
        errdefer self.allocator.free(entries);

        for (m.entries, 0..) |entry, i| {
            entries[i] = .{
                .key = entry.key,
                .value = try self.evaluate(entry.value, row, ctx),
            };
        }

        return .{ .map_val = entries };
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
    // Regex helpers
    // ========================================================================

    fn regexMatch(_: *Self, left: EvalResult, right: EvalResult) EvalResult {
        const text = switch (left) {
            .string_val => |s| s,
            else => return .{ .null_val = {} },
        };

        const pattern = switch (right) {
            .string_val => |s| s,
            else => return .{ .null_val = {} },
        };

        // Cypher =~ does a full match (pattern anchored to start and end)
        return .{ .bool_val = regexFullMatch(text, pattern) };
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
// Regex Engine
// ============================================================================

/// Match text against pattern (full match — pattern must match the entire text).
/// Supports: . * + ? | () [] [^] \d \w \s \D \W \S and literal escapes.
fn regexFullMatch(text: []const u8, pattern: []const u8) bool {
    // Try to match the full pattern against the full text
    const result = regexMatchHere(text, 0, pattern, 0);
    return result != null and result.? == text.len;
}

/// Try to match pattern[pi..] against text[ti..].
/// Returns the text index after the match, or null if no match.
fn regexMatchHere(text: []const u8, ti: usize, pattern: []const u8, pi: usize) ?usize {
    // End of pattern — match succeeds at current text position
    if (pi >= pattern.len) return ti;

    // Handle alternation: try left side, then right side
    if (findAlternation(pattern, pi)) |alt_pos| {
        // Try left branch (pattern[pi..alt_pos])
        const left_result = regexMatchBranch(text, ti, pattern[pi..alt_pos]);
        if (left_result != null) return left_result;
        // Try right branch (pattern[alt_pos+1..])
        return regexMatchBranch(text, ti, pattern[alt_pos + 1 ..]);
    }

    // Handle grouping with parentheses
    if (pi < pattern.len and pattern[pi] == '(') {
        if (findClosingParen(pattern, pi)) |close| {
            const sub_pattern = pattern[pi + 1 .. close];
            const after_close = close + 1;

            // Check for quantifier after group
            if (after_close < pattern.len) {
                switch (pattern[after_close]) {
                    '*' => return matchQuantified(text, ti, sub_pattern, pattern, after_close + 1, 0, text.len),
                    '+' => return matchQuantified(text, ti, sub_pattern, pattern, after_close + 1, 1, text.len),
                    '?' => {
                        // Try matching the group, then continue
                        const group_match = regexMatchBranch(text, ti, sub_pattern);
                        if (group_match) |end| {
                            const rest = regexMatchHere(text, end, pattern, after_close + 1);
                            if (rest != null) return rest;
                        }
                        // Try skipping the group
                        return regexMatchHere(text, ti, pattern, after_close + 1);
                    },
                    else => {},
                }
            }

            // No quantifier — match group exactly once
            const group_match = regexMatchBranch(text, ti, sub_pattern);
            if (group_match) |end| {
                return regexMatchHere(text, end, pattern, after_close);
            }
            return null;
        }
        // Unmatched paren — treat as literal
    }

    // Parse current atom
    const atom = parseAtom(pattern, pi) orelse return null;

    // Check for quantifier after atom
    const after_atom = atom.end;
    if (after_atom < pattern.len) {
        switch (pattern[after_atom]) {
            '*' => return matchAtomQuantified(text, ti, atom, pattern, after_atom + 1, 0, text.len),
            '+' => return matchAtomQuantified(text, ti, atom, pattern, after_atom + 1, 1, text.len),
            '?' => {
                // Try matching atom then rest
                if (ti < text.len and matchAtomChar(atom, text[ti])) {
                    const rest = regexMatchHere(text, ti + 1, pattern, after_atom + 1);
                    if (rest != null) return rest;
                }
                // Try skipping atom
                return regexMatchHere(text, ti, pattern, after_atom + 1);
            },
            else => {},
        }
    }

    // No quantifier — match exactly one character
    if (ti >= text.len) return null;
    if (!matchAtomChar(atom, text[ti])) return null;
    return regexMatchHere(text, ti + 1, pattern, after_atom);
}

/// Match a branch (sub-pattern without top-level alternation)
fn regexMatchBranch(text: []const u8, ti: usize, pattern: []const u8) ?usize {
    return regexMatchHere(text, ti, pattern, 0);
}

/// Find top-level alternation '|' (not inside parentheses)
fn findAlternation(pattern: []const u8, start: usize) ?usize {
    var depth: usize = 0;
    var i = start;
    while (i < pattern.len) : (i += 1) {
        switch (pattern[i]) {
            '(' => depth += 1,
            ')' => {
                if (depth > 0) depth -= 1;
            },
            '|' => {
                if (depth == 0) return i;
            },
            '\\' => i += 1, // skip escaped char
            else => {},
        }
    }
    return null;
}

/// Find matching closing parenthesis
fn findClosingParen(pattern: []const u8, open: usize) ?usize {
    var depth: usize = 1;
    var i = open + 1;
    while (i < pattern.len) : (i += 1) {
        switch (pattern[i]) {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if (depth == 0) return i;
            },
            '\\' => i += 1,
            else => {},
        }
    }
    return null;
}

/// Atom representation — a single matchable unit
const Atom = struct {
    kind: AtomKind,
    end: usize, // index in pattern after this atom

    const AtomKind = union(enum) {
        dot: void, // .
        literal: u8, // exact character
        char_class: CharClass, // [...]
        shorthand: Shorthand, // \d, \w, \s, \D, \W, \S
    };
};

const CharClass = struct {
    start: usize, // start of class content in pattern
    end: usize, // end of class content (before ])
    negated: bool,
    pattern: []const u8, // reference to full pattern
};

const Shorthand = enum { digit, word, space, not_digit, not_word, not_space };

/// Parse a single atom from pattern at position pi
fn parseAtom(pattern: []const u8, pi: usize) ?Atom {
    if (pi >= pattern.len) return null;

    switch (pattern[pi]) {
        '.' => return .{ .kind = .{ .dot = {} }, .end = pi + 1 },
        '[' => {
            // Character class
            var i = pi + 1;
            const negated = i < pattern.len and pattern[i] == '^';
            if (negated) i += 1;
            const content_start = i;
            // First char after ^ can be ] without closing
            if (i < pattern.len and pattern[i] == ']') i += 1;
            while (i < pattern.len and pattern[i] != ']') : (i += 1) {
                if (pattern[i] == '\\' and i + 1 < pattern.len) i += 1;
            }
            if (i >= pattern.len) return null; // unclosed bracket
            return .{
                .kind = .{ .char_class = .{
                    .start = content_start,
                    .end = i,
                    .negated = negated,
                    .pattern = pattern,
                } },
                .end = i + 1,
            };
        },
        '\\' => {
            if (pi + 1 >= pattern.len) return null;
            const next = pattern[pi + 1];
            const shorthand: ?Shorthand = switch (next) {
                'd' => .digit,
                'D' => .not_digit,
                'w' => .word,
                'W' => .not_word,
                's' => .space,
                'S' => .not_space,
                else => null,
            };
            if (shorthand) |sh| {
                return .{ .kind = .{ .shorthand = sh }, .end = pi + 2 };
            }
            // Escaped literal
            return .{ .kind = .{ .literal = next }, .end = pi + 2 };
        },
        else => |c| return .{ .kind = .{ .literal = c }, .end = pi + 1 },
    }
}

/// Check if a character matches an atom
fn matchAtomChar(atom: Atom, ch: u8) bool {
    switch (atom.kind) {
        .dot => return ch != '\n', // . matches anything except newline
        .literal => |c| return ch == c,
        .shorthand => |sh| return matchShorthand(sh, ch),
        .char_class => |cc| return matchCharClass(cc, ch),
    }
}

fn matchShorthand(sh: Shorthand, ch: u8) bool {
    return switch (sh) {
        .digit => std.ascii.isDigit(ch),
        .not_digit => !std.ascii.isDigit(ch),
        .word => std.ascii.isAlphanumeric(ch) or ch == '_',
        .not_word => !(std.ascii.isAlphanumeric(ch) or ch == '_'),
        .space => std.ascii.isWhitespace(ch),
        .not_space => !std.ascii.isWhitespace(ch),
    };
}

fn matchCharClass(cc: CharClass, ch: u8) bool {
    var matched = false;
    var i = cc.start;
    while (i < cc.end) {
        var c: u8 = undefined;
        if (cc.pattern[i] == '\\' and i + 1 < cc.end) {
            const next = cc.pattern[i + 1];
            // Check shorthands inside character class
            const sh: ?Shorthand = switch (next) {
                'd' => .digit,
                'D' => .not_digit,
                'w' => .word,
                'W' => .not_word,
                's' => .space,
                'S' => .not_space,
                else => null,
            };
            if (sh) |s| {
                if (matchShorthand(s, ch)) matched = true;
                i += 2;
                continue;
            }
            c = next;
            i += 2;
        } else {
            c = cc.pattern[i];
            i += 1;
        }

        // Check for range (e.g., a-z)
        if (i < cc.end and cc.pattern[i] == '-' and i + 1 < cc.end) {
            var end_c: u8 = undefined;
            if (cc.pattern[i + 1] == '\\' and i + 2 < cc.end) {
                end_c = cc.pattern[i + 2];
                i += 3;
            } else {
                end_c = cc.pattern[i + 1];
                i += 2;
            }
            if (ch >= c and ch <= end_c) matched = true;
        } else {
            if (ch == c) matched = true;
        }
    }

    return if (cc.negated) !matched else matched;
}

/// Match an atom with quantifier (* or +), greedy, then try rest of pattern
fn matchAtomQuantified(text: []const u8, ti: usize, atom: Atom, pattern: []const u8, pi: usize, min: usize, max: usize) ?usize {
    // Count how many times the atom matches (greedy)
    var count: usize = 0;
    var pos = ti;
    while (pos < text.len and count < max) {
        if (!matchAtomChar(atom, text[pos])) break;
        count += 1;
        pos += 1;
    }

    // Try from longest match down to minimum (greedy backtracking)
    while (count >= min) {
        const rest = regexMatchHere(text, ti + count, pattern, pi);
        if (rest != null) return rest;
        if (count == 0) break;
        count -= 1;
    }
    return null;
}

/// Match a group sub-pattern with quantifier (* or +)
fn matchQuantified(text: []const u8, ti: usize, sub_pattern: []const u8, pattern: []const u8, pi: usize, min: usize, max: usize) ?usize {
    // Collect all possible match lengths for the group
    // For groups, we need to try matching the sub-pattern repeatedly
    var positions: [256]usize = undefined;
    var pos_count: usize = 0;

    // Start with current position
    positions[0] = ti;
    pos_count = 1;

    // Greedily try to match sub_pattern as many times as possible
    var count: usize = 0;
    var current = ti;
    while (count < max and pos_count < 256) {
        const match_end = regexMatchBranch(text, current, sub_pattern);
        if (match_end == null or match_end.? == current) break; // no progress
        current = match_end.?;
        count += 1;
        positions[pos_count] = current;
        pos_count += 1;
    }

    // Try from longest match down to minimum (greedy)
    var idx: usize = pos_count;
    while (idx > 0) {
        idx -= 1;
        if (idx < min) break;
        const rest = regexMatchHere(text, positions[idx], pattern, pi);
        if (rest != null) return rest;
    }
    return null;
}

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
    defer allocator.free(result.string_val);
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

test "fromPropertyValue converts list" {
    const allocator = std.testing.allocator;

    var items = [_]PropertyValue{
        .{ .int_val = 1 },
        .{ .int_val = 2 },
        .{ .string_val = "three" },
    };
    const pv = PropertyValue{ .list_val = &items };

    const result = EvalResult.fromPropertyValue(pv, allocator);
    defer {
        allocator.free(result.list_val[2].string_val);
        allocator.free(result.list_val);
    }

    try std.testing.expectEqual(@as(usize, 3), result.list_val.len);
    try std.testing.expectEqual(@as(i64, 1), result.list_val[0].int_val);
    try std.testing.expectEqual(@as(i64, 2), result.list_val[1].int_val);
    try std.testing.expectEqualStrings("three", result.list_val[2].string_val);
}

test "fromPropertyValue converts vector" {
    const allocator = std.testing.allocator;

    const vec = [_]f32{ 1.0, 2.5, 3.0 };
    const pv = PropertyValue{ .vector_val = &vec };

    const result = EvalResult.fromPropertyValue(pv, allocator);
    defer allocator.free(result.vector_val);

    try std.testing.expectEqual(@as(usize, 3), result.vector_val.len);
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), result.vector_val[0], 0.001);
    try std.testing.expectApproxEqAbs(@as(f32, 2.5), result.vector_val[1], 0.001);
    try std.testing.expectApproxEqAbs(@as(f32, 3.0), result.vector_val[2], 0.001);
}

test "fromPropertyValue converts map" {
    const allocator = std.testing.allocator;

    var entries = [_]PropertyValue.MapEntry{
        .{ .key = "name", .value = .{ .string_val = "Alice" } },
        .{ .key = "age", .value = .{ .int_val = 30 } },
    };
    const pv = PropertyValue{ .map_val = &entries };

    const result = EvalResult.fromPropertyValue(pv, allocator);
    defer {
        allocator.free(result.map_val[0].key);
        allocator.free(result.map_val[0].value.string_val);
        allocator.free(result.map_val[1].key);
        allocator.free(result.map_val);
    }

    try std.testing.expectEqual(@as(usize, 2), result.map_val.len);
    try std.testing.expectEqualStrings("name", result.map_val[0].key);
    try std.testing.expectEqualStrings("Alice", result.map_val[0].value.string_val);
    try std.testing.expectEqualStrings("age", result.map_val[1].key);
    try std.testing.expectEqual(@as(i64, 30), result.map_val[1].value.int_val);
}

test "fromPropertyValue converts nested list" {
    const allocator = std.testing.allocator;

    var inner = [_]PropertyValue{ .{ .int_val = 10 }, .{ .int_val = 20 } };
    var items = [_]PropertyValue{
        .{ .int_val = 1 },
        .{ .list_val = &inner },
    };
    const pv = PropertyValue{ .list_val = &items };

    const result = EvalResult.fromPropertyValue(pv, allocator);
    defer {
        // Free inner list allocation
        allocator.free(result.list_val[1].list_val);
        allocator.free(result.list_val);
    }

    try std.testing.expectEqual(@as(usize, 2), result.list_val.len);
    try std.testing.expectEqual(@as(i64, 1), result.list_val[0].int_val);
    const nested = result.list_val[1].list_val;
    try std.testing.expectEqual(@as(usize, 2), nested.len);
    try std.testing.expectEqual(@as(i64, 10), nested[0].int_val);
    try std.testing.expectEqual(@as(i64, 20), nested[1].int_val);
}

test "toPropertyValue converts list" {
    const allocator = std.testing.allocator;

    const items = [_]EvalResult{
        .{ .int_val = 5 },
        .{ .string_val = "hello" },
        .{ .bool_val = true },
    };
    const eval = EvalResult{ .list_val = &items };

    const pv = eval.toPropertyValue(allocator);
    defer allocator.free(pv.list_val);

    try std.testing.expectEqual(@as(usize, 3), pv.list_val.len);
    try std.testing.expectEqual(@as(i64, 5), pv.list_val[0].int_val);
    try std.testing.expectEqualStrings("hello", pv.list_val[1].string_val);
    try std.testing.expectEqual(true, pv.list_val[2].bool_val);
}

test "toPropertyValue converts vector" {
    const allocator = std.testing.allocator;

    const vec = [_]f32{ 0.5, 1.5, 2.5 };
    const eval = EvalResult{ .vector_val = &vec };

    const pv = eval.toPropertyValue(allocator);

    try std.testing.expectEqual(@as(usize, 3), pv.vector_val.len);
    try std.testing.expectApproxEqAbs(@as(f32, 0.5), pv.vector_val[0], 0.001);
    try std.testing.expectApproxEqAbs(@as(f32, 1.5), pv.vector_val[1], 0.001);
    try std.testing.expectApproxEqAbs(@as(f32, 2.5), pv.vector_val[2], 0.001);
}

test "toPropertyValue converts map" {
    const allocator = std.testing.allocator;

    const entries = [_]EvalResult.MapEntry{
        .{ .key = "x", .value = .{ .int_val = 10 } },
        .{ .key = "y", .value = .{ .float_val = 3.14 } },
    };
    const eval = EvalResult{ .map_val = &entries };

    const pv = eval.toPropertyValue(allocator);
    defer allocator.free(pv.map_val);

    try std.testing.expectEqual(@as(usize, 2), pv.map_val.len);
    try std.testing.expectEqualStrings("x", pv.map_val[0].key);
    try std.testing.expectEqual(@as(i64, 10), pv.map_val[0].value.int_val);
    try std.testing.expectEqualStrings("y", pv.map_val[1].key);
    try std.testing.expectApproxEqAbs(@as(f64, 3.14), pv.map_val[1].value.float_val, 0.001);
}

test "fromPropertyValue/toPropertyValue round-trip for list" {
    const allocator = std.testing.allocator;

    var items = [_]PropertyValue{
        .{ .int_val = 42 },
        .{ .float_val = 2.718 },
        .{ .bool_val = false },
    };
    const original = PropertyValue{ .list_val = &items };

    // PropertyValue -> EvalResult -> PropertyValue
    const eval = EvalResult.fromPropertyValue(original, allocator);
    defer allocator.free(eval.list_val);

    const roundtrip = eval.toPropertyValue(allocator);
    defer allocator.free(roundtrip.list_val);

    try std.testing.expectEqual(@as(usize, 3), roundtrip.list_val.len);
    try std.testing.expectEqual(@as(i64, 42), roundtrip.list_val[0].int_val);
    try std.testing.expectApproxEqAbs(@as(f64, 2.718), roundtrip.list_val[1].float_val, 0.001);
    try std.testing.expectEqual(false, roundtrip.list_val[2].bool_val);
}

test "fromPropertyValue/toPropertyValue round-trip for vector" {
    const allocator = std.testing.allocator;

    const vec = [_]f32{ 1.0, 2.0, 3.0 };
    const original = PropertyValue{ .vector_val = &vec };

    const eval = EvalResult.fromPropertyValue(original, allocator);
    defer allocator.free(eval.vector_val);

    const roundtrip = eval.toPropertyValue(allocator);
    // vector_val in PropertyValue just copies the slice from EvalResult (no new alloc)
    // so no need to free roundtrip.vector_val separately

    try std.testing.expectEqual(@as(usize, 3), roundtrip.vector_val.len);
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), roundtrip.vector_val[0], 0.001);
    try std.testing.expectApproxEqAbs(@as(f32, 2.0), roundtrip.vector_val[1], 0.001);
    try std.testing.expectApproxEqAbs(@as(f32, 3.0), roundtrip.vector_val[2], 0.001);
}

test "fromPropertyValue/toPropertyValue round-trip for map" {
    const allocator = std.testing.allocator;

    var entries = [_]PropertyValue.MapEntry{
        .{ .key = "status", .value = .{ .string_val = "active" } },
        .{ .key = "count", .value = .{ .int_val = 7 } },
    };
    const original = PropertyValue{ .map_val = &entries };

    const eval = EvalResult.fromPropertyValue(original, allocator);
    defer {
        allocator.free(eval.map_val[0].key);
        allocator.free(eval.map_val[0].value.string_val);
        allocator.free(eval.map_val[1].key);
        allocator.free(eval.map_val);
    }

    const roundtrip = eval.toPropertyValue(allocator);
    defer allocator.free(roundtrip.map_val);

    try std.testing.expectEqual(@as(usize, 2), roundtrip.map_val.len);
    try std.testing.expectEqualStrings("status", roundtrip.map_val[0].key);
    try std.testing.expectEqualStrings("active", roundtrip.map_val[0].value.string_val);
    try std.testing.expectEqualStrings("count", roundtrip.map_val[1].key);
    try std.testing.expectEqual(@as(i64, 7), roundtrip.map_val[1].value.int_val);
}

test "list_val and map_val truthiness" {
    const empty_list = EvalResult{ .list_val = &[_]EvalResult{} };
    try std.testing.expect(!empty_list.isTruthy());

    const nonempty_list = EvalResult{ .list_val = &[_]EvalResult{.{ .int_val = 1 }} };
    try std.testing.expect(nonempty_list.isTruthy());

    const empty_vec = EvalResult{ .vector_val = &[_]f32{} };
    try std.testing.expect(!empty_vec.isTruthy());

    const nonempty_vec = EvalResult{ .vector_val = &[_]f32{1.0} };
    try std.testing.expect(nonempty_vec.isTruthy());

    const empty_map = EvalResult{ .map_val = &[_]EvalResult.MapEntry{} };
    try std.testing.expect(!empty_map.isTruthy());

    const map_entries = [_]EvalResult.MapEntry{.{ .key = "a", .value = .{ .int_val = 1 } }};
    const nonempty_map = EvalResult{ .map_val = &map_entries };
    try std.testing.expect(nonempty_map.isTruthy());
}
