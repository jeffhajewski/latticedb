//! Expression evaluator for query execution.
//!
//! Evaluates AST expressions against rows during query execution.
//! Supports literals, variables, property access, binary/unary operations,
//! and function calls.

const std = @import("std");
const regex = @import("regex.zig");
const Allocator = std.mem.Allocator;

const ast = @import("ast.zig");
const executor = @import("executor.zig");

const types = @import("../core/types.zig");
const NodeId = types.NodeId;
const EdgeId = types.EdgeId;
const PropertyValue = types.PropertyValue;
const SymbolId = @import("../graph/symbols.zig").SymbolId;
const NodeRecordCache = @import("../graph/node.zig").RecordCache;

const vector_distance = @import("../vector/distance.zig");
const fts_ops = @import("operators/fts.zig");
const tokenizer = @import("../fts/tokenizer.zig");
const fts_query = @import("../fts/query.zig");

const Row = executor.Row;

/// How many index hits to consider when `@@` is evaluated as a filter rather
/// than planned as a scan.
///
/// The filter only needs to know whether one particular node is among the
/// matches, so this bounds the work rather than expressing a result limit. It
/// matches the bound the index scan uses, because the two have to agree: when
/// they did not, the same predicate returned a different number of rows at the
/// top of a WHERE clause than it did inside an OR.
const FTS_FILTER_LIMIT: u32 = fts_ops.NO_RESULT_LIMIT;
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
    /// Integer arithmetic produced a result outside the 64-bit range
    IntegerOverflow,
    /// range() would produce more than `MAX_RANGE_LENGTH` elements
    RangeTooLarge,
    /// `=~` was given a pattern its engine does not accept
    InvalidRegex,
    /// Reading the database failed
    StorageError,
};

/// The most elements one range() call may produce.
///
/// The list is built in memory, so `range(0, 9223372036854775807)` would
/// otherwise try to allocate until the process ran out. Ten million covers
/// generating test data with UNWIND while bounding one call to a few hundred
/// megabytes.
pub const MAX_RANGE_LENGTH: i128 = 10_000_000;

/// A built-in scalar function: its name, how many arguments it takes and,
/// for error messages, what they are.
pub const FunctionSpec = struct {
    name: []const u8,
    min_args: u8,
    /// `ANY_COUNT` when there is no upper bound.
    max_args: u8,
    /// The arguments it takes, as written in an error message.
    signature: []const u8,

    pub const ANY_COUNT: u8 = 255;
};

/// Every scalar function `evaluateFunction` knows. The analyser reports a
/// call to anything else, or with the wrong number of arguments, before the
/// query runs; a test keeps this list and `evaluateFunction` in step.
/// Aggregates (count, sum, ...) are listed in operators/aggregate.zig.
pub const builtin_functions = [_]FunctionSpec{
    .{ .name = "id", .min_args = 1, .max_args = 1, .signature = "(node or relationship)" },
    .{ .name = "coalesce", .min_args = 1, .max_args = FunctionSpec.ANY_COUNT, .signature = "(any, ...)" },
    .{ .name = "abs", .min_args = 1, .max_args = 1, .signature = "(number)" },
    .{ .name = "size", .min_args = 1, .max_args = 1, .signature = "(string or list)" },
    .{ .name = "toInteger", .min_args = 1, .max_args = 1, .signature = "(any)" },
    .{ .name = "toFloat", .min_args = 1, .max_args = 1, .signature = "(any)" },
    .{ .name = "toString", .min_args = 1, .max_args = 1, .signature = "(any)" },
    .{ .name = "toLower", .min_args = 1, .max_args = 1, .signature = "(string)" },
    .{ .name = "toUpper", .min_args = 1, .max_args = 1, .signature = "(string)" },
    .{ .name = "trim", .min_args = 1, .max_args = 1, .signature = "(string)" },
    .{ .name = "substring", .min_args = 2, .max_args = 3, .signature = "(string, integer[, integer])" },
    .{ .name = "replace", .min_args = 3, .max_args = 3, .signature = "(string, string, string)" },
    .{ .name = "split", .min_args = 2, .max_args = 2, .signature = "(string, string)" },
    .{ .name = "head", .min_args = 1, .max_args = 1, .signature = "(list)" },
    .{ .name = "tail", .min_args = 1, .max_args = 1, .signature = "(list)" },
    .{ .name = "last", .min_args = 1, .max_args = 1, .signature = "(list)" },
    .{ .name = "range", .min_args = 2, .max_args = 3, .signature = "(integer, integer[, integer])" },
    .{ .name = "type", .min_args = 1, .max_args = 1, .signature = "(relationship)" },
    .{ .name = "labels", .min_args = 1, .max_args = 1, .signature = "(node)" },
    .{ .name = "properties", .min_args = 1, .max_args = 1, .signature = "(node, relationship or map)" },
    .{ .name = "score", .min_args = 1, .max_args = 1, .signature = "(a variable a @@ search matched)" },
    .{ .name = "length", .min_args = 1, .max_args = 1, .signature = "(path)" },
    .{ .name = "nodes", .min_args = 1, .max_args = 1, .signature = "(path)" },
    .{ .name = "relationships", .min_args = 1, .max_args = 1, .signature = "(path)" },
    .{ .name = "exists", .min_args = 1, .max_args = 1, .signature = "(any)" },
};

/// The built-in scalar function called `name`, ignoring case.
pub fn findFunction(name: []const u8) ?*const FunctionSpec {
    for (&builtin_functions) |*spec| {
        if (std.ascii.eqlIgnoreCase(spec.name, name)) return spec;
    }
    return null;
}

/// An operator as it is written, for error messages.
fn operatorSymbol(op: ast.BinaryOp) []const u8 {
    return switch (op) {
        .eq => "=",
        .neq => "<>",
        .lt => "<",
        .lte => "<=",
        .gt => ">",
        .gte => ">=",
        .and_ => "AND",
        .or_ => "OR",
        .xor => "XOR",
        .add => "+",
        .sub => "-",
        .mul => "*",
        .div => "/",
        .mod => "%",
        .pow => "^",
        .contains => "CONTAINS",
        .starts_with => "STARTS WITH",
        .ends_with => "ENDS WITH",
        .regex_match => "=~",
        .in_ => "IN",
        .vector_distance => "<=>",
        .fts_match => "@@",
    };
}

/// The name Cypher gives a value's type, for error messages.
pub fn typeName(value: EvalResult) []const u8 {
    return switch (value) {
        .null_val => "null",
        .bool_val => "boolean",
        .int_val => "integer",
        .float_val => "float",
        .string_val => "string",
        .bytes_val => "bytes",
        .node_ref => "node",
        .edge_ref => "relationship",
        .list_val => "list",
        .vector_val => "vector",
        .map_val => "map",
        .path_val => "path",
    };
}

/// A value as an error message shows it: numbers and booleans in full, a
/// string quoted and cut short, anything else by its type.
const Brief = struct {
    value: EvalResult,

    pub fn format(self: Brief, writer: *std.Io.Writer) std.Io.Writer.Error!void {
        switch (self.value) {
            .int_val => |i| try writer.print("{d}", .{i}),
            .float_val => |f| try writeFloat(writer, f),
            .bool_val => |b| try writer.writeAll(if (b) "true" else "false"),
            .null_val => try writer.writeAll("null"),
            .string_val => |s| {
                const max = 40;
                if (s.len <= max) {
                    try writer.print("'{s}'", .{s});
                } else {
                    // Cut on a character boundary.
                    var end: usize = max;
                    while (end > 0 and (s[end] & 0xC0) == 0x80) end -= 1;
                    try writer.print("'{s}...'", .{s[0..end]});
                }
            },
            else => |v| try writer.print("a {s}", .{typeName(v)}),
        }
    }
};

/// A float in a message: plain decimal at ordinary magnitudes, scientific
/// notation where decimal would run to hundreds of digits.
pub fn writeFloat(writer: *std.Io.Writer, f: f64) std.Io.Writer.Error!void {
    const magnitude = @abs(f);
    if (std.math.isFinite(f) and f != 0 and (magnitude >= 1e16 or magnitude < 1e-6)) {
        try writer.print("{e}", .{f});
    } else {
        try writer.print("{d}", .{f});
        // A whole number keeps a ".0", so 2.0 reads as a float, not as 2.
        if (std.math.isFinite(f) and @trunc(f) == f) try writer.writeAll(".0");
    }
}

/// The types of a list of values, as "(string, integer)".
const TypeList = struct {
    values: []const EvalResult,

    pub fn format(self: TypeList, writer: *std.Io.Writer) std.Io.Writer.Error!void {
        try writer.writeByte('(');
        for (self.values, 0..) |value, i| {
            if (i > 0) try writer.writeAll(", ");
            try writer.writeAll(typeName(value));
        }
        try writer.writeByte(')');
    }
};

/// A list of values, as "(1, 'a')".
const ValueList = struct {
    values: []const EvalResult,

    pub fn format(self: ValueList, writer: *std.Io.Writer) std.Io.Writer.Error!void {
        try writer.writeByte('(');
        for (self.values, 0..) |value, i| {
            if (i > 0) try writer.writeAll(", ");
            try (Brief{ .value = value }).format(writer);
        }
        try writer.writeByte(')');
    }
};

/// A float as an i64, truncated toward zero, or null when it is NaN, infinite
/// or outside the i64 range. `@intFromFloat` is undefined behaviour for those
/// in a release build.
pub fn floatToInt(f: f64) ?i64 {
    if (std.math.isNan(f)) return null;
    // 2^63 is exactly representable; every float below it truncates into
    // range, and -2^63 itself is minInt(i64).
    const limit: f64 = 9223372036854775808.0;
    if (f >= limit or f < -limit) return null;
    return @intFromFloat(f);
}

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
    /// Bytes result (borrowed, valid for query lifetime)
    bytes_val: []const u8,
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
    /// A path, in memory that lasts as long as the query.
    path_val: *const executor.Path,

    pub const MapEntry = struct {
        key: []const u8,
        value: EvalResult,
    };

    const Self = @This();

    /// Check if result is null
    pub fn isNull(self: Self) bool {
        return self == .null_val;
    }

    /// Whether this is a list or map with a node, relationship or path in
    /// it, at any depth: what converting it to a property value would lose.
    pub fn holdsReferences(self: Self) bool {
        return switch (self) {
            .list_val => |l| for (l) |item| {
                if (item.isReference() or item.holdsReferences()) break true;
            } else false,
            .map_val => |m| for (m) |entry| {
                if (entry.value.isReference() or entry.value.holdsReferences()) break true;
            } else false,
            else => false,
        };
    }

    fn isReference(self: Self) bool {
        return self == .node_ref or self == .edge_ref or self == .path_val;
    }

    /// The slot value for a list or map: a property value, unless it holds
    /// nodes, relationships or paths, which are kept as they are.
    pub fn containerToSlotValue(self: Self, allocator: Allocator) Allocator.Error!SlotValue {
        if (self.holdsReferences()) {
            const copy = try allocator.create(EvalResult);
            copy.* = try self.cloneInto(allocator);
            return .{ .value = copy };
        }
        return .{ .property = try self.toPropertyValue(allocator) };
    }

    /// A copy whose lists, maps and strings are in `allocator`, so that it
    /// outlives whatever produced it, as `toPropertyValue` does for values
    /// without references.
    fn cloneInto(self: Self, allocator: Allocator) error{OutOfMemory}!EvalResult {
        return switch (self) {
            .string_val => |v| .{ .string_val = try allocator.dupe(u8, v) },
            .bytes_val => |v| .{ .bytes_val = try allocator.dupe(u8, v) },
            .vector_val => |v| .{ .vector_val = try allocator.dupe(f32, v) },
            .list_val => |l| blk: {
                const out = try allocator.alloc(EvalResult, l.len);
                for (l, out) |item, *o| o.* = try item.cloneInto(allocator);
                break :blk .{ .list_val = out };
            },
            .map_val => |m| blk: {
                const out = try allocator.alloc(MapEntry, m.len);
                for (m, out) |entry, *o| o.* = .{ .key = try allocator.dupe(u8, entry.key), .value = try entry.value.cloneInto(allocator) };
                break :blk .{ .map_val = out };
            },
            else => self,
        };
    }

    /// Check if result is truthy.
    ///
    /// Two-valued: null counts as false here. That is the right reading for a
    /// WHERE clause, which keeps a row only when its predicate is true, but not
    /// inside an expression, where `NOT null` has to stay null. Logical
    /// operators use `truthValue` instead.
    pub fn isTruthy(self: Self) bool {
        return switch (self) {
            .null_val => false,
            .bool_val => |b| b,
            .int_val => |i| i != 0,
            .float_val => |f| f != 0.0,
            .string_val => |s| s.len > 0,
            .bytes_val => |b| b.len > 0,
            .node_ref => true,
            .edge_ref => true,
            .list_val => |l| l.len > 0,
            .vector_val => |v| v.len > 0,
            .map_val => |m| m.len > 0,
            .path_val => true,
        };
    }

    /// Convert to boolean
    pub fn toBool(self: Self) ?bool {
        return switch (self) {
            .bool_val => |b| b,
            else => null,
        };
    }

    /// Convert to integer. Null for non-numbers, and for floats that are NaN,
    /// infinite or outside the i64 range.
    pub fn toInt(self: Self) ?i64 {
        return switch (self) {
            .int_val => |i| i,
            .float_val => |f| floatToInt(f),
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
    ///
    /// Running out of memory is an error. It used to produce a null, so a
    /// query that ran short of memory returned a plausible wrong answer.
    pub fn fromPropertyValue(pv: PropertyValue, allocator: Allocator) Allocator.Error!Self {
        return switch (pv) {
            .null_val => .{ .null_val = {} },
            .bool_val => |b| .{ .bool_val = b },
            .int_val => |i| .{ .int_val = i },
            .float_val => |f| .{ .float_val = f },
            .string_val => |s| .{ .string_val = try allocator.dupe(u8, s) },
            .bytes_val => |b| .{ .bytes_val = try allocator.dupe(u8, b) },
            .vector_val => |v| .{ .vector_val = try allocator.dupe(f32, v) },
            .list_val => |list| blk: {
                const results = try allocator.alloc(EvalResult, list.len);
                for (list, 0..) |item, i| {
                    results[i] = try fromPropertyValue(item, allocator);
                }
                break :blk .{ .list_val = results };
            },
            .map_val => |map| blk: {
                const entries = try allocator.alloc(MapEntry, map.len);
                for (map, 0..) |entry, i| {
                    entries[i] = .{
                        .key = try allocator.dupe(u8, entry.key),
                        .value = try fromPropertyValue(entry.value, allocator),
                    };
                }
                break :blk .{ .map_val = entries };
            },
        };
    }

    /// Convert to PropertyValue (allocator needed for list/map conversion).
    /// Running out of memory is an error, not a null.
    pub fn toPropertyValue(self: Self, allocator: Allocator) Allocator.Error!PropertyValue {
        return switch (self) {
            .null_val => .{ .null_val = {} },
            .bool_val => |b| .{ .bool_val = b },
            .int_val => |i| .{ .int_val = i },
            .float_val => |f| .{ .float_val = f },
            .string_val => |s| .{ .string_val = s },
            .bytes_val => |b| .{ .bytes_val = b },
            .node_ref => |id| .{ .int_val = @intCast(id) },
            .edge_ref => |id| .{ .int_val = @intCast(id) },
            .vector_val => |v| .{ .vector_val = v },
            .list_val => |list| blk: {
                const props = try allocator.alloc(PropertyValue, list.len);
                for (list, 0..) |item, i| {
                    props[i] = try item.toPropertyValue(allocator);
                }
                break :blk .{ .list_val = props };
            },
            .map_val => |map| blk: {
                const entries = try allocator.alloc(PropertyValue.MapEntry, map.len);
                for (map, 0..) |entry, i| {
                    entries[i] = .{
                        .key = entry.key,
                        .value = try entry.value.toPropertyValue(allocator),
                    };
                }
                break :blk .{ .map_val = entries };
            },
            // A property cannot hold a path, or the nodes and relationships
            // in it, so a path inside a list or map is kept as their ids.
            .path_val => |path| try pathToPropertyValue(path.*, allocator),
        };
    }

    /// A path as a property value: `{nodes: [ids], relationships: [ids]}`.
    pub fn pathToPropertyValue(path: executor.Path, allocator: Allocator) Allocator.Error!PropertyValue {
        const nodes = try allocator.alloc(PropertyValue, path.nodes.len);
        for (path.nodes, nodes) |id, *n| n.* = .{ .int_val = @intCast(id) };
        const edges = try allocator.alloc(PropertyValue, path.edges.len);
        for (path.edges, edges) |id, *e| e.* = .{ .int_val = @intCast(id) };
        const entries = try allocator.alloc(PropertyValue.MapEntry, 2);
        entries[0] = .{ .key = "nodes", .value = .{ .list_val = nodes } };
        entries[1] = .{ .key = "relationships", .value = .{ .list_val = edges } };
        return .{ .map_val = entries };
    }
};

/// Turn an overflowing integer operation into a query error.
///
/// Plain `+`, `-` and `*` on i64 are undefined behaviour on overflow in the
/// ReleaseFast builds that ship, so integer arithmetic goes through the
/// checked `std.math` functions and lands here.
fn checkedInt(result: anytype) EvalError!i64 {
    return result catch |err| {
        if (err == error.Overflow) return EvalError.IntegerOverflow;
        // Only divTrunc can fail otherwise, on a zero divisor, and callers
        // reject that before dividing.
        return EvalError.DivisionByZero;
    };
}

/// The bytes of the character starting at `s[i]`: its UTF-8 length, or 1
/// for a byte that does not start a valid sequence, which string functions
/// then treat as a character of its own.
fn codepointLen(s: []const u8, i: usize) usize {
    const c = s[i];
    if (c < 0x80) return 1;
    const n = std.unicode.utf8ByteSequenceLength(c) catch return 1;
    if (i + n > s.len) return 1;
    _ = std.unicode.utf8Decode(s[i .. i + n]) catch return 1;
    return n;
}

/// How many characters `s` holds, by `codepointLen`.
fn codepointCount(s: []const u8) usize {
    // The library count is fast on ASCII, but refuses invalid UTF-8.
    if (std.unicode.utf8CountCodepoints(s)) |n| return n else |_| {}
    var count: usize = 0;
    var i: usize = 0;
    while (i < s.len) : (count += 1) i += codepointLen(s, i);
    return count;
}

/// The byte offset `count` characters after byte offset `from`, or the end
/// of `s` when it has fewer.
fn advanceCodepoints(s: []const u8, from: usize, count: usize) usize {
    var i = from;
    var left = count;
    while (left > 0 and i < s.len) : (left -= 1) {
        i += if (s[i] < 0x80) 1 else codepointLen(s, i);
    }
    return i;
}

/// How an integer compares with a float, exactly: by the numbers they are,
/// not by the integer rounded to a float, so 2^53 + 1 is greater than
/// 2^53.0 although converting it to a float makes them equal. Null when the
/// float is NaN, which is not less than, equal to or greater than anything.
pub fn compareIntFloat(i: i64, f: f64) ?std.math.Order {
    if (std.math.isNan(f)) return null;
    const limit: f64 = 9223372036854775808.0; // 2^63
    if (f >= limit) return .lt;
    if (f < -limit) return .gt;
    // In range, the whole part of the float is an i64 exactly.
    const whole = @trunc(f);
    const t: i64 = @intFromFloat(whole);
    if (i != t) return std.math.order(i, t);
    const fraction = f - whole;
    return if (fraction > 0) .lt else if (fraction < 0) .gt else .eq;
}

/// A truth value in Cypher's three-valued logic.
const TruthValue = enum { false_, true_, unknown };

/// Read a value as a truth value for NOT, AND and OR.
///
/// Null is unknown. Anything else keeps the truthiness it has always had, so
/// only the treatment of null changes here.
fn truthValue(value: EvalResult) TruthValue {
    if (value.isNull()) return .unknown;
    return if (value.isTruthy()) .true_ else .false_;
}

/// A failed database read as an evaluation error. Reads used to turn these
/// into null, which made a damaged page look like a missing property.
fn storageError(err: anyerror) EvalError {
    return if (err == error.OutOfMemory) EvalError.OutOfMemory else EvalError.StorageError;
}

/// `storageError`, recorded with what was being read, so the caller can
/// tell a damaged database from a fault in the query.
fn readFailure(
    ctx: *const ExecutionContext,
    err: anyerror,
    location: ?ast.SourceLocation,
    comptime what: []const u8,
    args: anytype,
) EvalError {
    @branchHint(.cold);
    const mapped = storageError(err);
    if (ctx.diagnostic) |d| {
        if (mapped == EvalError.OutOfMemory) {
            d.record(.out_of_memory, "OutOfMemory", location, "Out of memory while " ++ what, args);
        } else {
            d.record(.storage, "StorageError", location, "Storage error while " ++ what ++ ": {s}", args ++ .{@errorName(err)});
        }
    }
    return mapped;
}

/// An EvalResult taking over a value decoded into `allocator`. Scalars and
/// strings are used in place; lists and maps are converted, which copies
/// them, and the decoded original is freed.
fn evalResultFromOwned(value: PropertyValue, allocator: Allocator) Allocator.Error!EvalResult {
    switch (value) {
        .string_val => |v| return .{ .string_val = v },
        .bytes_val => |v| return .{ .bytes_val = v },
        .vector_val => |v| return .{ .vector_val = v },
        .list_val, .map_val => {
            var owned = value;
            defer owned.deinit(allocator);
            return EvalResult.fromPropertyValue(value, allocator);
        },
        else => return EvalResult.fromPropertyValue(value, allocator),
    }
}

// ============================================================================
// Expression Evaluator
// ============================================================================

/// Evaluator for AST expressions
pub const ExpressionEvaluator = struct {
    /// Allocator for evaluation temporaries
    allocator: Allocator,

    /// Property keys this evaluator has already resolved to symbol ids.
    ///
    /// Resolving a name is a B-tree lookup plus an allocation, and an
    /// operator evaluates the same few `n.prop` expressions on every row.
    /// Only keys that exist are remembered: a key that is missing now can be
    /// created by a SET later in the same query, while an id, once given
    /// out, keeps its name.
    key_cache: [KEY_CACHE_SIZE]KeyCacheEntry = undefined,
    key_cache_len: u8 = 0,
    key_cache_next: u8 = 0,

    /// The record of the node whose property was read last, so that
    /// `n.a, n.b, n.c` on one row finds `n` in the B-tree once. Used only
    /// when the query does not write (`ExecutionContext.stable_reads`).
    node_record: NodeRecordCache = .{},
    /// The same for the relationship whose property was read last.
    edge_record: NodeRecordCache = .{},

    /// Variables bound by the comprehension or reduce() being evaluated,
    /// innermost last. They live here rather than in the row, so they take
    /// no slots; a name here hides the query's variable of that name.
    locals: [MAX_LOCALS]Local = undefined,
    locals_len: u8 = 0,

    const MAX_LOCALS = 16;
    const Local = struct { name: []const u8, value: EvalResult };

    const KEY_CACHE_SIZE = 8;
    const KeyCacheEntry = struct { name: []const u8, id: SymbolId };

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
            .function_call => |f| self.evaluateFunction(f, row, ctx) catch |err| self.functionFailure(f, err, row, ctx),
            .list => |l| self.evaluateList(l, row, ctx),
            .map => |m| self.evaluateMap(m, row, ctx),
            .index => |i| self.evaluateIndex(i, row, ctx),
            .slice => |s| self.evaluateSlice(s, row, ctx),
            .case_expr => |c| self.evaluateCase(c, row, ctx),
            .comprehension => |c| self.evaluateComprehension(c, row, ctx),
            .reduce => |r| self.evaluateReduce(r, row, ctx),
            .subquery => |sq| self.evaluateSubquery(sq, row, ctx),
        };
    }

    /// Run a pattern subquery's plan for this row: whether it matches, how
    /// many times, or the list of its value per match.
    fn evaluateSubquery(
        self: *Self,
        sq: *const ast.PatternSubquery,
        row: *const Row,
        ctx: *const ExecutionContext,
    ) EvalError!EvalResult {
        const host = ctx.subqueries orelse
            return ctx.fail(EvalError.InvalidOperation, sq.location, "This subquery was not planned, so it cannot run here", .{});
        const plan = host.find(sq) orelse
            return ctx.fail(EvalError.InvalidOperation, sq.location, "This subquery was not planned, so it cannot run here", .{});
        const Visitor = struct {
            evaluator: *Self,
            node: *const ast.PatternSubquery,
            ctx: *const ExecutionContext,
            count: i64 = 0,
            values: std.ArrayListUnmanaged(EvalResult) = .empty,
            failed: ?EvalError = null,

            pub fn visit(v: *@This(), match: *Row) executor.OperatorError!void {
                v.count += 1;
                const map = v.node.map orelse return;
                const value = v.evaluator.evaluate(map, match, v.ctx) catch |err| {
                    v.failed = err;
                    return executor.OperatorError.EvaluationError;
                };
                v.values.append(v.evaluator.allocator, value) catch return executor.OperatorError.OutOfMemory;
            }
        };
        var visitor = Visitor{ .evaluator = self, .node = sq, .ctx = ctx };
        // Operators take the context they run in as mutable; evaluation is
        // handed it as const only because expressions do not change it.
        plan.run(@constCast(ctx), row, &visitor) catch |err| {
            if (visitor.failed) |failed| return failed;
            const mapped: EvalError = switch (err) {
                executor.OperatorError.OutOfMemory => EvalError.OutOfMemory,
                executor.OperatorError.TypeError => EvalError.TypeError,
                executor.OperatorError.StorageError => EvalError.StorageError,
                else => EvalError.InvalidOperation,
            };
            // The operator that failed has usually said why already.
            if (ctx.failureRecorded()) return mapped;
            if (plan.running) return ctx.fail(mapped, sq.location, "A subquery cannot run inside itself", .{});
            return ctx.fail(mapped, sq.location, "The subquery failed: {s}", .{@errorName(err)});
        };
        return switch (sq.kind) {
            .exists => .{ .bool_val = visitor.count > 0 },
            .count => .{ .int_val = visitor.count },
            .comprehension => .{ .list_val = visitor.values.toOwnedSlice(self.allocator) catch return EvalError.OutOfMemory },
        };
    }

    /// Describe a failed function call, unless the failure was already
    /// described where it happened (in an argument, say).
    ///
    /// Only reached on failure. The arguments are evaluated again to say
    /// what the call was given; the first evaluation's values are gone.
    fn functionFailure(
        self: *Self,
        f: *const ast.FunctionCall,
        err: EvalError,
        row: *const Row,
        ctx: *const ExecutionContext,
    ) EvalError {
        @branchHint(.cold);
        if (ctx.failureRecorded()) return err;
        const loc = f.location;
        const spec = findFunction(f.name) orelse
            return ctx.fail(err, loc, "Unknown function '{s}'", .{f.name});

        var values: [8]EvalResult = undefined;
        var count: usize = 0;
        for (f.arguments[0..@min(f.arguments.len, values.len)]) |arg| {
            values[count] = self.evaluate(arg, row, ctx) catch break;
            count += 1;
        }
        // Evaluating them again may itself have failed and said why.
        if (ctx.failureRecorded()) return err;
        const args = values[0..count];

        return switch (err) {
            EvalError.InvalidArgumentCount => ctx.fail(err, loc, "{s} takes {s}, but was given {d} argument{s}", .{ spec.name, spec.signature, f.arguments.len, if (f.arguments.len == 1) "" else "s" }),
            EvalError.TypeError => ctx.fail(err, loc, "Type error: {s}{f} is not defined; {s} takes {s}", .{ spec.name, TypeList{ .values = args }, spec.name, spec.signature }),
            EvalError.IntegerOverflow => ctx.fail(err, loc, "Integer overflow: {s}{f} does not fit in a 64-bit integer", .{ spec.name, ValueList{ .values = args } }),
            EvalError.RangeTooLarge => ctx.fail(err, loc, "{s}{f} would produce more than {d} elements", .{ spec.name, ValueList{ .values = args }, MAX_RANGE_LENGTH }),
            EvalError.InvalidOperation => ctx.fail(err, loc, "{s}{f} is not defined: the step must not be 0", .{ spec.name, ValueList{ .values = args } }),
            else => ctx.fail(err, loc, "{s}{f} failed: {s}", .{ spec.name, ValueList{ .values = args }, @errorName(err) }),
        };
    }

    /// Describe a failed operator, with the values it was applied to.
    fn binaryFailure(
        _: *Self,
        b: *const ast.BinaryExpr,
        err: EvalError,
        left: EvalResult,
        right: EvalResult,
        ctx: *const ExecutionContext,
    ) EvalError {
        @branchHint(.cold);
        if (ctx.failureRecorded()) return err;
        const symbol = operatorSymbol(b.operator);
        const l = Brief{ .value = left };
        const r = Brief{ .value = right };
        return switch (err) {
            EvalError.DivisionByZero => ctx.fail(err, b.location, "Division by zero: {f} {s} {f}", .{ l, symbol, r }),
            EvalError.IntegerOverflow => ctx.fail(err, b.location, "Integer overflow: {f} {s} {f} does not fit in a 64-bit integer", .{ l, symbol, r }),
            EvalError.TypeError => ctx.fail(err, b.location, "Type error: {s} {s} {s} is not defined", .{ typeName(left), symbol, typeName(right) }),
            else => ctx.fail(err, b.location, "{f} {s} {f} failed: {s}", .{ l, symbol, r, @errorName(err) }),
        };
    }

    /// Bind a comprehension's variable. Paired with `popLocal`.
    fn pushLocal(
        self: *Self,
        name: []const u8,
        value: EvalResult,
        ctx: *const ExecutionContext,
        location: ast.SourceLocation,
    ) EvalError!void {
        if (self.locals_len == MAX_LOCALS) {
            return ctx.fail(EvalError.InvalidOperation, location, "List comprehensions, quantifiers and reduce() can be nested at most {d} variables deep", .{MAX_LOCALS});
        }
        self.locals[self.locals_len] = .{ .name = name, .value = value };
        self.locals_len += 1;
    }

    fn popLocal(self: *Self) void {
        self.locals_len -= 1;
    }

    /// The list a comprehension, quantifier or reduce() iterates over, or
    /// null when it is null.
    fn iteratedList(self: *Self, list_expr: *const ast.Expression, row: *const Row, ctx: *const ExecutionContext) EvalError!?[]const EvalResult {
        const value = try self.evaluate(list_expr, row, ctx);
        return switch (value) {
            .list_val => |l| l,
            .null_val => null,
            else => ctx.fail(EvalError.TypeError, list_expr.getLocation(), "Type error: `x IN {s}` is not defined; a comprehension, quantifier or reduce() iterates over a list", .{typeName(value)}),
        };
    }

    /// `[x IN list WHERE cond | value]`, and the quantifiers over the same
    /// shape. An element is kept when its condition is true (null is not).
    /// The quantifiers follow openCypher's three-valued logic: a null
    /// condition is unknown, so `all` over [true, null] is null, while `any`
    /// over [true, null] is true. A null list gives null.
    fn evaluateComprehension(
        self: *Self,
        c: *const ast.ListComprehension,
        row: *const Row,
        ctx: *const ExecutionContext,
    ) EvalError!EvalResult {
        const list = try self.iteratedList(c.list, row, ctx) orelse return .{ .null_val = {} };
        var kept: std.ArrayListUnmanaged(EvalResult) = .empty;
        var trues: usize = 0;
        var unknowns: usize = 0;
        for (list) |element| {
            try self.pushLocal(c.variable, element, ctx, c.location);
            defer self.popLocal();
            const truth: TruthValue = if (c.where) |w| truthValue(try self.evaluate(w, row, ctx)) else .true_;
            switch (truth) {
                .true_ => trues += 1,
                .unknown => unknowns += 1,
                .false_ => {},
            }
            switch (c.kind) {
                .list => if (truth == .true_) {
                    const value = if (c.map) |m| try self.evaluate(m, row, ctx) else element;
                    kept.append(self.allocator, value) catch return EvalError.OutOfMemory;
                },
                // Settled as soon as one element decides it.
                .any => if (truth == .true_) return .{ .bool_val = true },
                .none => if (truth == .true_) return .{ .bool_val = false },
                .all => if (truth == .false_) return .{ .bool_val = false },
                .single => if (trues > 1) return .{ .bool_val = false },
            }
        }
        return switch (c.kind) {
            .list => .{ .list_val = kept.toOwnedSlice(self.allocator) catch return EvalError.OutOfMemory },
            .any => if (unknowns > 0) .{ .null_val = {} } else .{ .bool_val = false },
            .none => if (unknowns > 0) .{ .null_val = {} } else .{ .bool_val = true },
            .all => if (unknowns > 0) .{ .null_val = {} } else .{ .bool_val = true },
            .single => if (unknowns > 0) .{ .null_val = {} } else .{ .bool_val = trues == 1 },
        };
    }

    /// `reduce(acc = initial, x IN list | expression)`. A null list gives
    /// null; an empty one gives `initial`.
    fn evaluateReduce(
        self: *Self,
        r: *const ast.ReduceExpr,
        row: *const Row,
        ctx: *const ExecutionContext,
    ) EvalError!EvalResult {
        var acc = try self.evaluate(r.initial, row, ctx);
        const list = try self.iteratedList(r.list, row, ctx) orelse return .{ .null_val = {} };
        for (list) |element| {
            try self.pushLocal(r.accumulator, acc, ctx, r.location);
            defer self.popLocal();
            try self.pushLocal(r.variable, element, ctx, r.location);
            defer self.popLocal();
            acc = try self.evaluate(r.expression, row, ctx);
        }
        return acc;
    }

    /// CASE picks the first branch that applies and evaluates only that
    /// branch's value, so a branch can guard against an error in another
    /// (`CASE WHEN x = 0 THEN 0 ELSE 1 / x END`).
    ///
    /// The searched form applies a branch when its condition is true; a null
    /// condition is not true. The simple form applies a branch when the
    /// subject `=` its value is true, so a null subject or a null WHEN value
    /// never matches, not even null. With no branch applying, the value is
    /// the ELSE, or null without one.
    fn evaluateCase(
        self: *Self,
        c: *const ast.CaseExpr,
        row: *const Row,
        ctx: *const ExecutionContext,
    ) EvalError!EvalResult {
        if (c.subject) |subject_expr| {
            const subject = try self.evaluate(subject_expr, row, ctx);
            for (c.branches) |branch| {
                if (subject.isNull()) break;
                const candidate = try self.evaluate(branch.when, row, ctx);
                if (candidate.isNull()) continue;
                const equal = self.compareEqual(subject, candidate).toBool() orelse false;
                if (equal) return self.evaluate(branch.then, row, ctx);
            }
        } else {
            for (c.branches) |branch| {
                const condition = try self.evaluate(branch.when, row, ctx);
                if (truthValue(condition) == .true_) return self.evaluate(branch.then, row, ctx);
            }
        }
        if (c.else_expr) |else_expr| return self.evaluate(else_expr, row, ctx);
        return .{ .null_val = {} };
    }

    /// `object[index]`.
    ///
    /// A string index reads a map entry, node property or relationship
    /// property by name, exactly as `object.name` would, so `n['age']` and
    /// `n.age` cannot disagree. An integer index reads a list element: from
    /// the front when it is 0 or more, from the back when it is negative
    /// (`[-1]` is the last). An index outside the list gives null, as does a
    /// null list or a null index. Any other combination is a type error.
    fn evaluateIndex(
        self: *Self,
        ix: *const ast.IndexExpr,
        row: *const Row,
        ctx: *const ExecutionContext,
    ) EvalError!EvalResult {
        const index = try self.evaluate(ix.index, row, ctx);
        switch (index) {
            .null_val => return .{ .null_val = {} },
            .string_val => |key| {
                const object = try self.evaluate(ix.object, row, ctx);
                switch (object) {
                    .null_val => return .{ .null_val = {} },
                    .map_val => |entries| {
                        for (entries) |entry| {
                            if (std.mem.eql(u8, entry.key, key)) return entry.value;
                        }
                        return .{ .null_val = {} };
                    },
                    // Read through the property access itself, which looks
                    // the object up again: a node or relationship is a
                    // variable or something as cheap in practice.
                    .node_ref, .edge_ref => {
                        const access = ast.PropertyAccess{ .object = ix.object, .property = key, .location = ix.location };
                        return self.evaluatePropertyAccess(&access, row, ctx);
                    },
                    else => return ctx.fail(EvalError.TypeError, ix.location, "Type error: {s}[string] is not defined; a string key reads a map, node or relationship", .{typeName(object)}),
                }
            },
            .int_val => |i| {
                const object = try self.evaluate(ix.object, row, ctx);
                const items = switch (object) {
                    .null_val => return .{ .null_val = {} },
                    .list_val => |l| l,
                    else => return ctx.fail(EvalError.TypeError, ix.location, "Type error: {s}[integer] is not defined; an integer index reads a list", .{typeName(object)}),
                };
                const pos = listPosition(i, items.len) orelse return .{ .null_val = {} };
                if (pos >= items.len) return .{ .null_val = {} };
                return items[pos];
            },
            else => return ctx.fail(EvalError.TypeError, ix.location, "Type error: an index must be an integer or a string, but this is {f}", .{Brief{ .value = index }}),
        }
    }

    /// `list[from..to]`: the elements from `from` up to but not including
    /// `to`. Negative bounds count from the end, bounds past either end are
    /// clamped to it, a missing bound means that end of the list, and a
    /// range that ends before it starts is empty. A null list or a null
    /// bound gives null.
    fn evaluateSlice(
        self: *Self,
        sl: *const ast.SliceExpr,
        row: *const Row,
        ctx: *const ExecutionContext,
    ) EvalError!EvalResult {
        const object = try self.evaluate(sl.object, row, ctx);
        const items = switch (object) {
            .null_val => return .{ .null_val = {} },
            .list_val => |l| l,
            else => return ctx.fail(EvalError.TypeError, sl.location, "Type error: {s}[..] is not defined; a range of positions reads a list", .{typeName(object)}),
        };
        const from = try self.sliceBound(sl.from, row, ctx, 0, items.len) orelse return .{ .null_val = {} };
        const to = try self.sliceBound(sl.to, row, ctx, items.len, items.len) orelse return .{ .null_val = {} };
        if (to <= from) return .{ .list_val = &[_]EvalResult{} };
        return .{ .list_val = items[from..to] };
    }

    /// One bound of a slice as a position in `0..len`, or null when the bound
    /// evaluates to null.
    fn sliceBound(
        self: *Self,
        bound: ?*const ast.Expression,
        row: *const Row,
        ctx: *const ExecutionContext,
        default: usize,
        len: usize,
    ) EvalError!?usize {
        const expr = bound orelse return default;
        const value = try self.evaluate(expr, row, ctx);
        const i = switch (value) {
            .null_val => return null,
            .int_val => |i| i,
            else => return ctx.fail(EvalError.TypeError, expr.getLocation(), "Type error: the bounds of a list range must be integers, but this is {f}", .{Brief{ .value = value }}),
        };
        return @min(listPosition(i, len) orelse 0, len);
    }

    /// A list index as a position from the front: `i` itself when it is 0 or
    /// more, `len + i` when it is negative. Null when a negative index
    /// reaches before the start.
    fn listPosition(i: i64, len: usize) ?usize {
        if (i >= 0) return std.math.cast(usize, i) orelse std.math.maxInt(usize);
        // -(i + 1) cannot overflow, even for the smallest i64.
        const back = (std.math.cast(usize, -(i + 1)) orelse return null) + 1;
        if (back > len) return null;
        return len - back;
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
        // A comprehension's own variable first: it hides the query's.
        var i = self.locals_len;
        while (i > 0) {
            i -= 1;
            if (std.mem.eql(u8, self.locals[i].name, v.name)) return self.locals[i].value;
        }

        // Look up the slot for this variable
        const slot = ctx.getVariableSlot(v.name) orelse
            return ctx.fail(EvalError.UnboundVariable, v.location, "Variable `{s}` is not defined here", .{v.name});

        // Get the value from the row
        const slot_val = row.getSlot(slot) orelse
            return ctx.fail(EvalError.EmptySlot, v.location, "Variable `{s}` has no value here", .{v.name});

        return switch (slot_val) {
            .empty => ctx.fail(EvalError.EmptySlot, v.location, "Variable `{s}` has no value here", .{v.name}),
            .node_ref => |id| .{ .node_ref = id },
            .edge_ref => |id| .{ .edge_ref = id },
            .property => |pv| try EvalResult.fromPropertyValue(pv, self.allocator),
            .path => |p| .{ .path_val = p },
            .value => |held| held.*,
        };
    }

    /// Evaluate a parameter reference
    fn evaluateParameter(
        self: *Self,
        p: ast.ParameterRef,
        ctx: *const ExecutionContext,
    ) EvalError!EvalResult {
        const pv = ctx.getParameter(p.name) orelse
            return ctx.fail(EvalError.ParameterNotFound, p.location, "Parameter ${s} was not given a value", .{p.name});
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

            // Special pseudo-property for vector embeddings stored outside node properties.
            if (std.mem.eql(u8, pa.property, "embedding")) {
                if (ctx.database) |database| {
                    const found = database.getNodeVectorInTxn(ctx.txn, node_id) catch |err|
                        return readFailure(ctx, err, pa.location, "reading the vector of node {d}", .{node_id});
                    if (found) |vector| {
                        // The database's copy was never freed, leaking one
                        // vector per row; evaluation temporaries live in
                        // the evaluator's allocator.
                        defer database.allocator.free(vector);
                        const owned = self.allocator.dupe(f32, vector) catch return EvalError.OutOfMemory;
                        return .{ .vector_val = owned };
                    }
                }
            }

            const database = ctx.database orelse return .{ .null_val = {} };
            const symbol_table = ctx.symbol_table orelse return .{ .null_val = {} };

            // A key no node has ever used cannot be set on this one.
            const key_id = (self.propertyKeyId(symbol_table, pa.property) catch |err|
                return readFailure(ctx, err, pa.location, "looking up property key '{s}'", .{pa.property})) orelse return .{ .null_val = {} };

            // Decode just this property, straight into the evaluator's
            // allocator. A node that no longer exists has no properties.
            const record_cache = if (ctx.stable_reads) &self.node_record else null;
            const found = database.getNodePropertyByKeyInTxn(ctx.txn, node_id, key_id, self.allocator, record_cache) catch |err|
                return readFailure(ctx, err, pa.location, "reading property '{s}' of node {d}", .{ pa.property, node_id });
            const value = found orelse return .{ .null_val = {} };
            return evalResultFromOwned(value, self.allocator);
        }

        // Handle edge property access
        if (obj == .edge_ref) {
            const edge_id = obj.edge_ref;

            const database = ctx.database orelse return .{ .null_val = {} };
            const symbol_table = ctx.symbol_table orelse return .{ .null_val = {} };

            // As for a node: resolve the key once, decode only its value.
            const key_id = (self.propertyKeyId(symbol_table, pa.property) catch |err|
                return readFailure(ctx, err, pa.location, "looking up property key '{s}'", .{pa.property})) orelse return .{ .null_val = {} };
            const record_cache = if (ctx.stable_reads) &self.edge_record else null;
            const found = database.getEdgePropertyByKeyInTxn(ctx.txn, edge_id, key_id, self.allocator, record_cache) catch |err|
                return readFailure(ctx, err, pa.location, "reading property '{s}' of relationship {d}", .{ pa.property, edge_id });
            const value = found orelse return .{ .null_val = {} };
            return evalResultFromOwned(value, self.allocator);
        }

        // A map's entry by key. A key the map does not have reads as null,
        // like a property a node does not have.
        if (obj == .map_val) {
            for (obj.map_val) |entry| {
                if (std.mem.eql(u8, entry.key, pa.property)) return entry.value;
            }
            return .{ .null_val = {} };
        }

        // Unknown object type
        return .{ .null_val = {} };
    }

    /// The symbol id of property key `name`, or null when no such key exists.
    fn propertyKeyId(self: *Self, symbol_table: anytype, name: []const u8) EvalError!?SymbolId {
        for (self.key_cache[0..self.key_cache_len]) |entry| {
            if (std.mem.eql(u8, entry.name, name)) return entry.id;
        }
        const id = symbol_table.lookup(name) catch |err| return switch (err) {
            error.NotFound => null,
            error.OutOfMemory => EvalError.OutOfMemory,
            else => EvalError.StorageError,
        };
        if (self.key_cache_len < KEY_CACHE_SIZE) {
            self.key_cache[self.key_cache_len] = .{ .name = name, .id = id };
            self.key_cache_len += 1;
        } else {
            self.key_cache[self.key_cache_next] = .{ .name = name, .id = id };
            self.key_cache_next = (self.key_cache_next + 1) % KEY_CACHE_SIZE;
        }
        return id;
    }

    /// Evaluate a binary expression
    fn evaluateBinary(
        self: *Self,
        b: *const ast.BinaryExpr,
        row: *const Row,
        ctx: *const ExecutionContext,
    ) EvalError!EvalResult {
        // Logical operators use Kleene's three-valued logic, where null stands
        // for "unknown". A false on either side decides AND and a true decides
        // OR whatever the other side is, so the right side is skipped when the
        // left one already settles it. Otherwise an unknown on either side
        // leaves the answer unknown.
        if (b.operator == .and_) {
            const left = truthValue(try self.evaluate(b.left, row, ctx));
            if (left == .false_) return .{ .bool_val = false };
            const right = truthValue(try self.evaluate(b.right, row, ctx));
            if (right == .false_) return .{ .bool_val = false };
            if (left == .unknown or right == .unknown) return .{ .null_val = {} };
            return .{ .bool_val = true };
        }

        if (b.operator == .or_) {
            const left = truthValue(try self.evaluate(b.left, row, ctx));
            if (left == .true_) return .{ .bool_val = true };
            const right = truthValue(try self.evaluate(b.right, row, ctx));
            if (right == .true_) return .{ .bool_val = true };
            if (left == .unknown or right == .unknown) return .{ .null_val = {} };
            return .{ .bool_val = false };
        }

        // The distance a vector search already measured for this entity, when
        // the expression asks for exactly that: the same property, compared
        // with the same query. Anything else is computed below.
        if (b.operator == .vector_distance) {
            if (searchedDistance(b, row, ctx)) |distance| return .{ .float_val = distance };
        }

        // Full-text match is answered by the index, not by looking at the text in
        // front of us, so it is handled before the left side is evaluated to a
        // string.
        //
        // Reaching this point at all means the planner did not turn the whole
        // WHERE clause into an index scan, which happens as soon as `@@` appears
        // inside AND or OR. Falling back to a substring test would make the same
        // operator mean two different things depending on where it sits: an
        // indexed, stemmed, scored search at the top level, and a case-insensitive
        // substring check anywhere else. Those disagree about which documents
        // match, so a query could be changed from correct to incorrect by adding
        // an unrelated OR beside it.
        if (b.operator == .fts_match) {
            return self.ftsMatchIndexed(b, row, ctx);
        }

        // Evaluate both sides
        const left = try self.evaluate(b.left, row, ctx);
        const right = try self.evaluate(b.right, row, ctx);

        // A null operand makes every remaining operator null: comparisons
        // (including `null = null`, since two unknowns are not known to be
        // equal), XOR, arithmetic and the string operators alike. IN is the
        // exception, because an empty list contains nothing, null included.
        //
        // This is `=` only. Grouping, DISTINCT and ORDER BY treat nulls as
        // equivalent to each other through their own key encoding, and MERGE
        // matches properties with its own comparison, so neither goes
        // through here.
        if (left.isNull() or right.isNull()) {
            if (b.operator == .in_) return self.listContains(left, right);
            return .{ .null_val = {} };
        }

        return switch (b.operator) {
            // Comparison operators
            .eq => self.compareEqual(left, right),
            .neq => blk: {
                const eq = self.compareEqual(left, right);
                // Lists and maps can compare as null, when an element is.
                const known = eq.toBool() orelse break :blk .{ .null_val = {} };
                break :blk .{ .bool_val = !known };
            },
            .lt => self.compareLessThan(left, right),
            .lte => self.compareLessOrEqual(left, right),
            .gt => blk: {
                const lte = self.compareLessOrEqual(left, right);
                if (lte.toBool()) |lte_val| {
                    break :blk .{ .bool_val = !lte_val };
                }
                break :blk .{ .null_val = {} };
            },
            .gte => blk: {
                const lt = self.compareLessThan(left, right);
                if (lt.toBool()) |lt_val| {
                    break :blk .{ .bool_val = !lt_val };
                }
                break :blk .{ .null_val = {} };
            },

            // Logical operators (already handled above for short-circuit)
            .and_, .or_ => unreachable,
            .xor => .{ .bool_val = left.isTruthy() != right.isTruthy() },

            // Arithmetic operators
            .add => self.arithmeticAdd(left, right) catch |err| return self.binaryFailure(b, err, left, right, ctx),
            .sub => self.arithmeticSub(left, right) catch |err| return self.binaryFailure(b, err, left, right, ctx),
            .mul => self.arithmeticMul(left, right) catch |err| return self.binaryFailure(b, err, left, right, ctx),
            .div => self.arithmeticDiv(left, right) catch |err| return self.binaryFailure(b, err, left, right, ctx),
            .mod => self.arithmeticMod(left, right) catch |err| return self.binaryFailure(b, err, left, right, ctx),
            .pow => self.arithmeticPow(left, right),

            // String operators
            .contains => self.stringContains(left, right),
            .starts_with => self.stringStartsWith(left, right),
            .ends_with => self.stringEndsWith(left, right),
            .regex_match => try self.regexMatch(b, left, right, ctx),

            // List operator
            .in_ => self.listContains(left, right),

            // Special operators (handled by dedicated operators)
            .vector_distance => try self.vectorDistance(left, right, ctx),
            .fts_match => try self.ftsMatch(b, left, right, ctx),
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
            .not => switch (truthValue(operand)) {
                .true_ => .{ .bool_val = false },
                .false_ => .{ .bool_val = true },
                .unknown => .{ .null_val = {} },
            },
            .neg => switch (operand) {
                .int_val => |i| .{ .int_val = checkedInt(std.math.sub(i64, 0, i)) catch |err|
                    return ctx.fail(err, u.location, "Integer overflow: -({d}) does not fit in a 64-bit integer", .{i}) },
                .float_val => |f| .{ .float_val = -f },
                .null_val => .{ .null_val = {} },
                else => ctx.fail(EvalError.TypeError, u.location, "Type error: -{s} is not defined", .{typeName(operand)}),
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
        if (std.ascii.eqlIgnoreCase(f.name, "id")) {
            if (f.arguments.len != 1) return EvalError.InvalidArgumentCount;
            const arg = try self.evaluate(f.arguments[0], row, ctx);
            return switch (arg) {
                .node_ref => |id| .{ .int_val = @intCast(id) },
                .edge_ref => |id| .{ .int_val = @intCast(id) },
                .null_val => .{ .null_val = {} },
                else => EvalError.TypeError,
            };
        }

        if (std.ascii.eqlIgnoreCase(f.name, "coalesce")) {
            for (f.arguments) |arg_expr| {
                const arg = try self.evaluate(arg_expr, row, ctx);
                if (!arg.isNull()) return arg;
            }
            return .{ .null_val = {} };
        }

        if (std.ascii.eqlIgnoreCase(f.name, "abs")) {
            if (f.arguments.len != 1) return EvalError.InvalidArgumentCount;
            const arg = try self.evaluate(f.arguments[0], row, ctx);
            return switch (arg) {
                .int_val => |i| .{ .int_val = if (i < 0) try checkedInt(std.math.sub(i64, 0, i)) else i },
                .float_val => |fv| .{ .float_val = @abs(fv) },
                .null_val => .{ .null_val = {} },
                else => EvalError.TypeError,
            };
        }

        if (std.ascii.eqlIgnoreCase(f.name, "size")) {
            if (f.arguments.len != 1) return EvalError.InvalidArgumentCount;
            const arg = try self.evaluate(f.arguments[0], row, ctx);
            return switch (arg) {
                // Characters (code points), not bytes: size('é') is 1.
                .string_val => |s| .{ .int_val = @intCast(codepointCount(s)) },
                .bytes_val => |b| .{ .int_val = @intCast(b.len) },
                .list_val => |l| .{ .int_val = @intCast(l.len) },
                .null_val => .{ .null_val = {} },
                else => EvalError.TypeError,
            };
        }

        if (std.ascii.eqlIgnoreCase(f.name, "toInteger")) {
            if (f.arguments.len != 1) return EvalError.InvalidArgumentCount;
            const arg = try self.evaluate(f.arguments[0], row, ctx);
            return switch (arg) {
                .int_val => arg,
                .float_val => |fv| .{ .int_val = floatToInt(fv) orelse return EvalError.IntegerOverflow },
                .string_val => |s| blk: {
                    const i = std.fmt.parseInt(i64, s, 10) catch return .{ .null_val = {} };
                    break :blk .{ .int_val = i };
                },
                else => .{ .null_val = {} },
            };
        }

        if (std.ascii.eqlIgnoreCase(f.name, "toFloat")) {
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

        if (std.ascii.eqlIgnoreCase(f.name, "toString")) {
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
                // toString(null) is null, like every function of null.
                .null_val => .{ .null_val = {} },
                else => .{ .null_val = {} },
            };
        }

        // String functions
        if (std.ascii.eqlIgnoreCase(f.name, "toLower")) {
            if (f.arguments.len != 1) return EvalError.InvalidArgumentCount;
            const arg = try self.evaluate(f.arguments[0], row, ctx);
            return switch (arg) {
                .string_val => |s| .{ .string_val = try self.mapCase(s, .lower) },
                .null_val => .{ .null_val = {} },
                else => EvalError.TypeError,
            };
        }

        if (std.ascii.eqlIgnoreCase(f.name, "toUpper")) {
            if (f.arguments.len != 1) return EvalError.InvalidArgumentCount;
            const arg = try self.evaluate(f.arguments[0], row, ctx);
            return switch (arg) {
                .string_val => |s| .{ .string_val = try self.mapCase(s, .upper) },
                .null_val => .{ .null_val = {} },
                else => EvalError.TypeError,
            };
        }

        if (std.ascii.eqlIgnoreCase(f.name, "trim")) {
            if (f.arguments.len != 1) return EvalError.InvalidArgumentCount;
            const arg = try self.evaluate(f.arguments[0], row, ctx);
            return switch (arg) {
                .string_val => |s| .{ .string_val = std.mem.trim(u8, s, " \t\n\r") },
                .null_val => .{ .null_val = {} },
                else => EvalError.TypeError,
            };
        }

        if (std.ascii.eqlIgnoreCase(f.name, "substring")) {
            if (f.arguments.len < 2 or f.arguments.len > 3) return EvalError.InvalidArgumentCount;
            const arg = try self.evaluate(f.arguments[0], row, ctx);
            const start_arg = try self.evaluate(f.arguments[1], row, ctx);
            const len_arg: EvalResult = if (f.arguments.len == 3) try self.evaluate(f.arguments[2], row, ctx) else .{ .int_val = std.math.maxInt(i64) };
            // A null in any argument makes the answer null.
            if (arg.isNull() or start_arg.isNull() or len_arg.isNull()) return .{ .null_val = {} };
            const s = switch (arg) {
                .string_val => |sv| sv,
                else => return EvalError.TypeError,
            };
            const start_raw = start_arg.toInt() orelse return EvalError.TypeError;
            const length_raw = len_arg.toInt() orelse return EvalError.TypeError;
            // Positions count characters (code points), not bytes, so a
            // multi-byte character is never cut in half.
            const begin = advanceCodepoints(s, 0, if (start_raw < 0) 0 else @intCast(start_raw));
            const end = advanceCodepoints(s, begin, if (length_raw < 0) 0 else @intCast(length_raw));
            return .{ .string_val = s[begin..end] };
        }

        if (std.ascii.eqlIgnoreCase(f.name, "replace")) {
            if (f.arguments.len != 3) return EvalError.InvalidArgumentCount;
            const arg = try self.evaluate(f.arguments[0], row, ctx);
            const search_arg = try self.evaluate(f.arguments[1], row, ctx);
            const replace_arg = try self.evaluate(f.arguments[2], row, ctx);
            // A null in any argument makes the answer null.
            if (arg.isNull() or search_arg.isNull() or replace_arg.isNull()) return .{ .null_val = {} };
            const s = switch (arg) {
                .string_val => |sv| sv,
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

        if (std.ascii.eqlIgnoreCase(f.name, "split")) {
            if (f.arguments.len != 2) return EvalError.InvalidArgumentCount;
            const arg = try self.evaluate(f.arguments[0], row, ctx);
            const delim_arg = try self.evaluate(f.arguments[1], row, ctx);
            // A null in any argument makes the answer null.
            if (arg.isNull() or delim_arg.isNull()) return .{ .null_val = {} };
            const s = switch (arg) {
                .string_val => |sv| sv,
                else => return EvalError.TypeError,
            };
            const delimiter = switch (delim_arg) {
                .string_val => |sv| sv,
                else => return EvalError.TypeError,
            };
            var parts: std.ArrayListUnmanaged(EvalResult) = .empty;
            errdefer parts.deinit(self.allocator);
            if (delimiter.len == 0) {
                // An empty delimiter splits between characters. It used to
                // reach splitSequence, which asserts a non-empty delimiter.
                var i: usize = 0;
                while (i < s.len) {
                    const n = codepointLen(s, i);
                    parts.append(self.allocator, .{ .string_val = s[i .. i + n] }) catch return EvalError.OutOfMemory;
                    i += n;
                }
                return .{ .list_val = parts.toOwnedSlice(self.allocator) catch return EvalError.OutOfMemory };
            }
            var iter = std.mem.splitSequence(u8, s, delimiter);
            while (iter.next()) |part| {
                parts.append(self.allocator, .{ .string_val = part }) catch return EvalError.OutOfMemory;
            }
            return .{ .list_val = parts.toOwnedSlice(self.allocator) catch return EvalError.OutOfMemory };
        }

        // List functions
        if (std.ascii.eqlIgnoreCase(f.name, "head")) {
            if (f.arguments.len != 1) return EvalError.InvalidArgumentCount;
            const arg = try self.evaluate(f.arguments[0], row, ctx);
            return switch (arg) {
                .list_val => |l| if (l.len > 0) l[0] else .{ .null_val = {} },
                .null_val => .{ .null_val = {} },
                else => EvalError.TypeError,
            };
        }

        if (std.ascii.eqlIgnoreCase(f.name, "tail")) {
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

        if (std.ascii.eqlIgnoreCase(f.name, "last")) {
            if (f.arguments.len != 1) return EvalError.InvalidArgumentCount;
            const arg = try self.evaluate(f.arguments[0], row, ctx);
            return switch (arg) {
                .list_val => |l| if (l.len > 0) l[l.len - 1] else .{ .null_val = {} },
                .null_val => .{ .null_val = {} },
                else => EvalError.TypeError,
            };
        }

        if (std.ascii.eqlIgnoreCase(f.name, "range")) {
            if (f.arguments.len < 2 or f.arguments.len > 3) return EvalError.InvalidArgumentCount;
            const start_arg = try self.evaluate(f.arguments[0], row, ctx);
            const end_arg = try self.evaluate(f.arguments[1], row, ctx);
            // A function of null is null.
            if (start_arg.isNull() or end_arg.isNull()) return .{ .null_val = {} };
            const start = start_arg.toInt() orelse return EvalError.TypeError;
            const end = end_arg.toInt() orelse return EvalError.TypeError;
            var step: i64 = 1;
            if (f.arguments.len == 3) {
                const step_arg = try self.evaluate(f.arguments[2], row, ctx);
                if (step_arg.isNull()) return .{ .null_val = {} };
                step = step_arg.toInt() orelse return EvalError.TypeError;
                if (step == 0) return EvalError.InvalidOperation;
            }
            // Cypher range() is inclusive on both ends. Count first, in i128 so
            // neither the span nor the step can overflow, then refuse a list
            // too large to build instead of allocating until memory runs out.
            const span = @as(i128, end) - @as(i128, start);
            const count: i128 = if ((step > 0 and span < 0) or (step < 0 and span > 0))
                0
            else
                @divTrunc(span, step) + 1;
            if (count > MAX_RANGE_LENGTH) return EvalError.RangeTooLarge;
            const items = self.allocator.alloc(EvalResult, @intCast(count)) catch return EvalError.OutOfMemory;
            for (items, 0..) |*item, k| {
                // Every element lies between start and end, so it fits in i64.
                item.* = .{ .int_val = @intCast(@as(i128, start) + @as(i128, @intCast(k)) * step) };
            }
            return .{ .list_val = items };
        }

        // Type/graph functions
        if (std.ascii.eqlIgnoreCase(f.name, "type")) {
            if (f.arguments.len != 1) return EvalError.InvalidArgumentCount;
            const arg = try self.evaluate(f.arguments[0], row, ctx);
            return switch (arg) {
                .edge_ref => |edge_id| blk: {
                    const symbol_table = ctx.symbol_table orelse break :blk .{ .null_val = {} };
                    const database = ctx.database orelse break :blk .{ .null_val = {} };
                    // The type alone, not the relationship's properties.
                    const edge_type = (database.getEdgeTypeInTxn(ctx.txn, edge_id) catch |err|
                        return readFailure(ctx, err, f.location, "reading relationship {d}", .{edge_id})) orelse break :blk .{ .null_val = {} };
                    const resolved = symbol_table.resolve(edge_type) catch |err|
                        return readFailure(ctx, err, f.location, "resolving the type of relationship {d}", .{edge_id});
                    defer symbol_table.freeString(resolved);
                    const owned = self.allocator.dupe(u8, resolved) catch return EvalError.OutOfMemory;
                    break :blk .{ .string_val = owned };
                },
                .null_val => .{ .null_val = {} },
                else => EvalError.TypeError,
            };
        }

        if (std.ascii.eqlIgnoreCase(f.name, "labels")) {
            if (f.arguments.len != 1) return EvalError.InvalidArgumentCount;
            const arg = try self.evaluate(f.arguments[0], row, ctx);
            return switch (arg) {
                .node_ref => |node_id| blk: {
                    const database = ctx.database orelse break :blk .{ .null_val = {} };
                    const symbol_table = ctx.symbol_table orelse break :blk .{ .null_val = {} };
                    // The labels alone, not the node's properties.
                    const label_ids = (database.getNodeLabelIdsInTxn(ctx.txn, node_id, self.allocator) catch |err|
                        return readFailure(ctx, err, f.location, "reading node {d}", .{node_id})) orelse break :blk .{ .null_val = {} };
                    defer self.allocator.free(label_ids);
                    var label_list: std.ArrayListUnmanaged(EvalResult) = .empty;
                    for (label_ids) |label_id| {
                        const resolved = symbol_table.resolve(label_id) catch |err|
                            return readFailure(ctx, err, f.location, "resolving label {d} of node {d}", .{ label_id, node_id });
                        defer symbol_table.freeString(resolved);
                        const owned = self.allocator.dupe(u8, resolved) catch return EvalError.OutOfMemory;
                        label_list.append(self.allocator, .{ .string_val = owned }) catch return EvalError.OutOfMemory;
                    }
                    break :blk .{ .list_val = label_list.toOwnedSlice(self.allocator) catch return EvalError.OutOfMemory };
                },
                .null_val => .{ .null_val = {} },
                else => EvalError.TypeError,
            };
        }

        if (std.ascii.eqlIgnoreCase(f.name, "score")) {
            if (f.arguments.len != 1) return EvalError.InvalidArgumentCount;
            // The relevance the full-text search that matched this row gave
            // the entity: BM25, higher is better, the best of an OR's
            // searches. Null for an entity no search scored on this row.
            const variable = switch (f.arguments[0].*) {
                .variable => |v| v,
                else => return ctx.fail(EvalError.TypeError, f.location, "score() takes a variable, as in score(d) after WHERE d.text @@ 'query'", .{}),
            };
            const slot = ctx.getVariableSlot(variable.name) orelse
                return ctx.fail(EvalError.UnboundVariable, variable.location, "Variable `{s}` is not defined here", .{variable.name});
            const value = row.searchScore(slot) orelse return .{ .null_val = {} };
            return .{ .float_val = value };
        }

        if (std.ascii.eqlIgnoreCase(f.name, "properties")) {
            if (f.arguments.len != 1) return EvalError.InvalidArgumentCount;
            const arg = try self.evaluate(f.arguments[0], row, ctx);
            if (arg == .node_ref) {
                const node_id = arg.node_ref;
                const database = ctx.database orelse return .{ .null_val = {} };
                const symbol_table = ctx.symbol_table orelse return .{ .null_val = {} };
                var node = (database.getNodeInTxn(ctx.txn, node_id) catch |err|
                    return readFailure(ctx, err, f.location, "reading node {d}", .{node_id})) orelse return .{ .null_val = {} };
                defer node.deinit(ctx.allocator);
                var entries: std.ArrayListUnmanaged(EvalResult.MapEntry) = .empty;
                for (node.properties) |prop| {
                    const key = symbol_table.resolve(prop.key_id) catch |err|
                        return readFailure(ctx, err, f.location, "resolving property key {d} of node {d}", .{ prop.key_id, node_id });
                    defer symbol_table.freeString(key);
                    const owned_key = self.allocator.dupe(u8, key) catch return EvalError.OutOfMemory;
                    entries.append(self.allocator, .{
                        .key = owned_key,
                        .value = try EvalResult.fromPropertyValue(prop.value, self.allocator),
                    }) catch return EvalError.OutOfMemory;
                }
                return .{ .map_val = entries.toOwnedSlice(self.allocator) catch return EvalError.OutOfMemory };
            } else if (arg == .edge_ref) {
                const edge_id = arg.edge_ref;
                const database = ctx.database orelse return .{ .null_val = {} };
                const symbol_table = ctx.symbol_table orelse return .{ .null_val = {} };
                var edge = (database.getEdgeInTxn(ctx.txn, edge_id) catch |err|
                    return readFailure(ctx, err, f.location, "reading relationship {d}", .{edge_id})) orelse return .{ .null_val = {} };
                defer edge.deinit(ctx.allocator);
                var entries: std.ArrayListUnmanaged(EvalResult.MapEntry) = .empty;
                for (edge.properties) |prop| {
                    const key = symbol_table.resolve(prop.key_id) catch |err|
                        return readFailure(ctx, err, f.location, "resolving property key {d} of relationship {d}", .{ prop.key_id, edge_id });
                    defer symbol_table.freeString(key);
                    const owned_key = self.allocator.dupe(u8, key) catch return EvalError.OutOfMemory;
                    entries.append(self.allocator, .{
                        .key = owned_key,
                        .value = try EvalResult.fromPropertyValue(prop.value, self.allocator),
                    }) catch return EvalError.OutOfMemory;
                }
                return .{ .map_val = entries.toOwnedSlice(self.allocator) catch return EvalError.OutOfMemory };
            } else if (arg == .null_val) {
                return .{ .null_val = {} };
            } else if (arg == .map_val) {
                // A map's properties are its entries.
                return arg;
            }
            return EvalError.TypeError;
        }

        if (std.ascii.eqlIgnoreCase(f.name, "length") or
            std.ascii.eqlIgnoreCase(f.name, "nodes") or
            std.ascii.eqlIgnoreCase(f.name, "relationships"))
        {
            return self.evaluatePathFunction(f, row, ctx);
        }

        // exists(n.prop): whether the property is there, which is whether
        // it is not null.
        if (std.ascii.eqlIgnoreCase(f.name, "exists")) {
            if (f.arguments.len != 1) return EvalError.InvalidArgumentCount;
            const arg = try self.evaluate(f.arguments[0], row, ctx);
            return .{ .bool_val = !arg.isNull() };
        }

        return EvalError.UnknownFunction;
    }

    /// `length(p)`, the number of relationships in a path; `nodes(p)` and
    /// `relationships(p)`, its nodes and relationships in order. Null for a
    /// null path; anything else is a type error.
    fn evaluatePathFunction(
        self: *Self,
        f: *const ast.FunctionCall,
        row: *const Row,
        ctx: *const ExecutionContext,
    ) EvalError!EvalResult {
        if (f.arguments.len != 1) return EvalError.InvalidArgumentCount;
        const arg = try self.evaluate(f.arguments[0], row, ctx);
        const path = switch (arg) {
            .path_val => |p| p,
            .null_val => return .{ .null_val = {} },
            else => return EvalError.TypeError,
        };
        if (std.ascii.eqlIgnoreCase(f.name, "length")) return .{ .int_val = @intCast(path.edges.len) };
        if (std.ascii.eqlIgnoreCase(f.name, "nodes")) {
            const out = self.allocator.alloc(EvalResult, path.nodes.len) catch return EvalError.OutOfMemory;
            for (path.nodes, out) |id, *o| o.* = .{ .node_ref = id };
            return .{ .list_val = out };
        }
        const out = self.allocator.alloc(EvalResult, path.edges.len) catch return EvalError.OutOfMemory;
        for (path.edges, out) |id, *o| o.* = .{ .edge_ref = id };
        return .{ .list_val = out };
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

    /// `s` in lower or upper case, copied into the evaluator's allocator.
    /// ASCII is mapped a byte at a time; other characters through the case
    /// tables full-text search uses (Latin, Greek, Cyrillic, Armenian, and
    /// full-width Latin). Bytes that are not valid UTF-8 are kept as they
    /// are. No mapping makes a character longer, so the copy never grows.
    fn mapCase(self: *Self, s: []const u8, comptime to: enum { lower, upper }) EvalError![]const u8 {
        const out = self.allocator.alloc(u8, s.len) catch return EvalError.OutOfMemory;
        var len: usize = 0;
        var i: usize = 0;
        while (i < s.len) {
            const c = s[i];
            if (c < 0x80) {
                out[len] = if (to == .lower) std.ascii.toLower(c) else std.ascii.toUpper(c);
                len += 1;
                i += 1;
                continue;
            }
            const n = codepointLen(s, i);
            if (n == 1) {
                out[len] = c;
                len += 1;
                i += 1;
                continue;
            }
            const cp = std.unicode.utf8Decode(s[i .. i + n]) catch unreachable; // codepointLen checked it
            const mapped = if (to == .lower) tokenizer.lowerCodepoint(cp) else tokenizer.upperCodepoint(cp);
            len += std.unicode.utf8Encode(mapped, out[len..]) catch unreachable; // never longer
            i += n;
        }
        return out[0..len];
    }

    // ========================================================================
    // Comparison helpers
    // ========================================================================

    fn compareEqual(self: *Self, left: EvalResult, right: EvalResult) EvalResult {
        const result = switch (left) {
            .bool_val => |l| switch (right) {
                .bool_val => |r| l == r,
                else => false,
            },
            .int_val => |l| switch (right) {
                .int_val => |r| l == r,
                .float_val => |r| compareIntFloat(l, r) == .eq,
                else => false,
            },
            .float_val => |l| switch (right) {
                .float_val => |r| l == r,
                .int_val => |r| compareIntFloat(r, l) == .eq,
                else => false,
            },
            .string_val => |l| switch (right) {
                .string_val => |r| std.mem.eql(u8, l, r),
                else => false,
            },
            .bytes_val => |l| switch (right) {
                .bytes_val => |r| std.mem.eql(u8, l, r),
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
            // Lists and maps used to compare unequal even to themselves.
            .list_val => |l| switch (right) {
                .list_val => |r| return self.listsEqual(l, r),
                else => false,
            },
            .map_val => |l| switch (right) {
                .map_val => |r| return self.mapsEqual(l, r),
                else => false,
            },
            .vector_val => |l| switch (right) {
                .vector_val => |r| l.len == r.len and for (l, r) |a, b| {
                    if (a != b) break false;
                } else true,
                else => false,
            },
            .path_val => |l| switch (right) {
                .path_val => |r| std.mem.eql(NodeId, l.nodes, r.nodes) and std.mem.eql(EdgeId, l.edges, r.edges),
                else => false,
            },
            .null_val => return .{ .null_val = {} },
        };
        return .{ .bool_val = result };
    }

    /// `=` between elements of a list or map, where a null is unknown.
    fn elementsEqual(self: *Self, left: EvalResult, right: EvalResult) EvalResult {
        if (left.isNull() or right.isNull()) return .{ .null_val = {} };
        return self.compareEqual(left, right);
    }

    /// Lists are equal when they have the same length and their elements
    /// are equal in order. A pair known to differ makes them unequal; else
    /// a null element makes the answer null, as `[1, null] = [1, null]` is.
    fn listsEqual(self: *Self, left: []const EvalResult, right: []const EvalResult) EvalResult {
        if (left.len != right.len) return .{ .bool_val = false };
        var unknown = false;
        for (left, right) |a, b| {
            const eq = self.elementsEqual(a, b).toBool() orelse {
                unknown = true;
                continue;
            };
            if (!eq) return .{ .bool_val = false };
        }
        if (unknown) return .{ .null_val = {} };
        return .{ .bool_val = true };
    }

    /// Maps are equal when they have the same keys and equal values under
    /// each, in any order.
    fn mapsEqual(self: *Self, left: []const EvalResult.MapEntry, right: []const EvalResult.MapEntry) EvalResult {
        if (left.len != right.len) return .{ .bool_val = false };
        var unknown = false;
        for (left) |entry| {
            const other = for (right) |candidate| {
                if (std.mem.eql(u8, candidate.key, entry.key)) break candidate;
            } else return .{ .bool_val = false };
            const eq = self.elementsEqual(entry.value, other.value).toBool() orelse {
                unknown = true;
                continue;
            };
            if (!eq) return .{ .bool_val = false };
        }
        if (unknown) return .{ .null_val = {} };
        return .{ .bool_val = true };
    }

    fn compareLessThan(_: *Self, left: EvalResult, right: EvalResult) EvalResult {
        const result: ?bool = switch (left) {
            // false < true, as Cypher orders booleans.
            .bool_val => |l| switch (right) {
                .bool_val => |r| !l and r,
                else => null,
            },
            .int_val => |l| switch (right) {
                .int_val => |r| l < r,
                .float_val => |r| compareIntFloat(l, r) == .lt,
                else => null,
            },
            .float_val => |l| switch (right) {
                .float_val => |r| l < r,
                .int_val => |r| compareIntFloat(r, l) == .gt,
                else => null,
            },
            .string_val => |l| switch (right) {
                .string_val => |r| std.mem.order(u8, l, r) == .lt,
                else => null,
            },
            .bytes_val => |l| switch (right) {
                .bytes_val => |r| std.mem.order(u8, l, r) == .lt,
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

    fn arithmeticAdd(self: *Self, left: EvalResult, right: EvalResult) EvalError!EvalResult {
        if (left == .list_val or right == .list_val) return self.listConcat(left, right);
        return switch (left) {
            .int_val => |l| switch (right) {
                .int_val => |r| .{ .int_val = try checkedInt(std.math.add(i64, l, r)) },
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
                    const result = self.allocator.alloc(u8, l.len + r.len) catch return EvalError.OutOfMemory;
                    @memcpy(result[0..l.len], l);
                    @memcpy(result[l.len..], r);
                    break :blk .{ .string_val = result };
                },
                else => .{ .null_val = {} },
            },
            else => .{ .null_val = {} },
        };
    }

    /// `list + list` joins two lists; `list + x` and `x + list` add one
    /// element at the end or the front. With a null side the result is null.
    fn listConcat(self: *Self, left: EvalResult, right: EvalResult) EvalError!EvalResult {
        if (left.isNull() or right.isNull()) return .{ .null_val = {} };
        const l: []const EvalResult = if (left == .list_val) left.list_val else &.{left};
        const r: []const EvalResult = if (right == .list_val) right.list_val else &.{right};
        const out = self.allocator.alloc(EvalResult, l.len + r.len) catch return EvalError.OutOfMemory;
        @memcpy(out[0..l.len], l);
        @memcpy(out[l.len..], r);
        return .{ .list_val = out };
    }

    fn arithmeticSub(_: *Self, left: EvalResult, right: EvalResult) EvalError!EvalResult {
        return switch (left) {
            .int_val => |l| switch (right) {
                .int_val => |r| .{ .int_val = try checkedInt(std.math.sub(i64, l, r)) },
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

    fn arithmeticMul(_: *Self, left: EvalResult, right: EvalResult) EvalError!EvalResult {
        return switch (left) {
            .int_val => |l| switch (right) {
                .int_val => |r| .{ .int_val = try checkedInt(std.math.mul(i64, l, r)) },
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
                // minInt / -1 is the one quotient that does not fit.
                .int_val => |r| .{ .int_val = try checkedInt(std.math.divTrunc(i64, l, r)) },
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

    /// `%` is a remainder that takes the sign of the dividend, as in Cypher
    /// (and Java): `-7 % 2` is -1 and `7 % -2` is 1. That is a truncating
    /// remainder, not the floored modulus `@mod` computes.
    fn arithmeticMod(_: *Self, left: EvalResult, right: EvalResult) EvalError!EvalResult {
        // A zero divisor is an error, as it is for `/`.
        switch (right) {
            .int_val => |r| if (r == 0) return EvalError.DivisionByZero,
            .float_val => |r| if (r == 0.0) return EvalError.DivisionByZero,
            else => {},
        }

        return switch (left) {
            .int_val => |l| switch (right) {
                // minInt % -1 is 0, but computing it overflows, so answer it
                // directly. Every other remainder fits.
                .int_val => |r| .{ .int_val = if (r == -1) 0 else @rem(l, r) },
                .float_val => |r| .{ .float_val = @rem(@as(f64, @floatFromInt(l)), r) },
                else => .{ .null_val = {} },
            },
            .float_val => |l| switch (right) {
                .float_val => |r| .{ .float_val = @rem(l, r) },
                .int_val => |r| .{ .float_val = @rem(l, @as(f64, @floatFromInt(r))) },
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

    /// `text =~ pattern`: whether the pattern matches the whole text. A
    /// pattern the engine does not accept fails the query rather than
    /// matching nothing. See regex.zig for the syntax.
    fn regexMatch(self: *Self, b: *const ast.BinaryExpr, left: EvalResult, right: EvalResult, ctx: *const ExecutionContext) EvalError!EvalResult {
        const text = switch (left) {
            .string_val => |s| s,
            else => return .{ .null_val = {} },
        };

        const pattern = switch (right) {
            .string_val => |s| s,
            else => return .{ .null_val = {} },
        };

        const matched = (if (ctx.regex_cache) |cache|
            cache.fullMatch(pattern, text)
        else
            regex.fullMatch(self.allocator, pattern, text)) catch |err| return switch (err) {
            error.OutOfMemory => EvalError.OutOfMemory,
            error.InvalidPattern => ctx.fail(EvalError.InvalidRegex, b.right.getLocation(), "Invalid regular expression {f}: =~ accepts a subset of Java's syntax, without backreferences or lookaround", .{Brief{ .value = right }}),
            error.PatternTooComplex => ctx.fail(EvalError.InvalidRegex, b.right.getLocation(), "Regular expression {f} is too complex: it compiles to more than {d} instructions, or repeats more than {d} times", .{ Brief{ .value = right }, regex.MAX_PROGRAM, regex.MAX_REPEAT }),
        };
        return .{ .bool_val = matched };
    }

    // ========================================================================
    // List helpers
    // ========================================================================

    /// `elem IN list`, with nulls handled the way `=` handles them.
    ///
    /// A match anywhere makes the answer true. Without one, a null element or
    /// a null in the list means a match cannot be ruled out, so the answer is
    /// null rather than false. An empty list contains nothing, so that answer
    /// is false even for a null element.
    fn listContains(self: *Self, elem: EvalResult, list: EvalResult) EvalResult {
        const items = switch (list) {
            .list_val => |l| l,
            else => return .{ .null_val = {} },
        };

        if (items.len == 0) return .{ .bool_val = false };
        if (elem.isNull()) return .{ .null_val = {} };

        var saw_null = false;
        for (items) |item| {
            if (item.isNull()) {
                saw_null = true;
                continue;
            }
            const eq = self.compareEqual(elem, item);
            if (eq.toBool()) |is_eq| {
                if (is_eq) return .{ .bool_val = true };
            }
        }

        if (saw_null) return .{ .null_val = {} };
        return .{ .bool_val = false };
    }

    /// The distance the row's vector search measured, if `b` is the
    /// expression that search answered: `x.prop <=> $q` or a literal vector,
    /// on the variable, property and query the search used.
    fn searchedDistance(b: *const ast.BinaryExpr, row: *const Row, ctx: *const ExecutionContext) ?f64 {
        const access = switch (b.left.*) {
            .property_access => |pa| pa,
            else => return null,
        };
        const variable = switch (access.object.*) {
            .variable => |v| v,
            else => return null,
        };
        const slot = ctx.getVariableSlot(variable.name) orelse return null;
        const distance = row.searchDistance(slot) orelse return null;
        const search = (ctx.vector_queries orelse return null).get(slot) orelse return null;
        if (!std.mem.eql(u8, search.property, access.property)) return null;
        switch (b.right.*) {
            .parameter => |p| {
                const name = search.param_name orelse return null;
                if (!std.mem.eql(u8, name, p.name)) return null;
            },
            .list => |list| {
                const literal = search.literal orelse return null;
                if (literal.len != list.elements.len) return null;
                for (literal, list.elements) |want, elem| {
                    const got: f32 = switch (elem.*) {
                        .literal => |lit| switch (lit.value) {
                            .float => |f| @floatCast(f),
                            .integer => |n| @floatFromInt(n),
                            else => return null,
                        },
                        else => return null,
                    };
                    if (got != want) return null;
                }
            },
            else => return null,
        }
        return distance;
    }

    /// `a <=> b`: the distance under the database's vector metric, the same
    /// number a vector search reports, or cosine distance where there is no
    /// vector index to say otherwise.
    fn vectorDistance(self: *Self, left: EvalResult, right: EvalResult, ctx: *const ExecutionContext) EvalError!EvalResult {
        const left_vec = (try self.asVector(left)) orelse return .{ .null_val = {} };
        const right_vec = (try self.asVector(right)) orelse return .{ .null_val = {} };

        if (left_vec.len == 0 or right_vec.len == 0) return .{ .null_val = {} };
        if (left_vec.len != right_vec.len) return .{ .null_val = {} };

        if (ctx.database) |database| {
            return .{ .float_val = database.vectorDistance(left_vec, right_vec) };
        }
        return .{ .float_val = vector_distance.cosineDistance(left_vec, right_vec) };
    }

    fn asVector(self: *Self, value: EvalResult) EvalError!?[]const f32 {
        return switch (value) {
            .vector_val => |v| v,
            .list_val => |items| blk: {
                const vec = try self.allocator.alloc(f32, items.len);
                for (items, 0..) |item, i| {
                    vec[i] = switch (item) {
                        .float_val => |f| @floatCast(f),
                        .int_val => |n| @floatFromInt(n),
                        else => {
                            self.allocator.free(vec);
                            break :blk null;
                        },
                    };
                }
                break :blk vec;
            },
            else => null,
        };
    }

    /// Ask the declared full-text index whether this row's node matches.
    ///
    /// The left side names a node through a property access, and the property
    /// selects which declared index answers. Reached when `@@` sits inside a
    /// larger condition, so the planner could not turn the whole WHERE into a
    /// single index scan.
    fn ftsMatchIndexed(
        self: *Self,
        b: *const ast.BinaryExpr,
        row: *const Row,
        ctx: *const ExecutionContext,
    ) EvalError!EvalResult {
        const database = ctx.database orelse return .{ .null_val = {} };

        // The node being tested, reached through the variable the property
        // access hangs off.
        const property_access = switch (b.left.*) {
            .property_access => |pa| pa,
            // Anything that is not a property on a node has no indexed document
            // behind it, so there is nothing to look up. `"some text" @@ "query"`
            // asks whether one piece of text matches another, which is a
            // different and self-contained question.
            else => return self.ftsMatchLiteral(b, row, ctx),
        };
        const variable = switch (property_access.object.*) {
            .variable => |v| v,
            else => return self.ftsMatchLiteral(b, row, ctx),
        };
        const slot = ctx.variables.get(variable.name) orelse {
            return self.ftsMatchLiteral(b, row, ctx);
        };
        // The slot says whether this is a node or an edge. Both can carry a
        // declared index over a property, and reading the slot as one kind
        // regardless would make the other silently never match.
        const slot_value = row.slots[slot];
        const is_edge = slot_value.asEdgeId() != null;
        const doc_id = (if (is_edge) slot_value.asEdgeId() else slot_value.asNodeId()) orelse {
            return self.ftsMatchLiteral(b, row, ctx);
        };

        const query = switch (try self.evaluate(b.right, row, ctx)) {
            .string_val => |q| q,
            else => return .{ .null_val = {} },
        };
        if (query.len == 0) return .{ .bool_val = false };

        // Asking the index one node at a time is slower than scanning it once,
        // which is why the planner prefers an index scan when it can build one.
        // Correctness first: a filter that agrees with the scan beats a fast
        // filter that disagrees with it.
        // The cache lives for one query execution, so every row after the first
        // reads a set rather than searching the index again.
        const matched = (if (is_edge)
            database.ftsEdgeMatches(ctx.fts_cache, ctx.txn, doc_id, property_access.property, query, FTS_FILTER_LIMIT)
        else
            database.ftsNodeMatches(ctx.fts_cache, ctx.txn, doc_id, property_access.property, query, FTS_FILTER_LIMIT)) catch |err| {
            // A query the index refuses is the query's fault, said as the
            // index scan says it.
            if (err == error.InvalidArgument) switch (fts_query.refusalOf(self.allocator, query)) {
                .only_excludes => return ctx.fail(EvalError.InvalidOperation, b.right.getLocation(), "Full-text query '{s}' only excludes; it needs a word to match (write NOT x @@ 'word' to exclude)", .{query}),
                .phrase => return ctx.fail(EvalError.InvalidOperation, b.right.getLocation(), "Full-text query '{s}' has a phrase, and the index for {s}.{s} stores no word positions to match one (indexes built before 0.17.0 have none); drop the index and declare it again", .{ query, variable.name, property_access.property }),
                .neither => {},
            };
            // A failed search is an error. Reading it as null made a damaged
            // index look like a document that does not match.
            return readFailure(ctx, err, b.location, "searching the full-text index for {s}.{s}", .{ variable.name, property_access.property });
        };

        // No index declared for this property on any label the node carries, or
        // on the edge's type. The
        // planner reports this as an error when it can see the whole condition;
        // here the condition is a larger expression and there is no way to raise
        // one, so the honest answer is the unknown that a null already means in
        // three-valued logic — not a false, which would read as "searched and
        // found nothing".
        const answer = matched orelse return .{ .null_val = {} };
        return .{ .bool_val = answer };
    }

    /// Match text against a query without involving any index.
    ///
    /// Used when the left side is not a node's property, as in
    /// `"some text" @@ "query"`. There is no document to look up, so this asks
    /// the self-contained question instead: does this text contain these terms.
    fn ftsMatchLiteral(
        self: *Self,
        b: *const ast.BinaryExpr,
        row: *const Row,
        ctx: *const ExecutionContext,
    ) EvalError!EvalResult {
        const left = try self.evaluate(b.left, row, ctx);
        const right = try self.evaluate(b.right, row, ctx);
        return self.ftsMatch(b, left, right, ctx);
    }

    /// Whether text matches a query in the full-text query language, with
    /// the default analysis, as an index declared with defaults would. A
    /// query the language refuses (only exclusions) is an error, as it is
    /// against an index.
    fn ftsMatch(self: *Self, b: *const ast.BinaryExpr, left: EvalResult, right: EvalResult, ctx: *const ExecutionContext) EvalError!EvalResult {
        const text = switch (left) {
            .string_val => |s| s,
            else => return .{ .null_val = {} },
        };
        const query = switch (right) {
            .string_val => |s| s,
            else => return .{ .null_val = {} },
        };

        const matched = fts_query.textMatches(self.allocator, query, text, .{}) catch |err| return switch (err) {
            error.OutOfMemory => EvalError.OutOfMemory,
            error.InvalidQuery => ctx.fail(EvalError.InvalidOperation, b.right.getLocation(), "Full-text query {f} only excludes; it needs a word to match (write NOT x @@ 'word' to exclude)", .{Brief{ .value = right }}),
        };
        return .{ .bool_val = matched };
    }
};

// ============================================================================
// Tests
// ============================================================================

test {
    _ = regex;
}

test "every function evaluateFunction answers is in builtin_functions" {
    // The analyser refuses a name missing from the list, so a function added
    // to the evaluator alone would be unreachable. Read the names out of
    // this file's own source.
    const source = @embedFile("expression.zig");
    const marker = "eqlIgnoreCase(f.name, \"";
    var checked: usize = 0;
    var rest: []const u8 = source;
    while (std.mem.indexOf(u8, rest, marker)) |at| {
        rest = rest[at + marker.len ..];
        const end = std.mem.indexOfScalar(u8, rest, '"') orelse break;
        const name = rest[0..end];
        if (findFunction(name) == null) {
            std.debug.print("function '{s}' is evaluated but missing from builtin_functions\n", .{name});
            return error.TestUnexpectedResult;
        }
        checked += 1;
    }
    try std.testing.expect(checked >= builtin_functions.len);
}

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

test "evaluate gt with incompatible types returns null" {
    const allocator = std.testing.allocator;
    var eval = ExpressionEvaluator.init(allocator);
    var row = Row.init();
    var ctx = ExecutionContext.init(allocator);
    defer ctx.deinit();

    var left = ast.Expression{
        .literal = .{
            .value = .{ .string = "abc" },
            .location = .{ .line = 1, .column = 1 },
        },
    };
    var right = ast.Expression{
        .literal = .{
            .value = .{ .integer = 1 },
            .location = .{ .line = 1, .column = 7 },
        },
    };
    var binary = ast.BinaryExpr{
        .left = &left,
        .operator = .gt,
        .right = &right,
        .location = .{ .line = 1, .column = 5 },
    };
    var expr = ast.Expression{ .binary = &binary };

    const result = try eval.evaluate(&expr, &row, &ctx);
    try std.testing.expect(result.isNull());
}

test "evaluate gte with incompatible types returns null" {
    const allocator = std.testing.allocator;
    var eval = ExpressionEvaluator.init(allocator);
    var row = Row.init();
    var ctx = ExecutionContext.init(allocator);
    defer ctx.deinit();

    var left = ast.Expression{
        .literal = .{
            .value = .{ .string = "abc" },
            .location = .{ .line = 1, .column = 1 },
        },
    };
    var right = ast.Expression{
        .literal = .{
            .value = .{ .integer = 1 },
            .location = .{ .line = 1, .column = 8 },
        },
    };
    var binary = ast.BinaryExpr{
        .left = &left,
        .operator = .gte,
        .right = &right,
        .location = .{ .line = 1, .column = 5 },
    };
    var expr = ast.Expression{ .binary = &binary };

    const result = try eval.evaluate(&expr, &row, &ctx);
    try std.testing.expect(result.isNull());
}

test "fromPropertyValue converts list" {
    const allocator = std.testing.allocator;

    var items = [_]PropertyValue{
        .{ .int_val = 1 },
        .{ .int_val = 2 },
        .{ .string_val = "three" },
    };
    const pv = PropertyValue{ .list_val = &items };

    const result = try EvalResult.fromPropertyValue(pv, allocator);
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

    const result = try EvalResult.fromPropertyValue(pv, allocator);
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

    const result = try EvalResult.fromPropertyValue(pv, allocator);
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

    const result = try EvalResult.fromPropertyValue(pv, allocator);
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

    const pv = try eval.toPropertyValue(allocator);
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

    const pv = try eval.toPropertyValue(allocator);

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

    const pv = try eval.toPropertyValue(allocator);
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
    const eval = try EvalResult.fromPropertyValue(original, allocator);
    defer allocator.free(eval.list_val);

    const roundtrip = try eval.toPropertyValue(allocator);
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

    const eval = try EvalResult.fromPropertyValue(original, allocator);
    defer allocator.free(eval.vector_val);

    const roundtrip = try eval.toPropertyValue(allocator);
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

    const eval = try EvalResult.fromPropertyValue(original, allocator);
    defer {
        allocator.free(eval.map_val[0].key);
        allocator.free(eval.map_val[0].value.string_val);
        allocator.free(eval.map_val[1].key);
        allocator.free(eval.map_val);
    }

    const roundtrip = try eval.toPropertyValue(allocator);
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
