//! Abstract Syntax Tree (AST) definitions for Cypher queries.
//!
//! Defines the node types that represent parsed Cypher queries.
//! All AST nodes are allocated from an arena allocator and freed together.

const std = @import("std");
const Allocator = std.mem.Allocator;

const parser_types = @import("parser.zig");
pub const BinaryOp = parser_types.BinaryOp;
pub const UnaryOp = parser_types.UnaryOp;
pub const EdgeDirection = parser_types.EdgeDirection;

// ============================================================================
// Source Location
// ============================================================================

/// Source location for error reporting
pub const SourceLocation = struct {
    line: u32,
    column: u32,
    length: u32 = 0,

    pub fn format(self: SourceLocation, writer: anytype) !void {
        try writer.print("{d}:{d}", .{ self.line, self.column });
    }
};

// ============================================================================
// Query Structure
// ============================================================================

/// Root of the AST - a complete query
pub const Query = struct {
    clauses: []Clause,
    allocator: Allocator,

    pub fn deinit(self: *Query) void {
        _ = self;
        // All nodes allocated from arena, freed together
    }
};

/// A single clause in a query
pub const Clause = union(enum) {
    match: *MatchClause,
    where: *WhereClause,
    return_: *ReturnClause,
    order_by: *OrderByClause,
    limit: *LimitClause,
    skip: *SkipClause,
};

// ============================================================================
// Clause Definitions
// ============================================================================

/// MATCH clause - pattern matching
pub const MatchClause = struct {
    patterns: []Pattern,
    location: SourceLocation,
};

/// WHERE clause - filtering condition
pub const WhereClause = struct {
    condition: *Expression,
    location: SourceLocation,
};

/// RETURN clause - projection
pub const ReturnClause = struct {
    distinct: bool,
    items: []ReturnItem,
    location: SourceLocation,
};

/// Single item in RETURN clause
pub const ReturnItem = struct {
    expression: *Expression,
    alias: ?[]const u8,
};

/// ORDER BY clause
pub const OrderByClause = struct {
    items: []OrderItem,
    location: SourceLocation,
};

/// Single item in ORDER BY
pub const OrderItem = struct {
    expression: *Expression,
    descending: bool,
};

/// LIMIT clause
pub const LimitClause = struct {
    count: *Expression,
    location: SourceLocation,
};

/// SKIP clause
pub const SkipClause = struct {
    count: *Expression,
    location: SourceLocation,
};

// ============================================================================
// Pattern Definitions
// ============================================================================

/// A graph pattern (sequence of nodes and edges)
pub const Pattern = struct {
    elements: []PatternElement,
};

/// Element in a pattern (node or edge)
pub const PatternElement = union(enum) {
    node: *NodePattern,
    edge: *EdgePattern,
};

/// Node pattern: (variable:Label:Label {props})
pub const NodePattern = struct {
    variable: ?[]const u8,
    labels: [][]const u8,
    properties: ?[]PropertyEntry,
    location: SourceLocation,
};

/// Edge pattern: -[variable:TYPE {props}]->
pub const EdgePattern = struct {
    variable: ?[]const u8,
    types: [][]const u8,
    direction: EdgeDirection,
    properties: ?[]PropertyEntry,
    location: SourceLocation,
};

/// Property key-value pair
pub const PropertyEntry = struct {
    key: []const u8,
    value: *Expression,
};

// ============================================================================
// Expression Definitions
// ============================================================================

/// Expression node
pub const Expression = union(enum) {
    // Literals
    literal: Literal,

    // References
    variable: VariableRef,
    parameter: ParameterRef,
    property_access: *PropertyAccess,

    // Operations
    binary: *BinaryExpr,
    unary: *UnaryExpr,

    // Composites
    function_call: *FunctionCall,
    list: *ListExpr,
    map: *MapExpr,

    /// Get source location for this expression
    pub fn getLocation(self: *const Expression) SourceLocation {
        return switch (self.*) {
            .literal => |l| l.location,
            .variable => |v| v.location,
            .parameter => |p| p.location,
            .property_access => |pa| pa.location,
            .binary => |b| b.location,
            .unary => |u| u.location,
            .function_call => |f| f.location,
            .list => |l| l.location,
            .map => |m| m.location,
        };
    }
};

/// Literal value
pub const Literal = struct {
    value: LiteralValue,
    location: SourceLocation,
};

/// Literal value types
pub const LiteralValue = union(enum) {
    integer: i64,
    float: f64,
    string: []const u8,
    boolean: bool,
    null_value: void,
};

/// Variable reference
pub const VariableRef = struct {
    name: []const u8,
    location: SourceLocation,
};

/// Parameter reference ($name)
pub const ParameterRef = struct {
    name: []const u8, // Without the $ prefix
    location: SourceLocation,
};

/// Property access (object.property)
pub const PropertyAccess = struct {
    object: *Expression,
    property: []const u8,
    location: SourceLocation,
};

/// Binary expression (left op right)
pub const BinaryExpr = struct {
    left: *Expression,
    operator: BinaryOp,
    right: *Expression,
    location: SourceLocation,
};

/// Unary expression (op operand)
pub const UnaryExpr = struct {
    operator: UnaryOp,
    operand: *Expression,
    location: SourceLocation,
};

/// Function call
pub const FunctionCall = struct {
    name: []const u8,
    arguments: []*Expression,
    location: SourceLocation,
};

/// List literal [a, b, c]
pub const ListExpr = struct {
    elements: []*Expression,
    location: SourceLocation,
};

/// Map literal {a: 1, b: 2}
pub const MapExpr = struct {
    entries: []PropertyEntry,
    location: SourceLocation,
};

// ============================================================================
// Tests
// ============================================================================

test "source location format" {
    const loc = SourceLocation{ .line = 10, .column = 5, .length = 3 };
    var buf: [32]u8 = undefined;
    const result = std.fmt.bufPrint(&buf, "{f}", .{loc}) catch unreachable;
    try std.testing.expectEqualStrings("10:5", result);
}

test "expression location" {
    const loc = SourceLocation{ .line = 1, .column = 1 };
    const expr = Expression{
        .literal = Literal{
            .value = .{ .integer = 42 },
            .location = loc,
        },
    };
    try std.testing.expectEqual(@as(u32, 1), expr.getLocation().line);
}
