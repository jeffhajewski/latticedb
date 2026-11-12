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
    /// The clauses of the query, or of its first single query when it has
    /// UNIONs.
    clauses: []Clause,
    allocator: Allocator,
    /// Each UNION and the single query after it, in order.
    unions: []UnionBranch = &.{},
    /// Every pattern subquery in the query's expressions, so that the
    /// planner can tell whether it planned them all.
    subqueries: []*PatternSubquery = &.{},

    pub fn deinit(self: *Query) void {
        _ = self;
        // All nodes allocated from arena, freed together
    }

    /// Whether running this query can change the database.
    ///
    /// Callers use this to decide whether a query needs a read-write
    /// transaction. Only one write transaction may be open at a time, so
    /// running every query as a writer would serialise reads that could have
    /// run together, and running every query as a reader would make writes
    /// impossible. Clauses do not nest, so a scan of the top level is enough.
    pub fn writesData(self: *const Query) bool {
        if (clausesWriteData(self.clauses)) return true;
        for (self.unions) |branch| {
            if (clausesWriteData(branch.clauses)) return true;
        }
        return false;
    }

    fn clausesWriteData(clauses: []const Clause) bool {
        for (clauses) |clause| {
            switch (clause) {
                .create, .delete, .set, .remove, .merge, .foreach => return true,
                .call_subquery => |sq| {
                    if (clausesWriteData(sq.clauses)) return true;
                    for (sq.unions) |branch| {
                        if (clausesWriteData(branch.clauses)) return true;
                    }
                },
                .match, .where, .return_, .order_by, .limit, .skip, .with, .unwind, .call => {},
            }
        }
        return false;
    }
};

/// `UNION [ALL]` and the single query that follows it.
pub const UnionBranch = struct {
    /// UNION ALL keeps duplicate rows; UNION removes them.
    all: bool,
    clauses: []Clause,
    location: SourceLocation,
};

/// A single clause in a query
pub const Clause = union(enum) {
    match: *MatchClause,
    where: *WhereClause,
    return_: *ReturnClause,
    order_by: *OrderByClause,
    limit: *LimitClause,
    skip: *SkipClause,
    create: *CreateClause,
    delete: *DeleteClause,
    set: *SetClause,
    remove: *RemoveClause,
    with: *WithClause,
    merge: *MergeClause,
    unwind: *UnwindClause,
    call: *CallClause,
    foreach: *ForeachClause,
    call_subquery: *CallSubquery,
};

/// `CALL { query }`: the query runs once for each incoming row. A leading
/// `WITH a, b` imports those variables; without it the query sees none.
/// Ending in RETURN, each row it returns extends the incoming row with the
/// returned columns; otherwise (a unit subquery, for its writes) the
/// incoming row passes on once.
pub const CallSubquery = struct {
    clauses: []Clause,
    /// UNION [ALL] and the queries after it, inside the braces.
    unions: []UnionBranch,
    location: SourceLocation,
};

/// `FOREACH (variable IN list | clauses)`: the updating clauses run once
/// for each element, and each input row passes on once, unchanged.
pub const ForeachClause = struct {
    variable: []const u8,
    list: *Expression,
    /// CREATE, MERGE, SET, REMOVE, DELETE and FOREACH only.
    clauses: []Clause,
    location: SourceLocation,
};

/// `CALL procedure(args) [YIELD column [AS name], ... | YIELD *]`.
pub const CallClause = struct {
    /// The dotted name, as written: `db.labels`.
    procedure: []const u8,
    arguments: []*Expression,
    /// The columns asked for; null without YIELD.
    yields: ?[]YieldItem,
    /// `YIELD *`.
    yield_all: bool,
    location: SourceLocation,
};

pub const YieldItem = struct {
    column: []const u8,
    alias: ?[]const u8,
    location: SourceLocation,
};

// ============================================================================
// Clause Definitions
// ============================================================================

/// MATCH clause - pattern matching
pub const MatchClause = struct {
    patterns: []Pattern,
    location: SourceLocation,
    /// OPTIONAL MATCH: a row the patterns do not match is kept, with the
    /// variables they would have bound set to null.
    optional: bool = false,
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
    /// The item's expression as written in the query, without its alias.
    /// An unaliased item's result column is named after it.
    text: []const u8 = "",
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
    /// `NULLS FIRST` / `NULLS LAST`; without it nulls sort as the largest
    /// value (last ascending, first descending).
    nulls: ?NullsOrder = null,
};

pub const NullsOrder = enum { first, last };

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

/// CREATE clause - creates nodes and edges from patterns
pub const CreateClause = struct {
    patterns: []Pattern,
    location: SourceLocation,
};

/// DELETE clause - deletes nodes/edges
pub const DeleteClause = struct {
    detach: bool, // true for DETACH DELETE
    expressions: []*Expression, // Variables to delete (n, r, etc.)
    location: SourceLocation,
};

/// SET clause - modifies properties and labels
pub const SetClause = struct {
    items: []SetItem,
    location: SourceLocation,
};

/// Individual SET operation
pub const SetItem = union(enum) {
    /// SET n.prop = expr
    property: struct {
        target: *Expression, // Variable reference (n)
        property_name: []const u8,
        value: *Expression, // Value expression
    },
    /// SET n:Label or SET n:Label1:Label2
    labels: struct {
        target: *Expression, // Variable reference
        label_names: []const []const u8,
    },
    /// SET n = {map} (replace all)
    replace_properties: struct {
        target: *Expression,
        map: *Expression, // Map literal expression
    },
    /// SET n += {map} (merge)
    merge_properties: struct {
        target: *Expression,
        map: *Expression,
    },
};

/// REMOVE clause - removes properties and labels
pub const RemoveClause = struct {
    items: []RemoveItem,
    location: SourceLocation,
};

/// Individual REMOVE operation
pub const RemoveItem = union(enum) {
    /// REMOVE n.prop
    property: struct {
        target: *Expression, // Variable reference (n)
        property_name: []const u8,
    },
    /// REMOVE n:Label or REMOVE n:Label1:Label2
    labels: struct {
        target: *Expression, // Variable reference
        label_names: []const []const u8,
    },
};

/// WITH clause - projection that feeds into the next query part
pub const WithClause = struct {
    distinct: bool,
    items: []ReturnItem,
    where: ?*Expression, // optional WHERE filter after WITH
    location: SourceLocation,
};

/// MERGE clause - match or create a pattern
pub const MergeClause = struct {
    pattern: Pattern,
    on_create: ?[]SetItem, // ON CREATE SET ...
    on_match: ?[]SetItem, // ON MATCH SET ...
    location: SourceLocation,
};

/// UNWIND clause - expand a list into rows
pub const UnwindClause = struct {
    expression: *Expression,
    variable: []const u8, // AS variable
    location: SourceLocation,
};

// ============================================================================
// Pattern Definitions
// ============================================================================

/// A graph pattern (sequence of nodes and edges)
pub const Pattern = struct {
    elements: []PatternElement,
    /// `p` in `p = (a)-->(b)`: a variable bound to the whole path.
    path_variable: ?[]const u8 = null,
    path_location: SourceLocation = .{ .line = 0, .column = 0 },
    /// `shortestPath(...)` or `allShortestPaths(...)` around the pattern.
    shortest: Shortest = .none,

    pub const Shortest = enum { none, one, all };
};

/// Element in a pattern (node or edge)
pub const PatternElement = union(enum) {
    node: *NodePattern,
    edge: *EdgePattern,
};

/// Node pattern: (variable:Label:Label {props})
pub const NodePattern = struct {
    variable: ?[]const u8,
    /// Labels the node must all have: `(n:A:B)`, and the `&`-joined plain
    /// labels of a label expression.
    labels: [][]const u8,
    properties: ?[]PropertyEntry,
    location: SourceLocation,
    /// The rest of a label expression, which the labels above do not say:
    /// `(n:A|B)`, `(n:!A)`, `(n:%)`. The node must also satisfy it.
    label_expr: ?*LabelExpr = null,
};

/// A label expression: `A`, `%` (any label), `!e`, `e & e`, `e | e`.
pub const LabelExpr = union(enum) {
    label: []const u8,
    wildcard,
    not_: *LabelExpr,
    and_: [2]*LabelExpr,
    or_: [2]*LabelExpr,

    pub fn eql(a: *const LabelExpr, b: *const LabelExpr) bool {
        if (std.meta.activeTag(a.*) != std.meta.activeTag(b.*)) return false;
        return switch (a.*) {
            .label => |l| std.mem.eql(u8, l, b.label),
            .wildcard => true,
            .not_ => |e| e.eql(b.not_),
            .and_ => |pair| pair[0].eql(b.and_[0]) and pair[1].eql(b.and_[1]),
            .or_ => |pair| pair[0].eql(b.or_[0]) and pair[1].eql(b.or_[1]),
        };
    }
};

/// `x:A`, `x:A|B`, `x:!A`: whether a node has the labels, or a
/// relationship the type, that the label expression asks for.
pub const LabelTest = struct {
    subject: *Expression,
    labels: *LabelExpr,
    location: SourceLocation,
};

/// Range quantifier for variable-length paths: *min..max
pub const RangeQuantifier = struct {
    /// Minimum hops (default 1)
    min_hops: u32,
    /// Maximum hops (null means unbounded)
    max_hops: ?u32,
};

/// Edge pattern: -[variable:TYPE*min..max {props}]->
pub const EdgePattern = struct {
    variable: ?[]const u8,
    types: [][]const u8,
    direction: EdgeDirection,
    properties: ?[]PropertyEntry,
    /// Optional range quantifier for variable-length paths
    quantifier: ?RangeQuantifier,
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

    // Subscripts
    index: *IndexExpr,
    slice: *SliceExpr,

    // Conditionals
    case_expr: *CaseExpr,

    // Iteration over a list, with a variable of its own
    comprehension: *ListComprehension,
    reduce: *ReduceExpr,

    // Patterns matched from inside an expression
    subquery: *PatternSubquery,

    // `n {.name, key: value, .*}`
    map_projection: *MapProjection,

    // `n:A|B`, `r:KNOWS`
    label_test: *LabelTest,

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
            .index => |i| i.location,
            .slice => |s| s.location,
            .case_expr => |c| c.location,
            .comprehension => |c| c.location,
            .reduce => |r| r.location,
            .subquery => |s| s.location,
            .map_projection => |m| m.location,
            .label_test => |t| t.location,
        };
    }
};

/// `[x IN list WHERE cond | value]`, and the quantifiers
/// `all|any|none|single(x IN list WHERE cond)`, which share its shape.
/// `variable` is bound to each element in turn, only inside `where` and
/// `map`.
pub const ListComprehension = struct {
    kind: Kind,
    variable: []const u8,
    list: *Expression,
    where: ?*Expression,
    /// The value for each element kept; the element itself when absent.
    /// Only a list comprehension has one.
    map: ?*Expression,
    location: SourceLocation,

    pub const Kind = enum { list, all, any, none, single };
};

/// `reduce(acc = initial, x IN list | expression)`: `expression` computes
/// the next `acc` from it and each element `x` in turn.
pub const ReduceExpr = struct {
    accumulator: []const u8,
    initial: *Expression,
    variable: []const u8,
    list: *Expression,
    expression: *Expression,
    location: SourceLocation,
};

/// Patterns matched for each row an expression is evaluated on, with the
/// row's variables bound: `EXISTS { MATCH patterns WHERE cond }` and a
/// pattern used as a predicate, `COUNT { ... }`, and a pattern
/// comprehension `[pattern WHERE cond | value]`. Variables the patterns
/// introduce are visible only inside it.
pub const PatternSubquery = struct {
    kind: Kind,
    patterns: []Pattern,
    where: ?*Expression,
    /// The value per match of a pattern comprehension.
    map: ?*Expression,
    location: SourceLocation,
    /// A whole query in the braces, `EXISTS { MATCH ... WITH ... RETURN ... }`,
    /// in place of the patterns and WHERE. It sees the variables around
    /// it. COLLECT { } always has one, ending in a RETURN of one column.
    body: ?*CallSubquery = null,

    pub const Kind = enum { exists, count, comprehension, collect };

    /// What the subquery is called in messages.
    pub fn name(self: *const PatternSubquery) []const u8 {
        return switch (self.kind) {
            .exists => "EXISTS { ... }",
            .count => "COUNT { ... }",
            .collect => "COLLECT { ... }",
            .comprehension => "a pattern comprehension",
        };
    }
};

/// `object[index]`: a list element by position, or a map entry, node or
/// relationship property by name.
pub const IndexExpr = struct {
    object: *Expression,
    index: *Expression,
    location: SourceLocation,
};

/// `CASE WHEN cond THEN value ... ELSE value END`, or with a subject,
/// `CASE subject WHEN value THEN value ... END`.
pub const CaseExpr = struct {
    /// The value the simple form compares each WHEN against; null for the
    /// searched form, whose WHENs are conditions.
    subject: ?*Expression,
    branches: []CaseBranch,
    /// The value when no branch applies; null when there is no ELSE.
    else_expr: ?*Expression,
    location: SourceLocation,
};

pub const CaseBranch = struct {
    when: *Expression,
    then: *Expression,
};

/// `list[from..to]`: the elements from `from` up to but not including `to`.
/// Either bound may be left out.
pub const SliceExpr = struct {
    object: *Expression,
    from: ?*Expression,
    to: ?*Expression,
    location: SourceLocation,
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
    distinct: bool = false,
    /// Written as `count(*)`. It takes no arguments and counts rows, the same
    /// as `count()`; the flag only keeps the spelling for the column name.
    star: bool = false,
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

/// `n {.name, key: value, x, .*}`: a map built from a node, relationship
/// or map. The parser turns `.name` into the entry `name: n.name` and `x`
/// into `x: x`; `.*` is an entry with no value, standing for every
/// property. A later entry replaces an earlier one with the same key.
pub const MapProjection = struct {
    /// The variable projected, as a variable expression.
    subject: *Expression,
    entries: []Entry,
    location: SourceLocation,

    pub const Entry = struct {
        key: []const u8,
        /// Null for `.*`.
        value: ?*Expression,
    };
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
