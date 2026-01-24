//! Cypher query parser for Lattice database.
//!
//! Parses Cypher queries with vector and full-text search extensions.
//! Uses recursive descent with Pratt parsing for expressions.

const std = @import("std");
const Allocator = std.mem.Allocator;

const ast = @import("ast.zig");
const lexer_mod = @import("lexer.zig");
const Lexer = lexer_mod.Lexer;

/// Query type classification
pub const QueryType = enum {
    /// MATCH pattern query
    match,
    /// CREATE nodes/edges
    create,
    /// MERGE (create if not exists)
    merge,
    /// DELETE nodes/edges
    delete,
    /// SET properties
    set,
    /// REMOVE properties/labels
    remove,
    /// RETURN results
    return_,
    /// WITH clause
    with,
    /// UNWIND list
    unwind,
    /// CALL procedure
    call,
};

/// AST node types
pub const AstNodeType = enum {
    /// Query root
    query,
    /// MATCH clause
    match_clause,
    /// WHERE clause
    where_clause,
    /// RETURN clause
    return_clause,
    /// CREATE clause
    create_clause,
    /// Node pattern (n:Label {props})
    node_pattern,
    /// Edge pattern -[r:TYPE]->
    edge_pattern,
    /// Property access (n.property)
    property_access,
    /// Binary expression (a + b)
    binary_expr,
    /// Unary expression (!a)
    unary_expr,
    /// Function call
    function_call,
    /// Literal value
    literal,
    /// Parameter ($param)
    parameter,
    /// Variable reference
    variable,
    /// List expression [a, b, c]
    list_expr,
    /// Map expression {a: 1, b: 2}
    map_expr,
    /// Vector distance operator (<=>)
    vector_distance,
    /// Full-text search operator (@@)
    fts_match,
    /// ORDER BY clause
    order_by,
    /// LIMIT clause
    limit,
    /// SKIP clause
    skip,
};

/// Binary operators
pub const BinaryOp = enum {
    // Comparison
    eq, // =
    neq, // <>
    lt, // <
    lte, // <=
    gt, // >
    gte, // >=
    // Logical
    and_,
    or_,
    xor,
    // Arithmetic
    add,
    sub,
    mul,
    div,
    mod,
    pow,
    // String
    contains,
    starts_with,
    ends_with,
    regex_match,
    // List
    in_,
    // Special
    vector_distance, // <=>
    fts_match, // @@
};

/// Unary operators
pub const UnaryOp = enum {
    not,
    neg,
    is_null,
    is_not_null,
};

/// Direction for edges
pub const EdgeDirection = enum {
    outgoing, // -->
    incoming, // <--
    both, // --
};

/// AST node (simplified for type stubs)
pub const AstNode = struct {
    node_type: AstNodeType,
    // In full implementation: children, value, position, etc.
};

/// Parse result
pub const ParseResult = struct {
    root: ?*AstNode,
    errors: []const ParseError,
};

/// Parse error
pub const ParseError = struct {
    message: []const u8,
    line: u32,
    column: u32,
    length: u32,
};

/// Token from lexer
pub const Token = struct {
    token_type: TokenType,
    text: []const u8,
    line: u32,
    column: u32,
};

/// Token types for lexer
pub const TokenType = enum {
    // Keywords
    kw_match,
    kw_where,
    kw_return,
    kw_create,
    kw_delete,
    kw_detach,
    kw_set,
    kw_remove,
    kw_merge,
    kw_with,
    kw_unwind,
    kw_on,
    kw_as,
    kw_order,
    kw_by,
    kw_limit,
    kw_skip,
    kw_and,
    kw_or,
    kw_not,
    kw_xor,
    kw_null,
    kw_true,
    kw_false,
    kw_in,
    kw_is,
    kw_contains,
    kw_starts,
    kw_ends,
    kw_distinct,
    kw_asc,
    kw_desc,
    // Literals
    integer,
    float,
    string,
    // Identifiers
    identifier,
    parameter,
    // Operators
    eq,
    neq,
    lt,
    lte,
    gt,
    gte,
    plus,
    plus_equal, // +=
    minus,
    star,
    slash,
    percent,
    caret,
    vector_distance, // <=>
    fts_match, // @@
    // Punctuation
    lparen,
    rparen,
    lbracket,
    rbracket,
    lbrace,
    rbrace,
    colon,
    comma,
    dot,
    dotdot, // .. for range quantifiers
    pipe,
    arrow_right,
    arrow_left,
    dash,
    // Special
    eof,
    invalid,
};

// ============================================================================
// Parser Implementation
// ============================================================================

/// Parser for Cypher queries
pub const Parser = struct {
    allocator: Allocator,
    arena: std.heap.ArenaAllocator,
    lexer: Lexer,
    current: Token,
    previous: Token,
    errors: std.ArrayList(ParseError),
    had_error: bool,
    panic_mode: bool,
    /// Tracks if any errors were dropped due to OOM
    errors_dropped: bool,
    /// When false, the arena is owned externally (e.g., by the query cache)
    /// and deinit will not free it.
    owns_arena: bool = true,

    const Self = @This();

    /// Initialize parser with source query
    pub fn init(allocator: Allocator, source: []const u8) Self {
        const arena = std.heap.ArenaAllocator.init(allocator);
        return .{
            .allocator = allocator,
            .arena = arena,
            .lexer = Lexer.init(source),
            .current = undefined,
            .previous = undefined,
            .errors = .empty,
            .had_error = false,
            .panic_mode = false,
            .errors_dropped = false,
            .owns_arena = true,
        };
    }

    /// Initialize parser with an externally-owned arena.
    /// The caller retains ownership of the arena; deinit will not free it.
    /// Use this when the AST should outlive the parser (e.g., for caching).
    pub fn initWithArena(allocator: Allocator, source: []const u8, arena: std.heap.ArenaAllocator) Self {
        return .{
            .allocator = allocator,
            .arena = arena,
            .lexer = Lexer.init(source),
            .current = undefined,
            .previous = undefined,
            .errors = .empty,
            .had_error = false,
            .panic_mode = false,
            .errors_dropped = false,
            .owns_arena = false,
        };
    }

    /// Free parser resources
    pub fn deinit(self: *Self) void {
        if (self.owns_arena) {
            self.arena.deinit();
        }
        self.errors.deinit(self.allocator);
    }

    /// Parse the query and return result
    pub fn parse(self: *Self) ParserResult {
        // Prime the parser with first token
        self.advance();

        var clauses: std.ArrayList(ast.Clause) = .empty;

        while (!self.check(.eof)) {
            if (self.parseClause()) |clause| {
                clauses.append(self.arena.allocator(), clause) catch {
                    self.errorAtCurrent("Out of memory");
                    break;
                };
            } else {
                self.synchronize();
            }
        }

        // Fail if we had errors or if errors were dropped due to OOM
        if (self.had_error or self.errors_dropped) {
            return .{
                .query = null,
                .errors = self.errors.items,
                .errors_dropped = self.errors_dropped,
            };
        }

        const query = self.arena.allocator().create(ast.Query) catch {
            return .{
                .query = null,
                .errors = self.errors.items,
                .errors_dropped = self.errors_dropped,
            };
        };
        query.* = .{
            .clauses = clauses.toOwnedSlice(self.arena.allocator()) catch &[_]ast.Clause{},
            .allocator = self.arena.allocator(),
        };

        return .{
            .query = query,
            .errors = self.errors.items,
            .errors_dropped = self.errors_dropped,
        };
    }

    // ========================================================================
    // Clause Parsing
    // ========================================================================

    fn parseClause(self: *Self) ?ast.Clause {
        if (self.match(.kw_match)) {
            return self.parseMatchClause();
        } else if (self.match(.kw_where)) {
            return self.parseWhereClause();
        } else if (self.match(.kw_return)) {
            return self.parseReturnClause();
        } else if (self.match(.kw_order)) {
            return self.parseOrderByClause();
        } else if (self.match(.kw_limit)) {
            return self.parseLimitClause();
        } else if (self.match(.kw_skip)) {
            return self.parseSkipClause();
        } else if (self.match(.kw_create)) {
            return self.parseCreateClause();
        } else if (self.match(.kw_detach)) {
            // DETACH DELETE
            if (!self.consume(.kw_delete, "Expected DELETE after DETACH")) {
                return null;
            }
            return self.parseDeleteClause(true);
        } else if (self.match(.kw_delete)) {
            return self.parseDeleteClause(false);
        } else if (self.match(.kw_set)) {
            return self.parseSetClause();
        } else if (self.match(.kw_remove)) {
            return self.parseRemoveClause();
        } else if (self.match(.kw_with)) {
            return self.parseWithClause();
        } else if (self.match(.kw_merge)) {
            return self.parseMergeClause();
        } else if (self.match(.kw_unwind)) {
            return self.parseUnwindClause();
        } else {
            self.errorAtCurrent("Expected MATCH, WHERE, RETURN, CREATE, DELETE, SET, REMOVE, WITH, MERGE, UNWIND, ORDER BY, LIMIT, or SKIP");
            return null;
        }
    }

    fn parseMatchClause(self: *Self) ?ast.Clause {
        const loc = self.previousLocation();
        var patterns: std.ArrayList(ast.Pattern) = .empty;

        // Parse first pattern
        if (self.parsePattern()) |pattern| {
            patterns.append(self.arena.allocator(), pattern) catch return null;
        } else {
            return null;
        }

        // Parse additional comma-separated patterns
        while (self.match(.comma)) {
            if (self.parsePattern()) |pattern| {
                patterns.append(self.arena.allocator(), pattern) catch return null;
            } else {
                return null;
            }
        }

        const clause = self.arena.allocator().create(ast.MatchClause) catch return null;
        clause.* = .{
            .patterns = patterns.toOwnedSlice(self.arena.allocator()) catch return null,
            .location = loc,
        };

        return .{ .match = clause };
    }

    fn parseWhereClause(self: *Self) ?ast.Clause {
        const loc = self.previousLocation();

        if (self.parseExpression()) |expr| {
            const clause = self.arena.allocator().create(ast.WhereClause) catch return null;
            clause.* = .{
                .condition = expr,
                .location = loc,
            };
            return .{ .where = clause };
        }

        return null;
    }

    fn parseReturnClause(self: *Self) ?ast.Clause {
        const loc = self.previousLocation();
        const distinct = self.match(.kw_distinct);

        var items: std.ArrayList(ast.ReturnItem) = .empty;

        // Parse first return item
        if (self.parseReturnItem()) |item| {
            items.append(self.arena.allocator(), item) catch return null;
        } else {
            return null;
        }

        // Parse additional comma-separated items
        while (self.match(.comma)) {
            if (self.parseReturnItem()) |item| {
                items.append(self.arena.allocator(), item) catch return null;
            } else {
                return null;
            }
        }

        const clause = self.arena.allocator().create(ast.ReturnClause) catch return null;
        clause.* = .{
            .distinct = distinct,
            .items = items.toOwnedSlice(self.arena.allocator()) catch return null,
            .location = loc,
        };

        return .{ .return_ = clause };
    }

    fn parseReturnItem(self: *Self) ?ast.ReturnItem {
        const expr = self.parseExpression() orelse return null;
        var alias: ?[]const u8 = null;

        if (self.match(.kw_as)) {
            if (!self.check(.identifier)) {
                self.errorAtCurrent("Expected identifier after AS");
                return null;
            }
            alias = self.current.text;
            self.advance();
        }

        return .{
            .expression = expr,
            .alias = alias,
        };
    }

    fn parseOrderByClause(self: *Self) ?ast.Clause {
        // Expect BY after ORDER
        if (!self.consume(.kw_by, "Expected BY after ORDER")) {
            return null;
        }

        const loc = self.previousLocation();
        var items: std.ArrayList(ast.OrderItem) = .empty;

        // Parse first order item
        if (self.parseOrderItem()) |item| {
            items.append(self.arena.allocator(), item) catch return null;
        } else {
            return null;
        }

        // Parse additional comma-separated items
        while (self.match(.comma)) {
            if (self.parseOrderItem()) |item| {
                items.append(self.arena.allocator(), item) catch return null;
            } else {
                return null;
            }
        }

        const clause = self.arena.allocator().create(ast.OrderByClause) catch return null;
        clause.* = .{
            .items = items.toOwnedSlice(self.arena.allocator()) catch return null,
            .location = loc,
        };

        return .{ .order_by = clause };
    }

    fn parseOrderItem(self: *Self) ?ast.OrderItem {
        const expr = self.parseExpression() orelse return null;
        var descending = false;

        if (self.match(.kw_desc)) {
            descending = true;
        } else {
            _ = self.match(.kw_asc); // optional ASC
        }

        return .{
            .expression = expr,
            .descending = descending,
        };
    }

    fn parseLimitClause(self: *Self) ?ast.Clause {
        const loc = self.previousLocation();
        const expr = self.parseExpression() orelse return null;

        const clause = self.arena.allocator().create(ast.LimitClause) catch return null;
        clause.* = .{
            .count = expr,
            .location = loc,
        };

        return .{ .limit = clause };
    }

    fn parseSkipClause(self: *Self) ?ast.Clause {
        const loc = self.previousLocation();
        const expr = self.parseExpression() orelse return null;

        const clause = self.arena.allocator().create(ast.SkipClause) catch return null;
        clause.* = .{
            .count = expr,
            .location = loc,
        };

        return .{ .skip = clause };
    }

    fn parseCreateClause(self: *Self) ?ast.Clause {
        const loc = self.previousLocation();
        var patterns: std.ArrayList(ast.Pattern) = .empty;

        // Parse first pattern (reuse same pattern parsing as MATCH)
        if (self.parsePattern()) |pattern| {
            patterns.append(self.arena.allocator(), pattern) catch return null;
        } else {
            return null;
        }

        // Parse additional comma-separated patterns
        while (self.match(.comma)) {
            if (self.parsePattern()) |pattern| {
                patterns.append(self.arena.allocator(), pattern) catch return null;
            } else {
                return null;
            }
        }

        const clause = self.arena.allocator().create(ast.CreateClause) catch return null;
        clause.* = .{
            .patterns = patterns.toOwnedSlice(self.arena.allocator()) catch return null,
            .location = loc,
        };

        return .{ .create = clause };
    }

    fn parseDeleteClause(self: *Self, detach: bool) ?ast.Clause {
        const loc = self.previousLocation();
        var expressions: std.ArrayList(*ast.Expression) = .empty;

        // Parse first expression (variable to delete)
        if (self.parseExpression()) |expr| {
            expressions.append(self.arena.allocator(), expr) catch return null;
        } else {
            return null;
        }

        // Parse additional comma-separated expressions
        while (self.match(.comma)) {
            if (self.parseExpression()) |expr| {
                expressions.append(self.arena.allocator(), expr) catch return null;
            } else {
                return null;
            }
        }

        const clause = self.arena.allocator().create(ast.DeleteClause) catch return null;
        clause.* = .{
            .detach = detach,
            .expressions = expressions.toOwnedSlice(self.arena.allocator()) catch return null,
            .location = loc,
        };

        return .{ .delete = clause };
    }

    fn parseSetClause(self: *Self) ?ast.Clause {
        const loc = self.previousLocation();
        var items: std.ArrayList(ast.SetItem) = .empty;

        // Parse first SET item
        if (self.parseSetItem()) |item| {
            items.append(self.arena.allocator(), item) catch return null;
        } else {
            return null;
        }

        // Parse additional comma-separated items
        while (self.match(.comma)) {
            if (self.parseSetItem()) |item| {
                items.append(self.arena.allocator(), item) catch return null;
            } else {
                return null;
            }
        }

        const clause = self.arena.allocator().create(ast.SetClause) catch return null;
        clause.* = .{
            .items = items.toOwnedSlice(self.arena.allocator()) catch return null,
            .location = loc,
        };

        return .{ .set = clause };
    }

    fn parseSetItem(self: *Self) ?ast.SetItem {
        // Parse variable name first
        if (!self.check(.identifier)) {
            self.errorAtCurrent("Expected variable name in SET");
            return null;
        }
        const var_name = self.current.text;
        const var_loc = self.currentLocation();
        self.advance();

        // Create variable expression
        const var_expr = self.arena.allocator().create(ast.Expression) catch return null;
        var_expr.* = .{ .variable = .{ .name = var_name, .location = var_loc } };

        // Determine the type of SET operation based on what follows
        if (self.match(.colon)) {
            // SET n:Label - label assignment
            return self.parseSetLabels(var_expr);
        } else if (self.match(.plus_equal)) {
            // SET n += {map} - merge properties
            return self.parseSetMerge(var_expr);
        } else if (self.match(.eq)) {
            // SET n = {map} - replace all properties
            return self.parseSetReplace(var_expr);
        } else if (self.match(.dot)) {
            // SET n.prop = value - property assignment
            if (!self.check(.identifier)) {
                self.errorAtCurrent("Expected property name after '.'");
                return null;
            }
            const prop_name = self.current.text;
            self.advance();

            if (!self.consume(.eq, "Expected '=' after property name in SET")) {
                return null;
            }

            const value = self.parseExpression() orelse return null;
            return .{ .property = .{
                .target = var_expr,
                .property_name = prop_name,
                .value = value,
            } };
        }

        self.errorAtCurrent("Expected '.', ':', '=', or '+=' after variable in SET");
        return null;
    }

    fn parseSetLabels(self: *Self, target: *ast.Expression) ?ast.SetItem {
        // Already consumed first colon, parse labels
        var labels: std.ArrayList([]const u8) = .empty;

        // First label
        if (!self.check(.identifier)) {
            self.errorAtCurrent("Expected label name after ':'");
            return null;
        }
        labels.append(self.arena.allocator(), self.current.text) catch return null;
        self.advance();

        // Additional labels (SET n:Label1:Label2)
        while (self.match(.colon)) {
            if (!self.check(.identifier)) {
                self.errorAtCurrent("Expected label name after ':'");
                return null;
            }
            labels.append(self.arena.allocator(), self.current.text) catch return null;
            self.advance();
        }

        return .{ .labels = .{
            .target = target,
            .label_names = labels.toOwnedSlice(self.arena.allocator()) catch return null,
        } };
    }

    fn parseSetMerge(self: *Self, target: *ast.Expression) ?ast.SetItem {
        // Already consumed +=, expect map expression
        const map = self.parseExpression() orelse return null;
        return .{ .merge_properties = .{
            .target = target,
            .map = map,
        } };
    }

    fn parseSetReplace(self: *Self, target: *ast.Expression) ?ast.SetItem {
        // Already consumed =, expect map expression
        const map = self.parseExpression() orelse return null;
        return .{ .replace_properties = .{
            .target = target,
            .map = map,
        } };
    }

    fn parseRemoveClause(self: *Self) ?ast.Clause {
        const loc = self.previousLocation();
        var items: std.ArrayList(ast.RemoveItem) = .empty;

        // Parse first REMOVE item
        if (self.parseRemoveItem()) |item| {
            items.append(self.arena.allocator(), item) catch return null;
        } else {
            return null;
        }

        // Parse additional comma-separated items
        while (self.match(.comma)) {
            if (self.parseRemoveItem()) |item| {
                items.append(self.arena.allocator(), item) catch return null;
            } else {
                return null;
            }
        }

        const clause = self.arena.allocator().create(ast.RemoveClause) catch return null;
        clause.* = .{
            .items = items.toOwnedSlice(self.arena.allocator()) catch return null,
            .location = loc,
        };

        return .{ .remove = clause };
    }

    fn parseRemoveItem(self: *Self) ?ast.RemoveItem {
        // Parse variable name first
        if (!self.check(.identifier)) {
            self.errorAtCurrent("Expected variable name in REMOVE");
            return null;
        }
        const var_name = self.current.text;
        const var_loc = self.currentLocation();
        self.advance();

        // Create variable expression
        const var_expr = self.arena.allocator().create(ast.Expression) catch return null;
        var_expr.* = .{ .variable = .{ .name = var_name, .location = var_loc } };

        // Determine the type of REMOVE operation based on what follows
        if (self.match(.colon)) {
            // REMOVE n:Label - label removal
            return self.parseRemoveLabels(var_expr);
        } else if (self.match(.dot)) {
            // REMOVE n.prop - property removal
            if (!self.check(.identifier)) {
                self.errorAtCurrent("Expected property name after '.'");
                return null;
            }
            const prop_name = self.current.text;
            self.advance();

            return .{ .property = .{
                .target = var_expr,
                .property_name = prop_name,
            } };
        }

        self.errorAtCurrent("Expected '.' or ':' after variable in REMOVE");
        return null;
    }

    fn parseRemoveLabels(self: *Self, target: *ast.Expression) ?ast.RemoveItem {
        // Already consumed first colon, parse labels
        var labels: std.ArrayList([]const u8) = .empty;

        // First label
        if (!self.check(.identifier)) {
            self.errorAtCurrent("Expected label name after ':'");
            return null;
        }
        labels.append(self.arena.allocator(), self.current.text) catch return null;
        self.advance();

        // Additional labels (REMOVE n:Label1:Label2)
        while (self.match(.colon)) {
            if (!self.check(.identifier)) {
                self.errorAtCurrent("Expected label name after ':'");
                return null;
            }
            labels.append(self.arena.allocator(), self.current.text) catch return null;
            self.advance();
        }

        return .{ .labels = .{
            .target = target,
            .label_names = labels.toOwnedSlice(self.arena.allocator()) catch return null,
        } };
    }

    // ========================================================================
    // WITH, MERGE, UNWIND Parsing
    // ========================================================================

    fn parseWithClause(self: *Self) ?ast.Clause {
        const loc = self.previousLocation();
        const distinct = self.match(.kw_distinct);

        var items: std.ArrayList(ast.ReturnItem) = .empty;

        // Parse first WITH item (same syntax as RETURN items)
        if (self.parseReturnItem()) |item| {
            items.append(self.arena.allocator(), item) catch return null;
        } else {
            return null;
        }

        // Parse additional comma-separated items
        while (self.match(.comma)) {
            if (self.parseReturnItem()) |item| {
                items.append(self.arena.allocator(), item) catch return null;
            } else {
                return null;
            }
        }

        // Optional WHERE after WITH
        var where_expr: ?*ast.Expression = null;
        if (self.match(.kw_where)) {
            where_expr = self.parseExpression();
            if (where_expr == null) return null;
        }

        const clause = self.arena.allocator().create(ast.WithClause) catch return null;
        clause.* = .{
            .distinct = distinct,
            .items = items.toOwnedSlice(self.arena.allocator()) catch return null,
            .where = where_expr,
            .location = loc,
        };

        return .{ .with = clause };
    }

    fn parseMergeClause(self: *Self) ?ast.Clause {
        const loc = self.previousLocation();

        // Parse a single pattern (MERGE takes exactly one pattern)
        const pattern = self.parsePattern() orelse return null;

        // Parse optional ON CREATE SET / ON MATCH SET
        var on_create: ?[]ast.SetItem = null;
        var on_match: ?[]ast.SetItem = null;

        while (self.match(.kw_on)) {
            if (self.match(.kw_create)) {
                if (!self.consume(.kw_set, "Expected SET after ON CREATE")) return null;
                on_create = self.parseMergeSetItems();
                if (on_create == null) return null;
            } else if (self.match(.kw_match)) {
                if (!self.consume(.kw_set, "Expected SET after ON MATCH")) return null;
                on_match = self.parseMergeSetItems();
                if (on_match == null) return null;
            } else {
                self.errorAtCurrent("Expected CREATE or MATCH after ON");
                return null;
            }
        }

        const clause = self.arena.allocator().create(ast.MergeClause) catch return null;
        clause.* = .{
            .pattern = pattern,
            .on_create = on_create,
            .on_match = on_match,
            .location = loc,
        };

        return .{ .merge = clause };
    }

    /// Parse comma-separated SET items for MERGE's ON CREATE/ON MATCH SET
    fn parseMergeSetItems(self: *Self) ?[]ast.SetItem {
        var items: std.ArrayList(ast.SetItem) = .empty;

        if (self.parseSetItem()) |item| {
            items.append(self.arena.allocator(), item) catch return null;
        } else {
            return null;
        }

        while (self.match(.comma)) {
            if (self.parseSetItem()) |item| {
                items.append(self.arena.allocator(), item) catch return null;
            } else {
                return null;
            }
        }

        return items.toOwnedSlice(self.arena.allocator()) catch null;
    }

    fn parseUnwindClause(self: *Self) ?ast.Clause {
        const loc = self.previousLocation();

        // Parse the expression to unwind
        const expr = self.parseExpression() orelse return null;

        // Expect AS variable
        if (!self.consume(.kw_as, "Expected AS after UNWIND expression")) return null;

        if (!self.check(.identifier)) {
            self.errorAtCurrent("Expected variable name after AS");
            return null;
        }
        const variable = self.current.text;
        self.advance();

        const clause = self.arena.allocator().create(ast.UnwindClause) catch return null;
        clause.* = .{
            .expression = expr,
            .variable = variable,
            .location = loc,
        };

        return .{ .unwind = clause };
    }

    // ========================================================================
    // Pattern Parsing
    // ========================================================================

    fn parsePattern(self: *Self) ?ast.Pattern {
        var elements: std.ArrayList(ast.PatternElement) = .empty;

        // Pattern must start with a node
        if (self.parseNodePattern()) |node| {
            const node_ptr = self.arena.allocator().create(ast.NodePattern) catch return null;
            node_ptr.* = node;
            elements.append(self.arena.allocator(), .{ .node = node_ptr }) catch return null;
        } else {
            return null;
        }

        // Parse alternating edges and nodes
        while (self.checkEdgeStart()) {
            if (self.parseEdgePattern()) |edge| {
                const edge_ptr = self.arena.allocator().create(ast.EdgePattern) catch return null;
                edge_ptr.* = edge;
                elements.append(self.arena.allocator(), .{ .edge = edge_ptr }) catch return null;
            } else {
                return null;
            }

            // Edge must be followed by a node
            if (self.parseNodePattern()) |node| {
                const node_ptr = self.arena.allocator().create(ast.NodePattern) catch return null;
                node_ptr.* = node;
                elements.append(self.arena.allocator(), .{ .node = node_ptr }) catch return null;
            } else {
                self.errorAtCurrent("Expected node pattern after relationship");
                return null;
            }
        }

        return .{
            .elements = elements.toOwnedSlice(self.arena.allocator()) catch return null,
        };
    }

    fn checkEdgeStart(self: *Self) bool {
        return self.check(.dash) or self.check(.arrow_left);
    }

    fn parseNodePattern(self: *Self) ?ast.NodePattern {
        if (!self.consume(.lparen, "Expected '(' to start node pattern")) {
            return null;
        }

        const loc = self.previousLocation();
        var variable: ?[]const u8 = null;
        var labels: std.ArrayList([]const u8) = .empty;
        var properties: ?[]ast.PropertyEntry = null;

        // Optional variable
        if (self.check(.identifier)) {
            variable = self.current.text;
            self.advance();
        }

        // Optional labels
        while (self.match(.colon)) {
            if (!self.check(.identifier)) {
                self.errorAtCurrent("Expected label after ':'");
                return null;
            }
            labels.append(self.arena.allocator(), self.current.text) catch return null;
            self.advance();
        }

        // Optional properties
        if (self.check(.lbrace)) {
            properties = self.parsePropertyMap();
        }

        if (!self.consume(.rparen, "Expected ')' to end node pattern")) {
            return null;
        }

        return .{
            .variable = variable,
            .labels = labels.toOwnedSlice(self.arena.allocator()) catch return null,
            .properties = properties,
            .location = loc,
        };
    }

    fn parseEdgePattern(self: *Self) ?ast.EdgePattern {
        const loc = self.currentLocation();
        var direction: ast.EdgeDirection = .both;
        var left_arrow = false;

        // Parse left side: <- or -
        if (self.match(.arrow_left)) {
            left_arrow = true;
            direction = .incoming;
        } else if (!self.match(.dash)) {
            self.errorAtCurrent("Expected '-' or '<-' to start relationship");
            return null;
        }

        // Parse optional relationship details: [variable:TYPE*min..max {props}]
        var variable: ?[]const u8 = null;
        var types_list: std.ArrayList([]const u8) = .empty;
        var properties: ?[]ast.PropertyEntry = null;
        var quantifier: ?ast.RangeQuantifier = null;

        if (self.match(.lbracket)) {
            // Optional variable
            if (self.check(.identifier)) {
                variable = self.current.text;
                self.advance();
            }

            // Optional types
            while (self.match(.colon)) {
                if (!self.check(.identifier)) {
                    self.errorAtCurrent("Expected relationship type after ':'");
                    return null;
                }
                types_list.append(self.arena.allocator(), self.current.text) catch return null;
                self.advance();

                // Handle multiple types with |
                while (self.match(.pipe)) {
                    if (!self.check(.identifier)) {
                        self.errorAtCurrent("Expected relationship type after '|'");
                        return null;
                    }
                    types_list.append(self.arena.allocator(), self.current.text) catch return null;
                    self.advance();
                }
            }

            // Optional range quantifier: *min..max
            if (self.match(.star)) {
                quantifier = self.parseRangeQuantifier();
            }

            // Optional properties
            if (self.check(.lbrace)) {
                properties = self.parsePropertyMap();
            }

            if (!self.consume(.rbracket, "Expected ']' to end relationship")) {
                return null;
            }
        }

        // Parse right side: -> or -
        if (self.match(.arrow_right)) {
            if (left_arrow) {
                direction = .both; // <-[]->, but we treat as undirected
            } else {
                direction = .outgoing;
            }
        } else if (self.match(.dash)) {
            // Already handled direction from left side
        } else {
            self.errorAtCurrent("Expected '->' or '-' to end relationship");
            return null;
        }

        return .{
            .variable = variable,
            .types = types_list.toOwnedSlice(self.arena.allocator()) catch return null,
            .direction = direction,
            .properties = properties,
            .quantifier = quantifier,
            .location = loc,
        };
    }

    /// Parse a range quantifier: *, *n, *n..m, *n.., *..m
    fn parseRangeQuantifier(self: *Self) ast.RangeQuantifier {
        var min_hops: u32 = 1;
        var max_hops: ?u32 = null;

        // Check for min value
        if (self.check(.integer)) {
            min_hops = @intCast(std.fmt.parseInt(u32, self.current.text, 10) catch 1);
            self.advance();

            // Check for range: ..max or just ..
            if (self.match(.dotdot)) {
                if (self.check(.integer)) {
                    max_hops = @intCast(std.fmt.parseInt(u32, self.current.text, 10) catch min_hops);
                    self.advance();
                }
                // else max_hops stays null (unbounded)
            } else {
                // Exactly n hops (e.g., *3 means exactly 3)
                max_hops = min_hops;
            }
        } else if (self.match(.dotdot)) {
            // *..max format (min defaults to 1)
            if (self.check(.integer)) {
                max_hops = @intCast(std.fmt.parseInt(u32, self.current.text, 10) catch 1);
                self.advance();
            }
            // else unbounded: *.. (same as *)
        }
        // else just * with no numbers (min=1, max=null means unbounded)

        return .{
            .min_hops = min_hops,
            .max_hops = max_hops,
        };
    }

    fn parsePropertyMap(self: *Self) ?[]ast.PropertyEntry {
        if (!self.consume(.lbrace, "Expected '{' to start property map")) {
            return null;
        }

        var entries: std.ArrayList(ast.PropertyEntry) = .empty;

        if (!self.check(.rbrace)) {
            // Parse first entry
            if (self.parsePropertyEntry()) |entry| {
                entries.append(self.arena.allocator(), entry) catch return null;
            } else {
                return null;
            }

            // Parse additional entries
            while (self.match(.comma)) {
                if (self.parsePropertyEntry()) |entry| {
                    entries.append(self.arena.allocator(), entry) catch return null;
                } else {
                    return null;
                }
            }
        }

        if (!self.consume(.rbrace, "Expected '}' to end property map")) {
            return null;
        }

        return entries.toOwnedSlice(self.arena.allocator()) catch null;
    }

    fn parsePropertyEntry(self: *Self) ?ast.PropertyEntry {
        if (!self.check(.identifier)) {
            self.errorAtCurrent("Expected property name");
            return null;
        }
        const key = self.current.text;
        self.advance();

        if (!self.consume(.colon, "Expected ':' after property name")) {
            return null;
        }

        const value = self.parseExpression() orelse return null;

        return .{
            .key = key,
            .value = value,
        };
    }

    // ========================================================================
    // Expression Parsing (Pratt Parser)
    // ========================================================================

    const Precedence = enum(u8) {
        none,
        or_,        // OR
        xor,        // XOR
        and_,       // AND
        not,        // NOT (prefix)
        equality,   // = <> IN CONTAINS STARTS WITH ENDS WITH
        comparison, // < <= > >= <=> @@
        term,       // + -
        factor,     // * / %
        power,      // ^
        unary,      // - + (prefix)
        call,       // ()
        primary,    // . (property access)
    };

    fn parseExpression(self: *Self) ?*ast.Expression {
        return self.parsePrecedence(.or_);
    }

    fn parsePrecedence(self: *Self, min_prec: Precedence) ?*ast.Expression {
        // Parse prefix expression
        var left = self.parsePrefixExpr() orelse return null;

        // Parse infix expressions
        while (true) {
            const prec = self.getInfixPrecedence();
            if (@intFromEnum(prec) < @intFromEnum(min_prec)) break;

            left = self.parseInfixExpr(left) orelse return null;
        }

        return left;
    }

    fn parsePrefixExpr(self: *Self) ?*ast.Expression {
        const loc = self.currentLocation();

        if (self.match(.kw_not)) {
            const operand = self.parsePrecedence(.not) orelse return null;
            return self.makeUnary(.not, operand, loc);
        }

        if (self.match(.minus)) {
            const operand = self.parsePrecedence(.unary) orelse return null;
            return self.makeUnary(.neg, operand, loc);
        }

        if (self.match(.plus)) {
            // Unary plus is a no-op, just parse operand
            return self.parsePrecedence(.unary);
        }

        return self.parsePrimaryExpr();
    }

    fn parsePrimaryExpr(self: *Self) ?*ast.Expression {
        const loc = self.currentLocation();

        // Literals
        if (self.match(.integer)) {
            return self.makeIntegerLiteral(self.previous.text, loc);
        }

        if (self.match(.float)) {
            return self.makeFloatLiteral(self.previous.text, loc);
        }

        if (self.match(.string)) {
            return self.makeStringLiteral(self.previous.text, loc);
        }

        if (self.match(.kw_true)) {
            return self.makeBoolLiteral(true, loc);
        }

        if (self.match(.kw_false)) {
            return self.makeBoolLiteral(false, loc);
        }

        if (self.match(.kw_null)) {
            return self.makeNullLiteral(loc);
        }

        // Parameter
        if (self.match(.parameter)) {
            return self.makeParameter(self.previous.text, loc);
        }

        // List literal
        if (self.match(.lbracket)) {
            return self.parseListLiteral(loc);
        }

        // Map literal
        if (self.match(.lbrace)) {
            return self.parseMapLiteral(loc);
        }

        // Grouped expression
        if (self.match(.lparen)) {
            const expr = self.parseExpression() orelse return null;
            if (!self.consume(.rparen, "Expected ')' after expression")) {
                return null;
            }
            return expr;
        }

        // Identifier (variable or function call)
        if (self.match(.identifier)) {
            const name = self.previous.text;

            // Check for function call
            if (self.match(.lparen)) {
                return self.parseFunctionCall(name, loc);
            }

            return self.makeVariable(name, loc);
        }

        self.errorAtCurrent("Expected expression");
        return null;
    }

    fn parseListLiteral(self: *Self, loc: ast.SourceLocation) ?*ast.Expression {
        var elements: std.ArrayList(*ast.Expression) = .empty;

        if (!self.check(.rbracket)) {
            const first = self.parseExpression() orelse return null;
            elements.append(self.arena.allocator(), first) catch return null;

            while (self.match(.comma)) {
                const elem = self.parseExpression() orelse return null;
                elements.append(self.arena.allocator(), elem) catch return null;
            }
        }

        if (!self.consume(.rbracket, "Expected ']' after list")) {
            return null;
        }

        const list = self.arena.allocator().create(ast.ListExpr) catch return null;
        list.* = .{
            .elements = elements.toOwnedSlice(self.arena.allocator()) catch return null,
            .location = loc,
        };

        const expr = self.arena.allocator().create(ast.Expression) catch return null;
        expr.* = .{ .list = list };
        return expr;
    }

    fn parseMapLiteral(self: *Self, loc: ast.SourceLocation) ?*ast.Expression {
        var entries: std.ArrayList(ast.PropertyEntry) = .empty;

        if (!self.check(.rbrace)) {
            const first = self.parsePropertyEntry() orelse return null;
            entries.append(self.arena.allocator(), first) catch return null;

            while (self.match(.comma)) {
                const entry = self.parsePropertyEntry() orelse return null;
                entries.append(self.arena.allocator(), entry) catch return null;
            }
        }

        if (!self.consume(.rbrace, "Expected '}' after map")) {
            return null;
        }

        const map = self.arena.allocator().create(ast.MapExpr) catch return null;
        map.* = .{
            .entries = entries.toOwnedSlice(self.arena.allocator()) catch return null,
            .location = loc,
        };

        const expr = self.arena.allocator().create(ast.Expression) catch return null;
        expr.* = .{ .map = map };
        return expr;
    }

    fn parseFunctionCall(self: *Self, name: []const u8, loc: ast.SourceLocation) ?*ast.Expression {
        var args: std.ArrayList(*ast.Expression) = .empty;

        if (!self.check(.rparen)) {
            const first = self.parseExpression() orelse return null;
            args.append(self.arena.allocator(), first) catch return null;

            while (self.match(.comma)) {
                const arg = self.parseExpression() orelse return null;
                args.append(self.arena.allocator(), arg) catch return null;
            }
        }

        if (!self.consume(.rparen, "Expected ')' after arguments")) {
            return null;
        }

        const call = self.arena.allocator().create(ast.FunctionCall) catch return null;
        call.* = .{
            .name = name,
            .arguments = args.toOwnedSlice(self.arena.allocator()) catch return null,
            .location = loc,
        };

        const expr = self.arena.allocator().create(ast.Expression) catch return null;
        expr.* = .{ .function_call = call };
        return expr;
    }

    fn getInfixPrecedence(self: *Self) Precedence {
        return switch (self.current.token_type) {
            .kw_or => .or_,
            .kw_xor => .xor,
            .kw_and => .and_,
            .eq, .neq, .kw_in, .kw_contains => .equality,
            .kw_starts, .kw_ends => .equality,
            .lt, .lte, .gt, .gte, .vector_distance, .fts_match => .comparison,
            .plus, .minus => .term,
            .star, .slash, .percent => .factor,
            .caret => .power,
            .dot => .primary,
            .kw_is => .equality,
            else => .none,
        };
    }

    fn parseInfixExpr(self: *Self, left: *ast.Expression) ?*ast.Expression {
        const loc = self.currentLocation();
        const token = self.current.token_type;

        // Property access
        if (token == .dot) {
            self.advance();
            if (!self.check(.identifier)) {
                self.errorAtCurrent("Expected property name after '.'");
                return null;
            }
            const prop_name = self.current.text;
            self.advance();
            return self.makePropertyAccess(left, prop_name, loc);
        }

        // IS NULL / IS NOT NULL
        if (token == .kw_is) {
            self.advance();
            if (self.match(.kw_not)) {
                if (!self.consume(.kw_null, "Expected NULL after IS NOT")) {
                    return null;
                }
                return self.makeUnary(.is_not_null, left, loc);
            }
            if (!self.consume(.kw_null, "Expected NULL after IS")) {
                return null;
            }
            return self.makeUnary(.is_null, left, loc);
        }

        // STARTS WITH / ENDS WITH
        if (token == .kw_starts or token == .kw_ends) {
            self.advance();
            if (!self.consume(.kw_with, "Expected WITH after STARTS/ENDS")) {
                return null;
            }
            const prec = self.getNextPrecedence(.equality);
            const right = self.parsePrecedence(prec) orelse return null;
            const op: BinaryOp = if (token == .kw_starts) .starts_with else .ends_with;
            return self.makeBinary(left, op, right, loc);
        }

        // Regular binary operators
        const op = self.tokenToBinaryOp(token) orelse {
            self.errorAtCurrent("Expected operator");
            return null;
        };
        self.advance();

        const prec = self.getInfixPrecedence();
        const next_prec = self.getNextPrecedence(prec);
        const right = self.parsePrecedence(next_prec) orelse return null;

        return self.makeBinary(left, op, right, loc);
    }

    fn getNextPrecedence(self: *Self, prec: Precedence) Precedence {
        _ = self;
        // Right-associative for power, left-associative for others
        // For primary (highest precedence), stay at primary to avoid overflow
        return switch (prec) {
            .power => .power, // right-associative
            .primary => .primary, // already at highest precedence
            else => @enumFromInt(@intFromEnum(prec) + 1),
        };
    }

    fn tokenToBinaryOp(self: *Self, token: TokenType) ?BinaryOp {
        _ = self;
        return switch (token) {
            .eq => .eq,
            .neq => .neq,
            .lt => .lt,
            .lte => .lte,
            .gt => .gt,
            .gte => .gte,
            .kw_and => .and_,
            .kw_or => .or_,
            .kw_xor => .xor,
            .kw_in => .in_,
            .kw_contains => .contains,
            .plus => .add,
            .minus => .sub,
            .star => .mul,
            .slash => .div,
            .percent => .mod,
            .caret => .pow,
            .vector_distance => .vector_distance,
            .fts_match => .fts_match,
            else => null,
        };
    }

    // ========================================================================
    // AST Node Helpers
    // ========================================================================

    fn makeIntegerLiteral(self: *Self, text: []const u8, loc: ast.SourceLocation) ?*ast.Expression {
        const value = std.fmt.parseInt(i64, text, 10) catch {
            self.errorAt(loc, "Invalid integer literal");
            return null;
        };
        const expr = self.arena.allocator().create(ast.Expression) catch return null;
        expr.* = .{
            .literal = .{
                .value = .{ .integer = value },
                .location = loc,
            },
        };
        return expr;
    }

    fn makeFloatLiteral(self: *Self, text: []const u8, loc: ast.SourceLocation) ?*ast.Expression {
        const value = std.fmt.parseFloat(f64, text) catch {
            self.errorAt(loc, "Invalid float literal");
            return null;
        };
        const expr = self.arena.allocator().create(ast.Expression) catch return null;
        expr.* = .{
            .literal = .{
                .value = .{ .float = value },
                .location = loc,
            },
        };
        return expr;
    }

    fn makeStringLiteral(self: *Self, text: []const u8, loc: ast.SourceLocation) ?*ast.Expression {
        // Remove quotes and handle escapes
        const content = if (text.len >= 2) text[1 .. text.len - 1] else text;
        const expr = self.arena.allocator().create(ast.Expression) catch return null;
        expr.* = .{
            .literal = .{
                .value = .{ .string = content },
                .location = loc,
            },
        };
        return expr;
    }

    fn makeBoolLiteral(self: *Self, value: bool, loc: ast.SourceLocation) ?*ast.Expression {
        const expr = self.arena.allocator().create(ast.Expression) catch return null;
        expr.* = .{
            .literal = .{
                .value = .{ .boolean = value },
                .location = loc,
            },
        };
        return expr;
    }

    fn makeNullLiteral(self: *Self, loc: ast.SourceLocation) ?*ast.Expression {
        const expr = self.arena.allocator().create(ast.Expression) catch return null;
        expr.* = .{
            .literal = .{
                .value = .null_value,
                .location = loc,
            },
        };
        return expr;
    }

    fn makeVariable(self: *Self, name: []const u8, loc: ast.SourceLocation) ?*ast.Expression {
        const expr = self.arena.allocator().create(ast.Expression) catch return null;
        expr.* = .{
            .variable = .{
                .name = name,
                .location = loc,
            },
        };
        return expr;
    }

    fn makeParameter(self: *Self, text: []const u8, loc: ast.SourceLocation) ?*ast.Expression {
        // Remove $ prefix
        const name = if (text.len > 0 and text[0] == '$') text[1..] else text;
        const expr = self.arena.allocator().create(ast.Expression) catch return null;
        expr.* = .{
            .parameter = .{
                .name = name,
                .location = loc,
            },
        };
        return expr;
    }

    fn makeUnary(self: *Self, op: UnaryOp, operand: *ast.Expression, loc: ast.SourceLocation) ?*ast.Expression {
        const unary = self.arena.allocator().create(ast.UnaryExpr) catch return null;
        unary.* = .{
            .operator = op,
            .operand = operand,
            .location = loc,
        };
        const expr = self.arena.allocator().create(ast.Expression) catch return null;
        expr.* = .{ .unary = unary };
        return expr;
    }

    fn makeBinary(self: *Self, left: *ast.Expression, op: BinaryOp, right: *ast.Expression, loc: ast.SourceLocation) ?*ast.Expression {
        const binary = self.arena.allocator().create(ast.BinaryExpr) catch return null;
        binary.* = .{
            .left = left,
            .operator = op,
            .right = right,
            .location = loc,
        };
        const expr = self.arena.allocator().create(ast.Expression) catch return null;
        expr.* = .{ .binary = binary };
        return expr;
    }

    fn makePropertyAccess(self: *Self, object: *ast.Expression, property: []const u8, loc: ast.SourceLocation) ?*ast.Expression {
        const pa = self.arena.allocator().create(ast.PropertyAccess) catch return null;
        pa.* = .{
            .object = object,
            .property = property,
            .location = loc,
        };
        const expr = self.arena.allocator().create(ast.Expression) catch return null;
        expr.* = .{ .property_access = pa };
        return expr;
    }

    // ========================================================================
    // Token Handling
    // ========================================================================

    fn advance(self: *Self) void {
        self.previous = self.current;
        self.current = self.lexer.nextToken();

        if (self.current.token_type == .invalid) {
            self.errorAtCurrent("Invalid token");
        }
    }

    fn check(self: *Self, token_type: TokenType) bool {
        return self.current.token_type == token_type;
    }

    fn match(self: *Self, token_type: TokenType) bool {
        if (!self.check(token_type)) return false;
        self.advance();
        return true;
    }

    fn consume(self: *Self, token_type: TokenType, message: []const u8) bool {
        if (self.check(token_type)) {
            self.advance();
            return true;
        }
        self.errorAtCurrent(message);
        return false;
    }

    fn currentLocation(self: *Self) ast.SourceLocation {
        return .{
            .line = self.current.line,
            .column = self.current.column,
            .length = @intCast(self.current.text.len),
        };
    }

    fn previousLocation(self: *Self) ast.SourceLocation {
        return .{
            .line = self.previous.line,
            .column = self.previous.column,
            .length = @intCast(self.previous.text.len),
        };
    }

    // ========================================================================
    // Error Handling
    // ========================================================================

    fn errorAtCurrent(self: *Self, message: []const u8) void {
        self.errorAt(self.currentLocation(), message);
    }

    fn errorAt(self: *Self, loc: ast.SourceLocation, message: []const u8) void {
        if (self.panic_mode) return;
        self.panic_mode = true;
        self.had_error = true;

        self.errors.append(self.allocator, .{
            .message = message,
            .line = loc.line,
            .column = loc.column,
            .length = loc.length,
        }) catch {
            self.errors_dropped = true;
        };
    }

    fn synchronize(self: *Self) void {
        self.panic_mode = false;

        while (!self.check(.eof)) {
            // Synchronize at clause boundaries
            switch (self.current.token_type) {
                .kw_match, .kw_where, .kw_return, .kw_order, .kw_limit, .kw_skip, .kw_create, .kw_delete, .kw_detach, .kw_set, .kw_remove => return,
                else => self.advance(),
            }
        }
    }
};

/// Result of parsing
pub const ParserResult = struct {
    query: ?*ast.Query,
    errors: []ParseError,
    /// True if some errors were dropped due to OOM
    errors_dropped: bool,
};

// ============================================================================
// Parser Tests
// ============================================================================

test "parse simple match return" {
    const source = "MATCH (n) RETURN n";
    var parser = Parser.init(std.testing.allocator, source);
    defer parser.deinit();

    const result = parser.parse();
    try std.testing.expect(result.query != null);
    try std.testing.expectEqual(@as(usize, 2), result.query.?.clauses.len);

    // First clause is MATCH
    try std.testing.expect(result.query.?.clauses[0] == .match);

    // Second clause is RETURN
    try std.testing.expect(result.query.?.clauses[1] == .return_);
}

test "parse match with label" {
    const source = "MATCH (n:Person) RETURN n";
    var parser = Parser.init(std.testing.allocator, source);
    defer parser.deinit();

    const result = parser.parse();
    try std.testing.expect(result.query != null);

    const match_clause = result.query.?.clauses[0].match;
    try std.testing.expectEqual(@as(usize, 1), match_clause.patterns.len);

    const pattern = match_clause.patterns[0];
    try std.testing.expectEqual(@as(usize, 1), pattern.elements.len);

    const node = pattern.elements[0].node;
    try std.testing.expectEqualStrings("n", node.variable.?);
    try std.testing.expectEqual(@as(usize, 1), node.labels.len);
    try std.testing.expectEqualStrings("Person", node.labels[0]);
}

test "parse match with properties" {
    const source = "MATCH (n:Person {name: \"Alice\"}) RETURN n";
    var parser = Parser.init(std.testing.allocator, source);
    defer parser.deinit();

    const result = parser.parse();
    try std.testing.expect(result.query != null);

    const node = result.query.?.clauses[0].match.patterns[0].elements[0].node;
    try std.testing.expect(node.properties != null);
    try std.testing.expectEqual(@as(usize, 1), node.properties.?.len);
    try std.testing.expectEqualStrings("name", node.properties.?[0].key);
}

test "parse relationship pattern" {
    const source = "MATCH (a)-[:KNOWS]->(b) RETURN a, b";
    var parser = Parser.init(std.testing.allocator, source);
    defer parser.deinit();

    const result = parser.parse();
    try std.testing.expect(result.query != null);

    const pattern = result.query.?.clauses[0].match.patterns[0];
    try std.testing.expectEqual(@as(usize, 3), pattern.elements.len);

    // First element is node
    try std.testing.expect(pattern.elements[0] == .node);

    // Second element is edge
    try std.testing.expect(pattern.elements[1] == .edge);
    const edge = pattern.elements[1].edge;
    try std.testing.expectEqual(@as(usize, 1), edge.types.len);
    try std.testing.expectEqualStrings("KNOWS", edge.types[0]);
    try std.testing.expectEqual(ast.EdgeDirection.outgoing, edge.direction);

    // Third element is node
    try std.testing.expect(pattern.elements[2] == .node);
}

test "parse where clause" {
    const source = "MATCH (n) WHERE n.age > 30 RETURN n";
    var parser = Parser.init(std.testing.allocator, source);
    defer parser.deinit();

    const result = parser.parse();
    try std.testing.expect(result.query != null);
    try std.testing.expectEqual(@as(usize, 3), result.query.?.clauses.len);

    // Second clause is WHERE
    try std.testing.expect(result.query.?.clauses[1] == .where);
    const where = result.query.?.clauses[1].where;
    try std.testing.expect(where.condition.* == .binary);
}

test "parse vector distance" {
    const source = "MATCH (c:Chunk) WHERE c.embedding <=> $query < 0.5 RETURN c";
    var parser = Parser.init(std.testing.allocator, source);
    defer parser.deinit();

    const result = parser.parse();
    try std.testing.expect(result.query != null);

    const where = result.query.?.clauses[1].where;
    // The condition should be a comparison with vector_distance on the left
    try std.testing.expect(where.condition.* == .binary);
}

test "parse fts match" {
    const source = "MATCH (d:Doc) WHERE d.text @@ \"search\" RETURN d";
    var parser = Parser.init(std.testing.allocator, source);
    defer parser.deinit();

    const result = parser.parse();
    try std.testing.expect(result.query != null);

    const where = result.query.?.clauses[1].where;
    try std.testing.expect(where.condition.* == .binary);
    try std.testing.expectEqual(BinaryOp.fts_match, where.condition.binary.operator);
}

test "parse order by limit skip" {
    const source = "MATCH (n) RETURN n ORDER BY n.name DESC LIMIT 10 SKIP 5";
    var parser = Parser.init(std.testing.allocator, source);
    defer parser.deinit();

    const result = parser.parse();
    try std.testing.expect(result.query != null);
    try std.testing.expectEqual(@as(usize, 5), result.query.?.clauses.len);

    try std.testing.expect(result.query.?.clauses[2] == .order_by);
    try std.testing.expect(result.query.?.clauses[3] == .limit);
    try std.testing.expect(result.query.?.clauses[4] == .skip);
}

test "parse function call" {
    const source = "MATCH (n) RETURN count(n)";
    var parser = Parser.init(std.testing.allocator, source);
    defer parser.deinit();

    const result = parser.parse();
    try std.testing.expect(result.query != null);

    const return_clause = result.query.?.clauses[1].return_;
    const expr = return_clause.items[0].expression;
    try std.testing.expect(expr.* == .function_call);
    try std.testing.expectEqualStrings("count", expr.function_call.name);
}

test "parse list literal" {
    const source = "MATCH (n) WHERE n.id IN [1, 2, 3] RETURN n";
    var parser = Parser.init(std.testing.allocator, source);
    defer parser.deinit();

    const result = parser.parse();
    try std.testing.expect(result.query != null);

    const where = result.query.?.clauses[1].where;
    try std.testing.expect(where.condition.* == .binary);
    try std.testing.expectEqual(BinaryOp.in_, where.condition.binary.operator);
    try std.testing.expect(where.condition.binary.right.* == .list);
}

test "parse error recovery" {
    const source = "MATCH (n RETURN n"; // Missing closing paren
    var parser = Parser.init(std.testing.allocator, source);
    defer parser.deinit();

    const result = parser.parse();
    try std.testing.expect(result.query == null or result.errors.len > 0);
}

test "query types" {
    const q = QueryType.match;
    try std.testing.expectEqual(QueryType.match, q);
}

test "binary operators" {
    const op = BinaryOp.vector_distance;
    try std.testing.expectEqual(BinaryOp.vector_distance, op);
}

test "parse REMOVE property" {
    const source = "MATCH (n:Person) REMOVE n.age";
    var parser = Parser.init(std.testing.allocator, source);
    defer parser.deinit();

    const result = parser.parse();
    try std.testing.expect(result.query != null);
    try std.testing.expectEqual(@as(usize, 2), result.query.?.clauses.len);

    // Second clause is REMOVE
    try std.testing.expect(result.query.?.clauses[1] == .remove);
    const remove = result.query.?.clauses[1].remove;
    try std.testing.expectEqual(@as(usize, 1), remove.items.len);
    try std.testing.expect(remove.items[0] == .property);
    try std.testing.expectEqualStrings("age", remove.items[0].property.property_name);
}

test "parse REMOVE labels" {
    const source = "MATCH (n:Person) REMOVE n:Admin";
    var parser = Parser.init(std.testing.allocator, source);
    defer parser.deinit();

    const result = parser.parse();
    try std.testing.expect(result.query != null);
    try std.testing.expectEqual(@as(usize, 2), result.query.?.clauses.len);

    // Second clause is REMOVE
    try std.testing.expect(result.query.?.clauses[1] == .remove);
    const remove = result.query.?.clauses[1].remove;
    try std.testing.expectEqual(@as(usize, 1), remove.items.len);
    try std.testing.expect(remove.items[0] == .labels);
    try std.testing.expectEqual(@as(usize, 1), remove.items[0].labels.label_names.len);
    try std.testing.expectEqualStrings("Admin", remove.items[0].labels.label_names[0]);
}

test "parse REMOVE multiple labels" {
    const source = "MATCH (n:Person) REMOVE n:Admin:Verified";
    var parser = Parser.init(std.testing.allocator, source);
    defer parser.deinit();

    const result = parser.parse();
    try std.testing.expect(result.query != null);

    const remove = result.query.?.clauses[1].remove;
    try std.testing.expectEqual(@as(usize, 1), remove.items.len);
    try std.testing.expect(remove.items[0] == .labels);
    try std.testing.expectEqual(@as(usize, 2), remove.items[0].labels.label_names.len);
    try std.testing.expectEqualStrings("Admin", remove.items[0].labels.label_names[0]);
    try std.testing.expectEqualStrings("Verified", remove.items[0].labels.label_names[1]);
}

test "parse REMOVE multiple items" {
    const source = "MATCH (n:Person) REMOVE n.age, n.email, n:Temp";
    var parser = Parser.init(std.testing.allocator, source);
    defer parser.deinit();

    const result = parser.parse();
    try std.testing.expect(result.query != null);

    const remove = result.query.?.clauses[1].remove;
    try std.testing.expectEqual(@as(usize, 3), remove.items.len);
    try std.testing.expect(remove.items[0] == .property);
    try std.testing.expect(remove.items[1] == .property);
    try std.testing.expect(remove.items[2] == .labels);
}
