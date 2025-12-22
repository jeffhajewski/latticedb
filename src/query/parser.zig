//! Cypher query parser for Lattice database.
//!
//! Parses Cypher queries with vector and full-text search extensions.

const std = @import("std");
const types = @import("../core/types.zig");

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
    kw_set,
    kw_merge,
    kw_with,
    kw_as,
    kw_order,
    kw_by,
    kw_limit,
    kw_skip,
    kw_and,
    kw_or,
    kw_not,
    kw_null,
    kw_true,
    kw_false,
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
    pipe,
    arrow_right,
    arrow_left,
    dash,
    // Special
    eof,
    invalid,
};

test "query types" {
    const q = QueryType.match;
    try std.testing.expectEqual(QueryType.match, q);
}

test "binary operators" {
    const op = BinaryOp.vector_distance;
    try std.testing.expectEqual(BinaryOp.vector_distance, op);
}
