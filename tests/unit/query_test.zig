//! Behavioral tests for Query Layer.
//!
//! These tests verify the contracts of query processing components:
//! - Lexer: tokenization of Cypher queries
//! - Parser: AST construction from tokens
//! - Executor: query execution with operators

const std = @import("std");
const lattice = @import("lattice");

const lexer_mod = lattice.query.lexer;
const parser_mod = lattice.query.parser;
const executor_mod = lattice.query.executor;
const ast = lattice.query.ast;

const Lexer = lexer_mod.Lexer;
const Parser = parser_mod.Parser;
const TokenType = parser_mod.TokenType;
const BinaryOp = parser_mod.BinaryOp;
const Row = executor_mod.Row;
const SlotValue = executor_mod.SlotValue;
const ExecutionContext = executor_mod.ExecutionContext;
const QueryResult = executor_mod.QueryResult;

// ============================================================================
// Lexer Tests
// ============================================================================

test "lexer: keywords are case insensitive" {
    const keywords = [_]struct { text: []const u8, expected: TokenType }{
        .{ .text = "match", .expected = .kw_match },
        .{ .text = "MATCH", .expected = .kw_match },
        .{ .text = "Match", .expected = .kw_match },
        .{ .text = "mAtCh", .expected = .kw_match },
        .{ .text = "where", .expected = .kw_where },
        .{ .text = "WHERE", .expected = .kw_where },
        .{ .text = "return", .expected = .kw_return },
        .{ .text = "RETURN", .expected = .kw_return },
        .{ .text = "create", .expected = .kw_create },
        .{ .text = "CREATE", .expected = .kw_create },
    };

    for (keywords) |kw| {
        var lexer = Lexer.init(kw.text);
        const token = lexer.nextToken();
        try std.testing.expectEqual(kw.expected, token.token_type);
    }
}

test "lexer: <=> tokenized as single operator" {
    var lexer = Lexer.init("a <=> b");

    const t1 = lexer.nextToken();
    try std.testing.expectEqual(TokenType.identifier, t1.token_type);

    const t2 = lexer.nextToken();
    try std.testing.expectEqual(TokenType.vector_distance, t2.token_type);
    try std.testing.expectEqualStrings("<=>", t2.text);

    const t3 = lexer.nextToken();
    try std.testing.expectEqual(TokenType.identifier, t3.token_type);
}

test "lexer: @@ tokenized as single operator" {
    var lexer = Lexer.init("d.text @@ \"query\"");

    _ = lexer.nextToken(); // d
    _ = lexer.nextToken(); // .
    _ = lexer.nextToken(); // text

    const op = lexer.nextToken();
    try std.testing.expectEqual(TokenType.fts_match, op.token_type);
    try std.testing.expectEqualStrings("@@", op.text);
}

test "lexer: unterminated string returns invalid" {
    var lexer = Lexer.init("\"unterminated");
    const token = lexer.nextToken();
    try std.testing.expectEqual(TokenType.invalid, token.token_type);
}

test "lexer: unterminated string at newline returns invalid" {
    var lexer = Lexer.init("\"line1\nline2\"");
    const token = lexer.nextToken();
    try std.testing.expectEqual(TokenType.invalid, token.token_type);
}

test "lexer: string with escape sequences" {
    var lexer = Lexer.init("\"hello\\\"world\"");
    const token = lexer.nextToken();
    try std.testing.expectEqual(TokenType.string, token.token_type);
    try std.testing.expectEqualStrings("\"hello\\\"world\"", token.text);
}

test "lexer: single-quoted strings" {
    var lexer = Lexer.init("'single quoted'");
    const token = lexer.nextToken();
    try std.testing.expectEqual(TokenType.string, token.token_type);
}

test "lexer: line and column tracking accurate" {
    var lexer = Lexer.init("MATCH\n  (n)\n    RETURN");

    const t1 = lexer.nextToken(); // MATCH
    try std.testing.expectEqual(@as(u32, 1), t1.line);
    try std.testing.expectEqual(@as(u32, 1), t1.column);

    const t2 = lexer.nextToken(); // (
    try std.testing.expectEqual(@as(u32, 2), t2.line);
    try std.testing.expectEqual(@as(u32, 3), t2.column);

    const t3 = lexer.nextToken(); // n
    try std.testing.expectEqual(@as(u32, 2), t3.line);
    try std.testing.expectEqual(@as(u32, 4), t3.column);

    _ = lexer.nextToken(); // )

    const t5 = lexer.nextToken(); // RETURN
    try std.testing.expectEqual(@as(u32, 3), t5.line);
    try std.testing.expectEqual(@as(u32, 5), t5.column);
}

test "lexer: parameters recognized" {
    var lexer = Lexer.init("$param $name123 $_underscore");

    const t1 = lexer.nextToken();
    try std.testing.expectEqual(TokenType.parameter, t1.token_type);
    try std.testing.expectEqualStrings("$param", t1.text);

    const t2 = lexer.nextToken();
    try std.testing.expectEqual(TokenType.parameter, t2.token_type);
    try std.testing.expectEqualStrings("$name123", t2.text);

    const t3 = lexer.nextToken();
    try std.testing.expectEqual(TokenType.parameter, t3.token_type);
    try std.testing.expectEqualStrings("$_underscore", t3.text);
}

test "lexer: all comparison operators" {
    var lexer = Lexer.init("= <> != < <= > >=");

    try std.testing.expectEqual(TokenType.eq, lexer.nextToken().token_type);
    try std.testing.expectEqual(TokenType.neq, lexer.nextToken().token_type);
    try std.testing.expectEqual(TokenType.neq, lexer.nextToken().token_type);
    try std.testing.expectEqual(TokenType.lt, lexer.nextToken().token_type);
    try std.testing.expectEqual(TokenType.lte, lexer.nextToken().token_type);
    try std.testing.expectEqual(TokenType.gt, lexer.nextToken().token_type);
    try std.testing.expectEqual(TokenType.gte, lexer.nextToken().token_type);
}

test "lexer: arrow operators" {
    var lexer = Lexer.init("-> <- -");

    try std.testing.expectEqual(TokenType.arrow_right, lexer.nextToken().token_type);
    try std.testing.expectEqual(TokenType.arrow_left, lexer.nextToken().token_type);
    try std.testing.expectEqual(TokenType.dash, lexer.nextToken().token_type);
}

test "lexer: numbers - integers and floats" {
    var lexer = Lexer.init("42 0 -1 3.14 0.5 1e10 2.5e-3");

    try std.testing.expectEqual(TokenType.integer, lexer.nextToken().token_type);
    try std.testing.expectEqual(TokenType.integer, lexer.nextToken().token_type);
    try std.testing.expectEqual(TokenType.dash, lexer.nextToken().token_type); // unary minus
    try std.testing.expectEqual(TokenType.integer, lexer.nextToken().token_type);
    try std.testing.expectEqual(TokenType.float, lexer.nextToken().token_type);
    try std.testing.expectEqual(TokenType.float, lexer.nextToken().token_type);
    try std.testing.expectEqual(TokenType.float, lexer.nextToken().token_type);
    try std.testing.expectEqual(TokenType.float, lexer.nextToken().token_type);
}

test "lexer: comments skipped" {
    var lexer = Lexer.init("MATCH // comment\nRETURN");

    try std.testing.expectEqual(TokenType.kw_match, lexer.nextToken().token_type);
    try std.testing.expectEqual(TokenType.kw_return, lexer.nextToken().token_type);
}

test "lexer: boolean keywords" {
    var lexer = Lexer.init("true false TRUE FALSE");

    try std.testing.expectEqual(TokenType.kw_true, lexer.nextToken().token_type);
    try std.testing.expectEqual(TokenType.kw_false, lexer.nextToken().token_type);
    try std.testing.expectEqual(TokenType.kw_true, lexer.nextToken().token_type);
    try std.testing.expectEqual(TokenType.kw_false, lexer.nextToken().token_type);
}

test "lexer: null keyword" {
    var lexer = Lexer.init("null NULL");

    try std.testing.expectEqual(TokenType.kw_null, lexer.nextToken().token_type);
    try std.testing.expectEqual(TokenType.kw_null, lexer.nextToken().token_type);
}

// ============================================================================
// Parser Tests
// ============================================================================

test "parser: MATCH (n) RETURN n parses" {
    var parser = Parser.init(std.testing.allocator, "MATCH (n) RETURN n");
    defer parser.deinit();

    const result = parser.parse();
    try std.testing.expect(result.query != null);
    try std.testing.expectEqual(@as(usize, 0), result.errors.len);
    try std.testing.expectEqual(@as(usize, 2), result.query.?.clauses.len);
}

test "parser: WHERE clause with comparison" {
    var parser = Parser.init(std.testing.allocator, "MATCH (n) WHERE n.age > 30 RETURN n");
    defer parser.deinit();

    const result = parser.parse();
    try std.testing.expect(result.query != null);
    try std.testing.expectEqual(@as(usize, 3), result.query.?.clauses.len);

    // Middle clause is WHERE
    try std.testing.expect(result.query.?.clauses[1] == .where);
    const where = result.query.?.clauses[1].where;
    try std.testing.expect(where.condition.* == .binary);
    try std.testing.expectEqual(BinaryOp.gt, where.condition.binary.operator);
}

test "parser: node with properties" {
    var parser = Parser.init(std.testing.allocator, "MATCH (n:Person {name: \"Alice\", age: 30}) RETURN n");
    defer parser.deinit();

    const result = parser.parse();
    try std.testing.expect(result.query != null);

    const node = result.query.?.clauses[0].match.patterns[0].elements[0].node;
    try std.testing.expect(node.properties != null);
    try std.testing.expectEqual(@as(usize, 2), node.properties.?.len);
}

test "parser: pattern with edge type" {
    var parser = Parser.init(std.testing.allocator, "MATCH (a)-[:KNOWS]->(b) RETURN a, b");
    defer parser.deinit();

    const result = parser.parse();
    try std.testing.expect(result.query != null);

    const pattern = result.query.?.clauses[0].match.patterns[0];
    try std.testing.expectEqual(@as(usize, 3), pattern.elements.len);

    const edge = pattern.elements[1].edge;
    try std.testing.expectEqual(@as(usize, 1), edge.types.len);
    try std.testing.expectEqualStrings("KNOWS", edge.types[0]);
    try std.testing.expectEqual(ast.EdgeDirection.outgoing, edge.direction);
}

test "parser: vector distance expression" {
    var parser = Parser.init(std.testing.allocator, "MATCH (c) WHERE c.vec <=> $query < 0.5 RETURN c");
    defer parser.deinit();

    const result = parser.parse();
    try std.testing.expect(result.query != null);
    try std.testing.expectEqual(@as(usize, 0), result.errors.len);
}

test "parser: full-text search expression" {
    var parser = Parser.init(std.testing.allocator, "MATCH (d) WHERE d.text @@ \"keyword\" RETURN d");
    defer parser.deinit();

    const result = parser.parse();
    try std.testing.expect(result.query != null);

    const where = result.query.?.clauses[1].where;
    try std.testing.expect(where.condition.* == .binary);
    try std.testing.expectEqual(BinaryOp.fts_match, where.condition.binary.operator);
}

test "parser: operator precedence - multiplication before addition" {
    var parser = Parser.init(std.testing.allocator, "MATCH (n) RETURN n.a + n.b * n.c");
    defer parser.deinit();

    const result = parser.parse();
    try std.testing.expect(result.query != null);

    // The expression should be: a + (b * c), not (a + b) * c
    const return_clause = result.query.?.clauses[1].return_;
    const expr = return_clause.items[0].expression;
    try std.testing.expect(expr.* == .binary);
    try std.testing.expectEqual(BinaryOp.add, expr.binary.operator);
    // Right side should be the multiplication
    try std.testing.expect(expr.binary.right.* == .binary);
    try std.testing.expectEqual(BinaryOp.mul, expr.binary.right.binary.operator);
}

test "parser: operator precedence - AND before OR" {
    var parser = Parser.init(std.testing.allocator, "MATCH (n) WHERE n.a OR n.b AND n.c RETURN n");
    defer parser.deinit();

    const result = parser.parse();
    try std.testing.expect(result.query != null);

    // Expression should be: a OR (b AND c)
    const where = result.query.?.clauses[1].where;
    try std.testing.expect(where.condition.* == .binary);
    try std.testing.expectEqual(BinaryOp.or_, where.condition.binary.operator);
}

test "parser: function call" {
    var parser = Parser.init(std.testing.allocator, "MATCH (n) RETURN count(n), sum(n.value)");
    defer parser.deinit();

    const result = parser.parse();
    try std.testing.expect(result.query != null);

    const return_clause = result.query.?.clauses[1].return_;
    try std.testing.expectEqual(@as(usize, 2), return_clause.items.len);

    const count_expr = return_clause.items[0].expression;
    try std.testing.expect(count_expr.* == .function_call);
    try std.testing.expectEqualStrings("count", count_expr.function_call.name);

    const sum_expr = return_clause.items[1].expression;
    try std.testing.expect(sum_expr.* == .function_call);
    try std.testing.expectEqualStrings("sum", sum_expr.function_call.name);
}

test "parser: list literal in expression" {
    var parser = Parser.init(std.testing.allocator, "MATCH (n) WHERE n.id IN [1, 2, 3] RETURN n");
    defer parser.deinit();

    const result = parser.parse();
    try std.testing.expect(result.query != null);

    const where = result.query.?.clauses[1].where;
    try std.testing.expect(where.condition.* == .binary);
    try std.testing.expectEqual(BinaryOp.in_, where.condition.binary.operator);
    try std.testing.expect(where.condition.binary.right.* == .list);
    try std.testing.expectEqual(@as(usize, 3), where.condition.binary.right.list.elements.len);
}

test "parser: ORDER BY with DESC" {
    var parser = Parser.init(std.testing.allocator, "MATCH (n) RETURN n ORDER BY n.name DESC");
    defer parser.deinit();

    const result = parser.parse();
    try std.testing.expect(result.query != null);
    try std.testing.expectEqual(@as(usize, 3), result.query.?.clauses.len);
    try std.testing.expect(result.query.?.clauses[2] == .order_by);

    const order_by = result.query.?.clauses[2].order_by;
    try std.testing.expectEqual(@as(usize, 1), order_by.items.len);
    try std.testing.expect(order_by.items[0].descending);
}

test "parser: LIMIT and SKIP" {
    var parser = Parser.init(std.testing.allocator, "MATCH (n) RETURN n LIMIT 10 SKIP 5");
    defer parser.deinit();

    const result = parser.parse();
    try std.testing.expect(result.query != null);
    try std.testing.expectEqual(@as(usize, 4), result.query.?.clauses.len);
    try std.testing.expect(result.query.?.clauses[2] == .limit);
    try std.testing.expect(result.query.?.clauses[3] == .skip);
}

test "parser: RETURN DISTINCT" {
    var parser = Parser.init(std.testing.allocator, "MATCH (n) RETURN DISTINCT n.name");
    defer parser.deinit();

    const result = parser.parse();
    try std.testing.expect(result.query != null);

    const return_clause = result.query.?.clauses[1].return_;
    try std.testing.expect(return_clause.distinct);
}

test "parser: alias with AS" {
    var parser = Parser.init(std.testing.allocator, "MATCH (n) RETURN n.name AS name");
    defer parser.deinit();

    const result = parser.parse();
    try std.testing.expect(result.query != null);

    const return_clause = result.query.?.clauses[1].return_;
    try std.testing.expectEqual(@as(usize, 1), return_clause.items.len);
    try std.testing.expectEqualStrings("name", return_clause.items[0].alias.?);
}

test "parser: syntax error gives error" {
    var parser = Parser.init(std.testing.allocator, "MATCH (n RETURN n"); // missing )
    defer parser.deinit();

    const result = parser.parse();
    try std.testing.expect(result.errors.len > 0);
}

test "parser: IS NULL and IS NOT NULL" {
    var parser = Parser.init(std.testing.allocator, "MATCH (n) WHERE n.x IS NULL OR n.y IS NOT NULL RETURN n");
    defer parser.deinit();

    const result = parser.parse();
    try std.testing.expect(result.query != null);
    try std.testing.expectEqual(@as(usize, 0), result.errors.len);
}

test "parser: multiple labels on node" {
    var parser = Parser.init(std.testing.allocator, "MATCH (n:Person:Employee) RETURN n");
    defer parser.deinit();

    const result = parser.parse();
    try std.testing.expect(result.query != null);

    const node = result.query.?.clauses[0].match.patterns[0].elements[0].node;
    try std.testing.expectEqual(@as(usize, 2), node.labels.len);
    try std.testing.expectEqualStrings("Person", node.labels[0]);
    try std.testing.expectEqualStrings("Employee", node.labels[1]);
}

test "parser: bidirectional edge" {
    var parser = Parser.init(std.testing.allocator, "MATCH (a)-[r]-(b) RETURN a, b");
    defer parser.deinit();

    const result = parser.parse();
    try std.testing.expect(result.query != null);

    const edge = result.query.?.clauses[0].match.patterns[0].elements[1].edge;
    try std.testing.expectEqual(ast.EdgeDirection.both, edge.direction);
}

test "parser: incoming edge" {
    var parser = Parser.init(std.testing.allocator, "MATCH (a)<-[:KNOWS]-(b) RETURN a, b");
    defer parser.deinit();

    const result = parser.parse();
    try std.testing.expect(result.query != null);

    const edge = result.query.?.clauses[0].match.patterns[0].elements[1].edge;
    try std.testing.expectEqual(ast.EdgeDirection.incoming, edge.direction);
}

// ============================================================================
// Executor Tests
// ============================================================================

test "executor: row slot operations" {
    var row = Row.init();

    // Initially all slots empty
    try std.testing.expect(!row.hasSlot(0));
    try std.testing.expect(!row.hasSlot(5));
    try std.testing.expect(row.getSlot(0) == null);

    // Set a node reference
    row.setSlot(0, .{ .node_ref = 42 });
    try std.testing.expect(row.hasSlot(0));
    try std.testing.expectEqual(@as(u64, 42), row.getSlot(0).?.asNodeId().?);

    // Set a property value
    row.setSlot(1, .{ .property = .{ .int_val = 100 } });
    try std.testing.expect(row.hasSlot(1));
    try std.testing.expectEqual(@as(i64, 100), row.getSlot(1).?.asProperty().?.int_val);
}

test "executor: row distances for vector search" {
    var row = Row.init();

    row.setSlot(0, .{ .node_ref = 1 });
    row.setDistance(0, 0.25);

    try std.testing.expectEqual(@as(f32, 0.25), row.getDistance(0));
}

test "executor: row scores for FTS" {
    var row = Row.init();

    row.setSlot(0, .{ .node_ref = 1 });
    row.setScore(0, 2.5);

    try std.testing.expectEqual(@as(f32, 2.5), row.getScore(0));
}

test "executor: row clone preserves data" {
    var row = Row.init();
    row.setSlot(0, .{ .node_ref = 100 });
    row.setSlot(3, .{ .property = .{ .bool_val = true } });
    row.setDistance(0, 0.5);
    row.setScore(3, 1.5);

    const cloned = row.clone();

    try std.testing.expectEqual(@as(u64, 100), cloned.getSlot(0).?.asNodeId().?);
    try std.testing.expectEqual(true, cloned.getSlot(3).?.asProperty().?.bool_val);
    try std.testing.expectEqual(@as(f32, 0.5), cloned.getDistance(0));
    try std.testing.expectEqual(@as(f32, 1.5), cloned.getScore(3));
}

test "executor: row copyFrom merges slots" {
    var row1 = Row.init();
    row1.setSlot(0, .{ .node_ref = 1 });

    var row2 = Row.init();
    row2.setSlot(1, .{ .node_ref = 2 });

    row1.copyFrom(&row2);

    try std.testing.expect(row1.hasSlot(0));
    try std.testing.expect(row1.hasSlot(1));
    try std.testing.expectEqual(@as(u64, 1), row1.getSlot(0).?.asNodeId().?);
    try std.testing.expectEqual(@as(u64, 2), row1.getSlot(1).?.asNodeId().?);
}

test "executor: row clear resets all slots" {
    var row = Row.init();
    row.setSlot(0, .{ .node_ref = 1 });
    row.setSlot(5, .{ .node_ref = 2 });

    row.clear();

    try std.testing.expect(!row.hasSlot(0));
    try std.testing.expect(!row.hasSlot(5));
}

test "executor: context parameter operations" {
    const allocator = std.testing.allocator;
    var ctx = ExecutionContext.init(allocator);
    defer ctx.deinit();

    try ctx.setParameter("name", .{ .string_val = "Alice" });
    try ctx.setParameter("age", .{ .int_val = 30 });

    const name = ctx.getParameter("name").?;
    try std.testing.expectEqualStrings("Alice", name.string_val);

    const age = ctx.getParameter("age").?;
    try std.testing.expectEqual(@as(i64, 30), age.int_val);

    try std.testing.expect(ctx.getParameter("nonexistent") == null);
}

test "executor: context variable registration" {
    const allocator = std.testing.allocator;
    var ctx = ExecutionContext.init(allocator);
    defer ctx.deinit();

    try ctx.registerVariable("n", 0);
    try ctx.registerVariable("r", 1);
    try ctx.registerVariable("m", 2);

    try std.testing.expectEqual(@as(u8, 0), ctx.getVariableSlot("n").?);
    try std.testing.expectEqual(@as(u8, 1), ctx.getVariableSlot("r").?);
    try std.testing.expectEqual(@as(u8, 2), ctx.getVariableSlot("m").?);
    try std.testing.expect(ctx.getVariableSlot("x") == null);
}

test "executor: context row allocation" {
    const allocator = std.testing.allocator;
    var ctx = ExecutionContext.init(allocator);
    defer ctx.deinit();

    const row1 = try ctx.allocRow();
    const row2 = try ctx.allocRow();

    // Both should be valid and different
    try std.testing.expect(row1 != row2);

    row1.setSlot(0, .{ .node_ref = 1 });
    row2.setSlot(0, .{ .node_ref = 2 });

    try std.testing.expectEqual(@as(u64, 1), row1.getSlot(0).?.asNodeId().?);
    try std.testing.expectEqual(@as(u64, 2), row2.getSlot(0).?.asNodeId().?);
}

test "executor: context row arena reset" {
    const allocator = std.testing.allocator;
    var ctx = ExecutionContext.init(allocator);
    defer ctx.deinit();

    _ = try ctx.allocRow();
    _ = try ctx.allocRow();

    // Reset should work without issues
    ctx.resetRowArena();

    // Should be able to allocate again
    const row = try ctx.allocRow();
    row.setSlot(0, .{ .node_ref = 100 });
    try std.testing.expectEqual(@as(u64, 100), row.getSlot(0).?.asNodeId().?);
}

test "executor: query result operations" {
    const allocator = std.testing.allocator;
    var result = QueryResult.init(allocator);
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 0), result.rowCount());

    var row1 = Row.init();
    row1.setSlot(0, .{ .node_ref = 1 });
    try result.addRow(row1);

    var row2 = Row.init();
    row2.setSlot(0, .{ .node_ref = 2 });
    try result.addRow(row2);

    try std.testing.expectEqual(@as(usize, 2), result.rowCount());
}

test "executor: slot value type checks" {
    const node_val = SlotValue{ .node_ref = 42 };
    try std.testing.expect(node_val.asNodeId() != null);
    try std.testing.expect(node_val.asEdgeId() == null);
    try std.testing.expect(node_val.asProperty() == null);
    try std.testing.expect(!node_val.isEmpty());

    const edge_val = SlotValue{ .edge_ref = 100 };
    try std.testing.expect(edge_val.asNodeId() == null);
    try std.testing.expect(edge_val.asEdgeId() != null);
    try std.testing.expect(edge_val.asProperty() == null);

    const prop_val = SlotValue{ .property = .{ .int_val = 50 } };
    try std.testing.expect(prop_val.asNodeId() == null);
    try std.testing.expect(prop_val.asEdgeId() == null);
    try std.testing.expect(prop_val.asProperty() != null);

    const empty_val = SlotValue{ .empty = {} };
    try std.testing.expect(empty_val.isEmpty());
}

// ============================================================================
// CREATE/DELETE Parsing Tests
// ============================================================================

test "parser: CREATE (n:Person) parses" {
    var parser = Parser.init(std.testing.allocator, "CREATE (n:Person)");
    defer parser.deinit();

    const result = parser.parse();
    try std.testing.expect(result.query != null);
    try std.testing.expectEqual(@as(usize, 0), result.errors.len);
    try std.testing.expectEqual(@as(usize, 1), result.query.?.clauses.len);
    try std.testing.expect(result.query.?.clauses[0] == .create);
}

test "parser: CREATE with properties parses" {
    var parser = Parser.init(std.testing.allocator, "CREATE (n:Person {name: \"Alice\", age: 30})");
    defer parser.deinit();

    const result = parser.parse();
    try std.testing.expect(result.query != null);
    try std.testing.expectEqual(@as(usize, 0), result.errors.len);

    const create = result.query.?.clauses[0].create;
    try std.testing.expectEqual(@as(usize, 1), create.patterns.len);

    const pattern = create.patterns[0];
    try std.testing.expectEqual(@as(usize, 1), pattern.elements.len);

    const node = pattern.elements[0].node;
    try std.testing.expect(node.variable != null);
    try std.testing.expectEqualStrings("n", node.variable.?);
    try std.testing.expectEqual(@as(usize, 1), node.labels.len);
    try std.testing.expectEqualStrings("Person", node.labels[0]);
    try std.testing.expect(node.properties != null);
    try std.testing.expectEqual(@as(usize, 2), node.properties.?.len);
}

test "parser: DELETE n parses" {
    var parser = Parser.init(std.testing.allocator, "MATCH (n:Person) DELETE n");
    defer parser.deinit();

    const result = parser.parse();
    try std.testing.expect(result.query != null);
    try std.testing.expectEqual(@as(usize, 0), result.errors.len);
    try std.testing.expectEqual(@as(usize, 2), result.query.?.clauses.len);

    // Second clause is DELETE
    try std.testing.expect(result.query.?.clauses[1] == .delete);
    const delete = result.query.?.clauses[1].delete;
    try std.testing.expect(!delete.detach);
    try std.testing.expectEqual(@as(usize, 1), delete.expressions.len);
}

test "parser: DETACH DELETE parses" {
    var parser = Parser.init(std.testing.allocator, "MATCH (n:Person) DETACH DELETE n");
    defer parser.deinit();

    const result = parser.parse();
    try std.testing.expect(result.query != null);
    try std.testing.expectEqual(@as(usize, 0), result.errors.len);

    // Second clause is DELETE with detach=true
    const delete = result.query.?.clauses[1].delete;
    try std.testing.expect(delete.detach);
}

test "parser: DELETE multiple variables parses" {
    var parser = Parser.init(std.testing.allocator, "MATCH (a), (b) DELETE a, b");
    defer parser.deinit();

    const result = parser.parse();
    try std.testing.expect(result.query != null);
    try std.testing.expectEqual(@as(usize, 0), result.errors.len);

    const delete = result.query.?.clauses[1].delete;
    try std.testing.expectEqual(@as(usize, 2), delete.expressions.len);
}

test "lexer: DETACH keyword recognized" {
    var lexer = Lexer.init("DETACH DELETE");

    const t1 = lexer.nextToken();
    try std.testing.expectEqual(TokenType.kw_detach, t1.token_type);

    const t2 = lexer.nextToken();
    try std.testing.expectEqual(TokenType.kw_delete, t2.token_type);
}

test "parser: CREATE with edge pattern parses" {
    var parser = Parser.init(std.testing.allocator, "CREATE (a:Person)-[:KNOWS]->(b:Person)");
    defer parser.deinit();

    const result = parser.parse();
    try std.testing.expect(result.query != null);
    try std.testing.expectEqual(@as(usize, 0), result.errors.len);
    try std.testing.expectEqual(@as(usize, 1), result.query.?.clauses.len);
    try std.testing.expect(result.query.?.clauses[0] == .create);

    const create = result.query.?.clauses[0].create;
    try std.testing.expectEqual(@as(usize, 1), create.patterns.len);

    // Pattern should have: node, edge, node
    const pattern = create.patterns[0];
    try std.testing.expectEqual(@as(usize, 3), pattern.elements.len);
    try std.testing.expect(pattern.elements[0] == .node);
    try std.testing.expect(pattern.elements[1] == .edge);
    try std.testing.expect(pattern.elements[2] == .node);

    // Check edge type
    const edge = pattern.elements[1].edge;
    try std.testing.expectEqual(@as(usize, 1), edge.types.len);
    try std.testing.expectEqualStrings("KNOWS", edge.types[0]);
}

test "parser: CREATE edge between existing nodes parses" {
    // This pattern is used after MATCH to create edge between matched nodes
    var parser = Parser.init(std.testing.allocator, "MATCH (a:Person), (b:Person) CREATE (a)-[:KNOWS]->(b)");
    defer parser.deinit();

    const result = parser.parse();
    try std.testing.expect(result.query != null);
    try std.testing.expectEqual(@as(usize, 0), result.errors.len);
    try std.testing.expectEqual(@as(usize, 2), result.query.?.clauses.len);

    // First clause is MATCH
    try std.testing.expect(result.query.?.clauses[0] == .match);
    // Second clause is CREATE
    try std.testing.expect(result.query.?.clauses[1] == .create);
}

test "parser: DELETE edge variable parses" {
    var parser = Parser.init(std.testing.allocator, "MATCH (a)-[r:KNOWS]->(b) DELETE r");
    defer parser.deinit();

    const result = parser.parse();
    try std.testing.expect(result.query != null);
    try std.testing.expectEqual(@as(usize, 0), result.errors.len);
    try std.testing.expectEqual(@as(usize, 2), result.query.?.clauses.len);

    // Second clause is DELETE
    const delete = result.query.?.clauses[1].delete;
    try std.testing.expectEqual(@as(usize, 1), delete.expressions.len);
    try std.testing.expect(delete.expressions[0].* == .variable);
    try std.testing.expectEqualStrings("r", delete.expressions[0].variable.name);
}

test "parser: CREATE with incoming edge direction parses" {
    var parser = Parser.init(std.testing.allocator, "CREATE (a:Person)<-[:KNOWS]-(b:Person)");
    defer parser.deinit();

    const result = parser.parse();
    try std.testing.expect(result.query != null);
    try std.testing.expectEqual(@as(usize, 0), result.errors.len);

    const create = result.query.?.clauses[0].create;
    const pattern = create.patterns[0];
    const edge = pattern.elements[1].edge;

    // Check it's an incoming edge
    try std.testing.expectEqual(parser_mod.EdgeDirection.incoming, edge.direction);
}

// ============================================================================
// SET Clause Tests
// ============================================================================

test "parser: SET property parses" {
    var parser = Parser.init(std.testing.allocator, "MATCH (n:Person) SET n.age = 30");
    defer parser.deinit();

    const result = parser.parse();
    try std.testing.expect(result.query != null);
    try std.testing.expectEqual(@as(usize, 0), result.errors.len);
    try std.testing.expectEqual(@as(usize, 2), result.query.?.clauses.len);

    // Second clause is SET
    try std.testing.expect(result.query.?.clauses[1] == .set);
    const set = result.query.?.clauses[1].set;
    try std.testing.expectEqual(@as(usize, 1), set.items.len);

    // First item is a property assignment
    const item = set.items[0];
    try std.testing.expect(item == .property);
    try std.testing.expectEqualStrings("age", item.property.property_name);
}

test "parser: SET multiple properties parses" {
    var parser = Parser.init(std.testing.allocator, "MATCH (n:Person) SET n.age = 30, n.name = \"Alice\"");
    defer parser.deinit();

    const result = parser.parse();
    try std.testing.expect(result.query != null);
    try std.testing.expectEqual(@as(usize, 0), result.errors.len);

    const set = result.query.?.clauses[1].set;
    try std.testing.expectEqual(@as(usize, 2), set.items.len);

    // Both items are property assignments
    try std.testing.expect(set.items[0] == .property);
    try std.testing.expect(set.items[1] == .property);
    try std.testing.expectEqualStrings("age", set.items[0].property.property_name);
    try std.testing.expectEqualStrings("name", set.items[1].property.property_name);
}

test "parser: SET labels parses" {
    var parser = Parser.init(std.testing.allocator, "MATCH (n:Person) SET n:Admin:Verified");
    defer parser.deinit();

    const result = parser.parse();
    try std.testing.expect(result.query != null);
    try std.testing.expectEqual(@as(usize, 0), result.errors.len);

    const set = result.query.?.clauses[1].set;
    try std.testing.expectEqual(@as(usize, 1), set.items.len);

    // Item is a labels assignment
    const item = set.items[0];
    try std.testing.expect(item == .labels);
    try std.testing.expectEqual(@as(usize, 2), item.labels.label_names.len);
    try std.testing.expectEqualStrings("Admin", item.labels.label_names[0]);
    try std.testing.expectEqualStrings("Verified", item.labels.label_names[1]);
}

test "parser: SET replace properties parses" {
    var parser = Parser.init(std.testing.allocator, "MATCH (n:Person) SET n = {name: \"Alice\", age: 30}");
    defer parser.deinit();

    const result = parser.parse();
    try std.testing.expect(result.query != null);
    try std.testing.expectEqual(@as(usize, 0), result.errors.len);

    const set = result.query.?.clauses[1].set;
    try std.testing.expectEqual(@as(usize, 1), set.items.len);

    // Item is a replace_properties assignment
    const item = set.items[0];
    try std.testing.expect(item == .replace_properties);
    try std.testing.expect(item.replace_properties.map.* == .map);
}

test "parser: SET merge properties parses" {
    var parser = Parser.init(std.testing.allocator, "MATCH (n:Person) SET n += {email: \"alice@example.com\"}");
    defer parser.deinit();

    const result = parser.parse();
    try std.testing.expect(result.query != null);
    try std.testing.expectEqual(@as(usize, 0), result.errors.len);

    const set = result.query.?.clauses[1].set;
    try std.testing.expectEqual(@as(usize, 1), set.items.len);

    // Item is a merge_properties assignment
    const item = set.items[0];
    try std.testing.expect(item == .merge_properties);
    try std.testing.expect(item.merge_properties.map.* == .map);
}

test "lexer: += tokenized as single operator" {
    var lexer = Lexer.init("n += {a: 1}");

    const t1 = lexer.nextToken();
    try std.testing.expectEqual(TokenType.identifier, t1.token_type);

    const t2 = lexer.nextToken();
    try std.testing.expectEqual(TokenType.plus_equal, t2.token_type);
    try std.testing.expectEqualStrings("+=", t2.text);
}
