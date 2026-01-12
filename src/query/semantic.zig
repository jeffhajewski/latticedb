//! Semantic analysis for Cypher queries.
//!
//! Validates parsed queries by:
//! - Tracking variable bindings from MATCH patterns
//! - Checking variable references in WHERE/RETURN/ORDER BY
//! - Type checking expressions and operators
//! - Validating LIMIT/SKIP arguments

const std = @import("std");
const Allocator = std.mem.Allocator;

const ast = @import("ast.zig");
const parser_types = @import("parser.zig");
const BinaryOp = parser_types.BinaryOp;
const UnaryOp = parser_types.UnaryOp;

// ============================================================================
// Types
// ============================================================================

/// Kind of variable (where it was introduced)
pub const VariableKind = enum {
    node,
    edge,
    alias,
};

/// Information about a bound variable
pub const VariableInfo = struct {
    name: []const u8,
    kind: VariableKind,
    location: ast.SourceLocation,
};

/// Semantic error codes
pub const ErrorCode = enum {
    unbound_variable,
    duplicate_variable,
    invalid_operator_types,
    non_integer_limit,
    non_boolean_condition,
    invalid_property_access,
    invalid_delete_target,
    invalid_set_target,
    invalid_remove_target,
    internal_error,
};

/// A semantic error with location info
pub const SemanticError = struct {
    code: ErrorCode,
    message: []const u8,
    location: ast.SourceLocation,

    pub fn format(self: SemanticError, writer: anytype) !void {
        try writer.print("{d}:{d}: error: {s}", .{ self.location.line, self.location.column, self.message });
    }
};

/// Result of semantic analysis
pub const AnalysisResult = struct {
    success: bool,
    errors: []const SemanticError,
    variables: []const VariableInfo,
    /// True if some errors were dropped due to OOM
    errors_dropped: bool,
};

/// Inferred type for expressions
pub const InferredType = enum {
    unknown,
    boolean,
    integer,
    float,
    string,
    list,
    map,
    node,
    edge,
    any, // Schema-free property access
};

// ============================================================================
// Semantic Analyzer
// ============================================================================

/// Semantic analyzer for Cypher queries
pub const SemanticAnalyzer = struct {
    allocator: Allocator,
    errors: std.ArrayList(SemanticError),
    variables: std.StringHashMap(VariableInfo),
    variable_list: std.ArrayList(VariableInfo),
    /// Tracks if any errors were dropped due to OOM
    errors_dropped: bool,

    const Self = @This();

    /// Initialize a new semantic analyzer
    pub fn init(allocator: Allocator) Self {
        return .{
            .allocator = allocator,
            .errors = .empty,
            .variables = std.StringHashMap(VariableInfo).init(allocator),
            .variable_list = .empty,
            .errors_dropped = false,
        };
    }

    /// Free analyzer resources
    pub fn deinit(self: *Self) void {
        // Free error messages
        for (self.errors.items) |err| {
            self.allocator.free(err.message);
        }
        self.errors.deinit(self.allocator);
        self.variables.deinit();
        self.variable_list.deinit(self.allocator);
    }

    /// Analyze a parsed query
    pub fn analyze(self: *Self, query: *const ast.Query) AnalysisResult {
        // Process clauses in order
        for (query.clauses) |clause| {
            switch (clause) {
                .match => |m| self.analyzeMatchClause(m),
                .where => |w| self.analyzeWhereClause(w),
                .return_ => |r| self.analyzeReturnClause(r),
                .order_by => |o| self.analyzeOrderByClause(o),
                .limit => |l| self.analyzeLimitClause(l),
                .skip => |s| self.analyzeSkipClause(s),
                .create => |c| self.analyzeCreateClause(c),
                .delete => |d| self.analyzeDeleteClause(d),
                .set => |s| self.analyzeSetClause(s),
                .remove => |r| self.analyzeRemoveClause(r),
            }
        }

        return .{
            .success = self.errors.items.len == 0 and !self.errors_dropped,
            .errors = self.errors.items,
            .variables = self.variable_list.items,
            .errors_dropped = self.errors_dropped,
        };
    }

    // ========================================================================
    // Clause Analysis
    // ========================================================================

    fn analyzeMatchClause(self: *Self, clause: *const ast.MatchClause) void {
        for (clause.patterns) |pattern| {
            for (pattern.elements) |element| {
                switch (element) {
                    .node => |n| self.registerNodeVariable(n),
                    .edge => |e| self.registerEdgeVariable(e),
                }
            }
        }
    }

    fn analyzeWhereClause(self: *Self, clause: *const ast.WhereClause) void {
        self.analyzeExpression(clause.condition);

        // WHERE condition should be boolean
        const cond_type = self.inferType(clause.condition);
        // Allow any/unknown since we're schema-free and can't always determine types statically
        // Also allow float since vector distance comparisons might parse ambiguously
        if (cond_type != .boolean and cond_type != .any and cond_type != .unknown and cond_type != .float) {
            self.addError(.non_boolean_condition, clause.location, "WHERE condition must be boolean");
        }
    }

    fn analyzeReturnClause(self: *Self, clause: *const ast.ReturnClause) void {
        for (clause.items) |item| {
            self.analyzeExpression(item.expression);
        }
    }

    fn analyzeOrderByClause(self: *Self, clause: *const ast.OrderByClause) void {
        for (clause.items) |item| {
            self.analyzeExpression(item.expression);
        }
    }

    fn analyzeLimitClause(self: *Self, clause: *const ast.LimitClause) void {
        self.analyzeExpression(clause.count);

        const expr_type = self.inferType(clause.count);
        if (expr_type != .integer and expr_type != .any and expr_type != .unknown) {
            self.addError(.non_integer_limit, clause.location, "LIMIT requires integer expression");
        }
    }

    fn analyzeSkipClause(self: *Self, clause: *const ast.SkipClause) void {
        self.analyzeExpression(clause.count);

        const expr_type = self.inferType(clause.count);
        if (expr_type != .integer and expr_type != .any and expr_type != .unknown) {
            self.addError(.non_integer_limit, clause.location, "SKIP requires integer expression");
        }
    }

    fn analyzeCreateClause(self: *Self, clause: *const ast.CreateClause) void {
        // Register variables from CREATE patterns (creates new nodes/edges)
        for (clause.patterns) |pattern| {
            for (pattern.elements) |element| {
                switch (element) {
                    .node => |n| {
                        self.registerNodeVariable(n);
                        // Analyze property expressions
                        if (n.properties) |props| {
                            for (props) |prop| {
                                self.analyzeExpression(prop.value);
                            }
                        }
                    },
                    .edge => |e| {
                        self.registerEdgeVariable(e);
                        // Analyze property expressions
                        if (e.properties) |props| {
                            for (props) |prop| {
                                self.analyzeExpression(prop.value);
                            }
                        }
                    },
                }
            }
        }
    }

    fn analyzeDeleteClause(self: *Self, clause: *const ast.DeleteClause) void {
        _ = clause.detach; // detach flag doesn't affect semantic analysis

        // Each expression should be a bound variable
        for (clause.expressions) |expr| {
            self.analyzeExpression(expr);

            // Verify it's a variable reference (not a complex expression)
            if (expr.* != .variable) {
                self.addError(.invalid_delete_target, expr.getLocation(), "DELETE requires variable references");
            }
        }
    }

    fn analyzeSetClause(self: *Self, clause: *const ast.SetClause) void {
        for (clause.items) |item| {
            switch (item) {
                .property => |p| {
                    // Verify target is a bound variable
                    self.analyzeExpression(p.target);
                    if (p.target.* != .variable) {
                        self.addError(.invalid_set_target, p.target.getLocation(), "SET property target must be a variable");
                    }
                    // Analyze value expression
                    self.analyzeExpression(p.value);
                },
                .labels => |l| {
                    // Verify target is a bound node variable
                    self.analyzeExpression(l.target);
                    if (l.target.* != .variable) {
                        self.addError(.invalid_set_target, l.target.getLocation(), "SET labels target must be a variable");
                    } else {
                        // Check it's a node (not an edge)
                        const var_name = l.target.variable.name;
                        if (self.variables.get(var_name)) |info| {
                            if (info.kind == .edge) {
                                self.addError(.invalid_set_target, l.target.getLocation(), "Cannot SET labels on an edge");
                            }
                        }
                    }
                },
                .replace_properties => |r| {
                    self.analyzeExpression(r.target);
                    if (r.target.* != .variable) {
                        self.addError(.invalid_set_target, r.target.getLocation(), "SET = target must be a variable");
                    }
                    self.analyzeExpression(r.map);
                },
                .merge_properties => |m| {
                    self.analyzeExpression(m.target);
                    if (m.target.* != .variable) {
                        self.addError(.invalid_set_target, m.target.getLocation(), "SET += target must be a variable");
                    }
                    self.analyzeExpression(m.map);
                },
            }
        }
    }

    fn analyzeRemoveClause(self: *Self, clause: *const ast.RemoveClause) void {
        for (clause.items) |item| {
            switch (item) {
                .property => |p| {
                    // Verify target is a bound variable
                    self.analyzeExpression(p.target);
                    if (p.target.* != .variable) {
                        self.addError(.invalid_remove_target, p.target.getLocation(), "REMOVE property target must be a variable");
                    }
                },
                .labels => |l| {
                    // Verify target is a bound node variable
                    self.analyzeExpression(l.target);
                    if (l.target.* != .variable) {
                        self.addError(.invalid_remove_target, l.target.getLocation(), "REMOVE labels target must be a variable");
                    } else {
                        // Check it's a node (not an edge)
                        const var_name = l.target.variable.name;
                        if (self.variables.get(var_name)) |info| {
                            if (info.kind == .edge) {
                                self.addError(.invalid_remove_target, l.target.getLocation(), "Cannot REMOVE labels from an edge");
                            }
                        }
                    }
                },
            }
        }
    }

    // ========================================================================
    // Variable Registration
    // ========================================================================

    fn registerNodeVariable(self: *Self, node: *const ast.NodePattern) void {
        if (node.variable) |name| {
            self.registerVariable(name, .node, node.location);
        }

        // Analyze property expressions
        if (node.properties) |props| {
            for (props) |prop| {
                self.analyzeExpression(prop.value);
            }
        }
    }

    fn registerEdgeVariable(self: *Self, edge: *const ast.EdgePattern) void {
        if (edge.variable) |name| {
            self.registerVariable(name, .edge, edge.location);
        }

        // Analyze property expressions
        if (edge.properties) |props| {
            for (props) |prop| {
                self.analyzeExpression(prop.value);
            }
        }
    }

    fn registerVariable(self: *Self, name: []const u8, kind: VariableKind, location: ast.SourceLocation) void {
        if (self.variables.contains(name)) {
            self.addErrorFmt(.duplicate_variable, location, "Variable '{s}' is already defined", .{name});
        } else {
            const info = VariableInfo{
                .name = name,
                .kind = kind,
                .location = location,
            };
            self.variables.put(name, info) catch |err| {
                self.addErrorFmt(.internal_error, location, "Failed to register variable '{s}': out of memory ({any})", .{ name, err });
                return;
            };
            self.variable_list.append(self.allocator, info) catch |err| {
                self.addErrorFmt(.internal_error, location, "Failed to track variable '{s}': out of memory ({any})", .{ name, err });
            };
        }
    }

    // ========================================================================
    // Expression Analysis
    // ========================================================================

    fn analyzeExpression(self: *Self, expr: *const ast.Expression) void {
        switch (expr.*) {
            .literal => {},
            .parameter => {},
            .variable => |v| self.checkVariableBound(v),
            .property_access => |pa| self.analyzePropertyAccess(pa),
            .binary => |b| self.analyzeBinaryExpr(b),
            .unary => |u| self.analyzeUnaryExpr(u),
            .function_call => |f| self.analyzeFunctionCall(f),
            .list => |l| self.analyzeListExpr(l),
            .map => |m| self.analyzeMapExpr(m),
        }
    }

    fn checkVariableBound(self: *Self, ref: ast.VariableRef) void {
        if (!self.variables.contains(ref.name)) {
            self.addErrorFmt(.unbound_variable, ref.location, "Variable '{s}' is not defined", .{ref.name});
        }
    }

    fn analyzePropertyAccess(self: *Self, pa: *const ast.PropertyAccess) void {
        // Analyzing the object will already check for unbound variables
        self.analyzeExpression(pa.object);
    }

    fn analyzeBinaryExpr(self: *Self, expr: *const ast.BinaryExpr) void {
        self.analyzeExpression(expr.left);
        self.analyzeExpression(expr.right);
        self.checkBinaryOpTypes(expr);
    }

    fn analyzeUnaryExpr(self: *Self, expr: *const ast.UnaryExpr) void {
        self.analyzeExpression(expr.operand);
        // Type checking for unary ops is simpler - NOT needs boolean, NEG needs numeric
    }

    fn analyzeFunctionCall(self: *Self, func: *const ast.FunctionCall) void {
        for (func.arguments) |arg| {
            self.analyzeExpression(arg);
        }
        // Could add function signature validation here
    }

    fn analyzeListExpr(self: *Self, list: *const ast.ListExpr) void {
        for (list.elements) |elem| {
            self.analyzeExpression(elem);
        }
    }

    fn analyzeMapExpr(self: *Self, map: *const ast.MapExpr) void {
        for (map.entries) |entry| {
            self.analyzeExpression(entry.value);
        }
    }

    // ========================================================================
    // Type Inference
    // ========================================================================

    fn inferType(self: *Self, expr: *const ast.Expression) InferredType {
        return switch (expr.*) {
            .literal => |l| inferLiteralType(l),
            .parameter => .any, // Parameters could be any type
            .variable => |v| self.getVariableType(v.name),
            .property_access => .any, // Schema-free: properties can be any type
            .binary => |b| self.inferBinaryType(b),
            .unary => |u| self.inferUnaryType(u),
            .function_call => .any, // Would need function registry for precise types
            .list => .list,
            .map => .map,
        };
    }

    fn inferLiteralType(lit: ast.Literal) InferredType {
        return switch (lit.value) {
            .integer => .integer,
            .float => .float,
            .string => .string,
            .boolean => .boolean,
            .null_value => .unknown,
        };
    }

    fn getVariableType(self: *Self, name: []const u8) InferredType {
        if (self.variables.get(name)) |info| {
            return switch (info.kind) {
                .node => .node,
                .edge => .edge,
                .alias => .any,
            };
        }
        return .unknown;
    }

    fn inferBinaryType(self: *Self, expr: *const ast.BinaryExpr) InferredType {
        _ = self;
        return switch (expr.operator) {
            // Comparison operators return boolean
            .eq, .neq, .lt, .lte, .gt, .gte => .boolean,
            // Logical operators return boolean
            .and_, .or_, .xor => .boolean,
            // String operators return boolean
            .contains, .starts_with, .ends_with, .regex_match => .boolean,
            // IN returns boolean
            .in_ => .boolean,
            // Arithmetic - could be int or float
            .add, .sub, .mul, .div, .mod, .pow => .any,
            // Special operators
            .vector_distance => .float,
            .fts_match => .boolean,
        };
    }

    fn inferUnaryType(self: *Self, expr: *const ast.UnaryExpr) InferredType {
        return switch (expr.operator) {
            .not => .boolean,
            .neg => self.inferType(expr.operand),
            .is_null, .is_not_null => .boolean,
        };
    }

    // ========================================================================
    // Type Checking
    // ========================================================================

    fn checkBinaryOpTypes(self: *Self, expr: *const ast.BinaryExpr) void {
        const left_type = self.inferType(expr.left);
        const right_type = self.inferType(expr.right);

        // Skip validation if either side is unknown/any (schema-free)
        if (left_type == .any or left_type == .unknown) return;
        if (right_type == .any or right_type == .unknown) return;

        const valid = switch (expr.operator) {
            // Equality: any types can be compared
            .eq, .neq => true,

            // Comparison: need orderable types
            .lt, .lte, .gt, .gte => isOrderable(left_type) and isOrderable(right_type),

            // Logical: need booleans
            .and_, .or_, .xor => left_type == .boolean and right_type == .boolean,

            // Arithmetic: need numeric
            .add, .sub, .mul, .div, .mod, .pow => isNumeric(left_type) and isNumeric(right_type),

            // String operations
            .contains, .starts_with, .ends_with, .regex_match => left_type == .string and right_type == .string,

            // IN: right side must be list
            .in_ => right_type == .list,

            // Special operators - validated at runtime
            .vector_distance, .fts_match => true,
        };

        if (!valid) {
            self.addError(.invalid_operator_types, expr.location, "Operator not valid for these types");
        }
    }

    // ========================================================================
    // Error Helpers
    // ========================================================================

    fn addError(self: *Self, code: ErrorCode, location: ast.SourceLocation, message: []const u8) void {
        const msg_copy = self.allocator.dupe(u8, message) catch {
            self.errors_dropped = true;
            return;
        };
        self.errors.append(self.allocator, .{
            .code = code,
            .message = msg_copy,
            .location = location,
        }) catch {
            self.allocator.free(msg_copy);
            self.errors_dropped = true;
        };
    }

    fn addErrorFmt(self: *Self, code: ErrorCode, location: ast.SourceLocation, comptime fmt: []const u8, args: anytype) void {
        const message = std.fmt.allocPrint(self.allocator, fmt, args) catch {
            self.errors_dropped = true;
            return;
        };
        self.errors.append(self.allocator, .{
            .code = code,
            .message = message,
            .location = location,
        }) catch {
            self.allocator.free(message);
            self.errors_dropped = true;
        };
    }
};

// ============================================================================
// Type Helpers
// ============================================================================

fn isNumeric(t: InferredType) bool {
    return t == .integer or t == .float;
}

fn isOrderable(t: InferredType) bool {
    return t == .integer or t == .float or t == .string;
}

// ============================================================================
// Tests
// ============================================================================

const Parser = parser_types.Parser;

test "valid simple query" {
    const allocator = std.testing.allocator;
    const source = "MATCH (n) RETURN n";

    var parser = Parser.init(allocator, source);
    defer parser.deinit();
    const parse_result = parser.parse();

    if (parse_result.query) |query| {
        var analyzer = SemanticAnalyzer.init(allocator);
        defer analyzer.deinit();
        const result = analyzer.analyze(query);

        try std.testing.expect(result.success);
        try std.testing.expectEqual(@as(usize, 0), result.errors.len);
        try std.testing.expectEqual(@as(usize, 1), result.variables.len);
        try std.testing.expectEqualStrings("n", result.variables[0].name);
    } else {
        try std.testing.expect(false); // Parse should succeed
    }
}

test "unbound variable error" {
    const allocator = std.testing.allocator;
    const source = "MATCH (n) WHERE x.name = \"foo\" RETURN n";

    var parser = Parser.init(allocator, source);
    defer parser.deinit();
    const parse_result = parser.parse();

    if (parse_result.query) |query| {
        var analyzer = SemanticAnalyzer.init(allocator);
        defer analyzer.deinit();
        const result = analyzer.analyze(query);

        try std.testing.expect(!result.success);
        try std.testing.expectEqual(@as(usize, 1), result.errors.len);
        try std.testing.expectEqual(ErrorCode.unbound_variable, result.errors[0].code);
    } else {
        try std.testing.expect(false);
    }
}

test "duplicate variable error" {
    const allocator = std.testing.allocator;
    const source = "MATCH (n), (n:Person) RETURN n";

    var parser = Parser.init(allocator, source);
    defer parser.deinit();
    const parse_result = parser.parse();

    if (parse_result.query) |query| {
        var analyzer = SemanticAnalyzer.init(allocator);
        defer analyzer.deinit();
        const result = analyzer.analyze(query);

        try std.testing.expect(!result.success);
        try std.testing.expectEqual(@as(usize, 1), result.errors.len);
        try std.testing.expectEqual(ErrorCode.duplicate_variable, result.errors[0].code);
    } else {
        try std.testing.expect(false);
    }
}

test "valid relationship pattern" {
    const allocator = std.testing.allocator;
    const source = "MATCH (a)-[r:KNOWS]->(b) RETURN a, r, b";

    var parser = Parser.init(allocator, source);
    defer parser.deinit();
    const parse_result = parser.parse();

    if (parse_result.query) |query| {
        var analyzer = SemanticAnalyzer.init(allocator);
        defer analyzer.deinit();
        const result = analyzer.analyze(query);

        try std.testing.expect(result.success);
        try std.testing.expectEqual(@as(usize, 3), result.variables.len);
    } else {
        try std.testing.expect(false);
    }
}

test "valid where clause" {
    const allocator = std.testing.allocator;
    const source = "MATCH (n:Person) WHERE n.age > 30 RETURN n";

    var parser = Parser.init(allocator, source);
    defer parser.deinit();
    const parse_result = parser.parse();

    if (parse_result.query) |query| {
        var analyzer = SemanticAnalyzer.init(allocator);
        defer analyzer.deinit();
        const result = analyzer.analyze(query);

        try std.testing.expect(result.success);
    } else {
        try std.testing.expect(false);
    }
}

test "unbound in return" {
    const allocator = std.testing.allocator;
    const source = "MATCH (n) RETURN x";

    var parser = Parser.init(allocator, source);
    defer parser.deinit();
    const parse_result = parser.parse();

    if (parse_result.query) |query| {
        var analyzer = SemanticAnalyzer.init(allocator);
        defer analyzer.deinit();
        const result = analyzer.analyze(query);

        try std.testing.expect(!result.success);
        try std.testing.expectEqual(ErrorCode.unbound_variable, result.errors[0].code);
    } else {
        try std.testing.expect(false);
    }
}

test "vector distance query" {
    const allocator = std.testing.allocator;
    const source = "MATCH (c:Chunk) WHERE c.embedding <=> $query < 0.5 RETURN c";

    var parser = Parser.init(allocator, source);
    defer parser.deinit();
    const parse_result = parser.parse();

    if (parse_result.query) |query| {
        var analyzer = SemanticAnalyzer.init(allocator);
        defer analyzer.deinit();
        const result = analyzer.analyze(query);

        try std.testing.expect(result.success);
    } else {
        try std.testing.expect(false);
    }
}

test "fts match query" {
    const allocator = std.testing.allocator;
    const source = "MATCH (d:Doc) WHERE d.text @@ \"search\" RETURN d";

    var parser = Parser.init(allocator, source);
    defer parser.deinit();
    const parse_result = parser.parse();

    if (parse_result.query) |query| {
        var analyzer = SemanticAnalyzer.init(allocator);
        defer analyzer.deinit();
        const result = analyzer.analyze(query);

        try std.testing.expect(result.success);
    } else {
        try std.testing.expect(false);
    }
}
