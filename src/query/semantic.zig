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
const procedures = @import("procedures.zig");
const ast_util = @import("ast_util.zig");
const parser_types = @import("parser.zig");
const expression = @import("expression.zig");
const aggregate_ops = @import("operators/aggregate.zig");
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
    unsupported_pattern,
    /// A call to a function that does not exist.
    unknown_function,
    /// A call with more or fewer arguments than the function takes.
    wrong_argument_count,
    /// An aggregate where there are no rows to aggregate over: WHERE, SET...
    invalid_aggregate,
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
    /// The scopes around each CALL { } being analysed, innermost last. A
    /// variable inside one may not take a name from them that it did not
    /// import.
    enclosing: std.ArrayList(Enclosing) = .empty,
    /// The clause being analysed when it is one where an aggregate has no
    /// rows to aggregate over (WHERE, SET, UNWIND, ...); null in RETURN,
    /// WITH and ORDER BY, where the planner places them.
    aggregates_refused_in: ?[]const u8 = null,

    const Enclosing = struct {
        variables: *const std.StringHashMap(VariableInfo),
        /// What the subquery is called in messages; null for CALL { },
        /// whose message says how to import.
        subquery: ?[]const u8 = null,
    };

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
        self.enclosing.deinit(self.allocator);
    }

    /// Analyze a parsed query
    pub fn analyze(self: *Self, query: *const ast.Query) AnalysisResult {
        self.analyzeClauses(query.clauses);
        // Each single query of a UNION sees only its own variables.
        for (query.unions) |branch| {
            self.variables.clearRetainingCapacity();
            self.analyzeClauses(branch.clauses);
        }

        return .{
            .success = self.errors.items.len == 0 and !self.errors_dropped,
            .errors = self.errors.items,
            .variables = self.variable_list.items,
            .errors_dropped = self.errors_dropped,
        };
    }

    fn analyzeClauses(self: *Self, clauses: []const ast.Clause) void {
        const saved_refusal = self.aggregates_refused_in;
        defer self.aggregates_refused_in = saved_refusal;
        // Process clauses in order
        for (clauses, 0..) |clause, i| {
            self.aggregates_refused_in = switch (clause) {
                .match => "MATCH",
                .where => "WHERE",
                .create => "CREATE",
                .delete => "DELETE",
                .set => "SET",
                .remove => "REMOVE",
                .merge => "MERGE",
                .unwind => "UNWIND",
                .foreach => "FOREACH",
                .limit => "LIMIT",
                .skip => "SKIP",
                .call => "CALL",
                .return_, .with, .order_by, .call_subquery => null,
            };
            switch (clause) {
                .match => |m| self.analyzeMatchClause(m),
                .where => |w| {
                    self.analyzeWhereClause(w);
                    if (i > 0 and clauses[i - 1] == .match) self.checkShortestWhere(clauses[i - 1].match, w);
                },
                .return_ => |r| self.analyzeReturnClause(r),
                .order_by => |o| self.analyzeOrderByClause(o),
                .limit => |l| self.analyzeLimitClause(l),
                .skip => |s| self.analyzeSkipClause(s),
                .create => |c| self.analyzeCreateClause(c),
                .delete => |d| self.analyzeDeleteClause(d),
                .set => |s| self.analyzeSetClause(s),
                .remove => |r| self.analyzeRemoveClause(r),
                .with => |w| self.analyzeWithClause(w),
                .merge => |m| self.analyzeMergeClause(m),
                .unwind => |u| self.analyzeUnwindClause(u),
                .foreach => |f| self.analyzeForeach(f),
                .call_subquery => |sq| self.analyzeCallSubquery(sq),
                .call => |c| {
                    for (c.arguments) |arg| self.analyzeExpression(arg);
                    if (c.yields) |items| {
                        for (items) |item| self.registerVariable(item.alias orelse item.column, .alias, item.location);
                    } else if (c.yield_all) {
                        if (procedures.Procedure.find(c.procedure)) |p| for (p.columns()) |column| self.registerVariable(column, .alias, c.location);
                    }
                },
            }
        }
    }

    // ========================================================================
    // Clause Analysis
    // ========================================================================

    fn analyzeMatchClause(self: *Self, clause: *const ast.MatchClause) void {
        for (clause.patterns) |pattern| {
            if (pattern.shortest != .none) self.checkShortestPattern(pattern);
            for (pattern.elements) |element| {
                switch (element) {
                    .node => |n| self.registerMatchNode(n),
                    .edge => |e| {
                        self.validateEdgePattern(e, true, "MATCH");
                        if (e.variable) |name| {
                            if (self.variables.get(name)) |info| {
                                self.addErrorFmt(
                                    .duplicate_variable,
                                    e.location,
                                    "Variable '{s}' is already bound to {s}; a relationship variable cannot be matched again",
                                    .{ name, kindPhrase(info.kind) },
                                );
                                continue;
                            }
                        }
                        self.registerEdgeVariable(e);
                    },
                }
            }
            if (pattern.path_variable) |name| self.registerVariable(name, .alias, pattern.path_location);
        }
    }

    /// shortestPath(): one relationship between two nodes, at least zero or
    /// one long, without a property map.
    fn checkShortestPattern(self: *Self, pattern: ast.Pattern) void {
        const loc = pattern.elements[0].node.location;
        if (pattern.elements.len != 3) {
            self.addError(.unsupported_pattern, loc, "shortestPath() needs one relationship between two nodes, as in shortestPath((a)-[:R*]->(b))");
            return;
        }
        const edge = pattern.elements[1].edge;
        const min = if (edge.quantifier) |q| q.min_hops else 1;
        if (min > 1) {
            self.addErrorFmt(.unsupported_pattern, edge.location, "shortestPath() finds paths of at least 0 or 1 relationships, not {d}", .{min});
        }
        if (edge.properties != null) {
            self.addError(.unsupported_pattern, edge.location, "shortestPath() does not support a property map on its relationship; the search could not take it into account");
        }
    }

    /// A WHERE right after a MATCH filters what the MATCH found. For a
    /// shortest path it would have to steer the search instead, to find the
    /// shortest path that passes, so one that reads the path or its
    /// relationships is refused rather than answered wrongly.
    fn checkShortestWhere(self: *Self, match: *const ast.MatchClause, where: *const ast.WhereClause) void {
        var names: [32][]const u8 = undefined;
        var count: usize = 0;
        for (match.patterns) |pattern| {
            if (pattern.shortest == .none) continue;
            if (pattern.path_variable) |p| {
                if (count < names.len) names[count] = p;
                count += 1;
            }
            if (pattern.elements.len == 3) if (pattern.elements[1].edge.variable) |r| {
                if (count < names.len) names[count] = r;
                count += 1;
            };
        }
        if (count == 0) return;
        const Only = struct {
            names: []const []const u8,
            pub fn skip(self_: @This(), name: []const u8) bool {
                for (self_.names) |n| if (std.mem.eql(u8, n, name)) return false;
                return true;
            }
        };
        const only = Only{ .names = names[0..@min(count, names.len)] };
        if (ast_util.firstFreeVariableWith(where.condition, only)) |name| {
            self.addErrorFmt(
                .unsupported_pattern,
                where.condition.getLocation(),
                "A WHERE that reads `{s}` of a shortestPath() is not supported: the search finds the shortest path first and would not look for a longer one that passes",
                .{name},
            );
        }
    }

    /// A MATCH node may name a node variable that is already bound, by this
    /// clause or an earlier one: the pattern then matches that same node, as
    /// in `MATCH (a)-->(b)-->(a)` or `MATCH (a:X) MATCH (a)-->(b)`.
    fn registerMatchNode(self: *Self, node: *const ast.NodePattern) void {
        if (node.variable) |name| {
            if (self.variables.get(name)) |info| {
                if (info.kind != .node) {
                    self.addErrorFmt(
                        .duplicate_variable,
                        node.location,
                        "Variable '{s}' is already bound to {s}, not a node",
                        .{ name, kindPhrase(info.kind) },
                    );
                }
                if (node.properties) |props| {
                    for (props) |prop| self.analyzeExpression(prop.value);
                }
                return;
            }
        }
        self.registerNodeVariable(node);
    }

    fn kindPhrase(kind: VariableKind) []const u8 {
        return switch (kind) {
            .node => "a node",
            .edge => "a relationship",
            .alias => "a value",
        };
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
        for (clause.items, 0..) |item, i| {
            self.analyzeExpression(item.expression);
            // Two columns of one name could not be told apart.
            const name = item.alias orelse item.text;
            for (clause.items[0..i]) |earlier| {
                if (std.mem.eql(u8, earlier.alias orelse earlier.text, name)) {
                    self.addErrorFmt(
                        .duplicate_variable,
                        item.expression.getLocation(),
                        "Column `{s}` is returned twice; give one of them another name with AS",
                        .{name},
                    );
                    break;
                }
            }
        }

        // An alias introduced here is a name a following ORDER BY may use, as
        // in `RETURN count(d) AS papers ORDER BY papers`. Register it so the
        // reference resolves; the planner substitutes the aliased expression
        // when it builds the sort.
        for (clause.items) |item| {
            const alias = item.alias orelse continue;
            self.variables.put(alias, .{
                .name = alias,
                .kind = .alias,
                .location = item.expression.getLocation(),
            }) catch {};
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
        // Register variables from CREATE patterns.
        // Variables already bound (e.g. from MATCH) are skipped — referencing
        // an existing variable in CREATE means "use this node/edge", not
        // "create a new one".
        for (clause.patterns) |pattern| {
            if (pattern.shortest != .none) {
                self.addError(.unsupported_pattern, clause.location, "shortestPath() and allShortestPaths() are only supported in MATCH");
                return;
            }
            if (!self.checkPlainLabels(pattern, "CREATE")) return;
            for (pattern.elements) |element| {
                switch (element) {
                    .node => |n| {
                        if (n.variable) |name| {
                            if (!self.variables.contains(name)) {
                                self.registerNodeVariable(n);
                            } else if (n.labels.len > 0 or n.properties != null) {
                                // These were ignored without a word.
                                self.addErrorFmt(
                                    .duplicate_variable,
                                    n.location,
                                    "'{s}' is already bound; CREATE cannot give it labels or properties (use SET)",
                                    .{name},
                                );
                            }
                        } else {
                            self.registerNodeVariable(n);
                        }
                        // Analyze property expressions
                        if (n.properties) |props| {
                            for (props) |prop| {
                                self.analyzeExpression(prop.value);
                            }
                        }
                    },
                    .edge => |e| {
                        self.validateEdgePattern(e, false, "CREATE");
                        if (e.variable) |name| {
                            if (!self.variables.contains(name)) {
                                self.registerEdgeVariable(e);
                            }
                        } else {
                            self.registerEdgeVariable(e);
                        }
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
            } else if (self.variables.get(expr.variable.name)) |info| {
                // A value (from UNWIND or a computed WITH item) was skipped
                // without a word.
                if (info.kind == .alias) {
                    self.addErrorFmt(
                        .invalid_delete_target,
                        expr.getLocation(),
                        "DELETE needs a node or relationship; '{s}' is a value",
                        .{expr.variable.name},
                    );
                }
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

    fn analyzeWithClause(self: *Self, clause: *const ast.WithClause) void {
        // Analyze projection expressions in the incoming scope.
        for (clause.items) |item| {
            self.analyzeExpression(item.expression);
        }

        // WITH introduces a new scope composed only of projected items.
        var projected = std.ArrayList(VariableInfo).empty;
        defer projected.deinit(self.allocator);

        for (clause.items) |item| {
            const name: []const u8 = if (item.alias) |alias|
                alias
            else if (item.expression.* == .variable)
                item.expression.variable.name
            else
                continue;
            if (self.shadowsEnclosing(name, clause.location)) continue;

            for (projected.items) |existing| {
                if (std.mem.eql(u8, existing.name, name)) {
                    self.addErrorFmt(.duplicate_variable, clause.location, "Variable '{s}' is already defined", .{name});
                    break;
                }
            } else {
                const kind = self.inferWithItemKind(item);
                projected.append(self.allocator, .{
                    .name = name,
                    .kind = kind,
                    .location = clause.location,
                }) catch {
                    self.addErrorFmt(.internal_error, clause.location, "Failed to track WITH variable '{s}': out of memory", .{name});
                    return;
                };
            }
        }

        self.variables.clearRetainingCapacity();
        self.variable_list.clearRetainingCapacity();

        for (projected.items) |info| {
            self.variables.put(info.name, info) catch {
                self.addErrorFmt(.internal_error, info.location, "Failed to register WITH variable '{s}': out of memory", .{info.name});
                return;
            };
            self.variable_list.append(self.allocator, info) catch {
                self.addErrorFmt(.internal_error, info.location, "Failed to track WITH variable '{s}': out of memory", .{info.name});
                return;
            };
        }

        // WHERE after WITH must be validated against projected scope.
        if (clause.where) |condition| {
            const saved_refusal = self.aggregates_refused_in;
            defer self.aggregates_refused_in = saved_refusal;
            self.aggregates_refused_in = "WHERE";
            self.analyzeExpression(condition);
        }
    }

    fn inferWithItemKind(self: *Self, item: ast.ReturnItem) VariableKind {
        if (item.expression.* == .variable) {
            if (self.variables.get(item.expression.variable.name)) |info| {
                return info.kind;
            }
        }
        return .alias;
    }

    fn analyzeMergeClause(self: *Self, clause: *const ast.MergeClause) void {
        if (clause.pattern.shortest != .none) {
            self.addError(.unsupported_pattern, clause.location, "shortestPath() and allShortestPaths() are only supported in MATCH");
            return;
        }
        if (!self.checkPlainLabels(clause.pattern, "MERGE")) return;
        const elem_count = clause.pattern.elements.len;
        if (elem_count != 1 and elem_count != 3) {
            self.addError(
                .unsupported_pattern,
                clause.location,
                "MERGE currently supports only a single node or a single relationship pattern",
            );
            return;
        }

        if (elem_count == 3) {
            if (clause.pattern.elements[0] != .node or clause.pattern.elements[1] != .edge or clause.pattern.elements[2] != .node) {
                self.addError(
                    .unsupported_pattern,
                    clause.location,
                    "MERGE relationship pattern must be of the form (node)-[edge]->(node)",
                );
                return;
            }
        }

        const merge_set_target: ?[]const u8 = blk: {
            if (clause.pattern.elements.len == 1) {
                break :blk switch (clause.pattern.elements[0]) {
                    .node => |n| n.variable,
                    .edge => null,
                };
            }
            if (clause.pattern.elements.len == 3) {
                break :blk switch (clause.pattern.elements[1]) {
                    .edge => |e| e.variable,
                    else => null,
                };
            }
            break :blk null;
        };

        // Analyze the pattern (register variables, check expressions)
        for (clause.pattern.elements) |element| {
            switch (element) {
                .node => |n| {
                    if (n.variable) |name| {
                        if (!self.variables.contains(name)) {
                            self.registerNodeVariable(n);
                        } else if (n.properties) |props| {
                            for (props) |prop| {
                                self.analyzeExpression(prop.value);
                            }
                        }
                    } else {
                        self.registerNodeVariable(n);
                    }
                },
                .edge => |e| {
                    self.validateEdgePattern(e, false, "MERGE");
                    if (e.variable) |name| {
                        if (!self.variables.contains(name)) {
                            self.registerEdgeVariable(e);
                        } else if (e.properties) |props| {
                            for (props) |prop| {
                                self.analyzeExpression(prop.value);
                            }
                        }
                    } else {
                        self.registerEdgeVariable(e);
                    }
                },
            }
        }

        if (clause.on_create) |items| {
            self.analyzeMergeSetItems(items, merge_set_target, "ON CREATE SET");
        }
        if (clause.on_match) |items| {
            self.analyzeMergeSetItems(items, merge_set_target, "ON MATCH SET");
        }
    }

    fn analyzeUnwindClause(self: *Self, clause: *const ast.UnwindClause) void {
        // Analyze the expression
        self.analyzeExpression(clause.expression);
        // UNWIND can bind scalars, maps, or nodes/edges from list elements.
        // Track as alias/any instead of forcing node semantics.
        self.registerVariable(clause.variable, .alias, clause.location);
    }

    fn analyzeMergeSetItems(
        self: *Self,
        items: []const ast.SetItem,
        merge_target_var: ?[]const u8,
        clause_name: []const u8,
    ) void {
        if (merge_target_var == null and items.len > 0) {
            const loc = switch (items[0]) {
                .property => |p| p.target.getLocation(),
                .labels => |l| l.target.getLocation(),
                .replace_properties => |r| r.target.getLocation(),
                .merge_properties => |m| m.target.getLocation(),
            };
            self.addErrorFmt(
                .unsupported_pattern,
                loc,
                "{s} requires a variable on the MERGE target pattern",
                .{clause_name},
            );
        }

        for (items) |item| {
            switch (item) {
                .property => |p| {
                    self.analyzeExpression(p.target);
                    self.analyzeExpression(p.value);

                    if (merge_target_var) |expected| {
                        const actual: ?[]const u8 = switch (p.target.*) {
                            .variable => p.target.variable.name,
                            else => null,
                        };
                        if (actual == null or !std.mem.eql(u8, actual.?, expected)) {
                            self.addErrorFmt(
                                .unsupported_pattern,
                                p.target.getLocation(),
                                "{s} currently supports only assignments to '{s}.*'",
                                .{ clause_name, expected },
                            );
                        }
                    }
                },
                .labels => |l| {
                    self.analyzeExpression(l.target);
                    self.addErrorFmt(
                        .unsupported_pattern,
                        l.target.getLocation(),
                        "{s} currently supports only property assignments",
                        .{clause_name},
                    );
                },
                .replace_properties => |r| {
                    self.analyzeExpression(r.target);
                    self.analyzeExpression(r.map);
                    self.addErrorFmt(
                        .unsupported_pattern,
                        r.target.getLocation(),
                        "{s} currently supports only property assignments",
                        .{clause_name},
                    );
                },
                .merge_properties => |m| {
                    self.analyzeExpression(m.target);
                    self.analyzeExpression(m.map);
                    self.addErrorFmt(
                        .unsupported_pattern,
                        m.target.getLocation(),
                        "{s} currently supports only property assignments",
                        .{clause_name},
                    );
                },
            }
        }
    }

    fn validateEdgePattern(
        self: *Self,
        edge: *const ast.EdgePattern,
        allow_in_clause: bool,
        clause_name: []const u8,
    ) void {
        if (!std.mem.eql(u8, clause_name, "MATCH")) {
            if (edge.types.len == 0) {
                self.addErrorFmt(
                    .unsupported_pattern,
                    edge.location,
                    "{s} relationships require an explicit type",
                    .{clause_name},
                );
            }

            if (edge.direction == .both) {
                self.addErrorFmt(
                    .unsupported_pattern,
                    edge.location,
                    "{s} does not support undirected relationship patterns (<-[]->)",
                    .{clause_name},
                );
            }
        }

        // MATCH may name several types to accept any of them. A relationship
        // that CREATE or MERGE might make needs exactly one.
        if (edge.types.len > 1 and !std.mem.eql(u8, clause_name, "MATCH")) {
            self.addErrorFmt(
                .unsupported_pattern,
                edge.location,
                "{s} relationships need exactly one type; '|' is only allowed in MATCH",
                .{clause_name},
            );
        }

        const quant = edge.quantifier orelse return;

        if (!allow_in_clause) {
            self.addErrorFmt(
                .unsupported_pattern,
                edge.location,
                "Variable-length relationships are only supported in MATCH (found in {s})",
                .{clause_name},
            );
            return;
        }

        if (quant.max_hops) |max_hops| {
            if (max_hops < quant.min_hops) {
                self.addErrorFmt(
                    .unsupported_pattern,
                    edge.location,
                    "Invalid variable-length range: min hops ({d}) exceeds max hops ({d})",
                    .{ quant.min_hops, max_hops },
                );
                return;
            }
        }
    }

    // ========================================================================
    // Variable Registration
    // ========================================================================

    /// CREATE and MERGE give a node the labels they name, so a label
    /// expression that does not say which (`:A|B`, `:!A`) is refused.
    fn checkPlainLabels(self: *Self, pattern: ast.Pattern, comptime clause_name: []const u8) bool {
        for (pattern.elements) |element| {
            if (element == .node and element.node.label_expr != null) {
                self.addError(.unsupported_pattern, element.node.location, clause_name ++ " gives a node the labels it names, as in (n:A:B) or (n:A&B); a label expression with '|', '!' or '%' does not say which to give");
                return false;
            }
        }
        return true;
    }

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
            // A variable-length relationship's variable is the list of the
            // relationships on the path, a value.
            const kind: VariableKind = if (edge.quantifier != null) .alias else .edge;
            self.registerVariable(name, kind, edge.location);
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
        } else if (self.shadowsEnclosing(name, location)) {
            return;
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

    /// Whether `name`, about to be bound inside CALL { }, belongs to a scope
    /// around it, which the query cannot do without importing it; reports
    /// it if so.
    fn shadowsEnclosing(self: *Self, name: []const u8, location: ast.SourceLocation) bool {
        if (self.variables.contains(name)) return false;
        for (self.enclosing.items) |scope| {
            if (scope.variables.contains(name)) {
                if (scope.subquery) |what| {
                    self.addErrorFmt(.duplicate_variable, location, "Variable '{s}' is already defined outside {s}; use another name", .{ name, what });
                } else {
                    self.addErrorFmt(
                        .duplicate_variable,
                        location,
                        "Variable '{s}' is already defined outside CALL {{ ... }}; import it with WITH {s}, or use another name",
                        .{ name, name },
                    );
                }
                return true;
            }
        }
        return false;
    }

    /// CALL { [WITH imports] query [UNION query ...] }.
    fn analyzeCallSubquery(self: *Self, sq: *const ast.CallSubquery) void {
        var outer = self.variables.clone() catch {
            self.addError(.internal_error, sq.location, "Failed to check CALL { ... }: out of memory");
            return;
        };
        defer outer.deinit();
        const listed = self.variable_list.items.len;
        self.enclosing.append(self.allocator, .{ .variables = &outer }) catch {
            self.addError(.internal_error, sq.location, "Failed to check CALL { ... }: out of memory");
            return;
        };

        // The columns of the first query, which the others must match, and
        // whether it returns any.
        var columns: ?[]const ast.ReturnItem = null;
        var returns = false;
        var kinds: [16]VariableKind = undefined;
        const branch_count = sq.unions.len + 1;
        for (0..branch_count) |b| {
            const clauses = if (b == 0) sq.clauses else sq.unions[b - 1].clauses;
            const ret = returnOf(clauses);
            if (b == 0) {
                returns = ret != null;
            } else if (returns != (ret != null)) {
                self.addError(.unsupported_pattern, sq.location, "Every query in a UNION inside CALL { ... } must end with RETURN, or none");
                break;
            }
            self.variables.clearRetainingCapacity();
            var rest = clauses;
            if (clauses.len > 0 and clauses[0] == .with and self.importsOnly(clauses[0].with, &outer)) {
                for (clauses[0].with.items) |item| {
                    const name = item.expression.variable.name;
                    self.variables.put(name, outer.get(name).?) catch {};
                }
                rest = clauses[1..];
            }
            self.analyzeClauses(rest);
            if (ret) |r| {
                for (r.items, 0..) |item, i| {
                    if (item.alias == null and item.expression.* != .variable) {
                        self.addErrorFmt(.unsupported_pattern, item.expression.getLocation(), "CALL {{ ... }} must name what it returns: add AS to `{s}`", .{item.text});
                    }
                    if (b == 0 and i < kinds.len) kinds[i] = self.inferWithItemKind(item);
                }
                if (b == 0) columns = r.items;
            }
        }

        _ = self.enclosing.pop();
        std.mem.swap(std.StringHashMap(VariableInfo), &self.variables, &outer);
        // A WITH inside resets the list.
        self.variable_list.shrinkRetainingCapacity(@min(listed, self.variable_list.items.len));

        const items = columns orelse return;
        for (items, 0..) |item, i| {
            const name = item.alias orelse if (item.expression.* == .variable) item.expression.variable.name else continue;
            if (self.variables.contains(name)) {
                self.addErrorFmt(.duplicate_variable, item.expression.getLocation(), "Variable '{s}' returned by CALL {{ ... }} is already defined outside it; rename it with AS", .{name});
                continue;
            }
            self.registerVariable(name, if (i < kinds.len) kinds[i] else .alias, item.expression.getLocation());
        }
    }

    /// The RETURN that ends `clauses`, before any ORDER BY, SKIP or LIMIT.
    fn returnOf(clauses: []const ast.Clause) ?*const ast.ReturnClause {
        var last = clauses.len;
        while (last > 0) : (last -= 1) {
            switch (clauses[last - 1]) {
                .order_by, .skip, .limit => {},
                .return_ => |r| return r,
                else => return null,
            }
        }
        return null;
    }

    /// A WITH that starts CALL { } imports variables: only plain variables
    /// from outside. Anything else there is an error.
    fn importsOnly(self: *Self, with: *const ast.WithClause, outer: *const std.StringHashMap(VariableInfo)) bool {
        var ok = with.where == null and !with.distinct;
        for (with.items) |item| {
            if (item.expression.* != .variable or (item.alias != null and !std.mem.eql(u8, item.alias.?, item.expression.variable.name))) {
                ok = false;
            } else if (!outer.contains(item.expression.variable.name)) {
                self.addErrorFmt(.unbound_variable, item.expression.getLocation(), "Variable '{s}' is not defined", .{item.expression.variable.name});
                return false;
            }
        }
        if (!ok) {
            self.addError(.unsupported_pattern, with.location, "The WITH that starts CALL { ... } imports variables and can only list them, as in WITH a, b; project or filter in a second WITH");
        }
        return ok;
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
            .index => |i| {
                self.analyzeExpression(i.object);
                self.analyzeExpression(i.index);
            },
            .slice => |sl| {
                self.analyzeExpression(sl.object);
                if (sl.from) |from| self.analyzeExpression(from);
                if (sl.to) |to| self.analyzeExpression(to);
            },
            .case_expr => |c| {
                if (c.subject) |subject| self.analyzeExpression(subject);
                for (c.branches) |branch| {
                    self.analyzeExpression(branch.when);
                    self.analyzeExpression(branch.then);
                }
                if (c.else_expr) |e| self.analyzeExpression(e);
            },
            .comprehension => |c| {
                self.analyzeExpression(c.list);
                const names = [_][]const u8{c.variable};
                const saved = self.enterLocalScope(&names, c.location) orelse return;
                defer self.leaveLocalScope(&names, saved);
                if (c.where) |w| self.analyzeExpression(w);
                if (c.map) |m| self.analyzeExpression(m);
            },
            .reduce => |r| {
                self.analyzeExpression(r.initial);
                self.analyzeExpression(r.list);
                if (std.mem.eql(u8, r.accumulator, r.variable)) {
                    self.addErrorFmt(.duplicate_variable, r.location, "reduce() needs two different names, but both are '{s}'", .{r.variable});
                    return;
                }
                const names = [_][]const u8{ r.accumulator, r.variable };
                const saved = self.enterLocalScope(&names, r.location) orelse return;
                defer self.leaveLocalScope(&names, saved);
                self.analyzeExpression(r.expression);
            },
            .subquery => |sq| self.analyzeSubquery(sq),
            .label_test => |t| self.analyzeExpression(t.subject),
            .map_projection => |m| {
                self.analyzeExpression(m.subject);
                for (m.entries) |entry| if (entry.value) |v| self.analyzeExpression(v);
            },
        }
    }

    /// FOREACH's variable and whatever its clauses create are visible only
    /// inside it.
    fn analyzeForeach(self: *Self, f: *const ast.ForeachClause) void {
        self.analyzeExpression(f.list);
        var outer = self.variables.clone() catch {
            self.addError(.internal_error, f.location, "Failed to check FOREACH: out of memory");
            return;
        };
        const listed = self.variable_list.items.len;
        self.variables.put(f.variable, .{ .name = f.variable, .kind = .alias, .location = f.location }) catch {};
        self.analyzeClauses(f.clauses);
        std.mem.swap(std.StringHashMap(VariableInfo), &self.variables, &outer);
        outer.deinit();
        self.variable_list.shrinkRetainingCapacity(listed);
    }

    /// A subquery's patterns are checked as a MATCH, in a scope of its own:
    /// the variables they introduce are gone afterwards.
    fn analyzeSubquery(self: *Self, sq: *const ast.PatternSubquery) void {
        if (sq.body) |body| return self.analyzeSubqueryBody(sq, body);
        for (sq.patterns) |pattern| {
            if (pattern.shortest != .none) {
                self.addError(.unsupported_pattern, sq.location, "shortestPath() and allShortestPaths() are only supported in MATCH");
                return;
            }
        }
        var outer = self.variables.clone() catch {
            self.addError(.internal_error, sq.location, "Failed to check subquery: out of memory");
            return;
        };
        const listed = self.variable_list.items.len;
        const clause = ast.MatchClause{ .patterns = sq.patterns, .location = sq.location };
        // An aggregate here has no group at all, which the planner says
        // more exactly than "WHERE cannot aggregate".
        const saved_refusal = self.aggregates_refused_in;
        defer self.aggregates_refused_in = saved_refusal;
        self.aggregates_refused_in = null;
        self.analyzeMatchClause(&clause);
        if (sq.where) |w| self.analyzeExpression(w);
        if (sq.map) |m| self.analyzeExpression(m);
        std.mem.swap(std.StringHashMap(VariableInfo), &self.variables, &outer);
        outer.deinit();
        self.variable_list.shrinkRetainingCapacity(listed);
    }

    /// `EXISTS { query }`, `COUNT { query }`, `COLLECT { query }`: the query
    /// sees every variable around it and binds its own, which are gone
    /// after it. It cannot write, and COLLECT's must return one column.
    fn analyzeSubqueryBody(self: *Self, sq: *const ast.PatternSubquery, body: *const ast.CallSubquery) void {
        const what = sq.name();
        const q = ast.Query{ .clauses = body.clauses, .unions = body.unions, .allocator = self.allocator };
        if (q.writesData()) {
            self.addErrorFmt(.unsupported_pattern, sq.location, "{s} cannot change the database; use CALL {{ ... }} to write for each row", .{what});
            return;
        }
        var outer = self.variables.clone() catch {
            self.addError(.internal_error, sq.location, "Failed to check subquery: out of memory");
            return;
        };
        defer outer.deinit();
        const listed = self.variable_list.items.len;
        self.enclosing.append(self.allocator, .{ .variables = &outer, .subquery = what }) catch {
            self.addError(.internal_error, sq.location, "Failed to check subquery: out of memory");
            return;
        };
        var returns = false;
        for (0..body.unions.len + 1) |b| {
            const clauses = if (b == 0) body.clauses else body.unions[b - 1].clauses;
            const ret = returnOf(clauses);
            if (b == 0) {
                returns = ret != null;
            } else if (returns != (ret != null)) {
                self.addErrorFmt(.unsupported_pattern, sq.location, "Every query in a UNION inside {s} must end with RETURN, or none", .{what});
                break;
            }
            if (sq.kind == .collect) {
                if (ret == null or ret.?.items.len != 1) {
                    self.addError(.unsupported_pattern, sq.location, "COLLECT { ... } must end with a RETURN of one column, the values it collects");
                    break;
                }
            }
            // Each query starts from the variables around the subquery.
            self.variables.clearRetainingCapacity();
            var it = outer.iterator();
            while (it.next()) |entry| self.variables.put(entry.key_ptr.*, entry.value_ptr.*) catch {};
            self.analyzeClauses(clauses);
        }
        _ = self.enclosing.pop();
        self.variables.clearRetainingCapacity();
        var it = outer.iterator();
        while (it.next()) |entry| self.variables.put(entry.key_ptr.*, entry.value_ptr.*) catch {};
        self.variable_list.shrinkRetainingCapacity(@min(listed, self.variable_list.items.len));
    }

    /// Variables of an expression's own (a list comprehension's, reduce's),
    /// visible only inside part of it. They hide a variable of the same name
    /// from the query, which comes back afterwards. Null when out of memory.
    fn enterLocalScope(self: *Self, names: []const []const u8, location: ast.SourceLocation) ?[2]?VariableInfo {
        var saved: [2]?VariableInfo = .{ null, null };
        for (names, 0..) |name, i| {
            saved[i] = self.variables.get(name);
            self.variables.put(name, .{ .name = name, .kind = .alias, .location = location }) catch {
                self.addErrorFmt(.internal_error, location, "Failed to register variable '{s}': out of memory", .{name});
                return null;
            };
        }
        return saved;
    }

    fn leaveLocalScope(self: *Self, names: []const []const u8, saved: [2]?VariableInfo) void {
        for (names, 0..) |name, i| {
            if (saved[i]) |info| {
                self.variables.put(name, info) catch {};
            } else {
                _ = self.variables.remove(name);
            }
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
        // A misspelt function used to be found only when a row first reached
        // it, so a query over no rows succeeded and the same query over data
        // failed. The name and the argument count are known now.
        if (expression.findFunction(func.name)) |spec| {
            const n = func.arguments.len;
            if (func.star or n < spec.min_args or (spec.max_args != expression.FunctionSpec.ANY_COUNT and n > spec.max_args)) {
                self.addErrorFmt(
                    .wrong_argument_count,
                    func.location,
                    "{s} takes {s}, but was given {d} argument{s}",
                    .{ spec.name, spec.signature, n, if (n == 1) "" else "s" },
                );
            }
            return;
        }
        if (aggregate_ops.isAggregateFunction(func.name)) {
            const n = func.arguments.len;
            const is_count = std.ascii.eqlIgnoreCase(func.name, "count");
            if (is_count and n > 1) {
                self.addErrorFmt(.wrong_argument_count, func.location, "{s} takes one argument or *, but was given {d} arguments", .{ func.name, n });
            } else if (!is_count and n != 1) {
                self.addErrorFmt(.wrong_argument_count, func.location, "{s} takes one argument, but was given {d} arguments", .{ func.name, n });
            } else if (self.aggregates_refused_in) |clause| {
                self.addErrorFmt(
                    .invalid_aggregate,
                    func.location,
                    "{s}() aggregates rows, which {s} cannot do; aggregate in a WITH first, as in WITH collect(n.v) AS vs",
                    .{ func.name, clause },
                );
            }
            return;
        }
        self.addErrorFmt(.unknown_function, func.location, "Unknown function '{s}'", .{func.name});
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
            .index => .any,
            .slice => .any,
            .case_expr => .any,
            .comprehension => |c| if (c.kind == .list) .list else .boolean,
            .reduce => .any,
            .subquery => |sq| if (sq.kind == .exists) .boolean else if (sq.kind == .count) .integer else .list,
            .map_projection => .map,
            .label_test => .boolean,
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
            for ([_]*const ast.Expression{ expr.left, expr.right }) |operand| {
                if (operand.* == .subquery and operand.subquery.kind == .exists) {
                    self.addError(.invalid_operator_types, operand.getLocation(), "A pattern, or EXISTS { ... }, is a test, true or false, not a value; COUNT { ... } counts its matches and [(a)-->(b) | b] lists them");
                    return;
                }
            }
            self.addErrorFmt(.invalid_operator_types, expr.location, "Operator not valid for these types: {s} and {s}", .{ @tagName(left_type), @tagName(right_type) });
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
    return t == .integer or t == .float or t == .string or t == .boolean;
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

test "a node variable may be matched again" {
    const allocator = std.testing.allocator;
    const sources = [_][]const u8{
        "MATCH (n), (n:Person) RETURN n",
        "MATCH (a)-->(b)-->(a) RETURN a, b",
        "MATCH (a:X) MATCH (a)-->(b) RETURN b",
        "MATCH (a) WITH a MATCH (a)-->(c) RETURN c",
    };
    for (sources) |source| {
        var parser = Parser.init(allocator, source);
        defer parser.deinit();
        const query = parser.parse().query orelse return error.TestUnexpectedResult;
        var analyzer = SemanticAnalyzer.init(allocator);
        defer analyzer.deinit();
        const result = analyzer.analyze(query);
        try std.testing.expect(result.success);
    }
}

test "duplicate variable error" {
    const allocator = std.testing.allocator;
    const source = "MATCH ()-[r]->() MATCH ()-[r]->() RETURN r";

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

test "MATCH variable-length relationship binds its variable to a list" {
    const allocator = std.testing.allocator;
    const source = "MATCH (a)-[r:REL*1..2]->(b) RETURN r";

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

test "CREATE variable-length relationship is rejected" {
    const allocator = std.testing.allocator;
    const source = "CREATE (a:Person)-[:REL*2]->(b:Person)";

    var parser = Parser.init(allocator, source);
    defer parser.deinit();
    const parse_result = parser.parse();

    if (parse_result.query) |query| {
        var analyzer = SemanticAnalyzer.init(allocator);
        defer analyzer.deinit();
        const result = analyzer.analyze(query);

        try std.testing.expect(!result.success);
        try std.testing.expect(result.errors.len > 0);
        try std.testing.expectEqual(ErrorCode.unsupported_pattern, result.errors[0].code);
    } else {
        try std.testing.expect(false);
    }
}

test "MERGE ON CREATE SET labels is rejected semantically" {
    const allocator = std.testing.allocator;
    const source = "MERGE (n:Person {name: \"Alice\"}) ON CREATE SET n:Admin";

    var parser = Parser.init(allocator, source);
    defer parser.deinit();
    const parse_result = parser.parse();

    if (parse_result.query) |query| {
        var analyzer = SemanticAnalyzer.init(allocator);
        defer analyzer.deinit();
        const result = analyzer.analyze(query);

        try std.testing.expect(!result.success);
        try std.testing.expect(result.errors.len > 0);
        try std.testing.expectEqual(ErrorCode.unsupported_pattern, result.errors[0].code);
    } else {
        try std.testing.expect(false);
    }
}

test "MERGE anonymous target with ON CREATE SET is rejected semantically" {
    const allocator = std.testing.allocator;
    const source = "MATCH (x) MERGE (:Person {name: \"Alice\"}) ON CREATE SET x.flag = true RETURN x";

    var parser = Parser.init(allocator, source);
    defer parser.deinit();
    const parse_result = parser.parse();

    if (parse_result.query) |query| {
        var analyzer = SemanticAnalyzer.init(allocator);
        defer analyzer.deinit();
        const result = analyzer.analyze(query);

        try std.testing.expect(!result.success);
        try std.testing.expect(result.errors.len > 0);
        try std.testing.expectEqual(ErrorCode.unsupported_pattern, result.errors[0].code);
    } else {
        try std.testing.expect(false);
    }
}

test "MATCH variable-length range with min greater than max is rejected semantically" {
    const allocator = std.testing.allocator;
    const source = "MATCH (a)-[:REL*3..1]->(b) RETURN b";

    var parser = Parser.init(allocator, source);
    defer parser.deinit();
    const parse_result = parser.parse();

    if (parse_result.query) |query| {
        var analyzer = SemanticAnalyzer.init(allocator);
        defer analyzer.deinit();
        const result = analyzer.analyze(query);

        try std.testing.expect(!result.success);
        try std.testing.expect(result.errors.len > 0);
        try std.testing.expectEqual(ErrorCode.unsupported_pattern, result.errors[0].code);
    } else {
        try std.testing.expect(false);
    }
}

test "relationship type alternation is accepted in MATCH only" {
    const allocator = std.testing.allocator;
    {
        var parser = Parser.init(allocator, "MATCH (a)-[:REL_A|REL_B]->(b) RETURN b");
        defer parser.deinit();
        const query = parser.parse().query orelse return error.TestUnexpectedResult;
        var analyzer = SemanticAnalyzer.init(allocator);
        defer analyzer.deinit();
        try std.testing.expect(analyzer.analyze(query).success);
    }

    const source = "MATCH (a), (b) MERGE (a)-[:REL_A|REL_B]->(b)";

    var parser = Parser.init(allocator, source);
    defer parser.deinit();
    const parse_result = parser.parse();

    if (parse_result.query) |query| {
        var analyzer = SemanticAnalyzer.init(allocator);
        defer analyzer.deinit();
        const result = analyzer.analyze(query);

        try std.testing.expect(!result.success);
        try std.testing.expect(result.errors.len > 0);
        try std.testing.expectEqual(ErrorCode.unsupported_pattern, result.errors[0].code);
    } else {
        try std.testing.expect(false);
    }
}

test "CREATE relationship without type is rejected semantically" {
    const allocator = std.testing.allocator;
    const source = "CREATE (:Person)-[]->(:Person)";

    var parser = Parser.init(allocator, source);
    defer parser.deinit();
    const parse_result = parser.parse();

    if (parse_result.query) |query| {
        var analyzer = SemanticAnalyzer.init(allocator);
        defer analyzer.deinit();
        const result = analyzer.analyze(query);

        try std.testing.expect(!result.success);
        try std.testing.expect(result.errors.len > 0);
        try std.testing.expectEqual(ErrorCode.unsupported_pattern, result.errors[0].code);
    } else {
        try std.testing.expect(false);
    }
}

test "MERGE undirected relationship pattern is rejected semantically" {
    const allocator = std.testing.allocator;
    const source = "MERGE (:Person)<-[:REL]->(:Person)";

    var parser = Parser.init(allocator, source);
    defer parser.deinit();
    const parse_result = parser.parse();

    if (parse_result.query) |query| {
        var analyzer = SemanticAnalyzer.init(allocator);
        defer analyzer.deinit();
        const result = analyzer.analyze(query);

        try std.testing.expect(!result.success);
        try std.testing.expect(result.errors.len > 0);
        try std.testing.expectEqual(ErrorCode.unsupported_pattern, result.errors[0].code);
    } else {
        try std.testing.expect(false);
    }
}

test "MERGE multi-hop chain pattern is rejected semantically" {
    const allocator = std.testing.allocator;
    const source = "MERGE (:Person)-[:REL]->(:Person)-[:REL]->(:Person)";

    var parser = Parser.init(allocator, source);
    defer parser.deinit();
    const parse_result = parser.parse();

    if (parse_result.query) |query| {
        var analyzer = SemanticAnalyzer.init(allocator);
        defer analyzer.deinit();
        const result = analyzer.analyze(query);

        try std.testing.expect(!result.success);
        try std.testing.expect(result.errors.len > 0);
        try std.testing.expectEqual(ErrorCode.unsupported_pattern, result.errors[0].code);
    } else {
        try std.testing.expect(false);
    }
}
