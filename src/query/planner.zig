//! Query planner for Cypher queries.
//!
//! Transforms parsed AST into executable operator trees.
//! Handles MATCH, WHERE, RETURN, ORDER BY, LIMIT, SKIP clauses.
//! Recognizes vector distance (<=>)  and FTS match (@@) operators
//! and creates specialized search operators for them.

const std = @import("std");
const Allocator = std.mem.Allocator;

const ast = @import("ast.zig");
const semantic = @import("semantic.zig");
const executor = @import("executor.zig");
const expression = @import("expression.zig");
const parser = @import("parser.zig");
const ast_util = @import("ast_util.zig");

const Operator = executor.Operator;
const OperatorError = executor.OperatorError;
const Row = executor.Row;
const ExecutionContext = executor.ExecutionContext;
const MAX_SLOTS = executor.MAX_SLOTS;

const scan_ops = @import("operators/scan.zig");
const filter_ops = @import("operators/filter.zig");
const project_ops = @import("operators/project.zig");
const expand_ops = @import("operators/expand.zig");
const var_expand_ops = @import("operators/var_expand.zig");
const limit_ops = @import("operators/limit.zig");
const vector_ops = @import("operators/vector.zig");
const fts_ops = @import("operators/fts.zig");
const range_ops = @import("operators/index_range_scan.zig");
const edge_type_scan_ops = @import("operators/edge_type_scan.zig");
const mutation_ops = @import("operators/mutation.zig");
const aggregate_ops = @import("operators/aggregate.zig");
const distinct_ops = @import("operators/distinct.zig");
const unwind_ops = @import("operators/unwind.zig");
const source_ops = @import("operators/source.zig");
const materialize_ops = @import("operators/materialize.zig");
const cross_product_ops = @import("operators/cross_product.zig");
const union_ops = @import("operators/union.zig");
const optional_ops = @import("operators/optional.zig");
const path_ops = @import("operators/path.zig");
const subquery_ops = @import("operators/subquery.zig");
const procedure_ops = @import("operators/procedure.zig");

const btree = @import("../storage/btree.zig");
const BTree = btree.BTree;

const label_index = @import("../graph/label_index.zig");
const LabelIndex = label_index.LabelIndex;

const edge_mod = @import("../graph/edge.zig");
const EdgeStore = edge_mod.EdgeStore;

const symbols = @import("../graph/symbols.zig");
const SymbolTable = symbols.SymbolTable;
const SymbolId = symbols.SymbolId;

const hnsw = @import("../vector/hnsw.zig");
const HnswIndex = hnsw.HnswIndex;

const fts_index_mod = @import("../fts/index.zig");
const FtsIndex = fts_index_mod.FtsIndex;

const database_mod = @import("../storage/database.zig");
const Database = database_mod.Database;

// ============================================================================
// Types
// ============================================================================

/// Planner errors
pub const PlannerError = error{
    /// Out of memory
    OutOfMemory,
    /// Invalid query structure
    InvalidQuery,
    /// Unknown label
    UnknownLabel,
    /// Unknown edge type
    UnknownEdgeType,
    /// Too many variables
    TooManyVariables,
    /// Missing storage reference
    MissingStorage,
    /// Unsupported operation
    Unsupported,
    /// Internal error (unexpected condition)
    InternalError,
    /// `@@` named a property with no full-text index declared for it.
    MissingFtsIndex,
    /// `@@` was used on a variable whose pattern carries no label, so there is
    /// nothing to resolve the property against.
    UnlabeledFtsMatch,
};

/// Storage context for planning (references to storage structures)
pub const StorageContext = struct {
    /// Node B+Tree
    node_tree: ?*BTree,
    /// Label index
    label_index: ?*LabelIndex,
    /// Edge store
    edge_store: ?*EdgeStore,
    /// Symbol table
    symbol_table: ?*SymbolTable,
    /// HNSW vector index (optional)
    hnsw_index: ?*HnswIndex = null,
    /// FTS index (optional)
    fts_index: ?*FtsIndex = null,
    /// Database for mutation operations (optional)
    database: ?*Database = null,
};

/// Variable binding during planning
pub const VarBinding = struct {
    name: []const u8,
    slot: u8,
    kind: semantic.VariableKind,
};

/// Edge binding with additional metadata for deletion
pub const EdgeBinding = struct {
    source_slot: u8,
    target_slot: u8,
    edge_type: []const u8,
};

// ============================================================================
// Query Planner
// ============================================================================

/// Query planner that transforms AST to operator trees
pub const QueryPlanner = struct {
    allocator: Allocator,
    storage: StorageContext,
    bindings: std.StringHashMap(VarBinding),
    /// The labels each variable was written with in its MATCH pattern.
    ///
    /// `@@` needs these to say which declared index it means: a property name on
    /// its own does not identify one, since two labels can each declare an index
    /// on `title`. The slices point into the query's own syntax tree, which
    /// outlives planning.
    pattern_labels: std.StringHashMap([]const []const u8),
    /// The types each edge variable was written with in its MATCH pattern.
    ///
    /// The edge equivalent of `pattern_labels`. `r.note @@ "x"` needs a type for
    /// the same reason `d.title @@ "x"` needs a label: two edge types can each
    /// declare an index on `note`.
    pattern_edge_types: std.StringHashMap([]const []const u8),
    /// A specific explanation for the most recent planning failure.
    ///
    /// A plan error otherwise reaches the user as "could not create execution
    /// plan", which tells someone who mistyped a property name nothing at all.
    /// Written into a fixed buffer rather than allocated, because it has to
    /// outlive the arena the plan was built in without owning anything.
    detail_buf: [256]u8 = undefined,
    detail_len: usize = 0,
    edge_bindings: std.StringHashMap(EdgeBinding),
    hidden_binding_names: std.ArrayList([]u8),
    /// One past the highest slot allocated so far.
    next_slot: u8,
    /// The slot each variable name was first bound to in this query.
    ///
    /// Expressions find a variable's slot by name when the query runs, from
    /// one name-to-slot map for the whole query. A name therefore has to keep
    /// its slot across WITH scopes: `MATCH (a)-->(b) WITH b` once moved `b`
    /// to slot 0, where the WITH, reading `b` by name, then found `a`.
    name_slots: std.StringHashMap(u8),
    /// Bindings a WITH dropped from scope that its own expressions read.
    /// Registered for execution, but invisible to later clauses.
    outer_bindings: std.StringHashMap(VarBinding),
    /// Slots in use in the current scope.
    used_slots: u16 = 0,
    /// Slots used at any point of the query.
    ever_used_slots: u16 = 0,
    /// A name that had to be bound to a second slot, which the plan refuses.
    slot_conflict: ?[]const u8 = null,
    /// Number of output columns from the RETURN clause (set during planning)
    output_columns: u8 = 0,
    /// Explicit output column names from RETURN aliases.
    output_column_names: [MAX_SLOTS]?[]const u8,
    /// Plans of the pattern subqueries found so far, made only for a query
    /// that has some (`has_subqueries`).
    subquery_plans: std.ArrayList(subquery_ops.SubqueryPlan) = .empty,
    has_subqueries: bool = false,
    /// Which scan the planner chose for the most recent node pattern.
    ///
    /// An index scan and a label scan return the same rows, so behaviour alone
    /// cannot tell you which one ran. This records the decision so that tests
    /// can assert an index is actually being used, rather than assuming it from
    /// a result set that would look identical either way.
    last_scan_kind: ScanKind = .none,
    /// The full-text search each slot's index seek runs, when the MATCH
    /// being planned seeks one index for it. The WHERE that follows does not
    /// search again for the same predicate. Reset by every MATCH. Only the
    /// slots in `fts_seek_mask` hold a search; the rest are undefined, so
    /// planning a query without one costs nothing here.
    fts_seeks: [MAX_SLOTS]fts_ops.Search = undefined,
    fts_seek_ops: [MAX_SLOTS]*fts_ops.FtsIndexSeek = undefined,
    fts_seek_mask: u16 = 0,
    /// How many `@@` row filters (FtsSearchWithInput) the plan holds, so
    /// tests can see a search planned once rather than twice.
    fts_filter_count: u32 = 0,
    /// How many index seeks a LIMIT bounds (see `boundSeek`), for tests.
    fts_bounded_seeks: u32 = 0,
    /// The query each slot's vector search uses; see
    /// ExecutionContext.vector_queries.
    vector_queries: executor.VectorQueries = .{},
    /// WHERE conjuncts an index range scan answers, which the WHERE's filter
    /// then leaves out.
    consumed_conjuncts: std.ArrayList(*const ast.Expression) = .empty,
    /// An ascending ORDER BY on a node's property, with a LIMIT, that the
    /// first scan of the query could serve by reading an index in order.
    order_hint: ?OrderHint = null,
    /// Set once a scan reads in that order: the sort key to cut off on.
    order_cutoff_key: ?*const ast.Expression = null,

    const OrderHint = struct {
        variable: []const u8,
        property: []const u8,
        sort_key: *const ast.Expression,
    };

    /// How the planner resolved a node pattern to rows.
    pub const ScanKind = enum {
        /// No node pattern has been planned yet.
        none,
        /// Every node carrying the label, filtered afterwards.
        label_scan,
        /// Straight to the matching nodes through a property index.
        property_index_scan,
        /// A range of an ordered property index (`<`, `>=`, `STARTS WITH`...).
        property_range_scan,
        /// Every node with the label, read through an ordered index in the
        /// order an ORDER BY asks for.
        property_order_scan,
        /// Straight to the matching entities through a full-text index.
        fts_index_seek,
        /// A pattern starting from a relationship type, as one walk of the
        /// edges rather than an expand from every node.
        edge_type_scan,
    };

    const Self = @This();

    /// Create a new query planner
    pub fn init(allocator: Allocator, storage: StorageContext) Self {
        return Self{
            .allocator = allocator,
            .storage = storage,
            .bindings = std.StringHashMap(VarBinding).init(allocator),
            .pattern_labels = std.StringHashMap([]const []const u8).init(allocator),
            .pattern_edge_types = std.StringHashMap([]const []const u8).init(allocator),
            .edge_bindings = std.StringHashMap(EdgeBinding).init(allocator),
            .hidden_binding_names = .empty,
            .next_slot = 0,
            .name_slots = std.StringHashMap(u8).init(allocator),
            .outer_bindings = std.StringHashMap(VarBinding).init(allocator),
            .output_columns = 0,
            .output_column_names = [_]?[]const u8{null} ** MAX_SLOTS,
            .last_scan_kind = .none,
        };
    }

    /// Free planner resources
    pub fn deinit(self: *Self) void {
        self.clearHiddenBindingNames();
        self.hidden_binding_names.deinit(self.allocator);
        self.bindings.deinit();
        self.pattern_labels.deinit();
        self.pattern_edge_types.deinit();
        self.edge_bindings.deinit();
        self.name_slots.deinit();
        self.outer_bindings.deinit();
        self.consumed_conjuncts.deinit(self.allocator);
    }

    /// Plan a complete query
    pub fn plan(self: *Self, query: *const ast.Query, analysis: *const semantic.AnalysisResult) PlannerError!Operator {
        _ = analysis; // Used for validation, already done

        // Reset bindings for this query
        self.name_slots.clearRetainingCapacity();
        self.outer_bindings.clearRetainingCapacity();
        self.used_slots = 0;
        self.ever_used_slots = 0;
        self.slot_conflict = null;
        self.clearHiddenBindingNames();
        self.bindings.clearRetainingCapacity();
        self.pattern_labels.clearRetainingCapacity();
        self.pattern_edge_types.clearRetainingCapacity();
        self.edge_bindings.clearRetainingCapacity();
        self.next_slot = 0;
        self.output_columns = 0;
        self.output_column_names = [_]?[]const u8{null} ** MAX_SLOTS;
        self.consumed_conjuncts.clearRetainingCapacity();
        self.order_hint = orderHintFor(query.clauses);
        self.order_cutoff_key = null;
        self.subquery_plans = .empty;
        self.has_subqueries = query.subqueries.len > 0;

        var current_op: ?Operator = if (query.unions.len == 0)
            try self.planClauses(query.clauses)
        else
            try self.planUnion(query);

        if (self.has_subqueries) current_op = try self.hostSubqueries(query, current_op orelse return PlannerError.InvalidQuery);

        if (self.slot_conflict) |name| {
            self.setPlanDetail("Variable '{s}' is bound again where its earlier slot is taken; use a different name", .{name});
            return PlannerError.InvalidQuery;
        }
        // Expressions a WITH evaluated still read the bindings it dropped.
        var outer = self.outer_bindings.iterator();
        while (outer.next()) |entry| {
            if (self.bindings.contains(entry.key_ptr.*)) continue;
            self.bindings.put(entry.key_ptr.*, entry.value_ptr.*) catch return PlannerError.OutOfMemory;
        }

        return current_op orelse PlannerError.InvalidQuery;
    }

    /// Plan the clauses of one single query, in order.
    fn planClauses(self: *Self, clauses: []const ast.Clause) PlannerError!?Operator {
        return self.planClausesOver(clauses, null);
    }

    /// Plan clauses over `input`, the rows they start from.
    fn planClausesOver(self: *Self, clauses: []const ast.Clause, input: ?Operator) PlannerError!?Operator {
        var current_op: ?Operator = input;

        // Process each clause. RETURN + trailing ORDER BY/SKIP/LIMIT is
        // planned as sort/window first, then projection, so ORDER BY can
        // reference non-returned variables (e.g. RETURN n.name ORDER BY n.age).
        var clause_idx: usize = 0;
        while (clause_idx < clauses.len) : (clause_idx += 1) {
            const clause = clauses[clause_idx];
            if (self.has_subqueries) try self.planClauseSubqueries(clause);
            switch (clause) {
                .match => |m| if (m.optional) {
                    const where: ?*const ast.WhereClause = if (clause_idx + 1 < clauses.len)
                        switch (clauses[clause_idx + 1]) {
                            .where => |w| w,
                            else => null,
                        }
                    else
                        null;
                    // The WHERE is part of the OPTIONAL MATCH.
                    if (where != null) clause_idx += 1;
                    current_op = try self.planOptionalMatch(m, where, current_op orelse try self.singleRow());
                } else {
                    const where_hint: ?*const ast.Expression = if (clause_idx + 1 < clauses.len)
                        switch (clauses[clause_idx + 1]) {
                            .where => |w| w.condition,
                            else => null,
                        }
                    else
                        null;
                    current_op = try self.planMatch(m, current_op, where_hint);
                },
                .where => |w| current_op = try self.planWhere(w, current_op, vectorLimitHint(clauses[clause_idx + 1 ..])),
                .return_ => |r| {
                    // Where ORDER BY, SKIP, and LIMIT belong depends on whether
                    // the projection aggregates.
                    //
                    // Without aggregation they go underneath, because sorting by
                    // `n.name` needs the `n` that projection is about to discard.
                    //
                    // With aggregation they have to go on top. `ORDER BY papers`
                    // where `papers` is `count(p)` cannot be evaluated against
                    // rows that have not been grouped yet — there is no count to
                    // read. Planning it underneath sorted the raw matches and
                    // then regrouped them, which threw the ordering away and
                    // returned rows in an order nobody asked for, with nothing
                    // to indicate anything had gone wrong.
                    const aggregates = returnAggregates(r);

                    var input_for_return: ?Operator = current_op orelse try self.singleRow();
                    var lookahead = clause_idx + 1;
                    const first_trailing = lookahead;
                    while (lookahead < clauses.len) : (lookahead += 1) {
                        switch (clauses[lookahead]) {
                            .order_by, .skip, .limit => {},
                            else => break,
                        }
                        if (aggregates) continue;
                        // DISTINCT sits on top of the projection, and SKIP
                        // and LIMIT count the rows it lets through. Planned
                        // underneath they cut the input first, so
                        // `RETURN DISTINCT x LIMIT 2` over 1, 1, 1, 2 returned
                        // one row. The sort stays underneath, where it can
                        // still see what the projection discards, but it
                        // keeps every row: a bound would be the same cut.
                        // Distinct passes rows on in the order they arrive.
                        switch (clauses[lookahead]) {
                            .order_by => |o| input_for_return = try self.planOrderBy(
                                o,
                                try self.orderedCutoff(input_for_return, clauses[lookahead + 1 ..]),
                                r,
                                if (r.distinct) &.{} else clauses[lookahead + 1 ..],
                            ),
                            .skip => |s| if (!r.distinct) {
                                input_for_return = try self.planSkip(s, input_for_return);
                            },
                            .limit => |l| if (!r.distinct) {
                                input_for_return = try self.planLimit(l, input_for_return);
                            },
                            else => unreachable,
                        }
                    }

                    current_op = try self.planReturn(r, input_for_return);

                    if (r.distinct and !aggregates) {
                        var i = first_trailing;
                        while (i < lookahead) : (i += 1) {
                            switch (clauses[i]) {
                                .order_by => {},
                                .skip => |sk| current_op = try self.planSkip(sk, current_op),
                                .limit => |l| current_op = try self.planLimit(l, current_op),
                                else => unreachable,
                            }
                        }
                    }

                    if (aggregates) {
                        var i = first_trailing;
                        while (i < lookahead) : (i += 1) {
                            switch (clauses[i]) {
                                .order_by => |o| current_op = try self.planOrderByOnOutput(o, current_op, r, clauses[i + 1 .. lookahead]),
                                .skip => |sk| current_op = try self.planSkip(sk, current_op),
                                .limit => |l| current_op = try self.planLimit(l, current_op),
                                else => unreachable,
                            }
                        }
                    }

                    clause_idx = lookahead - 1;
                },
                .order_by => |o| current_op = try self.planOrderBy(o, current_op, null, clauses[clause_idx + 1 ..]),
                .limit => |l| current_op = try self.planLimit(l, current_op),
                .skip => |s| current_op = try self.planSkip(s, current_op),
                .create => |c| current_op = try self.planCreate(c, current_op),
                .delete => |d| current_op = try self.planDelete(d, current_op),
                .set => |s| current_op = try self.planSet(s, current_op),
                .remove => |r| current_op = try self.planRemove(r, current_op),
                .with => |w| current_op = try self.planWith(w, current_op orelse try self.singleRow()),
                .merge => |m| current_op = try self.planMerge(m, current_op),
                .unwind => |u| current_op = try self.planUnwind(u, current_op),
                .foreach => |f| current_op = try self.planForeach(f, current_op orelse try self.singleRow()),
                .call => |c| {
                    // A CALL that is the whole query (with at most a WHERE on
                    // what it yields) returns its columns itself.
                    var standalone = clause_idx == 0;
                    for (clauses[clause_idx + 1 ..]) |rest| {
                        if (rest != .where) standalone = false;
                    }
                    current_op = try self.planCall(c, current_op orelse try self.singleRow(), standalone);
                },
            }
        }
        return current_op;
    }

    /// Plan `q1 UNION [ALL] q2 ...`: each single query on its own, then
    /// their rows one query after another, without duplicates for UNION.
    ///
    /// Each single query starts with no variables in scope. Variables are
    /// found by name when the query runs, through one map for the whole
    /// query, so a name keeps its slot across the single queries the way it
    /// does across WITH scopes (`name_slots`); the bindings of the queries
    /// already planned are kept as outer bindings so that they are
    /// registered too.
    fn planUnion(self: *Self, query: *const ast.Query) PlannerError!Operator {
        const inputs = self.allocator.alloc(Operator, query.unions.len + 1) catch return PlannerError.OutOfMemory;
        var planned: usize = 0;
        errdefer for (inputs[0..planned]) |input| input.deinit(self.allocator);

        inputs[0] = try self.planUnionPart(query.clauses, 1);
        planned = 1;
        const columns = self.output_columns;
        var names: [MAX_SLOTS]?[]const u8 = self.output_column_names;

        for (query.unions, 1..) |branch, i| {
            try self.startUnionPart();
            inputs[i] = try self.planUnionPart(branch.clauses, i + 1);
            planned += 1;
            if (self.output_columns != columns) {
                self.setPlanDetail("All queries in a UNION must return the same number of columns; query 1 returns {d}, query {d} returns {d}", .{ columns, i + 1, self.output_columns });
                return PlannerError.InvalidQuery;
            }
            for (0..columns) |c| {
                const first = names[c] orelse "";
                const this = self.output_column_names[c] orelse "";
                if (!std.mem.eql(u8, first, this)) {
                    self.setPlanDetail("All queries in a UNION must return the same column names; column {d} is `{s}` in query 1 and `{s}` in query {d}", .{ c + 1, first, this, i + 1 });
                    return PlannerError.InvalidQuery;
                }
            }
            names = self.output_column_names;
        }

        const union_op = union_ops.Union.init(self.allocator, inputs) catch return PlannerError.OutOfMemory;
        // From here the union owns the inputs.
        planned = 0;
        if (query.unions[0].all) return union_op.operator();

        const distinct = distinct_ops.Distinct.init(self.allocator, union_op.operator()) catch {
            union_op.operator().deinit(self.allocator);
            return PlannerError.OutOfMemory;
        };
        return distinct.operator();
    }

    /// Plan one single query of a UNION, which has to end in RETURN.
    fn planUnionPart(self: *Self, clauses: []const ast.Clause, number: usize) PlannerError!Operator {
        var last = clauses.len;
        while (last > 0) : (last -= 1) {
            switch (clauses[last - 1]) {
                .order_by, .skip, .limit => {},
                else => break,
            }
        }
        if (last == 0 or clauses[last - 1] != .return_) {
            self.setPlanDetail("Every query in a UNION must end with RETURN; query {d} does not", .{number});
            return PlannerError.InvalidQuery;
        }
        // Each query has its own ORDER BY and LIMIT, so each may read an
        // index in its own order; no query's order may reach another's scan.
        self.order_hint = orderHintFor(clauses);
        self.order_cutoff_key = null;
        return (try self.planClauses(clauses)) orelse PlannerError.InvalidQuery;
    }

    /// Clear the scope for the next single query of a UNION.
    fn startUnionPart(self: *Self) PlannerError!void {
        var it = self.bindings.iterator();
        while (it.next()) |entry| {
            if (self.outer_bindings.contains(entry.key_ptr.*)) continue;
            self.outer_bindings.put(entry.key_ptr.*, entry.value_ptr.*) catch return PlannerError.OutOfMemory;
        }
        self.bindings.clearRetainingCapacity();
        self.pattern_labels.clearRetainingCapacity();
        self.pattern_edge_types.clearRetainingCapacity();
        self.edge_bindings.clearRetainingCapacity();
        self.used_slots = 0;
        self.output_columns = 0;
        self.output_column_names = [_]?[]const u8{null} ** MAX_SLOTS;
    }

    /// Plan a MATCH clause
    fn planMatch(
        self: *Self,
        match: *const ast.MatchClause,
        input: ?Operator,
        where_hint: ?*const ast.Expression,
    ) PlannerError!Operator {
        var op = input;
        self.fts_seek_mask = 0;

        var edges: std.ArrayList(*const ast.EdgePattern) = .empty;
        for (match.patterns) |pattern| {
            for (pattern.elements) |element| {
                switch (element) {
                    .edge => |edge| edges.append(self.allocator, edge) catch return PlannerError.OutOfMemory,
                    .node => {},
                }
            }
        }
        var state = MatchEdges{
            .edges = edges.items,
            .slots = self.allocator.alloc(?u8, edges.items.len) catch return PlannerError.OutOfMemory,
        };
        @memset(state.slots, null);

        for (match.patterns) |pattern| {
            op = try self.planPattern(pattern, op, where_hint, &state);
        }

        return op orelse PlannerError.InvalidQuery;
    }

    /// Plan `OPTIONAL MATCH patterns [WHERE condition]` over `input`.
    ///
    /// The patterns and the WHERE are planned as for MATCH, but over an
    /// Argument leaf instead of the input, and an Apply runs that plan once
    /// for each input row: every row it produces is passed on, and an input
    /// row it produces nothing from is passed on once with the slots the plan
    /// binds set to null. The WHERE belongs to the OPTIONAL MATCH, so a row
    /// it rejects becomes nulls rather than disappearing.
    fn planOptionalMatch(
        self: *Self,
        match: *const ast.MatchClause,
        where: ?*const ast.WhereClause,
        input: Operator,
    ) PlannerError!Operator {
        const argument = optional_ops.Argument.init(self.allocator) catch return PlannerError.OutOfMemory;
        const slots_before = self.used_slots;

        var inner = self.planMatch(match, argument.operator(), if (where) |w| w.condition else null) catch |err| {
            argument.operator().deinit(self.allocator);
            return err;
        };
        if (where) |w| {
            if (self.has_subqueries) try self.planSubqueriesIn(w.condition);
            try self.refuseBareDistance(w.condition, "OPTIONAL MATCH");
            // A plain filter: the vector and full-text access paths that
            // planWhere can choose consume their input up front, which an
            // argument that yields one row per round does not allow.
            const filter = filter_ops.Filter.init(self.allocator, inner, w.condition) catch {
                inner.deinit(self.allocator);
                return PlannerError.OutOfMemory;
            };
            inner = filter.operator();
        }

        const new_slots = self.used_slots & ~slots_before;
        const apply = optional_ops.Apply.init(self.allocator, input, inner, argument, true, new_slots) catch {
            inner.deinit(self.allocator);
            return PlannerError.OutOfMemory;
        };
        return apply.operator();
    }

    /// Plan the pattern subqueries in the expressions a clause evaluates
    /// over the rows in scope before it. A WHERE after WITH or OPTIONAL
    /// MATCH sees the variables they bind, so its subqueries are planned
    /// there instead.
    fn planClauseSubqueries(self: *Self, clause: ast.Clause) PlannerError!void {
        switch (clause) {
            .where => |w| try self.planSubqueriesIn(w.condition),
            .return_ => |r| for (r.items) |item| try self.planSubqueriesIn(item.expression),
            .with => |w| for (w.items) |item| try self.planSubqueriesIn(item.expression),
            .order_by => |o| for (o.items) |item| try self.planSubqueriesIn(item.expression),
            .unwind => |u| try self.planSubqueriesIn(u.expression),
            .delete => |d| for (d.expressions) |e| try self.planSubqueriesIn(e),
            .set => |s| for (s.items) |item| switch (item) {
                .property => |p| try self.planSubqueriesIn(p.value),
                .replace_properties => |p| try self.planSubqueriesIn(p.map),
                .merge_properties => |p| try self.planSubqueriesIn(p.map),
                .labels => {},
            },
            // Elsewhere a subquery is left unplanned, and `hostSubqueries`
            // refuses the query.
            .foreach => |f| try self.planSubqueriesIn(f.list),
            .match, .create, .merge, .remove, .limit, .skip, .call => {},
        }
    }

    /// Plan every subquery in `expr` not planned yet, outermost first; one
    /// inside another is planned with the outer one, which binds what it
    /// sees.
    fn planSubqueriesIn(self: *Self, expr: *const ast.Expression) PlannerError!void {
        var found: std.ArrayList(*const ast.PatternSubquery) = .empty;
        ast_util.collectSubqueries(self.allocator, expr, &found) catch |err| switch (err) {
            error.OutOfMemory => return PlannerError.OutOfMemory,
            error.Refused => {
                self.setPlanDetail("A pattern predicate, EXISTS {{...}}, COUNT {{...}} or pattern comprehension inside a list comprehension, quantifier or reduce() is not supported", .{});
                return PlannerError.InvalidQuery;
            },
        };
        for (found.items) |sq| try self.planSubquery(sq);
    }

    /// Plan one subquery over an Argument leaf, as an OPTIONAL MATCH is
    /// planned, in a scope of its own: the variables it introduces are
    /// dropped from scope afterwards, and the slots they took are free again
    /// for the clauses that follow, since its matches are rows of its own.
    fn planSubquery(self: *Self, sq: *const ast.PatternSubquery) PlannerError!void {
        for (self.subquery_plans.items) |p| {
            if (p.node == sq) return;
        }
        // Each match is one value, not a group: an aggregate has nothing to
        // aggregate over there.
        for ([_]?*ast.Expression{ sq.where, sq.map }) |part| {
            const expr = part orelse continue;
            if (aggregate_ops.containsAggregate(expr)) {
                self.setPlanDetail("An aggregate cannot be used inside a pattern comprehension, EXISTS {{...}} or COUNT {{...}} (line {d}, column {d})", .{ sq.location.line, sq.location.column });
                return PlannerError.InvalidQuery;
            }
        }
        const slots_before = self.used_slots;
        var introduced: std.ArrayList([]const u8) = .empty;
        for (sq.patterns) |pattern| {
            if (pattern.path_variable) |name| {
                if (!self.bindings.contains(name)) introduced.append(self.allocator, name) catch return PlannerError.OutOfMemory;
            }
            for (pattern.elements) |element| {
                const name = switch (element) {
                    .node => |n| n.variable,
                    .edge => |e| e.variable,
                } orelse continue;
                if (!self.bindings.contains(name)) introduced.append(self.allocator, name) catch return PlannerError.OutOfMemory;
            }
        }

        const argument = optional_ops.Argument.init(self.allocator) catch return PlannerError.OutOfMemory;
        const clause = self.allocator.create(ast.MatchClause) catch return PlannerError.OutOfMemory;
        clause.* = .{ .patterns = sq.patterns, .location = sq.location };
        var op = self.planMatch(clause, argument.operator(), sq.where) catch |err| {
            argument.operator().deinit(self.allocator);
            return err;
        };
        if (sq.where) |condition| {
            try self.planSubqueriesIn(condition);
            try self.refuseBareDistance(condition, "a subquery or pattern comprehension");
            const filter = filter_ops.Filter.init(self.allocator, op, condition) catch return PlannerError.OutOfMemory;
            op = filter.operator();
        }
        if (sq.map) |map| try self.planSubqueriesIn(map);
        self.subquery_plans.append(self.allocator, .{ .node = sq, .plan = op, .argument = argument }) catch
            return PlannerError.OutOfMemory;

        for (introduced.items) |name| {
            if (self.bindings.fetchRemove(name)) |entry| {
                self.outer_bindings.put(entry.key, entry.value) catch return PlannerError.OutOfMemory;
            }
            _ = self.pattern_labels.remove(name);
            _ = self.pattern_edge_types.remove(name);
            _ = self.edge_bindings.remove(name);
        }
        self.used_slots = slots_before;
    }

    /// Put the subqueries' plans at the root, where execution can find them,
    /// after checking that every subquery was planned: one where none is
    /// planned (in a MATCH or CREATE pattern's properties, say) is refused.
    fn hostSubqueries(self: *Self, query: *const ast.Query, root: Operator) PlannerError!Operator {
        for (query.subqueries) |sq| {
            for (self.subquery_plans.items) |p| {
                if (p.node == sq) break;
            } else {
                self.setPlanDetail("A pattern predicate, EXISTS {{...}}, COUNT {{...}} or pattern comprehension is not supported here (line {d}, column {d})", .{ sq.location.line, sq.location.column });
                return PlannerError.InvalidQuery;
            }
        }
        const plans = self.subquery_plans.toOwnedSlice(self.allocator) catch return PlannerError.OutOfMemory;
        const host = subquery_ops.SubqueryHost.init(self.allocator, root, plans) catch return PlannerError.OutOfMemory;
        return host.operator();
    }

    /// Plan `FOREACH (x IN list | clauses)`: an UNWIND of the list and the
    /// clauses, over an Argument leaf, run to the end for each input row,
    /// which then passes on unchanged. What FOREACH binds goes out of scope
    /// after it, and the slots it took are free again.
    fn planForeach(self: *Self, f: *const ast.ForeachClause, input: Operator) PlannerError!Operator {
        const slots_before = self.used_slots;
        var bound_before = std.StringHashMap(void).init(self.allocator);
        defer bound_before.deinit();
        var names = self.bindings.keyIterator();
        while (names.next()) |name| bound_before.put(name.*, {}) catch return PlannerError.OutOfMemory;

        const argument = optional_ops.Argument.init(self.allocator) catch return PlannerError.OutOfMemory;
        const unwind = self.allocator.create(ast.UnwindClause) catch return PlannerError.OutOfMemory;
        unwind.* = .{ .expression = f.list, .variable = f.variable, .location = f.location };
        const over_list = self.planUnwind(unwind, argument.operator()) catch |err| {
            argument.operator().deinit(self.allocator);
            return err;
        };
        const inner = (try self.planClausesOver(f.clauses, over_list)) orelse return PlannerError.InvalidQuery;

        var introduced: std.ArrayList([]const u8) = .empty;
        defer introduced.deinit(self.allocator);
        var bound = self.bindings.keyIterator();
        while (bound.next()) |name| {
            if (!bound_before.contains(name.*)) introduced.append(self.allocator, name.*) catch return PlannerError.OutOfMemory;
        }
        for (introduced.items) |name| {
            if (self.bindings.fetchRemove(name)) |entry| {
                self.outer_bindings.put(entry.key, entry.value) catch return PlannerError.OutOfMemory;
            }
            _ = self.pattern_labels.remove(name);
            _ = self.pattern_edge_types.remove(name);
            _ = self.edge_bindings.remove(name);
        }
        self.used_slots = slots_before;

        // The writes run while the input is still being read: read it in
        // full first, as SET and DELETE do, to release its page latches.
        const materialized = try self.materializeInput(input);
        const foreach = optional_ops.Foreach.init(self.allocator, materialized, inner, argument) catch return PlannerError.OutOfMemory;
        return foreach.operator();
    }

    /// Plan `CALL procedure() [YIELD ...]`: each input row once per row of the
    /// procedure's answer, with the yielded columns bound as variables.
    fn planCall(self: *Self, call: *const ast.CallClause, input: Operator, standalone: bool) PlannerError!Operator {
        const procedure = procedure_ops.Procedure.find(call.procedure) orelse {
            self.setPlanDetail("Unknown procedure '{s}'; the procedures are db.labels(), db.relationshipTypes(), db.propertyKeys() and db.indexes()", .{call.procedure});
            return PlannerError.InvalidQuery;
        };
        if (call.arguments.len > 0) {
            self.setPlanDetail("{s}() takes no arguments", .{procedure.name()});
            return PlannerError.InvalidQuery;
        }
        if (!standalone and call.yield_all) {
            self.setPlanDetail("YIELD * is only allowed when CALL {s}() is the whole query", .{procedure.name()});
            return PlannerError.InvalidQuery;
        }
        if (!standalone and call.yields == null) {
            self.setPlanDetail("CALL {s}() inside a larger query needs YIELD to name the columns it adds", .{procedure.name()});
            return PlannerError.InvalidQuery;
        }
        const columns = procedure.columns();
        const slots = self.allocator.alloc(?u8, columns.len) catch return PlannerError.OutOfMemory;
        @memset(slots, null);
        var names: [MAX_SLOTS][]const u8 = undefined;
        var order: [MAX_SLOTS]u8 = undefined;
        var count: usize = 0;

        if (call.yields) |items| {
            for (items) |item| {
                const index = for (columns, 0..) |column, i| {
                    if (std.mem.eql(u8, column, item.column)) break i;
                } else {
                    self.setPlanDetail("{s}() has no column `{s}`; it yields {s}", .{ procedure.name(), item.column, columnList(procedure) });
                    return PlannerError.InvalidQuery;
                };
                if (slots[index] != null) {
                    self.setPlanDetail("Column `{s}` is yielded twice", .{item.column});
                    return PlannerError.InvalidQuery;
                }
                const name = item.alias orelse item.column;
                const slot = try self.allocateNamedSlot(name);
                try self.bindVariable(name, slot, .alias);
                slots[index] = slot;
                names[count] = name;
                order[count] = slot;
                count += 1;
            }
        } else {
            for (columns, 0..) |column, i| {
                const slot = try self.allocateNamedSlot(column);
                try self.bindVariable(column, slot, .alias);
                slots[i] = slot;
                names[count] = column;
                order[count] = slot;
                count += 1;
            }
        }

        if (standalone) {
            // The columns are the result: the yielded ones, in the order
            // asked for, which is the order their slots were taken in.
            for (order[0..count], 0..) |slot, i| {
                if (slot != i) return PlannerError.InvalidQuery;
                self.output_column_names[i] = names[i];
            }
            self.output_columns = @intCast(count);
        }

        const database = self.storage.database orelse return PlannerError.MissingStorage;
        const op = procedure_ops.ProcedureCall.init(self.allocator, input, procedure, slots, database) catch
            return PlannerError.OutOfMemory;
        return op.operator();
    }

    fn columnList(procedure: procedure_ops.Procedure) []const u8 {
        return switch (procedure) {
            .labels => "`label`",
            .relationship_types => "`relationshipType`",
            .property_keys => "`propertyKey`",
            .indexes => "`type`, `entityType`, `labelOrType` and `property`",
        };
    }

    /// The relationships of one MATCH clause, for relationship uniqueness.
    ///
    /// Cypher never binds one relationship twice within a MATCH, across all of
    /// its comma-separated patterns. Each single-hop expand therefore skips
    /// edges already bound by an earlier relationship of the clause. Only
    /// relationships whose type sets overlap can collide, so only those get a
    /// slot to hold their edge, which keeps the extra slots off ordinary
    /// multi-type paths.
    ///
    /// A variable-length relationship that a later one could collide with
    /// keeps its path's edge ids in a slot of their own, as a list, and
    /// later relationships, single-hop or variable-length, skip those too.
    /// Its own path never reuses an edge.
    const MatchEdges = struct {
        /// Every relationship pattern of the clause, in order.
        edges: []const *const ast.EdgePattern,
        /// The slot each planned relationship's edge, or variable-length
        /// path, went to.
        slots: []?u8,
        /// How many relationships have been planned.
        planned: usize = 0,

        fn typesOverlap(a: []const []const u8, b: []const []const u8) bool {
            if (a.len == 0 or b.len == 0) return true;
            for (a) |x| {
                for (b) |y| {
                    if (std.mem.eql(u8, x, y)) return true;
                }
            }
            return false;
        }

        /// Whether relationship `k` must keep its edge in a slot, because a
        /// later relationship of the clause could match the same edge.
        fn needsSlot(self: *const MatchEdges, k: usize) bool {
            const edge = self.edges[k];
            for (self.edges[k + 1 ..]) |later| {
                if (typesOverlap(edge.types, later.types)) return true;
            }
            return false;
        }

        /// The slots of earlier relationships that relationship `k` must not
        /// repeat.
        fn distinctFrom(self: *const MatchEdges, allocator: Allocator, k: usize) PlannerError![]const u8 {
            var slots: std.ArrayList(u8) = .empty;
            for (self.edges[0..k], self.slots[0..k]) |earlier, slot| {
                const s = slot orelse continue;
                if (!typesOverlap(earlier.types, self.edges[k].types)) continue;
                slots.append(allocator, s) catch return PlannerError.OutOfMemory;
            }
            return slots.items;
        }
    };

    /// Plan a single pattern
    fn planPattern(
        self: *Self,
        pattern: ast.Pattern,
        input: ?Operator,
        where_hint: ?*const ast.Expression,
        match_edges: *MatchEdges,
    ) PlannerError!Operator {
        var op = input;
        var prev_node_slot: ?u8 = null;
        // The pattern starts with a scan of every node and nothing on top of
        // it, which a relationship type can replace.
        var starts_with_all_nodes = false;
        // A named path is put together from the slots its pattern fills.
        var path_start: ?u8 = null;
        var path_steps: std.ArrayList(path_ops.Step) = .empty;

        for (pattern.elements, 0..) |element, element_index| {
            switch (element) {
                .node => |node_pattern| {
                    // A node variable bound earlier (in this clause or a
                    // previous one) is that same node: no scan, only checks.
                    const bound_slot: ?u8 = if (node_pattern.variable) |name| self.boundNodeSlot(name) else null;

                    // If prev_node_slot is set, this node follows an edge.
                    // Use the target slot from the Expand operator instead of allocating a new slot.
                    const slot = if (prev_node_slot != null and op != null)
                        prev_node_slot.? // Use target slot from preceding edge's Expand
                    else if (bound_slot) |bound|
                        bound
                    else
                        try self.allocateNamedSlot(node_pattern.variable);
                    var node_var_name: ?[]const u8 = null;

                    // Bind variable if present
                    if (node_pattern.variable) |name| {
                        node_var_name = name;
                        if (bound_slot == null) {
                            try self.bindVariable(name, slot, .node);
                        }
                        if (bound_slot == null or node_pattern.labels.len > 0) {
                            self.pattern_labels.put(name, node_pattern.labels) catch {
                                return PlannerError.OutOfMemory;
                            };
                        }
                    }

                    // Determine if we need to create a scan or filter
                    const is_target_of_edge = prev_node_slot != null and op != null;

                    if (is_target_of_edge or (bound_slot != null and op != null)) {
                        // The node comes from the input: the target of an
                        // Expand, or a node bound earlier. Labels the pattern
                        // names are checked with filters.
                        if (node_pattern.labels.len > 0) {
                            const symbol_table = self.storage.symbol_table orelse return PlannerError.MissingStorage;
                            const label_id = symbol_table.lookup(node_pattern.labels[0]) catch |err| switch (err) {
                                symbols.SymbolError.NotFound => symbols.NULL_SYMBOL,
                                else => return PlannerError.InternalError,
                            };

                            // Create a label filter for the target node
                            const label_filter = filter_ops.LabelFilter.init(self.allocator, op.?, slot, label_id) catch {
                                return PlannerError.OutOfMemory;
                            };
                            op = label_filter.operator();

                            // Chain filters for additional labels (AND semantics)
                            for (node_pattern.labels[1..]) |label_name| {
                                const extra_id = symbol_table.lookup(label_name) catch |err| switch (err) {
                                    symbols.SymbolError.NotFound => symbols.NULL_SYMBOL,
                                    else => return PlannerError.InternalError,
                                };
                                const extra_filter = filter_ops.LabelFilter.init(self.allocator, op.?, slot, extra_id) catch {
                                    return PlannerError.OutOfMemory;
                                };
                                op = extra_filter.operator();
                            }
                        }
                        // If no labels, no filter needed - just use the Expand output as-is
                    } else if (node_pattern.labels.len > 0) {
                        // Node with labels - create a label scan
                        _ = self.storage.label_index orelse return PlannerError.MissingStorage;
                        const symbol_table = self.storage.symbol_table orelse return PlannerError.MissingStorage;

                        const database = self.storage.database orelse return PlannerError.MissingStorage;
                        var scan_label_index: usize = 0;
                        var indexed_property: ?IndexedProperty = null;
                        if (node_pattern.properties) |properties| {
                            search: for (node_pattern.labels, 0..) |label_name, label_idx| {
                                for (properties) |property| {
                                    if (!isIndependentExpression(property.value)) continue;
                                    if (database.hasNodePropertyIndex(label_name, property.key) catch false) {
                                        scan_label_index = label_idx;
                                        indexed_property = .{
                                            .name = property.key,
                                            .value = property.value,
                                        };
                                        break :search;
                                    }
                                }
                            }
                        }
                        if (indexed_property == null) {
                            if (node_var_name) |variable_name| {
                                if (where_hint) |condition| {
                                    if (self.findWherePropertyIndex(
                                        variable_name,
                                        node_pattern.labels,
                                        condition,
                                    )) |candidate| {
                                        scan_label_index = candidate.label_index;
                                        indexed_property = .{
                                            .name = candidate.property_name,
                                            .value = candidate.value,
                                        };
                                    }
                                }
                            }
                        }

                        // A declared full-text index answers the question and
                        // names the label, so seeking it replaces the label scan
                        // rather than sitting on top of one.
                        var fts_candidates: [MAX_FTS_DISJUNCTS][]const fts_ops.Search = undefined;
                        var fts_candidate_count: usize = 0;
                        if (indexed_property == null) {
                            if (node_var_name) |variable_name| {
                                if (where_hint) |condition| {
                                    var seek_infos: [MAX_FTS_DISJUNCTS]FtsSearchInfo = undefined;
                                    var seek_count: usize = 0;
                                    self.collectWhereFtsSeeks(variable_name, condition, &seek_infos, &seek_count);

                                    // Every candidate has to cover the same label.
                                    //
                                    // The label the access path guarantees is the
                                    // one whose filter is skipped below. Which
                                    // candidate runs is decided when the query
                                    // runs, so candidates on different labels
                                    // would mean skipping a filter for a label the
                                    // chosen one does not guarantee, and returning
                                    // entities that lack it. Keeping them to one
                                    // label costs nothing in practice: two
                                    // properties of the same label is the shape
                                    // this exists for.
                                    var seek_label: ?[]const u8 = null;
                                    for (seek_infos[0..seek_count]) |info| {
                                        const resolved = self.resolveFtsIndex(info) catch continue;
                                        var search = resolved;
                                        search.param_name = info.param_name;
                                        search.literal_query = info.query_text;
                                        if (seek_label) |chosen_label| {
                                            if (!std.mem.eql(u8, chosen_label, search.scope)) continue;
                                        }
                                        // Only when the index covers a label this
                                        // pattern actually asks for.
                                        for (node_pattern.labels, 0..) |label_name, label_idx| {
                                            if (!std.mem.eql(u8, label_name, search.scope)) continue;
                                            const group = self.allocator.alloc(fts_ops.Search, 1) catch {
                                                return PlannerError.OutOfMemory;
                                            };
                                            group[0] = search;
                                            fts_candidates[fts_candidate_count] = group;
                                            fts_candidate_count += 1;
                                            if (seek_label == null) {
                                                seek_label = search.scope;
                                                scan_label_index = label_idx;
                                            }
                                            break;
                                        }
                                        if (fts_candidate_count == fts_candidates.len) break;
                                    }
                                }
                            }
                        }

                        // An ordered index answers ranges, and reads in order.
                        var range_choice: ?RangeChoice = null;
                        if (indexed_property == null and fts_candidate_count == 0) {
                            if (node_var_name) |variable_name| {
                                range_choice = try self.chooseRangeScan(variable_name, node_pattern.labels, where_hint, op == null);
                                if (range_choice) |choice| scan_label_index = choice.label_index;
                            }
                        }

                        const scan_label_name = node_pattern.labels[scan_label_index];
                        const label_id = symbol_table.lookup(scan_label_name) catch |err| switch (err) {
                            symbols.SymbolError.NotFound => symbols.NULL_SYMBOL,
                            else => return PlannerError.InternalError,
                        };

                        var new_op: Operator = if (fts_candidate_count > 0) blk: {
                            const owned = self.allocator.alloc([]const fts_ops.Search, fts_candidate_count) catch {
                                return PlannerError.OutOfMemory;
                            };
                            @memcpy(owned, fts_candidates[0..fts_candidate_count]);
                            const seek = fts_ops.FtsIndexSeek.init(
                                self.allocator,
                                slot,
                                owned,
                                fts_ops.NO_RESULT_LIMIT,
                                database,
                            ) catch return PlannerError.OutOfMemory;
                            self.last_scan_kind = .fts_index_seek;
                            // With one candidate the seek always runs that
                            // search, so it answers the predicate itself.
                            if (fts_candidate_count == 1) {
                                self.fts_seeks[slot] = fts_candidates[0][0];
                                self.fts_seek_ops[slot] = seek;
                                self.fts_seek_mask |= @as(u16, 1) << @intCast(slot);
                            }
                            break :blk seek.operator();
                        } else if (indexed_property) |property| blk: {
                            const property_scan = scan_ops.PropertyIndexScan.init(
                                self.allocator,
                                slot,
                                scan_label_name,
                                property.name,
                                property.value,
                                database,
                            ) catch return PlannerError.OutOfMemory;
                            self.last_scan_kind = .property_index_scan;
                            break :blk property_scan.operator();
                        } else if (range_choice) |choice| blk: {
                            const range_scan = range_ops.IndexRangeScan.init(
                                self.allocator,
                                slot,
                                scan_label_name,
                                choice.property,
                                choice.conditions,
                                database,
                            ) catch return PlannerError.OutOfMemory;
                            self.last_scan_kind = if (choice.conditions.len == 0) .property_order_scan else .property_range_scan;
                            break :blk range_scan.operator();
                        } else blk: {
                            const label_scan = scan_ops.LabelScan.init(self.allocator, slot, label_id, database) catch {
                                return PlannerError.OutOfMemory;
                            };
                            self.last_scan_kind = .label_scan;
                            break :blk label_scan.operator();
                        };

                        // Chain filters for additional labels (AND semantics)
                        for (node_pattern.labels, 0..) |label_name, label_idx| {
                            if (label_idx == scan_label_index) continue;
                            const extra_id = symbol_table.lookup(label_name) catch |err| switch (err) {
                                symbols.SymbolError.NotFound => symbols.NULL_SYMBOL,
                                else => return PlannerError.InternalError,
                            };
                            const extra_filter = filter_ops.LabelFilter.init(self.allocator, new_op, slot, extra_id) catch {
                                return PlannerError.OutOfMemory;
                            };
                            new_op = extra_filter.operator();
                        }

                        // If there's an existing operator from a previous disconnected pattern, cross join
                        if (op) |existing_op| {
                            const cross = cross_product_ops.CrossProduct.init(self.allocator, existing_op, new_op) catch {
                                return PlannerError.OutOfMemory;
                            };
                            op = cross.operator();
                        } else {
                            op = new_op;
                        }
                    } else if (op == null) {
                        // All nodes scan (no labels, no previous operator)
                        _ = self.storage.node_tree orelse return PlannerError.MissingStorage;
                        const database = self.storage.database orelse return PlannerError.MissingStorage;
                        const all_scan = scan_ops.AllNodesScan.init(self.allocator, slot, database) catch {
                            return PlannerError.OutOfMemory;
                        };
                        op = all_scan.operator();
                        starts_with_all_nodes = element_index == 0 and node_pattern.properties == null;
                    } else {
                        // No labels but existing operator — cross join with all nodes scan
                        _ = self.storage.node_tree orelse return PlannerError.MissingStorage;
                        const database = self.storage.database orelse return PlannerError.MissingStorage;
                        const all_scan = scan_ops.AllNodesScan.init(self.allocator, slot, database) catch {
                            return PlannerError.OutOfMemory;
                        };
                        const cross = cross_product_ops.CrossProduct.init(self.allocator, op.?, all_scan.operator()) catch {
                            return PlannerError.OutOfMemory;
                        };
                        op = cross.operator();
                    }

                    // Add property filters for inline properties: {key: value} → Filter(n.key = value)
                    if (node_pattern.properties) |props| {
                        if (node_var_name == null) {
                            // Anonymous node with inline properties still
                            // needs a bound slot for synthesized filters.
                            node_var_name = try self.bindInternalVariable(slot, .node);
                        }
                        const var_name = node_var_name orelse return PlannerError.InvalidQuery;
                        for (props) |prop| {
                            // Synthesize: variable.property = value
                            const loc = node_pattern.location;

                            // Create variable reference expression
                            const var_expr = self.allocator.create(ast.Expression) catch return PlannerError.OutOfMemory;
                            var_expr.* = .{ .variable = .{ .name = var_name, .location = loc } };

                            // Create property access expression (var.key)
                            const prop_access = self.allocator.create(ast.PropertyAccess) catch return PlannerError.OutOfMemory;
                            prop_access.* = .{ .object = var_expr, .property = prop.key, .location = loc };

                            const prop_expr = self.allocator.create(ast.Expression) catch return PlannerError.OutOfMemory;
                            prop_expr.* = .{ .property_access = prop_access };

                            // Create binary expression (prop_access = value)
                            const bin_expr = self.allocator.create(ast.BinaryExpr) catch return PlannerError.OutOfMemory;
                            bin_expr.* = .{ .left = prop_expr, .operator = .eq, .right = prop.value, .location = loc };

                            const predicate = self.allocator.create(ast.Expression) catch return PlannerError.OutOfMemory;
                            predicate.* = .{ .binary = bin_expr };

                            const prop_filter = filter_ops.Filter.init(self.allocator, op.?, predicate) catch return PlannerError.OutOfMemory;
                            op = prop_filter.operator();
                        }
                    }

                    prev_node_slot = slot;
                    if (path_start == null) path_start = slot;
                },
                .edge => |edge_pattern| {
                    // Need previous node to expand from
                    const source_slot = prev_node_slot orelse return PlannerError.InvalidQuery;
                    // When the far end is a bound node, expand into it.
                    const next_node: ?*const ast.NodePattern = if (element_index + 1 < pattern.elements.len) switch (pattern.elements[element_index + 1]) {
                        .node => |n| n,
                        .edge => null,
                    } else null;
                    const next_variable: ?[]const u8 = if (next_node) |n| n.variable else null;
                    const bound_target: ?u8 = if (next_variable) |name| self.boundNodeSlot(name) else null;
                    const target_slot = bound_target orelse try self.allocateNamedSlot(next_variable);

                    // Optional edge variable
                    var edge_slot: ?u8 = null;
                    var edge_var_name: ?[]const u8 = null;
                    if (edge_pattern.variable) |name| {
                        edge_slot = try self.allocateNamedSlot(name);
                        try self.bindVariable(name, edge_slot.?, .edge);
                        edge_var_name = name;
                        // Every type the pattern named, because `-[r:A|B]->` has
                        // to try each in turn the way a multi-label node does.
                        self.pattern_edge_types.put(name, edge_pattern.types) catch {
                            return PlannerError.OutOfMemory;
                        };

                        // Store edge binding metadata for DELETE support
                        if (edge_pattern.types.len > 0) {
                            self.edge_bindings.put(name, .{
                                .source_slot = source_slot,
                                .target_slot = target_slot,
                                .edge_type = edge_pattern.types[0],
                            }) catch return PlannerError.OutOfMemory;
                        }
                    } else if (edge_pattern.properties) |props| {
                        // Anonymous relationship with inline properties still
                        // needs an edge slot for property filtering.
                        if (props.len > 0) {
                            edge_slot = try self.allocateSlot();
                            edge_var_name = try self.bindInternalVariable(edge_slot.?, .edge);
                        }
                    }

                    // A named path needs every relationship it goes through.
                    if (edge_slot == null and pattern.path_variable != null and edge_pattern.quantifier == null) {
                        edge_slot = try self.allocateSlot();
                        _ = try self.bindInternalVariable(edge_slot.?, .edge);
                    }

                    // Relationship uniqueness within the MATCH clause.
                    const edge_index = match_edges.planned;
                    match_edges.planned += 1;
                    var path_slot: ?u8 = null;
                    if (edge_pattern.quantifier != null) {
                        // A path's edges go in a slot of their own, never in
                        // the relationship's variable.
                        if (match_edges.needsSlot(edge_index)) {
                            path_slot = try self.allocateSlot();
                            _ = try self.bindInternalVariable(path_slot.?, .alias);
                        }
                        match_edges.slots[edge_index] = path_slot;
                    } else {
                        if (edge_slot == null and match_edges.needsSlot(edge_index)) {
                            edge_slot = try self.allocateSlot();
                            _ = try self.bindInternalVariable(edge_slot.?, .edge);
                        }
                        match_edges.slots[edge_index] = edge_slot;
                    }
                    const distinct_from = try match_edges.distinctFrom(self.allocator, edge_index);

                    // Every type the pattern accepts, as in `-[:A|B]->`.
                    const edge_types = try self.resolveEdgeTypes(edge_pattern.types);

                    // Map direction
                    const expand_dir: expand_ops.ExpandDirection = switch (edge_pattern.direction) {
                        .outgoing => .outgoing,
                        .incoming => .incoming,
                        .both => .both,
                    };

                    // Create expand operator (variable-length or regular)
                    _ = self.storage.edge_store orelse return PlannerError.MissingStorage;
                    const database = self.storage.database orelse return PlannerError.MissingStorage;

                    // `(a)-[r:T]->(b)` from an unlabelled, unbound `a`: walk
                    // the edges of those types once instead of expanding
                    // every node.
                    const type_scan = starts_with_all_nodes and element_index == 1 and
                        edge_pattern.quantifier == null and edge_types.len > 0 and
                        expand_dir != .both and bound_target == null and distinct_from.len == 0;
                    if (type_scan) {
                        op.?.deinit(self.allocator);
                        const scan = edge_type_scan_ops.EdgeTypeScan.init(
                            self.allocator,
                            source_slot,
                            target_slot,
                            edge_slot,
                            edge_types,
                            expand_dir == .incoming,
                            database,
                        ) catch return PlannerError.OutOfMemory;
                        op = scan.operator();
                        self.last_scan_kind = .edge_type_scan;
                        if (pattern.path_variable != null) {
                            path_steps.append(self.allocator, .{ .single = .{ .edge_slot = edge_slot.?, .node_slot = target_slot } }) catch
                                return PlannerError.OutOfMemory;
                        }
                    } else if (edge_pattern.quantifier) |quant| {
                        // Variable-length path: use VariableLengthExpand
                        const var_expand = var_expand_ops.VariableLengthExpand.init(
                            self.allocator,
                            op.?,
                            source_slot,
                            target_slot,
                            .{ .types = edge_types, .distinct_from = distinct_from, .target_bound = bound_target != null },
                            expand_dir,
                            database,
                            quant.min_hops,
                            quant.max_hops,
                        ) catch {
                            return PlannerError.OutOfMemory;
                        };
                        var_expand.path_slot = path_slot;
                        if (pattern.path_variable != null) {
                            const walk_slot = try self.allocateSlot();
                            _ = try self.bindInternalVariable(walk_slot, .alias);
                            var_expand.walk_slot = walk_slot;
                            path_steps.append(self.allocator, .{ .walk = walk_slot }) catch return PlannerError.OutOfMemory;
                        }
                        op = var_expand.operator();
                    } else {
                        // Regular single-hop expand
                        const expand = expand_ops.Expand.init(
                            self.allocator,
                            op.?,
                            source_slot,
                            target_slot,
                            edge_slot,
                            .{ .types = edge_types, .distinct_from = distinct_from, .target_bound = bound_target != null },
                            expand_dir,
                            database,
                        ) catch {
                            return PlannerError.OutOfMemory;
                        };
                        if (pattern.path_variable != null) {
                            path_steps.append(self.allocator, .{ .single = .{ .edge_slot = edge_slot.?, .node_slot = target_slot } }) catch
                                return PlannerError.OutOfMemory;
                        }
                        op = expand.operator();
                    }

                    // Add relationship property filters for inline maps:
                    // [:TYPE {k: v}] -> Filter(edge.k = v)
                    if (edge_pattern.properties) |props| {
                        const var_name = edge_var_name orelse return PlannerError.InvalidQuery;
                        const loc = edge_pattern.location;
                        for (props) |prop| {
                            const var_expr = self.allocator.create(ast.Expression) catch return PlannerError.OutOfMemory;
                            var_expr.* = .{ .variable = .{ .name = var_name, .location = loc } };

                            const prop_access = self.allocator.create(ast.PropertyAccess) catch return PlannerError.OutOfMemory;
                            prop_access.* = .{ .object = var_expr, .property = prop.key, .location = loc };

                            const prop_expr = self.allocator.create(ast.Expression) catch return PlannerError.OutOfMemory;
                            prop_expr.* = .{ .property_access = prop_access };

                            const bin_expr = self.allocator.create(ast.BinaryExpr) catch return PlannerError.OutOfMemory;
                            bin_expr.* = .{ .left = prop_expr, .operator = .eq, .right = prop.value, .location = loc };

                            const predicate = self.allocator.create(ast.Expression) catch return PlannerError.OutOfMemory;
                            predicate.* = .{ .binary = bin_expr };

                            const prop_filter = filter_ops.Filter.init(self.allocator, op.?, predicate) catch return PlannerError.OutOfMemory;
                            op = prop_filter.operator();
                        }
                    }

                    // Target becomes the new "previous node" for next edge
                    prev_node_slot = target_slot;
                },
            }
        }

        if (pattern.path_variable) |name| {
            const path_slot = try self.allocateNamedSlot(name);
            try self.bindVariable(name, path_slot, .alias);
            const steps = path_steps.toOwnedSlice(self.allocator) catch return PlannerError.OutOfMemory;
            const bind = path_ops.BindPath.init(self.allocator, op orelse return PlannerError.InvalidQuery, path_start.?, steps, path_slot) catch
                return PlannerError.OutOfMemory;
            op = bind.operator();
        }

        return op orelse PlannerError.InvalidQuery;
    }

    /// Resolve a relationship pattern's type names to symbol ids.
    ///
    /// A type the database has never seen resolves to `NULL_SYMBOL`, which no
    /// edge carries, so it matches nothing rather than failing the plan.
    fn resolveEdgeTypes(self: *Self, names: []const []const u8) PlannerError![]const SymbolId {
        if (names.len == 0) return &.{};
        const symbol_table = self.storage.symbol_table orelse return PlannerError.MissingStorage;
        const ids = self.allocator.alloc(SymbolId, names.len) catch return PlannerError.OutOfMemory;
        for (names, ids) |name, *id| {
            id.* = symbol_table.lookup(name) catch |err| switch (err) {
                symbols.SymbolError.NotFound => symbols.NULL_SYMBOL,
                else => return PlannerError.InternalError,
            };
        }
        return ids;
    }

    fn isIndependentExpression(expr: *const ast.Expression) bool {
        return switch (expr.*) {
            .literal, .parameter => true,
            .list => |list| blk: {
                for (list.elements) |element| {
                    if (!isIndependentExpression(element)) break :blk false;
                }
                break :blk true;
            },
            .map => |map| blk: {
                for (map.entries) |entry| {
                    if (!isIndependentExpression(entry.value)) break :blk false;
                }
                break :blk true;
            },
            else => false,
        };
    }

    const IndexedProperty = struct {
        name: []const u8,
        value: *const ast.Expression,
    };

    const WherePropertyIndex = struct {
        label_index: usize,
        property_name: []const u8,
        value: *const ast.Expression,
    };

    /// A full-text predicate on `variable_name` that an index can answer as the
    /// access path, rather than as a filter over a label scan.
    ///
    /// Only conjunctive positions count. Under an OR the predicate does not
    /// constrain which entities the query is about — the other side may admit
    /// rows the index never names — so seeking would drop them.
    /// Every full-text predicate on `variable_name` that could be the access path.
    ///
    /// Only conjunctive positions count. Under an OR the predicate does not
    /// constrain which entities the query is about — the other side may admit rows
    /// the index never names — so seeking would drop them.
    ///
    /// Several are collected rather than the first because they are rarely equally
    /// cheap. `d.title @@ "the" AND d.body @@ "sourdough"` can start from a term
    /// most documents share or from one a handful do, and the operator picks
    /// between them when it runs, where the query text is known even if it came
    /// from a parameter.
    fn collectWhereFtsSeeks(
        self: *Self,
        variable_name: []const u8,
        expr: *const ast.Expression,
        out: *[MAX_FTS_DISJUNCTS]FtsSearchInfo,
        count: *usize,
    ) void {
        const binary = switch (expr.*) {
            .binary => |binary| binary,
            else => return,
        };

        if (binary.operator == .and_) {
            self.collectWhereFtsSeeks(variable_name, binary.left, out, count);
            self.collectWhereFtsSeeks(variable_name, binary.right, out, count);
            return;
        }
        if (binary.operator != .fts_match) return;
        if (count.* >= out.len) return;

        const info = self.extractFtsInfo(binary.*) orelse return;
        const named = info.variable_name orelse return;
        if (!std.mem.eql(u8, named, variable_name)) return;

        out[count.*] = info;
        count.* += 1;
    }

    fn findWherePropertyIndex(
        self: *Self,
        variable_name: []const u8,
        labels: []const []const u8,
        expr: *const ast.Expression,
    ) ?WherePropertyIndex {
        const binary = switch (expr.*) {
            .binary => |binary| binary,
            else => return null,
        };

        if (binary.operator == .and_) {
            return self.findWherePropertyIndex(variable_name, labels, binary.left) orelse
                self.findWherePropertyIndex(variable_name, labels, binary.right);
        }
        if (binary.operator != .eq) return null;

        const indexed_property = propertyEqualityForVariable(
            variable_name,
            binary.left,
            binary.right,
        ) orelse propertyEqualityForVariable(
            variable_name,
            binary.right,
            binary.left,
        ) orelse return null;

        const database = self.storage.database orelse return null;
        for (labels, 0..) |label_name, label_idx| {
            if (database.hasNodePropertyIndex(label_name, indexed_property.name) catch false) {
                return .{
                    .label_index = label_idx,
                    .property_name = indexed_property.name,
                    .value = indexed_property.value,
                };
            }
        }
        return null;
    }

    fn propertyEqualityForVariable(
        variable_name: []const u8,
        property_expr: *const ast.Expression,
        value_expr: *const ast.Expression,
    ) ?IndexedProperty {
        if (!isIndependentExpression(value_expr)) return null;

        const property_access = switch (property_expr.*) {
            .property_access => |property_access| property_access,
            else => return null,
        };
        const object_variable = switch (property_access.object.*) {
            .variable => |variable| variable,
            else => return null,
        };
        if (!std.mem.eql(u8, object_variable.name, variable_name)) return null;

        return .{
            .name = property_access.property,
            .value = value_expr,
        };
    }

    const RangeChoice = struct {
        label_index: usize,
        property: []const u8,
        /// Empty for a read of the whole label in the property's order.
        conditions: []const range_ops.Condition,
    };

    const RangeConjunct = struct {
        property: []const u8,
        condition: range_ops.Condition,
    };

    const MAX_RANGE_CONJUNCTS = 16;

    /// Whether to read the node pattern's label through an ordered index, and
    /// which conjuncts of the WHERE that answers.
    ///
    /// The rule, without statistics to go on: a range on an indexed property
    /// is always read from the index, which reads only matching entries and
    /// so is never much worse than the label scan and filter it replaces
    /// (measured at 200,000 nodes, a range matching every node costs about the
    /// same either way). Equality indexes win over ranges (planned before
    /// this), and ranges over the ORDER BY: a range on the ORDER BY's property
    /// is read in order; a range on another property is read as a range and
    /// sorted. With no usable range, an ORDER BY on an indexed property with a
    /// LIMIT reads the index in order and stops after the first k rows.
    fn chooseRangeScan(
        self: *Self,
        variable_name: []const u8,
        labels: []const []const u8,
        where_hint: ?*const ast.Expression,
        first_scan: bool,
    ) PlannerError!?RangeChoice {
        // Vector and full-text predicates plan their own access paths from
        // the WHERE as written, and emit their rows nearest or best first,
        // not in the index's order.
        const searches = if (where_hint) |condition| hasSearchConjunct(condition) else false;
        var hint: ?OrderHint = null;
        if (first_scan and !searches) {
            if (self.order_hint) |h| {
                if (std.mem.eql(u8, h.variable, variable_name)) hint = h;
            }
        }

        var conjuncts: [MAX_RANGE_CONJUNCTS]RangeConjunct = undefined;
        var count: usize = 0;
        if (where_hint) |condition| {
            if (!searches) collectRangeConjuncts(variable_name, condition, &conjuncts, &count);
        }

        var chosen_property: ?[]const u8 = null;
        var chosen_label: usize = 0;
        var ordered = false;
        if (hint) |h| {
            if (self.orderedIndexLabel(labels, h.property)) |label_idx| {
                if (hasConjunctOn(conjuncts[0..count], h.property)) {
                    chosen_property = h.property;
                    chosen_label = label_idx;
                    ordered = true;
                }
            }
        }
        if (chosen_property == null) {
            for (conjuncts[0..count]) |conjunct| {
                if (self.orderedIndexLabel(labels, conjunct.property)) |label_idx| {
                    chosen_property = conjunct.property;
                    chosen_label = label_idx;
                    break;
                }
            }
        }
        if (chosen_property == null) {
            if (hint) |h| {
                if (self.orderedIndexLabel(labels, h.property)) |label_idx| {
                    chosen_property = h.property;
                    chosen_label = label_idx;
                    ordered = true;
                }
            }
        }
        const property = chosen_property orelse return null;

        var conditions: std.ArrayList(range_ops.Condition) = .empty;
        for (conjuncts[0..count]) |conjunct| {
            if (!std.mem.eql(u8, conjunct.property, property)) continue;
            conditions.append(self.allocator, conjunct.condition) catch return PlannerError.OutOfMemory;
            self.consumed_conjuncts.append(self.allocator, conjunct.condition.predicate) catch return PlannerError.OutOfMemory;
        }
        if (ordered) {
            self.order_cutoff_key = hint.?.sort_key;
            self.order_hint = null;
        }
        return .{
            .label_index = chosen_label,
            .property = property,
            .conditions = conditions.toOwnedSlice(self.allocator) catch return PlannerError.OutOfMemory,
        };
    }

    /// The first of `labels` with an ordered index on `property`.
    fn orderedIndexLabel(self: *Self, labels: []const []const u8, property: []const u8) ?usize {
        const database = self.storage.database orelse return null;
        for (labels, 0..) |label_name, label_idx| {
            const definition = (database.nodePropertyIndex(label_name, property) catch null) orelse continue;
            if (definition.encoding == .ordered) return label_idx;
        }
        return null;
    }

    fn hasConjunctOn(conjuncts: []const RangeConjunct, property: []const u8) bool {
        for (conjuncts) |conjunct| {
            if (std.mem.eql(u8, conjunct.property, property)) return true;
        }
        return false;
    }

    /// Whether a conjunct of `expr` is a vector or full-text predicate.
    fn hasSearchConjunct(expr: *const ast.Expression) bool {
        const binary = switch (expr.*) {
            .binary => |binary| binary,
            else => return false,
        };
        switch (binary.operator) {
            .and_ => return hasSearchConjunct(binary.left) or hasSearchConjunct(binary.right),
            .vector_distance, .fts_match => return true,
            .lt, .lte => return binary.left.* == .binary and binary.left.binary.operator == .vector_distance,
            else => return false,
        }
    }

    /// The conjuncts of `expr` that compare a property of `variable_name`
    /// with a value independent of the row: `<`, `<=`, `>`, `>=` either way
    /// round, and `STARTS WITH`.
    fn collectRangeConjuncts(
        variable_name: []const u8,
        expr: *const ast.Expression,
        out: *[MAX_RANGE_CONJUNCTS]RangeConjunct,
        count: *usize,
    ) void {
        const binary = switch (expr.*) {
            .binary => |binary| binary,
            else => return,
        };
        if (binary.operator == .and_) {
            collectRangeConjuncts(variable_name, binary.left, out, count);
            collectRangeConjuncts(variable_name, binary.right, out, count);
            return;
        }
        if (count.* == out.len) return;
        const op: range_ops.RangeOp, const flipped: range_ops.RangeOp = switch (binary.operator) {
            .lt => .{ .lt, .gt },
            .lte => .{ .lte, .gte },
            .gt => .{ .gt, .lt },
            .gte => .{ .gte, .lte },
            .starts_with => .{ .starts_with, .starts_with },
            else => return,
        };
        if (propertyEqualityForVariable(variable_name, binary.left, binary.right)) |found| {
            out[count.*] = .{ .property = found.name, .condition = .{ .op = op, .value = found.value, .predicate = expr } };
            count.* += 1;
        } else if (op != .starts_with) {
            if (propertyEqualityForVariable(variable_name, binary.right, binary.left)) |found| {
                out[count.*] = .{ .property = found.name, .condition = .{ .op = flipped, .value = found.value, .predicate = expr } };
                count.* += 1;
            }
        }
    }

    /// `expr` without the conjuncts a scan below answers, or null when
    /// nothing is left.
    fn residualCondition(self: *Self, expr: *const ast.Expression) PlannerError!?*const ast.Expression {
        for (self.consumed_conjuncts.items) |consumed| {
            if (consumed == expr) return null;
        }
        const binary = switch (expr.*) {
            .binary => |binary| binary,
            else => return expr,
        };
        if (binary.operator != .and_) return expr;
        const left = try self.residualCondition(binary.left);
        const right = try self.residualCondition(binary.right);
        const l = left orelse return right;
        const r = right orelse return l;
        if (l == binary.left and r == binary.right) return expr;
        const narrowed = self.allocator.create(ast.BinaryExpr) catch return PlannerError.OutOfMemory;
        narrowed.* = .{ .left = @constCast(l), .operator = .and_, .right = @constCast(r), .location = binary.location };
        const result = self.allocator.create(ast.Expression) catch return PlannerError.OutOfMemory;
        result.* = .{ .binary = narrowed };
        return result;
    }

    /// The ORDER BY a query's first scan could serve in order: a single
    /// MATCH pattern, an optional WHERE, then a RETURN that neither
    /// aggregates nor deduplicates, sorted ascending first by a property of
    /// a variable, with a LIMIT.
    fn orderHintFor(clauses: []const ast.Clause) ?OrderHint {
        if (clauses.len < 3) return null;
        const match = switch (clauses[0]) {
            .match => |m| m,
            else => return null,
        };
        if (match.patterns.len != 1) return null;
        // OPTIONAL MATCH plans its pattern over an argument row, never as
        // the query's first scan, and pads a miss with nulls; keep its order
        // out of the scan's hands rather than rely on that.
        if (match.optional) return null;
        var i: usize = 1;
        if (std.meta.activeTag(clauses[i]) == .where) i += 1;
        if (i + 1 >= clauses.len) return null;
        const ret = switch (clauses[i]) {
            .return_ => |r| r,
            else => return null,
        };
        if (ret.distinct or returnAggregates(ret)) return null;
        const order = switch (clauses[i + 1]) {
            .order_by => |o| o,
            else => return null,
        };
        if (order.items.len == 0 or order.items[0].descending) return null;
        var limited = false;
        for (clauses[i + 2 ..]) |clause| {
            switch (clause) {
                .limit => limited = true,
                .skip => {},
                else => break,
            }
        }
        if (!limited) return null;
        const key = resolveOrderAlias(order.items[0].expression, ret);
        const access = switch (key.*) {
            .property_access => |access| access,
            else => return null,
        };
        const variable = switch (access.object.*) {
            .variable => |variable| variable,
            else => return null,
        };
        return .{ .variable = variable.name, .property = access.property, .sort_key = key };
    }

    /// The input, under a cutoff when a scan below reads in the sort's order.
    fn orderedCutoff(self: *Self, input: ?Operator, following: []const ast.Clause) PlannerError!?Operator {
        const key = self.order_cutoff_key orelse return input;
        self.order_cutoff_key = null;
        const input_op = input orelse return input;
        const cutoff = range_ops.OrderedCutoff.init(self.allocator, input_op, key, try self.sortBounds(following)) catch {
            return PlannerError.OutOfMemory;
        };
        return cutoff.operator();
    }

    /// Plan a WHERE clause
    /// Detects vector distance (<=>)  and FTS match (@@) operators and creates
    /// specialized search operators for them instead of generic filters.
    fn planWhere(
        self: *Self,
        where: *const ast.WhereClause,
        input: ?Operator,
        limit_hint: VectorLimitHint,
    ) PlannerError!Operator {
        const input_op = input orelse return PlannerError.InvalidQuery;
        // A seek answers only the WHERE of its own MATCH, planned next.
        defer self.fts_seek_mask = 0;

        // What an index range scan below already answers is left out.
        var condition: *const ast.Expression = where.condition;
        if (self.consumed_conjuncts.items.len > 0) {
            const residual = try self.residualCondition(where.condition);
            self.consumed_conjuncts.clearRetainingCapacity();
            condition = residual orelse return input_op;
        }

        // Check for vector search pattern: x.embedding <=> $query [< threshold]
        if (self.detectVectorSearch(condition)) |vector_info| {
            // An ORDER BY keeps the LIMIT bounding the search only when it
            // sorts nearest first by this search's own distance, the order
            // the search already returns its results in.
            var hint = limit_hint;
            if (hint.order) |order| {
                if (!self.orderFollowsSearch(order, hint.ret, vector_info)) hint = .{};
            }
            return self.planVectorSearch(input_op, vector_info, hint);
        }

        // Check for FTS pattern: x.text @@ $query, or a disjunction of them
        if (self.detectFtsSearch(condition)) |fts_info| {
            // The MATCH already seeks this very search for the variable, and
            // every row it gives matches: searching again filters nothing
            // out. It used to run the whole search a second time.
            if (self.seekAnswers(fts_info)) {
                self.boundSeek(input_op, fts_info, limit_hint);
                return input_op;
            }
            return self.planFtsSearch(input_op, &[_]FtsSearchInfo{fts_info});
        }
        {
            var disjuncts: [MAX_FTS_DISJUNCTS]FtsSearchInfo = undefined;
            var count: usize = 0;
            if (self.collectFtsDisjuncts(condition, &disjuncts, &count) and count > 1) {
                return self.planFtsSearch(input_op, disjuncts[0..count]);
            }
        }

        // An OR of searches beside other conditions runs as that search, so
        // it scores what it matches, and the rest filters its rows. As a
        // filter alone it matched the same rows but left score() null,
        // unless an index range scan had taken the other conditions.
        if (condition.* == .binary and condition.binary.operator == .and_) {
            var disjuncts: [MAX_FTS_DISJUNCTS]FtsSearchInfo = undefined;
            if (self.ftsDisjunctionConjunct(condition, &disjuncts)) |found| {
                const search = try self.planFtsSearch(input_op, disjuncts[0..found.count]);
                std.debug.assert(self.consumed_conjuncts.items.len == 0);
                self.consumed_conjuncts.append(self.allocator, found.conjunct) catch return PlannerError.OutOfMemory;
                const rest = try self.residualCondition(condition);
                self.consumed_conjuncts.clearRetainingCapacity();
                const filter = filter_ops.Filter.init(self.allocator, search, rest.?) catch {
                    return PlannerError.OutOfMemory;
                };
                return filter.operator();
            }
        }

        // Default: create a filter operator
        try self.refuseBareDistance(condition, "this MATCH");
        const filter = filter_ops.Filter.init(self.allocator, input_op, condition) catch {
            return PlannerError.OutOfMemory;
        };

        return filter.operator();
    }

    /// A WHERE that is a bare `x <=> q` is a search only where planWhere
    /// plans it as one: the whole WHERE of a MATCH, over a property and a
    /// vector. As a row filter it would be a distance read as true or false,
    /// passing every row but an exact match, in no order; it is refused.
    fn refuseBareDistance(self: *Self, condition: *const ast.Expression, comptime where_written: []const u8) PlannerError!void {
        if (condition.* != .binary or condition.binary.operator != .vector_distance) return;
        self.setPlanDetail("A WHERE of only `<=>` ranks the rows of a MATCH by distance, which " ++ where_written ++ " cannot do; give it a bound instead, as in `WHERE n.embedding <=> $q < 0.5`", .{});
        return PlannerError.InvalidQuery;
    }

    /// Whether the index seek planned for the predicate's variable runs the
    /// same search as the predicate: same index, same query text or
    /// parameter.
    fn seekAnswers(self: *Self, info: FtsSearchInfo) bool {
        const slot = info.variable_slot orelse return false;
        if (self.fts_seek_mask & (@as(u16, 1) << @intCast(slot)) == 0) return false;
        const seek = self.fts_seeks[slot];
        if (seek.kind != .node) return false;
        const property = info.property_name orelse return false;
        if (!std.mem.eql(u8, seek.property, property)) return false;
        if (!optionalEql(seek.param_name, info.param_name)) return false;
        if (!optionalEql(seek.literal_query, info.query_text)) return false;
        // The index the predicate means, resolved the way planFtsSearch
        // would; the seek's is the one its pattern's labels name.
        const resolved = self.resolveFtsIndex(info) catch return false;
        return resolved.kind == seek.kind and std.mem.eql(u8, resolved.scope, seek.scope);
    }

    /// Let the RETURN's LIMIT bound the seek that answers the WHERE, when
    /// its rows reach the LIMIT one for one (nothing between the seek and
    /// the WHERE) and in the seek's order: no ORDER BY, or `score(x)`
    /// descending for the seek's variable. The seek then keeps the best
    /// SKIP + LIMIT matches (an exact top k) instead of every match.
    fn boundSeek(self: *Self, input_op: Operator, info: FtsSearchInfo, hint: VectorLimitHint) void {
        const slot = info.variable_slot orelse return;
        const seek = self.fts_seek_ops[slot];
        // A named path over the seek passes its rows on one for one.
        const below = if (path_ops.BindPath.of(input_op)) |bind| bind.input else input_op;
        if (below.ptr != @as(*anyopaque, @ptrCast(seek))) return;
        const limit = hint.limit orelse return;
        if (hint.order) |order| {
            if (!self.orderFollowsScore(order, hint.ret, slot)) return;
        }
        seek.bound = .{ .limit = limit, .skip = hint.skip };
        self.fts_bounded_seeks += 1;
    }

    /// Whether `order` is one descending `score(x)` for the variable in
    /// `slot`, or the alias of a RETURN item that is.
    fn orderFollowsScore(self: *Self, order: *const ast.OrderByClause, ret: ?*const ast.ReturnClause, slot: u8) bool {
        if (order.items.len != 1 or !order.items[0].descending) return false;
        var expr = order.items[0].expression;
        if (expr.* == .variable) {
            const r = ret orelse return false;
            const name = expr.variable.name;
            expr = for (r.items) |item| {
                if (item.alias) |alias| {
                    if (std.mem.eql(u8, alias, name)) break item.expression;
                }
            } else return false;
        }
        if (expr.* != .function_call) return false;
        const call = expr.function_call;
        if (!std.ascii.eqlIgnoreCase(call.name, "score") or call.arguments.len != 1) return false;
        const arg = call.arguments[0];
        if (arg.* != .variable) return false;
        return self.getSlot(arg.variable.name) == slot;
    }

    fn optionalEql(a: ?[]const u8, b: ?[]const u8) bool {
        if (a == null or b == null) return a == null and b == null;
        return std.mem.eql(u8, a.?, b.?);
    }

    /// The LIMIT and SKIP that bound a vector predicate's answer.
    const VectorLimitHint = struct {
        limit: ?*const ast.Expression = null,
        skip: ?*const ast.Expression = null,
        /// The ORDER BY between the RETURN and the LIMIT, which the hint
        /// holds only if it sorts by the search's distance.
        order: ?*const ast.OrderByClause = null,
        ret: ?*const ast.ReturnClause = null,
    };

    /// The LIMIT (and SKIP) of the RETURN that follows a WHERE, when the rows
    /// the WHERE produces reach that LIMIT in order and one for one: a
    /// RETURN that neither aggregates nor deduplicates. An ORDER BY is
    /// passed along for planWhere to check against the search. Anything
    /// else between them could drop or reorder rows, so no hint.
    fn vectorLimitHint(rest: []const ast.Clause) VectorLimitHint {
        if (rest.len == 0) return .{};
        const ret = switch (rest[0]) {
            .return_ => |r| r,
            else => return .{},
        };
        if (ret.distinct or returnAggregates(ret)) return .{};
        var hint = VectorLimitHint{ .ret = ret };
        for (rest[1..]) |clause| {
            switch (clause) {
                .skip => |s| hint.skip = s.count,
                .limit => |l| hint.limit = l.count,
                .order_by => |o| hint.order = o,
                else => break,
            }
        }
        if (hint.limit == null) return .{};
        return hint;
    }

    /// Whether `order` sorts nearest first by the distance the search in
    /// `info` measures: one ascending item that is `x.prop <=> query` for the
    /// search's variable, property and query, or the alias of a RETURN item
    /// that is.
    fn orderFollowsSearch(
        self: *Self,
        order: *const ast.OrderByClause,
        ret: ?*const ast.ReturnClause,
        info: VectorSearchInfo,
    ) bool {
        if (order.items.len != 1 or order.items[0].descending) return false;
        var expr = order.items[0].expression;
        if (expr.* == .variable) {
            const r = ret orelse return false;
            const name = expr.variable.name;
            expr = for (r.items) |item| {
                if (item.alias) |alias| {
                    if (std.mem.eql(u8, alias, name)) break item.expression;
                }
            } else return false;
        }
        if (expr.* != .binary or expr.binary.operator != .vector_distance) return false;
        const same = self.extractVectorInfo(expr.binary.*) orelse return false;
        if (same.variable_slot != info.variable_slot) return false;
        if (!std.mem.eql(u8, same.property_name orelse return false, info.property_name orelse return false)) return false;
        if (info.param_name) |name| {
            return same.param_name != null and std.mem.eql(u8, same.param_name.?, name);
        }
        const want = info.query_vector orelse return false;
        const got = same.query_vector orelse return false;
        return std.mem.eql(f32, want, got);
    }

    /// Information extracted from a vector search pattern
    const VectorSearchInfo = struct {
        /// The variable slot being searched (e.g., slot for 'n' in n.embedding)
        variable_slot: ?u8,
        /// Parameter name for query vector (e.g., "query" from $query)
        param_name: ?[]const u8,
        /// Literal query vector (if provided directly)
        query_vector: ?[]const f32,
        /// Distance threshold (e.g., 0.5 from < 0.5, or $t)
        threshold: ?vector_ops.VectorSearchWithInput.Threshold,
        /// The property being searched
        property_name: ?[]const u8,
    };

    /// Detect vector search pattern in expression
    /// Patterns: `x.prop <=> $param < threshold` or `x.prop <=> $param`
    fn detectVectorSearch(self: *Self, expr: *const ast.Expression) ?VectorSearchInfo {
        // Pattern 1: (x.prop <=> $param) < threshold
        if (expr.* == .binary) {
            const binary = expr.binary;

            // Check for comparison with threshold: (vector_distance_expr) < threshold
            if (binary.operator == .lt or binary.operator == .lte) {
                if (binary.left.* == .binary) {
                    const inner = binary.left.binary;
                    if (inner.operator == .vector_distance) {
                        var info = self.extractVectorInfo(inner.*) orelse return null;
                        const inclusive = binary.operator == .lte;
                        // The threshold has to be a number or a parameter.
                        // Anything else is left to the filter, which evaluates
                        // the comparison exactly; ignoring it here returned
                        // rows regardless of the bound.
                        switch (binary.right.*) {
                            .literal => |lit| switch (lit.value) {
                                .float => |f| info.threshold = .{ .value = .{ .literal = @floatCast(f) }, .inclusive = inclusive },
                                .integer => |i| info.threshold = .{ .value = .{ .literal = @floatFromInt(i) }, .inclusive = inclusive },
                                else => return null,
                            },
                            .parameter => |p| info.threshold = .{ .value = .{ .parameter = p.name }, .inclusive = inclusive },
                            else => return null,
                        }
                        return info;
                    }
                }
            }

            // Pattern 2: x.prop <=> $param (no threshold)
            if (binary.operator == .vector_distance) {
                return self.extractVectorInfo(binary.*);
            }
        }

        return null;
    }

    /// Extract vector search info from a binary expression with vector_distance operator
    fn extractVectorInfo(self: *Self, binary: ast.BinaryExpr) ?VectorSearchInfo {
        var info = VectorSearchInfo{
            .variable_slot = null,
            .param_name = null,
            .query_vector = null,
            .threshold = null,
            .property_name = null,
        };

        // Left side should be property access: x.embedding
        if (binary.left.* == .property_access) {
            const prop_access = binary.left.property_access;
            info.property_name = prop_access.property;

            // Get the variable's slot
            if (prop_access.object.* == .variable) {
                const var_name = prop_access.object.variable.name;
                info.variable_slot = self.getSlot(var_name);
            }
        }

        // Right side should be parameter: $query
        if (binary.right.* == .parameter) {
            info.param_name = binary.right.parameter.name;
        } else if (binary.right.* == .list) {
            info.query_vector = parseVectorLiteral(self.allocator, binary.right.list) orelse null;
        }

        // Must have at least variable and parameter/literal vector
        if (info.variable_slot != null and (info.param_name != null or info.query_vector != null)) {
            return info;
        }

        return null;
    }

    /// Plan a vector search operator
    fn planVectorSearch(self: *Self, input: Operator, info: VectorSearchInfo, limit_hint: VectorLimitHint) PlannerError!Operator {
        _ = self.storage.hnsw_index orelse return PlannerError.MissingStorage;
        const database = self.storage.database orelse return PlannerError.MissingStorage;

        const output_slot = info.variable_slot orelse return PlannerError.InvalidQuery;
        self.vector_queries.set(output_slot, .{
            .property = info.property_name orelse return PlannerError.InvalidQuery,
            .param_name = info.param_name,
            .literal = info.query_vector,
        });

        const bound = vector_ops.VectorSearchWithInput.Bound{
            .threshold = info.threshold,
            .limit = limit_hint.limit,
            .skip = limit_hint.skip,
        };
        const vector_search = if (info.query_vector) |query_vector|
            vector_ops.VectorSearchWithInput.initWithLiteral(
                self.allocator,
                input,
                output_slot,
                query_vector,
                bound,
                database,
            ) catch return PlannerError.OutOfMemory
        else blk: {
            const param_name = info.param_name orelse return PlannerError.InvalidQuery;
            break :blk vector_ops.VectorSearchWithInput.init(
                self.allocator,
                input,
                output_slot,
                param_name,
                bound,
                database,
            ) catch return PlannerError.OutOfMemory;
        };

        return vector_search.operator();
    }

    fn parseVectorLiteral(allocator: Allocator, list: *const ast.ListExpr) ?[]const f32 {
        const values = allocator.alloc(f32, list.elements.len) catch return null;

        for (list.elements, 0..) |elem, i| {
            if (elem.* != .literal) {
                allocator.free(values);
                return null;
            }

            const lit = elem.literal;
            values[i] = switch (lit.value) {
                .float => |f| @floatCast(f),
                .integer => |n| @floatFromInt(n),
                else => {
                    allocator.free(values);
                    return null;
                },
            };
        }

        return values;
    }

    /// Information extracted from an FTS search pattern
    const FtsSearchInfo = struct {
        /// The variable slot being searched
        variable_slot: ?u8,
        /// Parameter name for query text
        param_name: ?[]const u8,
        /// Literal query text (if provided directly)
        query_text: ?[]const u8,
        /// The property being searched
        property_name: ?[]const u8,
        /// The variable the property hangs off, used to find its pattern labels
        variable_name: ?[]const u8,
    };

    /// Detect FTS search pattern in expression
    /// Pattern: `x.prop @@ $param` or `x.prop @@ "literal"`
    fn detectFtsSearch(self: *Self, expr: *const ast.Expression) ?FtsSearchInfo {
        if (expr.* == .binary) {
            const binary = expr.binary;

            if (binary.operator == .fts_match) {
                return self.extractFtsInfo(binary.*);
            }
        }

        return null;
    }

    /// How many `@@` predicates one WHERE clause may union.
    ///
    /// Well past anything written by hand, and bounded so the collected searches
    /// can live in a fixed array rather than an allocation the operator would
    /// have to outlive.
    const MAX_FTS_DISJUNCTS = 16;

    /// Collect a disjunction of `@@` predicates that can be planned as one scan.
    ///
    /// Every disjunct has to be an `@@` on the same variable. Anything else — a
    /// different variable, a comparison, a nested AND — means the union would not
    /// answer the same question as the original condition, so those keep the row
    /// filter.
    fn collectFtsDisjuncts(
        self: *Self,
        expr: *const ast.Expression,
        out: *[MAX_FTS_DISJUNCTS]FtsSearchInfo,
        count: *usize,
    ) bool {
        const binary = switch (expr.*) {
            .binary => |binary| binary,
            else => return false,
        };

        if (binary.operator == .or_) {
            return self.collectFtsDisjuncts(binary.left, out, count) and
                self.collectFtsDisjuncts(binary.right, out, count);
        }

        if (binary.operator != .fts_match) return false;
        if (count.* >= MAX_FTS_DISJUNCTS) return false;

        const info = self.extractFtsInfo(binary.*) orelse return false;
        if (count.* > 0 and info.variable_slot != out[0].variable_slot) return false;

        out[count.*] = info;
        count.* += 1;
        return true;
    }

    /// The first conjunct of `expr`, an AND, that is an OR of `@@`
    /// predicates `collectFtsDisjuncts` can plan as one search, with the
    /// searches in `out`.
    fn ftsDisjunctionConjunct(
        self: *Self,
        expr: *const ast.Expression,
        out: *[MAX_FTS_DISJUNCTS]FtsSearchInfo,
    ) ?struct { conjunct: *const ast.Expression, count: usize } {
        if (expr.* != .binary) return null;
        const binary = expr.binary;
        if (binary.operator == .and_) {
            return self.ftsDisjunctionConjunct(binary.left, out) orelse self.ftsDisjunctionConjunct(binary.right, out);
        }
        if (binary.operator != .or_) return null;
        var count: usize = 0;
        if (!self.collectFtsDisjuncts(expr, out, &count) or count < 2) return null;
        return .{ .conjunct = expr, .count = count };
    }

    /// Extract FTS search info from a binary expression with fts_match operator
    fn extractFtsInfo(self: *Self, binary: ast.BinaryExpr) ?FtsSearchInfo {
        var info = FtsSearchInfo{
            .variable_slot = null,
            .param_name = null,
            .query_text = null,
            .property_name = null,
            .variable_name = null,
        };

        // Left side should be property access: x.text
        if (binary.left.* == .property_access) {
            const prop_access = binary.left.property_access;
            info.property_name = prop_access.property;

            // Get the variable's slot
            if (prop_access.object.* == .variable) {
                const var_name = prop_access.object.variable.name;
                info.variable_slot = self.getSlot(var_name);
                info.variable_name = var_name;
            }
        }

        // Right side: parameter ($query) or literal ("search text")
        if (binary.right.* == .parameter) {
            info.param_name = binary.right.parameter.name;
        } else if (binary.right.* == .literal) {
            const lit = binary.right.literal;
            if (lit.value == .string) {
                info.query_text = lit.value.string;
            }
        }

        // Must have variable and either parameter or literal
        if (info.variable_slot != null and (info.param_name != null or info.query_text != null)) {
            return info;
        }

        return null;
    }

    /// Which declared index `x.prop @@ ...` means.
    ///
    /// A property name does not identify an index on its own, because two labels
    /// can each declare one on `title`. The label comes from the pattern the
    /// variable was written in, which is where property index planning already
    /// takes it from.
    /// Which declared index `x.prop @@ ...` means.
    ///
    /// A property name does not identify an index on its own, because two labels
    /// — or two edge types — can each declare one on `title`. The scope comes
    /// from the pattern the variable was written in, which is where property
    /// index planning already takes it from.
    fn resolveFtsIndex(self: *Self, info: FtsSearchInfo) PlannerError!fts_ops.Search {
        const database = self.storage.database orelse return PlannerError.MissingStorage;
        const property = info.property_name orelse return PlannerError.InvalidQuery;
        const variable = info.variable_name orelse return PlannerError.InvalidQuery;

        const binding = self.bindings.get(variable) orelse return PlannerError.InvalidQuery;
        const is_edge = binding.kind == .edge;

        const scopes = if (is_edge)
            self.pattern_edge_types.get(variable) orelse &[_][]const u8{}
        else
            self.pattern_labels.get(variable) orelse &[_][]const u8{};

        if (scopes.len == 0) {
            if (is_edge) {
                self.setPlanDetail(
                    "`{s}.{s} @@ ...` needs a relationship type to say which full-text index it means. Write it in the pattern, as in -[{s}:TYPE]->.",
                    .{ variable, property, variable },
                );
            } else {
                self.setPlanDetail(
                    "`{s}.{s} @@ ...` needs a label to say which full-text index it means. Write it in the pattern, as in ({s}:Label).",
                    .{ variable, property, variable },
                );
            }
            return PlannerError.UnlabeledFtsMatch;
        }

        for (scopes) |scope| {
            const declared = if (is_edge)
                database.hasEdgeFtsIndex(scope, property) catch false
            else
                database.hasNodeFtsIndex(scope, property) catch false;
            if (declared) {
                return .{
                    .kind = if (is_edge) .edge else .node,
                    .scope = scope,
                    .property = property,
                };
            }
        }

        self.setPlanDetail(
            "No full-text index is declared for {s}.{s}. Declare one before searching it.",
            .{ scopes[0], property },
        );
        return PlannerError.MissingFtsIndex;
    }

    /// Plan an FTS search operator over one or more `@@` predicates.
    ///
    /// Several predicates mean a disjunction, which is planned as one scan of
    /// each index rather than a filter that searches an index per row. A document
    /// found by more than one takes its best score, which is what the ranking of
    /// an OR should mean: either side matching is enough, and matching both is
    /// not evidence of matching either one better.
    fn planFtsSearch(self: *Self, input: Operator, infos: []const FtsSearchInfo) PlannerError!Operator {
        _ = self.storage.fts_index orelse return PlannerError.MissingStorage;
        const database = self.storage.database orelse return PlannerError.MissingStorage;
        if (infos.len == 0) return PlannerError.InvalidQuery;

        const output_slot = infos[0].variable_slot orelse return PlannerError.InvalidQuery;

        const searches = self.allocator.alloc(fts_ops.Search, infos.len) catch {
            return PlannerError.OutOfMemory;
        };
        for (infos, 0..) |info, i| {
            if (info.query_text == null and info.param_name == null) return PlannerError.InvalidQuery;
            var search = try self.resolveFtsIndex(info);
            search.param_name = info.param_name;
            search.literal_query = info.query_text;
            searches[i] = search;
        }

        // Every disjunct has to search the same kind of thing, because one
        // operator filters one slot and a slot holds a node or an edge, not
        // either. Mixing them would need two scans joined, which is a different
        // shape than this.
        for (searches[1..]) |search| {
            if (search.kind != searches[0].kind) {
                self.setPlanDetail("An OR of @@ predicates must search only nodes or only relationships", .{});
                return PlannerError.InvalidQuery;
            }
        }

        const fts_search = fts_ops.FtsSearchWithInput.init(
            self.allocator,
            input,
            output_slot,
            searches,
            fts_ops.NO_RESULT_LIMIT,
            database,
        ) catch {
            return PlannerError.OutOfMemory;
        };
        self.fts_filter_count += 1;

        return fts_search.operator();
    }

    /// Plan a RETURN clause
    fn planReturn(self: *Self, ret: *const ast.ReturnClause, input: ?Operator) PlannerError!Operator {
        const input_op = input orelse return PlannerError.InvalidQuery;
        try self.checkProjectionWidth("RETURN", ret.items.len);

        // Check if any RETURN items contain aggregate functions
        var has_aggregates = false;
        for (ret.items) |item| {
            if (aggregate_ops.containsAggregate(item.expression)) {
                has_aggregates = true;
                break;
            }
        }

        if (has_aggregates) {
            var result_op = try self.planAggregateReturn(ret, input_op);
            if (ret.distinct) {
                const distinct = distinct_ops.Distinct.init(self.allocator, result_op) catch {
                    return PlannerError.OutOfMemory;
                };
                result_op = distinct.operator();
            }
            return result_op;
        }

        // No aggregates - create simple projection
        var items = self.allocator.alloc(project_ops.ProjectItem, ret.items.len) catch {
            return PlannerError.OutOfMemory;
        };
        errdefer self.allocator.free(items);

        self.output_columns = @intCast(ret.items.len);

        for (ret.items, 0..) |item, i| {
            items[i] = .{
                .expr = item.expression,
                .output_slot = @intCast(i),
            };
            self.output_column_names[i] = columnName(self, item);
        }

        const project = project_ops.Project.init(self.allocator, input_op, items) catch {
            return PlannerError.OutOfMemory;
        };

        var result_op = project.operator();
        if (ret.distinct) {
            const distinct = distinct_ops.Distinct.init(self.allocator, result_op) catch {
                return PlannerError.OutOfMemory;
            };
            result_op = distinct.operator();
        }
        return result_op;
    }

    /// Refuse a RETURN or WITH with more items than a row has slots.
    ///
    /// A projection writes item i to slot i of its output row, and a row has
    /// `MAX_SLOTS` slots. Nothing checked the count, so a wider projection
    /// wrote past the end of the row: a panic in a debug build and undefined
    /// behaviour in a release one.
    fn checkProjectionWidth(self: *Self, clause_name: []const u8, item_count: usize) PlannerError!void {
        if (item_count <= MAX_SLOTS) return;
        self.setPlanDetail(
            "{s} has {d} items, but at most {d} are supported.",
            .{ clause_name, item_count, MAX_SLOTS },
        );
        return PlannerError.TooManyVariables;
    }

    /// Name a RETURN item's result column.
    ///
    /// An alias wins. Otherwise the column is named after the expression,
    /// falling back to the expression's text as written. A column must never
    /// be left unnamed here, because the caller would then name it after
    /// whichever variable happens to share its slot.
    fn columnName(self: *Self, item: ast.ReturnItem) ?[]const u8 {
        if (item.alias) |alias| return alias;
        // A quoted name keeps its backticks in the column, as written:
        // `n.my prop` would read as something else.
        if (std.mem.indexOfScalar(u8, item.text, '`') != null) return item.text;
        if (self.columnNameForExpression(item.expression)) |name| return name;
        if (item.text.len > 0) return item.text;
        return null;
    }

    /// Name an unaliased RETURN item after the expression it projects.
    ///
    /// Cypher names a result column after the text that produced it, so
    /// `RETURN a.n` yields a column called `a.n`. Returns null for shapes with
    /// no obvious rendering, leaving the caller to fall back to a positional
    /// name.
    fn columnNameForExpression(self: *Self, expr: *const ast.Expression) ?[]const u8 {
        return switch (expr.*) {
            .variable => |v| v.name,
            .parameter => |param| std.fmt.allocPrint(
                self.allocator,
                "${s}",
                .{param.name},
            ) catch null,
            .property_access => |access| blk: {
                const object = self.columnNameForExpression(access.object) orelse break :blk null;
                break :blk std.fmt.allocPrint(
                    self.allocator,
                    "{s}.{s}",
                    .{ object, access.property },
                ) catch null;
            },
            .function_call => |call| blk: {
                if (call.star) {
                    break :blk std.fmt.allocPrint(self.allocator, "{s}(*)", .{call.name}) catch null;
                }
                if (call.arguments.len != 1) break :blk null;
                const arg = self.columnNameForExpression(call.arguments[0]) orelse break :blk null;
                break :blk std.fmt.allocPrint(
                    self.allocator,
                    "{s}({s})",
                    .{ call.name, arg },
                ) catch null;
            },
            else => null,
        };
    }

    /// Plan a RETURN clause with aggregations
    fn planAggregateReturn(self: *Self, ret: *const ast.ReturnClause, input_op: Operator) PlannerError!Operator {
        return self.planAggregateReturnInto(ret, input_op, null);
    }

    /// Plan an aggregating projection whose item i goes to `slots[i]`, or to
    /// slot i when `slots` is null.
    fn planAggregateReturnInto(
        self: *Self,
        ret: *const ast.ReturnClause,
        input_op: Operator,
        slots: ?[]const u8,
    ) PlannerError!Operator {
        for (ret.items) |item| {
            if (self.needsAggregateProjection(item.expression)) {
                return self.planAggregateProjection(ret, input_op, slots);
            }
        }

        self.output_columns = @intCast(ret.items.len);

        // Separate items into grouping keys and aggregates
        var group_keys: std.ArrayList(aggregate_ops.GroupKey) = .empty;
        defer group_keys.deinit(self.allocator);

        var agg_items: std.ArrayList(aggregate_ops.AggregateItem) = .empty;
        defer agg_items.deinit(self.allocator);

        // Also build projection items for the final output
        var proj_items: std.ArrayList(project_ops.ProjectItem) = .empty;
        defer proj_items.deinit(self.allocator);

        for (ret.items, 0..) |item, i| {
            const slot: u8 = if (slots) |given| given[i] else @intCast(i);
            self.output_column_names[i] = columnName(self, item);

            if (self.isDirectAggregate(item.expression)) |agg_info| {
                // Direct aggregate function call: count(n), sum(n.val), etc.
                agg_items.append(self.allocator, .{
                    .func = agg_info.func,
                    .expr = agg_info.arg,
                    .output_slot = slot,
                    .distinct = agg_info.distinct,
                }) catch return PlannerError.OutOfMemory;

                // Projection just passes through the aggregate result
                proj_items.append(self.allocator, .{
                    .expr = item.expression,
                    .output_slot = slot,
                }) catch return PlannerError.OutOfMemory;
            } else if (aggregate_ops.containsAggregate(item.expression)) {
                // Complex expression containing aggregate - not yet supported
                // For now, treat as error
                return PlannerError.InvalidQuery;
            } else {
                // Non-aggregate expression - becomes a grouping key
                group_keys.append(self.allocator, .{
                    .expr = item.expression,
                    .output_slot = slot,
                }) catch return PlannerError.OutOfMemory;

                proj_items.append(self.allocator, .{
                    .expr = item.expression,
                    .output_slot = slot,
                }) catch return PlannerError.OutOfMemory;
            }
        }

        // Create owned slices for the Aggregate operator
        const owned_group_keys = self.allocator.dupe(aggregate_ops.GroupKey, group_keys.items) catch {
            return PlannerError.OutOfMemory;
        };
        errdefer self.allocator.free(owned_group_keys);

        const owned_agg_items = self.allocator.dupe(aggregate_ops.AggregateItem, agg_items.items) catch {
            return PlannerError.OutOfMemory;
        };
        errdefer self.allocator.free(owned_agg_items);

        // Create the aggregate operator
        const aggregate = aggregate_ops.Aggregate.init(
            self.allocator,
            input_op,
            owned_group_keys,
            owned_agg_items,
        ) catch {
            return PlannerError.OutOfMemory;
        };

        // The aggregate operator already outputs to the correct slots,
        // so we can return it directly without an additional projection
        return aggregate.operator();
    }

    /// Whether a projection item needs aggregates computed first and an
    /// expression evaluated over them afterwards: `count(n) + 1`, an
    /// aggregate inside CASE, or an aggregate nested in another (which is
    /// refused there).
    fn needsAggregateProjection(self: *Self, expr: *const ast.Expression) bool {
        if (!aggregate_ops.containsAggregate(expr)) return false;
        const direct = self.isDirectAggregate(expr) orelse return true;
        const arg = direct.arg orelse return false;
        return aggregate_ops.containsAggregate(arg);
    }

    /// One aggregate call of an aggregating projection and the slot of the
    /// aggregate's output row that holds its value.
    const AggregateCall = struct {
        expr: *const ast.Expression,
        slot: u8,
    };

    /// Replaces aggregate calls and grouping keys inside a projection item
    /// with the slots that hold their values after aggregation.
    const AggregateRewrite = struct {
        planner: *QueryPlanner,
        calls: *std.ArrayList(AggregateCall),
        keys: []const aggregate_ops.GroupKey,
        aggs: *std.ArrayList(aggregate_ops.AggregateItem),
        /// Slots of the aggregate's output row already taken.
        used: *u16,
        clause_name: []const u8,

        pub fn replace(self: *AggregateRewrite, e: *ast.Expression) ast_util.RewriteError!?*ast.Expression {
            for (self.keys) |key| {
                if (ast_util.expressionsEqual(key.expr, e)) return try self.planner.aggregateSlotVariable(key.output_slot);
            }
            const call = switch (e.*) {
                .function_call => |f| f,
                else => return null,
            };
            if (!aggregate_ops.isAggregateFunction(call.name)) return null;
            for (self.calls.items) |known| {
                if (ast_util.expressionsEqual(known.expr, e)) return try self.planner.aggregateSlotVariable(known.slot);
            }
            const info = self.planner.isDirectAggregate(e) orelse {
                self.planner.setPlanDetail("{s}() takes one argument", .{call.name});
                return error.Refused;
            };
            if (info.arg) |arg| {
                if (aggregate_ops.containsAggregate(arg)) {
                    self.planner.setPlanDetail("{s}() cannot take an aggregate as its argument; aggregates do not nest", .{call.name});
                    return error.Refused;
                }
            }
            const free = all_slots & ~self.used.*;
            if (free == 0) {
                self.planner.setPlanDetail(
                    "{s} needs more than {d} values for its grouping keys and aggregates; a row holds {d}",
                    .{ self.clause_name, MAX_SLOTS, MAX_SLOTS },
                );
                return error.Refused;
            }
            const slot: u8 = @ctz(free);
            self.used.* |= slotBit(slot);
            try self.aggs.append(self.planner.allocator, .{
                .func = info.func,
                .expr = info.arg,
                .output_slot = slot,
                .distinct = info.distinct,
            });
            try self.calls.append(self.planner.allocator, .{ .expr = e, .slot = slot });
            return try self.planner.aggregateSlotVariable(slot);
        }
    };

    /// The aggregate-slot variables the rewrite put in, which are not free.
    fn isAggregateSlotName(name: []const u8) bool {
        return std.mem.startsWith(u8, name, AGGREGATE_SLOT_PREFIX);
    }

    const AGGREGATE_SLOT_PREFIX = "  aggregate slot ";

    /// A reference to slot `slot` of an aggregate's output row, by a name
    /// no query can spell (it starts with spaces), registered for execution.
    fn aggregateSlotVariable(self: *Self, slot: u8) error{OutOfMemory}!*ast.Expression {
        const name = std.fmt.allocPrint(self.allocator, AGGREGATE_SLOT_PREFIX ++ "{d}", .{slot}) catch return error.OutOfMemory;
        self.outer_bindings.put(name, .{ .name = name, .slot = slot, .kind = .alias }) catch return error.OutOfMemory;
        const expr = self.allocator.create(ast.Expression) catch return error.OutOfMemory;
        expr.* = .{ .variable = .{ .name = name, .location = .{ .line = 0, .column = 0 } } };
        return expr;
    }

    /// Plan a projection whose items are expressions over aggregates, such
    /// as `count(n) + 1`, `sum(n.x) / count(n)` or an aggregate inside CASE.
    ///
    /// The aggregation is planned over the distinct aggregate calls found in
    /// the items, each written to a slot of the aggregate's output row, and
    /// grouped by the items that contain no aggregate. A projection then
    /// evaluates each item over that row, with every aggregate call and every
    /// grouping key replaced by the slot holding its value. Anything else an
    /// item uses outside an aggregate would have no value after grouping, so
    /// it is refused, as openCypher refuses it.
    fn planAggregateProjection(
        self: *Self,
        ret: *const ast.ReturnClause,
        input_op: Operator,
        slots: ?[]const u8,
    ) PlannerError!Operator {
        const items = ret.items;
        const clause_name: []const u8 = if (slots == null) "RETURN" else "WITH";
        self.output_columns = @intCast(items.len);

        var keys: std.ArrayList(aggregate_ops.GroupKey) = .empty;
        var aggs: std.ArrayList(aggregate_ops.AggregateItem) = .empty;
        var calls: std.ArrayList(AggregateCall) = .empty;
        var used: u16 = 0;

        // Grouping keys and plain aggregate items keep their output slots.
        for (items, 0..) |item, i| {
            const slot: u8 = if (slots) |given| given[i] else @intCast(i);
            self.output_column_names[i] = columnName(self, item);
            if (!aggregate_ops.containsAggregate(item.expression)) {
                keys.append(self.allocator, .{ .expr = item.expression, .output_slot = slot }) catch return PlannerError.OutOfMemory;
                used |= slotBit(slot);
            } else if (!self.needsAggregateProjection(item.expression)) {
                const info = self.isDirectAggregate(item.expression).?;
                aggs.append(self.allocator, .{
                    .func = info.func,
                    .expr = info.arg,
                    .output_slot = slot,
                    .distinct = info.distinct,
                }) catch return PlannerError.OutOfMemory;
                calls.append(self.allocator, .{ .expr = item.expression, .slot = slot }) catch return PlannerError.OutOfMemory;
                used |= slotBit(slot);
            }
        }

        var rewriter = AggregateRewrite{
            .planner = self,
            .calls = &calls,
            .keys = keys.items,
            .aggs = &aggs,
            .used = &used,
            .clause_name = clause_name,
        };
        const project_items = self.allocator.alloc(project_ops.ProjectItem, items.len) catch return PlannerError.OutOfMemory;
        for (items, project_items, 0..) |item, *project_item, i| {
            const slot: u8 = if (slots) |given| given[i] else @intCast(i);
            const expr = if (self.needsAggregateProjection(item.expression))
                ast_util.rewrite(self.allocator, item.expression, &rewriter) catch |err| return switch (err) {
                    error.OutOfMemory => PlannerError.OutOfMemory,
                    error.Refused => PlannerError.InvalidQuery,
                }
            else
                self.aggregateSlotVariable(slot) catch return PlannerError.OutOfMemory;

            if (ast_util.firstFreeVariable(expr, &isAggregateSlotName)) |name| {
                self.setPlanDetail(
                    "{s} item `{s}` uses `{s}` outside an aggregate, but `{s}` is not a grouping key; return it as an item of its own or aggregate it",
                    .{ clause_name, item.text, name, name },
                );
                return PlannerError.InvalidQuery;
            }
            project_item.* = .{ .expr = expr, .output_slot = slot };
        }

        const aggregate = aggregate_ops.Aggregate.init(
            self.allocator,
            input_op,
            keys.toOwnedSlice(self.allocator) catch return PlannerError.OutOfMemory,
            aggs.toOwnedSlice(self.allocator) catch return PlannerError.OutOfMemory,
        ) catch return PlannerError.OutOfMemory;
        const project = project_ops.Project.init(self.allocator, aggregate.operator(), project_items) catch {
            return PlannerError.OutOfMemory;
        };
        return project.operator();
    }

    /// Check if an expression is a direct aggregate function call (e.g., count(n), sum(n.val))
    /// Returns the aggregate function type, argument, and distinct flag if so
    fn isDirectAggregate(self: *Self, expr: *const ast.Expression) ?struct { func: aggregate_ops.AggregateFunc, arg: ?*const ast.Expression, distinct: bool } {
        _ = self;
        switch (expr.*) {
            .function_call => |f| {
                if (aggregate_ops.parseAggregateFunc(f.name)) |func| {
                    // COUNT(*) has no arguments
                    if (func == .count and f.arguments.len == 0) {
                        return .{ .func = .count_star, .arg = null, .distinct = f.distinct };
                    }
                    // Other aggregates need exactly one argument
                    if (f.arguments.len == 1) {
                        return .{ .func = func, .arg = f.arguments[0], .distinct = f.distinct };
                    }
                }
            },
            else => {},
        }
        return null;
    }

    /// Resolve `ORDER BY <name>` against the aliases a RETURN introduces.
    ///
    /// Sorting happens before projection, so an alias is not yet a column when
    /// the sort runs. Substituting the expression the alias stands for gives
    /// the same ordering without needing the projected row.
    fn resolveOrderAlias(
        expr: *ast.Expression,
        ret: ?*const ast.ReturnClause,
    ) *ast.Expression {
        const clause = ret orelse return expr;
        const name = switch (expr.*) {
            .variable => |v| v.name,
            else => return expr,
        };
        for (clause.items) |item| {
            const alias = item.alias orelse continue;
            if (std.mem.eql(u8, alias, name)) return item.expression;
        }
        return expr;
    }

    /// Does this RETURN clause aggregate?
    fn returnAggregates(ret: *const ast.ReturnClause) bool {
        for (ret.items) |item| {
            if (aggregate_ops.containsAggregate(item.expression)) return true;
        }
        return false;
    }

    /// Which output column of a RETURN clause does this ORDER BY item name?
    ///
    /// After aggregation the row is the projected columns and nothing else, so
    /// sorting has to name one of them. An alias matches by name, and a bare
    /// expression matches an item written the same way, which is what lets
    /// `ORDER BY count(p)` work without an alias.
    fn outputSlotFor(expr: *ast.Expression, ret: *const ast.ReturnClause) ?u8 {
        if (expr.* == .variable) {
            const name = expr.variable.name;
            for (ret.items, 0..) |item, i| {
                if (item.alias) |alias| {
                    if (std.mem.eql(u8, alias, name)) return @intCast(i);
                }
            }
        }

        for (ret.items, 0..) |item, i| {
            if (expressionsMatch(expr, item.expression)) return @intCast(i);
        }
        return null;
    }

    /// Compare two expressions closely enough to tell whether ORDER BY names a
    /// column the RETURN already produces.
    fn expressionsMatch(a: *const ast.Expression, b: *const ast.Expression) bool {
        if (std.meta.activeTag(a.*) != std.meta.activeTag(b.*)) return false;
        return switch (a.*) {
            .variable => |av| std.mem.eql(u8, av.name, b.variable.name),
            .property_access => |ap| blk: {
                const bp = b.property_access;
                if (!std.mem.eql(u8, ap.property, bp.property)) break :blk false;
                break :blk expressionsMatch(ap.object, bp.object);
            },
            .function_call => |af| blk: {
                const bf = b.function_call;
                if (!std.ascii.eqlIgnoreCase(af.name, bf.name)) break :blk false;
                if (af.arguments.len != bf.arguments.len) break :blk false;
                for (af.arguments, bf.arguments) |aa, ba| {
                    if (!expressionsMatch(aa, ba)) break :blk false;
                }
                break :blk true;
            },
            else => ast_util.expressionsEqual(a, b),
        };
    }

    /// Plan an ORDER BY that runs after projection, sorting by output column.
    fn planOrderByOnOutput(
        self: *Self,
        order: *const ast.OrderByClause,
        input: ?Operator,
        ret: *const ast.ReturnClause,
        following: []const ast.Clause,
    ) PlannerError!Operator {
        const input_op = input orelse return PlannerError.InvalidQuery;

        var sort_items = self.allocator.alloc(limit_ops.SortItem, order.items.len) catch {
            return PlannerError.OutOfMemory;
        };
        errdefer self.allocator.free(sort_items);

        for (order.items, 0..) |item, i| {
            // Naming something the projection does not produce is an error
            // rather than something to sort arbitrarily by. Silently returning
            // rows in an unhelpful order is how this went unnoticed before.
            const slot = outputSlotFor(item.expression, ret) orelse {
                self.allocator.free(sort_items);
                self.setPlanDetail("After an aggregating RETURN, ORDER BY can only name a returned column or its alias; ORDER BY item {d} is neither", .{i + 1});
                return PlannerError.InvalidQuery;
            };
            sort_items[i] = .{
                .expr = item.expression,
                .descending = item.descending,
                .nulls = item.nulls,
                .slot = slot,
            };
        }

        const sort = limit_ops.Sort.initBounded(self.allocator, input_op, sort_items, try self.sortBounds(following)) catch {
            return PlannerError.OutOfMemory;
        };

        return sort.operator();
    }

    /// Plan an ORDER BY clause
    fn planOrderBy(
        self: *Self,
        order: *const ast.OrderByClause,
        input: ?Operator,
        ret: ?*const ast.ReturnClause,
        following: []const ast.Clause,
    ) PlannerError!Operator {
        const input_op = input orelse return PlannerError.InvalidQuery;

        var sort_items = self.allocator.alloc(limit_ops.SortItem, order.items.len) catch {
            return PlannerError.OutOfMemory;
        };
        errdefer self.allocator.free(sort_items);

        for (order.items, 0..) |item, i| {
            sort_items[i] = .{
                .expr = resolveOrderAlias(item.expression, ret),
                .descending = item.descending,
                .nulls = item.nulls,
            };
        }

        const sort = limit_ops.Sort.initBounded(self.allocator, input_op, sort_items, try self.sortBounds(following)) catch {
            return PlannerError.OutOfMemory;
        };

        return sort.operator();
    }

    /// The SKIP and LIMIT clauses that directly follow an ORDER BY, which
    /// tell the sort how many rows it has to keep.
    fn sortBounds(self: *Self, following: []const ast.Clause) PlannerError![]const limit_ops.RowBound {
        var count: usize = 0;
        while (count < following.len) : (count += 1) {
            switch (following[count]) {
                .skip, .limit => {},
                else => break,
            }
        }
        const bounds = self.allocator.alloc(limit_ops.RowBound, count) catch return PlannerError.OutOfMemory;
        for (following[0..count], bounds) |clause, *bound| {
            bound.* = switch (clause) {
                .skip => |sk| .{ .skip = sk.count },
                .limit => |l| .{ .limit = l.count },
                else => unreachable,
            };
        }
        return bounds;
    }

    /// Plan a LIMIT clause
    fn planLimit(self: *Self, lim: *const ast.LimitClause, input: ?Operator) PlannerError!Operator {
        const input_op = input orelse return PlannerError.InvalidQuery;

        const limit = limit_ops.Limit.initExpr(self.allocator, input_op, lim.count) catch {
            return PlannerError.OutOfMemory;
        };

        return limit.operator();
    }

    /// Plan a SKIP clause
    fn planSkip(self: *Self, skip_clause: *const ast.SkipClause, input: ?Operator) PlannerError!Operator {
        const input_op = input orelse return PlannerError.InvalidQuery;

        const skip = limit_ops.Skip.initExpr(self.allocator, input_op, skip_clause.count) catch {
            return PlannerError.OutOfMemory;
        };

        return skip.operator();
    }

    /// Plan a CREATE clause
    fn planCreate(self: *Self, create: *const ast.CreateClause, input: ?Operator) PlannerError!Operator {
        const database = self.storage.database orelse return PlannerError.MissingStorage;
        var op: ?Operator = if (input) |inp| try self.materializeInput(inp) else null;

        // Pending edge info for deferred creation
        const PendingEdge = struct {
            source_slot: u8,
            edge_pattern: *const ast.EdgePattern,
        };

        // Process each pattern in CREATE
        for (create.patterns) |pattern| {
            var prev_node_slot: ?u8 = null;
            var pending_edge: ?PendingEdge = null;

            for (pattern.elements) |element| {
                switch (element) {
                    .node => |node_pattern| {
                        var slot: u8 = undefined;
                        var need_create = true;

                        // Check if this references an existing variable (from MATCH)
                        if (node_pattern.variable) |name| {
                            if (self.bindings.get(name)) |existing| {
                                // Variable already bound - use existing slot, don't create
                                slot = existing.slot;
                                need_create = false;
                            } else {
                                // New variable - allocate slot and bind
                                slot = try self.allocateNamedSlot(name);
                                try self.bindVariable(name, slot, .node);
                            }
                        } else {
                            // Anonymous node - always create
                            slot = try self.allocateSlot();
                        }

                        if (need_create) {
                            // Build properties list
                            var properties: std.ArrayList(mutation_ops.CreateNode.PropertyKV) = .empty;
                            if (node_pattern.properties) |props| {
                                for (props) |prop| {
                                    properties.append(self.allocator, .{
                                        .key = prop.key,
                                        .value_expr = prop.value,
                                    }) catch return PlannerError.OutOfMemory;
                                }
                            }

                            // Create the CreateNode operator
                            const create_node = mutation_ops.CreateNode.init(
                                self.allocator,
                                op,
                                node_pattern.labels,
                                properties.toOwnedSlice(self.allocator) catch return PlannerError.OutOfMemory,
                                slot,
                                database,
                            ) catch return PlannerError.OutOfMemory;

                            op = create_node.operator();
                        }

                        // If there's a pending edge, now we have the target - create it
                        if (pending_edge) |pe| {
                            if (pe.edge_pattern.types.len == 0) {
                                self.setPlanDetail("CREATE needs a relationship type, as in -[:TYPE]->", .{});
                                return PlannerError.InvalidQuery;
                            }

                            // Bind edge variable if present
                            var edge_slot: ?u8 = null;
                            if (pe.edge_pattern.variable) |edge_name| {
                                const edge_var_slot = try self.allocateNamedSlot(edge_name);
                                try self.bindVariable(edge_name, edge_var_slot, .edge);
                                edge_slot = edge_var_slot;
                            }

                            var edge_properties: []const mutation_ops.CreateNode.PropertyKV = &.{};
                            if (pe.edge_pattern.properties) |props| {
                                const kvs = self.allocator.alloc(mutation_ops.CreateNode.PropertyKV, props.len) catch {
                                    return PlannerError.OutOfMemory;
                                };
                                for (props, 0..) |prop, i| {
                                    kvs[i] = .{
                                        .key = prop.key,
                                        .value_expr = prop.value,
                                    };
                                }
                                edge_properties = kvs;
                            }

                            // Determine direction and create edge
                            // For incoming edges (<-[]-), the current node is the source
                            const source_slot = if (pe.edge_pattern.direction == .incoming) slot else pe.source_slot;
                            const target_slot = if (pe.edge_pattern.direction == .incoming) pe.source_slot else slot;

                            const create_edge = mutation_ops.CreateEdge.init(
                                self.allocator,
                                op.?,
                                source_slot,
                                target_slot,
                                pe.edge_pattern.types[0], // Use first type
                                edge_properties,
                                edge_slot,
                                database,
                            ) catch return PlannerError.OutOfMemory;

                            op = create_edge.operator();
                            pending_edge = null;
                        }

                        prev_node_slot = slot;
                    },
                    .edge => |edge_pattern| {
                        // Edge requires a source node from previous element
                        const source = prev_node_slot orelse return PlannerError.InvalidQuery;

                        // Defer edge creation until we have the target node
                        pending_edge = .{
                            .source_slot = source,
                            .edge_pattern = edge_pattern,
                        };
                    },
                }
            }

            // Check for dangling edge (edge without target node)
            if (pending_edge != null) {
                self.setPlanDetail("CREATE needs a node at each end of a relationship", .{});
                return PlannerError.InvalidQuery;
            }
        }

        return op orelse PlannerError.InvalidQuery;
    }

    /// Plan a DELETE clause
    fn planDelete(self: *Self, delete: *const ast.DeleteClause, input: ?Operator) PlannerError!Operator {
        const database = self.storage.database orelse return PlannerError.MissingStorage;
        const input_op = input orelse return PlannerError.InvalidQuery;
        var op = try self.materializeInput(input_op);

        // Process each expression to delete
        for (delete.expressions) |expr| {
            // Expression should be a variable reference
            if (expr.* != .variable) {
                self.setPlanDetail("DELETE takes variables bound to nodes or relationships", .{});
                return PlannerError.InvalidQuery;
            }

            const var_name = expr.variable.name;
            const binding = self.bindings.get(var_name) orelse return PlannerError.InvalidQuery;

            if (binding.kind == .node) {
                // Create DeleteNode operator
                const delete_node = mutation_ops.DeleteNode.init(
                    self.allocator,
                    op,
                    binding.slot,
                    delete.detach,
                    database,
                ) catch return PlannerError.OutOfMemory;

                op = delete_node.operator();
            } else if (binding.kind == .edge) {
                // Create DeleteEdge operator
                const delete_edge = mutation_ops.DeleteEdge.init(
                    self.allocator,
                    op,
                    binding.slot,
                    database,
                ) catch return PlannerError.OutOfMemory;

                op = delete_edge.operator();
            } else {
                self.setPlanDetail("DELETE needs a node or relationship; '{s}' is a value", .{var_name});
                return PlannerError.InvalidQuery;
            }
        }

        return op;
    }

    /// Plan a SET clause
    fn planSet(self: *Self, set_clause: *const ast.SetClause, input: ?Operator) PlannerError!Operator {
        const database = self.storage.database orelse return PlannerError.MissingStorage;
        var op = try self.materializeInput(input orelse return PlannerError.InvalidQuery);

        for (set_clause.items) |item| {
            switch (item) {
                .property => |p| {
                    // Get variable slot from target expression
                    const var_name = getVariableName(p.target) orelse return PlannerError.InvalidQuery;
                    const binding = self.bindings.get(var_name) orelse return PlannerError.InvalidQuery;

                    const set_prop = mutation_ops.SetProperty.init(
                        self.allocator,
                        op,
                        binding.slot,
                        p.property_name,
                        p.value,
                        database,
                    ) catch return PlannerError.OutOfMemory;

                    op = set_prop.operator();
                },
                .labels => |l| {
                    // Get variable slot
                    const var_name = getVariableName(l.target) orelse return PlannerError.InvalidQuery;
                    const binding = self.bindings.get(var_name) orelse return PlannerError.InvalidQuery;

                    if (binding.kind != .node) {
                        self.setPlanDetail("SET can add labels only to a node; '{s}' is not one", .{var_name});
                        return PlannerError.InvalidQuery;
                    }

                    const set_labels = mutation_ops.SetLabels.init(
                        self.allocator,
                        op,
                        binding.slot,
                        l.label_names,
                        database,
                    ) catch return PlannerError.OutOfMemory;

                    op = set_labels.operator();
                },
                .replace_properties => |r| {
                    // Get variable slot
                    const var_name = getVariableName(r.target) orelse return PlannerError.InvalidQuery;
                    const binding = self.bindings.get(var_name) orelse return PlannerError.InvalidQuery;

                    const set_replace = mutation_ops.SetPropertiesReplace.init(
                        self.allocator,
                        op,
                        binding.slot,
                        r.map,
                        database,
                    ) catch return PlannerError.OutOfMemory;

                    op = set_replace.operator();
                },
                .merge_properties => |m| {
                    // Get variable slot
                    const var_name = getVariableName(m.target) orelse return PlannerError.InvalidQuery;
                    const binding = self.bindings.get(var_name) orelse return PlannerError.InvalidQuery;

                    const set_merge = mutation_ops.SetPropertiesMerge.init(
                        self.allocator,
                        op,
                        binding.slot,
                        m.map,
                        database,
                    ) catch return PlannerError.OutOfMemory;

                    op = set_merge.operator();
                },
            }
        }

        return op;
    }

    /// Plan a REMOVE clause
    fn planRemove(self: *Self, remove_clause: *const ast.RemoveClause, input: ?Operator) PlannerError!Operator {
        const database = self.storage.database orelse return PlannerError.MissingStorage;
        var op = try self.materializeInput(input orelse return PlannerError.InvalidQuery);

        for (remove_clause.items) |item| {
            switch (item) {
                .property => |p| {
                    // Get variable slot from target expression
                    const var_name = getVariableName(p.target) orelse return PlannerError.InvalidQuery;
                    const binding = self.bindings.get(var_name) orelse return PlannerError.InvalidQuery;

                    const remove_prop = mutation_ops.RemoveProperty.init(
                        self.allocator,
                        op,
                        binding.slot,
                        p.property_name,
                        database,
                    ) catch return PlannerError.OutOfMemory;

                    op = remove_prop.operator();
                },
                .labels => |l| {
                    // Get variable slot
                    const var_name = getVariableName(l.target) orelse return PlannerError.InvalidQuery;
                    const binding = self.bindings.get(var_name) orelse return PlannerError.InvalidQuery;

                    if (binding.kind != .node) return PlannerError.InvalidQuery;

                    const remove_labels = mutation_ops.RemoveLabels.init(
                        self.allocator,
                        op,
                        binding.slot,
                        l.label_names,
                        database,
                    ) catch return PlannerError.OutOfMemory;

                    op = remove_labels.operator();
                },
            }
        }

        return op;
    }

    /// Plan a WITH clause (projection that feeds into next query part)
    fn planWith(self: *Self, with_clause: *const ast.WithClause, input: ?Operator) PlannerError!Operator {
        const input_op = input orelse return PlannerError.InvalidQuery;
        try self.checkProjectionWidth("WITH", with_clause.items.len);

        // The slot each item goes to. A name keeps the slot it already had
        // (see `name_slots`); other items take slots no earlier scope used
        // where there are any.
        const items = with_clause.items;
        const out_slots = self.allocator.alloc(u8, items.len) catch return PlannerError.OutOfMemory;
        const placed = self.allocator.alloc(bool, items.len) catch return PlannerError.OutOfMemory;
        @memset(placed, false);
        var taken: u16 = 0;
        for (items, 0..) |item, i| {
            const name = withItemName(item) orelse continue;
            const slot = self.name_slots.get(name) orelse continue;
            if (taken & slotBit(slot) != 0) continue;
            out_slots[i] = slot;
            placed[i] = true;
            taken |= slotBit(slot);
        }
        for (items, 0..) |_, i| {
            if (placed[i]) continue;
            var free = all_slots & ~taken & ~self.ever_used_slots;
            if (free == 0) free = all_slots & ~taken;
            if (free == 0) return PlannerError.TooManyVariables;
            out_slots[i] = @ctz(free);
            taken |= slotBit(out_slots[i]);
        }

        // Check for aggregates (same logic as RETURN)
        var has_aggregates = false;
        for (items) |item| {
            if (aggregate_ops.containsAggregate(item.expression)) {
                has_aggregates = true;
                break;
            }
        }

        var op: Operator = undefined;
        if (has_aggregates) {
            // Reuse the aggregate return planning by creating a temporary ReturnClause
            const ret = self.allocator.create(ast.ReturnClause) catch return PlannerError.OutOfMemory;
            ret.* = .{
                .distinct = with_clause.distinct,
                .items = items,
                .location = with_clause.location,
            };
            op = try self.planAggregateReturnInto(ret, input_op, out_slots);
        } else {
            // Simple projection
            var project_items = self.allocator.alloc(project_ops.ProjectItem, items.len) catch {
                return PlannerError.OutOfMemory;
            };
            for (items, 0..) |item, i| {
                project_items[i] = .{
                    .expr = item.expression,
                    .output_slot = out_slots[i],
                };
            }
            const project = project_ops.Project.init(self.allocator, input_op, project_items) catch {
                return PlannerError.OutOfMemory;
            };
            op = project.operator();
        }

        // Wrap with DISTINCT if specified
        if (with_clause.distinct) {
            const distinct = distinct_ops.Distinct.init(self.allocator, op) catch {
                return PlannerError.OutOfMemory;
            };
            op = distinct.operator();
        }

        // What each item carries on: a node or relationship variable stays
        // one, anything else is a value.
        const item_kinds = self.allocator.alloc(semantic.VariableKind, items.len) catch {
            return PlannerError.OutOfMemory;
        };
        for (items, item_kinds) |item, *kind| {
            kind.* = .alias;
            if (item.expression.* == .variable) {
                if (self.bindings.get(item.expression.variable.name)) |binding| kind.* = binding.kind;
            }
        }

        // Expressions before the WITH, its own included, still read the
        // bindings it does not carry on. They stay registered for execution,
        // out of later clauses' scope.
        var upstream = self.bindings.iterator();
        while (upstream.next()) |entry| {
            self.outer_bindings.put(entry.key_ptr.*, entry.value_ptr.*) catch return PlannerError.OutOfMemory;
        }

        // Reset bindings to WITH aliases (WITH introduces a new scope)
        self.bindings.clearRetainingCapacity();
        // What comes through a WITH is an alias rather than a node written with a
        // label, so the labels that were in scope before it no longer describe it.
        self.pattern_labels.clearRetainingCapacity();
        self.pattern_edge_types.clearRetainingCapacity();
        self.used_slots = taken;

        for (items, 0..) |item, i| {
            const name = withItemName(item) orelse continue;
            try self.bindVariable(name, out_slots[i], item_kinds[i]);
        }

        // Add WHERE filter if present
        if (with_clause.where) |condition| {
            if (self.has_subqueries) try self.planSubqueriesIn(condition);
            try self.refuseBareDistance(condition, "WITH");
            const filter = filter_ops.Filter.init(self.allocator, op, condition) catch {
                return PlannerError.OutOfMemory;
            };
            op = filter.operator();
        }

        return op;
    }

    /// The name a WITH item is known by afterwards: its alias, or the
    /// variable it passes on.
    fn withItemName(item: ast.ReturnItem) ?[]const u8 {
        if (item.alias) |alias| return alias;
        return getVariableName(item.expression);
    }

    /// Plan a MERGE clause (node pattern or simple relationship pattern).
    fn planMerge(self: *Self, merge_clause: *const ast.MergeClause, input: ?Operator) PlannerError!Operator {
        const database = self.storage.database orelse return PlannerError.MissingStorage;
        const pattern = merge_clause.pattern;
        if (pattern.elements.len == 0) return PlannerError.InvalidQuery;

        // Node MERGE: MERGE (n {...})
        if (pattern.elements.len == 1) {
            const node_pattern = switch (pattern.elements[0]) {
                .node => |n| n,
                .edge => return PlannerError.InvalidQuery,
            };

            const slot: u8 = blk: {
                if (node_pattern.variable) |name| {
                    if (self.bindings.get(name)) |binding| break :blk binding.slot;
                }
                const new_slot = try self.allocateSlot();
                if (node_pattern.variable) |name| {
                    try self.bindVariable(name, new_slot, .node);
                }
                break :blk new_slot;
            };

            var properties: []const mutation_ops.CreateNode.PropertyKV = &.{};
            if (node_pattern.properties) |props| {
                const kvs = self.allocator.alloc(mutation_ops.CreateNode.PropertyKV, props.len) catch {
                    return PlannerError.OutOfMemory;
                };
                for (props, 0..) |prop, i| {
                    kvs[i] = .{
                        .key = prop.key,
                        .value_expr = prop.value,
                    };
                }
                properties = kvs;
            }

            var on_create_props: []const mutation_ops.CreateNode.PropertyKV = &.{};
            if (merge_clause.on_create) |items| {
                if (node_pattern.variable) |name| {
                    on_create_props = try self.extractSetPropertiesForTarget(items, name);
                } else {
                    on_create_props = try self.extractSetProperties(items);
                }
            }

            var on_match_props: []const mutation_ops.CreateNode.PropertyKV = &.{};
            if (merge_clause.on_match) |items| {
                if (node_pattern.variable) |name| {
                    on_match_props = try self.extractSetPropertiesForTarget(items, name);
                } else {
                    on_match_props = try self.extractSetProperties(items);
                }
            }

            const materialized_input: ?Operator = if (input) |inp| try self.materializeInput(inp) else null;

            const merge_op = mutation_ops.MergeNode.init(
                self.allocator,
                materialized_input,
                node_pattern.labels,
                properties,
                slot,
                database,
                on_create_props,
                on_match_props,
            ) catch return PlannerError.OutOfMemory;

            return merge_op.operator();
        }

        // Relationship MERGE: MERGE (a)-[r:TYPE]->(b)
        if (pattern.elements.len == 3) {
            const left = switch (pattern.elements[0]) {
                .node => |n| n,
                else => return PlannerError.InvalidQuery,
            };
            const edge = switch (pattern.elements[1]) {
                .edge => |e| e,
                else => return PlannerError.InvalidQuery,
            };
            const right = switch (pattern.elements[2]) {
                .node => |n| n,
                else => return PlannerError.InvalidQuery,
            };

            if (edge.types.len == 0) {
                self.setPlanDetail("MERGE needs a relationship type, as in -[:TYPE]->", .{});
                return PlannerError.InvalidQuery;
            }
            if (edge.direction == .both) {
                self.setPlanDetail("MERGE needs a relationship direction, -> or <-", .{});
                return PlannerError.InvalidQuery;
            }

            var op: ?Operator = if (input) |inp| try self.materializeInput(inp) else null;
            const left_slot = try self.resolveMergeNodeSlot(&op, left, database);
            const right_slot = try self.resolveMergeNodeSlot(&op, right, database);

            var source_slot = left_slot;
            var target_slot = right_slot;
            if (edge.direction == .incoming) {
                source_slot = right_slot;
                target_slot = left_slot;
            }

            const edge_slot: ?u8 = blk: {
                if (edge.variable) |name| {
                    if (self.bindings.get(name)) |binding| {
                        if (binding.kind != .edge) return PlannerError.InvalidQuery;
                        break :blk binding.slot;
                    }
                    const slot = try self.allocateNamedSlot(name);
                    try self.bindVariable(name, slot, .edge);
                    break :blk slot;
                }
                break :blk null;
            };

            var edge_props: []const mutation_ops.CreateNode.PropertyKV = &.{};
            if (edge.properties) |props| {
                const kvs = self.allocator.alloc(mutation_ops.CreateNode.PropertyKV, props.len) catch {
                    return PlannerError.OutOfMemory;
                };
                for (props, 0..) |prop, i| {
                    kvs[i] = .{
                        .key = prop.key,
                        .value_expr = prop.value,
                    };
                }
                edge_props = kvs;
            }

            var on_create_props: []const mutation_ops.CreateNode.PropertyKV = &.{};
            if (merge_clause.on_create) |items| {
                const edge_name = edge.variable orelse return PlannerError.InvalidQuery;
                on_create_props = try self.extractSetPropertiesForTarget(items, edge_name);
            }

            var on_match_props: []const mutation_ops.CreateNode.PropertyKV = &.{};
            if (merge_clause.on_match) |items| {
                const edge_name = edge.variable orelse return PlannerError.InvalidQuery;
                on_match_props = try self.extractSetPropertiesForTarget(items, edge_name);
            }

            const merge_edge = mutation_ops.MergeEdge.init(
                self.allocator,
                op orelse return PlannerError.InvalidQuery,
                source_slot,
                target_slot,
                edge.types[0],
                edge_slot,
                database,
                edge_props,
                on_create_props,
                on_match_props,
            ) catch return PlannerError.OutOfMemory;

            return merge_edge.operator();
        }

        return PlannerError.InvalidQuery;
    }

    /// Ensure a MERGE node pattern is bound to a slot, planning a MergeNode
    /// sub-operator when the variable is not already bound.
    fn resolveMergeNodeSlot(
        self: *Self,
        op: *?Operator,
        node_pattern: *const ast.NodePattern,
        database: *Database,
    ) PlannerError!u8 {
        if (node_pattern.variable) |name| {
            if (self.bindings.get(name)) |binding| {
                if (binding.kind != .node) return PlannerError.InvalidQuery;
                return binding.slot;
            }
        }

        const slot = try self.allocateSlot();
        if (node_pattern.variable) |name| {
            try self.bindVariable(name, slot, .node);
        }

        var properties: []const mutation_ops.CreateNode.PropertyKV = &.{};
        if (node_pattern.properties) |props| {
            const kvs = self.allocator.alloc(mutation_ops.CreateNode.PropertyKV, props.len) catch {
                return PlannerError.OutOfMemory;
            };
            for (props, 0..) |prop, i| {
                kvs[i] = .{
                    .key = prop.key,
                    .value_expr = prop.value,
                };
            }
            properties = kvs;
        }

        const merge_node = mutation_ops.MergeNode.init(
            self.allocator,
            op.*,
            node_pattern.labels,
            properties,
            slot,
            database,
            &.{},
            &.{},
        ) catch return PlannerError.OutOfMemory;
        op.* = merge_node.operator();
        return slot;
    }

    /// Wrap an input operator in a Materialize barrier, so every row of the
    /// input is read before the clause downstream writes anything.
    fn materializeInput(self: *Self, input: Operator) PlannerError!Operator {
        const mat = materialize_ops.Materialize.init(self.allocator, input) catch return PlannerError.OutOfMemory;
        return mat.operator();
    }

    /// Extract property key-value pairs from SET items (for MERGE ON CREATE/ON MATCH)
    fn extractSetProperties(self: *Self, items: []const ast.SetItem) PlannerError![]const mutation_ops.CreateNode.PropertyKV {
        var kvs: std.ArrayList(mutation_ops.CreateNode.PropertyKV) = .empty;
        defer kvs.deinit(self.allocator);

        for (items) |item| {
            switch (item) {
                .property => |p| {
                    kvs.append(self.allocator, .{
                        .key = p.property_name,
                        .value_expr = p.value,
                    }) catch return PlannerError.OutOfMemory;
                },
                else => {}, // Labels and map operations not supported in MERGE ON CREATE/MATCH
            }
        }

        return kvs.toOwnedSlice(self.allocator) catch return PlannerError.OutOfMemory;
    }

    /// Extract SET property key/value pairs for a specific target variable.
    fn extractSetPropertiesForTarget(
        self: *Self,
        items: []const ast.SetItem,
        target_name: []const u8,
    ) PlannerError![]const mutation_ops.CreateNode.PropertyKV {
        var kvs: std.ArrayList(mutation_ops.CreateNode.PropertyKV) = .empty;
        defer kvs.deinit(self.allocator);

        for (items) |item| {
            switch (item) {
                .property => |p| {
                    const var_name = getVariableName(p.target) orelse return PlannerError.InvalidQuery;
                    if (!std.mem.eql(u8, var_name, target_name)) return PlannerError.InvalidQuery;
                    kvs.append(self.allocator, .{
                        .key = p.property_name,
                        .value_expr = p.value,
                    }) catch return PlannerError.OutOfMemory;
                },
                else => return PlannerError.InvalidQuery,
            }
        }

        return kvs.toOwnedSlice(self.allocator) catch return PlannerError.OutOfMemory;
    }

    /// The input for a clause that opens a query without reading anything:
    /// one row with nothing bound, so `RETURN 1` and `UNWIND [1, 2] AS x`
    /// evaluate their expressions exactly once.
    fn singleRow(self: *Self) PlannerError!Operator {
        const single = source_ops.SingleRow.init(self.allocator) catch return PlannerError.OutOfMemory;
        return single.operator();
    }

    /// Plan an UNWIND clause (expand list into rows)
    fn planUnwind(self: *Self, unwind_clause: *const ast.UnwindClause, input: ?Operator) PlannerError!Operator {
        // Standalone UNWIND uses a synthetic single-row input.
        const input_op = input orelse try self.singleRow();

        const slot = try self.allocateNamedSlot(unwind_clause.variable);
        try self.bindVariable(unwind_clause.variable, slot, .alias);

        const unwind = unwind_ops.Unwind.init(
            self.allocator,
            input_op,
            unwind_clause.expression,
            slot,
        ) catch return PlannerError.OutOfMemory;

        return unwind.operator();
    }

    fn bindInternalVariable(self: *Self, slot: u8, kind: semantic.VariableKind) PlannerError![]const u8 {
        var attempt: u32 = 0;
        while (true) : (attempt += 1) {
            var buf: [48]u8 = undefined;
            const candidate = std.fmt.bufPrint(&buf, "__edge_prop_{d}_{d}", .{ slot, attempt }) catch {
                return PlannerError.InternalError;
            };
            if (self.bindings.contains(candidate)) continue;

            const owned = self.allocator.dupe(u8, candidate) catch return PlannerError.OutOfMemory;
            self.hidden_binding_names.append(self.allocator, owned) catch {
                self.allocator.free(owned);
                return PlannerError.OutOfMemory;
            };
            try self.bindVariable(owned, slot, kind);
            return owned;
        }
    }

    fn clearHiddenBindingNames(self: *Self) void {
        for (self.hidden_binding_names.items) |name| {
            self.allocator.free(name);
        }
        self.hidden_binding_names.clearRetainingCapacity();
    }

    const all_slots: u16 = std.math.maxInt(u16) >> (16 - MAX_SLOTS);

    /// Allocate a new variable slot.
    ///
    /// A slot no earlier scope used is preferred, so that two names rarely
    /// share a slot across WITH scopes (see `name_slots`); once those run
    /// out, any slot free in the current scope.
    fn allocateSlot(self: *Self) PlannerError!u8 {
        var free = all_slots & ~self.ever_used_slots;
        if (free == 0) free = all_slots & ~self.used_slots;
        if (free == 0) {
            self.setPlanDetail("The query uses more than {d} variables, including ones the planner adds for patterns", .{MAX_SLOTS});
            return PlannerError.TooManyVariables;
        }
        const slot: u8 = @ctz(free);
        self.markSlot(slot);
        return slot;
    }

    /// Allocate the slot for a variable about to be bound: the slot the name
    /// had earlier in the query, if it is free in this scope.
    fn allocateNamedSlot(self: *Self, name: ?[]const u8) PlannerError!u8 {
        if (name) |n| {
            if (self.name_slots.get(n)) |slot| {
                if (self.used_slots & slotBit(slot) == 0) {
                    self.markSlot(slot);
                    return slot;
                }
            }
        }
        return self.allocateSlot();
    }

    fn slotBit(slot: u8) u16 {
        return @as(u16, 1) << @intCast(slot);
    }

    fn markSlot(self: *Self, slot: u8) void {
        self.used_slots |= slotBit(slot);
        self.ever_used_slots |= slotBit(slot);
        if (slot >= self.next_slot) self.next_slot = slot + 1;
    }

    /// Bind a variable name to a slot
    fn bindVariable(self: *Self, name: []const u8, slot: u8, kind: semantic.VariableKind) PlannerError!void {
        self.bindings.put(name, .{
            .name = name,
            .slot = slot,
            .kind = kind,
        }) catch {
            return PlannerError.OutOfMemory;
        };
        self.markSlot(slot);
        const first = self.name_slots.getOrPut(name) catch return PlannerError.OutOfMemory;
        if (!first.found_existing) {
            first.value_ptr.* = slot;
        } else if (first.value_ptr.* != slot and self.slot_conflict == null) {
            self.slot_conflict = name;
        }
    }

    fn setPlanDetail(self: *Self, comptime fmt: []const u8, args: anytype) void {
        const written = std.fmt.bufPrint(&self.detail_buf, fmt, args) catch {
            // A detail that does not fit is worth less than the error itself, so
            // the caller falls back to the generic message rather than failing.
            self.detail_len = 0;
            return;
        };
        self.detail_len = written.len;
    }

    /// Why planning failed, when there is something specific to say.
    pub fn planErrorDetail(self: *const Self) ?[]const u8 {
        if (self.detail_len == 0) return null;
        return self.detail_buf[0..self.detail_len];
    }

    /// The slot of a node variable that is already bound.
    fn boundNodeSlot(self: *const Self, name: []const u8) ?u8 {
        const binding = self.bindings.get(name) orelse return null;
        if (binding.kind != .node) return null;
        return binding.slot;
    }

    /// Get the slot for a variable
    pub fn getSlot(self: *const Self, name: []const u8) ?u8 {
        const binding = self.bindings.get(name) orelse return null;
        return binding.slot;
    }

    /// Register all variable bindings in execution context
    pub fn registerBindings(self: *const Self, ctx: *ExecutionContext) !void {
        var iter = self.bindings.iterator();
        while (iter.next()) |entry| {
            try ctx.registerVariable(entry.key_ptr.*, entry.value_ptr.slot);
        }
    }
};

/// Get variable name from an expression (if it's a simple variable reference)
fn getVariableName(expr: *const ast.Expression) ?[]const u8 {
    return switch (expr.*) {
        .variable => |v| v.name,
        else => null,
    };
}

// ============================================================================
// Tests
// ============================================================================

test "planner initialization" {
    const allocator = std.testing.allocator;
    const storage = StorageContext{
        .node_tree = null,
        .label_index = null,
        .edge_store = null,
        .symbol_table = null,
    };

    var planner = QueryPlanner.init(allocator, storage);
    defer planner.deinit();

    try std.testing.expectEqual(@as(u8, 0), planner.next_slot);
}

test "slot allocation" {
    const allocator = std.testing.allocator;
    const storage = StorageContext{
        .node_tree = null,
        .label_index = null,
        .edge_store = null,
        .symbol_table = null,
    };

    var planner = QueryPlanner.init(allocator, storage);
    defer planner.deinit();

    const slot1 = try planner.allocateSlot();
    const slot2 = try planner.allocateSlot();
    const slot3 = try planner.allocateSlot();

    try std.testing.expectEqual(@as(u8, 0), slot1);
    try std.testing.expectEqual(@as(u8, 1), slot2);
    try std.testing.expectEqual(@as(u8, 2), slot3);
}

test "variable binding" {
    const allocator = std.testing.allocator;
    const storage = StorageContext{
        .node_tree = null,
        .label_index = null,
        .edge_store = null,
        .symbol_table = null,
    };

    var planner = QueryPlanner.init(allocator, storage);
    defer planner.deinit();

    const slot = try planner.allocateSlot();
    try planner.bindVariable("n", slot, .node);

    try std.testing.expectEqual(@as(?u8, 0), planner.getSlot("n"));
    try std.testing.expectEqual(@as(?u8, null), planner.getSlot("m"));
}
