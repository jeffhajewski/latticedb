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
const mutation_ops = @import("operators/mutation.zig");
const aggregate_ops = @import("operators/aggregate.zig");
const distinct_ops = @import("operators/distinct.zig");
const unwind_ops = @import("operators/unwind.zig");
const source_ops = @import("operators/source.zig");
const materialize_ops = @import("operators/materialize.zig");
const cross_product_ops = @import("operators/cross_product.zig");

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
    next_slot: u8,
    /// Number of output columns from the RETURN clause (set during planning)
    output_columns: u8 = 0,
    /// Explicit output column names from RETURN aliases.
    output_column_names: [MAX_SLOTS]?[]const u8,
    /// Which scan the planner chose for the most recent node pattern.
    ///
    /// An index scan and a label scan return the same rows, so behaviour alone
    /// cannot tell you which one ran. This records the decision so that tests
    /// can assert an index is actually being used, rather than assuming it from
    /// a result set that would look identical either way.
    last_scan_kind: ScanKind = .none,

    /// How the planner resolved a node pattern to rows.
    pub const ScanKind = enum {
        /// No node pattern has been planned yet.
        none,
        /// Every node carrying the label, filtered afterwards.
        label_scan,
        /// Straight to the matching nodes through a property index.
        property_index_scan,
        /// Straight to the matching entities through a full-text index.
        fts_index_seek,
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
    }

    /// Plan a complete query
    pub fn plan(self: *Self, query: *const ast.Query, analysis: *const semantic.AnalysisResult) PlannerError!Operator {
        _ = analysis; // Used for validation, already done

        // Reset bindings for this query
        self.clearHiddenBindingNames();
        self.bindings.clearRetainingCapacity();
        self.pattern_labels.clearRetainingCapacity();
        self.pattern_edge_types.clearRetainingCapacity();
        self.edge_bindings.clearRetainingCapacity();
        self.next_slot = 0;
        self.output_columns = 0;
        self.output_column_names = [_]?[]const u8{null} ** MAX_SLOTS;

        var current_op: ?Operator = null;

        // Process each clause. RETURN + trailing ORDER BY/SKIP/LIMIT is
        // planned as sort/window first, then projection, so ORDER BY can
        // reference non-returned variables (e.g. RETURN n.name ORDER BY n.age).
        var clause_idx: usize = 0;
        while (clause_idx < query.clauses.len) : (clause_idx += 1) {
            const clause = query.clauses[clause_idx];
            switch (clause) {
                .match => |m| {
                    const where_hint: ?*const ast.Expression = if (clause_idx + 1 < query.clauses.len)
                        switch (query.clauses[clause_idx + 1]) {
                            .where => |w| w.condition,
                            else => null,
                        }
                    else
                        null;
                    current_op = try self.planMatch(m, current_op, where_hint);
                },
                .where => |w| current_op = try self.planWhere(w, current_op),
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

                    var input_for_return = current_op;
                    var lookahead = clause_idx + 1;
                    const first_trailing = lookahead;
                    while (lookahead < query.clauses.len) : (lookahead += 1) {
                        switch (query.clauses[lookahead]) {
                            .order_by, .skip, .limit => {},
                            else => break,
                        }
                        if (aggregates) continue;
                        switch (query.clauses[lookahead]) {
                            .order_by => |o| input_for_return = try self.planOrderBy(o, input_for_return, r),
                            .skip => |s| input_for_return = try self.planSkip(s, input_for_return),
                            .limit => |l| input_for_return = try self.planLimit(l, input_for_return),
                            else => unreachable,
                        }
                    }

                    current_op = try self.planReturn(r, input_for_return);

                    if (aggregates) {
                        var i = first_trailing;
                        while (i < lookahead) : (i += 1) {
                            switch (query.clauses[i]) {
                                .order_by => |o| current_op = try self.planOrderByOnOutput(o, current_op, r),
                                .skip => |sk| current_op = try self.planSkip(sk, current_op),
                                .limit => |l| current_op = try self.planLimit(l, current_op),
                                else => unreachable,
                            }
                        }
                    }

                    clause_idx = lookahead - 1;
                },
                .order_by => |o| current_op = try self.planOrderBy(o, current_op, null),
                .limit => |l| current_op = try self.planLimit(l, current_op),
                .skip => |s| current_op = try self.planSkip(s, current_op),
                .create => |c| current_op = try self.planCreate(c, current_op),
                .delete => |d| current_op = try self.planDelete(d, current_op),
                .set => |s| current_op = try self.planSet(s, current_op),
                .remove => |r| current_op = try self.planRemove(r, current_op),
                .with => |w| current_op = try self.planWith(w, current_op),
                .merge => |m| current_op = try self.planMerge(m, current_op),
                .unwind => |u| current_op = try self.planUnwind(u, current_op),
            }
        }

        return current_op orelse PlannerError.InvalidQuery;
    }

    /// Plan a MATCH clause
    fn planMatch(
        self: *Self,
        match: *const ast.MatchClause,
        input: ?Operator,
        where_hint: ?*const ast.Expression,
    ) PlannerError!Operator {
        var op = input;

        for (match.patterns) |pattern| {
            op = try self.planPattern(pattern, op, where_hint);
        }

        return op orelse PlannerError.InvalidQuery;
    }

    /// Plan a single pattern
    fn planPattern(
        self: *Self,
        pattern: ast.Pattern,
        input: ?Operator,
        where_hint: ?*const ast.Expression,
    ) PlannerError!Operator {
        var op = input;
        var prev_node_slot: ?u8 = null;

        for (pattern.elements) |element| {
            switch (element) {
                .node => |node_pattern| {
                    // If prev_node_slot is set, this node follows an edge.
                    // Use the target slot from the Expand operator instead of allocating a new slot.
                    const slot = if (prev_node_slot != null and op != null)
                        prev_node_slot.? // Use target slot from preceding edge's Expand
                    else
                        try self.allocateSlot();
                    var node_var_name: ?[]const u8 = null;

                    // Bind variable if present
                    if (node_pattern.variable) |name| {
                        try self.bindVariable(name, slot, .node);
                        node_var_name = name;
                        self.pattern_labels.put(name, node_pattern.labels) catch {
                            return PlannerError.OutOfMemory;
                        };
                    }

                    // Determine if we need to create a scan or filter
                    const is_target_of_edge = prev_node_slot != null and op != null;

                    if (is_target_of_edge) {
                        // This node is the target of an edge - data comes from Expand operator.
                        // If labels are specified, add a filter to check them.
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
                },
                .edge => |edge_pattern| {
                    // Need previous node to expand from
                    const source_slot = prev_node_slot orelse return PlannerError.InvalidQuery;
                    const target_slot = try self.allocateSlot();

                    // Optional edge variable
                    var edge_slot: ?u8 = null;
                    var edge_var_name: ?[]const u8 = null;
                    if (edge_pattern.variable) |name| {
                        edge_slot = try self.allocateSlot();
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

                    // Determine edge type
                    var edge_type: ?SymbolId = null;
                    if (edge_pattern.types.len > 0) {
                        const symbol_table = self.storage.symbol_table orelse return PlannerError.MissingStorage;
                        edge_type = symbol_table.lookup(edge_pattern.types[0]) catch |err| switch (err) {
                            // Unknown relationship type should behave like an empty match, not a planning error.
                            symbols.SymbolError.NotFound => symbols.NULL_SYMBOL,
                            else => return PlannerError.InternalError,
                        };
                    }

                    // Map direction
                    const expand_dir: expand_ops.ExpandDirection = switch (edge_pattern.direction) {
                        .outgoing => .outgoing,
                        .incoming => .incoming,
                        .both => .both,
                    };

                    // Create expand operator (variable-length or regular)
                    _ = self.storage.edge_store orelse return PlannerError.MissingStorage;
                    const database = self.storage.database orelse return PlannerError.MissingStorage;

                    if (edge_pattern.quantifier) |quant| {
                        // Variable-length path: use VariableLengthExpand
                        const var_expand = var_expand_ops.VariableLengthExpand.init(
                            self.allocator,
                            op.?,
                            source_slot,
                            target_slot,
                            edge_type,
                            expand_dir,
                            database,
                            quant.min_hops,
                            quant.max_hops,
                        ) catch {
                            return PlannerError.OutOfMemory;
                        };
                        op = var_expand.operator();
                    } else {
                        // Regular single-hop expand
                        const expand = expand_ops.Expand.init(
                            self.allocator,
                            op.?,
                            source_slot,
                            target_slot,
                            edge_slot,
                            edge_type,
                            expand_dir,
                            database,
                        ) catch {
                            return PlannerError.OutOfMemory;
                        };
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

        return op orelse PlannerError.InvalidQuery;
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

    /// Plan a WHERE clause
    /// Detects vector distance (<=>)  and FTS match (@@) operators and creates
    /// specialized search operators for them instead of generic filters.
    fn planWhere(self: *Self, where: *const ast.WhereClause, input: ?Operator) PlannerError!Operator {
        const input_op = input orelse return PlannerError.InvalidQuery;

        // Check for vector search pattern: x.embedding <=> $query [< threshold]
        if (self.detectVectorSearch(where.condition)) |vector_info| {
            return self.planVectorSearch(input_op, vector_info);
        }

        // Check for FTS pattern: x.text @@ $query, or a disjunction of them
        if (self.detectFtsSearch(where.condition)) |fts_info| {
            return self.planFtsSearch(input_op, &[_]FtsSearchInfo{fts_info});
        }
        {
            var disjuncts: [MAX_FTS_DISJUNCTS]FtsSearchInfo = undefined;
            var count: usize = 0;
            if (self.collectFtsDisjuncts(where.condition, &disjuncts, &count) and count > 1) {
                return self.planFtsSearch(input_op, disjuncts[0..count]);
            }
        }

        // Default: create a filter operator
        const filter = filter_ops.Filter.init(self.allocator, input_op, where.condition) catch {
            return PlannerError.OutOfMemory;
        };

        return filter.operator();
    }

    /// Information extracted from a vector search pattern
    const VectorSearchInfo = struct {
        /// The variable slot being searched (e.g., slot for 'n' in n.embedding)
        variable_slot: ?u8,
        /// Parameter name for query vector (e.g., "query" from $query)
        param_name: ?[]const u8,
        /// Literal query vector (if provided directly)
        query_vector: ?[]const f32,
        /// Distance threshold (e.g., 0.5 from < 0.5)
        threshold: ?f32,
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
                        // Extract threshold from right side
                        if (binary.right.* == .literal) {
                            const lit = binary.right.literal;
                            if (lit.value == .float) {
                                info.threshold = @floatCast(lit.value.float);
                            } else if (lit.value == .integer) {
                                info.threshold = @floatFromInt(lit.value.integer);
                            }
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
    fn planVectorSearch(self: *Self, input: Operator, info: VectorSearchInfo) PlannerError!Operator {
        _ = self.storage.hnsw_index orelse return PlannerError.MissingStorage;
        const database = self.storage.database orelse return PlannerError.MissingStorage;

        const output_slot = info.variable_slot orelse return PlannerError.InvalidQuery;

        const k: u32 = 100; // Default k for search
        const vector_search = if (info.query_vector) |query_vector|
            vector_ops.VectorSearchWithInput.initWithLiteral(
                self.allocator,
                input,
                output_slot,
                query_vector,
                k,
                info.threshold,
                database,
            ) catch return PlannerError.OutOfMemory
        else blk: {
            const param_name = info.param_name orelse return PlannerError.InvalidQuery;
            break :blk vector_ops.VectorSearchWithInput.init(
                self.allocator,
                input,
                output_slot,
                param_name,
                k,
                info.threshold,
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
            if (search.kind != searches[0].kind) return PlannerError.InvalidQuery;
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

        return fts_search.operator();
    }

    /// Plan a RETURN clause
    fn planReturn(self: *Self, ret: *const ast.ReturnClause, input: ?Operator) PlannerError!Operator {
        const input_op = input orelse return PlannerError.InvalidQuery;

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
            self.output_column_names[i] = item.alias orelse
                self.columnNameForExpression(item.expression);
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
                if (call.arguments.len != 1) break :blk call.name;
                const arg = self.columnNameForExpression(call.arguments[0]) orelse break :blk call.name;
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
            const slot: u8 = @intCast(i);
            self.output_column_names[i] = item.alias orelse
                self.columnNameForExpression(item.expression);

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
            else => false,
        };
    }

    /// Plan an ORDER BY that runs after projection, sorting by output column.
    fn planOrderByOnOutput(
        self: *Self,
        order: *const ast.OrderByClause,
        input: ?Operator,
        ret: *const ast.ReturnClause,
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
                return PlannerError.InvalidQuery;
            };
            sort_items[i] = .{
                .expr = item.expression,
                .descending = item.descending,
                .slot = slot,
            };
        }

        const sort = limit_ops.Sort.init(self.allocator, input_op, sort_items) catch {
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
            };
        }

        const sort = limit_ops.Sort.init(self.allocator, input_op, sort_items) catch {
            return PlannerError.OutOfMemory;
        };

        return sort.operator();
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
                                slot = try self.allocateSlot();
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
                                return PlannerError.InvalidQuery; // Edge type required
                            }

                            // Bind edge variable if present
                            var edge_slot: ?u8 = null;
                            if (pe.edge_pattern.variable) |edge_name| {
                                const edge_var_slot = try self.allocateSlot();
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

                    if (binding.kind != .node) return PlannerError.InvalidQuery;

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

        // Preserve upstream bindings referenced by WITH expressions so runtime
        // evaluation can resolve them even after WITH scope rebinding.
        var required_input_bindings: std.ArrayList(VarBinding) = .empty;
        defer required_input_bindings.deinit(self.allocator);
        for (with_clause.items) |item| {
            try self.collectReferencedBindings(item.expression, &required_input_bindings);
        }

        // Check for aggregates (same logic as RETURN)
        var has_aggregates = false;
        for (with_clause.items) |item| {
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
                .items = with_clause.items,
                .location = with_clause.location,
            };
            op = try self.planAggregateReturn(ret, input_op);
        } else {
            // Simple projection
            var items = self.allocator.alloc(project_ops.ProjectItem, with_clause.items.len) catch {
                return PlannerError.OutOfMemory;
            };
            for (with_clause.items, 0..) |item, i| {
                items[i] = .{
                    .expr = item.expression,
                    .output_slot = @intCast(i),
                };
            }
            const project = project_ops.Project.init(self.allocator, input_op, items) catch {
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

        // Reset bindings to WITH aliases (WITH introduces a new scope)
        self.bindings.clearRetainingCapacity();
        // What comes through a WITH is an alias rather than a node written with a
        // label, so the labels that were in scope before it no longer describe it.
        self.pattern_labels.clearRetainingCapacity();
        self.pattern_edge_types.clearRetainingCapacity();
        self.next_slot = 0;

        for (with_clause.items, 0..) |item, i| {
            const name = item.alias orelse getExpressionName(item.expression) orelse continue;
            try self.bindVariable(name, @intCast(i), .node);
            self.next_slot = @intCast(i + 1);
        }

        // Keep upstream bindings used by WITH expressions available for execution.
        for (required_input_bindings.items) |binding| {
            if (!self.bindings.contains(binding.name)) {
                try self.bindVariable(binding.name, binding.slot, binding.kind);
                if (binding.slot >= self.next_slot) {
                    self.next_slot = binding.slot + 1;
                }
            }
        }

        // Add WHERE filter if present
        if (with_clause.where) |condition| {
            const filter = filter_ops.Filter.init(self.allocator, op, condition) catch {
                return PlannerError.OutOfMemory;
            };
            op = filter.operator();
        }

        return op;
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

            if (edge.types.len == 0) return PlannerError.InvalidQuery;
            if (edge.direction == .both) return PlannerError.InvalidQuery;

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
                    const slot = try self.allocateSlot();
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

    /// Wrap an input operator in a Materialize barrier to release page latches
    /// before downstream mutation operators write to the same storage pages.
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

    /// Plan an UNWIND clause (expand list into rows)
    fn planUnwind(self: *Self, unwind_clause: *const ast.UnwindClause, input: ?Operator) PlannerError!Operator {
        // Standalone UNWIND uses a synthetic single-row input.
        const input_op = if (input) |op|
            op
        else blk: {
            const single = source_ops.SingleRow.init(self.allocator) catch return PlannerError.OutOfMemory;
            break :blk single.operator();
        };

        const slot = try self.allocateSlot();
        try self.bindVariable(unwind_clause.variable, slot, .alias);

        const unwind = unwind_ops.Unwind.init(
            self.allocator,
            input_op,
            unwind_clause.expression,
            slot,
        ) catch return PlannerError.OutOfMemory;

        return unwind.operator();
    }

    fn collectReferencedBindings(self: *Self, expr: *const ast.Expression, out: *std.ArrayList(VarBinding)) PlannerError!void {
        switch (expr.*) {
            .variable => |v| {
                const binding = self.bindings.get(v.name) orelse return PlannerError.InvalidQuery;
                for (out.items) |existing| {
                    if (std.mem.eql(u8, existing.name, binding.name)) return;
                }
                out.append(self.allocator, binding) catch return PlannerError.OutOfMemory;
            },
            .property_access => |pa| try self.collectReferencedBindings(pa.object, out),
            .binary => |b| {
                try self.collectReferencedBindings(b.left, out);
                try self.collectReferencedBindings(b.right, out);
            },
            .unary => |u| try self.collectReferencedBindings(u.operand, out),
            .function_call => |f| {
                for (f.arguments) |arg| try self.collectReferencedBindings(arg, out);
            },
            .list => |l| {
                for (l.elements) |elem| try self.collectReferencedBindings(elem, out);
            },
            .map => |m| {
                for (m.entries) |entry| try self.collectReferencedBindings(entry.value, out);
            },
            .literal, .parameter => {},
        }
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

    /// Allocate a new variable slot
    fn allocateSlot(self: *Self) PlannerError!u8 {
        if (self.next_slot >= MAX_SLOTS) {
            return PlannerError.TooManyVariables;
        }
        const slot = self.next_slot;
        self.next_slot += 1;
        return slot;
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

/// Get a name for an expression (variable name or property access path)
fn getExpressionName(expr: *const ast.Expression) ?[]const u8 {
    return switch (expr.*) {
        .variable => |v| v.name,
        .property_access => |pa| pa.property,
        .function_call => |f| f.name,
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
