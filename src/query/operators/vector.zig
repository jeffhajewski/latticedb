//! Vector search operator for query execution.
//!
//! Performs k-NN search using HNSW index.
//! Returns nodes ordered by vector distance.

const std = @import("std");
const Allocator = std.mem.Allocator;

const executor = @import("../executor.zig");
const Operator = executor.Operator;
const OperatorError = executor.OperatorError;
const Row = executor.Row;
const SlotValue = executor.SlotValue;
const ExecutionContext = executor.ExecutionContext;

const types = @import("../../core/types.zig");
const NodeId = types.NodeId;

const hnsw = @import("../../vector/hnsw.zig");
const SearchResult = hnsw.SearchResult;

const database_mod = @import("../../storage/database.zig");
const Database = database_mod.Database;

const ast = @import("../ast.zig");
const ExpressionEvaluator = @import("../expression.zig").ExpressionEvaluator;
const limit_ops = @import("limit.zig");
const scan_ops = @import("scan.zig");
const SymbolId = @import("../../graph/symbols.zig").SymbolId;

// ============================================================================
// VectorSearch Operator
// ============================================================================

/// Vector search operator using HNSW index.
/// Performs k-NN search and returns nodes with their distances.
pub const VectorSearch = struct {
    /// Output slot for node IDs
    output_slot: u8,
    /// Query vector
    query_vector: []const f32,
    /// Number of results to return
    k: u32,
    /// Optional distance threshold
    distance_threshold: ?f32,
    /// Database for txn-aware vector search
    database: *Database,
    /// Search results
    results: ?[]SearchResult,
    /// Current result index
    current_index: usize,
    /// Output row
    output_row: ?*Row,
    /// Allocator
    allocator: Allocator,

    const Self = @This();

    /// Create a new VectorSearch operator
    pub fn init(
        allocator: Allocator,
        output_slot: u8,
        query_vector: []const f32,
        k: u32,
        distance_threshold: ?f32,
        database: *Database,
    ) !*Self {
        const self = try allocator.create(Self);
        self.* = Self{
            .output_slot = output_slot,
            .query_vector = query_vector,
            .k = k,
            .distance_threshold = distance_threshold,
            .database = database,
            .results = null,
            .current_index = 0,
            .output_row = null,
            .allocator = allocator,
        };
        return self;
    }

    /// Get the Operator interface
    pub fn operator(self: *Self) Operator {
        return Operator{
            .vtable = &vtable,
            .ptr = self,
        };
    }

    const vtable = Operator.VTable{
        .open = open,
        .next = next,
        .close = close,
        .deinit = deinit,
    };

    fn open(ptr: *anyopaque, ctx: *ExecutionContext) OperatorError!void {
        const self: *Self = @ptrCast(@alignCast(ptr));

        // Allocate output row
        self.output_row = ctx.allocRow() catch return OperatorError.OutOfMemory;

        // Perform the search
        self.results = self.database.vectorSearchInTxn(ctx.txn, self.query_vector, self.k, null) catch |err|
            return ctx.storageFailure(err, "searching the vector index", .{});
        self.current_index = 0;
    }

    fn next(ptr: *anyopaque, _: *ExecutionContext) OperatorError!?*Row {
        const self: *Self = @ptrCast(@alignCast(ptr));

        const results = self.results orelse return OperatorError.NotInitialized;
        const output_row = self.output_row orelse return OperatorError.NotInitialized;

        while (self.current_index < results.len) {
            const result = results[self.current_index];
            self.current_index += 1;

            // Apply distance threshold if specified
            if (self.distance_threshold) |threshold| {
                if (result.distance > threshold) {
                    continue;
                }
            }

            // Build output row
            output_row.clear();
            output_row.setSlot(self.output_slot, .{ .node_ref = result.node_id });
            output_row.setDistance(self.output_slot, result.distance);

            return output_row;
        }

        return null;
    }

    fn close(ptr: *anyopaque, _: *ExecutionContext) void {
        const self: *Self = @ptrCast(@alignCast(ptr));

        if (self.results) |results| {
            self.database.freeVectorSearchResults(results);
            self.results = null;
        }
    }

    fn deinit(ptr: *anyopaque, allocator: Allocator) void {
        const self: *Self = @ptrCast(@alignCast(ptr));

        // Free results if not already freed
        if (self.results) |results| {
            self.database.freeVectorSearchResults(results);
        }

        allocator.destroy(self);
    }
};

// ============================================================================
// VectorSearchWithInput Operator
// ============================================================================

/// Answers a WHERE clause that is a vector distance predicate, such as
/// `MATCH (d:Doc) WHERE d.embedding <=> $q < 0.3 RETURN d`.
///
/// The input rows are the candidates. The operator reads them all, then asks
/// for the nearest vectors among the candidates' nodes only
/// (`Database.vectorSearchAmongInTxn`): a small set is measured node by
/// node, a larger one searched through the graph with the set as a filter.
/// Every result is a candidate, so it asks for as many nodes as it needs:
///
/// - Under a RETURN with a LIMIT and no ORDER BY: SKIP + LIMIT nodes (each
///   has at least one row), within the threshold if there is one.
/// - With a threshold alone (`< t` or `<= t`, a literal or a parameter):
///   every candidate within it. The search widens (doubling k) until the
///   farthest result is beyond the threshold or there are no more.
/// - Otherwise: the `DEFAULT_K` nearest candidate nodes.
///
/// Rows come out nearest first and carry their distance. What remains
/// approximate is the graph search of a large set: HNSW can miss a true
/// neighbour, so a candidate within the threshold is occasionally left out.
pub const VectorSearchWithInput = struct {
    /// Input operator providing candidate nodes to filter against
    input: Operator,
    /// Slot to output results to
    output_slot: u8,
    /// Parameter name containing query vector (null for literal vectors)
    param_name: ?[]const u8,
    /// Literal query vector (null for parameter-based queries)
    literal_query: ?[]const f32,
    /// What bounds the answer
    bound: Bound,
    /// Database for txn-aware vector search
    database: *Database,
    /// Candidate nodes that matched, nearest first
    results: ?[]SearchResult,
    /// Whether `results` belongs to this operator rather than the database
    owned_results: bool = false,
    /// Threshold resolved at open
    threshold: ?f32,
    /// How many matching nodes to return, when nothing else bounds the answer
    node_cap: ?usize,
    /// Matching nodes returned so far
    nodes_returned: usize,
    /// Current result index
    current_index: usize,
    /// Current row index within the active node group
    current_node_row_index: usize,
    /// Input rows grouped by node
    rows_by_node: std.AutoHashMapUnmanaged(NodeId, Candidate),
    /// Row built for candidates that carried nothing but their node
    scratch_row: ?*Row,
    /// Whether opened
    opened: bool,
    /// Allocator
    allocator: Allocator,

    const Self = @This();

    /// How many nearest nodes a bare `<=>` predicate returns when neither a
    /// threshold nor a LIMIT bounds it.
    pub const DEFAULT_K: u32 = 100;

    /// The distance bound of `<=> q < t` or `<=> q <= t`.
    pub const Threshold = struct {
        value: union(enum) {
            literal: f32,
            parameter: []const u8,
        },
        /// `<=` rather than `<`
        inclusive: bool,
    };

    pub const Bound = struct {
        threshold: ?Threshold = null,
        /// LIMIT and SKIP of the RETURN that directly follows, when nothing
        /// between them reorders or drops rows.
        limit: ?*const ast.Expression = null,
        skip: ?*const ast.Expression = null,
    };

    /// The input rows for one node. Rows that carry nothing but the node
    /// (what a scan produces) are only counted, which keeps a large label
    /// scan from being copied in full.
    const Candidate = struct {
        bare_rows: u32 = 0,
        rows: std.ArrayListUnmanaged(Row) = .empty,

        fn count(self: Candidate) usize {
            return self.bare_rows + self.rows.items.len;
        }
    };

    /// Create a new VectorSearchWithInput operator
    pub fn init(
        allocator: Allocator,
        input: Operator,
        output_slot: u8,
        param_name: []const u8,
        bound: Bound,
        database: *Database,
    ) !*Self {
        return create(allocator, input, output_slot, param_name, null, bound, database);
    }

    /// Create a new VectorSearchWithInput operator with literal query vector
    pub fn initWithLiteral(
        allocator: Allocator,
        input: Operator,
        output_slot: u8,
        query_vector: []const f32,
        bound: Bound,
        database: *Database,
    ) !*Self {
        return create(allocator, input, output_slot, null, query_vector, bound, database);
    }

    fn create(
        allocator: Allocator,
        input: Operator,
        output_slot: u8,
        param_name: ?[]const u8,
        literal_query: ?[]const f32,
        bound: Bound,
        database: *Database,
    ) !*Self {
        const self = try allocator.create(Self);
        self.* = Self{
            .input = input,
            .output_slot = output_slot,
            .param_name = param_name,
            .literal_query = literal_query,
            .bound = bound,
            .database = database,
            .results = null,
            .threshold = null,
            .node_cap = null,
            .nodes_returned = 0,
            .current_index = 0,
            .current_node_row_index = 0,
            .rows_by_node = .{},
            .scratch_row = null,
            .opened = false,
            .allocator = allocator,
        };
        return self;
    }

    /// Get the Operator interface
    pub fn operator(self: *Self) Operator {
        return Operator{
            .vtable = &vtable,
            .ptr = self,
        };
    }

    const vtable = Operator.VTable{
        .open = open,
        .next = next,
        .close = close,
        .deinit = deinit,
    };

    fn open(ptr: *anyopaque, ctx: *ExecutionContext) OperatorError!void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        try self.input.open(ctx);
        self.opened = true;
        errdefer {
            self.clearRowsByNode();
            self.freeResults();
            if (self.opened) {
                self.input.close(ctx);
                self.opened = false;
            }
        }

        self.clearRowsByNode();
        self.freeResults();
        self.current_index = 0;
        self.current_node_row_index = 0;
        self.nodes_returned = 0;
        self.scratch_row = ctx.allocRow() catch return OperatorError.OutOfMemory;

        // Get query vector from literal or parameters
        const query_vector = if (self.literal_query) |literal|
            literal
        else if (self.param_name) |param_name| blk: {
            const param_value = ctx.getParameter(param_name) orelse
                return ctx.fail(OperatorError.UnboundVariable, null, "Parameter ${s} was not given a value", .{param_name});

            break :blk extractVectorFromParam(param_value) orelse
                return ctx.fail(OperatorError.TypeError, null, "Parameter ${s} must be a vector for <=>, but it is a {s}", .{ param_name, @tagName(param_value) });
        } else {
            return OperatorError.UnboundVariable;
        };

        // A null threshold compares as null, which no row satisfies.
        var empty = false;
        self.threshold = null;
        if (self.bound.threshold) |threshold| {
            switch (threshold.value) {
                .literal => |t| self.threshold = t,
                .parameter => |name| {
                    const value = ctx.getParameter(name) orelse
                        return ctx.fail(OperatorError.UnboundVariable, null, "Parameter ${s} was not given a value", .{name});
                    switch (value) {
                        .int_val => |i| self.threshold = @floatFromInt(i),
                        .float_val => |f| self.threshold = @floatCast(f),
                        .null_val => empty = true,
                        else => return ctx.fail(OperatorError.TypeError, null, "Parameter ${s} must be a number to bound a vector distance, but it is a {s}", .{ name, @tagName(value) }),
                    }
                },
            }
        }

        // How many rows the RETURN's LIMIT takes. Without a LIMIT or a
        // threshold, the answer is the DEFAULT_K nearest nodes.
        var rows_wanted: ?usize = null;
        self.node_cap = null;
        if (self.bound.limit) |limit_expr| {
            var evaluator = ExpressionEvaluator.init(ctx.row_arena.allocator());
            const limit = try limit_ops.evaluateCountExpression(&evaluator, limit_expr, ctx);
            const skip = if (self.bound.skip) |skip_expr|
                try limit_ops.evaluateCountExpression(&evaluator, skip_expr, ctx)
            else
                0;
            rows_wanted = @intCast(@min(limit +| skip, std.math.maxInt(u32)));
        } else if (self.bound.threshold == null) {
            self.node_cap = DEFAULT_K;
        }

        const first = try self.input.next(ctx);
        if (empty or first == null) return;
        if (rows_wanted) |wanted| {
            if (wanted == 0) return;
        }

        if (scan_ops.LabelScan.of(self.input)) |label_scan| {
            if (label_scan.output_slot == self.output_slot) {
                return self.openOverLabel(ctx, query_vector, rows_wanted, label_scan.label_id, first.?);
            }
        }

        // Every input row, grouped by node; the nodes are what the search
        // ranks.
        const bare_mask: u16 = @as(u16, 1) << @intCast(self.output_slot);
        var next_row = first;
        while (next_row) |row| : (next_row = try self.input.next(ctx)) {
            const slot_val = row.getSlot(self.output_slot) orelse continue;
            const node_id = slot_val.asNodeId() orelse continue;
            // Bare: nothing but the node, and no search has measured or
            // scored it. A distance or score of exactly zero is still one.
            const bare = row.populated == bare_mask and row.measured == 0 and row.scored == 0;
            const candidate = try self.candidateFor(node_id);
            if (bare) {
                candidate.bare_rows += 1;
            } else {
                candidate.rows.append(self.allocator, row.*) catch return OperatorError.OutOfMemory;
            }
        }
        const node_count = self.rows_by_node.count();
        if (node_count == 0) return;

        const ids = self.allocator.alloc(NodeId, node_count) catch return OperatorError.OutOfMemory;
        defer self.allocator.free(ids);
        var keys = self.rows_by_node.keyIterator();
        var n: usize = 0;
        while (keys.next()) |id| : (n += 1) ids[n] = id.*;
        const member: database_mod.VectorFilter = .{
            .context = &self.rows_by_node,
            .allowsFn = struct {
                fn allows(context: *const anyopaque, node_id: NodeId) bool {
                    const map: *const std.AutoHashMapUnmanaged(NodeId, Candidate) = @ptrCast(@alignCast(context));
                    return map.contains(node_id);
                }
            }.allows,
        };

        // Each result is a candidate node with at least one row, so a LIMIT
        // or a node cap needs that many nodes; a threshold alone needs the
        // search widened until it passes the threshold.
        const k: u32 = blk: {
            const start: usize = rows_wanted orelse self.node_cap orelse DEFAULT_K;
            break :blk @intCast(@max(@min(start, node_count, std.math.maxInt(u32) / 2), 1));
        };
        var found: std.AutoHashMapUnmanaged(NodeId, void) = .{};
        defer found.deinit(self.allocator);
        const exhausted = try self.searchWidening(ctx, query_vector, k, rows_wanted, node_count, .{ .set = .{ .ids = ids, .member = member } }, &found);

        // next() walks the results. Candidates a complete search did not
        // return (ones the graph could not reach) are measured directly.
        if (exhausted) try self.measureMissing(ctx, query_vector, &found);
    }

    /// Search until the answer is complete: once for a LIMIT or a node cap,
    /// doubling k for a threshold alone until the furthest result is beyond
    /// it. Whether the search ran out of nodes to give.
    fn searchWidening(
        self: *Self,
        ctx: *ExecutionContext,
        query_vector: []const f32,
        start_k: u32,
        rows_wanted: ?usize,
        node_count: usize,
        among: Among,
        found: *std.AutoHashMapUnmanaged(NodeId, void),
    ) OperatorError!bool {
        var k = start_k;
        while (true) {
            try self.search(ctx, query_vector, k, among, found);
            const results = self.results orelse return false;
            // There are no more to give.
            if (results.len < k) return true;
            if (rows_wanted != null or self.node_cap != null) return false;
            const t = self.threshold orelse return false;
            // Everything further out is beyond the threshold too.
            if (!self.within(results[results.len - 1].distance, t)) return false;
            if (k >= node_count or k >= std.math.maxInt(u32) / 2) return false;
            k = @intCast(@min(@as(usize, k) * 2, node_count));
        }
    }

    /// The candidates when the input is a label scan, as `openOverLabel`
    /// gives them.
    ///
    /// Reading every input row and filing it by node cost more than the
    /// search for a label holding many of the vectors: about 2 ms of the
    /// 3 ms at 50k of 100k. A label scan's rows are its nodes, one each, so
    /// a large label is searched through the graph with the label index as
    /// the filter, and only as much of the scan is read as it takes to see
    /// the label is too large to measure node by node.
    const LabelFilter = struct {
        database: *Database,
        txn: @FieldType(ExecutionContext, "txn"),
        label_id: SymbolId,
        /// A failure the filter could not return; the search is failed
        /// with it afterwards.
        failure: ?anyerror = null,

        fn allows(context: *const anyopaque, node_id: NodeId) bool {
            const self: *LabelFilter = @ptrCast(@alignCast(@constCast(context)));
            if (self.database.overlayNodeHasLabel(self.txn, node_id, self.label_id)) |has| return has;
            return self.database.label_index.contains(self.label_id, node_id) catch |err| {
                self.failure = err;
                return false;
            };
        }
    };

    const Among = union(enum) {
        /// These nodes; `member` tells whether a node is one of them.
        set: struct { ids: []const NodeId, member: ?database_mod.VectorFilter },
        /// The nodes the filter allows, through the graph.
        filter: *LabelFilter,
    };

    /// `open` for an input that is a label scan (`first` is its first row).
    ///
    /// Reads the scan only until it holds more nodes than measuring each
    /// would pay for (`Database.exactScanCount`). A label that ends before
    /// that is measured node by node; a larger one is searched through the
    /// graph with the label as the filter, and the rest of the scan is
    /// never read. Only the nodes the search returns get a row.
    fn openOverLabel(
        self: *Self,
        ctx: *ExecutionContext,
        query_vector: []const f32,
        rows_wanted: ?usize,
        label_id: SymbolId,
        first: *Row,
    ) OperatorError!void {
        const index = &(self.database.hnsw_index orelse return ctx.storageFailure(error.MissingIndex, "searching the vector index", .{}));
        const enough = Database.exactScanCount(index.vector_count, index.config.ef_search, index.config.m_max0);

        var ids: std.ArrayListUnmanaged(NodeId) = .empty;
        defer ids.deinit(self.allocator);
        var next_row: ?*Row = first;
        while (next_row) |row| {
            const node_id = row.getSlot(self.output_slot).?.asNodeId().?;
            ids.append(self.allocator, node_id) catch return OperatorError.OutOfMemory;
            if (ids.items.len > enough) break;
            next_row = try self.input.next(ctx);
        }
        const whole = next_row == null;

        const start: usize = rows_wanted orelse self.node_cap orelse DEFAULT_K;
        var found: std.AutoHashMapUnmanaged(NodeId, void) = .{};
        defer found.deinit(self.allocator);
        var filter: LabelFilter = .{ .database = self.database, .txn = ctx.txn, .label_id = label_id };
        const exhausted = if (whole) blk: {
            const k: u32 = @intCast(@max(@min(start, ids.items.len, std.math.maxInt(u32) / 2), 1));
            break :blk try self.searchWidening(ctx, query_vector, k, rows_wanted, ids.items.len, .{ .set = .{ .ids = ids.items, .member = null } }, &found);
        } else blk: {
            const k: u32 = @intCast(@max(@min(start, std.math.maxInt(u32) / 2), 1));
            const ran_out = try self.searchWidening(ctx, query_vector, k, rows_wanted, std.math.maxInt(u32), .{ .filter = &filter }, &found);
            if (filter.failure) |err| return ctx.storageFailure(err, "reading the label index", .{});
            break :blk ran_out;
        };

        // Rows for the nodes returned, each the scan's one row.
        if (exhausted) {
            // Rare: the search could not give as many as asked. Every node
            // of the label is a candidate to measure, as on the general path.
            if (!whole) {
                while (try self.input.next(ctx)) |row| {
                    ids.append(self.allocator, row.getSlot(self.output_slot).?.asNodeId().?) catch return OperatorError.OutOfMemory;
                }
            }
            for (ids.items) |id| (try self.candidateFor(id)).bare_rows = 1;
            try self.measureMissing(ctx, query_vector, &found);
        }
        for (self.results orelse &.{}) |result| (try self.candidateFor(result.node_id)).bare_rows = 1;
    }

    /// Ask for the k nearest vectors among `among`; `found` becomes their
    /// nodes.
    fn search(
        self: *Self,
        ctx: *ExecutionContext,
        query_vector: []const f32,
        k: u32,
        among: Among,
        found: *std.AutoHashMapUnmanaged(NodeId, void),
    ) OperatorError!void {
        self.freeResults();
        const results = switch (among) {
            .set => |set| self.database.vectorSearchAmongInTxn(ctx.txn, query_vector, k, null, set.ids, set.member),
            .filter => |f| self.database.vectorSearchWhereInTxn(ctx.txn, query_vector, k, null, .{ .context = f, .allowsFn = LabelFilter.allows }),
        } catch |err|
            return ctx.storageFailure(err, "searching the vector index", .{});
        self.results = results;
        found.clearRetainingCapacity();
        found.ensureTotalCapacity(self.allocator, @intCast(results.len)) catch return OperatorError.OutOfMemory;
        for (results) |result| found.putAssumeCapacity(result.node_id, {});
    }

    fn candidateFor(self: *Self, node_id: NodeId) OperatorError!*Candidate {
        const gop = self.rows_by_node.getOrPut(self.allocator, node_id) catch return OperatorError.OutOfMemory;
        if (!gop.found_existing) gop.value_ptr.* = .{};
        return gop.value_ptr;
    }

    /// Add to the results, in distance order, the candidates a complete
    /// search did not return (`found` holds the nodes it did).
    ///
    /// Such a candidate is either without a vector or one the graph could
    /// not reach: HNSW can leave a point unreachable from its entry point, as
    /// when a tight cluster's neighbour lists all point inside the cluster.
    fn measureMissing(
        self: *Self,
        ctx: *ExecutionContext,
        query_vector: []const f32,
        found: *const std.AutoHashMapUnmanaged(NodeId, void),
    ) OperatorError!void {
        var missing: usize = 0;
        var keys = self.rows_by_node.keyIterator();
        while (keys.next()) |node_id| {
            if (!found.contains(node_id.*)) missing += 1;
        }
        if (missing == 0) return;

        var matches: std.ArrayListUnmanaged(SearchResult) = .empty;
        errdefer matches.deinit(self.allocator);
        if (self.results) |results| {
            for (results) |result| {
                if (!self.rows_by_node.contains(result.node_id)) continue;
                matches.append(self.allocator, result) catch return OperatorError.OutOfMemory;
            }
        }
        // The database's own measure, the one the index's results are on.
        // The index's distance function would not do: under cosine it expects
        // both vectors already at unit length, and neither need be here.
        keys = self.rows_by_node.keyIterator();
        while (keys.next()) |node_id| {
            if (found.contains(node_id.*)) continue;
            const vector = (self.database.getNodeVectorInTxn(ctx.txn, node_id.*) catch |err|
                return ctx.storageFailure(err, "reading the vector of node {d}", .{node_id.*})) orelse continue;
            defer self.database.allocator.free(vector);
            if (vector.len != query_vector.len) continue;
            matches.append(self.allocator, .{
                .node_id = node_id.*,
                .distance = self.database.vectorDistance(query_vector, vector),
            }) catch return OperatorError.OutOfMemory;
        }
        std.mem.sort(SearchResult, matches.items, {}, struct {
            fn lessThan(_: void, lhs: SearchResult, rhs: SearchResult) bool {
                return lhs.distance < rhs.distance;
            }
        }.lessThan);

        const owned = matches.toOwnedSlice(self.allocator) catch return OperatorError.OutOfMemory;
        self.freeResults();
        self.results = owned;
        self.owned_results = true;
    }

    fn within(self: *const Self, distance: f32, threshold: f32) bool {
        const inclusive = if (self.bound.threshold) |t| t.inclusive else true;
        return if (inclusive) distance <= threshold else distance < threshold;
    }

    fn next(ptr: *anyopaque, _: *ExecutionContext) OperatorError!?*Row {
        const self: *Self = @ptrCast(@alignCast(ptr));

        if (!self.opened) return OperatorError.NotInitialized;

        const results = self.results orelse return null;

        while (self.current_index < results.len) {
            const result = results[self.current_index];

            if (self.threshold) |threshold| {
                // Results are nearest first, so nothing after this qualifies.
                if (!self.within(result.distance, threshold)) return null;
            }

            if (self.rows_by_node.getPtr(result.node_id)) |candidate| {
                if (self.current_node_row_index == 0) {
                    if (self.node_cap) |cap| {
                        if (self.nodes_returned >= cap) return null;
                    }
                    self.nodes_returned += 1;
                }
                const index = self.current_node_row_index;
                if (index < candidate.count()) {
                    self.current_node_row_index += 1;
                    const row = if (index < candidate.bare_rows) blk: {
                        const scratch = self.scratch_row orelse return OperatorError.NotInitialized;
                        scratch.clear();
                        scratch.setSlot(self.output_slot, .{ .node_ref = result.node_id });
                        break :blk scratch;
                    } else &candidate.rows.items[index - candidate.bare_rows];
                    row.setDistance(self.output_slot, result.distance);
                    return row;
                }
            }

            self.current_index += 1;
            self.current_node_row_index = 0;
        }

        return null;
    }

    fn freeResults(self: *Self) void {
        if (self.results) |results| {
            if (self.owned_results) {
                self.allocator.free(results);
            } else {
                self.database.freeVectorSearchResults(results);
            }
            self.results = null;
        }
        self.owned_results = false;
    }

    fn close(ptr: *anyopaque, ctx: *ExecutionContext) void {
        const self: *Self = @ptrCast(@alignCast(ptr));

        self.freeResults();
        self.clearRowsByNode();

        if (self.opened) {
            self.input.close(ctx);
            self.opened = false;
        }
    }

    fn deinit(ptr: *anyopaque, allocator: Allocator) void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        self.freeResults();
        self.clearRowsByNode();
        self.input.deinit(allocator);
        allocator.destroy(self);
    }

    fn clearRowsByNode(self: *Self) void {
        var iter = self.rows_by_node.iterator();
        while (iter.next()) |entry| {
            entry.value_ptr.rows.deinit(self.allocator);
        }
        self.rows_by_node.deinit(self.allocator);
        self.rows_by_node = .{};
    }
};

/// Extract a vector ([]const f32) from a PropertyValue parameter
fn extractVectorFromParam(param: types.PropertyValue) ?[]const f32 {
    return switch (param) {
        .vector_val => |v| v,
        else => null,
    };
}

// ============================================================================
// Tests
// ============================================================================

test "VectorSearch basic structure" {
    const vtable = VectorSearch.vtable;
    try std.testing.expect(@TypeOf(vtable.open) != void);
    try std.testing.expect(@TypeOf(vtable.next) != void);
    try std.testing.expect(@TypeOf(vtable.close) != void);
    try std.testing.expect(@TypeOf(vtable.deinit) != void);
}

test "VectorSearchWithInput basic structure" {
    const vtable = VectorSearchWithInput.vtable;
    try std.testing.expect(@TypeOf(vtable.open) != void);
    try std.testing.expect(@TypeOf(vtable.next) != void);
    try std.testing.expect(@TypeOf(vtable.close) != void);
    try std.testing.expect(@TypeOf(vtable.deinit) != void);
}
