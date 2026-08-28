//! Full-text search operator for query execution.
//!
//! Performs BM25-scored full-text search using the FTS index.
//! Returns documents ordered by relevance score.

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

const scorer = @import("../../fts/scorer.zig");
const ScoredDoc = scorer.ScoredDoc;

const fts_catalog = @import("../../fts/catalog.zig");
const FtsEntityKind = fts_catalog.FtsEntityKind;

const database_mod = @import("../../storage/database.zig");
const Database = database_mod.Database;

// ============================================================================
// FtsSearch Operator
// ============================================================================

/// Full-text search operator using FTS index with BM25 scoring.
/// How many matches a full-text search may return when the query asked for no
/// particular number.
///
/// There is no cap. A predicate with no LIMIT beside it means every row that
/// matches, and any smaller number would drop rows the user asked for without
/// saying so. The search applies this by trimming a result set it has already
/// built, so a large value costs nothing up front.
///
/// This was 100 for both constructors, which made `WHERE d.title @@ 'loaf'`
/// return a hundred rows out of five thousand matches, silently, with no LIMIT
/// anywhere in the query — and return all five thousand once the same predicate
/// was moved inside an OR, because the row filter used a different bound. Same
/// predicate, same data, two answers depending on where it sat.
pub const NO_RESULT_LIMIT: u32 = std.math.maxInt(u32);

/// One `x.property @@ query` to run.
///
/// The label and property are resolved during planning from the pattern the
/// variable was written in, so by the time the operator runs the index is known
/// to exist.
pub const Search = struct {
    /// Whether this searches a node index or an edge one.
    kind: FtsEntityKind,
    /// The label for a node index, the relationship type for an edge one.
    scope: []const u8,
    property: []const u8,
    /// Parameter name holding the query text, for `@@ $param`
    param_name: ?[]const u8 = null,
    /// Literal query text, for `@@ "text"`
    literal_query: ?[]const u8 = null,
};

/// Read matching entities straight out of a declared index.
///
/// This is a leaf: it has no input to filter, because the index already knows
/// which entities match. A `Doc.title` index holds only `Doc` nodes, so seeking
/// it does the work a label scan would have done and answers the text question at
/// the same time.
///
/// The alternative, and what this replaces where it applies, is scanning every
/// node carrying the label and keeping the ones the index named. That costs the
/// whole corpus to answer a query about one document: measured on eight thousand
/// documents, a one-hit query took 72ms that way against 31us for the index
/// lookup it was already doing.
///
/// Rows come out in score order, best first, which is the order the search
/// returns and the order a caller sorting by relevance already expects.
pub const FtsIndexSeek = struct {
    /// Slot to bind each matching entity into
    output_slot: u8,
    /// The searches whose results this unions, as in FtsSearchWithInput
    searches: []const Search,
    limit: u32,
    database: *Database,
    results: ?[]ScoredDoc,
    current_index: usize,
    current_row: ?*Row,
    allocator: Allocator,

    const Self = @This();

    pub fn init(
        allocator: Allocator,
        output_slot: u8,
        searches: []const Search,
        limit: u32,
        database: *Database,
    ) !*Self {
        const self = try allocator.create(Self);
        self.* = Self{
            .output_slot = output_slot,
            .searches = searches,
            .limit = limit,
            .database = database,
            .results = null,
            .current_index = 0,
            .current_row = null,
            .allocator = allocator,
        };
        return self;
    }

    pub fn operator(self: *Self) Operator {
        return Operator{ .vtable = &vtable, .ptr = self };
    }

    const vtable = Operator.VTable{
        .open = open,
        .next = next,
        .close = close,
        .deinit = deinit,
    };

    fn open(ptr: *anyopaque, ctx: *ExecutionContext) OperatorError!void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        self.current_row = ctx.allocRow() catch return OperatorError.OutOfMemory;
        self.results = try runSearches(self.database, ctx, self.searches, self.limit, self.allocator);
        self.current_index = 0;
    }

    fn next(ptr: *anyopaque, _: *ExecutionContext) OperatorError!?*Row {
        const self: *Self = @ptrCast(@alignCast(ptr));

        const results = self.results orelse return OperatorError.NotInitialized;
        const row = self.current_row orelse return OperatorError.NotInitialized;

        if (self.current_index >= results.len) return null;
        const hit = results[self.current_index];
        self.current_index += 1;

        row.clear();
        const searching_edges = self.searches.len > 0 and self.searches[0].kind == .edge;
        row.setSlot(self.output_slot, if (searching_edges)
            .{ .edge_ref = hit.doc_id }
        else
            .{ .node_ref = hit.doc_id });
        row.setScore(self.output_slot, hit.score);
        return row;
    }

    fn close(ptr: *anyopaque, _: *ExecutionContext) void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        if (self.results) |results| {
            self.allocator.free(results);
            self.results = null;
        }
        self.current_index = 0;
    }

    fn deinit(ptr: *anyopaque, allocator: Allocator) void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        if (self.results) |results| {
            self.allocator.free(results);
            self.results = null;
        }
        allocator.destroy(self);
    }
};

/// Run every search and union the results, taking each document's best score.
///
/// Shared by the seek and the filtering operator so the two cannot disagree
/// about what a disjunction means.
fn runSearches(
    database: *Database,
    ctx: *ExecutionContext,
    searches: []const Search,
    limit: u32,
    allocator: Allocator,
) OperatorError![]ScoredDoc {
    var best: std.AutoHashMapUnmanaged(NodeId, f32) = .{};
    defer best.deinit(allocator);

    for (searches) |search| {
        const query_text = if (search.literal_query) |lit|
            lit
        else if (search.param_name) |pname| blk: {
            const param_value = ctx.getParameter(pname) orelse return OperatorError.UnboundVariable;
            break :blk extractTextFromParam(param_value) orelse return OperatorError.TypeError;
        } else {
            return OperatorError.UnboundVariable;
        };

        const hits = database.ftsSearchIndexInTxn(
            ctx.txn,
            search.kind,
            search.scope,
            search.property,
            query_text,
            limit,
        ) catch return OperatorError.StorageError;
        defer database.freeFtsSearchResults(hits);

        for (hits) |hit| {
            const gop = best.getOrPut(allocator, hit.doc_id) catch return OperatorError.OutOfMemory;
            if (!gop.found_existing or hit.score > gop.value_ptr.*) {
                gop.value_ptr.* = hit.score;
            }
        }
    }

    const merged = allocator.alloc(ScoredDoc, best.count()) catch return OperatorError.OutOfMemory;
    var len: usize = 0;
    var iter = best.iterator();
    while (iter.next()) |entry| {
        merged[len] = .{ .doc_id = entry.key_ptr.*, .score = entry.value_ptr.* };
        len += 1;
    }
    std.mem.sort(ScoredDoc, merged[0..len], {}, ScoredDoc.lessThan);
    if (len > limit) len = limit;
    return merged[0..len];
}

pub const FtsSearchWithInput = struct {
    /// Input operator - used to filter FTS results
    input: Operator,
    /// Slot to read node IDs from input (and output results to)
    output_slot: u8,
    /// The searches whose results this unions.
    ///
    /// One entry for a plain `x.p @@ q`, several for a disjunction of them. A
    /// disjunction planned as one operator reads each index once. Left to the row
    /// filter it searches an index per candidate row, which is quadratic for a
    /// term most documents contain.
    searches: []const Search,
    /// Maximum results
    limit: u32,
    /// Database for txn-aware FTS search
    database: *Database,
    /// Search results
    results: ?[]ScoredDoc,
    /// Current result index
    current_index: usize,
    /// Current row index within the active document group
    current_doc_row_index: usize,
    /// Whether opened
    opened: bool,
    /// Candidate input rows grouped by document/node ID
    rows_by_doc: std.AutoHashMapUnmanaged(NodeId, std.ArrayListUnmanaged(Row)),
    /// Allocator
    allocator: Allocator,

    const Self = @This();

    /// Create an operator over one or more searches.
    ///
    /// The slice belongs to the caller and must outlive the operator, which the
    /// planner's arena guarantees.
    pub fn init(
        allocator: Allocator,
        input: Operator,
        output_slot: u8,
        searches: []const Search,
        limit: u32,
        database: *Database,
    ) !*Self {
        const self = try allocator.create(Self);
        self.* = Self{
            .input = input,
            .output_slot = output_slot,
            .searches = searches,
            .limit = limit,
            .database = database,
            .results = null,
            .current_index = 0,
            .current_doc_row_index = 0,
            .opened = false,
            .rows_by_doc = .{},
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
            self.clearRowsByDoc();
            if (self.results) |results| {
                self.allocator.free(results);
                self.results = null;
            }
            if (self.opened) {
                self.input.close(ctx);
                self.opened = false;
            }
        }

        self.clearRowsByDoc();

        self.results = try runSearches(self.database, ctx, self.searches, self.limit, self.allocator);

        // Keep only input rows whose node IDs are present in FTS results while
        // preserving full row context and multiplicity.
        var allowed_docs: std.AutoHashMapUnmanaged(NodeId, void) = .{};
        defer allowed_docs.deinit(self.allocator);
        if (self.results) |results| {
            for (results) |result| {
                allowed_docs.put(self.allocator, result.doc_id, {}) catch return OperatorError.OutOfMemory;
            }
        }

        // A slot holds a node or an edge, and which one decides where the
        // document id comes from. Reading it as a node id regardless would make
        // an edge search silently match nothing.
        const searching_edges = self.searches.len > 0 and self.searches[0].kind == .edge;
        while (try self.input.next(ctx)) |row| {
            const slot_val = row.getSlot(self.output_slot) orelse continue;
            const doc_id = (if (searching_edges) slot_val.asEdgeId() else slot_val.asNodeId()) orelse continue;
            if (!allowed_docs.contains(doc_id)) continue;

            const gop = self.rows_by_doc.getOrPut(self.allocator, doc_id) catch return OperatorError.OutOfMemory;
            if (!gop.found_existing) gop.value_ptr.* = .empty;
            gop.value_ptr.append(self.allocator, row.*) catch return OperatorError.OutOfMemory;
        }

        self.current_index = 0;
        self.current_doc_row_index = 0;
    }

    fn next(ptr: *anyopaque, _: *ExecutionContext) OperatorError!?*Row {
        const self: *Self = @ptrCast(@alignCast(ptr));

        if (!self.opened) return OperatorError.NotInitialized;

        const results = self.results orelse return null;
        while (self.current_index < results.len) {
            const result = results[self.current_index];

            if (self.rows_by_doc.getPtr(result.doc_id)) |rows| {
                if (self.current_doc_row_index < rows.items.len) {
                    const row = &rows.items[self.current_doc_row_index];
                    self.current_doc_row_index += 1;
                    row.setScore(self.output_slot, result.score);
                    return row;
                }
            }

            self.current_index += 1;
            self.current_doc_row_index = 0;
        }

        return null;
    }

    fn close(ptr: *anyopaque, ctx: *ExecutionContext) void {
        const self: *Self = @ptrCast(@alignCast(ptr));

        // The union is this operator's own allocation. Each search's results
        // were freed as that search finished; what survives is what was built
        // here, from this allocator.
        if (self.results) |results| {
            self.allocator.free(results);
            self.results = null;
        }

        self.clearRowsByDoc();

        if (self.opened) {
            self.input.close(ctx);
            self.opened = false;
        }
    }

    fn deinit(ptr: *anyopaque, allocator: Allocator) void {
        const self: *Self = @ptrCast(@alignCast(ptr));

        // Free results if not already freed
        if (self.results) |results| {
            self.allocator.free(results);
        }

        self.clearRowsByDoc();

        self.input.deinit(allocator);
        allocator.destroy(self);
    }

    fn clearRowsByDoc(self: *Self) void {
        var iter = self.rows_by_doc.iterator();
        while (iter.next()) |entry| {
            entry.value_ptr.deinit(self.allocator);
        }
        self.rows_by_doc.deinit(self.allocator);
        self.rows_by_doc = .{};
    }
};

/// Extract query text from a PropertyValue parameter
fn extractTextFromParam(param: types.PropertyValue) ?[]const u8 {
    return switch (param) {
        .string_val => |s| s,
        else => null,
    };
}

// ============================================================================
// Tests
// ============================================================================

test "FtsSearchWithInput basic structure" {
    // Verify vtable is properly structured
    const vtable = FtsSearchWithInput.vtable;
    try std.testing.expect(@TypeOf(vtable.open) != void);
    try std.testing.expect(@TypeOf(vtable.next) != void);
    try std.testing.expect(@TypeOf(vtable.close) != void);
    try std.testing.expect(@TypeOf(vtable.deinit) != void);
}
