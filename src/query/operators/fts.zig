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

const fts_index = @import("../../fts/index.zig");
const FtsIndex = fts_index.FtsIndex;
const FtsError = fts_index.FtsError;

const scorer = @import("../../fts/scorer.zig");
const ScoredDoc = scorer.ScoredDoc;

// ============================================================================
// FtsSearch Operator
// ============================================================================

/// Full-text search operator using FTS index with BM25 scoring.
pub const FtsSearch = struct {
    /// Output slot for document/node IDs
    output_slot: u8,
    /// Query text
    query_text: []const u8,
    /// Maximum results to return
    limit: u32,
    /// Minimum score threshold (optional)
    min_score: ?f32,
    /// FTS index
    index: *FtsIndex,
    /// Search results
    results: ?[]ScoredDoc,
    /// Current result index
    current_index: usize,
    /// Output row
    output_row: ?*Row,
    /// Allocator
    allocator: Allocator,

    const Self = @This();

    /// Create a new FtsSearch operator
    pub fn init(
        allocator: Allocator,
        output_slot: u8,
        query_text: []const u8,
        limit: u32,
        min_score: ?f32,
        index: *FtsIndex,
    ) !*Self {
        const self = try allocator.create(Self);
        self.* = Self{
            .output_slot = output_slot,
            .query_text = query_text,
            .limit = limit,
            .min_score = min_score,
            .index = index,
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
        self.results = self.index.search(self.query_text, self.limit) catch |err| {
            return mapFtsError(err);
        };
        self.current_index = 0;
    }

    fn next(ptr: *anyopaque, _: *ExecutionContext) OperatorError!?*Row {
        const self: *Self = @ptrCast(@alignCast(ptr));

        const results = self.results orelse return OperatorError.NotInitialized;
        const output_row = self.output_row orelse return OperatorError.NotInitialized;

        while (self.current_index < results.len) {
            const result = results[self.current_index];
            self.current_index += 1;

            // Apply score threshold if specified
            if (self.min_score) |threshold| {
                if (result.score < threshold) {
                    continue;
                }
            }

            // Build output row
            output_row.clear();
            // Doc ID is the node ID in our system
            output_row.setSlot(self.output_slot, .{ .node_ref = result.doc_id });
            output_row.setScore(self.output_slot, result.score);

            return output_row;
        }

        return null;
    }

    fn close(ptr: *anyopaque, _: *ExecutionContext) void {
        const self: *Self = @ptrCast(@alignCast(ptr));

        if (self.results) |results| {
            self.index.freeResults(results);
            self.results = null;
        }
    }

    fn deinit(ptr: *anyopaque, allocator: Allocator) void {
        const self: *Self = @ptrCast(@alignCast(ptr));

        // Free results if not already freed
        if (self.results) |results| {
            self.index.freeResults(results);
        }

        allocator.destroy(self);
    }
};

/// Map FTS errors to OperatorError
fn mapFtsError(err: FtsError) OperatorError {
    return switch (err) {
        FtsError.OutOfMemory => OperatorError.OutOfMemory,
        else => OperatorError.StorageError,
    };
}

// ============================================================================
// FtsSearchWithInput Operator
// ============================================================================

/// FTS search operator that filters results against an input operator.
/// Performs full-text search using FTS index with BM25 scoring, then
/// filters results to only include nodes that the input operator produces.
/// Used for queries like: MATCH (d:Doc) WHERE d.text @@ $query
/// or: MATCH (d:Doc) WHERE d.text @@ "literal search"
pub const FtsSearchWithInput = struct {
    /// Input operator - used to filter FTS results
    input: Operator,
    /// Slot to read node IDs from input (and output results to)
    output_slot: u8,
    /// Parameter name containing query text (null for literal queries)
    param_name: ?[]const u8,
    /// Literal query text (null for parameter queries)
    literal_query: ?[]const u8,
    /// Maximum results
    limit: u32,
    /// FTS index
    index: *FtsIndex,
    /// Search results
    results: ?[]ScoredDoc,
    /// Current result index
    current_index: usize,
    /// Output row
    output_row: ?*Row,
    /// Whether opened
    opened: bool,
    /// Set of valid node IDs from input operator (for filtering)
    valid_nodes: std.AutoHashMapUnmanaged(NodeId, void),
    /// Allocator
    allocator: Allocator,

    const Self = @This();

    /// Create a new FtsSearchWithInput operator with parameter-based query
    pub fn init(
        allocator: Allocator,
        input: Operator,
        output_slot: u8,
        param_name: []const u8,
        limit: u32,
        index: *FtsIndex,
    ) !*Self {
        const self = try allocator.create(Self);
        self.* = Self{
            .input = input,
            .output_slot = output_slot,
            .param_name = param_name,
            .literal_query = null,
            .limit = limit,
            .index = index,
            .results = null,
            .current_index = 0,
            .output_row = null,
            .opened = false,
            .valid_nodes = .{},
            .allocator = allocator,
        };
        return self;
    }

    /// Create a new FtsSearchWithInput operator with literal query text
    pub fn initWithLiteral(
        allocator: Allocator,
        input: Operator,
        output_slot: u8,
        query_text: []const u8,
        limit: u32,
        index: *FtsIndex,
    ) !*Self {
        const self = try allocator.create(Self);
        self.* = Self{
            .input = input,
            .output_slot = output_slot,
            .param_name = null,
            .literal_query = query_text,
            .limit = limit,
            .index = index,
            .results = null,
            .current_index = 0,
            .output_row = null,
            .opened = false,
            .valid_nodes = .{},
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

        // Open input and collect valid node IDs
        try self.input.open(ctx);
        self.opened = true;

        // Drain input operator to build set of valid node IDs
        while (try self.input.next(ctx)) |row| {
            if (row.getSlot(self.output_slot)) |slot_val| {
                if (slot_val.asNodeId()) |node_id| {
                    self.valid_nodes.put(self.allocator, node_id, {}) catch {
                        return OperatorError.OutOfMemory;
                    };
                }
            }
        }

        // Get query text - either from literal or parameter
        const query_text = if (self.literal_query) |lit|
            lit
        else if (self.param_name) |pname| blk: {
            const param_value = ctx.getParameter(pname) orelse {
                return OperatorError.UnboundVariable;
            };
            break :blk extractTextFromParam(param_value) orelse {
                return OperatorError.TypeError;
            };
        } else {
            return OperatorError.UnboundVariable;
        };

        // Perform the FTS search
        self.results = self.index.search(query_text, self.limit) catch |err| {
            return mapFtsError(err);
        };
        self.current_index = 0;
    }

    fn next(ptr: *anyopaque, _: *ExecutionContext) OperatorError!?*Row {
        const self: *Self = @ptrCast(@alignCast(ptr));

        if (!self.opened) return OperatorError.NotInitialized;

        const results = self.results orelse return null;
        const output_row = self.output_row orelse return OperatorError.NotInitialized;

        // Find next FTS result that is also in the valid_nodes set
        while (self.current_index < results.len) {
            const result = results[self.current_index];
            self.current_index += 1;

            // Filter: only return nodes that the input operator produced
            if (self.valid_nodes.contains(result.doc_id)) {
                // Build output row
                output_row.clear();
                output_row.setSlot(self.output_slot, .{ .node_ref = result.doc_id });
                output_row.setScore(self.output_slot, result.score);
                return output_row;
            }
        }

        return null;
    }

    fn close(ptr: *anyopaque, ctx: *ExecutionContext) void {
        const self: *Self = @ptrCast(@alignCast(ptr));

        // Free search results
        if (self.results) |results| {
            self.index.freeResults(results);
            self.results = null;
        }

        // Clear valid nodes set
        self.valid_nodes.deinit(self.allocator);
        self.valid_nodes = .{};

        if (self.opened) {
            self.input.close(ctx);
            self.opened = false;
        }
    }

    fn deinit(ptr: *anyopaque, allocator: Allocator) void {
        const self: *Self = @ptrCast(@alignCast(ptr));

        // Free results if not already freed
        if (self.results) |results| {
            self.index.freeResults(results);
        }

        // Free valid nodes if not already freed
        self.valid_nodes.deinit(self.allocator);

        self.input.deinit(allocator);
        allocator.destroy(self);
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

test "FtsSearch basic structure" {
    // Verify vtable is properly structured
    const vtable = FtsSearch.vtable;
    try std.testing.expect(@TypeOf(vtable.open) != void);
    try std.testing.expect(@TypeOf(vtable.next) != void);
    try std.testing.expect(@TypeOf(vtable.close) != void);
    try std.testing.expect(@TypeOf(vtable.deinit) != void);
}

test "FtsSearchWithInput basic structure" {
    // Verify vtable is properly structured
    const vtable = FtsSearchWithInput.vtable;
    try std.testing.expect(@TypeOf(vtable.open) != void);
    try std.testing.expect(@TypeOf(vtable.next) != void);
    try std.testing.expect(@TypeOf(vtable.close) != void);
    try std.testing.expect(@TypeOf(vtable.deinit) != void);
}
