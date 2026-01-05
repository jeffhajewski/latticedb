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

/// FTS search operator that takes query from parameters.
/// Performs full-text search using FTS index with BM25 scoring.
/// Used for queries like: MATCH (d:Doc) WHERE d.text @@ $query
pub const FtsSearchWithInput = struct {
    /// Input operator (not used in current implementation - index is primary source)
    input: Operator,
    /// Slot to output results to
    output_slot: u8,
    /// Parameter name containing query text
    param_name: []const u8,
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
    /// Allocator
    allocator: Allocator,

    const Self = @This();

    /// Create a new FtsSearchWithInput operator
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
            .limit = limit,
            .index = index,
            .results = null,
            .current_index = 0,
            .output_row = null,
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

        // Allocate output row
        self.output_row = ctx.allocRow() catch return OperatorError.OutOfMemory;

        // Open input (we maintain lifecycle even if not using results)
        try self.input.open(ctx);
        self.opened = true;

        // Get query text from parameters
        const param_value = ctx.getParameter(self.param_name) orelse {
            // No parameter provided - can't perform search
            return OperatorError.UnboundVariable;
        };

        // Extract query text from parameter
        const query_text = extractTextFromParam(param_value) orelse {
            return OperatorError.TypeError;
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

        if (self.current_index < results.len) {
            const result = results[self.current_index];
            self.current_index += 1;

            // Build output row
            output_row.clear();
            output_row.setSlot(self.output_slot, .{ .node_ref = result.doc_id });
            output_row.setScore(self.output_slot, result.score);

            return output_row;
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
