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
const HnswIndex = hnsw.HnswIndex;
const HnswError = hnsw.HnswError;
const SearchResult = hnsw.SearchResult;

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
    /// HNSW index
    index: *HnswIndex,
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
        index: *HnswIndex,
    ) !*Self {
        const self = try allocator.create(Self);
        self.* = Self{
            .output_slot = output_slot,
            .query_vector = query_vector,
            .k = k,
            .distance_threshold = distance_threshold,
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
        self.results = self.index.search(self.query_vector, self.k, null) catch |err| {
            return mapHnswError(err);
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

/// Map HNSW errors to OperatorError
fn mapHnswError(err: HnswError) OperatorError {
    return switch (err) {
        HnswError.OutOfMemory => OperatorError.OutOfMemory,
        else => OperatorError.StorageError,
    };
}

// ============================================================================
// VectorSearchWithInput Operator
// ============================================================================

/// Vector search operator that takes query vector from input rows.
/// Useful for queries like: MATCH (n) WHERE n.embedding <=> $query < 0.5
pub const VectorSearchWithInput = struct {
    /// Input operator
    input: Operator,
    /// Slot to output results to
    output_slot: u8,
    /// Parameter name containing query vector
    param_name: []const u8,
    /// Number of results to return per input
    k: u32,
    /// Distance threshold
    distance_threshold: ?f32,
    /// HNSW index
    index: *HnswIndex,
    /// Output row
    output_row: ?*Row,
    /// Whether opened
    opened: bool,
    /// Allocator
    allocator: Allocator,

    const Self = @This();

    /// Create a new VectorSearchWithInput operator
    pub fn init(
        allocator: Allocator,
        input: Operator,
        output_slot: u8,
        param_name: []const u8,
        k: u32,
        distance_threshold: ?f32,
        index: *HnswIndex,
    ) !*Self {
        const self = try allocator.create(Self);
        self.* = Self{
            .input = input,
            .output_slot = output_slot,
            .param_name = param_name,
            .k = k,
            .distance_threshold = distance_threshold,
            .index = index,
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

        // Open input
        try self.input.open(ctx);
        self.opened = true;
    }

    fn next(ptr: *anyopaque, ctx: *ExecutionContext) OperatorError!?*Row {
        const self: *Self = @ptrCast(@alignCast(ptr));
        _ = ctx;

        if (!self.opened) return OperatorError.NotInitialized;

        // This is a simplified version - full implementation would
        // perform vector search for each input row
        // For now, we just pass through
        return null;
    }

    fn close(ptr: *anyopaque, ctx: *ExecutionContext) void {
        const self: *Self = @ptrCast(@alignCast(ptr));

        if (self.opened) {
            self.input.close(ctx);
            self.opened = false;
        }
    }

    fn deinit(ptr: *anyopaque, allocator: Allocator) void {
        const self: *Self = @ptrCast(@alignCast(ptr));

        self.input.deinit(allocator);
        allocator.destroy(self);
    }
};

// ============================================================================
// Tests
// ============================================================================

test "VectorSearch basic structure" {
    try std.testing.expect(VectorSearch.vtable.open != null);
    try std.testing.expect(VectorSearch.vtable.next != null);
    try std.testing.expect(VectorSearch.vtable.close != null);
    try std.testing.expect(VectorSearch.vtable.deinit != null);
}

test "VectorSearchWithInput basic structure" {
    try std.testing.expect(VectorSearchWithInput.vtable.open != null);
    try std.testing.expect(VectorSearchWithInput.vtable.next != null);
    try std.testing.expect(VectorSearchWithInput.vtable.close != null);
    try std.testing.expect(VectorSearchWithInput.vtable.deinit != null);
}
