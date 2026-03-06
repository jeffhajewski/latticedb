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

/// Vector search operator that takes query vector from parameters.
/// Performs k-NN search using HNSW index and filters by distance threshold.
/// Used for queries like: MATCH (n) WHERE n.embedding <=> $query < 0.5
pub const VectorSearchWithInput = struct {
    /// Input operator providing candidate nodes to filter against
    input: Operator,
    /// Slot to output results to
    output_slot: u8,
    /// Parameter name containing query vector (null for literal vectors)
    param_name: ?[]const u8,
    /// Literal query vector (null for parameter-based queries)
    literal_query: ?[]const f32,
    /// Number of results to return
    k: u32,
    /// Distance threshold (optional)
    distance_threshold: ?f32,
    /// HNSW index
    index: *HnswIndex,
    /// Search results from HNSW
    results: ?[]SearchResult,
    /// Current result index
    current_index: usize,
    /// Current row index within the active node group
    current_node_row_index: usize,
    /// Candidate input rows grouped by node ID
    rows_by_node: std.AutoHashMapUnmanaged(NodeId, std.ArrayListUnmanaged(Row)),
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
            .literal_query = null,
            .k = k,
            .distance_threshold = distance_threshold,
            .index = index,
            .results = null,
            .current_index = 0,
            .current_node_row_index = 0,
            .rows_by_node = .{},
            .opened = false,
            .allocator = allocator,
        };
        return self;
    }

    /// Create a new VectorSearchWithInput operator with literal query vector
    pub fn initWithLiteral(
        allocator: Allocator,
        input: Operator,
        output_slot: u8,
        query_vector: []const f32,
        k: u32,
        distance_threshold: ?f32,
        index: *HnswIndex,
    ) !*Self {
        const self = try allocator.create(Self);
        self.* = Self{
            .input = input,
            .output_slot = output_slot,
            .param_name = null,
            .literal_query = query_vector,
            .k = k,
            .distance_threshold = distance_threshold,
            .index = index,
            .results = null,
            .current_index = 0,
            .current_node_row_index = 0,
            .rows_by_node = .{},
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
            if (self.results) |results| {
                self.index.freeResults(results);
                self.results = null;
            }
            if (self.opened) {
                self.input.close(ctx);
                self.opened = false;
            }
        }

        self.clearRowsByNode();

        // Get query vector from literal or parameters
        const query_vector = if (self.literal_query) |literal|
            literal
        else if (self.param_name) |param_name| blk: {
            const param_value = ctx.getParameter(param_name) orelse {
                return OperatorError.UnboundVariable;
            };

            break :blk extractVectorFromParam(param_value) orelse {
                return OperatorError.TypeError;
            };
        } else {
            return OperatorError.UnboundVariable;
        };

        // Perform the HNSW search
        self.results = self.index.search(query_vector, self.k, null) catch |err| {
            return mapHnswError(err);
        };

        // Precompute node IDs that survived vector-side filtering to avoid
        // storing irrelevant input rows.
        var allowed_nodes: std.AutoHashMapUnmanaged(NodeId, void) = .{};
        defer allowed_nodes.deinit(self.allocator);
        if (self.results) |results| {
            for (results) |result| {
                if (self.distance_threshold) |threshold| {
                    if (result.distance > threshold) continue;
                }
                allowed_nodes.put(self.allocator, result.node_id, {}) catch return OperatorError.OutOfMemory;
            }
        }

        // Drain input and retain full rows for each candidate node so next()
        // preserves upstream bindings and row multiplicity.
        while (try self.input.next(ctx)) |row| {
            const slot_val = row.getSlot(self.output_slot) orelse continue;
            const node_id = slot_val.asNodeId() orelse continue;
            if (!allowed_nodes.contains(node_id)) continue;

            const gop = self.rows_by_node.getOrPut(self.allocator, node_id) catch return OperatorError.OutOfMemory;
            if (!gop.found_existing) {
                gop.value_ptr.* = .{};
            }
            gop.value_ptr.append(self.allocator, row.*) catch return OperatorError.OutOfMemory;
        }

        self.current_index = 0;
        self.current_node_row_index = 0;
    }

    fn next(ptr: *anyopaque, _: *ExecutionContext) OperatorError!?*Row {
        const self: *Self = @ptrCast(@alignCast(ptr));

        if (!self.opened) return OperatorError.NotInitialized;

        const results = self.results orelse return null;

        while (self.current_index < results.len) {
            const result = results[self.current_index];

            // Apply distance threshold if specified
            if (self.distance_threshold) |threshold| {
                if (result.distance > threshold) {
                    self.current_index += 1;
                    self.current_node_row_index = 0;
                    continue;
                }
            }

            if (self.rows_by_node.getPtr(result.node_id)) |rows| {
                if (self.current_node_row_index < rows.items.len) {
                    const row = &rows.items[self.current_node_row_index];
                    self.current_node_row_index += 1;
                    row.setDistance(self.output_slot, result.distance);
                    return row;
                }
            }

            self.current_index += 1;
            self.current_node_row_index = 0;
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

        self.clearRowsByNode();

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

        self.clearRowsByNode();

        self.input.deinit(allocator);
        allocator.destroy(self);
    }

    fn clearRowsByNode(self: *Self) void {
        var iter = self.rows_by_node.iterator();
        while (iter.next()) |entry| {
            entry.value_ptr.deinit(self.allocator);
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
