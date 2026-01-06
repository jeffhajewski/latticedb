//! Query executor using the Volcano iterator model.
//!
//! This module defines the core types for query execution:
//! - Operator: The pull-based iterator interface
//! - Row: Tuple flowing between operators
//! - ExecutionContext: Runtime context for query evaluation

const std = @import("std");
const Allocator = std.mem.Allocator;

const lattice = @import("lattice");

const types = lattice.core.types;
const NodeId = types.NodeId;
const EdgeId = types.EdgeId;
const PropertyValue = types.PropertyValue;

const node_mod = lattice.graph.node;
const NodeStore = node_mod.NodeStore;

const symbols_mod = lattice.graph.symbols;
const SymbolTable = symbols_mod.SymbolTable;

// ============================================================================
// Constants
// ============================================================================

/// Maximum number of variable slots per row
pub const MAX_SLOTS: u8 = 16;

// ============================================================================
// Errors
// ============================================================================

/// Errors that can occur during operator execution
pub const OperatorError = error{
    /// Operator was not opened before next() was called
    NotInitialized,
    /// Iterator has been exhausted
    IteratorExhausted,
    /// Underlying storage error
    StorageError,
    /// Out of memory
    OutOfMemory,
    /// Expression evaluation failed
    EvaluationError,
    /// Operator in invalid state
    InvalidState,
    /// Type mismatch during evaluation
    TypeError,
    /// Variable not bound
    UnboundVariable,
    /// Property not found
    PropertyNotFound,
};

// ============================================================================
// Slot Value
// ============================================================================

/// A value stored in a row slot
pub const SlotValue = union(enum) {
    /// Slot is empty/unset
    empty: void,
    /// Reference to a node (lazy - just the ID)
    node_ref: NodeId,
    /// Reference to an edge (lazy - just the ID)
    edge_ref: EdgeId,
    /// Scalar property value
    property: PropertyValue,

    const Self = @This();

    /// Check if this slot is empty
    pub fn isEmpty(self: Self) bool {
        return self == .empty;
    }

    /// Get as node ID if this is a node reference
    pub fn asNodeId(self: Self) ?NodeId {
        return switch (self) {
            .node_ref => |id| id,
            else => null,
        };
    }

    /// Get as edge ID if this is an edge reference
    pub fn asEdgeId(self: Self) ?EdgeId {
        return switch (self) {
            .edge_ref => |id| id,
            else => null,
        };
    }

    /// Get as property value if this is a property
    pub fn asProperty(self: Self) ?PropertyValue {
        return switch (self) {
            .property => |p| p,
            else => null,
        };
    }
};

// ============================================================================
// Row
// ============================================================================

/// A row/tuple flowing through the operator tree.
/// Contains variable bindings in fixed slots plus optional metadata.
pub const Row = struct {
    /// Variable slots indexed by slot number
    slots: [MAX_SLOTS]SlotValue,
    /// Vector distances for vector search results
    distances: [MAX_SLOTS]f32,
    /// FTS scores for full-text search results
    scores: [MAX_SLOTS]f32,
    /// Bitmask indicating which slots are populated
    populated: u16,

    const Self = @This();

    /// Create an empty row
    pub fn init() Self {
        return Self{
            .slots = [_]SlotValue{.{ .empty = {} }} ** MAX_SLOTS,
            .distances = [_]f32{0.0} ** MAX_SLOTS,
            .scores = [_]f32{0.0} ** MAX_SLOTS,
            .populated = 0,
        };
    }

    /// Set a slot value
    pub fn setSlot(self: *Self, slot: u8, value: SlotValue) void {
        std.debug.assert(slot < MAX_SLOTS);
        self.slots[slot] = value;
        self.populated |= (@as(u16, 1) << @intCast(slot));
    }

    /// Get a slot value, returns null if slot is not populated
    pub fn getSlot(self: *const Self, slot: u8) ?SlotValue {
        std.debug.assert(slot < MAX_SLOTS);
        if (!self.hasSlot(slot)) return null;
        const value = self.slots[slot];
        if (value == .empty) return null;
        return value;
    }

    /// Check if a slot is populated
    pub fn hasSlot(self: *const Self, slot: u8) bool {
        std.debug.assert(slot < MAX_SLOTS);
        return (self.populated & (@as(u16, 1) << @intCast(slot))) != 0;
    }

    /// Set the distance for a slot (from vector search)
    pub fn setDistance(self: *Self, slot: u8, distance: f32) void {
        std.debug.assert(slot < MAX_SLOTS);
        self.distances[slot] = distance;
    }

    /// Get the distance for a slot
    pub fn getDistance(self: *const Self, slot: u8) f32 {
        std.debug.assert(slot < MAX_SLOTS);
        return self.distances[slot];
    }

    /// Set the score for a slot (from FTS search)
    pub fn setScore(self: *Self, slot: u8, score: f32) void {
        std.debug.assert(slot < MAX_SLOTS);
        self.scores[slot] = score;
    }

    /// Get the score for a slot
    pub fn getScore(self: *const Self, slot: u8) f32 {
        std.debug.assert(slot < MAX_SLOTS);
        return self.scores[slot];
    }

    /// Clone the row (shallow copy - slot values are not deep copied)
    pub fn clone(self: *const Self) Self {
        return self.*;
    }

    /// Copy populated slots from another row
    pub fn copyFrom(self: *Self, other: *const Self) void {
        var i: u8 = 0;
        while (i < MAX_SLOTS) : (i += 1) {
            if (other.hasSlot(i)) {
                self.slots[i] = other.slots[i];
                self.distances[i] = other.distances[i];
                self.scores[i] = other.scores[i];
                self.populated |= (@as(u16, 1) << @intCast(i));
            }
        }
    }

    /// Clear all slots
    pub fn clear(self: *Self) void {
        self.slots = [_]SlotValue{.{ .empty = {} }} ** MAX_SLOTS;
        self.distances = [_]f32{0.0} ** MAX_SLOTS;
        self.scores = [_]f32{0.0} ** MAX_SLOTS;
        self.populated = 0;
    }
};

// ============================================================================
// Operator Interface
// ============================================================================

/// Physical operator interface using vtable pattern.
/// All operators implement open/next/close for pull-based iteration.
pub const Operator = struct {
    /// Virtual function table
    vtable: *const VTable,
    /// Opaque pointer to operator-specific state
    ptr: *anyopaque,

    pub const VTable = struct {
        /// Initialize the operator and acquire resources.
        /// Called once before any next() calls.
        open: *const fn (ptr: *anyopaque, ctx: *ExecutionContext) OperatorError!void,

        /// Get the next row, or null if exhausted.
        /// May be called repeatedly until null is returned.
        next: *const fn (ptr: *anyopaque, ctx: *ExecutionContext) OperatorError!?*Row,

        /// Release resources acquired in open().
        /// Called once when done with the operator.
        close: *const fn (ptr: *anyopaque, ctx: *ExecutionContext) void,

        /// Free the operator itself.
        /// Called to deallocate the operator struct.
        deinit: *const fn (ptr: *anyopaque, allocator: Allocator) void,
    };

    const Self = @This();

    /// Initialize the operator
    pub fn open(self: Self, ctx: *ExecutionContext) OperatorError!void {
        return self.vtable.open(self.ptr, ctx);
    }

    /// Get the next row
    pub fn next(self: Self, ctx: *ExecutionContext) OperatorError!?*Row {
        return self.vtable.next(self.ptr, ctx);
    }

    /// Close the operator
    pub fn close(self: Self, ctx: *ExecutionContext) void {
        self.vtable.close(self.ptr, ctx);
    }

    /// Free the operator
    pub fn deinit(self: Self, allocator: Allocator) void {
        self.vtable.deinit(self.ptr, allocator);
    }
};

// ============================================================================
// Execution Context
// ============================================================================

/// Runtime context for query execution.
/// Provides access to storage, parameters, and memory management.
pub const ExecutionContext = struct {
    /// Allocator for query lifetime allocations
    allocator: Allocator,

    /// Arena for row allocations (can be reset between queries)
    row_arena: std.heap.ArenaAllocator,

    /// Query parameters ($name -> value)
    parameters: std.StringHashMap(PropertyValue),

    /// Variable name to slot mapping
    variables: std.StringHashMap(u8),

    /// Node store for property lookups (optional)
    node_store: ?*NodeStore,

    /// Symbol table for property key resolution (optional)
    symbol_table: ?*SymbolTable,

    const Self = @This();

    /// Create a new execution context
    pub fn init(allocator: Allocator) Self {
        return Self{
            .allocator = allocator,
            .row_arena = std.heap.ArenaAllocator.init(allocator),
            .parameters = std.StringHashMap(PropertyValue).init(allocator),
            .variables = std.StringHashMap(u8).init(allocator),
            .node_store = null,
            .symbol_table = null,
        };
    }

    /// Create a new execution context with storage access
    pub fn initWithStorage(allocator: Allocator, node_store: *NodeStore, symbol_table: *SymbolTable) Self {
        return Self{
            .allocator = allocator,
            .row_arena = std.heap.ArenaAllocator.init(allocator),
            .parameters = std.StringHashMap(PropertyValue).init(allocator),
            .variables = std.StringHashMap(u8).init(allocator),
            .node_store = node_store,
            .symbol_table = symbol_table,
        };
    }

    /// Free the execution context
    pub fn deinit(self: *Self) void {
        self.row_arena.deinit();
        self.parameters.deinit();
        self.variables.deinit();
    }

    /// Allocate a row from the row arena
    pub fn allocRow(self: *Self) !*Row {
        const row = try self.row_arena.allocator().create(Row);
        row.* = Row.init();
        return row;
    }

    /// Reset the row arena (call between queries)
    pub fn resetRowArena(self: *Self) void {
        _ = self.row_arena.reset(.retain_capacity);
    }

    /// Set a query parameter
    pub fn setParameter(self: *Self, name: []const u8, value: PropertyValue) !void {
        try self.parameters.put(name, value);
    }

    /// Get a query parameter
    pub fn getParameter(self: *const Self, name: []const u8) ?PropertyValue {
        return self.parameters.get(name);
    }

    /// Register a variable binding
    pub fn registerVariable(self: *Self, name: []const u8, slot: u8) !void {
        try self.variables.put(name, slot);
    }

    /// Get the slot for a variable
    pub fn getVariableSlot(self: *const Self, name: []const u8) ?u8 {
        return self.variables.get(name);
    }
};

// ============================================================================
// Query Result
// ============================================================================

/// Result of executing a query
pub const QueryResult = struct {
    /// Column names
    columns: []const []const u8,
    /// Result rows
    rows: std.ArrayList(Row),
    /// Allocator used
    allocator: Allocator,

    const Self = @This();

    /// Create an empty result
    pub fn init(allocator: Allocator) Self {
        return Self{
            .columns = &[_][]const u8{},
            .rows = .empty,
            .allocator = allocator,
        };
    }

    /// Free the result
    pub fn deinit(self: *Self) void {
        self.rows.deinit(self.allocator);
    }

    /// Get number of rows
    pub fn rowCount(self: *const Self) usize {
        return self.rows.items.len;
    }

    /// Add a row to the result
    pub fn addRow(self: *Self, row: Row) !void {
        try self.rows.append(self.allocator, row);
    }
};

// ============================================================================
// Executor
// ============================================================================

/// Execute an operator tree and collect results
pub fn execute(
    allocator: Allocator,
    root: Operator,
    ctx: *ExecutionContext,
) !QueryResult {
    var result = QueryResult.init(allocator);
    errdefer result.deinit();

    // Open the operator tree
    try root.open(ctx);
    defer root.close(ctx);

    // Pull all rows
    while (try root.next(ctx)) |row| {
        try result.addRow(row.*);
    }

    return result;
}

// ============================================================================
// Tests
// ============================================================================

test "row init and slot operations" {
    var row = Row.init();

    // Initially empty
    try std.testing.expect(!row.hasSlot(0));
    try std.testing.expect(row.getSlot(0) == null);

    // Set a slot
    row.setSlot(0, .{ .node_ref = 42 });
    try std.testing.expect(row.hasSlot(0));

    const val = row.getSlot(0).?;
    try std.testing.expectEqual(@as(NodeId, 42), val.asNodeId().?);
}

test "row distances and scores" {
    var row = Row.init();

    row.setDistance(0, 0.5);
    row.setScore(0, 1.5);

    try std.testing.expectEqual(@as(f32, 0.5), row.getDistance(0));
    try std.testing.expectEqual(@as(f32, 1.5), row.getScore(0));
}

test "row clone" {
    var row = Row.init();
    row.setSlot(0, .{ .node_ref = 100 });
    row.setDistance(0, 0.25);

    const cloned = row.clone();
    try std.testing.expectEqual(@as(NodeId, 100), cloned.getSlot(0).?.asNodeId().?);
    try std.testing.expectEqual(@as(f32, 0.25), cloned.getDistance(0));
}

test "execution context parameters" {
    const allocator = std.testing.allocator;
    var ctx = ExecutionContext.init(allocator);
    defer ctx.deinit();

    try ctx.setParameter("name", .{ .string_val = "test" });

    const val = ctx.getParameter("name").?;
    try std.testing.expectEqualStrings("test", val.string_val);

    try std.testing.expect(ctx.getParameter("nonexistent") == null);
}

test "execution context variables" {
    const allocator = std.testing.allocator;
    var ctx = ExecutionContext.init(allocator);
    defer ctx.deinit();

    try ctx.registerVariable("n", 0);
    try ctx.registerVariable("r", 1);

    try std.testing.expectEqual(@as(u8, 0), ctx.getVariableSlot("n").?);
    try std.testing.expectEqual(@as(u8, 1), ctx.getVariableSlot("r").?);
    try std.testing.expect(ctx.getVariableSlot("x") == null);
}

test "execution context row allocation" {
    const allocator = std.testing.allocator;
    var ctx = ExecutionContext.init(allocator);
    defer ctx.deinit();

    const row = try ctx.allocRow();
    row.setSlot(0, .{ .node_ref = 1 });

    try std.testing.expectEqual(@as(NodeId, 1), row.getSlot(0).?.asNodeId().?);
}
