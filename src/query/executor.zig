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

const txn_mod = lattice.transaction.manager;
const Transaction = txn_mod.Transaction;

const database_mod = lattice.storage.database;
const Database = database_mod.Database;

const regex = @import("regex.zig");
const ast = @import("ast.zig");

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
    /// Operation not supported
    Unsupported,
    /// DELETE of a node that still has relationships (use DETACH DELETE)
    NodeHasRelationships,
};

/// What went wrong in a failed execution, in words, written where the
/// failure is found.
///
/// An error value says only which kind of thing failed: every evaluation
/// failure used to reach the user as "EvaluationError: query failed", the
/// same for a division by zero as for an unknown function. The code that
/// finds a failure knows the operator, the function and the values, so it
/// records them here, and the database reports this text through the same
/// path that carries parse errors.
///
/// Nothing is formatted until something fails. The first failure recorded
/// wins: errors propagate outwards, and the innermost one is the cause.
pub const Diagnostic = struct {
    pub const Kind = enum {
        /// Something about the query or its data: a type error, an overflow.
        query,
        /// Reading or writing the database failed.
        storage,
        out_of_memory,
    };

    buf: [MESSAGE_CAPACITY]u8 = undefined,
    len: usize = 0,
    recorded: bool = false,
    kind: Kind = .query,
    /// A short name for the failure, such as "DivisionByZero".
    code: []const u8 = "",
    /// Where in the query text, when the failing expression is known.
    location: ?ast.SourceLocation = null,

    pub const MESSAGE_CAPACITY = 384;

    pub fn record(
        self: *Diagnostic,
        kind: Kind,
        code: []const u8,
        location: ?ast.SourceLocation,
        comptime fmt: []const u8,
        args: anytype,
    ) void {
        @branchHint(.cold);
        if (self.recorded) return;
        self.recorded = true;
        self.kind = kind;
        self.code = code;
        self.location = location;
        var writer: std.Io.Writer = .fixed(&self.buf);
        writer.print(fmt, args) catch {
            // Keep what fitted and say it was cut.
            const ellipsis = "...";
            self.len = self.buf.len;
            @memcpy(self.buf[self.buf.len - ellipsis.len ..], ellipsis);
            return;
        };
        self.len = writer.end;
    }

    pub fn message(self: *const Diagnostic) ?[]const u8 {
        if (!self.recorded or self.len == 0) return null;
        return self.buf[0..self.len];
    }
};

/// The kind of failure an error value stands for, for errors recorded
/// without a more specific kind.
pub fn diagnosticKind(err: anyerror) Diagnostic.Kind {
    return switch (err) {
        error.StorageError => .storage,
        error.OutOfMemory => .out_of_memory,
        else => .query,
    };
}

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

    /// Whether the slot holds null, as a variable that OPTIONAL MATCH did
    /// not bind does.
    pub fn isNull(self: Self) bool {
        return self == .property and self.property == .null_val;
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
    /// Slots whose distance a vector search set, and whose score a full-text
    /// search set, for this row's entity. A slot given a new value loses
    /// both, so a number never outlives the entity it was measured for.
    measured: u16 = 0,
    scored: u16 = 0,

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
        const bit = @as(u16, 1) << @intCast(slot);
        self.populated |= bit;
        self.measured &= ~bit;
        self.scored &= ~bit;
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
        self.measured |= @as(u16, 1) << @intCast(slot);
    }

    /// The distance a vector search measured for the entity in `slot`, or
    /// null when no search did.
    pub fn searchDistance(self: *const Self, slot: u8) ?f32 {
        std.debug.assert(slot < MAX_SLOTS);
        if (self.measured & (@as(u16, 1) << @intCast(slot)) == 0) return null;
        return self.distances[slot];
    }

    /// The score a full-text search gave the entity in `slot`, or null when
    /// no search did.
    pub fn searchScore(self: *const Self, slot: u8) ?f32 {
        std.debug.assert(slot < MAX_SLOTS);
        if (self.scored & (@as(u16, 1) << @intCast(slot)) == 0) return null;
        return self.scores[slot];
    }

    /// Carry `slot`'s search distance and score over from `other`, where
    /// the slot holds the same entity in both.
    pub fn copySearchMarks(self: *Self, other: *const Self, slot: u8) void {
        const bit = @as(u16, 1) << @intCast(slot);
        self.distances[slot] = other.distances[slot];
        self.scores[slot] = other.scores[slot];
        self.measured = (self.measured & ~bit) | (other.measured & bit);
        self.scored = (self.scored & ~bit) | (other.scored & bit);
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
        self.scored |= @as(u16, 1) << @intCast(slot);
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
        self.measured = (self.measured & ~other.populated) | other.measured;
        self.scored = (self.scored & ~other.populated) | other.scored;
    }

    /// Clear all slots
    pub fn clear(self: *Self) void {
        self.slots = [_]SlotValue{.{ .empty = {} }} ** MAX_SLOTS;
        self.distances = [_]f32{0.0} ** MAX_SLOTS;
        self.scores = [_]f32{0.0} ** MAX_SLOTS;
        self.populated = 0;
        self.measured = 0;
        self.scored = 0;
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

/// Results of one full-text search, held for the length of a query.
///
/// A `@@` the planner could not turn into an access path is evaluated per row,
/// and every row asks the same index the same question. Without this, a term
/// most documents contain costs one index search per row, which is quadratic in
/// the corpus: measured at eight thousand documents, twenty-one seconds against
/// a hundred microseconds for the same query the planner could seek.
///
/// The engine already treats a search as stable for the length of an operator —
/// the scanning operators search once when they open and use that result for
/// every row they emit. This is the same assumption at the same granularity,
/// which is why it is safe to reuse an answer within one execution.
pub const FtsFilterCache = struct {
    allocator: Allocator,
    entries: std.StringHashMapUnmanaged(std.AutoHashMapUnmanaged(u64, void)) = .{},

    pub fn init(allocator: Allocator) FtsFilterCache {
        return .{ .allocator = allocator };
    }

    pub fn deinit(self: *FtsFilterCache) void {
        var iter = self.entries.iterator();
        while (iter.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            entry.value_ptr.deinit(self.allocator);
        }
        self.entries.deinit(self.allocator);
    }

    /// The document ids for a search, or null when it has not been run yet.
    pub fn get(self: *FtsFilterCache, key: []const u8) ?*std.AutoHashMapUnmanaged(u64, void) {
        return self.entries.getPtr(key);
    }

    /// Remember a search's document ids. Takes ownership of `ids`.
    pub fn put(
        self: *FtsFilterCache,
        key: []const u8,
        ids: std.AutoHashMapUnmanaged(u64, void),
    ) !void {
        const owned_key = try self.allocator.dupe(u8, key);
        errdefer self.allocator.free(owned_key);
        try self.entries.put(self.allocator, owned_key, ids);
    }
};

/// Runtime context for query execution.
/// Provides access to storage, parameters, and memory management.
/// What a planned vector search compared its entities with.
pub const VectorQuery = struct {
    property: []const u8,
    param_name: ?[]const u8 = null,
    literal: ?[]const f32 = null,
};

/// The vector search, if any, behind each slot. Only the slots in `mask`
/// hold one and the rest of `items` is undefined, so a query without a
/// vector search pays nothing to set this up.
pub const VectorQueries = struct {
    mask: u16 = 0,
    items: [MAX_SLOTS]VectorQuery = undefined,

    pub fn get(self: *const VectorQueries, slot: u8) ?VectorQuery {
        if (self.mask & (@as(u16, 1) << @intCast(slot)) == 0) return null;
        return self.items[slot];
    }

    pub fn set(self: *VectorQueries, slot: u8, query: VectorQuery) void {
        self.items[slot] = query;
        self.mask |= @as(u16, 1) << @intCast(slot);
    }
};

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

    /// Database reference for mutation operators (optional)
    database: ?*Database,

    /// Active transaction for txn-aware reads and writes (optional)
    txn: ?*Transaction,

    /// Full-text searches already answered during this execution.
    ///
    /// A pointer, so a filter holding a `*const ExecutionContext` can still
    /// record what it looked up. Null until the first search needs it.
    fts_cache: ?*FtsFilterCache = null,

    /// Patterns `=~` has compiled during this execution. A pointer for the
    /// same reason as `fts_cache`; null when it could not be allocated, and
    /// then each match compiles its pattern.
    regex_cache: ?*regex.Cache = null,

    /// Whether nothing writes to the stores while this query runs, so a
    /// node record read once can be read again from memory. False unless
    /// the caller knows the query only reads.
    stable_reads: bool = false,

    /// The vector search, if any, that put each slot's entity in the row:
    /// which property it searched and with what query. `x.prop <=> query`
    /// written the same way returns the distance that search measured
    /// rather than computing it again.
    /// The planner's, borrowed for the execution; null when no planner
    /// set it.
    vector_queries: ?*const VectorQueries = null,

    /// Where a failure is described for the caller, or null when nobody
    /// will read it. A pointer, so code holding a `*const ExecutionContext`
    /// can still write to it.
    diagnostic: ?*Diagnostic = null,

    const Self = @This();

    /// Record why the query failed and return `err`, for
    /// `return ctx.fail(error.X, location, "...", .{...})`. The message is
    /// formatted only here, on the way out.
    pub fn fail(
        self: *const Self,
        err: anytype,
        location: ?ast.SourceLocation,
        comptime fmt: []const u8,
        args: anytype,
    ) @TypeOf(err) {
        @branchHint(.cold);
        if (self.diagnostic) |d| d.record(diagnosticKind(err), @errorName(err), location, fmt, args);
        return err;
    }

    /// Whether a failure has already been described, so an outer layer
    /// does not replace the cause with its own, vaguer, account.
    pub fn failureRecorded(self: *const Self) bool {
        const d = self.diagnostic orelse return true;
        return d.recorded;
    }

    /// A failed database call as an operator error, recorded with what was
    /// being done, so that a failure of the storage underneath cannot be
    /// mistaken for a fault in the query, or the other way round.
    ///
    /// Some database errors are the caller's doing: a value too large to
    /// store, a write in a read-only transaction. Those are reported as
    /// query errors under their own names; the rest are storage errors.
    pub fn storageFailure(
        self: *const Self,
        err: anyerror,
        comptime what: []const u8,
        args: anytype,
    ) OperatorError {
        @branchHint(.cold);
        if (err == error.OutOfMemory) {
            if (self.diagnostic) |d| d.record(.out_of_memory, "OutOfMemory", null, "Out of memory while " ++ what, args);
            return OperatorError.OutOfMemory;
        }
        if (isCallerError(err)) {
            if (self.diagnostic) |d| d.record(.query, @errorName(err), null, "Cannot complete " ++ what ++ ": {s}", args ++ .{callerErrorText(err)});
            return OperatorError.InvalidState;
        }
        if (self.diagnostic) |d| d.record(.storage, "StorageError", null, "Storage error while " ++ what ++ ": {s}", args ++ .{@errorName(err)});
        return OperatorError.StorageError;
    }

    fn isCallerError(err: anyerror) bool {
        return switch (err) {
            error.ValueTooLarge,
            error.TransactionReadOnly,
            error.ReadOnly,
            error.PermissionDenied,
            error.TransactionNotActive,
            error.TransactionConflict,
            error.InvalidArgument,
            error.MissingIndex,
            error.NotFound,
            error.NodeHasRelationships,
            => true,
            else => false,
        };
    }

    fn callerErrorText(err: anyerror) []const u8 {
        return switch (err) {
            error.ValueTooLarge => "the value is too large to store",
            error.TransactionReadOnly => "the transaction is read-only",
            error.ReadOnly, error.PermissionDenied => "the database is open read-only",
            error.TransactionNotActive => "the transaction is no longer active",
            error.TransactionConflict => "another transaction conflicts with this one",
            error.InvalidArgument => "an argument is not valid",
            error.MissingIndex => "the index does not exist",
            error.NotFound => "a node or relationship it needs does not exist, or was deleted earlier in the query",
            error.NodeHasRelationships => "the node still has relationships; delete them first, or use DETACH DELETE",
            else => @errorName(err),
        };
    }

    /// Create a new execution context
    /// Somewhere to remember full-text searches for the length of a query.
    ///
    /// Null when it could not be allocated, which costs speed and nothing else:
    /// the filter searches every time instead of once.
    fn newFtsCache(allocator: Allocator) ?*FtsFilterCache {
        const cache = allocator.create(FtsFilterCache) catch return null;
        cache.* = FtsFilterCache.init(allocator);
        return cache;
    }

    fn newRegexCache(allocator: Allocator) ?*regex.Cache {
        const cache = allocator.create(regex.Cache) catch return null;
        cache.* = regex.Cache.init(allocator);
        return cache;
    }

    pub fn init(allocator: Allocator) Self {
        return Self{
            .allocator = allocator,
            .row_arena = std.heap.ArenaAllocator.init(allocator),
            .parameters = std.StringHashMap(PropertyValue).init(allocator),
            .variables = std.StringHashMap(u8).init(allocator),
            .node_store = null,
            .symbol_table = null,
            .database = null,
            .txn = null,
            .fts_cache = newFtsCache(allocator),
            .regex_cache = newRegexCache(allocator),
            .diagnostic = null,
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
            .database = null,
            .txn = null,
            .fts_cache = newFtsCache(allocator),
            .regex_cache = newRegexCache(allocator),
        };
    }

    /// Free the execution context
    pub fn deinit(self: *Self) void {
        if (self.fts_cache) |cache| {
            cache.deinit();
            self.allocator.destroy(cache);
            self.fts_cache = null;
        }
        if (self.regex_cache) |cache| {
            cache.deinit();
            self.allocator.destroy(cache);
            self.regex_cache = null;
        }
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
