//! Scan operators for query execution.
//!
//! Provides operators for scanning nodes:
//! - AllNodesScan: Scans all nodes in the graph
//! - LabelScan: Scans nodes with a specific label

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

const database_mod = @import("../../storage/database.zig");
const Database = database_mod.Database;

const symbols = @import("../../graph/symbols.zig");
const SymbolId = symbols.SymbolId;

// ============================================================================
// AllNodesScan Operator
// ============================================================================

/// Scans all nodes in the graph using a B+Tree iterator.
/// Produces one row per node with the node ID in the output slot.
pub const AllNodesScan = struct {
    /// The slot to output node IDs to
    output_slot: u8,
    /// Database reference for txn-aware scans
    database: *Database,
    /// Materialized node ids visible in the current txn
    node_ids: ?[]NodeId,
    /// Current result index
    current_index: usize,
    /// Current row being returned
    current_row: ?*Row,

    const Self = @This();

    /// Create a new AllNodesScan operator
    pub fn init(allocator: Allocator, output_slot: u8, database: *Database) !*Self {
        const self = try allocator.create(Self);
        self.* = Self{
            .output_slot = output_slot,
            .database = database,
            .node_ids = null,
            .current_index = 0,
            .current_row = null,
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

        // Allocate row for reuse
        self.current_row = ctx.allocRow() catch return OperatorError.OutOfMemory;

        self.node_ids = self.database.getAllNodeIdsInTxn(ctx.txn) catch return OperatorError.StorageError;
        self.current_index = 0;
    }

    fn next(ptr: *anyopaque, _: *ExecutionContext) OperatorError!?*Row {
        const self: *Self = @ptrCast(@alignCast(ptr));

        const node_ids = self.node_ids orelse return OperatorError.NotInitialized;
        const row = self.current_row orelse return OperatorError.NotInitialized;

        if (self.current_index < node_ids.len) {
            const node_id = node_ids[self.current_index];
            self.current_index += 1;
            row.clear();
            row.setSlot(self.output_slot, .{ .node_ref = node_id });
            return row;
        }

        return null;
    }

    fn close(ptr: *anyopaque, _: *ExecutionContext) void {
        const self: *Self = @ptrCast(@alignCast(ptr));

        if (self.node_ids) |node_ids| {
            self.database.allocator.free(node_ids);
            self.node_ids = null;
        }
    }

    fn deinit(ptr: *anyopaque, allocator: Allocator) void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        allocator.destroy(self);
    }
};

// ============================================================================
// LabelScan Operator
// ============================================================================

/// Scans nodes with a specific label using the LabelIndex.
/// Produces one row per node with the node ID in the output slot.
pub const LabelScan = struct {
    /// The slot to output node IDs to
    output_slot: u8,
    /// The label ID to scan for
    label_id: SymbolId,
    /// Database reference for txn-aware scans
    database: *Database,
    /// Materialized node ids visible in the current txn
    node_ids: ?[]NodeId,
    /// Current result index
    current_index: usize,
    /// Current row being returned
    current_row: ?*Row,

    const Self = @This();

    /// Create a new LabelScan operator
    pub fn init(allocator: Allocator, output_slot: u8, label_id: SymbolId, database: *Database) !*Self {
        const self = try allocator.create(Self);
        self.* = Self{
            .output_slot = output_slot,
            .label_id = label_id,
            .database = database,
            .node_ids = null,
            .current_index = 0,
            .current_row = null,
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

        // Allocate row for reuse
        self.current_row = ctx.allocRow() catch return OperatorError.OutOfMemory;

        self.node_ids = self.database.getNodesByLabelIdInTxn(ctx.txn, self.label_id) catch return OperatorError.StorageError;
        self.current_index = 0;
    }

    fn next(ptr: *anyopaque, _: *ExecutionContext) OperatorError!?*Row {
        const self: *Self = @ptrCast(@alignCast(ptr));

        const node_ids = self.node_ids orelse return OperatorError.NotInitialized;
        const row = self.current_row orelse return OperatorError.NotInitialized;

        if (self.current_index < node_ids.len) {
            const node_id = node_ids[self.current_index];
            self.current_index += 1;
            // Set the node reference in the output slot
            row.clear();
            row.setSlot(self.output_slot, .{ .node_ref = node_id });

            return row;
        }

        return null;
    }

    fn close(ptr: *anyopaque, _: *ExecutionContext) void {
        const self: *Self = @ptrCast(@alignCast(ptr));

        if (self.node_ids) |node_ids| {
            self.database.allocator.free(node_ids);
            self.node_ids = null;
        }
    }

    fn deinit(ptr: *anyopaque, allocator: Allocator) void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        allocator.destroy(self);
    }
};

// ============================================================================
// Tests
// ============================================================================

test "AllNodesScan basic structure" {
    // Verify vtable is properly structured
    const vtable = AllNodesScan.vtable;
    try std.testing.expect(@TypeOf(vtable.open) != void);
    try std.testing.expect(@TypeOf(vtable.next) != void);
    try std.testing.expect(@TypeOf(vtable.close) != void);
    try std.testing.expect(@TypeOf(vtable.deinit) != void);
}

test "LabelScan basic structure" {
    // Verify vtable is properly structured
    const vtable = LabelScan.vtable;
    try std.testing.expect(@TypeOf(vtable.open) != void);
    try std.testing.expect(@TypeOf(vtable.next) != void);
    try std.testing.expect(@TypeOf(vtable.close) != void);
    try std.testing.expect(@TypeOf(vtable.deinit) != void);
}
