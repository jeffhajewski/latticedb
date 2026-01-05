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

const btree = @import("../../storage/btree.zig");
const BTree = btree.BTree;

const label_index = @import("../../graph/label_index.zig");
const LabelIndex = label_index.LabelIndex;
const LabelIndexError = label_index.LabelIndexError;

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
    /// The B+Tree containing node data
    tree: *BTree,
    /// Current iterator (set during open)
    iterator: ?BTree.Iterator,
    /// Current row being returned
    current_row: ?*Row,

    const Self = @This();

    /// Create a new AllNodesScan operator
    pub fn init(allocator: Allocator, output_slot: u8, tree: *BTree) !*Self {
        const self = try allocator.create(Self);
        self.* = Self{
            .output_slot = output_slot,
            .tree = tree,
            .iterator = null,
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

        // Create iterator for all nodes (no start or end key)
        self.iterator = self.tree.range(null, null) catch return OperatorError.StorageError;
    }

    fn next(ptr: *anyopaque, _: *ExecutionContext) OperatorError!?*Row {
        const self: *Self = @ptrCast(@alignCast(ptr));

        const iter = &(self.iterator orelse return OperatorError.NotInitialized);
        const row = self.current_row orelse return OperatorError.NotInitialized;

        // Get next entry from iterator
        const entry = iter.next() catch return OperatorError.StorageError;

        if (entry) |e| {
            // Key is node_id as little-endian u64
            if (e.key.len < 8) return OperatorError.StorageError;
            const node_id = std.mem.readInt(u64, e.key[0..8], .little);

            // Set the node reference in the output slot
            row.clear();
            row.setSlot(self.output_slot, .{ .node_ref = node_id });

            return row;
        }

        return null;
    }

    fn close(ptr: *anyopaque, _: *ExecutionContext) void {
        const self: *Self = @ptrCast(@alignCast(ptr));

        if (self.iterator) |*iter| {
            iter.deinit();
            self.iterator = null;
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
    /// The label index
    index: *LabelIndex,
    /// Current iterator (set during open)
    iterator: ?LabelIndex.NodeIterator,
    /// Current row being returned
    current_row: ?*Row,

    const Self = @This();

    /// Create a new LabelScan operator
    pub fn init(allocator: Allocator, output_slot: u8, label_id: SymbolId, index: *LabelIndex) !*Self {
        const self = try allocator.create(Self);
        self.* = Self{
            .output_slot = output_slot,
            .label_id = label_id,
            .index = index,
            .iterator = null,
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

        // Create iterator for nodes with this label
        self.iterator = self.index.iterNodesByLabel(self.label_id) catch |err| {
            return mapLabelIndexError(err);
        };
    }

    fn next(ptr: *anyopaque, _: *ExecutionContext) OperatorError!?*Row {
        const self: *Self = @ptrCast(@alignCast(ptr));

        var iter = &(self.iterator orelse return OperatorError.NotInitialized);
        const row = self.current_row orelse return OperatorError.NotInitialized;

        // Get next node ID from iterator
        const node_id_opt = iter.next() catch return OperatorError.StorageError;

        if (node_id_opt) |node_id| {
            // Set the node reference in the output slot
            row.clear();
            row.setSlot(self.output_slot, .{ .node_ref = node_id });

            return row;
        }

        return null;
    }

    fn close(ptr: *anyopaque, _: *ExecutionContext) void {
        const self: *Self = @ptrCast(@alignCast(ptr));

        if (self.iterator) |*iter| {
            iter.deinit();
            self.iterator = null;
        }
    }

    fn deinit(ptr: *anyopaque, allocator: Allocator) void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        allocator.destroy(self);
    }
};

/// Map LabelIndex errors to OperatorError
fn mapLabelIndexError(err: LabelIndexError) OperatorError {
    return switch (err) {
        LabelIndexError.OutOfMemory => OperatorError.OutOfMemory,
        else => OperatorError.StorageError,
    };
}

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
