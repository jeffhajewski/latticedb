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

const ast = @import("../ast.zig");
const expression = @import("../expression.zig");

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
// PropertyIndexScan Operator
// ============================================================================

/// Scans an explicit label/property equality index. The value expression is
/// evaluated once at open time, allowing both literals and query parameters.
/// The nodes with `label` whose indexed `property` equals `value`, as
/// Cypher compares: 1 and 1.0 are equal.
///
/// The index keys a value by a digest of its type and bits, so an integer
/// and the float of the same number land under different keys. The other
/// form is looked up as well and the two lists merged. The result is owned
/// by `database.allocator`.
pub fn findNodesByIndexedValue(
    database: *Database,
    ctx: *const ExecutionContext,
    label: []const u8,
    property: []const u8,
    value: types.PropertyValue,
) OperatorError![]NodeId {
    const first = database.findNodesByLabelProperty(ctx.txn, label, property, value, std.math.maxInt(usize)) catch return OperatorError.StorageError;
    const other: types.PropertyValue = switch (value) {
        .int_val => |i| blk: {
            const f: f64 = @floatFromInt(i);
            // Only when the float is exactly this integer.
            if (f >= 0x1p63 or @as(i64, @intFromFloat(f)) != i) return first;
            break :blk .{ .float_val = f };
        },
        .float_val => |f| blk: {
            if (!std.math.isFinite(f) or @trunc(f) != f or f < -0x1p63 or f >= 0x1p63) return first;
            break :blk .{ .int_val = @intFromFloat(f) };
        },
        else => return first,
    };
    defer database.allocator.free(first);
    const second = database.findNodesByLabelProperty(ctx.txn, label, property, other, std.math.maxInt(usize)) catch return OperatorError.StorageError;
    defer database.allocator.free(second);

    const merged = database.allocator.alloc(NodeId, first.len + second.len) catch return OperatorError.OutOfMemory;
    @memcpy(merged[0..first.len], first);
    @memcpy(merged[first.len..], second);
    std.mem.sort(NodeId, merged, {}, std.sort.asc(NodeId));
    var kept: usize = 0;
    for (merged) |id| {
        if (kept > 0 and merged[kept - 1] == id) continue;
        merged[kept] = id;
        kept += 1;
    }
    if (kept == merged.len) return merged;
    // The caller frees the slice it gets, so it must be the allocation.
    defer database.allocator.free(merged);
    return database.allocator.dupe(NodeId, merged[0..kept]) catch OperatorError.OutOfMemory;
}

pub const PropertyIndexScan = struct {
    output_slot: u8,
    label: []const u8,
    property: []const u8,
    value_expression: *const ast.Expression,
    database: *Database,
    node_ids: ?[]NodeId,
    current_index: usize,
    current_row: ?*Row,

    const Self = @This();

    pub fn init(
        allocator: Allocator,
        output_slot: u8,
        label: []const u8,
        property: []const u8,
        value_expression: *const ast.Expression,
        database: *Database,
    ) !*Self {
        const self = try allocator.create(Self);
        self.* = .{
            .output_slot = output_slot,
            .label = label,
            .property = property,
            .value_expression = value_expression,
            .database = database,
            .node_ids = null,
            .current_index = 0,
            .current_row = null,
        };
        return self;
    }

    pub fn operator(self: *Self) Operator {
        return .{ .vtable = &vtable, .ptr = self };
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

        const temporary_allocator = ctx.row_arena.allocator();
        var evaluator = expression.ExpressionEvaluator.init(temporary_allocator);
        const empty_row = Row.init();
        const evaluated = evaluator.evaluate(self.value_expression, &empty_row, ctx) catch return OperatorError.EvaluationError;
        const value = evaluated.toPropertyValue(temporary_allocator);
        self.node_ids = try findNodesByIndexedValue(self.database, ctx, self.label, self.property, value);
        self.current_index = 0;
    }

    fn next(ptr: *anyopaque, _: *ExecutionContext) OperatorError!?*Row {
        const self: *Self = @ptrCast(@alignCast(ptr));
        const node_ids = self.node_ids orelse return OperatorError.NotInitialized;
        const row = self.current_row orelse return OperatorError.NotInitialized;
        if (self.current_index >= node_ids.len) return null;

        const node_id = node_ids[self.current_index];
        self.current_index += 1;
        row.clear();
        row.setSlot(self.output_slot, .{ .node_ref = node_id });
        return row;
    }

    fn close(ptr: *anyopaque, _: *ExecutionContext) void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        if (self.node_ids) |node_ids| {
            self.database.allocator.free(node_ids);
            self.node_ids = null;
        }
    }

    fn deinit(ptr: *anyopaque, allocator: Allocator) void {
        allocator.destroy(@as(*Self, @ptrCast(@alignCast(ptr))));
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

test "PropertyIndexScan basic structure" {
    try std.testing.expect(@TypeOf(PropertyIndexScan.vtable.open) != void);
}
