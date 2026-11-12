//! Range scans and ordered reads over an ordered property index.
//!
//! - IndexRangeScan: the nodes with a label whose indexed property lies in a
//!   range (`<`, `<=`, `>`, `>=`, `STARTS WITH`, any conjunction of them on
//!   the property), or, with no range, every node with the label in the order
//!   of the property: the indexed ones by value, then the ones without it.
//! - OrderedCutoff: under a sort by that property with a LIMIT, stops
//!   reading once no further row can be among the first k.

const std = @import("std");
const Allocator = std.mem.Allocator;

const executor = @import("../executor.zig");
const Operator = executor.Operator;
const OperatorError = executor.OperatorError;
const Row = executor.Row;
const ExecutionContext = executor.ExecutionContext;

const types = @import("../../core/types.zig");
const NodeId = types.NodeId;
const PropertyValue = types.PropertyValue;

const database_mod = @import("../../storage/database.zig");
const Database = database_mod.Database;

const symbols = @import("../../graph/symbols.zig");
const SymbolId = symbols.SymbolId;

const property_index = @import("../../graph/property_index.zig");
const ValueRange = property_index.ValueRange;
pub const RangeOp = property_index.RangeOp;
const OrderedTag = property_index.OrderedTag;

const ast = @import("../ast.zig");
const expression = @import("../expression.zig");
const ExpressionEvaluator = expression.ExpressionEvaluator;
const EvalError = expression.EvalError;

const scan_ops = @import("scan.zig");
const limit_ops = @import("limit.zig");

/// One conjunct of the WHERE that the scan answers: `property <op> value`.
pub const Condition = struct {
    op: RangeOp,
    /// Independent of the row: a literal or a parameter.
    value: *const ast.Expression,
    /// The conjunct as written, evaluated against a row when the index
    /// cannot settle it alone.
    predicate: *const ast.Expression,
};

/// The nodes with `label` whose `property` satisfies every condition, read
/// from the label's ordered index on the property, in index order: by value,
/// and by id among equal values.
///
/// The conditions leave the WHERE: the scan answers them. Where the index
/// cannot tell (a string bound longer than the part of a value a key holds,
/// a NaN bound, NaN values under a lower bound only) it evaluates the
/// conditions on the row itself.
///
/// With no conditions it returns every node with the label, the ones with
/// the property first, in order, then the ones without it in id order, which
/// is where ascending ORDER BY puts nulls.
///
/// A query that only reads streams the index `CHUNK` keys at a time. One
/// that writes takes every id when it opens, as LabelScan does, so that its
/// own writes cannot change which nodes the scan visits.
pub const IndexRangeScan = struct {
    allocator: Allocator,
    output_slot: u8,
    label: []const u8,
    property: []const u8,
    conditions: []const Condition,
    database: *Database,
    evaluator: ExpressionEvaluator,

    current_row: ?*Row = null,
    state: ?State = null,
    /// Every id, in order (queries that write).
    ids: ?[]NodeId = null,
    ids_pos: usize = 0,

    pub const CHUNK = 256;

    const State = struct {
        definition: property_index.Definition,
        label_id: SymbolId,
        range: ValueRange,
        /// Full keys bounding the scan; `start` moves past each chunk.
        start: [property_index.ORDERED_KEY_MAX + 1]u8 = undefined,
        start_len: usize = 0,
        end: [property_index.ORDERED_KEY_MAX + 1]u8 = undefined,
        end_len: usize = 0,
        keys: std.ArrayList(u8) = .empty,
        ends: std.ArrayList(u32) = .empty,
        chunk_pos: usize = 0,
        exhausted: bool = false,
        /// NaN's keys, read after the range when a condition compares
        /// with a number and the range does not hold them.
        nan_segment: bool = false,
        /// NaN's keys are still to be read.
        nan_pending: bool = false,
        /// Nodes the transaction's overlay holds that fall in the range,
        /// sorted by key. The keys are slices of `overlay_bytes`.
        overlay: std.ArrayList(OverlayEntry) = .empty,
        overlay_bytes: std.ArrayList(u8) = .empty,
        overlay_pos: usize = 0,
        /// The next committed entry, read but not yet emitted.
        pending: ?Entry = null,
        /// After the index, the nodes without the property (no conditions).
        missing: ?*scan_ops.LabelScan = null,
        missing_open: bool = false,
        done_index: bool = false,

        fn deinit(self: *State, allocator: Allocator, ctx: ?*ExecutionContext) void {
            self.keys.deinit(allocator);
            self.ends.deinit(allocator);
            self.overlay.deinit(allocator);
            self.overlay_bytes.deinit(allocator);
            if (self.missing) |scan| {
                const op = scan.operator();
                if (self.missing_open) {
                    if (ctx) |c| op.close(c);
                }
                op.deinit(allocator);
                self.missing = null;
            }
        }
    };

    const OverlayEntry = struct { from: u32, to: u32, id: NodeId };

    const Entry = struct {
        key: []const u8,
        id: NodeId,
    };

    const Self = @This();

    pub fn init(
        allocator: Allocator,
        output_slot: u8,
        label: []const u8,
        property: []const u8,
        conditions: []const Condition,
        database: *Database,
    ) !*Self {
        const self = try allocator.create(Self);
        self.* = .{
            .allocator = allocator,
            .output_slot = output_slot,
            .label = label,
            .property = property,
            .conditions = conditions,
            .database = database,
            .evaluator = ExpressionEvaluator.init(allocator),
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
        self.release(ctx);
        self.current_row = ctx.allocRow() catch return OperatorError.OutOfMemory;

        const definition = (self.database.nodePropertyIndex(self.label, self.property) catch |err|
            return ctx.storageFailure(err, "reading the property index on {s}.{s}", .{ self.label, self.property })) orelse {
            // Dropped since the plan was made.
            return ctx.fail(OperatorError.StorageError, null, "The property index on {s}.{s} is gone", .{ self.label, self.property });
        };
        if (definition.encoding != .ordered) {
            return ctx.fail(OperatorError.StorageError, null, "The property index on {s}.{s} cannot answer ranges", .{ self.label, self.property });
        }

        var range = ValueRange.all();
        for (self.conditions) |condition| {
            const value = try self.evaluateIndependent(condition.value, ctx);
            range.constrain(condition.op, value);
        }

        self.state = .{ .definition = definition, .label_id = definition.scope_id, .range = range };
        const state = &self.state.?;
        state.nan_segment = range.nan_check and !range.holdsNan();
        state.nan_pending = state.nan_segment;
        if (range.empty) {
            state.exhausted = true;
            if (state.nan_pending) {
                setNanSegment(state);
            } else {
                state.done_index = true;
            }
        } else {
            setBounds(state, range.startBytes(), range.endBytes());
        }
        if (!state.done_index) try self.collectOverlay(state, ctx);

        if (self.conditions.len == 0) {
            state.missing = scan_ops.LabelScan.init(self.allocator, self.output_slot, state.label_id, self.database) catch
                return OperatorError.OutOfMemory;
        }

        if (!ctx.stable_reads) {
            var ids: std.ArrayList(NodeId) = .empty;
            errdefer ids.deinit(self.allocator);
            while (try self.nextId(ctx)) |id| ids.append(self.allocator, id) catch return OperatorError.OutOfMemory;
            self.ids = ids.toOwnedSlice(self.allocator) catch return OperatorError.OutOfMemory;
            self.ids_pos = 0;
        }
    }

    fn setBounds(state: *State, start_value: []const u8, end_value: []const u8) void {
        var buf: property_index.EntryKeyBuffer = undefined;
        const start = property_index.orderedPrefix(state.definition, start_value, &buf);
        @memcpy(state.start[0..start.len], start);
        state.start_len = start.len;
        const end = property_index.orderedPrefix(state.definition, end_value, &buf);
        @memcpy(state.end[0..end.len], end);
        state.end_len = end.len;
    }

    /// Move on to NaN's keys, after the range.
    fn setNanSegment(state: *State) void {
        setBounds(state, &.{OrderedTag.nan}, &.{OrderedTag.nan + 1});
        state.nan_pending = false;
        state.exhausted = false;
        state.keys.clearRetainingCapacity();
        state.ends.clearRetainingCapacity();
        state.chunk_pos = 0;
    }

    fn inRange(state: *const State, key: []const u8) bool {
        const value = property_index.orderedValueOf(key);
        if (state.nan_segment and value[0] == OrderedTag.nan) return true;
        if (state.range.empty) return false;
        return std.mem.order(u8, value, state.range.startBytes()) != .lt and
            std.mem.order(u8, value, state.range.endBytes()) == .lt;
    }

    fn evaluateIndependent(self: *Self, expr: *const ast.Expression, ctx: *ExecutionContext) OperatorError!PropertyValue {
        const empty_row = Row.init();
        const result = self.evaluator.evaluate(expr, &empty_row, ctx) catch |err| return mapEvalError(err);
        return result.toPropertyValue(ctx.row_arena.allocator()) catch OperatorError.OutOfMemory;
    }

    /// The overlay's nodes with the label whose value falls in the range.
    fn collectOverlay(self: *Self, state: *State, ctx: *ExecutionContext) OperatorError!void {
        const ids = self.database.overlayNodesWithLabel(ctx.txn, state.label_id) catch |err|
            return ctx.storageFailure(err, "reading the transaction's :{s} nodes", .{self.label});
        defer self.database.allocator.free(ids);
        if (ids.len == 0) return;

        for (ids) |id| {
            var value = (self.database.getNodePropertyByKeyInTxn(ctx.txn, id, state.definition.property_id, self.allocator, null) catch |err|
                return ctx.storageFailure(err, "reading {s} of node {d}", .{ self.property, id })) orelse continue;
            defer value.deinit(self.allocator);
            var buf: property_index.EntryKeyBuffer = undefined;
            const key = property_index.entryKeyFor(state.definition, value, id, &buf) orelse continue;
            if (!inRange(state, key)) continue;
            const from: u32 = @intCast(state.overlay_bytes.items.len);
            state.overlay_bytes.appendSlice(self.allocator, key) catch return OperatorError.OutOfMemory;
            state.overlay.append(self.allocator, .{ .from = from, .to = @intCast(state.overlay_bytes.items.len), .id = id }) catch
                return OperatorError.OutOfMemory;
        }
        const Sorter = struct {
            fn lessThan(bytes: []const u8, a: OverlayEntry, b: OverlayEntry) bool {
                return std.mem.order(u8, bytes[a.from..a.to], bytes[b.from..b.to]) == .lt;
            }
        };
        std.mem.sort(OverlayEntry, state.overlay.items, @as([]const u8, state.overlay_bytes.items), Sorter.lessThan);
    }

    /// The next committed entry the overlay does not hold.
    fn nextCommitted(self: *Self, state: *State, ctx: *ExecutionContext) OperatorError!?Entry {
        while (true) {
            if (state.chunk_pos == state.ends.items.len) {
                if (state.exhausted) {
                    if (!state.nan_pending) return null;
                    setNanSegment(state);
                }
                // Start after the last key read: it followed by a zero byte
                // is the least key above it.
                if (state.ends.items.len > 0) {
                    const last = state.keys.items[self.keyFrom(state, state.ends.items.len - 1)..];
                    @memcpy(state.start[0..last.len], last);
                    state.start[last.len] = 0;
                    state.start_len = last.len + 1;
                }
                state.keys.clearRetainingCapacity();
                state.ends.clearRetainingCapacity();
                state.chunk_pos = 0;
                var index = &(self.database.property_index orelse
                    return ctx.fail(OperatorError.StorageError, null, "The property index on {s}.{s} is gone", .{ self.label, self.property }));
                state.exhausted = index.readRange(
                    .node,
                    state.start[0..state.start_len],
                    state.end[0..state.end_len],
                    CHUNK,
                    self.allocator,
                    &state.keys,
                    &state.ends,
                ) catch |err| return ctx.storageFailure(err, "reading the property index on {s}.{s}", .{ self.label, self.property });
                if (state.ends.items.len == 0) continue;
            }
            const from = self.keyFrom(state, state.chunk_pos);
            const key = state.keys.items[from..state.ends.items[state.chunk_pos]];
            state.chunk_pos += 1;
            const id = property_index.entityIdOf(key);
            // A node the overlay holds is in `overlay` if it belongs.
            if (self.database.overlayNodeHasLabel(ctx.txn, id, state.label_id) != null) continue;
            return .{ .key = key, .id = id };
        }
    }

    fn keyFrom(_: *Self, state: *const State, i: usize) usize {
        return if (i == 0) 0 else state.ends.items[i - 1];
    }

    /// The next entry of the index part, committed and overlay merged.
    fn nextIndexed(self: *Self, state: *State, ctx: *ExecutionContext) OperatorError!?Entry {
        if (state.pending == null) state.pending = try self.nextCommitted(state, ctx);
        const overlay: ?Entry = if (state.overlay_pos < state.overlay.items.len) blk: {
            const e = state.overlay.items[state.overlay_pos];
            break :blk .{ .key = state.overlay_bytes.items[e.from..e.to], .id = e.id };
        } else null;
        if (overlay) |o| {
            if (state.pending == null or std.mem.order(u8, o.key, state.pending.?.key) == .lt) {
                state.overlay_pos += 1;
                return o;
            }
        }
        const committed = state.pending;
        state.pending = null;
        return committed;
    }

    fn needsCheck(state: *const State, key: []const u8) bool {
        if (!state.range.exact) return true;
        return state.range.nan_check and property_index.orderedValueOf(key)[0] == OrderedTag.nan;
    }

    fn passes(self: *Self, id: NodeId, ctx: *ExecutionContext) OperatorError!bool {
        const row = self.current_row orelse return OperatorError.NotInitialized;
        row.clear();
        row.setSlot(self.output_slot, .{ .node_ref = id });
        for (self.conditions) |condition| {
            const result = self.evaluator.evaluate(condition.predicate, row, ctx) catch |err| return mapEvalError(err);
            if (!result.isTruthy()) return false;
        }
        return true;
    }

    fn nextId(self: *Self, ctx: *ExecutionContext) OperatorError!?NodeId {
        const state = &(self.state orelse return OperatorError.NotInitialized);
        while (!state.done_index) {
            const entry = (try self.nextIndexed(state, ctx)) orelse {
                state.done_index = true;
                break;
            };
            if (needsCheck(state, entry.key) and !try self.passes(entry.id, ctx)) continue;
            return entry.id;
        }

        const scan = state.missing orelse return null;
        const op = scan.operator();
        if (!state.missing_open) {
            try op.open(ctx);
            state.missing_open = true;
        }
        while (try op.next(ctx)) |row| {
            const id = switch (row.slots[self.output_slot]) {
                .node_ref => |n| n,
                else => continue,
            };
            var value = (self.database.getNodePropertyByKeyInTxn(ctx.txn, id, state.definition.property_id, self.allocator, null) catch |err|
                return ctx.storageFailure(err, "reading {s} of node {d}", .{ self.property, id })) orelse return id;
            value.deinit(self.allocator);
        }
        return null;
    }

    fn next(ptr: *anyopaque, ctx: *ExecutionContext) OperatorError!?*Row {
        const self: *Self = @ptrCast(@alignCast(ptr));
        const row = self.current_row orelse return OperatorError.NotInitialized;
        const id: NodeId = if (self.ids) |ids| blk: {
            if (self.ids_pos >= ids.len) return null;
            self.ids_pos += 1;
            break :blk ids[self.ids_pos - 1];
        } else (try self.nextId(ctx)) orelse return null;
        row.clear();
        row.setSlot(self.output_slot, .{ .node_ref = id });
        return row;
    }

    fn release(self: *Self, ctx: ?*ExecutionContext) void {
        if (self.state) |*state| {
            state.deinit(self.allocator, ctx);
            self.state = null;
        }
        if (self.ids) |ids| {
            self.allocator.free(ids);
            self.ids = null;
        }
    }

    fn close(ptr: *anyopaque, ctx: *ExecutionContext) void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        self.release(ctx);
    }

    fn deinit(ptr: *anyopaque, allocator: Allocator) void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        self.release(null);
        allocator.destroy(self);
    }
};

/// Passes rows through until no later row can be among the first k of an
/// ascending sort on `sort_key`, then ends its input early.
///
/// Its input has to arrive in that order already, up to ties: the planner
/// puts it above an IndexRangeScan read in order, with only filters and
/// expansions between. Values are compared by their place in the ordered
/// index encoding, where values ORDER BY calls equal share a key, so the
/// cutoff waits for a value strictly past the k-th row's. Lists, maps and
/// vectors are keyed by digest, out of order among themselves, so they count
/// as one value here; strings sharing their first 128 bytes likewise.
///
/// Only in queries that do not write: the sort above reads all of its input
/// otherwise, because rows below may be writing.
pub const OrderedCutoff = struct {
    allocator: Allocator,
    input: Operator,
    sort_key: *const ast.Expression,
    bounds: []const limit_ops.RowBound,
    evaluator: ExpressionEvaluator,
    needed: ?u64 = null,
    passed: u64 = 0,
    /// The group of the k-th row passed.
    last_group: [property_index.ORDERED_VALUE_MAX]u8 = undefined,
    last_group_len: usize = 0,
    done: bool = false,
    opened: bool = false,

    const Self = @This();

    /// Above every value in the encoding: where nulls sort.
    const NULL_GROUP = [_]u8{0xFF};

    pub fn init(
        allocator: Allocator,
        input: Operator,
        sort_key: *const ast.Expression,
        bounds: []const limit_ops.RowBound,
    ) !*Self {
        const self = try allocator.create(Self);
        self.* = .{
            .allocator = allocator,
            .input = input,
            .sort_key = sort_key,
            .bounds = bounds,
            .evaluator = ExpressionEvaluator.init(allocator),
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
        try self.input.open(ctx);
        self.opened = true;
        self.passed = 0;
        self.done = false;
        self.last_group_len = 0;
        self.needed = if (ctx.stable_reads) try self.rowsNeeded(ctx) else null;
    }

    /// As the sort computes it from the SKIP and LIMIT above it.
    fn rowsNeeded(self: *Self, ctx: *ExecutionContext) OperatorError!?u64 {
        var needed: ?u64 = null;
        var i = self.bounds.len;
        while (i > 0) {
            i -= 1;
            switch (self.bounds[i]) {
                .limit => |expr| {
                    const n = try limit_ops.evaluateCountExpression(&self.evaluator, expr, ctx);
                    needed = if (needed) |m| @min(m, n) else n;
                },
                .skip => |expr| {
                    const n = try limit_ops.evaluateCountExpression(&self.evaluator, expr, ctx);
                    if (needed) |m| needed = std.math.add(u64, m, n) catch null;
                },
            }
        }
        return needed;
    }

    fn groupOf(self: *Self, row: *const Row, ctx: *ExecutionContext, out: *[property_index.ORDERED_VALUE_MAX]u8) OperatorError![]const u8 {
        const result = self.evaluator.evaluate(self.sort_key, row, ctx) catch |err| return mapEvalError(err);
        const value = result.toPropertyValue(ctx.row_arena.allocator()) catch return OperatorError.OutOfMemory;
        const encoded = property_index.encodeOrderedValue(value) orelse {
            out[0] = NULL_GROUP[0];
            return out[0..1];
        };
        const group = encoded.orderGroup();
        @memcpy(out[0..group.len], group);
        return out[0..group.len];
    }

    fn next(ptr: *anyopaque, ctx: *ExecutionContext) OperatorError!?*Row {
        const self: *Self = @ptrCast(@alignCast(ptr));
        if (!self.opened) return OperatorError.NotInitialized;
        if (self.done) return null;
        const needed = self.needed orelse return self.input.next(ctx);
        if (needed == 0) {
            self.done = true;
            return null;
        }
        const row = (try self.input.next(ctx)) orelse return null;
        if (self.passed < needed) {
            self.passed += 1;
            if (self.passed == needed) {
                self.last_group_len = (try self.groupOf(row, ctx, &self.last_group)).len;
            }
            return row;
        }
        var buf: [property_index.ORDERED_VALUE_MAX]u8 = undefined;
        const group = try self.groupOf(row, ctx, &buf);
        if (!std.mem.eql(u8, group, self.last_group[0..self.last_group_len])) {
            self.done = true;
            return null;
        }
        return row;
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

fn mapEvalError(err: EvalError) OperatorError {
    return switch (err) {
        EvalError.OutOfMemory => OperatorError.OutOfMemory,
        EvalError.TypeError => OperatorError.TypeError,
        EvalError.UnboundVariable => OperatorError.UnboundVariable,
        EvalError.PropertyNotFound => OperatorError.PropertyNotFound,
        EvalError.StorageError => OperatorError.StorageError,
        else => OperatorError.EvaluationError,
    };
}

test "IndexRangeScan and OrderedCutoff have operator tables" {
    try std.testing.expect(@TypeOf(IndexRangeScan.vtable.open) != void);
    try std.testing.expect(@TypeOf(OrderedCutoff.vtable.open) != void);
}
