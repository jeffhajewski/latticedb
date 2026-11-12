//! Range scans and ordered reads over an ordered property index.
//!
//! - IndexRangeScan: the nodes with a label whose indexed property lies in a
//!   range (`<`, `<=`, `>`, `>=`, `STARTS WITH`, any conjunction of them on
//!   the property), or, with no range, every node with the label in the order
//!   of the property, ascending or descending, nulls where ORDER BY puts them.
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

const ast = @import("../ast.zig");
const expression = @import("../expression.zig");
const ExpressionEvaluator = expression.ExpressionEvaluator;
const EvalError = expression.EvalError;

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
/// from the label's ordered index on the property in the order of the
/// value, ascending or descending, and by id among equal values either way.
/// That is the order a sort on the property leaves rows in, since the sort
/// keeps ties in input order and a scan by label reads by id.
///
/// The conditions leave the WHERE: the scan answers them. Where the index
/// cannot tell (a string bound longer than the part of a value a key holds)
/// it evaluates the conditions on the row itself. NaN passes no comparison,
/// so its keys, after every number, are outside every range.
///
/// With no conditions it returns every node with the label. That takes a
/// dense index, which holds the nodes without the property after every
/// value: last ascending and first descending, where ORDER BY puts nulls.
///
/// Read descending, the index gives equal values with the highest id first,
/// so the scan collects each run of equal keys and returns it from the end.
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
    descending: bool,
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
        /// Nodes the transaction's overlay holds that fall in the range,
        /// in the order the scan reads. The keys are slices of `overlay_bytes`.
        overlay: std.ArrayList(OverlayEntry) = .empty,
        overlay_bytes: std.ArrayList(u8) = .empty,
        overlay_pos: usize = 0,
        /// The next committed entry, read but not yet emitted.
        pending: ?Entry = null,
        /// Descending: the ids of a run of equal values, highest first, and
        /// how many are left to return from its end.
        run: std.ArrayList(NodeId) = .empty,
        run_left: usize = 0,
        /// Descending: the entry that ended the last run, which begins the
        /// next.
        lookahead: ?Entry = null,
        done: bool = false,

        fn deinit(self: *State, allocator: Allocator) void {
            self.keys.deinit(allocator);
            self.ends.deinit(allocator);
            self.overlay.deinit(allocator);
            self.overlay_bytes.deinit(allocator);
            self.run.deinit(allocator);
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
        descending: bool,
        database: *Database,
    ) !*Self {
        const self = try allocator.create(Self);
        self.* = .{
            .allocator = allocator,
            .output_slot = output_slot,
            .label = label,
            .property = property,
            .conditions = conditions,
            .descending = descending,
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
        self.release();
        self.current_row = ctx.allocRow() catch return OperatorError.OutOfMemory;

        const definition = (self.database.nodePropertyIndex(self.label, self.property) catch |err|
            return ctx.storageFailure(err, "reading the property index on {s}.{s}", .{ self.label, self.property })) orelse {
            // Dropped since the plan was made.
            return ctx.fail(OperatorError.StorageError, null, "The property index on {s}.{s} is gone", .{ self.label, self.property });
        };
        if (!definition.encoding.isOrdered()) {
            return ctx.fail(OperatorError.StorageError, null, "The property index on {s}.{s} cannot answer ranges", .{ self.label, self.property });
        }
        if (self.conditions.len == 0 and definition.encoding != .ordered_dense) {
            return ctx.fail(OperatorError.StorageError, null, "The property index on {s}.{s} does not hold the nodes without the property", .{ self.label, self.property });
        }

        var range = ValueRange.all();
        for (self.conditions) |condition| {
            const value = try self.evaluateIndependent(condition.value, ctx);
            range.constrain(condition.op, value);
        }

        self.state = .{ .definition = definition, .label_id = definition.scope_id, .range = range };
        const state = &self.state.?;
        if (range.empty) {
            state.exhausted = true;
            state.done = true;
        } else {
            setBounds(state, range.startBytes(), range.endBytes());
            try self.collectOverlay(state, ctx);
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

    fn inRange(state: *const State, key: []const u8) bool {
        const value = property_index.orderedValueOf(key);
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
            // Null is the property missing, which a dense index keys too.
            var value = (self.database.getNodePropertyByKeyInTxn(ctx.txn, id, state.definition.property_id, self.allocator, null) catch |err|
                return ctx.storageFailure(err, "reading {s} of node {d}", .{ self.property, id })) orelse PropertyValue{ .null_val = {} };
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
            fn greaterThan(bytes: []const u8, a: OverlayEntry, b: OverlayEntry) bool {
                return std.mem.order(u8, bytes[a.from..a.to], bytes[b.from..b.to]) == .gt;
            }
        };
        const bytes: []const u8 = state.overlay_bytes.items;
        if (self.descending)
            std.mem.sort(OverlayEntry, state.overlay.items, bytes, Sorter.greaterThan)
        else
            std.mem.sort(OverlayEntry, state.overlay.items, bytes, Sorter.lessThan);
    }

    /// The next committed entry the overlay does not hold.
    fn nextCommitted(self: *Self, state: *State, ctx: *ExecutionContext) OperatorError!?Entry {
        while (true) {
            if (state.chunk_pos == state.ends.items.len) {
                if (state.exhausted) return null;
                if (state.ends.items.len > 0) {
                    const last = state.keys.items[self.keyFrom(state, state.ends.items.len - 1)..];
                    if (self.descending) {
                        // End at the last key read, which the end excludes.
                        @memcpy(state.end[0..last.len], last);
                        state.end_len = last.len;
                    } else {
                        // Start after the last key read: it followed by a
                        // zero byte is the least key above it.
                        @memcpy(state.start[0..last.len], last);
                        state.start[last.len] = 0;
                        state.start_len = last.len + 1;
                    }
                }
                state.keys.clearRetainingCapacity();
                state.ends.clearRetainingCapacity();
                state.chunk_pos = 0;
                var index = &(self.database.property_index orelse
                    return ctx.fail(OperatorError.StorageError, null, "The property index on {s}.{s} is gone", .{ self.label, self.property }));
                state.exhausted = index.readRange(
                    .node,
                    if (self.descending) .backward else .forward,
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
            const first: std.math.Order = if (self.descending) .gt else .lt;
            if (state.pending == null or std.mem.order(u8, o.key, state.pending.?.key) == first) {
                state.overlay_pos += 1;
                return o;
            }
        }
        const committed = state.pending;
        state.pending = null;
        return committed;
    }

    fn needsCheck(state: *const State) bool {
        return !state.range.exact;
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

    /// The next id in the scan's order, before any check.
    fn nextOrdered(self: *Self, state: *State, ctx: *ExecutionContext) OperatorError!?NodeId {
        if (!self.descending) {
            const entry = (try self.nextIndexed(state, ctx)) orelse return null;
            return entry.id;
        }
        if (state.run_left == 0) {
            // Gather the next run of equal values. Its first entry's key
            // goes stale on the next read, so its value is copied first.
            const first = state.lookahead orelse (try self.nextIndexed(state, ctx)) orelse return null;
            state.lookahead = null;
            var value_buf: [property_index.ORDERED_VALUE_MAX]u8 = undefined;
            const first_value = property_index.orderedValueOf(first.key);
            @memcpy(value_buf[0..first_value.len], first_value);
            const value = value_buf[0..first_value.len];
            state.run.clearRetainingCapacity();
            state.run.append(self.allocator, first.id) catch return OperatorError.OutOfMemory;
            while (try self.nextIndexed(state, ctx)) |entry| {
                if (!std.mem.eql(u8, property_index.orderedValueOf(entry.key), value)) {
                    state.lookahead = entry;
                    break;
                }
                state.run.append(self.allocator, entry.id) catch return OperatorError.OutOfMemory;
            }
            state.run_left = state.run.items.len;
        }
        state.run_left -= 1;
        return state.run.items[state.run_left];
    }

    fn nextId(self: *Self, ctx: *ExecutionContext) OperatorError!?NodeId {
        const state = &(self.state orelse return OperatorError.NotInitialized);
        while (!state.done) {
            const id = (try self.nextOrdered(state, ctx)) orelse {
                state.done = true;
                break;
            };
            if (needsCheck(state) and !try self.passes(id, ctx)) continue;
            return id;
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

    fn release(self: *Self) void {
        if (self.state) |*state| {
            state.deinit(self.allocator);
            self.state = null;
        }
        if (self.ids) |ids| {
            self.allocator.free(ids);
            self.ids = null;
        }
    }

    fn close(ptr: *anyopaque, _: *ExecutionContext) void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        self.release();
    }

    fn deinit(ptr: *anyopaque, allocator: Allocator) void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        self.release();
        allocator.destroy(self);
    }
};

/// Passes rows through until no later row can be among the first k of a
/// sort on `sort_key`, then ends its input early.
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

    /// Not the group of any value. Only compared for equality, so the
    /// direction of the sort does not matter.
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
