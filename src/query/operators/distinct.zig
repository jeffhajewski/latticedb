//! Distinct operator for query execution.
//!
//! Removes duplicate rows from the input stream.
//! Blocking operator: consumes all input during open(), then emits unique rows.

const std = @import("std");
const Allocator = std.mem.Allocator;

const executor = @import("../executor.zig");
const Operator = executor.Operator;
const OperatorError = executor.OperatorError;
const Row = executor.Row;
const SlotValue = executor.SlotValue;
const ExecutionContext = executor.ExecutionContext;
const MAX_SLOTS = executor.MAX_SLOTS;

const lattice = @import("lattice");
const PropertyValue = lattice.core.types.PropertyValue;

// ============================================================================
// Distinct Operator
// ============================================================================

/// Distinct operator that removes duplicate rows.
/// Consumes all input during open(), deduplicates by row fingerprint,
/// then emits unique rows via next().
pub const Distinct = struct {
    /// Input operator
    input: Operator,
    /// Allocator
    allocator: Allocator,
    /// Set of seen fingerprints
    seen: std.StringHashMap(void),
    /// Materialized unique rows
    rows: std.ArrayListUnmanaged(Row),
    /// Current output index
    output_index: usize,
    /// Whether the input has been consumed
    opened: bool,

    const Self = @This();

    /// Create a new Distinct operator
    pub fn init(allocator: Allocator, input: Operator) !*Self {
        const self = try allocator.create(Self);
        self.* = Self{
            .input = input,
            .allocator = allocator,
            .seen = std.StringHashMap(void).init(allocator),
            .rows = .empty,
            .output_index = 0,
            .opened = false,
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
        .deinit = deinitOp,
    };

    fn open(ptr: *anyopaque, ctx: *ExecutionContext) OperatorError!void {
        const self: *Self = @ptrCast(@alignCast(ptr));

        // Open input
        try self.input.open(ctx);

        // Consume all input rows and deduplicate
        while (true) {
            const row = try self.input.next(ctx) orelse break;

            // Compute fingerprint for this row
            var fp_buf: [1024]u8 = undefined;
            const fp_len = computeRowFingerprint(row, &fp_buf);
            const fp = fp_buf[0..fp_len];

            // Check if we've seen this fingerprint
            const key_copy = self.allocator.dupe(u8, fp) catch return OperatorError.OutOfMemory;
            const gop = self.seen.getOrPut(key_copy) catch {
                self.allocator.free(key_copy);
                return OperatorError.OutOfMemory;
            };

            if (!gop.found_existing) {
                // New unique row - store a copy
                self.rows.append(self.allocator, row.clone()) catch return OperatorError.OutOfMemory;
            } else {
                // Duplicate - free the key copy we allocated (the map kept the old key)
                self.allocator.free(key_copy);
            }
        }

        self.opened = true;
        self.output_index = 0;
    }

    fn next(ptr: *anyopaque, ctx: *ExecutionContext) OperatorError!?*Row {
        const self: *Self = @ptrCast(@alignCast(ptr));

        if (!self.opened) return OperatorError.NotInitialized;

        if (self.output_index >= self.rows.items.len) return null;

        // Allocate output row and copy
        const output_row = ctx.allocRow() catch return OperatorError.OutOfMemory;
        output_row.* = self.rows.items[self.output_index];
        self.output_index += 1;

        return output_row;
    }

    fn close(ptr: *anyopaque, ctx: *ExecutionContext) void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        self.input.close(ctx);
    }

    fn deinitOp(ptr: *anyopaque, allocator: Allocator) void {
        const self: *Self = @ptrCast(@alignCast(ptr));

        // Free seen map keys
        var it = self.seen.iterator();
        while (it.next()) |entry| {
            allocator.free(entry.key_ptr.*);
        }
        self.seen.deinit();

        // Free rows list
        self.rows.deinit(allocator);

        // Clean up input
        self.input.deinit(allocator);

        allocator.destroy(self);
    }
};

/// Compute a fingerprint for a row by hashing all populated slots.
fn computeRowFingerprint(row: *const Row, buf: []u8) usize {
    var len: usize = 0;
    var slot: u8 = 0;
    while (slot < MAX_SLOTS) : (slot += 1) {
        if (!row.hasSlot(slot)) continue;
        const sv = row.slots[slot];
        len += hashSlotValue(sv, buf[len..]);
    }
    return len;
}

/// Hash a SlotValue into bytes for fingerprinting.
fn hashSlotValue(sv: SlotValue, buf: []u8) usize {
    var len: usize = 0;
    switch (sv) {
        .empty => {
            buf[0] = 0;
            len = 1;
        },
        .node_ref => |id| {
            buf[0] = 1;
            const bytes = std.mem.asBytes(&id);
            @memcpy(buf[1 .. 1 + bytes.len], bytes);
            len = 1 + bytes.len;
        },
        .edge_ref => |id| {
            buf[0] = 2;
            const bytes = std.mem.asBytes(&id);
            @memcpy(buf[1 .. 1 + bytes.len], bytes);
            len = 1 + bytes.len;
        },
        .property => |pv| {
            buf[0] = 3;
            len = 1 + hashPropertyValue(pv, buf[1..]);
        },
    }
    return len;
}

/// Hash a PropertyValue into bytes.
fn hashPropertyValue(pv: PropertyValue, buf: []u8) usize {
    var len: usize = 0;
    switch (pv) {
        .null_val => {
            buf[0] = 0;
            len = 1;
        },
        .bool_val => |b| {
            buf[0] = if (b) 1 else 2;
            len = 1;
        },
        .int_val => |i| {
            buf[0] = 3;
            @memcpy(buf[1..9], std.mem.asBytes(&i));
            len = 9;
        },
        .float_val => |f| {
            buf[0] = 4;
            @memcpy(buf[1..9], std.mem.asBytes(&f));
            len = 9;
        },
        .string_val => |s| {
            buf[0] = 5;
            const copy_len = @min(s.len, buf.len - 1);
            @memcpy(buf[1 .. 1 + copy_len], s[0..copy_len]);
            len = 1 + copy_len;
        },
        .bytes_val => |b| {
            buf[0] = 6;
            const copy_len = @min(b.len, buf.len - 1);
            @memcpy(buf[1 .. 1 + copy_len], b[0..copy_len]);
            len = 1 + copy_len;
        },
        .vector_val => |v| {
            buf[0] = 7;
            const byte_len = @min(v.len * @sizeOf(f32), buf.len - 1);
            @memcpy(buf[1 .. 1 + byte_len], std.mem.sliceAsBytes(v)[0..byte_len]);
            len = 1 + byte_len;
        },
        .list_val => {
            buf[0] = 8;
            len = 1;
        },
        .map_val => {
            buf[0] = 9;
            len = 1;
        },
    }
    return len;
}

// ============================================================================
// Tests
// ============================================================================

test "Distinct basic structure" {
    const vtable_check = Distinct.vtable;
    try std.testing.expect(@TypeOf(vtable_check.open) != void);
    try std.testing.expect(@TypeOf(vtable_check.next) != void);
    try std.testing.expect(@TypeOf(vtable_check.close) != void);
    try std.testing.expect(@TypeOf(vtable_check.deinit) != void);
}

test "computeRowFingerprint deterministic" {
    var row1 = Row.init();
    row1.setSlot(0, .{ .property = .{ .int_val = 42 } });
    row1.setSlot(1, .{ .property = .{ .string_val = "hello" } });

    var row2 = Row.init();
    row2.setSlot(0, .{ .property = .{ .int_val = 42 } });
    row2.setSlot(1, .{ .property = .{ .string_val = "hello" } });

    var buf1: [1024]u8 = undefined;
    var buf2: [1024]u8 = undefined;
    const len1 = computeRowFingerprint(&row1, &buf1);
    const len2 = computeRowFingerprint(&row2, &buf2);

    try std.testing.expectEqual(len1, len2);
    try std.testing.expectEqualSlices(u8, buf1[0..len1], buf2[0..len2]);
}

test "computeRowFingerprint different rows" {
    var row1 = Row.init();
    row1.setSlot(0, .{ .property = .{ .int_val = 42 } });

    var row2 = Row.init();
    row2.setSlot(0, .{ .property = .{ .int_val = 43 } });

    var buf1: [1024]u8 = undefined;
    var buf2: [1024]u8 = undefined;
    const len1 = computeRowFingerprint(&row1, &buf1);
    const len2 = computeRowFingerprint(&row2, &buf2);

    try std.testing.expect(!std.mem.eql(u8, buf1[0..len1], buf2[0..len2]));
}
