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
const ExecutionContext = executor.ExecutionContext;
const key_encoding = @import("key_encoding.zig");

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
        var key_buf: std.ArrayList(u8) = .empty;
        defer key_buf.deinit(self.allocator);

        while (true) {
            const row = try self.input.next(ctx) orelse break;

            // Compute exact key for this row
            key_buf.clearRetainingCapacity();
            key_encoding.appendRowKey(&key_buf, self.allocator, row) catch return OperatorError.OutOfMemory;

            // Check if we've seen this key
            const key_copy = self.allocator.dupe(u8, key_buf.items) catch return OperatorError.OutOfMemory;
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

test "row key encoding is deterministic" {
    var row1 = Row.init();
    row1.setSlot(0, .{ .property = .{ .int_val = 42 } });
    row1.setSlot(1, .{ .property = .{ .string_val = "hello" } });

    var row2 = Row.init();
    row2.setSlot(0, .{ .property = .{ .int_val = 42 } });
    row2.setSlot(1, .{ .property = .{ .string_val = "hello" } });

    var buf1: std.ArrayList(u8) = .empty;
    defer buf1.deinit(std.testing.allocator);
    var buf2: std.ArrayList(u8) = .empty;
    defer buf2.deinit(std.testing.allocator);
    try key_encoding.appendRowKey(&buf1, std.testing.allocator, &row1);
    try key_encoding.appendRowKey(&buf2, std.testing.allocator, &row2);

    try std.testing.expectEqual(buf1.items.len, buf2.items.len);
    try std.testing.expectEqualSlices(u8, buf1.items, buf2.items);
}

test "row key encoding differs for different rows" {
    var row1 = Row.init();
    row1.setSlot(0, .{ .property = .{ .int_val = 42 } });

    var row2 = Row.init();
    row2.setSlot(0, .{ .property = .{ .int_val = 43 } });

    var buf1: std.ArrayList(u8) = .empty;
    defer buf1.deinit(std.testing.allocator);
    var buf2: std.ArrayList(u8) = .empty;
    defer buf2.deinit(std.testing.allocator);
    try key_encoding.appendRowKey(&buf1, std.testing.allocator, &row1);
    try key_encoding.appendRowKey(&buf2, std.testing.allocator, &row2);

    try std.testing.expect(!std.mem.eql(u8, buf1.items, buf2.items));
}

test "row key encoding distinguishes long shared-prefix strings" {
    const allocator = std.testing.allocator;
    const prefix_len: usize = 1300;

    var s1 = try allocator.alloc(u8, prefix_len + 1);
    defer allocator.free(s1);
    var s2 = try allocator.alloc(u8, prefix_len + 1);
    defer allocator.free(s2);

    @memset(s1[0..prefix_len], 'a');
    @memset(s2[0..prefix_len], 'a');
    s1[prefix_len] = 'x';
    s2[prefix_len] = 'y';

    var row1 = Row.init();
    row1.setSlot(0, .{ .property = .{ .string_val = s1 } });

    var row2 = Row.init();
    row2.setSlot(0, .{ .property = .{ .string_val = s2 } });

    var buf1: std.ArrayList(u8) = .empty;
    defer buf1.deinit(allocator);
    var buf2: std.ArrayList(u8) = .empty;
    defer buf2.deinit(allocator);
    try key_encoding.appendRowKey(&buf1, allocator, &row1);
    try key_encoding.appendRowKey(&buf2, allocator, &row2);

    try std.testing.expect(!std.mem.eql(u8, buf1.items, buf2.items));
}
