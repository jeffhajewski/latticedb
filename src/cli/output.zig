//! Output formatting for Lattice CLI (table, JSON, CSV).

const std = @import("std");
const args = @import("args.zig");

/// Managed array list for allocator tracking
fn ManagedArrayList(comptime T: type) type {
    return std.array_list.Managed(T);
}

pub const OutputFormat = args.OutputFormat;

/// A single cell value that can be formatted.
pub const Value = union(enum) {
    null_val,
    bool_val: bool,
    int_val: i64,
    float_val: f64,
    string_val: []const u8,

    pub fn format(self: Value, writer: anytype) !void {
        switch (self) {
            .null_val => try writer.writeAll("null"),
            .bool_val => |v| try writer.writeAll(if (v) "true" else "false"),
            .int_val => |v| try writer.print("{d}", .{v}),
            .float_val => |v| try writer.print("{d:.2}", .{v}),
            .string_val => |v| try writer.writeAll(v),
        }
    }

    pub fn formatJson(self: Value, writer: anytype) !void {
        switch (self) {
            .null_val => try writer.writeAll("null"),
            .bool_val => |v| try writer.writeAll(if (v) "true" else "false"),
            .int_val => |v| try writer.print("{d}", .{v}),
            .float_val => |v| try writer.print("{d}", .{v}),
            .string_val => |v| {
                try writer.writeByte('"');
                for (v) |c| {
                    switch (c) {
                        '"' => try writer.writeAll("\\\""),
                        '\\' => try writer.writeAll("\\\\"),
                        '\n' => try writer.writeAll("\\n"),
                        '\r' => try writer.writeAll("\\r"),
                        '\t' => try writer.writeAll("\\t"),
                        else => try writer.writeByte(c),
                    }
                }
                try writer.writeByte('"');
            },
        }
    }

    pub fn formatCsv(self: Value, writer: anytype) !void {
        switch (self) {
            .null_val => {},
            .bool_val => |v| try writer.writeAll(if (v) "true" else "false"),
            .int_val => |v| try writer.print("{d}", .{v}),
            .float_val => |v| try writer.print("{d}", .{v}),
            .string_val => |v| {
                // Quote if contains comma, quote, or newline
                const needs_quote = for (v) |c| {
                    if (c == ',' or c == '"' or c == '\n' or c == '\r') break true;
                } else false;

                if (needs_quote) {
                    try writer.writeByte('"');
                    for (v) |c| {
                        if (c == '"') {
                            try writer.writeAll("\"\"");
                        } else {
                            try writer.writeByte(c);
                        }
                    }
                    try writer.writeByte('"');
                } else {
                    try writer.writeAll(v);
                }
            },
        }
    }

    pub fn width(self: Value, allocator: std.mem.Allocator) !usize {
        var buf = ManagedArrayList(u8).init(allocator);
        defer buf.deinit();
        try self.format(buf.writer());
        return buf.items.len;
    }
};

/// Table output with box-drawing characters.
pub const TableFormatter = struct {
    allocator: std.mem.Allocator,
    columns: []const []const u8,
    rows: ManagedArrayList([]Value),
    col_widths: []usize,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, columns: []const []const u8) !Self {
        var col_widths = try allocator.alloc(usize, columns.len);
        for (columns, 0..) |col, i| {
            col_widths[i] = col.len;
        }

        return Self{
            .allocator = allocator,
            .columns = columns,
            .rows = ManagedArrayList([]Value).init(allocator),
            .col_widths = col_widths,
        };
    }

    pub fn deinit(self: *Self) void {
        for (self.rows.items) |row| {
            self.allocator.free(row);
        }
        self.rows.deinit();
        self.allocator.free(self.col_widths);
    }

    pub fn addRow(self: *Self, values: []const Value) !void {
        const row = try self.allocator.alloc(Value, values.len);
        @memcpy(row, values);

        // Update column widths
        for (values, 0..) |val, i| {
            if (i < self.col_widths.len) {
                const w = try val.width(self.allocator);
                if (w > self.col_widths[i]) {
                    self.col_widths[i] = w;
                }
            }
        }

        try self.rows.append(row);
    }

    pub fn render(self: *Self, writer: anytype) !void {
        // Top border
        try self.renderBorder(writer, .top);

        // Header
        try self.renderRow(writer, null);

        // Header separator
        try self.renderBorder(writer, .middle);

        // Data rows
        for (self.rows.items) |row| {
            try self.renderRow(writer, row);
        }

        // Bottom border
        try self.renderBorder(writer, .bottom);

        // Row count
        try writer.print("{d} row{s}\n", .{
            self.rows.items.len,
            if (self.rows.items.len == 1) "" else "s",
        });
    }

    const BorderType = enum { top, middle, bottom };

    fn renderBorder(self: *Self, writer: anytype, border_type: BorderType) !void {
        const chars = switch (border_type) {
            .top => .{ "┌", "┬", "┐", "─" },
            .middle => .{ "├", "┼", "┤", "─" },
            .bottom => .{ "└", "┴", "┘", "─" },
        };

        try writer.writeAll(chars[0]);
        for (self.col_widths, 0..) |w, i| {
            for (0..w + 2) |_| try writer.writeAll(chars[3]);
            if (i < self.col_widths.len - 1) {
                try writer.writeAll(chars[1]);
            }
        }
        try writer.writeAll(chars[2]);
        try writer.writeByte('\n');
    }

    fn renderRow(self: *Self, writer: anytype, row: ?[]Value) !void {
        try writer.writeAll("│");
        for (self.col_widths, 0..) |w, i| {
            try writer.writeByte(' ');

            var buf = ManagedArrayList(u8).init(self.allocator);
            defer buf.deinit();

            if (row) |r| {
                if (i < r.len) {
                    try r[i].format(buf.writer());
                }
            } else {
                // Header row
                if (i < self.columns.len) {
                    try buf.appendSlice(self.columns[i]);
                }
            }

            try writer.writeAll(buf.items);

            // Padding
            const padding = w - buf.items.len;
            for (0..padding) |_| try writer.writeByte(' ');
            try writer.writeAll(" │");
        }
        try writer.writeByte('\n');
    }
};

/// JSON output formatter.
pub const JsonFormatter = struct {
    allocator: std.mem.Allocator,
    columns: []const []const u8,
    rows: ManagedArrayList([]Value),

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, columns: []const []const u8) Self {
        return Self{
            .allocator = allocator,
            .columns = columns,
            .rows = ManagedArrayList([]Value).init(allocator),
        };
    }

    pub fn deinit(self: *Self) void {
        for (self.rows.items) |row| {
            self.allocator.free(row);
        }
        self.rows.deinit();
    }

    pub fn addRow(self: *Self, values: []const Value) !void {
        const row = try self.allocator.alloc(Value, values.len);
        @memcpy(row, values);
        try self.rows.append(row);
    }

    pub fn render(self: *Self, writer: anytype) !void {
        try writer.writeAll("{\"columns\":[");

        for (self.columns, 0..) |col, i| {
            if (i > 0) try writer.writeByte(',');
            try writer.writeByte('"');
            try writer.writeAll(col);
            try writer.writeByte('"');
        }

        try writer.writeAll("],\"rows\":[");

        for (self.rows.items, 0..) |row, row_idx| {
            if (row_idx > 0) try writer.writeByte(',');
            try writer.writeByte('{');

            for (row, 0..) |val, i| {
                if (i > 0) try writer.writeByte(',');
                try writer.writeByte('"');
                if (i < self.columns.len) {
                    try writer.writeAll(self.columns[i]);
                }
                try writer.writeAll("\":");
                try val.formatJson(writer);
            }

            try writer.writeByte('}');
        }

        try writer.print("],\"count\":{d}}}\n", .{self.rows.items.len});
    }
};

/// CSV output formatter.
pub const CsvFormatter = struct {
    allocator: std.mem.Allocator,
    columns: []const []const u8,
    rows: ManagedArrayList([]Value),

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, columns: []const []const u8) Self {
        return Self{
            .allocator = allocator,
            .columns = columns,
            .rows = ManagedArrayList([]Value).init(allocator),
        };
    }

    pub fn deinit(self: *Self) void {
        for (self.rows.items) |row| {
            self.allocator.free(row);
        }
        self.rows.deinit();
    }

    pub fn addRow(self: *Self, values: []const Value) !void {
        const row = try self.allocator.alloc(Value, values.len);
        @memcpy(row, values);
        try self.rows.append(row);
    }

    pub fn render(self: *Self, writer: anytype) !void {
        // Header
        for (self.columns, 0..) |col, i| {
            if (i > 0) try writer.writeByte(',');
            try writer.writeAll(col);
        }
        try writer.writeByte('\n');

        // Rows
        for (self.rows.items) |row| {
            for (row, 0..) |val, i| {
                if (i > 0) try writer.writeByte(',');
                try val.formatCsv(writer);
            }
            try writer.writeByte('\n');
        }
    }
};

// Simple message output
pub fn printError(writer: anytype, comptime fmt: []const u8, fmtArgs: anytype) void {
    writer.print("Error: " ++ fmt ++ "\n", fmtArgs) catch {};
}

pub fn printSuccess(writer: anytype, comptime fmt: []const u8, fmtArgs: anytype) void {
    writer.print(fmt ++ "\n", fmtArgs) catch {};
}

pub fn printInfo(writer: anytype, comptime fmt: []const u8, fmtArgs: anytype) void {
    writer.print(fmt ++ "\n", fmtArgs) catch {};
}

test "table formatter" {
    const allocator = std.testing.allocator;
    var table = try TableFormatter.init(allocator, &.{ "name", "age" });
    defer table.deinit();

    try table.addRow(&.{ .{ .string_val = "Alice" }, .{ .int_val = 30 } });
    try table.addRow(&.{ .{ .string_val = "Bob" }, .{ .int_val = 25 } });

    var buf = ManagedArrayList(u8).init(allocator);
    defer buf.deinit();

    try table.render(buf.writer());
    try std.testing.expect(buf.items.len > 0);
}

test "json formatter" {
    const allocator = std.testing.allocator;
    var json = JsonFormatter.init(allocator, &.{ "name", "age" });
    defer json.deinit();

    try json.addRow(&.{ .{ .string_val = "Alice" }, .{ .int_val = 30 } });

    var buf = ManagedArrayList(u8).init(allocator);
    defer buf.deinit();

    try json.render(buf.writer());
    try std.testing.expect(std.mem.indexOf(u8, buf.items, "\"Alice\"") != null);
}
