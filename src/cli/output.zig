//! Output formatting for Lattice CLI (table, JSON, CSV).

const std = @import("std");
const lattice = @import("lattice");
const args = @import("args.zig");
const QueryFailure = lattice.storage.database.QueryFailure;
const QueryFailureLocation = lattice.storage.database.QueryFailureLocation;

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

/// Print a query failure with stage, message, and optional source location hint.
pub fn printQueryFailure(writer: anytype, query: []const u8, failure: QueryFailure) void {
    const stage_name = switch (failure.stage) {
        .parse => "Parse",
        .semantic => "Semantic",
        .plan => "Plan",
        .execution => "Execution",
    };

    if (failure.location) |loc| {
        if (failure.code) |code| {
            printError(
                writer,
                "{s} error ({s}) at {d}:{d}: {s}",
                .{ stage_name, code, loc.line, loc.column, failure.message },
            );
        } else {
            printError(
                writer,
                "{s} error at {d}:{d}: {s}",
                .{ stage_name, loc.line, loc.column, failure.message },
            );
        }
        printSourceHint(writer, query, loc);
        return;
    }

    if (failure.code) |code| {
        printError(writer, "{s} error ({s}): {s}", .{ stage_name, code, failure.message });
    } else {
        printError(writer, "{s} error: {s}", .{ stage_name, failure.message });
    }
}

fn printSourceHint(writer: anytype, query: []const u8, loc: QueryFailureLocation) void {
    const raw_line = sourceLineAt(query, loc.line) orelse return;
    const line = std.mem.trimRight(u8, raw_line, "\r");

    writer.print("  {d} | {s}\n", .{ loc.line, line }) catch return;
    writer.writeAll("    | ") catch return;

    const col: usize = if (loc.column > 0) @intCast(loc.column - 1) else 0;
    for (0..col) |_| writer.writeByte(' ') catch return;

    const len_raw: usize = if (loc.length > 0) @intCast(loc.length) else 1;
    const caret_len: usize = @min(len_raw, 80);
    for (0..caret_len) |_| writer.writeByte('^') catch return;
    writer.writeByte('\n') catch {};
}

fn sourceLineAt(query: []const u8, target_line: u32) ?[]const u8 {
    if (target_line == 0) return null;

    var line: u32 = 1;
    var start: usize = 0;
    var i: usize = 0;
    while (i <= query.len) : (i += 1) {
        if (i == query.len or query[i] == '\n') {
            if (line == target_line) {
                return query[start..i];
            }
            line += 1;
            start = i + 1;
        }
    }
    return null;
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
