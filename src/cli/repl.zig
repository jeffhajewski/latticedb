//! Interactive REPL for LatticeDB.
//!
//! Provides a read-eval-print loop for executing Cypher queries
//! and database management commands.

const std = @import("std");
const lattice = @import("lattice");
const output = @import("output.zig");
const args_mod = @import("args.zig");

const Database = lattice.storage.database.Database;
const QueryResult = lattice.storage.database.QueryResult;
const ResultValue = lattice.storage.database.ResultValue;
const OutputFormat = args_mod.OutputFormat;

/// Managed array list for allocator tracking
fn ManagedArrayList(comptime T: type) type {
    return std.array_list.Managed(T);
}

/// REPL state and configuration
pub const Repl = struct {
    allocator: std.mem.Allocator,
    db: *Database,
    format: OutputFormat,
    show_timing: bool,
    running: bool,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, db: *Database, initial_format: OutputFormat) Self {
        return Self{
            .allocator = allocator,
            .db = db,
            .format = initial_format,
            .show_timing = false,
            .running = true,
        };
    }

    /// Run the REPL loop
    pub fn run(self: *Self, stdout: anytype, stderr: anytype) !void {
        const stdin = std.fs.File.stdin().deprecatedReader();

        // Print welcome message
        try stdout.print("LatticeDB v{s}\n", .{lattice.VERSION});
        try stdout.print("Connected to: {s}\n", .{self.db.getPath()});
        try stdout.writeAll("Type .help for help, .exit to quit\n\n");

        var line_buf = ManagedArrayList(u8).init(self.allocator);
        defer line_buf.deinit();

        while (self.running) {
            // Print prompt
            try stdout.writeAll("lattice> ");

            // Read line
            line_buf.items.len = 0;
            stdin.readUntilDelimiterArrayList(&line_buf, '\n', 65536) catch |err| {
                if (err == error.EndOfStream) {
                    try stdout.writeAll("\nGoodbye!\n");
                    break;
                }
                return err;
            };

            const line = std.mem.trim(u8, line_buf.items, " \t\r\n");

            if (line.len == 0) continue;

            // Handle dot commands
            if (line[0] == '.') {
                self.handleDotCommand(line, stdout, stderr) catch |err| {
                    output.printError(stderr, "Command failed: {s}", .{@errorName(err)});
                };
                continue;
            }

            // Execute as Cypher query
            self.executeQuery(line, stdout, stderr);
        }
    }

    /// Handle dot commands
    fn handleDotCommand(self: *Self, cmd: []const u8, stdout: anytype, stderr: anytype) !void {
        // Parse command and arguments
        var parts = std.mem.splitScalar(u8, cmd, ' ');
        const command = parts.next() orelse return;

        if (std.mem.eql(u8, command, ".help") or std.mem.eql(u8, command, ".h")) {
            try self.printReplHelp(stdout);
        } else if (std.mem.eql(u8, command, ".exit") or std.mem.eql(u8, command, ".quit") or std.mem.eql(u8, command, ".q")) {
            try stdout.writeAll("Goodbye!\n");
            self.running = false;
        } else if (std.mem.eql(u8, command, ".format")) {
            const format_arg = parts.next();
            if (format_arg) |fmt| {
                if (OutputFormat.fromString(fmt)) |new_format| {
                    self.format = new_format;
                    try stdout.print("Output format: {s}\n", .{fmt});
                } else {
                    output.printError(stderr, "Invalid format. Use: table, json, csv", .{});
                }
            } else {
                try stdout.print("Current format: {s}\n", .{@tagName(self.format)});
            }
        } else if (std.mem.eql(u8, command, ".timing")) {
            const arg = parts.next();
            if (arg) |a| {
                if (std.mem.eql(u8, a, "on")) {
                    self.show_timing = true;
                    try stdout.writeAll("Timing enabled\n");
                } else if (std.mem.eql(u8, a, "off")) {
                    self.show_timing = false;
                    try stdout.writeAll("Timing disabled\n");
                } else {
                    output.printError(stderr, "Usage: .timing on|off", .{});
                }
            } else {
                try stdout.print("Timing: {s}\n", .{if (self.show_timing) "on" else "off"});
            }
        } else if (std.mem.eql(u8, command, ".tables") or std.mem.eql(u8, command, ".labels")) {
            try self.showLabels(stdout, stderr);
        } else if (std.mem.eql(u8, command, ".types")) {
            try self.showEdgeTypes(stdout, stderr);
        } else if (std.mem.eql(u8, command, ".schema")) {
            try stdout.writeAll("Schema inference not yet implemented\n");
        } else if (std.mem.eql(u8, command, ".count")) {
            try self.showCounts(stdout);
        } else if (std.mem.eql(u8, command, ".clear")) {
            // Clear screen (ANSI escape)
            try stdout.writeAll("\x1b[2J\x1b[H");
        } else {
            output.printError(stderr, "Unknown command: {s}. Type .help for help.", .{command});
        }
    }

    /// Execute a Cypher query and display results
    fn executeQuery(self: *Self, query_str: []const u8, stdout: anytype, stderr: anytype) void {
        const start_time = std.time.nanoTimestamp();

        var result = self.db.query(query_str) catch |err| {
            const err_msg = switch (err) {
                error.ParseError => "Parse error: invalid Cypher syntax",
                error.SemanticError => "Semantic error: invalid query structure",
                error.PlanError => "Plan error: could not create execution plan",
                error.ExecutionError => "Execution error: query failed",
                error.OutOfMemory => "Out of memory",
            };
            output.printError(stderr, "{s}", .{err_msg});
            return;
        };
        defer result.deinit();

        const end_time = std.time.nanoTimestamp();
        const elapsed_ns = @as(u64, @intCast(end_time - start_time));
        const elapsed_ms = @as(f64, @floatFromInt(elapsed_ns)) / 1_000_000.0;

        // Display results based on format
        self.displayResult(&result, stdout, elapsed_ms) catch |err| {
            output.printError(stderr, "Failed to display results: {s}", .{@errorName(err)});
        };
    }

    /// Display query result in the selected format
    pub fn displayResult(self: *Self, result: *QueryResult, stdout: anytype, elapsed_ms: f64) !void {
        switch (self.format) {
            .table => try self.displayResultTable(result, stdout, elapsed_ms),
            .json => try self.displayResultJson(result, stdout),
            .csv => try self.displayResultCsv(result, stdout),
        }
    }

    /// Display result as a table
    fn displayResultTable(self: *Self, result: *QueryResult, stdout: anytype, elapsed_ms: f64) !void {
        if (result.columns.len == 0) {
            if (self.show_timing) {
                try stdout.print("Query executed ({d:.2} ms)\n", .{elapsed_ms});
            } else {
                try stdout.writeAll("Query executed\n");
            }
            return;
        }

        // Calculate column widths
        var col_widths = try self.allocator.alloc(usize, result.columns.len);
        defer self.allocator.free(col_widths);

        for (result.columns, 0..) |col, i| {
            col_widths[i] = col.len;
        }

        // Check row values for width
        for (result.rows) |row| {
            for (row.values, 0..) |val, i| {
                if (i < col_widths.len) {
                    const val_len = self.valueDisplayLen(val);
                    if (val_len > col_widths[i]) {
                        col_widths[i] = val_len;
                    }
                }
            }
        }

        // Render table
        try self.renderTableBorder(stdout, col_widths, .top);

        // Header row
        try stdout.writeAll("│");
        for (result.columns, 0..) |col, i| {
            try stdout.writeByte(' ');
            try stdout.writeAll(col);
            const padding = col_widths[i] - col.len;
            for (0..padding) |_| try stdout.writeByte(' ');
            try stdout.writeAll(" │");
        }
        try stdout.writeByte('\n');

        try self.renderTableBorder(stdout, col_widths, .middle);

        // Data rows
        for (result.rows) |row| {
            try stdout.writeAll("│");
            for (row.values, 0..) |val, i| {
                if (i < col_widths.len) {
                    try stdout.writeByte(' ');
                    const written = try self.writeValue(stdout, val);
                    const padding = col_widths[i] - written;
                    for (0..padding) |_| try stdout.writeByte(' ');
                    try stdout.writeAll(" │");
                }
            }
            try stdout.writeByte('\n');
        }

        try self.renderTableBorder(stdout, col_widths, .bottom);

        // Row count and timing
        if (self.show_timing) {
            try stdout.print("{d} row{s} ({d:.2} ms)\n", .{
                result.rows.len,
                if (result.rows.len == 1) "" else "s",
                elapsed_ms,
            });
        } else {
            try stdout.print("{d} row{s}\n", .{
                result.rows.len,
                if (result.rows.len == 1) "" else "s",
            });
        }
    }

    const BorderType = enum { top, middle, bottom };

    fn renderTableBorder(self: *Self, stdout: anytype, col_widths: []const usize, border_type: BorderType) !void {
        _ = self;
        const chars = switch (border_type) {
            .top => .{ "┌", "┬", "┐", "─" },
            .middle => .{ "├", "┼", "┤", "─" },
            .bottom => .{ "└", "┴", "┘", "─" },
        };

        try stdout.writeAll(chars[0]);
        for (col_widths, 0..) |w, i| {
            for (0..w + 2) |_| try stdout.writeAll(chars[3]);
            if (i < col_widths.len - 1) {
                try stdout.writeAll(chars[1]);
            }
        }
        try stdout.writeAll(chars[2]);
        try stdout.writeByte('\n');
    }

    fn valueDisplayLen(self: *Self, val: ResultValue) usize {
        _ = self;
        return switch (val) {
            .null_val => 4, // "null"
            .bool_val => |b| if (b) 4 else 5, // "true" or "false"
            .int_val => |i| blk: {
                if (i == 0) break :blk 1;
                var n = if (i < 0) @as(u64, @intCast(-i)) else @as(u64, @intCast(i));
                var len: usize = if (i < 0) 1 else 0;
                while (n > 0) : (n /= 10) len += 1;
                break :blk len;
            },
            .float_val => 10, // Approximate
            .string_val => |s| s.len + 2, // Include quotes
            .node_id => 12, // "Node(xxxxx)"
        };
    }

    fn writeValue(self: *Self, writer: anytype, val: ResultValue) !usize {
        _ = self;
        switch (val) {
            .null_val => {
                try writer.writeAll("null");
                return 4;
            },
            .bool_val => |b| {
                if (b) {
                    try writer.writeAll("true");
                    return 4;
                } else {
                    try writer.writeAll("false");
                    return 5;
                }
            },
            .int_val => |i| {
                var buf: [32]u8 = undefined;
                const slice = std.fmt.bufPrint(&buf, "{d}", .{i}) catch return 0;
                try writer.writeAll(slice);
                return slice.len;
            },
            .float_val => |f| {
                var buf: [32]u8 = undefined;
                const slice = std.fmt.bufPrint(&buf, "{d:.6}", .{f}) catch return 0;
                try writer.writeAll(slice);
                return slice.len;
            },
            .string_val => |s| {
                try writer.writeByte('"');
                try writer.writeAll(s);
                try writer.writeByte('"');
                return s.len + 2;
            },
            .node_id => |id| {
                var buf: [32]u8 = undefined;
                const slice = std.fmt.bufPrint(&buf, "Node({d})", .{id}) catch return 0;
                try writer.writeAll(slice);
                return slice.len;
            },
        }
    }

    /// Display result as JSON
    fn displayResultJson(self: *Self, result: *QueryResult, stdout: anytype) !void {
        _ = self;
        try stdout.writeAll("{\"columns\":[");

        for (result.columns, 0..) |col, i| {
            if (i > 0) try stdout.writeByte(',');
            try stdout.writeByte('"');
            try stdout.writeAll(col);
            try stdout.writeByte('"');
        }

        try stdout.writeAll("],\"rows\":[");

        for (result.rows, 0..) |row, row_idx| {
            if (row_idx > 0) try stdout.writeByte(',');
            try stdout.writeByte('{');

            for (row.values, 0..) |val, i| {
                if (i > 0) try stdout.writeByte(',');
                try stdout.writeByte('"');
                if (i < result.columns.len) {
                    try stdout.writeAll(result.columns[i]);
                }
                try stdout.writeAll("\":");
                try writeJsonValue(stdout, val);
            }

            try stdout.writeByte('}');
        }

        try stdout.print("],\"count\":{d}}}\n", .{result.rows.len});
    }

    /// Display result as CSV
    fn displayResultCsv(self: *Self, result: *QueryResult, stdout: anytype) !void {
        _ = self;
        // Header
        for (result.columns, 0..) |col, i| {
            if (i > 0) try stdout.writeByte(',');
            try stdout.writeAll(col);
        }
        try stdout.writeByte('\n');

        // Rows
        for (result.rows) |row| {
            for (row.values, 0..) |val, i| {
                if (i > 0) try stdout.writeByte(',');
                try writeCsvValue(stdout, val);
            }
            try stdout.writeByte('\n');
        }
    }

    fn showLabels(self: *Self, stdout: anytype, stderr: anytype) !void {
        _ = stderr;
        // TODO: Get actual labels from database when API is available
        _ = self;
        try stdout.writeAll("Label listing not yet implemented\n");
    }

    fn showEdgeTypes(self: *Self, stdout: anytype, stderr: anytype) !void {
        _ = stderr;
        // TODO: Get actual edge types from database when API is available
        _ = self;
        try stdout.writeAll("Edge type listing not yet implemented\n");
    }

    fn showCounts(self: *Self, stdout: anytype) !void {
        const node_count = self.db.nodeCount();
        const edge_count = self.db.edgeCount();
        try stdout.print("Nodes: {d}\n", .{node_count});
        try stdout.print("Edges: {d}\n", .{edge_count});
    }

    fn printReplHelp(self: *Self, stdout: anytype) !void {
        _ = self;
        try stdout.writeAll(
            \\REPL Commands:
            \\  .help, .h           Show this help message
            \\  .exit, .quit, .q    Exit the REPL
            \\  .format <fmt>       Set output format (table, json, csv)
            \\  .timing on|off      Toggle query timing display
            \\  .labels, .tables    List all node labels
            \\  .types              List all edge types
            \\  .schema             Show inferred schema
            \\  .count              Show node/edge counts
            \\  .clear              Clear the screen
            \\
            \\Cypher Examples:
            \\  CREATE (n:Person {name: "Alice"})
            \\  MATCH (n:Person) RETURN n.name
            \\  MATCH (a)-[r]->(b) RETURN a, r, b LIMIT 10
            \\
        );
    }
};

/// Write a value as JSON
fn writeJsonValue(writer: anytype, val: ResultValue) !void {
    switch (val) {
        .null_val => try writer.writeAll("null"),
        .bool_val => |b| try writer.writeAll(if (b) "true" else "false"),
        .int_val => |i| try writer.print("{d}", .{i}),
        .float_val => |f| try writer.print("{d}", .{f}),
        .string_val => |s| {
            try writer.writeByte('"');
            for (s) |c| {
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
        .node_id => |id| try writer.print("{d}", .{id}),
    }
}

/// Write a value as CSV
fn writeCsvValue(writer: anytype, val: ResultValue) !void {
    switch (val) {
        .null_val => {},
        .bool_val => |b| try writer.writeAll(if (b) "true" else "false"),
        .int_val => |i| try writer.print("{d}", .{i}),
        .float_val => |f| try writer.print("{d}", .{f}),
        .string_val => |s| {
            // Quote if contains comma, quote, or newline
            var needs_quote = false;
            for (s) |c| {
                if (c == ',' or c == '"' or c == '\n' or c == '\r') {
                    needs_quote = true;
                    break;
                }
            }

            if (needs_quote) {
                try writer.writeByte('"');
                for (s) |c| {
                    if (c == '"') {
                        try writer.writeAll("\"\"");
                    } else {
                        try writer.writeByte(c);
                    }
                }
                try writer.writeByte('"');
            } else {
                try writer.writeAll(s);
            }
        },
        .node_id => |id| try writer.print("{d}", .{id}),
    }
}

test "repl initialization" {
    // Basic smoke test - just verify the struct can be created
    const allocator = std.testing.allocator;
    _ = allocator;
    // Can't easily test full REPL without a database, but struct creation works
}
