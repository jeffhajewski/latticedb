//! Interactive REPL for LatticeDB.
//!
//! Provides a read-eval-print loop for executing Cypher queries
//! and database management commands.

const std = @import("std");
const lattice = @import("lattice");
const output = @import("output.zig");
const args_mod = @import("args.zig");
const history_mod = @import("history.zig");

const Database = lattice.storage.database.Database;
const QueryResult = lattice.storage.database.QueryResult;
const ResultValue = lattice.storage.database.ResultValue;
const OutputFormat = args_mod.OutputFormat;
const History = history_mod.History;

/// Managed array list for allocator tracking
fn ManagedArrayList(comptime T: type) type {
    return std.array_list.Managed(T);
}

const LineReadAction = enum {
    line,
    canceled,
    eof,
};

const RawTerminalMode = struct {
    enabled: bool = false,
    original: std.posix.termios = undefined,

    fn enableIfTty() !RawTerminalMode {
        if (!std.posix.isatty(std.posix.STDIN_FILENO)) {
            return .{};
        }

        const original = try std.posix.tcgetattr(std.posix.STDIN_FILENO);
        var raw = original;
        raw.lflag.ECHO = false;
        raw.lflag.ICANON = false;
        raw.lflag.IEXTEN = false;
        // Keep ISIG enabled so Ctrl-C still behaves as expected.
        raw.iflag.ICRNL = false;
        raw.iflag.IXON = false;
        raw.cc[@intFromEnum(std.posix.V.MIN)] = 1;
        raw.cc[@intFromEnum(std.posix.V.TIME)] = 0;

        try std.posix.tcsetattr(std.posix.STDIN_FILENO, .FLUSH, raw);
        return .{
            .enabled = true,
            .original = original,
        };
    }

    fn restore(self: *RawTerminalMode) void {
        if (!self.enabled) return;
        std.posix.tcsetattr(std.posix.STDIN_FILENO, .FLUSH, self.original) catch {};
        self.enabled = false;
    }
};

fn readByte(file: std.fs.File) !?u8 {
    var buf: [1]u8 = undefined;
    const n = try file.read(&buf);
    if (n == 0) return null;
    return buf[0];
}

fn setLineBuffer(line_buf: *ManagedArrayList(u8), value: []const u8) !void {
    line_buf.items.len = 0;
    try line_buf.appendSlice(value);
}

fn refreshInputLine(stdout: anytype, prompt: []const u8, line: []const u8, cursor: usize) !void {
    try stdout.writeAll("\r");
    try stdout.writeAll(prompt);
    try stdout.writeAll(line);
    try stdout.writeAll("\x1b[K");

    if (cursor <= line.len) {
        const move_left = line.len - cursor;
        if (move_left > 0) {
            try stdout.print("\x1b[{d}D", .{move_left});
        }
    }
}

fn moveCursorLeft(stdout: anytype, count: usize) !void {
    if (count == 0) return;
    try stdout.print("\x1b[{d}D", .{count});
}

fn moveCursorRight(stdout: anytype, count: usize) !void {
    if (count == 0) return;
    try stdout.print("\x1b[{d}C", .{count});
}

/// REPL state and configuration
pub const Repl = struct {
    allocator: std.mem.Allocator,
    db: *Database,
    format: OutputFormat,
    show_timing: bool,
    running: bool,
    history: History,
    multiline_enabled: bool,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, db: *Database, initial_format: OutputFormat) Self {
        // Try to load history from default path
        const history = if (History.getDefaultPath(allocator)) |path| blk: {
            defer allocator.free(path);
            break :blk History.initWithFile(allocator, path);
        } else History.init(allocator);

        return Self{
            .allocator = allocator,
            .db = db,
            .format = initial_format,
            .show_timing = false,
            .running = true,
            .history = history,
            .multiline_enabled = true,
        };
    }

    pub fn deinit(self: *Self) void {
        self.history.save() catch {};
        self.history.deinit();
    }

    /// Run the REPL loop
    pub fn run(self: *Self, stdout: anytype, stderr: anytype) !void {
        defer self.deinit();

        // Print welcome message
        try stdout.print("LatticeDB v{s}\n", .{lattice.VERSION});
        try stdout.print("Connected to: {s}\n", .{self.db.getPath()});
        try stdout.writeAll("Type .help for help, .exit to quit\n\n");

        var line_buf = ManagedArrayList(u8).init(self.allocator);
        defer line_buf.deinit();

        var query_buf = ManagedArrayList(u8).init(self.allocator);
        defer query_buf.deinit();

        var in_multiline = false;

        while (self.running) {
            const action = try self.readLine(in_multiline, &line_buf, stdout);
            switch (action) {
                .eof => {
                    try stdout.writeAll("\nGoodbye!\n");
                    break;
                },
                .canceled => {
                    // Ctrl-G cancels the current in-progress input/query.
                    query_buf.items.len = 0;
                    in_multiline = false;
                    continue;
                },
                .line => {},
            }

            const line = std.mem.trim(u8, line_buf.items, " \t\r\n");

            // Empty line in multiline mode continues, otherwise skip
            if (line.len == 0) {
                if (in_multiline) {
                    // Empty line in multiline - execute what we have
                    if (query_buf.items.len > 0) {
                        const query = std.mem.trim(u8, query_buf.items, " \t\r\n");
                        if (query.len > 0) {
                            self.history.add(query) catch {};
                            self.executeQuery(query, stdout, stderr);
                        }
                        query_buf.items.len = 0;
                    }
                    in_multiline = false;
                }
                continue;
            }

            // Handle dot commands (only at start of input, not in multiline)
            if (!in_multiline and line[0] == '.') {
                self.handleDotCommand(line, stdout, stderr) catch |err| {
                    output.printError(stderr, "Command failed: {s}", .{@errorName(err)});
                };
                continue;
            }

            // Accumulate query
            if (in_multiline and query_buf.items.len > 0) {
                try query_buf.append(' ');
            }
            try query_buf.appendSlice(line);

            // Check if query is complete
            if (self.multiline_enabled and !isQueryComplete(query_buf.items)) {
                in_multiline = true;
                continue;
            }

            // Execute complete query
            const query = std.mem.trim(u8, query_buf.items, " \t\r\n");
            if (query.len > 0) {
                self.history.add(query) catch {};
                self.executeQuery(query, stdout, stderr);
            }

            query_buf.items.len = 0;
            in_multiline = false;
        }
    }

    fn promptFor(in_multiline: bool) []const u8 {
        return if (in_multiline) "     ...> " else "lattice> ";
    }

    fn readLine(self: *Self, in_multiline: bool, line_buf: *ManagedArrayList(u8), stdout: anytype) !LineReadAction {
        const prompt = promptFor(in_multiline);
        line_buf.items.len = 0;

        if (!std.posix.isatty(std.posix.STDIN_FILENO)) {
            const stdin = std.fs.File.stdin().deprecatedReader();
            try stdout.writeAll(prompt);
            stdin.readUntilDelimiterArrayList(line_buf, '\n', 65536) catch |err| {
                if (err == error.EndOfStream) return .eof;
                return err;
            };
            return .line;
        }

        return self.readLineInteractive(prompt, line_buf, stdout);
    }

    fn readLineInteractive(self: *Self, prompt: []const u8, line_buf: *ManagedArrayList(u8), stdout: anytype) !LineReadAction {
        var raw_mode = try RawTerminalMode.enableIfTty();
        defer raw_mode.restore();

        const stdin_file = std.fs.File.stdin();
        try stdout.writeAll(prompt);

        self.history.resetPosition();
        var cursor: usize = 0;
        var history_scratch: ?[]u8 = null;
        var browsing_history = false;
        defer if (history_scratch) |scratch| self.allocator.free(scratch);

        while (true) {
            const key = (try readByte(stdin_file)) orelse return .eof;
            switch (key) {
                '\r', '\n' => {
                    try stdout.writeAll("\r\n");
                    return .line;
                },
                7 => { // Ctrl-G: cancel current line
                    line_buf.items.len = 0;
                    try refreshInputLine(stdout, prompt, line_buf.items, 0);
                    try stdout.writeAll("\r\n");
                    return .canceled;
                },
                4 => { // Ctrl-D: EOF on empty input, otherwise delete under cursor
                    if (line_buf.items.len == 0) return .eof;
                    if (cursor < line_buf.items.len) {
                        _ = line_buf.orderedRemove(cursor);
                        try refreshInputLine(stdout, prompt, line_buf.items, cursor);
                    }
                },
                1 => { // Ctrl-A
                    if (cursor > 0) {
                        try moveCursorLeft(stdout, cursor);
                        cursor = 0;
                    }
                },
                5 => { // Ctrl-E
                    if (cursor < line_buf.items.len) {
                        try moveCursorRight(stdout, line_buf.items.len - cursor);
                        cursor = line_buf.items.len;
                    }
                },
                8, 127 => { // Backspace
                    if (cursor > 0) {
                        _ = line_buf.orderedRemove(cursor - 1);
                        cursor -= 1;
                        if (cursor == line_buf.items.len) {
                            try stdout.writeByte('\x08');
                            try stdout.writeAll("\x1b[K");
                        } else {
                            try refreshInputLine(stdout, prompt, line_buf.items, cursor);
                        }
                    }
                },
                27 => { // Escape sequence
                    const b1 = (try readByte(stdin_file)) orelse continue;
                    if (b1 != '[') continue;
                    const b2 = (try readByte(stdin_file)) orelse continue;
                    switch (b2) {
                        'A' => { // Up: previous history entry
                            if (!browsing_history) {
                                history_scratch = try self.allocator.dupe(u8, line_buf.items);
                                browsing_history = true;
                            }
                            if (self.history.previous()) |entry| {
                                try setLineBuffer(line_buf, entry);
                                cursor = line_buf.items.len;
                                try refreshInputLine(stdout, prompt, line_buf.items, cursor);
                            }
                        },
                        'B' => { // Down: next history entry
                            if (browsing_history) {
                                if (self.history.next()) |entry| {
                                    try setLineBuffer(line_buf, entry);
                                } else if (history_scratch) |scratch| {
                                    try setLineBuffer(line_buf, scratch);
                                    browsing_history = false;
                                } else {
                                    line_buf.items.len = 0;
                                    browsing_history = false;
                                }
                                cursor = line_buf.items.len;
                                try refreshInputLine(stdout, prompt, line_buf.items, cursor);
                            }
                        },
                        'C' => { // Right
                            if (cursor < line_buf.items.len) {
                                cursor += 1;
                                try moveCursorRight(stdout, 1);
                            }
                        },
                        'D' => { // Left
                            if (cursor > 0) {
                                cursor -= 1;
                                try moveCursorLeft(stdout, 1);
                            }
                        },
                        '3' => { // Delete: ESC [ 3 ~
                            const b3 = (try readByte(stdin_file)) orelse continue;
                            if (b3 == '~' and cursor < line_buf.items.len) {
                                _ = line_buf.orderedRemove(cursor);
                                try refreshInputLine(stdout, prompt, line_buf.items, cursor);
                            }
                        },
                        else => {},
                    }
                },
                else => {
                    if (key < 32) continue;
                    if (cursor == line_buf.items.len) {
                        try line_buf.append(key);
                        cursor += 1;
                        try stdout.writeByte(key);
                    } else {
                        try line_buf.insert(cursor, key);
                        cursor += 1;
                        try refreshInputLine(stdout, prompt, line_buf.items, cursor);
                    }
                },
            }
        }
    }

    /// Check if a query is complete (ends with semicolon and balanced brackets)
    fn isQueryComplete(query: []const u8) bool {
        const trimmed = std.mem.trim(u8, query, " \t\r\n");
        if (trimmed.len == 0) return true;

        // Check for balanced brackets
        var paren_count: i32 = 0;
        var bracket_count: i32 = 0;
        var brace_count: i32 = 0;
        var in_string = false;
        var escape_next = false;

        for (trimmed) |c| {
            if (escape_next) {
                escape_next = false;
                continue;
            }

            if (c == '\\') {
                escape_next = true;
                continue;
            }

            if (c == '"' or c == '\'') {
                in_string = !in_string;
                continue;
            }

            if (in_string) continue;

            switch (c) {
                '(' => paren_count += 1,
                ')' => paren_count -= 1,
                '[' => bracket_count += 1,
                ']' => bracket_count -= 1,
                '{' => brace_count += 1,
                '}' => brace_count -= 1,
                else => {},
            }
        }

        // Unbalanced brackets means incomplete
        if (paren_count != 0 or bracket_count != 0 or brace_count != 0) {
            return false;
        }

        // Query ending with semicolon is complete
        if (trimmed[trimmed.len - 1] == ';') {
            return true;
        }

        // Queries without semicolon are also accepted (single line)
        // But keywords that typically continue might indicate incomplete
        const upper = std.ascii.allocUpperString(std.heap.page_allocator, trimmed) catch return true;
        defer std.heap.page_allocator.free(upper);

        // If ends with certain keywords, probably incomplete
        const incomplete_endings = [_][]const u8{
            "WHERE",
            "AND",
            "OR",
            "SET",
            "WITH",
            "ORDER BY",
            "RETURN",
            "MATCH",
            "CREATE",
            "MERGE",
            "DELETE",
            "REMOVE",
        };

        for (incomplete_endings) |ending| {
            if (std.mem.endsWith(u8, upper, ending)) {
                return false;
            }
        }

        return true;
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
            try self.showSchema(stdout, stderr);
        } else if (std.mem.eql(u8, command, ".count")) {
            try self.showCounts(stdout);
        } else if (std.mem.eql(u8, command, ".clear")) {
            // Clear screen (ANSI escape)
            try stdout.writeAll("\x1b[2J\x1b[H");
        } else if (std.mem.eql(u8, command, ".history")) {
            try self.showHistory(stdout, parts.next());
        } else if (std.mem.eql(u8, command, ".multiline")) {
            const arg = parts.next();
            if (arg) |a| {
                if (std.mem.eql(u8, a, "on")) {
                    self.multiline_enabled = true;
                    try stdout.writeAll("Multi-line input enabled\n");
                } else if (std.mem.eql(u8, a, "off")) {
                    self.multiline_enabled = false;
                    try stdout.writeAll("Multi-line input disabled\n");
                } else {
                    output.printError(stderr, "Usage: .multiline on|off", .{});
                }
            } else {
                try stdout.print("Multi-line: {s}\n", .{if (self.multiline_enabled) "on" else "off"});
            }
        } else {
            output.printError(stderr, "Unknown command: {s}. Type .help for help.", .{command});
        }
    }

    fn showHistory(self: *Self, stdout: anytype, count_arg: ?[]const u8) !void {
        const max_show: usize = if (count_arg) |arg|
            std.fmt.parseInt(usize, arg, 10) catch 20
        else
            20;

        const total = self.history.count();
        if (total == 0) {
            try stdout.writeAll("No history\n");
            return;
        }

        const start = if (total > max_show) total - max_show else 0;
        for (self.history.entries.items[start..], start..) |entry, i| {
            try stdout.print("{d:>4}  {s}\n", .{ i + 1, entry });
        }
        try stdout.print("({d} entries total)\n", .{total});
    }

    /// Execute a Cypher query and display results
    fn executeQuery(self: *Self, query_str: []const u8, stdout: anytype, stderr: anytype) void {
        const start_time = std.time.nanoTimestamp();

        var detailed = self.db.queryDetailed(query_str) catch |err| {
            const err_msg = switch (err) {
                error.OutOfMemory => "Out of memory",
                error.ParseError => "Parse error: invalid Cypher syntax",
                error.SemanticError => "Semantic error: invalid query structure",
                error.PlanError => "Plan error: could not create execution plan",
                error.ExecutionError => "Execution error: query failed",
            };
            output.printError(stderr, "{s}", .{err_msg});
            return;
        };
        if (detailed == .failure) {
            output.printQueryFailure(stderr, query_str, detailed.failure);
            detailed.failure.deinit();
            return;
        }
        defer detailed.success.deinit();

        const end_time = std.time.nanoTimestamp();
        const elapsed_ns = @as(u64, @intCast(end_time - start_time));
        const elapsed_ms = @as(f64, @floatFromInt(elapsed_ns)) / 1_000_000.0;

        // Display results based on format
        self.displayResult(&detailed.success, stdout, elapsed_ms) catch |err| {
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
            .edge_id => 12, // "Edge(xxxxx)"
            .bytes_val => |b| 10 + countDigits(b.len), // "<bytes:N>"
            .vector_val => |v| 10 + countDigits(v.len), // "<vector:N>"
            .list_val => |list| blk: {
                if (list.len == 0) break :blk 2; // "[]"
                var len: usize = 2; // brackets
                for (list, 0..) |item, i| {
                    if (i > 0) len += 2; // ", "
                    len += self.valueDisplayLen(item);
                }
                break :blk len;
            },
            .map_val => |map| blk: {
                if (map.len == 0) break :blk 2; // "{}"
                var len: usize = 2; // braces
                for (map, 0..) |entry, i| {
                    if (i > 0) len += 2; // ", "
                    len += entry.key.len + 2; // "key: "
                    len += self.valueDisplayLen(entry.value);
                }
                break :blk len;
            },
        };
    }

    fn writeValue(self: *Self, writer: anytype, val: ResultValue) !usize {
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
            .edge_id => |id| {
                var buf: [32]u8 = undefined;
                const slice = std.fmt.bufPrint(&buf, "Edge({d})", .{id}) catch return 0;
                try writer.writeAll(slice);
                return slice.len;
            },
            .bytes_val => |b| {
                var buf: [32]u8 = undefined;
                const slice = std.fmt.bufPrint(&buf, "<bytes:{d}>", .{b.len}) catch return 0;
                try writer.writeAll(slice);
                return slice.len;
            },
            .vector_val => |v| {
                var buf: [32]u8 = undefined;
                const slice = std.fmt.bufPrint(&buf, "<vector:{d}>", .{v.len}) catch return 0;
                try writer.writeAll(slice);
                return slice.len;
            },
            .list_val => |list| {
                var total: usize = 0;
                try writer.writeByte('[');
                total += 1;
                for (list, 0..) |item, i| {
                    if (i > 0) {
                        try writer.writeAll(", ");
                        total += 2;
                    }
                    total += try self.writeValue(writer, item);
                }
                try writer.writeByte(']');
                total += 1;
                return total;
            },
            .map_val => |map| {
                var total: usize = 0;
                try writer.writeByte('{');
                total += 1;
                for (map, 0..) |entry, i| {
                    if (i > 0) {
                        try writer.writeAll(", ");
                        total += 2;
                    }
                    try writer.writeAll(entry.key);
                    try writer.writeAll(": ");
                    total += entry.key.len + 2;
                    total += try self.writeValue(writer, entry.value);
                }
                try writer.writeByte('}');
                total += 1;
                return total;
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
        const labels = self.db.getAllLabels() catch |err| {
            output.printError(stderr, "Failed to get labels: {s}", .{@errorName(err)});
            return;
        };
        defer self.db.freeLabelInfos(labels);

        if (labels.len == 0) {
            try stdout.writeAll("No labels found\n");
            return;
        }

        try stdout.writeAll("Labels:\n");
        for (labels) |info| {
            try stdout.print("  {s} ({d} node{s})\n", .{
                info.name,
                info.count,
                if (info.count == 1) "" else "s",
            });
        }
        try stdout.print("{d} label{s} total\n", .{
            labels.len,
            if (labels.len == 1) "" else "s",
        });
    }

    fn showEdgeTypes(self: *Self, stdout: anytype, stderr: anytype) !void {
        const types = self.db.getAllEdgeTypes() catch |err| {
            output.printError(stderr, "Failed to get edge types: {s}", .{@errorName(err)});
            return;
        };
        defer self.db.freeEdgeTypeInfos(types);

        if (types.len == 0) {
            try stdout.writeAll("No edge types found\n");
            return;
        }

        try stdout.writeAll("Edge Types:\n");
        for (types) |info| {
            try stdout.print("  {s} ({d} edge{s})\n", .{
                info.name,
                info.count,
                if (info.count == 1) "" else "s",
            });
        }
        try stdout.print("{d} edge type{s} total\n", .{
            types.len,
            if (types.len == 1) "" else "s",
        });
    }

    fn showSchema(self: *Self, stdout: anytype, stderr: anytype) !void {
        const labels = self.db.getAllLabels() catch |err| {
            output.printError(stderr, "Failed to get labels: {s}", .{@errorName(err)});
            return;
        };
        defer self.db.freeLabelInfos(labels);

        const edge_types = self.db.getAllEdgeTypes() catch |err| {
            output.printError(stderr, "Failed to get edge types: {s}", .{@errorName(err)});
            return;
        };
        defer self.db.freeEdgeTypeInfos(edge_types);

        try stdout.writeAll("Schema\n");
        try stdout.writeAll("══════\n\n");

        if (labels.len == 0) {
            try stdout.writeAll("No node labels defined\n\n");
        } else {
            try stdout.writeAll("Node Labels:\n");
            for (labels) |info| {
                try stdout.print("  (:{s}) - {d} node(s)\n", .{ info.name, info.count });
            }
            try stdout.writeAll("\n");
        }

        if (edge_types.len == 0) {
            try stdout.writeAll("No edge types defined\n");
        } else {
            try stdout.writeAll("Edge Types:\n");
            for (edge_types) |info| {
                try stdout.print("  [:{s}] - {d} edge(s)\n", .{ info.name, info.count });
            }
        }

        try stdout.writeAll("\n");
        try stdout.print("Total: {d} label(s), {d} edge type(s)\n", .{ labels.len, edge_types.len });
    }

    fn showCounts(self: *Self, stdout: anytype) !void {
        const node_count = self.db.nodeCount();
        const edge_count = self.db.edgeCount();

        const labels = self.db.getAllLabels() catch &[_]Database.LabelInfo{};
        defer if (labels.len > 0) self.db.freeLabelInfos(@constCast(labels));

        const edge_types = self.db.getAllEdgeTypes() catch &[_]Database.EdgeTypeInfo{};
        defer if (edge_types.len > 0) self.db.freeEdgeTypeInfos(@constCast(edge_types));

        try stdout.print("Nodes:      {d}\n", .{node_count});
        try stdout.print("Edges:      {d}\n", .{edge_count});
        try stdout.print("Labels:     {d}\n", .{labels.len});
        try stdout.print("Edge Types: {d}\n", .{edge_types.len});
    }

    fn printReplHelp(self: *Self, stdout: anytype) !void {
        _ = self;
        try stdout.writeAll(
            \\REPL Commands:
            \\  .help, .h           Show this help message
            \\  .exit, .quit, .q    Exit the REPL
            \\  .format <fmt>       Set output format (table, json, csv)
            \\  .timing on|off      Toggle query timing display
            \\  .multiline on|off   Toggle multi-line input mode
            \\  .history [n]        Show last n commands (default: 20)
            \\  .labels, .tables    List all node labels
            \\  .types              List all edge types
            \\  .schema             Show inferred schema
            \\  .count              Show node/edge counts
            \\  .clear              Clear the screen
            \\
            \\Multi-line Input:
            \\  Queries with unclosed brackets continue on the next line.
            \\  Enter an empty line to execute a multi-line query.
            \\  Ctrl-G cancels the current input line.
            \\
            \\Line Editing:
            \\  Up/Down arrows      Browse command history
            \\  Left/Right arrows   Move cursor within the current line
            \\  Backspace/Delete    Remove characters
            \\  Ctrl-A / Ctrl-E     Jump to start/end of line
            \\
            \\Cypher Examples:
            \\  CREATE (n:Person {name: "Alice"})
            \\  MATCH (n:Person) RETURN n.name
            \\  MATCH (a)-[r]->(b) RETURN a, r, b LIMIT 10
            \\
        );
    }
};

/// Count decimal digits in a number
fn countDigits(n: usize) usize {
    if (n == 0) return 1;
    var count: usize = 0;
    var val = n;
    while (val > 0) : (val /= 10) count += 1;
    return count;
}

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
        .edge_id => |id| try writer.print("{d}", .{id}),
        .bytes_val => |b| {
            // Output as base64-encoded string in JSON
            try writer.print("\"<bytes:{d}>\"", .{b.len});
        },
        .vector_val => |v| {
            try writer.writeByte('[');
            for (v, 0..) |f, i| {
                if (i > 0) try writer.writeByte(',');
                try writer.print("{d}", .{f});
            }
            try writer.writeByte(']');
        },
        .list_val => |list| {
            try writer.writeByte('[');
            for (list, 0..) |item, i| {
                if (i > 0) try writer.writeByte(',');
                try writeJsonValue(writer, item);
            }
            try writer.writeByte(']');
        },
        .map_val => |map| {
            try writer.writeByte('{');
            for (map, 0..) |entry, i| {
                if (i > 0) try writer.writeByte(',');
                try writer.print("\"{s}\":", .{entry.key});
                try writeJsonValue(writer, entry.value);
            }
            try writer.writeByte('}');
        },
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
        .edge_id => |id| try writer.print("{d}", .{id}),
        .bytes_val => |b| try writer.print("<bytes:{d}>", .{b.len}),
        .vector_val => |v| try writer.print("<vector:{d}>", .{v.len}),
        .list_val, .map_val => {
            // For CSV, output complex types as quoted JSON
            try writer.writeByte('"');
            try writeJsonValue(writer, val);
            try writer.writeByte('"');
        },
    }
}

test "repl initialization" {
    // Basic smoke test - just verify the struct can be created
    const allocator = std.testing.allocator;
    _ = allocator;
    // Can't easily test full REPL without a database, but struct creation works
}
