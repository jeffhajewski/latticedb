//! Command history for the LatticeDB REPL.
//!
//! Provides in-memory history with optional file persistence.

const std = @import("std");

/// Managed array list for allocator tracking
fn ManagedArrayList(comptime T: type) type {
    return std.array_list.Managed(T);
}

/// Command history manager
pub const History = struct {
    allocator: std.mem.Allocator,
    entries: ManagedArrayList([]const u8),
    position: usize,
    max_entries: usize,
    file_path: ?[]const u8,

    const Self = @This();
    const DEFAULT_MAX_ENTRIES = 1000;

    /// Initialize history
    pub fn init(allocator: std.mem.Allocator) Self {
        return Self{
            .allocator = allocator,
            .entries = ManagedArrayList([]const u8).init(allocator),
            .position = 0,
            .max_entries = DEFAULT_MAX_ENTRIES,
            .file_path = null,
        };
    }

    /// Initialize with file persistence
    pub fn initWithFile(allocator: std.mem.Allocator, path: []const u8) Self {
        var self = init(allocator);
        self.file_path = allocator.dupe(u8, path) catch null;
        self.load() catch {};
        return self;
    }

    /// Deinitialize and free resources
    pub fn deinit(self: *Self) void {
        for (self.entries.items) |entry| {
            self.allocator.free(entry);
        }
        self.entries.deinit();
        if (self.file_path) |path| {
            self.allocator.free(path);
        }
    }

    /// Add a command to history
    pub fn add(self: *Self, command: []const u8) !void {
        // Don't add empty commands or duplicates of the last entry
        if (command.len == 0) return;
        if (self.entries.items.len > 0) {
            const last = self.entries.items[self.entries.items.len - 1];
            if (std.mem.eql(u8, last, command)) return;
        }

        // Copy the command
        const entry = try self.allocator.dupe(u8, command);
        errdefer self.allocator.free(entry);

        // Remove oldest entry if at capacity
        if (self.entries.items.len >= self.max_entries) {
            const removed = self.entries.orderedRemove(0);
            self.allocator.free(removed);
        }

        try self.entries.append(entry);
        self.position = self.entries.items.len;
    }

    /// Get previous entry (up arrow)
    pub fn previous(self: *Self) ?[]const u8 {
        if (self.entries.items.len == 0) return null;
        if (self.position > 0) {
            self.position -= 1;
        }
        return self.entries.items[self.position];
    }

    /// Get next entry (down arrow)
    pub fn next(self: *Self) ?[]const u8 {
        if (self.entries.items.len == 0) return null;
        if (self.position < self.entries.items.len - 1) {
            self.position += 1;
            return self.entries.items[self.position];
        }
        self.position = self.entries.items.len;
        return null; // Return to empty line
    }

    /// Reset position to end (for new input)
    pub fn resetPosition(self: *Self) void {
        self.position = self.entries.items.len;
    }

    /// Get entry count
    pub fn count(self: *const Self) usize {
        return self.entries.items.len;
    }

    /// Load history from file
    pub fn load(self: *Self) !void {
        const path = self.file_path orelse return;

        const file = std.fs.cwd().openFile(path, .{}) catch |err| {
            if (err == error.FileNotFound) return;
            return err;
        };
        defer file.close();

        const content = file.readToEndAlloc(self.allocator, 10 * 1024 * 1024) catch return;
        defer self.allocator.free(content);

        var lines = std.mem.splitScalar(u8, content, '\n');
        while (lines.next()) |line| {
            if (line.len > 0) {
                self.add(line) catch continue;
            }
        }
    }

    /// Save history to file
    pub fn save(self: *Self) !void {
        const path = self.file_path orelse return;

        const file = std.fs.cwd().createFile(path, .{}) catch return;
        defer file.close();

        const writer = file.deprecatedWriter();
        for (self.entries.items) |entry| {
            writer.writeAll(entry) catch continue;
            writer.writeByte('\n') catch continue;
        }
    }

    /// Get the default history file path
    pub fn getDefaultPath(allocator: std.mem.Allocator) ?[]const u8 {
        // Try $HOME/.lattice_history
        if (std.posix.getenv("HOME")) |home| {
            return std.fmt.allocPrint(allocator, "{s}/.lattice_history", .{home}) catch null;
        }
        return null;
    }
};

test "history add and retrieve" {
    const allocator = std.testing.allocator;
    var history = History.init(allocator);
    defer history.deinit();

    try history.add("MATCH (n) RETURN n");
    try history.add("CREATE (n:Person)");
    try history.add("MATCH (n) RETURN n"); // Duplicate of first, should be added

    try std.testing.expectEqual(@as(usize, 3), history.count());

    // Navigate backwards
    const prev1 = history.previous();
    try std.testing.expect(prev1 != null);
    try std.testing.expectEqualStrings("MATCH (n) RETURN n", prev1.?);

    const prev2 = history.previous();
    try std.testing.expect(prev2 != null);
    try std.testing.expectEqualStrings("CREATE (n:Person)", prev2.?);
}

test "history empty commands" {
    const allocator = std.testing.allocator;
    var history = History.init(allocator);
    defer history.deinit();

    try history.add("");
    try std.testing.expectEqual(@as(usize, 0), history.count());
}

test "history duplicate suppression" {
    const allocator = std.testing.allocator;
    var history = History.init(allocator);
    defer history.deinit();

    try history.add("MATCH (n) RETURN n");
    try history.add("MATCH (n) RETURN n"); // Immediate duplicate

    try std.testing.expectEqual(@as(usize, 1), history.count());
}
