//! Command history for the LatticeDB REPL.
//!
//! Provides in-memory history with optional file persistence.

const std = @import("std");
const builtin = @import("builtin");

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

        const file = @import("compat").fs.cwd().openFile(path, .{}) catch |err| {
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

        const file = @import("compat").fs.cwd().createFile(path, .{}) catch return;
        defer file.close();

        const writer = file.deprecatedWriter();
        for (self.entries.items) |entry| {
            writer.writeAll(entry) catch continue;
            writer.writeByte('\n') catch continue;
        }
    }

    /// Get the default history file path
    /// Where the history file lives.
    ///
    /// POSIX puts per-user state in a dotfile under $HOME. Windows has no HOME
    /// and no dotfile convention: per-user application data belongs under
    /// %APPDATA%, with %USERPROFILE% as the fallback for setups that lack it.
    /// Getting this wrong is quiet — history simply stops persisting, with
    /// nothing printed — so it is worth being explicit about rather than
    /// letting the POSIX path fail to resolve.
    pub fn getDefaultPath(allocator: std.mem.Allocator) ?[]const u8 {
        const dir = homeDirectory(allocator) orelse return null;
        defer allocator.free(dir);

        const sep = std.fs.path.sep_str;
        const name = if (builtin.os.tag == .windows) "lattice_history" else ".lattice_history";
        return std.fmt.allocPrint(allocator, "{s}{s}{s}", .{ dir, sep, name }) catch null;
    }

    /// The directory that per-user state belongs in. Caller owns the result.
    fn homeDirectory(allocator: std.mem.Allocator) ?[]const u8 {
        if (builtin.os.tag == .windows) {
            return windowsEnv(allocator, "APPDATA") orelse windowsEnv(allocator, "USERPROFILE");
        }

        const home = if (@hasDecl(std.posix, "getenv"))
            std.posix.getenv("HOME")
        else if (@hasDecl(std, "c"))
            if (std.c.getenv("HOME")) |home_ptr| std.mem.span(home_ptr) else null
        else
            null;

        const path = home orelse return null;
        return allocator.dupe(u8, path) catch null;
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

/// Read an environment variable on Windows.
///
/// `std.posix.getenv` is not available there, and the environment is UTF-16, so
/// this goes to the API directly and converts. WTF-8 rather than UTF-8 on the way
/// out, because Windows permits unpaired surrogates in these values and refusing
/// them would be worse than carrying them.
fn windowsEnv(allocator: std.mem.Allocator, comptime key: []const u8) ?[]const u8 {
    if (builtin.os.tag != .windows) return null;

    const key_w = std.unicode.utf8ToUtf16LeStringLiteral(key);
    var buf: [std.fs.max_path_bytes]u16 = undefined;
    const len = GetEnvironmentVariableW(key_w, &buf, buf.len);
    // Zero means unset or unreadable; a length at or past the buffer means the
    // value is longer than any path we would build from it.
    if (len == 0 or len >= buf.len) return null;

    return std.unicode.wtf16LeToWtf8Alloc(allocator, buf[0..len]) catch null;
}

extern "kernel32" fn GetEnvironmentVariableW(
    lpName: [*:0]const u16,
    lpBuffer: [*]u16,
    nSize: u32,
) callconv(.winapi) u32;
