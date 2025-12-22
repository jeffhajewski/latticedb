//! Lattice CLI - Command-line interface for Lattice database.
//!
//! Provides interactive and batch operations for database management.

const std = @import("std");
const lattice = @import("lattice");

const usage =
    \\Usage: lattice [command] [options]
    \\
    \\Commands:
    \\  create <path>     Create a new database
    \\  info <path>       Show database information
    \\  query <path>      Run interactive query shell
    \\  version           Show version information
    \\  help              Show this help message
    \\
    \\Options:
    \\  -h, --help        Show help for a command
    \\  -v, --version     Show version
    \\
;

pub fn main() !void {
    const allocator = std.heap.page_allocator;
    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    if (args.len < 2) {
        printUsage();
        return;
    }

    const command = args[1];

    if (std.mem.eql(u8, command, "version") or std.mem.eql(u8, command, "-v") or std.mem.eql(u8, command, "--version")) {
        printVersion();
    } else if (std.mem.eql(u8, command, "help") or std.mem.eql(u8, command, "-h") or std.mem.eql(u8, command, "--help")) {
        printUsage();
    } else if (std.mem.eql(u8, command, "create")) {
        if (args.len < 3) {
            std.debug.print("Error: missing database path\n", .{});
            return error.InvalidArgument;
        }
        createDatabase(args[2]);
    } else if (std.mem.eql(u8, command, "info")) {
        if (args.len < 3) {
            std.debug.print("Error: missing database path\n", .{});
            return error.InvalidArgument;
        }
        showInfo(args[2]);
    } else if (std.mem.eql(u8, command, "query")) {
        if (args.len < 3) {
            std.debug.print("Error: missing database path\n", .{});
            return error.InvalidArgument;
        }
        runQueryShell(args[2]);
    } else {
        std.debug.print("Unknown command: {s}\n", .{command});
        printUsage();
    }
}

fn printVersion() void {
    std.debug.print("Lattice {s}\n", .{lattice.VERSION});
    std.debug.print("Format version: {d}\n", .{lattice.core.types.FORMAT_VERSION});
}

fn printUsage() void {
    std.debug.print("{s}", .{usage});
}

fn createDatabase(path: []const u8) void {
    std.debug.print("Creating database: {s}\n", .{path});
    std.debug.print("Not yet implemented\n", .{});
}

fn showInfo(path: []const u8) void {
    std.debug.print("Database: {s}\n", .{path});
    std.debug.print("Not yet implemented\n", .{});
}

fn runQueryShell(path: []const u8) void {
    std.debug.print("Opening database: {s}\n", .{path});
    std.debug.print("Interactive query shell not yet implemented\n", .{});
}
