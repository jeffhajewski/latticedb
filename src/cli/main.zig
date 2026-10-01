//! Lattice CLI - Command-line interface for Lattice database.
//!
//! Provides interactive and batch operations for database management.

const std = @import("std");
const lattice = @import("lattice");
const args_mod = @import("args.zig");
const output = @import("output.zig");
const repl_mod = @import("repl.zig");
const import_export = @import("import_export.zig");

// A file's tests reach the test binary only once something analyzes the file,
// and much of the CLI is generic code no test instantiates, so the files are
// named here. args.zig and key.zig are test roots of their own.
test {
    _ = output;
    _ = repl_mod;
    _ = import_export;
    _ = @import("history.zig");
}

const Args = args_mod.Args;
const Command = args_mod.Command;
const OutputFormat = args_mod.OutputFormat;
const Repl = repl_mod.Repl;

// Database types
const Database = lattice.storage.database.Database;
const DatabaseError = lattice.storage.database.DatabaseError;
const DatabaseConfig = lattice.storage.database.DatabaseConfig;
const OpenOptions = lattice.storage.database.OpenOptions;
const PageHeader = lattice.storage.page.PageHeader;
const PageManager = lattice.storage.page_manager.PageManager;
const PageManagerError = lattice.storage.page_manager.PageManagerError;
const PosixVfs = lattice.storage.vfs.PosixVfs;

pub const VERSION = lattice.VERSION;
const INSTALL_SCRIPT_URL = "https://raw.githubusercontent.com/jeffhajewski/latticedb/main/dist/install.sh";
const UPDATE_SHELL_COMMAND = "set -o pipefail; curl -fsSL " ++ INSTALL_SCRIPT_URL ++ " | LATTICE_INSTALL_MODE=update bash";

const CliError = error{
    CommandFailed,
};

const CheckError = error{
    FileNotFound,
    PermissionDenied,
    InvalidDatabase,
    ChecksumMismatch,
    IoError,
    OutOfMemory,
    DatabaseLocked,
};

const CheckStats = struct {
    pages_checked: u32,
    wal_present: bool,
};

fn failCommand(stderr: anytype, comptime fmt: []const u8, args: anytype) CliError {
    output.printError(stderr, fmt, args);
    return error.CommandFailed;
}

pub fn main(init: std.process.Init) !u8 {
    const allocator = init.gpa;
    const argv = try collectProcessArgs(allocator, init.minimal.args);
    defer freeProcessArgs(allocator, argv);

    var stdout_buffer: [4096]u8 = undefined;
    var stdout_writer = std.Io.File.stdout().writer(init.io, &stdout_buffer);
    const stdout = &stdout_writer.interface;
    defer stdout.flush() catch {};

    var stderr_buffer: [4096]u8 = undefined;
    var stderr_writer = std.Io.File.stderr().writer(init.io, &stderr_buffer);
    const stderr = &stderr_writer.interface;
    defer stderr.flush() catch {};

    var parsed_args = Args.parse(allocator, argv) catch |err| {
        switch (err) {
            error.UnknownOption => output.printError(stderr, "Unknown option. Use 'lattice help' for available options.", .{}),
            error.InvalidFormat => output.printError(stderr, "Invalid format. Use: table, json, or csv", .{}),
            error.InvalidVectorDims => output.printError(stderr, "Invalid vector dimensions. Must be in the range 1..4096.", .{}),
            error.InvalidCacheSize => output.printError(stderr, "Invalid cache size. Must be a positive integer.", .{}),
            error.InvalidPageSize => output.printError(stderr, "Invalid page size. Must be in the range 4096..65535 bytes.", .{}),
            error.InvalidBatchSize => output.printError(stderr, "Invalid batch size. Must be a positive integer.", .{}),
            error.InvalidInterval => output.printError(stderr, "Invalid interval. Must be a whole number of seconds, at least 1.", .{}),
            error.InvalidTimestamp => output.printError(stderr, "Invalid time. Use a UTC time such as 2026-08-25T14:30:00Z or a date such as 2026-08-25.", .{}),
            else => output.printError(stderr, "Failed to parse arguments", .{}),
        }
        stderr.flush() catch {};
        return 1;
    };
    defer parsed_args.deinit(allocator);

    // Handle help flag
    if (parsed_args.help_requested) {
        if (parsed_args.command) |cmd| {
            printCommandHelp(stdout, cmd);
        } else {
            printUsage(stdout);
        }
        return 0;
    }

    // No command provided
    if (parsed_args.command == null) {
        printUsage(stdout);
        return 0;
    }

    const command = parsed_args.command.?;

    // Check for required path
    if (command.requiresPath() and parsed_args.path == null) {
        output.printError(stderr, "Missing database path", .{});
        try stderr.print("Usage: lattice {s} <path>\n", .{@tagName(command)});
        stderr.flush() catch {};
        return 1;
    }

    // Dispatch to command handler
    runCommand(init.io, allocator, stdout, stderr, command, &parsed_args) catch |err| {
        if (err != error.CommandFailed) {
            output.printError(stderr, "Command failed: {s}", .{@errorName(err)});
        }
        stderr.flush() catch {};
        return 1;
    };

    return 0;
}

fn collectProcessArgs(allocator: std.mem.Allocator, process_args: std.process.Args) ![]const []const u8 {
    var iter = try std.process.Args.Iterator.initAllocator(process_args, allocator);
    defer iter.deinit();

    var argv: std.ArrayList([]const u8) = .empty;
    errdefer {
        for (argv.items) |arg| {
            allocator.free(@constCast(arg));
        }
        argv.deinit(allocator);
    }

    while (iter.next()) |arg| {
        const owned = try allocator.dupe(u8, arg);
        errdefer allocator.free(owned);
        try argv.append(allocator, owned);
    }

    return argv.toOwnedSlice(allocator);
}

fn freeProcessArgs(allocator: std.mem.Allocator, argv: []const []const u8) void {
    for (argv) |arg| {
        allocator.free(@constCast(arg));
    }
    allocator.free(argv);
}

fn runCommand(
    io: std.Io,
    allocator: std.mem.Allocator,
    stdout: anytype,
    stderr: anytype,
    command: Command,
    parsed_args: *const Args,
) !void {
    switch (command) {
        .update => try cmdUpdate(io, stderr),
        .version => printVersion(stdout),
        .help => printUsage(stdout),
        .create => try cmdCreate(allocator, stdout, stderr, parsed_args),
        .info => try cmdInfo(allocator, stdout, stderr, parsed_args),
        .compact => try cmdCompact(allocator, stdout, stderr, parsed_args),
        .checkpoint => try cmdCheckpoint(allocator, stdout, stderr, parsed_args),
        .backup => try cmdBackup(allocator, stdout, stderr, parsed_args),
        .replicate => try cmdReplicate(allocator, stdout, stderr, parsed_args),
        .restore => try cmdRestore(allocator, stdout, stderr, parsed_args),
        .count => try cmdCount(allocator, stdout, stderr, parsed_args),
        .query => try cmdQuery(allocator, stdout, stderr, parsed_args),
        .exec => try cmdExec(allocator, stdout, stderr, parsed_args),
        .labels => try cmdLabels(allocator, stdout, stderr, parsed_args),
        .types => try cmdTypes(allocator, stdout, stderr, parsed_args),
        .schema => try cmdSchema(allocator, stdout, stderr, parsed_args),
        .check => try cmdCheck(allocator, stdout, stderr, parsed_args),
        .import => try cmdImport(allocator, stdout, stderr, parsed_args),
        .@"export" => try cmdExport(allocator, stdout, stderr, parsed_args),
        .dump => try cmdDump(allocator, stdout, stderr, parsed_args),
    }
}

// ============================================
// Command Implementations (stubs for now)
// ============================================

fn cmdUpdate(io: std.Io, stderr: anytype) !void {
    try runUpdateShellCommand(io, stderr, UPDATE_SHELL_COMMAND);
}

fn runUpdateShellCommand(io: std.Io, stderr: anytype, shell_command: []const u8) !void {
    var child = std.process.spawn(io, .{
        .argv = &[_][]const u8{ "bash", "-c", shell_command },
    }) catch |err| {
        return failCommand(stderr, "Failed to start updater: {s}", .{@errorName(err)});
    };
    defer child.kill(io);

    const term = child.wait(io) catch |err| {
        return failCommand(stderr, "Failed to wait for updater: {s}", .{@errorName(err)});
    };

    switch (term) {
        .exited => |code| {
            if (code == 0) {
                return;
            }
            return failCommand(stderr, "Updater exited with status {d}", .{code});
        },
        .signal => |sig| return failCommand(stderr, "Updater terminated by signal {d}", .{@intFromEnum(sig)}),
        .stopped => |sig| return failCommand(stderr, "Updater stopped by signal {d}", .{@intFromEnum(sig)}),
        .unknown => |code| return failCommand(stderr, "Updater ended with unknown status {d}", .{code}),
    }
}

test "update runner uses supplied io for process spawning" {
    if (!std.process.can_spawn) return error.SkipZigTest;
    // The updater is a bash pipeline, which a Windows machine has only if
    // something else installed it. Updating there needs a path of its own.
    if (@import("builtin").os.tag == .windows) return error.SkipZigTest;

    var stderr_buf: [256]u8 = undefined;
    var stderr_stream = @import("compat").fixedBufferStream(&stderr_buf);
    try runUpdateShellCommand(std.testing.io, stderr_stream.writer(), "exit 0");
}

fn cmdCreate(
    allocator: std.mem.Allocator,
    stdout: anytype,
    stderr: anytype,
    parsed_args: *const Args,
) !void {
    const path = parsed_args.path.?;

    // Check if file already exists
    if (@import("compat").fs.cwd().access(path, .{})) |_| {
        return failCommand(stderr, "Database already exists: {s}", .{path});
    } else |_| {
        // File doesn't exist, which is what we want for create
    }

    // Configure the database
    const config = DatabaseConfig{
        .enable_vector = parsed_args.enable_vector,
        .vector_dimensions = parsed_args.vector_dims,
        .enable_fts = parsed_args.enable_fts,
        .buffer_pool_size = @as(usize, parsed_args.cache_size_mb) * 1024 * 1024,
    };

    // Create the database
    const db = Database.open(allocator, path, .{
        .create = true,
        .page_size = parsed_args.page_size,
        .config = config,
    }) catch |err| {
        return failCommand(stderr, "Failed to create database: {s}", .{@errorName(err)});
    };
    db.close();

    output.printSuccess(stdout, "Created database: {s}", .{path});

    if (parsed_args.enable_vector) {
        try stdout.print("  Vector index enabled (dimensions: {d})\n", .{parsed_args.vector_dims});
    }
    if (parsed_args.enable_fts) {
        try stdout.writeAll("  Full-text search enabled\n");
    }
}

fn cmdInfo(
    allocator: std.mem.Allocator,
    stdout: anytype,
    stderr: anytype,
    parsed_args: *const Args,
) !void {
    const path = parsed_args.path.?;

    // Open database
    const db = Database.open(allocator, path, .{
        .read_only = true,
        .lock = !parsed_args.no_lock,
    }) catch |err| {
        return failOpen(stderr, path, err);
    };
    defer db.close();

    const node_count = db.nodeCount();
    const edge_count = db.edgeCount();

    // Get file size
    const file = @import("compat").fs.cwd().openFile(path, .{}) catch {
        return failCommand(stderr, "Cannot read file info", .{});
    };
    defer file.close();
    const stat = file.stat() catch {
        return failCommand(stderr, "Cannot stat file", .{});
    };
    const size_kb = stat.size / 1024;

    switch (parsed_args.format) {
        .table => {
            try stdout.print("Database: {s}\n", .{path});
            try stdout.writeAll("─────────────────────────────\n");
            try stdout.print("Size:         {d} KB\n", .{size_kb});
            try stdout.print("Nodes:        {d}\n", .{node_count});
            try stdout.print("Edges:        {d}\n", .{edge_count});
            try stdout.print("Format:       v{d}\n", .{lattice.core.types.FORMAT_VERSION});
            try stdout.print("Vector:       {s}\n", .{if (db.hasPersistedVectorIndex()) "enabled" else "disabled"});
            try stdout.print("FTS:          {s}\n", .{if (db.hasPersistedFtsIndex()) "enabled" else "disabled"});
        },
        .json => {
            try stdout.print("{{\"path\":\"{s}\",\"size_kb\":{d},\"nodes\":{d},\"edges\":{d},\"format_version\":{d},\"vector_enabled\":{},\"fts_enabled\":{}}}\n", .{
                path,
                size_kb,
                node_count,
                edge_count,
                lattice.core.types.FORMAT_VERSION,
                db.hasPersistedVectorIndex(),
                db.hasPersistedFtsIndex(),
            });
        },
        .csv => {
            try stdout.writeAll("property,value\n");
            try stdout.print("path,{s}\n", .{path});
            try stdout.print("size_kb,{d}\n", .{size_kb});
            try stdout.print("nodes,{d}\n", .{node_count});
            try stdout.print("edges,{d}\n", .{edge_count});
            try stdout.print("format_version,{d}\n", .{lattice.core.types.FORMAT_VERSION});
            try stdout.print("vector_enabled,{}\n", .{db.hasPersistedVectorIndex()});
            try stdout.print("fts_enabled,{}\n", .{db.hasPersistedFtsIndex()});
        },
    }
}

fn cmdCount(
    allocator: std.mem.Allocator,
    stdout: anytype,
    stderr: anytype,
    parsed_args: *const Args,
) !void {
    const path = parsed_args.path.?;

    // Open database
    var db = Database.open(allocator, path, .{
        .read_only = true,
        .lock = !parsed_args.no_lock,
    }) catch |err| {
        return failOpen(stderr, path, err);
    };
    defer db.close();

    const node_count = db.nodeCount();
    const edge_count = db.edgeCount();

    // Get label and edge type counts
    const labels = db.getAllLabels() catch &[_]Database.LabelInfo{};
    defer if (labels.len > 0) db.freeLabelInfos(@constCast(labels));

    const edge_types = db.getAllEdgeTypes() catch &[_]Database.EdgeTypeInfo{};
    defer if (edge_types.len > 0) db.freeEdgeTypeInfos(@constCast(edge_types));

    const label_count: u64 = labels.len;
    const edge_type_count: u64 = edge_types.len;

    switch (parsed_args.format) {
        .table => {
            try stdout.writeAll("┌───────────┬────────────┐\n");
            try stdout.writeAll("│ Type      │ Count      │\n");
            try stdout.writeAll("├───────────┼────────────┤\n");
            try stdout.print("│ Nodes     │ {d: >10} │\n", .{node_count});
            try stdout.print("│ Edges     │ {d: >10} │\n", .{edge_count});
            try stdout.print("│ Labels    │ {d: >10} │\n", .{label_count});
            try stdout.print("│ EdgeTypes │ {d: >10} │\n", .{edge_type_count});
            try stdout.writeAll("└───────────┴────────────┘\n");
        },
        .json => {
            try stdout.print("{{\"nodes\":{d},\"edges\":{d},\"labels\":{d},\"edge_types\":{d}}}\n", .{
                node_count,
                edge_count,
                label_count,
                edge_type_count,
            });
        },
        .csv => {
            try stdout.writeAll("type,count\n");
            try stdout.print("nodes,{d}\n", .{node_count});
            try stdout.print("edges,{d}\n", .{edge_count});
            try stdout.print("labels,{d}\n", .{label_count});
            try stdout.print("edge_types,{d}\n", .{edge_type_count});
        },
    }
}

fn cmdQuery(
    allocator: std.mem.Allocator,
    stdout: anytype,
    stderr: anytype,
    parsed_args: *const Args,
) !void {
    const path = parsed_args.path.?;

    // Open database
    const db = Database.open(allocator, path, .{ .lock = !parsed_args.no_lock }) catch |err| {
        return failOpen(stderr, path, err);
    };
    defer db.close();

    // Start REPL
    var repl = Repl.init(allocator, db, parsed_args.format);
    try repl.run(stdout, stderr);
}

fn cmdExec(
    allocator: std.mem.Allocator,
    stdout: anytype,
    stderr: anytype,
    parsed_args: *const Args,
) !void {
    const path = parsed_args.path.?;

    // Get query string from --query or --file
    var query_string: []const u8 = undefined;
    var query_owned = false;

    if (parsed_args.query_string) |qs| {
        query_string = qs;
    } else if (parsed_args.file) |file_path| {
        // Read query from file
        const file = @import("compat").fs.cwd().openFile(file_path, .{}) catch |err| {
            return failCommand(stderr, "Cannot open query file: {s}", .{@errorName(err)});
        };
        defer file.close();

        query_string = file.readToEndAlloc(allocator, 1024 * 1024) catch |err| {
            return failCommand(stderr, "Cannot read query file: {s}", .{@errorName(err)});
        };
        query_owned = true;
    } else {
        return failCommand(stderr, "No query provided. Use --query=\"...\" or --file=<path>", .{});
    }
    defer if (query_owned) allocator.free(query_string);

    // Open database
    const db = Database.open(allocator, path, .{ .lock = !parsed_args.no_lock }) catch |err| {
        return failOpen(stderr, path, err);
    };
    defer db.close();

    var detailed = db.queryDetailed(query_string) catch |err| {
        const err_msg = switch (err) {
            error.OutOfMemory => "Out of memory",
            error.ParseError => "Parse error: invalid Cypher syntax",
            error.SemanticError => "Semantic error: invalid query structure",
            error.PlanError => "Plan error: could not create execution plan",
            error.ExecutionError => "Execution error: query failed",
        };
        return failCommand(stderr, "{s}", .{err_msg});
    };

    if (detailed == .failure) {
        output.printQueryFailure(stderr, query_string, detailed.failure);
        detailed.failure.deinit();
        return error.CommandFailed;
    }

    // Display result using REPL's display logic
    var repl = Repl.init(allocator, db, parsed_args.format);
    defer repl.deinit();
    repl.show_timing = false; // No timing for exec command
    defer detailed.success.deinit();
    repl.displayResult(&detailed.success, stdout, 0) catch |err| {
        return failCommand(stderr, "Failed to display results: {s}", .{@errorName(err)});
    };
}

fn cmdLabels(
    allocator: std.mem.Allocator,
    stdout: anytype,
    stderr: anytype,
    parsed_args: *const Args,
) !void {
    const path = parsed_args.path.?;

    // Open database
    var db = Database.open(allocator, path, .{
        .read_only = true,
        .lock = !parsed_args.no_lock,
    }) catch |err| {
        return failOpen(stderr, path, err);
    };
    defer db.close();

    // Get all labels
    const labels = db.getAllLabels() catch |err| {
        return failCommand(stderr, "Failed to get labels: {s}", .{@errorName(err)});
    };
    defer db.freeLabelInfos(labels);

    switch (parsed_args.format) {
        .table => {
            if (labels.len == 0) {
                try stdout.writeAll("No labels found\n");
                return;
            }

            // Calculate max label width for formatting
            var max_width: usize = 5; // "Label"
            for (labels) |info| {
                if (info.name.len > max_width) max_width = info.name.len;
            }

            // Print table header
            try stdout.writeAll("┌─");
            for (0..max_width) |_| try stdout.writeAll("─");
            try stdout.writeAll("─┬────────────┐\n");

            try stdout.writeAll("│ ");
            try stdout.print("{s}", .{"Label"});
            for (0..max_width - 5) |_| try stdout.writeAll(" ");
            try stdout.writeAll(" │ Count      │\n");

            try stdout.writeAll("├─");
            for (0..max_width) |_| try stdout.writeAll("─");
            try stdout.writeAll("─┼────────────┤\n");

            // Print rows
            for (labels) |info| {
                try stdout.writeAll("│ ");
                try stdout.print("{s}", .{info.name});
                for (0..max_width - info.name.len) |_| try stdout.writeAll(" ");
                try stdout.print(" │ {d: >10} │\n", .{info.count});
            }

            try stdout.writeAll("└─");
            for (0..max_width) |_| try stdout.writeAll("─");
            try stdout.writeAll("─┴────────────┘\n");

            try stdout.print("{d} label(s)\n", .{labels.len});
        },
        .json => {
            try stdout.writeAll("{\"labels\":[");
            for (labels, 0..) |info, i| {
                if (i > 0) try stdout.writeAll(",");
                try stdout.print("{{\"name\":\"{s}\",\"count\":{d}}}", .{ info.name, info.count });
            }
            try stdout.writeAll("]}\n");
        },
        .csv => {
            try stdout.writeAll("label,count\n");
            for (labels) |info| {
                try stdout.print("{s},{d}\n", .{ info.name, info.count });
            }
        },
    }
}

fn cmdTypes(
    allocator: std.mem.Allocator,
    stdout: anytype,
    stderr: anytype,
    parsed_args: *const Args,
) !void {
    const path = parsed_args.path.?;

    // Open database
    var db = Database.open(allocator, path, .{
        .read_only = true,
        .lock = !parsed_args.no_lock,
    }) catch |err| {
        return failOpen(stderr, path, err);
    };
    defer db.close();

    // Get all edge types
    const types = db.getAllEdgeTypes() catch |err| {
        return failCommand(stderr, "Failed to get edge types: {s}", .{@errorName(err)});
    };
    defer db.freeEdgeTypeInfos(types);

    switch (parsed_args.format) {
        .table => {
            if (types.len == 0) {
                try stdout.writeAll("No edge types found\n");
                return;
            }

            // Calculate max type width for formatting
            var max_width: usize = 4; // "Type"
            for (types) |info| {
                if (info.name.len > max_width) max_width = info.name.len;
            }

            // Print table header
            try stdout.writeAll("┌─");
            for (0..max_width) |_| try stdout.writeAll("─");
            try stdout.writeAll("─┬────────────┐\n");

            try stdout.writeAll("│ ");
            try stdout.print("{s}", .{"Type"});
            for (0..max_width - 4) |_| try stdout.writeAll(" ");
            try stdout.writeAll(" │ Count      │\n");

            try stdout.writeAll("├─");
            for (0..max_width) |_| try stdout.writeAll("─");
            try stdout.writeAll("─┼────────────┤\n");

            // Print rows
            for (types) |info| {
                try stdout.writeAll("│ ");
                try stdout.print("{s}", .{info.name});
                for (0..max_width - info.name.len) |_| try stdout.writeAll(" ");
                try stdout.print(" │ {d: >10} │\n", .{info.count});
            }

            try stdout.writeAll("└─");
            for (0..max_width) |_| try stdout.writeAll("─");
            try stdout.writeAll("─┴────────────┘\n");

            try stdout.print("{d} edge type(s)\n", .{types.len});
        },
        .json => {
            try stdout.writeAll("{\"types\":[");
            for (types, 0..) |info, i| {
                if (i > 0) try stdout.writeAll(",");
                try stdout.print("{{\"name\":\"{s}\",\"count\":{d}}}", .{ info.name, info.count });
            }
            try stdout.writeAll("]}\n");
        },
        .csv => {
            try stdout.writeAll("type,count\n");
            for (types) |info| {
                try stdout.print("{s},{d}\n", .{ info.name, info.count });
            }
        },
    }
}

fn cmdSchema(
    allocator: std.mem.Allocator,
    stdout: anytype,
    stderr: anytype,
    parsed_args: *const Args,
) !void {
    const path = parsed_args.path.?;

    // Open database
    var db = Database.open(allocator, path, .{
        .read_only = true,
        .lock = !parsed_args.no_lock,
    }) catch |err| {
        return failOpen(stderr, path, err);
    };
    defer db.close();

    // Get all labels
    const labels = db.getAllLabels() catch |err| {
        return failCommand(stderr, "Failed to get labels: {s}", .{@errorName(err)});
    };
    defer db.freeLabelInfos(labels);

    // Get all edge types
    const edge_types = db.getAllEdgeTypes() catch |err| {
        return failCommand(stderr, "Failed to get edge types: {s}", .{@errorName(err)});
    };
    defer db.freeEdgeTypeInfos(edge_types);

    switch (parsed_args.format) {
        .table => {
            try stdout.writeAll("Schema\n");
            try stdout.writeAll("══════\n\n");

            // Print node labels
            if (labels.len == 0) {
                try stdout.writeAll("No node labels defined\n\n");
            } else {
                try stdout.writeAll("Node Labels:\n");
                for (labels) |info| {
                    try stdout.print("  (:{s}) - {d} node(s)\n", .{ info.name, info.count });
                }
                try stdout.writeAll("\n");
            }

            // Print edge types
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
        },
        .json => {
            try stdout.writeAll("{\"schema\":{\"labels\":[");
            for (labels, 0..) |info, i| {
                if (i > 0) try stdout.writeAll(",");
                try stdout.print("{{\"name\":\"{s}\",\"count\":{d}}}", .{ info.name, info.count });
            }
            try stdout.writeAll("],\"edge_types\":[");
            for (edge_types, 0..) |info, i| {
                if (i > 0) try stdout.writeAll(",");
                try stdout.print("{{\"name\":\"{s}\",\"count\":{d}}}", .{ info.name, info.count });
            }
            try stdout.writeAll("]}}\n");
        },
        .csv => {
            try stdout.writeAll("type,name,count\n");
            for (labels) |info| {
                try stdout.print("label,{s},{d}\n", .{ info.name, info.count });
            }
            for (edge_types) |info| {
                try stdout.print("edge_type,{s},{d}\n", .{ info.name, info.count });
            }
        },
    }
}

fn cmdCompact(
    allocator: std.mem.Allocator,
    stdout: anytype,
    stderr: anytype,
    parsed_args: *const Args,
) !void {
    const path = parsed_args.path.?;
    const db = Database.open(allocator, path, .{ .lock = !parsed_args.no_lock }) catch |err| {
        return failOpen(stderr, path, err);
    };
    defer db.close();

    const stats = db.compact() catch |err| {
        return failCommand(stderr, "Failed to compact database: {s}", .{@errorName(err)});
    };

    switch (parsed_args.format) {
        .table => {
            output.printSuccess(stdout, "Compacted database: {s}", .{path});
            try stdout.print("  Pages before:    {d}\n", .{stats.pages_before});
            try stdout.print("  Pages after:     {d}\n", .{stats.pages_after});
            try stdout.print("  Pages removed:   {d}\n", .{stats.pages_removed});
            try stdout.print("  Bytes reclaimed: {d}\n", .{stats.bytes_reclaimed});
        },
        .json => try stdout.print(
            "{{\"path\":\"{s}\",\"pages_before\":{d},\"pages_after\":{d},\"pages_removed\":{d},\"bytes_reclaimed\":{d}}}\n",
            .{ path, stats.pages_before, stats.pages_after, stats.pages_removed, stats.bytes_reclaimed },
        ),
        .csv => {
            try stdout.writeAll("path,pages_before,pages_after,pages_removed,bytes_reclaimed\n");
            try stdout.print(
                "{s},{d},{d},{d},{d}\n",
                .{ path, stats.pages_before, stats.pages_after, stats.pages_removed, stats.bytes_reclaimed },
            );
        },
    }
}

fn cmdBackup(
    allocator: std.mem.Allocator,
    stdout: anytype,
    stderr: anytype,
    parsed_args: *const Args,
) !void {
    const path = parsed_args.path.?;
    const dest = parsed_args.file orelse {
        return failCommand(stderr, "No destination provided. Use --file=<path>", .{});
    };

    const db = Database.open(allocator, path, .{ .lock = !parsed_args.no_lock }) catch |err| {
        return failOpen(stderr, path, err);
    };
    defer db.close();

    const stats = db.backup(dest) catch |err| {
        return failCommand(stderr, "Failed to back up database: {s}", .{@errorName(err)});
    };

    switch (parsed_args.format) {
        .table => {
            output.printSuccess(stdout, "Backed up {s} to {s}", .{ path, dest });
            try stdout.print("  Bytes copied:    {d}\n", .{stats.bytes_copied});
            try stdout.print("  Pages:           {d}\n", .{stats.pages_copied});
            try stdout.print("  Pages flushed:   {d}\n", .{stats.pages_flushed});
            try stdout.print("  Duration:        {d} ms\n", .{stats.duration_ns / std.time.ns_per_ms});
        },
        .json => try stdout.print(
            "{{\"path\":\"{s}\",\"destination\":\"{s}\",\"bytes_copied\":{d},\"pages\":{d},\"pages_flushed\":{d},\"duration_ns\":{d}}}\n",
            .{ path, dest, stats.bytes_copied, stats.pages_copied, stats.pages_flushed, stats.duration_ns },
        ),
        .csv => {
            try stdout.writeAll("path,destination,bytes_copied,pages,pages_flushed,duration_ns\n");
            try stdout.print(
                "{s},{s},{d},{d},{d},{d}\n",
                .{ path, dest, stats.bytes_copied, stats.pages_copied, stats.pages_flushed, stats.duration_ns },
            );
        },
    }
}

/// Print what one replication pass did, in whichever format was asked for.
fn reportReplicationPass(
    stdout: anytype,
    format: args_mod.OutputFormat,
    path: []const u8,
    dest: []const u8,
    stats: lattice.storage.replicate.ReplicateStats,
) !void {
    switch (format) {
        .table => {
            if (stats.started_generation) {
                output.printSuccess(
                    stdout,
                    "Started generation {d} for {s} in {s}",
                    .{ stats.generation, path, dest },
                );
                try stdout.print("  Snapshot bytes:  {d}\n", .{stats.snapshot_bytes});
            } else if (stats.frames_shipped == 0) {
                output.printSuccess(
                    stdout,
                    "Nothing new to ship for {s}",
                    .{path},
                );
            } else {
                output.printSuccess(
                    stdout,
                    "Shipped {s} to {s}",
                    .{ path, dest },
                );
            }
            try stdout.print("  Generation:      {d}\n", .{stats.generation});
            try stdout.print("  Frames shipped:  {d}\n", .{stats.frames_shipped});
            try stdout.print("  Bytes shipped:   {d}\n", .{stats.bytes_shipped});
            try stdout.print("  Duration:        {d} ms\n", .{stats.duration_ns / std.time.ns_per_ms});
        },
        .json => try stdout.print(
            "{{\"path\":\"{s}\",\"destination\":\"{s}\",\"generation\":{d},\"started_generation\":{}," ++
                "\"frames_shipped\":{d},\"bytes_shipped\":{d},\"snapshot_bytes\":{d},\"duration_ns\":{d}}}\n",
            .{
                path,          dest,
                stats.generation, stats.started_generation,
                stats.frames_shipped, stats.bytes_shipped,
                stats.snapshot_bytes, stats.duration_ns,
            },
        ),
        .csv => try stdout.print(
            "{s},{s},{d},{},{d},{d},{d},{d}\n",
            .{
                path,          dest,
                stats.generation, stats.started_generation,
                stats.frames_shipped, stats.bytes_shipped,
                stats.snapshot_bytes, stats.duration_ns,
            },
        ),
    }
}

fn cmdReplicate(
    allocator: std.mem.Allocator,
    stdout: anytype,
    stderr: anytype,
    parsed_args: *const Args,
) !void {
    const path = parsed_args.path.?;
    const dest = parsed_args.to orelse {
        return failCommand(stderr, "No destination provided. Use --to=<directory>", .{});
    };

    const db = Database.open(allocator, path, .{ .lock = !parsed_args.no_lock }) catch |err| {
        return failOpen(stderr, path, err);
    };
    defer db.close();

    // The CSV header belongs above the rows rather than repeated per pass.
    if (parsed_args.format == .csv) {
        try stdout.writeAll(
            "path,destination,generation,started_generation,frames_shipped,bytes_shipped,snapshot_bytes,duration_ns\n",
        );
    }

    const interval_ns = @as(u64, parsed_args.interval_secs) * std.time.ns_per_s;

    while (true) {
        const stats = db.replicateTo(dest) catch |err| switch (err) {
            error.UuidMismatch => return failCommand(
                stderr,
                "{s} already holds backups of a different database",
                .{dest},
            ),
            error.NoWal => return failCommand(
                stderr,
                "Database has no write-ahead log, so there is nothing to follow",
                .{},
            ),
            error.UnsupportedManifest => return failCommand(
                stderr,
                "{s} holds a manifest this version does not understand",
                .{dest},
            ),
            else => return failCommand(stderr, "Failed to replicate: {s}", .{@errorName(err)}),
        };

        try reportReplicationPass(stdout, parsed_args.format, path, dest, stats);

        if (!parsed_args.follow) break;
        @import("compat").sleep(interval_ns);
    }
}

/// Report a database that would not open.
///
/// A locked database gets its own wording, because "DatabaseLocked" tells you
/// what happened and not what to do about it.
fn failOpen(stderr: anytype, path: []const u8, err: anyerror) anyerror {
    if (err == DatabaseError.DatabaseLocked) {
        return failCommand(
            stderr,
            "{s} is open in another process. Close it first, or pass --no-lock if you are certain nothing is writing to it.",
            .{path},
        );
    }
    return failCommand(stderr, "Failed to open database: {s}", .{@errorName(err)});
}

fn cmdRestore(
    allocator: std.mem.Allocator,
    stdout: anytype,
    stderr: anytype,
    parsed_args: *const Args,
) !void {
    const source = parsed_args.path.?;
    const output_path = parsed_args.output orelse {
        return failCommand(stderr, "No output path provided. Use --output=<path>", .{});
    };

    const stats = lattice.storage.restore.restore(allocator, source, output_path, .{
        .at_ms = parsed_args.at_ms,
        .overwrite = parsed_args.force,
    }) catch |err| switch (err) {
        error.NoBackup => return failCommand(
            stderr,
            "{s} does not hold a backup, so there is nothing to restore",
            .{source},
        ),
        error.OutputExists => return failCommand(
            stderr,
            "{s} already exists. Pass --force to overwrite it",
            .{output_path},
        ),
        error.NothingAtThatTime => return failCommand(
            stderr,
            "No backup had been taken yet at that moment",
            .{},
        ),
        error.MissingData => return failCommand(
            stderr,
            "A snapshot or segment the backup names is missing or unreadable",
            .{},
        ),
        error.UnsupportedManifest => return failCommand(
            stderr,
            "{s} holds a backup this version does not understand",
            .{source},
        ),
        else => return failCommand(stderr, "Failed to restore: {s}", .{@errorName(err)}),
    };

    switch (parsed_args.format) {
        .table => {
            output.printSuccess(stdout, "Restored {s} to {s}", .{ source, output_path });
            try stdout.print("  Generation:      {d}\n", .{stats.generation});
            try stdout.print("  Segments:        {d}\n", .{stats.segments_applied});
            try stdout.print("  Frames replayed: {d}\n", .{stats.frames_applied});
            try stdout.print("  Bytes written:   {d}\n", .{stats.bytes_written});
            var when: [32]u8 = undefined;
            try stdout.print(
                "  Restored to:     {s}\n",
                .{args_mod.formatTimestamp(&when, stats.restored_to_ms)},
            );
            try stdout.print("  Duration:        {d} ms\n", .{stats.duration_ns / std.time.ns_per_ms});
        },
        .json => try stdout.print(
            "{{\"source\":\"{s}\",\"output\":\"{s}\",\"generation\":{d},\"segments_applied\":{d}," ++
                "\"frames_applied\":{d},\"bytes_written\":{d},\"restored_to_ms\":{d},\"duration_ns\":{d}}}\n",
            .{
                source,
                output_path,
                stats.generation,
                stats.segments_applied,
                stats.frames_applied,
                stats.bytes_written,
                stats.restored_to_ms,
                stats.duration_ns,
            },
        ),
        .csv => {
            try stdout.writeAll(
                "source,output,generation,segments_applied,frames_applied,bytes_written,restored_to_ms,duration_ns\n",
            );
            try stdout.print(
                "{s},{s},{d},{d},{d},{d},{d},{d}\n",
                .{
                    source,
                    output_path,
                    stats.generation,
                    stats.segments_applied,
                    stats.frames_applied,
                    stats.bytes_written,
                    stats.restored_to_ms,
                    stats.duration_ns,
                },
            );
        },
    }
}

fn cmdCheckpoint(
    allocator: std.mem.Allocator,
    stdout: anytype,
    stderr: anytype,
    parsed_args: *const Args,
) !void {
    const path = parsed_args.path.?;
    const db = Database.open(allocator, path, .{ .lock = !parsed_args.no_lock }) catch |err| {
        return failOpen(stderr, path, err);
    };
    defer db.close();

    const maybe_stats = db.checkpoint(.truncate) catch |err| {
        return failCommand(stderr, "Failed to checkpoint database: {s}", .{@errorName(err)});
    };

    const stats = maybe_stats orelse {
        return failCommand(stderr, "Database has no write-ahead log to checkpoint", .{});
    };

    switch (parsed_args.format) {
        .table => {
            output.printSuccess(stdout, "Checkpointed database: {s}", .{path});
            try stdout.print("  Pages flushed:   {d}\n", .{stats.pages_flushed});
            try stdout.print("  Checkpoint LSN:  {d}\n", .{stats.checkpoint_lsn});
            try stdout.print("  WAL truncated:   {s}\n", .{if (stats.wal_truncated) "yes" else "no"});
            try stdout.print("  Duration:        {d} ms\n", .{stats.duration_ns / std.time.ns_per_ms});
        },
        .json => try stdout.print(
            "{{\"path\":\"{s}\",\"pages_flushed\":{d},\"checkpoint_lsn\":{d},\"wal_truncated\":{},\"duration_ns\":{d}}}\n",
            .{ path, stats.pages_flushed, stats.checkpoint_lsn, stats.wal_truncated, stats.duration_ns },
        ),
        .csv => {
            try stdout.writeAll("path,pages_flushed,checkpoint_lsn,wal_truncated,duration_ns\n");
            try stdout.print(
                "{s},{d},{d},{},{d}\n",
                .{ path, stats.pages_flushed, stats.checkpoint_lsn, stats.wal_truncated, stats.duration_ns },
            );
        },
    }
}

fn cmdCheck(
    allocator: std.mem.Allocator,
    stdout: anytype,
    stderr: anytype,
    parsed_args: *const Args,
) !void {
    const path = parsed_args.path.?;
    const stats = checkDatabaseFile(allocator, path, !parsed_args.no_lock) catch |err| switch (err) {
        error.FileNotFound => return failCommand(stderr, "Database file not found: {s}", .{path}),
        error.PermissionDenied => return failCommand(stderr, "Permission denied while checking: {s}", .{path}),
        error.InvalidDatabase => return failCommand(stderr, "Invalid database file: {s}", .{path}),
        error.ChecksumMismatch => return failCommand(stderr, "Checksum mismatch detected in database file: {s}", .{path}),
        error.DatabaseLocked => return failCommand(
            stderr,
            "{s} is open in another process. Checking it while it is being written would report damage that is not there. Close it first, or pass --no-lock.",
            .{path},
        ),
        error.OutOfMemory => return err,
        else => return failCommand(stderr, "Failed to check database file: {s}", .{@errorName(err)}),
    };

    switch (parsed_args.format) {
        .table => {
            output.printSuccess(stdout, "Database file checks passed", .{});
            try stdout.print("  Pages checked: {d}\n", .{stats.pages_checked});
            if (stats.wal_present) {
                try stdout.writeAll("  Note: sibling WAL file exists but was not validated\n");
            }
        },
        .json => {
            try stdout.print("{{\"path\":\"{s}\",\"pages_checked\":{d},\"wal_present\":{},\"wal_validated\":false}}\n", .{
                path,
                stats.pages_checked,
                stats.wal_present,
            });
        },
        .csv => {
            try stdout.writeAll("property,value\n");
            try stdout.print("path,{s}\n", .{path});
            try stdout.print("pages_checked,{d}\n", .{stats.pages_checked});
            try stdout.print("wal_present,{}\n", .{stats.wal_present});
            try stdout.writeAll("wal_validated,false\n");
        },
    }
}

/// Verify every page checksum in the main database file.
///
/// Takes a shared lock by default, because reading pages while another process
/// flushes them reports corruption that is not there.
fn checkDatabaseFile(allocator: std.mem.Allocator, path: []const u8, lock: bool) CheckError!CheckStats {
    var posix_vfs = PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    var page_manager = PageManager.init(allocator, vfs_impl, path, .{
        .read_only = true,
        .lock = lock,
    }) catch |err| return mapPageManagerCheckError(err);
    defer page_manager.deinit();

    const page_size: usize = @intCast(page_manager.getPageSize());
    const page_alignment = comptime std.mem.Alignment.fromByteUnits(@alignOf(PageHeader));
    const page_buf = allocator.alignedAlloc(u8, page_alignment, page_size) catch {
        return CheckError.OutOfMemory;
    };
    defer allocator.free(page_buf);

    const page_count = page_manager.pageCount();
    var page_id: u32 = 1;
    while (page_id < page_count) : (page_id += 1) {
        page_manager.readPage(page_id, page_buf) catch |err| return mapPageManagerCheckError(err);
    }

    return .{
        .pages_checked = page_count -| 1,
        .wal_present = hasWalSibling(path),
    };
}

fn mapPageManagerCheckError(err: PageManagerError) CheckError {
    return switch (err) {
        error.FileNotFound => CheckError.FileNotFound,
        error.PermissionDenied => CheckError.PermissionDenied,
        error.ChecksumMismatch => CheckError.ChecksumMismatch,
        error.DatabaseLocked => CheckError.DatabaseLocked,
        error.InvalidHeader, error.InvalidMagic, error.VersionTooNew, error.InvalidPageId, error.PageNotAllocated => CheckError.InvalidDatabase,
        else => CheckError.IoError,
    };
}

fn hasWalSibling(path: []const u8) bool {
    var wal_path_buf: [512]u8 = undefined;
    const wal_path = std.fmt.bufPrint(&wal_path_buf, "{s}-wal", .{path}) catch return false;
    @import("compat").fs.cwd().access(wal_path, .{}) catch return false;
    return true;
}

fn cmdImport(
    allocator: std.mem.Allocator,
    stdout: anytype,
    stderr: anytype,
    parsed_args: *const Args,
) !void {
    const path = parsed_args.path.?;
    const file = parsed_args.file orelse {
        return failCommand(stderr, "No import file specified. Use --file=<path>", .{});
    };

    // Open database
    var db = Database.open(allocator, path, .{ .lock = !parsed_args.no_lock }) catch |err| {
        return failOpen(stderr, path, err);
    };
    defer db.close();

    output.printInfo(stdout, "Importing {s} into {s}...", .{ file, path });

    // Detect file format by extension
    const is_json = std.mem.endsWith(u8, file, ".json");
    const is_csv = std.mem.endsWith(u8, file, ".csv");

    if (!is_json and !is_csv) {
        return failCommand(stderr, "Unknown file format. Use .json or .csv extension.", .{});
    }

    const stats = if (is_json)
        import_export.importJson(allocator, db, file, parsed_args.batch_size, parsed_args.on_error_skip)
    else
        import_export.importCsv(allocator, db, file, parsed_args.batch_size, parsed_args.on_error_skip);

    if (stats) |s| {
        switch (parsed_args.format) {
            .table => {
                output.printSuccess(stdout, "Import complete", .{});
                try stdout.print("  Nodes imported: {d}\n", .{s.nodes_imported});
                if (s.nodes_failed > 0) {
                    try stdout.print("  Nodes failed:   {d}\n", .{s.nodes_failed});
                }
                try stdout.print("  Edges imported: {d}\n", .{s.edges_imported});
                if (s.edges_failed > 0) {
                    try stdout.print("  Edges failed:   {d}\n", .{s.edges_failed});
                }
            },
            .json => {
                try stdout.print("{{\"nodes_imported\":{d},\"nodes_failed\":{d},\"edges_imported\":{d},\"edges_failed\":{d}}}\n", .{
                    s.nodes_imported,
                    s.nodes_failed,
                    s.edges_imported,
                    s.edges_failed,
                });
            },
            .csv => {
                try stdout.writeAll("metric,count\n");
                try stdout.print("nodes_imported,{d}\n", .{s.nodes_imported});
                try stdout.print("nodes_failed,{d}\n", .{s.nodes_failed});
                try stdout.print("edges_imported,{d}\n", .{s.edges_imported});
                try stdout.print("edges_failed,{d}\n", .{s.edges_failed});
            },
        }
    } else |err| {
        return failCommand(stderr, "Import failed: {s}", .{@errorName(err)});
    }
}

fn cmdExport(
    allocator: std.mem.Allocator,
    stdout: anytype,
    stderr: anytype,
    parsed_args: *const Args,
) !void {
    const path = parsed_args.path.?;
    const file = parsed_args.file orelse {
        return failCommand(stderr, "No export file specified. Use --file=<path>", .{});
    };

    // Open database
    var db = Database.open(allocator, path, .{
        .read_only = true,
        .lock = !parsed_args.no_lock,
    }) catch |err| {
        return failOpen(stderr, path, err);
    };
    defer db.close();

    output.printInfo(stdout, "Exporting {s} to {s}...", .{ path, file });

    const ExportKind = enum { json, jsonl, csv, dot };
    const export_kind: ExportKind = blk: {
        if (std.mem.endsWith(u8, file, ".json")) break :blk .json;
        if (std.mem.endsWith(u8, file, ".jsonl")) break :blk .jsonl;
        if (std.mem.endsWith(u8, file, ".csv")) break :blk .csv;
        if (std.mem.endsWith(u8, file, ".dot")) break :blk .dot;
        return failCommand(stderr, "Unknown file format. Use .json, .jsonl, .csv, or .dot extension.", .{});
    };

    if (export_kind != .csv) {
        // Single-file exports: JSON, JSONL, DOT.
        const out_file = @import("compat").fs.cwd().createFile(file, .{}) catch |err| {
            return failCommand(stderr, "Cannot create output file: {s}", .{@errorName(err)});
        };
        defer out_file.close();

        const writer = out_file.deprecatedWriter();
        const stats = switch (export_kind) {
            .json => import_export.exportJson(allocator, db, writer, parsed_args.label_filter),
            .jsonl => import_export.exportJsonl(allocator, db, writer, parsed_args.label_filter),
            .dot => import_export.exportDot(allocator, db, writer, parsed_args.label_filter),
            .csv => unreachable,
        };

        if (stats) |s| {
            switch (parsed_args.format) {
                .table => {
                    output.printSuccess(stdout, "Export complete", .{});
                    try stdout.print("  Nodes exported: {d}\n", .{s.nodes_exported});
                    try stdout.print("  Edges exported: {d}\n", .{s.edges_exported});
                },
                .json => {
                    try stdout.print("{{\"nodes_exported\":{d},\"edges_exported\":{d}}}\n", .{
                        s.nodes_exported,
                        s.edges_exported,
                    });
                },
                .csv => {
                    try stdout.writeAll("metric,count\n");
                    try stdout.print("nodes_exported,{d}\n", .{s.nodes_exported});
                    try stdout.print("edges_exported,{d}\n", .{s.edges_exported});
                },
            }
        } else |err| {
            return failCommand(stderr, "Export failed: {s}", .{@errorName(err)});
        }
    } else {
        // Export to CSV - creates two files: file_nodes.csv and file_edges.csv
        const base_name = file[0 .. file.len - 4]; // Remove .csv

        var nodes_path_buf: [1024]u8 = undefined;
        const nodes_path = std.fmt.bufPrint(&nodes_path_buf, "{s}_nodes.csv", .{base_name}) catch {
            return failCommand(stderr, "Path too long", .{});
        };

        var edges_path_buf: [1024]u8 = undefined;
        const edges_path = std.fmt.bufPrint(&edges_path_buf, "{s}_edges.csv", .{base_name}) catch {
            return failCommand(stderr, "Path too long", .{});
        };

        const nodes_file = @import("compat").fs.cwd().createFile(nodes_path, .{}) catch |err| {
            return failCommand(stderr, "Cannot create nodes file: {s}", .{@errorName(err)});
        };
        defer nodes_file.close();

        const edges_file = @import("compat").fs.cwd().createFile(edges_path, .{}) catch |err| {
            return failCommand(stderr, "Cannot create edges file: {s}", .{@errorName(err)});
        };
        defer edges_file.close();

        const nodes_writer = nodes_file.deprecatedWriter();
        const edges_writer = edges_file.deprecatedWriter();

        const stats = import_export.exportCsv(allocator, db, nodes_writer, edges_writer, parsed_args.label_filter);

        if (stats) |s| {
            switch (parsed_args.format) {
                .table => {
                    output.printSuccess(stdout, "Export complete", .{});
                    try stdout.print("  Nodes exported: {d} -> {s}\n", .{ s.nodes_exported, nodes_path });
                    try stdout.print("  Edges exported: {d} -> {s}\n", .{ s.edges_exported, edges_path });
                },
                .json => {
                    try stdout.print("{{\"nodes_exported\":{d},\"edges_exported\":{d},\"nodes_file\":\"{s}\",\"edges_file\":\"{s}\"}}\n", .{
                        s.nodes_exported,
                        s.edges_exported,
                        nodes_path,
                        edges_path,
                    });
                },
                .csv => {
                    try stdout.writeAll("metric,value\n");
                    try stdout.print("nodes_exported,{d}\n", .{s.nodes_exported});
                    try stdout.print("edges_exported,{d}\n", .{s.edges_exported});
                    try stdout.print("nodes_file,{s}\n", .{nodes_path});
                    try stdout.print("edges_file,{s}\n", .{edges_path});
                },
            }
        } else |err| {
            return failCommand(stderr, "Export failed: {s}", .{@errorName(err)});
        }
    }
}

fn cmdDump(
    allocator: std.mem.Allocator,
    stdout: anytype,
    stderr: anytype,
    parsed_args: *const Args,
) !void {
    const path = parsed_args.path.?;

    // Open database
    var db = Database.open(allocator, path, .{
        .read_only = true,
        .lock = !parsed_args.no_lock,
    }) catch |err| {
        return failOpen(stderr, path, err);
    };
    defer db.close();

    // Dump to stdout as canonical JSON
    const stats = import_export.dumpCanonicalJson(allocator, db, stdout, parsed_args.label_filter);

    if (stats) |s| {
        // Print stats to stderr so they don't mix with JSON output
        try stderr.print("Dumped {d} nodes, {d} edges\n", .{ s.nodes_exported, s.edges_exported });
    } else |err| {
        return failCommand(stderr, "Dump failed: {s}", .{@errorName(err)});
    }
}

// ============================================
// Help and Version
// ============================================

fn printVersion(writer: anytype) void {
    writer.print("LatticeDB v{s}\n", .{VERSION}) catch {};
    writer.print("Format version: {d}\n", .{lattice.core.types.FORMAT_VERSION}) catch {};
}

fn printUsage(writer: anytype) void {
    writer.writeAll(
        \\LatticeDB - Embedded knowledge graph database
        \\
        \\Usage: lattice <command> [options] [path]
        \\
        \\Commands:
        \\  Database:
        \\    create <path>       Create a new database
        \\    info <path>         Show database information
        \\    compact <path>      Reclaim free pages from physical EOF
        \\    checkpoint <path>   Flush pending writes and reset the WAL
        \\    backup <path>       Copy to another file, database stays open
        \\    replicate <path>    Ship changes to a directory, once or continuously
        \\    restore <dir>       Rebuild a database from what replication shipped
        \\    check <path>        Verify main database file checksums
        \\
        \\  Query:
        \\    query <path>        Interactive Cypher REPL
        \\    exec <path>         Execute a single query
        \\
        \\  Import/Export:
        \\    import <path>       Import data from JSON/CSV
        \\    export <path>       Export data to JSON/JSONL/CSV/DOT
        \\    dump <path>         Dump full database as canonical JSON
        \\
        \\  Introspection:
        \\    labels <path>       List all node labels
        \\    types <path>        List all edge types
        \\    schema <path>       Show inferred schema
        \\    count <path>        Show node/edge counts
        \\
        \\  Utility:
        \\    update              Update the local LatticeDB installation
        \\    version             Show version information
        \\    help                Show this help message
        \\
        \\Options:
        \\  --format=<fmt>        Output format: table, json, csv (default: table)
        \\  --enable-vector       Enable vector index (for create)
        \\  --vector-dims=<n>     Vector dimensions, 1..4096 (default: 128)
        \\  --enable-fts          Enable full-text search (default: on)
        \\  --no-fts              Disable full-text search
        \\  --cache-size=<mb>     Buffer pool size in MB (default: 64)
        \\  --page-size=<bytes>   Page size in bytes, 4096..65535 (default: 4096)
        \\  --file=<path>         Input/output file for import/export
        \\  --query=<cypher>      Query string for exec command
        \\  --no-lock             Skip the file lock, for filesystems without one
        \\  -h, --help            Show help for a command
        \\  -v, --version         Show version
        \\
        \\Examples:
        \\  lattice create mydb.lattice --enable-vector --vector-dims=384
        \\  lattice query mydb.lattice
        \\  lattice exec mydb.lattice --query="MATCH (n) RETURN n LIMIT 10"
        \\  lattice count mydb.lattice --format=json
        \\  lattice check mydb.lattice
        \\  lattice export mydb.lattice --file=backup.json
        \\  lattice update
        \\
    ) catch {};
}

fn printCommandHelp(writer: anytype, command: Command) void {
    switch (command) {
        .create => writer.writeAll(
            \\Usage: lattice create <path> [options]
            \\
            \\Create a new LatticeDB database file.
            \\
            \\Options:
            \\  --enable-vector       Enable HNSW vector index
            \\  --vector-dims=<n>     Vector dimensions (default: 128, max: 4096)
            \\  --enable-fts          Enable BM25 full-text search (default: on)
            \\  --no-fts              Disable full-text search
            \\  --cache-size=<mb>     Buffer pool size in MB (default: 64)
            \\  --page-size=<bytes>   Page size in bytes (default: 4096)
            \\
            \\Examples:
            \\  lattice create mydb.lattice
            \\  lattice create embeddings.lattice --enable-vector --vector-dims=1536
            \\
        ) catch {},
        .update => writer.writeAll(
            \\Usage: lattice update
            \\
            \\Update the local LatticeDB installation to the latest release.
            \\The updater streams download and install progress directly to
            \\the terminal.
            \\
        ) catch {},
        .query => writer.writeAll(
            \\Usage: lattice query <path>
            \\
            \\Start an interactive Cypher query shell.
            \\
            \\REPL Commands:
            \\  .help                 Show REPL help
            \\  .labels               List all node labels
            \\  .types                List all edge types
            \\  .schema               Show inferred schema
            \\  .format <table|json|csv>  Set output format
            \\  .timing on|off        Toggle query timing
            \\  .exit, .quit          Exit the REPL
            \\
            \\Example:
            \\  lattice query mydb.lattice
            \\
        ) catch {},
        .backup => writer.writeAll(
            \\Usage: lattice backup <path> --file=<destination>
            \\
            \\Copy a database to another file. The source stays open and usable
            \\afterwards; you do not have to stop anything first.
            \\
            \\Pending writes are flushed into the file before the copy starts, so
            \\the destination is a complete database on its own and needs no
            \\write-ahead log beside it.
            \\
            \\The copy is written next to the destination and renamed into place
            \\once it is complete, so an interrupted backup does not leave a
            \\partial file that looks usable.
            \\
            \\Options:
            \\  --file=<path>         Where to write the copy
            \\  --format=<fmt>        Output format: table, json, csv
            \\
            \\Example:
            \\  lattice backup mydb.lattice --file=/backups/mydb-$(date +%F).lattice
            \\
        ) catch {},
        .replicate => writer.writeAll(
            \\Usage: lattice replicate <path> --to=<directory> [options]
            \\
            \\Ship a database's changes into a directory, so a disk failure costs
            \\you the last few seconds rather than everything since your last
            \\manual copy.
            \\
            \\The first pass writes a full snapshot. Every pass after that copies
            \\only the write-ahead log frames that have appeared since, which is
            \\why running it often is cheap. A pass with nothing to ship is normal
            \\and is not an error.
            \\
            \\Whenever the write-ahead log is reset, frame numbering restarts and
            \\a new generation begins with a fresh snapshot. Older generations are
            \\left in place, because restoring to a point inside one still needs
            \\its frames.
            \\
            \\This command opens the database, and LatticeDB does not lock a
            \\database across processes. Do not run it against a database another
            \\process currently has open. To replicate a database while your
            \\application is using it, call replicateTo on your own handle instead
            \\and let this command cover the case where nothing else is running.
            \\
            \\Options:
            \\  --to=<directory>      Where to ship to, created if missing
            \\  --follow              Keep running, shipping on an interval
            \\  --interval=<seconds>  How long to wait between passes (default 10)
            \\  --format=<fmt>        Output format: table, json, csv
            \\
            \\Examples:
            \\  lattice replicate mydb.lattice --to=/mnt/backup/mydb
            \\  lattice replicate mydb.lattice --to=/mnt/backup/mydb --follow --interval=30
            \\
        ) catch {},
        .restore => writer.writeAll(
            \\Usage: lattice restore <directory> --output=<path> [options]
            \\
            \\Rebuild a database from a directory that lattice replicate has been
            \\shipping to.
            \\
            \\The snapshot is copied, the changes shipped after it are replayed on
            \\top the same way recovery replays them, and the result is folded into
            \\a single file. What you get back is a database you can open, copy, or
            \\move on its own.
            \\
            \\With --at you get the state as of a moment in the past. What that
            \\lands on is the last replication pass at or before the moment you
            \\asked for, so whatever interval you replicate on is also how precisely
            \\you can rewind. Times are read as UTC.
            \\
            \\Options:
            \\  --output=<path>       Where to write the restored database
            \\  --at=<time>           Restore as of this moment, UTC
            \\  --force               Overwrite whatever is at the output path
            \\  --format=<fmt>        Output format: table, json, csv
            \\
            \\Examples:
            \\  lattice restore /mnt/backup/social --output=recovered.lattice
            \\  lattice restore /mnt/backup/social --output=recovered.lattice --at="2026-08-25T14:00:00Z"
            \\
        ) catch {},
        .checkpoint => writer.writeAll(
            \\Usage: lattice checkpoint <path> [options]
            \\
            \\Flush pending writes into the database file and reset the
            \\write-ahead log. Use this to bound WAL growth on a database that
            \\stays open for a long time, or to reach a clean point before
            \\copying the file.
            \\
            \\Unlike compact, this does not move or reclaim pages. The database
            \\file does not shrink; the WAL does.
            \\
            \\Options:
            \\  --format=<fmt>        Output format: table, json, csv
            \\
            \\Example:
            \\  lattice checkpoint mydb.lattice
            \\
        ) catch {},
        .compact => writer.writeAll(
            \\Usage: lattice compact <path> [options]
            \\
            \\Flush durable state, rebuild the retained freelist, and truncate
            \\contiguous free pages from the physical end of the database.
            \\Live pages are never relocated.
            \\
            \\Options:
            \\  --format=<fmt>        Output format: table, json, csv
            \\
            \\Example:
            \\  lattice compact mydb.lattice
            \\
        ) catch {},
        .check => writer.writeAll(
            \\Usage: lattice check <path> [options]
            \\
            \\Open the main database file read-only and verify the stored
            \\ per-page checksums.
            \\
            \\Options:
            \\  --format=<fmt>        Output format: table, json, csv
            \\
            \\Notes:
            \\  A sibling <path>-wal file is reported if present, but WAL
            \\  frames are not currently validated by this command.
            \\
            \\Example:
            \\  lattice check mydb.lattice
            \\
        ) catch {},
        .exec => writer.writeAll(
            \\Usage: lattice exec <path> --query="<cypher>" [options]
            \\
            \\Execute a single Cypher query and exit.
            \\
            \\Options:
            \\  --query=<cypher>      The Cypher query to execute
            \\  --file=<path>         Read query from file instead
            \\  --format=<fmt>        Output format: table, json, csv
            \\
            \\Examples:
            \\  lattice exec mydb.lattice --query="MATCH (n) RETURN count(n)"
            \\  lattice exec mydb.lattice --file=query.cypher --format=json
            \\
        ) catch {},
        .import => writer.writeAll(
            \\Usage: lattice import <path> --file=<input> [options]
            \\
            \\Import data from JSON or CSV files.
            \\
            \\Options:
            \\  --file=<path>         Input file (JSON or CSV)
            \\  --batch-size=<n>      Commit every N items (default: 1000)
            \\  --on-error=skip       Skip invalid records instead of aborting
            \\
            \\When --on-error=skip is set, records are applied individually so
            \\ successful rows are preserved.
            \\
            \\JSON format:
            \\  {"nodes": [...], "edges": [...]}
            \\
            \\CSV format (nodes):
            \\  _id,_labels,name,age
            \\  n1,"Person;Employee",Alice,30
            \\
            \\CSV format (edges):
            \\  _source,_target,_type,since
            \\  n1,n2,KNOWS,2020
            \\
        ) catch {},
        .@"export" => writer.writeAll(
            \\Usage: lattice export <path> --file=<output> [options]
            \\
            \\Export data to JSON, JSONL, CSV, or DOT files.
            \\
            \\Options:
            \\  --file=<path>         Output file path (.json, .jsonl, .csv, .dot)
            \\  --format=<fmt>        CLI output format: table, json, csv
            \\  --labels=<list>       Filter by labels (comma-separated)
            \\  --query=<cypher>      Export query results instead
            \\
            \\Examples:
            \\  lattice export mydb.lattice --file=backup.json
            \\  lattice export mydb.lattice --file=graph.jsonl
            \\  lattice export mydb.lattice --file=graph.dot
            \\  lattice export mydb.lattice --file=people.csv --labels=Person
            \\
        ) catch {},
        else => {
            writer.print("Usage: lattice {s} <path>\n\n", .{@tagName(command)}) catch {};
            writer.print("{s}\n", .{command.description()}) catch {};
        },
    }
}

fn cleanupTestDatabaseFiles(path: []const u8) void {
    @import("compat").fs.cwd().deleteFile(path) catch {};

    var wal_path_buf: [512]u8 = undefined;
    const wal_path = std.fmt.bufPrint(&wal_path_buf, "{s}-wal", .{path}) catch return;
    @import("compat").fs.cwd().deleteFile(wal_path) catch {};
}

test "checkDatabaseFile validates database pages" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_cli_check_ok.ltdb";
    cleanupTestDatabaseFiles(path);
    defer cleanupTestDatabaseFiles(path);

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{
            .enable_wal = false,
            .enable_fts = false,
        },
    });

    _ = try db.createNode(null, &[_][]const u8{"Person"});
    db.close();

    const stats = try checkDatabaseFile(allocator, path, true);
    try std.testing.expect(stats.pages_checked > 0);
    try std.testing.expect(!stats.wal_present);
}

test "checkDatabaseFile detects checksum mismatches" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_cli_check_corrupt.ltdb";
    cleanupTestDatabaseFiles(path);
    defer cleanupTestDatabaseFiles(path);

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{
            .enable_wal = false,
            .enable_fts = false,
        },
    });

    _ = try db.createNode(null, &[_][]const u8{"Person"});
    db.close();

    var file = try @import("compat").fs.cwd().openFile(path, .{ .mode = .read_write });
    defer file.close();
    try file.pwriteAll(&[_]u8{0xFF}, 4096 + 16);

    try std.testing.expectError(CheckError.ChecksumMismatch, checkDatabaseFile(allocator, path, true));
}

test "parse and run version" {
    // Basic smoke test
    const allocator = std.testing.allocator;
    var args = try Args.parse(allocator, &.{ "lattice", "version" });
    defer args.deinit(allocator);
    try std.testing.expectEqual(Command.version, args.command.?);
}
