//! Lattice CLI - Command-line interface for Lattice database.
//!
//! Provides interactive and batch operations for database management.

const std = @import("std");
const lattice = @import("lattice");
const args_mod = @import("args.zig");
const output = @import("output.zig");
const repl_mod = @import("repl.zig");
const import_export = @import("import_export.zig");

const Args = args_mod.Args;
const Command = args_mod.Command;
const OutputFormat = args_mod.OutputFormat;
const Repl = repl_mod.Repl;

// Database types
const Database = lattice.storage.database.Database;
const DatabaseConfig = lattice.storage.database.DatabaseConfig;
const OpenOptions = lattice.storage.database.OpenOptions;
const PageHeader = lattice.storage.page.PageHeader;
const PageManager = lattice.storage.page_manager.PageManager;
const PageManagerError = lattice.storage.page_manager.PageManagerError;
const PosixVfs = lattice.storage.vfs.PosixVfs;

pub const VERSION = lattice.VERSION;

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
};

const CheckStats = struct {
    pages_checked: u32,
    wal_present: bool,
};

fn failCommand(stderr: anytype, comptime fmt: []const u8, args: anytype) CliError {
    output.printError(stderr, fmt, args);
    return error.CommandFailed;
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const argv = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, argv);

    const stdout = std.fs.File.stdout().deprecatedWriter();
    const stderr = std.fs.File.stderr().deprecatedWriter();

    var parsed_args = Args.parse(allocator, argv) catch |err| {
        switch (err) {
            error.UnknownOption => output.printError(stderr, "Unknown option. Use 'lattice help' for available options.", .{}),
            error.InvalidFormat => output.printError(stderr, "Invalid format. Use: table, json, or csv", .{}),
            error.InvalidVectorDims => output.printError(stderr, "Invalid vector dimensions. Must be a positive integer.", .{}),
            error.InvalidCacheSize => output.printError(stderr, "Invalid cache size. Must be a positive integer.", .{}),
            error.InvalidBatchSize => output.printError(stderr, "Invalid batch size. Must be a positive integer.", .{}),
            else => output.printError(stderr, "Failed to parse arguments", .{}),
        }
        std.process.exit(1);
    };
    defer parsed_args.deinit(allocator);

    // Handle help flag
    if (parsed_args.help_requested) {
        if (parsed_args.command) |cmd| {
            printCommandHelp(stdout, cmd);
        } else {
            printUsage(stdout);
        }
        return;
    }

    // No command provided
    if (parsed_args.command == null) {
        printUsage(stdout);
        return;
    }

    const command = parsed_args.command.?;

    // Check for required path
    if (command.requiresPath() and parsed_args.path == null) {
        output.printError(stderr, "Missing database path", .{});
        try stderr.print("Usage: lattice {s} <path>\n", .{@tagName(command)});
        std.process.exit(1);
    }

    // Dispatch to command handler
    runCommand(allocator, stdout, stderr, command, &parsed_args) catch |err| {
        if (err != error.CommandFailed) {
            output.printError(stderr, "Command failed: {s}", .{@errorName(err)});
        }
        std.process.exit(1);
    };
}

fn runCommand(
    allocator: std.mem.Allocator,
    stdout: anytype,
    stderr: anytype,
    command: Command,
    parsed_args: *const Args,
) !void {
    switch (command) {
        .version => printVersion(stdout),
        .help => printUsage(stdout),
        .create => try cmdCreate(allocator, stdout, stderr, parsed_args),
        .info => try cmdInfo(allocator, stdout, stderr, parsed_args),
        .count => try cmdCount(allocator, stdout, stderr, parsed_args),
        .query => try cmdQuery(allocator, stdout, stderr, parsed_args),
        .exec => try cmdExec(allocator, stdout, stderr, parsed_args),
        .labels => try cmdLabels(allocator, stdout, stderr, parsed_args),
        .types => try cmdTypes(allocator, stdout, stderr, parsed_args),
        .schema => try cmdSchema(allocator, stdout, stderr, parsed_args),
        .compact => try cmdCompact(stdout, stderr, parsed_args),
        .check => try cmdCheck(allocator, stdout, stderr, parsed_args),
        .import => try cmdImport(allocator, stdout, stderr, parsed_args),
        .@"export" => try cmdExport(allocator, stdout, stderr, parsed_args),
        .dump => try cmdDump(allocator, stdout, stderr, parsed_args),
    }
}

// ============================================
// Command Implementations (stubs for now)
// ============================================

fn cmdCreate(
    allocator: std.mem.Allocator,
    stdout: anytype,
    stderr: anytype,
    parsed_args: *const Args,
) !void {
    const path = parsed_args.path.?;

    // Check if file already exists
    if (std.fs.cwd().access(path, .{})) |_| {
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
    }) catch |err| {
        return failCommand(stderr, "Failed to open database: {s}", .{@errorName(err)});
    };
    defer db.close();

    const node_count = db.nodeCount();
    const edge_count = db.edgeCount();

    // Get file size
    const file = std.fs.cwd().openFile(path, .{}) catch {
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
            try stdout.print("Vector:       {s}\n", .{if (db.config.enable_vector) "enabled" else "disabled"});
            try stdout.print("FTS:          {s}\n", .{if (db.config.enable_fts) "enabled" else "disabled"});
        },
        .json => {
            try stdout.print("{{\"path\":\"{s}\",\"size_kb\":{d},\"nodes\":{d},\"edges\":{d},\"format_version\":{d},\"vector_enabled\":{},\"fts_enabled\":{}}}\n", .{
                path,
                size_kb,
                node_count,
                edge_count,
                lattice.core.types.FORMAT_VERSION,
                db.config.enable_vector,
                db.config.enable_fts,
            });
        },
        .csv => {
            try stdout.writeAll("property,value\n");
            try stdout.print("path,{s}\n", .{path});
            try stdout.print("size_kb,{d}\n", .{size_kb});
            try stdout.print("nodes,{d}\n", .{node_count});
            try stdout.print("edges,{d}\n", .{edge_count});
            try stdout.print("format_version,{d}\n", .{lattice.core.types.FORMAT_VERSION});
            try stdout.print("vector_enabled,{}\n", .{db.config.enable_vector});
            try stdout.print("fts_enabled,{}\n", .{db.config.enable_fts});
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
    }) catch |err| {
        return failCommand(stderr, "Failed to open database: {s}", .{@errorName(err)});
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
    const db = Database.open(allocator, path, .{}) catch |err| {
        return failCommand(stderr, "Failed to open database: {s}", .{@errorName(err)});
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
        const file = std.fs.cwd().openFile(file_path, .{}) catch |err| {
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
    const db = Database.open(allocator, path, .{}) catch |err| {
        return failCommand(stderr, "Failed to open database: {s}", .{@errorName(err)});
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
    }) catch |err| {
        return failCommand(stderr, "Failed to open database: {s}", .{@errorName(err)});
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
    }) catch |err| {
        return failCommand(stderr, "Failed to open database: {s}", .{@errorName(err)});
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
    }) catch |err| {
        return failCommand(stderr, "Failed to open database: {s}", .{@errorName(err)});
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

fn cmdCompact(stdout: anytype, stderr: anytype, parsed_args: *const Args) !void {
    _ = stdout;
    _ = parsed_args;
    return failCommand(stderr, "The 'compact' command is reserved but not currently supported.", .{});
}

fn cmdCheck(
    allocator: std.mem.Allocator,
    stdout: anytype,
    stderr: anytype,
    parsed_args: *const Args,
) !void {
    const path = parsed_args.path.?;
    const stats = checkDatabaseFile(allocator, path) catch |err| switch (err) {
        error.FileNotFound => return failCommand(stderr, "Database file not found: {s}", .{path}),
        error.PermissionDenied => return failCommand(stderr, "Permission denied while checking: {s}", .{path}),
        error.InvalidDatabase => return failCommand(stderr, "Invalid database file: {s}", .{path}),
        error.ChecksumMismatch => return failCommand(stderr, "Checksum mismatch detected in database file: {s}", .{path}),
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

fn checkDatabaseFile(allocator: std.mem.Allocator, path: []const u8) CheckError!CheckStats {
    var posix_vfs = PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    var page_manager = PageManager.init(allocator, vfs_impl, path, .{
        .read_only = true,
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
        error.InvalidHeader, error.InvalidMagic, error.VersionTooNew, error.InvalidPageId, error.PageNotAllocated => CheckError.InvalidDatabase,
        else => CheckError.IoError,
    };
}

fn hasWalSibling(path: []const u8) bool {
    var wal_path_buf: [512]u8 = undefined;
    const wal_path = std.fmt.bufPrint(&wal_path_buf, "{s}-wal", .{path}) catch return false;
    std.fs.cwd().access(wal_path, .{}) catch return false;
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
    var db = Database.open(allocator, path, .{}) catch |err| {
        return failCommand(stderr, "Failed to open database: {s}", .{@errorName(err)});
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
    }) catch |err| {
        return failCommand(stderr, "Failed to open database: {s}", .{@errorName(err)});
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
        const out_file = std.fs.cwd().createFile(file, .{}) catch |err| {
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

        const nodes_file = std.fs.cwd().createFile(nodes_path, .{}) catch |err| {
            return failCommand(stderr, "Cannot create nodes file: {s}", .{@errorName(err)});
        };
        defer nodes_file.close();

        const edges_file = std.fs.cwd().createFile(edges_path, .{}) catch |err| {
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
    }) catch |err| {
        return failCommand(stderr, "Failed to open database: {s}", .{@errorName(err)});
    };
    defer db.close();

    // Dump to stdout as canonical JSON
    const stats = import_export.dumpCanonicalJson(allocator, db, stdout, parsed_args.label_filter);

    if (stats) |s| {
        // Print stats to stderr so they don't mix with JSON output
        const stderr_writer = std.fs.File.stderr().deprecatedWriter();
        try stderr_writer.print("Dumped {d} nodes, {d} edges\n", .{ s.nodes_exported, s.edges_exported });
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
        \\    version             Show version information
        \\    help                Show this help message
        \\
        \\Options:
        \\  --format=<fmt>        Output format: table, json, csv (default: table)
        \\  --enable-vector       Enable vector index (for create)
        \\  --vector-dims=<n>     Vector dimensions (default: 128)
        \\  --enable-fts          Enable full-text search (default: on)
        \\  --no-fts              Disable full-text search
        \\  --cache-size=<mb>     Buffer pool size in MB (default: 64)
        \\  --file=<path>         Input/output file for import/export
        \\  --query=<cypher>      Query string for exec command
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
        .compact => writer.writeAll(
            \\Usage: lattice compact <path>
            \\
            \\This command name is reserved but not currently supported.
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
    std.fs.cwd().deleteFile(path) catch {};

    var wal_path_buf: [512]u8 = undefined;
    const wal_path = std.fmt.bufPrint(&wal_path_buf, "{s}-wal", .{path}) catch return;
    std.fs.cwd().deleteFile(wal_path) catch {};
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

    const stats = try checkDatabaseFile(allocator, path);
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

    var file = try std.fs.cwd().openFile(path, .{ .mode = .read_write });
    defer file.close();
    try file.pwriteAll(&[_]u8{0xFF}, 4096 + 16);

    try std.testing.expectError(CheckError.ChecksumMismatch, checkDatabaseFile(allocator, path));
}

test "parse and run version" {
    // Basic smoke test
    const allocator = std.testing.allocator;
    var args = try Args.parse(allocator, &.{ "lattice", "version" });
    defer args.deinit(allocator);
    try std.testing.expectEqual(Command.version, args.command.?);
}
