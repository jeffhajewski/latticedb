//! Lattice CLI - Command-line interface for Lattice database.
//!
//! Provides interactive and batch operations for database management.

const std = @import("std");
const lattice = @import("lattice");
const args_mod = @import("args.zig");
const output = @import("output.zig");

const Args = args_mod.Args;
const Command = args_mod.Command;
const OutputFormat = args_mod.OutputFormat;

// Database types
const Database = lattice.storage.database.Database;
const DatabaseConfig = lattice.storage.database.DatabaseConfig;
const OpenOptions = lattice.storage.database.OpenOptions;

pub const VERSION = lattice.VERSION;

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
            error.UnknownCommand => output.printError(stderr, "Unknown command. Use 'lattice help' for available commands.", .{}),
            error.UnknownOption => output.printError(stderr, "Unknown option. Use 'lattice help' for available options.", .{}),
            error.InvalidFormat => output.printError(stderr, "Invalid format. Use: table, json, or csv", .{}),
            error.InvalidVectorDims => output.printError(stderr, "Invalid vector dimensions. Must be a positive integer.", .{}),
            error.InvalidCacheSize => output.printError(stderr, "Invalid cache size. Must be a positive integer.", .{}),
            error.InvalidBatchSize => output.printError(stderr, "Invalid batch size. Must be a positive integer.", .{}),
            else => output.printError(stderr, "Failed to parse arguments", .{}),
        }
        return;
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
        return;
    }

    // Dispatch to command handler
    runCommand(allocator, stdout, stderr, command, &parsed_args) catch |err| {
        output.printError(stderr, "Command failed: {s}", .{@errorName(err)});
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
        .labels => try cmdLabels(allocator, stdout, parsed_args),
        .types => try cmdTypes(allocator, stdout, parsed_args),
        .schema => try cmdSchema(allocator, stdout, parsed_args),
        .compact => try cmdCompact(stdout, stderr, parsed_args),
        .check => try cmdCheck(stdout, stderr, parsed_args),
        .import => try cmdImport(stdout, stderr, parsed_args),
        .@"export" => try cmdExport(stdout, stderr, parsed_args),
        .dump => try cmdDump(stdout, stderr, parsed_args),
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
        output.printError(stderr, "Database already exists: {s}", .{path});
        return;
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
        output.printError(stderr, "Failed to create database: {s}", .{@errorName(err)});
        return;
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
        output.printError(stderr, "Failed to open database: {s}", .{@errorName(err)});
        return;
    };
    defer db.close();

    const node_count = db.nodeCount();
    const edge_count = db.edgeCount();

    // Get file size
    const file = std.fs.cwd().openFile(path, .{}) catch {
        output.printError(stderr, "Cannot read file info", .{});
        return;
    };
    defer file.close();
    const stat = file.stat() catch {
        output.printError(stderr, "Cannot stat file", .{});
        return;
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
    const db = Database.open(allocator, path, .{
        .read_only = true,
    }) catch |err| {
        output.printError(stderr, "Failed to open database: {s}", .{@errorName(err)});
        return;
    };
    defer db.close();

    const node_count = db.nodeCount();
    const edge_count = db.edgeCount();

    // TODO: Get actual label and edge type counts when API is available
    const label_count: u64 = 0;
    const edge_type_count: u64 = 0;

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
    _ = allocator;
    const path = parsed_args.path.?;
    _ = stderr;

    // TODO: Implement REPL
    try stdout.print("LatticeDB v{s}\n", .{VERSION});
    try stdout.print("Connected to: {s}\n", .{path});
    try stdout.writeAll("Type .help for help, .exit to quit\n\n");
    try stdout.writeAll("Interactive query shell not yet implemented\n");
}

fn cmdExec(
    allocator: std.mem.Allocator,
    stdout: anytype,
    stderr: anytype,
    parsed_args: *const Args,
) !void {
    _ = allocator;

    const query_string = parsed_args.query_string orelse {
        output.printError(stderr, "No query provided. Use --query=\"...\" or provide as positional argument", .{});
        return;
    };

    // TODO: Execute query
    _ = query_string;
    output.printInfo(stdout, "Query execution not yet implemented", .{});
}

fn cmdLabels(
    allocator: std.mem.Allocator,
    stdout: anytype,
    parsed_args: *const Args,
) !void {
    _ = allocator;

    // TODO: List labels from database
    switch (parsed_args.format) {
        .table => {
            try stdout.writeAll("┌───────┬───────┐\n");
            try stdout.writeAll("│ Label │ Count │\n");
            try stdout.writeAll("├───────┼───────┤\n");
            try stdout.writeAll("│ (none)│ 0     │\n");
            try stdout.writeAll("└───────┴───────┘\n");
        },
        .json => try stdout.writeAll("{\"labels\":[]}\n"),
        .csv => try stdout.writeAll("label,count\n"),
    }
}

fn cmdTypes(
    allocator: std.mem.Allocator,
    stdout: anytype,
    parsed_args: *const Args,
) !void {
    _ = allocator;

    // TODO: List edge types from database
    switch (parsed_args.format) {
        .table => {
            try stdout.writeAll("┌──────┬───────┐\n");
            try stdout.writeAll("│ Type │ Count │\n");
            try stdout.writeAll("├──────┼───────┤\n");
            try stdout.writeAll("│(none)│ 0     │\n");
            try stdout.writeAll("└──────┴───────┘\n");
        },
        .json => try stdout.writeAll("{\"types\":[]}\n"),
        .csv => try stdout.writeAll("type,count\n"),
    }
}

fn cmdSchema(
    allocator: std.mem.Allocator,
    stdout: anytype,
    parsed_args: *const Args,
) !void {
    _ = allocator;
    _ = parsed_args;

    // TODO: Infer schema from database
    try stdout.writeAll("Schema inference not yet implemented\n");
}

fn cmdCompact(stdout: anytype, stderr: anytype, parsed_args: *const Args) !void {
    _ = stderr;
    const path = parsed_args.path.?;

    // TODO: Compact database
    output.printInfo(stdout, "Compacting {s}...", .{path});
    output.printInfo(stdout, "Compaction not yet implemented", .{});
}

fn cmdCheck(stdout: anytype, stderr: anytype, parsed_args: *const Args) !void {
    _ = stderr;
    const path = parsed_args.path.?;

    // TODO: Check database integrity
    output.printInfo(stdout, "Checking {s}...", .{path});
    output.printInfo(stdout, "Integrity check not yet implemented", .{});
}

fn cmdImport(stdout: anytype, stderr: anytype, parsed_args: *const Args) !void {
    const path = parsed_args.path.?;
    const file = parsed_args.file orelse {
        output.printError(stderr, "No import file specified. Use --file=<path>", .{});
        return;
    };

    // TODO: Import data
    output.printInfo(stdout, "Importing {s} into {s}...", .{ file, path });
    output.printInfo(stdout, "Import not yet implemented", .{});
}

fn cmdExport(stdout: anytype, stderr: anytype, parsed_args: *const Args) !void {
    const path = parsed_args.path.?;
    const file = parsed_args.file orelse {
        output.printError(stderr, "No export file specified. Use --file=<path>", .{});
        return;
    };

    // TODO: Export data
    output.printInfo(stdout, "Exporting {s} to {s}...", .{ path, file });
    output.printInfo(stdout, "Export not yet implemented", .{});
}

fn cmdDump(stdout: anytype, stderr: anytype, parsed_args: *const Args) !void {
    _ = stderr;
    const path = parsed_args.path.?;

    // TODO: Dump database
    output.printInfo(stdout, "Dumping {s}...", .{path});
    output.printInfo(stdout, "Dump not yet implemented", .{});
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
        \\    compact <path>      Compact database (reclaim space)
        \\    check <path>        Verify database integrity
        \\
        \\  Query:
        \\    query <path>        Interactive Cypher REPL
        \\    exec <path>         Execute a single query
        \\
        \\  Import/Export:
        \\    import <path>       Import data from JSON/CSV
        \\    export <path>       Export data to JSON/CSV
        \\    dump <path>         Dump full database as JSON
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
            \\Export data to JSON or CSV files.
            \\
            \\Options:
            \\  --file=<path>         Output file path
            \\  --format=<fmt>        Output format: json, csv
            \\  --labels=<list>       Filter by labels (comma-separated)
            \\  --query=<cypher>      Export query results instead
            \\
            \\Examples:
            \\  lattice export mydb.lattice --file=backup.json
            \\  lattice export mydb.lattice --file=people.csv --labels=Person
            \\
        ) catch {},
        else => {
            writer.print("Usage: lattice {s} <path>\n\n", .{@tagName(command)}) catch {};
            writer.print("{s}\n", .{command.description()}) catch {};
        },
    }
}

test "parse and run version" {
    // Basic smoke test
    const allocator = std.testing.allocator;
    var args = try Args.parse(allocator, &.{ "lattice", "version" });
    defer args.deinit(allocator);
    try std.testing.expectEqual(Command.version, args.command.?);
}
