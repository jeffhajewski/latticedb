//! Command-line argument parsing for Lattice CLI.

const std = @import("std");

/// Managed array list for allocator tracking
fn ManagedArrayList(comptime T: type) type {
    return std.array_list.Managed(T);
}

pub const OutputFormat = enum {
    table,
    json,
    csv,

    pub fn fromString(s: []const u8) ?OutputFormat {
        if (std.mem.eql(u8, s, "table")) return .table;
        if (std.mem.eql(u8, s, "json")) return .json;
        if (std.mem.eql(u8, s, "csv")) return .csv;
        return null;
    }
};

pub const Command = enum {
    // Database
    create,
    info,
    compact,
    check,

    // Query
    query,
    exec,

    // Import/Export
    import,
    @"export",
    dump,

    // Introspection
    labels,
    types,
    schema,
    count,

    // Utility
    version,
    help,

    pub fn fromString(s: []const u8) ?Command {
        if (std.mem.eql(u8, s, "create")) return .create;
        if (std.mem.eql(u8, s, "info")) return .info;
        if (std.mem.eql(u8, s, "compact")) return .compact;
        if (std.mem.eql(u8, s, "check")) return .check;
        if (std.mem.eql(u8, s, "query")) return .query;
        if (std.mem.eql(u8, s, "exec")) return .exec;
        if (std.mem.eql(u8, s, "import")) return .import;
        if (std.mem.eql(u8, s, "export")) return .@"export";
        if (std.mem.eql(u8, s, "dump")) return .dump;
        if (std.mem.eql(u8, s, "labels")) return .labels;
        if (std.mem.eql(u8, s, "types")) return .types;
        if (std.mem.eql(u8, s, "schema")) return .schema;
        if (std.mem.eql(u8, s, "count")) return .count;
        if (std.mem.eql(u8, s, "version") or std.mem.eql(u8, s, "-v") or std.mem.eql(u8, s, "--version")) return .version;
        if (std.mem.eql(u8, s, "help") or std.mem.eql(u8, s, "-h") or std.mem.eql(u8, s, "--help")) return .help;
        return null;
    }

    pub fn requiresPath(self: Command) bool {
        return switch (self) {
            .version, .help => false,
            else => true,
        };
    }

    pub fn description(self: Command) []const u8 {
        return switch (self) {
            .create => "Create a new database",
            .info => "Show database information",
            .compact => "Compact database (reclaim space)",
            .check => "Verify database integrity",
            .query => "Interactive Cypher REPL",
            .exec => "Execute a single query",
            .import => "Import data from JSON/CSV",
            .@"export" => "Export data to JSON/CSV",
            .dump => "Dump full database as JSON",
            .labels => "List all node labels",
            .types => "List all edge types",
            .schema => "Show inferred schema",
            .count => "Show node/edge counts",
            .version => "Show version information",
            .help => "Show help message",
        };
    }
};

pub const Args = struct {
    command: ?Command = null,
    path: ?[]const u8 = null,
    query_string: ?[]const u8 = null,
    file: ?[]const u8 = null,
    format: OutputFormat = .table,
    help_requested: bool = false,

    // Database options
    enable_vector: bool = false,
    vector_dims: u16 = 128,
    enable_fts: bool = true,
    cache_size_mb: u32 = 64,
    page_size: u32 = 4096,

    // Import options
    batch_size: u32 = 1000,
    on_error_skip: bool = false,

    // Filter options
    label_filter: ?[]const u8 = null,

    // Remaining positional args
    positional: []const []const u8 = &.{},

    pub fn parse(allocator: std.mem.Allocator, argv: []const []const u8) !Args {
        var args = Args{};
        var positional = ManagedArrayList([]const u8).init(allocator);
        errdefer positional.deinit();

        var i: usize = 1; // Skip program name
        while (i < argv.len) : (i += 1) {
            const arg = argv[i];

            if (std.mem.startsWith(u8, arg, "--")) {
                // Long option
                if (std.mem.eql(u8, arg, "--help")) {
                    args.help_requested = true;
                } else if (std.mem.eql(u8, arg, "--version")) {
                    args.command = .version;
                } else if (std.mem.startsWith(u8, arg, "--format=")) {
                    const value = arg["--format=".len..];
                    args.format = OutputFormat.fromString(value) orelse {
                        return error.InvalidFormat;
                    };
                } else if (std.mem.eql(u8, arg, "--enable-vector")) {
                    args.enable_vector = true;
                } else if (std.mem.startsWith(u8, arg, "--vector-dims=")) {
                    const value = arg["--vector-dims=".len..];
                    args.vector_dims = std.fmt.parseInt(u16, value, 10) catch {
                        return error.InvalidVectorDims;
                    };
                } else if (std.mem.eql(u8, arg, "--enable-fts")) {
                    args.enable_fts = true;
                } else if (std.mem.eql(u8, arg, "--no-fts")) {
                    args.enable_fts = false;
                } else if (std.mem.startsWith(u8, arg, "--cache-size=")) {
                    const value = arg["--cache-size=".len..];
                    args.cache_size_mb = std.fmt.parseInt(u32, value, 10) catch {
                        return error.InvalidCacheSize;
                    };
                } else if (std.mem.startsWith(u8, arg, "--batch-size=")) {
                    const value = arg["--batch-size=".len..];
                    args.batch_size = std.fmt.parseInt(u32, value, 10) catch {
                        return error.InvalidBatchSize;
                    };
                } else if (std.mem.eql(u8, arg, "--on-error=skip")) {
                    args.on_error_skip = true;
                } else if (std.mem.startsWith(u8, arg, "--labels=")) {
                    args.label_filter = arg["--labels=".len..];
                } else if (std.mem.startsWith(u8, arg, "--file=")) {
                    args.file = arg["--file=".len..];
                } else if (std.mem.startsWith(u8, arg, "--query=")) {
                    args.query_string = arg["--query=".len..];
                } else {
                    return error.UnknownOption;
                }
            } else if (std.mem.startsWith(u8, arg, "-")) {
                // Short option
                if (std.mem.eql(u8, arg, "-h")) {
                    args.help_requested = true;
                } else if (std.mem.eql(u8, arg, "-v")) {
                    args.command = .version;
                } else if (std.mem.eql(u8, arg, "-f") and i + 1 < argv.len) {
                    i += 1;
                    args.format = OutputFormat.fromString(argv[i]) orelse {
                        return error.InvalidFormat;
                    };
                } else {
                    return error.UnknownOption;
                }
            } else {
                // Positional argument
                if (args.command == null) {
                    args.command = Command.fromString(arg);
                    if (args.command == null) {
                        // Not a known command - treat as path and default to query (REPL)
                        args.command = .query;
                        args.path = arg;
                    }
                } else if (args.path == null and args.command.?.requiresPath()) {
                    args.path = arg;
                } else {
                    try positional.append(arg);
                }
            }
        }

        args.positional = try positional.toOwnedSlice();
        return args;
    }

    pub fn deinit(self: *Args, allocator: std.mem.Allocator) void {
        allocator.free(self.positional);
    }
};

pub const Error = error{
    InvalidFormat,
    InvalidVectorDims,
    InvalidCacheSize,
    InvalidBatchSize,
    UnknownOption,
    MissingPath,
    OutOfMemory,
};

test "parse basic command" {
    const allocator = std.testing.allocator;
    var args = try Args.parse(allocator, &.{ "lattice", "version" });
    defer args.deinit(allocator);

    try std.testing.expectEqual(Command.version, args.command.?);
}

test "parse command with path" {
    const allocator = std.testing.allocator;
    var args = try Args.parse(allocator, &.{ "lattice", "info", "test.db" });
    defer args.deinit(allocator);

    try std.testing.expectEqual(Command.info, args.command.?);
    try std.testing.expectEqualStrings("test.db", args.path.?);
}

test "parse format option" {
    const allocator = std.testing.allocator;
    var args = try Args.parse(allocator, &.{ "lattice", "count", "test.db", "--format=json" });
    defer args.deinit(allocator);

    try std.testing.expectEqual(OutputFormat.json, args.format);
}
