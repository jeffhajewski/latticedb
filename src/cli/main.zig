//! Lattice CLI - Command-line interface for Lattice database.
//!
//! Provides interactive and batch operations for database management.

const std = @import("std");
const compat = @import("compat");
const lattice = @import("lattice");
const args_mod = @import("args.zig");
const output = @import("output.zig");
const repl_mod = @import("repl.zig");
const import_export = @import("import_export.zig");
const rdf = import_export.rdf;

// A file's tests reach the test binary only once something analyzes the file,
// and much of the CLI is generic code no test instantiates, so the files are
// named here. args.zig and key.zig are test roots of their own.
test {
    _ = output;
    _ = repl_mod;
    _ = import_export;
    _ = @import("history.zig");
    _ = @import("rdf/rdf.zig");
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
const QueryResult = lattice.storage.database.QueryResult;
const PageHeader = lattice.storage.page.PageHeader;
const FileHeader = lattice.storage.page.FileHeader;
const PageManager = lattice.storage.page_manager.PageManager;
const PageManagerError = lattice.storage.page_manager.PageManagerError;
const PosixVfs = lattice.storage.vfs.PosixVfs;
const OverlayVfs = lattice.storage.overlay_vfs.OverlayVfs;
const journal_mod = lattice.storage.journal;

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
    VersionTooNew,
};

const CheckStats = struct {
    pages_checked: u32,
    /// The header's `min_reader_version` and the reasons it records.
    min_reader_version: u16 = 0,
    min_reader_reasons: u16 = 0,
    /// Pages never written (all zeros), counted in pages_checked.
    blank_pages: u32 = 0,
    /// A flush a crash cut short was finished from the journal, in memory.
    journal_applied: bool = false,
    wal_present: bool,
};

/// The names of the `FileHeader.ReaderReason` bits in `reasons`, joined by
/// `separator`, each in quotes when `quoted`.
fn writeReaderReasons(writer: anytype, reasons: u16, separator: []const u8, quoted: bool) !void {
    var first = true;
    for (FileHeader.ReaderReason.all) |reason| {
        if (reasons & reason.bit == 0) continue;
        if (!first) try writer.writeAll(separator);
        first = false;
        if (quoted) try writer.writeAll("\"");
        try writer.writeAll(reason.name);
        if (quoted) try writer.writeAll("\"");
    }
}

/// The table line for the reader version: which older releases are refused,
/// and why, when any are.
fn writeReaderVersionLine(writer: anytype, label: []const u8, version: u16, reasons: u16) !void {
    try writer.print("{s}v{d}", .{ label, version });
    if (version > lattice.core.types.MIN_READER_VERSION) {
        try writer.print(", so {s} cannot open it (", .{refusedReleases(version)});
        if (reasons == 0) {
            try writer.writeAll("reason not recorded");
        } else {
            try writeReaderReasons(writer, reasons, ", ", false);
        }
        try writer.writeAll(")");
    }
    try writer.writeAll("\n");
}

/// The releases a file with `min_reader_version` `version` (above 3) shuts
/// out. Format 4 began with 0.17.0, format 5 after 0.18.
fn refusedReleases(version: u16) []const u8 {
    return if (version >= lattice.core.types.ORDERED_ID_KEYS_READER_VERSION) "0.18 and earlier" else "0.16.0";
}

/// What `lattice info` says about the file's id keys.
fn idKeysDescription(order: lattice.core.id_key.IdKeyOrder) []const u8 {
    return switch (order) {
        .big => "big-endian (format 5: scans return ascending ids)",
        .little => "little-endian (created before format 5)",
    };
}

fn failCommand(stderr: anytype, comptime fmt: []const u8, args: anytype) CliError {
    output.printError(stderr, fmt, args);
    return error.CommandFailed;
}

pub fn main(init: std.process.Init) !u8 {
    const allocator = init.gpa;
    const argv = try collectProcessArgs(allocator, init.minimal.args);
    defer freeProcessArgs(allocator, argv);

    // Streaming, not positional: output redirected with >> must append, and
    // output to a file opened at an offset must not start back at zero.
    var stdout_buffer: [4096]u8 = undefined;
    var stdout_writer = std.Io.File.stdout().writerStreaming(init.io, &stdout_buffer);
    const stdout = &stdout_writer.interface;
    defer stdout.flush() catch {};

    var stderr_buffer: [4096]u8 = undefined;
    var stderr_writer = std.Io.File.stderr().writerStreaming(init.io, &stderr_buffer);
    const stderr = &stderr_writer.interface;
    defer stderr.flush() catch {};

    var parsed_args = Args.parse(allocator, argv) catch |err| {
        switch (err) {
            error.UnknownOption => output.printError(stderr, "Unknown option. Use 'lattice help' for available options.", .{}),
            error.InvalidFormat => output.printError(stderr, "Invalid format. Use: table, json, or csv", .{}),
            error.InvalidVectorDims => output.printError(stderr, "Invalid vector dimensions. Must be in the range 1..4096.", .{}),
            error.InvalidVectorMetric => output.printError(stderr, "Invalid vector metric. Use: cosine, euclidean or dot_product", .{}),
            error.InvalidVectorM => output.printError(stderr, "Invalid --vector-m. Must be in the range 2..32.", .{}),
            error.InvalidVectorEfConstruction => output.printError(stderr, "Invalid --vector-ef-construction. Must be in the range 1..4096.", .{}),
            error.InvalidChangefeedRetention => output.printError(stderr, "Invalid --changefeed. Use: all, off, last:<events> or age:<duration> (such as age:7d, age:12h, age:30m).", .{}),
            error.VectorOptionWithoutEnableVector => output.printError(stderr, "--vector-dims, --vector-metric, --vector-m and --vector-ef-construction need --enable-vector.", .{}),
            error.IntervalWithoutFollow => output.printError(stderr, "--interval needs --follow.", .{}),
            error.InvalidCacheSize => output.printError(stderr, "Invalid cache size. Must be a positive integer.", .{}),
            error.InvalidAutoCheckpoint => output.printError(stderr, "Invalid --auto-checkpoint. Must be a whole number of log frames, 0 for none.", .{}),
            error.InvalidPageSize => output.printError(stderr, "Invalid page size. Must be in the range 4096..65535 bytes.", .{}),
            error.InvalidBatchSize => output.printError(stderr, "Invalid batch size. Must be a positive integer.", .{}),
            error.InvalidInterval => output.printError(stderr, "Invalid interval. Must be a whole number of seconds, at least 1.", .{}),
            error.InvalidTimestamp => output.printError(stderr, "Invalid time. Use a UTC time such as 2026-08-25T14:30:00Z or a date such as 2026-08-25.", .{}),
            error.InvalidRdfOption => output.printError(stderr, "Invalid RDF option. See 'lattice import --help' for the values each --rdf-* option takes; --base takes an absolute IRI.", .{}),
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
        .changefeed => try cmdChangefeed(allocator, stdout, stderr, parsed_args),
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
        .vector_metric = parsed_args.vector_metric,
        .vector_m = parsed_args.vector_m,
        .vector_ef_construction = parsed_args.vector_ef_construction,
        .enable_fts = parsed_args.enable_fts,
        // --cache-size wins over LATTICE_BUFFER_POOL_MB, which wins over 64 MB.
        .buffer_pool_size = if (parsed_args.cache_size_mb) |mb|
            @as(usize, mb) * 1024 * 1024
        else if (DatabaseConfig.environmentBufferPoolSize() != null)
            0
        else
            64 * 1024 * 1024,
        .changefeed_retention = parsed_args.changefeed_retention,
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
        const metric_name = if (parsed_args.vector_metric) |metric| metric.name() else "cosine";
        try stdout.print("  Vector index enabled (dimensions: {d}, metric: {s})\n", .{ parsed_args.vector_dims, metric_name });
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
    const db = try openDatabase(allocator, stderr, parsed_args, .read_only);
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
    // A database with a vector index opens with it, so its configuration is
    // the file's.
    const vector_config = db.vectorConfig();

    const feed = db.changefeedStatus() catch |err| {
        return failCommand(stderr, "Failed to read the changefeed: {s}", .{@errorName(err)});
    };
    var policy_buf: [48]u8 = undefined;
    const policy = feed.retention.format(&policy_buf);
    const indexes = db.propertyIndexes(allocator) catch |err| {
        return failCommand(stderr, "Failed to list the property indexes: {s}", .{@errorName(err)});
    };
    defer lattice.storage.database.Database.freePropertyIndexes(allocator, indexes);
    const file_header = db.page_manager.getHeader();
    const reader_version = file_header.min_reader_version;
    const reader_reasons = file_header.readerReasons();
    // The file's own format, as the header records it: info opens read-only
    // and so does not upgrade it. Id keys are fixed when a file is created.
    const file_format = file_header.format_version;
    const id_keys = file_header.idKeyOrder();

    switch (parsed_args.format) {
        .table => {
            try stdout.print("Database: {s}\n", .{path});
            try stdout.writeAll("─────────────────────────────\n");
            try stdout.print("Size:         {d} KB\n", .{size_kb});
            try stdout.print("Nodes:        {d}\n", .{node_count});
            try stdout.print("Edges:        {d}\n", .{edge_count});
            try stdout.print("Format:       v{d}\n", .{file_format});
            try stdout.print("Id keys:      {s}\n", .{idKeysDescription(id_keys)});
            if (id_keys == .little) {
                try stdout.writeAll("              'lattice compact --upgrade-id-keys' converts them (one-way: only format-5 readers open the result)\n");
            }
            try writeReaderVersionLine(stdout, "Min reader:   ", reader_version, reader_reasons);
            try stdout.print("Vector:       {s}\n", .{if (db.hasPersistedVectorIndex()) "enabled" else "disabled"});
            if (vector_config) |config| {
                try stdout.print("  Dimensions: {d}\n", .{config.dimensions});
                try stdout.print("  Metric:     {s}\n", .{config.metric.name()});
                try stdout.print("  M:          {d}\n", .{config.m});
                try stdout.print("  ef_construction: {d}\n", .{config.ef_construction});
            }
            try stdout.print("FTS:          {s}\n", .{if (db.hasPersistedFtsIndex()) "enabled" else "disabled"});
            if (feed.retention == .off) {
                try stdout.writeAll("Changefeed:   off, ");
            } else {
                try stdout.print("Changefeed:   keep {s}, ", .{policy});
            }
            if (feed.oldest_sequence) |oldest| {
                try stdout.print("events {d}..{d}\n", .{ oldest, feed.last_sequence });
            } else {
                try stdout.print("no events held (last sequence {d})\n", .{feed.last_sequence});
            }
            for (indexes) |index| {
                const open_mark, const close_mark = if (index.kind == .node) .{ ":", "" } else .{ "[:", "]" };
                try stdout.print("Index:        {s}{s}{s}({s})  {s}\n", .{ open_mark, index.scope, close_mark, index.property, index.encoding.name() });
            }
        },
        .json => {
            try stdout.print("{{\"path\":\"{s}\",\"size_kb\":{d},\"nodes\":{d},\"edges\":{d},\"format_version\":{d},\"id_keys\":\"{s}\",\"vector_enabled\":{},", .{
                path,
                size_kb,
                node_count,
                edge_count,
                file_format,
                id_keys.name(),
                db.hasPersistedVectorIndex(),
            });
            try stdout.print("\"min_reader_version\":{d},\"min_reader_reasons\":[", .{reader_version});
            try writeReaderReasons(stdout, reader_reasons, ",", true);
            try stdout.writeAll("],");
            if (vector_config) |config| {
                try stdout.print("\"vector_dimensions\":{d},\"vector_metric\":\"{s}\",\"vector_m\":{d},\"vector_ef_construction\":{d},", .{
                    config.dimensions,
                    config.metric.name(),
                    config.m,
                    config.ef_construction,
                });
            }
            try stdout.print("\"fts_enabled\":{},\"changefeed_retention\":\"{s}\",\"changefeed_oldest_sequence\":{d},\"changefeed_last_sequence\":{d},\"property_indexes\":[", .{
                db.hasPersistedFtsIndex(),
                policy,
                feed.oldest_sequence orelse 0,
                feed.last_sequence,
            });
            for (indexes, 0..) |index, i| {
                if (i > 0) try stdout.writeAll(",");
                try stdout.print("{{\"entity\":\"{s}\",\"scope\":{f},\"property\":{f},\"encoding\":\"{s}\"}}", .{
                    if (index.kind == .node) "node" else "relationship",
                    std.json.fmt(index.scope, .{}),
                    std.json.fmt(index.property, .{}),
                    index.encoding.name(),
                });
            }
            try stdout.writeAll("]}\n");
        },
        .csv => {
            try stdout.writeAll("property,value\n");
            try stdout.print("path,{s}\n", .{path});
            try stdout.print("size_kb,{d}\n", .{size_kb});
            try stdout.print("nodes,{d}\n", .{node_count});
            try stdout.print("edges,{d}\n", .{edge_count});
            try stdout.print("format_version,{d}\n", .{file_format});
            try stdout.print("id_keys,{s}\n", .{id_keys.name()});
            try stdout.print("min_reader_version,{d}\nmin_reader_reasons,", .{reader_version});
            try writeReaderReasons(stdout, reader_reasons, ";", false);
            try stdout.writeAll("\n");
            try stdout.print("vector_enabled,{}\n", .{db.hasPersistedVectorIndex()});
            if (vector_config) |config| {
                try stdout.print("vector_dimensions,{d}\n", .{config.dimensions});
                try stdout.print("vector_metric,{s}\n", .{config.metric.name()});
                try stdout.print("vector_m,{d}\n", .{config.m});
                try stdout.print("vector_ef_construction,{d}\n", .{config.ef_construction});
            }
            try stdout.print("fts_enabled,{}\n", .{db.hasPersistedFtsIndex()});
            try stdout.print("changefeed_retention,{s}\n", .{policy});
            try stdout.print("changefeed_oldest_sequence,{d}\n", .{feed.oldest_sequence orelse 0});
            try stdout.print("changefeed_last_sequence,{d}\n", .{feed.last_sequence});
            for (indexes) |index| {
                const open_mark, const close_mark = if (index.kind == .node) .{ ":", "" } else .{ "[:", "]" };
                try stdout.print("property_index,{s}{s}{s}({s}) {s}\n", .{ open_mark, index.scope, close_mark, index.property, index.encoding.name() });
            }
        },
    }
}

fn cmdCount(
    allocator: std.mem.Allocator,
    stdout: anytype,
    stderr: anytype,
    parsed_args: *const Args,
) !void {

    // Open database
    const db = try openDatabase(allocator, stderr, parsed_args, .read_only);
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

    // A database this version may read but not write still opens, for
    // reading, with a word about why writes will be refused.
    const path = parsed_args.path.?;
    const db = Database.open(allocator, path, openOptions(parsed_args, .read_write)) catch |err| blk: {
        if (err != DatabaseError.NeedsNewerWriter) return failOpen(stderr, path, err, parsed_args.enable_vector);
        const reader = try openDatabase(allocator, stderr, parsed_args, .read_only);
        output.printInfo(stderr, "{s} was written by a newer LatticeDB that does not let this version write to it; opened read-only.", .{path});
        break :blk reader;
    };
    defer db.close();
    applyAutoCheckpoint(db, parsed_args);

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

    // A statement that only reads opens the database read-only: it takes the
    // shared lock, changes nothing on disk, and works on a file this version
    // may read but not write.
    // An in-memory database exists only once opened for writing.
    const reads_only = !statementWrites(allocator, query_string) and
        !lattice.storage.database.isMemoryPath(parsed_args.path.?);
    const mode: OpenMode = if (reads_only) .read_only else .read_write;
    const db = try openDatabase(allocator, stderr, parsed_args, mode);
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

/// Whether a statement can change the database, as the engine decides it
/// for choosing a transaction. One that does not parse counts as reading:
/// running it reports the parse error either way.
fn statementWrites(allocator: std.mem.Allocator, statement: []const u8) bool {
    var parser = lattice.query.parser.Parser.init(allocator, statement);
    defer parser.deinit();
    const parsed = parser.parse().query orelse return false;
    return parsed.writesData();
}

fn cmdLabels(
    allocator: std.mem.Allocator,
    stdout: anytype,
    stderr: anytype,
    parsed_args: *const Args,
) !void {

    // Open database
    const db = try openDatabase(allocator, stderr, parsed_args, .read_only);
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

    // Open database
    const db = try openDatabase(allocator, stderr, parsed_args, .read_only);
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

    // Open database
    const db = try openDatabase(allocator, stderr, parsed_args, .read_only);
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

    // First, before anything else opens the file: the upgraded copy takes
    // the original's place, and the rest of compact runs on it.
    const key_upgrade: ?IdKeyUpgradeStats = if (parsed_args.upgrade_id_keys)
        try upgradeIdKeys(allocator, stderr, parsed_args)
    else
        null;

    const db = try openDatabase(allocator, stderr, parsed_args, .read_write);
    defer db.close();

    // Rebuilt first, so that the pages the old entries free can go too.
    const upgraded: ?usize = if (parsed_args.upgrade_indexes)
        db.upgradePropertyIndexes() catch |err| {
            return failCommand(stderr, "Failed to rebuild the property indexes: {s}", .{@errorName(err)});
        }
    else
        null;

    const stats = db.compact() catch |err| {
        return failCommand(stderr, "Failed to compact database: {s}", .{@errorName(err)});
    };

    switch (parsed_args.format) {
        .table => {
            if (key_upgrade) |k| {
                if (k.already_ordered) {
                    try stdout.writeAll("Id keys are already big-endian (format 5); nothing to upgrade.\n");
                } else {
                    output.printSuccess(stdout, "Upgraded the id keys of {s} to big-endian (format 5)", .{path});
                    try stdout.writeAll("  This is one-way: 0.18 and earlier can no longer open this file.\n");
                    for (IdKeyUpgrade.IdTree.all) |tree| {
                        const t = k.tree(tree);
                        if (!t.present) continue;
                        try stdout.print("  {s}: {d} entries, {d} pages before, {d} after\n", .{ tree.name(), t.entries, t.pages_before, t.pages_after });
                    }
                    try stdout.print("  Entries checked: {d}\n", .{k.entries_verified});
                    try stdout.print("  Pages checked unchanged: {d}\n", .{k.pages_compared});
                    try stdout.print("  Size before:     {d} bytes\n", .{k.bytes_before});
                    try stdout.print("  Size upgraded:   {d} bytes\n", .{k.bytes_after});
                    try stdout.print("  {d} pages freed for reuse; the file shrinks only if they are at its end.\n", .{k.pages_freed});
                    try stdout.writeAll("  The file has a new id: replicate it to a new directory, not an old one.\n");
                }
            }
            output.printSuccess(stdout, "Compacted database: {s}", .{path});
            if (upgraded) |n| try stdout.print("  Indexes rebuilt: {d}\n", .{n});
            try stdout.print("  Pages before:    {d}\n", .{stats.pages_before});
            try stdout.print("  Pages after:     {d}\n", .{stats.pages_after});
            try stdout.print("  Pages removed:   {d}\n", .{stats.pages_removed});
            try stdout.print("  Bytes reclaimed: {d}\n", .{stats.bytes_reclaimed});
        },
        .json => {
            try stdout.print(
                "{{\"path\":\"{s}\",\"pages_before\":{d},\"pages_after\":{d},\"pages_removed\":{d},\"bytes_reclaimed\":{d}",
                .{ path, stats.pages_before, stats.pages_after, stats.pages_removed, stats.bytes_reclaimed },
            );
            if (upgraded) |n| try stdout.print(",\"indexes_rebuilt\":{d}", .{n});
            if (key_upgrade) |k| {
                try stdout.print(
                    ",\"id_keys_upgraded\":{},\"id_keys\":\"big-endian\",\"id_key_entries_checked\":{d},\"id_key_pages_freed\":{d},\"upgrade_bytes_before\":{d},\"upgrade_bytes_after\":{d}",
                    .{ !k.already_ordered, k.entries_verified, k.pages_freed, k.bytes_before, k.bytes_after },
                );
            }
            try stdout.writeAll("}\n");
        },
        .csv => {
            try stdout.writeAll("path,pages_before,pages_after,pages_removed,bytes_reclaimed");
            if (upgraded != null) try stdout.writeAll(",indexes_rebuilt");
            if (key_upgrade != null) try stdout.writeAll(",id_keys_upgraded,id_keys,id_key_entries_checked,id_key_pages_freed,upgrade_bytes_before,upgrade_bytes_after");
            try stdout.print(
                "\n{s},{d},{d},{d},{d}",
                .{ path, stats.pages_before, stats.pages_after, stats.pages_removed, stats.bytes_reclaimed },
            );
            if (upgraded) |n| try stdout.print(",{d}", .{n});
            if (key_upgrade) |k| {
                try stdout.print(",{},big-endian,{d},{d},{d},{d}", .{ !k.already_ordered, k.entries_verified, k.pages_freed, k.bytes_before, k.bytes_after });
            }
            try stdout.writeAll("\n");
        },
    }
}

const IdKeyUpgrade = lattice.storage.id_key_upgrade;
const IdKeyUpgradeStats = IdKeyUpgrade.UpgradeStats;

/// Where `compact --upgrade-id-keys` writes the upgraded copy: beside the
/// database, so the rename that replaces it stays on one filesystem.
fn upgradeCopyPath(allocator: std.mem.Allocator, path: []const u8) ![]u8 {
    return std.fmt.allocPrint(allocator, "{s}.upgrade-id-keys", .{path});
}

/// Upgrade the database's id keys to big-endian and put the upgraded file in
/// its place (`Database.upgradeIdKeysTo`, then
/// `id_key_upgrade.replaceWithUpgraded`). The original is left as it was
/// unless every check passed and the rename happened.
fn upgradeIdKeys(allocator: std.mem.Allocator, stderr: anytype, parsed_args: *const Args) !IdKeyUpgradeStats {
    const path = parsed_args.path.?;
    const copy_path = try upgradeCopyPath(allocator, path);
    defer allocator.free(copy_path);

    // Taking the database's lock first means a second run against the same
    // database fails here, before it can touch the first run's copy.
    const db = try openDatabase(allocator, stderr, parsed_args, .read_write);
    var open = true;
    defer if (open) db.close();

    // A copy an earlier run left behind was never put in place: whether it
    // is whole is not known, so it is started again. Only the run holding
    // the database's lock gets here.
    IdKeyUpgrade.removeCopy(allocator, copy_path);

    var diagnostic = IdKeyUpgrade.Diagnostic{};
    const stats = db.upgradeIdKeysTo(copy_path, &diagnostic) catch |err| {
        return failCommand(
            stderr,
            "Failed to upgrade the id keys of {s}; the database is as it was. {s}: {s}",
            .{ path, @errorName(err), diagnostic.message() },
        );
    };
    if (stats.already_ordered) return stats;

    // Everything into the file, so that the log is empty once it closes:
    // the original has to be complete on its own while it is replaced.
    _ = db.checkpoint(.truncate) catch |err| {
        IdKeyUpgrade.removeCopy(allocator, copy_path);
        return failCommand(stderr, "Failed to upgrade the id keys of {s}; the database is as it was. Checkpoint: {s}", .{ path, @errorName(err) });
    };
    // Closed, but its file stays locked until the copy has replaced it, so
    // no other process can open the database in between (on Windows, until
    // just before the rename).
    const held = db.closeKeepingLock() catch |err| {
        IdKeyUpgrade.removeCopy(allocator, copy_path);
        return failCommand(stderr, "Failed to upgrade the id keys of {s}; the database is as it was. Close: {s}", .{ path, @errorName(err) });
    };
    open = false;

    IdKeyUpgrade.replaceWithUpgraded(allocator, path, copy_path, held, null) catch |err| {
        IdKeyUpgrade.removeCopy(allocator, copy_path);
        const why: []const u8 = switch (err) {
            error.LogNotEmpty => "its log still held work after it closed",
            else => @errorName(err),
        };
        return failCommand(stderr, "Did not replace {s} with its upgraded copy: {s}. The database is as it was.", .{ path, why });
    };
    return stats;
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

    // A backup only reads. A read-only handle has already recovered whatever
    // the log held, in memory, so the copy has every committed transaction.
    const db = try openDatabase(allocator, stderr, parsed_args, .read_only);
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
                path,                 dest,
                stats.generation,     stats.started_generation,
                stats.frames_shipped, stats.bytes_shipped,
                stats.snapshot_bytes, stats.duration_ns,
            },
        ),
        .csv => try stdout.print(
            "{s},{s},{d},{},{d},{d},{d},{d}\n",
            .{
                path,                 dest,
                stats.generation,     stats.started_generation,
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

    // The CSV header belongs above the rows rather than repeated per pass.
    if (parsed_args.format == .csv) {
        try stdout.writeAll(
            "path,destination,generation,started_generation,frames_shipped,bytes_shipped,snapshot_bytes,duration_ns\n",
        );
    }

    const interval_ns = @as(u64, parsed_args.interval_secs) * std.time.ns_per_s;

    // Each pass opens the database and closes it again. Holding it open
    // between passes would hold the file lock for as long as --follow runs,
    // and then no other process could write anything for it to ship.
    var first_pass = true;
    while (true) : (first_pass = false) {
        _ = try replicationPass(allocator, stdout, stderr, parsed_args, path, dest, !first_pass);
        // A follower is read through a pipe or a log file, one pass at a time.
        stdout.flush() catch {};
        stderr.flush() catch {};

        if (!parsed_args.follow) break;
        @import("compat").sleep(interval_ns);
    }
}

const PassOutcome = enum { shipped, skipped_busy };

/// One replication pass on a handle of its own.
///
/// `tolerate_busy` is for the passes of --follow after the first: a database
/// another process has open at that moment is skipped until the next
/// interval, where the first pass, like a single run, reports it and stops.
fn replicationPass(
    allocator: std.mem.Allocator,
    stdout: anytype,
    stderr: anytype,
    parsed_args: *const Args,
    path: []const u8,
    dest: []const u8,
    tolerate_busy: bool,
) !PassOutcome {
    const db = Database.open(allocator, path, openOptions(parsed_args, .read_write)) catch |err| {
        if (tolerate_busy and err == DatabaseError.DatabaseLocked) {
            output.printError(stderr, "{s} is open in another process; skipping this pass", .{path});
            return .skipped_busy;
        }
        return failOpen(stderr, path, err, parsed_args.enable_vector);
    };
    defer db.close();

    const stats = db.replicateTo(dest) catch |err| switch (err) {
        error.UuidMismatch => return failCommand(
            stderr,
            "{s} already holds backups of a different database. A database upgraded with 'compact --upgrade-id-keys' counts as a different one: replicate it to a new directory (the old one still restores it as it was before the upgrade)",
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
    return .shipped;
}

const OpenMode = enum { read_only, read_write };

/// The options every command that opens an existing database passes.
///
/// A database with a vector index records its configuration and opens with
/// it, so the vector options are not needed to open one. They still serve two
/// purposes: creating an index in a database that has none, and opening a
/// database written by 0.16.0 whose vector pages cannot be read for their
/// dimensions. Without --vector-dims the dimensions are the file's.
fn openOptions(parsed_args: *const Args, mode: OpenMode) OpenOptions {
    return .{
        .read_only = mode == .read_only,
        .lock = !parsed_args.no_lock,
        .config = .{
            .enable_vector = parsed_args.enable_vector,
            .vector_dimensions = if (parsed_args.vector_dims_set) parsed_args.vector_dims else 0,
            .changefeed_retention = parsed_args.changefeed_retention,
        },
    };
}

/// Open the database named on the command line, or explain why it would not.
fn openDatabase(
    allocator: std.mem.Allocator,
    stderr: anytype,
    parsed_args: *const Args,
    mode: OpenMode,
) !*Database {
    const path = parsed_args.path.?;
    const db = Database.open(allocator, path, openOptions(parsed_args, mode)) catch |err| {
        return failOpen(stderr, path, err, parsed_args.enable_vector);
    };
    applyAutoCheckpoint(db, parsed_args);
    return db;
}

/// How commits checkpoint: --auto-checkpoint, else
/// LATTICE_AUTO_CHECKPOINT_FRAMES, else the engine's default. Off (0) stops
/// the checkpoints commits run; the database is still folded into its file
/// when the command closes it.
fn applyAutoCheckpoint(db: *Database, parsed_args: *const Args) void {
    const AutoCheckpointConfig = lattice.storage.checkpoint.AutoCheckpointConfig;
    if (parsed_args.auto_checkpoint_frames) |frames| {
        db.setAutoCheckpoint(if (frames == 0) null else .{ .max_wal_frames = frames });
    } else if (AutoCheckpointConfig.fromEnvironment()) |setting| {
        db.setAutoCheckpoint(setting.get());
    }
}

/// What a file in a format this build cannot read gets, from every command.
const newer_format_message = "{s} was written by a newer LatticeDB, in a format this version cannot read. Use that newer version.";

/// Report a database that would not open.
///
/// The engine's error names say what happened and not what to do about it, so
/// the ones with a likely remedy get wording that names it.
fn failOpen(stderr: anytype, path: []const u8, err: anyerror, vectors_enabled: bool) anyerror {
    if (err == DatabaseError.DatabaseLocked) {
        return failCommand(
            stderr,
            "{s} is open in another process. Close it first, or pass --no-lock if you are certain nothing is writing to it.",
            .{path},
        );
    }
    if (err == DatabaseError.NotFound or err == error.FileNotFound) {
        return failCommand(stderr, "Database not found: {s}. Create it first with 'lattice create'.", .{path});
    }
    if (err == DatabaseError.VersionTooNew) {
        return failCommand(stderr, newer_format_message, .{path});
    }
    if (err == DatabaseError.NeedsNewerWriter) {
        return failCommand(
            stderr,
            "{s} was written by a newer LatticeDB that does not let this version write to it. " ++
                "info, count, labels, types, schema, export, dump, backup, query and exec (for statements that only read) " ++
                "still read it; use that newer version for anything else.",
            .{path},
        );
    }
    if (err == DatabaseError.VectorConfigMismatch) {
        return failCommand(
            stderr,
            "Failed to open database {s}: its vector index has a different number of dimensions than --vector-dims " ++
                "gives. A database with a vector index opens with it as it was created; leave out --vector-dims.",
            .{path},
        );
    }
    if (err == DatabaseError.InvalidArgument and vectors_enabled) {
        return failCommand(
            stderr,
            "Failed to open database {s}: it has no vector index to open. To create one, give its dimensions " ++
                "with --vector-dims=<n>.",
            .{path},
        );
    }
    // A database written by 0.16.0 records no vector configuration. Its
    // dimensions are read from its vector pages, and only when that fails
    // does replaying vector changes in its log need them spelled out.
    if (err == DatabaseError.IoError and !vectors_enabled) {
        return failCommand(
            stderr,
            "Failed to open database {s}: IoError. If it was written by LatticeDB 0.16.0 and stores vectors, its " ++
                "write-ahead log may hold vector changes that need the vector index. Retry with --enable-vector " ++
                "--vector-dims=<n>, where <n> is the number of dimensions the database was created with.",
            .{path},
        );
    }
    if (err == DatabaseError.IoError and vectors_enabled) {
        return failCommand(
            stderr,
            "Failed to open database {s}: IoError. With --enable-vector, check that --vector-dims matches the " ++
                "number of dimensions the database was created with.",
            .{path},
        );
    }
    return failCommand(stderr, "Failed to open database {s}: {s}", .{ path, @errorName(err) });
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

/// Show the changefeed's policy and what it holds, and with --trim apply
/// the policy now. --changefeed=<policy> sets the policy first.
fn cmdChangefeed(
    allocator: std.mem.Allocator,
    stdout: anytype,
    stderr: anytype,
    parsed_args: *const Args,
) !void {
    const path = parsed_args.path.?;
    const writes = parsed_args.trim or parsed_args.changefeed_retention != null;
    const db = try openDatabase(allocator, stderr, parsed_args, if (writes) .read_write else .read_only);
    defer db.close();

    var removed: u64 = 0;
    if (parsed_args.trim) {
        removed = db.trimChangefeed() catch |err| {
            return failCommand(stderr, "Failed to trim the changefeed: {s}", .{@errorName(err)});
        };
    }
    const feed = db.changefeedStatus() catch |err| {
        return failCommand(stderr, "Failed to read the changefeed: {s}", .{@errorName(err)});
    };
    const held = db.changefeedSize() catch |err| {
        return failCommand(stderr, "Failed to read the changefeed: {s}", .{@errorName(err)});
    };
    var policy_buf: [48]u8 = undefined;
    const policy = feed.retention.format(&policy_buf);

    switch (parsed_args.format) {
        .table => {
            try stdout.print("Changefeed of {s}\n", .{path});
            try stdout.print("  Retention:      {s}\n", .{policy});
            try stdout.print("  Events held:    {d}", .{held.events});
            if (held.legacy_events > 0) try stdout.print(" ({d} in the 0.16 format)", .{held.legacy_events});
            try stdout.writeAll("\n");
            try stdout.print("  Bytes:          {d}\n", .{held.bytes});
            if (feed.oldest_sequence) |oldest| {
                try stdout.print("  Sequences:      {d}..{d}\n", .{ oldest, feed.last_sequence });
            } else {
                try stdout.print("  Last sequence:  {d}\n", .{feed.last_sequence});
            }
            if (parsed_args.trim) try stdout.print("  Trimmed:        {d} events\n", .{removed});
        },
        .json => try stdout.print(
            "{{\"path\":\"{s}\",\"retention\":\"{s}\",\"events\":{d},\"legacy_events\":{d},\"bytes\":{d},\"oldest_sequence\":{d},\"last_sequence\":{d},\"trimmed\":{d}}}\n",
            .{ path, policy, held.events, held.legacy_events, held.bytes, feed.oldest_sequence orelse 0, feed.last_sequence, removed },
        ),
        .csv => {
            try stdout.writeAll("property,value\n");
            try stdout.print("retention,{s}\n", .{policy});
            try stdout.print("events,{d}\n", .{held.events});
            try stdout.print("legacy_events,{d}\n", .{held.legacy_events});
            try stdout.print("bytes,{d}\n", .{held.bytes});
            try stdout.print("oldest_sequence,{d}\n", .{feed.oldest_sequence orelse 0});
            try stdout.print("last_sequence,{d}\n", .{feed.last_sequence});
            try stdout.print("trimmed,{d}\n", .{removed});
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
    const db = try openDatabase(allocator, stderr, parsed_args, .read_write);
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
        error.VersionTooNew => return failCommand(stderr, newer_format_message, .{path}),
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
            try writeReaderVersionLine(stdout, "  Min reader:    ", stats.min_reader_version, stats.min_reader_reasons);
            if (stats.blank_pages > 0) {
                try stdout.print("  Blank pages:   {d} (never written; a crash can leave these unused)\n", .{stats.blank_pages});
            }
            if (stats.journal_applied) {
                try stdout.writeAll("  Note: an interrupted flush was finished from the journal in memory; the file is unchanged until a read-write open finishes it\n");
            }
            if (stats.wal_present) {
                try stdout.writeAll("  Note: sibling WAL file exists but was not validated\n");
            }
        },
        .json => {
            try stdout.print("{{\"path\":\"{s}\",\"pages_checked\":{d},\"blank_pages\":{d},\"journal_applied\":{},\"wal_present\":{},\"wal_validated\":false,\"min_reader_version\":{d},\"min_reader_reasons\":[", .{
                path,
                stats.pages_checked,
                stats.blank_pages,
                stats.journal_applied,
                stats.wal_present,
                stats.min_reader_version,
            });
            try writeReaderReasons(stdout, stats.min_reader_reasons, ",", true);
            try stdout.writeAll("]}\n");
        },
        .csv => {
            try stdout.writeAll("property,value\n");
            try stdout.print("path,{s}\n", .{path});
            try stdout.print("pages_checked,{d}\n", .{stats.pages_checked});
            try stdout.print("blank_pages,{d}\n", .{stats.blank_pages});
            try stdout.print("journal_applied,{}\n", .{stats.journal_applied});
            try stdout.print("wal_present,{}\n", .{stats.wal_present});
            try stdout.writeAll("wal_validated,false\n");
            try stdout.print("min_reader_version,{d}\nmin_reader_reasons,", .{stats.min_reader_version});
            try writeReaderReasons(stdout, stats.min_reader_reasons, ";", false);
            try stdout.writeAll("\n");
        },
    }
}

/// Verify every page checksum in the main database file.
///
/// Takes a shared lock by default, because reading pages while another process
/// flushes them reports corruption that is not there.
///
/// A flush that a crash cut short leaves pages half written in place and the
/// journal that finishes it. The engine applies the journal before it reads
/// anything; so does the check, as a read-only open does, on an overlay that
/// keeps what it writes in memory, so the file and the journal are left as
/// they are. Without it the check refused such a file, or would report
/// damage the journal repairs.
fn checkDatabaseFile(allocator: std.mem.Allocator, path: []const u8, lock: bool) CheckError!CheckStats {
    var posix_vfs = PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    // The lock is held from before the journal is read, so that no writer
    // can start a flush under the check. Locks belong to the open file, so
    // the handles opened below do not disturb it.
    const locked_file: ?lattice.storage.vfs.File = if (lock) blk: {
        const file = vfs_impl.open(path, .{ .read = true }) catch |err| return switch (err) {
            error.FileNotFound => CheckError.FileNotFound,
            error.PermissionDenied => CheckError.PermissionDenied,
            else => CheckError.IoError,
        };
        errdefer file.close();
        const acquired = file.tryLock(.shared) catch return CheckError.IoError;
        if (!acquired) return CheckError.DatabaseLocked;
        break :blk file;
    } else null;
    defer if (locked_file) |file| {
        file.unlock();
        file.close();
    };

    const journal_path = std.fmt.allocPrint(allocator, "{s}-journal", .{path}) catch return CheckError.OutOfMemory;
    defer allocator.free(journal_path);
    const journal_size: u64 = if (vfs_impl.exists(journal_path)) blk: {
        const file = vfs_impl.open(journal_path, .{ .read = true }) catch break :blk 0;
        defer file.close();
        break :blk file.size() catch 0;
    } else 0;

    // The overlay holds at most the pages the journal carries.
    const overlay_budget = std.math.cast(usize, journal_size +| 1024 * 1024) orelse return CheckError.OutOfMemory;
    var overlay = OverlayVfs.init(allocator, vfs_impl, overlay_budget);
    defer overlay.deinit();
    const check_vfs = overlay.vfs();

    var journal_applied = false;
    if (journal_size > 0) {
        const db_file = check_vfs.open(path, .{ .read = true, .write = true }) catch |err| return switch (err) {
            error.FileNotFound => CheckError.FileNotFound,
            error.PermissionDenied => CheckError.PermissionDenied,
            else => CheckError.IoError,
        };
        defer db_file.close();
        journal_applied = journal_mod.recover(allocator, check_vfs, journal_path, db_file, false) catch |err| return switch (err) {
            error.OutOfMemory => CheckError.OutOfMemory,
            else => CheckError.IoError,
        };
    }

    var page_manager = PageManager.init(allocator, check_vfs, path, .{
        .read_only = true,
        .lock = false,
    }) catch |err| return mapPageManagerCheckError(err);
    defer page_manager.deinit();

    const page_size: usize = @intCast(page_manager.getPageSize());
    const page_alignment = comptime std.mem.Alignment.fromByteUnits(@alignOf(PageHeader));
    const page_buf = allocator.alignedAlloc(u8, page_alignment, page_size) catch {
        return CheckError.OutOfMemory;
    };
    defer allocator.free(page_buf);

    // A blank page is one the file was extended with and nothing wrote,
    // which a crash can leave behind unused. Whether anything refers to one
    // is not known here; the engine refuses to read one that is in use in a
    // file whose pages are all sealed.
    const page_count = page_manager.pageCount();
    var blank_pages: u32 = 0;
    var page_id: u32 = 1;
    while (page_id < page_count) : (page_id += 1) {
        const blank = page_manager.readPageForScan(page_id, page_buf) catch |err| return mapPageManagerCheckError(err);
        if (blank) blank_pages += 1;
    }

    return .{
        .blank_pages = blank_pages,
        .journal_applied = journal_applied,
        .min_reader_version = page_manager.getHeader().min_reader_version,
        .min_reader_reasons = page_manager.getHeader().readerReasons(),
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
        error.VersionTooNew => CheckError.VersionTooNew,
        error.InvalidHeader, error.InvalidMagic, error.InvalidPageId, error.PageNotAllocated => CheckError.InvalidDatabase,
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

    // Turtle and N-Triples, by --format or by extension.
    if (rdf.command.syntaxOf(parsed_args, file)) |syntax| {
        const db = try openDatabase(allocator, stderr, parsed_args, .read_write);
        defer db.close();
        return rdf.command.runImport(allocator, stdout, stderr, db, parsed_args, file, syntax);
    }

    // Detect file format by extension
    const is_json = std.mem.endsWith(u8, file, ".json");
    const is_jsonl = std.mem.endsWith(u8, file, ".jsonl");
    const is_csv = std.mem.endsWith(u8, file, ".csv");

    if (!is_json and !is_jsonl and !is_csv) {
        return failCommand(stderr, "Unknown file format. Use a .json, .jsonl, or .csv extension.", .{});
    }

    // A CSV export writes <base>_nodes.csv and <base>_edges.csv. Naming the
    // export's own --file value imports both, so edges can refer to nodes by
    // the _id values the nodes file was written with.
    var csv_pair_storage: CsvPair = .{};
    var csv_pair: ?*const CsvPair = null;
    if (is_csv and !fileExists(file)) {
        try findCsvPair(stderr, file, &csv_pair_storage);
        csv_pair = &csv_pair_storage;
    }

    const db = try openImportTarget(allocator, stderr, parsed_args, if (is_json) file else null);
    defer db.close();

    if (csv_pair) |pair| {
        output.printInfo(stdout, "Importing {s} and {s} into {s}...", .{
            pair.nodes orelse "(no nodes file)",
            pair.edges orelse "(no edges file)",
            path,
        });
    } else {
        output.printInfo(stdout, "Importing {s} into {s}...", .{ file, path });
    }

    var diagnostics = import_export.Diagnostics.init(allocator);
    defer diagnostics.deinit();
    const import_options: import_export.ImportOptions = .{
        .batch_size = parsed_args.batch_size,
        .on_error_skip = parsed_args.on_error_skip,
        .diagnostics = &diagnostics,
    };

    const stats = if (is_json)
        import_export.importJson(allocator, db, file, import_options)
    else if (is_jsonl)
        import_export.importJsonl(allocator, db, file, import_options)
    else if (csv_pair) |pair|
        import_export.importCsvFiles(allocator, db, pair.nodes, pair.edges, import_options)
    else
        import_export.importCsv(allocator, db, file, import_options);

    if (stats) |s| {
        // --on-error=skip goes on past bad records; say which ones it left out.
        for (diagnostics.messages.items) |message| {
            try stderr.print("Skipped {s}\n", .{message});
        }
        if (diagnostics.omitted > 0) {
            try stderr.print("Skipped {d} more records.\n", .{diagnostics.omitted});
        }
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
        if (diagnostics.messages.items.len > 0) {
            const committed = diagnostics.committed;
            // The failure that stopped the import is the last message; any
            // before it are records --on-error=skip went past.
            const messages = diagnostics.messages.items;
            output.printError(stderr, "Import failed at {s}", .{messages[messages.len - 1]});
            if (committed.nodes_imported + committed.edges_imported > 0) {
                try stderr.print(
                    "{d} nodes and {d} edges imported in earlier batches were kept.",
                    .{ committed.nodes_imported, committed.edges_imported },
                );
            } else {
                try stderr.writeAll("Nothing was imported.");
            }
            // Skipping goes past a bad record, not past text that cannot be
            // read, and is no advice to someone already skipping.
            if (diagnostics.syntax or parsed_args.on_error_skip) {
                try stderr.writeAll(" Fix the file and import it again.\n");
            } else {
                try stderr.writeAll(" Use --on-error=skip to import the records that are valid.\n");
            }
            return error.CommandFailed;
        }
        return switch (err) {
            error.VectorsNotEnabled => if (is_json) failCommand(
                stderr,
                "Import failed: {s} contains vectors, and {s} has no vector index. Retry with --enable-vector: " ++
                    "the index is created with the dimensions, metric and HNSW settings the file declares " ++
                    "(--vector-dims=<n> where it declares none).",
                .{ file, path },
            ) else failCommand(
                stderr,
                "Import failed: {s} contains vectors, and {s} has no vector index. Retry with --enable-vector " ++
                    "--vector-dims=<n>, where <n> is the number of numbers in each vector.",
                .{ file, path },
            ),
            error.VectorDimensionMismatch => failCommand(
                stderr,
                "Import failed: the vectors in {s} do not have the {d} dimensions {s} was opened with.",
                .{ file, import_export.vectorDimensions(db) orelse 0, path },
            ),
            error.UnsupportedVersion => failCommand(
                stderr,
                "Import failed: {s} was written in an export format version this lattice does not know. Upgrade lattice.",
                .{file},
            ),
            else => failCommand(stderr, "Import failed: {s}", .{@errorName(err)}),
        };
    }
}

/// Open the database an import writes to.
///
/// With --enable-vector, a database that has no vector index gets one. A
/// JSON export says which index its vectors came from: dimensions, metric
/// and HNSW settings. The new index takes them from there, --vector-dims
/// winning over the file's dimensions. A database that already has an index
/// keeps it as it is: the file's settings are not imposed on it, and an
/// import whose vectors have other dimensions fails as before.
fn openImportTarget(
    allocator: std.mem.Allocator,
    stderr: anytype,
    parsed_args: *const Args,
    json_file: ?[]const u8,
) !*Database {
    const path = parsed_args.path.?;
    const plain = openOptions(parsed_args, .read_write);
    const json_path = json_file orelse return openDatabase(allocator, stderr, parsed_args, .read_write);
    if (!parsed_args.enable_vector) return openDatabase(allocator, stderr, parsed_args, .read_write);

    const declared = blk: {
        const json = compat.fs.cwd().openFile(json_path, .{}) catch {
            return failCommand(stderr, "Import failed: cannot read {s}", .{json_path});
        };
        defer json.close();
        const content = json.readToEndAlloc(allocator, import_export.max_import_file_bytes) catch {
            return failCommand(stderr, "Import failed: cannot read {s}", .{json_path});
        };
        defer allocator.free(content);
        break :blk import_export.declaredVectorConfig(allocator, content) catch {
            return failCommand(stderr, "Import failed: {s} is not a JSON export (its vector fields could not be read)", .{json_path});
        };
    };

    var options = plain;
    if (!parsed_args.vector_dims_set) {
        if (declared.dimensions) |dims| options.config.vector_dimensions = dims;
    }
    options.config.vector_metric = declared.metric;
    options.config.vector_m = declared.m;
    options.config.vector_ef_construction = declared.ef_construction;

    const db = Database.open(allocator, path, options) catch |err| blk: {
        // A database with an index that the file's settings contradict:
        // open it with its own, which the import then checks the vectors
        // against.
        if (err == DatabaseError.VectorConfigMismatch) {
            break :blk Database.open(allocator, path, plain) catch |retry_err| {
                return failOpen(stderr, path, retry_err, parsed_args.enable_vector);
            };
        }
        return failOpen(stderr, path, err, parsed_args.enable_vector);
    };
    applyAutoCheckpoint(db, parsed_args);
    return db;
}

const CsvPair = struct {
    nodes: ?[]const u8 = null,
    edges: ?[]const u8 = null,
    nodes_buf: [1024]u8 = undefined,
    edges_buf: [1024]u8 = undefined,
};

fn fileExists(path: []const u8) bool {
    @import("compat").fs.cwd().access(path, .{}) catch return false;
    return true;
}

/// The nodes and edges files a CSV export wrote for this --file value.
fn findCsvPair(stderr: anytype, file: []const u8, pair: *CsvPair) !void {
    const base = file[0 .. file.len - ".csv".len];
    const nodes = std.fmt.bufPrint(&pair.nodes_buf, "{s}_nodes.csv", .{base}) catch {
        return failCommand(stderr, "Path too long", .{});
    };
    const edges = std.fmt.bufPrint(&pair.edges_buf, "{s}_edges.csv", .{base}) catch {
        return failCommand(stderr, "Path too long", .{});
    };
    if (fileExists(nodes)) pair.nodes = nodes;
    if (fileExists(edges)) pair.edges = edges;
    if (pair.nodes == null and pair.edges == null) {
        return failCommand(stderr, "Cannot read {s}, nor {s} or {s}", .{ file, nodes, edges });
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
    const db = try openDatabase(allocator, stderr, parsed_args, .read_only);
    defer db.close();

    var query_result: ?QueryResult = null;
    defer if (query_result) |*result| result.deinit();
    const scope = try exportScope(stderr, db, parsed_args, &query_result);

    output.printInfo(stdout, "Exporting {s} to {s}...", .{ path, file });
    warnIfVectorsLeftOut(stderr, db, path);

    if (rdf.command.syntaxOf(parsed_args, file)) |syntax| {
        return rdf.command.runExport(allocator, stdout, stderr, db, parsed_args, file, syntax, scope);
    }

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

        var out_buffer: [export_buffer_size]u8 = undefined;
        var out_writer = out_file.file.writerStreaming(compat.io, &out_buffer);
        const writer = &out_writer.interface;
        const stats = finishExport(switch (export_kind) {
            .json => import_export.exportJson(allocator, db, writer, scope),
            .jsonl => import_export.exportJsonl(allocator, db, writer, scope),
            .dot => import_export.exportDot(allocator, db, writer, scope),
            .csv => unreachable,
        }, &.{writer}, &.{file});

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
            return failExport(stderr, err);
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

        var nodes_buffer: [export_buffer_size]u8 = undefined;
        var nodes_file_writer = nodes_file.file.writerStreaming(compat.io, &nodes_buffer);
        const nodes_writer = &nodes_file_writer.interface;
        var edges_buffer: [export_buffer_size]u8 = undefined;
        var edges_file_writer = edges_file.file.writerStreaming(compat.io, &edges_buffer);
        const edges_writer = &edges_file_writer.interface;

        const stats = finishExport(
            import_export.exportCsv(allocator, db, nodes_writer, edges_writer, scope),
            &.{ nodes_writer, edges_writer },
            &.{ nodes_path, edges_path },
        );

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
            return failExport(stderr, err);
        }
    }
}

/// Work out what an export covers, running its query if it has one.
///
/// The query's result is kept in `result_slot`, which the caller frees once the
/// export is written.
fn exportScope(
    stderr: anytype,
    db: *Database,
    parsed_args: *const Args,
    result_slot: *?QueryResult,
) !import_export.ExportScope {
    const query = parsed_args.query_string orelse return .{ .label_filter = parsed_args.label_filter };
    if (parsed_args.label_filter != null) {
        return failCommand(stderr, "--labels and --query cannot be combined. Match the labels in the query instead.", .{});
    }

    var detailed = db.queryDetailed(query) catch |err| {
        return failCommand(stderr, "Query failed: {s}", .{@errorName(err)});
    };
    switch (detailed) {
        .failure => |*failure| {
            output.printQueryFailure(stderr, query, failure.*);
            failure.deinit();
            return error.CommandFailed;
        },
        .success => |result| result_slot.* = result,
    }
    return .{ .query_result = &result_slot.*.? };
}

const export_buffer_size = 64 * 1024;

/// Flush an export's files, and remove them when it failed, so a failed
/// export does not leave a file that looks like a complete one.
fn finishExport(
    result: anyerror!import_export.ExportStats,
    writers: []const *std.Io.Writer,
    paths: []const []const u8,
) anyerror!import_export.ExportStats {
    const stats = result catch |err| {
        for (paths) |path| compat.fs.cwd().deleteFile(path) catch {};
        return err;
    };
    for (writers) |writer| {
        writer.flush() catch |err| {
            for (paths) |path| compat.fs.cwd().deleteFile(path) catch {};
            return err;
        };
    }
    return stats;
}

fn failExport(stderr: anytype, err: anyerror) anyerror {
    if (err == error.QueryReturnedNoGraph) {
        return failCommand(
            stderr,
            "Export failed: the query returned no nodes or relationships to export. Return them, for example MATCH (n:Person) RETURN n.",
            .{},
        );
    }
    return failCommand(stderr, "Export failed: {s}", .{@errorName(err)});
}

/// Say so when a database has vectors that an export or dump will not include.
///
/// Vectors can only be read through the vector index. A database opens with
/// its index whenever the file records the index's configuration or its
/// vector pages show the dimensions, so this only fires for a 0.16.0 database
/// where neither works and the command was not told the dimensions.
fn warnIfVectorsLeftOut(stderr: anytype, db: *Database, path: []const u8) void {
    if (!db.hasPersistedVectorIndex() or import_export.vectorDimensions(db) != null) return;
    stderr.print(
        "Warning: {s} has a vector index, and vectors are not included without --enable-vector " ++
            "--vector-dims=<n>, where <n> is the number of dimensions the database was created with.\n",
        .{path},
    ) catch {};
}

fn cmdDump(
    allocator: std.mem.Allocator,
    stdout: anytype,
    stderr: anytype,
    parsed_args: *const Args,
) !void {

    // Open database
    const db = try openDatabase(allocator, stderr, parsed_args, .read_only);
    defer db.close();

    // Dump to stdout as canonical JSON
    var query_result: ?QueryResult = null;
    defer if (query_result) |*result| result.deinit();
    const scope = try exportScope(stderr, db, parsed_args, &query_result);

    warnIfVectorsLeftOut(stderr, db, parsed_args.path.?);
    const stats = import_export.dumpCanonicalJson(allocator, db, stdout, scope);

    if (stats) |s| {
        // Print stats to stderr so they don't mix with JSON output
        try stderr.print("Dumped {d} nodes, {d} edges\n", .{ s.nodes_exported, s.edges_exported });
    } else |err| {
        return failExport(stderr, err);
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
        \\    changefeed <path>   Show or trim the changefeed (__lattice_changes)
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
        \\    import <path>       Import data from JSON/JSONL/CSV, Turtle/N-Triples
        \\    export <path>       Export data to JSON/JSONL/CSV/DOT, Turtle/N-Triples
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
        \\  --enable-vector       Create a vector index. A database that has one
        \\                        opens with it without this flag. The
        \\                        --vector-* options need it.
        \\  --vector-dims=<n>     Vector dimensions, 1..4096 (default for create: 128)
        \\  --vector-metric=<m>   With create: cosine, euclidean or dot_product
        \\                        (default: cosine)
        \\  --enable-fts          Enable full-text search (default: on)
        \\  --no-fts              Disable full-text search
        \\  --cache-size=<mb>     Buffer pool size in MB (default: LATTICE_BUFFER_POOL_MB, else 64)
        \\  --page-size=<bytes>   Page size in bytes, 4096..65535 (default: 4096)
        \\  --auto-checkpoint=<n> Checkpoint after n log frames, 0 for never during
        \\                        the command (default: LATTICE_AUTO_CHECKPOINT_FRAMES,
        \\                        else an eighth of the cache, 1000 to 16384 frames;
        \\                        2048 with the 64 MB cache). Closing still checkpoints.
        \\  --file=<path>         Input/output file for import/export
        \\  --query=<cypher>      Query string for exec command
        \\  --no-lock             Skip the file lock, for filesystems without one
        \\  --changefeed=<policy> How much of the changefeed to keep: all, off,
        \\                        last:<events> or age:<duration> (7d, 12h, 30m).
        \\                        Recorded in the file; a writing command sets it.
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
            \\  --enable-vector       Enable HNSW vector index (the --vector-*
            \\                        options below need it)
            \\  --vector-dims=<n>     Vector dimensions (default: 128, max: 4096)
            \\  --vector-metric=<m>   Distance: cosine, euclidean or dot_product
            \\                        (negative dot product) (default: cosine)
            \\  --vector-m=<n>        HNSW connections per node, 2..32 (default: 16)
            \\  --vector-ef-construction=<n>
            \\                        HNSW build candidate list, 1..4096 (default: 200)
            \\  --enable-fts          Enable BM25 full-text search (default: on)
            \\  --no-fts              Disable full-text search
            \\  --cache-size=<mb>     Buffer pool size in MB (default: LATTICE_BUFFER_POOL_MB, else 64)
            \\  --page-size=<bytes>   Page size in bytes (default: 4096)
            \\
            \\Examples:
            \\  lattice create mydb.lattice
            \\  lattice create embeddings.lattice --enable-vector --vector-dims=1536
            \\
            \\The vector index's configuration is stored in the database, and
            \\later commands open the index with it without any vector flags.
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
            \\Options:
            \\  --enable-vector       Create a vector index if the database has
            \\                        none (needs --vector-dims). A database with
            \\                        one opens with it without this flag.
            \\  --vector-dims=<n>     Dimensions for a new index, or for a 0.16.0
            \\                        database whose dimensions cannot be read
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
            \\  --enable-vector       Create a vector index if the database has
            \\                        none (needs --vector-dims). A database with
            \\                        one opens with it without this flag.
            \\  --vector-dims=<n>     Dimensions for a new index, or for a 0.16.0
            \\                        database whose dimensions cannot be read
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
            \\A process that writes and then closes the database folds the log
            \\into the file and resets it, so the pass after it starts a new
            \\generation. A pass with nothing changed ships nothing.
            \\
            \\Each pass opens the database for writing, which takes the file lock,
            \\and closes it again. A single run fails if another process has the
            \\database open. With --follow, other processes can use the database
            \\between passes, and a pass that finds it open elsewhere is skipped
            \\until the next interval. To replicate a database your application
            \\keeps open, call replicateTo on your own handle instead.
            \\
            \\Options:
            \\  --to=<directory>      Where to ship to, created if missing
            \\  --follow              Keep running, shipping on an interval
            \\  --interval=<seconds>  With --follow, how long to wait between passes
            \\                        (default 10)
            \\  --format=<fmt>        Output format: table, json, csv
            \\  --enable-vector       Create a vector index if the database has
            \\                        none (needs --vector-dims). A database with
            \\                        one opens with it without this flag.
            \\  --vector-dims=<n>     Dimensions for a new index, or for a 0.16.0
            \\                        database whose dimensions cannot be read
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
        .changefeed => writer.writeAll(
            \\Usage: lattice changefeed <path> [options]
            \\
            \\Show the changefeed (__lattice_changes): its retention policy, how
            \\many events it holds and their sequence numbers.
            \\
            \\The file records the policy. Every event is kept unless a policy
            \\says otherwise. Retention drops the oldest events as the database
            \\checkpoints, a bounded number at a time; --trim applies it in full.
            \\
            \\Options:
            \\  --changefeed=<policy> Set the policy: all, off, last:<events>, or
            \\                        age:<duration> with ms, s, m, h or d
            \\  --trim                Drop what the policy no longer keeps, now
            \\  --format=<fmt>        Output format: table, json, csv
            \\
            \\A reader that asks for the events after one that has been dropped
            \\gets an error rather than a silent gap. With the policy off, no
            \\events are written and reading the changefeed is an error.
            \\
            \\Examples:
            \\  lattice changefeed mydb.lattice
            \\  lattice changefeed mydb.lattice --changefeed=age:30d --trim
            \\  lattice changefeed mydb.lattice --changefeed=all
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
            \\  --enable-vector       Create a vector index if the database has
            \\                        none (needs --vector-dims). A database with
            \\                        one opens with it without this flag.
            \\  --vector-dims=<n>     Dimensions for a new index, or for a 0.16.0
            \\                        database whose dimensions cannot be read
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
            \\  --enable-vector       Create a vector index if the database has
            \\                        none (needs --vector-dims). A database with
            \\                        one opens with it without this flag.
            \\  --vector-dims=<n>     Dimensions for a new index, or for a 0.16.0
            \\                        database whose dimensions cannot be read
            \\  --upgrade-indexes     First rebuild property indexes an older
            \\                        release created (equality only) so they
            \\                        answer ranges and sorting. 0.16.0 cannot
            \\                        open the file afterwards.
            \\  --upgrade-id-keys     First convert a database created before
            \\                        format 5 to big-endian id keys, the layout
            \\                        a new database has. The upgrade is written
            \\                        to a copy, checked against the original,
            \\                        and only then put in its place. One-way:
            \\                        0.18 and earlier cannot open the result.
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
            \\  --enable-vector       Create a vector index if the database has
            \\                        none (needs --vector-dims). A database with
            \\                        one opens with it without this flag.
            \\  --vector-dims=<n>     Dimensions for a new index, or for a 0.16.0
            \\                        database whose dimensions cannot be read
            \\
            \\Examples:
            \\  lattice exec mydb.lattice --query="MATCH (n) RETURN count(n)"
            \\  lattice exec mydb.lattice --file=query.cypher --format=json
            \\
        ) catch {},
        .import => writer.writeAll(
            \\Usage: lattice import <path> --file=<input> [options]
            \\
            \\Import data from JSON, JSONL, or CSV files, such as those export writes,
            \\or RDF in Turtle or N-Triples.
            \\
            \\Options:
            \\  --file=<path>         Input file (.json, .jsonl, .csv, .ttl, .nt)
            \\  --batch-size=<n>      Commit every N items (default: 1000)
            \\  --on-error=skip       Skip invalid records instead of aborting
            \\  --enable-vector       Create a vector index if the database has
            \\                        none (needs --vector-dims). A database with
            \\                        one opens with it without this flag.
            \\  --vector-dims=<n>     Dimensions for a new index, or for a 0.16.0
            \\                        database whose dimensions cannot be read
            \\
            \\A JSON export records the vector index its vectors came from:
            \\dimensions, metric, M and ef_construction. With --enable-vector, a
            \\database that has no vector index gets one with those settings
            \\(--vector-dims wins over the file's dimensions); one that has an
            \\index keeps it. JSONL and CSV exports cannot record them: pass
            \\--vector-dims, and create the database with `lattice create` first
            \\if the metric or HNSW settings are not the defaults.
            \\
            \\Without --on-error=skip, the import stops at the first invalid record
            \\and says which it was; batches committed before it are kept. With it,
            \\each record is committed on its own, an invalid one is left out whole,
            \\and each one left out is listed.
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
            \\RDF (Turtle .ttl, N-Triples .nt):
            \\  Each IRI becomes a node labelled Resource with an iri property, found
            \\  again by the index Resource(iri) the import declares; literals become
            \\  properties, other objects edges, rdf:type labels. Names are
            \\  <prefix>_<local>, such as foaf_name; the table of names and prefixes
            \\  is kept in the database for export. A record is one statement: an
            \\  invalid one is left out whole, and --batch-size counts triples,
            \\  committed at the end of a statement. A Turtle syntax error stops
            \\  the import; in N-Triples, --on-error=skip goes on at the next line.
            \\
            \\  --format=turtle|ntriples   The syntax, when the extension does not say
            \\  --base=<iri>               Base for relative IRIs (default: the file's
            \\                             file:// URL)
            \\  --rdf-types=<mode>         rdf:type as labels, edges, or auto: labels
            \\                             unless there are over 1000 classes (default)
            \\  --rdf-lang=<tag>           Language whose strings take the plain name;
            \\                             others get <name>_<tag> (default: en)
            \\  --rdf-decimal=<mode>       xsd:decimal as float when that is exact, else
            \\                             string (float, default), or always string
            \\  --rdf-vectors=omit         Leave lattice:vector values out
            \\  --rdf-fts=<names>          Declare full-text indexes on these
            \\                             properties of Resource (comma-separated)
            \\
            \\Examples:
            \\  lattice import mydb.lattice --file=data.json
            \\  lattice import mydb.lattice --file=skos.ttl --rdf-fts=skos_prefLabel
            \\
        ) catch {},
        .@"export" => writer.writeAll(
            \\Usage: lattice export <path> --file=<output> [options]
            \\
            \\Export data to JSON, JSONL, CSV, or DOT files, or RDF in Turtle or
            \\N-Triples.
            \\
            \\Vectors are exported with their nodes. A JSON export (and dump) also
            \\records the vector index's dimensions, metric, M and ef_construction,
            \\which an import that creates an index uses; JSONL, CSV and DOT
            \\exports cannot record them.
            \\
            \\Options:
            \\  --file=<path>         Output file path (.json, .jsonl, .csv, .dot,
            \\                        .ttl, .nt)
            \\  --format=<fmt>        CLI output format: table, json, csv
            \\  --enable-vector       Create a vector index if the database has
            \\                        none (needs --vector-dims). A database with
            \\                        one opens with it without this flag.
            \\  --vector-dims=<n>     Dimensions for a new index, or for a 0.16.0
            \\                        database whose dimensions cannot be read
            \\  --labels=<list>       Filter by labels (comma-separated)
            \\  --query=<cypher>      Export query results instead
            \\
            \\RDF (Turtle .ttl, N-Triples .nt):
            \\  Nodes and names imported from RDF get their IRIs back. Other names are
            \\  written under the base, other nodes as blank nodes, or under the base
            \\  when --base is given. Turtle groups each node's triples and
            \\  abbreviates with the prefixes the database knows.
            \\
            \\  --format=turtle|ntriples     The syntax, when the extension does not say
            \\  --base=<iri>                 Base for names and nodes without an IRI
            \\                               (default for names: urn:lattice:)
            \\  --rdf-edge-properties=<mode> Edge properties as RDF-star annotations
            \\                               (star, default), rdf:Statement nodes
            \\                               (reify), or left out (drop)
            \\  --rdf-vectors=omit           Leave node vectors out
            \\
            \\Examples:
            \\  lattice export mydb.lattice --file=backup.json
            \\  lattice export mydb.lattice --file=graph.jsonl
            \\  lattice export mydb.lattice --file=graph.dot
            \\  lattice export mydb.lattice --file=people.csv --labels=Person
            \\  lattice export mydb.lattice --file=graph.ttl --base=https://example.org/
            \\
        ) catch {},
        else => {
            if (!command.requiresPath()) {
                writer.print("Usage: lattice {s}\n\n{s}\n", .{ @tagName(command), command.description() }) catch {};
                return;
            }
            writer.print("Usage: lattice {s} <path> [options]\n\n", .{@tagName(command)}) catch {};
            writer.print("{s}\n\n", .{command.description()}) catch {};
            writer.writeAll(
                \\Options:
                \\  --format=<fmt>        Output format: table, json, csv
                \\  --enable-vector       Create a vector index if the database has
                \\                        none (needs --vector-dims)
                \\  --vector-dims=<n>     Dimensions for a new index, or for a 0.16.0
                \\                        database whose dimensions cannot be read
                \\  --no-lock             Skip the file lock, for filesystems without one
                \\
            ) catch {};
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

fn runForTest(allocator: std.mem.Allocator, argv: []const []const u8, out: *std.Io.Writer.Allocating) !void {
    var err_buf: std.Io.Writer.Allocating = .init(allocator);
    defer err_buf.deinit();
    var parsed = try Args.parse(allocator, argv);
    defer parsed.deinit(allocator);
    switch (parsed.command.?) {
        .changefeed => try cmdChangefeed(allocator, &out.writer, &err_buf.writer, &parsed),
        .info => try cmdInfo(allocator, &out.writer, &err_buf.writer, &parsed),
        else => unreachable,
    }
}

test "lcy an unreadable CSV import does not suggest --on-error=skip" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lcy_cli_csv_syntax.ltdb";
    const csv_path = "/tmp/lcy_cli_csv_syntax.csv";
    cleanupTestDatabaseFiles(path);
    defer cleanupTestDatabaseFiles(path);
    defer @import("compat").fs.cwd().deleteFile(csv_path) catch {};
    {
        var db = try Database.open(allocator, path, .{ .create = true, .config = .{ .enable_fts = false } });
        db.close();
    }

    var out: std.Io.Writer.Allocating = .init(allocator);
    defer out.deinit();
    var err: std.Io.Writer.Allocating = .init(allocator);
    defer err.deinit();
    const Case = struct { text: []const u8, skip: bool, advice: []const u8 };
    const cases = [_]Case{
        // An unclosed quote: no record past it can be read, skipping or not.
        // Both runs said "Use --on-error=skip to import the records that are
        // valid".
        .{ .text = "_id,_labels,name\n1,P,\"bob\n2,P,ann\n", .skip = false, .advice = "Nothing was imported. Fix the file and import it again.\n" },
        .{ .text = "_id,_labels,name\n1,P,\"bob\n2,P,ann\n", .skip = true, .advice = "Nothing was imported. Fix the file and import it again.\n" },
        // A bad record: skipping it is the way on.
        .{ .text = "_id,_labels,age:json\n1,P,{\n", .skip = false, .advice = "Nothing was imported. Use --on-error=skip to import the records that are valid.\n" },
    };
    for (cases) |case| {
        {
            const f = try @import("compat").fs.cwd().createFile(csv_path, .{ .truncate = true });
            defer f.close();
            try f.pwriteAll(case.text, 0);
        }
        out.clearRetainingCapacity();
        err.clearRetainingCapacity();
        const argv: []const []const u8 = if (case.skip)
            &.{ "lattice", "import", path, "--file=" ++ csv_path, "--on-error=skip" }
        else
            &.{ "lattice", "import", path, "--file=" ++ csv_path };
        var parsed = try Args.parse(allocator, argv);
        defer parsed.deinit(allocator);
        try std.testing.expectError(error.CommandFailed, cmdImport(allocator, &out.writer, &err.writer, &parsed));
        try std.testing.expect(std.mem.endsWith(u8, err.written(), case.advice));
    }
}

test "lcf changefeed command sets the policy, trims and reports it" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lcf_cli_changefeed.ltdb";
    cleanupTestDatabaseFiles(path);
    defer cleanupTestDatabaseFiles(path);

    {
        var db = try Database.open(allocator, path, .{
            .create = true,
            .config = .{ .enable_fts = false },
        });
        // Closing checkpoints, so the next open has no log to fold in (a
        // checkpoint applies the policy too).
        defer db.close();
        for (0..5) |_| {
            var txn = try db.beginTransaction(.read_write);
            _ = try db.createNode(&txn, &.{"N"});
            try db.commitTransaction(&txn);
        } // events 1..10
    }

    var out: std.Io.Writer.Allocating = .init(allocator);
    defer out.deinit();

    // Reading leaves everything as it is: the default keeps every event.
    try runForTest(allocator, &.{ "lattice", "changefeed", path, "--format=json" }, &out);
    try std.testing.expect(std.mem.indexOf(u8, out.written(), "\"retention\":\"all\",\"events\":10,") != null);
    try std.testing.expect(std.mem.indexOf(u8, out.written(), "\"oldest_sequence\":1,\"last_sequence\":10,\"trimmed\":0") != null);

    // Setting a policy and trimming drops what it no longer keeps.
    out.clearRetainingCapacity();
    try runForTest(allocator, &.{ "lattice", "changefeed", path, "--changefeed=last:4", "--trim", "--format=json" }, &out);
    try std.testing.expect(std.mem.indexOf(u8, out.written(), "\"retention\":\"last:4\",\"events\":4,") != null);
    try std.testing.expect(std.mem.indexOf(u8, out.written(), "\"oldest_sequence\":7,\"last_sequence\":10,\"trimmed\":6") != null);

    // The file keeps the policy; info shows it.
    out.clearRetainingCapacity();
    try runForTest(allocator, &.{ "lattice", "info", path }, &out);
    try std.testing.expect(std.mem.indexOf(u8, out.written(), "Changefeed:   keep last:4, events 7..10") != null);
    out.clearRetainingCapacity();
    try runForTest(allocator, &.{ "lattice", "info", path, "--format=csv" }, &out);
    try std.testing.expect(std.mem.indexOf(u8, out.written(), "changefeed_retention,last:4\n") != null);

    // Off: nothing held, and the last sequence stays where it was.
    out.clearRetainingCapacity();
    try runForTest(allocator, &.{ "lattice", "changefeed", path, "--changefeed=off", "--trim" }, &out);
    try std.testing.expect(std.mem.indexOf(u8, out.written(), "Retention:      off") != null);
    try std.testing.expect(std.mem.indexOf(u8, out.written(), "Events held:    0") != null);
    try std.testing.expect(std.mem.indexOf(u8, out.written(), "Last sequence:  10") != null);
}
