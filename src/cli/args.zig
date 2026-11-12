//! Command-line argument parsing for Lattice CLI.

const std = @import("std");
const lattice = @import("lattice");

const types = lattice.core.types;
const rdf_options = @import("rdf/options.zig");
const page_manager = lattice.storage.page_manager;
const VectorMetric = lattice.storage.database.VectorMetric;
const Retention = lattice.stream.changefeed.Retention;

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
    checkpoint,
    changefeed,
    backup,
    replicate,
    restore,
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
    update,
    version,
    help,

    pub fn fromString(s: []const u8) ?Command {
        if (std.mem.eql(u8, s, "create")) return .create;
        if (std.mem.eql(u8, s, "info")) return .info;
        if (std.mem.eql(u8, s, "compact")) return .compact;
        if (std.mem.eql(u8, s, "checkpoint")) return .checkpoint;
        if (std.mem.eql(u8, s, "changefeed")) return .changefeed;
        if (std.mem.eql(u8, s, "backup")) return .backup;
        if (std.mem.eql(u8, s, "replicate")) return .replicate;
        if (std.mem.eql(u8, s, "restore")) return .restore;
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
        if (std.mem.eql(u8, s, "update")) return .update;
        if (std.mem.eql(u8, s, "version") or std.mem.eql(u8, s, "-v") or std.mem.eql(u8, s, "--version")) return .version;
        if (std.mem.eql(u8, s, "help") or std.mem.eql(u8, s, "-h") or std.mem.eql(u8, s, "--help")) return .help;
        return null;
    }

    pub fn requiresPath(self: Command) bool {
        return switch (self) {
            .update, .version, .help => false,
            else => true,
        };
    }

    pub fn description(self: Command) []const u8 {
        return switch (self) {
            .create => "Create a new database",
            .info => "Show database information",
            .compact => "Reclaim free pages from the end of a database file",
            .checkpoint => "Flush pending writes and reset the write-ahead log",
            .changefeed => "Show or trim the changefeed and set how much of it to keep",
            .backup => "Copy a database to another file without closing it",
            .replicate => "Ship changes to a directory, once or continuously",
            .restore => "Rebuild a database from what replication shipped",
            .check => "Verify main database file checksums",
            .query => "Interactive Cypher REPL",
            .exec => "Execute a single query",
            .import => "Import data from JSON/CSV",
            .@"export" => "Export data to JSON/JSONL/CSV/DOT",
            .dump => "Dump full database as canonical JSON",
            .labels => "List all node labels",
            .types => "List all edge types",
            .schema => "Show inferred schema",
            .count => "Show node/edge counts",
            .update => "Update the local LatticeDB installation",
            .version => "Show version information",
            .help => "Show help message",
        };
    }
};

/// How many days there are from 1970-01-01 to the given date.
///
/// This is Howard Hinnant's `days_from_civil`, which works by shifting the year
/// so that it starts in March. February then falls at the end, which is what
/// makes the leap day a special case only at the boundary rather than in the
/// middle of the arithmetic.
fn daysFromCivil(year: i64, month: u32, day: u32) i64 {
    const y = year - @as(i64, if (month <= 2) 1 else 0);
    const era = @divFloor(if (y >= 0) y else y - 399, 400);
    const yoe = y - era * 400;
    const mp: i64 = @intCast((month + 9) % 12);
    const doy = @divTrunc(153 * mp + 2, 5) + @as(i64, @intCast(day)) - 1;
    const doe = yoe * 365 + @divTrunc(yoe, 4) - @divTrunc(yoe, 100) + doy;
    return era * 146097 + doe - 719468;
}

fn parseField(text: []const u8, comptime T: type) ?T {
    return std.fmt.parseInt(T, text, 10) catch null;
}

/// Read a moment in time, given as UTC.
///
/// Accepts `2026-08-25T14:30:00Z`, the same with a space instead of the `T`, and
/// a bare date meaning midnight. Everything is treated as UTC, because a backup
/// that restores to a different moment depending on the machine's time zone
/// would be a trap.
pub fn parseTimestamp(text: []const u8) ?i64 {
    if (text.len < 10) return null;
    if (text[4] != '-' or text[7] != '-') return null;

    const year = parseField(text[0..4], i64) orelse return null;
    const month = parseField(text[5..7], u32) orelse return null;
    const day = parseField(text[8..10], u32) orelse return null;
    if (month < 1 or month > 12 or day < 1 or day > 31) return null;

    var seconds = daysFromCivil(year, month, day) * 86400;

    if (text.len > 10) {
        if (text[10] != 'T' and text[10] != ' ') return null;
        if (text.len < 16) return null;
        if (text[13] != ':') return null;

        const hour = parseField(text[11..13], i64) orelse return null;
        const minute = parseField(text[14..16], i64) orelse return null;
        if (hour > 23 or minute > 59) return null;
        seconds += hour * 3600 + minute * 60;

        if (text.len > 16) {
            if (text[16] != ':') return null;
            if (text.len < 19) return null;
            const second = parseField(text[17..19], i64) orelse return null;
            if (second > 59) return null;
            seconds += second;

            // A trailing Z is allowed and means what is already assumed.
            if (text.len > 19 and !(text.len == 20 and text[19] == 'Z')) return null;
        } else if (text.len != 16) {
            return null;
        }
    }

    return seconds * 1000;
}

/// The date that many days after 1970-01-01.
///
/// The inverse of `daysFromCivil`, and it inverts the same trick: the year is
/// treated as starting in March so that February, leap day and all, lands at the
/// end where it does not disturb the arithmetic.
fn civilFromDays(days: i64) struct { year: i64, month: u32, day: u32 } {
    const z = days + 719468;
    const era = @divFloor(if (z >= 0) z else z - 146096, 146097);
    const doe = z - era * 146097;
    const yoe = @divTrunc(doe - @divTrunc(doe, 1460) + @divTrunc(doe, 36524) - @divTrunc(doe, 146096), 365);
    const y = yoe + era * 400;
    const doy = doe - (365 * yoe + @divTrunc(yoe, 4) - @divTrunc(yoe, 100));
    const mp = @divTrunc(5 * doy + 2, 153);
    const d = doy - @divTrunc(153 * mp + 2, 5) + 1;
    const m = mp + @as(i64, if (mp < 10) 3 else -9);
    return .{
        .year = y + @as(i64, if (m <= 2) 1 else 0),
        .month = @intCast(m),
        .day = @intCast(d),
    };
}

/// Write a moment in time the way a person reads one.
///
/// A raw count of milliseconds tells nobody anything, and this number is the one
/// that says which version of their data they just got back.
pub fn formatTimestamp(buf: []u8, at_ms: i64) []const u8 {
    const seconds = @divFloor(at_ms, 1000);
    const days = @divFloor(seconds, 86400);
    const rest = seconds - days * 86400;

    const date = civilFromDays(days);
    // The year is formatted unsigned, because a signed value prints its sign and
    // "+2026" is not a date anybody writes.
    return std.fmt.bufPrint(buf, "{d:0>4}-{d:0>2}-{d:0>2}T{d:0>2}:{d:0>2}:{d:0>2}Z", .{
        @as(u32, @intCast(@max(date.year, 0))),
        date.month,
        date.day,
        @as(u32, @intCast(@divTrunc(rest, 3600))),
        @as(u32, @intCast(@divTrunc(@rem(rest, 3600), 60))),
        @as(u32, @intCast(@rem(rest, 60))),
    }) catch "unknown";
}

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
    /// Whether --vector-dims was given. Opening an existing database without
    /// it takes the dimensions from the file.
    vector_dims_set: bool = false,
    /// --vector-metric, --vector-m and --vector-ef-construction, for `create`.
    vector_metric: ?VectorMetric = null,
    vector_m: ?u16 = null,
    vector_ef_construction: ?u16 = null,
    enable_fts: bool = true,
    /// --changefeed=<policy>: replaces the file's changefeed retention
    /// policy when the database is opened for writing.
    changefeed_retention: ?Retention = null,
    /// --trim, for `changefeed`.
    trim: bool = false,
    /// --cache-size; null when not given: LATTICE_BUFFER_POOL_MB if set, else 64.
    cache_size_mb: ?u32 = null,
    page_size: u32 = 4096,

    // Import options
    batch_size: u32 = 1000,
    on_error_skip: bool = false,

    // RDF import and export options
    rdf: rdf_options.Options = .{},

    // Filter options
    label_filter: ?[]const u8 = null,

    // Replication options
    to: ?[]const u8 = null,
    follow: bool = false,
    interval_secs: u32 = 10,
    /// Whether --interval was given; it means something only with --follow.
    interval_set: bool = false,

    /// Skip the file lock. For filesystems where locking does not work, not
    /// for arranging concurrent access.
    no_lock: bool = false,

    // Restore options
    output: ?[]const u8 = null,
    at_ms: ?i64 = null,
    force: bool = false,

    /// --upgrade-indexes, for `compact`.
    upgrade_indexes: bool = false,

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
                    if (rdf_options.syntaxFromName(value)) |syntax| {
                        args.rdf.syntax = syntax;
                    } else {
                        args.format = OutputFormat.fromString(value) orelse {
                            return error.InvalidFormat;
                        };
                    }
                } else if (std.mem.eql(u8, arg, "--enable-vector")) {
                    args.enable_vector = true;
                } else if (std.mem.startsWith(u8, arg, "--vector-dims=")) {
                    const value = arg["--vector-dims=".len..];
                    const parsed_dims = std.fmt.parseInt(u32, value, 10) catch {
                        return error.InvalidVectorDims;
                    };
                    types.validateVectorDimensions(parsed_dims) catch return error.InvalidVectorDims;
                    args.vector_dims = @intCast(parsed_dims);
                    args.vector_dims_set = true;
                } else if (std.mem.startsWith(u8, arg, "--vector-metric=")) {
                    const value = arg["--vector-metric=".len..];
                    args.vector_metric = VectorMetric.fromName(value) orelse return error.InvalidVectorMetric;
                } else if (std.mem.startsWith(u8, arg, "--vector-m=")) {
                    const value = arg["--vector-m=".len..];
                    const m = std.fmt.parseInt(u16, value, 10) catch return error.InvalidVectorM;
                    if (m < 2 or m > 32) return error.InvalidVectorM;
                    args.vector_m = m;
                } else if (std.mem.startsWith(u8, arg, "--vector-ef-construction=")) {
                    const value = arg["--vector-ef-construction=".len..];
                    const ef = std.fmt.parseInt(u16, value, 10) catch return error.InvalidVectorEfConstruction;
                    if (ef < 1 or ef > 4096) return error.InvalidVectorEfConstruction;
                    args.vector_ef_construction = ef;
                } else if (std.mem.startsWith(u8, arg, "--changefeed=")) {
                    args.changefeed_retention = Retention.parse(arg["--changefeed=".len..]) orelse
                        return error.InvalidChangefeedRetention;
                } else if (std.mem.eql(u8, arg, "--trim")) {
                    args.trim = true;
                } else if (std.mem.eql(u8, arg, "--enable-fts")) {
                    args.enable_fts = true;
                } else if (std.mem.eql(u8, arg, "--no-fts")) {
                    args.enable_fts = false;
                } else if (std.mem.startsWith(u8, arg, "--cache-size=")) {
                    const value = arg["--cache-size=".len..];
                    args.cache_size_mb = std.fmt.parseInt(u32, value, 10) catch {
                        return error.InvalidCacheSize;
                    };
                } else if (std.mem.startsWith(u8, arg, "--page-size=")) {
                    const value = arg["--page-size=".len..];
                    args.page_size = std.fmt.parseInt(u32, value, 10) catch {
                        return error.InvalidPageSize;
                    };
                    if (!page_manager.isValidPageSize(args.page_size)) {
                        return error.InvalidPageSize;
                    }
                } else if (std.mem.startsWith(u8, arg, "--batch-size=")) {
                    const value = arg["--batch-size=".len..];
                    args.batch_size = std.fmt.parseInt(u32, value, 10) catch {
                        return error.InvalidBatchSize;
                    };
                } else if (std.mem.eql(u8, arg, "--on-error=skip")) {
                    args.on_error_skip = true;
                } else if (std.mem.startsWith(u8, arg, "--labels=")) {
                    args.label_filter = arg["--labels=".len..];
                } else if (std.mem.startsWith(u8, arg, "--to=")) {
                    args.to = arg["--to=".len..];
                } else if (std.mem.eql(u8, arg, "--follow")) {
                    args.follow = true;
                } else if (std.mem.startsWith(u8, arg, "--interval=")) {
                    const value = arg["--interval=".len..];
                    args.interval_secs = std.fmt.parseInt(u32, value, 10) catch {
                        return error.InvalidInterval;
                    };
                    if (args.interval_secs == 0) return error.InvalidInterval;
                    args.interval_set = true;
                } else if (std.mem.eql(u8, arg, "--no-lock")) {
                    args.no_lock = true;
                } else if (std.mem.startsWith(u8, arg, "--output=")) {
                    args.output = arg["--output=".len..];
                } else if (std.mem.eql(u8, arg, "--force")) {
                    args.force = true;
                } else if (std.mem.eql(u8, arg, "--upgrade-indexes")) {
                    args.upgrade_indexes = true;
                } else if (std.mem.startsWith(u8, arg, "--at=")) {
                    args.at_ms = parseTimestamp(arg["--at=".len..]) orelse {
                        return error.InvalidTimestamp;
                    };
                } else if (std.mem.startsWith(u8, arg, "--file=")) {
                    args.file = arg["--file=".len..];
                } else if (std.mem.startsWith(u8, arg, "--query=")) {
                    args.query_string = arg["--query=".len..];
                } else if (try rdf_options.parseArg(&args.rdf, arg)) {
                    // An RDF import or export option.
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

        // An option that only means something with another one would
        // otherwise be ignored without a word: a database created with
        // --vector-dims and no --enable-vector has no vector index.
        if (!args.enable_vector and (args.vector_dims_set or args.vector_metric != null or
            args.vector_m != null or args.vector_ef_construction != null))
        {
            return error.VectorOptionWithoutEnableVector;
        }
        if (args.interval_set and !args.follow) return error.IntervalWithoutFollow;

        args.positional = try positional.toOwnedSlice();
        return args;
    }

    pub fn deinit(self: *Args, allocator: std.mem.Allocator) void {
        allocator.free(self.positional);
    }
};

pub const Error = error{
    InvalidFormat,
    InvalidInterval,
    InvalidTimestamp,
    InvalidVectorDims,
    InvalidVectorMetric,
    InvalidVectorM,
    InvalidVectorEfConstruction,
    InvalidChangefeedRetention,
    VectorOptionWithoutEnableVector,
    IntervalWithoutFollow,
    InvalidCacheSize,
    InvalidPageSize,
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

test "parse update command" {
    const allocator = std.testing.allocator;
    var args = try Args.parse(allocator, &.{ "lattice", "update" });
    defer args.deinit(allocator);

    try std.testing.expectEqual(Command.update, args.command.?);
    try std.testing.expect(!args.command.?.requiresPath());
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

test "parse the index upgrade option for compact" {
    const allocator = std.testing.allocator;
    var plain = try Args.parse(allocator, &.{ "lattice", "compact", "test.db" });
    defer plain.deinit(allocator);
    try std.testing.expect(!plain.upgrade_indexes);

    var args = try Args.parse(allocator, &.{ "lattice", "compact", "test.db", "--upgrade-indexes" });
    defer args.deinit(allocator);
    try std.testing.expectEqual(Command.compact, args.command.?);
    try std.testing.expectEqualStrings("test.db", args.path.?);
    try std.testing.expect(args.upgrade_indexes);
}

test "parse vector dimensions range" {
    const allocator = std.testing.allocator;
    var args = try Args.parse(allocator, &.{ "lattice", "create", "test.db", "--enable-vector", "--vector-dims=4096" });
    defer args.deinit(allocator);

    try std.testing.expect(args.enable_vector);
    try std.testing.expectEqual(@as(u16, 4096), args.vector_dims);

    try std.testing.expectError(error.InvalidVectorDims, Args.parse(allocator, &.{ "lattice", "create", "test.db", "--vector-dims=0" }));
    try std.testing.expectError(error.InvalidVectorDims, Args.parse(allocator, &.{ "lattice", "create", "test.db", "--vector-dims=4097" }));
}

test "parse vector index options for create" {
    const allocator = std.testing.allocator;
    var args = try Args.parse(allocator, &.{ "lattice", "create", "test.db", "--enable-vector", "--vector-dims=8", "--vector-metric=dot_product", "--vector-m=24", "--vector-ef-construction=64" });
    defer args.deinit(allocator);
    try std.testing.expect(args.vector_dims_set);
    try std.testing.expectEqual(@as(?VectorMetric, .dot_product), args.vector_metric);
    try std.testing.expectEqual(@as(?u16, 24), args.vector_m);
    try std.testing.expectEqual(@as(?u16, 64), args.vector_ef_construction);

    var plain = try Args.parse(allocator, &.{ "lattice", "info", "test.db", "--enable-vector" });
    defer plain.deinit(allocator);
    try std.testing.expect(!plain.vector_dims_set);
    try std.testing.expect(plain.vector_metric == null);

    try std.testing.expectError(error.InvalidVectorMetric, Args.parse(allocator, &.{ "lattice", "create", "t.db", "--vector-metric=manhattan" }));
    try std.testing.expectError(error.InvalidChangefeedRetention, Args.parse(allocator, &.{ "lattice", "create", "t.db", "--changefeed=sometimes" }));
    var feed = try Args.parse(allocator, &.{ "lattice", "changefeed", "t.db", "--changefeed=last:1000", "--trim" });
    defer feed.deinit(allocator);
    try std.testing.expectEqual(@as(?Command, .changefeed), feed.command);
    try std.testing.expectEqual(@as(u64, 1000), feed.changefeed_retention.?.keep_last);
    try std.testing.expect(feed.trim);
    // One spelling: the one the bindings and `lattice info` use.
    try std.testing.expectError(error.InvalidVectorMetric, Args.parse(allocator, &.{ "lattice", "create", "t.db", "--enable-vector", "--vector-metric=dot" }));
    try std.testing.expectError(error.InvalidVectorMetric, Args.parse(allocator, &.{ "lattice", "create", "t.db", "--enable-vector", "--vector-metric=inner_product" }));
    try std.testing.expectError(error.InvalidVectorM, Args.parse(allocator, &.{ "lattice", "create", "t.db", "--vector-m=1" }));
    try std.testing.expectError(error.InvalidVectorM, Args.parse(allocator, &.{ "lattice", "create", "t.db", "--vector-m=33" }));
    try std.testing.expectError(error.InvalidVectorEfConstruction, Args.parse(allocator, &.{ "lattice", "create", "t.db", "--vector-ef-construction=0" }));
}

test "lfmt vector options without --enable-vector are refused, not ignored" {
    const allocator = std.testing.allocator;
    for ([_][]const u8{ "--vector-dims=8", "--vector-metric=cosine", "--vector-m=16", "--vector-ef-construction=64" }) |option| {
        try std.testing.expectError(error.VectorOptionWithoutEnableVector, Args.parse(allocator, &.{ "lattice", "create", "t.db", option }));
        try std.testing.expectError(error.VectorOptionWithoutEnableVector, Args.parse(allocator, &.{ "lattice", "info", "t.db", option }));
        var with = try Args.parse(allocator, &.{ "lattice", "create", "t.db", option, "--enable-vector" });
        with.deinit(allocator);
    }
}

test "lfmt --interval without --follow is refused, not ignored" {
    const allocator = std.testing.allocator;
    try std.testing.expectError(error.IntervalWithoutFollow, Args.parse(allocator, &.{ "lattice", "replicate", "t.db", "--to=r", "--interval=5" }));
    var with = try Args.parse(allocator, &.{ "lattice", "replicate", "t.db", "--to=r", "--follow", "--interval=5" });
    defer with.deinit(allocator);
    try std.testing.expectEqual(@as(u32, 5), with.interval_secs);
}

test "parse page size option" {
    const allocator = std.testing.allocator;
    var args = try Args.parse(allocator, &.{ "lattice", "create", "test.db", "--page-size=32768" });
    defer args.deinit(allocator);

    try std.testing.expectEqual(@as(u32, 32768), args.page_size);

    try std.testing.expectError(error.InvalidPageSize, Args.parse(allocator, &.{ "lattice", "create", "test.db", "--page-size=4095" }));
    try std.testing.expectError(error.InvalidPageSize, Args.parse(allocator, &.{ "lattice", "create", "test.db", "--page-size=65536" }));
}

test "timestamps read and print as the same moment" {
    const cases = [_][]const u8{
        "2026-08-25T14:30:00Z",
        "1970-01-01T00:00:00Z",
        "2000-02-29T12:00:00Z",
        "2100-03-01T23:59:59Z",
        "1999-12-31T23:59:59Z",
    };

    var buf: [32]u8 = undefined;
    for (cases) |text| {
        const at_ms = parseTimestamp(text) orelse return error.ParseFailed;
        try std.testing.expectEqualStrings(text, formatTimestamp(&buf, at_ms));
    }
}

test "timestamps accept the shapes people actually type" {
    // A bare date means midnight, and the epoch is a value everyone can check.
    try std.testing.expectEqual(@as(?i64, 0), parseTimestamp("1970-01-01"));
    try std.testing.expectEqual(@as(?i64, 0), parseTimestamp("1970-01-01T00:00"));
    try std.testing.expectEqual(@as(?i64, 0), parseTimestamp("1970-01-01T00:00:00"));
    try std.testing.expectEqual(@as(?i64, 0), parseTimestamp("1970-01-01T00:00:00Z"));

    // A space instead of the T, because that is how a shell prompt tends to
    // produce them.
    try std.testing.expectEqual(@as(?i64, 0), parseTimestamp("1970-01-01 00:00:00"));

    // One hour in.
    try std.testing.expectEqual(@as(?i64, 3_600_000), parseTimestamp("1970-01-01T01:00:00Z"));
}

test "a time that is not a time is refused" {
    const bad = [_][]const u8{
        "",
        "not-a-time",
        "2026-13-01",
        "2026-00-01",
        "2026-08-32",
        "2026-08-25T25:00:00",
        "2026-08-25T12:60:00",
        "2026-08-25T12:00:61",
        "2026/08/25",
        "2026-08-25X12:00:00",
        "2026-08-25T12:00:00X",
        "2026-08-25T12",
    };

    for (bad) |text| {
        try std.testing.expectEqual(@as(?i64, null), parseTimestamp(text));
    }
}
