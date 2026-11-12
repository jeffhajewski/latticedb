//! `lattice import` and `lattice export` for Turtle and N-Triples files.

const std = @import("std");
const lattice = @import("lattice");
const args_mod = @import("../args.zig");
const output = @import("../output.zig");
const import_export = @import("../import_export.zig");
const options_mod = @import("options.zig");
const importer = @import("importer.zig");
const exporter = @import("exporter.zig");

const Database = lattice.storage.database.Database;
const Args = args_mod.Args;
pub const Syntax = options_mod.Syntax;

/// The RDF syntax a command's file is in, or null when it is not RDF.
pub fn syntaxOf(parsed_args: *const Args, file: []const u8) ?Syntax {
    return parsed_args.rdf.syntax orelse options_mod.syntaxFromPath(file);
}

fn fail(stderr: anytype, comptime fmt: []const u8, args: anytype) error{CommandFailed} {
    output.printError(stderr, fmt, args);
    return error.CommandFailed;
}

pub fn runImport(
    allocator: std.mem.Allocator,
    stdout: anytype,
    stderr: anytype,
    db: *Database,
    parsed_args: *const Args,
    file: []const u8,
    syntax: Syntax,
) !void {
    output.printInfo(stdout, "Importing {s} ({s}) into {s}...", .{ file, @tagName(syntax), parsed_args.path.? });

    var diagnostics = import_export.Diagnostics.init(allocator);
    defer diagnostics.deinit();
    var syntax_problem = importer.SyntaxProblem{};
    const started = std.Io.Clock.awake.now(@import("compat").io);
    const result = importer.importFile(allocator, db, file, syntax, parsed_args.rdf, .{
        .batch_size = parsed_args.batch_size,
        .on_error_skip = parsed_args.on_error_skip,
        .diagnostics = &diagnostics,
    }, &syntax_problem);
    const elapsed_ns: u64 = @intCast(@max(0, started.durationTo(std.Io.Clock.awake.now(@import("compat").io)).nanoseconds));

    const s = result catch |err| {
        switch (err) {
            error.SyntaxError => {
                const d = syntax_problem.diag;
                output.printError(stderr, "Import failed: {s} line {d}, column {d}: {s} (at \"{f}\")", .{
                    file, d.pos.line, d.pos.column, d.messageText(), std.zig.fmtString(d.tokenText()),
                });
                try reportKept(stderr, diagnostics.committed, true);
            },
            error.ImportFailed => {
                output.printError(stderr, "Import failed at {s}", .{if (diagnostics.messages.items.len > 0) diagnostics.messages.items[0] else "a statement"});
                try reportKept(stderr, diagnostics.committed, false);
            },
            error.IoError => output.printError(stderr, "Import failed: cannot read {s}", .{file}),
            else => output.printError(stderr, "Import failed: {s}", .{@errorName(err)}),
        }
        return error.CommandFailed;
    };

    for (diagnostics.messages.items) |message| try stderr.print("Skipped {s}\n", .{message});
    if (diagnostics.omitted > 0) try stderr.print("Skipped {d} more statements.\n", .{diagnostics.omitted});

    const seconds = @as(f64, @floatFromInt(elapsed_ns)) / std.time.ns_per_s;
    const per_second: u64 = if (elapsed_ns == 0) 0 else @intFromFloat(@as(f64, @floatFromInt(s.triples)) / seconds);
    const types: []const u8 = if (s.types_as_labels) "labels" else "edges";
    switch (parsed_args.format) {
        .table => {
            output.printSuccess(stdout, "Import complete", .{});
            try stdout.print("  Triples read:        {d} ({d} statements, {d:.1}s, {d} triples/s)\n", .{ s.triples, s.statements, seconds, per_second });
            try stdout.print("  Nodes created:       {d}\n", .{s.nodes_created});
            try stdout.print("  Edges created:       {d}\n", .{s.edges_created});
            try stdout.print("  Property values:     {d} on nodes, {d} on edges\n", .{ s.properties_set, s.edge_properties_set });
            try stdout.print("  Labels added:        {d}\n", .{s.labels_added});
            try stdout.print("  rdf:type as:         {s}", .{types});
            if (parsed_args.rdf.types == .auto) {
                try stdout.print(" ({d}{s} distinct classes; labels up to {d} with the existing labels)", .{
                    s.distinct_types, if (s.distinct_types > importer.type_label_threshold) "+" else "", importer.type_label_threshold,
                });
            }
            try stdout.writeByte('\n');
            try stdout.print("  Names created:       {d} ({d} numbered to stay distinct), prefixes stored: {d}\n", .{ s.names_created, s.names_numbered, s.prefixes_stored });
            if (s.duplicate_edges + s.duplicate_values > 0) {
                try stdout.print("  Already present:     {d} edges, {d} values (not added twice)\n", .{ s.duplicate_edges, s.duplicate_values });
            }
            if (s.map_values > 0) try stdout.print("  Kept as maps:        {d} values whose datatype differs from their property's\n", .{s.map_values});
            if (s.vectors_set + s.vectors_omitted > 0) try stdout.print("  Vectors:             {d} set, {d} left out\n", .{ s.vectors_set, s.vectors_omitted });
            if (s.statements_failed > 0) try stdout.print("  Statements skipped:  {d} ({d} triples)\n", .{ s.statements_failed, s.triples_skipped });
            if (s.index_declared) try stdout.writeAll("  Declared the property index Resource(iri)\n");
            if (s.fts_declared > 0) try stdout.print("  Full-text indexes declared: {d}\n", .{s.fts_declared});
        },
        .json => {
            try stdout.print(
                "{{\"triples\":{d},\"statements\":{d},\"seconds\":{d:.3},\"nodes_created\":{d},\"edges_created\":{d},\"properties_set\":{d},\"edge_properties_set\":{d},\"labels_added\":{d},\"types_as\":\"{s}\",\"distinct_types\":{d},\"names_created\":{d},\"names_numbered\":{d},\"prefixes_stored\":{d},\"duplicate_edges\":{d},\"duplicate_values\":{d},\"map_values\":{d},\"vectors_set\":{d},\"vectors_omitted\":{d},\"statements_failed\":{d},\"triples_skipped\":{d}}}\n",
                .{ s.triples, s.statements, seconds, s.nodes_created, s.edges_created, s.properties_set, s.edge_properties_set, s.labels_added, types, s.distinct_types, s.names_created, s.names_numbered, s.prefixes_stored, s.duplicate_edges, s.duplicate_values, s.map_values, s.vectors_set, s.vectors_omitted, s.statements_failed, s.triples_skipped },
            );
        },
        .csv => {
            try stdout.writeAll("metric,count\n");
            inline for (.{ "triples", "statements", "nodes_created", "edges_created", "properties_set", "edge_properties_set", "labels_added", "names_created", "names_numbered", "prefixes_stored", "duplicate_edges", "duplicate_values", "map_values", "vectors_set", "vectors_omitted", "statements_failed", "triples_skipped" }) |field| {
                try stdout.print(field ++ ",{d}\n", .{@field(s, field)});
            }
        },
    }
}

fn reportKept(stderr: anytype, committed: import_export.ImportStats, syntax: bool) !void {
    if (committed.nodes_imported + committed.edges_imported > 0) {
        try stderr.print("{d} nodes and {d} edges imported in earlier batches were kept.", .{ committed.nodes_imported, committed.edges_imported });
    } else {
        try stderr.writeAll("Nothing was imported.");
    }
    if (syntax) {
        try stderr.writeAll(" Fix the file and import it again; IRIs already imported are reused.\n");
    } else {
        try stderr.writeAll(" Use --on-error=skip to import the statements that are valid.\n");
    }
}

pub fn runExport(
    allocator: std.mem.Allocator,
    stdout: anytype,
    stderr: anytype,
    db: *Database,
    parsed_args: *const Args,
    file: []const u8,
    syntax: Syntax,
    scope: import_export.ExportScope,
) !void {
    const compat = @import("compat");
    const out_file = compat.fs.cwd().createFile(file, .{}) catch |err| {
        return fail(stderr, "Cannot create output file: {s}", .{@errorName(err)});
    };
    defer out_file.close();
    var buffer: [64 * 1024]u8 = undefined;
    var file_writer = out_file.file.writerStreaming(compat.io, &buffer);
    const writer = &file_writer.interface;

    const result = exporter.exportGraph(allocator, db, writer, syntax, parsed_args.rdf, scope);
    const s = blk: {
        const stats = result catch |err| {
            compat.fs.cwd().deleteFile(file) catch {};
            if (err == error.QueryReturnedNoGraph) {
                return fail(stderr, "Export failed: the query returned no nodes or relationships to export. Return them, for example MATCH (n:Person) RETURN n.", .{});
            }
            return fail(stderr, "Export failed: {s}", .{@errorName(err)});
        };
        writer.flush() catch |err| {
            compat.fs.cwd().deleteFile(file) catch {};
            return fail(stderr, "Export failed: {s}", .{@errorName(err)});
        };
        break :blk stats;
    };

    if (s.edge_properties_dropped > 0) {
        try stderr.print("Note: edge property values left out (--rdf-edge-properties=drop): {d}\n", .{s.edge_properties_dropped});
    }
    if (s.vectors_omitted > 0) {
        try stderr.print("Note: node vectors left out (--rdf-vectors=omit): {d}\n", .{s.vectors_omitted});
    }
    switch (parsed_args.format) {
        .table => {
            output.printSuccess(stdout, "Export complete", .{});
            try stdout.print("  Triples written:     {d}\n", .{s.triples});
            try stdout.print("  Nodes exported:      {d} ({d} with an IRI, {d} minted, {d} blank)\n", .{ s.nodes, s.iri_nodes, s.minted_nodes, s.blank_nodes });
            try stdout.print("  Edges exported:      {d} ({d} with properties)\n", .{ s.edges, s.edges_with_properties });
            if (s.vectors > 0) try stdout.print("  Vectors written:     {d}\n", .{s.vectors});
            if (s.minted_names > 0) try stdout.print("  Names without an IRI, written under the base: {d}\n", .{s.minted_names});
        },
        .json => try stdout.print(
            "{{\"triples\":{d},\"nodes_exported\":{d},\"edges_exported\":{d},\"iri_nodes\":{d},\"minted_nodes\":{d},\"blank_nodes\":{d},\"edges_with_properties\":{d},\"edge_properties_dropped\":{d},\"vectors\":{d},\"vectors_omitted\":{d},\"minted_names\":{d}}}\n",
            .{ s.triples, s.nodes, s.edges, s.iri_nodes, s.minted_nodes, s.blank_nodes, s.edges_with_properties, s.edge_properties_dropped, s.vectors, s.vectors_omitted, s.minted_names },
        ),
        .csv => {
            try stdout.writeAll("metric,count\n");
            inline for (.{ "triples", "nodes", "edges", "iri_nodes", "minted_nodes", "blank_nodes", "edges_with_properties", "edge_properties_dropped", "vectors", "vectors_omitted", "minted_names" }) |field| {
                try stdout.print(field ++ ",{d}\n", .{@field(s, field)});
            }
        },
    }
}
