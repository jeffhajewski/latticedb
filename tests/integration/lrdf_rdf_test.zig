//! RDF import and export through the `lattice` command: Turtle and
//! N-Triples into a property graph and back.
//!
//! The files under tests/rdf/data each exercise one part of the grammar or
//! one mapping rule. Round trips are checked as RDF graphs, with blank nodes
//! matched up and numbers compared by value, so they hold however the export
//! happens to spell things.

const std = @import("std");
const build_options = @import("build_options");
const compat = @import("compat");
const import_export = @import("import_export");

const rdf = import_export.rdf;
const parser = rdf.parser;
const isomorphism = rdf.isomorphism;

const io = std.testing.io;
const data_dir = "tests/rdf/data/";

const CliRun = struct {
    exit_code: ?u8,
    stdout: []u8,
    stderr: []u8,

    fn deinit(self: CliRun, allocator: std.mem.Allocator) void {
        allocator.free(self.stdout);
        allocator.free(self.stderr);
    }

    fn succeeded(self: CliRun) bool {
        return self.exit_code != null and self.exit_code.? == 0;
    }
};

fn runCli(allocator: std.mem.Allocator, args: []const []const u8) !CliRun {
    var argv: std.ArrayList([]const u8) = .empty;
    defer argv.deinit(allocator);
    try argv.append(allocator, build_options.cli_path);
    try argv.appendSlice(allocator, args);
    const result = try std.process.run(allocator, io, .{ .argv = argv.items });
    return .{
        .exit_code = switch (result.term) {
            .exited => |code| code,
            else => null,
        },
        .stdout = result.stdout,
        .stderr = result.stderr,
    };
}

/// Run the command and fail the test, showing its output, when it fails.
fn mustRun(allocator: std.mem.Allocator, args: []const []const u8) !void {
    const run = try runCli(allocator, args);
    defer run.deinit(allocator);
    if (!run.succeeded()) {
        std.debug.print("lattice {s} failed:\n{s}{s}\n", .{ args[0], run.stdout, run.stderr });
        return error.TestUnexpectedResult;
    }
}

fn deleteDatabaseFiles(path: []const u8) void {
    compat.fs.cwd().deleteFile(path) catch {};
    var wal_buf: [256]u8 = undefined;
    const wal = std.fmt.bufPrint(&wal_buf, "{s}-wal", .{path}) catch return;
    compat.fs.cwd().deleteFile(wal) catch {};
}

fn freshDatabase(allocator: std.mem.Allocator, path: []const u8) !void {
    deleteDatabaseFiles(path);
    try mustRun(allocator, &.{ "create", path });
}

fn readFileAlloc(allocator: std.mem.Allocator, path: []const u8) ![]u8 {
    const file = try compat.fs.cwd().openFile(path, .{});
    defer file.close();
    return file.readToEndAlloc(allocator, 1 << 30);
}

/// A file's triples, collections spelled out as rdf:first/rdf:rest.
fn loadGraph(arena: std.mem.Allocator, path: []const u8, collector: *parser.Collector) !void {
    const text = try readFileAlloc(arena, path);
    const syntax: parser.Syntax = if (std.mem.endsWith(u8, path, ".nt")) .ntriples else .turtle;
    var diag: parser.Diagnostic = .{};
    parser.parseText(arena, text, .{ .syntax = syntax, .base = "file:///base/" }, collector, &diag) catch |err| {
        std.debug.print("{s} line {d}, column {d}: {s}\n", .{ path, diag.pos.line, diag.pos.column, diag.messageText() });
        return err;
    };
}

/// Fail unless the two files hold the same RDF graph.
fn expectSameGraph(allocator: std.mem.Allocator, expected_path: []const u8, actual_path: []const u8) !void {
    var arena_state = std.heap.ArenaAllocator.init(allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    var expected = parser.Collector.init(arena);
    var actual = parser.Collector.init(arena);
    try loadGraph(arena, expected_path, &expected);
    try loadGraph(arena, actual_path, &actual);
    const result = try isomorphism.compare(arena, expected.triples.items, actual.triples.items, .{ .by_value = true });
    if (!result.same) {
        const written = readFileAlloc(arena, actual_path) catch "";
        std.debug.print(
            "{s} and {s} are not the same graph ({d} and {d} triples)\n  only in the first: {s}\n  only in the second: {s}\n{s}\n",
            .{ expected_path, actual_path, expected.triples.items.len, actual.triples.items.len, result.only_in_a orelse "-", result.only_in_b orelse "-", written },
        );
        return error.TestUnexpectedResult;
    }
}

/// Run a query with JSON output and fail unless it contains `expected`.
fn expectQuery(allocator: std.mem.Allocator, db: []const u8, query: []const u8, expected: []const u8) !void {
    const query_arg = try std.fmt.allocPrint(allocator, "--query={s}", .{query});
    defer allocator.free(query_arg);
    const run = try runCli(allocator, &.{ "exec", db, query_arg, "--format=json" });
    defer run.deinit(allocator);
    if (!run.succeeded() or std.mem.indexOf(u8, run.stdout, expected) == null) {
        std.debug.print("{s}\n  expected to contain: {s}\n  got: {s}{s}\n", .{ query, expected, run.stdout, run.stderr });
        return error.TestUnexpectedResult;
    }
}

const round_trip_files = [_][]const u8{
    "grammar_directives.ttl",
    "grammar_literals.ttl",
    "grammar_blank_nodes.ttl",
    "grammar_collections.ttl",
    "grammar_star.ttl",
    "mapping_types.ttl",
    "mapping_datatypes.ttl",
    "mapping_languages.ttl",
    "mapping_names.ttl",
    "mapping_repeated.ttl",
    "basic.nt",
};

test "lrdf: each grammar and mapping file comes back from the database as the same graph" {
    const allocator = std.testing.allocator;
    const db = "/tmp/lrdf_round_trip.ltdb";
    const ttl = "/tmp/lrdf_round_trip.ttl";
    const nt = "/tmp/lrdf_round_trip.nt";
    defer deleteDatabaseFiles(db);
    defer compat.fs.cwd().deleteFile(ttl) catch {};
    defer compat.fs.cwd().deleteFile(nt) catch {};

    for (round_trip_files) |name| {
        const path = try std.fmt.allocPrint(allocator, data_dir ++ "{s}", .{name});
        defer allocator.free(path);
        const file_arg = try std.fmt.allocPrint(allocator, "--file={s}", .{path});
        defer allocator.free(file_arg);

        try freshDatabase(allocator, db);
        // The comparison reads the original with this base too.
        try mustRun(allocator, &.{ "import", db, file_arg, "--base=file:///base/" });
        try mustRun(allocator, &.{ "export", db, "--file=" ++ ttl });
        try mustRun(allocator, &.{ "export", db, "--file=" ++ nt });
        try expectSameGraph(allocator, path, ttl);
        try expectSameGraph(allocator, path, nt);
    }
}

test "lrdf: Cypher sees types as labels, literals as typed properties, and the rest as edges" {
    const allocator = std.testing.allocator;
    const db = "/tmp/lrdf_mapping.ltdb";
    defer deleteDatabaseFiles(db);
    try freshDatabase(allocator, db);
    for ([_][]const u8{ "mapping_types.ttl", "mapping_datatypes.ttl", "mapping_languages.ttl", "mapping_repeated.ttl", "mapping_names.ttl", "grammar_star.ttl", "grammar_collections.ttl" }) |name| {
        const file_arg = try std.fmt.allocPrint(allocator, "--file=" ++ data_dir ++ "{s}", .{name});
        defer allocator.free(file_arg);
        try mustRun(allocator, &.{ "import", db, file_arg });
    }

    // rdf:type
    try expectQuery(allocator, db, "MATCH (p:foaf_Person:ex_Employee) RETURN p.foaf_name", "\"Ann\"");
    try expectQuery(allocator, db, "MATCH (o:foaf_Organization) RETURN count(o) AS n", "\"n\":1");
    // Datatypes
    const s = "MATCH (s:Resource {iri: 'http://example.org/s'}) RETURN ";
    try expectQuery(allocator, db, s ++ "s.ex_int AS v", "\"v\":17");
    try expectQuery(allocator, db, s ++ "s.ex_long AS v", "\"v\":-9223372036854775808");
    try expectQuery(allocator, db, s ++ "s.ex_exactDecimal AS v", "\"v\":2.5");
    try expectQuery(allocator, db, s ++ "s.ex_inexactDecimal AS v", "\"v\":\"0.1000000000000000000001\"");
    try expectQuery(allocator, db, s ++ "s.ex_bool AS v", "\"v\":true");
    try expectQuery(allocator, db, s ++ "s.ex_json.a AS v", "\"v\":[1,2]");
    try expectQuery(allocator, db, s ++ "s.ex_custom AS v", "\"v\":\"x-42\"");
    // Each kind of value keeps the datatype it was first seen with: ex:mixed
    // holds an integer, a string and a float, each plain.
    try expectQuery(allocator, db, s ++ "s.ex_mixed AS v", "\"v\":1");
    const t = "MATCH (t:Resource {iri: 'http://example.org/t'}) RETURN ";
    try expectQuery(allocator, db, t ++ "t.ex_mixed AS v", "\"v\":\"one\"");
    // A string whose datatype is not the one ex:custom's strings have is a map.
    try expectQuery(allocator, db, t ++ "t.ex_custom AS v", "\"v\":{\"@value\":\"plain\",\"@type\":\"http://www.w3.org/2001/XMLSchema#string\"}");
    // Languages: English is the primary property, others are suffixed.
    const c = "MATCH (c:Resource {iri: 'http://example.org/c'}) RETURN ";
    try expectQuery(allocator, db, c ++ "c.skos_prefLabel AS v", "\"v\":\"Cat\"");
    try expectQuery(allocator, db, c ++ "c.skos_prefLabel_fr AS v", "\"v\":\"Chat\"");
    try expectQuery(allocator, db, c ++ "c.skos_altLabel AS v", "\"v\":[\"Kitty\",\"Puss\"]");
    // Plain strings of a property whose strings are English have their own.
    try expectQuery(allocator, db, "MATCH (d:Resource {iri: 'http://example.org/d'}) RETURN d.skos_prefLabel_plain AS v", "\"v\":\"Dog\"");
    // Repeated values are a list, once each; repeated edges are one edge.
    try expectQuery(allocator, db, s ++ "s.ex_alias AS v", "\"v\":[\"a\",\"b\",\"c\"]");
    try expectQuery(allocator, db, "MATCH (:Resource {iri: 'http://example.org/s'})-[r:ex_links]->() RETURN count(r) AS n", "\"n\":2");
    // Names: cleaned up, and numbered when two IRIs would share one.
    try expectQuery(allocator, db, s ++ "s.ex_first_name AS a, s.ex_first_name_2 AS b, s.other_first_name AS c", "\"a\":\"a\",\"b\":\"b\",\"c\":\"c\"");
    // RDF-star annotations are edge properties.
    try expectQuery(allocator, db, "MATCH (:Resource {iri: 'http://example.org/ann'})-[r:ex_worksFor]->() RETURN r.ex_since AS v", "\"v\":2019");
    try expectQuery(allocator, db, "MATCH (:Resource {iri: 'http://example.org/ann'})-[r:ex_knows]->() RETURN r.ex_certainty AS v", "\"v\":0.9");
    // A collection of literals is one list property.
    try expectQuery(allocator, db, s ++ "s.ex_tags_list AS v", "\"v\":[\"red\",\"green\",\"blue\"]");
}

test "lrdf: a property graph exported as Turtle imports back as the same graph" {
    const allocator = std.testing.allocator;
    const db = "/tmp/lrdf_pg.ltdb";
    const db2 = "/tmp/lrdf_pg2.ltdb";
    const first = "/tmp/lrdf_pg_first.ttl";
    const second = "/tmp/lrdf_pg_second.ttl";
    defer deleteDatabaseFiles(db);
    defer deleteDatabaseFiles(db2);
    defer compat.fs.cwd().deleteFile(first) catch {};
    defer compat.fs.cwd().deleteFile(second) catch {};

    try freshDatabase(allocator, db);
    try mustRun(allocator, &.{
        "exec", db,
        "--query=CREATE (a:Person:Employee {name: 'Ann', age: 41, score: 3.25, tags: ['x', 'y'], meta: {k: 1}, ok: false})" ++
            "-[:WORKS_AT {since: 2019, role: 'dev'}]->(c:Company {name: 'Acme'}), (b:Person {name: 'Ben'})-[:WORKS_AT]->(c)",
    });
    try mustRun(allocator, &.{ "export", db, "--file=" ++ first });
    try freshDatabase(allocator, db2);
    try mustRun(allocator, &.{ "import", db2, "--file=" ++ first });
    try mustRun(allocator, &.{ "export", db2, "--file=" ++ second });
    try expectSameGraph(allocator, first, second);

    // The same labels, names and values, without any trace of RDF.
    try expectQuery(allocator, db2, "MATCH (p:Person:Employee)-[r:WORKS_AT]->(c:Company) RETURN p.name AS n, p.age AS a, p.score AS s, p.tags AS t, p.meta.k AS k, p.ok AS o, r.since AS y, r.role AS role, c.name AS c", "\"n\":\"Ann\",\"a\":41,\"s\":3.25,\"t\":[\"x\",\"y\"],\"k\":1,\"o\":false,\"y\":2019,\"role\":\"dev\",\"c\":\"Acme\"");
    try expectQuery(allocator, db2, "MATCH (p:Person) RETURN count(p) AS n", "\"n\":2");
    try expectQuery(allocator, db2, "MATCH (:Person)-[r:WORKS_AT]->(:Company) RETURN count(r) AS n", "\"n\":2");
}

test "lrdf: export options mint IRIs, reify or drop edge properties, and scope by label" {
    const allocator = std.testing.allocator;
    const db = "/tmp/lrdf_options.ltdb";
    const out = "/tmp/lrdf_options.ttl";
    defer deleteDatabaseFiles(db);
    defer compat.fs.cwd().deleteFile(out) catch {};

    try freshDatabase(allocator, db);
    try mustRun(allocator, &.{ "exec", db, "--query=CREATE (a:Person {name: 'Ann'})-[:KNOWS {since: 2001}]->(b:Person {name: 'Ben'}), (c:City {name: 'Oslo'})" });

    try mustRun(allocator, &.{ "export", db, "--file=" ++ out, "--base=https://example.org/g/" });
    var text = try readFileAlloc(allocator, out);
    try std.testing.expect(std.mem.indexOf(u8, text, "<https://example.org/g/node/1>") != null);
    try std.testing.expect(std.mem.indexOf(u8, text, "@prefix : <https://example.org/g/> .") != null);
    try std.testing.expect(std.mem.indexOf(u8, text, "{| :since 2001 |}") != null);
    allocator.free(text);

    try mustRun(allocator, &.{ "export", db, "--file=" ++ out, "--rdf-edge-properties=reify" });
    text = try readFileAlloc(allocator, out);
    try std.testing.expect(std.mem.indexOf(u8, text, "a rdf:Statement") != null);
    try std.testing.expect(std.mem.indexOf(u8, text, "{|") == null);
    allocator.free(text);

    const dropped = try runCli(allocator, &.{ "export", db, "--file=" ++ out, "--rdf-edge-properties=drop" });
    defer dropped.deinit(allocator);
    try std.testing.expect(dropped.succeeded());
    try std.testing.expect(std.mem.indexOf(u8, dropped.stderr, "edge property values left out (--rdf-edge-properties=drop): 1") != null);

    try mustRun(allocator, &.{ "export", db, "--file=" ++ out, "--labels=City" });
    text = try readFileAlloc(allocator, out);
    defer allocator.free(text);
    try std.testing.expect(std.mem.indexOf(u8, text, "\"Oslo\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, text, "\"Ann\"") == null);
}

test "lrdf: a statement that cannot be stored stops the import, or is left out whole" {
    const allocator = std.testing.allocator;
    const db = "/tmp/lrdf_errors.ltdb";
    defer deleteDatabaseFiles(db);
    const file_arg = "--file=" ++ data_dir ++ "error_statement.ttl";

    try freshDatabase(allocator, db);
    {
        const run = try runCli(allocator, &.{ "import", db, file_arg });
        defer run.deinit(allocator);
        try std.testing.expect(!run.succeeded());
        try std.testing.expect(std.mem.indexOf(u8, run.stderr, "line 4") != null);
        try std.testing.expect(std.mem.indexOf(u8, run.stderr, "does not fit in a 64-bit integer") != null);
        try std.testing.expect(std.mem.indexOf(u8, run.stderr, "Nothing was imported") != null);
    }
    try expectQuery(allocator, db, "MATCH (n:Resource) RETURN count(n) AS n", "\"n\":0");

    // Skipping leaves out the whole statement, its name as well as its count.
    {
        const run = try runCli(allocator, &.{ "import", db, file_arg, "--on-error=skip" });
        defer run.deinit(allocator);
        try std.testing.expect(run.succeeded());
        try std.testing.expect(std.mem.indexOf(u8, run.stderr, "Skipped line 4") != null);
    }
    try expectQuery(allocator, db, "MATCH (n:Resource) RETURN n.ex_name AS name ORDER BY name", "[{\"name\":\"A\"},{\"name\":\"C\"}]");

    // N-Triples goes on at the next line after a syntax error.
    try freshDatabase(allocator, db);
    {
        const run = try runCli(allocator, &.{ "import", db, "--file=" ++ data_dir ++ "error_syntax.nt" });
        defer run.deinit(allocator);
        try std.testing.expect(!run.succeeded());
        try std.testing.expect(std.mem.indexOf(u8, run.stderr, "line 2") != null);
    }
    try mustRun(allocator, &.{ "import", db, "--file=" ++ data_dir ++ "error_syntax.nt", "--on-error=skip" });
    try expectQuery(allocator, db, "MATCH (n:Resource) RETURN count(n) AS n", "\"n\":2");
}

test "lrdf: importing the same file twice adds nothing the second time" {
    const allocator = std.testing.allocator;
    const db = "/tmp/lrdf_twice.ltdb";
    defer deleteDatabaseFiles(db);
    try freshDatabase(allocator, db);
    const file_arg = "--file=" ++ data_dir ++ "mapping_repeated.ttl";
    try mustRun(allocator, &.{ "import", db, file_arg });
    try mustRun(allocator, &.{ "import", db, file_arg });
    try expectQuery(allocator, db, "MATCH (n:Resource) RETURN count(n) AS n", "\"n\":3");
    try expectQuery(allocator, db, "MATCH (:Resource)-[r]->() RETURN count(r) AS n", "\"n\":2");
    try expectQuery(allocator, db, "MATCH (s:Resource {iri: 'http://example.org/s'}) RETURN s.ex_alias AS v", "\"v\":[\"a\",\"b\",\"c\"]");
}
