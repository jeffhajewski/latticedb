//! Runs the W3C Turtle and N-Triples test suites through LatticeDB's RDF
//! parser:
//!
//!     zig build rdf-w3c -- <path to a checkout of github.com/w3c/rdf-tests>
//!
//! The suite is not part of this repository. Its manifests are read with the
//! parser under test; every positive syntax test must parse, every negative
//! one must fail, and every evaluation test must give a graph isomorphic to
//! its expected N-Triples. Nothing in the suite is executed.

const std = @import("std");
const compat = @import("compat");
const rdf = @import("rdf_syntax");

const parser = rdf.parser;
const term = rdf.term;

const mf = "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#";
const rdft = "http://www.w3.org/ns/rdftest#";

const Suite = struct {
    dir: []const u8,
    test_base: []const u8,
};

const suites = [_]Suite{
    .{ .dir = "rdf/rdf11/rdf-turtle", .test_base = "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-turtle/" },
    .{ .dir = "rdf/rdf11/rdf-n-triples", .test_base = "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-n-triples/" },
};

const Kind = enum { positive, negative, eval, other };

const Counts = struct { passed: usize = 0, failed: usize = 0 };

pub fn main(init: std.process.Init) !u8 {
    const allocator = init.gpa;
    var iter = try std.process.Args.Iterator.initAllocator(init.minimal.args, allocator);
    defer iter.deinit();
    _ = iter.next();
    const root = iter.next() orelse {
        std.debug.print("usage: rdf-w3c <path to rdf-tests checkout>\n", .{});
        return 2;
    };

    var total = Counts{};
    for (suites) |suite| {
        const counts = try runSuite(allocator, root, suite);
        total.passed += counts.passed;
        total.failed += counts.failed;
    }
    std.debug.print("\nall suites: {d} passed, {d} failed\n", .{ total.passed, total.failed });
    return if (total.failed == 0) 0 else 1;
}

fn readFile(allocator: std.mem.Allocator, path: []const u8) ![]u8 {
    const file = try compat.fs.cwd().openFile(path, .{});
    defer file.close();
    return file.readToEndAlloc(allocator, 64 * 1024 * 1024);
}

fn runSuite(allocator: std.mem.Allocator, root: []const u8, suite: Suite) !Counts {
    var arena_state = std.heap.ArenaAllocator.init(allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const dir = try std.fs.path.join(arena, &.{ root, suite.dir });
    const manifest_path = try std.fs.path.join(arena, &.{ dir, "manifest.ttl" });
    const manifest_text = readFile(arena, manifest_path) catch |err| {
        std.debug.print("{s}: cannot read ({s}); skipped\n", .{ manifest_path, @errorName(err) });
        return .{};
    };

    var manifest = parser.Collector.init(allocator);
    defer manifest.deinit();
    const manifest_base = try std.fmt.allocPrint(arena, "{s}manifest.ttl", .{suite.test_base});
    var diag: parser.Diagnostic = .{};
    parser.parseText(allocator, manifest_text, .{ .base = manifest_base }, &manifest, &diag) catch {
        std.debug.print("{s}: the manifest does not parse: {d}:{d}: {s}\n", .{ manifest_path, diag.pos.line, diag.pos.column, diag.messageText() });
        return .{ .failed = 1 };
    };

    var counts = Counts{};
    var by_kind = [_]Counts{.{}} ** 4;
    std.debug.print("== {s}\n", .{suite.dir});
    for (manifest.triples.items) |t| {
        if (!std.mem.eql(u8, t.predicate, term.rdf_type) or t.object != .iri) continue;
        const type_iri = t.object.iri;
        if (!std.mem.startsWith(u8, type_iri, rdft)) continue;
        const type_name = type_iri[rdft.len..];
        const kind: Kind = if (std.mem.endsWith(u8, type_name, "PositiveSyntax"))
            .positive
        else if (std.mem.endsWith(u8, type_name, "NegativeSyntax"))
            .negative
        else if (std.mem.endsWith(u8, type_name, "Eval"))
            .eval
        else
            .other;
        if (kind == .other) continue;
        const syntax: parser.Syntax = if (std.mem.startsWith(u8, type_name, "TestNTriples")) .ntriples else .turtle;

        const action = objectOf(manifest.triples.items, t.subject, mf ++ "action") orelse continue;
        const name = lastSegment(action);
        const action_path = try std.fs.path.join(arena, &.{ dir, name });
        const base = try std.fmt.allocPrint(arena, "{s}{s}", .{ suite.test_base, name });

        const reason = try runTest(allocator, arena, kind, syntax, action_path, base, objectOf(manifest.triples.items, t.subject, mf ++ "result"), dir);
        if (reason) |why| {
            counts.failed += 1;
            by_kind[@intFromEnum(kind)].failed += 1;
            std.debug.print("FAIL {s} ({s}): {s}\n", .{ name, @tagName(kind), why });
        } else {
            counts.passed += 1;
            by_kind[@intFromEnum(kind)].passed += 1;
        }
    }
    for ([_]Kind{ .positive, .negative, .eval }) |k| {
        const c = by_kind[@intFromEnum(k)];
        std.debug.print("  {s}: {d} passed, {d} failed\n", .{ @tagName(k), c.passed, c.failed });
    }
    return counts;
}

fn objectOf(triples: []const term.Triple, subject: term.Term, predicate: []const u8) ?[]const u8 {
    for (triples) |t| {
        if (std.mem.eql(u8, t.predicate, predicate) and t.subject.eql(subject) and t.object == .iri) return t.object.iri;
    }
    return null;
}

fn lastSegment(iri: []const u8) []const u8 {
    const slash = std.mem.lastIndexOfScalar(u8, iri, '/') orelse return iri;
    return iri[slash + 1 ..];
}

/// Null when the test passes, otherwise why it failed.
fn runTest(
    allocator: std.mem.Allocator,
    arena: std.mem.Allocator,
    kind: Kind,
    syntax: parser.Syntax,
    action_path: []const u8,
    base: []const u8,
    result: ?[]const u8,
    dir: []const u8,
) !?[]const u8 {
    const text = readFile(arena, action_path) catch |err| return try std.fmt.allocPrint(arena, "cannot read: {s}", .{@errorName(err)});
    var got = parser.Collector.init(allocator);
    defer got.deinit();
    var diag: parser.Diagnostic = .{};
    const parsed = parser.parseText(allocator, text, .{ .syntax = syntax, .base = base }, &got, &diag);
    switch (kind) {
        .negative => {
            if (parsed) |_| return "parsed, and should have been refused" else |_| return null;
        },
        .positive => {
            if (parsed) |_| return null else |_| return try std.fmt.allocPrint(arena, "refused at {d}:{d}: {s} (at '{s}')", .{ diag.pos.line, diag.pos.column, diag.messageText(), diag.tokenText() });
        },
        .eval => {
            parsed catch return try std.fmt.allocPrint(arena, "refused at {d}:{d}: {s} (at '{s}')", .{ diag.pos.line, diag.pos.column, diag.messageText(), diag.tokenText() });
            const result_iri = result orelse return "no expected result";
            const result_path = try std.fs.path.join(arena, &.{ dir, lastSegment(result_iri) });
            const expected_text = readFile(arena, result_path) catch |err| return try std.fmt.allocPrint(arena, "cannot read the result: {s}", .{@errorName(err)});
            var expected = parser.Collector.init(allocator);
            defer expected.deinit();
            parser.parseText(allocator, expected_text, .{ .syntax = .ntriples }, &expected, &diag) catch {
                return try std.fmt.allocPrint(arena, "the expected result does not parse at {d}:{d}: {s}", .{ diag.pos.line, diag.pos.column, diag.messageText() });
            };
            const cmp = try rdf.isomorphism.compare(allocator, got.triples.items, expected.triples.items, .{});
            defer rdf.isomorphism.freeResult(allocator, cmp);
            if (cmp.same) return null;
            return try std.fmt.allocPrint(arena, "graph differs ({d} triples, expected {d}); only in ours: {s}; only expected: {s}", .{
                got.triples.items.len,
                expected.triples.items.len,
                cmp.only_in_a orelse "-",
                cmp.only_in_b orelse "-",
            });
        },
        .other => return null,
    }
}
