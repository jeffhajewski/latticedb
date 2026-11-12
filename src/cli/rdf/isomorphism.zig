//! Whether two RDF graphs are the same graph: equal as sets of triples once
//! blank nodes are matched up (RDF 1.1 Concepts, 3.6, graph isomorphism).
//!
//! Blank nodes are coloured by the triples around them, the colours refined
//! until they stop splitting, and nodes of one colour are then matched by
//! backtracking, checking each complete matching against the other graph.

const std = @import("std");
const term = @import("term.zig");
const out = @import("syntax_out.zig");

const Term = term.Term;
const Triple = term.Triple;
const Literal = term.Literal;

pub const Options = struct {
    /// Compare numbers and booleans by value rather than by how they are
    /// written ("01"^^xsd:integer = "1"^^xsd:integer), as a store that keeps
    /// values rather than lexical forms will give them back.
    by_value: bool = false,
};

pub const Result = struct {
    same: bool,
    /// When not the same, one triple of each graph that the other lacks, for
    /// a message. Blank node labels are each graph's own.
    only_in_a: ?[]const u8 = null,
    only_in_b: ?[]const u8 = null,
};

/// A string-keyed array hash map that remembers its allocator.
fn StrMap(comptime V: type) type {
    return struct {
        inner: std.StringArrayHashMapUnmanaged(V) = .empty,
        a: std.mem.Allocator,
        fn init(a: std.mem.Allocator) @This() {
            return .{ .a = a };
        }
        fn put(self: *@This(), k: []const u8, v: V) !void {
            try self.inner.put(self.a, k, v);
        }
        fn get(self: *const @This(), k: []const u8) ?V {
            return self.inner.get(k);
        }
        fn keys(self: *const @This()) [][]const u8 {
            return self.inner.keys();
        }
        fn values(self: *const @This()) []V {
            return self.inner.values();
        }
    };
}

const Graph = struct {
    triples: []const Triple,
    blanks: [][]const u8,
};

pub fn compare(allocator: std.mem.Allocator, a: []const Triple, b: []const Triple, options: Options) !Result {
    var arena_state = std.heap.ArenaAllocator.init(allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const ga = try prepare(arena, a, options);
    const gb = try prepare(arena, b, options);

    if (ga.triples.len != gb.triples.len or ga.blanks.len != gb.blanks.len) {
        return mismatch(allocator, arena, ga, gb, null);
    }

    const colors_a = try refine(arena, ga);
    const colors_b = try refine(arena, gb);

    // The multiset of colours must agree.
    const sorted_a = try arena.dupe(u64, colors_a.values());
    const sorted_b = try arena.dupe(u64, colors_b.values());
    std.mem.sort(u64, sorted_a, {}, std.sort.asc(u64));
    std.mem.sort(u64, sorted_b, {}, std.sort.asc(u64));
    if (!std.mem.eql(u64, sorted_a, sorted_b)) return mismatch(allocator, arena, ga, gb, null);

    var target = std.StringHashMap(void).init(arena);
    for (gb.triples) |t| try target.put(try keyOf(arena, t, null), {});

    var search = Search{
        .arena = arena,
        .ga = ga,
        .colors_a = colors_a,
        .colors_b = colors_b,
        .target = &target,
        .mapping = std.StringHashMap([]const u8).init(arena),
        .used = std.StringHashMap(void).init(arena),
    };
    if (try search.assign(0)) return .{ .same = true };
    return mismatch(allocator, arena, ga, gb, &search.mapping);
}

fn mismatch(
    allocator: std.mem.Allocator,
    arena: std.mem.Allocator,
    ga: Graph,
    gb: Graph,
    mapping: ?*std.StringHashMap([]const u8),
) !Result {
    _ = mapping;
    // Report ground triples (no blank nodes) that differ, which is what a
    // person can act on; otherwise just say they differ.
    var set_b = std.StringHashMap(void).init(arena);
    for (gb.triples) |t| try set_b.put(try keyOf(arena, t, null), {});
    var set_a = std.StringHashMap(void).init(arena);
    for (ga.triples) |t| try set_a.put(try keyOf(arena, t, null), {});
    var result = Result{ .same = false };
    for (ga.triples) |t| {
        const k = try keyOf(arena, t, null);
        if (!set_b.contains(k)) {
            result.only_in_a = try allocator.dupe(u8, k);
            break;
        }
    }
    for (gb.triples) |t| {
        const k = try keyOf(arena, t, null);
        if (!set_a.contains(k)) {
            result.only_in_b = try allocator.dupe(u8, k);
            break;
        }
    }
    return result;
}

pub fn freeResult(allocator: std.mem.Allocator, result: Result) void {
    if (result.only_in_a) |s| allocator.free(s);
    if (result.only_in_b) |s| allocator.free(s);
}

/// Normalise literals, drop duplicate triples, list the blank nodes.
fn prepare(arena: std.mem.Allocator, triples: []const Triple, options: Options) !Graph {
    var seen = std.StringHashMap(void).init(arena);
    var list: std.ArrayList(Triple) = .empty;
    var blanks = StrMap(void).init(arena);
    for (triples) |t| {
        const n = try normalizeTriple(arena, t, options);
        const k = try keyOf(arena, n, null);
        if ((try seen.getOrPut(k)).found_existing) continue;
        try list.append(arena, n);
        try collectBlanks(n, &blanks);
    }
    return .{ .triples = list.items, .blanks = blanks.keys() };
}

fn collectBlanks(t: Triple, blanks: *StrMap(void)) !void {
    for ([_]Term{ t.subject, t.object }) |x| switch (x) {
        .blank => |l| try blanks.put(l, {}),
        .triple => |q| try collectBlanks(q.*, blanks),
        else => {},
    };
}

fn normalizeTriple(arena: std.mem.Allocator, t: Triple, options: Options) !Triple {
    return .{
        .subject = try normalizeTerm(arena, t.subject, options),
        .predicate = t.predicate,
        .object = try normalizeTerm(arena, t.object, options),
    };
}

fn normalizeTerm(arena: std.mem.Allocator, t: Term, options: Options) std.mem.Allocator.Error!Term {
    switch (t) {
        .literal => |l| {
            var lit = l;
            if (lit.lang.len > 0) lit.lang = try std.ascii.allocLowerString(arena, lit.lang);
            if (options.by_value) lit.lexical = try canonicalLexical(arena, lit);
            return .{ .literal = lit };
        },
        .triple => |q| {
            const p = try arena.create(Triple);
            p.* = try normalizeTriple(arena, q.*, options);
            return .{ .triple = p };
        },
        else => return t,
    }
}

const integer_types = [_][]const u8{
    "integer",            "int",             "long",            "short",              "byte",
    "nonNegativeInteger", "positiveInteger", "negativeInteger", "nonPositiveInteger", "unsignedLong",
    "unsignedInt",        "unsignedShort",   "unsignedByte",
};

fn isXsd(datatype: []const u8, local: []const u8) bool {
    return datatype.len == term.xsd_ns.len + local.len and std.mem.startsWith(u8, datatype, term.xsd_ns) and
        std.mem.eql(u8, datatype[term.xsd_ns.len..], local);
}

/// A canonical spelling of a numeric or boolean literal's value, or the
/// lexical form unchanged when it is not one of those or does not parse.
fn canonicalLexical(arena: std.mem.Allocator, lit: Literal) ![]const u8 {
    for (integer_types) |local| {
        if (isXsd(lit.datatype, local)) {
            const v = std.fmt.parseInt(i128, lit.lexical, 10) catch return lit.lexical;
            return std.fmt.allocPrint(arena, "{d}", .{v});
        }
    }
    if (isXsd(lit.datatype, "decimal") or isXsd(lit.datatype, "double") or isXsd(lit.datatype, "float")) {
        const v = parseXsdFloat(lit.lexical) orelse return lit.lexical;
        return std.fmt.allocPrint(arena, "{e}", .{v});
    }
    if (isXsd(lit.datatype, "boolean")) {
        if (std.mem.eql(u8, lit.lexical, "1")) return "true";
        if (std.mem.eql(u8, lit.lexical, "0")) return "false";
    }
    return lit.lexical;
}

pub fn parseXsdFloat(text: []const u8) ?f64 {
    if (std.mem.eql(u8, text, "INF") or std.mem.eql(u8, text, "+INF")) return std.math.inf(f64);
    if (std.mem.eql(u8, text, "-INF")) return -std.math.inf(f64);
    if (std.mem.eql(u8, text, "NaN")) return std.math.nan(f64);
    if (text.len == 0) return null;
    for (text) |c| switch (c) {
        '0'...'9', '+', '-', '.', 'e', 'E' => {},
        else => return null,
    };
    return std.fmt.parseFloat(f64, text) catch null;
}

/// A triple as text, blank nodes renamed through `mapping` when given.
fn keyOf(arena: std.mem.Allocator, t: Triple, mapping: ?*const std.StringHashMap([]const u8)) ![]const u8 {
    var buf: std.Io.Writer.Allocating = .init(arena);
    try writeKey(&buf.writer, t, mapping);
    return buf.written();
}

fn writeKey(w: *std.Io.Writer, t: Triple, mapping: ?*const std.StringHashMap([]const u8)) std.Io.Writer.Error!void {
    try writeKeyTerm(w, t.subject, mapping);
    try w.writeByte(' ');
    try out.writeIri(w, t.predicate);
    try w.writeByte(' ');
    try writeKeyTerm(w, t.object, mapping);
}

fn writeKeyTerm(w: *std.Io.Writer, t: Term, mapping: ?*const std.StringHashMap([]const u8)) std.Io.Writer.Error!void {
    switch (t) {
        .blank => |l| {
            const name = if (mapping) |m| m.get(l) orelse l else l;
            try w.writeAll("_:");
            try w.writeAll(name);
        },
        .triple => |q| {
            try w.writeAll("<< ");
            try writeKey(w, q.*, mapping);
            try w.writeAll(" >>");
        },
        else => try out.writeTermNt(w, t),
    }
}

/// Colour refinement: a blank node's colour is a hash of the triples it is
/// in, written with itself as "@", other blank nodes as their colours, and
/// everything else as itself. Repeated until the number of colours settles.
fn refine(arena: std.mem.Allocator, g: Graph) !StrMap(u64) {
    var colors = StrMap(u64).init(arena);
    for (g.blanks) |b| try colors.put(b, 0);
    var distinct: usize = 1;
    var round: usize = 0;
    while (round < g.blanks.len + 1) : (round += 1) {
        var next = StrMap(u64).init(arena);
        for (g.blanks) |b| {
            var sigs: std.ArrayList(u64) = .empty;
            for (g.triples) |t| {
                if (!mentions(t, b)) continue;
                var h = std.hash.Wyhash.init(0);
                hashTriple(&h, t, b, &colors);
                try sigs.append(arena, h.final());
            }
            std.mem.sort(u64, sigs.items, {}, std.sort.asc(u64));
            var h = std.hash.Wyhash.init(colors.get(b).?);
            h.update(std.mem.sliceAsBytes(sigs.items));
            try next.put(b, h.final());
        }
        var unique = std.AutoHashMap(u64, void).init(arena);
        for (next.values()) |c| try unique.put(c, {});
        colors = next;
        if (unique.count() <= distinct) break;
        distinct = unique.count();
    }
    return colors;
}

fn mentions(t: Triple, b: []const u8) bool {
    for ([_]Term{ t.subject, t.object }) |x| switch (x) {
        .blank => |l| if (std.mem.eql(u8, l, b)) return true,
        .triple => |q| if (mentions(q.*, b)) return true,
        else => {},
    };
    return false;
}

fn hashTriple(h: *std.hash.Wyhash, t: Triple, self: []const u8, colors: *const StrMap(u64)) void {
    hashTerm(h, t.subject, self, colors);
    h.update("|");
    h.update(t.predicate);
    h.update("|");
    hashTerm(h, t.object, self, colors);
}

fn hashTerm(h: *std.hash.Wyhash, t: Term, self: []const u8, colors: *const StrMap(u64)) void {
    switch (t) {
        .iri => |s| {
            h.update("I");
            h.update(s);
        },
        .blank => |l| {
            if (std.mem.eql(u8, l, self)) {
                h.update("@");
            } else {
                h.update("B");
                h.update(std.mem.asBytes(&colors.get(l).?));
            }
        },
        .literal => |l| {
            h.update("L");
            h.update(l.lexical);
            h.update("^");
            h.update(l.datatype);
            h.update("@");
            h.update(l.lang);
        },
        .triple => |q| {
            h.update("<<");
            hashTriple(h, q.*, self, colors);
            h.update(">>");
        },
        .list => {},
    }
}

const Search = struct {
    arena: std.mem.Allocator,
    ga: Graph,
    colors_a: StrMap(u64),
    colors_b: StrMap(u64),
    target: *std.StringHashMap(void),
    mapping: std.StringHashMap([]const u8),
    used: std.StringHashMap(void),
    attempts: usize = 0,

    const max_attempts = 200_000;

    fn assign(self: *Search, index: usize) !bool {
        if (index == self.ga.blanks.len) return self.check();
        self.attempts += 1;
        if (self.attempts > max_attempts) return false;
        const b = self.ga.blanks[index];
        const color = self.colors_a.get(b).?;
        for (self.colors_b.keys(), self.colors_b.values()) |candidate, c| {
            if (c != color or self.used.contains(candidate)) continue;
            try self.mapping.put(b, candidate);
            try self.used.put(candidate, {});
            if (try self.assign(index + 1)) return true;
            _ = self.used.remove(candidate);
            _ = self.mapping.remove(b);
        }
        return false;
    }

    fn check(self: *Search) !bool {
        for (self.ga.triples) |t| {
            const k = try keyOf(self.arena, t, &self.mapping);
            if (!self.target.contains(k)) return false;
        }
        return true;
    }
};

test "rdf isomorphism: blank nodes are matched, not compared by label" {
    const parser = @import("parser.zig");
    const allocator = std.testing.allocator;
    var a = parser.Collector.init(allocator);
    defer a.deinit();
    var b = parser.Collector.init(allocator);
    defer b.deinit();
    try parser.parseText(allocator, "_:x <http://p> _:y .\n_:y <http://p> _:x .\n_:y <http://q> \"1\" .", .{ .syntax = .ntriples }, &a, null);
    try parser.parseText(allocator, "_:m <http://p> _:n .\n_:n <http://p> _:m .\n_:m <http://q> \"1\" .", .{ .syntax = .ntriples }, &b, null);
    const same = try compare(allocator, a.triples.items, b.triples.items, .{});
    try std.testing.expect(same.same);

    var c = parser.Collector.init(allocator);
    defer c.deinit();
    try parser.parseText(allocator, "_:m <http://p> _:n .\n_:n <http://p> _:m .\n_:m <http://q> \"2\" .", .{ .syntax = .ntriples }, &c, null);
    const differs = try compare(allocator, a.triples.items, c.triples.items, .{});
    defer freeResult(allocator, differs);
    try std.testing.expect(!differs.same);

    var d = parser.Collector.init(allocator);
    defer d.deinit();
    var e = parser.Collector.init(allocator);
    defer e.deinit();
    try parser.parseText(allocator, "<http://s> <http://p> \"01\"^^<http://www.w3.org/2001/XMLSchema#integer> .", .{ .syntax = .ntriples }, &d, null);
    try parser.parseText(allocator, "<http://s> <http://p> \"1\"^^<http://www.w3.org/2001/XMLSchema#integer> .", .{ .syntax = .ntriples }, &e, null);
    const lexical = try compare(allocator, d.triples.items, e.triples.items, .{});
    defer freeResult(allocator, lexical);
    try std.testing.expect(!lexical.same);
    try std.testing.expect((try compare(allocator, d.triples.items, e.triples.items, .{ .by_value = true })).same);
}
