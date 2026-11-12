//! RDF export: a graph as Turtle or N-Triples, one node at a time.
//!
//! Each node is one subject block: its types, its properties and its outgoing
//! edges, with edge properties as RDF-star annotations, reification, or left
//! out. Names come back to IRIs through the table import keeps in the
//! database; names that are not in it, and nodes without an IRI when --base
//! is given, are written under the base.

const std = @import("std");
const lattice = @import("lattice");
const term = @import("term.zig");
const chars = @import("chars.zig");
const out = @import("syntax_out.zig");
const vocabulary = @import("vocabulary.zig");
const values = @import("values.zig");
const options_mod = @import("options.zig");
const import_export = @import("../import_export.zig");

const Database = lattice.storage.database.Database;
const NodeId = lattice.core.types.NodeId;
const EdgeId = lattice.core.types.EdgeId;
const PropertyValue = lattice.core.types.PropertyValue;
const Literal = term.Literal;
const Writer = std.Io.Writer;
const Syntax = options_mod.Syntax;
const Options = options_mod.Options;
const Vocabulary = vocabulary.Vocabulary;

pub const default_base = "urn:lattice:";

/// Edge targets whose IRIs export remembers at a time.
const iri_cache_limit = 1 << 20;

pub const Stats = struct {
    triples: u64 = 0,
    nodes: u64 = 0,
    edges: u64 = 0,
    iri_nodes: u64 = 0,
    minted_nodes: u64 = 0,
    blank_nodes: u64 = 0,
    edges_with_properties: u64 = 0,
    edge_properties_dropped: u64 = 0,
    vectors: u64 = 0,
    vectors_omitted: u64 = 0,
    minted_names: u64 = 0,
};

/// A term to write: an IRI, a blank node label, a literal, or a collection
/// of literals.
const Obj = union(enum) {
    iri: []const u8,
    blank: []const u8,
    literal: Literal,
    list: []const Literal,
};

/// One predicate and its objects, each with its annotation, if any.
const Group = struct {
    predicate: []const u8,
    objects: std.ArrayList(Item) = .empty,
};

const Item = struct {
    object: Obj,
    annotation: []const Group = &.{},
};

const Exporter = struct {
    allocator: std.mem.Allocator,
    arena: std.heap.ArenaAllocator,
    db: *Database,
    w: *Writer,
    syntax: Syntax,
    options: Options,
    vocab: Vocabulary,
    base: []const u8,
    /// namespace -> prefix, longest first, for abbreviating in Turtle.
    prefixes: std.ArrayList([2][]const u8) = .empty,
    minted: std.StringHashMapUnmanaged(void) = .empty,
    stats: Stats = .{},
    /// The symbol of the `Resource` label, when the database has one.
    resource_id: ?u16 = null,
    /// Node -> its IRI (null: none), for edge targets.
    iri_cache: std.AutoHashMapUnmanaged(NodeId, ?[]const u8) = .empty,
    /// Numbers the blank nodes of collections spelled out in N-Triples.
    list_counter: u64 = 0,

    fn a(self: *Exporter) std.mem.Allocator {
        return self.arena.allocator();
    }

    // ------------------------------------------------------------------
    // Names and nodes as IRIs

    fn nameIri(self: *Exporter, name: []const u8) ![]const u8 {
        if (self.vocab.byName(name)) |rec| return rec.iri;
        if (!self.minted.contains(name)) {
            try self.minted.put(self.allocator, try self.allocator.dupe(u8, name), {});
            self.stats.minted_names += 1;
        }
        return std.fmt.allocPrint(self.a(), "{s}{s}", .{ self.base, name });
    }

    /// The IRI a node has, or null for one without.
    fn nodeIri(self: *Exporter, id: NodeId) !?[]const u8 {
        const resource_id = self.resource_id orelse return null;
        if (!try self.db.nodeHasLabelInTxn(null, id, resource_id)) return null;
        var value = (try self.db.getNodePropertyInTxn(null, id, vocabulary.iri_property)) orelse return null;
        defer value.deinit(self.db.allocator);
        if (value != .string_val) return null;
        return try self.a().dupe(u8, value.string_val);
    }

    /// The term for a node whose IRI, if any, is known.
    fn termOf(self: *Exporter, id: NodeId, iri: ?[]const u8) !Obj {
        if (iri) |i| return .{ .iri = i };
        if (self.options.base) |base| return .{ .iri = try std.fmt.allocPrint(self.a(), "{s}node/{d}", .{ base, id }) };
        return .{ .blank = try std.fmt.allocPrint(self.a(), "n{d}", .{id}) };
    }

    /// The term for the node at the end of an edge. Most nodes are the end
    /// of many edges, so their IRIs are remembered, up to a bound.
    fn targetTerm(self: *Exporter, id: NodeId) !Obj {
        if (self.iri_cache.get(id)) |cached| return self.termOf(id, cached);
        const iri = try self.nodeIri(id);
        if (self.iri_cache.count() >= iri_cache_limit) self.clearIriCache();
        const owned: ?[]const u8 = if (iri) |i| try self.allocator.dupe(u8, i) else null;
        errdefer if (owned) |o| self.allocator.free(o);
        try self.iri_cache.put(self.allocator, id, owned);
        return self.termOf(id, iri);
    }

    fn clearIriCache(self: *Exporter) void {
        var it = self.iri_cache.valueIterator();
        while (it.next()) |v| if (v.*) |iri| self.allocator.free(iri);
        self.iri_cache.clearRetainingCapacity();
    }

    // ------------------------------------------------------------------
    // Values as literals

    /// The objects a property value stands for: one per list element, except
    /// for a collection property, which is one collection.
    fn valueObjects(self: *Exporter, name: []const u8, value: PropertyValue, into: *std.ArrayList(Item)) !void {
        const rec = self.vocab.byName(name);
        if (value == .list_val) {
            if (rec != null and rec.?.list) {
                var members: std.ArrayList(Literal) = .empty;
                for (value.list_val) |item| {
                    if (try self.valueObject(rec, item)) |obj| {
                        if (obj == .literal) try members.append(self.a(), obj.literal);
                    }
                }
                try into.append(self.a(), .{ .object = .{ .list = members.items } });
                return;
            }
            for (value.list_val) |item| {
                if (item == .list_val) {
                    try into.append(self.a(), .{ .object = .{ .literal = try self.jsonLiteral(item) } });
                } else if (try self.valueObject(rec, item)) |obj| {
                    try into.append(self.a(), .{ .object = obj });
                }
            }
            return;
        }
        if (try self.valueObject(rec, value)) |obj| try into.append(self.a(), .{ .object = obj });
    }

    fn valueObject(self: *Exporter, rec: ?*vocabulary.Record, value: PropertyValue) !?Obj {
        const typeOf = struct {
            fn get(r: ?*vocabulary.Record, kind: vocabulary.Kind) ?[]const u8 {
                const record = r orelse return null;
                return record.typeOf(kind);
            }
        }.get;
        return switch (value) {
            .null_val => null,
            .bool_val => |b| .{ .literal = .{ .lexical = if (b) "true" else "false", .datatype = term.xsd_boolean } },
            .int_val => |i| .{ .literal = .{
                .lexical = try std.fmt.allocPrint(self.a(), "{d}", .{i}),
                .datatype = typeOf(rec, .int) orelse term.xsd_integer,
            } },
            .float_val => |f| .{ .literal = try self.floatLiteral(f, typeOf(rec, .float) orelse term.xsd_double) },
            .string_val => |s| blk: {
                if (rec) |r| {
                    if (r.lang.len > 0) break :blk .{ .literal = .{ .lexical = s, .datatype = term.rdf_lang_string, .lang = r.lang } };
                }
                const dt = typeOf(rec, .string) orelse term.xsd_string;
                if (dt.len > 0 and dt[0] == '@') break :blk .{ .literal = .{ .lexical = s, .datatype = term.rdf_lang_string, .lang = dt[1..] } };
                break :blk .{ .literal = .{ .lexical = s, .datatype = dt } };
            },
            .bytes_val => |bytes| blk: {
                const encoder = std.base64.standard.Encoder;
                const buf = try self.a().alloc(u8, encoder.calcSize(bytes.len));
                break :blk .{ .literal = .{ .lexical = encoder.encode(buf, bytes), .datatype = term.xsd_base64 } };
            },
            .vector_val => |v| .{ .literal = .{ .lexical = try self.vectorText(v), .datatype = term.lattice_vector } },
            .map_val => blk: {
                if (values.literalOfMap(value)) |lit| break :blk .{ .literal = lit };
                if (values.iriOfMap(value)) |iri| break :blk .{ .iri = iri };
                break :blk .{ .literal = try self.jsonLiteral(value) };
            },
            .list_val => .{ .literal = try self.jsonLiteral(value) },
        };
    }

    fn jsonLiteral(self: *Exporter, value: PropertyValue) !Literal {
        var buf: Writer.Allocating = .init(self.a());
        try import_export.shared.writeValue(&buf.writer, value);
        return .{ .lexical = buf.written(), .datatype = term.rdf_json };
    }

    fn vectorText(self: *Exporter, v: []const f32) ![]const u8 {
        var buf: Writer.Allocating = .init(self.a());
        try buf.writer.writeByte('[');
        for (v, 0..) |x, i| {
            if (i > 0) try buf.writer.writeByte(',');
            try buf.writer.print("{d}", .{x});
        }
        try buf.writer.writeByte(']');
        return buf.written();
    }

    fn floatLiteral(self: *Exporter, f: f64, datatype: []const u8) !Literal {
        if (std.math.isNan(f)) return .{ .lexical = "NaN", .datatype = if (values.isXsd(datatype, "float")) datatype else term.xsd_double };
        if (std.math.isInf(f)) return .{ .lexical = if (f > 0) "INF" else "-INF", .datatype = if (values.isXsd(datatype, "float")) datatype else term.xsd_double };
        if (values.isXsd(datatype, "decimal")) {
            var text = try values.formatDecimal(self.a(), f);
            if (std.mem.indexOfScalar(u8, text, '.') == null) text = try std.fmt.allocPrint(self.a(), "{s}.0", .{text});
            return .{ .lexical = text, .datatype = datatype };
        }
        var text = try std.fmt.allocPrint(self.a(), "{e}", .{f});
        // "1e0" reads back fine, but "1.0E0" is the form people expect.
        if (std.mem.indexOfScalar(u8, text, '.') == null) {
            const e = std.mem.indexOfScalar(u8, text, 'e').?;
            text = try std.fmt.allocPrint(self.a(), "{s}.0{s}", .{ text[0..e], text[e..] });
        }
        return .{ .lexical = text, .datatype = datatype };
    }

    // ------------------------------------------------------------------
    // Writing

    fn writeHeader(self: *Exporter) !void {
        var list: std.ArrayList([2][]const u8) = .empty;
        var taken = std.StringHashMapUnmanaged(void).empty;
        defer taken.deinit(self.allocator);
        for (self.vocab.prefix_of.keys(), self.vocab.prefix_of.values()) |ns, prefix| {
            try list.append(self.a(), .{ prefix, ns });
            try taken.put(self.allocator, prefix, {});
        }
        // The base, so that names written under it come back bare.
        if (!self.vocab.prefix_of.contains(self.base)) {
            const prefix: ?[]const u8 = if (!taken.contains("")) "" else if (!taken.contains("lattice")) "lattice" else null;
            if (prefix) |p| {
                try list.append(self.a(), .{ p, self.base });
                try taken.put(self.allocator, p, {});
            }
        }
        for ([_][2][]const u8{ .{ "rdf", term.rdf_ns }, .{ "xsd", term.xsd_ns }, .{ "lattice", term.lattice_ns } }) |wk| {
            if (taken.contains(wk[0]) or self.vocab.prefix_of.contains(wk[1])) continue;
            try list.append(self.a(), wk);
            try taken.put(self.allocator, wk[0], {});
        }
        std.mem.sort([2][]const u8, list.items, {}, struct {
            fn lt(_: void, x: [2][]const u8, y: [2][]const u8) bool {
                return std.mem.order(u8, x[0], y[0]) == .lt;
            }
        }.lt);
        if (self.syntax == .turtle) {
            for (list.items) |entry| {
                if (!chars.isPrefixName(entry[0])) continue;
                try self.w.print("@prefix {s}: ", .{entry[0]});
                try out.writeIri(self.w, entry[1]);
                try self.w.writeAll(" .\n");
                try self.prefixes.append(self.a(), .{ entry[1], entry[0] });
            }
            if (list.items.len > 0) try self.w.writeByte('\n');
            // Longest namespace first, so the most specific prefix wins.
            std.mem.sort([2][]const u8, self.prefixes.items, {}, struct {
                fn lt(_: void, x: [2][]const u8, y: [2][]const u8) bool {
                    return x[0].len > y[0].len;
                }
            }.lt);
        }
    }

    fn writeIri(self: *Exporter, iri: []const u8) !void {
        if (self.syntax == .turtle) {
            for (self.prefixes.items) |entry| {
                const ns = entry[0];
                if (iri.len >= ns.len and std.mem.startsWith(u8, iri, ns) and chars.isPlainLocalName(iri[ns.len..])) {
                    try self.w.print("{s}:{s}", .{ entry[1], iri[ns.len..] });
                    return;
                }
            }
        }
        try out.writeIri(self.w, iri);
    }

    fn writeLiteral(self: *Exporter, lit: Literal) !void {
        if (self.syntax == .turtle) {
            if (shorthand(lit)) return self.w.writeAll(lit.lexical);
            try self.w.writeByte('"');
            try out.writeStringBody(self.w, lit.lexical);
            try self.w.writeByte('"');
            if (lit.lang.len > 0) {
                try self.w.print("@{s}", .{lit.lang});
            } else if (!std.mem.eql(u8, lit.datatype, term.xsd_string)) {
                try self.w.writeAll("^^");
                try self.writeIri(lit.datatype);
            }
            return;
        }
        try out.writeLiteralNt(self.w, lit);
    }

    fn writeObj(self: *Exporter, obj: Obj) !void {
        switch (obj) {
            .iri => |iri| try self.writeIri(iri),
            .blank => |label| {
                try self.w.writeAll("_:");
                try self.w.writeAll(label);
            },
            .literal => |lit| try self.writeLiteral(lit),
            .list => |items| {
                // Only Turtle; N-Triples spells collections out first.
                try self.w.writeByte('(');
                for (items) |lit| {
                    try self.w.writeByte(' ');
                    try self.writeLiteral(lit);
                }
                try self.w.writeAll(" )");
            },
        }
    }

    fn writeTripleNt(self: *Exporter, s: Obj, p: []const u8, o: Obj) !void {
        try self.writeObj(s);
        try self.w.writeByte(' ');
        try self.writeIri(p);
        try self.w.writeByte(' ');
        try self.writeObj(o);
        try self.w.writeAll(" .\n");
        self.stats.triples += 1;
    }

    fn writeQuotedNt(self: *Exporter, s: Obj, p: []const u8, o: Obj) !void {
        try self.w.writeAll("<< ");
        try self.writeObj(s);
        try self.w.writeByte(' ');
        try self.writeIri(p);
        try self.w.writeByte(' ');
        try self.writeObj(o);
        try self.w.writeAll(" >>");
    }

    /// One subject and its groups, in the syntax being written.
    fn writeBlock(self: *Exporter, subject: Obj, groups: []const Group) !void {
        if (groups.len == 0) return;
        if (self.syntax == .ntriples) {
            for (groups) |g| {
                for (g.objects.items) |item| {
                    const obj = try self.spellOutList(item.object);
                    try self.writeTripleNt(subject, g.predicate, obj);
                    for (item.annotation) |ag| {
                        for (ag.objects.items) |ai| {
                            try self.writeQuotedNt(subject, g.predicate, obj);
                            try self.w.writeByte(' ');
                            try self.writeIri(ag.predicate);
                            try self.w.writeByte(' ');
                            try self.writeObj(ai.object);
                            try self.w.writeAll(" .\n");
                            self.stats.triples += 1;
                        }
                    }
                }
            }
            return;
        }
        try self.writeObj(subject);
        try self.writeGroups(groups, "\n    ");
        try self.w.writeAll(" .\n\n");
    }

    fn writeGroups(self: *Exporter, groups: []const Group, indent: []const u8) anyerror!void {
        for (groups, 0..) |g, gi| {
            if (gi > 0) try self.w.writeAll(" ;");
            try self.w.writeAll(indent);
            if (std.mem.eql(u8, g.predicate, term.rdf_type)) try self.w.writeByte('a') else try self.writeIri(g.predicate);
            for (g.objects.items, 0..) |item, oi| {
                try self.w.writeAll(if (oi > 0) ", " else " ");
                try self.writeObj(item.object);
                self.stats.triples += 1;
                if (item.annotation.len > 0) {
                    try self.w.writeAll(" {|");
                    for (item.annotation, 0..) |ag, ai| {
                        if (ai > 0) try self.w.writeAll(" ;");
                        try self.w.writeByte(' ');
                        try self.writeIri(ag.predicate);
                        for (ag.objects.items, 0..) |x, xi| {
                            try self.w.writeAll(if (xi > 0) ", " else " ");
                            try self.writeObj(x.object);
                            self.stats.triples += 1;
                        }
                    }
                    try self.w.writeAll(" |}");
                }
            }
        }
    }

    /// In N-Triples a collection becomes rdf:first/rdf:rest blank nodes,
    /// written here, and the triple points at its head.
    fn spellOutList(self: *Exporter, obj: Obj) !Obj {
        if (obj != .list) return obj;
        const items = obj.list;
        if (items.len == 0) return .{ .iri = term.rdf_nil };
        const labels = try self.a().alloc([]const u8, items.len);
        for (labels) |*l| {
            self.list_counter += 1;
            l.* = try std.fmt.allocPrint(self.a(), "l{d}", .{self.list_counter});
        }
        for (items, 0..) |lit, i| {
            try self.writeTripleNt(.{ .blank = labels[i] }, term.rdf_first, .{ .literal = lit });
            const rest: Obj = if (i + 1 < items.len) .{ .blank = labels[i + 1] } else .{ .iri = term.rdf_nil };
            try self.writeTripleNt(.{ .blank = labels[i] }, term.rdf_rest, rest);
        }
        return .{ .blank = labels[0] };
    }

    fn group(self: *Exporter, groups: *std.ArrayList(Group), predicate: []const u8) !*Group {
        for (groups.items) |*g| if (std.mem.eql(u8, g.predicate, predicate)) return g;
        try groups.append(self.a(), .{ .predicate = predicate });
        return &groups.items[groups.items.len - 1];
    }

    fn propertyGroups(self: *Exporter, props: []const Database.PropertyEntry, skip_iri: bool, groups: *std.ArrayList(Group)) !void {
        for (props) |p| {
            if (skip_iri and std.mem.eql(u8, p.key, vocabulary.iri_property)) continue;
            const predicate = try self.nameIri(p.key);
            const g = try self.group(groups, predicate);
            try self.valueObjects(p.key, p.value, &g.objects);
            if (g.objects.items.len == 0) _ = groups.pop();
        }
    }

    fn exportNode(self: *Exporter, id: NodeId, selection: *const import_export.shared.ExportSelection, by_query: bool) !void {
        _ = self.arena.reset(.retain_capacity);
        // Read from the node itself: getNodeLabels scans the whole label
        // index for every node.
        const labels = try self.db.getNodeLabelsInTxn(null, id);
        defer {
            for (labels) |l| self.db.allocator.free(l);
            self.db.allocator.free(labels);
        }
        for (labels) |l| {
            if (std.mem.eql(u8, l, vocabulary.name_label) or std.mem.eql(u8, l, vocabulary.namespace_label)) return;
        }
        const props = try self.db.getNodeProperties(id);
        defer self.db.freePropertyEntries(props);
        // The node's IRI: its iri property, when it is labelled Resource.
        var own_iri: ?[]const u8 = null;
        for (labels) |l| {
            if (!std.mem.eql(u8, l, vocabulary.resource_label)) continue;
            for (props) |p| {
                if (std.mem.eql(u8, p.key, vocabulary.iri_property) and p.value == .string_val) own_iri = p.value.string_val;
            }
        }
        const subject = try self.termOf(id, own_iri);
        if (own_iri != null) {
            self.stats.iri_nodes += 1;
        } else if (subject == .iri) {
            self.stats.minted_nodes += 1;
        } else {
            self.stats.blank_nodes += 1;
        }
        self.stats.nodes += 1;
        const is_resource = own_iri != null;

        var groups: std.ArrayList(Group) = .empty;
        // Types.
        for (labels) |l| {
            if (std.mem.eql(u8, l, vocabulary.resource_label)) continue;
            const g = try self.group(&groups, term.rdf_type);
            try g.objects.append(self.a(), .{ .object = .{ .iri = try self.nameIri(l) } });
        }
        // Properties.
        try self.propertyGroups(props, is_resource, &groups);
        // The node's vector.
        if (self.db.vector_storage != null) {
            if (try self.db.getNodeVectorInTxn(null, id)) |vector| {
                defer self.db.allocator.free(vector);
                if (self.options.vectors == .omit) {
                    self.stats.vectors_omitted += 1;
                } else {
                    const g = try self.group(&groups, term.lattice_vector);
                    try g.objects.append(self.a(), .{ .object = .{ .literal = .{ .lexical = try self.vectorText(vector), .datatype = term.lattice_vector } } });
                    self.stats.vectors += 1;
                }
            }
        }
        // Edges.
        var reified: std.ArrayList(struct { label: []const u8, predicate: []const u8, object: Obj, groups: []const Group }) = .empty;
        const edges = try self.db.getOutgoingEdges(id);
        defer self.db.freeEdgeInfos(edges);
        // Edge properties are written with the block, after the loop, so they
        // are freed only once it is written.
        var held: std.ArrayList([]Database.PropertyEntry) = .empty;
        defer for (held.items) |eprops| self.db.freePropertyEntries(eprops);
        for (edges) |e| {
            if (by_query and !import_export.shared.includesEdge(selection, e)) continue;
            const predicate = try self.nameIri(e.edge_type);
            const object = try self.targetTerm(e.target);
            self.stats.edges += 1;
            var annotation: std.ArrayList(Group) = .empty;
            const eprops = try self.db.getEdgeProperties(e.id);
            held.append(self.a(), eprops) catch |err| {
                self.db.freePropertyEntries(eprops);
                return err;
            };
            if (eprops.len > 0) {
                self.stats.edges_with_properties += 1;
                switch (self.options.edge_properties) {
                    .drop => {
                        for (eprops) |p| self.stats.edge_properties_dropped += if (p.value == .list_val) p.value.list_val.len else 1;
                    },
                    .star => try self.propertyGroups(eprops, false, &annotation),
                    .reify => {
                        var rgroups: std.ArrayList(Group) = .empty;
                        try self.propertyGroups(eprops, false, &rgroups);
                        try reified.append(self.a(), .{
                            .label = try std.fmt.allocPrint(self.a(), "r{d}", .{e.id}),
                            .predicate = predicate,
                            .object = object,
                            .groups = rgroups.items,
                        });
                    },
                }
            }
            const g = try self.group(&groups, predicate);
            try g.objects.append(self.a(), .{ .object = object, .annotation = annotation.items });
        }
        try self.writeBlock(subject, groups.items);

        for (reified.items) |r| {
            var rg: std.ArrayList(Group) = .empty;
            const t = try self.group(&rg, term.rdf_type);
            try t.objects.append(self.a(), .{ .object = .{ .iri = term.rdf_statement } });
            for ([_]struct { []const u8, Obj }{
                .{ term.rdf_subject, subject },
                .{ term.rdf_predicate, .{ .iri = r.predicate } },
                .{ term.rdf_object, r.object },
            }) |pair| {
                const g = try self.group(&rg, pair[0]);
                try g.objects.append(self.a(), .{ .object = pair[1] });
            }
            try rg.appendSlice(self.a(), r.groups);
            try self.writeBlock(.{ .blank = r.label }, rg.items);
        }
    }
};

/// Whether Turtle can write a literal bare: integers, decimals, doubles and
/// booleans in a form its grammar reads back as the same literal.
fn shorthand(lit: Literal) bool {
    if (lit.lang.len > 0) return false;
    const text = lit.lexical;
    if (std.mem.eql(u8, lit.datatype, term.xsd_boolean)) return std.mem.eql(u8, text, "true") or std.mem.eql(u8, text, "false");
    const is_int = std.mem.eql(u8, lit.datatype, term.xsd_integer);
    const is_dec = std.mem.eql(u8, lit.datatype, term.xsd_decimal);
    const is_dbl = std.mem.eql(u8, lit.datatype, term.xsd_double);
    if (!is_int and !is_dec and !is_dbl) return false;
    var body = text;
    if (body.len > 0 and (body[0] == '-' or body[0] == '+')) body = body[1..];
    if (body.len == 0 or !std.ascii.isDigit(body[0])) return false;
    var dots: usize = 0;
    var exps: usize = 0;
    for (body, 0..) |c, i| switch (c) {
        '0'...'9' => {},
        '.' => {
            dots += 1;
            if (i + 1 >= body.len or !std.ascii.isDigit(body[i + 1])) return false;
        },
        'e', 'E' => exps += 1,
        '+', '-' => if (i == 0 or (body[i - 1] != 'e' and body[i - 1] != 'E')) return false,
        else => return false,
    };
    if (is_int) return dots == 0 and exps == 0;
    if (is_dec) return dots == 1 and exps == 0;
    return exps == 1 and dots <= 1;
}

pub fn exportGraph(
    allocator: std.mem.Allocator,
    db: *Database,
    w: *Writer,
    syntax: Syntax,
    options: Options,
    scope: import_export.ExportScope,
) !Stats {
    var exporter = Exporter{
        .allocator = allocator,
        .arena = std.heap.ArenaAllocator.init(allocator),
        .db = db,
        .w = w,
        .syntax = syntax,
        .options = options,
        .vocab = Vocabulary.init(allocator),
        .base = options.base orelse default_base,
    };
    defer {
        var it = exporter.minted.keyIterator();
        while (it.next()) |k| allocator.free(k.*);
        exporter.minted.deinit(allocator);
        exporter.vocab.deinit();
        exporter.arena.deinit();
        exporter.clearIriCache();
        exporter.iri_cache.deinit(allocator);
    }
    try exporter.vocab.load(db);
    exporter.resource_id = db.symbol_table.lookup(vocabulary.resource_label) catch null;

    var header_arena = std.heap.ArenaAllocator.init(allocator);
    defer header_arena.deinit();
    // The header's strings must outlive the per-node arena resets.
    const saved = exporter.arena;
    exporter.arena = header_arena;
    try exporter.writeHeader();
    header_arena = exporter.arena;
    exporter.arena = saved;

    var selection = try import_export.shared.select(allocator, db, scope);
    defer import_export.shared.deselect(&selection);
    const by_query = scope.query_result != null;
    for (selection.node_ids) |id| try exporter.exportNode(id, &selection, by_query);
    return exporter.stats;
}

test "rdf export: shorthand only where Turtle reads the same literal back" {
    try std.testing.expect(shorthand(.{ .lexical = "-12", .datatype = term.xsd_integer }));
    try std.testing.expect(shorthand(.{ .lexical = "1.5", .datatype = term.xsd_decimal }));
    try std.testing.expect(!shorthand(.{ .lexical = "1.", .datatype = term.xsd_decimal }));
    try std.testing.expect(shorthand(.{ .lexical = "1.5e0", .datatype = term.xsd_double }));
    try std.testing.expect(!shorthand(.{ .lexical = "INF", .datatype = term.xsd_double }));
    try std.testing.expect(!shorthand(.{ .lexical = "1", .datatype = term.xsd_double }));
}
