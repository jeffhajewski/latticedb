//! The table between RDF IRIs and LatticeDB names, kept in the database.
//!
//! Every label, relationship type and property key that RDF import makes has
//! a `_RdfName` node saying which IRI it stands for and, for a property, the
//! datatype its values of each kind had. Every namespace has a
//! `_RdfNamespace` node with its prefix. Export reads only these.

const std = @import("std");
const lattice = @import("lattice");
const term = @import("term.zig");

const Database = lattice.storage.database.Database;
const NodeId = lattice.core.types.NodeId;
const PropertyValue = lattice.core.types.PropertyValue;
const Transaction = lattice.Transaction;

pub const name_label = "_RdfName";
pub const namespace_label = "_RdfNamespace";
pub const resource_label = "Resource";
pub const iri_property = "iri";

/// Names nothing in RDF may take, because the mapping uses them itself.
const reserved_names = [_][]const u8{ resource_label, iri_property, name_label, namespace_label };

const max_name_len = 200;

/// The kinds of value a property holds, each with its own datatype.
pub const Kind = enum { int, float, string, bytes };

const kind_keys = [_][]const u8{ "int_type", "float_type", "string_type", "bytes_type" };

pub const Record = struct {
    node: NodeId,
    name: []const u8,
    iri: []const u8,
    /// Set on a language variant (`skos_prefLabel_de`).
    lang: []const u8 = "",
    /// A collection-of-literals variant (`ex_coords_list`).
    list: bool = false,
    /// The plain strings of a property whose primary property holds strings
    /// in the primary language (`rdfs_label_plain`).
    plain: bool = false,
    types: [4]?[]const u8 = .{ null, null, null, null },

    pub fn typeOf(self: *const Record, kind: Kind) ?[]const u8 {
        return self.types[@intFromEnum(kind)];
    }
};

pub const well_known = [_][2][]const u8{
    .{ "rdf", term.rdf_ns },
    .{ "rdfs", term.rdfs_ns },
    .{ "owl", term.owl_ns },
    .{ "xsd", term.xsd_ns },
    .{ "skos", "http://www.w3.org/2004/02/skos/core#" },
    .{ "skosxl", "http://www.w3.org/2008/05/skos-xl#" },
    .{ "dc", "http://purl.org/dc/elements/1.1/" },
    .{ "dcterms", "http://purl.org/dc/terms/" },
    .{ "foaf", "http://xmlns.com/foaf/0.1/" },
    .{ "schema", "https://schema.org/" },
    .{ "schema", "http://schema.org/" },
    .{ "prov", "http://www.w3.org/ns/prov#" },
    .{ "sh", "http://www.w3.org/ns/shacl#" },
    .{ "void", "http://rdfs.org/ns/void#" },
    .{ "dcat", "http://www.w3.org/ns/dcat#" },
    .{ "vann", "http://purl.org/vocab/vann/" },
    .{ "time", "http://www.w3.org/2006/time#" },
    .{ "geo", "http://www.opengis.net/ont/geosparql#" },
    .{ "org", "http://www.w3.org/ns/org#" },
    .{ "lattice", term.lattice_ns },
};

pub const Vocabulary = struct {
    allocator: std.mem.Allocator,
    arena: std.heap.ArenaAllocator,
    /// name -> record
    names: std.StringHashMapUnmanaged(*Record) = .empty,
    /// IRI -> its primary record
    primary: std.StringHashMapUnmanaged(*Record) = .empty,
    /// "<iri> <lang>" or "<iri> ()" -> variant record
    variants: std.StringHashMapUnmanaged(*Record) = .empty,
    /// namespace -> prefix, and prefix -> namespace, as stored
    prefix_of: std.StringArrayHashMapUnmanaged([]const u8) = .empty,
    namespace_of: std.StringHashMapUnmanaged([]const u8) = .empty,
    /// The current file's own prefixes, which only suggest names.
    file_prefixes: std.StringArrayHashMapUnmanaged([]const u8) = .empty,
    /// How many names had to be numbered to stay distinct.
    numbered: u64 = 0,
    created_names: u64 = 0,
    created_prefixes: u64 = 0,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator) Self {
        return .{ .allocator = allocator, .arena = std.heap.ArenaAllocator.init(allocator) };
    }

    pub fn deinit(self: *Self) void {
        var it = self.file_prefixes.iterator();
        while (it.next()) |e| {
            self.allocator.free(e.key_ptr.*);
            self.allocator.free(e.value_ptr.*);
        }
        self.file_prefixes.deinit(self.allocator);
        self.arena.deinit();
    }

    fn a(self: *Self) std.mem.Allocator {
        return self.arena.allocator();
    }

    /// Read the table from the database, as committed.
    pub fn load(self: *Self, db: *Database) !void {
        // Everything but the file's own prefixes lives in the arena.
        self.arena.deinit();
        self.arena = std.heap.ArenaAllocator.init(self.allocator);
        self.names = .empty;
        self.primary = .empty;
        self.variants = .empty;
        self.prefix_of = .empty;
        self.namespace_of = .empty;

        const ns_nodes = try db.getNodesByLabel(namespace_label);
        defer db.allocator.free(ns_nodes);
        for (ns_nodes) |node| {
            const props = try db.getNodeProperties(node);
            defer db.freePropertyEntries(props);
            const prefix = stringProp(props, "prefix") orelse continue;
            const namespace = stringProp(props, "namespace") orelse continue;
            try self.putNamespace(try self.a().dupe(u8, prefix), try self.a().dupe(u8, namespace));
        }

        const name_nodes = try db.getNodesByLabel(name_label);
        defer db.allocator.free(name_nodes);
        for (name_nodes) |node| {
            const props = try db.getNodeProperties(node);
            defer db.freePropertyEntries(props);
            const name = stringProp(props, "name") orelse continue;
            const iri = stringProp(props, "iri") orelse continue;
            const rec = try self.a().create(Record);
            rec.* = .{ .node = node, .name = try self.a().dupe(u8, name), .iri = try self.a().dupe(u8, iri) };
            if (stringProp(props, "lang")) |lang| rec.lang = try self.a().dupe(u8, lang);
            for (props) |p| {
                if (std.mem.eql(u8, p.key, "list") and p.value == .bool_val) rec.list = p.value.bool_val;
                if (std.mem.eql(u8, p.key, "plain") and p.value == .bool_val) rec.plain = p.value.bool_val;
            }
            for (kind_keys, 0..) |key, i| {
                if (stringProp(props, key)) |t| rec.types[i] = try self.a().dupe(u8, t);
            }
            try self.index(rec);
        }
    }

    fn putNamespace(self: *Self, prefix: []const u8, namespace: []const u8) !void {
        try self.prefix_of.put(self.a(), namespace, prefix);
        try self.namespace_of.put(self.a(), prefix, namespace);
    }

    fn index(self: *Self, rec: *Record) !void {
        try self.names.put(self.a(), rec.name, rec);
        if (rec.plain) {
            try self.variants.put(self.a(), try self.plainKey(rec.iri), rec);
        } else if (rec.lang.len > 0 or rec.list) {
            try self.variants.put(self.a(), try self.variantKey(rec.iri, rec.lang, rec.list), rec);
        } else {
            try self.primary.put(self.a(), rec.iri, rec);
        }
    }

    fn variantKey(self: *Self, iri: []const u8, lang: []const u8, list: bool) ![]const u8 {
        if (list) return std.fmt.allocPrint(self.a(), "{s} ()", .{iri});
        const lower = try std.ascii.allocLowerString(self.a(), lang);
        return std.fmt.allocPrint(self.a(), "{s} {s}", .{ iri, lower });
    }

    fn plainKey(self: *Self, iri: []const u8) ![]const u8 {
        return std.fmt.allocPrint(self.a(), "{s} \"\"", .{iri});
    }

    /// A prefix the file being imported declares.
    pub fn noteFilePrefix(self: *Self, db: *Database, txn: ?*Transaction, prefix: []const u8, namespace: []const u8) !void {
        const gop = try self.file_prefixes.getOrPut(self.allocator, prefix);
        if (gop.found_existing) {
            self.allocator.free(gop.value_ptr.*);
        } else {
            gop.key_ptr.* = try self.allocator.dupe(u8, prefix);
        }
        gop.value_ptr.* = try self.allocator.dupe(u8, namespace);
        // Stored as the file says when neither side is taken yet.
        if (self.prefix_of.contains(namespace) or self.namespace_of.contains(prefix)) return;
        try self.bindNamespace(db, txn, prefix, namespace);
    }

    fn bindNamespace(self: *Self, db: *Database, txn: ?*Transaction, prefix: []const u8, namespace: []const u8) !void {
        const p = try self.a().dupe(u8, prefix);
        const n = try self.a().dupe(u8, namespace);
        const node = try db.createNode(txn, &.{namespace_label});
        try db.setNodeProperty(txn, node, "prefix", .{ .string_val = p });
        try db.setNodeProperty(txn, node, "namespace", .{ .string_val = n });
        try self.putNamespace(p, n);
        self.created_prefixes += 1;
    }

    /// The stored prefix of a namespace, binding one when it has none.
    fn prefixFor(self: *Self, db: *Database, txn: ?*Transaction, namespace: []const u8) ![]const u8 {
        if (self.prefix_of.get(namespace)) |p| return p;
        var candidates: std.ArrayList([]const u8) = .empty;
        var it = self.file_prefixes.iterator();
        while (it.next()) |e| if (std.mem.eql(u8, e.value_ptr.*, namespace)) try candidates.append(self.a(), e.key_ptr.*);
        for (well_known) |wk| if (std.mem.eql(u8, wk[1], namespace)) try candidates.append(self.a(), wk[0]);
        for (candidates.items) |c| {
            if (!self.namespace_of.contains(c)) {
                try self.bindNamespace(db, txn, c, namespace);
                return self.prefix_of.get(namespace).?;
            }
        }
        // Taken: number the first candidate, or make one up.
        const stem: []const u8 = if (candidates.items.len > 0 and candidates.items[0].len > 0) candidates.items[0] else "ns";
        var n: usize = if (std.mem.eql(u8, stem, "ns")) 1 else 2;
        while (true) : (n += 1) {
            const p = try std.fmt.allocPrint(self.a(), "{s}{d}", .{ stem, n });
            if (!self.namespace_of.contains(p)) {
                try self.bindNamespace(db, txn, p, namespace);
                return self.prefix_of.get(namespace).?;
            }
        }
    }

    /// Split an IRI into a namespace and a local part: the longest namespace
    /// known here that it starts with, else after its last '#', '/' or ':'.
    pub fn split(self: *Self, iri: []const u8) struct { namespace: []const u8, local: []const u8 } {
        var best: usize = 0;
        for (self.prefix_of.keys()) |ns| best = longer(iri, ns, best);
        for (self.file_prefixes.values()) |ns| best = longer(iri, ns, best);
        for (well_known) |wk| best = longer(iri, wk[1], best);
        if (best == 0) {
            const cut = if (std.mem.lastIndexOfScalar(u8, iri, '#')) |i|
                i + 1
            else if (std.mem.lastIndexOfScalar(u8, iri, '/')) |i|
                i + 1
            else if (std.mem.lastIndexOfScalar(u8, iri, ':')) |i|
                i + 1
            else
                0;
            best = cut;
        }
        return .{ .namespace = iri[0..best], .local = iri[best..] };
    }

    fn longer(iri: []const u8, ns: []const u8, best: usize) usize {
        if (ns.len > best and ns.len < iri.len and std.mem.startsWith(u8, iri, ns)) return ns.len;
        return best;
    }

    /// The record for an IRI's primary name, made when it has none.
    pub fn primaryFor(self: *Self, db: *Database, txn: ?*Transaction, iri: []const u8) !*Record {
        if (self.primary.get(iri)) |rec| return rec;
        const parts = self.split(iri);
        const prefix = try self.prefixFor(db, txn, parts.namespace);
        var buf: std.ArrayList(u8) = .empty;
        if (prefix.len > 0) {
            try appendSanitized(self.a(), &buf, prefix);
            if (parts.local.len > 0) try buf.append(self.a(), '_');
        }
        try appendSanitized(self.a(), &buf, parts.local);
        if (buf.items.len == 0) try buf.append(self.a(), '_');
        if (std.ascii.isDigit(buf.items[0])) try buf.insert(self.a(), 0, '_');
        return self.makeRecord(db, txn, buf.items, iri, "", false);
    }

    /// The record for strings of `lang` under `iri` that do not go to the
    /// primary property, made when missing.
    pub fn langVariant(self: *Self, db: *Database, txn: ?*Transaction, iri: []const u8, lang: []const u8) !*Record {
        if (self.variants.get(try self.variantKey(iri, lang, false))) |rec| return rec;
        const base = try self.primaryFor(db, txn, iri);
        var buf: std.ArrayList(u8) = .empty;
        try buf.appendSlice(self.a(), base.name);
        try buf.append(self.a(), '_');
        for (lang) |c| try buf.append(self.a(), if (c == '-') '_' else std.ascii.toLower(c));
        const rec = try self.makeRecord(db, txn, buf.items, iri, lang, false);
        try self.setType(db, txn, rec, .string, try std.fmt.allocPrint(self.a(), "@{s}", .{lang}));
        return rec;
    }

    pub fn findLangVariant(self: *Self, iri: []const u8, lang: []const u8) !?*Record {
        return self.variants.get(try self.variantKey(iri, lang, false));
    }

    pub fn listVariant(self: *Self, db: *Database, txn: ?*Transaction, iri: []const u8) !*Record {
        if (self.variants.get(try self.variantKey(iri, "", true))) |rec| return rec;
        const base = try self.primaryFor(db, txn, iri);
        const name = try std.fmt.allocPrint(self.a(), "{s}_list", .{base.name});
        return self.makeRecord(db, txn, name, iri, "", true);
    }

    /// The record for plain strings under `iri` when its primary property
    /// holds strings with a language tag, made when missing.
    pub fn plainVariant(self: *Self, db: *Database, txn: ?*Transaction, iri: []const u8) !*Record {
        if (self.variants.get(try self.plainKey(iri))) |rec| return rec;
        const base = try self.primaryFor(db, txn, iri);
        const name = try std.fmt.allocPrint(self.a(), "{s}_plain", .{base.name});
        const rec = try self.makeRecord(db, txn, name, iri, "", false);
        // Indexed as a primary by makeRecord; it is a variant. The keys are
        // the records' own copies: `iri` belongs to the caller.
        try self.primary.put(self.a(), base.iri, base);
        rec.plain = true;
        try db.setNodeProperty(txn, rec.node, "plain", .{ .bool_val = true });
        try self.variants.put(self.a(), try self.plainKey(base.iri), rec);
        try self.setType(db, txn, rec, .string, term.xsd_string);
        return rec;
    }

    fn makeRecord(self: *Self, db: *Database, txn: ?*Transaction, wanted: []const u8, iri: []const u8, lang: []const u8, list: bool) !*Record {
        var name: []const u8 = try self.a().dupe(u8, wanted[0..@min(wanted.len, max_name_len)]);
        if (self.taken(name)) {
            self.numbered += 1;
            var n: usize = 2;
            while (true) : (n += 1) {
                const candidate = try std.fmt.allocPrint(self.a(), "{s}_{d}", .{ name, n });
                if (!self.taken(candidate)) {
                    name = candidate;
                    break;
                }
            }
        }
        const node = try db.createNode(txn, &.{name_label});
        const rec = try self.a().create(Record);
        rec.* = .{ .node = node, .name = name, .iri = try self.a().dupe(u8, iri), .lang = try self.a().dupe(u8, lang), .list = list };
        try db.setNodeProperty(txn, node, "name", .{ .string_val = rec.name });
        try db.setNodeProperty(txn, node, "iri", .{ .string_val = rec.iri });
        if (lang.len > 0) try db.setNodeProperty(txn, node, "lang", .{ .string_val = rec.lang });
        if (list) try db.setNodeProperty(txn, node, "list", .{ .bool_val = true });
        try self.index(rec);
        self.created_names += 1;
        return rec;
    }

    fn taken(self: *Self, name: []const u8) bool {
        for (reserved_names) |r| if (std.mem.eql(u8, r, name)) return true;
        return self.names.contains(name);
    }

    /// Fix the datatype of a kind of value in a record.
    pub fn setType(self: *Self, db: *Database, txn: ?*Transaction, rec: *Record, kind: Kind, datatype: []const u8) !void {
        const owned = try self.a().dupe(u8, datatype);
        try db.setNodeProperty(txn, rec.node, kind_keys[@intFromEnum(kind)], .{ .string_val = owned });
        rec.types[@intFromEnum(kind)] = owned;
    }

    pub fn byName(self: *const Self, name: []const u8) ?*Record {
        return self.names.get(name);
    }
};

fn stringProp(props: []const Database.PropertyEntry, key: []const u8) ?[]const u8 {
    for (props) |p| {
        if (std.mem.eql(u8, p.key, key) and p.value == .string_val) return p.value.string_val;
    }
    return null;
}

/// Append `text` with every run of characters a Cypher name cannot hold
/// turned into one '_'.
fn appendSanitized(allocator: std.mem.Allocator, buf: *std.ArrayList(u8), text: []const u8) !void {
    var replaced = false;
    for (text) |c| {
        if (std.ascii.isAlphanumeric(c) or c == '_') {
            try buf.append(allocator, c);
            replaced = false;
        } else if (!replaced) {
            try buf.append(allocator, '_');
            replaced = true;
        }
    }
}
