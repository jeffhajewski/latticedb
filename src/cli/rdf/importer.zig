//! RDF import: triples from the parser into nodes, labels, properties and
//! edges, as the mapping in the user guide (book/src/guides/rdf.md) says.
//!
//! A record is one statement: a Turtle statement (a subject with all its
//! predicates and objects) or one N-Triples line. A statement goes in whole or
//! not at all. Statements are committed in batches of --batch-size; without
//! --on-error=skip the first failing statement stops the import and batches
//! committed before it are kept, with it each statement is its own
//! transaction and a failing one is left out and reported.

const std = @import("std");
const lattice = @import("lattice");
const compat = @import("compat");
const term = @import("term.zig");
const parser_mod = @import("parser.zig");
const vocabulary = @import("vocabulary.zig");
const values = @import("values.zig");
const options_mod = @import("options.zig");
const out = @import("syntax_out.zig");
const import_export = @import("../import_export.zig");

const Database = lattice.storage.database.Database;
const NodeId = lattice.core.types.NodeId;
const EdgeId = lattice.core.types.EdgeId;
const PropertyValue = lattice.core.types.PropertyValue;
const Transaction = lattice.Transaction;
const Term = term.Term;
const Triple = term.Triple;
const Literal = term.Literal;
const Pos = term.Pos;
const Vocabulary = vocabulary.Vocabulary;
const Record = vocabulary.Record;

pub const Options = options_mod.Options;

/// Past this many distinct classes (with the labels already in the
/// database), --rdf-types=auto keeps types as edges rather than labels.
pub const type_label_threshold = 1000;

/// How many IRIs of committed nodes to remember before starting over.
const iri_cache_limit = 200_000;

pub const Stats = struct {
    triples: u64 = 0,
    statements: u64 = 0,
    nodes_created: u64 = 0,
    edges_created: u64 = 0,
    duplicate_edges: u64 = 0,
    properties_set: u64 = 0,
    duplicate_values: u64 = 0,
    edge_properties_set: u64 = 0,
    labels_added: u64 = 0,
    map_values: u64 = 0,
    vectors_set: u64 = 0,
    vectors_omitted: u64 = 0,
    statements_failed: u64 = 0,
    triples_skipped: u64 = 0,
    names_created: u64 = 0,
    names_numbered: u64 = 0,
    prefixes_stored: u64 = 0,
    types_as_labels: bool = true,
    distinct_types: u64 = 0,
    index_declared: bool = false,
    fts_declared: u64 = 0,
};

pub const Error = error{ ImportFailed, IoError, SyntaxError, OutOfMemory, DatabaseError, VectorsNotEnabled };

/// Why one statement failed, in words.
const Problem = struct {
    buf: [512]u8 = undefined,
    len: usize = 0,

    fn set(self: *Problem, comptime fmt: []const u8, args: anytype) void {
        var w = std.Io.Writer.fixed(&self.buf);
        w.print(fmt, args) catch {
            @memcpy(self.buf[self.buf.len - 3 ..], "...");
            self.len = self.buf.len;
            return;
        };
        self.len = w.end;
    }

    fn text(self: *const Problem) []const u8 {
        return self.buf[0..self.len];
    }
};

pub const ImportSettings = struct {
    batch_size: u32 = 1000,
    on_error_skip: bool = false,
    diagnostics: ?*import_export.Diagnostics = null,
};

pub const Importer = struct {
    allocator: std.mem.Allocator,
    db: *Database,
    options: Options,
    settings: ImportSettings,
    vocab: Vocabulary,
    stats: Stats = .{},
    committed: Stats = .{},

    txn: Transaction = undefined,
    txn_active: bool = false,
    /// Triples in the open transaction. A batch is counted in triples, not
    /// statements, because one Turtle statement can hold any number of them
    /// and a transaction gets slower the more it holds.
    pending_triples: u64 = 0,
    /// IRIs whose nodes this transaction created: not yet visible to the index.
    txn_iris: std.StringHashMapUnmanaged(NodeId) = .empty,
    /// IRIs of committed nodes, bounded.
    cache: std.StringHashMapUnmanaged(NodeId) = .empty,
    /// Labelled blank nodes, for the whole import.
    blanks: std.StringHashMapUnmanaged(NodeId) = .empty,
    /// Labels added to `blanks` by the open transaction.
    blank_pending: std.ArrayList([]const u8) = .empty,
    /// Blank nodes the parser made up, which live for one statement.
    stmt_blanks: std.StringHashMapUnmanaged(NodeId) = .empty,
    stmt_arena: std.heap.ArenaAllocator,
    stmt_triples: u64 = 0,
    stmt_failed: bool = false,
    stmt_problem: Problem = .{},
    stmt_pos: Pos = .{},
    fallback_counter: u64 = 0,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, db: *Database, options: Options, settings: ImportSettings) Self {
        var s = settings;
        if (s.on_error_skip) s.batch_size = 1;
        if (s.batch_size == 0) s.batch_size = 1;
        return .{
            .allocator = allocator,
            .db = db,
            .options = options,
            .settings = s,
            .vocab = Vocabulary.init(allocator),
            .stmt_arena = std.heap.ArenaAllocator.init(allocator),
        };
    }

    pub fn deinit(self: *Self) void {
        if (self.txn_active) self.db.abortTransaction(&self.txn) catch {};
        freeKeys(self.allocator, &self.txn_iris);
        freeKeys(self.allocator, &self.cache);
        freeKeys(self.allocator, &self.blanks);
        self.blank_pending.deinit(self.allocator);
        self.stmt_blanks.deinit(self.allocator);
        self.stmt_arena.deinit();
        self.vocab.deinit();
    }

    fn freeKeys(allocator: std.mem.Allocator, map: *std.StringHashMapUnmanaged(NodeId)) void {
        var it = map.keyIterator();
        while (it.next()) |k| allocator.free(k.*);
        map.deinit(allocator);
    }

    /// Everything that must be in place before the first triple.
    pub fn prepare(self: *Self, types_as_labels: bool) !void {
        self.stats.types_as_labels = types_as_labels;
        const has_index = self.db.hasNodePropertyIndex(vocabulary.resource_label, vocabulary.iri_property) catch false;
        if (!has_index) {
            self.db.createNodePropertyIndex(vocabulary.resource_label, vocabulary.iri_property) catch |err| {
                if (err != error.AlreadyExists) return error.DatabaseError;
            };
            self.stats.index_declared = true;
        }
        try self.vocab.load(self.db);
    }

    // ------------------------------------------------------------------
    // Transactions

    fn ensureTxn(self: *Self) !?*Transaction {
        if (!self.txn_active) {
            self.txn = self.db.beginTransaction(.read_write) catch |err| switch (err) {
                error.TransactionsNotEnabled => return null,
                else => return error.DatabaseError,
            };
            self.txn_active = true;
        }
        return &self.txn;
    }

    fn currentTxn(self: *Self) ?*Transaction {
        return if (self.txn_active) &self.txn else null;
    }

    pub fn commit(self: *Self) !void {
        if (self.txn_active) {
            self.db.commitTransaction(&self.txn) catch return error.DatabaseError;
            self.txn_active = false;
        }
        self.pending_triples = 0;
        // What this transaction created is now visible to the index.
        var it = self.txn_iris.iterator();
        while (it.next()) |e| {
            if (self.cache.count() >= iri_cache_limit) {
                var keys = self.cache.keyIterator();
                while (keys.next()) |k| self.allocator.free(k.*);
                self.cache.clearRetainingCapacity();
            }
            const gop = try self.cache.getOrPut(self.allocator, e.key_ptr.*);
            if (gop.found_existing) self.allocator.free(e.key_ptr.*);
            gop.value_ptr.* = e.value_ptr.*;
        }
        self.txn_iris.clearRetainingCapacity();
        self.blank_pending.clearRetainingCapacity();
        self.syncVocabStats();
        self.committed = self.stats;
    }

    fn rollback(self: *Self) !void {
        if (self.txn_active) {
            self.db.abortTransaction(&self.txn) catch return error.DatabaseError;
            self.txn_active = false;
        }
        self.pending_triples = 0;
        var it = self.txn_iris.keyIterator();
        while (it.next()) |k| self.allocator.free(k.*);
        self.txn_iris.clearRetainingCapacity();
        for (self.blank_pending.items) |label| {
            if (self.blanks.fetchRemove(label)) |kv| self.allocator.free(kv.key);
        }
        self.blank_pending.clearRetainingCapacity();
        // Names made in the rolled-back transaction are gone from the database.
        try self.vocab.load(self.db);
        // Counts go back to what was committed, plus what failed.
        const failed = self.stats.statements_failed;
        const skipped = self.stats.triples_skipped;
        const triples = self.stats.triples;
        const statements = self.stats.statements;
        const flags = .{ self.stats.types_as_labels, self.stats.distinct_types, self.stats.index_declared };
        self.stats = self.committed;
        self.stats.statements_failed = failed;
        self.stats.triples_skipped = skipped;
        self.stats.triples = triples;
        self.stats.statements = statements;
        self.stats.types_as_labels = flags[0];
        self.stats.distinct_types = flags[1];
        self.stats.index_declared = flags[2];
    }

    fn syncVocabStats(self: *Self) void {
        self.stats.names_created = self.vocab.created_names;
        self.stats.names_numbered = self.vocab.numbered;
        self.stats.prefixes_stored = self.vocab.created_prefixes;
    }

    // ------------------------------------------------------------------
    // The parser's sink

    pub fn onPrefix(self: *Self, prefix: []const u8, namespace: []const u8) !void {
        if (self.stmt_failed) return;
        const txn = try self.ensureTxn();
        self.vocab.noteFilePrefix(self.db, txn, prefix, namespace) catch |err| {
            if (err == error.OutOfMemory) return err;
            self.failStatement(.{}, null, "storing prefix '{s}:' failed: {s}", .{ prefix, @errorName(err) });
        };
    }

    pub fn onTriple(self: *Self, t: Triple, at: Pos) !void {
        self.stats.triples += 1;
        self.stmt_triples += 1;
        if (self.stmt_failed) return;
        _ = try self.ensureTxn();
        self.handle(t, at) catch |err| switch (err) {
            error.OutOfMemory => return err,
            error.Refused => {},
            else => self.failStatement(at, t, "{s}", .{@errorName(err)}),
        };
    }

    pub fn onStatementEnd(self: *Self) !void {
        self.stats.statements += 1;
        defer {
            self.stmt_blanks.clearRetainingCapacity();
            _ = self.stmt_arena.reset(.retain_capacity);
            self.stmt_triples = 0;
            self.stmt_failed = false;
            self.stmt_problem = .{};
        }
        if (self.stmt_failed) {
            self.stats.statements_failed += 1;
            self.stats.triples_skipped += self.stmt_triples;
            try self.rollback();
            self.note(self.stmt_problem.text());
            if (!self.settings.on_error_skip) return error.ImportFailed;
            return;
        }
        if (!self.txn_active) return;
        // Committed at the end of the statement that fills the batch, so a
        // statement is never split across two transactions.
        self.pending_triples += @max(self.stmt_triples, 1);
        if (self.pending_triples >= self.settings.batch_size) try self.commit();
    }

    /// Mark the statement failed, describing where and why.
    fn failStatement(self: *Self, at: Pos, t: ?Triple, comptime fmt: []const u8, args: anytype) void {
        if (self.stmt_failed) return;
        self.stmt_failed = true;
        var buf: [400]u8 = undefined;
        var w = std.Io.Writer.fixed(&buf);
        if (t) |triple| out.writeTripleNt(&w, triple) catch {};
        var shown = std.mem.trimEnd(u8, w.buffered(), " .\n");
        if (shown.len > 200) shown = shown[0..200];
        var reason_buf: [300]u8 = undefined;
        const reason = std.fmt.bufPrint(&reason_buf, fmt, args) catch "(reason too long)";
        if (t != null) {
            self.stmt_problem.set("line {d}, column {d}: {s}: {s}", .{ at.line, at.column, shown, reason });
        } else {
            self.stmt_problem.set("line {d}: {s}", .{ at.line, reason });
        }
    }

    fn refuse(self: *Self, at: Pos, t: Triple, comptime fmt: []const u8, args: anytype) error{Refused} {
        self.failStatement(at, t, fmt, args);
        return error.Refused;
    }

    /// Note a failure for the command to report.
    pub fn note(self: *Self, message: []const u8) void {
        const diagnostics = self.settings.diagnostics orelse return;
        diagnostics.committed = .{ .nodes_imported = self.committed.nodes_created, .edges_imported = self.committed.edges_created };
        if (diagnostics.messages.items.len >= import_export.Diagnostics.max_messages) {
            diagnostics.omitted += 1;
            return;
        }
        const copy = diagnostics.allocator.dupe(u8, message) catch {
            diagnostics.omitted += 1;
            return;
        };
        diagnostics.messages.append(diagnostics.allocator, copy) catch {
            diagnostics.allocator.free(copy);
            diagnostics.omitted += 1;
        };
    }

    // ------------------------------------------------------------------
    // One triple

    fn handle(self: *Self, t: Triple, at: Pos) !void {
        if (t.subject == .triple) return self.annotate(t, at);
        if (t.subject == .literal or t.subject == .list) return self.refuse(at, t, "a literal cannot be a subject", .{});
        const subject = try self.resolve(t.subject);
        switch (t.object) {
            .literal => |lit| {
                if (std.mem.eql(u8, t.predicate, term.lattice_vector) and std.mem.eql(u8, lit.datatype, term.lattice_vector)) {
                    return self.setVector(subject, lit, t, at);
                }
                const rv = try self.routeLiteral(t.predicate, lit, t, at);
                try self.addValue(.{ .node = subject }, rv.name, rv.value);
                self.stats.properties_set += 1;
            },
            .iri, .blank => {
                if (std.mem.eql(u8, t.predicate, term.rdf_type) and t.object == .iri and self.stats.types_as_labels) {
                    const rec = try self.vocab.primaryFor(self.db, self.currentTxn(), t.object.iri);
                    const has = if (self.db.symbol_table.lookup(rec.name)) |label_id|
                        try self.db.nodeHasLabelInTxn(self.currentTxn(), subject, label_id)
                    else |_|
                        false;
                    if (!has) {
                        try self.db.addNodeLabel(self.currentTxn(), subject, rec.name);
                        self.stats.labels_added += 1;
                    }
                    return;
                }
                const object = try self.resolve(t.object);
                const rec = try self.vocab.primaryFor(self.db, self.currentTxn(), t.predicate);
                _ = try self.edge(subject, rec.name, object, true);
            },
            .list => |items| try self.collection(subject, t, items, at),
            .triple => return self.refuse(at, t, "a quoted triple cannot be stored as an object; only statements about a triple (as a subject) can", .{}),
        }
    }

    /// The node an IRI or blank node stands for, made when it has none.
    fn resolve(self: *Self, t: Term) !NodeId {
        const txn = self.currentTxn();
        switch (t) {
            .iri => |iri| {
                if (self.txn_iris.get(iri)) |id| return id;
                if (self.cache.get(iri)) |id| return id;
                // Probed outside the transaction: everything it made is in txn_iris.
                const found = try self.db.findNodesByLabelProperty(null, vocabulary.resource_label, vocabulary.iri_property, .{ .string_val = iri }, 1);
                defer self.db.allocator.free(found);
                if (found.len > 0) {
                    try self.cachePut(iri, found[0]);
                    return found[0];
                }
                const id = try self.db.createNode(txn, &.{vocabulary.resource_label});
                try self.db.setNodeProperty(txn, id, vocabulary.iri_property, .{ .string_val = iri });
                const key = try self.allocator.dupe(u8, iri);
                errdefer self.allocator.free(key);
                try self.txn_iris.put(self.allocator, key, id);
                self.stats.nodes_created += 1;
                return id;
            },
            .blank => |label| {
                if (term.isGeneratedBlank(label)) {
                    if (self.stmt_blanks.get(label)) |id| return id;
                    const id = try self.db.createNode(txn, &.{});
                    // The label lives in the parser's or this statement's arena.
                    try self.stmt_blanks.put(self.allocator, label, id);
                    self.stats.nodes_created += 1;
                    return id;
                }
                if (self.blanks.get(label)) |id| return id;
                const id = try self.db.createNode(txn, &.{});
                const key = try self.allocator.dupe(u8, label);
                errdefer self.allocator.free(key);
                try self.blanks.put(self.allocator, key, id);
                try self.blank_pending.append(self.allocator, key);
                self.stats.nodes_created += 1;
                return id;
            },
            else => unreachable,
        }
    }

    fn cachePut(self: *Self, iri: []const u8, id: NodeId) !void {
        if (self.cache.count() >= iri_cache_limit) {
            var keys = self.cache.keyIterator();
            while (keys.next()) |k| self.allocator.free(k.*);
            self.cache.clearRetainingCapacity();
        }
        const key = try self.allocator.dupe(u8, iri);
        errdefer self.allocator.free(key);
        try self.cache.put(self.allocator, key, id);
    }

    const Routed = struct { name: []const u8, value: PropertyValue };

    /// Which property a literal of `predicate` goes to, and as what value.
    fn routeLiteral(self: *Self, predicate: []const u8, lit: Literal, t: Triple, at: Pos) !Routed {
        const arena = self.stmt_arena.allocator();
        const txn = self.currentTxn();
        const conv = values.fromLiteral(arena, lit, self.options.decimal) catch |err| switch (err) {
            error.IntegerOutOfRange => return self.refuse(at, t, "{s} does not fit in a 64-bit integer", .{lit.lexical}),
            error.OutOfMemory => return error.OutOfMemory,
        };
        if (lit.lang.len > 0) {
            if (try self.vocab.findLangVariant(predicate, lit.lang)) |rec| return .{ .name = rec.name, .value = conv.value };
            const primary = try self.vocab.primaryFor(self.db, txn, predicate);
            if (std.ascii.eqlIgnoreCase(lit.lang, self.options.lang)) {
                if (primary.typeOf(.string)) |held| {
                    if (std.ascii.eqlIgnoreCase(held, conv.datatype_key)) return .{ .name = primary.name, .value = conv.value };
                } else {
                    try self.vocab.setType(self.db, txn, primary, .string, conv.datatype_key);
                    return .{ .name = primary.name, .value = conv.value };
                }
            }
            const rec = try self.vocab.langVariant(self.db, txn, predicate, lit.lang);
            return .{ .name = rec.name, .value = conv.value };
        }
        const rec = try self.vocab.primaryFor(self.db, txn, predicate);
        // Plain strings, where the property's strings carry a language tag,
        // have a property of their own rather than each being a map.
        if (conv.kind == .string and std.mem.eql(u8, conv.datatype_key, term.xsd_string)) {
            if (rec.typeOf(.string)) |held| {
                if (held.len > 0 and held[0] == '@') {
                    const plain = try self.vocab.plainVariant(self.db, txn, predicate);
                    return .{ .name = plain.name, .value = conv.value };
                }
            }
        }
        return .{ .name = rec.name, .value = try self.fitKind(rec, conv, lit) };
    }

    /// The value as stored in `rec`'s property: itself when its datatype is
    /// the one fixed for its kind (fixing it when none is), else the map form.
    fn fitKind(self: *Self, rec: *Record, conv: values.Converted, lit: Literal) !PropertyValue {
        const kind = conv.kind orelse return conv.value;
        if (rec.typeOf(kind)) |held| {
            if (std.mem.eql(u8, held, conv.datatype_key)) return conv.value;
            if (kind == .string and held.len > 0 and held[0] == '@' and std.ascii.eqlIgnoreCase(held, conv.datatype_key)) return conv.value;
            self.stats.map_values += 1;
            return values.literalMap(self.stmt_arena.allocator(), lit);
        }
        try self.vocab.setType(self.db, self.currentTxn(), rec, kind, conv.datatype_key);
        return conv.value;
    }

    const Owner = union(enum) { node: NodeId, edge: EdgeId };

    /// Add a value to a property: set it, or add it to the values already
    /// there unless it is one of them.
    fn addValue(self: *Self, owner: Owner, name: []const u8, value: PropertyValue) !void {
        const txn = self.currentTxn();
        var existing = switch (owner) {
            .node => |id| try self.db.getNodePropertyInTxn(txn, id, name),
            .edge => |id| try self.db.getEdgePropertyByIdInTxn(txn, id, name),
        } orelse return self.setValue(owner, name, value);
        defer existing.deinit(self.db.allocator);
        const arena = self.stmt_arena.allocator();
        const next: PropertyValue = switch (existing) {
            .list_val => |items| blk: {
                for (items) |item| if (values.valueEql(item, value)) {
                    self.stats.duplicate_values += 1;
                    return;
                };
                const list = try arena.alloc(PropertyValue, items.len + 1);
                @memcpy(list[0..items.len], items);
                list[items.len] = value;
                break :blk .{ .list_val = list };
            },
            else => blk: {
                if (values.valueEql(existing, value)) {
                    self.stats.duplicate_values += 1;
                    return;
                }
                const list = try arena.alloc(PropertyValue, 2);
                list[0] = existing;
                list[1] = value;
                break :blk .{ .list_val = list };
            },
        };
        try self.setValue(owner, name, next);
    }

    fn setValue(self: *Self, owner: Owner, name: []const u8, value: PropertyValue) !void {
        const txn = self.currentTxn();
        switch (owner) {
            .node => |id| try self.db.setNodeProperty(txn, id, name, value),
            .edge => |id| try self.db.setEdgePropertyById(txn, id, name, value),
        }
    }

    /// The edge `source -type-> target`, made unless it exists.
    fn edge(self: *Self, source: NodeId, type_name: []const u8, target: NodeId, count: bool) !EdgeId {
        const txn = self.currentTxn();
        const existing = try self.db.getOutgoingEdgesByTypeInTxn(txn, source, type_name, std.math.maxInt(usize));
        defer self.db.freeEdgeInfos(existing);
        for (existing) |e| {
            if (e.target == target) {
                if (count) self.stats.duplicate_edges += 1;
                return e.id;
            }
        }
        const id = try self.db.createEdgeAndGetId(txn, source, target, type_name);
        self.stats.edges_created += 1;
        return id;
    }

    /// `<< s p o >> q v`: a property on the edge s -p-> o, which is made
    /// when it does not exist yet.
    fn annotate(self: *Self, t: Triple, at: Pos) !void {
        const q = t.subject.triple;
        if (q.subject == .triple or q.object == .triple) return self.refuse(at, t, "nested quoted triples cannot be stored", .{});
        if (q.object == .literal or q.object == .list) {
            return self.refuse(at, t, "the quoted triple has a literal object, which is a property, and a property cannot carry properties", .{});
        }
        if (std.mem.eql(u8, q.predicate, term.rdf_type) and q.object == .iri and self.stats.types_as_labels) {
            return self.refuse(at, t, "rdf:type triples are labels here, which cannot carry properties; import with --rdf-types=edges to keep them", .{});
        }
        const source = try self.resolve(q.subject);
        const target = try self.resolve(q.object);
        const rec = try self.vocab.primaryFor(self.db, self.currentTxn(), q.predicate);
        const edge_id = try self.edge(source, rec.name, target, false);
        switch (t.object) {
            .literal => |lit| {
                const rv = try self.routeLiteral(t.predicate, lit, t, at);
                try self.addValue(.{ .edge = edge_id }, rv.name, rv.value);
            },
            .iri => |iri| {
                const prop = try self.vocab.primaryFor(self.db, self.currentTxn(), t.predicate);
                try self.addValue(.{ .edge = edge_id }, prop.name, try values.iriMap(self.stmt_arena.allocator(), iri));
            },
            else => return self.refuse(at, t, "a statement about a triple must have an IRI or a literal as its object", .{}),
        }
        self.stats.edge_properties_set += 1;
    }

    /// A collection of literals: one list property `<name>_list`, or, when
    /// the subject already has one for this predicate, the rdf:first/rdf:rest
    /// nodes it stands for.
    fn collection(self: *Self, subject: NodeId, t: Triple, items: []const Literal, at: Pos) !void {
        const txn = self.currentTxn();
        const rec = try self.vocab.listVariant(self.db, txn, t.predicate);
        const existing = try self.db.getNodePropertyInTxn(txn, subject, rec.name);
        if (existing) |value| {
            var v = value;
            v.deinit(self.db.allocator);
            return self.expandCollection(t, items, at);
        }
        const arena = self.stmt_arena.allocator();
        const list = try arena.alloc(PropertyValue, items.len);
        for (items, list) |lit, *slot| {
            const conv = values.fromLiteral(arena, lit, self.options.decimal) catch |err| switch (err) {
                error.IntegerOutOfRange => return self.refuse(at, t, "{s} does not fit in a 64-bit integer", .{lit.lexical}),
                error.OutOfMemory => return error.OutOfMemory,
            };
            slot.* = try self.fitKind(rec, conv, lit);
        }
        try self.db.setNodeProperty(txn, subject, rec.name, .{ .list_val = list });
        self.stats.properties_set += 1;
    }

    fn expandCollection(self: *Self, t: Triple, items: []const Literal, at: Pos) anyerror!void {
        const arena = self.stmt_arena.allocator();
        const labels = try arena.alloc([]const u8, items.len);
        for (labels) |*label| {
            self.fallback_counter += 1;
            label.* = try std.fmt.allocPrint(arena, "!c{d}", .{self.fallback_counter});
        }
        try self.handle(.{ .subject = t.subject, .predicate = t.predicate, .object = .{ .blank = labels[0] } }, at);
        for (items, 0..) |lit, i| {
            try self.handle(.{ .subject = .{ .blank = labels[i] }, .predicate = term.rdf_first, .object = .{ .literal = lit } }, at);
            const rest: Term = if (i + 1 < items.len) .{ .blank = labels[i + 1] } else .{ .iri = term.rdf_nil };
            try self.handle(.{ .subject = .{ .blank = labels[i] }, .predicate = term.rdf_rest, .object = rest }, at);
        }
    }

    fn setVector(self: *Self, subject: NodeId, lit: Literal, t: Triple, at: Pos) !void {
        if (self.options.vectors == .omit) {
            self.stats.vectors_omitted += 1;
            return;
        }
        const vector = values.parseVector(self.stmt_arena.allocator(), lit.lexical) catch {
            return self.refuse(at, t, "the vector is not a JSON array of numbers", .{});
        };
        const dims = import_export.vectorDimensions(self.db) orelse return self.refuse(
            at,
            t,
            "it is a vector, and the database was opened without vectors. Retry with --enable-vector --vector-dims={d}",
            .{vector.len},
        );
        if (vector.len != dims) return self.refuse(at, t, "the vector has {d} dimensions, and the database's have {d}", .{ vector.len, dims });
        try self.db.setNodeVectorInTxn(self.currentTxn(), subject, vector);
        self.stats.vectors_set += 1;
    }
};

// ----------------------------------------------------------------------
// Driving an import from a file

const FileReader = struct {
    file: compat.fs.File,

    fn read(ctx: *anyopaque, buf: []u8) anyerror!usize {
        const self: *FileReader = @ptrCast(@alignCast(ctx));
        // readSome reports the end of the file as EndOfStream, not as 0.
        return self.file.readSome(buf) catch |err| switch (err) {
            error.EndOfStream => 0,
            else => err,
        };
    }
};

/// Counts the distinct classes a file types things with, up to a limit.
const TypeCounter = struct {
    allocator: std.mem.Allocator,
    seen: std.StringHashMapUnmanaged(void) = .empty,
    limit: usize,

    pub fn onTriple(self: *TypeCounter, t: Triple, at: Pos) !void {
        _ = at;
        if (self.seen.count() > self.limit) return;
        if (!std.mem.eql(u8, t.predicate, term.rdf_type) or t.object != .iri) return;
        if (self.seen.contains(t.object.iri)) return;
        try self.seen.put(self.allocator, try self.allocator.dupe(u8, t.object.iri), {});
    }
    pub fn onPrefix(_: *TypeCounter, _: []const u8, _: []const u8) !void {}
    pub fn onStatementEnd(_: *TypeCounter) !void {}

    fn deinit(self: *TypeCounter) void {
        var it = self.seen.keyIterator();
        while (it.next()) |k| self.allocator.free(k.*);
        self.seen.deinit(self.allocator);
    }
};

/// The base a file's relative IRIs resolve against: its file:// URL.
pub fn fileBase(allocator: std.mem.Allocator, path: []const u8) ![]u8 {
    const absolute = std.Io.Dir.cwd().realPathFileAlloc(compat.io, path, allocator) catch return allocator.dupe(u8, "file:///");
    defer allocator.free(absolute);
    var out_buf: std.ArrayList(u8) = .empty;
    errdefer out_buf.deinit(allocator);
    try out_buf.appendSlice(allocator, "file://");
    for (absolute) |c| {
        if (std.ascii.isAlphanumeric(c) or std.mem.indexOfScalar(u8, "/-._~", c) != null) {
            try out_buf.append(allocator, c);
        } else try out_buf.print(allocator, "%{X:0>2}", .{c});
    }
    return out_buf.toOwnedSlice(allocator);
}

pub const SyntaxProblem = struct {
    diag: parser_mod.Diagnostic = .{},
    happened: bool = false,
};

/// Import one RDF file. On error.SyntaxError, `syntax_problem` says where;
/// on error.ImportFailed the diagnostics say which statement and why.
pub fn importFile(
    allocator: std.mem.Allocator,
    db: *Database,
    path: []const u8,
    syntax: parser_mod.Syntax,
    options: Options,
    settings: ImportSettings,
    syntax_problem: *SyntaxProblem,
) !Stats {
    const owned_base = if (options.base == null) try fileBase(allocator, path) else null;
    defer if (owned_base) |b| allocator.free(b);
    const base = options.base orelse owned_base.?;

    // Labels or edges for rdf:type.
    var types_as_labels = options.types != .edges;
    var distinct_types: u64 = 0;
    if (options.types == .auto) {
        const labels = try db.getAllLabels();
        const existing = labels.len;
        db.freeLabelInfos(labels);
        var counter = TypeCounter{ .allocator = allocator, .limit = type_label_threshold };
        defer counter.deinit();
        try parseWith(allocator, *TypeCounter, &counter, path, syntax, base, syntax_problem, settings.on_error_skip);
        syntax_problem.* = .{};
        distinct_types = counter.seen.count();
        types_as_labels = distinct_types + existing <= type_label_threshold;
    }

    var importer = Importer.init(allocator, db, options, settings);
    defer importer.deinit();
    try importer.prepare(types_as_labels);
    importer.stats.distinct_types = distinct_types;

    parseWith(allocator, *Importer, &importer, path, syntax, base, syntax_problem, settings.on_error_skip) catch |err| {
        if (err == error.SyntaxError) {
            // The statement it was in goes, as any failing statement does.
            importer.rollback() catch {};
            importer.stats.statements_failed += 1;
        } else if (err != error.ImportFailed) {
            importer.rollback() catch {};
        }
        return err;
    };
    try importer.commit();

    if (options.fts) |list| {
        var it = std.mem.splitScalar(u8, list, ',');
        while (it.next()) |raw| {
            const prop = std.mem.trim(u8, raw, " ");
            if (prop.len == 0) continue;
            if (db.hasNodeFtsIndex(vocabulary.resource_label, prop) catch false) continue;
            db.createNodeFtsIndex(vocabulary.resource_label, prop) catch return error.DatabaseError;
            importer.stats.fts_declared += 1;
        }
    }
    importer.syncVocabStats();
    return importer.stats;
}

fn parseWith(
    allocator: std.mem.Allocator,
    comptime Sink: type,
    sink: Sink,
    path: []const u8,
    syntax: parser_mod.Syntax,
    base: []const u8,
    syntax_problem: *SyntaxProblem,
    skip_bad_lines: bool,
) !void {
    const file = compat.fs.cwd().openFile(path, .{}) catch return error.IoError;
    defer file.close();
    var reader = FileReader{ .file = file };
    var parser = try parser_mod.Parser(Sink).init(allocator, sink, &reader, FileReader.read, .{
        .syntax = syntax,
        .base = base,
        .fold_literal_lists = true,
    });
    defer parser.deinit();
    while (true) {
        const more = parser.parseStatement() catch |err| {
            if (err != error.SyntaxError) return err;
            syntax_problem.* = .{ .diag = parser.diag, .happened = true };
            // N-Triples can go on at the next line; Turtle cannot know where
            // the next statement starts.
            if (skip_bad_lines and syntax == .ntriples) {
                if (Sink == *Importer) {
                    var msg_buf: [300]u8 = undefined;
                    const msg = std.fmt.bufPrint(&msg_buf, "line {d}, column {d}: {s} (at \"{f}\")", .{
                        parser.diag.pos.line, parser.diag.pos.column, parser.diag.messageText(), std.zig.fmtString(parser.diag.tokenText()),
                    }) catch "a syntax error";
                    sink.stats.statements_failed += 1;
                    try sink.rollback();
                    sink.note(msg);
                    sink.stmt_failed = false;
                    sink.stmt_triples = 0;
                }
                try parser.skipLine();
                continue;
            }
            return err;
        };
        if (!more) return;
    }
}
