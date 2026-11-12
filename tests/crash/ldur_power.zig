//! Power-loss simulation for the whole engine.
//!
//! A workload of committed transactions runs against a `FaultVfs`, which
//! records every write, truncation, sync and directory change the engine
//! makes. Then, for each point at which the power is cut (every sync, just
//! before and just after it, and seeded random points in between), the
//! recorded changes up to that point are replayed into a disk model, a state
//! the disk could hold after the cut is chosen, and the database is opened on
//! it. The open must succeed and the database must hold exactly the state
//! after some prefix of the transactions:
//!
//! - every transaction whose commit had returned before the cut, in the
//!   default sync mode; in `.normal` mode every one that returned before the
//!   log was last synced; without a log, every one that returned before the
//!   last `sync()` or close returned;
//! - no transaction in part;
//! - trees that can be walked, counts that agree, edges whose ends exist, a
//!   label index, a property index, a full-text index and a vector index that
//!   agree with the graph.
//!
//! Some cuts are followed by a second cut during the recovery that the open
//! runs, and the result must be what the first recovery would have given.
//!
//! Everything is driven by one seed. A failure prints the seed, the cut and
//! the style of the cut, which `Options.only_cut` replays on its own.

const std = @import("std");
const lattice = @import("lattice");

const Allocator = std.mem.Allocator;
const Database = lattice.storage.database.Database;
const DatabaseConfig = lattice.storage.database.DatabaseConfig;
const fault = lattice.storage.fault_vfs;
const FaultVfs = fault.FaultVfs;
const Image = fault.Image;
const Op = fault.Op;
const NodeId = lattice.core.types.NodeId;
const EdgeId = lattice.core.types.EdgeId;
const PropertyValue = lattice.core.types.PropertyValue;

pub const DB_PATH = "db";
pub const WAL_PATH = "db-wal";

pub const Options = struct {
    seed: u64,
    txns: usize = 120,
    sync_mode: DatabaseConfig.SyncMode = .full,
    vectors: bool = false,
    fts: bool = false,
    /// Property values large enough to need overflow pages and to spread a
    /// transaction over several log frames.
    big_values: bool = false,
    pool_pages: usize = 24,
    max_wal_frames: u64 = 24,
    /// Close and reopen the database this many times during the workload.
    reopens: usize = 1,
    /// Remove the log while the database is closed for a reopen, as when a
    /// backup is put in place without one.
    drop_log_on_reopen: bool = false,
    /// Open the database without a log (`enable_wal = false`). Commits then
    /// reach the file only when the pages are flushed, and a cut may lose
    /// any of them since the last `sync()` or close, but never part of one.
    no_log: bool = false,
    /// Cuts at random points, besides the ones at each sync.
    random_cuts: usize = 60,
    /// Every this many cuts, also cut the power during the recovery.
    second_cut_every: usize = 5,
    /// Run only this cut, by its position in the trace.
    only_cut: ?usize = null,
    /// Print each cut as it runs.
    verbose: bool = false,
    /// Carry on past a failing cut, and fail at the end with a count.
    keep_going: bool = false,
};

const DIMS = 4;

// ============================================================================
// The expected state
// ============================================================================

const MNode = struct {
    a: bool = false,
    b: bool = false,
    doc: bool = false,
    n: ?i64 = null,
    s: ?[]u8 = null,
    text: ?[]u8 = null,
    vector: ?[DIMS]f32 = null,
};

const MEdge = struct {
    source: NodeId,
    target: NodeId,
    kind: u1,
    w: ?i64 = null,
};

const Model = struct {
    allocator: Allocator,
    nodes: std.AutoArrayHashMapUnmanaged(NodeId, MNode) = .empty,
    edges: std.AutoArrayHashMapUnmanaged(EdgeId, MEdge) = .empty,
    /// What a transaction that will be rolled back has deleted so far. Its
    /// changes stay out of the model, so without these a later change in it
    /// could pick a node or edge it already deleted.
    gone_nodes: std.AutoHashMapUnmanaged(NodeId, void) = .empty,
    gone_edges: std.AutoHashMapUnmanaged(EdgeId, void) = .empty,

    fn deinit(self: *Model) void {
        for (self.nodes.values()) |node| freeNode(self.allocator, node);
        self.nodes.deinit(self.allocator);
        self.edges.deinit(self.allocator);
        self.gone_nodes.deinit(self.allocator);
        self.gone_edges.deinit(self.allocator);
    }

    fn freeNode(allocator: Allocator, node: MNode) void {
        if (node.s) |s| allocator.free(s);
        if (node.text) |t| allocator.free(t);
    }

    fn digest(self: *Model) u64 {
        var d = Digest.init();
        const ids = self.allocator.dupe(NodeId, self.nodes.keys()) catch @panic("oom");
        defer self.allocator.free(ids);
        std.mem.sort(NodeId, ids, {}, std.sort.asc(NodeId));
        for (ids) |id| {
            const node = self.nodes.get(id).?;
            d.node(id);
            if (node.a) d.label("A");
            if (node.b) d.label("B");
            if (node.doc) d.label("Doc");
            // Keys in name order: n, s, text.
            if (node.n) |n| d.prop("n", .{ .int_val = n });
            if (node.s) |s| d.prop("s", .{ .string_val = s });
            if (node.text) |t| d.prop("text", .{ .string_val = t });
            if (node.vector) |v| d.vector(&v);
        }
        const edge_ids = self.allocator.dupe(EdgeId, self.edges.keys()) catch @panic("oom");
        defer self.allocator.free(edge_ids);
        std.mem.sort(EdgeId, edge_ids, {}, std.sort.asc(EdgeId));
        for (edge_ids) |id| {
            const edge = self.edges.get(id).?;
            d.edge(id, edge.source, edge.target, if (edge.kind == 0) "T" else "U");
            if (edge.w) |w| d.prop("w", .{ .int_val = w });
        }
        return d.final();
    }
};

/// A hash of a database's logical contents, in a canonical order, so the
/// model and a recovered database can be compared without holding either.
const Digest = struct {
    h: std.hash.Wyhash,

    fn init() Digest {
        return .{ .h = std.hash.Wyhash.init(0) };
    }
    fn tag(self: *Digest, t: u8) void {
        self.h.update(&[_]u8{t});
    }
    fn int(self: *Digest, v: u64) void {
        self.h.update(std.mem.asBytes(&v));
    }
    fn str(self: *Digest, s: []const u8) void {
        self.int(s.len);
        self.h.update(s);
    }
    fn node(self: *Digest, id: NodeId) void {
        self.tag('N');
        self.int(id);
    }
    fn label(self: *Digest, name: []const u8) void {
        self.tag('L');
        self.str(name);
    }
    fn prop(self: *Digest, key: []const u8, value: PropertyValue) void {
        self.tag('P');
        self.str(key);
        switch (value) {
            .int_val => |v| {
                self.tag('i');
                self.int(@bitCast(v));
            },
            .string_val => |v| {
                self.tag('s');
                self.str(v);
            },
            else => self.tag('?'),
        }
    }
    fn vector(self: *Digest, v: []const f32) void {
        // The index keeps vectors normalized, so compare directions, at a
        // precision that rounding in the normalization cannot reach.
        self.tag('V');
        var norm: f64 = 0;
        for (v) |x| norm += @as(f64, x) * x;
        norm = @sqrt(norm);
        for (v) |x| {
            const unit = if (norm == 0) 0 else @as(f64, x) / norm;
            self.int(@bitCast(@as(i64, @intFromFloat(@round(unit * 10_000)))));
        }
    }
    fn edge(self: *Digest, id: EdgeId, source: NodeId, target: NodeId, kind: []const u8) void {
        self.tag('E');
        self.int(id);
        self.int(source);
        self.int(target);
        self.str(kind);
    }
    fn final(self: *Digest) u64 {
        return self.h.final();
    }
};

// ============================================================================
// The workload
// ============================================================================

/// What the workload recorded: where in the trace each commit returned, and
/// the state it left.
const Run = struct {
    allocator: Allocator,
    fs: FaultVfs,
    /// `states[k]` is the digest after the first k transactions;
    /// `acked_at[k]` is the trace length when the k-th commit returned.
    states: std.ArrayListUnmanaged(u64) = .empty,
    acked_at: std.ArrayListUnmanaged(usize) = .empty,
    /// Cuts before this point are not tested: the database is still being
    /// set up.
    start: usize = 0,
    /// Where a `sync()` or a close returned, and how many transactions had
    /// committed by then: without a log, what a cut must keep.
    durable: std.ArrayListUnmanaged(struct { at: usize, k: usize }) = .empty,

    fn deinit(self: *Run) void {
        self.states.deinit(self.allocator);
        self.acked_at.deinit(self.allocator);
        self.durable.deinit(self.allocator);
        self.fs.deinit();
    }
};

fn dbConfig(options: Options) DatabaseConfig {
    return .{
        .enable_wal = !options.no_log,
        .enable_fts = options.fts,
        .enable_vector = options.vectors,
        .vector_dimensions = DIMS,
        .buffer_pool_size = options.pool_pages * 4096,
        .auto_checkpoint = .{ .max_wal_frames = options.max_wal_frames },
        .sync_mode = options.sync_mode,
        .enable_query_cache = false,
    };
}

fn openDb(allocator: Allocator, fs: *FaultVfs, options: Options, create: bool, seed: u64) !*Database {
    const db = try Database.openWithVfs(allocator, DB_PATH, .{ .create = create, .config = dbConfig(options) }, fs.vfs());
    // The vector index picks levels at random, seeded from the clock. Seed
    // it from the run so that a run can be repeated.
    if (db.hnsw_index) |*hnsw| hnsw.rng = std.Random.DefaultPrng.init(seed);
    return db;
}

fn randomText(allocator: Allocator, random: std.Random, id: NodeId) ![]u8 {
    return std.fmt.allocPrint(allocator, "common w{d} u{d} tail", .{ random.uintLessThan(u8, 12), id });
}

fn randomString(allocator: Allocator, random: std.Random, big: bool) ![]u8 {
    const len = if (big and random.uintLessThan(u8, 3) == 0)
        500 + random.uintLessThan(usize, 7000)
    else
        1 + random.uintLessThan(usize, 40);
    const s = try allocator.alloc(u8, len);
    const c = 'a' + random.uintLessThan(u8, 26);
    @memset(s, c);
    s[0] = 'a' + random.uintLessThan(u8, 26);
    return s;
}

fn randomVector(random: std.Random) [DIMS]f32 {
    var v: [DIMS]f32 = undefined;
    for (&v) |*x| x.* = @floatFromInt(random.intRangeAtMost(i32, -50, 50));
    v[random.uintLessThan(usize, DIMS)] += 100;
    return v;
}

fn pickNode(model: *Model, random: std.Random) ?NodeId {
    const keys = model.nodes.keys();
    if (keys.len == 0) return null;
    return keys[random.uintLessThan(usize, keys.len)];
}

/// One change, made to the database inside `txn` and, when `model` is
/// given, to the model too.
fn randomOp(
    allocator: Allocator,
    db: *Database,
    txn: *lattice.transaction.manager.Transaction,
    model: *Model,
    random: std.Random,
    options: Options,
    apply_to_model: bool,
) !void {
    const choice = random.uintLessThan(u8, 100);
    const target = pickNode(model, random);
    if (choice < 25 or target == null) {
        const doc = options.fts and random.uintLessThan(u8, 3) == 0;
        const a = random.boolean();
        var labels_buf: [2][]const u8 = undefined;
        var labels_len: usize = 0;
        if (a) {
            labels_buf[labels_len] = "A";
            labels_len += 1;
        }
        if (doc) {
            labels_buf[labels_len] = "Doc";
            labels_len += 1;
        }
        const id = try db.createNode(txn, labels_buf[0..labels_len]);
        var node = MNode{ .a = a, .doc = doc };
        const n = @as(i64, random.uintLessThan(u32, 10));
        try db.setNodeProperty(txn, id, "n", .{ .int_val = n });
        node.n = n;
        if (random.boolean()) {
            const s = try randomString(allocator, random, options.big_values);
            try db.setNodeProperty(txn, id, "s", .{ .string_val = s });
            node.s = s;
        }
        if (doc) {
            const t = try randomText(allocator, random, id);
            try db.setNodeProperty(txn, id, "text", .{ .string_val = t });
            node.text = t;
        }
        if (options.vectors and random.boolean()) {
            const v = randomVector(random);
            try db.setNodeVectorInTxn(txn, id, &v);
            node.vector = v;
        }
        if (apply_to_model) try model.nodes.put(allocator, id, node) else Model.freeNode(allocator, node);
        return;
    }
    const id = target.?;
    if (!apply_to_model and model.gone_nodes.contains(id)) return;
    const node = model.nodes.getPtr(id).?;
    if (choice < 40) {
        const n = @as(i64, random.uintLessThan(u32, 10));
        try db.setNodeProperty(txn, id, "n", .{ .int_val = n });
        if (apply_to_model) node.n = n;
    } else if (choice < 50) {
        const s = try randomString(allocator, random, options.big_values);
        try db.setNodeProperty(txn, id, "s", .{ .string_val = s });
        if (apply_to_model) {
            if (node.s) |old| allocator.free(old);
            node.s = s;
        } else allocator.free(s);
    } else if (choice < 55) {
        try db.removeNodeProperty(txn, id, "s");
        if (apply_to_model) {
            if (node.s) |old| allocator.free(old);
            node.s = null;
        }
    } else if (choice < 60) {
        if (node.b) {
            try db.removeNodeLabel(txn, id, "B");
            if (apply_to_model) node.b = false;
        } else {
            try db.addNodeLabel(txn, id, "B");
            if (apply_to_model) node.b = true;
        }
    } else if (choice < 64) {
        if (node.a) {
            try db.removeNodeLabel(txn, id, "A");
            if (apply_to_model) node.a = false;
        } else {
            try db.addNodeLabel(txn, id, "A");
            if (apply_to_model) node.a = true;
        }
    } else if (choice < 70 and node.doc) {
        const t = try randomText(allocator, random, id);
        try db.setNodeProperty(txn, id, "text", .{ .string_val = t });
        if (apply_to_model) {
            if (node.text) |old| allocator.free(old);
            node.text = t;
        } else allocator.free(t);
    } else if (choice < 74 and options.vectors) {
        const v = randomVector(random);
        try db.setNodeVectorInTxn(txn, id, &v);
        if (apply_to_model) node.vector = v;
    } else if (choice < 84) {
        const other = pickNode(model, random).?;
        if (!apply_to_model and model.gone_nodes.contains(other)) return;
        const kind: u1 = @intCast(random.uintLessThan(u8, 2));
        const edge_id = try db.createEdgeAndGetId(txn, id, other, if (kind == 0) "T" else "U");
        var edge = MEdge{ .source = id, .target = other, .kind = kind };
        if (random.boolean()) {
            const w = @as(i64, random.uintLessThan(u32, 1000));
            try db.setEdgePropertyById(txn, edge_id, "w", .{ .int_val = w });
            edge.w = w;
        }
        if (apply_to_model) try model.edges.put(allocator, edge_id, edge);
    } else if (choice < 90 and model.edges.count() > 0) {
        const keys = model.edges.keys();
        const edge_id = keys[random.uintLessThan(usize, keys.len)];
        if (!apply_to_model and model.gone_edges.contains(edge_id)) return;
        if (random.boolean()) {
            try db.deleteEdgeById(txn, edge_id);
            if (apply_to_model) _ = model.edges.orderedRemove(edge_id) else try model.gone_edges.put(allocator, edge_id, {});
        } else {
            const w = @as(i64, random.uintLessThan(u32, 1000));
            try db.setEdgePropertyById(txn, edge_id, "w", .{ .int_val = w });
            if (apply_to_model) model.edges.getPtr(edge_id).?.w = w;
        }
    } else {
        // A node goes with its edges, as DETACH DELETE does it.
        var i: usize = 0;
        while (i < model.edges.count()) {
            const e = model.edges.values()[i];
            const edge_id = model.edges.keys()[i];
            if ((e.source == id or e.target == id) and (apply_to_model or !model.gone_edges.contains(edge_id))) {
                try db.deleteEdgeById(txn, edge_id);
                if (!apply_to_model) try model.gone_edges.put(allocator, edge_id, {});
                if (apply_to_model) {
                    _ = model.edges.orderedRemove(edge_id);
                    continue;
                }
            }
            i += 1;
        }
        try db.deleteNode(txn, id);
        if (apply_to_model) {
            const removed = model.nodes.fetchOrderedRemove(id).?;
            Model.freeNode(allocator, removed.value);
        } else try model.gone_nodes.put(allocator, id, {});
    }
}

fn runWorkload(allocator: Allocator, options: Options) !Run {
    var result = Run{ .allocator = allocator, .fs = FaultVfs.init(allocator) };
    errdefer result.deinit();

    var prng = std.Random.DefaultPrng.init(options.seed);
    const random = prng.random();
    var model = Model{ .allocator = allocator };
    defer model.deinit();

    var db = try openDb(allocator, &result.fs, options, true, options.seed);
    var db_open = true;
    defer if (db_open) db.close();
    if (options.fts) try db.createNodeFtsIndex("Doc", "text");
    try db.createNodePropertyIndex("A", "n");
    // Without a log the indexes reach the file with the next flush, and the
    // cuts start from a file that has them.
    if (options.no_log) try db.sync();

    result.start = result.fs.traceLen();
    try result.states.append(allocator, model.digest());
    try result.acked_at.append(allocator, result.start);

    const reopen_every = if (options.reopens == 0) std.math.maxInt(usize) else options.txns / (options.reopens + 1) + 1;

    for (1..options.txns + 1) |k| {
        // Now and then, some housekeeping between transactions.
        const roll = random.uintLessThan(u8, 100);
        if (roll < 4) {
            try db.sync();
            try result.durable.append(allocator, .{ .at = result.fs.traceLen(), .k = k - 1 });
        } else if (roll < 6) {
            _ = try db.checkpoint(.full);
        } else if (roll < 9) {
            // A transaction that is rolled back leaves no trace.
            var txn = try db.beginTransaction(.read_write);
            model.gone_nodes.clearRetainingCapacity();
            model.gone_edges.clearRetainingCapacity();
            for (0..1 + random.uintLessThan(usize, 3)) |_| {
                try randomOp(allocator, db, &txn, &model, random, options, false);
            }
            try db.abortTransaction(&txn);
        }

        if (k % reopen_every == 0) {
            db.close();
            db_open = false;
            try result.durable.append(allocator, .{ .at = result.fs.traceLen(), .k = k - 1 });
            if (options.drop_log_on_reopen) {
                try result.fs.vfs().delete(WAL_PATH);
                result.fs.vfs().syncParentDir(WAL_PATH);
            }
            db = try openDb(allocator, &result.fs, options, false, options.seed +% k);
            db_open = true;
        }

        var txn = try db.beginTransaction(.read_write);
        const big = random.uintLessThan(u8, 12) == 0;
        const ops = if (big) 15 + random.uintLessThan(usize, 25) else 1 + random.uintLessThan(usize, 4);
        for (0..ops) |_| try randomOp(allocator, db, &txn, &model, random, options, true);
        try db.commitTransaction(&txn);
        try result.states.append(allocator, model.digest());
        try result.acked_at.append(allocator, result.fs.traceLen());
    }

    db.close();
    db_open = false;
    return result;
}

// ============================================================================
// Checking a recovered database
// ============================================================================

const Failure = error{ Invariant, OpenFailed };

fn fail(comptime fmt: []const u8, args: anytype) Failure {
    std.debug.print("    invariant: " ++ fmt ++ "\n", args);
    return error.Invariant;
}

/// Digest of the database's logical contents, and a structural check.
fn checkDatabase(allocator: Allocator, db: *Database, options: Options) !u64 {
    var d = Digest.init();

    const ids = try db.getAllNodeIdsInTxn(null);
    defer allocator.free(ids);
    std.mem.sort(NodeId, ids, {}, std.sort.asc(NodeId));

    if (db.nodeCount() != ids.len) return fail("nodeCount {d} but {d} nodes", .{ db.nodeCount(), ids.len });

    var with_label: [3]std.AutoHashMapUnmanaged(NodeId, void) = .{ .empty, .empty, .empty };
    defer for (&with_label) |*set| set.deinit(allocator);
    var docs: std.AutoHashMapUnmanaged(NodeId, void) = .empty;
    defer docs.deinit(allocator);
    var vectored: std.AutoHashMapUnmanaged(NodeId, void) = .empty;
    defer vectored.deinit(allocator);
    var by_n: [10]std.AutoHashMapUnmanaged(NodeId, void) = @splat(.empty);
    defer for (&by_n) |*set| set.deinit(allocator);

    for (ids) |id| {
        d.node(id);
        const labels = try db.getNodeLabelsInTxn(null, id);
        defer {
            for (labels) |l| allocator.free(l);
            allocator.free(labels);
        }
        std.mem.sort([]const u8, labels, {}, struct {
            fn lt(_: void, a: []const u8, b: []const u8) bool {
                return std.mem.lessThan(u8, a, b);
            }
        }.lt);
        var is_a = false;
        for (labels) |l| {
            d.label(l);
            const idx: ?usize = if (std.mem.eql(u8, l, "A")) 0 else if (std.mem.eql(u8, l, "B")) 1 else if (std.mem.eql(u8, l, "Doc")) 2 else null;
            if (idx) |i| try with_label[i].put(allocator, id, {}) else return fail("node {d} has unknown label {s}", .{ id, l });
            if (idx == 0) is_a = true;
        }

        const props = try db.getNodePropertiesInTxn(null, id);
        defer db.freePropertyEntries(props);
        std.mem.sort(Database.PropertyEntry, props, {}, struct {
            fn lt(_: void, a: Database.PropertyEntry, b: Database.PropertyEntry) bool {
                return std.mem.lessThan(u8, a.key, b.key);
            }
        }.lt);
        var has_text = false;
        for (props) |p| {
            d.prop(p.key, p.value);
            if (std.mem.eql(u8, p.key, "text")) has_text = true;
            if (is_a and std.mem.eql(u8, p.key, "n") and p.value == .int_val and p.value.int_val >= 0 and p.value.int_val < 10) {
                try by_n[@intCast(p.value.int_val)].put(allocator, id, {});
            }
        }
        if (has_text and with_label[2].contains(id)) try docs.put(allocator, id, {});

        if (options.vectors) {
            if (try db.getNodeVectorInTxn(null, id)) |v| {
                defer allocator.free(v);
                d.vector(v);
                try vectored.put(allocator, id, {});
            }
        }
    }

    const edges = try db.scanEdgesInTxn(null, null, std.math.maxInt(usize));
    defer db.freeEdgeInfos(edges);
    std.mem.sort(Database.EdgeInfo, edges, {}, struct {
        fn lt(_: void, a: Database.EdgeInfo, b: Database.EdgeInfo) bool {
            return a.id < b.id;
        }
    }.lt);
    if (db.edgeCount() != edges.len) return fail("edgeCount {d} but {d} edges", .{ db.edgeCount(), edges.len });
    var out_degree: std.AutoHashMapUnmanaged(NodeId, usize) = .empty;
    defer out_degree.deinit(allocator);
    var in_degree: std.AutoHashMapUnmanaged(NodeId, usize) = .empty;
    defer in_degree.deinit(allocator);
    for (edges) |e| {
        d.edge(e.id, e.source, e.target, e.edge_type);
        if (!try db.nodeExists(e.source)) return fail("edge {d} source {d} missing", .{ e.id, e.source });
        if (!try db.nodeExists(e.target)) return fail("edge {d} target {d} missing", .{ e.id, e.target });
        const props = try db.getEdgePropertiesInTxn(null, e.id);
        defer db.freePropertyEntries(props);
        for (props) |p| d.prop(p.key, p.value);
        (try out_degree.getOrPutValue(allocator, e.source, 0)).value_ptr.* += 1;
        (try in_degree.getOrPutValue(allocator, e.target, 0)).value_ptr.* += 1;
    }

    // Adjacency agrees with the edge scan.
    for (ids) |id| {
        const out = try db.getOutgoingEdges(id);
        defer db.freeEdgeInfos(out);
        const incoming = try db.getIncomingEdges(id);
        defer db.freeEdgeInfos(incoming);
        if (out.len != (out_degree.get(id) orelse 0)) return fail("node {d}: {d} outgoing, scan says {d}", .{ id, out.len, out_degree.get(id) orelse 0 });
        if (incoming.len != (in_degree.get(id) orelse 0)) return fail("node {d}: {d} incoming, scan says {d}", .{ id, incoming.len, in_degree.get(id) orelse 0 });
    }

    // The label index agrees with the nodes.
    for ([_][]const u8{ "A", "B", "Doc" }, 0..) |label, i| {
        const listed = try db.getNodesByLabel(label);
        defer allocator.free(listed);
        for (listed) |id| if (!with_label[i].contains(id)) return fail("label {s}: index lists {d}, which {s}", .{ label, id, if (try db.nodeExists(id)) "does not have it" else "does not exist" });
        if (listed.len != with_label[i].count()) return fail("label {s}: index lists {d}, nodes say {d}", .{ label, listed.len, with_label[i].count() });
    }

    // The property index agrees with the nodes.
    for (0..10) |n| {
        const found = try db.findNodesByLabelProperty(null, "A", "n", .{ .int_val = @intCast(n) }, std.math.maxInt(usize));
        defer allocator.free(found);
        if (found.len != by_n[n].count()) return fail("index A.n={d}: {d} found, nodes say {d}", .{ n, found.len, by_n[n].count() });
        for (found) |id| if (!by_n[n].contains(id)) return fail("index A.n={d} finds {d}", .{ n, id });
    }

    // The full-text index agrees with the documents.
    if (options.fts) {
        const all = try db.ftsSearchIndex(.node, "Doc", "text", "common", @intCast(docs.count() + 10));
        defer db.freeFtsSearchResults(all);
        if (all.len != docs.count()) return fail("text index finds {d} documents, there are {d}", .{ all.len, docs.count() });
        for (all) |hit| if (!docs.contains(hit.doc_id)) return fail("text index finds {d}", .{hit.doc_id});
        var checked: usize = 0;
        var it = docs.keyIterator();
        while (it.next()) |id| {
            if (checked == 8) break;
            checked += 1;
            var buf: [32]u8 = undefined;
            const term = try std.fmt.bufPrint(&buf, "u{d}", .{id.*});
            const one = try db.ftsSearchIndex(.node, "Doc", "text", term, 4);
            defer db.freeFtsSearchResults(one);
            if (one.len != 1 or one[0].doc_id != id.*) return fail("text index: {s} finds {d} documents", .{ term, one.len });
        }
    }

    // Every vector can be reached.
    if (options.vectors and vectored.count() > 0) {
        const probe = [_]f32{1} ** DIMS;
        const count: u32 = @intCast(vectored.count());
        const near = try db.vectorSearch(&probe, count, @intCast(@min(count + 16, 65535)));
        defer db.freeVectorSearchResults(near);
        if (near.len != count) return fail("vector search reaches {d} of {d}", .{ near.len, count });
        for (near) |r| if (!vectored.contains(r.node_id)) return fail("vector search finds {d}", .{r.node_id});
    }

    return d.final();
}

fn inodeName(trace: []const Op, inode: u32) []const u8 {
    for (trace) |op| switch (op) {
        .create => |c| if (c.inode == inode) return c.name,
        else => {},
    };
    return "?";
}

/// Print the changes around a position in a trace.
pub fn dumpTrace(trace: []const Op, from: usize, to: usize) void {
    for (trace[from..@min(to, trace.len)], from..) |op, i| switch (op) {
        .create => |c| std.debug.print("  {d}: create {s} ({d})\n", .{ i, c.name, c.inode }),
        .delete => |d| std.debug.print("  {d}: delete {s}\n", .{ i, d.name }),
        .write => |w| std.debug.print("  {d}: write {s} @{d} +{d}\n", .{ i, inodeName(trace, w.inode), w.offset, w.data.len }),
        .truncate => |t| std.debug.print("  {d}: truncate {s} to {d}\n", .{ i, inodeName(trace, t.inode), t.size }),
        .sync => |y| std.debug.print("  {d}: sync {s}\n", .{ i, inodeName(trace, y.inode) }),
        .sync_dir => std.debug.print("  {d}: sync dir\n", .{i}),
    };
}

/// Open a database on `image`, check it, and return its digest. The trace
/// of what the open and close did is left in `fs`.
fn openAndCheck(allocator: Allocator, fs: *FaultVfs, options: Options) !u64 {
    var db = openDb(allocator, fs, options, false, options.seed) catch |err| {
        std.debug.print("    open failed: {s}\n", .{@errorName(err)});
        return error.OpenFailed;
    };
    defer db.close();
    return checkDatabase(allocator, db, options);
}

const Stats = struct {
    cuts: usize = 0,
    second_cuts: usize = 0,
    states_seen: usize = 0,
    failures: usize = 0,
};

fn styleFor(random: std.Random) fault.DiskModel.Style {
    return switch (random.uintLessThan(u8, 6)) {
        0 => .everything,
        1 => .in_order,
        2, 3 => .reordered,
        else => .sectors,
    };
}

fn checkCut(
    allocator: Allocator,
    options: Options,
    workload: *const Run,
    cut: usize,
    style: fault.DiskModel.Style,
    image: *const Image,
    random: std.Random,
    least: usize,
    most: usize,
    second_cut: bool,
) !void {
    const trace_len = workload.fs.trace.items.len;
    var fs = try FaultVfs.initFromImage(allocator, image);
    defer fs.deinit();
    const got = openAndCheck(allocator, &fs, options) catch |err| {
        std.debug.print("power-loss case failed: seed {d}, cut {d} of {d}, style {s}: {s}\n", .{ options.seed, cut, trace_len, @tagName(style), @errorName(err) });
        return err;
    };
    for (least..most + 1) |k| {
        if (workload.states.items[k] == got) break;
    } else {
        var found: ?usize = null;
        for (workload.states.items, 0..) |s, k| if (s == got) {
            found = k;
        };
        std.debug.print("power-loss case failed: seed {d}, cut {d} of {d}, style {s}: state is after {?d} transactions, expected {d} to {d}\n", .{ options.seed, cut, trace_len, @tagName(style), found, least, most });
        return error.Invariant;
    }

    // A second cut, during the recovery this open ran.
    if (!second_cut or fs.trace.items.len == 0) return;
    const second = random.uintAtMost(usize, fs.trace.items.len);
    const second_style = styleFor(random);
    var image2 = try fault.crashImage(allocator, image, fs.trace.items[0..second], random, second_style);
    defer image2.deinit(allocator);
    var fs2 = try FaultVfs.initFromImage(allocator, &image2);
    defer fs2.deinit();
    const got2 = openAndCheck(allocator, &fs2, options) catch |err| {
        std.debug.print("power-loss case failed: seed {d}, cut {d} of {d}, style {s}, second cut {d} of {d}, style {s}: {s}\n", .{ options.seed, cut, trace_len, @tagName(style), second, fs.trace.items.len, @tagName(second_style), @errorName(err) });
        if (options.verbose) dumpTrace(fs.trace.items, second -| 30, second + 3);
        return err;
    };
    if (got2 != got) {
        std.debug.print("power-loss case failed: seed {d}, cut {d} of {d}, style {s}, second cut {d} of {d}, style {s}: recovery after the second cut differs from the first\n", .{ options.seed, cut, trace_len, @tagName(style), second, fs.trace.items.len, @tagName(second_style) });
        if (options.verbose) dumpTrace(fs.trace.items, second -| 30, second + 3);
        return error.Invariant;
    }
}

pub fn run(allocator: Allocator, options: Options) !Stats {
    var workload = try runWorkload(allocator, options);
    defer workload.deinit();
    const trace = workload.fs.trace.items;

    // The cuts: before and after every sync, and some at random.
    var cuts: std.ArrayListUnmanaged(usize) = .empty;
    defer cuts.deinit(allocator);
    // The end of the run, with everything on the disk, holds everything.
    {
        var model = fault.DiskModel.init(allocator);
        defer model.deinit();
        try model.applyAll(trace);
        var prng = std.Random.DefaultPrng.init(0);
        var image = try model.crash(prng.random(), .everything);
        defer image.deinit(allocator);
        var fs = try FaultVfs.initFromImage(allocator, &image);
        defer fs.deinit();
        const got = try openAndCheck(allocator, &fs, options);
        if (got != workload.states.items[workload.states.items.len - 1]) {
            var found: ?usize = null;
            for (workload.states.items, 0..) |s, k| if (s == got) {
                found = k;
            };
            std.debug.print("power-loss case failed: seed {d}: the closed database holds the state after {?d} transactions, not {d}\n", .{ options.seed, found, workload.states.items.len - 1 });
            return error.Invariant;
        }
    }

    if (options.only_cut) |cut| {
        try cuts.append(allocator, cut);
    } else {
        for (trace[workload.start..], workload.start..) |op, i| {
            if (op.isSync()) {
                try cuts.append(allocator, i);
                try cuts.append(allocator, i + 1);
            }
        }
        var prng = std.Random.DefaultPrng.init(options.seed ^ 0xC0FFEE);
        for (0..options.random_cuts) |_| {
            try cuts.append(allocator, prng.random().intRangeAtMost(usize, workload.start, trace.len));
        }
        std.mem.sort(usize, cuts.items, {}, std.sort.asc(usize));
    }

    var stats = Stats{};
    var model = fault.DiskModel.init(allocator);
    defer model.deinit();
    var applied: usize = 0;
    var last_cut: ?usize = null;

    // Which files have been the log, and where it was last synced.
    var log_inodes: std.AutoHashMapUnmanaged(u32, void) = .empty;
    defer log_inodes.deinit(allocator);
    for (trace) |op| switch (op) {
        .create => |c| if (std.mem.eql(u8, c.name, WAL_PATH)) try log_inodes.put(allocator, c.inode, {}),
        else => {},
    };
    var last_log_sync: usize = 0;

    for (cuts.items, 0..) |cut, cut_number| {
        if (last_cut == cut) continue;
        last_cut = cut;
        for (trace[applied..cut], applied..) |op, i| switch (op) {
            .sync => |sync_op| if (log_inodes.contains(sync_op.inode)) {
                last_log_sync = i;
            },
            else => {},
        };
        try model.applyAll(trace[applied..cut]);
        applied = cut;

        // What must and may be there.
        var acked: usize = 0;
        while (acked + 1 < workload.acked_at.items.len and workload.acked_at.items[acked + 1] <= cut) acked += 1;
        const most = @min(acked + 1, workload.states.items.len - 1);
        var least = acked;
        if (options.no_log) {
            least = 0;
            for (workload.durable.items) |d| {
                if (d.at <= cut) least = d.k;
            }
        } else if (options.sync_mode == .normal) {
            least = 0;
            while (least + 1 < workload.acked_at.items.len and workload.acked_at.items[least + 1] <= last_log_sync) least += 1;
        }

        var prng = std.Random.DefaultPrng.init(options.seed *% 0x9E3779B97F4A7C15 +% cut);
        const random = prng.random();
        const style = styleFor(random);
        if (options.verbose) std.debug.print("cut {d} ({s}) acked {d}\n", .{ cut, @tagName(style), acked });

        var image = try model.crash(random, style);
        defer image.deinit(allocator);
        stats.cuts += 1;
        const second_cut = options.second_cut_every > 0 and cut_number % options.second_cut_every == 0;
        if (second_cut) stats.second_cuts += 1;
        checkCut(allocator, options, &workload, cut, style, &image, random, least, most, second_cut) catch |err| {
            if (options.verbose) dumpTrace(trace, cut -| 40, cut + 3);
            if (!options.keep_going) return err;
            stats.failures += 1;
        };
    }
    if (stats.failures > 0) {
        std.debug.print("{d} of {d} cuts failed\n", .{ stats.failures, stats.cuts });
        return error.Invariant;
    }
    stats.states_seen = workload.states.items.len;
    return stats;
}
