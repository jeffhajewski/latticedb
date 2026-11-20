//! The edges of some types, found by one walk of the edge id index.
//!
//! A pattern that starts from a relationship type, `MATCH (a)-[r:KNOWS]->(b)`
//! with `a` unlabelled and unbound, was planned as every node expanded by
//! type: one index probe per node, most of them finding nothing. There is
//! no index from type to edge (see the graph storage notes for why), but the
//! edge id index holds every edge's endpoints and type in the value, so one
//! walk of it answers the pattern.

const std = @import("std");
const Allocator = std.mem.Allocator;

const executor = @import("../executor.zig");
const Operator = executor.Operator;
const OperatorError = executor.OperatorError;
const Row = executor.Row;
const ExecutionContext = executor.ExecutionContext;

const types = @import("../../core/types.zig");
const NodeId = types.NodeId;

const database_mod = @import("../../storage/database.zig");
const Database = database_mod.Database;

const edge_mod = @import("../../graph/edge.zig");
const EdgeRef = edge_mod.EdgeRef;

const symbols = @import("../../graph/symbols.zig");
const SymbolId = symbols.SymbolId;

/// One row per edge of one of `edge_types`, with the node the pattern starts
/// from in `start_slot`, the other end in `end_slot` and the edge in
/// `edge_slot`. For `.outgoing` the start is the edge's source; for
/// `.incoming`, its target.
///
/// It returns what expanding every node would: an edge whose start node no
/// longer exists is left out, as the node scan never offered it. Rows come
/// in the order of the edge id index: ascending edge id in a format-5 file,
/// a stable order that is not id order in a file created earlier.
///
/// A query that only reads streams the index `CHUNK` edges at a time; one
/// that writes takes them all when it opens, so that its own writes cannot
/// change which edges it visits.
pub const EdgeTypeScan = struct {
    allocator: Allocator,
    start_slot: u8,
    end_slot: u8,
    edge_slot: ?u8,
    edge_types: []const SymbolId,
    incoming: bool,
    database: *Database,

    current_row: ?*Row = null,
    /// Every visible node, ascending, for the start node check.
    nodes: ?[]NodeId = null,
    /// The same as a bit per id, when ids are dense enough for that to be
    /// small: a lookup per edge in the sorted ids cost most of the walk.
    node_bits: ?std.DynamicBitSetUnmanaged = null,
    cursor: edge_mod.EdgeStore.RefCursor = .{},
    chunk: ?[]EdgeRef = null,
    chunk_len: usize = 0,
    chunk_pos: usize = 0,
    /// Edges the transaction staged, after the committed ones.
    overlay: ?[]EdgeRef = null,
    overlay_pos: usize = 0,
    overlay_edges: bool = false,
    /// Every row's edge (queries that write).
    all: ?[]EdgeRef = null,
    all_pos: usize = 0,

    pub const CHUNK = 256;

    const Self = @This();

    pub fn init(
        allocator: Allocator,
        start_slot: u8,
        end_slot: u8,
        edge_slot: ?u8,
        edge_types: []const SymbolId,
        incoming: bool,
        database: *Database,
    ) !*Self {
        const self = try allocator.create(Self);
        self.* = .{
            .allocator = allocator,
            .start_slot = start_slot,
            .end_slot = end_slot,
            .edge_slot = edge_slot,
            .edge_types = edge_types,
            .incoming = incoming,
            .database = database,
        };
        return self;
    }

    pub fn operator(self: *Self) Operator {
        return .{ .vtable = &vtable, .ptr = self };
    }

    const vtable = Operator.VTable{
        .open = open,
        .next = next,
        .close = close,
        .deinit = deinit,
    };

    fn open(ptr: *anyopaque, ctx: *ExecutionContext) OperatorError!void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        self.release();
        self.current_row = ctx.allocRow() catch return OperatorError.OutOfMemory;
        self.cursor = .{};
        self.chunk_len = 0;
        self.chunk_pos = 0;
        self.overlay_pos = 0;

        const nodes = self.database.getAllNodeIdsInTxn(ctx.txn) catch |err|
            return ctx.storageFailure(err, "reading the node ids", .{});
        self.nodes = nodes;
        if (nodes.len > 0 and nodes[nodes.len - 1] / 64 <= nodes.len) {
            var bits = std.DynamicBitSetUnmanaged.initEmpty(self.allocator, @intCast(nodes[nodes.len - 1] + 1)) catch
                return OperatorError.OutOfMemory;
            for (nodes) |id| bits.set(@intCast(id));
            self.node_bits = bits;
        }
        self.overlay_edges = self.database.overlayHasEdges(ctx.txn);
        self.overlay = self.database.overlayEdgesOfTypes(ctx.txn, self.edge_types) catch |err|
            return ctx.storageFailure(err, "reading the transaction's relationships", .{});
        self.chunk = self.allocator.alloc(EdgeRef, CHUNK) catch return OperatorError.OutOfMemory;

        if (!ctx.stable_reads) {
            var all: std.ArrayList(EdgeRef) = .empty;
            errdefer all.deinit(self.allocator);
            while (try self.nextEdge(ctx)) |edge| all.append(self.allocator, edge) catch return OperatorError.OutOfMemory;
            self.all = all.toOwnedSlice(self.allocator) catch return OperatorError.OutOfMemory;
            self.all_pos = 0;
        }
    }

    fn startOf(self: *const Self, edge: EdgeRef) NodeId {
        return if (self.incoming) edge.target else edge.source;
    }

    fn startExists(self: *const Self, edge: EdgeRef) bool {
        if (self.node_bits) |bits| {
            const id = self.startOf(edge);
            return id < bits.bit_length and bits.isSet(@intCast(id));
        }
        const nodes = self.nodes orelse return false;
        return std.sort.binarySearch(NodeId, nodes, self.startOf(edge), struct {
            fn order(key: NodeId, item: NodeId) std.math.Order {
                return std.math.order(key, item);
            }
        }.order) != null;
    }

    fn accepts(self: *const Self, edge_type: SymbolId) bool {
        for (self.edge_types) |t| {
            if (t == edge_type) return true;
        }
        return false;
    }

    fn nextEdge(self: *Self, ctx: *ExecutionContext) OperatorError!?EdgeRef {
        const chunk = self.chunk orelse return OperatorError.NotInitialized;
        while (true) {
            if (self.chunk_pos == self.chunk_len) {
                if (self.cursor.done) break;
                self.chunk_len = self.database.edge_store.readRefs(&self.cursor, chunk) catch |err|
                    return ctx.storageFailure(err, "reading the relationships", .{});
                self.chunk_pos = 0;
                if (self.chunk_len == 0) break;
            }
            const edge = chunk[self.chunk_pos];
            self.chunk_pos += 1;
            if (!self.accepts(edge.edge_type)) continue;
            // The overlay's version of an edge it holds comes after.
            if (self.overlay_edges and self.database.overlayHoldsEdge(ctx.txn, edge.id)) continue;
            if (!self.startExists(edge)) continue;
            return edge;
        }
        const overlay = self.overlay orelse return null;
        while (self.overlay_pos < overlay.len) {
            const edge = overlay[self.overlay_pos];
            self.overlay_pos += 1;
            if (!self.startExists(edge)) continue;
            return edge;
        }
        return null;
    }

    fn next(ptr: *anyopaque, ctx: *ExecutionContext) OperatorError!?*Row {
        const self: *Self = @ptrCast(@alignCast(ptr));
        const row = self.current_row orelse return OperatorError.NotInitialized;
        const edge: EdgeRef = if (self.all) |all| blk: {
            if (self.all_pos >= all.len) return null;
            self.all_pos += 1;
            break :blk all[self.all_pos - 1];
        } else (try self.nextEdge(ctx)) orelse return null;

        row.clear();
        row.setSlot(self.start_slot, .{ .node_ref = self.startOf(edge) });
        row.setSlot(self.end_slot, .{ .node_ref = if (self.incoming) edge.source else edge.target });
        if (self.edge_slot) |slot| row.setSlot(slot, .{ .edge_ref = edge.id });
        return row;
    }

    fn release(self: *Self) void {
        if (self.nodes) |nodes| self.database.allocator.free(nodes);
        self.nodes = null;
        if (self.node_bits) |*bits| bits.deinit(self.allocator);
        self.node_bits = null;
        if (self.overlay) |overlay| self.database.allocator.free(overlay);
        self.overlay = null;
        if (self.chunk) |chunk| self.allocator.free(chunk);
        self.chunk = null;
        if (self.all) |all| self.allocator.free(all);
        self.all = null;
    }

    fn close(ptr: *anyopaque, _: *ExecutionContext) void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        self.release();
    }

    fn deinit(ptr: *anyopaque, allocator: Allocator) void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        self.release();
        allocator.destroy(self);
    }
};

test "EdgeTypeScan has an operator table" {
    try std.testing.expect(@TypeOf(EdgeTypeScan.vtable.open) != void);
}
