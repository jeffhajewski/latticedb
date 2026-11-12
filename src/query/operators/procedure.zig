//! Built-in procedures for `CALL name() [YIELD ...]`: what the database's
//! schema-free catalog holds. Each answers with rows of strings, sorted so
//! that the answer does not depend on storage order.
//!
//! - `db.labels()`: `label`, every node label in use.
//! - `db.relationshipTypes()`: `relationshipType`, every relationship type
//!   in use.
//! - `db.propertyKeys()`: `propertyKey`, every property key some node or
//!   relationship has. This reads every node and relationship.
//! - `db.indexes()`: `type` (`property` or `fulltext`), `entityType` (`NODE`
//!   or `RELATIONSHIP`), `labelOrType`, `property`, `encoding`: every
//!   declared property and full-text index. A property index's encoding is
//!   `ordered_dense` (a label's, created by this release), `ordered` (a
//!   relationship type's), or `digest` (created by 0.16.0: equality only
//!   until `lattice compact --upgrade-indexes` rebuilds it); a full-text
//!   index's is `fulltext`.
//!
//! Only listing procedures exist; searching is done with `@@` and `<=>` in
//! WHERE.

const std = @import("std");
const Allocator = std.mem.Allocator;

const executor = @import("../executor.zig");
const Operator = executor.Operator;
const OperatorError = executor.OperatorError;
const Row = executor.Row;
const ExecutionContext = executor.ExecutionContext;
const MAX_SLOTS = executor.MAX_SLOTS;

const database_mod = @import("../../storage/database.zig");
const Database = database_mod.Database;
const property_index_mod = @import("../../graph/property_index.zig");
const fts_catalog_mod = @import("../../fts/catalog.zig");
const symbols = @import("../../graph/symbols.zig");
const expand_ops = @import("expand.zig");

/// The procedures a query can CALL, their names and columns.
pub const Procedure = @import("../procedures.zig").Procedure;

/// Calls a procedure for each input row and passes on the input row once per
/// row of the procedure's answer, with the columns asked for in their slots.
/// The answer is the same for every input row, so it is computed once.
pub const ProcedureCall = struct {
    input: Operator,
    procedure: Procedure,
    /// For each column of the procedure, the slot it goes to, or null when
    /// it was not asked for.
    column_slots: []const ?u8,
    database: *Database,
    allocator: Allocator,

    answer: ?[]const []const []const u8 = null,
    current: ?*Row = null,
    index: usize = 0,
    output_row: ?*Row = null,

    const Self = @This();

    pub fn init(allocator: Allocator, input: Operator, procedure: Procedure, column_slots: []const ?u8, database: *Database) !*Self {
        const self = try allocator.create(Self);
        self.* = .{
            .input = input,
            .procedure = procedure,
            .column_slots = column_slots,
            .database = database,
            .allocator = allocator,
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
        .deinit = deinitOp,
    };

    fn open(ptr: *anyopaque, ctx: *ExecutionContext) OperatorError!void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        self.output_row = ctx.allocRow() catch return OperatorError.OutOfMemory;
        self.current = null;
        try self.input.open(ctx);
    }

    fn next(ptr: *anyopaque, ctx: *ExecutionContext) OperatorError!?*Row {
        const self: *Self = @ptrCast(@alignCast(ptr));
        const out = self.output_row orelse return OperatorError.NotInitialized;
        const answer = self.answer orelse blk: {
            const computed = try self.compute(ctx);
            self.answer = computed;
            break :blk computed;
        };
        while (true) {
            const input_row = self.current orelse blk: {
                const row = try self.input.next(ctx) orelse return null;
                self.current = row;
                self.index = 0;
                break :blk row;
            };
            if (self.index >= answer.len) {
                self.current = null;
                continue;
            }
            const values = answer[self.index];
            self.index += 1;
            out.* = input_row.*;
            for (self.column_slots, values) |slot, value| {
                const s = slot orelse continue;
                out.setSlot(s, .{ .property = .{ .string_val = value } });
            }
            return out;
        }
    }

    const Rows = std.ArrayList([]const []const u8);

    fn compute(self: *Self, ctx: *ExecutionContext) OperatorError![]const []const []const u8 {
        var rows: Rows = .empty;
        switch (self.procedure) {
            .labels => {
                const labels = self.database.getAllLabels() catch |err| return ctx.storageFailure(err, "listing the labels for db.labels()", .{});
                defer self.database.freeLabelInfos(labels);
                for (labels) |info| try self.addRow(&rows, &.{info.name});
            },
            .relationship_types => {
                const types = self.database.getAllEdgeTypes() catch |err| return ctx.storageFailure(err, "listing the relationship types for db.relationshipTypes()", .{});
                defer self.database.freeEdgeTypeInfos(types);
                for (types) |info| try self.addRow(&rows, &.{info.name});
            },
            .property_keys => try self.propertyKeys(ctx, &rows),
            .indexes => try self.indexes(ctx, &rows),
        }
        const items = rows.toOwnedSlice(self.allocator) catch return OperatorError.OutOfMemory;
        std.mem.sort([]const []const u8, items, {}, rowLessThan);
        return items;
    }

    fn rowLessThan(_: void, a: []const []const u8, b: []const []const u8) bool {
        for (a, b) |x, y| {
            switch (std.mem.order(u8, x, y)) {
                .lt => return true,
                .gt => return false,
                .eq => {},
            }
        }
        return false;
    }

    fn addRow(self: *Self, rows: *Rows, values: []const []const u8) OperatorError!void {
        const row = self.allocator.alloc([]const u8, values.len) catch return OperatorError.OutOfMemory;
        for (values, row) |v, *r| r.* = self.allocator.dupe(u8, v) catch return OperatorError.OutOfMemory;
        rows.append(self.allocator, row) catch return OperatorError.OutOfMemory;
    }

    fn addSymbolRow(self: *Self, ctx: *ExecutionContext, rows: *Rows, prefix: []const []const u8, ids: []const symbols.SymbolId) OperatorError!void {
        var values: [4][]const u8 = undefined;
        for (prefix, 0..) |p, i| values[i] = p;
        var resolved: [4]?[]const u8 = .{ null, null, null, null };
        defer for (resolved) |r| if (r) |s| self.database.symbol_table.freeString(s);
        for (ids, prefix.len..) |id, i| {
            const s = self.database.symbol_table.resolve(id) catch |err| return ctx.storageFailure(err, "reading the name of symbol {d}", .{id});
            resolved[i] = s;
            values[i] = s;
        }
        try self.addRow(rows, values[0 .. prefix.len + ids.len]);
    }

    /// Every key some node or relationship has, from reading them all.
    fn propertyKeys(self: *Self, ctx: *ExecutionContext, rows: *Rows) OperatorError!void {
        const db = self.database;
        var seen = std.AutoHashMap(symbols.SymbolId, void).init(self.allocator);
        defer seen.deinit();
        const node_ids = db.getAllNodeIdsInTxn(ctx.txn) catch |err| return ctx.storageFailure(err, "listing the nodes for db.propertyKeys()", .{});
        defer db.allocator.free(node_ids);
        for (node_ids) |node_id| {
            if (db.getNodeInTxn(ctx.txn, node_id) catch |err| return ctx.storageFailure(err, "reading node {d} for db.propertyKeys()", .{node_id})) |found| {
                var node = found;
                defer node.deinit(db.allocator);
                for (node.properties) |prop| seen.put(prop.key_id, {}) catch return OperatorError.OutOfMemory;
            }
            const edges = try expand_ops.fetchEdgeRefs(db, ctx, node_id, &.{}, false);
            defer db.allocator.free(edges);
            for (edges) |edge_ref| {
                var edge = (db.getEdgeInTxn(ctx.txn, edge_ref.id) catch |err| return ctx.storageFailure(err, "reading relationship {d} for db.propertyKeys()", .{edge_ref.id})) orelse continue;
                defer edge.deinit(db.allocator);
                for (edge.properties) |prop| seen.put(prop.key_id, {}) catch return OperatorError.OutOfMemory;
            }
        }
        var it = seen.keyIterator();
        while (it.next()) |id| try self.addSymbolRow(ctx, rows, &.{}, &.{id.*});
    }

    /// Every declared property index and full-text index.
    fn indexes(self: *Self, ctx: *ExecutionContext, rows: *Rows) OperatorError!void {
        const db = self.database;
        if (db.property_index) |*index| {
            inline for (.{ property_index_mod.EntityKind.node, property_index_mod.EntityKind.edge }) |kind| {
                var iter: property_index_mod.PropertyIndex.DefinitionIterator = undefined;
                index.iterateDefinitions(kind, &iter) catch |err| return ctx.storageFailure(err, "listing the property indexes for db.indexes()", .{});
                defer iter.deinit();
                while (iter.next() catch |err| return ctx.storageFailure(err, "listing the indexes for db.indexes()", .{})) |d| {
                    try self.addIndexRow(ctx, rows, "property", kind == .node, d.scope_id, d.property_id, d.encoding.name());
                }
            }
        }
        if (db.property_index_catalog_tree) |*tree| {
            var catalog = fts_catalog_mod.FtsCatalog.init(tree);
            inline for (.{ fts_catalog_mod.FtsEntityKind.node, fts_catalog_mod.FtsEntityKind.edge }) |kind| {
                var iter: fts_catalog_mod.FtsCatalog.DefinitionIterator = undefined;
                catalog.iterate(kind, &iter) catch |err| return ctx.storageFailure(err, "listing the full-text indexes for db.indexes()", .{});
                defer iter.deinit();
                while (iter.next() catch |err| return ctx.storageFailure(err, "listing the indexes for db.indexes()", .{})) |d| {
                    try self.addIndexRow(ctx, rows, "fulltext", kind == .node, d.scope_id, d.property_id, "fulltext");
                }
            }
        }
    }

    fn addIndexRow(
        self: *Self,
        ctx: *ExecutionContext,
        rows: *Rows,
        index_type: []const u8,
        node: bool,
        scope_id: symbols.SymbolId,
        property_id: symbols.SymbolId,
        encoding: []const u8,
    ) OperatorError!void {
        const db = self.database;
        const scope = db.symbol_table.resolve(scope_id) catch |err| return ctx.storageFailure(err, "reading the name of symbol {d}", .{scope_id});
        defer db.symbol_table.freeString(scope);
        const property = db.symbol_table.resolve(property_id) catch |err| return ctx.storageFailure(err, "reading the name of symbol {d}", .{property_id});
        defer db.symbol_table.freeString(property);
        try self.addRow(rows, &.{ index_type, entityName(node), scope, property, encoding });
    }

    fn entityName(node: bool) []const u8 {
        return if (node) "NODE" else "RELATIONSHIP";
    }

    fn close(ptr: *anyopaque, ctx: *ExecutionContext) void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        self.input.close(ctx);
    }

    fn deinitOp(ptr: *anyopaque, allocator: Allocator) void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        self.input.deinit(allocator);
        allocator.destroy(self);
    }
};

test "procedures are found by name in any case" {
    try std.testing.expectEqual(Procedure.labels, Procedure.find("DB.LABELS").?);
    try std.testing.expect(Procedure.find("db.nope") == null);
}
