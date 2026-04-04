//! Import/Export functionality for LatticeDB CLI.
//!
//! Supports JSON, JSONL, CSV, and DOT formats for importing/exporting graph data.

const std = @import("std");
const lattice = @import("lattice");

const Database = lattice.storage.database.Database;
const PropertyValue = lattice.core.types.PropertyValue;
const NodeId = lattice.core.types.NodeId;
const EdgeId = lattice.core.types.EdgeId;
const Transaction = lattice.Transaction;
const TxnMode = lattice.TxnMode;

/// Managed array list for allocator tracking
fn ManagedArrayList(comptime T: type) type {
    return std.array_list.Managed(T);
}

const ImportItemKind = enum {
    node,
    edge,
};

const ImportBatcher = struct {
    db: *Database,
    batch_size: u32,
    batching_enabled: bool,
    txn_active: bool = false,
    txn: Transaction = undefined,
    pending_count: u32 = 0,
    pending_stats: ImportStats = .{},

    const Self = @This();

    fn init(db: *Database, batch_size: u32, on_error_skip: bool) Self {
        return .{
            .db = db,
            .batch_size = @max(batch_size, 1),
            // Preserve row-level skip semantics by only batching when failures abort.
            .batching_enabled = batch_size > 1 and !on_error_skip,
        };
    }

    fn transaction(self: *Self) ImportExportError!?*Transaction {
        if (!self.batching_enabled) return null;

        if (!self.txn_active) {
            self.txn = self.db.beginTransaction(TxnMode.read_write) catch |err| switch (err) {
                error.TransactionsNotEnabled => {
                    self.batching_enabled = false;
                    return null;
                },
                else => return mapDatabaseError(err),
            };
            self.txn_active = true;
        }

        return &self.txn;
    }

    fn recordSuccess(self: *Self, stats: *ImportStats, kind: ImportItemKind) ImportExportError!void {
        if (!self.txn_active) {
            incrementImported(stats, kind);
            return;
        }

        incrementImported(&self.pending_stats, kind);
        self.pending_count += 1;
        if (self.pending_count >= self.batch_size) {
            try self.commit(stats);
        }
    }

    fn recordFailure(self: *Self, stats: *ImportStats, kind: ImportItemKind) ImportExportError!void {
        if (self.txn_active) {
            try self.abort();
        }
        incrementFailed(stats, kind);
    }

    fn flush(self: *Self, stats: *ImportStats) ImportExportError!void {
        if (self.txn_active and self.pending_count > 0) {
            try self.commit(stats);
        }
    }

    fn hasPendingTransaction(self: *const Self) bool {
        return self.txn_active;
    }

    fn commit(self: *Self, stats: *ImportStats) ImportExportError!void {
        if (!self.txn_active) return;

        self.db.commitTransaction(&self.txn) catch |err| return mapDatabaseError(err);
        mergeImportStats(stats, self.pending_stats);
        self.txn_active = false;
        self.pending_count = 0;
        self.pending_stats = .{};
    }

    fn abort(self: *Self) ImportExportError!void {
        if (!self.txn_active) return;

        self.db.abortTransaction(&self.txn) catch |err| return mapDatabaseError(err);
        self.txn_active = false;
        self.pending_count = 0;
        self.pending_stats = .{};
    }
};

fn incrementImported(stats: *ImportStats, kind: ImportItemKind) void {
    switch (kind) {
        .node => stats.nodes_imported += 1,
        .edge => stats.edges_imported += 1,
    }
}

fn incrementFailed(stats: *ImportStats, kind: ImportItemKind) void {
    switch (kind) {
        .node => stats.nodes_failed += 1,
        .edge => stats.edges_failed += 1,
    }
}

fn mergeImportStats(stats: *ImportStats, pending: ImportStats) void {
    stats.nodes_imported += pending.nodes_imported;
    stats.nodes_failed += pending.nodes_failed;
    stats.edges_imported += pending.edges_imported;
    stats.edges_failed += pending.edges_failed;
}

fn mapDatabaseError(err: anyerror) ImportExportError {
    return switch (err) {
        error.OutOfMemory => ImportExportError.OutOfMemory,
        else => ImportExportError.DatabaseError,
    };
}

/// Import data from a JSON file into the database.
/// JSON format:
/// {
///   "nodes": [
///     {"id": "n1", "labels": ["Person"], "properties": {"name": "Alice", "age": 30}},
///     ...
///   ],
///   "edges": [
///     {"source": "n1", "target": "n2", "type": "KNOWS", "properties": {"since": 2020}},
///     ...
///   ]
/// }
pub fn importJson(
    allocator: std.mem.Allocator,
    db: *Database,
    file_path: []const u8,
    batch_size: u32,
    on_error_skip: bool,
) ImportExportError!ImportStats {
    const file = std.fs.cwd().openFile(file_path, .{}) catch {
        return ImportExportError.IoError;
    };
    defer file.close();

    const content = file.readToEndAlloc(allocator, 100 * 1024 * 1024) catch {
        return ImportExportError.IoError;
    };
    defer allocator.free(content);

    return importJsonContent(allocator, db, content, batch_size, on_error_skip);
}

/// Import JSON from a string content
pub fn importJsonContent(
    allocator: std.mem.Allocator,
    db: *Database,
    content: []const u8,
    batch_size: u32,
    on_error_skip: bool,
) ImportExportError!ImportStats {
    var stats = ImportStats{};
    var batcher = ImportBatcher.init(db, batch_size, on_error_skip);
    errdefer if (batcher.hasPendingTransaction()) batcher.abort() catch {};

    // Parse JSON
    const parsed = std.json.parseFromSlice(std.json.Value, allocator, content, .{}) catch {
        return ImportExportError.ParseError;
    };
    defer parsed.deinit();

    const root = parsed.value;
    if (root != .object) {
        return ImportExportError.ParseError;
    }

    // Map from string IDs to NodeIds
    var id_map = std.StringHashMap(NodeId).init(allocator);
    defer id_map.deinit();

    // Import nodes
    if (root.object.get("nodes")) |nodes_val| {
        if (nodes_val == .array) {
            for (nodes_val.array.items) |node_val| {
                const txn = try batcher.transaction();
                if (importNode(allocator, db, txn, node_val, &id_map)) |_| {
                    try batcher.recordSuccess(&stats, .node);
                } else |_| {
                    try batcher.recordFailure(&stats, .node);
                    if (!on_error_skip) {
                        return ImportExportError.DatabaseError;
                    }
                }
            }
        }
    }

    try batcher.flush(&stats);

    // Import edges
    if (root.object.get("edges")) |edges_val| {
        if (edges_val == .array) {
            for (edges_val.array.items) |edge_val| {
                const txn = try batcher.transaction();
                if (importEdge(db, txn, edge_val, &id_map)) |_| {
                    try batcher.recordSuccess(&stats, .edge);
                } else |_| {
                    try batcher.recordFailure(&stats, .edge);
                    if (!on_error_skip) {
                        return ImportExportError.DatabaseError;
                    }
                }
            }
        }
    }

    try batcher.flush(&stats);
    return stats;
}

fn importNode(
    allocator: std.mem.Allocator,
    db: *Database,
    txn: ?*Transaction,
    node_val: std.json.Value,
    id_map: *std.StringHashMap(NodeId),
) !void {
    if (node_val != .object) return error.InvalidFormat;

    const obj = node_val.object;

    // Get node ID (required)
    const id_str = if (obj.get("id")) |v| switch (v) {
        .string => |s| s,
        else => return error.InvalidFormat,
    } else return error.MissingRequiredField;

    // Get labels
    var labels_list = ManagedArrayList([]const u8).init(allocator);
    defer labels_list.deinit();

    if (obj.get("labels")) |labels_val| {
        if (labels_val == .array) {
            for (labels_val.array.items) |label| {
                if (label == .string) {
                    try labels_list.append(label.string);
                }
            }
        }
    }

    // Create the node
    const node_id = db.createNode(txn, labels_list.items) catch {
        return error.DatabaseError;
    };

    // Store the mapping
    try id_map.put(id_str, node_id);

    // Set properties
    if (obj.get("properties")) |props_val| {
        if (props_val == .object) {
            var iter = props_val.object.iterator();
            while (iter.next()) |entry| {
                const prop_val = jsonToPropertyValue(entry.value_ptr.*) catch continue;
                db.setNodeProperty(txn, node_id, entry.key_ptr.*, prop_val) catch continue;
            }
        }
    }
}

fn importEdge(
    db: *Database,
    txn: ?*Transaction,
    edge_val: std.json.Value,
    id_map: *const std.StringHashMap(NodeId),
) !void {
    if (edge_val != .object) return error.InvalidFormat;

    const obj = edge_val.object;

    // Get source ID
    const source_str = if (obj.get("source")) |v| switch (v) {
        .string => |s| s,
        else => return error.InvalidFormat,
    } else return error.MissingRequiredField;

    // Get target ID
    const target_str = if (obj.get("target")) |v| switch (v) {
        .string => |s| s,
        else => return error.InvalidFormat,
    } else return error.MissingRequiredField;

    // Get edge type
    const edge_type = if (obj.get("type")) |v| switch (v) {
        .string => |s| s,
        else => return error.InvalidFormat,
    } else return error.MissingRequiredField;

    // Look up node IDs
    const source_id = id_map.get(source_str) orelse return error.InvalidNodeId;
    const target_id = id_map.get(target_str) orelse return error.InvalidNodeId;

    // Create the edge and retain its stable ID so properties can be applied.
    const edge_id = db.createEdgeAndGetId(txn, source_id, target_id, edge_type) catch {
        return error.DatabaseError;
    };

    if (obj.get("properties")) |props_val| {
        if (props_val == .object) {
            var iter = props_val.object.iterator();
            while (iter.next()) |entry| {
                const prop_val = jsonToPropertyValue(entry.value_ptr.*) catch continue;
                db.setEdgePropertyById(txn, edge_id, entry.key_ptr.*, prop_val) catch continue;
            }
        }
    }
}

fn jsonToPropertyValue(val: std.json.Value) !PropertyValue {
    return switch (val) {
        .null => .{ .null_val = {} },
        .bool => |b| .{ .bool_val = b },
        .integer => |i| .{ .int_val = i },
        .float => |f| .{ .float_val = f },
        .string => |s| .{ .string_val = s },
        else => error.InvalidFormat,
    };
}

/// Import/Export errors
pub const ImportExportError = error{
    InvalidFormat,
    ParseError,
    IoError,
    DatabaseError,
    OutOfMemory,
    UnsupportedFormat,
    InvalidNodeId,
    MissingRequiredField,
};

/// Export database to JSON format
pub fn exportJson(
    allocator: std.mem.Allocator,
    db: *Database,
    writer: anytype,
    label_filter: ?[]const u8,
) !ExportStats {
    var stats = ExportStats{};
    const node_ids = try collectCanonicalNodeIds(allocator, db, label_filter);
    defer allocator.free(node_ids);

    try writer.writeAll("{\"nodes\":[");

    var first_node = true;
    for (node_ids) |node_id| {
        if (!first_node) {
            try writer.writeAll(",");
        }
        first_node = false;

        try writeNodeJson(allocator, db, writer, node_id);
        stats.nodes_exported += 1;
    }

    try writer.writeAll("],\"edges\":[");

    var first_edge = true;
    for (node_ids) |node_id| {
        const edges = db.getOutgoingEdges(node_id) catch continue;
        defer db.freeEdgeInfos(edges);

        for (edges) |edge| {
            if (!first_edge) {
                try writer.writeAll(",");
            }
            first_edge = false;

            try writeEdgeJson(db, writer, edge);
            stats.edges_exported += 1;
        }
    }

    try writer.writeAll("]}\n");

    return stats;
}

/// Dump database to canonical JSON format for cross-engine state comparison.
///
/// Canonical guarantees:
/// - includes unlabeled nodes
/// - nodes sorted by node ID ascending
/// - edges sorted by source ID, target ID, type name, then edge ID
/// - labels sorted lexicographically
/// - property keys sorted lexicographically
/// - nested MAP keys sorted lexicographically
/// - stable edge IDs included in the output
pub fn dumpCanonicalJson(
    allocator: std.mem.Allocator,
    db: *Database,
    writer: anytype,
    label_filter: ?[]const u8,
) !ExportStats {
    var stats = ExportStats{};

    const node_ids = try collectCanonicalNodeIds(allocator, db, label_filter);
    defer allocator.free(node_ids);

    try writer.writeAll("{\"nodes\":[");
    for (node_ids, 0..) |node_id, i| {
        if (i > 0) try writer.writeByte(',');
        try writeNodeJsonCanonical(allocator, db, writer, node_id);
        stats.nodes_exported += 1;
    }
    try writer.writeAll("],\"edges\":[");

    var first_edge = true;
    for (node_ids) |node_id| {
        const edges = db.getOutgoingEdges(node_id) catch continue;
        defer db.freeEdgeInfos(edges);

        std.mem.sort(Database.EdgeInfo, edges, {}, struct {
            fn lessThan(_: void, a: Database.EdgeInfo, b: Database.EdgeInfo) bool {
                if (a.source != b.source) return a.source < b.source;
                if (a.target != b.target) return a.target < b.target;
                const type_order = std.mem.order(u8, a.edge_type, b.edge_type);
                if (type_order != .eq) return type_order == .lt;
                return a.id < b.id;
            }
        }.lessThan);

        for (edges) |edge| {
            if (!first_edge) try writer.writeByte(',');
            first_edge = false;
            try writeEdgeJsonCanonical(allocator, db, writer, edge);
            stats.edges_exported += 1;
        }
    }

    try writer.writeAll("]}\n");
    return stats;
}

/// Export database to JSONL format (one JSON object per line).
pub fn exportJsonl(
    allocator: std.mem.Allocator,
    db: *Database,
    writer: anytype,
    label_filter: ?[]const u8,
) !ExportStats {
    var stats = ExportStats{};
    const node_ids = try collectCanonicalNodeIds(allocator, db, label_filter);
    defer allocator.free(node_ids);

    for (node_ids) |node_id| {
        try writeNodeJsonlRecord(allocator, db, writer, node_id);
        try writer.writeByte('\n');
        stats.nodes_exported += 1;
    }

    for (node_ids) |node_id| {
        const edges = db.getOutgoingEdges(node_id) catch continue;
        defer db.freeEdgeInfos(edges);

        for (edges) |edge| {
            try writeEdgeJsonlRecord(db, writer, edge);
            try writer.writeByte('\n');
            stats.edges_exported += 1;
        }
    }

    return stats;
}

/// Export database to Graphviz DOT format.
pub fn exportDot(
    allocator: std.mem.Allocator,
    db: *Database,
    writer: anytype,
    label_filter: ?[]const u8,
) !ExportStats {
    var stats = ExportStats{};
    const node_ids = try collectCanonicalNodeIds(allocator, db, label_filter);
    defer allocator.free(node_ids);
    try writer.writeAll("digraph G {\n");

    for (node_ids) |node_id| {
        try writeNodeDot(allocator, db, writer, node_id);
        stats.nodes_exported += 1;
    }

    for (node_ids) |node_id| {
        const edges = db.getOutgoingEdges(node_id) catch continue;
        defer db.freeEdgeInfos(edges);

        for (edges) |edge| {
            try writeEdgeDot(writer, edge);
            stats.edges_exported += 1;
        }
    }

    try writer.writeAll("}\n");
    return stats;
}

fn writeJsonStringContent(writer: anytype, s: []const u8) !void {
    for (s) |c| {
        switch (c) {
            '"' => try writer.writeAll("\\\""),
            '\\' => try writer.writeAll("\\\\"),
            '\n' => try writer.writeAll("\\n"),
            '\r' => try writer.writeAll("\\r"),
            '\t' => try writer.writeAll("\\t"),
            else => {
                if (c < 0x20) {
                    try writer.print("\\u00{x:0>2}", .{c});
                } else {
                    try writer.writeByte(c);
                }
            },
        }
    }
}

fn writeJsonString(writer: anytype, s: []const u8) !void {
    try writer.writeByte('"');
    try writeJsonStringContent(writer, s);
    try writer.writeByte('"');
}

fn writeDotStringContent(writer: anytype, s: []const u8) !void {
    for (s) |c| {
        switch (c) {
            '"' => try writer.writeAll("\\\""),
            '\\' => try writer.writeAll("\\\\"),
            '\n' => try writer.writeAll("\\n"),
            '\r' => try writer.writeAll("\\r"),
            '\t' => try writer.writeAll("\\t"),
            else => if (c < 0x20) {
                try writer.writeByte(' ');
            } else {
                try writer.writeByte(c);
            },
        }
    }
}

fn writeNodeJson(
    allocator: std.mem.Allocator,
    db: *Database,
    writer: anytype,
    node_id: NodeId,
) !void {
    try writer.print("{{\"id\":\"{d}\",\"labels\":[", .{node_id});

    // Get labels for this node
    const node_labels = db.getNodeLabels(node_id) catch |err| return mapDatabaseError(err);
    defer {
        for (node_labels) |label| {
            allocator.free(label);
        }
        allocator.free(node_labels);
    }

    for (node_labels, 0..) |label, i| {
        if (i > 0) try writer.writeAll(",");
        try writeJsonString(writer, label);
    }

    try writer.writeAll("],\"properties\":{");

    // Get properties
    const props = db.getNodeProperties(node_id) catch |err| return mapDatabaseError(err);
    defer db.freePropertyEntries(props);

    for (props, 0..) |prop, i| {
        if (i > 0) try writer.writeAll(",");
        try writeJsonString(writer, prop.key);
        try writer.writeByte(':');
        try writePropertyValueJson(writer, prop.value);
    }

    try writer.writeAll("}}");
}

fn writeNodeJsonCanonical(
    allocator: std.mem.Allocator,
    db: *Database,
    writer: anytype,
    node_id: NodeId,
) !void {
    try writer.print("{{\"id\":\"{d}\",\"labels\":[", .{node_id});

    const node_labels = db.getNodeLabels(node_id) catch |err| return mapDatabaseError(err);
    defer {
        for (node_labels) |label| allocator.free(label);
        allocator.free(node_labels);
    }

    const sorted_labels = try allocator.dupe([]const u8, node_labels);
    defer allocator.free(sorted_labels);
    std.mem.sort([]const u8, sorted_labels, {}, struct {
        fn lessThan(_: void, a: []const u8, b: []const u8) bool {
            return std.mem.order(u8, a, b) == .lt;
        }
    }.lessThan);

    for (sorted_labels, 0..) |label, i| {
        if (i > 0) try writer.writeByte(',');
        try writeJsonString(writer, label);
    }

    try writer.writeAll("],\"properties\":{");

    const props = db.getNodeProperties(node_id) catch |err| return mapDatabaseError(err);
    defer db.freePropertyEntries(props);
    try writeSortedPropertyEntriesJson(allocator, writer, props);
    try writer.writeAll("}}");
}

fn writeEdgeJson(db: *Database, writer: anytype, edge: Database.EdgeInfo) !void {
    try writer.print("{{\"source\":\"{d}\",\"target\":\"{d}\",\"type\":", .{ edge.source, edge.target });
    try writeJsonString(writer, edge.edge_type);
    try writer.writeAll(",\"properties\":{");

    const props = db.getEdgeProperties(edge.id) catch |err| return mapDatabaseError(err);
    defer db.freePropertyEntries(props);

    for (props, 0..) |prop, i| {
        if (i > 0) try writer.writeAll(",");
        try writeJsonString(writer, prop.key);
        try writer.writeByte(':');
        try writePropertyValueJson(writer, prop.value);
    }

    try writer.writeAll("}}");
}

fn writeEdgeJsonCanonical(
    allocator: std.mem.Allocator,
    db: *Database,
    writer: anytype,
    edge: Database.EdgeInfo,
) !void {
    try writer.print(
        "{{\"id\":\"{d}\",\"source\":\"{d}\",\"target\":\"{d}\",\"type\":",
        .{ edge.id, edge.source, edge.target },
    );
    try writeJsonString(writer, edge.edge_type);
    try writer.writeAll(",\"properties\":{");

    const props = db.getEdgeProperties(edge.id) catch |err| return mapDatabaseError(err);
    defer db.freePropertyEntries(props);

    try writeSortedPropertyEntriesJson(allocator, writer, props);

    try writer.writeAll("}}");
}

fn writeNodeJsonlRecord(
    allocator: std.mem.Allocator,
    db: *Database,
    writer: anytype,
    node_id: NodeId,
) !void {
    try writer.writeAll("{\"kind\":\"node\",\"id\":");
    try writer.print("\"{d}\",\"labels\":[", .{node_id});

    const node_labels = db.getNodeLabels(node_id) catch |err| return mapDatabaseError(err);
    defer {
        for (node_labels) |label| {
            allocator.free(label);
        }
        allocator.free(node_labels);
    }

    for (node_labels, 0..) |label, i| {
        if (i > 0) try writer.writeAll(",");
        try writeJsonString(writer, label);
    }

    try writer.writeAll("],\"properties\":{");

    const props = db.getNodeProperties(node_id) catch |err| return mapDatabaseError(err);
    defer db.freePropertyEntries(props);

    for (props, 0..) |prop, i| {
        if (i > 0) try writer.writeAll(",");
        try writeJsonString(writer, prop.key);
        try writer.writeByte(':');
        try writePropertyValueJson(writer, prop.value);
    }

    try writer.writeAll("}}");
}

fn writeEdgeJsonlRecord(db: *Database, writer: anytype, edge: Database.EdgeInfo) !void {
    try writer.print("{{\"kind\":\"edge\",\"source\":\"{d}\",\"target\":\"{d}\",\"type\":", .{ edge.source, edge.target });
    try writeJsonString(writer, edge.edge_type);
    try writer.writeAll(",\"properties\":{");

    const props = db.getEdgeProperties(edge.id) catch |err| return mapDatabaseError(err);
    defer db.freePropertyEntries(props);

    for (props, 0..) |prop, i| {
        if (i > 0) try writer.writeAll(",");
        try writeJsonString(writer, prop.key);
        try writer.writeByte(':');
        try writePropertyValueJson(writer, prop.value);
    }

    try writer.writeAll("}}");
}

fn writeNodeDot(
    allocator: std.mem.Allocator,
    db: *Database,
    writer: anytype,
    node_id: NodeId,
) !void {
    try writer.print("  n{d} [label=\"{d}", .{ node_id, node_id });

    const node_labels = db.getNodeLabels(node_id) catch |err| return mapDatabaseError(err);
    defer {
        for (node_labels) |label| {
            allocator.free(label);
        }
        allocator.free(node_labels);
    }

    if (node_labels.len > 0) {
        try writer.writeAll(" : ");
        for (node_labels, 0..) |label, i| {
            if (i > 0) try writer.writeAll(":");
            try writeDotStringContent(writer, label);
        }
    }

    try writer.writeAll("\"];\n");
}

fn writeEdgeDot(writer: anytype, edge: Database.EdgeInfo) !void {
    try writer.print("  n{d} -> n{d} [label=\"", .{ edge.source, edge.target });
    try writeDotStringContent(writer, edge.edge_type);
    try writer.writeAll("\"];\n");
}

fn writePropertyValueJson(writer: anytype, val: PropertyValue) !void {
    switch (val) {
        .null_val => try writer.writeAll("null"),
        .bool_val => |b| try writer.writeAll(if (b) "true" else "false"),
        .int_val => |i| try writer.print("{d}", .{i}),
        .float_val => |f| try writer.print("{d}", .{f}),
        .string_val => |s| try writeJsonString(writer, s),
        .bytes_val => |bytes| {
            // Encode bytes as hex string
            try writer.writeByte('"');
            for (bytes) |b| {
                try writer.print("{x:0>2}", .{b});
            }
            try writer.writeByte('"');
        },
        .vector_val => |vec| {
            // Output vector as JSON array of floats
            try writer.writeByte('[');
            for (vec, 0..) |f, i| {
                if (i > 0) try writer.writeByte(',');
                try writer.print("{d}", .{f});
            }
            try writer.writeByte(']');
        },
        .list_val => |list| {
            try writer.writeByte('[');
            for (list, 0..) |item, i| {
                if (i > 0) try writer.writeByte(',');
                try writePropertyValueJson(writer, item);
            }
            try writer.writeByte(']');
        },
        .map_val => |entries| {
            try writer.writeByte('{');
            for (entries, 0..) |entry, i| {
                if (i > 0) try writer.writeByte(',');
                try writeJsonString(writer, entry.key);
                try writer.writeByte(':');
                try writePropertyValueJson(writer, entry.value);
            }
            try writer.writeByte('}');
        },
    }
}

fn writePropertyValueJsonCanonical(
    allocator: std.mem.Allocator,
    writer: anytype,
    val: PropertyValue,
) !void {
    switch (val) {
        .null_val => try writer.writeAll("null"),
        .bool_val => |b| try writer.writeAll(if (b) "true" else "false"),
        .int_val => |i| try writer.print("{d}", .{i}),
        .float_val => |f| try writer.print("{d}", .{f}),
        .string_val => |s| try writeJsonString(writer, s),
        .bytes_val => |bytes| {
            try writer.writeByte('"');
            for (bytes) |b| try writer.print("{x:0>2}", .{b});
            try writer.writeByte('"');
        },
        .vector_val => |vec| {
            try writer.writeByte('[');
            for (vec, 0..) |f, i| {
                if (i > 0) try writer.writeByte(',');
                try writer.print("{d}", .{f});
            }
            try writer.writeByte(']');
        },
        .list_val => |list| {
            try writer.writeByte('[');
            for (list, 0..) |item, i| {
                if (i > 0) try writer.writeByte(',');
                try writePropertyValueJsonCanonical(allocator, writer, item);
            }
            try writer.writeByte(']');
        },
        .map_val => |entries| {
            const sorted_entries = try allocator.dupe(PropertyValue.MapEntry, entries);
            defer allocator.free(sorted_entries);
            std.mem.sort(PropertyValue.MapEntry, sorted_entries, {}, struct {
                fn lessThan(_: void, a: PropertyValue.MapEntry, b: PropertyValue.MapEntry) bool {
                    return std.mem.order(u8, a.key, b.key) == .lt;
                }
            }.lessThan);

            try writer.writeByte('{');
            for (sorted_entries, 0..) |entry, i| {
                if (i > 0) try writer.writeByte(',');
                try writeJsonString(writer, entry.key);
                try writer.writeByte(':');
                try writePropertyValueJsonCanonical(allocator, writer, entry.value);
            }
            try writer.writeByte('}');
        },
    }
}

fn writeSortedPropertyEntriesJson(
    allocator: std.mem.Allocator,
    writer: anytype,
    props: []const Database.PropertyEntry,
) !void {
    const sorted_props = try allocator.dupe(Database.PropertyEntry, props);
    defer allocator.free(sorted_props);
    std.mem.sort(Database.PropertyEntry, sorted_props, {}, struct {
        fn lessThan(_: void, a: Database.PropertyEntry, b: Database.PropertyEntry) bool {
            return std.mem.order(u8, a.key, b.key) == .lt;
        }
    }.lessThan);

    for (sorted_props, 0..) |prop, i| {
        if (i > 0) try writer.writeByte(',');
        try writeJsonString(writer, prop.key);
        try writer.writeByte(':');
        try writePropertyValueJsonCanonical(allocator, writer, prop.value);
    }
}

fn collectCanonicalNodeIds(
    allocator: std.mem.Allocator,
    db: *Database,
    label_filter: ?[]const u8,
) ![]NodeId {
    var node_ids: std.ArrayList(NodeId) = .empty;
    errdefer node_ids.deinit(allocator);

    if (label_filter) |filter| {
        var seen = std.AutoHashMap(NodeId, void).init(allocator);
        defer seen.deinit();

        var filters = std.mem.splitScalar(u8, filter, ',');
        while (filters.next()) |raw_label| {
            const label = std.mem.trim(u8, raw_label, " \t\r");
            if (label.len == 0) continue;

            const filtered = db.getNodesByLabel(label) catch |err| return mapDatabaseError(err);
            defer allocator.free(filtered);

            for (filtered) |node_id| {
                const gop = seen.getOrPut(node_id) catch return ImportExportError.OutOfMemory;
                if (gop.found_existing) continue;
                try node_ids.append(allocator, node_id);
            }
        }
    } else {
        var iter = db.node_tree.range(null, null) catch return ImportExportError.DatabaseError;
        defer iter.deinit();

        while (true) {
            const entry = iter.next() catch return ImportExportError.DatabaseError;
            if (entry) |e| {
                if (e.key.len == 8) {
                    try node_ids.append(allocator, std.mem.readInt(u64, e.key[0..8], .little));
                }
            } else {
                break;
            }
        }
    }

    const owned = try node_ids.toOwnedSlice(allocator);
    std.mem.sort(NodeId, owned, {}, std.sort.asc(NodeId));
    return owned;
}

/// Import data from a CSV file.
/// CSV format for nodes: _id,_labels,prop1,prop2,...
/// CSV format for edges: _source,_target,_type,prop1,prop2,...
pub fn importCsv(
    allocator: std.mem.Allocator,
    db: *Database,
    file_path: []const u8,
    batch_size: u32,
    on_error_skip: bool,
) ImportExportError!ImportStats {
    const file = std.fs.cwd().openFile(file_path, .{}) catch {
        return ImportExportError.IoError;
    };
    defer file.close();

    const content = file.readToEndAlloc(allocator, 100 * 1024 * 1024) catch {
        return ImportExportError.IoError;
    };
    defer allocator.free(content);

    // Detect if this is a nodes or edges CSV by checking the header
    var lines = std.mem.splitScalar(u8, content, '\n');
    const header_line = lines.next() orelse return ImportExportError.ParseError;

    if (std.mem.startsWith(u8, header_line, "_id,_labels")) {
        return importNodesCsv(allocator, db, content, batch_size, on_error_skip);
    } else if (std.mem.startsWith(u8, header_line, "_source,_target,_type")) {
        return importEdgesCsv(db, content, batch_size, on_error_skip);
    } else {
        return ImportExportError.InvalidFormat;
    }
}

fn importNodesCsv(
    allocator: std.mem.Allocator,
    db: *Database,
    content: []const u8,
    batch_size: u32,
    on_error_skip: bool,
) ImportExportError!ImportStats {
    var stats = ImportStats{};
    var batcher = ImportBatcher.init(db, batch_size, on_error_skip);
    errdefer if (batcher.hasPendingTransaction()) batcher.abort() catch {};

    var lines = std.mem.splitScalar(u8, content, '\n');

    // Parse header
    const header_line = lines.next() orelse return ImportExportError.ParseError;
    var headers = ManagedArrayList([]const u8).init(allocator);
    defer headers.deinit();

    var header_parts = std.mem.splitScalar(u8, header_line, ',');
    while (header_parts.next()) |part| {
        headers.append(std.mem.trim(u8, part, " \t\r")) catch {
            return ImportExportError.OutOfMemory;
        };
    }

    // Process data lines
    while (lines.next()) |line| {
        const trimmed = std.mem.trim(u8, line, " \t\r");
        if (trimmed.len == 0) continue;

        const txn = try batcher.transaction();
        if (importNodeCsvLine(allocator, db, txn, trimmed, headers.items)) |_| {
            try batcher.recordSuccess(&stats, .node);
        } else |_| {
            try batcher.recordFailure(&stats, .node);
            if (!on_error_skip) {
                return ImportExportError.DatabaseError;
            }
        }
    }

    try batcher.flush(&stats);
    return stats;
}

fn importNodeCsvLine(
    allocator: std.mem.Allocator,
    db: *Database,
    txn: ?*Transaction,
    line: []const u8,
    headers: []const []const u8,
) !void {
    var values = ManagedArrayList([]const u8).init(allocator);
    defer values.deinit();

    // Parse CSV values (simple, doesn't handle quoted values with commas)
    var parts = std.mem.splitScalar(u8, line, ',');
    while (parts.next()) |part| {
        try values.append(std.mem.trim(u8, part, " \t\r\""));
    }

    if (values.items.len < 2) return error.InvalidFormat;

    // Parse labels (semicolon-separated)
    var labels_list = ManagedArrayList([]const u8).init(allocator);
    defer labels_list.deinit();

    if (values.items.len > 1 and values.items[1].len > 0) {
        var label_parts = std.mem.splitScalar(u8, values.items[1], ';');
        while (label_parts.next()) |label| {
            const trimmed = std.mem.trim(u8, label, " ");
            if (trimmed.len > 0) {
                try labels_list.append(trimmed);
            }
        }
    }

    // Create node
    const node_id = db.createNode(txn, labels_list.items) catch {
        return error.DatabaseError;
    };

    // Set properties (columns after _id and _labels)
    for (headers[2..], 2..) |header, i| {
        if (i < values.items.len and values.items[i].len > 0) {
            const val = values.items[i];
            // Try to parse as number, otherwise use as string
            const prop_val: PropertyValue = if (std.fmt.parseInt(i64, val, 10)) |int_val|
                .{ .int_val = int_val }
            else |_| if (std.fmt.parseFloat(f64, val)) |float_val|
                .{ .float_val = float_val }
            else |_|
                .{ .string_val = val };

            db.setNodeProperty(txn, node_id, header, prop_val) catch continue;
        }
    }
}

fn importEdgesCsv(
    db: *Database,
    content: []const u8,
    batch_size: u32,
    on_error_skip: bool,
) ImportExportError!ImportStats {
    var stats = ImportStats{};
    var batcher = ImportBatcher.init(db, batch_size, on_error_skip);
    errdefer if (batcher.hasPendingTransaction()) batcher.abort() catch {};

    var lines = std.mem.splitScalar(u8, content, '\n');

    // Skip header
    _ = lines.next();

    // Process data lines
    while (lines.next()) |line| {
        const trimmed = std.mem.trim(u8, line, " \t\r");
        if (trimmed.len == 0) continue;

        const txn = try batcher.transaction();
        if (importEdgeCsvLine(db, txn, trimmed)) |_| {
            try batcher.recordSuccess(&stats, .edge);
        } else |_| {
            try batcher.recordFailure(&stats, .edge);
            if (!on_error_skip) {
                return ImportExportError.DatabaseError;
            }
        }
    }

    try batcher.flush(&stats);
    return stats;
}

fn importEdgeCsvLine(db: *Database, txn: ?*Transaction, line: []const u8) !void {
    var parts = std.mem.splitScalar(u8, line, ',');

    const source_str = std.mem.trim(u8, parts.next() orelse return error.InvalidFormat, " \t\r\"");
    const target_str = std.mem.trim(u8, parts.next() orelse return error.InvalidFormat, " \t\r\"");
    const edge_type = std.mem.trim(u8, parts.next() orelse return error.InvalidFormat, " \t\r\"");

    // Parse node IDs
    const source_id = std.fmt.parseInt(NodeId, source_str, 10) catch {
        return error.InvalidNodeId;
    };
    const target_id = std.fmt.parseInt(NodeId, target_str, 10) catch {
        return error.InvalidNodeId;
    };

    db.createEdge(txn, source_id, target_id, edge_type) catch {
        return error.DatabaseError;
    };
}

/// Export to CSV format
pub fn exportCsv(
    allocator: std.mem.Allocator,
    db: *Database,
    nodes_writer: anytype,
    edges_writer: anytype,
    label_filter: ?[]const u8,
) !ExportStats {
    var stats = ExportStats{};
    const node_ids = try collectCanonicalNodeIds(allocator, db, label_filter);
    defer allocator.free(node_ids);

    // Write nodes header
    try nodes_writer.writeAll("_id,_labels\n");

    for (node_ids) |node_id| {
        const node_labels = db.getNodeLabels(node_id) catch |err| return mapDatabaseError(err);
        defer {
            for (node_labels) |label| {
                allocator.free(label);
            }
            allocator.free(node_labels);
        }

        try nodes_writer.print("{d},\"", .{node_id});
        for (node_labels, 0..) |label, i| {
            if (i > 0) try nodes_writer.writeAll(";");
            try nodes_writer.writeAll(label);
        }
        try nodes_writer.writeAll("\"\n");
        stats.nodes_exported += 1;
    }

    // Write edges header
    try edges_writer.writeAll("_source,_target,_type\n");

    for (node_ids) |node_id| {
        const edges = db.getOutgoingEdges(node_id) catch continue;
        defer db.freeEdgeInfos(edges);

        for (edges) |edge| {
            try edges_writer.print("{d},{d},{s}\n", .{
                edge.source,
                edge.target,
                edge.edge_type,
            });
            stats.edges_exported += 1;
        }
    }

    return stats;
}

/// Import statistics
pub const ImportStats = struct {
    nodes_imported: u64 = 0,
    nodes_failed: u64 = 0,
    edges_imported: u64 = 0,
    edges_failed: u64 = 0,
};

/// Export statistics
pub const ExportStats = struct {
    nodes_exported: u64 = 0,
    edges_exported: u64 = 0,
};

fn cleanupTestDatabaseFiles(path: []const u8) void {
    std.fs.cwd().deleteFile(path) catch {};

    var wal_path_buf: [512]u8 = undefined;
    const wal_path = std.fmt.bufPrint(&wal_path_buf, "{s}-wal", .{path}) catch return;
    std.fs.cwd().deleteFile(wal_path) catch {};
}

test "import/export JSON batching rolls back failed batch" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_import_json_batch_rollback.ltdb";
    cleanupTestDatabaseFiles(path);

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{
            .enable_fts = false,
        },
    });
    defer {
        db.close();
        cleanupTestDatabaseFiles(path);
    }

    const json_content =
        \\{"nodes":[
        \\  {"id":"n1","labels":["Person"],"properties":{"name":"Alice"}},
        \\  {"labels":["Person"],"properties":{"name":"Broken"}}
        \\]}
    ;

    try std.testing.expectError(
        ImportExportError.DatabaseError,
        importJsonContent(allocator, &db, json_content, 2, false),
    );
    try std.testing.expectEqual(@as(u64, 0), db.nodeCount());
}

test "import/export JSON skip preserves valid records" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_import_json_skip.ltdb";
    cleanupTestDatabaseFiles(path);

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{
            .enable_fts = false,
        },
    });
    defer {
        db.close();
        cleanupTestDatabaseFiles(path);
    }

    const json_content =
        \\{"nodes":[
        \\  {"id":"n1","labels":["Person"],"properties":{"name":"Alice"}},
        \\  {"labels":["Person"],"properties":{"name":"Broken"}},
        \\  {"id":"n2","labels":["Person"],"properties":{"name":"Bob"}}
        \\]}
    ;

    const stats = try importJsonContent(allocator, &db, json_content, 2, true);
    try std.testing.expectEqual(@as(u64, 2), stats.nodes_imported);
    try std.testing.expectEqual(@as(u64, 1), stats.nodes_failed);
    try std.testing.expectEqual(@as(u64, 2), db.nodeCount());
}

test "import/export CSV batching rolls back failed batch" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_import_csv_batch_rollback.ltdb";
    cleanupTestDatabaseFiles(path);

    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{
            .enable_fts = false,
        },
    });
    defer {
        db.close();
        cleanupTestDatabaseFiles(path);
    }

    const csv_content =
        \\_id,_labels,name
        \\n1,Person,Alice
        \\bad
    ;

    try std.testing.expectError(
        ImportExportError.DatabaseError,
        importNodesCsv(allocator, &db, csv_content, 2, false),
    );
    try std.testing.expectEqual(@as(u64, 0), db.nodeCount());
}

test "import/export JSON roundtrip" {
    const allocator = std.testing.allocator;

    // Create a test database
    const path = "/tmp/lattice_import_export_test.ltdb";
    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{
            .enable_wal = false,
            .enable_fts = false,
        },
    });
    defer {
        db.close();
        std.fs.cwd().deleteFile(path) catch {};
    }

    // Import JSON
    const json_content =
        \\{"nodes":[
        \\  {"id":"n1","labels":["Person"],"properties":{"name":"Alice","age":30}},
        \\  {"id":"n2","labels":["Person"],"properties":{"name":"Bob","age":25}}
        \\],"edges":[
        \\  {"source":"n1","target":"n2","type":"KNOWS","properties":{"since":2020,"status":"active"}}
        \\]}
    ;

    const import_stats = try importJsonContent(allocator, &db, json_content, 1000, false);
    try std.testing.expectEqual(@as(u64, 2), import_stats.nodes_imported);
    try std.testing.expectEqual(@as(u64, 1), import_stats.edges_imported);

    // Verify data
    try std.testing.expectEqual(@as(u64, 2), db.nodeCount());
    try std.testing.expectEqual(@as(u64, 1), db.edgeCount());

    const edges = try db.getOutgoingEdges(1);
    defer db.freeEdgeInfos(edges);
    try std.testing.expectEqual(@as(usize, 1), edges.len);

    const since = try db.getEdgePropertyById(edges[0].id, "since");
    try std.testing.expect(since != null);
    try std.testing.expectEqual(@as(i64, 2020), since.?.int_val);
    var owned_since = since.?;
    owned_since.deinit(allocator);

    const status = try db.getEdgePropertyById(edges[0].id, "status");
    try std.testing.expect(status != null);
    try std.testing.expectEqualStrings("active", status.?.string_val);
    var owned_status = status.?;
    owned_status.deinit(allocator);
}

test "import_export: dumpCanonicalJson is deterministic and includes unlabeled nodes" {
    const allocator = std.testing.allocator;
    const path_a = "/tmp/lattice_dump_canonical_a.ltdb";
    const path_b = "/tmp/lattice_dump_canonical_b.ltdb";

    std.fs.cwd().deleteFile(path_a) catch {};
    std.fs.cwd().deleteFile(path_b) catch {};

    var db_a = try Database.open(allocator, path_a, .{
        .create = true,
        .config = .{ .enable_wal = false, .enable_fts = false },
    });
    defer {
        db_a.close();
        std.fs.cwd().deleteFile(path_a) catch {};
    }

    var db_b = try Database.open(allocator, path_b, .{
        .create = true,
        .config = .{ .enable_wal = false, .enable_fts = false },
    });
    defer {
        db_b.close();
        std.fs.cwd().deleteFile(path_b) catch {};
    }

    const alice_a = try db_a.createNode(null, &.{ "Person", "Employee" });
    const unlabeled_a = try db_a.createNode(null, &.{});
    try db_a.setNodeProperty(null, alice_a, "zeta", .{ .int_val = 1 });
    try db_a.setNodeProperty(null, alice_a, "alpha", .{ .string_val = "A" });
    try db_a.setNodeProperty(null, unlabeled_a, "meta", .{
        .map_val = &.{
            .{ .key = "z", .value = .{ .int_val = 2 } },
            .{ .key = "a", .value = .{ .string_val = "x" } },
        },
    });
    const edge_a = try db_a.createEdgeAndGetId(null, alice_a, unlabeled_a, "REL");
    try db_a.setEdgePropertyById(null, edge_a, "status", .{ .string_val = "active" });
    try db_a.setEdgePropertyById(null, edge_a, "since", .{ .int_val = 2020 });

    const alice_b = try db_b.createNode(null, &.{ "Employee", "Person" });
    const unlabeled_b = try db_b.createNode(null, &.{});
    try db_b.setNodeProperty(null, alice_b, "alpha", .{ .string_val = "A" });
    try db_b.setNodeProperty(null, alice_b, "zeta", .{ .int_val = 1 });
    try db_b.setNodeProperty(null, unlabeled_b, "meta", .{
        .map_val = &.{
            .{ .key = "a", .value = .{ .string_val = "x" } },
            .{ .key = "z", .value = .{ .int_val = 2 } },
        },
    });
    const edge_b = try db_b.createEdgeAndGetId(null, alice_b, unlabeled_b, "REL");
    try db_b.setEdgePropertyById(null, edge_b, "since", .{ .int_val = 2020 });
    try db_b.setEdgePropertyById(null, edge_b, "status", .{ .string_val = "active" });

    var buf_a: [8192]u8 = undefined;
    var stream_a = std.io.fixedBufferStream(&buf_a);
    const stats_a = try dumpCanonicalJson(allocator, db_a, stream_a.writer(), null);
    try std.testing.expectEqual(@as(u64, 2), stats_a.nodes_exported);
    try std.testing.expectEqual(@as(u64, 1), stats_a.edges_exported);

    var buf_b: [8192]u8 = undefined;
    var stream_b = std.io.fixedBufferStream(&buf_b);
    const stats_b = try dumpCanonicalJson(allocator, db_b, stream_b.writer(), null);
    try std.testing.expectEqual(@as(u64, 2), stats_b.nodes_exported);
    try std.testing.expectEqual(@as(u64, 1), stats_b.edges_exported);

    const out_a = buf_a[0..stream_a.pos];
    const out_b = buf_b[0..stream_b.pos];
    try std.testing.expectEqualStrings(out_a, out_b);
    try std.testing.expect(std.mem.indexOf(u8, out_a, "\"labels\":[]") != null);
    try std.testing.expect(std.mem.indexOf(u8, out_a, "\"id\":\"1\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, out_a, "\"id\":\"1\",\"source\":\"1\",\"target\":\"2\"") != null);
}
