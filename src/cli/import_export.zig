//! Import/Export functionality for LatticeDB CLI.
//!
//! Supports JSON, JSONL, CSV, and DOT formats for importing/exporting graph data.

const std = @import("std");
const lattice = @import("lattice");

const Database = lattice.storage.database.Database;
const PropertyValue = lattice.core.types.PropertyValue;
const NodeId = lattice.core.types.NodeId;

/// Managed array list for allocator tracking
fn ManagedArrayList(comptime T: type) type {
    return std.array_list.Managed(T);
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
    _ = batch_size; // TODO: implement batching

    const file = std.fs.cwd().openFile(file_path, .{}) catch {
        return ImportExportError.IoError;
    };
    defer file.close();

    const content = file.readToEndAlloc(allocator, 100 * 1024 * 1024) catch {
        return ImportExportError.IoError;
    };
    defer allocator.free(content);

    return importJsonContent(allocator, db, content, on_error_skip);
}

/// Import JSON from a string content
pub fn importJsonContent(
    allocator: std.mem.Allocator,
    db: *Database,
    content: []const u8,
    on_error_skip: bool,
) ImportExportError!ImportStats {
    var stats = ImportStats{};

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
                if (importNode(allocator, db, node_val, &id_map)) |_| {
                    stats.nodes_imported += 1;
                } else |_| {
                    stats.nodes_failed += 1;
                    if (!on_error_skip) {
                        return ImportExportError.DatabaseError;
                    }
                }
            }
        }
    }

    // Import edges
    if (root.object.get("edges")) |edges_val| {
        if (edges_val == .array) {
            for (edges_val.array.items) |edge_val| {
                if (importEdge(db, edge_val, &id_map)) |_| {
                    stats.edges_imported += 1;
                } else |_| {
                    stats.edges_failed += 1;
                    if (!on_error_skip) {
                        return ImportExportError.DatabaseError;
                    }
                }
            }
        }
    }

    return stats;
}

fn importNode(
    allocator: std.mem.Allocator,
    db: *Database,
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
    const node_id = db.createNode(null, labels_list.items) catch {
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
                db.setNodeProperty(null, node_id, entry.key_ptr.*, prop_val) catch continue;
            }
        }
    }
}

fn importEdge(
    db: *Database,
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
    const edge_id = db.createEdgeAndGetId(null, source_id, target_id, edge_type) catch {
        return error.DatabaseError;
    };

    if (obj.get("properties")) |props_val| {
        if (props_val == .object) {
            var iter = props_val.object.iterator();
            while (iter.next()) |entry| {
                const prop_val = jsonToPropertyValue(entry.value_ptr.*) catch continue;
                db.setEdgePropertyById(null, edge_id, entry.key_ptr.*, prop_val) catch continue;
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

    try writer.writeAll("{\"nodes\":[");

    // Get all labels or apply filter.
    const labels = db.getAllLabels() catch {
        return ImportExportError.DatabaseError;
    };
    defer db.freeLabelInfos(labels);

    // Export each node once, even when it has multiple labels.
    var seen_nodes = std.AutoHashMap(NodeId, void).init(allocator);
    defer seen_nodes.deinit();

    var first_node = true;
    for (labels) |label_info| {
        if (label_filter) |filter| {
            if (!std.mem.eql(u8, label_info.name, filter)) continue;
        }

        const nodes = db.getNodesByLabel(label_info.name) catch continue;
        defer allocator.free(nodes);

        for (nodes) |node_id| {
            const node_gop = seen_nodes.getOrPut(node_id) catch return ImportExportError.OutOfMemory;
            if (node_gop.found_existing) continue;

            if (!first_node) {
                try writer.writeAll(",");
            }
            first_node = false;

            try writeNodeJson(allocator, db, writer, node_id);
            stats.nodes_exported += 1;
        }
    }

    try writer.writeAll("],\"edges\":[");

    // Export edges
    var first_edge = true;
    // Visit each source node once to avoid duplicate edges from multi-label nodes.
    var seen_sources = std.AutoHashMap(NodeId, void).init(allocator);
    defer seen_sources.deinit();

    for (labels) |label_info| {
        if (label_filter) |filter| {
            if (!std.mem.eql(u8, label_info.name, filter)) continue;
        }

        const nodes = db.getNodesByLabel(label_info.name) catch continue;
        defer allocator.free(nodes);

        for (nodes) |node_id| {
            const source_gop = seen_sources.getOrPut(node_id) catch return ImportExportError.OutOfMemory;
            if (source_gop.found_existing) continue;

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

    const labels = db.getAllLabels() catch {
        return ImportExportError.DatabaseError;
    };
    defer db.freeLabelInfos(labels);

    // Emit node records first (deduplicated by node ID).
    var seen_nodes = std.AutoHashMap(NodeId, void).init(allocator);
    defer seen_nodes.deinit();

    for (labels) |label_info| {
        if (label_filter) |filter| {
            if (!std.mem.eql(u8, label_info.name, filter)) continue;
        }

        const nodes = db.getNodesByLabel(label_info.name) catch continue;
        defer allocator.free(nodes);

        for (nodes) |node_id| {
            const node_gop = seen_nodes.getOrPut(node_id) catch return ImportExportError.OutOfMemory;
            if (node_gop.found_existing) continue;

            try writeNodeJsonlRecord(allocator, db, writer, node_id);
            try writer.writeByte('\n');
            stats.nodes_exported += 1;
        }
    }

    // Emit edge records once per source node.
    var seen_sources = std.AutoHashMap(NodeId, void).init(allocator);
    defer seen_sources.deinit();

    for (labels) |label_info| {
        if (label_filter) |filter| {
            if (!std.mem.eql(u8, label_info.name, filter)) continue;
        }

        const nodes = db.getNodesByLabel(label_info.name) catch continue;
        defer allocator.free(nodes);

        for (nodes) |node_id| {
            const source_gop = seen_sources.getOrPut(node_id) catch return ImportExportError.OutOfMemory;
            if (source_gop.found_existing) continue;

            const edges = db.getOutgoingEdges(node_id) catch continue;
            defer db.freeEdgeInfos(edges);

            for (edges) |edge| {
                try writeEdgeJsonlRecord(db, writer, edge);
                try writer.writeByte('\n');
                stats.edges_exported += 1;
            }
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
    try writer.writeAll("digraph G {\n");

    const labels = db.getAllLabels() catch {
        return ImportExportError.DatabaseError;
    };
    defer db.freeLabelInfos(labels);

    // Emit each node once.
    var seen_nodes = std.AutoHashMap(NodeId, void).init(allocator);
    defer seen_nodes.deinit();

    for (labels) |label_info| {
        if (label_filter) |filter| {
            if (!std.mem.eql(u8, label_info.name, filter)) continue;
        }

        const nodes = db.getNodesByLabel(label_info.name) catch continue;
        defer allocator.free(nodes);

        for (nodes) |node_id| {
            const node_gop = seen_nodes.getOrPut(node_id) catch return ImportExportError.OutOfMemory;
            if (node_gop.found_existing) continue;

            try writeNodeDot(allocator, db, writer, node_id);
            stats.nodes_exported += 1;
        }
    }

    // Emit edges once per source node.
    var seen_sources = std.AutoHashMap(NodeId, void).init(allocator);
    defer seen_sources.deinit();

    for (labels) |label_info| {
        if (label_filter) |filter| {
            if (!std.mem.eql(u8, label_info.name, filter)) continue;
        }

        const nodes = db.getNodesByLabel(label_info.name) catch continue;
        defer allocator.free(nodes);

        for (nodes) |node_id| {
            const source_gop = seen_sources.getOrPut(node_id) catch return ImportExportError.OutOfMemory;
            if (source_gop.found_existing) continue;

            const edges = db.getOutgoingEdges(node_id) catch continue;
            defer db.freeEdgeInfos(edges);

            for (edges) |edge| {
                try writeEdgeDot(writer, edge);
                stats.edges_exported += 1;
            }
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
    const node_labels = db.getNodeLabels(node_id) catch &[_][]const u8{};
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
    const props = db.getNodeProperties(node_id) catch &[_]Database.PropertyEntry{};
    defer db.freePropertyEntries(@constCast(props));

    for (props, 0..) |prop, i| {
        if (i > 0) try writer.writeAll(",");
        try writeJsonString(writer, prop.key);
        try writer.writeByte(':');
        try writePropertyValueJson(writer, prop.value);
    }

    try writer.writeAll("}}");
}

fn writeEdgeJson(db: *Database, writer: anytype, edge: Database.EdgeInfo) !void {
    try writer.print("{{\"source\":\"{d}\",\"target\":\"{d}\",\"type\":", .{ edge.source, edge.target });
    try writeJsonString(writer, edge.edge_type);
    try writer.writeAll(",\"properties\":{");

    const maybe_props = db.getEdgeProperties(edge.id) catch null;
    defer if (maybe_props) |props| db.freePropertyEntries(props);

    if (maybe_props) |props| {
        for (props, 0..) |prop, i| {
            if (i > 0) try writer.writeAll(",");
            try writeJsonString(writer, prop.key);
            try writer.writeByte(':');
            try writePropertyValueJson(writer, prop.value);
        }
    }

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

    const node_labels = db.getNodeLabels(node_id) catch &[_][]const u8{};
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

    const props = db.getNodeProperties(node_id) catch &[_]Database.PropertyEntry{};
    defer db.freePropertyEntries(@constCast(props));

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

    const maybe_props = db.getEdgeProperties(edge.id) catch null;
    defer if (maybe_props) |props| db.freePropertyEntries(props);

    if (maybe_props) |props| {
        for (props, 0..) |prop, i| {
            if (i > 0) try writer.writeAll(",");
            try writeJsonString(writer, prop.key);
            try writer.writeByte(':');
            try writePropertyValueJson(writer, prop.value);
        }
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

    const node_labels = db.getNodeLabels(node_id) catch &[_][]const u8{};
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
    _ = batch_size;

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
        return importNodesCsv(allocator, db, content, on_error_skip);
    } else if (std.mem.startsWith(u8, header_line, "_source,_target,_type")) {
        return importEdgesCsv(allocator, db, content, on_error_skip);
    } else {
        return ImportExportError.InvalidFormat;
    }
}

fn importNodesCsv(
    allocator: std.mem.Allocator,
    db: *Database,
    content: []const u8,
    on_error_skip: bool,
) ImportExportError!ImportStats {
    var stats = ImportStats{};

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

        if (importNodeCsvLine(allocator, db, trimmed, headers.items)) |_| {
            stats.nodes_imported += 1;
        } else |_| {
            stats.nodes_failed += 1;
            if (!on_error_skip) {
                return ImportExportError.DatabaseError;
            }
        }
    }

    return stats;
}

fn importNodeCsvLine(
    allocator: std.mem.Allocator,
    db: *Database,
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
    const node_id = db.createNode(null, labels_list.items) catch {
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

            db.setNodeProperty(null, node_id, header, prop_val) catch continue;
        }
    }
}

fn importEdgesCsv(
    allocator: std.mem.Allocator,
    db: *Database,
    content: []const u8,
    on_error_skip: bool,
) ImportExportError!ImportStats {
    _ = allocator;
    var stats = ImportStats{};

    var lines = std.mem.splitScalar(u8, content, '\n');

    // Skip header
    _ = lines.next();

    // Process data lines
    while (lines.next()) |line| {
        const trimmed = std.mem.trim(u8, line, " \t\r");
        if (trimmed.len == 0) continue;

        if (importEdgeCsvLine(db, trimmed)) |_| {
            stats.edges_imported += 1;
        } else |_| {
            stats.edges_failed += 1;
            if (!on_error_skip) {
                return ImportExportError.DatabaseError;
            }
        }
    }

    return stats;
}

fn importEdgeCsvLine(db: *Database, line: []const u8) !void {
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

    db.createEdge(null, source_id, target_id, edge_type) catch {
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

    // Write nodes header
    try nodes_writer.writeAll("_id,_labels\n");

    // Get all labels
    const labels = db.getAllLabels() catch {
        return ImportExportError.DatabaseError;
    };
    defer db.freeLabelInfos(labels);

    // Export nodes (deduplicated by node ID).
    var seen_nodes = std.AutoHashMap(NodeId, void).init(allocator);
    defer seen_nodes.deinit();

    for (labels) |label_info| {
        if (label_filter) |filter| {
            if (!std.mem.eql(u8, label_info.name, filter)) continue;
        }

        const nodes = db.getNodesByLabel(label_info.name) catch continue;
        defer allocator.free(nodes);

        for (nodes) |node_id| {
            const node_gop = seen_nodes.getOrPut(node_id) catch return ImportExportError.OutOfMemory;
            if (node_gop.found_existing) continue;

            // Get all labels for this node
            const node_labels = db.getNodeLabels(node_id) catch &[_][]const u8{};
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
    }

    // Write edges header
    try edges_writer.writeAll("_source,_target,_type\n");

    // Export edges once per source node.
    var seen_sources = std.AutoHashMap(NodeId, void).init(allocator);
    defer seen_sources.deinit();

    for (labels) |label_info| {
        if (label_filter) |filter| {
            if (!std.mem.eql(u8, label_info.name, filter)) continue;
        }

        const nodes = db.getNodesByLabel(label_info.name) catch continue;
        defer allocator.free(nodes);

        for (nodes) |node_id| {
            const source_gop = seen_sources.getOrPut(node_id) catch return ImportExportError.OutOfMemory;
            if (source_gop.found_existing) continue;

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

    const import_stats = try importJsonContent(allocator, &db, json_content, false);
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
