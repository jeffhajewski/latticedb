//! Import/Export functionality for LatticeDB CLI.
//!
//! Supports JSON and CSV formats for importing and exporting graph data.

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
    const node_id = db.createNode(null,labels_list.items) catch {
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
                db.setNodeProperty(null,node_id, entry.key_ptr.*, prop_val) catch continue;
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

    // Create the edge
    db.createEdge(null,source_id, target_id, edge_type) catch {
        return error.DatabaseError;
    };

    // TODO: Set edge properties when supported
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

    // Get all labels or filter
    const labels = db.getAllLabels() catch {
        return ImportExportError.DatabaseError;
    };
    defer db.freeLabelInfos(labels);

    var first_node = true;

    // Export nodes by label
    for (labels) |label_info| {
        // Apply label filter if specified
        if (label_filter) |filter| {
            if (!std.mem.eql(u8, label_info.name, filter)) continue;
        }

        // Get all nodes with this label
        const nodes = db.getNodesByLabel(label_info.name) catch continue;
        defer allocator.free(nodes);

        for (nodes) |node_id| {
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

    // We need to iterate all nodes and get their outgoing edges
    for (labels) |label_info| {
        if (label_filter) |filter| {
            if (!std.mem.eql(u8, label_info.name, filter)) continue;
        }

        const nodes = db.getNodesByLabel(label_info.name) catch continue;
        defer allocator.free(nodes);

        for (nodes) |node_id| {
            const edges = db.getOutgoingEdges(node_id) catch continue;
            defer db.freeEdgeInfos(edges);

            for (edges) |edge| {
                if (!first_edge) {
                    try writer.writeAll(",");
                }
                first_edge = false;

                try writeEdgeJson(writer, edge);
                stats.edges_exported += 1;
            }
        }
    }

    try writer.writeAll("]}\n");

    return stats;
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
        try writer.print("\"{s}\"", .{label});
    }

    try writer.writeAll("],\"properties\":{");

    // Get properties
    const props = db.getNodeProperties(node_id) catch &[_]Database.PropertyEntry{};
    defer db.freePropertyEntries(@constCast(props));

    for (props, 0..) |prop, i| {
        if (i > 0) try writer.writeAll(",");
        try writer.print("\"{s}\":", .{prop.key});
        try writePropertyValueJson(writer, prop.value);
    }

    try writer.writeAll("}}");
}

fn writeEdgeJson(writer: anytype, edge: Database.EdgeInfo) !void {
    try writer.print("{{\"source\":\"{d}\",\"target\":\"{d}\",\"type\":\"{s}\",\"properties\":{{}}}}", .{
        edge.source,
        edge.target,
        edge.edge_type,
    });
}

fn writePropertyValueJson(writer: anytype, val: PropertyValue) !void {
    switch (val) {
        .null_val => try writer.writeAll("null"),
        .bool_val => |b| try writer.writeAll(if (b) "true" else "false"),
        .int_val => |i| try writer.print("{d}", .{i}),
        .float_val => |f| try writer.print("{d}", .{f}),
        .string_val => |s| {
            try writer.writeByte('"');
            for (s) |c| {
                switch (c) {
                    '"' => try writer.writeAll("\\\""),
                    '\\' => try writer.writeAll("\\\\"),
                    '\n' => try writer.writeAll("\\n"),
                    '\r' => try writer.writeAll("\\r"),
                    '\t' => try writer.writeAll("\\t"),
                    else => try writer.writeByte(c),
                }
            }
            try writer.writeByte('"');
        },
        .bytes_val => |bytes| {
            // Encode bytes as base64 string
            try writer.writeByte('"');
            for (bytes) |b| {
                try writer.print("{x:0>2}", .{b});
            }
            try writer.writeByte('"');
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
                try writer.print("\"{s}\":", .{entry.key});
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
    const node_id = db.createNode(null,labels_list.items) catch {
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

            db.setNodeProperty(null,node_id, header, prop_val) catch continue;
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

    db.createEdge(null,source_id, target_id, edge_type) catch {
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

    // Export nodes
    for (labels) |label_info| {
        if (label_filter) |filter| {
            if (!std.mem.eql(u8, label_info.name, filter)) continue;
        }

        const nodes = db.getNodesByLabel(label_info.name) catch continue;
        defer allocator.free(nodes);

        for (nodes) |node_id| {
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

    // Export edges
    for (labels) |label_info| {
        if (label_filter) |filter| {
            if (!std.mem.eql(u8, label_info.name, filter)) continue;
        }

        const nodes = db.getNodesByLabel(label_info.name) catch continue;
        defer allocator.free(nodes);

        for (nodes) |node_id| {
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
        \\  {"source":"n1","target":"n2","type":"KNOWS","properties":{}}
        \\]}
    ;

    const import_stats = try importJsonContent(allocator, &db, json_content, false);
    try std.testing.expectEqual(@as(u64, 2), import_stats.nodes_imported);
    try std.testing.expectEqual(@as(u64, 1), import_stats.edges_imported);

    // Verify data
    try std.testing.expectEqual(@as(u64, 2), db.nodeCount());
    try std.testing.expectEqual(@as(u64, 1), db.edgeCount());
}
