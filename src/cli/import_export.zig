//! Import/Export functionality for LatticeDB CLI.
//!
//! Supports JSON, JSONL, CSV, and DOT formats for importing/exporting graph data.

const std = @import("std");
const lattice = @import("lattice");

const Database = lattice.storage.database.Database;
const PropertyValue = lattice.core.types.PropertyValue;
const NodeId = lattice.core.types.NodeId;
const EdgeId = lattice.core.types.EdgeId;
const QueryResult = lattice.storage.database.QueryResult;
const ResultValue = lattice.storage.database.ResultValue;
const Transaction = lattice.Transaction;
const TxnMode = lattice.TxnMode;

const max_import_file_bytes: usize = 100 * 1024 * 1024;
const node_key_size = @sizeOf(NodeId);
const wal_path_buffer_size = 512;
const decimal_base = 10;

const json_field_edges = "edges";
const json_field_id = "id";
const json_field_kind = "kind";
const json_field_labels = "labels";
const json_field_nodes = "nodes";
const json_field_properties = "properties";
const json_field_source = "source";
const json_field_target = "target";
const json_field_type = "type";
const json_record_kind_edge = "edge";
const json_record_kind_node = "node";
const json_field_vector = "vector";
const json_field_vector_dimensions = "vector_dimensions";

const csv_edges_header = "_source,_target,_type";
const csv_nodes_header = "_id,_labels";
const csv_edges_header_line = csv_edges_header ++ "\n";
const csv_nodes_header_line = csv_nodes_header ++ "\n";
const csv_field_separator = ',';
const csv_label_separator = ';';
const csv_trim_chars = " \t\r";
const csv_quoted_trim_chars = " \t\r\"";
const label_filter_separator = ',';
const label_trim_chars = " ";

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
    const file = @import("compat").fs.cwd().openFile(file_path, .{}) catch {
        return ImportExportError.IoError;
    };
    defer file.close();

    const content = file.readToEndAlloc(allocator, max_import_file_bytes) catch {
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
    const parsed = parseJson(allocator, content) catch {
        return ImportExportError.ParseError;
    };
    defer parsed.deinit();

    const root = parsed.value;
    if (root != .object) {
        return ImportExportError.ParseError;
    }
    const version = formatVersionOf(root.object) catch |err| switch (err) {
        error.UnsupportedVersion => return ImportExportError.UnsupportedVersion,
        else => return ImportExportError.InvalidFormat,
    };
    try checkVectorsCanBeImported(db, root.object);

    // Map from string IDs to NodeIds
    var id_map = std.StringHashMap(NodeId).init(allocator);
    defer id_map.deinit();

    // Import nodes
    if (root.object.get(json_field_nodes)) |nodes_val| {
        if (nodes_val == .array) {
            for (nodes_val.array.items) |node_val| {
                const txn = try batcher.transaction();
                if (importNode(allocator, db, txn, node_val, &id_map, version)) |_| {
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
    if (root.object.get(json_field_edges)) |edges_val| {
        if (edges_val == .array) {
            for (edges_val.array.items) |edge_val| {
                const txn = try batcher.transaction();
                if (importEdge(allocator, db, txn, edge_val, &id_map, version)) |_| {
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

/// Refuse, before anything is written, a file whose vectors this handle cannot store.
fn checkVectorsCanBeImported(db: *Database, root: std.json.ObjectMap) ImportExportError!void {
    const nodes_val = root.get(json_field_nodes) orelse return;
    if (nodes_val != .array) return;
    var has_vectors = false;
    for (nodes_val.array.items) |node_val| {
        if (node_val == .object and node_val.object.get(json_field_vector) != null) {
            has_vectors = true;
            break;
        }
    }
    if (!has_vectors) return;

    const dims = vectorDimensions(db) orelse return ImportExportError.VectorsNotEnabled;
    if (root.get(json_field_vector_dimensions)) |declared| {
        const text = jsonNumberText(declared) orelse return ImportExportError.InvalidFormat;
        const declared_dims = std.fmt.parseInt(u32, text, decimal_base) catch return ImportExportError.InvalidFormat;
        if (declared_dims != dims) return ImportExportError.VectorDimensionMismatch;
    }
}

/// The dimensions of the vector index this handle has open, or null when it
/// was opened without one.
pub fn vectorDimensions(db: *const Database) ?u16 {
    const storage = db.vector_storage orelse return null;
    return storage.dimensions;
}

fn importNode(
    allocator: std.mem.Allocator,
    db: *Database,
    txn: ?*Transaction,
    node_val: std.json.Value,
    id_map: *std.StringHashMap(NodeId),
    version: FormatVersion,
) !void {
    if (node_val != .object) return error.InvalidFormat;

    const obj = node_val.object;

    // Get node ID (required)
    const id_str = if (obj.get(json_field_id)) |v| switch (v) {
        .string => |s| s,
        else => return error.InvalidFormat,
    } else return error.MissingRequiredField;

    // Get labels
    var labels_list = ManagedArrayList([]const u8).init(allocator);
    defer labels_list.deinit();

    if (obj.get(json_field_labels)) |labels_val| {
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
    if (obj.get(json_field_properties)) |props_val| {
        if (props_val == .object) {
            var iter = props_val.object.iterator();
            while (iter.next()) |entry| {
                var prop_val = jsonToPropertyValue(allocator, entry.value_ptr.*, version) catch continue;
                defer prop_val.deinit(allocator);
                db.setNodeProperty(txn, node_id, entry.key_ptr.*, prop_val) catch continue;
            }
        }
    }

    if (obj.get(json_field_vector)) |vector_val| {
        try importNodeVector(allocator, db, txn, node_id, vector_val);
    }
}

fn importNodeVector(
    allocator: std.mem.Allocator,
    db: *Database,
    txn: ?*Transaction,
    node_id: NodeId,
    vector_val: std.json.Value,
) !void {
    if (vector_val == .null) return;
    if (vector_val != .array) return error.InvalidFormat;
    const vector = try jsonToVector(allocator, vector_val.array.items);
    defer allocator.free(vector);

    const dims = vectorDimensions(db) orelse return error.VectorsNotEnabled;
    if (vector.len != dims) return error.VectorDimensionMismatch;
    db.setNodeVectorInTxn(txn, node_id, vector) catch return error.DatabaseError;
}

fn importEdge(
    allocator: std.mem.Allocator,
    db: *Database,
    txn: ?*Transaction,
    edge_val: std.json.Value,
    id_map: *const std.StringHashMap(NodeId),
    version: FormatVersion,
) !void {
    if (edge_val != .object) return error.InvalidFormat;

    const obj = edge_val.object;

    // Get source ID
    const source_str = if (obj.get(json_field_source)) |v| switch (v) {
        .string => |s| s,
        else => return error.InvalidFormat,
    } else return error.MissingRequiredField;

    // Get target ID
    const target_str = if (obj.get(json_field_target)) |v| switch (v) {
        .string => |s| s,
        else => return error.InvalidFormat,
    } else return error.MissingRequiredField;

    // Get edge type
    const edge_type = if (obj.get(json_field_type)) |v| switch (v) {
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

    if (obj.get(json_field_properties)) |props_val| {
        if (props_val == .object) {
            var iter = props_val.object.iterator();
            while (iter.next()) |entry| {
                var prop_val = jsonToPropertyValue(allocator, entry.value_ptr.*, version) catch continue;
                defer prop_val.deinit(allocator);
                db.setEdgePropertyById(txn, edge_id, entry.key_ptr.*, prop_val) catch continue;
            }
        }
    }
}

/// Versions of the JSON export format.
///
/// Version 1 is what 0.16.0 and earlier wrote, with no "version" field. It
/// wrote bytes as bare hex strings, which nothing can tell apart from a string
/// that happens to look like hex, and floats with no fractional part as
/// integers. Version 2 writes every value so that its type survives the trip:
/// strings are always strings, floats always carry a decimal point or an
/// exponent, and the types JSON has no notation for are written as an object
/// with a single "$"-prefixed key:
///
///   {"$bytes": "<base64>"}      bytes
///   {"$vector": [0.5, 1.0]}     a vector-valued property
///   {"$float": "NaN"}           NaN, "Infinity" or "-Infinity"
///   {"$map": {...}}             a map that would otherwise read as one of these
///
/// A map is wrapped in "$map" only when it has exactly one key and that key
/// starts with "$", so ordinary maps are written as plain objects.
pub const FormatVersion = enum(u8) {
    v1 = 1,
    v2 = 2,
};

/// The version this build writes.
pub const current_format_version: FormatVersion = .v2;

const json_field_version = "version";
const tag_bytes = "$bytes";
const tag_vector = "$vector";
const tag_float = "$float";
const tag_map = "$map";

/// Read the "version" field of an export, defaulting to 1 when it is absent.
fn formatVersionOf(root: std.json.ObjectMap) !FormatVersion {
    const value = root.get(json_field_version) orelse return .v1;
    const text = switch (value) {
        .number_string => |n| n,
        .integer => |i| return versionFromInt(i),
        else => return error.InvalidFormat,
    };
    const number = std.fmt.parseInt(i64, text, decimal_base) catch return error.InvalidFormat;
    return versionFromInt(number);
}

fn versionFromInt(number: i64) !FormatVersion {
    return switch (number) {
        1 => .v1,
        2 => .v2,
        else => error.UnsupportedVersion,
    };
}

/// Parse an export's JSON text, keeping numbers as text so that each one can
/// be read at the precision its destination needs.
fn parseJson(allocator: std.mem.Allocator, text: []const u8) !std.json.Parsed(std.json.Value) {
    return std.json.parseFromSlice(std.json.Value, allocator, text, .{ .parse_numbers = false });
}

fn jsonNumberText(val: std.json.Value) ?[]const u8 {
    return switch (val) {
        .number_string => |n| n,
        else => null,
    };
}

/// Why a value in an import file could not be read.
const ValueError = error{ InvalidFormat, OutOfMemory };

fn jsonToPropertyValue(allocator: std.mem.Allocator, val: std.json.Value, version: FormatVersion) ValueError!PropertyValue {
    return switch (val) {
        .null => .{ .null_val = {} },
        .bool => |b| .{ .bool_val = b },
        .integer => |i| .{ .int_val = i },
        .float => |f| .{ .float_val = f },
        .number_string => |n| try parseJsonNumber(n),
        // A string is a string. Version 1 wrote bytes as hex strings, but so
        // is "cafe", so a hex-looking string cannot be taken for bytes.
        .string => |s| .{ .string_val = try allocator.dupe(u8, s) },
        .array => |array| blk: {
            // Version 1 wrote vector properties as plain arrays of numbers, and
            // 0.16.0 read every such array back as a vector.
            if (version == .v1 and array.items.len > 0 and isNumericJsonArray(array.items)) {
                break :blk .{ .vector_val = try jsonToVector(allocator, array.items) };
            }
            break :blk .{ .list_val = try jsonToList(allocator, array.items, version) };
        },
        .object => |object| blk: {
            if (version != .v1) {
                if (taggedValue(object)) |tagged| {
                    break :blk try decodeTaggedValue(allocator, tagged.tag, tagged.value, version);
                }
            }
            break :blk .{ .map_val = try jsonToMap(allocator, object, version) };
        },
    };
}

fn parseJsonNumber(text: []const u8) ValueError!PropertyValue {
    if (std.json.isNumberFormattedLikeAnInteger(text)) {
        return .{ .int_val = std.fmt.parseInt(i64, text, decimal_base) catch return error.InvalidFormat };
    }
    return .{ .float_val = std.fmt.parseFloat(f64, text) catch return error.InvalidFormat };
}

const TaggedValue = struct {
    tag: []const u8,
    value: std.json.Value,
};

/// An object with exactly one key that starts with "$" is a tagged value.
fn taggedValue(object: std.json.ObjectMap) ?TaggedValue {
    if (object.count() != 1) return null;
    var iter = object.iterator();
    const entry = iter.next().?;
    if (!std.mem.startsWith(u8, entry.key_ptr.*, "$")) return null;
    return .{ .tag = entry.key_ptr.*, .value = entry.value_ptr.* };
}

fn decodeTaggedValue(
    allocator: std.mem.Allocator,
    tag: []const u8,
    value: std.json.Value,
    version: FormatVersion,
) ValueError!PropertyValue {
    if (std.mem.eql(u8, tag, tag_bytes)) {
        if (value != .string) return error.InvalidFormat;
        return .{ .bytes_val = try decodeBase64(allocator, value.string) };
    }
    if (std.mem.eql(u8, tag, tag_vector)) {
        if (value != .array) return error.InvalidFormat;
        return .{ .vector_val = try jsonToVector(allocator, value.array.items) };
    }
    if (std.mem.eql(u8, tag, tag_float)) {
        return .{ .float_val = try nonFiniteFloat(f64, value) };
    }
    if (std.mem.eql(u8, tag, tag_map)) {
        if (value != .object) return error.InvalidFormat;
        return .{ .map_val = try jsonToMap(allocator, value.object, version) };
    }
    return error.InvalidFormat;
}

fn decodeBase64(allocator: std.mem.Allocator, text: []const u8) ValueError![]const u8 {
    const decoder = std.base64.standard.Decoder;
    const size = decoder.calcSizeForSlice(text) catch return error.InvalidFormat;
    const bytes = try allocator.alloc(u8, size);
    errdefer allocator.free(bytes);
    decoder.decode(bytes, text) catch return error.InvalidFormat;
    return bytes;
}

/// Read one float, as a number or as a tagged non-finite value.
fn jsonToFloat(comptime T: type, val: std.json.Value) ValueError!T {
    switch (val) {
        .number_string => |n| return std.fmt.parseFloat(T, n) catch error.InvalidFormat,
        .integer => |i| return @floatFromInt(i),
        .float => |f| return @floatCast(f),
        .object => |object| {
            const tagged = taggedValue(object) orelse return error.InvalidFormat;
            if (!std.mem.eql(u8, tagged.tag, tag_float)) return error.InvalidFormat;
            return nonFiniteFloat(T, tagged.value);
        },
        else => return error.InvalidFormat,
    }
}

fn nonFiniteFloat(comptime T: type, val: std.json.Value) ValueError!T {
    if (val != .string) return error.InvalidFormat;
    if (std.mem.eql(u8, val.string, "NaN")) return std.math.nan(T);
    if (std.mem.eql(u8, val.string, "Infinity")) return std.math.inf(T);
    if (std.mem.eql(u8, val.string, "-Infinity")) return -std.math.inf(T);
    return error.InvalidFormat;
}

/// Read an array of numbers as f32 components. Each is parsed from its text
/// straight to f32, so a component written at f32 precision reads back exactly.
fn jsonToVector(allocator: std.mem.Allocator, items: []const std.json.Value) ValueError![]const f32 {
    const vector = try allocator.alloc(f32, items.len);
    errdefer allocator.free(vector);
    for (items, 0..) |item, i| {
        vector[i] = try jsonToFloat(f32, item);
    }
    return vector;
}

fn jsonToList(allocator: std.mem.Allocator, items: []const std.json.Value, version: FormatVersion) ValueError![]const PropertyValue {
    const list = try allocator.alloc(PropertyValue, items.len);
    var initialized: usize = 0;
    errdefer {
        for (list[0..initialized]) |*item| {
            item.deinit(allocator);
        }
        allocator.free(list);
    }

    for (items, 0..) |item, i| {
        list[i] = try jsonToPropertyValue(allocator, item, version);
        initialized += 1;
    }
    return list;
}

fn jsonToMap(allocator: std.mem.Allocator, object: std.json.ObjectMap, version: FormatVersion) ValueError![]const PropertyValue.MapEntry {
    const map = try allocator.alloc(PropertyValue.MapEntry, object.count());
    var initialized: usize = 0;
    errdefer {
        for (map[0..initialized]) |*entry| {
            allocator.free(entry.key);
            entry.value.deinit(allocator);
        }
        allocator.free(map);
    }

    var iter = object.iterator();
    while (iter.next()) |entry| {
        const key = try allocator.dupe(u8, entry.key_ptr.*);
        errdefer allocator.free(key);

        const value = try jsonToPropertyValue(allocator, entry.value_ptr.*, version);
        map[initialized] = .{
            .key = key,
            .value = value,
        };
        initialized += 1;
    }
    return map;
}

fn isNumericJsonArray(items: []const std.json.Value) bool {
    for (items) |item| {
        switch (item) {
            .integer, .float, .number_string => {},
            else => return false,
        }
    }
    return true;
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
    UnsupportedVersion,
    /// The file holds vectors and the database was opened without them.
    VectorsNotEnabled,
    /// The file's vectors have a different number of dimensions than the database's.
    VectorDimensionMismatch,
    /// An export by query whose rows hold no nodes or edges to export.
    QueryReturnedNoGraph,
};

/// Export database to JSON format
pub fn exportJson(
    allocator: std.mem.Allocator,
    db: *Database,
    writer: anytype,
    scope: ExportScope,
) !ExportStats {
    var stats = ExportStats{};
    var selection = try Selection.init(allocator, db, scope);
    defer selection.deinit();
    const node_ids = selection.node_ids;

    try writeJsonHeader(db, writer);

    var first_node = true;
    for (node_ids) |node_id| {
        if (!first_node) {
            try writer.writeAll(",");
        }
        first_node = false;

        try writeNodeJson(allocator, db, writer, node_id);
        stats.nodes_exported += 1;
    }

    try writer.writeAll("],\"" ++ json_field_edges ++ "\":[");

    var first_edge = true;
    for (node_ids) |node_id| {
        const edges = db.getOutgoingEdges(node_id) catch continue;
        defer db.freeEdgeInfos(edges);

        for (edges) |edge| {
            if (!selection.includesEdge(edge)) continue;
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
    scope: ExportScope,
) !ExportStats {
    var stats = ExportStats{};

    var selection = try Selection.init(allocator, db, scope);
    defer selection.deinit();
    const node_ids = selection.node_ids;

    try writeJsonHeader(db, writer);
    for (node_ids, 0..) |node_id, i| {
        if (i > 0) try writer.writeByte(',');
        try writeNodeJsonCanonical(allocator, db, writer, node_id);
        stats.nodes_exported += 1;
    }
    try writer.writeAll("],\"" ++ json_field_edges ++ "\":[");

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
            if (!selection.includesEdge(edge)) continue;
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
    scope: ExportScope,
) !ExportStats {
    var stats = ExportStats{};
    var selection = try Selection.init(allocator, db, scope);
    defer selection.deinit();
    const node_ids = selection.node_ids;

    for (node_ids) |node_id| {
        try writeNodeJsonlRecord(allocator, db, writer, node_id);
        try writer.writeByte('\n');
        stats.nodes_exported += 1;
    }

    for (node_ids) |node_id| {
        const edges = db.getOutgoingEdges(node_id) catch continue;
        defer db.freeEdgeInfos(edges);

        for (edges) |edge| {
            if (!selection.includesEdge(edge)) continue;
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
    scope: ExportScope,
) !ExportStats {
    var stats = ExportStats{};
    var selection = try Selection.init(allocator, db, scope);
    defer selection.deinit();
    const node_ids = selection.node_ids;
    try writer.writeAll("digraph G {\n");

    for (node_ids) |node_id| {
        try writeNodeDot(allocator, db, writer, node_id);
        stats.nodes_exported += 1;
    }

    for (node_ids) |node_id| {
        const edges = db.getOutgoingEdges(node_id) catch continue;
        defer db.freeEdgeInfos(edges);

        for (edges) |edge| {
            if (!selection.includesEdge(edge)) continue;
            try writeEdgeDot(writer, edge);
            stats.edges_exported += 1;
        }
    }

    try writer.writeAll("}\n");
    return stats;
}

/// Open a JSON export: the format version, the vector dimensions when the
/// handle has vectors, and the start of the nodes array.
fn writeJsonHeader(db: *const Database, writer: anytype) !void {
    try writer.print("{{\"" ++ json_field_version ++ "\":{d},", .{@intFromEnum(current_format_version)});
    if (vectorDimensions(db)) |dims| {
        try writer.print("\"" ++ json_field_vector_dimensions ++ "\":{d},", .{dims});
    }
    try writer.writeAll("\"" ++ json_field_nodes ++ "\":[");
}

/// Write a node's vector as a "vector" field, when the handle has vectors and
/// the node has one.
fn writeNodeVectorField(db: *Database, writer: anytype, node_id: NodeId) !void {
    if (db.vector_storage == null) return;
    const vector = (db.getNodeVectorInTxn(null, node_id) catch |err| return mapDatabaseError(err)) orelse return;
    defer db.allocator.free(vector);
    try writer.writeAll(",\"" ++ json_field_vector ++ "\":");
    try writeJsonVector(writer, vector);
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
    try writer.print("{{\"" ++ json_field_id ++ "\":\"{d}\",\"" ++ json_field_labels ++ "\":[", .{node_id});

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

    try writer.writeAll("],\"" ++ json_field_properties ++ "\":{");

    // Get properties
    const props = db.getNodeProperties(node_id) catch |err| return mapDatabaseError(err);
    defer db.freePropertyEntries(props);

    for (props, 0..) |prop, i| {
        if (i > 0) try writer.writeAll(",");
        try writeJsonString(writer, prop.key);
        try writer.writeByte(':');
        try writePropertyValueJson(writer, prop.value);
    }

    try writer.writeByte('}');
    try writeNodeVectorField(db, writer, node_id);
    try writer.writeByte('}');
}

fn writeNodeJsonCanonical(
    allocator: std.mem.Allocator,
    db: *Database,
    writer: anytype,
    node_id: NodeId,
) !void {
    try writer.print("{{\"" ++ json_field_id ++ "\":\"{d}\",\"" ++ json_field_labels ++ "\":[", .{node_id});

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

    try writer.writeAll("],\"" ++ json_field_properties ++ "\":{");

    const props = db.getNodeProperties(node_id) catch |err| return mapDatabaseError(err);
    defer db.freePropertyEntries(props);
    try writeSortedPropertyEntriesJson(allocator, writer, props);
    try writer.writeByte('}');
    try writeNodeVectorField(db, writer, node_id);
    try writer.writeByte('}');
}

fn writeEdgeJson(db: *Database, writer: anytype, edge: Database.EdgeInfo) !void {
    try writer.print(
        "{{\"" ++ json_field_source ++ "\":\"{d}\",\"" ++ json_field_target ++ "\":\"{d}\",\"" ++ json_field_type ++ "\":",
        .{ edge.source, edge.target },
    );
    try writeJsonString(writer, edge.edge_type);
    try writer.writeAll(",\"" ++ json_field_properties ++ "\":{");

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
        "{{\"" ++ json_field_id ++ "\":\"{d}\",\"" ++ json_field_source ++ "\":\"{d}\",\"" ++ json_field_target ++ "\":\"{d}\",\"" ++ json_field_type ++ "\":",
        .{ edge.id, edge.source, edge.target },
    );
    try writeJsonString(writer, edge.edge_type);
    try writer.writeAll(",\"" ++ json_field_properties ++ "\":{");

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
    try writer.writeAll("{\"" ++ json_field_kind ++ "\":\"" ++ json_record_kind_node ++ "\",\"" ++ json_field_id ++ "\":");
    try writer.print("\"{d}\",\"" ++ json_field_labels ++ "\":[", .{node_id});

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

    try writer.writeAll("],\"" ++ json_field_properties ++ "\":{");

    const props = db.getNodeProperties(node_id) catch |err| return mapDatabaseError(err);
    defer db.freePropertyEntries(props);

    for (props, 0..) |prop, i| {
        if (i > 0) try writer.writeAll(",");
        try writeJsonString(writer, prop.key);
        try writer.writeByte(':');
        try writePropertyValueJson(writer, prop.value);
    }

    try writer.writeByte('}');
    try writeNodeVectorField(db, writer, node_id);
    try writer.writeByte('}');
}

fn writeEdgeJsonlRecord(db: *Database, writer: anytype, edge: Database.EdgeInfo) !void {
    try writer.print(
        "{{\"" ++ json_field_kind ++ "\":\"" ++ json_record_kind_edge ++ "\",\"" ++ json_field_source ++ "\":\"{d}\",\"" ++ json_field_target ++ "\":\"{d}\",\"" ++ json_field_type ++ "\":",
        .{ edge.source, edge.target },
    );
    try writeJsonString(writer, edge.edge_type);
    try writer.writeAll(",\"" ++ json_field_properties ++ "\":{");

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
    try writeValueJson(null, writer, val);
}

fn writePropertyValueJsonCanonical(
    allocator: std.mem.Allocator,
    writer: anytype,
    val: PropertyValue,
) !void {
    try writeValueJson(allocator, writer, val);
}

/// Write a value in the version 2 encoding (see `FormatVersion`).
///
/// With an allocator, map keys are written in sorted order, for output that
/// must not depend on the order properties were stored in.
fn writeValueJson(sort_allocator: ?std.mem.Allocator, writer: anytype, val: PropertyValue) !void {
    switch (val) {
        .null_val => try writer.writeAll("null"),
        .bool_val => |b| try writer.writeAll(if (b) "true" else "false"),
        .int_val => |i| try writer.print("{d}", .{i}),
        .float_val => |f| try writeJsonFloat(f64, writer, f),
        .string_val => |s| try writeJsonString(writer, s),
        .bytes_val => |bytes| {
            try writer.writeAll("{\"" ++ tag_bytes ++ "\":\"");
            try writeBase64(writer, bytes);
            try writer.writeAll("\"}");
        },
        .vector_val => |vec| {
            try writer.writeAll("{\"" ++ tag_vector ++ "\":");
            try writeJsonVector(writer, vec);
            try writer.writeByte('}');
        },
        .list_val => |list| {
            try writer.writeByte('[');
            for (list, 0..) |item, i| {
                if (i > 0) try writer.writeByte(',');
                try writeValueJson(sort_allocator, writer, item);
            }
            try writer.writeByte(']');
        },
        .map_val => |entries| {
            // A map shaped like a tagged value is wrapped so it reads back as a map.
            const wrap = entries.len == 1 and std.mem.startsWith(u8, entries[0].key, "$");
            if (wrap) try writer.writeAll("{\"" ++ tag_map ++ "\":");

            var ordered = entries;
            var sorted: ?[]PropertyValue.MapEntry = null;
            defer if (sorted) |owned| sort_allocator.?.free(owned);
            if (sort_allocator) |allocator| {
                const copy = try allocator.dupe(PropertyValue.MapEntry, entries);
                std.mem.sort(PropertyValue.MapEntry, copy, {}, struct {
                    fn lessThan(_: void, a: PropertyValue.MapEntry, b: PropertyValue.MapEntry) bool {
                        return std.mem.order(u8, a.key, b.key) == .lt;
                    }
                }.lessThan);
                sorted = copy;
                ordered = copy;
            }

            try writer.writeByte('{');
            for (ordered, 0..) |entry, i| {
                if (i > 0) try writer.writeByte(',');
                try writeJsonString(writer, entry.key);
                try writer.writeByte(':');
                try writeValueJson(sort_allocator, writer, entry.value);
            }
            try writer.writeByte('}');

            if (wrap) try writer.writeByte('}');
        },
    }
}

fn writeJsonVector(writer: anytype, vec: []const f32) !void {
    try writer.writeByte('[');
    for (vec, 0..) |f, i| {
        if (i > 0) try writer.writeByte(',');
        try writeJsonFloat(f32, writer, f);
    }
    try writer.writeByte(']');
}

/// Write a float so that it reads back as a float of the same value.
///
/// The shortest decimal that round-trips is used, with ".0" added when it would
/// otherwise look like an integer, and scientific notation when the decimal
/// form would be long. JSON has no notation for NaN or the infinities, so those
/// are written as tagged values.
fn writeJsonFloat(comptime T: type, writer: anytype, f: T) !void {
    if (std.math.isNan(f)) return writer.writeAll("{\"" ++ tag_float ++ "\":\"NaN\"}");
    if (std.math.isInf(f)) {
        return writer.writeAll(if (f > 0)
            "{\"" ++ tag_float ++ "\":\"Infinity\"}"
        else
            "{\"" ++ tag_float ++ "\":\"-Infinity\"}");
    }

    const max_decimal_len = 24;
    var buf: [512]u8 = undefined;
    const decimal = std.fmt.bufPrint(&buf, "{d}", .{f}) catch unreachable;
    if (decimal.len <= max_decimal_len) {
        try writer.writeAll(decimal);
        if (std.mem.indexOfAny(u8, decimal, ".e") == null) try writer.writeAll(".0");
        return;
    }
    try writer.writeAll(std.fmt.bufPrint(&buf, "{e}", .{f}) catch unreachable);
}

fn writeBase64(writer: anytype, bytes: []const u8) !void {
    const encoder = std.base64.standard.Encoder;
    // Whole groups of three bytes encode to whole groups of four characters.
    const chunk_len = 3 * 64;
    var buf: [4 * 64]u8 = undefined;
    var offset: usize = 0;
    while (offset < bytes.len) {
        const end = @min(offset + chunk_len, bytes.len);
        try writer.writeAll(encoder.encode(&buf, bytes[offset..end]));
        offset = end;
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

        var filters = std.mem.splitScalar(u8, filter, label_filter_separator);
        while (filters.next()) |raw_label| {
            const label = std.mem.trim(u8, raw_label, csv_trim_chars);
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
                if (e.key.len == node_key_size) {
                    try node_ids.append(allocator, std.mem.readInt(NodeId, e.key[0..node_key_size], .little));
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

/// Which part of the graph an export covers. The default is all of it.
pub const ExportScope = struct {
    /// Only nodes with at least one of these labels, comma-separated, and the
    /// edges between them.
    label_filter: ?[]const u8 = null,
    /// Only the nodes and edges this query returned, plus the endpoints of
    /// those edges so that the export imports on its own.
    query_result: ?*const QueryResult = null,
};

/// The nodes and edges one export writes.
const Selection = struct {
    allocator: std.mem.Allocator,
    /// Ascending, so every format writes nodes in the same order.
    node_ids: []NodeId,
    nodes: std.AutoHashMap(NodeId, void),
    /// Null means every edge whose endpoints are both selected.
    edges: ?std.AutoHashMap(EdgeId, void),

    fn init(allocator: std.mem.Allocator, db: *Database, scope: ExportScope) !Selection {
        if (scope.query_result) |result| return fromQueryResult(allocator, db, result);

        const node_ids = try collectCanonicalNodeIds(allocator, db, scope.label_filter);
        errdefer allocator.free(node_ids);
        var nodes = std.AutoHashMap(NodeId, void).init(allocator);
        errdefer nodes.deinit();
        for (node_ids) |node_id| try nodes.put(node_id, {});
        return .{ .allocator = allocator, .node_ids = node_ids, .nodes = nodes, .edges = null };
    }

    fn fromQueryResult(allocator: std.mem.Allocator, db: *Database, result: *const QueryResult) !Selection {
        var nodes = std.AutoHashMap(NodeId, void).init(allocator);
        errdefer nodes.deinit();
        var edges = std.AutoHashMap(EdgeId, void).init(allocator);
        errdefer edges.deinit();

        for (result.rows) |row| {
            for (row.values) |value| try collectGraphValues(value, &nodes, &edges);
        }
        if (result.rows.len > 0 and nodes.count() == 0 and edges.count() == 0) {
            return ImportExportError.QueryReturnedNoGraph;
        }

        // An edge is only importable alongside its endpoints.
        var edge_iter = edges.keyIterator();
        while (edge_iter.next()) |edge_id| {
            var edge = (db.getEdgeInTxn(null, edge_id.*) catch |err| return mapDatabaseError(err)) orelse continue;
            defer edge.deinit(db.allocator);
            try nodes.put(edge.source, {});
            try nodes.put(edge.target, {});
        }

        const node_ids = try allocator.alloc(NodeId, nodes.count());
        var node_iter = nodes.keyIterator();
        var i: usize = 0;
        while (node_iter.next()) |node_id| : (i += 1) node_ids[i] = node_id.*;
        std.mem.sort(NodeId, node_ids, {}, std.sort.asc(NodeId));

        return .{ .allocator = allocator, .node_ids = node_ids, .nodes = nodes, .edges = edges };
    }

    fn deinit(self: *Selection) void {
        self.allocator.free(self.node_ids);
        self.nodes.deinit();
        if (self.edges) |*edges| edges.deinit();
    }

    fn includesEdge(self: *const Selection, edge: Database.EdgeInfo) bool {
        if (!self.nodes.contains(edge.source) or !self.nodes.contains(edge.target)) return false;
        const edges = self.edges orelse return true;
        return edges.contains(edge.id);
    }
};

/// Gather the node and edge ids a query result value refers to, looking
/// inside lists and maps, which is where paths and collected values put them.
fn collectGraphValues(
    value: ResultValue,
    nodes: *std.AutoHashMap(NodeId, void),
    edges: *std.AutoHashMap(EdgeId, void),
) error{OutOfMemory}!void {
    switch (value) {
        .node_id => |id| try nodes.put(id, {}),
        .edge_id => |id| try edges.put(id, {}),
        .list_val => |items| for (items) |item| try collectGraphValues(item, nodes, edges),
        .map_val => |entries| for (entries) |entry| try collectGraphValues(entry.value, nodes, edges),
        else => {},
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
    const file = @import("compat").fs.cwd().openFile(file_path, .{}) catch {
        return ImportExportError.IoError;
    };
    defer file.close();

    const content = file.readToEndAlloc(allocator, max_import_file_bytes) catch {
        return ImportExportError.IoError;
    };
    defer allocator.free(content);

    // Detect if this is a nodes or edges CSV by checking the header
    var lines = std.mem.splitScalar(u8, content, '\n');
    const header_line = lines.next() orelse return ImportExportError.ParseError;

    if (std.mem.startsWith(u8, header_line, csv_nodes_header)) {
        return importNodesCsv(allocator, db, content, batch_size, on_error_skip);
    } else if (std.mem.startsWith(u8, header_line, csv_edges_header)) {
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

    var header_parts = std.mem.splitScalar(u8, header_line, csv_field_separator);
    while (header_parts.next()) |part| {
        headers.append(std.mem.trim(u8, part, csv_trim_chars)) catch {
            return ImportExportError.OutOfMemory;
        };
    }

    // Process data lines
    while (lines.next()) |line| {
        const trimmed = std.mem.trim(u8, line, csv_trim_chars);
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
    var parts = std.mem.splitScalar(u8, line, csv_field_separator);
    while (parts.next()) |part| {
        try values.append(std.mem.trim(u8, part, csv_quoted_trim_chars));
    }

    if (values.items.len < 2) return error.InvalidFormat;

    // Parse labels (semicolon-separated)
    var labels_list = ManagedArrayList([]const u8).init(allocator);
    defer labels_list.deinit();

    if (values.items.len > 1 and values.items[1].len > 0) {
        var label_parts = std.mem.splitScalar(u8, values.items[1], csv_label_separator);
        while (label_parts.next()) |label| {
            const trimmed = std.mem.trim(u8, label, label_trim_chars);
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
            const prop_val: PropertyValue = if (std.fmt.parseInt(i64, val, decimal_base)) |int_val|
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
        const trimmed = std.mem.trim(u8, line, csv_trim_chars);
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
    var parts = std.mem.splitScalar(u8, line, csv_field_separator);

    const source_str = std.mem.trim(u8, parts.next() orelse return error.InvalidFormat, csv_quoted_trim_chars);
    const target_str = std.mem.trim(u8, parts.next() orelse return error.InvalidFormat, csv_quoted_trim_chars);
    const edge_type = std.mem.trim(u8, parts.next() orelse return error.InvalidFormat, csv_quoted_trim_chars);

    // Parse node IDs
    const source_id = std.fmt.parseInt(NodeId, source_str, decimal_base) catch {
        return error.InvalidNodeId;
    };
    const target_id = std.fmt.parseInt(NodeId, target_str, decimal_base) catch {
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
    scope: ExportScope,
) !ExportStats {
    var stats = ExportStats{};
    var selection = try Selection.init(allocator, db, scope);
    defer selection.deinit();
    const node_ids = selection.node_ids;

    // Write nodes header
    try nodes_writer.writeAll(csv_nodes_header_line);

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
            if (i > 0) try nodes_writer.writeByte(csv_label_separator);
            try nodes_writer.writeAll(label);
        }
        try nodes_writer.writeAll("\"\n");
        stats.nodes_exported += 1;
    }

    // Write edges header
    try edges_writer.writeAll(csv_edges_header_line);

    for (node_ids) |node_id| {
        const edges = db.getOutgoingEdges(node_id) catch continue;
        defer db.freeEdgeInfos(edges);

        for (edges) |edge| {
            if (!selection.includesEdge(edge)) continue;
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
    @import("compat").fs.cwd().deleteFile(path) catch {};

    var wal_path_buf: [wal_path_buffer_size]u8 = undefined;
    const wal_path = std.fmt.bufPrint(&wal_path_buf, "{s}-wal", .{path}) catch return;
    @import("compat").fs.cwd().deleteFile(wal_path) catch {};
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
        importJsonContent(allocator, db, json_content, 2, false),
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

    const stats = try importJsonContent(allocator, db, json_content, 2, true);
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
        importNodesCsv(allocator, db, csv_content, 2, false),
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
        @import("compat").fs.cwd().deleteFile(path) catch {};
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

    const import_stats = try importJsonContent(allocator, db, json_content, 1000, false);
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

    @import("compat").fs.cwd().deleteFile(path_a) catch {};
    @import("compat").fs.cwd().deleteFile(path_b) catch {};

    var db_a = try Database.open(allocator, path_a, .{
        .create = true,
        .config = .{ .enable_wal = false, .enable_fts = false },
    });
    defer {
        db_a.close();
        @import("compat").fs.cwd().deleteFile(path_a) catch {};
    }

    var db_b = try Database.open(allocator, path_b, .{
        .create = true,
        .config = .{ .enable_wal = false, .enable_fts = false },
    });
    defer {
        db_b.close();
        @import("compat").fs.cwd().deleteFile(path_b) catch {};
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
    var stream_a = @import("compat").fixedBufferStream(&buf_a);
    const stats_a = try dumpCanonicalJson(allocator, db_a, stream_a.writer(), .{});
    try std.testing.expectEqual(@as(u64, 2), stats_a.nodes_exported);
    try std.testing.expectEqual(@as(u64, 1), stats_a.edges_exported);

    var buf_b: [8192]u8 = undefined;
    var stream_b = @import("compat").fixedBufferStream(&buf_b);
    const stats_b = try dumpCanonicalJson(allocator, db_b, stream_b.writer(), .{});
    try std.testing.expectEqual(@as(u64, 2), stats_b.nodes_exported);
    try std.testing.expectEqual(@as(u64, 1), stats_b.edges_exported);

    const out_a = buf_a[0..stream_a.pos];
    const out_b = buf_b[0..stream_b.pos];
    try std.testing.expectEqualStrings(out_a, out_b);
    try std.testing.expect(std.mem.indexOf(u8, out_a, "\"labels\":[]") != null);
    try std.testing.expect(std.mem.indexOf(u8, out_a, "\"id\":\"1\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, out_a, "\"id\":\"1\",\"source\":\"1\",\"target\":\"2\"") != null);
}
