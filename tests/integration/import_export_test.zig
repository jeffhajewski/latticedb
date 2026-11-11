//! End-to-end integration tests for graph export formats.
//!
//! Verifies JSON, CSV, JSONL, and DOT exports against real database state.

const std = @import("std");
const lattice = @import("lattice");

const Database = lattice.storage.database.Database;
const PropertyValue = lattice.core.types.PropertyValue;
const import_export = @import("import_export");

const database_key_size = @sizeOf(u64);
const small_export_buffer_size = 4096;
const large_export_buffer_size = 8192;
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

const employee_label = "Employee";
const person_label = "Person";
const company_label = "Company";
const rel_type = "REL";
const works_at_type = "WORKS_AT";

const active_status = "active";
const alice_name = "Alice";
const bob_name = "Bob";
const carol_name = "Carol";
const acme_name = "Acme";
const mystery_name = "Mystery";
const invalid_payload = "bad";

const name_property = "name";
const since_property = "since";
const status_property = "status";
const first_since_year = 2020;
const second_since_year = 2021;

const line_trim_chars = " \t\r";

fn openTestDb(path: []const u8) !*Database {
    @import("compat").fs.cwd().deleteFile(path) catch {};
    return try Database.open(std.testing.allocator, path, .{
        .create = true,
        .config = .{
            .enable_wal = false,
            .enable_fts = false,
        },
    });
}

fn cleanupTestDb(db: *Database, path: []const u8) void {
    db.close();
    @import("compat").fs.cwd().deleteFile(path) catch {};
}

fn overwriteNodePayloadWithInvalidData(db: *Database, node_id: u64) !void {
    var key: [database_key_size]u8 = undefined;
    std.mem.writeInt(u64, &key, node_id, .little);
    try db.node_tree.delete(&key);
    try db.node_tree.insert(&key, invalid_payload);
}

fn overwriteEdgePayloadWithInvalidData(db: *Database, edge_id: u64) !void {
    var key: [database_key_size]u8 = undefined;
    std.mem.writeInt(u64, &key, edge_id, .little);
    try db.edge_store.edge_id_index.delete(&key);
    try db.edge_store.edge_id_index.insert(&key, invalid_payload);
}

fn seedMultiLabelParallelGraph(db: *Database) !void {
    const alice = try db.createNode(null, &.{ person_label, employee_label });
    const bob = try db.createNode(null, &.{person_label});

    try db.setNodeProperty(null, alice, name_property, .{ .string_val = alice_name });
    try db.setNodeProperty(null, bob, name_property, .{ .string_val = bob_name });

    // Parallel edges with the same type must both survive export.
    const first = try db.createEdgeAndGetId(null, alice, bob, rel_type);
    const second = try db.createEdgeAndGetId(null, alice, bob, rel_type);
    try db.setEdgePropertyById(null, first, since_property, .{ .int_val = first_since_year });
    try db.setEdgePropertyById(null, first, status_property, .{ .string_val = active_status });
    try db.setEdgePropertyById(null, second, since_property, .{ .int_val = second_since_year });
}

fn seedGraphWithUnlabeledNode(db: *Database) !void {
    const labeled = try db.createNode(null, &.{person_label});
    const unlabeled = try db.createNode(null, &.{});

    try db.setNodeProperty(null, labeled, name_property, .{ .string_val = alice_name });
    try db.setNodeProperty(null, unlabeled, name_property, .{ .string_val = mystery_name });
    _ = try db.createEdgeAndGetId(null, labeled, unlabeled, rel_type);
}

fn seedGraphForMultiLabelFilter(db: *Database) !void {
    const alice = try db.createNode(null, &.{ person_label, employee_label });
    const bob = try db.createNode(null, &.{person_label});
    const carol = try db.createNode(null, &.{employee_label});

    try db.setNodeProperty(null, alice, name_property, .{ .string_val = alice_name });
    try db.setNodeProperty(null, bob, name_property, .{ .string_val = bob_name });
    try db.setNodeProperty(null, carol, name_property, .{ .string_val = carol_name });

    _ = try db.createEdgeAndGetId(null, alice, bob, rel_type);
    _ = try db.createEdgeAndGetId(null, carol, bob, rel_type);
}

fn seedGraphForLabelScopedExport(db: *Database) !void {
    const alice = try db.createNode(null, &.{person_label});
    const acme = try db.createNode(null, &.{company_label});

    try db.setNodeProperty(null, alice, name_property, .{ .string_val = alice_name });
    try db.setNodeProperty(null, acme, name_property, .{ .string_val = acme_name });
    _ = try db.createEdgeAndGetId(null, alice, acme, works_at_type);
}

fn countNonEmptyLines(s: []const u8) usize {
    var count: usize = 0;
    var lines = std.mem.splitScalar(u8, s, '\n');
    while (lines.next()) |line| {
        if (std.mem.trim(u8, line, line_trim_chars).len > 0) {
            count += 1;
        }
    }
    return count;
}

fn mapValue(entries: []const PropertyValue.MapEntry, key: []const u8) ?PropertyValue {
    for (entries) |entry| {
        if (std.mem.eql(u8, entry.key, key)) return entry.value;
    }
    return null;
}

test "import_export: exportJson deduplicates multi-label nodes and preserves parallel edges" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_export_json_dedup_test.ltdb";
    const db = try openTestDb(path);
    defer cleanupTestDb(db, path);

    try seedMultiLabelParallelGraph(db);

    var buf: [large_export_buffer_size]u8 = undefined;
    var stream = @import("compat").fixedBufferStream(&buf);

    const stats = try import_export.exportJson(allocator, db, stream.writer(), .{});
    try std.testing.expectEqual(@as(u64, 2), stats.nodes_exported);
    try std.testing.expectEqual(@as(u64, 2), stats.edges_exported);

    const output = buf[0..stream.pos];
    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, output, .{});
    defer parsed.deinit();

    const root = parsed.value;
    try std.testing.expect(root == .object);
    const nodes = root.object.get(json_field_nodes).?.array.items;
    const edges = root.object.get(json_field_edges).?.array.items;
    try std.testing.expectEqual(@as(usize, 2), nodes.len);
    try std.testing.expectEqual(@as(usize, 2), edges.len);

    var found_2020 = false;
    var found_2021 = false;
    for (edges) |edge| {
        const props = edge.object.get(json_field_properties).?.object;
        const since = props.get(since_property).?.integer;
        if (since == first_since_year) {
            found_2020 = true;
            try std.testing.expectEqualStrings(active_status, props.get(status_property).?.string);
        } else if (since == second_since_year) {
            found_2021 = true;
        }
    }
    try std.testing.expect(found_2020);
    try std.testing.expect(found_2021);
}

test "import_export: JSON roundtrip preserves nested property types" {
    const allocator = std.testing.allocator;
    const source_path = "/tmp/lattice_export_json_nested_source.ltdb";
    const imported_path = "/tmp/lattice_export_json_nested_imported.ltdb";
    const source_db = try openTestDb(source_path);
    defer cleanupTestDb(source_db, source_path);
    const imported_db = try openTestDb(imported_path);
    defer cleanupTestDb(imported_db, imported_path);

    const node = try source_db.createNode(null, &.{person_label});
    const blob = [_]u8{ 0xde, 0xad, 0xbe, 0xef };
    const embedding = [_]f32{ 0.25, 1.5, -3.0 };
    const nested_list = [_]PropertyValue{
        .{ .string_val = "inner" },
        .{ .bool_val = false },
    };
    const tags = [_]PropertyValue{
        .{ .string_val = "graph" },
        .{ .int_val = 7 },
        .{ .bool_val = true },
        .{ .list_val = &nested_list },
    };
    const meta = [_]PropertyValue.MapEntry{
        .{ .key = "city", .value = .{ .string_val = "Portland" } },
        .{ .key = "score", .value = .{ .float_val = 98.5 } },
        .{ .key = "flags", .value = .{ .list_val = &nested_list } },
        .{ .key = "embedding", .value = .{ .vector_val = &embedding } },
    };

    try source_db.setNodeProperty(null, node, "tags", .{ .list_val = &tags });
    try source_db.setNodeProperty(null, node, "meta", .{ .map_val = &meta });
    try source_db.setNodeProperty(null, node, "blob", .{ .bytes_val = &blob });
    try source_db.setNodeProperty(null, node, "embedding", .{ .vector_val = &embedding });

    var export_buf: [large_export_buffer_size]u8 = undefined;
    var export_stream = @import("compat").fixedBufferStream(&export_buf);
    const export_stats = try import_export.exportJson(allocator, source_db, export_stream.writer(), .{});
    try std.testing.expectEqual(@as(u64, 1), export_stats.nodes_exported);
    try std.testing.expectEqual(@as(u64, 0), export_stats.edges_exported);

    const import_stats = try import_export.importJsonContent(
        allocator,
        imported_db,
        export_buf[0..export_stream.pos],
        1000,
        false,
    );
    try std.testing.expectEqual(@as(u64, 1), import_stats.nodes_imported);
    try std.testing.expectEqual(@as(u64, 0), import_stats.nodes_failed);

    var imported_tags = (try imported_db.getNodeProperty(1, "tags")).?;
    defer imported_tags.deinit(allocator);
    try std.testing.expect(imported_tags == .list_val);
    try std.testing.expectEqual(@as(usize, 4), imported_tags.list_val.len);
    try std.testing.expectEqualStrings("graph", imported_tags.list_val[0].string_val);
    try std.testing.expectEqual(@as(i64, 7), imported_tags.list_val[1].int_val);
    try std.testing.expectEqual(true, imported_tags.list_val[2].bool_val);
    try std.testing.expect(imported_tags.list_val[3] == .list_val);
    try std.testing.expectEqualStrings("inner", imported_tags.list_val[3].list_val[0].string_val);
    try std.testing.expectEqual(false, imported_tags.list_val[3].list_val[1].bool_val);

    var imported_meta = (try imported_db.getNodeProperty(1, "meta")).?;
    defer imported_meta.deinit(allocator);
    try std.testing.expect(imported_meta == .map_val);
    try std.testing.expectEqual(@as(usize, 4), imported_meta.map_val.len);
    try std.testing.expectEqualStrings("Portland", mapValue(imported_meta.map_val, "city").?.string_val);
    try std.testing.expectApproxEqAbs(@as(f64, 98.5), mapValue(imported_meta.map_val, "score").?.float_val, 0.001);
    const flags = mapValue(imported_meta.map_val, "flags").?;
    try std.testing.expect(flags == .list_val);
    try std.testing.expectEqualStrings("inner", flags.list_val[0].string_val);
    try std.testing.expectEqual(false, flags.list_val[1].bool_val);
    const meta_embedding = mapValue(imported_meta.map_val, "embedding").?;
    try std.testing.expect(meta_embedding == .vector_val);
    try std.testing.expectApproxEqAbs(@as(f32, 0.25), meta_embedding.vector_val[0], 0.001);
    try std.testing.expectApproxEqAbs(@as(f32, 1.5), meta_embedding.vector_val[1], 0.001);
    try std.testing.expectApproxEqAbs(@as(f32, -3.0), meta_embedding.vector_val[2], 0.001);

    var imported_blob = (try imported_db.getNodeProperty(1, "blob")).?;
    defer imported_blob.deinit(allocator);
    try std.testing.expect(imported_blob == .bytes_val);
    try std.testing.expectEqualSlices(u8, blob[0..], imported_blob.bytes_val);

    var imported_embedding = (try imported_db.getNodeProperty(1, "embedding")).?;
    defer imported_embedding.deinit(allocator);
    try std.testing.expect(imported_embedding == .vector_val);
    try std.testing.expectEqual(@as(usize, embedding.len), imported_embedding.vector_val.len);
    try std.testing.expectApproxEqAbs(embedding[0], imported_embedding.vector_val[0], 0.001);
    try std.testing.expectApproxEqAbs(embedding[1], imported_embedding.vector_val[1], 0.001);
    try std.testing.expectApproxEqAbs(embedding[2], imported_embedding.vector_val[2], 0.001);
}

test "import_export: exportCsv deduplicates multi-label nodes and preserves parallel edges" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_export_csv_dedup_test.ltdb";
    const db = try openTestDb(path);
    defer cleanupTestDb(db, path);

    try seedMultiLabelParallelGraph(db);

    var nodes_buf: [small_export_buffer_size]u8 = undefined;
    var nodes_stream = @import("compat").fixedBufferStream(&nodes_buf);

    var edges_buf: [small_export_buffer_size]u8 = undefined;
    var edges_stream = @import("compat").fixedBufferStream(&edges_buf);

    const stats = try import_export.exportCsv(allocator, db, nodes_stream.writer(), edges_stream.writer(), .{});
    try std.testing.expectEqual(@as(u64, 2), stats.nodes_exported);
    try std.testing.expectEqual(@as(u64, 2), stats.edges_exported);

    const nodes_csv = nodes_buf[0..nodes_stream.pos];
    const edges_csv = edges_buf[0..edges_stream.pos];

    try std.testing.expectEqual(@as(usize, 3), countNonEmptyLines(nodes_csv)); // header + 2 nodes
    try std.testing.expectEqual(@as(usize, 3), countNonEmptyLines(edges_csv)); // header + 2 edges
}

test "import_export: exportJsonl emits node and edge records without duplication" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_export_jsonl_test.ltdb";
    const db = try openTestDb(path);
    defer cleanupTestDb(db, path);

    try seedMultiLabelParallelGraph(db);

    var buf: [large_export_buffer_size]u8 = undefined;
    var stream = @import("compat").fixedBufferStream(&buf);

    const stats = try import_export.exportJsonl(allocator, db, stream.writer(), .{});
    try std.testing.expectEqual(@as(u64, 2), stats.nodes_exported);
    try std.testing.expectEqual(@as(u64, 2), stats.edges_exported);

    const output = buf[0..stream.pos];
    var node_ids = std.AutoHashMap(u64, void).init(allocator);
    defer node_ids.deinit();

    var node_count: usize = 0;
    var edge_count: usize = 0;

    var lines = std.mem.splitScalar(u8, output, '\n');
    while (lines.next()) |line| {
        const trimmed = std.mem.trim(u8, line, line_trim_chars);
        if (trimmed.len == 0) continue;

        const parsed = try std.json.parseFromSlice(std.json.Value, allocator, trimmed, .{});
        defer parsed.deinit();

        const root = parsed.value;
        try std.testing.expect(root == .object);
        const kind = root.object.get(json_field_kind).?.string;

        if (std.mem.eql(u8, kind, json_record_kind_node)) {
            node_count += 1;
            const id_str = root.object.get(json_field_id).?.string;
            const id = try std.fmt.parseInt(u64, id_str, decimal_base);
            _ = try node_ids.getOrPut(id);
        } else if (std.mem.eql(u8, kind, json_record_kind_edge)) {
            edge_count += 1;
            _ = root.object.get(json_field_source).?;
            _ = root.object.get(json_field_target).?;
            _ = root.object.get(json_field_type).?;
            const props = root.object.get(json_field_properties).?.object;
            try std.testing.expect(props.get(since_property) != null);
        } else {
            return error.InvalidFormat;
        }
    }

    try std.testing.expectEqual(@as(usize, 2), node_count);
    try std.testing.expectEqual(@as(usize, 2), edge_count);
    try std.testing.expectEqual(@as(usize, 2), node_ids.count());
}

test "import_export: exportDot emits valid graph with deduplicated nodes and parallel edges" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_export_dot_test.ltdb";
    const db = try openTestDb(path);
    defer cleanupTestDb(db, path);

    try seedMultiLabelParallelGraph(db);

    var buf: [large_export_buffer_size]u8 = undefined;
    var stream = @import("compat").fixedBufferStream(&buf);

    const stats = try import_export.exportDot(allocator, db, stream.writer(), .{});
    try std.testing.expectEqual(@as(u64, 2), stats.nodes_exported);
    try std.testing.expectEqual(@as(u64, 2), stats.edges_exported);

    const output = buf[0..stream.pos];
    try std.testing.expect(std.mem.startsWith(u8, output, "digraph G {\n"));
    try std.testing.expect(std.mem.endsWith(u8, output, "}\n"));

    var node_lines: usize = 0;
    var edge_lines: usize = 0;
    var lines = std.mem.splitScalar(u8, output, '\n');
    while (lines.next()) |line| {
        const trimmed = std.mem.trim(u8, line, line_trim_chars);
        if (trimmed.len == 0) continue;
        if (std.mem.indexOf(u8, trimmed, " -> ") != null) {
            edge_lines += 1;
        } else if (std.mem.startsWith(u8, trimmed, "n") and std.mem.endsWith(u8, trimmed, "];")) {
            node_lines += 1;
        }
    }

    try std.testing.expectEqual(@as(usize, 2), node_lines);
    try std.testing.expectEqual(@as(usize, 2), edge_lines);
}

test "import_export: all export formats include unlabeled nodes" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_export_unlabeled_test.ltdb";
    const db = try openTestDb(path);
    defer cleanupTestDb(db, path);

    try seedGraphWithUnlabeledNode(db);

    var json_buf: [small_export_buffer_size]u8 = undefined;
    var json_stream = @import("compat").fixedBufferStream(&json_buf);
    const json_stats = try import_export.exportJson(allocator, db, json_stream.writer(), .{});
    try std.testing.expectEqual(@as(u64, 2), json_stats.nodes_exported);
    try std.testing.expectEqual(@as(u64, 1), json_stats.edges_exported);

    const json_output = json_buf[0..json_stream.pos];
    const json_parsed = try std.json.parseFromSlice(std.json.Value, allocator, json_output, .{});
    defer json_parsed.deinit();
    const json_nodes = json_parsed.value.object.get(json_field_nodes).?.array.items;
    try std.testing.expectEqual(@as(usize, 2), json_nodes.len);
    var found_empty_labels_json = false;
    for (json_nodes) |node| {
        const labels = node.object.get(json_field_labels).?.array.items;
        if (labels.len == 0) found_empty_labels_json = true;
    }
    try std.testing.expect(found_empty_labels_json);

    var jsonl_buf: [small_export_buffer_size]u8 = undefined;
    var jsonl_stream = @import("compat").fixedBufferStream(&jsonl_buf);
    const jsonl_stats = try import_export.exportJsonl(allocator, db, jsonl_stream.writer(), .{});
    try std.testing.expectEqual(@as(u64, 2), jsonl_stats.nodes_exported);
    try std.testing.expectEqual(@as(u64, 1), jsonl_stats.edges_exported);

    var found_empty_labels_jsonl = false;
    var jsonl_lines = std.mem.splitScalar(u8, jsonl_buf[0..jsonl_stream.pos], '\n');
    while (jsonl_lines.next()) |line| {
        const trimmed = std.mem.trim(u8, line, line_trim_chars);
        if (trimmed.len == 0) continue;

        const parsed = try std.json.parseFromSlice(std.json.Value, allocator, trimmed, .{});
        defer parsed.deinit();

        if (std.mem.eql(u8, parsed.value.object.get(json_field_kind).?.string, json_record_kind_node)) {
            const labels = parsed.value.object.get(json_field_labels).?.array.items;
            if (labels.len == 0) found_empty_labels_jsonl = true;
        }
    }
    try std.testing.expect(found_empty_labels_jsonl);

    var nodes_csv_buf: [small_export_buffer_size]u8 = undefined;
    var nodes_csv_stream = @import("compat").fixedBufferStream(&nodes_csv_buf);
    var edges_csv_buf: [small_export_buffer_size]u8 = undefined;
    var edges_csv_stream = @import("compat").fixedBufferStream(&edges_csv_buf);
    const csv_stats = try import_export.exportCsv(allocator, db, nodes_csv_stream.writer(), edges_csv_stream.writer(), .{});
    try std.testing.expectEqual(@as(u64, 2), csv_stats.nodes_exported);
    try std.testing.expectEqual(@as(u64, 1), csv_stats.edges_exported);
    try std.testing.expect(std.mem.indexOf(u8, nodes_csv_buf[0..nodes_csv_stream.pos], "2,\"\"") != null);

    var dot_buf: [small_export_buffer_size]u8 = undefined;
    var dot_stream = @import("compat").fixedBufferStream(&dot_buf);
    const dot_stats = try import_export.exportDot(allocator, db, dot_stream.writer(), .{});
    try std.testing.expectEqual(@as(u64, 2), dot_stats.nodes_exported);
    try std.testing.expectEqual(@as(u64, 1), dot_stats.edges_exported);
    const dot_output = dot_buf[0..dot_stream.pos];
    try std.testing.expect(std.mem.indexOf(u8, dot_output, "n2 [label=\"2\"];\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, dot_output, "n1 -> n2 [label=\"REL\"];\n") != null);
}

test "import_export: comma-separated label filter unions matching nodes without duplicates" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_export_multilabel_filter_test.ltdb";
    const db = try openTestDb(path);
    defer cleanupTestDb(db, path);

    try seedGraphForMultiLabelFilter(db);

    var json_buf: [small_export_buffer_size]u8 = undefined;
    var json_stream = @import("compat").fixedBufferStream(&json_buf);
    const json_stats = try import_export.exportJson(allocator, db, json_stream.writer(), .{ .label_filter = person_label ++ ", " ++ employee_label });
    try std.testing.expectEqual(@as(u64, 3), json_stats.nodes_exported);
    try std.testing.expectEqual(@as(u64, 2), json_stats.edges_exported);

    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, json_buf[0..json_stream.pos], .{});
    defer parsed.deinit();

    const nodes = parsed.value.object.get(json_field_nodes).?.array.items;
    const edges = parsed.value.object.get(json_field_edges).?.array.items;
    try std.testing.expectEqual(@as(usize, 3), nodes.len);
    try std.testing.expectEqual(@as(usize, 2), edges.len);

    var ids = std.AutoHashMap(u64, void).init(allocator);
    defer ids.deinit();
    for (nodes) |node| {
        const id = try std.fmt.parseInt(u64, node.object.get(json_field_id).?.string, decimal_base);
        _ = try ids.getOrPut(id);
    }
    try std.testing.expectEqual(@as(usize, 3), ids.count());

    var dump_buf: [small_export_buffer_size]u8 = undefined;
    var dump_stream = @import("compat").fixedBufferStream(&dump_buf);
    const dump_stats = try import_export.dumpCanonicalJson(allocator, db, dump_stream.writer(), .{ .label_filter = person_label ++ "," ++ employee_label });
    try std.testing.expectEqual(@as(u64, 3), dump_stats.nodes_exported);
    try std.testing.expectEqual(@as(u64, 2), dump_stats.edges_exported);
}

test "import_export: label-filtered exports keep only edges within exported node set" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_export_label_scoped_edges_test.ltdb";
    const db = try openTestDb(path);
    defer cleanupTestDb(db, path);

    try seedGraphForLabelScopedExport(db);

    var json_buf: [small_export_buffer_size]u8 = undefined;
    var json_stream = @import("compat").fixedBufferStream(&json_buf);
    const json_stats = try import_export.exportJson(allocator, db, json_stream.writer(), .{ .label_filter = person_label });
    try std.testing.expectEqual(@as(u64, 1), json_stats.nodes_exported);
    try std.testing.expectEqual(@as(u64, 0), json_stats.edges_exported);
    const json_parsed = try std.json.parseFromSlice(std.json.Value, allocator, json_buf[0..json_stream.pos], .{});
    defer json_parsed.deinit();
    try std.testing.expectEqual(@as(usize, 1), json_parsed.value.object.get(json_field_nodes).?.array.items.len);
    try std.testing.expectEqual(@as(usize, 0), json_parsed.value.object.get(json_field_edges).?.array.items.len);

    var jsonl_buf: [small_export_buffer_size]u8 = undefined;
    var jsonl_stream = @import("compat").fixedBufferStream(&jsonl_buf);
    const jsonl_stats = try import_export.exportJsonl(allocator, db, jsonl_stream.writer(), .{ .label_filter = person_label });
    try std.testing.expectEqual(@as(u64, 1), jsonl_stats.nodes_exported);
    try std.testing.expectEqual(@as(u64, 0), jsonl_stats.edges_exported);
    try std.testing.expectEqual(@as(usize, 1), countNonEmptyLines(jsonl_buf[0..jsonl_stream.pos]));

    var nodes_csv_buf: [small_export_buffer_size]u8 = undefined;
    var nodes_csv_stream = @import("compat").fixedBufferStream(&nodes_csv_buf);
    var edges_csv_buf: [small_export_buffer_size]u8 = undefined;
    var edges_csv_stream = @import("compat").fixedBufferStream(&edges_csv_buf);
    const csv_stats = try import_export.exportCsv(allocator, db, nodes_csv_stream.writer(), edges_csv_stream.writer(), .{ .label_filter = person_label });
    try std.testing.expectEqual(@as(u64, 1), csv_stats.nodes_exported);
    try std.testing.expectEqual(@as(u64, 0), csv_stats.edges_exported);
    try std.testing.expectEqual(@as(usize, 2), countNonEmptyLines(nodes_csv_buf[0..nodes_csv_stream.pos]));
    try std.testing.expectEqual(@as(usize, 1), countNonEmptyLines(edges_csv_buf[0..edges_csv_stream.pos]));

    var dot_buf: [small_export_buffer_size]u8 = undefined;
    var dot_stream = @import("compat").fixedBufferStream(&dot_buf);
    const dot_stats = try import_export.exportDot(allocator, db, dot_stream.writer(), .{ .label_filter = person_label });
    try std.testing.expectEqual(@as(u64, 1), dot_stats.nodes_exported);
    try std.testing.expectEqual(@as(u64, 0), dot_stats.edges_exported);
    const dot_output = dot_buf[0..dot_stream.pos];
    try std.testing.expect(std.mem.indexOf(u8, dot_output, "n1 [label=\"1 : Person\"];\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, dot_output, works_at_type) == null);

    var dump_buf: [small_export_buffer_size]u8 = undefined;
    var dump_stream = @import("compat").fixedBufferStream(&dump_buf);
    const dump_stats = try import_export.dumpCanonicalJson(allocator, db, dump_stream.writer(), .{ .label_filter = person_label });
    try std.testing.expectEqual(@as(u64, 1), dump_stats.nodes_exported);
    try std.testing.expectEqual(@as(u64, 0), dump_stats.edges_exported);
}

test "import_export: JSON-family exports fail on unreadable node properties" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_export_json_node_error_test.ltdb";
    const db = try openTestDb(path);
    defer cleanupTestDb(db, path);

    const node = try db.createNode(null, &.{person_label});
    try db.setNodeProperty(null, node, name_property, .{ .string_val = alice_name });
    try overwriteNodePayloadWithInvalidData(db, node);

    var json_buf: [small_export_buffer_size]u8 = undefined;
    var json_stream = @import("compat").fixedBufferStream(&json_buf);
    try std.testing.expectError(
        import_export.ImportExportError.DatabaseError,
        import_export.exportJson(allocator, db, json_stream.writer(), .{}),
    );

    var jsonl_buf: [small_export_buffer_size]u8 = undefined;
    var jsonl_stream = @import("compat").fixedBufferStream(&jsonl_buf);
    try std.testing.expectError(
        import_export.ImportExportError.DatabaseError,
        import_export.exportJsonl(allocator, db, jsonl_stream.writer(), .{}),
    );

    var canonical_buf: [small_export_buffer_size]u8 = undefined;
    var canonical_stream = @import("compat").fixedBufferStream(&canonical_buf);
    try std.testing.expectError(
        import_export.ImportExportError.DatabaseError,
        import_export.dumpCanonicalJson(allocator, db, canonical_stream.writer(), .{}),
    );
}

test "import_export: JSON-family exports fail on unreadable edge properties" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_export_json_edge_error_test.ltdb";
    const db = try openTestDb(path);
    defer cleanupTestDb(db, path);

    const alice = try db.createNode(null, &.{person_label});
    const bob = try db.createNode(null, &.{person_label});
    const edge_id = try db.createEdgeAndGetId(null, alice, bob, rel_type);
    try db.setEdgePropertyById(null, edge_id, since_property, .{ .int_val = first_since_year });
    try overwriteEdgePayloadWithInvalidData(db, edge_id);

    var json_buf: [small_export_buffer_size]u8 = undefined;
    var json_stream = @import("compat").fixedBufferStream(&json_buf);
    try std.testing.expectError(
        import_export.ImportExportError.DatabaseError,
        import_export.exportJson(allocator, db, json_stream.writer(), .{}),
    );

    var jsonl_buf: [small_export_buffer_size]u8 = undefined;
    var jsonl_stream = @import("compat").fixedBufferStream(&jsonl_buf);
    try std.testing.expectError(
        import_export.ImportExportError.DatabaseError,
        import_export.exportJsonl(allocator, db, jsonl_stream.writer(), .{}),
    );

    var canonical_buf: [small_export_buffer_size]u8 = undefined;
    var canonical_stream = @import("compat").fixedBufferStream(&canonical_buf);
    try std.testing.expectError(
        import_export.ImportExportError.DatabaseError,
        import_export.dumpCanonicalJson(allocator, db, canonical_stream.writer(), .{}),
    );
}

fn expectStringProperty(db: *Database, node_id: u64, key: []const u8, expected: []const u8) !void {
    var value = (try db.getNodeProperty(node_id, key)) orelse return error.TestUnexpectedResult;
    defer value.deinit(std.testing.allocator);
    if (value != .string_val) {
        std.debug.print("property {s}: expected string \"{s}\", got {s}\n", .{ key, expected, @tagName(value) });
        return error.TestUnexpectedResult;
    }
    try std.testing.expectEqualStrings(expected, value.string_val);
}

fn exportJsonAlloc(allocator: std.mem.Allocator, db: *Database) ![]u8 {
    var list: std.ArrayList(u8) = .empty;
    errdefer list.deinit(allocator);
    _ = try import_export.exportJson(allocator, db, @import("compat").arrayListWriterWithAllocator(&list, allocator), .{});
    return list.toOwnedSlice(allocator);
}

test "import_export: JSON import keeps strings that look like hex or numbers as strings" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_import_hexlike_strings.ltdb";
    const db = try openTestDb(path);
    defer cleanupTestDb(db, path);

    // Unversioned, as written by hand or by 0.16.0.
    const json_content =
        \\{"nodes":[{"id":"a","labels":["Thing"],"properties":{"year":"2024","word":"cafe","hex":"DEADBEEF","odd":"abc"}}]}
    ;
    _ = try import_export.importJsonContent(allocator, db, json_content, 1000, false);

    try expectStringProperty(db, 1, "year", "2024");
    try expectStringProperty(db, 1, "word", "cafe");
    try expectStringProperty(db, 1, "hex", "DEADBEEF");
    try expectStringProperty(db, 1, "odd", "abc");
}

test "import_export: JSON export and import keep every value type distinct" {
    const allocator = std.testing.allocator;
    const source_path = "/tmp/lattice_export_value_types_source.ltdb";
    const imported_path = "/tmp/lattice_export_value_types_imported.ltdb";
    const source = try openTestDb(source_path);
    defer cleanupTestDb(source, source_path);
    const imported = try openTestDb(imported_path);
    defer cleanupTestDb(imported, imported_path);

    const node = try source.createNode(null, &.{person_label});
    const ints = [_]PropertyValue{ .{ .int_val = 1 }, .{ .int_val = 2 } };
    const embedding = [_]f32{ 0.1, -2.5, 3.0e-8 };
    const looks_tagged = [_]PropertyValue.MapEntry{.{ .key = "$bytes", .value = .{ .string_val = "AAAA" } }};
    try source.setNodeProperty(null, node, "hexlike", .{ .string_val = "cafe" });
    try source.setNodeProperty(null, node, "bytes", .{ .bytes_val = &[_]u8{ 0xca, 0xfe, 0x00, 0xff } });
    try source.setNodeProperty(null, node, "empty_bytes", .{ .bytes_val = &[_]u8{} });
    try source.setNodeProperty(null, node, "whole_float", .{ .float_val = 1.0 });
    try source.setNodeProperty(null, node, "huge_float", .{ .float_val = 1e300 });
    try source.setNodeProperty(null, node, "nan", .{ .float_val = std.math.nan(f64) });
    try source.setNodeProperty(null, node, "int", .{ .int_val = -9223372036854775807 });
    try source.setNodeProperty(null, node, "ints", .{ .list_val = &ints });
    try source.setNodeProperty(null, node, "empty_list", .{ .list_val = &.{} });
    try source.setNodeProperty(null, node, "embedding", .{ .vector_val = &embedding });
    try source.setNodeProperty(null, node, "looks_tagged", .{ .map_val = &looks_tagged });

    const first = try exportJsonAlloc(allocator, source);
    defer allocator.free(first);
    _ = try import_export.importJsonContent(allocator, imported, first, 1000, false);
    const second = try exportJsonAlloc(allocator, imported);
    defer allocator.free(second);
    try std.testing.expectEqualStrings(first, second);

    try expectStringProperty(imported, 1, "hexlike", "cafe");

    var bytes = (try imported.getNodeProperty(1, "bytes")).?;
    defer bytes.deinit(allocator);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0xca, 0xfe, 0x00, 0xff }, bytes.bytes_val);

    var empty_bytes = (try imported.getNodeProperty(1, "empty_bytes")).?;
    defer empty_bytes.deinit(allocator);
    try std.testing.expect(empty_bytes == .bytes_val);

    var whole = (try imported.getNodeProperty(1, "whole_float")).?;
    defer whole.deinit(allocator);
    try std.testing.expect(whole == .float_val);
    try std.testing.expectEqual(@as(f64, 1.0), whole.float_val);

    var huge = (try imported.getNodeProperty(1, "huge_float")).?;
    defer huge.deinit(allocator);
    try std.testing.expectEqual(@as(f64, 1e300), huge.float_val);

    var nan = (try imported.getNodeProperty(1, "nan")).?;
    defer nan.deinit(allocator);
    try std.testing.expect(std.math.isNan(nan.float_val));

    var ints_back = (try imported.getNodeProperty(1, "ints")).?;
    defer ints_back.deinit(allocator);
    try std.testing.expect(ints_back == .list_val);
    try std.testing.expectEqual(@as(i64, 2), ints_back.list_val[1].int_val);

    var empty_list = (try imported.getNodeProperty(1, "empty_list")).?;
    defer empty_list.deinit(allocator);
    try std.testing.expect(empty_list == .list_val);

    var vector = (try imported.getNodeProperty(1, "embedding")).?;
    defer vector.deinit(allocator);
    try std.testing.expect(vector == .vector_val);
    try std.testing.expectEqualSlices(f32, &embedding, vector.vector_val);

    var map = (try imported.getNodeProperty(1, "looks_tagged")).?;
    defer map.deinit(allocator);
    try std.testing.expect(map == .map_val);
    try std.testing.expectEqualStrings("AAAA", mapValue(map.map_val, "$bytes").?.string_val);
}

test "import_export: JSON import reads 0.16.0 files and refuses versions it does not know" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_import_format_versions.ltdb";
    const db = try openTestDb(path);
    defer cleanupTestDb(db, path);

    // 0.16.0 wrote vector properties as plain arrays of numbers.
    const version_one =
        \\{"nodes":[{"id":"1","labels":[],"properties":{"embedding":[0.5,1,-2],"n":3,"x":2.5}}],"edges":[]}
    ;
    _ = try import_export.importJsonContent(allocator, db, version_one, 1000, false);
    var embedding = (try db.getNodeProperty(1, "embedding")).?;
    defer embedding.deinit(allocator);
    try std.testing.expect(embedding == .vector_val);
    try std.testing.expectEqualSlices(f32, &[_]f32{ 0.5, 1, -2 }, embedding.vector_val);

    try std.testing.expectError(
        import_export.ImportExportError.UnsupportedVersion,
        import_export.importJsonContent(allocator, db, "{\"version\":3,\"nodes\":[]}", 1000, false),
    );
}

const test_vector_dims = 4;

fn openVectorTestDb(path: []const u8) !*Database {
    @import("compat").fs.cwd().deleteFile(path) catch {};
    return try Database.open(std.testing.allocator, path, .{
        .create = true,
        .config = .{
            .enable_wal = false,
            .enable_fts = false,
            .enable_vector = true,
            .vector_dimensions = test_vector_dims,
        },
    });
}

fn exportAlloc(
    allocator: std.mem.Allocator,
    db: *Database,
    comptime exporter: anytype,
) ![]u8 {
    var list: std.ArrayList(u8) = .empty;
    errdefer list.deinit(allocator);
    _ = try exporter(allocator, db, @import("compat").arrayListWriterWithAllocator(&list, allocator), .{});
    return list.toOwnedSlice(allocator);
}

test "import_export: export writes node vectors and import restores them" {
    const allocator = std.testing.allocator;
    const source_path = "/tmp/lattice_export_vectors_source.ltdb";
    const imported_path = "/tmp/lattice_export_vectors_imported.ltdb";
    const source = try openVectorTestDb(source_path);
    defer cleanupTestDb(source, source_path);
    const imported = try openVectorTestDb(imported_path);
    defer cleanupTestDb(imported, imported_path);

    // Unit vectors along an axis are stored exactly as given.
    const with_vector = try source.createNode(null, &.{"Chunk"});
    const without_vector = try source.createNode(null, &.{"Chunk"});
    try source.setNodeVector(with_vector, &[_]f32{ 0, 1, 0, 0 });
    _ = try source.createEdgeAndGetId(null, with_vector, without_vector, rel_type);

    const first = try exportAlloc(allocator, source, import_export.exportJson);
    defer allocator.free(first);
    try std.testing.expect(std.mem.indexOf(u8, first, "\"vector_dimensions\":4") != null);
    try std.testing.expect(std.mem.indexOf(u8, first, "\"vector\":[0.0,1.0,0.0,0.0]") != null);

    _ = try import_export.importJsonContent(allocator, imported, first, 1000, false);
    const vector = (try imported.getNodeVectorInTxn(null, 1)).?;
    defer allocator.free(vector);
    try std.testing.expectEqualSlices(f32, &[_]f32{ 0, 1, 0, 0 }, vector);
    try std.testing.expect((try imported.getNodeVectorInTxn(null, 2)) == null);

    const second = try exportAlloc(allocator, imported, import_export.exportJson);
    defer allocator.free(second);
    try std.testing.expectEqualStrings(first, second);

    const jsonl = try exportAlloc(allocator, source, import_export.exportJsonl);
    defer allocator.free(jsonl);
    try std.testing.expect(std.mem.indexOf(u8, jsonl, "\"vector\":[0.0,1.0,0.0,0.0]") != null);

    const dump = try exportAlloc(allocator, source, import_export.dumpCanonicalJson);
    defer allocator.free(dump);
    try std.testing.expect(std.mem.indexOf(u8, dump, "\"vector\":[0.0,1.0,0.0,0.0]") != null);
}

test "import_export: importing vectors needs a database opened with matching vectors" {
    const allocator = std.testing.allocator;
    const plain_path = "/tmp/lattice_import_vectors_plain.ltdb";
    const plain = try openTestDb(plain_path);
    defer cleanupTestDb(plain, plain_path);

    const with_vectors =
        \\{"version":2,"vector_dimensions":4,"nodes":[{"id":"a","labels":[],"properties":{},"vector":[1,0,0,0]}],"edges":[]}
    ;
    try std.testing.expectError(
        import_export.ImportExportError.VectorsNotEnabled,
        import_export.importJsonContent(allocator, plain, with_vectors, 1000, false),
    );
    // Refused before anything was written.
    try std.testing.expectEqual(@as(u64, 0), plain.nodeCount());

    const vector_path = "/tmp/lattice_import_vectors_mismatch.ltdb";
    const vector_db = try openVectorTestDb(vector_path);
    defer cleanupTestDb(vector_db, vector_path);
    const three_dims =
        \\{"version":2,"vector_dimensions":3,"nodes":[{"id":"a","labels":[],"properties":{},"vector":[1,0,0]}],"edges":[]}
    ;
    try std.testing.expectError(
        import_export.ImportExportError.VectorDimensionMismatch,
        import_export.importJsonContent(allocator, vector_db, three_dims, 1000, false),
    );
    try std.testing.expectEqual(@as(u64, 0), vector_db.nodeCount());
}

test "import_export: export by query writes the nodes and edges the query returned" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_export_by_query.ltdb";
    const db = try openTestDb(path);
    defer cleanupTestDb(db, path);

    try seedGraphForLabelScopedExport(db); // Alice -WORKS_AT-> Acme
    const carol = try db.createNode(null, &.{person_label});
    _ = try db.createEdgeAndGetId(null, 1, carol, rel_type);

    // Nodes only: the edge between Alice and Carol was not returned.
    {
        var result = try db.query("MATCH (p:Person) RETURN p");
        defer result.deinit();
        var buf: [small_export_buffer_size]u8 = undefined;
        var stream = @import("compat").fixedBufferStream(&buf);
        const stats = try import_export.exportJson(allocator, db, stream.writer(), .{ .query_result = &result });
        try std.testing.expectEqual(@as(u64, 2), stats.nodes_exported);
        try std.testing.expectEqual(@as(u64, 0), stats.edges_exported);
    }

    // An edge brings its endpoints, and only that edge.
    {
        var result = try db.query("MATCH (p)-[r:WORKS_AT]->(c) RETURN r");
        defer result.deinit();
        var buf: [small_export_buffer_size]u8 = undefined;
        var stream = @import("compat").fixedBufferStream(&buf);
        const stats = try import_export.exportJson(allocator, db, stream.writer(), .{ .query_result = &result });
        try std.testing.expectEqual(@as(u64, 2), stats.nodes_exported);
        try std.testing.expectEqual(@as(u64, 1), stats.edges_exported);
        try std.testing.expect(std.mem.indexOf(u8, buf[0..stream.pos], works_at_type) != null);
        try std.testing.expect(std.mem.indexOf(u8, buf[0..stream.pos], "\"" ++ rel_type ++ "\"") == null);
    }

    // Rows with nothing to export are refused rather than written as an empty graph.
    {
        var result = try db.query("MATCH (p:Person) RETURN p.name");
        defer result.deinit();
        var buf: [small_export_buffer_size]u8 = undefined;
        var stream = @import("compat").fixedBufferStream(&buf);
        try std.testing.expectError(
            import_export.ImportExportError.QueryReturnedNoGraph,
            import_export.exportJson(allocator, db, stream.writer(), .{ .query_result = &result }),
        );
    }
}

test "import_export: CSV import reads quoted fields as RFC 4180 describes" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_import_csv_rfc4180.ltdb";
    const db = try openTestDb(path);
    defer cleanupTestDb(db, path);

    const nodes_csv = "_id,_labels,name,bio,quote\n" ++
        "n1,\"Person;Employee\",\"Smith, Alice\",\"line one\nline two\",\"she said \"\"hi\"\"\"\n" ++
        "n2, Person ,Bob,,plain\n";
    const stats = try import_export.importCsvContent(allocator, db, nodes_csv, 1000, false);
    try std.testing.expectEqual(@as(u64, 2), stats.nodes_imported);

    try expectStringProperty(db, 1, "name", "Smith, Alice");
    try expectStringProperty(db, 1, "bio", "line one\nline two");
    try expectStringProperty(db, 1, "quote", "she said \"hi\"");
    try expectStringProperty(db, 2, "name", "Bob");
    try expectStringProperty(db, 2, "quote", "plain");
    try std.testing.expect((try db.getNodeProperty(2, "bio")) == null);

    const labels = try db.getNodeLabels(1);
    defer {
        for (labels) |label| allocator.free(label);
        allocator.free(labels);
    }
    try std.testing.expectEqual(@as(usize, 2), labels.len);

    const edges_csv = "_source,_target,_type\n1,2,\"KNOWS, WELL\"\n";
    _ = try import_export.importCsvContent(allocator, db, edges_csv, 1000, false);
    const edges = try db.getOutgoingEdges(1);
    defer db.freeEdgeInfos(edges);
    try std.testing.expectEqual(@as(usize, 1), edges.len);
    try std.testing.expectEqualStrings("KNOWS, WELL", edges[0].edge_type);

    try std.testing.expectError(
        import_export.ImportExportError.ParseError,
        import_export.importCsvContent(allocator, db, "_id,_labels,name\nn3,Person,\"unterminated\n", 1000, false),
    );
}

const CsvExport = struct {
    nodes: []u8,
    edges: []u8,

    fn deinit(self: CsvExport, allocator: std.mem.Allocator) void {
        allocator.free(self.nodes);
        allocator.free(self.edges);
    }
};

fn exportCsvAlloc(allocator: std.mem.Allocator, db: *Database) !CsvExport {
    const compat = @import("compat");
    var nodes: std.ArrayList(u8) = .empty;
    errdefer nodes.deinit(allocator);
    var edges: std.ArrayList(u8) = .empty;
    errdefer edges.deinit(allocator);
    _ = try import_export.exportCsv(
        allocator,
        db,
        compat.arrayListWriterWithAllocator(&nodes, allocator),
        compat.arrayListWriterWithAllocator(&edges, allocator),
        .{},
    );
    return .{ .nodes = try nodes.toOwnedSlice(allocator), .edges = try edges.toOwnedSlice(allocator) };
}

test "import_export: CSV export and import keep properties, types, ids and vectors" {
    const allocator = std.testing.allocator;
    const source_path = "/tmp/lattice_csv_roundtrip_source.ltdb";
    const imported_path = "/tmp/lattice_csv_roundtrip_imported.ltdb";
    const source = try openVectorTestDb(source_path);
    defer cleanupTestDb(source, source_path);
    const imported = try openVectorTestDb(imported_path);
    defer cleanupTestDb(imported, imported_path);

    const alice = try source.createNode(null, &.{ person_label, employee_label });
    const bob = try source.createNode(null, &.{});
    try source.setNodeProperty(null, alice, name_property, .{ .string_val = "Smith, \"Alice\"\nJr." });
    try source.setNodeProperty(null, alice, "code", .{ .string_val = "2024" });
    try source.setNodeProperty(null, alice, "blob", .{ .bytes_val = &[_]u8{ 1, 2, 3 } });
    try source.setNodeProperty(null, alice, "score", .{ .float_val = 2.0 });
    try source.setNodeProperty(null, bob, "nothing", .{ .null_val = {} });
    try source.setNodeVector(alice, &[_]f32{ 0, 0, 1, 0 });
    const first = try source.createEdgeAndGetId(null, alice, bob, rel_type);
    const second = try source.createEdgeAndGetId(null, alice, bob, rel_type);
    try source.setEdgePropertyById(null, first, since_property, .{ .int_val = first_since_year });
    try source.setEdgePropertyById(null, second, status_property, .{ .string_val = "a,b" });

    const exported = try exportCsvAlloc(allocator, source);
    defer exported.deinit(allocator);

    const stats = try import_export.importCsvPairContent(allocator, imported, exported.nodes, exported.edges, 1000, false);
    try std.testing.expectEqual(@as(u64, 2), stats.nodes_imported);
    try std.testing.expectEqual(@as(u64, 2), stats.edges_imported);

    const again = try exportCsvAlloc(allocator, imported);
    defer again.deinit(allocator);
    try std.testing.expectEqualStrings(exported.nodes, again.nodes);
    try std.testing.expectEqualStrings(exported.edges, again.edges);

    try expectStringProperty(imported, 1, "code", "2024");
    var blob = (try imported.getNodeProperty(1, "blob")).?;
    defer blob.deinit(allocator);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 1, 2, 3 }, blob.bytes_val);
    var nothing = (try imported.getNodeProperty(2, "nothing")).?;
    defer nothing.deinit(allocator);
    try std.testing.expect(nothing == .null_val);
    const vector = (try imported.getNodeVectorInTxn(null, 1)).?;
    defer allocator.free(vector);
    try std.testing.expectEqualSlices(f32, &[_]f32{ 0, 0, 1, 0 }, vector);
}

test "import_export: CSV import refuses a repeated _id and CSV export a label it cannot hold" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_csv_duplicate_id.ltdb";
    const db = try openTestDb(path);
    defer cleanupTestDb(db, path);

    try std.testing.expectError(
        import_export.ImportExportError.DatabaseError,
        import_export.importCsvContent(allocator, db, "_id,_labels\nn1,A\nn1,B\n", 1000, false),
    );

    _ = try db.createNode(null, &.{"has;semicolon"});
    try std.testing.expectError(import_export.ImportExportError.LabelNotRepresentable, exportCsvAlloc(allocator, db));
}
