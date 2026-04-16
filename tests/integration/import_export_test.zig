//! End-to-end integration tests for graph export formats.
//!
//! Verifies JSON, CSV, JSONL, and DOT exports against real database state.

const std = @import("std");
const lattice = @import("lattice");

const Database = lattice.storage.database.Database;
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
    std.fs.cwd().deleteFile(path) catch {};
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
    std.fs.cwd().deleteFile(path) catch {};
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

test "import_export: exportJson deduplicates multi-label nodes and preserves parallel edges" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_export_json_dedup_test.ltdb";
    const db = try openTestDb(path);
    defer cleanupTestDb(db, path);

    try seedMultiLabelParallelGraph(db);

    var buf: [large_export_buffer_size]u8 = undefined;
    var stream = std.io.fixedBufferStream(&buf);

    const stats = try import_export.exportJson(allocator, db, stream.writer(), null);
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

test "import_export: exportCsv deduplicates multi-label nodes and preserves parallel edges" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_export_csv_dedup_test.ltdb";
    const db = try openTestDb(path);
    defer cleanupTestDb(db, path);

    try seedMultiLabelParallelGraph(db);

    var nodes_buf: [small_export_buffer_size]u8 = undefined;
    var nodes_stream = std.io.fixedBufferStream(&nodes_buf);

    var edges_buf: [small_export_buffer_size]u8 = undefined;
    var edges_stream = std.io.fixedBufferStream(&edges_buf);

    const stats = try import_export.exportCsv(allocator, db, nodes_stream.writer(), edges_stream.writer(), null);
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
    var stream = std.io.fixedBufferStream(&buf);

    const stats = try import_export.exportJsonl(allocator, db, stream.writer(), null);
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
    var stream = std.io.fixedBufferStream(&buf);

    const stats = try import_export.exportDot(allocator, db, stream.writer(), null);
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
    var json_stream = std.io.fixedBufferStream(&json_buf);
    const json_stats = try import_export.exportJson(allocator, db, json_stream.writer(), null);
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
    var jsonl_stream = std.io.fixedBufferStream(&jsonl_buf);
    const jsonl_stats = try import_export.exportJsonl(allocator, db, jsonl_stream.writer(), null);
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
    var nodes_csv_stream = std.io.fixedBufferStream(&nodes_csv_buf);
    var edges_csv_buf: [small_export_buffer_size]u8 = undefined;
    var edges_csv_stream = std.io.fixedBufferStream(&edges_csv_buf);
    const csv_stats = try import_export.exportCsv(
        allocator,
        db,
        nodes_csv_stream.writer(),
        edges_csv_stream.writer(),
        null,
    );
    try std.testing.expectEqual(@as(u64, 2), csv_stats.nodes_exported);
    try std.testing.expectEqual(@as(u64, 1), csv_stats.edges_exported);
    try std.testing.expect(std.mem.indexOf(u8, nodes_csv_buf[0..nodes_csv_stream.pos], "2,\"\"") != null);

    var dot_buf: [small_export_buffer_size]u8 = undefined;
    var dot_stream = std.io.fixedBufferStream(&dot_buf);
    const dot_stats = try import_export.exportDot(allocator, db, dot_stream.writer(), null);
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
    var json_stream = std.io.fixedBufferStream(&json_buf);
    const json_stats = try import_export.exportJson(allocator, db, json_stream.writer(), person_label ++ ", " ++ employee_label);
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
    var dump_stream = std.io.fixedBufferStream(&dump_buf);
    const dump_stats = try import_export.dumpCanonicalJson(allocator, db, dump_stream.writer(), person_label ++ "," ++ employee_label);
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
    var json_stream = std.io.fixedBufferStream(&json_buf);
    const json_stats = try import_export.exportJson(allocator, db, json_stream.writer(), person_label);
    try std.testing.expectEqual(@as(u64, 1), json_stats.nodes_exported);
    try std.testing.expectEqual(@as(u64, 0), json_stats.edges_exported);
    const json_parsed = try std.json.parseFromSlice(std.json.Value, allocator, json_buf[0..json_stream.pos], .{});
    defer json_parsed.deinit();
    try std.testing.expectEqual(@as(usize, 1), json_parsed.value.object.get(json_field_nodes).?.array.items.len);
    try std.testing.expectEqual(@as(usize, 0), json_parsed.value.object.get(json_field_edges).?.array.items.len);

    var jsonl_buf: [small_export_buffer_size]u8 = undefined;
    var jsonl_stream = std.io.fixedBufferStream(&jsonl_buf);
    const jsonl_stats = try import_export.exportJsonl(allocator, db, jsonl_stream.writer(), person_label);
    try std.testing.expectEqual(@as(u64, 1), jsonl_stats.nodes_exported);
    try std.testing.expectEqual(@as(u64, 0), jsonl_stats.edges_exported);
    try std.testing.expectEqual(@as(usize, 1), countNonEmptyLines(jsonl_buf[0..jsonl_stream.pos]));

    var nodes_csv_buf: [small_export_buffer_size]u8 = undefined;
    var nodes_csv_stream = std.io.fixedBufferStream(&nodes_csv_buf);
    var edges_csv_buf: [small_export_buffer_size]u8 = undefined;
    var edges_csv_stream = std.io.fixedBufferStream(&edges_csv_buf);
    const csv_stats = try import_export.exportCsv(
        allocator,
        db,
        nodes_csv_stream.writer(),
        edges_csv_stream.writer(),
        person_label,
    );
    try std.testing.expectEqual(@as(u64, 1), csv_stats.nodes_exported);
    try std.testing.expectEqual(@as(u64, 0), csv_stats.edges_exported);
    try std.testing.expectEqual(@as(usize, 2), countNonEmptyLines(nodes_csv_buf[0..nodes_csv_stream.pos]));
    try std.testing.expectEqual(@as(usize, 1), countNonEmptyLines(edges_csv_buf[0..edges_csv_stream.pos]));

    var dot_buf: [small_export_buffer_size]u8 = undefined;
    var dot_stream = std.io.fixedBufferStream(&dot_buf);
    const dot_stats = try import_export.exportDot(allocator, db, dot_stream.writer(), person_label);
    try std.testing.expectEqual(@as(u64, 1), dot_stats.nodes_exported);
    try std.testing.expectEqual(@as(u64, 0), dot_stats.edges_exported);
    const dot_output = dot_buf[0..dot_stream.pos];
    try std.testing.expect(std.mem.indexOf(u8, dot_output, "n1 [label=\"1 : Person\"];\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, dot_output, works_at_type) == null);

    var dump_buf: [small_export_buffer_size]u8 = undefined;
    var dump_stream = std.io.fixedBufferStream(&dump_buf);
    const dump_stats = try import_export.dumpCanonicalJson(allocator, db, dump_stream.writer(), person_label);
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
    var json_stream = std.io.fixedBufferStream(&json_buf);
    try std.testing.expectError(
        import_export.ImportExportError.DatabaseError,
        import_export.exportJson(allocator, db, json_stream.writer(), null),
    );

    var jsonl_buf: [small_export_buffer_size]u8 = undefined;
    var jsonl_stream = std.io.fixedBufferStream(&jsonl_buf);
    try std.testing.expectError(
        import_export.ImportExportError.DatabaseError,
        import_export.exportJsonl(allocator, db, jsonl_stream.writer(), null),
    );

    var canonical_buf: [small_export_buffer_size]u8 = undefined;
    var canonical_stream = std.io.fixedBufferStream(&canonical_buf);
    try std.testing.expectError(
        import_export.ImportExportError.DatabaseError,
        import_export.dumpCanonicalJson(allocator, db, canonical_stream.writer(), null),
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
    var json_stream = std.io.fixedBufferStream(&json_buf);
    try std.testing.expectError(
        import_export.ImportExportError.DatabaseError,
        import_export.exportJson(allocator, db, json_stream.writer(), null),
    );

    var jsonl_buf: [small_export_buffer_size]u8 = undefined;
    var jsonl_stream = std.io.fixedBufferStream(&jsonl_buf);
    try std.testing.expectError(
        import_export.ImportExportError.DatabaseError,
        import_export.exportJsonl(allocator, db, jsonl_stream.writer(), null),
    );

    var canonical_buf: [small_export_buffer_size]u8 = undefined;
    var canonical_stream = std.io.fixedBufferStream(&canonical_buf);
    try std.testing.expectError(
        import_export.ImportExportError.DatabaseError,
        import_export.dumpCanonicalJson(allocator, db, canonical_stream.writer(), null),
    );
}
