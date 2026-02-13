//! Integration tests for hash embedding and embedding C API.
//!
//! Tests the hash embedding pipeline end-to-end:
//! - Hash embed → vector search pipeline
//! - Deterministic output
//! - Similarity properties
//! - HTTP client config construction

const std = @import("std");
const lattice = @import("lattice");

const Database = lattice.storage.database.Database;
const OpenOptions = lattice.storage.database.OpenOptions;
const distance = lattice.vector.distance;

test "hash embed: deterministic output" {
    const allocator = std.testing.allocator;

    const v1 = try lattice.hashEmbed(allocator, "machine learning algorithms", .{});
    defer allocator.free(v1);
    const v2 = try lattice.hashEmbed(allocator, "machine learning algorithms", .{});
    defer allocator.free(v2);

    for (v1, v2) |a, b| {
        try std.testing.expectEqual(a, b);
    }
}

test "hash embed: similarity ordering" {
    const allocator = std.testing.allocator;

    const v_ml = try lattice.hashEmbed(allocator, "machine learning neural networks deep learning", .{});
    defer allocator.free(v_ml);
    const v_ml2 = try lattice.hashEmbed(allocator, "deep learning neural network training", .{});
    defer allocator.free(v_ml2);
    const v_db = try lattice.hashEmbed(allocator, "database storage index query optimization", .{});
    defer allocator.free(v_db);

    // ML queries should be closer to each other than to DB
    const dist_ml = distance.cosineDistance(v_ml, v_ml2);
    const dist_cross = distance.cosineDistance(v_ml, v_db);
    try std.testing.expect(dist_ml < dist_cross);
}

test "hash embed: vector search pipeline" {
    const allocator = std.testing.allocator;
    const path = "/tmp/lattice_hash_embed_test.ltdb";

    // Clean up
    std.fs.cwd().deleteFile(path) catch {};
    std.fs.cwd().deleteFile(path ++ "-wal") catch {};

    // Create database with vector support
    var db = try Database.open(allocator, path, .{
        .create = true,
        .config = .{
            .enable_wal = false,
            .enable_fts = false,
            .enable_vector = true,
            .vector_dimensions = 128,
        },
    });
    defer {
        db.close();
        std.fs.cwd().deleteFile(path) catch {};
    }

    // Hash embed 3 documents and insert
    const docs = [_][]const u8{
        "machine learning neural networks deep learning algorithms",
        "natural language processing text classification NLP",
        "database indexing query optimization storage engine",
    };

    var node_ids: [3]u64 = undefined;
    for (docs, 0..) |doc, i| {
        const vec = try lattice.hashEmbed(allocator, doc, .{ .dimensions = 128 });
        defer allocator.free(vec);

        node_ids[i] = try db.createNode(null, &[_][]const u8{"Document"});
        try db.setNodeVector(node_ids[i], vec);
    }

    // Search with ML query
    const query_vec = try lattice.hashEmbed(allocator, "machine learning training", .{ .dimensions = 128 });
    defer allocator.free(query_vec);

    const results = try db.vectorSearch(query_vec, 3, null);
    defer allocator.free(results);

    // Should get results
    try std.testing.expect(results.len > 0);

    // First result should be the ML document (node_ids[0])
    try std.testing.expectEqual(node_ids[0], results[0].node_id);
}

test "hash embed: HTTP client config round-trip" {
    // Verify that creating an embedding client with a config works
    // (no actual HTTP calls - just config construction)
    const allocator = std.testing.allocator;

    var client = lattice.EmbeddingClient.init(allocator, .{
        .endpoint = "http://localhost:11434/api/embeddings",
        .model = "nomic-embed-text",
        .api_format = .ollama,
        .api_key = null,
        .timeout_ms = 5000,
    });
    defer client.deinit();

    try std.testing.expectEqualStrings("nomic-embed-text", client.config.model);
    try std.testing.expectEqual(lattice.EmbeddingApiFormat.ollama, client.config.api_format);
    try std.testing.expectEqual(@as(u32, 5000), client.config.timeout_ms);
}
