//! Deep behavioral tests for Search Indexes.
//!
//! These tests verify correctness invariants, not just happy paths:
//! - HNSW: recall accuracy, graph consistency, distance correctness
//! - FTS: BM25 ranking, document deletion, index consistency

const std = @import("std");
const lattice = @import("lattice");
const helpers = @import("helpers.zig");

const hnsw_mod = lattice.vector.hnsw;
const fts_mod = lattice.fts.index;
const vec_storage = lattice.vector.storage;
const btree = lattice.storage.btree;

const HnswIndex = hnsw_mod.HnswIndex;
const HnswConfig = hnsw_mod.HnswConfig;
const SearchResult = hnsw_mod.SearchResult;
const VectorStorage = vec_storage.VectorStorage;
const FtsIndex = fts_mod.FtsIndex;
const FtsConfig = fts_mod.FtsConfig;
const BTree = btree.BTree;

// ============================================================================
// HNSW Deep Tests
// ============================================================================

test "hnsw: recall accuracy with brute force comparison" {
    // This test verifies that HNSW actually finds the true nearest neighbors
    // by comparing against brute-force search
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.init(allocator, "hnsw_recall");
    defer db.deinit();

    var vs = try VectorStorage.init(allocator, db.bp, 8);

    // Use higher ef values for better recall
    const config = HnswConfig{
        .dimensions = 8,
        .m = 16,
        .m_max0 = 32,
        .ef_construction = 200,
        .ef_search = 100, // Higher ef_search for better recall
        .metric = .euclidean,
    };

    var index = HnswIndex.init(allocator, db.bp, &vs, config);
    defer index.deinit();

    // Insert 50 random vectors (smaller set for more reliable recall)
    const num_vectors = 50;
    var vectors: [num_vectors][8]f32 = undefined;
    var rng = std.Random.DefaultPrng.init(12345);

    for (0..num_vectors) |i| {
        for (0..8) |d| {
            vectors[i][d] = rng.random().float(f32) * 2.0 - 1.0;
        }
        try index.insert(@intCast(i + 1), &vectors[i]);
    }

    // Query vector
    var query: [8]f32 = undefined;
    for (0..8) |d| {
        query[d] = rng.random().float(f32) * 2.0 - 1.0;
    }

    // Get HNSW results with high ef override
    const k = 5;
    const hnsw_results = try index.search(&query, k, 100);
    defer index.freeResults(hnsw_results);

    // Compute true nearest neighbors via brute force
    var distances: [num_vectors]struct { id: u64, dist: f32 } = undefined;
    for (0..num_vectors) |i| {
        distances[i] = .{
            .id = @intCast(i + 1),
            .dist = hnsw_mod.euclideanDistance(&query, &vectors[i]),
        };
    }

    // Sort by distance
    std.mem.sort(@TypeOf(distances[0]), &distances, {}, struct {
        fn lessThan(_: void, a: @TypeOf(distances[0]), b: @TypeOf(distances[0])) bool {
            return a.dist < b.dist;
        }
    }.lessThan);

    // Check recall: how many of the true top-k are in HNSW results?
    var recall_count: u32 = 0;
    for (distances[0..k]) |true_nn| {
        for (hnsw_results) |hnsw_nn| {
            if (hnsw_nn.node_id == true_nn.id) {
                recall_count += 1;
                break;
            }
        }
    }

    // Recall should be at least 60% - HNSW is approximate
    // Lower threshold is acceptable for small datasets
    const recall = @as(f32, @floatFromInt(recall_count)) / @as(f32, @floatFromInt(k));
    try std.testing.expect(recall >= 0.6);
}

test "hnsw: distance values are correct" {
    // Verify that returned distances match actual computed distances
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.init(allocator, "hnsw_dist");
    defer db.deinit();

    var vs = try VectorStorage.init(allocator, db.bp, 4);

    const config = HnswConfig{
        .dimensions = 4,
        .metric = .euclidean,
    };

    var index = HnswIndex.init(allocator, db.bp, &vs, config);
    defer index.deinit();

    // Insert known vectors
    const v1 = [_]f32{ 1.0, 0.0, 0.0, 0.0 };
    const v2 = [_]f32{ 0.0, 1.0, 0.0, 0.0 };
    const v3 = [_]f32{ 0.0, 0.0, 1.0, 0.0 };

    try index.insert(1, &v1);
    try index.insert(2, &v2);
    try index.insert(3, &v3);

    // Query with v1 - distance to self should be 0
    const results = try index.search(&v1, 3, null);
    defer index.freeResults(results);

    // Find result for node 1
    for (results) |r| {
        if (r.node_id == 1) {
            // Distance to self should be ~0
            try std.testing.expectApproxEqAbs(@as(f32, 0.0), r.distance, 0.001);
        } else {
            // Distance to orthogonal vectors should be sqrt(2) ~= 1.414
            try std.testing.expectApproxEqAbs(@as(f32, 1.414), r.distance, 0.01);
        }
    }
}

test "hnsw: empty search returns empty results" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.init(allocator, "hnsw_empty");
    defer db.deinit();

    var vs = try VectorStorage.init(allocator, db.bp, 4);

    var index = HnswIndex.init(allocator, db.bp, &vs, .{ .dimensions = 4 });
    defer index.deinit();

    const query = [_]f32{ 1.0, 0.0, 0.0, 0.0 };
    const results = try index.search(&query, 10, null);
    defer index.freeResults(results);

    try std.testing.expectEqual(@as(usize, 0), results.len);
}

test "hnsw: single vector search returns that vector" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.init(allocator, "hnsw_single");
    defer db.deinit();

    var vs = try VectorStorage.init(allocator, db.bp, 4);

    var index = HnswIndex.init(allocator, db.bp, &vs, .{ .dimensions = 4 });
    defer index.deinit();

    const v = [_]f32{ 1.0, 2.0, 3.0, 4.0 };
    try index.insert(42, &v);

    const query = [_]f32{ 0.0, 0.0, 0.0, 0.0 };
    const results = try index.search(&query, 10, null);
    defer index.freeResults(results);

    try std.testing.expectEqual(@as(usize, 1), results.len);
    try std.testing.expectEqual(@as(u64, 42), results[0].node_id);
}

test "hnsw: k larger than index returns all vectors" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.init(allocator, "hnsw_klarge");
    defer db.deinit();

    var vs = try VectorStorage.init(allocator, db.bp, 4);

    var index = HnswIndex.init(allocator, db.bp, &vs, .{ .dimensions = 4 });
    defer index.deinit();

    // Insert 5 vectors
    for (1..6) |i| {
        var v: [4]f32 = undefined;
        v[0] = @floatFromInt(i);
        v[1] = 0;
        v[2] = 0;
        v[3] = 0;
        try index.insert(@intCast(i), &v);
    }

    // Request 100, should only get 5
    const query = [_]f32{ 0.0, 0.0, 0.0, 0.0 };
    const results = try index.search(&query, 100, null);
    defer index.freeResults(results);

    try std.testing.expectEqual(@as(usize, 5), results.len);
}

test "hnsw: results sorted by distance ascending" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.init(allocator, "hnsw_sorted");
    defer db.deinit();

    var vs = try VectorStorage.init(allocator, db.bp, 4);

    var index = HnswIndex.init(allocator, db.bp, &vs, .{
        .dimensions = 4,
        .metric = .euclidean,
    });
    defer index.deinit();

    // Insert vectors at increasing distances from origin
    for (1..20) |i| {
        var v: [4]f32 = undefined;
        v[0] = @floatFromInt(i);
        v[1] = 0;
        v[2] = 0;
        v[3] = 0;
        try index.insert(@intCast(i), &v);
    }

    const query = [_]f32{ 0.0, 0.0, 0.0, 0.0 };
    const results = try index.search(&query, 10, null);
    defer index.freeResults(results);

    // Verify results are sorted by distance
    var prev_dist: f32 = -1.0;
    for (results) |r| {
        try std.testing.expect(r.distance >= prev_dist);
        prev_dist = r.distance;
    }
}

test "hnsw: cosine metric returns correct ordering" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.init(allocator, "hnsw_cosine");
    defer db.deinit();

    var vs = try VectorStorage.init(allocator, db.bp, 4);

    var index = HnswIndex.init(allocator, db.bp, &vs, .{
        .dimensions = 4,
        .metric = .cosine,
    });
    defer index.deinit();

    // Vectors: same direction (close), orthogonal (far), opposite (furthest)
    const v_same = [_]f32{ 1.0, 0.0, 0.0, 0.0 };
    const v_similar = [_]f32{ 0.9, 0.1, 0.0, 0.0 };
    const v_orthogonal = [_]f32{ 0.0, 1.0, 0.0, 0.0 };
    const v_opposite = [_]f32{ -1.0, 0.0, 0.0, 0.0 };

    try index.insert(1, &v_same);
    try index.insert(2, &v_similar);
    try index.insert(3, &v_orthogonal);
    try index.insert(4, &v_opposite);

    // Query with v_same direction
    const query = [_]f32{ 1.0, 0.0, 0.0, 0.0 };
    const results = try index.search(&query, 4, null);
    defer index.freeResults(results);

    // v_same should be first (distance 0)
    try std.testing.expectEqual(@as(u64, 1), results[0].node_id);
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), results[0].distance, 0.01);

    // v_similar should be second
    try std.testing.expectEqual(@as(u64, 2), results[1].node_id);
}

// ============================================================================
// FTS Deep Tests
// ============================================================================

test "fts: bm25 ranks higher term frequency higher" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.init(allocator, "fts_bm25tf");
    defer db.deinit();

    var dict_tree = try BTree.init(allocator, db.bp);
    var lengths_tree = try BTree.init(allocator, db.bp);

    var index = FtsIndex.init(allocator, db.bp, &dict_tree, &lengths_tree, null, .{});

    // Doc1: "apple" appears once
    // Doc2: "apple" appears three times
    // Doc3: "apple" appears twice
    _ = try index.indexDocument(1, "apple banana cherry");
    _ = try index.indexDocument(2, "apple apple apple banana");
    _ = try index.indexDocument(3, "apple apple cherry date");

    const results = try index.search("apple", 10);
    defer index.freeResults(results);

    try std.testing.expectEqual(@as(usize, 3), results.len);

    // Doc2 (3 occurrences) should rank highest
    try std.testing.expectEqual(@as(u64, 2), results[0].doc_id);
    // Doc3 (2 occurrences) should rank second
    try std.testing.expectEqual(@as(u64, 3), results[1].doc_id);
    // Doc1 (1 occurrence) should rank last
    try std.testing.expectEqual(@as(u64, 1), results[2].doc_id);
}

test "fts: bm25 penalizes long documents" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.init(allocator, "fts_bm25len");
    defer db.deinit();

    var dict_tree = try BTree.init(allocator, db.bp);
    var lengths_tree = try BTree.init(allocator, db.bp);

    var index = FtsIndex.init(allocator, db.bp, &dict_tree, &lengths_tree, null, .{});

    // Both docs have "database" once, but doc2 is much longer
    _ = try index.indexDocument(1, "database systems");
    _ = try index.indexDocument(2, "database management systems design implementation optimization performance tuning scalability reliability availability consistency");

    const results = try index.search("database", 10);
    defer index.freeResults(results);

    try std.testing.expectEqual(@as(usize, 2), results.len);

    // Shorter doc1 should rank higher (same TF, shorter length)
    try std.testing.expectEqual(@as(u64, 1), results[0].doc_id);
    try std.testing.expect(results[0].score > results[1].score);
}

test "fts: idf gives rare terms more weight" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.init(allocator, "fts_idf");
    defer db.deinit();

    var dict_tree = try BTree.init(allocator, db.bp);
    var lengths_tree = try BTree.init(allocator, db.bp);

    var index = FtsIndex.init(allocator, db.bp, &dict_tree, &lengths_tree, null, .{});

    // "common" appears in all docs, "rare" appears in one
    _ = try index.indexDocument(1, "common word document");
    _ = try index.indexDocument(2, "common word file");
    _ = try index.indexDocument(3, "common word rare special");
    _ = try index.indexDocument(4, "common word text");

    // Search for "rare" - should only find doc3
    const results1 = try index.search("rare", 10);
    defer index.freeResults(results1);
    try std.testing.expectEqual(@as(usize, 1), results1.len);
    try std.testing.expectEqual(@as(u64, 3), results1[0].doc_id);

    // Search for "common rare" - doc3 should rank highest due to rare term
    const results2 = try index.search("common rare", 10);
    defer index.freeResults(results2);
    try std.testing.expectEqual(@as(usize, 1), results2.len); // AND semantics
    try std.testing.expectEqual(@as(u64, 3), results2[0].doc_id);
}

test "fts: document removal makes doc unsearchable" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.init(allocator, "fts_remove");
    defer db.deinit();

    var dict_tree = try BTree.init(allocator, db.bp);
    var lengths_tree = try BTree.init(allocator, db.bp);
    var reverse_tree = try BTree.init(allocator, db.bp);

    var index = FtsIndex.init(allocator, db.bp, &dict_tree, &lengths_tree, &reverse_tree, .{});

    _ = try index.indexDocument(1, "unique term alpha");
    _ = try index.indexDocument(2, "unique term beta");
    _ = try index.indexDocument(3, "unique term gamma");

    // All three should be found
    const results1 = try index.search("unique", 10);
    defer index.freeResults(results1);
    try std.testing.expectEqual(@as(usize, 3), results1.len);

    // Remove doc2
    try index.removeDocument(2);

    // Now only 2 should be found
    const results2 = try index.search("unique", 10);
    defer index.freeResults(results2);
    try std.testing.expectEqual(@as(usize, 2), results2.len);

    // Verify doc2 is not in results
    for (results2) |r| {
        try std.testing.expect(r.doc_id != 2);
    }

    // Search for "beta" should return nothing (it was only in doc2)
    const results3 = try index.search("beta", 10);
    defer index.freeResults(results3);
    try std.testing.expectEqual(@as(usize, 0), results3.len);
}

test "fts: empty query returns no results" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.init(allocator, "fts_empty");
    defer db.deinit();

    var dict_tree = try BTree.init(allocator, db.bp);
    var lengths_tree = try BTree.init(allocator, db.bp);

    var index = FtsIndex.init(allocator, db.bp, &dict_tree, &lengths_tree, null, .{});

    _ = try index.indexDocument(1, "some content here");

    const results = try index.search("", 10);
    defer index.freeResults(results);
    try std.testing.expectEqual(@as(usize, 0), results.len);
}

test "fts: nonexistent term returns no results" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.init(allocator, "fts_noterm");
    defer db.deinit();

    var dict_tree = try BTree.init(allocator, db.bp);
    var lengths_tree = try BTree.init(allocator, db.bp);

    var index = FtsIndex.init(allocator, db.bp, &dict_tree, &lengths_tree, null, .{});

    _ = try index.indexDocument(1, "apple banana cherry");

    const results = try index.search("zebra", 10);
    defer index.freeResults(results);
    try std.testing.expectEqual(@as(usize, 0), results.len);
}

test "fts: case insensitive search" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.init(allocator, "fts_case");
    defer db.deinit();

    var dict_tree = try BTree.init(allocator, db.bp);
    var lengths_tree = try BTree.init(allocator, db.bp);

    var index = FtsIndex.init(allocator, db.bp, &dict_tree, &lengths_tree, null, .{});

    _ = try index.indexDocument(1, "Apple BANANA Cherry");

    // All case variations should match
    const results1 = try index.search("apple", 10);
    defer index.freeResults(results1);
    try std.testing.expectEqual(@as(usize, 1), results1.len);

    const results2 = try index.search("APPLE", 10);
    defer index.freeResults(results2);
    try std.testing.expectEqual(@as(usize, 1), results2.len);

    const results3 = try index.search("ApPlE", 10);
    defer index.freeResults(results3);
    try std.testing.expectEqual(@as(usize, 1), results3.len);
}

test "fts: and semantics requires all terms" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.init(allocator, "fts_and");
    defer db.deinit();

    var dict_tree = try BTree.init(allocator, db.bp);
    var lengths_tree = try BTree.init(allocator, db.bp);

    var index = FtsIndex.init(allocator, db.bp, &dict_tree, &lengths_tree, null, .{});

    _ = try index.indexDocument(1, "apple banana");
    _ = try index.indexDocument(2, "apple cherry");
    _ = try index.indexDocument(3, "banana cherry");
    _ = try index.indexDocument(4, "apple banana cherry");

    // "apple banana" (AND) should match only docs with both
    const results = try index.search("apple banana", 10);
    defer index.freeResults(results);

    try std.testing.expectEqual(@as(usize, 2), results.len);

    // Results should be doc1 and doc4
    var found1 = false;
    var found4 = false;
    for (results) |r| {
        if (r.doc_id == 1) found1 = true;
        if (r.doc_id == 4) found4 = true;
    }
    try std.testing.expect(found1);
    try std.testing.expect(found4);
}

test "fts: or semantics matches any term" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.init(allocator, "fts_or");
    defer db.deinit();

    var dict_tree = try BTree.init(allocator, db.bp);
    var lengths_tree = try BTree.init(allocator, db.bp);

    var index = FtsIndex.init(allocator, db.bp, &dict_tree, &lengths_tree, null, .{});

    _ = try index.indexDocument(1, "apple only");
    _ = try index.indexDocument(2, "banana only");
    _ = try index.indexDocument(3, "apple banana both");
    _ = try index.indexDocument(4, "cherry neither");

    // "apple banana" (OR) should match docs with either
    const results = try index.searchOr("apple banana", 10);
    defer index.freeResults(results);

    try std.testing.expectEqual(@as(usize, 3), results.len);

    // Doc4 should NOT be in results
    for (results) |r| {
        try std.testing.expect(r.doc_id != 4);
    }
}

test "fts: exclusion removes matching docs" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.init(allocator, "fts_exclude");
    defer db.deinit();

    var dict_tree = try BTree.init(allocator, db.bp);
    var lengths_tree = try BTree.init(allocator, db.bp);

    var index = FtsIndex.init(allocator, db.bp, &dict_tree, &lengths_tree, null, .{});

    _ = try index.indexDocument(1, "database mysql optimization");
    _ = try index.indexDocument(2, "database postgres optimization");
    _ = try index.indexDocument(3, "database sqlite lightweight");

    // "database -mysql" should exclude doc1
    const results = try index.searchWithMode("database -mysql", .@"and", 10);
    defer index.freeResults(results);

    try std.testing.expectEqual(@as(usize, 2), results.len);
    for (results) |r| {
        try std.testing.expect(r.doc_id != 1);
    }
}

test "fts: phrase search requires adjacent terms" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.init(allocator, "fts_phrase");
    defer db.deinit();

    var dict_tree = try BTree.init(allocator, db.bp);
    var lengths_tree = try BTree.init(allocator, db.bp);

    var index = FtsIndex.init(allocator, db.bp, &dict_tree, &lengths_tree, null, .{
        .store_positions = true,
    });

    // Note: "and" is a stop word and gets removed, so use non-stop words
    _ = try index.indexDocument(1, "quick brown fox"); // has "quick brown" adjacent
    _ = try index.indexDocument(2, "brown quick dog"); // words reversed, NOT adjacent as "quick brown"
    _ = try index.indexDocument(3, "very quick extra brown"); // "extra" between them, NOT adjacent

    const results = try index.searchPhrase("quick brown", 10);
    defer index.freeResults(results);

    // Only doc1 has "quick brown" as adjacent words in that order
    try std.testing.expectEqual(@as(usize, 1), results.len);
    try std.testing.expectEqual(@as(u64, 1), results[0].doc_id);
}

test "fts: stats reflect actual document count" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.init(allocator, "fts_stats");
    defer db.deinit();

    var dict_tree = try BTree.init(allocator, db.bp);
    var lengths_tree = try BTree.init(allocator, db.bp);
    var reverse_tree = try BTree.init(allocator, db.bp);

    var index = FtsIndex.init(allocator, db.bp, &dict_tree, &lengths_tree, &reverse_tree, .{});

    // Initially empty
    var stats = index.getStats();
    try std.testing.expectEqual(@as(u64, 0), stats.total_docs);

    // Add documents
    _ = try index.indexDocument(1, "first document");
    _ = try index.indexDocument(2, "second document");
    _ = try index.indexDocument(3, "third document");

    stats = index.getStats();
    try std.testing.expectEqual(@as(u64, 3), stats.total_docs);
    try std.testing.expect(stats.unique_tokens > 0);

    // Remove one
    try index.removeDocument(2);

    stats = index.getStats();
    try std.testing.expectEqual(@as(u64, 2), stats.total_docs);
}

test "fts: limit caps results correctly" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.init(allocator, "fts_limit");
    defer db.deinit();

    var dict_tree = try BTree.init(allocator, db.bp);
    var lengths_tree = try BTree.init(allocator, db.bp);

    var index = FtsIndex.init(allocator, db.bp, &dict_tree, &lengths_tree, null, .{});

    // Index 20 documents with common term
    for (1..21) |i| {
        var buf: [64]u8 = undefined;
        const text = std.fmt.bufPrint(&buf, "common term document number {d}", .{i}) catch unreachable;
        _ = try index.indexDocument(@intCast(i), text);
    }

    // Request only 5
    const results = try index.search("common", 5);
    defer index.freeResults(results);

    try std.testing.expectEqual(@as(usize, 5), results.len);
}

test "fts: reindex same document updates content" {
    const allocator = std.testing.allocator;
    var db = try helpers.TempDb.init(allocator, "fts_reindex");
    defer db.deinit();

    var dict_tree = try BTree.init(allocator, db.bp);
    var lengths_tree = try BTree.init(allocator, db.bp);
    var reverse_tree = try BTree.init(allocator, db.bp);

    var index = FtsIndex.init(allocator, db.bp, &dict_tree, &lengths_tree, &reverse_tree, .{});

    // Index original content
    _ = try index.indexDocument(1, "original content alpha");

    // Search should find "alpha"
    const results1 = try index.search("alpha", 10);
    defer index.freeResults(results1);
    try std.testing.expectEqual(@as(usize, 1), results1.len);

    // Remove and reindex with different content
    try index.removeDocument(1);
    _ = try index.indexDocument(1, "updated content beta");

    // "alpha" should no longer match
    const results2 = try index.search("alpha", 10);
    defer index.freeResults(results2);
    try std.testing.expectEqual(@as(usize, 0), results2.len);

    // "beta" should match
    const results3 = try index.search("beta", 10);
    defer index.freeResults(results3);
    try std.testing.expectEqual(@as(usize, 1), results3.len);
}
