//! Lattice unit test runner.
//!
//! Imports and runs all unit tests from the source modules and behavioral tests.

const std = @import("std");

// Import the lattice module to run its tests
const lattice = @import("lattice");

// Import behavioral test modules
const btree_test = @import("btree_test.zig");
const wal_test = @import("wal_test.zig");
const buffer_pool_test = @import("buffer_pool_test.zig");
const page_manager_test = @import("page_manager_test.zig");
const graph_test = @import("graph_test.zig");

// Re-export tests from all modules
test {
    // Core types
    _ = lattice.core.types;

    // Storage - inline tests
    _ = lattice.storage.page;
    _ = lattice.storage.btree;

    // Vector search
    _ = lattice.vector.hnsw;

    // Full-text search
    _ = lattice.fts.tokenizer;

    // Query
    _ = lattice.query.parser;

    // Transaction
    _ = lattice.transaction.manager;

    // Concurrency
    _ = lattice.concurrency.locking;

    // C API
    _ = lattice.c_api;

    // Graph layer - inline tests
    _ = lattice.graph.symbols;
    _ = lattice.graph.node;
    _ = lattice.graph.edge;
    _ = lattice.graph.label_index;

    // Behavioral tests
    _ = btree_test;
    _ = wal_test;
    _ = buffer_pool_test;
    _ = page_manager_test;
    _ = graph_test;
}

test "lattice version" {
    try std.testing.expectEqualStrings("0.1.0", lattice.VERSION);
}

test "magic number" {
    try std.testing.expectEqual(@as(u32, 0x4C544442), lattice.core.types.MAGIC_NUMBER);
}
