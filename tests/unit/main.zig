//! Lattice unit test runner.
//!
//! Imports and runs all unit tests from the source modules.

const std = @import("std");

// Import the lattice module to run its tests
const lattice = @import("lattice");

// Re-export tests from all modules
test {
    // Core types
    _ = lattice.core.types;

    // Storage
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
}

test "lattice version" {
    try std.testing.expectEqualStrings("0.1.0", lattice.VERSION);
}

test "magic number" {
    try std.testing.expectEqual(@as(u32, 0x4C544442), lattice.core.types.MAGIC_NUMBER);
}
