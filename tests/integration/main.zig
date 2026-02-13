//! LatticeDB Integration Test Runner
//!
//! Runs end-to-end tests that verify the Database API works correctly
//! across persistence, transactions, search, and queries.

const std = @import("std");
const lattice = @import("lattice");

// Import integration test modules
const database_test = @import("database_test.zig");
const mvcc_test = @import("mvcc_test.zig");
const embedding_test = @import("embedding_test.zig");

// Re-export tests from all modules
test {
    // Database integration tests
    _ = database_test;
    // MVCC integration tests
    _ = mvcc_test;
    // Embedding integration tests
    _ = embedding_test;
}

test "integration test runner initialized" {
    // Verify we can access the lattice module
    try std.testing.expectEqualStrings("0.1.0", lattice.VERSION);
}
