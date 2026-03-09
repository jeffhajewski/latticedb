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
const query_mutation_test = @import("query_mutation_test.zig");
const import_export_test = @import("import_export_test.zig");

// Re-export tests from all modules
test {
    // Database integration tests
    _ = database_test;
    // MVCC integration tests
    _ = mvcc_test;
    // Embedding integration tests
    _ = embedding_test;
    // Query mutation end-to-end tests
    _ = query_mutation_test;
    // Import/export end-to-end tests
    _ = import_export_test;
}

test "integration test runner initialized" {
    // Verify the string and split components remain internally consistent.
    const expected = try std.fmt.allocPrint(
        std.testing.allocator,
        "{d}.{d}.{d}",
        .{ lattice.VERSION_MAJOR, lattice.VERSION_MINOR, lattice.VERSION_PATCH },
    );
    defer std.testing.allocator.free(expected);

    try std.testing.expectEqualStrings(expected, lattice.VERSION);
}
