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
const stream_test = @import("stream_test.zig");
const cli_test = @import("cli_test.zig");
const search_correctness_test = @import("search_correctness_test.zig");
const cypher_semantics_test = @import("cypher_semantics_test.zig");
const lfmt_format_test = @import("lfmt_format_test.zig");
const lfmt_delete_test = @import("lfmt_delete_test.zig");
const lfmt_sync_test = @import("lfmt_sync_test.zig");
const lqs_query_semantics_test = @import("lqs_query_semantics_test.zig");
const llv_label_vector_test = @import("llv_label_vector_test.zig");
const lvc_vector_crud_test = @import("lvc_vector_crud_test.zig");
const lrdf_rdf_test = @import("lrdf_rdf_test.zig");
const lgr_graph_storage_test = @import("lgr_graph_storage_test.zig");
const lcf_changefeed_golden_test = @import("lcf_changefeed_golden_test.zig");
const lcf_changefeed_order_test = @import("lcf_changefeed_order_test.zig");
const lcf_changefeed_retention_test = @import("lcf_changefeed_retention_test.zig");
const lcy_cypher_features_test = @import("lcy_cypher_features_test.zig");
const lnl_nolog_test = @import("lnl_nolog_test.zig");
const lse_fts_space_test = @import("lse_fts_space_test.zig");
const lse_fts_query_test = @import("lse_fts_query_test.zig");
const lse_fts_ddl_test = @import("lse_fts_ddl_test.zig");
const lse_vector_filter_test = @import("lse_vector_filter_test.zig");
const lcy_cypher_phase2_test = @import("lcy_cypher_phase2_test.zig");

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
    // Durable stream and graph changefeed tests
    _ = stream_test;
    // The command-line tool, run as a separate process
    _ = cli_test;
    // Complete and correct full-text and vector search results
    _ = search_correctness_test;
    // Cypher expression semantics, end to end
    _ = cypher_semantics_test;
    // Node ids and the vector configuration kept in the file header
    _ = lfmt_format_test;
    // Deletes that would leave edges behind, and edges to missing nodes
    _ = lfmt_delete_test;
    // Explicit durability settings against the environment
    _ = lfmt_sync_test;
    // Query errors that name their cause, and query semantics fixes
    _ = lqs_query_semantics_test;
    // A node's vector stays when its last label goes
    _ = llv_label_vector_test;
    // Reading and removing a node's vector
    _ = lvc_vector_crud_test;
    // RDF import and export through the command-line tool
    _ = lrdf_rdf_test;
    // Graph storage: edge updates, ordered property indexes, type scans
    _ = lgr_graph_storage_test;
    _ = lcf_changefeed_golden_test;
    _ = lcf_changefeed_order_test;
    _ = lcf_changefeed_retention_test;
    // OPTIONAL MATCH, UNION, CASE, comprehensions, paths, CALL, FOREACH
    _ = lcy_cypher_features_test;
    // Transactions on a database opened without a log
    _ = lnl_nolog_test;
    // Full-text search giving back the space of what leaves it
    _ = lse_fts_space_test;
    // The full-text query language, the same on every path
    _ = lse_fts_query_test;
    // Declaring a full-text index happens whole or not at all
    _ = lse_fts_ddl_test;
    _ = lse_vector_filter_test;
    _ = lcy_cypher_phase2_test;
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
