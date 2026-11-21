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
const query_test = @import("query_test.zig");
const search_test = @import("search_test.zig");
const stream_store_test = @import("stream_store_test.zig");
const c_api_test = @import("c_api_test.zig");
const lfmt_c_api_test = @import("lfmt_c_api_test.zig");
const lnl_c_api_test = @import("lnl_c_api_test.zig");
const lnl_buffer_pool_test = @import("lnl_buffer_pool_test.zig");
const lfmt_checkpoint_test = @import("lfmt_checkpoint_test.zig");
const lfmt_reader_gate_test = @import("lfmt_reader_gate_test.zig");
const concurrency_test = @import("concurrency_test.zig");
const transaction_test = @import("transaction_test.zig");
const quipu_repro_test = @import("quipu_longmemeval_repro.zig");
const vector_test = @import("vector_test.zig");
const lcf_btree_split_test = @import("lcf_btree_split_test.zig");
const lcf_c_api_changefeed_test = @import("lcf_c_api_changefeed_test.zig");

// Re-export tests from all modules
test {
    // Core types
    _ = lattice.core.types;
    _ = lattice.core.id_key;
    _ = lattice.storage.id_key_upgrade;

    // Storage - inline tests
    _ = lattice.storage.page;
    _ = lattice.storage.btree;

    // Vector search
    _ = lattice.vector.hnsw;

    // Full-text search
    _ = lattice.fts.tokenizer;

    // Query layer - inline tests
    _ = lattice.query.lexer;
    _ = lattice.query.parser;
    _ = lattice.query.executor;

    // Transaction
    _ = lattice.transaction.manager;

    // Streams
    _ = lattice.stream.payload;
    _ = lattice.stream.store;

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
    _ = query_test;
    _ = search_test;
    _ = stream_store_test;
    _ = c_api_test;
    _ = lfmt_c_api_test;
    _ = lnl_c_api_test;
    _ = lnl_buffer_pool_test;
    _ = lfmt_checkpoint_test;
    _ = lfmt_reader_gate_test;
    _ = concurrency_test;
    _ = transaction_test;
    _ = quipu_repro_test;
    _ = vector_test;
    _ = lcf_btree_split_test;
    _ = lcf_c_api_changefeed_test;
}

test "lattice version" {
    const expected = try std.fmt.allocPrint(
        std.testing.allocator,
        "{d}.{d}.{d}",
        .{ lattice.VERSION_MAJOR, lattice.VERSION_MINOR, lattice.VERSION_PATCH },
    );
    defer std.testing.allocator.free(expected);

    try std.testing.expectEqualStrings(expected, lattice.VERSION);
}

test "magic number" {
    try std.testing.expectEqual(@as(u32, 0x4C544442), lattice.core.types.MAGIC_NUMBER);
}
