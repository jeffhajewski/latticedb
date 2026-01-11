//! Fuzz testing entry point for LatticeDB.
//!
//! This module provides fuzz targets for security-critical components:
//! - Cypher query parser (malformed input handling)
//! - WAL payload deserialization (corrupt data handling)
//! - Property value serialization (roundtrip consistency)
//! - B+Tree operations (arbitrary key/value handling)
//! - HNSW vector index (vector insert/search)
//! - Symbol table (string interning)
//!
//! Run with: zig build fuzz
//! Or for specific target: zig build fuzz -- --fuzz-target=parser

const std = @import("std");

// Import all fuzz targets
pub const parser_fuzz = @import("parser_fuzz.zig");
pub const wal_fuzz = @import("wal_fuzz.zig");
pub const btree_fuzz = @import("btree_fuzz.zig");
pub const hnsw_fuzz = @import("hnsw_fuzz.zig");
pub const symbol_fuzz = @import("symbol_fuzz.zig");

test {
    // Reference all fuzz tests so they're included
    std.testing.refAllDecls(@This());
}
