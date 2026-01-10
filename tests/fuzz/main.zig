//! Fuzz testing entry point for LatticeDB.
//!
//! This module provides fuzz targets for security-critical components:
//! - Cypher query parser (malformed input handling)
//! - WAL payload deserialization (corrupt data handling)
//! - Property value serialization (roundtrip consistency)
//!
//! Run with: zig build fuzz
//! Or for specific target: zig build fuzz -- --fuzz-target=parser

const std = @import("std");

// Import all fuzz targets
pub const parser_fuzz = @import("parser_fuzz.zig");
pub const wal_fuzz = @import("wal_fuzz.zig");

test {
    // Reference all fuzz tests so they're included
    std.testing.refAllDecls(@This());
}
