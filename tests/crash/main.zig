//! LatticeDB Crash Recovery Test Runner
//!
//! Tests that verify committed transactions survive simulated crashes
//! and that recovery correctly replays WAL records.

const crash_test = @import("crash_test.zig");
const ldur_redo_test = @import("ldur_redo_test.zig");

test {
    _ = crash_test;
    _ = ldur_redo_test;
}
