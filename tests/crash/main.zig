//! LatticeDB Crash Recovery Test Runner
//!
//! Tests that verify committed transactions survive simulated crashes
//! and that recovery correctly replays WAL records.

const crash_test = @import("crash_test.zig");
const ldur_redo_test = @import("ldur_redo_test.zig");
const ldur_files_test = @import("ldur_files_test.zig");
const ldur_power_test = @import("ldur_power_test.zig");
const ldur_readonly_test = @import("ldur_readonly_test.zig");
const lcf_wal_creation_test = @import("lcf_wal_creation_test.zig");
const lcf_changefeed_symbols_test = @import("lcf_changefeed_symbols_test.zig");
const lfmt_sealed_test = @import("lfmt_sealed_test.zig");

test {
    _ = crash_test;
    _ = ldur_redo_test;
    _ = ldur_files_test;
    _ = ldur_power_test;
    _ = ldur_readonly_test;
    _ = lcf_wal_creation_test;
    _ = lcf_changefeed_symbols_test;
    _ = lfmt_sealed_test;
}
