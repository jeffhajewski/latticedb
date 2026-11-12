//! Power-loss tests: the workload in ldur_power.zig, cut at every sync and
//! at random points, under several configurations.
//!
//! The configurations here are sized for the crash suite. For a longer
//! search, set LDUR_SEEDS to a number of seeds: each configuration then
//! runs that many times at full size, from seeds counted up from
//! LDUR_SEED_BASE (default 100). LDUR_SEED reruns one configuration at
//! another seed, LDUR_ONLY_CUT one cut of it; LDUR_VERBOSE prints each cut
//! and LDUR_KEEP_GOING counts failures instead of stopping at the first.

const std = @import("std");
const power = @import("ldur_power.zig");

fn envInt(name: [*:0]const u8) !?u64 {
    const v = std.c.getenv(name) orelse return null;
    return try std.fmt.parseInt(u64, std.mem.span(v), 10);
}

fn sweep(options: power.Options) !void {
    var opts = options;
    if (std.c.getenv("LDUR_VERBOSE") != null) opts.verbose = true;
    if (std.c.getenv("LDUR_KEEP_GOING") != null) opts.keep_going = true;
    if (try envInt("LDUR_SEED")) |seed| opts.seed = seed;
    if (try envInt("LDUR_ONLY_CUT")) |cut| opts.only_cut = @intCast(cut);

    if (try envInt("LDUR_SEEDS")) |count| {
        const base = (try envInt("LDUR_SEED_BASE")) orelse 100;
        opts.txns *= 3;
        opts.random_cuts *= 3;
        var failed: usize = 0;
        for (0..count) |i| {
            opts.seed = base + i;
            _ = power.run(std.testing.allocator, opts) catch |err| {
                if (!opts.keep_going) return err;
                failed += 1;
            };
        }
        if (failed > 0) {
            std.debug.print("{d} of {d} seeds failed\n", .{ failed, count });
            return error.Invariant;
        }
        return;
    }

    const stats = try power.run(std.testing.allocator, opts);
    try std.testing.expect(stats.cuts > 0);
}

test "power loss: default sync mode, graph and property index" {
    try sweep(.{ .seed = 1, .txns = 30, .random_cuts = 20 });
}

test "power loss: values in overflow pages and transactions over several frames" {
    try sweep(.{ .seed = 2, .txns = 25, .random_cuts = 20, .big_values = true, .pool_pages = 32, .max_wal_frames = 40 });
}

test "power loss: normal sync mode" {
    try sweep(.{ .seed = 3, .txns = 30, .random_cuts = 20, .sync_mode = .normal, .big_values = true });
}

test "power loss: vectors and full-text" {
    try sweep(.{ .seed = 4, .txns = 25, .random_cuts = 20, .vectors = true, .fts = true, .pool_pages = 40 });
}

test "power loss: a log removed while the database was closed" {
    try sweep(.{ .seed = 5, .txns = 25, .random_cuts = 20, .reopens = 2, .drop_log_on_reopen = true });
}

test "power loss: no log, so whole transactions up to the last flush" {
    try sweep(.{ .seed = 6, .txns = 30, .random_cuts = 20, .no_log = true, .big_values = true });
}

test "power loss: no log, with vectors and full-text" {
    try sweep(.{ .seed = 7, .txns = 25, .random_cuts = 20, .no_log = true, .vectors = true, .fts = true, .pool_pages = 40 });
}
