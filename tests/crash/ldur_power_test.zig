//! Power-loss tests: the workload in ldur_power.zig, cut at every sync and
//! at random points, under several configurations.

const std = @import("std");
const power = @import("ldur_power.zig");

fn sweep(options: power.Options) !void {
    var opts = options;
    if (std.c.getenv("LDUR_VERBOSE") != null) opts.verbose = true;
    if (std.c.getenv("LDUR_KEEP_GOING") != null) opts.keep_going = true;
    if (std.c.getenv("LDUR_SEED")) |v| opts.seed = try std.fmt.parseInt(u64, std.mem.span(v), 10);
    if (std.c.getenv("LDUR_ONLY_CUT")) |v| opts.only_cut = try std.fmt.parseInt(usize, std.mem.span(v), 10);
    const stats = try power.run(std.testing.allocator, opts);
    try std.testing.expect(stats.cuts > 0);
}

test "power loss: default sync mode, graph and property index" {
    try sweep(.{ .seed = 1, .txns = 80 });
}

test "power loss: values in overflow pages and transactions over several frames" {
    try sweep(.{ .seed = 2, .txns = 60, .big_values = true, .pool_pages = 32, .max_wal_frames = 40 });
}

test "power loss: normal sync mode" {
    try sweep(.{ .seed = 3, .txns = 80, .sync_mode = .normal, .big_values = true });
}

test "power loss: vectors and full-text" {
    try sweep(.{ .seed = 4, .txns = 60, .vectors = true, .fts = true, .pool_pages = 40 });
}

test "power loss: a log removed while the database was closed" {
    try sweep(.{ .seed = 5, .txns = 60, .reopens = 2, .drop_log_on_reopen = true });
}
