//! Repeated-Term FTS Indexing Benchmark
//!
//! Benchmarks the indexing path for documents that all share common terms.
//! This is the workload that previously exposed quadratic behavior in
//! posting-list append.
//!
//! Run with:
//!   zig build fts-benchmark
//!   zig build fts-benchmark -- --scale 10000

const std = @import("std");
const lattice = @import("lattice");

const Database = lattice.storage.database.Database;

const DEFAULT_SCALES = [_]usize{ 10_000, 50_000 };

const BenchmarkResult = struct {
    docs: usize,
    total_ns: u64,
    mean_ns: u64,
    p50_ns: u64,
    p95_ns: u64,
    p99_ns: u64,
    max_ns: u64,
    docs_per_sec: f64,
    search_ns: u64,

    fn print(self: BenchmarkResult) void {
        std.debug.print(
            "  {d:>8} docs  total={d:>8.2} ms  mean={d:>7.2} μs  p50={d:>7.2} μs  p95={d:>7.2} μs  p99={d:>7.2} μs  max={d:>7.2} μs  throughput={d:>10.0} docs/s  search={d:>6.2} μs\n",
            .{
                self.docs,
                nsToMs(self.total_ns),
                nsToUs(self.mean_ns),
                nsToUs(self.p50_ns),
                nsToUs(self.p95_ns),
                nsToUs(self.p99_ns),
                nsToUs(self.max_ns),
                self.docs_per_sec,
                nsToUs(self.search_ns),
            },
        );
    }
};

fn nsToUs(ns: u64) f64 {
    return @as(f64, @floatFromInt(ns)) / 1_000.0;
}

fn nsToMs(ns: u64) f64 {
    return @as(f64, @floatFromInt(ns)) / 1_000_000.0;
}

fn percentile(timings: []u64, p: f64) u64 {
    std.mem.sort(u64, timings, {}, std.sort.asc(u64));
    const idx_f = p / 100.0 * @as(f64, @floatFromInt(timings.len - 1));
    const idx: usize = @intFromFloat(idx_f);
    return timings[@min(idx, timings.len - 1)];
}

fn setupDatabase(allocator: std.mem.Allocator, path: []const u8) !*Database {
    std.fs.cwd().deleteFile(path) catch {};

    return Database.open(allocator, path, .{
        .create = true,
        .config = .{
            .enable_wal = false,
            .enable_fts = true,
            .buffer_pool_size = 64 * 1024 * 1024,
            .enable_query_cache = false,
        },
    });
}

fn createNodes(db: *Database, count: usize) ![]u64 {
    const labels = &[_][]const u8{"Doc"};
    const node_ids = try db.allocator.alloc(u64, count);
    errdefer db.allocator.free(node_ids);

    for (0..count) |i| {
        node_ids[i] = try db.createNode(null, labels);
    }

    return node_ids;
}

fn benchmarkScale(allocator: std.mem.Allocator, docs: usize) !BenchmarkResult {
    var path_buf: [128]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, "/tmp/lattice_bench_fts_index_{d}.ltdb", .{docs});

    var db = try setupDatabase(allocator, path);
    defer {
        db.close();
        std.fs.cwd().deleteFile(path) catch {};
    }

    const node_ids = try createNodes(db, docs);
    defer allocator.free(node_ids);

    const timings = try allocator.alloc(u64, docs);
    defer allocator.free(timings);

    var total_ns: u64 = 0;
    var max_ns: u64 = 0;

    for (node_ids, 0..) |node_id, i| {
        var text_buf: [256]u8 = undefined;
        const text = try std.fmt.bufPrint(
            &text_buf,
            "Ticket #{d}: Internet connection drops every few minutes randomly on my network causing timeouts",
            .{i},
        );

        const start = std.time.nanoTimestamp();
        try db.ftsIndexDocument(node_id, text);
        const end = std.time.nanoTimestamp();

        const elapsed: u64 = @intCast(end - start);
        timings[i] = elapsed;
        total_ns += elapsed;
        if (elapsed > max_ns) max_ns = elapsed;
    }

    const search_start = std.time.nanoTimestamp();
    const results = try db.ftsSearch("connection", 10);
    const search_end = std.time.nanoTimestamp();
    defer db.freeFtsSearchResults(results);

    if (results.len != 10) return error.InvalidData;

    const timings_for_percentiles = try allocator.dupe(u64, timings);
    defer allocator.free(timings_for_percentiles);

    return .{
        .docs = docs,
        .total_ns = total_ns,
        .mean_ns = total_ns / docs,
        .p50_ns = percentile(timings_for_percentiles, 50.0),
        .p95_ns = percentile(timings_for_percentiles, 95.0),
        .p99_ns = percentile(timings_for_percentiles, 99.0),
        .max_ns = max_ns,
        .docs_per_sec = @as(f64, @floatFromInt(docs)) / (@as(f64, @floatFromInt(total_ns)) / 1_000_000_000.0),
        .search_ns = @intCast(search_end - search_start),
    };
}

fn printHeader() void {
    std.debug.print("\n", .{});
    std.debug.print("╔══════════════════════════════════════════════════════════════════════════════╗\n", .{});
    std.debug.print("║                 Repeated-Term FTS Indexing Benchmark                        ║\n", .{});
    std.debug.print("╚══════════════════════════════════════════════════════════════════════════════╝\n", .{});
    std.debug.print("\n", .{});
    std.debug.print("Workload:\n", .{});
    std.debug.print("  • One indexed document per node\n", .{});
    std.debug.print("  • Every document shares the same common terms\n", .{});
    std.debug.print("  • Times only Database.ftsIndexDocument() per document\n", .{});
    std.debug.print("  • Uses WAL disabled to isolate the posting-list append path\n", .{});
    std.debug.print("\n", .{});
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const argv = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, argv);

    var single_scale: ?usize = null;
    var i: usize = 1;
    while (i < argv.len) : (i += 1) {
        if (std.mem.eql(u8, argv[i], "--scale")) {
            i += 1;
            if (i < argv.len) {
                single_scale = std.fmt.parseInt(usize, argv[i], 10) catch null;
            }
        }
    }

    var single_scale_arr: [1]usize = undefined;
    const scales: []const usize = if (single_scale) |scale| blk: {
        single_scale_arr[0] = scale;
        break :blk &single_scale_arr;
    } else &DEFAULT_SCALES;

    printHeader();
    if (single_scale) |scale| {
        std.debug.print("Running single scale: {d}\n\n", .{scale});
    }

    for (scales) |scale| {
        std.debug.print("Running scale={d}...\n", .{scale});
        const result = try benchmarkScale(allocator, scale);
        result.print();
    }

    std.debug.print("\nDone.\n", .{});
}
