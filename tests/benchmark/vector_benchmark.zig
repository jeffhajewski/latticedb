//! HNSW Vector Performance Benchmark
//!
//! Measures insertion throughput, search latency (mean/p50/p99),
//! recall vs brute-force, and memory usage at 1K / 10K / 100K / 1M vectors.
//! Also sweeps ef_search to show the latency-vs-recall tradeoff.
//!
//! Uses clustered vector data (Gaussian noise around random centers) to
//! produce realistic recall figures. Pass `--quick` to skip the 1M scale.

const std = @import("std");
const lattice = @import("lattice");

const hnsw_mod = lattice.vector.hnsw;
const vec_storage = lattice.vector.storage;
const buffer_pool_mod = lattice.storage.buffer_pool;
const page_manager_mod = lattice.storage.page_manager;
const vfs_mod = lattice.storage.vfs;

const HnswIndex = hnsw_mod.HnswIndex;
const HnswConfig = hnsw_mod.HnswConfig;
const SearchResult = hnsw_mod.SearchResult;
const VectorStorage = vec_storage.VectorStorage;
const BufferPool = buffer_pool_mod.BufferPool;
const PageManager = page_manager_mod.PageManager;
const PosixVfs = vfs_mod.PosixVfs;

const Allocator = std.mem.Allocator;

// ============================================================================
// Benchmark Parameters
// ============================================================================

const DIMENSIONS: u16 = 128;
const M: u16 = 16;
const M_MAX0: u16 = 32;
const EF_CONSTRUCTION: u16 = 200;
const DEFAULT_EF_SEARCH: u16 = 64;
const K: u32 = 10;
const SEED: u64 = 42;
const NUM_SEARCH_QUERIES: usize = 100;
const NUM_WARMUP_QUERIES: usize = 10;
const NUM_RECALL_QUERIES: usize = 10;

const ALL_SCALES = [_]usize{ 1_000, 10_000, 100_000, 1_000_000 };
const QUICK_SCALES = [_]usize{ 1_000, 10_000, 100_000 };
const EF_SEARCH_VALUES = [_]u16{ 16, 32, 64, 128, 256 };

// ============================================================================
// Timing Utilities
// ============================================================================

fn formatDuration(ns: u64) struct { value: f64, unit: []const u8 } {
    if (ns < 1_000) return .{ .value = @floatFromInt(ns), .unit = "ns" };
    if (ns < 1_000_000) return .{ .value = @as(f64, @floatFromInt(ns)) / 1_000.0, .unit = "μs" };
    if (ns < 1_000_000_000) return .{ .value = @as(f64, @floatFromInt(ns)) / 1_000_000.0, .unit = "ms" };
    return .{ .value = @as(f64, @floatFromInt(ns)) / 1_000_000_000.0, .unit = "s" };
}

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

// ============================================================================
// Vector Generation (Clustered)
// ============================================================================

/// Marsaglia polar method — returns one standard-normal f32 sample.
fn gaussianRandom(rng: *std.Random.DefaultPrng) f32 {
    while (true) {
        const u = rng.random().float(f32) * 2.0 - 1.0;
        const v = rng.random().float(f32) * 2.0 - 1.0;
        const s = u * u + v * v;
        if (s > 0.0 and s < 1.0) {
            const factor: f32 = @floatCast(@sqrt(-2.0 * @log(@as(f64, s)) / @as(f64, s)));
            return u * factor;
        }
    }
}

/// Generate `n` random unit-vector cluster centers.
fn generateClusterCenters(allocator: Allocator, n: usize, dimensions: u16, rng: *std.Random.DefaultPrng) ![][]f32 {
    const centers = try allocator.alloc([]f32, n);
    var initialized: usize = 0;
    errdefer {
        for (centers[0..initialized]) |c| allocator.free(c);
        allocator.free(centers);
    }

    for (0..n) |i| {
        const c = try allocator.alloc(f32, dimensions);
        var norm: f32 = 0;
        for (0..dimensions) |d| {
            c[d] = rng.random().float(f32) * 2.0 - 1.0;
            norm += c[d] * c[d];
        }
        norm = @sqrt(norm);
        if (norm > 0) {
            for (0..dimensions) |d| c[d] /= norm;
        }
        centers[i] = c;
        initialized += 1;
    }
    return centers;
}

/// Generate `count` vectors clustered around random centers with Gaussian noise
/// (σ=0.1), then normalized to unit length. This produces realistic distance
/// structure so recall measurements are meaningful.
fn generateClusteredVectors(allocator: Allocator, count: usize, dimensions: u16, rng: *std.Random.DefaultPrng) ![][]f32 {
    const num_clusters = @max(count / 100, 1);
    const centers = try generateClusterCenters(allocator, num_clusters, dimensions, rng);
    defer {
        for (centers) |c| allocator.free(c);
        allocator.free(centers);
    }

    const vectors = try allocator.alloc([]f32, count);
    var initialized: usize = 0;
    errdefer {
        for (vectors[0..initialized]) |v| allocator.free(v);
        allocator.free(vectors);
    }

    const noise_scale: f32 = 0.1;
    for (0..count) |i| {
        const center = centers[i % num_clusters];
        const v = try allocator.alloc(f32, dimensions);
        var norm: f32 = 0;
        for (0..dimensions) |d| {
            v[d] = center[d] + gaussianRandom(rng) * noise_scale;
            norm += v[d] * v[d];
        }
        norm = @sqrt(norm);
        if (norm > 0) {
            for (0..dimensions) |d| v[d] /= norm;
        }
        vectors[i] = v;
        initialized += 1;
    }

    return vectors;
}

fn freeVectors(allocator: Allocator, vectors: [][]f32) void {
    for (vectors) |v| allocator.free(v);
    allocator.free(vectors);
}

// ============================================================================
// Brute-Force and Recall
// ============================================================================

const IdDist = struct {
    id: u64,
    dist: f32,
};

fn bruteForceTopK(allocator: Allocator, query: []const f32, vectors: [][]f32, k: usize) ![10]IdDist {
    const all_dists = try allocator.alloc(IdDist, vectors.len);
    defer allocator.free(all_dists);

    for (0..vectors.len) |i| {
        all_dists[i] = .{
            .id = @intCast(i + 1),
            .dist = hnsw_mod.cosineDistance(query, vectors[i]),
        };
    }

    std.mem.sort(IdDist, all_dists, {}, struct {
        fn lessThan(_: void, a: IdDist, b: IdDist) bool {
            return a.dist < b.dist;
        }
    }.lessThan);

    var result: [10]IdDist = undefined;
    const count = @min(k, vectors.len);
    for (0..count) |i| {
        result[i] = all_dists[i];
    }

    return result;
}

fn computeRecall(hnsw_results: []SearchResult, brute_top_k: []const IdDist, k: usize) f64 {
    var hits: u32 = 0;
    for (brute_top_k[0..k]) |true_nn| {
        for (hnsw_results) |hnsw_nn| {
            if (hnsw_nn.node_id == true_nn.id) {
                hits += 1;
                break;
            }
        }
    }
    return @as(f64, @floatFromInt(hits)) / @as(f64, @floatFromInt(k));
}

// ============================================================================
// Index Setup / Teardown
// ============================================================================

const BenchIndex = struct {
    allocator: Allocator,
    posix_vfs: PosixVfs,
    pm: *PageManager,
    bp: *BufferPool,
    vs: *VectorStorage,
    index: *HnswIndex,
    path: []const u8,

    fn init(allocator: Allocator, scale: usize, ef_search: u16) !BenchIndex {
        var path_buf: [128]u8 = undefined;
        const timestamp = std.time.milliTimestamp();
        const random_val = std.crypto.random.int(u32);
        const path = try std.fmt.bufPrint(&path_buf, "/tmp/lattice_vecbench_{d}_{d}_{x}.db", .{ scale, timestamp, random_val });
        const path_copy = try allocator.dupe(u8, path);
        errdefer allocator.free(path_copy);

        var posix_vfs = PosixVfs.init(allocator);
        const vfs_impl = posix_vfs.vfs();
        vfs_impl.delete(path_copy) catch {};

        const pm = try allocator.create(PageManager);
        errdefer allocator.destroy(pm);
        pm.* = try PageManager.init(allocator, vfs_impl, path_copy, .{ .create = true });
        errdefer pm.deinit();

        // Size buffer pool generously for the scale
        const pool_bytes: usize = if (scale >= 1_000_000)
            @as(usize, 4) * 1024 * 1024 * 1024 // 4GB for 1M+
        else
            @min(
                @as(usize, scale) * 4096 + 64 * 1024 * 1024, // scale * 4K + 64MB headroom
                @as(usize, 2) * 1024 * 1024 * 1024, // cap at 2GB
            );

        const bp = try allocator.create(BufferPool);
        errdefer allocator.destroy(bp);
        bp.* = try BufferPool.init(allocator, pm, pool_bytes);

        const vs = try allocator.create(VectorStorage);
        errdefer allocator.destroy(vs);
        vs.* = try VectorStorage.init(allocator, bp, DIMENSIONS);

        const config = HnswConfig{
            .dimensions = DIMENSIONS,
            .m = M,
            .m_max0 = M_MAX0,
            .ef_construction = EF_CONSTRUCTION,
            .ef_search = ef_search,
            .metric = .cosine,
        };

        const index = try allocator.create(HnswIndex);
        errdefer allocator.destroy(index);
        index.* = HnswIndex.init(allocator, bp, vs, config);

        return BenchIndex{
            .allocator = allocator,
            .posix_vfs = posix_vfs,
            .pm = pm,
            .bp = bp,
            .vs = vs,
            .index = index,
            .path = path_copy,
        };
    }

    fn deinit(self: *BenchIndex) void {
        self.index.deinit();
        self.bp.deinit();
        self.pm.deinit();

        const vfs_impl = self.posix_vfs.vfs();
        vfs_impl.delete(self.path) catch {};

        self.allocator.destroy(self.index);
        self.allocator.destroy(self.vs);
        self.allocator.destroy(self.bp);
        self.allocator.destroy(self.pm);
        self.allocator.free(self.path);
    }
};

// ============================================================================
// Scale Benchmark
// ============================================================================

const ScaleResult = struct {
    scale: usize,
    insert_ms: f64,
    mean_search_us: f64,
    p50_search_us: f64,
    p99_search_us: f64,
    recall: f64,
    memory_mb: f64,
};

fn runScaleBenchmark(allocator: Allocator, scale: usize) !ScaleResult {
    std.debug.print("  Running scale={d}...\n", .{scale});

    var bench = try BenchIndex.init(allocator, scale, DEFAULT_EF_SEARCH);
    defer bench.deinit();

    // Generate clustered vectors for realistic recall
    var rng = std.Random.DefaultPrng.init(SEED);
    const vectors = try generateClusteredVectors(allocator, scale, DIMENSIONS, &rng);
    defer freeVectors(allocator, vectors);

    // --- Insertion ---
    const report_interval = @max(scale / 10, 10_000);
    const insert_start = std.time.nanoTimestamp();
    for (0..scale) |i| {
        try bench.index.insert(@intCast(i + 1), vectors[i]);
        if ((i + 1) % report_interval == 0) {
            std.debug.print("    Inserted {d}/{d}...\n", .{ i + 1, scale });
        }
    }
    const insert_end = std.time.nanoTimestamp();
    const insert_ns: u64 = @intCast(insert_end - insert_start);

    // Generate search queries from same clustered distribution
    const queries = try generateClusteredVectors(allocator, NUM_SEARCH_QUERIES, DIMENSIONS, &rng);
    defer freeVectors(allocator, queries);

    // --- Warmup ---
    for (0..NUM_WARMUP_QUERIES) |i| {
        const results = try bench.index.search(queries[i], K, null);
        bench.index.freeResults(results);
    }

    // --- Search latency ---
    var timings: [NUM_SEARCH_QUERIES]u64 = undefined;
    for (0..NUM_SEARCH_QUERIES) |i| {
        const t0 = std.time.nanoTimestamp();
        const results = try bench.index.search(queries[i], K, null);
        const t1 = std.time.nanoTimestamp();
        bench.index.freeResults(results);
        timings[i] = @intCast(t1 - t0);
    }

    var total_ns: u64 = 0;
    for (timings[0..NUM_SEARCH_QUERIES]) |t| total_ns += t;
    const mean_ns = total_ns / NUM_SEARCH_QUERIES;
    const p50_ns = percentile(timings[0..NUM_SEARCH_QUERIES], 50.0);
    const p99_ns = percentile(timings[0..NUM_SEARCH_QUERIES], 99.0);

    // --- Recall ---
    var total_recall: f64 = 0;
    for (0..NUM_RECALL_QUERIES) |i| {
        const hnsw_results = try bench.index.search(queries[i], K, null);
        defer bench.index.freeResults(hnsw_results);

        var bf_topk = try bruteForceTopK(allocator, queries[i], vectors, K);
        const recall = computeRecall(hnsw_results, &bf_topk, K);
        total_recall += recall;
    }
    const avg_recall = total_recall / @as(f64, @floatFromInt(NUM_RECALL_QUERIES));

    // --- Memory ---
    const stats = bench.index.getStats();
    const memory_mb = @as(f64, @floatFromInt(stats.memory_bytes)) / (1024.0 * 1024.0);

    return ScaleResult{
        .scale = scale,
        .insert_ms = nsToMs(insert_ns),
        .mean_search_us = nsToUs(mean_ns),
        .p50_search_us = nsToUs(p50_ns),
        .p99_search_us = nsToUs(p99_ns),
        .recall = avg_recall * 100.0,
        .memory_mb = memory_mb,
    };
}

// ============================================================================
// ef_search Sensitivity
// ============================================================================

const EfResult = struct {
    ef_search: u16,
    mean_search_us: f64,
    recall: f64,
};

fn runEfSearchSensitivity(allocator: Allocator, scale: usize) ![]EfResult {
    std.debug.print("\n  Building index for ef_search sensitivity ({d} vectors)...\n", .{scale});

    // Build one index, test multiple ef values
    var bench = try BenchIndex.init(allocator, scale, DEFAULT_EF_SEARCH);
    defer bench.deinit();

    var rng = std.Random.DefaultPrng.init(SEED);
    const vectors = try generateClusteredVectors(allocator, scale, DIMENSIONS, &rng);
    defer freeVectors(allocator, vectors);

    const report_interval = @max(scale / 10, 10_000);
    for (0..scale) |i| {
        try bench.index.insert(@intCast(i + 1), vectors[i]);
        if ((i + 1) % report_interval == 0) {
            std.debug.print("    Inserted {d}/{d}...\n", .{ i + 1, scale });
        }
    }

    // Generate queries from same clustered distribution
    const queries = try generateClusteredVectors(allocator, NUM_SEARCH_QUERIES, DIMENSIONS, &rng);
    defer freeVectors(allocator, queries);

    var results = try allocator.alloc(EfResult, EF_SEARCH_VALUES.len);
    errdefer allocator.free(results);

    for (EF_SEARCH_VALUES, 0..) |ef, ef_idx| {
        std.debug.print("  Testing ef_search={d}...\n", .{ef});

        // Warmup
        for (0..NUM_WARMUP_QUERIES) |i| {
            const r = try bench.index.search(queries[i], K, ef);
            bench.index.freeResults(r);
        }

        // Latency
        var total_ns: u64 = 0;
        for (0..NUM_SEARCH_QUERIES) |i| {
            const t0 = std.time.nanoTimestamp();
            const r = try bench.index.search(queries[i], K, ef);
            const t1 = std.time.nanoTimestamp();
            bench.index.freeResults(r);
            total_ns += @as(u64, @intCast(t1 - t0));
        }
        const mean_ns = total_ns / NUM_SEARCH_QUERIES;

        // Recall
        var total_recall: f64 = 0;
        for (0..NUM_RECALL_QUERIES) |i| {
            const hnsw_results = try bench.index.search(queries[i], K, ef);
            defer bench.index.freeResults(hnsw_results);

            var bf_topk = try bruteForceTopK(allocator, queries[i], vectors, K);
            const recall = computeRecall(hnsw_results, &bf_topk, K);
            total_recall += recall;
        }
        const avg_recall = total_recall / @as(f64, @floatFromInt(NUM_RECALL_QUERIES));

        results[ef_idx] = EfResult{
            .ef_search = ef,
            .mean_search_us = nsToUs(mean_ns),
            .recall = avg_recall * 100.0,
        };
    }

    return results;
}

// ============================================================================
// Output Formatting
// ============================================================================

fn printHeader() void {
    std.debug.print("\n", .{});
    std.debug.print("╔══════════════════════════════════════════════════════════╗\n", .{});
    std.debug.print("║       HNSW Vector Performance Benchmark                ║\n", .{});
    std.debug.print("╠══════════════════════════════════════════════════════════╣\n", .{});
    std.debug.print("║ Dimensions: {d:<4} Metric: cosine  M: {d:<3} ef_c: {d:<4}     ║\n", .{ DIMENSIONS, M, EF_CONSTRUCTION });
    std.debug.print("╚══════════════════════════════════════════════════════════╝\n", .{});
}

fn printScaleResults(results: []const ScaleResult) void {
    std.debug.print("\n═══ Scale Results ═══\n", .{});
    std.debug.print("┌─────────┬─────────────┬─────────────┬─────────────┬───────────┬─────────────┐\n", .{});
    std.debug.print("│ Scale   │ Insert (ms) │ Mean (μs)   │ P99 (μs)    │ Recall@10 │ Memory (MB) │\n", .{});
    std.debug.print("├─────────┼─────────────┼─────────────┼─────────────┼───────────┼─────────────┤\n", .{});

    for (results) |r| {
        std.debug.print("│ {d:>7} │ {d:>11.2} │ {d:>11.2} │ {d:>11.2} │   {d:>5.1}%  │ {d:>11.1} │\n", .{
            r.scale,
            r.insert_ms,
            r.mean_search_us,
            r.p99_search_us,
            r.recall,
            r.memory_mb,
        });
    }

    std.debug.print("└─────────┴─────────────┴─────────────┴─────────────┴───────────┴─────────────┘\n", .{});
}

fn printEfResults(results: []const EfResult, scale: usize) void {
    std.debug.print("\n═══ ef_search Sensitivity ({d} vectors) ═══\n", .{scale});
    std.debug.print("┌───────────┬─────────────┬───────────┐\n", .{});
    std.debug.print("│ ef_search │ Mean (μs)   │ Recall@10 │\n", .{});
    std.debug.print("├───────────┼─────────────┼───────────┤\n", .{});

    for (results) |r| {
        std.debug.print("│ {d:>9} │ {d:>11.2} │   {d:>5.1}%  │\n", .{
            r.ef_search,
            r.mean_search_us,
            r.recall,
        });
    }

    std.debug.print("└───────────┴─────────────┴───────────┘\n", .{});
}

// ============================================================================
// Main
// ============================================================================

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Parse CLI args for --quick flag
    const argv = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, argv);

    var quick = false;
    for (argv[1..]) |arg| {
        if (std.mem.eql(u8, arg, "--quick")) {
            quick = true;
        }
    }

    const scales: []const usize = if (quick) &QUICK_SCALES else &ALL_SCALES;

    printHeader();
    if (quick) {
        std.debug.print("  (quick mode — skipping 1M scale)\n", .{});
    }

    // --- Scale benchmarks ---
    const scale_results = try allocator.alloc(ScaleResult, scales.len);
    defer allocator.free(scale_results);

    for (scales, 0..) |scale, i| {
        scale_results[i] = try runScaleBenchmark(allocator, scale);
    }
    printScaleResults(scale_results);

    // --- ef_search sensitivity at largest scale ---
    const ef_scale = scales[scales.len - 1];
    const ef_results = try runEfSearchSensitivity(allocator, ef_scale);
    defer allocator.free(ef_results);
    printEfResults(ef_results, ef_scale);

    std.debug.print("\nBenchmark complete.\n", .{});
}
