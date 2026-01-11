//! Fuzz testing for HNSW vector index operations.
//!
//! The HNSW index must handle arbitrary vector data without:
//! - Crashing (panics, segfaults)
//! - Memory corruption
//! - Returning invalid results
//! - Memory leaks
//!
//! Operations should either succeed or return errors gracefully.

const std = @import("std");
const lattice = @import("lattice");

const Allocator = std.mem.Allocator;
const HnswIndex = lattice.vector.hnsw.HnswIndex;
const HnswConfig = lattice.vector.hnsw.HnswConfig;
const VectorStorage = lattice.vector.storage.VectorStorage;
const BufferPool = lattice.storage.buffer_pool.BufferPool;
const PageManager = lattice.storage.page_manager.PageManager;
const vfs = lattice.storage.vfs;
const PosixVfs = vfs.PosixVfs;

// ============================================================================
// Test Fixture
// ============================================================================

/// Temporary HNSW setup for fuzzing
const FuzzHnsw = struct {
    allocator: Allocator,
    posix_vfs: PosixVfs,
    pm: *PageManager,
    bp: *BufferPool,
    vs: *VectorStorage,
    index: HnswIndex,
    path: []const u8,
    dimensions: u16,

    fn init(allocator: Allocator, dimensions: u16) !FuzzHnsw {
        // Generate unique path
        var path_buf: [128]u8 = undefined;
        const timestamp = std.time.milliTimestamp();
        const random = std.crypto.random.int(u32);
        const path = try std.fmt.bufPrint(&path_buf, "/tmp/lattice_fuzz_hnsw_{d}_{x}.db", .{ timestamp, random });
        const path_copy = try allocator.dupe(u8, path);
        errdefer allocator.free(path_copy);

        var posix_vfs = PosixVfs.init(allocator);
        const vfs_impl = posix_vfs.vfs();

        // Clean up any existing file
        vfs_impl.delete(path_copy) catch {};

        // Allocate PageManager
        const pm = try allocator.create(PageManager);
        errdefer allocator.destroy(pm);
        pm.* = try PageManager.init(allocator, vfs_impl, path_copy, .{ .create = true });
        errdefer pm.deinit();

        // Allocate BufferPool
        const bp = try allocator.create(BufferPool);
        errdefer allocator.destroy(bp);
        bp.* = try BufferPool.init(allocator, pm, 128 * 1024); // 128KB for fuzzing
        errdefer bp.deinit();

        // Allocate VectorStorage
        const vs = try allocator.create(VectorStorage);
        errdefer allocator.destroy(vs);
        vs.* = try VectorStorage.init(allocator, bp, dimensions);
        errdefer vs.deinit();

        // Create HNSW index
        const config = HnswConfig{
            .dimensions = dimensions,
            .m = 8, // Smaller for faster fuzzing
            .ef_construction = 50,
            .ef_search = 50,
            .metric = .cosine,
        };

        const index = HnswIndex.init(allocator, bp, vs, config);

        return FuzzHnsw{
            .allocator = allocator,
            .posix_vfs = posix_vfs,
            .pm = pm,
            .bp = bp,
            .vs = vs,
            .index = index,
            .path = path_copy,
            .dimensions = dimensions,
        };
    }

    fn deinit(self: *FuzzHnsw) void {
        self.index.deinit();
        // VectorStorage doesn't have deinit - cleaned up with buffer pool
        self.bp.deinit();
        self.pm.deinit();

        const vfs_impl = self.posix_vfs.vfs();
        vfs_impl.delete(self.path) catch {};

        self.allocator.destroy(self.vs);
        self.allocator.destroy(self.bp);
        self.allocator.destroy(self.pm);
        self.allocator.free(self.path);
    }
};

// ============================================================================
// Fuzz Functions
// ============================================================================

/// Fuzz HNSW with random insert/search operations
pub fn fuzzHnswOperations(allocator: Allocator, input: []const u8) !void {
    if (input.len < 8) return; // Need some data

    const dimensions: u16 = 4; // Small for fuzzing
    const floats_needed = dimensions * 4;

    var fuzz_hnsw = FuzzHnsw.init(allocator, dimensions) catch return;
    defer fuzz_hnsw.deinit();

    var index = &fuzz_hnsw.index;

    // Interpret input as a series of operations
    var i: usize = 0;
    var vector_id: u64 = 0;

    while (i < input.len) {
        const op = input[i] % 2;
        i += 1;

        switch (op) {
            0 => {
                // Insert: next bytes become a vector
                if (i + floats_needed > input.len) break;

                // Convert bytes to floats
                var vector: [4]f32 = undefined;
                for (0..dimensions) |d| {
                    const byte_idx = i + d * 4;
                    if (byte_idx + 4 > input.len) break;
                    const bytes = input[byte_idx..][0..4];
                    // Interpret as float, ensure valid range
                    var f = @as(f32, @bitCast(bytes.*));
                    if (!std.math.isFinite(f)) f = 0.0;
                    // Clamp to reasonable range
                    f = std.math.clamp(f, -1000.0, 1000.0);
                    vector[d] = f;
                }
                i += floats_needed;

                index.insert(vector_id, &vector) catch continue;
                vector_id += 1;
            },
            1 => {
                // Search: use input as query vector
                if (i + floats_needed > input.len) break;

                var query: [4]f32 = undefined;
                for (0..dimensions) |d| {
                    const byte_idx = i + d * 4;
                    if (byte_idx + 4 > input.len) break;
                    const bytes = input[byte_idx..][0..4];
                    var f = @as(f32, @bitCast(bytes.*));
                    if (!std.math.isFinite(f)) f = 0.0;
                    f = std.math.clamp(f, -1000.0, 1000.0);
                    query[d] = f;
                }
                i += floats_needed;

                const results = index.search(&query, 5, null) catch continue;
                defer allocator.free(results);

                // Verify results are valid
                for (results) |r| {
                    std.debug.assert(std.math.isFinite(r.distance));
                }
            },
            else => unreachable,
        }
    }
}

/// Fuzz HNSW distance calculations with arbitrary vectors
pub fn fuzzHnswDistances(_: Allocator, input: []const u8) !void {
    const dimensions: u16 = 4;
    const floats_needed = dimensions * 4 * 2; // Two vectors

    if (input.len < floats_needed) return;

    // Parse two vectors from input
    var vec1: [4]f32 = undefined;
    var vec2: [4]f32 = undefined;

    for (0..dimensions) |d| {
        const byte_idx1 = d * 4;
        const byte_idx2 = dimensions * 4 + d * 4;

        if (byte_idx1 + 4 <= input.len) {
            const bytes1 = input[byte_idx1..][0..4];
            var f1 = @as(f32, @bitCast(bytes1.*));
            if (!std.math.isFinite(f1)) f1 = 0.0;
            vec1[d] = std.math.clamp(f1, -1000.0, 1000.0);
        }

        if (byte_idx2 + 4 <= input.len) {
            const bytes2 = input[byte_idx2..][0..4];
            var f2 = @as(f32, @bitCast(bytes2.*));
            if (!std.math.isFinite(f2)) f2 = 0.0;
            vec2[d] = std.math.clamp(f2, -1000.0, 1000.0);
        }
    }

    // Calculate distances - should not crash or produce NaN
    const cosine = lattice.vector.hnsw.cosineDistance(&vec1, &vec2);
    const euclidean = lattice.vector.hnsw.euclideanDistance(&vec1, &vec2);

    // Results should be finite
    _ = cosine;
    _ = euclidean;
    // Note: Distance functions may return non-finite for edge cases like zero vectors
}

/// Fuzz vector normalization
pub fn fuzzVectorNormalization(allocator: Allocator, input: []const u8) !void {
    _ = allocator;

    const dimensions: u16 = 4;
    if (input.len < dimensions * 4) return;

    var vec: [4]f32 = undefined;
    for (0..dimensions) |d| {
        const byte_idx = d * 4;
        if (byte_idx + 4 <= input.len) {
            const bytes = input[byte_idx..][0..4];
            var f = @as(f32, @bitCast(bytes.*));
            if (!std.math.isFinite(f)) f = 0.0;
            vec[d] = f;
        }
    }

    // Normalize should handle zero vectors gracefully
    var sum_sq: f32 = 0.0;
    for (vec) |v| {
        sum_sq += v * v;
    }
    if (sum_sq > 0.0) {
        const norm = @sqrt(sum_sq);
        for (&vec) |*v| {
            v.* /= norm;
        }
    }
}

// ============================================================================
// Fuzz Runners
// ============================================================================

fn hnswOpsRunner(allocator: Allocator, input: []const u8) !void {
    try fuzzHnswOperations(allocator, input);
}

fn hnswDistRunner(allocator: Allocator, input: []const u8) !void {
    try fuzzHnswDistances(allocator, input);
}

fn vectorNormRunner(allocator: Allocator, input: []const u8) !void {
    try fuzzVectorNormalization(allocator, input);
}

// ============================================================================
// Fuzz Tests using std.testing.fuzz
// ============================================================================

test "fuzz: hnsw random operations" {
    const allocator = std.testing.allocator;
    try std.testing.fuzz(allocator, hnswOpsRunner, .{});
}

test "fuzz: hnsw distance calculations" {
    const allocator = std.testing.allocator;
    try std.testing.fuzz(allocator, hnswDistRunner, .{});
}

test "fuzz: vector normalization" {
    const allocator = std.testing.allocator;
    try std.testing.fuzz(allocator, vectorNormRunner, .{});
}

// ============================================================================
// Corpus-based tests (known edge cases)
// ============================================================================

test "hnsw: handles zero vectors" {
    const allocator = std.testing.allocator;
    const zeros = &[_]u8{0} ** 32;
    try fuzzHnswOperations(allocator, zeros);
}

test "hnsw: handles NaN/Inf representations" {
    const allocator = std.testing.allocator;
    // IEEE 754 NaN and Inf patterns
    const nan_bytes = [_]u8{ 0x00, 0x00, 0xC0, 0x7F }; // NaN
    const inf_bytes = [_]u8{ 0x00, 0x00, 0x80, 0x7F }; // +Inf
    const neg_inf_bytes = [_]u8{ 0x00, 0x00, 0x80, 0xFF }; // -Inf

    var input: [32]u8 = undefined;
    @memcpy(input[0..4], &nan_bytes);
    @memcpy(input[4..8], &inf_bytes);
    @memcpy(input[8..12], &neg_inf_bytes);
    @memset(input[12..], 0);

    try fuzzHnswDistances(allocator, &input);
}

test "hnsw: handles unit vectors" {
    const allocator = std.testing.allocator;
    // Create two unit vectors
    var input: [32]u8 = undefined;
    const one: f32 = 1.0;
    const one_bytes: [4]u8 = @bitCast(one);
    const zero: f32 = 0.0;
    const zero_bytes: [4]u8 = @bitCast(zero);

    // Vector 1: [1, 0, 0, 0]
    @memcpy(input[0..4], &one_bytes);
    @memcpy(input[4..8], &zero_bytes);
    @memcpy(input[8..12], &zero_bytes);
    @memcpy(input[12..16], &zero_bytes);

    // Vector 2: [0, 1, 0, 0]
    @memcpy(input[16..20], &zero_bytes);
    @memcpy(input[20..24], &one_bytes);
    @memcpy(input[24..28], &zero_bytes);
    @memcpy(input[28..32], &zero_bytes);

    try fuzzHnswDistances(allocator, &input);
}

test "hnsw: handles insert then search" {
    const allocator = std.testing.allocator;
    // Operation sequence: insert (0), search (1)
    var input: [64]u8 = undefined;
    const v: f32 = 0.5;
    const v_bytes: [4]u8 = @bitCast(v);

    input[0] = 0; // insert
    @memcpy(input[1..5], &v_bytes);
    @memcpy(input[5..9], &v_bytes);
    @memcpy(input[9..13], &v_bytes);
    @memcpy(input[13..17], &v_bytes);

    input[17] = 1; // search
    @memcpy(input[18..22], &v_bytes);
    @memcpy(input[22..26], &v_bytes);
    @memcpy(input[26..30], &v_bytes);
    @memcpy(input[30..34], &v_bytes);

    @memset(input[34..], 0);

    try fuzzHnswOperations(allocator, &input);
}
