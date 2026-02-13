//! SIMD-Optimized Distance Functions
//!
//! Vectorized implementations of distance metrics for vector search.
//! Uses Zig's @Vector for portable SIMD across architectures.
//!
//! Performance: ~4-8x speedup over scalar implementations for typical
//! embedding dimensions (384, 768, 1536).

const std = @import("std");

/// SIMD vector width - 8 floats for AVX-256 compatibility
/// Also works well on ARM NEON (128-bit, processes as 2x4)
const SIMD_WIDTH = 8;
const SimdF32 = @Vector(SIMD_WIDTH, f32);

/// Compute Euclidean distance (L2) between two vectors using SIMD
/// Returns: sqrt(sum((a[i] - b[i])^2))
pub fn euclideanDistance(a: []const f32, b: []const f32) f32 {
    std.debug.assert(a.len == b.len);
    return @sqrt(euclideanDistanceSquared(a, b));
}

/// Compute squared Euclidean distance (avoids sqrt for comparisons)
/// Returns: sum((a[i] - b[i])^2)
pub fn euclideanDistanceSquared(a: []const f32, b: []const f32) f32 {
    std.debug.assert(a.len == b.len);

    const len = a.len;
    const simd_len = len - (len % SIMD_WIDTH);

    var sum_vec: SimdF32 = @splat(0.0);

    // SIMD loop - process 8 floats at a time
    var i: usize = 0;
    while (i < simd_len) : (i += SIMD_WIDTH) {
        const va: SimdF32 = a[i..][0..SIMD_WIDTH].*;
        const vb: SimdF32 = b[i..][0..SIMD_WIDTH].*;
        const diff = va - vb;
        sum_vec += diff * diff;
    }

    // Horizontal sum of SIMD vector
    var sum = @reduce(.Add, sum_vec);

    // Scalar remainder
    while (i < len) : (i += 1) {
        const diff = a[i] - b[i];
        sum += diff * diff;
    }

    return sum;
}

/// Compute cosine distance between two vectors using SIMD
/// Returns: 1 - (a · b) / (||a|| * ||b||)
/// Range: [0, 2] where 0 = identical, 1 = orthogonal, 2 = opposite
pub fn cosineDistance(a: []const f32, b: []const f32) f32 {
    std.debug.assert(a.len == b.len);

    const len = a.len;
    const simd_len = len - (len % SIMD_WIDTH);

    var dot_vec: SimdF32 = @splat(0.0);
    var norm_a_vec: SimdF32 = @splat(0.0);
    var norm_b_vec: SimdF32 = @splat(0.0);

    // SIMD loop - compute dot product and norms simultaneously
    var i: usize = 0;
    while (i < simd_len) : (i += SIMD_WIDTH) {
        const va: SimdF32 = a[i..][0..SIMD_WIDTH].*;
        const vb: SimdF32 = b[i..][0..SIMD_WIDTH].*;

        dot_vec += va * vb;
        norm_a_vec += va * va;
        norm_b_vec += vb * vb;
    }

    // Horizontal sums
    var dot = @reduce(.Add, dot_vec);
    var norm_a = @reduce(.Add, norm_a_vec);
    var norm_b = @reduce(.Add, norm_b_vec);

    // Scalar remainder
    while (i < len) : (i += 1) {
        dot += a[i] * b[i];
        norm_a += a[i] * a[i];
        norm_b += b[i] * b[i];
    }

    const denom = @sqrt(norm_a) * @sqrt(norm_b);
    if (denom == 0.0) return 0.0;

    // Clamp to handle floating point errors
    const similarity = std.math.clamp(dot / denom, -1.0, 1.0);
    return 1.0 - similarity;
}

/// Compute inner product distance (negative dot product)
/// Returns: -sum(a[i] * b[i])
/// Note: For normalized vectors, this is equivalent to cosine distance
pub fn innerProductDistance(a: []const f32, b: []const f32) f32 {
    return -dotProduct(a, b);
}

/// Compute dot product using SIMD
/// Returns: sum(a[i] * b[i])
pub fn dotProduct(a: []const f32, b: []const f32) f32 {
    std.debug.assert(a.len == b.len);

    const len = a.len;
    const simd_len = len - (len % SIMD_WIDTH);

    var dot_vec: SimdF32 = @splat(0.0);

    var i: usize = 0;
    while (i < simd_len) : (i += SIMD_WIDTH) {
        const va: SimdF32 = a[i..][0..SIMD_WIDTH].*;
        const vb: SimdF32 = b[i..][0..SIMD_WIDTH].*;
        dot_vec += va * vb;
    }

    var dot = @reduce(.Add, dot_vec);

    while (i < len) : (i += 1) {
        dot += a[i] * b[i];
    }

    return dot;
}

/// Compute L2 norm (magnitude) of a vector using SIMD
/// Returns: sqrt(sum(a[i]^2))
pub fn norm(a: []const f32) f32 {
    const len = a.len;
    const simd_len = len - (len % SIMD_WIDTH);

    var sum_vec: SimdF32 = @splat(0.0);

    var i: usize = 0;
    while (i < simd_len) : (i += SIMD_WIDTH) {
        const va: SimdF32 = a[i..][0..SIMD_WIDTH].*;
        sum_vec += va * va;
    }

    var sum = @reduce(.Add, sum_vec);

    while (i < len) : (i += 1) {
        sum += a[i] * a[i];
    }

    return @sqrt(sum);
}

/// Normalize a vector in-place using SIMD
/// After normalization: ||a|| = 1
pub fn normalize(a: []f32) void {
    const n = norm(a);
    if (n == 0.0) return;

    const len = a.len;
    const simd_len = len - (len % SIMD_WIDTH);
    const inv_norm: SimdF32 = @splat(1.0 / n);

    var i: usize = 0;
    while (i < simd_len) : (i += SIMD_WIDTH) {
        const va: SimdF32 = a[i..][0..SIMD_WIDTH].*;
        const normalized = va * inv_norm;
        a[i..][0..SIMD_WIDTH].* = normalized;
    }

    const inv_norm_scalar = 1.0 / n;
    while (i < len) : (i += 1) {
        a[i] *= inv_norm_scalar;
    }
}

// ============================================================================
// Batch Distance Functions (4-way interleaved)
// ============================================================================

/// Compute dot product of query against 4 target vectors simultaneously.
/// Loads each query chunk once and reuses across 4 targets — saves 3/4 of
/// query memory loads vs 4 individual dotProduct calls.
pub fn dotProductBatch4(
    query: []const f32,
    v0: []const f32,
    v1: []const f32,
    v2: []const f32,
    v3: []const f32,
) [4]f32 {
    std.debug.assert(query.len == v0.len);
    std.debug.assert(query.len == v1.len);
    std.debug.assert(query.len == v2.len);
    std.debug.assert(query.len == v3.len);

    const len = query.len;
    const simd_len = len - (len % SIMD_WIDTH);

    var dot0: SimdF32 = @splat(0.0);
    var dot1: SimdF32 = @splat(0.0);
    var dot2: SimdF32 = @splat(0.0);
    var dot3: SimdF32 = @splat(0.0);

    var i: usize = 0;
    while (i < simd_len) : (i += SIMD_WIDTH) {
        const vq: SimdF32 = query[i..][0..SIMD_WIDTH].*;
        dot0 += vq * @as(SimdF32, v0[i..][0..SIMD_WIDTH].*);
        dot1 += vq * @as(SimdF32, v1[i..][0..SIMD_WIDTH].*);
        dot2 += vq * @as(SimdF32, v2[i..][0..SIMD_WIDTH].*);
        dot3 += vq * @as(SimdF32, v3[i..][0..SIMD_WIDTH].*);
    }

    var result = [4]f32{
        @reduce(.Add, dot0),
        @reduce(.Add, dot1),
        @reduce(.Add, dot2),
        @reduce(.Add, dot3),
    };

    // Scalar remainder
    while (i < len) : (i += 1) {
        result[0] += query[i] * v0[i];
        result[1] += query[i] * v1[i];
        result[2] += query[i] * v2[i];
        result[3] += query[i] * v3[i];
    }

    return result;
}

/// Compute squared Euclidean distance of query against 4 target vectors simultaneously.
pub fn euclideanDistanceSquaredBatch4(
    query: []const f32,
    v0: []const f32,
    v1: []const f32,
    v2: []const f32,
    v3: []const f32,
) [4]f32 {
    std.debug.assert(query.len == v0.len);
    std.debug.assert(query.len == v1.len);
    std.debug.assert(query.len == v2.len);
    std.debug.assert(query.len == v3.len);

    const len = query.len;
    const simd_len = len - (len % SIMD_WIDTH);

    var sum0: SimdF32 = @splat(0.0);
    var sum1: SimdF32 = @splat(0.0);
    var sum2: SimdF32 = @splat(0.0);
    var sum3: SimdF32 = @splat(0.0);

    var i: usize = 0;
    while (i < simd_len) : (i += SIMD_WIDTH) {
        const vq: SimdF32 = query[i..][0..SIMD_WIDTH].*;
        const d0 = vq - @as(SimdF32, v0[i..][0..SIMD_WIDTH].*);
        const d1 = vq - @as(SimdF32, v1[i..][0..SIMD_WIDTH].*);
        const d2 = vq - @as(SimdF32, v2[i..][0..SIMD_WIDTH].*);
        const d3 = vq - @as(SimdF32, v3[i..][0..SIMD_WIDTH].*);
        sum0 += d0 * d0;
        sum1 += d1 * d1;
        sum2 += d2 * d2;
        sum3 += d3 * d3;
    }

    var result = [4]f32{
        @reduce(.Add, sum0),
        @reduce(.Add, sum1),
        @reduce(.Add, sum2),
        @reduce(.Add, sum3),
    };

    // Scalar remainder
    while (i < len) : (i += 1) {
        const d0 = query[i] - v0[i];
        const d1 = query[i] - v1[i];
        const d2 = query[i] - v2[i];
        const d3 = query[i] - v3[i];
        result[0] += d0 * d0;
        result[1] += d1 * d1;
        result[2] += d2 * d2;
        result[3] += d3 * d3;
    }

    return result;
}

/// Compute inner product distance (negative dot product) of query against 4 targets.
pub fn innerProductDistanceBatch4(
    query: []const f32,
    v0: []const f32,
    v1: []const f32,
    v2: []const f32,
    v3: []const f32,
) [4]f32 {
    const dots = dotProductBatch4(query, v0, v1, v2, v3);
    return .{ -dots[0], -dots[1], -dots[2], -dots[3] };
}

// ============================================================================
// Tests
// ============================================================================

const testing = std.testing;

test "euclidean distance - identical vectors" {
    const a = [_]f32{ 1.0, 2.0, 3.0, 4.0 };
    const b = [_]f32{ 1.0, 2.0, 3.0, 4.0 };

    const dist = euclideanDistance(&a, &b);
    try testing.expectApproxEqAbs(@as(f32, 0.0), dist, 0.0001);
}

test "euclidean distance - known values" {
    const a = [_]f32{ 0.0, 0.0, 0.0 };
    const b = [_]f32{ 1.0, 2.0, 2.0 };

    // sqrt(1 + 4 + 4) = sqrt(9) = 3
    const dist = euclideanDistance(&a, &b);
    try testing.expectApproxEqAbs(@as(f32, 3.0), dist, 0.0001);
}

test "euclidean distance - large vector (SIMD path)" {
    var a: [384]f32 = undefined;
    var b: [384]f32 = undefined;

    for (0..384) |i| {
        a[i] = @floatFromInt(i);
        b[i] = @floatFromInt(i);
    }

    const dist = euclideanDistance(&a, &b);
    try testing.expectApproxEqAbs(@as(f32, 0.0), dist, 0.0001);

    // Modify one element
    b[100] = a[100] + 10.0;
    const dist2 = euclideanDistance(&a, &b);
    try testing.expectApproxEqAbs(@as(f32, 10.0), dist2, 0.0001);
}

test "cosine distance - identical vectors" {
    const a = [_]f32{ 1.0, 2.0, 3.0 };
    const b = [_]f32{ 1.0, 2.0, 3.0 };

    const dist = cosineDistance(&a, &b);
    try testing.expectApproxEqAbs(@as(f32, 0.0), dist, 0.0001);
}

test "cosine distance - orthogonal vectors" {
    const a = [_]f32{ 1.0, 0.0, 0.0 };
    const b = [_]f32{ 0.0, 1.0, 0.0 };

    const dist = cosineDistance(&a, &b);
    try testing.expectApproxEqAbs(@as(f32, 1.0), dist, 0.0001);
}

test "cosine distance - opposite vectors" {
    const a = [_]f32{ 1.0, 2.0, 3.0 };
    const b = [_]f32{ -1.0, -2.0, -3.0 };

    const dist = cosineDistance(&a, &b);
    try testing.expectApproxEqAbs(@as(f32, 2.0), dist, 0.0001);
}

test "cosine distance - scaled vectors same direction" {
    const a = [_]f32{ 1.0, 2.0, 3.0 };
    const b = [_]f32{ 2.0, 4.0, 6.0 };

    // Should be 0 - same direction regardless of magnitude
    const dist = cosineDistance(&a, &b);
    try testing.expectApproxEqAbs(@as(f32, 0.0), dist, 0.0001);
}

test "cosine distance - large vector (SIMD path)" {
    var a: [768]f32 = undefined;
    var b: [768]f32 = undefined;

    // Same direction, different magnitudes
    for (0..768) |i| {
        const val: f32 = @floatFromInt(i + 1);
        a[i] = val;
        b[i] = val * 2.0;
    }

    const dist = cosineDistance(&a, &b);
    try testing.expectApproxEqAbs(@as(f32, 0.0), dist, 0.0001);
}

test "inner product distance - known values" {
    const a = [_]f32{ 1.0, 2.0, 3.0 };
    const b = [_]f32{ 4.0, 5.0, 6.0 };

    // dot = 1*4 + 2*5 + 3*6 = 4 + 10 + 18 = 32
    // inner product distance = -32
    const dist = innerProductDistance(&a, &b);
    try testing.expectApproxEqAbs(@as(f32, -32.0), dist, 0.0001);
}

test "inner product distance - large vector (SIMD path)" {
    var a: [512]f32 = undefined;
    var b: [512]f32 = undefined;

    for (0..512) |i| {
        a[i] = 1.0;
        b[i] = 2.0;
    }

    // dot = 512 * (1 * 2) = 1024
    const dist = innerProductDistance(&a, &b);
    try testing.expectApproxEqAbs(@as(f32, -1024.0), dist, 0.0001);
}

test "dot product" {
    const a = [_]f32{ 1.0, 2.0, 3.0 };
    const b = [_]f32{ 4.0, 5.0, 6.0 };

    const dot = dotProduct(&a, &b);
    try testing.expectApproxEqAbs(@as(f32, 32.0), dot, 0.0001);
}

test "norm" {
    const a = [_]f32{ 3.0, 4.0 };

    // sqrt(9 + 16) = sqrt(25) = 5
    const n = norm(&a);
    try testing.expectApproxEqAbs(@as(f32, 5.0), n, 0.0001);
}

test "normalize" {
    var a = [_]f32{ 3.0, 4.0 };

    normalize(&a);

    // Should be unit vector: 3/5, 4/5
    try testing.expectApproxEqAbs(@as(f32, 0.6), a[0], 0.0001);
    try testing.expectApproxEqAbs(@as(f32, 0.8), a[1], 0.0001);

    // Norm should be 1
    try testing.expectApproxEqAbs(@as(f32, 1.0), norm(&a), 0.0001);
}

test "normalize - large vector (SIMD path)" {
    var a: [256]f32 = undefined;

    for (0..256) |i| {
        a[i] = @floatFromInt(i + 1);
    }

    normalize(&a);

    // Norm should be 1
    try testing.expectApproxEqAbs(@as(f32, 1.0), norm(&a), 0.0001);
}

test "simd remainder handling" {
    // Test vectors that don't divide evenly by SIMD_WIDTH (8)
    // 13 elements = 8 SIMD + 5 scalar
    const a = [_]f32{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13 };
    const b = [_]f32{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13 };

    const dist = euclideanDistance(&a, &b);
    try testing.expectApproxEqAbs(@as(f32, 0.0), dist, 0.0001);
}

test "empty vectors" {
    const a = [_]f32{};
    const b = [_]f32{};

    try testing.expectApproxEqAbs(@as(f32, 0.0), euclideanDistance(&a, &b), 0.0001);
    try testing.expectApproxEqAbs(@as(f32, 0.0), dotProduct(&a, &b), 0.0001);
}

test "dotProductBatch4 matches individual calls" {
    var query: [128]f32 = undefined;
    var v0: [128]f32 = undefined;
    var v1: [128]f32 = undefined;
    var v2: [128]f32 = undefined;
    var v3: [128]f32 = undefined;

    for (0..128) |i| {
        const fi: f32 = @floatFromInt(i);
        query[i] = fi * 0.1;
        v0[i] = fi * 0.2 + 1.0;
        v1[i] = fi * 0.3 - 0.5;
        v2[i] = fi * 0.05 + 2.0;
        v3[i] = fi * 0.15 - 1.0;
    }

    const batch = dotProductBatch4(&query, &v0, &v1, &v2, &v3);
    try testing.expectApproxEqAbs(dotProduct(&query, &v0), batch[0], 0.01);
    try testing.expectApproxEqAbs(dotProduct(&query, &v1), batch[1], 0.01);
    try testing.expectApproxEqAbs(dotProduct(&query, &v2), batch[2], 0.01);
    try testing.expectApproxEqAbs(dotProduct(&query, &v3), batch[3], 0.01);
}

test "dotProductBatch4 non-simd-aligned length" {
    // 13 elements: 8 SIMD + 5 scalar
    var query = [_]f32{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13 };
    var v0 = [_]f32{ 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1 };
    var v1 = [_]f32{ 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 };
    var v2 = [_]f32{ 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2 };
    var v3 = [_]f32{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };

    const batch = dotProductBatch4(&query, &v0, &v1, &v2, &v3);
    try testing.expectApproxEqAbs(dotProduct(&query, &v0), batch[0], 0.0001);
    try testing.expectApproxEqAbs(dotProduct(&query, &v1), batch[1], 0.0001);
    try testing.expectApproxEqAbs(dotProduct(&query, &v2), batch[2], 0.0001);
    try testing.expectApproxEqAbs(dotProduct(&query, &v3), batch[3], 0.0001);
}

test "euclideanDistanceSquaredBatch4 matches individual calls" {
    var query: [128]f32 = undefined;
    var v0: [128]f32 = undefined;
    var v1: [128]f32 = undefined;
    var v2: [128]f32 = undefined;
    var v3: [128]f32 = undefined;

    for (0..128) |i| {
        const fi: f32 = @floatFromInt(i);
        query[i] = fi * 0.1;
        v0[i] = fi * 0.2 + 1.0;
        v1[i] = fi * 0.3 - 0.5;
        v2[i] = fi * 0.05 + 2.0;
        v3[i] = fi * 0.15 - 1.0;
    }

    const batch = euclideanDistanceSquaredBatch4(&query, &v0, &v1, &v2, &v3);
    try testing.expectApproxEqAbs(euclideanDistanceSquared(&query, &v0), batch[0], 0.1);
    try testing.expectApproxEqAbs(euclideanDistanceSquared(&query, &v1), batch[1], 0.1);
    try testing.expectApproxEqAbs(euclideanDistanceSquared(&query, &v2), batch[2], 0.1);
    try testing.expectApproxEqAbs(euclideanDistanceSquared(&query, &v3), batch[3], 0.1);
}

test "innerProductDistanceBatch4 matches individual calls" {
    var query: [64]f32 = undefined;
    var v0: [64]f32 = undefined;
    var v1: [64]f32 = undefined;
    var v2: [64]f32 = undefined;
    var v3: [64]f32 = undefined;

    for (0..64) |i| {
        const fi: f32 = @floatFromInt(i);
        query[i] = fi * 0.1;
        v0[i] = fi * 0.2 + 1.0;
        v1[i] = fi * 0.3 - 0.5;
        v2[i] = fi * 0.05 + 2.0;
        v3[i] = fi * 0.15 - 1.0;
    }

    const batch = innerProductDistanceBatch4(&query, &v0, &v1, &v2, &v3);
    try testing.expectApproxEqAbs(innerProductDistance(&query, &v0), batch[0], 0.01);
    try testing.expectApproxEqAbs(innerProductDistance(&query, &v1), batch[1], 0.01);
    try testing.expectApproxEqAbs(innerProductDistance(&query, &v2), batch[2], 0.01);
    try testing.expectApproxEqAbs(innerProductDistance(&query, &v3), batch[3], 0.01);
}
