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
