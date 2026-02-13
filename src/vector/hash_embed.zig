//! Hash-based Text Embedding
//!
//! Zero-dependency feature-hashing function that converts text to fixed-dimension
//! vectors. Uses the FTS tokenizer + Wyhash + sign hashing + L2 normalization.
//!
//! Useful for lightweight text similarity without external embedding services.

const std = @import("std");
const Allocator = std.mem.Allocator;
const Wyhash = std.hash.Wyhash;
const tokenizer_mod = @import("../fts/tokenizer.zig");
const Tokenizer = tokenizer_mod.Tokenizer;
const TokenizerConfig = tokenizer_mod.TokenizerConfig;
const distance = @import("distance.zig");

pub const HashEmbedConfig = struct {
    dimensions: u16 = 128,
    tokenizer: TokenizerConfig = .{},
};

pub const HashEmbedError = error{ OutOfMemory, EmptyInput, InvalidDimensions };

/// Generate a hash embedding vector from text.
/// Caller owns the returned slice and must free it with the same allocator.
pub fn hashEmbed(allocator: Allocator, text: []const u8, config: HashEmbedConfig) HashEmbedError![]f32 {
    if (text.len == 0) return HashEmbedError.EmptyInput;
    if (config.dimensions == 0) return HashEmbedError.InvalidDimensions;

    const dims: usize = config.dimensions;
    const vector = allocator.alloc(f32, dims) catch return HashEmbedError.OutOfMemory;
    errdefer allocator.free(vector);
    @memset(vector, 0.0);

    var tok = Tokenizer.init(allocator, text, config.tokenizer);
    var token_count: usize = 0;
    var lower_buf: [64]u8 = undefined;

    while (tok.next()) |token| {
        // Lowercase the token text for consistent hashing
        // (Tokenizer returns original case; it only lowercases internally for stop word checks)
        const token_text = if (config.tokenizer.lowercase and token.text.len <= lower_buf.len) blk: {
            for (token.text, 0..) |c, i| {
                lower_buf[i] = std.ascii.toLower(c);
            }
            break :blk lower_buf[0..token.text.len];
        } else token.text;

        const hash = Wyhash.hash(0, token_text);
        const bucket = hash % dims;
        const sign: f32 = if ((hash >> 63) == 0) 1.0 else -1.0;
        vector[bucket] += sign;
        token_count += 1;
    }

    if (token_count > 0) {
        distance.normalize(vector);
    }

    return vector;
}

// ============================================================================
// Tests
// ============================================================================

const testing = std.testing;

test "hash embed basic" {
    const allocator = testing.allocator;
    const vector = try hashEmbed(allocator, "machine learning algorithms", .{});
    defer allocator.free(vector);

    try testing.expectEqual(@as(usize, 128), vector.len);

    // Should be normalized (unit length) since we have tokens
    const n = distance.norm(vector);
    try testing.expectApproxEqAbs(@as(f32, 1.0), n, 0.001);
}

test "hash embed deterministic" {
    const allocator = testing.allocator;
    const v1 = try hashEmbed(allocator, "hello world test", .{});
    defer allocator.free(v1);
    const v2 = try hashEmbed(allocator, "hello world test", .{});
    defer allocator.free(v2);

    for (v1, v2) |a, b| {
        try testing.expectEqual(a, b);
    }
}

test "hash embed normalized" {
    const allocator = testing.allocator;
    const vector = try hashEmbed(allocator, "the quick brown fox jumps over the lazy dog", .{});
    defer allocator.free(vector);

    const n = distance.norm(vector);
    try testing.expectApproxEqAbs(@as(f32, 1.0), n, 0.001);
}

test "hash embed empty input" {
    const allocator = testing.allocator;
    const result = hashEmbed(allocator, "", .{});
    try testing.expectError(HashEmbedError.EmptyInput, result);
}

test "hash embed invalid dimensions" {
    const allocator = testing.allocator;
    const result = hashEmbed(allocator, "test", .{ .dimensions = 0 });
    try testing.expectError(HashEmbedError.InvalidDimensions, result);
}

test "hash embed different dimensions" {
    const allocator = testing.allocator;

    const v64 = try hashEmbed(allocator, "machine learning", .{ .dimensions = 64 });
    defer allocator.free(v64);
    try testing.expectEqual(@as(usize, 64), v64.len);

    const v256 = try hashEmbed(allocator, "machine learning", .{ .dimensions = 256 });
    defer allocator.free(v256);
    try testing.expectEqual(@as(usize, 256), v256.len);
}

test "hash embed similar texts closer" {
    const allocator = testing.allocator;

    const v_ml1 = try hashEmbed(allocator, "machine learning neural networks deep learning", .{});
    defer allocator.free(v_ml1);
    const v_ml2 = try hashEmbed(allocator, "deep learning neural network training", .{});
    defer allocator.free(v_ml2);
    const v_db = try hashEmbed(allocator, "database storage index query optimization", .{});
    defer allocator.free(v_db);

    // ML texts should be more similar to each other than to DB text
    const dist_ml = distance.cosineDistance(v_ml1, v_ml2);
    const dist_cross = distance.cosineDistance(v_ml1, v_db);
    try testing.expect(dist_ml < dist_cross);
}
