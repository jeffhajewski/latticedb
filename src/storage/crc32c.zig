//! CRC-32C (Castagnoli), bit-identical to `std.hash.crc.Crc32Iscsi`.
//!
//! Every page read, every page write, every WAL frame and every overflow
//! value is checksummed, so this sits on the latency path of almost every
//! storage operation. The standard library's implementation walks one byte
//! at a time through a single 256-entry table, with each step depending on
//! the last, which costs several microseconds per 4 KB page.
//!
//! This module uses the CPU's CRC-32C instruction where there is one (SSE4.2
//! `crc32` on x86_64, the ARMv8 CRC extension on aarch64) and a slicing-by-8
//! table walk everywhere else. Release binaries are built for baseline CPUs
//! (x86_64 without SSE4.2, generic aarch64 without the CRC extension), so the
//! instruction is chosen at run time unless the build target already
//! guarantees it.
//!
//! The polynomial, initial value, reflection and final xor are those of
//! Crc32Iscsi, so checksums written by older builds verify here and the
//! reverse; the tests compare against the byte-wise reference directly.

const std = @import("std");
const builtin = @import("builtin");

/// Reflected Castagnoli polynomial.
const POLY: u32 = 0x82F63B78;

/// Compute the CRC-32C of `data` in one call.
pub fn hash(data: []const u8) u32 {
    return ~update(0xFFFF_FFFF, data);
}

/// Advance a raw (pre-inverted) CRC state over `data`. `hash` is
/// `~update(0xFFFFFFFF, data)`.
pub fn update(crc: u32, data: []const u8) u32 {
    return switch (comptime hardwareSupport()) {
        .always => updateHardware(crc, data),
        .never => updateSoftware(crc, data),
        .runtime => if (hardwareAvailable()) updateHardware(crc, data) else updateSoftware(crc, data),
    };
}

// ============================================================================
// Software: slicing-by-8
// ============================================================================

const tables: [8][256]u32 = blk: {
    @setEvalBranchQuota(20_000);
    var t: [8][256]u32 = undefined;
    for (0..256) |i| {
        var c: u32 = @intCast(i);
        for (0..8) |_| c = (c >> 1) ^ (if (c & 1 != 0) POLY else 0);
        t[0][i] = c;
    }
    for (0..256) |i| {
        for (1..8) |k| {
            const prev = t[k - 1][i];
            t[k][i] = (prev >> 8) ^ t[0][prev & 0xFF];
        }
    }
    break :blk t;
};

/// Portable fallback, processing eight bytes per step through eight tables.
pub fn updateSoftware(crc_in: u32, data: []const u8) u32 {
    var crc = crc_in;
    var i: usize = 0;
    while (i + 8 <= data.len) : (i += 8) {
        const lo = std.mem.readInt(u32, data[i..][0..4], .little) ^ crc;
        const hi = std.mem.readInt(u32, data[i + 4 ..][0..4], .little);
        crc = tables[7][lo & 0xFF] ^
            tables[6][(lo >> 8) & 0xFF] ^
            tables[5][(lo >> 16) & 0xFF] ^
            tables[4][lo >> 24] ^
            tables[3][hi & 0xFF] ^
            tables[2][(hi >> 8) & 0xFF] ^
            tables[1][(hi >> 16) & 0xFF] ^
            tables[0][hi >> 24];
    }
    while (i < data.len) : (i += 1) {
        crc = (crc >> 8) ^ tables[0][(crc ^ data[i]) & 0xFF];
    }
    return crc;
}

// ============================================================================
// Hardware
// ============================================================================

const HardwareSupport = enum { always, never, runtime };

fn hardwareSupport() HardwareSupport {
    // Only LLVM assembles the CRC instructions; the self-hosted backends used
    // for some Debug builds do not.
    if (builtin.zig_backend != .stage2_llvm) return .never;
    return switch (builtin.cpu.arch) {
        .x86_64 => if (std.Target.x86.featureSetHas(builtin.cpu.features, .sse4_2)) .always else .runtime,
        .aarch64 => if (std.Target.aarch64.featureSetHas(builtin.cpu.features, .crc))
            .always
        else switch (builtin.os.tag) {
            .linux, .windows => .runtime,
            else => .never,
        },
        else => .never,
    };
}

/// 0 = not yet probed, 1 = absent, 2 = present. Probing is idempotent, so a
/// race between first callers only repeats the probe.
var hw_state: std.atomic.Value(u8) = .init(0);

fn hardwareAvailable() bool {
    const state = hw_state.load(.monotonic);
    if (state != 0) return state == 2;
    const present = probeHardware();
    hw_state.store(if (present) 2 else 1, .monotonic);
    return present;
}

fn probeHardware() bool {
    switch (builtin.cpu.arch) {
        .x86_64 => {
            // CPUID leaf 1, ECX bit 20 = SSE4.2.
            var eax: u32 = 1;
            var ebx: u32 = undefined;
            var ecx: u32 = 0;
            var edx: u32 = undefined;
            asm volatile ("cpuid"
                : [eax] "={eax}" (eax),
                  [ebx] "={ebx}" (ebx),
                  [ecx] "={ecx}" (ecx),
                  [edx] "={edx}" (edx),
                : [in_eax] "{eax}" (eax),
                  [in_ecx] "{ecx}" (ecx),
            );
            return (ecx >> 20) & 1 != 0;
        },
        .aarch64 => switch (builtin.os.tag) {
            .linux => {
                const HWCAP_CRC32: usize = 1 << 7;
                return std.os.linux.getauxval(std.elf.AT_HWCAP) & HWCAP_CRC32 != 0;
            },
            .windows => return std.os.windows.IsProcessorFeaturePresent(.ARM_V8_CRC32_INSTRUCTIONS_AVAILABLE),
            else => return false,
        },
        else => return false,
    }
}

/// Only called when the instruction is known to exist.
fn updateHardware(crc_in: u32, data: []const u8) u32 {
    var crc = crc_in;
    var i: usize = 0;
    while (i + 8 <= data.len) : (i += 8) {
        crc = step64(crc, std.mem.readInt(u64, data[i..][0..8], .little));
    }
    while (i < data.len) : (i += 1) {
        crc = step8(crc, data[i]);
    }
    return crc;
}

inline fn step64(crc: u32, v: u64) u32 {
    switch (builtin.cpu.arch) {
        .x86_64 => {
            var c: u64 = crc;
            asm ("crc32q %[v], %[c]"
                : [c] "+r" (c),
                : [v] "r" (v),
            );
            return @truncate(c);
        },
        .aarch64 => return asm (
            \\.arch_extension crc
            \\crc32cx %[out:w], %[c:w], %[v:x]
            : [out] "=r" (-> u32),
            : [c] "r" (crc),
              [v] "r" (v),
        ),
        else => unreachable,
    }
}

inline fn step8(crc: u32, b: u8) u32 {
    switch (builtin.cpu.arch) {
        .x86_64 => {
            var c: u32 = crc;
            asm ("crc32b %[v], %[c]"
                : [c] "+r" (c),
                : [v] "r" (b),
            );
            return c;
        },
        .aarch64 => return asm (
            \\.arch_extension crc
            \\crc32cb %[out:w], %[c:w], %[v:w]
            : [out] "=r" (-> u32),
            : [c] "r" (crc),
              [v] "r" (@as(u32, b)),
        ),
        else => unreachable,
    }
}

// ============================================================================
// Tests
// ============================================================================

/// The routine every release up to 0.16.0 used, kept as the reference.
fn referenceBytewise(data: []const u8) u32 {
    return std.hash.crc.Crc32Iscsi.hash(data);
}

test "crc32c known vectors" {
    try std.testing.expectEqual(@as(u32, 0x00000000), hash(""));
    try std.testing.expectEqual(@as(u32, 0xE3069283), hash("123456789"));
    // RFC 3720 B.4: 32 bytes of zeros / ones / incrementing / decrementing.
    var buf: [32]u8 = undefined;
    @memset(&buf, 0);
    try std.testing.expectEqual(@as(u32, 0x8A9136AA), hash(&buf));
    @memset(&buf, 0xFF);
    try std.testing.expectEqual(@as(u32, 0x62A8AB43), hash(&buf));
    for (&buf, 0..) |*b, i| b.* = @intCast(i);
    try std.testing.expectEqual(@as(u32, 0x46DD794E), hash(&buf));
    for (&buf, 0..) |*b, i| b.* = @intCast(31 - i);
    try std.testing.expectEqual(@as(u32, 0x113FDB5C), hash(&buf));
}

test "crc32c matches the byte-wise reference on every length, alignment and path" {
    var prng = std.Random.DefaultPrng.init(0x5eed_c2c3);
    const random = prng.random();
    var buf: [9000]u8 = undefined;
    random.bytes(&buf);

    const has_hw = switch (comptime hardwareSupport()) {
        .always => true,
        .never => false,
        .runtime => hardwareAvailable(),
    };

    // Every length up to 300 at every alignment 0..7 covers all head/tail
    // shapes of both the 8-byte loops.
    for (0..8) |offset| {
        for (0..300) |len| {
            const data = buf[offset..][0..len];
            const expected = referenceBytewise(data);
            try std.testing.expectEqual(expected, hash(data));
            try std.testing.expectEqual(expected, ~updateSoftware(0xFFFF_FFFF, data));
            if (has_hw) try std.testing.expectEqual(expected, ~updateHardware(0xFFFF_FFFF, data));
        }
    }

    // Page-shaped inputs, including the `bytes 8..end` slice the page manager
    // checksums, and random lengths.
    for ([_]usize{ 4088, 4096, 8184, 8192, 1, 7, 8, 9, 63, 64, 65 }) |len| {
        try std.testing.expectEqual(referenceBytewise(buf[0..len]), hash(buf[0..len]));
        try std.testing.expectEqual(referenceBytewise(buf[3..][0..len]), hash(buf[3..][0..len]));
    }
    for (0..200) |_| {
        const start = random.uintLessThan(usize, 512);
        const len = random.uintLessThan(usize, buf.len - start);
        const data = buf[start..][0..len];
        try std.testing.expectEqual(referenceBytewise(data), hash(data));
    }

    // Edge-case contents: all zeros, all ones.
    var zeros = [_]u8{0} ** 4096;
    try std.testing.expectEqual(referenceBytewise(&zeros), hash(&zeros));
    try std.testing.expectEqual(referenceBytewise(zeros[8..]), hash(zeros[8..]));
    var ones = [_]u8{0xFF} ** 4096;
    try std.testing.expectEqual(referenceBytewise(&ones), hash(&ones));
}

test "crc32c incremental update equals one-shot hash" {
    var prng = std.Random.DefaultPrng.init(42);
    var buf: [5000]u8 = undefined;
    prng.random().bytes(&buf);
    for ([_]usize{ 0, 1, 7, 8, 13, 4095, 4096 }) |split| {
        const crc = update(update(0xFFFF_FFFF, buf[0..split]), buf[split..]);
        try std.testing.expectEqual(referenceBytewise(&buf), ~crc);
    }
}
