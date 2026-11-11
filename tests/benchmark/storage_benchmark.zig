//! Storage-core micro-benchmarks: checksums, page allocation and the B+Tree.
//!
//! These drive the storage layer directly, without the graph or query layers
//! on top, so a change to the page manager, buffer pool or B+Tree shows up
//! undiluted. Each line reports nanoseconds per operation.
//!
//! Run with: zig build storage-benchmark

const std = @import("std");
const lattice = @import("lattice");
const compat = @import("compat");

const page = lattice.storage.page;
const vfs = lattice.storage.vfs;
const PageManager = lattice.storage.page_manager.PageManager;
const BufferPool = lattice.storage.buffer_pool.BufferPool;
const BTree = lattice.storage.btree.BTree;

const allocator = std.heap.c_allocator;

const TREE_KEYS: usize = 200_000;
const VALUE = "v" ** 32;

fn now() i128 {
    return compat.nanoTimestamp();
}

fn report(name: []const u8, ns: i128, ops: usize) void {
    const per_op = @as(f64, @floatFromInt(ns)) / @as(f64, @floatFromInt(ops));
    std.debug.print("{s:<44} {d:>10.1} ns/op  ({d} ops)\n", .{ name, per_op, ops });
}

const Store = struct {
    posix_vfs: vfs.PosixVfs,
    pm: *PageManager,
    bp: *BufferPool,
    path: []const u8,

    fn open(path: []const u8, pool_bytes: usize) !Store {
        var posix_vfs = vfs.PosixVfs.init(allocator);
        const v = posix_vfs.vfs();
        v.delete(path) catch {};
        const pm = try allocator.create(PageManager);
        pm.* = try PageManager.init(allocator, v, path, .{ .create = true });
        const bp = try allocator.create(BufferPool);
        bp.* = try BufferPool.init(allocator, pm, pool_bytes);
        return .{ .posix_vfs = posix_vfs, .pm = pm, .bp = bp, .path = path };
    }

    fn close(self: *Store) void {
        self.bp.deinit();
        self.pm.deinit();
        allocator.destroy(self.bp);
        allocator.destroy(self.pm);
        self.posix_vfs.vfs().delete(self.path) catch {};
    }
};

fn leKey(buf: *[8]u8, id: u64) []const u8 {
    std.mem.writeInt(u64, buf, id, .little);
    return buf;
}

fn beKey(buf: *[8]u8, id: u64) []const u8 {
    std.mem.writeInt(u64, buf, id, .big);
    return buf;
}

fn shuffled(n: usize, seed: u64) ![]u64 {
    const ids = try allocator.alloc(u64, n);
    for (ids, 0..) |*id, i| id.* = i + 1;
    var prng = std.Random.DefaultPrng.init(seed);
    prng.random().shuffle(u64, ids);
    return ids;
}

fn benchCrc() void {
    var buf: [4096]u8 = undefined;
    var prng = std.Random.DefaultPrng.init(1);
    prng.random().bytes(&buf);
    const iters: usize = 200_000;
    var acc: u32 = 0;
    const start = now();
    for (0..iters) |i| {
        buf[8] = @truncate(i);
        acc ^= page.calculateChecksum(buf[8..]);
    }
    const elapsed = now() - start;
    std.mem.doNotOptimizeAway(acc);
    report("crc32c of one 4 KB page", elapsed, iters);

    // The portable fallback and the routine 0.16.0 used, for comparison.
    if (@hasDecl(page, "crc32c")) {
        const start_sw = now();
        for (0..iters) |i| {
            buf[8] = @truncate(i);
            acc ^= ~page.crc32c.updateSoftware(0xFFFF_FFFF, buf[8..]);
        }
        std.mem.doNotOptimizeAway(acc);
        report("crc32c 4 KB, slicing-by-8 fallback", now() - start_sw, iters);
    }
    const ref_iters = iters / 10;
    const start_ref = now();
    for (0..ref_iters) |i| {
        buf[8] = @truncate(i);
        acc ^= std.hash.crc.Crc32Iscsi.hash(buf[8..]);
    }
    std.mem.doNotOptimizeAway(acc);
    report("crc32c 4 KB, byte-wise (0.16.0)", now() - start_ref, ref_iters);
}

fn benchPageAlloc() !void {
    var store = try Store.open("/tmp/lattice_storage_bench_alloc.db", 4 * 1024 * 1024);
    defer store.close();
    const n: usize = 5_000;
    const ids = try allocator.alloc(u32, n);
    defer allocator.free(ids);

    var start = now();
    for (ids) |*id| id.* = try store.pm.allocatePage();
    report("page allocate (extend file)", now() - start, n);

    start = now();
    for (ids) |id| try store.pm.freePage(id);
    report("page free", now() - start, n);

    start = now();
    for (ids) |*id| id.* = try store.pm.allocatePage();
    report("page allocate (from freelist)", now() - start, n);
}

fn benchTree() !void {
    var store = try Store.open("/tmp/lattice_storage_bench_tree.db", 256 * 1024 * 1024);
    defer store.close();
    var tree = try BTree.init(allocator, store.bp);

    const ids = try shuffled(TREE_KEYS, 7);
    defer allocator.free(ids);
    var key_buf: [8]u8 = undefined;

    var start = now();
    for (ids) |id| try tree.insert(leKey(&key_buf, id), VALUE);
    report("btree insert, random u64 keys (200k)", now() - start, ids.len);

    const lookups = try shuffled(TREE_KEYS, 11);
    defer allocator.free(lookups);

    start = now();
    for (0..5) |_| {
        for (lookups) |id| {
            const v = (try tree.get(leKey(&key_buf, id))).?;
            tree.freeValue(v);
        }
    }
    report("btree get hit, warm pool", now() - start, lookups.len * 5);

    start = now();
    var found: usize = 0;
    for (0..5) |_| {
        for (lookups) |id| {
            if (try tree.contains(leKey(&key_buf, id))) found += 1;
        }
    }
    std.mem.doNotOptimizeAway(found);
    report("btree contains, warm pool", now() - start, lookups.len * 5);

    const updates = lookups[0 .. TREE_KEYS / 2];
    start = now();
    for (updates) |id| {
        const k = leKey(&key_buf, id);
        try tree.delete(k);
        try tree.insert(k, VALUE);
    }
    report("btree update (delete + insert)", now() - start, updates.len);

    start = now();
    for (lookups) |id| try tree.delete(leKey(&key_buf, id));
    report("btree delete, random (200k)", now() - start, lookups.len);
}

fn benchSequential() !void {
    var store = try Store.open("/tmp/lattice_storage_bench_seq.db", 256 * 1024 * 1024);
    defer store.close();
    var tree = try BTree.init(allocator, store.bp);
    var key_buf: [8]u8 = undefined;
    const start = now();
    for (1..TREE_KEYS + 1) |id| try tree.insert(beKey(&key_buf, id), VALUE);
    report("btree insert, sequential BE keys (200k)", now() - start, TREE_KEYS);
}

fn benchStringKeys() !void {
    var store = try Store.open("/tmp/lattice_storage_bench_str.db", 256 * 1024 * 1024);
    defer store.close();
    var tree = try BTree.init(allocator, store.bp);
    const n: usize = 50_000;
    var buf: [64]u8 = undefined;
    for (0..n) |i| {
        const k = try std.fmt.bufPrint(&buf, "property_name_{d}_suffix", .{i *% 2654435761 % 1_000_003});
        tree.insert(k, "\x01\x02") catch |err| switch (err) {
            error.DuplicateKey => {},
            else => return err,
        };
    }
    const start = now();
    for (0..4) |_| {
        for (0..n) |i| {
            const k = try std.fmt.bufPrint(&buf, "property_name_{d}_suffix", .{i *% 2654435761 % 1_000_003});
            if (try tree.get(k)) |v| tree.freeValue(v);
        }
    }
    report("btree get, ~30-byte string keys (50k)", now() - start, n * 4);
}

fn benchCold() !void {
    // A pool of 128 frames over a tree of ~1500 pages: most lookups miss and
    // read from the file, and evictions write dirty frames.
    var store = try Store.open("/tmp/lattice_storage_bench_cold.db", 512 * 1024);
    defer store.close();
    var tree = try BTree.init(allocator, store.bp);
    const ids = try shuffled(TREE_KEYS / 2, 3);
    defer allocator.free(ids);
    var key_buf: [8]u8 = undefined;

    var start = now();
    for (ids) |id| try tree.insert(leKey(&key_buf, id), VALUE);
    report("btree insert, pool << tree (100k)", now() - start, ids.len);
    _ = try store.bp.flushAll();

    start = now();
    for (ids) |id| {
        const v = (try tree.get(leKey(&key_buf, id))).?;
        tree.freeValue(v);
    }
    report("btree get, pool << tree (100k)", now() - start, ids.len);
}

pub fn main() !void {
    std.debug.print("\nLatticeDB storage-core micro-benchmarks\n", .{});
    std.debug.print("---------------------------------------\n", .{});
    benchCrc();
    try benchPageAlloc();
    try benchTree();
    try benchSequential();
    try benchStringKeys();
    try benchCold();
}
