//! How a node, edge or document id is written as a B-tree key.
//!
//! The B-tree compares keys byte by byte. An id written little-endian puts
//! its lowest byte first, so ids handed out in sequence do not land in
//! sequence: 256 sorts between 0 and 1, and a tree filled with ids 1, 2, 3, …
//! takes each new one in a different leaf, half filling all of them. Written
//! big-endian, byte order is numeric order, new ids go on the right edge and
//! the leaves fill.
//!
//! Files created before format 5 keep little-endian id keys: opening one
//! converts nothing, and every read and write on it uses the old order. Only
//! `lattice compact --upgrade-id-keys` (`Database.upgradeIdKeysTo`, in
//! `storage/id_key_upgrade.zig`) converts one, by writing a big-endian copy
//! and putting it in the file's place; that is one-way.
//!
//! A file says which order it uses in its header (`FileFlags.ORDERED_ID_KEYS`).
//! The database reads that once at open and hands the order to every store
//! that keys a tree by id. Nothing else decides it.

const std = @import("std");

/// The byte order of the id keys in one file.
pub const IdKeyOrder = enum {
    /// Before format 5: lowest byte first. Key order is not id order.
    little,
    /// Format 5: highest byte first. Key order is id order.
    big,

    pub const KEY_SIZE = 8;

    /// The key for `id`.
    pub fn encode(self: IdKeyOrder, id: u64) [KEY_SIZE]u8 {
        var key: [KEY_SIZE]u8 = undefined;
        std.mem.writeInt(u64, &key, id, self.endian());
        return key;
    }

    /// The id a key holds. The key must be eight bytes.
    pub fn decode(self: IdKeyOrder, key: *const [KEY_SIZE]u8) u64 {
        return std.mem.readInt(u64, key, self.endian());
    }

    /// Whether the key for `a` sorts before the key for `b` in a tree.
    pub fn keyLessThan(self: IdKeyOrder, a: u64, b: u64) bool {
        return switch (self) {
            .big => a < b,
            .little => @byteSwap(a) < @byteSwap(b),
        };
    }

    /// The name `lattice info` shows.
    pub fn name(self: IdKeyOrder) []const u8 {
        return switch (self) {
            .little => "little-endian",
            .big => "big-endian",
        };
    }

    fn endian(self: IdKeyOrder) std.builtin.Endian {
        return switch (self) {
            .little => .little,
            .big => .big,
        };
    }
};

test "id keys round-trip in both orders" {
    const ids = [_]u64{ 0, 1, 2, 255, 256, 257, 65535, 65536, 1 << 32, std.math.maxInt(u64) - 1, std.math.maxInt(u64) };
    for ([_]IdKeyOrder{ .little, .big }) |order| {
        for (ids) |id| {
            const key = order.encode(id);
            try std.testing.expectEqual(id, order.decode(&key));
        }
    }
}

test "big-endian keys sort in id order, little-endian ones do not" {
    var prng = std.Random.DefaultPrng.init(0x1d5);
    const random = prng.random();
    var little_out_of_order = false;
    for (0..2000) |i| {
        // Sequential neighbours, and random pairs across the whole range.
        const a: u64 = if (i < 1000) i else random.int(u64);
        const b: u64 = if (i < 1000) i + 1 else random.int(u64);
        const big_a = IdKeyOrder.big.encode(a);
        const big_b = IdKeyOrder.big.encode(b);
        try std.testing.expectEqual(a < b, std.mem.order(u8, &big_a, &big_b) == .lt);
        try std.testing.expectEqual(a < b, IdKeyOrder.big.keyLessThan(a, b));

        const little_a = IdKeyOrder.little.encode(a);
        const little_b = IdKeyOrder.little.encode(b);
        // keyLessThan describes the tree's order exactly, in either layout.
        try std.testing.expectEqual(std.mem.order(u8, &little_a, &little_b) == .lt, IdKeyOrder.little.keyLessThan(a, b));
        if ((a < b) != (std.mem.order(u8, &little_a, &little_b) == .lt)) little_out_of_order = true;
    }
    try std.testing.expect(little_out_of_order);
    // The example that motivates format 5: 256 sorts between 0 and 1.
    const k0 = IdKeyOrder.little.encode(0);
    const k1 = IdKeyOrder.little.encode(1);
    const k256 = IdKeyOrder.little.encode(256);
    try std.testing.expect(std.mem.order(u8, &k0, &k256) == .lt and std.mem.order(u8, &k256, &k1) == .lt);
}
