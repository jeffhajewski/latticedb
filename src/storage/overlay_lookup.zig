//! Where a transaction's staged nodes and edges are, by indexed value.
//!
//! An index lookup inside a transaction has to see the entities the
//! transaction changed, which the committed index does not hold yet. Walking
//! every staged entity for each lookup made a load that looks up by an
//! indexed key as it goes (MERGE, imports) quadratic in its batch. This keeps,
//! per declaration a lookup has asked about, the ids staged under each value.
//!
//! It is filled lazily: nothing is kept until the transaction's first lookup
//! for a declaration, which notes every entity staged so far; after that,
//! each staged write notes the entity under its new value. Entries are never
//! taken out when a value changes or an entity is deleted. A lookup gets the
//! ids noted under the value it asks for, possibly stale or repeated, and has
//! to check each against the staged state, as it already did.

const std = @import("std");
const Allocator = std.mem.Allocator;

const types = @import("../core/types.zig");
const PropertyValue = types.PropertyValue;
const property_index = @import("../graph/property_index.zig");
const Definition = property_index.Definition;

pub const OverlayLookup = struct {
    /// The declarations noted so far, with an entry for every staged
    /// entity in scope since.
    covered: std.ArrayListUnmanaged(Definition) = .empty,
    buckets: std.AutoHashMapUnmanaged(u64, std.ArrayListUnmanaged(u64)) = .empty,

    pub fn deinit(self: *OverlayLookup, allocator: Allocator) void {
        var iter = self.buckets.valueIterator();
        while (iter.next()) |ids| ids.deinit(allocator);
        self.buckets.deinit(allocator);
        self.covered.deinit(allocator);
        self.* = .{};
    }

    pub fn covers(self: *const OverlayLookup, definition: Definition) bool {
        for (self.covered.items) |d| {
            if (sameDeclaration(d, definition)) return true;
        }
        return false;
    }

    /// Start keeping `definition`. The caller then notes every entity
    /// already staged.
    pub fn cover(self: *OverlayLookup, allocator: Allocator, definition: Definition) Allocator.Error!void {
        try self.covered.append(allocator, definition);
    }

    /// Record that `id` is staged with `value` under `definition`.
    pub fn note(self: *OverlayLookup, allocator: Allocator, definition: Definition, id: u64, value: PropertyValue) Allocator.Error!void {
        const key = bucketKey(definition, value) orelse return;
        const entry = try self.buckets.getOrPut(allocator, key);
        if (!entry.found_existing) entry.value_ptr.* = .empty;
        const ids = entry.value_ptr;
        // The same entity written again with the same value, the common
        // case of several properties set one after another.
        if (ids.items.len > 0 and ids.items[ids.items.len - 1] == id) return;
        try ids.append(allocator, id);
    }

    /// The ids noted under `value`: a superset of the staged entities whose
    /// value equals it, to be checked one by one.
    pub fn candidates(self: *const OverlayLookup, definition: Definition, value: PropertyValue) []const u64 {
        const key = bucketKey(definition, value) orelse return &.{};
        const ids = self.buckets.getPtr(key) orelse return &.{};
        return ids.items;
    }

    fn sameDeclaration(a: Definition, b: Definition) bool {
        return a.kind == b.kind and a.scope_id == b.scope_id and a.property_id == b.property_id;
    }

    /// Values that compare equal share a bucket: the ordered encoding gives
    /// 3 and 3.0 the same bytes, and lists, maps and vectors share one per
    /// declaration. Null is never indexed.
    fn bucketKey(definition: Definition, value: PropertyValue) ?u64 {
        const encoded = property_index.encodeOrderedValue(value) orelse return null;
        var hasher = std.hash.Wyhash.init(0);
        hasher.update(&.{@intFromEnum(definition.kind)});
        hasher.update(std.mem.asBytes(&definition.scope_id));
        hasher.update(std.mem.asBytes(&definition.property_id));
        hasher.update(encoded.orderGroup());
        return hasher.final();
    }
};

test "overlay lookup buckets equal numbers together and keeps declarations apart" {
    const allocator = std.testing.allocator;
    var lookup = OverlayLookup{};
    defer lookup.deinit(allocator);
    const a = Definition{ .kind = .node, .scope_id = 1000, .property_id = 1001 };
    const b = Definition{ .kind = .node, .scope_id = 1000, .property_id = 1002 };
    try lookup.cover(allocator, a);
    try std.testing.expect(lookup.covers(a) and !lookup.covers(b));
    try lookup.note(allocator, a, 7, .{ .int_val = 3 });
    try lookup.note(allocator, a, 7, .{ .int_val = 3 });
    try lookup.note(allocator, a, 8, .{ .float_val = 3.0 });
    try lookup.note(allocator, b, 9, .{ .int_val = 3 });
    try std.testing.expectEqualSlices(u64, &.{ 7, 8 }, lookup.candidates(a, .{ .int_val = 3 }));
    try std.testing.expectEqual(@as(usize, 0), lookup.candidates(a, .{ .int_val = 4 }).len);
    try std.testing.expectEqual(@as(usize, 0), lookup.candidates(a, .{ .null_val = {} }).len);
}
