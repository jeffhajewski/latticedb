//! Explicit secondary indexes for node and edge properties.
//!
//! Definitions are stored separately from index entries. An entry key is the
//! scope (label or edge type) and property ids, the value in the index's
//! encoding, and the entity ID. The catalog records the encoding per index:
//!
//! - `digest`, the only one before 0.17.0, keys a value by a SHA-256 digest
//!   of its type and bits. Keys are fixed-size, which suits any value, but
//!   they are in no useful order, so such an index answers equality only.
//! - `ordered` keys a value so that byte order is value order
//!   (`encodeOrderedValue`). It answers equality, ranges and prefixes. What
//!   `CREATE` makes for a relationship type from 0.17.0 on.
//! - `ordered_dense`, what `CREATE` makes for a label from 0.17.0 on, is
//!   `ordered` plus an entry under `OrderedTag.absent` for every node with
//!   the label but not the property. Every node with the label is in it, in
//!   the order ORDER BY puts them, so it can also be read in order, either
//!   way, nulls included.
//!
//! Callers verify an entity found by equality against its record, which
//! makes digest collisions and truncated string keys harmless.

const std = @import("std");
const lattice = @import("lattice");

const Allocator = std.mem.Allocator;
const BTree = lattice.storage.btree.BTree;
const BTreeError = lattice.storage.btree.BTreeError;
const PropertyValue = lattice.core.types.PropertyValue;
const Property = lattice.graph.node.Property;
const SymbolId = lattice.graph.symbols.SymbolId;

pub const EntityKind = enum(u8) {
    node = 1,
    edge = 2,
};

pub const PropertyIndexError = error{
    NotFound,
    AlreadyExists,
    InvalidData,
    IoError,
    OutOfMemory,
};

const DEFINITION_KEY_SIZE = 5;
const SCOPE_PREFIX_SIZE = 4;
const ENTRY_PREFIX_SIZE = 36;
const ENTRY_KEY_SIZE = ENTRY_PREFIX_SIZE + 8;
const VALUE_DIGEST_SIZE = 32;

/// How an index's entry keys encode the value. Stored as the catalog entry's
/// value: empty for `digest`, which is what 0.16.0 writes and reads (it never
/// looks at the value), and one byte otherwise.
pub const Encoding = enum(u8) {
    digest = 0,
    ordered = 1,
    ordered_dense = 2,

    /// Keys in value order: the index answers ranges and prefixes.
    pub fn isOrdered(self: Encoding) bool {
        return self != .digest;
    }
};

pub const Definition = struct {
    kind: EntityKind,
    scope_id: SymbolId,
    property_id: SymbolId,
    /// Not part of the definition's identity: the catalog key leaves it out,
    /// and a definition built from names takes it from `resolve`.
    encoding: Encoding = .digest,
};

/// Strings and byte arrays longer than this are keyed by their first
/// `ORDERED_STRING_PREFIX` bytes, marked as cut short.
pub const ORDERED_STRING_PREFIX = 128;
/// The longest value part of an ordered key: tag, every prefix byte escaped,
/// terminator.
pub const ORDERED_VALUE_MAX = 1 + 2 * ORDERED_STRING_PREFIX + 2;
/// The longest ordered entry key.
pub const ORDERED_KEY_MAX = SCOPE_PREFIX_SIZE + ORDERED_VALUE_MAX + 8;

/// The first byte of an ordered value, which orders values of different
/// types the way ORDER BY does: maps, lists and vectors, then byte arrays,
/// strings, booleans, numbers, NaN, and last, where nulls sort, the entities
/// a dense index holds without the property.
pub const OrderedTag = struct {
    /// Lists, maps and vectors: a digest, so equality only.
    pub const other: u8 = 0x10;
    pub const bytes: u8 = 0x20;
    pub const string: u8 = 0x30;
    pub const boolean: u8 = 0x40;
    pub const number: u8 = 0x50;
    pub const nan: u8 = 0x51;
    /// No value: the property is missing (`ordered_dense` only). The tag
    /// alone, which no range a comparison makes reaches.
    pub const absent: u8 = 0xF0;
};

/// A value encoded for an ordered index.
pub const OrderedValue = struct {
    buf: [ORDERED_VALUE_MAX]u8 = undefined,
    len: usize = 0,
    /// A string or byte array longer than `ORDERED_STRING_PREFIX`: every
    /// value sharing the prefix has these same bytes.
    truncated: bool = false,

    pub fn bytes(self: *const OrderedValue) []const u8 {
        return self.buf[0..self.len];
    }

    /// The bytes that tell this value's place in ORDER BY's order apart
    /// from other values' places. The same as `bytes`, except for lists,
    /// maps and vectors, whose digests are in no order: all of them share
    /// the tag.
    pub fn orderGroup(self: *const OrderedValue) []const u8 {
        if (self.buf[0] == OrderedTag.other) return self.buf[0..1];
        return self.bytes();
    }
};

/// Encode a value so that byte order is the order ORDER BY puts values in,
/// and values ORDER BY calls equal encode the same. Null for null, which is
/// not indexed.
///
/// - Numbers: tag 0x50, then the largest double F not above the value, as 8
///   bytes that order like the double (sign bit flipped for positives, every
///   bit flipped for negatives; -0 is written as +0), then the 16-bit
///   difference between the value and F. A float is its own F and has
///   difference 0. An integer has difference 0 up to 2^53 in magnitude, and
///   beyond that at most 2^10 - 1. So 3 and 3.0 encode the same, and an
///   integer and a float compare exactly. NaN is tag 0x51 alone.
/// - Strings (0x30) and byte arrays (0x20): up to the first 128 bytes, each
///   0x00 written as 0x00 0xFF, then 0x00 0x00 when that was all of it or
///   0x00 0x01 when the value is longer.
/// - Booleans: 0x40, then 0 or 1.
/// - Lists, maps and vectors: 0x10, then the digest a `digest` index uses.
pub fn encodeOrderedValue(value: PropertyValue) ?OrderedValue {
    var out = OrderedValue{};
    switch (value) {
        .null_val => return null,
        .bool_val => |b| {
            out.buf[0] = OrderedTag.boolean;
            out.buf[1] = @intFromBool(b);
            out.len = 2;
        },
        .int_val => |i| {
            const split = floorDouble(i);
            writeNumber(&out, split.floor, split.rest);
        },
        .float_val => |f| {
            if (std.math.isNan(f)) {
                out.buf[0] = OrderedTag.nan;
                out.len = 1;
            } else {
                writeNumber(&out, f, 0);
            }
        },
        .string_val => |s| writeEscaped(&out, OrderedTag.string, s),
        .bytes_val => |s| writeEscaped(&out, OrderedTag.bytes, s),
        .vector_val, .list_val, .map_val => {
            out.buf[0] = OrderedTag.other;
            hashPropertyValue(value, out.buf[1..][0..VALUE_DIGEST_SIZE]);
            out.len = 1 + VALUE_DIGEST_SIZE;
        },
    }
    return out;
}

/// The bytes of a string prefix as they begin every ordered key of a string
/// that starts with it, without a terminator. `truncated` when the prefix is
/// longer than the part of a value a key holds.
pub fn encodeOrderedStringPrefix(prefix: []const u8) OrderedValue {
    var out = OrderedValue{};
    out.buf[0] = OrderedTag.string;
    out.len = 1;
    const kept = prefix[0..@min(prefix.len, ORDERED_STRING_PREFIX)];
    for (kept) |byte| appendEscaped(&out, byte);
    out.truncated = prefix.len > ORDERED_STRING_PREFIX;
    return out;
}

fn appendEscaped(out: *OrderedValue, byte: u8) void {
    if (byte == 0) {
        out.buf[out.len] = 0x00;
        out.buf[out.len + 1] = 0xFF;
        out.len += 2;
    } else {
        out.buf[out.len] = byte;
        out.len += 1;
    }
}

fn writeEscaped(out: *OrderedValue, tag: u8, value: []const u8) void {
    out.buf[0] = tag;
    out.len = 1;
    const kept = value[0..@min(value.len, ORDERED_STRING_PREFIX)];
    for (kept) |byte| appendEscaped(out, byte);
    out.truncated = value.len > ORDERED_STRING_PREFIX;
    out.buf[out.len] = 0x00;
    out.buf[out.len + 1] = if (out.truncated) 0x01 else 0x00;
    out.len += 2;
}

fn writeNumber(out: *OrderedValue, floor: f64, rest: u16) void {
    const normalized: f64 = if (floor == 0) 0 else floor;
    const bits: u64 = @bitCast(normalized);
    const ordered = if (bits >> 63 == 1) ~bits else bits | (@as(u64, 1) << 63);
    out.buf[0] = OrderedTag.number;
    std.mem.writeInt(u64, out.buf[1..9], ordered, .big);
    std.mem.writeInt(u16, out.buf[9..11], rest, .big);
    out.len = 11;
}

/// An integer as the largest double not above it plus what is left over.
fn floorDouble(value: i64) struct { floor: f64, rest: u16 } {
    var floor: f64 = @floatFromInt(value);
    // The conversion rounds to nearest, so it can land above the integer.
    // Doubles this large are whole numbers, and 2^63 still fits an i128.
    var whole: i128 = @intFromFloat(floor);
    if (whole > value) {
        floor = std.math.nextAfter(f64, floor, -std.math.inf(f64));
        whole = @intFromFloat(floor);
    }
    return .{ .floor = floor, .rest = @intCast(@as(i128, value) - whole) };
}

/// One side of a range over an ordered index, in value space: the bytes an
/// entry key has after its scope and property ids.
pub const ValueRange = struct {
    start: [ORDERED_VALUE_MAX + 1]u8 = undefined,
    start_len: usize = 0,
    end: [ORDERED_VALUE_MAX + 1]u8 = undefined,
    end_len: usize = 0,
    /// The range holds no key.
    empty: bool = false,
    /// Every entry in the range satisfies the conditions. False when a bound
    /// was a string cut short: the entries are then a superset to be checked.
    exact: bool = true,

    pub fn startBytes(self: *const ValueRange) []const u8 {
        return self.start[0..self.start_len];
    }

    pub fn endBytes(self: *const ValueRange) []const u8 {
        return self.end[0..self.end_len];
    }

    /// Every value of every type.
    pub fn all() ValueRange {
        var range = ValueRange{};
        range.start[0] = 0x00;
        range.start_len = 1;
        range.end[0] = 0xFF;
        range.end_len = 1;
        return range;
    }

    fn setStart(self: *ValueRange, bytes_: []const u8) void {
        @memcpy(self.start[0..bytes_.len], bytes_);
        self.start_len = bytes_.len;
    }

    fn setEnd(self: *ValueRange, bytes_: []const u8) void {
        @memcpy(self.end[0..bytes_.len], bytes_);
        self.end_len = bytes_.len;
    }

    fn setStartAfter(self: *ValueRange, bytes_: []const u8) void {
        self.setStart(bytes_);
        _ = incrementBytes(self.start[0..self.start_len]);
    }

    fn setEndAfter(self: *ValueRange, bytes_: []const u8) void {
        self.setEnd(bytes_);
        // Tags are below 0xFF, so there is always a byte to increment.
        _ = incrementBytes(self.end[0..self.end_len]);
    }

    /// Narrow to the values a comparison `property <op> bound` lets
    /// through. `op` is one of `eq`, `lt`, `lte`, `gt`, `gte`,
    /// `starts_with`.
    pub fn constrain(self: *ValueRange, op: RangeOp, bound: PropertyValue) void {
        var next = ValueRange{};
        constrainOne(&next, op, bound);
        self.intersect(&next);
    }

    fn intersect(self: *ValueRange, other: *const ValueRange) void {
        if (other.empty) self.empty = true;
        if (!other.exact) self.exact = false;
        if (std.mem.order(u8, other.startBytes(), self.startBytes()) == .gt) self.setStart(other.startBytes());
        if (std.mem.order(u8, other.endBytes(), self.endBytes()) == .lt) self.setEnd(other.endBytes());
        if (std.mem.order(u8, self.startBytes(), self.endBytes()) != .lt) self.empty = true;
    }
};

pub const RangeOp = enum { eq, lt, lte, gt, gte, starts_with };

fn constrainOne(range: *ValueRange, op: RangeOp, bound: PropertyValue) void {
    if (op == .eq) {
        const encoded = encodeOrderedValue(bound) orelse {
            // Nothing equals null.
            range.empty = true;
            return;
        };
        switch (encoded.buf[0]) {
            // Nor NaN.
            OrderedTag.nan => range.empty = true,
            // A list equals one whose numbers are of the other kind, which
            // has another digest: every list, map and vector, to be checked.
            OrderedTag.other => {
                range.setStart(&.{OrderedTag.other});
                range.setEnd(&.{OrderedTag.other + 1});
                range.exact = false;
            },
            else => {
                range.setStart(encoded.bytes());
                range.setEndAfter(encoded.bytes());
                range.exact = !encoded.truncated;
            },
        }
        return;
    }
    if (op == .starts_with) {
        const prefix = switch (bound) {
            .string_val => |s| s,
            else => {
                range.empty = true;
                return;
            },
        };
        const encoded = encodeOrderedStringPrefix(prefix);
        range.setStart(encoded.bytes());
        range.setEndAfter(encoded.bytes());
        range.exact = !encoded.truncated;
        return;
    }

    // The values a bound of this type can be compared with, as the
    // comparison operators define it: anything else compares as null. NaN
    // is not among them: every ordering comparison with it is false.
    const class_start: u8, const class_end: u8 = switch (bound) {
        .int_val, .float_val => .{ OrderedTag.number, OrderedTag.nan },
        .string_val => .{ OrderedTag.string, OrderedTag.string + 1 },
        .bytes_val => .{ OrderedTag.bytes, OrderedTag.bytes + 1 },
        .bool_val => .{ OrderedTag.boolean, OrderedTag.boolean + 1 },
        else => {
            range.empty = true;
            return;
        },
    };
    range.setStart(&.{class_start});
    range.setEnd(&.{class_end});

    const encoded = encodeOrderedValue(bound).?;
    if (encoded.buf[0] == OrderedTag.nan) {
        range.empty = true;
        return;
    }
    if (encoded.truncated) range.exact = false;
    switch (op) {
        .gte => range.setStart(encoded.bytes()),
        // A cut-short bound shares its key with values on both sides of it.
        .gt => if (encoded.truncated) range.setStart(encoded.bytes()) else range.setStartAfter(encoded.bytes()),
        .lt => if (encoded.truncated) range.setEndAfter(encoded.bytes()) else range.setEnd(encoded.bytes()),
        .lte => range.setEndAfter(encoded.bytes()),
        .eq, .starts_with => unreachable,
    }
}

pub const PropertyIndex = struct {
    allocator: Allocator,
    catalog: *BTree,
    node_entries: *BTree,
    edge_entries: *BTree,
    /// The declarations, decoded once, so that keeping the indexes current
    /// on every write does not read the catalog for every entity, and costs
    /// nothing at all when nothing is declared. Dropped on create and drop.
    cache: DefinitionCache = .{},

    const Self = @This();

    /// Held inline, so the index owns no memory. A database with more
    /// declarations of one kind than fit reads the catalog as before.
    pub const CACHED_DEFINITIONS = 32;

    const DefinitionCache = struct {
        loaded: bool = false,
        /// More of a kind than fit: that kind reads the catalog.
        node_overflow: bool = false,
        edge_overflow: bool = false,
        node_count: usize = 0,
        edge_count: usize = 0,
        nodes: [CACHED_DEFINITIONS]Definition = undefined,
        edges: [CACHED_DEFINITIONS]Definition = undefined,
    };

    pub fn init(allocator: Allocator, catalog: *BTree, node_entries: *BTree, edge_entries: *BTree) Self {
        return .{
            .allocator = allocator,
            .catalog = catalog,
            .node_entries = node_entries,
            .edge_entries = edge_entries,
        };
    }

    pub fn createDefinition(self: *Self, definition: Definition) PropertyIndexError!void {
        const key = definitionKey(definition);
        if (self.catalog.contains(&key) catch return PropertyIndexError.IoError) {
            return PropertyIndexError.AlreadyExists;
        }
        const value: []const u8 = switch (definition.encoding) {
            .digest => &.{},
            .ordered, .ordered_dense => &.{@intFromEnum(definition.encoding)},
        };
        self.cache.loaded = false;
        self.catalog.insert(&key, value) catch |err| return mapBTreeError(err);
    }

    fn loadCache(self: *Self) PropertyIndexError!void {
        var cache = DefinitionCache{};
        inline for (.{ EntityKind.node, EntityKind.edge }) |kind| {
            var iter: DefinitionIterator = undefined;
            try self.iterateDefinitions(kind, &iter);
            defer iter.deinit();
            while (try iter.next()) |definition| {
                const count = if (kind == .node) &cache.node_count else &cache.edge_count;
                if (count.* == CACHED_DEFINITIONS) {
                    if (kind == .node) cache.node_overflow = true else cache.edge_overflow = true;
                    break;
                }
                if (kind == .node) cache.nodes[count.*] = definition else cache.edges[count.*] = definition;
                count.* += 1;
            }
        }
        cache.loaded = true;
        self.cache = cache;
    }

    /// The declarations of one kind from the cache, or null when they do not
    /// fit and the catalog has to be walked.
    fn cachedDefinitions(self: *Self, kind: EntityKind) PropertyIndexError!?[]const Definition {
        if (!self.cache.loaded) try self.loadCache();
        return switch (kind) {
            .node => if (self.cache.node_overflow) null else self.cache.nodes[0..self.cache.node_count],
            .edge => if (self.cache.edge_overflow) null else self.cache.edges[0..self.cache.edge_count],
        };
    }

    /// Call `visit` with each declaration of `kind`.
    fn forEachDefinition(
        self: *Self,
        kind: EntityKind,
        context: anytype,
        comptime visit: fn (@TypeOf(context), Definition) PropertyIndexError!void,
    ) PropertyIndexError!void {
        if (try self.cachedDefinitions(kind)) |definitions| {
            for (definitions) |definition| try visit(context, definition);
            return;
        }
        var iter: DefinitionIterator = undefined;
        try self.iterateDefinitions(kind, &iter);
        defer iter.deinit();
        while (try iter.next()) |definition| try visit(context, definition);
    }

    /// Whether any index of `kind` is declared.
    pub fn anyDeclared(self: *Self, kind: EntityKind) PropertyIndexError!bool {
        const definitions = (try self.cachedDefinitions(kind)) orelse return true;
        return definitions.len > 0;
    }

    /// The stored definition, which carries the index's encoding, or null
    /// when there is none.
    pub fn resolve(self: *Self, definition: Definition) PropertyIndexError!?Definition {
        const key = definitionKey(definition);
        var borrowed = (self.catalog.getBorrowed(&key) catch |err| return mapBTreeError(err)) orelse return null;
        defer borrowed.release();
        var resolved = definition;
        resolved.encoding = try decodeEncoding(borrowed.bytes);
        return resolved;
    }

    pub fn dropDefinition(self: *Self, definition: Definition) PropertyIndexError!void {
        const key = definitionKey(definition);
        self.cache.loaded = false;
        self.catalog.delete(&key) catch |err| return mapBTreeError(err);
        try self.clearEntries(definition);
    }

    pub fn hasDefinition(self: *Self, definition: Definition) PropertyIndexError!bool {
        const key = definitionKey(definition);
        return self.catalog.contains(&key) catch return PropertyIndexError.IoError;
    }

    pub fn add(
        self: *Self,
        definition: Definition,
        entity_id: u64,
        value: PropertyValue,
    ) PropertyIndexError!void {
        var buf: EntryKeyBuffer = undefined;
        const key = entryKeyFor(definition, value, entity_id, &buf) orelse return;
        self.entryTree(definition.kind).insert(key, &.{}) catch |err| switch (err) {
            BTreeError.DuplicateKey => return,
            else => return mapBTreeError(err),
        };
    }

    pub fn remove(
        self: *Self,
        definition: Definition,
        entity_id: u64,
        value: PropertyValue,
    ) PropertyIndexError!void {
        var buf: EntryKeyBuffer = undefined;
        const key = entryKeyFor(definition, value, entity_id, &buf) orelse return;
        self.entryTree(definition.kind).delete(key) catch |err| switch (err) {
            BTreeError.KeyNotFound => return,
            else => return mapBTreeError(err),
        };
    }

    pub fn indexNode(self: *Self, node_id: u64, labels: []const SymbolId, properties: []const Property) PropertyIndexError!void {
        try self.replaceNode(node_id, &.{}, &.{}, labels, properties);
    }

    pub fn removeNode(self: *Self, node_id: u64, labels: []const SymbolId, properties: []const Property) PropertyIndexError!void {
        try self.replaceNode(node_id, labels, properties, &.{}, &.{});
    }

    /// Move a node's entries from its old labels and properties to its new
    /// ones. An entry whose key does not change is left where it is, so an
    /// update that does not touch an indexed property writes nothing here.
    pub fn replaceNode(
        self: *Self,
        node_id: u64,
        old_labels: []const SymbolId,
        old_properties: []const Property,
        new_labels: []const SymbolId,
        new_properties: []const Property,
    ) PropertyIndexError!void {
        const Change = struct {
            index: *Self,
            id: u64,
            old_labels: []const SymbolId,
            old_properties: []const Property,
            new_labels: []const SymbolId,
            new_properties: []const Property,

            fn visit(change: *const @This(), definition: Definition) PropertyIndexError!void {
                const was: ?PropertyValue = if (containsSymbol(change.old_labels, definition.scope_id))
                    scopedValue(definition, change.old_properties)
                else
                    null;
                const now: ?PropertyValue = if (containsSymbol(change.new_labels, definition.scope_id))
                    scopedValue(definition, change.new_properties)
                else
                    null;
                try change.index.replaceEntry(definition, change.id, was, now);
            }
        };
        const change = Change{
            .index = self,
            .id = node_id,
            .old_labels = old_labels,
            .old_properties = old_properties,
            .new_labels = new_labels,
            .new_properties = new_properties,
        };
        try self.forEachDefinition(.node, &change, Change.visit);
    }

    pub fn indexEdge(self: *Self, edge_id: u64, edge_type: SymbolId, properties: []const Property) PropertyIndexError!void {
        try self.replaceEdge(edge_id, null, &.{}, edge_type, properties);
    }

    pub fn removeEdge(self: *Self, edge_id: u64, edge_type: SymbolId, properties: []const Property) PropertyIndexError!void {
        try self.replaceEdge(edge_id, edge_type, properties, null, &.{});
    }

    /// The same for an edge, scoped by its type. A null type is an edge
    /// that does not exist on that side.
    pub fn replaceEdge(
        self: *Self,
        edge_id: u64,
        old_type: ?SymbolId,
        old_properties: []const Property,
        new_type: ?SymbolId,
        new_properties: []const Property,
    ) PropertyIndexError!void {
        const Change = struct {
            index: *Self,
            id: u64,
            old_type: ?SymbolId,
            old_properties: []const Property,
            new_type: ?SymbolId,
            new_properties: []const Property,

            fn visit(change: *const @This(), definition: Definition) PropertyIndexError!void {
                const was: ?PropertyValue = if (change.old_type == definition.scope_id)
                    scopedValue(definition, change.old_properties)
                else
                    null;
                const now: ?PropertyValue = if (change.new_type == definition.scope_id)
                    scopedValue(definition, change.new_properties)
                else
                    null;
                try change.index.replaceEntry(definition, change.id, was, now);
            }
        };
        const change = Change{
            .index = self,
            .id = edge_id,
            .old_type = old_type,
            .old_properties = old_properties,
            .new_type = new_type,
            .new_properties = new_properties,
        };
        try self.forEachDefinition(.edge, &change, Change.visit);
    }

    fn replaceEntry(self: *Self, definition: Definition, id: u64, was: ?PropertyValue, now: ?PropertyValue) PropertyIndexError!void {
        var old_buf: EntryKeyBuffer = undefined;
        var new_buf: EntryKeyBuffer = undefined;
        const old_key: ?[]const u8 = if (was) |v| entryKeyFor(definition, v, id, &old_buf) else null;
        const new_key: ?[]const u8 = if (now) |v| entryKeyFor(definition, v, id, &new_buf) else null;
        if (old_key != null and new_key != null and std.mem.eql(u8, old_key.?, new_key.?)) return;
        if (old_key) |key| {
            self.entryTree(definition.kind).delete(key) catch |err| switch (err) {
                BTreeError.KeyNotFound => {},
                else => return mapBTreeError(err),
            };
        }
        if (new_key) |key| {
            self.entryTree(definition.kind).insert(key, &.{}) catch |err| switch (err) {
                BTreeError.DuplicateKey => {},
                else => return mapBTreeError(err),
            };
        }
    }

    pub fn lookup(
        self: *Self,
        definition: Definition,
        value: PropertyValue,
        limit: usize,
    ) PropertyIndexError![]u64 {
        const stored = (try self.resolve(definition)) orelse return PropertyIndexError.NotFound;

        var prefix_buf: EntryKeyBuffer = undefined;
        const prefix: []const u8 = switch (stored.encoding) {
            .digest => blk: {
                const digest_prefix = entryPrefix(stored.scope_id, stored.property_id, value);
                @memcpy(prefix_buf[0..ENTRY_PREFIX_SIZE], &digest_prefix);
                break :blk prefix_buf[0..ENTRY_PREFIX_SIZE];
            },
            .ordered, .ordered_dense => blk: {
                // Nothing equals null, though a dense index holds nodes
                // without the property.
                const encoded = encodeOrderedValue(value) orelse
                    return self.allocator.alloc(u64, 0) catch return PropertyIndexError.OutOfMemory;
                break :blk orderedPrefix(stored, encoded.bytes(), &prefix_buf);
            },
        };
        var end_buf: EntryKeyBuffer = undefined;
        const end = end_buf[0..prefix.len];
        @memcpy(end, prefix);
        if (!incrementBytes(end)) return self.allocator.alloc(u64, 0) catch return PropertyIndexError.OutOfMemory;

        var iter = self.entryTree(stored.kind).range(prefix, end) catch |err| return mapBTreeError(err);
        defer iter.deinit();

        var results: std.ArrayList(u64) = .empty;
        errdefer results.deinit(self.allocator);
        while (results.items.len < limit) {
            const entry = iter.next() catch |err| return mapBTreeError(err);
            const found = entry orelse break;
            // An ordered value is never a prefix of another, so every key in
            // the range is this value followed by an id.
            if (found.key.len != prefix.len + 8) return PropertyIndexError.InvalidData;
            results.append(self.allocator, entityIdOf(found.key)) catch {
                return PropertyIndexError.OutOfMemory;
            };
        }
        return results.toOwnedSlice(self.allocator) catch return PropertyIndexError.OutOfMemory;
    }

    pub const Direction = enum { forward, backward };

    /// Read up to `max` entries of an ordered index whose keys lie in
    /// [start, end), first to last or last to first, appending each key to
    /// `keys` and its end offset to `ends`. Returns whether the range has
    /// nothing beyond what was read.
    ///
    /// A chunk at a time rather than an iterator, so that no page stays
    /// pinned between calls: the next call starts after the last key read,
    /// or, backward, ends at it.
    pub fn readRange(
        self: *Self,
        kind: EntityKind,
        direction: Direction,
        start: []const u8,
        end: []const u8,
        max: usize,
        allocator: Allocator,
        keys: *std.ArrayList(u8),
        ends: *std.ArrayList(u32),
    ) PropertyIndexError!bool {
        switch (direction) {
            .forward => {
                var iter = self.entryTree(kind).range(start, end) catch |err| return mapBTreeError(err);
                defer iter.deinit();
                return readEntries(&iter, max, allocator, keys, ends);
            },
            .backward => {
                var iter = self.entryTree(kind).rangeBackward(start, end) catch |err| return mapBTreeError(err);
                defer iter.deinit();
                return readEntries(&iter, max, allocator, keys, ends);
            },
        }
    }

    /// How many entries of an index lie in [start, end), counting no
    /// further than `max`.
    pub fn countRange(self: *Self, kind: EntityKind, start: []const u8, end: []const u8, max: usize) PropertyIndexError!usize {
        var iter = self.entryTree(kind).range(start, end) catch |err| return mapBTreeError(err);
        defer iter.deinit();
        var count: usize = 0;
        while (count < max) : (count += 1) {
            _ = (iter.next() catch |err| return mapBTreeError(err)) orelse break;
        }
        return count;
    }

    fn readEntries(
        iter: anytype,
        max: usize,
        allocator: Allocator,
        keys: *std.ArrayList(u8),
        ends: *std.ArrayList(u32),
    ) PropertyIndexError!bool {
        var read: usize = 0;
        while (read < max) : (read += 1) {
            const entry = (iter.next() catch |err| return mapBTreeError(err)) orelse return true;
            if (entry.key.len < SCOPE_PREFIX_SIZE + 1 + 8) return PropertyIndexError.InvalidData;
            keys.appendSlice(allocator, entry.key) catch return PropertyIndexError.OutOfMemory;
            ends.append(allocator, @intCast(keys.items.len)) catch return PropertyIndexError.OutOfMemory;
        }
        return false;
    }

    pub fn clearEntries(self: *Self, definition: Definition) PropertyIndexError!void {
        var prefix: [SCOPE_PREFIX_SIZE]u8 = undefined;
        std.mem.writeInt(u16, prefix[0..2], definition.scope_id, .big);
        std.mem.writeInt(u16, prefix[2..4], definition.property_id, .big);
        var end = prefix;
        if (!incrementBytes(&end)) return;

        // Entries of both encodings: a declaration may be replacing one
        // dropped in the other.
        var keys: std.ArrayList(u8) = .empty;
        defer keys.deinit(self.allocator);
        var ends: std.ArrayList(u32) = .empty;
        defer ends.deinit(self.allocator);
        var iter = self.entryTree(definition.kind).range(&prefix, &end) catch |err| return mapBTreeError(err);
        while (iter.next() catch |err| {
            iter.deinit();
            return mapBTreeError(err);
        }) |entry| {
            keys.appendSlice(self.allocator, entry.key) catch {
                iter.deinit();
                return PropertyIndexError.OutOfMemory;
            };
            ends.append(self.allocator, @intCast(keys.items.len)) catch {
                iter.deinit();
                return PropertyIndexError.OutOfMemory;
            };
        }
        iter.deinit();

        var from: usize = 0;
        for (ends.items) |to| {
            self.entryTree(definition.kind).delete(keys.items[from..to]) catch |err| switch (err) {
                BTreeError.KeyNotFound => {},
                else => return mapBTreeError(err),
            };
            from = to;
        }
    }

    /// A walk over the definitions of one kind.
    ///
    /// The iterator owns its bounds. The tree's iterator keeps the end key as a
    /// slice and compares against it on every step, so building the bounds in
    /// locals and returning the iterator leaves those comparisons reading a stack
    /// frame that no longer exists. That does not crash. It reads whatever the
    /// next call left there, and the walk either ends early or never starts —
    /// silently, and differently depending on what the caller happens to have on
    /// its stack, which is why this stood for so long without a failing test.
    pub const DefinitionIterator = struct {
        inner: BTree.Iterator,
        start: [1]u8 = undefined,
        end: [1]u8 = undefined,

        pub fn next(self: *DefinitionIterator) PropertyIndexError!?Definition {
            return nextDefinition(&self.inner);
        }

        pub fn deinit(self: *DefinitionIterator) void {
            self.inner.deinit();
        }
    };

    /// Walk every definition of one kind.
    ///
    /// Written into `out` rather than returned, because the iterator holds the
    /// bounds it compares against and moving it would leave those comparisons
    /// pointing at the copy left behind.
    pub fn iterateDefinitions(self: *Self, kind: EntityKind, out: *DefinitionIterator) PropertyIndexError!void {
        out.start = .{@intFromEnum(kind)};
        out.end = .{@intFromEnum(kind) + 1};
        out.inner = self.catalog.range(&out.start, &out.end) catch |err| return mapBTreeError(err);
    }

    pub fn nextDefinition(iter: *BTree.Iterator) PropertyIndexError!?Definition {
        const entry = iter.next() catch |err| return mapBTreeError(err);
        const found = entry orelse return null;
        if (found.key.len != DEFINITION_KEY_SIZE) return PropertyIndexError.InvalidData;
        const kind: EntityKind = switch (found.key[0]) {
            @intFromEnum(EntityKind.node) => .node,
            @intFromEnum(EntityKind.edge) => .edge,
            else => return PropertyIndexError.InvalidData,
        };
        return .{
            .kind = kind,
            .scope_id = std.mem.readInt(u16, found.key[1..3], .big),
            .property_id = std.mem.readInt(u16, found.key[3..5], .big),
            .encoding = try decodeEncoding(found.value),
        };
    }

    fn entryTree(self: *Self, kind: EntityKind) *BTree {
        return switch (kind) {
            .node => self.node_entries,
            .edge => self.edge_entries,
        };
    }
};

fn definitionKey(definition: Definition) [DEFINITION_KEY_SIZE]u8 {
    var key: [DEFINITION_KEY_SIZE]u8 = undefined;
    key[0] = @intFromEnum(definition.kind);
    std.mem.writeInt(u16, key[1..3], definition.scope_id, .big);
    std.mem.writeInt(u16, key[3..5], definition.property_id, .big);
    return key;
}

fn decodeEncoding(value: []const u8) PropertyIndexError!Encoding {
    if (value.len == 0) return .digest;
    if (value.len == 1) {
        if (value[0] == @intFromEnum(Encoding.ordered)) return .ordered;
        if (value[0] == @intFromEnum(Encoding.ordered_dense)) return .ordered_dense;
    }
    return PropertyIndexError.InvalidData;
}

/// Room for any entry key.
pub const EntryKeyBuffer = [ORDERED_KEY_MAX]u8;

/// The scope and property ids, which begin every entry key of an index.
pub fn scopePrefix(definition: Definition) [SCOPE_PREFIX_SIZE]u8 {
    var prefix: [SCOPE_PREFIX_SIZE]u8 = undefined;
    std.mem.writeInt(u16, prefix[0..2], definition.scope_id, .big);
    std.mem.writeInt(u16, prefix[2..4], definition.property_id, .big);
    return prefix;
}

/// The scope prefix followed by `value_bytes`, in `buf`.
pub fn orderedPrefix(definition: Definition, value_bytes: []const u8, buf: *EntryKeyBuffer) []u8 {
    const scope = scopePrefix(definition);
    @memcpy(buf[0..SCOPE_PREFIX_SIZE], &scope);
    @memcpy(buf[SCOPE_PREFIX_SIZE..][0..value_bytes.len], value_bytes);
    return buf[0 .. SCOPE_PREFIX_SIZE + value_bytes.len];
}

/// The entity an entry key names: its last eight bytes.
pub fn entityIdOf(key: []const u8) u64 {
    return std.mem.readInt(u64, key[key.len - 8 ..][0..8], .big);
}

/// The value part of an ordered entry key.
pub fn orderedValueOf(key: []const u8) []const u8 {
    return key[SCOPE_PREFIX_SIZE .. key.len - 8];
}

/// The key an entity's value has in an index, or null when the encoding
/// leaves the value out. Null is the missing property: a dense index keys it
/// as absent, an `ordered` one leaves it out.
pub fn entryKeyFor(definition: Definition, value: PropertyValue, entity_id: u64, buf: *EntryKeyBuffer) ?[]const u8 {
    switch (definition.encoding) {
        .digest => {
            const key = entryKey(definition.scope_id, definition.property_id, value, entity_id);
            @memcpy(buf[0..ENTRY_KEY_SIZE], &key);
            return buf[0..ENTRY_KEY_SIZE];
        },
        .ordered, .ordered_dense => {
            const absent = [_]u8{OrderedTag.absent};
            const value_bytes: []const u8 = if (encodeOrderedValue(value)) |encoded|
                encoded.bytes()
            else if (definition.encoding == .ordered_dense)
                &absent
            else
                return null;
            const prefix = orderedPrefix(definition, value_bytes, buf);
            std.mem.writeInt(u64, buf[prefix.len..][0..8], entity_id, .big);
            return buf[0 .. prefix.len + 8];
        },
    }
}

fn entryPrefix(scope_id: SymbolId, property_id: SymbolId, value: PropertyValue) [ENTRY_PREFIX_SIZE]u8 {
    var prefix: [ENTRY_PREFIX_SIZE]u8 = undefined;
    std.mem.writeInt(u16, prefix[0..2], scope_id, .big);
    std.mem.writeInt(u16, prefix[2..4], property_id, .big);
    hashPropertyValue(value, prefix[4..36]);
    return prefix;
}

fn entryKey(scope_id: SymbolId, property_id: SymbolId, value: PropertyValue, entity_id: u64) [ENTRY_KEY_SIZE]u8 {
    var key: [ENTRY_KEY_SIZE]u8 = undefined;
    const prefix = entryPrefix(scope_id, property_id, value);
    @memcpy(key[0..ENTRY_PREFIX_SIZE], &prefix);
    std.mem.writeInt(u64, key[ENTRY_PREFIX_SIZE..ENTRY_KEY_SIZE], entity_id, .big);
    return key;
}

fn hashPropertyValue(value: PropertyValue, output: *[VALUE_DIGEST_SIZE]u8) void {
    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    hashValue(&hasher, value);
    hasher.final(output);
}

fn hashValue(hasher: *std.crypto.hash.sha2.Sha256, value: PropertyValue) void {
    const tag: u8 = @intFromEnum(value);
    hasher.update(&.{tag});
    switch (value) {
        .null_val => {},
        .bool_val => |v| hasher.update(&.{@intFromBool(v)}),
        .int_val => |v| hashInt(hasher, i64, v),
        .float_val => |v| hashInt(hasher, u64, @bitCast(if (v == 0) @as(f64, 0) else v)),
        .string_val => |v| hashBytes(hasher, v),
        .bytes_val => |v| hashBytes(hasher, v),
        .vector_val => |values| {
            hashInt(hasher, u64, values.len);
            for (values) |v| hashInt(hasher, u32, @bitCast(if (v == 0) @as(f32, 0) else v));
        },
        .list_val => |values| {
            hashInt(hasher, u64, values.len);
            for (values) |item| hashValue(hasher, item);
        },
        .map_val => |entries| {
            hashInt(hasher, u64, entries.len);
            for (entries) |entry| {
                hashBytes(hasher, entry.key);
                hashValue(hasher, entry.value);
            }
        },
    }
}

fn hashBytes(hasher: *std.crypto.hash.sha2.Sha256, bytes: []const u8) void {
    hashInt(hasher, u64, bytes.len);
    hasher.update(bytes);
}

fn hashInt(hasher: *std.crypto.hash.sha2.Sha256, comptime T: type, value: T) void {
    var bytes: [@sizeOf(T)]u8 = undefined;
    std.mem.writeInt(T, &bytes, value, .big);
    hasher.update(&bytes);
}

fn containsSymbol(values: []const SymbolId, needle: SymbolId) bool {
    for (values) |value| if (value == needle) return true;
    return false;
}

fn findProperty(properties: []const Property, property_id: SymbolId) ?Property {
    for (properties) |property| if (property.key_id == property_id) return property;
    return null;
}

/// What an entity in an index's scope is indexed under: its value, null
/// for a dense index's absent entry, or nothing.
fn scopedValue(definition: Definition, properties: []const Property) ?PropertyValue {
    if (findProperty(properties, definition.property_id)) |p| return p.value;
    return if (definition.encoding == .ordered_dense) .{ .null_val = {} } else null;
}

fn incrementBytes(bytes: []u8) bool {
    var index = bytes.len;
    while (index > 0) {
        index -= 1;
        if (bytes[index] != 0xff) {
            bytes[index] += 1;
            @memset(bytes[index + 1 ..], 0);
            return true;
        }
    }
    return false;
}

fn mapBTreeError(err: BTreeError) PropertyIndexError {
    return switch (err) {
        BTreeError.KeyNotFound => PropertyIndexError.NotFound,
        BTreeError.DuplicateKey => PropertyIndexError.AlreadyExists,
        BTreeError.OutOfMemory => PropertyIndexError.OutOfMemory,
        BTreeError.InvalidPage => PropertyIndexError.InvalidData,
        else => PropertyIndexError.IoError,
    };
}

test "property value digest normalizes signed zero" {
    var positive: [VALUE_DIGEST_SIZE]u8 = undefined;
    var negative: [VALUE_DIGEST_SIZE]u8 = undefined;
    hashPropertyValue(.{ .float_val = 0.0 }, &positive);
    hashPropertyValue(.{ .float_val = -0.0 }, &negative);
    try std.testing.expectEqualSlices(u8, &positive, &negative);
}

fn expectEncodedOrder(a: PropertyValue, b: PropertyValue, expected: std.math.Order) !void {
    const ea = encodeOrderedValue(a).?;
    const eb = encodeOrderedValue(b).?;
    try std.testing.expectEqual(expected, std.mem.order(u8, ea.bytes(), eb.bytes()));
}

test "ordered encoding orders numbers exactly, integers and floats together" {
    const big: i64 = (1 << 60) + 1;
    const cases = [_]PropertyValue{
        .{ .float_val = -std.math.inf(f64) },
        .{ .int_val = std.math.minInt(i64) },
        .{ .float_val = -1e18 },
        .{ .int_val = -3 },
        .{ .float_val = -2.5 },
        .{ .int_val = -1 },
        .{ .float_val = -0.5 },
        .{ .int_val = 0 },
        .{ .float_val = 0.25 },
        .{ .int_val = 1 },
        .{ .float_val = 1.5 },
        .{ .int_val = 2 },
        .{ .float_val = @floatFromInt(@as(i64, 1 << 60)) },
        .{ .int_val = big },
        .{ .int_val = std.math.maxInt(i64) - 1 },
        .{ .int_val = std.math.maxInt(i64) },
        .{ .float_val = 0x1p63 },
        .{ .float_val = std.math.inf(f64) },
        .{ .float_val = std.math.nan(f64) },
    };
    for (cases[0 .. cases.len - 1], cases[1..]) |a, b| try expectEncodedOrder(a, b, .lt);
    try expectEncodedOrder(.{ .int_val = 3 }, .{ .float_val = 3.0 }, .eq);
    try expectEncodedOrder(.{ .float_val = -0.0 }, .{ .int_val = 0 }, .eq);
    try expectEncodedOrder(.{ .int_val = 1 << 60 }, .{ .float_val = 0x1p60 }, .eq);
}

test "ordered encoding orders strings by bytes, prefixes first, NUL included" {
    const cases = [_][]const u8{ "", "\x00", "\x00\x00", "\x00\x01", "a", "a\x00", "a\x00b", "a\x01", "ab", "b", "\xff" };
    for (cases[0 .. cases.len - 1], cases[1..]) |a, b| {
        try expectEncodedOrder(.{ .string_val = a }, .{ .string_val = b }, .lt);
    }
}

test "ordered encoding of long strings keeps order between groups" {
    var long_a: [200]u8 = undefined;
    @memset(&long_a, 'a');
    var long_b = long_a;
    long_b[150] = 'b';
    const exact = long_a[0..ORDERED_STRING_PREFIX];
    // Strings sharing the first 128 bytes share a key, above the 128-byte one.
    try expectEncodedOrder(.{ .string_val = &long_a }, .{ .string_val = &long_b }, .eq);
    try expectEncodedOrder(.{ .string_val = exact }, .{ .string_val = &long_a }, .lt);
    var after = long_a;
    after[127] = 'b';
    try expectEncodedOrder(.{ .string_val = &long_a }, .{ .string_val = after[0..128] }, .lt);
    try std.testing.expect(encodeOrderedValue(.{ .string_val = &long_a }).?.truncated);
}

test "ordered encoding puts types in ORDER BY's order and leaves null out" {
    const list = [_]PropertyValue{.{ .int_val = 1 }};
    try expectEncodedOrder(.{ .list_val = &list }, .{ .bytes_val = "z" }, .lt);
    try expectEncodedOrder(.{ .bytes_val = "z" }, .{ .string_val = "" }, .lt);
    try expectEncodedOrder(.{ .string_val = "zzz" }, .{ .bool_val = false }, .lt);
    try expectEncodedOrder(.{ .bool_val = false }, .{ .bool_val = true }, .lt);
    try expectEncodedOrder(.{ .bool_val = true }, .{ .float_val = -std.math.inf(f64) }, .lt);
    try std.testing.expect(encodeOrderedValue(.{ .null_val = {} }) == null);
}

fn rangeHas(range: *const ValueRange, value: PropertyValue) bool {
    const encoded = encodeOrderedValue(value).?;
    // An entry key follows the value with an id; any id will do.
    var key: [ORDERED_VALUE_MAX + 8]u8 = undefined;
    @memcpy(key[0..encoded.len], encoded.bytes());
    @memset(key[encoded.len..][0..8], 0x5a);
    const k = key[0 .. encoded.len + 8];
    return std.mem.order(u8, k, range.startBytes()) != .lt and std.mem.order(u8, k, range.endBytes()) == .lt;
}

test "value ranges hold exactly the values their comparisons accept" {
    var r = ValueRange.all();
    r.constrain(.gte, .{ .int_val = 18 });
    r.constrain(.lt, .{ .float_val = 65.0 });
    try std.testing.expect(r.exact and !r.empty);
    try std.testing.expect(rangeHas(&r, .{ .int_val = 18 }));
    try std.testing.expect(rangeHas(&r, .{ .float_val = 18.0 }));
    try std.testing.expect(rangeHas(&r, .{ .float_val = 64.999 }));
    try std.testing.expect(!rangeHas(&r, .{ .float_val = 17.999 }));
    try std.testing.expect(!rangeHas(&r, .{ .int_val = 65 }));
    try std.testing.expect(!rangeHas(&r, .{ .string_val = "30" }));
    try std.testing.expect(!rangeHas(&r, .{ .float_val = std.math.nan(f64) }));

    var gt = ValueRange.all();
    gt.constrain(.gt, .{ .int_val = 60 });
    try std.testing.expect(!rangeHas(&gt, .{ .float_val = 60.0 }));
    try std.testing.expect(rangeHas(&gt, .{ .int_val = 61 }));
    try std.testing.expect(rangeHas(&gt, .{ .float_val = 60.0000001 }));
    // No comparison lets NaN through, whichever way round it is written.
    try std.testing.expect(!rangeHas(&gt, .{ .float_val = std.math.nan(f64) }));
    var nan_bound = ValueRange.all();
    nan_bound.constrain(.lt, .{ .float_val = std.math.nan(f64) });
    try std.testing.expect(nan_bound.empty);

    var prefix = ValueRange.all();
    prefix.constrain(.starts_with, .{ .string_val = "Jo" });
    try std.testing.expect(prefix.exact);
    try std.testing.expect(rangeHas(&prefix, .{ .string_val = "Jo" }));
    try std.testing.expect(rangeHas(&prefix, .{ .string_val = "John" }));
    try std.testing.expect(rangeHas(&prefix, .{ .string_val = "Jo\x00" }));
    try std.testing.expect(!rangeHas(&prefix, .{ .string_val = "J" }));
    try std.testing.expect(!rangeHas(&prefix, .{ .string_val = "Jp" }));

    var lte = ValueRange.all();
    lte.constrain(.lte, .{ .string_val = "b" });
    try std.testing.expect(rangeHas(&lte, .{ .string_val = "b" }));
    try std.testing.expect(rangeHas(&lte, .{ .string_val = "a\xff\xff" }));
    try std.testing.expect(!rangeHas(&lte, .{ .string_val = "b\x00" }));

    var mixed = ValueRange.all();
    mixed.constrain(.gt, .{ .int_val = 1 });
    mixed.constrain(.lt, .{ .string_val = "x" });
    try std.testing.expect(mixed.empty);

    var none = ValueRange.all();
    none.constrain(.gt, .{ .null_val = {} });
    try std.testing.expect(none.empty);

    var eq = ValueRange.all();
    eq.constrain(.eq, .{ .int_val = 3 });
    try std.testing.expect(eq.exact and !eq.empty);
    try std.testing.expect(rangeHas(&eq, .{ .float_val = 3.0 }));
    try std.testing.expect(!rangeHas(&eq, .{ .int_val = 4 }));
    try std.testing.expect(!rangeHas(&eq, .{ .float_val = 2.9999999 }));
    var eq_nan = ValueRange.all();
    eq_nan.constrain(.eq, .{ .float_val = std.math.nan(f64) });
    try std.testing.expect(eq_nan.empty);
    const list = [_]PropertyValue{.{ .int_val = 1 }};
    const list_f = [_]PropertyValue{.{ .float_val = 1.0 }};
    var eq_list = ValueRange.all();
    eq_list.constrain(.eq, .{ .list_val = &list });
    try std.testing.expect(!eq_list.exact);
    try std.testing.expect(rangeHas(&eq_list, .{ .list_val = &list_f }));
}
