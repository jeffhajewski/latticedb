//! The built-in changefeed: its compact event encoding, where events live,
//! how they are read back as the records applications have always seen,
//! and how retention trims them.
//!
//! ## Where events live
//!
//! Events share the stream events tree with user streams. A user stream's key
//! is `[name length][name][sequence: u64 big-endian]`, and names are 1 to 255
//! bytes, so a key whose first byte is 0 can belong to no user stream. The
//! changefeed's events (encoding version 1) use
//!
//!     key = 0x00, n, sequence as n big-endian bytes (n = 1..8, minimal)
//!
//! which sorts by sequence (a longer number has a larger n) and is 3 to 10
//! bytes instead of 26. Events written before this encoding, by 0.16.0 or by
//! 0.17.0 before it, stay where they were, under the legacy key
//! `[17]"__lattice_changes"[sequence]`, in the legacy record format, and are
//! read as before. A read merges both ranges by sequence.
//!
//! ## Event value, version 1
//!
//!     byte 0      vvv t oooo: v = encoding version (1), t = a transaction
//!                 header follows, o = operation (`Op`)
//!     [t]         commit time: varint, milliseconds since the Unix epoch
//!     id          varint: node id, or edge id for edge operations
//!     edge ops    insert/delete: source varint, target varint, type symbol
//!     label ops   label symbol
//!     prop ops    key symbol, old value, new value
//!
//! A varint is unsigned LEB128. A symbol is a varint s: an even s is the
//! symbol id s/2 in the database's symbol table; an odd s is followed by
//! (s-1)/2 bytes of UTF-8, the name itself. Values use the engine's value
//! encoding (`wal_payload.serializePropertyValue`): a type tag byte, then the
//! value; an absent old or new value is the null value, one byte 0.
//!
//! Byte 0 of a legacy record is the length of its kind, 11 to 20 for the
//! changefeed's kinds; byte 0 of a version 1 event is 0x21 to 0x3a. The two
//! never collide, and the key already says which a value is.
//!
//! The first event of each transaction carries the transaction header (its
//! commit time); the other events of the transaction take it from there. A
//! trim that removes the head of a transaction rewrites the new oldest event
//! to carry the header, so every retained compact event can be dated.
//!
//! ## Symbols
//!
//! Symbols are never removed from the symbol table and an id never changes
//! meaning, so an id in the tree stays resolvable: the file is only ever
//! written as a whole image of the trees (the flush journal), and a symbol
//! is interned before any event that names it is applied. Interning is not
//! logged, though, and a crash can lose a symbol interned since the last
//! flush; replay then interns the name again, possibly under another id. So
//! the log never carries bare ids: each logged batch of events carries the
//! names of the symbols it uses, and replay maps them to the recovered ids.

const std = @import("std");
const lattice = @import("lattice");

const Allocator = std.mem.Allocator;
const PropertyValue = lattice.core.types.PropertyValue;
const BTree = lattice.storage.btree.BTree;
const BTreeError = lattice.storage.btree.BTreeError;
const wal_payload = lattice.transaction.wal_payload;
const store = lattice.stream.store;

pub const ENCODING_VERSION: u8 = 1;
/// Version of the logged batch of events (`PayloadType.changefeed_events`).
pub const LOG_VERSION: u8 = 1;

pub const Error = error{
    InvalidRecord,
    OutOfMemory,
    IoError,
    ValueTooLarge,
};

pub const Op = enum(u4) {
    node_insert = 1,
    node_delete = 2,
    node_label_add = 3,
    node_label_remove = 4,
    node_property_set = 5,
    node_property_remove = 6,
    edge_insert = 7,
    edge_delete = 8,
    edge_property_set = 9,
    edge_property_remove = 10,

    pub fn kind(self: Op) []const u8 {
        return switch (self) {
            .node_insert => "node.insert",
            .node_delete => "node.delete",
            .node_label_add => "node.label_add",
            .node_label_remove => "node.label_remove",
            .node_property_set => "node.property_set",
            .node_property_remove => "node.property_remove",
            .edge_insert => "edge.insert",
            .edge_delete => "edge.delete",
            .edge_property_set => "edge.property_set",
            .edge_property_remove => "edge.property_remove",
        };
    }

    fn opName(self: Op) []const u8 {
        return switch (self) {
            .node_insert, .edge_insert => "insert",
            .node_delete, .edge_delete => "delete",
            .node_label_add => "label_add",
            .node_label_remove => "label_remove",
            .node_property_set, .edge_property_set => "property_set",
            .node_property_remove, .edge_property_remove => "property_remove",
        };
    }

    pub fn isEdge(self: Op) bool {
        return @intFromEnum(self) >= @intFromEnum(Op.edge_insert);
    }

    pub fn hasEndpoints(self: Op) bool {
        return self == .edge_insert or self == .edge_delete;
    }

    pub fn isLabel(self: Op) bool {
        return self == .node_label_add or self == .node_label_remove;
    }

    pub fn isProperty(self: Op) bool {
        return switch (self) {
            .node_property_set, .node_property_remove, .edge_property_set, .edge_property_remove => true,
            else => false,
        };
    }

    pub fn hasSymbol(self: Op) bool {
        return self.hasEndpoints() or self.isLabel() or self.isProperty();
    }
};

pub const Symbol = union(enum) {
    id: u16,
    name: []const u8,
};

/// One event, decoded. Slices point into the bytes it was decoded from.
pub const Event = struct {
    op: Op,
    /// Commit time in milliseconds since the Unix epoch, on the first event
    /// of a transaction.
    commit_ms: ?u64 = null,
    /// Node id, or edge id for edge operations.
    id: u64,
    source: u64 = 0,
    target: u64 = 0,
    symbol: Symbol = .{ .id = 0 },
    /// Encoded values (engine value encoding), for property operations.
    old_value: []const u8 = &.{},
    new_value: []const u8 = &.{},
};

// ---------------------------------------------------------------------------
// Varints
// ---------------------------------------------------------------------------

pub fn varintLen(value: u64) usize {
    var v = value;
    var n: usize = 1;
    while (v >= 0x80) : (v >>= 7) n += 1;
    return n;
}

pub fn writeVarint(out: []u8, value: u64) usize {
    var v = value;
    var i: usize = 0;
    while (v >= 0x80) : (v >>= 7) {
        out[i] = @as(u8, @truncate(v)) | 0x80;
        i += 1;
    }
    out[i] = @truncate(v);
    return i + 1;
}

pub fn readVarint(bytes: []const u8, pos: *usize) Error!u64 {
    var result: u64 = 0;
    var shift: u7 = 0;
    while (true) {
        if (pos.* >= bytes.len or shift > 63) return Error.InvalidRecord;
        const b = bytes[pos.*];
        pos.* += 1;
        const part: u64 = b & 0x7f;
        if (shift == 63 and part > 1) return Error.InvalidRecord;
        result |= part << @intCast(shift);
        if (b & 0x80 == 0) return result;
        shift += 7;
    }
}

fn appendVarint(list: *std.ArrayListUnmanaged(u8), allocator: Allocator, value: u64) Error!void {
    var buf: [10]u8 = undefined;
    const n = writeVarint(&buf, value);
    list.appendSlice(allocator, buf[0..n]) catch return Error.OutOfMemory;
}

// ---------------------------------------------------------------------------
// Keys
// ---------------------------------------------------------------------------

pub const MAX_KEY_LEN = 10;

/// The tree key of event `sequence` (sequence >= 1).
pub fn encodeKey(buf: *[MAX_KEY_LEN]u8, sequence: u64) []const u8 {
    const n: usize = @max(1, 8 - @as(usize, @clz(sequence)) / 8);
    buf[0] = 0;
    buf[1] = @intCast(n);
    var i: usize = 0;
    while (i < n) : (i += 1) {
        buf[2 + i] = @truncate(sequence >> @intCast(8 * (n - 1 - i)));
    }
    return buf[0 .. 2 + n];
}

/// Past every compact event key.
pub const end_key = [_]u8{ 0x00, 0x09 };

pub fn decodeKey(key: []const u8) Error!u64 {
    if (key.len < 3 or key[0] != 0) return Error.InvalidRecord;
    const n: usize = key[1];
    if (n < 1 or n > 8 or key.len != 2 + n) return Error.InvalidRecord;
    var v: u64 = 0;
    for (key[2..]) |b| v = (v << 8) | b;
    return v;
}

// ---------------------------------------------------------------------------
// Values
// ---------------------------------------------------------------------------

/// Length of the one value encoded at the start of `bytes`.
pub fn valueLen(bytes: []const u8) Error!usize {
    var pos: usize = 0;
    try skipValue(bytes, &pos, 0);
    return pos;
}

fn readU32(bytes: []const u8, pos: *usize) Error!u32 {
    if (bytes.len < pos.* + 4) return Error.InvalidRecord;
    const v = std.mem.readInt(u32, bytes[pos.*..][0..4], .little);
    pos.* += 4;
    return v;
}

fn skip(bytes: []const u8, pos: *usize, n: usize) Error!void {
    if (bytes.len - pos.* < n) return Error.InvalidRecord;
    pos.* += n;
}

fn skipValue(bytes: []const u8, pos: *usize, depth: usize) Error!void {
    if (depth > 64) return Error.InvalidRecord;
    if (pos.* >= bytes.len) return Error.InvalidRecord;
    const tag = bytes[pos.*];
    pos.* += 1;
    switch (tag) {
        0 => {},
        1 => try skip(bytes, pos, 1),
        2, 3 => try skip(bytes, pos, 8),
        4, 5 => try skip(bytes, pos, try readU32(bytes, pos)),
        6 => try skip(bytes, pos, @as(usize, try readU32(bytes, pos)) * 4),
        7 => {
            const count = try readU32(bytes, pos);
            for (0..count) |_| try skipValue(bytes, pos, depth + 1);
        },
        8 => {
            const count = try readU32(bytes, pos);
            for (0..count) |_| {
                try skip(bytes, pos, try readU32(bytes, pos));
                try skipValue(bytes, pos, depth + 1);
            }
        },
        else => return Error.InvalidRecord,
    }
}

const null_value = [_]u8{0};

// ---------------------------------------------------------------------------
// Encoding and decoding events
// ---------------------------------------------------------------------------

fn symbolLen(symbol: Symbol) usize {
    return switch (symbol) {
        .id => |id| varintLen(@as(u64, id) << 1),
        .name => |name| varintLen((@as(u64, name.len) << 1) | 1) + name.len,
    };
}

pub fn encodedLen(event: Event) usize {
    var n: usize = 1 + varintLen(event.id);
    if (event.commit_ms) |ms| n += varintLen(ms);
    if (event.op.hasEndpoints()) n += varintLen(event.source) + varintLen(event.target);
    if (event.op.hasSymbol()) n += symbolLen(event.symbol);
    if (event.op.isProperty()) {
        n += if (event.old_value.len == 0) 1 else event.old_value.len;
        n += if (event.new_value.len == 0) 1 else event.new_value.len;
    }
    return n;
}

/// Encode into `out`, which must be `encodedLen(event)` bytes.
pub fn encodeInto(event: Event, out: []u8) void {
    var pos: usize = 0;
    out[0] = (ENCODING_VERSION << 5) | (@as(u8, @intFromBool(event.commit_ms != null)) << 4) | @intFromEnum(event.op);
    pos = 1;
    if (event.commit_ms) |ms| pos += writeVarint(out[pos..], ms);
    pos += writeVarint(out[pos..], event.id);
    if (event.op.hasEndpoints()) {
        pos += writeVarint(out[pos..], event.source);
        pos += writeVarint(out[pos..], event.target);
    }
    if (event.op.hasSymbol()) {
        switch (event.symbol) {
            .id => |id| pos += writeVarint(out[pos..], @as(u64, id) << 1),
            .name => |name| {
                pos += writeVarint(out[pos..], (@as(u64, name.len) << 1) | 1);
                @memcpy(out[pos..][0..name.len], name);
                pos += name.len;
            },
        }
    }
    if (event.op.isProperty()) {
        for ([_][]const u8{ event.old_value, event.new_value }) |value| {
            const bytes: []const u8 = if (value.len == 0) &null_value else value;
            @memcpy(out[pos..][0..bytes.len], bytes);
            pos += bytes.len;
        }
    }
    std.debug.assert(pos == out.len);
}

pub fn encodeAlloc(allocator: Allocator, event: Event) Error![]u8 {
    const out = allocator.alloc(u8, encodedLen(event)) catch return Error.OutOfMemory;
    encodeInto(event, out);
    return out;
}

/// Whether `bytes` is a compact (version 1) event value rather than a
/// legacy record.
pub fn isCompact(bytes: []const u8) bool {
    return bytes.len > 0 and bytes[0] >> 5 == ENCODING_VERSION;
}

pub fn decode(bytes: []const u8) Error!Event {
    if (bytes.len < 2) return Error.InvalidRecord;
    if (bytes[0] >> 5 != ENCODING_VERSION) return Error.InvalidRecord;
    const op = std.enums.fromInt(Op, @as(u4, @truncate(bytes[0]))) orelse return Error.InvalidRecord;
    var event = Event{ .op = op, .id = 0 };
    var pos: usize = 1;
    if (bytes[0] & 0x10 != 0) event.commit_ms = try readVarint(bytes, &pos);
    event.id = try readVarint(bytes, &pos);
    if (op.hasEndpoints()) {
        event.source = try readVarint(bytes, &pos);
        event.target = try readVarint(bytes, &pos);
    }
    if (op.hasSymbol()) {
        const s = try readVarint(bytes, &pos);
        if (s & 1 == 0) {
            if (s >> 1 > std.math.maxInt(u16)) return Error.InvalidRecord;
            event.symbol = .{ .id = @intCast(s >> 1) };
        } else {
            const len = s >> 1;
            if (len > bytes.len - pos) return Error.InvalidRecord;
            event.symbol = .{ .name = bytes[pos..][0..@intCast(len)] };
            pos += @intCast(len);
        }
    }
    if (op.isProperty()) {
        const old_len = try valueLen(bytes[pos..]);
        event.old_value = bytes[pos..][0..old_len];
        pos += old_len;
        const new_len = try valueLen(bytes[pos..]);
        event.new_value = bytes[pos..][0..new_len];
        pos += new_len;
    }
    if (pos != bytes.len) return Error.InvalidRecord;
    return event;
}

/// The same event re-encoded with its transaction header set or cleared.
pub fn withCommitTime(allocator: Allocator, bytes: []const u8, commit_ms: ?u64) Error![]u8 {
    var event = try decode(bytes);
    event.commit_ms = commit_ms;
    return encodeAlloc(allocator, event);
}

// ---------------------------------------------------------------------------
// Records, as applications read them
// ---------------------------------------------------------------------------

/// Turns a symbol id into its name. The name is the resolver's to free with
/// `free`.
pub const Resolver = struct {
    context: *anyopaque,
    resolve: *const fn (context: *anyopaque, id: u16) Error![]const u8,
    free: *const fn (context: *anyopaque, name: []const u8) void,
};

fn idValue(id: u64) PropertyValue {
    return .{ .int_val = @bitCast(id) };
}

/// The record an application reads for `event`: the kind and payload map
/// the changefeed has always returned, field for field.
pub fn toRecord(allocator: Allocator, sequence: u64, event: Event, resolver: Resolver) Error!store.Record {
    var resolved: ?[]const u8 = null;
    defer if (resolved) |name| resolver.free(resolver.context, name);
    const symbol_name: []const u8 = if (!event.op.hasSymbol()) "" else switch (event.symbol) {
        .name => |name| name,
        .id => |id| blk: {
            resolved = try resolver.resolve(resolver.context, id);
            break :blk resolved.?;
        },
    };

    var old_value: PropertyValue = .{ .null_val = {} };
    var new_value: PropertyValue = .{ .null_val = {} };
    defer old_value.deinit(allocator);
    defer new_value.deinit(allocator);
    if (event.op.isProperty()) {
        old_value = wal_payload.deserializePropertyValueFromBytes(allocator, event.old_value) catch |err| return switch (err) {
            error.OutOfMemory => Error.OutOfMemory,
            else => Error.InvalidRecord,
        };
        new_value = wal_payload.deserializePropertyValueFromBytes(allocator, event.new_value) catch |err| return switch (err) {
            error.OutOfMemory => Error.OutOfMemory,
            else => Error.InvalidRecord,
        };
    }

    const entity: []const u8 = if (event.op.isEdge()) "edge" else "node";
    const id_key: []const u8 = if (event.op.isEdge()) "edge_id" else "node_id";
    var entries_buf: [6]PropertyValue.MapEntry = undefined;
    entries_buf[0] = .{ .key = "entity", .value = .{ .string_val = entity } };
    entries_buf[1] = .{ .key = "op", .value = .{ .string_val = event.op.opName() } };
    entries_buf[2] = .{ .key = id_key, .value = idValue(event.id) };
    var n: usize = 3;
    if (event.op.hasEndpoints()) {
        entries_buf[3] = .{ .key = "source_id", .value = idValue(event.source) };
        entries_buf[4] = .{ .key = "target_id", .value = idValue(event.target) };
        entries_buf[5] = .{ .key = "type", .value = .{ .string_val = symbol_name } };
        n = 6;
    } else if (event.op.isLabel()) {
        entries_buf[3] = .{ .key = "label", .value = .{ .string_val = symbol_name } };
        n = 4;
    } else if (event.op.isProperty()) {
        entries_buf[3] = .{ .key = "key", .value = .{ .string_val = symbol_name } };
        entries_buf[4] = .{ .key = "old_value", .value = old_value };
        entries_buf[5] = .{ .key = "new_value", .value = new_value };
        n = 6;
    }
    const borrowed = PropertyValue{ .map_val = entries_buf[0..n] };
    var payload = borrowed.clone(allocator) catch return Error.OutOfMemory;
    errdefer payload.deinit(allocator);
    const kind = allocator.dupe(u8, event.op.kind()) catch return Error.OutOfMemory;
    return .{ .sequence = sequence, .kind = kind, .payload = payload };
}

// ---------------------------------------------------------------------------
// Meta entries
// ---------------------------------------------------------------------------

/// Keys in the stream meta tree. A stream name is never empty, so a key
/// starting with 0 is no stream's.
const floor_key = "\x00changefeed.floor";
const retention_key = "\x00changefeed.retention";

fn mapBTreeError(err: BTreeError) Error {
    return switch (err) {
        BTreeError.OutOfMemory => Error.OutOfMemory,
        BTreeError.PageFull, BTreeError.ValueTooLarge => Error.ValueTooLarge,
        else => Error.IoError,
    };
}

fn mapStoreError(err: store.StreamError) Error {
    return switch (err) {
        store.StreamError.OutOfMemory => Error.OutOfMemory,
        store.StreamError.ValueTooLarge => Error.ValueTooLarge,
        store.StreamError.IoError => Error.IoError,
        else => Error.InvalidRecord,
    };
}

fn getU64(tree: *BTree, key: []const u8) Error!?u64 {
    const value = tree.get(key) catch |err| return mapBTreeError(err);
    const bytes = value orelse return null;
    defer tree.freeValue(bytes);
    if (bytes.len != 8) return Error.InvalidRecord;
    return std.mem.readInt(u64, bytes[0..8], .little);
}

fn putU64(tree: *BTree, key: []const u8, value: u64) Error!void {
    var bytes: [8]u8 = undefined;
    std.mem.writeInt(u64, &bytes, value, .little);
    tree.put(key, &bytes) catch |err| return mapBTreeError(err);
}

/// The last sequence handed to a changefeed event (0 if none ever was).
pub fn lastSequence(trees: store.TreeSet) Error!u64 {
    return (try getU64(trees.meta, last_key)) orelse 0;
}

/// The changefeed's entry in the meta tree, where a stream keeps its last
/// sequence: `[17]"__lattice_changes"`, as 0.16.0 wrote it, so sequences
/// carry on from where a 0.16.0 file left them.
const last_key = [_]u8{store.changes_stream.len} ++ store.changes_stream;

pub fn setLastSequence(trees: store.TreeSet, sequence: u64) Error!void {
    try putU64(trees.meta, last_key, sequence);
}

/// Every event with a sequence below the floor has been dropped (or was
/// never written). 1 when nothing has been.
pub fn floor(trees: store.TreeSet) Error!u64 {
    return (try getU64(trees.meta, floor_key)) orelse 1;
}

fn setFloor(trees: store.TreeSet, value: u64) Error!void {
    try putU64(trees.meta, floor_key, value);
}

// ---------------------------------------------------------------------------
// Retention policy
// ---------------------------------------------------------------------------

pub const Retention = union(enum) {
    /// Keep every event (the default).
    keep_all,
    /// Keep the newest N events.
    keep_last: u64,
    /// Keep events committed within this many milliseconds.
    keep_for_ms: u64,
    /// Write no events, and drop the ones there are.
    off,

    pub fn eql(a: Retention, b: Retention) bool {
        return switch (a) {
            .keep_all => b == .keep_all,
            .off => b == .off,
            .keep_last => |n| b == .keep_last and b.keep_last == n,
            .keep_for_ms => |ms| b == .keep_for_ms and b.keep_for_ms == ms,
        };
    }

    /// Mode numbers as the C API and the file use them.
    pub fn mode(self: Retention) u8 {
        return switch (self) {
            .keep_all => 1,
            .keep_last => 2,
            .keep_for_ms => 3,
            .off => 4,
        };
    }

    pub fn value(self: Retention) u64 {
        return switch (self) {
            .keep_last => |n| n,
            .keep_for_ms => |ms| ms,
            else => 0,
        };
    }

    /// From a mode number and its value; null for an invalid pair.
    pub fn fromMode(m: u8, v: u64) ?Retention {
        return switch (m) {
            1 => .keep_all,
            2 => if (v == 0) null else .{ .keep_last = v },
            3 => if (v == 0) null else .{ .keep_for_ms = v },
            4 => .off,
            else => null,
        };
    }

    /// Parse the text form used by the CLI and LATTICE_CHANGEFEED: `all`,
    /// `off`, `last:<events>`, `age:<duration>` with a duration such as
    /// `500ms`, `90s`, `30m`, `12h` or `7d`.
    pub fn parse(text: []const u8) ?Retention {
        if (std.ascii.eqlIgnoreCase(text, "all")) return .keep_all;
        if (std.ascii.eqlIgnoreCase(text, "off")) return .off;
        if (std.mem.startsWith(u8, text, "last:")) {
            const n = std.fmt.parseInt(u64, text[5..], 10) catch return null;
            return if (n == 0) null else .{ .keep_last = n };
        }
        if (std.mem.startsWith(u8, text, "age:")) {
            const ms = parseDurationMs(text[4..]) orelse return null;
            return if (ms == 0) null else .{ .keep_for_ms = ms };
        }
        return null;
    }

    /// The text form, as `parse` reads it.
    pub fn format(self: Retention, buf: []u8) []const u8 {
        return switch (self) {
            .keep_all => "all",
            .off => "off",
            .keep_last => |n| std.fmt.bufPrint(buf, "last:{d}", .{n}) catch "last",
            .keep_for_ms => |ms| formatDuration(buf, ms),
        };
    }
};

pub fn parseDurationMs(text: []const u8) ?u64 {
    const units = [_]struct { suffix: []const u8, ms: u64 }{
        .{ .suffix = "ms", .ms = 1 },
        .{ .suffix = "s", .ms = 1000 },
        .{ .suffix = "m", .ms = 60 * 1000 },
        .{ .suffix = "h", .ms = 60 * 60 * 1000 },
        .{ .suffix = "d", .ms = 24 * 60 * 60 * 1000 },
    };
    for (units) |unit| {
        if (text.len > unit.suffix.len and std.mem.endsWith(u8, text, unit.suffix)) {
            const number = text[0 .. text.len - unit.suffix.len];
            const n = std.fmt.parseInt(u64, number, 10) catch continue;
            return std.math.mul(u64, n, unit.ms) catch null;
        }
    }
    return null;
}

fn formatDuration(buf: []u8, ms: u64) []const u8 {
    const units = [_]struct { suffix: []const u8, ms: u64 }{
        .{ .suffix = "d", .ms = 24 * 60 * 60 * 1000 },
        .{ .suffix = "h", .ms = 60 * 60 * 1000 },
        .{ .suffix = "m", .ms = 60 * 1000 },
        .{ .suffix = "s", .ms = 1000 },
    };
    for (units) |unit| {
        if (ms % unit.ms == 0) return std.fmt.bufPrint(buf, "age:{d}{s}", .{ ms / unit.ms, unit.suffix }) catch "age";
    }
    return std.fmt.bufPrint(buf, "age:{d}ms", .{ms}) catch "age";
}

/// The policy the file records, or null if it records none (keep all).
pub fn storedRetention(trees: store.TreeSet) Error!?Retention {
    const value = trees.meta.get(retention_key) catch |err| return mapBTreeError(err);
    const bytes = value orelse return null;
    defer trees.meta.freeValue(bytes);
    if (bytes.len != 9) return Error.InvalidRecord;
    return Retention.fromMode(bytes[0], std.mem.readInt(u64, bytes[1..9], .little)) orelse Error.InvalidRecord;
}

/// Record `new` as the file's policy. Switching the feed back on after it
/// was off leaves a gap of one sequence number and raises the floor past
/// it, so a reader that stopped before the feed went off is told it missed
/// the changes made while it was off, instead of reading on as if nothing
/// had happened. Nothing is dropped here: retention acts at trim points.
pub fn storeRetention(trees: store.TreeSet, old: Retention, new: Retention) Error!void {
    var bytes: [9]u8 = undefined;
    bytes[0] = new.mode();
    std.mem.writeInt(u64, bytes[1..9], new.value(), .little);
    trees.meta.put(retention_key, &bytes) catch |err| return mapBTreeError(err);
    if (old == .off and new != .off) {
        const last = try lastSequence(trees);
        if (last == std.math.maxInt(u64) - 1) return Error.ValueTooLarge;
        try setLastSequence(trees, last + 1);
        try setFloor(trees, last + 2);
    }
}

// ---------------------------------------------------------------------------
// Writing
// ---------------------------------------------------------------------------

/// A transaction's events, encoded, waiting for their sequence numbers.
pub const Batch = struct {
    bytes: std.ArrayListUnmanaged(u8) = .empty,
    /// End offset of each event in `bytes`.
    ends: std.ArrayListUnmanaged(u32) = .empty,
    /// The symbol id each event names, 0 for none (ids start at 1000), so
    /// logging the batch need not decode it again.
    symbols: std.ArrayListUnmanaged(u16) = .empty,
    /// The first event's sequence, once assigned at commit.
    first_sequence: u64 = 0,

    pub fn deinit(self: *Batch, allocator: Allocator) void {
        self.bytes.deinit(allocator);
        self.ends.deinit(allocator);
        self.symbols.deinit(allocator);
        self.* = .{};
    }

    pub fn count(self: *const Batch) usize {
        return self.ends.items.len;
    }

    pub fn event(self: *const Batch, index: usize) []const u8 {
        const start: usize = if (index == 0) 0 else self.ends.items[index - 1];
        return self.bytes.items[start..self.ends.items[index]];
    }

    /// Append an event whose symbol, if it has one, is an id. The first
    /// event carries the transaction header.
    pub fn append(self: *Batch, allocator: Allocator, event_in: Event, commit_ms: u64) Error!void {
        var e = event_in;
        e.commit_ms = if (self.ends.items.len == 0) commit_ms else null;
        const symbol: u16 = if (e.op.hasSymbol()) switch (e.symbol) {
            .id => |id| id,
            .name => return Error.InvalidRecord,
        } else 0;
        const len = encodedLen(e);
        const start = self.bytes.items.len;
        if (start + len > std.math.maxInt(u32)) return Error.ValueTooLarge;
        self.ends.ensureUnusedCapacity(allocator, 1) catch return Error.OutOfMemory;
        self.symbols.ensureUnusedCapacity(allocator, 1) catch return Error.OutOfMemory;
        self.bytes.resize(allocator, start + len) catch return Error.OutOfMemory;
        encodeInto(e, self.bytes.items[start..]);
        self.ends.appendAssumeCapacity(@intCast(start + len));
        self.symbols.appendAssumeCapacity(symbol);
    }
};

/// Put a batch whose sequences are assigned into the tree and move the last
/// sequence on.
pub fn applyBatch(trees: store.TreeSet, batch: *const Batch) Error!void {
    if (batch.count() == 0) return;
    var key_buf: [MAX_KEY_LEN]u8 = undefined;
    for (0..batch.count()) |i| {
        const key = encodeKey(&key_buf, batch.first_sequence + i);
        trees.events.insert(key, batch.event(i)) catch |err| switch (err) {
            BTreeError.DuplicateKey => {},
            else => return mapBTreeError(err),
        };
    }
    const last = batch.first_sequence + batch.count() - 1;
    if (last > try lastSequence(trees)) try setLastSequence(trees, last);
}

// ---------------------------------------------------------------------------
// The log
// ---------------------------------------------------------------------------

/// Largest logged batch built before starting another, so a record fits a
/// log frame. A single larger event gets a record of its own, which the log
/// splits into fragments.
pub const LOG_CHUNK_BYTES: usize = 3800;

/// The names of the symbols a run of events uses, for logging them with it.
pub const SymbolNames = struct {
    ids: std.ArrayListUnmanaged(u16) = .empty,

    pub fn deinit(self: *SymbolNames, allocator: Allocator) void {
        self.ids.deinit(allocator);
    }

    fn note(self: *SymbolNames, allocator: Allocator, id: u16) Error!bool {
        if (std.mem.indexOfScalar(u16, self.ids.items, id) != null) return false;
        self.ids.append(allocator, id) catch return Error.OutOfMemory;
        return true;
    }
};

/// Encode the events `[from, to)` of `batch` as one logged record:
///
///     type 0x23, version 1, first sequence varint, event count varint,
///     symbol count varint, then per symbol: id varint, name length varint,
///     name; then per event: length varint, the event as stored.
pub fn encodeLogRecord(
    allocator: Allocator,
    batch: *const Batch,
    from: usize,
    to: usize,
    resolver: Resolver,
) Error![]u8 {
    var names: SymbolNames = .{};
    defer names.deinit(allocator);
    var symbol_bytes: std.ArrayListUnmanaged(u8) = .empty;
    defer symbol_bytes.deinit(allocator);
    for (batch.symbols.items[from..to]) |id| {
        if (id == 0 or !try names.note(allocator, id)) continue;
        const name = try resolver.resolve(resolver.context, id);
        defer resolver.free(resolver.context, name);
        try appendVarint(&symbol_bytes, allocator, id);
        try appendVarint(&symbol_bytes, allocator, name.len);
        symbol_bytes.appendSlice(allocator, name) catch return Error.OutOfMemory;
    }

    var out: std.ArrayListUnmanaged(u8) = .empty;
    errdefer out.deinit(allocator);
    out.append(allocator, @intFromEnum(wal_payload.PayloadType.changefeed_events)) catch return Error.OutOfMemory;
    out.append(allocator, LOG_VERSION) catch return Error.OutOfMemory;
    try appendVarint(&out, allocator, batch.first_sequence + from);
    try appendVarint(&out, allocator, to - from);
    try appendVarint(&out, allocator, names.ids.items.len);
    out.appendSlice(allocator, symbol_bytes.items) catch return Error.OutOfMemory;
    for (from..to) |i| {
        const bytes = batch.event(i);
        try appendVarint(&out, allocator, bytes.len);
        out.appendSlice(allocator, bytes) catch return Error.OutOfMemory;
    }
    return out.toOwnedSlice(allocator) catch Error.OutOfMemory;
}

/// Replay one logged record: intern the names it carries, map its symbol
/// ids to the recovered ones, and put every event at or above the floor
/// that is not there yet. `intern` returns the id `name` has now.
pub fn redoLogRecord(
    trees: store.TreeSet,
    allocator: Allocator,
    payload: []const u8,
    intern_context: *anyopaque,
    intern: *const fn (context: *anyopaque, name: []const u8) Error!u16,
) Error!void {
    if (payload.len < 2 or payload[0] != @intFromEnum(wal_payload.PayloadType.changefeed_events)) return Error.InvalidRecord;
    if (payload[1] != LOG_VERSION) return Error.InvalidRecord;
    var pos: usize = 2;
    const first = try readVarint(payload, &pos);
    const count = try readVarint(payload, &pos);
    const symbol_count = try readVarint(payload, &pos);
    if (first == 0 or count == 0) return Error.InvalidRecord;
    if (count > payload.len or symbol_count > payload.len) return Error.InvalidRecord;

    var map: std.AutoHashMapUnmanaged(u16, u16) = .empty;
    defer map.deinit(allocator);
    var identity = true;
    for (0..@intCast(symbol_count)) |_| {
        const logged = try readVarint(payload, &pos);
        if (logged > std.math.maxInt(u16)) return Error.InvalidRecord;
        const len = try readVarint(payload, &pos);
        if (len > payload.len - pos) return Error.InvalidRecord;
        const name = payload[pos..][0..@intCast(len)];
        pos += @intCast(len);
        const now = try intern(intern_context, name);
        if (now != logged) identity = false;
        map.put(allocator, @intCast(logged), now) catch return Error.OutOfMemory;
    }

    const low = try floor(trees);
    var key_buf: [MAX_KEY_LEN]u8 = undefined;
    var i: u64 = 0;
    while (i < count) : (i += 1) {
        const len = try readVarint(payload, &pos);
        if (len > payload.len - pos) return Error.InvalidRecord;
        const bytes = payload[pos..][0..@intCast(len)];
        pos += @intCast(len);
        var e = try decode(bytes);
        const sequence = first + i;
        if (sequence < low) continue;

        var owned: ?[]u8 = null;
        defer if (owned) |o| allocator.free(o);
        var stored = bytes;
        if (e.op.hasSymbol() and e.symbol == .id) {
            const now = map.get(e.symbol.id) orelse return Error.InvalidRecord;
            if (!identity and now != e.symbol.id) {
                e.symbol = .{ .id = now };
                owned = try encodeAlloc(allocator, e);
                stored = owned.?;
            }
        }
        trees.events.insert(encodeKey(&key_buf, sequence), stored) catch |err| switch (err) {
            BTreeError.DuplicateKey => {},
            else => return mapBTreeError(err),
        };
    }
    if (pos != payload.len) return Error.InvalidRecord;
    const last = first + count - 1;
    if (last > try lastSequence(trees)) try setLastSequence(trees, last);
}

// ---------------------------------------------------------------------------
// Reading
// ---------------------------------------------------------------------------

fn legacyKey(allocator: Allocator, sequence: u64) Error![]u8 {
    return store.encodeEventKey(allocator, store.changes_stream, sequence) catch Error.OutOfMemory;
}

fn legacyEndKey(allocator: Allocator) Error![]u8 {
    return store.encodeEventEndKey(allocator, store.changes_stream) catch Error.OutOfMemory;
}

/// Both kinds of event, in sequence order: the legacy range and the compact
/// range walked side by side.
pub const MergedIterator = struct {
    allocator: Allocator,
    legacy: BTree.Iterator,
    compact: BTree.Iterator,
    legacy_next: ?lattice.storage.btree.Entry = null,
    compact_next: ?lattice.storage.btree.Entry = null,
    legacy_seq: u64 = 0,
    compact_seq: u64 = 0,
    legacy_bounds: [2][]u8,
    pending: enum { none, legacy, compact } = .none,

    pub const Item = struct {
        sequence: u64,
        value: []const u8,
        compact: bool,
    };

    pub fn init(allocator: Allocator, events: *BTree, from_sequence: u64) Error!MergedIterator {
        const start = try legacyKey(allocator, @max(from_sequence, 1));
        errdefer allocator.free(start);
        const end = try legacyEndKey(allocator);
        errdefer allocator.free(end);
        var legacy = events.range(start, end) catch |err| return mapBTreeError(err);
        errdefer legacy.deinit();
        var key_buf: [MAX_KEY_LEN]u8 = undefined;
        // The iterator keeps the end key, so it must outlive it: a constant.
        var compact = events.range(encodeKey(&key_buf, @max(from_sequence, 1)), &end_key) catch |err| return mapBTreeError(err);
        errdefer compact.deinit();
        var self = MergedIterator{
            .allocator = allocator,
            .legacy = legacy,
            .compact = compact,
            .legacy_bounds = .{ start, end },
        };
        try self.advanceLegacy();
        try self.advanceCompact();
        return self;
    }

    pub fn deinit(self: *MergedIterator) void {
        self.legacy.deinit();
        self.compact.deinit();
        for (self.legacy_bounds) |b| self.allocator.free(b);
    }

    fn advanceLegacy(self: *MergedIterator) Error!void {
        self.legacy_next = self.legacy.next() catch |err| return mapBTreeError(err);
        if (self.legacy_next) |e| self.legacy_seq = store.decodeEventSequence(e.key) catch return Error.InvalidRecord;
    }

    fn advanceCompact(self: *MergedIterator) Error!void {
        self.compact_next = self.compact.next() catch |err| return mapBTreeError(err);
        if (self.compact_next) |e| self.compact_seq = try decodeKey(e.key);
    }

    /// The next event. Its value is valid until the following call.
    pub fn next(self: *MergedIterator) Error!?Item {
        const take_legacy = if (self.legacy_next == null)
            false
        else if (self.compact_next == null)
            true
        else
            self.legacy_seq < self.compact_seq;
        if (take_legacy) {
            // The entry's bytes stay valid until its iterator moves, which
            // is not before `consume`.
            self.pending = .legacy;
            return .{ .sequence = self.legacy_seq, .value = self.legacy_next.?.value, .compact = false };
        }
        if (self.compact_next) |e| {
            self.pending = .compact;
            return .{ .sequence = self.compact_seq, .value = e.value, .compact = true };
        }
        return null;
    }

    /// Move past the item `next` returned.
    pub fn consume(self: *MergedIterator) Error!void {
        switch (self.pending) {
            .none => {},
            .legacy => try self.advanceLegacy(),
            .compact => try self.advanceCompact(),
        }
        self.pending = .none;
    }
};

fn legacyRecord(allocator: Allocator, sequence: u64, bytes: []const u8) Error!store.Record {
    return store.deserializeRecordValue(allocator, sequence, bytes) catch |err| return mapStoreError(err);
}

/// Records from `from_sequence` on, oldest first, at most `limit`. The
/// caller decides where a reader starts (see `readStart`).
pub fn read(
    trees: store.TreeSet,
    allocator: Allocator,
    from_sequence: u64,
    limit: usize,
    resolver: Resolver,
) Error!store.Batch {
    var records: std.ArrayListUnmanaged(store.Record) = .empty;
    errdefer {
        for (records.items) |*record| record.deinit(allocator);
        records.deinit(allocator);
    }
    var iter = try MergedIterator.init(allocator, trees.events, from_sequence);
    defer iter.deinit();
    while (records.items.len < limit) {
        const item = (try iter.next()) orelse break;
        var record = if (item.compact)
            try toRecord(allocator, item.sequence, try decode(item.value), resolver)
        else
            try legacyRecord(allocator, item.sequence, item.value);
        records.append(allocator, record) catch {
            record.deinit(allocator);
            return Error.OutOfMemory;
        };
        try iter.consume();
    }
    return .{
        .allocator = allocator,
        .records = records.toOwnedSlice(allocator) catch return Error.OutOfMemory,
    };
}

pub const ReadStartError = error{
    /// Events after the reader's position have been dropped.
    Trimmed,
};

/// Where a reader that has seen everything through `after_sequence` reads
/// from next. 0 means "from the oldest event held". A reader whose next
/// event has been dropped is told so rather than silently skipped ahead:
/// it missed changes, and only it knows whether that matters.
pub fn readStart(trees: store.TreeSet, after_sequence: u64) (Error || ReadStartError)!u64 {
    const low = try floor(trees);
    if (after_sequence == 0) return low;
    if (after_sequence == std.math.maxInt(u64)) return after_sequence;
    if (after_sequence + 1 < low) return ReadStartError.Trimmed;
    return after_sequence + 1;
}

/// Sequence of the oldest event still held, or null if none is.
pub fn oldestSequence(trees: store.TreeSet, allocator: Allocator) Error!?u64 {
    // Events below the floor that a trim has not reached yet are as good
    // as gone.
    var iter = try MergedIterator.init(allocator, trees.events, try floor(trees));
    defer iter.deinit();
    const item = (try iter.next()) orelse return null;
    return item.sequence;
}

/// Events held and the bytes their keys and values take.
pub const Size = struct {
    events: u64 = 0,
    legacy_events: u64 = 0,
    bytes: u64 = 0,
};

pub fn size(trees: store.TreeSet, allocator: Allocator) Error!Size {
    var s = Size{};
    var iter = try MergedIterator.init(allocator, trees.events, 1);
    defer iter.deinit();
    var key_buf: [MAX_KEY_LEN]u8 = undefined;
    while (try iter.next()) |item| {
        s.events += 1;
        if (!item.compact) s.legacy_events += 1;
        const key_len = if (item.compact) encodeKey(&key_buf, item.sequence).len else 1 + store.changes_stream.len + 8;
        s.bytes += key_len + item.value.len;
        try iter.consume();
    }
    return s;
}

// ---------------------------------------------------------------------------
// Trimming
// ---------------------------------------------------------------------------

pub const TrimResult = struct {
    removed: u64 = 0,
    /// False if the bound stopped the pass before it reached its target.
    done: bool = true,
};

/// Drop every event below `target` and below the floor, raising the floor
/// to `target`, at most `max_events` of them. A pass cut short by its bound
/// raises the floor only to the first event it kept. If the oldest event
/// left belongs to a transaction whose head was dropped, it is rewritten to
/// carry the transaction header.
pub fn trimBelow(trees: store.TreeSet, allocator: Allocator, target_in: u64, max_events: u64) Error!TrimResult {
    var result = TrimResult{};
    const current_floor = try floor(trees);
    const target = @max(target_in, current_floor);

    var legacy_keys: std.ArrayListUnmanaged([]u8) = .empty;
    defer {
        for (legacy_keys.items) |k| allocator.free(k);
        legacy_keys.deinit(allocator);
    }
    var compact_seqs: std.ArrayListUnmanaged(u64) = .empty;
    defer compact_seqs.deinit(allocator);

    var new_floor = target;
    var txn_time: ?u64 = null;
    var first_kept: ?struct { sequence: u64, header: bool } = null;
    {
        var iter = try MergedIterator.init(allocator, trees.events, 1);
        defer iter.deinit();
        while (try iter.next()) |item| {
            if (item.sequence >= target) {
                if (item.compact) first_kept = .{ .sequence = item.sequence, .header = item.value.len > 0 and item.value[0] & 0x10 != 0 };
                break;
            }
            if (result.removed == max_events) {
                new_floor = item.sequence;
                result.done = false;
                if (item.compact) first_kept = .{ .sequence = item.sequence, .header = item.value.len > 0 and item.value[0] & 0x10 != 0 };
                break;
            }
            if (item.compact) {
                if (item.value.len > 0 and item.value[0] & 0x10 != 0) {
                    const e = try decode(item.value);
                    txn_time = e.commit_ms;
                }
                compact_seqs.append(allocator, item.sequence) catch return Error.OutOfMemory;
            } else {
                // A legacy event ends any compact transaction before it.
                txn_time = null;
                const key = try legacyKey(allocator, item.sequence);
                legacy_keys.append(allocator, key) catch {
                    allocator.free(key);
                    return Error.OutOfMemory;
                };
            }
            result.removed += 1;
            try iter.consume();
        }
    }

    // The floor first: replay must not bring back what is dropped below.
    if (new_floor > current_floor) try setFloor(trees, new_floor);
    if (result.removed == 0) return result;

    var key_buf: [MAX_KEY_LEN]u8 = undefined;
    for (compact_seqs.items) |sequence| {
        trees.events.delete(encodeKey(&key_buf, sequence)) catch |err| switch (err) {
            BTreeError.KeyNotFound => {},
            else => return mapBTreeError(err),
        };
    }
    for (legacy_keys.items) |key| {
        trees.events.delete(key) catch |err| switch (err) {
            BTreeError.KeyNotFound => {},
            else => return mapBTreeError(err),
        };
    }

    // The transaction that was cut keeps its date on its new first event.
    if (first_kept) |kept| {
        if (!kept.header and txn_time != null and compact_seqs.items.len > 0 and
            compact_seqs.items[compact_seqs.items.len - 1] + 1 == kept.sequence)
        {
            const key = encodeKey(&key_buf, kept.sequence);
            const old = (trees.events.get(key) catch |err| return mapBTreeError(err)) orelse return result;
            defer trees.events.freeValue(old);
            const rewritten = try withCommitTime(allocator, old, txn_time);
            defer allocator.free(rewritten);
            trees.events.replace(key, rewritten) catch |err| return mapBTreeError(err);
        }
    }
    return result;
}

/// Where `retention` puts the floor now: events below it are to go.
/// For an age policy, a legacy event (which carries no time) counts as
/// committed when the next dated event was, so it goes no earlier than
/// that; an event whose time cannot be told is kept. At most `max_scan`
/// events are examined.
pub fn retentionTarget(
    trees: store.TreeSet,
    allocator: Allocator,
    retention: Retention,
    now_ms: u64,
    max_scan: u64,
) Error!u64 {
    const last = try lastSequence(trees);
    const current = try floor(trees);
    return switch (retention) {
        .keep_all => current,
        .off => @max(current, last +| 1),
        .keep_last => |n| if (last > n) @max(current, last - n + 1) else current,
        .keep_for_ms => |age| blk: {
            const cutoff = now_ms -| age;
            var target = current;
            var txn_time: ?u64 = null;
            var scanned: u64 = 0;
            var iter = try MergedIterator.init(allocator, trees.events, 1);
            defer iter.deinit();
            while (try iter.next()) |item| : (scanned += 1) {
                if (scanned == max_scan) break;
                if (!item.compact) {
                    txn_time = null;
                } else {
                    const header = item.value.len > 0 and item.value[0] & 0x10 != 0;
                    if (header) txn_time = (try decode(item.value)).commit_ms;
                    const t = txn_time orelse break;
                    if (t >= cutoff) break;
                    target = item.sequence + 1;
                }
                try iter.consume();
            }
            break :blk @max(current, target);
        },
    };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "changefeed keys sort by sequence and round-trip" {
    var a: [MAX_KEY_LEN]u8 = undefined;
    var b: [MAX_KEY_LEN]u8 = undefined;
    const seqs = [_]u64{ 1, 2, 255, 256, 65535, 65536, 1 << 40, std.math.maxInt(u64) };
    for (seqs[0 .. seqs.len - 1], seqs[1..]) |x, y| {
        const ka = encodeKey(&a, x);
        const kb = encodeKey(&b, y);
        try std.testing.expect(std.mem.order(u8, ka, kb) == .lt);
        try std.testing.expect(std.mem.order(u8, kb, &end_key) == .lt);
        try std.testing.expectEqual(x, try decodeKey(ka));
    }
    try std.testing.expectEqual(@as(usize, 3), encodeKey(&a, 200).len);
    try std.testing.expectEqual(@as(usize, 5), encodeKey(&a, 1_000_000).len);
}

test "changefeed events round-trip" {
    const allocator = std.testing.allocator;
    var value_buf: [16]u8 = undefined;
    const n = try wal_payload.serializePropertyValueToBuf(&value_buf, .{ .int_val = 7 });
    const events = [_]Event{
        .{ .op = .node_insert, .id = 932, .commit_ms = 1_700_000_000_000 },
        .{ .op = .node_label_add, .id = 932, .symbol = .{ .id = 1000 } },
        .{ .op = .node_property_set, .id = 1, .symbol = .{ .name = "age" }, .new_value = value_buf[0..n] },
        .{ .op = .edge_delete, .id = 5, .source = 1, .target = 2, .symbol = .{ .id = 1001 } },
    };
    for (events) |e| {
        const bytes = try encodeAlloc(allocator, e);
        defer allocator.free(bytes);
        try std.testing.expect(isCompact(bytes));
        const d = try decode(bytes);
        try std.testing.expectEqual(e.op, d.op);
        try std.testing.expectEqual(e.id, d.id);
        try std.testing.expectEqual(e.commit_ms, d.commit_ms);
        try std.testing.expectEqual(e.source, d.source);
        try std.testing.expectEqual(e.target, d.target);
        if (e.op.isProperty()) {
            try std.testing.expectEqualSlices(u8, &[_]u8{0}, d.old_value);
            try std.testing.expectEqualSlices(u8, e.new_value, d.new_value);
        }
    }
    try std.testing.expectError(Error.InvalidRecord, decode(&[_]u8{ 0x2f, 0x01 }));
}

test "retention text forms" {
    try std.testing.expect(Retention.parse("all").? == .keep_all);
    try std.testing.expect(Retention.parse("off").? == .off);
    try std.testing.expectEqual(@as(u64, 1000), Retention.parse("last:1000").?.keep_last);
    try std.testing.expectEqual(@as(u64, 7 * 24 * 3600 * 1000), Retention.parse("age:7d").?.keep_for_ms);
    try std.testing.expectEqual(@as(u64, 500), Retention.parse("age:500ms").?.keep_for_ms);
    try std.testing.expect(Retention.parse("last:0") == null);
    try std.testing.expect(Retention.parse("age:7") == null);
    try std.testing.expect(Retention.parse("sometimes") == null);
    var buf: [32]u8 = undefined;
    try std.testing.expectEqualStrings("age:12h", (Retention{ .keep_for_ms = 12 * 3600 * 1000 }).format(&buf));
    try std.testing.expectEqualStrings("last:5", (Retention{ .keep_last = 5 }).format(&buf));
}
