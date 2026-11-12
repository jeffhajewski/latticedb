//! Converting between RDF literals and LatticeDB property values.

const std = @import("std");
const lattice = @import("lattice");
const term = @import("term.zig");
const vocabulary = @import("vocabulary.zig");
const isomorphism = @import("isomorphism.zig");

const PropertyValue = lattice.core.types.PropertyValue;
const Literal = term.Literal;
const Kind = vocabulary.Kind;

pub const integer_types = [_][]const u8{
    "integer",            "int",             "long",            "short",              "byte",
    "nonNegativeInteger", "positiveInteger", "negativeInteger", "nonPositiveInteger", "unsignedLong",
    "unsignedInt",        "unsignedShort",   "unsignedByte",
};

pub fn isXsd(datatype: []const u8, local: []const u8) bool {
    return datatype.len == term.xsd_ns.len + local.len and std.mem.startsWith(u8, datatype, term.xsd_ns) and
        std.mem.eql(u8, datatype[term.xsd_ns.len..], local);
}

pub fn isIntegerType(datatype: []const u8) bool {
    for (integer_types) |local| if (isXsd(datatype, local)) return true;
    return false;
}

/// A literal read into a value.
pub const Converted = struct {
    /// The kind whose datatype the record fixes, or null for values whose
    /// datatype is implied by their type (booleans, maps, vectors).
    kind: ?Kind,
    value: PropertyValue,
    /// The datatype IRI, or "@<tag>" for a language string.
    datatype_key: []const u8,
};

pub const ConvertError = error{ IntegerOutOfRange, OutOfMemory };

pub const DecimalMode = @import("options.zig").DecimalMode;

/// Read a literal. Anything that does not parse as its datatype says is kept
/// as its lexical string, with the datatype, so nothing is lost.
pub fn fromLiteral(arena: std.mem.Allocator, lit: Literal, decimal: DecimalMode) ConvertError!Converted {
    const dt = lit.datatype;
    const as_string = Converted{ .kind = .string, .value = .{ .string_val = lit.lexical }, .datatype_key = dt };
    if (lit.lang.len > 0) {
        return .{ .kind = .string, .value = .{ .string_val = lit.lexical }, .datatype_key = try std.fmt.allocPrint(arena, "@{s}", .{lit.lang}) };
    }
    if (std.mem.eql(u8, dt, term.xsd_string)) return as_string;
    if (isIntegerType(dt)) {
        if (!isIntegerLexical(lit.lexical)) return as_string;
        const v = std.fmt.parseInt(i64, lit.lexical, 10) catch return error.IntegerOutOfRange;
        return .{ .kind = .int, .value = .{ .int_val = v }, .datatype_key = dt };
    }
    if (isXsd(dt, "double") or isXsd(dt, "float")) {
        const v = isomorphism.parseXsdFloat(lit.lexical) orelse return as_string;
        return .{ .kind = .float, .value = .{ .float_val = v }, .datatype_key = dt };
    }
    if (isXsd(dt, "decimal")) {
        if (decimal == .string) return as_string;
        const canonical = canonicalDecimal(arena, lit.lexical) catch |err| switch (err) {
            error.Invalid => return as_string,
            error.OutOfMemory => return error.OutOfMemory,
        };
        const v = std.fmt.parseFloat(f64, lit.lexical) catch return as_string;
        // Only when the float is exactly the decimal written.
        const back = canonicalDecimal(arena, try formatDecimal(arena, v)) catch |err| switch (err) {
            error.Invalid => return as_string,
            error.OutOfMemory => return error.OutOfMemory,
        };
        if (!std.mem.eql(u8, canonical, back)) return as_string;
        return .{ .kind = .float, .value = .{ .float_val = v }, .datatype_key = dt };
    }
    if (isXsd(dt, "boolean")) {
        if (std.mem.eql(u8, lit.lexical, "true") or std.mem.eql(u8, lit.lexical, "1")) return .{ .kind = null, .value = .{ .bool_val = true }, .datatype_key = dt };
        if (std.mem.eql(u8, lit.lexical, "false") or std.mem.eql(u8, lit.lexical, "0")) return .{ .kind = null, .value = .{ .bool_val = false }, .datatype_key = dt };
        return as_string;
    }
    if (isXsd(dt, "base64Binary")) {
        const decoder = std.base64.standard.Decoder;
        const size = decoder.calcSizeForSlice(lit.lexical) catch return as_string;
        const bytes = try arena.alloc(u8, size);
        decoder.decode(bytes, lit.lexical) catch return as_string;
        return .{ .kind = .bytes, .value = .{ .bytes_val = bytes }, .datatype_key = dt };
    }
    if (std.mem.eql(u8, dt, term.rdf_json)) {
        const shared = @import("../import_export.zig").shared;
        const value = shared.jsonToValue(arena, lit.lexical) catch return as_string;
        // Only objects become maps, and not ones shaped like this mapping's
        // own literal and IRI forms.
        if (value != .map_val or isLiteralMap(value) or iriOfMap(value) != null) return as_string;
        return .{ .kind = null, .value = value, .datatype_key = dt };
    }
    if (std.mem.eql(u8, dt, term.lattice_vector)) {
        const vector = parseVector(arena, lit.lexical) catch return as_string;
        return .{ .kind = null, .value = .{ .vector_val = vector }, .datatype_key = dt };
    }
    return as_string;
}

fn isIntegerLexical(text: []const u8) bool {
    var digits = text;
    if (digits.len > 0 and (digits[0] == '+' or digits[0] == '-')) digits = digits[1..];
    if (digits.len == 0) return false;
    for (digits) |c| if (!std.ascii.isDigit(c)) return false;
    return true;
}

pub fn parseVector(arena: std.mem.Allocator, text: []const u8) ![]const f32 {
    const trimmed = std.mem.trim(u8, text, " \t\r\n");
    if (trimmed.len < 2 or trimmed[0] != '[' or trimmed[trimmed.len - 1] != ']') return error.Invalid;
    var list: std.ArrayList(f32) = .empty;
    var parts = std.mem.splitScalar(u8, trimmed[1 .. trimmed.len - 1], ',');
    while (parts.next()) |part| {
        const p = std.mem.trim(u8, part, " \t\r\n");
        if (p.len == 0) return error.Invalid;
        try list.append(arena, std.fmt.parseFloat(f32, p) catch return error.Invalid);
    }
    if (list.items.len == 0) return error.Invalid;
    return list.items;
}

/// A decimal written with no sign for zero, no leading zeros, no trailing
/// fractional zeros, and no '.' when there is no fraction.
fn canonicalDecimal(arena: std.mem.Allocator, text: []const u8) error{ Invalid, OutOfMemory }![]const u8 {
    var rest = text;
    var negative = false;
    if (rest.len > 0 and (rest[0] == '+' or rest[0] == '-')) {
        negative = rest[0] == '-';
        rest = rest[1..];
    }
    const dot = std.mem.indexOfScalar(u8, rest, '.');
    var int_part = if (dot) |d| rest[0..d] else rest;
    var frac_part = if (dot) |d| rest[d + 1 ..] else "";
    if (int_part.len == 0 and frac_part.len == 0) return error.Invalid;
    for (int_part) |c| if (!std.ascii.isDigit(c)) return error.Invalid;
    for (frac_part) |c| if (!std.ascii.isDigit(c)) return error.Invalid;
    int_part = std.mem.trimStart(u8, int_part, "0");
    frac_part = std.mem.trimEnd(u8, frac_part, "0");
    if (int_part.len == 0 and frac_part.len == 0) return "0";
    return std.fmt.allocPrint(arena, "{s}{s}{s}{s}{s}", .{
        if (negative) "-" else "",
        if (int_part.len == 0) "0" else int_part,
        if (frac_part.len > 0) "." else "",
        frac_part,
        "",
    });
}

/// A float in plain decimal notation, the shortest that reads back the same.
pub fn formatDecimal(arena: std.mem.Allocator, v: f64) ![]const u8 {
    const text = try std.fmt.allocPrint(arena, "{d}", .{v});
    if (std.mem.indexOfAny(u8, text, "eE") == null) return text;
    // Spell an exponent out.
    const sci = try std.fmt.allocPrint(arena, "{e}", .{v});
    const e = std.mem.indexOfAny(u8, sci, "eE").?;
    const exp = std.fmt.parseInt(i32, sci[e + 1 ..], 10) catch return text;
    var mantissa = sci[0..e];
    var negative = false;
    if (mantissa[0] == '-') {
        negative = true;
        mantissa = mantissa[1..];
    }
    var digits: std.ArrayList(u8) = .empty;
    for (mantissa) |c| if (c != '.') try digits.append(arena, c);
    // The decimal point sits after the first digit, moved by `exp`.
    const point: i64 = 1 + @as(i64, exp);
    var out: std.ArrayList(u8) = .empty;
    if (negative) try out.append(arena, '-');
    if (point <= 0) {
        try out.appendSlice(arena, "0.");
        for (0..@intCast(-point)) |_| try out.append(arena, '0');
        try out.appendSlice(arena, digits.items);
    } else if (point >= digits.items.len) {
        try out.appendSlice(arena, digits.items);
        for (0..@as(usize, @intCast(point)) - digits.items.len) |_| try out.append(arena, '0');
    } else {
        const p: usize = @intCast(point);
        try out.appendSlice(arena, digits.items[0..p]);
        try out.append(arena, '.');
        try out.appendSlice(arena, digits.items[p..]);
    }
    return out.items;
}

/// The map that keeps a literal whose datatype differs from the one its
/// property's values of that kind have: {"@value": ..., "@type"|"@language": ...}.
pub fn literalMap(arena: std.mem.Allocator, lit: Literal) !PropertyValue {
    const entries = try arena.alloc(PropertyValue.MapEntry, 2);
    entries[0] = .{ .key = "@value", .value = .{ .string_val = lit.lexical } };
    entries[1] = if (lit.lang.len > 0)
        .{ .key = "@language", .value = .{ .string_val = lit.lang } }
    else
        .{ .key = "@type", .value = .{ .string_val = lit.datatype } };
    return .{ .map_val = entries };
}

/// The map that stands for an IRI as an edge property value: {"@id": ...}.
pub fn iriMap(arena: std.mem.Allocator, iri: []const u8) !PropertyValue {
    const entries = try arena.alloc(PropertyValue.MapEntry, 1);
    entries[0] = .{ .key = "@id", .value = .{ .string_val = iri } };
    return .{ .map_val = entries };
}

/// The literal a `literalMap` holds, or null when the map is not one.
pub fn literalOfMap(value: PropertyValue) ?Literal {
    if (value != .map_val) return null;
    const entries = value.map_val;
    if (entries.len != 2 or !std.mem.eql(u8, entries[0].key, "@value") or entries[0].value != .string_val) return null;
    if (entries[1].value != .string_val) return null;
    if (std.mem.eql(u8, entries[1].key, "@type")) return .{ .lexical = entries[0].value.string_val, .datatype = entries[1].value.string_val };
    if (std.mem.eql(u8, entries[1].key, "@language")) return .{ .lexical = entries[0].value.string_val, .datatype = term.rdf_lang_string, .lang = entries[1].value.string_val };
    return null;
}

fn isLiteralMap(value: PropertyValue) bool {
    if (value != .map_val) return false;
    for (value.map_val) |e| if (std.mem.eql(u8, e.key, "@value")) return true;
    return false;
}

pub fn iriOfMap(value: PropertyValue) ?[]const u8 {
    if (value != .map_val) return null;
    const entries = value.map_val;
    if (entries.len != 1 or !std.mem.eql(u8, entries[0].key, "@id") or entries[0].value != .string_val) return null;
    return entries[0].value.string_val;
}

/// Deep equality, for dropping a value a property already holds.
pub fn valueEql(a: PropertyValue, b: PropertyValue) bool {
    if (std.meta.activeTag(a) != std.meta.activeTag(b)) return false;
    return switch (a) {
        .null_val => true,
        .bool_val => |x| x == b.bool_val,
        .int_val => |x| x == b.int_val,
        .float_val => |x| x == b.float_val or (std.math.isNan(x) and std.math.isNan(b.float_val)),
        .string_val => |x| std.mem.eql(u8, x, b.string_val),
        .bytes_val => |x| std.mem.eql(u8, x, b.bytes_val),
        .vector_val => |x| std.mem.eql(f32, x, b.vector_val),
        .list_val => |x| blk: {
            if (x.len != b.list_val.len) break :blk false;
            for (x, b.list_val) |p, q| if (!valueEql(p, q)) break :blk false;
            break :blk true;
        },
        .map_val => |x| blk: {
            if (x.len != b.map_val.len) break :blk false;
            for (x, b.map_val) |p, q| {
                if (!std.mem.eql(u8, p.key, q.key) or !valueEql(p.value, q.value)) break :blk false;
            }
            break :blk true;
        },
    };
}

test "rdf values: decimals stay floats only when exact" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const exact = try fromLiteral(arena, .{ .lexical = "1.50", .datatype = term.xsd_decimal }, .float);
    try std.testing.expectEqual(@as(f64, 1.5), exact.value.float_val);
    const long = try fromLiteral(arena, .{ .lexical = "3.14159265358979323846", .datatype = term.xsd_decimal }, .float);
    try std.testing.expectEqualStrings("3.14159265358979323846", long.value.string_val);
    const big = try fromLiteral(arena, .{ .lexical = "100000000000000000000.0", .datatype = term.xsd_decimal }, .float);
    try std.testing.expect(big.value == .float_val);
    try std.testing.expectError(error.IntegerOutOfRange, fromLiteral(arena, .{ .lexical = "9223372036854775808", .datatype = term.xsd_integer }, .float));
    const ill = try fromLiteral(arena, .{ .lexical = "abc", .datatype = term.xsd_integer }, .float);
    try std.testing.expectEqualStrings("abc", ill.value.string_val);
    try std.testing.expectEqualStrings("0.0000001", try formatDecimal(arena, 1e-7));
    try std.testing.expectEqualStrings("123000000000000000000000", try formatDecimal(arena, 1.23e23));
}
