//! Resolving relative IRIs against a base, as RFC 3986 section 5.2 says.

const std = @import("std");

const Parts = struct {
    scheme: ?[]const u8 = null,
    authority: ?[]const u8 = null,
    path: []const u8 = "",
    query: ?[]const u8 = null,
    fragment: ?[]const u8 = null,
};

fn split(ref: []const u8) Parts {
    var parts = Parts{};
    var rest = ref;
    if (schemeEnd(rest)) |end| {
        parts.scheme = rest[0..end];
        rest = rest[end + 1 ..];
    }
    if (std.mem.startsWith(u8, rest, "//")) {
        const after = rest[2..];
        const end = std.mem.indexOfAny(u8, after, "/?#") orelse after.len;
        parts.authority = after[0..end];
        rest = after[end..];
    }
    if (std.mem.indexOfScalar(u8, rest, '#')) |hash| {
        parts.fragment = rest[hash + 1 ..];
        rest = rest[0..hash];
    }
    if (std.mem.indexOfScalar(u8, rest, '?')) |q| {
        parts.query = rest[q + 1 ..];
        rest = rest[0..q];
    }
    parts.path = rest;
    return parts;
}

/// The index of the ':' that ends a scheme, or null when `ref` has none.
fn schemeEnd(ref: []const u8) ?usize {
    if (ref.len == 0 or !std.ascii.isAlphabetic(ref[0])) return null;
    for (ref[1..], 1..) |c, i| {
        if (c == ':') return i;
        if (!(std.ascii.isAlphanumeric(c) or c == '+' or c == '-' or c == '.')) return null;
    }
    return null;
}

/// Whether an IRI is absolute: it starts with a scheme.
pub fn isAbsolute(iri: []const u8) bool {
    return schemeEnd(iri) != null;
}

/// Resolve `ref` against `base`. A `ref` that is already absolute comes back
/// with its dot segments removed, as the RFC says.
pub fn resolve(allocator: std.mem.Allocator, base: []const u8, ref: []const u8) ![]u8 {
    const r = split(ref);
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);

    if (r.scheme) |scheme| {
        try out.appendSlice(allocator, scheme);
        try out.append(allocator, ':');
        if (r.authority) |a| try appendAuthority(allocator, &out, a);
        try removeDotSegments(allocator, &out, r.path);
        try appendTail(allocator, &out, r.query, r.fragment);
        return out.toOwnedSlice(allocator);
    }

    const b = split(base);
    if (b.scheme) |scheme| {
        try out.appendSlice(allocator, scheme);
        try out.append(allocator, ':');
    }
    if (r.authority) |a| {
        try appendAuthority(allocator, &out, a);
        try removeDotSegments(allocator, &out, r.path);
        try appendTail(allocator, &out, r.query, r.fragment);
        return out.toOwnedSlice(allocator);
    }
    if (b.authority) |a| try appendAuthority(allocator, &out, a);
    if (r.path.len == 0) {
        try out.appendSlice(allocator, b.path);
        try appendTail(allocator, &out, r.query orelse b.query, r.fragment);
        return out.toOwnedSlice(allocator);
    }
    if (r.path[0] == '/') {
        try removeDotSegments(allocator, &out, r.path);
    } else {
        // Merge: the base path up to its last '/', then the reference's path.
        var merged: std.ArrayList(u8) = .empty;
        defer merged.deinit(allocator);
        if (b.authority != null and b.path.len == 0) {
            try merged.append(allocator, '/');
        } else if (std.mem.lastIndexOfScalar(u8, b.path, '/')) |slash| {
            try merged.appendSlice(allocator, b.path[0 .. slash + 1]);
        }
        try merged.appendSlice(allocator, r.path);
        try removeDotSegments(allocator, &out, merged.items);
    }
    try appendTail(allocator, &out, r.query, r.fragment);
    return out.toOwnedSlice(allocator);
}

fn appendAuthority(allocator: std.mem.Allocator, out: *std.ArrayList(u8), authority: []const u8) !void {
    try out.appendSlice(allocator, "//");
    try out.appendSlice(allocator, authority);
}

fn appendTail(allocator: std.mem.Allocator, out: *std.ArrayList(u8), query: ?[]const u8, fragment: ?[]const u8) !void {
    if (query) |q| {
        try out.append(allocator, '?');
        try out.appendSlice(allocator, q);
    }
    if (fragment) |f| {
        try out.append(allocator, '#');
        try out.appendSlice(allocator, f);
    }
}

/// RFC 3986 5.2.4, appending the result to `out`.
fn removeDotSegments(allocator: std.mem.Allocator, out: *std.ArrayList(u8), path: []const u8) !void {
    const start = out.items.len;
    var input = path;
    while (input.len > 0) {
        if (std.mem.startsWith(u8, input, "../")) {
            input = input[3..];
        } else if (std.mem.startsWith(u8, input, "./")) {
            input = input[2..];
        } else if (std.mem.startsWith(u8, input, "/./")) {
            input = input[2..];
        } else if (std.mem.eql(u8, input, "/.")) {
            input = "/";
        } else if (std.mem.startsWith(u8, input, "/../") or std.mem.eql(u8, input, "/..")) {
            input = if (input.len == 3) "/" else input[3..];
            // Drop the last segment written, and the '/' before it.
            const written = out.items[start..];
            const cut = std.mem.lastIndexOfScalar(u8, written, '/') orelse 0;
            out.shrinkRetainingCapacity(start + cut);
        } else if (std.mem.eql(u8, input, ".") or std.mem.eql(u8, input, "..")) {
            input = "";
        } else {
            // Move the first segment, with its leading '/', to the output.
            const from: usize = if (input[0] == '/') 1 else 0;
            const end = if (std.mem.indexOfScalarPos(u8, input, from, '/')) |slash| slash else input.len;
            try out.appendSlice(allocator, input[0..end]);
            input = input[end..];
        }
    }
}

test "rdf iri: RFC 3986 normal and abnormal examples" {
    const allocator = std.testing.allocator;
    const base = "http://a/b/c/d;p?q";
    const cases = [_][2][]const u8{
        .{ "g:h", "g:h" },                        .{ "g", "http://a/b/c/g" },
        .{ "./g", "http://a/b/c/g" },             .{ "g/", "http://a/b/c/g/" },
        .{ "/g", "http://a/g" },                  .{ "//g", "http://g" },
        .{ "?y", "http://a/b/c/d;p?y" },          .{ "g?y", "http://a/b/c/g?y" },
        .{ "#s", "http://a/b/c/d;p?q#s" },        .{ "g#s", "http://a/b/c/g#s" },
        .{ ";x", "http://a/b/c/;x" },             .{ "", "http://a/b/c/d;p?q" },
        .{ ".", "http://a/b/c/" },                .{ "./", "http://a/b/c/" },
        .{ "..", "http://a/b/" },                 .{ "../", "http://a/b/" },
        .{ "../g", "http://a/b/g" },              .{ "../..", "http://a/" },
        .{ "../../g", "http://a/g" },             .{ "../../../g", "http://a/g" },
        .{ "/./g", "http://a/g" },                .{ "/../g", "http://a/g" },
        .{ "g.", "http://a/b/c/g." },             .{ ".g", "http://a/b/c/.g" },
        .{ "./../g", "http://a/b/g" },            .{ "./g/.", "http://a/b/c/g/" },
        .{ "g/./h", "http://a/b/c/g/h" },         .{ "g/../h", "http://a/b/c/h" },
        .{ "g;x=1/./y", "http://a/b/c/g;x=1/y" }, .{ "g;x=1/../y", "http://a/b/c/y" },
    };
    for (cases) |case| {
        const got = try resolve(allocator, base, case[0]);
        defer allocator.free(got);
        try std.testing.expectEqualStrings(case[1], got);
    }
}
