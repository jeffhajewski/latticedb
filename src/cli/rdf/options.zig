//! The command-line options of RDF import and export.

const std = @import("std");

pub const Syntax = @import("parser.zig").Syntax;

pub const TypesMode = enum { auto, labels, edges };
pub const DecimalMode = enum { float, string };
pub const EdgePropertiesMode = enum { star, reify, drop };
pub const VectorsMode = enum { include, omit };

pub const Options = struct {
    /// From --format=turtle|ntriples; otherwise the file's extension decides.
    syntax: ?Syntax = null,
    types: TypesMode = .auto,
    lang: []const u8 = "en",
    decimal: DecimalMode = .float,
    edge_properties: EdgePropertiesMode = .star,
    vectors: VectorsMode = .include,
    /// Import: the base for relative IRIs. Export: the base for minted IRIs.
    base: ?[]const u8 = null,
    /// Import: properties of `Resource` to declare full-text indexes on.
    fts: ?[]const u8 = null,
};

pub const ParseError = error{InvalidRdfOption};

/// The RDF file syntax a --format value names, if it names one.
pub fn syntaxFromName(name: []const u8) ?Syntax {
    if (std.mem.eql(u8, name, "turtle") or std.mem.eql(u8, name, "ttl")) return .turtle;
    if (std.mem.eql(u8, name, "ntriples") or std.mem.eql(u8, name, "nt") or std.mem.eql(u8, name, "n-triples")) return .ntriples;
    return null;
}

/// The syntax a file's name implies, or null when it is not an RDF file.
pub fn syntaxFromPath(path: []const u8) ?Syntax {
    const ext = std.fs.path.extension(path);
    if (std.ascii.eqlIgnoreCase(ext, ".ttl") or std.ascii.eqlIgnoreCase(ext, ".turtle")) return .turtle;
    if (std.ascii.eqlIgnoreCase(ext, ".nt") or std.ascii.eqlIgnoreCase(ext, ".ntriples")) return .ntriples;
    return null;
}

fn enumValue(comptime E: type, arg: []const u8, comptime flag: []const u8) ParseError!?E {
    if (!std.mem.startsWith(u8, arg, flag)) return null;
    return std.meta.stringToEnum(E, arg[flag.len..]) orelse error.InvalidRdfOption;
}

/// Read one RDF option into `options`. False when `arg` is not one.
pub fn parseArg(options: *Options, arg: []const u8) ParseError!bool {
    if (try enumValue(TypesMode, arg, "--rdf-types=")) |v| {
        options.types = v;
    } else if (try enumValue(DecimalMode, arg, "--rdf-decimal=")) |v| {
        options.decimal = v;
    } else if (try enumValue(EdgePropertiesMode, arg, "--rdf-edge-properties=")) |v| {
        options.edge_properties = v;
    } else if (try enumValue(VectorsMode, arg, "--rdf-vectors=")) |v| {
        options.vectors = v;
    } else if (std.mem.startsWith(u8, arg, "--rdf-lang=")) {
        const tag = arg["--rdf-lang=".len..];
        if (!isLanguageTag(tag)) return error.InvalidRdfOption;
        options.lang = tag;
    } else if (std.mem.startsWith(u8, arg, "--rdf-fts=")) {
        options.fts = arg["--rdf-fts=".len..];
    } else if (std.mem.startsWith(u8, arg, "--base=")) {
        const base = arg["--base=".len..];
        if (!@import("iri.zig").isAbsolute(base)) return error.InvalidRdfOption;
        options.base = base;
    } else return false;
    return true;
}

fn isLanguageTag(tag: []const u8) bool {
    if (tag.len == 0) return false;
    var parts = std.mem.splitScalar(u8, tag, '-');
    var first = true;
    while (parts.next()) |part| {
        if (part.len == 0) return false;
        for (part) |c| {
            if (!(std.ascii.isAlphabetic(c) or (!first and std.ascii.isDigit(c)))) return false;
        }
        first = false;
    }
    return true;
}

test "rdf options: values are checked" {
    var o = Options{};
    try std.testing.expect(try parseArg(&o, "--rdf-types=edges"));
    try std.testing.expectEqual(TypesMode.edges, o.types);
    try std.testing.expect(try parseArg(&o, "--rdf-lang=de-CH"));
    try std.testing.expectEqualStrings("de-CH", o.lang);
    try std.testing.expectError(error.InvalidRdfOption, parseArg(&o, "--rdf-types=sometimes"));
    try std.testing.expectError(error.InvalidRdfOption, parseArg(&o, "--base=relative/path"));
    try std.testing.expect(!try parseArg(&o, "--labels=Person"));
    try std.testing.expectEqual(Syntax.turtle, syntaxFromPath("x/Onto.TTL").?);
    try std.testing.expectEqual(@as(?Syntax, null), syntaxFromPath("x.json"));
}
