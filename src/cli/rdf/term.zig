//! RDF terms and triples, as the parser hands them over and the writer takes them.
//!
//! A term's text is borrowed: the parser keeps it alive until the statement it
//! belongs to ends, and the writer only reads it.

const std = @import("std");

pub const rdf_ns = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
pub const rdfs_ns = "http://www.w3.org/2000/01/rdf-schema#";
pub const xsd_ns = "http://www.w3.org/2001/XMLSchema#";
pub const owl_ns = "http://www.w3.org/2002/07/owl#";
/// LatticeDB's own terms: the vector datatype and the node-vector predicate.
pub const lattice_ns = "https://latticedb.org/ns#";

pub const rdf_type = rdf_ns ++ "type";
pub const rdf_first = rdf_ns ++ "first";
pub const rdf_rest = rdf_ns ++ "rest";
pub const rdf_nil = rdf_ns ++ "nil";
pub const rdf_lang_string = rdf_ns ++ "langString";
pub const rdf_json = rdf_ns ++ "JSON";
pub const rdf_statement = rdf_ns ++ "Statement";
pub const rdf_subject = rdf_ns ++ "subject";
pub const rdf_predicate = rdf_ns ++ "predicate";
pub const rdf_object = rdf_ns ++ "object";

pub const xsd_string = xsd_ns ++ "string";
pub const xsd_boolean = xsd_ns ++ "boolean";
pub const xsd_integer = xsd_ns ++ "integer";
pub const xsd_decimal = xsd_ns ++ "decimal";
pub const xsd_double = xsd_ns ++ "double";
pub const xsd_float = xsd_ns ++ "float";
pub const xsd_base64 = xsd_ns ++ "base64Binary";

pub const lattice_vector = lattice_ns ++ "vector";

pub const Literal = struct {
    lexical: []const u8,
    /// Always set: xsd:string for a plain literal, rdf:langString with a tag.
    datatype: []const u8 = xsd_string,
    lang: []const u8 = "",
};

pub const Term = union(enum) {
    iri: []const u8,
    /// A blank node's label. Labels the parser makes up for `[]` and `( )`
    /// start with '!', which no label in a file can.
    blank: []const u8,
    literal: Literal,
    /// An RDF-star quoted triple.
    triple: *const Triple,
    /// A collection whose members are all literals, handed over whole when the
    /// parser is asked to (see `Options.fold_literal_lists`). Only ever an object.
    list: []const Literal,

    pub fn eql(a: Term, b: Term) bool {
        if (std.meta.activeTag(a) != std.meta.activeTag(b)) return false;
        return switch (a) {
            .iri => |s| std.mem.eql(u8, s, b.iri),
            .blank => |s| std.mem.eql(u8, s, b.blank),
            .literal => |l| literalEql(l, b.literal),
            .triple => |t| t.eql(b.triple.*),
            .list => |items| blk: {
                if (items.len != b.list.len) break :blk false;
                for (items, b.list) |x, y| if (!literalEql(x, y)) break :blk false;
                break :blk true;
            },
        };
    }
};

pub fn literalEql(a: Literal, b: Literal) bool {
    return std.mem.eql(u8, a.lexical, b.lexical) and std.mem.eql(u8, a.datatype, b.datatype) and
        std.ascii.eqlIgnoreCase(a.lang, b.lang);
}

pub const Triple = struct {
    subject: Term,
    predicate: []const u8,
    object: Term,

    pub fn eql(a: Triple, b: Triple) bool {
        return std.mem.eql(u8, a.predicate, b.predicate) and a.subject.eql(b.subject) and a.object.eql(b.object);
    }
};

/// Where in the input something starts: a line and a column, both from 1,
/// the column counted in characters.
pub const Pos = struct {
    line: usize = 1,
    column: usize = 1,
};

pub fn isGeneratedBlank(label: []const u8) bool {
    return label.len > 0 and label[0] == '!';
}
