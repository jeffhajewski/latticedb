//! The RDF syntax layer on its own: terms, the parser, writing terms, and
//! graph comparison. Nothing here touches a database.

pub const term = @import("term.zig");
pub const parser = @import("parser.zig");
pub const iri = @import("iri.zig");
pub const out = @import("syntax_out.zig");
pub const isomorphism = @import("isomorphism.zig");
pub const chars = @import("chars.zig");

test {
    _ = term;
    _ = parser;
    _ = iri;
    _ = out;
    _ = isomorphism;
}
