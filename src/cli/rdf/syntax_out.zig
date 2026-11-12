//! Writing terms in N-Triples and Turtle syntax.

const std = @import("std");
const term = @import("term.zig");
const chars = @import("chars.zig");

const Term = term.Term;
const Literal = term.Literal;
const Writer = std.Io.Writer;

pub fn writeIri(w: *Writer, iri: []const u8) Writer.Error!void {
    try w.writeByte('<');
    for (iri) |c| {
        if (chars.isIriForbidden(c)) {
            try w.print("\\u{X:0>4}", .{c});
        } else try w.writeByte(c);
    }
    try w.writeByte('>');
}

/// A blank node label as written: made-up labels ("!12") become "g12", and a
/// label that is not a valid BLANK_NODE_LABEL is spelled out in hex.
pub fn writeBlank(w: *Writer, label: []const u8) Writer.Error!void {
    try w.writeAll("_:");
    if (term.isGeneratedBlank(label)) {
        try w.writeByte('g');
        return w.writeAll(label[1..]);
    }
    if (validBlankLabel(label)) return w.writeAll(label);
    try w.writeByte('x');
    for (label) |c| try w.print("{x:0>2}", .{c});
}

fn validBlankLabel(label: []const u8) bool {
    if (label.len == 0) return false;
    const view = std.unicode.Utf8View.init(label) catch return false;
    var it = view.iterator();
    var first = true;
    var last: u21 = 0;
    while (it.nextCodepoint()) |cp| {
        const ok = if (first) chars.isPnCharsU(cp) or (cp >= '0' and cp <= '9') else chars.isPnChars(cp) or cp == '.';
        if (!ok) return false;
        first = false;
        last = cp;
    }
    return last != '.';
}

pub fn writeStringBody(w: *Writer, s: []const u8) Writer.Error!void {
    for (s) |c| {
        switch (c) {
            '"' => try w.writeAll("\\\""),
            '\\' => try w.writeAll("\\\\"),
            '\n' => try w.writeAll("\\n"),
            '\r' => try w.writeAll("\\r"),
            else => try w.writeByte(c),
        }
    }
}

/// A literal in N-Triples form: always quoted, datatype in full.
pub fn writeLiteralNt(w: *Writer, lit: Literal) Writer.Error!void {
    try w.writeByte('"');
    try writeStringBody(w, lit.lexical);
    try w.writeByte('"');
    if (lit.lang.len > 0) {
        try w.writeByte('@');
        try w.writeAll(lit.lang);
    } else if (!std.mem.eql(u8, lit.datatype, term.xsd_string)) {
        try w.writeAll("^^");
        try writeIri(w, lit.datatype);
    }
}

pub fn writeTermNt(w: *Writer, t: Term) Writer.Error!void {
    switch (t) {
        .iri => |s| try writeIri(w, s),
        .blank => |s| try writeBlank(w, s),
        .literal => |l| try writeLiteralNt(w, l),
        .triple => |q| {
            try w.writeAll("<< ");
            try writeTermNt(w, q.subject);
            try w.writeByte(' ');
            try writeIri(w, q.predicate);
            try w.writeByte(' ');
            try writeTermNt(w, q.object);
            try w.writeAll(" >>");
        },
        .list => unreachable, // the parser folds lists only when asked to
    }
}

pub fn writeTripleNt(w: *Writer, t: term.Triple) Writer.Error!void {
    try writeTermNt(w, t.subject);
    try w.writeByte(' ');
    try writeIri(w, t.predicate);
    try w.writeByte(' ');
    try writeTermNt(w, t.object);
    try w.writeAll(" .\n");
}
