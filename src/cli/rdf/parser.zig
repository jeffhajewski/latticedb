//! A streaming parser for Turtle 1.1 and N-Triples, with the RDF-star
//! extensions (`<< s p o >>` and `{| ... |}`).
//!
//! It reads one statement at a time and hands each triple to a sink as soon
//! as it is complete. What it keeps between statements is the prefix map, the
//! base and a counter for made-up blank node labels; everything a statement
//! needs lives in an arena that is reset when the next one starts, so memory
//! is bounded by the largest statement, not by the file.
//!
//! The sink is any type with these methods:
//!   fn onTriple(self, triple: Triple, at: Pos) !void
//!   fn onPrefix(self, prefix: []const u8, namespace: []const u8) !void
//!   fn onStatementEnd(self) !void

const std = @import("std");
const term = @import("term.zig");
const chars = @import("chars.zig");
const iri_mod = @import("iri.zig");
const source_mod = @import("source.zig");

const Term = term.Term;
const Triple = term.Triple;
const Literal = term.Literal;
const Pos = term.Pos;
pub const Source = source_mod.Source;
pub const SliceReader = source_mod.SliceReader;

pub const Syntax = enum { turtle, ntriples };

pub const Options = struct {
    syntax: Syntax = .turtle,
    /// The base relative IRIs resolve against until an @base says otherwise.
    base: []const u8 = "",
    /// Hand a collection whose members are all literals, in object position,
    /// over as one `.list` term instead of rdf:first/rdf:rest triples.
    fold_literal_lists: bool = false,
};

/// What a syntax error was, and where.
pub const Diagnostic = struct {
    pos: Pos = .{},
    message: [160]u8 = undefined,
    message_len: usize = 0,
    token: [48]u8 = undefined,
    token_len: usize = 0,

    pub fn messageText(self: *const Diagnostic) []const u8 {
        return self.message[0..self.message_len];
    }

    pub fn tokenText(self: *const Diagnostic) []const u8 {
        return self.token[0..self.token_len];
    }
};

pub const Error = error{SyntaxError} || std.mem.Allocator.Error;

/// Where a term is being read, which decides what it may be.
const Place = enum { subject, object, list_member, quoted, annotation };

pub fn Parser(comptime Sink: type) type {
    return struct {
        allocator: std.mem.Allocator,
        arena: std.heap.ArenaAllocator,
        sink: Sink,
        options: Options,
        src: Source,
        prefixes: std.StringHashMapUnmanaged([]const u8) = .empty,
        base: []u8,
        blank_counter: u64 = 0,
        token: std.ArrayList(u8) = .empty,
        diag: Diagnostic = .{},

        const Self = @This();

        pub fn init(
            allocator: std.mem.Allocator,
            sink: Sink,
            ctx: *anyopaque,
            read_fn: source_mod.ReadFn,
            options: Options,
        ) !Self {
            const base = try allocator.dupe(u8, options.base);
            errdefer allocator.free(base);
            return .{
                .allocator = allocator,
                .arena = std.heap.ArenaAllocator.init(allocator),
                .sink = sink,
                .options = options,
                .src = try Source.init(allocator, ctx, read_fn),
                .base = base,
            };
        }

        pub fn deinit(self: *Self) void {
            var it = self.prefixes.iterator();
            while (it.next()) |entry| {
                self.allocator.free(entry.key_ptr.*);
                self.allocator.free(entry.value_ptr.*);
            }
            self.prefixes.deinit(self.allocator);
            self.allocator.free(self.base);
            self.token.deinit(self.allocator);
            self.src.deinit(self.allocator);
            self.arena.deinit();
        }

        /// Parse every statement, stopping at the first error.
        pub fn parseAll(self: *Self) anyerror!void {
            while (try self.parseStatement()) {}
        }

        /// Parse one statement and hand over its triples. False at the end of
        /// the input.
        pub fn parseStatement(self: *Self) anyerror!bool {
            _ = self.arena.reset(.retain_capacity);
            try self.skipWs(true);
            if (try self.peek(0) == null) return false;
            switch (self.options.syntax) {
                .turtle => try self.turtleStatement(),
                .ntriples => try self.ntriplesStatement(),
            }
            try self.sink.onStatementEnd();
            return true;
        }

        /// After a syntax error in N-Triples, skip to the start of the next
        /// line so the rest of the file can still be read.
        pub fn skipLine(self: *Self) !void {
            while (try self.peek(0)) |c| {
                self.src.advance();
                if (c == '\n') return;
            }
        }

        pub fn position(self: *const Self) Pos {
            return self.src.pos;
        }

        // ------------------------------------------------------------------
        // Errors

        fn fail(self: *Self, comptime fmt: []const u8, args: anytype) error{SyntaxError} {
            return self.failAt(self.src.pos, fmt, args);
        }

        fn failAt(self: *Self, pos: Pos, comptime fmt: []const u8, args: anytype) error{SyntaxError} {
            self.diag.pos = pos;
            const msg = std.fmt.bufPrint(&self.diag.message, fmt, args) catch blk: {
                const m = self.diag.message[0 .. self.diag.message.len - 3];
                @memcpy(self.diag.message[m.len..], "...");
                break :blk self.diag.message[0..];
            };
            self.diag.message_len = msg.len;
            // The offending token: what follows, up to whitespace.
            const ahead = self.src.preview(self.diag.token.len);
            var n: usize = 0;
            while (n < ahead.len and !std.ascii.isWhitespace(ahead[n])) : (n += 1) {}
            if (n == 0 and ahead.len > 0) n = 1;
            @memcpy(self.diag.token[0..n], ahead[0..n]);
            self.diag.token_len = n;
            return error.SyntaxError;
        }

        // ------------------------------------------------------------------
        // Input

        inline fn peek(self: *Self, n: usize) !?u8 {
            return self.src.peek(n);
        }

        fn peekIs(self: *Self, n: usize, c: u8) !bool {
            return (try self.peek(n)) == c;
        }

        fn expect(self: *Self, c: u8, comptime what: []const u8) !void {
            if (try self.peek(0) != c) return self.fail("expected " ++ what, .{});
            self.src.advance();
        }

        /// The code point at `n` bytes ahead and its length in bytes.
        fn peekCodepoint(self: *Self, n: usize) !?struct { cp: u21, len: u3 } {
            const first = (try self.peek(n)) orelse return null;
            const len = std.unicode.utf8ByteSequenceLength(first) catch return self.fail("invalid UTF-8", .{});
            var bytes: [4]u8 = undefined;
            bytes[0] = first;
            for (1..len) |i| bytes[i] = (try self.peek(n + i)) orelse return self.fail("invalid UTF-8", .{});
            const cp = std.unicode.utf8Decode(bytes[0..len]) catch return self.fail("invalid UTF-8", .{});
            return .{ .cp = cp, .len = len };
        }

        /// Skip whitespace and comments. In N-Triples a line break ends a
        /// statement, so inside one `cross_lines` is false and a break is an error.
        fn skipWs(self: *Self, cross_lines: bool) !void {
            while (try self.peek(0)) |c| {
                switch (c) {
                    ' ', '\t' => self.src.advance(),
                    '\r', '\n' => {
                        if (!cross_lines) return self.fail("a triple must end with '.' on the line it starts", .{});
                        self.src.advance();
                    },
                    '#' => while (try self.peek(0)) |d| {
                        if (d == '\n' or d == '\r') break;
                        self.src.advance();
                    },
                    else => return,
                }
            }
        }

        fn ws(self: *Self) !void {
            return self.skipWs(self.options.syntax == .turtle);
        }

        fn dupe(self: *Self, bytes: []const u8) ![]const u8 {
            return self.arena.allocator().dupe(u8, bytes);
        }

        fn newBlank(self: *Self) ![]const u8 {
            self.blank_counter += 1;
            return std.fmt.allocPrint(self.arena.allocator(), "!{d}", .{self.blank_counter});
        }

        fn emit(self: *Self, subject: Term, predicate: []const u8, object: Term, at: Pos) !void {
            try self.sink.onTriple(.{ .subject = subject, .predicate = predicate, .object = object }, at);
        }

        // ------------------------------------------------------------------
        // Statements

        fn turtleStatement(self: *Self) !void {
            const c = (try self.peek(0)).?;
            if (c == '@') {
                if (try self.matchWord("@prefix", true)) return self.prefixDirective(true);
                if (try self.matchWord("@base", true)) return self.baseDirective(true);
                return self.fail("expected @prefix or @base", .{});
            }
            if (c == 'P' or c == 'p') {
                if (try self.matchWord("PREFIX", false)) return self.prefixDirective(false);
            }
            if (c == 'B' or c == 'b') {
                if (try self.matchWord("BASE", false)) return self.baseDirective(false);
            }
            try self.triples();
            try self.ws();
            try self.expect('.', "'.' to end the statement");
        }

        /// Consume `word` when it stands alone here (followed by whitespace,
        /// '<', or the end), ignoring case unless `exact`.
        fn matchWord(self: *Self, comptime word: []const u8, comptime exact: bool) !bool {
            inline for (word, 0..) |w, i| {
                const c = (try self.peek(i)) orelse return false;
                if (exact) {
                    if (c != w) return false;
                } else if (std.ascii.toLower(c) != std.ascii.toLower(w)) return false;
            }
            const after = try self.peek(word.len);
            if (after) |a| {
                if (!(std.ascii.isWhitespace(a) or a == '<' or a == '#')) {
                    // `@prefix` must be followed by a space; a keyword
                    // followed by ':' is a prefixed name instead.
                    if (exact) return self.fail("unknown directive", .{});
                    return false;
                }
            }
            self.src.advanceN(word.len);
            return true;
        }

        fn prefixDirective(self: *Self, dotted: bool) !void {
            try self.ws();
            const at = self.src.pos;
            self.token.clearRetainingCapacity();
            try self.readPrefixName();
            if (try self.peek(0) != ':') return self.failAt(at, "expected a prefix name ending in ':'", .{});
            self.src.advance();
            const prefix = try self.allocator.dupe(u8, self.token.items);
            errdefer self.allocator.free(prefix);
            try self.ws();
            const namespace = try self.allocator.dupe(u8, try self.iriRef());
            errdefer self.allocator.free(namespace);
            if (dotted) {
                try self.ws();
                try self.expect('.', "'.' after @prefix");
            }
            const gop = try self.prefixes.getOrPut(self.allocator, prefix);
            if (gop.found_existing) {
                self.allocator.free(prefix);
                self.allocator.free(gop.value_ptr.*);
            }
            gop.value_ptr.* = namespace;
            try self.sink.onPrefix(gop.key_ptr.*, namespace);
        }

        fn baseDirective(self: *Self, dotted: bool) !void {
            try self.ws();
            const resolved = try self.allocator.dupe(u8, try self.iriRef());
            self.allocator.free(self.base);
            self.base = resolved;
            if (dotted) {
                try self.ws();
                try self.expect('.', "'.' after @base");
            }
        }

        fn triples(self: *Self) !void {
            const c = (try self.peek(0)).?;
            if (c == '[') {
                const at = self.src.pos;
                const was_anon = try self.isAnon();
                const subject = try self.blankNodePropertyList();
                try self.ws();
                if (!was_anon and try self.peek(0) == '.') return;
                _ = at;
                return self.predicateObjectList(.{ .blank = subject });
            }
            const subject = try self.readTerm(.subject);
            try self.ws();
            try self.predicateObjectList(subject);
        }

        fn ntriplesStatement(self: *Self) !void {
            const subject = try self.readTerm(.subject);
            try self.skipWs(false);
            const at_pred = self.src.pos;
            if (try self.peek(0) != '<') return self.fail("expected a predicate IRI", .{});
            const predicate = try self.iriRef();
            _ = at_pred;
            try self.skipWs(false);
            const at = self.src.pos;
            const object = try self.readTerm(.object);
            try self.skipWs(false);
            try self.expect('.', "'.' to end the triple");
            try self.emit(subject, predicate, object, at);
            // Nothing else may share the line.
            while (try self.peek(0)) |c| {
                switch (c) {
                    ' ', '\t' => self.src.advance(),
                    '#' => while (try self.peek(0)) |d| {
                        if (d == '\n' or d == '\r') break;
                        self.src.advance();
                    },
                    '\r', '\n' => return,
                    else => return self.fail("expected the end of the line after '.'", .{}),
                }
            }
        }

        fn predicateObjectList(self: *Self, subject: Term) anyerror!void {
            while (true) {
                const predicate = try self.verb();
                try self.ws();
                try self.objectList(subject, predicate);
                try self.ws();
                if (try self.peek(0) != ';') return;
                while (try self.peek(0) == ';') {
                    self.src.advance();
                    try self.ws();
                }
                const next = (try self.peek(0)) orelse return;
                if (next == '.' or next == ']' or next == '|' or next == '}') return;
            }
        }

        fn objectList(self: *Self, subject: Term, predicate: []const u8) anyerror!void {
            while (true) {
                const at = self.src.pos;
                const object = try self.readTerm(.object);
                try self.emit(subject, predicate, object, at);
                try self.ws();
                if (try self.peek(0) == '{' and try self.peekIs(1, '|')) {
                    self.src.advanceN(2);
                    try self.ws();
                    const quoted = try self.arena.allocator().create(Triple);
                    quoted.* = .{ .subject = subject, .predicate = predicate, .object = object };
                    if (object == .list) return self.failAt(at, "a collection cannot be annotated", .{});
                    try self.predicateObjectList(.{ .triple = quoted });
                    try self.ws();
                    if (!(try self.peek(0) == '|' and try self.peekIs(1, '}'))) return self.fail("expected '|}}' to close the annotation", .{});
                    self.src.advanceN(2);
                    try self.ws();
                }
                if (try self.peek(0) != ',') return;
                self.src.advance();
                try self.ws();
            }
        }

        fn verb(self: *Self) ![]const u8 {
            const c = (try self.peek(0)) orelse return self.fail("expected a predicate", .{});
            if (c == 'a') {
                const after = try self.peek(1);
                if (after == null or std.ascii.isWhitespace(after.?) or after.? == '<' or after.? == '[' or
                    after.? == '"' or after.? == '\'' or after.? == '(' or after.? == '_' or after.? == '#')
                {
                    self.src.advance();
                    return term.rdf_type;
                }
            }
            if (c == '<') {
                if (try self.peekIs(1, '<')) return self.fail("a quoted triple cannot be a predicate", .{});
                return self.iriRef();
            }
            return self.prefixedName() catch |err| switch (err) {
                error.NotAName => self.fail("expected a predicate", .{}),
                else => err,
            };
        }

        // ------------------------------------------------------------------
        // Terms

        fn readTerm(self: *Self, place: Place) anyerror!Term {
            const nt = self.options.syntax == .ntriples;
            const c = (try self.peek(0)) orelse return self.fail("unexpected end of input", .{});
            switch (c) {
                '<' => {
                    if (try self.peekIs(1, '<')) {
                        if (place == .list_member) return self.fail("a quoted triple cannot be a collection member", .{});
                        return .{ .triple = try self.quotedTriple() };
                    }
                    return .{ .iri = try self.iriRef() };
                },
                '_' => return .{ .blank = try self.blankLabel() },
                '"' => {
                    if (place == .subject) return self.fail("a literal cannot be a subject", .{});
                    return .{ .literal = try self.literal() };
                },
                '\'' => {
                    if (nt) return self.fail("N-Triples strings use double quotes", .{});
                    if (place == .subject) return self.fail("a literal cannot be a subject", .{});
                    return .{ .literal = try self.literal() };
                },
                else => {},
            }
            if (nt) return self.fail("expected an IRI, a blank node or a literal", .{});
            switch (c) {
                '[' => {
                    if (place == .quoted) {
                        if (!try self.isAnon()) return self.fail("only [] may appear in a quoted triple", .{});
                    }
                    return .{ .blank = try self.blankNodePropertyList() };
                },
                '(' => {
                    if (place == .quoted) return self.fail("a collection cannot appear in a quoted triple", .{});
                    return self.collection(place == .object and self.options.fold_literal_lists);
                },
                '0'...'9', '+', '-', '.' => {
                    if (place == .subject) return self.fail("a number cannot be a subject", .{});
                    return .{ .literal = try self.number() };
                },
                else => {},
            }
            if (place != .subject) {
                if (try self.keyword("true")) return .{ .literal = .{ .lexical = "true", .datatype = term.xsd_boolean } };
                if (try self.keyword("false")) return .{ .literal = .{ .lexical = "false", .datatype = term.xsd_boolean } };
            }
            const name = self.prefixedName() catch |err| switch (err) {
                error.NotAName => return self.fail("expected an IRI, a blank node or a literal", .{}),
                else => return err,
            };
            return .{ .iri = name };
        }

        /// `word` standing alone (not the start of a prefixed name).
        fn keyword(self: *Self, comptime word: []const u8) !bool {
            inline for (word, 0..) |w, i| {
                if (try self.peek(i) != w) return false;
            }
            if (try self.peekCodepoint(word.len)) |next| {
                if (chars.isPnChars(next.cp) or next.cp == ':') return false;
            }
            self.src.advanceN(word.len);
            return true;
        }

        /// `[]` with only whitespace inside, without consuming it.
        fn isAnon(self: *Self) !bool {
            var i: usize = 1;
            while (try self.peek(i)) |c| : (i += 1) {
                if (c == ']') return true;
                if (!(c == ' ' or c == '\t' or c == '\r' or c == '\n')) return false;
            }
            return false;
        }

        fn blankNodePropertyList(self: *Self) anyerror![]const u8 {
            self.src.advance(); // '['
            try self.ws();
            const label = try self.newBlank();
            if (try self.peek(0) == ']') {
                self.src.advance();
                return label;
            }
            try self.predicateObjectList(.{ .blank = label });
            try self.ws();
            try self.expect(']', "']' to close the blank node");
            return label;
        }

        fn collection(self: *Self, fold: bool) anyerror!Term {
            self.src.advance(); // '('
            var members: std.ArrayList(Term) = .empty;
            var positions: std.ArrayList(Pos) = .empty;
            const arena = self.arena.allocator();
            try self.ws();
            while (try self.peek(0) != ')') {
                if (try self.peek(0) == null) return self.fail("expected ')' to close the collection", .{});
                try positions.append(arena, self.src.pos);
                try members.append(arena, try self.readTerm(.list_member));
                try self.ws();
            }
            self.src.advance(); // ')'
            if (members.items.len == 0) return .{ .iri = term.rdf_nil };

            if (fold) {
                var all_literals = true;
                for (members.items) |m| {
                    if (m != .literal) all_literals = false;
                }
                if (all_literals) {
                    const items = try arena.alloc(Literal, members.items.len);
                    for (members.items, items) |m, *item| item.* = m.literal;
                    return .{ .list = items };
                }
            }

            const head = try self.newBlank();
            var node = head;
            for (members.items, positions.items, 0..) |m, at, i| {
                try self.emit(.{ .blank = node }, term.rdf_first, m, at);
                const rest: Term = if (i + 1 == members.items.len) .{ .iri = term.rdf_nil } else .{ .blank = try self.newBlank() };
                try self.emit(.{ .blank = node }, term.rdf_rest, rest, at);
                if (rest == .blank) node = rest.blank;
            }
            return .{ .blank = head };
        }

        fn quotedTriple(self: *Self) anyerror!*const Triple {
            self.src.advanceN(2); // '<<'
            try self.ws();
            const subject = try self.readTerm(.subject);
            try self.ws();
            const predicate = if (self.options.syntax == .ntriples) blk: {
                if (try self.peek(0) != '<') return self.fail("expected a predicate IRI", .{});
                break :blk try self.iriRef();
            } else try self.verb();
            try self.ws();
            const object = try self.readTerm(.quoted);
            try self.ws();
            if (!(try self.peek(0) == '>' and try self.peekIs(1, '>'))) return self.fail("expected '>>' to close the quoted triple", .{});
            self.src.advanceN(2);
            const quoted = try self.arena.allocator().create(Triple);
            quoted.* = .{ .subject = subject, .predicate = predicate, .object = object };
            return quoted;
        }

        fn blankLabel(self: *Self) ![]const u8 {
            if (try self.peek(1) != ':') return self.fail("expected '_:' to start a blank node label", .{});
            self.src.advanceN(2);
            self.token.clearRetainingCapacity();
            const first = (try self.peekCodepoint(0)) orelse return self.fail("expected a blank node label", .{});
            if (!(chars.isPnCharsU(first.cp) or (first.cp >= '0' and first.cp <= '9'))) {
                return self.fail("a blank node label cannot start with this character", .{});
            }
            try self.takeCodepoint(first.len);
            try self.readNameTail(false);
            return self.dupe(self.token.items);
        }

        fn takeCodepoint(self: *Self, len: u3) !void {
            for (0..len) |_| {
                try self.token.append(self.allocator, (try self.peek(0)).?);
                self.src.advance();
            }
        }

        /// The rest of a name made of PN_CHARS and inner dots: a dot is taken
        /// only when a name character follows it. With `local`, also ':' and
        /// the escapes of a local name.
        fn readNameTail(self: *Self, local: bool) !void {
            while (true) {
                const c = (try self.peek(0)) orelse return;
                if (c == '.') {
                    var n: usize = 1;
                    while (try self.peek(n) == '.') n += 1;
                    const after = (try self.peekCodepoint(n)) orelse return;
                    const ok = chars.isPnChars(after.cp) or (local and (after.cp == ':' or after.cp == '%' or after.cp == '\\'));
                    if (!ok) return;
                    for (0..n) |_| {
                        try self.token.append(self.allocator, '.');
                        self.src.advance();
                    }
                    continue;
                }
                if (local and c == ':') {
                    try self.token.append(self.allocator, ':');
                    self.src.advance();
                    continue;
                }
                if (local and (c == '%' or c == '\\')) {
                    try self.localEscape();
                    continue;
                }
                const cp = (try self.peekCodepoint(0)) orelse return;
                if (!chars.isPnChars(cp.cp)) return;
                try self.takeCodepoint(cp.len);
            }
        }

        fn localEscape(self: *Self) !void {
            const c = (try self.peek(0)).?;
            if (c == '%') {
                const h1 = (try self.peek(1)) orelse 0;
                const h2 = (try self.peek(2)) orelse 0;
                if (!chars.isHex(h1) or !chars.isHex(h2)) return self.fail("'%' must be followed by two hex digits", .{});
                try self.token.appendSlice(self.allocator, &.{ '%', h1, h2 });
                self.src.advanceN(3);
                return;
            }
            const e = (try self.peek(1)) orelse 0;
            if (!chars.isLocalEscape(e)) return self.fail("invalid escape in a local name", .{});
            try self.token.append(self.allocator, e);
            self.src.advanceN(2);
        }

        /// PN_PREFIX into `token`, possibly empty.
        fn readPrefixName(self: *Self) !void {
            const first = (try self.peekCodepoint(0)) orelse return;
            if (first.cp == ':') return;
            if (!chars.isPnCharsBase(first.cp)) return self.fail("invalid prefix name", .{});
            try self.takeCodepoint(first.len);
            try self.readNameTail(false);
        }

        fn prefixedName(self: *Self) (anyerror || error{NotAName})![]const u8 {
            const at = self.src.pos;
            self.token.clearRetainingCapacity();
            const first = (try self.peekCodepoint(0)) orelse return error.NotAName;
            if (first.cp != ':' and !chars.isPnCharsBase(first.cp)) return error.NotAName;
            try self.readPrefixName();
            if (try self.peek(0) != ':') return self.fail("expected ':' in a prefixed name", .{});
            self.src.advance();
            const namespace = self.prefixes.get(self.token.items) orelse
                return self.failAt(at, "prefix '{s}:' is not declared", .{self.token.items});
            self.token.clearRetainingCapacity();
            try self.token.appendSlice(self.allocator, namespace);
            // PN_LOCAL: the first character may also be ':' or a digit.
            if (try self.peekCodepoint(0)) |c| {
                if (chars.isPnCharsU(c.cp) or c.cp == ':' or (c.cp >= '0' and c.cp <= '9')) {
                    try self.takeCodepoint(c.len);
                    try self.readNameTail(true);
                } else if (c.cp == '%' or c.cp == '\\') {
                    try self.localEscape();
                    try self.readNameTail(true);
                }
            }
            return self.dupe(self.token.items);
        }

        /// An IRIREF, unescaped and resolved against the base.
        fn iriRef(self: *Self) ![]const u8 {
            const at = self.src.pos;
            try self.expect('<', "'<' to start an IRI");
            self.token.clearRetainingCapacity();
            while (true) {
                const c = (try self.peek(0)) orelse return self.fail("unterminated IRI", .{});
                if (c == '>') {
                    self.src.advance();
                    break;
                }
                if (c == '\\') {
                    const e = (try self.peek(1)) orelse 0;
                    if (e != 'u' and e != 'U') return self.fail("only \\u and \\U escapes are allowed in an IRI", .{});
                    const cp = try self.uchar();
                    if (cp <= 0x20 or (cp < 0x80 and chars.isIriForbidden(@intCast(cp)))) {
                        return self.failAt(at, "an IRI cannot contain this character", .{});
                    }
                    try self.appendCodepoint(cp);
                    continue;
                }
                if (chars.isIriForbidden(c)) return self.fail("an IRI cannot contain this character", .{});
                try self.token.append(self.allocator, c);
                self.src.advance();
            }
            if (iri_mod.isAbsolute(self.token.items)) return self.dupe(self.token.items);
            if (self.options.syntax == .ntriples) return self.failAt(at, "N-Triples IRIs must be absolute", .{});
            if (self.base.len == 0) return self.failAt(at, "relative IRI <{s}> with no base to resolve it against", .{self.token.items});
            const resolved = try iri_mod.resolve(self.arena.allocator(), self.base, self.token.items);
            return resolved;
        }

        /// A \uXXXX or \UXXXXXXXX escape, at the backslash.
        fn uchar(self: *Self) !u21 {
            const n_digits: usize = if ((try self.peek(1)).? == 'u') 4 else 8;
            var value: u32 = 0;
            for (0..n_digits) |i| {
                const h = (try self.peek(2 + i)) orelse 0;
                if (!chars.isHex(h)) return self.fail("invalid \\u escape", .{});
                value = value * 16 + (std.fmt.charToDigit(h, 16) catch unreachable);
            }
            if (value > 0x10FFFF or (value >= 0xD800 and value <= 0xDFFF)) return self.fail("escape names no Unicode character", .{});
            self.src.advanceN(2 + n_digits);
            return @intCast(value);
        }

        fn appendCodepoint(self: *Self, cp: u21) !void {
            var buf: [4]u8 = undefined;
            const n = std.unicode.utf8Encode(cp, &buf) catch return self.fail("escape names no Unicode character", .{});
            try self.token.appendSlice(self.allocator, buf[0..n]);
        }

        // ------------------------------------------------------------------
        // Literals

        fn literal(self: *Self) !Literal {
            const lexical = try self.string();
            var lit = Literal{ .lexical = lexical };
            if (try self.peek(0) == '@') {
                self.src.advance();
                self.token.clearRetainingCapacity();
                while (try self.peek(0)) |c| {
                    if (!std.ascii.isAlphabetic(c)) break;
                    try self.token.append(self.allocator, c);
                    self.src.advance();
                }
                if (self.token.items.len == 0) return self.fail("expected a language tag after '@'", .{});
                while (try self.peek(0) == '-') {
                    try self.token.append(self.allocator, '-');
                    self.src.advance();
                    var n: usize = 0;
                    while (try self.peek(0)) |c| {
                        if (!std.ascii.isAlphanumeric(c)) break;
                        try self.token.append(self.allocator, c);
                        self.src.advance();
                        n += 1;
                    }
                    if (n == 0) return self.fail("empty subtag in a language tag", .{});
                }
                lit.lang = try self.dupe(self.token.items);
                lit.datatype = term.rdf_lang_string;
            } else if (try self.peek(0) == '^' and try self.peekIs(1, '^')) {
                self.src.advanceN(2);
                if (try self.peek(0) == '<') {
                    lit.datatype = try self.iriRef();
                } else if (self.options.syntax == .ntriples) {
                    return self.fail("expected a datatype IRI after '^^'", .{});
                } else {
                    lit.datatype = self.prefixedName() catch |err| switch (err) {
                        error.NotAName => return self.fail("expected a datatype IRI after '^^'", .{}),
                        else => return err,
                    };
                }
            }
            return lit;
        }

        /// Any of the four string forms, unescaped.
        fn string(self: *Self) ![]const u8 {
            const quote = (try self.peek(0)).?;
            const long = try self.peekIs(1, quote) and try self.peekIs(2, quote);
            if (long and self.options.syntax == .ntriples) return self.fail("N-Triples has no long strings", .{});
            self.src.advanceN(if (long) 3 else 1);
            self.token.clearRetainingCapacity();
            while (true) {
                const c = (try self.peek(0)) orelse return self.fail("unterminated string", .{});
                if (c == quote) {
                    if (!long) {
                        self.src.advance();
                        break;
                    }
                    // The first three quotes in a row end a long string.
                    if (try self.peekIs(1, quote) and try self.peekIs(2, quote)) {
                        self.src.advanceN(3);
                        break;
                    }
                    try self.token.append(self.allocator, c);
                    self.src.advance();
                    continue;
                }
                if (c == '\\') {
                    const e = (try self.peek(1)) orelse return self.fail("unterminated string", .{});
                    const plain: ?u8 = switch (e) {
                        't' => '\t',
                        'b' => 0x08,
                        'n' => '\n',
                        'r' => '\r',
                        'f' => 0x0C,
                        '"' => '"',
                        '\'' => '\'',
                        '\\' => '\\',
                        else => null,
                    };
                    if (plain) |p| {
                        try self.token.append(self.allocator, p);
                        self.src.advanceN(2);
                    } else if (e == 'u' or e == 'U') {
                        try self.appendCodepoint(try self.uchar());
                    } else return self.fail("invalid escape in a string", .{});
                    continue;
                }
                if (!long and (c == '\n' or c == '\r')) return self.fail("a line break in a short string must be escaped", .{});
                try self.token.append(self.allocator, c);
                self.src.advance();
            }
            if (!std.unicode.utf8ValidateSlice(self.token.items)) return self.fail("invalid UTF-8 in a string", .{});
            return self.dupe(self.token.items);
        }

        /// INTEGER, DECIMAL or DOUBLE, kept as written.
        fn number(self: *Self) !Literal {
            self.token.clearRetainingCapacity();
            const first = (try self.peek(0)).?;
            if (first == '+' or first == '-') {
                try self.token.append(self.allocator, first);
                self.src.advance();
            }
            const int_digits = try self.digits();
            var datatype: []const u8 = term.xsd_integer;
            if (try self.peek(0) == '.') {
                const after = (try self.peek(1)) orelse 0;
                const exp_follows = (after == 'e' or after == 'E') and int_digits > 0 and try self.exponentAt(1);
                if (std.ascii.isDigit(after) or exp_follows) {
                    try self.token.append(self.allocator, '.');
                    self.src.advance();
                    const frac = try self.digits();
                    datatype = term.xsd_decimal;
                    if (frac == 0 and !exp_follows) return self.fail("expected digits after '.'", .{});
                }
            }
            if (int_digits == 0 and datatype.ptr == term.xsd_integer.ptr) return self.fail("expected a number", .{});
            const e = (try self.peek(0)) orelse 0;
            if (e == 'e' or e == 'E') {
                if (!try self.exponentAt(0)) return self.fail("expected digits in the exponent", .{});
                try self.token.append(self.allocator, e);
                self.src.advance();
                const sign = (try self.peek(0)).?;
                if (sign == '+' or sign == '-') {
                    try self.token.append(self.allocator, sign);
                    self.src.advance();
                }
                _ = try self.digits();
                datatype = term.xsd_double;
            }
            return .{ .lexical = try self.dupe(self.token.items), .datatype = datatype };
        }

        fn digits(self: *Self) !usize {
            var n: usize = 0;
            while (try self.peek(0)) |c| {
                if (!std.ascii.isDigit(c)) break;
                try self.token.append(self.allocator, c);
                self.src.advance();
                n += 1;
            }
            return n;
        }

        /// Whether an exponent (`e`, an optional sign, a digit) starts `n` ahead.
        fn exponentAt(self: *Self, n: usize) !bool {
            const e = (try self.peek(n)) orelse return false;
            if (e != 'e' and e != 'E') return false;
            var i = n + 1;
            const s = (try self.peek(i)) orelse return false;
            if (s == '+' or s == '-') i += 1;
            const d = (try self.peek(i)) orelse return false;
            return std.ascii.isDigit(d);
        }
    };
}

/// Collects every triple, with its terms copied, for tests and comparisons.
pub const Collector = struct {
    arena: std.heap.ArenaAllocator,
    triples: std.ArrayList(Triple) = .empty,
    prefixes: std.ArrayList([2][]const u8) = .empty,
    statements: usize = 0,

    pub fn init(allocator: std.mem.Allocator) Collector {
        return .{ .arena = std.heap.ArenaAllocator.init(allocator) };
    }

    pub fn deinit(self: *Collector) void {
        self.arena.deinit();
    }

    pub fn onTriple(self: *Collector, t: Triple, at: Pos) !void {
        _ = at;
        const a = self.arena.allocator();
        try self.triples.append(a, try copyTriple(a, t));
    }

    pub fn onPrefix(self: *Collector, prefix: []const u8, namespace: []const u8) !void {
        const a = self.arena.allocator();
        try self.prefixes.append(a, .{ try a.dupe(u8, prefix), try a.dupe(u8, namespace) });
    }

    pub fn onStatementEnd(self: *Collector) !void {
        self.statements += 1;
    }
};

pub fn copyTriple(a: std.mem.Allocator, t: Triple) std.mem.Allocator.Error!Triple {
    return .{ .subject = try copyTerm(a, t.subject), .predicate = try a.dupe(u8, t.predicate), .object = try copyTerm(a, t.object) };
}

pub fn copyTerm(a: std.mem.Allocator, t: Term) std.mem.Allocator.Error!Term {
    return switch (t) {
        .iri => |s| .{ .iri = try a.dupe(u8, s) },
        .blank => |s| .{ .blank = try a.dupe(u8, s) },
        .literal => |l| .{ .literal = try copyLiteral(a, l) },
        .triple => |q| blk: {
            const p = try a.create(Triple);
            p.* = try copyTriple(a, q.*);
            break :blk .{ .triple = p };
        },
        .list => |items| blk: {
            const out = try a.alloc(Literal, items.len);
            for (items, out) |l, *o| o.* = try copyLiteral(a, l);
            break :blk .{ .list = out };
        },
    };
}

fn copyLiteral(a: std.mem.Allocator, l: Literal) !Literal {
    return .{ .lexical = try a.dupe(u8, l.lexical), .datatype = try a.dupe(u8, l.datatype), .lang = try a.dupe(u8, l.lang) };
}

/// Parse all of `text` into `collector`. On a syntax error, `diag` says where.
pub fn parseText(
    allocator: std.mem.Allocator,
    text: []const u8,
    options: Options,
    collector: *Collector,
    diag: ?*Diagnostic,
) !void {
    var reader = SliceReader{ .data = text };
    var parser = try Parser(*Collector).init(allocator, collector, &reader, SliceReader.read, options);
    defer parser.deinit();
    parser.parseAll() catch |err| {
        if (diag) |d| d.* = parser.diag;
        return err;
    };
}

fn expectParseError(text: []const u8, syntax: Syntax, line: usize, column: usize) !void {
    var collector = Collector.init(std.testing.allocator);
    defer collector.deinit();
    var diag: Diagnostic = .{};
    try std.testing.expectError(error.SyntaxError, parseText(std.testing.allocator, text, .{ .syntax = syntax, .base = "http://b/" }, &collector, &diag));
    std.testing.expectEqual(line, diag.pos.line) catch |err| {
        std.debug.print("message: {s}, at {d}:{d}\n", .{ diag.messageText(), diag.pos.line, diag.pos.column });
        return err;
    };
    try std.testing.expectEqual(column, diag.pos.column);
}

test "rdf parser: the Turtle shorthands expand to the triples they stand for" {
    const text =
        \\@prefix : <http://ex/> .
        \\PREFIX x: <http://x/>
        \\@base <http://base/dir/> .
        \\:s a :C ; :p 1, -2.5, 3e1, true, "a"@en-GB, 'b', """c
        \\"d""", "e"^^x:t ;; :q [ :r <rel> ] ;
        \\   :l ( "u" 1 ), ( :i ) .
        \\[] :p _:b1 .
        \\<< :s :p :o >> :since 2020 .
        \\:s :p :o {| :w 0.5 |} .
    ;
    var collector = Collector.init(std.testing.allocator);
    defer collector.deinit();
    try parseText(std.testing.allocator, text, .{ .fold_literal_lists = true }, &collector, null);
    const t = collector.triples.items;
    try std.testing.expectEqual(@as(usize, 19), t.len);
    try std.testing.expectEqualStrings(term.rdf_type, t[0].predicate);
    try std.testing.expectEqualStrings("http://ex/C", t[0].object.iri);
    try std.testing.expectEqualStrings(term.xsd_integer, t[1].object.literal.datatype);
    try std.testing.expectEqualStrings("-2.5", t[2].object.literal.lexical);
    try std.testing.expectEqualStrings(term.xsd_decimal, t[2].object.literal.datatype);
    try std.testing.expectEqualStrings(term.xsd_double, t[3].object.literal.datatype);
    try std.testing.expectEqualStrings(term.xsd_boolean, t[4].object.literal.datatype);
    try std.testing.expectEqualStrings("en-GB", t[5].object.literal.lang);
    try std.testing.expectEqualStrings("c\n\"d", t[7].object.literal.lexical);
    try std.testing.expectEqualStrings("http://x/t", t[8].object.literal.datatype);
    try std.testing.expectEqualStrings("http://base/dir/rel", t[9].object.iri);
    try std.testing.expectEqual(@as(usize, 2), t[11].object.list.len);
    try std.testing.expectEqual(@as(usize, 7), collector.statements);
}

test "rdf parser: N-Triples refuses what only Turtle allows" {
    try expectParseError("<http://a> <http://b> 1 .\n", .ntriples, 1, 23);
    try expectParseError("<a> <http://b> <http://c> .\n", .ntriples, 1, 1);
    try expectParseError("<http://a> <http://b> <http://c> . <http://a> <http://b> <http://c> .\n", .ntriples, 1, 36);
    try expectParseError("<http://a> <http://b>\n <http://c> .\n", .ntriples, 1, 22);
}

test "rdf parser: errors say where" {
    try expectParseError("@prefix : <http://ex/> .\n:s :p :o ,\n.", .turtle, 3, 1);
    try expectParseError(":s :p :o .", .turtle, 1, 1);
    try expectParseError("<http://a> <http://b> \"abc\n\" .", .turtle, 1, 27);
    try expectParseError("<http://a> <http://b> <http://c>", .turtle, 1, 33);
}
