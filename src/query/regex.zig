//! Regular expressions for Cypher's `=~`.
//!
//! `text =~ pattern` is true when the pattern matches the whole text. The
//! pattern is compiled to a small instruction program and run as a Pike VM:
//! every possible match is followed at once, one character of text at a
//! time, so matching takes time proportional to text length times program
//! size. No pattern can make it backtrack exponentially, which matters
//! because patterns can come from query parameters.
//!
//! The syntax is a subset of the Java syntax Cypher specifies:
//!
//! - literals, `.`, `[...]` and `[^...]` classes with ranges, `\d \D \w \W
//!   \s \S` (also inside classes), escapes `\t \n \r \f \xhh \uhhhh` and a
//!   backslash before any punctuation
//! - groups `(...)` and `(?:...)`, alternation `|`
//! - quantifiers `* + ? {n} {n,} {n,m}`, each optionally lazy (`*?`); for a
//!   whole-text match lazy and greedy accept the same texts
//! - anchors `^ $ \A \z` and word boundaries `\b \B`
//! - flags `(?i)` (ASCII case-insensitive) and `(?s)` (`.` matches a
//!   newline), at the start of the pattern
//!
//! Text and pattern are read as UTF-8, so `.` matches one character, not
//! one byte. Anything else (backreferences, lookaround, possessive
//! quantifiers, flags elsewhere, Unicode classes) is rejected as an invalid
//! pattern rather than misread.

const std = @import("std");
const Allocator = std.mem.Allocator;

pub const Error = error{ OutOfMemory, InvalidPattern, PatternTooComplex };

/// The most instructions a compiled pattern may have. `{n,m}` copies its
/// operand, so `(a{1000}){1000}` would otherwise compile to a million.
pub const MAX_PROGRAM: usize = 20_000;
/// The largest count `{n,m}` accepts.
pub const MAX_REPEAT: u32 = 1000;

const Range = struct { lo: u21, hi: u21 };

const Class = struct {
    ranges: []const Range,
    negated: bool,

    fn contains(self: Class, c: u21) bool {
        var found = false;
        for (self.ranges) |r| {
            if (c >= r.lo and c <= r.hi) {
                found = true;
                break;
            }
        }
        return found != self.negated;
    }
};

const Assertion = enum { start, end, word_boundary, not_word_boundary };

const Inst = union(enum) {
    char: u21,
    /// Any character but a newline.
    any,
    /// Any character at all, under `(?s)`.
    any_newline,
    class: u32,
    split: struct { x: u32, y: u32 },
    jmp: u32,
    assert: Assertion,
    match,
};

pub const Regex = struct {
    program: []Inst,
    classes: []Class,
    arena: std.heap.ArenaAllocator,

    pub fn compile(allocator: Allocator, pattern: []const u8) Error!Regex {
        var arena = std.heap.ArenaAllocator.init(allocator);
        errdefer arena.deinit();
        const a = arena.allocator();

        var p = Parser{ .pattern = try decode(a, pattern), .arena = a };
        p.parseFlags();
        const root = try p.parseAlternation();
        if (p.pos < p.pattern.len) return error.InvalidPattern; // a stray ')'

        var e = Emitter{ .arena = a, .classes = &p.classes, .dot_all = p.dot_all };
        try e.emit(root);
        try e.add(.match);
        return .{
            .program = e.program.items,
            .classes = p.classes.items,
            .arena = arena,
        };
    }

    pub fn deinit(self: *Regex) void {
        self.arena.deinit();
    }

    /// Whether the pattern matches all of `text`.
    pub fn fullMatch(self: *const Regex, allocator: Allocator, text: []const u8) Error!bool {
        var scratch: Scratch = .{};
        defer scratch.deinit(allocator);
        return self.fullMatchWith(allocator, &scratch, text);
    }

    /// `fullMatch`, reusing buffers across calls.
    pub fn fullMatchWith(self: *const Regex, allocator: Allocator, scratch: *Scratch, text: []const u8) Error!bool {
        const chars = try decodeInto(allocator, &scratch.chars, text);
        const n = self.program.len;
        var current = try scratch.lists[0].reset(allocator, n);
        var next = try scratch.lists[1].reset(allocator, n);
        const stack = &scratch.stack;

        try self.addThread(allocator, current, stack, 0, chars, 0);
        var pos: usize = 0;
        while (pos < chars.len) : (pos += 1) {
            if (current.len == 0) return false;
            const c = chars[pos];
            next.clear();
            for (current.dense[0..current.len]) |pc| {
                const advances = switch (self.program[pc]) {
                    .char => |want| c == want,
                    .any => c != '\n',
                    .any_newline => true,
                    .class => |index| self.classes[index].contains(c),
                    else => false,
                };
                if (advances) try self.addThread(allocator, next, stack, pc + 1, chars, pos + 1);
            }
            std.mem.swap(*ThreadList, &current, &next);
        }
        for (current.dense[0..current.len]) |pc| {
            if (self.program[pc] == .match) return true;
        }
        return false;
    }

    /// Add `start` and every instruction reachable from it without reading a
    /// character, at text position `pos`.
    fn addThread(
        self: *const Regex,
        allocator: Allocator,
        list: *ThreadList,
        stack: *std.ArrayListUnmanaged(u32),
        start: u32,
        chars: []const u21,
        pos: usize,
    ) Error!void {
        stack.clearRetainingCapacity();
        try stack.append(allocator, start);
        while (stack.pop()) |pc| {
            if (list.contains(pc)) continue;
            list.add(pc);
            switch (self.program[pc]) {
                .jmp => |to| try stack.append(allocator, to),
                .split => |s| {
                    // Pushed in reverse so x is followed first.
                    try stack.append(allocator, s.y);
                    try stack.append(allocator, s.x);
                },
                .assert => |kind| if (holds(kind, chars, pos)) try stack.append(allocator, pc + 1),
                else => {},
            }
        }
    }
};

fn holds(kind: Assertion, chars: []const u21, pos: usize) bool {
    return switch (kind) {
        .start => pos == 0,
        .end => pos == chars.len,
        .word_boundary, .not_word_boundary => blk: {
            const before = pos > 0 and isWord(chars[pos - 1]);
            const after = pos < chars.len and isWord(chars[pos]);
            break :blk (before != after) == (kind == .word_boundary);
        },
    };
}

fn isWord(c: u21) bool {
    return c < 128 and (std.ascii.isAlphanumeric(@intCast(c)) or c == '_');
}

/// Buffers a match needs, kept between matches.
pub const Scratch = struct {
    chars: std.ArrayListUnmanaged(u21) = .empty,
    lists: [2]ThreadList = .{ .{}, .{} },
    stack: std.ArrayListUnmanaged(u32) = .empty,

    pub fn deinit(self: *Scratch, allocator: Allocator) void {
        self.chars.deinit(allocator);
        for (&self.lists) |*list| list.deinit(allocator);
        self.stack.deinit(allocator);
    }
};

/// A set of program counters, in insertion order, with O(1) membership.
const ThreadList = struct {
    dense: []u32 = &.{},
    sparse: []u32 = &.{},
    len: u32 = 0,

    /// Empty, with room for `n` program counters.
    fn reset(self: *ThreadList, allocator: Allocator, n: usize) Error!*ThreadList {
        if (self.dense.len < n) {
            self.deinit(allocator);
            self.dense = try allocator.alloc(u32, n);
            self.sparse = allocator.alloc(u32, n) catch |err| {
                allocator.free(self.dense);
                self.dense = &.{};
                return err;
            };
        }
        self.len = 0;
        return self;
    }

    fn deinit(self: *ThreadList, allocator: Allocator) void {
        allocator.free(self.dense);
        allocator.free(self.sparse);
        self.dense = &.{};
        self.sparse = &.{};
    }

    fn clear(self: *ThreadList) void {
        self.len = 0;
    }

    fn contains(self: *const ThreadList, pc: u32) bool {
        const i = self.sparse[pc];
        return i < self.len and self.dense[i] == pc;
    }

    fn add(self: *ThreadList, pc: u32) void {
        self.sparse[pc] = self.len;
        self.dense[self.len] = pc;
        self.len += 1;
    }
};

/// UTF-8 to code points. Bytes that are not valid UTF-8 are read one by one
/// as the code points of the same value, so any text can be matched.
fn decode(allocator: Allocator, bytes: []const u8) Error![]u21 {
    var out: std.ArrayListUnmanaged(u21) = .empty;
    errdefer out.deinit(allocator);
    _ = try decodeInto(allocator, &out, bytes);
    return out.toOwnedSlice(allocator);
}

fn decodeInto(allocator: Allocator, out: *std.ArrayListUnmanaged(u21), bytes: []const u8) Error![]const u21 {
    out.clearRetainingCapacity();
    try out.ensureTotalCapacity(allocator, bytes.len);
    var i: usize = 0;
    while (i < bytes.len) {
        const len = std.unicode.utf8ByteSequenceLength(bytes[i]) catch 1;
        if (len > 1 and i + len <= bytes.len) {
            if (std.unicode.utf8Decode(bytes[i .. i + len])) |c| {
                out.appendAssumeCapacity(c);
                i += len;
                continue;
            } else |_| {}
        }
        out.appendAssumeCapacity(bytes[i]);
        i += 1;
    }
    return out.items;
}

/// Compiled patterns for the length of one query, so a `=~` in a WHERE
/// compiles its pattern once rather than once per row.
pub const Cache = struct {
    allocator: Allocator,
    patterns: std.StringHashMapUnmanaged(Regex) = .empty,
    scratch: Scratch = .{},

    /// Patterns beyond this many are compiled per match instead, which
    /// bounds the memory a pattern built per row can take.
    pub const MAX_PATTERNS = 32;

    pub fn init(allocator: Allocator) Cache {
        return .{ .allocator = allocator };
    }

    pub fn deinit(self: *Cache) void {
        var it = self.patterns.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            entry.value_ptr.deinit();
        }
        self.patterns.deinit(self.allocator);
        self.scratch.deinit(self.allocator);
    }

    pub fn fullMatch(self: *Cache, pattern: []const u8, text: []const u8) Error!bool {
        if (self.patterns.getPtr(pattern)) |compiled| {
            return compiled.fullMatchWith(self.allocator, &self.scratch, text);
        }
        var compiled = try Regex.compile(self.allocator, pattern);
        if (self.patterns.count() >= MAX_PATTERNS) {
            defer compiled.deinit();
            return compiled.fullMatchWith(self.allocator, &self.scratch, text);
        }
        errdefer compiled.deinit();
        const key = try self.allocator.dupe(u8, pattern);
        errdefer self.allocator.free(key);
        try self.patterns.put(self.allocator, key, compiled);
        return self.patterns.getPtr(key).?.fullMatchWith(self.allocator, &self.scratch, text);
    }
};

const Node = union(enum) {
    empty,
    char: u21,
    any,
    class: u32,
    assert: Assertion,
    concat: []const *const Node,
    alternation: []const *const Node,
    repeat: struct { node: *const Node, min: u32, max: ?u32 },
};

const Parser = struct {
    pattern: []const u21,
    pos: usize = 0,
    arena: Allocator,
    classes: std.ArrayListUnmanaged(Class) = .empty,
    case_insensitive: bool = false,
    dot_all: bool = false,

    fn peek(self: *const Parser) ?u21 {
        return if (self.pos < self.pattern.len) self.pattern[self.pos] else null;
    }

    fn startsWith(self: *const Parser, comptime text: []const u8) bool {
        if (self.pos + text.len > self.pattern.len) return false;
        for (text, 0..) |c, i| {
            if (self.pattern[self.pos + i] != c) return false;
        }
        return true;
    }

    /// `(?i)`, `(?s)` or `(?is)` at the very start.
    fn parseFlags(self: *Parser) void {
        if (!self.startsWith("(?")) return;
        var i = self.pos + 2;
        var case_insensitive = false;
        var dot_all = false;
        while (i < self.pattern.len) : (i += 1) {
            switch (self.pattern[i]) {
                'i' => case_insensitive = true,
                's' => dot_all = true,
                ')' => {
                    if (i == self.pos + 2) return;
                    self.case_insensitive = case_insensitive;
                    self.dot_all = dot_all;
                    self.pos = i + 1;
                    return;
                },
                // Not a flag group this parser knows; parseAtom rejects it.
                else => return,
            }
        }
    }

    fn node(self: *Parser, value: Node) Error!*const Node {
        const n = try self.arena.create(Node);
        n.* = value;
        return n;
    }

    fn parseAlternation(self: *Parser) Error!*const Node {
        var branches: std.ArrayListUnmanaged(*const Node) = .empty;
        try branches.append(self.arena, try self.parseConcat());
        while (self.peek() == '|') {
            self.pos += 1;
            try branches.append(self.arena, try self.parseConcat());
        }
        if (branches.items.len == 1) return branches.items[0];
        return self.node(.{ .alternation = branches.items });
    }

    fn parseConcat(self: *Parser) Error!*const Node {
        var items: std.ArrayListUnmanaged(*const Node) = .empty;
        while (self.peek()) |c| {
            if (c == '|' or c == ')') break;
            try items.append(self.arena, try self.parseRepeat());
        }
        return switch (items.items.len) {
            0 => self.node(.empty),
            1 => items.items[0],
            else => self.node(.{ .concat = items.items }),
        };
    }

    fn parseRepeat(self: *Parser) Error!*const Node {
        const atom = try self.parseAtom();
        const c = self.peek() orelse return atom;
        var min: u32 = undefined;
        var max: ?u32 = undefined;
        switch (c) {
            '*' => {
                min = 0;
                max = null;
                self.pos += 1;
            },
            '+' => {
                min = 1;
                max = null;
                self.pos += 1;
            },
            '?' => {
                min = 0;
                max = 1;
                self.pos += 1;
            },
            '{' => {
                self.pos += 1;
                min = try self.parseCount();
                max = min;
                if (self.peek() == ',') {
                    self.pos += 1;
                    max = if (self.peek() == '}') null else try self.parseCount();
                }
                if (self.peek() != '}') return error.InvalidPattern;
                self.pos += 1;
                if (max) |m| {
                    if (m < min) return error.InvalidPattern;
                }
            },
            else => return atom,
        }
        // Lazy matching accepts the same whole texts; possessive does not.
        if (self.peek() == '?') {
            self.pos += 1;
        } else if (self.peek() == '+') {
            return error.InvalidPattern;
        }
        // A quantifier on a quantifier, as in `a**`.
        if (self.peek()) |after| {
            if (after == '*' or after == '+' or after == '?' or after == '{') return error.InvalidPattern;
        }
        return self.node(.{ .repeat = .{ .node = atom, .min = min, .max = max } });
    }

    fn parseCount(self: *Parser) Error!u32 {
        var value: u32 = 0;
        var digits: usize = 0;
        while (self.peek()) |c| {
            if (c < '0' or c > '9') break;
            value = value * 10 + @as(u32, c - '0');
            if (value > MAX_REPEAT) return error.PatternTooComplex;
            digits += 1;
            self.pos += 1;
        }
        if (digits == 0) return error.InvalidPattern;
        return value;
    }

    fn parseAtom(self: *Parser) Error!*const Node {
        const c = self.peek() orelse return error.InvalidPattern;
        self.pos += 1;
        switch (c) {
            '(' => {
                if (self.peek() == '?') {
                    if (!self.startsWith("?:")) return error.InvalidPattern;
                    self.pos += 2;
                }
                const inner = try self.parseAlternation();
                if (self.peek() != ')') return error.InvalidPattern;
                self.pos += 1;
                return inner;
            },
            '*', '+', '?', '{', ')' => return error.InvalidPattern,
            '.' => return self.node(.any),
            '^' => return self.node(.{ .assert = .start }),
            '$' => return self.node(.{ .assert = .end }),
            '[' => return self.parseClass(),
            '\\' => return self.parseEscape(),
            else => return self.literal(c),
        }
    }

    fn literal(self: *Parser, c: u21) Error!*const Node {
        if (self.case_insensitive and c < 128 and std.ascii.isAlphabetic(@intCast(c))) {
            const lower: u21 = std.ascii.toLower(@intCast(c));
            const upper: u21 = std.ascii.toUpper(@intCast(c));
            const ranges = try self.arena.alloc(Range, 2);
            ranges[0] = .{ .lo = lower, .hi = lower };
            ranges[1] = .{ .lo = upper, .hi = upper };
            return self.node(.{ .class = try self.addClass(ranges, false) });
        }
        return self.node(.{ .char = c });
    }

    fn addClass(self: *Parser, ranges: []const Range, negated: bool) Error!u32 {
        const index: u32 = @intCast(self.classes.items.len);
        try self.classes.append(self.arena, .{ .ranges = ranges, .negated = negated });
        return index;
    }

    fn parseEscape(self: *Parser) Error!*const Node {
        const c = self.peek() orelse return error.InvalidPattern;
        self.pos += 1;
        switch (c) {
            'b' => return self.node(.{ .assert = .word_boundary }),
            'B' => return self.node(.{ .assert = .not_word_boundary }),
            'A' => return self.node(.{ .assert = .start }),
            'z' => return self.node(.{ .assert = .end }),
            'd', 'D', 'w', 'W', 's', 'S' => {
                var ranges: std.ArrayListUnmanaged(Range) = .empty;
                try appendShorthand(self.arena, &ranges, c);
                return self.node(.{ .class = try self.addClass(ranges.items, false) });
            },
            else => return self.literal(try self.escapedChar(c)),
        }
    }

    /// The character an escape outside a shorthand stands for.
    fn escapedChar(self: *Parser, c: u21) Error!u21 {
        return switch (c) {
            't' => '\t',
            'n' => '\n',
            'r' => '\r',
            'f' => 0x0C,
            'e' => 0x1B,
            'x' => self.parseHex(2),
            'u' => self.parseHex(4),
            else => if (c < 128 and std.ascii.isAlphanumeric(@intCast(c)))
                error.InvalidPattern // a backreference or an escape not supported
            else
                c,
        };
    }

    fn parseHex(self: *Parser, digits: usize) Error!u21 {
        if (self.pos + digits > self.pattern.len) return error.InvalidPattern;
        var value: u21 = 0;
        for (self.pattern[self.pos .. self.pos + digits]) |d| {
            const v: u21 = if (d < 128) std.fmt.charToDigit(@intCast(d), 16) catch return error.InvalidPattern else return error.InvalidPattern;
            value = value * 16 + v;
        }
        self.pos += digits;
        return value;
    }

    fn parseClass(self: *Parser) Error!*const Node {
        var negated = false;
        if (self.peek() == '^') {
            negated = true;
            self.pos += 1;
        }
        var ranges: std.ArrayListUnmanaged(Range) = .empty;
        var first = true;
        while (true) {
            const c = self.peek() orelse return error.InvalidPattern;
            if (c == ']' and !first) {
                self.pos += 1;
                break;
            }
            first = false;
            self.pos += 1;
            var lo = c;
            if (c == '\\') {
                const e = self.peek() orelse return error.InvalidPattern;
                self.pos += 1;
                switch (e) {
                    'd', 'D', 'w', 'W', 's', 'S' => {
                        try appendShorthand(self.arena, &ranges, e);
                        continue;
                    },
                    'b' => {
                        lo = 0x08;
                    },
                    else => lo = try self.escapedChar(e),
                }
            } else if (c == '[') {
                // Java nests and intersects classes; not supported.
                return error.InvalidPattern;
            }
            var hi = lo;
            if (self.peek() == '-' and self.pos + 1 < self.pattern.len and self.pattern[self.pos + 1] != ']') {
                self.pos += 1;
                const h = self.peek() orelse return error.InvalidPattern;
                self.pos += 1;
                hi = if (h == '\\') blk: {
                    const e = self.peek() orelse return error.InvalidPattern;
                    self.pos += 1;
                    break :blk try self.escapedChar(e);
                } else h;
                if (hi < lo) return error.InvalidPattern;
            }
            try ranges.append(self.arena, .{ .lo = lo, .hi = hi });
            if (self.case_insensitive) try appendFolded(self.arena, &ranges, lo, hi);
        }
        return self.node(.{ .class = try self.addClass(ranges.items, negated) });
    }
};

/// The ASCII letters of [lo, hi] in the other case.
fn appendFolded(arena: Allocator, ranges: *std.ArrayListUnmanaged(Range), lo: u21, hi: u21) Error!void {
    const spans = [_][2]u21{ .{ 'a', 'z' }, .{ 'A', 'Z' } };
    for (spans, 0..) |span, i| {
        const start = @max(lo, span[0]);
        const end = @min(hi, span[1]);
        if (start > end) continue;
        const shift: i32 = if (i == 0) -32 else 32;
        try ranges.append(arena, .{
            .lo = @intCast(@as(i32, start) + shift),
            .hi = @intCast(@as(i32, end) + shift),
        });
    }
}

fn appendShorthand(arena: Allocator, ranges: *std.ArrayListUnmanaged(Range), c: u21) Error!void {
    const digit = [_]Range{.{ .lo = '0', .hi = '9' }};
    const word = [_]Range{ .{ .lo = '0', .hi = '9' }, .{ .lo = 'A', .hi = 'Z' }, .{ .lo = '_', .hi = '_' }, .{ .lo = 'a', .hi = 'z' } };
    const space = [_]Range{ .{ .lo = '\t', .hi = 0x0D }, .{ .lo = ' ', .hi = ' ' } };
    const set: []const Range = switch (c) {
        'd', 'D' => &digit,
        'w', 'W' => &word,
        's', 'S' => &space,
        else => unreachable,
    };
    if (c == 'd' or c == 'w' or c == 's') {
        try ranges.appendSlice(arena, set);
        return;
    }
    // The complement of a sorted, disjoint set.
    var next: u21 = 0;
    for (set) |r| {
        if (r.lo > next) try ranges.append(arena, .{ .lo = next, .hi = r.lo - 1 });
        next = r.hi + 1;
    }
    try ranges.append(arena, .{ .lo = next, .hi = std.math.maxInt(u21) });
}

const Emitter = struct {
    arena: Allocator,
    classes: *std.ArrayListUnmanaged(Class),
    program: std.ArrayListUnmanaged(Inst) = .empty,
    dot_all: bool = false,

    fn add(self: *Emitter, inst: Inst) Error!void {
        if (self.program.items.len >= MAX_PROGRAM) return error.PatternTooComplex;
        try self.program.append(self.arena, inst);
    }

    fn here(self: *const Emitter) u32 {
        return @intCast(self.program.items.len);
    }

    fn emit(self: *Emitter, n: *const Node) Error!void {
        switch (n.*) {
            .empty => {},
            .char => |c| try self.add(.{ .char = c }),
            .any => try self.add(if (self.dot_all) .any_newline else .any),
            .class => |index| try self.add(.{ .class = index }),
            .assert => |kind| try self.add(.{ .assert = kind }),
            .concat => |items| for (items) |item| try self.emit(item),
            .alternation => |branches| {
                // split L1, next; L1: branch; jmp end; next: split L2, ...
                var jumps: std.ArrayListUnmanaged(u32) = .empty;
                for (branches, 0..) |branch, i| {
                    if (i + 1 < branches.len) {
                        const split = self.here();
                        try self.add(.{ .split = .{ .x = split + 1, .y = 0 } });
                        try self.emit(branch);
                        try jumps.append(self.arena, self.here());
                        try self.add(.{ .jmp = 0 });
                        self.program.items[split].split.y = self.here();
                    } else {
                        try self.emit(branch);
                    }
                }
                for (jumps.items) |j| self.program.items[j] = .{ .jmp = self.here() };
            },
            .repeat => |r| {
                var i: u32 = 0;
                while (i < r.min) : (i += 1) try self.emit(r.node);
                if (r.max) |max| {
                    // Each optional copy: split body, end.
                    var splits: std.ArrayListUnmanaged(u32) = .empty;
                    while (i < max) : (i += 1) {
                        try splits.append(self.arena, self.here());
                        try self.add(.{ .split = .{ .x = self.here() + 1, .y = 0 } });
                        try self.emit(r.node);
                    }
                    for (splits.items) |s| self.program.items[s].split.y = self.here();
                } else {
                    // loop: split body, end; body; jmp loop
                    const loop = self.here();
                    try self.add(.{ .split = .{ .x = loop + 1, .y = 0 } });
                    try self.emit(r.node);
                    try self.add(.{ .jmp = loop });
                    self.program.items[loop].split.y = self.here();
                }
            },
        }
    }
};

/// Whether `pattern` matches all of `text`.
pub fn fullMatch(allocator: Allocator, pattern: []const u8, text: []const u8) Error!bool {
    var regex = try Regex.compile(allocator, pattern);
    defer regex.deinit();
    return regex.fullMatch(allocator, text);
}

// ============================================================================
// Tests
// ============================================================================

fn expectMatch(pattern: []const u8, text: []const u8, expected: bool) !void {
    const got = try fullMatch(std.testing.allocator, pattern, text);
    if (got != expected) {
        std.debug.print("pattern \"{s}\" on \"{s}\": expected {}, got {}\n", .{ pattern, text, expected, got });
        return error.TestUnexpectedResult;
    }
}

test "regex: literals, classes and the whole-text rule" {
    try expectMatch("abc", "abc", true);
    try expectMatch("abc", "abcd", false);
    try expectMatch("b", "abc", false);
    try expectMatch("a.c", "abc", true);
    try expectMatch("a.c", "a\nc", false);
    try expectMatch("(?s)a.c", "a\nc", true);
    try expectMatch("[a-c]+", "abcab", true);
    try expectMatch("[^a-c]+", "xyz", true);
    try expectMatch("[^a-c]+", "xaz", false);
    try expectMatch("\\d{3}-\\d{4}", "555-1234", true);
    try expectMatch("\\w+@\\w+\\.com", "joe@example.com", true);
    try expectMatch("[\\d.]+", "3.14", true);
    try expectMatch("a\\.b", "a.b", true);
    try expectMatch("a\\.b", "axb", false);
    try expectMatch("", "", true);
    try expectMatch("", "a", false);
}

test "regex: alternation and groups backtrack" {
    // The old matcher took the first branch of a group and never came back.
    try expectMatch("(a|ab)c", "abc", true);
    try expectMatch("(a|ab)(c|bcd)", "abcd", true);
    // Alternation splits the whole pattern, not just what follows a group.
    try expectMatch("(a)b|c", "c", true);
    try expectMatch("(a)b|c", "ab", true);
    try expectMatch("(a)b|c", "ac", false);
    try expectMatch("x(?:ab)*y", "xababy", true);
    try expectMatch("(a*)*b", "aaab", true);
}

test "regex: counted repetition and laziness" {
    try expectMatch("a{2,3}", "a", false);
    try expectMatch("a{2,3}", "aa", true);
    try expectMatch("a{2,3}", "aaaa", false);
    try expectMatch("a{2,}", "aaaaa", true);
    try expectMatch("a.*?b", "axxb", true);
    try expectMatch("(ab){2}", "abab", true);
}

test "regex: flags, anchors and word boundaries" {
    try expectMatch("(?i)hello", "HeLLo", true);
    try expectMatch("(?i)[a-c]+", "ABC", true);
    try expectMatch("hello", "HELLO", false);
    try expectMatch("^abc$", "abc", true);
    try expectMatch(".*\\bcat\\b.*", "a cat sat", true);
    try expectMatch(".*\\bcat\\b.*", "concatenate", false);
}

test "regex: dot matches a character, not a byte" {
    try expectMatch("caf.", "café", true);
    try expectMatch("...", "日本語", true);
    try expectMatch("[日本]+語", "日本語", true);
}

test "regex: no exponential blowup" {
    // Classic catastrophic-backtracking shapes finish at once.
    const text = "a" ** 5000 ++ "!";
    try expectMatch("(a+)+b", text, false);
    try expectMatch("(a|aa)*c", text, false);
    try expectMatch("(.*a){20}", "a" ** 30, true);
}

test "regex: unsupported or malformed patterns are errors" {
    const bad = [_][]const u8{ "(", ")", "[abc", "a**", "*a", "a{2,1}", "(?=a)", "(a)\\1", "a++", "\\p{L}", "a{", "[[a]]" };
    for (bad) |pattern| {
        try std.testing.expectError(error.InvalidPattern, fullMatch(std.testing.allocator, pattern, "a"));
    }
    try std.testing.expectError(error.PatternTooComplex, fullMatch(std.testing.allocator, "((a{1000}){1000})", "a"));
}

test "regex: the cache compiles each pattern once and stays bounded" {
    var cache = Cache.init(std.testing.allocator);
    defer cache.deinit();
    try std.testing.expect(try cache.fullMatch("a+", "aaa"));
    try std.testing.expect(!try cache.fullMatch("a+", "aab"));
    try std.testing.expectEqual(@as(u32, 1), cache.patterns.count());

    var buf: [8]u8 = undefined;
    for (0..Cache.MAX_PATTERNS + 5) |i| {
        const pattern = try std.fmt.bufPrint(&buf, "x{d}", .{i});
        try std.testing.expect(try cache.fullMatch(pattern, pattern));
    }
    try std.testing.expectEqual(@as(u32, Cache.MAX_PATTERNS), cache.patterns.count());
    try std.testing.expectError(error.InvalidPattern, cache.fullMatch("(", "a"));
}
