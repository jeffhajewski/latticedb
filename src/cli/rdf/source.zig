//! A byte source with a little lookahead, read in blocks, so a parser holds
//! one block of input at a time and never the whole file.

const std = @import("std");
const Pos = @import("term.zig").Pos;

pub const ReadFn = *const fn (ctx: *anyopaque, buf: []u8) anyerror!usize;

pub const Source = struct {
    ctx: *anyopaque,
    read_fn: ReadFn,
    buf: []u8,
    start: usize = 0,
    end: usize = 0,
    eof: bool = false,
    pos: Pos = .{},
    /// Bytes consumed so far.
    offset: u64 = 0,

    pub const block_size = 64 * 1024;

    pub fn init(allocator: std.mem.Allocator, ctx: *anyopaque, read_fn: ReadFn) !Source {
        return .{ .ctx = ctx, .read_fn = read_fn, .buf = try allocator.alloc(u8, block_size) };
    }

    pub fn deinit(self: *Source, allocator: std.mem.Allocator) void {
        allocator.free(self.buf);
    }

    /// Make at least `n` bytes available unless the input ends first.
    fn ensure(self: *Source, n: usize) !void {
        if (self.end - self.start >= n or self.eof) return;
        if (self.start > 0) {
            std.mem.copyForwards(u8, self.buf, self.buf[self.start..self.end]);
            self.end -= self.start;
            self.start = 0;
        }
        while (self.end - self.start < n and !self.eof) {
            const got = try self.read_fn(self.ctx, self.buf[self.end..]);
            if (got == 0) self.eof = true else self.end += got;
        }
    }

    /// The byte `n` places ahead, or null past the end of the input.
    pub inline fn peek(self: *Source, n: usize) !?u8 {
        if (self.end - self.start <= n) {
            try self.ensure(n + 1);
            if (self.end - self.start <= n) return null;
        }
        return self.buf[self.start + n];
    }

    pub inline fn advance(self: *Source) void {
        const c = self.buf[self.start];
        self.start += 1;
        self.offset += 1;
        if (c == '\n') {
            self.pos.line += 1;
            self.pos.column = 1;
        } else if (c & 0xC0 != 0x80) {
            self.pos.column += 1;
        }
    }

    pub fn advanceN(self: *Source, n: usize) void {
        for (0..n) |_| self.advance();
    }

    /// Up to `max` bytes from here, for an error message, not consumed.
    pub fn preview(self: *Source, max: usize) []const u8 {
        self.ensure(max) catch {};
        const avail = self.buf[self.start..self.end];
        return avail[0..@min(max, avail.len)];
    }
};

/// Reads from a slice, for tests and for text already in memory.
pub const SliceReader = struct {
    data: []const u8,
    at: usize = 0,

    pub fn read(ctx: *anyopaque, buf: []u8) anyerror!usize {
        const self: *SliceReader = @ptrCast(@alignCast(ctx));
        const n = @min(buf.len, self.data.len - self.at);
        @memcpy(buf[0..n], self.data[self.at .. self.at + n]);
        self.at += n;
        return n;
    }
};
