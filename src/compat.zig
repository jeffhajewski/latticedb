const std = @import("std");

pub const io = std.Options.debug_io;

pub const Mutex = struct {
    state: std.atomic.Mutex = .unlocked,

    pub fn lock(self: *Mutex) void {
        while (!self.state.tryLock()) {
            std.atomic.spinLoopHint();
        }
    }

    pub fn unlock(self: *Mutex) void {
        self.state.unlock();
    }
};

pub const Condition = struct {
    pub fn broadcast(self: *Condition) void {
        _ = self;
    }

    pub fn timedWait(self: *Condition, mutex: *Mutex, ns: u64) !void {
        _ = self;
        mutex.unlock();
        sleep(ns);
        mutex.lock();
    }
};

pub fn timestamp() i64 {
    return @intCast(std.Io.Clock.real.now(io).toSeconds());
}

pub fn milliTimestamp() i64 {
    return std.Io.Clock.real.now(io).toMilliseconds();
}

pub fn nanoTimestamp() i128 {
    return @intCast(std.Io.Clock.boot.now(io).toNanoseconds());
}

pub fn sleep(ns: u64) void {
    std.Io.Clock.Duration.sleep(.{
        .clock = .boot,
        .raw = .fromNanoseconds(@intCast(ns)),
    }, io) catch {};
}

pub fn randomBytes(buf: []u8) void {
    var seed: u64 = @intCast(nanoTimestamp());
    seed ^= @intFromPtr(buf.ptr);
    var prng = std.Random.DefaultPrng.init(seed);
    prng.random().bytes(buf);
}

pub fn randomInt(comptime T: type) T {
    var prng = std.Random.DefaultPrng.init(@intCast(nanoTimestamp()));
    return prng.random().int(T);
}

pub fn fixedBufferStream(buffer: anytype) FixedBufferStream(@TypeOf(buffer)) {
    return .{ .buffer = buffer };
}

pub fn arrayListWriter(list: anytype) ArrayListWriter(@TypeOf(list)) {
    return .{ .list = list };
}

pub fn arrayListWriterWithAllocator(list: anytype, allocator: std.mem.Allocator) ArrayListWriterWithAllocator(@TypeOf(list)) {
    return .{ .list = list, .allocator = allocator };
}

fn ArrayListWriter(comptime ListPtr: type) type {
    return struct {
        list: ListPtr,

        const Self = @This();

        pub fn writeByte(self: Self, byte: u8) !void {
            try self.list.append(byte);
        }

        pub fn writeAll(self: Self, bytes: []const u8) !void {
            try self.list.appendSlice(bytes);
        }

        pub fn print(self: Self, comptime fmt: []const u8, args: anytype) !void {
            var buf: [128]u8 = undefined;
            const rendered = try std.fmt.bufPrint(&buf, fmt, args);
            try self.writeAll(rendered);
        }
    };
}

fn ArrayListWriterWithAllocator(comptime ListPtr: type) type {
    return struct {
        list: ListPtr,
        allocator: std.mem.Allocator,

        const Self = @This();

        pub fn writeByte(self: Self, byte: u8) !void {
            try self.list.append(self.allocator, byte);
        }

        pub fn writeAll(self: Self, bytes: []const u8) !void {
            try self.list.appendSlice(self.allocator, bytes);
        }

        pub fn print(self: Self, comptime fmt: []const u8, args: anytype) !void {
            var buf: [128]u8 = undefined;
            const rendered = try std.fmt.bufPrint(&buf, fmt, args);
            try self.writeAll(rendered);
        }
    };
}

pub fn FixedBufferStream(comptime Buffer: type) type {
    return struct {
        buffer: Buffer,
        pos: usize = 0,

        const Self = @This();

        pub fn writer(self: *Self) FixedWriter(*Self) {
            return .{ .stream = self };
        }

        pub fn reader(self: *Self) FixedReader(*Self) {
            return .{ .stream = self };
        }
    };
}

fn FixedWriter(comptime StreamPtr: type) type {
    return struct {
        stream: StreamPtr,

        const Self = @This();

        pub fn writeByte(self: Self, byte: u8) error{NoSpaceLeft}!void {
            try self.writeAll(&.{byte});
        }

        pub fn writeAll(self: Self, bytes: []const u8) error{NoSpaceLeft}!void {
            const end = self.stream.pos + bytes.len;
            if (end > self.stream.buffer.len) return error.NoSpaceLeft;
            @memcpy(self.stream.buffer[self.stream.pos..end], bytes);
            self.stream.pos = end;
        }

        pub fn writeInt(self: Self, comptime T: type, value: T, endian: std.builtin.Endian) error{NoSpaceLeft}!void {
            var bytes: [@sizeOf(T)]u8 = undefined;
            std.mem.writeInt(T, &bytes, value, endian);
            try self.writeAll(&bytes);
        }

        pub fn print(self: Self, comptime fmt: []const u8, args: anytype) error{NoSpaceLeft}!void {
            var buf: [128]u8 = undefined;
            const rendered = std.fmt.bufPrint(&buf, fmt, args) catch return error.NoSpaceLeft;
            try self.writeAll(rendered);
        }
    };
}

fn FixedReader(comptime StreamPtr: type) type {
    return struct {
        stream: StreamPtr,

        const Self = @This();

        pub fn readByte(self: Self) error{EndOfStream}!u8 {
            if (self.stream.pos >= self.stream.buffer.len) return error.EndOfStream;
            const byte = self.stream.buffer[self.stream.pos];
            self.stream.pos += 1;
            return byte;
        }

        pub fn readInt(self: Self, comptime T: type, endian: std.builtin.Endian) error{EndOfStream}!T {
            const end = self.stream.pos + @sizeOf(T);
            if (end > self.stream.buffer.len) return error.EndOfStream;
            const bytes: *const [@sizeOf(T)]u8 = @ptrCast(self.stream.buffer[self.stream.pos..end].ptr);
            const value = std.mem.readInt(T, bytes, endian);
            self.stream.pos = end;
            return value;
        }

        pub fn readAll(self: Self, dest: []u8) error{}!usize {
            const available = self.stream.buffer.len - self.stream.pos;
            const n = @min(dest.len, available);
            @memcpy(dest[0..n], self.stream.buffer[self.stream.pos..][0..n]);
            self.stream.pos += n;
            return n;
        }
    };
}

pub const fs = struct {
    pub fn cwd() Cwd {
        return .{ .dir = .cwd() };
    }

    pub const Cwd = struct {
        dir: std.Io.Dir,

        pub fn deleteFile(self: Cwd, path: []const u8) !void {
            return self.dir.deleteFile(io, path);
        }

        pub fn access(self: Cwd, path: []const u8, options: std.Io.Dir.AccessOptions) !void {
            return self.dir.access(io, path, options);
        }

        pub fn openFile(self: Cwd, path: []const u8, options: std.Io.Dir.OpenFileOptions) !File {
            return .{ .file = try self.dir.openFile(io, path, options) };
        }

        pub fn createFile(self: Cwd, path: []const u8, options: std.Io.Dir.CreateFileOptions) !File {
            return .{ .file = try self.dir.createFile(io, path, options) };
        }
    };

    pub const File = struct {
        file: std.Io.File,

        pub fn close(self: File) void {
            self.file.close(io);
        }

        pub fn stat(self: File) !std.Io.File.Stat {
            return self.file.stat(io);
        }

        pub fn preadAll(self: File, buf: []u8, offset: u64) !usize {
            return self.file.readPositionalAll(io, buf, offset);
        }

        pub fn pwriteAll(self: File, bytes: []const u8, offset: u64) !void {
            return self.file.writePositionalAll(io, bytes, offset);
        }

        pub fn readToEndAlloc(self: File, allocator: std.mem.Allocator, max_bytes: usize) ![]u8 {
            var buf: [4096]u8 = undefined;
            var reader = self.file.reader(io, &buf);
            return reader.interface.allocRemaining(allocator, .limited(max_bytes));
        }

        pub fn deprecatedWriter(self: File) FileWriter {
            return .{ .file = self.file };
        }
    };

    pub const FileWriter = struct {
        file: std.Io.File,

        pub fn writeAll(self: FileWriter, bytes: []const u8) !void {
            return self.file.writeStreamingAll(io, bytes);
        }

        pub fn writeByte(self: FileWriter, byte: u8) !void {
            return self.writeAll(&.{byte});
        }

        pub fn print(self: FileWriter, comptime fmt: []const u8, args: anytype) !void {
            var buf: [512]u8 = undefined;
            const rendered = try std.fmt.bufPrint(&buf, fmt, args);
            try self.writeAll(rendered);
        }
    };
};
