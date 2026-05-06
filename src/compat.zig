const std = @import("std");

pub const io = std.Options.debug_io;

const has_io_fs = @hasDecl(std.Io, "Dir");
const has_thread_condition = @hasDecl(std.Thread, "Condition");
const MutexState = if (@hasDecl(std.atomic, "Mutex")) std.atomic.Mutex else std.Thread.Mutex;
const ConditionState = if (has_thread_condition) std.Thread.Condition else void;

pub const Mutex = struct {
    state: MutexState = if (@hasDecl(std.atomic, "Mutex")) .unlocked else .{},

    pub fn lock(self: *Mutex) void {
        if (@hasDecl(std.atomic, "Mutex")) {
            while (!self.state.tryLock()) {
                std.atomic.spinLoopHint();
            }
        } else {
            self.state.lock();
        }
    }

    pub fn unlock(self: *Mutex) void {
        self.state.unlock();
    }
};

pub const Condition = struct {
    state: ConditionState = if (has_thread_condition) .{} else {},

    pub fn broadcast(self: *Condition) void {
        if (has_thread_condition) {
            self.state.broadcast();
        }
    }

    pub fn timedWait(self: *Condition, mutex: *Mutex, ns: u64) !void {
        if (has_thread_condition) {
            return self.state.timedWait(&mutex.state, ns);
        }

        const poll_ns = @min(ns, 10 * std.time.ns_per_ms);
        mutex.unlock();
        sleep(poll_ns);
        mutex.lock();
    }
};

pub fn timestamp() i64 {
    if (@hasDecl(std.Io, "Clock")) {
        return @intCast(std.Io.Clock.real.now(io).toSeconds());
    }
    return std.time.timestamp();
}

pub fn milliTimestamp() i64 {
    if (@hasDecl(std.Io, "Clock")) {
        return std.Io.Clock.real.now(io).toMilliseconds();
    }
    return std.time.milliTimestamp();
}

pub fn nanoTimestamp() i128 {
    if (@hasDecl(std.Io, "Clock")) {
        return @intCast(std.Io.Clock.boot.now(io).toNanoseconds());
    }
    return std.time.nanoTimestamp();
}

pub fn sleep(ns: u64) void {
    if (@hasDecl(std.Io, "Clock")) {
        std.Io.Clock.Duration.sleep(.{
            .clock = .boot,
            .raw = .fromNanoseconds(@intCast(ns)),
        }, io) catch {};
    } else {
        std.Thread.sleep(ns);
    }
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

pub fn httpClient(allocator: std.mem.Allocator) std.http.Client {
    if (@hasField(std.http.Client, "io")) {
        return .{ .allocator = allocator, .io = io };
    }
    return .{ .allocator = allocator };
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
    const DirHandle = if (has_io_fs) std.Io.Dir else std.fs.Dir;
    const FileHandle = if (has_io_fs) std.Io.File else std.fs.File;
    const AccessOptions = if (has_io_fs) std.Io.Dir.AccessOptions else std.fs.File.OpenFlags;
    const OpenFileOptions = if (has_io_fs) std.Io.Dir.OpenFileOptions else std.fs.File.OpenFlags;
    const CreateFileOptions = if (has_io_fs) std.Io.Dir.CreateFileOptions else std.fs.File.CreateFlags;
    const FileStat = if (has_io_fs) std.Io.File.Stat else std.fs.File.Stat;
    const FileLock = if (has_io_fs) std.Io.File.Lock else std.fs.File.Lock;

    pub fn cwd() Cwd {
        return .{ .dir = if (has_io_fs) std.Io.Dir.cwd() else std.fs.cwd() };
    }

    pub const Cwd = struct {
        dir: DirHandle,

        pub fn deleteFile(self: Cwd, path: []const u8) !void {
            if (has_io_fs) {
                return self.dir.deleteFile(io, path);
            }
            return self.dir.deleteFile(path);
        }

        pub fn access(self: Cwd, path: []const u8, options: AccessOptions) !void {
            if (has_io_fs) {
                return self.dir.access(io, path, options);
            }
            return self.dir.access(path, options);
        }

        pub fn openFile(self: Cwd, path: []const u8, options: OpenFileOptions) !File {
            return .{ .file = if (has_io_fs) try self.dir.openFile(io, path, options) else try self.dir.openFile(path, options) };
        }

        pub fn createFile(self: Cwd, path: []const u8, options: CreateFileOptions) !File {
            return .{ .file = if (has_io_fs) try self.dir.createFile(io, path, options) else try self.dir.createFile(path, options) };
        }
    };

    pub const File = struct {
        file: FileHandle,

        pub fn close(self: File) void {
            if (has_io_fs) {
                self.file.close(io);
            } else {
                self.file.close();
            }
        }

        pub fn stat(self: File) !FileStat {
            if (has_io_fs) {
                return self.file.stat(io);
            }
            return self.file.stat();
        }

        pub fn preadAll(self: File, buf: []u8, offset: u64) !usize {
            if (has_io_fs) {
                return self.file.readPositionalAll(io, buf, offset);
            }
            return self.file.preadAll(buf, offset);
        }

        pub fn pwriteAll(self: File, bytes: []const u8, offset: u64) !void {
            if (has_io_fs) {
                return self.file.writePositionalAll(io, bytes, offset);
            }
            return self.file.pwriteAll(bytes, offset);
        }

        pub fn sync(self: File) !void {
            if (has_io_fs) {
                return self.file.sync(io);
            }
            return self.file.sync();
        }

        pub fn setLength(self: File, length: u64) !void {
            if (has_io_fs) {
                return self.file.setLength(io, length);
            }
            return self.file.setEndPos(length);
        }

        pub fn lock(self: File, lock_mode: FileLock) !void {
            if (has_io_fs) {
                return self.file.lock(io, lock_mode);
            }
            return self.file.lock(lock_mode);
        }

        pub fn unlock(self: File) void {
            if (has_io_fs) {
                self.file.unlock(io);
            } else {
                self.file.unlock();
            }
        }

        pub fn readToEndAlloc(self: File, allocator: std.mem.Allocator, max_bytes: usize) ![]u8 {
            if (!has_io_fs) {
                return self.file.readToEndAlloc(allocator, max_bytes);
            }
            var buf: [4096]u8 = undefined;
            var reader = self.file.reader(io, &buf);
            return reader.interface.allocRemaining(allocator, .limited(max_bytes));
        }

        pub fn deprecatedWriter(self: File) FileWriter {
            return .{ .file = self.file };
        }
    };

    pub const FileWriter = struct {
        file: FileHandle,

        pub fn writeAll(self: FileWriter, bytes: []const u8) !void {
            if (has_io_fs) {
                return self.file.writeStreamingAll(io, bytes);
            }
            return self.file.writeAll(bytes);
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
