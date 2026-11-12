//! Where a database file's bytes go: one line per B+Tree.
//!
//! Opens the file read-only, walks every page of every tree named in the
//! header, and prints pages, entries, bytes of keys and values, and how full
//! the leaf and internal pages are. The stream events tree is also split by
//! stream, because the built-in changefeed and user streams share it.
//!
//! Run with: zig build space-report -- <database file>

const std = @import("std");
const lattice = @import("lattice");

const Database = lattice.storage.database.Database;
const BTree = lattice.storage.btree.BTree;
const page = lattice.storage.page;
const types = lattice.core.types;

const allocator = std.heap.c_allocator;

fn print(comptime fmt: []const u8, args: anytype) void {
    std.debug.print(fmt, args);
}

fn mb(bytes: u64) f64 {
    return @as(f64, @floatFromInt(bytes)) / 1e6;
}

pub fn main(init: std.process.Init) !void {
    var args = try init.minimal.args.iterateAllocator(allocator);
    defer args.deinit();
    _ = args.skip();
    const path = args.next() orelse {
        print("usage: space-report <database file>\n", .{});
        return error.InvalidArgument;
    };

    const db = try Database.open(allocator, path, .{
        .read_only = true,
        .config = .{ .enable_fts = false },
    });
    defer db.close();

    const header = db.page_manager.getHeader();
    const page_size: u64 = header.page_size;
    const file_pages = db.page_manager.pageCount();

    print("{s}: {d} pages of {d} bytes = {d:.1} MB\n\n", .{ path, file_pages, page_size, mb(file_pages * page_size) });
    print("{s:<24} {s:>8} {s:>9} {s:>9} {s:>10} {s:>10} {s:>6} {s:>6} {s:>9}\n", .{
        "tree", "pages", "MB", "entries", "key B", "value B", "leaf%", "int%", "B/entry",
    });

    var tree_pages: u64 = 0;
    inline for (std.meta.fields(page.TreeIndex)) |field| {
        const index: page.TreeIndex = @enumFromInt(field.value);
        const root = header.getTreeRoot(index);
        if (root != types.NULL_PAGE) {
            var tree = BTree.open(allocator, &db.buffer_pool, root);
            const s = try tree.spaceStats();
            tree_pages += s.pages();
            const per_entry: f64 = if (s.entries == 0) 0 else @as(f64, @floatFromInt(s.pages() * page_size)) / @as(f64, @floatFromInt(s.entries));
            print("{s:<24} {d:>8} {d:>9.2} {d:>9} {d:>10} {d:>10} {d:>6.1} {d:>6.1} {d:>9.1}\n", .{
                field.name,
                s.pages(),
                mb(s.pages() * page_size),
                s.entries,
                s.key_bytes,
                s.value_bytes,
                s.leafFill() * 100,
                s.internalFill() * 100,
                per_entry,
            });
        }
    }
    print("{s:<24} {d:>8} {d:>9.2}\n", .{ "other (vectors, free, header)", file_pages - @min(file_pages, tree_pages), mb((file_pages - @min(file_pages, tree_pages)) * page_size) });

    // The events tree, split by the stream each entry belongs to. The key
    // starts with the stream name's length; a length of zero is the compact
    // changefeed layout.
    const events_root = header.getTreeRoot(.stream_events);
    if (events_root == types.NULL_PAGE) return;
    var events = BTree.open(allocator, &db.buffer_pool, events_root);
    var iter = try events.range(null, null);
    defer iter.deinit();

    const Group = struct { entries: u64 = 0, key_bytes: u64 = 0, value_bytes: u64 = 0 };
    var groups: std.StringArrayHashMapUnmanaged(Group) = .empty;
    defer {
        for (groups.keys()) |k| allocator.free(k);
        groups.deinit(allocator);
    }
    while (try iter.next()) |entry| {
        const name_len: usize = if (entry.key.len > 0) entry.key[0] else 0;
        const name: []const u8 = if (name_len == 0)
            "(compact changefeed)"
        else if (entry.key.len >= 1 + name_len)
            entry.key[1..][0..name_len]
        else
            "(malformed)";
        const gop = try groups.getOrPut(allocator, name);
        if (!gop.found_existing) {
            gop.key_ptr.* = try allocator.dupe(u8, name);
            gop.value_ptr.* = .{};
        }
        gop.value_ptr.entries += 1;
        gop.value_ptr.key_bytes += entry.key.len;
        gop.value_ptr.value_bytes += entry.value.len;
    }
    print("\nstream_events by stream:\n", .{});
    print("{s:<24} {s:>9} {s:>10} {s:>10} {s:>9} {s:>9}\n", .{ "stream", "events", "key B", "value B", "key/ev", "value/ev" });
    var it = groups.iterator();
    while (it.next()) |g| {
        const n: f64 = @floatFromInt(@max(g.value_ptr.entries, 1));
        print("{s:<24} {d:>9} {d:>10} {d:>10} {d:>9.1} {d:>9.1}\n", .{
            g.key_ptr.*,
            g.value_ptr.entries,
            g.value_ptr.key_bytes,
            g.value_ptr.value_bytes,
            @as(f64, @floatFromInt(g.value_ptr.key_bytes)) / n,
            @as(f64, @floatFromInt(g.value_ptr.value_bytes)) / n,
        });
    }
}
