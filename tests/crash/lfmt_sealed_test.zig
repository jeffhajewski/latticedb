//! Pages in use that a fault turned to zeros.
//!
//! A file 0.16.0 created extends itself with pages of zeros and no checksum,
//! so a page of zeros reads there as one nothing has written, even when a
//! tree refers to it. A file this release creates seals every page it adds
//! (`FileFlags.SEALED_PAGES`), so the same zeros in a page in use are damage
//! and reading it fails instead of returning an empty page.

const std = @import("std");
const lattice = @import("lattice");

const Allocator = std.mem.Allocator;
const Database = lattice.storage.database.Database;
const DatabaseConfig = lattice.storage.database.DatabaseConfig;
const FaultVfs = lattice.storage.fault_vfs.FaultVfs;
const PageType = lattice.storage.page.PageType;
const NodeId = lattice.core.types.NodeId;

const DB_PATH = "db";
const PAGE_SIZE = 4096;
const NODES = 400;

const config: DatabaseConfig = .{
    .enable_fts = false,
    .enable_vector = false,
    .enable_query_cache = false,
};

fn open(allocator: Allocator, fs: *FaultVfs, create: bool) !*Database {
    return Database.openWithVfs(allocator, DB_PATH, .{ .create = create, .config = config }, fs.vfs());
}

/// Whether every node and its label still read back. An error on the way
/// counts as noticing the damage, which is what the test wants.
fn readsEverything(db: *Database, ids: []const NodeId) !bool {
    for (ids) |id| {
        if (!try db.nodeExists(id)) return false;
    }
    const labelled = try db.getNodesByLabel("Item");
    defer db.allocator.free(labelled);
    return labelled.len == ids.len;
}

test "lfmt a page in use that turned to zeros is damage, not an empty page" {
    const allocator = std.testing.allocator;

    var fs = FaultVfs.init(allocator);
    defer fs.deinit();
    var ids: [NODES]NodeId = undefined;
    {
        var db = try open(allocator, &fs, true);
        defer db.close();
        var txn = try db.beginTransaction(.read_write);
        for (&ids) |*id| id.* = try db.createNode(&txn, &.{"Item"});
        try db.commitTransaction(&txn);
    }

    // The file as the clean close left it, and the pages in it in use.
    const image = blk: {
        const file = try fs.vfs().open(DB_PATH, .{});
        defer file.close();
        const bytes = try allocator.alloc(u8, @intCast(try file.size()));
        errdefer allocator.free(bytes);
        try std.testing.expectEqual(bytes.len, try file.read(0, bytes));
        break :blk bytes;
    };
    defer allocator.free(image);

    var damaged_pages: usize = 0;
    var page_id: usize = 1;
    while (page_id < image.len / PAGE_SIZE) : (page_id += 1) {
        const page_type = image[page_id * PAGE_SIZE];
        if (page_type == @intFromEnum(PageType.free)) continue;
        damaged_pages += 1;

        var copy = FaultVfs.init(allocator);
        defer copy.deinit();
        {
            const file = try copy.vfs().open(DB_PATH, .{ .create = true, .write = true });
            defer file.close();
            try file.write(0, image);
            const zeros = [_]u8{0} ** PAGE_SIZE;
            try file.write(page_id * PAGE_SIZE, &zeros);
        }

        const db = open(allocator, &copy, false) catch continue;
        defer db.close();
        const intact = readsEverything(db, &ids) catch continue;
        // The page was in use: its zeros either fail a read or lose data,
        // and losing data quietly is what the seal rules out.
        if (intact) continue;
        std.debug.print("page {d} (type 0x{x:0>2}) read back as empty\n", .{ page_id, page_type });
        return error.TestUnexpectedResult;
    }
    try std.testing.expect(damaged_pages > 0);
}
