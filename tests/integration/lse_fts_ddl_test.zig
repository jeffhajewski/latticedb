//! Declaring a full-text index either happens or does not: an error from
//! the declaration leaves no index behind for the next attempt to trip on.

const std = @import("std");
const lattice = @import("lattice");

const Database = lattice.storage.database.Database;

const path = "/tmp/lse_fts_ddl_sync.ltdb";

fn removeFiles() void {
    @import("compat").fs.cwd().deleteFile(path) catch {};
    @import("compat").fs.cwd().deleteFile(path ++ "-wal") catch {};
}

fn open(allocator: std.mem.Allocator, create: bool) !*Database {
    return Database.open(allocator, path, .{
        .create = create,
        .config = .{ .enable_fts = true, .enable_vector = false },
    });
}

test "fts ddl: an index whose declaration could not be saved can be declared again" {
    const allocator = std.testing.allocator;
    removeFiles();
    defer removeFiles();

    {
        const db = try open(allocator, true);
        defer db.close();
        for ([_][]const u8{ "graph databases", "vector search", "graph traversal" }) |text| {
            const id = try db.createNode(null, &[_][]const u8{"Doc"});
            try db.setNodeProperty(null, id, "text", .{ .string_val = text });
        }
        try db.sync();

        // The final write of the declaration fails, as on a full disk.
        db.failed = true;
        try std.testing.expectError(error.IoError, db.createNodeFtsIndex("Doc", "text"));
        db.failed = false;

        // Asking again builds it. Before, this answered AlreadyExists for an
        // index the file never recorded.
        try db.createNodeFtsIndex("Doc", "text");
        const found = try db.ftsSearchIndex(.node, "Doc", "text", "graph", 10);
        defer db.freeFtsSearchResults(found);
        // Each document once: the failed attempt's entries are gone.
        try std.testing.expectEqual(@as(usize, 2), found.len);
    }

    const db = try open(allocator, false);
    defer db.close();
    const found = try db.ftsSearchIndex(.node, "Doc", "text", "graph", 10);
    defer db.freeFtsSearchResults(found);
    try std.testing.expectEqual(@as(usize, 2), found.len);
}
