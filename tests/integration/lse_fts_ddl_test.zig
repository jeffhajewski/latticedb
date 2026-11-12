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

/// The oldest release the file lets open it, and why, as the file on disk
/// says.
fn minReaderOnDisk(comptime p: []const u8) !struct { version: u16, reasons: u16 } {
    var buf: [4096]u8 = undefined;
    const file = try @import("compat").fs.cwd().openFile(p, .{});
    defer file.close();
    _ = try file.preadAll(&buf, 0);
    const header = std.mem.bytesToValue(lattice.storage.page.FileHeader, buf[0..@sizeOf(lattice.storage.page.FileHeader)]);
    return .{ .version = header.min_reader_version, .reasons = header.readerReasons() };
}

test "fts ddl: an index that stores positions keeps 0.16.0 from opening the file" {
    const allocator = std.testing.allocator;
    const types = lattice.core.types;
    const p = "/tmp/lse_fts_ddl_positions.ltdb";
    inline for (.{ true, false }) |positions| {
        @import("compat").fs.cwd().deleteFile(p) catch {};
        @import("compat").fs.cwd().deleteFile(p ++ "-wal") catch {};
        defer @import("compat").fs.cwd().deleteFile(p) catch {};
        defer @import("compat").fs.cwd().deleteFile(p ++ "-wal") catch {};
        var config: lattice.storage.database.DatabaseConfig = .{ .enable_fts = true, .enable_vector = false };
        config.fts_config.store_positions = positions;
        // Changefeed events would raise the version on their own.
        config.changefeed_retention = .off;
        {
            const db = try Database.open(allocator, p, .{ .create = true, .config = config });
            defer db.close();
            try db.createNodeFtsIndex("Doc", "text");
        }
        // Declared and empty: nothing 0.16.0 would damage yet.
        try std.testing.expectEqual(types.MIN_READER_VERSION, (try minReaderOnDisk(p)).version);
        {
            const db = try Database.open(allocator, p, .{ .config = config });
            defer db.close();
            const id = try db.createNode(null, &[_][]const u8{"Doc"});
            try db.setNodeProperty(null, id, "text", .{ .string_val = "the quick brown fox" });
        }
        // 0.16.0 appends without position counts to a page whose entries
        // carry them, so once the index stores positions the file asks for
        // a reader of this format.
        const on_disk = try minReaderOnDisk(p);
        try std.testing.expectEqual(if (positions) types.FORMAT_VERSION else types.MIN_READER_VERSION, on_disk.version);
        const Reason = lattice.storage.page.FileHeader.ReaderReason;
        try std.testing.expectEqual(if (positions) Reason.fulltext_positions else @as(u16, 0), on_disk.reasons);
    }
}
