//! Double-write journal: makes a flush of many pages to the database file
//! all-or-nothing.
//!
//! The write-ahead log holds logical records, not page images, so it can only
//! repair a database file that is a complete, consistent image as of some
//! commit. Pages written in place one at a time do not keep the file in that
//! state: a crash partway through leaves some pages of a B+Tree split new and
//! others old, and nothing in the log can put that right.
//!
//! So every flush goes through here first. The pages about to be written are
//! copied to `<database>-journal` and synced, and only then written in place.
//! If the process dies during the in-place writes, the next open finds a
//! complete journal and writes the same pages again. If it dies while the
//! journal is being written, the journal fails its checksums and is ignored,
//! and the database file has not been touched. Either way the file is a whole
//! image, before the flush or after it.
//!
//! Layout:
//!   header        4096 bytes
//!   table         one (page id, checksum) pair per page, padded to 4096
//!   page images   `page_size` bytes each, in table order

const std = @import("std");
const lattice = @import("lattice");

const Allocator = std.mem.Allocator;

const vfs_mod = lattice.storage.vfs;
const page = lattice.storage.page;
const types = lattice.core.types;

const Vfs = vfs_mod.Vfs;
const File = vfs_mod.File;
const PageId = types.PageId;

pub const JOURNAL_MAGIC: u32 = 0x4C54444A; // "LTDJ"
pub const JOURNAL_VERSION: u32 = 1;
const BLOCK: usize = 4096;

pub const JournalError = error{
    IoError,
    OutOfMemory,
    /// A complete journal is waiting, and this handle may not write the
    /// database file to apply it.
    NeedsRecovery,
};

const Header = extern struct {
    magic: u32 = JOURNAL_MAGIC,
    version: u32 = JOURNAL_VERSION,
    page_size: u32,
    page_count: u32,
    /// The database this journal belongs to, so that one left beside a
    /// different file of the same name is not applied to it.
    file_uuid: [16]u8,
    table_checksum: u32,
    /// Checksum of every field above.
    header_checksum: u32,

    fn checksum(self: *const Header) u32 {
        const bytes = std.mem.asBytes(self);
        return page.calculateChecksum(bytes[0 .. bytes.len - 4]);
    }
};

const TableEntry = extern struct {
    page_id: u32,
    checksum: u32,
};

fn tableBytes(page_count: usize) usize {
    const raw = page_count * @sizeOf(TableEntry);
    return std.mem.alignForward(usize, raw, BLOCK);
}

/// One page to be flushed. `data` is the exact bytes that will be written in
/// place, checksum already stamped.
pub const PageImage = struct {
    page_id: PageId,
    data: []const u8,
};

pub const Journal = struct {
    allocator: Allocator,
    vfs: Vfs,
    file: File,
    /// Set while the journal's directory entry may not be on disk yet.
    unsynced_dir_path: ?[]u8 = null,

    const Self = @This();

    /// Open the journal for a database that is open for writing, creating it
    /// if needed. Any journal left by a crash has already been dealt with by
    /// `recover`, which the page manager runs before it reads the header.
    pub fn open(allocator: Allocator, vfs_impl: Vfs, path: []const u8) JournalError!Self {
        const file = vfs_impl.open(path, .{ .read = true, .write = true, .create = true }) catch {
            return JournalError.IoError;
        };
        return .{ .allocator = allocator, .vfs = vfs_impl, .file = file };
    }

    pub fn deinit(self: *Self) void {
        if (self.unsynced_dir_path) |path| self.allocator.free(path);
        self.file.close();
    }

    /// Have the first `write` make the journal's directory entry durable.
    ///
    /// The journal is created at open and removed at close, so in every
    /// session it is a new file. fsync covers a file's contents and not the
    /// name that leads to it: without this, a power cut during a flush could
    /// leave pages half written in place and no journal to be found at the
    /// next open. It is done at the first write and not at open so that a
    /// session that never flushes pays nothing for it.
    pub fn syncDirectoryOnFirstWrite(self: *Self, path: []const u8) JournalError!void {
        if (self.unsynced_dir_path != null) return;
        self.unsynced_dir_path = self.allocator.dupe(u8, path) catch return JournalError.OutOfMemory;
    }

    /// Record the pages and make the record durable. When this returns, the
    /// pages may be written in place.
    pub fn write(self: *Self, file_uuid: [16]u8, page_size: u32, pages: []const PageImage) JournalError!void {
        // Nothing from an earlier flush may survive to be read as part of
        // this one.
        self.file.truncate(0) catch return JournalError.IoError;

        const table = self.allocator.alloc(u8, tableBytes(pages.len)) catch return JournalError.OutOfMemory;
        defer self.allocator.free(table);
        @memset(table, 0);

        var offset: u64 = BLOCK + table.len;
        for (pages, 0..) |image, i| {
            std.debug.assert(image.data.len == page_size);
            const entry = TableEntry{
                .page_id = image.page_id,
                .checksum = page.calculateChecksum(image.data),
            };
            @memcpy(table[i * @sizeOf(TableEntry) ..][0..@sizeOf(TableEntry)], std.mem.asBytes(&entry));
            self.file.write(offset, image.data) catch return JournalError.IoError;
            offset += page_size;
        }
        self.file.write(BLOCK, table) catch return JournalError.IoError;

        var header = Header{
            .page_size = page_size,
            .page_count = @intCast(pages.len),
            .file_uuid = file_uuid,
            .table_checksum = page.calculateChecksum(table),
            .header_checksum = 0,
        };
        header.header_checksum = header.checksum();
        var block = [_]u8{0} ** BLOCK;
        @memcpy(block[0..@sizeOf(Header)], std.mem.asBytes(&header));
        self.file.write(0, &block) catch return JournalError.IoError;

        self.file.sync() catch return JournalError.IoError;

        if (self.unsynced_dir_path) |path| {
            self.vfs.syncParentDir(path);
            self.allocator.free(path);
            self.unsynced_dir_path = null;
        }
    }

    /// The pages are in place and synced; the journal has done its job.
    ///
    /// Not synced. If the truncation is lost, the next open applies the
    /// journal again, which rewrites pages with the bytes they already hold.
    pub fn clear(self: *Self) JournalError!void {
        self.file.truncate(0) catch return JournalError.IoError;
    }
};

/// Finish a flush that a crash interrupted.
///
/// Called with the database file open and locked, before anything reads it,
/// because the header page itself may be one of the pages half written.
/// Returns true if a journal was applied.
///
/// A journal that does not verify in full is the trace of a crash during the
/// journal write. The database file was not touched in that case, so the
/// journal is simply discarded.
pub fn recover(
    allocator: Allocator,
    vfs_impl: Vfs,
    journal_path: []const u8,
    db_file: File,
    read_only: bool,
) JournalError!bool {
    if (!vfs_impl.exists(journal_path)) return false;

    var journal_file = vfs_impl.open(journal_path, .{ .read = true, .write = !read_only }) catch {
        // Unreadable is not the same as absent, but there is nothing here to
        // apply and refusing the database for it would help nobody.
        return false;
    };
    defer journal_file.close();

    const applied = try applyIfComplete(allocator, journal_file, db_file, read_only);

    if (!read_only) {
        journal_file.truncate(0) catch return JournalError.IoError;
        journal_file.sync() catch return JournalError.IoError;
    }
    return applied;
}

fn applyIfComplete(allocator: Allocator, journal_file: File, db_file: File, read_only: bool) JournalError!bool {
    const size = journal_file.size() catch return JournalError.IoError;
    if (size < BLOCK) return false;

    var block: [BLOCK]u8 = undefined;
    const n = journal_file.read(0, &block) catch return JournalError.IoError;
    if (n != BLOCK) return false;

    var header: Header = undefined;
    @memcpy(std.mem.asBytes(&header), block[0..@sizeOf(Header)]);
    if (header.magic != JOURNAL_MAGIC or header.version != JOURNAL_VERSION) return false;
    if (header.header_checksum != header.checksum()) return false;
    if (header.page_count == 0) return false;
    if (header.page_size < BLOCK or header.page_size > std.math.maxInt(u16)) return false;

    const table_len = tableBytes(header.page_count);
    const expected_size = @as(u64, BLOCK) + table_len + @as(u64, header.page_count) * header.page_size;
    if (size < expected_size) return false;

    const table = allocator.alloc(u8, table_len) catch return JournalError.OutOfMemory;
    defer allocator.free(table);
    const table_read = journal_file.read(BLOCK, table) catch return JournalError.IoError;
    if (table_read != table_len) return false;
    if (page.calculateChecksum(table) != header.table_checksum) return false;

    // A journal left next to a different database must not be applied to it.
    // The check is skipped when the database's own header cannot be read:
    // that header may be one of the pages this journal is here to restore.
    var db_block: [BLOCK]u8 = undefined;
    const db_read = db_file.read(0, &db_block) catch 0;
    if (db_read == BLOCK) {
        const db_header = std.mem.bytesAsValue(page.FileHeader, db_block[0..@sizeOf(page.FileHeader)]);
        if (db_header.magic == types.MAGIC_NUMBER and
            !std.mem.eql(u8, &db_header.file_uuid, &header.file_uuid))
        {
            return false;
        }
    }

    const image = allocator.alloc(u8, header.page_size) catch return JournalError.OutOfMemory;
    defer allocator.free(image);

    // Every image has to verify before any is applied. Applying the good half
    // of an incomplete journal would create the torn state it exists to
    // prevent.
    const images_start = @as(u64, BLOCK) + table_len;
    var i: usize = 0;
    while (i < header.page_count) : (i += 1) {
        const entry = std.mem.bytesAsValue(TableEntry, table[i * @sizeOf(TableEntry) ..][0..@sizeOf(TableEntry)]);
        const got = journal_file.read(images_start + @as(u64, i) * header.page_size, image) catch return JournalError.IoError;
        if (got != header.page_size) return false;
        if (page.calculateChecksum(image) != entry.checksum) return false;
    }

    if (read_only) return JournalError.NeedsRecovery;

    i = 0;
    while (i < header.page_count) : (i += 1) {
        const entry = std.mem.bytesAsValue(TableEntry, table[i * @sizeOf(TableEntry) ..][0..@sizeOf(TableEntry)]);
        const got = journal_file.read(images_start + @as(u64, i) * header.page_size, image) catch return JournalError.IoError;
        if (got != header.page_size) return JournalError.IoError;
        db_file.write(@as(u64, entry.page_id) * header.page_size, image) catch return JournalError.IoError;
    }
    db_file.sync() catch return JournalError.IoError;
    return true;
}

// ============================================================================
// Tests
// ============================================================================

const TestFiles = struct {
    posix: vfs_mod.PosixVfs,
    db_path: []const u8,
    journal_path: []const u8,

    fn init(comptime name: []const u8) TestFiles {
        var files = TestFiles{
            .posix = vfs_mod.PosixVfs.init(std.testing.allocator),
            .db_path = "/tmp/lattice_journal_test_" ++ name ++ ".db",
            .journal_path = "/tmp/lattice_journal_test_" ++ name ++ ".db-journal",
        };
        files.remove();
        return files;
    }

    fn remove(self: *TestFiles) void {
        self.posix.vfs().delete(self.db_path) catch {};
        self.posix.vfs().delete(self.journal_path) catch {};
    }

    /// A database file of `pages` pages, each filled with `fill`.
    fn createDb(self: *TestFiles, pages: usize, fill: u8) !File {
        const file = try self.posix.vfs().open(self.db_path, .{ .read = true, .write = true, .create = true });
        const buf = [_]u8{fill} ** BLOCK;
        for (0..pages) |i| try file.write(i * BLOCK, &buf);
        return file;
    }

    fn expectPage(file: File, page_id: usize, fill: u8) !void {
        var buf: [BLOCK]u8 = undefined;
        try std.testing.expectEqual(@as(usize, BLOCK), try file.read(page_id * BLOCK, &buf));
        for (buf) |byte| try std.testing.expectEqual(fill, byte);
    }
};

test "journal: a complete journal is applied and then emptied" {
    const allocator = std.testing.allocator;
    var files = TestFiles.init("applied");
    defer files.remove();
    const db = try files.createDb(4, 0xAA);
    defer db.close();

    const new_one = [_]u8{0x11} ** BLOCK;
    const new_three = [_]u8{0x33} ** BLOCK;
    {
        var journal = try Journal.open(allocator, files.posix.vfs(), files.journal_path);
        defer journal.deinit();
        // The directory entry is made durable by the first write, once.
        try journal.syncDirectoryOnFirstWrite(files.journal_path);
        try std.testing.expect(journal.unsynced_dir_path != null);
        try journal.write([_]u8{7} ** 16, BLOCK, &.{
            .{ .page_id = 1, .data = &new_one },
            .{ .page_id = 3, .data = &new_three },
        });
        try std.testing.expect(journal.unsynced_dir_path == null);
    }

    try std.testing.expect(try recover(allocator, files.posix.vfs(), files.journal_path, db, false));
    try TestFiles.expectPage(db, 0, 0xAA);
    try TestFiles.expectPage(db, 1, 0x11);
    try TestFiles.expectPage(db, 2, 0xAA);
    try TestFiles.expectPage(db, 3, 0x33);

    // Nothing left to apply a second time.
    try std.testing.expect(!try recover(allocator, files.posix.vfs(), files.journal_path, db, false));
}

test "journal: a journal cut short is discarded and the database is not touched" {
    const allocator = std.testing.allocator;
    var files = TestFiles.init("cut_short");
    defer files.remove();
    const db = try files.createDb(4, 0xAA);
    defer db.close();

    const new_one = [_]u8{0x11} ** BLOCK;
    const new_three = [_]u8{0x33} ** BLOCK;
    const images = [_]PageImage{
        .{ .page_id = 1, .data = &new_one },
        .{ .page_id = 3, .data = &new_three },
    };

    // Three ways a crash can leave the journal: missing its tail, with a
    // damaged page image, and with a damaged header.
    const Damage = enum { truncated, bad_image, bad_header };
    for ([_]Damage{ .truncated, .bad_image, .bad_header }) |damage| {
        {
            var journal = try Journal.open(allocator, files.posix.vfs(), files.journal_path);
            defer journal.deinit();
            try journal.write([_]u8{7} ** 16, BLOCK, &images);
            const size = try journal.file.size();
            switch (damage) {
                .truncated => try journal.file.truncate(size - BLOCK / 2),
                // The second image is the last block of the file.
                .bad_image => try journal.file.write(size - 100, "damage"),
                .bad_header => try journal.file.write(8, "damage"),
            }
        }

        try std.testing.expect(!try recover(allocator, files.posix.vfs(), files.journal_path, db, false));
        for (0..4) |page_id| try TestFiles.expectPage(db, page_id, 0xAA);
    }
}

test "journal: a journal from another database is not applied" {
    const allocator = std.testing.allocator;
    var files = TestFiles.init("foreign");
    defer files.remove();

    // A database file with a real header, so its identity can be read.
    const db = try files.posix.vfs().open(files.db_path, .{ .read = true, .write = true, .create = true });
    defer db.close();
    var header = page.FileHeader.init();
    header.file_uuid = [_]u8{1} ** 16;
    var header_page = [_]u8{0} ** BLOCK;
    @memcpy(header_page[0..@sizeOf(page.FileHeader)], std.mem.asBytes(&header));
    try db.write(0, &header_page);
    const old_one = [_]u8{0xAA} ** BLOCK;
    try db.write(BLOCK, &old_one);

    const new_one = [_]u8{0x11} ** BLOCK;
    {
        var journal = try Journal.open(allocator, files.posix.vfs(), files.journal_path);
        defer journal.deinit();
        try journal.write([_]u8{2} ** 16, BLOCK, &.{.{ .page_id = 1, .data = &new_one }});
    }

    try std.testing.expect(!try recover(allocator, files.posix.vfs(), files.journal_path, db, false));
    try TestFiles.expectPage(db, 1, 0xAA);
}

test "journal: a read-only open is refused while a complete journal waits" {
    const allocator = std.testing.allocator;
    var files = TestFiles.init("read_only");
    defer files.remove();
    const db = try files.createDb(2, 0xAA);
    defer db.close();

    const new_one = [_]u8{0x11} ** BLOCK;
    {
        var journal = try Journal.open(allocator, files.posix.vfs(), files.journal_path);
        defer journal.deinit();
        try journal.write([_]u8{7} ** 16, BLOCK, &.{.{ .page_id = 1, .data = &new_one }});
    }

    try std.testing.expectError(
        JournalError.NeedsRecovery,
        recover(allocator, files.posix.vfs(), files.journal_path, db, true),
    );
    try TestFiles.expectPage(db, 1, 0xAA);

    // A writer finishes the job, and readers are welcome again.
    try std.testing.expect(try recover(allocator, files.posix.vfs(), files.journal_path, db, false));
    try TestFiles.expectPage(db, 1, 0x11);
    try std.testing.expect(!try recover(allocator, files.posix.vfs(), files.journal_path, db, true));
}
