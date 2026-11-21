//! Upgrade a database whose id keys are little-endian to format 5.
//!
//! Files created before format 5 write node, edge and document ids into five
//! B-trees as little-endian keys (see `core/id_key.zig`). They keep working
//! as they are. This module turns one into a format-5 file, with the same
//! contents and big-endian keys, for anyone who wants the ordered layout.
//!
//! ## Never in place
//!
//! The original is not touched until a converted copy of it has been checked.
//! `upgrade` takes a copy with `Database.backup`, converts the copy, opens it
//! the ordinary way and compares it with the still-open original, store by
//! store and entry by entry. Anything wrong and the copy is deleted. Only
//! `replaceWithUpgraded`, run after the original is closed, puts the copy
//! in the original's place, by one rename.
//!
//! Because the copy is thrown away on any failure, the conversion itself
//! does not have to survive a crash: it writes with no log and no journal.
//!
//! ## What is converted
//!
//! The node tree, the edge-id index, the full-text document lengths and
//! reverse index, and the vector index's node tree. For each, every key is
//! read, re-encoded, sorted, and inserted into a new tree in key order, which
//! fills its leaves; each value is fetched from the old tree by its old key.
//! Then every page of the old tree is freed. The smallest tree goes first so
//! that the larger ones can reuse the pages it gave back.
//!
//! ## Pages
//!
//! A page freed while something still uses it does no harm until it is
//! reused, and then it corrupts whatever was using it, long after any check
//! of the data has passed. So the pages are accounted for: before the
//! conversion and after it, every B-tree in the file is walked and each page
//! it owns is marked, along with the freelist. A page claimed twice fails the
//! upgrade. The conversion frees only pages the walk found in the old tree
//! being replaced, and checks each against the accounting first.

const std = @import("std");
const builtin = @import("builtin");
const lattice = @import("lattice");
const compat = @import("compat");

const Allocator = std.mem.Allocator;
const types = lattice.core.types;
const PageId = types.PageId;
const NULL_PAGE = types.NULL_PAGE;
const IdKeyOrder = lattice.core.id_key.IdKeyOrder;
const page_mod = lattice.storage.page;
const FileHeader = page_mod.FileHeader;
const FileFlags = page_mod.FileFlags;
const TreeIndex = page_mod.TreeIndex;
const PageHeader = page_mod.PageHeader;
const MAX_TREES = page_mod.MAX_TREES;
const PageManager = lattice.storage.page_manager.PageManager;
const BufferPool = lattice.storage.buffer_pool.BufferPool;
const BTree = lattice.storage.btree.BTree;
const BTreeError = lattice.storage.btree.BTreeError;
const PosixVfs = lattice.storage.vfs.PosixVfs;
const WAL_HEADER_SIZE = lattice.storage.wal.WAL_HEADER_SIZE;
const scoped_tree = lattice.fts.scoped_tree;
const ScopedTree = scoped_tree.ScopedTree;
const DocLengthStore = lattice.fts.scorer.DocLengthStore;
const HnswIndex = lattice.vector.hnsw.HnswIndex;
const EdgeStore = lattice.graph.edge.EdgeStore;
const Property = lattice.graph.node.Property;
const PropertyValue = types.PropertyValue;
const database = lattice.storage.database;
const Database = database.Database;
const DatabaseError = database.DatabaseError;

pub const UpgradeError = DatabaseError || error{
    /// The database has no file (`:memory:`), so there is nothing to upgrade
    /// on disk. Export it and import it into a new database instead.
    InMemoryDatabase,
    /// The handle stopped accepting writes after a failure (`Database.failed`).
    /// Close it and open the database again first.
    HandleFailed,
    /// One of the id-keyed trees holds a key whose shape the upgrade does not
    /// know, so it cannot say how to re-encode it.
    UnexpectedKey,
    /// A page belongs to two structures, or a tree reaches a page that is
    /// not one of its own.
    PagesOwnedTwice,
    /// The converted copy does not match the database it was made from.
    VerificationFailed,
};

/// What went wrong, in words, for an upgrade that failed.
pub const Diagnostic = struct {
    buf: [400]u8 = undefined,
    len: usize = 0,

    pub fn message(self: *const Diagnostic) []const u8 {
        return self.buf[0..self.len];
    }

    fn set(self: ?*Diagnostic, comptime fmt: []const u8, args: anytype) void {
        const d = self orelse return;
        if (d.len != 0) return; // The first failure is the one that matters.
        const written = std.fmt.bufPrint(&d.buf, fmt, args) catch d.buf[0..];
        d.len = written.len;
    }
};

/// Where an upgrade is made to fail, for tests.
pub const Fault = enum {
    /// After the copy is written, before it is converted.
    after_copy,
    /// After the first tree is converted and its old pages freed.
    mid_conversion,
    /// Give the first two nodes each other's records, as a mistake in the key
    /// transform would. The check after the conversion must catch it.
    swap_node_values,
};

/// The five trees whose keys are ids.
pub const IdTree = enum {
    node,
    edge_id_index,
    fts_lengths,
    fts_reverse,
    hnsw_node,

    pub const all = [_]IdTree{ .node, .edge_id_index, .fts_lengths, .fts_reverse, .hnsw_node };

    pub fn treeIndex(self: IdTree) TreeIndex {
        return switch (self) {
            .node => .node,
            .edge_id_index => .edge_id_index,
            .fts_lengths => .fts_lengths,
            .fts_reverse => .fts_reverse,
            .hnsw_node => .hnsw_node,
        };
    }

    pub fn name(self: IdTree) []const u8 {
        return switch (self) {
            .node => "node tree",
            .edge_id_index => "edge id index",
            .fts_lengths => "full-text document lengths",
            .fts_reverse => "full-text reverse index",
            .hnsw_node => "vector index node tree",
        };
    }
};

/// How one tree came through.
pub const TreeStats = struct {
    /// Whether the file has this tree at all.
    present: bool = false,
    entries: u64 = 0,
    pages_before: u64 = 0,
    pages_after: u64 = 0,
    leaf_fill_before: f64 = 0,
    leaf_fill_after: f64 = 0,
};

pub const UpgradeStats = struct {
    /// The database already had big-endian id keys; nothing was done.
    already_ordered: bool = false,
    trees: [IdTree.all.len]TreeStats = [_]TreeStats{.{}} ** IdTree.all.len,
    /// Pages of the old trees given back to the freelist.
    pages_freed: u64 = 0,
    /// Size of the source file and of the converted copy, in bytes.
    bytes_before: u64 = 0,
    bytes_after: u64 = 0,
    /// Entries compared between the source and the copy, all trees together.
    entries_verified: u64 = 0,
    /// Pages of the copy on the freelist, and pages no B-tree or freelist
    /// claims (vector data, neighbour lists, postings, leaked pages).
    free_pages_after: u64 = 0,
    other_pages_after: u64 = 0,

    pub fn tree(self: *const UpgradeStats, which: IdTree) TreeStats {
        return self.trees[@intFromEnum(which)];
    }
};

// ============================================================================
// Key shapes
// ============================================================================

/// The longest key any of the five trees holds: a full-text scope prefix
/// and an id.
pub const MAX_KEY_LEN = scoped_tree.PREFIX_SIZE + 8;

pub const KeyShape = union(enum) {
    /// An id, after this many bytes that stay as they are.
    id: u8,
    /// Not an id: kept byte for byte.
    fixed,
};

/// What a key in `tree` is, or null for a key the upgrade does not know.
///
/// - node tree: the 8-byte node id (`graph/node.zig`, `NodeStore`).
/// - edge-id index: the 8-byte edge id, and the counter under id 0
///   (`EdgeStore.EDGE_ID_META_KEY`), which reads the same either way.
/// - document lengths: the 8-byte document id, and the 6-byte statistics and
///   analyzer keys (`DocLengthStore.STATS_KEY`, `ANALYSIS_KEY`); each of
///   them also behind a 4-byte scope prefix (`fts/scoped_tree.zig`).
/// - reverse index: the 8-byte document id, alone or behind a scope prefix.
/// - vector node tree: the 8-byte vector id, and the metadata key of eight
///   0xFF bytes (`HnswIndex.METADATA_KEY`).
pub fn keyShape(tree: IdTree, key: []const u8) ?KeyShape {
    const P = scoped_tree.PREFIX_SIZE;
    return switch (tree) {
        .node, .edge_id_index => if (key.len == 8) .{ .id = 0 } else null,
        .hnsw_node => if (key.len != 8)
            null
        else if (std.mem.eql(u8, key, &HnswIndex.METADATA_KEY))
            .fixed
        else
            .{ .id = 0 },
        .fts_reverse => switch (key.len) {
            8 => .{ .id = 0 },
            P + 8 => .{ .id = P },
            else => null,
        },
        .fts_lengths => switch (key.len) {
            8 => .{ .id = 0 },
            P + 8 => .{ .id = P },
            DocLengthStore.STATS_KEY.len => if (isLengthsConstant(key)) .fixed else null,
            P + DocLengthStore.STATS_KEY.len => if (isLengthsConstant(key[P..])) .fixed else null,
            else => null,
        },
    };
}

fn isLengthsConstant(key: []const u8) bool {
    return std.mem.eql(u8, key, &DocLengthStore.STATS_KEY) or std.mem.eql(u8, key, &DocLengthStore.ANALYSIS_KEY);
}

/// `key` with its id written in `to` order instead of `from`, in `out`.
pub fn rekey(shape: KeyShape, key: []const u8, from: IdKeyOrder, to: IdKeyOrder, out: *[MAX_KEY_LEN]u8) []const u8 {
    @memcpy(out[0..key.len], key);
    switch (shape) {
        .fixed => {},
        .id => |p| out[p..][0..8].* = to.encode(from.decode(key[p..][0..8])),
    }
    return out[0..key.len];
}

// ============================================================================
// Page accounting
// ============================================================================

const OWNER_NONE: u8 = 0xFF;
const OWNER_FREE: u8 = 0xFE;
const OWNER_HEADER: u8 = 0xFD;

/// Who owns each page of a file: a tree (by `TreeIndex`), the freelist, the
/// header, or nothing a B-tree walk can see.
pub const PageAccount = struct {
    allocator: Allocator,
    owners: []u8,
    tree_pages: [MAX_TREES]u64 = [_]u64{0} ** MAX_TREES,
    free_pages: u64 = 0,
    /// Pages no B-tree and not the freelist claims: vector data, HNSW
    /// neighbour lists, full-text postings, and pages a crash leaked.
    other_pages: u64 = 0,

    pub fn deinit(self: *PageAccount) void {
        self.allocator.free(self.owners);
    }
};

fn treeName(owner: u8) []const u8 {
    if (owner == OWNER_FREE) return "the freelist";
    if (owner == OWNER_HEADER) return "the header";
    if (owner >= MAX_TREES) return "nothing";
    return @tagName(@as(TreeIndex, @enumFromInt(owner)));
}

const Clash = struct { page: PageId, other: u8 };

/// Marks each page a tree walk visits as the tree's, and stops at one that
/// already has an owner.
const MarkVisitor = struct {
    owners: []u8,
    owner: u8,
    count: u64 = 0,
    clash: ?Clash = null,

    pub fn visit(self: *MarkVisitor, page_id: PageId, kind: BTree.PageKind) BTreeError!void {
        _ = kind;
        if (page_id >= self.owners.len) return BTreeError.InvalidPage;
        if (self.owners[page_id] != OWNER_NONE) {
            self.clash = .{ .page = page_id, .other = self.owners[page_id] };
            return BTreeError.InvalidPage;
        }
        self.owners[page_id] = self.owner;
        self.count += 1;
    }
};

/// Walk every B-tree the header names, the freelist, and the pages freed
/// since the last flush, and fail if any page is claimed twice or a tree
/// cannot be walked.
pub fn accountPages(
    allocator: Allocator,
    bp: *BufferPool,
    header: *const FileHeader,
    diag: ?*Diagnostic,
) UpgradeError!PageAccount {
    const count = bp.pm.pageCount();
    const owners = allocator.alloc(u8, count) catch return error.OutOfMemory;
    errdefer allocator.free(owners);
    @memset(owners, OWNER_NONE);
    if (count > 0) owners[0] = OWNER_HEADER;
    var account = PageAccount{ .allocator = allocator, .owners = owners };

    for (header.tree_roots, 0..) |root, i| {
        if (root == NULL_PAGE) continue;
        var tree = BTree.open(allocator, bp, root);
        var visitor = MarkVisitor{ .owners = owners, .owner = @intCast(i) };
        tree.forEachPage(&visitor) catch |err| {
            const this = treeName(@intCast(i));
            if (visitor.clash) |c| {
                Diagnostic.set(diag, "page {d} belongs to both {s} and {s}", .{ c.page, treeName(c.other), this });
                return error.PagesOwnedTwice;
            }
            if (err == BTreeError.InvalidPage) {
                Diagnostic.set(diag, "the {s} tree reaches a page that is not one of its own", .{this});
                return error.PagesOwnedTwice;
            }
            Diagnostic.set(diag, "the {s} tree could not be walked: {s}", .{ this, @errorName(err) });
            return mapBTreeError(err);
        };
        account.tree_pages[i] = visitor.count;
    }

    // The freelist as the file has it. It ends where the engine would stop
    // following it: at a page that is not free or does not read back.
    var page_id = header.freelist_page;
    while (page_id != NULL_PAGE and page_id < count) {
        const owner = owners[page_id];
        if (owner == OWNER_FREE) break; // A loop; the engine stops here too.
        if (owner != OWNER_NONE) {
            Diagnostic.set(diag, "page {d} is on the freelist and belongs to {s}", .{ page_id, treeName(owner) });
            return error.PagesOwnedTwice;
        }
        const frame = bp.fetchPage(page_id, .shared) catch break;
        const page_header: *const PageHeader = @ptrCast(@alignCast(frame.data.ptr));
        const is_free = page_header.page_type == .free;
        const next = std.mem.readInt(u32, frame.data[8..12], .little);
        bp.unpinPage(frame, false);
        if (!is_free) break;
        owners[page_id] = OWNER_FREE;
        account.free_pages += 1;
        page_id = next;
    }

    for (bp.pm.deferredFrees()) |freed| {
        if (freed >= count) continue;
        const owner = owners[freed];
        if (owner == OWNER_FREE) continue;
        if (owner != OWNER_NONE) {
            Diagnostic.set(diag, "page {d} was freed and still belongs to {s}", .{ freed, treeName(owner) });
            return error.PagesOwnedTwice;
        }
        owners[freed] = OWNER_FREE;
        account.free_pages += 1;
    }

    var owned: u64 = 0;
    for (account.tree_pages) |n| owned += n;
    account.other_pages = (count -| 1) -| owned -| account.free_pages;
    return account;
}

/// Lists the pages of a tree about to be freed, each checked to be the
/// tree's in the accounting taken before the conversion.
const FreeListVisitor = struct {
    allocator: Allocator,
    pages: *std.ArrayListUnmanaged(PageId),
    owners: []const u8,
    owner: u8,
    clash: ?Clash = null,

    pub fn visit(self: *FreeListVisitor, page_id: PageId, kind: BTree.PageKind) BTreeError!void {
        _ = kind;
        const recorded = if (page_id < self.owners.len) self.owners[page_id] else OWNER_NONE;
        if (recorded != self.owner) {
            self.clash = .{ .page = page_id, .other = recorded };
            return BTreeError.InvalidPage;
        }
        self.pages.append(self.allocator, page_id) catch return BTreeError.OutOfMemory;
    }
};

// ============================================================================
// Conversion
// ============================================================================

/// A key in its new form.
const NewKey = struct {
    len: u8,
    bytes: [MAX_KEY_LEN]u8,

    fn slice(self: *const NewKey) []const u8 {
        return self.bytes[0..self.len];
    }

    fn lessThan(_: void, a: NewKey, b: NewKey) bool {
        return std.mem.order(u8, a.slice(), b.slice()) == .lt;
    }
};

const Converter = struct {
    allocator: Allocator,
    bp: *BufferPool,
    /// Owners before the conversion; freed pages are marked free.
    owners: []u8,
    /// Flush once this many pages are dirty, so the pool does not grow.
    flush_at: usize,
    fault: ?Fault,
    diag: ?*Diagnostic,

    fn flushIfFull(self: *Converter) UpgradeError!void {
        if (self.bp.dirtyCount() < self.flush_at) return;
        _ = self.bp.flushAll() catch return error.IoError;
    }

    /// Build `tree` again with big-endian keys and free the old one.
    /// Returns the new root.
    fn convertTree(self: *Converter, tree: IdTree, old_root: PageId, stats: *TreeStats) UpgradeError!PageId {
        var old = BTree.open(self.allocator, self.bp, old_root);
        const before = old.spaceStats() catch |err| return self.treeFailed(tree, err);
        stats.present = true;
        stats.pages_before = before.pages();
        stats.leaf_fill_before = before.leafFill();

        // Every key, in its new form. The iterator is closed before the
        // pool is asked for anything else.
        var keys: std.ArrayListUnmanaged(NewKey) = .empty;
        defer keys.deinit(self.allocator);
        {
            var iter = old.range(null, null) catch |err| return self.treeFailed(tree, err);
            defer iter.deinit();
            while (iter.next() catch |err| return self.treeFailed(tree, err)) |entry| {
                const shape = keyShape(tree, entry.key) orelse {
                    Diagnostic.set(self.diag, "the {s} holds a {d}-byte key the upgrade does not know how to convert", .{ tree.name(), entry.key.len });
                    return error.UnexpectedKey;
                };
                var key = NewKey{ .len = @intCast(entry.key.len), .bytes = undefined };
                _ = rekey(shape, entry.key, .little, .big, &key.bytes);
                keys.append(self.allocator, key) catch return error.OutOfMemory;
            }
        }
        std.mem.sort(NewKey, keys.items, {}, NewKey.lessThan);
        if (keys.items.len > 1) for (keys.items[1..], keys.items[0 .. keys.items.len - 1]) |key, previous| {
            if (std.mem.eql(u8, key.slice(), previous.slice())) {
                Diagnostic.set(self.diag, "two keys of the {s} convert to the same key", .{tree.name()});
                return error.UnexpectedKey;
            }
        };
        stats.entries = keys.items.len;

        // The new tree, filled in key order. Each value comes from the old
        // tree by the old key.
        var new = BTree.init(self.allocator, self.bp) catch |err| return self.treeFailed(tree, err);
        for (keys.items, 0..) |*key, i| {
            var source = key;
            if (self.fault == .swap_node_values and tree == .node and keys.items.len >= 2 and i < 2) {
                source = &keys.items[1 - i];
            }
            var old_key_buf: [MAX_KEY_LEN]u8 = undefined;
            const old_key = rekey(keyShape(tree, source.slice()).?, source.slice(), .big, .little, &old_key_buf);
            const value = (old.get(old_key) catch |err| return self.treeFailed(tree, err)) orelse {
                Diagnostic.set(self.diag, "a key of the {s} disappeared while it was converted", .{tree.name()});
                return error.VerificationFailed;
            };
            defer old.freeValue(value);
            new.insert(key.slice(), value) catch |err| return self.treeFailed(tree, err);
            try self.flushIfFull();
        }
        const after = new.spaceStats() catch |err| return self.treeFailed(tree, err);
        stats.pages_after = after.pages();
        stats.leaf_fill_after = after.leafFill();

        // Free the old tree. Every page is listed first, by a walk that
        // checks each against the accounting, and only then freed.
        var pages: std.ArrayListUnmanaged(PageId) = .empty;
        defer pages.deinit(self.allocator);
        var lister = FreeListVisitor{
            .allocator = self.allocator,
            .pages = &pages,
            .owners = self.owners,
            .owner = @intFromEnum(tree.treeIndex()),
        };
        old.forEachPage(&lister) catch |err| {
            if (lister.clash) |c| {
                Diagnostic.set(self.diag, "page {d} of the old {s} is recorded as {s}'s; nothing was freed", .{ c.page, tree.name(), treeName(c.other) });
                return error.PagesOwnedTwice;
            }
            return self.treeFailed(tree, err);
        };
        for (pages.items) |page_id| {
            self.bp.freePage(page_id) catch return error.IoError;
            self.owners[page_id] = OWNER_FREE;
        }
        return new.getRootPage();
    }

    fn treeFailed(self: *Converter, tree: IdTree, err: BTreeError) UpgradeError {
        Diagnostic.set(self.diag, "converting the {s} failed: {s}", .{ tree.name(), @errorName(err) });
        return mapBTreeError(err);
    }
};

fn mapBTreeError(err: BTreeError) UpgradeError {
    return switch (err) {
        BTreeError.OutOfMemory => error.OutOfMemory,
        BTreeError.BufferPoolFull => error.BufferPoolFull,
        BTreeError.ValueTooLarge => error.ValueTooLarge,
        BTreeError.InvalidPage => error.InvalidDatabase,
        else => error.IoError,
    };
}

/// Convert the file at `path`, a disposable copy that nothing else has
/// open, in place: new trees with big-endian keys, the old ones freed, the
/// header marked. Written without a log or journal; the caller deletes the
/// file if this fails.
fn convertFile(
    allocator: Allocator,
    path: []const u8,
    pool_bytes: usize,
    fault: ?Fault,
    stats: *UpgradeStats,
    diag: ?*Diagnostic,
) UpgradeError!void {
    var posix = PosixVfs.init(allocator);
    var pm = PageManager.init(allocator, posix.vfs(), path, .{}) catch |err| {
        Diagnostic.set(diag, "the copy could not be opened for conversion: {s}", .{@errorName(err)});
        return error.IoError;
    };
    defer pm.deinit();
    var bp = BufferPool.init(allocator, &pm, pool_bytes) catch return error.OutOfMemory;
    defer bp.deinit();

    if (pm.getHeader().idKeyOrder() != .little) {
        Diagnostic.set(diag, "the copy already has big-endian id keys", .{});
        return error.VerificationFailed;
    }

    var before = try accountPages(allocator, &bp, pm.getHeader(), diag);
    defer before.deinit();

    var converter = Converter{
        .allocator = allocator,
        .bp = &bp,
        .owners = before.owners,
        .flush_at = @max(bp.frame_count / 2, 1),
        .fault = fault,
        .diag = diag,
    };

    // Smallest first, so that the larger trees can reuse what it freed.
    const Pending = struct { tree: IdTree, pages: u64 };
    var pending: [IdTree.all.len]Pending = undefined;
    for (IdTree.all, 0..) |tree, i| {
        pending[i] = .{ .tree = tree, .pages = before.tree_pages[@intFromEnum(tree.treeIndex())] };
    }
    std.mem.sort(Pending, &pending, {}, struct {
        fn lessThan(_: void, a: Pending, b: Pending) bool {
            return a.pages < b.pages;
        }
    }.lessThan);

    var converted: usize = 0;
    for (pending) |item| {
        const index = item.tree.treeIndex();
        const root = pm.getHeader().getTreeRoot(index);
        if (root == NULL_PAGE) continue;
        const tree_stats = &stats.trees[@intFromEnum(item.tree)];
        const new_root = try converter.convertTree(item.tree, root, tree_stats);
        stats.pages_freed += tree_stats.pages_before;

        var header = pm.getHeader().*;
        header.setTreeRoot(index, new_root);
        pm.updateHeader(&header) catch return error.IoError;
        converted += 1;
        if (fault == .mid_conversion and converted == 1) {
            Diagnostic.set(diag, "stopped after the first tree, for a test", .{});
            return error.IoError;
        }
    }

    // Only now does the file say its keys are big-endian. A new identity
    // keeps the original's log and journal, if one were ever found beside
    // this file, from being applied to it: both check the file's UUID.
    var header = pm.getHeader().*;
    header.flags |= FileFlags.ORDERED_ID_KEYS;
    header.raiseMinReader(FileHeader.ReaderReason.ordered_id_keys);
    compat.randomBytes(&header.file_uuid);
    pm.updateHeader(&header) catch return error.IoError;
    _ = bp.flushAll() catch return error.IoError;
    pm.sync() catch return error.IoError;

    var after = try accountPages(allocator, &bp, pm.getHeader(), diag);
    after.deinit();
}

// ============================================================================
// Verification
// ============================================================================

fn mismatch(diag: ?*Diagnostic, comptime fmt: []const u8, args: anytype) UpgradeError {
    Diagnostic.set(diag, fmt, args);
    return error.VerificationFailed;
}

fn valueEql(a: PropertyValue, b: PropertyValue) bool {
    if (std.meta.activeTag(a) != std.meta.activeTag(b)) return false;
    return switch (a) {
        .null_val => true,
        .bool_val => |v| v == b.bool_val,
        .int_val => |v| v == b.int_val,
        .float_val => |v| @as(u64, @bitCast(v)) == @as(u64, @bitCast(b.float_val)),
        .string_val => |v| std.mem.eql(u8, v, b.string_val),
        .bytes_val => |v| std.mem.eql(u8, v, b.bytes_val),
        .vector_val => |v| std.mem.eql(u8, std.mem.sliceAsBytes(v), std.mem.sliceAsBytes(b.vector_val)),
        .list_val => |v| blk: {
            if (v.len != b.list_val.len) break :blk false;
            for (v, b.list_val) |x, y| if (!valueEql(x, y)) break :blk false;
            break :blk true;
        },
        .map_val => |v| blk: {
            if (v.len != b.map_val.len) break :blk false;
            for (v, b.map_val) |x, y| {
                if (!std.mem.eql(u8, x.key, y.key) or !valueEql(x.value, y.value)) break :blk false;
            }
            break :blk true;
        },
    };
}

fn propertiesEql(a: []const Property, b: []const Property) bool {
    if (a.len != b.len) return false;
    for (a, b) |x, y| {
        if (x.key_id != y.key_id or !valueEql(x.value, y.value)) return false;
    }
    return true;
}

/// Compare one id-keyed tree of the source with the copy's. Every source
/// entry is looked up in the copy as the copy's stores look it up, by the id
/// in the copy's key order and, for a scoped full-text key, through a scoped
/// view; its value must be the same bytes. Then the copy is walked: as many
/// entries, every key of a known shape, and ids ascending within each scope.
/// The source ids of the node tree and edge-id index are collected in
/// `ids` for the store checks.
fn compareTree(
    src: *Database,
    dst: *Database,
    tree: IdTree,
    ids: ?*std.ArrayListUnmanaged(u64),
    stats: *UpgradeStats,
    diag: ?*Diagnostic,
) UpgradeError!void {
    const index = tree.treeIndex();
    const src_root = src.page_manager.getHeader().getTreeRoot(index);
    const dst_root = dst.page_manager.getHeader().getTreeRoot(index);
    if ((src_root == NULL_PAGE) != (dst_root == NULL_PAGE)) {
        return mismatch(diag, "the {s} exists in only one of the source and the copy", .{tree.name()});
    }
    if (src_root == NULL_PAGE) return;

    const allocator = src.allocator;
    var src_tree = BTree.open(allocator, &src.buffer_pool, src_root);
    var dst_tree = BTree.open(dst.allocator, &dst.buffer_pool, dst_root);
    const src_order = src.id_key_order;
    const dst_order = dst.id_key_order;

    var src_entries: u64 = 0;
    {
        var iter = src_tree.range(null, null) catch return error.IoError;
        defer iter.deinit();
        while (iter.next() catch return error.IoError) |entry| {
            const shape = keyShape(tree, entry.key) orelse {
                return mismatch(diag, "the source's {s} holds a key of unknown shape", .{tree.name()});
            };
            const found = switch (shape) {
                .fixed => dst_tree.get(entry.key),
                .id => |p| blk: {
                    const id = src_order.decode(entry.key[p..][0..8]);
                    if (ids) |list| {
                        if (!(tree == .edge_id_index and id == EdgeStore.EDGE_ID_META_KEY)) {
                            list.append(allocator, id) catch return error.OutOfMemory;
                        }
                    }
                    const key = dst_order.encode(id);
                    if (p == 0) break :blk dst_tree.get(&key);
                    const view = ScopedTree.scoped(&dst_tree, entry.key[0..scoped_tree.PREFIX_SIZE].*);
                    break :blk view.get(&key);
                },
            } catch return error.IoError;
            const value = found orelse {
                return mismatch(diag, "an entry of the {s} is missing from the copy", .{tree.name()});
            };
            defer dst_tree.freeValue(value);
            if (!std.mem.eql(u8, value, entry.value)) {
                return mismatch(diag, "an entry of the {s} has a different value in the copy", .{tree.name()});
            }
            src_entries += 1;
        }
    }

    var dst_entries: u64 = 0;
    {
        // The last id seen in each scope, keyed by scope prefix (or none).
        var last = std.AutoHashMapUnmanaged(u64, u64).empty;
        defer last.deinit(allocator);
        var iter = dst_tree.range(null, null) catch return error.IoError;
        defer iter.deinit();
        while (iter.next() catch return error.IoError) |entry| {
            const shape = keyShape(tree, entry.key) orelse {
                return mismatch(diag, "the copy's {s} holds a key of unknown shape", .{tree.name()});
            };
            switch (shape) {
                .fixed => {},
                .id => |p| {
                    const id = dst_order.decode(entry.key[p..][0..8]);
                    const scope: u64 = if (p == 0)
                        std.math.maxInt(u64)
                    else
                        std.mem.readInt(u32, entry.key[0..4], .big);
                    const slot = last.getOrPut(allocator, scope) catch return error.OutOfMemory;
                    if (slot.found_existing and slot.value_ptr.* >= id) {
                        return mismatch(diag, "the copy's {s} does not hold its ids in ascending order", .{tree.name()});
                    }
                    slot.value_ptr.* = id;
                },
            }
            dst_entries += 1;
        }
    }
    if (src_entries != dst_entries) {
        return mismatch(diag, "the {s} has {d} entries in the source and {d} in the copy", .{ tree.name(), src_entries, dst_entries });
    }
    stats.entries_verified += src_entries;
}

fn compareNodes(src: *Database, dst: *Database, ids: []const u64, diag: ?*Diagnostic) UpgradeError!void {
    for (ids) |id| {
        var a = src.node_store.get(id) catch return mismatch(diag, "node {d} cannot be read from the source", .{id});
        defer a.deinit(src.allocator);
        var b = dst.node_store.get(id) catch return mismatch(diag, "node {d} cannot be read from the copy", .{id});
        defer b.deinit(dst.allocator);
        if (!std.mem.eql(lattice.graph.symbols.SymbolId, a.labels, b.labels) or !propertiesEql(a.properties, b.properties)) {
            return mismatch(diag, "node {d} differs in the copy", .{id});
        }
    }
    if (src.node_store.next_id != dst.node_store.next_id) {
        return mismatch(diag, "the next node id is {d} in the source and {d} in the copy", .{ src.node_store.next_id, dst.node_store.next_id });
    }
}

fn compareEdges(src: *Database, dst: *Database, ids: []const u64, diag: ?*Diagnostic) UpgradeError!void {
    for (ids) |id| {
        var a = src.edge_store.getById(id) catch return mismatch(diag, "edge {d} cannot be read from the source", .{id});
        defer a.deinit(src.allocator);
        var b = dst.edge_store.getById(id) catch return mismatch(diag, "edge {d} cannot be read from the copy", .{id});
        defer b.deinit(dst.allocator);
        if (a.source != b.source or a.target != b.target or a.edge_type != b.edge_type or
            !propertiesEql(a.properties, b.properties))
        {
            return mismatch(diag, "edge {d} differs in the copy", .{id});
        }
    }
    if (src.edge_store.next_edge_id != dst.edge_store.next_edge_id) {
        return mismatch(diag, "the next edge id is {d} in the source and {d} in the copy", .{ src.edge_store.next_edge_id, dst.edge_store.next_edge_id });
    }
}

fn compareVectorIndex(src: *Database, dst: *Database, diag: ?*Diagnostic) UpgradeError!void {
    const a = if (src.hnsw_index) |*h| h else null;
    const b = if (dst.hnsw_index) |*h| h else null;
    if ((a == null) != (b == null)) {
        return mismatch(diag, "the vector index opens in only one of the source and the copy", .{});
    }
    const x = a orelse return;
    const y = b.?;
    if (x.nodes.count() != y.nodes.count() or x.vector_count != y.vector_count or
        x.entry_point != y.entry_point or x.max_layer != y.max_layer)
    {
        return mismatch(diag, "the vector index differs in the copy", .{});
    }
    var iter = x.nodes.iterator();
    while (iter.next()) |entry| {
        const other = y.nodes.get(entry.key_ptr.*) orelse {
            return mismatch(diag, "vector {d} is missing from the copy's index", .{entry.key_ptr.*});
        };
        if (!std.meta.eql(entry.value_ptr.*, other)) {
            return mismatch(diag, "vector {d} differs in the copy's index", .{entry.key_ptr.*});
        }
    }
}

/// Check the copy against the source it was made from.
fn verifyCopy(src: *Database, dst: *Database, stats: *UpgradeStats, diag: ?*Diagnostic) UpgradeError!void {
    const header = dst.page_manager.getHeader();
    if (dst.id_key_order != .big or header.idKeyOrder() != .big or
        !header.requiresReaderFor(FileHeader.ReaderReason.ordered_id_keys))
    {
        return mismatch(diag, "the copy does not open with big-endian id keys", .{});
    }

    // What `lattice check` checks: every page reads back with its checksum.
    {
        const page_size = dst.page_manager.getPageSize();
        const alignment = comptime std.mem.Alignment.fromByteUnits(4096);
        const buf = dst.allocator.alignedAlloc(u8, alignment, page_size) catch return error.OutOfMemory;
        defer dst.allocator.free(buf);
        var page_id: PageId = 1;
        while (page_id < dst.page_manager.pageCount()) : (page_id += 1) {
            _ = dst.page_manager.readPageForScan(page_id, buf) catch {
                return mismatch(diag, "page {d} of the copy does not read back", .{page_id});
            };
        }
    }

    // No page belongs to two structures.
    var account = try accountPages(dst.allocator, &dst.buffer_pool, header, diag);
    defer account.deinit();
    stats.free_pages_after = account.free_pages;
    stats.other_pages_after = account.other_pages;

    // The other trees were not touched.
    const src_header = src.page_manager.getHeader();
    for (0..MAX_TREES) |i| {
        const index: TreeIndex = @enumFromInt(i);
        const converted = for (IdTree.all) |t| {
            if (t.treeIndex() == index) break true;
        } else false;
        if (converted) continue;
        if (src_header.tree_roots[i] != header.tree_roots[i]) {
            return mismatch(diag, "the {s} tree moved in the copy", .{@tagName(index)});
        }
    }

    var node_ids: std.ArrayListUnmanaged(u64) = .empty;
    defer node_ids.deinit(src.allocator);
    var edge_ids: std.ArrayListUnmanaged(u64) = .empty;
    defer edge_ids.deinit(src.allocator);
    for (IdTree.all) |tree| {
        const ids: ?*std.ArrayListUnmanaged(u64) = switch (tree) {
            .node => &node_ids,
            .edge_id_index => &edge_ids,
            else => null,
        };
        try compareTree(src, dst, tree, ids, stats, diag);
    }
    try compareNodes(src, dst, node_ids.items, diag);
    try compareEdges(src, dst, edge_ids.items, diag);
    try compareVectorIndex(src, dst, diag);
}

// ============================================================================
// The operation
// ============================================================================

fn fileSize(path: []const u8) u64 {
    const file = compat.fs.cwd().openFile(path, .{}) catch return 0;
    defer file.close();
    const st = file.stat() catch return 0;
    return st.size;
}

fn exists(path: []const u8) bool {
    compat.fs.cwd().access(path, .{}) catch return false;
    return true;
}

/// The files an upgrade to `dest_path` may create.
const COPY_SUFFIXES = [_][]const u8{ "", ".partial", "-wal", "-journal" };

/// Remove the copy at `dest_path` and every file beside it that belongs to it.
pub fn removeCopy(allocator: Allocator, dest_path: []const u8) void {
    for (COPY_SUFFIXES) |suffix| {
        const path = std.fmt.allocPrint(allocator, "{s}{s}", .{ dest_path, suffix }) catch continue;
        defer allocator.free(path);
        compat.fs.cwd().deleteFile(path) catch {};
    }
}

/// Write a format-5 copy of `db` to `dest_path` and check it against `db`.
/// See `Database.upgradeIdKeysTo`.
pub fn upgrade(db: *Database, dest_path: []const u8, diag: ?*Diagnostic) UpgradeError!UpgradeStats {
    const allocator = db.allocator;
    if (database.isMemoryPath(db.path)) return error.InMemoryDatabase;
    if (db.read_only) return error.ReadOnly;
    if (db.failed) return error.HandleFailed;
    if (db.txn_overlays.count() != 0) return error.TransactionConflict;
    if (db.id_key_order == .big) return .{ .already_ordered = true };

    // Nothing may already be there: not a file this would have to delete,
    // and not a log the check below would open with the copy.
    for (COPY_SUFFIXES) |suffix| {
        const path = std.fmt.allocPrint(allocator, "{s}{s}", .{ dest_path, suffix }) catch return error.OutOfMemory;
        defer allocator.free(path);
        if (exists(path)) {
            Diagnostic.set(diag, "{s} already exists", .{path});
            return error.AlreadyExists;
        }
    }

    var stats = UpgradeStats{};
    _ = db.backup(dest_path) catch |err| {
        Diagnostic.set(diag, "the copy could not be written: {s}", .{@errorName(err)});
        removeCopy(allocator, dest_path);
        return err;
    };
    errdefer removeCopy(allocator, dest_path);
    stats.bytes_before = fileSize(dest_path);
    if (db.upgrade_fault_for_testing == .after_copy) return error.IoError;

    const pool_bytes = @max(db.buffer_pool.frame_count * db.page_manager.getPageSize(), 1024 * 1024);
    try convertFile(allocator, dest_path, pool_bytes, db.upgrade_fault_for_testing, &stats, diag);
    stats.bytes_after = fileSize(dest_path);

    // Open the copy the ordinary way and compare it with the source.
    var config = db.config;
    config.changefeed_retention = null;
    const copy = Database.open(allocator, dest_path, .{ .read_only = true, .config = config }) catch |err| {
        Diagnostic.set(diag, "the converted copy does not open: {s}", .{@errorName(err)});
        return error.VerificationFailed;
    };
    defer copy.close();
    try verifyCopy(db, copy, &stats, diag);
    return stats;
}

// ============================================================================
// Putting the copy in place
// ============================================================================

/// Where `replaceWithUpgraded` stops early, for tests of a crash at that point.
pub const SwapStep = enum {
    /// The original is held and its log and journal are empty.
    checked,
    /// The original's empty log and journal are gone.
    sidecars_removed,
    /// The upgraded copy is at the original's path.
    renamed,
};

pub const SwapError = error{
    /// Another process has the database open.
    DatabaseLocked,
    /// The original's log or journal still holds work, so the original is
    /// not complete on its own. Nothing was changed.
    LogNotEmpty,
    /// The original changed after the copy was made. Nothing was changed.
    DatabaseChanged,
    IoError,
    OutOfMemory,
    /// A test asked to stop at a `SwapStep`.
    StoppedForTesting,
};

/// The bytes of a closed database file that tell whether it changed: its
/// header page and its size.
pub const FileState = struct {
    header: [4096]u8,
    size: u64,

    pub fn capture(path: []const u8) SwapError!FileState {
        const file = compat.fs.cwd().openFile(path, .{}) catch return error.IoError;
        defer file.close();
        const st = file.stat() catch return error.IoError;
        var state = FileState{ .header = undefined, .size = st.size };
        const n = file.preadAll(&state.header, 0) catch return error.IoError;
        if (n != state.header.len) return error.IoError;
        return state;
    }
};

fn sidecarSize(allocator: Allocator, path: []const u8, suffix: []const u8) SwapError!?u64 {
    const sidecar = std.fmt.allocPrint(allocator, "{s}{s}", .{ path, suffix }) catch return error.OutOfMemory;
    defer allocator.free(sidecar);
    const file = compat.fs.cwd().openFile(sidecar, .{}) catch |err| switch (err) {
        error.FileNotFound => return null,
        else => return error.IoError,
    };
    defer file.close();
    const st = file.stat() catch return error.IoError;
    return st.size;
}

fn deleteSidecar(allocator: Allocator, path: []const u8, suffix: []const u8) SwapError!void {
    const sidecar = std.fmt.allocPrint(allocator, "{s}{s}", .{ path, suffix }) catch return error.OutOfMemory;
    defer allocator.free(sidecar);
    compat.fs.cwd().deleteFile(sidecar) catch |err| switch (err) {
        error.FileNotFound => {},
        else => return error.IoError,
    };
}

/// Put the verified copy at `upgraded_path` in place of the closed database
/// at `path`, as one rename.
///
/// In order, with what a crash leaves after each step:
///
/// 1. Take the original's lock and check that it is as `before` captured it
///    after its close, and that its log and journal hold nothing. A crash:
///    the original, untouched, and the copy beside it.
/// 2. Delete the original's empty log and journal, and sync the directory.
///    A crash: the original without them, which is complete (they were
///    empty) and opens, starting a new log.
/// 3. Rename the copy over the original, and sync the directory. The rename
///    is atomic: a crash leaves the original or the copy at `path`, each
///    whole, and no log or journal of the original beside it.
///
/// The log and journal go before the rename because the copy has a new UUID
/// and a log with another UUID makes an open fail; the journal would be
/// ignored, but there is no reason to leave it.
///
/// On POSIX the lock is held through the rename, so no process can open the
/// original from step 1 on. Windows cannot rename over a file that is open,
/// so there the lock is let go just before the rename, and a process that
/// opens the original in that moment makes the rename fail, which leaves
/// the original as it was.
pub fn replaceWithUpgraded(
    allocator: Allocator,
    path: []const u8,
    upgraded_path: []const u8,
    before: *const FileState,
    stop_after: ?SwapStep,
) SwapError!void {
    var posix = PosixVfs.init(allocator);
    const vfs = posix.vfs();

    const original = vfs.open(path, .{ .read = true, .write = true }) catch return error.IoError;
    var held = true;
    defer if (held) {
        original.unlock();
        original.close();
    };
    const locked = original.tryLock(.exclusive) catch return error.IoError;
    if (!locked) {
        original.close();
        held = false;
        return error.DatabaseLocked;
    }

    // 1. Nothing changed since the copy, and nothing is left in the log.
    // Read through the locked handle: on Windows the lock keeps any other
    // handle from reading the file.
    var now_header: [4096]u8 = undefined;
    const now_read = original.read(0, &now_header) catch return error.IoError;
    const now_size = original.size() catch return error.IoError;
    if (now_read != now_header.len or now_size != before.size or !std.mem.eql(u8, &now_header, &before.header)) {
        return error.DatabaseChanged;
    }
    if (try sidecarSize(allocator, path, "-wal")) |size| {
        if (size > WAL_HEADER_SIZE) return error.LogNotEmpty;
    }
    if (try sidecarSize(allocator, path, "-journal")) |size| {
        if (size > 0) return error.LogNotEmpty;
    }
    if (stop_after == .checked) return error.StoppedForTesting;

    // 2. The original's empty log and journal.
    try deleteSidecar(allocator, path, "-journal");
    try deleteSidecar(allocator, path, "-wal");
    PosixVfs.syncParentDir(path);
    if (stop_after == .sidecars_removed) return error.StoppedForTesting;

    // 3. The copy takes the original's place.
    if (builtin.os.tag == .windows) {
        original.unlock();
        original.close();
        held = false;
    }
    compat.fs.cwd().rename(upgraded_path, path) catch return error.IoError;
    PosixVfs.syncParentDir(path);
    if (stop_after == .renamed) return error.StoppedForTesting;
}

// ============================================================================
// Tests
// ============================================================================

test "key shapes of the five id-keyed trees" {
    const P = scoped_tree.PREFIX_SIZE;
    try std.testing.expectEqual(KeyShape{ .id = 0 }, keyShape(.node, &[_]u8{0} ** 8).?);
    try std.testing.expect(keyShape(.node, &[_]u8{0} ** 9) == null);
    try std.testing.expectEqual(KeyShape{ .id = 0 }, keyShape(.edge_id_index, &[_]u8{0} ** 8).?);
    try std.testing.expectEqual(KeyShape.fixed, keyShape(.hnsw_node, &HnswIndex.METADATA_KEY).?);
    try std.testing.expectEqual(KeyShape{ .id = 0 }, keyShape(.hnsw_node, &[_]u8{ 1, 0, 0, 0, 0, 0, 0, 0 }).?);
    try std.testing.expectEqual(KeyShape{ .id = P }, keyShape(.fts_reverse, &[_]u8{0} ** (P + 8)).?);
    try std.testing.expect(keyShape(.fts_reverse, &DocLengthStore.STATS_KEY) == null);
    try std.testing.expectEqual(KeyShape.fixed, keyShape(.fts_lengths, &DocLengthStore.STATS_KEY).?);
    try std.testing.expectEqual(KeyShape.fixed, keyShape(.fts_lengths, &DocLengthStore.ANALYSIS_KEY).?);
    const scoped_stats = [_]u8{ 9, 9, 9, 9 } ++ DocLengthStore.STATS_KEY;
    try std.testing.expectEqual(KeyShape.fixed, keyShape(.fts_lengths, &scoped_stats).?);
    try std.testing.expect(keyShape(.fts_lengths, &[_]u8{ 0, 's', 't', 'a', 't', 'x' }) == null);
    try std.testing.expectEqual(KeyShape{ .id = P }, keyShape(.fts_lengths, &[_]u8{0} ** (P + 8)).?);
}

test "rekey turns the id around and keeps the prefix" {
    var out: [MAX_KEY_LEN]u8 = undefined;
    const old = [_]u8{ 'a', 'b', 'c', 'd' } ++ IdKeyOrder.little.encode(0x0102);
    const new = rekey(.{ .id = 4 }, &old, .little, .big, &out);
    try std.testing.expectEqualSlices(u8, &([_]u8{ 'a', 'b', 'c', 'd' } ++ IdKeyOrder.big.encode(0x0102)), new);
    var back: [MAX_KEY_LEN]u8 = undefined;
    try std.testing.expectEqualSlices(u8, &old, rekey(.{ .id = 4 }, new, .big, .little, &back));
    try std.testing.expectEqualSlices(u8, &HnswIndex.METADATA_KEY, rekey(.fixed, &HnswIndex.METADATA_KEY, .little, .big, &out));
}
