//! Full-text entries that no declared index can reach, and taking them back.
//!
//! Before 0.15.0 there was one full-text index over everything, and its keys
//! carried no scope prefix. 0.15.0 left those entries where they were: the old
//! index might be the only place some text was ever searchable, so an upgrade
//! must not delete it, and nothing written since can find it. `compact` is
//! where a user who has moved to declared indexes gives that space back.
//!
//! What is taken, tree by tree, and why each is certain not to belong to a
//! declared index:
//!
//! - Document lengths: keys of six or eight bytes. A declared index's keys are
//!   its four-byte prefix in front of a six-byte statistics key or an
//!   eight-byte document key, so ten or twelve bytes, never six or eight.
//! - Reverse index: keys of eight bytes, against twelve for a declared index.
//! - Dictionary: a term key whose first four bytes are not the prefix of any
//!   declared index, or that is too short to carry one. Its posting list is
//!   freed with it. An unscoped term whose first four bytes happen to equal a
//!   declared prefix is indistinguishable from that index's own term and is
//!   left alone. A key under the prefix of an index that is no longer declared
//!   is taken too: dropping an index clears its scope, so only a drop or a
//!   failed declaration cut short by a crash leaves one, and declaring the
//!   index again clears that scope before it fills it.

const std = @import("std");
const lattice = @import("lattice");

const Allocator = std.mem.Allocator;
const BTree = lattice.storage.btree.BTree;
const BTreeError = lattice.storage.btree.BTreeError;
const BufferPool = lattice.storage.buffer_pool.BufferPool;

const posting_mod = @import("posting.zig");
const DictionaryEntry = @import("dictionary.zig").DictionaryEntry;
const PREFIX_SIZE = @import("scoped_tree.zig").PREFIX_SIZE;

pub const ReclaimError = error{ OutOfMemory, StorageError };

/// What one reclamation removed.
pub const ReclaimStats = struct {
    /// Dictionary terms removed, each with its posting list.
    terms: u64 = 0,
    /// Document-length and statistics keys removed.
    length_keys: u64 = 0,
    /// Reverse-index keys removed.
    reverse_keys: u64 = 0,
};

/// Remove every full-text entry that no index in `declared` can reach.
///
/// Freed posting pages go to the file's freelist, for later writes to reuse;
/// only free pages at the end of the file shrink it.
pub fn reclaimUnreachable(
    allocator: Allocator,
    bp: *BufferPool,
    dict_tree: *BTree,
    lengths_tree: *BTree,
    reverse_tree: ?*BTree,
    declared: []const [PREFIX_SIZE]u8,
) ReclaimError!ReclaimStats {
    var stats: ReclaimStats = .{};
    var store = posting_mod.PostingStore.init(allocator, bp);

    const Dict = struct {
        declared: []const [PREFIX_SIZE]u8,
        fn unreachable_(ctx: @This(), key: []const u8) bool {
            if (key.len <= PREFIX_SIZE) return true;
            for (ctx.declared) |prefix| {
                if (std.mem.eql(u8, key[0..PREFIX_SIZE], &prefix)) return false;
            }
            return true;
        }
    };
    const dict_ctx = Dict{ .declared = declared };

    const Unscoped = struct {
        fn unreachable_(_: @This(), key: []const u8) bool {
            return key.len == 6 or key.len == 8;
        }
    };
    const Document = struct {
        fn unreachable_(_: @This(), key: []const u8) bool {
            return key.len == 8;
        }
    };

    stats.terms = try deleteWhere(allocator, dict_tree, dict_ctx, &store);
    stats.length_keys = try deleteWhere(allocator, lengths_tree, Unscoped{}, null);
    if (reverse_tree) |tree| stats.reverse_keys = try deleteWhere(allocator, tree, Document{}, null);
    return stats;
}

/// Delete the keys of `tree` that `ctx.unreachable_` picks, a batch at a
/// time: deleting under an open iterator is not safe. With `store`, each
/// value is a dictionary entry whose posting list is freed after its key.
fn deleteWhere(allocator: Allocator, tree: *BTree, ctx: anytype, store: ?*posting_mod.PostingStore) ReclaimError!u64 {
    const BATCH = 512;
    var keys: [BATCH][]u8 = undefined;
    var heads: [BATCH]lattice.core.types.PageId = undefined;
    var resume_key: ?[]u8 = null;
    defer if (resume_key) |k| allocator.free(k);
    var removed: u64 = 0;

    while (true) {
        var count: usize = 0;
        var finished = true;
        {
            var iter = tree.range(resume_key, null) catch return ReclaimError.StorageError;
            defer iter.deinit();
            while (iter.next() catch return ReclaimError.StorageError) |entry| {
                if (resume_key) |k| {
                    if (std.mem.eql(u8, k, entry.key)) continue;
                }
                if (!ctx.unreachable_(entry.key)) continue;
                if (count == BATCH) {
                    finished = false;
                    break;
                }
                keys[count] = allocator.dupe(u8, entry.key) catch {
                    for (keys[0..count]) |k| allocator.free(k);
                    return ReclaimError.OutOfMemory;
                };
                heads[count] = if (store != null and entry.value.len >= @sizeOf(DictionaryEntry))
                    DictionaryEntry.deserialize(entry.value).posting_page
                else
                    0;
                count += 1;
            }
        }
        defer for (keys[0..count]) |k| allocator.free(k);

        for (keys[0..count], heads[0..count]) |key, head| {
            tree.delete(key) catch |err| switch (err) {
                BTreeError.KeyNotFound => continue,
                else => return ReclaimError.StorageError,
            };
            removed += 1;
            if (store) |s| {
                if (head != 0) s.freeList(head) catch return ReclaimError.StorageError;
            }
        }
        if (finished or count == 0) return removed;

        // Go on after the last key taken; everything before it is either
        // gone or kept.
        if (resume_key) |k| allocator.free(k);
        resume_key = null;
        resume_key = allocator.dupe(u8, keys[count - 1]) catch return ReclaimError.OutOfMemory;
    }
}
