//! A B-tree view that keeps one full-text index out of another's way.
//!
//! The dictionary, document lengths, and reverse index are each a single tree
//! shared by every declared index. Without something separating them, a term
//! indexed for one property would be found when searching another — which is the
//! behaviour per-property indexes exist to replace.
//!
//! Every key written through this view carries a four-byte prefix identifying the
//! index it belongs to. Reads prepend the same prefix, so an index sees its own
//! entries and nothing else.
//!
//! ## Why a wrapper rather than threading a prefix through
//!
//! The alternative is passing the prefix to each place that builds a key, of
//! which there are a dozen across three files. Missing one would not fail to
//! compile; it would write an entry that the matching read could never find, or
//! worse, read another index's. Putting the prefix in one place makes that
//! mistake impossible to write rather than merely unlikely.
//!
//! ## The empty prefix
//!
//! A view with no prefix reads and writes keys unchanged, which is what the
//! index built before any of this existed uses. That keeps old data reachable
//! while both schemes are alive.

const std = @import("std");
const lattice = @import("lattice");

const btree = lattice.storage.btree;
const BTree = btree.BTree;
const BTreeError = lattice.storage.btree.BTreeError;

/// Bytes identifying which index an entry belongs to.
pub const PREFIX_SIZE = 4;

/// Longest key any full-text tree stores.
///
/// Tokens are capped by the tokenizer, document keys are eight bytes, and the
/// statistics key is six. This is generous against all of them so a key can be
/// assembled on the stack rather than allocated on every read.
const MAX_KEY_SIZE = 256;

/// A key too long to carry a prefix is reported as `PageFull`.
///
/// That is not a euphemism: keys of this size do not fit a page either, so the
/// caller's situation is the one that error already describes. Keeping the error
/// set identical to the tree's own means the stores wrapping it need no new
/// handling for a case none of them can reach — tokens are capped well below the
/// limit and document keys are eight bytes.
pub const ScopedTreeError = BTreeError;

pub const ScopedTree = struct {
    tree: *BTree,
    /// Null for the unscoped view, which passes keys through untouched.
    prefix: ?[PREFIX_SIZE]u8,

    const Self = @This();

    /// A view over the whole tree, with keys used exactly as given.
    pub fn unscoped(tree: *BTree) Self {
        return .{ .tree = tree, .prefix = null };
    }

    /// A view confined to one index.
    pub fn scoped(tree: *BTree, prefix: [PREFIX_SIZE]u8) Self {
        return .{ .tree = tree, .prefix = prefix };
    }

    pub fn isScoped(self: Self) bool {
        return self.prefix != null;
    }

    /// Build the stored key for a caller's key.
    fn storedKey(self: Self, key: []const u8, buf: []u8) ScopedTreeError![]const u8 {
        const prefix = self.prefix orelse return key;
        if (key.len + PREFIX_SIZE > buf.len) return ScopedTreeError.PageFull;
        @memcpy(buf[0..PREFIX_SIZE], &prefix);
        @memcpy(buf[PREFIX_SIZE..][0..key.len], key);
        return buf[0 .. PREFIX_SIZE + key.len];
    }

    pub fn insert(self: Self, key: []const u8, value: []const u8) ScopedTreeError!void {
        var buf: [MAX_KEY_SIZE]u8 = undefined;
        return self.tree.insert(try self.storedKey(key, &buf), value);
    }

    pub fn get(self: Self, key: []const u8) ScopedTreeError!?[]const u8 {
        var buf: [MAX_KEY_SIZE]u8 = undefined;
        return self.tree.get(try self.storedKey(key, &buf));
    }

    pub fn delete(self: Self, key: []const u8) ScopedTreeError!void {
        var buf: [MAX_KEY_SIZE]u8 = undefined;
        return self.tree.delete(try self.storedKey(key, &buf));
    }

    pub fn contains(self: Self, key: []const u8) ScopedTreeError!bool {
        var buf: [MAX_KEY_SIZE]u8 = undefined;
        return self.tree.contains(try self.storedKey(key, &buf));
    }

    pub fn freeValue(self: Self, value: []const u8) void {
        self.tree.freeValue(value);
    }

    /// An iterator that owns the bounds it was built with.
    ///
    /// The tree's iterator keeps the end key as a slice and compares against it
    /// on every step, so that memory has to outlive the call that produced it.
    /// Building the prefixed bounds in locals and handing out slices to them
    /// leaves the iterator reading freed stack: it appears to work, because the
    /// bytes are usually still there, and then stops working when something
    /// unrelated changes the stack. Keeping the bounds beside the iterator makes
    /// the lifetime the caller's to see.
    pub const Iterator = struct {
        inner: BTree.Iterator,
        start_buf: [MAX_KEY_SIZE]u8 = undefined,
        end_buf: [MAX_KEY_SIZE]u8 = undefined,

        pub fn next(self: *Iterator) ScopedTreeError!?btree.Entry {
            return self.inner.next();
        }

        pub fn deinit(self: *Iterator) void {
            self.inner.deinit();
        }
    };

    /// Range over a caller-supplied span, confined to this index.
    ///
    /// Null bounds mean "from the start" and "to the end", which for a scoped
    /// view means the start and end of this index rather than of the tree. That
    /// is what makes an unqualified walk see one index instead of all of them.
    ///
    /// The returned iterator owns its bounds, so it must not be copied after it
    /// is built — the tree's iterator holds a slice into it.
    pub fn rangeOwned(
        self: Self,
        start: ?[]const u8,
        end: ?[]const u8,
        out: *Iterator,
    ) ScopedTreeError!void {
        const prefix = self.prefix orelse {
            out.inner = try self.tree.range(start, end);
            return;
        };

        const lower = if (start) |k|
            try self.storedKey(k, &out.start_buf)
        else blk: {
            @memcpy(out.start_buf[0..PREFIX_SIZE], &prefix);
            break :blk out.start_buf[0..PREFIX_SIZE];
        };

        const upper = if (end) |k|
            try self.storedKey(k, &out.end_buf)
        else blk: {
            @memcpy(out.end_buf[0..PREFIX_SIZE], &nextPrefix(prefix));
            break :blk out.end_buf[0..PREFIX_SIZE];
        };

        out.inner = try self.tree.range(lower, upper);
    }

    /// Range with bounds that already live somewhere stable.
    ///
    /// Only safe for an unscoped view, where the bounds are passed through
    /// untouched and belong to the caller.
    pub fn range(
        self: Self,
        start: ?[]const u8,
        end: ?[]const u8,
    ) ScopedTreeError!BTree.Iterator {
        std.debug.assert(self.prefix == null);
        return self.tree.range(start, end);
    }

    /// Whether an entry would fit, accounting for the prefix the key will carry.
    pub fn canFitLeafEntry(self: Self, key: []const u8, value_len: usize) bool {
        if (self.prefix == null) return self.tree.canFitLeafEntry(key, value_len);
        var buf: [MAX_KEY_SIZE]u8 = undefined;
        const stored = self.storedKey(key, &buf) catch return false;
        return self.tree.canFitLeafEntry(stored, value_len);
    }

    /// Walk every entry belonging to this index.
    ///
    /// A scoped view ranges over its prefix rather than the whole tree, so
    /// iterating one index never sees another's entries.
    pub fn iterateAll(self: Self, out: *Iterator) ScopedTreeError!void {
        return self.rangeOwned(null, null, out);
    }

    /// Delete everything belonging to this index.
    ///
    /// Deleting while iterating is not safe — the iterator holds a pinned leaf and
    /// a slot in it, and a delete can restructure the tree underneath both — so
    /// this collects a batch of keys, closes the iterator, deletes them, and goes
    /// round again. A fixed batch keeps the memory constant rather than
    /// proportional to the index, which matters because the thing being cleared is
    /// sometimes the largest structure in the database.
    pub fn clear(self: Self, allocator: std.mem.Allocator) ScopedTreeError!void {
        const BATCH = 512;
        var keys: [BATCH][]u8 = undefined;

        while (true) {
            var count: usize = 0;
            {
                var iter: Iterator = undefined;
                try self.iterateAll(&iter);
                defer iter.deinit();
                while (count < BATCH) {
                    const entry = (try iter.next()) orelse break;
                    keys[count] = allocator.dupe(u8, entry.key) catch return ScopedTreeError.OutOfMemory;
                    count += 1;
                }
            }
            if (count == 0) return;

            defer for (keys[0..count]) |key| allocator.free(key);
            for (keys[0..count]) |key| {
                // The keys came out of the tree already carrying the prefix, so
                // they go back to the tree as they are rather than through the
                // view, which would prefix them a second time.
                self.tree.delete(key) catch |err| switch (err) {
                    ScopedTreeError.KeyNotFound => {},
                    else => return err,
                };
            }
        }
    }

    /// Point lookups made in ascending key order through one iterator.
    ///
    /// Each lookup seeks forward from where the last one ended, which is a
    /// binary search in the leaf already pinned unless the key lies beyond it.
    /// A run of lookups over a sorted batch of keys therefore touches each leaf
    /// it needs about once, rather than descending from the root and copying the
    /// value out for every key as `get` does.
    ///
    /// The value returned borrows the pinned leaf and is valid until the next
    /// call or `deinit`. Keys must not decrease between calls. Built in place,
    /// because the iterator holds slices of its own bound buffers.
    pub const SortedLookup = struct {
        view: Self,
        iter: Iterator,

        pub fn init(out: *SortedLookup, view: Self) ScopedTreeError!void {
            out.view = view;
            try view.rangeOwned(null, null, &out.iter);
        }

        pub fn get(self: *SortedLookup, key: []const u8) ScopedTreeError!?[]const u8 {
            var buf: [MAX_KEY_SIZE]u8 = undefined;
            const stored = try self.view.storedKey(key, &buf);
            try self.iter.inner.seekForward(stored);
            const entry = (try self.iter.inner.next()) orelse return null;
            if (!std.mem.eql(u8, entry.key, stored)) return null;
            return entry.value;
        }

        pub fn deinit(self: *SortedLookup) void {
            self.iter.deinit();
        }
    };

    /// Strip the prefix from a key an iterator returned.
    pub fn callerKey(self: Self, stored: []const u8) ?[]const u8 {
        if (self.prefix == null) return stored;
        if (stored.len < PREFIX_SIZE) return null;
        return stored[PREFIX_SIZE..];
    }
};

/// The smallest prefix ordering after every key of `prefix`.
///
/// A prefix ending in 0xFF has to carry into the byte before it. Incrementing the
/// last byte alone would wrap to zero and produce a bound below the prefix, so a
/// range would return nothing and the index would look empty.
fn nextPrefix(prefix: [PREFIX_SIZE]u8) [PREFIX_SIZE]u8 {
    var end = prefix;
    var i: usize = PREFIX_SIZE;
    while (i > 0) {
        i -= 1;
        if (end[i] != 0xFF) {
            end[i] += 1;
            return end;
        }
        end[i] = 0;
    }
    // Every byte was 0xFF, so this is the last index there is and the range runs
    // to the end of the tree.
    return [_]u8{0xFF} ** PREFIX_SIZE;
}

test "an unscoped view leaves keys alone" {
    const view = ScopedTree{ .tree = undefined, .prefix = null };
    var buf: [MAX_KEY_SIZE]u8 = undefined;
    const stored = try view.storedKey("token", &buf);
    try std.testing.expectEqualStrings("token", stored);
    try std.testing.expect(!view.isScoped());
}

test "a scoped view puts its index in front" {
    const view = ScopedTree{ .tree = undefined, .prefix = [_]u8{ 0, 3, 0, 7 } };
    var buf: [MAX_KEY_SIZE]u8 = undefined;
    const stored = try view.storedKey("token", &buf);

    try std.testing.expectEqual(@as(usize, 9), stored.len);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0, 3, 0, 7 }, stored[0..4]);
    try std.testing.expectEqualStrings("token", stored[4..]);
    try std.testing.expectEqualStrings("token", view.callerKey(stored).?);
}

test "two indexes never produce the same stored key" {
    const title = ScopedTree{ .tree = undefined, .prefix = [_]u8{ 0, 3, 0, 1 } };
    const body = ScopedTree{ .tree = undefined, .prefix = [_]u8{ 0, 3, 0, 2 } };

    var a: [MAX_KEY_SIZE]u8 = undefined;
    var b: [MAX_KEY_SIZE]u8 = undefined;

    // The same term in two indexes has to land in two places, which is the whole
    // point: searching titles must not find a term that only appears in bodies.
    const in_title = try title.storedKey("bread", &a);
    const in_body = try body.storedKey("bread", &b);
    try std.testing.expect(!std.mem.eql(u8, in_title, in_body));
}

test "a key too long to prefix is refused rather than truncated" {
    const view = ScopedTree{ .tree = undefined, .prefix = [_]u8{ 0, 1, 0, 1 } };
    var buf: [8]u8 = undefined;
    // Silently writing a truncated key would put the entry somewhere no read
    // could find it, which is worse than refusing.
    try std.testing.expectError(
        ScopedTreeError.PageFull,
        view.storedKey("a key that will not fit", &buf),
    );
}

test "the range end covers every key of one index and no more" {
    // Incrementing the last byte alone would wrap to zero and give a bound below
    // the prefix, so the range would return nothing and the index would look
    // empty.
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0, 3, 1, 0 }, &nextPrefix([_]u8{ 0, 3, 0, 0xFF }));
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0, 3, 0, 8 }, &nextPrefix([_]u8{ 0, 3, 0, 7 }));
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0, 4, 0, 0 }, &nextPrefix([_]u8{ 0, 3, 0xFF, 0xFF }));
}
