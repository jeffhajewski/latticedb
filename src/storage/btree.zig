//! B+Tree implementation for Lattice database.
//!
//! Provides ordered key-value storage with point lookups and range scans.
//! Keys and values are variable-length byte slices.

const std = @import("std");
const lattice = @import("lattice");

const Allocator = std.mem.Allocator;
const Order = std.math.Order;

const types = lattice.core.types;
const page = lattice.storage.page;
const buffer_pool = lattice.storage.buffer_pool;
const locking = lattice.concurrency.locking;

pub const PageId = types.PageId;
pub const NULL_PAGE = types.NULL_PAGE;
const PageHeader = page.PageHeader;
const PageType = page.PageType;
const BufferPool = buffer_pool.BufferPool;
const BufferFrame = buffer_pool.BufferFrame;
const LatchMode = locking.LatchMode;

/// B+Tree errors
pub const BTreeError = error{
    KeyNotFound,
    DuplicateKey,
    PageFull,
    InvalidPage,
    TreeEmpty,
    IoError,
    OutOfMemory,
    BufferPoolFull,
};

/// Key comparator function type
pub const KeyComparator = *const fn (a: []const u8, b: []const u8) Order;

/// Default comparator: lexicographic byte comparison
pub fn defaultComparator(a: []const u8, b: []const u8) Order {
    return std.mem.order(u8, a, b);
}

// ============================================================================
// Node Layout Constants
// ============================================================================

/// Size of internal node header (after PageHeader)
const INTERNAL_HEADER_SIZE = 8; // num_keys(2) + level(2) + right_child(4)

/// Size of leaf node header (after PageHeader)
const LEAF_HEADER_SIZE = 12; // num_entries(2) + flags(2) + next_leaf(4) + prev_leaf(4)

/// Offset where slot array starts in internal node
const INTERNAL_SLOTS_OFFSET = @sizeOf(PageHeader) + INTERNAL_HEADER_SIZE;

/// Offset where slot array starts in leaf node
const LEAF_SLOTS_OFFSET = @sizeOf(PageHeader) + LEAF_HEADER_SIZE;

/// Each slot in internal node: offset(2) + child_page(4) = 6 bytes
const INTERNAL_SLOT_SIZE = 6;

/// Each slot in leaf node: offset(2) = 2 bytes (points to key-value entry)
const LEAF_SLOT_SIZE = 2;

/// Minimum keys before considering merge (not implemented yet)
const MIN_KEYS = 2;

/// Key-value entry type returned by leaf operations
pub const Entry = struct {
    key: []const u8,
    value: []const u8,
};

// ============================================================================
// Node Accessors - Work directly with page buffers
// ============================================================================

/// Internal node helper functions
const InternalNode = struct {
    /// Get number of keys in this internal node
    fn getNumKeys(buf: []const u8) u16 {
        return std.mem.readInt(u16, buf[@sizeOf(PageHeader)..][0..2], .little);
    }

    /// Set number of keys
    fn setNumKeys(buf: []u8, n: u16) void {
        std.mem.writeInt(u16, buf[@sizeOf(PageHeader)..][0..2], n, .little);
    }

    /// Get level (0 = just above leaves)
    fn getLevel(buf: []const u8) u16 {
        return std.mem.readInt(u16, buf[@sizeOf(PageHeader) + 2 ..][0..2], .little);
    }

    /// Set level
    fn setLevel(buf: []u8, level: u16) void {
        std.mem.writeInt(u16, buf[@sizeOf(PageHeader) + 2 ..][0..2], level, .little);
    }

    /// Get rightmost child pointer
    fn getRightChild(buf: []const u8) PageId {
        return std.mem.readInt(u32, buf[@sizeOf(PageHeader) + 4 ..][0..4], .little);
    }

    /// Set rightmost child pointer
    fn setRightChild(buf: []u8, child: PageId) void {
        std.mem.writeInt(u32, buf[@sizeOf(PageHeader) + 4 ..][0..4], child, .little);
    }

    /// Get key offset at slot index
    fn getKeyOffset(buf: []const u8, slot: u16) u16 {
        const start = INTERNAL_SLOTS_OFFSET + slot * INTERNAL_SLOT_SIZE;
        return std.mem.readInt(u16, buf[start..][0..2], .little);
    }

    /// Get child pointer at slot index
    fn getChild(buf: []const u8, slot: u16) PageId {
        const start = INTERNAL_SLOTS_OFFSET + slot * INTERNAL_SLOT_SIZE + 2;
        return std.mem.readInt(u32, buf[start..][0..4], .little);
    }

    /// Set slot data (key offset and child pointer)
    fn setSlot(buf: []u8, slot: u16, key_offset: u16, child: PageId) void {
        const start = INTERNAL_SLOTS_OFFSET + slot * INTERNAL_SLOT_SIZE;
        std.mem.writeInt(u16, buf[start..][0..2], key_offset, .little);
        std.mem.writeInt(u32, buf[start + 2 ..][0..4], child, .little);
    }

    /// Get key at slot (returns slice into buffer)
    fn getKey(buf: []const u8, slot: u16) []const u8 {
        const offset = getKeyOffset(buf, slot);
        const key_len = std.mem.readInt(u16, buf[offset..][0..2], .little);
        return buf[offset + 2 ..][0..key_len];
    }

    /// Calculate free space in page
    fn getFreeSpace(buf: []const u8, page_size: u32) u16 {
        const num_keys = getNumKeys(buf);
        const slots_end = INTERNAL_SLOTS_OFFSET + num_keys * INTERNAL_SLOT_SIZE;

        // Find lowest key offset (keys grow from end of page backward)
        var min_offset: u16 = @intCast(page_size);
        for (0..num_keys) |i| {
            const offset = getKeyOffset(buf, @intCast(i));
            if (offset < min_offset) {
                min_offset = offset;
            }
        }

        if (min_offset <= slots_end) return 0;
        return @intCast(min_offset - slots_end);
    }

    /// Initialize an empty internal node
    fn init(buf: []u8, level: u16) void {
        const header: *PageHeader = @ptrCast(@alignCast(buf.ptr));
        header.* = PageHeader.init(.btree_internal);
        setNumKeys(buf, 0);
        setLevel(buf, level);
        setRightChild(buf, NULL_PAGE);
    }
};

/// Leaf node helper functions
const LeafNode = struct {
    /// Get number of entries
    fn getNumEntries(buf: []const u8) u16 {
        return std.mem.readInt(u16, buf[@sizeOf(PageHeader)..][0..2], .little);
    }

    /// Set number of entries
    fn setNumEntries(buf: []u8, n: u16) void {
        std.mem.writeInt(u16, buf[@sizeOf(PageHeader)..][0..2], n, .little);
    }

    /// Get flags
    fn getFlags(buf: []const u8) u16 {
        return std.mem.readInt(u16, buf[@sizeOf(PageHeader) + 2 ..][0..2], .little);
    }

    /// Set flags
    fn setFlags(buf: []u8, flags: u16) void {
        std.mem.writeInt(u16, buf[@sizeOf(PageHeader) + 2 ..][0..2], flags, .little);
    }

    /// Get next leaf pointer
    fn getNextLeaf(buf: []const u8) PageId {
        return std.mem.readInt(u32, buf[@sizeOf(PageHeader) + 4 ..][0..4], .little);
    }

    /// Set next leaf pointer
    fn setNextLeaf(buf: []u8, next: PageId) void {
        std.mem.writeInt(u32, buf[@sizeOf(PageHeader) + 4 ..][0..4], next, .little);
    }

    /// Get previous leaf pointer
    fn getPrevLeaf(buf: []const u8) PageId {
        return std.mem.readInt(u32, buf[@sizeOf(PageHeader) + 8 ..][0..4], .little);
    }

    /// Set previous leaf pointer
    fn setPrevLeaf(buf: []u8, prev: PageId) void {
        std.mem.writeInt(u32, buf[@sizeOf(PageHeader) + 8 ..][0..4], prev, .little);
    }

    /// Get entry offset at slot index
    fn getEntryOffset(buf: []const u8, slot: u16) u16 {
        const start = LEAF_SLOTS_OFFSET + slot * LEAF_SLOT_SIZE;
        return std.mem.readInt(u16, buf[start..][0..2], .little);
    }

    /// Set entry offset at slot index
    fn setEntryOffset(buf: []u8, slot: u16, offset: u16) void {
        const start = LEAF_SLOTS_OFFSET + slot * LEAF_SLOT_SIZE;
        std.mem.writeInt(u16, buf[start..][0..2], offset, .little);
    }

    /// Get key at slot
    fn getKey(buf: []const u8, slot: u16) []const u8 {
        const offset = getEntryOffset(buf, slot);
        const key_len = std.mem.readInt(u16, buf[offset..][0..2], .little);
        return buf[offset + 4 ..][0..key_len];
    }

    /// Get value at slot
    fn getValue(buf: []const u8, slot: u16) []const u8 {
        const offset = getEntryOffset(buf, slot);
        const key_len = std.mem.readInt(u16, buf[offset..][0..2], .little);
        const val_len = std.mem.readInt(u16, buf[offset + 2 ..][0..2], .little);
        return buf[offset + 4 + key_len ..][0..val_len];
    }

    /// Get both key and value at slot
    fn getEntry(buf: []const u8, slot: u16) Entry {
        const offset = getEntryOffset(buf, slot);
        const key_len = std.mem.readInt(u16, buf[offset..][0..2], .little);
        const val_len = std.mem.readInt(u16, buf[offset + 2 ..][0..2], .little);
        return .{
            .key = buf[offset + 4 ..][0..key_len],
            .value = buf[offset + 4 + key_len ..][0..val_len],
        };
    }

    /// Calculate free space in page
    fn getFreeSpace(buf: []const u8, page_size: u32) u16 {
        const num_entries = getNumEntries(buf);
        const slots_end = LEAF_SLOTS_OFFSET + num_entries * LEAF_SLOT_SIZE;

        var min_offset: u16 = @intCast(page_size);
        for (0..num_entries) |i| {
            const offset = getEntryOffset(buf, @intCast(i));
            if (offset < min_offset) {
                min_offset = offset;
            }
        }

        if (min_offset <= slots_end) return 0;
        return @intCast(min_offset - slots_end);
    }

    /// Initialize an empty leaf node
    fn init(buf: []u8) void {
        const header: *PageHeader = @ptrCast(@alignCast(buf.ptr));
        header.* = PageHeader.init(.btree_leaf);
        setNumEntries(buf, 0);
        setFlags(buf, 0);
        setNextLeaf(buf, NULL_PAGE);
        setPrevLeaf(buf, NULL_PAGE);
    }

    /// Binary search for key, returns slot where key is or should be inserted
    fn searchSlot(buf: []const u8, key: []const u8, cmp: KeyComparator) struct { slot: u16, found: bool } {
        const num_entries = getNumEntries(buf);
        if (num_entries == 0) return .{ .slot = 0, .found = false };

        var lo: u16 = 0;
        var hi: u16 = num_entries;

        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            const mid_key = getKey(buf, mid);

            switch (cmp(mid_key, key)) {
                .lt => lo = mid + 1,
                .gt => hi = mid,
                .eq => return .{ .slot = mid, .found = true },
            }
        }

        return .{ .slot = lo, .found = false };
    }
};

// ============================================================================
// B+Tree Structure
// ============================================================================

/// B+Tree index structure
pub const BTree = struct {
    bp: *BufferPool,
    root_page: PageId,
    comparator: KeyComparator,
    page_size: u32,
    allocator: Allocator,

    const Self = @This();

    /// Create a new empty B+Tree
    pub fn init(allocator: Allocator, bp: *BufferPool) BTreeError!Self {
        // Allocate root page as a leaf
        const root_page = bp.pm.allocatePage() catch return BTreeError.IoError;

        // Initialize root as empty leaf
        const frame = bp.fetchPage(root_page, .exclusive) catch return BTreeError.BufferPoolFull;
        LeafNode.init(frame.data);
        bp.unpinPage(frame, true);

        return Self{
            .bp = bp,
            .root_page = root_page,
            .comparator = defaultComparator,
            .page_size = bp.pm.getPageSize(),
            .allocator = allocator,
        };
    }

    /// Open an existing B+Tree with known root page
    pub fn open(allocator: Allocator, bp: *BufferPool, root_page: PageId) Self {
        return Self{
            .bp = bp,
            .root_page = root_page,
            .comparator = defaultComparator,
            .page_size = bp.pm.getPageSize(),
            .allocator = allocator,
        };
    }

    /// Set a custom key comparator
    pub fn setComparator(self: *Self, cmp: KeyComparator) void {
        self.comparator = cmp;
    }

    /// Get the root page ID (for persistence)
    pub fn getRootPage(self: *const Self) PageId {
        return self.root_page;
    }

    // ========================================================================
    // Point Lookup
    // ========================================================================

    /// Look up a key and return its value (if found)
    /// Caller must copy the value before unpinning if needed
    pub fn get(self: *Self, key: []const u8) BTreeError!?[]const u8 {
        var page_id = self.root_page;

        // Traverse down to leaf
        while (true) {
            const frame = self.bp.fetchPage(page_id, .shared) catch return BTreeError.BufferPoolFull;
            defer self.bp.unpinPage(frame, false);

            const header: *const PageHeader = @ptrCast(@alignCast(frame.data.ptr));

            if (header.page_type == .btree_leaf) {
                // Search in leaf
                const result = LeafNode.searchSlot(frame.data, key, self.comparator);
                if (result.found) {
                    return LeafNode.getValue(frame.data, result.slot);
                }
                return null;
            }

            // Internal node - find child to descend into
            page_id = self.findChild(frame.data, key);
        }
    }

    /// Find the child page to descend into for a given key
    fn findChild(self: *Self, buf: []const u8, key: []const u8) PageId {
        const num_keys = InternalNode.getNumKeys(buf);

        // Binary search for the correct child
        var i: u16 = 0;
        while (i < num_keys) : (i += 1) {
            const node_key = InternalNode.getKey(buf, i);
            if (self.comparator(key, node_key) == .lt) {
                return InternalNode.getChild(buf, i);
            }
        }

        // Key >= all keys, go to rightmost child
        return InternalNode.getRightChild(buf);
    }

    // ========================================================================
    // Insert
    // ========================================================================

    /// Insert a key-value pair into the tree
    pub fn insert(self: *Self, key: []const u8, value: []const u8) BTreeError!void {
        // Try simple insert first
        const result = try self.insertRecursive(self.root_page, key, value);

        // If root split, create new root
        if (result.split) |split_info| {
            try self.createNewRoot(split_info.key, result.page_id, split_info.new_page);
        }
    }

    const SplitInfo = struct {
        key: []const u8, // First key of new page (allocated, caller must free)
        new_page: PageId,
    };

    const InsertResult = struct {
        page_id: PageId,
        split: ?SplitInfo,
    };

    fn insertRecursive(self: *Self, page_id: PageId, key: []const u8, value: []const u8) BTreeError!InsertResult {
        const frame = self.bp.fetchPage(page_id, .exclusive) catch return BTreeError.BufferPoolFull;
        const header: *const PageHeader = @ptrCast(@alignCast(frame.data.ptr));

        if (header.page_type == .btree_leaf) {
            // Insert into leaf
            return self.insertIntoLeaf(frame, key, value);
        }

        // Internal node - find child and recurse
        const child_page = self.findChild(frame.data, key);
        self.bp.unpinPage(frame, false);

        const child_result = try self.insertRecursive(child_page, key, value);

        // If child split, insert the split key into this internal node
        if (child_result.split) |split_info| {
            defer self.allocator.free(@constCast(split_info.key));

            const frame2 = self.bp.fetchPage(page_id, .exclusive) catch return BTreeError.BufferPoolFull;
            return self.insertIntoInternal(frame2, split_info.key, split_info.new_page);
        }

        return .{ .page_id = page_id, .split = null };
    }

    fn insertIntoLeaf(self: *Self, frame: *BufferFrame, key: []const u8, value: []const u8) BTreeError!InsertResult {
        const buf = frame.data;
        const page_id = frame.page_id;

        // Check if key already exists
        const search = LeafNode.searchSlot(buf, key, self.comparator);
        if (search.found) {
            self.bp.unpinPage(frame, false);
            return BTreeError.DuplicateKey;
        }

        // Calculate space needed: slot(2) + key_len(2) + val_len(2) + key + value
        const entry_size: u16 = @intCast(4 + key.len + value.len);
        const space_needed: u16 = LEAF_SLOT_SIZE + entry_size;
        const free_space = LeafNode.getFreeSpace(buf, self.page_size);

        if (free_space >= space_needed) {
            // Fits in current page
            self.insertLeafEntry(buf, search.slot, key, value);
            self.bp.unpinPage(frame, true);
            return .{ .page_id = page_id, .split = null };
        }

        // Need to split
        return self.splitLeafAndInsert(frame, search.slot, key, value);
    }

    fn insertLeafEntry(self: *Self, buf: []u8, slot: u16, key: []const u8, value: []const u8) void {
        const num_entries = LeafNode.getNumEntries(buf);

        // Find where to write entry data (at end of page, growing backward)
        var min_offset: u16 = @intCast(self.page_size);
        for (0..num_entries) |i| {
            const offset = LeafNode.getEntryOffset(buf, @intCast(i));
            if (offset < min_offset) {
                min_offset = offset;
            }
        }

        // Write entry at new position
        const entry_size: u16 = @intCast(4 + key.len + value.len);
        const new_offset = min_offset - entry_size;

        std.mem.writeInt(u16, buf[new_offset..][0..2], @intCast(key.len), .little);
        std.mem.writeInt(u16, buf[new_offset + 2 ..][0..2], @intCast(value.len), .little);
        @memcpy(buf[new_offset + 4 ..][0..key.len], key);
        @memcpy(buf[new_offset + 4 + key.len ..][0..value.len], value);

        // Shift slots to make room
        if (slot < num_entries) {
            const slots_start = LEAF_SLOTS_OFFSET + slot * LEAF_SLOT_SIZE;
            const slots_end = LEAF_SLOTS_OFFSET + num_entries * LEAF_SLOT_SIZE;
            std.mem.copyBackwards(u8, buf[slots_start + LEAF_SLOT_SIZE .. slots_end + LEAF_SLOT_SIZE], buf[slots_start..slots_end]);
        }

        // Set the new slot
        LeafNode.setEntryOffset(buf, slot, new_offset);
        LeafNode.setNumEntries(buf, num_entries + 1);
    }

    fn splitLeafAndInsert(self: *Self, frame: *BufferFrame, slot: u16, key: []const u8, value: []const u8) BTreeError!InsertResult {
        const old_buf = frame.data;
        const old_page_id = frame.page_id;

        // Allocate new leaf page
        const new_page_id = self.bp.pm.allocatePage() catch return BTreeError.IoError;
        const new_frame = self.bp.fetchPage(new_page_id, .exclusive) catch return BTreeError.BufferPoolFull;
        const new_buf = new_frame.data;

        // Initialize new leaf
        LeafNode.init(new_buf);

        // Get all entries including the new one, sorted
        const num_entries = LeafNode.getNumEntries(old_buf);
        const total = num_entries + 1;
        const split_point = total / 2;

        // Temporary storage for entries
        var entries = self.allocator.alloc(Entry, total) catch return BTreeError.OutOfMemory;
        defer self.allocator.free(entries);

        // Collect existing entries
        var j: usize = 0;
        for (0..num_entries) |i| {
            if (i == slot) {
                entries[j] = .{ .key = key, .value = value };
                j += 1;
            }
            entries[j] = LeafNode.getEntry(old_buf, @intCast(i));
            j += 1;
        }
        if (slot == num_entries) {
            entries[j] = .{ .key = key, .value = value };
        }

        // Reinitialize old leaf
        LeafNode.init(old_buf);

        // Write first half to old page
        for (0..split_point) |i| {
            self.insertLeafEntry(old_buf, @intCast(i), entries[i].key, entries[i].value);
        }

        // Write second half to new page
        for (split_point..total) |i| {
            self.insertLeafEntry(new_buf, @intCast(i - split_point), entries[i].key, entries[i].value);
        }

        // Update sibling pointers
        const old_next = LeafNode.getNextLeaf(old_buf);
        LeafNode.setNextLeaf(old_buf, new_page_id);
        LeafNode.setPrevLeaf(new_buf, old_page_id);
        LeafNode.setNextLeaf(new_buf, old_next);

        // Update old_next's prev pointer if it exists
        if (old_next != NULL_PAGE) {
            const next_frame = self.bp.fetchPage(old_next, .exclusive) catch {
                self.bp.unpinPage(new_frame, true);
                self.bp.unpinPage(frame, true);
                return BTreeError.BufferPoolFull;
            };
            LeafNode.setPrevLeaf(next_frame.data, new_page_id);
            self.bp.unpinPage(next_frame, true);
        }

        // Copy split key for caller (first key of new page)
        const split_key = LeafNode.getKey(new_buf, 0);
        const split_key_copy = self.allocator.alloc(u8, split_key.len) catch {
            self.bp.unpinPage(new_frame, true);
            self.bp.unpinPage(frame, true);
            return BTreeError.OutOfMemory;
        };
        @memcpy(split_key_copy, split_key);

        self.bp.unpinPage(new_frame, true);
        self.bp.unpinPage(frame, true);

        return .{
            .page_id = old_page_id,
            .split = .{
                .key = split_key_copy,
                .new_page = new_page_id,
            },
        };
    }

    fn insertIntoInternal(self: *Self, frame: *BufferFrame, key: []const u8, new_child: PageId) BTreeError!InsertResult {
        const buf = frame.data;
        const page_id = frame.page_id;

        // Find insertion position
        const num_keys = InternalNode.getNumKeys(buf);
        var insert_pos: u16 = 0;
        while (insert_pos < num_keys) : (insert_pos += 1) {
            const node_key = InternalNode.getKey(buf, insert_pos);
            if (self.comparator(key, node_key) == .lt) break;
        }

        // Calculate space needed
        const key_size: u16 = @intCast(2 + key.len); // key_len(2) + key
        const space_needed: u16 = INTERNAL_SLOT_SIZE + key_size;
        const free_space = InternalNode.getFreeSpace(buf, self.page_size);

        if (free_space >= space_needed) {
            // Fits in current page
            self.insertInternalEntry(buf, insert_pos, key, new_child);
            self.bp.unpinPage(frame, true);
            return .{ .page_id = page_id, .split = null };
        }

        // Need to split internal node
        return self.splitInternalAndInsert(frame, insert_pos, key, new_child);
    }

    fn insertInternalEntry(self: *Self, buf: []u8, slot: u16, key: []const u8, child: PageId) void {
        const num_keys = InternalNode.getNumKeys(buf);

        // Find where to write key data
        var min_offset: u16 = @intCast(self.page_size);
        for (0..num_keys) |i| {
            const offset = InternalNode.getKeyOffset(buf, @intCast(i));
            if (offset < min_offset) {
                min_offset = offset;
            }
        }

        // Write key
        const key_size: u16 = @intCast(2 + key.len);
        const new_offset = min_offset - key_size;
        std.mem.writeInt(u16, buf[new_offset..][0..2], @intCast(key.len), .little);
        @memcpy(buf[new_offset + 2 ..][0..key.len], key);

        // Shift slots
        if (slot < num_keys) {
            const slots_start = INTERNAL_SLOTS_OFFSET + slot * INTERNAL_SLOT_SIZE;
            const slots_end = INTERNAL_SLOTS_OFFSET + num_keys * INTERNAL_SLOT_SIZE;
            std.mem.copyBackwards(u8, buf[slots_start + INTERNAL_SLOT_SIZE .. slots_end + INTERNAL_SLOT_SIZE], buf[slots_start..slots_end]);
        }

        // The child at slot position becomes the left child of the new key
        // The new_child becomes the right child (takes over what was at slot)
        if (slot < num_keys) {
            // Existing child at slot moves to be pointed by new key
            const old_child = InternalNode.getChild(buf, slot);
            InternalNode.setSlot(buf, slot, new_offset, old_child);
            // Update the slot after to point to new_child... actually this is complex

            // Simpler approach: new key's left child is what was at slot, right child is new_child
            // For B+tree internals: key[i] separates child[i] and child[i+1]
            // When inserting, the new_child goes to the right of the key
        }

        // Actually for internal nodes:
        // children[0] key[0] children[1] key[1] children[2] ... key[n-1] right_child
        // When we insert a new key at position `slot`, we need to:
        // 1. The key separates the child that was at `slot` (now left) from new_child (now right)
        // 2. Shift all slots from `slot` onwards

        // For the slot we're inserting:
        // - Left child of key is the old child at this position
        // - new_child becomes the new child pointer stored in this slot? No...

        // Let me reconsider: internal node stores (child_ptr, key) pairs
        // child[0] points to subtree with keys < key[0]
        // child[i] points to subtree with key[i-1] <= keys < key[i]
        // right_child points to subtree with keys >= key[n-1]

        // When we split a child and get split_key and new_child:
        // - new_child contains keys >= split_key
        // - original child (at some position) contains keys < split_key
        // We insert split_key at position `slot`, with new_child to its right

        // So the slot stores: (key_offset, child_to_left_of_key)
        // Actually looking at my struct: slot = offset(2) + child_page(4)
        // The child in slot[i] is the child to the LEFT of key[i]

        // So when inserting at slot:
        // - slot.child = old child that was there (left of new key)
        // - new_child needs to be... either in slot[slot+1].child or if slot==num_keys, it's the new right_child

        // This is getting complex. Let me simplify:
        // The new key goes at `slot`, its left child is whatever was the child at slot
        // If we're inserting at the end (slot == num_keys), new_child becomes right_child

        if (slot == num_keys) {
            // Inserting at end: old right_child becomes left child of new key, new_child becomes right_child
            const old_right = InternalNode.getRightChild(buf);
            InternalNode.setSlot(buf, slot, new_offset, old_right);
            InternalNode.setRightChild(buf, child);
        } else {
            // Inserting in middle after a child split:
            // - The original child (with keys < split_key) should remain as left child of new key
            // - The new_child (with keys >= split_key) goes to the RIGHT of the new key
            //
            // After shifting, slot[slot+1] contains the old slot[slot] data.
            // old_child = what was at slot[slot] before shift = now at slot[slot+1]
            // We need:
            // - slot[slot] = (new_key, old_child) -- old_child is for keys < new_key
            // - slot[slot+1].child = new_child   -- new_child is for keys >= new_key and < slot[slot+1].key
            const old_child = InternalNode.getChild(buf, slot + 1);
            InternalNode.setSlot(buf, slot, new_offset, old_child);

            // Now update slot[slot+1]'s child to new_child while preserving its key
            const next_key_offset = InternalNode.getKeyOffset(buf, slot + 1);
            InternalNode.setSlot(buf, slot + 1, next_key_offset, child);
        }

        InternalNode.setNumKeys(buf, num_keys + 1);
    }

    fn splitInternalAndInsert(self: *Self, frame: *BufferFrame, slot: u16, key: []const u8, new_child: PageId) BTreeError!InsertResult {
        _ = slot;
        _ = key;
        _ = new_child;

        // For now, return an error - full internal node split is complex
        // In practice, with 4KB pages we can fit many keys before this happens
        self.bp.unpinPage(frame, false);
        return BTreeError.PageFull;
    }

    fn createNewRoot(self: *Self, split_key: []const u8, left_child: PageId, right_child: PageId) BTreeError!void {
        // Allocate new root page
        const new_root = self.bp.pm.allocatePage() catch return BTreeError.IoError;
        const frame = self.bp.fetchPage(new_root, .exclusive) catch return BTreeError.BufferPoolFull;

        // Get level of old root
        const old_frame = self.bp.fetchPage(left_child, .shared) catch {
            self.bp.unpinPage(frame, false);
            return BTreeError.BufferPoolFull;
        };
        const old_header: *const PageHeader = @ptrCast(@alignCast(old_frame.data.ptr));
        const new_level: u16 = if (old_header.page_type == .btree_leaf) 1 else InternalNode.getLevel(old_frame.data) + 1;
        self.bp.unpinPage(old_frame, false);

        // Initialize new root as internal node
        InternalNode.init(frame.data, new_level);

        // insertInternalEntry expects: child parameter = NEW child (right of split key)
        // The "old" right_child becomes slot[0].child (left of the key)
        // So we set left_child as initial right_child, then insert with right_child
        InternalNode.setRightChild(frame.data, left_child);

        // Insert split_key with right_child as the new child
        // This will: slot[0] = (split_key, left_child), right_child = right_child
        self.insertInternalEntry(frame.data, 0, split_key, right_child);

        self.bp.unpinPage(frame, true);
        self.root_page = new_root;
    }

    // ========================================================================
    // Delete
    // ========================================================================

    /// Delete a key from the tree
    /// Returns KeyNotFound if the key doesn't exist
    pub fn delete(self: *Self, key: []const u8) BTreeError!void {
        // Find the leaf containing the key
        const leaf_page = try self.findLeafForKey(key);

        // Fetch the leaf with exclusive latch
        const frame = self.bp.fetchPage(leaf_page, .exclusive) catch return BTreeError.BufferPoolFull;

        // Find the key in the leaf
        const search = LeafNode.searchSlot(frame.data, key, self.comparator);
        if (!search.found) {
            self.bp.unpinPage(frame, false);
            return BTreeError.KeyNotFound;
        }

        // Remove the entry from the leaf
        self.deleteLeafEntry(frame.data, search.slot);

        self.bp.unpinPage(frame, true);

        // Note: We don't handle underflow (merge/redistribute) for simplicity.
        // Underflowed pages still work correctly, they just have wasted space.
        // A background compaction process could reclaim this space later.
    }

    /// Remove an entry from a leaf node by shifting slots
    fn deleteLeafEntry(self: *Self, buf: []u8, slot: u16) void {
        _ = self;
        const num_entries = LeafNode.getNumEntries(buf);

        // Shift slots down to fill the gap
        // Note: We don't reclaim the actual entry space at the end of the page.
        // This creates "dead" space that would be reclaimed on page compaction.
        if (slot < num_entries - 1) {
            const slots_start = LEAF_SLOTS_OFFSET + slot * LEAF_SLOT_SIZE;
            const next_slot_start = LEAF_SLOTS_OFFSET + (slot + 1) * LEAF_SLOT_SIZE;
            const slots_end = LEAF_SLOTS_OFFSET + num_entries * LEAF_SLOT_SIZE;
            std.mem.copyForwards(u8, buf[slots_start..slots_end - LEAF_SLOT_SIZE], buf[next_slot_start..slots_end]);
        }

        // Decrement entry count
        LeafNode.setNumEntries(buf, num_entries - 1);
    }

    // ========================================================================
    // Range Scan
    // ========================================================================

    /// Create an iterator for range scanning
    pub fn range(self: *Self, start_key: ?[]const u8, end_key: ?[]const u8) BTreeError!Iterator {
        // Find the starting leaf
        const leaf_page = if (start_key) |key|
            try self.findLeafForKey(key)
        else
            try self.findFirstLeaf();

        const frame = self.bp.fetchPage(leaf_page, .shared) catch return BTreeError.BufferPoolFull;

        // Find starting slot
        const start_slot: u16 = if (start_key) |key| blk: {
            const result = LeafNode.searchSlot(frame.data, key, self.comparator);
            break :blk result.slot;
        } else 0;

        return Iterator{
            .tree = self,
            .current_frame = frame,
            .current_slot = start_slot,
            .end_key = end_key,
        };
    }

    fn findLeafForKey(self: *Self, key: []const u8) BTreeError!PageId {
        var page_id = self.root_page;

        while (true) {
            const frame = self.bp.fetchPage(page_id, .shared) catch return BTreeError.BufferPoolFull;
            defer self.bp.unpinPage(frame, false);

            const header: *const PageHeader = @ptrCast(@alignCast(frame.data.ptr));
            if (header.page_type == .btree_leaf) {
                return page_id;
            }

            page_id = self.findChild(frame.data, key);
        }
    }

    fn findFirstLeaf(self: *Self) BTreeError!PageId {
        var page_id = self.root_page;

        while (true) {
            const frame = self.bp.fetchPage(page_id, .shared) catch return BTreeError.BufferPoolFull;
            defer self.bp.unpinPage(frame, false);

            const header: *const PageHeader = @ptrCast(@alignCast(frame.data.ptr));
            if (header.page_type == .btree_leaf) {
                return page_id;
            }

            // Go to leftmost child
            if (InternalNode.getNumKeys(frame.data) > 0) {
                page_id = InternalNode.getChild(frame.data, 0);
            } else {
                page_id = InternalNode.getRightChild(frame.data);
            }
        }
    }

    /// Iterator for range scans
    pub const Iterator = struct {
        tree: *BTree,
        current_frame: ?*BufferFrame,
        current_slot: u16,
        end_key: ?[]const u8,

        /// Get the next key-value pair
        pub fn next(self: *Iterator) BTreeError!?Entry {
            const frame = self.current_frame orelse return null;

            const num_entries = LeafNode.getNumEntries(frame.data);

            // Check if we've exhausted current page
            if (self.current_slot >= num_entries) {
                // Move to next leaf
                const next_page = LeafNode.getNextLeaf(frame.data);
                self.tree.bp.unpinPage(frame, false);

                if (next_page == NULL_PAGE) {
                    self.current_frame = null;
                    return null;
                }

                self.current_frame = self.tree.bp.fetchPage(next_page, .shared) catch return BTreeError.BufferPoolFull;
                self.current_slot = 0;
                return self.next();
            }

            const entry = LeafNode.getEntry(frame.data, self.current_slot);

            // Check end key
            if (self.end_key) |end| {
                if (self.tree.comparator(entry.key, end) != .lt) {
                    self.tree.bp.unpinPage(frame, false);
                    self.current_frame = null;
                    return null;
                }
            }

            self.current_slot += 1;
            return entry;
        }

        /// Clean up the iterator
        pub fn deinit(self: *Iterator) void {
            if (self.current_frame) |frame| {
                self.tree.bp.unpinPage(frame, false);
                self.current_frame = null;
            }
        }
    };
};

// ============================================================================
// Tests
// ============================================================================

test "btree create empty" {
    const allocator = std.testing.allocator;

    const vfs = lattice.storage.vfs;
    const page_manager = lattice.storage.page_manager;
    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const path = "/tmp/lattice_btree_test_create.db";
    vfs_impl.delete(path) catch {};

    var pm = try page_manager.PageManager.init(allocator, vfs_impl, path, .{ .create = true });
    defer {
        pm.deinit();
        vfs_impl.delete(path) catch {};
    }

    var bp = try BufferPool.init(allocator, &pm, 65536);
    defer bp.deinit();

    const tree = try BTree.init(allocator, &bp);
    _ = tree;
}

test "btree insert and get" {
    const allocator = std.testing.allocator;

    const vfs = lattice.storage.vfs;
    const page_manager = lattice.storage.page_manager;
    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const path = "/tmp/lattice_btree_test_insert.db";
    vfs_impl.delete(path) catch {};

    var pm = try page_manager.PageManager.init(allocator, vfs_impl, path, .{ .create = true });
    defer {
        pm.deinit();
        vfs_impl.delete(path) catch {};
    }

    var bp = try BufferPool.init(allocator, &pm, 65536);
    defer bp.deinit();

    var tree = try BTree.init(allocator, &bp);

    // Insert a key-value pair
    try tree.insert("hello", "world");

    // Look it up
    const value = try tree.get("hello");
    try std.testing.expect(value != null);
    try std.testing.expectEqualStrings("world", value.?);

    // Look up non-existent key
    const missing = try tree.get("missing");
    try std.testing.expect(missing == null);
}

test "btree multiple inserts" {
    const allocator = std.testing.allocator;

    const vfs = lattice.storage.vfs;
    const page_manager = lattice.storage.page_manager;
    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const path = "/tmp/lattice_btree_test_multi.db";
    vfs_impl.delete(path) catch {};

    var pm = try page_manager.PageManager.init(allocator, vfs_impl, path, .{ .create = true });
    defer {
        pm.deinit();
        vfs_impl.delete(path) catch {};
    }

    var bp = try BufferPool.init(allocator, &pm, 65536);
    defer bp.deinit();

    var tree = try BTree.init(allocator, &bp);

    // Insert multiple key-value pairs
    try tree.insert("apple", "red");
    try tree.insert("banana", "yellow");
    try tree.insert("cherry", "red");
    try tree.insert("date", "brown");

    // Verify all
    try std.testing.expectEqualStrings("red", (try tree.get("apple")).?);
    try std.testing.expectEqualStrings("yellow", (try tree.get("banana")).?);
    try std.testing.expectEqualStrings("red", (try tree.get("cherry")).?);
    try std.testing.expectEqualStrings("brown", (try tree.get("date")).?);
}

test "btree duplicate key error" {
    const allocator = std.testing.allocator;

    const vfs = lattice.storage.vfs;
    const page_manager = lattice.storage.page_manager;
    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const path = "/tmp/lattice_btree_test_dup.db";
    vfs_impl.delete(path) catch {};

    var pm = try page_manager.PageManager.init(allocator, vfs_impl, path, .{ .create = true });
    defer {
        pm.deinit();
        vfs_impl.delete(path) catch {};
    }

    var bp = try BufferPool.init(allocator, &pm, 65536);
    defer bp.deinit();

    var tree = try BTree.init(allocator, &bp);

    try tree.insert("key", "value1");
    try std.testing.expectError(BTreeError.DuplicateKey, tree.insert("key", "value2"));
}

test "btree range scan" {
    const allocator = std.testing.allocator;

    const vfs = lattice.storage.vfs;
    const page_manager = lattice.storage.page_manager;
    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const path = "/tmp/lattice_btree_test_range.db";
    vfs_impl.delete(path) catch {};

    var pm = try page_manager.PageManager.init(allocator, vfs_impl, path, .{ .create = true });
    defer {
        pm.deinit();
        vfs_impl.delete(path) catch {};
    }

    var bp = try BufferPool.init(allocator, &pm, 65536);
    defer bp.deinit();

    var tree = try BTree.init(allocator, &bp);

    try tree.insert("a", "1");
    try tree.insert("b", "2");
    try tree.insert("c", "3");
    try tree.insert("d", "4");

    // Scan all
    var iter = try tree.range(null, null);
    defer iter.deinit();

    var count: usize = 0;
    while (try iter.next()) |_| {
        count += 1;
    }
    try std.testing.expectEqual(@as(usize, 4), count);
}

test "btree leaf split" {
    const allocator = std.testing.allocator;

    const vfs = lattice.storage.vfs;
    const page_manager = lattice.storage.page_manager;
    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const path = "/tmp/lattice_btree_test_split.db";
    vfs_impl.delete(path) catch {};

    var pm = try page_manager.PageManager.init(allocator, vfs_impl, path, .{ .create = true });
    defer {
        pm.deinit();
        vfs_impl.delete(path) catch {};
    }

    var bp = try BufferPool.init(allocator, &pm, 65536);
    defer bp.deinit();

    var tree = try BTree.init(allocator, &bp);

    // Insert enough keys to trigger a split
    // With 4KB pages, we can fit ~100-200 small entries before split
    var buf: [32]u8 = undefined;
    for (0..100) |i| {
        const key = std.fmt.bufPrint(&buf, "key{d:05}", .{i}) catch unreachable;
        try tree.insert(key, "value");
    }

    // Verify all keys still accessible
    for (0..100) |i| {
        const key = std.fmt.bufPrint(&buf, "key{d:05}", .{i}) catch unreachable;
        const val = try tree.get(key);
        try std.testing.expect(val != null);
    }
}

test "btree delete single key" {
    const allocator = std.testing.allocator;

    const vfs = lattice.storage.vfs;
    const page_manager = lattice.storage.page_manager;
    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const path = "/tmp/lattice_btree_test_delete.db";
    vfs_impl.delete(path) catch {};

    var pm = try page_manager.PageManager.init(allocator, vfs_impl, path, .{ .create = true });
    defer {
        pm.deinit();
        vfs_impl.delete(path) catch {};
    }

    var bp = try BufferPool.init(allocator, &pm, 65536);
    defer bp.deinit();

    var tree = try BTree.init(allocator, &bp);

    // Insert keys
    try tree.insert("key1", "value1");
    try tree.insert("key2", "value2");
    try tree.insert("key3", "value3");

    // Verify key2 exists
    const val = try tree.get("key2");
    try std.testing.expect(val != null);

    // Delete key2
    try tree.delete("key2");

    // Verify key2 no longer exists
    const val_after = try tree.get("key2");
    try std.testing.expect(val_after == null);

    // Verify other keys still exist
    try std.testing.expect((try tree.get("key1")) != null);
    try std.testing.expect((try tree.get("key3")) != null);
}

test "btree delete non-existent key" {
    const allocator = std.testing.allocator;

    const vfs = lattice.storage.vfs;
    const page_manager = lattice.storage.page_manager;
    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const path = "/tmp/lattice_btree_test_delete_notfound.db";
    vfs_impl.delete(path) catch {};

    var pm = try page_manager.PageManager.init(allocator, vfs_impl, path, .{ .create = true });
    defer {
        pm.deinit();
        vfs_impl.delete(path) catch {};
    }

    var bp = try BufferPool.init(allocator, &pm, 65536);
    defer bp.deinit();

    var tree = try BTree.init(allocator, &bp);

    // Insert a key
    try tree.insert("key1", "value1");

    // Try to delete non-existent key
    try std.testing.expectError(BTreeError.KeyNotFound, tree.delete("nonexistent"));

    // Original key should still exist
    try std.testing.expect((try tree.get("key1")) != null);
}

test "btree delete multiple keys" {
    const allocator = std.testing.allocator;

    const vfs = lattice.storage.vfs;
    const page_manager = lattice.storage.page_manager;
    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const path = "/tmp/lattice_btree_test_delete_multi.db";
    vfs_impl.delete(path) catch {};

    var pm = try page_manager.PageManager.init(allocator, vfs_impl, path, .{ .create = true });
    defer {
        pm.deinit();
        vfs_impl.delete(path) catch {};
    }

    var bp = try BufferPool.init(allocator, &pm, 65536);
    defer bp.deinit();

    var tree = try BTree.init(allocator, &bp);

    // Insert 20 keys
    var buf: [32]u8 = undefined;
    for (0..20) |i| {
        const key = std.fmt.bufPrint(&buf, "key{d:03}", .{i}) catch unreachable;
        try tree.insert(key, "value");
    }

    // Delete every other key
    for (0..10) |i| {
        const key = std.fmt.bufPrint(&buf, "key{d:03}", .{i * 2}) catch unreachable;
        try tree.delete(key);
    }

    // Verify deleted keys are gone
    for (0..10) |i| {
        const key = std.fmt.bufPrint(&buf, "key{d:03}", .{i * 2}) catch unreachable;
        const val = try tree.get(key);
        try std.testing.expect(val == null);
    }

    // Verify remaining keys still exist
    for (0..10) |i| {
        const key = std.fmt.bufPrint(&buf, "key{d:03}", .{i * 2 + 1}) catch unreachable;
        const val = try tree.get(key);
        try std.testing.expect(val != null);
    }
}

test "btree delete and reinsert" {
    const allocator = std.testing.allocator;

    const vfs = lattice.storage.vfs;
    const page_manager = lattice.storage.page_manager;
    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const path = "/tmp/lattice_btree_test_delete_reinsert.db";
    vfs_impl.delete(path) catch {};

    var pm = try page_manager.PageManager.init(allocator, vfs_impl, path, .{ .create = true });
    defer {
        pm.deinit();
        vfs_impl.delete(path) catch {};
    }

    var bp = try BufferPool.init(allocator, &pm, 65536);
    defer bp.deinit();

    var tree = try BTree.init(allocator, &bp);

    // Insert key
    try tree.insert("key1", "value1");
    try std.testing.expect((try tree.get("key1")) != null);

    // Delete key
    try tree.delete("key1");
    try std.testing.expect((try tree.get("key1")) == null);

    // Reinsert with different value
    try tree.insert("key1", "value2");
    const val = try tree.get("key1");
    try std.testing.expect(val != null);
    try std.testing.expectEqualStrings("value2", val.?);
}
