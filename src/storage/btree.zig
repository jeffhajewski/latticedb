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
    ValueTooLarge,
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

/// Leaf value length sentinel for generic B-tree overflow values.
const OVERFLOW_VALUE_LEN_SENTINEL: u16 = std.math.maxInt(u16);

/// Maximum serialized value accepted by the B-tree layer.
pub const MAX_VALUE_SIZE: usize = 64 * 1024 * 1024;

/// Keep a small prefix on the leaf page so scans can inspect a little local
/// value data without following the overflow chain in future callers.
const OVERFLOW_LOCAL_PREFIX_TARGET: usize = 64;

const OVERFLOW_DESCRIPTOR_MAGIC: u32 = 0x31564f4c; // "LOV1"
const OVERFLOW_PAGE_MAGIC: u32 = 0x31564f42; // "BOV1"
const OVERFLOW_DESCRIPTOR_FIXED_SIZE: usize = 28;
const OVERFLOW_PAGE_HEADER_SIZE: usize = @sizeOf(PageHeader) + 12;
const REBALANCE_OCCUPANCY_DIVISOR = 3;

/// Largest key a tree with this page size accepts.
///
/// A key can become a separator in an internal node, so internal nodes must
/// always be able to split into two non-empty halves around a promoted key.
/// Keeping every separator entry within a quarter of the internal payload
/// guarantees that: an overfull node then holds at least four entries, and a
/// byte-balanced split leaves each half at most five eighths full. It is 1012
/// bytes for 4 KB pages.
pub fn maxKeySize(page_size: u32) usize {
    if (page_size <= INTERNAL_SLOTS_OFFSET) return 0;
    const quarter = (@as(usize, page_size) - INTERNAL_SLOTS_OFFSET) / 4;
    if (quarter <= INTERNAL_SLOT_SIZE + 2) return 0;
    return quarter - INTERNAL_SLOT_SIZE - 2;
}

/// Key-value entry type returned by leaf operations
pub const Entry = struct {
    key: []const u8,
    value: []const u8,
};

const StoredEntry = struct {
    key: []const u8,
    value: []const u8,
    overflow: bool,
};

const StoredValue = struct {
    value: []const u8,
    overflow: bool,
};

const OverflowChain = struct {
    first_page: PageId,
    page_count: u32,
};

const OverflowDescriptor = struct {
    total_len: usize,
    first_page: PageId,
    page_count: u32,
    local_prefix_len: u16,
    flags: u16,
    checksum: u32,
    local_prefix: []const u8,

    fn storedLen(self: OverflowDescriptor) usize {
        return OVERFLOW_DESCRIPTOR_FIXED_SIZE + self.local_prefix_len;
    }
};

const PreparedStoredValue = struct {
    value: []const u8,
    overflow: bool,
    owns_value: bool,
    first_overflow_page: PageId,
    page_count: u32,

    fn deinit(self: *PreparedStoredValue, tree: *BTree) void {
        if (self.owns_value) {
            tree.allocator.free(@constCast(self.value));
            self.owns_value = false;
        }
    }

    fn rollback(self: *PreparedStoredValue, tree: *BTree) void {
        if (self.overflow and self.first_overflow_page != NULL_PAGE and self.page_count > 0) {
            tree.freeOverflowChain(self.first_overflow_page, self.page_count) catch {};
            self.first_overflow_page = NULL_PAGE;
            self.page_count = 0;
        }
    }
};

// ============================================================================
// Node Accessors - Work directly with page buffers
// ============================================================================

/// Internal node helper functions
pub const InternalNode = struct {
    /// Get number of keys in this internal node
    pub fn getNumKeys(buf: []const u8) u16 {
        return std.mem.readInt(u16, buf[@sizeOf(PageHeader)..][0..2], .little);
    }

    /// Set number of keys
    fn setNumKeys(buf: []u8, n: u16) void {
        std.mem.writeInt(u16, buf[@sizeOf(PageHeader)..][0..2], n, .little);
    }

    /// Get level (0 = just above leaves)
    pub fn getLevel(buf: []const u8) u16 {
        return std.mem.readInt(u16, buf[@sizeOf(PageHeader) + 2 ..][0..2], .little);
    }

    /// Set level
    fn setLevel(buf: []u8, level: u16) void {
        std.mem.writeInt(u16, buf[@sizeOf(PageHeader) + 2 ..][0..2], level, .little);
    }

    /// Get rightmost child pointer
    pub fn getRightChild(buf: []const u8) PageId {
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
    pub fn getChild(buf: []const u8, slot: u16) PageId {
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
    pub fn getKey(buf: []const u8, slot: u16) []const u8 {
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
pub const LeafNode = struct {
    /// Get number of entries
    pub fn getNumEntries(buf: []const u8) u16 {
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
    pub fn getNextLeaf(buf: []const u8) PageId {
        return std.mem.readInt(u32, buf[@sizeOf(PageHeader) + 4 ..][0..4], .little);
    }

    /// Set next leaf pointer
    fn setNextLeaf(buf: []u8, next: PageId) void {
        std.mem.writeInt(u32, buf[@sizeOf(PageHeader) + 4 ..][0..4], next, .little);
    }

    /// Get previous leaf pointer
    pub fn getPrevLeaf(buf: []const u8) PageId {
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
    pub fn getKey(buf: []const u8, slot: u16) []const u8 {
        const offset = getEntryOffset(buf, slot);
        const key_len = std.mem.readInt(u16, buf[offset..][0..2], .little);
        return buf[offset + 4 ..][0..key_len];
    }

    /// Get value at slot
    fn getStoredValue(buf: []const u8, slot: u16) StoredValue {
        const offset = getEntryOffset(buf, slot);
        const key_len = std.mem.readInt(u16, buf[offset..][0..2], .little);
        const val_len = std.mem.readInt(u16, buf[offset + 2 ..][0..2], .little);
        const value_offset = offset + 4 + key_len;
        if (val_len == OVERFLOW_VALUE_LEN_SENTINEL) {
            const desc = parseOverflowDescriptor(buf[value_offset..]) catch {
                return .{ .value = &[_]u8{}, .overflow = true };
            };
            return .{
                .value = buf[value_offset..][0..desc.storedLen()],
                .overflow = true,
            };
        }
        return .{
            .value = buf[value_offset..][0..val_len],
            .overflow = false,
        };
    }

    /// Get both key and value at slot
    fn getStoredEntry(buf: []const u8, slot: u16) StoredEntry {
        const offset = getEntryOffset(buf, slot);
        const key_len = std.mem.readInt(u16, buf[offset..][0..2], .little);
        const val_len = std.mem.readInt(u16, buf[offset + 2 ..][0..2], .little);
        const value_offset = offset + 4 + key_len;
        if (val_len == OVERFLOW_VALUE_LEN_SENTINEL) {
            const desc = parseOverflowDescriptor(buf[value_offset..]) catch {
                return .{
                    .key = buf[offset + 4 ..][0..key_len],
                    .value = &[_]u8{},
                    .overflow = true,
                };
            };
            return .{
                .key = buf[offset + 4 ..][0..key_len],
                .value = buf[value_offset..][0..desc.storedLen()],
                .overflow = true,
            };
        }
        return .{
            .key = buf[offset + 4 ..][0..key_len],
            .value = buf[value_offset..][0..val_len],
            .overflow = false,
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

fn overflowPagePayloadCapacity(page_size: u32) usize {
    if (page_size <= OVERFLOW_PAGE_HEADER_SIZE) return 0;
    return @as(usize, page_size) - OVERFLOW_PAGE_HEADER_SIZE;
}

fn parseOverflowDescriptor(bytes: []const u8) BTreeError!OverflowDescriptor {
    if (bytes.len < OVERFLOW_DESCRIPTOR_FIXED_SIZE) return BTreeError.InvalidPage;
    const magic = std.mem.readInt(u32, bytes[0..4], .little);
    if (magic != OVERFLOW_DESCRIPTOR_MAGIC) return BTreeError.InvalidPage;

    const total_len_u64 = std.mem.readInt(u64, bytes[4..12], .little);
    if (total_len_u64 > MAX_VALUE_SIZE) return BTreeError.ValueTooLarge;

    const first_page = std.mem.readInt(u32, bytes[12..16], .little);
    const page_count = std.mem.readInt(u32, bytes[16..20], .little);
    const local_prefix_len = std.mem.readInt(u16, bytes[20..22], .little);
    const flags = std.mem.readInt(u16, bytes[22..24], .little);
    const checksum = std.mem.readInt(u32, bytes[24..28], .little);
    const stored_len = OVERFLOW_DESCRIPTOR_FIXED_SIZE + @as(usize, local_prefix_len);
    if (bytes.len < stored_len) return BTreeError.InvalidPage;
    if (page_count == 0 and first_page != NULL_PAGE) return BTreeError.InvalidPage;
    if (page_count > 0 and first_page == NULL_PAGE) return BTreeError.InvalidPage;
    if (@as(u64, local_prefix_len) > total_len_u64) return BTreeError.InvalidPage;

    return .{
        .total_len = @intCast(total_len_u64),
        .first_page = first_page,
        .page_count = page_count,
        .local_prefix_len = local_prefix_len,
        .flags = flags,
        .checksum = checksum,
        .local_prefix = bytes[OVERFLOW_DESCRIPTOR_FIXED_SIZE..stored_len],
    };
}

fn writeOverflowDescriptor(
    buf: []u8,
    total_len: usize,
    first_page: PageId,
    page_count: u32,
    local_prefix: []const u8,
    checksum: u32,
) BTreeError!void {
    if (buf.len != OVERFLOW_DESCRIPTOR_FIXED_SIZE + local_prefix.len) return BTreeError.InvalidPage;
    if (total_len > MAX_VALUE_SIZE) return BTreeError.ValueTooLarge;
    if (local_prefix.len > std.math.maxInt(u16)) return BTreeError.PageFull;

    std.mem.writeInt(u32, buf[0..4], OVERFLOW_DESCRIPTOR_MAGIC, .little);
    std.mem.writeInt(u64, buf[4..12], @intCast(total_len), .little);
    std.mem.writeInt(u32, buf[12..16], first_page, .little);
    std.mem.writeInt(u32, buf[16..20], page_count, .little);
    std.mem.writeInt(u16, buf[20..22], @intCast(local_prefix.len), .little);
    std.mem.writeInt(u16, buf[22..24], 0, .little);
    std.mem.writeInt(u32, buf[24..28], checksum, .little);
    if (local_prefix.len > 0) {
        @memcpy(buf[OVERFLOW_DESCRIPTOR_FIXED_SIZE..], local_prefix);
    }
}

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

    /// Look up a key and return an owned copy of its value (if found).
    /// Caller must free the returned slice with freeValue().
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
                    const stored = LeafNode.getStoredValue(frame.data, result.slot);
                    return self.materializeStoredValue(stored) catch |err| return err;
                }
                return null;
            }

            // Internal node - find child to descend into
            page_id = self.findChild(frame.data, key);
        }
    }

    /// Check whether a key exists without materializing its value.
    pub fn contains(self: *Self, key: []const u8) BTreeError!bool {
        var page_id = self.root_page;

        while (true) {
            const frame = self.bp.fetchPage(page_id, .shared) catch return BTreeError.BufferPoolFull;
            defer self.bp.unpinPage(frame, false);

            const header: *const PageHeader = @ptrCast(@alignCast(frame.data.ptr));
            if (header.page_type == .btree_leaf) {
                return LeafNode.searchSlot(frame.data, key, self.comparator).found;
            }

            page_id = self.findChild(frame.data, key);
        }
    }

    /// Free a value returned by get().
    pub fn freeValue(self: *Self, value: []const u8) void {
        self.allocator.free(@constCast(value));
    }

    fn materializeStoredValue(self: *Self, stored: StoredValue) BTreeError![]u8 {
        if (!stored.overflow) {
            return self.allocator.dupe(u8, stored.value) catch return BTreeError.OutOfMemory;
        }

        const desc = try parseOverflowDescriptor(stored.value);
        const result = self.allocator.alloc(u8, desc.total_len) catch return BTreeError.OutOfMemory;
        errdefer self.allocator.free(result);

        if (desc.local_prefix_len > 0) {
            @memcpy(result[0..desc.local_prefix_len], desc.local_prefix);
        }

        var written: usize = desc.local_prefix_len;
        if (desc.page_count > 0) {
            try self.readOverflowChain(desc.first_page, desc.page_count, result[written..]);
            written = desc.total_len;
        }

        if (written != desc.total_len) return BTreeError.InvalidPage;
        if (page.calculateChecksum(result) != desc.checksum) return BTreeError.InvalidPage;
        return result;
    }

    fn readOverflowChain(
        self: *Self,
        first_page: PageId,
        page_count: u32,
        dest: []u8,
    ) BTreeError!void {
        if (page_count == 0) {
            if (dest.len != 0) return BTreeError.InvalidPage;
            return;
        }

        const capacity = overflowPagePayloadCapacity(self.page_size);
        if (capacity == 0) return BTreeError.InvalidPage;

        var page_id = first_page;
        var remaining = dest;
        var pages_read: u32 = 0;
        while (pages_read < page_count) : (pages_read += 1) {
            if (page_id == NULL_PAGE) return BTreeError.InvalidPage;

            const frame = self.bp.fetchPage(page_id, .shared) catch return BTreeError.BufferPoolFull;

            const header: *const PageHeader = @ptrCast(@alignCast(frame.data.ptr));
            if (header.page_type != .overflow) {
                self.bp.unpinPage(frame, false);
                return BTreeError.InvalidPage;
            }
            const magic = std.mem.readInt(u32, frame.data[@sizeOf(PageHeader)..][0..4], .little);
            if (magic != OVERFLOW_PAGE_MAGIC) {
                self.bp.unpinPage(frame, false);
                return BTreeError.InvalidPage;
            }

            const next_page = std.mem.readInt(u32, frame.data[@sizeOf(PageHeader) + 4 ..][0..4], .little);
            const chunk_len = std.mem.readInt(u16, frame.data[@sizeOf(PageHeader) + 8 ..][0..2], .little);
            if (chunk_len > capacity or chunk_len > remaining.len) {
                self.bp.unpinPage(frame, false);
                return BTreeError.InvalidPage;
            }

            if (chunk_len > 0) {
                @memcpy(remaining[0..chunk_len], frame.data[OVERFLOW_PAGE_HEADER_SIZE..][0..chunk_len]);
                remaining = remaining[chunk_len..];
            }

            page_id = next_page;
            self.bp.unpinPage(frame, false);
        }

        if (remaining.len != 0) return BTreeError.InvalidPage;
        if (page_id != NULL_PAGE) return BTreeError.InvalidPage;
    }

    fn prepareStoredValue(self: *Self, key: []const u8, value: []const u8) BTreeError!PreparedStoredValue {
        if (key.len > std.math.maxInt(u16)) return BTreeError.PageFull;
        if (value.len > MAX_VALUE_SIZE) return BTreeError.ValueTooLarge;

        if (value.len < OVERFLOW_VALUE_LEN_SENTINEL) {
            const inline_size = storedLeafEntrySize(key, value) orelse return BTreeError.PageFull;
            if (inline_size <= maxLeafEntrySize(self.page_size)) {
                const inline_space = storedLeafEntrySpace(key, value) orelse return BTreeError.PageFull;
                // Keep inline entries small enough that any full leaf plus
                // one new entry can always be split into two ordered leaves.
                const split_safe_inline_limit = leafPagePayloadCapacity(self.page_size) / 2;
                if (inline_space <= split_safe_inline_limit or self.chooseOverflowLocalPrefixLen(key, value.len) == null) {
                    return .{
                        .value = value,
                        .overflow = false,
                        .owns_value = false,
                        .first_overflow_page = NULL_PAGE,
                        .page_count = 0,
                    };
                }
            }
        }

        const prefix_len = self.chooseOverflowLocalPrefixLen(key, value.len) orelse {
            return BTreeError.PageFull;
        };
        const remaining_len = value.len - prefix_len;
        const capacity = overflowPagePayloadCapacity(self.page_size);
        if (remaining_len > 0 and capacity == 0) return BTreeError.PageFull;

        const page_count_usize = if (remaining_len == 0)
            0
        else
            (remaining_len + capacity - 1) / capacity;
        if (page_count_usize > std.math.maxInt(u32)) return BTreeError.ValueTooLarge;

        const chain = if (remaining_len > 0)
            try self.writeOverflowChain(value[prefix_len..], @intCast(page_count_usize))
        else
            OverflowChain{ .first_page = NULL_PAGE, .page_count = 0 };
        errdefer if (chain.first_page != NULL_PAGE and chain.page_count > 0) {
            self.freeOverflowChain(chain.first_page, chain.page_count) catch {};
        };

        const desc_len = OVERFLOW_DESCRIPTOR_FIXED_SIZE + prefix_len;
        const desc = self.allocator.alloc(u8, desc_len) catch return BTreeError.OutOfMemory;
        errdefer self.allocator.free(desc);

        try writeOverflowDescriptor(
            desc,
            value.len,
            chain.first_page,
            chain.page_count,
            value[0..prefix_len],
            page.calculateChecksum(value),
        );

        return .{
            .value = desc,
            .overflow = true,
            .owns_value = true,
            .first_overflow_page = chain.first_page,
            .page_count = chain.page_count,
        };
    }

    fn chooseOverflowLocalPrefixLen(self: *const Self, key: []const u8, value_len: usize) ?usize {
        if (key.len > std.math.maxInt(u16)) return null;
        const fixed_entry_size = std.math.add(usize, 4 + OVERFLOW_DESCRIPTOR_FIXED_SIZE, key.len) catch return null;
        const max_entry = maxLeafEntrySize(self.page_size);
        if (fixed_entry_size > max_entry) return null;
        const available = max_entry - fixed_entry_size;
        return @min(@min(OVERFLOW_LOCAL_PREFIX_TARGET, value_len), available);
    }

    fn writeOverflowChain(
        self: *Self,
        bytes: []const u8,
        page_count: u32,
    ) BTreeError!OverflowChain {
        if (page_count == 0) return .{ .first_page = NULL_PAGE, .page_count = 0 };
        const capacity = overflowPagePayloadCapacity(self.page_size);
        if (capacity == 0) return BTreeError.PageFull;

        const pages = self.allocator.alloc(PageId, page_count) catch return BTreeError.OutOfMemory;
        defer self.allocator.free(pages);

        var allocated: usize = 0;
        errdefer {
            for (pages[0..allocated]) |page_id| {
                self.freeSingleOverflowPage(page_id) catch {};
            }
        }

        for (pages) |*page_id| {
            page_id.* = self.bp.pm.allocatePage() catch return BTreeError.IoError;
            allocated += 1;
        }

        var offset: usize = 0;
        for (pages, 0..) |page_id, i| {
            const next_page = if (i + 1 < pages.len) pages[i + 1] else NULL_PAGE;
            const chunk_len = @min(capacity, bytes.len - offset);

            const frame = self.bp.fetchPage(page_id, .exclusive) catch return BTreeError.BufferPoolFull;
            const header: *PageHeader = @ptrCast(@alignCast(frame.data.ptr));
            header.* = PageHeader.init(.overflow);
            std.mem.writeInt(u32, frame.data[@sizeOf(PageHeader)..][0..4], OVERFLOW_PAGE_MAGIC, .little);
            std.mem.writeInt(u32, frame.data[@sizeOf(PageHeader) + 4 ..][0..4], next_page, .little);
            std.mem.writeInt(u16, frame.data[@sizeOf(PageHeader) + 8 ..][0..2], @intCast(chunk_len), .little);
            std.mem.writeInt(u16, frame.data[@sizeOf(PageHeader) + 10 ..][0..2], 0, .little);
            if (chunk_len > 0) {
                @memcpy(frame.data[OVERFLOW_PAGE_HEADER_SIZE..][0..chunk_len], bytes[offset..][0..chunk_len]);
            }
            if (OVERFLOW_PAGE_HEADER_SIZE + chunk_len < frame.data.len) {
                @memset(frame.data[OVERFLOW_PAGE_HEADER_SIZE + chunk_len ..], 0);
            }
            self.bp.unpinPage(frame, true);
            offset += chunk_len;
        }

        return .{ .first_page = pages[0], .page_count = page_count };
    }

    fn freeOverflowChain(self: *Self, first_page: PageId, page_count: u32) BTreeError!void {
        var page_id = first_page;
        var freed: u32 = 0;
        while (freed < page_count) : (freed += 1) {
            if (page_id == NULL_PAGE) return BTreeError.InvalidPage;

            const frame = self.bp.fetchPage(page_id, .exclusive) catch return BTreeError.BufferPoolFull;
            const header: *const PageHeader = @ptrCast(@alignCast(frame.data.ptr));
            if (header.page_type != .overflow) {
                self.bp.unpinPage(frame, false);
                return BTreeError.InvalidPage;
            }

            const magic = std.mem.readInt(u32, frame.data[@sizeOf(PageHeader)..][0..4], .little);
            if (magic != OVERFLOW_PAGE_MAGIC) {
                self.bp.unpinPage(frame, false);
                return BTreeError.InvalidPage;
            }

            const next_page = std.mem.readInt(u32, frame.data[@sizeOf(PageHeader) + 4 ..][0..4], .little);
            try self.markPageFreeInPlace(frame);
            page_id = next_page;
        }
        if (page_id != NULL_PAGE) return BTreeError.InvalidPage;
    }

    fn freeSingleOverflowPage(self: *Self, page_id: PageId) BTreeError!void {
        const frame = self.bp.fetchPage(page_id, .exclusive) catch return BTreeError.BufferPoolFull;
        try self.markPageFreeInPlace(frame);
    }

    fn markPageFreeInPlace(self: *Self, frame: *BufferFrame) BTreeError!void {
        var header_copy = self.bp.pm.getHeader().*;
        const old_freelist_head = header_copy.freelist_page;
        const page_id = frame.page_id;

        @memset(frame.data, 0);
        const header: *PageHeader = @ptrCast(@alignCast(frame.data.ptr));
        header.* = PageHeader.init(.free);
        std.mem.writeInt(u32, frame.data[@sizeOf(PageHeader)..][0..4], old_freelist_head, .little);

        self.bp.pm.writePage(page_id, frame.data) catch {
            self.bp.unpinPage(frame, false);
            return BTreeError.IoError;
        };
        frame.dirty = false;
        self.bp.unpinPage(frame, false);

        header_copy.freelist_page = page_id;
        self.bp.pm.updateHeader(&header_copy) catch return BTreeError.IoError;
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
        if (key.len > maxKeySize(self.page_size)) return BTreeError.PageFull;

        // Try simple insert first
        const result = try self.insertRecursive(self.root_page, key, value);

        // If root split, create new root
        if (result.split) |split_info| {
            defer self.allocator.free(@constCast(split_info.key));
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

        var prepared = self.prepareStoredValue(key, value) catch |err| {
            self.bp.unpinPage(frame, false);
            return err;
        };
        defer prepared.deinit(self);
        var overflow_published = false;
        errdefer if (!overflow_published) prepared.rollback(self);

        // Calculate space needed: slot(2) + key_len(2) + val_len(2) + key + value.
        // Use usize for the math so comically large values do not wrap u16
        // before we compare against the per-page upper bound; a key/value
        // pair that cannot physically fit in an empty page is rejected
        // with `PageFull` instead of panicking inside `insertLeafEntry`.
        const entry_size_usize = storedLeafEntrySize(key, prepared.value) orelse {
            self.bp.unpinPage(frame, false);
            return BTreeError.PageFull;
        };
        const max_leaf_entry = maxLeafEntrySize(self.page_size);
        if (entry_size_usize > max_leaf_entry) {
            self.bp.unpinPage(frame, false);
            return BTreeError.PageFull;
        }

        const space_needed_usize = std.math.add(usize, LEAF_SLOT_SIZE, entry_size_usize) catch {
            self.bp.unpinPage(frame, false);
            return BTreeError.PageFull;
        };
        const space_needed: u16 = @intCast(space_needed_usize);
        const free_space = LeafNode.getFreeSpace(buf, self.page_size);

        if (free_space >= space_needed) {
            // Fits in current page
            self.insertLeafEntryStored(buf, search.slot, key, prepared.value, prepared.overflow) catch |err| {
                self.bp.unpinPage(frame, false);
                return err;
            };
            overflow_published = true;
            self.bp.unpinPage(frame, true);
            return .{ .page_id = page_id, .split = null };
        }

        // Need to split
        const result = try self.splitLeafAndInsert(
            frame,
            search.slot,
            .{ .key = key, .value = prepared.value, .overflow = prepared.overflow },
            &overflow_published,
        );
        return result;
    }

    /// Maximum (key_len + value_len + per-entry framing) that a leaf page
    /// can hold. An entry larger than this cannot fit even into a freshly
    /// initialized leaf, so it must be rejected up-front. Per-entry
    /// framing = key_len(2) + val_len(2) = 4 bytes, already included here.
    fn maxLeafEntrySize(page_size: u32) usize {
        const reserved = @sizeOf(PageHeader) + LEAF_HEADER_SIZE + LEAF_SLOT_SIZE;
        if (page_size <= reserved) return 0;
        return @as(usize, page_size) - reserved;
    }

    fn leafPagePayloadCapacity(page_size: u32) usize {
        if (page_size <= LEAF_SLOTS_OFFSET) return 0;
        return @as(usize, page_size) - LEAF_SLOTS_OFFSET;
    }

    fn storedLeafEntrySize(key: []const u8, value: []const u8) ?usize {
        if (key.len > std.math.maxInt(u16)) return null;
        if (value.len > std.math.maxInt(u16)) return null;
        const with_key = std.math.add(usize, 4, key.len) catch return null;
        return std.math.add(usize, with_key, value.len) catch null;
    }

    fn storedLeafEntrySpace(key: []const u8, value: []const u8) ?usize {
        const entry_size = storedLeafEntrySize(key, value) orelse return null;
        return std.math.add(usize, LEAF_SLOT_SIZE, entry_size) catch null;
    }

    /// Check whether a (key, value) pair could theoretically fit into a
    /// single empty leaf page of the tree. Callers perform this before a
    /// delete+insert sequence so they can bail out with a clean error
    /// instead of leaving the tree missing an entry.
    pub fn canFitLeafEntry(self: *const Self, key: []const u8, value_len: usize) bool {
        if (key.len > maxKeySize(self.page_size) or value_len > MAX_VALUE_SIZE) return false;
        if (value_len < OVERFLOW_VALUE_LEN_SENTINEL) {
            const entry_size = std.math.add(usize, 4, key.len) catch return false;
            const entry_size_with_value = std.math.add(usize, entry_size, value_len) catch return false;
            if (entry_size_with_value <= maxLeafEntrySize(self.page_size)) return true;
        }
        return self.chooseOverflowLocalPrefixLen(key, value_len) != null;
    }

    fn insertLeafEntryStored(
        self: *Self,
        buf: []u8,
        slot: u16,
        key: []const u8,
        value: []const u8,
        overflow: bool,
    ) BTreeError!void {
        const num_entries = LeafNode.getNumEntries(buf);
        const entry_size_usize = storedLeafEntrySize(key, value) orelse return BTreeError.PageFull;
        const space_needed_usize = std.math.add(usize, LEAF_SLOT_SIZE, entry_size_usize) catch return BTreeError.PageFull;

        if (entry_size_usize > std.math.maxInt(u16) or space_needed_usize > std.math.maxInt(u16)) {
            return BTreeError.PageFull;
        }

        const space_needed: u16 = @intCast(space_needed_usize);
        if (LeafNode.getFreeSpace(buf, self.page_size) < space_needed) {
            return BTreeError.PageFull;
        }

        // Find where to write entry data (at end of page, growing backward)
        var min_offset: u16 = @intCast(self.page_size);
        for (0..num_entries) |i| {
            const offset = LeafNode.getEntryOffset(buf, @intCast(i));
            if (offset < min_offset) {
                min_offset = offset;
            }
        }

        // Write entry at new position
        const entry_size: u16 = @intCast(entry_size_usize);
        const new_offset = min_offset - entry_size;

        std.mem.writeInt(u16, buf[new_offset..][0..2], @intCast(key.len), .little);
        std.mem.writeInt(
            u16,
            buf[new_offset + 2 ..][0..2],
            if (overflow) OVERFLOW_VALUE_LEN_SENTINEL else @as(u16, @intCast(value.len)),
            .little,
        );
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

    fn chooseLeafSplitPoint(self: *Self, entries: []const StoredEntry) BTreeError!u16 {
        if (entries.len < 2 or entries.len > std.math.maxInt(u16)) {
            return BTreeError.PageFull;
        }

        const capacity = leafPagePayloadCapacity(self.page_size);
        var total_space: usize = 0;
        for (entries) |entry| {
            const entry_space = storedLeafEntrySpace(entry.key, entry.value) orelse return BTreeError.PageFull;
            if (entry_space > capacity) return BTreeError.PageFull;
            total_space = std.math.add(usize, total_space, entry_space) catch return BTreeError.PageFull;
        }

        var left_space: usize = 0;
        var best_split: ?usize = null;
        var best_imbalance: usize = std.math.maxInt(usize);
        for (entries[0 .. entries.len - 1], 0..) |entry, i| {
            const entry_space = storedLeafEntrySpace(entry.key, entry.value) orelse return BTreeError.PageFull;
            left_space += entry_space;
            const right_space = total_space - left_space;
            if (left_space <= capacity and right_space <= capacity) {
                const imbalance = if (left_space >= right_space)
                    left_space - right_space
                else
                    right_space - left_space;
                if (imbalance < best_imbalance) {
                    best_imbalance = imbalance;
                    best_split = i + 1;
                }
            }
        }

        return @intCast(best_split orelse return BTreeError.PageFull);
    }

    fn splitLeafAndInsert(
        self: *Self,
        frame: *BufferFrame,
        slot: u16,
        new_entry: StoredEntry,
        overflow_published: *bool,
    ) BTreeError!InsertResult {
        const old_buf = frame.data;
        const old_page_id = frame.page_id;

        // Get all entries including the new one, sorted
        const num_entries = LeafNode.getNumEntries(old_buf);
        const total = num_entries + 1;

        // IMPORTANT: Read sibling pointers BEFORE reinitializing old_buf
        const old_next = LeafNode.getNextLeaf(old_buf);
        const old_prev = LeafNode.getPrevLeaf(old_buf);

        // Calculate total size needed for key/value data copies
        var total_data_size: usize = 0;
        for (0..num_entries) |i| {
            const entry = LeafNode.getStoredEntry(old_buf, @intCast(i));
            total_data_size += entry.key.len + entry.value.len;
        }

        // Allocate temporary storage for entries and their data
        // We must copy entry data because entries point into old_buf which we reinitialize
        var entries = self.allocator.alloc(StoredEntry, total) catch {
            self.bp.unpinPage(frame, false);
            return BTreeError.OutOfMemory;
        };
        defer self.allocator.free(entries);

        var data_buffer = self.allocator.alloc(u8, total_data_size) catch {
            self.bp.unpinPage(frame, false);
            return BTreeError.OutOfMemory;
        };
        defer self.allocator.free(data_buffer);

        // Collect existing entries, copying their data to avoid aliasing
        var data_offset: usize = 0;
        var j: usize = 0;
        for (0..num_entries) |i| {
            if (i == slot) {
                // New entry doesn't need copying - it comes from outside old_buf
                entries[j] = new_entry;
                j += 1;
            }
            const entry = LeafNode.getStoredEntry(old_buf, @intCast(i));
            // Copy key
            const key_copy = data_buffer[data_offset..][0..entry.key.len];
            @memcpy(key_copy, entry.key);
            data_offset += entry.key.len;
            // Copy value
            const value_copy = data_buffer[data_offset..][0..entry.value.len];
            @memcpy(value_copy, entry.value);
            data_offset += entry.value.len;
            entries[j] = .{ .key = key_copy, .value = value_copy, .overflow = entry.overflow };
            j += 1;
        }
        if (slot == num_entries) {
            entries[j] = new_entry;
        }

        const split_point = self.chooseLeafSplitPoint(entries) catch |err| {
            self.bp.unpinPage(frame, false);
            return err;
        };

        // Allocate new leaf page after validating the split so allocation
        // failures cannot leave the original page partially rewritten.
        const new_page_id = self.bp.pm.allocatePage() catch {
            self.bp.unpinPage(frame, false);
            return BTreeError.IoError;
        };
        const new_frame = self.bp.fetchPage(new_page_id, .exclusive) catch {
            self.bp.pm.freePage(new_page_id) catch {};
            self.bp.unpinPage(frame, false);
            return BTreeError.BufferPoolFull;
        };
        const new_buf = new_frame.data;
        var old_frame_pinned = true;
        var new_frame_pinned = true;
        var split_pages_dirty = false;
        errdefer {
            if (new_frame_pinned) self.bp.unpinPage(new_frame, split_pages_dirty);
            if (old_frame_pinned) self.bp.unpinPage(frame, split_pages_dirty);
        }

        // Initialize new leaf
        LeafNode.init(new_buf);
        split_pages_dirty = true;

        // Reinitialize old leaf - now safe because we copied all data
        LeafNode.init(old_buf);

        // Write first half to old page
        for (0..split_point) |i| {
            try self.insertLeafEntryStored(old_buf, @intCast(i), entries[i].key, entries[i].value, entries[i].overflow);
        }

        // Write second half to new page
        for (split_point..total) |i| {
            try self.insertLeafEntryStored(new_buf, @intCast(i - split_point), entries[i].key, entries[i].value, entries[i].overflow);
        }

        overflow_published.* = true;

        // Update sibling pointers
        LeafNode.setNextLeaf(old_buf, new_page_id);
        LeafNode.setPrevLeaf(old_buf, old_prev);
        LeafNode.setPrevLeaf(new_buf, old_page_id);
        LeafNode.setNextLeaf(new_buf, old_next);

        // Update old_next's prev pointer if it exists
        if (old_next != NULL_PAGE) {
            const next_frame = self.bp.fetchPage(old_next, .exclusive) catch return BTreeError.BufferPoolFull;
            LeafNode.setPrevLeaf(next_frame.data, new_page_id);
            self.bp.unpinPage(next_frame, true);
        }

        // Copy split key for caller (first key of new page)
        const split_key = LeafNode.getKey(new_buf, 0);
        const split_key_copy = self.allocator.alloc(u8, split_key.len) catch return BTreeError.OutOfMemory;
        @memcpy(split_key_copy, split_key);

        self.bp.unpinPage(new_frame, true);
        new_frame_pinned = false;
        self.bp.unpinPage(frame, true);
        old_frame_pinned = false;

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
        const space_needed: usize = INTERNAL_SLOT_SIZE + 2 + key.len; // slot + key_len(2) + key
        const free_space = InternalNode.getFreeSpace(buf, self.page_size);

        if (free_space >= space_needed) {
            // Fits in current page
            self.insertInternalEntry(buf, insert_pos, key, new_child) catch |err| {
                self.bp.unpinPage(frame, false);
                return err;
            };
            self.bp.unpinPage(frame, true);
            return .{ .page_id = page_id, .split = null };
        }

        // Need to split internal node
        return self.splitInternalAndInsert(frame, insert_pos, key, new_child);
    }

    fn insertInternalEntry(self: *Self, buf: []u8, slot: u16, key: []const u8, child: PageId) BTreeError!void {
        const num_keys = InternalNode.getNumKeys(buf);

        // Find where to write key data
        var min_offset: usize = self.page_size;
        for (0..num_keys) |i| {
            const offset = InternalNode.getKeyOffset(buf, @intCast(i));
            if (offset < min_offset) {
                min_offset = offset;
            }
        }

        // Refuse rather than write outside the page or over the slot array.
        const key_size = 2 + key.len;
        const grown_slots_end = INTERNAL_SLOTS_OFFSET + (@as(usize, num_keys) + 1) * INTERNAL_SLOT_SIZE;
        if (key.len > std.math.maxInt(u16) or min_offset < grown_slots_end + key_size) return BTreeError.PageFull;

        // Write key
        const new_offset: u16 = @intCast(min_offset - key_size);
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
        const old_buf = frame.data;
        const old_page_id = frame.page_id;
        var old_frame_pinned = true;
        var old_frame_dirty = false;
        errdefer if (old_frame_pinned) self.bp.unpinPage(frame, old_frame_dirty);

        // Build the combined key/child image and choose the promotion point
        // by bytes before touching any page. Splitting by key count can put
        // more bytes on one side than a page holds once keys are long.
        var image = try self.internalImageInserting(old_buf, slot, key, new_child);
        defer image.deinit(self.allocator);
        const promotion = self.chooseInternalPromotion(image.keys) orelse return BTreeError.PageFull;

        const promoted_key_copy = self.allocator.dupe(u8, image.keys[promotion]) catch return BTreeError.OutOfMemory;
        errdefer self.allocator.free(promoted_key_copy);

        // Allocate new internal page
        const new_page_id = self.bp.pm.allocatePage() catch return BTreeError.IoError;
        const new_frame = self.bp.fetchPage(new_page_id, .exclusive) catch {
            self.bp.pm.freePage(new_page_id) catch {};
            return BTreeError.BufferPoolFull;
        };
        var new_frame_pinned = true;
        var new_frame_dirty = false;
        errdefer if (new_frame_pinned) self.bp.unpinPage(new_frame, new_frame_dirty);

        // Old page keeps keys[0..promotion] and children[0..promotion]; the
        // promoted key moves up; the new page takes the rest.
        new_frame_dirty = true;
        try self.writeInternalParts(
            new_frame.data,
            image.level,
            image.keys[promotion + 1 ..],
            image.children[promotion + 1 ..],
        );
        old_frame_dirty = true;
        try self.writeInternalParts(
            old_buf,
            image.level,
            image.keys[0..promotion],
            image.children[0 .. promotion + 1],
        );

        self.bp.unpinPage(new_frame, true);
        new_frame_pinned = false;
        self.bp.unpinPage(frame, true);
        old_frame_pinned = false;

        return .{
            .page_id = old_page_id,
            .split = .{
                .key = promoted_key_copy,
                .new_page = new_page_id,
            },
        };
    }

    /// Write a key/child slot into an internal node being built in order.
    /// Fails rather than writing outside the page if the key does not fit.
    fn insertInternalEntryDirect(self: *Self, buf: []u8, slot: u16, key: []const u8, child: PageId) BTreeError!void {
        const num_keys = InternalNode.getNumKeys(buf);

        // Find where to write key data
        var min_offset: usize = self.page_size;
        for (0..num_keys) |i| {
            const offset = InternalNode.getKeyOffset(buf, @intCast(i));
            if (offset < min_offset) {
                min_offset = offset;
            }
        }

        const key_size = 2 + key.len;
        const grown_slots_end = INTERNAL_SLOTS_OFFSET + (@as(usize, num_keys) + 1) * INTERNAL_SLOT_SIZE;
        if (key.len > std.math.maxInt(u16) or min_offset < grown_slots_end + key_size) return BTreeError.PageFull;

        // Write key
        const new_offset: u16 = @intCast(min_offset - key_size);
        std.mem.writeInt(u16, buf[new_offset..][0..2], @intCast(key.len), .little);
        @memcpy(buf[new_offset + 2 ..][0..key.len], key);

        // Set the slot (no shifting needed when building sequentially)
        InternalNode.setSlot(buf, slot, new_offset, child);
        InternalNode.setNumKeys(buf, num_keys + 1);
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
        self.insertInternalEntry(frame.data, 0, split_key, right_child) catch |err| {
            self.bp.unpinPage(frame, false);
            self.bp.pm.freePage(new_root) catch {};
            return err;
        };

        self.bp.unpinPage(frame, true);
        self.root_page = new_root;
    }

    // ========================================================================
    // Delete
    // ========================================================================

    /// Delete a key from the tree
    /// Returns KeyNotFound if the key doesn't exist
    pub fn delete(self: *Self, key: []const u8) BTreeError!void {
        const path = try self.findDeletePath(key);
        const leaf_page = path.leaf;

        // Fetch the leaf with exclusive latch
        const frame = self.bp.fetchPage(leaf_page, .exclusive) catch return BTreeError.BufferPoolFull;
        var frame_pinned = true;
        errdefer if (frame_pinned) self.bp.unpinPage(frame, false);

        // Find the key in the leaf
        const search = LeafNode.searchSlot(frame.data, key, self.comparator);
        if (!search.found) {
            self.bp.unpinPage(frame, false);
            frame_pinned = false;
            return BTreeError.KeyNotFound;
        }

        // Remove the entry from the leaf
        try self.deleteLeafEntry(frame.data, search.slot);

        self.bp.unpinPage(frame, true);
        frame_pinned = false;

        // A failed merge is only a missed space-reclamation opportunity: the
        // key has already been removed and the tree remains valid. Keep delete
        // semantics deterministic even under temporary memory or cache pressure.
        self.rebalanceAfterDelete(leaf_page, &path) catch {};
    }

    const MAX_TREE_HEIGHT = 64;

    const DeletePath = struct {
        leaf: PageId,
        ancestors: [MAX_TREE_HEIGHT]PageId,
        len: usize,
    };

    fn findDeletePath(self: *Self, key: []const u8) BTreeError!DeletePath {
        var page_id = self.root_page;
        var path = DeletePath{
            .leaf = NULL_PAGE,
            .ancestors = undefined,
            .len = 0,
        };

        while (true) {
            const frame = self.bp.fetchPage(page_id, .shared) catch return BTreeError.BufferPoolFull;
            defer self.bp.unpinPage(frame, false);

            const header: *const PageHeader = @ptrCast(@alignCast(frame.data.ptr));
            if (header.page_type == .btree_leaf) {
                path.leaf = page_id;
                return path;
            }
            if (header.page_type != .btree_internal) return BTreeError.InvalidPage;
            if (path.len == MAX_TREE_HEIGHT) return BTreeError.InvalidPage;

            path.ancestors[path.len] = page_id;
            path.len += 1;
            page_id = self.findChild(frame.data, key);
        }
    }

    fn internalChildAt(buf: []const u8, index: u16) PageId {
        const num_keys = InternalNode.getNumKeys(buf);
        return if (index < num_keys)
            InternalNode.getChild(buf, index)
        else
            InternalNode.getRightChild(buf);
    }

    fn storedLeafSpace(buf: []const u8) usize {
        var total: usize = 0;
        const count = LeafNode.getNumEntries(buf);
        for (0..count) |i| {
            const entry = LeafNode.getStoredEntry(buf, @intCast(i));
            total += storedLeafEntrySpace(entry.key, entry.value) orelse return std.math.maxInt(usize);
        }
        return total;
    }

    const InternalImage = struct {
        key_data: []u8,
        keys: [][]const u8,
        children: []PageId,
        level: u16,

        fn deinit(self: *InternalImage, allocator: Allocator) void {
            allocator.free(self.children);
            allocator.free(self.keys);
            allocator.free(self.key_data);
        }
    };

    fn copyKeyIntoImage(
        source: []const u8,
        key_data: []u8,
        key_offset: *usize,
        keys: [][]const u8,
        key_index: *usize,
    ) void {
        const copy = key_data[key_offset.*..][0..source.len];
        @memcpy(copy, source);
        keys[key_index.*] = copy;
        key_offset.* += source.len;
        key_index.* += 1;
    }

    fn internalImageWithoutSeparator(
        self: *Self,
        buf: []const u8,
        separator_index: u16,
    ) BTreeError!InternalImage {
        const num_keys = InternalNode.getNumKeys(buf);
        if (separator_index >= num_keys) return BTreeError.InvalidPage;

        const new_key_count = num_keys - 1;
        var key_bytes_len: usize = 0;
        for (0..num_keys) |key_index| {
            if (key_index == separator_index) continue;
            key_bytes_len += InternalNode.getKey(buf, @intCast(key_index)).len;
        }

        const key_data = self.allocator.alloc(u8, key_bytes_len) catch return BTreeError.OutOfMemory;
        errdefer self.allocator.free(key_data);
        const keys = self.allocator.alloc([]const u8, new_key_count) catch return BTreeError.OutOfMemory;
        errdefer self.allocator.free(keys);
        const children = self.allocator.alloc(PageId, @as(usize, new_key_count) + 1) catch return BTreeError.OutOfMemory;
        errdefer self.allocator.free(children);

        var key_offset: usize = 0;
        var key_out: usize = 0;
        for (0..num_keys) |key_index| {
            if (key_index == separator_index) continue;
            copyKeyIntoImage(
                InternalNode.getKey(buf, @intCast(key_index)),
                key_data,
                &key_offset,
                keys,
                &key_out,
            );
        }

        var child_out: usize = 0;
        var child_index: u16 = 0;
        while (child_index <= num_keys) : (child_index += 1) {
            if (child_index == separator_index + 1) continue;
            children[child_out] = internalChildAt(buf, child_index);
            child_out += 1;
        }

        return .{
            .key_data = key_data,
            .keys = keys,
            .children = children,
            .level = InternalNode.getLevel(buf),
        };
    }

    fn internalImageReplacingKey(
        self: *Self,
        buf: []const u8,
        key_index: u16,
        replacement: []const u8,
    ) BTreeError!InternalImage {
        const num_keys = InternalNode.getNumKeys(buf);
        if (key_index >= num_keys) return BTreeError.InvalidPage;

        var key_bytes_len: usize = replacement.len;
        for (0..num_keys) |i| {
            if (i == key_index) continue;
            key_bytes_len += InternalNode.getKey(buf, @intCast(i)).len;
        }

        const key_data = self.allocator.alloc(u8, key_bytes_len) catch return BTreeError.OutOfMemory;
        errdefer self.allocator.free(key_data);
        const keys = self.allocator.alloc([]const u8, num_keys) catch return BTreeError.OutOfMemory;
        errdefer self.allocator.free(keys);
        const children = self.allocator.alloc(PageId, @as(usize, num_keys) + 1) catch return BTreeError.OutOfMemory;
        errdefer self.allocator.free(children);

        var key_offset: usize = 0;
        var key_out: usize = 0;
        for (0..num_keys) |i| {
            const source = if (i == key_index) replacement else InternalNode.getKey(buf, @intCast(i));
            copyKeyIntoImage(source, key_data, &key_offset, keys, &key_out);
        }
        var i: u16 = 0;
        while (i <= num_keys) : (i += 1) {
            children[i] = internalChildAt(buf, i);
        }

        return .{
            .key_data = key_data,
            .keys = keys,
            .children = children,
            .level = InternalNode.getLevel(buf),
        };
    }

    /// Image of an internal node with `key` inserted at `slot` and
    /// `new_child` as the child to its right, as when the child at `slot`
    /// has just split.
    fn internalImageInserting(
        self: *Self,
        buf: []const u8,
        slot: u16,
        key: []const u8,
        new_child: PageId,
    ) BTreeError!InternalImage {
        const num_keys = InternalNode.getNumKeys(buf);
        if (slot > num_keys) return BTreeError.InvalidPage;

        var key_bytes_len: usize = key.len;
        for (0..num_keys) |i| key_bytes_len += InternalNode.getKey(buf, @intCast(i)).len;

        const key_data = self.allocator.alloc(u8, key_bytes_len) catch return BTreeError.OutOfMemory;
        errdefer self.allocator.free(key_data);
        const keys = self.allocator.alloc([]const u8, @as(usize, num_keys) + 1) catch return BTreeError.OutOfMemory;
        errdefer self.allocator.free(keys);
        const children = self.allocator.alloc(PageId, @as(usize, num_keys) + 2) catch return BTreeError.OutOfMemory;
        errdefer self.allocator.free(children);

        var key_offset: usize = 0;
        var key_out: usize = 0;
        var child_out: usize = 0;
        var i: u16 = 0;
        while (i <= num_keys) : (i += 1) {
            children[child_out] = internalChildAt(buf, i);
            child_out += 1;
            if (i == slot) {
                copyKeyIntoImage(key, key_data, &key_offset, keys, &key_out);
                children[child_out] = new_child;
                child_out += 1;
            }
            if (i < num_keys) {
                copyKeyIntoImage(InternalNode.getKey(buf, i), key_data, &key_offset, keys, &key_out);
            }
        }

        return .{
            .key_data = key_data,
            .keys = keys,
            .children = children,
            .level = InternalNode.getLevel(buf),
        };
    }

    fn mergedInternalImage(
        self: *Self,
        left: []const u8,
        separator: []const u8,
        right: []const u8,
    ) BTreeError!InternalImage {
        const left_count = InternalNode.getNumKeys(left);
        const right_count = InternalNode.getNumKeys(right);
        if (InternalNode.getLevel(left) != InternalNode.getLevel(right)) return BTreeError.InvalidPage;

        const total_keys: usize = @as(usize, left_count) + 1 + right_count;
        var key_bytes_len: usize = separator.len;
        for (0..left_count) |i| key_bytes_len += InternalNode.getKey(left, @intCast(i)).len;
        for (0..right_count) |i| key_bytes_len += InternalNode.getKey(right, @intCast(i)).len;

        const key_data = self.allocator.alloc(u8, key_bytes_len) catch return BTreeError.OutOfMemory;
        errdefer self.allocator.free(key_data);
        const keys = self.allocator.alloc([]const u8, total_keys) catch return BTreeError.OutOfMemory;
        errdefer self.allocator.free(keys);
        const children = self.allocator.alloc(PageId, total_keys + 1) catch return BTreeError.OutOfMemory;
        errdefer self.allocator.free(children);

        var key_offset: usize = 0;
        var key_out: usize = 0;
        for (0..left_count) |i| {
            copyKeyIntoImage(InternalNode.getKey(left, @intCast(i)), key_data, &key_offset, keys, &key_out);
        }
        copyKeyIntoImage(separator, key_data, &key_offset, keys, &key_out);
        for (0..right_count) |i| {
            copyKeyIntoImage(InternalNode.getKey(right, @intCast(i)), key_data, &key_offset, keys, &key_out);
        }

        var child_out: usize = 0;
        var i: u16 = 0;
        while (i <= left_count) : (i += 1) {
            children[child_out] = internalChildAt(left, i);
            child_out += 1;
        }
        i = 0;
        while (i <= right_count) : (i += 1) {
            children[child_out] = internalChildAt(right, i);
            child_out += 1;
        }

        return .{
            .key_data = key_data,
            .keys = keys,
            .children = children,
            .level = InternalNode.getLevel(left),
        };
    }

    fn internalImageFits(self: *const Self, image: *const InternalImage) bool {
        return self.internalKeysFit(image.keys);
    }

    fn internalKeysFit(self: *const Self, keys: []const []const u8) bool {
        var required: usize = 0;
        for (keys) |key| {
            required += INTERNAL_SLOT_SIZE + 2 + key.len;
        }
        return required <= @as(usize, self.page_size) - INTERNAL_SLOTS_OFFSET;
    }

    fn storedInternalSpace(buf: []const u8) usize {
        var required: usize = 0;
        const count = InternalNode.getNumKeys(buf);
        for (0..count) |i| {
            required += INTERNAL_SLOT_SIZE + 2 + InternalNode.getKey(buf, @intCast(i)).len;
        }
        return required;
    }

    fn writeInternalParts(
        self: *Self,
        buf: []u8,
        level: u16,
        keys: []const []const u8,
        children: []const PageId,
    ) BTreeError!void {
        InternalNode.init(buf, level);
        for (keys, 0..) |key, i| {
            try self.insertInternalEntryDirect(buf, @intCast(i), key, children[i]);
        }
        InternalNode.setRightChild(buf, children[keys.len]);
    }

    fn writeInternalImage(self: *Self, buf: []u8, image: *const InternalImage) BTreeError!void {
        try self.writeInternalParts(buf, image.level, image.keys, image.children);
    }

    fn findInternalChildIndex(buf: []const u8, child_page: PageId) ?u16 {
        const num_keys = InternalNode.getNumKeys(buf);
        var i: u16 = 0;
        while (i <= num_keys) : (i += 1) {
            if (internalChildAt(buf, i) == child_page) return i;
        }
        return null;
    }

    fn collapseRootIfEmpty(self: *Self) BTreeError!bool {
        const root_id = self.root_page;
        const root_frame = self.bp.fetchPage(root_id, .exclusive) catch return BTreeError.BufferPoolFull;
        const header: *const PageHeader = @ptrCast(@alignCast(root_frame.data.ptr));
        if (header.page_type != .btree_internal or InternalNode.getNumKeys(root_frame.data) != 0) {
            self.bp.unpinPage(root_frame, false);
            return false;
        }

        const new_root = InternalNode.getRightChild(root_frame.data);
        if (new_root == NULL_PAGE) {
            self.bp.unpinPage(root_frame, false);
            return BTreeError.InvalidPage;
        }
        self.root_page = new_root;
        self.bp.unpinPage(root_frame, false);

        const freed_root = self.bp.fetchPage(root_id, .exclusive) catch return BTreeError.BufferPoolFull;
        try self.markPageFreeInPlace(freed_root);
        return true;
    }

    fn rebalanceAfterDelete(self: *Self, leaf_page: PageId, path: *const DeletePath) BTreeError!void {
        if (path.len == 0) return;

        const parent_index = path.len - 1;
        const parent_id = path.ancestors[parent_index];
        if (!(try self.tryMergeLeaf(leaf_page, parent_id))) return;

        var node_id = parent_id;
        var ancestor_index = parent_index;
        while (true) {
            if (node_id == self.root_page) {
                _ = try self.collapseRootIfEmpty();
                return;
            }
            if (ancestor_index == 0) return BTreeError.InvalidPage;

            const grandparent_id = path.ancestors[ancestor_index - 1];
            if (!(try self.tryMergeInternal(node_id, grandparent_id))) return;

            node_id = grandparent_id;
            ancestor_index -= 1;
        }
    }

    const LeafImage = struct {
        data: []u8,
        entries: []StoredEntry,

        fn deinit(self: *LeafImage, allocator: Allocator) void {
            allocator.free(self.entries);
            allocator.free(self.data);
        }
    };

    fn combinedLeafImage(self: *Self, left: []const u8, right: []const u8) BTreeError!LeafImage {
        const left_count = LeafNode.getNumEntries(left);
        const right_count = LeafNode.getNumEntries(right);
        const total_count: usize = @as(usize, left_count) + right_count;

        var data_len: usize = 0;
        for (0..left_count) |i| {
            const entry = LeafNode.getStoredEntry(left, @intCast(i));
            data_len += entry.key.len + entry.value.len;
        }
        for (0..right_count) |i| {
            const entry = LeafNode.getStoredEntry(right, @intCast(i));
            data_len += entry.key.len + entry.value.len;
        }

        const data = self.allocator.alloc(u8, data_len) catch return BTreeError.OutOfMemory;
        errdefer self.allocator.free(data);
        const entries = self.allocator.alloc(StoredEntry, total_count) catch return BTreeError.OutOfMemory;
        errdefer self.allocator.free(entries);

        var data_offset: usize = 0;
        var entry_out: usize = 0;
        for ([_][]const u8{ left, right }) |page_buf| {
            const count = LeafNode.getNumEntries(page_buf);
            for (0..count) |i| {
                const source = LeafNode.getStoredEntry(page_buf, @intCast(i));
                const key_copy = data[data_offset..][0..source.key.len];
                @memcpy(key_copy, source.key);
                data_offset += source.key.len;
                const value_copy = data[data_offset..][0..source.value.len];
                @memcpy(value_copy, source.value);
                data_offset += source.value.len;
                entries[entry_out] = .{
                    .key = key_copy,
                    .value = value_copy,
                    .overflow = source.overflow,
                };
                entry_out += 1;
            }
        }

        return .{ .data = data, .entries = entries };
    }

    fn redistributeLeaves(
        self: *Self,
        parent_frame: *BufferFrame,
        separator_index: u16,
        left_frame: *BufferFrame,
        right_frame: *BufferFrame,
    ) BTreeError!bool {
        var image = try self.combinedLeafImage(left_frame.data, right_frame.data);
        defer image.deinit(self.allocator);
        if (image.entries.len < 2) return false;

        const split_point = try self.chooseLeafSplitPoint(image.entries);
        if (split_point == 0 or split_point >= image.entries.len) return false;

        const new_separator = image.entries[split_point].key;
        var parent_image = try self.internalImageReplacingKey(parent_frame.data, separator_index, new_separator);
        defer parent_image.deinit(self.allocator);
        if (!self.internalImageFits(&parent_image)) return false;

        const left_prev = LeafNode.getPrevLeaf(left_frame.data);
        const right_next = LeafNode.getNextLeaf(right_frame.data);
        LeafNode.init(left_frame.data);
        LeafNode.init(right_frame.data);

        for (image.entries[0..split_point], 0..) |entry, i| {
            try self.insertLeafEntryStored(left_frame.data, @intCast(i), entry.key, entry.value, entry.overflow);
        }
        for (image.entries[split_point..], 0..) |entry, i| {
            try self.insertLeafEntryStored(right_frame.data, @intCast(i), entry.key, entry.value, entry.overflow);
        }

        LeafNode.setPrevLeaf(left_frame.data, left_prev);
        LeafNode.setNextLeaf(left_frame.data, right_frame.page_id);
        LeafNode.setPrevLeaf(right_frame.data, left_frame.page_id);
        LeafNode.setNextLeaf(right_frame.data, right_next);
        try self.writeInternalImage(parent_frame.data, &parent_image);
        return true;
    }

    /// Merge a leaf with an adjacent sibling when both payloads fit on one
    /// page. This is deliberately opportunistic rather than occupancy based:
    /// every successful merge returns a page to the database freelist, while a
    /// pair that cannot fit remains a valid (possibly uneven) pair of leaves.
    fn tryMergeLeaf(self: *Self, leaf_page: PageId, parent_id: PageId) BTreeError!bool {
        const parent_frame = self.bp.fetchPage(parent_id, .exclusive) catch return BTreeError.BufferPoolFull;
        var parent_pinned = true;
        var parent_dirty = false;
        defer if (parent_pinned) self.bp.unpinPage(parent_frame, parent_dirty);

        const parent_header: *const PageHeader = @ptrCast(@alignCast(parent_frame.data.ptr));
        if (parent_header.page_type != .btree_internal) return BTreeError.InvalidPage;

        const num_keys = InternalNode.getNumKeys(parent_frame.data);
        if (num_keys == 0) return false;
        const current_index = findInternalChildIndex(parent_frame.data, leaf_page) orelse return BTreeError.InvalidPage;

        // Prefer the right sibling. If the leaf is already rightmost, merge it
        // into its left sibling instead. In both cases the surviving page is
        // `left_id`, and separator_index names the parent key to remove.
        const separator_index: u16 = if (current_index < num_keys) current_index else current_index - 1;
        const left_id = internalChildAt(parent_frame.data, separator_index);
        const right_id = internalChildAt(parent_frame.data, separator_index + 1);

        const left_frame = self.bp.fetchPage(left_id, .exclusive) catch return BTreeError.BufferPoolFull;
        var left_pinned = true;
        var left_dirty = false;
        defer if (left_pinned) self.bp.unpinPage(left_frame, left_dirty);

        const right_frame = self.bp.fetchPage(right_id, .exclusive) catch return BTreeError.BufferPoolFull;
        var right_pinned = true;
        defer if (right_pinned) self.bp.unpinPage(right_frame, false);

        const left_header: *const PageHeader = @ptrCast(@alignCast(left_frame.data.ptr));
        const right_header: *const PageHeader = @ptrCast(@alignCast(right_frame.data.ptr));
        if (left_header.page_type != .btree_leaf or right_header.page_type != .btree_leaf) return false;

        const combined_space = storedLeafSpace(left_frame.data) + storedLeafSpace(right_frame.data);
        if (combined_space > leafPagePayloadCapacity(self.page_size)) {
            const current_space = if (leaf_page == left_id)
                storedLeafSpace(left_frame.data)
            else
                storedLeafSpace(right_frame.data);
            if (current_space >= leafPagePayloadCapacity(self.page_size) / REBALANCE_OCCUPANCY_DIVISOR) {
                return false;
            }
            if (try self.redistributeLeaves(parent_frame, separator_index, left_frame, right_frame)) {
                self.bp.unpinPage(right_frame, true);
                right_pinned = false;
                self.bp.unpinPage(left_frame, true);
                left_pinned = false;
                self.bp.unpinPage(parent_frame, true);
                parent_pinned = false;
            }
            return false;
        }

        var parent_image = try self.internalImageWithoutSeparator(parent_frame.data, separator_index);
        defer parent_image.deinit(self.allocator);

        const next_id = LeafNode.getNextLeaf(right_frame.data);
        var next_frame: ?*BufferFrame = null;
        if (next_id != NULL_PAGE) {
            next_frame = self.bp.fetchPage(next_id, .exclusive) catch return BTreeError.BufferPoolFull;
        }
        defer if (next_frame) |frame| self.bp.unpinPage(frame, false);

        const left_count = LeafNode.getNumEntries(left_frame.data);
        const right_count = LeafNode.getNumEntries(right_frame.data);
        for (0..right_count) |right_index| {
            const entry = LeafNode.getStoredEntry(right_frame.data, @intCast(right_index));
            try self.insertLeafEntryStored(
                left_frame.data,
                left_count + @as(u16, @intCast(right_index)),
                entry.key,
                entry.value,
                entry.overflow,
            );
        }
        LeafNode.setNextLeaf(left_frame.data, next_id);
        if (next_frame) |frame| {
            LeafNode.setPrevLeaf(frame.data, left_id);
            self.bp.unpinPage(frame, true);
            next_frame = null;
        }
        left_dirty = true;

        try self.writeInternalImage(parent_frame.data, &parent_image);
        parent_dirty = true;

        self.bp.unpinPage(right_frame, false);
        right_pinned = false;
        self.bp.unpinPage(left_frame, true);
        left_pinned = false;
        self.bp.unpinPage(parent_frame, true);
        parent_pinned = false;

        const freed_right = self.bp.fetchPage(right_id, .exclusive) catch return BTreeError.BufferPoolFull;
        try self.markPageFreeInPlace(freed_right);
        return true;
    }

    fn tryMergeInternal(self: *Self, node_page: PageId, grandparent_id: PageId) BTreeError!bool {
        const grandparent_frame = self.bp.fetchPage(grandparent_id, .exclusive) catch return BTreeError.BufferPoolFull;
        var grandparent_pinned = true;
        var grandparent_dirty = false;
        defer if (grandparent_pinned) self.bp.unpinPage(grandparent_frame, grandparent_dirty);

        const grandparent_header: *const PageHeader = @ptrCast(@alignCast(grandparent_frame.data.ptr));
        if (grandparent_header.page_type != .btree_internal) return BTreeError.InvalidPage;

        const num_keys = InternalNode.getNumKeys(grandparent_frame.data);
        if (num_keys == 0) return false;
        const current_index = findInternalChildIndex(grandparent_frame.data, node_page) orelse return BTreeError.InvalidPage;
        const separator_index: u16 = if (current_index < num_keys) current_index else current_index - 1;
        const left_id = internalChildAt(grandparent_frame.data, separator_index);
        const right_id = internalChildAt(grandparent_frame.data, separator_index + 1);

        const left_frame = self.bp.fetchPage(left_id, .exclusive) catch return BTreeError.BufferPoolFull;
        var left_pinned = true;
        defer if (left_pinned) self.bp.unpinPage(left_frame, false);
        const right_frame = self.bp.fetchPage(right_id, .exclusive) catch return BTreeError.BufferPoolFull;
        var right_pinned = true;
        defer if (right_pinned) self.bp.unpinPage(right_frame, false);

        const left_header: *const PageHeader = @ptrCast(@alignCast(left_frame.data.ptr));
        const right_header: *const PageHeader = @ptrCast(@alignCast(right_frame.data.ptr));
        if (left_header.page_type != .btree_internal or right_header.page_type != .btree_internal) return false;

        var merged_image = try self.mergedInternalImage(
            left_frame.data,
            InternalNode.getKey(grandparent_frame.data, separator_index),
            right_frame.data,
        );
        defer merged_image.deinit(self.allocator);
        if (!self.internalImageFits(&merged_image)) {
            const current_space = if (node_page == left_id)
                storedInternalSpace(left_frame.data)
            else
                storedInternalSpace(right_frame.data);
            const capacity = @as(usize, self.page_size) - INTERNAL_SLOTS_OFFSET;
            if (current_space >= capacity / REBALANCE_OCCUPANCY_DIVISOR) return false;

            const promotion_index = self.chooseInternalPromotion(merged_image.keys) orelse return false;
            const promoted_key = merged_image.keys[promotion_index];
            var grandparent_image = try self.internalImageReplacingKey(
                grandparent_frame.data,
                separator_index,
                promoted_key,
            );
            defer grandparent_image.deinit(self.allocator);
            if (!self.internalImageFits(&grandparent_image)) return false;

            try self.writeInternalParts(
                left_frame.data,
                merged_image.level,
                merged_image.keys[0..promotion_index],
                merged_image.children[0 .. promotion_index + 1],
            );
            try self.writeInternalParts(
                right_frame.data,
                merged_image.level,
                merged_image.keys[promotion_index + 1 ..],
                merged_image.children[promotion_index + 1 ..],
            );
            try self.writeInternalImage(grandparent_frame.data, &grandparent_image);

            self.bp.unpinPage(right_frame, true);
            right_pinned = false;
            self.bp.unpinPage(left_frame, true);
            left_pinned = false;
            self.bp.unpinPage(grandparent_frame, true);
            grandparent_pinned = false;
            return false;
        }

        var grandparent_image = try self.internalImageWithoutSeparator(grandparent_frame.data, separator_index);
        defer grandparent_image.deinit(self.allocator);

        try self.writeInternalImage(left_frame.data, &merged_image);
        try self.writeInternalImage(grandparent_frame.data, &grandparent_image);
        grandparent_dirty = true;

        self.bp.unpinPage(right_frame, false);
        right_pinned = false;
        self.bp.unpinPage(left_frame, true);
        left_pinned = false;
        self.bp.unpinPage(grandparent_frame, true);
        grandparent_pinned = false;

        const freed_right = self.bp.fetchPage(right_id, .exclusive) catch return BTreeError.BufferPoolFull;
        try self.markPageFreeInPlace(freed_right);
        return true;
    }

    /// Choose the key to promote when an internal node splits so that both
    /// halves fit on a page and their byte sizes are as even as possible.
    /// Returns null when no such key exists. Both halves keep at least one
    /// key.
    fn chooseInternalPromotion(self: *const Self, keys: []const []const u8) ?usize {
        if (keys.len < 3) return null;

        const capacity = @as(usize, self.page_size) - INTERNAL_SLOTS_OFFSET;
        var total: usize = 0;
        for (keys) |key| total += internalEntrySpace(key);

        var best_index: ?usize = null;
        var best_imbalance: usize = std.math.maxInt(usize);
        var left_space: usize = internalEntrySpace(keys[0]);
        var promotion_index: usize = 1;
        while (promotion_index + 1 < keys.len) : (promotion_index += 1) {
            const promoted_space = internalEntrySpace(keys[promotion_index]);
            const right_space = total - left_space - promoted_space;
            if (left_space <= capacity and right_space <= capacity) {
                const imbalance = if (left_space >= right_space)
                    left_space - right_space
                else
                    right_space - left_space;
                if (imbalance < best_imbalance) {
                    best_imbalance = imbalance;
                    best_index = promotion_index;
                }
            }
            left_space += promoted_space;
        }
        return best_index;
    }

    fn internalEntrySpace(key: []const u8) usize {
        return INTERNAL_SLOT_SIZE + 2 + key.len;
    }

    /// Remove an entry from a leaf node and compact surviving entries.
    fn deleteLeafEntry(self: *Self, buf: []u8, slot: u16) BTreeError!void {
        const num_entries = LeafNode.getNumEntries(buf);
        const stored = LeafNode.getStoredValue(buf, slot);

        const remaining_count = num_entries - 1;
        if (remaining_count > 0) {
            var total_data_size: usize = 0;
            for (0..num_entries) |i| {
                if (i == slot) continue;
                const entry = LeafNode.getStoredEntry(buf, @intCast(i));
                total_data_size += entry.key.len + entry.value.len;
            }

            var entries = self.allocator.alloc(StoredEntry, remaining_count) catch return BTreeError.OutOfMemory;
            defer self.allocator.free(entries);

            var data_buffer = self.allocator.alloc(u8, total_data_size) catch return BTreeError.OutOfMemory;
            defer self.allocator.free(data_buffer);

            var data_offset: usize = 0;
            var entry_index: usize = 0;
            for (0..num_entries) |i| {
                if (i == slot) continue;

                const entry = LeafNode.getStoredEntry(buf, @intCast(i));
                const key_copy = data_buffer[data_offset..][0..entry.key.len];
                @memcpy(key_copy, entry.key);
                data_offset += entry.key.len;

                const value_copy = data_buffer[data_offset..][0..entry.value.len];
                @memcpy(value_copy, entry.value);
                data_offset += entry.value.len;

                entries[entry_index] = .{
                    .key = key_copy,
                    .value = value_copy,
                    .overflow = entry.overflow,
                };
                entry_index += 1;
            }

            const page_len: usize = self.page_size;
            const compacted = self.allocator.alloc(u8, page_len) catch return BTreeError.OutOfMemory;
            defer self.allocator.free(compacted);
            @memcpy(compacted[0..page_len], buf[0..page_len]);
            LeafNode.setNumEntries(compacted, 0);

            for (entries, 0..) |entry, i| {
                try self.insertLeafEntryStored(compacted, @intCast(i), entry.key, entry.value, entry.overflow);
            }

            if (stored.overflow) {
                const desc = try parseOverflowDescriptor(stored.value);
                if (desc.page_count > 0) {
                    try self.freeOverflowChain(desc.first_page, desc.page_count);
                }
            }

            @memcpy(buf[0..page_len], compacted[0..page_len]);
            return;
        }

        if (stored.overflow) {
            const desc = try parseOverflowDescriptor(stored.value);
            if (desc.page_count > 0) {
                try self.freeOverflowChain(desc.first_page, desc.page_count);
            }
        }

        LeafNode.setNumEntries(buf, 0);
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
            .scratch_value = null,
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
        scratch_value: ?[]u8,

        /// Get the next key-value pair
        pub fn next(self: *Iterator) BTreeError!?Entry {
            if (self.scratch_value) |scratch| {
                self.tree.allocator.free(scratch);
                self.scratch_value = null;
            }

            const frame = self.current_frame orelse return null;

            const num_entries = LeafNode.getNumEntries(frame.data);

            // Check if we've exhausted current page
            if (self.current_slot >= num_entries) {
                // Move to next leaf
                const next_page = LeafNode.getNextLeaf(frame.data);
                self.tree.bp.unpinPage(frame, false);
                self.current_frame = null;

                if (next_page == NULL_PAGE) {
                    return null;
                }

                self.current_frame = self.tree.bp.fetchPage(next_page, .shared) catch return BTreeError.BufferPoolFull;
                self.current_slot = 0;
                return self.next();
            }

            const stored_entry = LeafNode.getStoredEntry(frame.data, self.current_slot);

            // Check end key
            if (self.end_key) |end| {
                if (self.tree.comparator(stored_entry.key, end) != .lt) {
                    self.tree.bp.unpinPage(frame, false);
                    self.current_frame = null;
                    return null;
                }
            }

            self.current_slot += 1;
            if (stored_entry.overflow) {
                const materialized = try self.tree.materializeStoredValue(.{
                    .value = stored_entry.value,
                    .overflow = true,
                });
                self.scratch_value = materialized;
                return .{
                    .key = stored_entry.key,
                    .value = materialized,
                };
            }

            return .{
                .key = stored_entry.key,
                .value = stored_entry.value,
            };
        }

        /// Reposition the iterator to the first entry >= key.
        /// Optimized for forward seeks: if key is on the current leaf page,
        /// avoids root-to-leaf traversal entirely.
        pub fn seekForward(self: *Iterator, key: []const u8) BTreeError!void {
            if (self.current_frame) |frame| {
                // Fast path: check if key is within current leaf's range
                const num_entries = LeafNode.getNumEntries(frame.data);
                if (num_entries > 0) {
                    const last_key = LeafNode.getKey(frame.data, num_entries - 1);
                    if (self.tree.comparator(key, last_key) != .gt) {
                        // Key is within this page - binary search for slot
                        const result = LeafNode.searchSlot(frame.data, key, self.tree.comparator);
                        self.current_slot = result.slot;
                        return;
                    }
                }
                // Key is beyond current page - unpin and do full traversal
                self.tree.bp.unpinPage(frame, false);
                self.current_frame = null;
            }

            // Fall back to full tree traversal
            const leaf_page = try self.tree.findLeafForKey(key);
            const new_frame = self.tree.bp.fetchPage(leaf_page, .shared) catch return BTreeError.BufferPoolFull;
            const result = LeafNode.searchSlot(new_frame.data, key, self.tree.comparator);
            self.current_frame = new_frame;
            self.current_slot = result.slot;
        }

        /// Clean up the iterator
        pub fn deinit(self: *Iterator) void {
            if (self.scratch_value) |scratch| {
                self.tree.allocator.free(scratch);
                self.scratch_value = null;
            }
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
    defer if (value) |owned| tree.freeValue(owned);
    try std.testing.expectEqualStrings("world", value.?);

    // Look up non-existent key
    const missing = try tree.get("missing");
    try std.testing.expect(missing == null);
}

test "btree get returns owned value outside page buffer" {
    const allocator = std.testing.allocator;

    const vfs = lattice.storage.vfs;
    const page_manager = lattice.storage.page_manager;
    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const path = "/tmp/lattice_btree_test_owned_get.db";
    vfs_impl.delete(path) catch {};

    var pm = try page_manager.PageManager.init(allocator, vfs_impl, path, .{ .create = true });
    defer {
        pm.deinit();
        vfs_impl.delete(path) catch {};
    }

    var bp = try BufferPool.init(allocator, &pm, 65536);
    defer bp.deinit();

    var tree = try BTree.init(allocator, &bp);
    try tree.insert("key", "value");

    const value = (try tree.get("key")).?;
    defer tree.freeValue(value);

    const leaf_page = try tree.findLeafForKey("key");
    const frame = try bp.fetchPage(leaf_page, .shared);
    defer bp.unpinPage(frame, false);

    const page_start = @intFromPtr(frame.data.ptr);
    const page_end = page_start + frame.data.len;
    const value_start = @intFromPtr(value.ptr);

    try std.testing.expect(value_start < page_start or value_start >= page_end);
    try std.testing.expectEqualStrings("value", value);
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
    const apple = (try tree.get("apple")).?;
    defer tree.freeValue(apple);
    try std.testing.expectEqualStrings("red", apple);

    const banana = (try tree.get("banana")).?;
    defer tree.freeValue(banana);
    try std.testing.expectEqualStrings("yellow", banana);

    const cherry = (try tree.get("cherry")).?;
    defer tree.freeValue(cherry);
    try std.testing.expectEqualStrings("red", cherry);

    const date = (try tree.get("date")).?;
    defer tree.freeValue(date);
    try std.testing.expectEqualStrings("brown", date);
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
        if (val) |owned| tree.freeValue(owned);
    }
}

test "btree delete merges leaves and returns pages to freelist" {
    const allocator = std.testing.allocator;

    const vfs = lattice.storage.vfs;
    const page_manager = lattice.storage.page_manager;
    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const path = "/tmp/lattice_btree_test_delete_merge.db";
    vfs_impl.delete(path) catch {};

    var pm = try page_manager.PageManager.init(allocator, vfs_impl, path, .{ .create = true });
    defer {
        pm.deinit();
        vfs_impl.delete(path) catch {};
    }

    var bp = try BufferPool.init(allocator, &pm, 65536);
    defer bp.deinit();

    var tree = try BTree.init(allocator, &bp);
    const value = [_]u8{'v'} ** 96;

    var key_buf: [32]u8 = undefined;
    for (0..160) |i| {
        const key = std.fmt.bufPrint(&key_buf, "key{d:05}", .{i}) catch unreachable;
        try tree.insert(key, &value);
    }

    const page_count_before: u32 = @intCast((try pm.file.size()) / pm.getPageSize());
    try std.testing.expect(page_count_before > 2);

    for (0..150) |i| {
        const key = std.fmt.bufPrint(&key_buf, "key{d:05}", .{i}) catch unreachable;
        try tree.delete(key);
    }

    try std.testing.expect(pm.getHeader().freelist_page != NULL_PAGE);

    var iter = try tree.range(null, null);
    defer iter.deinit();
    var remaining: usize = 0;
    while (try iter.next()) |entry| {
        const expected = std.fmt.bufPrint(&key_buf, "key{d:05}", .{150 + remaining}) catch unreachable;
        try std.testing.expectEqualStrings(expected, entry.key);
        try std.testing.expectEqualSlices(u8, &value, entry.value);
        remaining += 1;
    }
    try std.testing.expectEqual(@as(usize, 10), remaining);

    // New allocations consume reclaimed pages before extending the file.
    const reused_page = try pm.allocatePage();
    try std.testing.expect(reused_page < page_count_before);
    try std.testing.expectEqual(@as(u64, page_count_before) * pm.getPageSize(), try pm.file.size());
    try pm.freePage(reused_page);
}

test "btree delete merges internal levels and collapses a deep root" {
    const allocator = std.testing.allocator;

    const vfs = lattice.storage.vfs;
    const page_manager = lattice.storage.page_manager;
    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const path = "/tmp/lattice_btree_test_delete_internal_merge.db";
    vfs_impl.delete(path) catch {};

    var pm = try page_manager.PageManager.init(allocator, vfs_impl, path, .{ .create = true });
    defer {
        pm.deinit();
        vfs_impl.delete(path) catch {};
    }

    var bp = try BufferPool.init(allocator, &pm, 1024 * 1024);
    defer bp.deinit();

    var tree = try BTree.init(allocator, &bp);
    var key_buf: [400]u8 = undefined;

    for (0..240) |i| {
        @memset(&key_buf, 'x');
        _ = std.fmt.bufPrint(&key_buf, "key{d:05}-", .{i}) catch unreachable;
        try tree.insert(&key_buf, "value");
    }

    const deep_root = tree.getRootPage();
    const root_frame = try bp.fetchPage(deep_root, .shared);
    const root_header: *const PageHeader = @ptrCast(@alignCast(root_frame.data.ptr));
    try std.testing.expectEqual(PageType.btree_internal, root_header.page_type);
    try std.testing.expect(InternalNode.getLevel(root_frame.data) >= 2);
    bp.unpinPage(root_frame, false);

    for (0..232) |i| {
        @memset(&key_buf, 'x');
        _ = std.fmt.bufPrint(&key_buf, "key{d:05}-", .{i}) catch unreachable;
        try tree.delete(&key_buf);
    }

    const collapsed_root = tree.getRootPage();
    try std.testing.expect(collapsed_root != deep_root);
    const collapsed_frame = try bp.fetchPage(collapsed_root, .shared);
    const collapsed_header: *const PageHeader = @ptrCast(@alignCast(collapsed_frame.data.ptr));
    try std.testing.expectEqual(PageType.btree_leaf, collapsed_header.page_type);
    bp.unpinPage(collapsed_frame, false);

    var iter = try tree.range(null, null);
    defer iter.deinit();
    var remaining: usize = 0;
    while (try iter.next()) |entry| {
        @memset(&key_buf, 'x');
        _ = std.fmt.bufPrint(&key_buf, "key{d:05}-", .{232 + remaining}) catch unreachable;
        try std.testing.expectEqualSlices(u8, &key_buf, entry.key);
        try std.testing.expectEqualStrings("value", entry.value);
        remaining += 1;
    }
    try std.testing.expectEqual(@as(usize, 8), remaining);
}

test "btree leaf split accounts for variable-sized entries" {
    const allocator = std.testing.allocator;

    const vfs = lattice.storage.vfs;
    const page_manager = lattice.storage.page_manager;
    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const path = "/tmp/lattice_btree_test_variable_split.db";
    vfs_impl.delete(path) catch {};

    var pm = try page_manager.PageManager.init(allocator, vfs_impl, path, .{ .create = true });
    defer {
        pm.deinit();
        vfs_impl.delete(path) catch {};
    }

    var bp = try BufferPool.init(allocator, &pm, 65536);
    defer bp.deinit();

    var tree = try BTree.init(allocator, &bp);

    const small_value = [_]u8{'s'} ** 20;
    const large_value = [_]u8{'l'} ** 3600;

    var key_buf: [16]u8 = undefined;
    for (0..127) |i| {
        const key = std.fmt.bufPrint(&key_buf, "m{d:05}", .{i}) catch unreachable;
        try tree.insert(key, &small_value);
    }

    try tree.insert("a-large", &large_value);

    const large_read = (try tree.get("a-large")).?;
    defer tree.freeValue(large_read);
    try std.testing.expectEqualSlices(u8, &large_value, large_read);

    for (0..127) |i| {
        const key = std.fmt.bufPrint(&key_buf, "m{d:05}", .{i}) catch unreachable;
        const small_read = (try tree.get(key)).?;
        defer tree.freeValue(small_read);
        try std.testing.expectEqualSlices(u8, &small_value, small_read);
    }
}

test "btree uses overflow for large inline-capable values to preserve split boundaries" {
    const allocator = std.testing.allocator;

    const vfs = lattice.storage.vfs;
    const page_manager = lattice.storage.page_manager;
    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const path = "/tmp/lattice_btree_test_large_inline_split_boundary.db";
    vfs_impl.delete(path) catch {};

    var pm = try page_manager.PageManager.init(allocator, vfs_impl, path, .{ .create = true, .page_size = 4096 });
    defer {
        pm.deinit();
        vfs_impl.delete(path) catch {};
    }

    var bp = try BufferPool.init(allocator, &pm, 65536);
    defer bp.deinit();

    var tree = try BTree.init(allocator, &bp);

    const left = [_]u8{'l'} ** 1500;
    const middle = [_]u8{'m'} ** 3000;
    const right = [_]u8{'r'} ** 1494;

    try tree.insert("a", &left);
    try tree.insert("c", &right);
    try tree.insert("b", &middle);

    const got_left = (try tree.get("a")).?;
    defer tree.freeValue(got_left);
    try std.testing.expectEqualSlices(u8, &left, got_left);

    const got_middle = (try tree.get("b")).?;
    defer tree.freeValue(got_middle);
    try std.testing.expectEqualSlices(u8, &middle, got_middle);

    const got_right = (try tree.get("c")).?;
    defer tree.freeValue(got_right);
    try std.testing.expectEqualSlices(u8, &right, got_right);
}

test "btree overflow values round-trip, scan, delete, and reinsert" {
    const allocator = std.testing.allocator;

    const vfs = lattice.storage.vfs;
    const page_manager = lattice.storage.page_manager;
    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const path = "/tmp/lattice_btree_test_overflow_values.db";
    vfs_impl.delete(path) catch {};

    var pm = try page_manager.PageManager.init(allocator, vfs_impl, path, .{ .create = true, .page_size = 4096 });
    defer {
        pm.deinit();
        vfs_impl.delete(path) catch {};
    }

    var bp = try BufferPool.init(allocator, &pm, 16 * 1024 * 1024);
    defer bp.deinit();

    var tree = try BTree.init(allocator, &bp);

    const small = "inline-value";
    const medium = try allocator.alloc(u8, 100 * 1024);
    defer allocator.free(medium);
    @memset(medium, 'm');
    const large = try allocator.alloc(u8, 1024 * 1024);
    defer allocator.free(large);
    @memset(large, 'l');

    try tree.insert("a-inline", small);
    try tree.insert("b-100kb", medium);
    try tree.insert("c-1mb", large);

    const got_small = (try tree.get("a-inline")).?;
    defer tree.freeValue(got_small);
    try std.testing.expectEqualStrings(small, got_small);

    const got_medium = (try tree.get("b-100kb")).?;
    defer tree.freeValue(got_medium);
    try std.testing.expectEqualSlices(u8, medium, got_medium);

    const got_large = (try tree.get("c-1mb")).?;
    defer tree.freeValue(got_large);
    try std.testing.expectEqualSlices(u8, large, got_large);

    var iter = try tree.range(null, null);
    defer iter.deinit();
    var seen: usize = 0;
    while (try iter.next()) |entry| {
        if (std.mem.eql(u8, entry.key, "a-inline")) {
            try std.testing.expectEqualStrings(small, entry.value);
        } else if (std.mem.eql(u8, entry.key, "b-100kb")) {
            try std.testing.expectEqualSlices(u8, medium, entry.value);
        } else if (std.mem.eql(u8, entry.key, "c-1mb")) {
            try std.testing.expectEqualSlices(u8, large, entry.value);
        }
        seen += 1;
    }
    try std.testing.expectEqual(@as(usize, 3), seen);

    try tree.delete("b-100kb");
    try std.testing.expect((try tree.get("b-100kb")) == null);
    try tree.insert("b-100kb", small);
    const got_reinsert = (try tree.get("b-100kb")).?;
    defer tree.freeValue(got_reinsert);
    try std.testing.expectEqualStrings(small, got_reinsert);

    try std.testing.expect(!tree.canFitLeafEntry("too-big", MAX_VALUE_SIZE + 1));
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
    if (val) |owned| tree.freeValue(owned);

    // Delete key2
    try tree.delete("key2");

    // Verify key2 no longer exists
    const val_after = try tree.get("key2");
    try std.testing.expect(val_after == null);

    // Verify other keys still exist
    try std.testing.expect(try tree.contains("key1"));
    try std.testing.expect(try tree.contains("key3"));
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
    try std.testing.expect(try tree.contains("key1"));
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
        if (val) |owned| tree.freeValue(owned);
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
    try std.testing.expect(try tree.contains("key1"));

    // Delete key
    try tree.delete("key1");
    try std.testing.expect(!(try tree.contains("key1")));

    // Reinsert with different value
    try tree.insert("key1", "value2");
    const val = try tree.get("key1");
    try std.testing.expect(val != null);
    defer if (val) |owned| tree.freeValue(owned);
    try std.testing.expectEqualStrings("value2", val.?);
}

test "btree delete compacts leaf payload space before reinsert" {
    const allocator = std.testing.allocator;

    const vfs = lattice.storage.vfs;
    const page_manager = lattice.storage.page_manager;
    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const path = "/tmp/lattice_btree_test_delete_compacts_leaf.db";
    vfs_impl.delete(path) catch {};

    var pm = try page_manager.PageManager.init(allocator, vfs_impl, path, .{ .create = true, .page_size = 4096 });
    defer {
        pm.deinit();
        vfs_impl.delete(path) catch {};
    }

    var bp = try BufferPool.init(allocator, &pm, 65536);
    defer bp.deinit();

    var tree = try BTree.init(allocator, &bp);

    const first = [_]u8{'a'} ** 1800;
    const second = [_]u8{'b'} ** 1800;
    const replacement = [_]u8{'c'} ** 1800;

    try tree.insert("a", &first);
    try tree.insert("b", &second);
    try tree.delete("a");
    try tree.insert("c", &replacement);

    const root = try tree.bp.fetchPage(tree.root_page, .shared);
    defer tree.bp.unpinPage(root, false);
    const header: *const PageHeader = @ptrCast(@alignCast(root.data.ptr));
    try std.testing.expectEqual(PageType.btree_leaf, header.page_type);
    try std.testing.expectEqual(@as(u16, 2), LeafNode.getNumEntries(root.data));

    const got_second = (try tree.get("b")).?;
    defer tree.freeValue(got_second);
    try std.testing.expectEqualSlices(u8, &second, got_second);

    const got_replacement = (try tree.get("c")).?;
    defer tree.freeValue(got_replacement);
    try std.testing.expectEqualSlices(u8, &replacement, got_replacement);
}
