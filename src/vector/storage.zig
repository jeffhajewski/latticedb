//! Vector Storage for HNSW index.
//!
//! Stores vectors in dedicated pages separate from the HNSW graph structure.
//! This separation improves cache efficiency during distance calculations.
//!
//! Page layout:
//!   - Header (24 bytes): dimensions, vector_count, bytes_per_vector, next_page
//!   - Vector data: [vector_id: u64][f32; dimensions] × vector_count

const std = @import("std");
const builtin = @import("builtin");
const lattice = @import("lattice");

const Allocator = std.mem.Allocator;

const types = lattice.core.types;
const page = lattice.storage.page;
const buffer_pool = lattice.storage.buffer_pool;
const locking = lattice.concurrency.locking;

const PageId = types.PageId;
const NULL_PAGE = types.NULL_PAGE;
const PageHeader = page.PageHeader;
const PageType = page.PageType;
const BufferPool = buffer_pool.BufferPool;
const BufferFrame = buffer_pool.BufferFrame;
const LatchMode = locking.LatchMode;

/// Vector storage errors
pub const VectorStorageError = error{
    /// Vector not found
    NotFound,
    /// Dimension mismatch
    DimensionMismatch,
    /// Vector too large for page
    VectorTooLarge,
    /// Storage is full
    StorageFull,
    /// I/O error
    IoError,
    /// Out of memory
    OutOfMemory,
    /// Buffer pool error
    BufferPoolError,
};

/// Location of a vector in storage
pub const VectorLocation = struct {
    page_id: PageId,
    slot_index: u16,
};

/// Vector page header (after base PageHeader)
pub const VectorPageHeader = struct {
    /// Vector dimensions
    dimensions: u16,
    /// Number of vectors currently on this page
    vector_count: u16,
    /// Bytes per vector entry (8 + dimensions * 4)
    bytes_per_vector: u32,
    /// Next page in chain (0 if none)
    next_page: PageId,
    /// Reserved for future use
    _reserved: u32,

    pub const SIZE: usize = 16;
    pub const OFFSET: usize = @sizeOf(PageHeader);

    pub fn read(buf: []const u8) VectorPageHeader {
        const data = buf[OFFSET..][0..SIZE];
        return VectorPageHeader{
            .dimensions = std.mem.readInt(u16, data[0..2], .little),
            .vector_count = std.mem.readInt(u16, data[2..4], .little),
            .bytes_per_vector = std.mem.readInt(u32, data[4..8], .little),
            .next_page = std.mem.readInt(u32, data[8..12], .little),
            ._reserved = std.mem.readInt(u32, data[12..16], .little),
        };
    }

    pub fn write(self: VectorPageHeader, buf: []u8) void {
        const data = buf[OFFSET..][0..SIZE];
        std.mem.writeInt(u16, data[0..2], self.dimensions, .little);
        std.mem.writeInt(u16, data[2..4], self.vector_count, .little);
        std.mem.writeInt(u32, data[4..8], self.bytes_per_vector, .little);
        std.mem.writeInt(u32, data[8..12], self.next_page, .little);
        std.mem.writeInt(u32, data[12..16], self._reserved, .little);
    }
};

/// Offset where vector data starts
const VECTOR_DATA_OFFSET: usize = @sizeOf(PageHeader) + VectorPageHeader.SIZE;

/// Vector storage manager
pub const VectorStorage = struct {
    allocator: Allocator,
    bp: *BufferPool,
    dimensions: u16,
    bytes_per_vector: u32,
    vectors_per_page: u16,
    first_page: PageId,
    last_page: PageId,

    const Self = @This();
    const PAGE_SIZE: usize = 4096;

    /// Initialize vector storage
    pub fn init(allocator: Allocator, bp: *BufferPool, dimensions: u16) !Self {
        const bytes_per_vector: u32 = 8 + @as(u32, dimensions) * 4; // id + floats

        // Check if vector fits in a page
        const available = PAGE_SIZE - VECTOR_DATA_OFFSET;
        if (bytes_per_vector > available) {
            return VectorStorageError.VectorTooLarge;
        }

        const vectors_per_page: u16 = @intCast(available / bytes_per_vector);

        // Allocate first page
        const first_page = bp.pm.allocatePage() catch return VectorStorageError.IoError;

        // Initialize first page
        const frame = bp.fetchPage(first_page, .exclusive) catch return VectorStorageError.BufferPoolError;

        // Write page header directly (PageHeader is extern struct)
        frame.data[0] = @intFromEnum(PageType.vector_data);
        frame.data[1] = 0; // flags
        std.mem.writeInt(u16, frame.data[2..4], 0, .little); // reserved
        std.mem.writeInt(u32, frame.data[4..8], 0, .little); // checksum

        // Write vector page header
        const vph = VectorPageHeader{
            .dimensions = dimensions,
            .vector_count = 0,
            .bytes_per_vector = bytes_per_vector,
            .next_page = NULL_PAGE,
            ._reserved = 0,
        };
        vph.write(frame.data);

        bp.unpinPage(frame, true);

        return Self{
            .allocator = allocator,
            .bp = bp,
            .dimensions = dimensions,
            .bytes_per_vector = bytes_per_vector,
            .vectors_per_page = vectors_per_page,
            .first_page = first_page,
            .last_page = first_page,
        };
    }

    /// Open existing vector storage from a known first page
    pub fn open(allocator: Allocator, bp: *BufferPool, first_page: PageId, dimensions: u16) !Self {
        const bytes_per_vector: u32 = 8 + @as(u32, dimensions) * 4;

        const available = PAGE_SIZE - VECTOR_DATA_OFFSET;
        if (bytes_per_vector > available) {
            return VectorStorageError.VectorTooLarge;
        }

        const vectors_per_page: u16 = @intCast(available / bytes_per_vector);

        // Validate the existing page and find last page in chain
        var last_page = first_page;
        {
            const frame = bp.fetchPage(first_page, .shared) catch return VectorStorageError.BufferPoolError;

            // Read and validate header
            const vph = VectorPageHeader.read(frame.data);
            if (vph.dimensions != dimensions) {
                bp.unpinPage(frame, false);
                return VectorStorageError.DimensionMismatch;
            }

            var next = vph.next_page;
            bp.unpinPage(frame, false);

            // Walk chain to find last page
            while (next != NULL_PAGE) {
                last_page = next;
                const f = bp.fetchPage(next, .shared) catch return VectorStorageError.BufferPoolError;
                next = VectorPageHeader.read(f.data).next_page;
                bp.unpinPage(f, false);
            }
        }

        return Self{
            .allocator = allocator,
            .bp = bp,
            .dimensions = dimensions,
            .bytes_per_vector = bytes_per_vector,
            .vectors_per_page = vectors_per_page,
            .first_page = first_page,
            .last_page = last_page,
        };
    }

    /// Store a vector and return its location
    pub fn store(self: *Self, vector_id: u64, vector: []const f32) VectorStorageError!VectorLocation {
        if (vector.len != self.dimensions) {
            return VectorStorageError.DimensionMismatch;
        }

        // Start from last known page (avoids O(n) chain walk)
        var current_page = self.last_page;
        var frame = self.bp.fetchPage(current_page, .exclusive) catch return VectorStorageError.BufferPoolError;

        while (true) {
            var vph = VectorPageHeader.read(frame.data);

            if (vph.vector_count < self.vectors_per_page) {
                // Found space - write vector here
                const slot = vph.vector_count;
                const offset = VECTOR_DATA_OFFSET + @as(usize, slot) * self.bytes_per_vector;

                // Write vector_id
                std.mem.writeInt(u64, frame.data[offset..][0..8], vector_id, .little);

                // Write vector data
                const float_offset = offset + 8;
                for (vector, 0..) |val, i| {
                    const byte_offset = float_offset + i * 4;
                    std.mem.writeInt(u32, frame.data[byte_offset..][0..4], @bitCast(val), .little);
                }

                // Update count
                vph.vector_count = slot + 1;
                vph.write(frame.data);

                self.bp.unpinPage(frame, true);

                return VectorLocation{
                    .page_id = current_page,
                    .slot_index = slot,
                };
            }

            // Page is full, check next page
            if (vph.next_page != NULL_PAGE) {
                const next_page = vph.next_page;
                self.bp.unpinPage(frame, false);
                current_page = next_page;
                frame = self.bp.fetchPage(current_page, .exclusive) catch return VectorStorageError.BufferPoolError;
            } else {
                // Allocate new page
                const new_page = self.bp.pm.allocatePage() catch {
                    self.bp.unpinPage(frame, false);
                    return VectorStorageError.IoError;
                };

                // Link to new page
                vph.next_page = new_page;
                vph.write(frame.data);
                self.bp.unpinPage(frame, true);

                // Initialize new page
                frame = self.bp.fetchPage(new_page, .exclusive) catch return VectorStorageError.BufferPoolError;

                // Write page header directly
                frame.data[0] = @intFromEnum(PageType.vector_data);
                frame.data[1] = 0; // flags
                std.mem.writeInt(u16, frame.data[2..4], 0, .little); // reserved
                std.mem.writeInt(u32, frame.data[4..8], 0, .little); // checksum

                const new_vph = VectorPageHeader{
                    .dimensions = self.dimensions,
                    .vector_count = 0,
                    .bytes_per_vector = self.bytes_per_vector,
                    .next_page = NULL_PAGE,
                    ._reserved = 0,
                };
                new_vph.write(frame.data);

                current_page = new_page;
                self.last_page = new_page;
                // Loop will find space on next iteration
            }
        }
    }

    /// Get a vector by location
    pub fn getByLocation(self: *Self, loc: VectorLocation) VectorStorageError![]f32 {
        const frame = self.bp.fetchPage(loc.page_id, .shared) catch return VectorStorageError.BufferPoolError;
        defer self.bp.unpinPage(frame, false);

        const vph = VectorPageHeader.read(frame.data);

        if (loc.slot_index >= vph.vector_count) {
            return VectorStorageError.NotFound;
        }

        const offset = VECTOR_DATA_OFFSET + @as(usize, loc.slot_index) * self.bytes_per_vector;
        const float_offset = offset + 8; // Skip vector_id

        // Allocate and copy vector data
        const result = self.allocator.alloc(f32, self.dimensions) catch return VectorStorageError.OutOfMemory;

        for (0..self.dimensions) |i| {
            const byte_offset = float_offset + i * 4;
            const bits = std.mem.readInt(u32, frame.data[byte_offset..][0..4], .little);
            result[i] = @bitCast(bits);
        }

        return result;
    }

    /// A borrowed vector that points directly into a pinned page buffer.
    /// Caller must call release() when done to unpin the page.
    pub const BorrowedVector = struct {
        data: []const f32,
        frame: *BufferFrame,
        bp: *BufferPool,

        pub fn release(self: BorrowedVector) void {
            self.bp.unpinPage(self.frame, false);
        }
    };

    /// Borrow a vector by location, returning a zero-copy view into page data.
    /// The page stays pinned until the caller calls release() on the result.
    /// Only valid on little-endian architectures (comptime checked).
    pub fn borrowByLocation(self: *Self, loc: VectorLocation) VectorStorageError!BorrowedVector {
        comptime std.debug.assert(builtin.cpu.arch.endian() == .little);
        const frame = self.bp.fetchPage(loc.page_id, .shared) catch return VectorStorageError.BufferPoolError;
        // No defer unpin — caller must release

        const vph = VectorPageHeader.read(frame.data);

        if (loc.slot_index >= vph.vector_count) {
            self.bp.unpinPage(frame, false);
            return VectorStorageError.NotFound;
        }

        const float_offset = VECTOR_DATA_OFFSET + @as(usize, loc.slot_index) * self.bytes_per_vector + 8;
        const f32_ptr: [*]const f32 = @ptrCast(@alignCast(frame.data[float_offset..].ptr));
        return BorrowedVector{
            .data = f32_ptr[0..self.dimensions],
            .frame = frame,
            .bp = self.bp,
        };
    }

    /// Get vector ID at a location
    pub fn getVectorId(self: *Self, loc: VectorLocation) VectorStorageError!u64 {
        const frame = self.bp.fetchPage(loc.page_id, .shared) catch return VectorStorageError.BufferPoolError;
        defer self.bp.unpinPage(frame, false);

        const vph = VectorPageHeader.read(frame.data);

        if (loc.slot_index >= vph.vector_count) {
            return VectorStorageError.NotFound;
        }

        const offset = VECTOR_DATA_OFFSET + @as(usize, loc.slot_index) * self.bytes_per_vector;
        return std.mem.readInt(u64, frame.data[offset..][0..8], .little);
    }

    /// Free a stored vector (for future use, currently no-op)
    pub fn free(self: *Self, vector: []f32) void {
        self.allocator.free(vector);
    }

    /// Get total vector count across all pages
    pub fn count(self: *Self) VectorStorageError!u64 {
        var total: u64 = 0;
        var current_page = self.first_page;

        while (current_page != NULL_PAGE) {
            const frame = self.bp.fetchPage(current_page, .shared) catch return VectorStorageError.BufferPoolError;
            const vph = VectorPageHeader.read(frame.data);
            total += vph.vector_count;
            current_page = vph.next_page;
            self.bp.unpinPage(frame, false);
        }

        return total;
    }

    /// Iterate over all stored vectors, calling the callback for each one.
    /// The callback receives (vector_id, vector_data) and returns true to continue, false to stop.
    /// The vector data is only valid during the callback.
    pub fn forEach(self: *Self, callback: *const fn (u64, []const f32) bool) VectorStorageError!void {
        var current_page = self.first_page;

        while (current_page != NULL_PAGE) {
            const frame = self.bp.fetchPage(current_page, .shared) catch return VectorStorageError.BufferPoolError;
            const vph = VectorPageHeader.read(frame.data);

            // Process each vector on this page
            var slot: u16 = 0;
            while (slot < vph.vector_count) : (slot += 1) {
                const offset = VECTOR_DATA_OFFSET + @as(usize, slot) * self.bytes_per_vector;

                // Read vector_id
                const vector_id = std.mem.readInt(u64, frame.data[offset..][0..8], .little);

                // Read vector data into temporary buffer
                const float_offset = offset + 8;
                var vector_buf: [1024]f32 = undefined; // Max 1024 dimensions
                for (0..self.dimensions) |i| {
                    const byte_offset = float_offset + i * 4;
                    const bits = std.mem.readInt(u32, frame.data[byte_offset..][0..4], .little);
                    vector_buf[i] = @bitCast(bits);
                }

                // Call callback with vector data
                const should_continue = callback(vector_id, vector_buf[0..self.dimensions]);
                if (!should_continue) {
                    self.bp.unpinPage(frame, false);
                    return;
                }
            }

            current_page = vph.next_page;
            self.bp.unpinPage(frame, false);
        }
    }

    /// Entry returned by the iterator
    pub const VectorEntry = struct {
        id: u64,
        location: VectorLocation,
    };

    /// Get all vector IDs and their locations
    pub fn getAllEntries(self: *Self, allocator: Allocator) VectorStorageError![]VectorEntry {
        // First count total vectors
        const total_count = self.count() catch return VectorStorageError.BufferPoolError;
        if (total_count == 0) return &[_]VectorEntry{};

        // Allocate result array
        var entries = allocator.alloc(VectorEntry, @intCast(total_count)) catch return VectorStorageError.OutOfMemory;
        errdefer allocator.free(entries);

        var index: usize = 0;
        var current_page = self.first_page;

        while (current_page != NULL_PAGE) {
            const frame = self.bp.fetchPage(current_page, .shared) catch return VectorStorageError.BufferPoolError;
            const vph = VectorPageHeader.read(frame.data);

            var slot: u16 = 0;
            while (slot < vph.vector_count) : (slot += 1) {
                const offset = VECTOR_DATA_OFFSET + @as(usize, slot) * self.bytes_per_vector;
                const vector_id = std.mem.readInt(u64, frame.data[offset..][0..8], .little);

                entries[index] = VectorEntry{
                    .id = vector_id,
                    .location = VectorLocation{
                        .page_id = current_page,
                        .slot_index = slot,
                    },
                };
                index += 1;
            }

            const next_page = vph.next_page;
            self.bp.unpinPage(frame, false);
            current_page = next_page;
        }

        return entries;
    }
};

// ============================================================================
// Tests
// ============================================================================

test "vector storage init" {
    const allocator = std.testing.allocator;

    const vfs = @import("../storage/vfs.zig");
    const page_manager = @import("../storage/page_manager.zig");

    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const db_path = "/tmp/lattice_vector_storage_test.db";
    vfs_impl.delete(db_path) catch {};

    var pm = try page_manager.PageManager.init(allocator, vfs_impl, db_path, .{ .create = true });
    defer {
        pm.deinit();
        vfs_impl.delete(db_path) catch {};
    }

    var bp = try buffer_pool.BufferPool.init(allocator, &pm, 64 * 4096);
    defer bp.deinit();

    var vs = try VectorStorage.init(allocator, &bp, 128);

    // Verify we can count vectors (should be 0)
    const cnt = try vs.count();
    try std.testing.expectEqual(@as(u64, 0), cnt);
}

test "vector storage store and retrieve" {
    const allocator = std.testing.allocator;

    const vfs = @import("../storage/vfs.zig");
    const page_manager = @import("../storage/page_manager.zig");

    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const db_path = "/tmp/lattice_vector_store_test.db";
    vfs_impl.delete(db_path) catch {};

    var pm = try page_manager.PageManager.init(allocator, vfs_impl, db_path, .{ .create = true });
    defer {
        pm.deinit();
        vfs_impl.delete(db_path) catch {};
    }

    var bp = try buffer_pool.BufferPool.init(allocator, &pm, 64 * 4096);
    defer bp.deinit();

    var storage = try VectorStorage.init(allocator, &bp, 4);

    // Store a vector
    const vector = [_]f32{ 1.0, 2.0, 3.0, 4.0 };
    const loc = try storage.store(42, &vector);

    try std.testing.expectEqual(@as(u16, 0), loc.slot_index);

    // Retrieve it
    const retrieved = try storage.getByLocation(loc);
    defer storage.free(retrieved);

    try std.testing.expectEqual(@as(usize, 4), retrieved.len);
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), retrieved[0], 0.001);
    try std.testing.expectApproxEqAbs(@as(f32, 2.0), retrieved[1], 0.001);
    try std.testing.expectApproxEqAbs(@as(f32, 3.0), retrieved[2], 0.001);
    try std.testing.expectApproxEqAbs(@as(f32, 4.0), retrieved[3], 0.001);

    // Verify vector ID
    const id = try storage.getVectorId(loc);
    try std.testing.expectEqual(@as(u64, 42), id);
}

test "vector storage multiple vectors" {
    const allocator = std.testing.allocator;

    const vfs = @import("../storage/vfs.zig");
    const page_manager = @import("../storage/page_manager.zig");

    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const db_path = "/tmp/lattice_vector_multi_test.db";
    vfs_impl.delete(db_path) catch {};

    var pm = try page_manager.PageManager.init(allocator, vfs_impl, db_path, .{ .create = true });
    defer {
        pm.deinit();
        vfs_impl.delete(db_path) catch {};
    }

    var bp = try buffer_pool.BufferPool.init(allocator, &pm, 64 * 4096);
    defer bp.deinit();

    var storage = try VectorStorage.init(allocator, &bp, 4);

    // Store multiple vectors
    const v1 = [_]f32{ 1.0, 0.0, 0.0, 0.0 };
    const v2 = [_]f32{ 0.0, 1.0, 0.0, 0.0 };
    const v3 = [_]f32{ 0.0, 0.0, 1.0, 0.0 };

    const loc1 = try storage.store(1, &v1);
    const loc2 = try storage.store(2, &v2);
    const loc3 = try storage.store(3, &v3);

    try std.testing.expectEqual(@as(u16, 0), loc1.slot_index);
    try std.testing.expectEqual(@as(u16, 1), loc2.slot_index);
    try std.testing.expectEqual(@as(u16, 2), loc3.slot_index);

    // All should be on same page
    try std.testing.expectEqual(loc1.page_id, loc2.page_id);
    try std.testing.expectEqual(loc2.page_id, loc3.page_id);

    // Count should be 3
    const cnt = try storage.count();
    try std.testing.expectEqual(@as(u64, 3), cnt);
}

test "vector storage page overflow" {
    const allocator = std.testing.allocator;

    const vfs = @import("../storage/vfs.zig");
    const page_manager = @import("../storage/page_manager.zig");

    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const db_path = "/tmp/lattice_vector_overflow_test.db";
    vfs_impl.delete(db_path) catch {};

    var pm = try page_manager.PageManager.init(allocator, vfs_impl, db_path, .{ .create = true });
    defer {
        pm.deinit();
        vfs_impl.delete(db_path) catch {};
    }

    var bp = try buffer_pool.BufferPool.init(allocator, &pm, 64 * 4096);
    defer bp.deinit();

    // Use 384 dimensions: 8 + 384*4 = 1544 bytes per vector
    // Available: 4096 - 24 = 4072 bytes
    // Vectors per page: 4072 / 1544 = 2
    var storage = try VectorStorage.init(allocator, &bp, 384);

    try std.testing.expectEqual(@as(u16, 2), storage.vectors_per_page);

    // Store 3 vectors - should span 2 pages
    var v1: [384]f32 = undefined;
    var v2: [384]f32 = undefined;
    var v3: [384]f32 = undefined;
    @memset(&v1, 1.0);
    @memset(&v2, 2.0);
    @memset(&v3, 3.0);

    const loc1 = try storage.store(1, &v1);
    const loc2 = try storage.store(2, &v2);
    const loc3 = try storage.store(3, &v3);

    // First two on page 1
    try std.testing.expectEqual(loc1.page_id, loc2.page_id);
    // Third on page 2
    try std.testing.expect(loc2.page_id != loc3.page_id);

    // Count should be 3
    const cnt = try storage.count();
    try std.testing.expectEqual(@as(u64, 3), cnt);
}
