//! Vector Storage for HNSW index.
//!
//! Stores vectors in dedicated pages separate from the HNSW graph structure.
//! This separation improves cache efficiency during distance calculations.
//!
//! Page layout:
//!   - Header (24 bytes): dimensions, vector_count, bytes_per_vector, next_page
//!   - Inline vector data: [vector_id: u64][f32; dimensions] × vector_count
//!   - Large vector data: [vector_id: u64][first_overflow_page: u32][reserved: u32]
//!     × vector_count, with f32 payloads stored in linked overflow pages.

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
const PAGE_SIZE: usize = 4096;

/// Vector storage errors
pub const VectorStorageError = error{
    /// Vector not found
    NotFound,
    /// Dimension mismatch
    DimensionMismatch,
    /// Vector too large for page
    VectorTooLarge,
    /// Vector dimension count is outside the supported range
    InvalidDimensions,
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
    /// Bytes per vector page record
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
const INLINE_VECTOR_ID_SIZE: usize = 8;
const OVERFLOW_HEAD_RECORD_SIZE: u32 = 16;
const OVERFLOW_LAYOUT_MAGIC: u32 = 0x564F_4631; // "VOF1"

/// Vector payload overflow page header (after base PageHeader)
const VectorOverflowPageHeader = struct {
    /// Next overflow page in this vector's payload chain
    next_page: PageId,
    /// Number of data bytes used in this page
    bytes_used: u32,

    pub const SIZE: usize = 8;
    pub const OFFSET: usize = @sizeOf(PageHeader);

    pub fn read(buf: []const u8) VectorOverflowPageHeader {
        const data = buf[OFFSET..][0..SIZE];
        return .{
            .next_page = std.mem.readInt(u32, data[0..4], .little),
            .bytes_used = std.mem.readInt(u32, data[4..8], .little),
        };
    }

    pub fn write(self: VectorOverflowPageHeader, buf: []u8) void {
        const data = buf[OFFSET..][0..SIZE];
        std.mem.writeInt(u32, data[0..4], self.next_page, .little);
        std.mem.writeInt(u32, data[4..8], self.bytes_used, .little);
    }
};

const OVERFLOW_DATA_OFFSET: usize = @sizeOf(PageHeader) + VectorOverflowPageHeader.SIZE;
const OVERFLOW_DATA_CAPACITY: usize = PAGE_SIZE - OVERFLOW_DATA_OFFSET;
const OVERFLOW_FLOATS_PER_PAGE: usize = OVERFLOW_DATA_CAPACITY / @sizeOf(f32);

pub const VectorLayout = enum {
    inline_data,
    overflow,
};

/// Vector storage manager
pub const VectorStorage = struct {
    allocator: Allocator,
    bp: *BufferPool,
    dimensions: u16,
    bytes_per_vector: u32,
    vectors_per_page: u16,
    first_page: PageId,
    last_page: PageId,
    layout: VectorLayout,

    const Self = @This();

    fn validateDimensions(dimensions: u16) VectorStorageError!void {
        types.validateVectorDimensions(dimensions) catch return VectorStorageError.InvalidDimensions;
    }

    fn layoutForDimensions(dimensions: u16) VectorLayout {
        const inline_bytes = inlineBytesPerVector(dimensions);
        return if (inline_bytes <= inlinePageCapacity()) .inline_data else .overflow;
    }

    fn inlinePageCapacity() usize {
        return PAGE_SIZE - VECTOR_DATA_OFFSET;
    }

    fn inlineBytesPerVector(dimensions: u16) u32 {
        return @as(u32, INLINE_VECTOR_ID_SIZE) + @as(u32, dimensions) * @as(u32, @sizeOf(f32));
    }

    fn bytesPerRecord(dimensions: u16, layout: VectorLayout) u32 {
        return switch (layout) {
            .inline_data => inlineBytesPerVector(dimensions),
            .overflow => OVERFLOW_HEAD_RECORD_SIZE,
        };
    }

    fn layoutMagic(layout: VectorLayout) u32 {
        return switch (layout) {
            .inline_data => 0,
            .overflow => OVERFLOW_LAYOUT_MAGIC,
        };
    }

    fn layoutFromHeader(vph: VectorPageHeader) VectorStorageError!VectorLayout {
        return switch (vph._reserved) {
            0 => .inline_data,
            OVERFLOW_LAYOUT_MAGIC => .overflow,
            else => VectorStorageError.IoError,
        };
    }

    fn initPageHeader(frame: *BufferFrame, page_type: PageType) void {
        frame.data[0] = @intFromEnum(page_type);
        frame.data[1] = 0; // flags
        std.mem.writeInt(u16, frame.data[2..4], 0, .little); // reserved
        std.mem.writeInt(u32, frame.data[4..8], 0, .little); // checksum
    }

    fn writeVectorPageHeader(frame: *BufferFrame, dimensions: u16, bytes_per_vector: u32, layout: VectorLayout) void {
        initPageHeader(frame, .vector_data);
        const vph = VectorPageHeader{
            .dimensions = dimensions,
            .vector_count = 0,
            .bytes_per_vector = bytes_per_vector,
            .next_page = NULL_PAGE,
            ._reserved = layoutMagic(layout),
        };
        vph.write(frame.data);
    }

    fn validatePageHeader(self: *const Self, vph: VectorPageHeader) VectorStorageError!void {
        if (vph.dimensions != self.dimensions) return VectorStorageError.DimensionMismatch;
        const layout = try layoutFromHeader(vph);
        if (layout != self.layout) return VectorStorageError.DimensionMismatch;
        if (vph.bytes_per_vector != self.bytes_per_vector) return VectorStorageError.DimensionMismatch;
        if (vph.vector_count > self.vectors_per_page) return VectorStorageError.IoError;
    }

    pub fn usesOverflowPages(self: *const Self) bool {
        return self.layout == .overflow;
    }

    pub fn overflowPagesPerVector(self: *const Self) u64 {
        if (!self.usesOverflowPages()) return 0;
        const floats_per_page: u64 = OVERFLOW_FLOATS_PER_PAGE;
        return (@as(u64, self.dimensions) + floats_per_page - 1) / floats_per_page;
    }

    /// Initialize vector storage
    pub fn init(allocator: Allocator, bp: *BufferPool, dimensions: u16) !Self {
        try validateDimensions(dimensions);

        const layout = layoutForDimensions(dimensions);
        const bytes_per_vector = bytesPerRecord(dimensions, layout);
        const vectors_per_page: u16 = @intCast(inlinePageCapacity() / bytes_per_vector);

        // Allocate first page
        const first_page = bp.pm.allocatePage() catch return VectorStorageError.IoError;

        // Initialize first page
        const frame = bp.fetchPage(first_page, .exclusive) catch return VectorStorageError.BufferPoolError;

        writeVectorPageHeader(frame, dimensions, bytes_per_vector, layout);
        bp.unpinPage(frame, true);

        return Self{
            .allocator = allocator,
            .bp = bp,
            .dimensions = dimensions,
            .bytes_per_vector = bytes_per_vector,
            .vectors_per_page = vectors_per_page,
            .first_page = first_page,
            .last_page = first_page,
            .layout = layout,
        };
    }

    /// Open existing vector storage from a known first page
    pub fn open(allocator: Allocator, bp: *BufferPool, first_page: PageId, dimensions: u16) !Self {
        try validateDimensions(dimensions);

        const expected_layout = layoutForDimensions(dimensions);
        const bytes_per_vector = bytesPerRecord(dimensions, expected_layout);
        const vectors_per_page: u16 = @intCast(inlinePageCapacity() / bytes_per_vector);

        var storage = Self{
            .allocator = allocator,
            .bp = bp,
            .dimensions = dimensions,
            .bytes_per_vector = bytes_per_vector,
            .vectors_per_page = vectors_per_page,
            .first_page = first_page,
            .last_page = first_page,
            .layout = expected_layout,
        };

        // Validate the existing page and find last page in chain
        var last_page = first_page;
        {
            const frame = bp.fetchPage(first_page, .shared) catch return VectorStorageError.BufferPoolError;

            // Read and validate header
            const vph = VectorPageHeader.read(frame.data);
            storage.validatePageHeader(vph) catch |err| {
                bp.unpinPage(frame, false);
                return err;
            };

            var next = vph.next_page;
            bp.unpinPage(frame, false);

            // Walk chain to find last page
            while (next != NULL_PAGE) {
                last_page = next;
                const f = bp.fetchPage(next, .shared) catch return VectorStorageError.BufferPoolError;
                const next_vph = VectorPageHeader.read(f.data);
                storage.validatePageHeader(next_vph) catch |err| {
                    bp.unpinPage(f, false);
                    return err;
                };
                next = next_vph.next_page;
                bp.unpinPage(f, false);
            }
        }

        storage.last_page = last_page;
        return storage;
    }

    /// Store a vector and return its location
    pub fn store(self: *Self, vector_id: u64, vector: []const f32) VectorStorageError!VectorLocation {
        if (vector.len != self.dimensions) {
            return VectorStorageError.DimensionMismatch;
        }

        return switch (self.layout) {
            .inline_data => self.storeInline(vector_id, vector),
            .overflow => self.storeOverflow(vector_id, vector),
        };
    }

    fn storeInline(self: *Self, vector_id: u64, vector: []const f32) VectorStorageError!VectorLocation {
        std.debug.assert(self.layout == .inline_data);

        // Start from last known page (avoids O(n) chain walk)
        var current_page = self.last_page;
        var frame = self.bp.fetchPage(current_page, .exclusive) catch return VectorStorageError.BufferPoolError;

        while (true) {
            var vph = VectorPageHeader.read(frame.data);
            self.validatePageHeader(vph) catch |err| {
                self.bp.unpinPage(frame, false);
                return err;
            };

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

                writeVectorPageHeader(frame, self.dimensions, self.bytes_per_vector, self.layout);

                current_page = new_page;
                self.last_page = new_page;
                // Loop will find space on next iteration
            }
        }
    }

    fn storeOverflow(self: *Self, vector_id: u64, vector: []const f32) VectorStorageError!VectorLocation {
        std.debug.assert(self.layout == .overflow);

        var current_page = self.last_page;
        var frame = self.bp.fetchPage(current_page, .exclusive) catch return VectorStorageError.BufferPoolError;

        while (true) {
            var vph = VectorPageHeader.read(frame.data);
            self.validatePageHeader(vph) catch |err| {
                self.bp.unpinPage(frame, false);
                return err;
            };

            if (vph.vector_count < self.vectors_per_page) {
                const slot = vph.vector_count;
                const first_overflow = self.writeOverflowChain(vector) catch |err| {
                    self.bp.unpinPage(frame, false);
                    return err;
                };

                const offset = VECTOR_DATA_OFFSET + @as(usize, slot) * self.bytes_per_vector;
                std.mem.writeInt(u64, frame.data[offset..][0..8], vector_id, .little);
                std.mem.writeInt(u32, frame.data[offset + 8 ..][0..4], first_overflow, .little);
                std.mem.writeInt(u32, frame.data[offset + 12 ..][0..4], 0, .little);

                vph.vector_count = slot + 1;
                vph.write(frame.data);

                self.bp.unpinPage(frame, true);
                return .{
                    .page_id = current_page,
                    .slot_index = slot,
                };
            }

            if (vph.next_page != NULL_PAGE) {
                const next_page = vph.next_page;
                self.bp.unpinPage(frame, false);
                current_page = next_page;
                frame = self.bp.fetchPage(current_page, .exclusive) catch return VectorStorageError.BufferPoolError;
            } else {
                const new_page = self.bp.pm.allocatePage() catch {
                    self.bp.unpinPage(frame, false);
                    return VectorStorageError.IoError;
                };

                vph.next_page = new_page;
                vph.write(frame.data);
                self.bp.unpinPage(frame, true);

                frame = self.bp.fetchPage(new_page, .exclusive) catch return VectorStorageError.BufferPoolError;
                writeVectorPageHeader(frame, self.dimensions, self.bytes_per_vector, self.layout);

                current_page = new_page;
                self.last_page = new_page;
            }
        }
    }

    fn writeOverflowChain(self: *Self, vector: []const f32) VectorStorageError!PageId {
        var first_page: PageId = NULL_PAGE;
        var previous_frame: ?*BufferFrame = null;
        errdefer if (previous_frame) |frame| self.bp.unpinPage(frame, false);

        var vector_offset: usize = 0;
        while (vector_offset < vector.len) {
            const page_id = self.bp.pm.allocatePage() catch return VectorStorageError.IoError;
            const frame = self.bp.fetchPage(page_id, .exclusive) catch return VectorStorageError.BufferPoolError;

            if (first_page == NULL_PAGE) {
                first_page = page_id;
            }

            if (previous_frame) |prev| {
                var prev_header = VectorOverflowPageHeader.read(prev.data);
                prev_header.next_page = page_id;
                prev_header.write(prev.data);
                self.bp.unpinPage(prev, true);
                previous_frame = null;
            }

            initPageHeader(frame, .overflow);

            const floats_this_page: usize = @min(OVERFLOW_FLOATS_PER_PAGE, vector.len - vector_offset);
            const bytes_used = @as(u32, @intCast(floats_this_page)) * @as(u32, @sizeOf(f32));
            const header = VectorOverflowPageHeader{
                .next_page = NULL_PAGE,
                .bytes_used = bytes_used,
            };
            header.write(frame.data);

            for (vector[vector_offset..][0..floats_this_page], 0..) |val, i| {
                const byte_offset = OVERFLOW_DATA_OFFSET + i * @sizeOf(f32);
                std.mem.writeInt(u32, frame.data[byte_offset..][0..4], @bitCast(val), .little);
            }

            previous_frame = frame;
            vector_offset += floats_this_page;
        }

        if (previous_frame) |frame| {
            self.bp.unpinPage(frame, true);
        }

        return first_page;
    }

    /// Get a vector by location
    pub fn getByLocation(self: *Self, loc: VectorLocation) VectorStorageError![]f32 {
        const frame = self.bp.fetchPage(loc.page_id, .shared) catch return VectorStorageError.BufferPoolError;
        defer self.bp.unpinPage(frame, false);

        const vph = VectorPageHeader.read(frame.data);
        try self.validatePageHeader(vph);

        if (loc.slot_index >= vph.vector_count) {
            return VectorStorageError.NotFound;
        }

        const result = self.allocator.alloc(f32, self.dimensions) catch return VectorStorageError.OutOfMemory;
        errdefer self.allocator.free(result);

        try self.readVectorFromHeadFrame(frame, loc.slot_index, result);
        return result;
    }

    fn readVectorFromHeadFrame(
        self: *Self,
        frame: *BufferFrame,
        slot_index: u16,
        result: []f32,
    ) VectorStorageError!void {
        std.debug.assert(result.len == self.dimensions);

        const offset = VECTOR_DATA_OFFSET + @as(usize, slot_index) * self.bytes_per_vector;

        switch (self.layout) {
            .inline_data => {
                const float_offset = offset + INLINE_VECTOR_ID_SIZE;

                for (0..self.dimensions) |i| {
                    const byte_offset = float_offset + i * @sizeOf(f32);
                    const bits = std.mem.readInt(u32, frame.data[byte_offset..][0..4], .little);
                    result[i] = @bitCast(bits);
                }
            },
            .overflow => {
                const first_overflow = std.mem.readInt(u32, frame.data[offset + 8 ..][0..4], .little);
                try self.readOverflowChain(first_overflow, result);
            },
        }
    }

    fn readOverflowChain(self: *Self, first_page: PageId, result: []f32) VectorStorageError!void {
        if (first_page == NULL_PAGE) return VectorStorageError.IoError;

        var current_page = first_page;
        var vector_offset: usize = 0;

        while (current_page != NULL_PAGE and vector_offset < result.len) {
            const frame = self.bp.fetchPage(current_page, .shared) catch return VectorStorageError.BufferPoolError;
            const oph = VectorOverflowPageHeader.read(frame.data);

            if (oph.bytes_used > OVERFLOW_DATA_CAPACITY or oph.bytes_used % @sizeOf(f32) != 0) {
                self.bp.unpinPage(frame, false);
                return VectorStorageError.IoError;
            }

            const floats_in_page = @min(@as(usize, oph.bytes_used) / @sizeOf(f32), result.len - vector_offset);
            for (0..floats_in_page) |i| {
                const byte_offset = OVERFLOW_DATA_OFFSET + i * @sizeOf(f32);
                const bits = std.mem.readInt(u32, frame.data[byte_offset..][0..4], .little);
                result[vector_offset + i] = @bitCast(bits);
            }

            vector_offset += floats_in_page;
            current_page = oph.next_page;
            self.bp.unpinPage(frame, false);
        }

        if (vector_offset != result.len) {
            return VectorStorageError.IoError;
        }
    }

    /// A borrowed vector that points directly into a pinned page buffer.
    /// Caller must call release() when done to unpin the page.
    pub const BorrowedVector = struct {
        data: []const f32,
        frame: ?*BufferFrame = null,
        bp: ?*BufferPool = null,
        owned_data: ?[]f32 = null,
        allocator: ?Allocator = null,

        pub fn release(self: BorrowedVector) void {
            if (self.frame) |frame| {
                self.bp.?.unpinPage(frame, false);
            }
            if (self.owned_data) |owned| {
                self.allocator.?.free(owned);
            }
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
        self.validatePageHeader(vph) catch |err| {
            self.bp.unpinPage(frame, false);
            return err;
        };

        if (loc.slot_index >= vph.vector_count) {
            self.bp.unpinPage(frame, false);
            return VectorStorageError.NotFound;
        }

        switch (self.layout) {
            .inline_data => {
                const float_offset = VECTOR_DATA_OFFSET + @as(usize, loc.slot_index) * self.bytes_per_vector + INLINE_VECTOR_ID_SIZE;
                const f32_ptr: [*]const f32 = @ptrCast(@alignCast(frame.data[float_offset..].ptr));
                return BorrowedVector{
                    .data = f32_ptr[0..self.dimensions],
                    .frame = frame,
                    .bp = self.bp,
                };
            },
            .overflow => {
                const owned = self.allocator.alloc(f32, self.dimensions) catch {
                    self.bp.unpinPage(frame, false);
                    return VectorStorageError.OutOfMemory;
                };
                errdefer self.allocator.free(owned);

                self.readVectorFromHeadFrame(frame, loc.slot_index, owned) catch |err| {
                    self.bp.unpinPage(frame, false);
                    return err;
                };
                self.bp.unpinPage(frame, false);

                return BorrowedVector{
                    .data = owned,
                    .owned_data = owned,
                    .allocator = self.allocator,
                };
            },
        }
    }

    /// Get vector ID at a location
    pub fn getVectorId(self: *Self, loc: VectorLocation) VectorStorageError!u64 {
        const frame = self.bp.fetchPage(loc.page_id, .shared) catch return VectorStorageError.BufferPoolError;
        defer self.bp.unpinPage(frame, false);

        const vph = VectorPageHeader.read(frame.data);
        try self.validatePageHeader(vph);

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
            self.validatePageHeader(vph) catch |err| {
                self.bp.unpinPage(frame, false);
                return err;
            };
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
        const vector_buf = self.allocator.alloc(f32, self.dimensions) catch return VectorStorageError.OutOfMemory;
        defer self.allocator.free(vector_buf);

        var current_page = self.first_page;

        while (current_page != NULL_PAGE) {
            const frame = self.bp.fetchPage(current_page, .shared) catch return VectorStorageError.BufferPoolError;
            const vph = VectorPageHeader.read(frame.data);
            self.validatePageHeader(vph) catch |err| {
                self.bp.unpinPage(frame, false);
                return err;
            };

            // Process each vector on this page
            var slot: u16 = 0;
            while (slot < vph.vector_count) : (slot += 1) {
                const offset = VECTOR_DATA_OFFSET + @as(usize, slot) * self.bytes_per_vector;

                // Read vector_id
                const vector_id = std.mem.readInt(u64, frame.data[offset..][0..8], .little);

                self.readVectorFromHeadFrame(frame, slot, vector_buf) catch |err| {
                    self.bp.unpinPage(frame, false);
                    return err;
                };

                // Call callback with vector data
                const should_continue = callback(vector_id, vector_buf);
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
        const total_count = try self.count();
        if (total_count == 0) return &[_]VectorEntry{};

        // Allocate result array
        var entries = allocator.alloc(VectorEntry, @intCast(total_count)) catch return VectorStorageError.OutOfMemory;
        errdefer allocator.free(entries);

        var index: usize = 0;
        var current_page = self.first_page;

        while (current_page != NULL_PAGE) {
            const frame = self.bp.fetchPage(current_page, .shared) catch return VectorStorageError.BufferPoolError;
            const vph = VectorPageHeader.read(frame.data);
            self.validatePageHeader(vph) catch |err| {
                self.bp.unpinPage(frame, false);
                return err;
            };

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

fn expectStoredVector(vector: []const f32, dimensions: usize, base: f32) !void {
    try std.testing.expectEqual(dimensions, vector.len);
    try std.testing.expectApproxEqAbs(base, vector[0], 0.001);
    try std.testing.expectApproxEqAbs(base + @as(f32, @floatFromInt(dimensions - 1)) * 0.001, vector[dimensions - 1], 0.001);
    try std.testing.expectApproxEqAbs(base + 0.127, vector[127], 0.001);
}

fn fillLargeVector(vector: []f32, base: f32) void {
    for (vector, 0..) |*value, i| {
        value.* = base + @as(f32, @floatFromInt(i)) * 0.001;
    }
}

fn testLargeVectorRoundTrip(comptime dimensions: usize) !void {
    const allocator = std.testing.allocator;

    const vfs = @import("../storage/vfs.zig");
    const page_manager = @import("../storage/page_manager.zig");

    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const db_path = try std.fmt.allocPrint(allocator, "/tmp/lattice_vector_large_{d}_test.db", .{dimensions});
    defer allocator.free(db_path);
    vfs_impl.delete(db_path) catch {};
    defer vfs_impl.delete(db_path) catch {};

    var loc: VectorLocation = undefined;
    var first_page: PageId = undefined;
    {
        var pm = try page_manager.PageManager.init(allocator, vfs_impl, db_path, .{ .create = true });
        defer pm.deinit();

        var bp = try buffer_pool.BufferPool.init(allocator, &pm, 96 * 4096);
        defer bp.deinit();

        var storage = try VectorStorage.init(allocator, &bp, @intCast(dimensions));
        first_page = storage.first_page;

        var vector: [dimensions]f32 = undefined;
        fillLargeVector(&vector, 10.0);

        loc = try storage.store(9001, &vector);
        try std.testing.expectEqual(@as(u16, 0), loc.slot_index);

        {
            const borrowed = try storage.borrowByLocation(loc);
            defer borrowed.release();
            try expectStoredVector(borrowed.data, dimensions, 10.0);
        }

        const vector_id = try storage.getVectorId(loc);
        try std.testing.expectEqual(@as(u64, 9001), vector_id);

        try bp.close();
    }

    {
        var pm = try page_manager.PageManager.init(allocator, vfs_impl, db_path, .{});
        defer pm.deinit();

        var bp = try buffer_pool.BufferPool.init(allocator, &pm, 96 * 4096);
        defer bp.deinit();

        var storage = try VectorStorage.open(allocator, &bp, first_page, @intCast(dimensions));
        const retrieved = try storage.getByLocation(loc);
        defer storage.free(retrieved);

        try expectStoredVector(retrieved, dimensions, 10.0);
        try std.testing.expectEqual(@as(u64, 1), try storage.count());
    }
}

test "vector storage large vector round trip 1536 dimensions" {
    try testLargeVectorRoundTrip(1536);
}

test "vector storage large vector round trip 3072 dimensions" {
    try testLargeVectorRoundTrip(3072);
}

test "vector storage large vector round trip 4096 dimensions" {
    try testLargeVectorRoundTrip(4096);
}

test "vector storage rejects invalid dimensions and mismatched vectors" {
    const allocator = std.testing.allocator;

    const vfs = @import("../storage/vfs.zig");
    const page_manager = @import("../storage/page_manager.zig");

    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const db_path = "/tmp/lattice_vector_invalid_dims_test.db";
    vfs_impl.delete(db_path) catch {};

    var pm = try page_manager.PageManager.init(allocator, vfs_impl, db_path, .{ .create = true });
    defer {
        pm.deinit();
        vfs_impl.delete(db_path) catch {};
    }

    var bp = try buffer_pool.BufferPool.init(allocator, &pm, 64 * 4096);
    defer bp.deinit();

    try std.testing.expectError(VectorStorageError.InvalidDimensions, VectorStorage.init(allocator, &bp, 0));
    try std.testing.expectError(VectorStorageError.InvalidDimensions, VectorStorage.init(allocator, &bp, 4097));

    var storage = try VectorStorage.init(allocator, &bp, 4);
    const wrong = [_]f32{ 1.0, 2.0, 3.0 };
    try std.testing.expectError(VectorStorageError.DimensionMismatch, storage.store(1, &wrong));
}
