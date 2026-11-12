//! Buffer Pool for Lattice database.
//!
//! Caches pages in memory with pin/unpin reference counting,
//! dirty page tracking, and Clock eviction algorithm.
//!
//! A dirty page leaves the pool for the database file in one way only: as
//! part of `flushAll`, which writes every dirty page together, through the
//! journal, so that the file goes from one complete image to the next.
//! Eviction never writes. The write-ahead log holds logical records, and
//! they can only be replayed onto a file that is a whole, consistent image;
//! a dirty page written on its own, between flushes, put half of a B+Tree
//! change into the one copy recovery starts from.

const std = @import("std");
const lattice = @import("lattice");

const Allocator = std.mem.Allocator;
const Alignment = std.mem.Alignment;

const page_manager = lattice.storage.page_manager;
const page = lattice.storage.page;
const journal_mod = lattice.storage.journal;
const locking = lattice.concurrency.locking;
const types = lattice.core.types;

const PageManager = page_manager.PageManager;
const PageManagerError = page_manager.PageManagerError;
const PageHeader = page.PageHeader;
const RwLatch = locking.RwLatch;
const LatchMode = locking.LatchMode;
const PageId = types.PageId;
const NULL_PAGE = types.NULL_PAGE;

/// Frame identifier within the buffer pool
pub const FrameId = u32;

/// Invalid frame marker
pub const INVALID_FRAME: FrameId = std.math.maxInt(FrameId);

/// Buffer Pool errors
pub const BufferPoolError = error{
    BufferPoolFull,
    PageNotFound,
    IoError,
    InvalidPageId,
    ChecksumMismatch,
};

/// A single frame in the buffer pool
pub const BufferFrame = struct {
    /// The page ID currently stored in this frame (0 if empty)
    page_id: PageId,
    /// Page data buffer (4KB aligned)
    data: []align(4096) u8,
    /// Reference count - page cannot be evicted while pinned
    pin_count: std.atomic.Value(u32),
    /// Whether the page has been modified
    dirty: bool,
    /// Usage count for Clock algorithm (second chance)
    usage_count: u8,
    /// Reader-writer latch for thread-safe access
    latch: RwLatch,

    /// Check if frame is in use (has a page loaded)
    pub fn isInUse(self: *const BufferFrame) bool {
        return self.page_id != NULL_PAGE;
    }

    /// Check if frame is pinned
    pub fn isPinned(self: *const BufferFrame) bool {
        return self.pin_count.load(.monotonic) > 0;
    }
};

/// Buffer Pool - caches database pages in memory
pub const BufferPool = struct {
    /// All frames in the pool
    frames: []BufferFrame,
    /// Maps page IDs to frame IDs for quick lookup.
    ///
    /// Every eviction removes a key and every miss adds one, and the map
    /// marks a removed slot with a tombstone it never clears on its own. A
    /// lookup of a page that is not there probes until it reaches a slot
    /// that was never used, so once churn has turned most of those into
    /// tombstones each miss walks much of the table. Removals go through
    /// `forgetPage`, which rehashes the table often enough to keep free
    /// slots, and the table is rebuilt at its usual size once the frames
    /// beyond the pool are given back, so a load that grew it does not
    /// leave it large.
    page_table: std.AutoHashMap(PageId, FrameId),
    /// Keys removed from `page_table` since it was last rehashed: an upper
    /// bound on its tombstones.
    page_table_removals: usize = 0,
    /// List of free (unused) frame IDs
    free_list: std.ArrayList(FrameId),
    /// Clock hand position for eviction algorithm
    clock_hand: usize,
    /// Underlying page manager for disk I/O
    pm: *PageManager,
    /// Allocator for dynamic memory
    allocator: Allocator,
    /// Mutex protecting page_table and free_list
    mutex: @import("compat").Mutex,
    /// Number of frames in the pool
    frame_count: usize,
    /// Frames beyond the pool's size, numbered from `frame_count` up.
    ///
    /// When every frame that could be evicted is dirty, the pool grows
    /// rather than write one of them out. These go back to the allocator at
    /// the next flush. A slot is null once its frame has been released.
    overflow: std.ArrayListUnmanaged(?*BufferFrame) = .empty,
    /// How many slots of `overflow` are null, so that growing the pool
    /// only searches for one when there is one to find.
    overflow_holes: usize = 0,
    /// The last clock sweep found no frame of the pool proper it could
    /// evict, and nearly all of them dirty. Nothing but a flush or
    /// `abandonDirty` makes a dirty frame clean, so until then a sweep would
    /// find much the same, and the pool grows without one: each sweep walks
    /// every frame twice. The few frames that were pinned and clean, and are
    /// let go meanwhile, wait for the flush. Guarded by `mutex`.
    all_frames_dirty: bool = false,
    /// Where a flush records its pages before writing them in place. Null
    /// for a database with no file behind it, which has nothing to tear.
    journal: ?*journal_mod.Journal = null,
    /// Roughly how many frames are dirty. Set exactly by each flush and
    /// raised by `unpinPage` in between, so it can run low if a caller sets
    /// `dirty` by hand; it only decides when to flush early.
    dirty_count: usize = 0,

    const Self = @This();

    /// Initialize a buffer pool with the given size in bytes.
    /// The pool will contain (pool_size / page_size) frames.
    pub fn init(allocator: Allocator, pm: *PageManager, pool_size: usize) !Self {
        const page_size = pm.getPageSize();
        const frame_count = pool_size / page_size;

        if (frame_count == 0) {
            return BufferPoolError.BufferPoolFull;
        }

        var frames = try allocator.alloc(BufferFrame, frame_count);
        errdefer allocator.free(frames);

        var free_list: std.ArrayList(FrameId) = .empty;
        errdefer free_list.deinit(allocator);

        // Initialize all frames
        var initialized_count: usize = 0;
        errdefer {
            for (frames[0..initialized_count]) |*frame| {
                allocator.free(frame.data);
            }
        }

        // 4096-byte alignment for page I/O
        const page_alignment = comptime Alignment.fromByteUnits(4096);

        for (frames, 0..) |*frame, i| {
            const data = try allocator.alignedAlloc(u8, page_alignment, page_size);
            frame.* = BufferFrame{
                .page_id = NULL_PAGE,
                .data = data,
                .pin_count = std.atomic.Value(u32).init(0),
                .dirty = false,
                .usage_count = 0,
                .latch = RwLatch{},
            };
            try free_list.append(allocator, @intCast(i));
            initialized_count += 1;
        }

        return Self{
            .frames = frames,
            .page_table = std.AutoHashMap(PageId, FrameId).init(allocator),
            .free_list = free_list,
            .clock_hand = 0,
            .pm = pm,
            .allocator = allocator,
            .mutex = .{},
            .frame_count = frame_count,
        };
    }

    /// Close the buffer pool, flushing all dirty pages.
    /// Returns an error if flushing fails - data may not be persisted.
    /// Call this before deinit() if you need durability guarantees.
    pub fn close(self: *Self) BufferPoolError!void {
        _ = try self.flushAll();
    }

    /// Clean up the buffer pool, flushing dirty pages (best effort).
    /// For guaranteed durability, call close() first and handle errors.
    pub fn deinit(self: *Self) void {
        // Flush all dirty pages before cleanup (best effort)
        _ = self.flushAll() catch 0;

        // Free all frame data buffers
        for (self.frames) |*frame| {
            self.allocator.free(frame.data);
        }
        for (self.overflow.items) |slot| {
            if (slot) |frame| {
                self.allocator.free(frame.data);
                self.allocator.destroy(frame);
            }
        }
        self.overflow.deinit(self.allocator);

        self.allocator.free(self.frames);
        self.page_table.deinit();
        self.free_list.deinit(self.allocator);
    }

    /// Fetch a page into the buffer pool and pin it.
    /// The page is latched according to the specified mode.
    /// Caller must call unpinPage when done with the page.
    pub fn fetchPage(self: *Self, page_id: PageId, mode: LatchMode) BufferPoolError!*BufferFrame {
        if (page_id == NULL_PAGE) {
            return BufferPoolError.InvalidPageId;
        }

        self.mutex.lock();

        // 1. Check if page is already in buffer pool
        if (self.page_table.get(page_id)) |frame_id| {
            const frame = self.frameAt(frame_id);

            // Increment pin count
            _ = frame.pin_count.fetchAdd(1, .monotonic);

            // Set usage count for Clock algorithm
            frame.usage_count = 1;

            self.mutex.unlock();

            // Acquire latch based on mode (outside mutex to avoid deadlock).
            // A fetch still reading the page from disk holds it exclusively,
            // so this waits for the page to be in the frame.
            self.acquireLatch(frame, mode);
            if (frame.page_id != page_id or mode == .none) {
                @branchHint(.unlikely);
                return self.settleHit(frame, page_id, mode);
            }

            return frame;
        }

        // 2. Get a free frame (from free list or evict)
        const frame_id = self.getFreeFrame() catch |err| {
            self.mutex.unlock();
            return err;
        };
        const frame = self.frameAt(frame_id);

        // 3. Update metadata before unlocking
        frame.page_id = page_id;
        frame.dirty = false;
        frame.usage_count = 1;
        _ = frame.pin_count.fetchAdd(1, .monotonic);
        self.page_table.put(page_id, frame_id) catch {
            self.mutex.unlock();
            return BufferPoolError.IoError;
        };

        // The page is in the table before its bytes are in the frame, and a
        // fetch of the same page that found it there read whatever the frame
        // held before. The frame is latched exclusively until the read is
        // done, and such a fetch waits on the latch. Nobody else holds the
        // latch of a frame that was free or evicted, so this takes it at once.
        const latched = frame.latch.tryAcquireExclusive();
        std.debug.assert(latched);

        self.mutex.unlock();

        // 4. Read page from disk (outside mutex)
        self.pm.readPage(page_id, frame.data) catch |err| {
            // On read failure, clean up. A fetch waiting on the latch has
            // pinned the frame too; it sees the frame no longer holds the
            // page and lets go, and the frame is then reclaimed like any
            // other unpinned one.
            self.mutex.lock();
            self.forgetPage(page_id);
            frame.page_id = NULL_PAGE;
            const others = frame.pin_count.fetchSub(1, .monotonic) - 1;
            frame.latch.releaseExclusive();
            if (others == 0) self.returnFrame(frame_id);
            self.mutex.unlock();

            return switch (err) {
                PageManagerError.ChecksumMismatch => BufferPoolError.ChecksumMismatch,
                else => BufferPoolError.IoError,
            };
        };

        // 5. Keep the latch the caller asked for.
        switch (mode) {
            .exclusive => {},
            .shared => {
                frame.latch.releaseExclusive();
                self.acquireLatch(frame, .shared);
            },
            .none => frame.latch.releaseExclusive(),
        }

        return frame;
    }

    /// The rare ends of a fetch that found its page in the table: the read
    /// that was bringing the page in failed, so the frame no longer holds it;
    /// or the caller takes no latch, and so has not yet waited for a read
    /// still in progress.
    fn settleHit(self: *Self, frame: *BufferFrame, page_id: PageId, mode: LatchMode) BufferPoolError!*BufferFrame {
        if (mode == .none) {
            self.acquireLatch(frame, .shared);
            frame.latch.releaseShared();
        }
        if (frame.page_id != page_id) {
            self.releaseLatch(frame);
            _ = frame.pin_count.fetchSub(1, .monotonic);
            return self.fetchPage(page_id, mode);
        }
        return frame;
    }

    /// Unpin a page and optionally mark it dirty.
    /// Releases the latch held on the frame.
    pub fn unpinPage(self: *Self, frame: *BufferFrame, dirty: bool) void {
        if (dirty) {
            self.markDirty(frame);
        }

        // Release latch (we don't know which mode, but release works for both)
        // The caller is responsible for knowing which mode they acquired
        // For simplicity, we release based on current state
        self.releaseLatch(frame);

        // Decrement pin count
        const prev = frame.pin_count.fetchSub(1, .monotonic);
        std.debug.assert(prev > 0);
    }

    /// Flush a specific page to disk if it's dirty.
    ///
    /// One page cannot go to the file on its own, so this flushes everything
    /// when the page is dirty.
    pub fn flushPage(self: *Self, page_id: PageId) !void {
        self.mutex.lock();
        const frame_id = self.page_table.get(page_id) orelse {
            self.mutex.unlock();
            return; // Page not in buffer pool
        };
        const dirty = self.frameAt(frame_id).dirty;
        self.mutex.unlock();

        if (dirty) _ = try self.flushAll();
    }

    /// Write every dirty page, the header, and the pages freed since the
    /// last flush to the database file as one atomic step. Returns how many
    /// pages were written.
    ///
    /// The pages are copied to the journal and synced before any is written
    /// in place. A crash during the in-place writes is finished from the
    /// journal at the next open; a crash before that leaves the file as it
    /// was. The caller must be between operations: every B+Tree a page
    /// belongs to has to be whole in memory, because the image written here
    /// is what recovery replays the log onto.
    ///
    /// Anything the pages depend on in the write-ahead log must be synced
    /// before this is called.
    pub fn flushAll(self: *Self) !u32 {
        // Pages freed since the last flush join the freelist now: each links
        // to the next and the last to the head the file already has.
        const frees = self.pm.deferredFrees();
        for (frees, 0..) |page_id, i| {
            const next = if (i + 1 < frees.len) frees[i + 1] else self.pm.getHeader().freelist_page;
            const frame = try self.frameForFreedPage(page_id);
            PageManager.formatFreePage(frame.data, next);
            self.markDirty(frame);
        }

        var dirty: std.ArrayListUnmanaged(*BufferFrame) = .empty;
        defer dirty.deinit(self.allocator);
        {
            self.mutex.lock();
            defer self.mutex.unlock();
            for (self.frames) |*frame| {
                if (frame.page_id != NULL_PAGE and frame.dirty) {
                    dirty.append(self.allocator, frame) catch return BufferPoolError.IoError;
                }
            }
            for (self.overflow.items) |slot| {
                const frame = slot orelse continue;
                if (frame.page_id != NULL_PAGE and frame.dirty) {
                    dirty.append(self.allocator, frame) catch return BufferPoolError.IoError;
                }
            }
        }

        if (dirty.items.len == 0 and !self.pm.hasPendingChanges()) {
            self.dirty_count = 0;
            self.mutex.lock();
            self.all_frames_dirty = false;
            self.mutex.unlock();
            return 0;
        }

        std.mem.sort(*BufferFrame, dirty.items, {}, struct {
            fn lessThan(_: void, a: *BufferFrame, b: *BufferFrame) bool {
                return a.page_id < b.page_id;
            }
        }.lessThan);

        try self.writeAtomically(dirty.items);

        for (dirty.items) |frame| frame.dirty = false;
        self.dirty_count = 0;
        self.pm.completeFlush();
        // Only now, with every latch released, may frames be given back.
        self.releaseIdleOverflow();

        return @intCast(dirty.items.len);
    }

    /// Journal the header and these pages, then write them in place and sync.
    fn writeAtomically(self: *Self, dirty: []const *BufferFrame) !void {
        var latched: usize = 0;
        defer for (dirty[0..latched]) |frame| frame.latch.releaseShared();
        for (dirty) |frame| {
            while (!frame.latch.tryAcquireShared()) {
                std.atomic.spinLoopHint();
            }
            latched += 1;
            self.pm.sealPage(frame.data);
        }

        const page_size = self.pm.getPageSize();
        const page_alignment = comptime Alignment.fromByteUnits(4096);
        const header_image = self.allocator.alignedAlloc(u8, page_alignment, page_size) catch {
            return BufferPoolError.IoError;
        };
        defer self.allocator.free(header_image);
        self.pm.headerImage(header_image);

        if (self.journal) |journal| {
            const images = self.allocator.alloc(journal_mod.PageImage, dirty.len + 1) catch {
                return BufferPoolError.IoError;
            };
            defer self.allocator.free(images);
            images[0] = .{ .page_id = 0, .data = header_image };
            for (dirty, 1..) |frame, i| {
                images[i] = .{ .page_id = frame.page_id, .data = frame.data };
            }
            journal.write(self.pm.getHeader().file_uuid, page_size, images) catch {
                return BufferPoolError.IoError;
            };
        }

        self.pm.writeSealed(0, header_image) catch return BufferPoolError.IoError;
        for (dirty) |frame| {
            self.pm.writeSealed(frame.page_id, frame.data) catch return BufferPoolError.IoError;
        }
        self.pm.sync() catch return BufferPoolError.IoError;

        // The pages are durable in place. A journal that fails to clear is
        // applied again at the next open, to no effect.
        if (self.journal) |journal| journal.clear() catch {};
    }

    /// Free a page: it becomes an empty free page in the pool, and joins the
    /// freelist in the file at the next flush.
    pub fn freePage(self: *Self, page_id: PageId) !void {
        self.pm.freePage(page_id) catch |err| return switch (err) {
            PageManagerError.InvalidPageId => BufferPoolError.InvalidPageId,
            else => BufferPoolError.IoError,
        };
        // Whoever is handed this page next finds a blank free page, not what
        // its last owner left in it.
        const frame = try self.frameForFreedPage(page_id);
        PageManager.formatFreePage(frame.data, NULL_PAGE);
        self.markDirty(frame);
    }

    /// Forget every unwritten change. For a database handle that has decided
    /// its in-memory state must not reach the file.
    pub fn abandonDirty(self: *Self) void {
        for (self.frames) |*frame| frame.dirty = false;
        for (self.overflow.items) |slot| {
            if (slot) |frame| frame.dirty = false;
        }
        self.dirty_count = 0;
        self.mutex.lock();
        self.all_frames_dirty = false;
        self.mutex.unlock();
    }

    /// Whether anything holds a page. Nothing pinned means no B+Tree
    /// operation is in flight.
    pub fn hasPinnedFrames(self: *Self) bool {
        self.mutex.lock();
        defer self.mutex.unlock();
        for (self.frames) |*frame| {
            if (frame.isPinned()) return true;
        }
        for (self.overflow.items) |slot| {
            const frame = slot orelse continue;
            if (frame.isPinned()) return true;
        }
        return false;
    }

    /// Roughly how many frames hold unwritten changes.
    pub fn dirtyCount(self: *const Self) usize {
        return self.dirty_count;
    }

    /// Flush and evict every cached page, then remove the physical free tail.
    ///
    /// Holding the page-table mutex across eviction and truncation prevents a
    /// concurrent fetch from installing a frame for a page that is about to
    /// disappear. Any pinned frame rejects the maintenance operation.
    pub fn truncateFreeTail(self: *Self) !page_manager.TruncateStats {
        {
            self.mutex.lock();
            defer self.mutex.unlock();
            for (self.frames) |*frame| {
                if (frame.page_id != NULL_PAGE and frame.isPinned()) {
                    return BufferPoolError.BufferPoolFull;
                }
            }
            for (self.overflow.items) |slot| {
                const frame = slot orelse continue;
                if (frame.page_id != NULL_PAGE and frame.isPinned()) {
                    return BufferPoolError.BufferPoolFull;
                }
            }
        }

        _ = try self.flushAll();

        self.mutex.lock();
        defer self.mutex.unlock();

        for (self.frames, 0..) |*frame, frame_index| {
            if (frame.page_id == NULL_PAGE) continue;
            frame.page_id = NULL_PAGE;
            frame.usage_count = 0;
            self.free_list.append(self.allocator, @intCast(frame_index)) catch return BufferPoolError.IoError;
        }
        for (self.overflow.items) |*slot| {
            const frame = slot.* orelse continue;
            self.allocator.free(frame.data);
            self.allocator.destroy(frame);
            slot.* = null;
        }
        self.overflow.clearRetainingCapacity();
        self.overflow_holes = 0;
        // Every page is gone, so the table starts over, without tombstones
        // and at its usual size.
        self.page_table.clearRetainingCapacity();
        self.resizePageTable();

        return self.pm.truncateFreeTail() catch return BufferPoolError.IoError;
    }

    /// Get a page directly without pinning (for internal use).
    /// Returns null if page is not in the buffer pool.
    pub fn getPageIfPresent(self: *Self, page_id: PageId) ?*BufferFrame {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.page_table.get(page_id)) |frame_id| {
            return self.frameAt(frame_id);
        }
        return null;
    }

    /// Get statistics about the buffer pool.
    pub fn getStats(self: *Self) BufferPoolStats {
        self.mutex.lock();
        defer self.mutex.unlock();

        var stats = BufferPoolStats{
            .total_frames = self.frame_count,
            .free_frames = self.free_list.items.len,
            .pinned_frames = 0,
            .dirty_frames = 0,
        };

        for (self.frames) |*frame| {
            if (frame.isPinned()) {
                stats.pinned_frames += 1;
            }
            if (frame.dirty) {
                stats.dirty_frames += 1;
            }
        }
        for (self.overflow.items) |slot| {
            const frame = slot orelse continue;
            stats.total_frames += 1;
            if (frame.isPinned()) {
                stats.pinned_frames += 1;
            }
            if (frame.dirty) {
                stats.dirty_frames += 1;
            }
        }

        return stats;
    }

    // ========================================================================
    // Internal methods
    // ========================================================================

    /// The frame with this id, in the pool proper or beyond it.
    fn frameAt(self: *Self, frame_id: FrameId) *BufferFrame {
        if (frame_id < self.frame_count) return &self.frames[frame_id];
        return self.overflow.items[frame_id - self.frame_count].?;
    }

    fn markDirty(self: *Self, frame: *BufferFrame) void {
        if (!frame.dirty) {
            frame.dirty = true;
            self.dirty_count += 1;
        }
    }

    /// Hand back a frame that turned out not to be needed. Caller must hold
    /// mutex.
    fn returnFrame(self: *Self, frame_id: FrameId) void {
        if (frame_id < self.frame_count) {
            self.free_list.append(self.allocator, frame_id) catch {};
            // Whatever it is given to next starts clean.
            self.all_frames_dirty = false;
            return;
        }
        const slot = &self.overflow.items[frame_id - self.frame_count];
        if (slot.*) |frame| {
            self.allocator.free(frame.data);
            self.allocator.destroy(frame);
            slot.* = null;
            self.overflow_holes += 1;
        }
    }

    /// The frame for a page that has been freed, without reading the page:
    /// what is on disk is its old content, which is about to be replaced.
    fn frameForFreedPage(self: *Self, page_id: PageId) BufferPoolError!*BufferFrame {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.page_table.get(page_id)) |frame_id| return self.frameAt(frame_id);

        const frame_id = try self.getFreeFrame();
        const frame = self.frameAt(frame_id);
        frame.page_id = page_id;
        frame.dirty = false;
        frame.usage_count = 0;
        self.page_table.put(page_id, frame_id) catch {
            frame.page_id = NULL_PAGE;
            self.returnFrame(frame_id);
            return BufferPoolError.IoError;
        };
        return frame;
    }

    /// Give frames beyond the pool's size back once nothing holds them.
    /// Called after a flush, when none of them is dirty.
    fn releaseIdleOverflow(self: *Self) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        // The flush cleaned every frame it could latch.
        self.all_frames_dirty = false;

        var released: usize = 0;
        for (self.overflow.items) |*slot| {
            const frame = slot.* orelse continue;
            if (frame.isPinned() or frame.dirty) continue;
            // Removed directly: the table is rebuilt below, which clears
            // these tombstones with the rest.
            if (frame.page_id != NULL_PAGE) _ = self.page_table.remove(frame.page_id);
            self.allocator.free(frame.data);
            self.allocator.destroy(frame);
            slot.* = null;
            released += 1;
        }
        while (self.overflow.items.len > 0 and self.overflow.items[self.overflow.items.len - 1] == null) {
            _ = self.overflow.pop();
        }
        if (self.overflow.items.len == 0) self.overflow.clearAndFree(self.allocator);
        self.overflow_holes = 0;
        for (self.overflow.items) |slot| {
            if (slot == null) self.overflow_holes += 1;
        }
        if (released > 0) self.resizePageTable();
    }

    /// Take a page out of the page table. Caller must hold mutex.
    fn forgetPage(self: *Self, page_id: PageId) void {
        if (!self.page_table.remove(page_id)) return;
        self.page_table_removals += 1;
        // A quarter of the slots at most turn to tombstones between
        // rehashes. The table is at most 80% full, so this leaves a free
        // slot in every few for a probe to stop at, and the rehash, which
        // walks every slot, costs a few slot visits per removal.
        if (self.page_table_removals * 4 >= self.page_table.capacity()) {
            self.page_table.rehash();
            self.page_table_removals = 0;
        }
    }

    /// Rebuild the page table at the size its entries need, or the pool's
    /// frame count if that is more. The frames beyond the pool's size grow
    /// it during a large load and it never shrinks on its own; a table many
    /// times larger than its entries, full of tombstones, makes every miss
    /// slow. Caller must hold mutex.
    fn resizePageTable(self: *Self) void {
        const wanted: u32 = @intCast(@max(self.frame_count, self.page_table.count()));
        var table = std.AutoHashMap(PageId, FrameId).init(self.allocator);
        if (table.ensureTotalCapacity(wanted)) |_| {
            if (table.capacity() < self.page_table.capacity()) {
                var it = self.page_table.iterator();
                while (it.next()) |entry| table.putAssumeCapacityNoClobber(entry.key_ptr.*, entry.value_ptr.*);
                self.page_table.deinit();
                self.page_table = table;
                self.page_table_removals = 0;
                return;
            }
        } else |_| {}
        // Already the right size, or no memory for a new one: clear the
        // tombstones in place.
        table.deinit();
        self.page_table.rehash();
        self.page_table_removals = 0;
    }

    /// How many slots of the page table have never held a key since it was
    /// last rehashed or rebuilt. A lookup of a page that is not in the pool
    /// probes until it reaches one. For tests.
    pub fn pageTableFreeSlots(self: *Self) usize {
        self.mutex.lock();
        defer self.mutex.unlock();
        const capacity = self.page_table.capacity();
        const metadata = self.page_table.unmanaged.metadata orelse return 0;
        var free: usize = 0;
        for (metadata[0..capacity]) |slot| {
            // A slot never used is all zero bits; a tombstone is not.
            if (@as(u8, @bitCast(slot)) == 0) free += 1;
        }
        return free;
    }

    /// Grow the pool by one frame. Caller must hold mutex.
    fn addOverflowFrame(self: *Self) BufferPoolError!FrameId {
        const page_alignment = comptime Alignment.fromByteUnits(4096);
        const frame = self.allocator.create(BufferFrame) catch return BufferPoolError.BufferPoolFull;
        errdefer self.allocator.destroy(frame);
        const data = self.allocator.alignedAlloc(u8, page_alignment, self.pm.getPageSize()) catch {
            return BufferPoolError.BufferPoolFull;
        };
        errdefer self.allocator.free(data);
        frame.* = BufferFrame{
            .page_id = NULL_PAGE,
            .data = data,
            .pin_count = std.atomic.Value(u32).init(0),
            .dirty = false,
            .usage_count = 0,
            .latch = RwLatch{},
        };

        if (self.overflow_holes > 0) {
            for (self.overflow.items, 0..) |*slot, i| {
                if (slot.* == null) {
                    slot.* = frame;
                    self.overflow_holes -= 1;
                    return @intCast(self.frame_count + i);
                }
            }
        }
        self.overflow.append(self.allocator, frame) catch return BufferPoolError.BufferPoolFull;
        return @intCast(self.frame_count + self.overflow.items.len - 1);
    }

    /// Get a free frame, either from free list or by eviction.
    /// Caller must hold mutex.
    ///
    /// Only a clean frame is evicted. A dirty one would have to be written
    /// first, and a page written on its own tears the image in the file.
    /// When the frames that could go are all dirty, the pool grows instead,
    /// until the next flush cleans them.
    fn getFreeFrame(self: *Self) BufferPoolError!FrameId {
        // Try free list first
        if (self.free_list.pop()) |frame_id| {
            return frame_id;
        }

        if (self.all_frames_dirty) return self.addOverflowFrame();

        // Clock eviction algorithm
        var iterations: usize = 0;
        const max_iterations = self.frame_count * 2; // Allow two full rotations
        var passed_over_dirty = false;
        var passed_over_held_clean: usize = 0;

        while (iterations < max_iterations) {
            const frame = &self.frames[self.clock_hand];
            const frame_id: FrameId = @intCast(self.clock_hand);

            // Skip if pinned
            if (frame.pin_count.load(.monotonic) > 0) {
                if (!frame.dirty) passed_over_held_clean += 1;
                self.advanceClock();
                iterations += 1;
                continue;
            }

            if (frame.dirty) {
                passed_over_dirty = true;
                self.advanceClock();
                iterations += 1;
                continue;
            }

            // Second chance: check usage count
            if (frame.usage_count > 0) {
                frame.usage_count -= 1;
                self.advanceClock();
                iterations += 1;
                continue;
            }

            // Found a clean victim - evict it
            self.forgetPage(frame.page_id);
            frame.page_id = NULL_PAGE;

            self.advanceClock();
            return frame_id;
        }

        // Every frame pinned is the caller holding too much at once, as it
        // always was. Frames that are merely dirty are a different matter.
        if (!passed_over_dirty) return BufferPoolError.BufferPoolFull;
        // Frames pinned and clean come free later. A few are the pages the
        // caller holds while it reads the next one, as a B+Tree does on
        // nearly every step; waiting for those would send every miss until
        // the flush round the whole pool twice, so the pool grows instead
        // and they are evicted after the flush. Many are readers at work,
        // and worth the next sweep.
        if (passed_over_held_clean * 8 < max_iterations) self.all_frames_dirty = true;
        return self.addOverflowFrame();
    }

    /// Advance the clock hand to the next frame.
    fn advanceClock(self: *Self) void {
        self.clock_hand = (self.clock_hand + 1) % self.frame_count;
    }

    /// Acquire latch based on mode.
    fn acquireLatch(self: *Self, frame: *BufferFrame, mode: LatchMode) void {
        _ = self;
        switch (mode) {
            .none => {},
            .shared => {
                while (!frame.latch.tryAcquireShared()) {
                    std.atomic.spinLoopHint();
                }
            },
            .exclusive => {
                while (!frame.latch.tryAcquireExclusive()) {
                    std.atomic.spinLoopHint();
                }
            },
        }
    }

    /// Release latch (tries exclusive first, then shared).
    fn releaseLatch(self: *Self, frame: *BufferFrame) void {
        _ = self;
        // We need to know which type of latch was held
        // Since RwLatch tracks state, check if writer bit is set
        const state = frame.latch.state.load(.monotonic);
        const WRITER_BIT: u32 = 1 << 31;
        if (state & WRITER_BIT != 0) {
            frame.latch.releaseExclusive();
        } else if (state > 0) {
            frame.latch.releaseShared();
        }
        // If state is 0, no latch was held (LatchMode.none)
    }
};

/// Buffer pool statistics
pub const BufferPoolStats = struct {
    total_frames: usize,
    free_frames: usize,
    pinned_frames: usize,
    dirty_frames: usize,
};

// ============================================================================
// Tests
// ============================================================================

test "buffer pool init and deinit" {
    const allocator = std.testing.allocator;

    const vfs = @import("vfs.zig");
    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const path = "/tmp/lattice_bp_test_init.db";
    vfs_impl.delete(path) catch {};

    var pm = try PageManager.init(allocator, vfs_impl, path, .{ .create = true });
    defer {
        pm.deinit();
        vfs_impl.delete(path) catch {};
    }

    // Create buffer pool with 16KB (4 frames)
    var bp = try BufferPool.init(allocator, &pm, 16384);
    defer bp.deinit();

    const stats = bp.getStats();
    try std.testing.expectEqual(@as(usize, 4), stats.total_frames);
    try std.testing.expectEqual(@as(usize, 4), stats.free_frames);
}

test "buffer pool fetch and unpin" {
    const allocator = std.testing.allocator;

    const vfs = @import("vfs.zig");
    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const path = "/tmp/lattice_bp_test_fetch.db";
    vfs_impl.delete(path) catch {};

    var pm = try PageManager.init(allocator, vfs_impl, path, .{ .create = true });
    defer {
        pm.deinit();
        vfs_impl.delete(path) catch {};
    }

    // Allocate a page to fetch
    const page_id = try pm.allocatePage();

    var bp = try BufferPool.init(allocator, &pm, 16384);
    defer bp.deinit();

    // Fetch the page
    const frame = try bp.fetchPage(page_id, .exclusive);
    try std.testing.expect(frame.isPinned());
    try std.testing.expectEqual(page_id, frame.page_id);

    // Unpin without marking dirty
    bp.unpinPage(frame, false);
    try std.testing.expect(!frame.isPinned());
}

test "buffer pool dirty page tracking" {
    const allocator = std.testing.allocator;

    const vfs = @import("vfs.zig");
    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const path = "/tmp/lattice_bp_test_dirty.db";
    vfs_impl.delete(path) catch {};

    var pm = try PageManager.init(allocator, vfs_impl, path, .{ .create = true });
    defer {
        pm.deinit();
        vfs_impl.delete(path) catch {};
    }

    const page_id = try pm.allocatePage();

    var bp = try BufferPool.init(allocator, &pm, 16384);
    defer bp.deinit();

    // Fetch and modify
    const frame = try bp.fetchPage(page_id, .exclusive);

    // Write some data
    const test_data = "Hello, Buffer Pool!";
    @memcpy(frame.data[8..][0..test_data.len], test_data);

    // Unpin as dirty
    bp.unpinPage(frame, true);

    var stats = bp.getStats();
    try std.testing.expectEqual(@as(usize, 1), stats.dirty_frames);

    // Flush
    try bp.flushPage(page_id);

    stats = bp.getStats();
    try std.testing.expectEqual(@as(usize, 0), stats.dirty_frames);
}

test "buffer pool eviction" {
    const allocator = std.testing.allocator;

    const vfs = @import("vfs.zig");
    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const path = "/tmp/lattice_bp_test_evict.db";
    vfs_impl.delete(path) catch {};

    var pm = try PageManager.init(allocator, vfs_impl, path, .{ .create = true });
    defer {
        pm.deinit();
        vfs_impl.delete(path) catch {};
    }

    // Create pool with only 2 frames
    var bp = try BufferPool.init(allocator, &pm, 8192);
    defer bp.deinit();

    // Allocate 3 pages
    const page1 = try pm.allocatePage();
    const page2 = try pm.allocatePage();
    const page3 = try pm.allocatePage();

    // Fetch page1, unpin it
    const frame1 = try bp.fetchPage(page1, .shared);
    bp.unpinPage(frame1, false);

    // Fetch page2, unpin it
    const frame2 = try bp.fetchPage(page2, .shared);
    bp.unpinPage(frame2, false);

    // Now pool is full, fetching page3 should evict one of the previous
    const frame3 = try bp.fetchPage(page3, .shared);
    try std.testing.expectEqual(page3, frame3.page_id);
    bp.unpinPage(frame3, false);

    // One of page1 or page2 should have been evicted
    const stats = bp.getStats();
    try std.testing.expectEqual(@as(usize, 0), stats.free_frames);
}

test "buffer pool full error" {
    const allocator = std.testing.allocator;

    const vfs = @import("vfs.zig");
    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const path = "/tmp/lattice_bp_test_full.db";
    vfs_impl.delete(path) catch {};

    var pm = try PageManager.init(allocator, vfs_impl, path, .{ .create = true });
    defer {
        pm.deinit();
        vfs_impl.delete(path) catch {};
    }

    // Create pool with only 2 frames
    var bp = try BufferPool.init(allocator, &pm, 8192);
    defer bp.deinit();

    const page1 = try pm.allocatePage();
    const page2 = try pm.allocatePage();
    const page3 = try pm.allocatePage();

    // Fetch and keep pinned
    _ = try bp.fetchPage(page1, .shared);
    _ = try bp.fetchPage(page2, .shared);

    // Pool is full with all pinned - should fail
    const result = bp.fetchPage(page3, .shared);
    try std.testing.expectError(BufferPoolError.BufferPoolFull, result);
}

test "buffer pool page caching" {
    const allocator = std.testing.allocator;

    const vfs = @import("vfs.zig");
    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const path = "/tmp/lattice_bp_test_cache.db";
    vfs_impl.delete(path) catch {};

    var pm = try PageManager.init(allocator, vfs_impl, path, .{ .create = true });
    defer {
        pm.deinit();
        vfs_impl.delete(path) catch {};
    }

    var bp = try BufferPool.init(allocator, &pm, 16384);
    defer bp.deinit();

    const page_id = try pm.allocatePage();

    // First fetch
    const frame1 = try bp.fetchPage(page_id, .exclusive);
    const frame_ptr = frame1;
    bp.unpinPage(frame1, false);

    // Second fetch should return same frame (cached)
    const frame2 = try bp.fetchPage(page_id, .shared);
    try std.testing.expectEqual(frame_ptr, frame2);
    bp.unpinPage(frame2, false);
}

test "buffer pool grows rather than write a dirty page out on its own" {
    const allocator = std.testing.allocator;

    const vfs = @import("vfs.zig");
    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const path = "/tmp/lattice_bp_test_no_steal.db";
    vfs_impl.delete(path) catch {};

    var pm = try PageManager.init(allocator, vfs_impl, path, .{ .create = true });
    defer {
        pm.deinit();
        vfs_impl.delete(path) catch {};
    }

    // Four frames, eight pages, every one dirtied.
    var bp = try BufferPool.init(allocator, &pm, 4 * 4096);
    defer bp.deinit();

    var pages: [8]PageId = undefined;
    for (&pages, 0..) |*page_id, i| {
        page_id.* = try pm.allocatePage();
        const frame = try bp.fetchPage(page_id.*, .exclusive);
        @memset(frame.data[@sizeOf(PageHeader)..], @as(u8, @intCast(0x10 + i)));
        bp.unpinPage(frame, true);
    }

    // None of them was pushed to the file to make room: the file still holds
    // the blank pages they were allocated as, and the pool holds all eight.
    const on_disk = try allocator.alignedAlloc(u8, comptime Alignment.fromByteUnits(4096), 4096);
    defer allocator.free(on_disk);
    for (pages) |page_id| {
        try pm.readPage(page_id, on_disk);
        try std.testing.expectEqual(@as(u8, 0), on_disk[@sizeOf(PageHeader)]);
    }
    try std.testing.expectEqual(@as(usize, 8), bp.getStats().total_frames);
    try std.testing.expectEqual(@as(usize, 8), bp.getStats().dirty_frames);

    // One flush writes them all, and the pool goes back to its own size.
    try std.testing.expectEqual(@as(u32, 8), try bp.flushAll());
    for (pages, 0..) |page_id, i| {
        try pm.readPage(page_id, on_disk);
        try std.testing.expectEqual(@as(u8, @intCast(0x10 + i)), on_disk[@sizeOf(PageHeader)]);
    }
    try std.testing.expectEqual(@as(usize, 4), bp.getStats().total_frames);
    try std.testing.expectEqual(@as(usize, 0), bp.getStats().dirty_frames);

    // With every frame clean, eviction works as it always did.
    for (pages) |page_id| {
        const frame = try bp.fetchPage(page_id, .shared);
        bp.unpinPage(frame, false);
    }
    try std.testing.expectEqual(@as(usize, 4), bp.getStats().total_frames);
}

test "buffer pool: a freed page reaches the file's freelist only with a flush" {
    const allocator = std.testing.allocator;

    const vfs = @import("vfs.zig");
    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const path = "/tmp/lattice_bp_test_deferred_free.db";
    vfs_impl.delete(path) catch {};

    var pm = try PageManager.init(allocator, vfs_impl, path, .{ .create = true });
    defer {
        pm.deinit();
        vfs_impl.delete(path) catch {};
    }
    var bp = try BufferPool.init(allocator, &pm, 16 * 4096);
    defer bp.deinit();

    // Two pages with content, flushed: this is the image in the file.
    const first = try pm.allocatePage();
    const second = try pm.allocatePage();
    for ([_]PageId{ first, second }) |page_id| {
        const frame = try bp.fetchPage(page_id, .exclusive);
        const header: *PageHeader = @ptrCast(@alignCast(frame.data.ptr));
        header.* = PageHeader.init(.btree_leaf);
        @memset(frame.data[@sizeOf(PageHeader)..], 0x5A);
        bp.unpinPage(frame, true);
    }
    _ = try bp.flushAll();

    const on_disk = try allocator.alignedAlloc(u8, comptime Alignment.fromByteUnits(4096), 4096);
    defer allocator.free(on_disk);

    // Free them. The file is not told: both pages are still what the image
    // says they are, and its freelist is still empty.
    try bp.freePage(first);
    try bp.freePage(second);
    try std.testing.expectError(BufferPoolError.InvalidPageId, bp.freePage(second));
    for ([_]PageId{ first, second }) |page_id| {
        try pm.readPage(page_id, on_disk);
        const header: *const PageHeader = @ptrCast(@alignCast(on_disk.ptr));
        try std.testing.expectEqual(page.PageType.btree_leaf, header.page_type);
        try std.testing.expectEqual(@as(u8, 0x5A), on_disk[@sizeOf(PageHeader)]);
    }
    try std.testing.expectEqual(NULL_PAGE, pm.getHeader().freelist_page);

    // A freed page is handed straight back out, blank.
    const reused = try pm.allocatePage();
    try std.testing.expectEqual(second, reused);
    {
        const frame = try bp.fetchPage(reused, .exclusive);
        defer bp.unpinPage(frame, true);
        const header: *PageHeader = @ptrCast(@alignCast(frame.data.ptr));
        try std.testing.expectEqual(page.PageType.free, header.page_type);
        try std.testing.expectEqual(@as(u8, 0), frame.data[@sizeOf(PageHeader) + 8]);
        header.* = PageHeader.init(.btree_leaf);
    }

    // The flush puts the page that stayed free on the freelist.
    _ = try bp.flushAll();
    try std.testing.expectEqual(first, pm.getHeader().freelist_page);
    try pm.readPage(first, on_disk);
    const freed_header: *const PageHeader = @ptrCast(@alignCast(on_disk.ptr));
    try std.testing.expectEqual(page.PageType.free, freed_header.page_type);
    try std.testing.expectEqual(NULL_PAGE, std.mem.readInt(u32, on_disk[8..12], .little));

    // And from there it is allocated like any other free page.
    try std.testing.expectEqual(first, try pm.allocatePage());
}
