//! Concurrency primitives for Lattice database.
//!
//! Provides locking and latching for thread-safe operations.

const std = @import("std");
const types = @import("../core/types.zig");

pub const PageId = types.PageId;

/// Latch mode for page-level locking
pub const LatchMode = enum {
    /// No latch held
    none,
    /// Shared latch (multiple readers)
    shared,
    /// Exclusive latch (single writer)
    exclusive,
};

/// Lock mode for transaction-level locking
pub const LockMode = enum {
    /// Shared lock (read)
    shared,
    /// Exclusive lock (write)
    exclusive,
    /// Update lock (read with intent to write)
    update,
    /// Intent shared (subtree contains shared locks)
    intent_shared,
    /// Intent exclusive (subtree contains exclusive locks)
    intent_exclusive,
};

/// Lock compatibility matrix result
pub const LockCompatibility = enum {
    compatible,
    incompatible,
    upgrade_needed,
};

/// Reader-writer latch (non-reentrant)
pub const RwLatch = struct {
    state: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),

    const WRITER_BIT: u32 = 1 << 31;
    const READER_MASK: u32 = ~WRITER_BIT;

    pub fn tryAcquireShared(self: *RwLatch) bool {
        var current = self.state.load(.acquire);
        while (true) {
            if (current & WRITER_BIT != 0) return false;
            const new_val = current + 1;
            if (self.state.cmpxchgWeak(current, new_val, .acquire, .acquire)) |updated| {
                current = updated;
            } else {
                return true;
            }
        }
    }

    pub fn releaseShared(self: *RwLatch) void {
        _ = self.state.fetchSub(1, .release);
    }

    pub fn tryAcquireExclusive(self: *RwLatch) bool {
        return self.state.cmpxchgStrong(0, WRITER_BIT, .acquire, .acquire) == null;
    }

    pub fn releaseExclusive(self: *RwLatch) void {
        self.state.store(0, .release);
    }
};

/// Spinlock for short critical sections
pub const Spinlock = struct {
    locked: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

    pub fn acquire(self: *Spinlock) void {
        while (self.locked.cmpxchgWeak(false, true, .acquire, .monotonic) != null) {
            std.atomic.spinLoopHint();
        }
    }

    pub fn release(self: *Spinlock) void {
        self.locked.store(false, .release);
    }

    pub fn tryAcquire(self: *Spinlock) bool {
        return self.locked.cmpxchgStrong(false, true, .acquire, .monotonic) == null;
    }
};

/// Page latch entry
pub const PageLatch = struct {
    page_id: PageId,
    latch: RwLatch,
    pin_count: std.atomic.Value(u32),
};

/// Lock request for deadlock detection
pub const LockRequest = struct {
    txn_id: u64,
    resource_id: u64,
    mode: LockMode,
    granted: bool,
};

/// Check lock compatibility
pub fn checkLockCompatibility(held: LockMode, requested: LockMode) LockCompatibility {
    const matrix = [_][5]LockCompatibility{
        // Requested:   S       X       U       IS      IX
        // Held: S
        .{ .compatible, .incompatible, .compatible, .compatible, .incompatible },
        // Held: X
        .{ .incompatible, .incompatible, .incompatible, .incompatible, .incompatible },
        // Held: U
        .{ .compatible, .incompatible, .incompatible, .compatible, .incompatible },
        // Held: IS
        .{ .compatible, .incompatible, .compatible, .compatible, .compatible },
        // Held: IX
        .{ .incompatible, .incompatible, .incompatible, .compatible, .compatible },
    };
    return matrix[@intFromEnum(held)][@intFromEnum(requested)];
}

test "spinlock basic" {
    var lock = Spinlock{};
    try std.testing.expect(lock.tryAcquire());
    try std.testing.expect(!lock.tryAcquire());
    lock.release();
    try std.testing.expect(lock.tryAcquire());
    lock.release();
}

test "rwlatch shared" {
    var latch = RwLatch{};
    try std.testing.expect(latch.tryAcquireShared());
    try std.testing.expect(latch.tryAcquireShared());
    latch.releaseShared();
    latch.releaseShared();
}

test "rwlatch exclusive blocks shared" {
    var latch = RwLatch{};
    try std.testing.expect(latch.tryAcquireExclusive());
    try std.testing.expect(!latch.tryAcquireShared());
    latch.releaseExclusive();
    try std.testing.expect(latch.tryAcquireShared());
    latch.releaseShared();
}
