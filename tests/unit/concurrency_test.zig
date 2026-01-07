//! Behavioral tests for concurrency primitives.
//!
//! These tests verify locking and latching behavior under concurrent access:
//! - RwLatch reader-writer semantics
//! - Spinlock mutual exclusion
//! - Lock compatibility matrix correctness
//! - Multi-threaded contention scenarios

const std = @import("std");
const lattice = @import("lattice");

const locking = lattice.concurrency.locking;
const RwLatch = locking.RwLatch;
const Spinlock = locking.Spinlock;
const LockMode = locking.LockMode;
const LockCompatibility = locking.LockCompatibility;
const checkLockCompatibility = locking.checkLockCompatibility;

// ============================================================================
// RwLatch Single-Threaded Tests
// ============================================================================

test "rwlatch: multiple shared acquisitions succeed" {
    var latch = RwLatch{};

    // Acquire multiple shared latches
    try std.testing.expect(latch.tryAcquireShared());
    try std.testing.expect(latch.tryAcquireShared());
    try std.testing.expect(latch.tryAcquireShared());

    // All should succeed - readers don't block readers
    latch.releaseShared();
    latch.releaseShared();
    latch.releaseShared();
}

test "rwlatch: exclusive blocks subsequent shared" {
    var latch = RwLatch{};

    // Acquire exclusive
    try std.testing.expect(latch.tryAcquireExclusive());

    // Shared should fail while exclusive held
    try std.testing.expect(!latch.tryAcquireShared());
    try std.testing.expect(!latch.tryAcquireShared());

    // Release exclusive
    latch.releaseExclusive();

    // Now shared should succeed
    try std.testing.expect(latch.tryAcquireShared());
    latch.releaseShared();
}

test "rwlatch: shared blocks exclusive" {
    var latch = RwLatch{};

    // Acquire shared
    try std.testing.expect(latch.tryAcquireShared());

    // Exclusive should fail while shared held
    try std.testing.expect(!latch.tryAcquireExclusive());

    // Release shared
    latch.releaseShared();

    // Now exclusive should succeed
    try std.testing.expect(latch.tryAcquireExclusive());
    latch.releaseExclusive();
}

test "rwlatch: exclusive blocks exclusive" {
    var latch = RwLatch{};

    try std.testing.expect(latch.tryAcquireExclusive());
    try std.testing.expect(!latch.tryAcquireExclusive());

    latch.releaseExclusive();

    try std.testing.expect(latch.tryAcquireExclusive());
    latch.releaseExclusive();
}

test "rwlatch: release shared allows exclusive" {
    var latch = RwLatch{};

    // Multiple readers
    try std.testing.expect(latch.tryAcquireShared());
    try std.testing.expect(latch.tryAcquireShared());

    // Writer blocked
    try std.testing.expect(!latch.tryAcquireExclusive());

    // Release one reader - still blocked
    latch.releaseShared();
    try std.testing.expect(!latch.tryAcquireExclusive());

    // Release last reader - writer can proceed
    latch.releaseShared();
    try std.testing.expect(latch.tryAcquireExclusive());
    latch.releaseExclusive();
}

// ============================================================================
// Spinlock Single-Threaded Tests
// ============================================================================

test "spinlock: acquire and release" {
    var lock = Spinlock{};

    lock.acquire();
    // Lock is held
    try std.testing.expect(!lock.tryAcquire());

    lock.release();
    // Lock is free
    try std.testing.expect(lock.tryAcquire());
    lock.release();
}

test "spinlock: tryAcquire fails when held" {
    var lock = Spinlock{};

    try std.testing.expect(lock.tryAcquire());
    try std.testing.expect(!lock.tryAcquire());
    try std.testing.expect(!lock.tryAcquire());

    lock.release();

    try std.testing.expect(lock.tryAcquire());
    lock.release();
}

// ============================================================================
// Lock Compatibility Matrix Tests
// ============================================================================

test "lock compatibility: shared with shared is compatible" {
    try std.testing.expectEqual(
        LockCompatibility.compatible,
        checkLockCompatibility(.shared, .shared),
    );
}

test "lock compatibility: shared with exclusive is incompatible" {
    try std.testing.expectEqual(
        LockCompatibility.incompatible,
        checkLockCompatibility(.shared, .exclusive),
    );
}

test "lock compatibility: exclusive blocks everything" {
    // Exclusive held blocks all other modes
    try std.testing.expectEqual(
        LockCompatibility.incompatible,
        checkLockCompatibility(.exclusive, .shared),
    );
    try std.testing.expectEqual(
        LockCompatibility.incompatible,
        checkLockCompatibility(.exclusive, .exclusive),
    );
    try std.testing.expectEqual(
        LockCompatibility.incompatible,
        checkLockCompatibility(.exclusive, .update),
    );
    try std.testing.expectEqual(
        LockCompatibility.incompatible,
        checkLockCompatibility(.exclusive, .intent_shared),
    );
    try std.testing.expectEqual(
        LockCompatibility.incompatible,
        checkLockCompatibility(.exclusive, .intent_exclusive),
    );
}

test "lock compatibility: intent locks" {
    // IS compatible with IS, IX, S, U
    try std.testing.expectEqual(
        LockCompatibility.compatible,
        checkLockCompatibility(.intent_shared, .intent_shared),
    );
    try std.testing.expectEqual(
        LockCompatibility.compatible,
        checkLockCompatibility(.intent_shared, .intent_exclusive),
    );
    try std.testing.expectEqual(
        LockCompatibility.compatible,
        checkLockCompatibility(.intent_shared, .shared),
    );

    // IX compatible with IS, IX
    try std.testing.expectEqual(
        LockCompatibility.compatible,
        checkLockCompatibility(.intent_exclusive, .intent_shared),
    );
    try std.testing.expectEqual(
        LockCompatibility.compatible,
        checkLockCompatibility(.intent_exclusive, .intent_exclusive),
    );

    // IX incompatible with S
    try std.testing.expectEqual(
        LockCompatibility.incompatible,
        checkLockCompatibility(.intent_exclusive, .shared),
    );
}

test "lock compatibility: update lock semantics" {
    // Update compatible with shared (can read alongside)
    try std.testing.expectEqual(
        LockCompatibility.compatible,
        checkLockCompatibility(.update, .shared),
    );

    // Update incompatible with exclusive
    try std.testing.expectEqual(
        LockCompatibility.incompatible,
        checkLockCompatibility(.update, .exclusive),
    );

    // Update incompatible with another update (prevents deadlock)
    try std.testing.expectEqual(
        LockCompatibility.incompatible,
        checkLockCompatibility(.update, .update),
    );
}

test "lock compatibility: full matrix verification" {
    // Verify the complete 5x5 matrix is symmetric where expected
    const modes = [_]LockMode{ .shared, .exclusive, .update, .intent_shared, .intent_exclusive };

    // S-S: compatible
    try std.testing.expectEqual(LockCompatibility.compatible, checkLockCompatibility(.shared, .shared));

    // X-anything: incompatible
    for (modes) |m| {
        try std.testing.expectEqual(LockCompatibility.incompatible, checkLockCompatibility(.exclusive, m));
    }

    // IS-IS, IS-IX, IX-IS, IX-IX: compatible (intent locks coexist)
    try std.testing.expectEqual(LockCompatibility.compatible, checkLockCompatibility(.intent_shared, .intent_shared));
    try std.testing.expectEqual(LockCompatibility.compatible, checkLockCompatibility(.intent_shared, .intent_exclusive));
    try std.testing.expectEqual(LockCompatibility.compatible, checkLockCompatibility(.intent_exclusive, .intent_shared));
    try std.testing.expectEqual(LockCompatibility.compatible, checkLockCompatibility(.intent_exclusive, .intent_exclusive));
}

// ============================================================================
// Multi-Threaded RwLatch Tests
// ============================================================================

test "rwlatch: concurrent readers" {
    var latch = RwLatch{};
    var readers_active = std.atomic.Value(u32).init(0);
    var max_concurrent = std.atomic.Value(u32).init(0);

    const num_threads = 4;
    var threads: [num_threads]std.Thread = undefined;

    for (&threads) |*t| {
        t.* = std.Thread.spawn(.{}, struct {
            fn run(l: *RwLatch, active: *std.atomic.Value(u32), max: *std.atomic.Value(u32)) void {
                for (0..100) |_| {
                    while (!l.tryAcquireShared()) {
                        std.atomic.spinLoopHint();
                    }

                    // Track concurrent readers
                    const current = active.fetchAdd(1, .seq_cst) + 1;

                    // Update max if needed
                    var old_max = max.load(.seq_cst);
                    while (current > old_max) {
                        if (max.cmpxchgWeak(old_max, current, .seq_cst, .seq_cst)) |updated| {
                            old_max = updated;
                        } else {
                            break;
                        }
                    }

                    // Small delay to increase overlap chance
                    std.atomic.spinLoopHint();

                    _ = active.fetchSub(1, .seq_cst);
                    l.releaseShared();
                }
            }
        }.run, .{ &latch, &readers_active, &max_concurrent }) catch unreachable;
    }

    for (&threads) |*t| {
        t.join();
    }

    // Should have seen multiple concurrent readers
    try std.testing.expect(max_concurrent.load(.seq_cst) > 1);
}

test "rwlatch: writer excludes readers" {
    var latch = RwLatch{};
    var writer_active = std.atomic.Value(bool).init(false);
    var violation_detected = std.atomic.Value(bool).init(false);

    const Reader = struct {
        fn run(l: *RwLatch, w_active: *std.atomic.Value(bool), violation: *std.atomic.Value(bool)) void {
            for (0..50) |_| {
                while (!l.tryAcquireShared()) {
                    std.atomic.spinLoopHint();
                }

                // Check invariant: writer should not be active while we hold shared
                if (w_active.load(.seq_cst)) {
                    violation.store(true, .seq_cst);
                }

                std.atomic.spinLoopHint();

                l.releaseShared();
            }
        }
    };

    const Writer = struct {
        fn run(l: *RwLatch, w_active: *std.atomic.Value(bool)) void {
            for (0..20) |_| {
                while (!l.tryAcquireExclusive()) {
                    std.atomic.spinLoopHint();
                }

                w_active.store(true, .seq_cst);
                std.atomic.spinLoopHint();
                std.atomic.spinLoopHint();
                w_active.store(false, .seq_cst);

                l.releaseExclusive();
            }
        }
    };

    var reader_threads: [3]std.Thread = undefined;
    var writer_thread: std.Thread = undefined;

    for (&reader_threads) |*t| {
        t.* = std.Thread.spawn(.{}, Reader.run, .{ &latch, &writer_active, &violation_detected }) catch unreachable;
    }

    writer_thread = std.Thread.spawn(.{}, Writer.run, .{ &latch, &writer_active }) catch unreachable;

    for (&reader_threads) |*t| {
        t.join();
    }
    writer_thread.join();

    // No violations should have occurred
    try std.testing.expect(!violation_detected.load(.seq_cst));
}

// ============================================================================
// Multi-Threaded Spinlock Tests
// ============================================================================

test "spinlock: mutual exclusion under contention" {
    var lock = Spinlock{};
    var counter: u64 = 0;

    const num_threads = 4;
    const increments_per_thread = 1000;
    var threads: [num_threads]std.Thread = undefined;

    for (&threads) |*t| {
        t.* = std.Thread.spawn(.{}, struct {
            fn run(l: *Spinlock, c: *u64) void {
                for (0..increments_per_thread) |_| {
                    l.acquire();
                    c.* += 1;
                    l.release();
                }
            }
        }.run, .{ &lock, &counter }) catch unreachable;
    }

    for (&threads) |*t| {
        t.join();
    }

    // If mutual exclusion works, counter should be exact
    try std.testing.expectEqual(@as(u64, num_threads * increments_per_thread), counter);
}

test "spinlock: no double acquisition" {
    var lock = Spinlock{};
    var in_critical = std.atomic.Value(u32).init(0);
    var max_in_critical = std.atomic.Value(u32).init(0);

    const num_threads = 4;
    var threads: [num_threads]std.Thread = undefined;

    for (&threads) |*t| {
        t.* = std.Thread.spawn(.{}, struct {
            fn run(l: *Spinlock, in_crit: *std.atomic.Value(u32), max_crit: *std.atomic.Value(u32)) void {
                for (0..100) |_| {
                    l.acquire();

                    const current = in_crit.fetchAdd(1, .seq_cst) + 1;

                    // Track max concurrent (should always be 1)
                    var old_max = max_crit.load(.seq_cst);
                    while (current > old_max) {
                        if (max_crit.cmpxchgWeak(old_max, current, .seq_cst, .seq_cst)) |updated| {
                            old_max = updated;
                        } else {
                            break;
                        }
                    }

                    std.atomic.spinLoopHint();

                    _ = in_crit.fetchSub(1, .seq_cst);
                    l.release();
                }
            }
        }.run, .{ &lock, &in_critical, &max_in_critical }) catch unreachable;
    }

    for (&threads) |*t| {
        t.join();
    }

    // Max should always be 1 (mutual exclusion)
    try std.testing.expectEqual(@as(u32, 1), max_in_critical.load(.seq_cst));
}

// ============================================================================
// Stress Tests
// ============================================================================

test "rwlatch: stress test mixed readers and writers" {
    var latch = RwLatch{};
    var read_count = std.atomic.Value(u64).init(0);
    var write_count = std.atomic.Value(u64).init(0);

    const num_readers = 4;
    const num_writers = 2;
    var reader_threads: [num_readers]std.Thread = undefined;
    var writer_threads: [num_writers]std.Thread = undefined;

    // Start readers
    for (&reader_threads) |*t| {
        t.* = std.Thread.spawn(.{}, struct {
            fn run(l: *RwLatch, count: *std.atomic.Value(u64)) void {
                for (0..200) |_| {
                    while (!l.tryAcquireShared()) {
                        std.atomic.spinLoopHint();
                    }
                    _ = count.fetchAdd(1, .monotonic);
                    l.releaseShared();
                }
            }
        }.run, .{ &latch, &read_count }) catch unreachable;
    }

    // Start writers
    for (&writer_threads) |*t| {
        t.* = std.Thread.spawn(.{}, struct {
            fn run(l: *RwLatch, count: *std.atomic.Value(u64)) void {
                for (0..50) |_| {
                    while (!l.tryAcquireExclusive()) {
                        std.atomic.spinLoopHint();
                    }
                    _ = count.fetchAdd(1, .monotonic);
                    l.releaseExclusive();
                }
            }
        }.run, .{ &latch, &write_count }) catch unreachable;
    }

    // Wait for all
    for (&reader_threads) |*t| {
        t.join();
    }
    for (&writer_threads) |*t| {
        t.join();
    }

    // Verify all operations completed
    try std.testing.expectEqual(@as(u64, num_readers * 200), read_count.load(.seq_cst));
    try std.testing.expectEqual(@as(u64, num_writers * 50), write_count.load(.seq_cst));
}
