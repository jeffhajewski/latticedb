//! MVCC Integration Tests
//!
//! Tests for Multi-Version Concurrency Control functionality,
//! verifying snapshot isolation and visibility semantics.

const std = @import("std");
const lattice = @import("lattice");

const Snapshot = lattice.Snapshot;
const VersionInfo = lattice.VersionInfo;
const VersionChain = lattice.VersionChain;
const isVisible = lattice.isVisible;
const canGarbageCollect = lattice.canGarbageCollect;
const mvcc = lattice.transaction.mvcc;

// ============================================================================
// Snapshot Isolation Tests
// ============================================================================

test "snapshot: isolation between concurrent transactions" {
    const allocator = std.testing.allocator;

    // Simulate two concurrent transactions
    // Txn 1 starts at ts=10, Txn 2 starts at ts=15

    // Txn 1's snapshot: sees nothing committed after ts=10
    var snapshot1 = Snapshot{
        .read_ts = 10,
        .active_txns = &[_]mvcc.TransactionId{ 2, 3 }, // Txn 2 and 3 are active
        .owner_txn_id = 1,
        .allocator = allocator,
    };
    _ = &snapshot1;

    // Txn 2's snapshot: sees nothing committed after ts=15
    var snapshot2 = Snapshot{
        .read_ts = 15,
        .active_txns = &[_]mvcc.TransactionId{ 1, 3 }, // Txn 1 and 3 are active
        .owner_txn_id = 2,
        .allocator = allocator,
    };
    _ = &snapshot2;

    // Version created by Txn 3, committed at ts=12
    const v1 = VersionInfo{
        .created_by = 3,
        .commit_ts = 12,
        .is_deleted = false,
        .created_ts = 11,
    };

    // Txn 1 (snapshot at ts=10) cannot see v1 (committed at ts=12)
    try std.testing.expect(!isVisible(&v1, &snapshot1));

    // Txn 2 (snapshot at ts=15) also cannot see v1 because Txn 3 was active at snapshot time
    // (Even though commit_ts=12 < read_ts=15, Txn 3 was in active list)
    try std.testing.expect(!isVisible(&v1, &snapshot2));

    // Now simulate Txn 3 committed before Txn 2 took its snapshot
    var snapshot2_after = Snapshot{
        .read_ts = 15,
        .active_txns = &[_]mvcc.TransactionId{1}, // Only Txn 1 is active now
        .owner_txn_id = 2,
        .allocator = allocator,
    };
    _ = &snapshot2_after;

    // Now Txn 2 can see v1
    try std.testing.expect(isVisible(&v1, &snapshot2_after));
}

test "snapshot: transaction sees own uncommitted changes" {
    const allocator = std.testing.allocator;

    var snapshot = Snapshot{
        .read_ts = 50,
        .active_txns = &[_]mvcc.TransactionId{},
        .owner_txn_id = 42,
        .allocator = allocator,
    };
    _ = &snapshot;

    // Uncommitted change by the same transaction
    const own_change = VersionInfo{
        .created_by = 42, // Same as snapshot owner
        .commit_ts = 0, // Not committed yet
        .is_deleted = false,
        .created_ts = 55,
    };

    // Transaction can see its own uncommitted changes
    try std.testing.expect(isVisible(&own_change, &snapshot));

    // Uncommitted change by different transaction
    const other_change = VersionInfo{
        .created_by = 99, // Different transaction
        .commit_ts = 0, // Not committed
        .is_deleted = false,
        .created_ts = 55,
    };

    // Cannot see other transaction's uncommitted changes
    try std.testing.expect(!isVisible(&other_change, &snapshot));
}

test "snapshot: deleted records are not visible" {
    const allocator = std.testing.allocator;

    var snapshot = Snapshot{
        .read_ts = 100,
        .active_txns = &[_]mvcc.TransactionId{},
        .owner_txn_id = 1,
        .allocator = allocator,
    };
    _ = &snapshot;

    // Committed deletion
    const deleted = VersionInfo{
        .created_by = 2,
        .commit_ts = 50,
        .is_deleted = true,
        .created_ts = 40,
    };

    // Deleted records are not visible through normal visibility check
    try std.testing.expect(!isVisible(&deleted, &snapshot));

    // But can be detected through isVisibleDeleted
    try std.testing.expect(mvcc.isVisibleDeleted(&deleted, &snapshot));
}

// ============================================================================
// Version Chain Tests
// ============================================================================

test "version_chain: multiple versions with correct visibility" {
    const allocator = std.testing.allocator;

    var chain = VersionChain.init(allocator);
    defer chain.deinit();

    // Add versions in chronological order (oldest first, so inserted at front)
    // Version 1: created at ts=10, committed at ts=15
    try chain.addVersion(.{
        .created_by = 1,
        .commit_ts = 15,
        .is_deleted = false,
        .created_ts = 10,
    });

    // Version 2: created at ts=20, committed at ts=25 (update to the record)
    try chain.addVersion(.{
        .created_by = 2,
        .commit_ts = 25,
        .is_deleted = false,
        .created_ts = 20,
    });

    // Version 3: created at ts=30, committed at ts=35 (another update)
    try chain.addVersion(.{
        .created_by = 3,
        .commit_ts = 35,
        .is_deleted = false,
        .created_ts = 30,
    });

    // Chain should have 3 versions, newest first
    try std.testing.expectEqual(@as(usize, 3), chain.versions.items.len);
    try std.testing.expectEqual(@as(u64, 3), chain.versions.items[0].created_by);

    // Snapshot at ts=12 sees nothing (version 1 committed at ts=15)
    var snap12 = Snapshot{
        .read_ts = 12,
        .active_txns = &[_]mvcc.TransactionId{},
        .owner_txn_id = 99,
        .allocator = allocator,
    };
    _ = &snap12;
    try std.testing.expect(chain.findVisible(&snap12) == null);

    // Snapshot at ts=20 sees version 1 (commit_ts=15)
    var snap20 = Snapshot{
        .read_ts = 20,
        .active_txns = &[_]mvcc.TransactionId{},
        .owner_txn_id = 99,
        .allocator = allocator,
    };
    _ = &snap20;
    const vis20 = chain.findVisible(&snap20);
    try std.testing.expect(vis20 != null);
    try std.testing.expectEqual(@as(u64, 1), vis20.?.created_by);

    // Snapshot at ts=30 sees version 2 (commit_ts=25)
    var snap30 = Snapshot{
        .read_ts = 30,
        .active_txns = &[_]mvcc.TransactionId{},
        .owner_txn_id = 99,
        .allocator = allocator,
    };
    _ = &snap30;
    const vis30 = chain.findVisible(&snap30);
    try std.testing.expect(vis30 != null);
    try std.testing.expectEqual(@as(u64, 2), vis30.?.created_by);

    // Snapshot at ts=100 sees version 3 (commit_ts=35)
    var snap100 = Snapshot{
        .read_ts = 100,
        .active_txns = &[_]mvcc.TransactionId{},
        .owner_txn_id = 99,
        .allocator = allocator,
    };
    _ = &snap100;
    const vis100 = chain.findVisible(&snap100);
    try std.testing.expect(vis100 != null);
    try std.testing.expectEqual(@as(u64, 3), vis100.?.created_by);
}

test "version_chain: deletion followed by recreation" {
    const allocator = std.testing.allocator;

    var chain = VersionChain.init(allocator);
    defer chain.deinit();

    // Original version
    try chain.addVersion(.{
        .created_by = 1,
        .commit_ts = 10,
        .is_deleted = false,
        .created_ts = 5,
    });

    // Deletion
    try chain.addVersion(.{
        .created_by = 2,
        .commit_ts = 20,
        .is_deleted = true,
        .created_ts = 15,
    });

    // Recreation (new record with same key)
    try chain.addVersion(.{
        .created_by = 3,
        .commit_ts = 30,
        .is_deleted = false,
        .created_ts = 25,
    });

    // Snapshot at ts=15 sees original version
    var snap15 = Snapshot{
        .read_ts = 15,
        .active_txns = &[_]mvcc.TransactionId{},
        .owner_txn_id = 99,
        .allocator = allocator,
    };
    _ = &snap15;
    const vis15 = chain.findVisible(&snap15);
    try std.testing.expect(vis15 != null);
    try std.testing.expectEqual(@as(u64, 1), vis15.?.created_by);

    // Snapshot at ts=25 sees deletion (record appears deleted)
    var snap25 = Snapshot{
        .read_ts = 25,
        .active_txns = &[_]mvcc.TransactionId{},
        .owner_txn_id = 99,
        .allocator = allocator,
    };
    _ = &snap25;
    const vis25 = chain.findVisible(&snap25);
    try std.testing.expect(vis25 == null); // Deleted, so no visible version

    // Snapshot at ts=35 sees recreated version
    var snap35 = Snapshot{
        .read_ts = 35,
        .active_txns = &[_]mvcc.TransactionId{},
        .owner_txn_id = 99,
        .allocator = allocator,
    };
    _ = &snap35;
    const vis35 = chain.findVisible(&snap35);
    try std.testing.expect(vis35 != null);
    try std.testing.expectEqual(@as(u64, 3), vis35.?.created_by);
}

// ============================================================================
// Garbage Collection Tests
// ============================================================================

test "garbage_collection: removes old versions" {
    const allocator = std.testing.allocator;

    var chain = VersionChain.init(allocator);
    defer chain.deinit();

    // Old version (can be GC'd if oldest_active > 50)
    try chain.addVersion(.{
        .created_by = 1,
        .commit_ts = 50,
        .is_deleted = false,
        .created_ts = 40,
    });

    // Medium version
    try chain.addVersion(.{
        .created_by = 2,
        .commit_ts = 100,
        .is_deleted = false,
        .created_ts = 90,
    });

    // Newest version (cannot be GC'd - it's the current version)
    try chain.addVersion(.{
        .created_by = 3,
        .commit_ts = 150,
        .is_deleted = false,
        .created_ts = 140,
    });

    try std.testing.expectEqual(@as(usize, 3), chain.versions.items.len);

    // GC with oldest_active=40: keeps all versions
    // (v1 at commit=50 >= 40, so might be needed by active transaction)
    var removed = chain.garbageCollect(40);
    try std.testing.expectEqual(@as(usize, 0), removed);
    try std.testing.expectEqual(@as(usize, 3), chain.versions.items.len);

    // GC with oldest_active=60: removes v1 (50 < 60, and there are newer versions)
    removed = chain.garbageCollect(60);
    try std.testing.expectEqual(@as(usize, 1), removed);
    try std.testing.expectEqual(@as(usize, 2), chain.versions.items.len);

    // GC with oldest_active=200: removes v2 as well
    removed = chain.garbageCollect(200);
    try std.testing.expectEqual(@as(usize, 1), removed);
    try std.testing.expectEqual(@as(usize, 1), chain.versions.items.len);

    // Final version cannot be GC'd (no newer version exists)
    try std.testing.expectEqual(@as(u64, 3), chain.versions.items[0].created_by);
}

test "garbage_collection: version store cleanup" {
    const allocator = std.testing.allocator;

    var store = mvcc.VersionStore(u64).init(allocator);
    defer store.deinit();

    // Record 1: has multiple versions
    const chain1 = try store.getOrCreateChain(1);
    try chain1.addVersion(.{
        .created_by = 1,
        .commit_ts = 50,
        .is_deleted = false,
        .created_ts = 40,
    });
    try chain1.addVersion(.{
        .created_by = 2,
        .commit_ts = 100,
        .is_deleted = false,
        .created_ts = 90,
    });

    // Record 2: only has a deletion marker
    const chain2 = try store.getOrCreateChain(2);
    try chain2.addVersion(.{
        .created_by = 3,
        .commit_ts = 60,
        .is_deleted = true,
        .created_ts = 55,
    });

    // Record 3: normal single version
    const chain3 = try store.getOrCreateChain(3);
    try chain3.addVersion(.{
        .created_by = 4,
        .commit_ts = 70,
        .is_deleted = false,
        .created_ts = 65,
    });

    // Before GC
    try std.testing.expect(store.getChain(1) != null);
    try std.testing.expect(store.getChain(2) != null);
    try std.testing.expect(store.getChain(3) != null);

    // Run GC with oldest_active=200 (all versions older than 200)
    const stats = store.garbageCollect(200);

    // Record 1: old version removed, newest kept
    try std.testing.expect(store.getChain(1) != null);
    try std.testing.expectEqual(@as(usize, 1), store.getChain(1).?.versions.items.len);

    // Record 2: fully deleted, chain removed
    try std.testing.expect(store.getChain(2) == null);

    // Record 3: single version kept (it's the newest)
    try std.testing.expect(store.getChain(3) != null);

    // Stats verification
    try std.testing.expectEqual(@as(u64, 4), stats.versions_examined);
    try std.testing.expect(stats.versions_collected >= 1);
    try std.testing.expectEqual(@as(u64, 1), stats.records_removed);
}

// ============================================================================
// Serialization Tests
// ============================================================================

test "version_info: serialization round-trip" {
    const original = VersionInfo{
        .created_by = 12345,
        .commit_ts = 67890,
        .is_deleted = true,
        .created_ts = 11111,
    };

    var buf: [32]u8 = undefined;
    const size = try original.serialize(&buf);
    try std.testing.expectEqual(VersionInfo.SERIALIZED_SIZE, size);

    const restored = try VersionInfo.deserialize(&buf);
    try std.testing.expectEqual(original.created_by, restored.created_by);
    try std.testing.expectEqual(original.commit_ts, restored.commit_ts);
    try std.testing.expectEqual(original.is_deleted, restored.is_deleted);
    try std.testing.expectEqual(original.created_ts, restored.created_ts);
}

test "version_info: commit marking" {
    var info = VersionInfo{
        .created_by = 1,
        .commit_ts = 0,
        .is_deleted = false,
        .created_ts = 10,
    };

    try std.testing.expect(!info.isCommitted());

    info.markCommitted(100);

    try std.testing.expect(info.isCommitted());
    try std.testing.expectEqual(@as(u64, 100), info.commit_ts);
}
