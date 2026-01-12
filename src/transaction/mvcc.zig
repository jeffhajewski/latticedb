//! Multi-Version Concurrency Control (MVCC) for Lattice database.
//!
//! Provides snapshot isolation by allowing readers to see consistent snapshots
//! without blocking writers. Each record can have multiple versions, with
//! visibility determined by transaction timestamps.
//!
//! Key concepts:
//! - Snapshot: A point-in-time view of the database captured at transaction start
//! - VersionInfo: Metadata tracking which transaction created/modified a record
//! - Visibility: Rules determining which versions a transaction can see

const std = @import("std");
const Allocator = std.mem.Allocator;

const lattice = @import("lattice");
const txn_mod = lattice.transaction.manager;
const Transaction = txn_mod.Transaction;
const TxnManager = txn_mod.TxnManager;

/// Transaction ID type
pub const TransactionId = u64;

/// Timestamp type (logical, monotonically increasing)
pub const Timestamp = u64;

/// Special timestamp values
pub const TIMESTAMP_MIN: Timestamp = 0;
pub const TIMESTAMP_MAX: Timestamp = std.math.maxInt(Timestamp);
pub const TXN_ID_NONE: TransactionId = 0;

// ============================================================================
// Snapshot
// ============================================================================

/// A snapshot captures the database state at a point in time.
/// Used for snapshot isolation - a transaction sees all changes committed
/// before its snapshot, and none committed after.
pub const Snapshot = struct {
    /// Read timestamp - versions with commit_ts <= read_ts are potentially visible
    read_ts: Timestamp,

    /// Transaction IDs that were active when the snapshot was taken.
    /// Versions created by these transactions are invisible (not yet committed).
    active_txns: []const TransactionId,

    /// The transaction that owns this snapshot (can always see its own changes)
    owner_txn_id: TransactionId,

    /// Allocator used for active_txns (needed for cleanup)
    allocator: Allocator,

    const Self = @This();

    /// Create a snapshot for a transaction.
    /// Captures the current timestamp and list of active transactions.
    pub fn create(
        allocator: Allocator,
        txn_manager: *TxnManager,
        txn: *const Transaction,
    ) !Self {
        txn_manager.mutex.lock();
        defer txn_manager.mutex.unlock();

        // Collect active transaction IDs (excluding the owner)
        var active_list = std.ArrayListUnmanaged(TransactionId){};
        errdefer active_list.deinit(allocator);

        var iter = txn_manager.active_txns.keyIterator();
        while (iter.next()) |id_ptr| {
            const id = id_ptr.*;
            if (id != txn.id) {
                try active_list.append(allocator, id);
            }
        }

        return Self{
            .read_ts = txn.start_ts,
            .active_txns = try active_list.toOwnedSlice(allocator),
            .owner_txn_id = txn.id,
            .allocator = allocator,
        };
    }

    /// Create a snapshot with a specific read timestamp.
    /// Useful for read-only queries at a specific point in time.
    pub fn createAt(
        allocator: Allocator,
        read_ts: Timestamp,
        active_txns: []const TransactionId,
    ) !Self {
        const owned_active = try allocator.dupe(TransactionId, active_txns);
        return Self{
            .read_ts = read_ts,
            .active_txns = owned_active,
            .owner_txn_id = TXN_ID_NONE,
            .allocator = allocator,
        };
    }

    /// Create a "current" snapshot that sees all committed data.
    /// No active transactions filter - sees everything committed up to read_ts.
    pub fn createCurrent(allocator: Allocator, read_ts: Timestamp) Self {
        return Self{
            .read_ts = read_ts,
            .active_txns = &[_]TransactionId{},
            .owner_txn_id = TXN_ID_NONE,
            .allocator = allocator,
        };
    }

    /// Free the snapshot's resources.
    pub fn deinit(self: *Self) void {
        if (self.active_txns.len > 0) {
            self.allocator.free(self.active_txns);
        }
        self.active_txns = &[_]TransactionId{};
    }

    /// Check if a transaction ID was active at snapshot time.
    pub fn wasActive(self: *const Self, txn_id: TransactionId) bool {
        for (self.active_txns) |active_id| {
            if (active_id == txn_id) {
                return true;
            }
        }
        return false;
    }
};

// ============================================================================
// Version Information
// ============================================================================

/// Version metadata stored with or alongside record data.
/// Each modification creates a new version with updated metadata.
pub const VersionInfo = struct {
    /// Transaction that created this version
    created_by: TransactionId,

    /// Commit timestamp (0 if not yet committed)
    /// Set when the creating transaction commits
    commit_ts: Timestamp,

    /// True if this version represents a deletion
    is_deleted: bool,

    /// Timestamp when this version was created (for ordering)
    created_ts: Timestamp,

    const Self = @This();

    /// Size of serialized version info (for embedding in records)
    pub const SERIALIZED_SIZE: usize = 8 + 8 + 1 + 8; // 25 bytes

    /// Create version info for a new record.
    pub fn createNew(txn: *const Transaction) Self {
        return Self{
            .created_by = txn.id,
            .commit_ts = 0, // Set on commit
            .is_deleted = false,
            .created_ts = txn.start_ts,
        };
    }

    /// Create version info for a deleted record.
    pub fn createDeleted(txn: *const Transaction) Self {
        return Self{
            .created_by = txn.id,
            .commit_ts = 0,
            .is_deleted = true,
            .created_ts = txn.start_ts,
        };
    }

    /// Mark this version as committed with the given timestamp.
    pub fn markCommitted(self: *Self, commit_ts: Timestamp) void {
        self.commit_ts = commit_ts;
    }

    /// Check if this version is committed.
    pub fn isCommitted(self: *const Self) bool {
        return self.commit_ts > 0;
    }

    /// Serialize to bytes for storage.
    pub fn serialize(self: *const Self, buf: []u8) !usize {
        if (buf.len < SERIALIZED_SIZE) return error.BufferTooSmall;

        var offset: usize = 0;

        // created_by (8 bytes)
        std.mem.writeInt(u64, buf[offset..][0..8], self.created_by, .little);
        offset += 8;

        // commit_ts (8 bytes)
        std.mem.writeInt(u64, buf[offset..][0..8], self.commit_ts, .little);
        offset += 8;

        // is_deleted (1 byte)
        buf[offset] = if (self.is_deleted) 1 else 0;
        offset += 1;

        // created_ts (8 bytes)
        std.mem.writeInt(u64, buf[offset..][0..8], self.created_ts, .little);
        offset += 8;

        return offset;
    }

    /// Deserialize from bytes.
    pub fn deserialize(buf: []const u8) !Self {
        if (buf.len < SERIALIZED_SIZE) return error.InvalidData;

        var offset: usize = 0;

        const created_by = std.mem.readInt(u64, buf[offset..][0..8], .little);
        offset += 8;

        const commit_ts = std.mem.readInt(u64, buf[offset..][0..8], .little);
        offset += 8;

        const is_deleted = buf[offset] != 0;
        offset += 1;

        const created_ts = std.mem.readInt(u64, buf[offset..][0..8], .little);

        return Self{
            .created_by = created_by,
            .commit_ts = commit_ts,
            .is_deleted = is_deleted,
            .created_ts = created_ts,
        };
    }
};

// ============================================================================
// Visibility Functions
// ============================================================================

/// MVCC visibility errors
pub const VisibilityError = error{
    /// Version info is invalid
    InvalidVersion,
    /// Out of memory
    OutOfMemory,
};

/// Determine if a version is visible to a given snapshot.
///
/// A version is visible if:
/// 1. It was committed (commit_ts > 0)
/// 2. It was committed before or at the snapshot's read_ts
/// 3. It was not created by a transaction that was active at snapshot time
///
/// Special case: A transaction can always see its own uncommitted changes.
pub fn isVisible(version: *const VersionInfo, snapshot: *const Snapshot) bool {
    // A transaction can always see its own changes (even uncommitted)
    if (version.created_by == snapshot.owner_txn_id) {
        return !version.is_deleted;
    }

    // Uncommitted versions from other transactions are invisible
    if (version.commit_ts == 0) {
        return false;
    }

    // Versions committed after snapshot are invisible
    if (version.commit_ts > snapshot.read_ts) {
        return false;
    }

    // Versions from transactions that were active at snapshot time are invisible
    // (they committed after the snapshot was taken)
    if (snapshot.wasActive(version.created_by)) {
        return false;
    }

    // Version is visible, but check if it's a deletion marker
    return !version.is_deleted;
}

/// Check if a version is visible as a deletion marker.
/// Used when we need to know if a record was deleted (not just invisible).
pub fn isVisibleDeleted(version: *const VersionInfo, snapshot: *const Snapshot) bool {
    // A transaction can see its own deletions
    if (version.created_by == snapshot.owner_txn_id) {
        return version.is_deleted;
    }

    // Uncommitted versions from other transactions are invisible
    if (version.commit_ts == 0) {
        return false;
    }

    // Versions committed after snapshot are invisible
    if (version.commit_ts > snapshot.read_ts) {
        return false;
    }

    // Versions from transactions that were active at snapshot time are invisible
    if (snapshot.wasActive(version.created_by)) {
        return false;
    }

    return version.is_deleted;
}

/// Check if any version in a chain is visible to a snapshot.
/// Used when we have multiple versions and need to find the visible one.
pub fn hasVisibleVersion(versions: []const VersionInfo, snapshot: *const Snapshot) bool {
    for (versions) |*version| {
        if (isVisible(version, snapshot)) {
            return true;
        }
    }
    return false;
}

/// Find the visible version in a chain (most recent visible).
/// Versions should be ordered newest-first.
/// Returns the index of the visible version, or null if none visible.
///
/// Note: If a deletion marker is encountered and visible (to the snapshot),
/// returns null since the record appears deleted at that point in time.
pub fn findVisibleVersionIndex(versions: []const VersionInfo, snapshot: *const Snapshot) ?usize {
    for (versions, 0..) |*version, i| {
        // Check if this version is visible to the snapshot (ignoring deletion status)
        const is_potentially_visible = blk: {
            // A transaction can always see its own changes
            if (version.created_by == snapshot.owner_txn_id) {
                break :blk true;
            }
            // Uncommitted versions from other transactions are invisible
            if (version.commit_ts == 0) {
                break :blk false;
            }
            // Versions committed after snapshot are invisible
            if (version.commit_ts > snapshot.read_ts) {
                break :blk false;
            }
            // Versions from transactions that were active at snapshot time are invisible
            if (snapshot.wasActive(version.created_by)) {
                break :blk false;
            }
            break :blk true;
        };

        if (is_potentially_visible) {
            // If this is a deletion marker, the record is deleted
            if (version.is_deleted) {
                return null;
            }
            // Otherwise, this is the visible version
            return i;
        }
    }
    return null;
}

// ============================================================================
// Garbage Collection
// ============================================================================

/// Check if a version can be garbage collected.
///
/// A version can be collected if:
/// 1. It is committed
/// 2. There is a newer committed version
/// 3. No active transaction can ever need this version
///    (its commit_ts < oldest_active_ts)
pub fn canGarbageCollect(
    version: *const VersionInfo,
    has_newer_version: bool,
    oldest_active_ts: Timestamp,
) bool {
    // Uncommitted versions cannot be collected
    if (version.commit_ts == 0) {
        return false;
    }

    // If this is the only/newest version, keep it
    if (!has_newer_version) {
        return false;
    }

    // If any active transaction might need this version, keep it
    // A transaction needs this version if its start_ts >= version.commit_ts
    // So we can collect if commit_ts < oldest_active_ts
    if (version.commit_ts >= oldest_active_ts) {
        return false;
    }

    return true;
}

/// Statistics for garbage collection
pub const GcStats = struct {
    /// Number of versions examined
    versions_examined: u64,
    /// Number of versions collected (freed)
    versions_collected: u64,
    /// Number of records with all versions deleted
    records_removed: u64,
};

/// Version chain manager for a single record.
/// Manages multiple versions with newest-first ordering.
pub const VersionChain = struct {
    versions: std.ArrayListUnmanaged(VersionInfo),
    allocator: Allocator,

    const Self = @This();

    pub fn init(allocator: Allocator) Self {
        return Self{
            .versions = .{},
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Self) void {
        self.versions.deinit(self.allocator);
    }

    /// Add a new version (newest-first).
    pub fn addVersion(self: *Self, version: VersionInfo) !void {
        try self.versions.insert(self.allocator, 0, version);
    }

    /// Get all versions.
    pub fn getVersions(self: *const Self) []const VersionInfo {
        return self.versions.items;
    }

    /// Find the visible version for a snapshot.
    pub fn findVisible(self: *const Self, snapshot: *const Snapshot) ?*const VersionInfo {
        const idx = findVisibleVersionIndex(self.versions.items, snapshot);
        if (idx) |i| {
            return &self.versions.items[i];
        }
        return null;
    }

    /// Mark the newest version as committed.
    pub fn markCommitted(self: *Self, commit_ts: Timestamp) void {
        if (self.versions.items.len > 0) {
            self.versions.items[0].markCommitted(commit_ts);
        }
    }

    /// Remove old versions that can be garbage collected.
    /// Returns the number of versions removed.
    pub fn garbageCollect(self: *Self, oldest_active_ts: Timestamp) usize {
        var removed: usize = 0;
        var i: usize = self.versions.items.len;

        // Iterate backwards to safely remove elements
        while (i > 0) {
            i -= 1;
            const has_newer = i > 0;
            if (canGarbageCollect(&self.versions.items[i], has_newer, oldest_active_ts)) {
                _ = self.versions.orderedRemove(i);
                removed += 1;
            }
        }

        return removed;
    }

    /// Check if this chain represents a fully deleted record
    /// (newest visible version is a deletion marker or all versions are GC'd).
    pub fn isFullyDeleted(self: *const Self) bool {
        if (self.versions.items.len == 0) {
            return true;
        }
        // Check if newest version is a committed deletion
        const newest = &self.versions.items[0];
        return newest.is_deleted and newest.isCommitted();
    }
};

// ============================================================================
// Version Store
// ============================================================================

/// In-memory version store tracking version chains for records.
/// Key is a generic identifier (node_id, edge_key, etc.).
pub fn VersionStore(comptime KeyType: type) type {
    return struct {
        chains: std.AutoHashMap(KeyType, VersionChain),
        allocator: Allocator,
        mutex: std.Thread.Mutex,

        const Self = @This();

        pub fn init(allocator: Allocator) Self {
            return Self{
                .chains = std.AutoHashMap(KeyType, VersionChain).init(allocator),
                .allocator = allocator,
                .mutex = .{},
            };
        }

        pub fn deinit(self: *Self) void {
            var iter = self.chains.valueIterator();
            while (iter.next()) |chain| {
                chain.deinit();
            }
            self.chains.deinit();
        }

        /// Get or create a version chain for a key.
        pub fn getOrCreateChain(self: *Self, key: KeyType) !*VersionChain {
            self.mutex.lock();
            defer self.mutex.unlock();

            const result = try self.chains.getOrPut(key);
            if (!result.found_existing) {
                result.value_ptr.* = VersionChain.init(self.allocator);
            }
            return result.value_ptr;
        }

        /// Get a version chain if it exists.
        pub fn getChain(self: *Self, key: KeyType) ?*VersionChain {
            self.mutex.lock();
            defer self.mutex.unlock();

            return self.chains.getPtr(key);
        }

        /// Remove a version chain.
        pub fn removeChain(self: *Self, key: KeyType) bool {
            self.mutex.lock();
            defer self.mutex.unlock();

            if (self.chains.fetchRemove(key)) |kv| {
                var chain = kv.value;
                chain.deinit();
                return true;
            }
            return false;
        }

        /// Run garbage collection on all chains.
        pub fn garbageCollect(self: *Self, oldest_active_ts: Timestamp) GcStats {
            self.mutex.lock();
            defer self.mutex.unlock();

            var stats = GcStats{
                .versions_examined = 0,
                .versions_collected = 0,
                .records_removed = 0,
            };

            // Collect keys of chains to remove (fully deleted records)
            var to_remove = std.ArrayListUnmanaged(KeyType){};
            defer to_remove.deinit(self.allocator);

            var iter = self.chains.iterator();
            while (iter.next()) |entry| {
                stats.versions_examined += entry.value_ptr.versions.items.len;

                const removed = entry.value_ptr.garbageCollect(oldest_active_ts);
                stats.versions_collected += removed;

                // If chain is now empty or fully deleted, mark for removal
                if (entry.value_ptr.versions.items.len == 0 or entry.value_ptr.isFullyDeleted()) {
                    to_remove.append(self.allocator, entry.key_ptr.*) catch {};
                }
            }

            // Remove empty chains
            for (to_remove.items) |key| {
                if (self.chains.fetchRemove(key)) |kv| {
                    var chain = kv.value;
                    chain.deinit();
                    stats.records_removed += 1;
                }
            }

            return stats;
        }
    };
}

// ============================================================================
// Tests
// ============================================================================

test "version_info: serialize and deserialize" {
    var info = VersionInfo{
        .created_by = 42,
        .commit_ts = 100,
        .is_deleted = false,
        .created_ts = 50,
    };

    var buf: [32]u8 = undefined;
    const size = try info.serialize(&buf);
    try std.testing.expectEqual(@as(usize, VersionInfo.SERIALIZED_SIZE), size);

    const restored = try VersionInfo.deserialize(&buf);
    try std.testing.expectEqual(@as(u64, 42), restored.created_by);
    try std.testing.expectEqual(@as(u64, 100), restored.commit_ts);
    try std.testing.expectEqual(false, restored.is_deleted);
    try std.testing.expectEqual(@as(u64, 50), restored.created_ts);
}

test "version_info: deleted flag" {
    var info = VersionInfo{
        .created_by = 1,
        .commit_ts = 0,
        .is_deleted = true,
        .created_ts = 10,
    };

    var buf: [32]u8 = undefined;
    _ = try info.serialize(&buf);

    const restored = try VersionInfo.deserialize(&buf);
    try std.testing.expectEqual(true, restored.is_deleted);
}

test "snapshot: basic visibility" {
    const allocator = std.testing.allocator;

    var snapshot = Snapshot{
        .read_ts = 100,
        .active_txns = &[_]TransactionId{},
        .owner_txn_id = 5,
        .allocator = allocator,
    };

    // Committed version before snapshot - visible
    const v1 = VersionInfo{
        .created_by = 1,
        .commit_ts = 50,
        .is_deleted = false,
        .created_ts = 40,
    };
    try std.testing.expect(isVisible(&v1, &snapshot));

    // Committed version after snapshot - not visible
    const v2 = VersionInfo{
        .created_by = 2,
        .commit_ts = 150,
        .is_deleted = false,
        .created_ts = 140,
    };
    try std.testing.expect(!isVisible(&v2, &snapshot));

    // Uncommitted version - not visible
    const v3 = VersionInfo{
        .created_by = 3,
        .commit_ts = 0,
        .is_deleted = false,
        .created_ts = 80,
    };
    try std.testing.expect(!isVisible(&v3, &snapshot));

    // Own uncommitted version - visible
    const v4 = VersionInfo{
        .created_by = 5, // Same as owner
        .commit_ts = 0,
        .is_deleted = false,
        .created_ts = 90,
    };
    try std.testing.expect(isVisible(&v4, &snapshot));
}

test "snapshot: active transaction filter" {
    const allocator = std.testing.allocator;
    const active = [_]TransactionId{ 10, 20, 30 };

    var snapshot = Snapshot{
        .read_ts = 100,
        .active_txns = &active,
        .owner_txn_id = 5,
        .allocator = allocator,
    };

    // Version from active transaction - not visible even if committed
    const v1 = VersionInfo{
        .created_by = 20, // Was active at snapshot time
        .commit_ts = 95, // Committed before read_ts
        .is_deleted = false,
        .created_ts = 90,
    };
    try std.testing.expect(!isVisible(&v1, &snapshot));

    // Version from non-active transaction - visible
    const v2 = VersionInfo{
        .created_by = 15, // Not in active list
        .commit_ts = 80,
        .is_deleted = false,
        .created_ts = 70,
    };
    try std.testing.expect(isVisible(&v2, &snapshot));
}

test "snapshot: deleted versions" {
    const allocator = std.testing.allocator;

    var snapshot = Snapshot{
        .read_ts = 100,
        .active_txns = &[_]TransactionId{},
        .owner_txn_id = 5,
        .allocator = allocator,
    };

    // Committed deletion - not visible as regular version
    const v1 = VersionInfo{
        .created_by = 1,
        .commit_ts = 50,
        .is_deleted = true,
        .created_ts = 40,
    };
    try std.testing.expect(!isVisible(&v1, &snapshot));
    try std.testing.expect(isVisibleDeleted(&v1, &snapshot));

    // Own deletion (uncommitted) - visible as deletion
    const v2 = VersionInfo{
        .created_by = 5,
        .commit_ts = 0,
        .is_deleted = true,
        .created_ts = 90,
    };
    try std.testing.expect(!isVisible(&v2, &snapshot));
    try std.testing.expect(isVisibleDeleted(&v2, &snapshot));
}

test "version_chain: basic operations" {
    const allocator = std.testing.allocator;

    var chain = VersionChain.init(allocator);
    defer chain.deinit();

    // Add versions
    try chain.addVersion(.{
        .created_by = 1,
        .commit_ts = 50,
        .is_deleted = false,
        .created_ts = 40,
    });

    try chain.addVersion(.{
        .created_by = 2,
        .commit_ts = 100,
        .is_deleted = false,
        .created_ts = 90,
    });

    // Newest version should be first
    try std.testing.expectEqual(@as(usize, 2), chain.versions.items.len);
    try std.testing.expectEqual(@as(u64, 2), chain.versions.items[0].created_by);
}

test "version_chain: find visible" {
    const allocator = std.testing.allocator;

    var chain = VersionChain.init(allocator);
    defer chain.deinit();

    // Old version
    try chain.addVersion(.{
        .created_by = 1,
        .commit_ts = 50,
        .is_deleted = false,
        .created_ts = 40,
    });

    // Newer version
    try chain.addVersion(.{
        .created_by = 2,
        .commit_ts = 100,
        .is_deleted = false,
        .created_ts = 90,
    });

    // Snapshot at ts=75 should see version 1
    var snapshot1 = Snapshot{
        .read_ts = 75,
        .active_txns = &[_]TransactionId{},
        .owner_txn_id = 99,
        .allocator = allocator,
    };

    const visible1 = chain.findVisible(&snapshot1);
    try std.testing.expect(visible1 != null);
    try std.testing.expectEqual(@as(u64, 1), visible1.?.created_by);

    // Snapshot at ts=150 should see version 2
    var snapshot2 = Snapshot{
        .read_ts = 150,
        .active_txns = &[_]TransactionId{},
        .owner_txn_id = 99,
        .allocator = allocator,
    };

    const visible2 = chain.findVisible(&snapshot2);
    try std.testing.expect(visible2 != null);
    try std.testing.expectEqual(@as(u64, 2), visible2.?.created_by);
}

test "garbage_collection: basic" {
    const allocator = std.testing.allocator;

    var chain = VersionChain.init(allocator);
    defer chain.deinit();

    // Old version (can be GC'd)
    try chain.addVersion(.{
        .created_by = 1,
        .commit_ts = 50,
        .is_deleted = false,
        .created_ts = 40,
    });

    // Newer version
    try chain.addVersion(.{
        .created_by = 2,
        .commit_ts = 100,
        .is_deleted = false,
        .created_ts = 90,
    });

    try std.testing.expectEqual(@as(usize, 2), chain.versions.items.len);

    // GC with oldest_active = 200 should remove old version
    const removed = chain.garbageCollect(200);
    try std.testing.expectEqual(@as(usize, 1), removed);
    try std.testing.expectEqual(@as(usize, 1), chain.versions.items.len);
    try std.testing.expectEqual(@as(u64, 2), chain.versions.items[0].created_by);
}

test "garbage_collection: keeps versions needed by active transactions" {
    const allocator = std.testing.allocator;

    var chain = VersionChain.init(allocator);
    defer chain.deinit();

    // Old version
    try chain.addVersion(.{
        .created_by = 1,
        .commit_ts = 50,
        .is_deleted = false,
        .created_ts = 40,
    });

    // Newer version
    try chain.addVersion(.{
        .created_by = 2,
        .commit_ts = 100,
        .is_deleted = false,
        .created_ts = 90,
    });

    // GC with oldest_active = 30 should keep both versions
    // (transaction at ts=30 might need the old version)
    const removed = chain.garbageCollect(30);
    try std.testing.expectEqual(@as(usize, 0), removed);
    try std.testing.expectEqual(@as(usize, 2), chain.versions.items.len);
}

test "version_store: basic operations" {
    const allocator = std.testing.allocator;

    var store = VersionStore(u64).init(allocator);
    defer store.deinit();

    // Create chain for key 1
    const chain1 = try store.getOrCreateChain(1);
    try chain1.addVersion(.{
        .created_by = 1,
        .commit_ts = 50,
        .is_deleted = false,
        .created_ts = 40,
    });

    // Get existing chain
    const chain1_again = store.getChain(1);
    try std.testing.expect(chain1_again != null);
    try std.testing.expectEqual(@as(usize, 1), chain1_again.?.versions.items.len);

    // Non-existent chain
    const chain2 = store.getChain(999);
    try std.testing.expect(chain2 == null);
}

test "version_store: garbage collection" {
    const allocator = std.testing.allocator;

    var store = VersionStore(u64).init(allocator);
    defer store.deinit();

    // Create chain with old and new versions
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

    // Create chain with only deleted version
    const chain2 = try store.getOrCreateChain(2);
    try chain2.addVersion(.{
        .created_by = 3,
        .commit_ts = 60,
        .is_deleted = true,
        .created_ts = 55,
    });

    const stats = store.garbageCollect(200);
    try std.testing.expectEqual(@as(u64, 3), stats.versions_examined);
    try std.testing.expectEqual(@as(u64, 1), stats.versions_collected);
    try std.testing.expectEqual(@as(u64, 1), stats.records_removed);
}
