//! Transaction Manager for Lattice database.
//!
//! Provides ACID transactions with WAL-based durability.
//! Manages transaction lifecycle: begin, commit, abort, savepoints.

const std = @import("std");
const lattice = @import("lattice");

const Allocator = std.mem.Allocator;

const types = lattice.core.types;
const wal_mod = lattice.storage.wal;

const WalManager = wal_mod.WalManager;
const WalRecordType = wal_mod.WalRecordType;
const WalError = wal_mod.WalError;

// ============================================================================
// Types
// ============================================================================

/// Transaction state
pub const TxnState = enum(u8) {
    /// Transaction is active and accepting operations
    active,
    /// Transaction is preparing to commit (2PC)
    preparing,
    /// Transaction has been committed
    committed,
    /// Transaction has been aborted/rolled back
    aborted,
};

/// Transaction mode
pub const TxnMode = enum(u8) {
    /// Read-only transaction (no modifications allowed)
    read_only,
    /// Read-write transaction (can modify data)
    read_write,
};

/// Transaction isolation level
pub const IsolationLevel = enum(u8) {
    /// Read committed - sees only committed data at statement start
    read_committed,
    /// Snapshot isolation - sees consistent snapshot from txn start
    snapshot,
    /// Serializable - full isolation with conflict detection
    serializable,
};

/// Transaction errors
pub const TxnError = error{
    /// Transaction is not active
    NotActive,
    /// Transaction is read-only
    ReadOnly,
    /// Transaction ID not found
    NotFound,
    /// Too many active transactions
    TooManyTransactions,
    /// Savepoint not found
    SavepointNotFound,
    /// WAL write failed
    WalError,
    /// Transaction was aborted due to conflict
    Conflict,
    /// Out of memory
    OutOfMemory,
};

/// Transaction handle (returned to user)
pub const Transaction = struct {
    /// Unique transaction ID
    id: u64,
    /// Current state
    state: TxnState,
    /// Transaction mode
    mode: TxnMode,
    /// Isolation level
    isolation: IsolationLevel,
    /// Start timestamp (for MVCC visibility)
    start_ts: u64,
    /// Commit timestamp (set on commit)
    commit_ts: u64,

    /// Check if transaction can perform writes
    pub fn canWrite(self: *const Transaction) bool {
        return self.mode == .read_write and self.state == .active;
    }

    /// Check if transaction is still active
    pub fn isActive(self: *const Transaction) bool {
        return self.state == .active;
    }
};

/// Internal transaction state (more detailed than handle)
const TxnEntry = struct {
    /// The transaction handle
    txn: Transaction,
    /// LSN of the last record for this transaction (for prev_lsn chain)
    last_lsn: u64,
    /// LSN of TXN_BEGIN record
    begin_lsn: u64,
    /// Savepoint stack
    savepoints: std.ArrayListUnmanaged(Savepoint),
    /// Undo buffer - stores operations for rollback
    undo_log: std.ArrayListUnmanaged(UndoEntry),
};

/// Savepoint for partial rollback
pub const Savepoint = struct {
    /// Savepoint name (user-provided)
    name: []const u8,
    /// LSN at savepoint creation
    lsn: u64,
    /// Undo log position at savepoint
    undo_position: usize,
};

/// Entity type for undo operations
pub const EntityType = enum(u8) {
    node,
    edge,
    property,
    label,
};

/// Undo log entry for rollback
pub const UndoEntry = struct {
    /// Type of operation to undo
    op_type: UndoOpType,
    /// Type of entity affected
    entity_type: EntityType,
    /// Primary entity ID (node_id for nodes/properties, source for edges)
    entity_id: u64,
    /// Secondary ID (target node for edges, 0 otherwise)
    secondary_id: u64,
    /// Type ID for edges, key_id for properties
    type_id: u16,
    /// Previous data for restoring on undo (owned, must be freed)
    prev_data: ?[]const u8,
};

pub const UndoOpType = enum(u8) {
    insert,
    update,
    delete,
};

/// Transaction statistics
pub const TxnStats = struct {
    /// Currently active transactions
    active_count: u32,
    /// Total committed transactions
    committed_count: u64,
    /// Total aborted transactions
    aborted_count: u64,
    /// Oldest active transaction ID
    oldest_active_id: u64,
    /// Current timestamp
    current_ts: u64,
};

// ============================================================================
// Transaction Manager
// ============================================================================

/// Maximum concurrent transactions
const MAX_TRANSACTIONS: usize = 1024;

/// Manages all transactions in the database
pub const TxnManager = struct {
    allocator: Allocator,
    /// WAL for durability
    wal: *WalManager,
    /// Active transactions by ID
    active_txns: std.AutoHashMap(u64, TxnEntry),
    /// Next transaction ID
    next_txn_id: u64,
    /// Global timestamp counter (for MVCC)
    current_ts: u64,
    /// Statistics
    committed_count: u64,
    aborted_count: u64,
    /// Mutex for thread safety
    mutex: std.Thread.Mutex,

    const Self = @This();

    /// Initialize the transaction manager
    pub fn init(allocator: Allocator, wal: *WalManager) Self {
        return Self{
            .allocator = allocator,
            .wal = wal,
            .active_txns = std.AutoHashMap(u64, TxnEntry).init(allocator),
            .next_txn_id = 1,
            .current_ts = 1,
            .committed_count = 0,
            .aborted_count = 0,
            .mutex = .{},
        };
    }

    /// Clean up resources
    pub fn deinit(self: *Self) void {
        // Clean up any remaining transactions
        var iter = self.active_txns.iterator();
        while (iter.next()) |entry| {
            entry.value_ptr.savepoints.deinit(self.allocator);
            entry.value_ptr.undo_log.deinit(self.allocator);
        }
        self.active_txns.deinit();
    }

    /// Begin a new transaction
    pub fn begin(self: *Self, mode: TxnMode, isolation: IsolationLevel) TxnError!Transaction {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.active_txns.count() >= MAX_TRANSACTIONS) {
            return TxnError.TooManyTransactions;
        }

        const txn_id = self.next_txn_id;
        self.next_txn_id += 1;

        const start_ts = self.current_ts;
        self.current_ts += 1;

        // Log TXN_BEGIN to WAL
        const begin_lsn = self.wal.appendRecord(.txn_begin, txn_id, 0, &[_]u8{}) catch {
            return TxnError.WalError;
        };

        const txn = Transaction{
            .id = txn_id,
            .state = .active,
            .mode = mode,
            .isolation = isolation,
            .start_ts = start_ts,
            .commit_ts = 0,
        };

        const entry = TxnEntry{
            .txn = txn,
            .last_lsn = begin_lsn,
            .begin_lsn = begin_lsn,
            .savepoints = .{},
            .undo_log = .{},
        };

        self.active_txns.put(txn_id, entry) catch {
            return TxnError.OutOfMemory;
        };

        return txn;
    }

    /// Commit a transaction (makes changes durable)
    pub fn commit(self: *Self, txn: *Transaction) TxnError!void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (txn.state != .active) {
            return TxnError.NotActive;
        }

        const entry = self.active_txns.getPtr(txn.id) orelse {
            return TxnError.NotFound;
        };

        // Log TXN_COMMIT to WAL
        _ = self.wal.appendRecord(.txn_commit, txn.id, entry.last_lsn, &[_]u8{}) catch {
            return TxnError.WalError;
        };

        // Sync WAL to ensure durability
        self.wal.sync() catch {
            return TxnError.WalError;
        };

        // Assign commit timestamp
        const commit_ts = self.current_ts;
        self.current_ts += 1;

        txn.state = .committed;
        txn.commit_ts = commit_ts;

        // Clean up transaction entry
        entry.savepoints.deinit(self.allocator);
        entry.undo_log.deinit(self.allocator);
        _ = self.active_txns.remove(txn.id);

        self.committed_count += 1;
    }

    /// Abort a transaction (rollback all changes)
    pub fn abort(self: *Self, txn: *Transaction) TxnError!void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (txn.state != .active) {
            return TxnError.NotActive;
        }

        const entry = self.active_txns.getPtr(txn.id) orelse {
            return TxnError.NotFound;
        };

        // Log TXN_ABORT to WAL
        _ = self.wal.appendRecord(.txn_abort, txn.id, entry.last_lsn, &[_]u8{}) catch {
            return TxnError.WalError;
        };

        // Sync WAL
        self.wal.sync() catch {
            return TxnError.WalError;
        };

        txn.state = .aborted;

        // Clean up transaction entry
        entry.savepoints.deinit(self.allocator);

        // Free prev_data in undo entries
        for (entry.undo_log.items) |undo_entry| {
            if (undo_entry.prev_data) |data| {
                self.allocator.free(data);
            }
        }
        entry.undo_log.deinit(self.allocator);
        _ = self.active_txns.remove(txn.id);

        self.aborted_count += 1;
    }

    /// Create a savepoint for partial rollback
    pub fn savepoint(self: *Self, txn: *Transaction, name: []const u8) TxnError!void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (txn.state != .active) {
            return TxnError.NotActive;
        }

        const entry = self.active_txns.getPtr(txn.id) orelse {
            return TxnError.NotFound;
        };

        // Log savepoint to WAL
        const lsn = self.wal.appendRecord(.savepoint, txn.id, entry.last_lsn, name) catch {
            return TxnError.WalError;
        };
        entry.last_lsn = lsn;

        // Record savepoint
        const sp = Savepoint{
            .name = name,
            .lsn = lsn,
            .undo_position = entry.undo_log.items.len,
        };

        entry.savepoints.append(self.allocator, sp) catch {
            return TxnError.OutOfMemory;
        };
    }

    /// Rollback to a savepoint
    pub fn rollbackToSavepoint(self: *Self, txn: *Transaction, name: []const u8) TxnError!void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (txn.state != .active) {
            return TxnError.NotActive;
        }

        const entry = self.active_txns.getPtr(txn.id) orelse {
            return TxnError.NotFound;
        };

        // Find the savepoint
        var found_idx: ?usize = null;
        for (entry.savepoints.items, 0..) |sp, i| {
            if (std.mem.eql(u8, sp.name, name)) {
                found_idx = i;
                break;
            }
        }

        const idx = found_idx orelse return TxnError.SavepointNotFound;
        const sp = entry.savepoints.items[idx];

        // Log rollback to WAL
        const lsn = self.wal.appendRecord(.savepoint_rollback, txn.id, entry.last_lsn, name) catch {
            return TxnError.WalError;
        };
        entry.last_lsn = lsn;

        // Truncate undo log to savepoint position
        entry.undo_log.shrinkRetainingCapacity(sp.undo_position);

        // Remove savepoints after this one
        entry.savepoints.shrinkRetainingCapacity(idx + 1);
    }

    /// Log an operation for a transaction (updates prev_lsn chain)
    pub fn logOperation(
        self: *Self,
        txn: *Transaction,
        record_type: WalRecordType,
        payload: []const u8,
    ) TxnError!u64 {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (!txn.canWrite()) {
            if (txn.mode == .read_only) {
                return TxnError.ReadOnly;
            }
            return TxnError.NotActive;
        }

        const entry = self.active_txns.getPtr(txn.id) orelse {
            return TxnError.NotFound;
        };

        const lsn = self.wal.appendRecord(record_type, txn.id, entry.last_lsn, payload) catch {
            return TxnError.WalError;
        };
        entry.last_lsn = lsn;

        return lsn;
    }

    /// Add an undo entry to the transaction's undo log
    /// Called by Database operations to enable rollback on abort
    pub fn addUndoEntry(
        self: *Self,
        txn: *Transaction,
        op_type: UndoOpType,
        entity_type: EntityType,
        entity_id: u64,
        secondary_id: u64,
        type_id: u16,
        prev_data: ?[]const u8,
    ) TxnError!void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (!txn.isActive()) {
            return TxnError.NotActive;
        }

        const entry = self.active_txns.getPtr(txn.id) orelse {
            return TxnError.NotFound;
        };

        // Copy prev_data if provided (we own the copy)
        const owned_data: ?[]const u8 = if (prev_data) |data| blk: {
            const copy = self.allocator.alloc(u8, data.len) catch {
                return TxnError.OutOfMemory;
            };
            @memcpy(copy, data);
            break :blk copy;
        } else null;

        entry.undo_log.append(self.allocator, .{
            .op_type = op_type,
            .entity_type = entity_type,
            .entity_id = entity_id,
            .secondary_id = secondary_id,
            .type_id = type_id,
            .prev_data = owned_data,
        }) catch {
            if (owned_data) |data| self.allocator.free(data);
            return TxnError.OutOfMemory;
        };
    }

    /// Get the undo log for a transaction (for executing undo operations before abort)
    /// Returns slice of undo entries in forward order; caller should iterate in reverse
    pub fn getUndoLog(self: *Self, txn: *Transaction) ?[]const UndoEntry {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.active_txns.getPtr(txn.id) orelse return null;
        return entry.undo_log.items;
    }

    /// Get transaction by ID (returns null if not found or not active)
    pub fn getTransaction(self: *Self, txn_id: u64) ?*Transaction {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.active_txns.getPtr(txn_id)) |entry| {
            return &entry.txn;
        }
        return null;
    }

    /// Get current statistics
    pub fn getStats(self: *Self) TxnStats {
        self.mutex.lock();
        defer self.mutex.unlock();

        var oldest_id: u64 = std.math.maxInt(u64);
        var iter = self.active_txns.iterator();
        while (iter.next()) |entry| {
            if (entry.key_ptr.* < oldest_id) {
                oldest_id = entry.key_ptr.*;
            }
        }

        return TxnStats{
            .active_count = @intCast(self.active_txns.count()),
            .committed_count = self.committed_count,
            .aborted_count = self.aborted_count,
            .oldest_active_id = if (oldest_id == std.math.maxInt(u64)) 0 else oldest_id,
            .current_ts = self.current_ts,
        };
    }

    /// Get the oldest active transaction's start timestamp
    /// Used for MVCC garbage collection
    pub fn getOldestActiveTs(self: *Self) u64 {
        self.mutex.lock();
        defer self.mutex.unlock();

        var oldest_ts: u64 = std.math.maxInt(u64);
        var iter = self.active_txns.iterator();
        while (iter.next()) |entry| {
            if (entry.value_ptr.txn.start_ts < oldest_ts) {
                oldest_ts = entry.value_ptr.txn.start_ts;
            }
        }

        return if (oldest_ts == std.math.maxInt(u64)) self.current_ts else oldest_ts;
    }
};

// ============================================================================
// Tests
// ============================================================================

test "begin and commit transaction" {
    const allocator = std.testing.allocator;

    // Set up WAL
    const vfs = @import("../storage/vfs.zig");
    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const wal_path = "/tmp/lattice_txn_test.wal";
    vfs_impl.delete(wal_path) catch {};

    var uuid: [16]u8 = undefined;
    std.crypto.random.bytes(&uuid);

    var wal = try WalManager.init(allocator, vfs_impl, wal_path, uuid);
    defer {
        wal.deinit();
        vfs_impl.delete(wal_path) catch {};
    }

    // Create transaction manager
    var tm = TxnManager.init(allocator, &wal);
    defer tm.deinit();

    // Begin transaction
    var txn = try tm.begin(.read_write, .snapshot);
    try std.testing.expectEqual(@as(u64, 1), txn.id);
    try std.testing.expectEqual(TxnState.active, txn.state);

    // Commit
    try tm.commit(&txn);
    try std.testing.expectEqual(TxnState.committed, txn.state);
    try std.testing.expect(txn.commit_ts > 0);

    // Stats
    const stats = tm.getStats();
    try std.testing.expectEqual(@as(u32, 0), stats.active_count);
    try std.testing.expectEqual(@as(u64, 1), stats.committed_count);
}

test "begin and abort transaction" {
    const allocator = std.testing.allocator;

    const vfs = @import("../storage/vfs.zig");
    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const wal_path = "/tmp/lattice_txn_abort_test.wal";
    vfs_impl.delete(wal_path) catch {};

    var uuid: [16]u8 = undefined;
    std.crypto.random.bytes(&uuid);

    var wal = try WalManager.init(allocator, vfs_impl, wal_path, uuid);
    defer {
        wal.deinit();
        vfs_impl.delete(wal_path) catch {};
    }

    var tm = TxnManager.init(allocator, &wal);
    defer tm.deinit();

    var txn = try tm.begin(.read_write, .snapshot);
    try tm.abort(&txn);

    try std.testing.expectEqual(TxnState.aborted, txn.state);

    const stats = tm.getStats();
    try std.testing.expectEqual(@as(u64, 1), stats.aborted_count);
}

test "multiple concurrent transactions" {
    const allocator = std.testing.allocator;

    const vfs = @import("../storage/vfs.zig");
    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const wal_path = "/tmp/lattice_txn_multi_test.wal";
    vfs_impl.delete(wal_path) catch {};

    var uuid: [16]u8 = undefined;
    std.crypto.random.bytes(&uuid);

    var wal = try WalManager.init(allocator, vfs_impl, wal_path, uuid);
    defer {
        wal.deinit();
        vfs_impl.delete(wal_path) catch {};
    }

    var tm = TxnManager.init(allocator, &wal);
    defer tm.deinit();

    // Start multiple transactions
    var txn1 = try tm.begin(.read_write, .snapshot);
    var txn2 = try tm.begin(.read_only, .read_committed);
    var txn3 = try tm.begin(.read_write, .snapshot);

    try std.testing.expectEqual(@as(u64, 1), txn1.id);
    try std.testing.expectEqual(@as(u64, 2), txn2.id);
    try std.testing.expectEqual(@as(u64, 3), txn3.id);

    var stats = tm.getStats();
    try std.testing.expectEqual(@as(u32, 3), stats.active_count);

    // Commit one, abort one
    try tm.commit(&txn1);
    try tm.abort(&txn3);

    stats = tm.getStats();
    try std.testing.expectEqual(@as(u32, 1), stats.active_count);
    try std.testing.expectEqual(@as(u64, 2), stats.oldest_active_id);

    try tm.commit(&txn2);
}

test "savepoint and rollback" {
    const allocator = std.testing.allocator;

    const vfs = @import("../storage/vfs.zig");
    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const wal_path = "/tmp/lattice_txn_savepoint_test.wal";
    vfs_impl.delete(wal_path) catch {};

    var uuid: [16]u8 = undefined;
    std.crypto.random.bytes(&uuid);

    var wal = try WalManager.init(allocator, vfs_impl, wal_path, uuid);
    defer {
        wal.deinit();
        vfs_impl.delete(wal_path) catch {};
    }

    var tm = TxnManager.init(allocator, &wal);
    defer tm.deinit();

    var txn = try tm.begin(.read_write, .snapshot);

    // Create savepoint
    try tm.savepoint(&txn, "sp1");

    // Do some operations
    _ = try tm.logOperation(&txn, .insert, "data1");
    _ = try tm.logOperation(&txn, .insert, "data2");

    // Create another savepoint
    try tm.savepoint(&txn, "sp2");
    _ = try tm.logOperation(&txn, .insert, "data3");

    // Rollback to sp1
    try tm.rollbackToSavepoint(&txn, "sp1");

    // Transaction should still be active
    try std.testing.expectEqual(TxnState.active, txn.state);

    // Commit
    try tm.commit(&txn);
}

test "read-only transaction cannot write" {
    const allocator = std.testing.allocator;

    const vfs = @import("../storage/vfs.zig");
    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const wal_path = "/tmp/lattice_txn_readonly_test.wal";
    vfs_impl.delete(wal_path) catch {};

    var uuid: [16]u8 = undefined;
    std.crypto.random.bytes(&uuid);

    var wal = try WalManager.init(allocator, vfs_impl, wal_path, uuid);
    defer {
        wal.deinit();
        vfs_impl.delete(wal_path) catch {};
    }

    var tm = TxnManager.init(allocator, &wal);
    defer tm.deinit();

    var txn = try tm.begin(.read_only, .snapshot);

    // Should fail to log an operation
    const result = tm.logOperation(&txn, .insert, "data");
    try std.testing.expectError(TxnError.ReadOnly, result);

    try tm.commit(&txn);
}

test "log operations update prev_lsn chain" {
    const allocator = std.testing.allocator;

    const vfs = @import("../storage/vfs.zig");
    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const wal_path = "/tmp/lattice_txn_chain_test.wal";
    vfs_impl.delete(wal_path) catch {};

    var uuid: [16]u8 = undefined;
    std.crypto.random.bytes(&uuid);

    var wal = try WalManager.init(allocator, vfs_impl, wal_path, uuid);
    defer {
        wal.deinit();
        vfs_impl.delete(wal_path) catch {};
    }

    var tm = TxnManager.init(allocator, &wal);
    defer tm.deinit();

    var txn = try tm.begin(.read_write, .snapshot);

    // Log multiple operations
    const lsn1 = try tm.logOperation(&txn, .insert, "op1");
    const lsn2 = try tm.logOperation(&txn, .update, "op2");
    const lsn3 = try tm.logOperation(&txn, .delete, "op3");

    // LSNs should be sequential
    try std.testing.expect(lsn2 > lsn1);
    try std.testing.expect(lsn3 > lsn2);

    try tm.commit(&txn);

    // Verify chain in WAL
    var iter = wal.iterate(1);
    var buf: [256]u8 = undefined;

    // Skip TXN_BEGIN
    _ = try iter.next(&buf);

    // Check INSERT has prev_lsn pointing to BEGIN
    const rec1 = (try iter.next(&buf)).?;
    try std.testing.expectEqual(wal_mod.WalRecordType.insert, rec1.header.record_type);

    // Check UPDATE has prev_lsn pointing to INSERT
    const rec2 = (try iter.next(&buf)).?;
    try std.testing.expectEqual(wal_mod.WalRecordType.update, rec2.header.record_type);
    try std.testing.expectEqual(lsn1, rec2.header.prev_lsn);

    // Check DELETE has prev_lsn pointing to UPDATE
    const rec3 = (try iter.next(&buf)).?;
    try std.testing.expectEqual(wal_mod.WalRecordType.delete, rec3.header.record_type);
    try std.testing.expectEqual(lsn2, rec3.header.prev_lsn);
}
