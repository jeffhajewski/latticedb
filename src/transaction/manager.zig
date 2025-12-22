//! Transaction manager for Lattice database.
//!
//! Provides ACID transactions with MVCC (Multi-Version Concurrency Control).

const std = @import("std");
const types = @import("../core/types.zig");

pub const NodeId = types.NodeId;
pub const PageId = types.PageId;

/// Transaction state
pub const TxnState = enum {
    /// Transaction is active and accepting operations
    active,
    /// Transaction has been committed
    committed,
    /// Transaction has been aborted/rolled back
    aborted,
};

/// Transaction mode
pub const TxnMode = enum {
    /// Read-only transaction (no modifications allowed)
    read_only,
    /// Read-write transaction (can modify data)
    read_write,
};

/// Transaction isolation level
pub const IsolationLevel = enum {
    /// Read committed - sees only committed data
    read_committed,
    /// Snapshot isolation - sees consistent snapshot
    snapshot,
    /// Serializable - full isolation
    serializable,
};

/// Transaction handle
pub const Transaction = struct {
    /// Unique transaction ID
    id: u64,
    /// Current state
    state: TxnState,
    /// Transaction mode
    mode: TxnMode,
    /// Isolation level
    isolation: IsolationLevel,
    /// Start timestamp (for MVCC)
    start_timestamp: u64,
    /// Commit timestamp (set on commit)
    commit_timestamp: u64,
    /// Savepoint stack depth
    savepoint_depth: u32,
};

/// Savepoint for partial rollback
pub const Savepoint = struct {
    /// Savepoint name
    name: []const u8,
    /// Transaction ID
    txn_id: u64,
    /// Log sequence number at savepoint
    lsn: u64,
    /// Depth in savepoint stack
    depth: u32,
};

/// Write-ahead log record types
pub const WalRecordType = enum(u8) {
    /// Begin transaction
    begin = 0x01,
    /// Commit transaction
    commit = 0x02,
    /// Abort transaction
    abort = 0x03,
    /// Insert node
    node_insert = 0x10,
    /// Delete node
    node_delete = 0x11,
    /// Update node property
    node_update = 0x12,
    /// Insert edge
    edge_insert = 0x20,
    /// Delete edge
    edge_delete = 0x21,
    /// Page write (for redo)
    page_write = 0x30,
    /// Checkpoint start
    checkpoint_begin = 0x40,
    /// Checkpoint end
    checkpoint_end = 0x41,
    /// Savepoint
    savepoint = 0x50,
    /// Rollback to savepoint
    savepoint_rollback = 0x51,
};

/// WAL record header
pub const WalRecordHeader = extern struct {
    /// Record type
    record_type: WalRecordType,
    /// Flags
    flags: u8,
    /// Record length (excluding header)
    length: u16,
    /// Transaction ID
    txn_id: u64,
    /// Log sequence number
    lsn: u64,
    /// Previous LSN for this transaction
    prev_lsn: u64,
    /// CRC32 checksum
    checksum: u32,
};

/// Transaction statistics
pub const TxnStats = struct {
    active_transactions: u32,
    committed_transactions: u64,
    aborted_transactions: u64,
    oldest_active_txn: u64,
};

test "transaction state transitions" {
    var txn = Transaction{
        .id = 1,
        .state = .active,
        .mode = .read_write,
        .isolation = .snapshot,
        .start_timestamp = 100,
        .commit_timestamp = 0,
        .savepoint_depth = 0,
    };
    try std.testing.expectEqual(TxnState.active, txn.state);
    txn.state = .committed;
    try std.testing.expectEqual(TxnState.committed, txn.state);
}

test "wal record header size" {
    // Size may include padding for alignment; verify it's at least 32 bytes
    try std.testing.expect(@sizeOf(WalRecordHeader) >= 32);
}
