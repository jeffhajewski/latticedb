//! Crash Recovery for Lattice database.
//!
//! Recovers the database to a consistent state after a crash by replaying
//! the WAL from the last checkpoint. Committed transactions are redone,
//! uncommitted transactions are ignored (their changes weren't applied).

const std = @import("std");
const lattice = @import("lattice");

const Allocator = std.mem.Allocator;

const wal_mod = lattice.storage.wal;
const page_manager = lattice.storage.page_manager;
const buffer_pool = lattice.storage.buffer_pool;
const page = lattice.storage.page;

const WalManager = wal_mod.WalManager;
const WalIterator = wal_mod.WalIterator;
const WalRecord = wal_mod.WalRecord;
const WalRecordType = wal_mod.WalRecordType;
const WalError = wal_mod.WalError;
const PageManager = page_manager.PageManager;
const BufferPool = buffer_pool.BufferPool;
const PageHeader = page.PageHeader;

// Graph layer for logical redo
const node_mod = lattice.graph.node;
const edge_mod = lattice.graph.edge;
const symbols_mod = lattice.graph.symbols;
const NodeStore = node_mod.NodeStore;
const EdgeStore = edge_mod.EdgeStore;
const SymbolTable = symbols_mod.SymbolTable;

// WAL payload deserialization
const wal_payload = lattice.transaction.wal_payload;

// ============================================================================
// Types
// ============================================================================

/// Recovery errors
pub const RecoveryError = error{
    /// WAL is corrupted (torn write at end, safe to proceed)
    CorruptedWal,
    /// WAL has corruption with valid data after it (real corruption, unsafe)
    MidLogCorruption,
    /// WAL UUID doesn't match database
    WalMismatch,
    /// I/O error during recovery
    IoError,
    /// Out of memory
    OutOfMemory,
    /// Invalid record in WAL
    InvalidRecord,
};

/// Transaction state during recovery
const TxnRecoveryState = enum {
    /// Transaction started but no outcome yet
    in_progress,
    /// Transaction committed
    committed,
    /// Transaction aborted
    aborted,
};

/// Information about a transaction during recovery
const TxnRecoveryInfo = struct {
    state: TxnRecoveryState,
    /// First LSN of this transaction
    first_lsn: u64,
    /// Last LSN of this transaction
    last_lsn: u64,
};

/// Recovery statistics
pub const RecoveryStats = struct {
    /// Starting LSN for recovery (checkpoint_lsn)
    start_lsn: u64,
    /// Ending LSN (last valid record)
    end_lsn: u64,
    /// Number of records scanned
    records_scanned: u64,
    /// Number of transactions found
    transactions_found: u32,
    /// Number of committed transactions
    transactions_committed: u32,
    /// Number of aborted transactions
    transactions_aborted: u32,
    /// Number of in-progress transactions (rolled back)
    transactions_rolled_back: u32,
    /// Number of redo operations applied
    redo_operations: u64,
    /// Time taken in nanoseconds
    duration_ns: u64,
    /// Whether recovery stopped due to tail corruption (torn write)
    stopped_at_corruption: bool,
    /// Frame number where corruption was detected (if any)
    corrupted_frame: ?u64,
};

/// Context for logical redo operations (optional)
/// When provided, recovery can redo insert/update/delete operations
pub const LogicalRecoveryContext = struct {
    node_store: *NodeStore,
    edge_store: *EdgeStore,
    symbol_table: *SymbolTable,
};

// ============================================================================
// Recovery Manager
// ============================================================================

/// Manages crash recovery
pub const RecoveryManager = struct {
    allocator: Allocator,
    /// Optional context for logical redo
    logical_ctx: ?LogicalRecoveryContext,

    const Self = @This();

    /// Initialize the recovery manager
    pub fn init(allocator: Allocator) Self {
        return Self{
            .allocator = allocator,
            .logical_ctx = null,
        };
    }

    /// Initialize with logical recovery context
    pub fn initWithLogicalContext(allocator: Allocator, ctx: LogicalRecoveryContext) Self {
        return Self{
            .allocator = allocator,
            .logical_ctx = ctx,
        };
    }

    /// Check if there are valid frames after the given frame number.
    /// Used to distinguish tail corruption (torn write) from mid-log corruption.
    fn hasValidFramesAfter(self: *Self, wal: *WalManager, corrupted_frame: u64) RecoveryError!bool {
        _ = self;

        const frame_count = wal.header.frame_count;

        // Check up to 10 frames ahead (or until end of WAL)
        // If any are valid, we have mid-log corruption
        const max_scan = @min(corrupted_frame + 10, frame_count);

        var frame_buf: [wal_mod.FRAME_SIZE]u8 align(4096) = undefined;

        var frame_num = corrupted_frame + 1;
        while (frame_num < max_scan) : (frame_num += 1) {
            // Read frame directly
            const offset = wal_mod.FRAME_SIZE + frame_num * wal_mod.FRAME_SIZE;
            const n = wal.file.read(offset, &frame_buf) catch {
                continue; // IO error, try next frame
            };

            if (n != wal_mod.FRAME_SIZE) {
                continue; // Incomplete read, try next frame
            }

            // Parse frame header and verify checksum
            const frame_header = std.mem.bytesAsValue(
                wal_mod.WalFrameHeader,
                frame_buf[0..@sizeOf(wal_mod.WalFrameHeader)],
            ).*;

            // Validate checksum
            if (frame_header.data_size > 0 and frame_header.data_size <= wal_mod.FRAME_DATA_SIZE) {
                const data = frame_buf[@sizeOf(wal_mod.WalFrameHeader)..][0..frame_header.data_size];
                const expected = page.calculateChecksum(data);

                if (frame_header.checksum == expected) {
                    // Found a valid frame after corruption - this is mid-log corruption
                    return true;
                }
            }
        }

        // No valid frames found after corruption - this is tail corruption
        return false;
    }

    /// Perform crash recovery
    /// Returns statistics about the recovery process
    pub fn recover(self: *Self, wal: *WalManager, pm: *PageManager) RecoveryError!RecoveryStats {
        const start_time = std.time.nanoTimestamp();

        var stats = RecoveryStats{
            .start_lsn = wal.getCheckpointLsn(),
            .end_lsn = 0,
            .records_scanned = 0,
            .transactions_found = 0,
            .transactions_committed = 0,
            .transactions_aborted = 0,
            .transactions_rolled_back = 0,
            .redo_operations = 0,
            .duration_ns = 0,
            .stopped_at_corruption = false,
            .corrupted_frame = null,
        };

        // Phase 1: Analysis - scan WAL to determine transaction states
        var txn_states = std.AutoHashMap(u64, TxnRecoveryInfo).init(self.allocator);
        defer txn_states.deinit();

        var redo_list: std.ArrayListUnmanaged(RedoEntry) = .{};
        defer redo_list.deinit(self.allocator);

        try self.analysisPass(wal, &txn_states, &redo_list, &stats);

        // Count transaction outcomes
        var iter = txn_states.iterator();
        while (iter.next()) |entry| {
            switch (entry.value_ptr.state) {
                .committed => stats.transactions_committed += 1,
                .aborted => stats.transactions_aborted += 1,
                .in_progress => stats.transactions_rolled_back += 1,
            }
        }

        // Phase 2: Redo - apply committed operations
        try self.redoPass(pm, &txn_states, &redo_list, &stats);

        // Sync to ensure all changes are durable
        pm.sync() catch return RecoveryError.IoError;

        const end_time = std.time.nanoTimestamp();
        stats.duration_ns = @intCast(end_time - start_time);

        return stats;
    }

    /// Analysis pass: scan WAL and determine transaction states
    fn analysisPass(
        self: *Self,
        wal: *WalManager,
        txn_states: *std.AutoHashMap(u64, TxnRecoveryInfo),
        redo_list: *std.ArrayListUnmanaged(RedoEntry),
        stats: *RecoveryStats,
    ) RecoveryError!void {

        const start_lsn = if (stats.start_lsn > 0) stats.start_lsn else 1;
        var wal_iter = wal.iterate(start_lsn);

        var payload_buf: [4096]u8 = undefined;

        while (true) {
            const maybe_record = wal_iter.next(&payload_buf) catch |err| {
                switch (err) {
                    WalError.ChecksumMismatch => {
                        // Got corruption - need to determine if it's tail or mid-log
                        // The iterator's current_frame is the frame that failed
                        const corrupted_frame = wal_iter.current_frame;
                        stats.corrupted_frame = corrupted_frame;

                        // Scan ahead to check for valid frames after corruption
                        if (try self.hasValidFramesAfter(wal, corrupted_frame)) {
                            // Valid frames exist after corruption - this is real corruption
                            return RecoveryError.MidLogCorruption;
                        }

                        // No valid frames after - this is tail corruption (torn write)
                        stats.stopped_at_corruption = true;
                        break;
                    },
                    WalError.IoError => return RecoveryError.IoError,
                    else => return RecoveryError.CorruptedWal,
                }
            };

            const record = maybe_record orelse break;

            stats.records_scanned += 1;
            stats.end_lsn = record.header.lsn;

            const txn_id = record.header.txn_id;

            switch (record.header.record_type) {
                .txn_begin => {
                    // New transaction
                    txn_states.put(txn_id, TxnRecoveryInfo{
                        .state = .in_progress,
                        .first_lsn = record.header.lsn,
                        .last_lsn = record.header.lsn,
                    }) catch return RecoveryError.OutOfMemory;
                    stats.transactions_found += 1;
                },

                .txn_commit => {
                    // Transaction committed
                    if (txn_states.getPtr(txn_id)) |info| {
                        info.state = .committed;
                        info.last_lsn = record.header.lsn;
                    }
                },

                .txn_abort => {
                    // Transaction aborted
                    if (txn_states.getPtr(txn_id)) |info| {
                        info.state = .aborted;
                        info.last_lsn = record.header.lsn;
                    }
                },

                .insert, .update, .delete, .page_write => {
                    // Data modification - add to redo list
                    if (txn_states.getPtr(txn_id)) |info| {
                        info.last_lsn = record.header.lsn;
                    }

                    // Store for redo phase
                    const payload_copy = self.allocator.alloc(u8, record.payload.len) catch {
                        return RecoveryError.OutOfMemory;
                    };
                    @memcpy(payload_copy, record.payload);

                    redo_list.append(self.allocator, RedoEntry{
                        .txn_id = txn_id,
                        .lsn = record.header.lsn,
                        .record_type = record.header.record_type,
                        .payload = payload_copy,
                    }) catch return RecoveryError.OutOfMemory;
                },

                .checkpoint_begin, .checkpoint_end => {
                    // Checkpoint markers - informational only
                },

                .savepoint, .savepoint_rollback => {
                    // Savepoint operations - update last_lsn
                    if (txn_states.getPtr(txn_id)) |info| {
                        info.last_lsn = record.header.lsn;
                    }
                },

                .clr => {
                    // Compensation log record - for undo operations
                    // In our simple model, we don't need to process these specially
                },
            }
        }
    }

    /// Redo pass: apply committed operations
    fn redoPass(
        self: *Self,
        pm: *PageManager,
        txn_states: *std.AutoHashMap(u64, TxnRecoveryInfo),
        redo_list: *std.ArrayListUnmanaged(RedoEntry),
        stats: *RecoveryStats,
    ) RecoveryError!void {
        for (redo_list.items) |entry| {
            // Only redo if transaction committed
            const txn_info = txn_states.get(entry.txn_id) orelse continue;
            if (txn_info.state != .committed) {
                // Free the payload since we won't use it
                self.allocator.free(entry.payload);
                continue;
            }

            // Apply the operation
            switch (entry.record_type) {
                .page_write => {
                    // Full page write - payload contains page_id + page data
                    if (entry.payload.len >= 4) {
                        const page_id = std.mem.readInt(u32, entry.payload[0..4], .little);
                        const page_data = entry.payload[4..];

                        if (page_data.len == pm.getPageSize()) {
                            // Write page directly
                            var buf: [4096]u8 align(4096) = undefined;
                            @memcpy(buf[0..page_data.len], page_data);
                            pm.writePage(page_id, &buf) catch {
                                self.allocator.free(entry.payload);
                                return RecoveryError.IoError;
                            };
                            stats.redo_operations += 1;
                        }
                    }
                },

                .insert => {
                    if (self.logical_ctx) |ctx| {
                        self.redoInsert(ctx, entry.payload) catch {};
                    }
                    stats.redo_operations += 1;
                },

                .update => {
                    if (self.logical_ctx) |ctx| {
                        self.redoUpdate(ctx, entry.payload) catch {};
                    }
                    stats.redo_operations += 1;
                },

                .delete => {
                    if (self.logical_ctx) |ctx| {
                        self.redoDelete(ctx, entry.payload) catch {};
                    }
                    stats.redo_operations += 1;
                },

                else => {},
            }

            self.allocator.free(entry.payload);
        }
    }

    /// Redo an insert operation
    fn redoInsert(self: *Self, ctx: LogicalRecoveryContext, payload: []const u8) !void {
        if (payload.len == 0) return;

        const payload_type = payload[0];

        // Node insert
        if (payload_type == @intFromEnum(wal_payload.PayloadType.node_insert)) {
            const node_payload = wal_payload.deserializeNodeInsert(payload) catch return;
            // Intern label strings to get label IDs
            var label_ids: std.ArrayListUnmanaged(u16) = .{};
            defer label_ids.deinit(self.allocator);
            var iter = node_payload.labelIterator();
            while (iter.next()) |label| {
                const label_id = ctx.symbol_table.intern(label) catch return;
                label_ids.append(self.allocator, label_id) catch return;
            }
            // Re-create the node with its original ID
            ctx.node_store.createWithId(
                node_payload.node_id,
                label_ids.items,
                &[_]node_mod.Property{},
            ) catch {};
        }
        // Edge insert
        else if (payload_type == @intFromEnum(wal_payload.PayloadType.edge_insert)) {
            const edge_payload = wal_payload.deserializeEdgeInsert(payload) catch return;
            // Intern edge type string to get type_id
            const type_id = ctx.symbol_table.intern(edge_payload.edge_type) catch return;
            ctx.edge_store.create(
                edge_payload.source,
                edge_payload.target,
                type_id,
                &[_]node_mod.Property{},
            ) catch {};
        }
    }

    /// Redo an update operation
    fn redoUpdate(self: *Self, ctx: LogicalRecoveryContext, payload: []const u8) !void {
        if (payload.len == 0) return;

        const payload_type = payload[0];

        // Property update
        if (payload_type == @intFromEnum(wal_payload.PayloadType.node_update)) {
            const update_payload = wal_payload.deserializePropertyUpdate(payload) catch return;

            // Intern the property key string to get key_id
            const key_id = ctx.symbol_table.intern(update_payload.key) catch return;

            // Get the current node (should already exist from redoInsert)
            var node = ctx.node_store.get(update_payload.node_id) catch return;
            defer node.deinit(self.allocator);

            if (update_payload.new_value.len == 0) {
                // Property removed — rebuild properties without this key
                var new_props: std.ArrayListUnmanaged(node_mod.Property) = .empty;
                defer new_props.deinit(self.allocator);
                for (node.properties) |prop| {
                    if (prop.key_id != key_id) {
                        new_props.append(self.allocator, prop) catch return;
                    }
                }
                ctx.node_store.update(update_payload.node_id, node.labels, new_props.items) catch {};
            } else {
                // Deserialize the new property value from WAL bytes
                var new_value = wal_payload.deserializePropertyValueFromBytes(
                    self.allocator,
                    update_payload.new_value,
                ) catch return;
                defer new_value.deinit(self.allocator);

                // Build updated properties list: replace existing or append new
                var new_props: std.ArrayListUnmanaged(node_mod.Property) = .empty;
                defer new_props.deinit(self.allocator);
                var found = false;
                for (node.properties) |prop| {
                    if (prop.key_id == key_id) {
                        new_props.append(self.allocator, .{ .key_id = key_id, .value = new_value }) catch return;
                        found = true;
                    } else {
                        new_props.append(self.allocator, prop) catch return;
                    }
                }
                if (!found) {
                    new_props.append(self.allocator, .{ .key_id = key_id, .value = new_value }) catch return;
                }
                ctx.node_store.update(update_payload.node_id, node.labels, new_props.items) catch {};
            }
        }
    }

    /// Redo a delete operation
    fn redoDelete(self: *Self, ctx: LogicalRecoveryContext, payload: []const u8) !void {
        _ = self;
        if (payload.len == 0) return;

        const payload_type = payload[0];

        // Node delete
        if (payload_type == @intFromEnum(wal_payload.PayloadType.node_delete)) {
            const node_payload = wal_payload.deserializeNodeDelete(payload) catch return;
            ctx.node_store.delete(node_payload.node_id) catch {};
        }
        // Edge delete
        else if (payload_type == @intFromEnum(wal_payload.PayloadType.edge_delete)) {
            const edge_payload = wal_payload.deserializeEdgeDelete(payload) catch return;
            ctx.edge_store.delete(
                edge_payload.source,
                edge_payload.target,
                edge_payload.type_id,
            ) catch {};
        }
    }
};

/// Entry in the redo list
const RedoEntry = struct {
    txn_id: u64,
    lsn: u64,
    record_type: WalRecordType,
    payload: []u8,
};

// ============================================================================
// Convenience function
// ============================================================================

/// Perform recovery on database open
pub fn recoverDatabase(
    allocator: Allocator,
    wal: *WalManager,
    pm: *PageManager,
) RecoveryError!RecoveryStats {
    var rm = RecoveryManager.init(allocator);
    return rm.recover(wal, pm);
}

// ============================================================================
// Tests
// ============================================================================

test "recovery with no WAL records" {
    const allocator = std.testing.allocator;

    const vfs = @import("vfs.zig");
    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const db_path = "/tmp/lattice_recovery_empty_test.db";
    const wal_path = "/tmp/lattice_recovery_empty_test.wal";

    vfs_impl.delete(db_path) catch {};
    vfs_impl.delete(wal_path) catch {};

    var pm = try PageManager.init(allocator, vfs_impl, db_path, .{ .create = true });
    defer {
        pm.deinit();
        vfs_impl.delete(db_path) catch {};
    }

    var uuid: [16]u8 = undefined;
    std.crypto.random.bytes(&uuid);

    var wal = try WalManager.init(allocator, vfs_impl, wal_path, uuid);
    defer {
        wal.deinit();
        vfs_impl.delete(wal_path) catch {};
    }

    // Recovery with empty WAL
    const stats = try recoverDatabase(allocator, &wal, &pm);

    try std.testing.expectEqual(@as(u64, 0), stats.records_scanned);
    try std.testing.expectEqual(@as(u32, 0), stats.transactions_found);
}

test "recovery with committed transaction" {
    const allocator = std.testing.allocator;

    const vfs = @import("vfs.zig");
    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const db_path = "/tmp/lattice_recovery_commit_test.db";
    const wal_path = "/tmp/lattice_recovery_commit_test.wal";

    vfs_impl.delete(db_path) catch {};
    vfs_impl.delete(wal_path) catch {};

    var pm = try PageManager.init(allocator, vfs_impl, db_path, .{ .create = true });
    defer {
        pm.deinit();
        vfs_impl.delete(db_path) catch {};
    }

    var uuid: [16]u8 = undefined;
    std.crypto.random.bytes(&uuid);

    var wal = try WalManager.init(allocator, vfs_impl, wal_path, uuid);
    defer {
        wal.deinit();
        vfs_impl.delete(wal_path) catch {};
    }

    // Simulate a committed transaction
    _ = try wal.appendRecord(.txn_begin, 1, 0, &[_]u8{});
    _ = try wal.appendRecord(.insert, 1, 1, "test data");
    _ = try wal.appendRecord(.txn_commit, 1, 2, &[_]u8{});
    try wal.sync();

    // Recovery
    const stats = try recoverDatabase(allocator, &wal, &pm);

    try std.testing.expectEqual(@as(u64, 3), stats.records_scanned);
    try std.testing.expectEqual(@as(u32, 1), stats.transactions_found);
    try std.testing.expectEqual(@as(u32, 1), stats.transactions_committed);
    try std.testing.expectEqual(@as(u32, 0), stats.transactions_aborted);
    try std.testing.expectEqual(@as(u32, 0), stats.transactions_rolled_back);
}

test "recovery with aborted transaction" {
    const allocator = std.testing.allocator;

    const vfs = @import("vfs.zig");
    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const db_path = "/tmp/lattice_recovery_abort_test.db";
    const wal_path = "/tmp/lattice_recovery_abort_test.wal";

    vfs_impl.delete(db_path) catch {};
    vfs_impl.delete(wal_path) catch {};

    var pm = try PageManager.init(allocator, vfs_impl, db_path, .{ .create = true });
    defer {
        pm.deinit();
        vfs_impl.delete(db_path) catch {};
    }

    var uuid: [16]u8 = undefined;
    std.crypto.random.bytes(&uuid);

    var wal = try WalManager.init(allocator, vfs_impl, wal_path, uuid);
    defer {
        wal.deinit();
        vfs_impl.delete(wal_path) catch {};
    }

    // Simulate an aborted transaction
    _ = try wal.appendRecord(.txn_begin, 1, 0, &[_]u8{});
    _ = try wal.appendRecord(.insert, 1, 1, "test data");
    _ = try wal.appendRecord(.txn_abort, 1, 2, &[_]u8{});
    try wal.sync();

    // Recovery
    const stats = try recoverDatabase(allocator, &wal, &pm);

    try std.testing.expectEqual(@as(u32, 1), stats.transactions_found);
    try std.testing.expectEqual(@as(u32, 0), stats.transactions_committed);
    try std.testing.expectEqual(@as(u32, 1), stats.transactions_aborted);
}

test "recovery with uncommitted transaction (crash simulation)" {
    const allocator = std.testing.allocator;

    const vfs = @import("vfs.zig");
    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const db_path = "/tmp/lattice_recovery_crash_test.db";
    const wal_path = "/tmp/lattice_recovery_crash_test.wal";

    vfs_impl.delete(db_path) catch {};
    vfs_impl.delete(wal_path) catch {};

    var pm = try PageManager.init(allocator, vfs_impl, db_path, .{ .create = true });
    defer {
        pm.deinit();
        vfs_impl.delete(db_path) catch {};
    }

    var uuid: [16]u8 = undefined;
    std.crypto.random.bytes(&uuid);

    var wal = try WalManager.init(allocator, vfs_impl, wal_path, uuid);
    defer {
        wal.deinit();
        vfs_impl.delete(wal_path) catch {};
    }

    // Simulate a transaction that was in progress when crash occurred
    _ = try wal.appendRecord(.txn_begin, 1, 0, &[_]u8{});
    _ = try wal.appendRecord(.insert, 1, 1, "test data 1");
    _ = try wal.appendRecord(.insert, 1, 2, "test data 2");
    // No commit or abort - simulates crash
    try wal.sync();

    // Recovery
    const stats = try recoverDatabase(allocator, &wal, &pm);

    try std.testing.expectEqual(@as(u32, 1), stats.transactions_found);
    try std.testing.expectEqual(@as(u32, 0), stats.transactions_committed);
    try std.testing.expectEqual(@as(u32, 0), stats.transactions_aborted);
    try std.testing.expectEqual(@as(u32, 1), stats.transactions_rolled_back);
}

test "recovery with mixed transactions" {
    const allocator = std.testing.allocator;

    const vfs = @import("vfs.zig");
    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const db_path = "/tmp/lattice_recovery_mixed_test.db";
    const wal_path = "/tmp/lattice_recovery_mixed_test.wal";

    vfs_impl.delete(db_path) catch {};
    vfs_impl.delete(wal_path) catch {};

    var pm = try PageManager.init(allocator, vfs_impl, db_path, .{ .create = true });
    defer {
        pm.deinit();
        vfs_impl.delete(db_path) catch {};
    }

    var uuid: [16]u8 = undefined;
    std.crypto.random.bytes(&uuid);

    var wal = try WalManager.init(allocator, vfs_impl, wal_path, uuid);
    defer {
        wal.deinit();
        vfs_impl.delete(wal_path) catch {};
    }

    // Txn 1: committed
    _ = try wal.appendRecord(.txn_begin, 1, 0, &[_]u8{});
    _ = try wal.appendRecord(.insert, 1, 1, "data1");

    // Txn 2: starts, interleaved with txn 1
    _ = try wal.appendRecord(.txn_begin, 2, 0, &[_]u8{});
    _ = try wal.appendRecord(.insert, 2, 3, "data2");

    // Txn 1: commits
    _ = try wal.appendRecord(.txn_commit, 1, 2, &[_]u8{});

    // Txn 2: more work, then abort
    _ = try wal.appendRecord(.update, 2, 4, "data2_updated");
    _ = try wal.appendRecord(.txn_abort, 2, 6, &[_]u8{});

    // Txn 3: in progress (uncommitted)
    _ = try wal.appendRecord(.txn_begin, 3, 0, &[_]u8{});
    _ = try wal.appendRecord(.delete, 3, 8, "key3");
    // No commit - crash

    try wal.sync();

    // Recovery
    const stats = try recoverDatabase(allocator, &wal, &pm);

    try std.testing.expectEqual(@as(u32, 3), stats.transactions_found);
    try std.testing.expectEqual(@as(u32, 1), stats.transactions_committed);
    try std.testing.expectEqual(@as(u32, 1), stats.transactions_aborted);
    try std.testing.expectEqual(@as(u32, 1), stats.transactions_rolled_back);
}

test "recovery respects checkpoint_lsn" {
    const allocator = std.testing.allocator;

    const vfs = @import("vfs.zig");
    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const db_path = "/tmp/lattice_recovery_checkpoint_test.db";
    const wal_path = "/tmp/lattice_recovery_checkpoint_test.wal";

    vfs_impl.delete(db_path) catch {};
    vfs_impl.delete(wal_path) catch {};

    var pm = try PageManager.init(allocator, vfs_impl, db_path, .{ .create = true });
    defer {
        pm.deinit();
        vfs_impl.delete(db_path) catch {};
    }

    var uuid: [16]u8 = undefined;
    std.crypto.random.bytes(&uuid);

    var wal = try WalManager.init(allocator, vfs_impl, wal_path, uuid);
    defer {
        wal.deinit();
        vfs_impl.delete(wal_path) catch {};
    }

    // Old transaction (before checkpoint)
    _ = try wal.appendRecord(.txn_begin, 1, 0, &[_]u8{});
    _ = try wal.appendRecord(.insert, 1, 1, "old data");
    _ = try wal.appendRecord(.txn_commit, 1, 2, &[_]u8{});

    // Set checkpoint after txn 1
    try wal.setCheckpointLsn(3);

    // New transaction (after checkpoint)
    _ = try wal.appendRecord(.txn_begin, 2, 0, &[_]u8{});
    _ = try wal.appendRecord(.insert, 2, 4, "new data");
    _ = try wal.appendRecord(.txn_commit, 2, 5, &[_]u8{});

    try wal.sync();

    // Recovery should only see txn 2 (after checkpoint)
    const stats = try recoverDatabase(allocator, &wal, &pm);

    try std.testing.expectEqual(@as(u64, 3), stats.start_lsn);
    try std.testing.expectEqual(@as(u32, 1), stats.transactions_found);
    try std.testing.expectEqual(@as(u32, 1), stats.transactions_committed);
}

test "recovery handles tail corruption (torn write)" {
    const allocator = std.testing.allocator;

    const vfs = @import("vfs.zig");
    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const db_path = "/tmp/lattice_recovery_tail_corrupt_test.db";
    const wal_path = "/tmp/lattice_recovery_tail_corrupt_test.wal";

    vfs_impl.delete(db_path) catch {};
    vfs_impl.delete(wal_path) catch {};

    var pm = try PageManager.init(allocator, vfs_impl, db_path, .{ .create = true });
    defer {
        pm.deinit();
        vfs_impl.delete(db_path) catch {};
    }

    var uuid: [16]u8 = undefined;
    std.crypto.random.bytes(&uuid);

    var wal = try WalManager.init(allocator, vfs_impl, wal_path, uuid);

    // Write a committed transaction
    _ = try wal.appendRecord(.txn_begin, 1, 0, &[_]u8{});
    _ = try wal.appendRecord(.insert, 1, 1, "test data");
    _ = try wal.appendRecord(.txn_commit, 1, 2, &[_]u8{});
    try wal.sync();

    // Corrupt the last frame by writing garbage after the valid data
    // This simulates a torn write at the end of WAL
    const frame_count = wal.header.frame_count;
    if (frame_count > 0) {
        // Write garbage to corrupt the last frame's checksum
        const last_frame_offset = wal_mod.FRAME_SIZE + (frame_count - 1) * wal_mod.FRAME_SIZE;
        var garbage: [64]u8 = undefined;
        @memset(&garbage, 0xFF);
        // Write garbage to data area (after header) to corrupt checksum
        _ = wal.file.write(last_frame_offset + @sizeOf(wal_mod.WalFrameHeader), &garbage) catch {};
    }

    // Close and reopen to simulate crash recovery
    wal.deinit();

    var wal2 = try WalManager.init(allocator, vfs_impl, wal_path, uuid);
    defer {
        wal2.deinit();
        vfs_impl.delete(wal_path) catch {};
    }

    // Recovery should succeed (tail corruption is tolerated)
    const stats = try recoverDatabase(allocator, &wal2, &pm);

    // Should indicate we stopped at corruption
    try std.testing.expect(stats.stopped_at_corruption);
    try std.testing.expect(stats.corrupted_frame != null);
}

test "recovery fails on mid-log corruption" {
    const allocator = std.testing.allocator;

    const vfs = @import("vfs.zig");
    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const db_path = "/tmp/lattice_recovery_midlog_corrupt_test.db";
    const wal_path = "/tmp/lattice_recovery_midlog_corrupt_test.wal";

    vfs_impl.delete(db_path) catch {};
    vfs_impl.delete(wal_path) catch {};

    var pm = try PageManager.init(allocator, vfs_impl, db_path, .{ .create = true });
    defer {
        pm.deinit();
        vfs_impl.delete(db_path) catch {};
    }

    var uuid: [16]u8 = undefined;
    std.crypto.random.bytes(&uuid);

    var wal = try WalManager.init(allocator, vfs_impl, wal_path, uuid);

    // Write first transaction and sync (frame 0)
    _ = try wal.appendRecord(.txn_begin, 1, 0, &[_]u8{});
    _ = try wal.appendRecord(.insert, 1, 1, "data1");
    _ = try wal.appendRecord(.txn_commit, 1, 2, &[_]u8{});
    try wal.sync();

    // Write second transaction and sync (frame 1)
    _ = try wal.appendRecord(.txn_begin, 2, 0, &[_]u8{});
    _ = try wal.appendRecord(.insert, 2, 4, "data2");
    _ = try wal.appendRecord(.txn_commit, 2, 5, &[_]u8{});
    try wal.sync();

    // Write third transaction and sync (frame 2)
    _ = try wal.appendRecord(.txn_begin, 3, 0, &[_]u8{});
    _ = try wal.appendRecord(.insert, 3, 7, "data3");
    _ = try wal.appendRecord(.txn_commit, 3, 8, &[_]u8{});
    try wal.sync();

    const frame_count = wal.header.frame_count;

    // Corrupt frame 1 (middle frame) - frame 2 will still be valid after it
    if (frame_count >= 2) {
        const mid_frame_offset = wal_mod.FRAME_SIZE + 0 * wal_mod.FRAME_SIZE; // Corrupt frame 0
        var garbage: [64]u8 = undefined;
        @memset(&garbage, 0xFF);
        // Write garbage to data area to corrupt checksum
        _ = wal.file.write(mid_frame_offset + @sizeOf(wal_mod.WalFrameHeader), &garbage) catch {};
    }

    // Close and reopen
    wal.deinit();

    var wal2 = try WalManager.init(allocator, vfs_impl, wal_path, uuid);
    defer {
        wal2.deinit();
        vfs_impl.delete(wal_path) catch {};
    }

    // Recovery should FAIL with MidLogCorruption because valid frames exist after corruption
    const result = recoverDatabase(allocator, &wal2, &pm);
    try std.testing.expectError(RecoveryError.MidLogCorruption, result);
}
