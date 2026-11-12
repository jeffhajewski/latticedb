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
const label_index_mod = lattice.graph.label_index;
const NodeStore = node_mod.NodeStore;
const EdgeStore = edge_mod.EdgeStore;
const SymbolTable = symbols_mod.SymbolTable;
const LabelIndex = label_index_mod.LabelIndex;
const PropertyValue = lattice.core.types.PropertyValue;

// WAL payload deserialization
const wal_payload = lattice.transaction.wal_payload;
const stream_store = lattice.stream.store;
const stream_payload = lattice.stream.payload;

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
    /// The log holds vector changes and the database was opened without
    /// its vector index. Not damage: opening with vectors enabled replays
    /// them.
    NeedsVectorIndex,
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
    /// Largest transaction id seen in the log. A new session has to number
    /// its transactions above this, or a later recovery would read two
    /// sessions' records as one transaction.
    max_txn_id: u64 = 0,
};

/// Context for logical redo operations (optional)
/// When provided, recovery can redo insert/update/delete operations
pub const LogicalRecoveryContext = struct {
    node_store: *NodeStore,
    edge_store: *EdgeStore,
    symbol_table: *SymbolTable,
    label_index: *LabelIndex,
    stream_trees: ?stream_store.TreeSet = null,
    hnsw_index: ?*lattice.vector.hnsw.HnswIndex = null,
    /// Whether the database file holds vector data. Decides what a vector
    /// removal means when there is no index to apply it to. Defaults to the
    /// answer that refuses.
    file_has_vectors: bool = true,
    /// Called between redo operations, when every tree is whole. Lets the
    /// owner flush, so that replaying a long log does not have to hold all of
    /// it in dirty pages.
    between_operations: ?Hook = null,

    pub const Hook = struct {
        context: *anyopaque,
        call: *const fn (context: *anyopaque) void,
    };
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
    /// Opening the log has already sorted damage into a torn tail, which it
    /// cut off, and corruption, which it recorded in `damaged_frame`. A
    /// frame that fails here went bad after the open, and is judged by the
    /// legacy rule: any frame that verifies after it makes it corruption.
    fn hasValidFramesAfter(self: *Self, wal: *WalManager, corrupted_frame: u64) RecoveryError!bool {
        _ = self;

        var frame_buf: [wal_mod.MAX_FRAME_SIZE]u8 align(4096) = undefined;

        var frame_num = corrupted_frame + 1;
        while (frame_num < wal.header.frame_count) : (frame_num += 1) {
            const offset = @as(u64, @sizeOf(wal_mod.WalHeader)) + frame_num * wal.frame_size;
            const frame = frame_buf[0..wal.frame_size];
            const n = wal.file.read(offset, frame) catch continue;
            if (n != wal.frame_size) continue;
            if (wal_mod.classifyFrame(frame) != .invalid) return true;
        }
        return false;
    }

    /// Perform crash recovery
    /// Returns statistics about the recovery process
    pub fn recover(self: *Self, wal: *WalManager, pm: *PageManager) RecoveryError!RecoveryStats {
        const start_time = @import("compat").nanoTimestamp();

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

        var redo_list: std.ArrayListUnmanaged(RedoEntry) = .empty;
        defer {
            for (redo_list.items) |entry| self.allocator.free(entry.payload);
            redo_list.deinit(self.allocator);
        }

        // Damage the open found in frames the log had promised were synced.
        if (wal.damaged_frame) |frame| {
            stats.corrupted_frame = frame;
            return RecoveryError.MidLogCorruption;
        }

        try self.analysisPass(wal, &txn_states, &redo_list, &stats);

        // A torn tail that was cut off when the log was opened is the same
        // event as one met while reading it.
        if (wal.trimmed_tail_frame) |frame| {
            stats.stopped_at_corruption = true;
            if (stats.corrupted_frame == null) stats.corrupted_frame = frame;
        }

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

        const end_time = @import("compat").nanoTimestamp();
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

        while (true) {
            const owned_record = wal_iter.nextAlloc(self.allocator) catch |err| {
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

            const record = owned_record orelse break;
            var payload_taken = false;
            defer if (!payload_taken) {
                var mutable_record = record;
                mutable_record.deinit();
            };

            stats.records_scanned += 1;
            stats.end_lsn = record.header.lsn;

            const txn_id = record.header.txn_id;
            stats.max_txn_id = @max(stats.max_txn_id, txn_id);

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

                    // Store for redo phase. `nextAlloc` already returned an
                    // owned logical payload, including any reassembled large
                    // fragment sequence.
                    redo_list.append(self.allocator, RedoEntry{
                        .txn_id = txn_id,
                        .lsn = record.header.lsn,
                        .record_type = record.header.record_type,
                        .payload = record.payload,
                    }) catch return RecoveryError.OutOfMemory;
                    payload_taken = true;
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

                .large_fragment => unreachable,
            }
        }
    }

    /// Redo pass: apply committed operations.
    ///
    /// Every record is applied as an assignment that gives the same result
    /// whether or not the image already holds its change: the image can be
    /// from any flush since the last checkpoint, so some records are already
    /// in it and some are not, and recovery does not know which. For each
    /// kind of record, what "already applied" looks like:
    ///
    /// - node insert: the node exists. It is set to the logged labels and no
    ///   properties; the properties the insert committed are the property
    ///   records that follow it, and any later change is later in the log.
    /// - node property set or removal, label add or removal: the node holds
    ///   the value, or lacks the key or label. A node that is missing
    ///   altogether is accepted only when a later committed record in this
    ///   log deletes it: the image is then from after that delete.
    /// - node delete: the node is missing.
    /// - edge insert: the edge exists. It is set to the logged endpoints and
    ///   type and no properties, which follow as records of their own.
    /// - edge property set or removal: as for a node, including the rule for
    ///   an edge that a later record deletes.
    /// - edge delete: the edge is missing. An edge snapshot logged as an
    ///   update replaces the edge outright.
    /// - vector set: the index holds exactly that vector for the node.
    ///   Vector removal: the node has no vector.
    /// - stream append: the event is there with the same sequence. Offset
    ///   set: an assignment. Trim: nothing at or below the sequence.
    ///
    /// The label index is set alongside each node record, entry by entry,
    /// rather than trusted to follow it, because an image can hold a node and
    /// its index entries from different moments only through a bug, and
    /// replay is where that would otherwise become permanent. The property
    /// and full-text indexes are rebuilt after replay.
    ///
    /// Anything else that goes wrong fails the open: a payload that does not
    /// decode, a payload type no release writes, a store that reports an
    /// error. Skipping such a record would open a database missing a change
    /// that was acknowledged.
    fn redoPass(
        self: *Self,
        pm: *PageManager,
        txn_states: *std.AutoHashMap(u64, TxnRecoveryInfo),
        redo_list: *std.ArrayListUnmanaged(RedoEntry),
        stats: *RecoveryStats,
    ) RecoveryError!void {
        _ = pm;

        // For each node and edge a committed transaction deletes, the last
        // position in the log at which it does.
        var deletes = Deletes{};
        defer deletes.deinit(self.allocator);
        for (redo_list.items) |entry| {
            const txn_info = txn_states.get(entry.txn_id) orelse continue;
            if (txn_info.state != .committed) continue;
            if (entry.record_type != .delete or entry.payload.len == 0) continue;
            if (entry.payload[0] == @intFromEnum(wal_payload.PayloadType.node_delete)) {
                const p = wal_payload.deserializeNodeDelete(entry.payload) catch return RecoveryError.InvalidRecord;
                deletes.nodes.put(self.allocator, p.node_id, entry.lsn) catch return RecoveryError.OutOfMemory;
            } else if (entry.payload[0] == @intFromEnum(wal_payload.PayloadType.edge_delete)) {
                const p = wal_payload.deserializeEdgeDelete(entry.payload) catch return RecoveryError.InvalidRecord;
                deletes.edges.put(self.allocator, p.edge_id, entry.lsn) catch return RecoveryError.OutOfMemory;
            }
        }

        for (redo_list.items) |entry| {
            // Only redo if transaction committed
            const txn_info = txn_states.get(entry.txn_id) orelse continue;
            if (txn_info.state != .committed) {
                continue;
            }

            switch (entry.record_type) {
                .insert, .update, .delete => {
                    if (self.logical_ctx) |ctx| {
                        try self.redoRecord(ctx, &deletes, entry);
                    }
                    stats.redo_operations += 1;
                },
                // A whole page image. No release has written one: the log
                // holds logical records only.
                .page_write => return RecoveryError.InvalidRecord,
                else => {},
            }

            if (self.logical_ctx) |ctx| {
                if (ctx.between_operations) |hook| hook.call(hook.context);
            }
        }
    }

    const Deletes = struct {
        nodes: std.AutoHashMapUnmanaged(u64, u64) = .empty,
        edges: std.AutoHashMapUnmanaged(u64, u64) = .empty,

        fn deinit(self: *Deletes, allocator: Allocator) void {
            self.nodes.deinit(allocator);
            self.edges.deinit(allocator);
        }
    };

    /// A record that changes a node or edge found it missing. That is the
    /// change already applied only if a later record deletes it.
    fn missingIsDeletedLater(deleted_at: *const std.AutoHashMapUnmanaged(u64, u64), id: u64, lsn: u64) RecoveryError!void {
        const at = deleted_at.get(id) orelse return RecoveryError.InvalidRecord;
        if (at <= lsn) return RecoveryError.InvalidRecord;
    }

    fn redoRecord(self: *Self, ctx: LogicalRecoveryContext, deletes: *const Deletes, entry: RedoEntry) RecoveryError!void {
        const payload = entry.payload;
        if (payload.len == 0) return RecoveryError.InvalidRecord;
        const payload_type = std.enums.fromInt(wal_payload.PayloadType, payload[0]) orelse return RecoveryError.InvalidRecord;

        switch (entry.record_type) {
            .insert => switch (payload_type) {
                .node_insert => try self.redoNodeInsert(ctx, payload),
                .edge_insert => try self.redoEdgeInsert(ctx, payload),
                .stream_append => try self.redoStreamAppend(ctx, payload),
                else => return RecoveryError.InvalidRecord,
            },
            .update => switch (payload_type) {
                .node_update => try self.redoNodeProperty(ctx, deletes, entry.lsn, payload),
                .label_add, .label_remove => try self.redoLabel(ctx, deletes, entry.lsn, payload, payload_type == .label_add),
                .edge_property_update => try self.redoEdgeProperty(ctx, deletes, entry.lsn, payload),
                .edge_delete => try self.redoEdgeSnapshot(ctx, payload),
                .vector_set, .vector_remove => self.redoVector(ctx, payload) catch |err| return switch (err) {
                    error.OutOfMemory => RecoveryError.OutOfMemory,
                    error.InvalidRecord => RecoveryError.InvalidRecord,
                    error.NeedsVectorIndex => RecoveryError.NeedsVectorIndex,
                    else => RecoveryError.IoError,
                },
                .stream_offset_set => try self.redoStreamOffset(ctx, payload),
                else => return RecoveryError.InvalidRecord,
            },
            .delete => switch (payload_type) {
                .node_delete => try self.redoNodeDelete(ctx, payload),
                .edge_delete => try self.redoEdgeDelete(ctx, payload),
                .stream_trim => try self.redoStreamTrim(ctx, payload),
                else => return RecoveryError.InvalidRecord,
            },
            else => unreachable,
        }
    }

    fn internAll(self: *Self, ctx: LogicalRecoveryContext, names: anytype, out: *std.ArrayListUnmanaged(symbols_mod.SymbolId)) RecoveryError!void {
        var iter = names;
        while (iter.next()) |name| {
            const id = ctx.symbol_table.intern(name) catch return RecoveryError.IoError;
            out.append(self.allocator, id) catch return RecoveryError.OutOfMemory;
        }
    }

    fn containsId(ids: []const symbols_mod.SymbolId, id: symbols_mod.SymbolId) bool {
        return std.mem.indexOfScalar(symbols_mod.SymbolId, ids, id) != null;
    }

    fn labelIndexSet(ctx: LogicalRecoveryContext, label_id: symbols_mod.SymbolId, node_id: u64, present: bool) RecoveryError!void {
        const has = ctx.label_index.contains(label_id, node_id) catch return RecoveryError.IoError;
        if (present and !has) {
            ctx.label_index.add(label_id, node_id) catch return RecoveryError.IoError;
        } else if (!present and has) {
            ctx.label_index.remove(label_id, node_id) catch return RecoveryError.IoError;
        }
    }

    /// The node as the trees hold it now, or null if it is missing.
    fn getNode(ctx: LogicalRecoveryContext, node_id: u64) RecoveryError!?node_mod.Node {
        return ctx.node_store.get(node_id) catch |err| switch (err) {
            error.NotFound => null,
            error.OutOfMemory => RecoveryError.OutOfMemory,
            else => RecoveryError.IoError,
        };
    }

    fn getEdge(ctx: LogicalRecoveryContext, edge_id: u64) RecoveryError!?edge_mod.Edge {
        return ctx.edge_store.getById(edge_id) catch |err| switch (err) {
            error.NotFound => null,
            error.OutOfMemory => RecoveryError.OutOfMemory,
            else => RecoveryError.IoError,
        };
    }

    /// Make the node exist with exactly these labels and properties.
    fn putNode(ctx: LogicalRecoveryContext, node_id: u64, exists: bool, labels: []const symbols_mod.SymbolId, properties: []const node_mod.Property) RecoveryError!void {
        if (exists) {
            ctx.node_store.update(node_id, labels, properties) catch return RecoveryError.IoError;
        } else {
            ctx.node_store.createWithId(node_id, labels, properties) catch return RecoveryError.IoError;
        }
    }

    /// Make the edge exist with exactly these endpoints, type and properties.
    fn putEdge(ctx: LogicalRecoveryContext, edge_id: u64, exists: bool, source: u64, target: u64, edge_type: symbols_mod.SymbolId, properties: []const node_mod.Property) RecoveryError!void {
        if (exists) ctx.edge_store.deleteById(edge_id) catch return RecoveryError.IoError;
        ctx.edge_store.createWithId(edge_id, source, target, edge_type, properties) catch return RecoveryError.IoError;
    }

    fn redoNodeInsert(self: *Self, ctx: LogicalRecoveryContext, payload: []const u8) RecoveryError!void {
        const p = wal_payload.deserializeNodeInsert(payload) catch return RecoveryError.InvalidRecord;
        var labels: std.ArrayListUnmanaged(symbols_mod.SymbolId) = .empty;
        defer labels.deinit(self.allocator);
        try self.internAll(ctx, p.labelIterator(), &labels);

        var existing = try getNode(ctx, p.node_id);
        defer if (existing) |*node| node.deinit(self.allocator);
        if (existing) |node| {
            for (node.labels) |label_id| {
                if (!containsId(labels.items, label_id)) try labelIndexSet(ctx, label_id, p.node_id, false);
            }
        }
        try putNode(ctx, p.node_id, existing != null, labels.items, &.{});
        for (labels.items) |label_id| try labelIndexSet(ctx, label_id, p.node_id, true);
    }

    fn redoNodeDelete(self: *Self, ctx: LogicalRecoveryContext, payload: []const u8) RecoveryError!void {
        const p = wal_payload.deserializeNodeDelete(payload) catch return RecoveryError.InvalidRecord;
        var existing = (try getNode(ctx, p.node_id)) orelse return;
        defer existing.deinit(self.allocator);
        for (existing.labels) |label_id| try labelIndexSet(ctx, label_id, p.node_id, false);
        ctx.node_store.delete(p.node_id) catch return RecoveryError.IoError;
    }

    fn redoNodeProperty(self: *Self, ctx: LogicalRecoveryContext, deletes: *const Deletes, lsn: u64, payload: []const u8) RecoveryError!void {
        const p = wal_payload.deserializePropertyUpdate(payload) catch return RecoveryError.InvalidRecord;
        var node = (try getNode(ctx, p.node_id)) orelse return missingIsDeletedLater(&deletes.nodes, p.node_id, lsn);
        defer node.deinit(self.allocator);
        const key_id = ctx.symbol_table.intern(p.key) catch return RecoveryError.IoError;

        var new_value: ?PropertyValue = null;
        if (p.new_value.len > 0) {
            new_value = wal_payload.deserializePropertyValueFromBytes(self.allocator, p.new_value) catch return RecoveryError.InvalidRecord;
        }
        defer if (new_value) |*v| v.deinit(self.allocator);

        const props = try self.withProperty(node.properties, key_id, new_value);
        defer self.allocator.free(props);
        try putNode(ctx, p.node_id, true, node.labels, props);
    }

    /// `properties` with `key_id` set to `value`, or removed when it is null.
    /// The values are borrowed, not copied.
    fn withProperty(self: *Self, properties: []const node_mod.Property, key_id: symbols_mod.SymbolId, value: ?PropertyValue) RecoveryError![]node_mod.Property {
        var out: std.ArrayListUnmanaged(node_mod.Property) = .empty;
        errdefer out.deinit(self.allocator);
        var found = false;
        for (properties) |prop| {
            if (prop.key_id == key_id) {
                found = true;
                if (value) |v| out.append(self.allocator, .{ .key_id = key_id, .value = v }) catch return RecoveryError.OutOfMemory;
            } else {
                out.append(self.allocator, prop) catch return RecoveryError.OutOfMemory;
            }
        }
        if (!found) {
            if (value) |v| out.append(self.allocator, .{ .key_id = key_id, .value = v }) catch return RecoveryError.OutOfMemory;
        }
        return out.toOwnedSlice(self.allocator) catch return RecoveryError.OutOfMemory;
    }

    fn redoLabel(self: *Self, ctx: LogicalRecoveryContext, deletes: *const Deletes, lsn: u64, payload: []const u8, add: bool) RecoveryError!void {
        const p = (if (add) wal_payload.deserializeLabelAdd(payload) else wal_payload.deserializeLabelRemove(payload)) catch return RecoveryError.InvalidRecord;
        var node = (try getNode(ctx, p.node_id)) orelse return missingIsDeletedLater(&deletes.nodes, p.node_id, lsn);
        defer node.deinit(self.allocator);
        const label_id = ctx.symbol_table.intern(p.label) catch return RecoveryError.IoError;

        if (containsId(node.labels, label_id) != add) {
            var labels: std.ArrayListUnmanaged(symbols_mod.SymbolId) = .empty;
            defer labels.deinit(self.allocator);
            for (node.labels) |existing| {
                if (existing != label_id) labels.append(self.allocator, existing) catch return RecoveryError.OutOfMemory;
            }
            if (add) labels.append(self.allocator, label_id) catch return RecoveryError.OutOfMemory;
            try putNode(ctx, p.node_id, true, labels.items, node.properties);
        }
        try labelIndexSet(ctx, label_id, p.node_id, add);
    }

    fn redoEdgeInsert(self: *Self, ctx: LogicalRecoveryContext, payload: []const u8) RecoveryError!void {
        _ = self;
        const p = wal_payload.deserializeEdgeInsert(payload) catch return RecoveryError.InvalidRecord;
        const type_id = ctx.symbol_table.intern(p.edge_type) catch return RecoveryError.IoError;
        var existing = try getEdge(ctx, p.edge_id);
        const exists = existing != null;
        if (existing) |*edge| edge.deinit(ctx.edge_store.allocator);
        try putEdge(ctx, p.edge_id, exists, p.source, p.target, type_id, &.{});
    }

    fn redoEdgeDelete(self: *Self, ctx: LogicalRecoveryContext, payload: []const u8) RecoveryError!void {
        _ = self;
        const p = wal_payload.deserializeEdgeDelete(payload) catch return RecoveryError.InvalidRecord;
        ctx.edge_store.deleteById(p.edge_id) catch |err| switch (err) {
            error.NotFound => {},
            else => return RecoveryError.IoError,
        };
    }

    /// An edge's whole state, logged as an update. Nothing since 0.16.0
    /// writes one; it replaces the edge outright.
    fn redoEdgeSnapshot(self: *Self, ctx: LogicalRecoveryContext, payload: []const u8) RecoveryError!void {
        const p = wal_payload.deserializeEdgeDelete(payload) catch return RecoveryError.InvalidRecord;
        var properties: []wal_payload.Property = &.{};
        if (p.properties.len > 0) {
            properties = wal_payload.deserializeProperties(self.allocator, p.properties) catch return RecoveryError.InvalidRecord;
        }
        defer if (p.properties.len > 0) {
            for (properties) |*prop| prop.value.deinit(self.allocator);
            self.allocator.free(properties);
        };
        var existing = try getEdge(ctx, p.edge_id);
        const exists = existing != null;
        if (existing) |*edge| edge.deinit(self.allocator);
        try putEdge(ctx, p.edge_id, exists, p.source, p.target, p.type_id, @ptrCast(properties));
    }

    fn redoEdgeProperty(self: *Self, ctx: LogicalRecoveryContext, deletes: *const Deletes, lsn: u64, payload: []const u8) RecoveryError!void {
        const p = wal_payload.deserializeEdgePropertyUpdate(payload) catch return RecoveryError.InvalidRecord;
        var edge = (try getEdge(ctx, p.edge_id)) orelse return missingIsDeletedLater(&deletes.edges, p.edge_id, lsn);
        defer edge.deinit(self.allocator);
        const key_id = ctx.symbol_table.intern(p.key) catch return RecoveryError.IoError;

        var new_value: ?PropertyValue = null;
        if (p.new_value.len > 0) {
            new_value = wal_payload.deserializePropertyValueFromBytes(self.allocator, p.new_value) catch return RecoveryError.InvalidRecord;
        }
        defer if (new_value) |*v| v.deinit(self.allocator);

        const props = try self.withProperty(edge.properties, key_id, new_value);
        defer self.allocator.free(props);
        try putEdge(ctx, p.edge_id, true, edge.source, edge.target, edge.edge_type, props);
    }

    fn streamTrees(ctx: LogicalRecoveryContext) RecoveryError!stream_store.TreeSet {
        return ctx.stream_trees orelse RecoveryError.InvalidRecord;
    }

    fn redoStreamAppend(self: *Self, ctx: LogicalRecoveryContext, payload: []const u8) RecoveryError!void {
        const trees = try streamTrees(ctx);
        const append = stream_store.deserializeAppendWal(payload) catch return RecoveryError.InvalidRecord;
        var value = stream_payload.decode(self.allocator, append.payload_bytes) catch return RecoveryError.InvalidRecord;
        defer value.deinit(self.allocator);
        // An event already there under its sequence is this one.
        stream_store.appendWithSequence(trees, self.allocator, append.stream, append.sequence, append.kind, value) catch return RecoveryError.IoError;
    }

    fn redoStreamOffset(self: *Self, ctx: LogicalRecoveryContext, payload: []const u8) RecoveryError!void {
        const trees = try streamTrees(ctx);
        const offset = stream_store.deserializeOffsetWal(payload) catch return RecoveryError.InvalidRecord;
        stream_store.setOffset(trees, self.allocator, offset.stream, offset.consumer, offset.sequence) catch return RecoveryError.IoError;
    }

    fn redoStreamTrim(self: *Self, ctx: LogicalRecoveryContext, payload: []const u8) RecoveryError!void {
        const trees = try streamTrees(ctx);
        const trim_payload = stream_store.deserializeTrimWal(payload) catch return RecoveryError.InvalidRecord;
        stream_store.trim(trees, self.allocator, trim_payload.stream, trim_payload.through_sequence) catch return RecoveryError.IoError;
    }

    fn redoVector(self: *Self, ctx: LogicalRecoveryContext, payload: []const u8) !void {
        const hnsw = ctx.hnsw_index orelse {
            // 0.16.0 logged a removal for every deleted node, whether or not
            // it had a vector, so logs from databases that never stored one
            // contain these. With no vectors in the file there is nothing the
            // record could remove.
            if (!ctx.file_has_vectors and
                payload.len > 0 and
                payload[0] == @intFromEnum(wal_payload.PayloadType.vector_remove))
            {
                return;
            }
            // Silently skipping any other vector record would acknowledge a
            // recovered database with missing committed vectors. Refuse an
            // incompatible open instead.
            return RecoveryError.NeedsVectorIndex;
        };
        const mutation = wal_payload.deserializeVectorMutation(self.allocator, payload) catch |err| switch (err) {
            error.OutOfMemory => return error.OutOfMemory,
            else => return RecoveryError.InvalidRecord,
        };
        defer if (mutation.vector) |vector| self.allocator.free(vector);
        if (mutation.vector) |vector| {
            // Already in the file, from a flush after this record was
            // written: leave the graph as it was saved.
            if (hnsw.holdsVector(mutation.node_id, vector)) return;
            try hnsw.insert(mutation.node_id, vector);
        } else {
            try hnsw.remove(mutation.node_id);
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
    @import("compat").randomBytes(&uuid);

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
    @import("compat").randomBytes(&uuid);

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
    @import("compat").randomBytes(&uuid);

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
    @import("compat").randomBytes(&uuid);

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
    @import("compat").randomBytes(&uuid);

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
    @import("compat").randomBytes(&uuid);

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
    @import("compat").randomBytes(&uuid);

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
    @import("compat").randomBytes(&uuid);

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

test "a long damaged run with valid frames behind it is not a torn tail" {
    const allocator = std.testing.allocator;

    const vfs = @import("vfs.zig");
    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const db_path = "/tmp/long_run_lattice_recovery_midlog_corrupt_test.db";
    const wal_path = "/tmp/long_run_lattice_recovery_midlog_corrupt_test.wal";

    vfs_impl.delete(db_path) catch {};
    vfs_impl.delete(wal_path) catch {};

    var pm = try PageManager.init(allocator, vfs_impl, db_path, .{ .create = true });
    defer {
        pm.deinit();
        vfs_impl.delete(db_path) catch {};
    }

    var uuid: [16]u8 = undefined;
    @import("compat").randomBytes(&uuid);

    var wal = try WalManager.init(allocator, vfs_impl, wal_path, uuid);

    // Fourteen transactions, one frame each.
    var txn_id: u64 = 1;
    while (txn_id <= 14) : (txn_id += 1) {
        _ = try wal.appendRecord(.txn_begin, txn_id, 0, &[_]u8{});
        _ = try wal.appendRecord(.insert, txn_id, 0, "data");
        _ = try wal.appendRecord(.txn_commit, txn_id, 0, &[_]u8{});
        try wal.sync();
    }
    try std.testing.expect(wal.header.frame_count >= 14);

    // Damage frames 1 to 11. Frames 12 and 13 stay valid, further away than
    // a ten-frame look ahead reaches.
    var garbage: [64]u8 = undefined;
    @memset(&garbage, 0xFF);
    var frame: u64 = 1;
    while (frame <= 11) : (frame += 1) {
        const offset = wal_mod.FRAME_SIZE + frame * wal_mod.FRAME_SIZE;
        _ = try wal.file.write(offset + @sizeOf(wal_mod.WalFrameHeader), &garbage);
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
