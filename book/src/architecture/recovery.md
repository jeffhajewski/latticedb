# Crash Recovery

## The Problem

Databases crash. Power fails, operating systems panic, processes get killed. When this happens, the database must be able to restart and return to a **consistent state** without losing committed data.

Consider what might be in-flight when a crash occurs:

```
Scenario: Crash during normal operation

Buffer Pool (RAM):
  Page 5: dirty, modified by a committed txn
  Page 12: dirty, modified by a txn whose commit record was not yet on disk

Transaction overlay (RAM):
  the open txn's staged changes - never logged, never applied

WAL (Disk):
  [committed txn records][records without a commit record][partial frame?]

Database File (Disk):
  [old versions of pages - some stale, some current]
```

After restart, we need to:
1. **Redo committed work** - If a transaction committed (COMMIT record in WAL) but its changes weren't flushed to the database file, redo them
2. **Ignore uncommitted work** - If a transaction never committed, pretend it never happened

## The ARIES Philosophy

Our recovery is inspired by ARIES (Algorithms for Recovery and Isolation Exploiting Semantics), a recovery algorithm developed at IBM, but it is much smaller: redo only, and logical rather than physical. The key insight:

> **Write-Ahead Logging + Redo at Recovery = Durability**

The WAL contains everything we need. After a crash:
1. Read the WAL from the last checkpoint
2. Determine which transactions committed
3. Redo only committed operations, by re-executing them against the stores

## Two-Phase Recovery

### Phase 1: Analysis

Scan the WAL to understand what happened:

| LSN | Record | Transaction | Meaning |
|----:|--------|------------:|---------|
| 1 | `TXN_BEGIN` | 1 | Transaction 1 started |
| 2 | `INSERT` | 1 | Transaction 1 modified data |
| 3 | `TXN_BEGIN` | 2 | Transaction 2 started |
| 4 | `INSERT` | 2 | Transaction 2 modified data |
| 5 | `TXN_COMMIT` | 1 | Transaction 1 committed |
| 6 | `UPDATE` | 2 | Transaction 2 modified data |

The process crashes after LSN 6, giving:

| Transaction | State |
|------------:|-------|
| 1 | `COMMITTED` — has `TXN_COMMIT` |
| 2 | `IN_PROGRESS` — no commit or abort |

During analysis, we build:
1. **Transaction table**: State of each transaction (committed, aborted, in-progress)
2. **Redo list**: All data operations that might need to be redone

### Phase 2: Redo

Apply committed operations to the database:

For each operation in the redo list:

1. Check whether its transaction committed. If not, skip it — the change is discarded.
2. Re-execute the operation against the stores (see [Logical Redo](#logical-redo)).
3. Continue to the next operation.

Against the log above:

| LSN | Operation | Transaction | Outcome |
|----:|-----------|------------:|---------|
| 2 | `INSERT` | 1 | Redo — transaction 1 committed |
| 4 | `INSERT` | 2 | Skip — transaction 2 never committed |
| 6 | `UPDATE` | 2 | Skip — transaction 2 never committed |

## Why No Undo?

Traditional ARIES has three phases: Analysis, Redo, **Undo**. We skip Undo because of how we structure our system:

**Our approach**: Keep a transaction's changes out of the stores until commit
- During a transaction, changes live in its in-memory overlay, not in pages
- On commit, the overlay is logged, applied to the stores (dirtying pages in the
  buffer pool), and then the commit record is fsynced; the dirty pages are written
  back later, by eviction or a checkpoint
- On crash, an open transaction's overlay is simply lost, and nothing of it was logged

There is one gap in that picture. The overlay is applied just before the commit
record's fsync, so a page the buffer pool evicts in that window can reach the
database file even though the commit record never does. Recovery has no undo pass
to take it back. See [Write-Ahead Log](./wal.md#how-latticedb-applies-it).

**Traditional approach**: Apply changes immediately, undo on abort
- Changes written to data pages as transaction runs
- If abort/crash, must read WAL backwards and undo each change
- More complex, but allows larger transactions (not limited by RAM)

For an embedded database like Lattice, the simpler "don't undo" approach works well.

## The Checkpoint Starting Point

Recovery doesn't scan the entire WAL - only from the last checkpoint:

<img class="diagram" src="../assets/diagrams/recovery-checkpoint-timeline.svg"
     alt="The WAL timeline: old records already flushed to disk, then the checkpoint marking checkpoint_lsn, then the records written since, which are the only ones recovery replays">

The `checkpoint_lsn` in the WAL header marks where recovery begins. The Checkpointer
sets it after flushing dirty pages and fsyncing the database file (a passive
checkpoint sets it even if it skipped some pages; see
[Checkpointing](./checkpointing.md#passive-mode)). A truncate checkpoint resets the
log, and with it `checkpoint_lsn`, to empty. Recovery redoes only transactions whose
`TXN_BEGIN` it reads from that point on.

## Transaction States

During recovery, each transaction can be in one of three states:

| State | Meaning | WAL Record | Action |
|-------|---------|------------|--------|
| `committed` | Completed successfully | TXN_COMMIT present | Redo operations |
| `aborted` | Explicitly rolled back | TXN_ABORT present | Ignore operations |
| `in_progress` | Crash during execution | No commit/abort | Ignore operations |

The distinction between `aborted` and `in_progress` is informational - both are handled the same way (ignore their changes).

## Record Type Handling

Different WAL records are handled differently:

```zig
switch (record.record_type) {
    .txn_begin => {
        // Track new transaction as in_progress
    },
    .txn_commit => {
        // Mark transaction as committed
    },
    .txn_abort => {
        // Mark transaction as aborted
    },
    .insert, .update, .delete, .page_write => {
        // Data modification - save for redo phase
    },
    .checkpoint_begin, .checkpoint_end => {
        // Informational - no action needed
    },
    .savepoint, .savepoint_rollback => {
        // Track the LSN only; a savepoint rollback is not applied
    },
    .clr => {
        // Compensation Log Record - nothing to do (and never written)
    },
}
```

`large_fragment` records never reach this switch: the WAL iterator reassembles a
fragmented record into its logical record before handing it to recovery.

## Logical Redo

Recovery is logical: it re-executes committed operations through the same stores
the database uses, rather than writing page images.

An `insert`, `update` or `delete` record carries a logical payload whose first byte
names the operation (`wal_payload.PayloadType`):

| Payload | Redo |
|---------|------|
| `node_insert`, `node_delete`, `node_update` | Node store and label index |
| `edge_insert`, `edge_delete`, `edge_property_update` | Edge store |
| `label_add`, `label_remove` | Node store and label index |
| `vector_set`, `vector_remove` | Vector index |
| `stream_append`, `stream_offset_set`, `stream_trim` | Stream trees |

The database opens recovery with a `LogicalRecoveryContext` that points at the node
store, edge store, symbol table, label index, stream trees and, when vectors are
enabled, the vector index. Labels, edge types and property keys are mostly logged
by name and re-interned through the symbol table. A graph or stream redo step that
fails is skipped rather than failing recovery; vector records are stricter, as
[Write-Ahead Log](./wal.md#write-flow) describes.

Recovery redoes *every* committed record from `checkpoint_lsn` on, whether or not
its effect already reached the database file. Missing nodes and edges are
re-created with their original ids, and properties and labels are set to their
logged values.

The `page_write` record type (a full page image) is still handled by the redo pass,
but nothing writes it.

## Corruption Detection

When recovery encounters a checksum mismatch, it must determine whether this is:

1. **Tail corruption** - A torn write at the end of the WAL (safe to proceed)
2. **Mid-log corruption** - Real corruption with valid data after it (unsafe)

### Scan-Ahead Detection

On checksum mismatch, we scan ahead to check for valid frames:

**Scenario 1 — tail corruption (a torn write)**

| Frame | Checksum |
|------:|----------|
| 0 | OK |
| 1 | OK |
| 2 | Mismatch |
| 3 | Nothing valid |
| 4 | Nothing valid |

No valid frames follow the corruption, so this is a torn write at the end of the WAL.
Recovery proceeds with frames 0 and 1.

**Scenario 2 — mid-log corruption (real corruption)**

| Frame | Checksum |
|------:|----------|
| 0 | OK |
| 1 | Mismatch |
| 2 | OK |
| 3 | OK |

Valid frames exist *after* the corrupt one, so this cannot be a torn tail. Recovery
fails with `MidLogCorruption` rather than silently discarding committed data.

### Why This Matters

Mid-log corruption is dangerous because the corrupted frame might contain:
- A `TXN_COMMIT` record we can't see
- Data modifications needed for consistency

If we skip the corrupted frame and continue, we might:
- Treat a committed transaction as uncommitted (data loss)
- Apply partial transaction state (inconsistency)

By failing on mid-log corruption, we force manual intervention (restore from backup) rather than silently losing data.

### Implementation

```zig
fn hasValidFramesAfter(wal: *WalManager, corrupted_frame: u64) bool {
    // Check the next nine frames (or up to the end of the log)
    for (corrupted_frame + 1 .. min(corrupted_frame + 10, frame_count)) |frame_num| {
        if (frameHasValidChecksum(frame_num)) {
            return true;  // Mid-log corruption!
        }
    }
    return false;  // Tail corruption, safe to stop
}
```

## Statistics

Recovery returns detailed statistics:

```zig
RecoveryStats{
    start_lsn: 1000,            // Where we started (checkpoint_lsn)
    end_lsn: 1523,              // Last valid record
    records_scanned: 523,        // Total records processed
    transactions_found: 15,      // Distinct transactions
    transactions_committed: 12,  // Successfully committed
    transactions_aborted: 2,     // Explicitly aborted
    transactions_rolled_back: 1, // In-progress at crash
    redo_operations: 156,        // Operations redone
    duration_ns: 45_000_000,    // 45ms
    stopped_at_corruption: true, // Hit tail corruption
    corrupted_frame: 42,         // Frame number with bad checksum
}
```

These are useful for:
- Monitoring recovery time
- Debugging transaction issues
- Capacity planning
- Detecting disk health issues (frequent tail corruption may indicate hardware problems)

## Recovery Flow Diagram

<img class="diagram" src="../assets/diagrams/recovery-flow.svg"
     alt="Recovery flow on startup. A read-only open, or one with the WAL disabled, skips recovery entirely. Otherwise: open the WAL file, check its UUID and read checkpoint_lsn; once the stores and the vector index are ready, run the analysis phase to track transaction states and build the redo list, then the redo phase re-executing committed operations through the stores and discarding the rest. If anything was redone, rebuild the property and full-text indexes and sync: persist the vector index, save the tree roots, flush the buffer pool and fsync the database file. Then the transaction manager starts and the database is ready for use. The skipped path has no transaction manager">

## API

This is what `Database.open` does (simplified):

```zig
var rm = RecoveryManager.initWithLogicalContext(allocator, .{
    .node_store = &self.node_store,
    .edge_store = &self.edge_store,
    .symbol_table = &self.symbol_table,
    .label_index = &self.label_index,
    .stream_trees = self.streamTrees(),
    .hnsw_index = if (self.hnsw_index) |*hnsw| hnsw else null,
});
const stats = try rm.recover(wal, &self.page_manager);
std.debug.print("Recovered {} transactions, redid {} operations in {}ms\n", .{
    stats.transactions_committed,
    stats.redo_operations,
    stats.duration_ns / 1_000_000,
});
```

`recoverDatabase(allocator, &wal, &pm)` and `RecoveryManager.init` also exist, but
they have no logical context: they scan the log and count what they would redo
without changing anything.

## Integration with Startup

`Database.open`, in order:

```
1. Open database file (PageManager) and the buffer pool
2. Open WAL file (WalManager) - only for a read-write open with the WAL enabled
3. Load or create the B+Trees, symbol table, graph stores, FTS, vector index
4. Run recovery - whenever the WAL is open; there is no "is recovery needed" check
5. If anything was redone: rebuild property and FTS indexes, sync, fsync the file
6. Start the transaction manager
7. Ready for operations
```

Recovery does not checkpoint afterwards or truncate the log. The redone state is
durable in the database file when open returns, but the records stay in the log
until the next checkpoint, so a later open replays them again.

A **read-only** open, or a database opened with `enable_wal = false`, never opens the
log and never runs recovery. A read-only open after a crash therefore sees only what
had reached the database file; open read-write once to replay the log.

## Summary

| Concept | Purpose |
|---------|---------|
| Analysis phase | Determine transaction outcomes |
| Redo phase | Apply committed operations |
| checkpoint_lsn | Recovery starting point |
| Transaction states | committed, aborted, in_progress |
| Checksum verification | Detect corruption/partial writes |
| Tail corruption | Torn write at end, safe to tolerate |
| Mid-log corruption | Real corruption, fail to prevent data loss |
| Scan-ahead detection | Distinguish tail from mid-log corruption |
| No Undo | Uncommitted changes stay in the overlay, never logged |
| Logical redo | Committed records re-executed through the stores |

Recovery transforms a potentially inconsistent crash state into a consistent database by leveraging the WAL as the authoritative record of committed transactions.
