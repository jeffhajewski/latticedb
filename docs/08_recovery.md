# Crash Recovery

## The Problem

Databases crash. Power fails, operating systems panic, processes get killed. When this happens, the database must be able to restart and return to a **consistent state** without losing committed data.

Consider what might be in-flight when a crash occurs:

```
Scenario: Crash during normal operation

Buffer Pool (RAM):
  Page 5: dirty, modified by committed txn
  Page 8: dirty, modified by uncommitted txn
  Page 12: dirty, partially written by in-progress txn

WAL (Disk):
  [committed txn records][uncommitted txn records][partial frame?]

Database File (Disk):
  [old versions of pages - some stale, some current]
```

After restart, we need to:
1. **Redo committed work** - If a transaction committed (COMMIT record in WAL) but its changes weren't flushed to the database file, redo them
2. **Ignore uncommitted work** - If a transaction never committed, pretend it never happened

## The ARIES Philosophy

Our recovery is inspired by ARIES (Algorithms for Recovery and Isolation Exploiting Semantics), a recovery algorithm developed at IBM. The key insight:

> **Write-Ahead Logging + Redo at Recovery = Durability**

The WAL contains everything we need. After a crash:
1. Read the WAL from the last checkpoint
2. Determine which transactions committed
3. Redo only committed operations

## Two-Phase Recovery

### Phase 1: Analysis

Scan the WAL to understand what happened:

```
WAL Contents:
┌────────────────────────────────────────────────────┐
│ LSN 1: TXN_BEGIN (txn=1)         → txn 1 started  │
│ LSN 2: INSERT (txn=1)            → txn 1 modified │
│ LSN 3: TXN_BEGIN (txn=2)         → txn 2 started  │
│ LSN 4: INSERT (txn=2)            → txn 2 modified │
│ LSN 5: TXN_COMMIT (txn=1)        → txn 1 COMMITTED│
│ LSN 6: UPDATE (txn=2)            → txn 2 modified │
│ ─── CRASH ───                                      │
└────────────────────────────────────────────────────┘

Analysis Result:
  Transaction 1: COMMITTED (has TXN_COMMIT)
  Transaction 2: IN_PROGRESS (no commit/abort)
```

During analysis, we build:
1. **Transaction table**: State of each transaction (committed, aborted, in-progress)
2. **Redo list**: All data operations that might need to be redone

### Phase 2: Redo

Apply committed operations to the database:

```
Redo Process:
┌─────────────────────────────────────────────────────┐
│ For each operation in redo_list:                    │
│   1. Check if transaction committed                 │
│      - If not: skip (discard uncommitted changes)   │
│   2. Apply the operation to database file           │
│   3. Continue to next operation                     │
└─────────────────────────────────────────────────────┘

Result:
  LSN 2 (INSERT, txn=1): REDO (txn 1 committed)
  LSN 4 (INSERT, txn=2): SKIP (txn 2 didn't commit)
  LSN 6 (UPDATE, txn=2): SKIP (txn 2 didn't commit)
```

## Why No Undo?

Traditional ARIES has three phases: Analysis, Redo, **Undo**. We skip Undo because of how we structure our system:

**Our approach**: Don't apply changes to data pages until commit
- During a transaction, changes are in the buffer pool (RAM)
- On commit, dirty pages are flushed
- On crash, uncommitted changes in RAM are simply lost

**Traditional approach**: Apply changes immediately, undo on abort
- Changes written to data pages as transaction runs
- If abort/crash, must read WAL backwards and undo each change
- More complex, but allows larger transactions (not limited by RAM)

For an embedded database like Lattice, the simpler "don't undo" approach works well.

## The Checkpoint Starting Point

Recovery doesn't scan the entire WAL - only from the last checkpoint:

```
WAL Timeline:
─────────────────────────────────────────────────────────────────►

[old records] │ CHECKPOINT │ [records since checkpoint]
              ▲            ▲
              │            │
    checkpoint_lsn         │
                           │
              Recovery starts here
              (everything before is safely on disk)
```

The `checkpoint_lsn` in the WAL header marks where recovery begins. The Checkpointer sets this after successfully flushing all dirty pages.

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
    .insert, .update, .delete => {
        // Data modification - save for redo phase
    },
    .page_write => {
        // Physical page write - save for redo phase
    },
    .checkpoint_begin, .checkpoint_end => {
        // Informational - no action needed
    },
    .savepoint, .savepoint_rollback => {
        // Track but no special handling
    },
    .clr => {
        // Compensation Log Record - for undo operations
    },
}
```

## Physical vs Logical Redo

Our recovery supports two types of operations:

### Physical: page_write
Contains the complete page image:
```
Payload: [page_id: 4 bytes][page_data: 4096 bytes]
```
Redo: Write entire page directly to database file

### Logical: insert, update, delete
Contains the operation parameters:
```
Payload: [key][value][metadata]
```
Redo: Re-execute the operation against the B+Tree

For simplicity, our implementation currently relies on `page_write` for physical durability, with logical operations tracked for statistics.

## Corruption Detection

When recovery encounters a checksum mismatch, it must determine whether this is:

1. **Tail corruption** - A torn write at the end of the WAL (safe to proceed)
2. **Mid-log corruption** - Real corruption with valid data after it (unsafe)

### Scan-Ahead Detection

On checksum mismatch, we scan ahead to check for valid frames:

```
Scenario 1: Tail Corruption (Torn Write)
─────────────────────────────────────────
Frame 0: checksum OK ✓
Frame 1: checksum OK ✓
Frame 2: checksum MISMATCH ✗
Frame 3: [nothing valid]
Frame 4: [nothing valid]

→ No valid frames after corruption
→ This is a torn write at end of WAL
→ Safe to proceed with frames 0-1


Scenario 2: Mid-Log Corruption (Real Corruption)
────────────────────────────────────────────────
Frame 0: checksum OK ✓
Frame 1: checksum MISMATCH ✗  ← corruption here
Frame 2: checksum OK ✓        ← but valid data after!
Frame 3: checksum OK ✓

→ Valid frames exist after corruption
→ This is real data corruption
→ FAIL with MidLogCorruption error
```

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
    // Check up to 10 frames ahead
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

```
┌─────────────────────────────────────────────────────────────────┐
│                     Database Startup                             │
└───────────────────────────────┬─────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                      Open WAL File                               │
│                 Read checkpoint_lsn from header                  │
└───────────────────────────────┬─────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                     ANALYSIS PHASE                               │
│                                                                  │
│  for each record from checkpoint_lsn to end:                    │
│    - Track transaction states                                    │
│    - Build redo list for data operations                        │
│    - Stop at corruption/end of valid log                        │
└───────────────────────────────┬─────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                       REDO PHASE                                 │
│                                                                  │
│  for each operation in redo_list:                               │
│    if transaction.state == committed:                           │
│      apply operation to database                                │
│    else:                                                        │
│      discard (transaction didn't commit)                        │
└───────────────────────────────┬─────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Sync Database File                            │
│                 Return recovery statistics                       │
└───────────────────────────────┬─────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                   Database Ready for Use                         │
└─────────────────────────────────────────────────────────────────┘
```

## API

```zig
// Simple recovery on startup
const stats = try recoverDatabase(allocator, &wal, &pm);
std.debug.print("Recovered {} transactions, redid {} operations in {}ms\n", .{
    stats.transactions_committed,
    stats.redo_operations,
    stats.duration_ns / 1_000_000,
});

// Or use RecoveryManager directly for more control
var rm = RecoveryManager.init(allocator);
const stats = try rm.recover(&wal, &pm);
```

## Integration with Startup

Typical database startup sequence:

```
1. Open database file (PageManager)
2. Open WAL file (WalManager)
3. Check if recovery needed (WAL has records past checkpoint?)
4. Run recovery
5. Checkpoint to clean slate
6. Ready for operations
```

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
| No Undo | Uncommitted changes not written to pages |

Recovery transforms a potentially inconsistent crash state into a consistent database by leveraging the WAL as the authoritative record of committed transactions.
