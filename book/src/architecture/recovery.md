# Crash Recovery

## The Problem

Databases crash. Power fails, operating systems panic, processes get killed. When this happens, the database must be able to restart and return to a **consistent state** without losing committed data.

Consider what is where when a crash occurs:

```
Scenario: Crash during normal operation

Buffer Pool (RAM):              gone
  dirty pages, all from committed transactions
  the vector index's in-memory node map

Transaction overlays (RAM):     gone
  open transactions' staged changes - never logged, never applied

Database File (Disk):
  a whole image of the database as of its last flush

Journal (Disk):
  empty, or the flush that was being written

WAL (Disk):
  [committed txn records][records without a commit record][partial frame?]
```

Because no page ever reaches the file on its own (see
[Checkpointing](./checkpointing.md#why-pages-cannot-go-one-at-a-time)), the file is
never a mixture of two images, except while a flush is writing it, and then the
journal holds the newer one.

After restart, we need to:
1. **Finish an interrupted flush** - so the file is one whole image
2. **Redo committed work** - If a transaction committed (COMMIT record in WAL) but its changes weren't flushed to the database file, redo them
3. **Ignore uncommitted work** - If a transaction never committed, pretend it never happened

## The ARIES Philosophy

Our recovery is inspired by ARIES (Algorithms for Recovery and Isolation Exploiting Semantics), a recovery algorithm developed at IBM, but it is much smaller: redo only, and logical rather than physical. The key insight:

> **Write-Ahead Logging + Redo at Recovery = Durability**

The WAL contains everything we need. After a crash:
1. Read the WAL from the last checkpoint
2. Determine which transactions committed
3. Redo only committed operations, by re-executing them against the stores

## The Order of Work at Open

Recovery is not one function but a sequence spread through `Database.open`:

<img class="diagram" src="../assets/diagrams/recovery-open.svg"
     alt="What Database.open does about a crash, in order. 1. Finish an interrupted flush: a complete journal is written in place, a read-only open refuses instead, and an incomplete journal is discarded. Then, only for a read-write open with the WAL enabled: 2. Open the log, check the database id, cut off a torn tail and rebuild a torn header. 3. Load the trees, stores, FTS and vector index. 4. Analysis: which transactions committed; damage in the middle of the log refuses the open. 5. Redo committed records in log order, flushing early if the pool fills. 6. If anything was redone, rebuild the property and FTS indexes and flush. 7. Number new transactions above the log, then checkpoint: flush and empty the log. Ready. A read-only open, or one without the WAL, uses the file only, with no log and no recovery">

1. The page manager finishes any interrupted flush from the journal, before it reads
   the file's header.
2. The log is opened: its database id is checked, a torn tail is cut off and a torn
   header rebuilt.
3. The trees, graph stores, full-text index and vector index are loaded from the
   file. The vector index has to exist before replay, because replay needs
   somewhere to put vectors.
4. Analysis and redo run (below).
5. If anything was redone, the property and full-text indexes are rebuilt from the
   trees, since replay does not maintain them, and the result is flushed.
6. The transaction manager starts, numbering transactions above the largest id the
   log contained.
7. Unless `auto_checkpoint` is null, a truncate checkpoint empties the log, so a
   second crash does not replay it all again and a torn frame does not sit in front
   of new ones.

A **read-only** open, or a database opened with `enable_wal = false`, never opens the
log and never runs recovery. A read-only open after a crash therefore sees only what
had reached the database file; open read-write once to replay the log.

## Finishing an Interrupted Flush

If a crash interrupted a flush, the journal (`<database>-journal`) holds it. A
journal is applied only if its header and table verify, it names this database, and
every page image in it matches its checksum. Then all its pages are written in place
and the file is synced. A journal that fails any check is the trace of a crash while
it was being written, before the database file was touched, and is discarded:
applying the images that verify and skipping the rest would recreate the torn image
the journal exists to prevent.

A read-only open cannot write the file, so finding a complete journal refuses the
open instead. Opening the database once read-write finishes the flush.

After this step the file is a whole image, from before the flush or after it.

## Opening the Log

A log whose header names a different database is refused. A header that fails its
checksum while its magic, version, frame size and database id are intact was torn in
the middle of a rewrite; it is rebuilt, with its frame count taken from the file's
length and `checkpoint_lsn` reset to zero, so the whole log is replayed.

Then the end of the log is found. The header's frame count says how many frames were
written, not how many survived. Frames are verified in order; at the first one that
fails, the rest of the file is checked. If no later frame is valid, the bad frame is
a torn write at the tail: the log is cut back to the frames that verify and the
header rewritten. If a later frame is valid, the log is left exactly as found, for
recovery to report.

## Two-Phase Recovery

### Phase 1: Analysis

Scan the WAL from `checkpoint_lsn` to understand what happened. Because a
transaction writes all its records at commit and there is only one writer, a
transaction's records are contiguous; a crash can cut the last one short.

| LSN | Record | Transaction | Meaning |
|----:|--------|------------:|---------|
| 1 | `TXN_BEGIN` | 1 | Transaction 1's records begin |
| 2 | `INSERT` | 1 | Transaction 1 modified data |
| 3 | `UPDATE` | 1 | Transaction 1 modified data |
| 4 | `TXN_COMMIT` | 1 | Transaction 1 committed |
| 5 | `TXN_BEGIN` | 2 | Transaction 2's records begin |
| 6 | `DELETE` | 2 | Transaction 2 modified data |

The process dies while transaction 2 is logging, giving:

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
| 3 | `UPDATE` | 1 | Redo — transaction 1 committed |
| 6 | `DELETE` | 2 | Skip — transaction 2 never committed |

Between records, recovery calls the same early flush that commits use, so a long
replay does not have to fit in the buffer pool: when half the pool is dirty, the log
is synced and the pages are flushed as one image. The log is not emptied until the
end, so a crash in the middle of recovery leaves a whole image and the whole log,
and the next attempt replays all of it again.

## Why No Undo?

Traditional ARIES has three phases: Analysis, Redo, **Undo**. We skip Undo because of how we structure our system:

**Our approach**: Keep a transaction's changes out of the stores until commit
- During a transaction, changes live in its in-memory overlay, not in pages
- On commit, the overlay is logged, the commit record is synced, and only then is
  the overlay applied to the stores (dirtying pages in the buffer pool); the dirty
  pages reach the file later, in a whole flush
- On crash, an open transaction's overlay is simply lost, and nothing of it was
  logged as committed or applied

So no flush, early or otherwise, can write an uncommitted change into the file, and
after a crash there is nothing of one to reverse. The one trace an uncommitted
transaction can leave is a symbol (a label, edge type or property name) it interned,
which nothing refers to.

**Traditional approach**: Apply changes immediately, undo on abort
- Changes written to data pages as transaction runs
- If abort/crash, must read WAL backwards and undo each change
- More complex, but allows larger transactions (not limited by RAM)

For an embedded database like Lattice, the simpler "don't undo" approach works well.
The price is that a transaction's overlay must fit in memory.

## The Checkpoint Starting Point

Recovery doesn't scan the entire WAL - only from the last checkpoint:

<img class="diagram" src="../assets/diagrams/recovery-checkpoint-timeline.svg"
     alt="The WAL timeline: old records already flushed to disk, then the checkpoint marking checkpoint_lsn, then the records written since, which are the only ones recovery replays">

The `checkpoint_lsn` in the WAL header marks where recovery begins. The Checkpointer
sets it after the flush has written the pages and synced the database file. A
passive checkpoint that finds a dirty page pinned writes nothing and leaves it where
it was (see [Checkpointing](./checkpointing.md#passive-mode)). A truncate checkpoint
resets the log, and with it `checkpoint_lsn`, to empty.

A transaction exists for analysis only from its `TXN_BEGIN`: a commit record whose
begin record lies before the point recovery starts reading belongs to no
transaction, and its records are not redone. That is why a checkpoint refuses to run
while an open transaction has records in the log
([Checkpointing](./checkpointing.md#refusals)).

## Transaction States

During recovery, each transaction can be in one of three states:

| State | Meaning | WAL Record | Action |
|-------|---------|------------|--------|
| `committed` | Completed successfully | TXN_COMMIT present | Redo operations |
| `aborted` | A commit failed after logging some records, and was rolled back | TXN_ABORT present | Ignore operations |
| `in_progress` | Crash while the transaction was logging | No commit/abort | Ignore operations |

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
by name and re-interned through the symbol table, so symbol ids need not match the
session that wrote the log.

### Replaying twice is safe

Recovery redoes *every* committed record from `checkpoint_lsn` on, whether or not
its effect already reached the database file: every early flush since the last
checkpoint wrote changes that are still in the log, and a flush in the middle of a
commit can have written part of a transaction. Recovery does not know which records
the image reflects, and does not try to.

That works because the records state outcomes rather than changes: "node 42 exists
with label Person", "property `age` of node 42 is 31", "edge 9 does not exist".
Replaying one onto a state that already has it changes nothing. Replaying an older
record onto a newer state can change something, but the later record is in the log
after it and is replayed after it, and the image never runs ahead of the log, since
the log is synced before every flush.

The vector index needed an exception. Inserting a vector the index already holds is
a replacement, which unlinks the node from its neighbours and links it again, and
doing that for every vector in the log left a graph that searches could no longer
cross. Replay now skips a vector write when the index already holds exactly that
vector for the node.

### Replay errors

A graph or stream redo step that fails is skipped rather than failing recovery;
some failures are expected (a node insert whose node is already in the file fails,
and that failure is the replay being a no-op). Vector records are stricter: a vector
record that cannot be applied, or any vector record when the database was opened
without vectors enabled, fails the open, since skipping it would leave committed
vectors missing. The one exception is a vector removal in a file that has never held
vectors, which 0.16.0 logged for every deleted node and which has nothing to remove.

The `page_write` record type (a full page image) is still handled by the redo pass,
but nothing writes it.

## Corruption Detection

When recovery encounters a checksum mismatch, it must determine whether this is:

1. **Tail corruption** - A torn write at the end of the WAL (safe to proceed)
2. **Mid-log corruption** - Real corruption with valid data after it (unsafe)

Opening the log has normally cut a torn tail off already (see
[Opening the Log](#opening-the-log)); recovery checks again as it reads.

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
fails with `MidLogCorruption`, and the open fails with `InvalidDatabase`, rather than
silently discarding committed data.

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

Opening the log checks every later frame; this check during recovery looks only
nine frames ahead. See [Known Limits](#known-limits).

## When an Open Is Refused

Opening refuses, rather than guess, when:

- the log belongs to a different database, or its header is damaged beyond the
  torn-header case above;
- the log is damaged in the middle;
- a read-only open finds a complete journal it may not apply;
- the log holds a vector record and the database was opened without the vector
  index (apart from the removal case above);
- on a read-write open, the node tree cannot be scanned to find the next node id:
  starting the ids over at 1 would overwrite existing nodes.

## What Survives What

The guarantee depends on what failed, the sync mode and, on macOS, whether
`full_fsync` is on (see [Durability and the Log](../configuration/durability.md)).

| Configuration | Process killed | Power lost or OS crash |
|---------------|----------------|------------------------|
| `sync_mode = .full` (default) on Linux, or on macOS with `full_fsync` | Every acknowledged commit survives | Every acknowledged commit survives, if the device honours its flush |
| `.full` on macOS without `full_fsync` | Every acknowledged commit survives | Not guaranteed: `fsync` does not flush the drive's cache, so neither the commit nor the ordering the journal relies on is assured |
| `sync_mode = .normal` | Every acknowledged commit survives | Commits since the last sync or checkpoint may be lost, from the end only |

"Acknowledged" means the commit call returned. A commit that was running when the
process died is either wholly present or wholly absent, according to whether its
commit record reached the disk.

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
    max_txn_id: 27,              // Largest transaction id in the log
}
```

`Database.open` uses `redo_operations` (to decide whether to rebuild indexes) and
`max_txn_id` (to number the new session's transactions) and discards the rest;
nothing reports them to the caller. They are available when you drive
`RecoveryManager` yourself.

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
    // Whether the file has ever held vectors (for 0.16.0's stray removals)
    .file_has_vectors = has_vector_data,
    // Called between records: the early flush
    .between_operations = .{ .context = self, .call = relieveBufferPressureHook },
});
const stats = try rm.recover(wal, &self.page_manager);
```

`recoverDatabase(allocator, &wal, &pm)` and `RecoveryManager.init` also exist, but
they have no logical context: they scan the log and count what they would redo
without changing anything.

## Known Limits

- **The look-ahead during recovery is short.** If the nine frames after a damaged
  one are all damaged too and valid frames follow them, the damage is treated as a
  torn tail: the commits after it are not replayed, and the checkpoint at the end
  of the open then empties the log.
- **Unsynced frames can reach the disk out of order.** A power cut can leave a later
  unsynced frame whole and an earlier one torn. The earlier frame then looks like
  mid-log damage and the open is refused, where dropping the unsynced frames would
  have been right. In the default sync mode this can only affect a commit that was
  still being written; in `.normal`, any frames since the last sync.
- **Replay errors are swallowed** for graph and stream records, as above, so a
  failure that is not the expected "already there" can leave a committed change out
  without an error.
- **The tests simulate kills, not power loss.** The crash tests copy the database
  and log while the database is open, as a kill would leave them, or cut the
  database file back to its header so everything has to come from the log; they
  cover torn and missing frames, a flush interrupted part way, and recovery after
  automatic checkpoints and early flushes. Nothing injects faults at the storage
  layer or reorders writes, so the power-loss column above rests on reasoning rather
  than a test.

## Summary

| Concept | Purpose |
|---------|---------|
| Journal | Finish or discard an interrupted flush, so the file is one whole image |
| Torn-tail trim | Cut a half-written last frame off the log at open |
| Analysis phase | Determine transaction outcomes |
| Redo phase | Apply committed operations |
| checkpoint_lsn | Recovery starting point |
| Transaction states | committed, aborted, in_progress |
| Checksum verification | Detect corruption/partial writes |
| Tail corruption | Torn write at end, safe to tolerate |
| Mid-log corruption | Real corruption, fail to prevent data loss |
| Scan-ahead detection | Distinguish tail from mid-log corruption |
| No Undo | Uncommitted changes stay in the overlay, never applied |
| Logical redo | Committed records re-executed through the stores; safe to repeat |
| Checkpoint at open | Empty the log once recovery is done |

Recovery transforms a potentially inconsistent crash state into a consistent database by leveraging the WAL as the authoritative record of committed transactions.
