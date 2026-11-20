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
     alt="What Database.open does about a crash, in order. A read-only open whose log and journal are empty uses the file only. A read-only open with something in either opens on an in-memory overlay, where the files are read and never written, and then goes through the same steps as a read-write open. 1. Finish an interrupted flush: a complete journal is written in place and an incomplete one discarded. Then, with the WAL enabled: 2. Open the log, check the database id, cut off an unsynced tail and rebuild a torn header. 3. Load the trees, stores, FTS and vector index. 4. Analysis: which transactions committed. 5. Redo committed records in log order, each as an assignment, flushing early if the pool fills. Damage in synced frames, or a record that cannot be replayed, refuses the open. 6. If anything was redone, rebuild the property and FTS indexes and flush. 7. Number new transactions above the log, then checkpoint: flush and empty the log. Ready. Without the WAL the open uses the file only">

1. The page manager finishes any interrupted flush from the journal, before it reads
   the file's header.
2. The log is opened: its database id is checked, an unsynced tail is cut off and a
   torn header rebuilt.
3. The trees, graph stores, full-text index and vector index are loaded from the
   file. The vector index has to exist before replay, because replay needs
   somewhere to put vectors; the file records the index's configuration, so it is
   loaded whatever the open's options say.
4. Analysis and redo run (below).
5. If anything was redone, the property and full-text indexes are rebuilt from the
   trees, since replay does not maintain them, and the result is flushed.
6. The transaction manager starts, numbering transactions above the largest id the
   log contained.
7. Unless `auto_checkpoint` is null, a truncate checkpoint empties the log, so a
   second crash does not replay it all again and a torn frame does not sit in front
   of new ones. A log that ends at its own last checkpoint is left as it is, since
   the file already holds everything in it.

A database opened with `enable_wal = false` never opens the log and never replays
it.

A **read-only** open of a database whose log holds frames, or whose journal holds a
flush, runs exactly these steps on an overlay filesystem (`OverlayVfs`): the files
are opened for reading only, and every write recovery makes, the journal applied,
the replay and the checkpoint that folds it in, goes to memory in 4 KB blocks over
the files. The handle is then read-only as usual. Nothing reaches the disk, and the
next read-write open recovers the files itself. The overlay may hold at most 64 MB
plus four times the size of the log and journal; a recovery that needs more fails
the open with `BufferPoolFull`, and a read-write open recovers such a database. A
read-only open with an empty log and no journal reads the file directly, as before.

The reader's shared lock keeps a writer out while it reads. Opened without locking
beside a live writer, a read-only handle recovers the log as far as it had been
written at that moment.

## Finishing an Interrupted Flush

If a crash interrupted a flush, the journal (`<database>-journal`) holds it. A
journal is applied only if its header and table verify, it names this database, and
every page image in it matches its checksum. Then all its pages are written in place
and the file is synced. A journal that fails any check is the trace of a crash while
it was being written, before the database file was touched, and is discarded:
applying the images that verify and skipping the rest would recreate the torn image
the journal exists to prevent.

A read-only open applies the journal to its in-memory overlay, so it sees the
finished flush without writing the file; the next read-write open applies it to the
file.

After this step the file is a whole image, from before the flush or after it.

## Opening the Log

A log whose header names a different database is refused. A header that fails its
checksum while its magic, version, frame size and database id are intact was torn in
the middle of a rewrite; it is rebuilt, with its frame count taken from the file's
length and `checkpoint_lsn` reset to zero, so the whole log is replayed.

Then the end of the log is found. The header's frame count says how many frames were
written, not how many survived. Every frame is checked, and each frame that verifies
says how many frames had been synced when it was written (`synced_frames`, see
[Write-Ahead Log](./wal.md#the-checksum)). The largest of those claims divides the
log. A bad frame at or above it belongs to the unsynced tail, which a power cut can
leave in any state: the log is cut back to the frame before it and the header
rewritten. A bad frame below it was synced, and its damage is real: the log is left
exactly as found, for recovery to refuse. A log written entirely by 0.16.0 carries
no claims, and a bad frame in it is damage if any later frame verifies.

A non-empty log is synced before anything new is written after it, since a killed
process's frames may still be only in the operating system's cache, and bytes past
the frames the header counts are removed.

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
| `changefeed_events` | Changefeed: a run of compact events, with the names of the symbols they use; the names are interned again and each event stored with the recovered ids. Events below the changefeed's floor, dropped by retention after they were logged, stay dropped, and the file's minimum reader version is raised again |

The database opens recovery with a `LogicalRecoveryContext` that points at the node
store, edge store, symbol table, label index, stream trees, the page manager and,
when vectors are enabled, the vector index. Labels, edge types and property keys are mostly logged
by name and re-interned through the symbol table, so symbol ids need not match the
session that wrote the log.

### Replaying twice is safe

Recovery redoes *every* committed record from `checkpoint_lsn` on, whether or not
its effect already reached the database file: every early flush since the last
checkpoint wrote changes that are still in the log, and a flush in the middle of a
commit can have written part of a transaction. Recovery does not know which records
the image reflects, and does not try to.

That works because each record is applied as an assignment of the state it logged,
never as a change to whatever is there:

| Record | Applied as |
|--------|------------|
| node insert | the node exists with exactly the logged labels, their label-index entries, and no properties (its property records follow it in the log) |
| property set or removal, label add or remove | exactly that key or label, and its label-index entry, is set or cleared |
| node or edge delete | the node or edge is absent |
| edge insert | the edge exists with exactly the logged endpoints and type, and no properties: its record and both of its traversal keys, each put in place if it is missing and left alone if it is there |

Replaying one onto a state that already has it changes nothing. Replaying an older
record onto a newer state can change something, but the later record is in the log
after it and is replayed after it, and the image never runs ahead of the log, since
the log is synced before every flush. A record that finds its node or edge missing is
accepted only when a later committed record in the same log deletes it, which means
the file is from after that delete.

An edge is three entries in two trees, a record and two traversal keys, and
recovery does not assume it finds all of them or none. A commit that creates many
edges inserts every traversal key before any record (see
[the order of a commit](./transaction-manager.md#3-commit)), so a flush in the
middle of it writes keys whose records do not exist yet, and a crash then leaves
them in the file. Replay looks for each of the three and adds what is missing
(`EdgeStore.ensureWithId`). If a record is there under the same id with other
endpoints or another type, its traversal keys are removed first, whichever of
them exist. Up to 0.18.1 the redo of an edge was "delete it if its record exists,
then create it", which fails on a traversal key without a record; those releases
never wrote such a file, so they never met one. They are not asked to recover one
either. 0.18 cannot open a format-5 file at all. A file in the older layout that
this release has written is one 0.18 may read but not write, and replaying a log
counts as writing: while the log holds work, 0.18.1 refuses the file with
`NeedsNewerWriter`, read-only opens included, and leaves it unchanged. Once this
release has opened it and recovered, 0.18.1 reads it again.

The vector index needed an exception. Inserting a vector the index already holds is
a replacement, which unlinks the node from its neighbours and links it again, and
doing that for every vector in the log left a graph that searches could no longer
cross. Replay now skips a vector write when the index already holds exactly that
vector for the node.

### Replay errors

A committed record that cannot be replayed fails the open rather than being skipped,
since skipping it would open a database missing a change that was acknowledged:

| What recovery meets | The open fails with |
|---------------------|---------------------|
| A payload that does not decode, a payload type no release writes, a `page_write` (page image) record, or a change to a node or edge that is missing and not deleted later in the log | `InvalidDatabase` (`LATTICE_ERROR_CORRUPTION`) |
| A store that reports an error while applying a record | `IoError` |
| Memory running out | `OutOfMemory` |
| Vector records, with no vector index to replay them into | `IoError` |

The last case can only arise for a database last written by 0.16.0 whose
dimensions cannot be read from its vector pages, since every other database records
its index; the CLI recognises it and asks for the dimensions. The one vector record
that is skipped is a removal in a file that has never held vectors, which 0.16.0
logged for every deleted node and which has nothing to remove.

## Corruption Detection

When a frame fails its checksum, recovery must determine whether this is:

1. **An unsynced tail** - frames a power cut or a kill caught before they were synced
   (safe to drop: nothing in them was promised)
2. **Damage to synced data** - a frame that was promised durable (unsafe to drop)

Opening the log has already sorted every bad frame into one or the other (see
[Opening the Log](#opening-the-log)): it cut an unsynced tail off and recorded
damage, which recovery reports.

### Telling Them Apart

Each frame says how many frames had been synced when it was written. Take the
largest such claim among the frames that verify:

**Scenario 1 — an unsynced tail**

| Frame | Checksum | `synced_frames` |
|------:|----------|----------------:|
| 0 | OK | 0 |
| 1 | OK | 1 |
| 2 | Mismatch | |
| 3 | OK | 2 |
| 4 | Mismatch | |

The largest claim is 2, and the first bad frame is frame 2, which no frame claims was
synced. Frames 2 to 4 were written after the last sync and a power cut left them in
any order, frame 3 whole and frames 2 and 4 not. The log is cut back to frames 0
and 1. A kill without a power cut would leave all of them whole; the old rule, which
called any bad frame with a good one after it corruption, refused such a database.

**Scenario 2 — damage to synced data**

| Frame | Checksum | `synced_frames` |
|------:|----------|----------------:|
| 0 | OK | 0 |
| 1 | Mismatch | |
| 2 | OK | 2 |
| 3 | OK | 3 |

Frame 2 says two frames had been synced before it, so frame 1 was durable and its
damage is real. Recovery fails with `MidLogCorruption`, and the open fails with
`InvalidDatabase`, rather than silently discarding committed data.

A frame verifies only if it holds records. A frame that never reached the disk reads
back as zeroes, which would otherwise pass as an empty frame and let recovery read on
past it into the whole frames after it, replaying a transaction without the records
of the lost frame.

A log written by 0.16.0 carries no claims, and keeps the rule it was written under:
a bad frame with any valid frame after it is damage.

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
// At open (WalManager.findLogEnd), over every frame the header counts:
//   S = the largest synced_frames among frames that verify (version 3)
//   b = the first frame that does not verify
const corrupt = if (any_version3_frame) b < S else valid_frame_after_b;
if (corrupt) wal.damaged_frame = b      // recovery returns MidLogCorruption
else truncate the log to b frames       // an unsynced tail
```

A frame that goes bad after the open, while recovery reads it, is judged by the
0.16.0 rule: any frame that verifies after it makes it damage.

A large record is logged as a chain of fragments over several frames. A process
killed while writing the chain leaves its first fragments and no commit record.
When the record after an unfinished chain belongs to another transaction, the chain
is skipped and that record read as itself; a break inside one transaction's chain is
still damage.

## When an Open Is Refused

Opening refuses, rather than guess, when:

- the log belongs to a different database, or its header is damaged beyond the
  torn-header case above;
- the log is damaged in frames that had been synced;
- a committed record cannot be replayed ([Replay errors](#replay-errors));
- a read-only recovery would need more memory than its overlay may hold;
- on a read-write open of a file that does not record its node id counter (one last
  written by 0.16.0), the node tree cannot be scanned to find the next node id:
  starting the ids over at 1 would overwrite existing nodes;
- on a read-write open of an existing file, the symbol table cannot be scanned or
  the edge id counter cannot be read: restarting either would give a new label,
  edge type or property name the id of an existing one, or a new edge the id of
  an existing edge.

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
commit record reached the disk. In `.normal` mode the database after a power cut
holds a whole number of transactions up to some point no earlier than the last log
sync, none applied in part. In every mode a crash during recovery, a second power
cut included, changes nothing: the next recovery reaches the same state.
[Durability and the Log](../configuration/durability.md#what-survives-what) states
the guarantee in full.

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
    // Whose header a replayed changefeed run raises the reader version in
    .page_manager = &self.page_manager,
});
const stats = rm.recover(wal, &self.page_manager) catch |err| switch (err) {
    // Damage in synced frames, or a committed record that cannot be applied
    error.MidLogCorruption, error.InvalidRecord, error.CorruptedWal => return DatabaseError.InvalidDatabase,
    error.OutOfMemory => return DatabaseError.OutOfMemory,
    else => return DatabaseError.IoError,
};
```

`recoverDatabase(allocator, &wal, &pm)` and `RecoveryManager.init` also exist, but
they have no logical context: they scan the log and count what they would redo
without changing anything.

## Known Limits

- **Damage in the newest sync batch reads as a torn tail.** Only a frame written
  after a later sync can vouch that a frame was synced. Damage to the last frames
  before the most recent sync, with nothing written after them, is cut off as an
  unsynced tail, and the commits in them are lost without an error.
- **A read-only recovery is bounded by memory**: 64 MB plus four times the size of
  the log and journal. Beyond that the read-only open fails, and a read-write open
  is needed.
- **What the power-loss tests do not reach.** The power-loss simulation (see
  [Running Tests](../development/testing.md#power-loss-tests)) drives the Zig API on
  an in-memory disk model. Backup, restore and replication use the filesystem
  directly, outside the model, and the C API layer, Cypher and streams are not in its
  workload.

## Summary

| Concept | Purpose |
|---------|---------|
| Journal | Finish or discard an interrupted flush, so the file is one whole image |
| Unsynced-tail trim | Cut frames written after the last sync off the log at open, if any is bad |
| Analysis phase | Determine transaction outcomes |
| Redo phase | Apply committed operations |
| checkpoint_lsn | Recovery starting point |
| Transaction states | committed, aborted, in_progress |
| Checksum verification | Detect corruption/partial writes |
| Unsynced tail | Frames a crash caught before their sync, safe to drop |
| Synced damage | Real corruption, fail to prevent data loss |
| `synced_frames` | Each frame's claim of how much was synced, which tells the two apart |
| Read-only recovery | The same recovery over an in-memory overlay, writing nothing |
| No Undo | Uncommitted changes stay in the overlay, never applied |
| Logical redo | Committed records applied as assignments through the stores; safe to repeat, and a record that cannot be applied refuses the open |
| Checkpoint at open | Empty the log once recovery is done |

Recovery transforms a potentially inconsistent crash state into a consistent database by leveraging the WAL as the authoritative record of committed transactions.
