# Write-Ahead Log (WAL)

## The Problem

Imagine you're updating a B+Tree. You need to:
1. Modify a leaf page
2. Maybe split it (modify parent)
3. Maybe split the parent (modify grandparent)
4. Update the root

If power fails between steps 2 and 3, your database is **corrupted** - the tree structure is broken.

Even a single page write isn't safe. A 4KB page write might not be atomic at the hardware level - you could end up with half old data, half new data (a "torn write").

## The Insight

Instead of modifying data pages directly, first write a **log** of what you intend to do. The log is append-only and sequential. Only after the log is safely on disk do you modify the actual data pages.

If you crash:
- **Before log write**: Nothing happened, no corruption
- **After log write, before data write**: Replay the log to finish the operation
- **After both**: Everything is fine

This is the **write-ahead** rule: log first, then data.

## Why Append-Only Sequential Writes Are Special

The WAL relies on a key property: **sequential append is much safer than random writes**.

```
Random writes (data pages):          Sequential append (WAL):
┌─────┐ ┌─────┐ ┌─────┐              ┌─────────────────────────┐
│ P1  │ │ P2  │ │ P3  │              │ Log Log Log Log ...     │
└─────┘ └─────┘ └─────┘              └─────────────────────────┘
   ↑       ↑       ↑                                         ↑
   │       │       │                                         │
 write   write   write                              single write position
```

With random writes, the disk head jumps around. A crash can leave any combination of pages in inconsistent states.

With sequential append:
- Only one "frontier" where writing happens
- Everything before the frontier is complete
- Everything after doesn't exist yet
- Much simpler to reason about crash states

## The System Primitive: fsync

The WAL's safety depends on one critical system call: **fsync** (or fdatasync).

```c
write(fd, data, len);  // Data goes to OS buffer cache
fsync(fd);             // Forces data to physical disk platters
```

Without fsync, your "written" data might sit in RAM for seconds or minutes. A power failure loses it. fsync is a **durability barrier** - when it returns, data is on persistent storage.

This is expensive (milliseconds, not microseconds), so we batch multiple records into frames before syncing.

## WAL Structure

```
┌────────────────────────────────────────────────────────────┐
│                    WAL File Layout                          │
├────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌─────────────────────────────────────────────────────┐   │
│  │ Header (4KB)                                         │   │
│  │  • magic: 0x574C4F47 ("WLOG")                        │   │
│  │  • database_uuid: links WAL to its database          │   │
│  │  • frame_count: how many frames written              │   │
│  │  • checkpoint_lsn: recovery starting point           │   │
│  │  • checksum: detects corruption                      │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                             │
│  ┌─────────────────────────────────────────────────────┐   │
│  │ Frame 0 (4KB)                                        │   │
│  │  ┌───────────────────────────────────────────────┐   │   │
│  │  │ Frame Header (32 bytes)                        │   │   │
│  │  │  • frame_number: 0                             │   │   │
│  │  │  • record_count: 3                             │   │   │
│  │  │  • data_size: 156                              │   │   │
│  │  │  • checksum: CRC of data area                  │   │   │
│  │  └───────────────────────────────────────────────┘   │   │
│  │  ┌───────────────────────────────────────────────┐   │   │
│  │  │ Record: TXN_BEGIN (txn=1, lsn=1)               │   │   │
│  │  └───────────────────────────────────────────────┘   │   │
│  │  ┌───────────────────────────────────────────────┐   │   │
│  │  │ Record: INSERT (txn=1, lsn=2, payload=...)     │   │   │
│  │  └───────────────────────────────────────────────┘   │   │
│  │  ┌───────────────────────────────────────────────┐   │   │
│  │  │ Record: TXN_COMMIT (txn=1, lsn=3)              │   │   │
│  │  └───────────────────────────────────────────────┘   │   │
│  │                     ... unused space ...              │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                             │
│  ┌─────────────────────────────────────────────────────┐   │
│  │ Frame 1 (4KB)                                        │   │
│  │   ...                                                │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                             │
└────────────────────────────────────────────────────────────┘
```

## LSN: The Universal Clock

Every record gets a **Log Sequence Number (LSN)** - a monotonically increasing integer.

```
LSN 1: TXN_BEGIN (txn 1)
LSN 2: INSERT key="alice" (txn 1)
LSN 3: INSERT key="bob" (txn 1)
LSN 4: TXN_BEGIN (txn 2)
LSN 5: DELETE key="charlie" (txn 2)
LSN 6: TXN_COMMIT (txn 1)
LSN 7: TXN_ABORT (txn 2)
```

LSN serves multiple purposes:
1. **Ordering**: Total order of all operations
2. **Recovery point**: "Replay from LSN 42"
3. **Page tracking**: Each data page stores the LSN of the last modification

## The prev_lsn Chain

Each record stores `prev_lsn` - the previous LSN **for the same transaction**:

```
LSN 1: TXN_BEGIN (txn=1, prev_lsn=0)
LSN 2: INSERT (txn=1, prev_lsn=1)      ←─┐
LSN 3: INSERT (txn=2, prev_lsn=0)         │
LSN 4: UPDATE (txn=1, prev_lsn=2)      ───┘
LSN 5: TXN_COMMIT (txn=1, prev_lsn=4)
```

This creates a **backward chain** for each transaction:

```
Transaction 1: LSN 5 → LSN 4 → LSN 2 → LSN 1
Transaction 2: LSN 3 (just one operation)
```

Why? **Rollback**. To abort transaction 1, follow the chain backward, undoing each operation.

## Write Flow

```
                    Application
                        │
                        ▼
              ┌─────────────────┐
              │   Transaction   │
              │   INSERT(k,v)   │
              └────────┬────────┘
                       │
                       ▼
              ┌─────────────────┐
              │   WAL Manager   │
              │                 │
              │  1. Assign LSN  │
              │  2. Build record│
              │  3. Append to   │
              │     frame buffer│
              └────────┬────────┘
                       │
          ┌────────────┴────────────┐
          │                         │
          ▼                         ▼
   Frame buffer full?          COMMIT requested?
          │                         │
          ▼                         ▼
   ┌─────────────┐           ┌─────────────┐
   │ Flush frame │           │ Flush frame │
   │ to disk     │           │ + fsync     │
   └─────────────┘           └─────────────┘
                                    │
                                    ▼
                            ┌─────────────┐
                            │ Now safe to │
                            │ return to   │
                            │ application │
                            └─────────────┘
```

The key insight: we buffer multiple records in memory, only hitting disk when:
1. The frame buffer is full (4KB of records accumulated)
2. A transaction commits (durability guarantee)

## Commit Protocol

When you call `COMMIT`:

```
1. Write TXN_COMMIT record to WAL buffer
2. Flush current frame to disk
3. Call fsync() - WAIT for disk acknowledgment
4. Return "committed" to application
```

After step 3, even if power fails immediately, the commit record is on disk. Recovery will see the commit and know the transaction succeeded.

If we crash before step 3? The commit record might be lost. Recovery won't see a commit for that transaction, so it will be rolled back. **No partial commits ever escape to the application.**

## Recovery: Redo and Undo

On startup after a crash, we need to:

1. **Find the end of valid log** - Scan frames, verify checksums, find last valid record
2. **Redo committed transactions** - Replay all operations from committed transactions
3. **Undo uncommitted transactions** - Roll back any transaction without a commit record

```
                     WAL Contents
    ┌───────────────────────────────────────────┐
    │ LSN 1: TXN_BEGIN (txn 1)                  │
    │ LSN 2: INSERT x=1 (txn 1)                 │
    │ LSN 3: TXN_BEGIN (txn 2)                  │
    │ LSN 4: INSERT y=2 (txn 2)                 │
    │ LSN 5: TXN_COMMIT (txn 1)    ← committed  │
    │ LSN 6: INSERT z=3 (txn 2)                 │
    │ ─── CRASH HERE ───                        │
    └───────────────────────────────────────────┘

    Recovery:
    • Txn 1: Has COMMIT → REDO (x=1 is permanent)
    • Txn 2: No COMMIT  → UNDO (y=2 and z=3 are rolled back)
```

## Why Frames?

Why not just append individual records?

**Reason 1: Atomicity**

A 4KB write is more likely to be atomic than arbitrary-sized writes. Many storage systems guarantee 512-byte or 4KB atomic writes. By aligning to page boundaries, we reduce torn write risk.

**Reason 2: Batching**

Each fsync is expensive (~1-10ms). By batching records into frames, we amortize the sync cost across many operations.

**Reason 3: Checksums**

Each frame has a checksum. During recovery, we can detect partial/corrupted frames and stop replay at the right point.

## The Checksum

We use CRC32 to detect corruption:

```
Frame on disk:
┌──────────────────────────────────────────┐
│ Header: checksum = 0xABCD1234            │
├──────────────────────────────────────────┤
│ Data: [record1][record2][record3]        │
└──────────────────────────────────────────┘

On read:
1. Read frame
2. Compute CRC32(data)
3. Compare with stored checksum
4. If mismatch → corruption detected, stop replay
```

This catches:
- Torn writes (partial frame written)
- Bit rot (storage degradation)
- Wrong WAL file (UUID also checked)

## The UUID Link

The WAL header stores the database file's UUID:

```
Database file header:      WAL header:
┌──────────────────┐      ┌──────────────────┐
│ uuid: ABC123...  │ ←──→ │ uuid: ABC123...  │
└──────────────────┘      └──────────────────┘
```

This prevents accidentally using database A's WAL with database B. The UUID is generated randomly when the database is created.

## Record Types

```zig
pub const WalRecordType = enum(u8) {
    // Transaction control
    txn_begin = 0x01,
    txn_commit = 0x02,
    txn_abort = 0x03,

    // Data modifications
    insert = 0x10,
    update = 0x11,
    delete = 0x12,

    // Page-level operations
    page_write = 0x20,

    // Checkpointing
    checkpoint_begin = 0x30,
    checkpoint_end = 0x31,

    // Savepoints
    savepoint = 0x40,
    savepoint_rollback = 0x41,

    // Compensation (for undo during recovery)
    clr = 0x50,
};
```

## WalManager API

```zig
// Append a record, get back its LSN
const lsn = try wal.appendRecord(.insert, txn_id, prev_lsn, payload);

// Force all buffered records to disk
try wal.sync();

// Iterate records for recovery
var iter = wal.iterate(start_lsn);
while (try iter.next(&buf)) |record| {
    // Process record
}

// Update checkpoint position
try wal.setCheckpointLsn(lsn);
```

## Summary

| Concept | Purpose |
|---------|---------|
| Write-ahead rule | Log before data = crash safety |
| Sequential append | Simpler crash states than random writes |
| fsync | Durability barrier to physical storage |
| LSN | Universal ordering of all operations |
| prev_lsn chain | Enables transaction rollback |
| Frames | Atomic-ish writes + batching |
| Checksums | Detect corruption and torn writes |
| UUID | Match WAL to correct database file |

The WAL transforms the problem from "make random writes atomic" (very hard) to "make sequential append reliable" (much easier).
