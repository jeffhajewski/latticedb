# Checkpointing

## The Problem

Without checkpointing, two bad things happen:

1. **WAL grows forever** - Every transaction appends records, file gets huge
2. **Recovery takes forever** - After crash, must replay entire WAL from the beginning

```
After 1 year of operation:

WAL: [millions of records from day 1 ... to today]
      ◄──────────────────────────────────────────►
              Recovery replays ALL of this
              Could take hours!
```

## The Solution

A checkpoint says: "Everything up to this point is safely on disk in the main database file. We don't need the old WAL records anymore."

```
Before checkpoint:
┌─────────────────────────────────────────────────────────┐
│                     Buffer Pool (RAM)                    │
│  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐        │
│  │ Page 5  │ │ Page 12 │ │ Page 3  │ │ Page 8  │        │
│  │ DIRTY   │ │ DIRTY   │ │ clean   │ │ DIRTY   │        │
│  └─────────┘ └─────────┘ └─────────┘ └─────────┘        │
└─────────────────────────────────────────────────────────┘
                        │
                        │ Changes only in RAM!
                        │ If power fails, they're lost
                        ▼
┌─────────────────────────────────────────────────────────┐
│                  Database File (Disk)                    │
│               [old versions of pages]                    │
└─────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────┐
│                      WAL (Disk)                          │
│  [record][record][record][record][record][record]...     │
│  ◄──────────────────────────────────────────────────►   │
│        Recovery needs ALL of these                       │
└─────────────────────────────────────────────────────────┘
```

```
After checkpoint:
┌─────────────────────────────────────────────────────────┐
│                     Buffer Pool (RAM)                    │
│  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐        │
│  │ Page 5  │ │ Page 12 │ │ Page 3  │ │ Page 8  │        │
│  │ clean   │ │ clean   │ │ clean   │ │ clean   │        │
│  └─────────┘ └─────────┘ └─────────┘ └─────────┘        │
└─────────────────────────────────────────────────────────┘
         │           │                       │
         │           │     FLUSHED!          │
         ▼           ▼                       ▼
┌─────────────────────────────────────────────────────────┐
│                  Database File (Disk)                    │
│               [current versions of pages]                │
└─────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────┐
│                      WAL (Disk)                          │
│  [old records...] │ CHECKPOINT │ [new records...]        │
│                   ▲                                      │
│            checkpoint_lsn                                │
│                   │                                      │
│     Recovery starts HERE ─────►                          │
│     (old records ignored)                                │
└─────────────────────────────────────────────────────────┘
```

## How It Works Step by Step

### Step 1: Write CHECKPOINT_BEGIN to WAL

```
WAL: [...existing records...][CHECKPOINT_BEGIN, lsn=1000]
```

This marks the start of the checkpoint. If we crash during checkpointing, recovery sees this and knows a checkpoint was in progress.

### Step 2: Sync WAL

```
fsync(wal_file)
```

Ensures CHECKPOINT_BEGIN is on disk before we start flushing pages.

### Step 3: Flush Dirty Pages

Scan the buffer pool and write dirty pages to the database file:

```
for each frame in buffer_pool:
    if frame.dirty:
        write frame.data to database_file at page offset
        frame.dirty = false
```

**Passive mode**: Skip pages that are currently pinned (in use by transactions). This avoids blocking active work.

**Full mode**: Wait for latches if needed. Guarantees all dirty pages are flushed.

### Step 4: Sync Database File

```
fsync(database_file)
```

Critical! This ensures all the page writes are actually on disk, not sitting in OS buffers.

### Step 5: Write CHECKPOINT_END to WAL

```
WAL: [...][CHECKPOINT_BEGIN][CHECKPOINT_END, lsn=1002]
```

This marks successful completion. The payload includes stats (pages flushed).

### Step 6: Update checkpoint_lsn

```
wal_header.checkpoint_lsn = 1002  // LSN of CHECKPOINT_END
fsync(wal_file)
```

This is the key marker for recovery. It says "everything before LSN 1002 is safely on disk."

## The Checkpoint Modes

### Passive Mode

```
"Checkpoint what you can without disrupting active work"

for each frame:
    if dirty AND not pinned AND can get latch immediately:
        flush it
    else:
        skip it (maybe next time)
```

Good for: Background checkpointing during normal operation. Doesn't block transactions.

Bad: May leave some dirty pages unflushed.

### Full Mode

```
"Checkpoint everything, wait if necessary"

for each frame:
    if dirty:
        wait for latch (spin until available)
        flush it
```

Good for: Complete checkpoint before shutdown, or when WAL is getting too large.

Bad: May briefly block transactions waiting for latches.

### Truncate Mode

```
"Full checkpoint, then reset WAL to beginning"

1. Do full checkpoint
2. Truncate WAL file to just the header
3. Reset frame_count to 0
```

Good for: Reclaiming disk space when WAL has grown large.

Requires: No active readers (they might be reading old WAL frames).

## Why CHECKPOINT_BEGIN and CHECKPOINT_END?

Consider what happens if we crash during checkpointing:

```
Crash scenario 1: After BEGIN, before any flushes
─────────────────────────────────────────────────
WAL: [...records...][CHECKPOINT_BEGIN]
                                     ▲
                                   crash

Recovery: Sees incomplete checkpoint (BEGIN but no END).
          Ignores it, replays from previous checkpoint_lsn.
          Safe!

Crash scenario 2: After some flushes, before END
─────────────────────────────────────────────────
WAL: [...records...][CHECKPOINT_BEGIN]
                                     ▲
                                   crash
Database file: Has SOME pages flushed, but not all

Recovery: Sees incomplete checkpoint.
          Some pages are updated, some aren't.
          Replays WAL from previous checkpoint_lsn.
          WAL replay overwrites pages with correct data.
          Safe!

Crash scenario 3: After END
───────────────────────────
WAL: [...records...][CHECKPOINT_BEGIN][CHECKPOINT_END]
                                                     ▲
                                                   crash

Recovery: Sees complete checkpoint.
          Starts replay from checkpoint_lsn.
          Fast recovery!
```

## Statistics

Each checkpoint returns stats:

```zig
CheckpointStats{
    pages_flushed: 42,        // How many dirty pages written
    duration_ns: 15_000_000,  // 15ms
    checkpoint_lsn: 1002,     // New checkpoint position
    wal_truncated: false,     // Whether WAL was reset
}
```

Useful for monitoring:
- If `pages_flushed` is always high, consider checkpointing more often
- If `duration_ns` is high, disk might be slow
- Track `checkpoint_lsn` growth over time

## When to Checkpoint

```zig
// Check if WAL has grown too large
if checkpointer.shouldCheckpoint(max_wal_frames: 1000) {
    try checkpointer.checkpoint(.passive);
}
```

Common strategies:
- **Size-based**: When WAL exceeds N frames
- **Time-based**: Every N minutes
- **Transaction-based**: After N commits
- **On shutdown**: Always do full checkpoint before closing

## Integration

```
┌─────────────────────────────────────────────────────────────────┐
│                    Checkpointer                                  │
│                         │                                        │
│         ┌───────────────┼───────────────┐                       │
│         ▼               ▼               ▼                       │
│   ┌──────────┐   ┌──────────┐   ┌──────────┐                   │
│   │BufferPool│   │PageManager│   │WalManager│                   │
│   │          │   │          │   │          │                   │
│   │• frames  │   │• writePage│   │• append  │                   │
│   │• dirty   │   │• sync     │   │• sync    │                   │
│   │• latches │   │          │   │• setLsn  │                   │
│   └──────────┘   └──────────┘   └──────────┘                   │
└─────────────────────────────────────────────────────────────────┘
```

The Checkpointer coordinates between:
- **BufferPool**: Knows which pages are dirty
- **PageManager**: Writes pages to database file
- **WalManager**: Records checkpoint markers, updates checkpoint_lsn

## API

```zig
var checkpointer = Checkpointer.init(allocator, &bp, &pm, &wal);

// Passive checkpoint (background)
const stats = try checkpointer.checkpoint(.passive);

// Full checkpoint (before shutdown)
const stats = try checkpointer.checkpoint(.full);

// Truncate checkpoint (reclaim WAL space)
const stats = try checkpointer.checkpoint(.truncate);

// Check if checkpoint needed
if (checkpointer.shouldCheckpoint(1000)) {
    _ = try checkpointer.checkpoint(.passive);
}

// Get last checkpoint stats
if (checkpointer.getLastStats()) |stats| {
    log("Flushed {} pages in {}ms", stats.pages_flushed, stats.duration_ns / 1_000_000);
}
```

## Summary

| Concept | Purpose |
|---------|---------|
| Dirty page flush | Write modified pages to database file |
| checkpoint_lsn | Recovery starting point |
| CHECKPOINT_BEGIN/END | Crash safety during checkpoint |
| Passive mode | Non-blocking background checkpoint |
| Full mode | Complete checkpoint, may block briefly |
| Truncate mode | Reclaim WAL disk space |

Checkpointing is the bridge between WAL-based durability (fast commits) and bounded recovery time (fast restart). Without it, durability requires replaying the entire history on every startup.
