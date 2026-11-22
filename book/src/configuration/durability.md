# Durability and the Log

Every change is written to a log before it reaches the database file. This page is
about what that buys you, the settings that control it, and what happens if you
turn it off.

For what a transaction guarantees, see
[Transactions and Durability](../guides/transactions.md). For how the log is
structured, see [Write-Ahead Log](../architecture/wal.md).

## What committing means

1. The change is written to the log.
2. The commit record is flushed to disk (in the default sync mode).
3. Only then is the change applied in memory and the commit reported as successful.

The database file itself is updated later, in a flush that writes every changed
page at once: at a checkpoint, at close, on an explicit `sync()`, or early when half
the page cache holds changes. A crash before that loses nothing: the log is replayed
on the next open and the change comes back.

This is why a commit is fast even though it is durable. Appending to a log is one
sequential write; updating the database file properly means several scattered ones,
and those are batched.

If a commit fails after its commit record may have reached the disk (the flush to
disk itself fails, or applying the change fails), the outcome cannot be taken back.
The handle marks itself failed: it refuses further transactions, `sync()` and
checkpoints, and writes nothing more to the database file. Close it and open the
database again; recovery replays the log and gives exactly the state it describes.

## Files next to the database

| File | What it is |
|------|------------|
| `<db>` | The database |
| `<db>-wal` | The write-ahead log. Emptied to a 4 KB header by truncating checkpoints, including the one at a clean close after writes |
| `<db>-journal` | The flush journal. Present while the database is open read-write, empty between flushes, deleted at a clean close |

The journal holds a copy of the pages a flush is writing, written and synced before
any of them goes into the database file. If the process or the machine dies part
way through the flush, the next open finishes it from the journal, so the database
file is always one whole image, never half of one flush and half of another. Leave
the journal alone: a journal left after a crash is what the next open needs. A
read-only open finishes the flush in memory, without writing; see
[Opening read-only after a crash](#opening-read-only-after-a-crash).

In-memory databases have neither a journal nor files of their own.

## Sync mode

Most of the time a small write transaction takes is the flush of its commit record
to disk. `sync_mode` decides whether a commit waits for it.

| Setting | Default | What it does |
|---------|---------|--------------|
| `sync_mode` | `.full` | `.full` flushes the log to disk on every commit. `.normal` hands the commit record to the operating system and returns. |

With `.normal`, a transaction that has returned survives the process being killed,
and reaches the disk at the next checkpoint, `sync()` or flush. A power cut or an
operating-system crash can lose the most recent commits. They are lost from the end
only, the database stays consistent, and everything up to the last sync survives.
[What survives what](#what-survives-what) states this exactly.

Set it when you open the database:

| API | Setting |
|-----|---------|
| Zig | `DatabaseConfig.sync_mode = .normal` |
| C | `lattice_open_options_v5.sync_mode = LATTICE_SYNC_MODE_NORMAL` |
| Python | `Database(..., sync_mode="normal")` |
| TypeScript | `new Database(path, { syncMode: 'normal' })` |
| Go | `OpenOptions{SyncMode: latticedb.SyncModeNormal}` |
| Java | `OpenOptions.defaults().syncMode(SyncMode.NORMAL)` |

A program that does not set it gets the `LATTICE_SYNC_MODE` environment variable if
that is set, and `.full` otherwise. A value the program sets wins over the
environment. The CLI has no flag for it, so there the variable is the way to set it:

```bash
LATTICE_SYNC_MODE=normal   # or full; case does not matter, other values are ignored
```

## Full fsync on macOS

On macOS, `fsync` returns once the drive has the data, possibly only in the drive's
own cache. A commit there survives a process or operating-system crash but not
necessarily a power cut, and the ordering the flush journal relies on is not assured
either.

| Setting | Default | What it does |
|---------|---------|--------------|
| `full_fsync` | off | On macOS, every sync uses `fcntl(F_FULLFSYNC)`, which asks the drive to flush its cache. No effect on other platforms, where `fsync` already asks for that. |

It is off by default because it makes every sync, and so every commit in the
default sync mode, several milliseconds slower. Turn it on if the database must
survive power loss on a Mac: `full_fsync = true` in Zig,
`LATTICE_FULL_FSYNC_ON` in the C API's `lattice_open_options_v5.full_fsync`,
`full_fsync=True` in Python, `fullFsync: true` in TypeScript,
`FullFsync: latticedb.FullFsyncOn` in Go, `.fullFsync(true)` in Java. As with
`sync_mode`, a program that leaves it unset gets the environment variable, and the
CLI is set only through it:

```bash
LATTICE_FULL_FSYNC=1       # or true; 0 or false turns it off; other values are ignored
```

## Environment variables

The engine reads these when a database is opened, whatever API opened it.
Each applies only when the program did not set the option itself.

| Variable | Values | Default | Effect |
|----------|--------|---------|--------|
| `LATTICE_SYNC_MODE` | `full`, `normal` | unset (`full`) | Whether a commit waits for the disk; see [Sync mode](#sync-mode) |
| `LATTICE_FULL_FSYNC` | `1`/`true`, `0`/`false` | unset (off) | `F_FULLFSYNC` on macOS; see [Full fsync on macOS](#full-fsync-on-macos) |
| `LATTICE_BUFFER_POOL_MB` | a positive integer | unset | Page cache size where the program sets none (an explicit size wins); see [Opening a Database](./opening.md#memory-and-caching) |
| `LATTICE_AUTO_CHECKPOINT_FRAMES` | a non-negative integer | unset (scaled to the cache) | Log frames a commit waits for before checkpointing, 0 for none, in the C API, the bindings and the CLI; used as given, not scaled; see [Checkpointing](#checkpointing) |

## Running without the log

`enable_wal = false` means one thing: nothing is logged. Everything else works as it
does with a log. Transactions begin, commit and roll back; a writing query outside
a transaction gets one of its own and is all or nothing; one writer at a time;
readers keep their snapshot; property indexes, full-text search, vectors, streams
and the changefeed are updated at commit and untouched by a rollback. There is no
`-wal` file.

What you give up is durability between flushes. A commit changes pages in memory,
and they reach the file only when they are flushed: at close, at the end of a
commit once half the page cache holds changes, or, in the Zig API, on `sync()`.
A flush never happens in the middle of applying a transaction, and each flush
goes through the journal, so the file always holds a whole number of
transactions.

| Event | What a database without a log keeps |
|-------|-------------------------------------|
| Process killed, or the machine loses power | The transactions up to some point at or after the last close, or Zig `sync()`, that returned. Never part of one. Commits after that point may be lost, from the end only. |
| A commit fails while it is being applied (the disk is full, say) | The handle marks itself failed and writes nothing more. Reopened, the database holds the state of the last flush: that commit and every one since are lost. |
| Clean close | Everything. |

After anything you could not redo, call `sync()` in Zig. The C API and the
bindings have no call that flushes without closing (`checkpoint` does nothing
without a log), so there close the database, and open it again to go on. The
power-loss case assumes the same of the storage as
[What survives what](#what-survives-what) does.

Two costs come with it. A transaction's changed pages stay in memory until the whole
transaction has been applied, so one very large transaction can hold the page cache
above its configured size until it commits. And in the Zig API, a write passed a
`null` transaction is applied as it is made, with or without a log, and is never
logged; without a log that makes no difference to what survives, but it is not part
of any transaction, so it does not appear in the changefeed.

### Bulk loading

Three things decide how long a large load takes, in this order: how many rows go
into each transaction, how large the page cache is, and whether the load needs the
log at all.

**Use large transactions for relationships.** A relationship is filed twice in
the edge tree, once under each of its two nodes, and those two places are wherever
the nodes' other relationships are. Relationships between arbitrary nodes
therefore touch pages all over the tree. A commit that creates 64 or more
relationships sorts all their entries first and inserts them in order, so each
page gets everything the transaction has for it in one visit and is written once.
The more relationships one transaction holds, the more of them share a page: with
2,000,000 relationships over 1,000,000 nodes the edge tree ends with about 50,000
leaf pages, so a transaction of 10,000 relationships puts about one entry on each
page it touches and a transaction of 100,000 about four.
A transaction keeps its changes in memory until it commits, 600 to 700 bytes per
row in this load (a relationship without properties), so 100,000 rows cost some
70 MB and 1,000,000 about 560 MB on top of the cache.

**Give it cache.** A flush writes each changed page twice, once to the journal
and once in place, and happens whenever half the cache holds changes. With a small
cache and writes spread over the whole graph, the same pages are changed and
written again flush after flush; once half the cache holds what the load changes,
each page is written about twice in all. The log follows the cache: by default
it may grow to an eighth of it before a checkpoint (see
[Why the interval follows the cache](#why-the-interval-follows-the-cache)).

**Drop the log if the load can be run again.** A load you can repeat from its
source does not need the log: if it is interrupted, delete the file and start
over. For such a load:

- open with `enable_wal = false`;
- set `changefeed_retention` to `off` if nothing will read the load's changes
  from the changefeed (the policy is kept in the file; open with `all` afterwards
  to record changes from then on);
- give it a page cache about twice the size the file will reach;
- then close (or, in Zig, `sync()`).

Loading 1,000,000 nodes and 2,000,000 relationships between random pairs of them
through the C API (ReleaseFast, Apple M5, 2026-11-20, a new format-5 file, the
median of three runs; "defaults" are the log on with `full` sync, the changefeed
keeping every event, and a 100 MB cache):

| Settings | Rows per transaction | Time | Written to disk | File | 0.18.1 |
|----------|---------------------:|-----:|----------------:|-----:|-------:|
| Defaults | 1,000 | 28.6 s | 29 GB | 452 MB | 37.9 s |
| Defaults | 10,000 | 23.4 s | 25 GB | 451 MB | 35.9 s |
| Defaults | 100,000 | 11.3 s | 5.2 GB | 461 MB | 33.6 s |
| Defaults | 1,000,000 | 8.7 s | 1.9 GB | 458 MB | |
| 1 GB cache | 10,000 | 9.3 s | 2.4 GB | 451 MB | 16.7 s |
| 1 GB cache | 100,000 | 8.6 s | 2.2 GB | 461 MB | |
| No log, changefeed off, 1 GB cache | 10,000 | 6.4 s | 0.9 GB | 315 MB | 8.2 s |
| No log, changefeed off, 1 GB cache | 100,000 | 5.9 s | 1.0 GB | 326 MB | |

The last column is 0.18.1 on the same machine with the same program, where it was
measured; its file was 500 MB with the defaults and 364 MB without the changefeed.
0.18.1 gained nothing from larger transactions. Peak memory was the cache plus what
the open transaction held: 130 MB at 10,000 rows with the default cache, 190 MB at
100,000 and 680 MB at 1,000,000.

With the default cache and 10,000 rows or fewer, most of the time still goes to
writing the same pages again: 25 GB written for a 450 MB file. If the transactions
cannot be larger, a larger cache does the same job.

[Benchmarks](../performance/benchmarks.md#bulk-load) splits the same runs into
the time for the nodes and the time for the relationships, and has the rates for
batched writes into a smaller graph.

## Checkpointing

The log grows as you write. A checkpoint folds it back into the database file and
resets it, which is what stops it growing forever.

This happens on its own, after a commit, once the log holds enough frames. How
many depends on the page cache: by default the log may grow to an eighth of the
cache, never less than 1,000 frames (4 MB) and never more than 64 MB. With the
100 MB cache the C API and the bindings open with, that is 3,200 frames, 12.5 MB of
log; the CLI's 64 MB cache gives 2,048; a cache of 512 MB or more gives 16,384, the
64 MB cap. Each write transaction adds at least one frame, so with a small cache a
checkpoint comes at most every thousand write transactions. Open readers do not
hold it back. It is skipped
only while an open transaction already has records in the log (one whose commit
failed part way and has not been rolled back yet), and the next commit tries again.

The automatic checkpoint runs inside the commit that triggers it, not in the
background, so that one commit takes longer. Of 20,000 single-node commits through
the C API with the default settings, measured when the interval was still a fixed
1,000 frames (see [Benchmarks](../performance/benchmarks.md)),
the median took 44 µs and the 99.9th percentile 1.4 ms, which is where the commits
that ran a checkpoint, and the disk's own stalls, show; the slowest single commit
ranged from 2 ms to over 200 ms from run to run, as it did in 0.16.0. If a pause of
that size matters to you, set a larger or smaller interval, or turn the automatic checkpoint off and run
checkpoints at moments you choose. A larger interval means fewer pauses, each
longer, a longer log on disk and more to replay after a crash; a smaller one means
shorter pauses, more often.

`auto_checkpoint` decides when. Zig sets it in `DatabaseConfig`, or on an open
database with `Database.setAutoCheckpoint`; C with
`lattice_set_auto_checkpoint(db, max_wal_frames, min_interval_ms)`; the bindings
with `set_auto_checkpoint` (Python), `setAutoCheckpoint` (TypeScript, Java) and
`SetAutoCheckpoint` (Go); the CLI with `--auto-checkpoint=<frames>`. Where none of
those is used, the C API, the bindings and the CLI take the frame count from
`LATTICE_AUTO_CHECKPOINT_FRAMES` if it is set:

| Setting | Default | What it does |
|---------|---------|--------------|
| `max_wal_frames` | an eighth of the cache, 1,000 to 16,384 frames | Frames written before a checkpoint is considered. A number you set is used as it is. |
| `min_interval_ns` | 0 | Shortest gap between two checkpoints. |
| `mode` | `truncate` | Only `truncate` resets the log, so it is the only mode that bounds its size. |

A frame count of 0 turns the checkpoints inside commits off, which is worth doing if
you want to choose the moment rather than have it land mid-request; the database
still checkpoints at open and close. From Zig, opening with `auto_checkpoint` set to
null also turns off the checkpoints at open and close described below.

The default is counted in bytes of log and converted to the log's own frames, one
page each, so a database with 16 KB pages checkpoints after a quarter as many
frames as one with 4 KB pages. A frame count you set, through any of the setters
above or `LATTICE_AUTO_CHECKPOINT_FRAMES`, is a frame count and is used as it is.

The minimum interval defaults to zero deliberately. Under `truncate` the frame
threshold already limits the rate, because a checkpoint resets the frame count and
the next one cannot happen until another `max_wal_frames` frames are written. A time gate
on top of that does not prevent thrash; it just lets the log grow without limit
during a burst of writes, which is the thing this is meant to stop.

A checkpoint also runs when a database is opened, after recovery, and when it is
closed, through any API or the CLI, if the log holds changes since its last
checkpoint. A cleanly closed database therefore leaves no journal and nothing in the
log to replay. A log that ends at a checkpoint is left as it is, which matters for
replication: see [Backup and Replication](../guides/backup-and-replication.md).

### Why the interval follows the cache

A checkpoint writes every page dirtied since the last one twice, once into the
journal and once in place, and syncs. With a large cache, checkpointing every
4 MB of log throws away what the cache could hold. A bulk load of a million nodes
and two million relationships, 10,000 rows per transaction, took 12.8 s with a 1 GB
cache and the old fixed 1,000 frames, and 9.2 s with 64 MB of log (8.5 GB written
against 2.4 GB). With the 100 MB cache the interval made no measurable difference:
there the flush that runs when half the cache is dirty does most of the writing.
The 64 MB cap bounds how much log a crash leaves to replay and how large the `-wal`
file grows. Replay is what the larger log costs. The same load killed with 60 MB in
the log took 2.0 s to open again; killed with 11 MB in the log (the 100 MB cache's
limit) it took 0.4 s, and with the old 4 MB, 0.2 to 0.4 s.

To get the old fixed interval back, set it to 1,000 frames: with
`LATTICE_AUTO_CHECKPOINT_FRAMES=1000`, `--auto-checkpoint=1000`, or 1000 in any of
the setters above.

## Checkpointing by hand

```bash
lattice checkpoint social.lattice
```

You mostly do not need this. It is worth running before copying a database file,
so the copy is complete on its own, and on a database that has been open a very
long time under heavy writes if you would rather pick the moment. A program
checkpoints its open database with `lattice_checkpoint(db, mode, &stats)` in C,
`checkpoint()` in Python, TypeScript and Java, `Checkpoint` in Go, or
`Database.checkpoint` in Zig.

This is not `compact`. Checkpointing shrinks the log, not the database file.

## What survives what

The guarantee after a power cut, assuming the storage honours `fsync` (and
`F_FULLFSYNC` on macOS when `full_fsync` is on) and that a completed sync is not
later undone or damaged by the device:

- **`sync_mode = .full`** (the default). A transaction whose commit call has
  returned survives. A transaction whose commit had not returned is either wholly
  there or wholly absent. After recovery the database holds exactly the
  transactions committed up to some point at or after the last acknowledged one,
  with every index consistent with the graph.
- **`sync_mode = .normal`.** The log is handed to the operating system at commit,
  and synced at checkpoints, on `sync()`, when a full cache forces a flush, and at
  close. After a power cut the database holds exactly the transactions up to some
  point no earlier than the last log sync: a whole number of transactions, none
  applied in part. Commits acknowledged after the last log sync may be lost. A
  process kill without a power cut loses nothing acknowledged.
- **Both modes.** A crash during recovery, a second power cut included, changes
  nothing: the next recovery reaches the same state. A log damaged where it had
  already been synced refuses to open (`InvalidDatabase`,
  `LATTICE_ERROR_CORRUPTION` in the C API) rather than silently dropping the
  commits in it. The one exception is the newest sync batch: damage there that no
  later frame vouches for cannot be told apart from a write the power cut tore, and
  is cut off as a torn tail. See
  [Write-Ahead Log](../architecture/wal.md#the-checksum) for how the log tells them
  apart.

These are checked by a power-loss simulation in the crash suite, which cuts the
power at every sync point of a random workload; see
[Running Tests](../development/testing.md#power-loss-tests).

| Event | What happens |
|-------|--------------|
| Process crashes or is killed | Every commit that returned is there on the next open, in either sync mode. A commit that was running is either all there or not at all. |
| Machine loses power, `sync_mode = .full`, Linux | Every commit that returned is there, as above. |
| Machine loses power, `sync_mode = .full`, macOS | The same only with `full_fsync` on. Without it, nothing is guaranteed. |
| Machine loses power, `sync_mode = .normal` | Commits since the last log sync may be lost, from the end only. |
| Database file is copied while open | Broken. The log holds committed changes the file does not have. Use `backup`. |
| The `-wal` or `-journal` file is deleted after a crash | Committed changes not yet in the file are gone, and an interrupted flush cannot be finished. Leave both in place. |
| An in-memory database's process exits | Everything is gone. That is what in-memory means. |

The copy row is the one that catches people, and it fails silently at copy time —
you find out at restore. [Backup and Replication](../guides/backup-and-replication.md)
covers the tools that do it correctly.

## Opening read-only after a crash

A read-only open recovers a crashed database in memory. When the log holds frames
or the journal holds a flush, the open reads the files without writing to them,
finishes the flush and replays the log exactly as a read-write open would, and keeps
every page that recovery changes in memory. The handle then shows every committed
transaction, and nothing on disk changes; the next read-write open recovers the
files itself. A database with an empty log and no journal opens read-only as it
always did, with nothing extra in memory.

The memory this takes is bounded at 64 MB plus four times the size of the log and
the journal. A recovery that would need more fails the open with `BufferPoolFull`
(`LATTICE_ERROR_FULL` in the C API); open the database read-write once to recover
it on disk. A log damaged in its synced part refuses a read-only open as it refuses
a read-write one.

The CLI's read-only commands (`count`, `info`, `labels`, `export` and the rest) open
this way. `lattice check` reads the database file's pages, not the log, and
finishes an interrupted flush from the journal in memory before it checks them, so
it checks the file as the journal leaves it, without changing it.

## Where to go next

- [Transactions and Durability](../guides/transactions.md) — what a transaction
  guarantees and the one-writer rule
- [Backup and Replication](../guides/backup-and-replication.md) — keeping a copy
  that is actually usable
- [Write-Ahead Log](../architecture/wal.md) — the format and the recovery path
