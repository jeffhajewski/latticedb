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
`LATTICE_SYNC_MODE` and `LATTICE_FULL_FSYNC` apply only when the program did not set
the option itself. `LATTICE_BUFFER_POOL_MB` overrides the cache size even when the
program set one.

| Variable | Values | Default | Effect |
|----------|--------|---------|--------|
| `LATTICE_SYNC_MODE` | `full`, `normal` | unset (`full`) | Whether a commit waits for the disk; see [Sync mode](#sync-mode) |
| `LATTICE_FULL_FSYNC` | `1`/`true`, `0`/`false` | unset (off) | `F_FULLFSYNC` on macOS; see [Full fsync on macOS](#full-fsync-on-macos) |
| `LATTICE_BUFFER_POOL_MB` | a positive integer | unset | Page cache size where the program sets none (an explicit size wins); see [Opening a Database](./opening.md#memory-and-caching) |

## Running without the log

`enable_wal = false` means one thing: nothing is logged. Everything else works as it
does with a log. Transactions begin, commit and roll back; a writing query outside
a transaction gets one of its own and is all or nothing; one writer at a time;
readers keep their snapshot; property indexes, full-text search, vectors, streams
and the changefeed are updated at commit and untouched by a rollback. There is no
`-wal` file.

What you give up is durability between flushes. A commit changes pages in memory,
and they reach the file only when they are flushed: on `sync()`, at close, or at
the end of a commit once half the page cache holds changes. A flush never happens
in the middle of applying a transaction, and each flush goes through the journal,
so the file always holds a whole number of transactions.

| Event | What a database without a log keeps |
|-------|-------------------------------------|
| Process killed, or the machine loses power | The transactions up to some point at or after the last `sync()` or close that returned. Never part of one. Commits after that point may be lost, from the end only. |
| A commit fails while it is being applied (the disk is full, say) | The handle marks itself failed and writes nothing more. Reopened, the database holds the state of the last flush: that commit and every one since are lost. |
| Clean close | Everything. |

Call `sync()` after anything you could not redo. The power-loss case assumes the
same of the storage as [What survives what](#what-survives-what) does.

Two costs come with it. A transaction's changed pages stay in memory until the whole
transaction has been applied, so one very large transaction can hold the page cache
above its configured size until it commits. And in the Zig API, a write passed a
`null` transaction is applied as it is made, with or without a log, and is never
logged; without a log that makes no difference to what survives, but it is not part
of any transaction, so it does not appear in the changefeed.

### Bulk loading

A load you can run again from its source does not need the log: if it is
interrupted, delete the file and start over. For such a load:

- open with `enable_wal = false`;
- set `changefeed_retention` to `off` if nothing will read the load's changes
  from the changefeed (the policy is kept in the file; open with `all` afterwards
  to record changes from then on);
- give it a page cache about twice the size the file will reach;
- write in transactions of some thousands of rows, then `sync()` or close.

The cache matters more than the log. A flush writes each changed page twice, once
to the journal and once in place, and happens whenever half the cache holds
changes. With a small cache and writes spread over the whole graph, the same pages
are changed and written again flush after flush; once half the cache holds what the
load changes, each page is written about twice in all.

Loading 1,000,000 nodes and 2,000,000 relationships between random pairs of them,
through the Zig API in transactions of 10,000 (release build, a busy macOS machine,
two runs each; the Zig API's default cache here is 28 MB, the bindings' 100 MB):

| Log | Changefeed | Cache | Time | Written to disk | File |
|-----|------------|-------|------|-----------------|------|
| on, `normal` sync | `all` | default | 75–76 s | 66 GB | 540 MB |
| off | `all` | default | 39–41 s | 33 GB | 540 MB |
| off | `off` | default | 36–38 s | 33 GB | 378 MB |
| on, `normal` sync | `all` | 256 MB | 27–29 s | 20 GB | 540 MB |
| off | `all` | 256 MB | 21–23 s | 13 GB | 540 MB |
| on, `normal` sync | `all` | 1 GB | 22–25 s | 13 GB | 540 MB |
| on, `normal` sync | `off` | 1 GB | 16–17 s | 10 GB | 378 MB |
| off | `all` | 1 GB | 13–15 s | 2.1 GB | 540 MB |
| off | `off` | 1 GB | 9–10 s | 1.1 GB | 378 MB |

Peak memory was the cache plus 25 to 80 MB. With the log off, the
changefeed off and a 1 GB cache, transactions cost no more than the same load
written directly with a `null` transaction (10–11 s), and they still roll back.

## Checkpointing

The log grows as you write. A checkpoint folds it back into the database file and
resets it, which is what stops it growing forever.

This happens on its own, after a commit, once the log holds enough frames. Each
write transaction adds at least one frame, so with the defaults that is roughly
every thousand write transactions. Open readers do not hold it back. It is skipped
only while an open transaction already has records in the log (one whose commit
failed part way and has not been rolled back yet), and the next commit tries again.

The automatic checkpoint runs inside the commit that triggers it, not in the
background, so that one commit takes longer. Measured through the C API on macOS
(20,000 single-node commits, default settings, release build, a busy machine), most
commits took about 50 µs and the ones that ran a checkpoint, about every
thousandth, took 5 to 12 ms and up to 27 ms. If a pause of that size matters to
you, turn the automatic checkpoint off from Zig and run checkpoints at moments you
choose.

`auto_checkpoint` decides when (it is set through the Zig API; the C API, the
bindings and the CLI always use the defaults below):

| Setting | Default | What it does |
|---------|---------|--------------|
| `max_wal_frames` | 1000 | Frames written before a checkpoint is considered. |
| `min_interval_ns` | 0 | Shortest gap between two checkpoints. |
| `mode` | `truncate` | Only `truncate` resets the log, so it is the only mode that bounds its size. |

From Zig, set `auto_checkpoint` to null to manage this yourself, which is worth doing if you
want to choose the moment rather than have it land mid-request. That also turns off
the checkpoints at open and close described below.

The minimum interval defaults to zero deliberately. Under `truncate` the frame
threshold already limits the rate, because a checkpoint resets the frame count and
the next one cannot happen until another thousand frames are written. A time gate
on top of that does not prevent thrash; it just lets the log grow without limit
during a burst of writes, which is the thing this is meant to stop.

A checkpoint also runs when a database is opened, after recovery, and when it is
closed, through any API or the CLI, if the log holds changes since its last
checkpoint. A cleanly closed database therefore leaves no journal and nothing in the
log to replay. A log that ends at a checkpoint is left as it is, which matters for
replication: see [Backup and Replication](../guides/backup-and-replication.md).

## Checkpointing by hand

```bash
lattice checkpoint social.lattice
```

You mostly do not need this. It is worth running before copying a database file,
so the copy is complete on its own, and on a database that has been open a very
long time under heavy writes if you would rather pick the moment.

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
this way. `lattice check` reads pages directly and does not recover first, so it
reports an interrupted flush as an error.

## Where to go next

- [Transactions and Durability](../guides/transactions.md) — what a transaction
  guarantees and the one-writer rule
- [Backup and Replication](../guides/backup-and-replication.md) — keeping a copy
  that is actually usable
- [Write-Ahead Log](../architecture/wal.md) — the format and the recovery path
