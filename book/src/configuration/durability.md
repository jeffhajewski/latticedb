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
read-only open that finds a complete journal refuses to open (it may not write the
file to finish the flush); open the database read-write once.

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

`sync_mode` is a field of the Zig `DatabaseConfig`. Everywhere else (the C API, the
bindings, the CLI) set it through the environment, which the engine reads when a
database is opened and which overrides the field:

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
survive power loss on a Mac. From the C API, the bindings and the CLI:

```bash
LATTICE_FULL_FSYNC=1       # or true; 0 or false turns it off; other values are ignored
```

## Environment variables

The engine reads these when a database is opened, whatever API opened it. Each one
overrides the corresponding setting.

| Variable | Values | Default | Effect |
|----------|--------|---------|--------|
| `LATTICE_SYNC_MODE` | `full`, `normal` | unset (`full`) | Whether a commit waits for the disk; see [Sync mode](#sync-mode) |
| `LATTICE_FULL_FSYNC` | `1`/`true`, `0`/`false` | unset (off) | `F_FULLFSYNC` on macOS; see [Full fsync on macOS](#full-fsync-on-macos) |
| `LATTICE_BUFFER_POOL_MB` | a positive integer | unset | Page cache size; see [Opening a Database](./opening.md#memory-and-caching) |

## Turning the log off is not a performance knob

`enable_wal` looks like a trade of safety for speed. It is not, and the reason is
worth knowing before you reach for it.

**Transactions are built on the log.** Without one, `beginTransaction` returns
`TransactionsNotEnabled`. No `BEGIN`, no rollback, no multi-statement atomicity.
Not slower — absent.

Single queries still work, because a writing query without a transaction falls
back to the older behaviour rather than being refused. But you have given up the
ability to make two changes succeed or fail together, which is usually the reason
someone wanted a database rather than a file. Without a log, changes reach the file
only when pages are flushed: on `sync()`, at close, or early when half the cache is
dirty. Call `sync()` when you need them on disk.

There is one case where turning it off is reasonable: bulk-loading a database you
are about to serialize or discard, where nothing needs to survive a crash and
nothing needs to be atomic. Even then, measure first.

## Checkpointing

The log grows as you write. A checkpoint folds it back into the database file and
resets it, which is what stops it growing forever.

This happens on its own, after a commit, once the log holds enough frames. Each
write transaction adds at least one frame, so with the defaults that is roughly
every thousand write transactions. Open readers do not hold it back. It is skipped
only while an open transaction already has records in the log (one whose commit
failed part way and has not been rolled back yet), and the next commit tries again.
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

| Event | What happens |
|-------|--------------|
| Process crashes or is killed | Every commit that returned is there on the next open, in either sync mode. A commit that was running is either all there or not at all. |
| Machine loses power, `sync_mode = .full`, Linux | Every commit that returned is there, as far as the disk honoured the flush. |
| Machine loses power, `sync_mode = .full`, macOS | The same only with `full_fsync` on. Without it, nothing is guaranteed. |
| Machine loses power, `sync_mode = .normal` | Commits since the last checkpoint, flush or `sync()` may be lost, from the end only. |
| Database file is copied while open | Broken. The log holds committed changes the file does not have. Use `backup`. |
| The `-wal` or `-journal` file is deleted after a crash | Committed changes not yet in the file are gone, and an interrupted flush cannot be finished. Leave both in place. |
| An in-memory database's process exits | Everything is gone. That is what in-memory means. |

The copy row is the one that catches people, and it fails silently at copy time —
you find out at restore. [Backup and Replication](../guides/backup-and-replication.md)
covers the tools that do it correctly.

A read-only open does not read the log. After a crash it sees the database file as
of its last flush, which can be behind the commits the log holds; opening the
database read-write once recovers them.

## Where to go next

- [Transactions and Durability](../guides/transactions.md) — what a transaction
  guarantees and the one-writer rule
- [Backup and Replication](../guides/backup-and-replication.md) — keeping a copy
  that is actually usable
- [Write-Ahead Log](../architecture/wal.md) — the format and the recovery path
