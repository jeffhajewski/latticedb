# Durability and the Log

Every change is written to a log before it reaches the database file. This page is
about what that buys you, the two knobs that control it, and what happens if you
turn it off.

For what a transaction guarantees, see
[Transactions and Durability](../guides/transactions.md). For how the log is
structured, see [Write-Ahead Log](../architecture/wal.md).

## What committing means

1. The change is written to the log.
2. The log is flushed to disk.
3. Only then is the commit reported as successful.

The database file itself is updated later, when the page cache writes changed
pages back or a checkpoint runs. So a crash before that loses nothing: the log is
replayed on the next open and the change comes back.

This is why a commit is fast even though it is durable. Appending to a log is one
sequential write; updating the database file properly would mean several scattered
ones.

## Turning the log off is not a performance knob

`enable_wal` looks like a trade of safety for speed. It is not, and the reason is
worth knowing before you reach for it.

**Transactions are built on the log.** Without one, `beginTransaction` returns
`TransactionsNotEnabled`. No `BEGIN`, no rollback, no multi-statement atomicity.
Not slower — absent.

Single queries still work, because a writing query without a transaction falls
back to the older behaviour rather than being refused. But you have given up the
ability to make two changes succeed or fail together, which is usually the reason
someone wanted a database rather than a file.

There is one case where turning it off is reasonable: bulk-loading a database you
are about to serialize or discard, where nothing needs to survive a crash and
nothing needs to be atomic. Even then, measure first.

## Checkpointing

The log grows as you write. A checkpoint folds it back into the database file and
resets it, which is what stops it growing forever.

This happens on its own, after a commit, once no transaction is open.
`auto_checkpoint` decides when (it is set through the Zig API; the C API, the
bindings and the CLI always use the defaults below):

| Setting | Default | What it does |
|---------|---------|--------------|
| `max_wal_frames` | 1000 | Frames written before a checkpoint is considered. |
| `min_interval_ns` | 0 | Shortest gap between two checkpoints. |
| `mode` | `truncate` | Only `truncate` resets the log, so it is the only mode that bounds its size. |

From Zig, set `auto_checkpoint` to null to manage this yourself, which is worth doing if you
want to choose the moment rather than have it land mid-request.

The minimum interval defaults to zero deliberately. Under `truncate` the frame
threshold already limits the rate, because a checkpoint resets the frame count and
the next one cannot happen until another thousand frames are written. A time gate
on top of that does not prevent thrash; it just lets the log grow without limit
during a burst of writes, which is the thing this is meant to stop.

## Checkpointing by hand

```bash
lattice checkpoint social.lattice
```

You mostly do not need this. It is worth running before copying a database file,
so the copy is complete on its own, and on a database that has been open a very
long time under heavy writes if you would rather pick the moment.

This is not `compact`. Checkpointing shrinks the log, not the database file.

Closing a database through the C API or a binding also syncs it and runs a
`truncate` checkpoint, so a cleanly closed database leaves no log to replay.

## What survives what

| Event | What happens |
|-------|--------------|
| Process crashes | Committed changes are replayed from the log on the next open. |
| Machine loses power | The same, as far as the disk honoured the flush. |
| Database file is copied while open | Broken. The log holds committed changes the file does not have. Use `backup`. |
| Log file is deleted while closed | Committed changes not yet checkpointed are gone. |
| An in-memory database's process exits | Everything is gone. That is what in-memory means. |

The third row is the one that catches people, and it fails silently at copy time —
you find out at restore. [Backup and Replication](../guides/backup-and-replication.md)
covers the tools that do it correctly.

## Where to go next

- [Transactions and Durability](../guides/transactions.md) — what a transaction
  guarantees and the one-writer rule
- [Backup and Replication](../guides/backup-and-replication.md) — keeping a copy
  that is actually usable
- [Write-Ahead Log](../architecture/wal.md) — the format and the recovery path
