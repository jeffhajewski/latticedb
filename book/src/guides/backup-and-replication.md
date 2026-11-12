# Backup and Replication

A database on one disk is one disk failure away from gone. This guide covers the
three things LatticeDB gives you to prevent that: taking a copy, keeping a copy
continuously up to date, and getting a database back from one.

If you have used [Litestream](https://litestream.io) with SQLite, the shape here
will be familiar. The goal is the same: one machine, one writer, a few minutes of
tolerable downtime, and no desire to lose the last hour of work when the disk
dies.

## The rule that comes first

**Do not back up a database by copying the file while it is open.**

This is the mistake that costs people their data, and it costs them quietly. The
write-ahead log holds committed changes that are not in the main file yet, so:

- Copying only the main file gives you a database that opens and then fails when
  you query it.
- Copying both files catches them at two different instants and gives you a pair
  that fails on open.

Neither announces itself at copy time. You find out when you need the backup,
which is the worst possible moment. Use one of the commands below, or stop the
process first.

## Taking one copy

`backup` copies a running database to another file:

```bash
lattice backup social.lattice --file=/backups/social-2026-08-25.lattice
```

From Zig code, the same thing is a method on the database. The C API and the
language bindings do not expose backup or replication; from Python, TypeScript,
Go or Java, use the `lattice` command while nothing else has the database open.

```zig
const stats = try db.backup("/backups/social-2026-08-25.lattice");
```

Pending writes are folded into the file before the copy starts, so what you get
is a complete database on its own. It needs no log beside it, and you can open it
directly.

Two things to know. The copy captures the database as of the moment it starts, so
writes that land while it runs are not included. And it refuses to run while a
transaction is open, because a copy taken while writes land underneath it is torn
in ways no later check would catch.

This is the right tool for a nightly snapshot. It is the wrong tool for "I do not
want to lose the last hour", because an hour is exactly how much you lose.

## Keeping a copy up to date

`replicate` ships changes into a directory. The first pass copies the whole
database; later passes copy the write-ahead log frames written since, when the
log still holds them (see [Generations](#generations)):

```bash
lattice replicate social.lattice --to=/mnt/backup/social
```

Run it on a timer, from cron or a scheduler, and the destination trails your
database by roughly that interval. A pass with nothing to ship is normal, says
so, and exits successfully, so this does not fill your logs with things that
look like failures.

`--follow` keeps the command running and replicating on an interval:

```bash
lattice replicate social.lattice --to=/mnt/backup/social --follow --interval=30
```

While it runs it holds the database's file lock, so nothing else can open the
database, and with no other writer its later passes have nothing to ship. For a
database that other processes write to, use a timer instead.

### Why later passes are cheap

Every change is written to the write-ahead log before it reaches the database
file. Replication copies frames out of that log, so a pass has to move only what
was actually written, not the whole database again. That holds while the log
still has those frames, which is the case for an application that keeps the
database open and replicates through its own handle (below).

### Generations

Every so often LatticeDB folds pending changes into the database file and starts
the log over. By default this happens once the log reaches a thousand frames, and
when a database that has been written to closes.

When the log starts over, frame numbering restarts from zero. Frame 40 before the
reset and frame 40 after it are completely different changes, so replication
cannot simply keep counting. Instead it starts a **generation**: a fresh full
snapshot, followed by the changes that came after it.

A process that writes and then closes the database therefore leaves a reset log
behind, and the next pass starts a new generation. When your writers are
separate short-lived processes, such as `lattice exec` or a script, each pass
after a change is a full snapshot, and a pass with nothing changed ships
nothing. Frames are shipped incrementally only when the process doing the
writing also does the replicating.

You will see this in the destination:

```text
/mnt/backup/social/manifest.json
/mnt/backup/social/gen-0000000001/snapshot.lattice
/mnt/backup/social/gen-0000000001/frames/0000000012-0000000016.frames
/mnt/backup/social/gen-0000000002/snapshot.lattice
```

(The `frames/` files come from passes made through `replicateTo`; passes made
by the command against a closed database add only `gen-` snapshots.)

Older generations are kept rather than cleaned up, because restoring to a moment
inside one still needs its frames. If you want to reclaim the space, deleting a
whole `gen-` directory costs you the ability to restore to any moment inside it
and nothing else.

### Replicating a database your application is using

`lattice replicate` opens the database, and a database can only be open in one
process at a time. Point it at a database your application has open and it will
refuse to start, rather than corrupt anything:

```text
Error: social.lattice is open in another process. Close it first, or pass
--no-lock if you are certain nothing is writing to it.
```

That makes the command right for a database nothing else is using. For the case
people most often want, which is replicating while an application runs, a Zig
application can call `replicateTo` on the handle it already has (the C API and
the language bindings do not offer it):

```zig
// Somewhere on a timer inside your application.
const stats = try db.replicateTo("/mnt/backup/social");
```

This lives on the database rather than in a separate process for a concrete
reason. A generation opens with a snapshot, a snapshot has to be taken with no
writes in flight, and only the handle that owns the database can arrange that.
Reading the log from another process is perfectly safe; taking the snapshot is
not.

Do not reach for `--no-lock` to work around this. It exists for filesystems where
locking does not work, and it does not make two processes safe — it only removes
the thing that was going to tell you they were not.

## Getting a database back

`restore` turns a replication directory back into a database:

```bash
lattice restore /mnt/backup/social --output=recovered.lattice
```

```text
Restored /mnt/backup/social to recovered.lattice
  Generation:      1
  Segments:        2
  Frames replayed: 20
  Bytes written:   73728
  Restored to:     2026-08-25T22:44:18Z
  Duration:        468 ms
```

It copies the snapshot, replays the changes shipped after it, and folds the
result into a single file. What you get back is a database you can open, copy, or
move, with nothing beside it that has to be kept together with it.

Restore will not write over an existing file unless you pass `--force`, because
the thing most likely to be sitting at the output path is a database somebody
still wants.

### Going back to an earlier moment

```bash
lattice restore /mnt/backup/social --output=recovered.lattice --at="2026-08-25T14:00:00Z"
```

Times are read as UTC, so a restore means the same thing wherever you run it. A
bare date works too and means midnight.

What you get is the state as of the last replication pass at or before the moment
you asked for, not the state at that exact instant. If you replicate every thirty
seconds, you can rewind to within thirty seconds. That is why the output reports
the moment it actually restored to instead of repeating the one you asked for:
those two are rarely the same, and the difference is the thing you need to know.

### Why restore reuses recovery

Restore does not have its own replay logic. It rebuilds a log from what was
shipped and then opens the database, which replays it exactly the way recovery
replays a log after a crash.

That is deliberate. Replaying log records is subtle, and a second implementation
would drift away from the real one over time. If recovery has a bug, restore
should have the same bug rather than a different one.

## Choosing a strategy

| What you want | What to use |
|---------------|-------------|
| A copy before an upgrade or a risky migration | `lattice backup` |
| A nightly snapshot | `lattice backup` from cron |
| To lose no more than a few seconds of work | `replicateTo` on a timer in your application (Zig only) |
| To keep restore points of a database written by separate processes | `lattice replicate` on a timer |
| To undo a bad migration or a bad delete | `lattice restore --at=...` |
| Many small databases kept in a bucket | `serialize` and `deserialize` |
| A database that never touches the disk | open `:memory:` |

## What this is not

This is backup and restore. It is not high availability, and it is not
clustering. There is no failover, no leader election, and no second writer. A
restored database is a database you open yourself, on purpose, after deciding
that you need it.

Replicating to object storage such as S3 is not supported yet. The destination
layout was designed with it in mind — whole files, written once, named rather
than modified, with a manifest listing them — so a directory you replicate to
today can be copied to a bucket by any tool you already trust.

## Keeping many small databases in object storage

Everything above assumes one database you want to protect. There is a different
shape of problem that comes up just as often: not one big graph, but many small
ones — a database per case, per tenant, per document — living in S3 or Azure Blob
Storage, pulled down when you need one and pushed back when you are done.

That works because a database here is a single file. Serializing one is reading
that file, and there is no container format to invent:

```python
blob = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
db = latticedb.deserialize(blob)

db.query("CREATE (n:Note {text: 'found something'})")

s3.put_object(Bucket=bucket, Key=key, Body=db.serialize(), IfMatch=etag)
```

The same pair exists in every binding: `serialize()` on the database returns
bytes, and `deserialize` (a package-level function in Python and TypeScript,
`latticedb.Deserialize` in Go, `Database.deserialize` in Java,
`lattice_serialize` and `lattice_deserialize` in C) opens a database from them.

Pending writes are folded in before the bytes are handed over, so what you upload
is a complete database that needs no log beside it. You can write those bytes to
a file and open it directly if you ever want to look at one by hand.

**The engine does no networking here, on purpose.** Your application already has
a storage client, credentials, and a retry policy somebody chose deliberately. A
database that reimplemented all of that would be doing a worse job of something
you already have, and it would have to hold your cloud credentials to do it.

### Never touching the disk

Pass `:memory:` as the path and the database has no files behind it at all:

```python
db = latticedb.Database(":memory:")
db.open()
```

```typescript
const db = new Database(':memory:');
await db.open();
```

```go
db, err := latticedb.Open(":memory:", latticedb.OpenOptions{})
```

```bash
lattice exec :memory: --query="CREATE (n:Note {t: 'scratch'})"
```

`deserialize` uses this too, so a database pulled out of a bucket never becomes a
file on the way in. That matters if you are running one of these per request, on
a read-only filesystem, or anywhere writing a temporary copy of somebody's data
to local disk would be awkward to explain.

An in-memory database behaves like any other. It has transactions, it has a
write-ahead log, and you can serialize it back out. The only differences are that
it disappears when you close it and that nothing locks it, since there is no
second process that could reach it.

It is also cheaper than you might expect for small databases. The page cache is
capped at 64 pages (256 KB at the default page size) whatever cache size you ask
for, because a miss against memory is only a copy. A hundred-kilobyte database
costs a few hundred kilobytes in memory, rather than the 100 MB cache a
file-backed one opened through the C API or a binding reserves by default. If you
are keeping many small databases open at once, that difference is most of what
you pay. See [In-Memory Databases](../configuration/in-memory.md) for
measurements.

### Loading without a second copy

By default `deserialize` copies your bytes, so the database exists twice while it
loads. Tell it not to and it points at your buffer instead:

```python
db = latticedb.deserialize(blob, copy=False)
```

```typescript
const db = deserialize(blob, {}, false);
```

Each page becomes a copy of its own the first time something writes to it, so a
database you read and edit lightly keeps one copy of nearly all of itself. Your
buffer is never modified, and the database holds a reference to it for as long as
it is open, so there is nothing for you to keep alive by hand.

Go and Java do not offer this. That is not an oversight in those bindings: Go's
rules say C code may not keep a pointer into the Go heap after a call returns,
and pinning a Java array for the lifetime of a database would hold up the garbage
collector for exactly that long. Both copy instead, which is correct and costs
one duplicate at load.

### Two workers, one blob

This pattern has a failure mode worth planning for, and it is not in the
database. If two workers read the same object, change it, and write it back, the
second one silently erases the first. Nothing reports an error, and nothing looks
wrong until somebody notices missing data.

That is what `IfMatch` is doing in the example above. Every major provider
supports some form of conditional write keyed on an entity tag or generation
number, and the write fails instead of destroying the other worker's changes.
What to do when it fails — retry, merge, queue — is a decision only your
application can make.

## Related reading

- [Transactions and Durability](./transactions.md) for what "committed" means
- [The `lattice` Command](../getting-started/cli.md) for every option these
  commands take
