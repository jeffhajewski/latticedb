# Storage Modes

A database can live in three places, and the choice is made by what you pass as
the path.

| You want | Open |
|----------|------|
| A database on disk | a path: `graph.lattice` |
| A database that never touches the disk | `:memory:` |
| A database you already have as bytes | `deserialize(blob)` |

Everything else behaves identically. The query language, transactions, the
write-ahead log, indexes, and serialization all work the same way, because the
engine reaches its storage through an interface and only the implementation
changes. That sameness is deliberate and worth relying on.

## A file

```python
db = latticedb.Database("graph.lattice", create=True)
db.open()  # or use it as a context manager: with latticedb.Database(...) as db:
```

The normal case. One file holds everything: nodes, edges, properties, indexes, and
vectors. Beside it, while the database is open, are the write-ahead log
(`-wal`) and, when it is open for writing, the flush journal (`-journal`); see
[Durability and the Log](./durability.md#files-next-to-the-database).

**Do not copy the file while the database is open.** The log holds committed
changes that are not in the main file yet, so copying only the main file gives you
a database that opens and then fails on query, and copying both catches them at
different instants. Use `backup`, or stop the process first. See
[Backup and Replication](../guides/backup-and-replication.md).

## Memory

```python
db = latticedb.Database(":memory:")
db.open()
```

```typescript
const db = new Database(':memory:');
```

```go
db, err := latticedb.Open(":memory:", latticedb.OpenOptions{})
```

```bash
lattice exec :memory: --query="CREATE (n:Note {t: 'scratch'}) RETURN n"
```

Nothing is written to disk and nothing survives closing the handle. Useful for a
scratch database, for tests, for a read-only filesystem, and for anywhere writing
somebody's data to local disk would be awkward to explain.

Opening `:memory:` implies creating it, since there is never a previous in-memory
database to find.

Three differences from a file-backed database, all deliberate:

- **It disappears when closed.** If you want to keep it, `serialize` it first.
- **Nothing locks it.** No other process can reach it, so there is nothing to
  exclude. `--no-lock` means nothing here.
- **The page cache is small and fixed.** A cache exists to keep pages off a disk,
  and a miss against memory is a copy from one part of RAM to another. Measured on
  a fourteen megabyte database, a 256 KB cache matched a 32 MB one for speed. So
  peak memory stays close to the size of the database: about 360 KB for a hundred
  kilobyte database, against the 100 MB cache a file-backed one opened through a
  binding reserves by default.

The write-ahead log stays on unless you turn it off, as a second file inside the
same memory. Turning it off saves its allocations and nothing else changes:
transactions work without it, and there was nothing durable to lose. See
[In-Memory Databases](./in-memory.md#the-write-ahead-log).

## Bytes

```python
blob = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
db = latticedb.deserialize(blob)

db.query("CREATE (n:Note {text: 'found something'})")

s3.put_object(Bucket=bucket, Key=key, Body=db.serialize(), IfMatch=etag)
```

`serialize` hands back the whole database as bytes, and `deserialize` opens one
from them. The result runs in memory, so this is the memory mode with a starting
point.

This is what makes it practical to keep a database per case, per tenant, or per
document in object storage. It is cheap because a database here is one file, so
serializing it is reading that file — there is no container format to maintain.

The bytes are a database file. Write them anywhere and they open, which means you
can always inspect one by hand.

### Loading without a second copy

By default the bytes are copied. Tell `deserialize` not to and it points at your
buffer instead, which halves what loading costs:

```python
db = latticedb.deserialize(blob, copy=False)
```

Each page becomes a copy of its own the first time something writes to it, so
reading a database and editing a little of it keeps one copy of nearly all of it.
Your buffer is never modified, and the database holds a reference to it for as
long as it is open.

Go and Java do not offer this, and that is a language rule rather than a gap in
those bindings. Go's own documentation says C code may not keep a pointer into the
Go heap after a call returns, and pinning a Java array for the lifetime of a
database would hold up the collector for exactly that long.

### Two workers, one blob

The failure this pattern invites is not in the database. If two workers read the
same object, change it, and write it back, the second silently erases the first.
That is what `IfMatch` is doing above: every major provider supports a conditional
write, and the write fails instead of destroying the other worker's changes.

## Where to go next

- [Opening a Database](./opening.md) — the rest of the options
- [Backup and Replication](../guides/backup-and-replication.md) — keeping a copy
  somewhere else
- [Portable Databases](../architecture/portable-databases.md) — how this works
  underneath, and why
