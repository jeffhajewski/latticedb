# In-Memory Databases

Pass `:memory:` as the path and the database has no files behind it at all.

```python
db = latticedb.Database(":memory:")
db.open()  # or: with latticedb.Database(":memory:") as db:
```

```typescript
const db = new Database(':memory:');
```

```go
db, err := latticedb.Open(":memory:", latticedb.OpenOptions{})
```

```c
lattice_open_options_v4 options = LATTICE_OPEN_OPTIONS_V4_DEFAULT;
lattice_database* db;
lattice_open_v4(":memory:", &options, &db);
```

```bash
lattice exec :memory: --query="CREATE (n:Note {t: 'scratch'}) RETURN n"
```

Nothing is written to disk and nothing survives closing the handle. You do not
need to pass `create`: there is never a previous in-memory database to find, so
opening one always makes it.

## When you want this

- **Trying something out.** The fastest way to run a query against a real database
  with nothing to clean up afterwards.
- **Tests.** No temporary directories, no files left behind by a failed run, and
  no chance of two tests sharing a database by accident.
- **A database per request.** Pull one out of object storage, work on it, hand the
  bytes back, and never write somebody's data to local disk. See
  [Storage Modes](./storage-modes.md).
- **A read-only filesystem**, or anywhere a local file would be awkward to
  explain.

## It is a real database

Everything works: transactions, the write-ahead log, indexes, vector search,
full-text search, and serialization. The query language does not change, and
neither does anything you write against it.

That is not a coincidence. The engine reaches its storage through an interface,
and this swaps the implementation rather than adding a second path through the
engine. If something works against a file it works here.

```python
import latticedb

with latticedb.Database(":memory:") as db:
    with db.write() as txn:
        alice = txn.create_node(labels=["Person"], properties={"name": "Alice"})
        bob = txn.create_node(labels=["Person"], properties={"name": "Bob"})
        txn.create_edge(alice.id, bob.id, "KNOWS")
        txn.commit()

    rows = db.query("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name")
```

## Three things that differ

**It disappears when you close it.** If you want to keep it, serialize it first:

```python
blob = db.serialize()          # bytes you can write anywhere
db2 = latticedb.deserialize(blob)   # and open again later
```

**Nothing locks it.** A file-backed database refuses a second process, because two
writers would corrupt it. No other process can reach memory this one owns, so
there is nothing to exclude and every lock succeeds. `--no-lock` means nothing
here.

**The page cache is small and fixed.** A cache exists to keep pages off a disk, and
a miss against memory is a copy from one part of RAM to another. Measured on a
fourteen megabyte database, a 256 KB cache matched a 32 MB one for speed, so an
in-memory database uses a small one and stays close to the size of the data:

| Database | Held in memory |
|----------|---------------:|
| 100 KB | about 360 KB |
| 7 MB | about 7.7 MB |
| 18 MB | about 18.7 MB |

A file-backed database would reserve sixteen megabytes of cache regardless, which
is why holding many small in-memory databases at once is practical.

## Two in-memory databases are two databases

Each has its own storage, so a shared path name means nothing:

```python
a = latticedb.Database(":memory:")
b = latticedb.Database(":memory:")   # a completely separate database
a.open()
b.open()
```

There is no way to share one between handles, and no equivalent of SQLite's shared
cache. If two parts of your program need the same in-memory database, pass the
same handle.

## The write-ahead log stays on

Turning it off looks free, since a process holding the only copy of a database
loses everything when it dies anyway. It is not free: transactions are built on
the log, so a database without one has no `BEGIN`, no rollback, and no
multi-statement atomicity. See [Durability and the Log](./durability.md).

The log lives in the same memory as the database and is bounded by automatic
checkpointing, so it costs a few megabytes at most.

## Where to go next

- [Storage Modes](./storage-modes.md) — files, memory, and bytes side by side
- [Opening a Database](./opening.md) — the rest of the options
- [Backup and Replication](../guides/backup-and-replication.md) — keeping many
  small databases in object storage
- [Portable Databases](../architecture/portable-databases.md) — how this works
  underneath, and why it was built this way
