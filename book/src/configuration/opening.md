# Opening a Database

Almost everything you can configure is decided when you open a database. This
page is the reference for those options: what each one does, when to change it,
and what it costs you to get wrong.

```python
db = latticedb.Database(
    "graph.lattice",
    create=True,
    enable_vectors=True,
    vector_dimensions=1536,
)
db.open()  # or use it as a context manager: with latticedb.Database(...) as db:
```

## Decided at creation, and permanent

Two options are baked into the file the first time it is created, and cannot be
changed later without rebuilding the database.

| Option | What it does |
|--------|--------------|
| `page_size` | Size of a page in bytes, 4096 to 65535. The default of 4096 matches almost every filesystem and is the right answer unless you have measured otherwise. |
| `vector_dimensions` | How many numbers are in each vector, 1 to 4096. This has to match whatever produces your embeddings. |

Getting `vector_dimensions` wrong is the one that bites, because it is only
discovered when the first vector is rejected. OpenAI's `text-embedding-3-small`
produces 1536 numbers, so that is the number you would pass for it.

## Which features exist

| Option | Default | What it does |
|--------|---------|--------------|
| `create` | off | Create the database if it is not there. Ignored for `:memory:`, which always creates. |
| `read_only` | off | Open without the ability to write. Takes a shared lock, so several readers can share a database nobody is writing. |
| `enable_vectors` | off | Turn on vector storage and the HNSW index. |
| `enable_fts` | on | Turn on full-text search. |
| `enable_wal` | on | Write-ahead logging. See below before turning this off. |
| `enable_adjacency_cache` | off | Keep an in-memory map of which nodes connect to which, which speeds up traversal at the cost of memory. |

Turning a feature off is not just a performance choice: it decides what is stored
in the file. A database created without vector support has no vector index, and
enabling it later means creating a new database and moving the data.

## Memory and caching

| Option | Default | What it does |
|--------|---------|--------------|
| `cache_size_mb` | sized automatically | How much memory to hold pages in. |
| `enable_query_cache` | on | Cache parsed queries so repeating one does not re-parse it. |
| `query_cache_size` | 128 | How many parsed queries to keep. |

Left alone, the page cache sizes itself from the features you turned on: 16 MB
for the graph, plus 12 MB each for full-text and vector search. So a database
with both enabled reserves about 40 MB.

Set `cache_size_mb` when you know better than that — a machine with little memory,
or a working set you have actually measured. The environment variable
`LATTICE_BUFFER_POOL_MB` overrides both, which is useful for trying a value
without changing code.

In-memory databases ignore all of this and use a small fixed cache, because a
cache miss against memory is a copy rather than a trip to a disk. See
[Storage Modes](./storage-modes.md).

## Safety

| Option | Default | What it does |
|--------|---------|--------------|
| `lock` | on | Take a lock on the file so two processes cannot tread on each other. |

A database can only be open in one process at a time. Opening takes a lock: a
read-write handle takes it exclusively and a read-only handle shares it, so
opening returns an error rather than waiting.

Turn `lock` off only where locking does not work, such as some network
filesystems. It does not make concurrent access safe; it removes the thing that
was going to tell you it was not. Go phrases this as `DisableLock` because a Go
`bool` cannot tell an omitted field from a deliberate `false`, and locking has to
stay on when the caller says nothing.

## Durability

`enable_wal` looks like a performance knob and is not. See
[Durability and the Log](./durability.md) — the short version is that
transactions are built on the log, so a database without one has no `BEGIN`, no
rollback, and no multi-statement atomicity.

`auto_checkpoint` controls how often the log is folded back into the database
file. The default is sensible and the same page explains when it is not.

## The same options, per language

```python
db = latticedb.Database("graph.lattice", create=True, cache_size_mb=64)
db.open()
```

```typescript
const db = new Database('graph.lattice', { create: true, cacheSizeMb: 64 });
```

```go
db, err := latticedb.Open("graph.lattice", latticedb.OpenOptions{
    Create:      true,
    CacheSizeMB: 64,
})
```

```c
lattice_open_options_v4 options = LATTICE_OPEN_OPTIONS_V4_DEFAULT;
options.create = true;
options.cache_size_mb = 64;

lattice_database* db;
lattice_open_v4("graph.lattice", &options, &db);
```

The C API grows a new options struct rather than changing the old one, so a
program compiled against an earlier version keeps working. Always start from the
matching `_DEFAULT` macro rather than zeroing the struct yourself: a zeroed
struct asks for no locking, which is the opposite of what you want.

## Where to go next

- [Storage Modes](./storage-modes.md) — a file, memory, or a block of bytes
- [Durability and the Log](./durability.md) — what survives what
- [Performance Tuning](../guides/performance-tuning.md) — once it works and you
  want it faster
