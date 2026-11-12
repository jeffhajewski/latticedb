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

Not every option below is available everywhere. The bindings and the C API expose
`create`, `read_only`, `cache_size_mb`, `enable_vectors`, `vector_dimensions`,
`enable_wal`, `enable_adjacency_cache` and `lock`; the C API, Go and Java also
take `page_size` (Python and TypeScript always use 4096). `enable_fts`, the query-cache settings and
`auto_checkpoint` are options of the Zig API only — everywhere else they keep
their defaults. `sync_mode` and `full_fsync` are Zig options too, and elsewhere are
set through the environment variables `LATTICE_SYNC_MODE` and `LATTICE_FULL_FSYNC`.

## Decided at creation, and permanent

| Option | What it does |
|--------|--------------|
| `page_size` | Size of a page in bytes, 4096 to 65535. Fixed when the file is created and stored in it. The default of 4096 matches almost every filesystem and is the right answer unless you have measured otherwise. |
| `vector_dimensions` | How many numbers are in each vector, 1 to 4096. This has to match whatever produces your embeddings. |

The file does not record `vector_dimensions`: it is taken from the options each
time you open with vectors enabled, so pass the same value every time. Opening a
database that already holds vectors with a different value is rejected with an
invalid-argument error.

Getting `vector_dimensions` wrong is the one that bites, because it is only
discovered when the first vector is rejected. OpenAI's `text-embedding-3-small`
produces 1536 numbers, so that is the number you would pass for it.

## Which features exist

| Option | Default | What it does |
|--------|---------|--------------|
| `create` | off | Create the database if it is not there. Ignored for `:memory:`, which always creates. |
| `read_only` | off | Open without the ability to write. Takes a shared lock, so several readers can share a database nobody is writing. |
| `enable_vectors` | off | Turn on vector storage and the HNSW index. |
| `enable_fts` | on | Turn on full-text search (Zig API only). |
| `enable_wal` | on | Write-ahead logging. See below before turning this off. |
| `enable_adjacency_cache` | off | Keep an in-memory map of which nodes connect to which. Queries and the C API's edge calls do not read it, so today it costs memory and a little work on each edge write and speeds up nothing reached through the bindings. |

A database created without vector support has no vector storage. Opening it later
with vectors enabled creates the vector storage then, so you can start adding
vectors to an existing database.

## Memory and caching

| Option | Default | What it does |
|--------|---------|--------------|
| `cache_size_mb` | 100 MB (C API and bindings); 64 MB (CLI) | How much memory to hold pages in. |
| `enable_query_cache` | on | Cache parsed queries so repeating one does not re-parse it (Zig API only). |
| `query_cache_size` | 128 | How many parsed queries to keep (Zig API only). |

The C API and every binding default `cache_size_mb` to 100, and the CLI to 64
(`--cache-size`). Set it when you know better — a machine with little memory, or a
working set you have actually measured.

Only the Zig API can leave the size unset (`buffer_pool_size = 0`). The cache then
sizes itself from the features you turned on: 16 MB for the graph, plus 12 MB each
for full-text and vector search, so about 40 MB with both.

The environment variable `LATTICE_BUFFER_POOL_MB` overrides all of these, including
an explicit `cache_size_mb`, which is useful for trying a value without changing
code.

In-memory databases cap the cache at 64 pages (256 KB at the default page size)
whatever you ask for, because a cache miss against memory is a copy rather than a
trip to a disk. See [Storage Modes](./storage-modes.md).

The size is the cache's resting size, not a hard ceiling. Changed pages are never
evicted to make room; they stay until the next flush writes them all together, and
the cache grows past its size if it has to hold them. The database flushes early
when half the cache holds changes, so this only shows during a single large
transaction or a long recovery.

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

| Option | Default | What it does |
|--------|---------|--------------|
| `enable_wal` | on | Write-ahead logging, and with it transactions. |
| `sync_mode` | `.full` | Whether a commit waits for its log record to reach the disk. `.normal` survives a killed process but can lose the latest commits to a power cut. Zig API, or `LATTICE_SYNC_MODE`. |
| `full_fsync` | off | On macOS, sync with `F_FULLFSYNC` so commits survive power loss, at several milliseconds per sync. Zig API, or `LATTICE_FULL_FSYNC=1`. |
| `auto_checkpoint` | every 1000 log frames, truncating | How often the log is folded back into the database file. Zig API only. |

[Durability and the Log](./durability.md) explains each of these and what survives
what.

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
