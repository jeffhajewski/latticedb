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
`vector_metric`, `vector_m`, `vector_ef_construction`, `enable_wal`,
`enable_adjacency_cache`, `lock`, `sync_mode`, `full_fsync` and
`changefeed_retention`; the C API, Go and Java also take `page_size` (Python and
TypeScript always use 4096). In the C API the vector, durability and changefeed
settings after `lock` are fields of `lattice_open_options_v5`. `enable_fts` and
the query-cache settings are options of the Zig API only — everywhere else they
keep their defaults. `auto_checkpoint` is an open option in Zig only; everywhere
else it is set on the open database, or through `LATTICE_AUTO_CHECKPOINT_FRAMES`
(see [Checkpointing](./durability.md#checkpointing)).

## Decided at creation, and permanent

| Option | What it does |
|--------|--------------|
| `page_size` | Size of a page in bytes, 4096 to 65535. Fixed when the file is created and stored in it. The default of 4096 matches almost every filesystem and is the right answer unless you have measured otherwise. |
| `vector_dimensions` | How many numbers are in each vector, 1 to 4096 (default 128). This has to match whatever produces your embeddings. |
| `vector_metric` | How the vector index measures distance: `cosine` (the default), `euclidean`, or `dot_product`. See below. |
| `vector_m` | HNSW connections per node, 2 to 32 (default 16; layer 0 keeps twice as many). More connections give better recall for more memory and slower inserts. |
| `vector_ef_construction` | HNSW candidate list size while building, 1 to 4096 (default 200). Larger builds a better graph, more slowly. |

The vector settings take effect when an open creates the vector index (with
`enable_vectors` on a database that has none), and the file records them. A
database that has a vector index opens with it whatever the options say, so
reopening it needs no vector options at all, after a crash included. Any vector
setting you do pass must match what the file records, or the open fails with a
configuration-mismatch error (`LATTICE_ERROR_CONFIG_MISMATCH`): the settings cannot
be changed after creation. To change them, export the data and import it into a new
database. With vectors enabled, dimensions 0 means "whatever the file records" in
every binding (`vector_dimensions=0`, `vectorDimensions: 0`, `VectorDimensions: 0`,
`vectorDimensions(0)`); where the open would have to create an index, 0 is
refused as an invalid argument.

The metric decides what `<=>` and vector search return, smallest first:

| Metric | Distance | Vectors are stored |
|--------|----------|--------------------|
| `cosine` | 1 − cos(a, b), from 0 to 2 | scaled to unit length |
| `euclidean` | the L2 distance, √Σ(aᵢ − bᵢ)² | as given |
| `dot_product` | the negative dot product, −(a · b), so the largest product comes first | as given |

Getting `vector_dimensions` wrong is the one that bites, because it is only
discovered when the first vector is rejected. OpenAI's `text-embedding-3-small`
produces 1536 numbers, so that is the number you would pass for it.

0.16.0 refuses to open a file whose index uses `euclidean` or `dot_product`, or an
`m` other than 16, because it would misread it. A cosine index with the default `m`
does not keep 0.16.0 out, though other things do, the changefeed above all; see
[Opening with 0.16.0](../guides/changefeed.md#opening-with-0160).

## Which features exist

| Option | Default | What it does |
|--------|---------|--------------|
| `create` | off | Create the database if it is not there. Ignored for `:memory:`, which always creates. |
| `read_only` | off | Open without the ability to write. Takes a shared lock, so several readers can share a database nobody is writing. |
| `enable_vectors` | off | Create vector storage and the HNSW index if the database has none. A database that has one opens with it without this. |
| `enable_fts` | on | Turn on full-text search (Zig API only). |
| `enable_wal` | on | Write-ahead logging. See below before turning this off. |
| `enable_adjacency_cache` | off | Keep an in-memory map of which nodes connect to which. Queries and the C API's edge calls do not read it, so today it costs memory and a little work on each edge write and speeds up nothing reached through the bindings. |

A database created without vector support has no vector storage. Opening it later
with vectors enabled creates the vector storage then, so you can start adding
vectors to an existing database. The new index's configuration is written to the
file during that open.

## Memory and caching

| Option | Default | What it does |
|--------|---------|--------------|
| `cache_size_mb` | 100 MB (C API and bindings); 64 MB (CLI) | How much memory to hold pages in. |
| `enable_query_cache` | on | Cache parsed queries so repeating one does not re-parse it (Zig API only). |
| `query_cache_size` | 128 | How many parsed queries to keep (Zig API only). |

The C API and every binding default `cache_size_mb` to 100, and the CLI to 64
(`--cache-size`). Set it when you know better — a machine with little memory, or a
working set you have actually measured. A bulk load gains most from a large cache;
see [Bulk loading](./durability.md#bulk-loading).

Only the Zig API can leave the size unset (`buffer_pool_size = 0`). The cache then
sizes itself from the features you turned on: 16 MB for the graph, plus 12 MB each
for full-text and vector search, so about 40 MB with both.

The environment variable `LATTICE_BUFFER_POOL_MB` sets the size wherever the program
leaves it unset: `buffer_pool_size = 0` in Zig, `cache_size_mb = 0` in C, no cache
size in a binding, no `--cache-size` in the CLI. A size the program sets wins over
the variable, as with `LATTICE_SYNC_MODE` and `LATTICE_FULL_FSYNC`. The bindings
and the C API leave it unset by default, so the variable is useful for trying a
value without changing code; without it they use 100 MB.

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

## The changefeed

| Option | Default | What it does |
|--------|---------|--------------|
| `changefeed_retention` | the file's policy; `all` for a new file | How much of the changefeed (`__lattice_changes`) to keep: `all`, `last:<N>`, `age:<duration>` or `off`. Recorded in the file; a value given when opening for writing replaces the file's. Python `changefeed_retention`, TypeScript `changefeedRetention`, Go `ChangefeedRetention`, Java `changefeedRetention`, C `lattice_open_options_v5.changefeed_retention`, CLI `--changefeed`. `LATTICE_CHANGEFEED_RETENTION` sets it for a new database. |

Every committed change is recorded as an event, and by default every event is
kept, so the changefeed grows with the number of changes ever made. See
[The Changefeed](../guides/changefeed.md) for the policies, what each costs and
when to use which.

## Durability

`enable_wal = false` means only that nothing is logged. Transactions, rollback and
all-or-nothing queries work the same without the log; what you give up is that a
commit survives a crash only once its pages have been flushed to the file, at
`sync()` or close. See
[Running without the log](./durability.md#running-without-the-log) for exactly
what a crash leaves and what it saves.

| Option | Default | What it does |
|--------|---------|--------------|
| `enable_wal` | on | Write-ahead logging: a commit survives a crash as soon as it returns. Off, commits survive from the next flush. |
| `sync_mode` | `.full` | Whether a commit waits for its log record to reach the disk. `.normal` survives a killed process but can lose the latest commits to a power cut. When the program leaves it unset, `LATTICE_SYNC_MODE` applies. |
| `full_fsync` | off | On macOS, sync with `F_FULLFSYNC` so commits survive power loss, at several milliseconds per sync. When the program leaves it unset, `LATTICE_FULL_FSYNC` applies. |
| `auto_checkpoint` | every 1000 log frames, truncating | How often the log is folded back into the database file. An open option in Zig; elsewhere `lattice_set_auto_checkpoint`, each binding's `set_auto_checkpoint`/`setAutoCheckpoint`/`SetAutoCheckpoint`, `--auto-checkpoint` or `LATTICE_AUTO_CHECKPOINT_FRAMES`. |

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
lattice_open_options_v5 options = LATTICE_OPEN_OPTIONS_V5_DEFAULT;
options.create = true;
options.cache_size_mb = 64;

lattice_database* db;
lattice_open_v5("graph.lattice", &options, &db);
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
