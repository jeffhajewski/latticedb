# C API

The C API is LatticeDB's primary interface. All language bindings (Python, TypeScript, Go, and Java through a JNI bridge) wrap this API. The header file is `include/lattice.h`.

## Overview

The API uses opaque handle types and follows a consistent pattern:
- Functions return `lattice_error` (0 = success, negative = error)
- Resources are allocated by the library and freed by the caller
- Strings and result sets must be explicitly freed

## Types

### Handles

```c
typedef struct lattice_database lattice_database;
typedef struct lattice_txn lattice_txn;
typedef struct lattice_query lattice_query;
typedef struct lattice_result lattice_result;
typedef struct lattice_vector_result lattice_vector_result;
typedef struct lattice_fts_result lattice_fts_result;
typedef struct lattice_edge_result lattice_edge_result;
typedef struct lattice_stream_batch lattice_stream_batch;
```

A handle is an opaque token, not the address of anything: pass it back to the
API, but never dereference or reinterpret it. Each handle gets a token that is
not reused (until a sequence of 2^53 handles wraps on 64-bit targets), so a
handle used after it was freed is rejected rather than reaching whatever the
allocator put in its place. A transaction handle used after commit or rollback
returns `LATTICE_ERROR_TXN_ABORTED`, also after the database is closed; any other
freed handle returns `LATTICE_ERROR_INVALID_ARG`.

Every call that reaches a database takes that database's lock, so a database
handle and its transactions can be called from several threads; the calls take
turns. That includes database-level calls such as `lattice_vector_search`,
`lattice_get_nodes_by_label`, the stream reads and the query cache calls, and a
handle stays valid for the whole of a call even if another thread commits or
closes it meanwhile. A waiting `lattice_stream_read` releases the lock while it
waits, so writers can commit what it is waiting for. Only one read-write
transaction can be open at a time (see below).

Enum arguments are checked: a value outside the declared set is rejected with
`LATTICE_ERROR_INVALID_ARG` (`lattice_begin` with an unknown mode,
`lattice_embedding_client_create` with an unknown format, a value of unknown
type), and `lattice_error_message` returns `"Unknown error"` for a code it does
not know.

### IDs

```c
typedef uint64_t lattice_node_id;
typedef uint64_t lattice_edge_id;
```

### Error Codes

```c
LATTICE_OK                  // 0  - Success
LATTICE_ERROR               // -1 - Generic error
LATTICE_ERROR_IO            // -2 - I/O error
LATTICE_ERROR_CORRUPTION    // -3 - Data corruption detected
LATTICE_ERROR_NOT_FOUND     // -4 - Resource not found
LATTICE_ERROR_ALREADY_EXISTS // -5 - Resource already exists
LATTICE_ERROR_INVALID_ARG   // -6 - Invalid argument
LATTICE_ERROR_TXN_ABORTED   // -7 - Transaction aborted
LATTICE_ERROR_LOCK_TIMEOUT  // -8 - Lock timeout
LATTICE_ERROR_READ_ONLY     // -9 - Write attempted on read-only txn
LATTICE_ERROR_FULL          // -10 - Database full
LATTICE_ERROR_VERSION_MISMATCH // -11 - Version mismatch
LATTICE_ERROR_CHECKSUM      // -12 - Checksum verification failed
LATTICE_ERROR_OUT_OF_MEMORY // -13 - Out of memory
LATTICE_ERROR_UNSUPPORTED   // -14 - Unsupported operation or value type
LATTICE_ERROR_VALUE_TOO_LARGE // -15 - Value exceeds engine storage limits
LATTICE_ERROR_DATABASE_LOCKED // -16 - Database is open in another process
LATTICE_ERROR_HAS_RELATIONSHIPS // -17 - Node still has relationships
LATTICE_ERROR_CONFIG_MISMATCH // -18 - Open options contradict the file's vector index
LATTICE_ERROR_NEEDS_NEWER_WRITER // -19 - File may be written only by a newer release
```

Some codes come from more than one place:

| Code | Returned by |
|------|-------------|
| `LATTICE_ERROR_VERSION_MISMATCH` | An open, read-only included, of a file or log in a format a newer release wrote |
| `LATTICE_ERROR_LOCK_TIMEOUT` | Also `lattice_checkpoint` while a transaction already has records in the log |
| `LATTICE_ERROR_CORRUPTION` | An open whose log holds damage in frames that had been synced, or a committed change that cannot be replayed |
| `LATTICE_ERROR_NOT_FOUND` | `lattice_edge_create` when either end does not exist; a query that creates a relationship to a node deleted earlier in it |
| `LATTICE_ERROR_FULL` | A read-only open whose crash recovery needs more memory than it is allowed (see [Read-only opens after a crash](#read-only-opens-after-a-crash)) |
| `LATTICE_ERROR_HAS_RELATIONSHIPS` | `lattice_node_delete`, and a query whose `DELETE` names a node that still has relationships |
| `LATTICE_ERROR_CONFIG_MISMATCH` | `lattice_open*` with vector options that contradict the index the file records |
| `LATTICE_ERROR_NEEDS_NEWER_WRITER` | A read-write open of a file whose header says only a newer release may write it; it still opens read-only |
| `LATTICE_ERROR_UNSUPPORTED` | A lookup or search in an index that does not exist: an undeclared property or full-text index, or, on a database created without a vector index, `lattice_node_set_vector`, `lattice_batch_insert` and vector search (`LATTICE_ERROR_IO` before 0.18.1) |
| `LATTICE_ERROR_IO`, `LATTICE_ERROR_OUT_OF_MEMORY` | Also `lattice_query_execute`, when the storage or an allocation failed under the query rather than the query itself |

### Value Types

```c
typedef enum {
    LATTICE_VALUE_NULL = 0,
    LATTICE_VALUE_BOOL = 1,
    LATTICE_VALUE_INT = 2,
    LATTICE_VALUE_FLOAT = 3,
    LATTICE_VALUE_STRING = 4,
    LATTICE_VALUE_BYTES = 5,
    LATTICE_VALUE_VECTOR = 6,
    LATTICE_VALUE_LIST = 7,
    LATTICE_VALUE_MAP = 8
} lattice_value_type;
```

### Property Value

```c
typedef struct {
    lattice_value_type type;
    union {
        bool bool_val;
        int64_t int_val;
        double float_val;
        struct { const char* ptr; size_t len; } string_val;
        struct { const uint8_t* ptr; size_t len; } bytes_val;
        struct { const float* ptr; uint32_t dimensions; } vector_val;
    } data;
} lattice_value;
```

## Database Operations

### Open

```c
lattice_open_options_v5 options = LATTICE_OPEN_OPTIONS_V5_DEFAULT;
options.create = true;
options.enable_vector = true;
options.vector_dimensions = 128;

lattice_database* db;
lattice_error err = lattice_open_v5("mydb.ltdb", &options, &db);
```

`lattice_open_v5` is the newest open function; the older ones (`lattice_open`
through `lattice_open_v4`, below) still work and behave as it does with the v5
fields left at 0.

### Close

```c
lattice_close(db);
```

Close refuses with `LATTICE_ERROR_INVALID_ARG` while a transaction or result
handle that depends on the database is still open, and the database stays open
so you can finish them and close again. A `lattice_stream_read` waiting on the
database in another thread does not hold the close up: it is woken and returns
`LATTICE_ERROR_INVALID_ARG` before the database is freed. Once close starts, it
always consumes the handle; `LATTICE_ERROR_IO` reports that the final sync or
checkpoint failed, and the handle must not be used again.

### Open Options

```c
typedef struct {
    bool create;              // Create if not exists
    bool read_only;           // Open in read-only mode
    uint32_t cache_size_mb;   // Cache size in MB (default: 100; 0: not set)
    uint32_t page_size;       // Page size in bytes (default: 4096)
    bool enable_vector;       // Create a vector index if the database has none
    uint16_t vector_dimensions; // Vector dimensions, 1..4096 (default: 128)
} lattice_open_options;
```

A `cache_size_mb` set in any options version wins over the
`LATTICE_BUFFER_POOL_MB` environment variable. 0 means "not set": the
variable's size if it is set, else 100 MB.

### Newer Open Options

`lattice_open_options` cannot grow without breaking every program compiled
against the old size, so newer options come as new structs. Each starts with its
own size, which is how the library knows which version you compiled against.

```c
lattice_open_options_v5 options = LATTICE_OPEN_OPTIONS_V5_DEFAULT;
options.create = true;
options.enable_vector = true;
options.vector_dimensions = 1536;
options.vector_metric = LATTICE_VECTOR_METRIC_EUCLIDEAN;
options.sync_mode = LATTICE_SYNC_MODE_NORMAL;

lattice_database* db;
lattice_open_v5("graph.lattice", &options, &db);
```

Use `lattice_open_v2` with `lattice_open_options_v2`, which adds `enable_wal` to
the original set. With `enable_wal` false nothing is logged, and everything else
works as with the log: read-write transactions, rollback, streams and the
changefeed. A commit then reaches the file at the next flush rather than when it
returns; see
[Running without the log](../configuration/durability.md#running-without-the-log).
`lattice_open_options_v3` adds `enable_adjacency_cache`, which keeps an
in-memory map of which nodes connect to which. Nothing behind the C API reads
that map, so it speeds up nothing; leave it off.
`lattice_open_options_v4` adds `lock`, which defaults to true and is described
below.
`lattice_open_options_v5` adds the durability settings and the vector index's
configuration, described in the next two sections. Each new field is 0 for "not
set", and a value outside its range makes the open return
`LATTICE_ERROR_INVALID_ARG`.

| Field | Values | When 0 |
|-------|--------|--------|
| `sync_mode` | `LATTICE_SYNC_MODE_FULL` (1), `LATTICE_SYNC_MODE_NORMAL` (2) | `LATTICE_SYNC_MODE` from the environment, else full |
| `full_fsync` | `LATTICE_FULL_FSYNC_OFF` (1), `LATTICE_FULL_FSYNC_ON` (2) | `LATTICE_FULL_FSYNC` from the environment, else off |
| `vector_metric` | `LATTICE_VECTOR_METRIC_COSINE` (1), `_EUCLIDEAN` (2), `_DOT_PRODUCT` (3) | The file's metric, or cosine for a new index |
| `vector_m` | 2..32, HNSW connections per node (layer 0 keeps twice as many) | The file's, or 16 for a new index |
| `vector_ef_construction` | 1..4096, HNSW candidate list size while building | The file's, or 200 for a new index |
| `changefeed_retention` | `LATTICE_CHANGEFEED_RETENTION_ALL` (1), `_LAST` (2), `_AGE` (3), `_OFF` (4); see [the changefeed](#the-changefeed) | The file's policy, or `LATTICE_CHANGEFEED_RETENTION` from the environment for a new file, else all |
| `changefeed_retention_value` | Events for `_LAST`, milliseconds for `_AGE`, at least 1 | Must be set for `_LAST` and `_AGE` |

`lattice_deserialize` and `lattice_deserialize_borrowed` take the v4 options;
`lattice_deserialize_v5` and `lattice_deserialize_borrowed_v5` take these, and
check them against the index the bytes record as an open checks a file.

### Durability settings

`sync_mode` chooses when a commit waits for the disk. `FULL`, the default, syncs
the log on every commit, so a commit that has returned survives a power cut.
`NORMAL` hands the commit to the operating system and returns: it survives the
process being killed, but a power cut or OS crash can lose the most recent
commits, from the end only, and the database stays consistent. `full_fsync`, on
macOS only, makes each sync ask the drive to flush its own cache
(`F_FULLFSYNC`); it makes each commit several milliseconds slower and has no
effect elsewhere. A value set in the options wins over the `LATTICE_SYNC_MODE`
and `LATTICE_FULL_FSYNC` environment variables, which apply only to a handle
that leaves the field at 0, as every older open function does. See
[Durability and the Log](../configuration/durability.md).

### Vector index configuration

The database file records its vector index: the dimensions, the metric, `m`
and `ef_construction`. A database that has an index opens with it whether or
not `enable_vector` is set, so reopening needs no vector options at all, also
after a crash. `enable_vector` means "create an index if the database has none",
and `vector_metric`, `vector_m` and `vector_ef_construction` configure that new
index. Once the index exists they are fixed; to change them, export the data
and import it into a new database.

Any of those three fields that is set, and `vector_dimensions` when
`enable_vector` is set, must match what the file records, or the open returns
`LATTICE_ERROR_CONFIG_MISMATCH` before anything is read. To open a database
whose dimensions you do not know, leave `enable_vector` off.

The metric decides what a search's distance is, smallest first, and what the
Cypher `<=>` operator returns:

| Metric | Distance | Vectors are stored |
|--------|----------|--------------------|
| `LATTICE_VECTOR_METRIC_COSINE` | `1 - cos(a, b)`, in [0, 2] | scaled to unit length |
| `LATTICE_VECTOR_METRIC_EUCLIDEAN` | `sqrt(sum((a - b)^2))` | as given |
| `LATTICE_VECTOR_METRIC_DOT_PRODUCT` | `-(a . b)`, so the largest dot product comes first | as given |

A database written by 0.16.0 records no configuration. Its index is cosine with
`m` 16 and `ef_construction` 200, at the dimensions its vector pages carry, and
that configuration is written into the file the first time this release opens
it for writing.

### Files written by newer releases

The file header says which releases may read the file and which may write it. A
file that only a newer release may write opens with `read_only` set; a
read-write open returns `LATTICE_ERROR_NEEDS_NEWER_WRITER` and writes nothing.
A file whose format this release cannot read at all is refused by every open
with `LATTICE_ERROR_VERSION_MISMATCH`.

### Read-only opens after a crash

A read-only open of a database that was not closed cleanly recovers it in
memory: the journal and log are applied as a read-write open would apply them,
every write that recovery makes is kept in memory, and nothing reaches the
files. The handle sees every committed transaction. The memory is bounded at
64 MB plus four times the size of the log and journal; a recovery that needs
more returns `LATTICE_ERROR_FULL`, and opening the database read-write once
recovers it on disk. A database with an empty log and no journal opens read-only
exactly as before.

Always initialise from the matching `_DEFAULT` macro rather than zeroing the
struct yourself, because `struct_size` has to be set correctly. With `lock` this
matters more than usual: a zeroed struct asks for no locking, which is the
opposite of what you want.

### In-memory databases

```c
lattice_open_options_v4 options = LATTICE_OPEN_OPTIONS_V4_DEFAULT;
lattice_database* db;
lattice_open_v4(":memory:", &options, &db);
```

Pass `:memory:` as the path and the database has no files behind it. Nothing is
written to disk and nothing survives closing it, which suits a scratch database, a
test, or one you pulled out of object storage and will hand back as bytes.

It behaves like any other database — transactions, the write-ahead log, and
serialization all work. The differences are that it disappears when closed, and
that nothing locks it, since no other process can reach it.

Opening `:memory:` implies creating it, so `create` does not need to be set: there
is never a previous in-memory database to find.

### The file lock

A database can only be open in one process at a time. Opening takes a lock on the
file: a read-write handle takes it exclusively and a read-only handle shares it,
so `lattice_open` returns `LATTICE_ERROR_DATABASE_LOCKED` if another process
holds it in a conflicting way. It does not wait.

Note that this is a different error from `LATTICE_ERROR_LOCK_TIMEOUT`, which
means a second write transaction inside your own process. One is a scheduling
problem you can retry your way out of; the other means the file belongs to
somebody else.

```c
lattice_open_options_v4 options = LATTICE_OPEN_OPTIONS_V4_DEFAULT;
options.lock = false;   // only where locking does not work
```

Turn `lock` off only on filesystems that do not support locking, such as some
network filesystems, where the alternative is not being able to open the database
at all. It does not make concurrent access safe. It removes the thing that was
going to tell you it was not.

### Checkpoints

A commit is durable once it is in the log. A checkpoint writes the pages the
log's commits changed into the database file, and can then empty the log. A
commit runs one once the log holds 1000 frames (one per page written), and open
and close fold whatever the log holds into the file.

```c
lattice_checkpoint_stats stats;
lattice_checkpoint(db, LATTICE_CHECKPOINT_TRUNCATE, &stats);  // stats may be NULL

lattice_set_auto_checkpoint(db, 10000, 0);  // every 10,000 frames, no minimum gap
lattice_set_auto_checkpoint(db, 0, 0);      // commits never checkpoint
```

`LATTICE_CHECKPOINT_PASSIVE` gives way if a changed page is in use (then it
writes nothing and reports 0 pages), `_FULL` writes every changed page, and
`_TRUNCATE` does that and empties the log; only truncate bounds the log's size.
The stats report pages written, the duration, the log position the file now
holds and whether the log was emptied. A read-only handle gets
`LATTICE_ERROR_READ_ONLY`.

`lattice_set_auto_checkpoint(db, max_wal_frames, min_interval_ms)` changes when
that handle's commits checkpoint from then on; 0 frames switches them off and the
log grows until `lattice_checkpoint` or close. Without it, the
`LATTICE_AUTO_CHECKPOINT_FRAMES` environment variable sets the frame count. A
larger interval means fewer pauses, each longer, a longer log and more to replay
after a crash; see [Checkpointing](../configuration/durability.md#checkpointing).

## Transaction Operations

```c
// Begin a transaction
lattice_txn* txn;
lattice_begin(db, LATTICE_TXN_READ_WRITE, &txn);

// ... do work ...

// Commit or rollback
lattice_commit(txn);
// or: lattice_rollback(txn);
```

Transaction modes:
- `LATTICE_TXN_READ_ONLY` — read-only, can run concurrently
- `LATTICE_TXN_READ_WRITE` — read-write, serialized

Any other mode value is rejected with `LATTICE_ERROR_INVALID_ARG`.

A successful commit or rollback frees the transaction handle. If
`lattice_commit` fails for any reason other than the transaction having already
ended (`LATTICE_ERROR_TXN_ABORTED`), the transaction stays open: retry the commit
or call `lattice_rollback`. Until you do, it holds the single writer slot, so
later writes fail with `LATTICE_ERROR_LOCK_TIMEOUT` and `lattice_close` is
refused.

## Node Operations

### Create a Node

```c
lattice_node_id node_id;
lattice_node_create(txn, "Person", &node_id);
```

### Set / Get Properties

```c
// Set a string property
lattice_value val = {
    .type = LATTICE_VALUE_STRING,
    .data.string_val = { "Alice", 5 }
};
lattice_node_set_property(txn, node_id, "name", &val);

// Get a property
lattice_value out;
lattice_node_get_property(txn, node_id, "name", &out);
```

Setting a property to a `LATTICE_VALUE_NULL` value removes it, as Cypher's
`SET n.x = null` does, and returns `LATTICE_OK` when the node has no such
property; `lattice_edge_set_property` does the same for an edge. No property
is ever stored as null, so a node created and then given a null property
simply does not have it. A null inside a list or a map is kept. Before 0.18.1
both calls stored the null, and a file written then can still hold one: it
reads back as a `LATTICE_VALUE_NULL` until it is set or removed.

### Check Existence

```c
bool exists;
lattice_node_exists(txn, node_id, &exists);
```

### Delete a Node

```c
lattice_error err = lattice_node_delete(txn, node_id);
if (err == LATTICE_ERROR_HAS_RELATIONSHIPS) {
    // The node still has relationships; nothing was deleted.
    lattice_node_detach_delete(txn, node_id);   // deletes them with it
}
```

`lattice_node_delete` refuses a node that still has relationships the
transaction can see, its own uncommitted ones included, with
`LATTICE_ERROR_HAS_RELATIONSHIPS`, as Cypher's `DELETE` does: deleting the node
alone would leave edges pointing at nothing. `lattice_node_detach_delete`
deletes the node and every relationship it has, as `DETACH DELETE` does, all in
the transaction. Deleting a node that does not exist succeeds and does nothing.

### Get Labels

```c
char* labels;
lattice_node_get_labels(txn, node_id, &labels);
// labels is a comma-separated string, e.g. "Person,Employee"
// ...
lattice_free_string(labels);
```

### Set a Vector

```c
float vector[128] = { /* ... */ };
lattice_node_set_vector(txn, node_id, "embedding", vector, 128);
```

A node holds one vector, which Cypher reads as `n.embedding`. The key must be
`"embedding"`, `""` or `NULL`; any other key returns `LATTICE_ERROR_UNSUPPORTED`.
A vector whose length is not the database's dimensions, or with a NaN or
infinite component, returns `LATTICE_ERROR_INVALID_ARG`.

A database created without a vector index has nowhere to keep a vector. There
`lattice_node_set_vector`, `lattice_batch_insert` (before it creates any node),
`lattice_vector_search` and `lattice_vector_search_txn` return
`LATTICE_ERROR_UNSUPPORTED`, the code a missing property or full-text index
gives; before 0.18.1 they returned `LATTICE_ERROR_IO`, which read like a disk
fault. `lattice_node_get_vector` finds no vector there and
`lattice_node_remove_vector` has nothing to remove, so both succeed. Opening the
database with `enable_vector` and `vector_dimensions` set adds an index.

### Get or Remove a Vector

```c
float* vector = NULL;
uint32_t dims = 0;
lattice_node_get_vector(txn, node_id, "embedding", &vector, &dims);
if (vector != NULL) {
    // ... use vector[0..dims) ...
    lattice_free_vector(vector, dims);
}

lattice_node_remove_vector(txn, node_id, "embedding");
```

`lattice_node_get_vector` returns the vector the transaction sees, including one
set or removed earlier in the same transaction. A node without a vector is not an
error: `vector` comes back `NULL` and `dims` 0. Free a returned vector with
`lattice_free_vector`. Under the cosine metric, the default, a committed vector
reads back normalised to unit length, because the index keeps only that copy: a
stored `(3, 4)` reads back as `(0.6, 0.8)`. Under the Euclidean and dot-product
metrics it reads back as stored, and an uncommitted vector always reads back as
written.

`lattice_node_remove_vector` takes the vector away. The node keeps its labels and
properties, and vector search no longer returns it. Removing a vector the node
does not have succeeds and changes nothing. Both calls take the same key as
`lattice_node_set_vector`, return `LATTICE_ERROR_NOT_FOUND` for a node that does
not exist, and the removal returns `LATTICE_ERROR_READ_ONLY` in a read-only
transaction. Both were added in 0.18.0.

### Explicit Property Indexes

Create property indexes outside an active write transaction. The lookup
functions below find equal values; Cypher also reads the same indexes for
ranges, `STARTS WITH` and `ORDER BY ... LIMIT`. Indexed lookup returns
`LATTICE_ERROR_UNSUPPORTED` if the requested definition does not exist; it never
silently falls back to a scan.

```c
lattice_node_property_index_create(db, "Person", "email");

lattice_value email = {
    .type = LATTICE_VALUE_STRING,
    .data.string_val = { "alice@example.com", 17 }
};
lattice_node_id* ids = NULL;
size_t count = 0;
lattice_nodes_find_by_label_property(
    txn, "Person", "email", &email, 10, &ids, &count
);
lattice_free_node_ids(ids, count);
```

Drop an index with `lattice_node_property_index_drop(db, "Person", "email")`.
Lookups against it start returning `LATTICE_ERROR_UNSUPPORTED` again once it is
gone.

Edge equivalents are `lattice_edge_property_index_create()`,
`lattice_edge_property_index_drop()`, and
`lattice_edges_find_by_type_property()`.

See [Property Indexes](../guides/property-indexes.md) for when an index is worth
adding and which queries the planner can use one for.

### Add and Remove Labels

A node's labels can change after it is created:

```c
lattice_node_add_label(txn, node_id, "Employee");
lattice_node_remove_label(txn, node_id, "Candidate");
```

### Find Nodes by Label

Get every node currently carrying a label. An unknown label is not an error; you
get a count of zero.

```c
lattice_node_id* ids;
size_t count;
lattice_get_nodes_by_label(db, "Person", 6, &ids, &count);
// ... use ids ...
lattice_free_node_ids(ids, count);
```

Use the `_txn` form to see the label as it looks inside a transaction, including
changes that transaction has made but not yet committed:

```c
lattice_get_nodes_by_label_txn(txn, "Person", 6, &ids, &count);
lattice_get_all_nodes_txn(txn, &ids, &count);   // every visible node
```

You own the returned array either way and must release it with
`lattice_free_node_ids`.

## Edge Operations

### Create / Delete

```c
lattice_edge_id edge_id;
lattice_edge_create(txn, source_id, target_id, "KNOWS", &edge_id);
lattice_edge_delete(txn, source_id, target_id, "KNOWS");
```

The returned `lattice_edge_id` is stable. Use it for edge property APIs and to
identify traversal results. `lattice_edge_create` returns
`LATTICE_ERROR_NOT_FOUND` unless both ends exist as the transaction sees them: a
node the transaction created counts, and one it deleted does not.

### Edge Properties

```c
lattice_value since = {
    .type = LATTICE_VALUE_INT,
    .data.int_val = 2020
};
lattice_edge_set_property(txn, edge_id, "since", &since);

lattice_value out;
if (lattice_edge_get_property(txn, edge_id, "since", &out) == LATTICE_OK) {
    /* consume out */
    lattice_value_free(&out);
}

lattice_edge_remove_property(txn, edge_id, "since");
```

### Traverse Edges

```c
lattice_edge_result* edges;
lattice_edge_get_outgoing(txn, node_id, &edges);

uint32_t count = lattice_edge_result_count(edges);
for (uint32_t i = 0; i < count; i++) {
    lattice_edge_id edge_id;
    lattice_node_id source, target;
    const char* type;
    uint32_t type_len;
    lattice_edge_result_get_id(edges, i, &edge_id);
    lattice_edge_result_get(edges, i, &source, &target, &type, &type_len);
}
lattice_edge_result_free(edges);
```

The type comes back as a pointer and a length. The bytes are also
NUL-terminated, so the pointer can be used as a C string; it stays valid until
the result is freed.

Typed traversal accepts a limit. Pass `0` for no limit:

```c
lattice_edge_get_outgoing_by_type(txn, node_id, "KNOWS", 100, &edges);
lattice_edge_get_incoming_by_type(txn, node_id, "KNOWS", 0, &edges);
```

`lattice_edge_scan(txn, edge_type_or_null, limit, &edges)` scans native edge
identities for administrative work such as index rebuilds or exports. It is not
the hot-path graph expansion API.

## Batch Insert

Insert many nodes with vectors in a single call:

```c
lattice_node_with_vector nodes[1000];
for (int i = 0; i < 1000; i++) {
    nodes[i].label = "Document";
    nodes[i].vector = vectors[i];  // float[128]
    nodes[i].dimensions = 128;
}

lattice_node_id ids[1000];
uint32_t count;
lattice_batch_insert(txn, nodes, 1000, ids, &count);
```

An empty label (`""`) creates an unlabeled node, as `lattice_node_create` does; a
`NULL` label is rejected.

## Vector Search

```c
float query[128] = { /* ... */ };
lattice_vector_result* results;
lattice_vector_search(db, query, 128, 10, 64, &results);

uint32_t count = lattice_vector_result_count(results);
for (uint32_t i = 0; i < count; i++) {
    lattice_node_id node_id;
    float distance;
    lattice_vector_result_get(results, i, &node_id, &distance);
    printf("Node %llu: distance=%.4f\n", node_id, distance);
}
lattice_vector_result_free(results);
```

Parameters:
- `k` — number of nearest neighbors to return
- `ef_search` — HNSW search parameter (0 = default 64). Higher values improve recall at the cost of latency. The search uses `max(ef_search, k)`, so a `k` above `ef_search` still returns `k` results when the database holds that many.

A query vector with a NaN or infinite component returns `LATTICE_ERROR_INVALID_ARG`.

## Full-Text Search

### Declare an Index

```c
/* One index per label and property. Declaring it reads the property from every
   node already carrying the label; writes maintain it from then on. */
lattice_node_fts_index_create(db, "Document", "text");
```

Labels and relationship types share one namespace, so a node index and a
relationship index (`lattice_edge_fts_index_create`) on the same name and
property would share storage: declaring the second one returns
`LATTICE_ERROR_ALREADY_EXISTS`.

### Search

```c
lattice_fts_result* results;
lattice_fts_search(db, "Document", "text", "quick fox", 9, 10, &results);

uint32_t count = lattice_fts_result_count(results);
for (uint32_t i = 0; i < count; i++) {
    lattice_node_id node_id;
    float score;
    lattice_fts_result_get(results, i, &node_id, &score);
    printf("Node %llu: score=%.4f\n", node_id, score);
}
lattice_fts_result_free(results);
```

### Fuzzy Search

```c
lattice_fts_result* results;
lattice_fts_search_fuzzy(db, "Document", "text", "quik fox", 8, 10, 2, 4, &results);
// max_distance=2, min_term_length=4
```

Plain search requires every term; a term no document contains gives no results.
Fuzzy search matches documents containing any of the words, or a near spelling
of one. Both read the same query syntax as Cypher's `@@`: `a OR b`, `-word`,
`"a phrase"`, `word*` and `word~` (see
[Query syntax](../guides/full-text-search.md#query-syntax)). A query that only
excludes, or a phrase against an index created before 0.17.0, which stores no
word positions, returns `LATTICE_ERROR_INVALID_ARG`.

## Embeddings

### Hash Embeddings (Built-in)

```c
float* vector;
uint32_t dims;
lattice_hash_embed("hello world", 11, 128, &vector, &dims);
// Use vector...
lattice_hash_embed_free(vector, dims);
```

### HTTP Embedding Client

```c
lattice_embedding_config config = {
    .endpoint = "http://localhost:11434/api/embeddings",  // the full URL to POST to
    .model = "nomic-embed-text",  // required; NULL is rejected with LATTICE_ERROR_INVALID_ARG
    .api_format = LATTICE_EMBEDDING_OLLAMA,
    .api_key = NULL,
    .timeout_ms = 0  // wait for the response to begin; 0 means 30 seconds
};

lattice_embedding_client* client;
lattice_embedding_client_create(&config, &client);

float* vector;
uint32_t dims;
lattice_embedding_client_embed(client, "hello world", 11, &vector, &dims);
// Use vector...
lattice_hash_embed_free(vector, dims);

lattice_embedding_client_free(client);
```

The client POSTs to `endpoint` exactly as given, with nothing appended, and
copies the endpoint, model and API key when it is created, so the caller's
strings can be freed afterwards. `timeout_ms` bounds the wait for the server to
start answering. It does not bound connecting or reading the rest of a response
that has begun, and it is not applied on Windows. A request that times out
returns `LATTICE_ERROR_IO`.

## Durable Streams

A stream is an append-only log stored inside the same database file. You publish
records to it, and consumers read forward from wherever they left off. Because
the log lives in the database, a record published in a transaction becomes
visible exactly when that transaction commits, and never if it rolls back.

See [Durable Streams](../guides/durable-streams.md) for what they are useful for.

### Publishing

```c
lattice_value payload = {
    .type = LATTICE_VALUE_STRING,
    .data.string_val = { "user signed up", 14 }
};

lattice_stream_publish(txn, "events", 6, "signup", 6, &payload);
```

Streams are created the first time you publish to one, so there is no separate
setup step. Passing `NULL` and `0` for the kind uses `"message"`. Names starting
with `__lattice_` are reserved for internal use.

If you need the sequence number the record was given, use the longer name:

```c
uint64_t sequence;
lattice_stream_publish_get_sequence(txn, "events", 6, NULL, 0, &payload, &sequence);
```

That sequence is only durable once the transaction commits.

### Reading

Reading happens on the database rather than inside a transaction. You pass the
sequence you last saw, and get back the records after it:

```c
lattice_stream_batch* batch;
lattice_stream_read(db, "events", 6, /* after_sequence */ 0, /* limit */ 100,
                    /* timeout_ms */ 1000, &batch);

size_t n = lattice_stream_batch_count(batch);
for (size_t i = 0; i < n; i++) {
    uint64_t sequence;
    const char* kind;
    size_t kind_len;
    const lattice_value* payload;

    lattice_stream_batch_get(batch, i, &sequence, &kind, &kind_len, &payload);
    // kind and payload are borrowed from the batch
}

lattice_stream_batch_free(batch);
```

Two things to be careful with. The kind and payload pointers belong to the
batch, so copy anything you need to keep before calling
`lattice_stream_batch_free`. And reading does not record how far you got; that
is a separate step, described below.

`timeout_ms` is how long to wait when there is nothing new. It wakes early if
another part of the same process commits a record, which makes it useful for a
consumer loop that should react promptly without spinning.

To find out where a stream currently ends:

```c
uint64_t last;
lattice_stream_get_last_sequence(db, "events", 6, &last);   // 0 if empty
```

### Remembering your place

Reading deliberately does not commit an offset, because that would mean losing a
record if your program stopped between reading and handling it. Store the offset
yourself once the work is actually done:

```c
lattice_stream_set_offset(txn, "events", 6, "billing-worker", 14, sequence);
```

Because that happens in a transaction, the offset and whatever else the
transaction wrote either both land or both do not.

To pick up where a consumer left off:

```c
bool exists;
uint64_t sequence;
lattice_stream_get_offset(db, "events", 6, "billing-worker", 14, &exists, &sequence);
```

`exists` is false the first time a consumer runs, which is when you start from
the beginning.

### Discarding old records

Once every consumer is past a point, the records before it can go:

```c
lattice_stream_trim(txn, "events", 6, /* through_sequence */ 5000);
```

Nothing trims automatically. A stream you never trim grows forever.

### The changefeed

The reserved stream `__lattice_changes` records every committed graph change,
and is read with `lattice_stream_read` like any other stream. It cannot be
published to; `lattice_stream_trim` drops its events through a sequence, as for
any stream. How much of it to keep is
a retention policy recorded in the file, set through `lattice_open_options_v5`:

```c
lattice_open_options_v5 options = LATTICE_OPEN_OPTIONS_V5_DEFAULT;
options.changefeed_retention = LATTICE_CHANGEFEED_RETENTION_LAST;
options.changefeed_retention_value = 100000; /* events; milliseconds for AGE */
lattice_open_v5("graph.lattice", &options, &db);
```

The values are `LATTICE_CHANGEFEED_RETENTION_DEFAULT` (keep the file's policy),
`_ALL`, `_LAST`, `_AGE` and `_OFF`. `lattice_changefeed_status(db, &mode,
&value, &oldest, &last)` reports the policy and the range held (`oldest` is 0
when no events are held), and `lattice_changefeed_trim(db, &removed)` applies
the policy in full now. Reading after a sequence whose next event was dropped
returns `LATTICE_ERROR_NOT_FOUND`; reading while the policy is off returns
`LATTICE_ERROR_UNSUPPORTED`. A transaction's events are its net effect, in an
order that can be replayed as it comes: the edges it deletes, the nodes that
exist after it, the edges that exist after it, then the nodes it deletes, each
group by ascending id. See [The Changefeed](../guides/changefeed.md).

## Query Operations

Queries use a prepare/bind/execute pattern:

```c
// 1. Prepare
lattice_query* query;
lattice_query_prepare(db, "MATCH (n) WHERE n.name = $name RETURN n", &query);

// 2. Bind parameters
lattice_value val = {
    .type = LATTICE_VALUE_STRING,
    .data.string_val = { "Alice", 5 }
};
lattice_query_bind(query, "name", &val);

// For vector parameters:
float vec[128] = { /* ... */ };
lattice_query_bind_vector(query, "embedding", vec, 128);

// 3. Execute
lattice_txn* txn;
lattice_begin(db, LATTICE_TXN_READ_ONLY, &txn);

lattice_result* result;
lattice_query_execute(query, txn, &result);

// 4. Iterate results
while (lattice_result_next(result)) {
    uint32_t cols = lattice_result_column_count(result);
    for (uint32_t i = 0; i < cols; i++) {
        const char* name = lattice_result_column_name(result, i);
        lattice_value val;
        lattice_result_get(result, i, &val);
        // Process val...
    }
}

// 5. Cleanup
lattice_result_free(result);
lattice_commit(txn);
lattice_query_free(query);
```

### Choosing a Transaction Mode

`lattice_query_execute` takes a transaction you opened, which means you have to
decide up front whether the query needs to write. Ask it:

```c
lattice_query* query;
lattice_query_prepare(db, cypher, &query);

lattice_txn* txn;
lattice_begin(db,
              lattice_query_writes(query) ? LATTICE_TXN_READ_WRITE
                                          : LATTICE_TXN_READ_ONLY,
              &txn);
```

This matters because the two modes fail in opposite directions. A read-only
transaction cannot run `CREATE`, `SET`, `DELETE`, `MERGE`, or `REMOVE`. A
read-write transaction takes the single writer slot, so opening one for a plain
read stops other reads running alongside it.

A query that does not parse is reported as not writing. Execution will report
the parse error, and a read transaction is the weaker thing to have opened in
the meantime.

Remember to commit rather than roll back when the query wrote something, or the
work is discarded.

### Finding Out What Went Wrong

`lattice_query_prepare` only stores the query text; parsing, analysis, planning
and execution all happen in `lattice_query_execute`, so that is where a bad query
fails. The return code says which kind of failure it was:

| Return code | Meaning |
|-------------|---------|
| `LATTICE_ERROR_INVALID_ARG` | The query does not parse, or does not make sense (an unknown function, a wrong argument count) |
| `LATTICE_ERROR` | It could not be planned, or failed while running (division by zero, integer overflow, a type error) |
| `LATTICE_ERROR_HAS_RELATIONSHIPS` | A `DELETE` named a node that still has relationships, as `lattice_node_delete` reports it |
| `LATTICE_ERROR_NOT_FOUND` | A write needed a node or relationship that does not exist, such as a relationship to a node deleted earlier in the query, as `lattice_edge_create` reports it |
| `LATTICE_ERROR_IO` | The storage failed under the query; the query itself may be fine |
| `LATTICE_ERROR_OUT_OF_MEMORY` | An allocation failed under the query |

A storage or allocation failure fails the query; it is never read as a missing
value. These functions describe the failure, and they read from the query
handle:

```c
if (lattice_query_execute(query, txn, &result) != LATTICE_OK) {
    printf("%s: %s\n",
           lattice_query_last_error_code(query),      // e.g. "invalid_operator_types"
           lattice_query_last_error_message(query));  // human-readable text

    if (lattice_query_last_error_has_location(query)) {
        printf("  at line %u, column %u, length %u\n",
               lattice_query_last_error_line(query),
               lattice_query_last_error_column(query),
               lattice_query_last_error_length(query));
    }
}
```

For `RETURN 1/0` this prints:

```text
DivisionByZero: Division by zero: 1 / 0
  at line 1, column 9, length 1
```

and for `MATCH (p:P) DELETE p` over a node with a relationship, with
`lattice_query_execute` returning `LATTICE_ERROR_HAS_RELATIONSHIPS`:

```text
NodeHasRelationships: Cannot delete node 3 because it still has relationships; delete them first, or use DETACH DELETE
  at line 1, column 20, length 1
```

Execution errors carry a location when the failure belongs to one expression or
clause; a refused delete points at the variable in the `DELETE`. Plan errors
carry none, and name the part of the query in the message instead. The location is what lets you point at the
offending part of the query, the way the `lattice` command-line tool underlines
it. The diagnostic code is a stable name for the kind of failure:
parse and semantic failures use lowercase names such as `unknown_function`, and
execution failures the engine's error names, such as `DivisionByZero`,
`IntegerOverflow`, `TypeError` or `NodeHasRelationships`.

`lattice_query_last_error_stage` tells you how far the query got:

```c
LATTICE_QUERY_STAGE_NONE       // 0 - no error
LATTICE_QUERY_STAGE_PARSE      // 1 - the text is not valid Cypher
LATTICE_QUERY_STAGE_SEMANTIC   // 2 - it parses, but does not make sense
LATTICE_QUERY_STAGE_PLAN       // 3 - no execution plan could be built
LATTICE_QUERY_STAGE_EXECUTION  // 4 - it failed while running
```

The distinction is useful when deciding whether to blame the query text or the
data: a parse or semantic failure will fail again no matter what the database
contains, while an execution failure might not.

The returned strings belong to the query handle and stay valid until the next
`lattice_query_execute` on it, or until you free it.

## Searching Inside a Transaction

Vector and full-text search have `_txn` variants that see the transaction's own
uncommitted changes, where the plain forms see only committed data. Use these
when you have just written something and need to search it in the same
transaction.

```c
lattice_vector_search_txn(txn, query_vector, 128, /* k */ 10,
                          /* ef_search */ 64, &vector_result);

lattice_fts_search_txn(txn, "Document", "text", "graph database", 14, /* limit */ 20, &fts_result);

lattice_fts_search_fuzzy_txn(txn, "Document", "text", "databse", 7, /* limit */ 20,
                             /* max_distance */ 2, /* min_term_length */ 4,
                             &fts_result);
```

Results are freed the same way as their non-transactional equivalents.

## Query Cache

```c
// Clear cache
lattice_query_cache_clear(db);

// Get statistics
uint32_t entries;
uint64_t hits, misses;
lattice_query_cache_stats(db, &entries, &hits, &misses);
```

## Utilities

```c
// Get version string
const char* version = lattice_version();  // e.g. "0.18.0"

// Get error message ("Unknown error" for a code the library does not define)
const char* msg = lattice_error_message(LATTICE_ERROR_NOT_FOUND);
```

### Releasing ID Arrays

Anything that hands you an array of IDs hands you ownership of it. Node IDs and
edge IDs have separate release functions, and they are not interchangeable:

```c
lattice_free_node_ids(node_ids, count);
lattice_free_edge_ids(edge_ids, count);
```
