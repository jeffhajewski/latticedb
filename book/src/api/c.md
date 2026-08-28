# C API

The C API is LatticeDB's primary interface. All language bindings (Python, TypeScript) wrap this API. The header file is `include/lattice.h`.

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
```

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
```

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
lattice_open_options opts = LATTICE_OPEN_OPTIONS_DEFAULT;
opts.create = true;
opts.enable_vector = true;
opts.vector_dimensions = 128;

lattice_database* db;
lattice_error err = lattice_open("mydb.ltdb", &opts, &db);
```

### Close

```c
lattice_close(db);
```

### Open Options

```c
typedef struct {
    bool create;              // Create if not exists
    bool read_only;           // Open in read-only mode
    uint32_t cache_size_mb;   // Cache size in MB (default: 100)
    uint32_t page_size;       // Page size in bytes (default: 4096)
    bool enable_vector;       // Enable vector storage
    uint16_t vector_dimensions; // Vector dimensions (default: 128)
} lattice_open_options;
```

### Newer Open Options

`lattice_open_options` cannot grow without breaking every program compiled
against the old size, so newer options come as new structs. Each starts with its
own size, which is how the library knows which version you compiled against.

```c
lattice_open_options_v4 options = LATTICE_OPEN_OPTIONS_V4_DEFAULT;
options.create = true;
options.enable_vector = true;
options.vector_dimensions = 1536;
options.enable_adjacency_cache = true;

lattice_database* db;
lattice_open_v4("graph.lattice", &options, &db);
```

Use `lattice_open_v2` with `lattice_open_options_v2`, which adds `enable_wal` to
the original set.
`lattice_open_options_v3` adds `enable_adjacency_cache`, which keeps an
in-memory map of which nodes connect to which and speeds up traversal.
`lattice_open_options_v4` adds `lock`, which defaults to true and is described
below.

Always initialise from the matching `_DEFAULT` macro rather than zeroing the
struct yourself, because `struct_size` has to be set correctly. With `lock` this
matters more than usual: a zeroed struct asks for no locking, which is the
opposite of what you want.

### The file lock

```c
lattice_open_options_v4 options = LATTICE_OPEN_OPTIONS_V4_DEFAULT;
lattice_database* db;
lattice_open_v4(":memory:", &options, &db);
```

### In-memory databases

Pass `:memory:` as the path and the database has no files behind it. Nothing is
written to disk and nothing survives closing it, which suits a scratch database, a
test, or one you pulled out of object storage and will hand back as bytes.

It behaves like any other database — transactions, the write-ahead log, and
serialization all work. The differences are that it disappears when closed, and
that nothing locks it, since no other process can reach it.

Opening `:memory:` implies creating it, so `create` does not need to be set: there
is never a previous in-memory database to find.

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

### Check Existence

```c
bool exists;
lattice_node_exists(txn, node_id, &exists);
```

### Delete a Node

```c
lattice_node_delete(txn, node_id);
```

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

### Explicit Property Indexes

Create equality indexes outside an active write transaction. Indexed lookup
returns `LATTICE_ERROR_UNSUPPORTED` if the requested definition does not exist;
it never silently falls back to a scan.

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
identify traversal results.

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
- `ef_search` — HNSW search parameter (0 = default 64). Higher values improve recall at the cost of latency.

## Full-Text Search

### Declare an Index

```c
/* One index per label and property. Declaring it reads the property from every
   node already carrying the label; writes maintain it from then on. */
lattice_node_fts_index_create(db, "Document", "text");
```

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
    .endpoint = "http://localhost:11434",
    .model = NULL,  // use default
    .api_format = LATTICE_EMBEDDING_OLLAMA,
    .api_key = NULL,
    .timeout_ms = 0  // default 30s
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

When `lattice_query_prepare` or `lattice_query_execute` fails, the return code
tells you that something failed but not what. These functions describe the
failure, and they read from the query handle:

```c
if (lattice_query_prepare(db, cypher, &query) != LATTICE_OK) {
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

The location is what lets you point at the offending part of the query, the way
the `lattice` command-line tool underlines it.

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

The returned strings belong to the query handle and stay valid until you prepare
something else on it or free it.

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
const char* version = lattice_version();  // e.g. "0.15.0"

// Get error message
const char* msg = lattice_error_message(LATTICE_ERROR_NOT_FOUND);
```

### Releasing ID Arrays

Anything that hands you an array of IDs hands you ownership of it. Node IDs and
edge IDs have separate release functions, and they are not interchangeable:

```c
lattice_free_node_ids(node_ids, count);
lattice_free_edge_ids(edge_ids, count);
```
