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

## Edge Operations

### Create / Delete

```c
lattice_edge_id edge_id;
lattice_edge_create(txn, source_id, target_id, "KNOWS", &edge_id);
lattice_edge_delete(txn, source_id, target_id, "KNOWS");
```

### Traverse Edges

```c
lattice_edge_result* edges;
lattice_edge_get_outgoing(txn, node_id, &edges);

uint32_t count = lattice_edge_result_count(edges);
for (uint32_t i = 0; i < count; i++) {
    lattice_node_id source, target;
    const char* type;
    uint32_t type_len;
    lattice_edge_result_get(edges, i, &source, &target, &type, &type_len);
}
lattice_edge_result_free(edges);
```

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

### Index a Document

```c
const char* text = "The quick brown fox jumps over the lazy dog";
lattice_fts_index(txn, node_id, text, strlen(text));
```

### Search

```c
lattice_fts_result* results;
lattice_fts_search(db, "quick fox", 9, 10, &results);

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
lattice_fts_search_fuzzy(db, "quik fox", 8, 10, 2, 4, &results);
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
const char* version = lattice_version();  // e.g. "0.1.0"

// Get error message
const char* msg = lattice_error_message(LATTICE_ERROR_NOT_FOUND);
```
