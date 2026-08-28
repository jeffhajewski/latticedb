/**
 * Lattice: Embedded Knowledge Graph Database
 *
 * A single-file knowledge graph database for AI/RAG applications.
 * Combines property graph storage, HNSW vector search, and BM25 full-text search.
 *
 * This is the stable C API. All language bindings wrap this interface.
 */

#ifndef LATTICE_H
#define LATTICE_H

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Version information */
#define LATTICE_VERSION "0.15.0"
#define LATTICE_VERSION_MAJOR 0
#define LATTICE_VERSION_MINOR 15
#define LATTICE_VERSION_PATCH 0

/* Opaque handle types */
typedef struct lattice_database lattice_database;
typedef struct lattice_txn lattice_txn;
typedef struct lattice_query lattice_query;
typedef struct lattice_result lattice_result;
typedef struct lattice_vector_result lattice_vector_result;
typedef struct lattice_fts_result lattice_fts_result;
typedef struct lattice_edge_result lattice_edge_result;
typedef struct lattice_stream_batch lattice_stream_batch;

/* ID types */
typedef uint64_t lattice_node_id;
typedef uint64_t lattice_edge_id;

/* Error codes */
typedef enum {
    LATTICE_OK = 0,
    LATTICE_ERROR = -1,
    LATTICE_ERROR_IO = -2,
    LATTICE_ERROR_CORRUPTION = -3,
    LATTICE_ERROR_NOT_FOUND = -4,
    LATTICE_ERROR_ALREADY_EXISTS = -5,
    LATTICE_ERROR_INVALID_ARG = -6,
    LATTICE_ERROR_TXN_ABORTED = -7,
    LATTICE_ERROR_LOCK_TIMEOUT = -8,
    LATTICE_ERROR_READ_ONLY = -9,
    LATTICE_ERROR_FULL = -10,
    LATTICE_ERROR_VERSION_MISMATCH = -11,
    LATTICE_ERROR_CHECKSUM = -12,
    LATTICE_ERROR_OUT_OF_MEMORY = -13,
    LATTICE_ERROR_UNSUPPORTED = -14,
    LATTICE_ERROR_VALUE_TOO_LARGE = -15,
    /* Another process holds the database. Distinct from LATTICE_ERROR_LOCK_TIMEOUT,
     * which reports a second writer inside this process. */
    LATTICE_ERROR_DATABASE_LOCKED = -16
} lattice_error;

/* Transaction modes */
typedef enum {
    LATTICE_TXN_READ_ONLY = 0,
    LATTICE_TXN_READ_WRITE = 1
} lattice_txn_mode;

/* Value types. */
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

typedef struct lattice_value lattice_value;
typedef struct lattice_list lattice_list;
typedef struct lattice_map_entry lattice_map_entry;
typedef struct lattice_map lattice_map;

/* Recursive list container for nested values. */
struct lattice_list {
    lattice_value* items; /* nullable when len == 0 */
    size_t len;
};

/* Recursive map container for nested values. */
struct lattice_map {
    lattice_map_entry* entries; /* nullable when len == 0 */
    size_t len;
};

/* Property value. LIST and MAP use pointer-indirected containers so values can
 * nest recursively. */
struct lattice_value {
    lattice_value_type type;
    union {
        bool bool_val;
        int64_t int_val;
        double float_val;
        struct {
            const char* ptr;
            size_t len;
        } string_val;
        struct {
            const uint8_t* ptr;
            size_t len;
        } bytes_val;
        struct {
            const float* ptr;
            uint32_t dimensions;
        } vector_val;
        lattice_list* list_val;
        lattice_map* map_val;
    } data;
};

/* A single map entry (key/value pair) for nested values. */
struct lattice_map_entry {
    const char* key; /* nullable when key_len == 0 */
    size_t key_len;
    lattice_value value;
};

/* Open options */
typedef struct {
    bool create;            /* Create if not exists */
    bool read_only;         /* Open in read-only mode */
    uint32_t cache_size_mb; /* Cache size in MB (default: 100) */
    uint32_t page_size;     /* Page size in bytes (default: 4096) */
    bool enable_vector;     /* Enable vector storage for embeddings */
    uint16_t vector_dimensions; /* Vector dimensions, 1..4096 (default: 128) */
} lattice_open_options;

/* Default open options */
#define LATTICE_OPEN_OPTIONS_DEFAULT { false, false, 100, 4096, false, 128 }

/* Open options v2 */
typedef struct {
    size_t struct_size;     /* Must be sizeof(lattice_open_options_v2) */
    bool create;            /* Create if not exists */
    bool read_only;         /* Open in read-only mode */
    uint32_t cache_size_mb; /* Cache size in MB (default: 100) */
    uint32_t page_size;     /* Page size in bytes (default: 4096) */
    bool enable_vector;     /* Enable vector storage for embeddings */
    uint16_t vector_dimensions; /* Vector dimensions, 1..4096 (default: 128) */
    bool enable_wal;        /* Enable WAL-backed transactions (default: true) */
} lattice_open_options_v2;

/* Default open options v2 */
#define LATTICE_OPEN_OPTIONS_V2_DEFAULT { sizeof(lattice_open_options_v2), false, false, 100, 4096, false, 128, true }

/* Open options v3 */
typedef struct {
    size_t struct_size;     /* Must be sizeof(lattice_open_options_v3) */
    bool create;            /* Create if not exists */
    bool read_only;         /* Open in read-only mode */
    uint32_t cache_size_mb; /* Cache size in MB (default: 100) */
    uint32_t page_size;     /* Page size in bytes (default: 4096) */
    bool enable_vector;     /* Enable vector storage for embeddings */
    uint16_t vector_dimensions; /* Vector dimensions, 1..4096 (default: 128) */
    bool enable_wal;        /* Enable WAL-backed transactions (default: true) */
    bool enable_adjacency_cache; /* Enable in-memory graph adjacency cache */
} lattice_open_options_v3;

/* Default open options v3 */
#define LATTICE_OPEN_OPTIONS_V3_DEFAULT { sizeof(lattice_open_options_v3), false, false, 100, 4096, false, 128, true, false }

/* Open options v4 */
typedef struct {
    size_t struct_size;     /* Must be sizeof(lattice_open_options_v4) */
    bool create;            /* Create if not exists */
    bool read_only;         /* Open in read-only mode */
    uint32_t cache_size_mb; /* Cache size in MB (default: 100) */
    uint32_t page_size;     /* Page size in bytes (default: 4096) */
    bool enable_vector;     /* Enable vector storage for embeddings */
    uint16_t vector_dimensions; /* Vector dimensions, 1..4096 (default: 128) */
    bool enable_wal;        /* Enable WAL-backed transactions (default: true) */
    bool enable_adjacency_cache; /* Enable in-memory graph adjacency cache */
    /* Take a lock on the file so two processes cannot tread on each other
     * (default: true). A read-write handle takes the file exclusively and a
     * read-only handle shares it, so opening returns LATTICE_ERROR_DATABASE_LOCKED
     * rather than waiting. Turn this off only where locking does not work, such
     * as some network filesystems; it does not make concurrent access safe. */
    bool lock;
} lattice_open_options_v4;

/* Default open options v4 */
#define LATTICE_OPEN_OPTIONS_V4_DEFAULT { sizeof(lattice_open_options_v4), false, false, 100, 4096, false, 128, true, false, true }

/*
 * Database operations
 */

/* Open a database file */
lattice_error lattice_open(
    const char* path,
    const lattice_open_options* options,
    lattice_database** db_out
);

/* Open a database file with versioned options */
lattice_error lattice_open_v2(
    const char* path,
    const lattice_open_options_v2* options,
    lattice_database** db_out
);

/* Open a database file with v3 options */
lattice_error lattice_open_v3(
    const char* path,
    const lattice_open_options_v3* options,
    lattice_database** db_out
);

/* Open a database file with v4 options.
 * Returns LATTICE_ERROR_DATABASE_LOCKED if another process holds the database in
 * a conflicting way: a read-write handle takes the file exclusively, so a reader
 * is refused while a writer has it open. */
lattice_error lattice_open_v4(
    const char* path,
    const lattice_open_options_v4* options,
    lattice_database** db_out
);

/* Close a database.
 * Returns LATTICE_ERROR_INVALID_ARG if transaction or result handles that
 * depend on this database are still active. Close those handles first; the
 * database remains open in that case.
 * Once finalization begins, this function always consumes the database handle.
 * In particular, LATTICE_ERROR_IO reports a sync/checkpoint failure but the
 * handle is still closed and must not be reused or passed to lattice_close. */
lattice_error lattice_close(lattice_database* db);

/*
 * Transaction operations
 */

/* Begin a transaction.
 * At most one read-write transaction may be active per database handle.
 * Beginning a second writer returns LATTICE_ERROR_LOCK_TIMEOUT; read-only
 * transactions may remain active alongside the writer. */
lattice_error lattice_begin(
    lattice_database* db,
    lattice_txn_mode mode,
    lattice_txn** txn_out
);

/* Commit a transaction.
 * On success, the transaction handle is closed and must not be used for new
 * operations. Reusing it before database close returns LATTICE_ERROR_TXN_ABORTED. */
lattice_error lattice_commit(lattice_txn* txn);

/* Rollback a transaction.
 * On success, the transaction handle is closed and must not be used for new
 * operations. Reusing it before database close returns LATTICE_ERROR_TXN_ABORTED. */
lattice_error lattice_rollback(lattice_txn* txn);

/*
 * Node operations
 */

/* Create a node with an optional single label.
 * Pass NULL or "" to create an unlabeled node. */
lattice_error lattice_node_create(
    lattice_txn* txn,
    const char* label,
    lattice_node_id* node_out
);

/* Add a label to an existing node */
lattice_error lattice_node_add_label(
    lattice_txn* txn,
    lattice_node_id node_id,
    const char* label
);

/* Remove a label from an existing node */
lattice_error lattice_node_remove_label(
    lattice_txn* txn,
    lattice_node_id node_id,
    const char* label
);

/* Delete a node */
lattice_error lattice_node_delete(
    lattice_txn* txn,
    lattice_node_id node_id
);

/* Set a property on a node.
 * The value tree is borrowed for the duration of the call only.
 * STRING/BYTES/VECTOR pointers may be NULL only when their lengths are zero.
 * LIST/MAP container pointers must be non-NULL; empty containers use len=0. */
lattice_error lattice_node_set_property(
    lattice_txn* txn,
    lattice_node_id node_id,
    const char* key,
    const lattice_value* value
);

/* Get a property from a node.
 * Ownership of the full returned value tree transfers to the caller.
 * Release any owned storage with lattice_value_free() after consuming it. */
lattice_error lattice_node_get_property(
    lattice_txn* txn,
    lattice_node_id node_id,
    const char* key,
    lattice_value* value_out
);

/* Check if a node exists */
lattice_error lattice_node_exists(
    lattice_txn* txn,
    lattice_node_id node_id,
    bool* exists_out
);

/* Get labels for a node (comma-separated string, caller must free) */
lattice_error lattice_node_get_labels(
    lattice_txn* txn,
    lattice_node_id node_id,
    char** labels_out
);

/* Return every node id that currently carries `label`. On success the
 * caller owns `*node_ids_out` and must release it with
 * lattice_free_node_ids(ids, count). An unknown label is not an error
 * and yields count=0 with node_ids_out=NULL. */
lattice_error lattice_get_nodes_by_label(
    lattice_database* db,
    const char* label,
    size_t label_len,
    lattice_node_id** node_ids_out,
    size_t* count_out
);

lattice_error lattice_get_nodes_by_label_txn(
    lattice_txn* txn,
    const char* label,
    size_t label_len,
    lattice_node_id** node_ids_out,
    size_t* count_out
);

/* Return every visible node id for a transaction snapshot. */
lattice_error lattice_get_all_nodes_txn(
    lattice_txn* txn,
    lattice_node_id** node_ids_out,
    size_t* count_out
);

/* Create or drop an explicit equality index for a node label/property pair.
 * Creation scans existing matching nodes. These schema operations fail with
 * LATTICE_ERROR_LOCK_TIMEOUT while a write transaction is active. */
lattice_error lattice_node_property_index_create(
    lattice_database* db,
    const char* label,
    const char* property
);

lattice_error lattice_node_property_index_drop(
    lattice_database* db,
    const char* label,
    const char* property
);

/* Create or drop a full-text index over one node label/property pair.
 *
 * The property holds the text. Creation reads it from every node already
 * carrying the label, and writes maintain it from then on. Only string
 * properties are indexed.
 *
 * `d.property @@ "query"` searches the index declared for that label and
 * property, and fails when none is declared rather than returning no rows,
 * because no rows is indistinguishable from a search that found nothing.
 *
 * These schema operations fail with LATTICE_ERROR_LOCK_TIMEOUT while a write
 * transaction is active. */
lattice_error lattice_node_fts_index_create(
    lattice_database* db,
    const char* label,
    const char* property
);

lattice_error lattice_node_fts_index_drop(
    lattice_database* db,
    const char* label,
    const char* property
);

lattice_error lattice_node_fts_index_exists(
    lattice_database* db,
    const char* label,
    const char* property,
    bool* exists_out
);

/* The same over a relationship type and property. A Cypher
 * `-[x:TYPE]->` pattern with `x.property @@ "query"` searches this index. */
lattice_error lattice_edge_fts_index_create(
    lattice_database* db,
    const char* edge_type,
    const char* property
);

lattice_error lattice_edge_fts_index_drop(
    lattice_database* db,
    const char* edge_type,
    const char* property
);

lattice_error lattice_edge_fts_index_exists(
    lattice_database* db,
    const char* edge_type,
    const char* property,
    bool* exists_out
);

/* Find visible node IDs through an explicit label/property equality index.
 * Returns LATTICE_ERROR_UNSUPPORTED if the requested index does not exist.
 * The caller owns *node_ids_out and must use lattice_free_node_ids(). */
lattice_error lattice_nodes_find_by_label_property(
    lattice_txn* txn,
    const char* label,
    const char* property,
    const lattice_value* value,
    size_t limit,
    lattice_node_id** node_ids_out,
    size_t* count_out
);

/* Free an array returned by lattice_get_nodes_by_label. */
void lattice_free_node_ids(lattice_node_id* node_ids, size_t count);

/* Free a string allocated by lattice (e.g., from lattice_node_get_labels) */
void lattice_free_string(char* str);

/*
 * Serialization
 *
 * A database is a single file, so serializing one is handing back that file's
 * bytes. Write them anywhere and they open; pass them to lattice_deserialize and
 * they open without becoming a file you have to name.
 *
 * This is the piece that makes a database easy to keep in object storage. Your
 * application does its own uploading and downloading, with the client,
 * credentials, and retry policy it already has.
 */

/* Hand back the whole database as bytes.
 * Pending writes are folded in first, so the result needs no write-ahead log
 * beside it. Returns LATTICE_ERROR_LOCK_TIMEOUT if a transaction is open, because
 * bytes captured while writes land underneath them are torn.
 * The caller owns *bytes_out and must release it with lattice_free_bytes(). */
lattice_error lattice_serialize(
    lattice_database* db,
    uint8_t** bytes_out,
    size_t* len_out
);

/* Open a database from bytes produced by lattice_serialize.
 * The bytes are copied, so the caller may free them as soon as this returns.
 * Changes made afterwards do not travel back to them; call lattice_serialize
 * again to get the new bytes.
 * Returns LATTICE_ERROR_CORRUPTION if the bytes are not a database. */
lattice_error lattice_deserialize(
    const uint8_t* bytes,
    size_t len,
    const lattice_open_options_v4* options,
    lattice_database** db_out
);

/* Open a database from bytes without copying them.
 *
 * Saves holding the database twice while it loads. Each page becomes a copy of
 * its own the first time it is written, so reading a database and changing a
 * little of it keeps one copy of nearly all of it.
 *
 * THE BYTES MUST OUTLIVE THE DATABASE. Nothing is copied, so releasing them
 * while the database is open leaves it reading freed memory. This is a separate
 * function rather than a flag because that obligation deserves to be visible at
 * the call site.
 *
 * Not every language can promise this. Go may not let C retain a pointer into
 * its heap at all, and pinning a Java array for the life of a database would
 * hold up the collector for just as long; those callers want
 * lattice_deserialize instead. */
lattice_error lattice_deserialize_borrowed(
    const uint8_t* bytes,
    size_t len,
    const lattice_open_options_v4* options,
    lattice_database** db_out
);

/* Release a buffer returned by lattice_serialize. */
void lattice_free_bytes(uint8_t* bytes, size_t len);

/* Free heap-backed storage inside a lattice_value returned by an owning API.
 * LIST/MAP values are released recursively.
 * Safe to call on null/bool/int/float values.
 *
 * Ownership currently transfers from lattice_node_get_property() and
 * lattice_edge_get_property().
 * Values returned by lattice_result_get() are borrowed from the result handle
 * and must not be passed to lattice_value_free().
 */
void lattice_value_free(lattice_value* value);

/*
 * Durable stream operations
 */

/* Publish a record in a write transaction. Named streams auto-create on first
 * publish. User stream names beginning with "__lattice_" are reserved. Pass
 * NULL/0 for kind to use "message". */
lattice_error lattice_stream_publish(
    lattice_txn* txn,
    const char* stream,
    size_t stream_len,
    const char* kind,
    size_t kind_len,
    const lattice_value* payload
);

/* Publish a record and return the sequence assigned inside this transaction.
 * The sequence is durable only after the transaction commits. */
lattice_error lattice_stream_publish_get_sequence(
    lattice_txn* txn,
    const char* stream,
    size_t stream_len,
    const char* kind,
    size_t kind_len,
    const lattice_value* payload,
    uint64_t* sequence_out
);

/* Read records after a sequence cursor. Reads do not commit offsets. If no
 * records are available, timeout_ms waits for a same-process commit wakeup. */
lattice_error lattice_stream_read(
    lattice_database* db,
    const char* stream,
    size_t stream_len,
    uint64_t after_sequence,
    size_t limit,
    uint32_t timeout_ms,
    lattice_stream_batch** batch_out
);

size_t lattice_stream_batch_count(lattice_stream_batch* batch);

/* Returned kind and payload are borrowed from the batch and remain valid until
 * lattice_stream_batch_free(). */
lattice_error lattice_stream_batch_get(
    lattice_stream_batch* batch,
    size_t index,
    uint64_t* sequence_out,
    const char** kind_out,
    size_t* kind_len_out,
    const lattice_value** payload_out
);

void lattice_stream_batch_free(lattice_stream_batch* batch);

lattice_error lattice_stream_get_offset(
    lattice_database* db,
    const char* stream,
    size_t stream_len,
    const char* consumer,
    size_t consumer_len,
    bool* exists_out,
    uint64_t* sequence_out
);

/* Return the latest sequence published to a stream, or 0 when the stream has
 * no records. */
lattice_error lattice_stream_get_last_sequence(
    lattice_database* db,
    const char* stream,
    size_t stream_len,
    uint64_t* sequence_out
);

lattice_error lattice_stream_set_offset(
    lattice_txn* txn,
    const char* stream,
    size_t stream_len,
    const char* consumer,
    size_t consumer_len,
    uint64_t sequence
);

lattice_error lattice_stream_trim(
    lattice_txn* txn,
    const char* stream,
    size_t stream_len,
    uint64_t through_sequence
);

/* Set a vector on a node */
lattice_error lattice_node_set_vector(
    lattice_txn* txn,
    lattice_node_id node_id,
    const char* key,
    const float* vector,
    uint32_t dimensions
);

/*
 * Batch insert operations
 */

/* Node spec for batch insert: label + vector */
typedef struct {
    const char* label;
    const float* vector;
    uint32_t dimensions;
} lattice_node_with_vector;

/* Create multiple nodes with vectors in a single call.
 * On success, node_ids_out[0..count_out] contains created node IDs.
 * On partial failure, count_out indicates how many succeeded;
 * caller should rollback the transaction.
 */
lattice_error lattice_batch_insert(
    lattice_txn* txn,
    const lattice_node_with_vector* nodes,
    uint32_t count,
    lattice_node_id* node_ids_out,  /* caller-allocated, size count */
    uint32_t* count_out             /* nodes successfully created */
);

/*
 * Vector search operations
 *
 * Search for similar vectors using HNSW index:
 *   1. Call lattice_vector_search() with query vector
 *   2. Get result count with lattice_vector_result_count()
 *   3. Iterate results with lattice_vector_result_get()
 *   4. Free with lattice_vector_result_free()
 *
 * Example:
 *   float query[128] = { ... };
 *   lattice_vector_result* results;
 *   lattice_vector_search(db, query, 128, 10, 64, &results);
 *   uint32_t count = lattice_vector_result_count(results);
 *   for (uint32_t i = 0; i < count; i++) {
 *       lattice_node_id node_id;
 *       float distance;
 *       lattice_vector_result_get(results, i, &node_id, &distance);
 *   }
 *   lattice_vector_result_free(results);
 */

/* Search for similar vectors */
lattice_error lattice_vector_search(
    lattice_database* db,
    const float* vector,
    uint32_t dimensions,
    uint32_t k,                 /* Number of results to return */
    uint16_t ef_search,         /* HNSW ef parameter (0 for default) */
    lattice_vector_result** result_out
);

lattice_error lattice_vector_search_txn(
    lattice_txn* txn,
    const float* vector,
    uint32_t dimensions,
    uint32_t k,
    uint16_t ef_search,
    lattice_vector_result** result_out
);

/* Get the number of results */
uint32_t lattice_vector_result_count(lattice_vector_result* result);

/* Get a result by index */
lattice_error lattice_vector_result_get(
    lattice_vector_result* result,
    uint32_t index,
    lattice_node_id* node_id_out,
    float* distance_out
);

/* Free vector search results */
void lattice_vector_result_free(lattice_vector_result* result);

/*
 * Full-text search operations
 *
 * BM25-scored full-text search over a declared index:
 *   1. Declare an index over the property holding the text, with
 *      lattice_node_fts_index_create()
 *   2. Write that property; the index is maintained from then on
 *   3. Search with lattice_fts_search()
 *   4. Get result count with lattice_fts_result_count()
 *   5. Iterate results with lattice_fts_result_get()
 *   6. Free with lattice_fts_result_free()
 *
 * Example:
 *   lattice_node_fts_index_create(db, "Article", "text");
 *   // ... write Article.text on some nodes ...
 *
 *   lattice_fts_result* results;
 *   lattice_fts_search(db, "Article", "text", "quick fox", 9, 10, &results);
 *   uint32_t count = lattice_fts_result_count(results);
 *   for (uint32_t i = 0; i < count; i++) {
 *       lattice_node_id node_id;
 *       float score;
 *       lattice_fts_result_get(results, i, &node_id, &score);
 *   }
 *   lattice_fts_result_free(results);
 *
 * lattice_fts_index() is gone. It put text into a single index shared by every
 * node, which meant the text a search matched was not stored anywhere the
 * database could show you, and every property name searched the same text.
 * Text that was indexed that way and never written to a property cannot be
 * migrated automatically: store it in a property and declare an index over it.
 *
 * Searching a label and property with no declared index returns
 * LATTICE_ERROR_UNSUPPORTED rather than an empty result, so a mistyped property
 * name does not look like a query that found nothing.
 */

/* Search one declared index for documents matching a text query */
lattice_error lattice_fts_search(
    lattice_database* db,
    const char* label,
    const char* property,
    const char* query,
    size_t query_len,
    uint32_t limit,
    lattice_fts_result** result_out
);

lattice_error lattice_fts_search_txn(
    lattice_txn* txn,
    const char* label,
    const char* property,
    const char* query,
    size_t query_len,
    uint32_t limit,
    lattice_fts_result** result_out
);

/* Search with fuzzy matching (typo tolerance).
 * max_distance: max Levenshtein edit distance (0 = default 2)
 * min_term_length: min term length for fuzzy expansion (0 = default 4)
 *
 * Text written in the current transaction and not yet committed is matched by
 * term presence rather than edit distance, so a typo will not find a document
 * this transaction has only just written. */
lattice_error lattice_fts_search_fuzzy(
    lattice_database* db,
    const char* label,
    const char* property,
    const char* query,
    size_t query_len,
    uint32_t limit,
    uint32_t max_distance,
    uint32_t min_term_length,
    lattice_fts_result** result_out
);

lattice_error lattice_fts_search_fuzzy_txn(
    lattice_txn* txn,
    const char* label,
    const char* property,
    const char* query,
    size_t query_len,
    uint32_t limit,
    uint32_t max_distance,
    uint32_t min_term_length,
    lattice_fts_result** result_out
);

/* Get the number of FTS results */
uint32_t lattice_fts_result_count(lattice_fts_result* result);

/* Get a result by index (returns node_id and BM25 score) */
lattice_error lattice_fts_result_get(
    lattice_fts_result* result,
    uint32_t index,
    lattice_node_id* node_id_out,
    float* score_out
);

/* Free FTS search results */
void lattice_fts_result_free(lattice_fts_result* result);

/*
 * Embedding operations
 */

typedef struct lattice_embedding_client lattice_embedding_client;

typedef enum {
    LATTICE_EMBEDDING_OLLAMA = 0,
    LATTICE_EMBEDDING_OPENAI = 1
} lattice_embedding_api_format;

typedef struct {
    const char* endpoint;
    const char* model;
    lattice_embedding_api_format api_format;
    const char* api_key;        /* NULL for no auth */
    uint32_t timeout_ms;        /* 0 = default 30s */
} lattice_embedding_config;

/* Generate a hash embedding (built-in, no external service).
 * Caller must free with lattice_hash_embed_free(). */
lattice_error lattice_hash_embed(
    const char* text, size_t text_len,
    uint16_t dimensions,
    float** vector_out, uint32_t* dims_out
);

/* Free a hash embedding vector */
void lattice_hash_embed_free(float* vector, uint32_t dimensions);

/* Create an HTTP embedding client */
lattice_error lattice_embedding_client_create(
    const lattice_embedding_config* config,
    lattice_embedding_client** client_out
);

/* Generate an embedding via HTTP.
 * Caller must free with lattice_hash_embed_free(). */
lattice_error lattice_embedding_client_embed(
    lattice_embedding_client* client,
    const char* text, size_t text_len,
    float** vector_out, uint32_t* dims_out
);

/* Free an HTTP embedding client */
void lattice_embedding_client_free(lattice_embedding_client* client);

/*
 * Edge operations
 */

/* Create an edge between two nodes */
lattice_error lattice_edge_create(
    lattice_txn* txn,
    lattice_node_id source,
    lattice_node_id target,
    const char* edge_type,
    lattice_edge_id* edge_out
);

/* Delete an edge between two nodes */
lattice_error lattice_edge_delete(
    lattice_txn* txn,
    lattice_node_id source,
    lattice_node_id target,
    const char* edge_type
);

/* Set a property on an edge by stable edge ID.
 * The value tree is borrowed for the duration of the call only. */
lattice_error lattice_edge_set_property(
    lattice_txn* txn,
    lattice_edge_id edge_id,
    const char* key,
    const lattice_value* value
);

/* Get a property from an edge by stable edge ID.
 * Ownership of the full returned value tree transfers to the caller.
 * Release any owned storage with lattice_value_free() after consuming it. */
lattice_error lattice_edge_get_property(
    lattice_txn* txn,
    lattice_edge_id edge_id,
    const char* key,
    lattice_value* value_out
);

/* Remove a property from an edge by stable edge ID */
lattice_error lattice_edge_remove_property(
    lattice_txn* txn,
    lattice_edge_id edge_id,
    const char* key
);

/* Create or drop an explicit equality index for an edge type/property pair. */
lattice_error lattice_edge_property_index_create(
    lattice_database* db,
    const char* edge_type,
    const char* property
);

lattice_error lattice_edge_property_index_drop(
    lattice_database* db,
    const char* edge_type,
    const char* property
);

/* Find visible edge IDs through an explicit type/property equality index.
 * Returns LATTICE_ERROR_UNSUPPORTED if the requested index does not exist.
 * The caller owns *edge_ids_out and must use lattice_free_edge_ids(). */
lattice_error lattice_edges_find_by_type_property(
    lattice_txn* txn,
    const char* edge_type,
    const char* property,
    const lattice_value* value,
    size_t limit,
    lattice_edge_id** edge_ids_out,
    size_t* count_out
);

void lattice_free_edge_ids(lattice_edge_id* edge_ids, size_t count);

/*
 * Edge traversal operations
 *
 * Get edges connected to a node:
 *   1. Call lattice_edge_get_outgoing() or lattice_edge_get_incoming()
 *   2. Get result count with lattice_edge_result_count()
 *   3. Iterate results with lattice_edge_result_get()
 *   4. Free with lattice_edge_result_free()
 *
 * Example:
 *   lattice_edge_result* edges;
 *   lattice_edge_get_outgoing(txn, node_id, &edges);
 *   uint32_t count = lattice_edge_result_count(edges);
 *   for (uint32_t i = 0; i < count; i++) {
 *       lattice_node_id source, target;
 *       const char* type;
 *       uint32_t type_len;
 *       lattice_edge_result_get(edges, i, &source, &target, &type, &type_len);
 *   }
 *   lattice_edge_result_free(edges);
 */

/* Get all outgoing edges from a node */
lattice_error lattice_edge_get_outgoing(
    lattice_txn* txn,
    lattice_node_id node_id,
    lattice_edge_result** result_out
);

/* Get all incoming edges to a node */
lattice_error lattice_edge_get_incoming(
    lattice_txn* txn,
    lattice_node_id node_id,
    lattice_edge_result** result_out
);

/* Get outgoing edges from a node filtered by edge type.
 * limit == 0 means unlimited. */
lattice_error lattice_edge_get_outgoing_by_type(
    lattice_txn* txn,
    lattice_node_id node_id,
    const char* edge_type,
    size_t limit,
    lattice_edge_result** result_out
);

/* Get incoming edges to a node filtered by edge type.
 * limit == 0 means unlimited. */
lattice_error lattice_edge_get_incoming_by_type(
    lattice_txn* txn,
    lattice_node_id node_id,
    const char* edge_type,
    size_t limit,
    lattice_edge_result** result_out
);

/* Scan native edges for admin/index rebuild use. This is not intended for
 * hot-path graph expansion. Pass NULL edge_type to return all visible edges.
 * limit == 0 means unlimited. */
lattice_error lattice_edge_scan(
    lattice_txn* txn,
    const char* edge_type,
    size_t limit,
    lattice_edge_result** result_out
);

/* Get the number of edges in a result set */
uint32_t lattice_edge_result_count(lattice_edge_result* result);

/* Get the stable edge ID for a result by index */
lattice_error lattice_edge_result_get_id(
    lattice_edge_result* result,
    uint32_t index,
    lattice_edge_id* edge_id_out
);

/* Get an edge from a result set by index */
lattice_error lattice_edge_result_get(
    lattice_edge_result* result,
    uint32_t index,
    lattice_node_id* source_out,
    lattice_node_id* target_out,
    const char** edge_type_out,
    uint32_t* edge_type_len_out
);

/* Free an edge result set */
void lattice_edge_result_free(lattice_edge_result* result);

/*
 * Query operations
 *
 * Queries use a prepare/bind/execute pattern:
 *   1. Prepare the query with lattice_query_prepare()
 *   2. Bind parameters with lattice_query_bind() (optional)
 *   3. Execute with lattice_query_execute()
 *   4. Iterate results with lattice_result_next() / lattice_result_get()
 *   5. Free resources with lattice_query_free() and lattice_result_free()
 *
 * Example:
 *   lattice_query* query;
 *   lattice_query_prepare(db, "MATCH (n) WHERE n.name = $name RETURN n", &query);
 *   lattice_value val = { .type = LATTICE_VALUE_STRING, .data.string_val = {"Alice", 5} };
 *   lattice_query_bind(query, "name", &val);
 *   lattice_query_execute(query, txn, &result);
 */

/* Query diagnostic stage returned by lattice_query_last_error_stage() */
typedef enum {
    LATTICE_QUERY_STAGE_NONE = 0,
    LATTICE_QUERY_STAGE_PARSE = 1,
    LATTICE_QUERY_STAGE_SEMANTIC = 2,
    LATTICE_QUERY_STAGE_PLAN = 3,
    LATTICE_QUERY_STAGE_EXECUTION = 4
} lattice_query_error_stage;

/* Prepare a Cypher query for execution */
lattice_error lattice_query_prepare(
    lattice_database* db,
    const char* cypher,
    lattice_query** query_out
);

/* Bind a parameter to a prepared query (use $name in Cypher).
 * The value tree is borrowed for the duration of the call only. */
lattice_error lattice_query_bind(
    lattice_query* query,
    const char* name,       /* Parameter name without $ prefix */
    const lattice_value* value
);

/* Bind a vector parameter to a prepared query */
lattice_error lattice_query_bind_vector(
    lattice_query* query,
    const char* name,
    const float* vector,
    uint32_t dimensions
);

/* Execute a prepared query and get results */
lattice_error lattice_query_execute(
    lattice_query* query,
    lattice_txn* txn,
    lattice_result** result_out
);

/* Last query diagnostics for a prepared query handle.
 * Populated when lattice_query_execute() fails.
 * Cleared automatically before each execute call.
 */
lattice_query_error_stage lattice_query_last_error_stage(lattice_query* query);
const char* lattice_query_last_error_message(lattice_query* query);
const char* lattice_query_last_error_code(lattice_query* query); /* nullable */
/* Whether executing this query could change the database.
 * Bindings use this to choose between a read-only and a read-write
 * transaction. Returns false for a query that does not parse; execution
 * reports the parse error, and a read transaction is the weaker thing to
 * have opened in the meantime. */
bool lattice_query_writes(lattice_query* query);

bool lattice_query_last_error_has_location(lattice_query* query);
uint32_t lattice_query_last_error_line(lattice_query* query);     /* 1-based, 0 if unavailable */
uint32_t lattice_query_last_error_column(lattice_query* query);   /* 1-based, 0 if unavailable */
uint32_t lattice_query_last_error_length(lattice_query* query);   /* token span length, 0 if unavailable */

/* Free a prepared query (call after done with results) */
void lattice_query_free(lattice_query* query);

/*
 * Result operations
 */

/* Get next row from result set */
bool lattice_result_next(lattice_result* result);

/* Get column count */
uint32_t lattice_result_column_count(lattice_result* result);

/* Get column name */
const char* lattice_result_column_name(lattice_result* result, uint32_t index);

/* Get column value.
 * The full returned value tree is borrowed from the result handle and stays
 * valid until lattice_result_free(). Do not pass it to lattice_value_free().
 */
lattice_error lattice_result_get(
    lattice_result* result,
    uint32_t index,
    lattice_value* value_out
);

/* Free a result set */
void lattice_result_free(lattice_result* result);

/*
 * Query cache operations
 */

/* Clear the query cache */
lattice_error lattice_query_cache_clear(lattice_database* db);

/* Get query cache statistics */
lattice_error lattice_query_cache_stats(
    lattice_database* db,
    uint32_t* entries_out,
    uint64_t* hits_out,
    uint64_t* misses_out
);

/*
 * Utility functions
 */

/* Get version string */
const char* lattice_version(void);

/* Get last error message */
const char* lattice_error_message(lattice_error code);

#ifdef __cplusplus
}
#endif

#endif /* LATTICE_H */
