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
#define LATTICE_VERSION "0.1.0"
#define LATTICE_VERSION_MAJOR 0
#define LATTICE_VERSION_MINOR 1
#define LATTICE_VERSION_PATCH 0

/* Opaque handle types */
typedef struct lattice_database lattice_database;
typedef struct lattice_txn lattice_txn;
typedef struct lattice_query lattice_query;
typedef struct lattice_result lattice_result;
typedef struct lattice_vector_result lattice_vector_result;
typedef struct lattice_fts_result lattice_fts_result;
typedef struct lattice_edge_result lattice_edge_result;

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
    LATTICE_ERROR_OUT_OF_MEMORY = -13
} lattice_error;

/* Transaction modes */
typedef enum {
    LATTICE_TXN_READ_ONLY = 0,
    LATTICE_TXN_READ_WRITE = 1
} lattice_txn_mode;

/* Value types */
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

/* Property value */
typedef struct {
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
    } data;
} lattice_value;

/* Open options */
typedef struct {
    bool create;            /* Create if not exists */
    bool read_only;         /* Open in read-only mode */
    uint32_t cache_size_mb; /* Cache size in MB (default: 100) */
    uint32_t page_size;     /* Page size in bytes (default: 4096) */
    bool enable_vector;     /* Enable vector storage for embeddings */
    uint16_t vector_dimensions; /* Vector dimensions (default: 128) */
} lattice_open_options;

/* Default open options */
#define LATTICE_OPEN_OPTIONS_DEFAULT { false, false, 100, 4096, false, 128 }

/*
 * Database operations
 */

/* Open a database file */
lattice_error lattice_open(
    const char* path,
    const lattice_open_options* options,
    lattice_database** db_out
);

/* Close a database */
lattice_error lattice_close(lattice_database* db);

/*
 * Transaction operations
 */

/* Begin a transaction */
lattice_error lattice_begin(
    lattice_database* db,
    lattice_txn_mode mode,
    lattice_txn** txn_out
);

/* Commit a transaction */
lattice_error lattice_commit(lattice_txn* txn);

/* Rollback a transaction */
lattice_error lattice_rollback(lattice_txn* txn);

/*
 * Node operations
 */

/* Create a node with a label */
lattice_error lattice_node_create(
    lattice_txn* txn,
    const char* label,
    lattice_node_id* node_out
);

/* Delete a node */
lattice_error lattice_node_delete(
    lattice_txn* txn,
    lattice_node_id node_id
);

/* Set a property on a node */
lattice_error lattice_node_set_property(
    lattice_txn* txn,
    lattice_node_id node_id,
    const char* key,
    const lattice_value* value
);

/* Get a property from a node */
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

/* Free a string allocated by lattice (e.g., from lattice_node_get_labels) */
void lattice_free_string(char* str);

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
 * BM25-scored full-text search:
 *   1. Index documents with lattice_fts_index()
 *   2. Search with lattice_fts_search()
 *   3. Get result count with lattice_fts_result_count()
 *   4. Iterate results with lattice_fts_result_get()
 *   5. Free with lattice_fts_result_free()
 *
 * Example:
 *   // Index a document
 *   const char* text = "The quick brown fox jumps over the lazy dog";
 *   lattice_fts_index(txn, node_id, text, strlen(text));
 *
 *   // Search
 *   lattice_fts_result* results;
 *   lattice_fts_search(db, "quick fox", 9, 10, &results);
 *   uint32_t count = lattice_fts_result_count(results);
 *   for (uint32_t i = 0; i < count; i++) {
 *       lattice_node_id node_id;
 *       float score;
 *       lattice_fts_result_get(results, i, &node_id, &score);
 *   }
 *   lattice_fts_result_free(results);
 */

/* Index a text document for full-text search */
lattice_error lattice_fts_index(
    lattice_txn* txn,
    lattice_node_id node_id,
    const char* text,
    size_t text_len
);

/* Search for documents matching a text query */
lattice_error lattice_fts_search(
    lattice_database* db,
    const char* query,
    size_t query_len,
    uint32_t limit,
    lattice_fts_result** result_out
);

/* Search with fuzzy matching (typo tolerance).
 * max_distance: max Levenshtein edit distance (0 = default 2)
 * min_term_length: min term length for fuzzy expansion (0 = default 4) */
lattice_error lattice_fts_search_fuzzy(
    lattice_database* db,
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

/* Get the number of edges in a result set */
uint32_t lattice_edge_result_count(lattice_edge_result* result);

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

/* Prepare a Cypher query for execution */
lattice_error lattice_query_prepare(
    lattice_database* db,
    const char* cypher,
    lattice_query** query_out
);

/* Bind a parameter to a prepared query (use $name in Cypher) */
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

/* Get column value */
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
