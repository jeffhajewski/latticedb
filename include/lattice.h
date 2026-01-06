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
    LATTICE_VALUE_LIST = 6,
    LATTICE_VALUE_MAP = 7
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
    } data;
} lattice_value;

/* Open options */
typedef struct {
    bool create;           /* Create if not exists */
    bool read_only;        /* Open in read-only mode */
    uint32_t cache_size_mb; /* Cache size in MB (default: 100) */
    uint32_t page_size;    /* Page size in bytes (default: 4096) */
} lattice_open_options;

/* Default open options */
#define LATTICE_OPEN_OPTIONS_DEFAULT { false, false, 100, 4096 }

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

/* Set a vector on a node */
lattice_error lattice_node_set_vector(
    lattice_txn* txn,
    lattice_node_id node_id,
    const char* key,
    const float* vector,
    uint32_t dimensions
);

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
 * Query operations
 */

/* Prepare a Cypher query */
lattice_error lattice_query_prepare(
    lattice_database* db,
    const char* cypher,
    lattice_query** query_out
);

/* Bind a parameter to a query */
lattice_error lattice_query_bind(
    lattice_query* query,
    const char* name,
    const lattice_value* value
);

/* Bind a vector parameter */
lattice_error lattice_query_bind_vector(
    lattice_query* query,
    const char* name,
    const float* vector,
    uint32_t dimensions
);

/* Execute a query */
lattice_error lattice_query_execute(
    lattice_query* query,
    lattice_txn* txn,
    lattice_result** result_out
);

/* Free a query */
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
