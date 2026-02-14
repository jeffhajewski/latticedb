/**
 * FFI function bindings to liblattice C API.
 *
 * These are low-level bindings that directly map to the C functions.
 * Use the wrapper in index.ts for a more ergonomic interface.
 */

import koffi from 'koffi';
import { getLibrary } from './library';

// Define opaque types for handles
const LatticeDatabase = koffi.opaque('lattice_database');
const LatticeTxn = koffi.opaque('lattice_txn');
const LatticeQuery = koffi.opaque('lattice_query');
const LatticeResult = koffi.opaque('lattice_result');
const LatticeVectorResult = koffi.opaque('lattice_vector_result');
const LatticeFtsResult = koffi.opaque('lattice_fts_result');
const LatticeEdgeResult = koffi.opaque('lattice_edge_result');
const LatticeEmbeddingClient = koffi.opaque('lattice_embedding_client');

// Define struct for embedding config
const LatticeEmbeddingConfig = koffi.struct('lattice_embedding_config', {
  endpoint: 'const char*',
  model: 'const char*',
  api_format: 'int',
  api_key: 'const char*',
  timeout_ms: 'uint32',
});

// Define struct for batch insert
const LatticeNodeWithVector = koffi.struct('lattice_node_with_vector', {
  label: 'const char*',
  vector: 'const float*',
  dimensions: 'uint32',
});

// Define struct for open options
const LatticeOpenOptions = koffi.struct('lattice_open_options', {
  create: 'bool',
  read_only: 'bool',
  cache_size_mb: 'uint32',
  page_size: 'uint32',
  enable_vector: 'bool',
  vector_dimensions: 'uint16',
});

// Define struct for string in value union
const LatticeStringValue = koffi.struct('lattice_string_value', {
  ptr: 'const char*',
  len: 'uintptr_t', // size_t
});

// Define struct for bytes in value union
const LatticeBytesValue = koffi.struct('lattice_bytes_value', {
  ptr: 'const uint8_t*',
  len: 'uintptr_t', // size_t
});

// Define struct for vector in value union
const LatticeVectorValue = koffi.struct('lattice_vector_value', {
  ptr: 'const float*',
  dimensions: 'uint32',
});

// Define the value union data
const LatticeValueData = koffi.union('lattice_value_data', {
  bool_val: 'bool',
  int_val: 'int64',
  float_val: 'double',
  string_val: LatticeStringValue,
  bytes_val: LatticeBytesValue,
  vector_val: LatticeVectorValue,
});

// Define the full value struct
const LatticeValue = koffi.struct('lattice_value', {
  type: 'int',
  data: LatticeValueData,
});

export interface LatticeBindings {
  // Database operations
  lattice_open: (
    path: string,
    options: unknown,
    db_out: unknown[]
  ) => number;
  lattice_close: (db: unknown) => number;

  // Transaction operations
  lattice_begin: (
    db: unknown,
    mode: number,
    txn_out: unknown[]
  ) => number;
  lattice_commit: (txn: unknown) => number;
  lattice_rollback: (txn: unknown) => number;

  // Node operations
  lattice_node_create: (
    txn: unknown,
    label: string | null,
    node_out: Buffer
  ) => number;
  lattice_node_delete: (txn: unknown, node_id: bigint) => number;
  lattice_node_exists: (
    txn: unknown,
    node_id: bigint,
    exists_out: Buffer
  ) => number;
  lattice_node_get_labels: (
    txn: unknown,
    node_id: bigint,
    labels_out: unknown[]
  ) => number;
  lattice_free_string: (str: unknown) => void;
  lattice_node_set_property: (
    txn: unknown,
    node_id: bigint,
    key: string,
    value: unknown
  ) => number;
  lattice_node_get_property: (
    txn: unknown,
    node_id: bigint,
    key: string,
    value_out: unknown
  ) => number;
  lattice_node_set_vector: (
    txn: unknown,
    node_id: bigint,
    key: string,
    vector: Float32Array,
    dimensions: number
  ) => number;
  lattice_batch_insert: (
    txn: unknown,
    nodes: unknown,
    count: number,
    node_ids_out: Buffer,
    count_out: Buffer
  ) => number;

  // Vector search operations
  lattice_vector_search: (
    db: unknown,
    vector: Float32Array,
    dimensions: number,
    k: number,
    ef_search: number,
    result_out: unknown[]
  ) => number;
  lattice_vector_result_count: (result: unknown) => number;
  lattice_vector_result_get: (
    result: unknown,
    index: number,
    node_id_out: Buffer,
    distance_out: Buffer
  ) => number;
  lattice_vector_result_free: (result: unknown) => void;

  // Full-text search operations
  lattice_fts_index: (
    txn: unknown,
    node_id: bigint,
    text: string,
    text_len: number
  ) => number;
  lattice_fts_search: (
    db: unknown,
    query: string,
    query_len: number,
    limit: number,
    result_out: unknown[]
  ) => number;
  lattice_fts_result_count: (result: unknown) => number;
  lattice_fts_result_get: (
    result: unknown,
    index: number,
    node_id_out: Buffer,
    score_out: Buffer
  ) => number;
  lattice_fts_result_free: (result: unknown) => void;

  // Edge operations
  lattice_edge_create: (
    txn: unknown,
    source: bigint,
    target: bigint,
    edge_type: string,
    edge_out: Buffer
  ) => number;
  lattice_edge_delete: (
    txn: unknown,
    source: bigint,
    target: bigint,
    edge_type: string
  ) => number;
  lattice_edge_get_outgoing: (
    txn: unknown,
    node_id: bigint,
    result_out: unknown[]
  ) => number;
  lattice_edge_get_incoming: (
    txn: unknown,
    node_id: bigint,
    result_out: unknown[]
  ) => number;
  lattice_edge_result_count: (result: unknown) => number;
  lattice_edge_result_get: (
    result: unknown,
    index: number,
    source_out: Buffer,
    target_out: Buffer,
    edge_type_out: unknown[],
    edge_type_len_out: Buffer
  ) => number;
  lattice_edge_result_free: (result: unknown) => void;

  // Query operations
  lattice_query_prepare: (
    db: unknown,
    cypher: string,
    query_out: unknown[]
  ) => number;
  lattice_query_bind: (
    query: unknown,
    name: string,
    value: unknown
  ) => number;
  lattice_query_bind_vector: (
    query: unknown,
    name: string,
    vector: Float32Array,
    dimensions: number
  ) => number;
  lattice_query_execute: (
    query: unknown,
    txn: unknown,
    result_out: unknown[]
  ) => number;
  lattice_query_free: (query: unknown) => void;
  lattice_query_cache_clear: (db: unknown) => number;
  lattice_query_cache_stats: (
    db: unknown,
    entries_out: Buffer,
    hits_out: Buffer,
    misses_out: Buffer
  ) => number;

  // Result operations
  lattice_result_next: (result: unknown) => boolean;
  lattice_result_column_count: (result: unknown) => number;
  lattice_result_column_name: (result: unknown, index: number) => string;
  lattice_result_get: (
    result: unknown,
    index: number,
    value_out: unknown
  ) => number;
  lattice_result_free: (result: unknown) => void;

  // Embedding operations
  lattice_hash_embed: (
    text: string,
    text_len: number,
    dimensions: number,
    vector_out: unknown[],
    dims_out: Buffer
  ) => number;
  lattice_hash_embed_free: (vector: unknown, dimensions: number) => void;
  lattice_embedding_client_create: (
    config: unknown,
    client_out: unknown[]
  ) => number;
  lattice_embedding_client_embed: (
    client: unknown,
    text: string,
    text_len: number,
    vector_out: unknown[],
    dims_out: Buffer
  ) => number;
  lattice_embedding_client_free: (client: unknown) => void;

  // Utility
  lattice_version: () => string;
  lattice_error_message: (code: number) => string;
}

let _bindings: LatticeBindings | null = null;

/**
 * Create the FFI bindings by loading functions from the library.
 */
function createBindings(): LatticeBindings {
  const lib = getLibrary();

  // Pointer types for out parameters
  const DatabasePtr = koffi.pointer(LatticeDatabase);
  const DatabasePtrPtr = koffi.pointer(DatabasePtr);
  const TxnPtr = koffi.pointer(LatticeTxn);
  const TxnPtrPtr = koffi.pointer(TxnPtr);
  const QueryPtr = koffi.pointer(LatticeQuery);
  const QueryPtrPtr = koffi.pointer(QueryPtr);
  const ResultPtr = koffi.pointer(LatticeResult);
  const ResultPtrPtr = koffi.pointer(ResultPtr);
  const VectorResultPtr = koffi.pointer(LatticeVectorResult);
  const VectorResultPtrPtr = koffi.pointer(VectorResultPtr);
  const FtsResultPtr = koffi.pointer(LatticeFtsResult);
  const FtsResultPtrPtr = koffi.pointer(FtsResultPtr);
  const EdgeResultPtr = koffi.pointer(LatticeEdgeResult);
  const EdgeResultPtrPtr = koffi.pointer(EdgeResultPtr);
  const OpenOptionsPtr = koffi.pointer(LatticeOpenOptions);
  const ValuePtr = koffi.pointer(LatticeValue);
  const EmbeddingClientPtr = koffi.pointer(LatticeEmbeddingClient);
  const EmbeddingClientPtrPtr = koffi.pointer(EmbeddingClientPtr);
  const EmbeddingConfigPtr = koffi.pointer(LatticeEmbeddingConfig);
  const NodeWithVectorPtr = koffi.pointer(LatticeNodeWithVector);

  return {
    // Database operations
    lattice_open: lib.func('lattice_open', 'int', [
      'str', // path
      OpenOptionsPtr, // options
      koffi.out(DatabasePtrPtr), // db_out
    ]),
    lattice_close: lib.func('lattice_close', 'int', [DatabasePtr]),

    // Transaction operations
    lattice_begin: lib.func('lattice_begin', 'int', [
      DatabasePtr,
      'int', // mode
      koffi.out(TxnPtrPtr), // txn_out
    ]),
    lattice_commit: lib.func('lattice_commit', 'int', [TxnPtr]),
    lattice_rollback: lib.func('lattice_rollback', 'int', [TxnPtr]),

    // Node operations
    lattice_node_create: lib.func('lattice_node_create', 'int', [
      TxnPtr,
      'str', // label
      koffi.out(koffi.pointer('uint64')), // node_out
    ]),
    lattice_node_delete: lib.func('lattice_node_delete', 'int', [
      TxnPtr,
      'uint64', // node_id
    ]),
    lattice_node_exists: lib.func('lattice_node_exists', 'int', [
      TxnPtr,
      'uint64', // node_id
      koffi.out(koffi.pointer('bool')), // exists_out
    ]),
    // NOTE: koffi auto-decodes char* to string, but we lose the pointer for freeing.
    // This causes a small memory leak per call. Acceptable for now, can revisit with
    // koffi.disposable() or modifying the C API to use caller-provided buffers.
    lattice_node_get_labels: lib.func('lattice_node_get_labels', 'int', [
      TxnPtr,
      'uint64', // node_id
      koffi.out(koffi.pointer('char*')), // labels_out - auto-decoded to string
    ]),
    lattice_free_string: lib.func('lattice_free_string', 'void', ['void*']),
    lattice_node_set_property: lib.func('lattice_node_set_property', 'int', [
      TxnPtr,
      'uint64', // node_id
      'str', // key
      ValuePtr, // value
    ]),
    lattice_node_get_property: lib.func('lattice_node_get_property', 'int', [
      TxnPtr,
      'uint64', // node_id
      'str', // key
      koffi.out(ValuePtr), // value_out
    ]),
    lattice_node_set_vector: lib.func('lattice_node_set_vector', 'int', [
      TxnPtr,
      'uint64', // node_id
      'str', // key
      koffi.pointer('float'), // vector
      'uint32', // dimensions
    ]),
    lattice_batch_insert: lib.func('lattice_batch_insert', 'int', [
      TxnPtr,
      NodeWithVectorPtr, // nodes array
      'uint32', // count
      koffi.out(koffi.pointer('uint64')), // node_ids_out
      koffi.out(koffi.pointer('uint32')), // count_out
    ]),

    // Vector search operations
    lattice_vector_search: lib.func('lattice_vector_search', 'int', [
      DatabasePtr,
      koffi.pointer('float'), // vector
      'uint32', // dimensions
      'uint32', // k
      'uint16', // ef_search
      koffi.out(VectorResultPtrPtr), // result_out
    ]),
    lattice_vector_result_count: lib.func('lattice_vector_result_count', 'uint32', [
      VectorResultPtr,
    ]),
    lattice_vector_result_get: lib.func('lattice_vector_result_get', 'int', [
      VectorResultPtr,
      'uint32', // index
      koffi.out(koffi.pointer('uint64')), // node_id_out
      koffi.out(koffi.pointer('float')), // distance_out
    ]),
    lattice_vector_result_free: lib.func('lattice_vector_result_free', 'void', [
      VectorResultPtr,
    ]),

    // Full-text search operations
    lattice_fts_index: lib.func('lattice_fts_index', 'int', [
      TxnPtr,
      'uint64', // node_id
      'str', // text
      'uintptr_t', // text_len (size_t)
    ]),
    lattice_fts_search: lib.func('lattice_fts_search', 'int', [
      DatabasePtr,
      'str', // query
      'uintptr_t', // query_len (size_t)
      'uint32', // limit
      koffi.out(FtsResultPtrPtr), // result_out
    ]),
    lattice_fts_result_count: lib.func('lattice_fts_result_count', 'uint32', [
      FtsResultPtr,
    ]),
    lattice_fts_result_get: lib.func('lattice_fts_result_get', 'int', [
      FtsResultPtr,
      'uint32', // index
      koffi.out(koffi.pointer('uint64')), // node_id_out
      koffi.out(koffi.pointer('float')), // score_out
    ]),
    lattice_fts_result_free: lib.func('lattice_fts_result_free', 'void', [
      FtsResultPtr,
    ]),

    // Edge operations
    lattice_edge_create: lib.func('lattice_edge_create', 'int', [
      TxnPtr,
      'uint64', // source
      'uint64', // target
      'str', // edge_type
      koffi.out(koffi.pointer('uint64')), // edge_out
    ]),
    lattice_edge_delete: lib.func('lattice_edge_delete', 'int', [
      TxnPtr,
      'uint64', // source
      'uint64', // target
      'str', // edge_type
    ]),
    lattice_edge_get_outgoing: lib.func('lattice_edge_get_outgoing', 'int', [
      TxnPtr,
      'uint64', // node_id
      koffi.out(EdgeResultPtrPtr), // result_out
    ]),
    lattice_edge_get_incoming: lib.func('lattice_edge_get_incoming', 'int', [
      TxnPtr,
      'uint64', // node_id
      koffi.out(EdgeResultPtrPtr), // result_out
    ]),
    lattice_edge_result_count: lib.func('lattice_edge_result_count', 'uint32', [
      EdgeResultPtr,
    ]),
    lattice_edge_result_get: lib.func('lattice_edge_result_get', 'int', [
      EdgeResultPtr,
      'uint32', // index
      koffi.out(koffi.pointer('uint64')), // source_out
      koffi.out(koffi.pointer('uint64')), // target_out
      koffi.out(koffi.pointer('str')), // edge_type_out
      koffi.out(koffi.pointer('uint32')), // edge_type_len_out
    ]),
    lattice_edge_result_free: lib.func('lattice_edge_result_free', 'void', [
      EdgeResultPtr,
    ]),

    // Query operations
    lattice_query_prepare: lib.func('lattice_query_prepare', 'int', [
      DatabasePtr,
      'str', // cypher
      koffi.out(QueryPtrPtr), // query_out
    ]),
    lattice_query_bind: lib.func('lattice_query_bind', 'int', [
      QueryPtr,
      'str', // name
      ValuePtr, // value
    ]),
    lattice_query_bind_vector: lib.func('lattice_query_bind_vector', 'int', [
      QueryPtr,
      'str', // name
      koffi.pointer('float'), // vector
      'uint32', // dimensions
    ]),
    lattice_query_execute: lib.func('lattice_query_execute', 'int', [
      QueryPtr,
      TxnPtr,
      koffi.out(ResultPtrPtr), // result_out
    ]),
    lattice_query_free: lib.func('lattice_query_free', 'void', [QueryPtr]),
    lattice_query_cache_clear: lib.func('lattice_query_cache_clear', 'int', [DatabasePtr]),
    lattice_query_cache_stats: lib.func('lattice_query_cache_stats', 'int', [
      DatabasePtr,
      koffi.out(koffi.pointer('uint32')),
      koffi.out(koffi.pointer('uint64')),
      koffi.out(koffi.pointer('uint64')),
    ]),

    // Result operations
    lattice_result_next: lib.func('lattice_result_next', 'bool', [ResultPtr]),
    lattice_result_column_count: lib.func('lattice_result_column_count', 'uint32', [
      ResultPtr,
    ]),
    lattice_result_column_name: lib.func('lattice_result_column_name', 'str', [
      ResultPtr,
      'uint32', // index
    ]),
    lattice_result_get: lib.func('lattice_result_get', 'int', [
      ResultPtr,
      'uint32', // index
      koffi.out(ValuePtr), // value_out
    ]),
    lattice_result_free: lib.func('lattice_result_free', 'void', [ResultPtr]),

    // Embedding operations
    lattice_hash_embed: lib.func('lattice_hash_embed', 'int', [
      'str', // text
      'uintptr_t', // text_len (size_t)
      'uint16', // dimensions
      koffi.out(koffi.pointer(koffi.pointer('float'))), // vector_out
      koffi.out(koffi.pointer('uint32')), // dims_out
    ]),
    lattice_hash_embed_free: lib.func('lattice_hash_embed_free', 'void', [
      koffi.pointer('float'), // vector
      'uint32', // dimensions
    ]),
    lattice_embedding_client_create: lib.func('lattice_embedding_client_create', 'int', [
      EmbeddingConfigPtr, // config
      koffi.out(EmbeddingClientPtrPtr), // client_out
    ]),
    lattice_embedding_client_embed: lib.func('lattice_embedding_client_embed', 'int', [
      EmbeddingClientPtr, // client
      'str', // text
      'uintptr_t', // text_len (size_t)
      koffi.out(koffi.pointer(koffi.pointer('float'))), // vector_out
      koffi.out(koffi.pointer('uint32')), // dims_out
    ]),
    lattice_embedding_client_free: lib.func('lattice_embedding_client_free', 'void', [
      EmbeddingClientPtr, // client
    ]),

    // Utility
    lattice_version: lib.func('lattice_version', 'str', []),
    lattice_error_message: lib.func('lattice_error_message', 'str', ['int']),
  };
}

/**
 * Get the FFI bindings (lazily initialized).
 */
export function getBindings(): LatticeBindings {
  if (!_bindings) {
    _bindings = createBindings();
  }
  return _bindings;
}
