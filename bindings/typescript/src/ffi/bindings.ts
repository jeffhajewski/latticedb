/**
 * FFI function bindings to liblattice C API.
 *
 * These are low-level bindings that directly map to the C functions.
 * Use the wrapper in index.ts for a more ergonomic interface.
 */

import koffi from 'koffi';
import { getLibrary } from './library';

function defineNamedType<T>(name: string, factory: () => T): T {
  try {
    return factory();
  } catch (err) {
    const message = typeof err === 'object' && err !== null && 'message' in err
      ? String((err as { message: unknown }).message)
      : String(err);
    if (message.includes('Duplicate type name')) {
      return koffi.resolve(name) as unknown as T;
    }
    throw err;
  }
}

function defineStructBody<T>(name: string, ref: T, factory: () => void): T {
  try {
    factory();
  } catch (err) {
    const message = typeof err === 'object' && err !== null && 'message' in err
      ? String((err as { message: unknown }).message)
      : String(err);
    if (!message.includes('Cannot redefine non-opaque type')) {
      throw err;
    }
  }
  return ref;
}

// Define opaque types for handles
const LatticeDatabase = defineNamedType('lattice_database', () => koffi.opaque('lattice_database'));
const LatticeTxn = defineNamedType('lattice_txn', () => koffi.opaque('lattice_txn'));
const LatticeQuery = defineNamedType('lattice_query', () => koffi.opaque('lattice_query'));
const LatticeResult = defineNamedType('lattice_result', () => koffi.opaque('lattice_result'));
const LatticeVectorResult = defineNamedType('lattice_vector_result', () => koffi.opaque('lattice_vector_result'));
const LatticeFtsResult = defineNamedType('lattice_fts_result', () => koffi.opaque('lattice_fts_result'));
const LatticeEdgeResult = defineNamedType('lattice_edge_result', () => koffi.opaque('lattice_edge_result'));
const LatticeStreamBatch = defineNamedType('lattice_stream_batch', () => koffi.opaque('lattice_stream_batch'));
const LatticeEmbeddingClient = defineNamedType(
  'lattice_embedding_client',
  () => koffi.opaque('lattice_embedding_client')
);

// Define struct for embedding config
const LatticeEmbeddingConfig = defineNamedType('lattice_embedding_config', () => koffi.struct('lattice_embedding_config', {
  endpoint: 'const char*',
  model: 'const char*',
  api_format: 'int',
  api_key: 'const char*',
  timeout_ms: 'uint32',
}));

// Define struct for batch insert
const LatticeNodeWithVector = defineNamedType('lattice_node_with_vector', () => koffi.struct('lattice_node_with_vector', {
  label: 'const char*',
  vector: 'const float*',
  dimensions: 'uint32',
}));

// Define struct for open options
const LatticeOpenOptions = defineNamedType('lattice_open_options', () => koffi.struct('lattice_open_options', {
  create: 'bool',
  read_only: 'bool',
  cache_size_mb: 'uint32',
  page_size: 'uint32',
  enable_vector: 'bool',
  vector_dimensions: 'uint16',
}));

const LatticeOpenOptionsV2 = defineNamedType('lattice_open_options_v2', () => koffi.struct('lattice_open_options_v2', {
  struct_size: 'uintptr_t',
  create: 'bool',
  read_only: 'bool',
  cache_size_mb: 'uint32',
  page_size: 'uint32',
  enable_vector: 'bool',
  vector_dimensions: 'uint16',
  enable_wal: 'bool',
}));

// Define struct for string in value union
const LatticeStringValue = defineNamedType('lattice_string_value', () => koffi.struct('lattice_string_value', {
  ptr: 'void*',
  len: 'uintptr_t', // size_t
}));

// Define struct for bytes in value union
const LatticeBytesValue = defineNamedType('lattice_bytes_value', () => koffi.struct('lattice_bytes_value', {
  ptr: 'const uint8_t*',
  len: 'uintptr_t', // size_t
}));

// Define struct for vector in value union
const LatticeVectorValue = defineNamedType('lattice_vector_value', () => koffi.struct('lattice_vector_value', {
  ptr: 'const float*',
  dimensions: 'uint32',
}));

// Forward declarations for recursive nested value types
const LatticeValue = defineNamedType('lattice_value', () => koffi.opaque('lattice_value'));
const LatticeMapEntry = defineNamedType('lattice_map_entry', () => koffi.opaque('lattice_map_entry'));

// Recursive list/map containers for nested values
const LatticeList = defineNamedType('lattice_list', () => koffi.struct('lattice_list', {
  items: koffi.pointer(LatticeValue),
  len: 'uintptr_t',
}));

const LatticeMap = defineNamedType('lattice_map', () => koffi.struct('lattice_map', {
  entries: koffi.pointer(LatticeMapEntry),
  len: 'uintptr_t',
}));

// Define the value union data
const LatticeValueData = defineNamedType('lattice_value_data', () => koffi.union('lattice_value_data', {
  bool_val: 'bool',
  int_val: 'int64',
  float_val: 'double',
  string_val: LatticeStringValue,
  bytes_val: LatticeBytesValue,
  vector_val: LatticeVectorValue,
  list_val: koffi.pointer(LatticeList),
  map_val: koffi.pointer(LatticeMap),
}));

// Define the full value struct
defineStructBody('lattice_value', LatticeValue, () => {
  koffi.struct(LatticeValue, {
    type: 'int',
    data: LatticeValueData,
  });
});

defineStructBody('lattice_map_entry', LatticeMapEntry, () => {
  koffi.struct(LatticeMapEntry, {
    key: 'void*',
    key_len: 'uintptr_t',
    value: LatticeValue,
  });
});

export interface LatticeBindings {
  // Database operations
  lattice_open: (
    path: string,
    options: unknown,
    db_out: unknown[]
  ) => number;
  lattice_open_v2?: (
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
  lattice_node_add_label: (
    txn: unknown,
    node_id: bigint,
    label: string
  ) => number;
  lattice_node_remove_label: (
    txn: unknown,
    node_id: bigint,
    label: string
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
  lattice_get_nodes_by_label: (
    db: unknown,
    label: string | null,
    label_len: number,
    node_ids_out: unknown[],
    count_out: number[]
  ) => number;
  lattice_get_nodes_by_label_txn: (
    txn: unknown,
    label: string | null,
    label_len: number,
    node_ids_out: unknown[],
    count_out: number[]
  ) => number;
  lattice_free_node_ids: (node_ids: unknown, count: number) => void;
  lattice_free_string: (str: unknown) => void;
  lattice_value_free: (value: unknown) => void;
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
  lattice_stream_publish: (
    txn: unknown,
    stream: Buffer,
    stream_len: number,
    kind: Buffer | null,
    kind_len: number,
    payload: unknown
  ) => number;
  lattice_stream_read: (
    db: unknown,
    stream: Buffer,
    stream_len: number,
    after_sequence: bigint,
    limit: number,
    timeout_ms: number,
    batch_out: unknown[]
  ) => number;
  lattice_stream_batch_count: (batch: unknown) => number;
  lattice_stream_batch_get: (
    batch: unknown,
    index: number,
    sequence_out: Buffer,
    kind_out: unknown[],
    kind_len_out: number[],
    payload_out: unknown[]
  ) => number;
  lattice_stream_batch_free: (batch: unknown) => void;
  lattice_stream_get_offset: (
    db: unknown,
    stream: Buffer,
    stream_len: number,
    consumer: Buffer,
    consumer_len: number,
    exists_out: Buffer,
    sequence_out: Buffer
  ) => number;
  lattice_stream_set_offset: (
    txn: unknown,
    stream: Buffer,
    stream_len: number,
    consumer: Buffer,
    consumer_len: number,
    sequence: bigint
  ) => number;
  lattice_stream_trim: (
    txn: unknown,
    stream: Buffer,
    stream_len: number,
    through_sequence: bigint
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
  lattice_vector_search_txn: (
    txn: unknown,
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
  lattice_fts_search_txn: (
    txn: unknown,
    query: string,
    query_len: number,
    limit: number,
    result_out: unknown[]
  ) => number;
  lattice_fts_search_fuzzy: (
    db: unknown,
    query: string,
    query_len: number,
    limit: number,
    max_distance: number,
    min_term_length: number,
    result_out: unknown[]
  ) => number;
  lattice_fts_search_fuzzy_txn: (
    txn: unknown,
    query: string,
    query_len: number,
    limit: number,
    max_distance: number,
    min_term_length: number,
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
  lattice_edge_set_property: (
    txn: unknown,
    edge_id: bigint,
    key: string,
    value: unknown
  ) => number;
  lattice_edge_get_property: (
    txn: unknown,
    edge_id: bigint,
    key: string,
    value_out: unknown
  ) => number;
  lattice_edge_remove_property: (
    txn: unknown,
    edge_id: bigint,
    key: string
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
  lattice_edge_result_get_id: (
    result: unknown,
    index: number,
    edge_id_out: Buffer
  ) => number;
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
  lattice_query_last_error_stage: (query: unknown) => number;
  lattice_query_last_error_message: (query: unknown) => string | null;
  lattice_query_last_error_code: (query: unknown) => string | null;
  lattice_query_last_error_has_location: (query: unknown) => boolean;
  lattice_query_last_error_line: (query: unknown) => number;
  lattice_query_last_error_column: (query: unknown) => number;
  lattice_query_last_error_length: (query: unknown) => number;
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
 * Try to bind a function, returning a throwing stub if not found.
 * This allows graceful handling of functions not yet in the shared library.
 */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
function tryFunc(lib: ReturnType<typeof getLibrary>, name: string, ret: string, args: unknown[]): any {
  try {
    return lib.func(name, ret, args as string[]);
  } catch {
    return () => { throw new Error(`Function '${name}' is not available in the shared library`); };
  }
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function tryOptionalFunc(lib: ReturnType<typeof getLibrary>, name: string, ret: string, args: unknown[]): any | undefined {
  try {
    return lib.func(name, ret, args as string[]);
  } catch {
    return undefined;
  }
}

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
  const StreamBatchPtr = koffi.pointer(LatticeStreamBatch);
  const StreamBatchPtrPtr = koffi.pointer(StreamBatchPtr);
  const OpenOptionsPtr = koffi.pointer(LatticeOpenOptions);
  const OpenOptionsV2Ptr = koffi.pointer(LatticeOpenOptionsV2);
  const ValuePtr = koffi.pointer(LatticeValue);
  const ValueConstPtr = koffi.pointer(LatticeValue);
  const ValueConstPtrPtr = koffi.pointer(ValueConstPtr);
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
    lattice_open_v2: tryOptionalFunc(lib, 'lattice_open_v2', 'int', [
      'str', // path
      OpenOptionsV2Ptr, // options
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
    lattice_node_add_label: lib.func('lattice_node_add_label', 'int', [
      TxnPtr,
      'uint64', // node_id
      'str', // label
    ]),
    lattice_node_remove_label: lib.func('lattice_node_remove_label', 'int', [
      TxnPtr,
      'uint64', // node_id
      'str', // label
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
    lattice_node_get_labels: lib.func('lattice_node_get_labels', 'int', [
      TxnPtr,
      'uint64', // node_id
      koffi.out(koffi.pointer('void*')), // labels_out - raw pointer for manual free
    ]),
    lattice_get_nodes_by_label: lib.func('lattice_get_nodes_by_label', 'int', [
      DatabasePtr,
      'str', // label
      'size_t', // label_len
      koffi.out(koffi.pointer('void*')), // node_ids_out - raw pointer for manual free
      koffi.out(koffi.pointer('size_t')), // count_out
    ]),
    lattice_get_nodes_by_label_txn: lib.func('lattice_get_nodes_by_label_txn', 'int', [
      TxnPtr,
      'str', // label
      'size_t', // label_len
      koffi.out(koffi.pointer('void*')), // node_ids_out - raw pointer for manual free
      koffi.out(koffi.pointer('size_t')), // count_out
    ]),
    lattice_free_node_ids: lib.func('lattice_free_node_ids', 'void', [
      'void*',
      'size_t',
    ]),
    lattice_free_string: lib.func('lattice_free_string', 'void', ['void*']),
    lattice_value_free: lib.func('lattice_value_free', 'void', [ValuePtr]),
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
      ValuePtr, // value_out
    ]),
    lattice_stream_publish: lib.func('lattice_stream_publish', 'int', [
      TxnPtr,
      koffi.pointer('uint8_t'),
      'size_t',
      koffi.pointer('uint8_t'),
      'size_t',
      ValuePtr,
    ]),
    lattice_stream_read: lib.func('lattice_stream_read', 'int', [
      DatabasePtr,
      koffi.pointer('uint8_t'),
      'size_t',
      'uint64',
      'size_t',
      'uint32',
      koffi.out(StreamBatchPtrPtr),
    ]),
    lattice_stream_batch_count: lib.func('lattice_stream_batch_count', 'size_t', [
      StreamBatchPtr,
    ]),
    lattice_stream_batch_get: lib.func('lattice_stream_batch_get', 'int', [
      StreamBatchPtr,
      'size_t',
      koffi.out(koffi.pointer('uint64')),
      koffi.out(koffi.pointer('void*')),
      koffi.out(koffi.pointer('size_t')),
      koffi.out(ValueConstPtrPtr),
    ]),
    lattice_stream_batch_free: lib.func('lattice_stream_batch_free', 'void', [
      StreamBatchPtr,
    ]),
    lattice_stream_get_offset: lib.func('lattice_stream_get_offset', 'int', [
      DatabasePtr,
      koffi.pointer('uint8_t'),
      'size_t',
      koffi.pointer('uint8_t'),
      'size_t',
      koffi.out(koffi.pointer('bool')),
      koffi.out(koffi.pointer('uint64')),
    ]),
    lattice_stream_set_offset: lib.func('lattice_stream_set_offset', 'int', [
      TxnPtr,
      koffi.pointer('uint8_t'),
      'size_t',
      koffi.pointer('uint8_t'),
      'size_t',
      'uint64',
    ]),
    lattice_stream_trim: lib.func('lattice_stream_trim', 'int', [
      TxnPtr,
      koffi.pointer('uint8_t'),
      'size_t',
      'uint64',
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
    lattice_vector_search_txn: lib.func('lattice_vector_search_txn', 'int', [
      TxnPtr,
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
    lattice_fts_search_txn: lib.func('lattice_fts_search_txn', 'int', [
      TxnPtr,
      'str', // query
      'uintptr_t', // query_len (size_t)
      'uint32', // limit
      koffi.out(FtsResultPtrPtr), // result_out
    ]),
    lattice_fts_search_fuzzy: tryFunc(lib, 'lattice_fts_search_fuzzy', 'int', [
      DatabasePtr,
      'str', // query
      'uintptr_t', // query_len (size_t)
      'uint32', // limit
      'uint32', // max_distance
      'uint32', // min_term_length
      koffi.out(FtsResultPtrPtr), // result_out
    ]),
    lattice_fts_search_fuzzy_txn: tryFunc(lib, 'lattice_fts_search_fuzzy_txn', 'int', [
      TxnPtr,
      'str', // query
      'uintptr_t', // query_len (size_t)
      'uint32', // limit
      'uint32', // max_distance
      'uint32', // min_term_length
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
    lattice_edge_set_property: lib.func('lattice_edge_set_property', 'int', [
      TxnPtr,
      'uint64', // edge_id
      'str', // key
      ValuePtr, // value
    ]),
    lattice_edge_get_property: lib.func('lattice_edge_get_property', 'int', [
      TxnPtr,
      'uint64', // edge_id
      'str', // key
      ValuePtr, // value_out
    ]),
    lattice_edge_remove_property: lib.func('lattice_edge_remove_property', 'int', [
      TxnPtr,
      'uint64', // edge_id
      'str', // key
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
    lattice_edge_result_get_id: lib.func('lattice_edge_result_get_id', 'int', [
      EdgeResultPtr,
      'uint32', // index
      koffi.out(koffi.pointer('uint64')), // edge_id_out
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
    lattice_query_last_error_stage: lib.func('lattice_query_last_error_stage', 'int', [
      QueryPtr,
    ]),
    lattice_query_last_error_message: lib.func('lattice_query_last_error_message', 'str', [
      QueryPtr,
    ]),
    lattice_query_last_error_code: lib.func('lattice_query_last_error_code', 'str', [
      QueryPtr,
    ]),
    lattice_query_last_error_has_location: lib.func('lattice_query_last_error_has_location', 'bool', [
      QueryPtr,
    ]),
    lattice_query_last_error_line: lib.func('lattice_query_last_error_line', 'uint32', [
      QueryPtr,
    ]),
    lattice_query_last_error_column: lib.func('lattice_query_last_error_column', 'uint32', [
      QueryPtr,
    ]),
    lattice_query_last_error_length: lib.func('lattice_query_last_error_length', 'uint32', [
      QueryPtr,
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
      ValuePtr, // value_out
    ]),
    lattice_result_free: lib.func('lattice_result_free', 'void', [ResultPtr]),

    // Embedding operations (optional - may not be in all builds)
    lattice_hash_embed: tryFunc(lib, 'lattice_hash_embed', 'int', [
      'str', // text
      'uintptr_t', // text_len (size_t)
      'uint16', // dimensions
      koffi.out(koffi.pointer(koffi.pointer('float'))), // vector_out
      koffi.out(koffi.pointer('uint32')), // dims_out
    ]),
    lattice_hash_embed_free: tryFunc(lib, 'lattice_hash_embed_free', 'void', [
      koffi.pointer('float'), // vector
      'uint32', // dimensions
    ]),
    lattice_embedding_client_create: tryFunc(lib, 'lattice_embedding_client_create', 'int', [
      EmbeddingConfigPtr, // config
      koffi.out(EmbeddingClientPtrPtr), // client_out
    ]),
    lattice_embedding_client_embed: tryFunc(lib, 'lattice_embedding_client_embed', 'int', [
      EmbeddingClientPtr, // client
      'str', // text
      'uintptr_t', // text_len (size_t)
      koffi.out(koffi.pointer(koffi.pointer('float'))), // vector_out
      koffi.out(koffi.pointer('uint32')), // dims_out
    ]),
    lattice_embedding_client_free: tryFunc(lib, 'lattice_embedding_client_free', 'void', [
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
