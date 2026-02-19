/**
 * High-level FFI wrapper with error handling and type conversion.
 *
 * This module provides an ergonomic TypeScript interface to the raw C bindings.
 */

import koffi from 'koffi';
import { getBindings, LatticeBindings } from './bindings';
import { isLibraryAvailable, getLibraryPath } from './library';
import {
  LatticeErrorCode,
  LatticeTxnMode,
  LatticeValueType,
} from './types';

export { isLibraryAvailable, getLibraryPath } from './library';
export { LatticeErrorCode, LatticeTxnMode, LatticeValueType } from './types';

/**
 * Error class for Lattice operations.
 */
export class LatticeError extends Error {
  constructor(
    message: string,
    public readonly code: LatticeErrorCode
  ) {
    super(message);
    this.name = 'LatticeError';
  }
}

/**
 * Opaque handle to a database.
 */
export type DatabaseHandle = unknown;

/**
 * Opaque handle to a transaction.
 */
export type TransactionHandle = unknown;

/**
 * Opaque handle to a query.
 */
export type QueryHandle = unknown;

/**
 * Opaque handle to a result set.
 */
export type ResultHandle = unknown;

/**
 * Opaque handle to vector search results.
 */
export type VectorResultHandle = unknown;

/**
 * Opaque handle to FTS results.
 */
export type FtsResultHandle = unknown;

/**
 * Opaque handle to edge results.
 */
export type EdgeResultHandle = unknown;

/**
 * Opaque handle to an embedding client.
 */
export type EmbeddingClientHandle = unknown;

/**
 * Options for opening a database.
 */
export interface OpenOptions {
  create?: boolean;
  readOnly?: boolean;
  cacheSizeMb?: number;
  pageSize?: number;
  enableVector?: boolean;
  vectorDimensions?: number;
}

/**
 * High-level FFI wrapper class.
 */
export class LatticeFFI {
  private bindings: LatticeBindings;

  constructor() {
    this.bindings = getBindings();
  }

  /**
   * Check an error code and throw if not OK.
   */
  private checkError(code: number): void {
    if (code !== LatticeErrorCode.Ok) {
      const message = this.bindings.lattice_error_message(code);
      throw new LatticeError(message, code as LatticeErrorCode);
    }
  }

  /**
   * Get the library version.
   */
  version(): string {
    return this.bindings.lattice_version();
  }

  // ============================================================
  // Database operations
  // ============================================================

  /**
   * Open a database.
   */
  open(path: string, options: OpenOptions = {}): DatabaseHandle {
    const opts = {
      create: options.create ?? false,
      read_only: options.readOnly ?? false,
      cache_size_mb: options.cacheSizeMb ?? 100,
      page_size: options.pageSize ?? 4096,
      enable_vector: options.enableVector ?? false,
      vector_dimensions: options.vectorDimensions ?? 128,
    };

    const dbOut: unknown[] = [null];
    const err = this.bindings.lattice_open(path, opts, dbOut);
    this.checkError(err);
    return dbOut[0];
  }

  /**
   * Close a database.
   */
  close(db: DatabaseHandle): void {
    const err = this.bindings.lattice_close(db);
    this.checkError(err);
  }

  // ============================================================
  // Transaction operations
  // ============================================================

  /**
   * Begin a transaction.
   */
  begin(db: DatabaseHandle, readOnly: boolean): TransactionHandle {
    const mode = readOnly ? LatticeTxnMode.ReadOnly : LatticeTxnMode.ReadWrite;
    const txnOut: unknown[] = [null];
    const err = this.bindings.lattice_begin(db, mode, txnOut);
    this.checkError(err);
    return txnOut[0];
  }

  /**
   * Commit a transaction.
   */
  commit(txn: TransactionHandle): void {
    const err = this.bindings.lattice_commit(txn);
    this.checkError(err);
  }

  /**
   * Rollback a transaction.
   */
  rollback(txn: TransactionHandle): void {
    const err = this.bindings.lattice_rollback(txn);
    this.checkError(err);
  }

  // ============================================================
  // Node operations
  // ============================================================

  /**
   * Create a node with an optional label.
   */
  createNode(txn: TransactionHandle, label?: string): bigint {
    const nodeOut = Buffer.alloc(8);
    const err = this.bindings.lattice_node_create(txn, label ?? null, nodeOut);
    this.checkError(err);
    return nodeOut.readBigUInt64LE();
  }

  /**
   * Delete a node.
   */
  deleteNode(txn: TransactionHandle, nodeId: bigint): void {
    const err = this.bindings.lattice_node_delete(txn, nodeId);
    this.checkError(err);
  }

  /**
   * Check if a node exists.
   */
  nodeExists(txn: TransactionHandle, nodeId: bigint): boolean {
    const existsOut = Buffer.alloc(1);
    const err = this.bindings.lattice_node_exists(txn, nodeId, existsOut);
    this.checkError(err);
    return existsOut.readUInt8() !== 0;
  }

  /**
   * Get labels for a node.
   */
  getNodeLabels(txn: TransactionHandle, nodeId: bigint): string[] {
    const labelsOut: unknown[] = [null];
    const err = this.bindings.lattice_node_get_labels(txn, nodeId, labelsOut);
    this.checkError(err);

    const ptr = labelsOut[0];
    if (!ptr) {
      return [];
    }

    try {
      const labelsStr = koffi.decode(ptr, 'char', -1) as string;
      if (!labelsStr || labelsStr.length === 0) {
        return [];
      }
      return labelsStr.split(',');
    } finally {
      this.bindings.lattice_free_string(ptr);
    }
  }

  /**
   * Set a vector on a node.
   */
  setVector(
    txn: TransactionHandle,
    nodeId: bigint,
    key: string,
    vector: Float32Array
  ): void {
    const err = this.bindings.lattice_node_set_vector(
      txn,
      nodeId,
      key,
      vector,
      vector.length
    );
    this.checkError(err);
  }

  /**
   * Batch insert multiple nodes with vectors.
   */
  batchInsert(
    txn: TransactionHandle,
    nodes: Array<{ label: string; vector: Float32Array }>
  ): bigint[] {
    const count = nodes.length;
    if (count === 0) {
      return [];
    }

    // Build C array of NodeWithVector structs
    const specs = nodes.map((n) => ({
      label: n.label,
      vector: n.vector,
      dimensions: n.vector.length,
    }));

    // Allocate output buffers
    const nodeIdsOut = Buffer.alloc(count * 8); // uint64 per node
    const countOut = Buffer.alloc(4); // uint32

    const err = this.bindings.lattice_batch_insert(
      txn,
      specs,
      count,
      nodeIdsOut,
      countOut
    );
    this.checkError(err);

    const created = countOut.readUInt32LE();
    if (created < count) {
      throw new LatticeError(
        `Batch insert partially failed: ${created}/${count} nodes created. Transaction should be rolled back.`,
        LatticeErrorCode.Error
      );
    }

    const ids: bigint[] = [];
    for (let i = 0; i < created; i++) {
      ids.push(nodeIdsOut.readBigUInt64LE(i * 8));
    }
    return ids;
  }

  // ============================================================
  // Property operations
  // ============================================================

  /**
   * Set a property on a node.
   */
  setProperty(
    txn: TransactionHandle,
    nodeId: bigint,
    key: string,
    value: unknown
  ): void {
    const latticeValue = this.jsToLatticeValue(value);
    const err = this.bindings.lattice_node_set_property(txn, nodeId, key, latticeValue);
    this.checkError(err);
  }

  /**
   * Get a property from a node.
   */
  getProperty(
    txn: TransactionHandle,
    nodeId: bigint,
    key: string
  ): unknown {
    const valueOut = this.makeEmptyValue();
    const err = this.bindings.lattice_node_get_property(txn, nodeId, key, valueOut);
    this.checkError(err);
    return this.latticeValueToJs(valueOut);
  }

  /**
   * Get a value from a result set column.
   */
  resultGet(result: ResultHandle, index: number): unknown {
    const valueOut = this.makeEmptyValue();
    const err = this.bindings.lattice_result_get(result, index, valueOut);
    this.checkError(err);
    return this.latticeValueToJs(valueOut);
  }

  /**
   * Bind a parameter to a query.
   */
  queryBind(query: QueryHandle, name: string, value: unknown): void {
    const latticeValue = this.jsToLatticeValue(value);
    const err = this.bindings.lattice_query_bind(query, name, latticeValue);
    this.checkError(err);
  }

  /**
   * Create an empty LatticeValue for use as an out parameter.
   * koffi needs all union fields present to decode output properly.
   */
  private makeEmptyValue(): Record<string, unknown> {
    return {
      type: 0,
      data: {
        bool_val: false,
        int_val: BigInt(0),
        float_val: 0.0,
        string_val: { ptr: null, len: 0 },
        bytes_val: { ptr: null, len: 0 },
        vector_val: { ptr: null, dimensions: 0 },
      },
    };
  }

  /**
   * Convert a JS value to a LatticeValue struct for FFI.
   * koffi unions require an object with a single property naming the active variant.
   */
  private jsToLatticeValue(value: unknown): Record<string, unknown> {
    if (value === null || value === undefined) {
      return { type: LatticeValueType.Null, data: { int_val: 0 } };
    }
    if (typeof value === 'boolean') {
      return { type: LatticeValueType.Bool, data: { bool_val: value } };
    }
    if (typeof value === 'number') {
      if (Number.isInteger(value)) {
        return { type: LatticeValueType.Int, data: { int_val: value } };
      }
      return { type: LatticeValueType.Float, data: { float_val: value } };
    }
    if (typeof value === 'bigint') {
      return { type: LatticeValueType.Int, data: { int_val: Number(value) } };
    }
    if (typeof value === 'string') {
      const buf = Buffer.from(value, 'utf8');
      return { type: LatticeValueType.String, data: { string_val: { ptr: buf, len: buf.length } } };
    }
    if (value instanceof Float32Array) {
      return { type: LatticeValueType.Vector, data: { vector_val: { ptr: value, dimensions: value.length } } };
    }
    if (value instanceof Uint8Array) {
      return { type: LatticeValueType.Bytes, data: { bytes_val: { ptr: value, len: value.length } } };
    }
    // Default: convert to string
    const str = String(value);
    const strBuf = Buffer.from(str, 'utf8');
    return { type: LatticeValueType.String, data: { string_val: { ptr: strBuf, len: strBuf.length } } };
  }

  /**
   * Convert a LatticeValue struct from FFI to a JS value.
   */
  private latticeValueToJs(val: Record<string, unknown>): unknown {
    const type = val.type as number;
    const data = val.data as Record<string, unknown>;

    switch (type) {
      case LatticeValueType.Null:
        return null;
      case LatticeValueType.Bool:
        return data.bool_val as boolean;
      case LatticeValueType.Int: {
        // koffi returns int64 as BigInt; preserve BigInt for consistency
        const iv = data.int_val;
        return typeof iv === 'bigint' ? iv : BigInt(iv as number);
      }
      case LatticeValueType.Float:
        return data.float_val as number;
      case LatticeValueType.String: {
        const sv = data.string_val as { ptr: unknown; len: number | bigint };
        if (!sv.ptr) return null;
        // koffi may decode const char* directly as a JS string
        if (typeof sv.ptr === 'string') return sv.ptr;
        const len = Number(sv.len);
        if (len === 0) return '';
        return koffi.decode(sv.ptr, 'char', len) as string;
      }
      default:
        return null;
    }
  }

  // ============================================================
  // Vector search operations
  // ============================================================

  /**
   * Search for similar vectors.
   */
  vectorSearch(
    db: DatabaseHandle,
    vector: Float32Array,
    k: number,
    efSearch: number = 0
  ): VectorResultHandle {
    const resultOut: unknown[] = [null];
    const err = this.bindings.lattice_vector_search(
      db,
      vector,
      vector.length,
      k,
      efSearch,
      resultOut
    );
    this.checkError(err);
    return resultOut[0];
  }

  /**
   * Get the count of vector search results.
   */
  vectorResultCount(result: VectorResultHandle): number {
    return this.bindings.lattice_vector_result_count(result);
  }

  /**
   * Get a vector search result by index.
   */
  vectorResultGet(
    result: VectorResultHandle,
    index: number
  ): { nodeId: bigint; distance: number } {
    const nodeIdOut = Buffer.alloc(8);
    const distanceOut = Buffer.alloc(4);
    const err = this.bindings.lattice_vector_result_get(
      result,
      index,
      nodeIdOut,
      distanceOut
    );
    this.checkError(err);
    return {
      nodeId: nodeIdOut.readBigUInt64LE(),
      distance: distanceOut.readFloatLE(),
    };
  }

  /**
   * Free vector search results.
   */
  vectorResultFree(result: VectorResultHandle): void {
    this.bindings.lattice_vector_result_free(result);
  }

  // ============================================================
  // Full-text search operations
  // ============================================================

  /**
   * Index a document for full-text search.
   */
  ftsIndex(txn: TransactionHandle, nodeId: bigint, text: string): void {
    const err = this.bindings.lattice_fts_index(txn, nodeId, text, Buffer.byteLength(text, 'utf8'));
    this.checkError(err);
  }

  /**
   * Search for documents.
   */
  ftsSearch(
    db: DatabaseHandle,
    query: string,
    limit: number
  ): FtsResultHandle {
    const resultOut: unknown[] = [null];
    const err = this.bindings.lattice_fts_search(
      db,
      query,
      Buffer.byteLength(query, 'utf8'),
      limit,
      resultOut
    );
    this.checkError(err);
    return resultOut[0];
  }

  /**
   * Search for documents with fuzzy matching.
   */
  ftsSearchFuzzy(
    db: DatabaseHandle,
    query: string,
    limit: number,
    maxDistance: number = 0,
    minTermLength: number = 0
  ): FtsResultHandle {
    const resultOut: unknown[] = [null];
    const err = this.bindings.lattice_fts_search_fuzzy(
      db,
      query,
      Buffer.byteLength(query, 'utf8'),
      limit,
      maxDistance,
      minTermLength,
      resultOut
    );
    this.checkError(err);
    return resultOut[0];
  }

  /**
   * Get the count of FTS results.
   */
  ftsResultCount(result: FtsResultHandle): number {
    return this.bindings.lattice_fts_result_count(result);
  }

  /**
   * Get an FTS result by index.
   */
  ftsResultGet(
    result: FtsResultHandle,
    index: number
  ): { nodeId: bigint; score: number } {
    const nodeIdOut = Buffer.alloc(8);
    const scoreOut = Buffer.alloc(4);
    const err = this.bindings.lattice_fts_result_get(
      result,
      index,
      nodeIdOut,
      scoreOut
    );
    this.checkError(err);
    return {
      nodeId: nodeIdOut.readBigUInt64LE(),
      score: scoreOut.readFloatLE(),
    };
  }

  /**
   * Free FTS results.
   */
  ftsResultFree(result: FtsResultHandle): void {
    this.bindings.lattice_fts_result_free(result);
  }

  // ============================================================
  // Edge operations
  // ============================================================

  /**
   * Create an edge between two nodes.
   */
  createEdge(
    txn: TransactionHandle,
    source: bigint,
    target: bigint,
    edgeType: string
  ): bigint {
    const edgeOut = Buffer.alloc(8);
    const err = this.bindings.lattice_edge_create(
      txn,
      source,
      target,
      edgeType,
      edgeOut
    );
    this.checkError(err);
    return edgeOut.readBigUInt64LE();
  }

  /**
   * Delete an edge.
   */
  deleteEdge(
    txn: TransactionHandle,
    source: bigint,
    target: bigint,
    edgeType: string
  ): void {
    const err = this.bindings.lattice_edge_delete(txn, source, target, edgeType);
    this.checkError(err);
  }

  /**
   * Get outgoing edges from a node.
   */
  getOutgoingEdges(txn: TransactionHandle, nodeId: bigint): EdgeResultHandle {
    const resultOut: unknown[] = [null];
    const err = this.bindings.lattice_edge_get_outgoing(txn, nodeId, resultOut);
    this.checkError(err);
    return resultOut[0];
  }

  /**
   * Get incoming edges to a node.
   */
  getIncomingEdges(txn: TransactionHandle, nodeId: bigint): EdgeResultHandle {
    const resultOut: unknown[] = [null];
    const err = this.bindings.lattice_edge_get_incoming(txn, nodeId, resultOut);
    this.checkError(err);
    return resultOut[0];
  }

  /**
   * Get the count of edge results.
   */
  edgeResultCount(result: EdgeResultHandle): number {
    return this.bindings.lattice_edge_result_count(result);
  }

  /**
   * Get an edge result by index.
   */
  edgeResultGet(
    result: EdgeResultHandle,
    index: number
  ): { source: bigint; target: bigint; edgeType: string } {
    const sourceOut = Buffer.alloc(8);
    const targetOut = Buffer.alloc(8);
    const edgeTypeOut: unknown[] = [null];
    const edgeTypeLenOut = Buffer.alloc(4);

    const err = this.bindings.lattice_edge_result_get(
      result,
      index,
      sourceOut,
      targetOut,
      edgeTypeOut,
      edgeTypeLenOut
    );
    this.checkError(err);

    return {
      source: sourceOut.readBigUInt64LE(),
      target: targetOut.readBigUInt64LE(),
      edgeType: edgeTypeOut[0] as string,
    };
  }

  /**
   * Free edge results.
   */
  edgeResultFree(result: EdgeResultHandle): void {
    this.bindings.lattice_edge_result_free(result);
  }

  // ============================================================
  // Query operations
  // ============================================================

  /**
   * Prepare a Cypher query.
   */
  queryPrepare(db: DatabaseHandle, cypher: string): QueryHandle {
    const queryOut: unknown[] = [null];
    const err = this.bindings.lattice_query_prepare(db, cypher, queryOut);
    this.checkError(err);
    return queryOut[0];
  }

  /**
   * Bind a vector parameter to a query.
   */
  queryBindVector(
    query: QueryHandle,
    name: string,
    vector: Float32Array
  ): void {
    const err = this.bindings.lattice_query_bind_vector(
      query,
      name,
      vector,
      vector.length
    );
    this.checkError(err);
  }

  /**
   * Execute a prepared query.
   */
  queryExecute(query: QueryHandle, txn: TransactionHandle): ResultHandle {
    const resultOut: unknown[] = [null];
    const err = this.bindings.lattice_query_execute(query, txn, resultOut);
    this.checkError(err);
    return resultOut[0];
  }

  /**
   * Free a prepared query.
   */
  queryFree(query: QueryHandle): void {
    this.bindings.lattice_query_free(query);
  }

  /**
   * Clear the query cache.
   */
  cacheClear(db: DatabaseHandle): void {
    const err = this.bindings.lattice_query_cache_clear(db);
    this.checkError(err);
  }

  /**
   * Get query cache statistics.
   */
  cacheStats(db: DatabaseHandle): { entries: number; hits: number; misses: number } {
    const entriesOut = Buffer.alloc(4);
    const hitsOut = Buffer.alloc(8);
    const missesOut = Buffer.alloc(8);
    const err = this.bindings.lattice_query_cache_stats(db, entriesOut, hitsOut, missesOut);
    this.checkError(err);
    return {
      entries: entriesOut.readUInt32LE(),
      hits: Number(hitsOut.readBigUInt64LE()),
      misses: Number(missesOut.readBigUInt64LE()),
    };
  }

  // ============================================================
  // Result operations
  // ============================================================

  /**
   * Advance to the next row in results.
   */
  resultNext(result: ResultHandle): boolean {
    return this.bindings.lattice_result_next(result);
  }

  /**
   * Get the column count.
   */
  resultColumnCount(result: ResultHandle): number {
    return this.bindings.lattice_result_column_count(result);
  }

  /**
   * Get a column name.
   */
  resultColumnName(result: ResultHandle, index: number): string {
    return this.bindings.lattice_result_column_name(result, index);
  }

  /**
   * Free a result set.
   */
  resultFree(result: ResultHandle): void {
    this.bindings.lattice_result_free(result);
  }

  // ============================================================
  // Embedding operations
  // ============================================================

  /**
   * Generate a hash embedding (built-in, no external service).
   */
  hashEmbed(text: string, dimensions: number): Float32Array {
    const vectorOut: unknown[] = [null];
    const dimsOut = Buffer.alloc(4);
    const err = this.bindings.lattice_hash_embed(
      text,
      Buffer.byteLength(text, 'utf8'),
      dimensions,
      vectorOut,
      dimsOut
    );
    this.checkError(err);

    const dims = dimsOut.readUInt32LE();
    const ptr = vectorOut[0];

    try {
      const floats = koffi.decode(ptr, 'float', dims) as number[];
      return new Float32Array(floats);
    } finally {
      this.bindings.lattice_hash_embed_free(ptr, dims);
    }
  }

  /**
   * Create an HTTP embedding client.
   */
  embeddingClientCreate(config: {
    endpoint: string;
    model?: string;
    apiFormat?: number;
    apiKey?: string | null;
    timeoutMs?: number;
  }): EmbeddingClientHandle {
    const cConfig = {
      endpoint: config.endpoint,
      model: config.model ?? 'nomic-embed-text',
      api_format: config.apiFormat ?? 0,
      api_key: config.apiKey ?? null,
      timeout_ms: config.timeoutMs ?? 0,
    };

    const clientOut: unknown[] = [null];
    const err = this.bindings.lattice_embedding_client_create(cConfig, clientOut);
    this.checkError(err);
    return clientOut[0];
  }

  /**
   * Generate an embedding via an HTTP embedding client.
   */
  embeddingClientEmbed(client: EmbeddingClientHandle, text: string): Float32Array {
    const vectorOut: unknown[] = [null];
    const dimsOut = Buffer.alloc(4);
    const err = this.bindings.lattice_embedding_client_embed(
      client,
      text,
      Buffer.byteLength(text, 'utf8'),
      vectorOut,
      dimsOut
    );
    this.checkError(err);

    const dims = dimsOut.readUInt32LE();
    const ptr = vectorOut[0];

    try {
      const floats = koffi.decode(ptr, 'float', dims) as number[];
      return new Float32Array(floats);
    } finally {
      this.bindings.lattice_hash_embed_free(ptr, dims);
    }
  }

  /**
   * Free an HTTP embedding client.
   */
  embeddingClientFree(client: EmbeddingClientHandle): void {
    this.bindings.lattice_embedding_client_free(client);
  }
}

// Singleton instance
let _ffi: LatticeFFI | null = null;

/**
 * Get the FFI instance (lazily initialized).
 */
export function getFFI(): LatticeFFI {
  if (!_ffi) {
    _ffi = new LatticeFFI();
  }
  return _ffi;
}
