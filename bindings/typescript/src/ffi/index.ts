/**
 * High-level FFI wrapper with error handling and type conversion.
 *
 * This module provides an ergonomic TypeScript interface to the raw C bindings.
 */

import koffi from 'koffi';
import { getBindings, LatticeBindings } from './bindings';
import {
  LatticeErrorCode,
  QueryErrorStage,
  LatticeTxnMode,
  LatticeValueType,
} from './types';

export { isLibraryAvailable, getLibraryPath } from './library';
export { LatticeErrorCode, QueryErrorStage, LatticeTxnMode, LatticeValueType } from './types';

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
 * Query error source location.
 */
export interface QueryErrorLocation {
  line: number;
  column: number;
  length: number;
}

/**
 * Rich query execution error with stage-aware diagnostics.
 */
export class LatticeQueryError extends LatticeError {
  constructor(
    message: string,
    code: LatticeErrorCode,
    public readonly stage: QueryErrorStage,
    public readonly diagnosticCode?: string,
    public readonly location?: QueryErrorLocation
  ) {
    super(message, code);
    this.name = 'LatticeQueryError';
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
  enableVectors?: boolean;
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

  private normalizeQueryStage(stage: number): QueryErrorStage {
    switch (stage) {
      case QueryErrorStage.Parse:
        return QueryErrorStage.Parse;
      case QueryErrorStage.Semantic:
        return QueryErrorStage.Semantic;
      case QueryErrorStage.Plan:
        return QueryErrorStage.Plan;
      case QueryErrorStage.Execution:
        return QueryErrorStage.Execution;
      default:
        return QueryErrorStage.None;
    }
  }

  /**
   * Check an error code for query execution and include detailed diagnostics.
   */
  private checkQueryError(code: number, query: QueryHandle): void {
    if (code === LatticeErrorCode.Ok) {
      return;
    }

    const stage = this.normalizeQueryStage(
      this.bindings.lattice_query_last_error_stage(query)
    );
    const detailMessage = this.bindings.lattice_query_last_error_message(query) ?? undefined;
    const detailCode = this.bindings.lattice_query_last_error_code(query) ?? undefined;

    let location: QueryErrorLocation | undefined;
    if (this.bindings.lattice_query_last_error_has_location(query)) {
      location = {
        line: this.bindings.lattice_query_last_error_line(query),
        column: this.bindings.lattice_query_last_error_column(query),
        length: this.bindings.lattice_query_last_error_length(query),
      };
    }

    if (stage !== QueryErrorStage.None || detailMessage !== undefined) {
      const fallbackMessage = this.bindings.lattice_error_message(code);
      throw new LatticeQueryError(
        detailMessage ?? fallbackMessage,
        code as LatticeErrorCode,
        stage,
        detailCode,
        location
      );
    }

    this.checkError(code);
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
      enable_vector: options.enableVectors ?? options.enableVector ?? false,
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
   * Add a label to an existing node.
   */
  addNodeLabel(txn: TransactionHandle, nodeId: bigint, label: string): void {
    const err = this.bindings.lattice_node_add_label(txn, nodeId, label);
    this.checkError(err);
  }

  /**
   * Remove a label from an existing node.
   */
  removeNodeLabel(txn: TransactionHandle, nodeId: bigint, label: string): void {
    const err = this.bindings.lattice_node_remove_label(txn, nodeId, label);
    this.checkError(err);
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
    try {
      return this.latticeValueToJs(valueOut);
    } finally {
      this.bindings.lattice_value_free(valueOut);
    }
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
        list_val: null,
        map_val: null,
      },
    };
  }

  private isPlainObject(value: unknown): value is Record<string, unknown> {
    if (value === null || typeof value !== 'object') {
      return false;
    }
    const proto = Object.getPrototypeOf(value);
    return proto === Object.prototype || proto === null;
  }

  private decodeListStruct(value: unknown): { items: unknown; len: number | bigint } {
    if (this.isPlainObject(value) && 'items' in value && 'len' in value) {
      return value as { items: unknown; len: number | bigint };
    }
    return koffi.decode(value as never, 'lattice_list') as { items: unknown; len: number | bigint };
  }

  private decodeMapStruct(value: unknown): { entries: unknown; len: number | bigint } {
    if (this.isPlainObject(value) && 'entries' in value && 'len' in value) {
      return value as { entries: unknown; len: number | bigint };
    }
    return koffi.decode(value as never, 'lattice_map') as { entries: unknown; len: number | bigint };
  }

  private decodeValueArray(value: unknown, len: number): Record<string, unknown>[] {
    if (Array.isArray(value)) {
      return value as Record<string, unknown>[];
    }
    if (!value || len === 0) {
      return [];
    }
    return koffi.decode(value as never, 'lattice_value', len) as Record<string, unknown>[];
  }

  private decodeMapEntryArray(value: unknown, len: number): Record<string, unknown>[] {
    if (Array.isArray(value)) {
      return value as Record<string, unknown>[];
    }
    if (!value || len === 0) {
      return [];
    }
    return koffi.decode(value as never, 'lattice_map_entry', len) as Record<string, unknown>[];
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
      return { type: LatticeValueType.Int, data: { int_val: value } };
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
    if (Array.isArray(value)) {
      const items = value.map((item) => this.jsToLatticeValue(item));
      return {
        type: LatticeValueType.List,
        data: {
          list_val: {
            items,
            len: items.length,
          },
        },
      };
    }
    if (this.isPlainObject(value)) {
      const entries = Object.entries(value).map(([key, entryValue]) => {
        const keyBuffer = Buffer.from(key, 'utf8');
        return {
          key: keyBuffer,
          key_len: keyBuffer.length,
          value: this.jsToLatticeValue(entryValue),
        };
      });
      return {
        type: LatticeValueType.Map,
        data: {
          map_val: {
            entries,
            len: entries.length,
          },
        },
      };
    }
    throw new TypeError(`Unsupported value type: ${typeof value}`);
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
        if (!sv.ptr) return '';
        // koffi may decode const char* directly as a JS string
        if (typeof sv.ptr === 'string') return sv.ptr;
        const len = Number(sv.len);
        if (len === 0) return '';
        return koffi.decode(sv.ptr, 'char', len) as string;
      }
      case LatticeValueType.Bytes: {
        const bv = data.bytes_val as { ptr: unknown; len: number | bigint };
        const len = Number(bv.len);
        if (!bv.ptr || len === 0) return new Uint8Array(0);
        const bytes = koffi.decode(bv.ptr, 'uint8_t', len) as number[];
        return Uint8Array.from(bytes);
      }
      case LatticeValueType.Vector: {
        const vv = data.vector_val as { ptr: unknown; dimensions: number | bigint };
        const dims = Number(vv.dimensions);
        if (!vv.ptr || dims === 0) return new Float32Array(0);
        const floats = koffi.decode(vv.ptr, 'float', dims) as number[];
        return new Float32Array(floats);
      }
      case LatticeValueType.List: {
        const listVal = this.decodeListStruct(data.list_val);
        const len = Number(listVal.len);
        const items = this.decodeValueArray(listVal.items, len);
        return items.map((item) => this.latticeValueToJs(item));
      }
      case LatticeValueType.Map: {
        const mapVal = this.decodeMapStruct(data.map_val);
        const len = Number(mapVal.len);
        const entries = this.decodeMapEntryArray(mapVal.entries, len);
        const result: Record<string, unknown> = {};
        for (const entry of entries) {
          const keyPtr = entry.key;
          const keyLen = Number(entry.key_len as number | bigint);
          const key = !keyPtr || keyLen === 0
            ? ''
            : koffi.decode(keyPtr as never, 'char', keyLen) as string;
          result[key] = this.latticeValueToJs(entry.value as Record<string, unknown>);
        }
        return result;
      }
      default:
        throw new LatticeError(
          `Unsupported native value type: ${type}`,
          LatticeErrorCode.Unsupported
        );
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
   * Set a property on an edge.
   */
  setEdgeProperty(
    txn: TransactionHandle,
    edgeId: bigint,
    key: string,
    value: unknown
  ): void {
    const latticeValue = this.jsToLatticeValue(value);
    const err = this.bindings.lattice_edge_set_property(txn, edgeId, key, latticeValue);
    this.checkError(err);
  }

  /**
   * Get a property from an edge.
   */
  getEdgeProperty(
    txn: TransactionHandle,
    edgeId: bigint,
    key: string
  ): unknown {
    const valueOut = this.makeEmptyValue();
    const err = this.bindings.lattice_edge_get_property(txn, edgeId, key, valueOut);
    if (err === LatticeErrorCode.NotFound) {
      return null;
    }
    this.checkError(err);
    try {
      return this.latticeValueToJs(valueOut);
    } finally {
      this.bindings.lattice_value_free(valueOut);
    }
  }

  /**
   * Remove a property from an edge.
   */
  removeEdgeProperty(
    txn: TransactionHandle,
    edgeId: bigint,
    key: string
  ): void {
    const err = this.bindings.lattice_edge_remove_property(txn, edgeId, key);
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
   * Get an edge result ID by index.
   */
  edgeResultGetId(
    result: EdgeResultHandle,
    index: number
  ): bigint {
    const edgeIdOut = Buffer.alloc(8);
    const err = this.bindings.lattice_edge_result_get_id(result, index, edgeIdOut);
    this.checkError(err);
    return edgeIdOut.readBigUInt64LE();
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
    this.checkQueryError(err, query);
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
