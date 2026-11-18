/**
 * Transaction class for Lattice TypeScript bindings.
 */

import {
  Node,
  Edge,
  PropertyValue,
  CreateNodeOptions,
  CreateEdgeOptions,
  DeleteNodeOptions,
  QueryResult,
  VectorSearchOptions,
  VectorSearchResult,
  FtsSearchOptions,
} from './types';
import {
  LatticeFFI,
  LatticeError,
  LatticeErrorCode,
  DatabaseHandle,
  TransactionHandle,
} from './ffi';

/**
 * The properties a create call stores: a null (or undefined) value stores
 * nothing, as in Cypher, so the returned node or edge leaves it out too.
 */
function withoutNulls(properties: Record<string, PropertyValue>): Record<string, PropertyValue> {
  const stored: Record<string, PropertyValue> = {};
  for (const [key, value] of Object.entries(properties)) {
    if (value !== null && value !== undefined) stored[key] = value;
  }
  return stored;
}

/**
 * A database transaction.
 *
 * Transactions provide atomic, isolated access to the database.
 * Use `Database.read()` or `Database.write()` to create transactions.
 */
export class Transaction {
  private ffi: LatticeFFI;
  private readonly dbHandle: DatabaseHandle | null;
  private txnHandle: TransactionHandle | null;
  private readonly readOnly: boolean;
  private committed = false;
  private rolledBack = false;

  /**
   * Create a new transaction wrapper.
   * @internal
   */
  constructor(
    ffi: LatticeFFI,
    txnHandle: TransactionHandle,
    readOnly: boolean,
    dbHandle?: DatabaseHandle
  ) {
    this.ffi = ffi;
    this.dbHandle = dbHandle ?? null;
    this.txnHandle = txnHandle;
    this.readOnly = readOnly;
  }

  /**
   * Commit the transaction.
   *
   * If the commit fails with code -7 (TxnAborted), the native transaction is
   * already gone and this one is over: rolling it back afterwards does
   * nothing. After any other failure it stays open for `rollback()`.
   */
  commit(): void {
    if (this.committed) {
      throw new Error('Transaction already committed');
    }
    if (this.rolledBack) {
      throw new Error('Transaction already rolled back');
    }
    if (this.txnHandle === null) {
      throw new Error('Transaction closed');
    }
    try {
      this.ffi.commit(this.txnHandle);
    } catch (error) {
      if (error instanceof LatticeError && error.code === LatticeErrorCode.TxnAborted) {
        // The native side has retired the handle; it cannot be committed or
        // rolled back again, so the transaction ended without committing.
        this.txnHandle = null;
        this.rolledBack = true;
      }
      throw error;
    }
    this.txnHandle = null;
    this.committed = true;
  }

  /**
   * Rollback the transaction.
   */
  rollback(): void {
    if (this.committed) {
      throw new Error('Transaction already committed');
    }
    if (this.rolledBack || this.txnHandle === null) {
      return;
    }
    this.ffi.rollback(this.txnHandle);
    this.txnHandle = null;
    this.rolledBack = true;
  }

  publishStream(stream: string, payload: PropertyValue, kind = 'message'): void {
    this.ensureWritable();
    this.ffi.publishStream(this.txnHandle!, stream, payload, kind);
  }

  publishStreamGetSequence(stream: string, payload: PropertyValue, kind = 'message'): bigint {
    this.ensureWritable();
    return this.ffi.publishStreamGetSequence(this.txnHandle!, stream, payload, kind);
  }

  setStreamOffset(stream: string, consumer: string, sequence: bigint): void {
    this.ensureWritable();
    this.ffi.setStreamOffset(this.txnHandle!, stream, consumer, sequence);
  }

  trimStream(stream: string, throughSequence: bigint): void {
    this.ensureWritable();
    this.ffi.trimStream(this.txnHandle!, stream, throughSequence);
  }

  /**
   * Create a new node.
   *
   * @param options - Node creation options
   * @returns The created node
   */
  async createNode(options: CreateNodeOptions = {}): Promise<Node> {
    this.ensureWritable();

    const labels = options.labels ?? [];
    const properties = options.properties ?? {};

    const nodeId =
      labels.length === 1
        ? this.ffi.createNode(this.txnHandle!, labels[0])
        : this.ffi.createNode(this.txnHandle!);

    if (labels.length > 1) {
      for (const label of labels) {
        this.ffi.addNodeLabel(this.txnHandle!, nodeId, label);
      }
    }

    // Set properties
    for (const [key, value] of Object.entries(properties)) {
      this.ffi.setProperty(this.txnHandle!, nodeId, key, value);
    }

    return {
      id: nodeId,
      labels,
      properties: withoutNulls(properties),
    };
  }

  /**
   * Delete a node.
   *
   * A node that still has relationships is not deleted unless `detach` is set,
   * since deleting it alone would leave edges pointing at nothing; this matches
   * Cypher's DELETE and DETACH DELETE. Deleting a node that does not exist does
   * nothing.
   *
   * @param nodeId - ID of the node to delete
   * @param options - `detach: true` also deletes the node's relationships
   * @throws LatticeError with code -17 (HasRelationships) if the node has
   *   relationships this transaction can see and `detach` is not set; nothing
   *   is deleted
   */
  async deleteNode(nodeId: bigint, options: DeleteNodeOptions = {}): Promise<void> {
    this.ensureWritable();
    if (options.detach) {
      this.ffi.detachDeleteNode(this.txnHandle!, nodeId);
    } else {
      this.ffi.deleteNode(this.txnHandle!, nodeId);
    }
  }

  /**
   * Check if a node exists.
   *
   * @param nodeId - The node ID
   * @returns True if the node exists
   */
  async nodeExists(nodeId: bigint): Promise<boolean> {
    this.ensureActive();
    return this.ffi.nodeExists(this.txnHandle!, nodeId);
  }

  /**
   * Get a node by ID.
   *
   * Note: The returned node's `properties` will be empty. Use
   * `getProperty()` to fetch individual properties by key.
   *
   * @param nodeId - The node ID
   * @returns The node, or null if not found
   */
  async getNode(nodeId: bigint): Promise<Node | null> {
    this.ensureActive();

    if (!this.ffi.nodeExists(this.txnHandle!, nodeId)) {
      return null;
    }

    const labels = this.ffi.getNodeLabels(this.txnHandle!, nodeId);

    return {
      id: nodeId,
      labels,
      properties: {},
    };
  }

  /**
   * Set a property on a node.
   *
   * A null value removes the property, as Cypher's `SET n.x = null` does;
   * removing one the node does not have does nothing. (Up to 0.18.0 it
   * stored the null.)
   *
   * @param nodeId - The node ID
   * @param key - Property key
   * @param value - Property value
   */
  async setProperty(
    nodeId: bigint,
    key: string,
    value: PropertyValue
  ): Promise<void> {
    this.ensureWritable();
    this.ffi.setProperty(this.txnHandle!, nodeId, key, value);
  }

  /**
   * Get a property from a node.
   *
   * @param nodeId - The node ID
   * @param key - Property key
   * @returns The property value, or null if not found
   */
  async getProperty(nodeId: bigint, key: string): Promise<PropertyValue> {
    this.ensureActive();
    return this.ffi.getProperty(this.txnHandle!, nodeId, key) as PropertyValue;
  }

  /**
   * Set a vector embedding on a node.
   *
   * @param nodeId - The node ID
   * @param key - Vector key: `'embedding'` or `''`
   * @param vector - Vector data
   * @throws LatticeError with code -14 (Unsupported) if the database was
   *   created without a vector index, or the key is another name
   */
  async setVector(
    nodeId: bigint,
    key: string,
    vector: Float32Array
  ): Promise<void> {
    this.ensureWritable();
    this.ffi.setVector(this.txnHandle!, nodeId, key, vector);
  }

  /**
   * Get a node's vector as this transaction sees it.
   *
   * Under the cosine metric, the default, a committed vector reads back
   * scaled to unit length: `[3, 4]` reads back as `[0.6, 0.8]`. The
   * direction, and so every cosine distance, is unchanged. Under the
   * euclidean and dot-product metrics it reads back as stored. A vector set
   * earlier in this transaction reads back as written.
   *
   * @param nodeId - The node ID
   * @param key - Vector key: `'embedding'` (the default) or `''`
   * @returns The vector, or null if the node has none
   * @throws LatticeError with code -4 (NotFound) if the node does not exist
   */
  async getVector(nodeId: bigint, key: string = 'embedding'): Promise<Float32Array | null> {
    this.ensureActive();
    return this.ffi.getVector(this.txnHandle!, nodeId, key);
  }

  /**
   * Remove a node's vector. The node keeps its labels and properties; vector
   * search no longer returns it. Removing a vector the node does not have
   * does nothing.
   *
   * @param nodeId - The node ID
   * @param key - Vector key: `'embedding'` (the default) or `''`
   * @throws LatticeError with code -4 (NotFound) if the node does not exist
   */
  async removeVector(nodeId: bigint, key: string = 'embedding'): Promise<void> {
    this.ensureWritable();
    this.ffi.removeVector(this.txnHandle!, nodeId, key);
  }

  /**
   * Insert multiple vector-bearing nodes in a single call.
   *
   * @param label - Label for all nodes
   * @param vectors - Array of vectors (one per node)
   * @returns Array of created node IDs
   * @throws LatticeError with code -14 (Unsupported) if the database was
   *   created without a vector index; no node is created
   */
  async batchInsertVectors(
    label: string,
    vectors: Float32Array[]
  ): Promise<bigint[]> {
    this.ensureWritable();
    const nodes = vectors.map((v) => ({ label, vector: v }));
    return this.ffi.batchInsert(this.txnHandle!, nodes);
  }

  /**
   * @deprecated Use batchInsertVectors().
   */
  async batchInsert(
    label: string,
    vectors: Float32Array[]
  ): Promise<bigint[]> {
    return this.batchInsertVectors(label, vectors);
  }

  /**
   * Create an edge between two nodes.
   *
   * @param sourceId - Source node ID
   * @param targetId - Target node ID
   * @param edgeType - Edge type/label
   * @param options - Edge creation options
   * @returns The created edge
   * @throws LatticeError with code -4 (NotFound) if either node does not exist
   *   as this transaction sees it
   */
  async createEdge(
    sourceId: bigint,
    targetId: bigint,
    edgeType: string,
    options: CreateEdgeOptions = {}
  ): Promise<Edge> {
    this.ensureWritable();

    const edgeId = this.ffi.createEdge(this.txnHandle!, sourceId, targetId, edgeType);

    const properties = options.properties ?? {};
    for (const [key, value] of Object.entries(properties)) {
      this.ffi.setEdgeProperty(this.txnHandle!, edgeId, key, value);
    }

    return {
      id: edgeId,
      sourceId,
      targetId,
      type: edgeType,
      properties: withoutNulls(properties),
    };
  }

  /**
   * Delete an edge.
   *
   * @param sourceId - Source node ID
   * @param targetId - Target node ID
   * @param edgeType - Edge type
   */
  async deleteEdge(
    sourceId: bigint,
    targetId: bigint,
    edgeType: string
  ): Promise<void> {
    this.ensureWritable();
    this.ffi.deleteEdge(this.txnHandle!, sourceId, targetId, edgeType);
  }

  /**
   * Set a property on an edge.
   *
   * A null value removes the property, as `setProperty` does.
   *
   * @param edgeId - The stable edge ID
   * @param key - Property key
   * @param value - Property value
   */
  async setEdgeProperty(
    edgeId: bigint,
    key: string,
    value: PropertyValue
  ): Promise<void> {
    this.ensureWritable();
    this.ffi.setEdgeProperty(this.txnHandle!, edgeId, key, value);
  }

  /**
   * Get a property from an edge.
   *
   * @param edgeId - The stable edge ID
   * @param key - Property key
   * @returns The property value, or null if not found
   */
  async getEdgeProperty(
    edgeId: bigint,
    key: string
  ): Promise<PropertyValue | null> {
    this.ensureActive();
    return this.ffi.getEdgeProperty(this.txnHandle!, edgeId, key) as PropertyValue | null;
  }

  /**
   * Remove a property from an edge.
   *
   * @param edgeId - The stable edge ID
   * @param key - Property key
   */
  async removeEdgeProperty(
    edgeId: bigint,
    key: string
  ): Promise<void> {
    this.ensureWritable();
    this.ffi.removeEdgeProperty(this.txnHandle!, edgeId, key);
  }

  /**
   * Get outgoing edges from a node.
   *
   * @param nodeId - The node ID
   * @returns Array of edges
   */
  async getOutgoingEdges(nodeId: bigint): Promise<Edge[]> {
    this.ensureActive();
    const resultHandle = this.ffi.getOutgoingEdges(this.txnHandle!, nodeId);
    try {
      const count = this.ffi.edgeResultCount(resultHandle);
      const edges: Edge[] = [];
      for (let i = 0; i < count; i++) {
        const edgeId = this.ffi.edgeResultGetId(resultHandle, i);
        const e = this.ffi.edgeResultGet(resultHandle, i);
        edges.push({
          id: edgeId,
          sourceId: e.source,
          targetId: e.target,
          type: e.edgeType,
          properties: {},
        });
      }
      return edges;
    } finally {
      this.ffi.edgeResultFree(resultHandle);
    }
  }

  /**
   * Get incoming edges to a node.
   *
   * @param nodeId - The node ID
   * @returns Array of edges
   */
  async getIncomingEdges(nodeId: bigint): Promise<Edge[]> {
    this.ensureActive();
    const resultHandle = this.ffi.getIncomingEdges(this.txnHandle!, nodeId);
    try {
      const count = this.ffi.edgeResultCount(resultHandle);
      const edges: Edge[] = [];
      for (let i = 0; i < count; i++) {
        const edgeId = this.ffi.edgeResultGetId(resultHandle, i);
        const e = this.ffi.edgeResultGet(resultHandle, i);
        edges.push({
          id: edgeId,
          sourceId: e.source,
          targetId: e.target,
          type: e.edgeType,
          properties: {},
        });
      }
      return edges;
    } finally {
      this.ffi.edgeResultFree(resultHandle);
    }
  }

  /**
   * Get outgoing edges from a node filtered by edge type.
   *
   * @param nodeId - The node ID
   * @param edgeType - Edge type/label
   * @param limit - Maximum number of edges to return; 0 means unlimited
   * @returns Array of edges
   */
  async getOutgoingEdgesByType(
    nodeId: bigint,
    edgeType: string,
    limit = 0
  ): Promise<Edge[]> {
    this.ensureActive();
    const resultHandle = this.ffi.getOutgoingEdgesByType(
      this.txnHandle!,
      nodeId,
      edgeType,
      limit
    );
    try {
      const count = this.ffi.edgeResultCount(resultHandle);
      const edges: Edge[] = [];
      for (let i = 0; i < count; i++) {
        const edgeId = this.ffi.edgeResultGetId(resultHandle, i);
        const e = this.ffi.edgeResultGet(resultHandle, i);
        edges.push({
          id: edgeId,
          sourceId: e.source,
          targetId: e.target,
          type: e.edgeType,
          properties: {},
        });
      }
      return edges;
    } finally {
      this.ffi.edgeResultFree(resultHandle);
    }
  }

  /**
   * Get incoming edges to a node filtered by edge type.
   *
   * @param nodeId - The node ID
   * @param edgeType - Edge type/label
   * @param limit - Maximum number of edges to return; 0 means unlimited
   * @returns Array of edges
   */
  async getIncomingEdgesByType(
    nodeId: bigint,
    edgeType: string,
    limit = 0
  ): Promise<Edge[]> {
    this.ensureActive();
    const resultHandle = this.ffi.getIncomingEdgesByType(
      this.txnHandle!,
      nodeId,
      edgeType,
      limit
    );
    try {
      const count = this.ffi.edgeResultCount(resultHandle);
      const edges: Edge[] = [];
      for (let i = 0; i < count; i++) {
        const edgeId = this.ffi.edgeResultGetId(resultHandle, i);
        const e = this.ffi.edgeResultGet(resultHandle, i);
        edges.push({
          id: edgeId,
          sourceId: e.source,
          targetId: e.target,
          type: e.edgeType,
          properties: {},
        });
      }
      return edges;
    } finally {
      this.ffi.edgeResultFree(resultHandle);
    }
  }

  /**
   * Execute a Cypher query inside this transaction.
   */
  async query(
    cypher: string,
    parameters?: Record<string, PropertyValue>
  ): Promise<QueryResult> {
    this.ensureActive();
    if (this.dbHandle === null) {
      throw new Error('Transaction is missing database handle');
    }

    const query = this.ffi.queryPrepare(this.dbHandle, cypher);
    try {
      if (parameters) {
        for (const [name, value] of Object.entries(parameters)) {
          if (value instanceof Float32Array) {
            this.ffi.queryBindVector(query, name, value);
          } else {
            this.ffi.queryBind(query, name, value);
          }
        }
      }

      const result = this.ffi.queryExecute(query, this.txnHandle!);
      try {
        const columnCount = this.ffi.resultColumnCount(result);
        const columns: string[] = [];
        for (let i = 0; i < columnCount; i++) {
          columns.push(this.ffi.resultColumnName(result, i));
        }

        const rows: Record<string, PropertyValue>[] = [];
        while (this.ffi.resultNext(result)) {
          const row: Record<string, PropertyValue> = {};
          for (let i = 0; i < columnCount; i++) {
            row[columns[i]!] = this.ffi.resultGet(result, i) as PropertyValue;
          }
          rows.push(row);
        }

        return { columns, rows };
      } finally {
        this.ffi.resultFree(result);
      }
    } finally {
      this.ffi.queryFree(query);
    }
  }

  /**
   * Return every node id that currently carries `label`.
   */
  async getNodesByLabel(label: string): Promise<bigint[]> {
    this.ensureActive();
    return this.ffi.getNodesByLabelInTxn(this.txnHandle!, label);
  }

  async findNodesByLabelProperty(
    label: string,
    property: string,
    value: PropertyValue,
    limit = 100
  ): Promise<bigint[]> {
    this.ensureActive();
    return this.ffi.findNodesByLabelProperty(this.txnHandle!, label, property, value, limit);
  }

  async findEdgesByTypeProperty(
    edgeType: string,
    property: string,
    value: PropertyValue,
    limit = 100
  ): Promise<bigint[]> {
    this.ensureActive();
    return this.ffi.findEdgesByTypeProperty(this.txnHandle!, edgeType, property, value, limit);
  }

  /**
   * Search for similar vectors inside this transaction.
   *
   * @throws LatticeError with code -14 (Unsupported) if the database was
   *   created without a vector index
   */
  async vectorSearch(
    vector: Float32Array,
    options?: VectorSearchOptions
  ): Promise<VectorSearchResult[]> {
    this.ensureActive();
    const k = options?.k ?? 10;
    const efSearch = options?.efSearch ?? 0;

    const resultHandle = this.ffi.vectorSearchInTxn(this.txnHandle!, vector, k, efSearch);
    try {
      const count = this.ffi.vectorResultCount(resultHandle);
      const results: VectorSearchResult[] = [];
      for (let i = 0; i < count; i++) {
        const r = this.ffi.vectorResultGet(resultHandle, i);
        results.push({ nodeId: r.nodeId, distance: r.distance });
      }
      return results;
    } finally {
      this.ffi.vectorResultFree(resultHandle);
    }
  }

  /**
   * Search full-text documents inside this transaction.
   */
  async ftsSearch(
    label: string,
    property: string,
    query: string,
    options?: FtsSearchOptions
  ): Promise<Array<{ nodeId: bigint; score: number }>> {
    this.ensureActive();
    const limit = options?.limit ?? 10;

    const resultHandle = this.ffi.ftsSearchInTxn(this.txnHandle!, label, property, query, limit);
    try {
      const count = this.ffi.ftsResultCount(resultHandle);
      const results: Array<{ nodeId: bigint; score: number }> = [];
      for (let i = 0; i < count; i++) {
        results.push(this.ffi.ftsResultGet(resultHandle, i));
      }
      return results;
    } finally {
      this.ffi.ftsResultFree(resultHandle);
    }
  }

  /**
   * Search full-text documents with fuzzy matching inside this transaction.
   */
  async ftsSearchFuzzy(
    label: string,
    property: string,
    query: string,
    options?: FtsSearchOptions & { maxDistance?: number; minTermLength?: number }
  ): Promise<Array<{ nodeId: bigint; score: number }>> {
    this.ensureActive();
    const limit = options?.limit ?? 10;
    const maxDistance = options?.maxDistance ?? 0;
    const minTermLength = options?.minTermLength ?? 0;

    const resultHandle = this.ffi.ftsSearchFuzzyInTxn(
      this.txnHandle!,
      label,
      property,
      query,
      limit,
      maxDistance,
      minTermLength
    );
    try {
      const count = this.ffi.ftsResultCount(resultHandle);
      const results: Array<{ nodeId: bigint; score: number }> = [];
      for (let i = 0; i < count; i++) {
        results.push(this.ffi.ftsResultGet(resultHandle, i));
      }
      return results;
    } finally {
      this.ffi.ftsResultFree(resultHandle);
    }
  }

  /**
   * Check if this is a read-only transaction.
   */
  isReadOnly(): boolean {
    return this.readOnly;
  }

  /**
   * Check if the transaction is still active.
   */
  isActive(): boolean {
    return !this.committed && !this.rolledBack && this.txnHandle !== null;
  }

  /**
   * Ensure transaction is active.
   */
  private ensureActive(): void {
    if (!this.isActive()) {
      throw new Error('Transaction is not active');
    }
  }

  /**
   * Ensure transaction is writable.
   */
  private ensureWritable(): void {
    this.ensureActive();
    if (this.readOnly) {
      throw new Error('Cannot write in read-only transaction');
    }
  }
}
