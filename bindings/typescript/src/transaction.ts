/**
 * Transaction class for Lattice TypeScript bindings.
 */

import {
  Node,
  Edge,
  PropertyValue,
  CreateNodeOptions,
  CreateEdgeOptions,
  QueryResult,
  VectorSearchOptions,
  VectorSearchResult,
  FtsSearchOptions,
} from './types';
import {
  LatticeFFI,
  DatabaseHandle,
  TransactionHandle,
} from './ffi';

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
    this.ffi.commit(this.txnHandle);
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
      properties,
    };
  }

  /**
   * Delete a node.
   *
   * @param nodeId - ID of the node to delete
   */
  async deleteNode(nodeId: bigint): Promise<void> {
    this.ensureWritable();
    this.ffi.deleteNode(this.txnHandle!, nodeId);
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
   * @param key - Vector property key
   * @param vector - Vector data
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
   * Insert multiple vector-bearing nodes in a single call.
   *
   * @param label - Label for all nodes
   * @param vectors - Array of vectors (one per node)
   * @returns Array of created node IDs
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
   * @deprecated Use batchInsertVectors(). Earliest removal is v0.6.0.
   */
  async batchInsert(
    label: string,
    vectors: Float32Array[]
  ): Promise<bigint[]> {
    return this.batchInsertVectors(label, vectors);
  }

  /**
   * Index text for full-text search.
   *
   * @param nodeId - The node ID
   * @param text - Text to index
   */
  async ftsIndex(nodeId: bigint, text: string): Promise<void> {
    this.ensureWritable();
    this.ffi.ftsIndex(this.txnHandle!, nodeId, text);
  }

  /**
   * Create an edge between two nodes.
   *
   * @param sourceId - Source node ID
   * @param targetId - Target node ID
   * @param edgeType - Edge type/label
   * @param options - Edge creation options
   * @returns The created edge
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
      properties,
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

  /**
   * Search for similar vectors inside this transaction.
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
    query: string,
    options?: FtsSearchOptions
  ): Promise<Array<{ nodeId: bigint; score: number }>> {
    this.ensureActive();
    const limit = options?.limit ?? 10;

    const resultHandle = this.ffi.ftsSearchInTxn(this.txnHandle!, query, limit);
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
    query: string,
    options?: FtsSearchOptions & { maxDistance?: number; minTermLength?: number }
  ): Promise<Array<{ nodeId: bigint; score: number }>> {
    this.ensureActive();
    const limit = options?.limit ?? 10;
    const maxDistance = options?.maxDistance ?? 0;
    const minTermLength = options?.minTermLength ?? 0;

    const resultHandle = this.ffi.ftsSearchFuzzyInTxn(
      this.txnHandle!,
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
