/**
 * Transaction class for Lattice TypeScript bindings.
 */

import {
  Node,
  Edge,
  PropertyValue,
  CreateNodeOptions,
  CreateEdgeOptions,
} from './types';
import {
  LatticeFFI,
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
  private txnHandle: TransactionHandle | null;
  private readonly readOnly: boolean;
  private committed = false;
  private rolledBack = false;

  /**
   * Create a new transaction wrapper.
   * @internal
   */
  constructor(ffi: LatticeFFI, txnHandle: TransactionHandle, readOnly: boolean) {
    this.ffi = ffi;
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

    if (labels.length > 1) {
      throw new Error('Multiple labels are not yet supported. Pass at most one label.');
    }

    const firstLabel = labels[0] ?? '';
    const nodeId = this.ffi.createNode(this.txnHandle!, firstLabel);

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
   * Batch insert multiple nodes with vectors in a single call.
   *
   * @param label - Label for all nodes
   * @param vectors - Array of vectors (one per node)
   * @returns Array of created node IDs
   */
  async batchInsert(
    label: string,
    vectors: Float32Array[]
  ): Promise<bigint[]> {
    this.ensureWritable();
    const nodes = vectors.map((v) => ({ label, vector: v }));
    return this.ffi.batchInsert(this.txnHandle!, nodes);
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

    if (options.properties && Object.keys(options.properties).length > 0) {
      throw new Error('Edge properties are not yet supported');
    }

    const edgeId = this.ffi.createEdge(this.txnHandle!, sourceId, targetId, edgeType);

    return {
      id: edgeId,
      sourceId,
      targetId,
      type: edgeType,
      properties: {},
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
        const e = this.ffi.edgeResultGet(resultHandle, i);
        edges.push({
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
        const e = this.ffi.edgeResultGet(resultHandle, i);
        edges.push({
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
