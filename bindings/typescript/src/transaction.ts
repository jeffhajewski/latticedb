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
import { NativeTransaction } from './native';

/**
 * A database transaction.
 *
 * Transactions provide atomic, isolated access to the database.
 * Use `Database.read()` or `Database.write()` to create transactions.
 */
export class Transaction {
  private native: NativeTransaction | null;
  private readonly readOnly: boolean;
  private committed = false;
  private rolledBack = false;

  /**
   * Create a new transaction wrapper.
   * @internal
   */
  constructor(native: NativeTransaction, readOnly: boolean) {
    this.native = native;
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
    if (this.native === null) {
      throw new Error('Transaction closed');
    }
    this.native.commit();
    this.native = null;
    this.committed = true;
  }

  /**
   * Rollback the transaction.
   */
  rollback(): void {
    if (this.committed) {
      throw new Error('Transaction already committed');
    }
    if (this.rolledBack || this.native === null) {
      return;
    }
    this.native.rollback();
    this.native = null;
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

    // Create node with first label (or empty)
    const firstLabel = labels[0] ?? '';
    const nodeId = this.native!.createNode(firstLabel);

    // Add additional labels
    for (let i = 1; i < labels.length; i++) {
      // Note: The C API currently only supports one label at creation
      // Additional labels would need a separate API call
    }

    // Set properties
    for (const [key, value] of Object.entries(properties)) {
      this.native!.setProperty(nodeId, key, value);
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
    this.native!.deleteNode(nodeId);
  }

  /**
   * Check if a node exists.
   *
   * @param nodeId - The node ID
   * @returns True if the node exists
   */
  async nodeExists(nodeId: bigint): Promise<boolean> {
    this.ensureActive();
    return this.native!.nodeExists(nodeId);
  }

  /**
   * Get a node by ID.
   *
   * @param nodeId - The node ID
   * @returns The node, or null if not found
   */
  async getNode(nodeId: bigint): Promise<Node | null> {
    this.ensureActive();

    if (!this.native!.nodeExists(nodeId)) {
      return null;
    }

    const labels = this.native!.getLabels(nodeId) ?? [];

    return {
      id: nodeId,
      labels,
      properties: {}, // Properties need to be fetched individually
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
    this.native!.setProperty(nodeId, key, value);
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
    return this.native!.getProperty(nodeId, key) as PropertyValue;
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
    this.native!.setVector(nodeId, key, vector);
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
    return this.native!.batchInsert(nodes);
  }

  /**
   * Index text for full-text search.
   *
   * @param nodeId - The node ID
   * @param text - Text to index
   */
  async ftsIndex(nodeId: bigint, text: string): Promise<void> {
    this.ensureWritable();
    this.native!.ftsIndex(nodeId, text);
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

    const edgeId = this.native!.createEdge(sourceId, targetId, edgeType);

    // Set properties if provided
    // Note: Edge properties would need additional C API support

    return {
      id: edgeId,
      sourceId,
      targetId,
      type: edgeType,
      properties: options.properties ?? {},
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
    this.native!.deleteEdge(sourceId, targetId, edgeType);
  }

  /**
   * Get outgoing edges from a node.
   *
   * @param nodeId - The node ID
   * @returns Array of edges
   */
  async getOutgoingEdges(nodeId: bigint): Promise<Edge[]> {
    this.ensureActive();
    const edges = this.native!.getOutgoingEdges(nodeId);
    return edges.map((e) => ({
      sourceId: e.sourceId,
      targetId: e.targetId,
      type: e.type,
      properties: {},
    }));
  }

  /**
   * Get incoming edges to a node.
   *
   * @param nodeId - The node ID
   * @returns Array of edges
   */
  async getIncomingEdges(nodeId: bigint): Promise<Edge[]> {
    this.ensureActive();
    const edges = this.native!.getIncomingEdges(nodeId);
    return edges.map((e) => ({
      sourceId: e.sourceId,
      targetId: e.targetId,
      type: e.type,
      properties: {},
    }));
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
    return !this.committed && !this.rolledBack && this.native !== null;
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
