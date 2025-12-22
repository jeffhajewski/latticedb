/**
 * Database class for Lattice TypeScript bindings.
 */

import { Transaction } from './transaction';
import {
  Node,
  QueryResult,
  VectorSearchResult,
  PropertyValue,
  VectorSearchOptions,
  FtsSearchOptions,
} from './types';

/**
 * Options for opening a database.
 */
export interface DatabaseOptions {
  /** Create database if it doesn't exist */
  create?: boolean;
  /** Open in read-only mode */
  readOnly?: boolean;
  /** Cache size in megabytes */
  cacheSizeMb?: number;
}

/**
 * Lattice database connection.
 *
 * @example
 * ```typescript
 * const db = new Database('knowledge.lattice', { create: true });
 * await db.write(async (txn) => {
 *   const node = await txn.createNode({
 *     labels: ['Person'],
 *     properties: { name: 'Alice' }
 *   });
 * });
 * await db.close();
 * ```
 */
export class Database {
  private readonly path: string;
  private readonly options: DatabaseOptions;
  private handle: unknown | null = null;
  private closed = false;

  /**
   * Create a new database connection.
   *
   * @param path - Path to the database file
   * @param options - Database options
   */
  constructor(path: string, options: DatabaseOptions = {}) {
    this.path = path;
    this.options = {
      create: false,
      readOnly: false,
      cacheSizeMb: 100,
      ...options,
    };
  }

  /**
   * Open the database connection.
   */
  async open(): Promise<void> {
    if (this.handle !== null) {
      return;
    }
    // TODO: Call native lattice_open
    this.handle = {};
  }

  /**
   * Close the database connection.
   */
  async close(): Promise<void> {
    if (this.closed) {
      return;
    }
    // TODO: Call native lattice_close
    this.handle = null;
    this.closed = true;
  }

  /**
   * Execute a read-only transaction.
   *
   * @param fn - Transaction function
   * @returns Result of the transaction function
   */
  async read<T>(fn: (txn: Transaction) => Promise<T>): Promise<T> {
    const txn = new Transaction(this, { readOnly: true });
    try {
      await txn.begin();
      const result = await fn(txn);
      return result;
    } finally {
      await txn.rollback();
    }
  }

  /**
   * Execute a read-write transaction.
   *
   * @param fn - Transaction function
   * @returns Result of the transaction function
   */
  async write<T>(fn: (txn: Transaction) => Promise<T>): Promise<T> {
    if (this.options.readOnly) {
      throw new Error('Cannot write to a read-only database');
    }
    const txn = new Transaction(this, { readOnly: false });
    try {
      await txn.begin();
      const result = await fn(txn);
      await txn.commit();
      return result;
    } catch (error) {
      await txn.rollback();
      throw error;
    }
  }

  /**
   * Execute a Cypher query.
   *
   * @param cypher - Cypher query string
   * @param parameters - Query parameters
   * @returns Query results
   */
  async query(
    cypher: string,
    parameters?: Record<string, PropertyValue>
  ): Promise<QueryResult> {
    // TODO: Implement query execution
    void cypher;
    void parameters;
    return { columns: [], rows: [] };
  }

  /**
   * Search for similar vectors.
   *
   * @param vector - Query vector
   * @param options - Search options
   * @returns Search results
   */
  async vectorSearch(
    vector: Float32Array,
    options?: VectorSearchOptions
  ): Promise<VectorSearchResult[]> {
    // TODO: Implement vector search
    void vector;
    void options;
    return [];
  }

  /**
   * Full-text search.
   *
   * @param query - Search query
   * @param options - Search options
   * @returns Matching nodes
   */
  async ftsSearch(query: string, options?: FtsSearchOptions): Promise<Node[]> {
    // TODO: Implement FTS search
    void query;
    void options;
    return [];
  }

  /**
   * Get the database file path.
   */
  getPath(): string {
    return this.path;
  }

  /**
   * Check if the database is open.
   */
  isOpen(): boolean {
    return this.handle !== null && !this.closed;
  }
}
