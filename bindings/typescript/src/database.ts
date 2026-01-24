/**
 * Database class for Lattice TypeScript bindings.
 */

import { Transaction } from './transaction';
import {
  QueryResult,
  VectorSearchResult,
  PropertyValue,
  VectorSearchOptions,
  FtsSearchOptions,
} from './types';
import { getNative, NativeDatabase, NativeQueryResult } from './native';

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
  /** Enable vector storage */
  enableVector?: boolean;
  /** Vector dimensions (default: 128) */
  vectorDimensions?: number;
}

/**
 * Lattice database connection.
 *
 * @example
 * ```typescript
 * const db = new Database('knowledge.db', { create: true });
 * await db.open();
 *
 * await db.write(async (txn) => {
 *   const nodeId = await txn.createNode({ labels: ['Person'], properties: { name: 'Alice' } });
 * });
 *
 * const result = await db.query('MATCH (n:Person) RETURN n.name');
 * console.log(result.rows);
 *
 * await db.close();
 * ```
 */
export class Database {
  private readonly path: string;
  private readonly options: DatabaseOptions;
  private native: NativeDatabase | null = null;
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
      enableVector: false,
      vectorDimensions: 128,
      ...options,
    };
  }

  /**
   * Open the database connection.
   */
  async open(): Promise<void> {
    if (this.native !== null) {
      return;
    }

    const mod = getNative();
    this.native = new mod.Database();
    this.native.open(this.path, {
      create: this.options.create,
      readOnly: this.options.readOnly,
      cacheSizeMb: this.options.cacheSizeMb,
      enableVector: this.options.enableVector,
      vectorDimensions: this.options.vectorDimensions,
    });
  }

  /**
   * Close the database connection.
   */
  async close(): Promise<void> {
    if (this.closed || this.native === null) {
      return;
    }
    this.native.close();
    this.native = null;
    this.closed = true;
  }

  /**
   * Execute a read-only transaction.
   *
   * @param fn - Transaction function
   * @returns Result of the transaction function
   */
  async read<T>(fn: (txn: Transaction) => Promise<T>): Promise<T> {
    this.ensureOpen();
    const nativeTxn = this.native!.begin(true);
    const txn = new Transaction(nativeTxn, true);
    try {
      const result = await fn(txn);
      return result;
    } finally {
      txn.rollback();
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
    this.ensureOpen();
    const nativeTxn = this.native!.begin(false);
    const txn = new Transaction(nativeTxn, false);
    try {
      const result = await fn(txn);
      txn.commit();
      return result;
    } catch (error) {
      txn.rollback();
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
    this.ensureOpen();
    const result: NativeQueryResult = this.native!.query(cypher, parameters as Record<string, unknown>);
    return {
      columns: result.columns,
      rows: result.rows as Record<string, PropertyValue>[],
    };
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
    this.ensureOpen();
    const results = this.native!.vectorSearch(vector, {
      k: options?.k ?? 10,
      efSearch: options?.efSearch ?? 0,
    });
    return results.map((r) => ({
      nodeId: r.nodeId,
      distance: r.distance,
    }));
  }

  /**
   * Full-text search.
   *
   * @param query - Search query
   * @param options - Search options
   * @returns Matching nodes with scores
   */
  async ftsSearch(
    query: string,
    options?: FtsSearchOptions
  ): Promise<Array<{ nodeId: bigint; score: number }>> {
    this.ensureOpen();
    const results = this.native!.ftsSearch(query, {
      limit: options?.limit ?? 10,
    });
    return results.map((r) => ({
      nodeId: r.nodeId,
      score: r.score,
    }));
  }

  /**
   * Clear the query cache.
   *
   * Removes all cached parsed queries, forcing re-parsing on next execution.
   */
  async cacheClear(): Promise<void> {
    this.ensureOpen();
    this.native!.cacheClear();
  }

  /**
   * Get query cache statistics.
   *
   * @returns Object with entries count, hit count, and miss count.
   */
  async cacheStats(): Promise<{ entries: number; hits: number; misses: number }> {
    this.ensureOpen();
    return this.native!.cacheStats();
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
    return this.native !== null && !this.closed;
  }

  /**
   * Ensure the database is open.
   */
  private ensureOpen(): void {
    if (this.native === null || this.closed) {
      throw new Error('Database is not open');
    }
  }
}
