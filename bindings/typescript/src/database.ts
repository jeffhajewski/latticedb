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
import {
  getFFI,
  LatticeFFI,
  DatabaseHandle,
} from './ffi';

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
  private ffi: LatticeFFI | null = null;
  private dbHandle: DatabaseHandle | null = null;
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
    if (this.dbHandle !== null) {
      return;
    }

    this.ffi = getFFI();
    this.dbHandle = this.ffi.open(this.path, {
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
    if (this.closed || this.dbHandle === null || this.ffi === null) {
      return;
    }
    this.ffi.close(this.dbHandle);
    this.dbHandle = null;
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
    const txnHandle = this.ffi!.begin(this.dbHandle!, true);
    const txn = new Transaction(this.ffi!, txnHandle, true);
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
    const txnHandle = this.ffi!.begin(this.dbHandle!, false);
    const txn = new Transaction(this.ffi!, txnHandle, false);
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
    const ffi = this.ffi!;

    // Prepare query
    const query = ffi.queryPrepare(this.dbHandle!, cypher);
    try {
      // Bind parameters
      if (parameters) {
        for (const [name, value] of Object.entries(parameters)) {
          if (value instanceof Float32Array) {
            ffi.queryBindVector(query, name, value);
          } else {
            ffi.queryBind(query, name, value);
          }
        }
      }

      // Execute within an auto-created read transaction
      const txnHandle = ffi.begin(this.dbHandle!, true);
      try {
        const result = ffi.queryExecute(query, txnHandle);
        try {
          // Read columns
          const columnCount = ffi.resultColumnCount(result);
          const columns: string[] = [];
          for (let i = 0; i < columnCount; i++) {
            columns.push(ffi.resultColumnName(result, i));
          }

          // Read rows
          const rows: Record<string, PropertyValue>[] = [];
          while (ffi.resultNext(result)) {
            const row: Record<string, PropertyValue> = {};
            for (let i = 0; i < columnCount; i++) {
              row[columns[i]!] = ffi.resultGet(result, i) as PropertyValue;
            }
            rows.push(row);
          }

          return { columns, rows };
        } finally {
          ffi.resultFree(result);
        }
      } finally {
        ffi.rollback(txnHandle);
      }
    } finally {
      ffi.queryFree(query);
    }
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
    const ffi = this.ffi!;
    const k = options?.k ?? 10;
    const efSearch = options?.efSearch ?? 0;

    const resultHandle = ffi.vectorSearch(this.dbHandle!, vector, k, efSearch);
    try {
      const count = ffi.vectorResultCount(resultHandle);
      const results: VectorSearchResult[] = [];
      for (let i = 0; i < count; i++) {
        const r = ffi.vectorResultGet(resultHandle, i);
        results.push({ nodeId: r.nodeId, distance: r.distance });
      }
      return results;
    } finally {
      ffi.vectorResultFree(resultHandle);
    }
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
    const ffi = this.ffi!;
    const limit = options?.limit ?? 10;

    const resultHandle = ffi.ftsSearch(this.dbHandle!, query, limit);
    try {
      const count = ffi.ftsResultCount(resultHandle);
      const results: Array<{ nodeId: bigint; score: number }> = [];
      for (let i = 0; i < count; i++) {
        results.push(ffi.ftsResultGet(resultHandle, i));
      }
      return results;
    } finally {
      ffi.ftsResultFree(resultHandle);
    }
  }

  /**
   * Fuzzy full-text search with typo tolerance.
   *
   * @param query - Search query
   * @param options - Search options including maxDistance and minTermLength
   * @returns Matching nodes with scores
   */
  async ftsSearchFuzzy(
    query: string,
    options?: FtsSearchOptions & { maxDistance?: number; minTermLength?: number }
  ): Promise<Array<{ nodeId: bigint; score: number }>> {
    this.ensureOpen();
    const ffi = this.ffi!;
    const limit = options?.limit ?? 10;
    const maxDistance = options?.maxDistance ?? 0;
    const minTermLength = options?.minTermLength ?? 0;

    const resultHandle = ffi.ftsSearchFuzzy(this.dbHandle!, query, limit, maxDistance, minTermLength);
    try {
      const count = ffi.ftsResultCount(resultHandle);
      const results: Array<{ nodeId: bigint; score: number }> = [];
      for (let i = 0; i < count; i++) {
        results.push(ffi.ftsResultGet(resultHandle, i));
      }
      return results;
    } finally {
      ffi.ftsResultFree(resultHandle);
    }
  }

  /**
   * Clear the query cache.
   *
   * Removes all cached parsed queries, forcing re-parsing on next execution.
   */
  async cacheClear(): Promise<void> {
    this.ensureOpen();
    this.ffi!.cacheClear(this.dbHandle!);
  }

  /**
   * Get query cache statistics.
   *
   * @returns Object with entries count, hit count, and miss count.
   */
  async cacheStats(): Promise<{ entries: number; hits: number; misses: number }> {
    this.ensureOpen();
    return this.ffi!.cacheStats(this.dbHandle!);
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
    return this.dbHandle !== null && !this.closed;
  }

  /**
   * Ensure the database is open.
   */
  private ensureOpen(): void {
    if (this.dbHandle === null || this.closed) {
      throw new Error('Database is not open');
    }
  }
}
