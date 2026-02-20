/**
 * Lattice: Embedded Knowledge Graph Database
 *
 * TypeScript bindings for the Lattice database.
 *
 * @example
 * ```typescript
 * import { Database } from 'lattice-db';
 *
 * const db = new Database('knowledge.db', { create: true });
 * await db.open();
 *
 * await db.write(async (txn) => {
 *   const node = await txn.createNode({
 *     labels: ['Person'],
 *     properties: { name: 'Alice', age: 30 }
 *   });
 *   console.log('Created node:', node.id);
 * });
 *
 * const result = await db.query('MATCH (n:Person) RETURN n.name');
 * for (const row of result.rows) {
 *   console.log(row);
 * }
 *
 * await db.close();
 * ```
 */

export { Database, DatabaseOptions } from './database';
export { Transaction } from './transaction';
export {
  Node,
  Edge,
  Value,
  QueryResult,
  VectorSearchResult,
  PropertyValue,
  CreateNodeOptions,
  CreateEdgeOptions,
  VectorSearchOptions,
  FtsSearchOptions,
  EmbeddingApiFormat,
  EmbeddingConfig,
} from './types';
export { isLibraryAvailable } from './ffi';

/**
 * Get the library version.
 */
export function version(): string {
  try {
    // eslint-disable-next-line @typescript-eslint/no-var-requires
    const { getFFI } = require('./ffi');
    return getFFI().version();
  } catch {
    return '0.2.1';
  }
}
