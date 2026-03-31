/**
 * Lattice: Embedded Property-Graph Database
 *
 * TypeScript bindings for the Lattice database.
 * Root embedding helpers are deprecated. Prefer the dedicated
 * `@hajewski/latticedb/embedding` entrypoint.
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
} from './types';
import {
  hashEmbed as embeddingHashEmbed,
  EmbeddingClient as EmbeddingClientClass,
  EmbeddingApiFormat as embeddingApiFormat,
  type EmbeddingConfig as EmbeddingConfigType,
} from './embedding';
export {
  isLibraryAvailable,
  LatticeError,
  LatticeQueryError,
  QueryErrorStage,
  QueryErrorLocation,
} from './ffi';

/**
 * @deprecated Use `@hajewski/latticedb/embedding` instead. Earliest removal is v0.6.0.
 */
export const hashEmbed: typeof embeddingHashEmbed = embeddingHashEmbed;

/**
 * @deprecated Use `@hajewski/latticedb/embedding` instead. Earliest removal is v0.6.0.
 */
export const EmbeddingClient: typeof EmbeddingClientClass = EmbeddingClientClass;

/**
 * @deprecated Use `@hajewski/latticedb/embedding` instead. Earliest removal is v0.6.0.
 */
export const EmbeddingApiFormat: typeof embeddingApiFormat = embeddingApiFormat;

/**
 * @deprecated Use `@hajewski/latticedb/embedding` instead. Earliest removal is v0.6.0.
 */
export type EmbeddingConfig = EmbeddingConfigType;

/**
 * Get the library version.
 */
export function version(): string {
  try {
    // eslint-disable-next-line @typescript-eslint/no-var-requires
    const { getFFI } = require('./ffi');
    return getFFI().version();
  } catch {
    return '0.4.2';
  }
}
