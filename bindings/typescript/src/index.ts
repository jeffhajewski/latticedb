/**
 * Lattice: Embedded Knowledge Graph Database
 *
 * TypeScript bindings for the Lattice database.
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
} from './types';
