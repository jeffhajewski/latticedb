/**
 * Native module loader and type definitions.
 *
 * This module loads the native N-API addon and provides TypeScript types
 * for the raw native interface.
 */

import { PropertyValue } from './types';

/**
 * Native edge object returned from edge traversal.
 */
export interface NativeEdge {
  sourceId: bigint;
  targetId: bigint;
  type: string;
}

/**
 * Native query result.
 */
export interface NativeQueryResult {
  columns: string[];
  rows: Array<Record<string, unknown>>;
}

/**
 * Native vector search result.
 */
export interface NativeVectorResult {
  nodeId: bigint;
  distance: number;
}

/**
 * Native FTS search result.
 */
export interface NativeFtsResult {
  nodeId: bigint;
  score: number;
}

/**
 * Native Transaction class interface.
 */
export interface NativeTransaction {
  commit(): void;
  rollback(): void;
  createNode(label?: string): bigint;
  deleteNode(nodeId: bigint): void;
  nodeExists(nodeId: bigint): boolean;
  getLabels(nodeId: bigint): string[] | null;
  setProperty(nodeId: bigint, key: string, value: unknown): void;
  getProperty(nodeId: bigint, key: string): unknown;
  setVector(nodeId: bigint, key: string, vector: Float32Array): void;
  batchInsert(nodes: Array<{ label: string; vector: Float32Array }>): bigint[];
  ftsIndex(nodeId: bigint, text: string): void;
  createEdge(sourceId: bigint, targetId: bigint, edgeType: string): bigint;
  deleteEdge(sourceId: bigint, targetId: bigint, edgeType: string): void;
  getOutgoingEdges(nodeId: bigint): NativeEdge[];
  getIncomingEdges(nodeId: bigint): NativeEdge[];
}

/**
 * Native Database class interface.
 */
export interface NativeDatabase {
  open(path: string, options?: {
    create?: boolean;
    readOnly?: boolean;
    cacheSizeMb?: number;
    enableVector?: boolean;
    vectorDimensions?: number;
  }): void;
  close(): void;
  begin(readOnly?: boolean): NativeTransaction;
  isOpen(): boolean;
  query(cypher: string, parameters?: Record<string, unknown>): NativeQueryResult;
  vectorSearch(vector: Float32Array, options?: { k?: number; efSearch?: number }): NativeVectorResult[];
  ftsSearch(query: string, options?: { limit?: number }): NativeFtsResult[];
  cacheClear(): void;
  cacheStats(): { entries: number; hits: number; misses: number };
}

/**
 * Native module interface.
 */
export interface NativeModule {
  Database: new () => NativeDatabase;
  Transaction: new () => NativeTransaction;
  version(): string;
}

/**
 * Try to load the native module.
 */
function loadNative(): NativeModule | null {
  try {
    // Try to load from the build directory
    // eslint-disable-next-line @typescript-eslint/no-require-imports
    return require('../build/Release/lattice_native.node') as NativeModule;
  } catch {
    try {
      // Try debug build
      // eslint-disable-next-line @typescript-eslint/no-require-imports
      return require('../build/Debug/lattice_native.node') as NativeModule;
    } catch {
      return null;
    }
  }
}

/**
 * The loaded native module, or null if not available.
 */
export const native: NativeModule | null = loadNative();

/**
 * Check if the native module is available.
 */
export function isNativeAvailable(): boolean {
  return native !== null;
}

/**
 * Get the native module, throwing if not available.
 */
export function getNative(): NativeModule {
  if (native === null) {
    throw new Error(
      'Native module not available. ' +
      'Make sure the native addon is built with "npm run build:native".'
    );
  }
  return native;
}
