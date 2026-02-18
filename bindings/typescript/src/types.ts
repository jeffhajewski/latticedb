/**
 * Type definitions for Lattice TypeScript bindings.
 */

/**
 * Property value types supported by Lattice.
 */
export type PropertyValue =
  | null
  | boolean
  | number
  | string
  | Uint8Array
  | PropertyValue[]
  | { [key: string]: PropertyValue };

/**
 * A node in the graph.
 */
export interface Node {
  /** Unique node identifier */
  id: bigint;
  /** Node labels */
  labels: string[];
  /** Node properties */
  properties: Record<string, PropertyValue>;
}

/**
 * An edge in the graph.
 */
export interface Edge {
  /** Unique edge identifier (not available on traversal results) */
  id?: bigint;
  /** Source node ID */
  sourceId: bigint;
  /** Target node ID */
  targetId: bigint;
  /** Edge type/label */
  type: string;
  /** Edge properties */
  properties: Record<string, PropertyValue>;
}

/**
 * Wrapper for property values.
 */
export class Value {
  constructor(public readonly value: PropertyValue) {}

  static null(): Value {
    return new Value(null);
  }

  static bool(v: boolean): Value {
    return new Value(v);
  }

  static int(v: number): Value {
    return new Value(v);
  }

  static float(v: number): Value {
    return new Value(v);
  }

  static string(v: string): Value {
    return new Value(v);
  }

  static bytes(v: Uint8Array): Value {
    return new Value(v);
  }
}

/**
 * Result of a Cypher query.
 */
export interface QueryResult {
  /** Column names */
  columns: string[];
  /** Result rows */
  rows: Record<string, PropertyValue>[];
}

/**
 * Result of a vector similarity search.
 */
export interface VectorSearchResult {
  /** Node ID */
  nodeId: bigint;
  /** Distance to query vector */
  distance: number;
  /** Optional full node data */
  node?: Node;
}

/**
 * Options for creating a node.
 */
export interface CreateNodeOptions {
  /** Node labels */
  labels?: string[];
  /** Node properties */
  properties?: Record<string, PropertyValue>;
}

/**
 * Options for creating an edge.
 */
export interface CreateEdgeOptions {
  /** Edge properties */
  properties?: Record<string, PropertyValue>;
}

/**
 * Options for vector search.
 */
export interface VectorSearchOptions {
  /** Vector property key */
  key?: string;
  /** Number of results */
  k?: number;
  /** HNSW ef parameter */
  efSearch?: number;
}

/**
 * Options for full-text search.
 */
export interface FtsSearchOptions {
  /** Text property key */
  key?: string;
  /** Maximum results */
  limit?: number;
}

/**
 * API format for HTTP embedding services.
 */
export enum EmbeddingApiFormat {
  Ollama = 0,
  OpenAI = 1,
}

/**
 * Configuration for an HTTP embedding client.
 */
export interface EmbeddingConfig {
  /** HTTP endpoint URL */
  endpoint: string;
  /** Model name (default: "nomic-embed-text") */
  model?: string;
  /** API format (default: Ollama) */
  apiFormat?: EmbeddingApiFormat;
  /** API key for authentication (null for no auth) */
  apiKey?: string;
  /** Request timeout in milliseconds (0 = default 30s) */
  timeoutMs?: number;
}
