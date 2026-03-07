/**
 * Basic tests for Lattice TypeScript bindings.
 *
 * These are unit tests for type definitions and basic functionality.
 * Integration tests require the native module to be built.
 */

import { Value, Node, Edge, QueryResult, VectorSearchResult } from '../src/types';
import { Database } from '../src/database';
import { EmbeddingClient, LatticeError, LatticeQueryError, QueryErrorStage, hashEmbed, version } from '../src/index';
import { isLibraryAvailable } from '../src/ffi';

describe('Value', () => {
  test('null value', () => {
    const v = Value.null();
    expect(v.value).toBeNull();
  });

  test('bool value', () => {
    const v = Value.bool(true);
    expect(v.value).toBe(true);
  });

  test('int value', () => {
    const v = Value.int(42);
    expect(v.value).toBe(42);
  });

  test('float value', () => {
    const v = Value.float(3.14);
    expect(v.value).toBe(3.14);
  });

  test('string value', () => {
    const v = Value.string('hello');
    expect(v.value).toBe('hello');
  });

  test('bytes value', () => {
    const bytes = new Uint8Array([1, 2, 3]);
    const v = Value.bytes(bytes);
    expect(v.value).toEqual(bytes);
  });
});

describe('Node', () => {
  test('create node', () => {
    const node: Node = {
      id: BigInt(1),
      labels: ['Person'],
      properties: { name: 'Alice', age: 30 },
    };
    expect(node.id).toBe(BigInt(1));
    expect(node.labels).toEqual(['Person']);
    expect(node.properties['name']).toBe('Alice');
  });
});

describe('Edge', () => {
  test('create edge', () => {
    const edge: Edge = {
      id: BigInt(1),
      sourceId: BigInt(10),
      targetId: BigInt(20),
      type: 'KNOWS',
      properties: { since: 2020 },
    };
    expect(edge.id).toBe(BigInt(1));
    expect(edge.sourceId).toBe(BigInt(10));
    expect(edge.targetId).toBe(BigInt(20));
    expect(edge.type).toBe('KNOWS');
  });
});

describe('QueryResult', () => {
  test('empty result', () => {
    const result: QueryResult = {
      columns: ['name', 'age'],
      rows: [],
    };
    expect(result.columns).toEqual(['name', 'age']);
    expect(result.rows).toHaveLength(0);
  });

  test('with rows', () => {
    const result: QueryResult = {
      columns: ['name'],
      rows: [{ name: 'Alice' }, { name: 'Bob' }],
    };
    expect(result.rows).toHaveLength(2);
    expect(result.rows[0]?.['name']).toBe('Alice');
  });
});

describe('VectorSearchResult', () => {
  test('create result', () => {
    const result: VectorSearchResult = {
      nodeId: BigInt(1),
      distance: 0.5,
    };
    expect(result.nodeId).toBe(BigInt(1));
    expect(result.distance).toBe(0.5);
    expect(result.node).toBeUndefined();
  });
});

describe('Database', () => {
  test('create database instance', () => {
    const db = new Database('test.db', { create: true });
    expect(db.getPath()).toBe('test.db');
    expect(db.isOpen()).toBe(false);
  });

  test('default options', () => {
    const db = new Database('test.db');
    expect(db.getPath()).toBe('test.db');
    expect(db.isOpen()).toBe(false);
  });
});

describe('Library availability', () => {
  test('check library availability', () => {
    // This test just verifies the function works
    const available = isLibraryAvailable();
    expect(typeof available).toBe('boolean');
  });
});

describe('Top-level exports', () => {
  test('version returns a non-empty string', () => {
    const v = version();
    expect(typeof v).toBe('string');
    expect(v.length).toBeGreaterThan(0);
  });

  test('LatticeQueryError carries stage and diagnostics', () => {
    const err = new LatticeQueryError(
      'parse failed',
      -1 as any,
      QueryErrorStage.Parse,
      'E_PARSE',
      { line: 1, column: 7, length: 1 }
    );

    expect(err).toBeInstanceOf(LatticeQueryError);
    expect(err).toBeInstanceOf(LatticeError);
    expect(err.stage).toBe(QueryErrorStage.Parse);
    expect(err.diagnosticCode).toBe('E_PARSE');
    expect(err.location).toEqual({ line: 1, column: 7, length: 1 });
  });
});

describe('Embedding utilities', () => {
  test('EmbeddingClient.embed throws after close', () => {
    const client = Object.create(EmbeddingClient.prototype) as { handle: unknown | null; embed: (text: string) => Float32Array };
    client.handle = null;
    expect(() => client.embed('hello')).toThrow(/closed/i);
  });

  test('EmbeddingClient.close is idempotent for a closed client', () => {
    const client = Object.create(EmbeddingClient.prototype) as { handle: unknown | null; close: () => void };
    client.handle = null;
    expect(() => client.close()).not.toThrow();
    expect(() => client.close()).not.toThrow();
  });

  const describeIfNative = isLibraryAvailable() ? describe : describe.skip;
  describeIfNative('hashEmbed native behavior', () => {
    test('hashEmbed returns deterministic vectors of requested dimensions', () => {
      const v1 = hashEmbed('hello world', 16);
      const v2 = hashEmbed('hello world', 16);
      const v3 = hashEmbed('different text', 16);

      expect(v1).toBeInstanceOf(Float32Array);
      expect(v1.length).toBe(16);
      expect(Array.from(v1)).toEqual(Array.from(v2));
      expect(Array.from(v1)).not.toEqual(Array.from(v3));
    });
  });
});
