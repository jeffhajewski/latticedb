/**
 * Basic tests for Lattice TypeScript bindings.
 *
 * These are unit tests for type definitions and basic functionality.
 * Integration tests require the native module to be built.
 */

import { Value, Node, Edge, QueryResult, VectorSearchResult } from '../src/types';
import { Database } from '../src/database';
import { isNativeAvailable } from '../src/native';

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

describe('Native module', () => {
  test('check native availability', () => {
    // This test just verifies the function works
    const available = isNativeAvailable();
    expect(typeof available).toBe('boolean');
  });
});
