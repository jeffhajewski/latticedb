/**
 * Tests for the v5 open options, node deletes that would strand edges, and
 * edges to missing nodes.
 */

import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { Database, deserialize } from '../src/database';
import { Transaction } from '../src/transaction';
import { getFFI, isLibraryAvailable, LatticeErrorCode, LatticeError } from '../src/ffi';

const describeIfNative = isLibraryAvailable() ? describe : describe.skip;

function hasOpenV5(): boolean {
  const bindings = (getFFI() as unknown as { bindings: { lattice_open_v5?: unknown } }).bindings;
  return Boolean(bindings.lattice_open_v5);
}

async function codeOf(promise: Promise<unknown>): Promise<number | undefined> {
  try {
    await promise;
  } catch (err) {
    expect(err).toBeInstanceOf(LatticeError);
    return (err as LatticeError).code;
  }
  return undefined;
}

describeIfNative('open options v5, detach delete and edge endpoints', () => {
  let dir: string;

  beforeEach(() => {
    dir = fs.mkdtempSync(path.join(os.tmpdir(), 'lattice_v5_'));
  });

  afterEach(() => {
    fs.rmSync(dir, { recursive: true, force: true });
  });

  test('a euclidean index reopens without vector options and searches by L2', async () => {
    if (!hasOpenV5()) return;
    const dbPath = path.join(dir, 'euclid.db');
    let a = BigInt(0);
    let b = BigInt(0);

    const db = new Database(dbPath, {
      create: true,
      enableVectors: true,
      vectorDimensions: 3,
      vectorMetric: 'euclidean',
      vectorM: 8,
      vectorEfConstruction: 64,
    });
    await db.open();
    await db.write(async (txn) => {
      a = (await txn.createNode({ labels: ['P'] })).id;
      b = (await txn.createNode({ labels: ['P'] })).id;
      await txn.setVector(a, 'embedding', new Float32Array([3, 0, 0]));
      await txn.setVector(b, 'embedding', new Float32Array([0, 4, 0]));
    });
    await db.close();

    const reopened = new Database(dbPath);
    await reopened.open();
    try {
      const results = await reopened.vectorSearch(new Float32Array([0, 0, 0]), { k: 2 });
      expect(results.map((r) => r.nodeId)).toEqual([a, b]);
      expect(results[0]!.distance).toBeCloseTo(3, 4);
      expect(results[1]!.distance).toBeCloseTo(4, 4);
    } finally {
      await reopened.close();
    }

    // vectorDimensions 0 with vectors enabled means "the file's".
    const zeroDims = new Database(dbPath, { enableVectors: true, vectorDimensions: 0 });
    await zeroDims.open();
    await zeroDims.close();
  });

  test('options that contradict the file are ConfigMismatch', async () => {
    if (!hasOpenV5()) return;
    const dbPath = path.join(dir, 'mismatch.db');
    const db = new Database(dbPath, { create: true, enableVectors: true, vectorDimensions: 3 });
    await db.open();
    await db.close();

    expect(
      await codeOf(new Database(dbPath, { enableVectors: true, vectorDimensions: 4 }).open())
    ).toBe(LatticeErrorCode.ConfigMismatch);
    expect(await codeOf(new Database(dbPath, { vectorMetric: 'dot_product' }).open())).toBe(
      LatticeErrorCode.ConfigMismatch
    );
    expect(await codeOf(new Database(dbPath, { vectorM: 12 }).open())).toBe(
      LatticeErrorCode.ConfigMismatch
    );
  });

  test('out-of-range values are InvalidArg and unknown names are TypeErrors', async () => {
    if (!hasOpenV5()) return;
    expect(
      await codeOf(
        new Database(path.join(dir, 'badm.db'), { create: true, enableVectors: true, vectorM: 64 }).open()
      )
    ).toBe(LatticeErrorCode.InvalidArg);
    await expect(
      new Database(path.join(dir, 'x.db'), {
        create: true,
        syncMode: 'sometimes' as unknown as 'full',
      }).open()
    ).rejects.toThrow(TypeError);
  });

  test('sync options are accepted', async () => {
    if (!hasOpenV5()) return;
    const dbPath = path.join(dir, 'sync.db');
    const settings: Array<{ syncMode?: 'full' | 'normal'; fullFsync?: boolean }> = [
      { syncMode: 'normal', fullFsync: false },
      { syncMode: 'full', fullFsync: true },
      {},
    ];
    for (const s of settings) {
      const db = new Database(dbPath, { create: true, ...s });
      await db.open();
      await db.write(async (txn) => {
        await txn.createNode({ labels: ['S'] });
      });
      await db.close();
    }
    const db = new Database(dbPath);
    await db.open();
    try {
      const result = await db.query('MATCH (n:S) RETURN count(n) AS c');
      expect(Number(result.rows[0]!.c)).toBe(3);
    } finally {
      await db.close();
    }
  });

  test('a v5-only option against a library without v5 is Unsupported', async () => {
    const bindings = (getFFI() as unknown as { bindings: { lattice_open_v5?: unknown } }).bindings;
    const saved = bindings.lattice_open_v5;
    bindings.lattice_open_v5 = undefined;
    try {
      expect(
        await codeOf(new Database(path.join(dir, 'v4.db'), { create: true, syncMode: 'normal' }).open())
      ).toBe(LatticeErrorCode.Unsupported);
      const db = new Database(path.join(dir, 'v4.db'), { create: true });
      await db.open();
      await db.close();
    } finally {
      bindings.lattice_open_v5 = saved;
    }
  });

  test('a file marked for newer writers opens only read-only', async () => {
    const dbPath = path.join(dir, 'newer-writer.db');
    const db = new Database(dbPath, { create: true });
    await db.open();
    await db.write(async (txn) => {
      await txn.createNode({ labels: ['Old'] });
    });
    await db.close();

    // min_writer_version is a little-endian u16 at byte 222 of the header.
    const fd = fs.openSync(dbPath, 'r+');
    try {
      fs.writeSync(fd, Buffer.from([5, 0]), 0, 2, 222);
    } finally {
      fs.closeSync(fd);
    }

    expect(await codeOf(new Database(dbPath).open())).toBe(LatticeErrorCode.NeedsNewerWriter);

    const reader = new Database(dbPath, { readOnly: true });
    await reader.open();
    try {
      const result = await reader.query('MATCH (n:Old) RETURN count(n) AS c');
      expect(Number(result.rows[0]!.c)).toBe(1);
    } finally {
      await reader.close();
    }
  });

  test('a file in a newer format is a version mismatch, read-only or not', async () => {
    const dbPath = path.join(dir, 'newer-reader.db');
    const db = new Database(dbPath, { create: true });
    await db.open();
    await db.write(async (txn) => {
      await txn.createNode({ labels: ['Old'] });
    });
    await db.close();

    // min_reader_version is a little-endian u16 at byte 6 of the header.
    const fd = fs.openSync(dbPath, 'r+');
    try {
      fs.writeSync(fd, Buffer.from([5, 0]), 0, 2, 6);
    } finally {
      fs.closeSync(fd);
    }

    expect(await codeOf(new Database(dbPath).open())).toBe(LatticeErrorCode.VersionMismatch);
    expect(await codeOf(new Database(dbPath, { readOnly: true }).open())).toBe(
      LatticeErrorCode.VersionMismatch
    );
  });

  test('a plain delete of a node with relationships is refused', async () => {
    const db = new Database(path.join(dir, 'del.db'), { create: true });
    await db.open();
    try {
      let a = BigInt(0);
      await db.write(async (txn) => {
        a = (await txn.createNode({ labels: ['N'] })).id;
        const b = (await txn.createNode({ labels: ['N'] })).id;
        await txn.createEdge(a, b, 'R');
        expect(await codeOf(txn.deleteNode(a))).toBe(LatticeErrorCode.HasRelationships);
        expect(await txn.nodeExists(a)).toBe(true);
      });
      await db.read(async (txn) => {
        expect(await txn.nodeExists(a)).toBe(true);
        expect((await txn.getOutgoingEdges(a)).length).toBe(1);
      });
    } finally {
      await db.close();
    }
  });

  test('detach delete removes the node and its relationships', async () => {
    const db = new Database(path.join(dir, 'detach.db'), { create: true });
    await db.open();
    try {
      let a = BigInt(0);
      let b = BigInt(0);
      let c = BigInt(0);
      await db.write(async (txn) => {
        a = (await txn.createNode({ labels: ['N'] })).id;
        b = (await txn.createNode({ labels: ['N'] })).id;
        c = (await txn.createNode({ labels: ['N'] })).id;
        await txn.createEdge(a, b, 'R');
        await txn.createEdge(c, a, 'R');
      });
      await db.write(async (txn) => {
        await txn.deleteNode(a, { detach: true });
      });
      await db.read(async (txn) => {
        expect(await txn.nodeExists(a)).toBe(false);
        expect(await txn.getIncomingEdges(b)).toEqual([]);
        expect(await txn.getOutgoingEdges(c)).toEqual([]);
      });
      await db.write(async (txn) => {
        await txn.deleteNode(b);
        await txn.deleteNode(BigInt(987654));
        await txn.deleteNode(BigInt(987654), { detach: true });
      });
    } finally {
      await db.close();
    }
  });

  test('a commit that reports TxnAborted ends the transaction', async () => {
    const db = new Database(path.join(dir, 'aborted-commit.db'), { create: true });
    await db.open();
    try {
      const ffi = getFFI();
      const dbHandle = (db as unknown as { dbHandle: unknown }).dbHandle;
      const handle = ffi.begin(dbHandle, false);
      const txn = new Transaction(ffi, handle, false, dbHandle);
      await txn.createNode({ labels: ['N'] });
      // Retire the native handle behind the wrapper's back, so the wrapper's
      // own commit gets TXN_ABORTED from the C side.
      ffi.commit(handle);

      let code: number | undefined;
      try {
        txn.commit();
      } catch (err) {
        code = (err as LatticeError).code;
      }
      expect(code).toBe(LatticeErrorCode.TxnAborted);
      // Nothing is left to roll back; this must not reach the retired handle.
      expect(() => txn.rollback()).not.toThrow();
      expect(() => txn.commit()).toThrow('Transaction already rolled back');
    } finally {
      await db.close();
    }
  });

  test('an edge to a missing node is NotFound', async () => {
    const db = new Database(path.join(dir, 'edge.db'), { create: true });
    await db.open();
    try {
      await db.write(async (txn) => {
        const a = (await txn.createNode({ labels: ['N'] })).id;
        expect(await codeOf(txn.createEdge(a, BigInt(987654), 'R'))).toBe(LatticeErrorCode.NotFound);
        expect(await codeOf(txn.createEdge(BigInt(987654), a, 'R'))).toBe(LatticeErrorCode.NotFound);
      });
    } finally {
      await db.close();
    }
  });
});

function hasDeserializeV5(): boolean {
  const bindings = (
    getFFI() as unknown as {
      bindings: { lattice_deserialize_v5?: unknown; lattice_deserialize_borrowed_v5?: unknown };
    }
  ).bindings;
  return Boolean(bindings.lattice_deserialize_v5 && bindings.lattice_deserialize_borrowed_v5);
}

function codeOfSync(fn: () => unknown): number | undefined {
  try {
    fn();
  } catch (err) {
    expect(err).toBeInstanceOf(LatticeError);
    return (err as LatticeError).code;
  }
  return undefined;
}

describeIfNative('deserialize with v5 options', () => {
  let dir: string;

  beforeEach(() => {
    dir = fs.mkdtempSync(path.join(os.tmpdir(), 'lattice_deser_v5_'));
  });

  afterEach(() => {
    fs.rmSync(dir, { recursive: true, force: true });
  });

  async function addPoints(db: Database): Promise<void> {
    await db.write(async (txn) => {
      const a = (await txn.createNode({ labels: ['P'] })).id;
      const b = (await txn.createNode({ labels: ['P'] })).id;
      await txn.setVector(a, 'embedding', new Float32Array([0, 0]));
      await txn.setVector(b, 'embedding', new Float32Array([3, 4]));
    });
  }

  async function euclideanBytes(): Promise<Uint8Array> {
    const db = new Database(path.join(dir, 'euclid.db'), {
      create: true,
      enableVectors: true,
      vectorDimensions: 2,
      vectorMetric: 'euclidean',
    });
    await db.open();
    try {
      await addPoints(db);
      return db.serialize();
    } finally {
      await db.close();
    }
  }

  async function secondDistance(db: Database): Promise<number> {
    const results = await db.vectorSearch(new Float32Array([0, 0]), { k: 2 });
    expect(results).toHaveLength(2);
    return results[1]!.distance;
  }

  for (const copy of [true, false]) {
    describe(copy ? 'copying' : 'borrowed', () => {
      test('the index in the bytes brings its metric', async () => {
        if (!hasDeserializeV5()) return;
        const db = deserialize(await euclideanBytes(), {}, copy);
        try {
          // L2 distance from the origin to (3, 4); cosine could not give 5.
          expect(await secondDistance(db)).toBeCloseTo(5, 4);
        } finally {
          await db.close();
        }
      });

      test('a contradicting metric is ConfigMismatch', async () => {
        if (!hasDeserializeV5()) return;
        const bytes = await euclideanBytes();
        expect(codeOfSync(() => deserialize(bytes, { vectorMetric: 'cosine' }, copy))).toBe(
          LatticeErrorCode.ConfigMismatch
        );
      });

      test('enableVectors creates an index configured by the vector options', async () => {
        if (!hasDeserializeV5()) return;
        const plain = new Database(path.join(dir, 'plain.db'), { create: true });
        await plain.open();
        let bytes: Uint8Array;
        try {
          bytes = plain.serialize();
        } finally {
          await plain.close();
        }

        const db = deserialize(
          bytes,
          { enableVectors: true, vectorDimensions: 2, vectorMetric: 'euclidean' },
          copy
        );
        try {
          await addPoints(db);
          expect(await secondDistance(db)).toBeCloseTo(5, 4);
        } finally {
          await db.close();
        }
      });
    });
  }

  test('a v5-only option against a library without deserialize v5 is Unsupported', async () => {
    const plain = new Database(path.join(dir, 'v4.db'), { create: true });
    await plain.open();
    let bytes: Uint8Array;
    try {
      bytes = plain.serialize();
    } finally {
      await plain.close();
    }

    const bindings = (
      getFFI() as unknown as {
        bindings: { lattice_deserialize_v5?: unknown; lattice_deserialize_borrowed_v5?: unknown };
      }
    ).bindings;
    const saved = [bindings.lattice_deserialize_v5, bindings.lattice_deserialize_borrowed_v5];
    bindings.lattice_deserialize_v5 = undefined;
    bindings.lattice_deserialize_borrowed_v5 = undefined;
    try {
      for (const copy of [true, false]) {
        expect(codeOfSync(() => deserialize(bytes, { syncMode: 'normal' }, copy))).toBe(
          LatticeErrorCode.Unsupported
        );
        // Without any v5-only option, the v4 entry point still opens.
        await deserialize(bytes, {}, copy).close();
      }
    } finally {
      [bindings.lattice_deserialize_v5, bindings.lattice_deserialize_borrowed_v5] = saved;
    }
  });
});
