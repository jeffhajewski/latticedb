/**
 * Tests for Database.checkpoint and Database.setAutoCheckpoint.
 */

import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { Database } from '../src/database';
import { getFFI, isLibraryAvailable, LatticeErrorCode, LatticeError } from '../src/ffi';
import type { CheckpointMode } from '../src';

const describeIfNative = isLibraryAvailable() ? describe : describe.skip;

const PAGE_SIZE = 4096;

type CheckpointBindings = {
  lattice_checkpoint?: unknown;
  lattice_set_auto_checkpoint?: unknown;
};

function bindingsOf(): CheckpointBindings {
  return (getFFI() as unknown as { bindings: CheckpointBindings }).bindings;
}

function hasCheckpoint(): boolean {
  const bindings = bindingsOf();
  return Boolean(bindings.lattice_checkpoint && bindings.lattice_set_auto_checkpoint);
}

function walSize(dbPath: string): number {
  const wal = `${dbPath}-wal`;
  return fs.existsSync(wal) ? fs.statSync(wal).size : 0;
}

async function commitOne(db: Database, i: number): Promise<void> {
  await db.write(async (txn) => {
    await txn.createNode({ labels: ['Item'], properties: { i } });
  });
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

describeIfNative('checkpoint and setAutoCheckpoint', () => {
  let dir: string;

  beforeEach(() => {
    dir = fs.mkdtempSync(path.join(os.tmpdir(), 'lattice_checkpoint_'));
  });

  afterEach(() => {
    fs.rmSync(dir, { recursive: true, force: true });
  });

  test('with automatic checkpoints off the log grows, and truncate empties it', async () => {
    if (!hasCheckpoint()) return;
    const dbPath = path.join(dir, 'off.db');
    const db = new Database(dbPath, { create: true });
    await db.open();
    try {
      await db.setAutoCheckpoint(0);
      await commitOne(db, 0);
      const first = walSize(dbPath);
      for (let i = 1; i < 100; i++) {
        await commitOne(db, i);
      }
      const grown = walSize(dbPath);
      // One small commit is one 4096-byte frame, and nothing folds them.
      expect(grown).toBeGreaterThanOrEqual(first + 99 * PAGE_SIZE);

      const full = await db.checkpoint('full');
      expect(full.walTruncated).toBe(false);
      expect(full.pagesFlushed).toBeGreaterThan(0);
      expect(typeof full.durationNs).toBe('bigint');
      expect(full.checkpointLsn).toBeGreaterThan(BigInt(0));
      expect(walSize(dbPath)).toBeGreaterThanOrEqual(grown);

      const truncated = await db.checkpoint('truncate');
      expect(truncated.walTruncated).toBe(true);
      expect(truncated.checkpointLsn).toBeGreaterThanOrEqual(full.checkpointLsn);
      expect(walSize(dbPath)).toBeLessThan(grown);
      expect(walSize(dbPath)).toBeLessThanOrEqual(2 * PAGE_SIZE);

      // The default mode is truncate.
      await commitOne(db, 100);
      expect((await db.checkpoint()).walTruncated).toBe(true);
    } finally {
      await db.close();
    }

    const reopened = new Database(dbPath);
    await reopened.open();
    try {
      expect(await reopened.getNodesByLabel('Item')).toHaveLength(101);
    } finally {
      await reopened.close();
    }
  });

  test('passive leaves the log in place', async () => {
    if (!hasCheckpoint()) return;
    const dbPath = path.join(dir, 'passive.db');
    const db = new Database(dbPath, { create: true });
    await db.open();
    try {
      await db.setAutoCheckpoint(0);
      for (let i = 0; i < 10; i++) {
        await commitOne(db, i);
      }
      const before = walSize(dbPath);
      const stats = await db.checkpoint('passive');
      expect(stats.walTruncated).toBe(false);
      expect(walSize(dbPath)).toBeGreaterThanOrEqual(before);
    } finally {
      await db.close();
    }
  });

  test('a small interval keeps the log short', async () => {
    if (!hasCheckpoint()) return;
    const dbPath = path.join(dir, 'four.db');
    const db = new Database(dbPath, { create: true });
    await db.open();
    try {
      await db.setAutoCheckpoint(4);
      let largest = 0;
      for (let i = 0; i < 50; i++) {
        await commitOne(db, i);
        largest = Math.max(largest, walSize(dbPath));
      }
      expect(largest).toBeGreaterThan(0);
      expect(largest).toBeLessThan(8 * PAGE_SIZE);
    } finally {
      await db.close();
    }

    const reopened = new Database(dbPath);
    await reopened.open();
    try {
      expect(await reopened.getNodesByLabel('Item')).toHaveLength(50);
    } finally {
      await reopened.close();
    }
  });

  test('a minimum interval holds automatic checkpoints back', async () => {
    if (!hasCheckpoint()) return;
    const dbPath = path.join(dir, 'interval.db');
    const db = new Database(dbPath, { create: true });
    await db.open();
    try {
      await db.setAutoCheckpoint(4, 60_000);
      for (let i = 0; i < 20; i++) {
        await commitOne(db, i);
      }
      // The first automatic checkpoint may run; a minute must pass before
      // another, so the log holds more than four frames by the end.
      expect(walSize(dbPath)).toBeGreaterThan(8 * PAGE_SIZE);
    } finally {
      await db.close();
    }
  });

  test('a read-only handle refuses checkpoint and ignores the setter', async () => {
    if (!hasCheckpoint()) return;
    const dbPath = path.join(dir, 'ro.db');
    const writer = new Database(dbPath, { create: true });
    await writer.open();
    await commitOne(writer, 0);
    await writer.close();

    const db = new Database(dbPath, { readOnly: true });
    await db.open();
    try {
      expect(await codeOf(db.checkpoint())).toBe(LatticeErrorCode.ReadOnly);
      expect(await codeOf(db.checkpoint('passive'))).toBe(LatticeErrorCode.ReadOnly);
      await db.setAutoCheckpoint(10);
      expect(await db.getNodesByLabel('Item')).toHaveLength(1);
    } finally {
      await db.close();
    }
  });

  test('bad arguments are refused before the library is called', async () => {
    if (!hasCheckpoint()) return;
    const dbPath = path.join(dir, 'args.db');
    const db = new Database(dbPath, { create: true });
    await db.open();
    try {
      await expect(db.checkpoint('sometimes' as CheckpointMode)).rejects.toThrow(TypeError);
      await expect(db.checkpoint('TRUNCATE' as CheckpointMode)).rejects.toThrow(TypeError);
      await expect(db.setAutoCheckpoint(-1)).rejects.toThrow(RangeError);
      await expect(db.setAutoCheckpoint(10, -5)).rejects.toThrow(RangeError);
      await expect(db.setAutoCheckpoint(1.5)).rejects.toThrow(RangeError);
      await expect(db.setAutoCheckpoint(Number.NaN)).rejects.toThrow(RangeError);
      await expect(db.setAutoCheckpoint(2 ** 64)).rejects.toThrow(RangeError);
    } finally {
      await db.close();
    }
  });

  test('an older library without the symbols throws Unsupported', async () => {
    const dbPath = path.join(dir, 'old.db');
    const db = new Database(dbPath, { create: true });
    await db.open();
    const bindings = bindingsOf();
    const saved = { ...bindings };
    bindings.lattice_checkpoint = undefined;
    bindings.lattice_set_auto_checkpoint = undefined;
    try {
      expect(await codeOf(db.checkpoint())).toBe(LatticeErrorCode.Unsupported);
      expect(await codeOf(db.setAutoCheckpoint(0))).toBe(LatticeErrorCode.Unsupported);
    } finally {
      bindings.lattice_checkpoint = saved.lattice_checkpoint;
      bindings.lattice_set_auto_checkpoint = saved.lattice_set_auto_checkpoint;
      await db.close();
    }
  });

  test('a closed database throws', async () => {
    const db = new Database(path.join(dir, 'closed.db'), { create: true });
    await expect(db.checkpoint()).rejects.toThrow('Database is not open');
    await expect(db.setAutoCheckpoint(0)).rejects.toThrow('Database is not open');
  });
});
