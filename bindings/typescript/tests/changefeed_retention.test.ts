/**
 * Tests for the changefeed's retention policy: setting it at open, reading its
 * status, trimming, and what a reader sees when events have been dropped.
 */

import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { Database } from '../src/database';
import {
  changefeedRetentionCode,
  ChangefeedRetention,
  getFFI,
  isLibraryAvailable,
  LatticeChangesTrimmedError,
  LatticeError,
  LatticeErrorCode,
} from '../src/ffi';

const describeIfNative = isLibraryAvailable() ? describe : describe.skip;

function hasChangefeed(): boolean {
  const bindings = (getFFI() as unknown as { bindings: { lattice_changefeed_status?: unknown } })
    .bindings;
  return Boolean(bindings.lattice_changefeed_status);
}

/** Commit `count` transactions of one labelled node: two events each. */
async function commitNodes(db: Database, count: number): Promise<void> {
  for (let i = 0; i < count; i++) {
    await db.write(async (txn) => {
      await txn.createNode({ labels: ['N'] });
    });
  }
}

async function sequences(db: Database, afterSequence = BigInt(0)): Promise<number[]> {
  return (await db.changes({ afterSequence })).map((r) => Number(r.sequence));
}

describe('changefeed retention policies are parsed before anything opens', () => {
  test.each([
    ['all', 1, BigInt(0)],
    ['OFF', 4, BigInt(0)],
    ['last:1000', 2, BigInt(1000)],
    ['age:500ms', 3, BigInt(500)],
    ['age:90s', 3, BigInt(90_000)],
    ['age:7d', 3, BigInt(7 * 86_400_000)],
  ])('%s', (policy, mode, value) => {
    expect(changefeedRetentionCode(policy as ChangefeedRetention)).toEqual([mode, value]);
  });

  test.each(['sometimes', 'last:0', 'last:', 'last:-3', 'age:7', 'age:0s', 'age:7w', 'age:d'])(
    '%s is refused',
    (policy) => {
      expect(() => changefeedRetentionCode(policy as ChangefeedRetention)).toThrow(TypeError);
    }
  );
});

describeIfNative('changefeed retention', () => {
  let dir: string;

  beforeEach(() => {
    dir = fs.mkdtempSync(path.join(os.tmpdir(), 'lattice_lcf_'));
  });

  afterEach(() => {
    fs.rmSync(dir, { recursive: true, force: true });
  });

  test('the default keeps everything', async () => {
    if (!hasChangefeed()) return;
    const db = new Database(path.join(dir, 'all.db'), { create: true });
    await db.open();
    await commitNodes(db, 3);
    expect(await db.changefeedStatus()).toEqual({
      retention: 'all',
      oldestSequence: BigInt(1),
      lastSequence: BigInt(6),
    });
    expect(await db.trimChangefeed()).toBe(BigInt(0));
    expect(await sequences(db)).toEqual([1, 2, 3, 4, 5, 6]);
    await db.close();
  });

  test('last:N keeps the newest events and tells a reader that fell behind', async () => {
    if (!hasChangefeed()) return;
    const db = new Database(path.join(dir, 'last.db'), {
      create: true,
      changefeedRetention: 'last:4',
    });
    await db.open();
    await commitNodes(db, 5); // events 1..10
    expect(await db.trimChangefeed()).toBe(BigInt(6));
    expect(await db.changefeedStatus()).toEqual({
      retention: 'last:4',
      oldestSequence: BigInt(7),
      lastSequence: BigInt(10),
    });
    expect(await sequences(db)).toEqual([7, 8, 9, 10]);
    expect(await sequences(db, BigInt(6))).toEqual([7, 8, 9, 10]);
    const missed = db.changes({ afterSequence: BigInt(3) });
    await expect(missed).rejects.toBeInstanceOf(LatticeChangesTrimmedError);
    await expect(missed).rejects.toMatchObject({ code: LatticeErrorCode.NotFound });
    const [first] = await db.changes({ afterSequence: BigInt(6), limit: 1 });
    expect(first?.kind).toBe('node.insert');
    await db.close();
  });

  test('the file keeps its policy until a writable open names another', async () => {
    if (!hasChangefeed()) return;
    const dbPath = path.join(dir, 'stored.db');
    const status = async (options: object): Promise<string> => {
      const db = new Database(dbPath, options);
      await db.open();
      const { retention } = await db.changefeedStatus();
      await db.close();
      return retention;
    };
    expect(await status({ create: true, changefeedRetention: 'age:12h' })).toBe('age:12h');
    expect(await status({})).toBe('age:12h');
    expect(await status({ readOnly: true, changefeedRetention: 'off' })).toBe('age:12h');
    expect(await status({ changefeedRetention: 'all' })).toBe('all');
    expect(await status({})).toBe('all');
  });

  test('off writes nothing, reading is unsupported, and switching back on leaves a gap', async () => {
    if (!hasChangefeed()) return;
    const dbPath = path.join(dir, 'off.db');
    let db = new Database(dbPath, { create: true });
    await db.open();
    await commitNodes(db, 2); // events 1..4
    await db.close();

    db = new Database(dbPath, { changefeedRetention: 'off' });
    await db.open();
    await commitNodes(db, 2);
    await expect(db.changes()).rejects.toMatchObject({ code: LatticeErrorCode.Unsupported });
    await db.trimChangefeed();
    expect(await db.changefeedStatus()).toEqual({
      retention: 'off',
      oldestSequence: null,
      lastSequence: BigInt(4),
    });
    await db.close();

    db = new Database(dbPath, { changefeedRetention: 'all' });
    await db.open();
    await commitNodes(db, 1);
    await expect(db.changes({ afterSequence: BigInt(4) })).rejects.toBeInstanceOf(
      LatticeChangesTrimmedError
    );
    expect(await sequences(db)).toEqual([6, 7]);
    await db.close();
  });

  test('trim is refused while a write transaction is open', async () => {
    if (!hasChangefeed()) return;
    const db = new Database(path.join(dir, 'busy.db'), {
      create: true,
      changefeedRetention: 'last:1',
    });
    await db.open();
    await commitNodes(db, 2);
    let code: number | undefined;
    await db
      .write(async () => {
        try {
          await db.trimChangefeed();
        } catch (err) {
          expect(err).toBeInstanceOf(LatticeError);
          code = (err as LatticeError).code;
        }
      })
      .catch(() => undefined);
    expect(code).toBe(LatticeErrorCode.LockTimeout);
    await db.close();
  });

  test('an invalid policy is refused at open', async () => {
    const db = new Database(path.join(dir, 'bad.db'), {
      create: true,
      changefeedRetention: 'last:0' as ChangefeedRetention,
    });
    await expect(db.open()).rejects.toThrow(TypeError);
  });
});
