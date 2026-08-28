// Crash-safe state I/O. Every mutation is a short transaction guarded by an
// exclusive-create lock file and committed with a temp-write + rename, so a process
// killed mid-write leaves either the old record or the new one — never a torn one.
import * as fs from "node:fs";
import * as path from "node:path";
import { randomBytes } from "node:crypto";
import { ensureDirs, journalPath } from "./paths.ts";

/** A lock older than this is assumed to belong to a dead process and is broken. */
const LOCK_STALE_MS = 15_000;
/** How long a caller waits for a contended lock before giving up. */
const LOCK_WAIT_MS = 10_000;
const LOCK_POLL_MS = 25;

export interface Versioned {
  version: number;
}

/** Reads the `code` off a thrown filesystem error without depending on Node's type defs. */
export function errorCode(err: unknown): string | undefined {
  if (typeof err === "object" && err !== null && "code" in err) {
    const code = (err as { code?: unknown }).code;
    return typeof code === "string" ? code : undefined;
  }
  return undefined;
}

/**
 * Blocks the thread. The CLI is deliberately synchronous — each command is one
 * short transaction, and blocking keeps the lock discipline obvious.
 */
function sleepSync(ms: number): void {
  const shared = new Int32Array(new SharedArrayBuffer(4));
  Atomics.wait(shared, 0, 0, ms);
}

/** Missing and corrupt both read as "absent"; callers decide what absent means. */
export function readJson<T>(file: string): T | null {
  try {
    return JSON.parse(fs.readFileSync(file, "utf8")) as T;
  } catch {
    return null;
  }
}

export function writeJsonAtomic(file: string, value: unknown): void {
  fs.mkdirSync(path.dirname(file), { recursive: true });
  const tmp = `${file}.${process.pid}.${randomBytes(4).toString("hex")}.tmp`;
  try {
    fs.writeFileSync(tmp, `${JSON.stringify(value, null, 2)}\n`, "utf8");
    fs.renameSync(tmp, file);
  } catch (err) {
    try {
      fs.unlinkSync(tmp);
    } catch {
      // The temp file is already gone or unreachable; the original error matters more.
    }
    throw err;
  }
}

/** Removes a lock whose owner died mid-transaction. Returns true if one was broken. */
function breakStaleLock(lockPath: string): boolean {
  try {
    const age = Date.now() - fs.statSync(lockPath).mtimeMs;
    if (age < LOCK_STALE_MS) return false;
    fs.unlinkSync(lockPath);
    return true;
  } catch {
    // Somebody else broke or released it first — either way, retry the acquire.
    return true;
  }
}

/**
 * Runs `fn` while holding an exclusive lock keyed on `file`. `wx` is atomic on both
 * NTFS and POSIX, which is what makes this safe across concurrent agent processes.
 */
export function withFileLock<T>(file: string, fn: () => T): T {
  const lockPath = `${file}.lock`;
  fs.mkdirSync(path.dirname(lockPath), { recursive: true });
  const deadline = Date.now() + LOCK_WAIT_MS;
  let fd: number | undefined;

  for (;;) {
    try {
      fd = fs.openSync(lockPath, "wx");
      break;
    } catch (err) {
      if (errorCode(err) !== "EEXIST") throw err;
      if (breakStaleLock(lockPath)) continue;
      if (Date.now() >= deadline) {
        throw new Error(`timed out waiting for the lock on ${path.basename(file)}`);
      }
      sleepSync(LOCK_POLL_MS);
    }
  }

  try {
    fs.writeSync(fd, `${process.pid}`);
    return fn();
  } finally {
    try {
      if (fd !== undefined) fs.closeSync(fd);
    } catch {
      // Closing a already-closed descriptor is not worth failing the transaction over.
    }
    try {
      fs.unlinkSync(lockPath);
    } catch {
      // Lock already broken by a reaper; the transaction itself still committed.
    }
  }
}

/**
 * Read-modify-write under lock. `mutate` receives the current record (or null when the
 * file does not exist) and returns the next one, or null to delete it.
 */
export function updateJson<T extends Versioned>(
  file: string,
  mutate: (current: T | null) => T | null,
): T | null {
  return withFileLock(file, () => {
    const current = readJson<T>(file);
    const next = mutate(current);
    if (next === null) {
      try {
        fs.unlinkSync(file);
      } catch {
        // Already deleted; the caller's intent is satisfied either way.
      }
      return null;
    }
    next.version = (current === null ? 0 : current.version) + 1;
    writeJsonAtomic(file, next);
    return next;
  });
}

/** Lists committed records in a state directory, skipping temp files and locks. */
export function listRecords<T>(dir: string): T[] {
  let names: string[];
  try {
    names = fs.readdirSync(dir);
  } catch {
    return [];
  }
  const out: T[] = [];
  for (const name of names) {
    if (!name.endsWith(".json")) continue;
    const record = readJson<T>(path.join(dir, name));
    if (record !== null) out.push(record);
  }
  return out;
}

/**
 * Append-only audit trail. Best-effort by design: losing a journal line is a shame,
 * losing the command that produced it is not acceptable.
 */
export function appendJournal(event: Record<string, unknown>): void {
  try {
    ensureDirs();
    const line = JSON.stringify({ at: new Date().toISOString(), ...event });
    fs.appendFileSync(journalPath(), `${line}\n`, "utf8");
  } catch {
    // Intentionally swallowed.
  }
}

export function nowIso(): string {
  return new Date().toISOString();
}
