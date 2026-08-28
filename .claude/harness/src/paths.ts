// Filesystem layout for the harness: where the code lives and where shared state goes.
import * as fs from "node:fs";
import * as path from "node:path";
import { fileURLToPath } from "node:url";

const SRC_DIR = path.dirname(fileURLToPath(import.meta.url));

/** `.claude/harness` — harness code and committed config. */
export const HARNESS_DIR = path.resolve(SRC_DIR, "..");
/** `.claude` — Claude Code project configuration. */
export const CLAUDE_DIR = path.resolve(HARNESS_DIR, "..");
/** Repository root. */
export const REPO_ROOT = path.resolve(CLAUDE_DIR, "..");

/**
 * Shared coordination state. `HARNESS_STATE_DIR` redirects it, which is how the
 * self-test runs against a throwaway directory instead of the live board.
 */
export function stateDir(): string {
  const override = process.env.HARNESS_STATE_DIR;
  return override !== undefined && override.length > 0
    ? path.resolve(override)
    : path.join(CLAUDE_DIR, "state");
}

export function tasksDir(): string {
  return path.join(stateDir(), "tasks");
}

export function leasesDir(): string {
  return path.join(stateDir(), "leases");
}

export function agentsDir(): string {
  return path.join(stateDir(), "agents");
}

export function journalPath(): string {
  return path.join(stateDir(), "journal.jsonl");
}

/** The rendered board. Stays beside the state whenever the state dir is redirected. */
export function progressPath(): string {
  const override = process.env.HARNESS_STATE_DIR;
  return override !== undefined && override.length > 0
    ? path.join(stateDir(), "PROGRESS.md")
    : path.join(CLAUDE_DIR, "PROGRESS.md");
}

export function ensureDirs(): void {
  for (const dir of [stateDir(), tasksDir(), leasesDir(), agentsDir()]) {
    fs.mkdirSync(dir, { recursive: true });
  }
}

/**
 * Repo-relative, POSIX-separated form of a path so it can be compared against scopes.
 * Paths outside the repo come back absolute and simply never match a scope.
 */
export function toRepoRelative(target: string): string {
  const abs = path.isAbsolute(target) ? target : path.resolve(REPO_ROOT, target);
  const rel = path.relative(REPO_ROOT, abs);
  if (rel.length === 0) return ".";
  if (rel.startsWith("..")) return abs.split(path.sep).join("/");
  return rel.split(path.sep).join("/");
}
