// Tunables for the harness. Everything here is safe to edit by hand; the file is
// committed so every agent working this repo plays by the same rules.
import * as fs from "node:fs";
import * as path from "node:path";
import { HARNESS_DIR } from "./paths.ts";

/** What the PreToolUse guard does when an edit lands on somebody else's scope. */
export type GuardMode = "deny" | "warn" | "off";

export interface HarnessConfig {
  /** How long a lease survives without a heartbeat before anyone may reclaim it. */
  leaseTtlMs: number;
  /** How often the hooks refresh a heartbeat. Purely informational; heartbeats are cheap. */
  heartbeatIntervalMs: number;
  /** Reclaims allowed before a task parks in `blocked` for a human instead of looping. */
  maxAttempts: number;
  guard: {
    mode: GuardMode;
    /** Globs nobody edits directly — the harness owns them, agents go through the CLI. */
    protectedPaths: string[];
  };
  /** Acceptance commands a task inherits when it does not declare its own. */
  defaultAcceptance: string[];
}

const DEFAULTS: HarnessConfig = {
  leaseTtlMs: 30 * 60 * 1000,
  heartbeatIntervalMs: 60 * 1000,
  maxAttempts: 3,
  guard: {
    mode: "deny",
    protectedPaths: [".claude/state/**"],
  },
  defaultAcceptance: [],
};

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function numberOr(value: unknown, fallback: number): number {
  return typeof value === "number" && Number.isFinite(value) && value > 0 ? value : fallback;
}

function stringsOr(value: unknown, fallback: string[]): string[] {
  if (!Array.isArray(value)) return fallback;
  return value.filter((entry): entry is string => typeof entry === "string");
}

function guardModeOr(value: unknown, fallback: GuardMode): GuardMode {
  return value === "deny" || value === "warn" || value === "off" ? value : fallback;
}

let cached: HarnessConfig | null = null;

/** Reads `config.json`, falling back to defaults for anything missing or malformed. */
export function loadConfig(): HarnessConfig {
  if (cached !== null) return cached;
  const override = process.env.HARNESS_CONFIG;
  const file = override !== undefined && override.length > 0
    ? path.resolve(override)
    : path.join(HARNESS_DIR, "config.json");

  let parsed: unknown = null;
  try {
    parsed = JSON.parse(fs.readFileSync(file, "utf8"));
  } catch {
    // A missing or broken config must not stop work: defaults are always usable.
    parsed = null;
  }

  const raw = isRecord(parsed) ? parsed : {};
  const guard = isRecord(raw["guard"]) ? raw["guard"] : {};

  cached = {
    leaseTtlMs: numberOr(raw["leaseTtlMs"], DEFAULTS.leaseTtlMs),
    heartbeatIntervalMs: numberOr(raw["heartbeatIntervalMs"], DEFAULTS.heartbeatIntervalMs),
    maxAttempts: numberOr(raw["maxAttempts"], DEFAULTS.maxAttempts),
    guard: {
      mode: guardModeOr(guard["mode"], DEFAULTS.guard.mode),
      protectedPaths: stringsOr(guard["protectedPaths"], DEFAULTS.guard.protectedPaths),
    },
    defaultAcceptance: stringsOr(raw["defaultAcceptance"], DEFAULTS.defaultAcceptance),
  };
  return cached;
}

/** Test seam: drops the memoized config so a changed file is picked up. */
export function resetConfigCache(): void {
  cached = null;
}
