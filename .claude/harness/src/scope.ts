// Scope matching. A scope is a path glob an agent claims for the duration of a task.
// The overlap test is deliberately conservative: over-reporting a conflict costs a
// little parallelism, under-reporting costs somebody their work.
import { createHash } from "node:crypto";

const WILDCARD = /[*?[\]]/;
const REGEX_LITERAL = /[.+^${}()|[\]\\]/;

/** Canonical form: POSIX separators, no `./` prefix, a bare directory means everything under it. */
export function normalizeScope(scope: string): string {
  let out = scope.trim().replace(/\\/g, "/");
  while (out.startsWith("./")) out = out.slice(2);
  while (out.startsWith("/")) out = out.slice(1);
  if (out.length > 1 && out.endsWith("/")) out = `${out.slice(0, -1)}/**`;
  return out.length === 0 ? "**" : out;
}

function escapeLiteral(ch: string): string {
  return REGEX_LITERAL.test(ch) ? `\\${ch}` : ch;
}

/** Translates a glob to a regex. `**` crosses directories, `*` and `?` stay within one. */
export function globToRegExp(glob: string): RegExp {
  let pattern = "";
  let i = 0;
  while (i < glob.length) {
    const ch = glob.charAt(i);
    if (ch === "*") {
      if (glob.charAt(i + 1) === "*") {
        if (glob.charAt(i + 2) === "/") {
          // `**/` also matches zero directories, so `src/**/x.zig` matches `src/x.zig`.
          pattern += "(?:.*/)?";
          i += 3;
        } else {
          pattern += ".*";
          i += 2;
        }
      } else {
        pattern += "[^/]*";
        i += 1;
      }
    } else if (ch === "?") {
      pattern += "[^/]";
      i += 1;
    } else {
      pattern += escapeLiteral(ch);
      i += 1;
    }
  }
  return new RegExp(`^${pattern}$`);
}

/** The longest wildcard-free directory the glob is rooted at. `""` means the repo root. */
export function staticPrefix(glob: string): string {
  const normalized = normalizeScope(glob);
  const wildcard = normalized.search(WILDCARD);
  if (wildcard === -1) return normalized;
  const head = normalized.slice(0, wildcard);
  const cut = head.lastIndexOf("/");
  return cut === -1 ? "" : head.slice(0, cut);
}

/** True when `child` is `parent` itself or sits underneath it. `""` is the repo root. */
export function isPathPrefix(parent: string, child: string): boolean {
  if (parent.length === 0) return true;
  return child === parent || child.startsWith(`${parent}/`);
}

/**
 * Does this scope cover this file? A wildcard-free scope covers the exact path and,
 * when it names a directory, everything beneath it — which is what people mean by
 * "I am working on `src/storage`".
 */
export function matchesPath(scope: string, filePath: string): boolean {
  const normalizedScope = normalizeScope(scope);
  const target = normalizeScope(filePath);
  if (!WILDCARD.test(normalizedScope)) return isPathPrefix(normalizedScope, target);
  return globToRegExp(normalizedScope).test(target);
}

/** Could two scopes ever touch the same file? */
export function scopesOverlap(a: string, b: string): boolean {
  const left = normalizeScope(a);
  const right = normalizeScope(b);
  if (left === right) return true;

  const leftPrefix = staticPrefix(left);
  const rightPrefix = staticPrefix(right);
  if (isPathPrefix(leftPrefix, rightPrefix) || isPathPrefix(rightPrefix, leftPrefix)) return true;

  return matchesPath(left, rightPrefix) || matchesPath(right, leftPrefix);
}

/** Stable, readable, collision-resistant filename for a scope's lease. */
export function leaseKey(scope: string): string {
  const normalized = normalizeScope(scope);
  const slug = normalized
    .replace(/[^a-zA-Z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 48);
  const digest = createHash("sha1").update(normalized).digest("hex").slice(0, 10);
  return `${slug.length === 0 ? "root" : slug}-${digest}`;
}
