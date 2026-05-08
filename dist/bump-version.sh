#!/bin/bash
#
# Bump all version files, commit, tag, and push a release.
# Ensures consistency between Zig, Python, and npm packages.
#
# Usage: ./dist/bump-version.sh <semver>
# Example: ./dist/bump-version.sh 0.10.0

set -euo pipefail

VERSION="${1:-}"
REPO="jeffhajewski/latticedb"

if [ -z "$VERSION" ]; then
    echo "Usage: $0 <version>"
    echo "Example: $0 0.10.0"
    exit 1
fi

# Validate semver-ish format
if ! echo "$VERSION" | grep -qE '^[0-9]+\.[0-9]+\.[0-9]+$'; then
    echo "Error: version must be in semver format (e.g. 0.10.0)"
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

# Check working directory is clean
if [ -n "$(git status --porcelain)" ]; then
    echo "Error: working directory is not clean. Commit or stash changes first."
    git status --short
    exit 1
fi

echo "Bumping LatticeDB to v$VERSION..."
echo ""

# ── 1. Update Zig version ────────────────────────────────────
echo "  build.zig: $(grep 'const version' build.zig | sed -E 's/.*\"([^\"]+)\".*/\1/') -> $VERSION"
sed -i '' -E "s/const version = \"[0-9]+\.[0-9]+\.[0-9]+\"/const version = \"$VERSION\"/" build.zig

# ── 2. Update Python version ─────────────────────────────────
echo "  pyproject.toml: $(grep '^version' bindings/python/pyproject.toml | sed -E 's/[^\"]*\"([^\"]+)\".*/\1/') -> $VERSION"
sed -i '' -E "s/^version = \"[0-9]+\.[0-9]+\.[0-9]+\"/version = \"$VERSION\"/" bindings/python/pyproject.toml

# ── 3. Update npm version ────────────────────────────────────
echo "  package.json: $(grep '"version"' bindings/typescript/package.json | head -1 | sed -E 's/.*\"version\": \"([^\"]+)\".*/\1/') -> $VERSION"
sed -i '' -E "s/\"version\": \"[0-9]+\.[0-9]+\.[0-9]+\"/\"version\": \"$VERSION\"/" bindings/typescript/package.json

echo ""

# ── 4. Verify all three match ────────────────────────────────
ZIG_V=$(grep 'const version' build.zig | sed -E 's/.*"([^"]+)".*/\1/')
PY_V=$(grep '^version' bindings/python/pyproject.toml | sed -E 's/[^"]*"([^"]+)".*/\1/')
NPM_V=$(grep '"version"' bindings/typescript/package.json | head -1 | sed -E 's/.*"version": "([^"]+)".*/\1/')

if [ "$ZIG_V" != "$VERSION" ] || [ "$PY_V" != "$VERSION" ] || [ "$NPM_V" != "$VERSION" ]; then
    echo "Error: Version mismatch after bump!"
    echo "  build.zig:    $ZIG_V"
    echo "  pyproject.toml: $PY_V"
    echo "  package.json:   $NPM_V"
    git checkout -- build.zig bindings/python/pyproject.toml bindings/typescript/package.json
    exit 1
fi

echo "✓ All version files updated to $VERSION"
echo ""

# ── 5. Commit ────────────────────────────────────────────────
git add build.zig bindings/python/pyproject.toml bindings/typescript/package.json
git commit -m "Release v$VERSION"

# ── 6. Tag ───────────────────────────────────────────────────
git tag -a "v$VERSION" -m "LatticeDB v$VERSION"

# ── 7. Push ──────────────────────────────────────────────────
git push origin main
git push origin "v$VERSION"

echo ""
echo "✓ v$VERSION committed and pushed to origin"
echo "  CI will build and publish automatically."
echo "  Monitor: https://github.com/$REPO/actions"
