#!/usr/bin/env bash
#
# Prepare a LatticeDB release in one command.
#
# This script orchestrates:
# 1) Version bump across all tracked version sources.
# 2) Optional npm lockfile refresh.
# 3) Consistency verification.
# 4) Optional test suite execution.
# 5) Optional git tag creation/push.
#
# Usage:
#   scripts/prepare_release.sh 0.3.0
#   scripts/prepare_release.sh 0.3.0 --skip-tests
#   scripts/prepare_release.sh 0.3.0 --tag
#   scripts/prepare_release.sh 0.3.0 --tag --push-tag
#   scripts/prepare_release.sh 0.3.0 --rehearsal

set -euo pipefail

usage() {
  cat <<'EOF'
Usage: scripts/prepare_release.sh <version> [options]

Options:
  --rehearsal      Print commands without executing them.
  --no-lockfile    Skip npm package-lock refresh.
  --skip-tests     Skip test commands.
  --tag            Create git tag v<version>.
  --push-tag       Push git tag to origin (implies --tag).
  -h, --help       Show this help text.

Examples:
  scripts/prepare_release.sh 0.3.0
  scripts/prepare_release.sh 0.3.0 --skip-tests --tag
  scripts/prepare_release.sh 0.3.0 --rehearsal
EOF
}

if [[ $# -lt 1 ]]; then
  usage
  exit 1
fi

VERSION="$1"
shift

REHEARSAL=0
REFRESH_LOCKFILE=1
RUN_TESTS=1
CREATE_TAG=0
PUSH_TAG=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --rehearsal)
      REHEARSAL=1
      shift
      ;;
    --no-lockfile)
      REFRESH_LOCKFILE=0
      shift
      ;;
    --skip-tests)
      RUN_TESTS=0
      shift
      ;;
    --tag)
      CREATE_TAG=1
      shift
      ;;
    --push-tag)
      CREATE_TAG=1
      PUSH_TAG=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "error: unknown option: $1" >&2
      usage
      exit 1
      ;;
  esac
done

if [[ ! "$VERSION" =~ ^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]]; then
  echo "error: VERSION must be strict semver MAJOR.MINOR.PATCH, got '$VERSION'" >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

run() {
  echo "+ $*"
  if [[ "$REHEARSAL" -eq 0 ]]; then
    "$@"
  fi
}

run_shell() {
  local cmd="$1"
  echo "+ $cmd"
  if [[ "$REHEARSAL" -eq 0 ]]; then
    bash -lc "$cmd"
  fi
}

echo "Preparing LatticeDB release $VERSION"
echo "Repository: $REPO_ROOT"

run python3 scripts/bump_version.py "$VERSION"

if [[ "$REFRESH_LOCKFILE" -eq 1 ]]; then
  run npm --prefix bindings/typescript install --package-lock-only --ignore-scripts
  # Re-assert expected versions after lockfile refresh.
  run python3 scripts/bump_version.py "$VERSION"
  run python3 scripts/bump_version.py --check "$VERSION" --strict-lockfile
else
  run python3 scripts/bump_version.py --check "$VERSION"
fi

if [[ "$RUN_TESTS" -eq 1 ]]; then
  run zig build test
  run_shell "cd bindings/python && python3 -m pytest tests -q"
  run npm --prefix bindings/typescript test -- --runInBand
fi

if [[ "$CREATE_TAG" -eq 1 ]]; then
  run git tag "v$VERSION"
fi

if [[ "$PUSH_TAG" -eq 1 ]]; then
  run git push origin "v$VERSION"
fi

run git status --short --branch

echo ""
echo "Release preparation complete for $VERSION."
echo "Next: commit changes (if not already committed) and push branch/tag."
