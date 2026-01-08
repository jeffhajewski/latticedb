#!/bin/bash
#
# Build release binaries for all supported platforms.
# Zig can cross-compile from any host to any target.
#
# Usage: ./dist/build-release.sh [version]
# Example: ./dist/build-release.sh 0.1.0

set -euo pipefail

VERSION="${1:-dev}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BUILD_DIR="$SCRIPT_DIR/build"

TARGETS=(
    "x86_64-linux-gnu"
    "aarch64-linux-gnu"
    "x86_64-macos"
    "aarch64-macos"
)

echo "Building LatticeDB $VERSION for all platforms..."
echo "Project root: $PROJECT_ROOT"
echo ""

rm -rf "$BUILD_DIR"
mkdir -p "$BUILD_DIR"

for target in "${TARGETS[@]}"; do
    echo "=== Building for $target ==="

    target_dir="$BUILD_DIR/$target"
    mkdir -p "$target_dir/bin" "$target_dir/lib" "$target_dir/include"

    # Build shared library
    echo "  Building shared library..."
    zig build shared \
        -Dtarget="$target" \
        -Doptimize=ReleaseFast \
        --prefix "$target_dir" \
        --search-prefix "$PROJECT_ROOT"

    # Build CLI
    echo "  Building CLI..."
    zig build cli \
        -Dtarget="$target" \
        -Doptimize=ReleaseFast \
        --prefix "$target_dir" \
        --search-prefix "$PROJECT_ROOT"

    # Copy header
    cp "$PROJECT_ROOT/include/lattice.h" "$target_dir/include/"

    # Write version file
    echo "$VERSION" > "$target_dir/VERSION"

    echo "  Done: $target_dir"
    echo ""
done

echo "All builds complete."
echo ""
echo "Output directory: $BUILD_DIR"
ls -la "$BUILD_DIR"
