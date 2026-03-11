#!/bin/bash
#
# Build cross-compiled Linux binaries and run container integration tests.
#
# Usage:
#   ./run-all.sh                    # Build + run all distros
#   ./run-all.sh ubuntu-22.04       # Build + run single distro
#   ./run-all.sh --skip-build       # Run without rebuilding binaries
#   ./run-all.sh --skip-build alpine-3.19

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
BUILD_DIR="$PROJECT_ROOT/dist/build"

SKIP_BUILD=false
DISTRO=""

for arg in "$@"; do
    case "$arg" in
        --skip-build)
            SKIP_BUILD=true
            ;;
        *)
            DISTRO="$arg"
            ;;
    esac
done

# Step 1: Build cross-compiled binaries
if [ "$SKIP_BUILD" = false ]; then
    echo "=== Building glibc binaries (x86_64-linux-gnu) ==="
    mkdir -p "$BUILD_DIR/x86_64-linux-gnu/bin" \
             "$BUILD_DIR/x86_64-linux-gnu/lib" \
             "$BUILD_DIR/x86_64-linux-gnu/include"

    cd "$PROJECT_ROOT"
    zig build cli -Dtarget=x86_64-linux-gnu -Doptimize=ReleaseFast
    cp zig-out/bin/lattice "$BUILD_DIR/x86_64-linux-gnu/bin/"
    zig build shared -Dtarget=x86_64-linux-gnu -Doptimize=ReleaseFast
    cp zig-out/lib/liblattice.so "$BUILD_DIR/x86_64-linux-gnu/lib/"
    cp include/lattice.h "$BUILD_DIR/x86_64-linux-gnu/include/"
    echo "  Done."

    echo ""
    echo "=== Building musl binaries (x86_64-linux-musl) ==="
    mkdir -p "$BUILD_DIR/x86_64-linux-musl/bin" \
             "$BUILD_DIR/x86_64-linux-musl/lib" \
             "$BUILD_DIR/x86_64-linux-musl/include"

    zig build cli -Dtarget=x86_64-linux-musl -Doptimize=ReleaseFast
    cp zig-out/bin/lattice "$BUILD_DIR/x86_64-linux-musl/bin/"
    zig build shared -Dtarget=x86_64-linux-musl -Doptimize=ReleaseFast
    cp zig-out/lib/liblattice.so "$BUILD_DIR/x86_64-linux-musl/lib/"
    cp include/lattice.h "$BUILD_DIR/x86_64-linux-musl/include/"
    echo "  Done."
    echo ""
fi

# Verify binaries exist
for target in x86_64-linux-gnu x86_64-linux-musl; do
    if [ ! -f "$BUILD_DIR/$target/bin/lattice" ]; then
        echo "ERROR: Missing $BUILD_DIR/$target/bin/lattice"
        echo "Run without --skip-build to build binaries first."
        exit 1
    fi
done

# Step 2: Run container tests
cd "$SCRIPT_DIR"

if [ -n "$DISTRO" ]; then
    echo "=== Running container tests for: $DISTRO ==="
    docker compose up --build --abort-on-container-exit "$DISTRO"
else
    echo "=== Running container tests for all distros ==="
    OVERALL_EXIT=0
    for distro in ubuntu-22.04 ubuntu-24.04 debian-bookworm alpine-3.19 fedora-40; do
        echo ""
        echo ">>> $distro <<<"
        if docker compose up --build --abort-on-container-exit "$distro" 2>&1; then
            echo ">>> $distro: PASSED <<<"
        else
            echo ">>> $distro: FAILED <<<"
            OVERALL_EXIT=1
        fi
        docker compose down --remove-orphans 2>/dev/null || true
    done

    echo ""
    echo "========================================"
    if [ "$OVERALL_EXIT" -eq 0 ]; then
        echo "ALL DISTROS PASSED"
    else
        echo "SOME DISTROS FAILED"
        exit 1
    fi
    echo "========================================"
fi
