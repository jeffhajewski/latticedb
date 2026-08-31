#!/bin/bash
#
# Package built binaries into release archives.
#
# Usage: ./dist/package-release.sh [version]
# Example: ./dist/package-release.sh 0.1.0
#
# Prerequisites:
#   Run build-release.sh first to create binaries in dist/build/

set -euo pipefail

VERSION="${1:-dev}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BUILD_DIR="$SCRIPT_DIR/build"
RELEASE_DIR="$SCRIPT_DIR/release"

if [ ! -d "$BUILD_DIR" ]; then
    echo "Error: Build directory not found: $BUILD_DIR"
    echo "Run ./dist/build-release.sh first"
    exit 1
fi

echo "Packaging LatticeDB $VERSION..."

rm -rf "$RELEASE_DIR"
mkdir -p "$RELEASE_DIR"

# Package each target
for target_dir in "$BUILD_DIR"/*; do
    if [ ! -d "$target_dir" ]; then
        continue
    fi

    target=$(basename "$target_dir")

    # Zip for Windows, which a Windows user can open without installing
    # anything first; a tarball everywhere else.
    case "$target" in
        *-windows-*) archive_name="latticedb-$VERSION-$target.zip" ;;
        *) archive_name="latticedb-$VERSION-$target.tar.gz" ;;
    esac

    echo "Creating $archive_name..."

    # Create a temp directory with the versioned name for consistent archive structure
    tmp_pkg="$BUILD_DIR/pkg-tmp"
    rm -rf "$tmp_pkg"
    mkdir -p "$tmp_pkg/latticedb-$VERSION"
    cp -r "$target_dir/bin" "$target_dir/lib" "$target_dir/include" "$target_dir/VERSION" "$tmp_pkg/latticedb-$VERSION/"

    case "$target" in
        *-windows-*)
            (cd "$tmp_pkg" && zip -qr "$RELEASE_DIR/$archive_name" "latticedb-$VERSION")
            ;;
        *)
            tar -czf "$RELEASE_DIR/$archive_name" -C "$tmp_pkg" "latticedb-$VERSION"
            ;;
    esac

    rm -rf "$tmp_pkg"
done

# Generate checksums
echo ""
echo "Generating checksums..."
(cd "$RELEASE_DIR" && shopt -s nullglob && shasum -a 256 *.tar.gz *.zip > SHA256SUMS)

echo ""
echo "Release packages created in: $RELEASE_DIR"
echo ""
ls -lh "$RELEASE_DIR"

echo ""
echo "SHA256 checksums:"
cat "$RELEASE_DIR/SHA256SUMS"
