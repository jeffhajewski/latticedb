#!/bin/bash
#
# Install LatticeDB CLI from GitHub releases.
#
# Usage:
#   curl -fsSL https://raw.githubusercontent.com/USER/latticedb/main/dist/install.sh | bash
#   curl -fsSL https://raw.githubusercontent.com/USER/latticedb/main/dist/install.sh | bash -s -- 0.1.0
#
# Environment variables:
#   LATTICE_INSTALL_DIR  - Installation directory (default: /usr/local/bin or ~/.local/bin)
#   LATTICE_VERSION      - Version to install (default: latest)

set -euo pipefail

REPO="jeffhajewski/latticedb"
VERSION="${1:-${LATTICE_VERSION:-latest}}"
INSTALL_DIR="${LATTICE_INSTALL_DIR:-}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

info() { echo -e "${GREEN}==>${NC} $1"; }
warn() { echo -e "${YELLOW}warning:${NC} $1"; }
error() { echo -e "${RED}error:${NC} $1" >&2; exit 1; }

# Detect OS
detect_os() {
    case "$(uname -s)" in
        Linux*)  echo "linux" ;;
        Darwin*) echo "macos" ;;
        *) error "Unsupported operating system: $(uname -s)" ;;
    esac
}

# Detect architecture
detect_arch() {
    case "$(uname -m)" in
        x86_64|amd64) echo "x86_64" ;;
        arm64|aarch64) echo "aarch64" ;;
        *) error "Unsupported architecture: $(uname -m)" ;;
    esac
}

# Get latest version from GitHub API
get_latest_version() {
    local response
    response=$(curl -fsSL "https://api.github.com/repos/$REPO/releases/latest" 2>/dev/null) || {
        error "Failed to fetch latest version. Check your internet connection."
    }
    echo "$response" | grep '"tag_name":' | sed -E 's/.*"v?([^"]+)".*/\1/'
}

# Determine install directory
get_install_dir() {
    if [ -n "$INSTALL_DIR" ]; then
        echo "$INSTALL_DIR"
    elif [ -w "/usr/local/bin" ]; then
        echo "/usr/local/bin"
    else
        mkdir -p "$HOME/.local/bin"
        echo "$HOME/.local/bin"
    fi
}

# Check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Main installation
main() {
    info "Installing LatticeDB..."

    # Check dependencies
    if ! command_exists curl; then
        error "curl is required but not installed"
    fi

    if ! command_exists tar; then
        error "tar is required but not installed"
    fi

    OS=$(detect_os)
    ARCH=$(detect_arch)

    info "Detected platform: $OS-$ARCH"

    # Resolve version
    if [ "$VERSION" = "latest" ]; then
        info "Fetching latest version..."
        VERSION=$(get_latest_version)
        if [ -z "$VERSION" ]; then
            error "Could not determine latest version"
        fi
    fi
    # Remove 'v' prefix if present
    VERSION="${VERSION#v}"
    info "Version: $VERSION"

    # Build download URL
    case "$OS" in
        linux)
            TARGET="${ARCH}-linux-gnu"
            ;;
        macos)
            TARGET="${ARCH}-macos"
            ;;
    esac

    URL="https://github.com/$REPO/releases/download/v$VERSION/latticedb-$VERSION-$TARGET.tar.gz"
    INSTALL_DIR=$(get_install_dir)

    info "Downloading from $URL..."

    # Create temp directory
    TMP_DIR=$(mktemp -d)
    trap 'rm -rf "$TMP_DIR"' EXIT

    # Download and extract
    if ! curl -fsSL "$URL" -o "$TMP_DIR/latticedb.tar.gz"; then
        error "Download failed. Check that version $VERSION exists."
    fi

    info "Extracting..."
    tar -xzf "$TMP_DIR/latticedb.tar.gz" -C "$TMP_DIR"

    # Find the extracted directory
    EXTRACT_DIR=$(find "$TMP_DIR" -maxdepth 1 -type d -name "latticedb-*" | head -1)
    if [ -z "$EXTRACT_DIR" ]; then
        EXTRACT_DIR="$TMP_DIR"
    fi

    # Install binary
    info "Installing to $INSTALL_DIR..."

    if [ -f "$EXTRACT_DIR/bin/lattice" ]; then
        cp "$EXTRACT_DIR/bin/lattice" "$INSTALL_DIR/"
        chmod +x "$INSTALL_DIR/lattice"
    else
        error "Binary not found in archive"
    fi

    # Install shared library (needed by Python/TypeScript bindings)
    if [ -d "$EXTRACT_DIR/lib" ]; then
        LIB_DIR="${LATTICE_LIB_DIR:-}"
        if [ -z "$LIB_DIR" ]; then
            if [ -w "/usr/local/lib" ]; then
                LIB_DIR="/usr/local/lib"
            else
                LIB_DIR="$HOME/.local/lib"
                mkdir -p "$LIB_DIR"
            fi
        fi
        cp "$EXTRACT_DIR/lib/"* "$LIB_DIR/" 2>/dev/null || true
        info "Libraries installed to $LIB_DIR"
    fi

    # Verify installation
    echo ""
    if [ -x "$INSTALL_DIR/lattice" ]; then
        info "LatticeDB v$VERSION installed successfully!"
        echo ""

        # Check if in PATH
        if ! command_exists lattice || [ "$(command -v lattice)" != "$INSTALL_DIR/lattice" ]; then
            warn "$INSTALL_DIR is not in your PATH (or an older version shadows it)"
            echo ""
            echo "Add to your shell config:"
            echo "  export PATH=\"$INSTALL_DIR:\$PATH\""
        fi
    else
        error "Installation failed"
    fi
}

main
