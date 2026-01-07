#!/usr/bin/env python3
"""
Build native library and copy to package for wheel distribution.

Usage:
    python build_native.py              # Build for current platform
    python build_native.py --target X   # Build for specific target

Targets:
    x86_64-linux-gnu
    aarch64-linux-gnu
    x86_64-macos
    aarch64-macos
    x86_64-windows-gnu
"""

import argparse
import platform
import shutil
import subprocess
import sys
from pathlib import Path

# Paths
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
LIB_DIR = SCRIPT_DIR / "src" / "latticedb" / "lib"

# Target to library name mapping
LIB_NAMES = {
    "linux": "liblattice.so",
    "darwin": "liblattice.dylib",
    "macos": "liblattice.dylib",
    "windows": "lattice.dll",
}

# Zig target strings
ZIG_TARGETS = {
    "linux-x86_64": "x86_64-linux-gnu",
    "linux-aarch64": "aarch64-linux-gnu",
    "darwin-x86_64": "x86_64-macos",
    "darwin-aarch64": "aarch64-macos",
    "macos-x86_64": "x86_64-macos",
    "macos-aarch64": "aarch64-macos",
    "windows-x86_64": "x86_64-windows-gnu",
}


def get_current_platform() -> str:
    """Get current platform identifier."""
    system = platform.system().lower()
    machine = platform.machine().lower()

    if machine in ("x86_64", "amd64"):
        machine = "x86_64"
    elif machine in ("arm64", "aarch64"):
        machine = "aarch64"

    return f"{system}-{machine}"


def get_lib_name(target: str) -> str:
    """Get library name for target."""
    os_part = target.split("-")[0]
    return LIB_NAMES.get(os_part, "liblattice.so")


def build_library(target: str) -> Path:
    """Build library for target and return path to built library."""
    zig_target = ZIG_TARGETS.get(target)
    if zig_target is None:
        # Assume target is already a Zig target string
        zig_target = target

    print(f"Building shared library for {zig_target}...")

    cmd = [
        "zig",
        "build",
        "shared",
        f"-Dtarget={zig_target}",
        "-Doptimize=ReleaseFast",
    ]

    result = subprocess.run(cmd, cwd=PROJECT_ROOT, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Build failed:\n{result.stderr}", file=sys.stderr)
        sys.exit(1)

    # Find built library
    lib_name = get_lib_name(target)
    lib_path = PROJECT_ROOT / "zig-out" / "lib" / lib_name

    if not lib_path.exists():
        print(f"Built library not found: {lib_path}", file=sys.stderr)
        sys.exit(1)

    return lib_path


def copy_library(lib_path: Path, target: str) -> None:
    """Copy library to package directory."""
    LIB_DIR.mkdir(parents=True, exist_ok=True)

    lib_name = get_lib_name(target)
    dest = LIB_DIR / lib_name

    shutil.copy2(lib_path, dest)
    print(f"Copied {lib_path} -> {dest}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Build native library for Python package")
    parser.add_argument(
        "--target",
        type=str,
        help="Target platform (e.g., x86_64-linux-gnu, aarch64-macos)",
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        help="Clean lib directory before building",
    )
    args = parser.parse_args()

    if args.clean and LIB_DIR.exists():
        print(f"Cleaning {LIB_DIR}...")
        shutil.rmtree(LIB_DIR)

    target = args.target or get_current_platform()
    print(f"Target platform: {target}")

    lib_path = build_library(target)
    copy_library(lib_path, target)

    print("\nBuild complete!")
    print(f"Library location: {LIB_DIR}")
    for f in LIB_DIR.iterdir():
        if f.name != ".gitkeep":
            print(f"  {f.name} ({f.stat().st_size / 1024:.1f} KB)")


if __name__ == "__main__":
    main()
