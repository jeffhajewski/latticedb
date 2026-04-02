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
from typing import Optional

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


def resolve_library_source(
    target: str,
    bundle_lib_path: Optional[str] = None,
    bundle_lib_dir: Optional[str] = None,
) -> Path:
    """Resolve the native library to bundle for the given target."""
    lib_name = get_lib_name(target)

    if bundle_lib_path:
        explicit_path = Path(bundle_lib_path)
        if explicit_path.is_dir():
            explicit_path = explicit_path / lib_name
        if not explicit_path.exists():
            raise FileNotFoundError(f"Bundled library not found: {explicit_path}")
        return explicit_path

    if bundle_lib_dir:
        explicit_dir = Path(bundle_lib_dir)
        explicit_path = explicit_dir / lib_name
        if not explicit_path.exists():
            raise FileNotFoundError(f"Bundled library not found: {explicit_path}")
        return explicit_path

    return build_library(target)


def copy_library(lib_path: Path, target: str, output_dir: Path = LIB_DIR) -> Path:
    """Copy library to package directory."""
    output_dir.mkdir(parents=True, exist_ok=True)

    lib_name = get_lib_name(target)
    dest = output_dir / lib_name

    shutil.copy2(lib_path, dest)
    print(f"Copied {lib_path} -> {dest}")
    return dest


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
    parser.add_argument(
        "--output-dir",
        type=str,
        help="Directory to copy the bundled library into",
    )
    parser.add_argument(
        "--bundle-lib-path",
        type=str,
        help="Path to a prebuilt native library to copy instead of building with Zig",
    )
    parser.add_argument(
        "--bundle-lib-dir",
        type=str,
        help="Directory containing a prebuilt native library to copy instead of building with Zig",
    )
    args = parser.parse_args()

    output_dir = Path(args.output_dir) if args.output_dir else LIB_DIR

    if args.clean and output_dir.exists():
        print(f"Cleaning {output_dir}...")
        shutil.rmtree(output_dir)

    target = args.target or get_current_platform()
    print(f"Target platform: {target}")

    lib_path = resolve_library_source(
        target,
        bundle_lib_path=args.bundle_lib_path,
        bundle_lib_dir=args.bundle_lib_dir,
    )
    copy_library(lib_path, target, output_dir=output_dir)

    print("\nBuild complete!")
    print(f"Library location: {output_dir}")
    for f in output_dir.iterdir():
        if f.name != ".gitkeep":
            print(f"  {f.name} ({f.stat().st_size / 1024:.1f} KB)")


if __name__ == "__main__":
    main()
