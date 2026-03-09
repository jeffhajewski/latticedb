#!/usr/bin/env python3
"""Bump and verify LatticeDB versions across native and client bindings.

Usage:
  python scripts/bump_version.py 0.3.0
  python scripts/bump_version.py 0.3.0 --dry-run
  python scripts/bump_version.py --check
  python scripts/bump_version.py --check 0.3.0
  python scripts/bump_version.py --check 0.3.0 --strict-lockfile
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, List, Optional, Sequence, Tuple


SEMVER_RE = re.compile(r"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$")


@dataclass(frozen=True)
class FileChange:
    path: Path
    old: str
    new: str


@dataclass(frozen=True)
class VersionObservation:
    label: str
    path: Path
    value: str


def _parse_semver(version: str) -> Tuple[str, str, str]:
    match = SEMVER_RE.match(version)
    if not match:
        raise ValueError(
            f"Invalid version '{version}'. Expected strict semver form: MAJOR.MINOR.PATCH"
        )
    return match.groups()


def _extract_exactly_one(
    text: str,
    pattern: str,
    path: Path,
    description: str,
    *,
    group: int = 1,
) -> str:
    matches = list(re.finditer(pattern, text, flags=re.MULTILINE))
    if len(matches) != 1:
        raise ValueError(
            f"{path}: expected exactly one {description} match for pattern {pattern!r}, got {len(matches)}"
        )
    return matches[0].group(group)


def _replace_exactly_one(text: str, pattern: str, repl: str, path: Path) -> str:
    updated, count = re.subn(pattern, repl, text, flags=re.MULTILINE)
    if count != 1:
        raise ValueError(f"{path}: expected exactly one match for pattern: {pattern!r}, got {count}")
    return updated


def _update_lattice_h(text: str, version: str, major: str, minor: str, patch: str, path: Path) -> str:
    text = _replace_exactly_one(
        text,
        r'^#define LATTICE_VERSION ".*"$',
        f'#define LATTICE_VERSION "{version}"',
        path,
    )
    text = _replace_exactly_one(
        text,
        r"^#define LATTICE_VERSION_MAJOR \d+$",
        f"#define LATTICE_VERSION_MAJOR {major}",
        path,
    )
    text = _replace_exactly_one(
        text,
        r"^#define LATTICE_VERSION_MINOR \d+$",
        f"#define LATTICE_VERSION_MINOR {minor}",
        path,
    )
    text = _replace_exactly_one(
        text,
        r"^#define LATTICE_VERSION_PATCH \d+$",
        f"#define LATTICE_VERSION_PATCH {patch}",
        path,
    )
    return text


def _update_main_zig(text: str, version: str, major: str, minor: str, patch: str, path: Path) -> str:
    text = _replace_exactly_one(
        text,
        r'^pub const VERSION = ".*";$',
        f'pub const VERSION = "{version}";',
        path,
    )
    text = _replace_exactly_one(
        text,
        r"^pub const VERSION_MAJOR = \d+;$",
        f"pub const VERSION_MAJOR = {major};",
        path,
    )
    text = _replace_exactly_one(
        text,
        r"^pub const VERSION_MINOR = \d+;$",
        f"pub const VERSION_MINOR = {minor};",
        path,
    )
    text = _replace_exactly_one(
        text,
        r"^pub const VERSION_PATCH = \d+;$",
        f"pub const VERSION_PATCH = {patch};",
        path,
    )
    return text


def _update_c_api_zig(text: str, version: str, path: Path) -> str:
    text = _replace_exactly_one(
        text,
        r'(pub export fn lattice_version\(\) \[\*c\]const u8 \{\n\s*return )"[^"]+";',
        rf'\1"{version}";',
        path,
    )
    text = _replace_exactly_one(
        text,
        r'(try std\.testing\.expectEqualStrings\()"[^"]+"(, std\.mem\.sliceTo\(version, 0\)\);)',
        rf'\1"{version}"\2',
        path,
    )
    return text


def _update_pyproject_toml(text: str, version: str, path: Path) -> str:
    return _replace_exactly_one(
        text,
        r'^version = ".*"$',
        f'version = "{version}"',
        path,
    )


def _update_python_init(text: str, version: str, path: Path) -> str:
    return _replace_exactly_one(
        text,
        r'^__version__ = ".*"$',
        f'__version__ = "{version}"',
        path,
    )


def _update_ts_index(text: str, version: str, path: Path) -> str:
    return _replace_exactly_one(
        text,
        r"(catch \{\n\s*return )'[^']+';",
        rf"\1'{version}';",
        path,
    )


def _extract_package_json_metadata(path: Path) -> Tuple[str, str, Sequence[str]]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if "name" not in data or not isinstance(data["name"], str):
        raise ValueError(f"{path}: missing top-level string 'name'")
    if "version" not in data or not isinstance(data["version"], str):
        raise ValueError(f"{path}: missing top-level string 'version'")

    dependencies = data.get("dependencies", {})
    dep_names: Sequence[str]
    if isinstance(dependencies, dict):
        dep_names = tuple(dependencies.keys())
    else:
        dep_names = tuple()
    return data["name"], data["version"], dep_names


def _update_package_json(text: str, version: str, path: Path) -> str:
    data = json.loads(text)
    if "version" not in data:
        raise ValueError(f"{path}: missing top-level 'version' key")
    data["version"] = version
    return json.dumps(data, indent=2) + "\n"


def _update_package_lock(text: str, version: str, package_name: str, path: Path) -> str:
    data = json.loads(text)
    if "version" not in data:
        raise ValueError(f"{path}: missing top-level 'version' key")
    data["version"] = version

    if "name" in data and isinstance(data["name"], str):
        data["name"] = package_name

    packages = data.get("packages")
    if isinstance(packages, dict):
        root_pkg = packages.get("")
        if isinstance(root_pkg, dict):
            if "version" in root_pkg and isinstance(root_pkg["version"], str):
                root_pkg["version"] = version
            if "name" in root_pkg and isinstance(root_pkg["name"], str):
                root_pkg["name"] = package_name

    return json.dumps(data, indent=2) + "\n"


def _load(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _validate_component_triplet(
    version: str,
    major: str,
    minor: str,
    patch: str,
    *,
    label: str,
    path: Path,
    problems: List[str],
) -> None:
    match = SEMVER_RE.match(version)
    if not match:
        problems.append(f"{path}: {label} has non-semver value '{version}'")
        return

    v_major, v_minor, v_patch = match.groups()
    if (v_major, v_minor, v_patch) != (major, minor, patch):
        problems.append(
            f"{path}: {label} triplet mismatch "
            f"(version={version}, major={major}, minor={minor}, patch={patch})"
        )


def _collect_version_observations(
    root: Path,
    *,
    strict_lockfile: bool,
) -> Tuple[List[VersionObservation], List[str], List[str]]:
    observations: List[VersionObservation] = []
    problems: List[str] = []
    warnings: List[str] = []

    # include/lattice.h
    lattice_h_path = root / "include/lattice.h"
    lattice_h = _load(lattice_h_path)
    lattice_h_version = _extract_exactly_one(
        lattice_h, r'^#define LATTICE_VERSION "([^"]+)"$', lattice_h_path, "LATTICE_VERSION"
    )
    lattice_h_major = _extract_exactly_one(
        lattice_h, r"^#define LATTICE_VERSION_MAJOR (\d+)$", lattice_h_path, "LATTICE_VERSION_MAJOR"
    )
    lattice_h_minor = _extract_exactly_one(
        lattice_h, r"^#define LATTICE_VERSION_MINOR (\d+)$", lattice_h_path, "LATTICE_VERSION_MINOR"
    )
    lattice_h_patch = _extract_exactly_one(
        lattice_h, r"^#define LATTICE_VERSION_PATCH (\d+)$", lattice_h_path, "LATTICE_VERSION_PATCH"
    )
    _validate_component_triplet(
        lattice_h_version,
        lattice_h_major,
        lattice_h_minor,
        lattice_h_patch,
        label="LATTICE_VERSION macros",
        path=lattice_h_path,
        problems=problems,
    )
    observations.append(
        VersionObservation("C header version", lattice_h_path, lattice_h_version)
    )

    # src/main.zig
    main_zig_path = root / "src/main.zig"
    main_zig = _load(main_zig_path)
    main_version = _extract_exactly_one(
        main_zig, r'^pub const VERSION = "([^"]+)";$', main_zig_path, "VERSION constant"
    )
    main_major = _extract_exactly_one(
        main_zig, r"^pub const VERSION_MAJOR = (\d+);$", main_zig_path, "VERSION_MAJOR constant"
    )
    main_minor = _extract_exactly_one(
        main_zig, r"^pub const VERSION_MINOR = (\d+);$", main_zig_path, "VERSION_MINOR constant"
    )
    main_patch = _extract_exactly_one(
        main_zig, r"^pub const VERSION_PATCH = (\d+);$", main_zig_path, "VERSION_PATCH constant"
    )
    _validate_component_triplet(
        main_version,
        main_major,
        main_minor,
        main_patch,
        label="main.zig VERSION constants",
        path=main_zig_path,
        problems=problems,
    )
    observations.append(
        VersionObservation("Core/CLI version", main_zig_path, main_version)
    )

    # src/api/c_api.zig
    c_api_path = root / "src/api/c_api.zig"
    c_api = _load(c_api_path)
    c_api_version = _extract_exactly_one(
        c_api,
        r'pub export fn lattice_version\(\) \[\*c\]const u8 \{\n\s*return "([^"]+)";',
        c_api_path,
        "lattice_version return value",
    )
    c_api_test_version = _extract_exactly_one(
        c_api,
        r'try std\.testing\.expectEqualStrings\("([^"]+)", std\.mem\.sliceTo\(version, 0\)\);',
        c_api_path,
        "version test expectation",
    )
    observations.append(
        VersionObservation("C API exported version", c_api_path, c_api_version)
    )
    observations.append(
        VersionObservation("C API test expectation", c_api_path, c_api_test_version)
    )

    # Python metadata
    pyproject_path = root / "bindings/python/pyproject.toml"
    pyproject = _load(pyproject_path)
    pyproject_version = _extract_exactly_one(
        pyproject, r'^version = "([^"]+)"$', pyproject_path, "pyproject version"
    )
    observations.append(
        VersionObservation("Python package version", pyproject_path, pyproject_version)
    )

    python_init_path = root / "bindings/python/src/latticedb/__init__.py"
    python_init = _load(python_init_path)
    python_init_version = _extract_exactly_one(
        python_init, r'^__version__ = "([^"]+)"$', python_init_path, "__version__"
    )
    observations.append(
        VersionObservation("Python __version__", python_init_path, python_init_version)
    )

    # TypeScript metadata
    package_json_path = root / "bindings/typescript/package.json"
    package_name, package_version, package_deps = _extract_package_json_metadata(package_json_path)
    observations.append(
        VersionObservation("TypeScript package version", package_json_path, package_version)
    )

    package_lock_path = root / "bindings/typescript/package-lock.json"
    package_lock = json.loads(_load(package_lock_path))

    lock_top_version = package_lock.get("version")
    if not isinstance(lock_top_version, str):
        problems.append(f"{package_lock_path}: missing top-level string 'version'")
    else:
        observations.append(
            VersionObservation("TypeScript lockfile version", package_lock_path, lock_top_version)
        )

    lock_top_name = package_lock.get("name")
    if isinstance(lock_top_name, str) and lock_top_name != package_name:
        problems.append(
            f"{package_lock_path}: top-level name '{lock_top_name}' does not match package.json name '{package_name}'"
        )

    packages = package_lock.get("packages")
    root_pkg = packages.get("") if isinstance(packages, dict) else None
    if not isinstance(root_pkg, dict):
        problems.append(f"{package_lock_path}: missing packages[''] object")
    else:
        lock_root_version = root_pkg.get("version")
        if not isinstance(lock_root_version, str):
            problems.append(f"{package_lock_path}: packages[''] missing string 'version'")
        else:
            observations.append(
                VersionObservation(
                    "TypeScript lockfile root version",
                    package_lock_path,
                    lock_root_version,
                )
            )

        lock_root_name = root_pkg.get("name")
        if isinstance(lock_root_name, str) and lock_root_name != package_name:
            problems.append(
                f"{package_lock_path}: packages[''].name '{lock_root_name}' does not match package.json name '{package_name}'"
            )

        root_deps_obj = root_pkg.get("dependencies", {})
        root_deps: Sequence[str]
        if isinstance(root_deps_obj, dict):
            root_deps = tuple(root_deps_obj.keys())
        else:
            root_deps = tuple()

        missing_deps = sorted(set(package_deps) - set(root_deps))
        extra_deps = sorted(set(root_deps) - set(package_deps))
        if missing_deps or extra_deps:
            mismatch_msg = (
                f"{package_lock_path}: packages[''].dependencies differs from package.json "
                f"(missing={missing_deps or '[]'}, extra={extra_deps or '[]'})"
            )
            if strict_lockfile:
                problems.append(mismatch_msg)
            else:
                warnings.append(mismatch_msg)

    ts_index_path = root / "bindings/typescript/src/index.ts"
    ts_index = _load(ts_index_path)
    ts_fallback_version = _extract_exactly_one(
        ts_index,
        r"catch \{\n\s*return '([^']+)';",
        ts_index_path,
        "TypeScript fallback version() return",
    )
    observations.append(
        VersionObservation("TypeScript fallback version", ts_index_path, ts_fallback_version)
    )

    for observation in observations:
        if not SEMVER_RE.match(observation.value):
            problems.append(
                f"{observation.path}: {observation.label} is not semver: '{observation.value}'"
            )

    return observations, problems, warnings


def _print_observations(root: Path, observations: Sequence[VersionObservation]) -> None:
    print("Observed version values:")
    label_width = max(len(o.label) for o in observations)
    for observation in observations:
        rel = observation.path.relative_to(root)
        print(f"  {observation.label:<{label_width}}  {observation.value:<8}  ({rel})")


def _check_consistency(root: Path, expected_version: Optional[str], *, strict_lockfile: bool) -> int:
    if expected_version is not None:
        _parse_semver(expected_version)

    try:
        observations, problems, warnings = _collect_version_observations(
            root,
            strict_lockfile=strict_lockfile,
        )
    except Exception as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    _print_observations(root, observations)

    canonical = expected_version if expected_version is not None else observations[0].value
    mismatches = [o for o in observations if o.value != canonical]
    if mismatches:
        mismatched_labels = ", ".join(f"{o.label}={o.value}" for o in mismatches)
        if expected_version is not None:
            problems.append(
                f"Version mismatch: expected all sources to be {expected_version}; got {mismatched_labels}"
            )
        else:
            problems.append(
                f"Version mismatch across sources (baseline {canonical}): {mismatched_labels}"
            )

    if warnings:
        print("")
        print("Warnings:")
        for warning in warnings:
            print(f"  - {warning}")

    if problems:
        print("")
        print("Consistency check failed:")
        for problem in problems:
            print(f"  - {problem}")
        return 1

    print("")
    if expected_version is None:
        print(f"Consistency check passed: all tracked version sources are {canonical}.")
    else:
        print(f"Consistency check passed: all tracked version sources are {expected_version}.")
    return 0


def _compute_changes(root: Path, version: str) -> Tuple[FileChange, ...]:
    major, minor, patch = _parse_semver(version)
    package_json_path = root / "bindings/typescript/package.json"
    package_name, _, _ = _extract_package_json_metadata(package_json_path)

    transforms: Dict[Path, Callable[[str], str]] = {
        root / "include/lattice.h": lambda t: _update_lattice_h(
            t, version, major, minor, patch, root / "include/lattice.h"
        ),
        root / "src/main.zig": lambda t: _update_main_zig(
            t, version, major, minor, patch, root / "src/main.zig"
        ),
        root / "src/api/c_api.zig": lambda t: _update_c_api_zig(
            t, version, root / "src/api/c_api.zig"
        ),
        root / "bindings/python/pyproject.toml": lambda t: _update_pyproject_toml(
            t, version, root / "bindings/python/pyproject.toml"
        ),
        root / "bindings/python/src/latticedb/__init__.py": lambda t: _update_python_init(
            t, version, root / "bindings/python/src/latticedb/__init__.py"
        ),
        root / "bindings/typescript/package.json": lambda t: _update_package_json(
            t, version, root / "bindings/typescript/package.json"
        ),
        root / "bindings/typescript/package-lock.json": lambda t: _update_package_lock(
            t,
            version,
            package_name,
            root / "bindings/typescript/package-lock.json",
        ),
        root / "bindings/typescript/src/index.ts": lambda t: _update_ts_index(
            t, version, root / "bindings/typescript/src/index.ts"
        ),
    }

    changes = []
    for path, transform in transforms.items():
        if not path.exists():
            raise FileNotFoundError(f"Expected file does not exist: {path}")
        old = _load(path)
        new = transform(old)
        changes.append(FileChange(path=path, old=old, new=new))
    return tuple(changes)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Bump or validate LatticeDB version consistency across repo files."
    )
    parser.add_argument(
        "version",
        nargs="?",
        help="Version in MAJOR.MINOR.PATCH format (example: 0.3.0)",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Validate current file versions for consistency; optionally enforce an expected VERSION.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show which files would change, without writing anything.",
    )
    parser.add_argument(
        "--rehearsal",
        action="store_true",
        help="Alias for --dry-run; safe rehearsal mode that makes no edits.",
    )
    parser.add_argument(
        "--strict-lockfile",
        action="store_true",
        help="Treat package-lock dependency drift as an error (default is warning-only).",
    )
    args = parser.parse_args()

    root = Path(__file__).resolve().parents[1]

    if args.check:
        if args.dry_run or args.rehearsal:
            parser.error("--dry-run/--rehearsal cannot be used with --check")
        return _check_consistency(root, args.version, strict_lockfile=args.strict_lockfile)

    if not args.version:
        parser.error("version is required unless --check is provided")

    try:
        changes = _compute_changes(root, args.version)
    except Exception as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    pending = [c for c in changes if c.old != c.new]
    if not pending:
        print(f"No changes needed; files already at version {args.version}.")
        return _check_consistency(root, args.version, strict_lockfile=args.strict_lockfile)

    rehearsal = args.dry_run or args.rehearsal

    for change in pending:
        rel = change.path.relative_to(root)
        verb = "would update" if rehearsal else "update"
        print(f"{verb}: {rel}")

    if rehearsal:
        print("rehearsal: no files written.")
        return 0

    for change in pending:
        change.path.write_text(change.new, encoding="utf-8")

    print(f"Updated {len(pending)} files to version {args.version}.")

    verify_rc = _check_consistency(root, args.version, strict_lockfile=args.strict_lockfile)
    if verify_rc != 0:
        print("error: post-write consistency check failed.", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
