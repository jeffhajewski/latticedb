# Releasing

Use `scripts/prepare_release.sh` as the supported release entry point. It wraps
the repository-wide version bumper, optional lockfile refresh, consistency
checks, tests, and optional tag creation.

## Patch Release Flow

```bash
scripts/prepare_release.sh 0.9.6
git diff
git status --short --branch
git add build.zig src/main.zig src/api/c_api.zig include/lattice.h bindings/ examples/ conformance/ book/src/api/c.md
git commit -m "Release v0.9.6"
git tag v0.9.6
git push origin main
git push origin v0.9.6
```

Use `--tag` or `--push-tag` when you want the script to create or push the tag
for you:

```bash
scripts/prepare_release.sh 0.9.6 --tag
scripts/prepare_release.sh 0.9.6 --tag --push-tag
```

## Version Sources

`scripts/bump_version.py` is the canonical version updater/checker. It updates
and validates:

- native source and C header versions: `build.zig`, `src/main.zig`,
  `src/api/c_api.zig`, `include/lattice.h`
- Python metadata: `bindings/python/pyproject.toml`,
  `bindings/python/src/latticedb/__init__.py`, `bindings/python/uv.lock`
- TypeScript metadata and fallback version: `bindings/typescript/package.json`,
  `bindings/typescript/package-lock.json`, `bindings/typescript/src/index.ts`
- example and conformance dependency pins for Go and TypeScript
- the C API version example in `book/src/api/c.md`

Before pushing a release tag, run:

```bash
python3 scripts/bump_version.py --check 0.9.6 --strict-lockfile
```

The GitHub release workflow runs the same strict check before building or
publishing artifacts.

## Validation

For a normal release, keep the default `scripts/prepare_release.sh` test run
enabled. For manual validation, use:

```bash
zig build test
zig build integration-test
zig build shared
cd bindings/python && uv run --extra dev pytest tests -q
cd bindings/typescript && npm test -- --runInBand
```

Storage changes that affect durability, page layout, large values, recovery, or
bindings should also run the relevant crash, container, or regression repro
tests before tagging.
