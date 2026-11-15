# Releasing

Use `scripts/prepare_release.sh` as the supported release entry point. It wraps
the repository-wide version bumper, optional lockfile refresh, consistency
checks, and tests. (`dist/bump-version.sh` is an older script that also bumps,
commits, tags and pushes; do not use it.)

## Patch Release Flow

```bash
scripts/prepare_release.sh 0.17.0
```

Then, before committing:

1. Promote `docs/release_notes_next.md` to `docs/release_notes_0.17.0.md`, covering
   every change since the last tag (`git log v0.16.0..origin/main`), and reset
   `release_notes_next.md` to the empty template.
2. Rewrite the "What's new" caption and cards in `website/index.html` by hand. The
   bumper only moves the heading and the release-notes link.

Then commit everything the bumper changed and push `main`:

```bash
git diff
git status --short --branch
git add build.zig build.zig.zon src/main.zig src/api/c_api.zig include/lattice.h \
  bindings/ examples/ conformance/ book/src/api/c.md website/index.html docs/
git commit -m "Release v0.17.0"
git push origin main
```

Wait for the CI run on that commit to pass on every platform, Windows included,
before tagging. Some failures only show up there: a runner image GitHub has
retired, a test that reads the database file while it is open (Windows enforces
the lock), a text fixture checked out with CRLF line endings. The Linux Zig job
takes about 45 minutes and the binding jobs wait for it.

```bash
gh run list --branch main --limit 5
gh run watch <run id>
```

Then tag the release commit and push the tag:

```bash
git tag -a v0.17.0 -m "LatticeDB 0.17.0"
git push origin v0.17.0
```

Pushing the tag runs `.github/workflows/release.yml`, which runs the full CI
again, builds the release and **publishes to PyPI and npm**. Those versions
cannot be reused, so be sure before you push it. If the release run fails before
it publishes, fix `main`, wait for CI, and move the tag to the new commit.

`prepare_release.sh` also accepts `--tag` and `--push-tag`, but they run before
anything is committed, so they tag the commit *before* the version bump. Do not
use them; tag by hand after committing as above.

## Version Sources

`scripts/bump_version.py` is the canonical version updater/checker. It updates
and validates:

- native source and C header versions: `build.zig`, `src/main.zig`,
  `src/api/c_api.zig`, `include/lattice.h`
- the Zig package manifest version: `build.zig.zon`
- Python metadata: `bindings/python/pyproject.toml`,
  `bindings/python/src/latticedb/__init__.py`, `bindings/python/uv.lock`
- TypeScript metadata and fallback version: `bindings/typescript/package.json`,
  `bindings/typescript/package-lock.json`, `bindings/typescript/src/index.ts`
- example and conformance dependency pins for Go and TypeScript
- the C API version example in `book/src/api/c.md`
- the Java binding version: `bindings/java/pom.xml`
- the version shown on the website: `website/index.html`

Before pushing a release tag, run:

```bash
python3 scripts/bump_version.py --check 0.17.0 --strict-lockfile
```

The GitHub release workflow runs the same strict check before building or
publishing artifacts.

## Validation

For a normal release, keep the default `scripts/prepare_release.sh` test run
enabled. It runs `zig build test` and the Python and TypeScript binding tests; the
TypeScript tests need the shared library, which the script does not build, so run
`zig build shared` first. For a fuller manual validation, use:

```bash
zig build test
zig build integration-test
zig build crash-test
zig build shared
cd bindings/python && uv run --extra dev pytest tests -q
cd bindings/typescript && npm test -- --runInBand
```

Storage changes that affect durability, page layout, large values, recovery, or
bindings should also run the relevant crash, container, or regression repro
tests before tagging.
