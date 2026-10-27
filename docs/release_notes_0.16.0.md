# LatticeDB 0.16.0 Release Notes

## Summary

LatticeDB runs on Windows, loads inside packaged Electron apps, and keeps vectors
that were committed before a crash. The interactive shell shows what you type
again: at a terminal it had printed nothing at all until you quit, in every
release since 0.8.7.

There are no breaking API changes and the file format is unchanged.

## Highlights

**Windows.** The library, the CLI, and both bindings build and ship for
`x86_64-windows` and `aarch64-windows`. Releases include zip archives, `win_amd64`
and `win_arm64` wheels, and the npm package bundles `lattice.dll` for `win32-x64`
and `win32-arm64`. `backup`, `serialize`, replication, and restore used to open a
second handle to the live database file, which Windows' mandatory file locks
refuse; they now read through the handle the database already holds, which is one
fewer open file on every platform. The history file lives under
`%USERPROFILE%`.

**Packaged Electron apps.** Electron reads through `.asar` archives, but the
native loader cannot, so a packaged app found the library inside `app.asar` and
then failed to load it with an opaque error. The TypeScript binding now redirects
to the `.asar.unpacked` copy, and when there is none, the error names the archived
path and the `asarUnpack` entry to add. `resolveLibraryPath()`,
`isElectronRuntime()`, and `ELECTRON_ASAR_UNPACK_GLOB` are exported for apps that
want to check their own packaging.

**Vectors survive a crash.** A vector written in a committed transaction could
vanish if the process exited before the database was closed, even though it had
been searchable after `commit()` returned. A database created with vectors and
full-text search could also refuse to reopen at all after such an exit, failing
with `LatticeIOError`. Vector inserts, replacements, and removals are now in the
write-ahead log and are replayed on open, so committed vectors come back and
uncommitted ones stay gone. See *Known limits* for what recovery needs from the
process that reopens the file.

**The REPL shows what it is doing.** `lattice query` printed no banner, no
prompt, no echo of the keys you typed, and no results; everything appeared at once
when you pressed Ctrl-D. Output was buffered and never flushed while the shell
waited for input. It now flushes before every wait.

## Performance

**Vector work is about three times faster.** Every vector and HNSW
neighbour-list read checked its page against the file's size, and that check
asked the operating system each time. The page count is now kept in memory.
Measured on ten thousand 768-dimension vectors through the C API:

| | before | after |
|---|---:|---:|
| insert 10,000 vectors | 38.3 s | 11.9 s |
| 1,000 searches, k=10 | 0.476 s | 0.183 s |
| search p50 / p95 | 0.460 / 0.631 ms | 0.174 / 0.258 ms |

That is the speed of skipping the check entirely, and of 0.3.0, before the check
existed. The check itself stays: corrupt page references are still caught. The
slowdown dates from 0.10.0.

**Full-text `AND` starts from the rarer term.** `d.title @@ "the" AND d.body @@
"sourdough"` can be answered from either index. Starting from the common one
means materialising most of the corpus to discard it. The term dictionary already
counts documents per term, so the planner now knows the cheaper side before
reading either. On eight thousand documents returning one row:

| query | before | after |
|---|---:|---:|
| rare predicate written first | 4.3 ms | 4.3 ms |
| common predicate written first | 244 ms | 4.2 ms |

The two rows agreeing matters more than the 59x: what a query costs no longer
depends on which predicate its author typed first. This holds for `$param`
queries too, because the choice is made when the query runs.

## Fixes

- **Backups without a log contain every commit.** With `enable_wal = false`,
  `serialize()` and `backup()` copied the file while committed pages were still
  in memory, so the copy could be missing recent writes, enough that it could not
  plan a query. They now write those pages back first. Affected since 0.14.0.
- **`BackupStats.pages_flushed`** is accurate whether or not the database has a
  log.

## New

- **A sample dataset.** `examples/sample-graph` generates a research citation
  graph of 1,072 nodes and 5,882 edges across nine relationship types, with
  example queries. It is large enough for query plans to differ, which the
  four-node quickstart graph is not. The same `--seed` always produces the same
  file.

## Known limits

- **Reopen a crashed vector database with vectors enabled.** If a process exits
  without closing a vector database and its log holds committed vector writes,
  the next open must enable vectors with the same dimensions the writer used.
  Any other open fails with an I/O error rather than silently dropping those
  vectors. That includes `lattice query`, `exec`, `checkpoint`, and `compact`,
  which have no vector options, and binding code that opens without vector
  settings. Open the file once from your application with its usual vector
  configuration; recovery completes, and every opener works again from then on.
- **The Windows REPL's line editor is compile-checked, not exercised.** The Zig
  test suites and both binding suites run in CI on Windows. The interactive
  console path has no automated test, because CI has no console to drive.
- **ARM64 Windows is built but not run.** CI compiles it on every pull request
  but has no ARM64 Windows machine to test on.
- The full-text planner's estimate is per term. Predicates whose terms are
  individually common and rarely occur together still estimate as common.

## Upgrade notes

- No API changes, and no change to the database file format.
- The log gains two record types, for vector writes. A 0.15.0 reader opening a
  0.16.0 log left behind by a crash skips them, losing those vectors as 0.15.0
  always did. If you need to go back to 0.15.0, open and close the database once
  with 0.16.0 first.

## Upgrading

```bash
pip install --upgrade latticedb
npm install @hajewski/latticedb@0.16.0
go get github.com/jeffhajewski/latticedb/bindings/go@v0.16.0
```

Java artifacts are published at `io.latticedb:latticedb:0.16.0`.
