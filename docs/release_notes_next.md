# LatticeDB Next Release Notes

Draft for the next release after `0.16.0`.

<!-- Query-engine fixes still on another branch (DELETE without DETACH, MERGE,
variable-length paths, relationship uniqueness, column naming, =~, aggregates)
are not described here yet. -->

## Summary

A database that was killed, or lost power, could reopen missing committed data,
or not reopen at all. Most of this release closes those holes: pages reach the
database file only in whole flushes through a new journal, checkpoints save
everything they depend on, a commit is durable before it becomes visible, and
the log survives a torn write at its end.

Several behaviours change, listed under *Breaking changes*: null follows
Cypher's three-valued logic, `%` takes the sign of the dividend, integer overflow
is an error, keys are capped at 1012 bytes, C API handles are opaque tokens, the
JSON export format is version 2, full-text search splits words by Unicode, and
`set_vector` refuses keys other than `"embedding"`. The page layout of the
database file is unchanged.

## Durability

**Checkpoints save what they depend on.** A truncating checkpoint flushed the
page cache and emptied the log, but left the B+Tree root page numbers and the
vector index's node map in memory; only `sync()` and `close()` saved them. A
process killed after an automatic checkpoint reopened onto old roots and an empty
log. With the default threshold, 3,000 committed single-node transactions
followed by a kill left 96 nodes. Every checkpoint now saves both before the log
is emptied.

**Pages reach the file only in whole flushes.** Between checkpoints the page
cache wrote a changed page to the database file whenever it needed the frame,
and freeing a page wrote to it on the spot. Neither write is in the log, so after
a checkpoint had emptied it, a kill could leave a tree half rewritten with
nothing to rebuild it from. Any database larger than its page cache was exposed
with default settings: 4,000 small transactions against a 48-page cache lost
nodes at the first kill. Changed pages now go to the file only in a flush of
every changed page together, at a checkpoint, at close, on `sync()`, or early
when half the cache holds changes. The cache grows rather than write a changed
page on its own, and gives the extra frames back at the next flush.

**A flush journal makes each flush all or nothing.** Before a flush writes pages
in place, it copies them to `<database>-journal` and syncs that. If the process
or machine dies part way through, the next open finishes the flush from the
journal, so the file always holds one whole image. The journal exists while the
database is open read-write and is deleted at a clean close. Leave it in place
after a crash: it is what the next open needs. A read-only open refuses a
database with a complete journal waiting; open it read-write once.

**A commit is durable before anyone sees it.** Commit applied a transaction's
changes and then wrote and synced the commit record, so a failure in between
left changes visible, and checkpointed, that the log said never happened. The
order is now: write the records, write and sync the commit record, then apply.
If applying fails, or the sync fails and the outcome is unknown, the handle marks
itself failed. It refuses further transactions, `sync()` and checkpoints, and
writes nothing more to the database file. Close it and reopen; recovery gives
exactly the state the log describes.

**A torn log opens.** A power cut could leave the log's header counting a frame
the file did not fully hold, a torn last frame that later frames were appended
behind, or a half-written header. Each of these made the database unopenable or
reported corruption. Opening now cuts a torn tail back to the last good frame,
and rebuilds a header whose checksum fails but whose identity fields match.
Damage with good frames after it is still reported as corruption.

**Open and close empty the log.** Recovery replayed the log and left it in
place, and closing through the Zig API or the CLI never checkpointed, so every
open replayed everything since the last truncation and rebuilt every index. Open
now checkpoints after recovery, and close checkpoints when the log holds frames.
Transaction ids continue above the highest id in the log, so records from two
sessions can no longer be read as one transaction.

**Syncs cover what they claim.** `sync()` synced the log but not the database
file, and with the log off nothing synced the file at all; it now does. A newly
created database's directory is synced, so a power cut right after creation
cannot leave the files missing from it (not on Windows).

**Full fsync on macOS.** On macOS `fsync` does not ask the drive to flush its own
cache, so a commit there survives a crash but not necessarily a power cut. The
new `full_fsync` option, or `LATTICE_FULL_FSYNC=1` from the C API, the bindings
and the CLI, uses `F_FULLFSYNC`. It is off by default: it makes each commit
several milliseconds slower.

**A sync mode for throughput.** `sync_mode = .normal`, or
`LATTICE_SYNC_MODE=normal`, returns from a commit without waiting for the disk. A
returned commit survives the process being killed; a power cut or OS crash can
lose the latest commits, from the end only, and the database stays consistent.
The default stays `.full`.

**Other recovery fixes.**

- Deleting a node logged a vector removal whether or not the node had a vector.
  On a database that had never stored a vector, one delete followed by a kill, or
  a close through the Zig API or the CLI, made every later open fail with an I/O
  error. Logs written by 0.16.0 that hold such records now open.
- A flush that ran before a checkpoint wrote vector pages without the index's
  node map, and replay re-linked every vector it saw, which could leave most of
  the graph unreachable by search (186 of 298 vectors in one test). Both are
  fixed.
- Opening an existing database for writing fails if the node tree cannot be
  scanned. It used to fall back to node id 1 and overwrite existing nodes.
- A B+Tree insert whose split fails part way, a delete whose overflow chain
  cannot be freed, and a damaged freelist head no longer leave pages with two
  owners or entries pointing at freed pages.
- `sync()` on a handle whose commit failed is refused. It could write a
  half-applied transaction into the file, and the C API's close calls it.

## Breaking changes

- **Null follows three-valued logic.** A comparison, arithmetic or string
  operation with a null operand is null, and `NOT`, `AND` and `OR` are Kleene:
  `null = null` is null, `NOT null` is null, `false AND null` is false, `true OR
  null` is true, `2 IN [1, null]` is null. A `WHERE` keeps a row only when its
  predicate is true, so `WHERE n.x <> 5` no longer keeps nodes with no `x`, and
  `MATCH (n {x: null})` matches nothing. `IS NULL` and `IS NOT NULL` are
  unchanged. Grouping, `DISTINCT`, `ORDER BY` and `MERGE` still treat nulls as
  equal.
- **`%` takes the sign of the dividend.** `-7 % 2` is `-1` and `7 % -2` is `1`
  (they were `1` and `-1`). `%` now works on floats (`7.5 % 2` is `1.5`), where it
  returned null, and a zero divisor is an error for floats as for integers.
- **`count(*)` is accepted**, and its column is named `count(*)`. A query may
  start with `RETURN` or `WITH`: `RETURN 1 + 2` returns one row, and `RETURN
  count(*)` returns 1.
- **Integer overflow is an error.** `+`, `-`, `*`, `/`, unary minus and `abs()`
  on integers fail the query when the result does not fit in 64 bits.
  `9223372036854775807 + 1` used to panic in debug builds and was undefined in
  release builds. Float arithmetic is unchanged.
- **`RETURN` and `WITH` take at most 16 items.** More fails to plan with
  `TooManyVariables`; it used to read past the end of the row.
- **Keys are capped at 1012 bytes** (for 4 KB pages). Labels, relationship types
  and property names are stored as keys, so a name of more than about a thousand
  bytes is refused with a storage error. A long name used to be able to overflow
  a B+Tree page in memory. Longer keys already in a file can still be read and
  deleted. Property values are not affected.
- **C API handles are opaque tokens**, not addresses, and are never reused, so a
  stale handle is rejected instead of reaching freed memory. A transaction handle
  used after commit or rollback returns `LATTICE_ERROR_TXN_ABORTED`, also after
  `lattice_close`, where it returned `LATTICE_ERROR_INVALID_ARG`; other stale
  handles return `LATTICE_ERROR_INVALID_ARG`. Enum arguments are checked: an
  unknown `lattice_begin` mode is `LATTICE_ERROR_INVALID_ARG` (any non-zero mode
  used to mean read-write). An empty batch-insert label means no label.
- **JSON export format version 2.** Exports start with `"version": 2`, write
  bytes as `{"$bytes": base64}`, vector properties as `{"$vector": [...]}`, floats
  always with a decimal point or exponent, and non-finite floats as
  `{"$float": "NaN"}`. Import reads version 2, and still reads 0.16.0 files (no
  version field), except that strings made of hex digits, such as `"cafe"`, stay
  strings; they used to come back as bytes. A file with a newer version is
  refused. 0.16.0 cannot read version 2 files. CSV export now writes properties,
  in `<key>:json` columns.
- **A `<database>-journal` file** sits beside a database open read-write (see
  *Durability*). Backup, sync or file-management tooling that copies a database
  by its file names should leave it alone; use `backup` to copy an open
  database.
- **Full-text search splits words by Unicode.** Words are runs of letters,
  digits and marks in any script, lowercased, with accents folded: "Café",
  "cafe" and "CAFÉ" are one term. Han characters and hiragana are a term each.
  Indexes that already hold documents keep 0.16.0's ASCII tokenizer until they
  are emptied; there is no reindex command, so drop and recreate an index to
  switch it. A crash-recovery replay also rebuilds it.
- **A node and a relationship full-text index on the same name and property are
  refused** with `ALREADY_EXISTS`; they would share storage. Databases that
  already have both keep working as before.
- **Vectors with NaN or infinite components are rejected** with
  `INVALID_ARG`, when stored and when used as a query.
- **`set_vector` refuses keys other than `"embedding"`**, `""` and `NULL`, with
  `UNSUPPORTED`. A node holds one vector, and another key silently replaced it.

## Performance

<!-- numbers to be re-measured on a quiet machine before release -->

The C API used Zig's debug allocator in every build, with one global lock per
allocation. Release builds now use `smp_allocator`. Label scans read the label
index instead of decoding every node, and changefeed records no longer make a
large commit quadratic. Through the C API, release build, 20,000 nodes:

| | before | after |
|---|---:|---:|
| read one property | 9.4 µs | 0.37 µs |
| `MATCH (n:Rare) … LIMIT 1` | 190 ms | 3.7 µs |
| `MATCH (n:Person) WHERE n.age > 60` | 478 ms | about 9 ms |
| read-only transaction, no work | 44 µs | 0.08 µs |
| commit 20,000 nodes in one transaction | 9.5 s | about 0.3–0.6 s |

A read-only transaction no longer writes to the log at all; it used to write two
records and an fsync.

Other changes, measured by their commits on a busy machine:

- Full-text search scores posting lists in batches and keeps only the best
  results: a common term over 100,000 documents went from 35 ms to 6.7 ms.
- Saving the vector index writes only the entries that changed: after one insert
  into 10,000 vectors, 58 ms became 0.05 ms. Storing a vector at 100,000 vectors
  went from 801 µs to 3.1 µs, and removing one from 1.96 ms to 0.15 ms.
- Page checksums use the CPU's CRC instruction where there is one (7.2 µs to
  0.39 µs per 4 KB page). They are bit-identical to before.
- Writes no longer invalidate the query cache, and B+Tree lookups and point
  reads avoid copies.
- A second thread waiting on a database lock sleeps instead of spinning on a
  core.

## Fixes

**Full-text search**

- AND search could miss matching documents after updates or multi-document
  transactions.
- A query term no document contains gives no results; it was ignored.
- All matches are ranked before the limit is applied; only the first 2 × limit
  were considered.
- Fuzzy search scores each word by its best spelling. Uncommitted text is
  matched by whole terms, so "cat" no longer matches "concatenate".
- Zig API: phrase alignment, stop words inside phrases, prefix words counted
  twice, exclusions applied after ranking, and `searchWithMode` looping forever
  on punctuation or accented queries are fixed. `use_stemming` now stems.
- Long posting entries no longer overrun their buffers.

**Vector search**

- `vector_search` with `k` above `ef_search` returns `k` results; Cypher's `<=>`
  returned 64 of its 100.
- HNSW inserts and removes are all or nothing. `m` and `m_max0` are capped at 64.
- Pending vectors are scored on the same scale as committed ones. Under cosine,
  committed vectors read back normalised (documented; unchanged).

**CLI**

- Every command that opens a database takes `--enable-vector` and
  `--vector-dims`, and an open that fails because the log holds vector changes
  says which options to retry with.
- Export, JSONL export and dump include node vectors, and import restores them.
- `export --query` exports what the query returns.
- CSV import follows RFC 4180, finds columns by header, resolves edges through
  `_id`, and keeps edge properties and value types.
- An import that fails names the record and the reason. Each record is imported
  whole or not at all, and `--on-error=skip` lists what it left out.
- JSONL files can be imported.
- `lattice ... >> file` appends; it overwrote the start of the file.
- A failed export removes its partial file.

**C API and bindings**

- Database-level C calls take the database lock, and close wakes a blocked stream
  reader instead of freeing memory under it.
- A failed `lattice_commit` leaves the transaction open for rollback. Python
  `query()` and Go `Update` and `Query` roll back after a failed commit; they held
  the writer slot.
- Edge types are read by length (TypeScript returned trailing bytes), and C also
  NUL-terminates them.
- The embedding client copies its configuration strings; TypeScript requests
  could read freed memory.
- Python: integers outside the 64-bit range raise `OverflowError` instead of
  wrapping, and unlabeled nodes no longer leak.
- TypeScript: `getProperty` returns `null` for a missing property, and a timed
  `readStream` waits on a worker thread instead of blocking the event loop.
- Go: buffers of 2 GiB or more are copied whole; they panicked or were
  truncated.
- Java: `close()` refused while work is open keeps the handle, `QueryException`
  carries the real error code, `BigDecimal` is stored as a float, and characters
  outside the BMP survive error text and column names.
- `LATTICE_PREFIX` on Windows looks for the DLL under `bin/`, then `lib/`.

## Known limits

- **Reopen a crashed vector database with vectors enabled.** If the log holds
  committed vector writes, the next open must enable vectors with the dimensions
  the writer used. Every CLI command now accepts the options, and the error says
  so.
- **A read-only open does not read the log.** After a crash it shows the
  database file as of its last flush, which can be behind the commits the log
  holds. Open the database read-write once to recover them.
<!-- Owner decision: fix before release, or ship with this limit. -->
- **`lattice replicate` sends a full snapshot on every run.** Each CLI run
  starts a new replication generation, so passes are not incremental. This is a
  regression in this release, from emptying the log at open and close.
- **The embedding client's timeout is not applied.** `timeout_ms` is accepted,
  but a request waits as long as the server takes.
- **`sync_mode` and `full_fsync` are set through the environment** outside Zig;
  the C API's open options do not include them.

## Upgrade notes

- The page layout and the log format are unchanged. Full-text indexes record how
  they analyse text when their first document goes in, under a new key.
- Close a database cleanly with this release before opening it with 0.16.0.
  0.16.0 does not know about the journal, so after a crash it would ignore an
  unfinished flush.
- 0.16.0 can open a database whose full-text indexes this release wrote, but it
  splits non-ASCII words the old way and will not find words with non-ASCII
  letters indexed by this release.

## Upgrading

```bash
pip install --upgrade latticedb
npm install @hajewski/latticedb@<version>
go get github.com/jeffhajewski/latticedb/bindings/go@v<version>
```

The Java binding (`io.latticedb:latticedb`) is not published to a Maven repository; build it from `bindings/java` (see its README).
