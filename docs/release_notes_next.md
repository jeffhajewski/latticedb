# LatticeDB Next Release Notes

Use this file as the draft for the next release after `0.18.1`.

## Summary

- File format 5: new databases store node, edge and document ids in id order,
  which makes loading faster and the file smaller. **0.18.x and earlier cannot
  open a database this release creates.** Existing databases are not changed
  and stay readable by the releases that could read them before.
  `lattice compact --upgrade-id-keys` converts one to format 5 if you want the
  new layout; it is optional and one-way.
- A commit that creates many relationships files them in key order, so a bulk
  load gets faster the larger its transactions are: 1M nodes and 2M
  relationships with the default settings load in 11.3 s at 100,000 rows per
  transaction, where 0.18.1 took 33.6 s.
- The automatic checkpoint interval now follows the page cache. With a large
  cache the log can grow to 64 MB before a commit checkpoints, instead of 4 MB,
  and bulk loads with the log on get faster.

## Highlights

- **Ids are stored in order.** The B+Tree compares keys byte by byte, and until
  now the node tree, the edge-ID index, the full-text document trees and the
  vector index's node tree wrote their u64 ids little-endian. Byte order and id
  order disagreed (id 256 sorts between 0 and 1), so ids handed out in sequence
  scattered across the whole tree: every insert dirtied a different leaf, and
  the trees ended about 70% full. Format 5 writes these keys big-endian. New
  ids append at the right edge and the leaves fill. On a load of 1,000,000
  nodes and 2,000,000 relationships, measured with a prototype of this change
  alone:
  the default load went from about 35 s to 27 s, a load without the log from
  8 s to 6.3 s, the node tree from 70.6 MB to 51.9 MB and the edge-ID index from
  100 MB to 69 MB (both 99% full, from 73% and 69%).
- **Older files can be upgraded to the new layout.** `lattice compact
  --upgrade-id-keys <path>` converts a database created by 0.18.1 or earlier to
  format 5. You do not have to: an existing database keeps working with this
  release as it is. Upgrading gives it what a new file has, scans in ascending
  id order and full id trees. The command writes the upgraded database to a
  copy beside the original, checks the copy against the original (every node,
  edge, full-text and vector entry, the next ids, and that no page is used twice
  or lost), and only then renames it into place. If anything fails, the copy is
  deleted and the original is as it was. **The upgrade is one-way: 0.18 and
  earlier cannot open the upgraded file**, and there is no way back but an
  export from this release. See
  [Upgrading an older file to format 5](../book/src/getting-started/cli.md#upgrading-an-older-file-to-format-5).
- **Large transactions load relationships much faster.** A relationship has two
  entries in the edge tree, one under each of its nodes, and they belong wherever
  those nodes' other relationships are. Applied one relationship at a time, a
  transaction's entries reached the tree in an order unrelated to where they go:
  once the tree outgrew the cache nearly every entry dirtied a page of its own,
  and each flush wrote all those pages (twice) only for the next entries to
  change them again. 0.18.1 wrote 37 GB to build a 500 MB file. A commit that
  creates 64 or more relationships now sorts all their entries and inserts them
  in order, then writes the relationship records. Each page gets everything the
  transaction has for it in one visit. The gain grows with the transaction: at
  10,000 relationships a transaction has about one entry per page it touches,
  at 100,000 about four. See the table below and
  [Bulk loading](../book/src/configuration/durability.md#bulk-loading).
- **Bulk load, all three changes together.** 1,000,000 nodes and 2,000,000
  relationships between random pairs, through the C API, median of three runs
  on an Apple M5; defaults are the log on with `full` sync, the changefeed on
  and a 100 MB cache:

  | Settings | Rows per transaction | 0.18.1 | This release | Written (0.18.1 → now) |
  |----------|---------------------:|-------:|-------------:|-----------------------:|
  | Defaults | 1,000 | 37.9 s | 28.6 s | 39 → 29 GB |
  | Defaults | 10,000 | 35.9 s | 23.4 s | 38 → 25 GB |
  | Defaults | 100,000 | 33.6 s | 11.3 s | 37 → 5.2 GB |
  | Defaults | 1,000,000 | | 8.7 s | 1.9 GB |
  | 1 GB cache | 10,000 | 16.7 s | 9.3 s | 12.9 → 2.4 GB |
  | No log, changefeed off, 1 GB cache | 10,000 | 8.2 s | 6.4 s | 1.1 → 0.9 GB |

  The file is 451 to 461 MB with the defaults (0.18.1: 500 MB). SQLite, loading
  the same rows into two tables with an index on each end of the relationship,
  WAL mode, `synchronous=FULL` and a 100 MB cache, took 8.7 s at 10,000 rows per
  transaction and 3.4 s at 100,000, with a 117 MB file.
- **Fewer checkpoints with a large cache.** A commit used to run a checkpoint
  every 1,000 log frames (4 MB), whatever the cache. A checkpoint writes every
  page changed since the last one twice and syncs, so a large cache was flushed
  long before it filled. The default now lets the log grow to an eighth of the
  page cache, never less than 1,000 frames and never more than 64 MB: 3,200
  frames (12.5 MB) with the 100 MB cache of the C API and the bindings, 2,048 with
  the CLI's 64 MB, and 16,384 (64 MB) with 512 MB or more. Loading 1M nodes and
  2M relationships, 10,000 rows per transaction with the log on, took 12.8 s with
  a 1 GB cache at the old interval and 9.2 s at the new one. With the 100 MB default cache the time does
  not change measurably: there the flush that runs when half the cache is dirty
  does most of the writing.

## API Notes

- `DatabaseConfig.new_database_id_keys` (Zig) chooses the id key layout of a
  database the open creates: `.big`, the default, is format 5; `.little` lays
  the file out as 0.17 and 0.18 do, for a new file an older release must read.
  It is ignored for an existing file, which keeps the layout it was created
  with. The C API and the bindings have no option for it; they create format-5
  files.
- `lattice info` shows the file's format version as its header records it
  (before, the build's format) and a new `Id keys` line (`id_keys` in JSON and
  CSV): `big-endian` or `little-endian`.
- `lattice info` and `lattice check` name the new reader reason `ordered id
  keys` and say `v5, so 0.18 and earlier cannot open it`.
- Zig: `AutoCheckpointConfig.max_wal_frames` is now `?u64`, and null, the
  default, means "scaled to the buffer pool". `Database.autoCheckpoint()` still
  returns the policy commits use, with the frame count filled in. The scaling is
  `AutoCheckpointConfig.defaultMaxWalFrames(cache_bytes, frame_size)`. Code that
  reads `max_wal_frames` from a policy now gets an optional.
- No change to the C API's signatures or the bindings' code. `lattice_set_auto_checkpoint`, each
  binding's setter, `--auto-checkpoint` and `LATTICE_AUTO_CHECKPOINT_FRAMES`
  take a frame count and use it as given.

## Upgrade Notes

- **A database created by this release cannot be opened by 0.18.x, 0.17.x or
  0.16.0.** Its header asks for a format-5 reader, so those releases refuse it
  and leave it untouched: 0.17 and 0.18 with their "version too new" error
  (`LATTICE_ERROR_VERSION_MISMATCH` through the C API and bindings), 0.16.0
  with the I/O error it gives any file whose reader version is above 3. To hand a new database to an
  older release, export it to JSON and import it there, or create it from Zig
  with `new_database_id_keys = .little`.
- **Existing databases are untouched.** A file created by 0.18.1 or earlier
  keeps its little-endian keys: nothing is converted when it is opened, and
  every read and write on it uses the old layout. Writing to it with this
  release does not stop 0.17 and 0.18 from reading it. Like every release
  since 0.17.0, this one marks a file it writes as needing its own format to
  write (`min_writer_version` 5), so 0.17 and 0.18 then open it read-only;
  0.16.0's gate is unchanged (it opens the file until it holds something it
  would misread).
- **Scan order of new files is ascending id.** In a format-5 file a walk of the
  edge-ID index, and so the row order of a pattern that starts from a
  relationship type with nothing else bound (`MATCH ()-[r:KNOWS]->()`), is
  ascending edge id, where a file created earlier gives a stable order that is
  not numeric (edge 256 before edge 2). `MATCH (n)` was already sorted and is
  unchanged. Neither order is promised; use `ORDER BY` when the order matters.
- The minimum reader version that changefeed events, ordered property
  indexes, vector index settings and full-text positions require is now 4, the
  format that introduced them, rather than "this release's format". It is the
  same number as before for files that already had it, and it is what keeps
  older files open to 0.17 and 0.18.
- **Upgrading an existing database is optional.** It keeps working with this
  release in its old layout. `lattice compact --upgrade-id-keys <path>`
  converts it to format 5 (it can be combined with `--upgrade-indexes`). What to
  know before running it:
  - **It is one-way.** 0.18, 0.17 and 0.16.0 refuse the upgraded file
    (`LATTICE_ERROR_VERSION_MISMATCH`) and leave it unchanged. Keep a backup if
    an older release still has to open the data.
  - The original is replaced only after the upgraded copy has passed every
    check. If the command is stopped part way, the path still holds a whole
    database, the original or the upgraded one, and running the command again
    finishes the job. It refuses while another process has the file open.
  - The upgraded file is usually a little **larger**, not smaller: the old
    trees' pages are freed in the middle of the file, where `compact` cannot cut
    them off, and are reused as the database grows. A 1.9 MB file created by
    0.18.1 grew 6%, to 2.0 MB, and a 7.8 MB test database with 66,000 nodes
    grew 20%, to 9.4 MB. Expect growth of up to about the size of the node
    tree, the largest tree converted, whose new pages have to exist while its
    old ones are read. The command needs that much free disk space beyond the
    second copy.
  - The upgraded file gets a new file id, so a replication directory made from
    it before the upgrade is refused (`replicate` reports a UUID mismatch).
    Start a new one.
  - Scan order changes as for a new file: a walk of the edge-ID index, as in
    `MATCH ()-[r:KNOWS]->()` with nothing else bound, now returns ascending edge
    ids.
  - It runs from the CLI only; the C API and the bindings have no call for it.
- **A crashed database is recovered by this release, not an older one.** That
  was already so: 0.17 and 0.18 may read, but not write, a file this release has
  written, and they refuse it outright while its log still holds work to replay
  (`LATTICE_ERROR_NEEDS_NEWER_WRITER`, the file left unchanged). It matters a
  little more now, because a large commit writes all of its relationships' tree
  entries before their records, and a crash in between leaves entries without
  a record in the file, which only this release's recovery completes. Checked
  with 0.18.1 on an old-layout file interrupted that way: refused before
  recovery, read correctly after this release had opened it once.
- For loads, prefer large transactions (100,000 relationships or more) over
  many small ones; a transaction holds 600 to 700 bytes of memory per row until
  it commits.
- With a cache above about 31 MB the `-wal` file can now grow larger before a
  commit empties it (up to 64 MB), and a crash can leave that much log to replay
  at the next open: about 2 s for a full 64 MB of bulk-load log, against 0.2 to
  0.4 s for the old 4 MB. To keep the old interval, set it to 1,000 frames:
  `LATTICE_AUTO_CHECKPOINT_FRAMES=1000`, `--auto-checkpoint=1000`,
  `lattice_set_auto_checkpoint(db, 1000, 0)`, a binding's
  `set_auto_checkpoint(1000)` / `setAutoCheckpoint(1000)` /
  `SetAutoCheckpoint(1000, 0)`, or `.auto_checkpoint = .{ .max_wal_frames = 1000 }`
  in Zig.
- Smaller caches keep 1,000 frames: the Zig API's default of 28 MB (16 MB with
  full-text search off) and every in-memory database. The Zig default with
  vectors on, 40 MB, now gives 1,280.
- The default is counted in bytes of log, so a database with pages larger than
  4096 bytes checkpoints after fewer of its frames than before: with 16 KB pages
  and a small cache, 250 frames (4 MB) where it used to be 1,000 (16 MB).

## Validation Notes

- The id key upgrade is tested on an old-layout database with something in
  every tree it rewrites: nodes with several labels, lists and values larger
  than a page, ids past 256 and 65,536, deleted nodes and relationships,
  relationship properties, three full-text indexes (two scopes on nodes, one on
  relationships), vectors, a property index and the changefeed. Queries,
  traversals both ways, full-text results and scores, vector search, property
  index lookups and `ORDER BY`, and the changefeed must all come back the same
  after the upgrade, after new writes, after a reopen, and after every page the
  upgrade freed has been reused; scans must come back in ascending order and the
  page accounting must find no page used twice. Also tested: an empty database,
  one with only nodes, one without full-text or vectors, a format-5 file (left
  alone), the refusals (open transaction, in memory, read-only, existing
  destination), a failure injected after the copy, half way through the
  conversion and in verification (source file unchanged, no files left), a stop
  after each step of the swap (the path opens whole and a rerun finishes), and
  the CLI's table, JSON and CSV output.
- By hand with 0.18.1: a file it created with full-text indexes, vectors and a
  property index gave the same query, search and changefeed results through
  this release's library before and after `compact --upgrade-id-keys` (except
  the order of an unordered `MATCH ()-[r:R]->()`, now ascending), and 0.18.1
  then refused it, read-only and read-write, leaving its bytes unchanged.

- The sorted apply is held to the edge-by-edge one: transactions on either side
  of the 64-relationship threshold, in both file layouts, must leave the same
  records, the same relationships from each end of every node, and exactly two
  tree entries and one record per relationship, before and after a reopen;
  likewise a large transaction that also updates and deletes relationships, one
  without a log, one rolled back, and one committed past an open reader.
- A crash test stops a large commit after 0, 1, half, all and all but one of
  its sorted entries have been flushed to the file, and requires recovery to
  bring back every relationship exactly once, from a copy taken as if the
  process had been killed and from the file after a clean close. With the old
  recovery code put back, that test fails and the other 82 pass.

- New tests cover both layouts: id key encoding and order, scans of a
  format-5 file in ascending order with its id trees over 90% full, a
  little-endian database through creation, updates, deletes, full-text and
  vector search and reopen with its header still at `min_reader_version` 4,
  the header gates, `lattice info`, and the power-loss crash suite run on a
  little-endian file as well.
