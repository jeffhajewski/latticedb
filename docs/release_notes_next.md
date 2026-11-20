# LatticeDB Next Release Notes

Use this file as the draft for the next release after `0.18.1`.

## Summary

- File format 5: new databases store node, edge and document ids in id order,
  which makes loading faster and the file smaller. **0.18.x and earlier cannot
  open a database this release creates.** Existing databases are not changed
  and stay readable by the releases that could read them before.
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
  nodes and 2,000,000 relationships, measured with a prototype of the change:
  the default load went from about 35 s to 27 s, a load without the log from
  8 s to 6.3 s, the node tree from 70.6 MB to 51.9 MB and the edge-ID index from
  100 MB to 69 MB (both 99% full, from 73% and 69%).
- **Fewer checkpoints with a large cache.** A commit used to run a checkpoint
  every 1,000 log frames (4 MB), whatever the cache. A checkpoint writes every
  page changed since the last one twice and syncs, so a large cache was flushed
  long before it filled. The default now lets the log grow to an eighth of the
  page cache, never less than 1,000 frames and never more than 64 MB: 3,200
  frames (12.5 MB) with the 100 MB cache of the C API and the bindings, 2,048 with
  the CLI's 64 MB, and 16,384 (64 MB) with 512 MB or more. Loading 1M nodes and
  2M relationships, 10,000 rows per transaction with the log on, took 12.1 s with
  a 1 GB cache before and 8.6 s now. With the 100 MB default cache the time does
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
- No converter yet: an existing database keeps the old layout until it is
  exported and imported into a new one.
- With a cache above about 31 MB the `-wal` file can now grow larger before a
  commit empties it (up to 64 MB), and a crash can leave that much log to replay
  at the next open. To keep the old interval, set it to 1,000 frames:
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

- New tests cover both layouts: id key encoding and order, scans of a
  format-5 file in ascending order with its id trees over 90% full, a
  little-endian database through creation, updates, deletes, full-text and
  vector search and reopen with its header still at `min_reader_version` 4,
  the header gates, `lattice info`, and the power-loss crash suite run on a
  little-endian file as well.
