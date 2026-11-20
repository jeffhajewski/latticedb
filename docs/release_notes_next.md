# LatticeDB Next Release Notes

Use this file as the draft for the next release after `0.18.1`.

## Summary

- The automatic checkpoint interval now follows the page cache. With a large
  cache the log can grow to 64 MB before a commit checkpoints, instead of 4 MB,
  and bulk loads with the log on get faster.

## Highlights

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

- Zig: `AutoCheckpointConfig.max_wal_frames` is now `?u64`, and null, the
  default, means "scaled to the buffer pool". `Database.autoCheckpoint()` still
  returns the policy commits use, with the frame count filled in. The scaling is
  `AutoCheckpointConfig.defaultMaxWalFrames(cache_bytes, frame_size)`. Code that
  reads `max_wal_frames` from a policy now gets an optional.
- No change to the C API's signatures or the bindings' code. `lattice_set_auto_checkpoint`, each
  binding's setter, `--auto-checkpoint` and `LATTICE_AUTO_CHECKPOINT_FRAMES`
  take a frame count and use it as given.

## Upgrade Notes

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

- None yet.
