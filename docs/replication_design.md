# Continuous Backup and Replication Design

> **Status (0.16.0): phases 1–5 are built for local directories; phase 6 (object storage) is
> not.** The text below is the original design and describes the state before it was built:
> automatic checkpointing now exists (by default once the WAL reaches an eighth of the page cache,
> 1000 frames to 64 MB), and the destination
> layout differs from the one sketched here (`manifest.json`, `gen-N/snapshot.lattice`,
> `gen-N/frames/A-B.frames`). `lattice restore s3://...` is not supported. For how it works today,
> see the book's [Backup and Replication](../book/src/guides/backup-and-replication.md) guide.

LatticeDB has no continuous backup story. Today the only correct backup is to
close the database and copy the file, or to take a logical export with
`lattice export`. Neither gives point-in-time recovery, and neither is
incremental.

This is the design for closing that gap, aimed at the case people actually ask
about: one machine, one writer, a few minutes of tolerable downtime, and a
desire not to lose the last hour of writes when the disk dies.

## Goals

- Continuously ship changes off the machine, so a disk failure costs seconds
  rather than everything since the last manual copy.
- Restore to a point in time, not just to the last snapshot.
- Require no change to how applications write. Replication is something you turn
  on beside the database, not something queries have to know about.
- Make an incomplete or interrupted backup detectable rather than silently
  partial.

## Non-goals

- High availability. There is no failover, no leader election, and no second
  writer. This is backup and restore, not clustering.
- Multi-writer replication. The engine allows one writer, and this does not
  change that.
- Streaming to a follower that serves reads. That is a larger feature and should
  not be smuggled in through the backup path.

## What exists today

Verified against the current source rather than assumed:

- **The WAL is append-only while the database is open.** `AutoCheckpointConfig`
  exists in `src/storage/checkpoint.zig` but nothing constructs it, so no
  automatic checkpointing happens. In a 400-write run the WAL grew from 8 KB to
  1.6 MB and never shrank.
- **Checkpointing happens only on close**, through `checkpointWal(.truncate)`,
  or explicitly via the C API and `lattice compact`. On a clean close the WAL is
  truncated to a bare 4096-byte header and the main file becomes self-contained.
- **Frames are individually addressable and checksummed.** `WalFrameHeader`
  carries `frame_number`, `record_count`, `data_size`, `prev_frame_lsn`, and a
  CRC32C of the frame data. Frames are fixed size, so frame `n` lives at
  `WAL_HEADER_SIZE + n * frame_size`.
- **The WAL header binds to one database.** It stores `database_uuid`, which must
  match the main file, plus `frame_count` and `checkpoint_lsn`.
- **A frame is published after it is durable.** `flushCurrentFrame` assembles the
  whole frame in a buffer, writes it in a single call at a computed offset, and
  only then increments `frame_count` and rewrites the header.

That last point is the important one. It means a second process can read
`frame_count` from the header and trust that every frame below it is complete. A
reader never has to guess whether it is looking at a half-written frame, and if
a torn read happens anyway the per-frame checksum catches it.

## Physical or logical

Two substrates are available.

**Logical**, by shipping the built-in `__lattice_changes` changefeed, which
already exists in `src/stream/store.zig`. Attractive because the machinery is
there and the output is inspectable.

**Physical**, by shipping WAL frames.

This design chooses physical, for three reasons.

Replaying a logical changefeed has to reproduce *derived* state, not just user
data. The HNSW vector index, the BM25 inverted index, and property indexes are
all rebuilt as a side effect of replay, and any divergence between how the
original and the replica build them is a silent correctness bug that only
surfaces as wrong query results much later. Physical replay copies the pages and
sidesteps the question.

A backup is judged on whether the restored database is the same database.
Physical shipping gives byte-equivalence; logical gives "equivalent as far as we
modelled".

And the property that usually makes physical replication hard is absent here.
Litestream's central difficulty with SQLite is that checkpointing recycles WAL
frames out from under the replicator, which is why it holds a long-lived read
transaction to block checkpoints. LatticeDB does not checkpoint automatically at
all, so frames are stable until an explicit truncate. The hard part is already
solved by accident.

## Design

### Generations

WAL frame numbering restarts after a truncate, so `(database_uuid, frame_number)`
is not unique over the life of a database. A **generation** is the span between
two truncations: it begins with a full snapshot of the main file and continues
as a sequence of WAL frames.

A generation id is needed in the WAL header. `WalHeader` has a `_reserved: u16`
and 4048 bytes of padding, so a `generation: u64` fits without changing the
header size or breaking the format version — though it does need a
`WAL_FORMAT_VERSION` bump and a read path for the older layout.

The generation counter increments on every truncate and is durable in the main
file header.

### Layout at the destination

```
<root>/<database_uuid>/
  generations/
    <generation>/
      snapshot.lattice.zst        full main file at generation start
      wal/
        0000000000.frames.zst     frames [0, 1024)
        0000001024.frames.zst     frames [1024, 2048)
        ...
      manifest.json               generation, frame ranges, checksums, timestamps
  latest.json                     pointer to the newest complete generation
```

Segments are batches of frames rather than one object per frame, because object
storage charges per request and a 4 KB object is all overhead. The batch size is
a tuning knob; the manifest records actual ranges so it can change without
breaking restore.

### The replicator loop

The replicator is a separate process reading the same files. It does not open
the database.

1. Read the WAL header. Verify magic, `database_uuid`, and header checksum.
2. If `generation` differs from the last known one, a truncate happened: start a
   new generation, which means taking a fresh snapshot.
3. Read frames from `last_shipped` to `frame_count`, at computed offsets.
4. Verify each frame's CRC32C. A mismatch means a torn read of a frame being
   written; back off and retry rather than treating it as corruption.
5. Batch, compress, upload. Record progress durably *after* the upload lands.
6. Sleep, repeat.

No coordination with the writer is required for any of this, which is what makes
it safe to add without touching the write path.

### Snapshots

A generation starts with a snapshot of the main file. Taking one requires the
main file to be consistent, which today means either a clean close or an
explicit checkpoint.

`lattice checkpoint` should therefore become a supported operation that flushes
without closing, so the replicator can ask for a consistent snapshot point on a
running database. The `full` checkpoint mode already exists and does not truncate,
which is exactly the semantics needed: flush dirty pages to the main file, leave
the WAL alone, do not disturb frame numbering.

### Restore

```bash
lattice restore s3://bucket/backups --output=restored.lattice
lattice restore s3://bucket/backups --output=restored.lattice --at="2026-08-20T14:00:00Z"
```

1. Read `latest.json`, pick the generation covering the target time.
2. Download and decompress the snapshot.
3. Download WAL segments in order, verifying checksums.
4. Reconstruct a WAL file containing frames up to the target LSN.
5. Open the database, which runs ordinary recovery over that WAL.

Restore deliberately reuses the existing recovery path rather than
reimplementing replay. If recovery has a bug, restore should have the same bug,
not a different one.

### The close problem

Truncation on close discards frames. If the replicator is behind when the
process exits, those frames are gone and the generation ends short.

Two mitigations, in order of preference:

- **Ship before truncating.** Close already does meaningful work; a hook that
  waits, briefly and with a timeout, for the replicator to acknowledge the
  current `frame_count` is cheap and closes the window in the normal case.
- **Snapshot on generation start.** Even if the tail is lost, the next
  generation opens with a full snapshot, so the backup is complete as of the
  restart. What is lost is point-in-time recovery *into* the gap.

Neither is free, and the first needs a timeout so a dead replicator cannot hang
shutdown.

## Prerequisite: bounded WAL growth

There is a problem to fix before any of this, and it is a problem independent of
replication.

Because nothing checkpoints automatically, a long-running process accumulates
WAL frames without limit. The exact deployment this feature targets — one
webserver holding the database open for weeks — is the worst case. The WAL grows
until the process restarts, and recovery time grows with it. `AutoCheckpointConfig`
was written for this and never wired up.

Replication makes the tension sharper rather than causing it: checkpointing
frequently keeps the WAL small but shortens the window in which frames are
available to ship. The policy needs to consider both, which argues for
checkpointing on a threshold *and* honouring a replication low-water mark, so a
checkpoint never discards frames the replicator has not acknowledged.

This should be built first. It is smaller, it is needed regardless, and getting
the checkpoint policy wrong afterwards would mean redesigning the replicator
around it.

## Phasing

**Phase 1 — bounded WAL.** *Done.* Automatic checkpointing on a frame threshold,
plus `lattice checkpoint`. The minimum-interval knob turned out to be actively
harmful under `.truncate` and defaults to zero; the frame threshold is
self-limiting because a checkpoint resets the counter.

**Phase 2 — `lattice backup`.** *Done.* `Database.backup` and `lattice backup`
take a consistent copy of a running database. The copy is standalone, written
beside the destination and renamed into place, and refused while a transaction is
open.

**Phase 3 — WAL reader.** *Done.* `storage/wal_reader.zig` opens a log read-only
and hands back whole frames, independently of the writer. It verifies the header,
verifies each frame, distinguishes a torn read from real damage, and reports a
truncation as `Rewound` rather than letting a follower carry on counting through
frame numbers that no longer mean what it thinks. `readFrameRetrying` handles the
transient case.

**Phase 4 — `lattice replicate`.** *Done.* `Database.replicateTo` ships changes
into a directory, and `lattice replicate` wraps it with a `--follow` loop. The
destination holds a manifest, a snapshot per generation, and frame segments named
for the range they cover. Segments and the manifest are written beside their final
name and renamed into place, so an interrupted pass leaves nothing that looks
complete.

Two things came out differently from the sketch above.

Replication turned out to belong *inside* the process rather than beside it. A
generation opens with a snapshot, a snapshot needs no writes in flight, and there
is no cross-process locking to arrange that from outside. Reading frames from
another process is still perfectly safe, as Phase 3 showed; taking the snapshot
is not.

The generation counter went into the database file header rather than the log.
`checkpoint_seq` had been sitting there unused since the format was written, so
no format change was needed. It also turned out to be the only place the counter
*could* live, because closing a database folds changes into the file: two runs of
a command-line tool can move an arbitrary amount of data while the log looks
untouched at both ends. Anything kept in the log is thrown away by exactly the
event it is supposed to record.

Building this also turned up a bug that would have made the whole feature a lie.
A writing query that was not handed a transaction ran straight against the pages
and never touched the log, which is how the command line and every client library
issue writes. A backup would have missed nearly everything and reported success.
That is fixed separately, because it was a durability bug in its own right.

**Phase 5 — restore and point-in-time.** *Done for local destinations.*
`storage/restore.zig` and `lattice restore` rebuild a database from a destination,
with `--at` for a moment in the past.

Restore copies the snapshot, rebuilds a log from the segments, opens the result so
ordinary recovery replays it, then folds that in and clears the log. Reusing
recovery rather than reimplementing replay was the main decision: a second
implementation would drift from the real one, and a restore that diverges from
recovery is worse than one that shares its bugs.

Frames are packed from position zero rather than kept at their original numbers.
A generation rarely starts shipping at frame zero, and a gap below the first
shipped frame reads as a stretch of empty frames that recovery treats as damage.
Recovery walks frames by position and never reads the number inside one, so
packing is free.

Point-in-time resolution is a replication pass, not a single write. The manifest
records when each segment landed, and a restore applies every segment shipped at
or before the target. Claiming finer precision would mean replaying part of a
frame, which is not something the log supports and not something anybody asked
for.

**Phase 6 — object storage.** S3-compatible destinations, which is the part of
the original Phase 5 that is still outstanding. The destination layout is already
the shape object storage wants: whole objects, written once, named by content
rather than mutated in place, with a manifest naming them all.

This is now folded into [blob storage and in-memory databases](blob_and_memory_design.md),
which needs the same thing for a different reason. One `BlobStore` interface
serves both, and the cost turned out lower than "a dependency decision" implied:
`std.crypto` covers the signing, the HTTP client is already in the tree for
embeddings, and one SigV4 implementation reaches S3, Cloudflare R2, and Google
Cloud Storage through its S3-compatible API.

Phases 1 through 3 are worth doing even if the rest never happens.

## Open questions

- **Does the generation id belong in the WAL header or the main file header, or
  both?** Both is probably right, since the replicator reads the WAL and the
  restore path reads the main file, and disagreement between them is a
  detectable error worth having.
- **Should the replicator be a subcommand or a separate binary?** A subcommand
  is easier to ship and document; a separate binary keeps the CLI free of
  long-running processes and cloud SDK dependencies.
- **How is the replication low-water mark communicated to the writer?** A file
  the replicator updates is the simplest thing that works and needs no IPC.
- **What is the compression story?** Frames are page-shaped and compress well,
  but a per-segment codec choice needs to be in the manifest so it can change.
- **How is this tested?** Crash-consistency testing is the hard part. The
  existing `zig build crash-test` harness is the natural place, extended to
  assert that a restored copy matches the original after a kill at an arbitrary
  point.
