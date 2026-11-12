# Blob Storage and In-Memory Databases

> **Status (0.16.0): partly built.** `serialize`/`deserialize` (buffer-based, not the
> reader/writer form specified below), `:memory:` databases and borrowed deserialization are
> built. Presigned upload, the signer layer and `BlobStore` are not. An in-memory database's
> page cache is a fixed 64 frames, not sized from the database as described below. See the
> book's [In-Memory Databases](../book/src/configuration/in-memory.md) page.

Two people have asked for the same shape of thing: many small graph databases
rather than one large one, kept in object storage, pulled down per unit of work,
mutated, and pushed back. One of them wants to skip the local filesystem
entirely and work in memory, the way SQLite's `:memory:` and its
`serialize`/`deserialize` pair let you.

This is the design for supporting that.

## Why this is cheap for us and expensive for others

The same request has been made of Kùzu, where it is hard, because a database
there is a catalog, metadata, data, index, and WAL spread across several files.
Serializing means inventing a container format for all of them and keeping it in
step with the engine.

A LatticeDB database is one file. Serializing it is reading that file. That is
not a clever trick, it is the whole reason the single-file format was worth
having, and it is the strongest answer we have to "why not Kùzu".

Better still, `Database.backup` already does most of the work. It flushes pending
writes into the file and copies it, producing something standalone that opens
with no write-ahead log beside it. That is exactly the blob somebody wants to put
in a bucket. Somebody could build this workflow on 0.13.0 today with a temporary
directory, and it would work.

## What is actually being asked for

Three separable pieces, worth naming because they have very different costs.

**Bytes in, bytes out.** Serialize a database to a buffer, and open a database
from one. Almost free, because of the above.

**Somewhere to put the bytes.** Getting them to S3, R2, Azure Blob, or GCS. This
looks like the expensive part and mostly is not ours to pay, for reasons below.

**Never touching the disk.** Running the whole engine against memory, so a
per-case database never becomes a file. This is the genuinely tricky one.

## What the engine already supports

Verified against the source rather than assumed, because the answer decides how
much of this is design and how much is plumbing.

The storage layer is already clean with respect to the filesystem. `wal.zig`,
`page_manager.zig`, `buffer_pool.zig`, `btree.zig`, `recovery.zig`, and
`checkpoint.zig` make **no direct filesystem calls at all** — every byte moves
through the `Vfs` interface. Nothing is memory-mapped, so there is no path that
assumes a real file underneath.

The `Vfs` vtable already covers what an alternative backend needs: `open`,
`delete`, and `exists`, plus per-file `read`, `write`, `sync`, `truncate`,
`size`, `close`, and the locking calls.

Only two places in the engine reach around it, both in `database.zig`: a
`deleteFile` on the log during one recovery case, and the `rename` that makes
`backup` atomic.

There is already an HTTP client with TLS in the tree, used by the embedding
client to reach Ollama and OpenAI, wrapped as `compat.httpClient`. `std.crypto`
provides HMAC-SHA256 and SHA-256.

The one real gap is that `Database.open` hardcodes its VFS: the field is a
concrete `PosixVfs` held by value and assigned during open, so there is no way to
hand the engine a different one.

## How the bytes should reach the cloud

This is the decision that shapes everything else, and the first draft of this
document got it wrong. It led with implementing SigV4 and SharedKey in the
engine. That is the wrong place to start, and the reason is not effort.

**Ask who is in the loop.** Both people who asked for this have applications. Those
applications already have a cloud SDK, already have credentials, and already have
a retry policy their operations team has opinions about. An engine that
implements SigV4 is reimplementing, worse, something the caller already has — and
taking custody of long-lived cloud credentials to do it. A database that holds
credentials is a different security proposition from one that does not, and it is
not a change to make in exchange for saving the caller four lines.

There is one case where nobody is in the loop: `lattice replicate` shipping to a
bucket with no application running. That case genuinely needs the engine to speak
to the network. It is also the case nobody has asked for yet.

So the design is three layers, and the value is heavily concentrated in the first.

### Layer 0 — bytes in, bytes out

```zig
try db.serializeTo(writer);                        // stream out
var db = try Database.deserializeFrom(reader, .{}); // stream in
```

No networking, no credentials, no providers. The application does its own I/O
with the SDK it already trusts:

```python
blob = s3.get_object(Bucket=b, Key=k)["Body"].read()
db = latticedb.deserialize(blob)
# ... mutate ...
s3.put_object(Bucket=b, Key=k, Body=db.serialize(), IfMatch=etag)
```

This completely serves both requests. It is also where conditional writes belong
for this workflow: the caller already holds the ETag and already knows what to do
when the condition fails, which is application logic we would only be guessing at.

Reader and writer based rather than `[]u8` based, so the same code streams to a
socket, a file, or a buffer without the whole database existing twice in memory.
The `[]u8` convenience wrappers sit on top for the languages that want them.

### Layer 1 — a URL and some headers

For when the engine should move the bytes but must not hold credentials, the
application presigns a request and hands over the result:

```zig
try db.uploadTo(.{ .url = presigned_put_url, .headers = extra });
```

The engine parses the URI, sets the method and headers, streams the body, and
reads the response status and ETag. That is the whole implementation. It has no
provider-specific code at all, because a presigned S3 URL, an R2 presigned URL,
an Azure SAS URL, and a GCS signed URL are all just URLs. A provider that appears
in five years works on the day it ships.

`std.http.Client` already streams request bodies, which is the real argument for
this layer over doing it in the application. Uploading a large database through
Layer 0 means materialising it as a buffer, handing that to the SDK, and probably
copying it again. Here the peak cost is one page. For the per-case databases
being asked about that does not matter; for a database that is actually large it
is the difference between working and running out of memory.

The cost is that presigned URLs expire and cannot express `list`.

### Layer 2 — native signers

Only for the headless case: `lattice replicate s3://bucket/path` with no
application anywhere. This is where SigV4, SharedKey, credential chains,
instance metadata, and per-provider quirks live, and it is the layer that should
wait until somebody needs it.

If it is built, one SigV4 implementation reaches S3, Cloudflare R2, and Google
Cloud Storage through its S3-compatible XML API with HMAC keys. That last one
matters: the native Google path needs OAuth2 with service-account JWTs signed
RS256, and `std.crypto` exposes RSA only inside certificate and TLS verification,
not as a signing API. Taking the S3-compatible route avoids writing RSA signing
from scratch, which is not a thing to do casually in a database. Azure needs a
second signer, but SharedKey is also HMAC-SHA256 over a canonicalised string.

No third-party dependency in any of it — the HTTP client is already in the tree
for embeddings, and `std.crypto` has the hashing. It still belongs behind a build
flag, because an embedded database that puts cloud support in every binary has
given something up.

### What replication needs

Replication's outstanding object-storage phase is the one real customer for
Layer 2, since a `--follow` loop writes objects whose names it decides as it goes
and no application is there to presign them. A `BlobStore` interface with a
directory-backed implementation comes first regardless, so that provider bugs and
logic bugs stay separable, and so the existing destination code has something to
become rather than sitting beside a parallel implementation.

### What serialize actually is

`serialize` is `backup` writing to a stream instead of a path, so it inherits the
behaviour that is already tested: pending writes are folded in first, and it
refuses while a transaction is open. Until Phase 2 lands, `deserializeFrom`
writes to a temporary file and opens that, which is not elegant but is correct
and useful immediately.

## Phase 2 — In-memory databases

### The plumbing

Four things, none of them deep:

1. `Database` holds a `Vfs` interface value rather than a concrete `PosixVfs`,
   and owns the backing object so its lifetime is not something callers have to
   reason about.
2. A `MemoryVfs` implementing the twelve vtable entries. Locking becomes a no-op
   that always succeeds, deliberately rather than accidentally: there is no
   second process to exclude from memory this process owns.
3. The two bypasses in `database.zig` route through the VFS, or a `:memory:`
   database will try to delete a real file called `:memory:-wal`.
4. `open` recognises `:memory:` as a path and picks the memory backend.

With those, `deserialize` stops needing a temporary file and `serialize` becomes a
copy out of memory.

### Asking for it

`:memory:` is a path rather than an option, following SQLite, and the reason is
that it costs nothing anywhere else. Every binding already takes an arbitrary
path string and passes it to `lattice_open`, so `Database(":memory:")` in Python,
`new Database(':memory:')` in TypeScript, `latticedb.Open(":memory:", opts)` in
Go, and `lattice query :memory:` all work without a single binding change.

An option would mean a new field, therefore a v5 options struct, therefore a
`lattice_open_v5`, therefore FFI declarations, public option types, and
documentation in four bindings — for the same capability.

The one binding that touches the path at all is Python, which wraps it in
`Path`, and `str(Path(":memory:"))` round-trips unchanged.

### Storing it in chunks

The memory VFS stores a file as a list of page-sized chunks rather than one
growing buffer.

This is not about allocation churn, though it avoids that too. A contiguous
buffer has to be reallocated as the file grows, and a reallocation moves every
byte to a new address. Any borrow handed out beforehand — the whole basis of
lending bytes to a caller or to the buffer pool — becomes a dangling pointer at
that moment. Chunked storage is what makes borrowing possible at all, so it goes
in from the start rather than being retrofitted under pressure later.

Worth deciding explicitly: an in-memory database should keep the log **on**.

The obvious argument runs the other way. A log exists so a crash cannot lose
committed data, a process holding the only copy of a database in its own heap
loses everything when it dies regardless, and the blob write is the real
durability boundary — so the log looks like pure cost.

Measured rather than assumed, that reasoning does not survive: with
`enable_wal = false`, `beginTransaction` returns `TransactionsNotEnabled`. There
are no explicit transactions at all, so no multi-statement atomicity and no
rollback. That is a far larger hole than the allocations it saves, and it would
make in-memory databases quietly less capable than file-backed ones in a way
nobody would expect.

(That was 0.16. Since 0.17.0 a database without a log has transactions too, and
`enable_wal = false` gives up only durability between flushes; the log stays on
for in-memory databases for the reasons above all the same.)

The log becomes a second file in the same memory VFS, bounded by automatic
checkpointing at a thousand frames, which is a few megabytes next to the database
it protects. Anyone who genuinely wants it off can still say so.

Bare writing queries keep working either way, because the implicit-transaction
path falls back to the older behaviour when transactions are unavailable rather
than refusing the write.

### What the buffer pool is for

Before deciding what to do about memory, it is worth being clear about what the
pool earns, because most of it has nothing to do with disks.

- **Pinning.** A page in use cannot be evicted from under its user. A B-tree
  traversal holds several at once.
- **Latching.** A reader-writer latch per frame is how concurrent access to one
  page stays correct.
- **Dirty tracking.** Knowing which pages need writing back is the checkpoint
  protocol, not an optimisation.
- **A stable address.** The engine holds a `*BufferFrame` and works through
  `frame.data`. That is its interface to storage.
- **Bounded memory.** Query a hundred gigabytes in forty megabytes of RAM.
- **Caching.** Avoid going back to storage.

Only the last two are about disk. In memory, caching is a copy versus a copy, and
bounded memory is moot when the database is in RAM by definition. The first four
are load-bearing whatever sits underneath, so any scheme that removes the pool
for in-memory databases has to reproduce all four. That is the real argument
against the more aggressive options below.

### Sizing the pool

The cost here is easy to state wrongly, and an earlier draft of this document did.
The pool is **fixed size and allocated eagerly**: `BufferPool.init` allocates a
page-sized buffer per frame in a loop, and the count comes from a configured
byte budget. It does not grow with the database.

So peak memory for an in-memory database is **database size plus pool size**, not
twice the database. For a gigabyte database against the default budget — sixteen
megabytes, plus twelve each for full-text and vector search — the overhead is a
few per cent.

Which turns the problem around. The waste is not large databases, it is small
ones: a five megabyte per-case database allocating forty megabytes of frames it
can never fill. That is precisely the workload this feature exists for.

The fix is to cap the in-memory pool at `min(default, database size + headroom)`.
For the five megabyte database that is a five megabyte pool, in which every page
fits, the hit rate is a hundred per cent, and nothing is ever evicted — a better
cache than the forty megabyte pool, which also had a hundred per cent hit rate and
simply held thirty-five megabytes of frames that were never used. For a gigabyte
database the minimum picks the default and nothing changes. The rule only bites
where the pool was over-provisioned relative to the data, so it is not a trade at
all in the case that matters.

**There is a hard floor, and it is a correctness requirement.** When the clock
sweep finds no evictable frame, `BufferPool` returns `BufferPoolFull`, which
surfaces as a failed query rather than a slow one. The pool must therefore always
hold at least the largest simultaneously-pinned set.

That number was measured rather than guessed, because guessing it trades memory
against query failures and neither side of that trade should be a hunch. Pools
from four frames upward were run against a fifteen-hundred node graph with edges
and a text index, through a deep variable-length traversal, a filtered scan, a
full-text search, and a bulk write. **Four frames was enough for all of it.** The
engine simply does not hold many pages pinned at once.

The floor is set at sixty-four, sixteen times the observed requirement, because
the measurement was single threaded and concurrent readers each pin pages of
their own. At 256 KB that margin is cheap.

An earlier attempt measured this by instrumenting the pin counter and produced
three hundred thousand concurrent pins on a two-thousand node database, which is
plainly impossible — there was a decrement path the instrumentation missed.
Measuring the thing that actually matters, the smallest pool that completes the
work, needed no instrumentation and could not drift.

Note also where the floor applies at all. The pool is the smaller of the
configured budget and the database plus the floor, so for anything but a very
small database the database term dominates. A database small enough for the floor
to matter has too few pages to pin many of them, which is the opposite of the
dangerous case.

### Borrowing the caller's bytes

`deserialize` copies the caller's buffer into the memory VFS today, which means a
database exists twice during load. Chunks can instead point into the caller's
buffer, with the first write to a chunk converting it to an owned copy. A
read-mostly workload — open a case, query it, put it back — then holds one copy
rather than two, without going anywhere near the buffer pool.

Whether a language can do this is not a matter of how carefully its binding is
written. Python and TypeScript can: hold a reference to the buffer on the wrapper
object and the collector cannot take it, and Python's `bytes` are immutable so
there is no mutation hazard either.

Go cannot. The cgo rules say plainly that "C code may not keep a copy of a Go
pointer after the call returns... C code may not keep a copy of a string, slice,
channel, and so forth, because they cannot be pinned with `runtime.Pinner`."
Java cannot either, for a different reason: pinning a `byte[]` for the lifetime
of a database can block the collector for as long as that database is open, which
is not a thing to do to somebody's JVM.

So there are two entry points, and each language uses the one its rules allow.
The copy happens inside the engine, so the bindings that must copy do not
hand-roll anything: they call the copying entry point. The same constraint
already applies in the other direction on `serialize`, where Go copies out with
`C.GoBytes` before the engine's buffer is released.

### Zero-copy pages: measured, and not needed

Pool-level borrowing — frames pointing into the memory VFS for clean pages,
copying on first write — was the original answer to double buffering. It is now
closed rather than deferred, because the numbers say there is nothing left for it
to save.

What an in-memory database actually costs, measured across sizes:

```text
   100 nodes | file    240 KB | vfs holds    372 KB | pool 256 KB | ratio 2.62x
  1000 nodes | file   1896 KB | vfs holds   3156 KB | pool 256 KB | ratio 1.80x
  4000 nodes | file   7416 KB | vfs holds   7420 KB | pool 256 KB | ratio 1.04x
 10000 nodes | file  18432 KB | vfs holds  18436 KB | pool 256 KB | ratio 1.01x
```

The pool is a constant 256 KB. Borrowing pages out of it could save at most that
much, against putting a new state — "this frame might not own its bytes" — into
the structure every read and write in the engine runs through. That is not a
trade worth making at any database size.

The reason a small pool is acceptable was also measured rather than assumed. The
pool exists to keep pages off a disk, and a miss against memory is a copy from one
part of RAM to another. On a fourteen megabyte in-memory database, twelve full
scans took 9.2 seconds against a 256 KB pool and 9.9 against a 32 MB one. The
larger cache bought nothing at a hundred and twenty times the memory.

Note what the remaining overhead is at small sizes. The 2.6x at a hundred nodes is
not the pool and not duplication; it is the write-ahead log, which is a second
file in the same memory backend. It disappears as automatic checkpointing starts
truncating it, which is why the ratio falls to 1.01x rather than rising.

There was one further alternative, recorded so it is not arrived at by elimination
later: make the pool itself the storage for in-memory databases, with the VFS a
view over frames and nothing ever evicted. It is rejected for the same reason and
one more — it would make the in-memory path structurally different from the file
path, and that sameness is why this feature was cheap.

## Phasing

**Phase 1 — Serialization.**
`serializeTo` and `deserializeFrom` over readers and writers, with buffer
conveniences on top, and the bindings to use them. No networking. This is the
whole of what was asked for and should ship on its own.

**Phase 1b — Presigned transfer.**
Upload and download against a caller-supplied URL and headers, streaming, with
the response ETag returned. No provider-specific code.

**Phase 2 — In-memory databases.**
A pluggable VFS, a chunked `MemoryVfs`, the two bypasses routed through it, and
`:memory:` recognised as a path. Pool sizing and the floor test come with it,
since together they are what makes a small in-memory database cheap. Borrowing
the caller's bytes on deserialize belongs here too, for the languages that permit
it.

**Phase 3 — Zero-copy pages.** *Closed, not needed.* The benchmark it was gated
on says the pool is a constant 256 KB and total overhead reaches 1.01x, so there
is nothing material left to save and no reason to put a new state into the buffer
pool.

Phase 1 is worth having on its own even if the rest never happens, since it is
what both requests actually need.

## What stays out

Credentials management beyond reading configuration. Bring your own credentials.

Coordination between workers over one blob. The conditional write is the tool;
deciding what to do when it fails is application logic, and a database that
invented a leasing protocol would be guessing at requirements.

Anything resembling a distributed database. This is copying files to a bucket and
back, and it should keep looking like that.

## Open questions

- **Should `serialize` compress?** Page-shaped data compresses well and blob
  storage charges for bytes, but a compressed blob is no longer a database file
  somebody can open directly, which is a real part of the appeal. Probably an
  option, defaulting to off.
- **Should `openFromBytes` take ownership of the buffer?** Borrowing avoids a copy
  of the whole database, at the cost of a lifetime rule that is easy to get wrong
  from a garbage-collected language through the C API.
- **How does this interact with the file lock?** An in-memory database has no
  file to lock and no second process to exclude. A blob pulled to a temporary
  path does, and two workers pulling the same object to the same path on one
  machine is a case the lock would usefully catch.
- **Does `list` belong in the interface?** Restore needs it to find generations.
  The blob workflow does not need it at all, and it is the operation whose
  semantics vary most between providers.
