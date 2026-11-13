# Portable Databases

## What It Is

A LatticeDB database can be handed around as a block of bytes, and it can run with
no files behind it at all. Those are two features and one idea: a database is a
single file, and the engine reaches that file through an interface it does not
control.

This chapter is about how that works and why it was built this way.

## Why one file matters

The same request — "let me keep many small databases in object storage and pull
one down when I need it" — has been made of several embedded graph databases. For
most of them it is hard, because a database is a catalog, a set of index files, a
data file, and a log. Serializing means inventing a container format for all of
them and keeping it in step with the engine as both change.

Here, serializing a database is reading its file. That is not a clever trick and
there is no format to maintain; it falls out of the storage layout. It is the
clearest practical argument for the single-file design, which otherwise looks like
a mere convenience.

## Serializing

`serialize` does two things:

```zig
_ = try self.quiesceForCopy();   // fold everything pending into the file
// ...then read the file
```

The first step is the interesting one. At any moment part of a database lives
somewhere other than its file: dirty pages sit in the buffer pool, committed
changes sit in the write-ahead log, and the vector index keeps state in memory
until asked to persist it. Copying the file without settling all of that gives you
something that opens and is then subtly wrong.

So quiescing persists the vector index, writes the tree roots, and runs a `.full`
checkpoint, which flushes dirty pages into the file and leaves the log alone. The
result is a file that needs no log beside it — which is exactly what somebody
uploading to a bucket wants, since they are going to upload one object.

`backup` performs the same step, and the two share it rather than keeping separate
copies that would drift apart.

### Why it refuses during a transaction

`serialize` returns an error if a transaction is open. A copy taken while writes
land underneath it is torn in a way nothing downstream can detect: the bytes are
individually valid, the pages pass their checksums, and the structure is
inconsistent. Failing at the point of the mistake is the only place the problem is
visible.

### Why deserialize validates

Opening a zero-length file is how a database gets created. So bytes that are empty
or truncated, written out and opened, would quietly become a brand new empty
database — and report success. `deserialize` therefore checks the length and magic
number before it does anything with the bytes.

This matters because the obvious failure in this workflow is a partial download.

## Running without files

Every byte the storage layer moves goes through the [VFS](./vfs.md) interface.
Swapping the implementation is therefore enough to give a database with nothing
underneath it, and `:memory:` does exactly that.

Nothing above the interface changes. The B+Tree, buffer pool, log, and recovery
all run as they always do, because none of them can tell the difference. That
sameness is the entire reason the feature was cheap, and it is worth protecting:
the tempting optimizations in this area are mostly ones that would make the
in-memory path structurally different, and each would buy a little memory in
exchange for a second code path to keep correct.

### Why `:memory:` is a path and not an option

Every binding already accepts a path string and passes it through to the C API. A
magic path therefore works everywhere the moment the engine recognises it —
Python, TypeScript, Go, C, and the command line — with no binding changes at all.

An option would have meant a new field, so a new versioned C struct, so a new
entry point, so FFI declarations and public option types and documentation in four
bindings, for exactly the same capability. The cost of the magic string is that it
is a magic string.

Opening `:memory:` also implies creating it, since there is never a previous
in-memory database to find. Requiring `create` would have been a formality every
caller had to remember, which would have undone the point.

### Why the log stays on

Turning the write-ahead log off in memory looks free. A log exists so a crash
cannot lose committed data, and a process holding the only copy of a database in
its own heap loses everything when it dies regardless.

Until 0.17.0 that reasoning did not survive contact with the code: transactions
were built on the log, so a database without one returned
`TransactionsNotEnabled` from `beginTransaction`, with no rollback and no
multi-statement atomicity. That was a much larger hole than the allocations it
saved, so the log stayed on in memory as on disk. A database without a log now
has transactions (see
[Running without the log](../configuration/durability.md#running-without-the-log)),
so `enable_wal = false` is a fair choice for an in-memory database; the default
stays the same for both kinds.

The log becomes a second file in the same memory backend, bounded by automatic
checkpointing.

### Why locks always succeed

A file-backed database takes a lock and refuses a conflicting opener, because two
processes writing one file corrupt it. No other process can reach memory this one
owns, so there is nothing to exclude, and every lock request in the memory backend
succeeds.

That is a decision rather than an omission, which is worth stating plainly given
the file-backed behaviour is the opposite.

## What it costs

```text
   100 nodes | database    240 KB | pool 256 KB | total    628 KB | 2.62x
  1000 nodes | database   1896 KB | pool 256 KB | total   3412 KB | 1.80x
  4000 nodes | database   7416 KB | pool 256 KB | total   7676 KB | 1.04x
 10000 nodes | database  18432 KB | pool 256 KB | total  18692 KB | 1.01x
```

Two things are worth reading out of that table.

The buffer pool is a small constant rather than a fraction of the data, because a
cache stops earning its keep when the storage underneath it is already RAM. See
[Buffer Pool](./buffer-pool.md) for the measurement behind that.

And the overhead at small sizes is not duplication, it is the write-ahead log
living as a second file in the same backend. That is why the ratio falls as the
database grows rather than rising: checkpointing truncates the log, and the fixed
costs stop mattering.

## Loading without a second copy

By default `deserialize` copies the caller's bytes. It can instead point at them,
which halves what loading costs.

The memory backend stores a file as page-sized chunks, and a chunk either owns its
bytes or borrows them:

```zig
const Chunk = union(enum) {
    borrowed: []const u8,
    owned: []u8,
};
```

Writing to a borrowed chunk copies it first. A database loaded this way starts out
borrowing all of itself and only the pages actually modified become private, so
reading a database and editing a little of it keeps one copy of nearly all of it.
The caller's buffer is never written to.

### Why chunks, not one buffer

One growing buffer is the obvious layout and the wrong one. Growing it means
reallocating, reallocating moves every byte, and anything holding a slice from
before that moment is left pointing at freed memory. Lending slices is the whole
point, so stable addresses had to come first.

### Why some languages cannot borrow

Python and TypeScript can: the binding keeps a reference to the buffer on the
wrapper object, and the collector cannot take it.

Go cannot, and this is a language rule rather than a gap in the binding. Its own
documentation says C code may not keep a copy of a Go pointer after the call
returns, and that slices cannot be pinned to work around it. Java cannot either,
for a different reason: pinning a `byte[]` for the lifetime of a database would
hold up the collector for exactly that long.

So there are two entry points and each language uses the one its rules allow. The
copy happens inside the engine, so the bindings that must copy do not implement
anything themselves.

## What was considered and rejected

**Frames borrowing from the memory backend.** The buffer pool could point at pages
in the memory backend for clean pages and copy on first write. This was the
original plan for reducing duplication, and the measurements above closed it: the
pool is a constant 256 KB, so there is nothing material left to save, and the
change would add a new state — "this frame might not own its bytes" — to the
structure that pinning, latching, and dirty tracking all run through, on every read
and write in the engine.

**Making the pool the storage.** For an in-memory database the pool could *be* the
file, with nothing ever evicted since there is nowhere to evict to. That gives
exactly one copy by construction. It is rejected because it makes the in-memory
path structurally different from the file path, and that sameness is why this works
at all.

## Related

- [Virtual File System](./vfs.md) — the interface all of this rests on
- [Buffer Pool](./buffer-pool.md) — why the cache is sized differently here
- [Backup and Replication](../guides/backup-and-replication.md) — how to use it
