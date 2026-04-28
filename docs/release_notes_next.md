# LatticeDB 0.6.0 Release Notes

## Summary

`0.6.0` adds durable named streams and a built-in graph changefeed, extends the
public C, Python, and TypeScript APIs around that event surface, and tightens
transaction/WAL recovery behavior so stream records commit atomically with graph
writes.

This is a feature release after `0.5.0`. The biggest new capability is that an
embedded application can now persist application events and graph mutation
events inside the same local database transaction that updates the graph.

## Highlights

### Durable named streams

LatticeDB now has internal stream storage backed by system B+Trees rather than
user graph data.

Stream behavior:

- stream names auto-create on first publish
- sequence numbers are per stream, start at `1`, and read in ascending order
- records contain `sequence`, `kind`, and typed `lattice_value` payloads
- the default record kind is `message`
- reads are cursor-based and do not auto-acknowledge records
- consumer offsets are explicit durable values written in write transactions
- `trim_stream(stream, through_sequence)` manually deletes records through a
  sequence

Streams are intentionally not a task queue in this first version. There are no
leases, retries, dead-letter queues, or cross-process notifications.

### Built-in graph changefeed

The reserved `__lattice_changes` stream records semantic graph mutations from
committed transactions.

Changefeed event kinds:

- `node.insert`
- `node.delete`
- `node.label_add`
- `node.label_remove`
- `node.property_set`
- `node.property_remove`
- `edge.insert`
- `edge.delete`
- `edge.property_set`
- `edge.property_remove`

Payloads are typed `lattice_value` maps with stable keys such as `entity`, `op`,
`node_id`, `edge_id`, `source_id`, `target_id`, `type`, `label`, `key`,
`old_value`, and `new_value`.

User publishes to streams beginning with `__lattice_` are rejected so system
streams remain reserved.

### Transactional durability and recovery

Stream appends, offset writes, trims, and graph changes now move through the same
transaction and WAL path.

Operational behavior:

- stream records become visible only after the surrounding transaction commits
- graph writes and their changefeed records become visible atomically
- committed stream records, offsets, and trims replay during recovery
- uncommitted or aborted stream and changefeed records remain invisible after
  recovery
- same-process readers can wait for newly committed stream records

Existing databases get stream system trees lazily on read-write open. Read-only
opens of old databases without stream roots return empty stream reads and reject
stream writes.

### Public API additions

The C API now exposes:

- `lattice_stream_publish`
- `lattice_stream_read`
- `lattice_stream_batch_count`
- `lattice_stream_batch_get`
- `lattice_stream_batch_free`
- `lattice_stream_get_offset`
- `lattice_stream_set_offset`
- `lattice_stream_trim`

C stream batch records borrow `kind` and payload pointers from the batch handle.
They remain valid until `lattice_stream_batch_free`.

Python now exposes:

- `Transaction.publish_stream`
- `Transaction.set_stream_offset`
- `Transaction.trim_stream`
- `Database.read_stream`
- `Database.get_stream_offset`
- `Database.changes`

TypeScript now exposes:

- `Transaction.publishStream`
- `Transaction.setStreamOffset`
- `Transaction.trimStream`
- `Database.readStream`
- `Database.getStreamOffset`
- `Database.changes`

Names, kinds, and consumer names must be non-empty UTF-8 and at most 255 bytes.
Read limits must be greater than zero.

### Storage and correctness fixes

This release also includes storage and query-path hardening that supported the
streams work:

- overlay transaction changes are logged to WAL at commit time
- reads and query execution are threaded through transaction context
- traversal reads are decoupled from edge payload deserialization
- BTree ownership and node-property decoding paths were fixed
- adjacency-only edge counting now uses edge refs
- export behavior was tightened for dangling edges, unreadable graph data,
  unlabeled nodes, and comma-separated label filters

### Documentation and website

New and updated docs cover the stream model, graph changefeed payloads, and the
new C/Python/TypeScript API surface:

- [Durable Streams and Graph Changefeeds](14_durable_streams.md)
- [C API Reference](../book/src/api/c.md)
- [Python API Reference](../bindings/python/README.md)
- [TypeScript API Reference](../bindings/typescript/README.md)

The website now calls out durable streams and graph changefeeds as part of the
main feature set.

## Upgrade Notes

### Databases

The database file format remains `2`, which was introduced by `0.5.0` for FTS
posting pages. `0.6.0` does not add a separate file-format bump for streams.

Existing databases should open normally. Stream system trees are created lazily
when an existing database is opened read-write. Back up production databases
before upgrading if you may need to return to an older binary.

Do not rely on older binaries to recover a database with `0.6.0` stream WAL
records.

### Bindings

Rebuild C consumers and language bindings against the `0.6.0` headers and
shared library if you use the new stream API.

Python and TypeScript packages include the new stream wrappers. The Go binding
does not yet expose ergonomic stream wrappers; use the C ABI directly if you
need streams from Go in this release.

Deprecated Python, TypeScript, and Go compatibility aliases remain available in
`0.6.0`; this release does not remove them.

### V1 stream scope

Durable streams provide local append-only records, explicit offsets, manual
trimming, and same-process read wakeups. They do not provide distributed queue
coordination, retry policy, leases, dead-lettering, or cross-process
LISTEN/NOTIFY-style delivery.

## Validation Notes

Release-surface validation for this cycle included:

```bash
zig build test
zig build integration-test
zig build crash-test
zig build shared
cd bindings/python && uv run pytest tests
cd bindings/typescript && npm test
tests/container/run-all.sh ubuntu-22.04
```

The container run used freshly rebuilt native artifacts and covered the package
install smoke path for the supported Linux container target.
