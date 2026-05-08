# Durable Streams and Graph Changefeeds

LatticeDB includes durable named streams for append-only event records and a
built-in graph changefeed exposed as the reserved `__lattice_changes` stream.

Streams are stored in internal system B+Trees, not as user graph data. Stream
records, committed consumer offsets, trims, and graph mutations are written
through the same transaction/WAL path, so a committed graph write and its
changefeed records become visible atomically.

## Stream Model

- Stream names auto-create on first publish.
- Sequence numbers are per stream, start at `1`, and are read in ascending order.
- Each record has a `sequence`, `kind`, and typed `lattice_value` payload.
- The default record kind is `message`.
- Reads are cursor-based: pass `after_sequence` to get later records.
- Reads do not acknowledge records.
- Consumer offsets are explicit durable values written in a write transaction.
- `trim_stream(stream, through_sequence)` deletes records through a sequence.
- User publishes to names beginning with `__lattice_` are rejected.

Names, kinds, and consumer names must be non-empty UTF-8 and at most 255 bytes.
Read limits must be greater than zero.

## Python

```python
from latticedb import Database

with Database("events.lattice", create=True) as db:
    with db.write() as txn:
        txn.publish_stream("jobs", {"id": 1, "status": "queued"}, kind="job.queued")
        txn.commit()

    records = db.read_stream("jobs", after_sequence=0, limit=100)
    for record in records:
        print(record.sequence, record.kind, record.payload)

    with db.write() as txn:
        txn.set_stream_offset("jobs", "worker-a", records[-1].sequence)
        txn.trim_stream("jobs", records[-1].sequence - 1)
        txn.commit()

    offset = db.get_stream_offset("jobs", "worker-a")
```

`Database.changes(after_sequence=0, limit=100, timeout_ms=0)` is a convenience
reader for the graph changefeed.

## TypeScript

```typescript
import { Database } from "@hajewski/latticedb";

const db = new Database("events.lattice", { create: true });
await db.open();

await db.write(async (txn) => {
  txn.publishStream("jobs", { id: 1, status: "queued" }, "job.queued");
});

const records = await db.readStream("jobs", {
  afterSequence: 0n,
  limit: 100,
});

await db.write(async (txn) => {
  txn.setStreamOffset("jobs", "worker-a", records.at(-1)!.sequence);
  txn.trimStream("jobs", records.at(-1)!.sequence - 1n);
});

const offset = await db.getStreamOffset("jobs", "worker-a");
await db.close();
```

`db.changes({ afterSequence, limit, timeoutMs })` reads the graph changefeed.

## C API

The stable C ABI exposes the same operations:

- `lattice_stream_publish`
- `lattice_stream_read`
- `lattice_stream_batch_count`
- `lattice_stream_batch_get`
- `lattice_stream_batch_free`
- `lattice_stream_get_offset`
- `lattice_stream_set_offset`
- `lattice_stream_trim`

Batch records borrow `kind` and payload pointers from the batch handle. They
remain valid until `lattice_stream_batch_free`.

## Graph Changefeed

The reserved `__lattice_changes` stream is populated from semantic graph
transaction diffs. It is not generated from low-level page writes.

Event kinds:

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

Payloads are `lattice_value` maps with stable keys. Depending on event kind,
the map can include:

- `entity`
- `op`
- `node_id`
- `edge_id`
- `source_id`
- `target_id`
- `type`
- `label`
- `key`
- `old_value`
- `new_value`

For very large property changes, `old_value` and/or `new_value` may be a
summary map instead of the original value so the built-in changefeed does not
make the graph write exceed internal record-size limits. Summary maps include
`__lattice_value_omitted`, `type`, and `encoded_bytes`.

Example Python reader:

```python
last_seen = 0

while True:
    changes = db.changes(after_sequence=last_seen, limit=100, timeout_ms=5000)
    for change in changes:
        last_seen = change.sequence
        print(change.kind, change.payload)
```

## V1 Scope

The V1 stream surface is intentionally small. It provides durable named streams,
manual trimming, explicit durable offsets, and same-process read wakeups. It
does not implement task queues, retry policy, dead-lettering, or cross-process
notification.
