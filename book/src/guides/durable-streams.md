# Durable Streams and Graph Changefeeds

LatticeDB includes durable named streams for append-only event records and a
built-in graph changefeed exposed as the reserved `__lattice_changes` stream.
Streams use the same transaction and WAL path as graph writes, so records become
visible only after commit.

## Stream Model

- Stream names auto-create on first publish.
- Sequence numbers are per stream, start at `1`, and read in ascending order.
- Records contain `sequence`, `kind`, and a typed payload.
- Reads are cursor-based and do not automatically acknowledge records.
- Consumer offsets are explicit durable values.
- `trim_stream` / `trimStream` deletes records through a sequence.

Streams are local durable logs, not a distributed task queue. They do not provide
leases, retries, dead-letter queues, or cross-process notifications.

## Python

```python
with db.write() as txn:
    txn.publish_stream("jobs", {"id": 1}, kind="job.queued")
    txn.commit()

records = db.read_stream("jobs", after_sequence=0, limit=100)
```

## TypeScript

```typescript
await db.write(async (txn) => {
  await txn.publishStream("jobs", { id: 1 }, "job.queued");
});

const records = await db.readStream("jobs", { afterSequence: 0, limit: 100 });
```

## Graph Changefeed

The reserved `__lattice_changes` stream records semantic graph mutations from
committed transactions.

Changefeed event kinds include:

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

Payload maps include stable keys such as `entity`, `op`, `node_id`, `edge_id`,
`source_id`, `target_id`, `type`, `label`, `key`, `old_value`, and `new_value`.
For very large property changes, `old_value` or `new_value` may be a summary map
with `__lattice_value_omitted`, `type`, and `encoded_bytes`.

Python exposes `Database.changes(...)`; TypeScript exposes `db.changes(...)`.
