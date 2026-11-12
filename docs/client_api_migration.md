# Client API Migration Notes

This page records preferred client names after the binding-surface cleanup in
0.5. Deprecated compatibility aliases remain available, but new code should use
the names below. No removal version has been set; the deprecation messages in the
bindings still say "Earliest removal is v0.6.0", which has passed.

## Python

Preferred embedding imports:

```python
from latticedb.embedding import hash_embed, EmbeddingClient, EmbeddingApiFormat
```

Compatibility aliases still exported from `latticedb`:

- `hash_embed`
- `EmbeddingClient`
- `EmbeddingApiFormat`

Preferred database option:

- `enable_vectors`

Compatibility alias:

- `enable_vector`

Preferred bulk vector method:

- `Transaction.batch_insert_vectors(label, vectors)`

Compatibility alias:

- `Transaction.batch_insert(label, vectors)`

## TypeScript

Preferred embedding imports:

```typescript
import { hashEmbed, EmbeddingClient, EmbeddingApiFormat } from "@hajewski/latticedb/embedding";
```

Compatibility aliases still exported from `@hajewski/latticedb`:

- `hashEmbed`
- `EmbeddingClient`
- `EmbeddingApiFormat`

Preferred database option:

- `enableVectors`

Compatibility alias:

- `enableVector`

Preferred bulk vector method:

- `txn.batchInsertVectors(label, vectors)`

Compatibility alias:

- `txn.batchInsert(label, vectors)`

## Go

Preferred embedding import:

```go
import latticeembedding "github.com/jeffhajewski/latticedb/bindings/go/embedding"
```

Compatibility aliases still exported from `latticedb`:

- `HashEmbed`
- `EmbeddingClient`, `NewEmbeddingClient`, `EmbeddingConfig`
- `EmbeddingAPIFormat`

Preferred database option:

- `OpenOptions.EnableVectors`

Compatibility alias:

- `OpenOptions.EnableVector`

Preferred bulk vector method:

- `tx.BatchInsertVectors(label, vectors)`

Compatibility alias:

- `tx.BatchInsert(label, vectors)`

Preferred manual transaction entrypoints:

- `BeginRead()`
- `BeginWrite()`

Compatibility alias:

- `Begin(readOnly bool)`

## Stable Edge IDs

All clients should treat the edge ID returned by edge creation or traversal as
the handle for edge properties. Source/target/type deletion remains available
for simple graph operations, while property APIs operate by stable edge ID.

## Node Deletion

Since the release after 0.16.0, a plain node delete refuses a node that still
has relationships, as Cypher's `DELETE` does, and deletes nothing; it used to
delete the node and leave its relationships pointing at nothing. Code that
deleted nodes with relationships should call the detach form, which deletes the
relationships with the node:

| Client | Plain delete (refused while relationships exist) | Detach delete |
|--------|-----------------------------------------------|---------------|
| C | `lattice_node_delete` returns `LATTICE_ERROR_HAS_RELATIONSHIPS` (-17) | `lattice_node_detach_delete` |
| Python | `txn.delete_node(id)` raises `LatticeHasRelationshipsError` | `txn.delete_node(id, detach=True)` |
| TypeScript | `txn.deleteNode(id)` throws a `LatticeError` with code -17 | `txn.deleteNode(id, { detach: true })` |
| Go | `tx.DeleteNode(id)` returns `ErrorHasRelationships` | `tx.DetachDeleteNode(id)` |
| Java | `txn.deleteNode(id)` throws `ErrorCode.HAS_RELATIONSHIPS` | `txn.detachDeleteNode(id)` |

Creating an edge whose source or target does not exist now fails with
`LATTICE_ERROR_NOT_FOUND` (-4) in every client.

## Vector Options on Reopen

A database file now records its vector index (dimensions, metric and HNSW
parameters), and a database that has one opens with it without any vector
options. Code that passed `enable_vectors` and the dimensions at every open can
stop doing so; if it keeps passing them, they must match the file, or the open
fails with `LATTICE_ERROR_CONFIG_MISMATCH` (-18).

