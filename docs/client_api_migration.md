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

## Other Behaviour Changes Since 0.16.0

These change what existing client code sees without renaming anything. The
release notes list them in full.

- A database opened without the log (`enable_wal=False`, `enableWal: false`,
  `DisableWAL`, `enableWal(false)`) has write transactions; they used to fail
  with `LATTICE_ERROR_UNSUPPORTED` (-14). A stream write on a read transaction
  there now fails with `LATTICE_ERROR_READ_ONLY` (-9).
- A Cypher query without `RETURN` returns no columns and no rows. A bare
  `CREATE` returned columns named after its variables or `col0`, `col1`, ...;
  add a `RETURN` to get values back.
- A file written by a newer release is refused with
  `LATTICE_ERROR_VERSION_MISMATCH` (-11), where it was an I/O error (-2).
- `sync_mode`, `full_fsync` and the cache size set in code win over the
  `LATTICE_SYNC_MODE`, `LATTICE_FULL_FSYNC` and `LATTICE_BUFFER_POOL_MB`
  environment variables, which now apply only when the option is left unset.
- A commit that fails with `LATTICE_ERROR_TXN_ABORTED` (-7) has ended the
  transaction in every binding; rolling it back afterwards does nothing.
- `changes()` and its equivalents fail with the binding's "changes trimmed"
  error (`LATTICE_ERROR_NOT_FOUND`, -4) when the changefeed's retention policy
  has dropped the events a reader asks for, and with
  `LATTICE_ERROR_UNSUPPORTED` (-14) when the policy is `off`. Events within a
  transaction come in a fixed order: the edges it deletes, the nodes that exist
  after it, the edges that exist after it, then the nodes it deletes.
- TypeScript exports `LatticeErrorCode` from the package root, and it and
  `QueryErrorStage` are plain enums.
