# Client API Migration Notes

This page records preferred client names after the recent binding-surface
cleanup. Deprecated compatibility aliases remain available for now, but new code
should use the names below.

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

Preferred manual transaction entrypoints:

- `BeginRead()`
- `BeginWrite()`

Compatibility alias:

- `Begin(readOnly bool)`

## Stable Edge IDs

All clients should treat the edge ID returned by edge creation or traversal as
the handle for edge properties. Source/target/type deletion remains available
for simple graph operations, while property APIs operate by stable edge ID.
