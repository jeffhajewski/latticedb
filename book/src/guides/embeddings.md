# Working with Embeddings

LatticeDB supports storing and searching vector embeddings on nodes. This guide covers the embedding options and how to use them.

## Overview

To use embeddings:

1. Enable vector storage when opening the database
2. Generate embeddings (built-in hash or external service)
3. Attach embeddings to nodes
4. Search by similarity

## Enabling Vector Storage

```python
db = Database(
    "mydb.db",
    create=True,
    enable_vector=True,
    vector_dimensions=128,  # Must match your embedding dimensions
)
```

```typescript
const db = new Database("mydb.db", {
  create: true,
  enableVector: true,
  vectorDimensions: 128,
});
```

## Hash Embeddings (Built-in)

`hash_embed` generates deterministic embeddings from text without an external service. It uses feature hashing to map text tokens to a fixed-dimension vector.

**When to use:** Testing, prototyping, or when keyword-level similarity is sufficient.

**Python:**
```python
from latticedb import hash_embed

vec = hash_embed("hello world", dimensions=128)
# Returns a numpy array of shape (128,)
```

**TypeScript:**
```typescript
import { hashEmbed } from "@hajewski/latticedb";

const vec = hashEmbed("hello world", 128);
// Returns a Float32Array of length 128
```

## HTTP Embedding Client

For production-quality semantic embeddings, use the HTTP client to connect to an embedding service.

### Ollama

```python
from latticedb import EmbeddingClient

with EmbeddingClient("http://localhost:11434") as client:
    vec = client.embed("hello world")
```

```typescript
import { EmbeddingClient } from "@hajewski/latticedb";

const client = new EmbeddingClient({
  endpoint: "http://localhost:11434",
});
const vec = client.embed("hello world");
client.close();
```

### OpenAI

```python
from latticedb import EmbeddingClient, EmbeddingApiFormat

with EmbeddingClient(
    "https://api.openai.com/v1",
    model="text-embedding-3-small",
    api_format=EmbeddingApiFormat.OPENAI,
    api_key="sk-...",
) as client:
    vec = client.embed("hello world")
```

```typescript
import { EmbeddingClient, EmbeddingApiFormat } from "@hajewski/latticedb";

const client = new EmbeddingClient({
  endpoint: "https://api.openai.com/v1",
  model: "text-embedding-3-small",
  apiFormat: EmbeddingApiFormat.OpenAI,
  apiKey: "sk-...",
});
const vec = client.embed("hello world");
client.close();
```

## Storing Embeddings

Attach a vector to a node within a write transaction:

```python
with db.write() as txn:
    node = txn.create_node(labels=["Chunk"], properties={"text": "some text"})
    embedding = hash_embed("some text", dimensions=128)
    txn.set_vector(node.id, "embedding", embedding)
    txn.commit()
```

## Searching

### Programmatic API

```python
query_vec = hash_embed("search query", dimensions=128)
results = db.vector_search(query_vec, k=10, ef_search=64)
for r in results:
    print(f"Node {r.node_id}: distance={r.distance:.4f}")
```

### Cypher

```cypher
MATCH (n:Chunk)
WHERE n.embedding <=> $query < 0.5
RETURN n.text
ORDER BY n.embedding <=> $query
LIMIT 10
```

## Batch Insert

For bulk loading, use `batch_insert` which is significantly faster than inserting one at a time:

```python
import numpy as np

with db.write() as txn:
    vectors = np.random.rand(10000, 128).astype(np.float32)
    node_ids = txn.batch_insert("Document", vectors)
    txn.commit()
```

## Choosing Dimensions

The `vector_dimensions` parameter must be set when opening the database and must match the embedding model output:

| Model | Dimensions |
|-------|-----------|
| `hash_embed` (built-in) | Configurable (default 128) |
| `text-embedding-3-small` (OpenAI) | 1536 |
| `text-embedding-3-large` (OpenAI) | 3072 |
| `nomic-embed-text` (Ollama) | 768 |
| `mxbai-embed-large` (Ollama) | 1024 |

Higher dimensions capture more semantic nuance but use more memory and slow down search.
