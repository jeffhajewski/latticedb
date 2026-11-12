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
from latticedb import Database

db = Database(
    "mydb.db",
    create=True,
    enable_vectors=True,
    vector_dimensions=128,  # Must match your embedding dimensions
)
db.open()
```

```typescript
const db = new Database("mydb.db", {
  create: true,
  enableVectors: true,
  vectorDimensions: 128,
});
await db.open();
```

A node holds one vector, and Cypher reads it as `n.embedding`. `set_vector` takes
a key argument, which must be `"embedding"` (an empty key also works); any other
key is rejected with an unsupported-operation error rather than stored under that
name. Setting a vector on a node that already has one replaces it.

Every component must be a finite number: a vector containing NaN or infinity is
rejected with an invalid-argument error, both when stored and when used as a
search query. Under the cosine metric the database keeps only the normalised
(unit-length) copy of a vector, so a committed vector reads back normalised.

## Hash Embeddings (Built-in)

`hash_embed` generates deterministic embeddings from text without an external service. It uses feature hashing to map text tokens to a fixed-dimension vector.

**When to use:** Testing, prototyping, or when keyword-level similarity is sufficient.

**Python:**
```python
from latticedb.embedding import hash_embed

vec = hash_embed("hello world", dimensions=128)
# Returns a numpy array of shape (128,)
```

**TypeScript:**
```typescript
import { hashEmbed } from "@hajewski/latticedb/embedding";

const vec = hashEmbed("hello world", 128);
// Returns a Float32Array of length 128
```

## HTTP Embedding Client

For production-quality semantic embeddings, use the HTTP client to connect to an embedding service.

The endpoint is the full URL the client POSTs to; nothing is appended to it. Give
Ollama's `/api/embeddings` path or OpenAI's `/v1/embeddings` path, not the
server's base URL: given `http://localhost:11434` alone, the client POSTs to `/`
on that server, which does not serve embeddings, and `embed` fails. The client
copies the endpoint, model and API key when it is created.

The timeout setting (`timeout_ms` in Python, `timeoutMs` in TypeScript, 30
seconds by default) is how long the client waits for the server to start
answering. A request that gets no response in that time fails with an I/O
error. Connecting, and reading the rest of a response that has begun, are not
bounded by it, and it is not applied on Windows.

### Ollama

```python
from latticedb.embedding import EmbeddingClient

with EmbeddingClient("http://localhost:11434/api/embeddings") as client:
    vec = client.embed("hello world")
```

```typescript
import { EmbeddingClient } from "@hajewski/latticedb/embedding";

const client = new EmbeddingClient({
  endpoint: "http://localhost:11434/api/embeddings",
});
const vec = client.embed("hello world");
client.close();
```

### OpenAI

```python
from latticedb.embedding import EmbeddingClient, EmbeddingApiFormat

with EmbeddingClient(
    "https://api.openai.com/v1/embeddings",
    model="text-embedding-3-small",
    api_format=EmbeddingApiFormat.OPENAI,
    api_key="sk-...",
) as client:
    vec = client.embed("hello world")
```

```typescript
import { EmbeddingClient, EmbeddingApiFormat } from "@hajewski/latticedb/embedding";

const client = new EmbeddingClient({
  endpoint: "https://api.openai.com/v1/embeddings",
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
from latticedb.embedding import hash_embed

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

In Cypher, `<=>` in `WHERE` first takes the 100 nearest vectors in the whole
database, then keeps the rows of your pattern whose node is among them. The label
in the pattern does not narrow the search, so a threshold query returns at most 100
rows and can miss matches when nodes of other labels are nearer. For an exact top-k
over the whole database, use `vector_search`. See
[Vector Search in Cypher](../cypher/vector-search.md).

## Batch Insert

For bulk loading, `batch_insert_vectors` creates a node and stores its vector for
every row in one call. It saves one binding call per node; each vector still goes
into the HNSW index individually:

```python
import numpy as np

with db.write() as txn:
    vectors = np.random.rand(10000, 128).astype(np.float32)
    node_ids = txn.batch_insert_vectors("Document", vectors)
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
