# LatticeDB Python Bindings

Python bindings for [LatticeDB](https://github.com/jeffhajewski/latticedb), an embedded knowledge graph database for AI/RAG applications.

## Installation

```bash
pip install latticedb
```

The native shared library (`liblattice.dylib` / `liblattice.so`) must be available on the system. Install it via the [install script](https://github.com/jeffhajewski/latticedb#installation) or build from source with `zig build shared`.

## Quick Start

```python
import numpy as np
from latticedb import Database

with Database("knowledge.db", create=True, enable_vector=True, vector_dimensions=4) as db:
    # Create nodes, edges, and index content
    with db.write() as txn:
        alice = txn.create_node(
            labels=["Person"],
            properties={"name": "Alice", "age": 30},
        )
        bob = txn.create_node(
            labels=["Person"],
            properties={"name": "Bob", "age": 25},
        )
        txn.create_edge(alice.id, bob.id, "KNOWS")

        # Index text for full-text search
        txn.fts_index(alice.id, "Alice works on machine learning research")
        txn.fts_index(bob.id, "Bob studies deep learning and neural networks")

        # Store vector embeddings
        txn.set_vector(alice.id, "embedding", np.array([1.0, 0.0, 0.0, 0.0], dtype=np.float32))
        txn.set_vector(bob.id, "embedding", np.array([0.0, 1.0, 0.0, 0.0], dtype=np.float32))

        txn.commit()

    # Query with Cypher
    result = db.query("MATCH (n:Person) WHERE n.age > 20 RETURN n.name, n.age")
    for row in result:
        print(row)

    # Vector similarity search
    query_vec = np.array([0.9, 0.1, 0.0, 0.0], dtype=np.float32)
    for r in db.vector_search(query_vec, k=2):
        print(f"Node {r.node_id}: distance={r.distance:.4f}")

    # Full-text search
    for r in db.fts_search("machine learning"):
        print(f"Node {r.node_id}: score={r.score:.4f}")

    # Fuzzy search (typo-tolerant)
    for r in db.fts_search_fuzzy("machin lerning"):
        print(f"Node {r.node_id}: score={r.score:.4f}")
```

## API Reference

### Database

```python
Database(
    path: str | Path,
    *,
    create: bool = False,        # Create if doesn't exist
    read_only: bool = False,     # Open in read-only mode
    cache_size_mb: int = 100,    # Page cache size
    enable_vector: bool = False, # Enable vector storage
    vector_dimensions: int = 128 # Vector dimensions
)
```

#### Methods

- `open()` / `close()` - Open/close the database (also works as context manager)
- `read()` - Start a read-only transaction (context manager)
- `write()` - Start a read-write transaction (context manager)
- `query(cypher, parameters=None)` - Execute a Cypher query
- `vector_search(vector, k=10, ef_search=64)` - k-NN vector search
- `fts_search(query, limit=10)` - Full-text search
- `fts_search_fuzzy(query, limit=10, max_distance=0, min_term_length=0)` - Fuzzy full-text search
- `cache_clear()` - Clear the query cache
- `cache_stats()` - Get cache hit/miss statistics

### Transaction

#### Read Operations

- `get_node(node_id)` - Get a node by ID, returns `Node` or `None`
- `node_exists(node_id)` - Check if a node exists
- `get_property(node_id, key)` - Get a property value
- `get_outgoing_edges(node_id)` - Get outgoing edges from a node
- `get_incoming_edges(node_id)` - Get incoming edges to a node
- `is_read_only` / `is_active` - Transaction state

#### Write Operations

- `create_node(labels=[], properties=None)` - Create a node
- `delete_node(node_id)` - Delete a node
- `set_property(node_id, key, value)` - Set a property on a node
- `set_vector(node_id, key, vector)` - Set a vector embedding
- `batch_insert(label, vectors)` - Batch insert nodes with vectors (see below)
- `fts_index(node_id, text)` - Index text for full-text search
- `create_edge(source_id, target_id, edge_type)` - Create an edge
- `delete_edge(source_id, target_id, edge_type)` - Delete an edge
- `commit()` / `rollback()` - Commit or rollback the transaction

### Batch Insert

Insert many nodes with vectors in a single efficient call:

```python
import numpy as np

with Database("vectors.db", create=True, enable_vector=True, vector_dimensions=128) as db:
    with db.write() as txn:
        vectors = np.random.rand(1000, 128).astype(np.float32)
        node_ids = txn.batch_insert("Document", vectors)
        print(f"Created {len(node_ids)} nodes")
        txn.commit()
```

### Full-Text Search

#### Exact Search

```python
results = db.fts_search("machine learning", limit=10)
for r in results:
    print(f"Node {r.node_id}: score={r.score:.4f}")
```

#### Fuzzy Search (Typo-Tolerant)

```python
# Finds "machine learning" even with typos
results = db.fts_search_fuzzy("machne lerning", limit=10)

# Control fuzzy matching sensitivity
results = db.fts_search_fuzzy(
    "machne",
    limit=10,
    max_distance=2,      # Max edit distance (default: 2)
    min_term_length=4,   # Min term length for fuzzy matching (default: 4)
)
```

### Embeddings

LatticeDB includes a built-in hash embedding function and an HTTP client for external embedding services.

#### Hash Embeddings (Built-in)

Deterministic, no external service needed. Useful for testing or simple keyword-based similarity:

```python
from latticedb import hash_embed

vec = hash_embed("hello world", dimensions=128)
print(vec.shape)  # (128,)
```

#### HTTP Embedding Client

Connect to Ollama, OpenAI, or compatible APIs:

```python
from latticedb import EmbeddingClient, EmbeddingApiFormat

# Ollama (default)
with EmbeddingClient("http://localhost:11434") as client:
    vec = client.embed("hello world")

# OpenAI-compatible API
with EmbeddingClient(
    "https://api.openai.com/v1",
    model="text-embedding-3-small",
    api_format=EmbeddingApiFormat.OPENAI,
    api_key="sk-...",
) as client:
    vec = client.embed("hello world")
```

### Edge Traversal

```python
with db.read() as txn:
    outgoing = txn.get_outgoing_edges(node_id)
    for edge in outgoing:
        print(f"{edge.source_id} --[{edge.edge_type}]--> {edge.target_id}")

    incoming = txn.get_incoming_edges(node_id)
    for edge in incoming:
        print(f"{edge.source_id} --[{edge.edge_type}]--> {edge.target_id}")
```

### Cypher Queries

```python
# Pattern matching
result = db.query("MATCH (n:Person) RETURN n.name")

# With parameters
result = db.query(
    "MATCH (n:Person) WHERE n.name = $name RETURN n",
    parameters={"name": "Alice"},
)

# Vector similarity in Cypher
result = db.query(
    "MATCH (n:Document) WHERE n.embedding <=> $vec < 0.5 RETURN n.title",
    parameters={"vec": query_vector},
)

# Full-text search in Cypher
result = db.query(
    'MATCH (n:Document) WHERE n.content @@ "machine learning" RETURN n.title'
)

# Data mutation
db.query("CREATE (n:Person {name: 'Charlie', age: 35})")
db.query("MATCH (n:Person {name: 'Charlie'}) SET n.age = 36")
db.query("MATCH (n:Person {name: 'Charlie'}) DETACH DELETE n")
```

### Query Cache

```python
# Get cache statistics
stats = db.cache_stats()
print(f"Entries: {stats['entries']}, Hits: {stats['hits']}, Misses: {stats['misses']}")

# Clear the cache
db.cache_clear()
```

## Supported Property Types

- `None` - Null value
- `bool` - Boolean
- `int` - 64-bit integer
- `float` - 64-bit float
- `str` - UTF-8 string
- `bytes` - Binary data

## Error Handling

```python
from latticedb import LatticeError, LatticeNotFoundError, LatticeIOError

try:
    with Database("nonexistent.db") as db:
        pass
except LatticeNotFoundError:
    print("Database not found")
except LatticeIOError:
    print("I/O error")
except LatticeError as e:
    print(f"Error: {e}")
```

## Requirements

- Python 3.9+
- NumPy (for vector operations)
- The native LatticeDB library (`liblattice.dylib` / `liblattice.so`)

## License

MIT
