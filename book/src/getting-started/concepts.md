# Core Concepts

## Property Graph

LatticeDB stores data as a **property graph**: a collection of nodes connected by edges, where both nodes and edges can have labels and key-value properties.

### Nodes

A node represents an entity. Each node has:

- A unique **ID** (assigned automatically)
- One or more **labels** that categorize it (e.g., `Person`, `Document`, `Chunk`)
- Zero or more **properties** — key-value pairs (e.g., `name: "Alice"`, `age: 30`)

```python
alice = txn.create_node(
    labels=["Person"],
    properties={"name": "Alice", "age": 30}
)
```

### Edges

An edge represents a relationship between two nodes. Each edge has:

- A **source** node and a **target** node
- An **edge type** (e.g., `KNOWS`, `AUTHORED_BY`, `PART_OF`)
- A direction — edges always point from source to target

```python
txn.create_edge(alice.id, bob.id, "KNOWS")
```

### Properties

Properties are typed key-value pairs attached to nodes. Supported types:

| Type | Python | TypeScript | C |
|------|--------|------------|---|
| Null | `None` | `null` | `LATTICE_VALUE_NULL` |
| Boolean | `bool` | `boolean` | `LATTICE_VALUE_BOOL` |
| Integer | `int` | `number` | `LATTICE_VALUE_INT` |
| Float | `float` | `number` | `LATTICE_VALUE_FLOAT` |
| String | `str` | `string` | `LATTICE_VALUE_STRING` |
| Binary | `bytes` | `Uint8Array` | `LATTICE_VALUE_BYTES` |

## Cypher Query Language

LatticeDB uses [Cypher](../cypher/overview.md) — a declarative graph query language. Nodes are written in parentheses, edges in square brackets:

```cypher
MATCH (person:Person)-[:KNOWS]->(friend:Person)
WHERE person.name = "Alice"
RETURN friend.name
```

LatticeDB extends Cypher with two operators:

- **`<=>`** — vector distance for similarity search
- **`@@`** — full-text search with BM25 scoring

## Vector Search

LatticeDB includes an HNSW (Hierarchical Navigable Small World) index for approximate nearest-neighbor search on vector embeddings.

To use vector search:

1. Enable vector storage when opening the database
2. Attach vector embeddings to nodes
3. Search by similarity using `vector_search()` or the `<=>` operator in Cypher

```python
# Store an embedding
txn.set_vector(node.id, "embedding", vector)

# Search by similarity
results = db.vector_search(query_vector, k=10)

# Or in Cypher
db.query("MATCH (n:Chunk) WHERE n.embedding <=> $q < 0.5 RETURN n", parameters={"q": query_vector})
```

### Embeddings

LatticeDB provides two ways to generate embeddings:

- **`hash_embed`** — a built-in deterministic hash function. No external service needed. Useful for testing and simple keyword-based similarity.
- **`EmbeddingClient`** — an HTTP client that connects to Ollama, OpenAI, or any compatible API for production-quality embeddings.

## Full-Text Search

LatticeDB includes a BM25-scored inverted index for full-text search. Index text content on nodes, then search across all indexed text.

```python
# Declare an index over the property holding the text
db.create_node_fts_index("Chunk", "text")

# Writing that property is what indexes it
txn.set_property(node.id, "text", "The transformer architecture uses self-attention")

# Search
results = db.fts_search("Chunk", "text", "transformer attention")

# Fuzzy search (typo-tolerant)
results = db.fts_search_fuzzy("Chunk", "text", "transformr atention")

# Or in Cypher
db.query('MATCH (n) WHERE n.text @@ "transformer attention" RETURN n')
```

## Transactions

All operations in LatticeDB happen within transactions. LatticeDB uses **snapshot isolation**: each transaction sees a consistent snapshot of the database as of when it started.

- **Read transactions** can run concurrently — readers never block other readers.
- **Write transactions** are serialized — only one write transaction can commit at a time.

```python
# Read transaction
with db.read() as txn:
    node = txn.get_node(node_id)

# Write transaction
with db.write() as txn:
    txn.create_node(labels=["Person"], properties={"name": "Alice"})
    txn.commit()
```

If a write transaction is not explicitly committed, it is rolled back when the context exits.

## Single-File Storage

The entire database — nodes, edges, properties, vector index, and text index —
lives in a single file, with the write-ahead log beside it while the database is
open. That makes a database easy to move: back it up or deploy it by moving one
file.

It also makes it easy to move without a file at all. Because a database is one
file, serializing it is reading that file, so you can hand a whole database around
as bytes and open it again elsewhere:

```python
blob = db.serialize()
db2 = latticedb.deserialize(blob)
```

That is what makes it practical to keep a database per case or per tenant in
object storage, pulling one down when you need it.

One caveat worth learning early: **do not copy the file while the database is
open.** The log holds committed changes the main file does not have yet, so a copy
taken then is broken in a way you only discover when you try to use it. Use
`lattice backup`, or stop the process first.

## Databases Without Files

A database does not have to be on disk at all. Open `:memory:` and it lives
entirely in memory:

```python
db = latticedb.Database(":memory:")
```

Everything works the same — transactions, indexes, vector and text search — and it
disappears when you close it. It is the quickest way to try something, the usual
choice for tests, and what a database loaded from bytes runs on.

See [In-Memory Databases](../configuration/in-memory.md).
