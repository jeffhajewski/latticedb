# LatticeDB

**The Embedded Knowledge Graph Database**

LatticeDB is a single-file, embeddable knowledge graph database designed for AI and RAG applications. It combines property graph storage, vector similarity search, and full-text search in one lightweight package.

## Install

```bash
curl -fsSL https://raw.githubusercontent.com/jeffhajewski/latticedb/main/dist/install.sh | bash
```

## Quick Example

```bash
$ lattice mydb.db
LatticeDB v0.1.0
Connected to: mydb.db

lattice> CREATE (p:Person {name: "Alice", age: 30})
Created 1 node

lattice> MATCH (p:Person) RETURN p.name, p.age
┌─────────┬───────┐
│ p.name  │ p.age │
├─────────┼───────┤
│ "Alice" │ 30    │
└─────────┴───────┘
```

```cypher
-- Find documents similar to a query, then traverse to related concepts
MATCH (chunk:Chunk)-[:PART_OF]->(doc:Document)
WHERE chunk.embedding <=> $query_vector < 0.3
MATCH (chunk)-[:MENTIONS]->(concept:Concept)
RETURN doc.title, chunk.text, collect(concept.name) AS concepts
```

## Features

- **Property Graph** — Nodes and edges with labels and arbitrary properties
- **Vector Search** — HNSW-based approximate nearest neighbor search for embeddings
- **Full-Text Search** — BM25-ranked search with tokenization and stemming
- **Cypher Queries** — Pattern matching with extensions for vector (`<=>`) and text (`@@`) search
- **ACID Transactions** — Crash recovery with write-ahead logging
- **Single File** — Your entire database is one portable file
- **Embeddable** — Links as a static library with a clean C API
- **Zero Configuration** — Open a file and start working

## Use Cases

- **RAG Systems** — Vector search finds relevant chunks, graph traversal gathers context
- **Knowledge Graphs** — Linked notes and documents with semantic search
- **AI Agents** — Persistent memory with relationship awareness
- **Local Development** — Lightweight alternative to Neo4j or Weaviate for testing

## Quick Start

### Python

```python
import lattice

# Open or create a database
db = lattice.open("knowledge.db")

with db.transaction() as tx:
    # Create nodes
    doc = tx.create_node(["Document"], {
        "title": "Introduction to Graph Databases",
        "author": "Jane Smith"
    })

    chunk = tx.create_node(["Chunk"], {
        "text": "Graph databases store data as nodes and edges...",
        "embedding": embedding_model.encode("Graph databases store...")
    })

    # Create relationship
    tx.create_edge(chunk, doc, "PART_OF")

# Query with Cypher
results = db.query("""
    MATCH (c:Chunk)-[:PART_OF]->(d:Document)
    WHERE c.embedding <=> $query < 0.5
    RETURN d.title, c.text
    LIMIT 10
""", {"query": query_embedding})

for row in results:
    print(f"{row['d.title']}: {row['c.text'][:100]}...")
```

### TypeScript

```typescript
import { Database } from 'lattice';

const db = await Database.open('knowledge.db');

await db.transaction(async (tx) => {
  // Create nodes
  const doc = await tx.createNode(['Document'], {
    title: 'Introduction to Graph Databases',
    author: 'Jane Smith'
  });

  const chunk = await tx.createNode(['Chunk'], {
    text: 'Graph databases store data as nodes and edges...',
    embedding: new Float32Array(await embed('Graph databases store...'))
  });

  // Create relationship
  await tx.createEdge(chunk, doc, 'PART_OF');
});

// Query with Cypher
const results = await db.query(`
  MATCH (c:Chunk)-[:PART_OF]->(d:Document)
  WHERE c.embedding <=> $query < 0.5
  RETURN d.title, c.text
  LIMIT 10
`, { query: queryEmbedding });
```

### C

```c
#include <lattice.h>

int main() {
    lattice_db *db;
    lattice_open("knowledge.db", 0, &db);

    lattice_txn *txn;
    lattice_txn_begin(db, LATTICE_TXN_READWRITE, &txn);

    // Create a node
    lattice_node_id doc;
    const char *labels[] = {"Document"};
    lattice_node_create(txn, labels, 1, &doc);
    lattice_node_set_string(txn, doc, "title", "Introduction to Graph Databases");

    lattice_txn_commit(txn);
    lattice_close(db);
    return 0;
}
```

## Building from Source

Lattice is written in Zig for performance and portability.

```bash
# Build the library
zig build

# Run tests
zig build test

# Build in release mode
zig build -Doptimize=ReleaseFast
```

### Build Outputs

| Output | Description |
|--------|-------------|
| `zig-out/lib/liblattice.a` | Static library |
| `zig-out/bin/lattice` | CLI tool |
| `zig-out/include/lattice.h` | C header |

## Installation

### Python

```bash
pip install lattice-db
```

### Node.js

```bash
npm install lattice-db
```

### From Source

```bash
git clone https://github.com/jeffhajewski/latticedb.git
cd latticedb
zig build -Doptimize=ReleaseFast
```

## Query Language

Lattice uses Cypher with extensions for vector and full-text search.

### Pattern Matching

```cypher
-- Find all people who know each other
MATCH (a:Person)-[:KNOWS]->(b:Person)
RETURN a.name, b.name

-- Variable-length paths
MATCH (a:Person)-[:KNOWS*1..3]->(b:Person)
WHERE a.name = "Alice"
RETURN b.name, length(path) AS distance
```

### Vector Search

```cypher
-- Find similar documents using vector distance
MATCH (d:Document)
WHERE d.embedding <=> $query_vector < 0.3
RETURN d.title, d.embedding <=> $query_vector AS distance
ORDER BY distance
LIMIT 10
```

### Full-Text Search

```cypher
-- Search document content
MATCH (d:Document)
WHERE d.content @@ "machine learning neural networks"
RETURN d.title, d.content
```

### Combined Search

```cypher
-- Hybrid: vector similarity + text match + graph traversal
MATCH (chunk:Chunk)-[:PART_OF]->(doc:Document)
WHERE chunk.embedding <=> $query < 0.4
  AND doc.content @@ "introduction"
MATCH (doc)-[:AUTHORED_BY]->(author:Person)
RETURN doc.title, author.name, chunk.text
```

### Data Mutation

```cypher
-- Create nodes and relationships
CREATE (a:Person {name: "Alice"})-[:KNOWS]->(b:Person {name: "Bob"})

-- Update properties
MATCH (p:Person {name: "Alice"})
SET p.age = 31, p.city = "NYC"

-- Add labels
MATCH (p:Person {name: "Alice"})
SET p:Admin:Verified

-- Remove properties and labels
MATCH (p:Person {name: "Alice"})
REMOVE p.city, p:Verified

-- Delete nodes (DETACH removes connected edges)
MATCH (p:Person {name: "Bob"})
DETACH DELETE p
```

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      Lattice Database                       │
├─────────────────────────────────────────────────────────────┤
│  Cypher Parser  │  Query Planner  │  Volcano Executor       │
├─────────────────────────────────────────────────────────────┤
│  Graph Store    │  HNSW Index     │  Inverted Index         │
├─────────────────────────────────────────────────────────────┤
│  Transaction Manager  │  MVCC  │  Write-Ahead Log           │
├─────────────────────────────────────────────────────────────┤
│  Buffer Pool    │  Page Manager   │  B+Tree                 │
├─────────────────────────────────────────────────────────────┤
│                     Single File Storage                     │
└─────────────────────────────────────────────────────────────┘
```

## Performance

Lattice delivers competitive performance across graph, vector, and full-text search operations.

### Benchmark Results

| Operation | Latency | Throughput | Target | Status |
|-----------|---------|------------|--------|--------|
| Node lookup | 0.13 us | 7.9M ops/sec | < 1 us | PASS |
| Node creation | 0.65 us | 1.5M ops/sec | - | - |
| Edge traversal | 9 us | 111K ops/sec | - | - |
| 10-NN vector search (100 vectors) | 516 us | 2K ops/sec | < 10ms @ 1M | On track |
| Full-text search (100 docs) | 19 us | 53K ops/sec | - | - |

*Benchmarks run on Apple M1, single-threaded, with auto-scaled buffer pool. Run `zig build benchmark` to reproduce.*

### Competitive Analysis

#### Graph Operations

| System | Node Lookup | Type |
|--------|-------------|------|
| **Lattice** | **0.13 us** | Embedded |
| Neo4j | 10-50 us | Server |
| Kuzu | 0.5-1 us | Embedded |
| RedisGraph | 1-5 us | In-memory |

Lattice's B+Tree-based graph storage achieves sub-microsecond node lookups, outperforming most graph databases.

#### Vector Search (HNSW)

| System | 10-NN @ 1M vectors | Type |
|--------|-------------------|------|
| **Lattice** | ~3-5 ms (projected) | Embedded |
| FAISS (CPU) | 1-5 ms | Library |
| LanceDB | 5-20 ms | Embedded |
| Pinecone | 10-50 ms | Cloud |
| Milvus | 5-20 ms | Distributed |

HNSW scales as O(log N). Projected performance at 1M vectors meets the < 10ms target.

#### Full-Text Search (BM25)

| System | Search Latency | Type |
|--------|----------------|------|
| **Lattice** | **19 us** | Embedded |
| SQLite FTS5 | 10-50 us | Embedded |
| Tantivy | 10-100 us | Library |
| Elasticsearch | 1-10 ms | Server |

Lattice's inverted index with BM25 scoring is highly competitive with dedicated search engines.

### Design Targets

| Goal | Target | Status |
|------|--------|--------|
| Binary Size | < 500KB | In progress |
| Node Lookup | < 1 us | PASS (0.13 us) |
| Vector Search | < 10ms @ 1M vectors | On track |
| Memory | Configurable | PASS |
| Portability | Linux, macOS, Windows | In progress |

## Documentation

- [Architecture Specification](ARCHITECTURE_SPEC.md) — Detailed design document
- [Implementation Roadmap](context/ROADMAP.md) — Development phases and tasks
- [Design Decisions](context/decisions.md) — Rationale behind key choices

## Project Status

Lattice is in active development. See the [roadmap](context/ROADMAP.md) for current progress.

## Contributing

Contributions are welcome! Please read the architecture documentation before submitting changes to understand the design philosophy.

## License

[MIT License](LICENSE)
