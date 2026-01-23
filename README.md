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
-- Find documents similar to a query, then traverse to author
MATCH (chunk:Chunk)-[:PART_OF]->(doc:Document)
WHERE chunk.embedding <=> $query_vector < 0.3
MATCH (doc)-[:AUTHORED_BY]->(author:Person)
RETURN doc.title, chunk.text, author.name
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
from lattice import Database

# Open or create a database
with Database("knowledge.db", create=True, enable_vector=True) as db:
    with db.write() as txn:
        # Create nodes
        doc = txn.create_node(
            labels=["Document"],
            properties={"title": "Introduction to Graph Databases", "author": "Jane Smith"}
        )

        chunk = txn.create_node(
            labels=["Chunk"],
            properties={"text": "Graph databases store data as nodes and edges..."}
        )
        txn.set_vector(chunk.id, "embedding", embedding_model.encode("Graph databases..."))

        # Create relationship
        txn.create_edge(chunk.id, doc.id, "PART_OF")
        txn.commit()

    # Query with Cypher
    results = db.query("""
        MATCH (c:Chunk)-[:PART_OF]->(d:Document)
        WHERE c.embedding <=> $query < 0.5
        RETURN d.title, c.text
        LIMIT 10
    """, parameters={"query": query_embedding})

    for row in results:
        print(f"{row['d.title']}: {row['c.text'][:100]}...")
```

### TypeScript

```typescript
import { Database } from 'lattice-db';

const db = new Database('knowledge.db', { create: true, enableVector: true });
await db.open();

await db.write(async (txn) => {
  // Create nodes
  const doc = await txn.createNode({
    labels: ['Document'],
    properties: { title: 'Introduction to Graph Databases', author: 'Jane Smith' }
  });

  const chunk = await txn.createNode({
    labels: ['Chunk'],
    properties: { text: 'Graph databases store data as nodes and edges...' }
  });
  await txn.setVector(chunk.id, 'embedding', new Float32Array(await embed('Graph databases...')));

  // Create relationship
  await txn.createEdge(chunk.id, doc.id, 'PART_OF');
});

// Query with Cypher
const results = await db.query(`
  MATCH (c:Chunk)-[:PART_OF]->(d:Document)
  WHERE c.embedding <=> $query < 0.5
  RETURN d.title, c.text
  LIMIT 10
`, { query: queryEmbedding });

await db.close();
```

### C

```c
#include <lattice.h>

int main() {
    lattice_database *db;
    lattice_open("knowledge.db", NULL, &db);

    lattice_txn *txn;
    lattice_begin(db, LATTICE_TXN_READ_WRITE, &txn);

    // Create a node
    lattice_node_id doc;
    lattice_node_create(txn, "Document", &doc);

    // Set a property
    lattice_value title = {
        .type = LATTICE_VALUE_STRING,
        .data.string_val = { "Introduction to Graph Databases", 31 }
    };
    lattice_node_set_property(txn, doc, "title", &title);

    lattice_commit(txn);
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
| `zig-out/lib/liblattice.dylib` | Shared library (macOS) |
| `zig-out/bin/lattice` | CLI tool |
| `include/lattice.h` | C header |

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

### Supported Features

| Feature | Status |
|---------|--------|
| MATCH, WHERE, RETURN | ✓ |
| CREATE, DELETE, SET, REMOVE | ✓ |
| ORDER BY, LIMIT, SKIP | ✓ |
| DETACH DELETE | ✓ |
| Aggregations (`count`, `sum`, `avg`, `min`, `max`, `collect`) | ✓ |
| Vector distance (`<=>`) | ✓ |
| Full-text search (`@@`) | ✓ |
| Functions: `id()`, `coalesce()`, `abs()`, `size()`, `toInteger()` | ✓ |
| Variable-length paths (`*1..3`) | ✓ |
| MERGE, WITH, OPTIONAL MATCH | Planned |

### Pattern Matching

```cypher
-- Find all people who know each other
MATCH (a:Person)-[:KNOWS]->(b:Person)
RETURN a.name, b.name

-- Multi-hop traversal
MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person)
WHERE a.name = "Alice"
RETURN c.name
```

### Variable-Length Paths

```cypher
-- Find all people within 1-3 hops
MATCH (a:Person)-[:KNOWS*1..3]->(b:Person)
RETURN a.name, b.name

-- Exactly 2 hops
MATCH (a:Person)-[:KNOWS*2]->(b:Person)
RETURN b.name

-- Any number of hops (unbounded)
MATCH (start:Root)-[:NEXT*]->(target:Node)
RETURN target

-- Minimum 2 hops, no maximum
MATCH (a:Person)-[:KNOWS*2..]->(b:Person)
RETURN b.name
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

### Aggregations

```cypher
-- Count nodes
MATCH (n:Person) RETURN count(n)

-- Statistics
MATCH (p:Person) RETURN min(p.age), max(p.age), avg(p.age), sum(p.age)

-- Collect values into a list
MATCH (p:Person) RETURN collect(p.name)
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

#### Graph Traversal vs SQLite

LatticeDB's native graph traversal compared to SQLite using JOINs and recursive CTEs on a social network graph with power-law degree distribution.

**Small Scale (10K nodes, 50K edges)**

| Workload | LatticeDB | SQLite | Speedup |
|----------|-----------|--------|--------:|
| 1-hop traversal | 840ns | 13.2μs | **15.7x** |
| 2-hop traversal | 6.2μs | 42.5μs | **6.8x** |
| 3-hop traversal | 19.9μs | 134.8μs | **6.8x** |
| Variable path (1..5) | 330.8μs | 4.5ms | **13.7x** |

**Medium Scale (100K nodes, 500K edges)**

| Workload | LatticeDB | SQLite | Speedup |
|----------|-----------|--------|--------:|
| 1-hop traversal | 13.0μs | 193.6μs | **14.9x** |
| 2-hop traversal | 69.5μs | 427.9μs | **6.2x** |
| 3-hop traversal | 380.7μs | 684.9μs | **1.8x** |
| Variable path (1..5) | 8.7ms | 9.0ms | **1.0x** |

LatticeDB outperforms SQLite across all workloads at both scales. The adjacency cache and increased buffer pool (16MB default) enable sub-millisecond 1-hop traversals and competitive variable-path performance even at 100K nodes. Run `zig build sqlite-benchmark` to reproduce.

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

Lattice has completed all core development phases:
- **Phase 1-3:** Storage engine, transactions, graph model
- **Phase 4:** Cypher query system with extensions
- **Phase 5:** HNSW vector search, BM25 full-text search
- **Phase 6:** C API, Python bindings, TypeScript bindings

See the [roadmap](context/ROADMAP.md) for detailed progress.

## Contributing

Contributions are welcome! Please read the architecture documentation before submitting changes to understand the design philosophy.

## License

[MIT License](LICENSE)
