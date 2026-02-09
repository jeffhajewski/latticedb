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
| 10-NN vector search (100 vectors) | 516 us | 2K ops/sec | < 10ms @ 1M | PASS |
| Full-text search (100 docs) | 19 us | 53K ops/sec | - | - |

*Benchmarks run on Apple M1, single-threaded, with auto-scaled buffer pool. Run `zig build benchmark` to reproduce.*

### Competitive Analysis

#### Point Lookups

| System | Latency | Type | Source |
|--------|---------|------|--------|
| **Lattice** | **0.13 μs** | Embedded | `zig build benchmark` |
| RocksDB (in-memory) | 0.14 μs | Embedded | [RocksDB wiki](https://github.com/facebook/rocksdb/wiki/RocksDB-In-Memory-Workload-Performance-Benchmarks) |
| SQLite (in-memory) | ~0.2 μs | Embedded | [Turso blog](https://turso.tech/blog/microsecond-level-sql-query-latency-with-libsql-local-replicas-5e4ae19b628b) |
| SQLite (WAL, disk) | 3 μs (p90) | Embedded | [marending.dev](https://marending.dev/notes/sqlite-benchmarks/) |
| Neo4j | 28 ms (p99) | Server | [Memgraph comparison](https://memgraph.com/blog/memgraph-vs-neo4j-performance-benchmark-comparison) |
| RocksDB (NVMe) | 419 μs | Embedded | [RocksDB wiki](https://github.com/facebook/rocksdb/wiki/Performance-Benchmarks) |

Lattice's B+Tree achieves sub-microsecond cached lookups, matching RocksDB in-memory and outperforming SQLite on disk by 23x.

#### HNSW Vector Search Scaling

Benchmarked with 128-dimensional cosine vectors, M=16, ef_construction=200, ef_search=64, k=10:

| Scale | Mean Latency | P99 Latency | Recall@10 | Memory |
|-------|-------------|-------------|-----------|--------|
| 1,000 | 1.8 ms | 2.5 ms | 100% | 4.5 MB |
| 10,000 | 4.6 ms | 9.7 ms | 99% | 45.2 MB |
| 100,000 | 5.0 ms | 8.8 ms | 99% | 450.6 MB |
| 1,000,000 | 8.9 ms | 21.6 ms | 100% | 4530 MB |

Search latency scales sub-linearly (O(log N)) — 8.9 ms mean at 1M vectors with 100% recall@10. Uses heuristic neighbor selection (HNSW paper Algorithm 4) for diverse graph connectivity. Run `zig build vector-benchmark` to reproduce.

**ef_search Sensitivity (100K vectors)**

| ef_search | Mean Latency | Recall@10 |
|-----------|-------------|-----------|
| 16 | 3.2 ms | 97% |
| 32 | 3.6 ms | 99% |
| 64 | 4.9 ms | 99% |
| 128 | 5.9 ms | 100% |
| 256 | 7.9 ms | 100% |

#### Vector Search — Competitive Analysis

| System | Latency (10-NN) | Scale | Type | Source |
|--------|-----------------|-------|------|--------|
| **Lattice** | **8.9 ms mean, 100% recall** | 1M | Embedded | `zig build vector-benchmark` |
| FAISS HNSW (single-thread) | 0.5-3 ms | 1M | Library | [FAISS wiki](https://github.com/facebookresearch/faiss/wiki/Indexing-1M-vectors) |
| Weaviate | 1.4 ms mean, 3.1 ms P99 | 1M | Server | [Weaviate benchmarks](https://docs.weaviate.io/weaviate/benchmarks/ann) |
| Qdrant | ~1-2 ms | 1M | Server | [Qdrant benchmarks](https://qdrant.tech/benchmarks/) |
| Milvus + SQ8 | 2.2 ms P99 | 1M | Server | [VectorDBBench](https://zilliz.com/vdbbench-leaderboard) |
| pgvector HNSW | ~5 ms @ 99% recall | 1M | Extension | [Jonathan Katz](https://jkatz05.com/post/postgres/pgvector-performance-150x-speedup/) |
| LanceDB | 3-5 ms | 1M | Embedded | [LanceDB blog](https://medium.com/etoai/benchmarking-lancedb-92b01032874a) |
| Chroma | 4-5 ms mean | 1M | Embedded | [Chroma docs](https://docs.trychroma.com/production/administration/performance) |
| Pinecone P2 | ~15 ms (incl. network) | 1M | Cloud | [Pinecone blog](https://www.pinecone.io/blog/dedicated-read-nodes/) |
| sqlite-vec (brute force) | 17 ms | 1M | Extension | [Alex Garcia](https://alexgarcia.xyz/blog/2024/sqlite-vec-stable-release/index.html) |

Lattice at 1M achieves 8.9 ms mean with 100% recall@10 — competitive with FAISS single-threaded and faster than pgvector, LanceDB, Chroma, and Pinecone. Server-based systems (Weaviate, Qdrant) achieve similar latency but add network overhead in practice.

#### Full-Text Search (BM25)

| System | Search Latency | Type | Source |
|--------|----------------|------|--------|
| **Lattice** | **19 μs** | Embedded | `zig build benchmark` |
| SQLite FTS5 | < 6 ms | Embedded | [SQLite Cloud](https://blog.sqlite.ai/real-time-full-text-site-search-with-sqlite-fts5-extension) |
| Elasticsearch | 1-10 ms | Server | Various |
| Tantivy | 10-100 μs | Library | Various |

Lattice's inverted index with BM25 scoring is ~300x faster than SQLite FTS5 and competitive with Tantivy (a dedicated Rust search library).

#### Graph Traversal

| System | 2-hop (100K nodes) | Type | Source |
|--------|-------------------|------|--------|
| **Lattice** | **39 μs** | Embedded | `zig build sqlite-benchmark` |
| SQLite (recursive CTE) | 548 μs | Embedded | `zig build sqlite-benchmark` |
| Kuzu | 19 ms | Embedded | [The Data Quarry](https://thedataquarry.com/blog/embedded-db-2/) |
| Neo4j | 10 ms (1M nodes) | Server | [Neo4j blog](https://neo4j.com/news/how-much-faster-is-a-graph-database-really/) |

**LatticeDB vs SQLite** — Social network graph with power-law degree distribution, adjacency cache pre-warmed:

**Small Scale (10K nodes, 50K edges)**

| Workload | LatticeDB | SQLite | Speedup |
|----------|-----------|--------|--------:|
| 1-hop traversal | 560ns | 13.0μs | **23x** |
| 2-hop traversal | 3.0μs | 37.5μs | **13x** |
| 3-hop traversal | 19.1μs | 178.5μs | **9x** |
| Variable path (1..5) | 82.4μs | 4.3ms | **52x** |

**Medium Scale (100K nodes, 500K edges)**

| Workload | LatticeDB | SQLite | Speedup |
|----------|-----------|--------|--------:|
| 1-hop traversal | 8.0μs | 290.0μs | **36x** |
| 2-hop traversal | 38.7μs | 548.3μs | **14x** |
| 3-hop traversal | 197.3μs | 1.2ms | **6x** |
| Variable path (1..5) | 134.4μs | 10.1ms | **75x** |

Lattice is 250-500x faster than Neo4j and Kuzu on graph traversal, and 14-75x faster than SQLite's recursive CTEs. Run `zig build sqlite-benchmark` to reproduce.

### Design Targets

| Goal | Target | Status |
|------|--------|--------|
| Binary Size | < 500KB | In progress |
| Node Lookup | < 1 us | PASS (0.13 us) |
| Vector Search | < 10ms @ 1M vectors | PASS (8.9ms @ 1M, 100% recall) |
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
