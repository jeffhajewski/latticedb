# LatticeDB

**The embedded knowledge graph for AI.**

LatticeDB is a single-file database that combines a property graph, vector search, and full-text search. It's built for RAG, agents, and any application where relationships between data matter as much as the data itself.

- **One file.** Your entire database is a single portable file. No server, no configuration.
- **Three search modes.** Graph traversal, HNSW vector similarity, and BM25 full-text — in one query.
- **Sub-millisecond.** 0.13 μs node lookups. 0.83 ms vector search at 1M vectors with 100% recall.

```cypher
-- Find chunks similar to a query, traverse to their document, then to the author
MATCH (chunk:Chunk)-[:PART_OF]->(doc:Document)-[:AUTHORED_BY]->(author:Person)
WHERE chunk.embedding <=> $query_vector < 0.3
  AND doc.content @@ "neural networks"
RETURN doc.title, chunk.text, author.name
ORDER BY chunk.embedding <=> $query_vector
LIMIT 10
```

## Install

**CLI**

```bash
curl -fsSL https://raw.githubusercontent.com/jeffhajewski/latticedb/main/dist/install.sh | bash
```

**Python**

```bash
pip install latticedb
```

**TypeScript / Node.js**

```bash
npm install @hajewski/latticedb
```

**Go**

See [bindings/go/README.md](bindings/go/README.md) for the current cgo workflow. The repo-local path uses `zig-out/lib`; installed builds can also use `pkg-config`.
There is also a runnable Graph RAG example in [examples/go](examples/go).

## Example

A complete example: create a small knowledge graph with documents and authors, store embeddings, index text, then query across all three search modes.

### Python

```python
from latticedb import Database, hash_embed

with Database("knowledge.db", create=True, enable_vector=True, vector_dimensions=128) as db:

    # --- Build the graph ---
    with db.write() as txn:
        # Create authors
        alice = txn.create_node(labels=["Person"], properties={"name": "Alice", "field": "ML"})
        bob = txn.create_node(labels=["Person"], properties={"name": "Bob", "field": "Systems"})
        txn.create_edge(alice.id, bob.id, "COLLABORATES_WITH")

        # Create documents with chunks
        for title, text, author in [
            ("Attention Is All You Need", "The transformer architecture uses self-attention...", alice),
            ("Scaling Laws for LLMs", "We find that model performance scales predictably...", alice),
            ("Log-Structured Merge Trees", "LSM trees optimize write-heavy workloads...", bob),
        ]:
            doc = txn.create_node(labels=["Document"], properties={"title": title})
            chunk = txn.create_node(labels=["Chunk"], properties={"text": text})

            # Store embedding and index text
            txn.set_vector(chunk.id, "embedding", hash_embed(text, dimensions=128))
            txn.fts_index(chunk.id, text)

            txn.create_edge(chunk.id, doc.id, "PART_OF")
            txn.create_edge(doc.id, author.id, "AUTHORED_BY")

        txn.commit()

    # --- Query: vector search + text match + graph traversal ---
    results = db.query("""
        MATCH (chunk:Chunk)-[:PART_OF]->(doc:Document)-[:AUTHORED_BY]->(author:Person)
        WHERE chunk.embedding <=> $query < 0.5
        RETURN doc.title, chunk.text, author.name
        ORDER BY chunk.embedding <=> $query
        LIMIT 5
    """, parameters={"query": hash_embed("transformer attention mechanism", dimensions=128)})

    for row in results:
        print(f"{row['doc.title']} by {row['author.name']}")

    # --- Full-text search ---
    for r in db.fts_search("self-attention transformer"):
        print(f"Node {r.node_id}: score={r.score:.4f}")

    # --- Aggregations ---
    stats = db.query("""
        MATCH (doc:Document)-[:AUTHORED_BY]->(p:Person)
        RETURN p.name, count(doc) AS papers
        ORDER BY papers DESC
    """)
    for row in stats:
        print(f"{row['p.name']}: {row['papers']} papers")
```

### TypeScript

```typescript
import { Database, hashEmbed } from "@hajewski/latticedb";

const db = new Database("knowledge.db", {
  create: true,
  enableVector: true,
  vectorDimensions: 128,
});
await db.open();

// Build a graph
await db.write(async (txn) => {
  const alice = await txn.createNode({
    labels: ["Person"],
    properties: { name: "Alice", field: "ML" },
  });
  const doc = await txn.createNode({
    labels: ["Document"],
    properties: { title: "Attention Is All You Need" },
  });
  const chunk = await txn.createNode({
    labels: ["Chunk"],
    properties: { text: "The transformer architecture uses self-attention..." },
  });

  await txn.setVector(chunk.id, "embedding", hashEmbed("transformer self-attention", 128));
  await txn.ftsIndex(chunk.id, "The transformer architecture uses self-attention...");

  await txn.createEdge(chunk.id, doc.id, "PART_OF");
  await txn.createEdge(doc.id, alice.id, "AUTHORED_BY");
});

// Query across vector search + graph traversal
const results = await db.query(
  `MATCH (chunk:Chunk)-[:PART_OF]->(doc:Document)-[:AUTHORED_BY]->(author:Person)
   WHERE chunk.embedding <=> $query < 0.5
   RETURN doc.title, chunk.text, author.name
   ORDER BY chunk.embedding <=> $query
   LIMIT 5`,
  { query: hashEmbed("attention mechanism", 128) }
);

for (const row of results.rows) {
  console.log(`${row["doc.title"]} by ${row["author.name"]}`);
}

await db.close();
```

### Go

```go
db, err := latticedb.Open("knowledge.db", latticedb.OpenOptions{
    Create: true,
    EnableVector: true,
    VectorDimensions: 128,
})
if err != nil {
    log.Fatal(err)
}
defer db.Close()

err = db.Update(func(tx *latticedb.Tx) error {
    node, err := tx.CreateNode(latticedb.CreateNodeOptions{
        Labels: []string{"Chunk"},
        Properties: map[string]latticedb.Value{"text": "The transformer architecture uses self-attention..."},
    })
    if err != nil {
        return err
    }
    if err := tx.SetVector(node.ID, "embedding", []float32{1, 0, 0, 0}); err != nil {
        return err
    }
    return tx.FTSIndex(node.ID, "The transformer architecture uses self-attention...")
})
if err != nil {
    log.Fatal(err)
}
```

## Performance

Benchmarked on Apple M1, single-threaded, with auto-scaled buffer pool. Run `zig build benchmark` to reproduce.

### Core Operations

| Operation | Latency | Throughput | Target | Status |
|-----------|---------|------------|--------|--------|
| Node lookup | 0.13 μs | 7.9M ops/sec | < 1 μs | PASS |
| Node creation | 0.65 μs | 1.5M ops/sec | — | — |
| Edge traversal | 9 μs | 111K ops/sec | — | — |
| Full-text search (100 docs) | 19 μs | 53K ops/sec | — | — |
| 10-NN vector search (1M vectors) | 0.83 ms | 1.2K ops/sec | < 10 ms @ 1M | PASS |

### Vector Search (HNSW) at Scale

128-dimensional cosine vectors, M=16, ef_construction=200, ef_search=64, k=10. Run `zig build vector-benchmark` to reproduce.

| Scale | Mean Latency | P99 Latency | Recall@10 | Memory |
|-------|-------------|-------------|-----------|--------|
| 1,000 | 65 μs | 70 μs | 100% | 1 MB |
| 10,000 | 174 μs | 695 μs | 99% | 10 MB |
| 100,000 | 438 μs | 1.2 ms | 99% | 101 MB |
| 1,000,000 | 832 μs | 1.8 ms | 100% | 1,040 MB |

Search latency scales sub-linearly (O(log N)) with 99–100% recall@10. Uses heuristic neighbor selection (HNSW paper Algorithm 4) for diverse graph connectivity, connection page packing for ~4.5x memory reduction, and pre-normalized dot product for fast cosine distance.

**ef_search Sensitivity (1M vectors)**

| ef_search | Mean Latency | Recall@10 |
|-----------|-------------|-----------|
| 16 | 506 μs | 57% |
| 32 | 1.9 ms | 79% |
| 64 | 990 μs | 100% |
| 128 | 3.2 ms | 100% |
| 256 | 11.6 ms | 100% |

### Competitive Analysis

#### Point Lookups

| System | Latency | Type | Source |
|--------|---------|------|--------|
| **LatticeDB** | **0.13 μs** | Embedded | `zig build benchmark` |
| RocksDB (in-memory) | 0.14 μs | Embedded | [RocksDB wiki](https://github.com/facebook/rocksdb/wiki/RocksDB-In-Memory-Workload-Performance-Benchmarks) |
| SQLite (in-memory) | ~0.2 μs | Embedded | [Turso blog](https://turso.tech/blog/microsecond-level-sql-query-latency-with-libsql-local-replicas-5e4ae19b628b) |
| SQLite (WAL, disk) | 3 μs (p90) | Embedded | [marending.dev](https://marending.dev/notes/sqlite-benchmarks/) |
| Neo4j | 28 ms (p99) | Server | [Memgraph comparison](https://memgraph.com/blog/memgraph-vs-neo4j-performance-benchmark-comparison) |

LatticeDB's B+Tree achieves sub-microsecond cached lookups, matching RocksDB in-memory and outperforming SQLite on disk by 23x.

#### Vector Search

| System | Latency (10-NN) | Scale | Type | Source |
|--------|-----------------|-------|------|--------|
| **LatticeDB** | **0.83 ms mean, 100% recall** | 1M | Embedded | `zig build vector-benchmark` |
| FAISS HNSW (single-thread) | 0.5–3 ms | 1M | Library | [FAISS wiki](https://github.com/facebookresearch/faiss/wiki/Indexing-1M-vectors) |
| Weaviate | 1.4 ms mean, 3.1 ms P99 | 1M | Server | [Weaviate benchmarks](https://docs.weaviate.io/weaviate/benchmarks/ann) |
| Qdrant | ~1–2 ms | 1M | Server | [Qdrant benchmarks](https://qdrant.tech/benchmarks/) |
| Milvus + SQ8 | 2.2 ms P99 | 1M | Server | [VectorDBBench](https://zilliz.com/vdbbench-leaderboard) |
| pgvector HNSW | ~5 ms @ 99% recall | 1M | Extension | [Jonathan Katz](https://jkatz05.com/post/postgres/pgvector-performance-150x-speedup/) |
| LanceDB | 3–5 ms | 1M | Embedded | [LanceDB blog](https://medium.com/etoai/benchmarking-lancedb-92b01032874a) |
| Chroma | 4–5 ms mean | 1M | Embedded | [Chroma docs](https://docs.trychroma.com/production/administration/performance) |
| Pinecone P2 | ~15 ms (incl. network) | 1M | Cloud | [Pinecone blog](https://www.pinecone.io/blog/dedicated-read-nodes/) |
| sqlite-vec (brute force) | 17 ms | 1M | Extension | [Alex Garcia](https://alexgarcia.xyz/blog/2024/sqlite-vec-stable-release/index.html) |

LatticeDB at 1M achieves 0.83 ms mean with 100% recall@10 — faster than FAISS single-threaded HNSW and competitive with Weaviate and Qdrant server-based systems (which add network overhead in practice).

#### Graph Traversal

| System | 2-hop (100K nodes) | Type | Source |
|--------|-------------------|------|--------|
| **LatticeDB** | **39 μs** | Embedded | `zig build sqlite-benchmark` |
| SQLite (recursive CTE) | 548 μs | Embedded | `zig build sqlite-benchmark` |
| Kuzu | 19 ms | Embedded | [The Data Quarry](https://thedataquarry.com/blog/embedded-db-2/) |
| Neo4j | 10 ms (1M nodes) | Server | [Neo4j blog](https://neo4j.com/news/how-much-faster-is-a-graph-database-really/) |

**LatticeDB vs SQLite** — Social network graph with power-law degree distribution, adjacency cache pre-warmed:

**Small Scale (10K nodes, 50K edges)**

| Workload | LatticeDB | SQLite | Speedup |
|----------|-----------|--------|--------:|
| 1-hop traversal | 560 ns | 13.0 μs | **23x** |
| 2-hop traversal | 3.0 μs | 37.5 μs | **13x** |
| 3-hop traversal | 19.1 μs | 178.5 μs | **9x** |
| Variable path (1..5) | 82.4 μs | 4.3 ms | **52x** |

**Medium Scale (100K nodes, 500K edges)**

| Workload | LatticeDB | SQLite | Speedup |
|----------|-----------|--------|--------:|
| 1-hop traversal | 8.0 μs | 290.0 μs | **36x** |
| 2-hop traversal | 38.7 μs | 548.3 μs | **14x** |
| 3-hop traversal | 197.3 μs | 1.2 ms | **6x** |
| Variable path (1..5) | 134.4 μs | 10.1 ms | **75x** |

**Depth-Limited Traversal (10K nodes, 50K edges)**

| Depth | LatticeDB | SQLite | Speedup |
|------:|----------:|-------:|--------:|
| 10    | 311 μs    | 121 ms | **390x** |
| 15    | 380 μs    | 271 ms | **713x** |
| 25    | 318 μs    | 587 ms | **1,848x** |
| 50    | 500 μs    | 1.4 s  | **2,819x** |

LatticeDB uses BFS with adjacency cache and bitset visited tracking. SQLite uses a recursive CTE with `UNION` deduplication. Both compute identical reachable node sets (~8K nodes). The gap widens at deeper depths as SQLite's CTE overhead grows with each recursion level. Run `zig build graph-benchmark -- --quick` to reproduce.

#### Full-Text Search (BM25)

| System | Search Latency | Type | Source |
|--------|----------------|------|--------|
| **LatticeDB** | **19 μs** | Embedded | `zig build benchmark` |
| SQLite FTS5 | < 6 ms | Embedded | [SQLite Cloud](https://blog.sqlite.ai/real-time-full-text-site-search-with-sqlite-fts5-extension) |
| Elasticsearch | 1–10 ms | Server | Various |
| Tantivy | 10–100 μs | Library | Various |

LatticeDB's inverted index with BM25 scoring is ~300x faster than SQLite FTS5 and competitive with Tantivy (a dedicated Rust search library).

## Features

**Graph**
- Nodes and edges with labels and arbitrary properties
- Multi-hop traversal, variable-length paths (`*1..3`)
- ACID transactions with snapshot isolation
- MERGE, WITH, UNWIND, aggregations (`count`, `sum`, `avg`, `min`, `max`, `collect`)

**Vector Search**
- HNSW approximate nearest neighbor with configurable M, ef
- Built-in hash embeddings or HTTP client for Ollama/OpenAI
- Batch insert for bulk loading

**Full-Text Search**
- BM25-ranked inverted index with tokenization and stemming
- Fuzzy search with configurable Levenshtein distance

**Cypher Query Language**
- MATCH, WHERE, RETURN, CREATE, DELETE, SET, REMOVE
- ORDER BY, LIMIT, SKIP, DETACH DELETE
- Vector distance operator: `<=>`
- Full-text search operator: `@@`
- Parameters: `$name`

**Operations**
- Single-file storage with write-ahead log for crash recovery
- Zero configuration — open a file and start working
- Clean C API; Python and TypeScript bindings wrap it

## Use Cases

- **RAG Systems** — Vector search finds relevant chunks, graph traversal gathers context
- **Knowledge Graphs** — Linked notes and documents with semantic search
- **AI Agents** — Persistent memory with relationship awareness
- **Local Development** — Lightweight alternative to Neo4j or Weaviate for prototyping

## When to Use Something Else

LatticeDB is fast, but speed is not the only thing that matters. Here are cases where a different tool is the better choice.

**You need multiple applications writing to the same database at the same time.**
LatticeDB is embedded with a single-writer model. One process opens the file and owns it. If you need many clients connecting over a network, use Neo4j, PostgreSQL, or another client-server database.

**Your data is fundamentally tabular.**
If your data fits naturally into rows and columns — sales records, user accounts, time series — a relational database like SQLite or PostgreSQL will be simpler and just as fast. Graph databases shine when relationships between records are the point, not an afterthought.

**You need to scale beyond a single machine.**
LatticeDB stores everything in one file on one machine. If you need sharding, replication, or distributed queries across billions of nodes, look at Neo4j cluster, Dgraph, or a managed service like Neptune.

**You need the full Cypher language.**
LatticeDB supports most of Cypher but not all of it. Features like `OPTIONAL MATCH` and `CALL` procedures are not yet implemented. If your queries depend on these, Neo4j is the complete implementation.

**You need mature tooling and ecosystem.**
Neo4j has visualization tools, admin dashboards, monitoring, drivers in every language, and years of community resources. PostgreSQL has decades of tooling. LatticeDB is new and lean — which is a strength for embedding, but a weakness if you need a rich operational ecosystem around your database.

## Building from Source

Written in Zig. No dependencies.

```bash
git clone https://github.com/jeffhajewski/latticedb.git
cd latticedb
zig build                  # build everything
zig build test             # run tests
zig build -Doptimize=ReleaseFast   # optimized build
```

## Documentation

- [Architecture Specification](ARCHITECTURE_SPEC.md)
- [Python API Reference](bindings/python/README.md)
- [TypeScript API Reference](bindings/typescript/README.md)
- [Go API Reference](bindings/go/README.md)
- [C API Header](include/lattice.h)

## License

[MIT](LICENSE)
