# Competitive Analysis

## Point Lookups

| System | Latency | Type | Source |
|--------|---------|------|--------|
| **LatticeDB** | **0.13 us** | Embedded | `zig build benchmark` |
| RocksDB (in-memory) | 0.14 us | Embedded | RocksDB wiki |
| SQLite (in-memory) | ~0.2 us | Embedded | Turso blog |
| SQLite (WAL, disk) | 3 us (p90) | Embedded | marending.dev |
| Neo4j | 28 ms (p99) | Server | Memgraph comparison |

LatticeDB's B+Tree achieves sub-microsecond cached lookups, matching RocksDB in-memory and outperforming SQLite on disk by 23x.

## Vector Search

| System | Latency (10-NN) | Scale | Type | Source |
|--------|-----------------|-------|------|--------|
| **LatticeDB** | **0.83 ms mean, 100% recall** | 1M | Embedded | `zig build vector-benchmark` |
| FAISS HNSW (single-thread) | 0.5-3 ms | 1M | Library | FAISS wiki |
| Weaviate | 1.4 ms mean, 3.1 ms P99 | 1M | Server | Weaviate benchmarks |
| Qdrant | ~1-2 ms | 1M | Server | Qdrant benchmarks |
| Milvus + SQ8 | 2.2 ms P99 | 1M | Server | VectorDBBench |
| pgvector HNSW | ~5 ms @ 99% recall | 1M | Extension | Jonathan Katz |
| LanceDB | 3-5 ms | 1M | Embedded | LanceDB blog |
| Chroma | 4-5 ms mean | 1M | Embedded | Chroma docs |
| Pinecone P2 | ~15 ms (incl. network) | 1M | Cloud | Pinecone blog |
| sqlite-vec (brute force) | 17 ms | 1M | Extension | Alex Garcia |

LatticeDB at 1M achieves 0.83 ms mean with 100% recall@10 — faster than FAISS single-threaded HNSW and competitive with Weaviate and Qdrant server-based systems (which add network overhead in practice).

## Graph Traversal

| System | 2-hop (100K nodes) | Type | Source |
|--------|-------------------|------|--------|
| **LatticeDB** | **39 us** | Embedded | `zig build sqlite-benchmark` |
| SQLite (recursive CTE) | 548 us | Embedded | `zig build sqlite-benchmark` |
| Kuzu | 19 ms | Embedded | The Data Quarry |
| Neo4j | 10 ms (1M nodes) | Server | Neo4j blog |

### LatticeDB vs SQLite

Social network graph with power-law degree distribution, adjacency cache pre-warmed.

**Small Scale (10K nodes, 50K edges)**

| Workload | LatticeDB | SQLite | Speedup |
|----------|-----------|--------|--------:|
| 1-hop traversal | 560 ns | 13.0 us | **23x** |
| 2-hop traversal | 3.0 us | 37.5 us | **13x** |
| 3-hop traversal | 19.1 us | 178.5 us | **9x** |
| Variable path (1..5) | 82.4 us | 4.3 ms | **52x** |

**Medium Scale (100K nodes, 500K edges)**

| Workload | LatticeDB | SQLite | Speedup |
|----------|-----------|--------|--------:|
| 1-hop traversal | 8.0 us | 290.0 us | **36x** |
| 2-hop traversal | 38.7 us | 548.3 us | **14x** |
| 3-hop traversal | 197.3 us | 1.2 ms | **6x** |
| Variable path (1..5) | 134.4 us | 10.1 ms | **75x** |

**Depth-Limited Traversal (10K nodes, 50K edges)**

| Depth | LatticeDB | SQLite | Speedup |
|------:|----------:|-------:|--------:|
| 10    | 311 us    | 121 ms | **390x** |
| 15    | 380 us    | 271 ms | **713x** |
| 25    | 318 us    | 587 ms | **1,848x** |
| 50    | 500 us    | 1.4 s  | **2,819x** |

LatticeDB uses BFS with adjacency cache and bitset visited tracking. SQLite uses a recursive CTE with `UNION` deduplication. Both compute identical reachable node sets (~8K nodes). The gap widens at deeper depths as SQLite's CTE overhead grows with each recursion level.

## Full-Text Search (BM25)

| System | Search Latency | Type | Source |
|--------|----------------|------|--------|
| **LatticeDB** | **19 us** | Embedded | `zig build benchmark` |
| SQLite FTS5 | < 6 ms | Embedded | SQLite Cloud |
| Elasticsearch | 1-10 ms | Server | Various |
| Tantivy | 10-100 us | Library | Various |

LatticeDB's inverted index with BM25 scoring is ~300x faster than SQLite FTS5 and competitive with Tantivy (a dedicated Rust search library).
