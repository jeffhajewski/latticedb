# Competitive Analysis

## Point Lookups

| System | Latency | Type | Source |
|--------|---------|------|--------|
| **LatticeDB** | **0.13 us** | Embedded | `zig build benchmark` |
| RocksDB (in-memory) | 0.14 us | Embedded | RocksDB wiki |
| SQLite (in-memory) | ~0.2 us | Embedded | Turso blog |
| SQLite (WAL, disk) | 3 us (p90) | Embedded | marending.dev |
| Neo4j | 28 ms (p99) | Server | Memgraph comparison |

The LatticeDB figure is a cached lookup with the WAL off; the SQLite rows are
in-memory and on-disk figures from other people's runs.

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

The LatticeDB figure calls the HNSW index directly on synthetic clustered
vectors; the other rows were measured by others on their own datasets (often
SIFT or GloVe) and hardware.

## Graph Traversal

| System | 2-hop (100K nodes) | Type | Source |
|--------|-------------------|------|--------|
| **LatticeDB** | **39 us** | Embedded | `zig build sqlite-benchmark` |
| SQLite (recursive CTE) | 548 us | Embedded | `zig build sqlite-benchmark` |
| Kuzu (archived Oct 2025) | 19 ms | Embedded | The Data Quarry |
| Neo4j | 10 ms (1M nodes) | Server | Neo4j blog |

> Numbers marked with a third-party source were measured on hardware and with
> methodology we do not control, and are not directly comparable to the LatticeDB
> figures. Only the SQLite comparison below runs on the same machine in the same
> harness. Kùzu was archived in
> October 2025 following its creators' acquisition; see
> [vs Kùzu and LadybugDB](../comparisons/vs-kuzu.md).

### LatticeDB vs SQLite

Social network graph with power-law degree distribution, adjacency cache pre-warmed.
LatticeDB runs with the WAL off. In the 1-hop to variable-path tables
(`zig build sqlite-benchmark`), the two sides do not do the same work: LatticeDB
counts paths with raw edge iterators, with no edge-type filter and no
deduplication, while SQLite runs `SELECT DISTINCT` with a type filter and a join.

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

Measured with `zig build graph-benchmark`. LatticeDB uses BFS with adjacency cache and bitset visited tracking. SQLite uses a recursive CTE with `UNION` deduplication. In this test both compute identical reachable node sets (~8K nodes). `UNION` deduplicates on (node, depth), so SQLite expands a node again at every depth it is reached, while the visited bitset expands each node once; the gap measures that difference in algorithm and widens with depth.

## Full-Text Search (BM25)

| System | Search Latency | Type | Source |
|--------|----------------|------|--------|
| **LatticeDB** | **19 us** | Embedded | `zig build benchmark` |
| SQLite FTS5 | < 6 ms | Embedded | SQLite Cloud |
| Elasticsearch | 1-10 ms | Server | Various |
| Tantivy | 10-100 us | Library | Various |

The LatticeDB figure searches a 100-document corpus. The other rows come from
other workloads, and the Elasticsearch and Tantivy rows have no specific source,
so they are not comparable to it.
