# Choosing an Embedded Graph Database

Embedded graph databases run inside your process instead of behind a network socket. This page compares LatticeDB with the other options in that space, and with the databases people most often reach for instead.

If you are deciding whether LatticeDB fits your problem at all, start with [When to Use LatticeDB](../getting-started/when-to-use.md) — it is candid about where a different tool is the better answer.

## The landscape

| | LatticeDB | LadybugDB (ex-Kùzu) | SQLite | Neo4j | Chroma / LanceDB |
|---|---|---|---|---|---|
| **Deployment** | Embedded, one file | Embedded | Embedded, one file | Server (JVM embedded available) | Embedded |
| **Data model** | Property graph | Property graph | Relational | Property graph | Vectors + metadata |
| **Query language** | Cypher subset | Cypher | SQL | Full Cypher | Python/SDK API |
| **Graph traversal** | Native | Native | Recursive CTE | Native | No |
| **Vector search** | Native HNSW | Native | Extension (sqlite-vec) | Native vector index | Native |
| **Full-text search** | Native BM25 | Native | FTS5 extension | Lucene index | Varies |
| **Durable streams** | Native | No | No | No | No |
| **Storage shape** | Row-oriented, OLTP | Columnar, analytical | Row-oriented | Row-oriented | Columnar / Lance |
| **Maturity** | New | Fork of a mature engine | 25 years | 15+ years | Young |

## Detailed comparisons

- [LatticeDB vs SQLite](./vs-sqlite.md) — the closest philosophical relative, and the only comparison here measured head to head on the same machine
- [LatticeDB vs Kùzu and LadybugDB](./vs-kuzu.md) — the other embedded property-graph engines, and what happened to Kùzu
- [LatticeDB vs Neo4j](./vs-neo4j.md) — embedded versus server, and what you give up
- [LatticeDB vs vector databases](./vs-vector-databases.md) — Chroma, LanceDB, pgvector, and sqlite-vec for RAG workloads

## How to read the numbers

Benchmark comparisons in this section come from two very different places, and it matters which is which.

**Head-to-head measurements.** The SQLite comparison runs both engines on the same machine, over the same data, in the same benchmark harness (`zig build sqlite-benchmark` and `zig build graph-benchmark`), and you can reproduce them yourself. They are closer to comparable than anything else here, but not identical work: LatticeDB runs with the WAL off, and in the 1- to 3-hop tests it counts paths without the type filter and deduplication the SQLite query does. [Competitive Analysis](../performance/competitive-analysis.md#latticedb-vs-sqlite) has the details.

**Published third-party figures.** Numbers for Kùzu, Neo4j, Weaviate, Qdrant, Chroma, and the rest are taken from their own documentation or from third-party blog posts, on hardware we do not control, with methodology we did not choose. They are useful for order-of-magnitude orientation and nothing more. Do not read a 2x difference between LatticeDB and a third-party figure as meaningful.

Every figure and its source is listed in [Competitive Analysis](../performance/competitive-analysis.md). The raw LatticeDB measurements, including the hardware they were taken on, are in [Benchmarks](../performance/benchmarks.md).

## The short answer

Use **LatticeDB** when relationships, vector similarity, and text relevance are all part of the same question, and you want that answered locally without running a server or synchronising two stores.

Use **SQLite** when your data is fundamentally tabular and traversal is occasional.

Use **LadybugDB** when your graph workload is analytical — large scans, aggregations, Arrow and Parquet interoperability.

Use **Neo4j** when you need full Cypher, multi-client access, clustering, or the operational tooling of a mature ecosystem.

Use a **dedicated vector database** when vectors are the whole problem and there is no graph in it.
