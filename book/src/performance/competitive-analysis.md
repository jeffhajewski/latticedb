# Competitive Analysis

This page keeps two kinds of figure apart.

- **Measured here.** LatticeDB and SQLite, on the same machine, over the same
  generated data, each through its own public C API with comparable durability
  settings, by the same program. These are the only comparative numbers this
  project stands behind.
- **Cited from others.** Figures other projects and authors published for their own
  systems, on their own hardware and data. They were never measured here and are
  not comparable to each other or to LatticeDB; they are listed for orientation,
  with their sources, in a separate section and never in one table with ours.

## Measured here: LatticeDB and SQLite

LatticeDB 0.17.0 and SQLite 3.51.0 (the system library of macOS 26.5), Apple M5,
2026-11-13, `zig build capi-benchmark -- --suite core --suite sqlite`. Both keep a
write-ahead log and fsync it on every commit (LatticeDB's default; SQLite with
`journal_mode=WAL`, `synchronous=FULL`), neither asks the drive to flush its cache
(`fullfsync` off on both), and both have a 100 MB page cache. SQLite runs prepared
statements; reads run inside `BEGIN`/`COMMIT`, as LatticeDB's run inside a read
transaction. Medians of five runs.

The data is the same on both sides: Person rows or nodes with an indexed integer
`uid` and a short name, and 5 `KNOWS` edges out of each to random others (a
`knows(src, dst)` table indexed on `src` for SQLite).

| Workload | LatticeDB | SQLite |
|----------|----------:|-------:|
| One insert, one commit: p50 | 44 us | 42 us |
| One insert, one commit, `normal` / `synchronous=NORMAL`: p50 | 6.4 us | 9.8 us |
| Inserts in transactions of 1,000 | 332,000/s | 1,840,000/s |
| Lookup by indexed `uid`, 100K: p50 | 2.3 us (Cypher) | 3.0 us |
| 1 hop, 10K nodes: p50 | 2.5 us | 2.5 us |
| 2 hops, 10K nodes: p50 | 4.4 us | 5.4 us |
| 3 hops, 10K nodes: p50 | 14 us | 14 us |
| Paths of 1 to 5 hops, 10K nodes (3,904 paths): p50 | 472 us | 1,038 us |
| 1 hop, 100K nodes: p50 | 4.8 us | 3.6 us |
| 2 hops, 100K nodes: p50 | 8.8 us | 8.9 us |
| 2 hops, distinct end nodes, 100K: p50 | 6.7 us | 18 us |
| 3 hops, 100K nodes: p50 | 21 us | 29 us |
| Paths of 1 to 5 hops, 100K nodes (3,905 paths): p50 | 600 us | 1,361 us |

The hop queries are `MATCH (a:Person {uid: $uid})-[:KNOWS]->()-[:KNOWS]->(c) RETURN
count(c)` and the like against joins of `knows` on itself; the 1-to-5 row is
`-[:KNOWS*1..5]->` against a recursive CTE with `UNION ALL`. Both count the same
paths (SQLite's walk may reuse an edge, which Cypher's may not; that differs by one
path in 3,905). The single-commit p50s are within each other's run-to-run spread
(SQLite's runs ranged 39–50 us, LatticeDB's 44–45 us); 20,000 commits a run.

Read this plainly: for one to three hops from an indexed starting point, an
indexed SQLite join is as fast as LatticeDB's Cypher. LatticeDB is about twice as
fast enumerating longer paths, and SQLite inserts rows several times faster than
LatticeDB creates nodes (a LatticeDB node writes a node record, a label index
entry, two property records and changefeed events, where SQLite writes a row and
an index entry). See [LatticeDB vs SQLite](../comparisons/vs-sqlite.md) for when
each is the better choice.

Earlier editions of this page compared SQLite with LatticeDB's in-process Zig
traversal over an adjacency cache, with LatticeDB's log off and SQLite doing more
work per query (deduplication, a type filter). Queries, the C API and the bindings
never use that cache, so those rows (including the depth-limited "2,819x") are
withdrawn.

## Cited from others

Collected for this documentation between December 2025 and August 2026 and not
re-checked since; each figure is the source's own, for its own system, hardware,
data and settings. None was measured here. LatticeDB's own figures are on
[Benchmarks](./benchmarks.md).

### Point lookups

| System | Published figure | Source |
|--------|------------------|--------|
| RocksDB (in-memory workload) | 0.14 us | [RocksDB wiki](https://github.com/facebook/rocksdb/wiki/RocksDB-In-Memory-Workload-Performance-Benchmarks) |
| SQLite (in-memory) | ~0.2 us | [Turso blog](https://turso.tech/blog/microsecond-level-sql-query-latency-with-libsql-local-replicas-5e4ae19b628b) |
| SQLite (WAL, on disk) | 3 us (p90) | [marending.dev](https://marending.dev/notes/sqlite-benchmarks/) |
| Neo4j (server) | 28 ms (p99) | [Memgraph comparison](https://memgraph.com/blog/memgraph-vs-neo4j-performance-benchmark-comparison) |

### Vector search, 10 nearest neighbours at about 1M vectors

| System | Published figure | Type | Source |
|--------|------------------|------|--------|
| FAISS HNSW (single thread) | 0.5–3 ms | Library | [FAISS wiki](https://github.com/facebookresearch/faiss/wiki/Indexing-1M-vectors) |
| Weaviate | 1.4 ms mean, 3.1 ms p99 | Server | [Weaviate benchmarks](https://docs.weaviate.io/weaviate/benchmarks/ann) |
| Qdrant | ~1–2 ms | Server | [Qdrant benchmarks](https://qdrant.tech/benchmarks/) |
| Milvus + SQ8 | 2.2 ms p99 | Server | [VectorDBBench](https://zilliz.com/vdbbench-leaderboard) |
| pgvector HNSW | ~5 ms at 99% recall | Extension | [Jonathan Katz](https://jkatz05.com/post/postgres/pgvector-performance-150x-speedup/) |
| LanceDB | 3–5 ms | Embedded | [LanceDB blog](https://medium.com/etoai/benchmarking-lancedb-92b01032874a) |
| Chroma | 4–5 ms mean | Embedded | [Chroma docs](https://docs.trychroma.com/production/administration/performance) |
| Pinecone P2 | ~15 ms including network | Cloud | [Pinecone blog](https://www.pinecone.io/blog/dedicated-read-nodes/) |
| sqlite-vec (brute force) | 17 ms | Extension | [Alex Garcia](https://alexgarcia.xyz/blog/2024/sqlite-vec-stable-release/index.html) |

These were measured mostly on public datasets (SIFT, GloVe and the like) at
recalls the sources chose; LatticeDB's vector figures are on synthetic clustered
vectors, so even a figure at the same scale is not a comparison.

### Graph traversal

| System | Published figure | Source |
|--------|------------------|--------|
| Kùzu (archived October 2025) | 19 ms, 2-hop, 100K nodes | [The Data Quarry](https://thedataquarry.com/blog/embedded-db-2/) |
| Neo4j | 10 ms, 1M nodes | [Neo4j blog](https://neo4j.com/news/how-much-faster-is-a-graph-database-really/) |

### Full-text search

| System | Published figure | Source |
|--------|------------------|--------|
| SQLite FTS5 | under 6 ms | [SQLite Cloud](https://blog.sqlite.ai/real-time-full-text-site-search-with-sqlite-fts5-extension) |

Rows that earlier editions gave without a source (Elasticsearch "1–10 ms", Tantivy
"10–100 us") are removed.
