# LatticeDB vs SQLite

SQLite is the closest thing LatticeDB has to a design ancestor: one file, no server, embedded in your process, and boring in all the right ways. The difference is what the file is organised for. SQLite organises rows into tables; LatticeDB organises nodes into a graph, with vector and full-text indexes over their properties.

This is the only comparison in this section measured head to head. Both engines run on the same machine, over the same generated data, in the same harness — `zig build sqlite-benchmark`. You can reproduce every number below.

## When SQLite is the right answer

Start here, because it often is.

- **Your data is tabular.** Sales records, user accounts, event logs, time series. If your queries are filters and aggregations over rows, SQLite will be simpler, smaller, and just as fast.
- **You need many concurrent readers across processes.** SQLite in WAL mode handles this well. LatticeDB is single-writer and single-process.
- **You need ubiquity.** SQLite ships inside every phone, browser, and operating system on earth, has bindings for every language, and will still be readable in thirty years. LatticeDB is new.
- **You need the ecosystem.** Migration tools, GUI browsers, ORMs, hosted replicas. SQLite has all of it. LatticeDB has continuous backup and point-in-time restore built in, and almost nothing else yet.

If relationships in your data are an occasional join rather than the point of the query, use SQLite.

## Where the graph model pulls ahead

Traversal in SQLite means a recursive common table expression. It works, and for one or two hops it works fine. A depth-limited search written with `UNION` deduplicates on (node, depth), so a node reached at several depths is expanded again at each one.

LatticeDB traverses with BFS over an adjacency cache and a bitset for visited tracking, which expands each node once. Both engines below run over the same social-network graph with a power-law degree distribution, LatticeDB with the WAL off. The LatticeDB side is Zig code calling the storage layer in process: Cypher queries, the C API and the bindings read edges from the B+Tree and never use the adjacency cache (`enable_adjacency_cache` is accepted there and changes nothing), and a query has no reachability operator, so `MATCH (a)-[*1..k]->(b)` enumerates paths rather than visiting each node once. In the hop tables the two sides do not do identical work: LatticeDB counts paths with raw edge iterators, with no edge-type filter and no deduplication, while SQLite runs `SELECT DISTINCT` with a type filter and a join.

### 100K nodes, 500K edges

| Workload | LatticeDB | SQLite | Speedup |
|----------|-----------|--------|--------:|
| 1-hop traversal | 8.0 us | 290.0 us | 36x |
| 2-hop traversal | 38.7 us | 548.3 us | 14x |
| 3-hop traversal | 197.3 us | 1.2 ms | 6x |
| Variable path (1..5) | 134.4 us | 10.1 ms | 75x |

### Depth-limited traversal, 10K nodes

Here both engines compute the same reachable node sets (`zig build graph-benchmark`). The gap widens with depth because SQLite's `UNION` expands a node again at every depth it is reached, while LatticeDB's visited bitset expands it once: it measures that difference in algorithm.

| Depth | LatticeDB | SQLite | Speedup |
|------:|----------:|-------:|--------:|
| 10 | 311 us | 121 ms | 390x |
| 15 | 380 us | 271 ms | 713x |
| 25 | 318 us | 587 ms | 1,848x |
| 50 | 500 us | 1.4 s | 2,819x |

Read these as "how much does depth cost you", not as a claim that LatticeDB is three thousand times faster than SQLite. On point lookups the two are far closer: LatticeDB measures 0.13 us against roughly 0.2 us for in-memory SQLite. The B+Tree underneath is doing similar work.

## Search: one engine or three

The more practical difference is what you have to assemble.

Doing hybrid retrieval on SQLite means composing three things: FTS5 for text, `sqlite-vec` or a similar extension for vectors, and recursive CTEs for relationships. Each is good. But they are separate indexes with separate query syntax, and combining them means either multiple round trips or a query you would rather not maintain.

In LatticeDB, all three are the same query:

```cypher
MATCH (chunk:Chunk)-[:PART_OF]->(doc:Document)-[:AUTHORED_BY]->(author:Person)
WHERE chunk.embedding <=> $query_vector < 0.3
  AND chunk.text @@ 'transaction isolation'
RETURN doc.title, author.name, chunk.text
ORDER BY chunk.embedding <=> $query_vector
LIMIT 10
```

For reference, published figures put SQLite FTS5 under 6 ms for full-text search; LatticeDB measures 19 us on its own benchmark. Vector search in `sqlite-vec` is brute-force, measured by its author at around 17 ms over 1M vectors, against 0.83 ms for LatticeDB's HNSW index at 100% recall@10. Those two are not head-to-head measurements — see [how to read the numbers](./overview.md#how-to-read-the-numbers).

## Durability and transactions

Both engines are ACID with write-ahead logging and recover committed transactions after a crash. LatticeDB's crash tests simulate a crash by resetting the file header after a clean close; they do not kill a running process or tear data pages. LatticeDB's WAL, checkpointing, and recovery design is documented in [Architecture](../architecture/wal.md).

LatticeDB adds one thing SQLite has no equivalent for: [durable streams and changefeeds](../guides/durable-streams.md). Graph mutations can be consumed as an ordered, replayable log from inside the same file, which is useful when something downstream — an index, a cache, an embedding pipeline — needs to react to writes.

## Summary

| | LatticeDB | SQLite |
|---|---|---|
| Best at | Traversal, hybrid retrieval, connected data | Tabular data, universal deployment |
| Query language | Cypher subset | SQL |
| Vector search | Native HNSW | Extension, brute force |
| Full-text search | Native BM25 | FTS5 extension |
| Concurrency | Single writer, single process | Multi-reader, single writer |
| Streams | Native | None |
| Maturity | New | 25 years, everywhere |

The honest framing: SQLite is a better general-purpose embedded database and will remain so. LatticeDB is a better embedded database for the specific shape of problem where relationships, semantics, and text all matter to the same query.
