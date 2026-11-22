# LatticeDB vs SQLite

SQLite is the closest thing LatticeDB has to a design ancestor: one file, no server, embedded in your process, and boring in all the right ways. The difference is what the file is organised for. SQLite organises rows into tables; LatticeDB organises nodes into a graph, with vector and full-text indexes over their properties.

This is the only comparison in this section measured head to head. Both engines run on the same machine, over the same generated data, each through its own public C API with comparable durability settings, in the same program — `zig build capi-benchmark -- --suite core --suite sqlite`. You can reproduce every number below.

## When SQLite is the right answer

Start here, because it often is.

- **Your data is tabular.** Sales records, user accounts, event logs, time series. If your queries are filters and aggregations over rows, SQLite will be simpler, smaller, and just as fast.
- **You need many concurrent readers across processes.** SQLite in WAL mode handles this well. LatticeDB is single-writer and single-process.
- **You need ubiquity.** SQLite ships inside every phone, browser, and operating system on earth, has bindings for every language, and will still be readable in thirty years. LatticeDB is new.
- **You need the ecosystem.** Migration tools, GUI browsers, ORMs, hosted replicas. SQLite has all of it. LatticeDB has continuous backup and point-in-time restore built in, and almost nothing else yet.

If relationships in your data are an occasional join rather than the point of the query, use SQLite.

## Traversal: closer than you might think

Traversal in SQLite means joins of an edge table on itself, or a recursive common table expression for paths of any length. With the edge table indexed on its source column, that is fast, and for a few hops it is as fast as LatticeDB.

The figures below are LatticeDB 0.17.0 and SQLite 3.51.0 on an Apple M5 (macOS 26.5, 2026-11-13). Both write a log and fsync it on every commit (`journal_mode=WAL` and `synchronous=FULL` for SQLite, LatticeDB's defaults), with a 100 MB page cache each. The graph is Person nodes (rows) with 5 `KNOWS` edges out of each to random others; each query starts from a random node found through an index on an integer `uid` and counts what it reaches. Medians of five runs, p50 of 2,000 queries each.

| Workload | 10K nodes: LatticeDB | SQLite | 100K nodes: LatticeDB | SQLite |
|----------|---------------------:|-------:|----------------------:|-------:|
| 1 hop (5 paths) | 2.5 us | 2.5 us | 4.8 us | 3.6 us |
| 2 hops (25) | 4.4 us | 5.4 us | 8.8 us | 8.9 us |
| 3 hops (125) | 14 us | 14 us | 21 us | 29 us |
| 1 to 5 hops (about 3,905) | 472 us | 1,038 us | 600 us | 1,361 us |

LatticeDB runs Cypher (`MATCH (a:Person {uid: $uid})-[:KNOWS*1..5]->(b) RETURN count(b)`); SQLite runs joins, and a recursive CTE with `UNION ALL` for the last row. Both count the same paths. For a few hops the two are level; enumerating longer paths, LatticeDB is about twice as fast. Neither has a reachability operator that visits each node once: both enumerate paths, so the cost of a deep pattern follows the number of paths.

Point lookups by an indexed value are level too: 2.3 us for the Cypher query against 3.0 us for SQLite at 100,000 rows. Writes are not: SQLite inserts rows in transactions of 1,000 at 1.8 million a second, LatticeDB creates nodes at 330,000 a second (410,000 in a format-5 file, which the release after 0.18.1 creates), because a node also writes a label index entry, separate property records and changefeed events. A single durable commit costs about the same in both (44 us and 42 us at p50, within run-to-run noise). The full table is in [Competitive Analysis](../performance/competitive-analysis.md#measured-here-latticedb-and-sqlite).

Earlier editions of this page showed LatticeDB tens to thousands of times faster than SQLite at traversal. Those figures came from Zig code calling the storage layer over an in-memory adjacency cache that queries and the bindings never use, with LatticeDB's log off and SQLite doing more work per query; they are withdrawn.

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

We have not measured FTS5 or `sqlite-vec` against LatticeDB's full-text and vector indexes. `sqlite-vec` searches by brute force, which its author measured at around 17 ms over 1M vectors; LatticeDB's own full-text and vector figures are on [Benchmarks](../performance/benchmarks.md). Those are not a head-to-head comparison — see [how to read the numbers](./overview.md#how-to-read-the-numbers).

## Durability and transactions

Both engines are ACID with write-ahead logging and recover committed transactions after a crash. LatticeDB's crash tests simulate a crash by resetting the file header after a clean close; they do not kill a running process or tear data pages. LatticeDB's WAL, checkpointing, and recovery design is documented in [Architecture](../architecture/wal.md).

LatticeDB adds one thing SQLite has no equivalent for: [durable streams and changefeeds](../guides/durable-streams.md). Graph mutations can be consumed as an ordered, replayable log from inside the same file, which is useful when something downstream — an index, a cache, an embedding pipeline — needs to react to writes.

## Summary

| | LatticeDB | SQLite |
|---|---|---|
| Best at | Hybrid retrieval, connected data in one query | Tabular data, universal deployment |
| Query language | Cypher subset | SQL |
| Vector search | Native HNSW | Extension, brute force |
| Full-text search | Native BM25 | FTS5 extension |
| Concurrency | Single writer, single process | Multi-reader, single writer |
| Streams | Native | None |
| Maturity | New | 25 years, everywhere |

The honest framing: SQLite is a better general-purpose embedded database and will remain so. LatticeDB is a better embedded database for the specific shape of problem where relationships, semantics, and text all matter to the same query.
