# LatticeDB vs Neo4j

Neo4j is the reference property-graph database and the reason most people know Cypher at all. LatticeDB speaks a subset of the same language over the same data model, but it is a library rather than a server, which changes almost everything downstream of that choice.

If you are looking for a Neo4j alternative, the first question is not performance. It is whether you actually want a database process at all.

## Server versus embedded

Neo4j runs as a service. Your application opens a Bolt connection, sends a query, and receives a result set over the network. That indirection buys you multi-client access, independent scaling, clustering, and an operational surface you can monitor and back up with standard tools.

LatticeDB runs inside your process. There is no connection, no serialisation, no network hop, and no second thing to deploy. A query is a function call against pages cached in your own process. The database is one file you can copy, ship, or delete.

Neo4j does offer an embedded mode, but it is JVM-only — you embed it in a Java or Kotlin application. If your application is Python, TypeScript, Rust, Go, or C, embedded Neo4j is not available to you and the server is the only option.

## What you give up

This is the important section, and [When to Use LatticeDB](../getting-started/when-to-use.md) says it plainly. Summarised:

**Full Cypher.** LatticeDB implements a substantial subset, including `OPTIONAL MATCH`, `UNION`, `CASE`, `CALL { }` subqueries and `shortestPath`. Quantified path patterns, `LOAD CSV`, user-defined and APOC procedures, temporal and spatial types, and schema statements are not implemented. If your queries depend on them, Neo4j is the complete implementation and LatticeDB will refuse those queries.

**Multi-client access.** LatticeDB is single-writer, single-process. One process opens the file and owns it. If several applications need to write concurrently over a network, you need a server, and that server is Neo4j or Postgres.

**Scale beyond one machine.** Everything lives in one file on one host. Neo4j clusters; LatticeDB does not. Sharding, replication, and distributed query are out of scope by design.

**Tooling.** Neo4j Browser and Bloom for visualisation, admin dashboards, monitoring integrations, official drivers in every language, a decade of Stack Overflow answers, certification courses, and a consulting ecosystem. LatticeDB has documentation and a GitHub repository.

**Operational maturity.** Fifteen years of production deployments have found bugs that a new engine has not yet found.

## What you gain

**No server.** No process to deploy, secure, monitor, upgrade, or pay for. For a desktop application, a CLI tool, an edge deployment, a notebook, or a test suite, this is often the entire argument.

**No network hop.** A third-party comparison put Neo4j point lookups around 28 ms at p99; a Cypher lookup by an indexed property in LatticeDB, in process, takes about 2.3 us ([Benchmarks](../performance/benchmarks.md#point-lookups)). Most of that gap is the network and serialisation, not the storage engine — which is exactly the point of embedding, but it also means the comparison is measuring architecture rather than engineering, and the two figures come from different machines and methods. See [how to read the numbers](./overview.md#how-to-read-the-numbers).

**Vector and text search in the same engine.** Neo4j does vector indexes and Lucene-backed full-text, but LatticeDB was designed around combining them with traversal in one query rather than adding them alongside it.

**Durable streams.** Graph mutations as an ordered, replayable log inside the same file. See [Durable Streams](../guides/durable-streams.md).

**One file.** Backup is `cp`. Distribution is shipping a file. There is no import step for a colleague to run.

## Side by side

| | LatticeDB | Neo4j |
|---|---|---|
| Deployment | Library, in-process | Server (JVM embedded available) |
| Languages | C, Python, TypeScript, Go, Java | Drivers for everything; embedded is JVM-only |
| Cypher | Subset | Complete, plus procedures |
| Concurrency | Single writer, single process | Many clients |
| Clustering | No | Yes |
| Vector search | Native HNSW | Vector index |
| Full-text search | Native BM25 | Lucene index |
| Durable streams | Native | Change Data Capture (Enterprise) |
| Visualisation | None | Browser, Bloom |
| Licence | MIT | GPL / commercial |
| Operational cost | A file | A cluster |

## Choosing

Choose **Neo4j** when multiple services query the same graph, when you need Cypher features LatticeDB does not implement, when the graph outgrows one machine, or when the tooling and ecosystem are worth the operational cost.

Choose **LatticeDB** when the graph belongs to one application, when you want retrieval that mixes relationships with vector similarity and text relevance, and when not running a database server is a feature rather than a compromise.

A common and reasonable pattern is both: Neo4j as the system of record for a shared graph, LatticeDB embedded in an application or on a device holding the slice it needs to query locally and quickly.
