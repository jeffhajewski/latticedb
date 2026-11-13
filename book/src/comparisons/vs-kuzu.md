# LatticeDB vs Kùzu and LadybugDB

Kùzu was the reference implementation of an embedded property-graph database: Cypher, vector search, and full-text search in a library you linked into your process. In October 2025 its creators were acquired by Apple and [the repository was archived](https://github.com/kuzudb/kuzu). It has received no commits since. If you are running Kùzu today, you are running unmaintained software, and this page exists partly to help you decide what to do about that.

## What replaced Kùzu

The community forked it. [LadybugDB](https://github.com/ladybugdb/ladybug) is the most active continuation, under development and positioning itself as a graph lakehouse — DuckDB storage interoperability, Arrow and Parquet in and out, object-store backends. It is a genuine successor and, if you want a drop-in path off Kùzu, it is the shortest one.

LatticeDB is not a Kùzu fork. It is an independent engine written in Zig with a different centre of gravity, so migrating to it is a port rather than a swap.

## The architectural difference that matters

Kùzu was built columnar, for analytical graph workloads: scan a large fraction of the graph, aggregate, join against tabular data. LadybugDB is doubling down on that with the lakehouse direction. It is the right design for "compute something over the whole graph."

LatticeDB is row-oriented and transactional. It is built for "answer this question about this neighbourhood, now, while a user waits" — point lookups, bounded traversals, retrieval, and writes that need to land durably and immediately.

Neither shape is better. They are answers to different questions, and the workload should pick.

| | LatticeDB | LadybugDB (ex-Kùzu) |
|---|---|---|
| Storage | Row-oriented | Columnar |
| Optimised for | Transactional, point and neighbourhood queries | Analytical, large scans and aggregations |
| Written in | Zig | C++ |
| Interoperability | Single self-contained file | Arrow, Parquet, DuckDB, object stores |
| Durable streams | Native | No |
| Cypher coverage | Subset | Broader |
| Maturity | New | Inherits a mature codebase |

## Published performance figures

We have not measured Kùzu or LadybugDB. The one figure in circulation is a
[third-party blog post](https://thedataquarry.com/blog/embedded-db-2/)'s 19 ms for a
2-hop traversal over 100K nodes, on hardware and with a query we do not know in
detail. LatticeDB's own 2-hop figure (a Cypher count from an indexed node, 100K
nodes and 500K edges, 8.8 us at p50) is on [Benchmarks](../performance/benchmarks.md#graph-traversal),
measured by a different method on different data, so the two are not a comparison.
If you care about the answer for your workload, run both.

## What LatticeDB has that Kùzu did not

**Durable streams and changefeeds.** Graph mutations are consumable as an ordered, replayable log from inside the same file. See [Durable Streams](../guides/durable-streams.md). Nothing in the Kùzu or LadybugDB lineage offers this.

**A single self-contained file.** No external format dependencies, no runtime, no dependencies at all. The database is one file you can copy.

**Active maintenance.** Relative to Kùzu specifically, which has none.

## What Kùzu had that LatticeDB does not

Being honest about this matters more than the section above.

**Broader Cypher coverage.** LatticeDB implements a subset. Quantified path patterns, `LOAD CSV`, user-defined procedures and functions, and temporal types are not there — see [When to Use LatticeDB](../getting-started/when-to-use.md).

**A mature codebase with real production mileage.** Kùzu had years of academic and industrial work behind it, and LadybugDB inherits all of it. LatticeDB is new, and new means undiscovered bugs.

**Analytical throughput.** If your query touches most of the graph, a columnar engine will beat a row-oriented one, and that is a structural property, not a tuning problem.

**Ecosystem interoperability.** Arrow, Parquet, and DuckDB integration are real advantages if your graph is one stage in a larger data pipeline.

## Choosing

Coming off Kùzu and want the shortest path with the least porting? **LadybugDB.**

Running analytical workloads over large graphs, or your graph lives in a lakehouse? **LadybugDB.**

Building an application that asks bounded questions about connected data — retrieval, recommendations, knowledge graphs behind an LLM — and wants vector similarity, text relevance, and traversal answered together, locally, with durable change streams? That is what **LatticeDB** is for.

Start with the [Quick Start](../getting-started/quickstart.md), or read [Core Concepts](../getting-started/concepts.md) if you want the data model first.
