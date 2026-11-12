# When to Use LatticeDB

LatticeDB is fast, but speed is not the only thing that matters. Here are cases where a different tool is the better choice.

## You need multiple applications writing to the same database at the same time

LatticeDB is embedded, and one process opens the file and owns it. If you need many
clients connecting over a network, use Neo4j, PostgreSQL, or another database built
around a server.

This applies inside a single process too: only one write transaction can be open at a
time. Beginning a second one while the first is still open fails immediately rather
than waiting. Reads are unrestricted — as many as you like, at the same time, alongside
the writer. If your program writes from several threads, they need to take turns. See
[One writer at a time](../guides/transactions.md#one-writer-at-a-time) for what that
looks like in practice.

## Your data is fundamentally tabular

If your data fits naturally into rows and columns — sales records, user accounts, time series — a relational database like SQLite or PostgreSQL will be simpler and just as fast. Graph databases shine when relationships between records are the point, not an afterthought.

## You need to scale beyond a single machine

LatticeDB stores everything in one file on one machine. If you need sharding, replication, or distributed queries across billions of nodes, look at Neo4j cluster, Dgraph, or a managed service like Neptune.

## You need the full Cypher language

LatticeDB implements most of the Cypher people write day to day, `OPTIONAL
MATCH`, `UNION`, `CASE`, `CALL { }` subqueries, comprehensions and
`shortestPath` among it, but not all of the language. These are not
implemented:

- quantified path patterns (`((a)-->(b)){1,3}`) and label expressions
  (`(n:A|B)`)
- `LOAD CSV` and `CALL { } IN TRANSACTIONS`
- user-defined procedures and functions; the only procedures are the catalog
  ones, such as `db.labels()`
- temporal and spatial types
- schema and administration statements (`CREATE INDEX`, `SHOW`, `EXPLAIN`)

If your queries depend on these, Neo4j is the complete implementation. See
[Not Supported](../cypher/overview.md#not-supported) for the full list; each one
fails with an error that names it.

## You need mature tooling and ecosystem

Neo4j has visualization tools, admin dashboards, monitoring, drivers in every language, and years of community resources. PostgreSQL has decades of tooling. LatticeDB is new and lean — which is a strength for embedding, but a weakness if you need a rich operational ecosystem around your database.
