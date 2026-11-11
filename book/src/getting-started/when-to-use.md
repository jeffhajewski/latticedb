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

LatticeDB supports the core of Cypher but not all of it. `OPTIONAL MATCH`, `CALL` procedures, `UNION`, `CASE`, `EXISTS`, list indexing, regular expressions (`=~`), and relationship type alternation (`[:A|B]`) are not implemented, and a variable bound in one `MATCH` cannot be named again in a later `MATCH`. If your queries depend on these, Neo4j is the complete implementation.

## You need mature tooling and ecosystem

Neo4j has visualization tools, admin dashboards, monitoring, drivers in every language, and years of community resources. PostgreSQL has decades of tooling. LatticeDB is new and lean — which is a strength for embedding, but a weakness if you need a rich operational ecosystem around your database.
