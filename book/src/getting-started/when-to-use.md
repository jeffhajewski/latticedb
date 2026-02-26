# When to Use LatticeDB

LatticeDB is fast, but speed is not the only thing that matters. Here are cases where a different tool is the better choice.

## You need multiple applications writing to the same database at the same time

LatticeDB is embedded with a single-writer model. One process opens the file and owns it. If you need many clients connecting over a network, use Neo4j, PostgreSQL, or another client-server database.

## Your data is fundamentally tabular

If your data fits naturally into rows and columns — sales records, user accounts, time series — a relational database like SQLite or PostgreSQL will be simpler and just as fast. Graph databases shine when relationships between records are the point, not an afterthought.

## You need to scale beyond a single machine

LatticeDB stores everything in one file on one machine. If you need sharding, replication, or distributed queries across billions of nodes, look at Neo4j cluster, Dgraph, or a managed service like Neptune.

## You need the full Cypher language

LatticeDB supports most of Cypher but not all of it. Features like `OPTIONAL MATCH` and `CALL` procedures are not yet implemented. If your queries depend on these, Neo4j is the complete implementation.

## You need mature tooling and ecosystem

Neo4j has visualization tools, admin dashboards, monitoring, drivers in every language, and years of community resources. PostgreSQL has decades of tooling. LatticeDB is new and lean — which is a strength for embedding, but a weakness if you need a rich operational ecosystem around your database.
