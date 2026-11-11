# MERGE

`MERGE` matches an existing pattern or creates it if it doesn't exist. It's an idempotent "get or create" operation.

## Basic Usage

```cypher
MERGE (n:Person {name: "Alice"})
RETURN n
```

If a `Person` node with `name: "Alice"` exists, it is returned. Otherwise, one is created.

## MERGE with Properties

```cypher
MERGE (n:Person {name: "Alice"})
SET n.last_seen = "2024-01-15"
RETURN n
```

The `SET` clause runs whether the node was matched or created.

## ON CREATE and ON MATCH

To set properties only when the node is created, or only when it already existed:

```cypher
MERGE (n:Person {name: "Alice"})
ON CREATE SET n.created = "2024-01-15"
ON MATCH SET n.last_seen = "2024-01-15"
RETURN n
```

## MERGE Edges

```cypher
MATCH (a:Person {name: "Alice"}), (b:Person {name: "Bob"})
MERGE (a)-[:KNOWS]->(b)
```

This creates the `KNOWS` edge only if it doesn't already exist.

`MERGE` accepts a single node pattern or a single relationship between two
variables, as above; longer patterns are rejected.

## Use Cases

MERGE is useful when:

- Importing data that may have duplicates
- Ensuring a relationship exists without creating duplicates
- Building graphs incrementally from multiple data sources
