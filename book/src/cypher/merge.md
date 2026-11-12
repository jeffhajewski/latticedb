# MERGE

`MERGE` matches an existing pattern or creates it if it doesn't exist. It's an idempotent "get or create" operation.

## Basic Usage

```cypher
MERGE (n:Person {name: "Alice"})
RETURN n
```

If a `Person` node with `name: "Alice"` exists, it is returned. Otherwise, one is created.

A node matches when it carries every label in the pattern and has every property
in it equal to the pattern's value, comparing as `=` does (an integer equals the
float of the same value, and lists and maps compare element by element). Other
labels and properties on the node do not matter. A pattern without a label
considers every node. Only when nothing matches is a node created, with all the
pattern's labels and properties.

When several nodes match, `MERGE` returns one row for each of them:

```cypher
MERGE (n:Person {field: "ML"})
RETURN n.name
```

A pattern property whose value is null fails the query: no node could ever match
it, so the `MERGE` would create a new node every time it ran.

When an explicit property index covers one of the pattern's labels and properties,
`MERGE` looks candidates up there instead of reading every node with the label.

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

`ON MATCH` applies to every node that matched.

## MERGE Edges

```cypher
MATCH (a:Person {name: "Alice"}), (b:Person {name: "Bob"})
MERGE (a)-[:KNOWS]->(b)
```

This creates the `KNOWS` edge only if it doesn't already exist. The relationship
needs exactly one type.

`MERGE` accepts a single node pattern or a single relationship, as above; longer
patterns are rejected.

## Use Cases

MERGE is useful when:

- Importing data that may have duplicates
- Ensuring a relationship exists without creating duplicates
- Building graphs incrementally from multiple data sources
