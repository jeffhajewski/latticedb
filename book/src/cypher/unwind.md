# UNWIND

`UNWIND` expands a list into individual rows. Each element of the list becomes a separate row in the output.

## Basic Usage

```cypher
UNWIND [1, 2, 3] AS x
RETURN x
```

Returns three rows: `1`, `2`, `3`.

A null element is a row like any other: `UNWIND [1, null] AS v RETURN v` returns
`1` and `null`. Unwinding null itself, or an empty list, gives no rows, and a
value that is not a list gives one row. If the expression cannot be evaluated,
for instance because a parameter is missing, the query fails.

## Creating Multiple Nodes

Use `UNWIND` with `CREATE` to create nodes from a list:

```cypher
UNWIND ["Alice", "Bob", "Charlie"] AS name
CREATE (n:Person {name: name})
```

## With Parameters

Pass a list as a parameter and unwind it:

```cypher
UNWIND $names AS name
MATCH (n:Person {name: name})
RETURN n
```

## Combining with MATCH

```cypher
UNWIND $tags AS tag
MATCH (d:Document)
WHERE d.title CONTAINS tag
RETURN tag, d.title
```
